// The `gossipgrid genesis finalize` subcommand (Milestone 5B, Stage 1B).
//
// This is the explicit, operator-authorized finalization that consumes the
// one-time genesis authorization and arms the data-loss protection. It is a
// one-shot, fail-closed operation: it performs a preflight (the Kubernetes API
// must be readable) and then a single, atomic resourceVersion
// compare-and-swap (CAS) update on the authoritative cluster-metadata
// ConfigMap that sets, all together in one update:
//
//   - Initialized=true
//   - GenesisAuthorized=false
//   - GenesisConsumer=<the designated seed identity>
//   - DataLossProtected=true
//
// and preserves every other field of the authoritative metadata document
// (ClusterID, EpochID, Generation, EverAcceptedMessages, LastReset). It does
// NOT create a new cluster identity or epoch, and it does NOT delete or
// recreate the ConfigMap.
//
// The subcommand is NEVER run automatically: it is invoked explicitly by the
// operator (the finalization Job) after the operator has externally verified
// that the designated seed is durably bound to the exact cluster/epoch (via
// the peer-status and lifecycle diagnostics documented in
// docs/genesis-runbook.md). The command verifies the authoritative metadata
// itself and requires an explicit --confirm-seed-bound flag as the operator's
// attestation; it CANNOT read the seed's local SQLite (the seed's emptyDir is
// not accessible to the Job, and the Job's RBAC is scoped only to the
// ConfigMap).
//
// Crash safety and idempotency:
//
//   - A successful finalization is idempotent: a retry against an
//     already-finalized record (Initialized=true, GenesisAuthorized=false,
//     GenesisConsumer=<expected>, DataLossProtected=true, with the exact
//     expected ClusterID/EpochID) reports "already finalized" (a benign,
//     expected outcome — the authorization is already consumed). No new
//     record is created and no new identity is generated; the command exits 0.
//   - A mismatched or corrupted finalized record fails closed (a non-zero
//     exit). It is never treated as a success.
//   - A CAS conflict (another writer advanced the resourceVersion) is
//     resolved by re-reading the authoritative object and re-checking the
//     preconditions; never by generating a replacement identity.
//   - An ambiguous API failure (the API server may have performed the update
//     but the response was lost) is NEVER resolved by inferring success: the
//     command re-reads the authoritative object and classifies it. Only a
//     confirmed, coherent finalized state matching the expected identities
//     reports as an idempotent success.
//
// The subcommand uses an injectable client (finalizeClient interface) so unit
// tests can supply a deterministic fake without the official Kubernetes client
// libraries. The production adapter (newFinalizeClient) uses the official
// client-go ConfigMap client (in-cluster configuration) and classifies API
// errors via k8s.io/apimachinery's typed StatusError (IsNotFound / IsForbidden
// / IsConflict / IsTimeout / IsServerTimeout) — not message-text matching.
//
// Namespace detection: when --namespace is not supplied, the namespace is
// read from the in-cluster service account's mounted namespace file
// (/var/run/secrets/kubernetes.io/serviceaccount/namespace). The file path is
// stored in a package-level variable (inClusterNamespacePath) so unit tests
// can override it with a temporary file (the same variable the `genesis`
// subcommand uses).
package main

import (
	"context"
	stderrors "errors"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"gossipgrid/internal/lifecycle/genesis"
	"gossipgrid/internal/lifecycle/k8smetadata"
)

// finalizeClient is the injectable client contract the `genesis finalize`
// subcommand uses to perform the CAS finalization. It is implemented by
// newFinalizeClient (the production adapter over the official client-go
// ConfigMap client) and by a deterministic fake in unit tests.
//
// The returned error from Get and Update must carry its API classification
// as a typed error (k8smetadata.NewAPIError / NewConfigMapNotFound /
// NewAPIUnavailable / NewAPIPermissionDenied / NewAPIConflict). The finalization
// classifies the error by its type, never by the error's message text.
//
// Update performs a resourceVersion compare-and-swap: it succeeds only when
// the current resourceVersion of the object equals expectedResourceVersion
// (the revision read earlier by Get). An advanced resourceVersion (another
// writer) yields a typed conflict (409) error (k8smetadata.NewAPIConflict).
type finalizeClient interface {
	Get(ctx context.Context, name, namespace string) (*k8smetadata.ConfigMap, error)
	Update(ctx context.Context, name, namespace, expectedResourceVersion string, data map[string]string) (*k8smetadata.ConfigMap, error)
}

// finalizeFlags holds the parsed flags for the `genesis finalize` subcommand.
type finalizeFlags struct {
	namespace        string
	configMap        string
	clusterID        string
	epochID          string
	genesisConsumer  string
	confirmSeedBound bool
}

// newFinalizeClient is the client factory for the `genesis finalize` subcommand.
// It is a package-level variable (not a function) so unit tests can override it
// with an injectable fake, enabling the complete CLI handling of the
// finalization to be tested without a live Kubernetes cluster.
var newFinalizeClient = newClientGoFinalizeClient

// runGenesisFinalize handles the `gossipgrid genesis finalize` subcommand.
// Flags:
//
//	--namespace <ns>           (default: the in-cluster namespace; required when
//	                              not running in-cluster)
//	--configmap <name>         (default: gossipgrid-cluster-metadata)
//	--cluster-id <id>          (required: the expected, authoritative cluster
//	                              identity from the grant)
//	--epoch-id <id>            (required: the expected, authoritative epoch
//	                              identity from the grant)
//	--genesis-consumer <id>    (required: the designated seed identity, e.g.
//	                              gossipgrid-0)
//	--confirm-seed-bound       (required: the operator's explicit attestation
//	                              that the seed is durably bound to the exact
//	                              cluster/epoch, verified externally)
//
// The subcommand performs the operator-authorized finalization. It exits 0 on
// a fresh finalization or when the finalization is idempotent (an existing
// coherent finalized record matching the expected identities — "already
// finalized"), and nonzero on any failure (a mismatched finalized record, no
// outstanding authorization, a corrupted record, an ambiguous result, a
// permission denial, or an API unavailability). The finalization is fail-closed:
// a command that cannot verify its preconditions (an unreadable API, or a
// missing --confirm-seed-bound) fails rather than inferring safety.
func runGenesisFinalize(args []string) error {
	f := finalizeFlags{
		namespace:        "",
		configMap:        genesis.ConfigMapName,
		clusterID:        "",
		epochID:          "",
		genesisConsumer:  "",
		confirmSeedBound: false,
	}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--namespace":
			if i+1 >= len(args) {
				return stderrors.New("genesis finalize: --namespace requires a value")
			}
			i++
			f.namespace = args[i]
		case "--configmap":
			if i+1 >= len(args) {
				return stderrors.New("genesis finalize: --configmap requires a value")
			}
			i++
			// The ConfigMap name is canonical (a single, cluster-wide record).
			// A different value has no effect (the finalization always uses the
			// canonical name), so it is rejected rather than silently accepted.
			if args[i] != genesis.ConfigMapName {
				return fmt.Errorf("genesis finalize: --configmap must be %q (the canonical cluster-metadata ConfigMap name); got %q", genesis.ConfigMapName, args[i])
			}
			f.configMap = args[i]
		case "--cluster-id":
			if i+1 >= len(args) {
				return stderrors.New("genesis finalize: --cluster-id requires a value")
			}
			i++
			f.clusterID = args[i]
		case "--epoch-id":
			if i+1 >= len(args) {
				return stderrors.New("genesis finalize: --epoch-id requires a value")
			}
			i++
			f.epochID = args[i]
		case "--genesis-consumer":
			if i+1 >= len(args) {
				return stderrors.New("genesis finalize: --genesis-consumer requires a value")
			}
			i++
			f.genesisConsumer = args[i]
		case "--confirm-seed-bound":
			// A boolean flag (no value). The operator's explicit attestation
			// that the seed is durably bound to the exact cluster/epoch.
			f.confirmSeedBound = true
		default:
			return fmt.Errorf("genesis finalize: unexpected argument %q", args[i])
		}
	}

	// The identity context is required. An empty ClusterID, EpochID, or
	// GenesisConsumer is a fail-closed condition (the finalization cannot
	// verify the record without the expected identities).
	if f.clusterID == "" || f.epochID == "" || f.genesisConsumer == "" {
		return stderrors.New("genesis finalize: --cluster-id, --epoch-id, and --genesis-consumer are all required")
	}

	// Build the client (in-cluster configuration). A client build failure
	// (no service account, no API server) is a fail-closed condition: the
	// finalization cannot proceed without a working Kubernetes API. The
	// factory is a package-level variable so unit tests can inject a
	// deterministic fake.
	client, err := newFinalizeClient()
	if err != nil {
		return fmt.Errorf("genesis finalize: building Kubernetes client: %w", err)
	}

	// Resolve the namespace. When the operator does not supply --namespace,
	// the namespace is read from the in-cluster service account's mounted
	// namespace file (the finalization Job runs in-cluster in the
	// application namespace). A missing or empty namespace is a fail-closed
	// condition (the finalization cannot update a ConfigMap without a
	// namespace).
	ns := f.namespace
	if ns == "" {
		ns = inClusterNamespace()
	}
	if ns == "" {
		return stderrors.New("genesis finalize: namespace is empty (the finalization cannot update a ConfigMap without a namespace; supply --namespace or run in-cluster)")
	}

	op, err := genesis.NewFinalizeOperation(ns, client)
	if err != nil {
		return err
	}

	// Perform the finalization. The context is the process's background
	// context (the finalization is a one-shot operation; it completes
	// promptly or fails closed). A process cancellation (SIGTERM) is
	// surfaced as a context error and the finalization fails closed.
	ctx := context.Background()
	cfg := genesis.FinalizeConfig{
		ClusterID:        f.clusterID,
		EpochID:          f.epochID,
		GenesisConsumer:  f.genesisConsumer,
		ConfirmSeedBound: f.confirmSeedBound,
	}
	res, ferr := op.Finalize(ctx, cfg)
	// An existing coherent finalized record matching the expected identities
	// is a successful idempotent outcome: the one-time authorization is
	// already consumed. The Finalize function returns ErrAlreadyFinalized as
	// an error (it is inspectable), but the CLI must treat it as success
	// (exit 0) — the documented contract.
	if ferr != nil {
		if stderrors.Is(ferr, genesis.ErrAlreadyFinalized) {
			fmt.Fprintf(os.Stderr,
				"genesis finalize: cluster metadata is already finalized with the expected identities (already finalized): cluster=%s epoch=%s genesisConsumer=%s dataLossProtected=%v\n",
				res.Metadata.ClusterID, res.Metadata.EpochID, res.Metadata.GenesisConsumer, res.Metadata.DataLossProtected)
			fmt.Fprintln(os.Stderr, "genesis finalize: the one-time authorization is already consumed; the seed will be promoted to READY when it next reads the metadata.")
			return nil
		}
		// A failed or ambiguous finalization: surface the error (the operator
		// investigates). A mismatched finalized record or a corrupted record
		// is never overwritten; the operator must inspect the authoritative
		// ConfigMap before retrying.
		return ferr
	}
	fmt.Fprintf(os.Stderr,
		"genesis finalize: cluster metadata finalized: cluster=%s epoch=%s genesisConsumer=%s dataLossProtected=%v\n",
		res.Metadata.ClusterID, res.Metadata.EpochID, res.Metadata.GenesisConsumer, res.Metadata.DataLossProtected)
	fmt.Fprintln(os.Stderr, "genesis finalize: the one-time genesis authorization is consumed (Initialized=true, GenesisAuthorized=false, GenesisConsumer=<seed>, DataLossProtected=true).")
	fmt.Fprintln(os.Stderr, "genesis finalize: the seed will be promoted to READY when it next reads the metadata; do NOT run this command again (it is idempotent but unnecessary).")
	return nil
}

// clientGoFinalizeConfigMap is the production adapter over the official
// client-go ConfigMap client for the `genesis finalize` subcommand. It
// implements finalizeClient and classifies API errors via
// k8s.io/apimachinery's typed StatusError (IsNotFound / IsConflict /
// IsForbidden / IsTimeout / IsServerTimeout) — not message-text matching.
//
// A 409 conflict (an advanced resourceVersion during an Update) is classified
// as APIClassConflict (k8smetadata.NewAPIConflict). The finalization's
// resolution logic re-reads the authoritative object on any non-success, so
// the conflict is resolved by re-reading and re-classifying the record
// (already finalized / mismatched / outstanding / corrupted) — never by
// inferring a success.
type clientGoFinalizeConfigMap struct {
	clientset *kubernetes.Clientset
}

// newClientGoFinalizeClient builds the production Kubernetes client using
// in-cluster configuration (the finalization Job runs in-cluster with the
// finalization ServiceAccount). A build failure (no service account, no API
// server, a malformed config) is returned as a non-nil error (fail-closed).
func newClientGoFinalizeClient() (finalizeClient, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building client-go clientset: %w", err)
	}
	return &clientGoFinalizeConfigMap{clientset: clientset}, nil
}

// Get implements finalizeClient. It reads the named ConfigMap and classifies
// the result: a missing object → k8smetadata.NewConfigMapNotFound; a
// permission denial → k8smetadata.NewAPIPermissionDenied; a transient
// unavailability (timeout or 5xx) → k8smetadata.NewAPIUnavailable; an
// unclassified error → k8smetadata.NewAPIError(APIClassUnknown, err).
func (c *clientGoFinalizeConfigMap) Get(ctx context.Context, name, namespace string) (*k8smetadata.ConfigMap, error) {
	cm, err := c.clientset.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, classifyFinalizeK8sError(err)
	}
	return &k8smetadata.ConfigMap{
		ResourceVersion: cm.ResourceVersion,
		Data:            cm.Data,
	}, nil
}

// Update implements finalizeClient. It performs a resourceVersion
// compare-and-swap on the named ConfigMap (Kubernetes update semantics:
// succeeds only when the object's current resourceVersion equals
// expectedResourceVersion; an advanced resourceVersion yields a 409 conflict
// error). The 409 is classified as APIClassConflict (k8smetadata.NewAPIConflict)
// and is resolved by the finalization's re-read path (resolveAfterUpdateError),
// which re-reads the authoritative object and classifies it (already finalized
// / mismatched / outstanding / corrupted). The exact class of a 409 is
// load-bearing only in that it must be distinguishable from an unavailability
// (a 409 means another writer advanced the revision; an unavailability means
// the API is degraded).
func (c *clientGoFinalizeConfigMap) Update(ctx context.Context, name, namespace, expectedResourceVersion string, data map[string]string) (*k8smetadata.ConfigMap, error) {
	// The ConfigMap is updated with the given data map (the metadata document
	// at the "metadata" key). The object's resourceVersion is set to
	// expectedResourceVersion (the CAS compare). No labels, annotations, or
	// other metadata: the record is a single, cluster-wide document.
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			ResourceVersion: expectedResourceVersion,
		},
		Data: data,
	}
	updated, err := c.clientset.CoreV1().ConfigMaps(namespace).Update(ctx, cm, metav1.UpdateOptions{})
	if err != nil {
		return nil, classifyFinalizeK8sError(err)
	}
	return &k8smetadata.ConfigMap{
		ResourceVersion: updated.ResourceVersion,
		Data:            updated.Data,
	}, nil
}

// classifyFinalizeK8sError maps a k8s.io/client-go API error to the k8smetadata
// typed error classification for the finalization. It uses
// k8s.io/apimachinery's typed StatusError predicates (IsNotFound /
// IsConflict / IsForbidden / IsTimeout / IsServerTimeout) — not message-text
// matching. A 409 conflict (an advanced resourceVersion) is classified as
// APIClassConflict (k8smetadata.NewAPIConflict): the finalization re-reads the
// authoritative object on any non-success and classifies it. An unclassified
// error is conservatively treated as unavailability (the finalization re-reads
// or fails closed; it never classifies by message text).
func classifyFinalizeK8sError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case apierrors.IsNotFound(err):
		return k8smetadata.NewConfigMapNotFound(err)
	case apierrors.IsConflict(err):
		// A 409 conflict (an advanced resourceVersion during an Update CAS).
		// Classified as APIClassConflict (a CAS whose compare no longer
		// matches). The finalization re-reads the authoritative object on
		// any non-success and re-checks the preconditions (already finalized
		// / mismatched / outstanding / corrupted). It never generates a
		// replacement identity and never infers a success from the conflict
		// alone.
		return k8smetadata.NewAPIConflict(err)
	case apierrors.IsForbidden(err):
		return k8smetadata.NewAPIPermissionDenied(err)
	case apierrors.IsTimeout(err):
		return k8smetadata.NewAPIUnavailable(err)
	case apierrors.IsServerTimeout(err):
		return k8smetadata.NewAPIUnavailable(err)
	default:
		// A 5xx or any other unclassified error: conservatively treated as an
		// unavailability (the finalization re-reads or fails closed; it never
		// classifies by message text).
		if is5xxStatus(err) {
			return k8smetadata.NewAPIUnavailable(err)
		}
		return k8smetadata.NewAPIError(k8smetadata.APIClassUnknown, err)
	}
}

// _ is a compile-time assertion that clientGoFinalizeConfigMap satisfies
// finalizeClient and genesis.ConfigMapUpdater (the FinalizeOperation's adapter
// contract).
var (
	_ finalizeClient           = (*clientGoFinalizeConfigMap)(nil)
	_ genesis.ConfigMapUpdater = (*clientGoFinalizeConfigMap)(nil)
)
