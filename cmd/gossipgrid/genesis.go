// The `gossipgrid genesis` subcommand (Milestone 5B, Stage 1A).
//
// This is the explicit, operator-authorized grant that creates the
// authoritative cluster-metadata ConfigMap. It is a one-shot, fail-closed
// operation: it performs a preflight (the Kubernetes API must be readable and
// the ConfigMap must be absent or a valid pending grant) and then a single
// create. An ambiguous failure may occur after the ConfigMap was created
// (e.g., the API server creates the object but times out before returning the
// response); the grant never generates a replacement identity, but the
// operator must inspect the authoritative ConfigMap metadata before retrying.
//
// The subcommand is NEVER run automatically: it is invoked explicitly by the
// operator (the genesis Job) after the operator has verified the preconditions
// documented in docs/genesis-runbook.md (a fresh, empty
// cluster — no prior epoch, no recoverable data). The existing 7A staging
// deployment is data-bearing and must NOT be treated as a fresh cluster.
//
// The subcommand uses an injectable client (k8sClient interface) so unit tests
// can supply a deterministic fake without the official Kubernetes client
// libraries. The production adapter (newClientGoClient) uses the official
// client-go ConfigMap client (in-cluster configuration) and classifies API
// errors via k8s.io/apimachinery's typed StatusError (IsNotFound / IsForbidden
// / IsTimeout) — not message-text matching.
//
// Namespace detection: when --namespace is not supplied, the namespace is
// read from the in-cluster service account's mounted namespace file
// (/var/run/secrets/kubernetes.io/serviceaccount/namespace), which
// Kubernetes populates as plain text. The file path is stored in a
// package-level variable (inClusterNamespacePath) so unit tests can
// override it with a temporary file.
package main

import (
	"context"
	stderrors "errors"
	"fmt"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"gossipgrid/internal/lifecycle/genesis"
	"gossipgrid/internal/lifecycle/k8smetadata"
)

// k8sClient is the injectable client contract the genesis subcommand uses to
// perform the grant. It is implemented by newClientGoClient (the production
// adapter over the official client-go ConfigMap client) and by a deterministic
// fake in unit tests.
//
// The returned error from Get must carry its API classification as a typed
// error (k8smetadata.NewAPIError / NewConfigMapNotFound / NewAPIUnavailable /
// NewAPIPermissionDenied). The genesis grant classifies the error by its type,
// never by the error's message text.
type k8sClient interface {
	Get(ctx context.Context, name, namespace string) (*k8smetadata.ConfigMap, error)
	Create(ctx context.Context, name, namespace string, data map[string]string) (*k8smetadata.ConfigMap, error)
}

// genesisFlags holds the parsed flags for the `genesis` subcommand.
type genesisFlags struct {
	namespace string
	configMap string
}

// newGenesisClient is the client factory for the genesis subcommand. It is a
// package-level variable (not a function) so unit tests can override it with
// an injectable fake, enabling the complete CLI handling of an existing grant
// to be tested without a live Kubernetes cluster.
var newGenesisClient = newClientGoClient

// runGenesis handles the `gossipgrid genesis` subcommand. Flags:
//
//	--namespace <ns>      (default: the in-cluster namespace; required when
//	                         not running in-cluster)
//	--configmap <name>    (default: gossipgrid-cluster-metadata)
//
// The subcommand performs the operator-authorized grant. It exits 0 on success
// (a fresh grant) or when the grant is idempotent (an existing valid pending
// grant — "already granted"), and nonzero on any failure (an initialized
// cluster, a corrupted record, an ambiguous result, a permission denial, or an
// API unavailability). The grant is fail-closed: a command that cannot verify
// its preconditions (an unreadable API) fails rather than inferring safety.
func runGenesis(args []string) error {
	f := genesisFlags{
		namespace: "",
		configMap: genesis.ConfigMapName,
	}
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--namespace":
			if i+1 >= len(args) {
				return stderrors.New("genesis: --namespace requires a value")
			}
			i++
			f.namespace = args[i]
		case "--configmap":
			if i+1 >= len(args) {
				return stderrors.New("genesis: --configmap requires a value")
			}
			i++
			// The ConfigMap name is canonical (a single, cluster-wide record).
			// A different value has no effect (the grant always uses the
			// canonical name), so it is rejected rather than silently
			// accepted.
			if args[i] != genesis.ConfigMapName {
				return fmt.Errorf("genesis: --configmap must be %q (the canonical cluster-metadata ConfigMap name); got %q", genesis.ConfigMapName, args[i])
			}
			f.configMap = args[i]
		default:
			return fmt.Errorf("genesis: unexpected argument %q", args[i])
		}
	}

	// Build the client (in-cluster configuration). A client build failure
	// (no service account, no API server) is a fail-closed condition: the
	// grant cannot proceed without a working Kubernetes API. The factory
	// is a package-level variable so unit tests can inject a deterministic
	// fake.
	client, err := newGenesisClient()
	if err != nil {
		return fmt.Errorf("genesis: building Kubernetes client: %w", err)
	}

	// Resolve the namespace. When the operator does not supply --namespace,
	// the namespace is read from the in-cluster service account's mounted
	// namespace file (the genesis Job runs in-cluster in the application
	// namespace). A missing or empty namespace is a fail-closed condition
	// (the grant cannot create a ConfigMap without a namespace).
	ns := f.namespace
	if ns == "" {
		ns = inClusterNamespace()
	}
	if ns == "" {
		return stderrors.New("genesis: namespace is empty (the grant cannot create a ConfigMap without a namespace; supply --namespace or run in-cluster)")
	}

	op, err := genesis.NewGrantOperation(ns, client)
	if err != nil {
		return err
	}

	// Perform the grant. The context is the process's background context
	// (the grant is a one-shot operation; it completes promptly or fails
	// closed). A process cancellation (SIGTERM) is surfaced as a context
	// error and the grant fails closed.
	ctx := context.Background()
	res, gerr := op.Grant(ctx)
	// An existing valid pending grant is a successful idempotent outcome:
	// the one-time authorization is already in place. The Grant function
	// returns ErrAlreadyGranted as an error (it is inspectable), but the
	// CLI must treat it as success (exit 0) — the documented contract.
	if gerr != nil {
		if stderrors.Is(gerr, genesis.ErrAlreadyGranted) {
			fmt.Fprintf(os.Stderr,
				"genesis: cluster metadata already exists with a pending genesis authorization (already granted): cluster=%s epoch=%s generation=%d\n",
				res.Metadata.ClusterID, res.Metadata.EpochID, res.Metadata.Generation)
			fmt.Fprintln(os.Stderr, "genesis: the one-time authorization is in place; the seed may bind when it next reads the metadata.")
			return nil
		}
		// A failed or ambiguous grant: surface the error (the operator
		// investigates). An ambiguous failure may have occurred after the
		// ConfigMap was created; the grant never generated a replacement
		// identity, but the operator must inspect the authoritative
		// ConfigMap before retrying.
		return gerr
	}
	fmt.Fprintf(os.Stderr,
		"genesis: cluster metadata created: cluster=%s epoch=%s generation=%d\n",
		res.Metadata.ClusterID, res.Metadata.EpochID, res.Metadata.Generation)
	fmt.Fprintln(os.Stderr, "genesis: the one-time genesis authorization is in place (Initialized=false, GenesisAuthorized=true).")
	fmt.Fprintln(os.Stderr, "genesis: do NOT run this grant again; the record is immutable and the cluster identity is established.")
	return nil
}

// clientGoConfigMap is the production adapter over the official client-go
// ConfigMap client. It implements k8sClient and classifies API errors via
// k8s.io/apimachinery's typed StatusError (IsNotFound / IsForbidden /
// IsTimeout) — not message-text matching.
type clientGoConfigMap struct {
	clientset *kubernetes.Clientset
}

// newClientGoClient builds the production Kubernetes client using in-cluster
// configuration (the genesis Job runs in-cluster with the genesis
// ServiceAccount). A build failure (no service account, no API server, a
// malformed config) is returned as a non-nil error (fail-closed).
func newClientGoClient() (k8sClient, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building client-go clientset: %w", err)
	}
	return &clientGoConfigMap{clientset: clientset}, nil
}

// Get implements k8sClient. It reads the named ConfigMap and classifies the
// result: a missing object → k8smetadata.NewConfigMapNotFound; a permission
// denial → k8smetadata.NewAPIPermissionDenied; a transient unavailability
// (timeout or 5xx) → k8smetadata.NewAPIUnavailable; an unclassified error →
// k8smetadata.NewAPIError(APIClassUnknown, err).
func (c *clientGoConfigMap) Get(ctx context.Context, name, namespace string) (*k8smetadata.ConfigMap, error) {
	cm, err := c.clientset.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, classifyK8sError(err)
	}
	return &k8smetadata.ConfigMap{
		ResourceVersion: cm.ResourceVersion,
		Data:            cm.Data,
	}, nil
}

// Create implements k8sClient. It creates the named ConfigMap (Kubernetes
// create semantics: succeeds only when the object does not already exist; an
// existing object yields a 409 conflict error). The 409 is classified as
// APIClassUnknown (it is not a NotFound, Forbidden, or Timeout error) and is
// resolved by the grant's re-read path (resolveAfterCreateError), which
// re-reads the authoritative object and classifies it (already granted /
// initialized / corrupted). The exact class of the 409 is not load-bearing:
// the grant re-reads the authoritative object on any non-success.
func (c *clientGoConfigMap) Create(ctx context.Context, name, namespace string, data map[string]string) (*k8smetadata.ConfigMap, error) {
	// The ConfigMap is created with the given data map (the metadata
	// document at the "metadata" key). No labels, annotations, or other
	// metadata: the record is a single, cluster-wide document.
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Data: data,
	}
	created, err := c.clientset.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
	if err != nil {
		return nil, classifyK8sError(err)
	}
	return &k8smetadata.ConfigMap{
		ResourceVersion: created.ResourceVersion,
		Data:            created.Data,
	}, nil
}

// classifyK8sError maps a k8s.io/client-go API error to the k8smetadata typed
// error classification. It uses k8s.io/apimachinery's typed StatusError
// predicates (IsNotFound / IsConflict / IsForbidden / IsTimeout /
// IsServerTimeout) — not message-text matching. A 409 conflict is classified
// as APIClassUnknown (the grant re-reads the authoritative object on any
// non-success, so the exact class is not load-bearing). An unclassified error
// is conservatively treated as unavailability (APIClassUnknown, which the
// grant resolves as a re-read or a fail-closed ambiguous outcome).
func classifyK8sError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case apierrors.IsNotFound(err):
		return k8smetadata.NewConfigMapNotFound(err)
	case apierrors.IsConflict(err):
		// A 409 conflict (e.g., the object already exists during a Create).
		// Classified as APIClassUnknown: the grant's resolution logic re-reads
		// the authoritative object on any non-success, so the exact class is
		// not load-bearing. A 409 is NOT a NotFound (the object exists; it
		// just couldn't be created because it was already there).
		return k8smetadata.NewAPIError(k8smetadata.APIClassUnknown, err)
	case apierrors.IsForbidden(err):
		return k8smetadata.NewAPIPermissionDenied(err)
	case apierrors.IsTimeout(err):
		return k8smetadata.NewAPIUnavailable(err)
	case apierrors.IsServerTimeout(err):
		return k8smetadata.NewAPIUnavailable(err)
	default:
		// A 5xx or any other unclassified error: conservatively treated as
		// an unavailability (the grant re-reads or fails closed; it never
		// classifies by message text).
		if is5xxStatus(err) {
			return k8smetadata.NewAPIUnavailable(err)
		}
		return k8smetadata.NewAPIError(k8smetadata.APIClassUnknown, err)
	}
}

// is5xxStatus reports whether err is a k8s.io/apimachinery StatusError with a
// 5xx ReasonStatusCode or a 5xx HTTP code. A 5xx is a transient unavailability
// (the API server is degraded); the grant re-reads or fails closed.
func is5xxStatus(err error) bool {
	var status *apierrors.StatusError
	if !stderrors.As(err, &status) {
		return false
	}
	code := status.ErrStatus.Code
	return code >= 500 && code < 600
}

// inClusterNamespacePath is the in-cluster path where Kubernetes mounts the
// pod's namespace as plain text. It is a package-level variable (not a
// constant) so unit tests can override it with a temporary file, making the
// namespace detection independently testable without a live Kubernetes
// cluster.
var inClusterNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace" // #nosec G101: a fixed, well-known in-cluster path.

// inClusterNamespace reads the namespace from the in-cluster service
// account's mounted namespace file. Kubernetes populates this file as plain
// text (the pod's namespace, e.g. "gossipgrid"). It is the namespace the
// genesis Job runs in (the application namespace). When the file is missing,
// unreadable, or contains only whitespace, it returns "" (the caller treats
// that as a fail-closed condition: the grant cannot create a ConfigMap without
// a namespace).
func inClusterNamespace() string {
	b, err := os.ReadFile(inClusterNamespacePath) // #nosec G304: inClusterNamespacePath is a fixed, well-known in-cluster path.
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// _ is a compile-time assertion that clientGoConfigMap satisfies k8sClient.
var _ k8sClient = (*clientGoConfigMap)(nil)
