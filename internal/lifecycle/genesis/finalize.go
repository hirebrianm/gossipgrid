// Package genesis: operator-authorized finalization of the one-time genesis
// authorization (Milestone 5B, Stage 1B).
//
// The finalization operation is the explicit, operator-authorized
// consumption of the one-time genesis authorization. It performs a single
// atomic Kubernetes resourceVersion compare-and-swap (CAS) on the
// authoritative cluster-metadata ConfigMap that sets, all together in one
// update:
//
//   - Initialized=true
//   - GenesisAuthorized=false
//   - GenesisConsumer=<the designated seed identity>
//   - DataLossProtected=true
//
// and preserves every other field of the authoritative metadata document
// (ClusterID, EpochID, Generation, EverAcceptedMessages, LastReset, and the
// schema version). It does NOT create a new cluster identity or epoch, and
// it does NOT delete or recreate the ConfigMap.
//
// The finalization is NEVER automatic. It is performed by a separate,
// operator-triggered Kubernetes Job with a dedicated ServiceAccount (the
// finalization RBAC; see the Helm chart and docs/genesis-runbook.md). The
// Job runs the `gossipgrid genesis finalize` subcommand.
//
// Crash safety and idempotency:
//
//   - A successful finalization is idempotent: a retry against an
//     already-finalized record (Initialized=true, GenesisAuthorized=false,
//     GenesisConsumer=<expected>, DataLossProtected=true, with the exact
//     expected ClusterID/EpochID) reports ErrAlreadyFinalized (a benign,
//     expected outcome — the authorization is already consumed). No new
//     record is created and no new identity is generated.
//   - A mismatched or corrupted finalized record (e.g. Initialized=true but
//     GenesisConsumer is a different identity, or DataLossProtected=false,
//     or the ClusterID/EpochID differ) fails closed (ErrMetadataConflict /
//     ErrFinalizationMismatch). It is never treated as a success.
//   - A CAS conflict (another writer advanced the resourceVersion between the
//     Job's Get and its Update) is resolved by re-reading the authoritative
//     object and re-checking the preconditions: if the record is now
//     finalized with the expected identities, the retry reports
//     ErrAlreadyFinalized; if the preconditions no longer hold (the
//     authorization was revoked or the epoch changed), it fails closed.
//   - An ambiguous API failure (the API server may have performed the
//     update but the response was lost) is NEVER resolved by inferring
//     success: the Job re-reads the authoritative object and classifies it.
//     Only a confirmed, coherent finalized state is reported as an
//     idempotent success.
//
// Security invariants (enforced by the finalization logic and the RBAC):
//
//  1. Only an explicitly invoked operator finalization Job can consume the
//     one-time authorization (the application ServiceAccount is read-only;
//     the seed cannot update the ConfigMap).
//  2. The finalization sets all four fields atomically in a single CAS
//     (Initialized=true, GenesisAuthorized=false, GenesisConsumer=<seed>,
//     DataLossProtected=true); the data-loss protection is armed in the same
//     CAS that consumes the authorization, so there is no
//     "initialized-but-unprotected" or "consumer-set-but-authorization-still-
//     true" window.
//  3. No new ClusterID or EpochID is generated; the existing identities are
//     preserved exactly.
//  4. A mismatched or corrupted record fails closed (it is never repaired
//     by overwriting, and no replacement identity is generated).
//  5. The ConfigMap is never deleted or recreated.
//  6. The operator must externally verify that the seed is durably bound to
//     the exact cluster/epoch (via the peer-status and lifecycle
//     diagnostics) before invoking finalization; the command verifies the
//     authoritative metadata itself and requires an explicit
//     --confirm-seed-bound flag as the operator's attestation. It does NOT
//     and CANNOT read the seed's local SQLite (the seed's emptyDir is not
//     accessible to the Job, and the Job's RBAC is scoped only to the
//     ConfigMap).
package genesis

import (
	"context"
	"errors"
	"fmt"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/lifecycle/k8smetadata"
)

// ConfigMapUpdater is the narrow, injectable adapter contract for the
// finalization's ConfigMap operations. It is implemented by an adapter over
// the official client-go ConfigMap client (at the wiring site) and by a
// deterministic fake in unit tests.
//
// The returned error from Get must carry its API classification as a typed
// error (see k8smetadata.APIErrorClass and the convenience constructors).
// The returned error from Update must carry its API classification as a
// typed error; a 409 conflict (a CAS whose resourceVersion no longer
// matches) must be classified as k8smetadata.APIClassConflict
// (k8smetadata.NewAPIConflict). The finalization classifies the error by its
// type, never by the error's message text.
type ConfigMapUpdater interface {
	// Get returns the named ConfigMap in the given namespace. On success it
	// returns the ConfigMap (with its resourceVersion and data) and a nil
	// error. On failure it returns a typed error and a nil ConfigMap.
	Get(ctx context.Context, name, namespace string) (*k8smetadata.ConfigMap, error)
	// Update performs a resourceVersion compare-and-swap on the named
	// ConfigMap in the given namespace. It succeeds only when the current
	// resourceVersion of the object equals expectedResourceVersion (the
	// revision read earlier by Get) and the data map is written; an
	// advanced resourceVersion (another writer) yields a typed conflict
	// (409) error. On success it returns the updated ConfigMap (with its
	// new resourceVersion) and a nil error.
	Update(ctx context.Context, name, namespace, expectedResourceVersion string, data map[string]string) (*k8smetadata.ConfigMap, error)
}

// ErrAlreadyFinalized is returned by Finalize when the authoritative
// metadata ConfigMap is already a coherent, finalized record that matches the
// expected identities (Initialized=true, GenesisAuthorized=false,
// GenesisConsumer=<expected seed>, DataLossProtected=true, and the exact
// expected ClusterID and EpochID). This is the idempotent case: the one-time
// authorization is already consumed, and the retry must report that without
// performing another update or generating a replacement identity. It is a
// benign, expected outcome (not a failure).
var ErrAlreadyFinalized = errors.New("genesis: cluster metadata is already finalized with the expected identities (already finalized; the one-time authorization is consumed)")

// ErrNoOutstandingAuthorization is returned by Finalize when the authoritative
// metadata does not report an outstanding, unconsumed genesis authorization
// (i.e. it is neither Initialized=false with GenesisAuthorized=true, nor an
// already-finalized record matching the expected identities). A cluster whose
// authorization was never granted, or that is in an incoherent intermediate
// state, cannot be finalized; the operator must inspect the record.
var ErrNoOutstandingAuthorization = errors.New("genesis: no outstanding genesis authorization to finalize (the record is not Initialized=false/GenesisAuthorized=true nor a coherent finalized record matching the expected identities)")

// ErrFinalizationMismatch is returned by Finalize when the authoritative
// metadata is a finalized record (Initialized=true) but it does NOT match the
// expected identities (a different ClusterID or EpochID, or a different
// GenesisConsumer, or DataLossProtected=false). A mismatched finalized record
// is incoherent with the expected identity; the finalization fails closed
// rather than overwriting it. The operator must inspect and repair the
// record (or follow the 5C recovery procedure) before retrying.
var ErrFinalizationMismatch = errors.New("genesis: existing finalized record does not match the expected cluster/epoch/consumer identities (fails closed; inspect and repair before retrying)")

// ErrUpdateAmbiguous is returned by Finalize when the Update call returned an
// error that is neither a clear conflict (409) nor a clear unavailability,
// and the subsequent re-read cannot establish a definitive state (the object
// is absent or unreadable, or is not finalized). An ambiguous API result must
// never be resolved by inferring a successful finalization; the finalization
// fails closed and the operator must verify the authoritative record.
var ErrUpdateAmbiguous = errors.New("genesis: update result is ambiguous and the authoritative object could not be confirmed as a coherent finalized record (fails closed; no success inferred; verify the ConfigMap)")

// ErrIdentityMismatch is returned by Finalize when, for a record that is not
// already finalized, the operator-supplied identity context does not match the
// authoritative record (a different ClusterID or EpochID), or when the
// designated seed identity is not the pending grant's intended consumer. This
// is the pre-CAS identity verification (Stage 1B correction 1): the operator's
// flags are checked against the PENDING record before any finalization update
// is attempted, so a mismatch fails closed (no Update) rather than writing a
// consumer field that contradicts the record's own identity. It is distinct
// from ErrFinalizationMismatch (which applies to an already-finalized record
// observed on the idempotent / CAS-conflict re-read path).
var ErrIdentityMismatch = errors.New("genesis: operator-supplied identity (cluster/epoch/consumer) does not match the authoritative pending record (fails closed before the CAS; verify the --cluster-id, --epoch-id, and --genesis-consumer flags against the grant")

// FinalizeResult is the outcome of a finalization attempt. It is a value type
// carrying the authoritative metadata (the updated or pre-existing record)
// and a benign "already finalized" flag.
type FinalizeResult struct {
	// Finalized reports whether this invocation actually performed the CAS
	// (true) or observed an existing coherent finalized record (false).
	Finalized bool
	// AlreadyFinalized reports whether the finalization observed an existing
	// coherent finalized record matching the expected identities (the
	// idempotent "already finalized" case). It is true only when Finalized
	// is false.
	AlreadyFinalized bool
	// Metadata is the authoritative cluster metadata (the updated record on a
	// fresh finalization; the existing record on an already-finalized
	// observation).
	Metadata lifecycle.ClusterMetadata
}

// FinalizeConfig holds the expected identity context the operator must supply
// for a finalization. The command verifies these against the authoritative
// ConfigMap before performing the CAS.
type FinalizeConfig struct {
	// ClusterID is the expected, authoritative cluster identity (from the
	// grant). It must match the ConfigMap's ClusterID exactly.
	ClusterID string
	// EpochID is the expected, authoritative epoch identity (from the
	// grant). It must match the ConfigMap's EpochID exactly.
	EpochID string
	// GenesisConsumer is the expected seed identity that consumed (or will
	// consume) the one-time authorization. It must match the ConfigMap's
	// GenesisConsumer on an already-finalized record; on a fresh finalization
	// it is the value written into the record.
	GenesisConsumer string
	// ConfirmSeedBound is the operator's explicit attestation that the
	// designated seed is durably bound to the exact cluster/epoch (verified
	// externally via the peer-status and lifecycle diagnostics). The
	// finalization CANNOT verify the seed's local SQLite (the seed's emptyDir
	// is not accessible to the Job); this flag is the explicit operator gate
	// that closes the "operator finalized before the seed actually bound"
	// case. It must be true for the finalization to proceed.
	ConfirmSeedBound bool
}

// Validate checks the finalization configuration for well-formedness. A
// finalization cannot proceed without the expected identity context; an
// empty ClusterID, EpochID, or GenesisConsumer is a fail-closed condition.
//
// Designated-seed policy (Stage 1B correction 4): the operator-supplied
// GenesisConsumer must equal the deterministic seed-identity convention
// (lifecycle.DesignatedGenesisSeed, "gossipgrid-0" — the ordinal-0 pod of the
// StatefulSet). The architecture designates gossipgrid-0 as the seed; a
// finalization with a different consumer must not arm the data-loss
// protection, so it fails closed BEFORE any ConfigMap update (0 Update calls;
// the record is unchanged). This is the "enforce the actual designated-seed
// policy before a finalization Update" correction: the pending-grant document
// does NOT carry a seed-identity field (a pending record has
// GenesisConsumer=""), so the finalization does not claim the pending
// ConfigMap authenticates the seed identity — it enforces the deterministic
// convention. A non-StatefulSet deployment that deliberately uses a different
// seed name must set its seed identity to match the pod it designates and
// accept that the finalization will not arm data-loss protection for a
// consumer that differs from this convention (the seed identity is
// operator-configured; this check is the deterministic policy, not an
// additional authority mechanism).
func (c FinalizeConfig) Validate() error {
	if c.ClusterID == "" {
		return errors.New("genesis: finalization ClusterID is empty (the expected cluster identity is required)")
	}
	if c.EpochID == "" {
		return errors.New("genesis: finalization EpochID is empty (the expected epoch identity is required)")
	}
	if c.GenesisConsumer == "" {
		return errors.New("genesis: finalization GenesisConsumer is empty (the designated seed identity is required)")
	}
	if c.GenesisConsumer != lifecycle.DesignatedGenesisSeed {
		return fmt.Errorf("genesis: finalization GenesisConsumer %q is not the designated seed %q (the deterministic seed-identity convention; the data-loss protection is armed only for the designated seed); the finalization fails closed before any ConfigMap update (verify the --genesis-consumer flag and the lifecycle.seedIdentity override against the StatefulSet ordinal-0 pod)", c.GenesisConsumer, lifecycle.DesignatedGenesisSeed)
	}
	return nil
}

// FinalizeOperation is the operator-authorized genesis finalization. It is
// safe for concurrent use (each Finalize invocation is independent; the CAS
// race is resolved by the Kubernetes API, not by in-process state).
type FinalizeOperation struct {
	namespace string
	updater   ConfigMapUpdater
}

// NewFinalizeOperation creates a FinalizeOperation for the given namespace.
// The updater must be non-nil and the namespace must be nonempty; both are
// validated and a non-nil error is returned otherwise.
func NewFinalizeOperation(namespace string, updater ConfigMapUpdater) (*FinalizeOperation, error) {
	if namespace == "" {
		return nil, errors.New("genesis: namespace is empty")
	}
	if updater == nil {
		return nil, errors.New("genesis: configmap updater is nil")
	}
	return &FinalizeOperation{namespace: namespace, updater: updater}, nil
}

// Finalize performs the operator-authorized genesis finalization. It is the
// single, explicit write that consumes the one-time genesis authorization
// and arms the data-loss protection.
//
// The finalization follows the fail-closed, idempotent, crash-safe protocol:
//
//  1. Read the authoritative ConfigMap (Get). A read failure (unavailable,
//     permission denied, or canceled) returns the error; the finalization
//     cannot proceed without a working, readable API.
//  2. Parse and validate the metadata document. A corrupted, incoherent, or
//     unsupported record fails closed (ErrMetadataConflict).
//  3. Classify the current state:
//     - "Already finalized, expected identities" (Initialized=true,
//     GenesisAuthorized=false, GenesisConsumer=<expected>,
//     DataLossProtected=true, and the exact expected ClusterID/EpochID):
//     return ErrAlreadyFinalized (the idempotent case). No update is
//     attempted.
//     - "Mismatched finalized record" (Initialized=true but the
//     ClusterID/EpochID/GenesisConsumer/DataLossProtected do not match the
//     expected identities): fail closed (ErrFinalizationMismatch). The
//     record is never overwritten.
//     - "Outstanding, unconsumed authorization" (Initialized=false,
//     GenesisAuthorized=true, GenesisConsumer=""): verify the operator's
//     attestation and proceed to the CAS.
//     - Any other state (no outstanding authorization, an incoherent
//     intermediate state): fail closed (ErrNoOutstandingAuthorization).
//  4. Construct the finalized metadata document: the same ClusterID, EpochID,
//     Generation, EverAcceptedMessages, and LastReset as the current record,
//     with Initialized=true, GenesisAuthorized=false,
//     GenesisConsumer=<expected>, and DataLossProtected=true. Validate the
//     complete document before submitting it (a document that fails
//     Validate is never submitted).
//  5. Perform the atomic CAS Update with the ConfigMap's current
//     resourceVersion (the compare-and-swap revision).
//  6. Resolve the update result:
//     - success (an updated ConfigMap is returned): return Finalized=true
//     with the finalized metadata.
//     - a conflict (409, another writer advanced the resourceVersion):
//     re-read the authoritative object and classify it (already finalized /
//     mismatched / outstanding / corrupted). Never generate a replacement
//     identity and never infer success from the conflict alone.
//     - a transient or unclassified error: re-read the authoritative object;
//     if it is a coherent finalized record matching the expected
//     identities, report ErrAlreadyFinalized (the update may have
//     succeeded before the error); otherwise fail closed (ErrUpdateAmbiguous).
//     - a permission-denied error: fail closed (the RBAC is misconfigured;
//     the Job lacks the update permission).
//  7. Return the result.
//
// A successful finalization is idempotent: a retry against an existing
// coherent finalized record matching the expected identities returns
// ErrAlreadyFinalized (a benign, expected outcome) without performing another
// update or generating a new identity. A mismatched or corrupted record fails
// closed.
func (o *FinalizeOperation) Finalize(ctx context.Context, cfg FinalizeConfig) (FinalizeResult, error) {
	if err := cfg.Validate(); err != nil {
		return FinalizeResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return FinalizeResult{}, err
	}

	cm, err := o.updater.Get(ctx, ConfigMapName, o.namespace)
	if err != nil {
		return FinalizeResult{}, classifyFinalizeAPIError(ctx, err)
	}
	if cm == nil {
		// A nil response with no error is an invalid or ambiguous API
		// response. It must NOT be treated as an absent object (which would
		// infer a state). Fail closed.
		return FinalizeResult{}, fmt.Errorf("genesis: invalid API response (nil ConfigMap, nil error); cannot finalize: %w", k8smetadata.NewAPIUnavailable(errors.New("nil configmap response with nil error")))
	}

	meta, perr := parseExistingMetadata(cm)
	if perr != nil {
		return FinalizeResult{}, perr
	}

	// Classify the current state.
	switch {
	case meta.Initialized:
		// The record is finalized. It must match the expected identities
		// (the idempotent case); a mismatch fails closed.
		return o.classifyFinalized(meta, cfg)
	case meta.GenesisAuthorized && !meta.Initialized && meta.GenesisConsumer == "":
		// An outstanding, unconsumed authorization: verify the operator's
		// attestation and the identity context against the PENDING record,
		// then proceed to the CAS.
		if !cfg.ConfirmSeedBound {
			return FinalizeResult{}, fmt.Errorf("genesis: operator did not confirm the seed is durably bound (--confirm-seed-bound is required); the finalization cannot verify the seed's local binding itself (the seed's emptyDir is not accessible to the Job); verify via the peer-status and lifecycle diagnostics first: %w", ErrNoOutstandingAuthorization)
		}
		// Pre-CAS identity verification (Stage 1B correction 1): the
		// operator-supplied ClusterID and EpochID must equal the record's
		// own ClusterID and EpochID. The record is a valid pending
		// authorization (GenesisConsumer is empty; a valid document cannot
		// carry a nonempty consumer here), so there is no consumer field to
		// match — the operator's --genesis-consumer is the designated seed
		// identity that will be WRITTEN by the CAS (it is the grant's
		// intended consumer), and the explicit --confirm-seed-bound
		// attestation above is the operator's confirmation that the seed is
		// bound to exactly this cluster/epoch. A mismatched ClusterID or
		// EpochID means the operator is finalizing the wrong record: fail
		// closed before any Update.
		if meta.ClusterID != cfg.ClusterID || meta.EpochID != cfg.EpochID {
			return FinalizeResult{}, fmt.Errorf("genesis: the operator-supplied identity (cluster=%s, epoch=%s) does not match the authoritative pending record (cluster=%s, epoch=%s); failing closed before the CAS (verify the --cluster-id and --epoch-id flags against the grant): %w", cfg.ClusterID, cfg.EpochID, meta.ClusterID, meta.EpochID, ErrIdentityMismatch)
		}
		return o.performFinalizationCAS(ctx, cm, meta, cfg)
	default:
		// No outstanding authorization (and not a finalized record): the
		// record is in a state that cannot be finalized (e.g. the
		// authorization was never granted, or it is in an incoherent
		// intermediate state). Fail closed.
		return FinalizeResult{}, fmt.Errorf("genesis: the record is not in a finalizable state (initialized=%v genesisAuthorized=%v genesisConsumer=%q); refusing to finalize: %w", meta.Initialized, meta.GenesisAuthorized, meta.GenesisConsumer, ErrNoOutstandingAuthorization)
	}
}

// classifyFinalized classifies an already-finalized record (Initialized=true)
// against the expected identities. It returns ErrAlreadyFinalized (the
// idempotent case) when the record is coherent and matches the expected
// ClusterID, EpochID, GenesisConsumer, and DataLossProtected; it returns
// ErrFinalizationMismatch (fail closed) when the record is mismatched.
func (o *FinalizeOperation) classifyFinalized(meta lifecycle.ClusterMetadata, cfg FinalizeConfig) (FinalizeResult, error) {
	if meta.ClusterID != cfg.ClusterID || meta.EpochID != cfg.EpochID {
		return FinalizeResult{}, fmt.Errorf("genesis: existing finalized record (cluster=%s, epoch=%s) does not match the expected identities (cluster=%s, epoch=%s); failing closed: %w", meta.ClusterID, meta.EpochID, cfg.ClusterID, cfg.EpochID, ErrFinalizationMismatch)
	}
	if meta.GenesisConsumer != cfg.GenesisConsumer {
		return FinalizeResult{}, fmt.Errorf("genesis: existing finalized record (genesisConsumer=%s) does not match the expected seed identity (%s); failing closed: %w", meta.GenesisConsumer, cfg.GenesisConsumer, ErrFinalizationMismatch)
	}
	if !meta.DataLossProtected {
		return FinalizeResult{}, fmt.Errorf("genesis: existing finalized record has DataLossProtected=false (the protection flag was not armed in the finalization CAS); failing closed: %w", ErrFinalizationMismatch)
	}
	if meta.GenesisAuthorized {
		// A coherent finalized record has GenesisAuthorized=false (the
		// authorization is consumed in the same CAS that sets Initialized).
		// GenesisAuthorized=true with Initialized=true is incoherent (a
		// Validate() rejection), so this is a corrupted record.
		return FinalizeResult{}, fmt.Errorf("genesis: existing finalized record has GenesisAuthorized=true (incoherent; the authorization is consumed in the same CAS that sets Initialized); failing closed: %w", ErrFinalizationMismatch)
	}
	// A coherent, finalized record matching the expected identities: the
	// idempotent "already finalized" outcome (benign, expected).
	return FinalizeResult{AlreadyFinalized: true, Metadata: meta}, ErrAlreadyFinalized
}

// performFinalizationCAS constructs the finalized metadata document and
// performs the atomic resourceVersion compare-and-swap Update. It resolves
// the update result (success, conflict, unavailability, or permission
// denial) by re-reading the authoritative object on any non-success.
func (o *FinalizeOperation) performFinalizationCAS(ctx context.Context, cm *k8smetadata.ConfigMap, meta lifecycle.ClusterMetadata, cfg FinalizeConfig) (FinalizeResult, error) {
	// Construct the finalized metadata document: preserve every other field
	// (ClusterID, EpochID, Generation, EverAcceptedMessages, LastReset) and
	// set the four finalization fields atomically.
	finalized := meta
	finalized.Initialized = true
	finalized.GenesisAuthorized = false
	finalized.GenesisConsumer = cfg.GenesisConsumer
	finalized.DataLossProtected = true
	// Validate the complete document before attempting the update. A
	// document that fails Validate (it should not, given the preserved
	// fields and the fixed finalization fields) is never submitted to the
	// API.
	if err := finalized.Validate(); err != nil {
		return FinalizeResult{}, fmt.Errorf("genesis: finalized metadata failed validation (refusing to update): %w", err)
	}
	doc, err := marshalMetadataDocument(finalized)
	if err != nil {
		return FinalizeResult{}, fmt.Errorf("genesis: marshaling finalized metadata document: %w", err)
	}

	updated, uerr := o.updater.Update(ctx, ConfigMapName, o.namespace, cm.ResourceVersion, map[string]string{k8smetadata.MetadataKey: doc})
	if uerr == nil {
		// A success: an updated ConfigMap must be returned. A nil response
		// with a nil error is an ambiguous (unverifiable) result.
		if updated == nil {
			return o.resolveAmbiguousUpdate(ctx, cfg)
		}
		return FinalizeResult{Finalized: true, Metadata: finalized}, nil
	}

	// The update returned an error. Classify and resolve.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return FinalizeResult{}, ctxErr
	}
	switch k8smetadata.ClassOf(uerr) {
	case k8smetadata.APIClassForbidden:
		// Permission denied: the RBAC is misconfigured (the Job lacks the
		// update permission). Fail closed; the finalization cannot proceed.
		return FinalizeResult{}, errors.Join(uerr, fmt.Errorf("genesis: update failed (permission denied); the RBAC is misconfigured: %w", k8smetadata.ErrMetadataPermissionDenied))
	default:
		// A conflict (409, another writer advanced the resourceVersion), a
		// not-found error, a transient unavailability, or an unclassified
		// error. Re-read the authoritative object to resolve the state: the
		// object may now be finalized (a concurrent finalization won the
		// race, or the update succeeded before the error), or it may be in a
		// different state.
		return o.resolveAfterUpdateError(ctx, uerr, cfg)
	}
}

// resolveAfterUpdateError re-reads the authoritative object after an update
// error that is not a clear permission denial. It classifies the re-read
// result and returns the appropriate outcome: an existing coherent finalized
// record matching the expected identities (ErrAlreadyFinalized), a mismatched
// finalized record (ErrFinalizationMismatch), a corrupted record
// (ErrMetadataConflict), or an ambiguous failure (ErrUpdateAmbiguous). It
// never generates a replacement identity and never infers success from the
// error alone.
func (o *FinalizeOperation) resolveAfterUpdateError(ctx context.Context, updateErr error, cfg FinalizeConfig) (FinalizeResult, error) {
	cm, err := o.updater.Get(ctx, ConfigMapName, o.namespace)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return FinalizeResult{}, ctxErr
		}
		switch k8smetadata.ClassOf(err) {
		case k8smetadata.APIClassForbidden:
			return FinalizeResult{}, errors.Join(updateErr, err, fmt.Errorf("genesis: re-read after update failed (permission denied); the RBAC is misconfigured: %w", k8smetadata.ErrMetadataPermissionDenied))
		default:
			// A not-found error, a transient unavailability, or an
			// unclassified error: the state cannot be established. Fail
			// closed (an ambiguous outcome).
			return FinalizeResult{}, errors.Join(updateErr, err, fmt.Errorf("genesis: re-read after update error failed (class=%v); the update result is ambiguous: %w", k8smetadata.ClassOf(err), ErrUpdateAmbiguous))
		}
	}
	if cm == nil {
		// A nil response with no error: ambiguous (the state cannot be
		// established). Fail closed.
		return FinalizeResult{}, errors.Join(updateErr, fmt.Errorf("genesis: re-read after update error returned nil ConfigMap; the update result is ambiguous: %w", ErrUpdateAmbiguous))
	}
	// The object exists. Classify it.
	meta, perr := parseExistingMetadata(cm)
	if perr != nil {
		if updateErr != nil {
			return FinalizeResult{}, errors.Join(updateErr, perr)
		}
		return FinalizeResult{}, perr
	}
	if meta.Initialized {
		// The record is finalized. Classify it against the expected
		// identities (idempotent success or a mismatch).
		return o.classifyFinalized(meta, cfg)
	}
	// The record is not finalized (e.g. a concurrent writer reset it, or the
	// authorization was revoked). This is an ambiguous outcome: the update
	// did not result in a coherent finalized record. Fail closed; the
	// operator must inspect the record.
	return FinalizeResult{}, errors.Join(updateErr, fmt.Errorf("genesis: re-read after update error shows the record is not finalized (initialized=%v genesisAuthorized=%v); the update result is ambiguous: %w", meta.Initialized, meta.GenesisAuthorized, ErrUpdateAmbiguous))
}

// resolveAmbiguousUpdate re-reads the authoritative object after an ambiguous
// update result (a nil response with a nil error). It is the same resolution
// as resolveAfterUpdateError but the update error is nil (the ambiguity is in
// the result, not the error).
func (o *FinalizeOperation) resolveAmbiguousUpdate(ctx context.Context, cfg FinalizeConfig) (FinalizeResult, error) {
	return o.resolveAfterUpdateError(ctx, nil, cfg)
}

// classifyFinalizeAPIError maps a ConfigMapUpdater.Get error to the
// finalization's error classification. It is a pure function (no shared
// state). The classification is based on the error's TYPED shape
// (k8smetadata.APIErrorClass), never on the error's message text:
//   - a context cancellation/timeout error → the context error
//   - a permission-denied error (403)       → k8smetadata.ErrMetadataPermissionDenied
//   - a not-found error                    → lifecycle.ErrMetadataUnavailable
//   - an unavailable or unclassified error  → lifecycle.ErrMetadataUnavailable
func classifyFinalizeAPIError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	switch k8smetadata.ClassOf(err) {
	case k8smetadata.APIClassForbidden:
		return fmt.Errorf("%w: %w", k8smetadata.ErrMetadataPermissionDenied, err)
	case k8smetadata.APIClassNotFound:
		return fmt.Errorf("%w: %w", lifecycle.ErrMetadataUnavailable, k8smetadata.NewConfigMapNotFound(err))
	default:
		return fmt.Errorf("%w: %w", lifecycle.ErrMetadataUnavailable, k8smetadata.NewAPIUnavailable(err))
	}
}
