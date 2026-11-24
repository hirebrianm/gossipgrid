// Package genesis: operator-authorized creation of GossipGrid's initial
// cluster metadata (Milestone 5B, Stage 1A).
//
// The grant operation is the explicit, operator-authorized establishment of
// the authoritative cluster identity. It creates a single, immutable ConfigMap
// record carrying the cluster identity, the first epoch, and the one-time
// genesis authorization (GenesisAuthorized=true, Initialized=false). No
// application pod can perform this write: the grant is performed by a separate,
// operator-triggered Kubernetes Job with a dedicated ServiceAccount (see the
// Helm chart and the operator runbook).
//
// The grant is NEVER automatic. Absence of the ConfigMap (an empty SQLite
// database, no reachable peers, a failed DNS lookup, a first application
// start, a Kubernetes/Argo CD deployment) is NOT evidence that creating a new
// cluster is safe. The only path to genesis is an explicit, operator-invoked
// grant that has verified, via an auditable preflight, that there is no
// existing authoritative epoch and no recoverable data. A command that cannot
// verify its preconditions fails closed rather than inferring safety.
//
// Concurrency and idempotency:
//
//   - Exactly one cluster identity wins the create race: the first Create to
//     succeed establishes the record; every other writer observes the existing
//     record (a 409 conflict is resolved by re-reading the authoritative object,
//     never by generating a replacement identity).
//   - A successful grant is idempotent: a retry against an existing valid
//     pending grant reports "already granted" (ErrAlreadyGranted) without
//     creating another record.
//   - An existing initialized cluster, or a corrupted/unsupported/malformed
//     record, fails closed (ErrClusterInitialized / ErrMetadataConflict) — a
//     replacement identity is never generated.
//   - An ambiguous failure may occur after the ConfigMap was created (e.g.,
//     the API server creates the object but times out before returning the
//     response). The grant never generates a replacement identity, but the
//     operator must inspect the authoritative ConfigMap metadata before
//     retrying. The ambiguous Create result is resolved by re-reading the
//     authoritative object; if the object is present and valid it is reported,
//     and if it is absent or unreadable the grant fails closed (no second
//     identity is generated).
//
// Security invariants (enforced by the grant logic and the RBAC; see the
// package documentation in the design document):
//
//  1. An empty SQLite database never authorizes genesis (the grant does not
//     read or trust the local database; it is a control-plane operation).
//  2. Only an explicitly invoked operator grant can create initial authority.
//  3. A successful grant creates exactly one immutable initial identity (the
//     Create is the only write; the record is never overwritten).
//  4. Ordinary application pods cannot create or mutate authoritative metadata
//     (the application ServiceAccount is read-only; the grant Job is separate).
//  5. An ambiguous failure may occur after the ConfigMap was created; the
//     grant never generates a replacement identity, but the operator must
//     inspect the authoritative ConfigMap metadata before retrying (an
//     ambiguous result is resolved by re-read, never by a second Create).
//  6. Public message writes remain unauthorized before finalization (the grant
//     sets Initialized=false; the 5A admission gate rejects writes while
//     Initialized=false).
//  7. A previously initialized cluster cannot be automatically reinitialized
//     (an existing initialized record is ErrClusterInitialized, not a grant).
//  8. Existing message data is never deleted or modified by the grant (the
//     grant writes only the metadata ConfigMap; it never touches the message
//     store).
//
// The package is deliberately small and dependency-free with respect to the
// Kubernetes client libraries: the ConfigMapWriter interface is the narrow,
// injectable adapter contract. A production adapter over the official
// client-go ConfigMap client is provided at the wiring site (the genesis Job
// entry point); unit tests supply a deterministic fake.
package genesis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/lifecycle/k8smetadata"
)

// ConfigMapName is the name of the authoritative cluster-metadata ConfigMap.
// It is the single, cluster-wide record holding the versioned metadata
// document. The grant creates it exactly once; it is never overwritten.
const ConfigMapName = "gossipgrid-cluster-metadata"

// ErrAlreadyGranted is returned by Grant when the authoritative metadata
// ConfigMap already exists and is a valid, pending genesis authorization
// (Initialized=false, GenesisAuthorized=true). This is the idempotent case:
// the one-time authorization has already been granted, and the retry must
// report that without generating a replacement identity. It is a benign,
// expected outcome (not a failure) — the operator can proceed as if the grant
// had just succeeded.
var ErrAlreadyGranted = errors.New("genesis: cluster metadata already exists with a pending genesis authorization (already granted)")

// ErrClusterInitialized is returned by Grant when the existing ConfigMap
// reports Initialized=true. A cluster that has already been initialized
// (a completed epoch exists) cannot be reinitialized by a grant; the
// operator must follow the recovery procedure (5C) if a reset is needed.
// A replacement identity is never generated.
var ErrClusterInitialized = errors.New("genesis: existing cluster metadata reports Initialized=true (a completed epoch exists; genesis is not permitted)")

// ErrMetadataConflict is returned by Grant when the existing ConfigMap is
// present but is corrupted, in an incoherent state, or has an unsupported
// schema version. The authoritative record is in an indeterminate or broken
// state; the grant fails closed rather than overwriting or replacing it.
// The operator must inspect and repair the record before retrying.
var ErrMetadataConflict = errors.New("genesis: existing cluster metadata is corrupted or in an incoherent state (fails closed; inspect and repair before retrying)")

// ErrCreateAmbiguous is returned by Grant when the Create call returned an
// error that is neither a clear "already exists" (409) nor a clear
// unavailability, and the subsequent re-read cannot establish a definitive
// state (the object is absent or unreadable). An ambiguous API response must
// never be resolved by blindly generating a replacement identity; the grant
// fails closed and the operator must verify the state.
var ErrCreateAmbiguous = errors.New("genesis: create result is ambiguous and the authoritative object could not be re-read (fails closed; no replacement identity generated)")

// GrantResult is the outcome of a grant attempt. It is a value type carrying
// the created or pre-existing metadata and a benign "already granted" flag.
type GrantResult struct {
	// Created reports whether this invocation actually created the ConfigMap
	// (true) or observed an existing valid pending grant (false).
	Created bool
	// AlreadyGranted reports whether the grant observed an existing, valid
	// pending genesis authorization (the idempotent "already granted" case).
	// It is true only when Created is false.
	AlreadyGranted bool
	// Metadata is the authoritative cluster metadata (the created record on a
	// fresh grant; the existing record on an already-granted or conflict
	// observation). On ErrAlreadyGranted this is the existing pending record.
	Metadata lifecycle.ClusterMetadata
}

// ConfigMapWriter is the narrow, injectable adapter contract for the genesis
// grant's ConfigMap operations. It is implemented by an adapter over the
// official client-go ConfigMap client (at the wiring site) and by a
// deterministic fake in unit tests.
//
// The returned error from Get must carry its API classification as a typed
// error (see k8smetadata.APIErrorClass and the convenience constructors), the
// same contract as k8smetadata.ConfigMapGetter. The grant classifies the error
// by its type, never by the error's message text.
//
// Create must implement Kubernetes create semantics: it succeeds only when
// the named object does not already exist, and returns a typed conflict
// (409) error when it does. A Create that returns a nil *ConfigMap with a nil
// error is treated as an ambiguous (unverifiable) result, not a success.
type ConfigMapWriter interface {
	// Get returns the named ConfigMap in the given namespace. On success it
	// returns the ConfigMap and a nil error. On failure it returns a typed
	// error (NewConfigMapNotFound / NewAPIUnavailable / NewAPIPermissionDenied
	// / NewAPIError) and a nil ConfigMap.
	Get(ctx context.Context, name, namespace string) (*k8smetadata.ConfigMap, error)
	// Create creates the named ConfigMap in the given namespace. It succeeds
	// only when the object does not already exist; an existing object yields
	// a typed conflict (409) error. On success it returns the created
	// ConfigMap (with its resourceVersion) and a nil error.
	Create(ctx context.Context, name, namespace string, data map[string]string) (*k8smetadata.ConfigMap, error)
}

// GrantOperation is the operator-authorized genesis grant. It is safe for
// concurrent use (each Grant invocation is independent; the create race is
// resolved by the Kubernetes API, not by in-process state).
type GrantOperation struct {
	namespace string
	writer    ConfigMapWriter
}

// NewGrantOperation creates a GrantOperation for the given namespace. The
// writer must be non-nil and the namespace must be nonempty; both are
// validated and a non-nil error is returned otherwise.
func NewGrantOperation(namespace string, writer ConfigMapWriter) (*GrantOperation, error) {
	if strings.TrimSpace(namespace) == "" {
		return nil, errors.New("genesis: namespace is empty")
	}
	if writer == nil {
		return nil, errors.New("genesis: configmap writer is nil")
	}
	return &GrantOperation{namespace: namespace, writer: writer}, nil
}

// Preflight verifies the preconditions that must hold before a grant is
// issued: there is no existing authoritative epoch and no recoverable data.
// It is the auditable, operator-visible check that distinguishes a
// deliberately authorized first installation from an existing cluster whose
// metadata was accidentally deleted.
//
// The preflight is fail-closed: a command that cannot verify its
// preconditions (the Kubernetes API is unavailable, the RBAC denies access,
// the context is canceled, or the existing record is corrupted or
// initialized) returns a non-nil error and the grant must NOT proceed. A
// missing ConfigMap (the object does not exist) with a readable, healthy API
// is the ONLY state in which the preflight succeeds and the grant is
// permitted to create the record.
//
// The preflight does NOT generate any identity and does NOT write any
// resource; it is a read-only observation. It returns a distinct, inspectable
// error for each failure class.
func (o *GrantOperation) Preflight(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	cm, err := o.writer.Get(ctx, ConfigMapName, o.namespace)
	if err != nil {
		// Classify the API error. A context cancellation takes priority (so
		// the operator can distinguish a canceled preflight from an API
		// failure). A permission-denied or unavailability failure means the
		// operator cannot verify the preconditions; fail closed. A not-found
		// result (the object does not exist) is the success condition.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		switch k8smetadata.ClassOf(err) {
		case k8smetadata.APIClassForbidden:
			return errors.Join(err, fmt.Errorf("genesis: preflight cannot verify (permission denied); refusing to grant: %w", k8smetadata.ErrMetadataPermissionDenied))
		case k8smetadata.APIClassNotFound:
			// The object does not exist and the API is readable: the only
			// state in which a grant is permitted. Preflight succeeds.
			return nil
		default:
			// An unavailability or unclassified error: the operator cannot
			// verify the preconditions. Fail closed (never infer safety).
			// k8smetadata.NewAPIUnavailable(err) carries the distinct
			// inspectable ErrAPIUnavailable marker and preserves the original
			// error, so the condition is inspectable via errors.As.
			return fmt.Errorf("genesis: preflight cannot verify (Kubernetes API unavailable or unclassified); refusing to grant: %w", k8smetadata.NewAPIUnavailable(err))
		}
	}
	// A nil ConfigMap response with no error is an invalid or ambiguous API
	// response. It must NOT be treated as a missing object (which would
	// authorize creation). Fail closed: the operator cannot verify the
	// preconditions from an unverifiable response.
	if cm == nil {
		return fmt.Errorf("genesis: preflight cannot verify (invalid API response: nil ConfigMap, nil error); refusing to grant: %w", k8smetadata.NewAPIUnavailable(errors.New("nil configmap response with nil error")))
	}

	// The object exists. Parse and inspect it: it must be a valid, pending
	// genesis authorization (Initialized=false, GenesisAuthorized=true) for
	// the preflight to allow a (no-op) grant. An initialized, corrupted, or
	// unsupported record is a preflight failure (fail closed).
	meta, perr := parseExistingMetadata(cm)
	if perr != nil {
		return perr
	}
	if meta.Initialized {
		return fmt.Errorf("genesis: preflight found an initialized cluster; refusing to grant: %w", ErrClusterInitialized)
	}
	if !meta.GenesisAuthorized {
		// Initialized=false but GenesisAuthorized=false: the record is not a
		// valid pending authorization. This is an incoherent or broken
		// state (a grant sets both Initialized=false and
		// GenesisAuthorized=true atomically). Fail closed.
		return fmt.Errorf("genesis: preflight found an incoherent record (initialized=%v genesisAuthorized=%v); refusing to grant: %w", meta.Initialized, meta.GenesisAuthorized, ErrMetadataConflict)
	}
	// A valid pending grant already exists: the preflight succeeds (a grant
	// would be a no-op "already granted").
	return nil
}

// Grant performs the operator-authorized genesis grant. It is the single,
// explicit write that establishes the authoritative cluster identity.
//
// The grant follows the fail-closed, idempotent, crash-safe protocol:
//
//  1. Verify the preconditions (Preflight). A failed preflight (initialized,
//     corrupted, unavailable, permission-denied, or canceled) returns the
//     preflight error and does NOT proceed to create.
//  2. Check for an existing valid pending grant. If the ConfigMap already
//     exists and is a valid, pending genesis authorization
//     (Initialized=false, GenesisAuthorized=true), return the existing
//     authoritative identity (ErrAlreadyGranted) without generating a new
//     identity pair or attempting a Create. This is the idempotent case:
//     the one-time authorization is already in place.
//  3. Generate the cluster identity (two distinct UUIDv7 identifiers) and
//     the schema-v1 pending metadata document (Initialized=false,
//     GenesisAuthorized=true, generation 1, no reset incident).
//  4. Validate the complete document before attempting creation (a document
//     that fails Validate is never submitted to the API).
//  5. Create the ConfigMap using Kubernetes create semantics.
//  6. Resolve the create result:
//     - success (a ConfigMap is returned): return Created=true with the
//     created metadata.
//     - a conflict (409, the object already exists): re-read the
//     authoritative object and classify it (already granted / initialized /
//     corrupted / unreadable). Never generate a replacement identity.
//     - a transient or unclassified error: re-read the authoritative object;
//     if it is present and valid report it (the create may have succeeded
//     before the error), otherwise fail closed (ErrCreateAmbiguous — no
//     second identity is generated).
//     - a permission-denied error: fail closed (the RBAC is misconfigured;
//     the grant cannot proceed).
//  7. Return the result.
//
// A successful grant is idempotent: a retry against an existing valid pending
// grant returns ErrAlreadyGranted (a benign, expected outcome) without
// creating another record or generating a new identity.
func (o *GrantOperation) Grant(ctx context.Context) (GrantResult, error) {
	if err := o.Preflight(ctx); err != nil {
		return GrantResult{}, err
	}

	// Check for an existing valid pending grant. If the ConfigMap already
	// exists and is a valid, pending genesis authorization, return the
	// existing authoritative identity without generating a new identity
	// pair or attempting a Create. This is the idempotent case.
	if existing, found, err := o.checkExistingPendingGrant(ctx); err != nil {
		return GrantResult{}, err
	} else if found {
		return GrantResult{AlreadyGranted: true, Metadata: existing}, ErrAlreadyGranted
	}

	clusterID, epochID, err := newIdentityPair()
	if err != nil {
		return GrantResult{}, fmt.Errorf("genesis: generating cluster identity: %w", err)
	}
	meta := lifecycle.ClusterMetadata{
		ClusterID:         clusterID,
		EpochID:           epochID,
		Generation:        1,
		Initialized:       false,
		GenesisAuthorized: true,
		GenesisConsumer:   "",
		DataLossProtected: false,
		// LastReset is nil (no reset incident on the first epoch).
	}
	// Validate the complete document before attempting creation. A document
	// that fails Validate (it should not, given newIdentityPair and the
	// fixed fields) is never submitted to the API.
	if err := meta.Validate(); err != nil {
		return GrantResult{}, fmt.Errorf("genesis: generated metadata failed validation (refusing to create): %w", err)
	}
	doc, err := marshalMetadataDocument(meta)
	if err != nil {
		return GrantResult{}, fmt.Errorf("genesis: marshaling metadata document: %w", err)
	}

	created, cerr := o.writer.Create(ctx, ConfigMapName, o.namespace, map[string]string{k8smetadata.MetadataKey: doc})
	if cerr == nil {
		// A success: a ConfigMap must be returned. A nil response with a nil
		// error is an ambiguous (unverifiable) result.
		if created == nil {
			return o.resolveAmbiguousCreate(ctx)
		}
		return GrantResult{Created: true, Metadata: meta}, nil
	}

	// The create returned an error. Classify and resolve.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return GrantResult{}, ctxErr
	}
	switch k8smetadata.ClassOf(cerr) {
	case k8smetadata.APIClassForbidden:
		// Permission denied: the RBAC is misconfigured (the Job lacks the
		// create permission). Fail closed; the grant cannot proceed.
		return GrantResult{}, errors.Join(cerr, fmt.Errorf("genesis: create failed (permission denied); the RBAC is misconfigured: %w", k8smetadata.ErrMetadataPermissionDenied))
	case k8smetadata.APIClassNotFound:
		// A not-found error from Create is contradictory (the object was
		// absent during preflight but Create reports not-found). Treat it as
		// an ambiguous result and re-read to establish the state. The
		// original create error is preserved in the resolution path.
		return o.resolveAfterCreateError(ctx, cerr)
	default:
		// A conflict (409), a transient unavailability, or an unclassified
		// error. Re-read the authoritative object to resolve the state: the
		// object may now exist (the create succeeded, or a concurrent writer
		// won the race), or it may not (a transient failure).
		return o.resolveAfterCreateError(ctx, cerr)
	}
}

// checkExistingPendingGrant reads the ConfigMap and reports whether it exists
// and is a valid, pending genesis authorization. It returns the parsed
// metadata (the existing authoritative identity), a found flag, and an error
// (a fail-closed condition: API unavailable, permission denied, or a
// non-pending existing record).
//
// This is the idempotency check: when found is true, the caller returns the
// existing identity without generating a new one or attempting a Create.
// When found is false (the object is absent), the caller proceeds to
// generate a new identity and create the record.
func (o *GrantOperation) checkExistingPendingGrant(ctx context.Context) (lifecycle.ClusterMetadata, bool, error) {
	cm, err := o.writer.Get(ctx, ConfigMapName, o.namespace)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return lifecycle.ClusterMetadata{}, false, ctxErr
		}
		switch k8smetadata.ClassOf(err) {
		case k8smetadata.APIClassNotFound:
			// The object is absent: proceed to create.
			return lifecycle.ClusterMetadata{}, false, nil
		case k8smetadata.APIClassForbidden:
			return lifecycle.ClusterMetadata{}, false, errors.Join(err, fmt.Errorf("genesis: cannot verify existing state (permission denied); refusing to grant: %w", k8smetadata.ErrMetadataPermissionDenied))
		default:
			// An unavailability or unclassified error: fail closed.
			return lifecycle.ClusterMetadata{}, false, fmt.Errorf("genesis: cannot verify existing state (Kubernetes API unavailable or unclassified); refusing to grant: %w", k8smetadata.NewAPIUnavailable(err))
		}
	}
	// Task 4: a nil ConfigMap response with no error is an invalid or
	// ambiguous API response. It must NOT be treated as "absent" (which
	// would authorize creation). Fail closed.
	if cm == nil {
		return lifecycle.ClusterMetadata{}, false, fmt.Errorf("genesis: invalid API response (nil ConfigMap, nil error); refusing to grant: %w", k8smetadata.NewAPIUnavailable(errors.New("nil configmap response with nil error")))
	}
	// The object exists. Parse and inspect it.
	meta, perr := parseExistingMetadata(cm)
	if perr != nil {
		// A corrupted, incoherent, or unsupported record: fail closed.
		return lifecycle.ClusterMetadata{}, false, perr
	}
	if meta.Initialized {
		return lifecycle.ClusterMetadata{}, false, fmt.Errorf("genesis: existing record reports Initialized=true; refusing to grant: %w", ErrClusterInitialized)
	}
	if !meta.GenesisAuthorized {
		return lifecycle.ClusterMetadata{}, false, fmt.Errorf("genesis: existing record is not a valid pending genesis authorization (initialized=%v genesisAuthorized=%v); refusing to grant: %w", meta.Initialized, meta.GenesisAuthorized, ErrMetadataConflict)
	}
	// A valid, pending genesis authorization already exists.
	return meta, true, nil
}

// resolveAfterCreateError re-reads the authoritative object after a create
// error that is not a clear permission denial. It classifies the re-read
// result and returns the appropriate outcome: an existing valid pending grant
// (ErrAlreadyGranted), an initialized cluster (ErrClusterInitialized), a
// corrupted record (ErrMetadataConflict), or an ambiguous failure
// (ErrCreateAmbiguous). It never generates a replacement identity.
func (o *GrantOperation) resolveAfterCreateError(ctx context.Context, createErr error) (GrantResult, error) {
	cm, err := o.writer.Get(ctx, ConfigMapName, o.namespace)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return GrantResult{}, ctxErr
		}
		switch k8smetadata.ClassOf(err) {
		case k8smetadata.APIClassForbidden:
			return GrantResult{}, errors.Join(createErr, err, fmt.Errorf("genesis: re-read after create failed (permission denied); the RBAC is misconfigured: %w", k8smetadata.ErrMetadataPermissionDenied))
		case k8smetadata.APIClassNotFound:
			// The object is absent even after a create error. This is
			// ambiguous (a transient failure, not a confirmed state): fail
			// closed. The operator must retry or verify the state.
			return GrantResult{}, errors.Join(createErr, err, fmt.Errorf("genesis: re-read after create error returned not-found; the create result is ambiguous: %w", ErrCreateAmbiguous))
		default:
			return GrantResult{}, errors.Join(createErr, err, fmt.Errorf("genesis: re-read after create error failed (unclassified); the create result is ambiguous: %w", ErrCreateAmbiguous))
		}
	}
	if cm == nil {
		// A nil response with no error: ambiguous (the state cannot be
		// established). Fail closed.
		return GrantResult{}, errors.Join(createErr, fmt.Errorf("genesis: re-read after create error returned nil ConfigMap; the create result is ambiguous: %w", ErrCreateAmbiguous))
	}
	// The object exists. Classify it.
	return o.classifyExisting(cm, createErr)
}

// resolveAmbiguousCreate re-reads the authoritative object after an ambiguous
// create result (a nil response with a nil error, or a not-found create
// error). It is the same resolution as resolveAfterCreateError but the create
// error is nil (the ambiguity is in the result, not the error).
func (o *GrantOperation) resolveAmbiguousCreate(ctx context.Context) (GrantResult, error) {
	return o.resolveAfterCreateError(ctx, nil)
}

// classifyExisting classifies an existing ConfigMap into one of the grant
// outcomes: an existing valid pending grant (ErrAlreadyGranted), an
// initialized cluster (ErrClusterInitialized), or a corrupted record
// (ErrMetadataConflict). The original create error (if any) is preserved in
// the returned error so the operator can see the full diagnostic chain.
func (o *GrantOperation) classifyExisting(cm *k8smetadata.ConfigMap, createErr error) (GrantResult, error) {
	meta, perr := parseExistingMetadata(cm)
	if perr != nil {
		if createErr != nil {
			perr = errors.Join(createErr, perr)
		}
		return GrantResult{}, perr
	}
	if meta.Initialized {
		if createErr != nil {
			return GrantResult{Metadata: meta}, errors.Join(createErr, fmt.Errorf("genesis: existing record reports Initialized=true; refusing to grant: %w", ErrClusterInitialized))
		}
		return GrantResult{Metadata: meta}, fmt.Errorf("genesis: existing record reports Initialized=true; refusing to grant: %w", ErrClusterInitialized)
	}
	if !meta.GenesisAuthorized {
		if createErr != nil {
			return GrantResult{Metadata: meta}, errors.Join(createErr, fmt.Errorf("genesis: existing record is not a valid pending genesis authorization (initialized=%v genesisAuthorized=%v); refusing to grant: %w", meta.Initialized, meta.GenesisAuthorized, ErrMetadataConflict))
		}
		return GrantResult{Metadata: meta}, fmt.Errorf("genesis: existing record is not a valid pending genesis authorization (initialized=%v genesisAuthorized=%v); refusing to grant: %w", meta.Initialized, meta.GenesisAuthorized, ErrMetadataConflict)
	}
	// A valid, pending genesis authorization already exists: the idempotent
	// "already granted" outcome (benign, expected).
	if createErr != nil {
		return GrantResult{AlreadyGranted: true, Metadata: meta}, errors.Join(createErr, ErrAlreadyGranted)
	}
	return GrantResult{AlreadyGranted: true, Metadata: meta}, ErrAlreadyGranted
}

// parseExistingMetadata parses and validates the metadata document stored in
// the given ConfigMap. It returns a distinct error for each failure class:
// ErrMetadataConflict (corrupted / incoherent / unsupported), using the
// k8smetadata package's typed error sentinels. A missing or blank document
// field is ErrMetadataConflict (a present object with a broken document is
// corrupted, not absent).
func parseExistingMetadata(cm *k8smetadata.ConfigMap) (lifecycle.ClusterMetadata, error) {
	raw, ok := cm.Data[k8smetadata.MetadataKey]
	if !ok || strings.TrimSpace(raw) == "" {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: %w", ErrMetadataConflict, k8smetadata.ErrMetadataMissingDocument)
	}
	var doc metadataDoc
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("genesis: metadata document is not valid JSON: %w", errors.Join(ErrMetadataConflict, err))
	}
	meta, perr := doc.toMetadata()
	if perr != nil {
		return lifecycle.ClusterMetadata{}, perr
	}
	if vErr := meta.Validate(); vErr != nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("genesis: metadata document failed validation: %w", errors.Join(ErrMetadataConflict, vErr))
	}
	return meta, nil
}

// metadataDoc is the strict, schema-v1 JSON document the grant writes and
// parses. Required fields use pointer types so a missing field is
// distinguishable from a zero value (the same discipline as the k8smetadata
// package). The grant always writes every required field, so a parse of a
// well-formed record is unambiguous.
type metadataDoc struct {
	SchemaVersion     *int    `json:"schemaVersion"`
	ClusterID         *string `json:"clusterId"`
	EpochID           *string `json:"epochId"`
	Generation        *int    `json:"generation"`
	Initialized       *bool   `json:"initialized"`
	GenesisAuthorized *bool   `json:"genesisAuthorized"`
	GenesisConsumer   *string `json:"genesisConsumer"`
	EverAccepted      *bool   `json:"everAcceptedMessages"`
	DataLossProtected *bool   `json:"dataLossProtected"`
	LastReset         *reset  `json:"lastReset,omitempty"`
}

// reset is the optional lastReset incident in the metadata document.
type reset struct {
	EpochID      *string `json:"epochId"`
	Cause        *string `json:"cause"`
	Acknowledged *bool   `json:"acknowledged"`
	Timestamp    *string `json:"timestamp"`
}

// toMetadata converts the document to a lifecycle.ClusterMetadata, enforcing
// the presence and type of every required field. A missing or mistyped field
// yields ErrMetadataConflict (the record is corrupted or in an unsupported
// state). A schemaVersion other than 1 yields ErrMetadataConflict as well
// (an out-of-date or future record is not a valid pending grant; the grant
// must fail closed rather than interpret a version it does not understand).
func (d metadataDoc) toMetadata() (lifecycle.ClusterMetadata, error) {
	if d.SchemaVersion == nil || *d.SchemaVersion != 1 {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: unsupported or missing schemaVersion (got %v)", ErrMetadataConflict, d.SchemaVersion)
	}
	if d.ClusterID == nil || *d.ClusterID == "" {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: clusterId missing or empty", ErrMetadataConflict)
	}
	if d.EpochID == nil || *d.EpochID == "" {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: epochId missing or empty", ErrMetadataConflict)
	}
	if d.Generation == nil || *d.Generation < 1 {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: generation missing or not positive", ErrMetadataConflict)
	}
	if d.Initialized == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: initialized missing", ErrMetadataConflict)
	}
	if d.GenesisAuthorized == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: genesisAuthorized missing", ErrMetadataConflict)
	}
	if d.GenesisConsumer == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: genesisConsumer missing", ErrMetadataConflict)
	}
	if d.EverAccepted == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: everAcceptedMessages missing", ErrMetadataConflict)
	}
	if d.DataLossProtected == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: dataLossProtected missing", ErrMetadataConflict)
	}
	m := lifecycle.ClusterMetadata{
		ClusterID:            *d.ClusterID,
		EpochID:              *d.EpochID,
		Generation:           *d.Generation,
		Initialized:          *d.Initialized,
		GenesisAuthorized:    *d.GenesisAuthorized,
		GenesisConsumer:      *d.GenesisConsumer,
		EverAcceptedMessages: *d.EverAccepted,
		DataLossProtected:    *d.DataLossProtected,
	}
	if d.LastReset != nil {
		if d.LastReset.EpochID == nil || d.LastReset.Cause == nil || d.LastReset.Acknowledged == nil || d.LastReset.Timestamp == nil {
			return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: lastReset present with missing subfields", ErrMetadataConflict)
		}
		m.LastReset = &lifecycle.ResetIncident{
			EpochID:      *d.LastReset.EpochID,
			Cause:        *d.LastReset.Cause,
			Acknowledged: *d.LastReset.Acknowledged,
			Timestamp:    *d.LastReset.Timestamp,
		}
	}
	return m, nil
}

// marshalMetadataDocument serializes the given metadata into the schema-v1
// JSON document stored at the ConfigMap's data key. The grant always writes
// every required field (including the explicit "everAcceptedMessages": false
// and "dataLossProtected": false), so the document is unambiguous and
// round-trips through parseExistingMetadata.
func marshalMetadataDocument(m lifecycle.ClusterMetadata) (string, error) {
	sv := 1
	doc := metadataDoc{
		SchemaVersion:     &sv,
		ClusterID:         &m.ClusterID,
		EpochID:           &m.EpochID,
		Generation:        &m.Generation,
		Initialized:       &m.Initialized,
		GenesisAuthorized: &m.GenesisAuthorized,
		GenesisConsumer:   &m.GenesisConsumer,
		EverAccepted:      &m.EverAcceptedMessages,
		DataLossProtected: &m.DataLossProtected,
	}
	if m.LastReset != nil {
		e, c, a, ts := m.LastReset.EpochID, m.LastReset.Cause, m.LastReset.Acknowledged, m.LastReset.Timestamp
		doc.LastReset = &reset{EpochID: &e, Cause: &c, Acknowledged: &a, Timestamp: &ts}
	}
	b, err := json.Marshal(doc)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// newIdentityPair generates two distinct, cryptographically secure UUIDv7
// identifiers: one for the ClusterID (the stable cluster identity) and one
// for the EpochID (the first epoch). UUIDv7 uses a 48-bit millisecond
// timestamp and 80 bits of randomness drawn from crypto/rand, so it is
// consistent with the project's documented UUIDv7 convention (see
// internal/store, which generates message IDs the same way).
//
// The two identifiers are guaranteed distinct: the 80-bit random component
// makes a collision a 2^-80 event; the check below is a belt-and-suspenders
// guard (a collision would be a bug, not a realistic outcome, but it is
// trivial to verify and the cost is negligible).
func newIdentityPair() (clusterID, epochID string, err error) {
	c, cerr := uuid.NewV7()
	if cerr != nil {
		return "", "", cerr
	}
	e, eerr := uuid.NewV7()
	if eerr != nil {
		return "", "", eerr
	}
	if c == e {
		// A collision is a 2^-80 event; treat it as a failure (it would
		// indicate a broken RNG, which is a security concern).
		return "", "", errors.New("genesis: generated ClusterID and EpochID are identical (RNG failure; refusing to create)")
	}
	return c.String(), e.String(), nil
}
