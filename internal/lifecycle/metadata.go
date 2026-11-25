// Package lifecycle: authoritative cluster metadata model.
//
// ClusterMetadata is the typed model for the future Kubernetes control-plane
// record (provisionally a ConfigMap). It carries the cluster identity, the
// current epoch identifier, and diagnostic information. It does NOT carry
// message bodies; it is a coordination and audit mechanism only.
//
// Semantics:
//   - ClusterID identifies the logical cluster across epochs. It is stable
//     for the lifetime of the cluster.
//   - EpochID distinguishes independent histories. A new epoch (e.g., after
//     a reset) gets a new EpochID. The EpochID is NOT a secret or bearer
//     authorization token — it is a correlation identifier that enables
//     fencing, not an authentication credential.
//   - Generation is a diagnostic, monotonically increasing display/ordering
//     number. It cannot authenticate an epoch.
//   - Initialized reports whether authorized genesis has occurred.
//   - EverAcceptedMessages reports whether messages are known to have been
//     accepted in the current epoch.
//   - Revision is the metadata version for future conditional updates
//     (Kubernetes resourceVersion-based compare-and-swap).
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ClusterMetadata is the authoritative cluster metadata observed from the
// control-plane record.
type ClusterMetadata struct {
	// ClusterID is the globally unique stable cluster identity. It is
	// invariant across epochs.
	ClusterID string

	// EpochID is the unpredictable, globally unique identifier for the
	// current epoch. It changes when a reset occurs.
	EpochID string

	// Generation is a positive, monotonically increasing display/ordering
	// generation. It is diagnostic only and cannot authenticate an epoch.
	Generation int

	// Initialized reports whether authorized genesis has occurred.
	Initialized bool

	// GenesisAuthorized reports whether the one-time authorization to
	// establish the first epoch has been granted and not yet consumed. It is
	// true only before genesis has been consumed, and false once consumed.
	// Future lifecycle code (5B/5C) inspects this to decide whether a
	// canonical seed may enter GENESIS; it is NOT an inference from an empty
	// local database.
	GenesisAuthorized bool

	// GenesisConsumer is the identity of the pod that consumed the one-time
	// genesis authorization (set on consumption). It is empty until the
	// authorization is consumed.
	GenesisConsumer string

	// EverAcceptedMessages reports whether messages are known to have been
	// accepted in the current epoch. It is a diagnostic/audit signal: it may
	// be a false positive after a crash, and that is the safe direction. It
	// is NOT the primary data-loss protection mechanism (that role is
	// played by DataLossProtected).
	EverAcceptedMessages bool

	// DataLossProtected is the conservative data-loss protection flag, armed
	// during genesis finalization (by the operator's finalization Job, in the
	// same CAS that sets GenesisAuthorized=false). When true, a replacement
	// pod with a fresh empty emptyDir cannot re-enter GENESIS (the
	// GenesisAuthorized token is consumed AND the protection is armed); public
	// writes are enabled. When false, the cluster is pre-finalization (a fresh,
	// un-granted cluster or a cluster whose genesis has not been finalized).
	// This is the safety mechanism that replaces the original first-write CAS
	// design (see the corrected design document).
	DataLossProtected bool

	// LastReset is the most recent reset incident, present only after a reset
	// has occurred. It is a diagnostic/audit record; a nil value means no
	// reset has occurred in the current epoch.
	LastReset *ResetIncident

	// Revision is the metadata version/revision for future conditional
	// updates (e.g., Kubernetes resourceVersion).
	Revision string
}

// ResetIncident is a diagnostic/audit record of a reset that produced the
// current epoch. It is present only after a reset has occurred.
type ResetIncident struct {
	// EpochID is the epoch that the reset established.
	EpochID string
	// Cause is the reset cause (e.g., "operator" for an operator-authorized
	// reset, or "catastrophic" for a confirmed catastrophic-loss reset).
	Cause string
	// Acknowledged reports whether the incident has been acknowledged by the
	// operator.
	Acknowledged bool
	// Timestamp is when the reset incident was recorded (RFC3339).
	Timestamp string
}

// MaxIDBytes bounds the byte-length of ClusterID, EpochID, and GenesisConsumer
// (the documented identifier length limit; it matches MaxEpochHeaderBytes).
const MaxIDBytes = 64

// DesignatedGenesisSeed is the deterministic seed-identity convention
// (Milestone 5B, Stage 1B): the canonical seed for a GossipGrid StatefulSet
// deployment is always the ordinal-0 pod, "gossipgrid-0" (the
// <statefulset-name>-0 pattern). It is the default and the only seed identity
// the genesis finalization will arm: the operator's finalization Job is
// validated against this constant (a finalization with a GenesisConsumer that
// differs from it fails closed before any ConfigMap update, so the
// data-loss protection is armed only for the designated seed). The seed is
// still EXPLICITLY operator-configured (lifecycle.seedIdentity /
// GOSSIPGRID_GENESIS_SEED_IDENTITY); this constant is the deterministic
// convention that the finalization enforces, not an additional authority
// mechanism. A non-StatefulSet deployment (or one that deliberately uses a
// different seed name) must set the seed identity to match the pod it designates
// and accept that the finalization will not arm data-loss protection for a
// consumer that differs from this convention.
const DesignatedGenesisSeed = "gossipgrid-0"

// Validate checks the metadata for well-formedness. It returns an error when
// any required field is empty or malformed, when an identifier exceeds the
// documented byte-length limit, or when the metadata state is incoherent
// (see the state-consistency rules below). A malformed observation must be
// rejected without changing the persisted local binding.
//
// Schema-v1 state combinations (each is explicitly validated):
//
//   - "Genesis authorized, not yet finalized": Initialized=false,
//     GenesisAuthorized=true, GenesisConsumer="". A cluster where the
//     operator's grant Job has run but the seed has not yet bound.
//   - "Initialized, protected cluster": Initialized=true,
//     GenesisAuthorized=false, GenesisConsumer nonempty,
//     DataLossProtected=true. The steady state after a successful
//     finalization (the data-loss protection flag is armed in the same
//     CAS that consumes the authorization).
//   - "Reset incident metadata": Initialized=true,
//     GenesisAuthorized=false, GenesisConsumer nonempty,
//     DataLossProtected=true, LastReset != nil. The steady state after a
//     confirmed 5C reset that established a new epoch and re-armed the
//     protection. (The LastReset's EpochID must match the current EpochID
//     — a reset establishes the current epoch.)
//
// Contradictory combinations are rejected (fail-closed; the authoritative
// record is corrupted or in an inconsistent in-flight state):
//
//   - DataLossProtected=true AND GenesisAuthorized=true: the protection
//     flag is armed only in the same CAS that consumes the authorization,
//     so the two can never both be true in a coherent document.
//   - GenesisConsumer nonempty AND GenesisAuthorized=true: the consumer is
//     set on consumption, which also sets GenesisAuthorized=false, so the
//     two can never both be true in a coherent document.
//   - GenesisConsumer nonempty AND Initialized=false: the consumer is set
//     on consumption, which also sets Initialized=true, so the two can
//     never contradict in a coherent document.
//   - GenesisConsumer nonempty AND DataLossProtected=false: the operator's
//     finalization CAS sets both the consumer and the data-loss protection
//     flag atomically, so an initialized-but-unprotected consumed-genesis
//     state is incoherent (a consumed authorization implies the protection
//     was armed in the same CAS).
//
// The 5A tests and the fake metadata sources are unaffected: they construct
// ClusterMetadata values that satisfy one of the three coherent states (or
// omit the new genesis fields entirely, in which case the combinations are
// vacuously satisfied).
func (m ClusterMetadata) Validate() error {
	if m.ClusterID == "" {
		return errors.New("cluster metadata: ClusterID is empty")
	}
	if m.EpochID == "" {
		return errors.New("cluster metadata: EpochID is empty")
	}
	if len([]byte(m.ClusterID)) > MaxIDBytes {
		return fmt.Errorf("cluster metadata: ClusterID exceeds %d bytes (got %d)", MaxIDBytes, len([]byte(m.ClusterID)))
	}
	if len([]byte(m.EpochID)) > MaxIDBytes {
		return fmt.Errorf("cluster metadata: EpochID exceeds %d bytes (got %d)", MaxIDBytes, len([]byte(m.EpochID)))
	}
	if len([]byte(m.GenesisConsumer)) > MaxIDBytes {
		return fmt.Errorf("cluster metadata: GenesisConsumer exceeds %d bytes (got %d)", MaxIDBytes, len([]byte(m.GenesisConsumer)))
	}
	if m.Generation < 1 {
		return fmt.Errorf("cluster metadata: Generation must be positive (got %d)", m.Generation)
	}

	// Coherent combinations (each is explicitly validated). The rules are
	// applied only when the new genesis fields are actually populated
	// (non-default); a metadata observation with zero-value genesis fields
	// (e.g., a 5A test fixture or a legacy observation) is not claimed to
	// be in any particular genesis state and is accepted.
	//
	// "Genesis authorized, not yet finalized": Initialized=false,
	// GenesisAuthorized=true, GenesisConsumer="".
	if m.Initialized && m.GenesisAuthorized {
		return errors.New("cluster metadata: Initialized and GenesisAuthorized are both true (incoherent; an initialized cluster has consumed the authorization, which sets GenesisAuthorized=false)")
	}
	// "Genesis authorization consumed" and "Initialized, protected cluster":
	// GenesisConsumer nonempty implies Initialized=true and
	// GenesisAuthorized=false (the consumer is recorded on consumption,
	// which also flips both flags).
	if m.GenesisConsumer != "" && !m.Initialized {
		return errors.New("cluster metadata: GenesisConsumer is set but Initialized is false (incoherent; the consumer is recorded on consumption, which also sets Initialized=true)")
	}
	if m.GenesisConsumer != "" && m.GenesisAuthorized {
		return errors.New("cluster metadata: GenesisConsumer is set and GenesisAuthorized is true (incoherent; the consumer is recorded on consumption, which also sets GenesisAuthorized=false)")
	}
	// "DataLossProtected=true and GenesisAuthorized=true": the protection
	// flag is armed in the same CAS that consumes the authorization, so the
	// two can never both be true in a coherent document. This rule does not
	// depend on the consumer being populated (a reset incident or a
	// finalization CAS could arm the protection and set GenesisAuthorized=false
	// while leaving the consumer empty — that is incoherent in another way,
	// but the protection/authorization pair is a distinct invariant).
	if m.DataLossProtected && m.GenesisAuthorized {
		return errors.New("cluster metadata: DataLossProtected and GenesisAuthorized are both true (incoherent; the protection flag is armed in the same CAS that consumes the authorization)")
	}
	// "GenesisConsumer nonempty AND DataLossProtected=false": the operator's
	// finalization CAS sets both the consumer and the data-loss protection
	// flag atomically, so an initialized-but-unprotected consumed-genesis
	// state is incoherent. A 5A fixture that leaves GenesisConsumer empty
	// is not affected (the rule only fires when the consumer is populated).
	if m.GenesisConsumer != "" && !m.DataLossProtected {
		return errors.New("cluster metadata: GenesisConsumer is set but DataLossProtected is false (incoherent; the operator's finalization CAS sets both atomically)")
	}
	// "Reset incident metadata": a lastReset incident is present only after
	// a 5C operator reset, which (a) establishes the current epoch
	// (lastReset.epochId must match the current EpochID) and (b) records
	// the consumer and arms the protection in the same CAS. A reset
	// incident with a mismatched epoch or on a non-protected cluster is
	// incoherent.
	if m.LastReset != nil {
		if m.LastReset.EpochID != m.EpochID {
			return fmt.Errorf("cluster metadata: lastReset.epochId %q does not match current EpochID %q (incoherent; a reset establishes the current epoch)", m.LastReset.EpochID, m.EpochID)
		}
		if !m.DataLossProtected {
			return errors.New("cluster metadata: lastReset is present but DataLossProtected is false (incoherent; a reset records the consumer and arms the protection in the same CAS)")
		}
	}
	return nil
}

// MatchesBinding reports whether this metadata is compatible with the given
// local database binding. It returns true only when both ClusterID and
// EpochID match exactly. Generation mismatch is NOT a mismatch for binding
// compatibility (Generation is diagnostic; the binding is keyed on
// ClusterID + EpochID).
func (m ClusterMetadata) MatchesBinding(binding *DatabaseBinding) bool {
	if binding == nil {
		return false
	}
	return m.ClusterID == binding.ClusterID && m.EpochID == binding.EpochID
}

// MetadataSource is the narrow, read-only interface for obtaining authoritative
// cluster metadata. The future Kubernetes implementation will implement this
// interface using resourceVersion-based conditional reads.
//
// For tests, a deterministic, concurrency-safe fake implementation is provided
// (see fake_metadata.go).
//
// No write/reset methods are part of this interface in Milestone 5A.
type MetadataSource interface {
	// Read returns the current authoritative cluster metadata. It returns
	// an error when the metadata is unavailable, stale, or malformed.
	// The context supports cancellation and timeouts for metadata access.
	Read(ctx context.Context) (ClusterMetadata, error)
}

// DatabaseBinding is the durable local binding between a SQLite database and
// a cluster epoch. It is stored in a separate table in the same SQLite file.
//
// The binding is the mechanism that prevents a database bound to epoch A from
// silently joining epoch B. It is set once (on first authorized initialization)
// and never automatically rewritten after an epoch change.
type DatabaseBinding struct {
	// ClusterID is the cluster identity this database belongs to.
	ClusterID string

	// EpochID is the epoch identity this database belongs to.
	EpochID string

	// Generation is the generation at the time the binding was created.
	// It is diagnostic; it is not used for compatibility checks.
	Generation int

	// CreatedAt is the time the binding was first established.
	CreatedAt time.Time
}

// Validate checks the binding for well-formedness.
func (b DatabaseBinding) Validate() error {
	if b.ClusterID == "" {
		return errors.New("database binding: ClusterID is empty")
	}
	if b.EpochID == "" {
		return errors.New("database binding: EpochID is empty")
	}
	return nil
}

// MatchesMetadata reports whether this binding is compatible with the given
// authoritative metadata. It returns true only when both ClusterID and EpochID
// match exactly.
func (b DatabaseBinding) MatchesMetadata(m ClusterMetadata) bool {
	return m.MatchesBinding(&b)
}

// ErrMetadataUnavailable is returned when the metadata source cannot provide
// a fresh, valid observation.
var ErrMetadataUnavailable = errors.New("cluster metadata unavailable")

// ErrMetadataMalformed is returned when the metadata source returns a
// malformed observation (failing ClusterMetadata.Validate).
var ErrMetadataMalformed = errors.New("cluster metadata malformed")
