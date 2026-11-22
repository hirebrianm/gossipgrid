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

	// EverAcceptedMessages reports whether messages are known to have been
	// accepted in the current epoch.
	EverAcceptedMessages bool

	// Revision is the metadata version/revision for future conditional
	// updates (e.g., Kubernetes resourceVersion).
	Revision string
}

// Validate checks the metadata for well-formedness. It returns an error
// when any required field is empty or malformed. A malformed observation
// must be rejected without changing the persisted local binding.
func (m ClusterMetadata) Validate() error {
	if m.ClusterID == "" {
		return errors.New("cluster metadata: ClusterID is empty")
	}
	if m.EpochID == "" {
		return errors.New("cluster metadata: EpochID is empty")
	}
	if m.Generation < 1 {
		return fmt.Errorf("cluster metadata: Generation must be positive (got %d)", m.Generation)
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
