// Package store: durable local database binding to a cluster epoch.
//
// The DatabaseBinding model is stored in a separate table (local_metadata)
// in the same SQLite file. It is a singleton row: the entire database is
// bound to exactly one cluster and one epoch.
//
// Rules:
//   - The binding is atomic and idempotent for identical values.
//   - A database already bound to epoch A cannot silently bind to epoch B.
//   - A database bound to another ClusterID must be rejected.
//   - A nonempty legacy database without an epoch binding must not silently
//     acquire one.
//   - An empty unbound database may be bound only after authoritative
//     initialization has been established by the lifecycle controller.
//   - Restarting with the same database and same authoritative epoch
//     preserves the binding.
//   - Restarting with a conflicting epoch fails closed.
//   - Malformed local metadata fails closed.
//   - Missing metadata on a previously initialized database is not proof
//     of a fresh installation.
//   - Never delete messages to resolve a binding mismatch.
//   - Never rewrite the binding automatically after an epoch change.
//   - Concurrent initialization races are prevented by the SQLite write
//     lock (single writer).
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gossipgrid/internal/lifecycle"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// localMetadata is the GORM model for the singleton local database binding
// row.
type localMetadata struct {
	// ID is always 1 (singleton row).
	ID int `gorm:"primaryKey" json:"id"`
	// ClusterID is the cluster identity this database belongs to.
	ClusterID string `gorm:"type:text;not null" json:"cluster_id"`
	// EpochID is the epoch identity this database belongs to.
	EpochID string `gorm:"type:text;not null" json:"epoch_id"`
	// Generation is the generation at binding creation time (diagnostic).
	Generation int `gorm:"type:integer;not null;default:0" json:"generation"`
	// CreatedAt is when the binding was first established.
	CreatedAt time.Time `json:"created_at"`
	// Provenance records the origin of the binding (0=genesis, 1=join,
	// 2=legacy). It was added in Stage 3A. Pre-existing rows created before
	// the column existed have a zero value, which is interpreted as
	// ProvenanceGenesis at read time (see the migration policy in
	// GetBinding): the only production binding-creation path before Stage
	// 3A was the operator-authorized genesis entry, so a zero-value
	// provenance is, on a supported database, a genesis-created binding.
	Provenance int `gorm:"type:integer;not null;default:0" json:"provenance"`
}

// Table returns the GORM table name.
func (localMetadata) TableName() string {
	return "local_metadata"
}

// ErrBindingMismatch is returned when a Bind call is made with a different
// ClusterID or EpochID than the existing binding.
var ErrBindingMismatch = errors.New("database binding mismatch: database is already bound to a different cluster or epoch")

// ErrBindingNonempty is returned when a Bind call is made on a nonempty
// database that has no existing binding. A nonempty legacy database must
// not silently acquire a binding.
var ErrBindingNonempty = errors.New("cannot bind nonempty database without an existing binding")

// ErrBindingMalformed is returned when the persisted binding row is
// malformed (e.g., empty ClusterID or EpochID).
var ErrBindingMalformed = errors.New("database binding is malformed")

// GetBinding returns the current database binding, or nil when no binding
// exists. It returns an error when the binding row is malformed.
//
// A nil binding with no error means the database has no binding row
// (either a fresh database or a legacy database from before Milestone 5A).
//
// Provenance migration policy (Stage 3A): a supported pre-Stage-3A
// database has a local_metadata row whose provenance column is absent;
// AutoMigrate adds the column with a zero default, so the existing row
// reads back with a zero-value provenance. The zero value is mapped to
// ProvenanceGenesis at read time. This mapping is accepted on the
// strength of a historical invariant: the ONLY production binding-creation
// path before Stage 3A was AuthorizeGenesis (the operator-authorized
// genesis entry for the designated seed). There was no other
// binding-creation path, so a zero-value provenance on a supported
// database can only belong to a genesis-created binding.
//
// The mapping is applied ONLY to the zero value. A row carrying an
// explicit, non-zero provenance (1=join, 2=legacy, or an invalid value)
// is read as-is: an explicitly unknown (legacy) provenance is NOT
// re-mapped to genesis and fails closed (it authorizes neither genesis
// entry nor ordinary joining), and an invalid value is malformed
// (ErrBindingMalformed). No migration rewrites existing rows; it only
// adds the column, so existing binding fields and records are preserved
// unchanged. This migration is tested against a real SQLite database
// (TestProvenanceMigration_RealSQLite).
func (s *Store) GetBinding() (*lifecycle.DatabaseBinding, error) {
	var row localMetadata
	err := s.db.Where("id = ?", 1).First(&row).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil // no binding row
		}
		return nil, fmt.Errorf("reading database binding: %w", err)
	}

	// Validate the row.
	if row.ClusterID == "" || row.EpochID == "" {
		return nil, ErrBindingMalformed
	}

	provenance := lifecycle.BindingProvenance(row.Provenance)
	switch provenance {
	case lifecycle.ProvenanceGenesis, lifecycle.ProvenanceJoin, lifecycle.ProvenanceLegacy:
		// Valid provenance values. The zero value (ProvenanceGenesis) is
		// also reachable on a migrated pre-Stage-3A row (see the migration
		// policy above) and is read as genesis per that policy.
	default:
		return nil, ErrBindingMalformed
	}

	return &lifecycle.DatabaseBinding{
		ClusterID:  row.ClusterID,
		EpochID:    row.EpochID,
		Generation: row.Generation,
		CreatedAt:  row.CreatedAt,
		Provenance: provenance,
	}, nil
}

// Bind atomically and idempotently binds the database to the given cluster
// and epoch with the given provenance.
//
// The operation is context-aware: the caller's context is checked before any
// database access and passed to the GORM transaction and every statement
// (s.db.WithContext(ctx)). A cancellation or timeout aborts the transaction
// without a partial commit. A context cancellation cannot, however, be
// claimed to roll back a binding that has ALREADY committed: the transaction
// is the linearization point and once COMMIT has succeeded the durable row
// exists. The store's SQLite busy-timeout (5s, set at open) is a best-effort
// contention-resolution limit, not a linearizability guarantee.
//
// Semantics:
//   - If no binding exists and the database is empty: the binding is
//     created (with the specified provenance) and returned.
//   - If no binding exists and the database is nonempty:
//     ErrBindingNonempty is returned (a nonempty legacy database must not
//     silently acquire a binding).
//   - If a binding exists and the values match: the existing binding is
//     returned unchanged (idempotent no-op; the existing provenance is
//     preserved — a join binding is never relabeled as genesis).
//   - If a binding exists and the values differ: ErrBindingMismatch is
//     returned (the existing binding is NOT overwritten).
//
// The entire validate + count + insert sequence is performed inside a single
// SQLite transaction, so it is genuinely atomic: the "is the database
// nonempty?" check and the binding insert cannot be interleaved with another
// operation. With the store's single-writer pool (MaxOpenConns(1)), a
// concurrent Bind is serialized behind the transaction; the first writer
// commits the binding and the rest observe it. A concurrent caller that
// requested a DIFFERENT epoch reads the winner back and returns
// ErrBindingMismatch rather than a successful result containing the
// competing epoch (the previous implementation returned GetBinding() on a
// lost INSERT without verifying the winner matched the caller's request,
// which could let a different-epoch caller receive a successful binding for
// the wrong epoch).
//
// A malformed persisted binding row (empty ClusterID or EpochID, or an
// invalid provenance value) is rejected with ErrBindingMalformed. A negative
// generation is rejected as invalid.
func (s *Store) Bind(ctx context.Context, clusterID, epochID string, generation int, provenance lifecycle.BindingProvenance) (*lifecycle.DatabaseBinding, error) {
	if clusterID == "" || epochID == "" {
		return nil, ErrBindingMalformed
	}
	if generation < 0 {
		return nil, fmt.Errorf("invalid binding generation: %d (must be non-negative)", generation)
	}
	// Provenance policy (Stage 3A): only genesis and join provenance may be
	// created through normal Bind calls. An explicit unknown/legacy
	// provenance is a fail-closed marker that is readable for diagnosis but
	// is never created through this path: it may only exist as a
	// pre-existing row inserted through raw SQLite (a test fixture for the
	// unknown-provenance policy) or as a historical migration artifact
	// (which is mapped to genesis at read time, not stored as legacy).
	switch provenance {
	case lifecycle.ProvenanceGenesis, lifecycle.ProvenanceJoin:
		// Valid creation provenance.
	case lifecycle.ProvenanceLegacy:
		return nil, fmt.Errorf("invalid binding provenance: %s (explicitly-unknown/legacy provenance may not be created through normal Bind calls; it is a fail-closed marker for pre-existing rows only)", provenance)
	default:
		return nil, fmt.Errorf("invalid binding provenance: %d (out-of-range; malformed binding, fail closed)", provenance)
	}
	// Context-aware: check cancellation before any database access.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	var result *lifecycle.DatabaseBinding
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Check if a binding already exists (within the transaction, so the
		// observation is consistent with the insert).
		var row localMetadata
		txErr := tx.Where("id = ?", 1).First(&row).Error
		if txErr != nil {
			if errors.Is(txErr, gorm.ErrRecordNotFound) {
				// No binding row. Check if the database is nonempty.
				var count int64
				if cErr := tx.Model(&Message{}).Count(&count).Error; cErr != nil {
					return fmt.Errorf("counting messages: %w", cErr)
				}
				if count > 0 {
					return ErrBindingNonempty
				}
				// Empty database, no binding: create the binding with
				// the specified provenance.
				newRow := localMetadata{
					ID:         1,
					ClusterID:  clusterID,
					EpochID:    epochID,
					Generation: generation,
					CreatedAt:  time.Now().UTC(),
					Provenance: int(provenance),
				}
				res := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "id"}},
					DoNothing: true,
				}).Create(&newRow)
				if res.Error != nil {
					return fmt.Errorf("creating database binding: %w", res.Error)
				}
				if res.RowsAffected == 0 {
					// A concurrent writer created the binding first (the
					// single-writer pool serializes us behind it, so this is
					// only reachable in the same transaction window). Read
					// the winner back and verify it matches.
					return s.readAndVerifyBinding(tx, clusterID, epochID, &result)
				}
				result = &lifecycle.DatabaseBinding{
					ClusterID:  clusterID,
					EpochID:    epochID,
					Generation: generation,
					CreatedAt:  newRow.CreatedAt,
					Provenance: provenance,
				}
				return nil
			}
			return fmt.Errorf("reading database binding: %w", txErr)
		}
		// A binding row exists. Validate and compare.
		if row.ClusterID == "" || row.EpochID == "" {
			return ErrBindingMalformed
		}
		rowProvenance := lifecycle.BindingProvenance(row.Provenance)
		switch rowProvenance {
		case lifecycle.ProvenanceGenesis, lifecycle.ProvenanceJoin, lifecycle.ProvenanceLegacy:
			// Valid.
		default:
			return ErrBindingMalformed
		}
		if row.ClusterID != clusterID || row.EpochID != epochID {
			return ErrBindingMismatch
		}
		// Idempotent: return the existing binding unchanged (the provenance
		// is immutable through ordinary bind operations; a join binding is
		// never relabeled as genesis by a subsequent bind call).
		result = &lifecycle.DatabaseBinding{
			ClusterID:  row.ClusterID,
			EpochID:    row.EpochID,
			Generation: row.Generation,
			CreatedAt:  row.CreatedAt,
			Provenance: rowProvenance,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		// Defensive: the transaction succeeded but no binding was recorded
		// (should be unreachable). Fail closed rather than returning a nil
		// binding with a nil error, which could be misread as "no binding".
		return nil, fmt.Errorf("binding transaction succeeded but no binding result was recorded")
	}
	return result, nil
}

// readAndVerifyBinding reads the existing binding row from the transaction and
// verifies it matches the requested cluster/epoch. If it matches, it is
// returned as a successful idempotent result; if it differs, ErrBindingMismatch
// is returned so a different-epoch caller never receives a successful result
// containing the competing epoch. A malformed row (including an invalid
// provenance value) yields ErrBindingMalformed.
func (s *Store) readAndVerifyBinding(tx *gorm.DB, clusterID, epochID string, result **lifecycle.DatabaseBinding) error {
	var row localMetadata
	if err := tx.Where("id = ?", 1).First(&row).Error; err != nil {
		return fmt.Errorf("re-reading database binding: %w", err)
	}
	if row.ClusterID == "" || row.EpochID == "" {
		return ErrBindingMalformed
	}
	rowProvenance := lifecycle.BindingProvenance(row.Provenance)
	switch rowProvenance {
	case lifecycle.ProvenanceGenesis, lifecycle.ProvenanceJoin, lifecycle.ProvenanceLegacy:
		// Valid.
	default:
		return ErrBindingMalformed
	}
	if row.ClusterID != clusterID || row.EpochID != epochID {
		return ErrBindingMismatch
	}
	*result = &lifecycle.DatabaseBinding{
		ClusterID:  row.ClusterID,
		EpochID:    row.EpochID,
		Generation: row.Generation,
		CreatedAt:  row.CreatedAt,
		Provenance: rowProvenance,
	}
	return nil
}

// CountMessages returns the total number of messages in the local store.
func (s *Store) CountMessages() (int64, error) {
	var count int64
	if err := s.db.Model(&Message{}).Count(&count).Error; err != nil {
		return 0, fmt.Errorf("counting messages: %w", err)
	}
	return count, nil
}
