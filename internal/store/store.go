// Package store provides local message persistence using GORM and SQLite.
//
// Each application instance owns an independent SQLite database. The store
// exposes two operations: CreateLocalMessage (generate identity + persist) and
// InsertReplicatedMessage (preserve existing identity + persist idempotently).
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// maxMessageBodyBytes bounds the stored message body. It is the wire-size
// bound for the body field in sync (anti-entropy) responses: the body is a
// single JSON string, so its serialized length is the stored length plus JSON
// escaping overhead.
const maxMessageBodyBytes = 4096

// maxMessageOriginBytes bounds the stored message origin.
//
// The origin is a variable-size field (set from the local node ID at
// creation) with no prior length limit, so it is now bounded to a finite
// value that mirrors the DNS name length cap (253) the node identity already
// obeys. It is part of the sync response size contract: a batch of
// maximum-size records (4096-byte body + 253-byte origin) must serialize to
// JSON within the sync client's response bound, and this bound is what makes
// that worst case finite and provable. The value is shared with the peer
// sync client (peertls.maxMessageOriginBytes), which enforces the same bound
// when validating sync responses.
const maxMessageOriginBytes = 253

var (
	// ErrNilMessage is returned when a nil message pointer is submitted.
	ErrNilMessage = errors.New("message must not be nil")

	// ErrInvalidID is returned when a message ID is not a valid UUIDv7 in
	// canonical lowercase-hyphenated form.
	ErrInvalidID = errors.New("message ID must be a valid canonical UUIDv7 (lowercase, hyphenated)")

	// ErrEmptyOrigin is returned when a message origin is empty.
	ErrEmptyOrigin = errors.New("message origin must not be empty")

	// ErrMissingTimestamp is returned when a message creation timestamp is zero.
	ErrMissingTimestamp = errors.New("message creation timestamp must be set")

	// ErrEmptyBody is returned when a message body is empty.
	ErrEmptyBody = errors.New("message body must not be empty")

	// ErrBodyTooLarge is returned when a message body exceeds 4096 bytes.
	ErrBodyTooLarge = errors.New("message body exceeds 4096 bytes")

	// ErrOriginTooLarge is returned when a message origin exceeds the
	// maximum origin length (253 bytes).
	ErrOriginTooLarge = errors.New("message origin exceeds 253 bytes")

	// ErrDuplicateIDConflict is returned when a duplicate ID is inserted with
	// conflicting content (different body, origin, or timestamp).
	ErrDuplicateIDConflict = errors.New("message ID already exists with conflicting content")

	// ErrNotFound is returned when a message ID does not exist.
	ErrNotFound = gorm.ErrRecordNotFound
)

// Store holds the database handle for local message persistence.
type Store struct {
	db     *gorm.DB
	dbPath string // absolute path to the SQLite database file (for WAL file size checks)
}

// NewWithLogger is like New but uses the supplied logger for non-fatal
// maintenance initialisation warnings (auto_vacuum, journal_size_limit).
// A nil logger falls back to the default New behaviour (stderr).
func NewWithLogger(path string, logger *slog.Logger) (*Store, error) {
	return newStore(path, logger)
}

// New opens (or creates) the SQLite database at path, applies WAL and busy
// timeout pragmas, sets up the connection pool, and runs AutoMigrate.
// It returns the Store and a close function that cleanly releases the DB.
//
// Non-fatal maintenance initialisation failures (auto_vacuum on a new
// database, journal_size_limit) are logged to stderr (no logger available).
// Use NewWithLogger to route them through a configured logger instead.
func New(path string) (*Store, error) {
	return newStore(path, nil)
}

// newStore is the shared implementation of New and NewWithLogger.
func newStore(path string, log *slog.Logger) (*Store, error) {
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		//nolint:gosec/G301 // 0o750: directory is owned by the application user; world-read is not needed
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("creating database directory %q: %w", dir, err)
		}
	}

	// Determine whether the database file already exists (before GORM
	// opens it). A non-existent file means a new database; we can set
	// auto_vacuum=INCREMENTAL before any schema is created. An existing
	// file means an existing database; we must NOT silently convert its
	// auto_vacuum mode (a full VACUUM is deferred to a manual operator
	// action).
	_, statErr := os.Stat(path)
	isNewDB := os.IsNotExist(statErr)

	// For a new database, set auto_vacuum=INCREMENTAL before GORM opens
	// it. This pragma can only be changed when the database is empty
	// (no tables), so it must be set before any schema is created.
	// For an existing database, skip this step (no silent conversion).
	//
	// Failure policy (new database): a failed auto_vacuum init is NOT
	// fatal — the database is fully functional, just without incremental
	// reclamation. The operator can manually run a full VACUUM + set
	// auto_vacuum=INCREMENTAL during a maintenance window. The failure is
	// logged (stderr) so it is observable rather than silently ignored.
	if isNewDB {
		if err := setNewDatabaseAutoVacuum(path); err != nil {
			msg := fmt.Sprintf("gossipgrid: warning: failed to set auto_vacuum=INCREMENTAL on new database %q: %v "+
				"(the database is functional; incremental page reclamation is unavailable; "+
				"run a full VACUUM during a maintenance window to convert the database)",
				path, err)
			if log != nil {
				log.Warn(msg)
			} else {
				fmt.Fprintf(os.Stderr, "%s\n", msg)
			}
		}
	}

	db, err := gorm.Open(sqlite.Open(path+"?_journal_mode=WAL&_busy_timeout=5000"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("opening database %q: %w", path, err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("accessing underlying sql.DB: %w", err)
	}

	// SQLite has a single writer; limit the pool to 1 to avoid
	// SQLITE_BUSY from concurrent writers. Readers are handled by WAL.
	sqlDB.SetMaxOpenConns(1)
	sqlDB.SetMaxIdleConns(1)

	// Apply WAL maintenance pragmas (journal_size_limit) after the
	// database is open. This is safe for both new and existing databases.
	//
	// Failure policy: a failed journal_size_limit is NOT fatal — the WAL
	// still works (it just may grow larger than the limit under normal
	// checkpointing). The failure is logged (stderr) so it is observable.
	if err := applyWALPragmas(sqlDB); err != nil {
		msg := fmt.Sprintf("gossipgrid: warning: failed to set PRAGMA journal_size_limit=%d on database %q: %v "+
			"(the database is functional; the WAL may grow beyond the 64 MiB retained-file target)",
			walJournalSizeLimitBytes, path, err)
		if log != nil {
			log.Warn(msg)
		} else {
			fmt.Fprintf(os.Stderr, "%s\n", msg)
		}
	}

	// AutoMigrate creates or updates the schema for both the messages table
	// and the local_metadata table (Milestone 5A). The local_metadata table
	// is a singleton row that binds the entire SQLite database to exactly
	// one cluster and one epoch. It is backward-compatible: a database from
	// before Milestone 5A simply has no local_metadata row, and the
	// binding functions handle the "no binding" case explicitly.
	if err := db.AutoMigrate(&Message{}, &localMetadata{}); err != nil {
		return nil, fmt.Errorf("auto-migrating schema: %w", err)
	}

	// Create the created_at index for efficient expiration queries
	// (DeleteExpired and the expiration filter in List/Get/Inventory).
	// The index is idempotent: on an existing database without the index,
	// this creates it; on a database that already has it, this is a no-op.
	// The created_at column is a plain TEXT column (no GORM auto-managed
	// index), so the index must be created explicitly. It is a non-unique
	// index (many messages share a creation timestamp).
	//
	// The index cost on an existing database is a one-time O(n) build (n =
	// number of rows). This is acceptable because it runs once at startup
	// and significantly speeds up every subsequent expiration query.
	// The index is NOT a migration that changes existing data; it only
	// adds a secondary structure.
	if err := ensureCreatedAtIndex(db); err != nil {
		return nil, fmt.Errorf("ensuring created_at index: %w", err)
	}

	// Resolve the absolute path for the WAL file size check.
	absPath := path
	if !filepath.IsAbs(path) {
		if cwd, err := os.Getwd(); err == nil {
			absPath = filepath.Join(cwd, path)
		}
	}
	return &Store{db: db, dbPath: absPath}, nil
}

// Close releases the underlying database connection.
func (s *Store) Close() error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

// validateMessage checks a message's immutable fields: a valid UUIDv7 ID in
// canonical lowercase-hyphenated form, a non-empty origin, a set (non-zero)
// creation timestamp, and a non-empty body within the size limit. It is
// applied to every message before any database access, so invalid input is
// rejected regardless of whether its ID exists.
func validateMessage(msg *Message) error {
	if msg == nil {
		return ErrNilMessage
	}
	u, err := uuid.Parse(msg.ID)
	if err != nil {
		return ErrInvalidID
	}
	if u.Version() != 7 {
		return ErrInvalidID
	}
	// Enforce the canonical textual representation: uuid.UUID.String() always
	// returns the lowercase, hyphenated form (8-4-4-4-12). Any alternate form
	// (uppercase, hyphenless, etc.) is rejected so that SQLite text primary
	// keys remain unique and idempotent inserts are not bypassed by textual
	// aliasing.
	if u.String() != msg.ID {
		return ErrInvalidID
	}
	if msg.Origin == "" {
		return ErrEmptyOrigin
	}
	if len([]byte(msg.Origin)) > maxMessageOriginBytes {
		return ErrOriginTooLarge
	}
	if msg.CreatedAt.IsZero() {
		return ErrMissingTimestamp
	}
	if msg.Body == "" {
		return ErrEmptyBody
	}
	if len([]byte(msg.Body)) > maxMessageBodyBytes {
		return ErrBodyTooLarge
	}
	return nil
}

// CreateLocalMessage generates a new UUIDv7, assigns the local origin and
// current UTC timestamp, and persists the message.
func (s *Store) CreateLocalMessage(origin, body string) (*Message, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generating UUIDv7: %w", err)
	}

	msg := &Message{
		ID:        id.String(),
		Body:      body,
		Origin:    origin,
		CreatedAt: time.Now().UTC(),
	}

	// The generated UUIDv7 ID is always valid; validateMessage here rejects an
	// empty/oversized body, an empty origin, and (defensively) a zero timestamp.
	if err := validateMessage(msg); err != nil {
		return nil, err
	}

	if err := s.db.Create(msg).Error; err != nil {
		return nil, fmt.Errorf("inserting local message: %w", err)
	}
	return msg, nil
}

// InsertReplicatedMessage persists an existing message preserving its original
// identity and metadata. It is atomic and idempotent:
//
//   - A duplicate ID with identical content is a successful no-op.
//   - A duplicate ID with conflicting content returns ErrDuplicateIDConflict.
//   - The original row is never overwritten, and concurrent identical inserts
//     collapse to a single row while concurrent conflicting inserts yield one
//     winner and explicit conflicts for the rest.
//
// The insert is a single atomic ON CONFLICT DO NOTHING statement (no
// SELECT-then-INSERT race); when it does not create a row, the existing row is
// fetched and its immutable fields compared.
func (s *Store) InsertReplicatedMessage(msg *Message) error {
	if err := validateMessage(msg); err != nil {
		return err
	}

	// Atomically insert, skipping if the ID already exists (ON CONFLICT DO
	// NOTHING). CreatedAt is a non-primary-key column, so it is preserved from
	// the incoming message rather than generated by GORM.
	res := s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoNothing: true,
	}).Create(msg)
	if res.Error != nil {
		return fmt.Errorf("inserting replicated message: %w", res.Error)
	}
	if res.RowsAffected == 1 {
		return nil // created
	}

	// The row already existed (insert skipped). Compare its immutable fields.
	var existing Message
	if err := s.db.Where("id = ?", msg.ID).First(&existing).Error; err != nil {
		return fmt.Errorf("fetching existing message %q: %w", msg.ID, err)
	}
	if existing.Body == msg.Body && existing.Origin == msg.Origin && existing.CreatedAt.Equal(msg.CreatedAt) {
		return nil // idempotent no-op
	}
	return ErrDuplicateIDConflict
}

// InsertReplicatedMessageReturningInserted is like InsertReplicatedMessage but
// also returns a boolean indicating whether a new row was actually created.
// The boolean is true only when the atomic ON CONFLICT DO NOTHING insert
// created a row (RowsAffected == 1). An identical duplicate (no-op) and a
// conflicting duplicate (ErrDuplicateIDConflict) both return false.
//
// This method exists for the gossip engine (Milestone 4B) which needs to
// distinguish "new message stored" from "duplicate no-op" to decide whether
// to forward the message further. It preserves the same validation, atomicity,
// and error semantics as InsertReplicatedMessage.
func (s *Store) InsertReplicatedMessageReturningInserted(msg *Message) (bool, error) {
	if err := validateMessage(msg); err != nil {
		return false, err
	}

	res := s.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoNothing: true,
	}).Create(msg)
	if res.Error != nil {
		return false, fmt.Errorf("inserting replicated message: %w", res.Error)
	}
	if res.RowsAffected == 1 {
		return true, nil // new row created
	}

	// The row already existed. Compare its immutable fields.
	var existing Message
	if err := s.db.Where("id = ?", msg.ID).First(&existing).Error; err != nil {
		return false, fmt.Errorf("fetching existing message %q: %w", msg.ID, err)
	}
	if existing.Body == msg.Body && existing.Origin == msg.Origin && existing.CreatedAt.Equal(msg.CreatedAt) {
		return false, nil // idempotent no-op
	}
	return false, ErrDuplicateIDConflict
}

// BootstrapInsertResult is the structured outcome of a bounded, transactional
// batch install of replicated messages (Milestone 5B, Stage 2, Issue 5 and
// Issue 6). It distinguishes per-record dispositions so the caller can build
// a BootstrapPageResult and report accurate counts. The record sets are
// mutually exclusive and exhaustive per input message ID.
type BootstrapInsertResult struct {
	// Inserted is the set of IDs whose new rows were created by this
	// transaction (the install succeeded for those records).
	Inserted []string
	// AlreadyPresent is the set of IDs that were already present locally with
	// identical content (idempotent no-op; the transaction preserved them).
	AlreadyPresent []string
	// Conflict is the set of IDs whose fetched content conflicted with a
	// different local row (ErrDuplicateIDConflict; the local row was preserved).
	// When Conflict is non-empty, the caller MUST treat the page as failed
	// (an unresolved conflict), and the transaction is rolled back (no
	// records in this batch are persisted).
	Conflict []string
	// Expired is the set of IDs whose fetched record was expired on the
	// requester side at the install boundary (CreatedAt strictly before the
	// expiration threshold) and was skipped without being stored. The page is
	// NOT failed by an expired record (this is a legitimate, expected
	// retention outcome); the ID is simply not installed.
	Expired []string
	// Error is set when a non-conflict, non-expired failure occurred (a
	// validation error, a context cancellation, or a database error). When
	// set, the transaction is rolled back and no records in this batch are
	// persisted.
	Error error
}

// InsertReplicatedMessagesBounded installs a bounded batch of replicated
// messages in a single, context-aware SQLite transaction (Milestone 5B,
// Stage 2). It is the "narrowly bounded transactional commit boundary" for
// bootstrap baseline installation:
//
//   - The transaction is bounded: it inserts at most len(msgs) rows in a
//     single BEGIN…COMMIT, and the work is capped by the caller's batch size
//     (the engine splits the missing IDs into FetchBatchSize batches, so a
//     single transaction never exceeds the configured batch).
//   - The transaction is context-aware: the context is checked before the
//     BEGIN and passed to every statement; a cancellation or the store's
//     SQLite busy-timeout (5s, set at open) aborts the transaction and rolls
//     it back (a graceful stop, not a partial commit).
//   - The transaction is atomic per batch: either every non-conflict,
//     non-expired record in the batch is committed, or none are (rollback on
//     a conflict or an error). This preserves the "already-committed valid
//     data is preserved" invariant across batches (each prior batch's commit
//     is independent and durable).
//
// Retention evaluation boundary (single-transaction correction):
// the expiration threshold is supplied by the caller as a callback
// (expirationThreshold). The callback is invoked INSIDE the transaction,
// after a no-op DML statement (DELETE FROM messages WHERE 0=1) that acquires
// the SQLite write lock on the store's single connection. Because the
// connection pool is limited to 1 connection and SQLite has a single writer,
// the transaction blocks (up to the 5s busy-timeout) if another writer
// holds the write lock, and the callback is not evaluated until the
// transaction holds write access. This eliminates the separate
// probe-write transaction and the window between its commit and the
// install transaction's BEGIN in which another operation could interleave.
// A nil callback disables retention filtering (all records are installable).
//
// Per-record semantics:
//
//   - new valid record: row created (Inserted);
//   - identical duplicate: idempotent no-op (AlreadyPresent);
//   - duplicate with conflicting content: Conflict (transaction rolled back,
//     local row preserved);
//   - expired (CreatedAt < threshold): Expired (skipped, not stored).
//
// The method is the linearization point for the batch: it is called from
// within the bootstrap commit guard (which holds the controller's
// authorization read lock), so the authorization is revalidated at the same
// lock that this transaction commits under. A context cancellation during
// the transaction (while waiting for a connection, waiting for the write
// lock, or during the insert) aborts without a partial commit.
func (s *Store) InsertReplicatedMessagesBounded(ctx context.Context, msgs []*Message, expirationThreshold func() time.Time) (BootstrapInsertResult, error) {
	var res BootstrapInsertResult
	if len(msgs) == 0 {
		return res, nil
	}

	// Step 1: validate all messages (rejects invalid input before any
	// database access; a validation error is a batch failure).
	for _, m := range msgs {
		if err := validateMessage(m); err != nil {
			return res, fmt.Errorf("validating bootstrap message %q: %w", m.ID, err)
		}
	}

	// Step 2: check context cancellation before the BEGIN.
	if !ctxOk(ctx) {
		return res, ctx.Err()
	}

	// Step 3: begin ONE SQLite transaction using the existing GORM database
	// and caller context. The single-connection pool means this blocks if
	// another operation holds the connection (connection-pool contention).
	tx := s.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		if ctxOk(ctx) {
			return res, fmt.Errorf("beginning bootstrap insert transaction: %w", tx.Error)
		}
		return res, ctx.Err()
	}

	// Step 4: deferred rollback protection.
	committed := false
	defer func() {
		if !committed {
			//nolint:errcheck // rollback during a failed/cancelled batch; error not observable
			_ = tx.Rollback()
		}
	}()

	// Step 5: if retention is enabled, acquire write access within this
	// SAME transaction. The no-op DML statement (DELETE matches zero rows)
	// acquires the SQLite write lock: it blocks if another writer holds the
	// lock (up to the 5s busy-timeout). A context cancellation while waiting
	// for the write lock aborts the transaction.
	if expirationThreshold != nil {
		if err := tx.Exec("DELETE FROM messages WHERE 0=1").Error; err != nil {
			if ctxOk(ctx) {
				return res, fmt.Errorf("acquiring SQLite write access in bootstrap insert transaction: %w", err)
			}
			return res, ctx.Err()
		}
	}

	// Step 6: check context cancellation after acquiring write access.
	if !ctxOk(ctx) {
		return res, ctx.Err()
	}

	// Step 7: invoke the expiration-threshold callback while the transaction
	// holds write access. The threshold reflects the time spent waiting for
	// both the connection and the write lock, eliminating the interleave
	// window of the separate probe-write approach.
	var threshold time.Time
	if expirationThreshold != nil {
		threshold = expirationThreshold()
	}

	// Step 8: classify expired records and insert retained records using
	// the SAME transaction. An expired record (CreatedAt < threshold) is
	// skipped (not stored); it is NOT a batch failure.
	var inserted, alreadyPresent []string
	for _, m := range msgs {
		if !threshold.IsZero() && m.CreatedAt.Before(threshold) {
			res.Expired = append(res.Expired, m.ID)
			continue
		}
		// Step 9: idempotent insert with immutable-content conflict checking.
		res2 := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}},
			DoNothing: true,
		}).Create(m)
		if res2.Error != nil {
			if ctxOk(ctx) {
				return res, fmt.Errorf("inserting bootstrap message %q: %w", m.ID, res2.Error)
			}
			return res, ctx.Err()
		}
		if res2.RowsAffected == 1 {
			inserted = append(inserted, m.ID)
			continue
		}
		var existing Message
		if err := tx.Where("id = ?", m.ID).First(&existing).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return res, fmt.Errorf("fetching existing bootstrap message %q: %w", m.ID, err)
			}
			return res, fmt.Errorf("fetching existing bootstrap message %q: %w", m.ID, err)
		}
		if existing.Body == m.Body && existing.Origin == m.Origin && existing.CreatedAt.Equal(m.CreatedAt) {
			alreadyPresent = append(alreadyPresent, m.ID)
			continue
		}
		// Immutable content conflict: the local row is preserved. The batch
		// is failed (the transaction is rolled back — no records in this
		// batch are persisted).
		res.Conflict = append(res.Conflict, m.ID)
		return res, nil
	}

	// Step 10: commit the transaction.
	if err := tx.Commit().Error; err != nil {
		return res, fmt.Errorf("committing bootstrap insert transaction: %w", err)
	}
	committed = true

	// Step 11: publish committed insertion accounting only after successful
	// COMMIT.
	res.Inserted = inserted
	res.AlreadyPresent = alreadyPresent
	return res, nil
}

// GetByID retrieves a single message by its ID.
// Returns gorm.ErrRecordNotFound if the message does not exist.
func (s *Store) GetByID(id string) (*Message, error) {
	var msg Message
	if err := s.db.Where("id = ?", id).First(&msg).Error; err != nil {
		return nil, err
	}
	return &msg, nil
}

// List returns up to limit messages, newest first, with optional pagination
// offset. Returns the messages and the total count in the table.
func (s *Store) List(limit, offset int) ([]Message, int64, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	var total int64
	if err := s.db.Model(&Message{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var msgs []Message
	if err := s.db.Order("created_at DESC").Limit(limit).Offset(offset).Find(&msgs).Error; err != nil {
		return nil, 0, err
	}
	return msgs, total, nil
}

// InventoryPage is a bounded page of message IDs from the keyset inventory
// scan. IDs are in strict ascending order with no duplicates.
type InventoryPage struct {
	// IDs is the ascending slice of message IDs in this page.
	IDs []string
	// NextCursor is the last ID in this page, to be passed as the cursor for
	// the next request. Empty on the final page (HasMore == false).
	NextCursor string
	// HasMore is true when additional records exist beyond this page.
	HasMore bool
}

// ListIDsAfter returns a bounded page of message IDs using keyset pagination:
//
//	SELECT id FROM messages WHERE id > ? ORDER BY id ASC LIMIT ?+1
//
// after is the exclusive cursor (empty string for the first page). limit is
// the maximum number of IDs to return. The method uses the primary-key index
// and does not scan or sort the entire table in application memory. It
// returns at most limit IDs (plus a HasMore indicator) and a NextCursor that
// identifies the last actual ID in the page.
//
// The query is cancellable through context.
func (s *Store) ListIDsAfter(ctx context.Context, after string, limit int) (*InventoryPage, error) {
	if limit < 1 {
		return nil, fmt.Errorf("page limit must be at least 1 (got %d)", limit)
	}

	query := s.db.WithContext(ctx).Model(&Message{}).Order("id ASC").Limit(limit + 1)
	if after != "" {
		query = query.Where("id > ?", after)
	}

	var ids []string
	if err := query.Pluck("id", &ids).Error; err != nil {
		return nil, fmt.Errorf("listing message IDs: %w", err)
	}

	page := &InventoryPage{IDs: ids}
	if len(ids) > limit {
		page.HasMore = true
		page.IDs = ids[:limit]
		page.NextCursor = ids[limit-1]
	} else {
		// Final page: HasMore is false and NextCursor is empty. The
		// client resets its local cursor to detect a completed sweep.
		page.HasMore = false
	}
	return page, nil
}

// FindMissingIDs returns which of the supplied IDs are absent from the local
// store. It validates each ID as a canonical UUIDv7 and performs a bounded
// query (no full table scan). The returned slice contains only IDs that do
// not exist locally, in the same order as supplied.
//
// The query is cancellable through context.
func (s *Store) FindMissingIDs(ctx context.Context, ids []string) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	// Validate all IDs first (rejects non-UUIDv7 values that could be
	// injection vectors or indicate a protocol defect).
	for _, id := range ids {
		if err := validateCanonicalUUIDv7(id); err != nil {
			return nil, err
		}
	}

	var existing []string
	if err := s.db.WithContext(ctx).Model(&Message{}).Where("id IN ?", ids).Pluck("id", &existing).Error; err != nil {
		return nil, fmt.Errorf("checking existing message IDs: %w", err)
	}

	existingSet := make(map[string]bool, len(existing))
	for _, id := range existing {
		existingSet[id] = true
	}

	var missing []string
	for _, id := range ids {
		if !existingSet[id] {
			missing = append(missing, id)
		}
	}
	return missing, nil
}

// GetMessagesByIDs retrieves complete message records for the supplied IDs.
// It validates each ID as a canonical UUIDv7 and returns the found messages
// in deterministic ID-ascending order. If any supplied ID is not found, the
// method returns the found messages plus a *missingIDsError carrying the
// missing IDs (the caller decides whether partial results are acceptable).
//
// The query is cancellable through context.
func (s *Store) GetMessagesByIDs(ctx context.Context, ids []string) ([]*Message, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := validateCanonicalUUIDv7(id); err != nil {
			return nil, err
		}
	}

	// Use the primary-key index for a bounded lookup.
	query := s.db.WithContext(ctx).Where("id IN ?", ids)
	var msgs []Message
	if err := query.Order("id ASC").Find(&msgs).Error; err != nil {
		return nil, fmt.Errorf("fetching messages by ID: %w", err)
	}
	return splitMissing(msgs, ids)
}

// splitMissing splits the found messages from the requested IDs into the
// found records (in ID-ascending order) and the requested IDs that were not
// found. When any IDs are missing it returns the found records plus a
// *MissingIDsError; otherwise it returns the records with a nil error.
func splitMissing(msgs []Message, ids []string) ([]*Message, error) {
	found := make(map[string]bool, len(msgs))
	for i := range msgs {
		found[msgs[i].ID] = true
	}
	var missing []string
	for _, id := range ids {
		if !found[id] {
			missing = append(missing, id)
		}
	}
	result := make([]*Message, len(msgs))
	for i := range msgs {
		result[i] = &msgs[i]
	}
	if len(missing) > 0 {
		return result, &MissingIDsError{missing: missing}
	}
	return result, nil
}

// MissingIDsError is a typed error carrying the set of message IDs that were
// requested but were not found (absent, or expired under a retention
// threshold). It exists so the sync-fetch endpoint can convey the missing IDs
// to a peer in a structured, machine-readable form (the 404 response's
// "missing_ids" field) rather than relying on a peer parsing a
// human-readable error string.
//
// The error is only ever produced by GetMessagesByIDs /
// GetMessagesByIDsWithExpiration for the exact IDs that were requested and
// not returned, so a peer that receives the corresponding 404 response can
// validate each reported ID against its original request and trust the set.
type MissingIDsError struct {
	missing []string
}

func (e *MissingIDsError) Error() string {
	return fmt.Sprintf("message IDs not found: %v", e.missing)
}

// MissingIDs returns the message IDs reported as missing by this error, in
// the order they were requested. The empty set means no IDs were missing.
func (e *MissingIDsError) MissingIDs() []string {
	return e.missing
}

// ExpirationThreshold returns the strict upper bound on a message's creation
// timestamp for it to be considered retained (non-expired) at the given
// reference time. A message with CreatedAt strictly before this threshold is
// expired; a message exactly at or after it is retained. The zero threshold
// means retention is disabled (no message is expired).
//
// The threshold is derived from a single, consistent reference time so that
// all read paths (API listing, single fetch, inventory, sync) and the cleanup
// path agree on the same cutoff. It is the canonical expression of "a message
// is expired when now - CreatedAt >= TTL": the boundary is created_at < now -
// TTL. Using a strict "<" at the boundary means a message whose age exactly
// equals the TTL is retained for that instant, and becomes expired on the
// next evaluation — a deliberately conservative choice that avoids deleting
// a message the instant it reaches the boundary under clock skew.
func ExpirationThreshold(ttl time.Duration, now time.Time) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(-ttl)
}

// expiredFilter returns the GORM Where clause that restricts a query to
// retained (non-expired) messages, or an empty string when retention is
// disabled (threshold is the zero time). The zero threshold is used as the
// "disabled" sentinel: when ttl is zero, ExpirationThreshold returns the zero
// time and this returns "", so callers may append it unconditionally without
// changing the query. Retained means CreatedAt >= threshold (a message exactly
// at the cutoff is retained; strictly older is expired).
func expiredFilter(threshold time.Time) string {
	if threshold.IsZero() {
		return ""
	}
	return "created_at >= ?"
}

// ListWithExpiration is like List but excludes expired messages (those with
// CreatedAt strictly before the given expiration threshold). When the
// threshold is the zero time (retention disabled), it is identical to List.
// The total count reflects only retained messages, so the API listing and its
// "total" cannot diverge on an expired row.
func (s *Store) ListWithExpiration(limit, offset int, threshold time.Time) ([]Message, int64, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	var total int64
	query := s.db.Model(&Message{})
	if f := expiredFilter(threshold); f != "" {
		query = query.Where(f, threshold)
	}
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	listQuery := s.db
	if f := expiredFilter(threshold); f != "" {
		listQuery = listQuery.Where(f, threshold)
	}
	var msgs []Message
	if err := listQuery.Order("created_at DESC").Limit(limit).Offset(offset).Find(&msgs).Error; err != nil {
		return nil, 0, err
	}
	return msgs, total, nil
}

// GetByIDWithExpiration is like GetByID but returns gorm.ErrRecordNotFound for
// an expired message (one with CreatedAt strictly before the threshold), so an
// expired message is indistinguishable from an absent one to the API. When the
// threshold is the zero time (retention disabled), it is identical to GetByID.
func (s *Store) GetByIDWithExpiration(id string, threshold time.Time) (*Message, error) {
	query := s.db.Where("id = ?", id)
	if f := expiredFilter(threshold); f != "" {
		query = query.Where(f, threshold)
	}
	var msg Message
	if err := query.First(&msg).Error; err != nil {
		return nil, err
	}
	return &msg, nil
}

// ListIDsAfterWithExpiration is like ListIDsAfter but the keyset inventory
// scan is restricted to retained messages (those with CreatedAt at or after
// the threshold). This is the anti-entropy resurrection guard: a peer's
// inventory (GET /internal/sync/ids) must not advertise IDs of messages it has
// already expired, otherwise a peer with a shorter retention would fetch and
// re-insert (resurrect) a message that the source considers dead. When the
// threshold is the zero time (retention disabled), it is identical to
// ListIDsAfter.
func (s *Store) ListIDsAfterWithExpiration(ctx context.Context, after string, limit int, threshold time.Time) (*InventoryPage, error) {
	if limit < 1 {
		return nil, fmt.Errorf("page limit must be at least 1 (got %d)", limit)
	}

	query := s.db.WithContext(ctx).Model(&Message{}).Order("id ASC").Limit(limit + 1)
	if f := expiredFilter(threshold); f != "" {
		query = query.Where(f, threshold)
	}
	if after != "" {
		query = query.Where("id > ?", after)
	}

	var ids []string
	if err := query.Pluck("id", &ids).Error; err != nil {
		return nil, fmt.Errorf("listing message IDs: %w", err)
	}

	page := &InventoryPage{IDs: ids}
	if len(ids) > limit {
		page.HasMore = true
		page.IDs = ids[:limit]
		page.NextCursor = ids[limit-1]
	} else {
		page.HasMore = false
	}
	return page, nil
}

// GetMessagesByIDsWithExpiration is like GetMessagesByIDs but, in addition to
// the "ID not found" error for absent records, it also reports as missing any
// record that has expired (CreatedAt strictly before the threshold). This is
// the second anti-entropy resurrection guard (defense in depth): even if an
// expired ID were advertised in an inventory page, the fetch (POST
// /internal/sync/messages) would return it as "not found", so the peer would
// not re-insert it. When the threshold is the zero time (retention disabled),
// it is identical to GetMessagesByIDs.
func (s *Store) GetMessagesByIDsWithExpiration(ctx context.Context, ids []string, threshold time.Time) ([]*Message, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := validateCanonicalUUIDv7(id); err != nil {
			return nil, err
		}
	}

	query := s.db.WithContext(ctx).Where("id IN ?", ids)
	if f := expiredFilter(threshold); f != "" {
		query = query.Where(f, threshold)
	}
	var msgs []Message
	if err := query.Order("id ASC").Find(&msgs).Error; err != nil {
		return nil, fmt.Errorf("fetching messages by ID: %w", err)
	}
	return splitMissing(msgs, ids)
}

// deleteBatchSize is the maximum number of rows deleted per batch in
// DeleteExpired. Bounding the batch size prevents the cleaner from holding
// the SQLite writer lock for excessively long periods on a database with
// many expired rows. The default is 1000 rows per batch: a batch of 1000
// row deletions (each with a small row size) completes quickly, and the
// remaining expired rows are deleted in subsequent batches within the same
// sweep. A batch that finds no more eligible rows terminates the sweep.
const deleteBatchSize = 1000

// DeleteExpired permanently removes messages with CreatedAt strictly before
// the given expiration threshold, in bounded batches, up to a total-row
// budget. It returns the number of rows deleted on this call. When the
// threshold is the zero time (retention disabled), it is a no-op returning 0.
//
// Batching: the deletion is performed in batches of deleteBatchSize rows.
// Each batch is a single bounded DELETE with a LIMIT clause (using
// SQLite's "DELETE FROM ... WHERE ... LIMIT ?" via a subquery on the
// primary-key index). This prevents the cleaner from holding the writer lock
// for excessively long periods and allows concurrent readers and writers to
// make progress between batches.
//
// Total-work budget: the batch loop stops as soon as it has deleted
// maxTotal rows on this call, even if more eligible rows remain. An
// unfinished backlog is not lost: it resumes on the next scheduled sweep
// (the caller re-runs DeleteExpired with a fresh budget). This guarantees a
// single sweep is bounded in work and cannot run indefinitely in the face of
// continuous incoming writes.
//
// Time budget: if deadline is non-nil, the loop also stops once time.Now()
// passes it. The deadline is passed via a derived context (ctx) so that both
// this check and the context check below bound the sweep in wall-clock time,
// not only in row count.
//
// Cancellation: the context is checked before each batch and passed to the
// database operation. If the context is cancelled (e.g., during shutdown, or
// the time-budget deadline has passed), the in-flight batch is aborted and the
// method returns the partial count with no error (a stopped sweep is not a
// failure; the remaining rows are deleted on the next sweep).
//
// Idempotency: re-running on an already-clean database deletes nothing (the
// first batch finds no eligible rows and returns immediately).
//
// maxTotal < 0 means "no total-row budget" (delete all eligible rows). This
// is a compatibility default; the retention cleaner always supplies a positive
// budget.
//
// The created_at index (created by ensureCreatedAtIndex) makes the
// "created_at < threshold" predicate efficient (an index range scan rather
// than a full table scan). The batch's inner query uses the primary-key
// index (WHERE id IN (SELECT ...)) to bound the deletion.
func (s *Store) DeleteExpired(ctx context.Context, threshold time.Time, maxTotal int64, deadline *time.Time) (int64, error) {
	if threshold.IsZero() {
		return 0, nil
	}
	sqlDB, err := s.db.DB()
	if err != nil {
		return 0, fmt.Errorf("accessing underlying sql.DB: %w", err)
	}
	var total int64
	for {
		if !ctxOk(ctx) {
			return total, nil // cancelled: return the partial count
		}
		// Time budget: stop once the wall-clock deadline has passed. The
		// deadline is a strict wall-clock cap on the sweep (it bounds the
		// sweep in time, not only in row count); it is checked against
		// time.Now. (The caller does NOT derive a context deadline from it,
		// so the deadline is a pure wall-clock check that is independent of
		// the caller's logical clock.)
		if deadline != nil && time.Now().After(*deadline) {
			return total, nil
		}
		// Total-row budget: stop once the budget for this call is exhausted.
		// A negative maxTotal means "no budget" (delete all eligible rows).
		if maxTotal >= 0 && total >= maxTotal {
			return total, nil
		}
		// Bounded batch: delete up to deleteBatchSize rows with
		// created_at < threshold, capped by the remaining budget. The inner
		// SELECT uses the created_at index to find the first N eligible IDs
		// (by creation time); the outer DELETE removes them by primary key.
		// The batch limit is min(deleteBatchSize, remaining budget) so the
		// batch never exceeds the total-row budget.
		batch := int64(deleteBatchSize)
		if maxTotal >= 0 {
			rem := maxTotal - total
			if rem < batch {
				batch = rem
			}
		}
		// Execute the batch delete in a short-lived transaction using a
		// PREPARED statement on the raw driver. Using database/sql directly
		// (rather than GORM's Exec) is essential: GORM's Exec reports the
		// subquery's row count (1000) for the DELETE ... IN (SELECT ... LIMIT)
		// form, not the number of rows actually deleted (up to 2000, because
		// the subquery can return duplicate IDs that resolve to the same
		// message row). A prepared statement's RowsAffected correctly counts
		// the deleted rows, so the batch accounting and budget are accurate.
		tx, terr := sqlDB.BeginTx(ctx, &sql.TxOptions{})
		if terr != nil {
			// A context cancellation during begin is a graceful stop; a real
			// error is returned.
			if ctxOk(ctx) {
				return total, fmt.Errorf("beginning delete batch: %w", terr)
			}
			return total, nil
		}
		deleted, derr := deleteBatchTx(ctx, tx, threshold, batch)
		if derr != nil {
			// Roll back the failed batch so its (partial) deletion is never
			// committed. A context cancellation during the delete is a
			// graceful stop, not a failure: return the partial count (rows
			// deleted in batches committed before the interruption) with no
			// error. The remaining rows are deleted on the next sweep.
			rollbackFailedBatch(tx)
			if ctxOk(ctx) {
				return total, fmt.Errorf("deleting expired messages: %w", derr)
			}
			return total, nil
		}
		// Commit only after the batch delete executed successfully. Check and
		// propagate the commit error: a commit that failed (e.g. a SQLite busy
		// or I/O error) means the batch's deletions were NOT persisted, so it
		// must not be counted. Roll the failed batch back and stop the sweep
		// (the caller logs it and resumes on the next sweep).
		if cerr := tx.Commit(); cerr != nil {
			rollbackFailedBatch(tx)
			if ctxOk(ctx) {
				return total, fmt.Errorf("committing delete batch: %w", cerr)
			}
			return total, nil
		}
		// Only a successfully committed batch contributes to the count.
		total += deleted
		// A batch that deleted fewer rows than the batch limit means all
		// eligible rows (or the budget) are gone. Terminate.
		if deleted < batch {
			return total, nil
		}
		// A full batch: continue to the next batch (subject to the budgets
		// and cancellation checks at the top of the loop).
	}
}

// deleteBatchTx executes one bounded batch of expired-message deletions
// inside the supplied transaction and returns the number of rows actually
// deleted (from the prepared statement's RowsAffected, which is accurate for
// the DELETE ... IN (SELECT ...) form, unlike GORM's Exec). The inner SELECT
// uses the created_at index to find the first `limit` eligible IDs (by
// creation time); the outer DELETE removes them by primary key.
func deleteBatchTx(ctx context.Context, tx *sql.Tx, threshold time.Time, limit int64) (int64, error) {
	stmt, err := tx.PrepareContext(ctx,
		"DELETE FROM messages WHERE id IN (SELECT id FROM messages WHERE created_at < ? ORDER BY created_at ASC, id ASC LIMIT ?)")
	if err != nil {
		return 0, err
	}
	defer func() {
		//nolint:errcheck,gosec/G104 // statement close during a batch; error not observable
		_ = stmt.Close()
	}()
	res, err := stmt.ExecContext(ctx, threshold, limit)
	if err != nil {
		return 0, err
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

// rollbackFailedBatch rolls back a delete batch transaction whose delete or
// commit failed, so a partial deletion is never committed. The error is not
// observable (the batch is already being abandoned as failed or cancelled),
// so it is discarded.
func rollbackFailedBatch(tx *sql.Tx) {
	//nolint:errcheck,gosec/G104 // rollback during a failed/cancelled batch; error not observable
	_ = tx.Rollback()
}

// ensureCreatedAtIndex creates the index on the messages.created_at column
// if it does not already exist. The index is named idx_messages_created_at
// and is a non-unique index (many messages share a creation timestamp).
// The operation is idempotent: on a database that already has the index,
// this is a no-op (CREATE INDEX IF NOT EXISTS).
//
// The index is required for efficient expiration queries:
//   - DeleteExpired: "created_at < threshold" (a range scan on the index);
//   - The expiration filter in List/Get/Inventory: "created_at >= threshold"
//     (also a range scan on the index).
//
// The index cost on an existing database is a one-time O(n) build. This is
// acceptable because it runs once at startup and significantly speeds up
// every subsequent expiration query. The index does not change existing
// data; it only adds a secondary lookup structure.
func ensureCreatedAtIndex(db *gorm.DB) error {
	return db.Exec("CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages (created_at)").Error
}

// validateCanonicalUUIDv7 checks that s is a valid canonical UUIDv7 in
// lowercase hyphenated form.
func validateCanonicalUUIDv7(s string) error {
	u, err := uuid.Parse(s)
	if err != nil {
		return ErrInvalidID
	}
	if u.Version() != 7 {
		return ErrInvalidID
	}
	if u.String() != s {
		return ErrInvalidID
	}
	return nil
}

// DB returns the underlying *gorm.DB for testing.
func (s *Store) DB() *gorm.DB {
	return s.db
}
