// Package store provides local message persistence using GORM and SQLite.
//
// Each application instance owns an independent SQLite database. The store
// exposes two operations: CreateLocalMessage (generate identity + persist) and
// InsertReplicatedMessage (preserve existing identity + persist idempotently).
package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

const maxMessageBodyBytes = 4096

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

	// ErrDuplicateIDConflict is returned when a duplicate ID is inserted with
	// conflicting content (different body, origin, or timestamp).
	ErrDuplicateIDConflict = errors.New("message ID already exists with conflicting content")

	// ErrNotFound is returned when a message ID does not exist.
	ErrNotFound = gorm.ErrRecordNotFound
)

// Store holds the database handle for local message persistence.
type Store struct {
	db *gorm.DB
}

// New opens (or creates) the SQLite database at path, applies WAL and busy
// timeout pragmas, sets up the connection pool, and runs AutoMigrate.
// It returns the Store and a close function that cleanly releases the DB.
func New(path string) (*Store, error) {
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		//nolint:gosec/G301 // 0o750: directory is owned by the application user; world-read is not needed
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("creating database directory %q: %w", dir, err)
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

	if err := db.AutoMigrate(&Message{}); err != nil {
		return nil, fmt.Errorf("auto-migrating schema: %w", err)
	}

	return &Store{db: db}, nil
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

// DB returns the underlying *gorm.DB for testing.
func (s *Store) DB() *gorm.DB {
	return s.db
}
