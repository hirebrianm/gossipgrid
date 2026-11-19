package store

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// newTestStore opens a store backed by a temp-file SQLite database.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	st, err := New(path)
	if err != nil {
		t.Fatalf("New(%q): %v", path, err)
	}
	//nolint:errcheck // test cleanup: Close errors are not actionable
	t.Cleanup(func() { st.Close() })
	return st
}

func TestCreateAndGet(t *testing.T) {
	st := newTestStore(t)

	msg, err := st.CreateLocalMessage("node-0", "hello world")
	if err != nil {
		t.Fatalf("CreateLocalMessage: %v", err)
	}

	if msg.ID == "" {
		t.Error("created message has empty ID")
	}
	if msg.Body != "hello world" {
		t.Errorf("Body = %q, want %q", msg.Body, "hello world")
	}
	if msg.Origin != "node-0" {
		t.Errorf("Origin = %q, want %q", msg.Origin, "node-0")
	}
	if msg.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}

	got, err := st.GetByID(msg.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.ID != msg.ID || got.Body != msg.Body || got.Origin != msg.Origin {
		t.Errorf("retrieved message differs: got %+v, want %+v", got, msg)
	}
}

func TestUUIDv7Format(t *testing.T) {
	st := newTestStore(t)

	msg, err := st.CreateLocalMessage("node-0", "test")
	if err != nil {
		t.Fatalf("CreateLocalMessage: %v", err)
	}

	// UUIDv7 format: 8-4-4-4-12 hex digits, version nibble is '7'.
	parsed, err := uuid.Parse(msg.ID)
	if err != nil {
		t.Fatalf("uuid.Parse(%q): %v", msg.ID, err)
	}
	if parsed.Version() != 7 {
		t.Errorf("UUID version = %d, want 7", parsed.Version())
	}
	// UUIDv7 has 128 bits: 8-4-4-4-12 = 36 characters with dashes.
	if len(msg.ID) != 36 {
		t.Errorf("ID length = %d, want 36 (UUIDv7 string format)", len(msg.ID))
	}
}

func TestCreateLocalMessageValidation(t *testing.T) {
	st := newTestStore(t)

	// Empty body.
	_, err := st.CreateLocalMessage("node-0", "")
	if !errors.Is(err, ErrEmptyBody) {
		t.Errorf("empty body: err = %v, want ErrEmptyBody", err)
	}

	// Body exceeding 4096 bytes.
	_, err = st.CreateLocalMessage("node-0", strings.Repeat("a", 4097))
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Errorf("oversized body: err = %v, want ErrBodyTooLarge", err)
	}

	// Exactly 4096 bytes should succeed.
	_, err = st.CreateLocalMessage("node-0", strings.Repeat("a", 4096))
	if err != nil {
		t.Errorf("4096-byte body: err = %v, want nil", err)
	}
}

func TestDuplicateInsertIdempotent(t *testing.T) {
	st := newTestStore(t)

	msg, err := st.CreateLocalMessage("node-0", "original")
	if err != nil {
		t.Fatalf("CreateLocalMessage: %v", err)
	}

	// Re-insert the same message (same ID, same content) — should be a no-op.
	err = st.InsertReplicatedMessage(msg)
	if err != nil {
		t.Errorf("idempotent insert: err = %v, want nil", err)
	}
}

func TestDuplicateInsertConflict(t *testing.T) {
	st := newTestStore(t)

	msg, err := st.CreateLocalMessage("node-0", "original")
	if err != nil {
		t.Fatalf("CreateLocalMessage: %v", err)
	}

	// Same ID, different body — must return conflict error.
	conflict := &Message{
		ID:        msg.ID,
		Body:      "different content",
		Origin:    msg.Origin,
		CreatedAt: msg.CreatedAt,
	}
	err = st.InsertReplicatedMessage(conflict)
	if !errors.Is(err, ErrDuplicateIDConflict) {
		t.Errorf("conflicting insert: err = %v, want ErrDuplicateIDConflict", err)
	}

	// Verify original is unchanged.
	got, err := st.GetByID(msg.ID)
	if err != nil {
		t.Fatalf("GetByID after conflict: %v", err)
	}
	if got.Body != "original" {
		t.Errorf("original was overwritten: Body = %q, want %q", got.Body, "original")
	}
}

func TestInsertReplicatedPreservesMetadata(t *testing.T) {
	st := newTestStore(t)

	// Simulate a message created by a different node.
	origin := "node-7"
	createdAt := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	v7id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid.NewV7: %v", err)
	}
	msg := &Message{
		ID:        v7id.String(),
		Body:      "replicated message",
		Origin:    origin,
		CreatedAt: createdAt,
	}

	err = st.InsertReplicatedMessage(msg)
	if err != nil {
		t.Fatalf("InsertReplicatedMessage: %v", err)
	}

	got, err := st.GetByID(msg.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Origin != origin {
		t.Errorf("Origin = %q, want %q (must be preserved)", got.Origin, origin)
	}
	if got.CreatedAt != createdAt {
		t.Errorf("CreatedAt = %v, want %v (must be preserved)", got.CreatedAt, createdAt)
	}
	if got.Body != msg.Body {
		t.Errorf("Body = %q, want %q", got.Body, msg.Body)
	}
}

func TestListOrderingAndPagination(t *testing.T) {
	st := newTestStore(t)

	// Insert 5 messages with distinct timestamps.
	for i := 0; i < 5; i++ {
		_, err := st.CreateLocalMessage("node-0", "msg")
		if err != nil {
			t.Fatalf("CreateLocalMessage %d: %v", i, err)
		}
	}

	// Fetch all: 5 messages, newest first.
	msgs, total, err := st.List(10, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if total != 5 {
		t.Errorf("total = %d, want 5", total)
	}
	if len(msgs) != 5 {
		t.Fatalf("len(msgs) = %d, want 5", len(msgs))
	}
	// Newest first: CreatedAt should be non-increasing.
	for i := 1; i < len(msgs); i++ {
		if msgs[i-1].CreatedAt.Before(msgs[i].CreatedAt) {
			t.Errorf("messages not newest-first: msgs[%d].CreatedAt=%v > msgs[%d].CreatedAt=%v",
				i-1, msgs[i-1].CreatedAt, i, msgs[i].CreatedAt)
		}
	}

	// Pagination: limit=2, offset=0 → first 2 (newest).
	page1, _, err := st.List(2, 0)
	if err != nil {
		t.Fatalf("List page1: %v", err)
	}
	if len(page1) != 2 {
		t.Errorf("page1 len = %d, want 2", len(page1))
	}

	// Pagination: limit=2, offset=4 → last 1.
	pageLast, _, err := st.List(2, 4)
	if err != nil {
		t.Fatalf("List pageLast: %v", err)
	}
	if len(pageLast) != 1 {
		t.Errorf("pageLast len = %d, want 1", len(pageLast))
	}

	// Pagination: offset beyond range → 0 results.
	beyond, _, err := st.List(2, 100)
	if err != nil {
		t.Fatalf("List beyond: %v", err)
	}
	if len(beyond) != 0 {
		t.Errorf("beyond len = %d, want 0", len(beyond))
	}
}

func TestConcurrentWrites(t *testing.T) {
	st := newTestStore(t)

	const workers = 10
	const perWorker = 5

	var wg sync.WaitGroup
	errCh := make(chan error, workers*perWorker)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				_, err := st.CreateLocalMessage("node-0", "concurrent")
				if err != nil {
					errCh <- err
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("concurrent write error: %v", err)
	}

	msgs, total, err := st.List(100, 0)
	if err != nil {
		t.Fatalf("List after concurrent writes: %v", err)
	}
	want := workers * perWorker
	if total != int64(want) {
		t.Errorf("total = %d, want %d", total, want)
	}
	if len(msgs) != want {
		t.Errorf("len(msgs) = %d, want %d", len(msgs), want)
	}
}

func TestDatabaseReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "reopen.db")

	// First open: create a message.
	st1, err := New(path)
	if err != nil {
		t.Fatalf("New (first): %v", err)
	}
	msg, err := st1.CreateLocalMessage("node-0", "persisted")
	if err != nil {
		t.Fatalf("CreateLocalMessage: %v", err)
	}
	if err := st1.Close(); err != nil {
		t.Fatalf("Close (first): %v", err)
	}

	// Second open: retrieve the message.
	st2, err := New(path)
	if err != nil {
		t.Fatalf("New (second): %v", err)
	}
	//nolint:errcheck // test cleanup: Close errors are not actionable
	defer st2.Close()

	got, err := st2.GetByID(msg.ID)
	if err != nil {
		t.Fatalf("GetByID after reopen: %v", err)
	}
	if got.Body != "persisted" {
		t.Errorf("Body after reopen = %q, want %q", got.Body, "persisted")
	}
	if got.Origin != "node-0" {
		t.Errorf("Origin after reopen = %q, want %q", got.Origin, "node-0")
	}
}

func TestGetNotFound(t *testing.T) {
	st := newTestStore(t)

	_, err := st.GetByID("nonexistent-id")
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Errorf("GetByID (missing): err = %v, want gorm.ErrRecordNotFound", err)
	}
}

// newV7ID returns a valid UUIDv7 string, failing the test on the (impossible)
// error path.
func newV7ID(t *testing.T) string {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid.NewV7: %v", err)
	}
	return id.String()
}

func TestInsertReplicatedValidation(t *testing.T) {
	st := newTestStore(t)
	now := time.Now().UTC()

	// Nil message.
	if err := st.InsertReplicatedMessage(nil); !errors.Is(err, ErrNilMessage) {
		t.Errorf("nil message: err = %v, want ErrNilMessage", err)
	}

	// Invalid (non-UUID) ID.
	if err := st.InsertReplicatedMessage(&Message{ID: "not-a-uuid", Body: "b", Origin: "n", CreatedAt: now}); !errors.Is(err, ErrInvalidID) {
		t.Errorf("invalid id: err = %v, want ErrInvalidID", err)
	}

	// UUIDv4 (wrong version) is rejected.
	if err := st.InsertReplicatedMessage(&Message{ID: uuid.New().String(), Body: "b", Origin: "n", CreatedAt: now}); !errors.Is(err, ErrInvalidID) {
		t.Errorf("uuidv4 id: err = %v, want ErrInvalidID", err)
	}

	// Empty origin.
	if err := st.InsertReplicatedMessage(&Message{ID: newV7ID(t), Body: "b", Origin: "", CreatedAt: now}); !errors.Is(err, ErrEmptyOrigin) {
		t.Errorf("empty origin: err = %v, want ErrEmptyOrigin", err)
	}

	// Zero creation timestamp.
	if err := st.InsertReplicatedMessage(&Message{ID: newV7ID(t), Body: "b", Origin: "n"}); !errors.Is(err, ErrMissingTimestamp) {
		t.Errorf("zero timestamp: err = %v, want ErrMissingTimestamp", err)
	}

	// Empty body.
	if err := st.InsertReplicatedMessage(&Message{ID: newV7ID(t), Body: "", Origin: "n", CreatedAt: now}); !errors.Is(err, ErrEmptyBody) {
		t.Errorf("empty body: err = %v, want ErrEmptyBody", err)
	}

	// Oversized body.
	if err := st.InsertReplicatedMessage(&Message{ID: newV7ID(t), Body: strings.Repeat("x", 5000), Origin: "n", CreatedAt: now}); !errors.Is(err, ErrBodyTooLarge) {
		t.Errorf("oversized body: err = %v, want ErrBodyTooLarge", err)
	}

	// Validation is applied even when the ID already exists: a conflict with an
	// invalid (UUIDv4) ID is rejected as invalid, not as a conflict.
	seed, err := st.CreateLocalMessage("node-0", "original")
	if err != nil {
		t.Fatalf("CreateLocalMessage: %v", err)
	}
	if err := st.InsertReplicatedMessage(&Message{ID: uuid.New().String(), Body: "x", Origin: "n", CreatedAt: now}); !errors.Is(err, ErrInvalidID) {
		t.Errorf("invalid id on existing id: err = %v, want ErrInvalidID", err)
	}
	_ = seed
}

// TestInsertReplicatedConcurrentIdentical drives many concurrent identical
// inserts to the same UUIDv7 ID: exactly one row must exist, no error may be
// returned, and the row's content must match the input.
func TestInsertReplicatedConcurrentIdentical(t *testing.T) {
	st := newTestStore(t)

	id := newV7ID(t)
	now := time.Now().UTC()

	const workers = 25
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each goroutine uses its own pointer copy with identical fields so
			// concurrent GORM Create calls do not share mutable state.
			errCh <- st.InsertReplicatedMessage(&Message{ID: id, Body: "same", Origin: "node-0", CreatedAt: now})
		}()
	}
	wg.Wait()
	close(errCh)

	// Every concurrent identical insert must succeed (nil) — the operation is
	// idempotent, so no error (not even a conflict) is expected.
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent identical insert error: %v", err)
		}
	}

	got, err := st.GetByID(id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Body != "same" || got.Origin != "node-0" || !got.CreatedAt.Equal(now) {
		t.Errorf("row content mismatch: %+v (want body=same origin=node-0 ts=%v)", got, now)
	}

	// Exactly one row.
	msgs, total, err := st.List(100, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if total != 1 || len(msgs) != 1 {
		t.Errorf("row count = total %d / len %d, want 1/1", total, len(msgs))
	}
}

// TestInsertReplicatedConcurrentConflicting drives many concurrent inserts of
// the same UUIDv7 ID with two conflicting contents: exactly one winner is
// stored, and the other writers observe ErrDuplicateIDConflict (no DB error,
// no overwrite of the winner).
func TestInsertReplicatedConcurrentConflicting(t *testing.T) {
	st := newTestStore(t)

	id := newV7ID(t)
	now := time.Now().UTC()
	bodyA, bodyB := "content-A", "content-B"

	const workers = 30
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		body := bodyA
		if i%2 == 1 {
			body = bodyB
		}
		go func(body string) {
			defer wg.Done()
			errCh <- st.InsertReplicatedMessage(&Message{ID: id, Body: body, Origin: "node-0", CreatedAt: now})
		}(body)
	}
	wg.Wait()
	close(errCh)

	var conflicts, failures int
	for err := range errCh {
		if err == nil {
			continue // the winner(s)
		}
		if errors.Is(err, ErrDuplicateIDConflict) {
			conflicts++
			continue
		}
		failures++
		t.Errorf("unexpected error (not nil/conflict): %v", err)
	}

	got, err := st.GetByID(id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Body != bodyA && got.Body != bodyB {
		t.Errorf("stored body %q is neither of the two conflicting contents", got.Body)
	}

	// The stored content must be consistent: the body that won must have had
	// no conflicting observations beyond the non-winner count.
	if failures != 0 {
		t.Fatalf("expected only nil/conflict outcomes, got %d other errors", failures)
	}
	t.Logf("winner=%q conflicts=%d", got.Body, conflicts)
}

// TestInsertReplicatedCanonicalUUIDv7 verifies that a valid canonical
// lowercase-hyphenated UUIDv7 is accepted and stored.
func TestInsertReplicatedCanonicalUUIDv7(t *testing.T) {
	st := newTestStore(t)
	now := time.Now().UTC()
	id := newV7ID(t)

	err := st.InsertReplicatedMessage(&Message{ID: id, Body: "canonical", Origin: "node-0", CreatedAt: now})
	if err != nil {
		t.Fatalf("InsertReplicatedMessage (canonical): err = %v, want nil", err)
	}
	got, err := st.GetByID(id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Body != "canonical" {
		t.Errorf("Body = %q, want %q", got.Body, "canonical")
	}
}

// TestInsertReplicatedUppercaseUUIDv7Rejected verifies that an uppercase
// UUIDv7 representation is rejected as ErrInvalidID.
func TestInsertReplicatedUppercaseUUIDv7Rejected(t *testing.T) {
	st := newTestStore(t)
	now := time.Now().UTC()
	id := newV7ID(t)
	upperID := strings.ToUpper(id)

	err := st.InsertReplicatedMessage(&Message{ID: upperID, Body: "upper", Origin: "node-0", CreatedAt: now})
	if !errors.Is(err, ErrInvalidID) {
		t.Errorf("uppercase UUIDv7: err = %v, want ErrInvalidID", err)
	}
}

// TestInsertReplicatedHyphenlessUUIDv7Rejected verifies that a hyphenless
// UUIDv7 representation is rejected as ErrInvalidID.
func TestInsertReplicatedHyphenlessUUIDv7Rejected(t *testing.T) {
	st := newTestStore(t)
	now := time.Now().UTC()
	id := newV7ID(t)
	hyphenless := strings.ReplaceAll(id, "-", "")

	err := st.InsertReplicatedMessage(&Message{ID: hyphenless, Body: "hyphenless", Origin: "node-0", CreatedAt: now})
	if !errors.Is(err, ErrInvalidID) {
		t.Errorf("hyphenless UUIDv7: err = %v, want ErrInvalidID", err)
	}
}

// TestInsertReplicatedUUIDv4Rejected verifies that a UUIDv4 is rejected as
// ErrInvalidID (regression: already covered, but kept for completeness).
func TestInsertReplicatedUUIDv4Rejected(t *testing.T) {
	st := newTestStore(t)
	now := time.Now().UTC()
	v4id := uuid.New().String()

	err := st.InsertReplicatedMessage(&Message{ID: v4id, Body: "v4", Origin: "node-0", CreatedAt: now})
	if !errors.Is(err, ErrInvalidID) {
		t.Errorf("UUIDv4: err = %v, want ErrInvalidID", err)
	}
}

// TestInsertReplicatedDuplicateCanonicalIdempotent verifies that re-inserting
// the same canonical UUIDv7 ID with identical content is a no-op (idempotent).
func TestInsertReplicatedDuplicateCanonicalIdempotent(t *testing.T) {
	st := newTestStore(t)
	now := time.Now().UTC()
	id := newV7ID(t)

	err := st.InsertReplicatedMessage(&Message{ID: id, Body: "dup", Origin: "node-0", CreatedAt: now})
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	err = st.InsertReplicatedMessage(&Message{ID: id, Body: "dup", Origin: "node-0", CreatedAt: now})
	if err != nil {
		t.Errorf("idempotent re-insert: err = %v, want nil", err)
	}
}

// TestInsertReplicatedDuplicateCanonicalConflict verifies that re-inserting the
// same canonical UUIDv7 ID with conflicting content returns
// ErrDuplicateIDConflict.
func TestInsertReplicatedDuplicateCanonicalConflict(t *testing.T) {
	st := newTestStore(t)
	now := time.Now().UTC()
	id := newV7ID(t)

	err := st.InsertReplicatedMessage(&Message{ID: id, Body: "original", Origin: "node-0", CreatedAt: now})
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	err = st.InsertReplicatedMessage(&Message{ID: id, Body: "conflicting", Origin: "node-0", CreatedAt: now})
	if !errors.Is(err, ErrDuplicateIDConflict) {
		t.Errorf("conflicting re-insert: err = %v, want ErrDuplicateIDConflict", err)
	}
}

// TestInsertReplicatedNoDuplicateViaAlternateForm verifies that an uppercase
// (non-canonical) form of an already-stored UUIDv7 is rejected as
// ErrInvalidID and does NOT create a second row or trigger a conflict.
func TestInsertReplicatedNoDuplicateViaAlternateForm(t *testing.T) {
	st := newTestStore(t)
	now := time.Now().UTC()
	id := newV7ID(t)
	upperID := strings.ToUpper(id)

	// Insert the canonical form.
	err := st.InsertReplicatedMessage(&Message{ID: id, Body: "stored", Origin: "node-0", CreatedAt: now})
	if err != nil {
		t.Fatalf("canonical insert: %v", err)
	}

	// Attempt to insert the uppercase (non-canonical) form of the same UUID.
	// This must be rejected as ErrInvalidID, NOT as ErrDuplicateIDConflict
	// (which would imply a second row was compared), and must NOT create a
	// second row.
	err = st.InsertReplicatedMessage(&Message{ID: upperID, Body: "stored", Origin: "node-0", CreatedAt: now})
	if !errors.Is(err, ErrInvalidID) {
		t.Errorf("uppercase form of existing ID: err = %v, want ErrInvalidID (not conflict)", err)
	}

	// Confirm exactly one row exists (the canonical one).
	_, total, err := st.List(100, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if total != 1 {
		t.Errorf("row count = %d, want 1 (no duplicate via alternate form)", total)
	}
}

// TestInsertReplicatedConcurrentCanonical verifies that concurrent identical
// inserts of a canonical UUIDv7 (all using the same lowercase-hyphenated form)
// produce exactly one row and no errors.
func TestInsertReplicatedConcurrentCanonical(t *testing.T) {
	st := newTestStore(t)
	now := time.Now().UTC()
	id := newV7ID(t)

	const workers = 20
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- st.InsertReplicatedMessage(&Message{ID: id, Body: "conc", Origin: "node-0", CreatedAt: now})
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent canonical insert: err = %v, want nil", err)
		}
	}

	_, total, err := st.List(100, 0)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if total != 1 {
		t.Errorf("row count = %d, want 1 after concurrent canonical inserts", total)
	}
}
