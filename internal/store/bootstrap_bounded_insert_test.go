// Tests for the InsertReplicatedMessagesBounded transactional commit boundary
// (Milestone 5B, Stage 2, Correction 1: single-transaction retention
// boundary). These tests use a REAL SQLite store (not fakes) to verify:
//
//  1. A batch of records is installed atomically: all new records are
//     committed in a single transaction (a partial commit is impossible).
//  2. An idempotent duplicate is a no-op (AlreadyPresent) without failing the
//     batch.
//  3. An immutable content conflict rolls the batch back (no records in the
//     batch are persisted; the local row is preserved).
//  4. An expired record (CreatedAt < threshold) is skipped (Expired) without
//     failing the batch.
//  5. A context cancellation during the transaction aborts it without a
//     partial commit.
//  6. Contention: a concurrent writer holding the SQLite write lock causes
//     the transaction to wait (up to the busy-timeout) and then fail or
//     succeed. The wait is bounded and is the only unavoidable database wait
//     in the commit-boundary contract.
//
// The linearization point is the COMMIT of the transaction: a concurrent
// revocation (which acquires the controller's write lock) is queued behind
// the commit's read locks. The SQLite write lock is a separate, inner
// lock: the transaction's COMMIT is the linearization point for the database
// content, and the commit guard's RLocks are the linearization point for the
// authorization. The two are composed (the transaction runs under the RLocks).

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"

	_ "gorm.io/driver/sqlite"
)

// newBoundedInsertTestStore opens a real SQLite store in a temp directory
// (named to avoid collision with newTestStore in store_test.go).
func newBoundedInsertTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	st, err := New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("opening test store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// TestBoundedInsert_AtomicBatch verifies that a batch of new records is
// installed atomically: all records are committed in a single transaction.
func TestBoundedInsert_AtomicBatch(t *testing.T) {
	st := newBoundedInsertTestStore(t)
	ctx := context.Background()
	now := time.Now()

	// A batch of 8 records (the maximum batch size).
	var msgs []*Message
	for i := 0; i < 8; i++ {
		msgs = append(msgs, &Message{
			ID:        fmt.Sprintf("019a0000-0000-7000-0000-%012d", i),
			Body:      fmt.Sprintf("body-%d", i),
			Origin:    "gossipgrid-0",
			CreatedAt: now,
		})
	}

	res, err := st.InsertReplicatedMessagesBounded(ctx, msgs, nil)
	if err != nil {
		t.Fatalf("expected batch insert to succeed, got: %v", err)
	}
	if len(res.Inserted) != 8 {
		t.Errorf("expected 8 inserted, got %d (%v)", len(res.Inserted), res.Inserted)
	}
	if len(res.AlreadyPresent) != 0 {
		t.Errorf("expected 0 already-present, got %d", len(res.AlreadyPresent))
	}
	if len(res.Conflict) != 0 {
		t.Errorf("expected 0 conflicts, got %d", len(res.Conflict))
	}
	if len(res.Expired) != 0 {
		t.Errorf("expected 0 expired, got %d", len(res.Expired))
	}

	// Verify the records are in the store.
	for i := 0; i < 8; i++ {
		var m Message
		if err := st.DB().Where("id = ?", msgs[i].ID).First(&m).Error; err != nil {
			t.Errorf("record %q not found in store: %v", msgs[i].ID, err)
		}
	}
}

// TestBoundedInsert_IdempotentDuplicate verifies that an identical duplicate
// is a no-op (AlreadyPresent) without failing the batch or modifying the
// existing row.
func TestBoundedInsert_IdempotentDuplicate(t *testing.T) {
	st := newBoundedInsertTestStore(t)
	ctx := context.Background()

	// Insert the record once.
	m := &Message{ID: "019a0000-0000-7000-0000-000000000001", Body: "body", Origin: "gossipgrid-0", CreatedAt: time.Now()}
	if _, err := st.InsertReplicatedMessageReturningInserted(m); err != nil {
		t.Fatalf("initial insert: %v", err)
	}

	// Insert the same record again (idempotent no-op).
	res, err := st.InsertReplicatedMessagesBounded(ctx, []*Message{m}, nil)
	if err != nil {
		t.Fatalf("expected idempotent insert to succeed, got: %v", err)
	}
	if len(res.AlreadyPresent) != 1 || res.AlreadyPresent[0] != m.ID {
		t.Errorf("expected [id] already-present, got %v", res.AlreadyPresent)
	}
	if len(res.Inserted) != 0 {
		t.Errorf("expected 0 inserted (duplicate), got %d", len(res.Inserted))
	}
	if len(res.Conflict) != 0 {
		t.Errorf("expected 0 conflicts (identical duplicate), got %d", len(res.Conflict))
	}
}

// TestBoundedInsert_ConflictRollsBackBatch verifies that an immutable content
// conflict (the local row has different content) rolls the entire batch back:
// no records in the batch are persisted, and the local row is preserved.
func TestBoundedInsert_ConflictRollsBackBatch(t *testing.T) {
	st := newBoundedInsertTestStore(t)
	ctx := context.Background()

	// Pre-insert id-1 with a specific body (the "local row").
	localRow := &Message{ID: "019a0000-0000-7000-0000-000000000001", Body: "LOCAL-BODY", Origin: "gossipgrid-1", CreatedAt: time.Now()}
	if _, err := st.InsertReplicatedMessageReturningInserted(localRow); err != nil {
		t.Fatalf("pre-insert local row: %v", err)
	}

	// A batch that includes id-1 with a DIFFERENT body (conflict) and id-2
	// (a new record that should be rolled back along with the conflict).
	batch := []*Message{
		{ID: "019a0000-0000-7000-0000-000000000001", Body: "REMOTE-BODY", Origin: "gossipgrid-0", CreatedAt: time.Now()}, // conflict
		{ID: "019a0000-0000-7000-0000-000000000002", Body: "body-2", Origin: "gossipgrid-0", CreatedAt: time.Now()},      // new (should be rolled back)
	}

	res, err := st.InsertReplicatedMessagesBounded(ctx, batch, nil)
	if err != nil {
		t.Fatalf("expected conflict to be reported without an error, got: %v", err)
	}
	if len(res.Conflict) != 1 || res.Conflict[0] != localRow.ID {
		t.Errorf("expected [id-1] conflict, got %v", res.Conflict)
	}
	// The new record (id-2) must NOT be persisted (the batch was rolled back).
	var m2 Message
	err = st.DB().Where("id = ?", batch[1].ID).First(&m2).Error
	if err == nil {
		t.Errorf("id-2 should NOT be persisted after a conflict rollback, but it was found")
	}
	// The local row (id-1) must be preserved (not modified by the remote body).
	var m1 Message
	if err := st.DB().Where("id = ?", localRow.ID).First(&m1).Error; err != nil {
		t.Fatalf("local row id-1 should be preserved: %v", err)
	}
	if m1.Body != "LOCAL-BODY" {
		t.Errorf("local row body should be preserved, got %q", m1.Body)
	}
}

// TestBoundedInsert_ExpiredSkipped verifies that an expired record
// (CreatedAt < threshold) is skipped (Expired) without failing the batch or
// storing the record.
func TestBoundedInsert_ExpiredSkipped(t *testing.T) {
	st := newBoundedInsertTestStore(t)
	ctx := context.Background()

	now := time.Now()
	threshold := now.Add(-1 * time.Hour) // 1-hour TTL

	// A batch with one fresh record and one expired record.
	batch := []*Message{
		{ID: "019a0000-0000-7000-0000-000000000001", Body: "fresh", Origin: "gossipgrid-0", CreatedAt: now},
		{ID: "019a0000-0000-7000-0000-000000000002", Body: "expired", Origin: "gossipgrid-0", CreatedAt: now.Add(-2 * time.Hour)},
	}

	res, err := st.InsertReplicatedMessagesBounded(ctx, batch, func() time.Time { return threshold })
	if err != nil {
		t.Fatalf("expected batch to complete (expired is not a failure), got: %v", err)
	}
	if len(res.Expired) != 1 || res.Expired[0] != batch[1].ID {
		t.Errorf("expected [id-2] expired, got %v", res.Expired)
	}
	if len(res.Inserted) != 1 || res.Inserted[0] != batch[0].ID {
		t.Errorf("expected [id-1] inserted, got %v", res.Inserted)
	}
	// The expired record must NOT be in the store.
	var m2 Message
	err = st.DB().Where("id = ?", batch[1].ID).First(&m2).Error
	if err == nil {
		t.Errorf("expired record should NOT be in the store, but it was found")
	}
}

// TestBoundedInsert_ContextCancellation verifies that a canceled context
// aborts the transaction without a partial commit: no records are persisted.
func TestBoundedInsert_ContextCancellation(t *testing.T) {
	st := newBoundedInsertTestStore(t)

	// Cancel the context before the insert.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var msgs []*Message
	for i := 0; i < 4; i++ {
		msgs = append(msgs, &Message{
			ID:        fmt.Sprintf("019a0000-0000-7000-0000-%012d", i),
			Body:      fmt.Sprintf("body-%d", i),
			Origin:    "gossipgrid-0",
			CreatedAt: time.Now(),
		})
	}

	_, err := st.InsertReplicatedMessagesBounded(ctx, msgs, nil)
	if err == nil {
		t.Fatal("expected error when context is canceled")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
	// No records should be persisted (the transaction was aborted).
	for i := 0; i < 4; i++ {
		var m Message
		if err := st.DB().Where("id = ?", msgs[i].ID).First(&m).Error; err == nil {
			t.Errorf("record %q should NOT be persisted after context cancellation, but it was found", msgs[i].ID)
		}
	}
}

// TestBoundedInsert_RealSQLiteWriteLockContention verifies that when a second
// connection to the SAME SQLite database file holds the write lock (via an
// open transaction with a write), the bounded insert's transaction waits for
// the write lock (up to the SQLite busy timeout of 5s) and then succeeds when
// the lock is released. This exercises actual SQLite write-lock contention,
// not a smoke test on a separate database file.
//
// The test opens a second *sql.DB connection to the same database file
// (bypassing the GORM pool, which is limited to 1 connection). The second
// connection begins a write transaction (CREATE TABLE IF NOT EXISTS holds the
// SQLite write lock). The bounded insert on the main store's GORM connection
// must wait for the write lock to be released.
//
// The linearization point is the COMMIT of the transaction: the SQLite write
// lock is a separate, inner lock. The test verifies:
//   - the insert succeeds after the write lock is released;
//   - the elapsed time is bounded (the write lock is released within the
//     busy timeout);
//   - the committed record accounting is accurate (4 inserted, 0 others).
func TestBoundedInsert_RealSQLiteWriteLockContention(t *testing.T) {
	st := newBoundedInsertTestStore(t)
	ctx := context.Background()

	// Open a second connection to the SAME database file (bypasses the GORM
	// pool, which is limited to 1 connection). The CGO sqlite3 driver
	// registers as "sqlite3".
	dbPath := st.dbPath
	writerDB, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("opening second SQLite connection: %v", err)
	}
	defer func() { _ = writerDB.Close() }()

	// The writer begins a write transaction on the SAME database. This
	// holds the SQLite write lock, causing the main store's transaction to
	// wait (contention on the same file).
	writerTx, err := writerDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("beginning writer transaction: %v", err)
	}

	// The writer does a write (holds the write lock on the shared DB).
	if _, err := writerTx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS _contention_test(x)"); err != nil {
		_ = writerTx.Rollback()
		t.Fatalf("writer write: %v", err)
	}

	// The writer commits after 300ms (releasing the write lock). This is
	// deterministic: the insert will block for ~300ms (the write lock is
	// held), then succeed. The 300ms is well within the 5s busy timeout.
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = writerTx.Commit()
	}()

	// Issue the bounded insert on the main store. The transaction's BEGIN
	// or first write will block on the SQLite write lock (contention).
	var msgs []*Message
	for i := 0; i < 4; i++ {
		msgs = append(msgs, &Message{
			ID:        fmt.Sprintf("019a0000-0000-7000-0000-%012d", i),
			Body:      fmt.Sprintf("body-%d", i),
			Origin:    "gossipgrid-0",
			CreatedAt: time.Now(),
		})
	}

	start := time.Now()
	res, err := st.InsertReplicatedMessagesBounded(ctx, msgs, nil)
	elapsed := time.Since(start)

	// The insert should succeed after the write lock is released.
	if err != nil {
		t.Fatalf("expected insert to succeed after write lock release, got: %v", err)
	}
	if len(res.Inserted) != 4 {
		t.Errorf("expected 4 inserted, got %v", res.Inserted)
	}
	if len(res.Conflict) != 0 {
		t.Errorf("expected 0 conflicts, got %v", res.Conflict)
	}
	if len(res.Expired) != 0 {
		t.Errorf("expected 0 expired, got %v", res.Expired)
	}

	// The operation should complete within a bounded time. The write lock
	// is released after 300ms, so the elapsed time should be ~300ms plus
	// the insert time, well within the 5s busy timeout.
	if elapsed > 5*time.Second {
		t.Errorf("insert took %v, expected bounded (write lock released within busy timeout)", elapsed)
	}
}

// TestBoundedInsert_ContextCancellationDuringWriteLockWait verifies the
// behavior when a context deadline expires while the bounded insert's
// transaction is waiting for the SQLite write lock.
//
// NOTE: SQLite's _busy_timeout (5000ms) is NOT context-aware. The CGO
// sqlite3 driver polls for the write lock for up to 5s regardless of
// context cancellation. The context cancellation IS honored by GORM at
// the query boundary (before the query starts, or at explicit checkpoints),
// but NOT during the SQLite internal busy-wait loop. This test verifies:
//   - the insert eventually fails (the lock is held longer than the context
//     deadline, so the operation fails — either by context if the driver
//     checks it, or by the 5s busy timeout);
//   - no partial commit occurred (the transaction was rolled back);
//   - the elapsed time is bounded (≤ 5s busy timeout + margin, not infinite).
//
// The test does NOT claim the context cancellation aborts the SQLite busy
// wait. It documents the actual behavior: the context deadline is not
// honored during the SQLite lock wait, but the operation still fails and
// no data is corrupted.
func TestBoundedInsert_ContextCancellationDuringWriteLockWait(t *testing.T) {
	st := newBoundedInsertTestStore(t)

	// Open a second connection to the SAME database file.
	dbPath := st.dbPath
	writerDB, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("opening second SQLite connection: %v", err)
	}
	defer func() { _ = writerDB.Close() }()

	// The writer holds the write lock (open transaction with a write)
	// INDEFINITELY (no commit). The insert's transaction will hit the
	// 5s busy timeout and fail with "database is locked".
	writerTx, err := writerDB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("beginning writer transaction: %v", err)
	}
	if _, err := writerTx.ExecContext(context.Background(), "CREATE TABLE IF NOT EXISTS _contention_cancel(x)"); err != nil {
		_ = writerTx.Rollback()
		t.Fatalf("writer write: %v", err)
	}
	// Roll back the writer transaction at the end (releases the lock).
	defer func() { _ = writerTx.Rollback() }()

	// Create a context with a short timeout (100ms) to document that the
	// context deadline is NOT honored during SQLite's busy wait.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var msgs []*Message
	for i := 0; i < 4; i++ {
		msgs = append(msgs, &Message{
			ID:        fmt.Sprintf("019a0000-0000-7000-0000-%012d", i),
			Body:      fmt.Sprintf("body-%d", i),
			Origin:    "gossipgrid-0",
			CreatedAt: time.Now(),
		})
	}

	start := time.Now()
	_, err = st.InsertReplicatedMessagesBounded(ctx, msgs, nil)
	elapsed := time.Since(start)

	// The insert must fail (the lock is held for the full 5s busy timeout,
	// which exceeds the 100ms context deadline). The error may be a
	// context error (if the driver checks ctx at a boundary) or a
	// "database is locked" error (if the busy timeout expires first).
	if err == nil {
		t.Fatal("expected error when the write lock is held beyond the context deadline, got nil")
	}
	// Document the actual error type (for the completion report).
	t.Logf("error: %v (elapsed: %v)", err, elapsed)

	// No records should be persisted (the transaction was rolled back).
	for i := 0; i < 4; i++ {
		var m Message
		if err := st.DB().Where("id = ?", msgs[i].ID).First(&m).Error; err == nil {
			t.Errorf("record %q should NOT be persisted after failed insert, but it was found", msgs[i].ID)
		}
	}

	// The operation should complete within a bounded time (≤ 5s busy timeout
	// + margin). It should NOT take longer than the busy timeout.
	if elapsed > 7*time.Second {
		t.Errorf("insert took %v, expected bounded by the 5s SQLite busy timeout", elapsed)
	}
}

// TestBoundedInsert_TransactionRollbackAccurateAccounting verifies that when a
// transaction is rolled back (due to a conflict or database error), the
// committed-record accounting is accurate: the result reports zero committed
// insertions and zero already-present records, even though the loop may have
// classified some records before the failure. Previously committed valid
// records (from prior batches) are preserved.
func TestBoundedInsert_TransactionRollbackAccurateAccounting(t *testing.T) {
	st := newBoundedInsertTestStore(t)
	ctx := context.Background()

	// Pre-insert a record that was committed in a prior batch (the
	// "previously committed valid record" that must be preserved).
	priorRecord := &Message{ID: "019a0000-0000-7000-0000-000000000001", Body: "prior", Origin: "gossipgrid-1", CreatedAt: time.Now()}
	if _, err := st.InsertReplicatedMessageReturningInserted(priorRecord); err != nil {
		t.Fatalf("pre-insert prior record: %v", err)
	}

	// A batch that includes a conflict (the local row has different content
	// for the same ID) and a new record. The conflict causes the transaction
	// to roll back; the new record is NOT persisted.
	localRow := &Message{ID: "019a0000-0000-7000-0000-000000000002", Body: "LOCAL", Origin: "gossipgrid-1", CreatedAt: time.Now()}
	if _, err := st.InsertReplicatedMessageReturningInserted(localRow); err != nil {
		t.Fatalf("pre-insert conflicting local row: %v", err)
	}
	batch := []*Message{
		{ID: "019a0000-0000-7000-0000-000000000002", Body: "REMOTE-CONFLICT", Origin: "gossipgrid-0", CreatedAt: time.Now()}, // conflict
		{ID: "019a0000-0000-7000-0000-000000000003", Body: "new", Origin: "gossipgrid-0", CreatedAt: time.Now()},             // new (rolled back)
	}

	res, err := st.InsertReplicatedMessagesBounded(ctx, batch, nil)
	if err != nil {
		t.Fatalf("expected conflict to be reported without an error, got: %v", err)
	}
	// The conflict must be reported.
	if len(res.Conflict) != 1 || res.Conflict[0] != localRow.ID {
		t.Errorf("expected [id-2] conflict, got %v", res.Conflict)
	}
	// Accurate accounting: zero committed insertions, zero already-present.
	if len(res.Inserted) != 0 {
		t.Errorf("expected 0 committed insertions (transaction rolled back), got %d: %v", len(res.Inserted), res.Inserted)
	}
	if len(res.AlreadyPresent) != 0 {
		t.Errorf("expected 0 already-present (transaction rolled back), got %d: %v", len(res.AlreadyPresent), res.AlreadyPresent)
	}

	// The new record (id-3) must NOT be in the store (rolled back).
	var m3 Message
	if err := st.DB().Where("id = ?", batch[1].ID).First(&m3).Error; err == nil {
		t.Errorf("id-3 should NOT be persisted after conflict rollback, but it was found")
	}

	// The previously committed valid record (id-1) must be preserved.
	var m1 Message
	if err := st.DB().Where("id = ?", priorRecord.ID).First(&m1).Error; err != nil {
		t.Errorf("prior record id-1 should be preserved: %v", err)
	}
	if m1.Body != "prior" {
		t.Errorf("prior record body should be preserved, got %q", m1.Body)
	}

	// The conflicting local row (id-2) must be preserved with its original body.
	var m2 Message
	if err := st.DB().Where("id = ?", localRow.ID).First(&m2).Error; err != nil {
		t.Errorf("local row id-2 should be preserved: %v", err)
	}
	if m2.Body != "LOCAL" {
		t.Errorf("local row body should be preserved, got %q", m2.Body)
	}
}

// TestBoundedInsert_StoreFunctionalAfterCancellation verifies that when the
// bounded insert is aborted due to a context cancellation, the store's single
// connection is not left in a broken state (subsequent operations on the
// store succeed). This is a store-level test: it verifies that a cancelled
// transaction does not corrupt the connection pool or leave the database in
// an inconsistent state.
func TestBoundedInsert_StoreFunctionalAfterCancellation(t *testing.T) {
	st := newBoundedInsertTestStore(t)

	// Cancel the context before the insert.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var msgs []*Message
	for i := 0; i < 4; i++ {
		msgs = append(msgs, &Message{
			ID:        fmt.Sprintf("019a0000-0000-7000-0000-%012d", i),
			Body:      fmt.Sprintf("body-%d", i),
			Origin:    "gossipgrid-0",
			CreatedAt: time.Now(),
		})
	}

	_, err := st.InsertReplicatedMessagesBounded(ctx, msgs, nil)
	if err == nil {
		t.Fatal("expected error when context is canceled")
	}

	// The store must still be functional after the cancellation: a
	// subsequent insert on a fresh context succeeds (the connection is not
	// left in a broken state, and the pool is intact).
	freshMsg := &Message{ID: "019a0000-0000-7000-0000-000000000009", Body: "post-cancel", Origin: "gossipgrid-0", CreatedAt: time.Now()}
	freshRes, err := st.InsertReplicatedMessagesBounded(context.Background(), []*Message{freshMsg}, nil)
	if err != nil {
		t.Fatalf("store should be functional after cancellation, got: %v", err)
	}
	if len(freshRes.Inserted) != 1 {
		t.Errorf("expected 1 inserted post-cancel, got %d", len(freshRes.Inserted))
	}
}

// TestBoundedInsert_SuccessAfterContendedResourceReleased verifies that when
// a contended resource (the SQLite write lock) is released while the bounded
// insert is waiting, the insert succeeds. This is the positive case: the
// contention is resolved by the writer completing, and the insert proceeds
// normally. The test uses a second connection to the same database file
// (real SQLite write-lock contention) and a short delay before the writer
// commits, ensuring the main store's transaction actually enters the wait.
func TestBoundedInsert_SuccessAfterContendedResourceReleased(t *testing.T) {
	st := newBoundedInsertTestStore(t)
	ctx := context.Background()

	// Open a second connection to the SAME database file.
	dbPath := st.dbPath
	writerDB, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("opening second SQLite connection: %v", err)
	}
	defer func() { _ = writerDB.Close() }()

	// The writer holds the write lock briefly.
	writerTx, err := writerDB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("beginning writer transaction: %v", err)
	}
	if _, err := writerTx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS _contention_success(x)"); err != nil {
		_ = writerTx.Rollback()
		t.Fatalf("writer write: %v", err)
	}

	// The writer releases the lock after a short delay (100ms), ensuring the
	// main store's transaction enters the write-lock wait and then proceeds.
	go func() {
		time.Sleep(100 * time.Millisecond)
		_ = writerTx.Commit()
	}()

	var msgs []*Message
	for i := 0; i < 4; i++ {
		msgs = append(msgs, &Message{
			ID:        fmt.Sprintf("019a0000-0000-7000-0000-%012d", i),
			Body:      fmt.Sprintf("body-%d", i),
			Origin:    "gossipgrid-0",
			CreatedAt: time.Now(),
		})
	}

	start := time.Now()
	res, err := st.InsertReplicatedMessagesBounded(ctx, msgs, nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected insert to succeed after contended resource released, got: %v", err)
	}
	if len(res.Inserted) != 4 {
		t.Errorf("expected 4 inserted, got %v", res.Inserted)
	}
	// The insert should complete shortly after the writer releases the lock.
	if elapsed > 10*time.Second {
		t.Errorf("insert took %v, expected to complete after 100ms delay + bounded write", elapsed)
	}
}

// TestBoundedInsert_C2Regression verifies the C2 fix: the first record is
// inserted successfully, the second record hits an immutable-content conflict,
// the transaction rolls back, the first record is absent from SQLite (it was
// in the same uncommitted transaction), the result reports zero committed
// insertions, and the original conflicting local record is unchanged.
func TestBoundedInsert_C2Regression(t *testing.T) {
	st := newBoundedInsertTestStore(t)
	ctx := context.Background()

	// Pre-insert the conflicting local record.
	localRow := &Message{ID: "019a0000-0000-7000-0000-000000000002", Body: "LOCAL-ORIGINAL", Origin: "gossipgrid-1", CreatedAt: time.Now()}
	if _, err := st.InsertReplicatedMessageReturningInserted(localRow); err != nil {
		t.Fatalf("pre-insert local row: %v", err)
	}

	// A batch where the FIRST record is new (would be inserted) and the
	// SECOND record conflicts with the pre-existing local row.
	batch := []*Message{
		{ID: "019a0000-0000-7000-0000-000000000001", Body: "new-record", Origin: "gossipgrid-0", CreatedAt: time.Now()},      // new (inserted first)
		{ID: "019a0000-0000-7000-0000-000000000002", Body: "REMOTE-CONFLICT", Origin: "gossipgrid-0", CreatedAt: time.Now()}, // conflict
	}

	res, err := st.InsertReplicatedMessagesBounded(ctx, batch, nil)
	if err != nil {
		t.Fatalf("expected conflict to be reported without an error, got: %v", err)
	}
	// The conflict must be reported.
	if len(res.Conflict) != 1 || res.Conflict[0] != localRow.ID {
		t.Errorf("expected [id-2] conflict, got %v", res.Conflict)
	}
	// C2: the result must report ZERO committed insertions — the first
	// record (id-1) was in the same uncommitted transaction and was rolled
	// back when the conflict was detected.
	if len(res.Inserted) != 0 {
		t.Errorf("C2: expected 0 committed insertions (transaction rolled back), got %d: %v", len(res.Inserted), res.Inserted)
	}
	if len(res.AlreadyPresent) != 0 {
		t.Errorf("C2: expected 0 already-present (transaction rolled back), got %d: %v", len(res.AlreadyPresent), res.AlreadyPresent)
	}

	// The first record (id-1) must be ABSENT from SQLite (it was rolled back).
	var m1 Message
	err = st.DB().Where("id = ?", batch[0].ID).First(&m1).Error
	if err == nil {
		t.Errorf("C2: first record should be ABSENT after rollback, but it was found")
	}

	// The original conflicting local record (id-2) must be UNCHANGED.
	var m2 Message
	if err := st.DB().Where("id = ?", localRow.ID).First(&m2).Error; err != nil {
		t.Fatalf("local row should be preserved: %v", err)
	}
	if m2.Body != "LOCAL-ORIGINAL" {
		t.Errorf("C2: local row body should be unchanged, got %q", m2.Body)
	}
}

// TestBoundedInsert_ConnectionPoolContention verifies that when the sole
// pooled connection is held by another goroutine, the store's
// SetMaxOpenConns(1) pool blocks the transaction's Begin until the
// connection is released. Pool statistics prove the connection was waited
// on. The insert succeeds after release.
func TestBoundedInsert_ConnectionPoolContention(t *testing.T) {
	st := newBoundedInsertTestStore(t)
	sqlDB, err := st.DB().DB()
	if err != nil {
		t.Fatalf("getting *sql.DB: %v", err)
	}
	ctx := context.Background()

	// Hold the sole pooled connection.
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatalf("acquiring pooled connection: %v", err)
	}

	// Begin a transaction on the held connection (acquires SQLite write lock).
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("beginning writer transaction on pooled conn: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS _pool_contention(x)"); err != nil {
		_ = tx.Rollback()
		_ = conn.Close()
		t.Fatalf("writer write: %v", err)
	}

	// Release the connection after a delay.
	releaseCh := make(chan struct{})
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = tx.Commit()
		_ = conn.Close()
		close(releaseCh)
	}()

	// Run the bounded insert in a goroutine; it will block on Begin
	// (the sole connection is occupied).
	var msgs []*Message
	for i := 0; i < 4; i++ {
		msgs = append(msgs, &Message{
			ID:        fmt.Sprintf("019a0000-0000-7000-0000-%012d", i+100),
			Body:      fmt.Sprintf("pool-contention-body-%d", i),
			Origin:    "gossipgrid-0",
			CreatedAt: time.Now(),
		})
	}
	insertErrCh := make(chan error, 1)
	go func() {
		_, ierr := st.InsertReplicatedMessagesBounded(ctx, msgs, nil)
		insertErrCh <- ierr
	}()

	// Wait a bit, then inspect pool stats to prove the insert is
	// waiting for a connection.
	time.Sleep(100 * time.Millisecond)
	stats := sqlDB.Stats()
	if stats.InUse != 1 {
		t.Fatalf("expected 1 connection in use (held by writer), got %d", stats.InUse)
	}
	if stats.WaitCount < 1 {
		t.Fatalf("expected WaitCount >= 1 (insert waiting for connection), got %d", stats.WaitCount)
	}
	if stats.WaitDuration < 0 {
		t.Fatalf("WaitDuration should be non-negative, got %v", stats.WaitDuration)
	}
	t.Logf("pool stats while contended: InUse=%d WaitCount=%d", stats.InUse, stats.WaitCount)

	// Wait for the insert to complete (after the connection is released).
	select {
	case err := <-insertErrCh:
		if err != nil {
			t.Fatalf("bounded insert should succeed after connection released, got: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("bounded insert did not complete within 10s")
	}
	<-releaseCh
}

// TestBoundedInsert_CancellationWhileWaitingForConnectionPool verifies that
// when the sole pooled connection is held and a context is cancelled while
// the bounded insert is waiting for the connection, the operation returns
// context.Canceled. The store must remain functional after the connection
// is released.
func TestBoundedInsert_CancellationWhileWaitingForConnectionPool(t *testing.T) {
	st := newBoundedInsertTestStore(t)
	sqlDB, err := st.DB().DB()
	if err != nil {
		t.Fatalf("getting *sql.DB: %v", err)
	}

	// Hold the sole pooled connection.
	bgCtx := context.Background()
	conn, err := sqlDB.Conn(bgCtx)
	if err != nil {
		t.Fatalf("acquiring pooled connection: %v", err)
	}
	tx, err := conn.BeginTx(bgCtx, nil)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("beginning writer transaction: %v", err)
	}
	if _, err := tx.ExecContext(bgCtx, "CREATE TABLE IF NOT EXISTS _cancel_pool(x)"); err != nil {
		_ = tx.Rollback()
		_ = conn.Close()
		t.Fatalf("writer write: %v", err)
	}

	// Cancel the insert context after a short delay.
	ctx, cancel := context.WithCancel(bgCtx)
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	// Run the bounded insert; it will block on Begin (connection held).
	freshMsg := &Message{ID: "019a0000-0000-7000-0000-000000000092", Body: "cancel-test", Origin: "gossipgrid-0", CreatedAt: time.Now()}
	start := time.Now()
	_, err = st.InsertReplicatedMessagesBounded(ctx, []*Message{freshMsg}, nil)
	elapsed := time.Since(start)

	// The context was cancelled while waiting for the connection.
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("insert took %v, expected prompt cancellation", elapsed)
	}

	// Release the writer.
	_ = tx.Commit()
	_ = conn.Close()

	// The store must remain functional after the cancellation.
	res, err := st.InsertReplicatedMessagesBounded(bgCtx, []*Message{freshMsg}, nil)
	if err != nil {
		t.Fatalf("store should be functional after cancellation, got: %v", err)
	}
	if len(res.Inserted) != 1 {
		t.Errorf("expected 1 inserted post-cancel, got %d", len(res.Inserted))
	}
}

// TestBoundedInsert_ExpirationWhileWaiting verifies that the retention
// threshold callback is evaluated AFTER the transaction acquires write
// access (not before). The test uses an injected clock: T0, TTL 1h,
// record created T0-1h+500ms (fresh at T0's threshold, expired at
// T0+1s's threshold). The clock is advanced to T0+1s BEFORE the
// contended resource is released. The callback must be evaluated after
// write access is obtained, yielding the advanced threshold, classifying
// the record as Expired. No row is inserted.
// This exercises both connection-pool contention and same-file write-lock
// contention (run via a helper parameterized on contention type).
func TestBoundedInsert_ExpirationWhileWaiting(t *testing.T) {
	t.Run("connection_pool", func(t *testing.T) {
		runExpirationWhileWaiting(t, "pool")
	})
	t.Run("write_lock", func(t *testing.T) {
		runExpirationWhileWaiting(t, "lock")
	})
}

func runExpirationWhileWaiting(t *testing.T, contentionType string) {
	t.Helper()
	st := newBoundedInsertTestStore(t)
	ctx := context.Background()

	// Injected clock: T0 → T0+1s when the test advances it.
	t0 := time.Now()
	var clockMu sync.Mutex
	var clockAdvanced bool
	clockNow := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		if clockAdvanced {
			return t0.Add(1 * time.Second)
		}
		return t0
	}

	// The record is created 500ms before the TTL boundary relative to T0.
	// At T0: threshold = T0 - 1h. CreatedAt = T0 - 1h + 500ms > threshold → fresh.
	// At T0+1s: threshold = T0+1s - 1h = T0 - 1h + 1s. CreatedAt < threshold → expired.
	retentionTTL := 1 * time.Hour
	msg := &Message{
		ID:        "019a0000-0000-7000-0000-000000000093",
		Body:      "boundary",
		Origin:    "gossipgrid-0",
		CreatedAt: t0.Add(-retentionTTL + 500*time.Millisecond),
	}

	var release func()
	var releaseErr error

	switch contentionType {
	case "pool":
		sqlDB, err := st.DB().DB()
		if err != nil {
			t.Fatalf("getting *sql.DB: %v", err)
		}
		conn, err := sqlDB.Conn(ctx)
		if err != nil {
			t.Fatalf("acquiring pooled connection: %v", err)
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			_ = conn.Close()
			t.Fatalf("beginning writer tx: %v", err)
		}
		if _, err := tx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS _exp_pool(x)"); err != nil {
			_ = tx.Rollback()
			_ = conn.Close()
			t.Fatalf("writer write: %v", err)
		}
		release = func() {
			_ = tx.Commit()
			_ = conn.Close()
		}
	case "lock":
		writerDB, err := sql.Open("sqlite3", st.dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
		if err != nil {
			t.Fatalf("opening second SQLite connection: %v", err)
		}
		wtx, err := writerDB.BeginTx(ctx, nil)
		if err != nil {
			_ = writerDB.Close()
			t.Fatalf("beginning writer tx: %v", err)
		}
		if _, err := wtx.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS _exp_lock(x)"); err != nil {
			_ = wtx.Rollback()
			_ = writerDB.Close()
			t.Fatalf("writer write: %v", err)
		}
		release = func() {
			_ = wtx.Commit()
			_ = writerDB.Close()
		}
	}

	// Run the bounded insert in a goroutine with the clock callback.
	insertErrCh := make(chan error, 1)
	go func() {
		_, ierr := st.InsertReplicatedMessagesBounded(ctx, []*Message{msg}, func() time.Time {
			return ExpirationThreshold(retentionTTL, clockNow())
		})
		insertErrCh <- ierr
	}()

	// Wait for the insert to be blocked (waiting for connection or write lock).
	time.Sleep(200 * time.Millisecond)

	// Advance the clock BEFORE releasing the contended resource. This
	// simulates 1s of real time passing during the wait.
	clockMu.Lock()
	clockAdvanced = true
	clockMu.Unlock()

	// Now release the contended resource.
	release()
	releaseErr = <-insertErrCh
	if releaseErr != nil {
		t.Fatalf("bounded insert should succeed after release, got: %v", releaseErr)
	}

	// Verify: the callback was evaluated AFTER write access was obtained
	// (at the advanced clock value T0+1s), so the record is classified
	// as Expired. No row is inserted.
	// At T0+1s: threshold = T0+1s - 1h = T0 - 1h + 1s.
	// msg.CreatedAt = T0 - 1h + 500ms < T0 - 1h + 1s → Expired.
	var m Message
	if err := st.db.Model(&m).Where("id = ?", msg.ID).First(&m).Error; err == nil {
		t.Fatal("record should NOT be inserted (classified as expired at advanced threshold)")
	}
}

// TestBoundedInsert_LifecycleAuthLockReleaseOnCancellation verifies that when
// a real lifecycle controller and real SQLite store are used together, a
// context cancellation during the bounded install (while waiting for the
// write lock or connection) causes the commit guard's RLocks to be released.
// After the cancellation, a subsequent state transition succeeds promptly,
// and a subsequent CommitUnderBootstrapAuthorization is rejected.
func TestBoundedInsert_LifecycleAuthLockReleaseOnCancellation(t *testing.T) {
	machine := lifecycle.NewMachine(nil)
	machine.ForceTransitionForTest(lifecycle.DISCOVERING, "test")
	if err := machine.TransitionForTest(lifecycle.SYNCING, "test"); err != nil {
		t.Fatalf("transition to SYNCING: %v", err)
	}

	st := newBoundedInsertTestStore(t)
	meta := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}
	if _, err := st.Bind(context.Background(), "c1", "e1", 1, lifecycle.ProvenanceGenesis); err != nil {
		t.Fatalf("binding store: %v", err)
	}

	ctrl := lifecycle.NewController(lifecycle.Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "gossipgrid-1",
		Enabled:              true,
	}, machine, lifecycle.NewFakeMetadataSourceUnavailable(), st, nil)
	ctrl.SetAcceptedMetadataForTest(meta)
	ctrl.SetBindingCacheForTest(&lifecycle.DatabaseBinding{
		ClusterID:  "c1",
		EpochID:    "e1",
		Generation: 1,
		CreatedAt:  time.Now(),
	})

	_, authed, err := ctrl.BootstrapAuthorizationSnapshot()
	if err != nil {
		t.Fatalf("BootstrapAuthorizationSnapshot: %v", err)
	}
	if !authed {
		t.Fatalf("precondition: controller should be bootstrap-authorized")
	}

	// Hold the sole pooled connection so the bounded insert's Begin blocks.
	sqlDB, err := st.DB().DB()
	if err != nil {
		t.Fatalf("getting *sql.DB: %v", err)
	}
	bgCtx := context.Background()
	conn, err := sqlDB.Conn(bgCtx)
	if err != nil {
		t.Fatalf("acquiring pooled connection: %v", err)
	}
	wtx, err := conn.BeginTx(bgCtx, nil)
	if err != nil {
		_ = conn.Close()
		t.Fatalf("beginning writer tx: %v", err)
	}
	if _, err := wtx.ExecContext(bgCtx, "CREATE TABLE IF NOT EXISTS _auth_cancel(x)"); err != nil {
		_ = wtx.Rollback()
		_ = conn.Close()
		t.Fatalf("writer write: %v", err)
	}

	// Run CommitUnderBootstrapAuthorization in a goroutine. The callback
	// calls the REAL bounded install, which blocks on the held connection.
	ctx, cancel := context.WithCancel(bgCtx)
	commitErrCh := make(chan error, 1)
	go func() {
		commitErrCh <- ctrl.CommitUnderBootstrapAuthorization(
			lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
			func() error {
				msg := &Message{ID: "019a0000-0000-7000-0000-000000000094", Body: "auth-cancel", Origin: "gossipgrid-0", CreatedAt: time.Now()}
				_, ierr := st.InsertReplicatedMessagesBounded(ctx, []*Message{msg}, nil)
				return ierr
			},
		)
	}()

	// Wait for the insert to enter the wait, then cancel.
	time.Sleep(200 * time.Millisecond)
	cancel()

	commitErr := <-commitErrCh
	if !errors.Is(commitErr, context.Canceled) {
		t.Fatalf("expected context.Canceled from commit guard, got: %v", commitErr)
	}

	// Release the writer.
	_ = wtx.Commit()
	_ = conn.Close()

	// After the cancellation, the controller's RLocks must be released.
	transStart := time.Now()
	if err := ctrl.SetRecoveryRequired("test cancellation"); err != nil {
		t.Fatalf("SetRecoveryRequired after cancellation should succeed (RLocks released), got: %v", err)
	}
	transElapsed := time.Since(transStart)
	if transElapsed > 1*time.Second {
		t.Errorf("SetRecoveryRequired took %v, expected prompt (RLocks should be released)", transElapsed)
	}

	// The bootstrap commit must now be rejected.
	ran := false
	err = ctrl.CommitUnderBootstrapAuthorization(
		lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		func() error { ran = true; return nil },
	)
	if err == nil {
		t.Fatal("CommitUnderBootstrapAuthorization should be rejected after SetRecoveryRequired")
	}
	if ran {
		t.Fatal("commit callback must not run after state transition to RECOVERY_REQUIRED")
	}
}
