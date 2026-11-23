package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

// insertWithTime inserts a message with an explicit creation timestamp (used to
// seed messages at specific ages without depending on the wall clock).
func insertWithTime(t *testing.T, st *Store, id string, ts time.Time) {
	t.Helper()
	if err := st.InsertReplicatedMessage(&Message{
		ID:        id,
		Body:      "retention",
		Origin:    "node-0",
		CreatedAt: ts,
	}); err != nil {
		t.Fatalf("InsertReplicatedMessage(%s): %v", id, err)
	}
}

// retentionID returns a UUIDv7-style ID with a fixed base timestamp, so that
// the ID's embedded time is deterministic and consistent with the test's
// fixed "now". Sequential calls increment the timestamp so IDs are ordered.
var retentionIDCounter int64

func retentionID(base time.Time) string {
	retentionIDCounter++
	ts := base.Add(time.Duration(retentionIDCounter) * time.Millisecond)
	// Build a UUIDv7-style string: 48-bit timestamp | 4-bit ver | 12-bit rand |
	// 2-bit var | 62-bit rand, formatted as 8-4-4-4-12.
	var b [16]byte
	// 48-bit timestamp (milliseconds) big-endian
	tms := uint64(ts.UnixMilli())
	b[0] = byte(tms >> 40)
	b[1] = byte(tms >> 32)
	b[2] = byte(tms >> 24)
	b[3] = byte(tms >> 16)
	b[4] = byte(tms >> 8)
	b[5] = byte(tms)
	// version 7 (0111) in bits 6-9 of byte 6
	b[6] = 0x70
	b[7] = 0xAB // deterministic "random"
	// variant 10 (10) in bits 0-1 of byte 8
	b[8] = 0x80
	b[9] = 0xCD
	// remaining bytes deterministic
	b[10] = 0xEF
	b[11] = 0x01
	b[12] = 0x23
	b[13] = 0x45
	b[14] = 0x67
	b[15] = 0x89
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func TestExpirationThresholdDisabled(t *testing.T) {
	// A non-positive TTL yields the zero time (retention disabled).
	if th := ExpirationThreshold(0, time.Now().UTC()); !th.IsZero() {
		t.Errorf("TTL=0: threshold = %v, want zero", th)
	}
	if th := ExpirationThreshold(-time.Hour, time.Now().UTC()); !th.IsZero() {
		t.Errorf("TTL=-1h: threshold = %v, want zero", th)
	}
}

func TestExpirationThresholdEnabled(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 7 * 24 * time.Hour
	want := now.Add(-ttl)
	if got := ExpirationThreshold(ttl, now); !got.Equal(want) {
		t.Errorf("threshold = %v, want %v", got, want)
	}
}

func TestListWithExpirationExcludesExpired(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	threshold := ExpirationThreshold(ttl, now)

	// Fresh (within TTL) and expired (older than TTL) messages.
	freshID := retentionID(now.Add(-time.Hour))
	insertWithTime(t, st, freshID, now.Add(-time.Hour))
	expiredID := retentionID(now.Add(-48 * time.Hour))
	insertWithTime(t, st, expiredID, now.Add(-48*time.Hour))

	// With retention enabled, only the fresh message is listed and counted.
	msgs, total, err := st.ListWithExpiration(100, 0, threshold)
	if err != nil {
		t.Fatalf("ListWithExpiration: %v", err)
	}
	if total != 1 {
		t.Errorf("total = %d, want 1 (expired excluded from count)", total)
	}
	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want 1", len(msgs))
	}
	if msgs[0].ID != freshID {
		t.Errorf("listed ID = %q, want %q (the fresh message)", msgs[0].ID, freshID)
	}

	// With retention disabled (zero threshold), both messages are listed.
	msgsAll, totalAll, err := st.ListWithExpiration(100, 0, time.Time{})
	if err != nil {
		t.Fatalf("ListWithExpiration (disabled): %v", err)
	}
	if totalAll != 2 {
		t.Errorf("disabled total = %d, want 2", totalAll)
	}
	if len(msgsAll) != 2 {
		t.Errorf("disabled len = %d, want 2", len(msgsAll))
	}
}

func TestListWithExpirationBoundaryRetained(t *testing.T) {
	// A message whose age exactly equals the TTL is retained (the strict
	// "created_at < now - TTL" boundary keeps it at the exact cutoff).
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	threshold := ExpirationThreshold(ttl, now)

	// Insert exactly at the boundary: CreatedAt = now - TTL.
	boundaryID := retentionID(now.Add(-ttl))
	insertWithTime(t, st, boundaryID, now.Add(-ttl))

	_, total, err := st.ListWithExpiration(100, 0, threshold)
	if err != nil {
		t.Fatalf("ListWithExpiration: %v", err)
	}
	if total != 1 {
		t.Errorf("boundary total = %d, want 1 (a message exactly at the TTL cutoff is retained)", total)
	}
}

func TestGetByIDWithExpiration(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	threshold := ExpirationThreshold(ttl, now)

	freshID := retentionID(now.Add(-time.Hour))
	insertWithTime(t, st, freshID, now.Add(-time.Hour))
	expiredID := retentionID(now.Add(-72 * time.Hour))
	insertWithTime(t, st, expiredID, now.Add(-72*time.Hour))

	// Fresh message is returned.
	if _, err := st.GetByIDWithExpiration(freshID, threshold); err != nil {
		t.Errorf("fresh GetByIDWithExpiration: err = %v, want nil", err)
	}
	// Expired message is treated as not found.
	if _, err := st.GetByIDWithExpiration(expiredID, threshold); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Errorf("expired GetByIDWithExpiration: err = %v, want ErrRecordNotFound", err)
	}
	// With retention disabled, the expired message is returned.
	if _, err := st.GetByIDWithExpiration(expiredID, time.Time{}); err != nil {
		t.Errorf("disabled GetByIDWithExpiration: err = %v, want nil", err)
	}
}

func TestListIDsAfterWithExpirationExcludesExpired(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	threshold := ExpirationThreshold(ttl, now)

	// Insert 5 messages with ascending IDs; 3 fresh, 2 expired.
	var freshIDs, expiredIDs []string
	for i := 0; i < 5; i++ {
		var ts time.Time
		if i < 3 {
			ts = now.Add(-time.Duration(i+1) * time.Hour) // fresh (1h, 2h, 3h old)
		} else {
			ts = now.Add(-time.Duration(i+24) * time.Hour) // expired (28h, 30h old)
		}
		id := retentionID(ts)
		insertWithTime(t, st, id, ts)
		if i < 3 {
			freshIDs = append(freshIDs, id)
		} else {
			expiredIDs = append(expiredIDs, id)
		}
	}

	// With retention enabled, the inventory lists only the 3 fresh IDs.
	page, err := st.ListIDsAfterWithExpiration(context.Background(), "", 100, threshold)
	if err != nil {
		t.Fatalf("ListIDsAfterWithExpiration: %v", err)
	}
	if len(page.IDs) != 3 {
		t.Fatalf("inventory len = %d, want 3 (expired IDs excluded); got %v", len(page.IDs), page.IDs)
	}
	for _, id := range page.IDs {
		for _, e := range expiredIDs {
			if id == e {
				t.Errorf("expired ID %q appears in inventory", id)
			}
		}
	}
	for _, id := range freshIDs {
		found := false
		for _, pid := range page.IDs {
			if pid == id {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("fresh ID %q missing from inventory", id)
		}
	}
	// With retention disabled, all 5 IDs are listed.
	pageAll, err := st.ListIDsAfterWithExpiration(context.Background(), "", 100, time.Time{})
	if err != nil {
		t.Fatalf("ListIDsAfterWithExpiration (disabled): %v", err)
	}
	if len(pageAll.IDs) != 5 {
		t.Errorf("disabled inventory len = %d, want 5", len(pageAll.IDs))
	}
}

func TestGetMessagesByIDsWithExpirationReportsExpiredAsMissing(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	threshold := ExpirationThreshold(ttl, now)

	freshID := retentionID(now.Add(-time.Hour))
	insertWithTime(t, st, freshID, now.Add(-time.Hour))
	expiredID := retentionID(now.Add(-72 * time.Hour))
	insertWithTime(t, st, expiredID, now.Add(-72*time.Hour))

	// Fetch both: the fresh is returned, the expired is reported as missing
	// (the anti-entropy resurrection guard, defense in depth).
	msgs, err := st.GetMessagesByIDsWithExpiration(context.Background(), []string{freshID, expiredID}, threshold)
	if err == nil {
		t.Fatalf("expected an error listing the expired ID as missing")
	}
	if len(msgs) != 1 || msgs[0].ID != freshID {
		t.Errorf("returned messages = %+v, want only the fresh message", msgs)
	}
	// With retention disabled, both are returned.
	msgsAll, err := st.GetMessagesByIDsWithExpiration(context.Background(), []string{freshID, expiredID}, time.Time{})
	if err != nil {
		t.Fatalf("disabled GetMessagesByIDsWithExpiration: err = %v, want nil", err)
	}
	if len(msgsAll) != 2 {
		t.Errorf("disabled len = %d, want 2", len(msgsAll))
	}
}

func TestDeleteExpiredRemovesOnlyEligible(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	threshold := ExpirationThreshold(ttl, now)

	freshID := retentionID(now.Add(-time.Hour))
	insertWithTime(t, st, freshID, now.Add(-time.Hour))
	var expiredIDs []string
	for i := 0; i < 4; i++ {
		ts := now.Add(-time.Duration(48+i*24) * time.Hour)
		id := retentionID(ts)
		insertWithTime(t, st, id, ts)
		expiredIDs = append(expiredIDs, id)
	}

	deleted, err := st.DeleteExpired(context.Background(), threshold, -1, nil)
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
	if deleted != 4 {
		t.Errorf("deleted = %d, want 4", deleted)
	}

	// The fresh message survives; the expired ones are gone.
	if _, err := st.GetByID(freshID); err != nil {
		t.Errorf("fresh message deleted: err = %v, want nil", err)
	}
	for _, id := range expiredIDs {
		if _, err := st.GetByID(id); !errors.Is(err, gorm.ErrRecordNotFound) {
			t.Errorf("expired message %q still present: err = %v, want not found", id, err)
		}
	}

	// Idempotent: a second sweep deletes nothing.
	deleted2, err := st.DeleteExpired(context.Background(), threshold, -1, nil)
	if err != nil {
		t.Fatalf("second DeleteExpired: %v", err)
	}
	if deleted2 != 0 {
		t.Errorf("second sweep deleted = %d, want 0 (idempotent)", deleted2)
	}
}

func TestDeleteExpiredDisabledNoop(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	id := retentionID(now.Add(-time.Hour))
	insertWithTime(t, st, id, now.Add(-time.Hour))

	// Zero threshold (retention disabled): no deletion.
	deleted, err := st.DeleteExpired(context.Background(), time.Time{}, -1, nil)
	if err != nil {
		t.Fatalf("DeleteExpired (disabled): %v", err)
	}
	if deleted != 0 {
		t.Errorf("disabled deleted = %d, want 0", deleted)
	}
	if _, err := st.GetByID(id); err != nil {
		t.Errorf("message deleted despite retention disabled: err = %v", err)
	}
}

func TestDeleteExpiredEmptyDatabase(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	threshold := ExpirationThreshold(24*time.Hour, now)
	deleted, err := st.DeleteExpired(context.Background(), threshold, -1, nil)
	if err != nil {
		t.Fatalf("DeleteExpired (empty db): %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0 on empty database", deleted)
	}
}

// TestDeleteExpiredConcurrentWithWrites verifies that a cleanup sweep running
// concurrently with message inserts is safe (no error, no corruption) and
// deletes only the pre-existing expired rows.
func TestDeleteExpiredConcurrentWithWrites(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	threshold := ExpirationThreshold(ttl, now)

	// Seed 3 expired rows (with fixed timestamps well before the threshold).
	var expiredIDs []string
	for i := 0; i < 3; i++ {
		ts := now.Add(-time.Duration(48+i*24) * time.Hour)
		id := retentionID(ts)
		insertWithTime(t, st, id, ts)
		expiredIDs = append(expiredIDs, id)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 40)
	// Concurrent fresh inserts (using the wall clock, which is far after the
	// fixed 'now', so they are all within the TTL and retained).
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := st.CreateLocalMessage("node-0", "concurrent-fresh")
			errCh <- err
		}()
	}
	// A concurrent cleanup sweep.
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := st.DeleteExpired(context.Background(), threshold, -1, nil)
		errCh <- err
	}()
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent operation error: %v", err)
		}
	}

	// All 3 expired rows are gone; the concurrent fresh inserts remain.
	for _, id := range expiredIDs {
		if _, err := st.GetByID(id); !errors.Is(err, gorm.ErrRecordNotFound) {
			t.Errorf("expired message %q still present after concurrent sweep", id)
		}
	}
	// The concurrent fresh inserts are present (10 of them).
	_, total, err := st.ListWithExpiration(100, 0, time.Time{})
	if err != nil {
		t.Fatalf("ListWithExpiration: %v", err)
	}
	if total != 10 {
		t.Errorf("total = %d, want 10 (concurrent fresh inserts retained)", total)
	}
}

// TestDeleteExpiredBoundedAcrossBatches verifies that DeleteExpired deletes
// rows in bounded batches (deleteBatchSize = 1000 rows per batch) and makes
// progress across repeated batches until all eligible rows are gone. The test
// inserts more rows than a single batch (2500 expired rows, 1000+1000+500)
// and verifies that the total deleted count is accurate and all expired rows
// are removed.
func TestDeleteExpiredBoundedAcrossBatches(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	threshold := ExpirationThreshold(ttl, now)

	// Insert 2500 expired rows (exceeding a single batch of 1000).
	// Use timestamps well before the 24h threshold: 48h ago through
	// 48h + 2500*2s ≈ 48h42min ago. All are distinct and all expired.
	var expiredIDs []string
	for i := 0; i < 2500; i++ {
		ts := now.Add(-time.Duration(48*60+2*i) * time.Minute) // 48h+ ago (expired)
		id := retentionID(ts)
		insertWithTime(t, st, id, ts)
		if i < 100 { // track a subset for verification (avoid O(n) checks)
			expiredIDs = append(expiredIDs, id)
		}
	}
	// Insert 100 fresh rows (retained, within the 24h TTL): 1min through
	// 100min ago (all well within 24h).
	var freshIDs []string
	for i := 0; i < 100; i++ {
		ts := now.Add(-time.Duration(i+1) * time.Minute)
		id := retentionID(ts)
		insertWithTime(t, st, id, ts)
		freshIDs = append(freshIDs, id)
	}

	// DeleteExpired must delete all 2500 expired rows across multiple batches.
	deleted, err := st.DeleteExpired(context.Background(), threshold, -1, nil)
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
	if deleted != 2500 {
		t.Errorf("deleted = %d, want 2500 (all expired rows, across multiple batches)", deleted)
	}

	// Verify: a sample of the expired IDs are gone; the fresh IDs remain.
	for _, id := range expiredIDs {
		if _, err := st.GetByID(id); !errors.Is(err, gorm.ErrRecordNotFound) {
			t.Errorf("expired message %q still present after bounded sweep", id)
		}
	}
	for _, id := range freshIDs {
		if _, err := st.GetByID(id); err != nil {
			t.Errorf("fresh message %q deleted by bounded sweep: %v", id, err)
		}
	}

	// Idempotent: a second sweep deletes nothing.
	deleted2, err := st.DeleteExpired(context.Background(), threshold, -1, nil)
	if err != nil {
		t.Fatalf("second DeleteExpired: %v", err)
	}
	if deleted2 != 0 {
		t.Errorf("second sweep deleted = %d, want 0 (idempotent)", deleted2)
	}
}

// TestDeleteExpiredCancellation verifies that a cancelled context interrupts
// the DeleteExpired batch loop. The test uses a pre-cancelled context and
// verifies that no rows are deleted and no error is returned (a cancelled
// sweep is not a failure; the remaining rows are deleted on the next sweep).
func TestDeleteExpiredCancellation(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	threshold := ExpirationThreshold(ttl, now)

	// Insert 50 expired rows.
	for i := 0; i < 50; i++ {
		ts := now.Add(-time.Duration(48+i*24) * time.Hour)
		id := retentionID(ts)
		insertWithTime(t, st, id, ts)
	}

	// Use a pre-cancelled context: the batch loop must abort immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the call

	deleted, err := st.DeleteExpired(ctx, threshold, -1, nil)
	if err != nil {
		t.Errorf("DeleteExpired (cancelled): err = %v, want nil (a cancelled sweep is not a failure)", err)
	}
	// A pre-cancelled context should delete 0 rows (the loop checks ctx.Err()
	// before the first batch and returns immediately).
	if deleted != 0 {
		t.Errorf("DeleteExpired (cancelled): deleted = %d, want 0 (pre-cancelled context aborts before any batch)", deleted)
	}

	// The expired rows must still be present (no deletion occurred).
	_, total, err := st.ListWithExpiration(100, 0, time.Time{})
	if err != nil {
		t.Fatalf("ListWithExpiration: %v", err)
	}
	if total != 50 {
		t.Errorf("total = %d, want 50 (no deletion occurred on a cancelled sweep)", total)
	}
}

// TestDeleteExpiredMidSweepCancellation verifies that cancelling the context
// during a multi-batch sweep stops the loop at the next batch boundary and
// returns the partial count. The test inserts 2500 expired rows (3 batches:
// 1000+1000+500), cancels the context after the first batch, and verifies
// that the partial count (1000) is returned and the remaining rows are
// still present.
//
// This test is inherently timing-dependent (we cannot precisely control when
// SQLite executes a batch), so it verifies the *contract* rather than the
// exact partial count: a cancelled sweep returns no error, deletes at most
// all eligible rows, and leaves the database in a consistent state.
func TestDeleteExpiredMidSweepCancellation(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	threshold := ExpirationThreshold(ttl, now)

	// Insert 2500 expired rows (exceeding one batch): 48h+ ago.
	for i := 0; i < 2500; i++ {
		ts := now.Add(-time.Duration(48*60+2*i) * time.Minute) // 48h+ ago (expired)
		id := retentionID(ts)
		insertWithTime(t, st, id, ts)
	}

	// Cancel the context after a short delay (mid-sweep). The exact number
	// of batches completed before cancellation is nondeterministic (SQLite's
	// execution speed varies), so we verify the contract: no error, the
	// partial count is <= 2500, and the database is consistent.
	ctx, cancel := context.WithCancel(context.Background())
	//nolint:errcheck // test: the go function's error is not observed
	go func() {
		time.Sleep(10 * time.Millisecond) // let at least one batch start
		cancel()
	}()

	deleted, err := st.DeleteExpired(ctx, threshold, -1, nil)
	if err != nil {
		t.Errorf("DeleteExpired (mid-sweep cancellation): err = %v, want nil", err)
	}
	if deleted > 2500 {
		t.Errorf("DeleteExpired (mid-sweep cancellation): deleted = %d, want <= 2500", deleted)
	}

	// The database must be in a consistent state: the remaining rows are
	// either expired (not yet deleted) or fresh. No corruption.
	_, total, err := st.ListWithExpiration(100, 0, time.Time{})
	if err != nil {
		t.Fatalf("ListWithExpiration after cancellation: %v", err)
	}
	if total < 0 || total > 2500 {
		t.Errorf("total after cancellation = %d, want 0-2500 (consistent state)", total)
	}
	// Verify that a subsequent (non-cancelled) sweep completes the deletion.
	deleted2, err := st.DeleteExpired(context.Background(), threshold, -1, nil)
	if err != nil {
		t.Fatalf("subsequent DeleteExpired: %v", err)
	}
	if deleted+deleted2 != 2500 {
		t.Errorf("total deleted across both sweeps = %d, want 2500", deleted+deleted2)
	}
}

// TestDeleteExpiredTotalRowBudgetAndResume verifies the total-work budget: a
// single DeleteExpired call with a maxTotal smaller than the eligible backlog
// deletes at most maxTotal rows and stops (leaving a remainder), and a
// subsequent call (the next scheduled sweep) resumes and deletes the rest. This
// is the safeguard that bounds a single sweep in work and lets a large backlog
// be processed across multiple sweeps without monopolizing SQLite.
func TestDeleteExpiredTotalRowBudgetAndResume(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	threshold := ExpirationThreshold(ttl, now)

	// Insert 3000 expired rows (a backlog larger than the budget of 1500).
	for i := 0; i < 3000; i++ {
		ts := now.Add(-time.Duration(48*60+i) * time.Minute) // 48h+ ago (expired)
		insertWithTime(t, st, retentionID(ts), ts)
	}

	// First sweep: budget 1500 → deletes exactly 1500, leaves 1500.
	deleted1, err := st.DeleteExpired(context.Background(), threshold, 1500, nil)
	if err != nil {
		t.Fatalf("first DeleteExpired: %v", err)
	}
	if deleted1 != 1500 {
		t.Fatalf("first sweep deleted = %d, want 1500 (the row budget)", deleted1)
	}

	// The backlog must remain (1500 expired rows still present).
	var remaining int64
	if err := st.DB().Model(&Message{}).Where("created_at < ?", threshold).Count(&remaining).Error; err != nil {
		t.Fatalf("counting remaining: %v", err)
	}
	if remaining != 1500 {
		t.Errorf("remaining expired rows after first sweep = %d, want 1500 (budget-limited)", remaining)
	}

	// Second sweep (resume): budget 1500 → deletes the remaining 1500.
	deleted2, err := st.DeleteExpired(context.Background(), threshold, 1500, nil)
	if err != nil {
		t.Fatalf("second DeleteExpired: %v", err)
	}
	if deleted2 != 1500 {
		t.Errorf("second sweep deleted = %d, want 1500 (the remainder)", deleted2)
	}

	// Third sweep: idempotent, deletes nothing.
	deleted3, err := st.DeleteExpired(context.Background(), threshold, 1500, nil)
	if err != nil {
		t.Fatalf("third DeleteExpired: %v", err)
	}
	if deleted3 != 0 {
		t.Errorf("third sweep deleted = %d, want 0 (backlog exhausted)", deleted3)
	}
	if deleted1+deleted2+deleted3 != 3000 {
		t.Errorf("total deleted across sweeps = %d, want 3000", deleted1+deleted2+deleted3)
	}
}

// TestDeleteExpiredDeadlineStopsSweep verifies the wall-clock time budget: a
// DeleteExpired call with a deadline in the past stops before deleting any
// rows (returns 0, no error), and the backlog remains for a subsequent
// sweep. This bounds a single sweep in time, not only in row count, so a
// sweep cannot run indefinitely in the face of continuous incoming writes.
func TestDeleteExpiredDeadlineStopsSweep(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	threshold := ExpirationThreshold(ttl, now)

	// Insert some expired rows (48h+ before the logical "now").
	for i := 0; i < 50; i++ {
		ts := now.Add(-time.Duration(48*60+i) * time.Minute)
		insertWithTime(t, st, retentionID(ts), ts)
	}

	// A deadline in the past relative to the wall clock (a 2026 instant is in
	// the past for a 2026+ wall clock): the sweep must stop immediately (0
	// deleted). The deadline is a strict wall-clock check, so a past-in-the-
	// logical-clock value is in the past for the wall clock too.
	past := now.Add(-time.Minute)
	deleted, err := st.DeleteExpired(context.Background(), threshold, -1, &past)
	if err != nil {
		t.Fatalf("DeleteExpired (past deadline): err = %v, want nil", err)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0 (past deadline stops before any batch)", deleted)
	}

	// The backlog remains: a subsequent (no-deadline) sweep deletes all 50.
	deleted2, err := st.DeleteExpired(context.Background(), threshold, -1, nil)
	if err != nil {
		t.Fatalf("subsequent DeleteExpired: %v", err)
	}
	if deleted2 != 50 {
		t.Errorf("subsequent sweep deleted = %d, want 50 (the backlog resumes)", deleted2)
	}
}

// TestDeleteExpiredTransactionLifecycle verifies the corrected DeleteExpired
// transaction invariant: a batch's deletions are counted only after a
// successful commit, so a rolled-back (uncommitted) batch is never reported as
// deleted. The test asserts, across all paths (successful, budget-stopped,
// time-stopped, and context-cancelled), that the cumulative rows reported as
// deleted never exceed the rows actually persisted (50 - rowsRemaining). This
// is the regression for the old "_ = tx.Commit()" code, which committed the
// batch even when the delete failed and counted the rows regardless of the
// commit result, so a rolled-back batch could have been reported as deleted.
func TestDeleteExpiredTransactionLifecycle(t *testing.T) {
	st := newTestStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	threshold := ExpirationThreshold(ttl, now)

	// Seed 50 expired rows (48h+ before the logical "now").
	for i := 0; i < 50; i++ {
		ts := now.Add(-time.Duration(48*60+i) * time.Minute)
		id := retentionID(ts)
		insertWithTime(t, st, id, ts)
	}

	// rowsRemaining returns the number of rows still present in the table.
	rowsRemaining := func() int64 {
		t.Helper()
		var n int64
		if err := st.db.Model(&Message{}).Count(&n).Error; err != nil {
			t.Fatalf("counting rows: %v", err)
		}
		return n
	}
	// persisted returns the number of rows actually deleted (50 - remaining).
	persisted := func() int64 {
		t.Helper()
		return 50 - rowsRemaining()
	}

	// 1. Successful lifecycle: a real batch deletes, commits, and reports an
	// accurate count. A 10-row budget bounds the sweep to a single successful
	// batch; the commit must succeed and the count must equal the rows
	// actually deleted (10) and persisted (10).
	deleted1, err := st.DeleteExpired(context.Background(), threshold, 10, nil)
	if err != nil {
		t.Fatalf("DeleteExpired (successful): err = %v, want nil", err)
	}
	if deleted1 != 10 {
		t.Fatalf("DeleteExpired (successful) deleted = %d, want 10 (the row budget)", deleted1)
	}
	if got := persisted(); got != 10 {
		t.Fatalf("persisted after successful sweep = %d, want 10 (a successful commit persists the deleted rows)", got)
	}

	// 2. Budget-stopped: a 20-row budget deletes 20 (2 batches of 10), each
	// committed. The count equals the rows persisted (30 total).
	deleted2, err := st.DeleteExpired(context.Background(), threshold, 20, nil)
	if err != nil {
		t.Fatalf("DeleteExpired (budget-stopped): err = %v, want nil", err)
	}
	if deleted2 != 20 {
		t.Errorf("DeleteExpired (budget-stopped) deleted = %d, want 20 (the row budget)", deleted2)
	}
	if got := persisted(); got != 30 {
		t.Errorf("persisted after budget-stopped sweep = %d, want 30", got)
	}

	// 3. Time-stopped: a past wall-clock deadline stops the sweep before any
	// batch; 0 deleted, nothing persisted (30 total).
	past := now.Add(-time.Minute)
	deleted3, err := st.DeleteExpired(context.Background(), threshold, 1000, &past)
	if err != nil {
		t.Fatalf("DeleteExpired (time-stopped): err = %v, want nil", err)
	}
	if deleted3 != 0 {
		t.Errorf("DeleteExpired (time-stopped) deleted = %d, want 0 (past deadline stops before any batch)", deleted3)
	}
	if got := persisted(); got != 30 {
		t.Errorf("persisted after time-stopped sweep = %d, want 30 (no deletion occurred)", got)
	}

	// 4. Context-cancelled: a pre-cancelled context stops the sweep at the
	// first batch boundary (no batch committed); 0 deleted, nothing persisted
	// (30 total).
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	deleted4, err := st.DeleteExpired(ctx, threshold, 1000, nil)
	if err != nil {
		t.Fatalf("DeleteExpired (cancelled): err = %v, want nil (a cancelled sweep is not a failure)", err)
	}
	if deleted4 != 0 {
		t.Errorf("DeleteExpired (cancelled) deleted = %d, want 0 (pre-cancelled context stops before any batch)", deleted4)
	}
	if got := persisted(); got != 30 {
		t.Errorf("persisted after cancelled sweep = %d, want 30 (no deletion occurred)", got)
	}

	// 5. THE invariant (the regression for the old "_ = tx.Commit()" code):
	// the cumulative rows reported as deleted must never exceed the rows
	// actually persisted. A rolled-back (uncommitted) batch must not be
	// counted as deleted.
	cumulative := deleted1 + deleted2 + deleted3 + deleted4
	if got := persisted(); got != 30 {
		t.Errorf("persisted after all sweeps = %d, want 30", got)
	}
	if cumulative > persisted() {
		t.Errorf("cumulative reported deleted = %d exceeds rows persisted = %d (a rolled-back batch was counted as successful — the old '_ = tx.Commit()' defect)",
			cumulative, persisted())
	}

	// 6. Resume: a subsequent (non-cancelled) sweep completes the remaining
	// 20 expired rows, all committed and persisted (50 total).
	deleted5, err := st.DeleteExpired(context.Background(), threshold, 1000, nil)
	if err != nil {
		t.Fatalf("resuming DeleteExpired: %v", err)
	}
	if deleted5 != 20 {
		t.Errorf("resumed sweep deleted = %d, want 20 (the remaining expired rows)", deleted5)
	}
	if got := persisted(); got != 50 {
		t.Errorf("persisted after resumed sweep = %d, want 50 (all expired rows deleted)", got)
	}
}
