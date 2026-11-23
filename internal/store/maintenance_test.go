package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// newMaintenanceStore creates a temp SQLite store for maintenance tests.
func newMaintenanceStore(t *testing.T) *Store {
	t.Helper()
	st, err := New(filepath.Join(t.TempDir(), "maintenance.db"))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() {
		//nolint:errcheck // test cleanup
		st.Close()
	})
	return st
}

// TestNewDatabaseAutoVacuumIncremental verifies that a newly created database
// has auto_vacuum=INCREMENTAL (mode 2) enabled by Store.New. This is the
// key precondition for incremental page reclamation: without it, deleted
// pages are not tracked on the freelist and cannot be reclaimed
// incrementally.
func TestNewDatabaseAutoVacuumIncremental(t *testing.T) {
	st := newMaintenanceStore(t)
	sqlDB, err := st.db.DB()
	if err != nil {
		t.Fatalf("db.DB: %v", err)
	}

	var mode int
	if err := sqlDB.QueryRowContext(context.Background(), "PRAGMA auto_vacuum").Scan(&mode); err != nil {
		t.Fatalf("PRAGMA auto_vacuum: %v", err)
	}
	if mode != 2 {
		t.Errorf("auto_vacuum mode = %d, want 2 (INCREMENTAL) for a new database", mode)
	}
}

// TestExistingDatabaseAutoVacuumNotConverted verifies that an existing
// database (one that already has a schema and data, created with
// auto_vacuum=NONE) is NOT silently converted to INCREMENTAL by Store.New.
// The test simulates this by creating a database with an explicit
// auto_vacuum=0, inserting a row, closing it, and re-opening it — the mode
// must remain 0.
func TestExistingDatabaseAutoVacuumNotConverted(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "existing.db")

	// Phase 1: create a database with auto_vacuum=0 (the default for
	// pre-maintenance databases), create the schema, and insert a row.
	db1, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	//nolint:errcheck // test: set WAL and busy timeout on the raw connection
	_, _ = db1.ExecContext(context.Background(), "PRAGMA journal_mode=WAL")
	//nolint:errcheck // test: set busy timeout on the raw connection
	_, _ = db1.ExecContext(context.Background(), "PRAGMA busy_timeout=5000")
	//nolint:errcheck // test: explicit auto_vacuum=0 to simulate a pre-maintenance DB
	_, _ = db1.ExecContext(context.Background(), "PRAGMA auto_vacuum = 0")
	if _, err := db1.ExecContext(context.Background(), "CREATE TABLE IF NOT EXISTS test_table (id TEXT PRIMARY KEY)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db1.ExecContext(context.Background(), "INSERT INTO test_table (id) VALUES ('row1')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	//nolint:errcheck // test cleanup
	db1.Close()

	// Phase 2: re-open with Store.New. The existing database (page_count > 0,
	// auto_vacuum=0) must NOT be converted.
	st := newMaintenanceStoreAt(t, dbPath)
	sqlDB, err := st.db.DB()
	if err != nil {
		t.Fatalf("db.DB: %v", err)
	}

	var mode int
	if err := sqlDB.QueryRowContext(context.Background(), "PRAGMA auto_vacuum").Scan(&mode); err != nil {
		t.Fatalf("PRAGMA auto_vacuum: %v", err)
	}
	if mode != 0 {
		t.Errorf("existing database auto_vacuum mode = %d, want 0 (not converted)", mode)
	}

	// The existing data must be preserved.
	var count int
	if err := sqlDB.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM test_table").Scan(&count); err != nil {
		t.Fatalf("SELECT COUNT: %v", err)
	}
	if count != 1 {
		t.Errorf("test_table count = %d, want 1 (data preserved)", count)
	}
}

// TestMaintenanceReclaimsPages verifies that RunMaintenance reclaims freelist
// pages on a database with auto_vacuum=INCREMENTAL. The test inserts and
// deletes rows to create freelist pages, then verifies that RunMaintenance
// reports the reclamation.
func TestMaintenanceReclaimsPages(t *testing.T) {
	st := newMaintenanceStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	// Insert 2000 rows (enough to create multiple pages), then delete half
	// of them to create freelist pages.
	for i := 0; i < 2000; i++ {
		ts := now.Add(-time.Duration(48+i) * time.Hour)
		id := retentionID(ts)
		if err := st.InsertReplicatedMessage(&Message{
			ID: id, Body: "maintenance-test", Origin: "n", CreatedAt: ts,
		}); err != nil {
			t.Fatalf("InsertReplicatedMessage: %v", err)
		}
	}

	// Delete half the rows (those with the oldest timestamps) to create
	// freelist pages.
	deleted, err := st.DeleteExpired(context.Background(), now.Add(-120*time.Hour), -1, nil)
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
	if deleted == 0 {
		t.Fatalf("DeleteExpired deleted 0 rows; expected some (old timestamps are expired)")
	}

	// Check the freelist count before maintenance.
	sqlDB, err := st.db.DB()
	if err != nil {
		t.Fatalf("db.DB: %v", err)
	}
	var freelistBefore int
	if err := sqlDB.QueryRowContext(context.Background(), "PRAGMA freelist_count").Scan(&freelistBefore); err != nil {
		t.Fatalf("PRAGMA freelist_count: %v", err)
	}
	if freelistBefore <= 0 {
		t.Fatalf("freelist_count = %d, want > 0 after deleting rows", freelistBefore)
	}

	// Run maintenance: it should reclaim some (or all) freelist pages.
	stats, err := st.RunMaintenance(context.Background())
	if err != nil {
		t.Fatalf("RunMaintenance: %v", err)
	}
	if stats.AutoVacuumMode != 2 {
		t.Errorf("auto_vacuum mode = %d, want 2 (INCREMENTAL)", stats.AutoVacuumMode)
	}
	if stats.PagesReclaimed <= 0 {
		t.Errorf("pages_reclaimed = %d, want > 0 (freelist pages exist)", stats.PagesReclaimed)
	}
	if stats.FreelistAfter >= stats.FreelistBefore {
		t.Errorf("freelist_after = %d >= freelist_before = %d; expected reduction",
			stats.FreelistAfter, stats.FreelistBefore)
	}

	// A second maintenance cycle should reclaim fewer (or no) additional pages.
	stats2, err := st.RunMaintenance(context.Background())
	if err != nil {
		t.Fatalf("second RunMaintenance: %v", err)
	}
	if stats2.PagesReclaimed > stats.PagesReclaimed {
		t.Errorf("second cycle reclaimed %d pages, more than first cycle's %d; unexpected",
			stats2.PagesReclaimed, stats.PagesReclaimed)
	}
}

// TestMaintenanceNoopWhenNoFreelist verifies that RunMaintenance on a database
// with no freelist pages (no deletions) is a no-op: it reports 0 pages
// reclaimed and does not error.
func TestMaintenanceNoopWhenNoFreelist(t *testing.T) {
	st := newMaintenanceStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	// Insert a few rows (no deletions, so no freelist pages).
	for i := 0; i < 10; i++ {
		ts := now.Add(-time.Duration(i+1) * time.Hour)
		id := retentionID(ts)
		if err := st.InsertReplicatedMessage(&Message{
			ID: id, Body: "no-deletions", Origin: "n", CreatedAt: ts,
		}); err != nil {
			t.Fatalf("InsertReplicatedMessage: %v", err)
		}
	}

	stats, err := st.RunMaintenance(context.Background())
	if err != nil {
		t.Fatalf("RunMaintenance: %v", err)
	}
	if stats.PagesReclaimed != 0 {
		t.Errorf("pages_reclaimed = %d, want 0 (no deletions, no freelist pages)", stats.PagesReclaimed)
	}
	if stats.FreelistBefore != 0 {
		t.Errorf("freelist_before = %d, want 0 (no deletions)", stats.FreelistBefore)
	}
}

// TestMaintenanceCancellation verifies that a cancelled context interrupts
// RunMaintenance. The test uses a pre-cancelled context and verifies that
// no error is returned and no reclamation occurs.
func TestMaintenanceCancellation(t *testing.T) {
	st := newMaintenanceStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	// Insert and delete rows to create freelist pages.
	for i := 0; i < 500; i++ {
		ts := now.Add(-time.Duration(48+i) * time.Hour)
		id := retentionID(ts)
		if err := st.InsertReplicatedMessage(&Message{
			ID: id, Body: "cancellation-test", Origin: "n", CreatedAt: ts,
		}); err != nil {
			t.Fatalf("InsertReplicatedMessage: %v", err)
		}
	}
	_, err := st.DeleteExpired(context.Background(), now.Add(-120*time.Hour), -1, nil)
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}

	// Pre-cancelled context: RunMaintenance must abort without error.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	stats, err := st.RunMaintenance(ctx)
	if err != nil {
		t.Errorf("RunMaintenance (cancelled): err = %v, want nil (cancellation is not a failure)", err)
	}
	if stats.PagesReclaimed > 0 {
		t.Errorf("RunMaintenance (cancelled): pages_reclaimed = %d, want 0 (pre-cancelled context)", stats.PagesReclaimed)
	}
}

// TestMaintenanceWALCheckpoint verifies that RunMaintenance performs a PASSIVE
// WAL checkpoint and reports the three values returned by
// PRAGMA wal_checkpoint(PASSIVE) (busy, log, checkpointed) with the correct
// success/partial/empty semantics.
//
// The test inserts rows (which write frames to the WAL) and then runs
// maintenance. After the inserts complete, there are outstanding WAL frames,
// so the checkpoint is expected to either be a full success (all frames
// checkpointed) or partial (some frames held by readers). Both are valid
// non-error outcomes; a "successful empty" checkpoint (log == 0) is also a
// success, not a failure. The test asserts the invariants rather than a
// specific count (the exact frame count is timing-dependent).
func TestMaintenanceWALCheckpoint(t *testing.T) {
	st := newMaintenanceStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	// Insert rows to generate WAL activity.
	for i := 0; i < 50; i++ {
		ts := now.Add(-time.Duration(i+1) * time.Hour)
		id := retentionID(ts)
		if err := st.InsertReplicatedMessage(&Message{
			ID: id, Body: "wal-test", Origin: "n", CreatedAt: ts,
		}); err != nil {
			t.Fatalf("InsertReplicatedMessage: %v", err)
		}
	}

	// Run maintenance: it must not error (a partial checkpoint is NOT an error)
	// and must report checkpoint status.
	stats, err := st.RunMaintenance(context.Background())
	if err != nil {
		t.Fatalf("RunMaintenance: %v", err)
	}
	if stats.DatabaseSize <= 0 {
		t.Errorf("database_size = %d, want > 0 (the database has data)", stats.DatabaseSize)
	}
	if stats.AutoVacuumMode != 2 {
		t.Errorf("auto_vacuum mode = %d, want 2 (INCREMENTAL)", stats.AutoVacuumMode)
	}
	// The checkpoint invariants (valid for all four outcomes: success, partial,
	// empty, or failed):
	//   - uncheckpointed = log - checkpointed (always);
	//   - checkpointed <= log (cannot checkpoint more than exist);
	//   - no failed checkpoint (no active readers/writers in this test);
	//   - OK XOR partial: exactly one of OK / partial is true (a failed
	//     checkpoint sets neither).
	if stats.WALUncheckpointedPages != stats.WALCheckpointLog-stats.WALCheckpointedPages {
		t.Errorf("uncheckpointed = %d, want log-checkpointed = %d",
			stats.WALUncheckpointedPages, stats.WALCheckpointLog-stats.WALCheckpointedPages)
	}
	if stats.WALCheckpointedPages > stats.WALCheckpointLog {
		t.Errorf("checkpointed = %d > log = %d (invalid)", stats.WALCheckpointedPages, stats.WALCheckpointLog)
	}
	if stats.WALCheckpointError != nil {
		t.Errorf("unexpected checkpoint error: %v", stats.WALCheckpointError)
	}
	if stats.WALCheckpointOK == stats.WALCheckpointPartial {
		t.Errorf("checkpoint OK=%v and partial=%v must be mutually exclusive (and one must be set for a non-failed checkpoint)",
			stats.WALCheckpointOK, stats.WALCheckpointPartial)
	}
}

// TestClassifyCheckpoint verifies the outcome classification of the three
// values returned by PRAGMA wal_checkpoint(PASSIVE) (busy, log, checkpointed).
// It is the regression for the "CheckpointOK requires checkpointed > 0" bug: a
// successful EMPTY checkpoint (log == 0) must be reported as OK (not a
// failure), a full SUCCESS (checkpointed == log > 0) is OK, a PARTIAL
// checkpoint (busy > 0 or checkpointed < log) is not OK and not failed, and a
// FAILED checkpoint (the statement errored) sets the error and neither flag.
func TestClassifyCheckpoint(t *testing.T) {
	cases := []struct {
		name         string
		busy, log    int
		checkpointed int
		err          error
		wantOK       bool
		wantPartial  bool
		wantErr      bool
	}{
		{name: "empty success", busy: 0, log: 0, checkpointed: 0, err: nil, wantOK: true, wantPartial: false, wantErr: false},
		{name: "full success", busy: 0, log: 10, checkpointed: 10, err: nil, wantOK: true, wantPartial: false, wantErr: false},
		{name: "partial busy", busy: 1, log: 10, checkpointed: 10, err: nil, wantOK: false, wantPartial: true, wantErr: false},
		{name: "partial incomplete", busy: 0, log: 10, checkpointed: 4, err: nil, wantOK: false, wantPartial: true, wantErr: false},
		{name: "failed", busy: 0, log: 10, checkpointed: 3, err: errors.New("sqlite busy"), wantOK: false, wantPartial: false, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stats MaintenanceStats
			classifyCheckpoint(&stats, tc.busy, tc.log, tc.checkpointed, tc.err)
			if stats.WALCheckpointOK != tc.wantOK {
				t.Errorf("WALCheckpointOK = %v, want %v", stats.WALCheckpointOK, tc.wantOK)
			}
			if stats.WALCheckpointPartial != tc.wantPartial {
				t.Errorf("WALCheckpointPartial = %v, want %v", stats.WALCheckpointPartial, tc.wantPartial)
			}
			if (stats.WALCheckpointError != nil) != tc.wantErr {
				t.Errorf("WALCheckpointError set = %v, want %v", stats.WALCheckpointError != nil, tc.wantErr)
			}
			if stats.WALUncheckpointedPages != int64(tc.log-tc.checkpointed) {
				t.Errorf("WALUncheckpointedPages = %d, want %d", stats.WALUncheckpointedPages, tc.log-tc.checkpointed)
			}
		})
	}
}

// TestMaintenanceJournalSizeLimitSet verifies that Store.New sets
// journal_size_limit to the expected value (64 MiB). This is a
// retained-file-size management setting (NOT a hard cap on WAL growth: the
// WAL can exceed it, particularly when checkpoint progress is obstructed),
// and is a key part of the WAL maintenance strategy.
func TestMaintenanceJournalSizeLimitSet(t *testing.T) {
	st := newMaintenanceStore(t)
	sqlDB, err := st.db.DB()
	if err != nil {
		t.Fatalf("db.DB: %v", err)
	}

	var limit int
	if err := sqlDB.QueryRowContext(context.Background(), "PRAGMA journal_size_limit").Scan(&limit); err != nil {
		t.Fatalf("PRAGMA journal_size_limit: %v", err)
	}
	if limit != walJournalSizeLimitBytes {
		t.Errorf("journal_size_limit = %d, want %d (64 MiB)", limit, walJournalSizeLimitBytes)
	}
}

// TestMaintenanceConcurrentWithWrites verifies that RunMaintenance is safe to
// call concurrently with message inserts (no error, no corruption).
func TestMaintenanceConcurrentWithWrites(t *testing.T) {
	st := newMaintenanceStore(t)
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	// Seed some rows.
	for i := 0; i < 100; i++ {
		ts := now.Add(-time.Duration(i+1) * time.Hour)
		id := retentionID(ts)
		if err := st.InsertReplicatedMessage(&Message{
			ID: id, Body: "concurrent", Origin: "n", CreatedAt: ts,
		}); err != nil {
			t.Fatalf("InsertReplicatedMessage: %v", err)
		}
	}

	// Run maintenance concurrently with fresh inserts.
	errCh := make(chan error, 20)
	//nolint:errcheck // test: go function error is observed via errCh
	go func() {
		_, err := st.RunMaintenance(context.Background())
		errCh <- err
	}()
	//nolint:errcheck // test: go function error is observed via errCh
	go func() {
		_, err := st.CreateLocalMessage("node-0", "concurrent-write")
		errCh <- err
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent operation error: %v", err)
		}
	}

	// The database must be in a consistent state.
	if _, total, err := st.ListWithExpiration(100, 0, time.Time{}); err != nil {
		t.Fatalf("ListWithExpiration: %v", err)
	} else if total < 100 {
		t.Errorf("total = %d, want >= 100 (all seeded rows retained)", total)
	}
}

// newMaintenanceStoreAt creates a temp SQLite store at a specific path (for
// the existing-database test).
func newMaintenanceStoreAt(t *testing.T, path string) *Store {
	t.Helper()
	st, err := New(path)
	if err != nil {
		t.Fatalf("store.New(%s): %v", path, err)
	}
	t.Cleanup(func() {
		//nolint:errcheck // test cleanup
		st.Close()
	})
	return st
}
