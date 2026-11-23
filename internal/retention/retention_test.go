package retention

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"gossipgrid/internal/store"
)

// fakeSweeper implements the sweeper interface with a deterministic set of
// messages to delete and an optional error (for testing the failure path).
type fakeSweeper struct {
	mu        sync.Mutex
	deletedAt time.Time
	err       error
	calls     int
	// maintenanceErr is an optional error for RunMaintenance (independent
	// of DeleteExpired's err, so tests can exercise the maintenance failure
	// path without failing the cleanup phase).
	maintenanceErr error
	// checkpointStats, when non-zero, is returned by RunMaintenance (for
	// exercising the checkpoint-warning path). When zero, the default
	// stand-in stats (reclaim 5 pages, checkpoint OK) are returned.
	checkpointStats store.MaintenanceStats
}

func (f *fakeSweeper) DeleteExpired(_ context.Context, threshold time.Time, _ int64, _ *time.Time) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.deletedAt = threshold
	if f.err != nil {
		return 0, f.err
	}
	// A deterministic stand-in: always delete 3 rows per successful sweep.
	return 3, nil
}

func (f *fakeSweeper) RunMaintenance(_ context.Context) (store.MaintenanceStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.maintenanceErr != nil {
		return store.MaintenanceStats{}, f.maintenanceErr
	}
	if f.checkpointStats != (store.MaintenanceStats{}) {
		return f.checkpointStats, nil
	}
	// A deterministic stand-in: reclaim 5 pages per maintenance cycle.
	return store.MaintenanceStats{
		PagesReclaimed:  5,
		FreelistBefore:  50,
		FreelistAfter:   45,
		AutoVacuumMode:  2,
		WALCheckpointOK: true,
	}, nil
}

func (f *fakeSweeper) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeSweeper) threshold() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deletedAt
}

// newV7ID returns a valid canonical UUIDv7 string, failing the test on the
// (impossible) error path.
func newV7ID(t *testing.T) string {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid.NewV7: %v", err)
	}
	return id.String()
}

func newTestCleaner(t *testing.T, cfg Config, db sweeper) *Cleaner {
	t.Helper()
	c := New(cfg, db, slog.New(slog.NewTextHandler(testWriter(t), nil)))
	t.Cleanup(func() {
		//nolint:errcheck // test cleanup
		c.Stop(context.Background())
	})
	return c
}

// testWriter is a discard io.Writer for the test logger.
type testWriterT struct{}

func testWriter(t *testing.T) *testWriterT         { return &testWriterT{} }
func (w *testWriterT) Write(p []byte) (int, error) { return len(p), nil }

// captureWriter records log output lines for assertion in tests.
type captureWriter struct {
	mu    sync.Mutex
	lines []string
}

func (w *captureWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lines = append(w.lines, string(p))
	return len(p), nil
}

func (w *captureWriter) hasLineContaining(needle string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, l := range w.lines {
		if strings.Contains(l, needle) {
			return true
		}
	}
	return false
}

// TestCleanerDisabledNoSweep verifies that a cleaner with a zero TTL (retention
// disabled) starts and stops cleanly but never performs a sweep.
func TestCleanerDisabledNoSweep(t *testing.T) {
	db := &fakeSweeper{}
	c := newTestCleaner(t, Config{TTL: 0, Interval: time.Second}, db)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Give the (non-existent) worker a moment; no sweep must occur.
	time.Sleep(50 * time.Millisecond)
	if db.count() != 0 {
		t.Errorf("sweeps = %d, want 0 (retention disabled)", db.count())
	}
}

// TestCleanerInitialSweep verifies that an enabled cleaner performs an initial
// sweep immediately on Start (not after waiting a full interval), and that the
// sweep uses the correct threshold (now - TTL) from the injected clock.
func TestCleanerInitialSweep(t *testing.T) {
	db := &fakeSweeper{}
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	var tNow = now
	c := WithClock(Config{TTL: 7 * 24 * time.Hour, Interval: time.Hour}, db, slog.Default(), func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return tNow
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Wait for the initial sweep to run.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if db.count() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if db.count() < 1 {
		t.Fatalf("expected at least one initial sweep, got %d", db.count())
	}
	wantThreshold := now.Add(-7 * 24 * time.Hour)
	if got := db.threshold(); !got.Equal(wantThreshold) {
		t.Errorf("sweep threshold = %v, want %v (now - TTL)", got, wantThreshold)
	}
}

// TestCleanerPeriodicSweeps verifies that an enabled cleaner repeats sweeps on
// the configured interval (using a short interval so the test is fast).
func TestCleanerPeriodicSweeps(t *testing.T) {
	db := &fakeSweeper{}
	c := newTestCleaner(t, Config{TTL: time.Hour, Interval: 20 * time.Millisecond}, db)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Wait long enough for ~4 sweeps (initial + 3 ticks at 20ms).
	time.Sleep(150 * time.Millisecond)
	c.Stop(context.Background())
	if db.count() < 3 {
		t.Errorf("sweeps = %d, want at least 3 (initial + periodic ticks)", db.count())
	}
	if c.TotalDeleted() == 0 {
		t.Errorf("TotalDeleted = 0, want > 0 (3 rows per sweep)")
	}
}

// TestCleanerStopBeforeStart verifies that Stop before Start returns promptly
// and does not panic, and that a stopped cleaner cannot be restarted.
func TestCleanerStopBeforeStart(t *testing.T) {
	db := &fakeSweeper{}
	c := newTestCleaner(t, Config{TTL: time.Hour, Interval: time.Second}, db)
	c.Stop(context.Background()) // must not panic
	if err := c.Start(context.Background()); err == nil {
		t.Error("Start after Stop-before-Start: expected an error, got nil")
	}
}

// TestCleanerDoubleStart verifies that a second Start returns an error.
func TestCleanerDoubleStart(t *testing.T) {
	db := &fakeSweeper{}
	c := newTestCleaner(t, Config{TTL: time.Hour, Interval: time.Second}, db)
	ctx := context.Background()
	if err := c.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if err := c.Start(ctx); err == nil {
		t.Error("second Start: expected an error, got nil")
	}
}

// TestCleanerSweepErrorLogged verifies that a store error in a sweep is logged
// (not returned) and does not stop the cleaner (subsequent sweeps still occur).
func TestCleanerSweepErrorLogged(t *testing.T) {
	db := &fakeSweeper{err: errors.New("sqlite busy")}
	c := newTestCleaner(t, Config{TTL: time.Hour, Interval: 20 * time.Millisecond}, db)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	c.Stop(context.Background())
	// The worker stays alive and keeps attempting sweeps despite the error.
	if db.count() < 2 {
		t.Errorf("sweeps = %d, want >= 2 (worker survives a store error)", db.count())
	}
	// No rows were deleted (all sweeps failed).
	if c.TotalDeleted() != 0 {
		t.Errorf("TotalDeleted = %d, want 0 (all sweeps failed)", c.TotalDeleted())
	}
}

// TestCleanerTotalDeletedAccumulates verifies the cumulative counter across
// successful sweeps.
func TestCleanerTotalDeletedAccumulates(t *testing.T) {
	db := &fakeSweeper{} // deletes 3 rows per successful sweep
	c := newTestCleaner(t, Config{TTL: time.Hour, Interval: 20 * time.Millisecond}, db)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(80 * time.Millisecond)
	c.Stop(context.Background())
	// The counter must equal 3 * (number of successful sweeps).
	if c.TotalDeleted()%3 != 0 {
		t.Errorf("TotalDeleted = %d, want a multiple of 3", c.TotalDeleted())
	}
}

// TestCleanerConfigValidationUnit exercises Config.Validate directly.
func TestCleanerConfigValidationUnit(t *testing.T) {
	if err := (Config{TTL: 0}).Validate(); err != nil {
		t.Errorf("TTL=0: err = %v, want nil (disabled is valid)", err)
	}
	if err := (Config{TTL: time.Hour, Interval: time.Second}).Validate(); err != nil {
		t.Errorf("valid: err = %v, want nil", err)
	}
	if err := (Config{TTL: time.Hour, Interval: 0}).Validate(); err == nil {
		t.Error("TTL>0 with zero interval: expected an error")
	}
	if err := (Config{TTL: time.Hour, Interval: 500 * time.Millisecond}).Validate(); err == nil {
		t.Error("sub-second interval: expected an error")
	}
	if err := (Config{TTL: time.Hour, Interval: 48 * time.Hour}).Validate(); err == nil {
		t.Error("48h interval: expected an error")
	}
}

// TestCleanerMaintenanceErrorDoesNotStopCleaner verifies that a
// RunMaintenance error is logged but does not stop the cleaner: subsequent
// sweeps still occur and the TotalDeleted counter still accumulates from
// successful DeleteExpired calls.
func TestCleanerMaintenanceErrorDoesNotStopCleaner(t *testing.T) {
	db := &fakeSweeper{maintenanceErr: errors.New("sqlite busy (maintenance)")}
	c := newTestCleaner(t, Config{TTL: time.Hour, Interval: 20 * time.Millisecond}, db)
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	c.Stop(context.Background())
	// The worker stays alive and keeps attempting sweeps despite the
	// maintenance error.
	if db.count() < 2 {
		t.Errorf("sweeps = %d, want >= 2 (worker survives a maintenance error)", db.count())
	}
	// DeleteExpired still succeeds (3 rows per sweep), so TotalDeleted
	// accumulates.
	if c.TotalDeleted() == 0 {
		t.Errorf("TotalDeleted = 0, want > 0 (DeleteExpired still succeeds despite maintenance failure)")
	}
	// No pages were reclaimed (all maintenance cycles failed).
	if c.TotalPagesReclaimed() != 0 {
		t.Errorf("TotalPagesReclaimed = %d, want 0 (all maintenance cycles failed)", c.TotalPagesReclaimed())
	}
}

// TestCleanerRealStoreIntegration performs a full end-to-end sweep against a
// real SQLite store: the cleaner deletes expired messages and retains fresh
// ones. A real *store.Store is used as the sweeper (it implements
// DeleteExpired), so this exercises the actual deletion path end to end.
func TestCleanerRealStoreIntegration(t *testing.T) {
	st, err := store.New(filepath.Join(t.TempDir(), "retention.db"))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	//nolint:errcheck // test cleanup
	t.Cleanup(func() { st.Close() })

	// A real wall clock: the sweep's wall-clock deadline (now + MaxSweepDuration)
	// must be in the future relative to time.Now, so the deadline does not stop
	// this small sweep. The threshold is now - TTL, so "48h ago" rows are
	// genuinely expired relative to the wall clock.
	now := time.Now()
	ttl := 24 * time.Hour

	// Seed 2 expired and 2 fresh messages.
	for i := 0; i < 2; i++ {
		if err := st.InsertReplicatedMessage(&store.Message{
			ID: newV7ID(t), Body: "exp", Origin: "n", CreatedAt: now.Add(-time.Duration(48+i*24) * time.Hour),
		}); err != nil {
			t.Fatalf("InsertReplicatedMessage: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		if err := st.InsertReplicatedMessage(&store.Message{
			ID: newV7ID(t), Body: "fresh", Origin: "n", CreatedAt: now.Add(-time.Duration(i+1) * time.Hour),
		}); err != nil {
			t.Fatalf("InsertReplicatedMessage: %v", err)
		}
	}

	// A cleaner backed by the real store, with a real clock so the threshold
	// (now - TTL) and the wall-clock deadline (now + MaxSweepDuration) are
	// consistent with time.Now. The default 10s time budget is a future deadline
	// for this small 2-row sweep. Drive one sweep directly.
	c := New(Config{TTL: ttl, Interval: time.Hour}, st, slog.Default())
	c.sweep(context.Background())

	if c.TotalDeleted() != 2 {
		t.Errorf("TotalDeleted = %d, want 2", c.TotalDeleted())
	}
	_, total, err := st.ListWithExpiration(100, 0, time.Time{})
	if err != nil {
		t.Fatalf("ListWithExpiration: %v", err)
	}
	if total != 2 {
		t.Errorf("total = %d, want 2 (only fresh messages remain)", total)
	}
}

// TestCleanerSweepBudgetAndResume verifies the cleaner's total-work budget: a
// single sweep deletes at most MaxSweepRows rows, and a backlog larger than the
// budget is processed across multiple sweeps (the remainder is not lost). This
// is end-to-end with a real store, exercising the actual budgeted deletion
// path (sweep → DeleteExpired with maxTotal) and the resume-on-next-sweep
// behavior.
func TestCleanerSweepBudgetAndResume(t *testing.T) {
	st, err := store.New(filepath.Join(t.TempDir(), "retention_budget.db"))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	//nolint:errcheck // test cleanup
	t.Cleanup(func() { st.Close() })

	// A real wall clock: the sweep's wall-clock deadline (now + MaxSweepDuration)
	// must be in the future relative to time.Now, so the row budget (not the
	// deadline) is the active bound. "48h ago" rows are genuinely expired.
	now := time.Now()
	ttl := 24 * time.Hour

	// Seed 3000 expired messages (a backlog larger than the budget of 1500).
	for i := 0; i < 3000; i++ {
		if err := st.InsertReplicatedMessage(&store.Message{
			ID: newV7ID(t), Body: "exp", Origin: "n", CreatedAt: now.Add(-time.Duration(48*60+i) * time.Minute),
		}); err != nil {
			t.Fatalf("InsertReplicatedMessage: %v", err)
		}
	}

	// A cleaner with a small row budget (1500) and a generous time budget
	// (24h, so the wall-clock deadline is in the future and the row budget is
	// the active bound), with a real clock.
	c := New(Config{TTL: ttl, Interval: time.Hour, MaxSweepRows: 1500, MaxSweepDuration: 24 * time.Hour}, st, slog.Default())

	// Sweep 1: deletes at most 1500 (the budget), leaving a remainder.
	c.sweep(context.Background())
	if got := c.TotalDeleted(); got != 1500 {
		t.Fatalf("sweep 1 TotalDeleted = %d, want 1500 (the row budget)", got)
	}

	// Sweep 2 (resume): deletes the remaining 1500.
	c.sweep(context.Background())
	if got := c.TotalDeleted(); got != 3000 {
		t.Fatalf("sweep 2 TotalDeleted = %d, want 3000 (backlog resumed)", got)
	}

	// Sweep 3: idempotent, no further deletions.
	c.sweep(context.Background())
	if got := c.TotalDeleted(); got != 3000 {
		t.Errorf("sweep 3 TotalDeleted = %d, want 3000 (backlog exhausted)", got)
	}
}

// TestCleanerCheckpointFailureWarns verifies that a failed WAL checkpoint
// (WALCheckpointError set, not a returned error) is surfaced as a warning
// through the cleaner, while a partial checkpoint (a normal, non-error
// outcome) is NOT surfaced as a failure warning. This is the regression for
// the "a failed checkpoint records WALCheckpointError but returns nil" gap.
func TestCleanerCheckpointFailureWarns(t *testing.T) {
	// Case 1: a failed checkpoint produces a warning.
	db := &fakeSweeper{
		checkpointStats: store.MaintenanceStats{
			PagesReclaimed:         5,
			WALCheckpointError:     errors.New("sqlite busy (checkpoint)"),
			WALUncheckpointedPages: 3,
		},
	}
	c := newTestCleanerWithCapture(t, Config{TTL: time.Hour, Interval: time.Hour}, db)
	c.sweep(context.Background())
	if !c.loggerCaptured.hasLineContaining("WAL checkpoint failed") {
		t.Errorf("expected a 'WAL checkpoint failed' warning for a failed checkpoint; captured:\n%s",
			c.loggerCaptured.snapshot())
	}

	// Case 2: a partial checkpoint (normal, non-error) does NOT produce a
	// failure warning.
	db2 := &fakeSweeper{
		checkpointStats: store.MaintenanceStats{
			PagesReclaimed:         5,
			WALCheckpointPartial:   true,
			WALUncheckpointedPages: 3,
			WALCheckpointBusy:      1,
		},
	}
	c2 := newTestCleanerWithCapture(t, Config{TTL: time.Hour, Interval: time.Hour}, db2)
	c2.sweep(context.Background())
	if c2.loggerCaptured.hasLineContaining("WAL checkpoint failed") {
		t.Errorf("a partial checkpoint must NOT produce a 'WAL checkpoint failed' warning (it is a normal, non-error outcome); captured:\n%s",
			c2.loggerCaptured.snapshot())
	}

	// Case 3: a successful (OK) checkpoint produces no failure warning.
	db3 := &fakeSweeper{
		checkpointStats: store.MaintenanceStats{PagesReclaimed: 5, WALCheckpointOK: true},
	}
	c3 := newTestCleanerWithCapture(t, Config{TTL: time.Hour, Interval: time.Hour}, db3)
	c3.sweep(context.Background())
	if c3.loggerCaptured.hasLineContaining("WAL checkpoint failed") {
		t.Errorf("a successful checkpoint must NOT produce a failure warning; captured:\n%s",
			c3.loggerCaptured.snapshot())
	}
}

// captureCleaner is a Cleaner whose logger writes to a captureWriter so log
// output can be asserted.
type captureCleaner struct {
	*Cleaner
	loggerCaptured *captureWriter
}

// newTestCleanerWithCapture creates a Cleaner backed by a capture logger (for
// asserting log output), with the same cleanup (Stop) as newTestCleaner.
func newTestCleanerWithCapture(t *testing.T, cfg Config, db sweeper) *captureCleaner {
	t.Helper()
	cw := &captureWriter{}
	c := New(cfg, db, slog.New(slog.NewTextHandler(cw, nil)))
	t.Cleanup(func() {
		//nolint:errcheck // test cleanup
		c.Stop(context.Background())
	})
	return &captureCleaner{Cleaner: c, loggerCaptured: cw}
}

// snapshot returns a newline-joined copy of the captured log lines.
func (w *captureWriter) snapshot() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.Join(w.lines, "\n")
}
