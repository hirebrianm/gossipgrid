// Package retention implements GossipGrid's configurable message retention:
// a periodic background cleaner that physically removes
// expired messages from the local SQLite store and performs bounded
// SQLite maintenance (incremental page reclamation and WAL checkpoint).
//
// # Scope and design
//
// This package owns ONLY the cleanup and maintenance worker. The actual
// expiration *filtering* (excluding expired messages from API listings, the
// peer sync inventory and fetch, and the cleanup deletion itself) lives in
// the store and is driven by a single, shared expiration threshold
// (store.ExpirationThreshold) computed from the same TTL and the same clock.
// The cleaner and the read paths therefore agree on the same cutoff, so a
// message is consistently treated as expired (or retained) across every point
// where it is stored, returned, exchanged, or reconciled.
//
// # Lifecycle
//
// A Cleaner is created, started exactly once, and stopped exactly once. Start
// launches a single goroutine that sweeps at a fixed interval (an initial sweep
// runs immediately so a freshly-started replica cleans up promptly rather than
// waiting a full interval). Stop cancels the engine context and waits (bounded
// by the supplied context) for the worker to finish. Stop before Start returns
// promptly. A stopped cleaner cannot be restarted.
//
// # Safety
//
// The sweep performs bounded, cancellable DELETE batches keyed on created_at,
// followed by bounded SQLite maintenance (incremental page reclamation and a
// PASSIVE WAL checkpoint). All operations are safe to run concurrently with
// message inserts (SQLite's WAL mode serializes writers), idempotent
// (re-running on an already-clean database deletes nothing and reclaims no
// pages), and cancellable via the context (shutdown interrupts in-flight work).
// An empty database is a no-op. The worker re-computes the threshold from the
// current time on every sweep, so clock progression (including NTP-driven
// jumps) is honoured without drift.
package retention

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"gossipgrid/internal/store"
)

// Config holds the validated retention cleaner parameters.
type Config struct {
	// TTL is the message retention period. Messages with CreatedAt strictly
	// before (now - TTL) are expired. A zero or negative TTL disables the
	// cleaner entirely (the default; the worker never starts).
	TTL time.Duration
	// Interval is the period between sweeps (an initial sweep runs immediately
	// on Start, then a sweep every Interval). Must be positive.
	Interval time.Duration
	// MaxSweepRows caps the total number of rows a single scheduled sweep may
	// delete. A sweep that deletes this many rows stops and the remaining
	// expired rows are deleted on the next sweep. This bounds the work of one
	// sweep so a large backlog (or continuous incoming writes) cannot keep one
	// sweep running indefinitely and monopolize the SQLite writer. Must be
	// positive. Zero (unvalidated) falls back to MaxSweepRowsDefault.
	MaxSweepRows int64
	// MaxSweepDuration caps the wall-clock time a single scheduled sweep may
	// take (delete phase only; maintenance is bounded separately). A sweep
	// that reaches this time stops and the remaining expired rows are deleted
	// on the next sweep. Must be positive. Zero (unvalidated) falls back to
	// MaxSweepDurationDefault.
	MaxSweepDuration time.Duration
}

// MaxSweepRowsDefault is the default total-row budget per scheduled sweep:
// 5,000 rows. Each row is a small message (at most ~4 KiB), so a 5,000-row
// sweep is a bounded, sub-second-to-a-few-seconds of work that does not hold
// the writer lock for long. A backlog larger than this is processed across
// multiple sweeps. The operator may increase it for very large databases.
const MaxSweepRowsDefault = 5000

// MaxSweepDurationDefault is the default wall-clock budget per scheduled sweep
// (delete phase): 10 seconds. Together with MaxSweepRowsDefault it bounds a
// single sweep in both rows and time, so a sweep can never run indefinitely.
const MaxSweepDurationDefault = 10 * time.Second

// Validate checks the retention configuration for out-of-range values. A
// zero/negative TTL is valid (it means "retention disabled"); in that case the
// interval and sweep budgets are not validated. When the TTL is non-zero, the
// interval must be a positive duration within documented bounds, and the sweep
// budgets (when set) must be positive.
func (c Config) Validate() error {
	if c.TTL <= 0 {
		return nil // disabled; no sweep
	}
	if c.Interval <= 0 {
		return errors.New("cleanup interval must be positive when retention is enabled")
	}
	if c.Interval < time.Second {
		return errors.New("cleanup interval must be at least 1s")
	}
	if c.Interval > 24*time.Hour {
		return errors.New("cleanup interval must be at most 24h")
	}
	if c.MaxSweepRows != 0 && c.MaxSweepRows <= 0 {
		return errors.New("max sweep rows must be positive when set")
	}
	if c.MaxSweepDuration != 0 && c.MaxSweepDuration <= 0 {
		return errors.New("max sweep duration must be positive when set")
	}
	return nil
}

// sweeper performs one cleanup sweep, deleting expired messages in bounded,
// cancellable batches (up to a total-row and time budget) and returning the
// number of rows deleted. It is an interface so tests can inject a
// deterministic (or failure-producing) sweeper without a real database. The
// context is checked before each batch and passed to the database operation,
// so shutdown cancellation (and the time-budget deadline) can interrupt an
// in-flight sweep.
type sweeper interface {
	// DeleteExpired deletes up to maxTotal expired rows (created_at <
	// threshold) in bounded batches, stopping early when maxTotal is reached
	// or the deadline (if non-nil) has passed. It returns the number of rows
	// deleted on this call; an unfinished backlog resumes on the next sweep.
	DeleteExpired(ctx context.Context, threshold time.Time, maxTotal int64, deadline *time.Time) (int64, error)
	// RunMaintenance performs one bounded maintenance cycle (incremental
	// page reclamation and PASSIVE WAL checkpoint) and returns the
	// resulting statistics. Called after each successful cleanup sweep.
	RunMaintenance(ctx context.Context) (store.MaintenanceStats, error)
}

// Cleaner is the periodic message-retention cleanup worker. It is safe for
// concurrent use. It is created, started exactly once, and stopped exactly
// once.
type Cleaner struct {
	cfg    Config
	logger *slog.Logger
	db     sweeper
	now    func() time.Time

	st             atomic.Uint64 // total messages deleted across all sweeps
	pagesReclaimed atomic.Uint64 // total pages reclaimed across all maintenance cycles

	mu           sync.Mutex
	started      bool
	stopped      bool
	engineCtx    context.Context
	engineCancel context.CancelFunc
	doneCh       chan struct{}
	doneOnce     sync.Once
	doneWG       sync.WaitGroup
}

// New creates a Cleaner. The sweeper is typically the local *store.Store; a
// nil logger uses slog.Default(). The now function defaults to time.Now and is
// injectable for deterministic tests.
func New(cfg Config, db sweeper, logger *slog.Logger) *Cleaner {
	if logger == nil {
		logger = slog.Default()
	}
	return &Cleaner{
		cfg:    cfg,
		logger: logger,
		db:     db,
		now:    time.Now,
		doneCh: make(chan struct{}),
	}
}

// WithClock returns a new Cleaner with an injectable clock (for testing).
func WithClock(cfg Config, db sweeper, logger *slog.Logger, now func() time.Time) *Cleaner {
	c := New(cfg, db, logger)
	c.now = now
	return c
}

// Start launches the periodic cleanup worker. It must be called exactly once.
// A stopped cleaner cannot be restarted. When the configured TTL is
// non-positive (retention disabled), Start is a no-op: the cleaner records the
// started state but launches no worker and performs no sweeps.
func (c *Cleaner) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started || c.stopped {
		return errors.New("retention cleaner already started or stopped")
	}
	c.started = true
	if c.cfg.TTL <= 0 {
		// Retention disabled: nothing to sweep. Mark done so Stop returns
		// promptly.
		close(c.doneCh)
		return nil
	}
	c.engineCtx, c.engineCancel = context.WithCancel(ctx)
	c.doneWG.Add(1)
	//nolint:contextcheck // c.engineCtx is a derived context created above from ctx via context.WithCancel; it inherits cancellation from ctx and is additionally cancellable via Stop().
	go c.run(c.engineCtx)
	go func() {
		c.doneWG.Wait()
		c.doneOnce.Do(func() { close(c.doneCh) })
	}()
	return nil
}

// Stop cancels the engine context (aborting an in-flight sweep), waits for the
// worker to finish (bounded by the supplied context), and is idempotent and
// safe to call from multiple goroutines. Stop before Start (or when retention
// is disabled) returns promptly. A stopped cleaner cannot be restarted.
func (c *Cleaner) Stop(ctx context.Context) {
	c.mu.Lock()
	if !c.started {
		c.stopped = true
		c.mu.Unlock()
		return
	}
	var doneCh chan struct{}
	if !c.stopped {
		c.stopped = true
		if c.engineCancel != nil {
			c.engineCancel()
		}
		doneCh = c.doneCh
	} else {
		doneCh = c.doneCh
	}
	c.mu.Unlock()

	if doneCh == nil {
		return
	}
	select {
	case <-doneCh:
	case <-ctx.Done():
		c.logger.Warn("retention cleaner shutdown timed out; the worker may still be running")
	}
}

// run is the periodic sweep loop: an initial sweep runs immediately, then a
// sweep every cfg.Interval until the engine context is cancelled.
//
//nolint:contextcheck // ctx is c.engineCtx, derived from Start's caller context.
func (c *Cleaner) run(ctx context.Context) {
	defer c.doneWG.Done()
	c.sweep(ctx)
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-c.engineCtx.Done():
			return
		case <-ticker.C:
			c.sweep(ctx)
		}
	}
}

// sweep performs one cleanup pass: it computes the expiration threshold from
// the current time and the configured TTL, deletes expired messages in
// bounded, cancellable batches subject to a total-row and wall-clock budget,
// performs bounded SQLite maintenance (incremental page reclamation and PASSIVE
// WAL checkpoint), and logs the result. The threshold is re-computed on every
// sweep, so clock progression (including NTP-driven jumps) is honoured without
// drift. The context is passed to DeleteExpired and RunMaintenance so shutdown
// cancellation can interrupt in-flight work. A store error is logged and the
// sweep is retried on the next tick; it never stops the cleaner.
//
// The sweep budget is the key safeguard against an unbounded sweep: a single
// sweep deletes at most MaxSweepRows rows (default 5,000) and runs for at most
// MaxSweepDuration (default 10s) in the delete phase. A backlog larger than
// the budget is not lost — the remaining expired rows are deleted on the next
// scheduled sweep — so a long-running replica processes a large backlog in
// bounded, resumable increments without monopolizing SQLite.
func (c *Cleaner) sweep(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	threshold := store.ExpirationThreshold(c.cfg.TTL, c.now())
	sweepStart := c.now()

	// Resolve the sweep budgets (falling back to documented defaults when the
	// operator leaves them unset).
	maxRows := c.cfg.MaxSweepRows
	if maxRows <= 0 {
		maxRows = MaxSweepRowsDefault
	}
	maxDur := c.cfg.MaxSweepDuration
	if maxDur <= 0 {
		maxDur = MaxSweepDurationDefault
	}
	// The time budget is an absolute deadline derived from the sweep start, so
	// it bounds the delete phase in wall-clock time regardless of row count.
	deadline := c.now().Add(maxDur)

	// Phase 1: delete expired messages in bounded batches, up to the total-row
	// budget and the wall-clock deadline. An unfinished backlog resumes on the
	// next sweep.
	cleanStart := c.now()
	deleted, err := c.db.DeleteExpired(ctx, threshold, maxRows, &deadline)
	cleanDuration := c.now().Sub(cleanStart)
	if err != nil {
		c.logger.Warn("retention cleanup sweep failed",
			slog.Any("error", err),
			slog.Int64("deleted_before_error", deleted),
			slog.Duration("duration", cleanDuration),
		)
		return
	}
	if deleted > 0 {
		c.st.Add(uint64(deleted))
		c.logger.Info("retention cleanup sweep deleted expired messages",
			slog.Int64("deleted", deleted),
			slog.Time("threshold", threshold),
			slog.Duration("duration", cleanDuration),
		)
	}

	// Phase 2: bounded SQLite maintenance (incremental page reclamation and
	// PASSIVE WAL checkpoint). Runs after the cleanup to reclaim pages
	// freed by the deletion and to checkpoint the WAL. A maintenance
	// failure does not stop the cleaner; it is logged and retried on the
	// next sweep.
	if ctx.Err() != nil {
		return
	}
	maintStart := c.now()
	stats, mErr := c.db.RunMaintenance(ctx)
	maintDuration := c.now().Sub(maintStart)
	if mErr != nil {
		c.logger.Warn("retention maintenance failed",
			slog.Any("error", mErr),
			slog.Duration("duration", maintDuration),
		)
		return
	}
	if stats.PagesReclaimed > 0 {
		c.pagesReclaimed.Add(uint64(stats.PagesReclaimed))
	}
	// A failed WAL checkpoint is a maintenance problem requiring operator
	// attention: it is recorded on the stats (WALCheckpointError) but not
	// returned as an error, so it would otherwise be silently ignored by the
	// DEBUG log below. Surface it as a warning. A partial checkpoint (a
	// normal, non-error outcome when active readers hold the WAL) is NOT
	// surfaced here — it is reported in the DEBUG log, not as a failure.
	if stats.WALCheckpointError != nil {
		c.logger.Warn("retention WAL checkpoint failed; the WAL was not fully checkpointed this cycle",
			slog.Any("error", stats.WALCheckpointError),
			slog.Int64("checkpoint_uncheckpointed_pages", stats.WALUncheckpointedPages),
		)
	}
	c.logger.Debug("retention maintenance cycle completed",
		slog.Int64("pages_reclaimed", stats.PagesReclaimed),
		slog.Int64("freelist_before", stats.FreelistBefore),
		slog.Int64("freelist_after", stats.FreelistAfter),
		slog.Int64("database_size_bytes", stats.DatabaseSize),
		slog.Int64("wal_size_bytes", stats.WALSize),
		slog.Int("auto_vacuum_mode", stats.AutoVacuumMode),
		slog.Bool("checkpoint_ok", stats.WALCheckpointOK),
		slog.Bool("checkpoint_partial", stats.WALCheckpointPartial),
		slog.Int("checkpoint_busy", stats.WALCheckpointBusy),
		slog.Int64("checkpoint_uncheckpointed_pages", stats.WALUncheckpointedPages),
		slog.Duration("duration", maintDuration),
		slog.Duration("sweep_duration", c.now().Sub(sweepStart)),
	)
}

// TotalDeleted returns the cumulative number of messages deleted across all
// completed sweeps (for observability). Sweeps that failed to delete (a store
// error) do not contribute to the count.
func (c *Cleaner) TotalDeleted() uint64 {
	return c.st.Load()
}

// TotalPagesReclaimed returns the cumulative number of freelist pages
// reclaimed across all completed maintenance cycles (for observability).
// Maintenance cycles that failed (a store error) do not contribute to the
// count.
func (c *Cleaner) TotalPagesReclaimed() uint64 {
	return c.pagesReclaimed.Load()
}
