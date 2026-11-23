// Package store: SQLite maintenance operations for long-running replicas.
//
// This file implements bounded, safe SQLite maintenance for GossipGrid
// replicas that may run continuously for months:
//
//   - Incremental auto-vacuum (new databases): PRAGMA auto_vacuum=INCREMENTAL
//     is enabled before schema initialization so that deleted pages are
//     tracked on the freelist and can be reclaimed incrementally without a
//     full VACUUM.
//   - Existing databases: the current auto_vacuum mode is detected at
//     startup. If the database was created without INCREMENTAL auto-vacuum
//     (the default for databases created before this feature), no
//     conversion is performed (a full VACUUM is expensive and would be
//     disruptive on a running replica). The operator can manually convert
//     during a maintenance window (documented).
//   - Incremental page reclamation: PRAGMA incremental_vacuum(N) reclaims up
//     to N freelist pages after a retention cleanup. This is bounded (N is
//     configurable) and does not perform a full VACUUM.
//   - WAL maintenance: PRAGMA wal_checkpoint(PASSIVE) performs a bounded,
//     non-blocking checkpoint that copies outstanding WAL frames into the
//     main database without blocking readers. PRAGMA journal_size_limit is a
//     retained-file-size management setting (NOT a hard cap on WAL growth:
//     the WAL can exceed it, particularly when checkpoint progress is
//     obstructed by active readers). These are applied at startup and the
//     checkpoint is run after each cleanup sweep.
//   - Physical file size: with incremental auto_vacuum, the physical main
//     database file can be truncated down to the logical size (page_count ×
//     page_size) as freelist pages are reclaimed (subject to WAL-mode
//     checkpoint behavior); a full VACUUM also rebuilds and shrinks the
//     physical database file. The physical main file, the physical WAL file,
//     the logical page count, and the freelist page count are distinct
//     quantities and are reported separately.
//
// The maintenance operations are integrated into the retention cleaner: they
// run after each cleanup sweep, are bounded, and are cancellable through the
// cleaner's context. When retention is disabled (the default), no maintenance
// runs (no cleaner is started).
package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	// Register the "sqlite3" driver (github.com/mattn/go-sqlite3) for use by
	// setNewDatabaseAutoVacuum, which opens a raw database/sql connection
	// before GORM creates the schema. The driver is already a transitive
	// dependency via gorm.io/driver/sqlite; this blank import makes the
	// dependency explicit and ensures the driver is registered.
	_ "github.com/mattn/go-sqlite3"
)

// MaintenanceStats holds the result of one maintenance cycle. These values are
// used for observability (logging) and do not affect the correctness of the
// cleanup.
//
// Disk-size semantics (kept distinct and reported accurately):
//   - DatabaseSize is the LOGICAL page count (PRAGMA page_count ×
//     page_size). This is NOT the on-disk file length: the physical main
//     database file may be shorter (freelist pages that have not yet been
//     reclaimed) or longer (unreclaimed pages). With incremental
//     auto_vacuum, the physical main file can be truncated down to the logical
//     size as freelist pages are reclaimed; a full VACUUM also rebuilds and
//     shrinks the file. The on-disk file length is read separately, not
//     conflated with the logical size.
//   - WALSize is the physical length of the WAL file (dbPath + "-wal"), in
//     bytes (0 when the file does not exist). The physical WAL size is NOT
//     necessarily the amount of uncheckpointed data: a PASSIVE checkpoint
//     does not truncate the file, so it may retain previously-checkpointed
//     bytes. The uncheckpointed amount is reported separately
//     (WALUncheckpointedPages) from the checkpoint result.
//   - The visible size of the database (main file + WAL) depends on
//     checkpoint timing: WAL frames not yet checkpointed are in the WAL file,
//     and once checkpointed they are in the main file. A frequent checkpoint
//     shifts the footprint from the WAL into the main file.
type MaintenanceStats struct {
	// PagesReclaimed is the number of freelist pages reclaimed by
	// incremental_vacuum (0 when auto_vacuum is not INCREMENTAL or there
	// are no freelist pages).
	PagesReclaimed int64
	// DatabaseSize is the LOGICAL database size in bytes
	// (PRAGMA page_count × PRAGMA page_size). This is not the on-disk file
	// length: the physical main file may be shorter (unreclaimed freelist
	// pages) or longer. With incremental auto_vacuum, the physical file can be
	// truncated toward the logical size as pages are reclaimed; a full VACUUM
	// also shrinks it. It reflects the actual data and freelist pages.
	DatabaseSize int64
	// WALSize is the physical length of the WAL file in bytes (0 when the
	// file does not exist). The physical size is not necessarily the
	// uncheckpointed amount (see WALUncheckpointedPages).
	WALSize int64
	// FreelistBefore is the number of freelist pages before reclamation.
	FreelistBefore int64
	// FreelistAfter is the number of freelist pages after reclamation.
	FreelistAfter int64
	// AutoVacuumMode is the current auto_vacuum mode (0=NONE, 1=FULL,
	// 2=INCREMENTAL). When 0, incremental reclamation is not available.
	AutoVacuumMode int
	// WALCheckpointBusy is the first value returned by
	// PRAGMA wal_checkpoint(PASSIVE). It is a boolean-style busy status
	// indicator, NOT a count of writer transactions: it is 1 when the
	// checkpoint was blocked (a concurrent write or read held the WAL and the
	// checkpoint could not make progress), and 0 otherwise. Non-zero (1)
	// means the checkpoint was obstructed (it may be partial).
	WALCheckpointBusy int
	// WALCheckpointLog is the second value returned by
	// PRAGMA wal_checkpoint(PASSIVE) (the number of pages in the WAL, i.e.
	// the total frames written into the WAL in this checkpoint cycle).
	WALCheckpointLog int64
	// WALCheckpointedPages is the third value returned by
	// PRAGMA wal_checkpoint(PASSIVE) (the number of WAL frames successfully
	// checkpointed into the main database).
	WALCheckpointedPages int64
	// WALUncheckpointedPages is WALCheckpointLog - WALCheckpointedPages: the
	// number of frames still outstanding in the WAL after the checkpoint.
	// Zero means the checkpoint is complete (all frames moved to the main
	// database); non-zero means partial (readers or writers held frames).
	WALUncheckpointedPages int64
	// WALCheckpointPartial is true when the checkpoint was a partial
	// success: it ran without error but did not checkpoint every outstanding
	// frame (busy > 0, or uncheckpointed > 0 with a non-empty WAL). This is
	// the normal, non-error outcome when active readers hold the WAL open; a
	// PASSIVE checkpoint does not block readers, so it simply stops short.
	WALCheckpointPartial bool
	// WALCheckpointOK is true when the PASSIVE checkpoint completed (ran
	// without error and checkpointed all outstanding frames, i.e. it is not
	// partial and there were no outstanding frames, or the WAL was empty).
	// A successful checkpoint with no outstanding frames is reported as OK
	// (not a failure).
	WALCheckpointOK bool
	// WALCheckpointError is non-nil when the checkpoint statement itself
	// failed (e.g. a SQLite busy or I/O error), as opposed to a partial
	// (non-error) checkpoint. When set, WALCheckpointOK is false.
	WALCheckpointError error
}

// setNewDatabaseAutoVacuum opens a short-lived raw SQLite connection to a
// new (non-existent) database file and sets auto_vacuum=INCREMENTAL before
// the schema is created. The pragma can only be changed when the database is
// truly empty (0 pages), so it must be set before GORM opens the connection
// and allocates the initial pages. The connection is closed immediately;
// GORM will open its own connection to the same file, which inherits the
// auto_vacuum mode from the on-disk database header.
//
// The driver "sqlite3" is registered by github.com/mattn/go-sqlite3, which
// is already a dependency of gorm.io/driver/sqlite (imported for its side
// effects in that package). We import it here explicitly to make the
// dependency visible.
//
// An error is returned if the pragma fails (e.g., the file appears between
// the stat and the open — a race that is extremely unlikely in practice,
// or an I/O error). The caller (Store.New / NewWithLogger) treats this as
// non-fatal: the database is still fully functional, just without
// incremental reclamation. The failure is logged as a warning so it is
// observable rather than silently ignored.
func setNewDatabaseAutoVacuum(dbPath string) error {
	ctx := context.Background() // initialization: no caller context available
	conn, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("opening raw SQLite connection: %w", err)
	}
	defer func() {
		//nolint:errcheck,gosec/G104 // connection close during setup; error not observable
		_ = conn.Close()
	}()
	// Set auto_vacuum=INCREMENTAL while the database is truly empty (0
	// pages). This must be done before any other pragma that allocates
	// pages (including journal_mode=WAL, which creates the WAL header
	// page); once the database has pages, SQLite refuses to change the
	// auto_vacuum mode (it requires a full VACUUM to convert).
	if _, err := conn.ExecContext(ctx, "PRAGMA auto_vacuum = INCREMENTAL"); err != nil {
		return fmt.Errorf("setting auto_vacuum=INCREMENTAL: %w", err)
	}
	// Now set WAL mode and busy timeout (matching the GORM DSN parameters)
	// so the database file is created with the same journal mode.
	if _, err := conn.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		return fmt.Errorf("setting journal_mode=WAL: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA busy_timeout=5000"); err != nil {
		return fmt.Errorf("setting busy_timeout: %w", err)
	}
	return nil
}

// walJournalSizeLimitBytes is the configured PRAGMA journal_size_limit
// (64 MiB). This is a retained-file-size management setting, NOT a hard cap
// on WAL growth: SQLite may let the WAL file exceed this size, particularly
// when checkpoint progress is obstructed by active readers. It tells SQLite
// how large to keep the WAL file under normal (unobstructed) checkpointing.
const walJournalSizeLimitBytes = 64 * 1024 * 1024

// applyWALPragmas sets the WAL journal size limit on an already-open database
// connection. The limit is a retained-file-size management setting (64 MiB),
// NOT an absolute maximum on WAL growth (see walJournalSizeLimitBytes). It is
// safe for both new and existing databases.
//
// An error is returned if the pragma fails. The caller (Store.New) treats a
// failed journal_size_limit as non-fatal but observable: the WAL still works
// (it just may grow larger than the limit under normal checkpointing), so the
// database is opened normally, but the failure is surfaced to the caller
// rather than silently swallowed (a silently-failed maintenance setting is
// harder to diagnose than a logged one).
func applyWALPragmas(sqlDB *sql.DB) error {
	ctx := context.Background() // initialization: no caller context available
	_, err := sqlDB.ExecContext(ctx, "PRAGMA journal_size_limit = "+itoa(walJournalSizeLimitBytes))
	if err != nil {
		return fmt.Errorf("setting PRAGMA journal_size_limit=%d: %w", walJournalSizeLimitBytes, err)
	}
	return nil
}

// itoa formats a non-negative integer as a decimal string (used to build the
// PRAGMA statement, which does not support parameter binding in SQLite). The
// value is a documented constant (walJournalSizeLimitBytes), not user input,
// so this is safe.
func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// runMaintenance performs one bounded maintenance cycle: it reads the
// freelist page count, reclaims up to maxPages pages via
// incremental_vacuum (when auto_vacuum=INCREMENTAL), performs a PASSIVE WAL
// checkpoint, and reads the resulting file sizes. The context is checked
// before each operation and passed to the database calls, so cancellation
// (e.g., during shutdown) can interrupt the cycle.
//
// The operations are bounded:
//   - incremental_vacuum(maxPages): reclaims at most maxPages pages (the
//     default is 100 pages, ~800 KiB at 8 KiB page size);
//   - wal_checkpoint(PASSIVE): does not block readers (a soft checkpoint
//     that completes only when no readers are holding the WAL open);
//   - PRAGMA page_count / page_size / freelist_count: single-row reads.
//
// A maintenance failure (e.g., a SQLite busy error) is returned as an error
// but does not crash the application; the caller (the retention cleaner)
// logs it and retries on the next sweep.
func (s *Store) runMaintenance(ctx context.Context, maxPages int) (MaintenanceStats, error) {
	var stats MaintenanceStats
	sqlDB, err := s.db.DB()
	if err != nil {
		return stats, fmt.Errorf("accessing underlying sql.DB: %w", err)
	}

	// Detect the auto_vacuum mode (0=NONE, 1=FULL, 2=INCREMENTAL).
	if !ctxOk(ctx) {
		return stats, nil
	}
	if err := sqlDB.QueryRowContext(ctx, "PRAGMA auto_vacuum").Scan(&stats.AutoVacuumMode); err != nil {
		return stats, fmt.Errorf("reading auto_vacuum mode: %w", err)
	}

	// Read the freelist page count before reclamation.
	if !ctxOk(ctx) {
		return stats, nil
	}
	if err := sqlDB.QueryRowContext(ctx, "PRAGMA freelist_count").Scan(&stats.FreelistBefore); err != nil {
		return stats, fmt.Errorf("reading freelist count: %w", err)
	}

	// Reclaim pages (only when auto_vacuum=INCREMENTAL and there are
	// freelist pages). incremental_vacuum(N) reclaims up to N pages from
	// the freelist. When auto_vacuum is not INCREMENTAL (0 or 1), this is
	// a no-op (SQLite ignores it) and no pages are reclaimed.
	//
	// PRAGMA incremental_vacuum(N) is a statement (not a query): it returns
	// no rows, so we use ExecContext. The number of pages reclaimed is
	// determined by re-reading freelist_count after the call.
	if stats.AutoVacuumMode == 2 && stats.FreelistBefore > 0 && maxPages > 0 {
		if !ctxOk(ctx) {
			return stats, nil
		}
		// PRAGMAs do not support parameter binding in SQLite; the page
		// count must be a literal in the statement. maxPages is an int
		// (defaultMaintenanceMaxPages), not user input, so Sprintf is safe.
		vacuumStmt := fmt.Sprintf("PRAGMA incremental_vacuum(%d)", maxPages)
		if _, err := sqlDB.ExecContext(ctx, vacuumStmt); err != nil {
			// Non-fatal: reclamation failed (e.g., a SQLite busy error).
			// The caller logs it and retries on the next sweep.
			return stats, fmt.Errorf("incremental_vacuum: %w", err)
		}
	}

	// Read the freelist page count after reclamation and compute the
	// number of pages reclaimed (before - after).
	if !ctxOk(ctx) {
		return stats, nil
	}
	if err := sqlDB.QueryRowContext(ctx, "PRAGMA freelist_count").Scan(&stats.FreelistAfter); err != nil {
		// Non-fatal: assume no reclamation occurred.
		stats.FreelistAfter = stats.FreelistBefore
	}
	stats.PagesReclaimed = stats.FreelistBefore - stats.FreelistAfter

	// PASSIVE WAL checkpoint: does not block readers. It copies outstanding
	// WAL frames into the main database, stopping short when active readers
	// or writers hold frames (a partial checkpoint, which is a normal,
	// non-error outcome — a PASSIVE checkpoint never blocks readers).
	//
	// PRAGMA wal_checkpoint(PASSIVE) returns three values:
	//   - busy: a boolean-style busy status indicator (1 when the checkpoint
	//     was blocked by a concurrent write or read, 0 otherwise) — it is NOT
	//     a count of writer transactions (non-zero ⇒ the checkpoint may be
	//     partial);
	//   - log: the number of pages (frames) in the WAL for this checkpoint
	//     cycle (the total frames to checkpoint);
	//   - checkpointed: the number of those frames actually copied into the
	//     main database.
	//
	// We distinguish four outcomes:
	//   - FAILED: the statement errored (e.g. a SQLite busy/I/O error).
	//     WALCheckpointError is set; WALCheckpointOK is false.
	//   - PARTIAL: the statement succeeded but did not checkpoint every
	//     outstanding frame (busy > 0, or checkpointed < log with a non-empty
	//     WAL). WALCheckpointPartial is true; WALCheckpointOK is false. This
	//     is the normal, non-error case when readers hold the WAL open.
	//   - EMPTY: the statement succeeded with no outstanding frames
	//     (log == 0). This is a SUCCESSFUL empty checkpoint — it is NOT a
	//     failure (a successful checkpoint with no outstanding frames must
	//     not be reported as a failure). WALCheckpointOK is true.
	//   - SUCCESS: the statement succeeded and checkpointed every outstanding
	//     frame (checkpointed == log, log > 0). WALCheckpointOK is true.
	if !ctxOk(ctx) {
		return stats, nil
	}
	var busy, log, checkpointed int
	if err := sqlDB.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)").Scan(&busy, &log, &checkpointed); err != nil {
		// FAILED: the checkpoint statement itself errored (e.g. a SQLite
		// busy/I/O error). This is distinct from a partial (non-error)
		// checkpoint. Report it as an error so it is observable without
		// being conflated with a routine partial checkpoint.
		classifyCheckpoint(&stats, busy, log, checkpointed, err)
		// A failed checkpoint is a maintenance problem (observable), not a
		// fatal error: the WAL still works and the checkpoint is retried on
		// the next cycle. Return the stats with the error recorded (no
		// error is returned to the caller, so a routine checkpoint failure
		// does not stop the cleaner).
		return stats, nil
	}
	classifyCheckpoint(&stats, busy, log, checkpointed, nil)

	// Read the database LOGICAL size: page_count * page_size. This is the
	// logical size of the database (including the freelist). It is NOT the
	// on-disk file length: the physical main file may be shorter (unreclaimed
	// freelist pages) or longer. With incremental auto_vacuum, the physical
	// main file can be truncated down to the logical size as freelist pages
	// are reclaimed; a full VACUUM also rebuilds and shrinks the file. The
	// physical file length is reported separately (WALSize for the WAL; the
	// main file length is not conflated with the logical size).
	if !ctxOk(ctx) {
		return stats, nil
	}
	var pageCount, pageSize int
	if err := sqlDB.QueryRowContext(ctx, "PRAGMA page_count").Scan(&pageCount); err != nil {
		return stats, fmt.Errorf("reading page count: %w", err)
	}
	if err := sqlDB.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err != nil {
		return stats, fmt.Errorf("reading page size: %w", err)
	}
	stats.DatabaseSize = int64(pageCount * pageSize)

	// Read the PHYSICAL WAL file size (dbPath + "-wal"), in bytes (0 when the
	// file does not exist). The physical size is not necessarily the
	// uncheckpointed amount: a PASSIVE checkpoint does not truncate the file,
	// so it may retain previously-checkpointed bytes. The uncheckpointed
	// amount is reported separately (WALUncheckpointedPages).
	stats.WALSize = walFileSize(s.dbPath)

	return stats, nil
}

// ctxOk reports whether the context has not been cancelled or expired.
// It is a thin wrapper around ctx.Err() == nil that avoids the nilerr
// linter pattern (checking ctx.Err() in an if-then-return-nil).
func ctxOk(ctx context.Context) bool {
	return ctx.Err() == nil
}

// classifyCheckpoint records the three values returned by
// PRAGMA wal_checkpoint(PASSIVE) (busy, log, checkpointed) on the stats and
// derives the checkpoint outcome (OK / partial / failed) from them. The
// "busy" value is a boolean-style status indicator (1 when the checkpoint was
// blocked, 0 otherwise) — NOT a count of writer transactions. It is the
// single source of truth for the checkpoint semantics, factored out so the
// outcome classification is unit-testable without a real (timing-dependent)
// checkpoint:
//
//   - err != nil ⇒ FAILED: the statement errored. WALCheckpointError is set;
//     both OK and partial are false.
//   - err == nil and busy > 0, or (log > 0 and checkpointed < log) ⇒ PARTIAL:
//     a non-error checkpoint that did not move every outstanding frame
//     (readers/writers held frames). WALCheckpointPartial true, OK false.
//   - err == nil and not partial ⇒ OK: the checkpoint is complete. This
//     includes the EMPTY case (log == 0 — a successful empty checkpoint, NOT
//     a failure) and the full-success case (checkpointed == log, log > 0).
func classifyCheckpoint(stats *MaintenanceStats, busy, log, checkpointed int, err error) {
	stats.WALCheckpointBusy = busy
	stats.WALCheckpointLog = int64(log)
	stats.WALCheckpointedPages = int64(checkpointed)
	stats.WALUncheckpointedPages = int64(log - checkpointed)
	if err != nil {
		stats.WALCheckpointPartial = false
		stats.WALCheckpointOK = false
		stats.WALCheckpointError = err
		return
	}
	stats.WALCheckpointPartial = busy > 0 || (log > 0 && checkpointed < log)
	stats.WALCheckpointOK = !stats.WALCheckpointPartial
}

// defaultMaintenanceMaxPages is the default maximum number of freelist pages
// to reclaim per maintenance cycle. 100 pages at the default 8 KiB page size
// is ~800 KiB — a small, bounded reclamation that does not hold the writer
// lock for long. The operator can increase this for larger databases.
const defaultMaintenanceMaxPages = 100

// RunMaintenance performs one bounded maintenance cycle (incremental vacuum
// and a PASSIVE WAL checkpoint) and returns the resulting statistics. It is
// called by the retention cleaner after each cleanup sweep. The context is
// checked before each operation and passed to the database calls, so
// cancellation (e.g., during shutdown) can interrupt the cycle.
//
// A returned error indicates a hard maintenance failure (e.g. a SQLite busy
// or I/O error reading a PRAGMA). It does not crash the application; the
// caller (the retention cleaner) logs it and retries on the next sweep. A
// PARTIAL WAL checkpoint is NOT an error (it is a normal, non-error outcome
// when readers hold the WAL open); it is reported in the stats
// (WALCheckpointPartial / WALUncheckpointedPages), not as a returned error.
//
// This method is safe to call concurrently with other store operations
// (SQLite's WAL mode serializes writers; the maintenance operations are
// bounded, single-statement PRAGMAs that do not hold locks for long).
func (s *Store) RunMaintenance(ctx context.Context) (MaintenanceStats, error) {
	return s.runMaintenance(ctx, defaultMaintenanceMaxPages)
}

// walFileSize returns the size of the WAL file (dbPath + "-wal") in bytes, or
// 0 when the file does not exist or cannot be stat'd. The WAL file is
// created by SQLite when in WAL journal mode; it is deleted (or truncated)
// after a successful TRUNCATE checkpoint. A PASSIVE checkpoint may not
// truncate the file, so the size may persist.
func walFileSize(dbPath string) int64 {
	if dbPath == "" {
		return 0
	}
	fi, err := os.Stat(dbPath + "-wal")
	if err != nil {
		return 0
	}
	return fi.Size()
}
