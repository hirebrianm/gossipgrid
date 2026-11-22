package auth

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// DefaultReloadInterval is the poll period for the credential reloader. It
// matches the peer mTLS reload interval (2s) so the operator uses the same
// mental model for both credential kinds. A shorter interval reduces the
// rotation convergence window but increases file-read frequency.
const DefaultReloadInterval = 2 * time.Second

// DefaultMaxStale is the bound on how long the reloader may keep serving the
// last valid snapshot after a reload failure. After this bound, the
// reloader reports "stale" and new sessions return 503, signalling an operator
// that the credential is stuck in a bad state. This prevents the "last valid
// snapshot" preservation rule from masking a permanently broken credential
// indefinitely (see docs/security.md, "A subsequent malformed credential
// version"). The default is 5 minutes.
const DefaultMaxStale = 5 * time.Minute

// CredentialState is the reloader's current state, reported in diagnostics.
// It distinguishes the three availability/validity conditions from
// docs/security.md without disclosing the credential material.
type CredentialState string

// Credential reloader states.
const (
	// StateUnavailable means no valid credential has ever been loaded (e.g. the
	// file is absent or the initial load failed). Authentication returns 503.
	StateUnavailable CredentialState = "unavailable"
	// StateLoaded means a valid credential snapshot is in effect. Authentication
	// can proceed (401 for a wrong password, 200 for a correct one).
	StateLoaded CredentialState = "loaded"
	// StateStale means a valid snapshot is in effect but a later reload failed
	// and the bound (MaxStale) has elapsed. New sessions return 503; existing
	// (not-yet-expired) sessions continue to verify until their own expiry.
	// This is the "stuck in a bad state" signal.
	StateStale CredentialState = "stale"
)

// Diagnostics is a non-sensitive summary of the reloader's state. It never
// includes the credential material (salt, verifier, signing key) or the
// password. It is safe to expose in /api/diagnostics and in logs.
type Diagnostics struct {
	// State is the current reloader state (unavailable/loaded/stale).
	State CredentialState `json:"state"`
	// Generation is the credential generation of the in-effect snapshot (0 when
	// no snapshot is loaded). This is what sessions are bound to.
	Generation int64 `json:"generation,omitempty"`
	// LoadedAt is when the in-effect snapshot was loaded (RFC3339; empty when
	// no snapshot is loaded).
	LoadedAt string `json:"loaded_at,omitempty"`
	// ReloadCount is the number of successful reloads since Start (0 after the
	// initial load).
	ReloadCount int `json:"reload_count"`
	// LastReloadFailed reports whether the most recent reload attempt failed.
	LastReloadFailed bool `json:"last_reload_failed"`
	// StaleSeconds is how long (in whole seconds) the in-effect snapshot has
	// been the most recent successful load, relative to now. It is 0 when no
	// snapshot is loaded. It is informational; the State field is the
	// authoritative "stale" signal.
	StaleSeconds int64 `json:"stale_seconds,omitempty"`
}

// Reloader polls a credential file and atomically publishes an immutable
// snapshot on a successful load. A failed load preserves the previous valid
// snapshot. It is safe for concurrent use: Snapshot, TryReload, and Shutdown
// may be called from multiple goroutines while a reload is in progress.
//
// Lifecycle: created → started → stopped.
//
// The reloader implements the "bounded failure" policy from docs/security.md:
// it keeps the last valid snapshot in effect while a reload fails, but after a
// configurable bound (MaxStale) it reports StateStale and new sessions return
// 503. It never crashes, never reverts to an unauthenticated mode, and never
// accepts a malformed version.
type Reloader struct {
	path     string
	interval time.Duration
	maxStale time.Duration
	logger   *slog.Logger
	now      func() time.Time

	mu          sync.RWMutex
	current     *CredentialFile // in-effect snapshot (nil if none ever loaded)
	loadedAt    time.Time
	reloadCount int
	lastFailed  bool

	state  int // 0=created, 1=started, 2=stopped
	stopCh chan struct{}
	done   chan struct{}
}

// NewReloader creates a Reloader for the credential file at path. The interval
// is the poll period and maxStale is the bound on serving a stale snapshot.
// Zero values fall back to the documented defaults.
func NewReloader(path string, interval, maxStale time.Duration, logger *slog.Logger) *Reloader {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = DefaultReloadInterval
	}
	if maxStale <= 0 {
		maxStale = DefaultMaxStale
	}
	return &Reloader{
		path:     path,
		interval: interval,
		maxStale: maxStale,
		logger:   logger,
		now:      time.Now,
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// WithClock replaces the clock used for stale detection. It is provided so
// tests can drive the stale bound deterministically without waiting.
func (r *Reloader) WithClock(now func() time.Time) *Reloader {
	r.now = now
	return r
}

// Start performs the initial credential load and starts the reload worker.
//
// A failed initial load does NOT fail Start (unlike the peer mTLS Manager,
// where a missing credential must fail startup). For the browser credential,
// the correct fail-closed behavior is: the process starts, the authentication
// endpoint returns 503 until a valid credential is loaded, and /healthz and
// /readyz remain 200. This is the "startup behavior" section of
// docs/security.md.
//
// Start can only be called once. A failed Start leaves the Reloader in the
// created state (no goroutine started), so a subsequent Shutdown returns
// promptly.
func (r *Reloader) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.state != 0 {
		r.mu.Unlock()
		return errAlreadyStarted
	}
	r.mu.Unlock()

	// Initial load (synchronous). On failure, the reloader stays in
	// "unavailable" state: the poll loop will keep retrying.
	cf, err := LoadCredentialFile(r.path)
	if err != nil {
		r.mu.Lock()
		r.lastFailed = true
		r.mu.Unlock()
		r.logger.Warn("initial credential load failed; authentication will return 503 until a valid credential is loaded",
			slog.String("path", r.path),
			slog.Any("error", err),
		)
	} else {
		r.mu.Lock()
		r.current = cf
		r.loadedAt = r.now()
		r.lastFailed = false
		r.state = 1 // started (the poll goroutine is about to start)
		r.mu.Unlock()
		r.logger.Info("credential loaded",
			slog.Int64("generation", cf.Generation),
			slog.Int("iterations", cf.Iterations),
		)
	}

	// If the initial load failed, we still start the poll loop so a later
	// provisioned file is picked up. The state is set to started here because
	// the loop runs regardless; the "unavailable" state is reported by
	// Snapshot()/Diagnostics() when r.current is nil.
	r.mu.Lock()
	if r.state == 0 {
		r.state = 1
	}
	r.mu.Unlock()

	reloadCtx, cancel := context.WithCancel(ctx)
	go r.runReloadLoop(reloadCtx, cancel)
	return nil
}

// errAlreadyStarted is the error returned by a repeated Start.
var errAlreadyStarted = errAlreadyStartedSentinel

// errAlreadyStartedSentinel is the sentinel error for a repeated Start. It is
// defined here (not in credential.go) to keep the error definitions local to
// the reloader.
var errAlreadyStartedSentinel = errors.New("reloader already started")

// runReloadLoop polls the credential file at the configured interval and
// atomically publishes the new snapshot on a successful load. A failed load
// preserves the previous valid snapshot and records the failure.
//
// The loop exits on r.stopCh (closed by Shutdown) or when the derived context
// is cancelled. After each reload attempt it re-checks the stop signal so a
// Shutdown that fires while a reload is in progress is not missed.
//
//nolint:contextcheck // reloadCtx is a derived context from Start's caller; the loop exits on r.stopCh (closed by Shutdown) or when reloadCtx is cancelled.
func (r *Reloader) runReloadLoop(ctx context.Context, cancel context.CancelFunc) {
	defer cancel()
	defer close(r.done)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			r.logger.Debug("credential reload loop stopped")
			return
		case <-ctx.Done():
			r.logger.Debug("credential reload loop stopped (context cancelled)")
			return
		case <-ticker.C:
			r.TryReload()
			select {
			case <-r.stopCh:
				r.logger.Debug("credential reload loop stopped")
				return
			default:
			}
		}
	}
}

// TryReload performs a single credential load and, on success, atomically
// publishes the new snapshot. On failure it preserves the previous valid
// snapshot and records the failure.
//
// The "change" is detected by comparing the new snapshot against the current
// one on ALL security-relevant fields (format version, algorithm, iterations,
// salt, verifier, generation, signing key). A file whose security-relevant
// content is unchanged is a no-op (no reload count increment). A file that
// differs on any security-relevant field — including a verifier, salt, or
// iteration-count change at an unchanged generation — atomically swaps the
// snapshot. This means a changed password verifier is never silently ignored:
// it is always picked up on the next reload.
//
// TryReload is safe to call from multiple goroutines (the poll loop calls it
// once per tick; tests may call it concurrently with requests).
func (r *Reloader) TryReload() {
	cf, err := LoadCredentialFile(r.path)
	if err != nil {
		r.mu.Lock()
		r.lastFailed = true
		r.mu.Unlock()
		r.logger.Warn("credential reload failed; previous credentials retained",
			slog.Any("error", err),
		)
		return
	}

	now := r.now()
	r.mu.Lock()
	// No-op if the file is unchanged on every security-relevant field.
	if r.current != nil && sameCredential(r.current, cf) {
		r.lastFailed = false
		r.mu.Unlock()
		return
	}
	// A change (any security-relevant field differs): publish the new snapshot
	// atomically. The snapshot is immutable (LoadCredentialFile returns a fresh
	// parsed struct), so swapping the pointer is a safe atomic publication.
	r.current = cf
	r.loadedAt = now
	r.reloadCount++
	r.lastFailed = false
	r.mu.Unlock()
	r.logger.Info("credential reloaded",
		slog.Int64("generation", cf.Generation),
		slog.Int("reload_count", r.reloadCount),
	)
}

// sameCredential reports whether two credential records are identical on every
// security-relevant field. It is the no-op test for TryReload: a file whose
// security-relevant content has not changed does not trigger a reload. All
// fields are compared, so a verifier, salt, iteration-count, or signing-key
// change is never silently ignored even when the generation is unchanged.
func sameCredential(a, b *CredentialFile) bool {
	return a.FormatVersion == b.FormatVersion &&
		a.Algorithm == b.Algorithm &&
		a.Iterations == b.Iterations &&
		a.Salt == b.Salt &&
		a.Verifier == b.Verifier &&
		a.Generation == b.Generation &&
		a.SigningKey == b.SigningKey
}

// Shutdown stops the reload worker and waits for it to exit. It is idempotent
// and safe to call from multiple goroutines.
func (r *Reloader) Shutdown() {
	r.mu.Lock()
	if r.state == 2 {
		r.mu.Unlock()
		return
	}
	r.state = 2
	r.mu.Unlock()

	close(r.stopCh)
	<-r.done
}

// Snapshot returns the in-effect credential snapshot. It returns nil when no
// valid credential has ever been loaded. The returned pointer is to an
// immutable CredentialFile; the reloader replaces the whole struct on a
// rotation, so callers may hold the pointer without a lock.
func (r *Reloader) Snapshot() *CredentialFile {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.current
}

// Stale reports whether the in-effect snapshot has aged past the max-stale
// bound. It is true only when a valid snapshot is in effect AND a later reload
// has failed AND the bound has elapsed. When no snapshot is loaded, it is
// false (the "unavailable" state is reported separately).
func (r *Reloader) Stale() bool {
	return r.State() == StateStale
}

// State returns the current reloader state (unavailable/loaded/stale). It is
// the authoritative signal for the authentication endpoint: 503 when
// unavailable or stale, 200/401 when loaded.
func (r *Reloader) State() CredentialState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current == nil {
		return StateUnavailable
	}
	// Stale when a later reload failed and the bound has elapsed since the
	// last successful load. The "lastFailed" flag is set by a failed reload
	// attempt; it is cleared by a successful load.
	if r.lastFailed && r.now().After(r.loadedAt.Add(r.maxStale)) {
		return StateStale
	}
	return StateLoaded
}

// Diagnostics returns a non-sensitive summary of the reloader's state. It never
// includes the credential material or the password.
func (r *Reloader) Diagnostics() Diagnostics {
	r.mu.RLock()
	defer r.mu.RUnlock()

	d := Diagnostics{
		ReloadCount:      r.reloadCount,
		LastReloadFailed: r.lastFailed,
	}
	if r.current == nil {
		d.State = StateUnavailable
		return d
	}
	d.Generation = r.current.Generation
	d.LoadedAt = r.loadedAt.Format(time.RFC3339)
	if !r.loadedAt.IsZero() {
		elapsed := r.now().Sub(r.loadedAt)
		if elapsed > 0 {
			d.StaleSeconds = int64(elapsed / time.Second)
		}
	}
	d.State = r.stateLocked()
	return d
}

// stateLocked is State() without the lock (the caller holds r.mu).
func (r *Reloader) stateLocked() CredentialState {
	if r.current == nil {
		return StateUnavailable
	}
	if r.lastFailed && r.now().After(r.loadedAt.Add(r.maxStale)) {
		return StateStale
	}
	return StateLoaded
}
