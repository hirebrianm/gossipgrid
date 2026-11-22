// Package lifecycle: the Controller.
//
// The Controller is the top-level orchestration component for the replica
// lifecycle. It:
//   - owns the state machine (Machine);
//   - owns the accepted authoritative metadata (ClusterMetadata);
//   - owns the durable local database binding (DatabaseBinding);
//   - runs a metadata worker that periodically reads the authoritative
//     metadata source and re-evaluates admission;
//   - exposes concurrency-safe state, metadata, and binding snapshots.
//
// The Controller is the single point through which the rest of the
// application (server, peertls, gossip, antientropy) accesses lifecycle
// state. It does NOT hold a lock across network operations; it publishes
// immutable snapshots atomically.
//
// The metadata worker uses a bounded interval (MetadataPollInterval) to
// poll the authoritative source. It does NOT busy-loop: if the source is
// unavailable, the worker waits for the full interval before retrying.
//
// The controller is started once and stopped once. Stop is idempotent and
// safe to call from multiple goroutines. Stop before Start returns
// promptly.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// BindingStore is the narrow interface the Controller uses to read and write
// the durable local database binding. It is implemented by store.Store.
type BindingStore interface {
	// GetBinding returns the current database binding, or nil when no
	// binding exists.
	GetBinding() (*DatabaseBinding, error)
	// Bind atomically and idempotently binds the database to the given
	// cluster and epoch. It returns an error when the database is already
	// bound to a different cluster or epoch, or when the database is
	// nonempty without a binding.
	Bind(clusterID, epochID string, generation int) (*DatabaseBinding, error)
	// CountMessages returns the total number of messages in the local store.
	CountMessages() (int64, error)
}

// Config holds the Controller's configuration.
type Config struct {
	// MetadataPollInterval is the period between metadata reads by the
	// metadata worker. It must be positive.
	MetadataPollInterval time.Duration

	// NodeID is the local node identity (for diagnostics).
	NodeID string

	// Enabled reports whether lifecycle protection is active. When false,
	// the Controller is not created and all existing behavior is preserved.
	Enabled bool
}

// Validate checks the controller configuration.
func (c Config) Validate() error {
	if c.MetadataPollInterval <= 0 {
		return fmt.Errorf("metadata poll interval must be positive (got %v)", c.MetadataPollInterval)
	}
	return nil
}

// Controller is the top-level lifecycle orchestration component. It is safe
// for concurrent use.
//
// Lifecycle: created → started → stopped.
type Controller struct {
	cfg     Config
	logger  *slog.Logger
	machine *Machine
	binding BindingStore
	source  MetadataSource
	now     func() time.Time

	mu         sync.RWMutex
	accepted   *ClusterMetadata // accepted authoritative metadata (nil if none)
	bindingVal *DatabaseBinding // current local database binding

	// Freshness tracking.
	freshAt time.Time // when the last successful read was

	// Authority state: "fresh", "missing", or "invalid".
	authorityState string

	// authGen is a monotonically increasing authorization generation counter.
	// It is incremented under the write lock whenever the accepted metadata
	// changes or the authority state transitions, so two authorization
	// snapshots taken around a change carry different generations. It lets a
	// caller detect that authorization changed between two reads.
	authGen uint64

	// Metadata worker.
	startMu     sync.Mutex
	started     bool
	stopped     bool
	stopClosed  bool // guards the single close of stopCh (always set, even before Start)
	startedOnce sync.Once
	ctrlCtx     context.Context
	ctrlCancel  context.CancelFunc
	stopCh      chan struct{}
	done        chan struct{}
	doneOnce    sync.Once
}

// NewController creates a Controller. The metadata source and binding store
// must be non-nil when lifecycle protection is enabled.
func NewController(cfg Config, machine *Machine, source MetadataSource, binding BindingStore, logger *slog.Logger) *Controller {
	if logger == nil {
		logger = slog.Default()
	}
	if machine == nil {
		machine = NewMachine(nil)
	}
	return &Controller{
		cfg:            cfg,
		logger:         logger,
		machine:        machine,
		source:         source,
		binding:        binding,
		now:            time.Now,
		stopCh:         make(chan struct{}),
		done:           make(chan struct{}),
		authorityState: "missing",
	}
}

// WithClock returns a copy of the controller with an injectable clock.
func (c *Controller) WithClock(now func() time.Time) *Controller {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
	return c
}

// Start performs the initial metadata read and starts the metadata worker.
// It must be called exactly once. A stopped controller cannot be restarted.
//
// The initial read is synchronous: if the metadata source is unavailable,
// the controller transitions to RECOVERY_REQUIRED (fail closed) rather than
// blocking startup indefinitely.
//
// startMu is NOT held across the blocking metadata read. This allows Stop
// to acquire startMu and cancel the controller's context even while the
// initial read is in progress. The started flag is set under startMu BEFORE
// the read begins, so concurrent Start calls are rejected.
func (c *Controller) Start(ctx context.Context) error {
	c.startMu.Lock()
	if c.started || c.stopped {
		c.startMu.Unlock()
		return errors.New("lifecycle controller already started or stopped")
	}
	c.ctrlCtx, c.ctrlCancel = context.WithCancel(ctx)
	c.started = true
	c.startMu.Unlock()

	// Perform the initial metadata read. startMu is NOT held here, so Stop
	// can cancel the context to interrupt a blocked read.
	//nolint:contextcheck // c.ctrlCtx is a derived context created above from ctx.
	c.evaluateMetadata(c.ctrlCtx)

	// Check whether Stop was called during the initial read. If so, the
	// controller is already terminated: close done (so Stop's waiters are
	// released) and return without starting the worker.
	if c.authorization().Stopped {
		c.doneOnce.Do(func() { close(c.done) })
		return nil
	}

	// Start the metadata worker (periodic re-evaluation) at most once.
	//nolint:contextcheck // c.ctrlCtx is a derived context created above from ctx.
	c.startedOnce.Do(func() {
		go c.runMetadataWorker(c.ctrlCtx)
	})

	return nil
}

// runMetadataWorker periodically reads the authoritative metadata source and
// re-evaluates admission. It does NOT busy-loop: if the source is
// unavailable, it waits for the full interval before retrying.
//
//nolint:contextcheck // ctx is c.ctrlCtx, derived from Start's caller context.
func (c *Controller) runMetadataWorker(ctx context.Context) {
	defer c.doneOnce.Do(func() { close(c.done) })

	ticker := time.NewTicker(c.cfg.MetadataPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.evaluateMetadata(ctx)
		}
	}
}

// evaluateMetadata performs a single metadata read and re-evaluates the
// lifecycle state. It is called by the initial read and the metadata worker.
func (c *Controller) evaluateMetadata(ctx context.Context) {
	meta, err := c.source.Read(ctx)
	if err != nil {
		c.handleMetadataError(err)
		return
	}

	// Validate the metadata.
	if vErr := meta.Validate(); vErr != nil {
		c.handleMetadataError(vErr)
		return
	}

	c.mu.Lock()
	c.accepted = &meta
	c.freshAt = c.now()
	// Advance the authorization generation whenever the accepted observation
	// changes (or authority transitions from a non-fresh state to fresh), so
	// admission decisions and snapshot comparisons can detect the change.
	if c.authorityState != "fresh" {
		c.authGen++
	}
	c.authorityState = "fresh"
	c.mu.Unlock()

	// Re-evaluate the lifecycle state based on the new metadata.
	c.reevaluateState(&meta)
}

// handleMetadataError records a metadata read failure and updates the
// authority state. It does NOT automatically clear RECOVERY_REQUIRED;
// the operator must perform an authorized recovery procedure.
func (c *Controller) handleMetadataError(err error) {
	c.mu.Lock()
	// A metadata read failure (unavailable or malformed) makes the
	// authoritative observation invalid. This is a fail-closed signal: the
	// last accepted observation may be retained for diagnostics, but it must
	// NOT continue to authorize operations once the authority is lost or
	// stale. Advance the authorization generation so admission decisions
	// taken after this change are distinguishable from those before it.
	if c.authorityState != "invalid" {
		c.authGen++
	}
	c.authorityState = "invalid"
	c.mu.Unlock()
	c.logger.Warn("metadata read failed",
		slog.Any("error", err),
		slog.String("authority_state", "invalid"),
	)
}

// reevaluateState compares the new metadata against the local database
// binding and transitions the state machine accordingly.
func (c *Controller) reevaluateState(newMeta *ClusterMetadata) {
	c.mu.RLock()
	binding := c.bindingVal
	c.mu.RUnlock()

	// Load the current binding if not cached.
	if binding == nil && c.binding != nil {
		b, err := c.binding.GetBinding()
		if err != nil {
			c.logger.Warn("reading database binding failed",
				slog.Any("error", err),
			)
			c.mu.Lock()
			c.authorityState = "invalid"
			c.mu.Unlock()
			return
		}
		binding = b
		c.mu.Lock()
		c.bindingVal = b
		c.mu.Unlock()
	}

	currentState := c.machine.State()

	// Case 1: No binding exists and metadata is available.
	// This is the first-authorization case. We do NOT automatically
	// transition to GENESIS (that requires explicit operator action in
	// future milestones). We transition to DISCOVERING to indicate the
	// replica has authorization to participate but has not yet been
	// initialized.
	if binding == nil {
		if newMeta.Initialized {
			// The cluster has been initialized; this replica can join.
			_ = c.transitionTo(DISCOVERING, "metadata available; no local binding; cluster initialized")
		} else {
			// The cluster has not been initialized yet.
			_ = c.transitionTo(DISCOVERING, "metadata available; no local binding; cluster not yet initialized")
		}
		return
	}

	// Case 2: A binding exists.
	if binding.MatchesMetadata(*newMeta) {
		// The binding matches the new metadata. If we were in
		// RECOVERY_REQUIRED (due to a prior metadata mismatch), we
		// can return to DISCOVERING for re-evaluation.
		if currentState == RECOVERY_REQUIRED {
			_ = c.transitionTo(DISCOVERING, "metadata restored with matching epoch; re-evaluation")
		}
		// Otherwise, the current state is maintained (READY, DEGRADED,
		// SYNCING, etc.). The state machine is not forced to a specific
		// state here; that is the job of the future snapshot catch-up
		// implementation.
		return
	}

	// Case 3: The binding does NOT match the new metadata.
	// This is a mismatch: the local database belongs to a different
	// cluster or epoch. We must fail closed.
	if currentState != RECOVERY_REQUIRED {
		_ = c.transitionTo(RECOVERY_REQUIRED, fmt.Sprintf(
			"epoch mismatch: local binding (cluster=%q, epoch=%q) vs metadata (cluster=%q, epoch=%q)",
			binding.ClusterID, binding.EpochID,
			newMeta.ClusterID, newMeta.EpochID,
		))
	}
}

// transitionTo attempts a state machine transition under the correct lock
// order (c.mu RLock → m.mu Lock). It coordinates the transition with the
// commit path (which holds c.mu RLock + m.mu RLock) so that a
// readiness-revoking transition cannot linearize while a cursor commit holds
// the RLocks: the transition is queued behind the commit and is observed as
// a revocation by the next authorization evaluation.
//
// It logs the transition and handles errors (invalid transitions are logged,
// not panics). The c.mu RLock is held for the duration of the transition to
// coordinate with concurrent metadata updates (which take c.mu Lock) and to
// ensure the transition and the commit are serialized.
func (c *Controller) transitionTo(to State, reason string) error {
	c.mu.RLock()
	c.machine.mu.Lock()
	err := c.machine.transitionLocked(to, reason)
	c.machine.mu.Unlock()
	c.mu.RUnlock()
	if err != nil {
		c.logger.Warn("lifecycle transition rejected",
			slog.String("from", c.machine.State().String()),
			slog.String("to", to.String()),
			slog.String("reason", reason),
			slog.Any("error", err),
		)
		return err
	}
	c.logger.Info("lifecycle state transition",
		slog.String("from", c.machine.Snapshot().PreviousState.String()),
		slog.String("to", to.String()),
		slog.String("reason", reason),
	)
	return nil
}

// SetReady transitions the controller to READY when the initial baseline
// synchronization has been authorized and completed. It is the production
// path to READY (besides DEGRADED → READY) and is called by the future
// snapshot catch-up implementation (Milestone 5B) when the initial baseline
// is established.
//
// The promotion is GATED: it is permitted only when ALL of the following
// hold (the same authorization policy admission applies):
//
//   - the lifecycle state currently permits a transition to READY (the
//     transition table is still enforced — SetReady never bypasses it);
//   - a fresh, valid authoritative metadata observation exists;
//   - a valid local database binding exists that matches that observation
//     (ClusterID and EpochID);
//   - the database is not a legacy nonempty database without a binding
//     (a legacy database must not be silently bound merely to satisfy
//     SetReady — that would require an explicit operator action);
//   - the controller has not been stopped.
//
// A controller with accepted metadata but no database binding CANNOT enter
// READY and CANNOT accept writes: the durable binding is what proves the
// local database belongs to the authorized epoch. Initializing metadata
// alone does not prove baseline synchronization completed; that policy is
// Milestone 5B's job. SetReady therefore never silently binds a database.
//
// Lock-order contract: c.mu RLock → m.mu Lock (via transitionTo). The
// authorization snapshot read (c.mu RLock + m.mu RLock) is followed by the
// transition (c.mu RLock + m.mu Lock), both following the same lock order.
func (c *Controller) SetReady(reason string) error {
	snap := c.authorization()
	if snap.Stopped {
		return fmt.Errorf("cannot set READY: controller is stopped")
	}
	if snap.Metadata == nil {
		return fmt.Errorf("cannot set READY: no authoritative metadata observation")
	}
	if !snap.Metadata.Initialized {
		return fmt.Errorf("cannot set READY: authoritative metadata reports Initialized=false (the cluster must be initialized before promotion)")
	}
	if snap.AuthorityState != "fresh" || snap.Stale {
		return fmt.Errorf("cannot set READY: authoritative metadata is not fresh (authority=%s, stale=%v)", snap.AuthorityState, snap.Stale)
	}
	if snap.Binding == nil {
		// A missing binding is NOT a signal to silently create one. The
		// database must be explicitly bound to the authorized epoch by an
		// operator action (Milestone 5B) before the replica can become
		// READY. Rejecting here prevents a legacy or uninitialized database
		// from being silently bound and promoted.
		return fmt.Errorf("cannot set READY: no local database binding (the database must be explicitly bound to the authorized epoch)")
	}
	if !snap.Binding.MatchesMetadata(*snap.Metadata) {
		return fmt.Errorf("cannot set READY: local binding (%s/%s) does not match authoritative metadata (%s/%s)",
			snap.Binding.ClusterID, snap.Binding.EpochID,
			snap.Metadata.ClusterID, snap.Metadata.EpochID)
	}
	return c.transitionTo(READY, reason)
}

// SetReadyForTest is an exported test-only method that promotes the
// controller to READY for a specific authorization context, bypassing the
// binding requirement. It exists SOLELY to support cross-package tests that
// need a READY controller without a durable SQLite binding (e.g. the public
// router tests). Production code must NOT call this; the production path to
// READY is SetReady, which is gated on a fresh observation and a valid
// matching durable binding.
//
// It still enforces the transition table (the current state must permit a
// transition to READY) so it cannot force an arbitrary state.
// Lock-order contract: c.mu RLock → m.mu Lock (via transitionTo).
func (c *Controller) SetReadyForTest(reason string) error {
	return c.transitionTo(READY, reason)
}

// SetDegraded transitions the controller to DEGRADED. This is called when
// a peer or convergence failure is detected after the replica has become
// READY.
// Lock-order contract: c.mu RLock → m.mu Lock (via transitionTo).
func (c *Controller) SetDegraded(reason string) error {
	return c.transitionTo(DEGRADED, reason)
}

// SetRecoveryRequired transitions the controller to RECOVERY_REQUIRED.
// This is the primary readiness-revoking transition: it is called by the
// metadata worker when a binding/metadata mismatch is detected, and it must
// be coordinated with the commit path so that a cursor commit that linearizes
// before the transition is permitted, and a transition that linearizes before
// a pending commit is observed as a revocation.
// Lock-order contract: c.mu RLock → m.mu Lock (via transitionTo).
func (c *Controller) SetRecoveryRequired(reason string) error {
	return c.transitionTo(RECOVERY_REQUIRED, reason)
}

// SetDiscovering transitions the controller to DISCOVERING.
// Lock-order contract: c.mu RLock → m.mu Lock (via transitionTo).
func (c *Controller) SetDiscovering(reason string) error {
	return c.transitionTo(DISCOVERING, reason)
}

// SetSyncing transitions the controller to SYNCING.
// Lock-order contract: c.mu RLock → m.mu Lock (via transitionTo).
func (c *Controller) SetSyncing(reason string) error {
	return c.transitionTo(SYNCING, reason)
}

// Snapshot returns a concurrency-safe view of the controller's state,
// metadata, and binding.
type ControllerSnapshot struct {
	State         State
	Reason        string
	Timestamp     time.Time
	PreviousState State
	Transitions   []transitionRecord

	// Accepted metadata (nil if no authoritative observation).
	Metadata *ClusterMetadata
	// Authority state: "fresh", "missing", or "invalid".
	AuthorityState string
	// FreshAt is when the last successful metadata read was.
	FreshAt time.Time
	// Stale reports whether the accepted observation is stale.
	Stale bool

	// Local database binding (nil if no binding).
	Binding *DatabaseBinding
}

// Snapshot returns a concurrency-safe, immutable view of the controller.
//
// Lock-order contract: Controller.mu (RLock) → Machine.mu (RLock). Both
// RLocks are held while reading all fields, matching the same order used by
// CommitUnderAuthorization and by all lifecycle state transitions. This
// ensures the snapshot is consistent and that no deadlock can arise from
// lock-order inversion with a concurrent commit or transition.
func (c *Controller) Snapshot() ControllerSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.machine.mu.RLock()
	defer c.machine.mu.RUnlock()

	meta := c.accepted
	binding := c.bindingVal
	authorityState := c.authorityState
	freshAt := c.freshAt
	mSnap := c.machine.snapshotLocked()
	now := c.now()
	stale := false
	if !freshAt.IsZero() {
		stale = now.After(freshAt.Add(c.cfg.MetadataPollInterval * 2))
	}

	var metaCopy *ClusterMetadata
	if meta != nil {
		m := *meta
		metaCopy = &m
	}
	var bindingCopy *DatabaseBinding
	if binding != nil {
		b := *binding
		bindingCopy = &b
	}

	return ControllerSnapshot{
		State:          mSnap.State,
		Reason:         mSnap.Reason,
		Timestamp:      mSnap.Timestamp,
		PreviousState:  mSnap.PreviousState,
		Transitions:    mSnap.Transitions,
		Metadata:       metaCopy,
		AuthorityState: authorityState,
		FreshAt:        freshAt,
		Stale:          stale,
		Binding:        bindingCopy,
	}
}

// AuthorizationSnapshot is a single, internally consistent, point-in-time
// view of everything the admission decision depends on. All fields are
// captured under one read lock, so they belong to the same authorization
// generation: a state transition, a metadata change, or a binding update
// cannot interleave between reading the state and reading the metadata or
// the binding. Callers (admission checks, readiness, and the peer status
// handler) use this to make one coherent authorization decision rather than
// reading independent mutable fields at different times.
//
// The snapshot is immutable: it copies the accepted metadata and the binding,
// so the caller can hold it without holding any controller lock.
type AuthorizationSnapshot struct {
	// State is the lifecycle state.
	State State
	// Reason is the state's diagnostic reason.
	Reason string
	// Metadata is the last accepted authoritative observation, or nil when
	// none has ever been accepted.
	Metadata *ClusterMetadata
	// AuthorityState is "fresh", "missing", or "invalid".
	AuthorityState string
	// FreshAt is when the last successful read was (zero if none).
	FreshAt time.Time
	// Stale reports whether the last successful observation has aged past the
	// bounded freshness policy (2x the metadata poll interval). When true,
	// the observation must NOT authorize operations even if the state is
	// READY: a stale observation is not evidence of current authority.
	Stale bool
	// Binding is the durable local database binding, or nil when none exists.
	Binding *DatabaseBinding
	// Stopped reports whether the controller has been stopped. A stopped
	// controller must not authorize any operation.
	Stopped bool
	// Generation is a monotonically increasing counter incremented on every
	// successful metadata acceptance or authority-state change. It lets a
	// caller detect that authorization changed between two snapshots.
	Generation uint64
}

// authorized reports the core authorization decision shared by all admission
// checks and by readiness. An operation may be admitted only when ALL of the
// following hold (the fail-closed policy; see the package docs):
//
//   - the lifecycle state is a ready state (READY or DEGRADED);
//   - the authoritative metadata is present, FRESH (not stale), and valid;
//   - a local database binding exists;
//   - the binding's ClusterID and EpochID match the authoritative metadata;
//   - no recovery lockout exists (the state is not RECOVERY_REQUIRED);
//   - the controller has not been stopped.
//
// A temporary metadata loss (authority "invalid" or "missing") or a stale
// observation therefore fails closed: it does NOT leave an unrestricted
// READY decision. Readiness and the peer status endpoint use this same
// decision as the public handlers, so they cannot diverge.
func (s AuthorizationSnapshot) authorized() bool {
	if s.Stopped {
		return false
	}
	if !s.State.IsReady() {
		return false
	}
	if s.Metadata == nil {
		return false
	}
	if !s.Metadata.Initialized {
		// A metadata record with Initialized=false must never authorize
		// ordinary READY operations. Initialization is confirmed by the
		// authoritative record itself, not inferred from a matching binding
		// or an empty database.
		return false
	}
	if s.AuthorityState != "fresh" {
		return false
	}
	if s.Stale {
		return false
	}
	if s.Binding == nil {
		return false
	}
	if !s.Binding.MatchesMetadata(*s.Metadata) {
		return false
	}
	return true
}

// Authorized reports the core authorization decision (see authorized). It is
// exported so external packages (the public server, the peer server, and the
// peer client) can reflect the same decision the handlers use without
// re-reading independent mutable fields.
func (s AuthorizationSnapshot) Authorized() bool { return s.authorized() }

// authorizationReason returns a stable diagnostic explaining the decision.
func (s AuthorizationSnapshot) authorizationReason() string {
	if s.Stopped {
		return "lifecycle controller stopped"
	}
	if !s.State.IsReady() {
		return "replica not ready (state=" + s.State.String() + ")"
	}
	if s.Metadata == nil {
		return "no authoritative metadata observation"
	}
	if !s.Metadata.Initialized {
		return "authoritative metadata reports Initialized=false (genesis not yet authorized)"
	}
	if s.AuthorityState != "fresh" {
		return "authoritative metadata not fresh (authority=" + s.AuthorityState + ")"
	}
	if s.Stale {
		return "authoritative metadata observation is stale"
	}
	if s.Binding == nil {
		return "no local database binding"
	}
	if !s.Binding.MatchesMetadata(*s.Metadata) {
		return "local binding does not match authoritative epoch (binding=" +
			s.Binding.ClusterID + "/" + s.Binding.EpochID +
			" metadata=" + s.Metadata.ClusterID + "/" + s.Metadata.EpochID + ")"
	}
	return "authorized"
}

// authorization returns the current, internally consistent authorization
// snapshot. It is the single source of truth for the admission decision.
//
// Lock-order contract: Controller.mu (RLock) → Machine.mu (RLock). The
// method acquires both RLocks before reading any field, matching the same
// order used by CommitUnderAuthorization and by all lifecycle state
// transitions (which acquire c.mu RLock then m.mu Lock). This ensures the
// admission check and the commit guard observe a consistent, concurrent-
// safe snapshot and that no deadlock can arise from lock-order inversion
// between the admission path and the commit path.
//
// When the cached binding is absent, the method reads the durable binding
// from the store (a short, nonblocking in-memory operation for the fake
// binding store used in tests; for the real SQLite store this is a fast
// single-row read that does not block on the write lock). The cache is
// only warmed by the metadata-evaluation path, so in the window before a
// binding is written the cache may be nil while the durable binding exists.
// Reading the store here keeps the authorization snapshot faithful to the
// durable binding. The cache is NOT mutated under the read lock (that would
// be a data race); it is updated only by the write-lock evaluation path.
func (c *Controller) authorization() AuthorizationSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.machine.mu.RLock()
	defer c.machine.mu.RUnlock()

	meta := c.accepted
	binding := c.bindingVal
	authorityState := c.authorityState
	freshAt := c.freshAt
	stopped := c.stopped
	generation := c.authGen
	state := c.machine.state
	reason := c.machine.reason

	stale := false
	if !freshAt.IsZero() {
		stale = c.now().After(freshAt.Add(c.cfg.MetadataPollInterval * 2))
	}

	// Read the binding the same way LocalBinding does: use the cache when
	// present, otherwise read the durable binding directly from the store.
	// The cache is only warmed by the metadata-evaluation path, so in the
	// window before a binding is written (and in tests that seed a binding
	// directly) the cache may be nil while the durable binding exists. Reading
	// the store here keeps the authorization snapshot faithful to the durable
	// binding. We do NOT mutate the cache under a read lock (that would be a
	// data race); the cache is updated only by the write-lock evaluation path.
	if binding == nil && c.binding != nil {
		b, err := c.binding.GetBinding()
		if err != nil {
			c.logger.Warn("reading local database binding failed",
				slog.Any("error", err),
			)
		} else {
			binding = b
		}
	}

	var metaCopy *ClusterMetadata
	if meta != nil {
		m := *meta
		metaCopy = &m
	}
	var bindingCopy *DatabaseBinding
	if binding != nil {
		b := *binding
		bindingCopy = &b
	}

	return AuthorizationSnapshot{
		State:          state,
		Reason:         reason,
		Metadata:       metaCopy,
		AuthorityState: authorityState,
		FreshAt:        freshAt,
		Stale:          stale,
		Binding:        bindingCopy,
		Stopped:        stopped,
		Generation:     generation,
	}
}

// State returns the current lifecycle state.
func (c *Controller) State() State {
	return c.machine.State()
}

// IsReady reports whether the replica is in a ready state (READY or DEGRADED).
func (c *Controller) IsReady() bool {
	return c.machine.State().IsReady()
}

// AcceptedMetadata returns the current accepted authoritative metadata, or
// nil when no authoritative observation is available.
func (c *Controller) AcceptedMetadata() *ClusterMetadata {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.accepted == nil {
		return nil
	}
	m := *c.accepted
	return &m
}

// LocalBinding returns the current local database binding, or nil when no
// binding exists.
//
// It performs a read-through: when the cached binding is absent it reads the
// durable binding directly from the store and returns it, WITHOUT mutating the
// cached field. Mutating shared state under a read lock is a data race (a
// concurrent reader could observe a torn pointer), so the cache is updated
// only by the controller's own metadata-evaluation path (which holds the
// write lock). This keeps concurrent LocalBinding() calls race-free.
func (c *Controller) LocalBinding() *DatabaseBinding {
	c.mu.RLock()
	cached := c.bindingVal
	c.mu.RUnlock()
	if cached != nil {
		b := *cached
		return &b
	}
	if c.binding == nil {
		return nil
	}
	b, err := c.binding.GetBinding()
	if err != nil {
		c.logger.Warn("reading local database binding failed",
			slog.Any("error", err),
		)
		return nil
	}
	return b
}

// Authorization returns the current, internally consistent authorization
// snapshot. All authorization checks (admission, readiness, peer status, and
// the peer client's outbound epoch context) read from this snapshot so they
// make the same coherent decision: a state, metadata, or binding change
// cannot interleave between reading the state and reading the metadata or
// binding.
func (c *Controller) Authorization() AuthorizationSnapshot {
	return c.authorization()
}

// Authorized reports whether the replica is currently authorized to serve
// ordinary current-epoch operations (public writes and reads, inbound and
// outbound replication). It applies the full fail-closed policy: ready state
// plus a fresh, matching, valid authoritative observation plus a matching
// durable database binding, with no recovery lockout and not stopped.
//
// This is the same decision the public and private handlers use, so readiness
// and the handlers cannot diverge from it.
func (c *Controller) Authorized() bool {
	return c.authorization().authorized()
}

// AuthorityState returns the current authority state ("fresh", "missing",
// or "invalid").
func (c *Controller) AuthorityState() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.authorityState
}

// Admission returns the admission guard for the current controller. The
// guard reads the controller's single consistent authorization snapshot at
// check time, so it is safe to call multiple times and to share across
// handlers and engines.
func (c *Controller) Admission() *Admission {
	return NewAdmission(c.cfg.Enabled, c)
}

// CommitUnderAuthorization executes a short, nonblocking commit callback
// while holding the controller's authorization read lock (c.mu RLock) and the
// lifecycle state machine's read lock (m.mu RLock). This establishes the
// shared synchronization boundary between lifecycle authorization and an
// in-memory cursor commit in the anti-entropy or gossip engine.
//
// It evaluates ONE complete authorization decision — lifecycle state,
// stopped status, metadata presence, Initialized, authority freshness,
// matching durable binding, and recovery lockout — from a single locked
// snapshot (authorizationSnapshotLocked) that reads all authorization
// fields under one c.mu RLock and one m.mu RLock, and only runs the
// callback when that decision is authorized. The RLocks are held until the
// callback's in-memory cursor mutation completes, so a revocation (Stop or
// a state/metadata change) that linearizes before the commit is observed
// (the authorization snapshot is no longer authorized) and the callback is
// not executed. A revocation that linearizes after the callback has
// completed is a normal, permitted revocation that cannot be undone
// retroactively.
//
// Lock-order contract: Controller.mu (RLock) → Machine.mu (RLock) →
// Engine.mu (Lock). The callback may take the engine's mutex (e.g. to mutate
// a per-peer cursor or completion diagnostics) but MUST NOT:
//   - acquire Controller.mu (full or read). The RLock is already held by
//     this method and is non-reentrant, so a callback that re-reads the
//     authorization (Admission.CheckReplicationOutbound, Controller.Authorization,
//     or any method that takes c.mu) would block on the RLock and deadlock
//     any writer (Stop or a metadata update) waiting for c.mu Lock. The
//     authorization is therefore evaluated here, once, before the callback,
//     and the callback performs only the in-memory cursor mutation.
//   - acquire Machine.mu (full or read). The m.mu RLock is already held;
//     calling Machine.State() or Machine.Snapshot() would deadlock.
//   - perform network I/O, SQLite queries, or any other blocking operation.
//   - acquire startMu (a different lock; holding it while c.mu RLock is
//     held would invert the startMu → c.mu order used by Stop).
//
// All lifecycle state transitions (SetRecoveryRequired, SetReady, SetDegraded,
// SetDiscovering, SetSyncing, and the metadata-driven reevaluationState)
// acquire c.mu RLock before m.mu Lock, matching this lock order. This ensures
// that a readiness-revoking transition cannot linearize while a cursor commit
// holds the RLocks: the transition is queued behind the commit's RLocks and
// is observed as a revocation by the next authorization evaluation.
//
// A cached binding that is absent or does not match the accepted metadata
// fails closed: no database read is attempted inside the protected commit
// (a read here could block on the SQLite write lock while c.mu RLock is
// held, and is prohibited by the lock contract). The binding cache is
// warmed by the metadata-evaluation path, so it is present whenever the
// controller is authorized.
//
// When lifecycle protection is disabled (c.cfg.Enabled is false), the
// callback is executed directly without the read lock, preserving the
// existing M4 behavior.
//
// The callback returns a non-nil error when the commit is rejected (e.g.
// revocation observed at commit time). This method propagates that error.
// When the controller is stopped or the authorization is not valid, the
// callback is NOT executed and a rejection error is returned.
func (c *Controller) CommitUnderAuthorization(fn func() error) error {
	if !c.cfg.Enabled {
		return fn()
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.machine.mu.RLock()
	defer c.machine.mu.RUnlock()
	snap := c.authorizationSnapshotLocked()
	if !snap.authorized() {
		return errors.New("lifecycle commit rejected: " + snap.authorizationReason())
	}
	return fn()
}

// authorizationSnapshotLocked returns a non-recursive, internally consistent
// authorization snapshot. It must be called with c.mu RLock AND m.mu RLock
// already held. It reads all authorization fields from the protected
// controller and machine state without acquiring any additional locks and
// without a database read: when the cached binding (c.bindingVal) is absent
// or does not match the accepted metadata, the snapshot fails closed (Binding
// is nil or the binding/metadata match is false) rather than reading the
// durable binding from the store.
//
// The lock order (c.mu → m.mu) is the same order used by all lifecycle state
// transitions (transitionLocked acquires c.mu RLock then m.mu Lock). Holding
// both RLocks during the commit ensures that:
//   - a metadata change (c.mu Lock) that linearizes before the commit is
//     observed in the snapshot;
//   - a state transition (c.mu RLock → m.mu Lock) that linearizes before the
//     commit is observed in the snapshot;
//   - no transition can linearize while the commit holds the RLocks (the
//     transition is queued behind them).
//
// Because the binding cache is warmed by the metadata-evaluation path (which
// holds the c.mu write lock), the cache is present whenever the controller is
// genuinely authorized, so failing closed on an absent cache never rejects a
// commit that should be permitted.
func (c *Controller) authorizationSnapshotLocked() AuthorizationSnapshot {
	// All fields are read under the held c.mu RLock and m.mu RLock.
	meta := c.accepted
	binding := c.bindingVal
	authorityState := c.authorityState
	freshAt := c.freshAt
	stopped := c.stopped
	generation := c.authGen
	state := c.machine.state
	reason := c.machine.reason

	stale := false
	if !freshAt.IsZero() {
		stale = c.now().After(freshAt.Add(c.cfg.MetadataPollInterval * 2))
	}

	var metaCopy *ClusterMetadata
	if meta != nil {
		m := *meta
		metaCopy = &m
	}
	var bindingCopy *DatabaseBinding
	if binding != nil {
		b := *binding
		bindingCopy = &b
	}

	return AuthorizationSnapshot{
		State:          state,
		Reason:         reason,
		Metadata:       metaCopy,
		AuthorityState: authorityState,
		FreshAt:        freshAt,
		Stale:          stale,
		Binding:        bindingCopy,
		Stopped:        stopped,
		Generation:     generation,
	}
}

// Machine returns the underlying state machine (for test purposes).
func (c *Controller) Machine() *Machine {
	return c.machine
}

// SetAcceptedMetadataForTest is an exported test-only method that sets the
// controller's accepted metadata directly, bypassing the metadata worker.
// It is used by tests in other packages to set up a specific metadata state
// without waiting for the periodic worker. Production code must NOT call this.
func (c *Controller) SetAcceptedMetadataForTest(meta ClusterMetadata) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.accepted = &meta
	c.authorityState = "fresh"
	c.freshAt = c.now()
}

// EvaluateMetadataForTest is an exported test-only method that performs a
// single metadata read and re-evaluation (the same path the metadata worker
// uses). It is used by cross-package tests to simulate a concurrent
// controller writer (a metadata update that takes c.mu Lock) while other
// operations are in flight, to verify the writer does not deadlock against
// an in-flight reader. Production code must NOT call this; the metadata
// worker is the production caller of the same path.
func (c *Controller) EvaluateMetadataForTest(ctx context.Context) {
	c.evaluateMetadata(ctx)
}

// AcceptMetadataForTest is an exported test-only method that performs the
// write-lock portion of the metadata-evaluation path (accept the supplied
// metadata, mark the authority fresh, advance the authorization generation)
// without reading from the metadata source. It is used by cross-package tests
// to simulate a concurrent controller writer that takes c.mu Lock while the
// controller remains in a stable authorized state (a writer that would flip
// the authority to "invalid" would change the admission decision, which is
// not what a writer-deadlock test is meant to exercise). Production code
// must NOT call this; the metadata-evaluation path is the production caller.
func (c *Controller) AcceptMetadataForTest(meta ClusterMetadata) {
	c.mu.Lock()
	c.accepted = &meta
	c.freshAt = c.now()
	if c.authorityState != "fresh" {
		c.authGen++
	}
	c.authorityState = "fresh"
	c.mu.Unlock()
}

// SetBindingCacheForTest is an exported test-only method that warms the
// controller's cached binding (c.bindingVal) directly, bypassing the durable
// binding store. It is used by cross-package tests (e.g. the anti-entropy
// commit-guard tests) to establish a controller that is authorized without
// requiring the metadata-evaluation path or a real SQLite store. Production
// code must NOT call this; the cache is set only by the metadata-evaluation
// path (which holds the write lock) and by the durable Bind operation.
func (c *Controller) SetBindingCacheForTest(b *DatabaseBinding) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bindingVal = b
}

// Stop cancels the metadata worker and waits for it to exit. It is
// idempotent and safe to call from multiple goroutines. Stop before
// Start returns promptly and establishes the terminal stopped state so that
// admission decisions reject all operations (a stopped controller must not
// authorize work).
//
// The stopped flag is written under c.mu (not startMu) so that it is
// synchronized with all concurrent readers of c.stopped (the authorization
// snapshot, Snapshot, and Authorization methods read c.stopped under c.mu).
// startMu guards the one-time-start / stop-cancellation bookkeeping; c.mu
// guards the terminal stopped state observed by admission and readiness.
// The two locks are never held simultaneously (Start holds startMu briefly;
// Stop holds startMu then releases it before taking c.mu), so no deadlock
// can arise from lock-ordering differences.
//
// Stop can be called while Start is blocked in the initial metadata read
// (startMu is not held during that read). In that case, ctrlCancel
// interrupts the read. Stop then waits on done with the caller's context
// deadline: when the blocked read observes the cancellation, Start's
// post-read code (or this Stop call) closes done via doneOnce, releasing
// all waiters. If the caller's deadline expires first, Stop returns with
// a warning (the controller is already in the terminal stopped state and
// admission is denied; the reader goroutine will exit when its context
// is canceled, with no leak).
func (c *Controller) Stop(ctx context.Context) {
	c.startMu.Lock()
	if !c.started {
		// Stop before Start: establish the terminal stopped state under
		// c.mu (so concurrent authorization readers see it safely) and
		// return promptly.
		c.mu.Lock()
		c.stopped = true
		c.mu.Unlock()
		c.startMu.Unlock()
		return
	}
	// Write c.stopped under c.mu (not startMu) so it is synchronized with
	// all concurrent authorization/snapshot readers.
	c.mu.Lock()
	c.stopped = true
	c.mu.Unlock()

	// The stopClosed bookkeeping is under startMu (held from above).
	if !c.stopClosed {
		c.ctrlCancel()
		c.stopClosed = true
		close(c.stopCh)
	}
	c.startMu.Unlock()

	// Wait for the controller to fully shut down. The done channel is
	// closed either by the metadata worker (when it was started) or by
	// Start's post-read code / this Stop call (when the worker was never
	// started). doneOnce ensures exactly one close across all paths.
	select {
	case <-c.done:
	case <-ctx.Done():
		// The caller's deadline expired. The controller is in the terminal
		// stopped state (admission denied). The blocked reader (if any)
		// will exit when c.ctrlCtx is canceled (already done above).
		c.logger.Warn("lifecycle controller shutdown timed out")
	}
}
