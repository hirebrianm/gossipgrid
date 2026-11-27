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

	// SeedIdentity is the canonical seed identity for operator-authorized
	// genesis (Milestone 5B, Stage 1B). Only the replica whose local identity
	// exactly equals this value may take the genesis authorization path
	// (AuthorizeGenesis). It is empty when the replica is not designated as
	// the seed, and then AuthorizeGenesis is a no-op for it. In a
	// StatefulSet the seed is the ordinal 0 pod (e.g. "gossipgrid-0"). The
	// value is matched as an exact, case-sensitive string; no other replica
	// may enter GENESIS.
	SeedIdentity string

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

	// Re-evaluate the lifecycle state based on the new metadata. The c.mu
	// lock is released before reevaluateState (the re-evaluation performs a
	// nonblocking store read and state transitions; holding the write lock
	// across it would deadlock against concurrent readers of the binding
	// cache and the authorization snapshot). The atomicity of the
	// seed-restart reconciliation + promotion is instead provided by
	// promoteFromGenesis, which reads the state under the c.mu RLock and
	// reconciles STARTING → DISCOVERING when needed before promoting
	// (see promoteFromGenesis).
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
	// transition to GENESIS (that requires an explicit, operator-authorized
	// seed action — see AuthorizeGenesis). We transition to DISCOVERING to
	// indicate the replica has authorization to participate but has not yet
	// been initialized.
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
		// The binding matches the new metadata.
		if currentState == RECOVERY_REQUIRED {
			// A prior metadata mismatch placed us in RECOVERY_REQUIRED; a
			// matching epoch restored is "re-evaluation" (safe and automatic).
			//
			// Stage 1B recovery-persistence correction: a confirmed,
			// same-epoch genesis authorization revocation (Initialized=false,
			// GenesisAuthorized=false, GenesisConsumer="",
			// DataLossProtected=false) is NOT a restoration — the matching
			// ClusterID/EpochID alone does not prove the authorization was
			// restored. It is a recovery incident that must remain
			// RECOVERY_REQUIRED across repeated identical observations. The
			// seed is not automatically reinstated; the operator must perform
			// an authorized recovery (a new epoch). A legitimate restoration
			// (e.g. the authoritative epoch was actually restored, or the
			// pending grant is re-established) is not a revoked observation
			// and still clears the recovery state to DISCOVERING.
			if c.isRevokedGenesisObservation(newMeta) {
				return
			}
			_ = c.transitionTo(DISCOVERING, "metadata restored with matching epoch; re-evaluation")
			return
		}
		// Milestone 5B (Stage 1B): production GENESIS → READY promotion and
		// seed restart reconciliation.
		//
		// A seed restarts in STARTING (a new process; the machine is created
		// in STARTING). On the initial metadata read, the matching binding is
		// observed and the state is still STARTING — the state machine has not
		// yet advanced past STARTING. The state must be reconciled to a
		// legitimate state using the matching binding and the fresh metadata
		// (without rebinding or consuming authorization again):
		//
		//   - A: pending finalization (Initialized=false,
		//     GenesisAuthorized=true, GenesisConsumer="",
		//     DataLossProtected=false): the seed was durably bound to this
		//     epoch before finalization and is now restarting. The
		//     legitimate pending state is GENESIS: STARTING → DISCOVERING →
		//     GENESIS (the legal transition table; the seed remains NotReady
		//     while Initialized=false and public writes stay blocked).
		//
		//   - B: finalized (the record is completely finalized:
		//     Initialized=true, GenesisAuthorized=false,
		//     GenesisConsumer=<this seed>, DataLossProtected=true): the seed
		//     bound and was finalized before this restart. Restore the
		//     authorized state: STARTING → DISCOVERING → READY (the seed is
		//     promoted without rebinding or consuming authorization again).
		//
		// This is the only production path by which a genesis-bound replica
		// reaches READY: the operator's finalization Job must have atomically
		// set Initialized=true (in the same CAS that consumes the
		// authorization and arms dataLossProtected). The seed cannot reach
		// READY while Initialized=false — there is no "seed is READY while
		// the authorization is unconsumed" window.
		//
		// Only the designated seed (c.cfg.SeedIdentity == c.cfg.NodeID) may
		// be reconciled to GENESIS or promoted to READY here. A non-seed
		// replica whose binding matches the metadata (e.g. a joining
		// replica) is NOT automatically promoted: it requires its own
		// initial synchronization, which is the job of the future snapshot
		// catch-up implementation. The seed-entry loop (main.go) is started
		// only on the actual designated seed, so it is not unnecessarily run
		// on all replicas.
		// Stage 1B correction 4: the entire seed-restart reconciliation
		// (the pending-grant check, the revocation check, and the transition)
		// is performed atomically under a single pair of locks (c.mu.Lock +
		// m.mu.Lock) in reconcileSeedRestartLocked. A metadata update that
		// linearizes between the check and the transition cannot interleave
		// (the c.mu write lock serializes it against evaluateMetadata, which
		// also takes c.mu.Lock), so a revocation observed at re-check time is
		// handled correctly.
		if c.cfg.SeedIdentity != "" && c.cfg.SeedIdentity == c.cfg.NodeID && (currentState == STARTING || currentState == GENESIS) {
			reconcileSeedRestartLocked(c, newMeta)
			return
		}
		// Otherwise, the current state is maintained (READY, DEGRADED,
		// SYNCING, etc.). The state machine is not forced to a specific
		// state here; that is the job of the future snapshot catch-up
		// implementation.
		return
	}

	// Case 3: The binding does NOT match the new metadata.
	// This is a mismatch: the local database belongs to a different
	// cluster or epoch. We must fail closed. A seed in GENESIS whose
	// binding no longer matches the authoritative epoch (e.g. the operator
	// revoked the authorization or the epoch was reset) is likewise
	// revoked to RECOVERY_REQUIRED (no automatic reset; the local binding
	// and database are preserved).
	if currentState != RECOVERY_REQUIRED {
		_ = c.transitionTo(RECOVERY_REQUIRED, fmt.Sprintf(
			"epoch mismatch: local binding (cluster=%q, epoch=%q) vs metadata (cluster=%q, epoch=%q)",
			binding.ClusterID, binding.EpochID,
			newMeta.ClusterID, newMeta.EpochID,
		))
	}
}

// reconcileSeedRestartLocked performs the atomic seed-restart reconciliation
// (Stage 1B correction 4) for a designated seed whose state is STARTING or
// GENESIS and whose durable binding matches the new metadata. It validates
// the CURRENT accepted metadata under the held lock and performs the legal
// transition in a single atomic operation.
//
// It must be called with NO controller lock held. It acquires c.mu.Lock and
// m.mu.Lock (c.mu → m.mu order; no recursive acquisition) and performs all
// reads and transitions under that pair. A metadata update (evaluateMetadata,
// which takes c.mu.Lock) cannot linearize between the validation and the
// transition: a revocation that linearizes before the check is observed in
// c.accepted and the transition is rejected; a revocation that linearizes
// after is a normal, permitted revocation handled by the next re-evaluation.
//
// The five cases (Stage 1B correction 1):
//
//   - Valid, fresh pending genesis authorization (Initialized=false,
//     GenesisAuthorized=true, GenesisConsumer="", DataLossProtected=false,
//     fresh, matching binding): STARTING → DISCOVERING → GENESIS; GENESIS is
//     a no-op.
//   - Fresh, same-epoch authorization revoked before finalization
//     (Initialized=false, GenesisAuthorized=false, GenesisConsumer="",
//     DataLossProtected=false, same epoch as the binding): the seed is
//     transitioned to RECOVERY_REQUIRED (a confirmed revocation of an
//     outstanding authorization on the same epoch is a recovery incident).
//   - Successfully finalized metadata (Initialized=true, GenesisAuthorized=
//     false, GenesisConsumer=<this seed>, DataLossProtected=true): the seed
//     is reconciled to DISCOVERING and promoted to READY (Case B).
//   - Different or conflicting epoch (binding does not match the metadata):
//     handled by the Case 3 mismatch path in reevaluateState (this method is
//     only called when the binding matches; a conflicting epoch is not
//     reachable here).
//   - Temporary metadata API failure (authority "invalid" or stale): the
//     seed is left unchanged (no RECOVERY_REQUIRED). The last accepted
//     observation is not fresh; a later fresh observation will restore the
//     correct state.
//
// No Kubernetes or SQLite I/O is performed while holding both locks. The
// binding read-through (when the cache is empty on a fresh restart) is a
// single-row, nonblocking read, acceptable inside the critical section.
func reconcileSeedRestartLocked(c *Controller, newMeta *ClusterMetadata) {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.machine
	m.mu.Lock()
	defer m.mu.Unlock()

	// Read all relevant state under the held locks (no I/O, all in-memory
	// except the single-row binding read-through).
	stopped := c.stopped
	freshAt := c.freshAt
	authorityState := c.authorityState
	accepted := c.accepted
	binding := c.bindingVal
	if binding == nil && c.binding != nil {
		b, err := c.binding.GetBinding()
		if err == nil {
			binding = b
		} else {
			c.logger.Warn("reading durable binding for genesis restart failed",
				slog.Any("error", err),
			)
		}
	}

	// A stopped controller must not perform any state transition.
	if stopped {
		return
	}

	// Temporary metadata API failure: the authority is not "fresh" or the
	// observation is stale. The seed is left unchanged (no
	// RECOVERY_REQUIRED) so a later fresh observation can restore the
	// correct state.
	if authorityState != "fresh" {
		return
	}
	stale := false
	if !freshAt.IsZero() {
		stale = c.now().After(freshAt.Add(c.cfg.MetadataPollInterval * 2))
	}
	if stale {
		return
	}

	// The CURRENT accepted metadata (not newMeta, which is the new
	// observation that triggered this re-evaluation). The c.mu write lock
	// is held, so c.accepted is the latest accepted observation.
	// In practice newMeta and accepted are the same value here
	// (reevaluateState is called immediately after the accept), but we
	// use accepted for the atomic re-check.
	meta := accepted
	if meta == nil {
		meta = newMeta
	}

	// The binding must match the current metadata.
	bindingMatch := binding != nil && binding.MatchesMetadata(*meta)

	state := m.state

	// Case: the authorization has been revoked (same epoch, fresh, not
	// initialized, but GenesisAuthorized=false). This is a confirmed
	// revocation of an outstanding authorization on the SAME epoch: the
	// operator revoked the grant before the seed bound. The seed must not
	// enter GENESIS; it is transitioned to RECOVERY_REQUIRED.
	//
	// Distinguishing this from a temporary metadata failure: the
	// observation is FRESH and VALID (authority "fresh", not stale), and
	// the epoch matches the binding (same ClusterID and EpochID). A
	// temporary failure would have authority "invalid" or a stale
	// observation (handled above).
	if !meta.Initialized && !meta.GenesisAuthorized && meta.GenesisConsumer == "" && !meta.DataLossProtected && bindingMatch {
		if state != RECOVERY_REQUIRED {
			if err := m.transitionLocked(RECOVERY_REQUIRED, fmt.Sprintf(
				"genesis authorization revoked: local binding (cluster=%q, epoch=%q) matched a fresh, same-epoch record with GenesisAuthorized=false (Initialized=false, GenesisConsumer=\"\", DataLossProtected=false); the one-time authorization was revoked before finalization; the seed is no longer eligible for GENESIS or READY (the binding and database are preserved; operator intervention is required)",
				binding.ClusterID, binding.EpochID,
			)); err != nil {
				c.logger.Warn("lifecycle transition rejected (revocation)",
					slog.String("from", m.state.String()),
					slog.String("to", RECOVERY_REQUIRED.String()),
					slog.Any("error", err),
				)
			} else {
				c.logger.Info("lifecycle state transition",
					slog.String("from", m.previous.String()),
					slog.String("to", RECOVERY_REQUIRED.String()),
					slog.String("reason", "genesis authorization revoked (same epoch, fresh observation)"),
				)
			}
		}
		return
	}

	// Case: the authorization is a complete pending grant.
	// The seed is (re)established in GENESIS (STARTING → DISCOVERING →
	// GENESIS; GENESIS is a no-op).
	if c.pendingGenesisAuthorization(meta) && bindingMatch {
		if state == STARTING {
			if err := m.transitionLocked(DISCOVERING, "seed restart: matching durable binding observed; reconciling to genesis state"); err != nil {
				c.logger.Warn("lifecycle transition rejected (restart reconcile to DISCOVERING)",
					slog.String("from", m.state.String()),
					slog.String("to", DISCOVERING.String()),
					slog.Any("error", err),
				)
				return
			}
			state = m.state
		}
		if state == GENESIS {
			// Already in GENESIS: no transition needed (idempotent).
			return
		}
		if err := m.transitionLocked(GENESIS, "seed restart: bound to authorized epoch; awaiting operator finalization (Initialized=false)"); err != nil {
			c.logger.Warn("lifecycle transition rejected (restart to GENESIS)",
				slog.String("from", m.state.String()),
				slog.String("to", GENESIS.String()),
				slog.Any("error", err),
			)
			return
		}
		c.logger.Info("lifecycle state transition",
			slog.String("from", m.previous.String()),
			slog.String("to", GENESIS.String()),
			slog.String("reason", "seed restart: bound to authorized epoch (atomic reconciliation)"),
		)
		return
	}

	// Case: the record is completely finalized. Promote to READY
	// (STARTING → DISCOVERING → READY; or GENESIS → READY).
	if c.finalizationComplete(meta) && bindingMatch {
		if state == STARTING {
			if err := m.transitionLocked(DISCOVERING, "seed restart: matching durable binding observed; reconciling to authorized state"); err != nil {
				c.logger.Warn("lifecycle transition rejected (restart reconcile to DISCOVERING before promotion)",
					slog.String("from", m.state.String()),
					slog.String("to", DISCOVERING.String()),
					slog.Any("error", err),
				)
				return
			}
		}
		if err := c.promoteFromGenesisLocked(m, meta, binding); err != nil {
			c.logger.Warn("lifecycle promotion rejected",
				slog.Any("error", err),
			)
		}
		return
	}

	// Case: the metadata is incoherent (neither a valid pending grant, nor a
	// revocation, nor a complete finalization) but the binding matches and
	// the observation is fresh. The seed is left unchanged; the next fresh
	// observation will re-evaluate.
	//
	// (In practice, a coherent, fresh, same-epoch metadata that is neither
	// a pending grant nor a complete finalization is the revocation case
	// handled above. An incoherent observation would have been rejected by
	// ClusterMetadata.Validate and never reach reevaluateState.)
}

// promoteFromGenesisLocked performs the GENESIS/DISCOVERING → READY promotion
// with the c.mu write lock and m.mu write lock ALREADY held (it is called
// from reconcileSeedRestartLocked, which holds both). It is the
// lock-free counterpart of promoteFromGenesis.
//
// It validates the complete finalization invariant (fresh observation,
// Initialized=true, GenesisAuthorized=false, GenesisConsumer=<seed>,
// DataLossProtected=true, matching binding, not stopped) and performs the
// transition under the held locks.
func (c *Controller) promoteFromGenesisLocked(m *Machine, meta *ClusterMetadata, binding *DatabaseBinding) error {
	if c.stopped {
		return errors.New("cannot promote from GENESIS: controller is stopped")
	}
	if !c.finalizationComplete(meta) {
		return fmt.Errorf("cannot promote from GENESIS: authoritative metadata is not a completely finalized record for the designated seed (Initialized=%v GenesisAuthorized=%v GenesisConsumer=%q DataLossProtected=%v)",
			meta.Initialized, meta.GenesisAuthorized, meta.GenesisConsumer, meta.DataLossProtected)
	}
	if binding == nil {
		return errors.New("cannot promote from GENESIS: no local database binding (the seed must be bound before promotion)")
	}
	if !binding.MatchesMetadata(*meta) {
		return fmt.Errorf("cannot promote from GENESIS: local binding (%s/%s) does not match authoritative metadata (%s/%s)",
			binding.ClusterID, binding.EpochID, meta.ClusterID, meta.EpochID)
	}
	state := m.state
	if state != GENESIS && state != DISCOVERING {
		return fmt.Errorf("cannot promote from GENESIS: lifecycle state is %s (only GENESIS or the STARTING-reconciled DISCOVERING are promotable)", state)
	}
	if err := m.transitionLocked(READY, fmt.Sprintf(
		"genesis finalized: observed completely finalized metadata (Initialized=true, GenesisAuthorized=false, GenesisConsumer=%s, DataLossProtected=true) with matching binding (cluster=%s, epoch=%s); promotion to READY",
		meta.GenesisConsumer, meta.ClusterID, meta.EpochID,
	)); err != nil {
		return err
	}
	return nil
}

// promoteFromGenesis transitions the controller from GENESIS to READY when
// the authoritative metadata is a fresh, valid, COMPLETELY finalized
// observation that matches the local database binding. It is the production
// promotion path for a genesis-bound seed.
//
// It is GATED: it is permitted only when ALL of the following hold (the same
// fail-closed authorization policy admission applies). Requiring the complete
// invariant — not merely Initialized=true — is load-bearing: the 5A metadata
// validator intentionally accepts some legacy fixture combinations, so
// Initialized=true alone is NOT proof of finalization. A coherent, fresh,
// finalized authoritative record has all four finalization fields set by the
// operator's finalization CAS in a single atomic update.
//
//   - the lifecycle state is exactly GENESIS (the caller checks this);
//   - the authoritative metadata is FRESH (authority "fresh", not stale) and
//     valid (a stale or invalid observation must not authorize promotion —
//     a temporary metadata loss fails closed);
//   - the record is completely finalized:
//   - Initialized=true (the operator's finalization Job set it);
//   - GenesisAuthorized=false (the one-time authorization is consumed);
//   - GenesisConsumer equals the designated seed identity (this replica
//     consumed the authorization; a different or missing consumer means
//     this record does not authorize THIS seed);
//   - DataLossProtected=true (the data-loss protection was armed in the
//     same CAS that consumed the authorization);
//   - a valid local database binding exists whose ClusterID and EpochID
//     match the observation;
//   - the controller has not been stopped.
//
// Lock ordering (Stage 1B correction 1): the method performs the
// validation and the transition ATOMICALLY under a single consistent pair of
// locks — the controller's write lock (c.mu.Lock) held, then the machine's
// write lock (m.mu.Lock) acquired (c.mu → m.mu, the same order as
// AuthorizeGenesis, setReady, and CommitUnderAuthorization — with NO
// recursive acquisition of c.mu). The previous implementation validated
// metadata under c.mu RLock, RELEASED the lock, and only then performed the
// transition (c.mu RLock → m.mu Lock). A metadata update (evaluateMetadata,
// which takes c.mu.Lock) could linearize in the gap between the validation
// and the transition, invalidating the observation the promotion had relied
// on.
//
// The correction: the promotion reads the authoritative metadata, the binding
// cache, the authority state, the freshness, the stopped flag, and the
// machine's current state, all under one held c.mu.Lock and one m.mu.Lock
// (no recursion), and then performs the GENESIS → READY transition with the
// SAME pair of locks still held. Because the c.mu write lock is held across
// the check and the transition, no metadata update (c.mu Lock) can interleave
// between them: a revocation that linearizes before the promotion is
// observed in the validation and the promotion is rejected; a revocation that
// linearizes after is a normal, permitted revocation that the next
// re-evaluation acts on. The commit path is preserved: a commit that
// linearizes before the promotion holds c.mu RLock (compatible with our
// write lock only when the commit has not yet acquired it), so the promotion
// queues behind the commit and revalidates after it; the commit's own
// admission re-checks the complete finalization invariant (see authorized())
// and rejects the write if the finalization has been invalidated. The only
// operation performed inside the critical section that may touch the store is
// a single-row, nonblocking binding read-through (no network I/O), performed
// only when the in-memory cache is empty on a fresh restart.
//
// It never creates a binding (the seed is already bound when in GENESIS)
// and never rewrites the authoritative metadata (that is the operator's
// finalization Job's job, performed externally).
//
// The transition table is still enforced (GENESIS → READY is permitted; the
// STARTING → DISCOVERING reconciliation is legal; any other current state is
// not, so this method cannot force an arbitrary state).
func (c *Controller) promoteFromGenesis() error {
	// The promotion is a check-then-act operation: the validation (state,
	// metadata, freshness, stopped, finalization invariant, binding) and the
	// READY transition must be atomic with respect to a concurrent
	// authoritative-metadata revocation. To achieve that, both the validation
	// and the transition happen under the SAME pair of locks — the
	// controller's write lock (c.mu.Lock) and the machine's write lock
	// (m.mu.Lock) — acquired in the fixed order c.mu → m.mu (the same order
	// used by AuthorizeGenesis, setReady, and the commit guard) and released
	// in reverse order. The c.mu write lock serializes the entire
	// check-and-transition against a metadata update (evaluateMetadata, which
	// takes c.mu.Lock), so a revocation cannot interleave between the check
	// and the transition: a revocation that linearizes before the promotion
	// is observed in the validation and the promotion is rejected; a
	// revocation that linearizes after is a normal, permitted revocation
	// acted on by the next re-evaluation.
	//
	// c.mu.Lock is acquired here (not re-acquired inside) so the promotion
	// never takes the controller lock recursively. m.mu is taken while c.mu
	// is held and released before c.mu. No network, SQLite, or Kubernetes
	// operation is performed inside the critical section (the validation
	// reads only in-memory state: the machine, the accepted-metadata cache,
	// the freshness timestamp, and the binding cache).
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.machine
	m.mu.Lock()
	defer m.mu.Unlock()

	if c.stopped {
		return errors.New("cannot promote from GENESIS: controller is stopped")
	}

	// Seed restart reconciliation (Case B): a fresh controller (a new process)
	// whose initial metadata read observes a fully finalized record with a
	// matching durable binding is still in STARTING (the machine is created
	// in STARTING; the STARTING → DISCOVERING reconciliation has not yet
	// run). The state must be reconciled to DISCOVERING before the
	// DISCOVERING → READY promotion (the legal transition table; STARTING
	// does not transition directly to READY). Only the designated seed may
	// be reconciled here; a non-seed with a matching binding is not
	// auto-promoted (it requires its own initial synchronization).
	//
	// m.mu is held, so the reconcile is performed via transitionLocked (a
	// pure in-memory transition; no I/O). The transition table is enforced
	// by the machine (STARTING → DISCOVERING is legal). After the reconcile
	// the state is re-read under the held m.mu (it is in the same critical
	// section; no metadata update can interleave).
	state := m.state
	if state == STARTING && c.cfg.SeedIdentity != "" && c.cfg.SeedIdentity == c.cfg.NodeID {
		if err := m.transitionLocked(DISCOVERING, "seed restart: matching durable binding observed; reconciling to authorized state before promotion"); err != nil {
			return fmt.Errorf("cannot promote from GENESIS: state reconciliation from STARTING was not possible (state=%s): %w", m.state, err)
		}
		state = m.state
	}

	if state != GENESIS && state != DISCOVERING {
		return fmt.Errorf("cannot promote from GENESIS: lifecycle state is %s (only GENESIS or the STARTING-reconciled DISCOVERING are promotable)", m.state)
	}

	// The remaining validation is the same fail-closed invariant the
	// promotion has always required (fresh observation, complete finalization
	// for this seed, matching binding), now read under the held c.mu + m.mu
	// pair so it is atomic with the transition.
	if err := c.validateGenesisPromotion(); err != nil {
		return err
	}
	if err := m.transitionLocked(READY, fmt.Sprintf(
		"genesis finalized: observed completely finalized metadata (Initialized=true, GenesisAuthorized=false, GenesisConsumer=%s, DataLossProtected=true) with matching binding (cluster=%s, epoch=%s); promotion to READY",
		c.accepted.GenesisConsumer, c.accepted.ClusterID, c.accepted.EpochID,
	)); err != nil {
		return err
	}
	return nil
}

// validateGenesisPromotion validates the complete genesis-promotion invariant
// under the held c.mu (write) + m.mu (write) locks (it is called from
// promoteFromGenesis with both already held; it acquires no locks). It
// returns an error when the promotion must not proceed: the observation is
// missing, stale, or not fresh; the record is not a complete finalization for
// the designated seed; or the local binding is missing or does not match. It
// does not mutate any state (the transition is performed by the caller). The
// only operation that may touch the store is a binding read-through (a fast,
// single-row GetBinding with no network I/O) performed when the in-memory
// cache is empty on a fresh restart; it is held inside the critical section,
// which is acceptable because it is a single-row read, not a network or
// multi-row SQLite operation.
func (c *Controller) validateGenesisPromotion() error {
	meta := c.accepted
	if meta == nil {
		return errors.New("cannot promote from GENESIS: no authoritative metadata observation")
	}
	if !c.finalizationComplete(meta) {
		return fmt.Errorf("cannot promote from GENESIS: authoritative metadata is not a completely finalized record for the designated seed (Initialized=%v GenesisAuthorized=%v GenesisConsumer=%q DataLossProtected=%v; the operator's finalization CAS must set all four atomically)",
			meta.Initialized, meta.GenesisAuthorized, meta.GenesisConsumer, meta.DataLossProtected)
	}
	freshAt := c.freshAt
	stale := false
	if !freshAt.IsZero() {
		stale = c.now().After(freshAt.Add(c.cfg.MetadataPollInterval * 2))
	}
	if c.authorityState != "fresh" || stale {
		return fmt.Errorf("cannot promote from GENESIS: authoritative metadata is not fresh (authority=%s, stale=%v; a stale or invalid observation must not authorize promotion)", c.authorityState, stale)
	}
	binding := c.bindingVal
	if binding == nil && c.binding != nil {
		// Read-through for the durable binding (the same way LocalBinding and
		// reevaluateState do it). The cache is warmed only by the
		// metadata-evaluation path and by AuthorizeGenesis, so on a fresh
		// restart the cache may be nil while the durable binding exists.
		b, err := c.binding.GetBinding()
		if err == nil {
			binding = b
		} else {
			c.logger.Warn("reading durable binding for promotion failed",
				slog.Any("error", err),
			)
		}
	}
	if binding == nil {
		return errors.New("cannot promote from GENESIS: no local database binding (the seed must be bound before promotion)")
	}
	if !binding.MatchesMetadata(*meta) {
		return fmt.Errorf("cannot promote from GENESIS: local binding (%s/%s) does not match authoritative metadata (%s/%s)",
			binding.ClusterID, binding.EpochID,
			meta.ClusterID, meta.EpochID)
	}
	return nil
}

// genesisEntryOutcome is the result of the genesis-entry guard check.
type genesisEntryOutcome int

const (
	// genesisEntryOK: the guard permits the GENESIS transition.
	genesisEntryOK genesisEntryOutcome = iota
	// genesisEntryAlreadyGenesis: the state is already GENESIS (a
	// legitimate idempotent success; no transition needed).
	genesisEntryAlreadyGenesis
	// genesisEntryRevoked: the authorization was revoked or became stale
	// (the current observation is not a valid pending grant); the seed must
	// not enter GENESIS.
	genesisEntryRevoked
	// genesisEntryStateInvalid: the lifecycle state does not permit a
	// transition to GENESIS.
	genesisEntryStateInvalid
)

// genesisEntryGuarded reports the outcome of the genesis-entry guard for the
// given just-written binding. It is called with BOTH the c.mu write lock and
// the machine's m.mu write lock ALREADY held (by AuthorizeGenesis, in the
// fixed order c.mu → m.mu; see the caller). It reads the CURRENT
// authoritative observation (c.accepted, which the metadata worker writes
// under the c.mu write lock) and the CURRENT machine state (under the held
// m.mu, so the read and the caller's transition are serialized against each
// other).
//
// It distinguishes the four outcomes (Stage 1B correction 5):
//   - genesisEntryOK: fresh, valid, complete pending grant; state is
//     DISCOVERING (or another state permitting GENESIS); binding matches.
//   - genesisEntryAlreadyGenesis: state is already GENESIS (idempotent
//     success; no transition needed).
//   - genesisEntryRevoked: the current observation is not a valid pending
//     grant (revoked, incoherent, stale, or not fresh); the seed must not
//     enter GENESIS.
//   - genesisEntryStateInvalid: the state does not permit a transition to
//     GENESIS (e.g. STARTING, SYNCING, READY, etc.).
//
// It performs no network I/O (all reads are in-memory under the held locks)
// and does not acquire any additional lock (both c.mu and m.mu are already
// held; the c.mu → m.mu order is preserved by the caller).
func (c *Controller) genesisEntryGuarded(b *DatabaseBinding) genesisEntryOutcome {
	if c.stopped {
		return genesisEntryRevoked
	}
	// Idempotent-restart case: if the state is already GENESIS (the seed was
	// already bound and in GENESIS), no transition is needed — the guard
	// returns genesisEntryAlreadyGenesis and AuthorizeGenesis returns nil
	// (a no-op). The state is read under the held m.mu, so a concurrent
	// transition to GENESIS is observed (no double transition).
	if c.machine.state == GENESIS {
		return genesisEntryAlreadyGenesis
	}
	meta := c.accepted
	if meta == nil {
		return genesisEntryRevoked
	}
	// Freshness: a stale or invalid observation must not authorize genesis
	// entry.
	if c.authorityState != "fresh" {
		return genesisEntryRevoked
	}
	freshAt := c.freshAt
	if !freshAt.IsZero() {
		if c.now().After(freshAt.Add(c.cfg.MetadataPollInterval * 2)) {
			return genesisEntryRevoked
		}
	}
	// The complete pending-grant invariant.
	if !c.pendingGenesisAuthorization(meta) {
		return genesisEntryRevoked
	}
	if !meta.MatchesBinding(b) {
		return genesisEntryRevoked
	}
	// The state must permit a transition to GENESIS.
	if !canTransition(c.machine.state, GENESIS) {
		return genesisEntryStateInvalid
	}
	return genesisEntryOK
}

// isRevokedGenesisObservation reports whether the authoritative metadata
// record is a confirmed, same-epoch genesis authorization revocation — the
// state reached when the operator revokes the one-time genesis grant before
// finalization (Initialized=false, GenesisAuthorized=false, GenesisConsumer="",
// DataLossProtected=false) while the local binding still matches the (same)
// epoch. A matching ClusterID/EpochID alone does NOT prove the authorization
// has been restored; this is a recovery incident, not a restoration.
//
// It is the gate that keeps a seed in RECOVERY_REQUIRED when it repeatedly
// observes an identical revoked observation (Stage 1B recovery-persistence
// correction). The observation must be FRESH and VALID (the metadata worker
// validates before calling reevaluateState, and the revocation branch of
// reconcileSeedRestartLocked requires authority "fresh"), so a temporary
// Kubernetes API failure is NOT treated as a confirmed revocation: it fails
// closed without clearing or generating a permanent recovery incident.
func (c *Controller) isRevokedGenesisObservation(meta *ClusterMetadata) bool {
	if meta == nil {
		return false
	}
	return !meta.Initialized && !meta.GenesisAuthorized && meta.GenesisConsumer == "" && !meta.DataLossProtected
}

// pendingGenesisAuthorization reports whether the authoritative metadata
// record is an outstanding, unconsumed, unprotected genesis authorization
// (the Stage 1B pending-grant invariant: Initialized=false,
// GenesisAuthorized=true, GenesisConsumer="", DataLossProtected=false). This
// is the gate for the seed's entry into and (re)establishment in the pending
// GENESIS state: a revoked (GenesisAuthorized=false) or incoherent
// (DataLossProtected=true, or a consumer already set) record must not place
// the seed in GENESIS (a revocation must not enter GENESIS).
func (c *Controller) pendingGenesisAuthorization(meta *ClusterMetadata) bool {
	if meta == nil {
		return false
	}
	return !meta.Initialized && meta.GenesisAuthorized && meta.GenesisConsumer == "" && !meta.DataLossProtected
}

// finalizationComplete reports whether the authoritative metadata record is a
// COMPLETELY finalized observation for the designated seed (the Stage 1B
// finalization invariant). Requiring the complete invariant — not merely
// Initialized=true — is load-bearing: the 5A metadata validator intentionally
// accepts some legacy fixture combinations, so Initialized=true alone is NOT
// proof of finalization. A coherent, fresh, finalized authoritative record has
// all four finalization fields set by the operator's finalization CAS in a
// single atomic update, and the consumer must be THIS seed (a different or
// missing consumer means the record does not authorize THIS seed).
func (c *Controller) finalizationComplete(meta *ClusterMetadata) bool {
	if meta == nil {
		return false
	}
	if !meta.Initialized {
		return false
	}
	if meta.GenesisAuthorized {
		return false
	}
	if meta.GenesisConsumer == "" {
		return false
	}
	if c.cfg.SeedIdentity == "" || meta.GenesisConsumer != c.cfg.SeedIdentity {
		return false
	}
	if !meta.DataLossProtected {
		return false
	}
	return true
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

// SetGenesisRestoredForTest is an exported test-only method that restores the
// controller to the pending GENESIS state for the designated seed, bypassing
// the transition table (it performs DISCOVERING → GENESIS directly; the
// caller is responsible for the legal STARTING → DISCOVERING prerequisite).
// It exists SOLELY to support cross-package tests that need a GENESIS
// controller without the full metadata-evaluation or durable-SQLite-binding
// flow. Production code must NOT call this: the production paths to GENESIS
// are reevaluateState (the seed restart reconciliation, Case A) and
// AuthorizeGenesis (the explicit, operator-authorized entry), both of which
// validate the seed identity, the fresh authorized metadata, and the durable
// binding before transitioning. It still enforces that the transition target
// is GENESIS (the method cannot force an arbitrary state).
//
// Lock-order contract: c.mu RLock → m.mu Lock (via transitionTo).
func (c *Controller) SetGenesisRestoredForTest(reason string) error {
	return c.transitionTo(GENESIS, reason)
}

// AuthorizeGenesis is the explicit, operator-authorized genesis entry path
// (Milestone 5B, Stage 1B). It is called by the application's startup path
// (and by the genesis-entry loop) for the designated seed, and it is a no-op
// for any non-seed replica (c.cfg.SeedIdentity does not match the local
// identity). It is the production path by which a seed with an EMPTY,
// UNBOUND local database (a fresh or first-time bootstrap) binds the
// database to the authorized epoch and enters GENESIS. The seed restart case
// (the database is ALREADY bound to the authorized epoch) is primarily
// reconciled by reevaluateState (the matching binding is observed on the
// metadata read, and the state is restored to GENESIS or promoted to READY
// without rebinding); this method is idempotent for that case (the durable
// Bind is a no-op) but is a no-op when the state is already GENESIS or
// READY (the seed-entry loop is not started on a replica that is already in
// a terminal state).
//
// The authorization is GATED: it is permitted only when ALL of the following
// hold (the fail-closed policy; an empty database or missing peers alone
// never authorize genesis):
//
//   - the local replica identity is EXACTLY the designated seed
//     (c.cfg.SeedIdentity == c.cfg.NodeID); any other replica is a no-op;
//   - the controller has not been stopped;
//   - a FRESH, valid authoritative metadata observation exists (authority
//     "fresh", not stale) — an expired or invalid observation cannot
//     authorize genesis;
//   - the metadata reports an outstanding, unconsumed genesis
//     authorization: Initialized=false, GenesisAuthorized=true,
//     GenesisConsumer="" (the one-time token is present and unspent);
//   - DataLossProtected=false (the cluster is pre-finalization; a
//     data-loss-protected cluster cannot re-enter genesis);
//   - the local database has NO existing binding and NO messages (a
//     nonempty legacy database must not silently acquire a binding);
//   - no conflicting authoritative epoch or recoverable bound peer has been
//     established (a mismatch with a prior binding, or a recovered
//     RECOVERY_REQUIRED lockout, blocks the entry).
//
// On success it performs the atomic, idempotent SQLite
// BindingStore.Bind(clusterID, epochID, generation), then RE-READS the
// authoritative metadata (to detect a revocation or epoch change that
// linearized between the authorization snapshot and the Bind — the operation
// is NOT atomic across Kubernetes and SQLite), and only when the CURRENT
// record is still a fresh, valid, outstanding, unconsumed, unprotected
// authorization that matches the just-written binding does it warm the
// binding cache and transition DISCOVERING → GENESIS. The seed remains in
// GENESIS while the metadata reports Initialized=false: public writes stay
// blocked (the 5A admission gate rejects non-ready states and
// Initialized=false) and SetReady fails (the 5A SetReady gate rejects
// Initialized=false). The seed is promoted to READY only after it observes
// the completely finalized metadata (see promoteFromGenesis in
// reevaluateState).
//
// It is idempotent on restart: if the local database is already bound to the
// exact authorized cluster/epoch, the Bind is a no-op and the method returns
// success (the seed is (re-)established in GENESIS). A conflicting binding
// (a different cluster or epoch), a nonempty unbound database, a malformed
// binding, a revoked or changed authorization observed in the post-Bind
// re-read, or any other fail-closed condition returns an error and the
// replica does NOT enter GENESIS: the binding and messages are never deleted
// or modified (a binding just written is PRESERVED; the operator's
// finalization CAS or the 5C recovery procedure is the only path that
// changes it), and the operator must intervene.
//
// Lock-order contract: the method reads the current binding under c.mu
// RLock, performs the durable Bind (which acquires the SQLite write lock, an
// independent lock outside the controller), re-reads the authoritative
// metadata (a short, nonblocking in-memory operation for the fake source; a
// fast single Get for the real Kubernetes source), warms the binding cache
// under c.mu Lock, and finally transitions via transitionTo (c.mu RLock →
// m.mu Lock). The Bind is the only operation that may block (a short SQLite
// transaction); it is not held across any network operation.
func (c *Controller) AuthorizeGenesis(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Only the designated seed may take this path. The seed identity is
	// matched as an exact, case-sensitive string; an empty SeedIdentity
	// (no seed designated) means this replica is not the seed.
	if c.cfg.SeedIdentity == "" || c.cfg.SeedIdentity != c.cfg.NodeID {
		return nil
	}

	c.mu.RLock()
	stopped := c.stopped
	c.mu.RUnlock()
	if stopped {
		return errors.New("cannot authorize genesis: controller is stopped")
	}

	// Read a fresh, valid authoritative observation. An expired or invalid
	// observation (a temporary metadata loss) must not authorize genesis:
	// the fail-closed policy requires a fresh, valid observation.
	snap := c.authorization()
	if snap.Metadata == nil {
		return errors.New("cannot authorize genesis: no authoritative metadata observation")
	}
	if snap.AuthorityState != "fresh" || snap.Stale {
		return fmt.Errorf("cannot authorize genesis: authoritative metadata is not fresh (authority=%s, stale=%v)", snap.AuthorityState, snap.Stale)
	}
	meta := *snap.Metadata
	if meta.Initialized {
		return errors.New("cannot authorize genesis: authoritative metadata reports Initialized=true (a completed epoch exists; genesis is not permitted)")
	}
	if !meta.GenesisAuthorized {
		return errors.New("cannot authorize genesis: no outstanding genesis authorization (GenesisAuthorized=false; the one-time token is absent or already consumed)")
	}
	if meta.GenesisConsumer != "" {
		return fmt.Errorf("cannot authorize genesis: genesis consumer already set (%q; the authorization is consumed)", meta.GenesisConsumer)
	}
	if meta.DataLossProtected {
		return errors.New("cannot authorize genesis: data-loss protection is armed (DataLossProtected=true; a protected cluster cannot re-enter genesis)")
	}

	// Read the current local binding (a read-through: the cache when
	// present, otherwise the durable binding from the store).
	binding := c.LocalBinding()

	// The local database must have no existing binding and no messages.
	// An existing binding is handled below (the idempotent restart case).
	if binding == nil {
		// No binding: the database must be empty. A nonempty legacy
		// database without a binding must not silently acquire one.
		count, cerr := c.binding.CountMessages()
		if cerr != nil {
			return fmt.Errorf("cannot authorize genesis: counting local messages: %w", cerr)
		}
		if count > 0 {
			return fmt.Errorf("cannot authorize genesis: local database contains %d unbound messages (a nonempty legacy database must not be bound; fail-closed, operator intervention required)", count)
		}
	} else {
		// An existing binding: it must match the authorized epoch (the
		// idempotent restart case — the seed was already bound to this
		// cluster/epoch and is restarting). A conflicting binding (a
		// different cluster or epoch) is a fail-closed mismatch: the
		// binding and messages are never overwritten, and the replica must
		// not enter GENESIS.
		if !binding.MatchesMetadata(meta) {
			return fmt.Errorf("cannot authorize genesis: existing local binding (%s/%s) conflicts with the authorized epoch (%s/%s); fail-closed, the binding is preserved",
				binding.ClusterID, binding.EpochID, meta.ClusterID, meta.EpochID)
		}
	}

	// Perform the atomic, idempotent SQLite Bind. This is the single
	// durable write: it creates the binding on an empty database, is a
	// no-op when the binding already matches, and returns an error (without
	// modifying the database) when the database is nonempty without a
	// binding or the binding conflicts. The generation is the authorized
	// metadata's generation (diagnostic only; the binding is keyed on
	// ClusterID + EpochID).
	//
	// The idempotent-restart case: if a matching binding already exists
	// (the seed was bound on a prior attempt and is restarting), the
	// durable Bind is a no-op (it would not modify the database) and the
	// existing binding is reused as-is — no re-bind, no new binding
	// object. Only the empty-database case (no existing binding) performs
	// the durable Bind that creates the binding.
	var b *DatabaseBinding
	var bindErr error
	if binding != nil {
		// The binding already matches the authorized epoch (checked above);
		// reuse it unchanged.
		b = binding
	} else {
		b, bindErr = c.binding.Bind(meta.ClusterID, meta.EpochID, meta.Generation)
	}
	if bindErr != nil {
		return fmt.Errorf("cannot authorize genesis: binding local database to authorized epoch: %w", bindErr)
	}

	// Re-read the authoritative metadata after the durable Bind. The
	// authorization snapshot read and the durable Bind are NOT atomic across
	// Kubernetes and SQLite: a metadata change (the operator revoking the
	// authorization or changing the epoch) may linearize between the snapshot
	// read and the Bind. Re-reading the authoritative record after the Bind
	// (the single durable write) ensures that a revoked or changed
	// authorization is observed before the GENESIS transition: the
	// transition is only permitted when the CURRENT record is still a
	// fresh, valid, outstanding, unconsumed authorization that matches the
	// just-written binding. A revoked or changed record is rejected (no
	// unauthorized GENESIS transition), and the binding just written is
	// PRESERVED (the operator's finalization CAS or the 5C recovery
	// procedure is the only path that changes it; it is never deleted or
	// overwritten).
	reMeta, reErr := c.source.Read(ctx)
	if reErr != nil {
		// The authoritative record is currently unavailable: fail closed.
		// The binding is preserved (it is durable); a later AuthorizeGenesis
		// attempt (the seed-entry loop) will succeed when the record is
		// available again.
		return fmt.Errorf("cannot authorize genesis: authoritative metadata became unavailable after the local binding was written (fail-closed; the binding is preserved and the operator's finalization Job or the 5C recovery procedure is the only path that changes it): %w", reErr)
	}
	if vErr := reMeta.Validate(); vErr != nil {
		return fmt.Errorf("cannot authorize genesis: authoritative metadata is malformed after the local binding was written (fail-closed; the binding is preserved): %w", vErr)
	}
	if !reMeta.MatchesBinding(b) {
		return fmt.Errorf("cannot authorize genesis: the local binding (%s/%s) no longer matches the authoritative metadata (%s/%s) after the Bind (the epoch changed or was revoked); fail-closed, the binding is preserved",
			b.ClusterID, b.EpochID, reMeta.ClusterID, reMeta.EpochID)
	}
	if reMeta.Initialized {
		return fmt.Errorf("cannot authorize genesis: the authoritative metadata now reports Initialized=true (the authorization was consumed after the Bind); fail-closed, the binding is preserved and the seed is not placed in GENESIS (it will be reconciled by reevaluateState on the next metadata read)")
	}
	if !reMeta.GenesisAuthorized || reMeta.GenesisConsumer != "" || reMeta.DataLossProtected {
		return fmt.Errorf("cannot authorize genesis: the authoritative metadata no longer reports an outstanding, unconsumed, unprotected genesis authorization after the Bind (genesisAuthorized=%v, consumer=%q, protected=%v); fail-closed, the binding is preserved",
			reMeta.GenesisAuthorized, reMeta.GenesisConsumer, reMeta.DataLossProtected)
	}

	// Warm the binding cache and guard the GENESIS transition atomically
	// (Stage 1B correction 5). The critical section holds BOTH the
	// controller's write lock (c.mu.Lock) and the machine's write lock
	// (m.mu.Lock) in the fixed order c.mu → m.mu (the same order as every
	// other transition and as CommitUnderAuthorization), and releases them
	// in reverse order. Both the binding-cache warm and the
	// genesisEntryGuarded + transition read/write machine state under m.mu,
	// so the guard's read of the current machine state and the transition's
	// write of the new state are serialized as one atomic operation against
	// a concurrent AuthorizeGenesis or a concurrent state change: a
	// revocation that the metadata worker accepted before this point is
	// observed in c.accepted and the transition is rejected, and no two
	// AuthorizeGenesis calls can both read "not yet GENESIS" and both
	// transition.
	//
	// The binding cache is warmed under the c.mu write lock (mutating it
	// under a read lock would be a data race), and the authorization
	// generation is advanced so admission decisions taken after the binding
	// are distinguishable from those before it.
	c.mu.Lock()
	c.bindingVal = b
	c.authGen++
	m := c.machine
	m.mu.Lock()
	guardOutcome := c.genesisEntryGuarded(b)
	var transErr error
	if guardOutcome == genesisEntryOK {
		// The transition is performed here, with m.mu held, via the
		// machine's lock-free transition (the c.mu → m.mu order is preserved:
		// m.mu Lock is acquired after c.mu Lock and released before it).
		// The transition table is enforced (a replica that is not in
		// DISCOVERING or a state permitting GENESIS is not forced); an
		// invalid transition is logged, not panicked.
		transErr = m.transitionLocked(GENESIS, fmt.Sprintf(
			"genesis authorized and bound: seed %s bound to cluster=%s epoch=%s (Initialized=false; awaiting operator finalization)",
			c.cfg.NodeID, meta.ClusterID, meta.EpochID,
		))
	}
	m.mu.Unlock()
	c.mu.Unlock()
	switch {
	case transErr != nil:
		c.logger.Warn("lifecycle transition rejected",
			slog.String("from", m.State().String()),
			slog.String("to", GENESIS.String()),
			slog.Any("error", transErr),
		)
		return transErr
	case guardOutcome == genesisEntryAlreadyGenesis:
		// Idempotent success: the seed is already in GENESIS. The binding is
		// preserved and the method returns nil (a legitimate no-op).
		c.logger.Info("genesis: seed is already in GENESIS (idempotent)",
			slog.String("node", c.cfg.NodeID),
		)
		return nil
	case guardOutcome == genesisEntryRevoked:
		// The authorization was revoked or became stale between the
		// post-Bind re-read and this point. The seed does NOT enter GENESIS;
		// the durable binding is preserved (it was written and cached above).
		// The next re-evaluation (reevaluateState) will transition the seed
		// to RECOVERY_REQUIRED (a revoked authorization must fail closed).
		c.logger.Warn("genesis: authorization rejected by guard (revoked or stale); seed not placed in GENESIS",
			slog.String("node", c.cfg.NodeID),
			slog.String("cluster", meta.ClusterID),
			slog.String("epoch", meta.EpochID),
		)
		return fmt.Errorf(
			"cannot authorize genesis: the authorization was revoked or became stale after the local binding was written (guard rejected: not a valid pending grant); the durable binding is preserved and the seed is not placed in GENESIS (the next re-evaluation will reconcile the state)",
		)
	case guardOutcome == genesisEntryStateInvalid:
		// The lifecycle state does not permit a transition to GENESIS.
		c.logger.Warn("genesis: guard rejected (invalid lifecycle state for GENESIS transition)",
			slog.String("node", c.cfg.NodeID),
			slog.String("state", m.State().String()),
		)
		return fmt.Errorf(
			"cannot authorize genesis: the lifecycle state (%s) does not permit a transition to GENESIS; the durable binding is preserved",
			m.State().String(),
		)
	default:
		// genesisEntryOK: the transition succeeded (transErr is nil).
		c.logger.Info("lifecycle state transition",
			slog.String("from", m.Snapshot().PreviousState.String()),
			slog.String("to", GENESIS.String()),
			slog.String("reason", "genesis authorized and bound (guarded)"),
		)
	}
	c.logger.Info("genesis: local database bound to authorized epoch",
		slog.String("cluster", meta.ClusterID),
		slog.String("epoch", meta.EpochID),
		slog.Int("generation", meta.Generation),
		slog.String("node", c.cfg.NodeID),
	)
	return nil
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
	// SeedIdentity is the designated seed identity of the controller
	// (Config.SeedIdentity). It is empty when no seed is designated. A
	// nonempty value marks a genesis-managed lifecycle configuration, for
	// which ordinary write admission additionally requires the complete
	// finalization invariant (see authorized / finalizationComplete). It is
	// a config value (immutable for the controller's lifetime), so copying
	// it into the snapshot is safe and keeps the snapshot self-contained.
	SeedIdentity string
}

// authorized reports the core authorization decision shared by all admission
// checks and by readiness. An operation may be admitted only when ALL of the
// following hold (the fail-closed policy; see the package docs):
//
//   - the lifecycle state is a ready state (READY or DEGRADED);
//   - the authoritative metadata is present, FRESH (not stale), and valid;
//   - the record is COMPLETELY finalized for the designated seed (Stage 1B
//     correction 2): Initialized=true, GenesisAuthorized=false,
//     GenesisConsumer=<this seed>, DataLossProtected=true. For a
//     genesis-managed lifecycle configuration (a designated SeedIdentity),
//     ordinary write admission must not succeed on an initialized-but-
//     unfinalized or incomplete record — a matching durable binding and
//     Initialized=true alone are not proof that the one-time authorization
//     was consumed and the data-loss protection armed.
//   - a local database binding exists;
//   - the binding's ClusterID and EpochID match the authoritative metadata;
//   - no recovery lockout exists (the state is not RECOVERY_REQUIRED);
//   - the controller has not been stopped.
//
// A temporary metadata loss (authority "invalid" or "missing") or a stale
// observation therefore fails closed: it does NOT leave an unrestricted
// READY decision. Readiness and the peer status endpoint use this same
// decision as the public handlers, so they cannot diverge.
//
// Compatibility (Stage 1B correction 2): when no designated seed is configured
// (SeedIdentity is empty — the legacy 5A fixtures and the lifecycle-disabled
// 7A behavior have no genesis-managed lifecycle), the finalization invariant
// is not enforced and only Initialized is required, preserving the existing
// M4/5A behavior.
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
	if s.genesisManaged() && !s.finalizationComplete() {
		// A genesis-managed lifecycle (a designated SeedIdentity) requires the
		// COMPLETE finalization invariant for admission: an initialized-but-
		// unfinalized or incomplete record must not authorize ordinary writes
		// (the one-time authorization is not consumed, or the data-loss
		// protection is not armed, or the consumer is not this seed).
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

// bootstrapAuthorized reports the dedicated bootstrap baseline fetch
// authorization decision (Milestone 5B, Stage 2). An operation may be admitted
// as a bootstrap fetch only when ALL of the following hold:
//
//   - the lifecycle state is exactly SYNCING (the only state in which a
//     replica obtains its initial baseline; a ready state would use the
//     ordinary authorization, a recovery or genesis state is not fetching a
//     baseline);
//   - the controller has not been stopped;
//   - the authoritative metadata is present;
//   - the authoritative metadata is initialized (Initialized=true: the
//     cluster has a baseline to fetch);
//   - for a genesis-managed lifecycle configuration (a designated
//     SeedIdentity), the record is COMPLETELY finalized (data-loss protection
//     armed, one-time authorization consumed, consumer is this seed);
//   - the authoritative metadata is FRESH (not stale) and valid;
//   - a local database binding exists;
//   - the binding's ClusterID and EpochID match the authoritative metadata
//     (a missing or conflicting binding, or a wrong-epoch observation, fails
//     closed).
//
// This is the narrow, dedicated decision for a joining, epoch-bound SYNCING
// replica to fetch a baseline. It is intentionally distinct from authorized
// (which requires a ready state and authorizes public writes and ordinary
// replication): a SYNCING replica is authorized to FETCH but not to write,
// gossip, or serve. A temporary metadata loss or a stale observation fails
// closed: a bootstrap fetch is not permitted on an old or lost observation.
func (s AuthorizationSnapshot) bootstrapAuthorized() bool {
	if s.Stopped {
		return false
	}
	if s.State != SYNCING {
		return false
	}
	if s.Metadata == nil {
		return false
	}
	if !s.Metadata.Initialized {
		return false
	}
	if s.genesisManaged() && !s.finalizationComplete() {
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

// bootstrapAuthorizationReason returns a stable diagnostic explaining the
// bootstrap fetch authorization decision (see bootstrapAuthorized).
func (s AuthorizationSnapshot) bootstrapAuthorizationReason() string {
	if s.Stopped {
		return "lifecycle controller stopped"
	}
	if s.State != SYNCING {
		return "bootstrap fetch requires the SYNCING state (state=" + s.State.String() + ")"
	}
	if s.Metadata == nil {
		return "no authoritative metadata observation"
	}
	if !s.Metadata.Initialized {
		return "authoritative metadata reports Initialized=false (no baseline to fetch)"
	}
	if s.genesisManaged() && !s.finalizationComplete() {
		return "authoritative metadata is not a completely finalized record for the designated seed (a joining replica cannot fetch from an unfinalized epoch)"
	}
	if s.AuthorityState != "fresh" {
		return "authoritative metadata not fresh (authority=" + s.AuthorityState + ")"
	}
	if s.Stale {
		return "authoritative metadata observation is stale"
	}
	if s.Binding == nil {
		return "no local database binding (the replica must be bound before fetching a baseline)"
	}
	if !s.Binding.MatchesMetadata(*s.Metadata) {
		return "local binding does not match authoritative epoch (binding=" +
			s.Binding.ClusterID + "/" + s.Binding.EpochID +
			" metadata=" + s.Metadata.ClusterID + "/" + s.Metadata.EpochID + ")"
	}
	return "bootstrap fetch authorized"
}

// genesisManaged reports whether this snapshot belongs to a genesis-managed
// lifecycle configuration (a designated SeedIdentity). When true, ordinary
// write admission additionally requires the complete finalization invariant
// (see finalizationComplete); when false (no designated seed — legacy 5A
// fixtures or lifecycle-disabled 7A behavior), only Initialized is required.
//
// This is a method (not a stored field) so the existing test fixtures and
// external constructors of AuthorizationSnapshot (which set only the fields
// they care about) remain compatible: a zero-value snapshot reports
// genesis-managed=false and the legacy Initialized-only behavior applies.
func (s AuthorizationSnapshot) genesisManaged() bool {
	return s.SeedIdentity != ""
}

// finalizationComplete reports whether the snapshot's authoritative metadata
// is a COMPLETELY finalized record for the designated seed (the Stage 1B
// finalization invariant: Initialized=true, GenesisAuthorized=false,
// GenesisConsumer=<this seed>, DataLossProtected=true). This is the admission
// counterpart to the controller's finalizationComplete (the promotion gate).
func (s AuthorizationSnapshot) finalizationComplete() bool {
	m := s.Metadata
	if m == nil {
		return false
	}
	if !m.Initialized || m.GenesisAuthorized || m.GenesisConsumer == "" || !m.DataLossProtected {
		return false
	}
	return s.SeedIdentity != "" && m.GenesisConsumer == s.SeedIdentity
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
		SeedIdentity:   c.cfg.SeedIdentity,
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

// BootstrapAuthorizationSnapshot returns a consistent, authorized view of the
// requester's bootstrap fetch authorization (Milestone 5B, Stage 2, Issue 1
// and Issue 4). It implements the antientropy.BootstrapAuthorizationSource
// interface: it reads the controller's single consistent authorization
// snapshot under its read locks (c.mu RLock → m.mu RLock) and returns the
// expected RequestContext (the accepted metadata's ClusterID and EpochID) and
// the DEDICATED bootstrap-authorized decision (bootstrapAuthorized: a bound,
// epoch-bound SYNCING replica with a fresh, valid, matching observation and a
// matching durable binding, not stopped).
//
// The returned context and the authorized flag come from the SAME locked
// snapshot, so the engine's pinned expected context and its admission decision
// are always consistent (no combining decisions from two different snapshots).
//
// When the controller is disabled (lifecycle protection off), the method
// returns a zero context and authorized=true (a passthrough: the engine does
// not pin or revalidate a context in the disabled path). When the snapshot is
// not bootstrap-authorized (revocation, state change, stop, stale/invalid
// observation, binding change, or an empty context), it returns authorized=false
// with the zero context (the engine fails the page before issuing a request).
func (c *Controller) BootstrapAuthorizationSnapshot() (RequestContext, bool, error) {
	if !c.cfg.Enabled {
		return RequestContext{}, true, nil
	}
	snap := c.authorization()
	if !snap.bootstrapAuthorized() {
		return RequestContext{}, false, nil
	}
	meta := snap.Metadata
	if meta == nil {
		return RequestContext{}, false, nil
	}
	return RequestContext{ClusterID: meta.ClusterID, EpochID: meta.EpochID}, true, nil
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

// CommitUnderBootstrapAuthorization executes a commit callback (installing a
// fetched bootstrap baseline into the local store) while holding the
// controller's authorization read lock (c.mu RLock) and the lifecycle state
// machine's read lock (m.mu RLock), and retains both for the duration of the
// callback. It is the commit-boundary counterpart to CheckBootstrapFetch: a
// successful bootstrap HTTP request does NOT by itself authorize installing
// its response; the installation is revalidated at the actual local write
// boundary.
//
// The callback runs only when the bootstrap authorization is STILL valid at
// commit time: the requester remains a bound, epoch-bound SYNCING replica
// (fresh, valid, matching authoritative observation plus a matching durable
// binding, no recovery lockout, not stopped). A revocation — a state change
// out of SYNCING, an epoch/metadata change, a stale or invalid observation,
// a binding change, or Stop — that linearizes before the guard enters is
// observed in the snapshot and the callback is not executed (a rejection
// error is returned). A revocation after the callback completes is a normal,
// permitted revocation that cannot be undone retroactively; already-committed
// valid data and the durable binding are preserved (this method never deletes
// or resets the store).
//
// The callback runs under the same lock order as CommitUnderAuthorization
// (Controller.mu RLock → Machine.mu RLock → callback) and must NOT re-read any
// method that acquires Controller.mu or Machine.mu (both RWMutexes are
// non-reentrant) or perform network I/O. The callback executes the bounded,
// context-aware bootstrap SQLite transaction (BEGIN, write access, retention
// evaluation, install, conflict check, COMMIT): it is a single transaction on
// one pooled connection, not a "multi-row or blocking query" in the forbidden
// sense. The SQLite busy timeout (the bounded wait for a contended write lock)
// is a known limitation of this contract — it is best-effort contention
// resolution, not a linearizability guarantee — and is accepted as such.
//
// The predicate is the dedicated bootstrap decision (bootstrapAuthorized),
// NOT the broad ordinary-replication decision (authorized): a SYNCING replica
// that was authorized to fetch may install the fetched baseline, but this
// guard does not authorize public writes, gossip, or acting as a source
// (those use CommitUnderAuthorization / authorized, which require a ready
// state). Ordinary anti-entropy is a separate path: it uses its own admission
// decision (which requires a ready state) and its own insert path, and does
// not go through this guard or the InsertReplicatedMessagesBounded bootstrap
// transaction. When lifecycle protection is disabled, the callback is
// executed directly (disabled-mode passthrough, legacy behavior preserved).
//
// The callback returns a non-nil error when the commit is rejected. This
// method propagates that error. When the bootstrap authorization is not
// valid, the callback is NOT executed and a rejection error is returned.
//
// expected (Milestone 5B, Stage 2, Issue 4) is the RequestContext pinned from
// the consistent authorized snapshot at the start of the bootstrap page (the
// cluster and epoch the page's data was fetched under). Under the same RLocks
// held for the bootstrap-authorized predicate, this method validates expected
// against the CURRENT snapshot: a non-authorized snapshot (revocation, state
// change, stop, stale/invalid observation, binding change) rejects the commit,
// and a cluster/epoch mismatch between expected and the current snapshot's
// metadata rejects the commit (an old response fetched under epoch A is never
// installed under newly acquired epoch-B authorization). When lifecycle
// protection is enabled, the expected context MUST be complete (both ClusterID
// and EpochID non-empty): an empty or incomplete expected context is rejected
// fail-closed. The disabled-mode passthrough (above) is the only path where no
// pin is present, and it performs no expected-context validation.
func (c *Controller) CommitUnderBootstrapAuthorization(expected RequestContext, fn func() error) error {
	if !c.cfg.Enabled {
		return fn()
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.machine.mu.RLock()
	defer c.machine.mu.RUnlock()
	snap := c.authorizationSnapshotLocked()
	if !snap.bootstrapAuthorized() {
		return errors.New("lifecycle bootstrap commit rejected: " + snap.bootstrapAuthorizationReason())
	}
	// Validate the pinned expected context against the CURRENT snapshot's
	// metadata (Issue 4: no old response installed under newly acquired
	// authorization). When lifecycle is enabled, the expected context MUST
	// be complete (both ClusterID and EpochID non-empty): an empty or
	// incomplete expected context is rejected (fail-closed). The disabled
	// passthrough (above) handles the no-pin case.
	if expected.ClusterID == "" || expected.EpochID == "" {
		return errors.New("lifecycle bootstrap commit rejected: expected context is incomplete (both ClusterID and EpochID are required when lifecycle is enabled)")
	}
	meta := snap.Metadata
	if meta == nil {
		return errors.New("lifecycle bootstrap commit rejected: no authoritative metadata to validate the expected context against")
	}
	if expected.ClusterID != meta.ClusterID {
		return errors.New("lifecycle bootstrap commit rejected: epoch changed — expected cluster " + expected.ClusterID + " but current snapshot is " + meta.ClusterID)
	}
	if expected.EpochID != meta.EpochID {
		return errors.New("lifecycle bootstrap commit rejected: epoch changed — expected epoch " + expected.EpochID + " but current snapshot is " + meta.EpochID)
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
		SeedIdentity:   c.cfg.SeedIdentity,
	}
}

// Machine returns the underlying state machine (for test purposes).
func (c *Controller) Machine() *Machine {
	return c.machine
}

// SetAcceptedMetadataForTest is an exported test-only method that sets the
// controller's accepted metadata directly, bypassing the metadata worker.
// It is used by tests in other packages to set up a specific metadata state
// without waiting for the periodic worker. It also sets freshAt to now (the
// real metadata-evaluation path always sets freshAt when accepting metadata),
// so that freshness-based guards (the genesis-entry guard, the promotion
// freshness check) see a FRESH observation. Production code must NOT call
// this.
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

// setBindingCacheLocked warms the cached binding under the ALREADY-HELD c.mu
// (write or read) lock. It is a test-only helper (used by the deterministic
// revocation-race tests) to establish the cached binding within a single
// critical section that also holds the c.mu RLock, so that the subsequent
// promoteFromGenesis re-read observes the binding atomically. Production code
// must NOT call this; the cache is warmed only by the metadata-evaluation
// path (write lock) and by AuthorizeGenesis (write lock).
func (c *Controller) setBindingCacheLocked(b *DatabaseBinding) {
	c.bindingVal = b
}

// forceStateLocked forces the state machine to the given state under the
// ALREADY-HELD c.mu RLock. It is a test-only helper (used by the
// deterministic seed-restart revocation test) to model a seed already in
// GENESIS (a prior restart) within a single critical section that also holds
// the c.mu RLock, so the subsequent promoteFromGenesis re-read observes the
// state atomically. It bypasses the transition table (a test-only escape
// hatch; production code must NOT call this — the production paths to
// GENESIS are reevaluateState and AuthorizeGenesis).
func (c *Controller) forceStateLocked(to State, reason string) {
	c.machine.forceTransitionLocked(to, reason)
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
