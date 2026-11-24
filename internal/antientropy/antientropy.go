// Package antientropy implements periodic anti-entropy reconciliation for
// GossipGrid (Milestone 4C).
//
// Each replica periodically compares bounded message ID inventories with
// authenticated peers using a PULL-based protocol:
//
//  1. Obtain an eligible peer through discovery.
//  2. Establish authenticated mTLS communication.
//  3. Request a bounded, deterministic page of message IDs (keyset pagination).
//  4. Determine which IDs are missing locally.
//  5. Retrieve the missing records in bounded batches.
//  6. Validate and insert using the existing idempotent store path.
//  7. Advance the per-peer cursor only after successful processing.
//  8. Repeat until the per-cycle page budget is exhausted.
//  9. Resume from the saved cursor on subsequent cycles.
//
// Key properties:
//   - Bounded: page size, fetch batch size, max pages per peer per cycle,
//     worker count, and request timeout are all finite and configurable.
//   - Idempotent: the store's ON CONFLICT DO NOTHING insert makes repeated
//     reconciliation safe.
//   - No overwrites: a conflicting ID never replaces an existing local row.
//   - Cursor-based: per-peer in-memory cursors resume across cycles; when a
//     full sweep completes (has_more=false), the cursor resets to the
//     beginning so later-inserted lower-sorting IDs are eventually found.
//   - Independent: does not depend on the gossip queue or retry scheduler.
//   - No unbounded resources: workers are bounded, per-peer state is bounded
//     by configured membership, and no new goroutines are spawned per request.
package antientropy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"gossipgrid/internal/peertls"
	"gossipgrid/internal/store"
)

// Config holds the validated anti-entropy parameters for a single node.
type Config struct {
	// Interval is the period between synchronization cycles.
	Interval time.Duration
	// PageSize is the maximum number of IDs per inventory page.
	PageSize int
	// FetchBatchSize is the maximum number of messages fetched per batch.
	FetchBatchSize int
	// MaxPagesPerPeer is the maximum inventory pages processed per peer per cycle.
	MaxPagesPerPeer int
	// Workers is the number of concurrent synchronization workers.
	Workers int
	// RequestTimeout bounds each individual HTTP request to a peer.
	RequestTimeout time.Duration
}

// Validate checks the anti-entropy configuration for out-of-range values.
func (c Config) Validate() error {
	if c.Interval <= 0 {
		return fmt.Errorf("interval must be positive (got %v)", c.Interval)
	}
	if c.Interval < time.Second {
		return fmt.Errorf("interval must be at least 1s (got %v)", c.Interval)
	}
	if c.Interval > 10*time.Minute {
		return fmt.Errorf("interval must be at most 10m (got %v)", c.Interval)
	}
	if c.PageSize < 1 || c.PageSize > 1024 {
		return fmt.Errorf("page size must be between 1 and 1024 (got %d)", c.PageSize)
	}
	// The fetch batch size is bounded to 8 so that a batch of 8 records at
	// the per-record wire-size allowance (32768 = conservative headroom above
	// the worst-case JSON-encoded body — a 4096-byte stored body escaping to
	// 24576 bytes via 6-byte \uXXXX sequences — plus origin/ID/timestamp
	// overhead) serializes to 8 x 32768 = 262144 = 256 KiB, within the sync
	// client's 256 KiB response bound (9 records would exceed it). This is a
	// conservative sizing of the validated field bounds, not a proof the
	// final encoded envelope always fits: the server additionally marshals
	// the full response and rejects it explicitly (500) if the JSON envelope
	// overhead plus records exceeds the limit, and the client rejects
	// oversized individual records. The peer server enforces the same bound
	// server-side, so an authenticated client cannot request an excessive
	// response.
	if c.FetchBatchSize < 1 || c.FetchBatchSize > 8 {
		return fmt.Errorf("fetch batch size must be between 1 and 8 (got %d); larger batches may exceed the 256 KiB response bound", c.FetchBatchSize)
	}
	if c.FetchBatchSize > c.PageSize {
		return fmt.Errorf("fetch batch size (%d) must not exceed page size (%d)", c.FetchBatchSize, c.PageSize)
	}
	if c.MaxPagesPerPeer < 1 || c.MaxPagesPerPeer > 64 {
		return fmt.Errorf("max pages per peer must be between 1 and 64 (got %d)", c.MaxPagesPerPeer)
	}
	if c.Workers < 1 || c.Workers > 16 {
		return fmt.Errorf("workers must be between 1 and 16 (got %d)", c.Workers)
	}
	if c.RequestTimeout <= 0 {
		return fmt.Errorf("request timeout must be positive (got %v)", c.RequestTimeout)
	}
	return nil
}

// PeerTarget identifies an eligible peer for anti-entropy synchronization:
// the network address to dial and the expected certificate DNS identity.
type PeerTarget struct {
	DialAddr         string
	ExpectedIdentity string
	PodIdentity      string
}

// MembershipSnapshot is a consistent, point-in-time view of the cluster
// membership used for one synchronization cycle. It carries the eligible
// network targets, the expected replica identities from the configured
// StatefulSet, and a flag indicating whether the membership is authoritative
// (safe to base cursor-cleanup decisions on). All three are derived from a
// single discovery snapshot so they cannot disagree.
type MembershipSnapshot struct {
	// Peers are the eligible peer targets for network synchronization. The
	// local replica is excluded. For a stale DNS snapshot, this is empty
	// (no new sync is performed while the snapshot is stale).
	Peers []PeerTarget
	// ExpectedIdentities are the configured StatefulSet pod identities
	// (e.g. "gossipgrid-0", "gossipgrid-1", "gossipgrid-2"). They represent
	// the authoritative cluster membership and are independent of DNS
	// presence: a successful DNS lookup that temporarily omits a pod does
	// NOT remove that identity from this list. A cursor record should be
	// retained for any identity in this list (or for an in-flight peer).
	ExpectedIdentities []string
	// Authoritative is true when the membership is safe to base cursor-
	// cleanup decisions on. For a fresh or static snapshot it is true.
	// For a stale DNS snapshot it is false: the engine preserves all
	// cursor state in that case.
	Authoritative bool
}

// PeerSource supplies a consistent view of the current peer membership for
// one synchronization cycle. It is an interface so unit tests can inject
// deterministic peer lists.
type PeerSource interface {
	// Snapshot returns a consistent membership view: eligible peer targets
	// for network synchronization, expected replica identities from the
	// configured StatefulSet, and a flag indicating whether the membership
	// is authoritative. The local replica is excluded from Peers. A
	// successful DNS lookup that temporarily omits a pod does NOT remove
	// that pod from ExpectedIdentities: DNS is not proof that all expected
	// pods were returned. Implementations must not hold discovery locks
	// while computing this.
	Snapshot() MembershipSnapshot
}

// InventoryClient fetches a paginated inventory from a specific peer.
type InventoryClient interface {
	FetchInventoryPage(ctx context.Context, dialAddr, expectedIdentity, after string, limit int) (*peertls.InventoryPageResponse, error)
}

// MessageFetcher retrieves a bounded set of messages by ID from a specific peer.
type MessageFetcher interface {
	FetchMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string) ([]peertls.SyncMessageWire, error)
}

// LocalStore provides the local SQLite operations needed for reconciliation.
type LocalStore interface {
	FindMissingIDs(ctx context.Context, ids []string) ([]string, error)
	InsertReplicatedMessageReturningInserted(msg *store.Message) (bool, error)
}

// CommitGuard executes a short, nonblocking commit callback while holding the
// lifecycle controller's authorization read lock (c.mu RLock) and the lifecycle
// state machine's read lock (m.mu RLock). This establishes the shared
// synchronization boundary between lifecycle authorization and the in-memory
// cursor commit: a revocation that completes before the guard enters is
// observed and the callback is not executed (the returned error signals the
// rejection); a revocation that follows an already-linearized commit is a
// normal, permitted revocation.
//
// Lock-order contract: Controller.mu (RLock) → Machine.mu (RLock) →
// Engine.mu (Lock). The callback may take the engine's mutex but must NOT
// acquire the controller's or machine's mutex, perform network I/O, or
// execute SQLite queries. A nil CommitGuard (lifecycle protection disabled)
// executes the callback directly.
//
// The callback returns a non-nil error when the commit is rejected (revocation
// observed at commit time). The caller (commitCursor) propagates that error to
// syncPeer, which does NOT advance the cursor or increment CompletedPasses.
type CommitGuard interface {
	CommitUnderAuthorization(fn func() error) error
}

// syncStats holds concurrency-safe counters for observability.
//
// Counters distinguish two levels of progress:
//   - CompletedPasses: a full sweep of one peer's ENTIRE inventory completed
//     (the sweep reached has_more=false with no unresolved conflicts or
//     errors). A page-budget limit (MaxPagesPerPeer pages processed with
//     has_more still true) is NOT a completed pass: the budget is a
//     per-cycle throttle, not the end of the remote inventory, so the
//     cursor is preserved for the next cycle.
//   - InventoryPages: individual inventory pages fetched and processed
//     successfully (a page may be part of an incomplete pass if a later page
//     fails or the page budget is exhausted). This is a page-level counter
//     and may exceed CompletedPasses.
type syncStats struct {
	completedPasses atomic.Uint64
	failedPasses    atomic.Uint64
	inventoryPages  atomic.Uint64
	idsInspected    atomic.Uint64
	missingIDsFound atomic.Uint64
	messagesFetched atomic.Uint64
	newInserts      atomic.Uint64
	alreadyPresent  atomic.Uint64
	conflicts       atomic.Uint64
	activeSyncs     atomic.Int32
}

// StatsSnapshot is an immutable point-in-time view of the engine's counters.
//
// CompletedPasses counts full sweeps that reached the end of a peer's entire
// inventory (has_more=false) with no unresolved conflicts. It never counts a
// partial sweep: a run that stops at the per-cycle page budget (has_more still
// true) is not a completed pass, so a partial sweep cannot appear to
// establish convergence. InventoryPages counts individual inventory pages
// fetched and processed successfully, which may exceed CompletedPasses when
// sweeps are incomplete (a later page failed, or the page budget was
// exhausted). LastSuccess (and the per-peer LastSuccess) reflects a genuinely
// completed successful synchronization; page-level progress is tracked
// separately via the per-peer LastCursor, not conflated into LastSuccess.
type StatsSnapshot struct {
	CompletedPasses uint64
	FailedPasses    uint64
	InventoryPages  uint64
	IDsInspected    uint64
	MissingIDsFound uint64
	MessagesFetched uint64
	NewInserts      uint64
	AlreadyPresent  uint64
	Conflicts       uint64
	ActiveSyncs     int32
	LastSuccess     time.Time
	PerPeer         map[string]PeerProgress
}

// PeerProgress is the per-peer progress or last error.
type PeerProgress struct {
	LastCursor  string
	LastSuccess time.Time
	LastError   string
}

// peerCursor tracks the per-peer inventory cursor and progress state.
type peerCursor struct {
	cursor      string
	sweepDone   bool // true when a full sweep completed (reset to start)
	lastSuccess time.Time
	lastError   string
}

// Engine owns the periodic anti-entropy synchronization. It is safe for
// concurrent use.
//
// Lifecycle: created → started → stopped. Start creates an engine-owned
// context derived from the caller's context and launches the worker pool and
// the periodic scheduler. Stop cancels that context, waits for all workers to
// finish (bounded by the supplied context), and is idempotent.
type Engine struct {
	cfg        Config
	logger     *slog.Logger
	peers      PeerSource
	invClient  InventoryClient
	msgFetcher MessageFetcher
	localStore LocalStore
	now        func() time.Time

	// retentionTTL: when non-zero, fetchAndInsert rejects fetched messages
	// whose creation timestamp is strictly older than the current expiration
	// threshold (now - TTL), preventing anti-entropy from resurrecting
	// messages that have expired. A zero TTL (the default) disables the
	// check and preserves the legacy behavior.
	retentionTTL time.Duration

	// Bounded worker channel: one slot per eligible peer (bounded by workers).
	workCh chan PeerTarget

	mu          sync.Mutex
	cursors     map[string]*peerCursor
	activePeers map[string]bool

	startMu      sync.Mutex
	started      bool
	stopped      bool
	engineCtx    context.Context
	engineCancel context.CancelFunc
	doneCh       chan struct{}
	doneOnce     sync.Once

	doneWG sync.WaitGroup
	st     syncStats

	// admissionCheck is an optional lifecycle admission guard (Milestone 5A).
	// When set, syncPeer checks it before performing any network operations.
	// A nil check means lifecycle protection is disabled.
	admissionCheck func(ctx context.Context) error

	// commitGuard is the lifecycle commit guard (Milestone 5A). When set,
	// commitCursor executes the cursor mutation while holding the controller's
	// authorization read lock (c.mu RLock) and the lifecycle state machine's
	// read lock (m.mu RLock), closing the window between the admission
	// decision and the in-memory commit. A nil guard (lifecycle protection
	// disabled) executes the commit directly under e.mu.
	commitGuard CommitGuard

	// initialSyncTracker is the opt-in initial anti-entropy synchronization
	// barrier tracker (Milestone 7A enhancement). When set, syncPeer records a
	// verified full inventory pass to it immediately after the full-sweep
	// accounting (commitCursor nil + CompletedPasses increment). A nil tracker
	// (the default) disables the barrier; no call is made. The tracker is
	// set before Start (via SetInitialSyncTracker) and is not mutated after.
	initialSyncTracker *InitialSyncTracker
}

// New creates an Engine with the given configuration and dependencies.
func New(cfg Config, peers PeerSource, invClient InventoryClient, msgFetcher MessageFetcher, localStore LocalStore, logger *slog.Logger) *Engine {
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		cfg:         cfg,
		logger:      logger,
		peers:       peers,
		invClient:   invClient,
		msgFetcher:  msgFetcher,
		localStore:  localStore,
		now:         time.Now,
		workCh:      make(chan PeerTarget, cfg.Workers*2),
		cursors:     make(map[string]*peerCursor),
		activePeers: make(map[string]bool),
		doneCh:      make(chan struct{}),
	}
}

// WithClock returns a new Engine with an injectable clock (for testing).
func WithClock(cfg Config, peers PeerSource, invClient InventoryClient, msgFetcher MessageFetcher, localStore LocalStore, logger *slog.Logger, now func() time.Time) *Engine {
	e := New(cfg, peers, invClient, msgFetcher, localStore, logger)
	e.now = now
	return e
}

// SetPeerSource configures the peer source. Must be called before Start.
func (e *Engine) SetPeerSource(p PeerSource) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.peers = p
}

// SetInventoryClient configures the inventory client. Must be called before Start.
func (e *Engine) SetInventoryClient(c InventoryClient) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.invClient = c
}

// SetMessageFetcher configures the message fetcher. Must be called before Start.
func (e *Engine) SetMessageFetcher(f MessageFetcher) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.msgFetcher = f
}

// SetLocalStore configures the local store. Must be called before Start.
func (e *Engine) SetLocalStore(s LocalStore) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.localStore = s
}

// SetAdmissionCheck configures the lifecycle admission guard for outbound
// anti-entropy (Milestone 5A). When set, syncPeer checks the guard before
// performing any network operations. A nil check means lifecycle protection
// is disabled.
func (e *Engine) SetAdmissionCheck(check func(ctx context.Context) error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.admissionCheck = check
}

// SetCommitGuard configures the lifecycle commit guard (Milestone 5A). When
// set, commitCursor executes the cursor mutation while holding the controller's
// authorization read lock (c.mu RLock) and the lifecycle state machine's read
// lock (m.mu RLock), so a revocation that completes before the guard enters
// is observed and the cursor is not advanced. A nil guard (lifecycle
// protection disabled) executes the commit directly under e.mu. Must be called
// before Start.
func (e *Engine) SetCommitGuard(guard CommitGuard) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.commitGuard = guard
}

// SetInitialSyncTracker configures the opt-in initial anti-entropy
// synchronization barrier tracker (Milestone 7A enhancement). When set,
// syncPeer records a verified full inventory pass to the tracker immediately
// after the full-sweep accounting. A nil tracker (the default) disables the
// barrier; no call is made and the engine's behavior is unchanged. The
// tracker must be set before Start.
func (e *Engine) SetInitialSyncTracker(t *InitialSyncTracker) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.initialSyncTracker = t
}

// SetRetentionTTL configures the message retention TTL for the anti-entropy
// engine. When non-zero, fetchAndInsert rejects fetched messages whose
// creation timestamp is strictly older than the current expiration threshold
// (now - TTL), preventing anti-entropy from resurrecting expired messages.
// A zero TTL (the default) disables the check. Must be called before Start.
func (e *Engine) SetRetentionTTL(ttl time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.retentionTTL = ttl
}

// admissionOK returns nil when outbound anti-entropy is currently authorized,
// or an error when the lifecycle admission guard rejects the operation. A nil
// guard (lifecycle protection disabled) always permits. It is checked both at
// the entry to a synchronization pass and immediately before each network
// request (inventory fetch, message fetch) and before each local insert, so
// a pass admitted while authorized does not advance its cursor or insert
// records after a subsequent lockout. The check must be non-blocking (a fast
// in-process check).
func (e *Engine) admissionOK(ctx context.Context) error {
	e.mu.Lock()
	check := e.admissionCheck
	e.mu.Unlock()
	if check == nil {
		return nil
	}
	return check(ctx)
}

// commitCursor is the local linearization point for a successful page
// progression (Milestone 5A). When the lifecycle commit guard is set, it
// executes the in-memory cursor mutation while holding the controller's
// authorization read lock (c.mu RLock) and the lifecycle state machine's
// read lock (m.mu RLock), and the commit is rejected (the guard returns an
// error) when the controller's authorization is no longer valid.
//
// The commit guard (Controller.CommitUnderAuthorization) evaluates the
// complete authorization decision — ready state, not stopped, fresh valid
// metadata with Initialized=true, and a matching durable binding with no
// recovery lockout — from a single locked snapshot (authorizationSnapshotLocked,
// one c.mu RLock + one m.mu RLock, no database read) and only runs the
// callback when that decision is authorized. The RLocks are held until the
// callback's in-memory cursor mutation completes. The callback therefore
// performs ONLY the short, in-memory cursor mutation and does NOT re-read
// the admission check or any other method that acquires Controller.mu or
// Machine.mu (both RWMutexes are non-reentrant, so a callback that
// re-reads the authorization would deadlock a writer waiting for the lock).
//
// Lock-order contract: Controller.mu (RLock) → Machine.mu (RLock) →
// Engine.mu (Lock). The commit guard acquires c.mu RLock, then m.mu RLock,
// then the callback takes e.mu. A revocation (Stop or a state/metadata
// change) must acquire c.mu Lock or m.mu Lock; it is blocked while the
// commit guard holds the RLocks. A revocation that completes before the
// commit guard enters is observed (the stopped flag is set or the authority
// no longer authorized) and the callback is not executed. A revocation that
// follows an already-linearized commit is a normal, permitted revocation.
//
// All lifecycle state transitions (SetRecoveryRequired, SetReady, SetDegraded,
// SetDiscovering, SetSyncing, and the metadata-driven reevaluateState)
// acquire c.mu RLock before m.mu Lock, matching this lock order. This ensures
// that a readiness-revoking transition cannot linearize while a cursor commit
// holds the RLocks: the transition is queued behind the commit's RLocks and
// is observed as a revocation by the next authorization evaluation.
//
// When the commit guard is nil (lifecycle protection disabled), the commit
// proceeds directly under e.mu, preserving the existing M4 behavior.
//
// The caller (syncPeer) must hold the cursor pointer but NOT e.mu when
// calling this method. If the method returns a non-nil error, the caller
// must NOT advance the cursor and must NOT increment CompletedPasses or
// update LastSuccess.
func (e *Engine) commitCursor(cursor *peerCursor, nextCursor string, fullSweep bool) error {
	// Read the commit guard under e.mu (short critical section; no
	// controller lock is held here).
	e.mu.Lock()
	guard := e.commitGuard
	e.mu.Unlock()

	if guard != nil {
		// Execute the in-memory cursor mutation under the controller's
		// authorization read lock and the machine's read lock. The guard has
		// already evaluated the complete authorization decision (a single
		// locked snapshot: c.mu RLock + m.mu RLock) and rejected the commit
		// when the authority was no longer valid; the callback performs only
		// the cursor mutation. Lock order: Controller.mu RLock (held by
		// guard) → Machine.mu RLock (held by guard) → Engine.mu Lock (held
		// by the callback).
		return guard.CommitUnderAuthorization(func() error {
			e.mu.Lock()
			cursor.lastError = ""
			if fullSweep {
				cursor.cursor = ""
				cursor.sweepDone = true
				cursor.lastSuccess = e.now()
			} else {
				cursor.cursor = nextCursor
			}
			e.mu.Unlock()
			return nil
		})
	}

	// Lifecycle protection disabled: commit directly under e.mu.
	e.mu.Lock()
	defer e.mu.Unlock()
	cursor.lastError = ""
	if fullSweep {
		cursor.cursor = ""
		cursor.sweepDone = true
		cursor.lastSuccess = e.now()
	} else {
		cursor.cursor = nextCursor
	}
	return nil
}

// Start creates the engine-owned context and launches the periodic scheduler
// and worker pool. It must be called exactly once. A stopped engine cannot
// be restarted.
func (e *Engine) Start(ctx context.Context) error {
	e.startMu.Lock()
	defer e.startMu.Unlock()
	if e.started || e.stopped {
		return errors.New("anti-entropy engine already started or stopped")
	}
	e.engineCtx, e.engineCancel = context.WithCancel(ctx)
	e.started = true

	for i := 0; i < e.cfg.Workers; i++ {
		e.doneWG.Add(1)
		go e.worker()
	}
	e.doneWG.Add(1)
	//nolint:contextcheck // e.engineCtx is a derived context created above from ctx via context.WithCancel; it inherits cancellation from ctx and is additionally cancellable via Stop().
	go e.runScheduler(e.engineCtx)

	go func() {
		e.doneWG.Wait()
		e.doneOnce.Do(func() {
			close(e.doneCh)
		})
	}()
	return nil
}

// Stop cancels the engine-owned context (aborting in-flight requests and
// periodic work), waits for all workers to finish (bounded by the supplied
// context), and is idempotent and safe to call from multiple goroutines.
// Stop before Start returns promptly. A stopped engine cannot be restarted.
func (e *Engine) Stop(ctx context.Context) {
	e.startMu.Lock()
	if !e.started {
		e.stopped = true
		e.startMu.Unlock()
		return
	}
	var doneCh chan struct{}
	if !e.stopped {
		e.stopped = true
		e.engineCancel()
		doneCh = e.doneCh
	} else {
		doneCh = e.doneCh
	}
	e.startMu.Unlock()

	select {
	case <-doneCh:
	case <-ctx.Done():
		e.logger.Warn("anti-entropy shutdown timed out; workers may still be running")
	}
}

// runScheduler is the periodic synchronization loop. It fires a cycle every
// cfg.Interval, enqueuing eligible peers for worker processing.
//
//nolint:contextcheck // ctx is e.engineCtx, derived from Start's caller context.
func (e *Engine) runScheduler(ctx context.Context) {
	defer e.doneWG.Done()

	ticker := time.NewTicker(e.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.engineCtx.Done():
			return
		case <-ticker.C:
			e.runCycle(ctx)
		}
	}
}

// runCycle performs one synchronization cycle: it reads a single consistent
// discovery snapshot (peer targets, expected membership, validity) and enqueues
// eligible peers for worker processing. Before enqueuing, it reconciles the
// per-peer cursor map against that same snapshot to bound state. The single
// snapshot read ensures the peer list, expected identities, and the validity
// flag cannot disagree (e.g. a stale-to-fresh transition between two separate
// reads).
func (e *Engine) runCycle(ctx context.Context) {
	snap := e.peers.Snapshot()
	e.reconcileCursors(&snap)

	if len(snap.Peers) == 0 {
		return
	}

	for _, p := range snap.Peers {
		if ctx.Err() != nil {
			return
		}
		// Non-blocking enqueue: if all workers are busy, skip this peer
		// for this cycle (it will be retried next cycle).
		select {
		case e.workCh <- p:
		default:
			e.logger.Debug("anti-entropy worker pool full; skipping peer this cycle",
				slog.String("peer", p.PodIdentity),
			)
		}
	}
}

// reconcileCursors bounds the per-peer cursor map against the expected
// cluster membership. It removes cursor entries only for replicas that are
// confirmed removed (absent from the expected StatefulSet membership and not
// currently being synced), preventing unbounded growth when the cluster is
// scaled down.
//
// The decision depends on whether membership is authoritative:
//
//   - Non-authoritative membership (authoritative == false, e.g. a stale DNS
//     snapshot or a nil snapshot function): the membership reflects a
//     last-known-good state, so a peer absent from it may have been
//     temporarily undiscovered rather than removed. In this case the cursor
//     map is NOT reconciled at all, so the progress of every peer (including
//     a peer temporarily missing from the stale snapshot) is preserved.
//     State is still bounded: the last-known-good peer list is finite, so
//     the map never grows beyond it plus in-flight peers. A truly removed
//     peer that is also absent from the stale snapshot is retained until
//     membership becomes fresh again (at which point it is confirmed removed
//     and cleaned up).
//   - Authoritative membership (authoritative == true): a replica absent
//     from the expected membership (ExpectedIdentities) is confirmed removed
//     (the StatefulSet was scaled down), so its cursor state is removed.
//     A replica that is present in the expected membership but temporarily
//     absent from DNS (a successful lookup that did not return all records)
//     is NOT removed: its cursor progress is preserved so the next cycle
//     can resume from it when the pod becomes discoverable again.
//
// Cursor state for expected replicas is ALWAYS preserved, including for
// completed sweeps (empty cursor). An empty cursor is a valid completed-
// sweep state: the peer may have newly-inserted records that will be found
// on the next sweep, and the LastSuccess and LastError diagnostics are
// valuable for observability across cycles.
func (e *Engine) reconcileCursors(snap *MembershipSnapshot) {
	if !snap.Authoritative {
		// Non-authoritative membership: do not drop any cursor state.
		// Preserving progress through large inventories across temporary
		// DNS failures takes priority; the map is bounded by the finite
		// last-known-good peer list plus in-flight peers.
		return
	}

	// Build the set of expected replica identities. These represent the
	// authoritative cluster membership (configured StatefulSet replicas)
	// and are independent of DNS presence: a successful DNS lookup that
	// temporarily omits a pod does NOT remove it from this set.
	expected := make(map[string]bool, len(snap.ExpectedIdentities))
	for _, id := range snap.ExpectedIdentities {
		expected[id] = true
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	for key := range e.cursors {
		// A replica not in the expected membership and not currently being
		// synced is confirmed removed (the StatefulSet was scaled down).
		// Remove its cursor state.
		if !expected[key] && !e.activePeers[key] {
			delete(e.cursors, key)
		}
	}
}

// worker processes synchronization work items from the work channel.
func (e *Engine) worker() {
	defer e.doneWG.Done()
	for {
		select {
		case <-e.engineCtx.Done():
			return
		case target := <-e.workCh:
			e.syncPeer(e.engineCtx, target)
		}
	}
}

// syncPeer performs a bounded synchronization pass against one peer:
// up to MaxPagesPerPeer inventory pages, fetching and inserting missing
// records. The per-peer cursor is advanced only after a page is fully
// processed without an unresolved conflict.
//
// Conflict semantics:
//   - An unresolved conflict (ErrDuplicateIDConflict) fails the affected
//     page: the cursor is NOT advanced past it, and the pass is not
//     reported as completed. Successfully inserted unrelated records in
//     the same batch are preserved (no rollback).
//   - The failed page is retried on a future cycle (cursor unchanged).
//   - A completed pass increments CompletedPasses and updates LastSuccess
//     ONLY when the sweep reaches has_more=false (the end of the peer's
//     entire inventory) with no unresolved conflicts or errors.
//   - A partial pass — a page failed, or the per-cycle page budget
//     (MaxPagesPerPeer) was exhausted with has_more still true — increments
//     InventoryPages for the pages that were processed but NOT
//     CompletedPasses, and preserves the cursor for the next cycle. A
//     partial sweep never appears to establish convergence.
func (e *Engine) syncPeer(ctx context.Context, target PeerTarget) {
	e.st.activeSyncs.Add(1)
	defer e.st.activeSyncs.Add(-1)

	key := target.PodIdentity
	if key == "" {
		key = target.ExpectedIdentity
	}

	// Milestone 5A: check the lifecycle admission guard before performing
	// any network operations. An unready or unauthorized replica must not
	// retrieve or insert messages from peers. The same guard is re-checked
	// immediately before each inventory request, each message-fetch request,
	// and before each local insert, so a pass admitted while authorized does
	// not advance its cursor or insert records after a subsequent lockout.
	if err := e.admissionOK(ctx); err != nil {
		e.logger.Debug("anti-entropy outbound blocked by lifecycle admission",
			slog.String("peer", key),
			slog.Any("error", err),
		)
		return
	}

	// Check for concurrent sync on the same peer.
	e.mu.Lock()
	if e.activePeers[key] {
		e.mu.Unlock()
		return // another worker is already syncing this peer
	}
	e.activePeers[key] = true
	e.mu.Unlock()

	defer func() {
		e.mu.Lock()
		delete(e.activePeers, key)
		e.mu.Unlock()
	}()

	// Get or create the per-peer cursor.
	e.mu.Lock()
	cursor, ok := e.cursors[key]
	if !ok {
		cursor = &peerCursor{}
		e.cursors[key] = cursor
	}
	startCursor := cursor.cursor
	e.mu.Unlock()

	pagesProcessed := 0
	after := startCursor

	for pagesProcessed < e.cfg.MaxPagesPerPeer {
		if ctx.Err() != nil {
			e.recordPeerProgress(key, after, ctx.Err().Error())
			e.st.failedPasses.Add(1)
			return
		}

		// Milestone 5A: re-check the lifecycle admission guard before each
		// inventory request, not only at pass entry. A pass admitted while
		// authorized must not request inventory after a subsequent lockout.
		// A lockout here fails the pass without advancing the cursor (the
		// same behavior as an inventory fetch error), so the cursor is not
		// advanced and the pass is not reported as completed.
		if err := e.admissionOK(ctx); err != nil {
			e.recordPeerProgress(key, after, "lifecycle lockout: "+err.Error())
			e.st.failedPasses.Add(1)
			e.logger.Debug("anti-entropy inventory blocked by lifecycle admission",
				slog.String("peer", key),
				slog.Any("error", err),
			)
			return // cursor not advanced; pass not completed
		}

		// Bound each request with the configured timeout.
		reqCtx, cancel := context.WithTimeout(ctx, e.cfg.RequestTimeout)

		page, err := e.invClient.FetchInventoryPage(reqCtx, target.DialAddr, target.ExpectedIdentity, after, e.cfg.PageSize)
		cancel()
		if err != nil {
			e.recordPeerProgress(key, after, err.Error())
			e.st.failedPasses.Add(1)
			e.logger.Warn("anti-entropy inventory fetch failed",
				slog.String("peer", key),
				slog.Any("error", err),
			)
			// Milestone 5A: an epoch mismatch is NOT a transient failure
			// to be retried. The peer belongs to a different epoch and
			// will not supply inventory until the local epoch is corrected.
			// The cursor is not advanced (the pass is not completed), but
			// the error is recorded so the operator can diagnose the
			// mismatch.
			return // cursor not advanced
		}

		e.st.inventoryPages.Add(1)
		e.st.idsInspected.Add(uint64(len(page.IDs)))

		// Determine which IDs are missing locally.
		if len(page.IDs) > 0 {
			reqCtx2, cancel2 := context.WithTimeout(ctx, e.cfg.RequestTimeout)
			missing, err := e.localStore.FindMissingIDs(reqCtx2, page.IDs)
			cancel2()
			if err != nil {
				e.recordPeerProgress(key, after, err.Error())
				e.st.failedPasses.Add(1)
				e.logger.Warn("anti-entropy find-missing-ids failed",
					slog.String("peer", key),
					slog.Any("error", err),
				)
				return // cursor not advanced; retry next cycle
			}

			if len(missing) > 0 {
				e.st.missingIDsFound.Add(uint64(len(missing)))
				// Fetch missing records in bounded batches.
				err := e.fetchAndInsert(ctx, target, missing)
				if err != nil {
					// A conflict or insert error: the page is failed.
					// The cursor is NOT advanced; the pass is NOT
					// reported as completed (no CompletedPasses increment).
					e.st.failedPasses.Add(1)
					e.recordPeerProgress(key, after, err.Error())
					e.logger.Warn("anti-entropy fetch/insert failed (cursor not advanced)",
						slog.String("peer", key),
						slog.Any("error", err),
					)
					return // cursor NOT advanced; retry this page next cycle
				}
			} else {
				// All IDs already present locally.
				e.st.alreadyPresent.Add(uint64(len(page.IDs)))
			}
		}

		// Page processed successfully (no unresolved conflict). Before
		// committing any pagination progress (cursor advancement or
		// completed-sweep reset), call commitCursor: the local
		// linearization point that atomically re-checks the lifecycle
		// admission guard and performs the in-memory cursor mutation while
		// holding e.mu. A revocation that occurs after page processing
		// but before the admission read inside commitCursor is observed
		// and the cursor is NOT advanced. Already-committed immutable
		// records are preserved (no rollback).
		if err := e.commitCursor(cursor, page.NextCursor, !page.HasMore); err != nil {
			e.recordPeerProgress(key, after, "lifecycle lockout at cursor commit: "+err.Error())
			e.st.failedPasses.Add(1)
			e.logger.Debug("anti-entropy cursor commit blocked by lifecycle admission",
				slog.String("peer", key),
				slog.Any("error", err),
			)
			return // cursor not advanced; pass not completed
		}

		pagesProcessed++

		// No more pages: full sweep complete. Reset cursor for future cycles
		// so a later-inserted lower-sorting ID is eventually found. This is
		// the only path that counts a CompletedPass and updates LastSuccess.
		//
		// Accounting ordering: commitCursor returned nil (the cursor mutation
		// linearized under the authorization boundary), so the commit is
		// already authorized. The completedPasses increment and the
		// recordPeerProgress call below are diagnostics published immediately
		// after an already-authorized commit. A revocation that completes
		// after the commit linearized is a normal, permitted revocation and
		// does not retroactively invalidate the completed-sweep accounting.
		if !page.HasMore {
			e.st.completedPasses.Add(1)
			e.recordPeerProgress(key, "", "")
			// Milestone 7A enhancement: publish the verified full pass to
			// the opt-in initial-sync barrier tracker (when enabled). This
			// is the single point where a completed sweep (has_more=false,
			// no unresolved conflict or error) is recorded for the barrier.
			// The tracker only records peers in the configured expected
			// membership and is one-shot (a transient outage after the
			// barrier is satisfied does not re-open it).
			e.mu.Lock()
			tracker := e.initialSyncTracker
			e.mu.Unlock()
			if tracker != nil {
				if tracker.RecordComplete(key) {
					e.logger.Info("initial anti-entropy synchronization barrier complete",
						slog.String("peer", key),
					)
				}
			}
			return
		}

		// Advance the cursor to the last ID in this page (page-level
		// progress; LastSuccess is not updated — a page budget or a later
		// failed page means the sweep is not complete).
		after = page.NextCursor
		e.recordPeerProgress(key, page.NextCursor, "")
	}

	// Max pages reached: the cursor is saved; the next cycle resumes from it.
	// This is NOT a completed pass (the remote inventory may continue beyond
	// the page budget), so CompletedPasses is NOT incremented and LastSuccess
	// is NOT updated. InventoryPages already reflects the pages processed.
}

// fetchAndInsert retrieves missing messages from the peer in bounded batches
// and inserts them using the existing idempotent store path.
//
// Conflict semantics:
//   - An ErrDuplicateIDConflict is recorded as a conflict and causes the
//     affected page to be reported as failed (the cursor is not advanced).
//     Successfully inserted records in the same batch are NOT rolled back;
//     they remain in the local store. The failed page is retried on a
//     future cycle.
//   - Other errors (network, store I/O) also fail the page.
//   - The method returns an error when any conflict or insert error occurs,
//     so the caller (syncPeer) does not advance the cursor.
func (e *Engine) fetchAndInsert(ctx context.Context, target PeerTarget, missing []string) error {
	for i := 0; i < len(missing); i += e.cfg.FetchBatchSize {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Milestone 5A: re-check the lifecycle admission guard before each
		// message-fetch request, not only at pass entry. A pass admitted
		// while authorized must not fetch records after a subsequent lockout.
		// Returning an error here fails the page (the caller does not advance
		// the cursor) without inserting any records.
		if err := e.admissionOK(ctx); err != nil {
			return fmt.Errorf("lifecycle lockout before message fetch from peer %s: %w", target.PodIdentity, err)
		}

		end := i + e.cfg.FetchBatchSize
		if end > len(missing) {
			end = len(missing)
		}
		batch := missing[i:end]

		reqCtx, cancel := context.WithTimeout(ctx, e.cfg.RequestTimeout)
		msgs, err := e.msgFetcher.FetchMessages(reqCtx, target.DialAddr, target.ExpectedIdentity, batch)
		cancel()
		if err != nil {
			return fmt.Errorf("fetching batch from peer %s: %w", target.PodIdentity, err)
		}

		e.st.messagesFetched.Add(uint64(len(msgs)))

		// Insert each message using the idempotent store path.
		for _, m := range msgs {
			msg := &store.Message{
				ID:     m.ID,
				Body:   m.Body,
				Origin: m.Origin,
			}
			// Parse the timestamp.
			ts, err := parseTimestamp(m.CreatedAt)
			if err != nil {
				// Malformed timestamp from a valid peer: reject, do not insert.
				e.logger.Warn("anti-entropy: rejecting message with malformed timestamp",
					slog.String("peer", target.PodIdentity),
					slog.String("id", m.ID),
					slog.Any("error", err),
				)
				return fmt.Errorf("message %q has malformed timestamp: %v", m.ID, err)
			}
			msg.CreatedAt = ts

			// Milestone 5A: re-check the lifecycle admission guard before each
			// local insert, so a lockout that occurs mid-batch does not insert
			// additional records under an invalidated authorization. Returning
			// an error fails the page (the caller does not advance the cursor);
			// already-inserted records in this batch are preserved (no rollback),
			// matching the existing conflict/insert-error behavior.
			if err := e.admissionOK(ctx); err != nil {
				return fmt.Errorf("lifecycle lockout before inserting message %q from peer %s: %w", m.ID, target.PodIdentity, err)
			}

			// Retention expiration check: when retention is enabled
			// (retentionTTL > 0), reject messages whose creation timestamp is
			// strictly older than the current expiration threshold (now - TTL).
			// This prevents anti-entropy from resurrecting a message that the
			// local replica has already expired (e.g. after a cleanup sweep).
			// The threshold is computed from the current clock (e.now, not a
			// value captured at engine start) so a long-running replica
			// reflects time progression. A zero retentionTTL (the default)
			// disables this check.
			//
			// Expiration between inventory, fetch, and insert is handled
			// gracefully: the message is skipped (no insert, no error, no
			// conflict), so the page is NOT failed and the cursor IS advanced.
			// This prevents endless anti-entropy retries for a message that
			// has legitimately expired. The next cycle's inventory will not
			// advertise the expired ID (the server-side filter excludes it),
			// so the skip is a one-time no-op.
			if e.retentionTTL > 0 && msg.CreatedAt.Before(store.ExpirationThreshold(e.retentionTTL, e.now())) {
				e.logger.Debug("anti-entropy: skipping expired message",
					slog.String("peer", target.PodIdentity),
					slog.String("id", m.ID),
				)
				e.st.alreadyPresent.Add(1)
				continue
			}

			inserted, err := e.localStore.InsertReplicatedMessageReturningInserted(msg)
			if err != nil {
				if errors.Is(err, store.ErrDuplicateIDConflict) {
					e.st.conflicts.Add(1)
					e.logger.Warn("anti-entropy: immutable content conflict (local row preserved; page will be retried)",
						slog.String("peer", target.PodIdentity),
						slog.String("id", m.ID),
					)
					// Fail the page: do not advance the cursor.
					// Already-inserted records in this batch are preserved.
					return fmt.Errorf("unresolved conflict on message %q: local row preserved, page failed", m.ID)
				}
				return fmt.Errorf("inserting message %q: %w", m.ID, err)
			}
			if inserted {
				e.st.newInserts.Add(1)
			} else {
				e.st.alreadyPresent.Add(1)
			}
		}
	}
	return nil
}

// recordPeerProgress updates the per-peer progress metadata. It is called
// from syncPeer after each page or on error to track the last cursor
// position and last error for observability. It does NOT set lastSuccess:
// that is set only when a full sweep completes (has_more=false) in syncPeer,
// so a page-level success (or a partial sweep) never appears to establish
// convergence.
func (e *Engine) recordPeerProgress(key, cursor, errMsg string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	pc, ok := e.cursors[key]
	if !ok {
		pc = &peerCursor{}
		e.cursors[key] = pc
	}
	if cursor != "" {
		pc.cursor = cursor
	}
	if errMsg != "" {
		pc.lastError = errMsg
	} else {
		pc.lastError = ""
	}
}

// Stats returns a concurrency-safe diagnostic snapshot. The PerPeer map is a
// fresh copy: callers can hold and mutate it without affecting the engine.
// LastSuccess is the most recent completed successful synchronization across
// all peers (the max of the per-peer LastSuccess values); it is zero until a
// full sweep completes, so a partial sweep or a failed page never sets it.
func (e *Engine) Stats() StatsSnapshot {
	var lastSuccess time.Time
	perPeer := make(map[string]PeerProgress)
	e.mu.Lock()
	for key, c := range e.cursors {
		perPeer[key] = PeerProgress{
			LastCursor:  c.cursor,
			LastSuccess: c.lastSuccess,
			LastError:   c.lastError,
		}
		if c.lastSuccess.After(lastSuccess) {
			lastSuccess = c.lastSuccess
		}
	}
	e.mu.Unlock()

	return StatsSnapshot{
		CompletedPasses: e.st.completedPasses.Load(),
		FailedPasses:    e.st.failedPasses.Load(),
		InventoryPages:  e.st.inventoryPages.Load(),
		IDsInspected:    e.st.idsInspected.Load(),
		MissingIDsFound: e.st.missingIDsFound.Load(),
		MessagesFetched: e.st.messagesFetched.Load(),
		NewInserts:      e.st.newInserts.Load(),
		AlreadyPresent:  e.st.alreadyPresent.Load(),
		Conflicts:       e.st.conflicts.Load(),
		ActiveSyncs:     e.st.activeSyncs.Load(),
		LastSuccess:     lastSuccess,
		PerPeer:         perPeer,
	}
}

// parseTimestamp parses an RFC3339 timestamp (with or without fractional seconds).
func parseTimestamp(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, errors.New("empty timestamp")
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}
