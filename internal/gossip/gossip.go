// Package gossip implements epidemic message propagation between authenticated
// GossipGrid replicas (Milestone 4B).
//
// When a message is committed locally (via POST /api/messages) or received from
// a peer (via POST /internal/messages), the Engine asynchronously fans the
// message out to a bounded number of authenticated peer targets using the
// existing peertls.Client.SendMessage transport.
//
// # Propagation work model
//
// Each enqueued message is a propagation "unit" with its own work state:
//   - a finite number of fanout rounds (cfg.Rounds), each selecting up to
//     cfg.Fanout distinct peers;
//   - a per-peer delivery budget (cfg.MaxAttempts = total attempts, including
//     the initial attempt). A peer that exhausts its budget stops receiving
//     attempts, and a failing peer never prevents attempts to other eligible
//     peers.
//
// Retries target the specific peer that failed transiently; they never reselect
// unrelated peers. Permanent errors (HTTP 409/400/413, invalid acknowledgment)
// are not retried. Ambiguous timeouts may be retried within the configured
// limit because the receiver's insert is idempotent.
//
// Key properties:
//   - Best-effort: a committed local message is never rolled back due to
//     gossip failure. Queue overflow or a dropped delayed retry drops the
//     propagation event, not the message.
//   - Bounded: queue capacity, worker count, delayed-retry capacity, and
//     per-message/per-peer budgets are all finite and configurable.
//   - Non-blocking: workers never sleep for backoff; delayed retries are held
//     in a bounded channel and their backoff is applied by short-lived timer
//     goroutines that deliver onto that channel.
//   - Idempotent: the receiver's ON CONFLICT DO NOTHING insert makes a
//     bounded retry safe (a timeout after a remote insert is not a failure).
//   - No anti-entropy: this milestone does not perform snapshot
//     synchronization or periodic inventory comparison (Milestone 4C).
package gossip

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"gossipgrid/internal/peertls"
)

// Config holds the validated gossip parameters for a single node.
type Config struct {
	// Fanout is the number of peers contacted per propagation round.
	Fanout int
	// Rounds is the number of fanout rounds per message.
	Rounds int
	// Workers is the number of concurrent delivery workers.
	Workers int
	// QueueCapacity is the maximum number of pending propagation events
	// (the work queue capacity; the delayed-retry channel has the same
	// capacity).
	QueueCapacity int
	// MaxAttempts is the TOTAL number of delivery attempts per peer,
	// including the initial attempt.
	MaxAttempts int
	// RetryBase is the base duration for exponential backoff.
	RetryBase time.Duration
	// RetryMax is the maximum backoff duration (cap).
	RetryMax time.Duration
}

// Validate checks the gossip configuration for out-of-range values.
func (c Config) Validate() error {
	if c.Fanout < 1 || c.Fanout > 32 {
		return fmt.Errorf("fanout must be between 1 and 32 (got %d)", c.Fanout)
	}
	if c.Rounds < 1 || c.Rounds > 32 {
		return fmt.Errorf("rounds must be between 1 and 32 (got %d)", c.Rounds)
	}
	if c.Workers < 1 || c.Workers > 64 {
		return fmt.Errorf("workers must be between 1 and 64 (got %d)", c.Workers)
	}
	if c.QueueCapacity < 1 || c.QueueCapacity > 1024 {
		return fmt.Errorf("queue capacity must be between 1 and 1024 (got %d)", c.QueueCapacity)
	}
	if c.MaxAttempts < 1 || c.MaxAttempts > 10 {
		return fmt.Errorf("max attempts must be between 1 and 10 (got %d)", c.MaxAttempts)
	}
	if c.RetryBase <= 0 {
		return fmt.Errorf("retry base must be positive (got %v)", c.RetryBase)
	}
	if c.RetryMax < c.RetryBase {
		return fmt.Errorf("retry max (%v) must be >= retry base (%v)", c.RetryMax, c.RetryBase)
	}
	return nil
}

// PeerTarget is a resolved delivery target: the network address to dial and
// the expected certificate DNS identity to verify.
type PeerTarget struct {
	DialAddr         string
	ExpectedIdentity string
	PodIdentity      string // e.g. "gossipgrid-1" (used for exclusion tracking)
}

// Key returns the canonical identity used for per-peer bookkeeping (the pod
// identity, or the expected certificate identity when no pod identity is set).
func (t PeerTarget) Key() string {
	if t.PodIdentity != "" {
		return t.PodIdentity
	}
	return t.ExpectedIdentity
}

// PeerSelector resolves eligible peer targets from a discovery snapshot.
// It is an interface so unit tests can inject deterministic selection.
type PeerSelector interface {
	// Select returns up to fanout peer targets from the given snapshot,
	// excluding any peers in the exclude set (self, the authenticated
	// sender, and peers already contacted). The returned slice has at most
	// fanout entries with no duplicates.
	Select(snapshot PeerSnapshot, fanout int, exclude map[string]bool) []PeerTarget
}

// PeerSnapshot is a read-only view of peer membership used by the
// PeerSelector. It mirrors the fields of discovery.Snapshot that the
// selector needs without importing the discovery package (avoiding a
// circular dependency).
type PeerSnapshot struct {
	Peers     []PeerInfo
	LocalNode *PeerInfo
	Stale     bool
	Mode      string
}

// PeerInfo is a single peer entry in the snapshot.
type PeerInfo struct {
	Hostname    string
	PodIdentity string
	Address     string
	LastSeen    time.Time
}

// Delivery abstracts the authenticated peer message send operation.
// It is an interface so unit tests can inject a fake transport and
// observe delivery calls without real mTLS.
type Delivery interface {
	// Send delivers a message to a specific peer over mTLS. It returns
	// an error on failure (transient or permanent). The caller
	// classifies the error to decide whether to retry.
	Send(ctx context.Context, target PeerTarget, msg peertls.MessageWire) (*peertls.MessageWire, error)
}

// RealDelivery implements Delivery using the peertls.Client.
type RealDelivery struct {
	client *peertls.Client
}

// NewRealDelivery creates a Delivery backed by the given peer client.
func NewRealDelivery(client *peertls.Client) *RealDelivery {
	return &RealDelivery{client: client}
}

// Send implements Delivery.
func (d *RealDelivery) Send(ctx context.Context, target PeerTarget, msg peertls.MessageWire) (*peertls.MessageWire, error) {
	return d.client.SendMessage(ctx, target.DialAddr, target.ExpectedIdentity, msg)
}

// MessageEvent is a propagation event queued by the HTTP handlers after a
// successful local commit. It carries the wire-form message and the identity
// of the sender (for exclusion: we avoid forwarding back to the peer we just
// received the message from).
type MessageEvent struct {
	Message     peertls.MessageWire
	SenderPodID string
}

// workItem is a unit of propagation work: a fanout round (target == nil) or a
// targeted retry of a specific peer (target != nil), both for the same
// message. The message itself is carried by the message's work state
// (msgState), which is looked up by msgID; workItem only records the
// per-unit details (retry target, attempt index, and the initial
// exclusion set to seed from).
type workItem struct {
	msgID    string
	target   *PeerTarget
	attempt  int
	excluded map[string]bool
	// fireAt is set when the item is a delayed retry waiting in the scheduler
	// heap (the time it should be delivered). It is the zero time for items
	// on the main queue or already fired.
	fireAt time.Time
}

// fireBefore reports whether the item's fire time is strictly before t.
func (w workItem) fireBefore(t time.Time) bool {
	return !w.fireAt.IsZero() && w.fireAt.Before(t)
}

// msgState is the per-message work state: the immutable message being
// propagated, the set of peers already successfully contacted (grows only on
// a successful delivery), the set of peers permanently rejected (never
// contacted again), the set of peers with a delayed retry outstanding (excluded
// from further fanout rounds until the retry is resolved or cancelled), the
// per-peer in-flight delivery count (prevents overlapping sends to the same
// peer/message when fanout rounds and delayed retries execute concurrently),
// the per-peer delivery attempt counts (bounded by MaxAttempts), and an
// in-flight count of work units (queued rounds or delayed retries) that must
// complete before the pending-ID deduplication entry is released.
type msgState struct {
	msg peertls.MessageWire
	// mu serializes access to excluded, permanent, failedPeers, pendingRetry,
	// inFlightPeer and attempts, which multiple workers may read or write
	// concurrently for the same message. The delivery-eligibility check
	// (tryReserveAttempt) and every mutation of peer state are performed
	// atomically under this lock.
	mu           sync.Mutex
	excluded     map[string]bool
	permanent    map[string]bool
	failedPeers  map[string]bool // peers that failed transiently in a fanout round (excluded from later rounds while a retry is pending)
	pendingRetry map[string]bool // peers with a delayed retry outstanding (eligibility gate: blocks fanout and overlapping sends)
	inFlightPeer map[string]int  // in-flight deliveries per peer (overlapping-send guard)
	attempts     map[string]int
	inFlight     int
}

// id returns the message's ID (for bookkeeping and logging).
func (s *msgState) id() string { return s.msg.ID }

// wire returns the immutable message carried by this work state.
func (s *msgState) wire() peertls.MessageWire { return s.msg }

// excludedCopy returns a copy of the current exclusion set (peers already
// successfully contacted). It is used to seed the exclusion set of a scheduled
// retry so the retry honours exclusions recorded after it was enqueued.
func (s *msgState) excludedCopy() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]bool, len(s.excluded))
	for k, v := range s.excluded {
		if v {
			out[k] = true
		}
	}
	return out
}

// stats holds concurrency-safe counters for observability.
type stats struct {
	messagesQueued   atomic.Uint64
	messagesDropped  atomic.Uint64
	deliveryAttempts atomic.Uint64
	successfulAcks   atomic.Uint64
	deliveryFailures atomic.Uint64
	retryAttempts    atomic.Uint64
	retriesScheduled atomic.Uint64
	retriesDropped   atomic.Uint64
}

// StatsSnapshot is an immutable point-in-time view of the engine's counters.
type StatsSnapshot struct {
	MessagesQueued   uint64
	MessagesDropped  uint64
	DeliveryAttempts uint64
	SuccessfulAcks   uint64
	DeliveryFailures uint64
	RetryAttempts    uint64
	RetriesScheduled uint64
	RetriesDropped   uint64
	QueueDepth       int
	PendingWork      int
	DelayedRetries   int
}

// safeRand wraps a *rand.Rand with a mutex because math/rand.Rand is not
// safe for concurrent use.
type safeRand struct {
	mu  sync.Mutex
	rnd *rand.Rand
}

func (s *safeRand) float64() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rnd.Float64()
}

func (s *safeRand) shuffle(n int, swap func(i, j int)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rnd.Shuffle(n, swap)
}

// Engine owns the gossip queue, workers, and bounded delayed-retry scheduler.
// It is safe for concurrent use.
//
// In-flight bookkeeping: each enqueued message reserves a pending entry with
// inFlight=1 BEFORE any work item is published to a consumer (the queue or the
// retry scheduler). Each additionally published work unit (a queued round or a
// scheduled retry) increments inFlight by one and is released exactly once
// (on completion, cancellation, or a rejection of the scheduling). The pending
// entry is removed only when inFlight reaches 0. A unit that is rejected
// before it is published (queue full, or scheduler at capacity) releases its
// reservation without recreating a pending entry, so a finished work item can
// never be resurrected by a later rejected unit.
//
// Lifecycle: created → started → stopped. Start creates an engine-owned
// context derived from the caller's context and launches the worker pool, the
// single retry scheduler, and a single completion goroutine. Stop cancels that
// context exactly once, performs a preliminary release of abandoned work
// (under startMu, which also blocks Enqueue), and waits for all workers and the
// scheduler to finish, bounded by the supplied context. The completion
// goroutine performs the FINAL abandoned-work drain after every worker and the
// scheduler have exited — and before closing the completion channel — so no
// scheduler or worker can publish additional work after the final drain, and
// every concurrent Stop caller (including the first) observes the same
// completion only once the drain has finished.
type Engine struct {
	cfg      Config
	logger   *slog.Logger
	selector PeerSelector
	delivery Delivery
	rng      *safeRand
	now      func() time.Time

	queue   chan workItem
	retryCh chan workItem
	delayed *retryHeap // bounded collection of delayed retries; one timer
	// retrySignal is a 1-element channel signalled by delayedPush when a new
	// delayed retry is added, so the single scheduler goroutine (which may be
	// blocked waiting for the next fire time or for the context) re-evaluates
	// promptly. It is only signalled while the engine is running.
	retrySignal chan struct{}
	st          stats

	mu             sync.Mutex
	snapshotSource func() PeerSnapshot

	startMu      sync.Mutex
	started      bool
	stopped      bool
	engineCtx    context.Context
	engineCancel context.CancelFunc
	doneCh       chan struct{} // closed when all workers + scheduler exit
	doneOnce     sync.Once

	pendingMu sync.Mutex
	pending   map[string]*msgState

	// doneWG tracks the workers and the retry scheduler (not the completion
	// goroutine). When it reaches zero, all propagation work has finished and
	// doneCh is closed (once). Stop waits on doneCh, so every concurrent Stop
	// caller observes the same completion rather than each spawning its own
	// WaitGroup-waiting goroutine.
	doneWG sync.WaitGroup
}

// New creates an Engine with the given configuration, peer selector,
// delivery transport, and logger.
func New(cfg Config, selector PeerSelector, delivery Delivery, logger *slog.Logger) *Engine {
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		cfg:      cfg,
		logger:   logger,
		selector: selector,
		delivery: delivery,
		rng:      &safeRand{rnd: rand.New(rand.NewSource(time.Now().UnixNano()))}, // #nosec G404: math/rand for peer selection, not security-sensitive
		now:      time.Now,
		queue:    make(chan workItem, cfg.QueueCapacity),
		retryCh:  make(chan workItem, cfg.QueueCapacity),
		delayed:  newRetryHeap(),
		pending:  make(map[string]*msgState),
		doneCh:   make(chan struct{}),
		// retrySignal is buffered by 1 so a signal is never dropped even if the
		// scheduler is momentarily not selecting on it.
		retrySignal: make(chan struct{}, 1),
	}
}

// WithRng returns a new Engine with a deterministic RNG (for testing).
func WithRng(cfg Config, selector PeerSelector, delivery Delivery, logger *slog.Logger, rng *rand.Rand) *Engine {
	e := New(cfg, selector, delivery, logger)
	e.rng = &safeRand{rnd: rng}
	return e
}

// WithClock returns a new Engine with an injectable clock (for testing).
func WithClock(cfg Config, selector PeerSelector, delivery Delivery, logger *slog.Logger, now func() time.Time) *Engine {
	e := New(cfg, selector, delivery, logger)
	e.now = now
	return e
}

// SetDelivery configures the delivery transport. Must be called before Start.
func (e *Engine) SetDelivery(d Delivery) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.delivery = d
}

// Start creates the engine-owned context (derived from the supplied ctx) and
// launches the worker pool. It must be called exactly once. A stopped engine
// cannot be restarted.
func (e *Engine) Start(ctx context.Context) error {
	e.startMu.Lock()
	defer e.startMu.Unlock()
	if e.started || e.stopped {
		return errors.New("gossip engine already started or stopped")
	}
	e.engineCtx, e.engineCancel = context.WithCancel(ctx)
	e.started = true
	for i := 0; i < e.cfg.Workers; i++ {
		e.doneWG.Add(1)
		go e.worker()
	}
	// One scheduler goroutine owns the bounded delayed-retry collection and a
	// single timer; workers never sleep for backoff.
	e.doneWG.Add(1)
	go e.runRetryScheduler()
	// A single goroutine (not one per Stop caller) performs the FINAL
	// abandoned-work drain and then closes doneCh when the worker pool and
	// scheduler have all exited. It is NOT itself added to e.doneWG (that
	// would deadlock: it would wait on a count that includes itself).
	//
	// The drain is deferred here — until every worker and the scheduler have
	// exited — so no scheduler or worker can publish additional work (onto
	// retryCh or the queue) after the final drain: by the time this runs the
	// context is cancelled and every delivery and every fireDueRetries
	// non-blocking push observe the cancellation and release their
	// reservation instead of publishing. Stop performs a preliminary drain
	// under startMu (which also blocks Enqueue) and, for a normal shutdown,
	// relies on this final drain for the guarantee that, once doneCh is
	// closed, QueueDepth, PendingWork, and DelayedRetries are all zero.
	go func() {
		e.doneWG.Wait()
		e.doneOnce.Do(func() {
			e.releaseAbandonedWork()
			close(e.doneCh)
		})
	}()
	return nil
}

// Stop cancels the engine-owned context (aborting in-flight deliveries and
// delayed retries), performs a preliminary release of abandoned queued and
// scheduled work, and waits for all workers and the scheduler to finish,
// bounded by the supplied context. The final abandoned-work drain is performed
// by the engine's single completion goroutine AFTER the worker pool and
// scheduler have exited, and before doneCh is closed, so once Stop has observed
// doneCh no scheduler or worker can publish additional work and the queue,
// delayed-retry, and pending-work depths are all zero.
//
// It is idempotent and safe to call from multiple goroutines. Engine
// cancellation is initiated exactly once by the first Stop caller; every Stop
// caller (including the first) waits on the same completion channel (doneCh)
// so no caller reports completed shutdown while workers are still executing
// cleanup. Stop before Start establishes the terminal stopped state and
// returns promptly. A stopped engine cannot be restarted.
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
		// Drain while holding startMu so that concurrent Enqueue calls are
		// blocked at admitEnqueue (which also takes startMu) and cannot
		// publish a new work item after this drain. This makes lifecycle
		// admission and queue publication mutually exclusive with Stop.
		e.releaseAbandonedWork()
		doneCh = e.doneCh
	} else {
		doneCh = e.doneCh
	}
	e.startMu.Unlock()

	select {
	case <-doneCh:
	case <-ctx.Done():
		e.logger.Warn("gossip engine shutdown timed out; workers may still be running")
	}
}

// releaseAbandonedWork releases the in-flight bookkeeping for every work unit
// that will never be consumed or scheduled after the engine has been stopped:
//
//   - queued work items (already accepted, but never consumed because a worker
//     exited before reaching them, or dropped because the queue was full);
//   - delayed retries still in the bounded scheduler.
//
// It is safe to call multiple times: each unit is released exactly once, and
// a pending entry is removed only when its in-flight count reaches zero. It
// does NOT execute any deliveries; it only releases resources so that after a
// completed shutdown the engine's queue, delayed-retry, and pending-work depths
// all read zero. A delayed retry drained here is discarded: its peer's
// pending-retry marker is cleared (the retry will never execute), so the marker
// never outlives the retry it guarded. Locally committed SQLite records are
// unaffected.
func (e *Engine) releaseAbandonedWork() {
	// Drain the ready work queue (both the main queue and the ready retry
	// channel). Each drained item corresponds to one reserved in-flight unit.
	e.drainReady(e.queue)
	e.drainReady(e.retryCh)
	// Drain the delayed-retry scheduler. Each item was reserved at scheduling
	// time and will never be delivered now; its peer's pending-retry marker is
	// cleared because the discarded retry will never execute.
	e.delayed.mu.Lock()
	for e.delayed.Len() > 0 {
		item := heap.Pop(e.delayed).(workItem)
		e.releaseUnit(item.msgID)
		e.clearPendingRetryFor(item)
	}
	e.delayed.mu.Unlock()
}

// drainReady releases in-flight bookkeeping for items still in the ready work
// queue (the main queue or the ready retry channel) that will never be
// consumed after the engine has been stopped.
func (e *Engine) drainReady(ch chan workItem) {
	for {
		select {
		case item := <-ch:
			e.releaseUnit(item.msgID)
		default:
			return
		}
	}
}

// Enqueue adds a message event to the gossip queue. It is non-blocking.
//
// Ownership: the message's pending entry is reserved (inFlight=1) BEFORE any
// work item is published to a consumer, so the deduplication entry can never be
// released and then recreated by a finished unit. A duplicate ID already in
// flight is suppressed. If the work queue is full (or the engine is stopping),
// the reservation is released and the event is dropped (the locally committed
// message is unaffected).
//
// The admission check, reservation, and queue publication are performed
// atomically under startMu, making them mutually exclusive with Stop's drain:
// if Stop has already drained the queue (its final drain point), the admission
// is rejected and the work item is never published after shutdown cleanup.
// The queue push is non-blocking, so holding startMu during it is safe.
func (e *Engine) Enqueue(event MessageEvent) {
	e.startMu.Lock()
	if !e.started || e.stopped {
		e.startMu.Unlock()
		return
	}
	// Reserve the message's pending entry and publish the work item while
	// holding startMu so that Stop (which also takes startMu for its drain)
	// cannot interleave between the admission check and the publication.
	if e.reserveMsg(event.Message.ID, event.Message) == nil {
		e.startMu.Unlock()
		return // already in-flight
	}
	item := workItem{
		msgID:    event.Message.ID,
		excluded: e.exclusionSetFor(event),
	}
	e.pushWorkLocked(item, event.Message.ID)
	e.startMu.Unlock()
}

// pushWorkLocked places a work item on the bounded work queue, non-blocking.
// The caller must hold e.startMu. On a successful enqueue the reservation
// stands (it is released when the unit completes). On a drop (queue full or
// engine stopping) the reservation is released.
func (e *Engine) pushWorkLocked(item workItem, reservedID string) {
	select {
	case e.queue <- item:
		e.st.addQueued()
	case <-e.engineCtx.Done():
		e.releaseUnit(reservedID)
	default:
		e.releaseUnit(reservedID)
		e.st.addDropped()
		e.logger.Warn("gossip queue full; dropping propagation work",
			slog.String("message_id", item.msgID),
			slog.Int("queue_capacity", e.cfg.QueueCapacity),
		)
	}
}

// exclusionSetFor builds the initial exclusion set: local self and, when a
// valid authenticated sender identity is present, the sender. Unknown/empty
// sender identities are not added.
func (e *Engine) exclusionSetFor(event MessageEvent) map[string]bool {
	snap := e.currentSnapshot()
	excluded := make(map[string]bool, 4)
	if snap.LocalNode != nil {
		if snap.LocalNode.PodIdentity != "" {
			excluded[snap.LocalNode.PodIdentity] = true
		}
		if snap.LocalNode.Hostname != "" {
			excluded[snap.LocalNode.Hostname] = true
		}
	}
	if event.SenderPodID != "" {
		excluded[event.SenderPodID] = true
	}
	return excluded
}

// reserveMsg records the message as in-flight (inFlight=1) and returns its
// work state, or nil when the ID is already in-flight (duplicate suppression).
// The message is stored on the state so that scheduled retries (which may be
// created after the round's work item is processed) can access the immutable
// message. It is called BEFORE any work item for the message is published, so
// the pending entry is alive for the message's entire work lifetime.
func (e *Engine) reserveMsg(id string, msg peertls.MessageWire) *msgState {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	if _, ok := e.pending[id]; ok {
		return nil
	}
	st := &msgState{
		msg:          msg,
		excluded:     make(map[string]bool),
		permanent:    make(map[string]bool),
		failedPeers:  make(map[string]bool),
		pendingRetry: make(map[string]bool),
		inFlightPeer: make(map[string]int),
		attempts:     make(map[string]int),
		inFlight:     1, // the reservation; each published work unit adds one
	}
	e.pending[id] = st
	return st
}

// releaseUnit releases one reserved work unit (a queued round, a scheduled
// retry, or the initial reservation) and removes the message's pending entry
// when its in-flight count reaches 0. It never recreates a released entry: an
// already-released message simply has no entry, so the call is a no-op.
// Each accepted unit is released exactly once, and each rejected unit releases
// its reservation.
func (e *Engine) releaseUnit(id string) {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	st, ok := e.pending[id]
	if !ok {
		return
	}
	st.inFlight--
	if st.inFlight <= 0 {
		delete(e.pending, id)
	}
}

// reserveUnit reserves an additional work unit for a message (a scheduled
// retry), creating a fresh state when the pending entry has been released
// concurrently. The reservation is released later by releaseUnit when the
// retry completes, is cancelled, or is rejected (scheduler at capacity).
func (e *Engine) reserveUnit(id string) {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	st, ok := e.pending[id]
	if !ok {
		st = &msgState{
			excluded:     make(map[string]bool),
			permanent:    make(map[string]bool),
			failedPeers:  make(map[string]bool),
			pendingRetry: make(map[string]bool),
			inFlightPeer: make(map[string]int),
			attempts:     make(map[string]int),
		}
		e.pending[id] = st
	}
	st.inFlight++
}

// tryReserveAttempt atomically verifies that a peer is eligible for another
// delivery and reserves one attempt slot, returning true only when the send
// may proceed. It returns false — and reserves nothing — when the peer is
// already acknowledged (excluded), permanently rejected, has a delayed retry
// outstanding (a fanout round must not bypass the configured backoff by
// re-contacting a transiently failing peer before its retry is due), has
// consumed its per-peer attempt budget (attempts >= MaxAttempts), or already
// has an in-flight delivery (preventing overlapping sends to the same
// peer/message when fanout rounds and delayed retries execute concurrently).
//
// For a targeted retry (isRetry=true) the peer's pending-retry marker is
// cleared atomically with the reservation, so the retry itself can execute
// (the marker is set when the retry is scheduled and must not block the very
// delivery it guards) while a fanout round (isRetry=false) is still blocked by
// it. This is the single atomic eligibility gate: it is the only place a
// delivery attempt is reserved (the attempt counter is incremented here, before
// any network I/O) and the only place a peer is marked in-flight. The caller
// must NOT hold st.mu.
func (e *Engine) tryReserveAttempt(st *msgState, key string, isRetry bool) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.excluded[key] || st.permanent[key] ||
		st.attempts[key] >= e.cfg.MaxAttempts || st.inFlightPeer[key] > 0 {
		return false
	}
	if !isRetry && st.pendingRetry[key] {
		return false
	}
	// For a retry that is about to execute, clear the pending-retry marker so
	// the retry is not blocked by the very marker set to protect it. For a
	// fanout round (isRetry=false) the marker stays set, keeping the peer
	// excluded from this and subsequent rounds until the retry resolves.
	if isRetry {
		delete(st.pendingRetry, key)
	}
	st.attempts[key]++
	st.inFlightPeer[key]++
	return true
}

// msgState returns the work state for a message, or nil if it is not in-flight.
func (e *Engine) msgState(id string) *msgState {
	e.pendingMu.Lock()
	defer e.pendingMu.Unlock()
	return e.pending[id]
}

// worker processes delivery items from the work queue and the delayed-retry
// channel until the engine's context is cancelled.
//
//nolint:contextcheck // uses the engine-owned context (e.engineCtx).
func (e *Engine) worker() {
	defer e.doneWG.Done()
	for {
		select {
		case <-e.engineCtx.Done():
			return
		case item := <-e.queue:
			e.processWork(item)
		case item := <-e.retryCh:
			e.processWork(item)
		}
	}
}

// processWork processes a work item, releasing the message's in-flight
// bookkeeping when the engine has stopped (so a unit consumed just as shutdown
// begins is not double-released by releaseAbandonedWork).
func (e *Engine) processWork(item workItem) {
	if e.engineCtx.Err() != nil {
		e.releaseUnit(item.msgID)
		return
	}
	e.processItem(item)
	e.releaseUnit(item.msgID)
}

// processItem performs one unit of propagation work. The per-message exclusion
// set (initially self + authenticated sender) grows as deliveries succeed and
// is shared across all work items for the message.
//
// A fanout round (target == nil) executes the message's full bounded
// propagation: up to cfg.Rounds fanout-selection rounds, each selecting up to
// cfg.Fanout eligible peers, preferring peers not yet successfully contacted,
// and stopping early when no eligible peers remain. A targeted retry
// (target != nil) re-attempts the single peer that previously failed
// transiently.
func (e *Engine) processItem(item workItem) {
	ctx := e.engineCtx
	st := e.msgState(item.msgID)
	if st == nil {
		return // released concurrently
	}
	// Seed the per-message exclusion set from the work item (self + sender).
	// It is idempotent: retries carry the same set, so re-seeding is a no-op.
	st.mu.Lock()
	for k, v := range item.excluded {
		if v {
			st.excluded[k] = true
		}
	}
	st.mu.Unlock()
	if item.target != nil {
		e.attemptDelivery(ctx, st, *item.target, true)
	} else {
		e.runFanoutRounds(ctx, st, item.excluded)
	}
}

// runFanoutRounds executes the message's bounded fanout: up to cfg.Rounds
// rounds, each selecting up to cfg.Fanout eligible peers.
//
// A peer is "eligible" when it is not in the exclusion set (self, the
// authenticated sender, and peers already successfully contacted) and has not
// exhausted its per-peer attempt budget. Later rounds therefore prefer peers
// not yet successfully contacted, and a peer that already acknowledged the
// message is never contacted again. The per-peer MaxAttempts budget is
// preserved across all rounds and retries. Stale discovery and the sender
// exclusion are honoured by the selector and the exclusion set.
//
// It stops early when a round yields no eligible peers, and immediately when
// the engine context is cancelled. All rounds run within this single message's
// work unit: no additional goroutines are spawned and no work is published to
// the queue (delayed retries for a failed peer are the only follow-on units).
func (e *Engine) runFanoutRounds(ctx context.Context, st *msgState, initialExcluded map[string]bool) {
	for round := 0; round < e.cfg.Rounds; round++ {
		if ctx.Err() != nil {
			return
		}
		e.logger.Debug("gossip propagation round",
			slog.String("message_id", st.id()),
			slog.Int("round", round),
		)
		snap := e.currentSnapshot()
		targets := e.selectRoundTargets(snap, st, initialExcluded)
		if len(targets) == 0 {
			// No eligible peers this round (e.g. all are failed-retry-pending
			// or exhausted). Continue to the next round: a later round may
			// select a different eligible peer (the failed peer is excluded
			// from this round but not from all remaining rounds). Stopping
			// early here would prevent other eligible peers from being
			// contacted in later rounds.
			continue
		}
		for i := range targets {
			if ctx.Err() != nil {
				return
			}
			e.attemptDelivery(ctx, st, targets[i], false)
		}
	}
}

// selectRoundTargets returns up to cfg.Fanout eligible peers for one round:
// peers not in the exclusion set (self + sender + already-contacted), not
// permanently rejected, not failed in a prior round with a retry pending, and
// with remaining attempt budget. The exclusion set is built BEFORE calling the
// selector so that exhausted, permanently rejected, or retry-pending peers
// cannot fill a selection slot and prevent another eligible peer from being
// contacted.
func (e *Engine) selectRoundTargets(snap PeerSnapshot, st *msgState, initialExcluded map[string]bool) []PeerTarget {
	// Build the exclusion set: the initial set (self + sender, captured at
	// enqueue time so a stale snapshot at enqueue still excludes correctly)
	// union the peers already successfully contacted, permanently rejected
	// peers, peers that failed transiently in a prior fanout round while their
	// delayed retry is pending (a fanout round must not bypass the configured
	// backoff by re-contacting a transiently failing peer before its retry is
	// due), and peers that have exhausted their attempt budget.
	st.mu.Lock()
	excluded := make(map[string]bool, len(st.excluded)+len(initialExcluded)+len(st.permanent)+len(st.failedPeers)+len(st.attempts))
	for k, v := range initialExcluded {
		if v {
			excluded[k] = true
		}
	}
	for k, v := range st.excluded {
		if v {
			excluded[k] = true
		}
	}
	for k, v := range st.permanent {
		if v {
			excluded[k] = true
		}
	}
	for k, v := range st.failedPeers {
		if v {
			excluded[k] = true
		}
	}
	for k, v := range st.attempts {
		if v > e.cfg.MaxAttempts {
			excluded[k] = true
		}
	}
	st.mu.Unlock()

	// Select from the eligible population (exhausted, permanently rejected,
	// and retry-pending peers are already excluded, so the selection slot is
	// not wasted on ineligible peers).
	return e.selector.Select(snap, e.cfg.Fanout, excluded)
}

// attemptDelivery performs one delivery attempt to a peer and classifies the
// outcome. The attempt is reserved atomically (tryReserveAttempt) BEFORE the
// network call, so the per-peer MaxAttempts budget is enforced across all
// rounds and delayed retries even when they execute concurrently. On success
// the peer is added to the excluded set (never contacted again). On a
// transient failure a targeted retry is scheduled if the peer still has
// budget; on a permanent failure the peer is marked permanently ineligible
// and no retry occurs.
func (e *Engine) attemptDelivery(ctx context.Context, st *msgState, target PeerTarget, isRetry bool) {
	key := target.Key()
	// Atomically verify the peer is eligible (not acknowledged, permanently
	// rejected, over budget, or in-flight; and, for a fanout round, not
	// retry-pending) and reserve the attempt slot and in-flight slot before
	// any network I/O. If any of those conditions holds, no further network
	// request is made. For a retry the pending-retry marker is cleared
	// atomically with the reservation so the retry is not blocked by it.
	if !e.tryReserveAttempt(st, key, isRetry) {
		return
	}
	e.st.addAttempt()
	// The in-flight slot is reserved above and must be released exactly once
	// on every path (success, permanent failure, and transient failure) so a
	// concurrent round or retry cannot start an overlapping send to the same
	// peer/message while this attempt is in progress.
	defer e.clearPeerInFlight(st, key)
	ack, err := e.delivery.Send(ctx, target, st.wire())
	if err != nil {
		reason := classifyError(err)
		e.st.addFailure()
		st.mu.Lock()
		attempt := st.attempts[key]
		if reason == errPermanent {
			st.permanent[key] = true
			// A permanent rejection supersedes any delayed retry: a queued
			// retry for this peer (scheduled by a concurrent or earlier
			// transient failure) must not execute after the rejection, so the
			// peer's pending-retry marker is cleared now. The queued unit is
			// discarded when the worker consumes it (tryReserveAttempt rejects
			// it) or on the final drain, each releasing its reservation once.
			delete(st.pendingRetry, key)
		} else {
			// A transient failure in a fanout round: mark the peer as failed
			// so later rounds exclude it while its delayed retry is pending
			// (a fanout round must not bypass the configured backoff). The
			// marker is cleared when a subsequent retry to this peer succeeds
			// (the peer has recovered and been acknowledged).
			st.failedPeers[key] = true
		}
		st.mu.Unlock()
		e.logger.Warn("gossip delivery failed",
			slog.String("message_id", st.id()),
			slog.String("peer", target.ExpectedIdentity),
			slog.Int("attempt", attempt),
			slog.String("reason", string(reason)),
		)
		if reason == errPermanent {
			return
		}
		if attempt < e.cfg.MaxAttempts {
			e.st.addRetry()
			e.scheduleRetry(st, target, attempt)
		}
		return
	}
	_ = ack
	e.st.addAck()
	if key != "" {
		st.mu.Lock()
		st.excluded[key] = true
		// A successful acknowledgment (from a retry to a previously failed
		// peer) clears the peer's failed-in-a-round marker: the peer has
		// recovered and been acknowledged, so it should no longer be excluded
		// from later rounds (it is in the excluded set, so it won't be
		// re-contacted anyway, but the marker must not linger).
		delete(st.failedPeers, key)
		st.mu.Unlock()
	}
}

// clearPeerInFlight releases a peer's in-flight delivery slot. It is deferred
// in attemptDelivery on every path so that an in-flight slot never outlives the
// delivery attempt it guards. The caller must NOT hold st.mu.
func (e *Engine) clearPeerInFlight(st *msgState, key string) {
	if key == "" {
		return
	}
	st.mu.Lock()
	st.inFlightPeer[key]--
	if st.inFlightPeer[key] <= 0 {
		delete(st.inFlightPeer, key)
	}
	st.mu.Unlock()
}

// scheduleRetry schedules a targeted retry for a specific failed peer through
// the single bounded retry scheduler. The retry is reserved (a pending-entry
// in-flight slot) BEFORE it is accepted by the scheduler, so the deduplication
// entry survives while the retry is delayed. The per-peer MaxAttempts budget
// (enforced by tryReserveAttempt before the network call) prevents duplicate
// retries from consuming additional budget: a concurrent failure that has
// already consumed the budget will not schedule a retry. If the scheduler is
// at capacity the retry is dropped and the reservation is released.
//
// The peer's pending-retry marker is set under st.mu BEFORE the retry is
// published, so from the moment a transient failure schedules a retry, later
// fanout rounds exclude the peer (selectRoundTargets) and any overlapping
// attempt is rejected (tryReserveAttempt) until the retry is resolved (success
// or permanent rejection) or cancelled (discarded on shutdown or a full
// ready-channel drop). The marker is cleared only by those resolution paths
// (attemptDelivery on success/permanent, clearPendingRetry on a drop).
func (e *Engine) scheduleRetry(st *msgState, target PeerTarget, attempt int) {
	key := target.Key()
	// Mark the peer as retry-pending before publishing the retry, so a
	// concurrent fanout round cannot bypass the configured backoff by
	// re-contacting this peer while its retry is outstanding.
	if key != "" {
		st.mu.Lock()
		st.pendingRetry[key] = true
		st.mu.Unlock()
	}
	delay := e.backoffDelay(attempt)
	t := target
	retry := workItem{
		msgID:    st.id(),
		target:   &t,
		attempt:  attempt,
		excluded: st.excludedCopy(),
	}
	// Reserve the retry's in-flight slot before accepting it, so a concurrent
	// completion of the current unit cannot release the entry before this
	// retry is accounted for.
	e.reserveUnit(st.id())
	accepted := e.delayedPush(retry, delay)
	if !accepted {
		// The retry was not accepted (scheduler at capacity or the engine is
		// stopping): no delayed retry is outstanding, so the pending marker
		// must not remain set (it would block all future attempts to the peer
		// for the rest of the message's life).
		if key != "" {
			st.mu.Lock()
			delete(st.pendingRetry, key)
			st.mu.Unlock()
		}
		e.releaseUnit(st.id())
		e.st.addRetriesDropped()
		e.logger.Warn("gossip delayed-retry scheduler at capacity; dropping retry",
			slog.String("message_id", st.id()),
			slog.String("peer", target.ExpectedIdentity),
			slog.Int("delayed_capacity", e.cfg.QueueCapacity),
		)
		return
	}
	e.st.addRetriesScheduled()
}

// clearPendingRetry clears a peer's pending-retry marker after its delayed
// retry is discarded without executing (a full ready-retry channel, or a
// delayed heap item released on the final drain). It is a no-op when the
// marker is already absent (e.g. the retry was already resolved by a success
// or permanent rejection). The caller must NOT hold st.mu.
func (e *Engine) clearPendingRetry(st *msgState, key string) {
	if key == "" {
		return
	}
	st.mu.Lock()
	delete(st.pendingRetry, key)
	st.mu.Unlock()
}

// currentSnapshot returns the current peer snapshot from the configured source.
func (e *Engine) currentSnapshot() PeerSnapshot {
	e.mu.Lock()
	fn := e.snapshotSource
	e.mu.Unlock()
	if fn == nil {
		return PeerSnapshot{}
	}
	return fn()
}

// SetSnapshotSource configures the snapshot source. Must be called before Start
// (or concurrently with Enqueue).
func (e *Engine) SetSnapshotSource(fn func() PeerSnapshot) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.snapshotSource = fn
}

// Stats returns an immutable snapshot of the engine's counters and queue depth.
// DelayedRetries reports the number of truly scheduled delayed retries: those
// still waiting in the bounded scheduler heap plus those that have fired and
// are in the ready retry channel (not merely the channel length, which would
// omit retries still delayed in the heap).
func (e *Engine) Stats() StatsSnapshot {
	e.pendingMu.Lock()
	pending := len(e.pending)
	e.pendingMu.Unlock()
	delayed := len(e.retryCh)
	e.delayed.mu.Lock()
	delayed += e.delayed.Len()
	e.delayed.mu.Unlock()
	return StatsSnapshot{
		MessagesQueued:   e.st.messagesQueued.Load(),
		MessagesDropped:  e.st.messagesDropped.Load(),
		DeliveryAttempts: e.st.deliveryAttempts.Load(),
		SuccessfulAcks:   e.st.successfulAcks.Load(),
		DeliveryFailures: e.st.deliveryFailures.Load(),
		RetryAttempts:    e.st.retryAttempts.Load(),
		RetriesScheduled: e.st.retriesScheduled.Load(),
		RetriesDropped:   e.st.retriesDropped.Load(),
		QueueDepth:       len(e.queue),
		PendingWork:      pending,
		DelayedRetries:   delayed,
	}
}

// backoffDelay computes the exponential backoff with jitter for a given
// attempt index (1-based: the first retry is attempt 1), bounded by RetryMax.
func (e *Engine) backoffDelay(attempt int) time.Duration {
	delay := e.cfg.RetryBase
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= e.cfg.RetryMax {
			return e.cfg.RetryMax
		}
	}
	if delay > e.cfg.RetryMax {
		delay = e.cfg.RetryMax
	}
	jitter := 0.5 + e.rng.float64()*0.5
	return time.Duration(float64(delay) * jitter)
}

// --- Bounded retry scheduler ---

// retryHeap is a bounded min-heap of delayed-retry work items ordered by their
// fire time. It is guarded by mu, which is also held while the heap is drained
// on shutdown (releaseAbandonedWork). The engine holds at most QueueCapacity
// delayed retries at once: capacity is enforced before a retry is accepted
// (delayedPush), so the collection can never grow past its bound.
type retryHeap struct {
	mu    sync.Mutex
	items []workItem
}

func newRetryHeap() *retryHeap {
	return &retryHeap{}
}

// Len implements heap.Interface.
func (h *retryHeap) Len() int { return len(h.items) }

// Less implements heap.Interface (a min-heap by fire time).
func (h *retryHeap) Less(i, j int) bool {
	return h.items[i].fireBefore(h.items[j].fireAt)
}

// Swap implements heap.Interface.
func (h *retryHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

// Push implements heap.Interface.
func (h *retryHeap) Push(x any) {
	h.items = append(h.items, x.(workItem))
}

// Pop implements heap.Interface.
func (h *retryHeap) Pop() any {
	n := len(h.items)
	it := h.items[n-1]
	h.items[n-1] = workItem{}
	h.items = h.items[:n-1]
	return it
}

// delayedPush attempts to add a delayed retry to the bounded collection. It
// returns false (and does not modify the heap) when the collection is at
// capacity or the engine context is already cancelled, so the caller releases
// the reservation. The caller must not hold e.delayed.mu.
func (e *Engine) delayedPush(item workItem, delay time.Duration) bool {
	item.fireAt = e.now().Add(delay)
	e.delayed.mu.Lock()
	defer e.delayed.mu.Unlock()
	if e.delayed.Len() >= e.cfg.QueueCapacity {
		return false
	}
	if e.engineCtx.Err() != nil {
		return false
	}
	heap.Push(e.delayed, item)
	// Signal the scheduler to re-evaluate the next fire time (non-blocking;
	// the channel is buffered by 1, so a signal is never lost).
	select {
	case e.retrySignal <- struct{}{}:
	default:
	}
	return true
}

// nextFireAt returns the earliest fire time in the heap, or the zero time when
// the heap is empty. The caller must hold e.delayed.mu.
func (e *Engine) nextFireAt() time.Time {
	e.delayed.mu.Lock()
	defer e.delayed.mu.Unlock()
	if e.delayed.Len() == 0 {
		return time.Time{}
	}
	return e.delayed.items[0].fireAt
}

// runRetryScheduler is the single scheduler goroutine. It owns one timer and the
// bounded delayed-retry heap: it sets the timer for the next retry to fire, and
// when that time arrives it moves the fired item onto the ready retry channel
// (or releases it on a full channel / cancelled context). Workers never sleep
// for backoff, and there is at most one timer goroutine regardless of how many
// delayed retries are pending (no goroutine per retry). It exits promptly when
// the engine context is cancelled, after releasing any still-pending retries.
//
//nolint:contextcheck // uses the engine-owned context (e.engineCtx).
func (e *Engine) runRetryScheduler() {
	defer e.doneWG.Done()
	ctx := e.engineCtx
	var timer *time.Timer

	for {
		if ctx.Err() != nil {
			e.releasePendingDelayed()
			return
		}
		next := e.nextFireAt()
		if next.IsZero() {
			// No delayed retries: block until the context is cancelled or a
			// new retry is scheduled (which signals retrySignal and wakes us
			// so we re-read the heap instead of busy-waiting).
			select {
			case <-ctx.Done():
				e.releasePendingDelayed()
				return
			case <-e.retrySignal:
			}
			continue
		}
		d := time.Until(next)
		if d < 0 {
			d = 0
		}
		// Create a fresh timer each iteration (avoiding the time.Timer Reset
		// pitfalls: a timer with a pending channel value, or a Reset on a
		// already-fired timer, both have undefined behaviour). The previous
		// timer, if any, is stopped and closed.
		if timer != nil {
			timer.Stop()
		}
		timer = time.NewTimer(d)
		select {
		case <-ctx.Done():
			e.releasePendingDelayed()
			return
		case <-timer.C:
			// Fire due retries: move each to the ready channel or release it.
			e.fireDueRetries()
		}
	}
}

// fireDueRetries moves all retries whose fire time has arrived onto the ready
// retry channel, releasing (and dropping) any that cannot be enqueued. It
// removes items while their fire time is at or before now (a due retry).
func (e *Engine) fireDueRetries() {
	e.delayed.mu.Lock()
	for e.delayed.Len() > 0 && e.delayed.items[0].fireAt.Before(time.Now().Add(time.Millisecond)) {
		item := heap.Pop(e.delayed).(workItem)
		e.delayed.mu.Unlock()
		e.deliverReadyRetry(item)
		e.delayed.mu.Lock()
	}
	e.delayed.mu.Unlock()
}

// deliverReadyRetry enqueues a fired retry onto the ready retry channel
// non-blockingly. On a full channel or a cancelled context it releases the
// retry's reservation, clears the peer's pending-retry marker (no delayed retry
// is outstanding anymore), and records the drop.
func (e *Engine) deliverReadyRetry(item workItem) {
	select {
	case e.retryCh <- item:
		e.st.addQueued()
	default:
		// The retry was dropped: no delayed retry is outstanding for this
		// peer, so the pending marker (set when the retry was scheduled) must
		// be cleared, or it would block all future attempts to the peer for
		// the rest of the message's life. The reservation is released exactly
		// once here.
		e.releaseUnit(item.msgID)
		e.clearPendingRetryFor(item)
		e.st.addRetriesDropped()
		e.logger.Warn("gossip ready-retry channel full; dropping retry",
			slog.String("message_id", item.msgID),
			slog.Int("queue_capacity", e.cfg.QueueCapacity),
		)
	}
}

// clearPendingRetryFor clears the pending-retry marker for a work item's target
// peer (a no-op for a fanout round, which has no target). It is called when a
// delayed retry is dropped so the marker does not outlive the retry it guarded.
func (e *Engine) clearPendingRetryFor(item workItem) {
	if item.target == nil {
		return
	}
	st := e.msgState(item.msgID)
	if st == nil {
		return
	}
	e.clearPendingRetry(st, item.target.Key())
}

// releasePendingDelayed releases all retries still in the delayed heap. Called
// when the scheduler is shutting down. Each discarded retry's peer
// pending-retry marker is cleared because the retry will never execute. The
// caller must not hold e.delayed.mu.
func (e *Engine) releasePendingDelayed() {
	e.delayed.mu.Lock()
	for e.delayed.Len() > 0 {
		item := heap.Pop(e.delayed).(workItem)
		e.releaseUnit(item.msgID)
		e.clearPendingRetryFor(item)
	}
	e.delayed.mu.Unlock()
}

// --- Error classification ---

type errorClass string

const (
	errTransient errorClass = "transient"
	errPermanent errorClass = "permanent"
)

// classifyError classifies a delivery error as transient or permanent using
// typed inspection (errors.As) of the errors returned by peertls.Client.
// String-matching the error is deliberately avoided: a network error such as
// "dial tcp 127.0.0.1:4090: connection refused" contains "409" but is a
// transient transport failure, not an HTTP 409 conflict.
//
// Classification:
//   - a typed peertls HTTP-status error is permanent for 400/404/409/413/415
//     (the peer deterministically rejected the message) and transient for 429
//     and 5xx (the peer is temporarily overloaded or failing);
//   - a typed peertls acknowledgment error (a successful response with an
//     invalid acknowledgment) is permanent (a protocol defect, not retryable);
//   - anything else (network failures, timeouts, ambiguous transport errors)
//     is transient within MaxAttempts (the receiver's insert is idempotent, so
//     a bounded retry is safe).
func classifyError(err error) errorClass {
	if err == nil {
		return errTransient
	}
	var httpErr *peertls.PeerHTTPStatusError
	if errors.As(err, &httpErr) {
		switch httpErr.StatusCode() {
		case http.StatusBadRequest, http.StatusNotFound, http.StatusConflict,
			http.StatusRequestEntityTooLarge, http.StatusUnsupportedMediaType:
			return errPermanent
		case http.StatusTooManyRequests:
			return errTransient
		}
		// 5xx (and any other non-success) are transient.
		if httpErr.StatusCode() >= 500 {
			return errTransient
		}
		return errPermanent
	}
	var ackErr *peertls.PeerAckError
	if errors.As(err, &ackErr) {
		return errPermanent
	}
	return errTransient
}

func (s *stats) addQueued()           { s.messagesQueued.Add(1) }
func (s *stats) addDropped()          { s.messagesDropped.Add(1) }
func (s *stats) addAttempt()          { s.deliveryAttempts.Add(1) }
func (s *stats) addAck()              { s.successfulAcks.Add(1) }
func (s *stats) addFailure()          { s.deliveryFailures.Add(1) }
func (s *stats) addRetry()            { s.retryAttempts.Add(1) }
func (s *stats) addRetriesScheduled() { s.retriesScheduled.Add(1) }
func (s *stats) addRetriesDropped()   { s.retriesDropped.Add(1) }

// --- Default peer selector ---

// DefaultPeerSelector implements PeerSelector using the configured identity
// suffix to derive expected certificate identities from peer pod identities.
type DefaultPeerSelector struct {
	IdentitySuffix   string
	RNG              *safeRand
	AddressOverrides map[string]string
}

// NewDefaultPeerSelector creates a DefaultPeerSelector with the given identity
// suffix and optional address overrides.
func NewDefaultPeerSelector(identitySuffix string, addressOverrides map[string]string) *DefaultPeerSelector {
	return &DefaultPeerSelector{
		IdentitySuffix:   identitySuffix,
		RNG:              &safeRand{rnd: rand.New(rand.NewSource(time.Now().UnixNano()))}, // #nosec G404: math/rand for peer selection, not security-sensitive
		AddressOverrides: addressOverrides,
	}
}

// Select implements PeerSelector. It selects a random subset of eligible peers
// (up to fanout), excluding self, the authenticated sender, and any peers in
// the exclude set.
//
// Stale DNS membership: for dns mode, a stale snapshot is not treated as
// current discovery — no new fanout is performed. Static mode is deterministic
// and never stale, so its configured addresses remain usable.
func (s *DefaultPeerSelector) Select(snap PeerSnapshot, fanout int, exclude map[string]bool) []PeerTarget {
	if len(snap.Peers) == 0 {
		return nil
	}
	if snap.Stale && snap.Mode == "dns" {
		return nil
	}
	var eligible []PeerInfo
	for _, p := range snap.Peers {
		peerID := p.PodIdentity
		if peerID == "" {
			peerID = p.Hostname
		}
		if exclude[peerID] {
			continue
		}
		if p.PodIdentity == "" {
			continue
		}
		eligible = append(eligible, p)
	}
	if len(eligible) == 0 {
		return nil
	}
	s.RNG.shuffle(len(eligible), func(i, j int) {
		eligible[i], eligible[j] = eligible[j], eligible[i]
	})
	if len(eligible) > fanout {
		eligible = eligible[:fanout]
	}
	targets := make([]PeerTarget, 0, len(eligible))
	for _, p := range eligible {
		identity := p.PodIdentity + "." + s.IdentitySuffix
		dialAddr := p.Address
		if override, ok := s.AddressOverrides[p.PodIdentity]; ok {
			dialAddr = override
		}
		targets = append(targets, PeerTarget{
			DialAddr:         dialAddr,
			ExpectedIdentity: identity,
			PodIdentity:      p.PodIdentity,
		})
	}
	return targets
}
