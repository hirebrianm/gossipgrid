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

	"gossipgrid/internal/lifecycle"
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
// FetchBootstrapInventoryPage is the bootstrap-fetch variant (Milestone 5B,
// Stage 2) that carries the bootstrap request-purpose header to an eligible
// authoritative source. The peertls Client implements both.
type InventoryClient interface {
	FetchInventoryPage(ctx context.Context, dialAddr, expectedIdentity, after string, limit int) (*peertls.InventoryPageResponse, error)
	// FetchBootstrapInventoryPage is the bootstrap-fetch variant (Milestone
	// 5B, Stage 2): the pinned epoch context is passed so the transport
	// binds the outbound headers to the pinned cluster/epoch (Issue 5).
	FetchBootstrapInventoryPage(ctx context.Context, dialAddr, expectedIdentity, after string, limit int, pinnedCtx *lifecycle.RequestContext) (*peertls.InventoryPageResponse, error)
}

// MessageFetcher retrieves a bounded set of messages by ID from a specific peer.
// FetchBootstrapMessages is the bootstrap-fetch variant (Milestone 5B, Stage 2)
// that carries the bootstrap request-purpose header to an eligible
// authoritative source. The peertls Client implements both.
type MessageFetcher interface {
	FetchMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string) ([]peertls.SyncMessageWire, error)
	// FetchBootstrapMessages is the bootstrap-fetch variant (Milestone 5B,
	// Stage 2): the pinned epoch context is passed so the transport binds
	// the outbound headers to the pinned cluster/epoch (Issue 5).
	FetchBootstrapMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string, pinnedCtx *lifecycle.RequestContext) ([]peertls.SyncMessageWire, error)
}

// LocalStore provides the local SQLite operations needed for reconciliation
// and bootstrap baseline installation.
type LocalStore interface {
	FindMissingIDs(ctx context.Context, ids []string) ([]string, error)
	InsertReplicatedMessageReturningInserted(msg *store.Message) (bool, error)
	// InsertReplicatedMessagesBounded installs a bounded batch of replicated
	// messages in a single, context-aware, narrowly bounded SQLite transaction
	// (Milestone 5B, Stage 2). It is the transactional commit boundary for
	// bootstrap baseline installation: a single BEGIN…COMMIT that inserts the
	// batch atomically (a conflict or error rolls the batch back; an idempotent
	// duplicate is a no-op; an expired record is skipped). The
	// expirationThreshold callback is invoked inside the transaction, after
	// the transaction acquires the SQLite write lock, so the threshold
	// reflects the time spent waiting for the connection and write lock. A
	// nil callback disables retention filtering. The *store.Store implements
	// this method.
	InsertReplicatedMessagesBounded(ctx context.Context, msgs []*store.Message, expirationThreshold func() time.Time) (store.BootstrapInsertResult, error)
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

// BootstrapGuard is the commit-boundary guard for installing a fetched
// bootstrap baseline (Milestone 5B, Stage 2). It executes a short,
// nonblocking commit callback (installing an authenticated, same-epoch
// bootstrap response into the local store) while holding the lifecycle
// controller's authorization read lock (c.mu RLock) and the lifecycle state
// machine's read lock (m.mu RLock), revalidating the DEDICATED bootstrap
// authorization decision (bootstrapAuthorized: a bound, epoch-bound SYNCING
// replica with a fresh, valid, matching authoritative observation and a
// matching durable binding, not stopped) at the actual local write boundary.
//
// A successful bootstrap HTTP request does NOT by itself authorize
// installation of its response: the installation is gated on the bootstrap
// authorization being valid at commit time. A revocation (a state change out
// of SYNCING, an epoch/metadata change, a stale or invalid observation, a
// binding change, or Stop) that linearizes before the guard enters is
// observed and the callback is not executed (a rejection error is returned);
// a revocation after the callback completes is a normal, permitted revocation
// (already-committed valid data is preserved).
//
// It is the same lock-order and non-recursive contract as CommitGuard
// (Controller.mu RLock → Machine.mu RLock → Engine.mu Lock): the callback
// must NOT re-read any method that acquires Controller.mu or Machine.mu,
// perform network I/O, or execute a blocking SQLite query. The callback
// performs the idempotent local insert of the fetched baseline records. A nil
// guard (lifecycle protection disabled) executes the callback directly
// (legacy behavior preserved).
//
// The expected parameter (Milestone 5B, Stage 2, Issue 4) is the RequestContext
// pinned from the consistent authorized snapshot at the start of the bootstrap
// page (the cluster and epoch the page's data was fetched under). The guard
// validates this expected context against the CURRENT authorization snapshot
// under the same locks it holds for the bootstrap-authorized predicate: a
// mismatch (the local epoch changed since the pin) or a non-authorized
// snapshot (revocation, state change, stop) rejects the commit (the callback
// is not executed and the transaction is rolled back). This prevents an old
// response (fetched under epoch A) from being installed under newly acquired
// authorization (epoch B). A zero expected context (lifecycle protection
// disabled) is not validated (the guard checks only the predicate).
type BootstrapGuard interface {
	CommitUnderBootstrapAuthorization(expected lifecycle.RequestContext, fn func() error) error
}

// BootstrapAuthorizationSource is the production-wired source of the
// requester's bootstrap authorization snapshot (Milestone 5B, Stage 2, Issue
// 1 and Issue 4). It provides a consistent, internally synchronized view of
// the requester's authorized cluster and epoch (the snapshot that the
// bootstrap admission decision is based on), so the engine can pin the
// expected RequestContext ONCE at the start of a bootstrap page and revalidate
// against the current snapshot at every boundary (inventory, each fetch batch
// and retry, and installation) without combining decisions from two different
// snapshots.
//
// The production implementation is the lifecycle controller, which exposes
// this via the bootstrap admission guard. The expected context is derived
// from the SAME authorized snapshot that the admission check uses (the
// controller's accepted metadata, read under its authorization locks), so the
// pinned expected context and the admission decision are always consistent.
//
// A nil source (lifecycle protection disabled) means the engine does not pin
// or revalidate a context (legacy behavior: the request carries whatever
// context the provider supplies and no local revalidation is performed).
type BootstrapAuthorizationSource interface {
	// AuthorizationSnapshot returns a consistent, authorized view of the
	// requester's cluster and epoch (and the admission decision for bootstrap
	// fetch) under the controller's authorization read locks. The returned
	// snapshot's RequestContext is the expected context the engine pins for
	// the bootstrap page; Authorized is false (or an error is returned) when
	// the requester is not authorized to fetch (not SYNCING, unbound, stale,
	// invalid, stopped, or an epoch/cluster mismatch).
	BootstrapAuthorizationSnapshot() (lifecycle.RequestContext, bool, error)
}

// BootstrapPageResult is the structured result of a single bootstrap baseline
// inventory page (Milestone 5B, Stage 2, Issue 6). It is returned by
// FetchBootstrapBaseline and is designed to be the unit of work that Stage 3
// (sweep scheduling and convergence evidence) will consume, without Stage 3
// needing to re-derive the authorization state.
//
// The result distinguishes the lifecycle of each record it touched and the
// completion state of the page, so a consumer can tell a fully-succeeded page
// from a partial or failed one without re-querying the controller.
type BootstrapPageResult struct {
	// Peer is the source the page was fetched from.
	Peer PeerTarget

	// InputCursor is the exclusive cursor that was passed to the inventory
	// fetch (the "after" parameter). It is empty for the first page. A
	// failed/revoked page must NEVER advance the sweep cursor: the next
	// sweep attempt resumes from InputCursor (the original page boundary),
	// not from NextCursor.
	InputCursor string

	// NextCursor is the exclusive cursor for the next inventory page. It is
	// empty on the final page (HasMore is false). Preserved verbatim from the
	// response so Stage 3 can resume a sweep from the same position. Only
	// valid (advanceable) when Completed is true; on a failed page the sweep
	// must resume from InputCursor.
	NextCursor string
	// HasMore is true when the source's inventory has additional pages beyond
	// this one.
	HasMore bool

	// Records: per-record accounting for every ID in this page.
	//
	// Processing-stage sets (overlapping; an ID can appear in multiple sets):
	//
	//   - Inventoried: the ID was present in this inventory page (the source
	//     advertised it as retained). This is the full page content.
	//   - Missing: the ID was in the page but absent from the local store at
	//     the diff (a candidate for fetch+install). A subset of Inventoried.
	//   - Fetched: the ID was fetched from the source (the source served the
	//     complete record). A subset of Missing (IDs that were actually
	//     served, not confirmed absent).
	//
	// Terminal record dispositions (mutually exclusive per fetched ID; each
	// fetched ID appears in exactly one of these sets):
	//
	//   - Inserted: the fetched record was actually stored locally (a new row
	//     was created under the guarded commit; the transaction committed).
	//   - AlreadyPresent: the fetched record was identical to an existing
	//     local row (idempotent no-op; the transaction committed).
	//   - Expired: the fetched record was expired on the requester side at
	//     the install boundary (retention threshold) and was skipped without
	//     being stored.
	//   - ConfirmedAbsent: the source confirmed the ID was absent or expired
	//     (the 404 missing-ID response reported it; it was not fetched).
	//   - Conflict: the fetched record conflicted with a different local row
	//     (an immutable content conflict; the local row was preserved and the
	//     page failed).
	Inventoried     []string
	Missing         []string
	Fetched         []string
	Inserted        []string
	AlreadyPresent  []string
	Expired         []string
	ConfirmedAbsent []string
	Conflict        []string

	// Completed is true when the page reached a clean terminal state (no
	// unresolved error, no conflict, and the authorization was validated at
	// the install boundary). A failed or revoked page (Err != nil) is NEVER
	// completed, even if some records were inserted before the failure.
	//
	// Completed is also set only after the bootstrap authorization is
	// validated at the install boundary — an empty or all-already-present page
	// is completed only if the requester was still authorized to install
	// (the authorization must be validated before reporting success, not
	// assumed from the absence of records to install).
	Completed bool

	// Error is set when the page failed (a fetch or install error, an
	// admission or commit-guard rejection, a context cancellation, or an
	// authorization mismatch). When Error is set, Completed is false and the
	// page must not be reported as successful. The already-inserted records
	// (Inserted) are preserved (no rollback); the remaining work is
	// resumable from InputCursor on the next Stage 3 sweep.
	Error error
}

// IsPartial returns true when the page failed (Error is set or a conflict
// occurred) but some records were committed before the failure. A partial
// page is NOT completed: the caller must NOT report success and must NOT
// advance the sweep cursor past InputCursor. The committed records remain
// valid and idempotent (a retry resumes from InputCursor and the already-
// inserted records are recognized as AlreadyPresent).
func (r *BootstrapPageResult) IsPartial() bool {
	return !r.Completed && (len(r.Inserted) > 0 || len(r.AlreadyPresent) > 0)
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

	// admissionCheck is the ordinary lifecycle admission guard for outbound
	// anti-entropy (Milestone 5A). When set, syncPeer checks it before
	// performing any network operations. It is wired to
	// Admission.CheckReplicationOutbound in production (a ready-state
	// requirement). A nil check means lifecycle protection is disabled.
	admissionCheck func(ctx context.Context) error

	// bootstrapAdmissionCheck is the DEDICATED bootstrap outbound admission
	// guard (Milestone 5B, Stage 2). When set, FetchBootstrapBaseline checks
	// it before issuing any bootstrap request and before each retry. It is
	// wired to Admission.CheckBootstrapFetch in production (a narrowly
	// scoped, SYNCING-only, epoch-bound decision distinct from the ordinary
	// anti-entropy admission, which requires a ready state that a joining
	// replica does not have). A nil check means lifecycle protection is
	// disabled (legacy passthrough).
	//
	// The separation is essential: a joining SYNCING replica is authorized for
	// bootstrap fetch but NOT for ordinary replication. Using the ordinary
	// admission check for bootstrap would make production bootstrap impossible
	// from SYNCING (CheckReplicationOutbound requires a ready state).
	bootstrapAdmissionCheck func(ctx context.Context) error

	// commitGuard is the lifecycle commit guard (Milestone 5A). When set,
	// commitCursor executes the cursor mutation while holding the controller's
	// authorization read lock (c.mu RLock) and the lifecycle state machine's
	// read lock (m.mu RLock), closing the window between the admission
	// decision and the in-memory commit. A nil guard (lifecycle protection
	// disabled) executes the commit directly under e.mu.
	commitGuard CommitGuard

	// bootstrapGuard is the lifecycle commit guard for installing a fetched
	// bootstrap baseline (Milestone 5B, Stage 2). When set, commitBootstrap
	// executes the local insert while holding the controller's authorization
	// read lock (c.mu RLock) and the machine's read lock (m.mu RLock),
	// revalidating the dedicated bootstrap authorization at the write boundary.
	// A nil guard (lifecycle protection disabled) executes the insert directly.
	bootstrapGuard BootstrapGuard

	// bootstrapAuthSource is the production-wired source of the requester's
	// bootstrap authorization snapshot (Milestone 5B, Stage 2, Issue 1 and
	// Issue 4). When set, FetchBootstrapBaseline pins the expected
	// RequestContext from a consistent authorized snapshot ONCE at the start
	// of a page and revalidates it against the current snapshot at every
	// boundary (inventory, each fetch batch and retry, and installation).
	// A nil source (lifecycle protection disabled) means no context is pinned
	// or revalidated (legacy behavior).
	bootstrapAuthSource BootstrapAuthorizationSource

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

// SetBootstrapGuard configures the lifecycle commit guard for installing a
// fetched bootstrap baseline (Milestone 5B, Stage 2). When set, commitBootstrap
// executes the local insert under the controller's authorization read lock
// (c.mu RLock) and machine read lock (m.mu RLock), revalidating the dedicated
// bootstrap authorization at the write boundary so a revocation or epoch change
// between the HTTP fetch and the local insert prevents installation. A nil
// guard (lifecycle protection disabled) executes the insert directly (legacy
// behavior preserved). Must be called before Start.
func (e *Engine) SetBootstrapGuard(guard BootstrapGuard) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.bootstrapGuard = guard
}

// SetBootstrapAdmissionCheck configures the DEDICATED bootstrap outbound
// admission guard (Milestone 5B, Stage 2). When set, FetchBootstrapBaseline
// checks the guard before issuing a bootstrap request and before each retry.
// It is wired to Admission.CheckBootstrapFetch in production — the narrowly
// scoped, SYNCING-only, epoch-bound bootstrap decision that is distinct from
// the ordinary anti-entropy admission (SetAdmissionCheck, wired to
// CheckReplicationOutbound, which requires a ready state). A nil check means
// lifecycle protection is disabled (legacy passthrough). Must be called before
// the bootstrap baseline fetch is used.
//
// The separation from SetAdmissionCheck is essential: a joining SYNCING replica
// is authorized for bootstrap fetch but not for ordinary replication, so the
// two admission paths must be independent.
func (e *Engine) SetBootstrapAdmissionCheck(check func(ctx context.Context) error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.bootstrapAdmissionCheck = check
}

// SetBootstrapAuthorizationSource configures the production-wired source of the
// requester's bootstrap authorization snapshot (Milestone 5B, Stage 2, Issue 1
// and Issue 4). When set, FetchBootstrapBaseline pins the expected
// RequestContext from a consistent authorized snapshot once per page and
// revalidates it at every boundary. A nil source (lifecycle protection
// disabled) means no context is pinned or revalidated (legacy behavior). Must
// be called before the bootstrap baseline fetch is used.
func (e *Engine) SetBootstrapAuthorizationSource(src BootstrapAuthorizationSource) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.bootstrapAuthSource = src
}

// commitBootstrap executes a short, nonblocking local insert callback for a
// fetched bootstrap baseline while holding the lifecycle controller's
// authorization read lock (c.mu RLock) and machine read lock (m.mu RLock),
// revalidating the DEDICATED bootstrap authorization (bootstrapAuthorized) at
// the actual local write boundary (Milestone 5B, Stage 2).
//
// A successful bootstrap HTTP request does not by itself authorize
// installation of its response. This method is the linearization point: if the
// bootstrap authorization is no longer valid at commit time (the replica left
// SYNCING, the epoch/metadata changed, the observation is stale or invalid,
// the binding changed, or the controller stopped), the callback is not
// executed and a rejection error is returned. Already-committed valid data is
// preserved (this method never deletes or resets the store).
//
// Lock-order contract: Controller.mu (RLock) → Machine.mu (RLock) →
// Engine.mu (Lock). The callback may take the engine's mutex but must NOT
// re-read any method that acquires Controller.mu or Machine.mu (both RWMutexes
// are non-reentrant), perform network I/O, or execute a blocking SQLite
// query. When the bootstrap guard is nil (lifecycle protection disabled), the
// callback is executed directly under e.mu (legacy behavior preserved).
//
// The expected parameter is the pinned RequestContext for the bootstrap page
// (Issue 4); the guard validates it against the current authorization snapshot
// under the same locks, rejecting a commit when the epoch changed or the
// authorization was revoked since the pin.
func (e *Engine) commitBootstrap(expected lifecycle.RequestContext, fn func() error) error {
	e.mu.Lock()
	guard := e.bootstrapGuard
	e.mu.Unlock()

	if guard != nil {
		return guard.CommitUnderBootstrapAuthorization(expected, fn)
	}
	// Lifecycle protection disabled: insert directly under e.mu.
	return fn()
}

// FetchBootstrapBaseline retrieves a bounded bootstrap baseline page from an
// eligible authoritative source and installs the fetched records into the local
// store through the guarded bootstrap commit boundary (Milestone 5B, Stage 2).
//
// It is the transport-and-commit mechanism for a joining, epoch-bound SYNCING
// replica to install an authenticated, same-epoch baseline. It is NOT the full
// synchronization orchestrator (Stage 3): it fetches a single inventory page
// (after is the exclusive cursor, empty for the first page) and the
// corresponding message records, and installs them in bounded batches. It
// performs NO source discovery or selection, NO READY promotion, and NO
// repeated sweeping; those are Stage 3's job. The eligible source is supplied
// by the caller.
//
// Authorization (narrowly scoped, fail-closed, epoch-bound):
//
//   - Before issuing the request, the engine's DEDICATED bootstrap admission
//     guard is checked (e.bootstrapAdmissionCheck, wired to
//     Admission.CheckBootstrapFetch in production — a SYNCING-only, epoch-bound
//     decision distinct from the ordinary anti-entropy admission, which
//     requires a ready state a joining replica lacks). A non-SYNCING, unbound,
//     or un-authorized requester is blocked (no request is sent).
//   - The expected cluster/epoch is pinned ONCE from a consistent authorized
//     snapshot (e.bootstrapAuthSource, the production-wired lifecycle
//     controller). The pinned context is revalidated against the CURRENT
//     snapshot before the inventory fetch, before every fetch batch and
//     retry, and before the installation commit, so a concurrent epoch change
//     or revocation is observed at the boundary and the page is failed (no
//     data installed). An old response is never installed under newly
//     acquired authorization.
//   - The request is sent over mTLS with the locally authorized epoch headers
//     and the bootstrap request-purpose header (the peertls client's
//     FetchBootstrap* methods). The response epoch envelope is validated
//     client-side, so a legacy or wrong-epoch peer is rejected as a source.
//   - The local install is executed in a single, bounded, context-aware
//     SQLite transaction under the bootstrap commit guard
//     (commitBootstrap → CommitUnderBootstrapAuthorization), which holds the
//     controller's authorization read lock for the duration of the
//     transaction. A revocation or epoch change between the fetch and the
//     commit prevents installation; the transaction is rolled back (no
//     partial commit).
//
// Boundedness: the missing IDs are split into batches of
// e.cfg.FetchBatchSize (the configured, validated batch limit; the peer
// protocol also caps a batch at 8). Each batch is a single transactional
// install (Issue 5). The per-HTTP-request timeout is owned by the peertls
// client (Issue 2): each network operation (inventory fetch, message-fetch
// batch, partial-404 retry) gets its own client.requestTimeout budget. The
// page-level parent context bounds the whole page (cancellation, overall
// deadline). The context is checked before every request and every batch.
//
// Retention (requester-side, single-transaction correction): the same
// requester-side expiration threshold used by ordinary anti-entropy
// (store.ExpirationThreshold(e.retentionTTL, e.now())) is applied at the
// local install boundary. The threshold is evaluated INSIDE the
// transactional install, after the transaction acquires the SQLite write
// lock, so it reflects the time spent waiting for the connection and write
// lock. A fetched record whose creation timestamp is strictly older than
// the threshold is skipped (classified Expired) without being stored,
// without resurrection, and without failing the page — the source's
// inventory (which excludes expired records) will not re-advertise it.
//
// Idempotency and preservation: the install uses the store's idempotent
// ON CONFLICT DO NOTHING path; duplicate records are no-ops and
// already-committed valid data is never deleted or reset. A rejected commit
// (revocation observed) rolls back the current batch (no partial commit);
// records committed in prior batches of the page are preserved. A content
// conflict fails the batch (rollback) and the page (unresolved conflict); the
// local row is preserved.
//
// The method returns a structured *BootstrapPageResult (Issue 6) that
// distinguishes NextCursor/HasMore, the per-record categories (inventoried,
// missing, fetched, inserted, already-present, expired, confirmed-absent,
// conflict), and completed-versus-partial. A failed or revoked page is NEVER
// marked completed, even if some records were committed in a prior batch.
// Authorization is validated before reporting successful completion,
// including empty and all-already-present pages (the authorization must be
// validated at the install boundary, not assumed from the absence of
// records).
func (e *Engine) FetchBootstrapBaseline(ctx context.Context, dialAddr, expectedIdentity, after string, limit int) *BootstrapPageResult {
	peer := PeerTarget{DialAddr: dialAddr, ExpectedIdentity: expectedIdentity}
	res := &BootstrapPageResult{Peer: peer, InputCursor: after}

	// Boundary 1 (before issuing any request): the DEDICATED bootstrap
	// admission check (wired to CheckBootstrapFetch in production, NOT the
	// ordinary CheckReplicationOutbound). A non-authorized requester (not
	// SYNCING, unbound, stale, invalid, stopped, or wrong epoch) is blocked
	// before any request. A nil check (lifecycle protection disabled)
	// always permits (legacy behavior).
	if err := e.bootstrapAdmissionOK(ctx); err != nil {
		res.Error = fmt.Errorf("lifecycle bootstrap admission: %w", err)
		return res
	}

	// Pin the expected cluster/epoch ONCE from a consistent authorized
	// snapshot (the production-wired lifecycle controller). The pinned
	// context is the single epoch the whole page is bound to; it is
	// revalidated at every boundary. A nil source (lifecycle protection
	// disabled) yields a zero context (no revalidation).
	expected, err := e.pinBootstrapAuth(ctx)
	if err != nil {
		res.Error = err
		return res
	}

	// Revalidate the pinned context before the inventory fetch (boundary 2:
	// a revocation or epoch change between the pin and the inventory fetch
	// fails the page before any network I/O). The revalidation context is
	// only used for a ctx.Err() cancellation check; the actual network
	// request gets its own request-timeout context (Issue 3: the request
	// timeout is per network operation, not a page-level snapshot).
	if err := e.revalidateBootstrapAuth(ctx, expected); err != nil {
		res.Error = err
		return res
	}

	// Fetch a bounded inventory page from the eligible source. The peertls
	// client's FetchBootstrapInventoryPage carries the bootstrap request
	// purpose and validates the response epoch envelope (a legacy or
	// wrong-epoch peer is rejected as a source).
	//
	// Timeout ownership (Issue 2): the engine passes the page-level parent
	// context (ctx) directly to FetchBootstrapInventoryPage WITHOUT wrapping
	// it in its own request timeout. The peertls client applies its own
	// per-HTTP-request timeout independently to the inventory request.
	// The engine's RequestTimeout is a page-level cancellation bound, not a
	// shared wall-clock deadline across multiple HTTP operations.
	page, err := e.invClient.FetchBootstrapInventoryPage(ctx, dialAddr, expectedIdentity, after, limit, &expected)
	if err != nil {
		res.Error = fmt.Errorf("fetching bootstrap inventory from peer %s: %w", expectedIdentity, err)
		return res
	}
	res.NextCursor = page.NextCursor
	res.HasMore = page.HasMore
	res.Inventoried = append(res.Inventoried, page.IDs...)
	if len(page.IDs) == 0 {
		// No records to install (the source has no baseline or the sweep is
		// complete). This is a successful no-op, but the authorization must
		// still be validated before reporting completion (Issue 6): a
		// revoked requester does not "complete" an empty page.
		if err := e.revalidateBootstrapAuth(ctx, expected); err != nil {
			res.Error = err
			return res
		}
		res.Completed = true
		return res
	}

	// Diff against the local store: fetch only the IDs we do not have.
	// FindMissingIDs is a local DB operation (no network); the page-level
	// parent context bounds it (the caller's ctx may independently cancel
	// the whole page).
	missing, err := e.localStore.FindMissingIDs(ctx, page.IDs)
	if err != nil {
		res.Error = fmt.Errorf("diffing bootstrap inventory against local store: %w", err)
		return res
	}
	res.Missing = append(res.Missing, missing...)

	// Partition the page's IDs into the already-present set (the complement
	// of missing).
	missingSet := make(map[string]bool, len(missing))
	for _, id := range missing {
		missingSet[id] = true
	}
	for _, id := range page.IDs {
		if !missingSet[id] {
			res.AlreadyPresent = append(res.AlreadyPresent, id)
		}
	}

	if len(missing) == 0 {
		// All records are already present: the baseline is installed (no-op).
		// The authorization must still be validated before reporting
		// completion (Issue 6).
		if err := e.revalidateBootstrapAuth(ctx, expected); err != nil {
			res.Error = err
			return res
		}
		res.Completed = true
		return res
	}

	// Split the missing IDs into bounded batches of e.cfg.FetchBatchSize
	// (Issue 2). Each batch is a single transactional install and each
	// network request uses the request-timeout context. A batch that fails
	// (fetch error, conflict, commit rejection, or epoch change) fails the
	// page; batches committed before the failure are preserved (no
	// rollback across batches).
	batchSize := e.cfg.FetchBatchSize
	if batchSize < 1 {
		batchSize = 1
	}
	for start := 0; start < len(missing); start += batchSize {
		end := start + batchSize
		if end > len(missing) {
			end = len(missing)
		}
		batch := missing[start:end]

		// Revalidate the pinned context before each fetch batch (boundary 3:
		// a revocation or epoch change between batches fails the page).
		// The revalidation context is the page-level parent (used only for
		// a ctx.Err() cancellation check).
		if err := e.revalidateBootstrapAuth(ctx, expected); err != nil {
			res.Error = err
			return res
		}

		// Fetch the batch's records from the eligible source. The peertls
		// client's FetchBootstrapMessages carries the bootstrap request
		// purpose, validates the response epoch envelope, and performs the
		// 404 missing-ID reconciliation (a record that expired on the
		// source between the inventory and the fetch is reported as
		// confirmed-absent, not fetched).
		//
		// Timeout ownership (Issue 2): the engine passes the page-level
		// parent context (ctx) directly to FetchBootstrapMessages WITHOUT
		// wrapping it in its own request timeout. The peertls client
		// applies its own per-HTTP-request timeout (client.requestTimeout)
		// independently to each network operation: the initial request and
		// the partial-404 retry each get their own RequestTimeout budget.
		// This is the authoritative per-HTTP-request timeout policy for
		// bootstrap transport. The engine's RequestTimeout is NOT a shared
		// budget across multiple HTTP operations; it is a page-level
		// cancellation bound (the caller can cancel the whole page via ctx),
		// not a wall-clock deadline applied to individual requests.
		msgs, err := e.msgFetcher.FetchBootstrapMessages(ctx, dialAddr, expectedIdentity, batch, &expected)
		if err != nil {
			res.Error = fmt.Errorf("fetching bootstrap messages from peer %s: %w", expectedIdentity, err)
			return res
		}
		fetchedSet := make(map[string]bool, len(msgs))
		for _, m := range msgs {
			fetchedSet[m.ID] = true
		}
		for _, id := range batch {
			if fetchedSet[id] {
				res.Fetched = append(res.Fetched, id)
			} else {
				// The source confirmed the ID was absent or expired (the
				// 404 reconciliation reported it); it was not fetched.
				res.ConfirmedAbsent = append(res.ConfirmedAbsent, id)
			}
		}
		if len(msgs) == 0 {
			// All of this batch's IDs were confirmed absent/expired by the
			// source: nothing to install. Continue to the next batch (a
			// confirmed-absent record is a legitimate, expected outcome, not
			// a failure).
			continue
		}

		// Build the store messages (validating the timestamp). A malformed
		// timestamp is a batch failure (the record cannot be installed).
		var storeMsgs []*store.Message
		for _, m := range msgs {
			ts, perr := parseTimestamp(m.CreatedAt)
			if perr != nil {
				res.Error = fmt.Errorf("bootstrap message %q has malformed timestamp: %v", m.ID, perr)
				return res
			}
			storeMsgs = append(storeMsgs, &store.Message{
				ID:        m.ID,
				Body:      m.Body,
				Origin:    m.Origin,
				CreatedAt: ts,
			})
		}

		// Revalidate the pinned context before the installation commit
		// (boundary 4: a revocation or epoch change between the fetch and
		// the commit fails the page; the commit guard ALSO revalidates at
		// the write boundary under the same locks).
		if err := e.revalidateBootstrapAuth(ctx, expected); err != nil {
			res.Error = err
			return res
		}

		// Install the batch through the guarded bootstrap commit boundary: a
		// single, bounded, context-aware SQLite transaction under the
		// controller's authorization read lock. The retention threshold
		// callback is evaluated INSIDE the transaction, after the
		// transaction acquires the SQLite write lock (single-transaction
		// correction). A nil callback (retention disabled) skips the
		// write-lock acquisition and threshold evaluation.
		var insertRes store.BootstrapInsertResult
		var thresholdCb func() time.Time
		if e.retentionTTL > 0 {
			thresholdCb = func() time.Time {
				return store.ExpirationThreshold(e.retentionTTL, e.now())
			}
		}
		ierr := e.commitBootstrap(expected, func() error {
			r, ierr := e.localStore.InsertReplicatedMessagesBounded(ctx, storeMsgs, thresholdCb)
			insertRes = r
			return ierr
		})
		if ierr != nil {
			// Commit rejected (revocation or epoch change observed at the
			// write boundary) or a non-conflict insert error: the batch is
			// rolled back (no partial commit) and the page is failed.
			// Batches committed before this one are preserved.
			res.Error = fmt.Errorf("installing bootstrap batch: %w", ierr)
			return res
		}
		// Record the per-disposition counts from the transactional install.
		res.Inserted = append(res.Inserted, insertRes.Inserted...)
		res.AlreadyPresent = append(res.AlreadyPresent, insertRes.AlreadyPresent...)
		res.Expired = append(res.Expired, insertRes.Expired...)
		if len(insertRes.Conflict) > 0 {
			// An immutable content conflict: the batch is rolled back (no
			// records in this batch persisted) and the page is failed
			// (unresolved conflict). The local row is preserved.
			res.Conflict = append(res.Conflict, insertRes.Conflict...)
			res.Error = fmt.Errorf("unresolved conflict on bootstrap batch (local row preserved): %v", insertRes.Conflict)
			return res
		}
	}

	// Revalidate the pinned context before reporting completion (Issue 6:
	// the authorization must be validated at the install boundary, not
	// assumed from the absence of records). A revoked or epoch-changed
	// requester does not "complete" a page that was fully processed.
	if err := e.revalidateBootstrapAuth(ctx, expected); err != nil {
		res.Error = err
		return res
	}
	res.Completed = true
	return res
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

// bootstrapAdmissionOK returns nil when the DEDICATED bootstrap outbound
// admission is currently authorized, or an error when the bootstrap admission
// guard rejects the operation. A nil guard (lifecycle protection disabled)
// always permits. It is the bootstrap counterpart to admissionOK: it uses
// e.bootstrapAdmissionCheck (wired to CheckBootstrapFetch in production), NOT
// e.admissionCheck (wired to CheckReplicationOutbound, which requires a ready
// state a joining replica lacks). It is checked before issuing a bootstrap
// request and before each retry, so a bootstrap pass admitted while authorized
// does not fetch after a subsequent revocation.
func (e *Engine) bootstrapAdmissionOK(ctx context.Context) error {
	e.mu.Lock()
	check := e.bootstrapAdmissionCheck
	e.mu.Unlock()
	if check == nil {
		return nil
	}
	return check(ctx)
}

// pinBootstrapAuth pins the expected RequestContext for a bootstrap page from
// a consistent, authorized snapshot (Milestone 5B, Stage 2, Issue 1 and Issue
// 4). It reads the bootstrap authorization source once (under the
// controller's authorization read locks) and returns the expected context the
// engine binds to for the whole page (inventory, every fetch batch and retry,
// and installation). The expected context comes from the SAME snapshot the
// bootstrap admission decision is based on, so the pinned context and the
// admission decision are always consistent (no combining decisions from two
// different snapshots).
//
// When the bootstrap authorization source is nil (lifecycle protection
// disabled), the expected context is the zero context and revalidation is a
// no-op (legacy behavior).
//
// It returns an error (and no pinned context) when the source is set but the
// requester is not authorized (not SYNCING, unbound, stale, invalid, stopped,
// or a context mismatch) — a bootstrap request must not be issued for a
// requester that is not authorized to fetch.
func (e *Engine) pinBootstrapAuth(ctx context.Context) (lifecycle.RequestContext, error) {
	if ctx.Err() != nil {
		return lifecycle.RequestContext{}, ctx.Err()
	}
	e.mu.Lock()
	src := e.bootstrapAuthSource
	e.mu.Unlock()
	if src == nil {
		// Lifecycle protection disabled: no context is pinned or revalidated.
		return lifecycle.RequestContext{}, nil
	}
	reqCtx, authorized, err := src.BootstrapAuthorizationSnapshot()
	if err != nil {
		return lifecycle.RequestContext{}, fmt.Errorf("reading bootstrap authorization snapshot: %w", err)
	}
	if !authorized {
		return lifecycle.RequestContext{}, errors.New("lifecycle bootstrap admission: requester is not authorized to fetch (not SYNCING, unbound, stale, invalid, stopped, or epoch/cluster mismatch)")
	}
	return reqCtx, nil
}

// revalidateBootstrapAuth revalidates the pinned expected RequestContext
// against the CURRENT bootstrap authorization snapshot (Milestone 5B, Stage 2,
// Issue 4). It is called at every boundary (before the inventory fetch, before
// every fetch batch and retry, and before the installation commit) so a
// concurrent epoch change or revocation between the pin and the boundary is
// observed and the page is failed (no request is issued, no data installed).
//
// The revalidation reads the bootstrap authorization source fresh (a new
// consistent snapshot) and compares its cluster and epoch against the pinned
// expected context: a mismatch means the local epoch changed since the pin and
// the page's data (fetched under the old epoch) must not be installed under
// the new authorization. A non-authorized snapshot (revocation, state change,
// stop) is likewise rejected. When the source is nil (lifecycle protection
// disabled), this is a no-op (nil).
func (e *Engine) revalidateBootstrapAuth(ctx context.Context, expected lifecycle.RequestContext) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	e.mu.Lock()
	src := e.bootstrapAuthSource
	e.mu.Unlock()
	if src == nil {
		return nil // lifecycle protection disabled
	}
	reqCtx, authorized, err := src.BootstrapAuthorizationSnapshot()
	if err != nil {
		return fmt.Errorf("revalidating bootstrap authorization snapshot: %w", err)
	}
	if !authorized {
		return errors.New("lifecycle bootstrap admission: requester is no longer authorized (revocation or state change observed at the boundary)")
	}
	if expected.ClusterID == "" && expected.EpochID == "" {
		// A zero pinned context (defensive; should not occur when src is
		// set) is not revalidated against a concrete expected value.
		return nil
	}
	if reqCtx.ClusterID != expected.ClusterID || reqCtx.EpochID != expected.EpochID {
		return fmt.Errorf("lifecycle bootstrap admission: epoch changed at the boundary (pinned=%s/%s, current=%s/%s); page data is not authorized for the current epoch",
			expected.ClusterID, expected.EpochID, reqCtx.ClusterID, reqCtx.EpochID)
	}
	return nil
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
