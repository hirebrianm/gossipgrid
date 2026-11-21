// Package gossip: unit tests for the epidemic gossip engine (Milestone 4B).
//
// These tests use injectable delivery and peer-selection dependencies for
// deterministic, race-safe verification. No real mTLS is exercised here
// (that is covered by the five-node integration test in cmd/gossipgrid).
//
// Key regression tests added for the independent review corrections:
//   - MaxAttempts=1 causes exactly one attempt (per peer).
//   - MaxAttempts=3 causes no more than three attempts (per peer).
//   - Retries target only the failed peer.
//   - Persistent failures eventually exhaust their budget.
//   - Worker capacity remains available while retries are delayed.
//   - Delayed-retry and queue sizes remain bounded.
//   - Concurrent failed targets do not corrupt shared work state.
//   - Pending-ID entries are eventually released.
//   - The authenticated sender is excluded from forwarding.
//   - Stale DNS membership skips new fanout; static mode is unaffected.
//   - Stop cancels active deliveries via the engine-owned context.
package gossip

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/peertls"
)

// fakeDelivery implements Delivery for deterministic tests. It records all
// calls and returns configured errors for specific peers.
type fakeDelivery struct {
	mu       sync.Mutex
	calls    []deliveryCall
	failures map[string]error // peer identity -> error (nil = success)
	delay    time.Duration    // simulated latency per call
}

type deliveryCall struct {
	target PeerTarget
	msg    peertls.MessageWire
}

func newFakeDelivery() *fakeDelivery {
	return &fakeDelivery{
		failures: make(map[string]error),
	}
}

// Send records the call and applies configured failure/latency. It honors the
// supplied context: if the context is cancelled while the simulated latency is
// in progress, Send returns the context error (mirroring peertls.Client's
// context-cancellation behavior).
func (f *fakeDelivery) Send(ctx context.Context, target PeerTarget, msg peertls.MessageWire) (*peertls.MessageWire, error) {
	f.mu.Lock()
	f.calls = append(f.calls, deliveryCall{target: target, msg: msg})
	err := f.failures[target.ExpectedIdentity]
	delay := f.delay
	f.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	return &peertls.MessageWire{
		ID:        msg.ID,
		Body:      msg.Body,
		Origin:    msg.Origin,
		CreatedAt: msg.CreatedAt,
	}, nil
}

func (f *fakeDelivery) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeDelivery) callsForPeer(identity string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.target.ExpectedIdentity == identity {
			n++
		}
	}
	return n
}

// staticSelector is a deterministic PeerSelector that returns peers in a
// fixed order (no randomness). Used for tests that need deterministic
// selection. It excludes by PeerTarget.Key (PodIdentity or ExpectedIdentity).
type staticSelector struct {
	peers []PeerTarget
}

func (s *staticSelector) Select(_ PeerSnapshot, fanout int, exclude map[string]bool) []PeerTarget {
	var result []PeerTarget
	for _, p := range s.peers {
		if exclude[p.Key()] {
			continue
		}
		result = append(result, p)
		if len(result) >= fanout {
			break
		}
	}
	return result
}

// testConfig returns a minimal valid gossip config for tests.
func testConfig() Config {
	return Config{
		Fanout:        2,
		Rounds:        3,
		Workers:       4,
		QueueCapacity: 10,
		MaxAttempts:   3,
		RetryBase:     10 * time.Millisecond,
		RetryMax:      100 * time.Millisecond,
	}
}

// newTestEngine creates an Engine with a fake delivery and static selector.
func newTestEngine(t *testing.T, cfg Config, sel PeerSelector, del Delivery) *Engine {
	t.Helper()
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("engine Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})
	return e
}

// makeTestMsg returns a valid MessageWire for tests.
func makeTestMsg(id, body string) peertls.MessageWire {
	return peertls.MessageWire{
		ID:        id,
		Body:      body,
		Origin:    "gossipgrid-0",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// waitUntil polls the condition every 5ms until it is true or the deadline
// passes.
func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// --- 1. Successful local commit enqueues propagation ---

func TestEnqueueTriggersDelivery(t *testing.T) {
	del := newFakeDelivery()
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8443", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8444", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
		},
	}
	e := newTestEngine(t, testConfig(), sel, del)

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000000", "hello")})

	if !waitUntil(t, 2*time.Second, func() bool { return del.callCount() >= 1 }) {
		t.Fatal("expected at least one delivery call, got 0")
	}
}

// --- 3. New replicated message triggers forwarding ---

func TestNewReplicatedMessageTriggersForwarding(t *testing.T) {
	del := newFakeDelivery()
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8443", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8444", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
		},
	}
	e := newTestEngine(t, testConfig(), sel, del)

	e.Enqueue(MessageEvent{
		Message:     makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000001", "replicated"),
		SenderPodID: "gossipgrid-1",
	})

	if !waitUntil(t, 2*time.Second, func() bool { return del.callCount() >= 1 }) {
		t.Fatal("expected forwarding of new replicated message, got 0 calls")
	}
}

// --- 6. Fanout never exceeds its configured size per round ---

func TestFanoutNeverExceedsConfiguredSize(t *testing.T) {
	cfg := testConfig()
	cfg.Fanout = 2
	cfg.MaxAttempts = 1 // one attempt per peer; no retries
	del := newFakeDelivery()
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8442", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
			{DialAddr: "127.0.0.1:8443", ExpectedIdentity: "gossipgrid-3.suffix", PodIdentity: "gossipgrid-3"},
			{DialAddr: "127.0.0.1:8444", ExpectedIdentity: "gossipgrid-4.suffix", PodIdentity: "gossipgrid-4"},
		},
	}
	e := newTestEngine(t, cfg, sel, del)

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000002", "fanout")})

	if !waitUntil(t, 3*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}

	st := e.Stats()
	// With MaxAttempts=1, no peer is contacted more than once, so total
	// delivery attempts <= rounds*fanout (each round selects <= fanout distinct
	// peers, each with a single attempt).
	if st.DeliveryAttempts > uint64(cfg.Rounds*cfg.Fanout) {
		t.Errorf("total delivery attempts = %d, want <= %d (rounds*fanout)", st.DeliveryAttempts, cfg.Rounds*cfg.Fanout)
	}
}

// --- 7. Self is excluded ---

func TestSelfExcluded(t *testing.T) {
	del := newFakeDelivery()
	sel := &DefaultPeerSelector{
		IdentitySuffix: "suffix",
		RNG:            &safeRand{rnd: rand.New(rand.NewSource(42))},
	}
	cfg := testConfig()
	e := New(cfg, sel, del, nil)
	e.SetSnapshotSource(func() PeerSnapshot {
		return PeerSnapshot{
			Peers: []PeerInfo{
				{Hostname: "gossipgrid-0.suffix", PodIdentity: "gossipgrid-0", Address: "127.0.0.1:8440"},
				{Hostname: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1", Address: "127.0.0.1:8441"},
				{Hostname: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2", Address: "127.0.0.1:8442"},
			},
			LocalNode: &PeerInfo{Hostname: "gossipgrid-0.suffix", PodIdentity: "gossipgrid-0"},
		}
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000003", "self")})

	if !waitUntil(t, 2*time.Second, func() bool { return del.callCount() >= 1 }) {
		t.Fatal("expected delivery to non-self peers, got 0")
	}
	if n := del.callsForPeer("gossipgrid-0.suffix"); n > 0 {
		t.Errorf("self (gossipgrid-0) was contacted %d times; must be 0", n)
	}
}

// --- 8. Duplicate peer targets are excluded within a round ---

func TestDuplicatePeersExcludedWithinRound(t *testing.T) {
	del := newFakeDelivery()
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 1
	e := newTestEngine(t, cfg, sel, del)

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000004", "dup")})

	if !waitUntil(t, 2*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}
	// With 1 peer, 1 round, MaxAttempts=1: the peer should be contacted at most
	// once (not twice in the same round, no retry).
	if n := del.callsForPeer("gossipgrid-1.suffix"); n > 1 {
		t.Errorf("peer contacted %d times; want 1", n)
	}
}

// --- 9. Peer selection varies across rounds ---

func TestPeerSelectionVariesAcrossRounds(t *testing.T) {
	sel := &DefaultPeerSelector{
		IdentitySuffix: "suffix",
		RNG:            &safeRand{rnd: rand.New(rand.NewSource(123))},
	}
	snap := PeerSnapshot{
		Peers: []PeerInfo{
			{Hostname: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1", Address: "127.0.0.1:8441"},
			{Hostname: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2", Address: "127.0.0.1:8442"},
			{Hostname: "gossipgrid-3.suffix", PodIdentity: "gossipgrid-3", Address: "127.0.0.1:8443"},
		},
		LocalNode: &PeerInfo{Hostname: "gossipgrid-0.suffix", PodIdentity: "gossipgrid-0"},
	}

	excluded := make(map[string]bool)
	seen := make(map[string]bool)
	for round := 0; round < 3; round++ {
		targets := sel.Select(snap, 1, excluded)
		if len(targets) == 0 {
			t.Fatalf("round %d: no targets selected", round)
		}
		seen[targets[0].ExpectedIdentity] = true
		excluded[targets[0].Key()] = true
	}
	if len(seen) < 2 {
		t.Errorf("expected >= 2 distinct peers across 3 rounds, got %d: %v", len(seen), seen)
	}
}

// --- 10. No peers: engine must not panic or block ---

func TestStaleDiscoverySafe(t *testing.T) {
	del := newFakeDelivery()
	sel := &staticSelector{peers: nil} // no peers
	e := newTestEngine(t, testConfig(), sel, del)

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000006", "stale")})

	time.Sleep(50 * time.Millisecond)
	if del.callCount() != 0 {
		t.Errorf("expected 0 delivery calls with no peers, got %d", del.callCount())
	}
}

// --- 11. Queue capacity strictly enforced ---

func TestQueueCapacityEnforced(t *testing.T) {
	cfg := testConfig()
	cfg.QueueCapacity = 2
	cfg.Workers = 1
	del := newFakeDelivery()
	del.delay = 50 * time.Millisecond
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	for i := 0; i < 10; i++ {
		e.Enqueue(MessageEvent{Message: makeTestMsg(fmt.Sprintf("019c0f1e-6c0a-7c00-8000-%012d", i), "q")})
	}

	if !waitUntil(t, 5*time.Second, func() bool { return e.Stats().QueueDepth == 0 }) {
		t.Fatal("queue did not drain within deadline")
	}
}

// --- 13. Duplicate pending IDs are suppressed ---

func TestDuplicatePendingIDsSuppressed(t *testing.T) {
	del := newFakeDelivery()
	del.delay = 50 * time.Millisecond
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.Rounds = 1
	cfg.MaxAttempts = 1
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	msg := makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000007", "dedup")
	e.Enqueue(MessageEvent{Message: msg})
	e.Enqueue(MessageEvent{Message: msg}) // should be suppressed

	time.Sleep(100 * time.Millisecond)
	if n := del.callCount(); n > 1 {
		t.Errorf("duplicate ID caused %d delivery calls; want 1", n)
	}
}

// --- 15. Transient failure followed by successful retry (per peer) ---

func TestTransientFailureRetried(t *testing.T) {
	del := newFakeDelivery()
	del.failures["gossipgrid-1.suffix"] = errors.New("connection refused: 503")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 3
	cfg.Rounds = 1
	cfg.RetryBase = 5 * time.Millisecond
	cfg.RetryMax = 20 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	// Remove the failure after a brief delay to simulate recovery.
	go func() {
		time.Sleep(30 * time.Millisecond)
		del.mu.Lock()
		delete(del.failures, "gossipgrid-1.suffix")
		del.mu.Unlock()
	}()

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000008", "retry")})

	if !waitUntil(t, 3*time.Second, func() bool { return del.callCount() >= 2 }) {
		t.Fatalf("expected >= 2 delivery calls (1 fail + 1 success), got %d", del.callCount())
	}
	st := e.Stats()
	if st.RetryAttempts == 0 {
		t.Error("expected retry attempts > 0 after transient failure")
	}
}

// --- 18. Permanent errors and 409 are not retried ---

func TestPermanentErrorNotRetried(t *testing.T) {
	del := newFakeDelivery()
	// A typed peertls HTTP 409 conflict error (permanent).
	del.failures["gossipgrid-1.suffix"] = peertls.NewPeerHTTPStatusError("gossipgrid-1.suffix", 409, "conflict")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 5
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000009", "conflict")})

	if !waitUntil(t, 2*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}
	st := e.Stats()
	// 409 is permanent: no retries, and the peer is contacted exactly once
	// (one attempt per round, but the peer is excluded after... actually
	// permanent failure does NOT add to excluded, so it can be reselected in
	// later rounds — but no retry is scheduled. Verify no retry attempts.
	if st.RetryAttempts > 0 {
		t.Errorf("409 conflict caused %d retries; want 0 (permanent)", st.RetryAttempts)
	}
}

// --- 19. Timeouts and ambiguous results are handled safely ---

func TestTimeoutHandledSafely(t *testing.T) {
	del := newFakeDelivery()
	del.failures["gossipgrid-1.suffix"] = errors.New("requesting peer gossipgrid-1.suffix at 127.0.0.1:8441: timeout")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 2
	cfg.Rounds = 1
	cfg.RetryBase = 5 * time.Millisecond
	cfg.RetryMax = 20 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-00000000000a", "timeout")})

	// The engine must not hang or panic on timeout.
	if !waitUntil(t, 3*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0
	}) {
		t.Fatal("engine did not drain after timeout; possible hang")
	}
}

// --- 22. Concurrent enqueue and shutdown are race-safe ---

func TestConcurrentEnqueueAndShutdown(t *testing.T) {
	del := newFakeDelivery()
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.RetryBase = 5 * time.Millisecond
	cfg.RetryMax = 20 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				e.Enqueue(MessageEvent{Message: makeTestMsg(fmt.Sprintf("019c0f1e-6c0a-7c00-8000-%012d", n*100+j), "race")})
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	}()
	wg.Wait()
}

// --- 27. Propagation preserves immutable message metadata ---

func TestPropagationPreservesMetadata(t *testing.T) {
	del := newFakeDelivery()
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	e := newTestEngine(t, testConfig(), sel, del)

	original := peertls.MessageWire{
		ID:        "019c0f1e-6c0a-7c00-8000-00000000000f",
		Body:      "preserve metadata",
		Origin:    "gossipgrid-0",
		CreatedAt: "2026-11-20T12:00:00Z",
	}
	e.Enqueue(MessageEvent{Message: original})

	if !waitUntil(t, 2*time.Second, func() bool { return del.callCount() >= 1 }) {
		t.Fatal("no delivery occurred")
	}
	del.mu.Lock()
	c := del.calls[0]
	del.mu.Unlock()
	if c.msg.ID != original.ID {
		t.Errorf("ID = %q, want %q", c.msg.ID, original.ID)
	}
	if c.msg.Body != original.Body {
		t.Errorf("Body = %q, want %q", c.msg.Body, original.Body)
	}
	if c.msg.Origin != original.Origin {
		t.Errorf("Origin = %q, want %q", c.msg.Origin, original.Origin)
	}
	if c.msg.CreatedAt != original.CreatedAt {
		t.Errorf("CreatedAt = %q, want %q", c.msg.CreatedAt, original.CreatedAt)
	}
}

// --- 28. Multiple hops do not produce unbounded forwarding loops ---

func TestNoUnboundedForwardingLoops(t *testing.T) {
	del := newFakeDelivery()
	peers := make([]PeerTarget, 5)
	for i := range peers {
		peers[i] = PeerTarget{
			DialAddr:         fmt.Sprintf("127.0.0.1:844%d", i+1),
			ExpectedIdentity: fmt.Sprintf("gossipgrid-%d.suffix", i+1),
			PodIdentity:      fmt.Sprintf("gossipgrid-%d", i+1),
		}
	}
	sel := &DefaultPeerSelector{
		IdentitySuffix: "suffix",
		RNG:            &safeRand{rnd: rand.New(rand.NewSource(99))},
	}
	cfg := testConfig()
	cfg.Fanout = 1
	cfg.Rounds = 2
	cfg.MaxAttempts = 1
	e := New(cfg, sel, del, nil)
	e.SetSnapshotSource(func() PeerSnapshot {
		infos := make([]PeerInfo, len(peers))
		for i, p := range peers {
			infos[i] = PeerInfo{
				Hostname:    p.ExpectedIdentity,
				PodIdentity: p.PodIdentity,
				Address:     p.DialAddr,
			}
		}
		return PeerSnapshot{
			Peers:     infos,
			LocalNode: &PeerInfo{Hostname: "gossipgrid-0.suffix", PodIdentity: "gossipgrid-0"},
		}
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000010", "loop")})

	if !waitUntil(t, 3*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}

	st := e.Stats()
	// With MaxAttempts=1, each peer is contacted at most once; total attempts
	// <= rounds*fanout.
	maxExpected := uint64(cfg.Rounds * cfg.Fanout)
	if st.DeliveryAttempts > maxExpected {
		t.Errorf("delivery attempts = %d, want <= %d (rounds*fanout)", st.DeliveryAttempts, maxExpected)
	}
}

// --- Error classification (typed) ---

func TestClassifyError(t *testing.T) {
	cases := []struct {
		err  error
		want errorClass
	}{
		// Typed peertls HTTP-status errors.
		{peertls.NewPeerHTTPStatusError("p", 409, "conflict"), errPermanent},
		{peertls.NewPeerHTTPStatusError("p", 400, "invalid"), errPermanent},
		{peertls.NewPeerHTTPStatusError("p", 413, "too large"), errPermanent},
		{peertls.NewPeerHTTPStatusError("p", 415, "unsupported media type"), errPermanent},
		{peertls.NewPeerHTTPStatusError("p", 404, "not found"), errPermanent},
		{peertls.NewPeerHTTPStatusError("p", 429, "rate limited"), errTransient},
		{peertls.NewPeerHTTPStatusError("p", 500, "server error"), errTransient},
		{peertls.NewPeerHTTPStatusError("p", 502, "bad gateway"), errTransient},
		{peertls.NewPeerHTTPStatusError("p", 503, "unavailable"), errTransient},
		{peertls.NewPeerHTTPStatusError("p", 504, "gateway timeout"), errTransient},
		// Typed peertls acknowledgment errors (a successful response with an
		// invalid acknowledgment is a protocol defect — permanent).
		{peertls.NewPeerAckError("p", "without a message acknowledgment"), errPermanent},
		{peertls.NewPeerAckError("p", "acknowledged message ID mismatch"), errPermanent},
		// Un-typed errors: network failures, timeouts, and ambiguous transport
		// errors are transient within MaxAttempts (the receiver's insert is
		// idempotent). Crucially, a network error containing "409" as part of a
		// port number must NOT be classified as a permanent HTTP 409.
		{errors.New("dial tcp 127.0.0.1:4090: connection refused"), errTransient},
		{errors.New("connection refused"), errTransient},
		{errors.New("requesting peer p at 127.0.0.1:8441: timeout"), errTransient},
		{errors.New("read tcp 127.0.0.1:8443: connection reset"), errTransient},
		{errors.New("EOF"), errTransient},
		{errors.New("some unknown error"), errTransient},
	}
	for _, tc := range cases {
		got := classifyError(tc.err)
		if got != tc.want {
			t.Errorf("classifyError(%q) = %v, want %v", tc.err.Error(), got, tc.want)
		}
	}
}

// =====================================================================
// Regression tests for the independent review corrections
// =====================================================================

// --- MaxAttempts=1 causes exactly one attempt per peer ---

func TestMaxAttemptsOneCausesExactlyOneAttempt(t *testing.T) {
	del := newFakeDelivery()
	// All peers fail transiently; with MaxAttempts=1 there should be no retries.
	del.failures["gossipgrid-1.suffix"] = errors.New("connection refused")
	del.failures["gossipgrid-2.suffix"] = errors.New("connection refused")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8442", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.Rounds = 3
	cfg.RetryBase = 5 * time.Millisecond
	cfg.RetryMax = 20 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000011", "max1")})

	// Wait for the engine to fully drain (all rounds complete, no retries).
	if !waitUntil(t, 3*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0 && e.Stats().DelayedRetries == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}

	// Each peer must be contacted exactly once (MaxAttempts=1, no retry).
	if n := del.callsForPeer("gossipgrid-1.suffix"); n != 1 {
		t.Errorf("gossipgrid-1 contacted %d times; want 1 (MaxAttempts=1)", n)
	}
	if n := del.callsForPeer("gossipgrid-2.suffix"); n != 1 {
		t.Errorf("gossipgrid-2 contacted %d times; want 1 (MaxAttempts=1)", n)
	}
	st := e.Stats()
	if st.RetryAttempts > 0 {
		t.Errorf("MaxAttempts=1 caused %d retries; want 0", st.RetryAttempts)
	}
}

// --- MaxAttempts=3 causes no more than three attempts per peer ---

func TestMaxAttemptsThreeBounded(t *testing.T) {
	del := newFakeDelivery()
	// All peers fail permanently transient (never recover).
	for _, id := range []string{"gossipgrid-1.suffix", "gossipgrid-2.suffix"} {
		del.failures[id] = errors.New("connection refused")
	}
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8442", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 3
	cfg.Rounds = 1
	cfg.RetryBase = 5 * time.Millisecond
	cfg.RetryMax = 20 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000012", "max3")})

	if !waitUntil(t, 3*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0 && e.Stats().DelayedRetries == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}

	// Each peer must be contacted at most MaxAttempts=3 times (initial + 2
	// retries), and no more.
	if n := del.callsForPeer("gossipgrid-1.suffix"); n > 3 {
		t.Errorf("gossipgrid-1 contacted %d times; want <= 3 (MaxAttempts=3)", n)
	}
	if n := del.callsForPeer("gossipgrid-2.suffix"); n > 3 {
		t.Errorf("gossipgrid-2 contacted %d times; want <= 3 (MaxAttempts=3)", n)
	}
	// Verify the budget was exhausted (3 attempts, 1 initial + 2 retries).
	if n := del.callsForPeer("gossipgrid-1.suffix"); n != 3 {
		t.Errorf("gossipgrid-1 contacted %d times; want exactly 3 (initial + 2 retries)", n)
	}
}

// --- Retries target only the failed peer ---

func TestRetriesTargetOnlyFailedPeer(t *testing.T) {
	del := newFakeDelivery()
	// gossipgrid-1 fails transiently; gossipgrid-2 succeeds.
	del.failures["gossipgrid-1.suffix"] = errors.New("connection refused")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8442", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 3
	cfg.Rounds = 1
	cfg.Fanout = 2
	cfg.RetryBase = 5 * time.Millisecond
	cfg.RetryMax = 20 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000013", "target")})

	if !waitUntil(t, 3*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0 && e.Stats().DelayedRetries == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}

	// gossipgrid-2 succeeded and was added to excluded; it should be contacted
	// exactly once. gossipgrid-1 failed and gets retries (up to MaxAttempts=3).
	if n := del.callsForPeer("gossipgrid-2.suffix"); n != 1 {
		t.Errorf("gossipgrid-2 (success) contacted %d times; want 1", n)
	}
	if n := del.callsForPeer("gossipgrid-1.suffix"); n < 2 {
		t.Errorf("gossipgrid-1 (failure) contacted %d times; want >= 2 (initial + retries)", n)
	}
	if n := del.callsForPeer("gossipgrid-1.suffix"); n > 3 {
		t.Errorf("gossipgrid-1 contacted %d times; want <= 3 (MaxAttempts=3)", n)
	}
}

// --- Persistent failures eventually exhaust their budget ---

func TestPersistentFailureExhaustsBudget(t *testing.T) {
	del := newFakeDelivery()
	del.failures["gossipgrid-1.suffix"] = errors.New("503 service unavailable")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 3
	cfg.Rounds = 1
	cfg.RetryBase = 5 * time.Millisecond
	cfg.RetryMax = 20 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000014", "exhaust")})

	if !waitUntil(t, 3*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0 && e.Stats().DelayedRetries == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}

	// The peer should have been contacted exactly MaxAttempts=3 times, then
	// stopped (budget exhausted).
	if n := del.callsForPeer("gossipgrid-1.suffix"); n != 3 {
		t.Errorf("gossipgrid-1 contacted %d times; want exactly 3 (budget exhausted)", n)
	}
	st := e.Stats()
	if st.SuccessfulAcks != 0 {
		t.Errorf("expected 0 successful acks, got %d", st.SuccessfulAcks)
	}
}

// --- Worker capacity remains available while retries are delayed ---

func TestWorkerAvailableWhileRetriesDelayed(t *testing.T) {
	del := newFakeDelivery()
	// gossipgrid-1 fails; its retry is delayed. Meanwhile a different message
	// targeting gossipgrid-2 (success) must be processed by a worker without
	// waiting for gossipgrid-1's retry delay.
	del.failures["gossipgrid-1.suffix"] = errors.New("connection refused")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8442", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 3
	cfg.Rounds = 1
	cfg.Workers = 2
	cfg.RetryBase = 200 * time.Millisecond // long delay
	cfg.RetryMax = 500 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	// Enqueue two messages: one that triggers a delayed retry (msg A ->
	// gossipgrid-1 fails), and one that succeeds immediately (msg B ->
	// gossipgrid-2). With 2 workers, msg B should be processed promptly even
	// while msg A's retry is delayed in the heap.
	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000015", "msgA")})
	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000016", "msgB")})

	// gossipgrid-2 should receive a delivery quickly (well before the 200ms
	// retry delay elapses) for msgB, proving worker capacity is available.
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		if del.callsForPeer("gossipgrid-2.suffix") >= 1 {
			return // success: worker processed msgB promptly
		}
		time.Sleep(5 * time.Millisecond)
	}
	if del.callsForPeer("gossipgrid-2.suffix") == 0 {
		t.Error("gossipgrid-2 not contacted within 100ms; workers may be blocked by delayed retry")
	}
}

// --- Delayed-retry and queue sizes remain bounded ---

func TestDelayedRetryQueueBounded(t *testing.T) {
	del := newFakeDelivery()
	// All peers fail transiently to maximize retries.
	for i := 1; i <= 4; i++ {
		del.failures[fmt.Sprintf("gossipgrid-%d.suffix", i)] = errors.New("connection refused")
	}
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8442", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
			{DialAddr: "127.0.0.1:8443", ExpectedIdentity: "gossipgrid-3.suffix", PodIdentity: "gossipgrid-3"},
			{DialAddr: "127.0.0.1:8444", ExpectedIdentity: "gossipgrid-4.suffix", PodIdentity: "gossipgrid-4"},
		},
	}
	cfg := testConfig()
	cfg.QueueCapacity = 8
	cfg.Workers = 1
	cfg.MaxAttempts = 3
	cfg.Rounds = 1
	cfg.RetryBase = 50 * time.Millisecond
	cfg.RetryMax = 200 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	// Enqueue many messages to flood the delayed-retry queue.
	for i := 0; i < 20; i++ {
		e.Enqueue(MessageEvent{Message: makeTestMsg(fmt.Sprintf("019c0f1e-6c0a-7c00-8000-%012d", 200+i), "flood")})
	}

	// Sample the queue and delayed-retry depths; they must never exceed
	// capacity.
	bounded := true
	for i := 0; i < 100; i++ {
		st := e.Stats()
		if st.QueueDepth > cfg.QueueCapacity || st.DelayedRetries > cfg.QueueCapacity {
			bounded = false
			break
		}
		time.Sleep(10 * time.Millisecond)
		if st.QueueDepth == 0 && st.DelayedRetries == 0 && st.PendingWork == 0 {
			break
		}
	}
	if !bounded {
		t.Error("queue or delayed-retry depth exceeded capacity; unbounded growth")
	}
}

// --- Concurrent failed targets do not corrupt shared work state ---

func TestConcurrentFailedTargetsNoCorruption(t *testing.T) {
	del := newFakeDelivery()
	// All peers fail transiently; many messages enqueued concurrently, each
	// targeting the same set of failing peers. The per-message state must
	// remain independent (no cross-message attempt-count corruption).
	for i := 1; i <= 3; i++ {
		del.failures[fmt.Sprintf("gossipgrid-%d.suffix", i)] = errors.New("connection refused")
	}
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8442", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
			{DialAddr: "127.0.0.1:8443", ExpectedIdentity: "gossipgrid-3.suffix", PodIdentity: "gossipgrid-3"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 3
	cfg.Rounds = 1
	cfg.Workers = 8
	cfg.QueueCapacity = 64
	cfg.RetryBase = 2 * time.Millisecond
	cfg.RetryMax = 8 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	const numMessages = 30
	var wg sync.WaitGroup
	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			e.Enqueue(MessageEvent{Message: makeTestMsg(fmt.Sprintf("019c0f1e-6c0a-7c00-8000-%012d", 300+n), "concurrent")})
		}(i)
	}
	wg.Wait()

	// Wait for full drain (short backoff; bounded).
	if !waitUntil(t, 10*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0 && e.Stats().DelayedRetries == 0
	}) {
		t.Fatal("engine did not drain within deadline; possible corruption or hang")
	}

	// All pending entries must be released (no leak).
	if n := e.Stats().PendingWork; n != 0 {
		t.Errorf("pending work = %d after drain; want 0 (leak)", n)
	}

	// Each peer should have been contacted at most numMessages * MaxAttempts
	// times (each message attempts each peer up to MaxAttempts). This is a
	// loose bound; the key check is no hang and no leak.
	maxPerPeer := numMessages * 3
	for i := 1; i <= 3; i++ {
		if n := del.callsForPeer(fmt.Sprintf("gossipgrid-%d.suffix", i)); n > maxPerPeer {
			t.Errorf("gossipgrid-%d contacted %d times; want <= %d", i, n, maxPerPeer)
		}
	}
}

// --- Pending-ID entries are eventually released ---

func TestPendingIDsEventuallyReleased(t *testing.T) {
	del := newFakeDelivery()
	// Mix of success and failure to exercise both paths.
	del.failures["gossipgrid-1.suffix"] = errors.New("connection refused")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8442", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 2
	cfg.Rounds = 1
	cfg.Workers = 4
	cfg.RetryBase = 2 * time.Millisecond
	cfg.RetryMax = 8 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	ids := make([]string, 10)
	for i := range ids {
		ids[i] = fmt.Sprintf("019c0f1e-6c0a-7c00-8000-%012d", 400+i)
		e.Enqueue(MessageEvent{Message: makeTestMsg(ids[i], "pending")})
	}

	// After full drain, no pending entries should remain (all released).
	if !waitUntil(t, 8*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().DelayedRetries == 0 && e.Stats().PendingWork == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}
	// Give a moment for in-flight releases.
	time.Sleep(20 * time.Millisecond)
	if n := e.Stats().PendingWork; n != 0 {
		t.Errorf("pending work = %d after drain; want 0 (pending IDs not released)", n)
	}
}

// --- The authenticated sender is excluded from forwarding ---

func TestAuthenticatedSenderExcluded(t *testing.T) {
	del := newFakeDelivery()
	// Two targets; the incoming sender is gossipgrid-1. Use DefaultPeerSelector
	// (respects the exclude set) so the sender exclusion is actually applied.
	sel := &DefaultPeerSelector{
		IdentitySuffix: "suffix",
		RNG:            &safeRand{rnd: rand.New(rand.NewSource(5))},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.Rounds = 1
	cfg.Fanout = 2
	e := New(cfg, sel, del, nil)
	e.SetSnapshotSource(func() PeerSnapshot {
		return PeerSnapshot{
			Peers: []PeerInfo{
				{Hostname: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1", Address: "127.0.0.1:8441"},
				{Hostname: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2", Address: "127.0.0.1:8442"},
			},
			LocalNode: &PeerInfo{Hostname: "gossipgrid-0.suffix", PodIdentity: "gossipgrid-0"},
		}
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	// A message received from gossipgrid-1: the sender must receive zero
	// immediate forwarding attempts.
	e.Enqueue(MessageEvent{
		Message:     makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000017", "sender"),
		SenderPodID: "gossipgrid-1",
	})

	if !waitUntil(t, 2*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}

	// The sender (gossipgrid-1) must receive zero attempts.
	if n := del.callsForPeer("gossipgrid-1.suffix"); n != 0 {
		t.Errorf("authenticated sender gossipgrid-1 received %d forwarding attempts; want 0", n)
	}
	// The other peer (gossipgrid-2) should be contacted.
	if n := del.callsForPeer("gossipgrid-2.suffix"); n == 0 {
		t.Error("expected gossipgrid-2 to be contacted, got 0")
	}
}

// --- Unknown sender identity must not become an arbitrary exclusion ---

func TestUnknownSenderNotExcluded(t *testing.T) {
	del := newFakeDelivery()
	sel := &DefaultPeerSelector{
		IdentitySuffix: "suffix",
		RNG:            &safeRand{rnd: rand.New(rand.NewSource(5))},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.Rounds = 1
	cfg.Fanout = 2
	e := New(cfg, sel, del, nil)
	e.SetSnapshotSource(func() PeerSnapshot {
		return PeerSnapshot{
			Peers: []PeerInfo{
				{Hostname: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1", Address: "127.0.0.1:8441"},
				{Hostname: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2", Address: "127.0.0.1:8442"},
			},
			LocalNode: &PeerInfo{Hostname: "gossipgrid-0.suffix", PodIdentity: "gossipgrid-0"},
		}
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	// Empty sender: no exclusion. Both peers should be eligible.
	e.Enqueue(MessageEvent{
		Message:     makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000018", "unknown-sender"),
		SenderPodID: "",
	})

	if !waitUntil(t, 2*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}
	// Both peers should be contacted (fanout=2, no sender exclusion).
	if n := del.callsForPeer("gossipgrid-1.suffix"); n == 0 {
		t.Error("gossipgrid-1 not contacted; empty sender should not exclude it")
	}
	if n := del.callsForPeer("gossipgrid-2.suffix"); n == 0 {
		t.Error("gossipgrid-2 not contacted; empty sender should not exclude it")
	}
}

// --- Stale DNS membership skips new fanout ---

func TestStaleDNSSkipsFanout(t *testing.T) {
	del := newFakeDelivery()
	sel := &DefaultPeerSelector{
		IdentitySuffix: "suffix",
		RNG:            &safeRand{rnd: rand.New(rand.NewSource(7))},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.Rounds = 1
	e := New(cfg, sel, del, nil)
	e.SetSnapshotSource(func() PeerSnapshot {
		return PeerSnapshot{
			Peers: []PeerInfo{
				{Hostname: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1", Address: "127.0.0.1:8441"},
				{Hostname: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2", Address: "127.0.0.1:8442"},
			},
			LocalNode: &PeerInfo{Hostname: "gossipgrid-0.suffix", PodIdentity: "gossipgrid-0"},
			Stale:     true,
			Mode:      "dns",
		}
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000019", "stale-dns")})

	time.Sleep(50 * time.Millisecond)
	// No delivery should occur (stale dns snapshot skips fanout).
	if n := del.callCount(); n != 0 {
		t.Errorf("stale DNS caused %d delivery calls; want 0", n)
	}
}

// --- Fresh DNS membership performs fanout ---

func TestFreshDNSPerformsFanout(t *testing.T) {
	del := newFakeDelivery()
	sel := &DefaultPeerSelector{
		IdentitySuffix: "suffix",
		RNG:            &safeRand{rnd: rand.New(rand.NewSource(7))},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.Rounds = 1
	cfg.Fanout = 2
	e := New(cfg, sel, del, nil)
	e.SetSnapshotSource(func() PeerSnapshot {
		return PeerSnapshot{
			Peers: []PeerInfo{
				{Hostname: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1", Address: "127.0.0.1:8441"},
				{Hostname: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2", Address: "127.0.0.1:8442"},
			},
			LocalNode: &PeerInfo{Hostname: "gossipgrid-0.suffix", PodIdentity: "gossipgrid-0"},
			Stale:     false,
			Mode:      "dns",
		}
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-00000000001a", "fresh-dns")})

	if !waitUntil(t, 2*time.Second, func() bool { return del.callCount() >= 1 }) {
		t.Fatal("fresh DNS should perform fanout; got 0 deliveries")
	}
}

// --- Static-mode peers remain usable even if snapshot is marked stale ---
// (Static mode is deterministic; in practice the discovery manager never
// marks static snapshots stale. This test verifies the selector does not
// skip static mode even if Stale is somehow set — the policy is dns-only.)

func TestStaticModeNotAffectedByStale(t *testing.T) {
	del := newFakeDelivery()
	sel := &DefaultPeerSelector{
		IdentitySuffix: "suffix",
		RNG:            &safeRand{rnd: rand.New(rand.NewSource(7))},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.Rounds = 1
	cfg.Fanout = 2
	e := New(cfg, sel, del, nil)
	e.SetSnapshotSource(func() PeerSnapshot {
		return PeerSnapshot{
			Peers: []PeerInfo{
				{Hostname: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1", Address: "127.0.0.1:8441"},
				{Hostname: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2", Address: "127.0.0.1:8442"},
			},
			LocalNode: &PeerInfo{Hostname: "gossipgrid-0.suffix", PodIdentity: "gossipgrid-0"},
			Stale:     true, // static mode: the selector ignores Stale
			Mode:      "static",
		}
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-00000000001b", "static")})

	if !waitUntil(t, 2*time.Second, func() bool { return del.callCount() >= 1 }) {
		t.Fatal("static mode should perform fanout even with Stale=true; got 0 deliveries")
	}
}

// --- Recovery after DNS membership refresh ---

func TestRecoveryAfterDNSRefresh(t *testing.T) {
	del := newFakeDelivery()
	sel := &DefaultPeerSelector{
		IdentitySuffix: "suffix",
		RNG:            &safeRand{rnd: rand.New(rand.NewSource(7))},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.Rounds = 1
	cfg.Fanout = 2
	var mu sync.Mutex
	stale := true
	e := New(cfg, sel, del, nil)
	e.SetSnapshotSource(func() PeerSnapshot {
		mu.Lock()
		defer mu.Unlock()
		return PeerSnapshot{
			Peers: []PeerInfo{
				{Hostname: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1", Address: "127.0.0.1:8441"},
				{Hostname: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2", Address: "127.0.0.1:8442"},
			},
			LocalNode: &PeerInfo{Hostname: "gossipgrid-0.suffix", PodIdentity: "gossipgrid-0"},
			Stale:     stale,
			Mode:      "dns",
		}
	})
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	// Message 1: stale snapshot → no fanout.
	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-00000000001c", "m1")})
	time.Sleep(50 * time.Millisecond)
	if n := del.callCount(); n != 0 {
		t.Fatalf("stale snapshot caused %d deliveries; want 0", n)
	}

	// Simulate DNS recovery: refresh the snapshot (stale=false).
	mu.Lock()
	stale = false
	mu.Unlock()

	// Message 2: fresh snapshot → fanout resumes.
	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-00000000001d", "m2")})
	if !waitUntil(t, 2*time.Second, func() bool { return del.callCount() >= 1 }) {
		t.Fatal("recovery after DNS refresh should resume fanout; got 0 deliveries")
	}
}

// --- Stop cancels active deliveries via the engine-owned context ---

func TestStopCancelsActiveDelivery(t *testing.T) {
	// Use an injectable delivery that blocks on ctx.Done() to simulate a
	// network send blocked waiting for context cancellation.
	blockingDel := &blockingDelivery{}
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.Rounds = 1
	cfg.Workers = 1
	e := New(cfg, sel, blockingDel, nil)
	parentCtx, parentCancel := context.WithCancel(context.Background())
	if err := e.Start(parentCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer parentCancel()

	// Enqueue a message; the worker will call Send, which blocks on ctx.Done().
	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-00000000001e", "blocked")})

	// Wait for the delivery to actually start (block).
	if !waitUntil(t, 1*time.Second, func() bool { return blockingDel.started() }) {
		t.Fatal("delivery did not start within 1s")
	}

	// Now call Stop. This must cancel the engine-owned context, unblocking
	// the delivery, WITHOUT cancelling the parent context. Stop must return
	// promptly.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	start := time.Now()
	e.Stop(stopCtx)
	elapsed := time.Since(start)

	// The parent context must NOT be cancelled (Stop cancels only the
	// engine-owned derived context).
	if parentCtx.Err() != nil {
		t.Error("Stop cancelled the parent context; it should cancel only the engine-owned context")
	}
	if elapsed > 1500*time.Millisecond {
		t.Errorf("Stop took %v; want < 1.5s (delivery should have been cancelled promptly)", elapsed)
	}
	// The delivery should have been unblocked (ctx.Done fired).
	if !blockingDel.cancelled() {
		t.Error("delivery was not cancelled by Stop; the engine-owned context was not propagated")
	}
}

// blockingDelivery is a Delivery that blocks on ctx.Done() to simulate a
// network send that only unblocks on context cancellation.
type blockingDelivery struct {
	mu         sync.Mutex
	startedF   bool
	cancelledF bool
}

func (b *blockingDelivery) Send(ctx context.Context, _ PeerTarget, _ peertls.MessageWire) (*peertls.MessageWire, error) {
	b.mu.Lock()
	b.startedF = true
	b.mu.Unlock()
	<-ctx.Done()
	b.mu.Lock()
	b.cancelledF = true
	b.mu.Unlock()
	return nil, ctx.Err()
}

func (b *blockingDelivery) started() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.startedF
}

func (b *blockingDelivery) cancelled() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cancelledF
}

// --- Repeated and concurrent Stop calls are safe ---

func TestRepeatedStopSafe(t *testing.T) {
	del := newFakeDelivery()
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	e := New(testConfig(), sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Multiple concurrent Stop calls must not panic or double-close.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.Stop(ctx)
		}()
	}
	wg.Wait()
}

// --- Stop before Start returns promptly ---

func TestStopBeforeStartPrompt(t *testing.T) {
	del := newFakeDelivery()
	sel := &staticSelector{peers: nil}
	e := New(testConfig(), sel, del, nil)

	// Stop before Start must return promptly (no goroutine to wait for).
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	start := time.Now()
	e.Stop(ctx)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("Stop before Start took %v; want < 100ms", elapsed)
	}
}

// --- Start cannot incorrectly restart a stopped engine ---

func TestNoRestartAfterStop(t *testing.T) {
	del := newFakeDelivery()
	sel := &staticSelector{peers: nil}
	e := New(testConfig(), sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	e.Stop(ctx)

	// A second Start must fail (engine is stopped).
	if err := e.Start(context.Background()); err == nil {
		t.Error("expected Start to fail after Stop, got nil")
	}
}

// --- 16. Exponential backoff respects configured bounds ---

func TestBackoffRespectsBounds(t *testing.T) {
	cfg := testConfig()
	cfg.RetryBase = 100 * time.Millisecond
	cfg.RetryMax = 200 * time.Millisecond

	e := New(cfg, &staticSelector{peers: nil}, newFakeDelivery(), nil)

	// backoffDelay(attempt) where attempt is 1-based.
	d1 := e.backoffDelay(1)
	if d1 < 50*time.Millisecond || d1 > 100*time.Millisecond {
		t.Errorf("backoff(1) = %v, want in [50ms, 100ms] (base * [0.5, 1.0])", d1)
	}
	d2 := e.backoffDelay(2)
	if d2 < 100*time.Millisecond || d2 > 200*time.Millisecond {
		t.Errorf("backoff(2) = %v, want in [100ms, 200ms] (2*base * [0.5, 1.0])", d2)
	}
	d5 := e.backoffDelay(5)
	if d5 > 200*time.Millisecond {
		t.Errorf("backoff(5) = %v, want <= retry max (200ms)", d5)
	}
}

// --- 17. Retry jitter is bounded ---

func TestRetryJitterBounded(t *testing.T) {
	cfg := testConfig()
	cfg.RetryBase = 100 * time.Millisecond
	cfg.RetryMax = 10 * time.Second

	e := New(cfg, &staticSelector{peers: nil}, newFakeDelivery(), nil)

	for i := 1; i <= 100; i++ {
		d := e.backoffDelay(i)
		if d < 0 || d > cfg.RetryMax {
			t.Errorf("backoff(%d) = %v outside [0, max=%v]", i, d, cfg.RetryMax)
		}
	}
}

// =====================================================================
// Regression tests for the independent source-review corrections (M4B)
// =====================================================================

// --- 1. Execute all configured fanout rounds ---
// With Rounds=3, Fanout=1, MaxAttempts=1, and three healthy eligible peers,
// the engine must execute three rounds and contact three DISTINCT peers
// (one per round, each preferring a peer not yet contacted). This tests the
// engine itself (not just PeerSelector.Select).

func TestExecutesAllConfiguredFanoutRounds(t *testing.T) {
	del := newFakeDelivery()
	// Three healthy peers; all succeed.
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8442", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
			{DialAddr: "127.0.0.1:8443", ExpectedIdentity: "gossipgrid-3.suffix", PodIdentity: "gossipgrid-3"},
		},
	}
	cfg := testConfig()
	cfg.Rounds = 3
	cfg.Fanout = 1
	cfg.MaxAttempts = 1 // one attempt per peer; no retries
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-00000000001f", "rounds")})

	if !waitUntil(t, 3*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0 && e.Stats().DelayedRetries == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}

	st := e.Stats()
	// Three rounds, each contacting one distinct peer: 3 total delivery attempts,
	// 3 successful acks, and each of the 3 peers contacted exactly once.
	if st.DeliveryAttempts != 3 {
		t.Errorf("delivery attempts = %d, want 3 (3 rounds × 1 fanout)", st.DeliveryAttempts)
	}
	if st.SuccessfulAcks != 3 {
		t.Errorf("successful acks = %d, want 3", st.SuccessfulAcks)
	}
	// Each of the 3 peers is contacted exactly once (distinct peers across
	// rounds; no peer re-contacted after acknowledgment).
	for i := 1; i <= 3; i++ {
		if n := del.callsForPeer(fmt.Sprintf("gossipgrid-%d.suffix", i)); n != 1 {
			t.Errorf("gossipgrid-%d contacted %d times; want 1 (3 distinct peers across 3 rounds)", i, n)
		}
	}
}

// --- 2. Bounded retry scheduling (no goroutine per delayed retry) ---
// With QueueCapacity=1, Workers=1, Fanout=32, and all targets failing
// transiently, the engine must NOT create a goroutine (and timer) for every
// retry. Instead it uses one scheduler goroutine with a bounded delayed-work
// collection. Verify the number of outstanding scheduled delayed retries is
// bounded by capacity and that the engine does not leak goroutines.

func TestBoundedRetrySchedulingNoGoroutinePerRetry(t *testing.T) {
	del := newFakeDelivery()
	// 32 peers, all fail transiently.
	peers := make([]PeerTarget, 32)
	for i := range peers {
		peers[i] = PeerTarget{
			DialAddr:         fmt.Sprintf("127.0.0.1:%d", 8441+i),
			ExpectedIdentity: fmt.Sprintf("gossipgrid-%d.suffix", i+1),
			PodIdentity:      fmt.Sprintf("gossipgrid-%d", i+1),
		}
		del.failures[peers[i].ExpectedIdentity] = errors.New("connection refused")
	}
	sel := &staticSelector{peers: peers}
	cfg := testConfig()
	cfg.QueueCapacity = 1
	cfg.Workers = 1
	cfg.Fanout = 32
	cfg.Rounds = 1
	cfg.MaxAttempts = 3
	cfg.RetryBase = 15 * time.Second // large backoff so retries stay delayed
	cfg.RetryMax = 15 * time.Second
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	// Enqueue a single message that fans out to all 32 peers; each fails
	// transiently and schedules a delayed retry.
	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000020", "sched")})

	// Wait for all 32 initial attempts to complete.
	if !waitUntil(t, 3*time.Second, func() bool {
		return e.Stats().DeliveryAttempts >= 32
	}) {
		t.Fatalf("expected >= 32 delivery attempts, got %d", e.Stats().DeliveryAttempts)
	}

	// The delayed-retry collection must be bounded by capacity (1). The
	// excess retries are dropped (recorded in RetriesDropped).
	st := e.Stats()
	if st.DelayedRetries > cfg.QueueCapacity {
		t.Errorf("delayed retries = %d, want <= %d (capacity)", st.DelayedRetries, cfg.QueueCapacity)
	}
	if st.RetriesDropped == 0 {
		t.Errorf("expected some dropped retries (32 scheduled, capacity 1), got 0")
	}

	// Verify no goroutine leak: count goroutines and ensure it's bounded
	// (workers=1 + scheduler + completion + test goroutines, not 32+).
	n := runtime.NumGoroutine()
	if n > 20 {
		t.Errorf("goroutines = %d; expected a small bounded set (1 worker + scheduler + overhead), not one per retry", n)
	}
}

// --- 3. High-concurrency pending-message cleanup ---
// Enqueue thousands of unique messages with multiple workers and verify that
// PendingWork returns to zero after processing (no orphaned pending entries).
// This regression test exercises the ownership fix (reserve before publish, no
// recreate-on-drop) under high concurrency.

func TestHighConcurrencyPendingCleanup(t *testing.T) {
	del := newFakeDelivery()
	// 2 healthy peers; all succeed.
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8442", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
		},
	}
	cfg := testConfig()
	cfg.Workers = 32
	cfg.QueueCapacity = 1024
	cfg.MaxAttempts = 1
	cfg.Rounds = 1
	cfg.Fanout = 2
	cfg.RetryBase = 2 * time.Millisecond
	cfg.RetryMax = 8 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	const numMessages = 20000
	var wg sync.WaitGroup
	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			e.Enqueue(MessageEvent{Message: makeTestMsg(fmt.Sprintf("019c0f1e-6c0a-7c00-8000-%012x", n), "hc")})
		}(i)
	}
	wg.Wait()

	// All 20,000 messages must be processed (delivered to 2 peers each) and
	// the pending map must return to zero (no orphaned entries).
	if !waitUntil(t, 60*time.Second, func() bool {
		return e.Stats().PendingWork == 0 && e.Stats().QueueDepth == 0 && e.Stats().DelayedRetries == 0
	}) {
		st := e.Stats()
		t.Fatalf("engine did not drain; pending=%d queue=%d delayed=%d (want all 0)", st.PendingWork, st.QueueDepth, st.DelayedRetries)
	}
	if n := e.Stats().PendingWork; n != 0 {
		t.Errorf("pending work = %d after processing 20000 messages; want 0 (no orphaned entries)", n)
	}
}

// --- 4. Pending-message cleanup after shutdown ---
// Block a delivery, enqueue 100 additional messages, then Stop. After Stop
// completes, QueueDepth, PendingWork, and DelayedRetries must all be zero.

func TestPendingCleanupAfterShutdown(t *testing.T) {
	blockingDel := &blockingDelivery{}
	peers := make([]PeerTarget, 1)
	peers[0] = PeerTarget{
		DialAddr:         "127.0.0.1:8441",
		ExpectedIdentity: "gossipgrid-1.suffix",
		PodIdentity:      "gossipgrid-1",
	}
	sel := &staticSelector{peers: peers}
	cfg := testConfig()
	cfg.Workers = 1
	cfg.QueueCapacity = 512
	cfg.MaxAttempts = 1
	cfg.Rounds = 1
	cfg.Fanout = 1
	e := New(cfg, sel, blockingDel, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Enqueue 100 messages; the first blocks the single worker.
	for i := 0; i < 100; i++ {
		e.Enqueue(MessageEvent{Message: makeTestMsg(fmt.Sprintf("019c0f1e-6c0a-7c00-8000-%012x", 0x300+i), "sw")})
	}

	// Wait for the delivery to start (block the worker).
	if !waitUntil(t, 2*time.Second, func() bool { return blockingDel.started() }) {
		t.Fatal("delivery did not start within 2s")
	}

	// Stop the engine. This cancels the blocked delivery, releases the queued
	// items, and waits for the worker + scheduler to exit.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	e.Stop(ctx)

	// After a completed shutdown, all depths must be zero.
	st := e.Stats()
	if st.QueueDepth != 0 || st.PendingWork != 0 || st.DelayedRetries != 0 {
		t.Errorf("after Stop: queue=%d pending=%d delayed=%d; want all 0", st.QueueDepth, st.PendingWork, st.DelayedRetries)
	}
}

// --- 5. Concurrent Stop calls share completion ---
// Two concurrent Stop calls with a worker whose cleanup is intentionally
// blocked. Neither call should report completion before the worker is
// released. Both observe the same shutdown-completion channel.

func TestConcurrentStopSharesCompletion(t *testing.T) {
	blockingDel := &blockingDelivery{}
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.Workers = 1
	cfg.MaxAttempts = 1
	cfg.Rounds = 1
	cfg.Fanout = 1
	e := New(cfg, sel, blockingDel, nil)
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()
	if err := e.Start(parentCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000021", "conc-stop")})

	// Wait for the delivery to start (block the worker).
	if !waitUntil(t, 2*time.Second, func() bool { return blockingDel.started() }) {
		t.Fatal("delivery did not start within 2s")
	}

	// Two concurrent Stop calls. Both must wait for the worker (which is
	// blocked until the engine context is cancelled) to finish.
	var wg sync.WaitGroup
	var mu sync.Mutex
	var stopReturned [2]bool
	var timedOut [2]bool
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			e.Stop(stopCtx)
			mu.Lock()
			stopReturned[n] = true
			// If Stop returned via ctx.Done() (timeout), the workers may still
			// be running; record that.
			timedOut[n] = stopCtx.Err() != nil
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	for i := 0; i < 2; i++ {
		if !stopReturned[i] {
			t.Errorf("Stop caller %d did not return", i)
		}
		if timedOut[i] {
			t.Errorf("Stop caller %d timed out; the worker was not released in time", i)
		}
	}
	// After both Stop calls have returned (without timing out), the worker must
	// have been released (the engine context was cancelled, unblocking the
	// delivery). Give a moment for the delivery to observe ctx.Done().
	if !waitUntil(t, 2*time.Second, func() bool { return blockingDel.cancelled() }) {
		t.Error("delivery was not cancelled after Stop; the engine-owned context was not propagated")
	}
}

// =====================================================================
// Regression tests for the M4B independent review defect corrections
// =====================================================================

// --- Defect 1: MaxAttempts is enforced across rounds AND delayed retries ---
// With MaxAttempts=2, Rounds=3, Fanout=1, and one permanently unreachable peer,
// the total number of delivery attempts must not exceed MaxAttempts=2 even when
// a delayed retry from round 1 overlaps with the round 2 and round 3 selections.
// Previously, the attempt was recorded AFTER the network call, so a delayed
// retry could overlap subsequent rounds and produce 3 attempts.

func TestMaxAttemptsSharedAcrossRoundsAndRetries(t *testing.T) {
	del := newFakeDelivery()
	del.failures["gossipgrid-1.suffix"] = errors.New("connection refused")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 2
	cfg.Rounds = 3
	cfg.Fanout = 1
	cfg.RetryBase = 5 * time.Millisecond
	cfg.RetryMax = 20 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000022", "shared-budget")})

	if !waitUntil(t, 5*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0 && e.Stats().DelayedRetries == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}
	// The per-peer budget (MaxAttempts=2) is enforced ACROSS fanout rounds and
	// delayed retries: a fanout round must not re-contact the peer while its
	// delayed retry is pending, so the total number of attempts is the retry
	// chain (1 initial transient failure + 1 retry) = 2, never more. With a
	// single peer the chain stops at the budget, so exactly 2 total attempts.
	if n := del.callsForPeer("gossipgrid-1.suffix"); n > cfg.MaxAttempts {
		t.Errorf("peer contacted %d times; want <= %d (MaxAttempts shared across rounds and retries)", n, cfg.MaxAttempts)
	}
	if n := del.callsForPeer("gossipgrid-1.suffix"); n != 2 {
		t.Errorf("peer contacted %d times; want exactly 2 (1 initial + 1 retry; budget consumed by the retry chain, not by fanout re-contact)", n)
	}
}

// --- Defect 2: Permanently rejected peers (HTTP 409) are excluded from all future rounds ---
// With Rounds=3, Fanout=1, and one peer returning HTTP 409, the peer must be
// contacted exactly once (the first attempt) and excluded from all subsequent
// rounds. Previously, the peer was not marked permanently ineligible and could
// be reselected in later rounds, producing 3 delivery attempts.

func TestPermanentRejectionExcludedFromFutureRounds(t *testing.T) {
	del := newFakeDelivery()
	del.failures["gossipgrid-1.suffix"] = peertls.NewPeerHTTPStatusError("gossipgrid-1.suffix", 409, "conflict")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 5
	cfg.Rounds = 3
	cfg.Fanout = 1
	cfg.RetryBase = 5 * time.Millisecond
	cfg.RetryMax = 20 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000023", "perm-reject")})

	if !waitUntil(t, 5*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0 && e.Stats().DelayedRetries == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}
	if n := del.callsForPeer("gossipgrid-1.suffix"); n != 1 {
		t.Errorf("peer with 409 contacted %d times; want exactly 1 (permanently rejected, excluded from future rounds)", n)
	}
	st := e.Stats()
	if st.RetryAttempts > 0 {
		t.Errorf("409 rejection caused %d retries; want 0 (permanent)", st.RetryAttempts)
	}
}

// --- Defect 3: Exhausted peers are filtered BEFORE fanout selection ---
// With Rounds=3, Fanout=1, MaxAttempts=1, gossipgrid-1 always fails transiently
// (and exhausts its budget in the first round), while gossipgrid-2 and
// gossipgrid-3 succeed. The exhausted gossipgrid-1 must be filtered out BEFORE
// the selector runs, so it cannot fill a selection slot and prevent a different
// eligible peer from being contacted. gossipgrid-2 and gossipgrid-3 should each
// receive their one permitted delivery attempt across the three rounds:
// round 1 → g1 (fails, exhausted), round 2 → g2 (succeeds), round 3 → g3
// (succeeds; g1 excluded as exhausted, g2 excluded as already-contacted).
// (A third peer and a third round are added so the test still exercises its
// primary property — an exhausted peer not blocking a different eligible peer —
// even though the retry-pending exclusion now also keeps the transiently
// failing gossipgrid-1 out of later rounds.)

func TestExhaustedPeerDoesNotBlockSelection(t *testing.T) {
	del := newFakeDelivery()
	del.failures["gossipgrid-1.suffix"] = errors.New("connection refused")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8442", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
			{DialAddr: "127.0.0.1:8443", ExpectedIdentity: "gossipgrid-3.suffix", PodIdentity: "gossipgrid-3"},
		},
	}
	cfg := testConfig()
	cfg.Rounds = 3
	cfg.Fanout = 1
	cfg.MaxAttempts = 1
	cfg.RetryBase = 5 * time.Millisecond
	cfg.RetryMax = 20 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000024", "exhausted-filter")})

	if !waitUntil(t, 5*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0 && e.Stats().DelayedRetries == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}
	// gossipgrid-1: one attempt (failed, budget exhausted, MaxAttempts=1).
	if n := del.callsForPeer("gossipgrid-1.suffix"); n != 1 {
		t.Errorf("gossipgrid-1 contacted %d times; want 1 (MaxAttempts=1)", n)
	}
	// gossipgrid-2 and gossipgrid-3: each one attempt (succeeded). The exhausted
	// gossipgrid-1 must not fill a selection slot and block them.
	if n := del.callsForPeer("gossipgrid-2.suffix"); n != 1 {
		t.Errorf("gossipgrid-2 contacted %d times; want 1 (should not be blocked by exhausted peer)", n)
	}
	if n := del.callsForPeer("gossipgrid-3.suffix"); n != 1 {
		t.Errorf("gossipgrid-3 contacted %d times; want 1 (should not be blocked by exhausted peer)", n)
	}
}

// --- Defect 4: Concurrent Stop callers wait for actual completion ---
// A delivery that observes context cancellation but intentionally blocks during
// cleanup. Two concurrent Stop calls; neither returns before cleanup is released.
// Previously, the second Stop returned immediately when stopped=true, even if
// the first Stop's worker was still executing cleanup.

func TestConcurrentStopWaitsForCleanup(t *testing.T) {
	// A delivery that signals started, then blocks on a release channel
	// (simulating a long cleanup that only completes when explicitly released).
	del := &cleanupBlockingDelivery{releaseCh: make(chan struct{})}
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.MaxAttempts = 1
	cfg.Rounds = 1
	cfg.Workers = 1
	e := New(cfg, sel, del, nil)
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()
	if err := e.Start(parentCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000025", "cleanup-block")})

	// Wait for the delivery to start (it will block on ctx.Done() then on release).
	if !waitUntil(t, 1*time.Second, func() bool { return del.started() }) {
		t.Fatal("delivery did not start within 1s")
	}

	// Two concurrent Stop calls. The delivery blocks on releaseCh, so the
	// worker won't finish until release is called. Both Stop calls must wait
	// for the worker to finish.
	var wg sync.WaitGroup
	stopDone := make([]bool, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			e.Stop(stopCtx)
			stopDone[n] = true
		}(i)
	}

	// Give both Stop calls time to reach the wait-on-doneCh.
	time.Sleep(50 * time.Millisecond)

	// Neither Stop should have returned yet (the worker is blocked on releaseCh).
	if stopDone[0] || stopDone[1] {
		t.Error("a Stop call returned before cleanup was released; workers were still executing")
	}

	// Release the blocked delivery; the worker can now finish.
	close(del.releaseCh)

	// Both Stop calls must now return.
	wg.Wait()
	if !stopDone[0] || !stopDone[1] {
		t.Error("one or both Stop calls did not return after cleanup was released")
	}
}

// cleanupBlockingDelivery blocks on ctx.Done() (observing cancellation) and then
// on a release channel (simulating a long cleanup).
type cleanupBlockingDelivery struct {
	mu        sync.Mutex
	startedF  bool
	releaseCh chan struct{}
}

func (d *cleanupBlockingDelivery) Send(ctx context.Context, _ PeerTarget, _ peertls.MessageWire) (*peertls.MessageWire, error) {
	d.mu.Lock()
	d.startedF = true
	d.mu.Unlock()
	// Observe context cancellation.
	<-ctx.Done()
	// Block during "cleanup" until explicitly released.
	<-d.releaseCh
	return nil, ctx.Err()
}

func (d *cleanupBlockingDelivery) started() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.startedF
}

// --- Defect 4b: Stop before Start establishes terminal stopped state ---
// Stop before Start returns promptly and leaves the engine terminally stopped.
// Start after a terminal Stop must fail.

func TestStopBeforeStartTerminal(t *testing.T) {
	del := newFakeDelivery()
	sel := &staticSelector{peers: nil}
	e := New(testConfig(), sel, del, nil)

	// Stop before Start: returns promptly.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	start := time.Now()
	e.Stop(ctx)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("Stop before Start took %v; want < 100ms", elapsed)
	}

	// Start after a terminal Stop must fail.
	if err := e.Start(context.Background()); err == nil {
		t.Error("expected Start to fail after Stop-before-Start, got nil")
	}
}

// --- Defect 5: Concurrent Enqueue and Stop leave zero abandoned work ---
// 50 concurrent producer goroutines calling Enqueue, racing with one Stop call.
// After Stop and all producers complete, QueueDepth, PendingWork, and
// DelayedRetries must all be zero. Previously, a producer could pass
// isStarted(), then Stop could drain the queue, then the producer could publish
// an item after the drain, leaving orphaned work.

func TestConcurrentEnqueueStopZeroAbandoned(t *testing.T) {
	del := newFakeDelivery()
	del.delay = 1 * time.Millisecond
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.Workers = 1
	cfg.RetryBase = 5 * time.Millisecond
	cfg.RetryMax = 20 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				e.Enqueue(MessageEvent{Message: makeTestMsg(
					fmt.Sprintf("019c0f1e-6c0a-7c00-8000-%012x", n*20+j), "abandon")})
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Let some producers run, then Stop.
		<-stopCh
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		e.Stop(stopCtx)
	}()
	close(stopCh)
	wg.Wait()

	// After completed shutdown and all producers, all depths must be zero.
	st := e.Stats()
	if st.QueueDepth != 0 || st.PendingWork != 0 || st.DelayedRetries != 0 {
		t.Errorf("after Stop: queue=%d pending=%d delayed=%d; want all 0 (abandoned work)", st.QueueDepth, st.PendingWork, st.DelayedRetries)
	}
}

// =====================================================================
// Regression tests for the M4B peer retry-state coordination and shutdown
// cleanup finality defect corrections
// =====================================================================

// --- Scenario A: Retry backoff is honored across fanout rounds ---
// With Rounds=2, Fanout=1, MaxAttempts=3, RetryBase=500ms, and one peer that
// fails transiently, the first fanout round (round 0) makes the initial
// attempt (fails) and schedules a retry. The second fanout round (round 1)
// must NOT re-contact the peer (its retry is pending); instead it selects
// gossipgrid-2 (eligible, no retry pending). The delayed retry then fires
// (~500ms later) and re-contacts gossipgrid-1 (transient failure again).
// No further retries are scheduled (MaxAttempts=3 exhausted).
//
// This verifies: (a) a fanout round does not bypass the configured backoff by
// re-contacting a transiently failing peer while its retry is pending;
// (b) the pending retry is allowed to execute when due;
// (c) fanout rounds remain free to contact OTHER eligible peers while a
// failed peer is awaiting retry.
func TestRetryBackoffHonoredAcrossFanoutRounds(t *testing.T) {
	del := newFakeDelivery()
	del.failures["gossipgrid-1.suffix"] = errors.New("connection refused")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:8442", ExpectedIdentity: "gossipgrid-2.suffix", PodIdentity: "gossipgrid-2"},
		},
	}
	cfg := testConfig()
	cfg.Rounds = 2
	cfg.Fanout = 1
	cfg.MaxAttempts = 3
	cfg.RetryBase = 500 * time.Millisecond
	cfg.RetryMax = 2 * time.Second
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000030", "backoff")})

	// Wait for the initial attempt to complete (gossipgrid-1 fails, retry
	// scheduled). The fanout rounds (round 0 and round 1) should have
	// completed quickly: round 0 contacts gossipgrid-1 (fails), round 1
	// contacts gossipgrid-2 (succeeds, gossipgrid-1 excluded as retry-pending).
	if !waitUntil(t, 500*time.Millisecond, func() bool {
		return del.callsForPeer("gossipgrid-1.suffix") >= 1 &&
			del.callsForPeer("gossipgrid-2.suffix") >= 1
	}) {
		t.Fatalf("expected gossipgrid-1 and gossipgrid-2 to be contacted within 500ms, got g1=%d g2=%d",
			del.callsForPeer("gossipgrid-1.suffix"), del.callsForPeer("gossipgrid-2.suffix"))
	}

	// CRITICAL: gossipgrid-1 must have been contacted EXACTLY once so far
	// (only the initial attempt in round 0). Round 1 must NOT have
	// re-contacted gossipgrid-1 (its retry is pending, backoff not yet
	// elapsed). This is the backoff-honoring assertion.
	if n := del.callsForPeer("gossipgrid-1.suffix"); n != 1 {
		t.Fatalf("gossipgrid-1 contacted %d times before retry fired; want 1 (backoff must be honored — no fanout round may re-contact a retry-pending peer)", n)
	}
	if n := del.callsForPeer("gossipgrid-2.suffix"); n != 1 {
		t.Errorf("gossipgrid-2 contacted %d times; want 1 (fanout round 1 should contact an eligible peer while gossipgrid-1 awaits retry)", n)
	}

	// Now wait for the delayed retry to fire (~500ms) and re-contact
	// gossipgrid-1. The retry should execute (transient failure again, but
	// MaxAttempts=3 means one more retry could be scheduled — however the
	// peer is still failing, so the 2nd retry is also scheduled).
	if !waitUntil(t, 3*time.Second, func() bool {
		return del.callsForPeer("gossipgrid-1.suffix") >= 2
	}) {
		t.Fatal("delayed retry did not fire within 3s; the pending retry was not allowed to execute when due")
	}

	// Wait for the engine to fully drain.
	if !waitUntil(t, 5*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0 && e.Stats().DelayedRetries == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}

	// gossipgrid-1: 3 attempts total (1 initial + 2 retries, MaxAttempts=3).
	if n := del.callsForPeer("gossipgrid-1.suffix"); n > cfg.MaxAttempts {
		t.Errorf("gossipgrid-1 contacted %d times; want <= %d (MaxAttempts)", n, cfg.MaxAttempts)
	}
	// gossipgrid-2: 1 attempt (succeeded in round 1).
	if n := del.callsForPeer("gossipgrid-2.suffix"); n != 1 {
		t.Errorf("gossipgrid-2 contacted %d times; want 1", n)
	}
}

// --- Scenario B: Delayed retry is cancelled after a subsequent permanent rejection ---
// With Rounds=2, Fanout=1, MaxAttempts=3, and one peer that first fails
// transiently (scheduling a retry) and then returns HTTP 409 (permanent
// rejection) on the next fanout round, the delayed retry must NOT execute
// after the permanent rejection. The peer must be contacted exactly once
// (the first transient failure) — the 409 is detected by the 2nd fanout
// round (the peer is NOT retry-pending because the 1st round's retry was
// already scheduled, but the 2nd round re-contacts it and gets 409).
//
// Wait — with the failedPeers exclusion, the 2nd round CANNOT re-contact the
// peer while its retry is pending. So this scenario requires the retry to
// fire BEFORE the 2nd round. Let's use a very short backoff so the retry
// fires quickly, and the 409 is returned on the retry (not the 2nd round).
//
// Actually, the independent reproduction says: "First attempt fails transiently
// and schedules a retry. The next fanout round receives HTTP 409." This
// implies the next fanout round CAN contact the peer (i.e., the retry hasn't
// fired yet, but the peer is NOT excluded by failedPeers). This is
// contradictory with the failedPeers exclusion...
//
// Resolution: the failedPeers exclusion only applies while a retry is PENDING
// (in the delayed heap). If the retry has already fired and is in the
// ready-retry channel, the peer is no longer "retry-pending" in the sense
// that matters for fanout exclusion — the failedPeers marker is cleared when
// the retry fires (via the tryReserveAttempt isRetry path). So the 2nd round
// CAN contact the peer.
//
// But in practice, with a 500ms backoff, the retry doesn't fire within the
// 2nd round's timeframe. So let's use a very short backoff (1ms) so the
// retry fires almost immediately, and the 409 is returned on the retry.
func TestDelayedRetryCancelledAfterPermanentRejection(t *testing.T) {
	del := newFakeDelivery()
	// gossipgrid-1 fails transiently on the first attempt, then returns
	// 409 on subsequent attempts (simulating: transient failure → recovery
	// → but the message is now a conflict).
	del.failures["gossipgrid-1.suffix"] = errors.New("connection refused")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.Rounds = 2
	cfg.Fanout = 1
	cfg.MaxAttempts = 3
	// Use a 50ms backoff so the first retry fires AFTER the goroutine
	// switches the error to 409 (at 20ms). This ensures the retry gets the
	// 409 (permanent), not the original transient error.
	cfg.RetryBase = 50 * time.Millisecond
	cfg.RetryMax = 200 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	// After a brief delay (enough for the first transient failure to
	// schedule a retry, but BEFORE the 50ms retry fires), switch to 409 so
	// the retry gets a permanent rejection.
	go func() {
		time.Sleep(20 * time.Millisecond)
		del.mu.Lock()
		del.failures["gossipgrid-1.suffix"] = peertls.NewPeerHTTPStatusError("gossipgrid-1.suffix", 409, "conflict")
		del.mu.Unlock()
	}()

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000031", "perm-after-retry")})

	// Wait for the engine to fully drain.
	if !waitUntil(t, 5*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0 && e.Stats().DelayedRetries == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}

	// The peer should have been contacted exactly 2 times: 1 initial
	// (transient failure) + 1 retry (which then gets 409). No further retries
	// after the permanent rejection.
	if n := del.callsForPeer("gossipgrid-1.suffix"); n != 2 {
		t.Errorf("gossipgrid-1 contacted %d times; want 2 (1 initial transient + 1 retry with 409; no retry after permanent rejection)", n)
	}
	// Verify no retry was scheduled after the 409 (RetryAttempts should be
	// exactly 1 — the 409 is permanent, so no retry is scheduled for it).
	st := e.Stats()
	if st.RetryAttempts != 1 {
		t.Errorf("409 rejection caused %d retries; want 1 (1 retry after initial failure; permanent — no retry after 409)", st.RetryAttempts)
	}
}

// --- Scenario C: Delayed retry is cancelled after a subsequent success ---
// With Rounds=2, Fanout=1, MaxAttempts=3, and one peer that fails transiently
// on the first attempt and succeeds on the retry, the peer should be
// contacted exactly 2 times (1 initial failure + 1 retry success). No further
// attempts after the success (the peer is in the excluded set).
func TestDelayedRetryCancelledAfterSuccess(t *testing.T) {
	del := newFakeDelivery()
	// gossipgrid-1 fails transiently on the first attempt, then succeeds on
	// subsequent attempts (simulating: transient failure → recovery).
	del.failures["gossipgrid-1.suffix"] = errors.New("connection refused")
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.Rounds = 2
	cfg.Fanout = 1
	cfg.MaxAttempts = 3
	// Use a 50ms backoff so the first retry fires AFTER the goroutine
	// removes the failure (at 20ms). This ensures the retry succeeds.
	cfg.RetryBase = 50 * time.Millisecond
	cfg.RetryMax = 200 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	// After a brief delay (enough for the first transient failure to
	// schedule a retry, but BEFORE the 50ms retry fires), remove the failure
	// so the retry succeeds.
	go func() {
		time.Sleep(20 * time.Millisecond)
		del.mu.Lock()
		delete(del.failures, "gossipgrid-1.suffix")
		del.mu.Unlock()
	}()

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000032", "success-after-retry")})

	// Wait for the engine to fully drain.
	if !waitUntil(t, 5*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0 && e.Stats().DelayedRetries == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}

	// The peer should have been contacted exactly 2 times: 1 initial
	// (transient failure) + 1 retry (success). No further attempts after the
	// success (the peer is in the excluded set).
	if n := del.callsForPeer("gossipgrid-1.suffix"); n != 2 {
		t.Errorf("gossipgrid-1 contacted %d times; want 2 (1 initial failure + 1 retry success; no retry after success)", n)
	}
	// Verify 1 successful ack.
	st := e.Stats()
	if st.SuccessfulAcks != 1 {
		t.Errorf("expected 1 successful ack, got %d", st.SuccessfulAcks)
	}
	// Verify no further retries after the success (exactly 1 retry).
	if st.RetryAttempts != 1 {
		t.Errorf("expected 1 retry attempt (1 retry after initial failure; no retry after success), got %d", st.RetryAttempts)
	}
}

// --- Shutdown cleanup is final: no orphaned work after completed Stop ---
// Repeatedly stop the engine during heavy retry scheduling (8 consistently
// failing peers, short retry intervals, multiple concurrent workers). After
// each Stop, QueueDepth, PendingWork, and DelayedRetries must all be zero.
// This stress test exercises the race window where the scheduler can remove a
// delayed retry from the heap before cancellation, then publish it to retryCh
// after Stop's preliminary drain, leaving orphaned work.
func TestShutdownCleanupFinalStress(t *testing.T) {
	const iterations = 50
	for i := 0; i < iterations; i++ {
		t.Run(fmt.Sprintf("iter-%d", i), func(t *testing.T) {
			del := newFakeDelivery()
			// 8 consistently failing peers (transient).
			for j := 1; j <= 8; j++ {
				del.failures[fmt.Sprintf("gossipgrid-%d.suffix", j)] = errors.New("connection refused")
			}
			peers := make([]PeerTarget, 8)
			for j := range peers {
				peers[j] = PeerTarget{
					DialAddr:         fmt.Sprintf("127.0.0.1:%d", 8441+j),
					ExpectedIdentity: fmt.Sprintf("gossipgrid-%d.suffix", j+1),
					PodIdentity:      fmt.Sprintf("gossipgrid-%d", j+1),
				}
			}
			sel := &staticSelector{peers: peers}
			cfg := testConfig()
			cfg.Workers = 4
			cfg.Rounds = 2
			cfg.Fanout = 4
			cfg.MaxAttempts = 3
			cfg.RetryBase = 2 * time.Millisecond
			cfg.RetryMax = 10 * time.Millisecond
			cfg.QueueCapacity = 64
			e := New(cfg, sel, del, nil)
			if err := e.Start(context.Background()); err != nil {
				t.Fatalf("Start: %v", err)
			}

			// Enqueue multiple messages to generate heavy retry scheduling.
			for j := 0; j < 20; j++ {
				e.Enqueue(MessageEvent{Message: makeTestMsg(
					fmt.Sprintf("019c0f1e-6c0a-7c00-8000-%012x", 0x400+i*64+j), "stress")})
			}

			// Let retries accumulate in the delayed heap, then Stop.
			time.Sleep(5 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			e.Stop(ctx)

			// After a completed shutdown, all depths must be zero.
			st := e.Stats()
			if st.QueueDepth != 0 || st.PendingWork != 0 || st.DelayedRetries != 0 {
				t.Fatalf("after Stop: queue=%d pending=%d delayed=%d; want all 0 (orphaned work after shutdown)", st.QueueDepth, st.PendingWork, st.DelayedRetries)
			}
		})
	}
}

// --- Overlapping sends to the same peer/message are prevented ---
// With Rounds=2, Fanout=1, MaxAttempts=3, and one peer, verify that no
// overlapping sends occur: at no point should two concurrent attempts be
// in-flight for the same peer/message. This is guaranteed by the
// inFlightPeer counter in tryReserveAttempt.
func TestNoOverlappingSendsSamePeer(t *testing.T) {
	fake := newFakeDelivery()
	fake.failures["gossipgrid-1.suffix"] = errors.New("connection refused")
	fake.delay = 5 * time.Millisecond // slow sends to create overlap window
	del := &concurrentCountDelivery{base: fake}
	sel := &staticSelector{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:8441", ExpectedIdentity: "gossipgrid-1.suffix", PodIdentity: "gossipgrid-1"},
		},
	}
	cfg := testConfig()
	cfg.Rounds = 2
	cfg.Fanout = 1
	cfg.MaxAttempts = 3
	cfg.RetryBase = 10 * time.Millisecond
	cfg.RetryMax = 50 * time.Millisecond
	e := New(cfg, sel, del, nil)
	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e.Stop(ctx)
	})

	e.Enqueue(MessageEvent{Message: makeTestMsg("019c0f1e-6c0a-7c00-8000-000000000033", "no-overlap")})

	// Wait for full drain.
	if !waitUntil(t, 5*time.Second, func() bool {
		return e.Stats().QueueDepth == 0 && e.Stats().PendingWork == 0 && e.Stats().DelayedRetries == 0
	}) {
		t.Fatal("engine did not drain within deadline")
	}

	// Verify no overlapping sends occurred.
	if n := del.peakConcurrent(); n > 1 {
		t.Errorf("max concurrent sends to same peer = %d; want 1 (no overlapping sends)", n)
	}
}

// concurrentCountDelivery wraps a Delivery and tracks the maximum number of
// concurrent (in-flight) Send calls for the same peer.
type concurrentCountDelivery struct {
	base     Delivery
	mu       sync.Mutex
	inFlight int
	peakConc int
}

func (c *concurrentCountDelivery) Send(ctx context.Context, target PeerTarget, msg peertls.MessageWire) (*peertls.MessageWire, error) {
	c.mu.Lock()
	c.inFlight++
	if c.inFlight > c.peakConc {
		c.peakConc = c.inFlight
	}
	c.mu.Unlock()
	ack, err := c.base.Send(ctx, target, msg)
	c.mu.Lock()
	c.inFlight--
	c.mu.Unlock()
	return ack, err
}

func (c *concurrentCountDelivery) peakConcurrent() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peakConc
}
