package antientropy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/peertls"
)

// This file verifies the anti-entropy commit guard against the REAL
// lifecycle controller (lifecycle.Controller) and the REAL production
// admission check (Admission.CheckReplicationOutbound), rather than a fake
// guard. It exercises the actual Controller.mu locking path:
// Engine.commitCursor → Controller.CommitUnderAuthorization (one c.mu RLock,
// a single non-recursive authorization snapshot) → the in-memory cursor
// callback. A revocation that completes before the guard enters is observed
// and the callback is not executed; a revocation that follows an already
// linearized commit is a normal, permitted revocation.
//
// The tests use controlled barriers (blocking the last insert of a page) so
// the test can change the controller's state deterministically in the window
// between the last pre-insert admission check and the cursor commit, and
// bounded completion checks (select with a timeout) rather than arbitrary
// scheduling delays.

// realBindingStore is a concurrency-safe in-memory implementation of
// lifecycle.BindingStore. It lets the test seed a matching binding (so the
// controller is authorized) and, when used with a binding that differs from
// the accepted metadata, drive a recovery lockout. It never blocks, so no
// SQLite read is involved.
type realBindingStore struct {
	mu      sync.RWMutex
	binding *lifecycle.DatabaseBinding
}

func (s *realBindingStore) GetBinding() (*lifecycle.DatabaseBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.binding, nil
}

func (s *realBindingStore) Bind(clusterID, epochID string, generation int) (*lifecycle.DatabaseBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := &lifecycle.DatabaseBinding{
		ClusterID:  clusterID,
		EpochID:    epochID,
		Generation: generation,
		CreatedAt:  time.Now(),
	}
	s.binding = b
	return b, nil
}

func (s *realBindingStore) CountMessages() (int64, error) { return 0, nil }

// newRealControllerEngine builds an anti-entropy engine wired to a real,
// started (then stopped) lifecycle controller. The controller is authorized:
// READY state, fresh metadata with Initialized=true, and a cached binding
// matching the metadata. The engine's admission check is the real production
// check (Admission.CheckReplicationOutbound) and its commit guard is the real
// controller (a CommitGuard).
func newRealControllerEngine(t *testing.T, cfg Config, invClient InventoryClient, msgFetcher MessageFetcher, localStore LocalStore) (*Engine, *lifecycle.Controller) {
	t.Helper()

	src := lifecycle.NewFakeMetadataSourceUnavailable()
	binding := &realBindingStore{}
	ctrl := lifecycle.NewController(lifecycle.Config{
		MetadataPollInterval: time.Hour, // far in the future: the observation never goes stale during the test
		NodeID:               "test-node",
		Enabled:              true,
	}, lifecycle.NewMachine(nil), src, binding, nil)

	// Establish an authorized controller without relying on the periodic
	// worker: force the state to READY, seed the accepted metadata, and warm
	// the binding cache (the production path warms the cache via the
	// metadata-evaluation path; here we set it directly for a deterministic
	// test setup).
	ctrl.Machine().ForceTransitionForTest(lifecycle.DISCOVERING, "test")
	if err := ctrl.Machine().TransitionForTest(lifecycle.READY, "test"); err != nil {
		t.Fatalf("transition to READY: %v", err)
	}
	ctrl.SetAcceptedMetadataForTest(lifecycle.ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	})
	ctrl.SetBindingCacheForTest(&lifecycle.DatabaseBinding{
		ClusterID:  "c1",
		EpochID:    "e1",
		Generation: 1,
		CreatedAt:  time.Now(),
	})

	admission := ctrl.Admission()
	engine := New(cfg, &fakePeerSource{valid: true}, invClient, msgFetcher, localStore, nil)
	// The real production admission check (non-recursive; safe to call both
	// outside and inside the commit boundary).
	engine.SetAdmissionCheck(func(ctx context.Context) error {
		d := admission.CheckReplicationOutbound(ctx)
		if !d.Allowed {
			return errors.New("lifecycle admission: " + d.Reason)
		}
		return nil
	})
	// The real controller as the commit guard.
	engine.SetCommitGuard(ctrl)
	return engine, ctrl
}

// TestRealControllerCommitCursorNoRevocation verifies that with the REAL
// controller, when authorization remains valid through the commit boundary,
// the full-sweep commit completes: the cursor resets, CompletedPasses is
// incremented, and LastSuccess is set.
func TestRealControllerCommitCursorNoRevocation(t *testing.T) {
	ids := genUUIDs(0, 3)
	invClient := &fakeInventoryClient{
		inv: map[string][]string{"gossipgrid-1": ids},
	}
	fakeStore := &fakeLocalStore{ids: make(map[string]bool)}
	msgFetcher := &fakeMessageFetcher{msgs: make(map[string]peertls.SyncMessageWire)}
	for _, id := range ids {
		msgFetcher.msgs[id] = peertls.SyncMessageWire{
			ID:        id,
			Body:      "body-" + id,
			Origin:    "gossipgrid-1.test",
			CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		}
	}
	barrierStore := &nthBarrierStore{
		inner:     fakeStore,
		blockNth:  3, // block the 3rd insert (the last of the final page)
		blockCh:   make(chan struct{}),
		enteredCh: make(chan struct{}),
	}

	cfg := Config{
		Interval:        time.Second,
		PageSize:        3,
		FetchBatchSize:  2,
		MaxPagesPerPeer: 2,
		Workers:         1,
		RequestTimeout:  5 * time.Second,
	}

	engine, ctrl := newRealControllerEngine(t, cfg, invClient, msgFetcher, barrierStore)
	_ = ctrl

	target := PeerTarget{DialAddr: "test:8443", ExpectedIdentity: "gossipgrid-1", PodIdentity: "gossipgrid-1"}
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		engine.syncPeer(ctx, target)
		close(done)
	}()

	// Wait for the 3rd insert to reach the barrier (deterministic), then
	// release it without revoking: the commit proceeds under a still-valid
	// authorization.
	select {
	case <-barrierStore.enteredCh:
	case <-time.After(5 * time.Second):
		t.Fatal("3rd insert did not reach the barrier within 5s")
	}
	close(barrierStore.blockCh)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("syncPeer did not complete within 10s")
	}

	stats := engine.Stats()
	if stats.CompletedPasses != 1 {
		t.Errorf("CompletedPasses should be 1 when authorization remains valid through the real commit guard, got %d", stats.CompletedPasses)
	}
	if stats.LastSuccess.IsZero() {
		t.Error("LastSuccess should be set when authorization remains valid through the real commit guard")
	}
	if stats.FailedPasses != 0 {
		t.Errorf("FailedPasses should be 0 when authorization remains valid through the real commit guard, got %d", stats.FailedPasses)
	}
}

// TestRealControllerCommitCursorRevocationBeforeCommit verifies the
// linearization boundary: when authorization is revoked (the controller
// transitions to RECOVERY_REQUIRED) between the last pre-insert admission
// check and the cursor commit, the real commit guard rejects the commit, the
// cursor is NOT advanced, and the pass is NOT completed.
func TestRealControllerCommitCursorRevocationBeforeCommit(t *testing.T) {
	ids := genUUIDs(0, 3)
	invClient := &fakeInventoryClient{
		inv: map[string][]string{"gossipgrid-1": ids},
	}
	fakeStore := &fakeLocalStore{ids: make(map[string]bool)}
	msgFetcher := &fakeMessageFetcher{msgs: make(map[string]peertls.SyncMessageWire)}
	for _, id := range ids {
		msgFetcher.msgs[id] = peertls.SyncMessageWire{
			ID:        id,
			Body:      "body-" + id,
			Origin:    "gossipgrid-1.test",
			CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		}
	}
	barrierStore := &nthBarrierStore{
		inner:     fakeStore,
		blockNth:  3, // block the 3rd insert (last of the final page)
		blockCh:   make(chan struct{}),
		enteredCh: make(chan struct{}),
	}

	cfg := Config{
		Interval:        time.Second,
		PageSize:        3,
		FetchBatchSize:  2,
		MaxPagesPerPeer: 2,
		Workers:         1,
		RequestTimeout:  5 * time.Second,
	}

	engine, ctrl := newRealControllerEngine(t, cfg, invClient, msgFetcher, barrierStore)

	target := PeerTarget{DialAddr: "test:8443", ExpectedIdentity: "gossipgrid-1", PodIdentity: "gossipgrid-1"}
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		engine.syncPeer(ctx, target)
		close(done)
	}()

	// Wait for the 3rd insert to reach the barrier. At this point the engine
	// thread is blocked in the barrier (not holding the engine or controller
	// lock). The last pre-insert admission check for insert 3 already passed.
	select {
	case <-barrierStore.enteredCh:
	case <-time.After(5 * time.Second):
		t.Fatal("3rd insert did not reach the barrier within 5s")
	}

	// Revoke authorization by moving the controller to RECOVERY_REQUIRED.
	// This is a real revocation: it takes the machine's write lock (a
	// different lock from c.mu) and sets the state, so the controller's
	// authorization snapshot (ready state check) is no longer authorized.
	if err := ctrl.SetRecoveryRequired("test revocation"); err != nil {
		t.Fatalf("SetRecoveryRequired: %v", err)
	}

	// Release the barrier: the engine thread calls commitCursor, the real
	// guard evaluates the (now-unauthorized) snapshot and rejects the commit.
	close(barrierStore.blockCh)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("syncPeer did not complete within 10s (possible deadlock at the commit guard)")
	}

	// Verify: CompletedPasses must NOT be incremented; LastSuccess must NOT be
	// set; the cursor must NOT have advanced.
	stats := engine.Stats()
	if stats.CompletedPasses != 0 {
		t.Errorf("CompletedPasses should be 0 after revocation before commit via the real guard, got %d", stats.CompletedPasses)
	}
	if !stats.LastSuccess.IsZero() {
		t.Errorf("LastSuccess should be zero after revocation before commit via the real guard, got %v", stats.LastSuccess)
	}
	if stats.FailedPasses == 0 {
		t.Error("FailedPasses should be > 0 after revocation before commit via the real guard")
	}
	peerProg, ok := stats.PerPeer["gossipgrid-1"]
	if !ok {
		t.Fatalf("expected per-peer progress for gossipgrid-1, got keys: %v", stats.PerPeer)
	}
	if peerProg.LastCursor != "" {
		t.Errorf("cursor should be empty (not advanced) after revocation before commit, got %q", peerProg.LastCursor)
	}
	// The 3 inserted IDs of the page should be preserved (immutable records).
	fakeStore.mu.Lock()
	insertedCount := len(fakeStore.inserted)
	fakeStore.mu.Unlock()
	if insertedCount < 3 {
		t.Errorf("expected at least 3 IDs to be inserted (immutable records preserved), got %d", insertedCount)
	}
}

// TestRealControllerCommitCursorCommitThenRevocation verifies the other side
// of the linearization boundary: a commit that linearizes while authorized
// may complete; a revocation that then completes (after the commit has
// linearized) is a normal, permitted revocation. The full-sweep commit is
// observed (CompletedPasses incremented, LastSuccess set) and the subsequent
// revocation is accepted.
func TestRealControllerCommitCursorCommitThenRevocation(t *testing.T) {
	ids := genUUIDs(0, 3)
	invClient := &fakeInventoryClient{
		inv: map[string][]string{"gossipgrid-1": ids},
	}
	fakeStore := &fakeLocalStore{ids: make(map[string]bool)}
	msgFetcher := &fakeMessageFetcher{msgs: make(map[string]peertls.SyncMessageWire)}
	for _, id := range ids {
		msgFetcher.msgs[id] = peertls.SyncMessageWire{
			ID:        id,
			Body:      "body-" + id,
			Origin:    "gossipgrid-1.test",
			CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		}
	}
	barrierStore := &nthBarrierStore{
		inner:     fakeStore,
		blockNth:  3,
		blockCh:   make(chan struct{}),
		enteredCh: make(chan struct{}),
	}

	cfg := Config{
		Interval:        time.Second,
		PageSize:        3,
		FetchBatchSize:  2,
		MaxPagesPerPeer: 2,
		Workers:         1,
		RequestTimeout:  5 * time.Second,
	}

	engine, ctrl := newRealControllerEngine(t, cfg, invClient, msgFetcher, barrierStore)

	target := PeerTarget{DialAddr: "test:8443", ExpectedIdentity: "gossipgrid-1", PodIdentity: "gossipgrid-1"}
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		engine.syncPeer(ctx, target)
		close(done)
	}()

	// Wait for the 3rd insert to reach the barrier, then release it WITHOUT
	// revoking: the commit linearizes while authorized and completes.
	select {
	case <-barrierStore.enteredCh:
	case <-time.After(5 * time.Second):
		t.Fatal("3rd insert did not reach the barrier within 5s")
	}
	close(barrierStore.blockCh)

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("syncPeer did not complete within 10s")
	}

	// The commit linearized and completed: CompletedPasses is 1.
	stats := engine.Stats()
	if stats.CompletedPasses != 1 {
		t.Errorf("CompletedPasses should be 1 for a commit that linearized before revocation, got %d", stats.CompletedPasses)
	}
	if stats.LastSuccess.IsZero() {
		t.Error("LastSuccess should be set for a commit that linearized before revocation")
	}

	// A revocation that completes after the commit has linearized is a normal,
	// permitted revocation (it is accepted; the controller is no longer
	// authorized). This verifies the revocation is not itself blocked by a
	// residual lock.
	if err := ctrl.SetRecoveryRequired("test revocation after commit"); err != nil {
		t.Fatalf("revocation after commit: %v", err)
	}
	if d := ctrl.Admission().CheckReplicationOutbound(ctx); d.Allowed {
		t.Error("after a post-commit revocation, outbound replication must NOT be allowed")
	}
}

// TestRealControllerCommitCursorWriterWaitingNoDeadlock verifies the
// core deadlock regression: a controller writer (a metadata update that
// wants c.mu Lock) must not deadlock an in-flight cursor commit. With the
// non-recursive guard, the commit holds only a single c.mu RLock and performs
// no further lock acquisition, so the writer is merely (briefly) queued
// behind the reader and both complete.
//
// The test drives a commit that blocks at the barrier (holding the engine
// lock briefly) while a concurrent goroutine repeatedly attempts
// metadata updates (which take c.mu Lock). It asserts the commit completes
// within the bounded window (no deadlock) and the metadata update also
// completes (the writer is not permanently blocked).
func TestRealControllerCommitCursorWriterWaitingNoDeadlock(t *testing.T) {
	ids := genUUIDs(0, 3)
	invClient := &fakeInventoryClient{
		inv: map[string][]string{"gossipgrid-1": ids},
	}
	fakeStore := &fakeLocalStore{ids: make(map[string]bool)}
	msgFetcher := &fakeMessageFetcher{msgs: make(map[string]peertls.SyncMessageWire)}
	for _, id := range ids {
		msgFetcher.msgs[id] = peertls.SyncMessageWire{
			ID:        id,
			Body:      "body-" + id,
			Origin:    "gossipgrid-1.test",
			CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		}
	}
	barrierStore := &nthBarrierStore{
		inner:     fakeStore,
		blockNth:  3,
		blockCh:   make(chan struct{}),
		enteredCh: make(chan struct{}),
	}

	cfg := Config{
		Interval:        time.Second,
		PageSize:        3,
		FetchBatchSize:  2,
		MaxPagesPerPeer: 2,
		Workers:         1,
		RequestTimeout:  5 * time.Second,
	}

	engine, ctrl := newRealControllerEngine(t, cfg, invClient, msgFetcher, barrierStore)

	target := PeerTarget{DialAddr: "test:8443", ExpectedIdentity: "gossipgrid-1", PodIdentity: "gossipgrid-1"}
	ctx := context.Background()

	// A concurrent writer repeatedly performs metadata-accept writes (c.mu
	// Lock) to simulate a queued controller writer while a commit is in
	// flight. It uses a stable, authorized metadata observation so the
	// admission decision is unchanged (the test is about the lock ordering,
	// not about a metadata-driven revocation).
	writerDone := make(chan struct{})
	writerOK := make(chan error, 1)
	stableMeta := lifecycle.ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	}
	go func() {
		defer close(writerDone)
		// AcceptMetadataForTest takes c.mu Lock. Repeating it exercises the
		// writer-waiting-on-reader path.
		for i := 0; i < 200; i++ {
			ctrl.AcceptMetadataForTest(stableMeta)
		}
		writerOK <- nil
	}()

	done := make(chan struct{})
	go func() {
		engine.syncPeer(ctx, target)
		close(done)
	}()

	// Wait for the 3rd insert to reach the barrier, then release it. The
	// commit must complete even though the writer is repeatedly contending
	// for c.mu Lock.
	select {
	case <-barrierStore.enteredCh:
	case <-time.After(5 * time.Second):
		t.Fatal("3rd insert did not reach the barrier within 5s")
	}
	close(barrierStore.blockCh)

	// The commit must complete within the bounded window (no deadlock).
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("cursor commit deadlocked: a writer waiting for c.mu Lock blocked the commit")
	}

	// The writer must also complete (it is not permanently blocked).
	select {
	case <-writerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("controller writer deadlocked: the commit held c.mu RLock indefinitely")
	}
	if err := <-writerOK; err != nil {
		t.Errorf("controller writer (metadata update) returned an error: %v", err)
	}
}

// TestRealControllerCommitGuardDisabledPath verifies that when the commit
// guard is nil (lifecycle protection disabled), the commit proceeds directly
// under e.mu without the controller (M4 behavior preserved).
func TestRealControllerCommitGuardDisabledPath(t *testing.T) {
	ids := genUUIDs(0, 3)
	invClient := &fakeInventoryClient{
		inv: map[string][]string{"gossipgrid-1": ids},
	}
	fakeStore := &fakeLocalStore{ids: make(map[string]bool)}
	msgFetcher := &fakeMessageFetcher{msgs: make(map[string]peertls.SyncMessageWire)}
	for _, id := range ids {
		msgFetcher.msgs[id] = peertls.SyncMessageWire{
			ID:        id,
			Body:      "body-" + id,
			Origin:    "gossipgrid-1.test",
			CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		}
	}

	cfg := Config{
		Interval:        time.Second,
		PageSize:        3,
		FetchBatchSize:  2,
		MaxPagesPerPeer: 2,
		Workers:         1,
		RequestTimeout:  5 * time.Second,
	}

	// No commit guard set (nil). No admission check either (M4 path).
	engine := New(cfg, &fakePeerSource{valid: true}, invClient, msgFetcher, fakeStore, nil)

	target := PeerTarget{DialAddr: "test:8443", ExpectedIdentity: "gossipgrid-1", PodIdentity: "gossipgrid-1"}
	ctx := context.Background()

	engine.syncPeer(ctx, target)

	stats := engine.Stats()
	if stats.CompletedPasses != 1 {
		t.Errorf("CompletedPasses should be 1 with no commit guard (M4 path), got %d", stats.CompletedPasses)
	}
	if stats.LastSuccess.IsZero() {
		t.Error("LastSuccess should be set with no commit guard (M4 path)")
	}
	if stats.FailedPasses != 0 {
		t.Errorf("FailedPasses should be 0 with no commit guard (M4 path), got %d", stats.FailedPasses)
	}
}
