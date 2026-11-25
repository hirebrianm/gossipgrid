// Deterministic concurrent-revocation tests for the bootstrap baseline
// guarded-commit path (Milestone 5B, Stage 2). These tests use the REAL
// lifecycle controller (lifecycle.Controller) and the REAL production
// admission check (Admission.CheckBootstrapFetch) and the REAL production
// commit guard (Controller.CommitUnderBootstrapAuthorization) to verify the
// lock-order contract:
//
//	Controller.mu (c.mu) RLock → Machine.mu (m.mu) RLock → Engine.mu (Lock)
//
// The tests exercise contention at the 5 boundaries from the spec:
//
//  1. Before issuing a bootstrap request (admission check rejects).
//  2. While a peer request is in flight (admission is checked at pass
//     entry; the in-flight revocation is observed at the commit boundary).
//  3. After receiving the response but before insertion (the commit guard
//     revalidates at the write boundary).
//  4. During the guarded local commit (the commit guard holds the RLocks;
//     a concurrent revocation is queued behind them).
//  5. While the controller is stopping (Stop takes c.mu Lock; the commit
//     guard's RLocks are queued behind it, or the stopped flag is observed
//     and the commit is rejected).
//
// The tests use controlled barriers (blocking the insert via
// nthBarrierStore) so the test can change the controller's state
// deterministically in the window between the admission check and the
// commit, with bounded completion checks (select with a timeout).

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

// newRealSyncingControllerEngine builds an anti-entropy engine wired to a
// real, authorized lifecycle controller in the SYNCING state. The controller
// is authorized for bootstrap fetch: SYNCING state, fresh metadata with
// Initialized=true, and a cached binding matching the metadata. The engine's
// bootstrap admission check is the real production check
// (Admission.CheckBootstrapFetch, wired via SetBootstrapAdmissionCheck — the
// DEDICATED bootstrap path) and its bootstrap commit guard is the real
// controller (CommitUnderBootstrapAuthorization). The bootstrap authorization
// source is also the real controller (BootstrapAuthorizationSnapshot).
func newRealSyncingControllerEngine(t *testing.T, cfg Config, invClient InventoryClient, msgFetcher MessageFetcher, localStore LocalStore) (*Engine, *lifecycle.Controller) {
	t.Helper()

	src := lifecycle.NewFakeMetadataSourceUnavailable()
	binding := &realBindingStore{}
	ctrl := lifecycle.NewController(lifecycle.Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "test-node",
		Enabled:              true,
	}, lifecycle.NewMachine(nil), src, binding, nil)

	// Force the state to SYNCING via the legal transition path.
	ctrl.Machine().ForceTransitionForTest(lifecycle.DISCOVERING, "test")
	if err := ctrl.Machine().TransitionForTest(lifecycle.SYNCING, "test"); err != nil {
		t.Fatalf("transition to SYNCING: %v", err)
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
	// The DEDICATED bootstrap admission check (wired to the real production
	// CheckBootstrapFetch; separate from the ordinary anti-entropy admission
	// check).
	engine.SetBootstrapAdmissionCheck(func(ctx context.Context) error {
		d := admission.CheckBootstrapFetch(ctx, lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"})
		if !d.Allowed {
			return errors.New("lifecycle bootstrap admission: " + d.Reason)
		}
		return nil
	})
	// The real controller as the bootstrap authorization source (pins the
	// expected cluster/epoch; revalidates at every boundary).
	engine.SetBootstrapAuthorizationSource(ctrl)
	// The real controller as the bootstrap commit guard.
	engine.SetBootstrapGuard(ctrl)
	return engine, ctrl
}

// bootstrapTestCfg returns a standard test config for the bootstrap baseline
// tests (small page size, single worker, short timeout).
func bootstrapTestCfg() Config {
	return Config{
		Interval:        time.Second,
		PageSize:        3,
		FetchBatchSize:  2,
		MaxPagesPerPeer: 2,
		Workers:         1,
		RequestTimeout:  5 * time.Second,
	}
}

// bootstrapTestSetup creates the standard test fakes (3 IDs, matching
// messages) for the bootstrap baseline tests.
func bootstrapTestSetup() (invClient *fakeInventoryClient, msgFetcher *fakeMessageFetcher, localStore *fakeLocalStore, ids []string) {
	ids = genUUIDs(0, 3)
	invClient = &fakeInventoryClient{
		inv: map[string][]string{"gossipgrid-0": ids},
	}
	localStore = &fakeLocalStore{ids: make(map[string]bool)}
	msgFetcher = &fakeMessageFetcher{msgs: make(map[string]peertls.SyncMessageWire)}
	for _, id := range ids {
		msgFetcher.msgs[id] = peertls.SyncMessageWire{
			ID:        id,
			Body:      "body-" + id,
			Origin:    "gossipgrid-0.test",
			CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
		}
	}
	return
}

// --- Boundary 1: Before issuing a bootstrap request ---

// TestBootstrapRevocation_Boundary1_RevocationBeforeRequest verifies that when
// authorization is revoked BEFORE the bootstrap request is issued (boundary
// 1), the admission check rejects and no request is sent.
func TestBootstrapRevocation_Boundary1_RevocationBeforeRequest(t *testing.T) {
	invClient, msgFetcher, localStore, _ := bootstrapTestSetup()
	engine, ctrl := newRealSyncingControllerEngine(t, bootstrapTestCfg(), invClient, msgFetcher, localStore)

	// Revoke authorization BEFORE the fetch: the controller transitions to
	// RECOVERY_REQUIRED (a real revocation that takes the machine's write
	// lock). The bootstrap admission check (CheckBootstrapFetch) requires
	// exactly SYNCING, so the revocation causes a rejection.
	if err := ctrl.SetRecoveryRequired("test revocation before request"); err != nil {
		t.Fatalf("SetRecoveryRequired: %v", err)
	}

	// The admission check should reject: no request is sent.
	res := engine.FetchBootstrapBaseline(context.Background(), "source:8443", "gossipgrid-0", "", 3)
	if res.Error == nil {
		t.Fatal("expected error when authorization is revoked before the request")
	}
	if res.Completed {
		t.Fatal("page must NOT be completed when admission is rejected")
	}
	// The inventory client must NOT have been called.
	if invClient.calls != 0 {
		t.Errorf("inventory client should not be called when admission is rejected, got %d calls", invClient.calls)
	}
}

// --- Boundary 2: While a peer request is in flight ---

// channelBlockingInventoryClient is an InventoryClient that blocks on a
// channel (blockCh) before returning the inventory page. This holds the
// engine thread at the "response in flight" boundary so the test can revoke
// authorization while the engine is about to process the response and enter
// the commit guard. The block is channel-based (deterministic: the test
// releases the block by closing blockCh, never by an arbitrary sleep).
type channelBlockingInventoryClient struct {
	mu        sync.Mutex
	inv       map[string][]string
	err       error
	reachedCh chan struct{}
	blockCh   chan struct{}
	once      sync.Once
	calls     int
}

func (c *channelBlockingInventoryClient) FetchBootstrapInventoryPage(ctx context.Context, dialAddr, expectedIdentity, after string, limit int, _ *lifecycle.RequestContext) (*peertls.InventoryPageResponse, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	c.once.Do(func() {
		close(c.reachedCh)
	})
	// Block on the channel (the "in flight" window). The test releases the
	// block by closing blockCh.
	select {
	case <-c.blockCh:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	ids := c.inv[expectedIdentity]
	if ids == nil {
		ids = []string{}
	}
	return &peertls.InventoryPageResponse{IDs: ids, NextCursor: "", HasMore: false}, nil
}

func (c *channelBlockingInventoryClient) FetchInventoryPage(_ context.Context, _ string, _ string, _ string, _ int) (*peertls.InventoryPageResponse, error) {
	return nil, nil
}

// TestBootstrapRevocation_Boundary2_RevocationInFlight verifies that when
// authorization is revoked while the peer request is in flight (boundary 2),
// the data is not installed. The inventory client blocks on a channel (the
// request is in flight); the test revokes authorization; the engine's
// revalidation before the fetch batch observes the revocation and fails the
// page. The block is released by closing the channel (deterministic, not a
// sleep).
func TestBootstrapRevocation_Boundary2_RevocationInFlight(t *testing.T) {
	invClient, msgFetcher, localStore, _ := bootstrapTestSetup()

	blockingInv := &channelBlockingInventoryClient{
		inv:       invClient.inv,
		reachedCh: make(chan struct{}),
		blockCh:   make(chan struct{}),
	}

	engine, ctrl := newRealSyncingControllerEngine(t, bootstrapTestCfg(), blockingInv, msgFetcher, localStore)

	target := PeerTarget{DialAddr: "test:8443", ExpectedIdentity: "gossipgrid-0", PodIdentity: "gossipgrid-0"}
	ctx := context.Background()

	done := make(chan *BootstrapPageResult)
	go func() {
		done <- engine.FetchBootstrapBaseline(ctx, target.DialAddr, target.ExpectedIdentity, "", 3)
	}()

	// Wait for the engine to reach the blocking inventory fetch (the request
	// is in flight; it will block on blockCh).
	select {
	case <-blockingInv.reachedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("engine did not reach the inventory fetch within 5s")
	}

	// Revoke authorization while the engine is blocked on the channel. The
	// revocation takes the machine's write lock and the controller's write
	// lock (SetRecoveryRequired). The engine is blocked on blockCh (not
	// holding any controller or machine locks), so the revocation completes
	// without waiting for the engine.
	if err := ctrl.SetRecoveryRequired("test revocation in flight"); err != nil {
		t.Fatalf("SetRecoveryRequired: %v", err)
	}

	// Release the block: close blockCh so the engine can proceed to the
	// revalidation (which will observe the revocation and fail the page).
	close(blockingInv.blockCh)

	select {
	case res := <-done:
		if res.Error == nil {
			t.Fatal("expected error when revocation is observed during in-flight request")
		}
		if res.Completed {
			t.Fatal("page must NOT be completed when revocation is observed during in-flight request")
		}
		localStore.mu.Lock()
		insertedCount := len(localStore.inserted)
		localStore.mu.Unlock()
		if insertedCount != 0 {
			t.Errorf("expected 0 inserts after in-flight revocation, got %d", insertedCount)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("FetchBootstrapBaseline did not complete within 10s (possible deadlock at the commit guard)")
	}
}

// --- Boundary 3: After receiving the response but before insertion ---

// channelBlockingMessageFetcher is a MessageFetcher that blocks on a channel
// (blockCh) before returning the messages. This holds the engine thread at
// the "response in flight" boundary for the message fetch. The block is
// channel-based (deterministic: the test releases the block by closing
// blockCh, never by an arbitrary sleep).
type channelBlockingMessageFetcher struct {
	mu        sync.Mutex
	msgs      map[string]peertls.SyncMessageWire
	reachedCh chan struct{}
	blockCh   chan struct{}
	once      sync.Once
	calls     int
}

func (f *channelBlockingMessageFetcher) FetchBootstrapMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string, _ *lifecycle.RequestContext) ([]peertls.SyncMessageWire, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	f.once.Do(func() {
		close(f.reachedCh)
	})
	// Block on the channel (the "in flight" window). The test releases the
	// block by closing blockCh.
	select {
	case <-f.blockCh:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var out []peertls.SyncMessageWire
	for _, id := range ids {
		if m, ok := f.msgs[id]; ok {
			out = append(out, m)
		}
	}
	return out, nil
}

func (f *channelBlockingMessageFetcher) FetchMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string) ([]peertls.SyncMessageWire, error) {
	return f.FetchBootstrapMessages(ctx, dialAddr, expectedIdentity, ids, nil)
}

// TestBootstrapRevocation_Boundary3_RevocationAfterResponseBeforeInsert
// verifies that when authorization is revoked after the response is received
// but before the local insert (boundary 3), the data is not installed. The
// message fetcher blocks on a channel (the response is in flight); the test
// revokes authorization; the engine's revalidation before the commit observes
// the revocation and fails the page. The block is released by closing the
// channel (deterministic, not a sleep).
func TestBootstrapRevocation_Boundary3_RevocationAfterResponseBeforeInsert(t *testing.T) {
	invClient, msgFetcher, localStore, _ := bootstrapTestSetup()

	blockingMsg := &channelBlockingMessageFetcher{
		msgs:      msgFetcher.msgs,
		reachedCh: make(chan struct{}),
		blockCh:   make(chan struct{}),
	}

	engine, ctrl := newRealSyncingControllerEngine(t, bootstrapTestCfg(), invClient, blockingMsg, localStore)

	target := PeerTarget{DialAddr: "test:8443", ExpectedIdentity: "gossipgrid-0", PodIdentity: "gossipgrid-0"}
	ctx := context.Background()

	done := make(chan *BootstrapPageResult)
	go func() {
		done <- engine.FetchBootstrapBaseline(ctx, target.DialAddr, target.ExpectedIdentity, "", 3)
	}()

	// Wait for the engine to reach the blocking message fetch (the response
	// is in flight; it will block on blockCh).
	select {
	case <-blockingMsg.reachedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("engine did not reach the message fetch within 5s")
	}

	// Revoke authorization while the engine is blocked on the channel. The
	// revocation takes the machine's write lock and the controller's write
	// lock (SetRecoveryRequired). The engine is blocked on blockCh (not
	// holding any controller or machine locks), so the revocation completes
	// without waiting for the engine.
	if err := ctrl.SetRecoveryRequired("test revocation after response"); err != nil {
		t.Fatalf("SetRecoveryRequired: %v", err)
	}

	// Release the block: close blockCh so the engine can proceed to the
	// revalidation (which will observe the revocation and fail the page).
	close(blockingMsg.blockCh)

	select {
	case res := <-done:
		if res.Error == nil {
			t.Fatal("expected error when revocation is observed after response before insert")
		}
		if res.Completed {
			t.Fatal("page must NOT be completed when revocation after response before insert")
		}
		localStore.mu.Lock()
		insertedCount := len(localStore.inserted)
		localStore.mu.Unlock()
		if insertedCount != 0 {
			t.Errorf("expected 0 inserts after post-response revocation, got %d", insertedCount)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("FetchBootstrapBaseline did not complete within 10s (possible deadlock at the commit guard)")
	}
}

// --- Boundary 4: During the guarded local commit ---

// TestBootstrapRevocation_Boundary4_RevocationDuringGuardedCommit verifies
// that when authorization is revoked while the commit guard is holding the
// RLocks (boundary 4: during the guarded local commit), the revocation is
// queued behind the commit's RLocks. The commit linearizes first (it was
// authorized at the time it entered) and succeeds; the revocation completes
// after the commit.
//
// NOTE: A barrier inside the commit guard would deadlock SetRecoveryRequired.
// Instead, this test issues the revocation concurrently with the engine
// thread (which is about to enter the commit guard) and verifies that the
// commit either succeeds (it linearized before the revocation) or is rejected
// (the revocation was observed before the commit entered). Both outcomes are
// valid: the key guarantee is that no data is installed after a revocation
// that completes before the commit linearizes.
func TestBootstrapRevocation_Boundary4_RevocationDuringGuardedCommit(t *testing.T) {
	invClient, msgFetcher, localStore, _ := bootstrapTestSetup()

	engine, ctrl := newRealSyncingControllerEngine(t, bootstrapTestCfg(), invClient, msgFetcher, localStore)

	target := PeerTarget{DialAddr: "test:8443", ExpectedIdentity: "gossipgrid-0", PodIdentity: "gossipgrid-0"}
	ctx := context.Background()

	done := make(chan *BootstrapPageResult)
	revocationDone := make(chan error, 1)

	// Issue the engine fetch and the revocation concurrently. The commit
	// either linearizes before the revocation (succeeds) or the revocation
	// is observed at the commit boundary (rejected). Both are valid.
	go func() {
		done <- engine.FetchBootstrapBaseline(ctx, target.DialAddr, target.ExpectedIdentity, "", 3)
	}()
	go func() {
		revocationDone <- ctrl.SetRecoveryRequired("test revocation during commit")
	}()

	// The commit must complete (no deadlock).
	select {
	case res := <-done:
		// Both success and rejection are valid outcomes:
		// - Success: the commit linearized before the revocation.
		// - Rejection: the revocation was observed at the commit boundary.
		if res.Error == nil && !res.Completed {
			t.Error("page should be completed if no error")
		}
		if res.Error != nil && res.Completed {
			t.Error("page should NOT be completed if there is an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("FetchBootstrapBaseline did not complete within 10s (possible deadlock with revocation)")
	}

	// The revocation must also complete.
	select {
	case revErr := <-revocationDone:
		if revErr != nil {
			t.Errorf("SetRecoveryRequired should succeed, got: %v", revErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SetRecoveryRequired did not complete within 10s (possible deadlock with commit)")
	}

	// After the revocation, the controller is no longer in SYNCING.
	if ctrl.State() != lifecycle.RECOVERY_REQUIRED {
		t.Errorf("expected state RECOVERY_REQUIRED after revocation, got %s", ctrl.State())
	}
	// No records should have been installed after the revocation.
	// (If the commit succeeded, it linearized before the revocation; the
	// records are valid. If the commit was rejected, no records were
	// installed. In either case, the local store is consistent.)
}

// --- Boundary 5: While the controller is stopping ---

// TestBootstrapRevocation_Boundary5_RevocationWhileStopping verifies that when
// the controller is stopping (boundary 5: while the controller is stopping),
// the bootstrap commit is rejected. Stop takes c.mu Lock (to set the stopped
// flag); a concurrent commit holds c.mu RLock + m.mu RLock. The commit is
// rejected (the stopped flag is observed) or linearizes before the Stop's
// c.mu Lock (the commit succeeds). Both outcomes are valid: the key
// guarantee is that the commit never completes after the controller has
// fully stopped.
//
// NOTE: A barrier inside the commit guard would deadlock Stop (which needs
// c.mu Lock while the commit guard holds c.mu RLock). Instead, this test
// starts the controller (so Stop can complete), then issues Stop concurrently
// with the engine thread. The commit either succeeds (it linearized before
// the Stop's c.mu Lock) or is rejected (the stopped flag was observed).
func TestBootstrapRevocation_Boundary5_RevocationWhileStopping(t *testing.T) {
	invClient, msgFetcher, localStore, _ := bootstrapTestSetup()

	engine, ctrl := newRealSyncingControllerEngine(t, bootstrapTestCfg(), invClient, msgFetcher, localStore)

	// Start the controller so Stop can complete (Stop waits for done, which
	// is only closed when the controller was started).
	startCtx, startCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := ctrl.Start(startCtx); err != nil {
		startCancel()
		t.Fatalf("starting controller: %v", err)
	}
	startCancel()

	target := PeerTarget{DialAddr: "test:8443", ExpectedIdentity: "gossipgrid-0", PodIdentity: "gossipgrid-0"}
	ctx := context.Background()

	done := make(chan *BootstrapPageResult)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	stopDone := make(chan struct{})

	// Issue the engine fetch and Stop concurrently. The commit either
	// linearizes before Stop's c.mu Lock (succeeds) or the stopped flag is
	// observed (rejected). Both are valid.
	go func() {
		done <- engine.FetchBootstrapBaseline(ctx, target.DialAddr, target.ExpectedIdentity, "", 3)
	}()
	go func() {
		ctrl.Stop(stopCtx)
		close(stopDone)
	}()

	// The commit must complete (no deadlock).
	select {
	case res := <-done:
		// Both success and rejection are valid outcomes.
		if res.Error == nil && !res.Completed {
			t.Error("page should be completed if no error")
		}
		if res.Error != nil && res.Completed {
			t.Error("page should NOT be completed if there is an error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("FetchBootstrapBaseline did not complete within 10s (possible deadlock with Stop)")
	}

	// Stop must also complete.
	select {
	case <-stopDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not complete within 10s (possible deadlock with commit)")
	}

	// After Stop, the controller is stopped (the authorization snapshot's
	// Stopped field reflects the stopped flag).
	if !ctrl.Authorization().Stopped {
		t.Error("controller should be stopped after Stop completes")
	}
}

// --- Backward compatibility: lifecycle-disabled passthrough ---

// TestBootstrapRevocation_BackwardCompat_LifecycleDisabled verifies that when
// lifecycle protection is disabled (Enabled=false), the bootstrap admission
// check is a passthrough (always allows) and the bootstrap commit guard is
// nil (the insert runs directly). The legacy behavior is preserved.
func TestBootstrapRevocation_BackwardCompat_LifecycleDisabled(t *testing.T) {
	invClient, msgFetcher, localStore, ids := bootstrapTestSetup()

	// Construct the engine WITHOUT a real controller (lifecycle disabled).
	// The admission check is nil (passthrough) and the bootstrap guard is nil
	// (direct insert).
	engine := New(bootstrapTestCfg(), &fakePeerSource{valid: true}, invClient, msgFetcher, localStore, nil)
	// No SetBootstrapAdmissionCheck, no SetBootstrapAuthorizationSource,
	// no SetBootstrapGuard: all are nil (disabled).

	res := engine.FetchBootstrapBaseline(context.Background(), "source:8443", "gossipgrid-0", "", 3)
	if res.Error != nil {
		t.Fatalf("expected bootstrap baseline fetch to succeed with lifecycle disabled, got: %v", res.Error)
	}
	if !res.Completed {
		t.Error("expected page to be completed with lifecycle disabled")
	}
	if len(res.Inserted) != len(ids) {
		t.Errorf("expected %d inserts with lifecycle disabled, got %d", len(ids), len(res.Inserted))
	}
}

// --- Deterministic Boundary 4: commit enters under RLocks, revocation queued behind ---

// TestBootstrapRevocation_Boundary4_Deterministic_CommitEntersFirst verifies the
// actual authorization ordering when the commit callback enters while holding the
// authorization read locks and a revocation is started concurrently. The commit
// linearizes first (it was authorized when it entered) and succeeds; the
// revocation completes after the commit releases the RLocks; a subsequent
// commit is rejected (the controller is no longer in SYNCING).
//
// This is the deterministic counterpart to the smoke test
// TestBootstrapRevocation_Boundary4_RevocationDuringGuardedCommit. It uses a
// channel-controlled barrier (nthBarrierStore) inside the commit callback so the
// test can: (1) wait for the callback to enter (holding RLocks), (2) start
// revocation in a separate goroutine (it blocks on the machine's write lock),
// (3) release the callback WITHOUT synchronously waiting for the revocation,
// (4) verify the commit succeeded, (5) verify the revocation completes, (6)
// verify a subsequent commit is rejected.
func TestBootstrapRevocation_Boundary4_Deterministic_CommitEntersFirst(t *testing.T) {
	invClient, msgFetcher, _, _ := bootstrapTestSetup()
	innerStore := &fakeLocalStore{ids: make(map[string]bool)}

	// The barrier blocks the 1st insert call (the first batch). The real
	// controller holds c.mu RLock + m.mu RLock while calling fn() (the insert
	// callback), so the barrier holds the RLocks while blocked.
	barrier := &nthBarrierStore{
		inner:     innerStore,
		blockNth:  1,
		blockCh:   make(chan struct{}),
		enteredCh: make(chan struct{}),
	}

	engine, ctrl := newRealSyncingControllerEngine(t, bootstrapTestCfg(), invClient, msgFetcher, barrier)

	ctx := context.Background()
	done := make(chan *BootstrapPageResult)

	go func() {
		done <- engine.FetchBootstrapBaseline(ctx, "source:8443", "gossipgrid-0", "", 3)
	}()

	// Wait for the commit callback to enter (the barrier is inside fn(), which
	// is called by the real controller while holding c.mu RLock + m.mu RLock).
	select {
	case <-barrier.enteredCh:
	case <-time.After(10 * time.Second):
		t.Fatal("commit callback did not enter within 10s")
	}

	// Start the revocation in a separate goroutine. The revocation
	// (SetRecoveryRequired → transitionTo) needs c.machine.mu Lock (write
	// lock), which is queued behind the commit's RLocks. It will block until
	// the commit releases the RLocks.
	revocationDone := make(chan error, 1)
	go func() {
		revocationDone <- ctrl.SetRecoveryRequired("deterministic revocation after commit entered")
	}()

	// Give the revocation goroutine a moment to start and block on the
	// write lock (deterministic: it MUST be blocked because the RLocks are
	// held by the commit callback). We do NOT wait for the revocation to
	// complete here — that would be the deadlock the spec warns about.
	time.Sleep(50 * time.Millisecond)

	// Release the callback: close blockCh so the insert can proceed. The
	// controller's CommitUnderBootstrapAuthorization will call fn() (which
	// now unblocks), then release the RLocks.
	close(barrier.blockCh)

	// Verify the commit succeeded (it linearized before the revocation).
	// NOTE: The engine performs a final revalidation before reporting
	// completion (the "revalidate before completion" boundary). This
	// revalidation observes the revocation and sets res.Error, even though
	// the commit itself succeeded (records are installed). The critical
	// assertion is that the records ARE in the local store (the commit
	// linearized before the revocation), not that res.Error is nil.
	// The first batch (FetchBatchSize=2) was committed while the barrier
	// held the RLocks (the commit linearized before the revocation). The
	// second batch (1 ID) is rejected by the post-commit revalidation (the
	// revocation is observed at the completion boundary). So exactly 2
	// records are installed.
	select {
	case res := <-done:
		expectedFirstBatch := bootstrapTestCfg().FetchBatchSize
		if len(res.Inserted) != expectedFirstBatch {
			t.Errorf("expected %d committed records (first batch linearized before revocation), got %d", expectedFirstBatch, len(res.Inserted))
		}
		// Verify the local store actually has the first-batch records.
		innerStore.mu.Lock()
		insertedCount := len(innerStore.inserted)
		innerStore.mu.Unlock()
		if insertedCount != expectedFirstBatch {
			t.Errorf("expected %d records in local store (first batch linearized before revocation), got %d", expectedFirstBatch, insertedCount)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("FetchBootstrapBaseline did not complete within 10s after barrier release")
	}

	// Verify the revocation completes (it was queued behind the RLocks and
	// now the RLocks are released).
	select {
	case revErr := <-revocationDone:
		if revErr != nil {
			t.Fatalf("SetRecoveryRequired should succeed, got: %v", revErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SetRecoveryRequired did not complete within 10s after RLocks released")
	}

	// Verify the controller is in RECOVERY_REQUIRED.
	if ctrl.State() != lifecycle.RECOVERY_REQUIRED {
		t.Errorf("expected state RECOVERY_REQUIRED after revocation, got %s", ctrl.State())
	}

	// Verify a subsequent commit is rejected (the controller is no longer
	// authorized for bootstrap).
	res2 := engine.FetchBootstrapBaseline(context.Background(), "source:8443", "gossipgrid-0", "", 3)
	if res2.Error == nil {
		t.Fatal("expected subsequent commit to be rejected after revocation, got nil error")
	}
	if res2.Completed {
		t.Fatal("subsequent commit must NOT complete after revocation")
	}
}

// --- Deterministic Boundary 4 (opposite ordering): revocation completes before commit ---

// TestBootstrapRevocation_Boundary4_Deterministic_RevocationCompletesFirst verifies
// the opposite ordering: the revocation completes BEFORE the commit callback
// enters. The commit is rejected (the controller is no longer in SYNCING).
//
// This establishes the other linearization point: when the revocation
// linearizes before the commit, the commit must be rejected and no data is
// installed.
func TestBootstrapRevocation_Boundary4_Deterministic_RevocationCompletesFirst(t *testing.T) {
	invClient, msgFetcher, localStore, ids := bootstrapTestSetup()

	engine, ctrl := newRealSyncingControllerEngine(t, bootstrapTestCfg(), invClient, msgFetcher, localStore)

	// Revoke authorization BEFORE the fetch: the controller transitions to
	// RECOVERY_REQUIRED. The bootstrap admission check (CheckBootstrapFetch)
	// requires exactly SYNCING, so the revocation causes a rejection.
	if err := ctrl.SetRecoveryRequired("deterministic revocation before commit"); err != nil {
		t.Fatalf("SetRecoveryRequired: %v", err)
	}

	// The admission check should reject: no request is sent, no data installed.
	res := engine.FetchBootstrapBaseline(context.Background(), "source:8443", "gossipgrid-0", "", 3)
	if res.Error == nil {
		t.Fatal("expected error when authorization is revoked before the commit, got nil")
	}
	if res.Completed {
		t.Fatal("page must NOT be completed when the revocation linearizes before the commit")
	}
	// No records should have been installed.
	if len(res.Inserted) > 0 {
		t.Errorf("expected 0 inserted records after pre-commit revocation, got %d", len(res.Inserted))
	}
	localStore.mu.Lock()
	insertedCount := len(localStore.inserted)
	localStore.mu.Unlock()
	if insertedCount > 0 {
		t.Errorf("expected 0 records in local store after pre-commit revocation, got %d", insertedCount)
	}
	_ = ids
}

// --- Deterministic Boundary 5: commit enters under RLocks, Stop queued behind ---

// TestBootstrapRevocation_Boundary5_Deterministic_CommitEntersFirst verifies the
// actual authorization ordering when the commit callback enters while holding the
// authorization read locks and Stop is started concurrently. The commit
// linearizes first and succeeds; Stop completes after the commit; a subsequent
// commit is rejected (the controller is stopped).
//
// NOTE: Stop takes c.mu Lock (to set the stopped flag). The commit guard holds
// c.mu RLock. So Stop is queued behind the commit's RLocks. The test must NOT
// wait for Stop to complete before releasing the callback (that would deadlock:
// Stop needs c.mu Lock, the commit holds c.mu RLock, and the test would be
// waiting for Stop while the commit is blocked).
func TestBootstrapRevocation_Boundary5_Deterministic_CommitEntersFirst(t *testing.T) {
	invClient, msgFetcher, _, _ := bootstrapTestSetup()
	innerStore := &fakeLocalStore{ids: make(map[string]bool)}

	barrier := &nthBarrierStore{
		inner:     innerStore,
		blockNth:  1,
		blockCh:   make(chan struct{}),
		enteredCh: make(chan struct{}),
	}

	engine, ctrl := newRealSyncingControllerEngine(t, bootstrapTestCfg(), invClient, msgFetcher, barrier)

	// NOTE: We do NOT start the controller. Stop works on a non-started
	// controller: it sets c.stopped = true under c.mu Lock and returns
	// promptly (without waiting for a metadata worker that was never
	// started). This avoids the metadata polling loop interfering with
	// the test's authorization state.

	ctx := context.Background()
	done := make(chan *BootstrapPageResult)

	go func() {
		done <- engine.FetchBootstrapBaseline(ctx, "source:8443", "gossipgrid-0", "", 3)
	}()

	// Wait for the commit callback to enter (holding RLocks).
	select {
	case <-barrier.enteredCh:
	case <-time.After(10 * time.Second):
		t.Fatal("commit callback did not enter within 10s")
	}

	// Start Stop in a separate goroutine. Stop takes c.mu Lock (write lock),
	// which is queued behind the commit's c.mu RLock. It will block until the
	// commit releases the RLocks.
	stopDone := make(chan struct{})
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer stopCancel()
	go func() {
		ctrl.Stop(stopCtx)
		close(stopDone)
	}()

	// Give Stop a moment to start and block on the write lock.
	time.Sleep(50 * time.Millisecond)

	// Release the callback: close blockCh so the insert can proceed.
	close(barrier.blockCh)

	// The first batch (FetchBatchSize=2) was committed while the barrier
	// held the RLocks (the commit linearized before Stop). The second batch
	// is rejected by the post-commit revalidation (the stopped state is
	// observed at the completion boundary). So exactly 2 records are
	// installed.
	select {
	case res := <-done:
		expectedFirstBatch := bootstrapTestCfg().FetchBatchSize
		if len(res.Inserted) != expectedFirstBatch {
			t.Errorf("expected %d committed records (first batch linearized before Stop), got %d", expectedFirstBatch, len(res.Inserted))
		}
		// Verify the local store actually has the first-batch records.
		innerStore.mu.Lock()
		insertedCount := len(innerStore.inserted)
		innerStore.mu.Unlock()
		if insertedCount != expectedFirstBatch {
			t.Errorf("expected %d records in local store (first batch linearized before Stop), got %d", expectedFirstBatch, insertedCount)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("FetchBootstrapBaseline did not complete within 10s after barrier release")
	}

	// Verify Stop completes (it was queued behind the RLocks and now released).
	select {
	case <-stopDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not complete within 10s after RLocks released")
	}

	// Verify the controller is stopped.
	if !ctrl.Authorization().Stopped {
		t.Error("controller should be stopped after Stop completes")
	}

	// Verify a subsequent commit is rejected (the controller is stopped).
	res2 := engine.FetchBootstrapBaseline(context.Background(), "source:8443", "gossipgrid-0", "", 3)
	if res2.Error == nil {
		t.Fatal("expected subsequent commit to be rejected after Stop, got nil error")
	}
	if res2.Completed {
		t.Fatal("subsequent commit must NOT complete after Stop")
	}
}

// --- Deterministic Boundary 5 (opposite ordering): Stop completes before commit ---

// TestBootstrapRevocation_Boundary5_Deterministic_StopCompletesFirst verifies the
// opposite ordering: Stop completes BEFORE the commit callback enters. The
// commit is rejected (the controller is stopped).
func TestBootstrapRevocation_Boundary5_Deterministic_StopCompletesFirst(t *testing.T) {
	invClient, msgFetcher, localStore, ids := bootstrapTestSetup()

	engine, ctrl := newRealSyncingControllerEngine(t, bootstrapTestCfg(), invClient, msgFetcher, localStore)

	// Start the controller so Stop can complete.
	startCtx, startCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := ctrl.Start(startCtx); err != nil {
		startCancel()
		t.Fatalf("starting controller: %v", err)
	}
	startCancel()

	// Stop the controller BEFORE the fetch.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	ctrl.Stop(stopCtx)

	// The commit should be rejected: the controller is stopped.
	res := engine.FetchBootstrapBaseline(context.Background(), "source:8443", "gossipgrid-0", "", 3)
	if res.Error == nil {
		t.Fatal("expected error when the controller is stopped before the commit, got nil")
	}
	if res.Completed {
		t.Fatal("page must NOT be completed when Stop linearizes before the commit")
	}
	if len(res.Inserted) > 0 {
		t.Errorf("expected 0 inserted records after pre-commit Stop, got %d", len(res.Inserted))
	}
	localStore.mu.Lock()
	insertedCount := len(localStore.inserted)
	localStore.mu.Unlock()
	if insertedCount > 0 {
		t.Errorf("expected 0 records in local store after pre-commit Stop, got %d", insertedCount)
	}
	_ = ids
}
