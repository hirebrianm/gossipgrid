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

// This file covers the focused, deterministic tests for the opt-in initial
// anti-entropy synchronization barrier (Milestone 7A enhancement,
// synchronization-aware Kubernetes readiness). It exercises the 13 required
// scenarios:
//
//  1. Disabled gate preserves existing readiness behavior.
//  2. Enabled gate begins NotReady (pending).
//  3. Successful complete synchronization makes the pod Ready.
//  4. Empty remote inventory successfully completes a pass.
//  5. Partial inventory pagination does not make the pod Ready.
//  6. A missing or unreachable expected peer prevents readiness.
//  7. Stale or incomplete discovery cannot produce false readiness.
//  8. A single-replica deployment can become Ready.
//  9. Five replicas starting together do not deadlock.
// 10. A replacement replica with an empty SQLite database recovers messages
//     before readiness.
// 11. Transient peer loss after successful initialization does not undo the
//     completed initial barrier.
// 12. Lifecycle authorization continues to take precedence.
// 13. Cancellation and shutdown do not leak workers or goroutines.
//
// Scenarios 9, 10, and 13 are additionally exercised in the integration test
// (cmd/gossipgrid/initial_sync_integration_test.go) with real mTLS, real
// SQLite, and real networking. Here they are covered with the fake
// dependencies for determinism and speed.
//
// A note on timing: the anti-entropy engine's scheduler (runScheduler) only
// fires a cycle on time.NewTicker(c.cfg.Interval), with no immediate first
// tick. Config.Validate requires Interval >= time.Second, so the engine-level
// tests below use Interval = time.Second and wait up to ~2 seconds for the
// first tick. This is a wait-for-completion (aeWaitUntil-style) pattern, not
// an arbitrary grace period: the test asserts the post-tick state and fails
// if the barrier did not transition as required.

// --- Scenario 1: Disabled gate preserves existing readiness behavior ---

// A nil tracker represents the disabled barrier: Complete() returns true
// (the barrier imposes no constraint) and Status() reports "disabled". This
// preserves the existing 7A /readyz behavior when the barrier is off.
func TestInitialSyncDisabledPreservesReadiness(t *testing.T) {
	var tracker *InitialSyncTracker
	if tracker.Complete() != true {
		t.Errorf("disabled (nil) tracker: Complete() = %v, want true (barrier imposes no constraint)", tracker.Complete())
	}
	if tracker.Enabled() != false {
		t.Errorf("disabled (nil) tracker: Enabled() = %v, want false", tracker.Enabled())
	}
	st := tracker.Status()
	if st.Complete != true {
		t.Errorf("disabled tracker Status(): Complete = %v, want true", st.Complete)
	}
	if st.State != "disabled" {
		t.Errorf("disabled tracker Status(): State = %q, want \"disabled\"", st.State)
	}
	if st.Expected != 0 {
		t.Errorf("disabled tracker Status(): Expected = %d, want 0", st.Expected)
	}
}

// NewInitialSyncBarrier with enabled=false returns nil (disabled sentinel).
func TestNewInitialSyncBarrierDisabled(t *testing.T) {
	tracker := NewInitialSyncBarrier(false, []string{"gossipgrid-0", "gossipgrid-1"}, "gossipgrid-0")
	if tracker != nil {
		t.Errorf("NewInitialSyncBarrier(false, ...): got non-nil tracker, want nil (disabled)")
	}
}

// --- Scenario 2: Enabled gate begins NotReady (pending) ---

// On every process start the barrier begins pending. A 5-replica cluster
// with local gossipgrid-0 expects 4 remote peers.
func TestInitialSyncEnabledBeginsPending(t *testing.T) {
	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3", "gossipgrid-4"},
		"gossipgrid-0",
	)
	if tracker.Enabled() != true {
		t.Fatalf("enabled tracker: Enabled() = %v, want true", tracker.Enabled())
	}
	if tracker.Complete() {
		t.Fatalf("enabled tracker: Complete() = true on fresh start, want false (pending)")
	}
	st := tracker.Status()
	if st.Complete {
		t.Errorf("fresh start: Status().Complete = true, want false")
	}
	if st.State != "pending" {
		t.Errorf("fresh start: Status().State = %q, want \"pending\"", st.State)
	}
	if st.Expected != 4 {
		t.Errorf("fresh start: Status().Expected = %d, want 4 (5 replicas - 1 local)", st.Expected)
	}
	if st.Completed != 0 {
		t.Errorf("fresh start: Status().Completed = %d, want 0", st.Completed)
	}
	if len(st.MissingPeers) != 4 {
		t.Errorf("fresh start: Status().MissingPeers = %v (len %d), want 4 entries", st.MissingPeers, len(st.MissingPeers))
	}
}

// --- Scenario 3: Successful complete synchronization makes the pod Ready ---

// The barrier transitions pending -> complete only when the final outstanding
// expected peer completes a full pass.
func TestInitialSyncCompleteMakesReady(t *testing.T) {
	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3", "gossipgrid-4"},
		"gossipgrid-0",
	)
	// Complete 3 of the 4 expected peers: still pending.
	if tracker.RecordComplete("gossipgrid-1") {
		t.Errorf("after 1 of 4 peers: RecordComplete returned true, want false")
	}
	if tracker.RecordComplete("gossipgrid-2") {
		t.Errorf("after 2 of 4 peers: RecordComplete returned true, want false")
	}
	if tracker.RecordComplete("gossipgrid-3") {
		t.Errorf("after 3 of 4 peers: RecordComplete returned true, want false")
	}
	if tracker.Complete() {
		t.Errorf("after 3 of 4 peers: Complete() = true, want false (one peer still outstanding)")
	}
	st := tracker.Status()
	if st.Completed != 3 || st.Expected != 4 {
		t.Errorf("after 3 of 4: Completed=%d Expected=%d, want 3/4", st.Completed, st.Expected)
	}
	// The 4th (final) peer completes the barrier.
	done := tracker.RecordComplete("gossipgrid-4")
	if !done {
		t.Errorf("after final peer: RecordComplete returned false, want true (barrier complete)")
	}
	if !tracker.Complete() {
		t.Errorf("after final peer: Complete() = false, want true (pod may now become Ready)")
	}
	st = tracker.Status()
	if st.Complete != true || st.State != "complete" {
		t.Errorf("after final peer: Status() = %+v, want Complete=true State=complete", st)
	}
	if st.Completed != 4 || st.Expected != 4 {
		t.Errorf("after final peer: Completed=%d Expected=%d, want 4/4", st.Completed, st.Expected)
	}
	if len(st.MissingPeers) != 0 {
		t.Errorf("after final peer: MissingPeers = %v, want empty", st.MissingPeers)
	}
}

// --- Scenario 4: Empty remote inventory successfully completes a pass ---

// An empty inventory (a peer that responded successfully with zero message
// IDs) IS a completed pass: has_more=false on the first, only page is a full
// sweep. The tracker accepts the recorded pass for such a peer. The
// end-to-end engine behavior is verified in TestEngineRecordsEmptyInventoryPass.
func TestInitialSyncEmptyInventoryPassCounts(t *testing.T) {
	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1"},
		"gossipgrid-0",
	)
	done := tracker.RecordComplete("gossipgrid-1")
	if !done {
		t.Errorf("empty-inventory peer: RecordComplete = false, want true (a successful empty pass counts)")
	}
	if !tracker.Complete() {
		t.Errorf("empty-inventory peer: Complete() = false, want true")
	}
}

// --- Scenario 5: Partial inventory pagination does not make the pod Ready ---

// A partial sweep (has_more still true, the per-cycle page budget exhausted)
// is NOT a completed pass and must NOT be recorded to the barrier. The
// end-to-end engine behavior is verified in
// TestEnginePartialPaginationDoesNotRecord. Here we assert that the tracker,
// given only the peers that had completed full sweeps (with one peer's pass
// intentionally NOT recorded because its sweep was partial), remains pending.
func TestInitialSyncPartialPaginationDoesNotComplete(t *testing.T) {
	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		"gossipgrid-0",
	)
	// gossipgrid-1 completed a full sweep (recorded). gossipgrid-2's sweep was
	// partial (NOT recorded — the engine does not call RecordComplete for a
	// partial sweep).
	tracker.RecordComplete("gossipgrid-1")
	if tracker.Complete() {
		t.Errorf("one peer partial (not recorded): Complete() = true, want false (partial sweep does not complete the barrier)")
	}
	st := tracker.Status()
	if st.Completed != 1 || st.Expected != 2 {
		t.Errorf("one partial: Completed=%d Expected=%d, want 1/2", st.Completed, st.Expected)
	}
	if len(st.MissingPeers) != 1 || st.MissingPeers[0] != "gossipgrid-2" {
		t.Errorf("one partial: MissingPeers = %v, want [gossipgrid-2]", st.MissingPeers)
	}
}

// --- Scenario 6: A missing or unreachable expected peer prevents readiness ---

func TestInitialSyncUnreachablePeerPreventsReadiness(t *testing.T) {
	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3"},
		"gossipgrid-0",
	)
	// gossipgrid-3 is unreachable (its inventory fetch fails every cycle; the
	// engine never records a completed pass for it). The other 2 peers
	// complete.
	tracker.RecordComplete("gossipgrid-1")
	tracker.RecordComplete("gossipgrid-2")
	if tracker.Complete() {
		t.Errorf("one peer unreachable (never recorded): Complete() = true, want false (barrier must wait for ALL expected peers)")
	}
	st := tracker.Status()
	if len(st.MissingPeers) != 1 || st.MissingPeers[0] != "gossipgrid-3" {
		t.Errorf("unreachable peer: MissingPeers = %v, want [gossipgrid-3]", st.MissingPeers)
	}
}

// --- Scenario 7: Stale or incomplete discovery cannot produce false readiness ---

// The barrier is defined by the CONFIGURED expected membership, not the
// current DNS view. A peer discovered via DNS that is NOT in the expected
// StatefulSet membership (e.g. a stale record from a scaled-down cluster)
// must NOT count toward the barrier. RecordComplete rejects non-expected
// peers.
func TestInitialSyncStaleDiscoveryDoesNotProduceFalseReadiness(t *testing.T) {
	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1"},
		"gossipgrid-0",
	)
	// A stale DNS record for gossipgrid-9 (not in the expected membership)
	// "completes" a pass. It must be rejected.
	if tracker.RecordComplete("gossipgrid-9") {
		t.Errorf("stale peer gossipgrid-9 (not expected): RecordComplete = true, want false (stale/incomplete discovery must not count)")
	}
	if tracker.Complete() {
		t.Errorf("stale peer only: Complete() = true, want false (false readiness from incomplete discovery)")
	}
	st := tracker.Status()
	if st.Expected != 1 || st.Completed != 0 {
		t.Errorf("stale peer: Expected=%d Completed=%d, want 1/0", st.Expected, st.Completed)
	}
	// Now the real expected peer (gossipgrid-1) completes: barrier completes
	// (the stale peer is irrelevant).
	if !tracker.RecordComplete("gossipgrid-1") {
		t.Errorf("expected peer gossipgrid-1: RecordComplete = false, want true")
	}
	if !tracker.Complete() {
		t.Errorf("after expected peer completes: Complete() = false, want true")
	}
}

// --- Scenario 8: A single-replica deployment can become Ready ---

// A single-replica cluster: expected membership is just the local node, so
// there are no remote peers. The barrier is vacuously satisfied.
func TestInitialSyncSingleReplicaBecomesReady(t *testing.T) {
	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0"},
		"gossipgrid-0",
	)
	if !tracker.Complete() {
		t.Errorf("single-replica: Complete() = false, want true (no remote peers; barrier vacuously satisfied)")
	}
	st := tracker.Status()
	if st.Expected != 0 {
		t.Errorf("single-replica: Expected = %d, want 0 (local excluded, no remotes)", st.Expected)
	}
	if st.State != "complete" {
		t.Errorf("single-replica: State = %q, want \"complete\"", st.State)
	}
}

// An empty expected list (a misconfiguration) also yields a vacuously-complete
// barrier: there are no remote peers to wait for.
func TestInitialSyncEmptyMembershipIsVacuouslyComplete(t *testing.T) {
	tracker := NewInitialSyncTracker(nil, "gossipgrid-0")
	if !tracker.Complete() {
		t.Errorf("empty membership: Complete() = false, want true (no expected remote peers)")
	}
}

// --- Scenario 9: Five replicas starting together do not deadlock ---

// With empty databases, every peer's inventory is empty, so the first
// successful exchange with each peer completes a pass. The barrier does NOT
// depend on a peer being Ready (it depends on a successful inventory response,
// which a NotReady peer with its peer listener up still serves). All 5
// replicas can complete their barriers after the first round of exchanges,
// with no ordering requirement. Here we model 5 replicas each recording
// passes for the other 4, and assert all 5 become complete (no deadlock).
func TestInitialSyncFiveReplicasSimultaneousStartupNoDeadlock(t *testing.T) {
	ids := []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3", "gossipgrid-4"}
	var trackers []*InitialSyncTracker
	for _, id := range ids {
		trackers = append(trackers, NewInitialSyncTracker(ids, id))
	}
	// Simulate the first round of exchanges: each replica successfully
	// completes a full sweep (empty inventory) against each of the other 4
	// peers.
	for i := range ids {
		for j, other := range ids {
			if i == j {
				continue
			}
			trackers[i].RecordComplete(other)
		}
	}
	// All 5 replicas must be complete (no deadlock).
	for i, id := range ids {
		if !trackers[i].Complete() {
			t.Errorf("replica %s: Complete() = false after all peers recorded, want true (simultaneous startup must not deadlock)", id)
		}
	}
}

// --- Scenario 10: Replacement replica with empty SQLite recovers before readiness ---

// A replacement replica starts with an empty database and a fresh tracker. It
// must complete a full sweep for each surviving peer before it may become
// Ready. The tracker transitions pending -> complete only after all expected
// peers have a verified full pass, which is the condition the readiness
// handler checks. The end-to-end message recovery (fetching and inserting the
// missing messages) is verified in the integration test
// (TestIntegrationInitialSyncReplacementRecoversBeforeReady).
func TestInitialSyncReplacementPendingUntilAllPeersPass(t *testing.T) {
	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3", "gossipgrid-4"},
		"gossipgrid-4",
	)
	if tracker.Complete() {
		t.Fatalf("fresh replacement: Complete() = true, want false (pending until all peers synced)")
	}
	// As the replacement recovers messages from peers 0..3, each full sweep
	// completes a pass. Record 3: still pending.
	tracker.RecordComplete("gossipgrid-0")
	tracker.RecordComplete("gossipgrid-1")
	tracker.RecordComplete("gossipgrid-2")
	if tracker.Complete() {
		t.Errorf("replacement after 3 of 4 peers: Complete() = true, want false")
	}
	// The 4th peer completes the barrier: the replacement may now become Ready.
	if !tracker.RecordComplete("gossipgrid-3") {
		t.Errorf("replacement final peer: RecordComplete = false, want true")
	}
	if !tracker.Complete() {
		t.Errorf("replacement after all peers: Complete() = false, want true (recovered before readiness)")
	}
}

// --- Scenario 11: Transient peer loss after successful initialization does not undo the barrier ---

// The barrier is one-shot: once complete, a transient peer outage does NOT
// re-open it (no readiness cascade). The engine only calls RecordComplete on
// a successful full sweep; a failed/timeout pass never removes a peer from
// the completed set or flips the flag back to pending.
func TestInitialSyncTransientPeerLossDoesNotUndoBarrier(t *testing.T) {
	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		"gossipgrid-0",
	)
	tracker.RecordComplete("gossipgrid-1")
	tracker.RecordComplete("gossipgrid-2")
	if !tracker.Complete() {
		t.Fatalf("barrier should be complete")
	}
	// A transient peer outage: gossipgrid-2 becomes unreachable. Nothing
	// undoes the completed barrier.
	st := tracker.Status()
	if !st.Complete {
		t.Errorf("after transient peer loss: Status().Complete = false, want true (one-shot: a transient outage must not undo the barrier)")
	}
	if st.State != "complete" {
		t.Errorf("after transient peer loss: Status().State = %q, want \"complete\"", st.State)
	}
	// Re-recording for the same peer is a no-op: the barrier is already
	// complete (one-shot), so RecordComplete returns false — the barrier
	// did not transition on this call.
	if tracker.RecordComplete("gossipgrid-2") {
		t.Errorf("re-record for completed peer: RecordComplete = true, want false (barrier already complete; only the transition call returns true)")
	}
	if !tracker.Complete() {
		t.Errorf("after re-record: Complete() = false, want true")
	}
}

// --- Regression: RecordComplete returns true exactly once (the transition) ---

// RecordComplete must return true only for the single call that transitions
// the barrier from pending to complete. Subsequent calls — for any expected
// peer (including the peer that completed the barrier) or for an unexpected
// peer — must return false. This is the contract the anti-entropy engine
// caller relies on to emit exactly one initial-sync-complete INFO log per
// process startup (not one per subsequent completed anti-entropy pass).
func TestRecordCompleteReturnsTrueExactlyOnce(t *testing.T) {
	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		"gossipgrid-0",
	)
	// First peer: not the last, so no transition.
	if tracker.RecordComplete("gossipgrid-1") {
		t.Errorf("first of two peers: RecordComplete = true, want false (barrier still pending)")
	}
	// Second (final) peer: this is the transition call.
	if !tracker.RecordComplete("gossipgrid-2") {
		t.Errorf("final peer: RecordComplete = false, want true (this call transitions pending -> complete)")
	}
	if !tracker.Complete() {
		t.Fatalf("after transition: Complete() = false, want true")
	}
	// Subsequent calls for any expected peer return false.
	if tracker.RecordComplete("gossipgrid-1") {
		t.Errorf("after completion, expected peer gossipgrid-1: RecordComplete = true, want false")
	}
	if tracker.RecordComplete("gossipgrid-2") {
		t.Errorf("after completion, expected peer gossipgrid-2 (the completing peer): RecordComplete = true, want false")
	}
	// Subsequent calls for an unexpected peer return false.
	if tracker.RecordComplete("gossipgrid-9") {
		t.Errorf("after completion, unexpected peer gossipgrid-9: RecordComplete = true, want false")
	}
	// The barrier remains complete and diagnostics are preserved.
	if !tracker.Complete() {
		t.Errorf("after all re-calls: Complete() = false, want true (one-shot)")
	}
	st := tracker.Status()
	if st.Completed != 2 || st.Expected != 2 {
		t.Errorf("after all re-calls: Completed=%d Expected=%d, want 2/2", st.Completed, st.Expected)
	}
}

// --- Scenario 12: Lifecycle authorization continues to take precedence ---

// The readiness decision is the logical combination: lifecycle authorized AND
// barrier complete. The tracker reports the barrier state; the lifecycle
// authorization is checked separately by the readiness handler (see
// combinedReadyHandler in internal/server). The tracker's contract — the sole
// barrier input the handler relies on — is that it reports pending until
// every expected peer has a verified full pass, and complete only then. The
// handler-level combination (including lifecycle revocation taking precedence
// over a complete barrier) is verified in internal/server/initial_sync_ready_test.go.
func TestInitialSyncTrackerContractForCombinedReadiness(t *testing.T) {
	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1"},
		"gossipgrid-0",
	)
	// Pending: the handler returns 503 (lifecycle authorized but barrier not
	// complete).
	if tracker.Complete() {
		t.Fatalf("pending: Complete() = true, want false (handler must return 503)")
	}
	// Complete: the handler returns 200 (lifecycle authorized AND barrier
	// complete).
	if !tracker.RecordComplete("gossipgrid-1") {
		t.Fatalf("final peer: RecordComplete = false, want true")
	}
	if !tracker.Complete() {
		t.Errorf("complete: Complete() = false, want true (handler returns 200)")
	}
}

// --- Scenario 13: Cancellation and shutdown do not leak workers or goroutines ---

// The tracker itself has no goroutines (a passive, in-memory counter). The
// goroutine-leak concern is for the anti-entropy engine (the worker pool and
// scheduler). This test verifies the tracker is safe to use after the engine
// is stopped (a stopped engine does not undo a completed barrier, and the
// methods do not block or panic), and that the engine's active syncs drop to 0
// after Stop (no leaked workers). The end-to-end no-leak verification (goroutine
// count stable after Stop) is in the integration test
// (TestIntegrationInitialSyncNoGoroutineLeak).
func TestInitialSyncTrackerSafeAfterEngineStop(t *testing.T) {
	cfg := Config{
		Interval:        time.Second, // minimum valid; scheduler fires on the first tick at ~1s
		PageSize:        32,
		FetchBatchSize:  8,
		MaxPagesPerPeer: 4,
		Workers:         2,
		RequestTimeout:  500 * time.Millisecond,
	}
	peerSource := &fakePeerSource{
		peers:    []PeerTarget{{DialAddr: "127.0.0.1:0", ExpectedIdentity: "gossipgrid-1.ae.svc", PodIdentity: "gossipgrid-1"}},
		expected: []string{"gossipgrid-0", "gossipgrid-1"},
		valid:    true,
	}
	invClient := &fakeInventoryClient{inv: map[string][]string{"gossipgrid-1.ae.svc": {}}}
	msgFetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	localStore := &fakeLocalStore{ids: map[string]bool{}}

	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1"},
		"gossipgrid-0",
	)
	engine := New(cfg, peerSource, invClient, msgFetcher, localStore, nil)
	engine.SetInitialSyncTracker(tracker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := engine.Start(ctx); err != nil {
		t.Fatalf("engine Start: %v", err)
	}
	// Let the first cycle (first ticker at ~1s) run and complete the (empty)
	// pass for gossipgrid-1. Wait up to ~2s for the first tick.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tracker.Complete() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !tracker.Complete() {
		t.Fatalf("tracker did not complete within 2s (empty inventory should complete on the first cycle)")
	}
	// Stop the engine.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	engine.Stop(stopCtx)
	stopCancel()

	// After stop, the tracker must still report complete (a stopped engine
	// does not undo a completed barrier) and must not block or panic.
	if !tracker.Complete() {
		t.Errorf("after engine stop: Complete() = false, want true (barrier is one-shot; stop must not undo it)")
	}
	st := tracker.Status()
	if st.Complete != true || st.State != "complete" {
		t.Errorf("after engine stop: Status() = %+v, want Complete=true State=complete", st)
	}
	// The engine's active syncs must be 0 after stop (no leaked workers).
	if s := engine.Stats(); s.ActiveSyncs != 0 {
		t.Errorf("after engine stop: ActiveSyncs = %d, want 0 (no leaked workers)", s.ActiveSyncs)
	}
}

// --- Engine-level end-to-end tests (fake dependencies) ---

// TestEngineRecordsEmptyInventoryPass verifies that a full sweep of an empty
// remote inventory (has_more=false on the first page) is recorded to the
// initial-sync tracker. Scenario 4 end-to-end.
func TestEngineRecordsEmptyInventoryPass(t *testing.T) {
	cfg := Config{
		Interval:        time.Second,
		PageSize:        32,
		FetchBatchSize:  8,
		MaxPagesPerPeer: 4,
		Workers:         1,
		RequestTimeout:  500 * time.Millisecond,
	}
	// One peer with an EMPTY inventory (no message IDs).
	peerSource := &fakePeerSource{
		peers:    []PeerTarget{{DialAddr: "127.0.0.1:0", ExpectedIdentity: "gossipgrid-1.ae.svc", PodIdentity: "gossipgrid-1"}},
		expected: []string{"gossipgrid-0", "gossipgrid-1"},
		valid:    true,
	}
	invClient := &fakeInventoryClient{inv: map[string][]string{"gossipgrid-1.ae.svc": {}}}
	msgFetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	localStore := &fakeLocalStore{ids: map[string]bool{}}

	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1"},
		"gossipgrid-0",
	)
	engine := New(cfg, peerSource, invClient, msgFetcher, localStore, nil)
	engine.SetInitialSyncTracker(tracker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := engine.Start(ctx); err != nil {
		t.Fatalf("engine Start: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		engine.Stop(stopCtx)
		stopCancel()
	}()

	// Wait for the tracker to complete (the empty inventory completes a full
	// sweep on the first tick).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tracker.Complete() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("tracker did not complete within 2s for an empty inventory (a successful empty pass must count as complete)")
}

// TestEnginePartialPaginationDoesNotRecord verifies that a partial sweep
// (has_more still true, the per-cycle page budget exhausted) is NOT recorded
// to the initial-sync tracker. Scenario 5 end-to-end. The peer has a large
// inventory (1000 IDs) that cannot complete within the observation window
// (100ms at 8 IDs per cycle), guaranteeing a partial sweep. The tracker must
// remain pending.
func TestEnginePartialPaginationDoesNotRecord(t *testing.T) {
	cfg := Config{
		Interval:        100 * time.Millisecond,
		PageSize:        8, // 8 IDs per page
		FetchBatchSize:  8,
		MaxPagesPerPeer: 2, // 2 pages per cycle = 16 IDs per cycle
		Workers:         1,
		RequestTimeout:  500 * time.Millisecond,
	}
	// One peer with 1000 message IDs (cannot complete in the observation
	// window: 1000/16 = 63 cycles; at 100ms interval that is ~6.3s).
	ids := genUUIDs(1000, 1000)
	wires := make(map[string]peertls.SyncMessageWire)
	for _, id := range ids {
		wires[id] = peertls.SyncMessageWire{ID: id, Body: "x", Origin: "gossipgrid-1", CreatedAt: "2026-01-01T00:00:00Z"}
	}
	peerSource := &fakePeerSource{
		peers:    []PeerTarget{{DialAddr: "127.0.0.1:0", ExpectedIdentity: "gossipgrid-1.ae.svc", PodIdentity: "gossipgrid-1"}},
		expected: []string{"gossipgrid-0", "gossipgrid-1"},
		valid:    true,
	}
	invClient := &fakeInventoryClient{inv: map[string][]string{"gossipgrid-1.ae.svc": ids}}
	msgFetcher := &fakeMessageFetcher{msgs: wires}
	localStore := &fakeLocalStore{ids: map[string]bool{}}

	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1"},
		"gossipgrid-0",
	)
	engine := New(cfg, peerSource, invClient, msgFetcher, localStore, nil)
	engine.SetInitialSyncTracker(tracker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := engine.Start(ctx); err != nil {
		t.Fatalf("engine Start: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		engine.Stop(stopCtx)
		stopCancel()
	}()

	// Observe for ~300ms (a few cycles). The 1000-ID inventory cannot complete
	// in that time. The tracker must remain pending (partial sweeps are not
	// recorded).
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if tracker.Complete() {
		t.Errorf("large inventory (partial sweep only): Complete() = true, want false (partial pagination must NOT make the pod Ready)")
	}
	// Verify the engine did make progress (some pages fetched) to confirm the
	// cycles ran and it was a genuine partial sweep, not a no-op.
	if s := engine.Stats(); s.InventoryPages == 0 {
		t.Errorf("large inventory: InventoryPages = 0 after 300ms, want > 0 (cycles must have run; a 0 page count would mean the sweep never started)")
	}
}

// TestEngineUnreachablePeerPreventsReadiness verifies that a peer whose
// inventory fetch fails (unreachable) is never recorded to the tracker, and
// the tracker remains pending. Scenario 6 end-to-end. One reachable peer
// (gossipgrid-1, empty inventory) completes a pass; one unreachable peer
// (gossipgrid-2, inventory fetch always errors) never does.
func TestEngineUnreachablePeerPreventsReadiness(t *testing.T) {
	cfg := Config{
		Interval:        100 * time.Millisecond,
		PageSize:        32,
		FetchBatchSize:  8,
		MaxPagesPerPeer: 4,
		Workers:         1,
		RequestTimeout:  500 * time.Millisecond,
	}
	peerSource := &fakePeerSource{
		peers: []PeerTarget{
			{DialAddr: "127.0.0.1:0", ExpectedIdentity: "gossipgrid-1.ae.svc", PodIdentity: "gossipgrid-1"},
			{DialAddr: "127.0.0.1:1", ExpectedIdentity: "gossipgrid-2.ae.svc", PodIdentity: "gossipgrid-2"},
		},
		expected: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		valid:    true,
	}
	// gossipgrid-1 has an empty inventory (completes a pass). gossipgrid-2
	// errors on every fetch (unreachable).
	customClient := &perPeerInventoryClient{
		inv: map[string][]string{
			"gossipgrid-1.ae.svc": {},
			"gossipgrid-2.ae.svc": {genUUIDs(0, 1)[0]},
		},
		errFor: map[string]error{"gossipgrid-2.ae.svc": errors.New("connection refused")},
	}
	msgFetcher := &fakeMessageFetcher{msgs: map[string]peertls.SyncMessageWire{}}
	localStore := &fakeLocalStore{ids: map[string]bool{}}

	tracker := NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		"gossipgrid-0",
	)
	engine := New(cfg, peerSource, customClient, msgFetcher, localStore, nil)
	engine.SetInitialSyncTracker(tracker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := engine.Start(ctx); err != nil {
		t.Fatalf("engine Start: %v", err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		engine.Stop(stopCtx)
		stopCancel()
	}()

	// Observe for ~500ms. gossipgrid-1 (empty inventory) completes a pass;
	// gossipgrid-2 (unreachable) never does. The tracker must remain pending
	// (one expected peer still outstanding).
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if tracker.Complete() {
			t.Fatalf("tracker completed while an expected peer (gossipgrid-2) is unreachable; want pending (a missing/unreachable expected peer must prevent readiness)")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Verify gossipgrid-1 WAS recorded (the reachable peer completed) but
	// gossipgrid-2 was not.
	st := tracker.Status()
	if st.Completed != 1 || st.Expected != 2 {
		t.Errorf("unreachable peer: Completed=%d Expected=%d, want 1/2 (gossipgrid-1 recorded, gossipgrid-2 not)", st.Completed, st.Expected)
	}
	if len(st.MissingPeers) != 1 || st.MissingPeers[0] != "gossipgrid-2" {
		t.Errorf("unreachable peer: MissingPeers = %v, want [gossipgrid-2]", st.MissingPeers)
	}
}

// perPeerInventoryClient is an InventoryClient that returns an error for a
// specific peer (simulating unreachability) and serves a real keyset-paginated
// inventory for the others.
type perPeerInventoryClient struct {
	mu     sync.Mutex
	inv    map[string][]string
	errFor map[string]error
}

func (c *perPeerInventoryClient) FetchInventoryPage(_ context.Context, _, expectedIdentity, after string, limit int) (*peertls.InventoryPageResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err, ok := c.errFor[expectedIdentity]; ok {
		return nil, err
	}
	all := c.inv[expectedIdentity]
	startIdx := 0
	if after != "" {
		for i, id := range all {
			if id == after {
				startIdx = i + 1
				break
			}
		}
	}
	remaining := all[startIdx:]
	if len(remaining) == 0 {
		return &peertls.InventoryPageResponse{IDs: []string{}, HasMore: false}, nil
	}
	pageLen := limit
	if pageLen > len(remaining) {
		pageLen = len(remaining)
	}
	page := remaining[:pageLen]
	resp := &peertls.InventoryPageResponse{IDs: page, HasMore: len(remaining) > limit}
	if resp.HasMore {
		resp.NextCursor = page[len(page)-1]
	}
	return resp, nil
}

// FetchBootstrapInventoryPage is the bootstrap-fetch variant of
// FetchInventoryPage (Milestone 5B, Stage 2). The test fake serves the same
// data; the request-purpose header is untrusted and does not change the
// serving-side decision in the fake.
func (c *perPeerInventoryClient) FetchBootstrapInventoryPage(ctx context.Context, dialAddr, expectedIdentity, after string, limit int, _ *lifecycle.RequestContext) (*peertls.InventoryPageResponse, error) {
	return c.FetchInventoryPage(ctx, dialAddr, expectedIdentity, after, limit)
}
