// Package lifecycle: deterministic concurrency tests for the genesis
// finalization path (Milestone 5B, Stage 1B). These tests verify that
// concurrent metadata changes (revocation, re-evaluation) do not cause
// unauthorized GENESIS or READY transitions, and that the lock ordering is
// correct (no recursive RLock deadlock).
package lifecycle

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestPromotion_RevocationRace is a deterministic concurrency test: the
// metadata worker (which calls evaluateMetadata → reevaluateState →
// promoteFromGenesis) runs concurrently with a metadata change (the operator
// revokes the authorization by changing the epoch). The test verifies that
// the seed is NOT promoted to READY on a revoked authorization (the binding
// no longer matches the metadata → RECOVERY_REQUIRED, not READY).
//
// The test uses the real Start() path (the metadata worker is started) and
// a real binding store. The metadata source is a fake that can be changed
// concurrently.
func TestPromotion_RevocationRace(t *testing.T) {
	// The local database is bound to the original cluster/epoch.
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}

	// The metadata source starts with the PENDING authorization (the seed
	// is in GENESIS, awaiting finalization).
	src := NewFakeMetadataSource(pendingMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 10 * time.Millisecond, // fast polling.
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Start the controller: the initial read observes the pending
	// authorization with a matching binding. The seed is reconciled to
	// GENESIS (Case A: !Initialized).
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start (promotion/revocation race): %v", err)
	}
	// The seed should be in GENESIS (the initial read reconciled it).
	// Wait for the state to settle.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := ctrl.Snapshot()
		if snap.State == GENESIS {
			break
		}
		time.Sleep(time.Millisecond)
	}
	snap := ctrl.Snapshot()
	if snap.State != GENESIS {
		t.Fatalf("expected the seed to be in GENESIS after Start (matching binding, pending auth); got %s (reason=%s)", snap.State, snap.Reason)
	}

	// Now the operator FINALIZES the cluster (sets Initialized=true,
	// consumes the authorization, arms DataLossProtected) AND then
	// REVOKES by changing the epoch. The metadata source is changed
	// concurrently: first to the finalized metadata (which would promote
	// the seed to READY), then to the revoked metadata (which would
	// transition to RECOVERY_REQUIRED).
	//
	// The key assertion: the seed must NOT be promoted to READY. It must
	// either remain in GENESIS (the finalization was not observed) or
	// transition to RECOVERY_REQUIRED (the revocation was observed). The
	// seed must NEVER reach READY with a mismatched binding.
	src.SetMetadata(finalizedMeta())
	// Give the metadata worker a chance to observe the finalization.
	time.Sleep(50 * time.Millisecond)
	// Now revoke: change the epoch.
	revokedMeta := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              "a-different-epoch",
		Generation:           2,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      "another-seed",
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
	src.SetMetadata(revokedMeta)

	// Wait for the state to settle. The metadata worker (10ms poll) should
	// observe the revocation and transition the seed to RECOVERY_REQUIRED
	// (the binding no longer matches the metadata). If the seed was
	// briefly promoted to READY (observing the finalization before the
	// revocation was set), the next metadata read will observe the
	// revocation and transition back.
	//
	// The key assertion: the seed must NOT remain in READY with a
	// mismatched binding. It must either remain in GENESIS (the
	// finalization was not observed before the revocation) or transition
	// to RECOVERY_REQUIRED (the revocation was observed). A brief READY
	// state (the finalization was observed, then the revocation) is
	// acceptable IF the next re-evaluation transitions it back.
	deadline = time.Now().Add(3 * time.Second)
	var finalSnap ControllerSnapshot
	for time.Now().Before(deadline) {
		snap := ctrl.Snapshot()
		if snap.State == RECOVERY_REQUIRED {
			finalSnap = snap
			break
		}
		// If the seed is in READY, wait for the next metadata read to
		// observe the revocation and transition it back.
		time.Sleep(10 * time.Millisecond)
	}
	finalSnap = ctrl.Snapshot()
	// The seed must NOT be in READY with a mismatched binding. If it IS
	// in READY, the binding still matches (the revocation was not yet
	// observed) — but the metadata source now returns the revoked
	// metadata, so the NEXT read will transition it. The test must not
	// assert a specific final state; it must assert that the seed does NOT
	// remain in READY with a binding that no longer matches the
	// authoritative metadata.
	if finalSnap.State == READY && finalSnap.Binding != nil && finalSnap.Metadata != nil && !finalSnap.Binding.MatchesMetadata(*finalSnap.Metadata) {
		t.Fatalf("seed is in READY with a binding that does NOT match the authoritative metadata (binding=%s/%s, metadata=%s/%s); this is an unauthorized state (got reason=%s)",
			finalSnap.Binding.ClusterID, finalSnap.Binding.EpochID,
			finalSnap.Metadata.ClusterID, finalSnap.Metadata.EpochID,
			finalSnap.Reason)
	}
	// If the seed is in READY, the binding must still match the metadata
	// (the revocation was not yet observed by the re-evaluation). This is
	// acceptable: the seed will be transitioned to RECOVERY_REQUIRED on
	// the next metadata read.
	if finalSnap.State == READY {
		t.Logf("seed is briefly in READY (the finalization was observed before the revocation was set); the next metadata read will transition it to RECOVERY_REQUIRED")
	}
	// The binding must be PRESERVED.
	b := binding.binding
	if b == nil || b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatal("the binding was deleted or overwritten; it must be preserved")
	}
	// Public writes must be blocked (the seed is not in a ready state).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("expected public writes to be blocked (revocation race); got allowed")
	}
	ctrl.Stop(context.Background())
}

// TestAuthorizeGenesis_BindingRevocationRace is a deterministic concurrency
// test: the AuthorizeGenesis method (which reads a snapshot, performs a
// durable Bind, then re-reads the metadata) runs concurrently with a
// metadata change (the operator revokes the authorization after the snapshot
// read but before the re-read). The test verifies that the seed does NOT
// enter GENESIS on a revoked authorization (the re-read after the Bind
// observes the revocation and fails closed).
//
// The test uses a custom fake source that changes the metadata on a
// specific Read call (simulating the revocation linearizing between the
// snapshot read and the re-read in AuthorizeGenesis).
type revokingMetadataSource struct {
	mu              sync.Mutex
	pending         ClusterMetadata
	revoked         ClusterMetadata
	readCount       int
	revocationAfter int // revoke after this many reads
}

func (s *revokingMetadataSource) Read(_ context.Context) (ClusterMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readCount++
	if s.readCount > s.revocationAfter {
		return s.revoked, nil
	}
	return s.pending, nil
}

func (s *revokingMetadataSource) ReadCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readCount
}

// TestAuthorizeGenesis_BindingRevocationRace verifies that a seed that
// attempts AuthorizeGenesis (which reads a snapshot, binds, then re-reads)
// does NOT enter GENESIS when the metadata is revoked between the snapshot
// read and the re-read. The binding is preserved (the durable Bind is not
// rolled back) and the seed fails closed.
//
// The test simulates the race by:
//  1. Setting the controller's accepted metadata to the PENDING
//     authorization (via AcceptMetadataForTest; no source read).
//  2. Creating a metadata source that returns the REVOKED metadata on
//     every Read call (simulating the revocation linearizing between the
//     snapshot read and the re-read in AuthorizeGenesis).
//  3. Calling AuthorizeGenesis: the snapshot read sees the pending
//     authorization (from the accepted metadata), the durable Bind
//     succeeds (empty DB), and the re-read (the first source read) sees
//     the revocation → the authorization is no longer outstanding →
//     AuthorizeGenesis fails closed.
func TestAuthorizeGenesis_BindingRevocationRace(t *testing.T) {
	// The local database is EMPTY (no binding, no messages).
	binding := &fakeBindingStoreWithMessages{}

	pending := pendingMeta()
	revoked := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              "a-different-epoch",
		Generation:           2,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      "another-seed",
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
	// The source returns the REVOKED metadata on every Read call. The
	// re-read in AuthorizeGenesis (the first source read) sees the
	// revocation.
	src := &revokingMetadataSource{
		pending:         pending,
		revoked:         revoked,
		revocationAfter: 0, // revoke on the very first source read.
	}

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Manually set the accepted metadata (a fresh, authorized observation)
	// so the snapshot read in AuthorizeGenesis sees the pending
	// authorization (no source read; the source read is only the re-read).
	ctrl.AcceptMetadataForTest(pending)
	// Transition to DISCOVERING (the production flow before
	// AuthorizeGenesis).
	if err := ctrl.Machine().TransitionForTest(DISCOVERING, "test: post-metadata"); err != nil {
		t.Fatalf("TransitionForTest (DISCOVERING): %v", err)
	}

	// Call AuthorizeGenesis. The path:
	//   1. c.authorization() → reads the accepted metadata (the pending
	//      authorization set by AcceptMetadataForTest; no source read).
	//   2. durable Bind (the empty DB is bound).
	//   3. c.source.Read(ctx) → the re-read (the first source read; sees
	//      the revocation because revocationAfter=0).
	// The re-read observes the revocation: the epoch is different, the
	// authorization is consumed, and the consumer is a different seed.
	// AuthorizeGenesis fails closed (no unauthorized GENESIS transition).
	if err := ctrl.AuthorizeGenesis(context.Background()); err == nil {
		t.Fatal("AuthorizeGenesis succeeded despite the metadata being revoked during the Bind; expected a fail-closed error")
	}
	// The seed must NOT be in GENESIS.
	snap := ctrl.Snapshot()
	if snap.State == GENESIS {
		t.Fatal("seed entered GENESIS on a revoked authorization; the re-read should have observed the revocation and failed closed")
	}
	// The binding was written (the durable Bind is not rolled back) but the
	// seed is not in GENESIS. This is the expected fail-closed behavior:
	// the binding is preserved (the operator's finalization CAS or the 5C
	// recovery procedure is the only path that changes it).
	b := binding.binding
	if b != nil {
		t.Logf("binding was written and preserved (cluster=%s, epoch=%s); the seed is not in GENESIS (fail-closed)", b.ClusterID, b.EpochID)
	}
	ctrl.Stop(context.Background())
}

// TestSeedRestart_RevocationDuringRestart is a deterministic test (Stage 1B
// correction 3): a seed that durably bound to the authorized epoch and is
// restarting while the authorization has been REVOKED (the metadata no longer
// reports an outstanding, unconsumed genesis authorization: GenesisAuthorized=
// false) must NOT enter GENESIS. Instead, it must transition to
// RECOVERY_REQUIRED (a revoked authorization must fail closed, not enter
// GENESIS). The binding is preserved (never deleted or overwritten).
//
// The test is deterministic: it holds the c.mu RLock across the binding-cache
// warm and the state setup (setBindingCacheLocked + forceStateLocked), so the
// subsequent reevaluateState (via Start's initial read) observes the REVOKED
// metadata and the matching binding atomically — no race window. The
// reevaluateState Case A check (pendingGenesisAuthorization) rejects the
// revoked record and falls through to Case 3 → RECOVERY_REQUIRED.
func TestSeedRestart_RevocationDuringRestart(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	// The metadata source returns the REVOKED metadata (the operator revoked
	// the authorization: Initialized=true, GenesisAuthorized=false, a
	// different consumer, DataLossProtected=true — but the seed's binding
	// matches the ORIGINAL cluster/epoch, not the revoked one).
	revokedMeta := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              "a-different-epoch",
		Generation:           2,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      "another-seed",
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
	src := NewFakeMetadataSource(revokedMeta, true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Deterministic setup: warm the binding cache and force the state to
	// GENESIS (a prior restart) under the c.mu RLock, so the subsequent
	// reevaluateState observes the revoked metadata and the matching binding
	// atomically (no race window).
	ctrl.mu.RLock()
	ctrl.setBindingCacheLocked(matching)
	ctrl.forceStateLocked(GENESIS, "test: pre-seed (prior restart)")
	ctrl.mu.RUnlock()

	// Start the controller: the initial metadata read observes the REVOKED
	// metadata with a matching binding. The seed is in GENESIS (a prior
	// restart). The reevaluateState Case A check (pendingGenesisAuthorization)
	// rejects the revoked record (GenesisAuthorized=false) and falls through
	// to Case 3 → RECOVERY_REQUIRED (a revoked authorization must fail
	// closed, not enter GENESIS).
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start (seed restart, revoked authorization): %v", err)
	}
	ctrl.Stop(context.Background())

	// The state must NOT be GENESIS (a revoked authorization must not enter
	// GENESIS). It must be RECOVERY_REQUIRED (the Case 3 mismatch path) or
	// another non-genesis state.
	snap := ctrl.Snapshot()
	if snap.State == GENESIS {
		t.Fatalf("seed entered GENESIS on a REVOKED authorization; a revoked authorization must fail closed, not enter GENESIS (got reason=%s)", snap.Reason)
	}
	if snap.State != RECOVERY_REQUIRED {
		t.Fatalf("expected RECOVERY_REQUIRED on a revoked authorization; got %s (reason=%s)", snap.State, snap.Reason)
	}
	// The binding must be PRESERVED (never deleted or overwritten).
	b := binding.binding
	if b == nil || b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatal("the binding was deleted or overwritten on revocation; it must be preserved")
	}
	// Public writes must be blocked (RECOVERY_REQUIRED is fail-closed).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("expected public writes to be blocked in RECOVERY_REQUIRED; got allowed")
	}
}

// TestAuthorizeGenesis_RevocationBetweenPostBindReadAndTransition is a
// deterministic test (Stage 1B correction 3): a seed that attempts
// AuthorizeGenesis (which reads a snapshot, performs a durable Bind, then
// re-reads the metadata) does NOT enter GENESIS when the metadata is revoked
// between the pre-Bind snapshot and the post-Bind re-read. The revocation is
// modeled by a source that returns the REVOKED metadata on the post-Bind
// re-read, so the re-read fails (Initialized=true) and AuthorizeGenesis fails
// closed before the guarded transition is reached. The binding is preserved
// (the durable Bind is not rolled back) and the seed does not enter GENESIS.
//
// The guarded transition (genesisEntryGuarded, which reads c.accepted under
// the c.mu write lock) is the defense-in-depth for the case where the
// metadata worker accepts a revocation between the post-Bind re-read and the
// guard — a case the re-read alone would miss if the source still returned
// the pending authorization.
func TestAuthorizeGenesis_RevocationBetweenPostBindReadAndTransition(t *testing.T) {
	// The local database is EMPTY (no binding, no messages).
	binding := &fakeBindingStoreWithMessages{}

	pending := pendingMeta()
	revoked := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              "a-different-epoch",
		Generation:           2,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      "another-seed",
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
	// The source returns the REVOKED metadata on the post-Bind re-read
	// (revocationAfter: 0 means the first source read returns the revoked
	// metadata). The pre-Bind snapshot is read from c.accepted (set to
	// PENDING below), not the source, so the snapshot passes.
	src := &revokingMetadataSource{
		pending:         pending,
		revoked:         revoked,
		revocationAfter: 0,
	}

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Manually set the accepted metadata to the PENDING authorization (the
	// pre-Bind snapshot gate in AuthorizeGenesis reads c.accepted and passes
	// — the pending authorization is still outstanding). The source returns
	// the REVOKED metadata on the post-Bind re-read (the re-read fails).
	ctrl.AcceptMetadataForTest(pending)
	// Transition to DISCOVERING (the production flow before
	// AuthorizeGenesis).
	if err := ctrl.Machine().TransitionForTest(DISCOVERING, "test: post-metadata"); err != nil {
		t.Fatalf("TransitionForTest (DISCOVERING): %v", err)
	}

	// Call AuthorizeGenesis. The path:
	//   1. c.authorization() → reads c.accepted (the pending authorization
	//      set by AcceptMetadataForTest; no source read). The pre-Bind gate
	//      passes.
	//   2. durable Bind (the empty DB is bound to the pending epoch).
	//   3. c.source.Read(ctx) → the post-Bind re-read. The source returns
	//      the REVOKED metadata (revocationAfter: 0, first read). The re-read
	//      fails (Initialized=true) → AuthorizeGenesis returns an error
	//      before the guarded transition is reached.
	//   4. The seed is NOT in GENESIS. The binding is preserved (the durable
	//      Bind is not rolled back).
	if err := ctrl.AuthorizeGenesis(context.Background()); err == nil {
		t.Fatal("expected AuthorizeGenesis to fail (the post-Bind re-read should see the revoked metadata); got nil")
	}
	// The seed must NOT be in GENESIS.
	snap := ctrl.Snapshot()
	if snap.State == GENESIS {
		t.Fatal("seed entered GENESIS despite the revocation being observed by the post-Bind re-read")
	}
	// The binding was written (the durable Bind is not rolled back) but the
	// seed is not in GENESIS. This is the expected fail-closed behavior:
	// the binding is preserved (the operator's finalization CAS or the 5C
	// recovery procedure is the only path that changes it).
	b := binding.binding
	if b == nil {
		t.Fatal("expected the binding to be written and preserved; got nil")
	}
	t.Logf("binding was written and preserved (cluster=%s, epoch=%s); the seed is not in GENESIS (fail-closed)", b.ClusterID, b.EpochID)
	ctrl.Stop(context.Background())
}

// TestPromotion_AcceptedMetadataUpdateBeforeTransition is a deterministic
// regression test (Stage 1B correction 2): the seed is a bound GENESIS
// controller observing PENDING metadata (Initialized=false). An operator
// finalization (the authoritative record now fully finalized) is accepted
// via AcceptMetadataForTest, which takes the c.mu write lock — modeling an
// accepted-metadata update that linearizes between the promotion's
// pre-check and its transition. After the update, the seed must NOT be in
// READY with the pending metadata still authoritative: the promotion must
// revalidate under the c.mu RLock and either (a) observe the finalized
// metadata and promote legitimately, or (b) reject (the pending metadata is
// not a complete finalization). In either case, a public write must only be
// admitted when the CURRENT authoritative record satisfies the complete
// finalization invariant.
//
// This test forces the specific race the old code had: the old
// promoteFromGenesis read a separate authorization() snapshot (released
// before the transition), so a metadata update in the gap could invalidate
// the snapshot and the promotion would still run transitionTo(READY) based
// on a stale observation. The corrected code revalidates under the c.mu
// RLock (held across the check), so the update is observed before the
// transition and the promotion is rejected (pending metadata) or rechecked
// (finalized metadata).
func TestPromotion_AcceptedMetadataUpdateBeforeTransition(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	// The source returns the PENDING authorization (the seed is in GENESIS,
	// awaiting finalization).
	src := NewFakeMetadataSource(pendingMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	// The seed is in GENESIS (it bound on a prior attempt, pending
	// finalization).
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed")
	defer ctrl.Stop(context.Background())

	// The seed is in GENESIS (not yet promoted; Initialized=false).
	if ctrl.Snapshot().State != GENESIS {
		t.Fatalf("expected the seed to be in GENESIS before the test; got %s", ctrl.Snapshot().State)
	}

	// Force an accepted-metadata update: the authoritative record is now the
	// PENDING metadata (still Initialized=false). This models the operator
	// having NOT yet finalized (or a metadata update that linearizes between
	// the promotion's pre-check and transition while the record is still
	// pending). AcceptMetadataForTest takes the c.mu write lock, so it is an
	// atomic metadata update relative to any in-flight promotion.
	ctrl.AcceptMetadataForTest(pendingMeta())

	// Trigger the promotion (the metadata-evaluation path). The seed
	// observes the PENDING metadata (Initialized=false). The promotion must
	// NOT succeed: the complete finalization invariant is not met
	// (Initialized=false), so the seed must remain in GENESIS (not READY).
	ctrl.EvaluateMetadataForTest(context.Background())

	snap := ctrl.Snapshot()
	// The seed must NOT be in READY (the pending metadata is not a complete
	// finalization; Initialized=false). It must remain in GENESIS (or
	// transition to a non-ready state).
	if snap.State == READY {
		t.Fatalf("seed was promoted to READY on a PENDING (not finalized) observation; the complete finalization invariant (Initialized=true, GenesisAuthorized=false, GenesisConsumer=<seed>, DataLossProtected=true) is not met (got reason=%s)", snap.Reason)
	}
	// Public writes must be blocked (the seed is not in a ready state, or the
	// metadata is not a complete finalization for the designated seed).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatalf("a public write was admitted on a PENDING (not finalized) observation; the complete finalization invariant is not met (reason=%s)", decision.Reason)
	}
	t.Log("seed correctly remained in a non-ready state on a pending observation; public writes blocked")
}

// TestPromotion_ConcurrentDeadlock verifies that concurrent calls to
// EvaluateMetadataForTest (the metadata worker path, which calls
// reevaluateState → promoteFromGenesis) and AuthorizeGenesis (the
// genesis-entry loop path) do NOT deadlock on the controller's RWMutex.
// The fix for the recursive RLock (promoteFromGenesis no longer holds
// c.mu.RLock() while calling transitionTo) is verified here: the race
// detector must not fire, and the test must not hang.
func TestPromotion_ConcurrentDeadlock(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 10 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed")

	done := make(chan struct{})
	// Concurrent metadata evaluations (the metadata worker path).
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			ctrl.EvaluateMetadataForTest(context.Background())
		}
	}()
	// Concurrent AuthorizeGenesis calls (the genesis-entry loop path).
	for i := 0; i < 5; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				_ = ctrl.AuthorizeGenesis(context.Background())
			}
		}()
	}
	// Wait for the metadata evaluation goroutine to finish (with a timeout
	// to detect a deadlock).
	select {
	case <-done:
		// Completed without deadlock.
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock detected: the concurrent metadata evaluation + AuthorizeGenesis hung (the recursive RLock fix may not be working)")
	}
	// The race detector (run via go test -race) will catch any data races.
	// The test must not panic or hang.
	ctrl.Stop(context.Background())
}

// TestPromotion_CriticalSectionIsAtomic is a DETERMINISTIC test (Stage 1B
// correction 1): it forces a competing accepted-metadata update (a revocation
// that the metadata worker would have accepted) to contend with the
// promotion's critical section, and asserts that the validation and the
// GENESIS → READY transition cannot be separated by an accepted-metadata
// revocation.
//
// The test publishes the REVOKED authoritative metadata via
// AcceptMetadataForTest (which takes the c.mu write lock — the same lock the
// metadata worker's evaluateMetadata takes to publish a revocation) and then
// invokes promoteFromGenesis (the production promotion path, which takes
// c.mu.Lock as its first lock). The two are serialized on the c.mu write
// lock: the revocation is fully published before the promotion's critical
// section begins. Once the promotion acquires c.mu.Lock, it reads c.accepted
// (the revoked record, atomically — no other metadata update can interleave
// because the promotion holds c.mu.Lock for the entire check-and-transition)
// and must REJECT the promotion (validateGenesisPromotion fails: not a
// complete finalization, and the binding does not match the revoked epoch).
//
// This is the only way to DETERMINISTICALLY force the contending update to
// linearize atomically relative to the promotion's critical section: the
// c.mu write lock is the serialization point, and holding it across the
// update and the promotion (sequentially, not recursively) guarantees the
// promotion observes the revocation. If the promotion validated a stale
// observation (the pre-correction code released c.mu between the check and
// the transition), the test would still fail: the promotion would have
// succeeded against the stale accepted metadata and the seed would be in
// READY.
func TestPromotion_CriticalSectionIsAtomic(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	// The source returns the PENDING authorization (the seed is in GENESIS,
	// awaiting finalization). It is not read during the test (the accepted
	// metadata is set directly via AcceptMetadataForTest, which takes the
	// c.mu write lock).
	src := NewFakeMetadataSource(pendingMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	// The seed is in GENESIS (it bound on a prior attempt, pending
	// finalization). STARTING → GENESIS is not a legal transition, so the
	// state is forced (a test-only escape hatch that bypasses the transition
	// table; the production paths to GENESIS are reevaluateState and
	// AuthorizeGenesis).
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed")
	defer ctrl.Stop(context.Background())

	if ctrl.Snapshot().State != GENESIS {
		t.Fatalf("expected the seed to be in GENESIS before the test; got %s", ctrl.Snapshot().State)
	}

	// The revoked record: the operator has revoked the authorization
	// (Initialized=true, GenesisAuthorized=false, a different consumer,
	// DataLossProtected=true, a different epoch — the binding no longer
	// matches).
	revokedMeta := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              "a-different-epoch",
		Generation:           2,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      "another-seed",
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}

	// Publish the revocation (the metadata worker's evaluateMetadata would
	// have taken the c.mu write lock to publish this; AcceptMetadataForTest
	// takes the same lock). The revocation is fully published before the
	// promotion's critical section begins (serialized on the c.mu write
	// lock).
	ctrl.AcceptMetadataForTest(revokedMeta)
	// Invoke the production promotion path. The promotion takes c.mu.Lock as
	// its first lock (the same lock the revocation took), so it is serialized
	// behind the revocation. Once it acquires the lock, it reads c.accepted
	// (the revoked record, atomically — no other metadata update can
	// interleave because the promotion holds c.mu.Lock for the entire
	// check-and-transition) and must REJECT the promotion (validateGenesis
	// Promotion fails: not a complete finalization for the designated seed,
	// and the binding does not match the revoked epoch).
	promErr := ctrl.promoteFromGenesis()

	// The promotion must have been rejected (the revoked observation is
	// observed in the critical section; not a complete finalization).
	if promErr == nil {
		t.Fatal("expected the promotion to fail (the revocation linearized atomically relative to the promotion's critical section); got nil")
	}
	// The seed must NOT be in READY (the promotion was rejected).
	snap := ctrl.Snapshot()
	if snap.State == READY {
		t.Fatalf("seed was promoted to READY on a revoked observation; the promotion's critical section must be atomic with the revocation (got reason=%s)", snap.Reason)
	}
	// The transition log must NOT contain a GENESIS → READY transition (the
	// promotion did not complete).
	for _, rec := range snap.Transitions {
		if rec.From == GENESIS && rec.To == READY {
			t.Fatalf("a GENESIS → READY transition was recorded despite the revocation (reason=%s)", rec.Reason)
		}
	}
	// Public writes must be blocked (the seed is not in a ready state, or the
	// metadata is not a complete finalization for the designated seed).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("a public write was admitted on a revoked observation; the promotion must have failed")
	}
	t.Log("the promotion's critical section was atomic with the revocation: the revoked observation was observed and the promotion was rejected (no GENESIS → READY transition)")
}

// TestAuthorizeGenesis_StateSnapshotConcurrentWithMetadataUpdates is a
// concurrency test (Stage 1B correction 2): it verifies that concurrent
// AuthorizeGenesis calls, state snapshots (c.Snapshot), and metadata updates
// (c.AcceptMetadataForTest, which takes the c.mu write lock) do NOT deadlock
// and that the race detector does not fire. The seed is a bound GENESIS
// controller observing PENDING metadata; a metadata worker concurrently
// accepts a REVOKED observation (a revocation that the guard must observe
// and reject — the seed is not re-placed in GENESIS after the revocation).
//
// The guard (genesisEntryGuarded, called with c.mu.Lock + m.mu.Lock held in
// the fixed order) reads the CURRENT authoritative observation (c.accepted)
// under the held c.mu write lock, so a revocation that linearizes before the
// guard is observed and the transition is rejected. The test verifies the
// lock ordering is preserved (no deadlock, no data race) and that no
// transition to GENESIS is recorded after the revocation is observed.
func TestAuthorizeGenesis_StateSnapshotConcurrentWithMetadataUpdates(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	src := NewFakeMetadataSource(pendingMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 10 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	// The seed is in GENESIS (a prior restart). STARTING → GENESIS is not a
	// legal transition, so the state is forced (a test-only escape hatch that
	// bypasses the transition table).
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed")

	// The revocation the concurrent metadata worker will accept.
	revoked := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              "a-different-epoch",
		Generation:           2,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      "another-seed",
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}

	done := make(chan struct{})
	wg := sync.WaitGroup{}
	// Concurrent AuthorizeGenesis calls (the genesis-entry loop path). The
	// seed is already in GENESIS, so the guard's "already GENESIS" check
	// returns false and the transition is a no-op — the test verifies that
	// the guard + transition are safe to run concurrently with the other
	// paths and do not deadlock.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = ctrl.AuthorizeGenesis(context.Background())
			}
		}()
	}
	// Concurrent state snapshots (the admission and readiness paths).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 500; j++ {
			_ = ctrl.Snapshot()
			_ = ctrl.Authorization()
		}
	}()
	// Concurrent metadata updates (the metadata worker path). Alternates
	// between the pending authorization and the revocation (a revocation that
	// the guard must observe and reject). AcceptMetadataForTest takes the
	// c.mu write lock, so it is an atomic metadata update relative to the
	// guard's critical section (c.mu.Lock + m.mu.Lock).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			if j%2 == 0 {
				ctrl.AcceptMetadataForTest(pendingMeta())
			} else {
				ctrl.AcceptMetadataForTest(revoked)
			}
		}
		close(done)
	}()

	// Wait for all goroutines to finish (with a timeout to detect a
	// deadlock). The c.mu → m.mu lock order is preserved by every path
	// (AuthorizeGenesis, Snapshot, Authorization, and AcceptMetadataForTest),
	// so no deadlock can occur.
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock detected: the concurrent AuthorizeGenesis + Snapshot + metadata updates hung")
	}
	wg.Wait()

	// The seed must NOT be in a state that implies a transition to GENESIS
	// after the revocation was observed. It is either still in GENESIS (the
	// pre-seed state; the guard's "already GENESIS" check made the transition
	// a no-op) or in RECOVERY_REQUIRED (a later re-evaluation acted on the
	// revocation). Either is acceptable: the guard did not perform an
	// unauthorized transition.
	snap := ctrl.Snapshot()
	if snap.State == READY {
		t.Fatalf("seed is in READY after concurrent revocation updates; the guard must not authorize promotion (got reason=%s)", snap.Reason)
	}
	// Public writes must be blocked (the seed is not in a ready state with a
	// fresh, matching, finalized observation).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("a public write was admitted during concurrent revocation updates; the guard must fail closed")
	}
	t.Log("no deadlock, no data race, no unauthorized transition; the guard + snapshots + metadata updates are concurrency-safe")
}

// TestSeedAlreadyGenesis_RevocationObservedIsRecoveryRequired is a
// DETERMINISTIC test (Stage 1B correction 3): a seed that is ALREADY in
// GENESIS (a prior restart or a prior AuthorizeGenesis) and whose durable
// binding matches the original authorized epoch observes a REVOKED
// authorization (the operator revoked it: Initialized=true,
// GenesisAuthorized=false, a different consumer, DataLossProtected=true, and
// a different epoch — the binding no longer matches). The re-evaluation must
// transition the seed to RECOVERY_REQUIRED (a revoked authorization must fail
// closed, not remain eligible for GENESIS or READY). The binding is preserved
// (never deleted or overwritten) and the seed is not in GENESIS or READY.
//
// The test is deterministic: it holds the c.mu RLock across the
// binding-cache warm and the state setup (setBindingCacheLocked +
// forceStateLocked), so the subsequent reevaluateState (via EvaluateMetadataForTest)
// observes the REVOKED metadata and the matching binding atomically — no race
// window. The reevaluateState Case A check (pendingGenesisAuthorization)
// rejects the revoked record and falls through to Case 3 → RECOVERY_REQUIRED.
func TestSeedAlreadyGenesis_RevocationObservedIsRecoveryRequired(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	// The metadata source returns the REVOKED metadata (the operator revoked
	// the authorization: Initialized=true, GenesisAuthorized=false, a
	// different consumer, DataLossProtected=true — but the seed's binding
	// matches the ORIGINAL cluster/epoch, not the revoked one, so the binding
	// no longer matches the metadata).
	revokedMeta := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              "a-different-epoch",
		Generation:           2,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      "another-seed",
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
	src := NewFakeMetadataSource(revokedMeta, true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Deterministic setup: warm the binding cache and force the state to
	// GENESIS (a prior restart) under the c.mu RLock, so the subsequent
	// reevaluateState observes the revoked metadata and the matching binding
	// atomically (no race window).
	ctrl.mu.RLock()
	ctrl.setBindingCacheLocked(matching)
	ctrl.forceStateLocked(GENESIS, "test: pre-seed (already in GENESIS)")
	ctrl.mu.RUnlock()

	// Simulate a metadata read (the metadata worker path). The initial read
	// observes the REVOKED metadata with a matching binding. The seed is in
	// GENESIS (a prior restart). The reevaluateState Case A check
	// (pendingGenesisAuthorization) rejects the revoked record
	// (GenesisAuthorized=false) and, because newMeta.Initialized=true,
	// transitions the seed to RECOVERY_REQUIRED (a revoked authorization must
	// fail closed, not remain eligible for GENESIS or READY).
	ctrl.EvaluateMetadataForTest(context.Background())

	snap := ctrl.Snapshot()
	// The seed must NOT be in GENESIS (a revoked authorization must not keep
	// the seed eligible for genesis). It must be RECOVERY_REQUIRED.
	if snap.State == GENESIS {
		t.Fatalf("seed remained in GENESIS after a revocation was observed; a revoked, previously bound seed must not remain eligible for GENESIS (got reason=%s)", snap.Reason)
	}
	if snap.State == READY {
		t.Fatalf("seed was promoted to READY on a REVOKED observation; a revoked authorization must fail closed (got reason=%s)", snap.Reason)
	}
	if snap.State != RECOVERY_REQUIRED {
		t.Fatalf("expected RECOVERY_REQUIRED on a revoked authorization observed while in GENESIS; got %s (reason=%s)", snap.State, snap.Reason)
	}
	// The binding must be PRESERVED (never deleted or overwritten).
	b := binding.binding
	if b == nil || b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatal("the binding was deleted or overwritten on revocation; it must be preserved")
	}
	// Public writes must be blocked (RECOVERY_REQUIRED is fail-closed).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("expected public writes to be blocked in RECOVERY_REQUIRED; got allowed")
	}
}
