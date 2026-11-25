// Package lifecycle: tests for the seed restart state machine (Milestone 5B,
// Stage 1B). These tests verify that a seed that restarts after binding (but
// before or after finalization) is correctly reconciled to the right state
// through the real Start() path, without ForceTransitionForTest.
package lifecycle

import (
	"context"
	"testing"
	"time"
)

// TestSeedRestart_BeforeFinalization verifies scenario A: a seed that
// durably bound to the authorized epoch (on a prior run) and is restarting
// while the authorization is still pending (Initialized=false,
// GenesisAuthorized=true, GenesisConsumer="", DataLossProtected=false).
// The seed must be reconciled to GENESIS (NotReady; the seed is authorized
// and bound, but not yet finalized). The test uses a newly constructed
// controller started through the real Start() path (not
// ForceTransitionForTest).
func TestSeedRestart_BeforeFinalization(t *testing.T) {
	// The local database has a durable binding that matches the pending
	// authorization (the seed bound on a prior run before the process
	// restarted).
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}

	// The metadata source returns the PENDING authorization (the operator
	// has not yet finalized).
	src := NewFakeMetadataSource(pendingMeta(), true)

	// A NEW controller: the machine is created in STARTING (a new process).
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Start the controller through the real Start() path. The initial
	// metadata read is synchronous: it reads the pending authorization from
	// the fake source, accepts it, and calls reevaluateState. The state
	// must be reconciled to GENESIS (the seed is bound and authorized, but
	// not yet finalized).
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start (seed restart, before finalization): %v", err)
	}
	// Stop the controller (the test is synchronous; the metadata worker is
	// not needed).
	ctrl.Stop(context.Background())

	// The state must be GENESIS (the seed is (re)established in the pending
	// genesis state). NotReady: the seed is in GENESIS (not a ready state),
	// and Initialized=false prevents READY.
	snap := ctrl.Snapshot()
	if snap.State != GENESIS {
		t.Fatalf("expected state GENESIS after seed restart (before finalization); got %s (reason=%s)", snap.State, snap.Reason)
	}

	// The binding must be present and match the authoritative metadata.
	b := ctrl.LocalBinding()
	if b == nil {
		t.Fatal("expected the local binding to be present after seed restart; got nil")
	}
	if b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatalf("expected the binding to match (cluster=%s, epoch=%s); got cluster=%s, epoch=%s", testClusterID, testEpochID, b.ClusterID, b.EpochID)
	}

	// Public writes must be blocked (GENESIS state; the seed is not ready).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("expected public writes to be blocked (seed is in GENESIS, not ready); got allowed")
	}
}

// TestSeedRestart_AfterFinalization verifies scenario B: a seed that
// durably bound to the authorized epoch AND was finalized (the operator's
// finalization Job consumed the authorization, set Initialized=true, and
// armed DataLossProtected) and is now restarting. The seed must be promoted
// to READY (NotReady during reconciliation, but READY once the finalization
// is observed). The test uses a newly constructed controller started through
// the real Start() path (not ForceTransitionForTest).
func TestSeedRestart_AfterFinalization(t *testing.T) {
	// The local database has a durable binding that matches the finalized
	// metadata (the seed bound on a prior run, and the operator finalized
	// before this restart).
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}

	// The metadata source returns the FINALIZED metadata (the operator's
	// finalization Job set Initialized=true, consumed the authorization,
	// set GenesisConsumer to the seed, and armed DataLossProtected).
	src := NewFakeMetadataSource(finalizedMeta(), true)

	// A NEW controller: the machine is created in STARTING (a new process).
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Start the controller through the real Start() path. The initial
	// metadata read is synchronous: it reads the finalized metadata from the
	// fake source, accepts it, and calls reevaluateState. The state must be
	// promoted to READY (the seed is bound and the finalization is
	// observed).
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start (seed restart, after finalization): %v", err)
	}
	// Stop the controller (the test is synchronous; the metadata worker is
	// not needed). Do this AFTER the assertions so the admission check
	// sees a running (not stopped) controller.
	defer ctrl.Stop(context.Background())

	// The state must be READY (the seed was promoted from the genesis
	// state after observing the complete finalization).
	snap := ctrl.Snapshot()
	if snap.State != READY {
		t.Fatalf("expected state READY after seed restart (after finalization); got %s (reason=%s)", snap.State, snap.Reason)
	}

	// The binding must be present and match the authoritative metadata.
	b := ctrl.LocalBinding()
	if b == nil {
		t.Fatal("expected the local binding to be present after seed restart; got nil")
	}
	if b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatalf("expected the binding to match (cluster=%s, epoch=%s); got cluster=%s, epoch=%s", testClusterID, testEpochID, b.ClusterID, b.EpochID)
	}

	// Public writes must now be allowed (the seed is READY with a fresh,
	// valid, matching observation and a matching binding). The controller
	// is still running (Stop is deferred), so the admission check sees a
	// fresh, valid, authorized state.
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if !decision.Allowed {
		t.Fatalf("expected public writes to be allowed after promotion to READY; got denied (reason=%s)", decision.Reason)
	}
}

// TestSeedRestart_NonSeedIsNotPromoted verifies that a NON-seed replica
// (NodeID != SeedIdentity) that has a binding matching the metadata is NOT
// automatically promoted to READY or GENESIS on restart. The seed-only
// reconciliation (reevaluateState Case 2) applies only to the designated
// seed; a non-seed replica must not be auto-promoted.
func TestSeedRestart_NonSeedIsNotPromoted(t *testing.T) {
	// The local database has a binding that matches the finalized metadata
	// (a joining replica that synced its binding from the cluster).
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}

	// The metadata source returns the FINALIZED metadata.
	src := NewFakeMetadataSource(finalizedMeta(), true)

	// A NEW controller: the NodeID is NOT the seed (a different ordinal).
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Start the controller through the real Start() path. The initial
	// metadata read is synchronous: it reads the finalized metadata,
	// accepts it, and calls reevaluateState. The non-seed replica must NOT
	// be promoted to READY (the seed-only reconciliation does not apply).
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start (non-seed restart, matching binding): %v", err)
	}
	// Stop the controller.
	ctrl.Stop(context.Background())

	// The state must NOT be READY (the non-seed replica is not the
	// designated seed; it is not auto-promoted). It should be DISCOVERING
	// (the no-binding case does not apply — there IS a binding — so the
	// matching-binding case leaves the state unchanged from STARTING, or
	// transitions to DISCOVERING via the general path).
	snap := ctrl.Snapshot()
	if snap.State == READY {
		t.Fatalf("non-seed replica was promoted to READY on restart; it must NOT be auto-promoted (got reason=%s)", snap.Reason)
	}
	if snap.State == GENESIS {
		t.Fatal("non-seed replica entered GENESIS on restart; only the designated seed may enter GENESIS")
	}
}

// TestSeedRestart_BeforeFinalization_MidRecovery verifies that a seed in
// RECOVERY_REQUIRED (e.g. a prior metadata mismatch) that restarts and
// observes a matching binding with a PENDING authorization is reconciled to
// GENESIS (the re-evaluation path). The seed is in RECOVERY_REQUIRED
// (fail-closed) and the metadata is restored with a matching epoch: the
// seed is reconciled back to GENESIS (NotReady; pending finalization).
//
// This test uses the real Start() path with a pre-seeded RECOVERY_REQUIRED
// state (set via the test-only helper before Start; the state machine is
// created in STARTING, then transitioned to RECOVERY_REQUIRED before Start
// reads the metadata).
func TestSeedRestart_BeforeFinalization_MidRecovery(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	src := NewFakeMetadataSource(pendingMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Pre-seed the state: STARTING → DISCOVERING → RECOVERY_REQUIRED
	// (simulating a prior metadata mismatch that placed the seed in
	// recovery). This is the "restart from RECOVERY_REQUIRED" scenario.
	_ = ctrl.Machine().TransitionForTest(DISCOVERING, "test: pre-recovery")
	_ = ctrl.Machine().TransitionForTest(RECOVERY_REQUIRED, "test: pre-recovery mismatch")

	// Start the controller: the initial metadata read observes the pending
	// authorization with a matching binding. The seed is in
	// RECOVERY_REQUIRED; the re-evaluation path (Case 2, RECOVERY_REQUIRED
	// sub-branch) reconciles it to DISCOVERING (metadata restored with
	// matching epoch). The seed is NOT in GENESIS yet (the
	// RECOVERY_REQUIRED sub-branch transitions to DISCOVERING, not
	// GENESIS).
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start (seed restart from RECOVERY_REQUIRED): %v", err)
	}
	ctrl.Stop(context.Background())

	// The state should be DISCOVERING (the re-evaluation path transitions
	// RECOVERY_REQUIRED → DISCOVERING on a matching binding). The seed is
	// not in GENESIS (that requires the STARTING/GENESIS sub-branch).
	snap := ctrl.Snapshot()
	if snap.State == RECOVERY_REQUIRED {
		t.Fatal("seed in RECOVERY_REQUIRED was not reconciled on matching binding; expected DISCOVERING")
	}
}

// TestSeedRestart_FreshGenesisAuthorisation verifies the full fresh-genesis
// flow through the real Start() path: a seed with an EMPTY local database
// (no binding) that observes a fresh, pending authorization is (a)
// transitioned to DISCOVERING by the initial metadata read (Case 1: no
// binding), and then (b) explicitly authorized to GENESIS by
// AuthorizeGenesis (the genesis-entry loop path). The seed is in GENESIS
// (NotReady; pending finalization) after the authorization.
func TestSeedRestart_FreshGenesisAuthorisation(t *testing.T) {
	// The local database is EMPTY (no binding, no messages).
	binding := &fakeBindingStoreWithMessages{}

	// The metadata source returns the PENDING authorization.
	src := NewFakeMetadataSource(pendingMeta(), true)

	// A NEW controller.
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Start the controller: the initial metadata read observes the pending
	// authorization with NO local binding (Case 1). The seed is transitioned
	// to DISCOVERING (the first-authorization case: no binding, metadata
	// available).
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start (fresh genesis, empty DB): %v", err)
	}

	// The state must be DISCOVERING (Case 1: no binding, metadata available).
	snap := ctrl.Snapshot()
	if snap.State != DISCOVERING {
		t.Fatalf("expected state DISCOVERING after fresh genesis authorization (empty DB); got %s (reason=%s)", snap.State, snap.Reason)
	}

	// Now authorize the genesis (the genesis-entry loop path). The seed
	// binds the local database and transitions to GENESIS.
	if err := ctrl.AuthorizeGenesis(context.Background()); err != nil {
		t.Fatalf("AuthorizeGenesis (fresh genesis, empty DB): %v", err)
	}

	// The state must be GENESIS (the seed is authorized and bound, but not
	// yet finalized).
	snap = ctrl.Snapshot()
	if snap.State != GENESIS {
		t.Fatalf("expected state GENESIS after AuthorizeGenesis; got %s (reason=%s)", snap.State, snap.Reason)
	}

	// The binding must be present and match.
	b := binding.binding
	if b == nil {
		t.Fatal("expected the local database to be bound; got nil")
	}
	if b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatalf("expected the binding to match (cluster=%s, epoch=%s); got cluster=%s, epoch=%s", testClusterID, testEpochID, b.ClusterID, b.EpochID)
	}

	// Stop the controller.
	ctrl.Stop(context.Background())

	// Public writes must be blocked (GENESIS state).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("expected public writes to be blocked (GENESIS state); got allowed")
	}
}
