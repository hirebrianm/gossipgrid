// Package lifecycle: negative tests for the genesis finalization invariant
// (Milestone 5B, Stage 1B). These tests verify that a bound GENESIS seed is
// NOT promoted to READY when the finalization invariant is not fully met.
// Requiring the complete invariant (not merely Initialized=true) is
// load-bearing: the 5A metadata validator intentionally accepts some legacy
// fixture combinations, so Initialized=true alone is NOT proof of
// finalization.
package lifecycle

import (
	"context"
	"testing"
	"time"
)

// makeBoundGenesisSeed creates a controller in the GENESIS state with a
// matching durable binding, ready to observe metadata. The controller is
// started (so the metadata worker is running) and then stopped before the
// assertions. The metadata source is set to the given metadata before
// EvaluateMetadataForTest is called.
func makeBoundGenesisSeed(t *testing.T, meta ClusterMetadata) (*Controller, *fakeBindingStoreWithMessages) {
	t.Helper()
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	src := NewFakeMetadataSource(meta, true)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	// Warm the binding cache and set the accepted metadata (the seed is
	// bound and in GENESIS; the metadata is the observation to evaluate).
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	// Force the state to GENESIS (the seed is bound and authorized, pending
	// finalization). The test simulates the state the seed is in after the
	// genesis authorization (AuthorizeGenesis or reevaluateState Case A).
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed")
	return ctrl, binding
}

// assertNotPromoted verifies that the seed is NOT in READY (it must remain
// in GENESIS or another non-ready state) and that public writes are blocked.
func assertNotPromoted(t *testing.T, ctrl *Controller, caseName string) {
	t.Helper()
	snap := ctrl.Snapshot()
	if snap.State == READY {
		t.Fatalf("%s: expected the seed to NOT be promoted to READY; got READY (reason=%s)", caseName, snap.Reason)
	}
	// Public writes must be blocked (the seed is not in a ready state, or
	// the metadata is not authorized).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatalf("%s: expected public writes to be blocked; got allowed (reason=%s)", caseName, decision.Reason)
	}
}

// TestPromoteFromGenesis_InitializedButNotProtected verifies that a bound
// GENESIS seed that observes metadata with Initialized=true but
// DataLossProtected=false is NOT promoted to READY. The finalization
// invariant requires DataLossProtected=true (the protection flag must be
// armed in the finalization CAS). Initialized=true alone is insufficient
// (the 5A validator accepts some legacy fixtures with this combination).
func TestPromoteFromGenesis_InitializedButNotProtected(t *testing.T) {
	meta := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              testEpochID,
		Generation:           1,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      genesisSeed,
		EverAcceptedMessages: true,
		DataLossProtected:    false, // NOT protected: the invariant is not met.
	}
	ctrl, _ := makeBoundGenesisSeed(t, meta)
	defer ctrl.Stop(context.Background())

	// The seed observes the (incomplete) metadata.
	ctrl.EvaluateMetadataForTest(context.Background())

	assertNotPromoted(t, ctrl, "Initialized=true but DataLossProtected=false")
}

// TestPromoteFromGenesis_MissingGenesisConsumer verifies that a bound
// GENESIS seed that observes metadata with Initialized=true and
// DataLossProtected=true but GenesisConsumer="" is NOT promoted to READY.
// The consumer field must be set to the designated seed (the finalization
// CAS sets it).
func TestPromoteFromGenesis_MissingGenesisConsumer(t *testing.T) {
	meta := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              testEpochID,
		Generation:           1,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      "", // MISSING: the invariant is not met.
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
	ctrl, _ := makeBoundGenesisSeed(t, meta)
	defer ctrl.Stop(context.Background())

	ctrl.EvaluateMetadataForTest(context.Background())

	assertNotPromoted(t, ctrl, "GenesisConsumer missing (empty)")
}

// TestPromoteFromGenesis_WrongGenesisConsumer verifies that a bound GENESIS
// seed that observes metadata with Initialized=true, DataLossProtected=true,
// and GenesisConsumer=<a different seed> is NOT promoted to READY. The
// consumer must be the DESIGNATED seed (the replica's own identity), not a
// different seed.
func TestPromoteFromGenesis_WrongGenesisConsumer(t *testing.T) {
	meta := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              testEpochID,
		Generation:           1,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      "gossipgrid-1", // WRONG: not the designated seed.
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
	ctrl, _ := makeBoundGenesisSeed(t, meta)
	defer ctrl.Stop(context.Background())

	ctrl.EvaluateMetadataForTest(context.Background())

	assertNotPromoted(t, ctrl, "GenesisConsumer is a different seed")
}

// TestPromoteFromGenesis_AuthorizationStillOutstanding verifies that a bound
// GENESIS seed that observes metadata with Initialized=true but
// GenesisAuthorized=true (the one-time token is NOT consumed) is NOT promoted
// to READY. The finalization must consume the authorization (set
// GenesisAuthorized=false); an outstanding authorization is not evidence of
// a completed finalization.
func TestPromoteFromGenesis_AuthorizationStillOutstanding(t *testing.T) {
	meta := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              testEpochID,
		Generation:           1,
		Initialized:          true,
		GenesisAuthorized:    true, // STILL OUTSTANDING: not consumed.
		GenesisConsumer:      genesisSeed,
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
	ctrl, _ := makeBoundGenesisSeed(t, meta)
	defer ctrl.Stop(context.Background())

	ctrl.EvaluateMetadataForTest(context.Background())

	assertNotPromoted(t, ctrl, "GenesisAuthorization still outstanding (not consumed)")
}

// TestPromoteFromGenesis_RevocationDuringPromotion verifies that a bound
// GENESIS seed whose binding no longer matches the authoritative metadata
// (the operator revoked the authorization by changing the epoch) is NOT
// promoted to READY. Instead, the seed transitions to RECOVERY_REQUIRED
// (the Case 3 mismatch path in reevaluateState). The binding is preserved
// (never deleted or overwritten).
func TestPromoteFromGenesis_RevocationDuringPromotion(t *testing.T) {
	// The metadata has a DIFFERENT epoch (the operator revoked by changing
	// the epoch). The seed's binding (the original epoch) no longer matches.
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
	ctrl, binding := makeBoundGenesisSeed(t, revokedMeta)
	defer ctrl.Stop(context.Background())

	// The seed observes the revoked metadata.
	ctrl.EvaluateMetadataForTest(context.Background())

	// The seed must NOT be promoted to READY. It must transition to
	// RECOVERY_REQUIRED (the binding no longer matches the metadata).
	snap := ctrl.Snapshot()
	if snap.State == READY {
		t.Fatalf("seed was promoted to READY on a revoked (mismatched) binding; expected RECOVERY_REQUIRED (got reason=%s)", snap.Reason)
	}
	if snap.State != RECOVERY_REQUIRED {
		t.Fatalf("expected RECOVERY_REQUIRED on revocation; got %s (reason=%s)", snap.State, snap.Reason)
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

// TestPromoteFromGenesis_StaleMetadata verifies that a bound GENESIS seed
// that observes a STALE (expired) metadata observation with
// Initialized=true is NOT promoted to READY. The promotion requires a FRESH
// observation; a stale observation is not evidence of current authority.
//
// The test simulates a stale observation by:
//  1. Setting the controller's accepted metadata to the finalized record.
//  2. Setting the freshAt to a time far in the past (stale).
//  3. Making the metadata source UNAVAILABLE (so a re-read fails, and the
//     controller retains the stale last-accepted observation).
//  4. Calling EvaluateMetadataForTest (the source read fails, the
//     authority becomes "invalid", and reevaluateState is called with the
//     stale observation — but promoteFromGenesis checks for a fresh
//     observation and fails).
func TestPromoteFromGenesis_StaleMetadata(t *testing.T) {
	// The metadata is fully finalized (all four fields set correctly).
	meta := finalizedMeta()
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	// The source is UNAVAILABLE (a transient metadata loss).
	src := NewFakeMetadataSourceUnavailable()
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	ctrl.SetBindingCacheForTest(matching)
	// Set the accepted metadata to the finalized record and mark it FRESH.
	ctrl.SetAcceptedMetadataForTest(meta)
	// Force the state to GENESIS.
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed")
	defer ctrl.Stop(context.Background())

	// Set the freshAt to a time far in the past (older than 2x the poll
	// interval = 200ms), so the observation is STALE.
	ctrl.mu.Lock()
	ctrl.freshAt = time.Now().Add(-10 * time.Minute)
	ctrl.mu.Unlock()

	// The seed observes the (stale) metadata via the metadata-evaluation
	// path. The source is unavailable (the read fails), so the authority
	// becomes "invalid". The reevaluateState is called with the stale
	// observation; promoteFromGenesis checks for a fresh observation and
	// fails (the authority is "invalid" and the observation is stale).
	ctrl.EvaluateMetadataForTest(context.Background())

	// The seed must NOT be promoted to READY (the observation is stale and
	// the authority is "invalid").
	snap := ctrl.Snapshot()
	if snap.State == READY {
		t.Fatalf("seed was promoted to READY on a stale/invalid observation; expected it to remain in GENESIS")
	}
	// Public writes must be blocked.
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("expected public writes to be blocked (stale/invalid observation); got allowed")
	}
}

// TestPromoteFromGenesis_ConflictingBinding verifies that a bound GENESIS
// seed whose local binding CONFLICTS with the authoritative metadata (a
// different cluster/epoch) is NOT promoted to READY. The binding is
// preserved (never deleted or overwritten) and the seed transitions to
// RECOVERY_REQUIRED.
func TestPromoteFromGenesis_ConflictingBinding(t *testing.T) {
	// The local database has a binding to a DIFFERENT cluster/epoch
	// (a conflict with the authoritative metadata).
	conflicting := &DatabaseBinding{ClusterID: "other-cluster", EpochID: "other-epoch", Generation: 9, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: conflicting}
	src := NewFakeMetadataSource(finalizedMeta(), true)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	ctrl.SetBindingCacheForTest(conflicting)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed")
	defer ctrl.Stop(context.Background())

	// The seed observes the (conflicting) metadata.
	ctrl.EvaluateMetadataForTest(context.Background())

	// The seed must NOT be promoted to READY. It must transition to
	// RECOVERY_REQUIRED (the binding conflicts with the metadata).
	snap := ctrl.Snapshot()
	if snap.State == READY {
		t.Fatalf("seed was promoted to READY on a conflicting binding; expected RECOVERY_REQUIRED (got reason=%s)", snap.Reason)
	}
	if snap.State != RECOVERY_REQUIRED {
		t.Fatalf("expected RECOVERY_REQUIRED on conflicting binding; got %s (reason=%s)", snap.State, snap.Reason)
	}
	// The binding must be PRESERVED.
	b := binding.binding
	if b == nil || b.ClusterID != "other-cluster" {
		t.Fatal("the conflicting binding was deleted or overwritten; it must be preserved")
	}
	// Public writes must be blocked.
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("expected public writes to be blocked in RECOVERY_REQUIRED; got allowed")
	}
}

// TestPromoteFromGenesis_CompleteInvariantPasses verifies the happy path:
// a bound GENESIS seed that observes a COMPLETELY finalized metadata
// observation (all four fields set correctly, fresh, matching binding) IS
// promoted to READY. This is the positive counterpart to the negative tests
// above.
func TestPromoteFromGenesis_CompleteInvariantPasses(t *testing.T) {
	meta := finalizedMeta()
	ctrl, _ := makeBoundGenesisSeed(t, meta)
	defer ctrl.Stop(context.Background())

	// The seed observes the fully finalized metadata.
	ctrl.EvaluateMetadataForTest(context.Background())

	// The seed must be promoted to READY.
	snap := ctrl.Snapshot()
	if snap.State != READY {
		t.Fatalf("expected the seed to be promoted to READY on a complete finalization; got %s (reason=%s)", snap.State, snap.Reason)
	}
	// Public writes must now be allowed.
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if !decision.Allowed {
		t.Fatalf("expected public writes to be allowed after promotion; got denied (reason=%s)", decision.Reason)
	}
}

// TestPromoteFromGenesis_StoppedController verifies that a STOPPED bound
// GENESIS seed is NOT promoted to READY, even when the metadata is fully
// finalized. The controller is stopped (a stopped controller must not
// authorize any operation).
func TestPromoteFromGenesis_StoppedController(t *testing.T) {
	meta := finalizedMeta()
	ctrl, _ := makeBoundGenesisSeed(t, meta)
	// Stop the controller BEFORE evaluating the metadata.
	ctrl.Stop(context.Background())

	// The seed observes the (fully finalized) metadata.
	ctrl.EvaluateMetadataForTest(context.Background())

	// The seed must NOT be promoted (the controller is stopped).
	snap := ctrl.Snapshot()
	if snap.State == READY {
		t.Fatal("a stopped controller was promoted to READY; a stopped controller must not authorize any operation")
	}
	// Public writes must be blocked (the controller is stopped).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("expected public writes to be blocked (stopped controller); got allowed")
	}
}
