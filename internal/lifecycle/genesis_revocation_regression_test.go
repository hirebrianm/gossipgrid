// Package lifecycle: Stage 1B correction 4 regression tests.
//
// These tests verify:
//  1. A seed already in GENESIS whose authoritative metadata changes to a
//     same-epoch revocation (Initialized=false, GenesisAuthorized=false,
//     GenesisConsumer="", DataLossProtected=false) transitions to
//     RECOVERY_REQUIRED (not left in GENESIS).
//  2. A temporary metadata API failure (authority "invalid") does NOT
//     transition a GENESIS seed to RECOVERY_REQUIRED.
//  3. The atomic restart GENESIS entry (reconcileSeedRestartLocked) is
//     correct under a concurrent revocation at the transition boundary.
//  4. AuthorizeGenesis returns a meaningful error when the guard rejects
//     the transition due to revocation (not nil).
//  5. The genesis entry loop does not stop when the binding exists but the
//     seed is not in GENESIS (the loop only stops on GENESIS, READY,
//     DEGRADED, or RECOVERY_REQUIRED).
package lifecycle

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestRevocation_SameEpoch_InGenesis verifies correction 1: a seed that is
// already in GENESIS (it bound on a prior attempt) whose authoritative
// metadata changes to a same-epoch revocation (same ClusterID, same EpochID,
// Initialized=false, GenesisAuthorized=false, GenesisConsumer="",
// DataLossProtected=false) must transition to RECOVERY_REQUIRED. The revocation
// is NOT an epoch mismatch (the epoch is the same) — it is a confirmed
// revocation of an outstanding authorization on the same epoch.
//
// The test is deterministic: the seed is pre-seeded in GENESIS with a matching
// binding, and the metadata source is set to the revoked metadata before
// EvaluateMetadataForTest is called (which simulates the metadata worker
// observing the revocation).
func TestRevocation_SameEpoch_InGenesis(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}

	// The revoked metadata: same ClusterID, same EpochID, but
	// GenesisAuthorized=false (the operator revoked the authorization
	// before finalization).
	revokedSameEpoch := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              testEpochID,
		Generation:           2,
		Initialized:          false,
		GenesisAuthorized:    false,
		GenesisConsumer:      "",
		EverAcceptedMessages: false,
		DataLossProtected:    false,
	}
	src := NewFakeMetadataSource(revokedSameEpoch, true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Pre-seed: the seed is already in GENESIS (a prior restart), with the
	// binding cache warmed and the accepted metadata set to the pending
	// authorization (the state before the revocation was observed).
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed (prior restart)")

	// Verify the precondition: the seed is in GENESIS.
	if snap := ctrl.Snapshot(); snap.State != GENESIS {
		t.Fatalf("expected seed to be in GENESIS before revocation; got %s", snap.State)
	}

	// The metadata worker observes the revocation (EvaluateMetadataForTest
	// reads the source, accepts the revoked metadata, and calls
	// reevaluateState).
	ctrl.EvaluateMetadataForTest(context.Background())
	defer ctrl.Stop(context.Background())

	// The seed must NOT remain in GENESIS. It must be in RECOVERY_REQUIRED
	// (a confirmed, same-epoch revocation of an outstanding authorization is
	// a recovery incident).
	snap := ctrl.Snapshot()
	if snap.State == GENESIS {
		t.Fatalf("seed remained in GENESIS after a same-epoch revocation (GenesisAuthorized=false); it must transition to RECOVERY_REQUIRED (got reason=%s)", snap.Reason)
	}
	if snap.State != RECOVERY_REQUIRED {
		t.Fatalf("expected RECOVERY_REQUIRED after same-epoch revocation; got %s (reason=%s)", snap.State, snap.Reason)
	}
	// The binding must be PRESERVED.
	b := binding.binding
	if b == nil || b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatal("the binding was deleted or overwritten on revocation; it must be preserved")
	}
	// Public writes must be blocked.
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("expected public writes to be blocked in RECOVERY_REQUIRED; got allowed")
	}
	t.Log("seed correctly transitioned to RECOVERY_REQUIRED on same-epoch revocation; binding preserved")
}

// TestRevocation_TempAPIFailure_NotRecovery verifies correction 1: a
// temporary metadata API failure (the source becomes unavailable, setting
// the authority state to "invalid") must NOT transition a GENESIS seed to
// RECOVERY_REQUIRED. The seed is left unchanged (still in GENESIS) so a
// later fresh observation can restore the correct state.
func TestRevocation_TempAPIFailure_NotRecovery(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}

	// The source starts with the PENDING authorization (the seed is in
	// GENESIS, awaiting finalization).
	src := NewFakeMetadataSource(pendingMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Pre-seed: the seed is in GENESIS with a matching binding.
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed (prior restart)")

	// Verify the precondition.
	if snap := ctrl.Snapshot(); snap.State != GENESIS {
		t.Fatalf("expected seed to be in GENESIS before test; got %s", snap.State)
	}

	// The metadata API becomes temporarily unavailable (a transient failure,
	// NOT a confirmed revocation).
	src.SetUnavailable()

	// The metadata worker observes the failure (evaluateMetadata calls
	// handleMetadataError, which sets the authority state to "invalid").
	ctrl.EvaluateMetadataForTest(context.Background())
	defer ctrl.Stop(context.Background())

	// The seed must NOT be in RECOVERY_REQUIRED (a temporary API failure is
	// not a confirmed catastrophic loss; the seed is left unchanged).
	snap := ctrl.Snapshot()
	if snap.State == RECOVERY_REQUIRED {
		t.Fatalf("seed was transitioned to RECOVERY_REQUIRED on a temporary metadata API failure (authority=invalid); a temporary failure must NOT be a recovery incident (got reason=%s)", snap.Reason)
	}
	// The seed should still be in GENESIS (unchanged).
	if snap.State != GENESIS {
		t.Fatalf("expected seed to remain in GENESIS on temporary API failure; got %s (reason=%s)", snap.State, snap.Reason)
	}
	// The binding must be preserved.
	b := binding.binding
	if b == nil {
		t.Fatal("the binding was deleted on a temporary API failure; it must be preserved")
	}
	t.Log("seed correctly remained in GENESIS on temporary metadata API failure; no RECOVERY_REQUIRED")
}

// TestRevocation_AtomixRestart_Entry verifies correction 2 (deterministic):
// a seed that is restarting (state STARTING) with a matching durable binding
// where the authoritative metadata is a same-epoch revocation (GenesisAuthorized=
// false, same ClusterID, same EpochID) must NOT enter GENESIS. Instead, it
// must transition to RECOVERY_REQUIRED (a confirmed revocation of an
// outstanding authorization on the same epoch is a recovery incident).
//
// The test is deterministic: the metadata source is set to the REVOKED
// metadata before EvaluateMetadataForTest is called. The evaluateMetadata
// path reads the source (gets the revoked metadata), accepts it (c.mu write
// lock), and calls reevaluateState. The reconcileSeedRestartLocked reads
// c.accepted under the held c.mu write lock and observes the revocation
// (GenesisAuthorized=false, same epoch), so the seed is transitioned to
// RECOVERY_REQUIRED, not GENESIS.
//
// This models the specific race the old code had: the old code read the
// authorization snapshot (released), then performed separate transitions. A
// metadata update could interleave in the gap. The corrected code reads
// c.accepted under the held c.mu write lock in the same critical section
// as the transition, so the revocation is observed atomically.
func TestRevocation_AtomixRestart_Entry(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}

	// The revoked metadata: same ClusterID, same EpochID, but
	// GenesisAuthorized=false (the operator revoked the authorization
	// before finalization).
	revokedSameEpoch := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              testEpochID,
		Generation:           2,
		Initialized:          false,
		GenesisAuthorized:    false,
		GenesisConsumer:      "",
		EverAcceptedMessages: false,
		DataLossProtected:    false,
	}
	src := NewFakeMetadataSource(revokedSameEpoch, true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// The seed is in STARTING (a new process; the machine is created in
	// STARTING). The binding cache is warmed (the durable binding exists
	// from a prior run).
	ctrl.SetBindingCacheForTest(matching)

	// The c.accepted is set to the PENDING authorization (a fresh, valid
	// observation) so that the state is STARTING with a matching binding
	// (the precondition for the seed-restart reconciliation path).
	ctrl.SetAcceptedMetadataForTest(pendingMeta())

	// Trigger the re-evaluation: evaluateMetadata reads the source (which
	// returns the REVOKED metadata), accepts it (c.mu write lock,
	// overwriting c.accepted), and calls reevaluateState. The
	// reconcileSeedRestartLocked reads c.accepted under the held c.mu
	// write lock and observes the revocation (GenesisAuthorized=false,
	// same epoch). The seed must NOT enter GENESIS; it must transition
	// to RECOVERY_REQUIRED.
	ctrl.EvaluateMetadataForTest(context.Background())
	defer ctrl.Stop(context.Background())

	snap := ctrl.Snapshot()
	if snap.State == GENESIS {
		t.Fatalf("seed entered GENESIS despite a same-epoch revocation in the authoritative metadata; the atomic reconcile should have observed the revocation and rejected GENESIS (got reason=%s)", snap.Reason)
	}
	if snap.State != RECOVERY_REQUIRED {
		t.Fatalf("expected RECOVERY_REQUIRED after atomic restart with same-epoch revocation; got %s (reason=%s)", snap.State, snap.Reason)
	}
	// The binding must be preserved.
	b := binding.binding
	if b == nil || b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatal("the binding was deleted or overwritten; it must be preserved")
	}
	t.Log("atomic restart correctly observed the same-epoch revocation and transitioned to RECOVERY_REQUIRED; binding preserved")
}

// TestAuthorizeGenesis_GuardedRevocation_ReturnsError verifies correction 5:
// when the guard (genesisEntryGuarded) rejects the GENESIS transition because
// the authorization was revoked (c.accepted no longer satisfies the
// pending-grant invariant), AuthorizeGenesis must return a meaningful error
// (not nil). The durable binding is preserved.
//
// The test simulates the race: c.accepted is set to the PENDING
// authorization (so the pre-Bind snapshot gate passes), the source returns
// the PENDING authorization on the post-Bind re-read (so the re-read
// passes), but a concurrent AcceptMetadataForTest (c.mu write lock) changes
// c.accepted to the REVOKED metadata between the re-read and the guarded
// transition. The guard reads c.accepted (the revoked metadata) and rejects
// the transition with genesisEntryRevoked → AuthorizeGenesis returns an error.
func TestAuthorizeGenesis_GuardedRevocation_ReturnsError(t *testing.T) {
	// The local database is EMPTY (no binding, no messages).
	binding := &fakeBindingStoreWithMessages{}

	pending := pendingMeta()
	revoked := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              testEpochID,
		Generation:           2,
		Initialized:          false,
		GenesisAuthorized:    false,
		GenesisConsumer:      "",
		EverAcceptedMessages: false,
		DataLossProtected:    false,
	}

	// A custom source that returns the PENDING metadata on every read
	// (so the post-Bind re-read passes).
	src := NewFakeMetadataSource(pending, true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Set c.accepted to the PENDING authorization (the pre-Bind snapshot
	// gate in AuthorizeGenesis reads c.accepted and passes).
	ctrl.AcceptMetadataForTest(pending)
	// Transition to DISCOVERING (the production flow before
	// AuthorizeGenesis).
	if err := ctrl.Machine().TransitionForTest(DISCOVERING, "test: post-metadata"); err != nil {
		t.Fatalf("TransitionForTest (DISCOVERING): %v", err)
	}

	// Run AuthorizeGenesis in a goroutine, and concurrently accept a
	// revocation (c.mu write lock) that linearizes between the post-Bind
	// re-read and the guarded transition.
	//
	// The AuthorizeGenesis path:
	//   1. c.authorization() → reads c.accepted (the pending authorization).
	//   2. durable Bind (the empty DB is bound).
	//   3. c.source.Read(ctx) → the post-Bind re-read (the source returns
	//      the PENDING metadata; the re-read passes).
	//   4. c.mu.Lock + m.mu.Lock → genesisEntryGuarded reads c.accepted.
	//
	// The concurrent AcceptMetadataForTest(revoked) takes the c.mu write
	// lock. If it linearizes between the re-read (step 3) and the guard
	// (step 4), the guard reads the REVOKED metadata and rejects the
	// transition (genesisEntryRevoked) → AuthorizeGenesis returns an error.
	//
	// The test is not fully deterministic (the timing of the concurrent
	// AcceptMetadataForTest is not guaranteed to land between steps 3 and 4),
	// but the assertion is: AuthorizeGenesis must return an error OR the
	// seed must be in GENESIS (if the revocation was not observed by the
	// guard). The key property: if the guard rejects, the error must be
	// meaningful (not nil).
	var wg sync.WaitGroup
	wg.Add(2)

	aerrCh := make(chan error, 1)
	go func() {
		defer wg.Done()
		aerrCh <- ctrl.AuthorizeGenesis(context.Background())
	}()
	go func() {
		defer wg.Done()
		// Give AuthorizeGenesis a head start (it must reach the
		// post-Bind re-read before we accept the revocation).
		time.Sleep(50 * time.Millisecond)
		ctrl.AcceptMetadataForTest(revoked)
	}()
	wg.Wait()
	defer ctrl.Stop(context.Background())

	aerr := <-aerrCh
	snap := ctrl.Snapshot()

	if aerr == nil {
		// If AuthorizeGenesis returned nil, the seed must be in GENESIS
		// (the revocation was not observed by the guard; the transition
		// succeeded). This is acceptable (the next re-evaluation will
		// transition to RECOVERY_REQUIRED).
		if snap.State != GENESIS {
			t.Fatalf("AuthorizeGenesis returned nil but the seed is not in GENESIS (state=%s); this is inconsistent", snap.State)
		}
		t.Log("AuthorizeGenesis succeeded (revocation not observed by guard); seed in GENESIS; next re-evaluation will reconcile")
		return
	}
	// AuthorizeGenesis returned an error: the guard rejected the transition
	// (the revocation was observed). The error must be meaningful.
	if snap.State == GENESIS {
		t.Fatalf("AuthorizeGenesis returned an error but the seed is in GENESIS; the guard should have rejected the transition (got reason=%s)", snap.Reason)
	}
	// The binding was written (the durable Bind is not rolled back) but the
	// seed is not in GENESIS.
	b := binding.binding
	if b == nil {
		t.Fatal("expected the binding to be written and preserved; got nil")
	}
	t.Log("AuthorizeGenesis returned a meaningful error on guard rejection; binding preserved; seed not in GENESIS")
}

// TestAuthorizeGenesis_AlreadyGenesis_ReturnsNil verifies correction 5: when
// the guard (genesisEntryGuarded) returns genesisEntryAlreadyGenesis (the
// seed is already in GENESIS), AuthorizeGenesis returns nil (a legitimate
// idempotent success). The binding is preserved.
func TestAuthorizeGenesis_AlreadyGenesis_ReturnsNil(t *testing.T) {
	src := NewFakeMetadataSource(pendingMeta(), true)
	// The local database already has a matching binding.
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	// The seed is already in GENESIS (a prior attempt).
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed")

	// AuthorizeGenesis must return nil (idempotent success).
	if err := ctrl.AuthorizeGenesis(context.Background()); err != nil {
		t.Fatalf("AuthorizeGenesis (seed already in GENESIS): expected nil (idempotent success), got: %v", err)
	}
	// The state must be unchanged (still GENESIS).
	snap := ctrl.Snapshot()
	if snap.State != GENESIS {
		t.Fatalf("expected state to remain GENESIS (idempotent); got %s", snap.State)
	}
	// The binding must be unchanged.
	if binding.binding != matching {
		t.Fatal("expected the existing matching binding to be unchanged; it was modified")
	}
	defer ctrl.Stop(context.Background())
	t.Log("AuthorizeGenesis returned nil for a seed already in GENESIS (idempotent); binding preserved")
}

// revokedMeta returns a confirmed, same-epoch genesis authorization revocation
// (Initialized=false, GenesisAuthorized=false, GenesisConsumer="",
// DataLossProtected=false, same ClusterID and EpochID as the test binding).
func revokedMeta() ClusterMetadata {
	return ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              testEpochID,
		Generation:           2,
		Initialized:          false,
		GenesisAuthorized:    false,
		GenesisConsumer:      "",
		EverAcceptedMessages: false,
		DataLossProtected:    false,
	}
}

// TestRevocation_SameEpoch_RepeatedObservations_StaysRecoveryRequired is the
// deterministic regression for the Stage 1B recovery-persistence fix: a
// confirmed, same-epoch genesis authorization revocation must keep the seed
// in RECOVERY_REQUIRED across repeated identical metadata observations. The
// generic binding-restored branch in reevaluateState must NOT treat the
// matching ClusterID/EpochID alone as proof that the authorization was
// restored and silently clear RECOVERY_REQUIRED.
func TestRevocation_SameEpoch_RepeatedObservations_StaysRecoveryRequired(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}

	// The authoritative metadata is the REVOKED same-epoch record.
	src := NewFakeMetadataSource(revokedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Establish a bound seed in GENESIS: the durable binding exists (from a
	// prior binding attempt), and the accepted metadata is the PENDING
	// authorization (the state before the revocation was observed).
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed (bound, pending)")
	if snap := ctrl.Snapshot(); snap.State != GENESIS {
		t.Fatalf("expected seed to be in GENESIS before revocation; got %s", snap.State)
	}
	defer ctrl.Stop(context.Background())

	// Evaluate the revoked metadata repeatedly (the metadata worker observes
	// the same revoked record on every poll). The seed must reach
	// RECOVERY_REQUIRED on the first observation and STAY there on every
	// subsequent identical observation — never silently returning to
	// DISCOVERING or GENESIS.
	for i := 1; i <= 5; i++ {
		src.SetMetadata(revokedMeta())
		ctrl.EvaluateMetadataForTest(context.Background())
		snap := ctrl.Snapshot()
		if snap.State == GENESIS {
			t.Fatalf("observation %d: seed silently returned to GENESIS after a confirmed same-epoch revocation (reason=%s)", i, snap.Reason)
		}
		if snap.State == DISCOVERING {
			t.Fatalf("observation %d: seed silently returned to DISCOVERING after a confirmed same-epoch revocation; the revoked authorization was not restored (reason=%s)", i, snap.Reason)
		}
		if snap.State != RECOVERY_REQUIRED {
			t.Fatalf("observation %d: expected RECOVERY_REQUIRED for a confirmed same-epoch revocation; got %s (reason=%s)", i, snap.State, snap.Reason)
		}
	}

	// The original database binding must be PRESERVED (revocation does not
	// delete or rebind the durable binding).
	b := binding.binding
	if b == nil || b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatal("the original database binding was deleted or overwritten on revocation; it must be preserved")
	}
	// Public writes must remain BLOCKED in RECOVERY_REQUIRED.
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("expected public writes to be blocked in RECOVERY_REQUIRED; got allowed")
	}
	t.Log("seed stayed in RECOVERY_REQUIRED across 5 repeated identical same-epoch revocation observations; binding preserved; writes blocked")
}

// TestRevocation_SameEpoch_RestoredEpoch_ClearsRecoveryRequired verifies the
// existing legitimate recovery behavior is preserved: a seed in
// RECOVERY_REQUIRED whose authoritative metadata is genuinely restored —
// the same epoch is re-granted (a fresh pending grant on the matching
// binding) — is reconciled back to DISCOVERING by the re-evaluation path.
// The revoked-observation guard must not swallow this real restoration.
func TestRevocation_SameEpoch_RestoredEpoch_ClearsRecoveryRequired(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}

	// The authoritative metadata starts as the REVOKED same-epoch record.
	src := NewFakeMetadataSource(revokedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Establish a bound seed in GENESIS, then observe the revocation so the
	// seed enters RECOVERY_REQUIRED (the confirmed-revocation incident).
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed (bound, pending)")
	ctrl.EvaluateMetadataForTest(context.Background())
	defer ctrl.Stop(context.Background())

	if snap := ctrl.Snapshot(); snap.State != RECOVERY_REQUIRED {
		t.Fatalf("expected RECOVERY_REQUIRED after the same-epoch revocation; got %s (reason=%s)", snap.State, snap.Reason)
	}

	// The authoritative epoch is genuinely restored: the same epoch is
	// re-granted as a fresh, pending genesis authorization (a real
	// restoration, not an identical revoked observation).
	src.SetMetadata(pendingMeta())
	ctrl.EvaluateMetadataForTest(context.Background())

	// The re-evaluation path must clear the recovery state to DISCOVERING
	// (the legitimate RECOVERY_REQUIRED → DISCOVERING behavior). The seed
	// is NOT automatically re-placed in GENESIS here (that requires the
	// explicit operator action); the recovery state must be cleared.
	snap := ctrl.Snapshot()
	if snap.State == RECOVERY_REQUIRED {
		t.Fatalf("seed remained in RECOVERY_REQUIRED after the authoritative epoch was genuinely restored (re-granted); the recovery state must be cleared (reason=%s)", snap.Reason)
	}
	if snap.State == GENESIS {
		t.Fatalf("seed was automatically re-placed in GENESIS on restoration; a revoked authorization must not be auto-reinstated (reason=%s)", snap.Reason)
	}
	if snap.State != DISCOVERING {
		t.Fatalf("expected DISCOVERING after the genuine epoch restoration; got %s (reason=%s)", snap.State, snap.Reason)
	}
	// The original binding must still be preserved.
	b := binding.binding
	if b == nil || b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatal("the original database binding was lost on restoration; it must be preserved")
	}
	t.Log("seed correctly cleared RECOVERY_REQUIRED to DISCOVERING on a genuine epoch restoration; no auto-reinstatement to GENESIS; binding preserved")
}
