package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// genesisSeed is the designated seed identity used by the genesis tests
// (a StatefulSet ordinal 0 pod, e.g. "gossipgrid-0").
const genesisSeed = "gossipgrid-0"

// testClusterID / testEpochID are the authoritative identities for the
// genesis tests (matching the metadata fixtures).
const (
	testClusterID = "0190e8a2-6b0c-7000-8000-000000000000"
	testEpochID   = "0190e8a3-1c2d-7000-9000-111111111111"
)

// fakeBindingStoreWithMessages is a concurrency-safe fake for the
// BindingStore interface that simulates a local database with a configurable
// message count (for the "unbound legacy messages" and "empty database"
// cases) and a configurable binding (for the "already bound" and "conflicting
// binding" cases).
type fakeBindingStoreWithMessages struct {
	mu           sync.RWMutex
	binding      *DatabaseBinding
	messageCount int64
	bindErr      error
}

func (s *fakeBindingStoreWithMessages) GetBinding() (*DatabaseBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.binding, nil
}

func (s *fakeBindingStoreWithMessages) Bind(_ context.Context, clusterID, epochID string, generation int, provenance BindingProvenance) (*DatabaseBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.bindErr != nil {
		return nil, s.bindErr
	}
	// A conflict: a pre-existing binding to a different cluster/epoch.
	if s.binding != nil && (s.binding.ClusterID != clusterID || s.binding.EpochID != epochID) {
		return nil, errors.New("binding conflict")
	}
	// A nonempty, unbound database: Bind fails (ErrBindingNonempty).
	if s.binding == nil && s.messageCount > 0 {
		return nil, errors.New("database is nonempty without a binding")
	}
	// Idempotent: a matching binding already exists (same cluster/epoch).
	// Preserve the existing binding unchanged (no overwrite, no relabel).
	// This mirrors the real Store.Bind idempotency contract.
	if s.binding != nil {
		return s.binding, nil
	}
	// An empty, unbound database: create the binding with the given provenance.
	b := &DatabaseBinding{ClusterID: clusterID, EpochID: epochID, Generation: generation, CreatedAt: time.Now(), Provenance: provenance}
	s.binding = b
	return b, nil
}

func (s *fakeBindingStoreWithMessages) CountMessages() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.messageCount, nil
}

// pendingMeta returns the authoritative metadata for an outstanding,
// unconsumed genesis authorization (Initialized=false, GenesisAuthorized=
// true, GenesisConsumer="", DataLossProtected=false).
func pendingMeta() ClusterMetadata {
	return ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              testEpochID,
		Generation:           1,
		Initialized:          false,
		GenesisAuthorized:    true,
		GenesisConsumer:      "",
		EverAcceptedMessages: false,
		DataLossProtected:    false,
	}
}

// finalizedMeta returns the authoritative metadata for a finalized cluster
// (Initialized=true, GenesisAuthorized=false, GenesisConsumer=<seed>,
// DataLossProtected=true).
func finalizedMeta() ClusterMetadata {
	return ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              testEpochID,
		Generation:           1,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      genesisSeed,
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
}

// TestAuthorizeGenesis_NonSeedIsNoOp verifies that AuthorizeGenesis is a
// no-op for a replica whose NodeID does NOT exactly equal the configured
// SeedIdentity (the non-seed case). It does not read the metadata, does not
// attempt a bind, and does not transition the state (the existing DISCOVERING
// state is preserved). This is the key safety property: only the designated
// seed (gossipgrid-0) may take the genesis path; no other replica may enter
// GENESIS.
func TestAuthorizeGenesis_NonSeedIsNoOp(t *testing.T) {
	src := NewFakeMetadataSource(pendingMeta(), true)
	binding := &fakeBindingStoreWithMessages{}
	// The NodeID is NOT the seed (a different ordinal).
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Manually set the accepted metadata (a fresh, authorized observation) so
	// the test exercises the path without waiting for the metadata worker.
	ctrl.AcceptMetadataForTest(pendingMeta())

	// AuthorizeGenesis must be a no-op (the replica is not the seed).
	if err := ctrl.AuthorizeGenesis(context.Background()); err != nil {
		t.Fatalf("AuthorizeGenesis (non-seed): expected a no-op (nil error), got: %v", err)
	}
	// The state must be unchanged (DISCOVERING or STARTING; the non-seed
	// replica does not enter GENESIS).
	snap := ctrl.Snapshot()
	if snap.State == GENESIS {
		t.Fatalf("non-seed replica entered GENESIS; expected the state to be unchanged (got %s)", snap.State)
	}
	// No binding must have been attempted (the non-seed replica does not
	// bind the local database).
	if binding.binding != nil {
		t.Fatal("non-seed replica: expected no binding, got one")
	}
}

// TestAuthorizeGenesis_SeedNoFreshMetadata verifies that AuthorizeGenesis
// fails closed for a seed replica when the authoritative metadata is not
// fresh and valid (the "no fresh, authorized metadata" case). It does not
// bind the local database and does not transition the state. This is the
// "an empty database or missing peers alone must never authorize genesis"
// property: a seed with an empty database but no fresh, authorized
// observation cannot enter GENESIS.
func TestAuthorizeGenesis_SeedNoFreshMetadata(t *testing.T) {
	// The metadata source is unavailable (no fresh observation).
	src := NewFakeMetadataSourceUnavailable()
	binding := &fakeBindingStoreWithMessages{}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// The authority state is "missing" (no observation has been accepted).
	// AuthorizeGenesis must fail closed (it requires a fresh, valid,
	// authorized observation).
	if err := ctrl.AuthorizeGenesis(context.Background()); err == nil {
		t.Fatal("AuthorizeGenesis (seed, no fresh metadata): expected a fail-closed error, got nil")
	}
	// No binding must have been attempted.
	if binding.binding != nil {
		t.Fatal("expected no binding (no fresh metadata), got one")
	}
}

// TestAuthorizeGenesis_SeedBindsAndEntersGenesis verifies the happy path: a
// seed replica with a fresh, valid, authorized observation (Initialized=
// false, GenesisAuthorized=true) and an empty, unbound local database binds
// the local database to the authoritative cluster/epoch and enters GENESIS.
// The seed remains in GENESIS while Initialized=false (it is not promoted to
// READY; SetReady must not succeed before finalization).
func TestAuthorizeGenesis_SeedBindsAndEntersGenesis(t *testing.T) {
	src := NewFakeMetadataSource(pendingMeta(), true)
	binding := &fakeBindingStoreWithMessages{} // empty, unbound DB.
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	// A fresh, valid, authorized observation is accepted.
	ctrl.AcceptMetadataForTest(pendingMeta())
	// The metadata worker transitions STARTING → DISCOVERING (the first
	// authorization case: no local binding, metadata available, cluster not
	// yet initialized). Model this so the seed is in DISCOVERING before
	// AuthorizeGenesis is called (the production flow).
	ctrl.Machine().ForceTransitionForTest(DISCOVERING, "test: post-metadata")

	// AuthorizeGenesis must bind the local database and enter GENESIS.
	if err := ctrl.AuthorizeGenesis(context.Background()); err != nil {
		t.Fatalf("AuthorizeGenesis (seed, fresh authorized metadata, empty DB): %v", err)
	}
	// The state must be GENESIS (the seed is authorized and bound, but not
	// yet finalized; Initialized=false, so it is not promoted to READY).
	snap := ctrl.Snapshot()
	if snap.State != GENESIS {
		t.Fatalf("expected state GENESIS; got %s (reason=%s)", snap.State, snap.Reason)
	}
	// The binding must be present and match the authoritative metadata.
	b := binding.binding
	if b == nil {
		t.Fatal("expected the local database to be bound, got nil")
	}
	if b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatalf("expected the binding to match (cluster=%s, epoch=%s); got cluster=%s, epoch=%s", testClusterID, testEpochID, b.ClusterID, b.EpochID)
	}
	// The controller's binding cache must also be set.
	if ctrl.LocalBinding() == nil {
		t.Fatal("expected the controller's binding cache to be set, got nil")
	}
	// Public writes must remain blocked: the replica is in GENESIS (not a
	// ready state), and Initialized=false prevents READY. The admission
	// check must deny writes.
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("expected public writes to be blocked (GENESIS state, not authorized); got allowed")
	}
}

// TestAuthorizeGenesis_SeedConflictingBinding verifies that AuthorizeGenesis
// fails closed for a seed replica when the local database already has a
// binding that CONFLICTS with the authoritative metadata (a different
// cluster/epoch). It does not delete or modify the existing binding and does
// not transition the state (the existing binding is preserved). This is the
// "no conflicting authoritative epoch" property: a seed with a binding to a
// different cluster/epoch cannot be re-bound to the new authorization.
func TestAuthorizeGenesis_SeedConflictingBinding(t *testing.T) {
	src := NewFakeMetadataSource(pendingMeta(), true)
	// The local database already has a binding to a DIFFERENT cluster/epoch
	// (a conflict).
	conflicting := &DatabaseBinding{ClusterID: "other-cluster", EpochID: "other-epoch", Generation: 9, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: conflicting}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	ctrl.AcceptMetadataForTest(pendingMeta())
	// Model the production flow: the metadata worker transitions STARTING →
	// DISCOVERING before AuthorizeGenesis is called.
	ctrl.Machine().ForceTransitionForTest(DISCOVERING, "test: post-metadata")

	// AuthorizeGenesis must fail closed (the existing binding conflicts with
	// the authoritative metadata).
	if err := ctrl.AuthorizeGenesis(context.Background()); err == nil {
		t.Fatal("AuthorizeGenesis (seed, conflicting binding): expected a fail-closed error, got nil")
	}
	// The existing binding must be UNCHANGED (no deletion, no modification).
	if binding.binding != conflicting || binding.binding.ClusterID != "other-cluster" {
		t.Fatal("expected the existing conflicting binding to be unchanged; it was modified")
	}
	// The state must be unchanged (not GENESIS; the seed did not enter
	// genesis).
	snap := ctrl.Snapshot()
	if snap.State == GENESIS {
		t.Fatal("seed with a conflicting binding entered GENESIS; expected the state to be unchanged")
	}
}

// TestAuthorizeGenesis_SeedUnboundLegacyMessages verifies that
// AuthorizeGenesis fails closed for a seed replica when the local database is
// NONEMPTY (has messages) but UNBOUND (no binding). It does not delete or
// modify the messages and does not transition the state. This is the "a DB
// with unbound legacy messages" property: a seed with an existing, unbound
// message store cannot be (re)bound to the genesis authorization (that would
// silently reinitialize an existing cluster).
func TestAuthorizeGenesis_SeedUnboundLegacyMessages(t *testing.T) {
	src := NewFakeMetadataSource(pendingMeta(), true)
	// The local database is NONEMPTY (has messages) but UNBOUND (no binding).
	binding := &fakeBindingStoreWithMessages{messageCount: 42} // 42 unbound messages.
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	ctrl.AcceptMetadataForTest(pendingMeta())
	// Model the production flow: the metadata worker transitions STARTING →
	// DISCOVERING before AuthorizeGenesis is called.
	ctrl.Machine().ForceTransitionForTest(DISCOVERING, "test: post-metadata")

	// AuthorizeGenesis must fail closed (the local database is nonempty
	// without a binding).
	if err := ctrl.AuthorizeGenesis(context.Background()); err == nil {
		t.Fatal("AuthorizeGenesis (seed, unbound legacy messages): expected a fail-closed error, got nil")
	}
	// The message count must be UNCHANGED (no deletion, no modification).
	n, _ := binding.CountMessages()
	if n != 42 {
		t.Fatalf("expected the 42 unbound messages to be unchanged; got %d", n)
	}
	// The state must be unchanged (not GENESIS).
	snap := ctrl.Snapshot()
	if snap.State == GENESIS {
		t.Fatal("seed with unbound legacy messages entered GENESIS; expected the state to be unchanged")
	}
}

// TestAuthorizeGenesis_SeedAlreadyBoundIsIdempotent verifies the safe,
// idempotent-restart case: a seed replica that is already bound to the
// authoritative metadata (a matching binding) is a no-op (it does not re-bind
// and does not transition the state; the existing GENESIS state is
// preserved). This is the "matching binding → no-op" property: a seed that
// has already bound (e.g. across a pod restart) does not re-bind or re-enter
// genesis.
func TestAuthorizeGenesis_SeedAlreadyBoundIsIdempotent(t *testing.T) {
	src := NewFakeMetadataSource(pendingMeta(), true)
	// The local database already has a binding that MATCHES the
	// authoritative metadata.
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	// The seed is already in GENESIS (it bound on a prior attempt).
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	// Force the state to GENESIS (bypassing the transition table; the seed
	// was bound on a prior attempt and is in GENESIS).
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed")

	// AuthorizeGenesis must be a no-op (the existing binding matches the
	// authoritative metadata; the seed is already in GENESIS).
	if err := ctrl.AuthorizeGenesis(context.Background()); err != nil {
		t.Fatalf("AuthorizeGenesis (seed, already bound): expected a no-op (nil error), got: %v", err)
	}
	// The binding must be UNCHANGED (no re-bind).
	if binding.binding != matching {
		t.Fatal("expected the existing matching binding to be unchanged; it was modified")
	}
	// The state must be UNCHANGED (still GENESIS).
	snap := ctrl.Snapshot()
	if snap.State != GENESIS {
		t.Fatalf("expected the state to remain GENESIS (idempotent); got %s", snap.State)
	}
}

// TestPromoteFromGenesis_BoundSeedObservingInitialized verifies the
// promotion path: a bound GENESIS seed that observes a fresh, valid
// authoritative metadata with Initialized=true (the finalization has
// occurred) is promoted to READY by the metadata worker (via
// reevaluateState → promoteFromGenesis). The promotion requires a fresh
// observation (a stale or invalid observation does NOT promote), a matching
// binding, and Initialized=true. This is the "the seed is promoted to READY
// when it observes the finalization through a fresh metadata read" property.
func TestPromoteFromGenesis_BoundSeedObservingInitialized(t *testing.T) {
	// The local database is bound to the authoritative metadata (the seed
	// has already bound).
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	// The metadata source returns the finalized observation (Initialized=
	// true), so EvaluateMetadataForTest reads it and reaches
	// reevaluateState (the promotion path) rather than failing on an
	// unavailable source.
	src := NewFakeMetadataSource(finalizedMeta(), true)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	// The seed is in GENESIS (it bound on a prior attempt).
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	// Force the state to GENESIS (bypassing the transition table; the seed
	// was bound on a prior attempt and is in GENESIS).
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed")

	// The seed is in GENESIS (not yet promoted; Initialized=false).
	snap := ctrl.Snapshot()
	if snap.State != GENESIS {
		t.Fatalf("expected the seed to be in GENESIS before finalization; got %s", snap.State)
	}

	// The finalization occurs (the authoritative metadata is now
	// Initialized=true). The seed observes it through a fresh metadata read
	// (EvaluateMetadataForTest simulates the metadata worker's read).
	ctrl.EvaluateMetadataForTest(context.Background())

	// The seed must be promoted to READY (it observed Initialized=true with
	// a matching binding and a fresh observation).
	snap = ctrl.Snapshot()
	if snap.State != READY {
		t.Fatalf("expected the seed to be promoted to READY after observing the finalization; got %s (reason=%s)", snap.State, snap.Reason)
	}
	// Public writes must now be allowed (the replica is in READY with a
	// fresh, valid, matching observation, a matching binding, and
	// Initialized=true).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if !decision.Allowed {
		t.Fatalf("expected public writes to be allowed after promotion to READY; got denied (reason=%s)", decision.Reason)
	}
}

// TestPromoteFromGenesis_StaleObservationDoesNotPromote verifies that a bound
// GENESIS seed that observes a STALE (or invalid) observation with
// Initialized=true is NOT promoted to READY. The promotion requires a FRESH
// observation (a stale observation is not evidence of current authority).
// This is the "an expired/invalid observation cannot authorize" property.
func TestPromoteFromGenesis_StaleObservationDoesNotPromote(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), NewFakeMetadataSourceUnavailable(), binding, nil)
	// The seed is in GENESIS.
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(finalizedMeta())
	// Force the state to GENESIS (bypassing the transition table; the seed
	// was bound on a prior attempt and is in GENESIS).
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed")

	// Simulate a STALE observation: the freshAt is far in the past (older
	// than 2x the poll interval), so the observation is stale. The metadata
	// is Initialized=true, but the observation is stale.
	ctrl.SetAcceptedMetadataForTest(finalizedMeta())
	// Set the freshAt to a time far in the past (stale).
	ctrl.mu.Lock()
	ctrl.freshAt = time.Now().Add(-10 * time.Minute) // far older than 2x 100ms.
	ctrl.mu.Unlock()

	// The seed observes the (stale) Initialized=true observation.
	ctrl.EvaluateMetadataForTest(context.Background())

	// The seed must NOT be promoted to READY (the observation is stale; a
	// stale observation does not authorize the promotion).
	snap := ctrl.Snapshot()
	if snap.State == READY {
		t.Fatal("bound GENESIS seed was promoted to READY on a stale observation; expected it to remain in GENESIS (or transition to a non-ready state)")
	}
}

// TestPromoteFromGenesis_RevocationGoesToRecoveryRequired verifies the
// revocation path: a bound GENESIS seed whose binding NO LONGER matches the
// authoritative metadata (e.g. the operator revoked the authorization by
// changing the epoch, or a different seed bound to a different cluster)
// transitions to RECOVERY_REQUIRED (the Case 3 mismatch path in
// reevaluateState). This is the "a GENESIS seed whose binding no longer
// matches (revocation) → RECOVERY_REQUIRED" property.
func TestPromoteFromGenesis_RevocationGoesToRecoveryRequired(t *testing.T) {
	matching := &DatabaseBinding{ClusterID: testClusterID, EpochID: testEpochID, Generation: 1, CreatedAt: time.Now()}
	binding := &fakeBindingStoreWithMessages{binding: matching}
	// A revocation: the authoritative metadata now has a DIFFERENT epoch
	// (the operator reset the epoch, or a different seed bound to a
	// different cluster). The seed's binding (the original epoch) no longer
	// matches. The metadata source returns the revoked observation, so
	// EvaluateMetadataForTest reaches reevaluateState (the Case 3 mismatch
	// path) rather than failing on an unavailable source.
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
	// The seed is in GENESIS, bound to the original cluster/epoch.
	ctrl.SetBindingCacheForTest(matching)
	ctrl.SetAcceptedMetadataForTest(pendingMeta())
	// Force the state to GENESIS (bypassing the transition table; the seed
	// was bound on a prior attempt and is in GENESIS).
	ctrl.Machine().ForceTransitionForTest(GENESIS, "test: pre-seed")

	// The seed observes the revoked observation through a fresh metadata
	// read (EvaluateMetadataForTest simulates the metadata worker's read).
	ctrl.EvaluateMetadataForTest(context.Background())

	// The seed must transition to RECOVERY_REQUIRED (its binding no longer
	// matches the authoritative metadata; the Case 3 mismatch path).
	snap := ctrl.Snapshot()
	if snap.State != RECOVERY_REQUIRED {
		t.Fatalf("expected the seed to transition to RECOVERY_REQUIRED on revocation (binding mismatch); got %s (reason=%s)", snap.State, snap.Reason)
	}
	// Public writes must be blocked (RECOVERY_REQUIRED is a fail-closed
	// state).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("expected public writes to be blocked in RECOVERY_REQUIRED; got allowed")
	}
}

// TestAuthorizeGenesis_Concurrent is a race test: concurrent calls to
// AuthorizeGenesis (from the genesis-entry loop, which may call it
// periodically) and concurrent Snapshot/Authorization reads do not race on
// the controller's binding cache or state. The race detector must not fire.
// This verifies that the AuthorizeGenesis method (which acquires the
// controller's write lock only to update the binding cache and transition
// state) is safe for concurrent use with the metadata worker and the
// diagnostics endpoints.
func TestAuthorizeGenesis_Concurrent(t *testing.T) {
	src := NewFakeMetadataSource(pendingMeta(), true)
	binding := &fakeBindingStoreWithMessages{}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	ctrl.AcceptMetadataForTest(pendingMeta())
	// Model the production flow: the metadata worker transitions STARTING →
	// DISCOVERING before the genesis-entry loop's AuthorizeGenesis attempts
	// begin.
	ctrl.Machine().ForceTransitionForTest(DISCOVERING, "test: post-metadata")

	var wg sync.WaitGroup
	const workers = 8
	// Concurrent AuthorizeGenesis calls (simulating the genesis-entry loop's
	// periodic attempts, which may overlap).
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = ctrl.AuthorizeGenesis(context.Background())
			}
		}()
	}
	// Concurrent snapshot/authorization readers (the diagnostics endpoints).
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = ctrl.Snapshot()
				_ = ctrl.Authorization()
				_ = ctrl.LocalBinding()
				_ = ctrl.State()
			}
		}()
	}
	wg.Wait()

	// After the concurrent attempts, the seed must be bound and in GENESIS
	// (the first AuthorizeGenesis to run bound it; the rest are idempotent
	// no-ops). The state must be consistent.
	snap := ctrl.Snapshot()
	if snap.State != GENESIS {
		t.Fatalf("expected the seed to be in GENESIS after concurrent AuthorizeGenesis attempts; got %s", snap.State)
	}
	if binding.binding == nil {
		t.Fatal("expected the local database to be bound after concurrent AuthorizeGenesis attempts")
	}
}
