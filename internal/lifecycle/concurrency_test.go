package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakeBindingStore is a concurrency-safe fake for the BindingStore interface.
type fakeBindingStore struct {
	mu      sync.RWMutex
	binding *DatabaseBinding
}

func (s *fakeBindingStore) GetBinding() (*DatabaseBinding, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.binding, nil
}

func (s *fakeBindingStore) Bind(_ context.Context, clusterID, epochID string, generation int, provenance BindingProvenance) (*DatabaseBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.binding != nil && (s.binding.ClusterID != clusterID || s.binding.EpochID != epochID) {
		return nil, errors.New("binding mismatch")
	}
	b := &DatabaseBinding{ClusterID: clusterID, EpochID: epochID, Generation: generation, CreatedAt: time.Now(), Provenance: provenance}
	s.binding = b
	return b, nil
}

func (s *fakeBindingStore) CountMessages() (int64, error) { return 0, nil }

// TestStopAuthorizationRace verifies that concurrent calls to Authorization()
// and Stop() do not race on the stopped flag. The stopped flag is written
// under c.mu and read under c.mu in the authorization snapshot, so the
// race detector must not fire.
func TestStopAuthorizationRace(t *testing.T) {
	src := NewFakeMetadataSourceUnavailable()
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var wg sync.WaitGroup
	const readers = 20
	const stoppers = 5

	// Concurrent authorization readers.
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = ctrl.Authorization()
				_ = ctrl.Authorized()
				_ = ctrl.Snapshot()
				_ = ctrl.State()
			}
		}()
	}

	// Concurrent Stop calls (idempotent; multiple goroutines call Stop).
	for i := 0; i < stoppers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctrl.Stop(context.Background())
		}()
	}

	wg.Wait()

	// After Stop, authorization must be denied.
	snap := ctrl.Authorization()
	if snap.Stopped {
		if snap.Authorized() {
			t.Error("stopped controller: Authorized() should be false")
		}
	} else {
		t.Error("stopped controller: Stopped should be true after Stop")
	}
}

// TestStopBeforeStart verifies that Stop before Start returns promptly and
// establishes the terminal stopped state (so admission rejects all operations).
func TestStopBeforeStart(t *testing.T) {
	src := NewFakeMetadataSourceUnavailable()
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, &fakeBindingStore{}, nil)

	// Stop before Start.
	ctrl.Stop(context.Background())

	// Start after Stop must fail.
	if err := ctrl.Start(context.Background()); err == nil {
		t.Error("Start after Stop should fail")
	}

	// Authorization must be denied (stopped).
	snap := ctrl.Authorization()
	if !snap.Stopped {
		t.Error("Stop-before-Start: Stopped should be true")
	}
	if snap.Authorized() {
		t.Error("Stop-before-Start: Authorized should be false")
	}
}

// TestConcurrentStopIdempotent verifies that multiple concurrent Stop calls
// are all safe (no panic, no deadlock) and the controller ends in the
// stopped state.
func TestConcurrentStopIdempotent(t *testing.T) {
	src := NewFakeMetadataSourceUnavailable()
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: 50 * time.Millisecond,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctrl.Stop(context.Background())
		}()
	}
	wg.Wait()

	snap := ctrl.Authorization()
	if !snap.Stopped {
		t.Error("expected stopped after concurrent Stop calls")
	}
}

// TestAuthorizedInitializedFalse verifies that a metadata record with
// Initialized=false must never authorize ordinary READY operations, even when
// the state is READY and the binding matches the metadata.
func TestAuthorizedInitializedFalse(t *testing.T) {
	src := NewFakeMetadataSourceUnavailable()
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Set the state to READY via the test-only path.
	ctrl.machine.ForceTransitionForTest(DISCOVERING, "test")
	if err := ctrl.machine.TransitionForTest(READY, "test"); err != nil {
		t.Fatalf("transition to READY: %v", err)
	}

	// Bind the database to the cluster/epoch.
	if _, err := binding.Bind(context.Background(), "c1", "e1", 1, ProvenanceGenesis); err != nil {
		t.Fatalf("bind: %v", err)
	}

	// Set metadata with Initialized=false.
	ctrl.SetAcceptedMetadataForTest(ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: false,
	})

	snap := ctrl.Authorization()
	if snap.State != READY {
		t.Fatalf("state: expected READY, got %s", snap.State)
	}
	if snap.Authorized() {
		t.Error("metadata with Initialized=false: Authorized() must be false (fail-closed)")
	}
}

// TestAuthorizedInitializedTrue verifies that a metadata record with
// Initialized=true CAN authorize when the state is READY and the binding
// matches.
func TestAuthorizedInitializedTrue(t *testing.T) {
	src := NewFakeMetadataSourceUnavailable()
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	ctrl.machine.ForceTransitionForTest(DISCOVERING, "test")
	if err := ctrl.machine.TransitionForTest(READY, "test"); err != nil {
		t.Fatalf("transition to READY: %v", err)
	}

	if _, err := binding.Bind(context.Background(), "c1", "e1", 1, ProvenanceGenesis); err != nil {
		t.Fatalf("bind: %v", err)
	}

	ctrl.SetAcceptedMetadataForTest(ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	})

	snap := ctrl.Authorization()
	if !snap.Authorized() {
		t.Errorf("metadata with Initialized=true, matching binding, READY state: expected Authorized, reason: %s", snap.Reason)
	}
}

// TestSetReadyRequiresInitialized verifies that SetReady rejects promotion
// when the authoritative metadata reports Initialized=false.
func TestSetReadyRequiresInitialized(t *testing.T) {
	src := NewFakeMetadataSourceUnavailable()
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	ctrl.machine.ForceTransitionForTest(DISCOVERING, "test")

	if _, err := binding.Bind(context.Background(), "c1", "e1", 1, ProvenanceGenesis); err != nil {
		t.Fatalf("bind: %v", err)
	}

	// Metadata with Initialized=false.
	ctrl.SetAcceptedMetadataForTest(ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: false,
	})

	// SetReady must fail: Initialized=false.
	if err := ctrl.SetReady("test promotion"); err == nil {
		t.Error("SetReady with Initialized=false metadata: expected error, got nil")
	}

	// Now set Initialized=true and retry.
	ctrl.SetAcceptedMetadataForTest(ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	})

	if err := ctrl.SetReady("test promotion"); err != nil {
		t.Errorf("SetReady with Initialized=true: expected success, got %v", err)
	}
	if ctrl.State() != READY {
		t.Errorf("state after SetReady: expected READY, got %s", ctrl.State())
	}
}

// TestSetReady_UnfinaledGenesisMetaRejected verifies that SetReady rejects
// initial promotion when a seed is designated but the authoritative metadata
// is only initialized and NOT completely finalized (the finalized-genesis
// invariant is not met). The rejection must not change the lifecycle state.
func TestSetReady_UnfinaledGenesisMetaRejected(t *testing.T) {
	src := NewFakeMetadataSourceUnavailable()
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	ctrl.machine.ForceTransitionForTest(DISCOVERING, "test")

	if _, err := binding.Bind(context.Background(), "c1", "e1", 1, ProvenanceGenesis); err != nil {
		t.Fatalf("bind: %v", err)
	}

	// Initialized=true but NOT a complete finalization (GenesisConsumer="",
	// DataLossProtected=false, GenesisAuthorized=true).
	ctrl.SetAcceptedMetadataForTest(ClusterMetadata{
		ClusterID:         "c1",
		EpochID:           "e1",
		Generation:        1,
		Initialized:       true,
		GenesisAuthorized: true,
	})

	before := ctrl.State()
	if err := ctrl.SetReady("test promotion"); err == nil {
		t.Error("SetReady with unfinalized genesis metadata (seed designated): expected error, got nil")
	}
	if ctrl.State() != before {
		t.Errorf("state changed after rejected SetReady: before=%s after=%s (must be unchanged)", before, ctrl.State())
	}
}

// TestSetReady_UnknownProvenanceDegradedRejected verifies that SetReady
// rejects a DEGRADED → READY recovery when the binding's provenance is
// explicitly-unknown (legacy) or out-of-range: an unknown provenance is
// rejected in ALL states, including DEGRADED. The rejection must not change
// the lifecycle state.
func TestSetReady_UnknownProvenanceDegradedRejected(t *testing.T) {
	tt := []struct {
		name       string
		provenance BindingProvenance
	}{
		{"legacy (explicitly-unknown) provenance in DEGRADED", ProvenanceLegacy},
		{"out-of-range provenance in DEGRADED", BindingProvenance(99)},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			src := NewFakeMetadataSourceUnavailable()
			bstore := &fakeBindingStoreWithMessages{binding: &DatabaseBinding{
				ClusterID:  "c1",
				EpochID:    "e1",
				Generation: 1,
				CreatedAt:  time.Now(),
				Provenance: tc.provenance,
			}}
			// No designated seed: the finalized-genesis invariant is not
			// enforced (preserving 5A compatibility), so the provenance
			// allowlist is the gate under test.
			ctrl := NewController(Config{
				MetadataPollInterval: 100 * time.Millisecond,
				NodeID:               "gossipgrid-1",
				Enabled:              true,
			}, NewMachine(nil), src, bstore, nil)

			// DEGRADED: only reachable via READY → DEGRADED in the legal
			// table; ForceTransitionForTest bypasses the table.
			ctrl.machine.ForceTransitionForTest(READY, "test")
			if err := ctrl.machine.TransitionForTest(DEGRADED, "test"); err != nil {
				t.Fatalf("transition to DEGRADED: %v", err)
			}

			// Fresh, valid, initialized metadata (no designated seed, so
			// finalization is not a factor).
			ctrl.SetAcceptedMetadataForTest(ClusterMetadata{
				ClusterID:   "c1",
				EpochID:     "e1",
				Generation:  1,
				Initialized: true,
			})

			before := ctrl.State()
			if err := ctrl.SetReady("test recovery"); err == nil {
				t.Errorf("SetReady with %s provenance in DEGRADED: expected error, got nil", tc.name)
			}
			if ctrl.State() != before {
				t.Errorf("state changed after rejected SetReady: before=%s after=%s (must be unchanged)", before, ctrl.State())
			}
		})
	}
}

// TestSetReady_JoinProvenanceSyncingRejected verifies that SetReady rejects a
// join-provenance binding in the SYNCING state: a join-origin replica cannot
// achieve initial READY promotion during Stage 3A (synchronization is not yet
// complete). The rejection must not change the lifecycle state.
func TestSetReady_JoinProvenanceSyncingRejected(t *testing.T) {
	src := NewFakeMetadataSourceUnavailable()
	bstore := &fakeBindingStoreWithMessages{binding: &DatabaseBinding{
		ClusterID:  "c1",
		EpochID:    "e1",
		Generation: 1,
		CreatedAt:  time.Now(),
		Provenance: ProvenanceJoin,
	}}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		Enabled:              true,
	}, NewMachine(nil), src, bstore, nil)

	ctrl.machine.ForceTransitionForTest(SYNCING, "test")

	ctrl.SetAcceptedMetadataForTest(ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	})

	before := ctrl.State()
	if err := ctrl.SetReady("test promotion"); err == nil {
		t.Error("SetReady with join provenance in SYNCING: expected error, got nil")
	}
	if ctrl.State() != before {
		t.Errorf("state changed after rejected SetReady: before=%s after=%s (must be unchanged)", before, ctrl.State())
	}
}

// TestSetReady_GenesisRecoveryPositiveControl verifies that a valid
// genesis-provenance binding in DEGRADED is promoted to READY (the positive
// control for the DEGRADED recovery path). The recovery is permitted because
// the binding is recognized (genesis) and the replica was previously READY.
func TestSetReady_GenesisRecoveryPositiveControl(t *testing.T) {
	src := NewFakeMetadataSourceUnavailable()
	bstore := &fakeBindingStoreWithMessages{binding: &DatabaseBinding{
		ClusterID:  "c1",
		EpochID:    "e1",
		Generation: 1,
		CreatedAt:  time.Now(),
		Provenance: ProvenanceGenesis,
	}}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		Enabled:              true,
	}, NewMachine(nil), src, bstore, nil)

	ctrl.machine.ForceTransitionForTest(READY, "test")
	if err := ctrl.machine.TransitionForTest(DEGRADED, "test"); err != nil {
		t.Fatalf("transition to DEGRADED: %v", err)
	}

	ctrl.SetAcceptedMetadataForTest(ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	})

	if err := ctrl.SetReady("test recovery"); err != nil {
		t.Errorf("SetReady with genesis provenance in DEGRADED: expected success, got %v", err)
	}
	if ctrl.State() != READY {
		t.Errorf("state after recovery SetReady: expected READY, got %s", ctrl.State())
	}
}
