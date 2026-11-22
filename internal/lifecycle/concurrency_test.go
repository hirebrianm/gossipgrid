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

func (s *fakeBindingStore) Bind(clusterID, epochID string, generation int) (*DatabaseBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.binding != nil && (s.binding.ClusterID != clusterID || s.binding.EpochID != epochID) {
		return nil, errors.New("binding mismatch")
	}
	b := &DatabaseBinding{ClusterID: clusterID, EpochID: epochID, Generation: generation, CreatedAt: time.Now()}
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
	if _, err := binding.Bind("c1", "e1", 1); err != nil {
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

	if _, err := binding.Bind("c1", "e1", 1); err != nil {
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

	if _, err := binding.Bind("c1", "e1", 1); err != nil {
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
