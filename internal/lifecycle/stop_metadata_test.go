package lifecycle

import (
	"context"
	"sync"
	"testing"
	"time"
)

// blockingMetadataSource is a MetadataSource that blocks in Read until the
// context is canceled or the test releases it via a channel.
type blockingMetadataSource struct {
	blockCh chan struct{}
	once    sync.Once
	mu      sync.Mutex
	blocked bool
}

func newBlockingMetadataSource() *blockingMetadataSource {
	return &blockingMetadataSource{
		blockCh: make(chan struct{}),
	}
}

func (s *blockingMetadataSource) Read(ctx context.Context) (ClusterMetadata, error) {
	s.mu.Lock()
	s.blocked = true
	s.mu.Unlock()

	select {
	case <-s.blockCh:
		// Released by test: return valid metadata.
		return ClusterMetadata{
			ClusterID:   "c1",
			EpochID:     "e1",
			Generation:  1,
			Initialized: true,
		}, nil
	case <-ctx.Done():
		// Context canceled: return error.
		return ClusterMetadata{}, ctx.Err()
	}
}

func (s *blockingMetadataSource) release() {
	s.once.Do(func() { close(s.blockCh) })
}

func (s *blockingMetadataSource) wasBlocked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.blocked
}

// TestStopDuringInitialMetadataRead verifies that Stop can interrupt a
// blocked initial metadata read. Start runs in a goroutine with a metadata
// source that blocks until context cancellation. Stop is called with a
// short deadline and must return within that deadline.
func TestStopDuringInitialMetadataRead(t *testing.T) {
	src := newBlockingMetadataSource()
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Start in a goroutine (the initial read will block).
	startErrCh := make(chan error, 1)
	go func() {
		startErrCh <- ctrl.Start(context.Background())
	}()

	// Wait for the metadata source to be blocked.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if src.wasBlocked() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !src.wasBlocked() {
		t.Fatal("metadata source did not enter blocked state within 2s")
	}

	// Call Stop with a 100ms deadline.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stopCancel()

	stopDone := make(chan struct{})
	stopStart := time.Now()
	go func() {
		ctrl.Stop(stopCtx)
		close(stopDone)
	}()

	select {
	case <-stopDone:
		elapsed := time.Since(stopStart)
		if elapsed > 500*time.Millisecond {
			t.Errorf("Stop took %v; expected < 500ms (context deadline is 100ms)", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return within 2s; it is blocked")
	}

	// The controller must be in the terminal stopped state.
	snap := ctrl.Authorization()
	if !snap.Stopped {
		t.Error("expected Stopped=true after Stop during initial read")
	}
	if snap.Authorized() {
		t.Error("expected Authorized=false after Stop")
	}

	// Wait for Start to complete (it should return nil or an error).
	select {
	case startErr := <-startErrCh:
		// Start returned (either nil or an error — both are acceptable).
		_ = startErr
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after Stop canceled its context")
	}
}

// TestStopDeadlineWhileMetadataSourceBlocked verifies that Stop honors the
// caller's shutdown deadline when the metadata source remains blocked
// (does not respond to context cancellation immediately).
func TestStopDeadlineWhileMetadataSourceBlocked(t *testing.T) {
	// Use a metadata source that ignores context cancellation (blocks forever
	// until the test releases it). This simulates a misbehaving source.
	src := &ignoringMetadataSource{
		blockCh:   make(chan struct{}),
		releaseCh: make(chan struct{}),
	}
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Start in a goroutine.
	startErrCh := make(chan error, 1)
	go func() {
		startErrCh <- ctrl.Start(context.Background())
	}()

	// Wait for the source to be blocked.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if src.wasBlocked() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !src.wasBlocked() {
		t.Fatal("metadata source did not enter blocked state within 2s")
	}

	// Call Stop with a 50ms deadline. The source ignores cancellation,
	// so Stop should return via the context deadline path.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stopCancel()

	stopStart := time.Now()
	ctrl.Stop(stopCtx)
	elapsed := time.Since(stopStart)

	// Stop should return within a reasonable time after the deadline.
	if elapsed > 1*time.Second {
		t.Errorf("Stop took %v; expected return within ~50ms of deadline", elapsed)
	}

	// The controller is in the terminal stopped state.
	snap := ctrl.Authorization()
	if !snap.Stopped {
		t.Error("expected Stopped=true")
	}

	// Release the source and wait for Start to complete (no goroutine leak).
	close(src.releaseCh)
	select {
	case startErr := <-startErrCh:
		_ = startErr
	case <-time.After(2 * time.Second):
		t.Fatal("Start goroutine leaked (did not return after source release)")
	}
}

// ignoringMetadataSource blocks in Read until the test releases it via
// releaseCh. It does NOT respond to context cancellation.
type ignoringMetadataSource struct {
	blockCh   chan struct{}
	releaseCh chan struct{}
	mu        sync.Mutex
	blocked   bool
}

func (s *ignoringMetadataSource) Read(ctx context.Context) (ClusterMetadata, error) {
	s.mu.Lock()
	s.blocked = true
	s.mu.Unlock()
	<-s.releaseCh
	return ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	}, nil
}

func (s *ignoringMetadataSource) wasBlocked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.blocked
}

// TestConcurrentStartAndStop verifies that concurrent Start and Stop cannot
// resurrect a terminally stopped controller.
func TestConcurrentStartAndStop(t *testing.T) {
	src := newBlockingMetadataSource()
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	var wg sync.WaitGroup
	startErrCh := make(chan error, 1)
	stopDone := make(chan struct{})

	wg.Add(2)
	go func() {
		defer wg.Done()
		startErrCh <- ctrl.Start(context.Background())
	}()
	go func() {
		defer wg.Done()
		ctrl.Stop(context.Background())
		close(stopDone)
	}()

	wg.Wait()

	// After both complete, the controller must be stopped and cannot be
	// restarted.
	snap := ctrl.Authorization()
	if !snap.Stopped {
		t.Error("expected Stopped=true after concurrent Start+Stop")
	}

	// A subsequent Start must fail.
	if err := ctrl.Start(context.Background()); err == nil {
		t.Error("Start after concurrent Start+Stop: expected error, got nil")
	}

	// Release the source to avoid goroutine leaks.
	src.release()
}

// TestStopBeforeStartPrompts verifies that Stop before Start returns promptly.
func TestStopBeforeStartPrompts(t *testing.T) {
	src := NewFakeMetadataSourceUnavailable()
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	start := time.Now()
	ctrl.Stop(context.Background())
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Errorf("Stop before Start took %v; expected prompt return", elapsed)
	}

	// Start after Stop must fail.
	if err := ctrl.Start(context.Background()); err == nil {
		t.Error("Start after Stop-before-Start: expected error, got nil")
	}
}

// TestConcurrentStopCallers verifies that multiple concurrent Stop callers
// are all safe and the controller ends in the stopped state.
func TestConcurrentStopCallers(t *testing.T) {
	src := NewFakeMetadataSource(ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	}, true)
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
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
		t.Error("expected Stopped=true after concurrent Stop callers")
	}
}

// TestMetadataSourceReturnsAfterCancellation verifies that when the metadata
// source returns valid metadata after the context was canceled (a late
// response), the controller remains in the stopped state and does not
// start the worker.
func TestMetadataSourceReturnsAfterCancellation(t *testing.T) {
	// Use a source that blocks until released, then returns valid metadata.
	// The test cancels via Stop before releasing, simulating a late response.
	src := &lateResponseMetadataSource{
		blockCh: make(chan struct{}),
		once:    sync.Once{},
	}
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	startErrCh := make(chan error, 1)
	go func() {
		startErrCh <- ctrl.Start(context.Background())
	}()

	// Wait for the source to be blocked.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if src.wasBlocked() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !src.wasBlocked() {
		t.Fatal("metadata source did not enter blocked state within 2s")
	}

	// Call Stop (which cancels the context).
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer stopCancel()
	ctrl.Stop(stopCtx)

	// Now release the source (simulating a late response after cancellation).
	src.release()

	// Wait for Start to return.
	select {
	case startErr := <-startErrCh:
		_ = startErr
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after late metadata response")
	}

	// The controller must be stopped (the late response did not resurrect it).
	snap := ctrl.Authorization()
	if !snap.Stopped {
		t.Error("expected Stopped=true after late metadata response post-Stop")
	}
	if snap.Authorized() {
		t.Error("expected Authorized=false after Stop; late response must not resurrect authorization")
	}
}

// lateResponseMetadataSource blocks until released, then returns valid metadata.
// It responds to context cancellation by returning an error, but if the
// context is NOT canceled (e.g., the test releases it after Stop), it
// returns the metadata.
type lateResponseMetadataSource struct {
	blockCh chan struct{}
	once    sync.Once
	mu      sync.Mutex
	blocked bool
}

func (s *lateResponseMetadataSource) Read(ctx context.Context) (ClusterMetadata, error) {
	s.mu.Lock()
	s.blocked = true
	s.mu.Unlock()

	select {
	case <-s.blockCh:
		// If the context was already canceled, return an error (the
		// evaluateMetadata caller will handle it). Otherwise return
		// valid metadata (a late response).
		if ctx.Err() != nil {
			return ClusterMetadata{}, ctx.Err()
		}
		return ClusterMetadata{
			ClusterID:   "c1",
			EpochID:     "e1",
			Generation:  1,
			Initialized: true,
		}, nil
	case <-ctx.Done():
		return ClusterMetadata{}, ctx.Err()
	}
}

func (s *lateResponseMetadataSource) release() {
	s.once.Do(func() { close(s.blockCh) })
}

func (s *lateResponseMetadataSource) wasBlocked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.blocked
}

// TestNoAuthorizationAfterTerminalStop verifies that after a terminal Stop,
// no subsequent operation can be authorized, even if metadata is updated.
func TestNoAuthorizationAfterTerminalStop(t *testing.T) {
	src := NewFakeMetadataSource(ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	}, true)
	binding := &fakeBindingStore{}
	if _, err := binding.Bind(context.Background(), "c1", "e1", 1, ProvenanceGenesis); err != nil {
		t.Fatalf("bind: %v", err)
	}
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctrl.Stop(context.Background())

	// Even if we force the state to READY and set fresh metadata,
	// the stopped controller must not authorize.
	ctrl.machine.ForceTransitionForTest(DISCOVERING, "test")
	if err := ctrl.machine.TransitionForTest(READY, "test"); err != nil {
		t.Fatalf("transition to READY: %v", err)
	}
	ctrl.SetAcceptedMetadataForTest(ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	})

	snap := ctrl.Authorization()
	if snap.Authorized() {
		t.Error("terminal Stop: Authorized must be false even with READY state and fresh metadata")
	}
	if !snap.Stopped {
		t.Error("expected Stopped=true")
	}

	// Admission checks must also reject.
	if d := ctrl.Admission().CheckWrite(context.Background()); d.Allowed {
		t.Error("terminal Stop: CheckWrite must be rejected")
	}
	if d := ctrl.Admission().CheckReplicationInbound(context.Background(), RequestContext{ClusterID: "c1", EpochID: "e1"}); d.Allowed {
		t.Error("terminal Stop: CheckReplicationInbound must be rejected")
	}
}

// TestDoneClosedExactlyOnce verifies that the done channel is closed exactly
// once for a valid lifecycle sequence (Start → Stop), and that a second
// Stop does not panic (double close).
func TestDoneClosedExactlyOnce(t *testing.T) {
	src := NewFakeMetadataSource(ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	}, true)
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctrl.Stop(context.Background())
	// Second Stop: must not panic (done is already closed; doneOnce guards it).
	ctrl.Stop(context.Background())
	// Third Stop: must not panic either.
	ctrl.Stop(context.Background())
}

// TestStartAfterStopFails verifies that Start after a terminal Stop fails
// with an error (the controller cannot be resurrected).
func TestStartAfterStopFails(t *testing.T) {
	src := NewFakeMetadataSourceUnavailable()
	binding := &fakeBindingStore{}
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctrl.Stop(context.Background())

	if err := ctrl.Start(context.Background()); err == nil {
		t.Error("Start after Stop: expected error, got nil")
	}
}
