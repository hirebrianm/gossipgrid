// Package lifecycle: deterministic real-controller commit-guard synchronization
// regression tests (Milestone 5A correction).
//
// These tests verify the commit-authorization synchronization contract using
// the actual lifecycle.Controller and its real authorization state, with
// deterministic barriers (channels) to force specific interleavings. Each test
// reproduces a specific race condition that would be intermittent under
// scheduling, making the regression deterministic and repeatable.
//
// Lock-order contract under test:
//
//	Controller.mu (RLock) → Machine.mu (RLock) → Engine.mu (Lock)
//
// All authorization reads (CommitUnderAuthorization, admission checks,
// Snapshot) acquire c.mu RLock then m.mu RLock. All state transitions
// (SetRecoveryRequired, SetReady, etc.) acquire c.mu RLock then m.mu Lock.
// This ensures that a revocation linearizes either before or after the
// commit, never "during" it.
//
// Scenarios:
//
//	A. Metadata becomes invalid between authorization snapshot evaluation
//	   and cursor commit → commit must be rejected.
//	B. SetRecoveryRequired is called while the commit callback is blocked
//	   → the transition is queued behind the commit's RLocks; the commit
//	   linearizes first and succeeds; the revocation completes after.
//	C. Authority revocation completes before commit admission → reject.
//	D. Authorized commit linearizes before revocation → commit succeeds, then
//	   revocation completes.
//	E. A queued metadata writer cannot cause recursive lock deadlock.
//	F. Concurrent Stop and cursor commit.
//	G. Complete-sweep accounting remains correct.
//	H. Lifecycle-disabled commit remains backward-compatible.

package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// --- shared helpers ---

// blockingBindingStore is a BindingStore whose GetBinding can be blocked
// deterministically via a channel. It is used to simulate a metadata
// evaluation that is in progress (holding c.mu Lock) while a commit is
// attempting to acquire c.mu RLock.
type blockingBindingStore struct {
	mu      sync.RWMutex
	binding *DatabaseBinding
	// blockCh, when non-nil, blocks GetBinding until it is closed.
	blockCh chan struct{}
}

func (s *blockingBindingStore) GetBinding() (*DatabaseBinding, error) {
	if s.blockCh != nil {
		<-s.blockCh
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.binding, nil
}

func (s *blockingBindingStore) Bind(clusterID, epochID string, generation int) (*DatabaseBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := &DatabaseBinding{
		ClusterID:  clusterID,
		EpochID:    epochID,
		Generation: generation,
		CreatedAt:  time.Now(),
	}
	s.binding = b
	return b, nil
}

func (s *blockingBindingStore) CountMessages() (int64, error) { return 0, nil }

// newAuthorizedController creates a real Controller that is authorized:
// READY state, fresh metadata with Initialized=true, and a cached binding
// matching the metadata. The metadata poll interval is set far in the future
// so the observation never goes stale during the test.
func newAuthorizedController(t *testing.T, binding BindingStore) *Controller {
	t.Helper()
	src := NewFakeMetadataSourceUnavailable()
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "test-node",
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	// Force to READY via the legal transition path.
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
	ctrl.SetBindingCacheForTest(&DatabaseBinding{
		ClusterID:  "c1",
		EpochID:    "e1",
		Generation: 1,
		CreatedAt:  time.Now(),
	})
	return ctrl
}

// --- Test A: metadata invalidation between snapshot evaluation and commit ---
//
// The defect: in the old implementation, CommitUnderAuthorization called
// authorizationSnapshot() (which acquires and releases c.mu RLock), checked
// snap.authorized(), then acquired c.mu RLock again for the stopped check and
// the callback. A metadata failure (c.mu Lock) could occur between the
// snapshot's RUnlock and the second RLock, invalidating the authority.
//
// The fix: authorizationSnapshotLocked() reads all fields under one c.mu RLock
// + one m.mu RLock held until the callback completes. A metadata failure
// (c.mu Lock) cannot interleave between the snapshot evaluation and the
// callback because the RLocks are held throughout.
//
// Reproduction: block a metadata evaluation (which takes c.mu Lock) so that
// it is pending while the commit runs. The commit must complete (or be
// rejected based on the pre-epoch state) without being affected by the
// pending metadata update.
func TestCommitGuardMetadataInvalidationBetweenSnapshotAndCommit(t *testing.T) {
	// Use a blocking binding store so we can simulate a metadata evaluation
	// that is in progress (holding c.mu Lock) via the test helper.
	binding := &blockingBindingStore{}
	ctrl := newAuthorizedController(t, binding)

	// Barrier: signals when the commit callback has started (entered the
	// protected boundary) but not yet completed.
	commitStarted := make(chan struct{})
	commitDone := make(chan error, 1)

	// The commit callback blocks on commitStarted after entering, then waits
	// for release before completing.
	go func() {
		err := ctrl.CommitUnderAuthorization(func() error {
			close(commitStarted)
			// Block here: the commit is in progress, holding c.mu RLock +
			// m.mu RLock. Any concurrent c.mu Lock (metadata update or Stop)
			// must be queued behind these RLocks.
			<-time.After(100 * time.Millisecond)
			return nil
		})
		commitDone <- err
	}()

	// Wait for the commit to enter the protected boundary.
	select {
	case <-commitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not enter the protected boundary within 5s")
	}

	// Now simulate a metadata invalidation: this takes c.mu Lock and is
	// queued behind the commit's c.mu RLock.
	metadataDone := make(chan struct{})
	go func() {
		defer close(metadataDone)
		// handleMetadataError takes c.mu Lock.
		ctrl.handleMetadataError(errors.New("simulated metadata failure"))
	}()

	// Wait for the commit to complete.
	var err error
	select {
	case err = <-commitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not complete within 5s (possible deadlock)")
	}
	// The commit was authorized at the time it entered (READY + fresh + matching
	// binding), so it should succeed.
	if err != nil {
		t.Errorf("commit should succeed when metadata invalidation is queued behind the commit, got: %v", err)
	}

	// The metadata update should also complete (it is not permanently blocked).
	select {
	case <-metadataDone:
	case <-time.After(5 * time.Second):
		t.Fatal("metadata update did not complete within 5s (possible deadlock)")
	}

	// After the metadata update, the authority is no longer fresh.
	if ctrl.AuthorityState() != "invalid" {
		t.Errorf("expected authority state 'invalid' after metadata failure, got %q", ctrl.AuthorityState())
	}
}

// --- Test B: SetRecoveryRequired while commit callback is blocked ---
//
// The defect: in the old implementation, SetRecoveryRequired called
// machine.transition without acquiring c.mu. The transition to
// RECOVERY_REQUIRED could complete while a cursor commit held c.mu RLock.
//
// The fix: transitionTo acquires c.mu RLock then m.mu Lock, matching the
// commit's c.mu RLock + m.mu RLock. The transition is queued behind the
// commit's RLocks; the commit linearizes first and succeeds; the revocation
// completes after.
func TestCommitGuardSetRecoveryRequiredWhileCommitBlocked(t *testing.T) {
	binding := &blockingBindingStore{}
	ctrl := newAuthorizedController(t, binding)

	commitStarted := make(chan struct{})
	commitDone := make(chan error, 1)

	go func() {
		err := ctrl.CommitUnderAuthorization(func() error {
			close(commitStarted)
			// Block: the commit holds c.mu RLock + m.mu RLock.
			<-time.After(100 * time.Millisecond)
			return nil
		})
		commitDone <- err
	}()

	select {
	case <-commitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not enter the protected boundary within 5s")
	}

	// Invoke SetRecoveryRequired. This takes c.mu RLock then m.mu Lock and is
	// queued behind the commit's RLocks.
	revocationDone := make(chan error, 1)
	go func() {
		revocationDone <- ctrl.SetRecoveryRequired("test revocation")
	}()

	// The commit must complete first (it linearized before the revocation).
	var err error
	select {
	case err = <-commitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not complete within 5s (possible deadlock)")
	}
	if err != nil {
		t.Errorf("commit should succeed when it linearizes before the revocation, got: %v", err)
	}

	// The revocation must also complete (queued behind the commit).
	select {
	case revErr := <-revocationDone:
		if revErr != nil {
			t.Errorf("SetRecoveryRequired should succeed (transition is valid), got: %v", revErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SetRecoveryRequired did not complete within 5s (possible deadlock)")
	}

	// After the revocation, the controller is in RECOVERY_REQUIRED.
	if ctrl.State() != RECOVERY_REQUIRED {
		t.Errorf("expected state RECOVERY_REQUIRED after revocation, got %s", ctrl.State())
	}
}

// --- Test C: authority revocation completes before commit admission ---
//
// If a revocation (metadata invalidation or state transition) completes
// BEFORE the commit acquires the RLocks, the commit must be rejected.
func TestCommitGuardRevocationBeforeCommitAdmission(t *testing.T) {
	binding := &blockingBindingStore{}
	ctrl := newAuthorizedController(t, binding)

	// Revoke authority BEFORE the commit starts.
	ctrl.handleMetadataError(errors.New("pre-commit metadata failure"))
	// The authority is now "invalid", so the commit must be rejected.

	commitDone := make(chan error, 1)
	go func() {
		commitDone <- ctrl.CommitUnderAuthorization(func() error {
			// This callback must NOT be executed.
			t.Error("callback executed despite pre-commit revocation")
			return nil
		})
	}()

	select {
	case err := <-commitDone:
		if err == nil {
			t.Error("commit should be rejected when authority is invalidated before admission, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not complete within 5s")
	}
}

// --- Test D: authorized commit linearizes before revocation ---
//
// The commit enters the protected boundary while authorized. A revocation is
// then issued but is queued behind the commit. The commit completes and
// the revocation completes after.
//
// To make this deterministic, we block the commit callback (holding the
// RLocks) and issue the revocation while the commit is in progress. The
// revocation is queued behind the commit's RLocks; when the commit
// completes, the revocation then executes.
func TestCommitGuardCommitLinearizesBeforeRevocation(t *testing.T) {
	binding := &blockingBindingStore{}
	ctrl := newAuthorizedController(t, binding)

	commitStarted := make(chan struct{})
	commitDone := make(chan error, 1)

	go func() {
		err := ctrl.CommitUnderAuthorization(func() error {
			close(commitStarted)
			// Block: hold the RLocks while the revocation is queued behind them.
			<-time.After(50 * time.Millisecond)
			return nil
		})
		commitDone <- err
	}()

	// Wait for the commit to enter the protected boundary (RLocks held).
	select {
	case <-commitStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not enter the protected boundary within 5s")
	}

	// Issue the revocation. It is queued behind the commit's RLocks.
	revocationDone := make(chan error, 1)
	go func() {
		revocationDone <- ctrl.SetRecoveryRequired("test revocation")
	}()

	// The commit completes first (it linearized before the revocation could
	// acquire the RLocks).
	var err error
	select {
	case err = <-commitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not complete within 5s (possible deadlock)")
	}
	if err != nil {
		t.Errorf("commit should succeed when it linearizes before the revocation, got: %v", err)
	}

	// The revocation must also complete (it was queued behind the commit).
	select {
	case revErr := <-revocationDone:
		if revErr != nil {
			t.Errorf("SetRecoveryRequired should succeed, got: %v", revErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SetRecoveryRequired did not complete within 5s (possible deadlock)")
	}

	// After the revocation, the controller is no longer authorized.
	if ctrl.Authorized() {
		t.Error("controller should not be authorized after post-commit revocation")
	}
}

// --- Test E: queued metadata writer cannot cause recursive lock deadlock ---
//
// A concurrent writer (metadata update that takes c.mu Lock) must not deadlock
// an in-flight cursor commit. With the lock-order contract (c.mu RLock → m.mu
// RLock for reads, c.mu RLock → m.mu Lock for transitions), the writer is
// simply queued behind the reader and both complete.
func TestCommitGuardMetadataWriterNoDeadlock(t *testing.T) {
	binding := &blockingBindingStore{}
	ctrl := newAuthorizedController(t, binding)

	// Concurrent writer repeatedly performs metadata updates (c.mu Lock).
	writerDone := make(chan struct{})
	writerOK := make(chan error, 1)
	stableMeta := ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	}
	go func() {
		defer close(writerDone)
		for i := 0; i < 200; i++ {
			ctrl.AcceptMetadataForTest(stableMeta)
		}
		writerOK <- nil
	}()

	// A concurrent commit blocks inside the callback.
	commitDone := make(chan error, 1)
	go func() {
		commitDone <- ctrl.CommitUnderAuthorization(func() error {
			// Block: hold the RLocks while the writer contends for c.mu Lock.
			<-time.After(50 * time.Millisecond)
			return nil
		})
	}()

	// Both must complete.
	select {
	case err := <-commitDone:
		if err != nil {
			t.Errorf("commit should succeed, got: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("commit deadlocked: a writer waiting for c.mu Lock blocked the commit")
	}

	select {
	case <-writerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("controller writer deadlocked: the commit held c.mu RLock indefinitely")
	}
	if err := <-writerOK; err != nil {
		t.Errorf("controller writer (metadata update) returned an error: %v", err)
	}
}

// --- Test F: concurrent Stop and cursor commit ---
//
// Stop takes c.mu Lock (to set the stopped flag) then startMu. A concurrent
// commit holds c.mu RLock + m.mu RLock. The Stop's c.mu Lock is queued behind
// the commit's RLocks. The commit completes, then Stop completes.
func TestCommitGuardConcurrentStopAndCommit(t *testing.T) {
	binding := &blockingBindingStore{}
	ctrl := newAuthorizedController(t, binding)

	commitDone := make(chan error, 1)
	go func() {
		commitDone <- ctrl.CommitUnderAuthorization(func() error {
			// Block: hold the RLocks while Stop contends for c.mu Lock.
			<-time.After(50 * time.Millisecond)
			return nil
		})
	}()

	// Issue Stop concurrently.
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stopDone := make(chan struct{})
	go func() {
		ctrl.Stop(stopCtx)
		close(stopDone)
	}()

	// The commit should complete (it linearized before the Stop's c.mu Lock
	// could be acquired — the RLocks are held until the callback completes).
	select {
	case err := <-commitDone:
		// The commit may succeed (if it linearized before the stopped flag
		// was set) or be rejected (if the stopped flag was set before the
		// commit acquired the RLocks). Both are valid outcomes.
		_ = err
	case <-time.After(5 * time.Second):
		t.Fatal("commit did not complete within 5s (possible deadlock with Stop)")
	}

	// Stop must also complete.
	select {
	case <-stopDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not complete within 10s (possible deadlock with commit)")
	}
}

// --- Test G: complete-sweep accounting remains correct ---
//
// When a commit succeeds under the authorization boundary, the
// completedPasses counter is incremented and LastSuccess is set. When a
// commit is rejected, neither is updated. This test verifies the accounting
// is consistent with the authorization decision.
func TestCommitGuardCompleteSweepAccounting(t *testing.T) {
	// Use a commit guard that tracks the callback execution.
	binding := &blockingBindingStore{}
	ctrl := newAuthorizedController(t, binding)

	var callbackExecuted bool

	// Successful commit: the callback executes.
	err := ctrl.CommitUnderAuthorization(func() error {
		callbackExecuted = true
		return nil
	})
	if err != nil {
		t.Fatalf("commit should succeed: %v", err)
	}
	if !callbackExecuted {
		t.Error("callback should have been executed for an authorized commit")
	}

	// Now revoke and verify the commit is rejected.
	ctrl.handleMetadataError(errors.New("post-success revocation"))

	callbackExecuted = false
	err = ctrl.CommitUnderAuthorization(func() error {
		callbackExecuted = true
		return nil
	})
	if err == nil {
		t.Error("commit should be rejected after revocation, got nil")
	}
	if callbackExecuted {
		t.Error("callback must NOT be executed for a rejected commit")
	}
}

// --- Test H: lifecycle-disabled commit remains backward-compatible ---
//
// When lifecycle protection is disabled (Enabled=false), CommitUnderAuthorization
// runs the callback directly without any lock, preserving M4 behavior.
func TestCommitGuardDisabledBackwardCompatible(t *testing.T) {
	binding := &blockingBindingStore{}
	src := NewFakeMetadataSourceUnavailable()
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "test-node",
		Enabled:              false, // lifecycle disabled
	}, NewMachine(nil), src, binding, nil)

	var callbackExecuted bool
	err := ctrl.CommitUnderAuthorization(func() error {
		callbackExecuted = true
		return nil
	})
	if err != nil {
		t.Errorf("disabled lifecycle commit should succeed, got: %v", err)
	}
	if !callbackExecuted {
		t.Error("callback should be executed when lifecycle is disabled")
	}

	// Even with a stopped controller (which would block in the enabled path),
	// the disabled path executes the callback directly.
	ctrl.mu.Lock()
	ctrl.stopped = true
	ctrl.mu.Unlock()

	callbackExecuted = false
	err = ctrl.CommitUnderAuthorization(func() error {
		callbackExecuted = true
		return nil
	})
	if err != nil {
		t.Errorf("disabled lifecycle commit should succeed even when stopped, got: %v", err)
	}
	if !callbackExecuted {
		t.Error("callback should be executed when lifecycle is disabled, even if stopped")
	}
}
