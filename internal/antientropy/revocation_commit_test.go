package antientropy

import (
	"context"
	"errors"
	"sync"
	"time"

	"gossipgrid/internal/store"
)

// NOTE: The commit-cursor linearization tests for the anti-entropy engine are
// in commit_guard_test.go, where they exercise the REAL lifecycle controller
// and the REAL production admission check (Admission.CheckReplicationOutbound)
// rather than a fake guard. The previous fake-guard variants
// (TestCommitCursorRevocationNonFinalPage, TestCommitCursorRevocationFullSweep,
// TestCommitCursorNoRevocation) relied on the admission re-check that used to
// run inside the commit callback; that re-check was removed because it
// recursively acquired the controller's RWMutex (a deadlock). The real
// controller's CommitUnderAuthorization evaluates the authorization once,
// non-recursively, before the callback, and the same linearization scenarios
// are now covered by TestRealControllerCommitCursor* in commit_guard_test.go.

// nthBarrierStore wraps a LocalStore and blocks only the Nth call to
// InsertReplicatedMessageReturningInserted (1-indexed). All other calls pass
// through immediately. It signals via enteredCh when the Nth call reaches the
// barrier, and the test releases the block via blockCh.
type nthBarrierStore struct {
	inner     LocalStore
	blockNth  int
	blockCh   chan struct{}
	enteredCh chan struct{}
	mu        sync.Mutex
	count     int
	once      sync.Once
}

func (s *nthBarrierStore) FindMissingIDs(ctx context.Context, ids []string) ([]string, error) {
	return s.inner.FindMissingIDs(ctx, ids)
}

func (s *nthBarrierStore) InsertReplicatedMessageReturningInserted(msg *store.Message) (bool, error) {
	s.mu.Lock()
	s.count++
	shouldBlock := s.count == s.blockNth
	s.mu.Unlock()

	if shouldBlock {
		// Signal that we have reached the barrier (before blocking).
		s.once.Do(func() {
			close(s.enteredCh)
		})
		// Block until the test releases us, or a timeout elapses (safety net).
		select {
		case <-s.blockCh:
		case <-time.After(30 * time.Second):
			return false, errors.New("barrier timeout (safety net)")
		}
	}

	return s.inner.InsertReplicatedMessageReturningInserted(msg)
}
