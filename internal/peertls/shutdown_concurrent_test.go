// Package peertls: deterministic concurrent Shutdown regression test.
//
// This test reproduces the defect where the first Manager.Shutdown caller
// waits for the reload worker to exit, but subsequent callers (who see
// state == 2) return immediately even when the reload goroutine is still
// running. The fix ensures every Shutdown caller waits for done, so all
// callers are blocked until the worker actually exits.
//
// Mechanism: a "block-all-but-first" slog.Handler lets the first log call
// (Start's "peer TLS credentials loaded" Info, which runs synchronously in
// the caller's goroutine) pass through, so Start completes and the reload
// worker is launched. All subsequent log calls (the worker's reload logs)
// block until unblock is closed. With a 5ms reload interval, the first
// reload tick fires within ~5ms of Start returning; the worker then blocks
// in the handler. The test waits (bounded) for the worker to enter the
// handler, starts two concurrent Shutdown calls, verifies both are blocked,
// then releases the worker.
package peertls

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockAllButFirstHandler is an slog.Handler that lets the first log call
// pass through (so Start's initial log completes and the worker launches),
// then blocks all subsequent log calls until unblock is closed. This
// deterministically blocks the reload goroutine in its first reload log.
type blockAllButFirstHandler struct {
	unblock   chan struct{}
	firstDone atomic.Bool
}

func (h *blockAllButFirstHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *blockAllButFirstHandler) Handle(_ context.Context, _ slog.Record) error {
	if h.firstDone.CompareAndSwap(false, true) {
		return nil // first call (Start's log) passes through
	}
	<-h.unblock // all subsequent calls (worker's reload logs) block
	return nil
}

func (h *blockAllButFirstHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *blockAllButFirstHandler) WithGroup(_ string) slog.Handler      { return h }

// TestConcurrentShutdownWaitsForWorker verifies that when the reload goroutine
// is blocked (via a block-all-but-first slog handler during a reload),
// concurrent Shutdown calls all remain blocked until the worker is released,
// and then all complete promptly.
func TestConcurrentShutdownWaitsForWorker(t *testing.T) {
	ca := newTestCA(t, "w")
	cert, key, trust := testCredentials(t, ca, "gossipgrid-0")

	unblock := make(chan struct{})
	logger := slog.New(&blockAllButFirstHandler{unblock: unblock})

	mgr := New(Config{
		CertFile:         cert,
		KeyFile:          key,
		TrustBundleFile:  trust,
		IdentitySuffix:   testSuffix,
		StatefulSetName:  testStatefulSet,
		NodeID:           "gossipgrid-0",
		ExpectedReplicas: 2,
		ReloadInterval:   5 * time.Millisecond,
	}, logger)

	// Start: the first log call passes through (firstDone set), so Start
	// completes and the reload worker is launched.
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait (bounded) for the worker to enter the blocking handler. With a
	// 5ms interval, the first tick fires within ~5ms; the reload calls
	// m.logger.Info("peer TLS credentials reloaded",...) which blocks on
	// unblock. 100ms is generous.
	time.Sleep(100 * time.Millisecond)

	// Now start two concurrent Shutdown calls. With the fix, BOTH must wait
	// for done. Without the fix, the second one returns immediately
	// (state==2, no wait) even though the worker is still blocked.
	var (
		wg        sync.WaitGroup
		returnedC = make(chan struct{}, 2)
		returned  atomic.Int32
	)

	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			mgr.Shutdown()
			returned.Add(1)
			returnedC <- struct{}{}
		}()
	}

	// If either Shutdown returns before we close unblock, the defect is
	// present: a caller didn't wait for the worker to exit.
	select {
	case <-returnedC:
		t.Fatal("a Shutdown call returned while the reload worker was still blocked; all Shutdown callers must wait for worker completion")
	case <-time.After(100 * time.Millisecond):
		// Good: neither Shutdown returned within 100ms; both are waiting
		// for the worker to exit.
	}

	// Release the worker.
	close(unblock)

	// Both Shutdown calls must complete promptly.
	deadline := time.Now().Add(5 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-returnedC:
		case <-time.After(time.Until(deadline)):
			t.Fatalf("Shutdown call %d did not complete after releasing the worker (returned=%d)", i, returned.Load())
		}
	}
	wg.Wait()
	if got := returned.Load(); got != 2 {
		t.Errorf("expected 2 Shutdown calls to return, got %d", got)
	}
}
