package peertls

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
)

// TestBootstrapTimeout_IndependentRequestBudgets verifies that the initial
// message-fetch request and its partial-404 retry each get their own per-request
// timeout budget. The test sets a per-request timeout (300ms) and a handler that
// takes 250ms. If the two requests shared one timeout context, the initial
// request would consume 250ms of the 300ms budget, leaving only 50ms for the
// retry — which would time out. With independent budgets, the retry gets a fresh
// 300ms and succeeds. The test asserts the retry succeeds (1 message returned),
// proving the budgets are independent.
func TestBootstrapTimeout_IndependentRequestBudgets(t *testing.T) {
	ids := genTestUUIDs(2)

	var mu sync.Mutex
	callCount := 0

	// The handler takes 250ms (less than the 300ms per-request timeout, but
	// enough that a shared budget would starve the retry). The first call
	// returns a 404 with one ID missing; the second call (retry) returns
	// the present ID.
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		callCount++
		current := callCount
		mu.Unlock()
		// Sleep 250ms: within the 300ms per-request budget, but would
		// exceed the 50ms remaining if the budget were shared.
		time.Sleep(250 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
		w.Header().Set(lifecycle.HeaderRespEpochID, "e1")
		if current == 1 {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(missingIDsResponse{
				Error:      "message IDs not found: " + ids[0],
				MissingIDs: ids[:1],
			})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(fetchMessagesResponse{
			Messages: []SyncMessageWire{
				{ID: ids[1], Body: "body", Origin: "gossipgrid-0", CreatedAt: "2026-01-01T00:00:00Z"},
			},
		})
	})

	ca := newTestCA(t, "w")
	cert, key, trust := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, cert, key, trust, "gossipgrid-0", 50*time.Millisecond)
	a, stop := startTestServer(t, mgr, handler)
	t.Cleanup(stop)

	client := NewClient(mgr)
	client.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	})
	client.SetBootstrapEpochProvider(&testBootstrapEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		authorized:  true,
		enforcement: true,
	})
	// Set a per-request timeout of 300ms. Each request gets its own 300ms
	// budget. The handler takes 250ms per call, so with independent budgets
	// both requests succeed. If the budget were shared, the retry would
	// only have 50ms left and would time out.
	client.SetRequestTimeout(300 * time.Millisecond)

	pinnedCtx := &lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}

	// Use a generous parent deadline (5s) so the parent doesn't expire first.
	parentCtx, parentCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer parentCancel()

	start := time.Now()
	msgs, err := client.FetchBootstrapMessages(parentCtx, a, "gossipgrid-0."+testSuffix, ids, pinnedCtx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected success (independent budgets allow both requests within 300ms each), got: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 fetched message (the present ID from the retry), got %d", len(msgs))
	}

	// The critical assertion: both the initial request AND the retry were
	// attempted (handler called twice), and the retry succeeded. This proves
	// the retry got its own independent timeout budget rather than being
	// starved by the initial request's consumption of a shared budget.
	mu.Lock()
	gotCalls := callCount
	mu.Unlock()
	if gotCalls != 2 {
		t.Errorf("expected handler called exactly 2 times (initial + retry), got %d", gotCalls)
	}

	// Total elapsed should be ~500ms (2×250ms) and well under the parent
	// deadline of 5s.
	if elapsed > 2*time.Second {
		t.Errorf("total elapsed %v is too high; expected ~500ms for two 250ms requests", elapsed)
	}
}

// TestBootstrapTimeout_ParentCancellation verifies that when the caller's
// parent context is cancelled, both the initial request and the retry are
// aborted promptly (not waiting for the per-request timeout to expire).
func TestBootstrapTimeout_ParentCancellation(t *testing.T) {
	ids := genTestUUIDs(2)

	var mu sync.Mutex
	callCount := 0

	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		callCount++
		current := callCount
		mu.Unlock()
		// Sleep long enough to exceed the per-request timeout, so the
		// request is in-flight when cancellation arrives.
		time.Sleep(2 * time.Second)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
		w.Header().Set(lifecycle.HeaderRespEpochID, "e1")
		if current == 1 {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(missingIDsResponse{
				Error:      "message IDs not found: " + ids[0],
				MissingIDs: ids[:1],
			})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(fetchMessagesResponse{
			Messages: []SyncMessageWire{
				{ID: ids[1], Body: "body", Origin: "gossipgrid-0", CreatedAt: "2026-01-01T00:00:00Z"},
			},
		})
	})

	ca := newTestCA(t, "w")
	cert, key, trust := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, cert, key, trust, "gossipgrid-0", 50*time.Millisecond)
	a, stop := startTestServer(t, mgr, handler)
	t.Cleanup(stop)

	client := NewClient(mgr)
	client.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	})
	client.SetBootstrapEpochProvider(&testBootstrapEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		authorized:  true,
		enforcement: true,
	})
	// Set a long per-request timeout (5s) so the per-request budget is not
	// the limiting factor; the parent cancellation should abort the request.
	client.SetRequestTimeout(5 * time.Second)

	pinnedCtx := &lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}

	parentCtx, parentCancel := context.WithCancel(context.Background())

	// Cancel the parent context after 100ms, while the initial request is
	// in-flight (the handler sleeps 2s).
	go func() {
		time.Sleep(100 * time.Millisecond)
		parentCancel()
	}()

	start := time.Now()
	_, err := client.FetchBootstrapMessages(parentCtx, a, "gossipgrid-0."+testSuffix, ids, pinnedCtx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error (parent context cancelled), got nil")
	}
	if !containsCanceled(err) && !containsDeadlineExceeded(err) {
		t.Fatalf("expected context.Canceled or deadline exceeded, got: %v", err)
	}

	// The request should have been aborted well before the 2s handler sleep
	// completes and well before the 5s per-request timeout.
	if elapsed > 1*time.Second {
		t.Errorf("elapsed %v is too high; parent cancellation should abort within ~100ms", elapsed)
	}
}

// containsDeadlineExceeded checks if the error chain contains
// context.DeadlineExceeded.
func containsDeadlineExceeded(err error) bool {
	return err != nil && strings.Contains(err.Error(), context.DeadlineExceeded.Error())
}

// containsCanceled checks if the error chain contains context.Canceled.
func containsCanceled(err error) bool {
	return err != nil && strings.Contains(err.Error(), context.Canceled.Error())
}
