// Package main: real end-to-end bootstrap baseline integration test
// (Milestone 5B, Stage 2). These tests use independent SQLite stores,
// distinct workload certificates, real TLS 1.3 mTLS, the production peer
// handlers, the production peer client, the production anti-entropy engine,
// and an injected deterministic authoritative metadata source.
//
// The test verifies the full bootstrap fetch flow: a SYNCING requester with
// a real authorized READY source, real mTLS, real lifecycle controllers,
// and real independent SQLite stores. The production bootstrap wiring
// (SetBootstrapAdmissionCheck, SetBootstrapAuthorizationSource,
// SetBootstrapGuard) is shared between this test and app startup via the
// wireBootstrapEngine helper.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/peertls"
	"gossipgrid/internal/store"
)

// testMetadata returns the canonical test metadata for the bootstrap
// integration tests (a single cluster/epoch shared by all nodes).
func testMetadata() lifecycle.ClusterMetadata {
	return lifecycle.ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	}
}

// sourceIdentity returns the mTLS identity of the source node (gossipgrid-0)
// in the integration test's internal DNS suffix. The peer client validates
// this against the certificate's identity policy.
func sourceIdentity() string {
	return "gossipgrid-0." + intSuffix
}

// seedSourceStore inserts n messages into the source store and returns their
// IDs. All IDs are valid UUIDv7.
func seedSourceStore(t *testing.T, st *store.Store, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("019a0000-0000-7000-0000-%012d", i+1)
		ids = append(ids, id)
		_, err := st.InsertReplicatedMessageReturningInserted(&store.Message{
			ID:        id,
			Body:      fmt.Sprintf("body-%d", i),
			Origin:    "gossipgrid-0.test",
			CreatedAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("seeding source store (record %d): %v", i, err)
		}
	}
	return ids
}

// newBootstrapNodePair creates two lifecycle test nodes with real mTLS:
// a READY source and a SYNCING requester (bound to the same cluster/epoch).
// The requester's anti-entropy engine is wired with the production bootstrap
// wiring (shared with app startup). aEngineRequestTimeout, when non-zero,
// overrides the requester's anti-entropy engine RequestTimeout before engine
// creation (so a test can align the engine and client timeouts for a
// regression check).
func newBootstrapNodePair(t *testing.T, ca *testCA) (*lifecycleTestNode, *lifecycleTestNode) {
	return newBootstrapNodePairWithTimeout(t, ca, 0)
}

// newBootstrapNodePairWithTimeout is like newBootstrapNodePair but with a
// configurable anti-entropy engine RequestTimeout for the requester.
func newBootstrapNodePairWithTimeout(t *testing.T, ca *testCA, aEngineRequestTimeout time.Duration) (*lifecycleTestNode, *lifecycleTestNode) {
	t.Helper()

	meta := testMetadata()

	// READY source: metadata available, bound to c1/e1, forced to READY.
	source := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{
		bindTo:     &meta,
		forceState: lifecycle.READY,
		engines:    true,
	})
	source.setMetadata(testMetadata())

	// SYNCING requester: metadata available, bound to c1/e1, forced to SYNCING.
	reqMeta := testMetadata()
	requester := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{
		bindTo:                &reqMeta,
		forceState:            lifecycle.SYNCING,
		engines:               true,
		aEngineRequestTimeout: aEngineRequestTimeout,
	})
	requester.setMetadata(testMetadata())

	// Wire the requester's anti-entropy engine with the production bootstrap
	// wiring (shared with app startup).
	wireBootstrapEngine(requester.aEngine, requester.ctrl)

	// Wire the anti-entropy peer source so the requester's engine knows the
	// source's address.
	requester.setAntiEntropyPeer(source)

	return source, requester
}

// --- Test: successful bootstrap of a full 128-ID inventory page ---

// TestBootstrapE2E_FullPage verifies the complete bootstrap flow: a SYNCING
// requester fetches a full 128-ID inventory page from a READY source with an
// 8-ID engine fetch batch size (16 batches). Every fetch batch succeeds, all
// expected retained records are committed locally, and the requester remains
// SYNCING (no self-promotion).
func TestBootstrapE2E_FullPage(t *testing.T) {
	ca := newTestCA(t, "bootstrap-e2e")
	source, requester := newBootstrapNodePair(t, ca)

	// Seed the source's store with 128 messages.
	ids := seedSourceStore(t, source.store, 128)

	res := requester.aEngine.FetchBootstrapBaseline(context.Background(), source.peerAddr, sourceIdentity(), "", 128)

	if res.Error != nil {
		t.Fatalf("FetchBootstrapBaseline error: %v", res.Error)
	}
	if !res.Completed {
		t.Fatalf("expected page to be Completed, got: Inserted=%d AlreadyPresent=%d Expired=%d Conflict=%d",
			len(res.Inserted), len(res.AlreadyPresent), len(res.Expired), len(res.Conflict))
	}
	if len(res.Inventoried) != 128 {
		t.Errorf("expected 128 inventoried IDs, got %d", len(res.Inventoried))
	}
	if len(res.Inserted) != 128 {
		t.Errorf("expected 128 inserted, got %d", len(res.Inserted))
	}
	if len(res.AlreadyPresent) != 0 {
		t.Errorf("expected 0 already present, got %d", len(res.AlreadyPresent))
	}
	if len(res.Expired) != 0 {
		t.Errorf("expected 0 expired, got %d", len(res.Expired))
	}
	if len(res.Conflict) != 0 {
		t.Errorf("expected 0 conflicts, got %d", len(res.Conflict))
	}

	// Verify the requester's store now has all 128 records.
	for _, id := range ids {
		msg, err := requester.store.GetByID(id)
		if err != nil {
			t.Errorf("requester store missing record %s: %v", id, err)
			break
		}
		if msg == nil {
			t.Errorf("requester store record %s is nil", id)
			break
		}
	}

	// The requester must remain SYNCING (bootstrap does NOT promote to READY).
	state := requester.ctrl.Machine().State()
	if state != lifecycle.SYNCING {
		t.Errorf("requester state after bootstrap = %s, want SYNCING", state.String())
	}

	// After successful bootstrap installation, the production-wired SYNCING
	// requester must remain unable to perform ordinary replication (outbound
	// gossip) or public writes: the admission checks reject SYNCING state.
	admission := requester.ctrl.Admission()
	repDecision := admission.CheckReplicationOutbound(context.Background())
	if repDecision.Allowed {
		t.Error("SYNCING requester should NOT be allowed to perform ordinary replication after bootstrap")
	}
	writeDecision := admission.CheckWrite(context.Background())
	if writeDecision.Allowed {
		t.Error("SYNCING requester should NOT be allowed to perform public writes after bootstrap")
	}
}

// --- Test: unauthorized requester (unbound) cannot bootstrap ---

// TestBootstrapE2E_UnauthorizedUnbound verifies that an unbound requester
// (no database binding) cannot perform a production-wired bootstrap fetch:
// the admission check rejects the request before any network I/O.
func TestBootstrapE2E_UnauthorizedUnbound(t *testing.T) {
	ca := newTestCA(t, "bootstrap-unbound")
	meta := testMetadata()
	source := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{
		bindTo:     &meta,
		forceState: lifecycle.READY,
		engines:    true,
	})
	source.setMetadata(testMetadata())

	// Requester WITHOUT a binding (no bindTo): the store has no database
	// binding, so the bootstrap admission check must reject the request.
	requester := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{
		forceState: lifecycle.SYNCING,
		engines:    true,
	})
	requester.setMetadata(testMetadata())
	wireBootstrapEngine(requester.aEngine, requester.ctrl)
	requester.setAntiEntropyPeer(source)

	res := requester.aEngine.FetchBootstrapBaseline(context.Background(), source.peerAddr, sourceIdentity(), "", 8)

	if res.Error == nil {
		t.Fatalf("expected error for unbound requester, got nil (Inserted=%d)", len(res.Inserted))
	}
	if res.Completed {
		t.Errorf("unbound requester should not complete a bootstrap page")
	}
}

// --- Test: ineligible source (SYNCING) rejected ---

// TestBootstrapE2E_IneligibleSource verifies that a SYNCING source is rejected
// as a bootstrap source: the server-side admission check requires a ready
// state, and SYNCING is not ready. The test seeds the SYNCING source with
// retained data (so the false-positive condition — an empty inventory passing
// the test — is eliminated) and requires an explicit 503 admission rejection.
// No data is installed and the page is not completed.
func TestBootstrapE2E_IneligibleSource(t *testing.T) {
	ca := newTestCA(t, "bootstrap-ineligible")

	srcMeta := testMetadata()
	source := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{
		bindTo:     &srcMeta,
		forceState: lifecycle.SYNCING,
		engines:    true,
	})
	source.setMetadata(testMetadata())

	// Seed the SYNCING source with retained data. This eliminates the
	// false-positive condition where an empty inventory (no data to serve)
	// could pass the test even if the server-side admission check were
	// broken.
	ids := seedSourceStore(t, source.store, 8)

	reqMeta := testMetadata()
	requester := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{
		bindTo:     &reqMeta,
		forceState: lifecycle.SYNCING,
		engines:    true,
	})
	requester.setMetadata(testMetadata())
	wireBootstrapEngine(requester.aEngine, requester.ctrl)
	requester.setAntiEntropyPeer(source)

	res := requester.aEngine.FetchBootstrapBaseline(context.Background(), source.peerAddr, sourceIdentity(), "", 8)

	// The source is SYNCING (not ready): the server-side admission check
	// rejects the request with HTTP 503. The error must be a
	// PeerHTTPStatusError with status 503 (explicit admission rejection).
	if res.Error == nil {
		t.Fatalf("expected explicit admission rejection from SYNCING source (it has %d seeded records), got nil", len(ids))
	}
	var httpErr *peertls.PeerHTTPStatusError
	if !errors.As(res.Error, &httpErr) {
		t.Fatalf("expected *peertls.PeerHTTPStatusError (503 admission rejection), got %T: %v", res.Error, res.Error)
	}
	if httpErr.StatusCode() != 503 {
		t.Errorf("expected HTTP 503 (unready/no-metadata rejection), got %d", httpErr.StatusCode())
	}
	// No data should be installed.
	if len(res.Inserted) > 0 {
		t.Errorf("no records should be installed from an ineligible source, got %d inserted", len(res.Inserted))
	}
	// The page must NOT be completed.
	if res.Completed {
		t.Fatal("page must NOT be completed when the source rejects the request")
	}
	// The requester's store must have no records (no data was installed).
	for _, id := range ids {
		if msg, _ := requester.store.GetByID(id); msg != nil {
			t.Errorf("requester store has record %s from an ineligible source", id)
			break
		}
	}
}

// --- Test: epoch mismatch rejected ---

// TestBootstrapE2E_EpochMismatch verifies that a requester bound to epoch e1
// cannot bootstrap from a source in epoch e2: the transport rejects the
// request (409 epoch mismatch) and no records are installed.
func TestBootstrapE2E_EpochMismatch(t *testing.T) {
	ca := newTestCA(t, "bootstrap-epoch-mismatch")

	// Source in epoch e2.
	altMeta := testMetadata()
	altMeta.EpochID = "e2"
	source := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{
		bindTo:     &altMeta,
		forceState: lifecycle.READY,
		engines:    true,
	})
	source.setMetadata(altMeta)

	// Requester in epoch e1.
	reqMeta := testMetadata()
	requester := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{
		bindTo:     &reqMeta,
		forceState: lifecycle.SYNCING,
		engines:    true,
	})
	requester.setMetadata(testMetadata())
	wireBootstrapEngine(requester.aEngine, requester.ctrl)
	requester.setAntiEntropyPeer(source)

	// Seed the source with retained data (eliminates the false-positive
	// condition where an empty inventory could pass the test even if the
	// epoch-mismatch rejection were broken).
	ids := seedSourceStore(t, source.store, 8)

	res := requester.aEngine.FetchBootstrapBaseline(context.Background(), source.peerAddr, sourceIdentity(), "", 8)

	// The source is in epoch e2, the requester is in epoch e1: the
	// transport rejects the request with HTTP 409 (epoch mismatch). The
	// error must be a PeerHTTPStatusError with status 409 (explicit
	// epoch-mismatch rejection).
	if res.Error == nil {
		t.Fatalf("expected explicit epoch-mismatch rejection (source has %d seeded records), got nil", len(ids))
	}
	var httpErr *peertls.PeerHTTPStatusError
	if !errors.As(res.Error, &httpErr) {
		t.Fatalf("expected *peertls.PeerHTTPStatusError (409 epoch mismatch), got %T: %v", res.Error, res.Error)
	}
	if httpErr.StatusCode() != 409 {
		t.Errorf("expected HTTP 409 (epoch mismatch), got %d", httpErr.StatusCode())
	}
	// No data should be installed.
	if len(res.Inserted) > 0 {
		t.Errorf("no records should be installed on epoch mismatch, got %d", len(res.Inserted))
	}
	// The page must NOT be completed.
	if res.Completed {
		t.Fatal("page must NOT be completed on epoch mismatch")
	}
	// The requester's store must have no records from the source.
	for _, id := range ids {
		if msg, _ := requester.store.GetByID(id); msg != nil {
			t.Errorf("requester store has record %s from an epoch-mismatched source", id)
			break
		}
	}
}

// --- Test: context cancellation honored ---

// TestBootstrapE2E_Cancellation verifies that a context cancellation during
// the bootstrap fetch is honored: the fetch returns an error, no records are
// committed, and the requester remains SYNCING.
func TestBootstrapE2E_Cancellation(t *testing.T) {
	ca := newTestCA(t, "bootstrap-cancel")
	source, requester := newBootstrapNodePair(t, ca)
	seedSourceStore(t, source.store, 16)

	// Cancel the context before the fetch starts.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res := requester.aEngine.FetchBootstrapBaseline(ctx, source.peerAddr, sourceIdentity(), "", 8)

	if res.Error == nil {
		t.Fatalf("expected error for cancelled context, got nil")
	}
	if len(res.Inserted) > 0 {
		t.Errorf("no records should be committed on cancellation, got %d", len(res.Inserted))
	}

	// Requester remains SYNCING.
	state := requester.ctrl.Machine().State()
	if state != lifecycle.SYNCING {
		t.Errorf("requester state after cancellation = %s, want SYNCING", state.String())
	}
}

// --- Test: lifecycle-disabled compatibility ---

// TestBootstrapE2E_LifecycleDisabled verifies that with no bootstrap wiring
// set on the requester's engine (lifecycle protection disabled), the engine's
// FetchBootstrapBaseline falls back to legacy behavior: no admission check,
// no pinned context, no revalidation. The key assertion is that the
// requester's engine does not reject the fetch at its own admission boundary.
func TestBootstrapE2E_LifecycleDisabled(t *testing.T) {
	ca := newTestCA(t, "bootstrap-legacy")
	meta := testMetadata()
	source := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{
		bindTo:     &meta,
		forceState: lifecycle.READY,
		engines:    true,
	})
	source.setMetadata(testMetadata())

	// Requester with NO bootstrap wiring (the engine's bootstrapAdmissionCheck
	// is nil, so bootstrapAdmissionOK always returns nil — legacy passthrough).
	requester := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{
		engines: true,
	})
	requester.setAntiEntropyPeer(source)

	seedSourceStore(t, source.store, 8)

	res := requester.aEngine.FetchBootstrapBaseline(context.Background(), source.peerAddr, sourceIdentity(), "", 8)

	// With no bootstrap wiring, the requester's engine does not fail at its
	// own admission boundary. The server may reject the request (the source's
	// controller is enabled and the request has no epoch headers), but the
	// error must NOT be a local admission rejection.
	if res.Error != nil && len(res.Inventoried) == 0 && containsAdmissionError(res.Error.Error()) {
		t.Fatalf("lifecycle-disabled requester should not be rejected at its own admission boundary: %v", res.Error)
	}
}

// --- Test: retention honored (expired messages skipped) ---

// TestBootstrapE2E_Retention verifies that when a retention TTL is set on the
// requester's engine, messages older than the threshold are skipped (Expired)
// and not installed.
func TestBootstrapE2E_Retention(t *testing.T) {
	ca := newTestCA(t, "bootstrap-retention")
	source, requester := newBootstrapNodePair(t, ca)

	// Set a 1-hour retention TTL on the requester's engine.
	requester.aEngine.SetRetentionTTL(time.Hour)

	// Seed the source with 8 messages, 4 of which are expired (created 2 hours ago).
	expiredIDs := make([]string, 0, 4)
	validIDs := make([]string, 0, 4)
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("019a0000-0000-7000-0000-%012d", i+1)
		msg := &store.Message{
			ID:        id,
			Body:      fmt.Sprintf("body-%d", i),
			Origin:    "gossipgrid-0.test",
			CreatedAt: time.Now(),
		}
		if i < 4 {
			msg.CreatedAt = time.Now().Add(-2 * time.Hour)
			expiredIDs = append(expiredIDs, id)
		} else {
			validIDs = append(validIDs, id)
		}
		_, _ = source.store.InsertReplicatedMessageReturningInserted(msg)
	}

	res := requester.aEngine.FetchBootstrapBaseline(context.Background(), source.peerAddr, sourceIdentity(), "", 8)

	if res.Error != nil {
		t.Fatalf("FetchBootstrapBaseline error: %v", res.Error)
	}
	if !res.Completed {
		t.Fatalf("expected page to be Completed")
	}
	if len(res.Expired) != 4 {
		t.Errorf("expected 4 expired, got %d: %v", len(res.Expired), res.Expired)
	}
	if len(res.Inserted) != 4 {
		t.Errorf("expected 4 inserted (non-expired), got %d: %v", len(res.Inserted), res.Inserted)
	}
	// The expired IDs must not be in the requester's store.
	for _, id := range expiredIDs {
		if msg, _ := requester.store.GetByID(id); msg != nil {
			t.Errorf("expired record %s should not be installed", id)
		}
	}
	// The valid IDs must be in the requester's store.
	for _, id := range validIDs {
		if msg, _ := requester.store.GetByID(id); msg == nil {
			t.Errorf("valid record %s should be installed", id)
		}
	}
}

// --- Test: per-HTTP-request timeout ownership (Issue 2 regression) ---

// TestBootstrapE2E_PerRequestTimeoutOwnership verifies that the per-HTTP-request
// timeout is owned by the peertls client (not shared across multiple HTTP
// operations by the engine). The test injects a 700ms delay into the source's
// HTTP handler so each message-fetch HTTP operation takes ~700ms. Both the
// requester's peer client and the anti-entropy engine are configured with a
// 1s RequestTimeout. The bootstrap flow performs 2 message-fetch HTTP
// operations (initial + partial-404 retry), so the combined duration (~1.4s)
// exceeds 1s (one RequestTimeout), but each individual operation (~700ms)
// stays within its own 1s budget.
//
// Regression guard: if the engine were to wrap the entire
// FetchBootstrapMessages call in its own 1s deadline (a shared engine-level
// timeout around the initial request AND the retry), the combined 1.4s would
// exceed that shared budget and the second operation would fail with a
// deadline-exceeded error. After the fix, the engine passes the page-level
// parent context (no shared request-level deadline) to the client, and the
// client applies its own 1s timeout independently to each network operation.
// The test exercises:
// (1) successful inventory retrieval, (2) initial message-fetch request,
// (3) valid partial-404 response identifying one requested record missing,
// (4) second message-fetch (retry) for the present IDs, (5) successful
// installation.
// Controlled 700ms delays in BOTH message-fetch operations make their combined
// duration exceed one configured RequestTimeout, while each operation
// individually remains within its own budget.
func TestBootstrapE2E_PerRequestTimeoutOwnership(t *testing.T) {
	ca := newTestCA(t, "bootstrap-timeout")
	// Configure both the engine and the peer client with a 1s RequestTimeout
	// so the regression is caught if an engine-level shared deadline is
	// reintroduced around FetchBootstrapMessages.
	source, requester := newBootstrapNodePairWithTimeout(t, ca, time.Second)

	// Seed the source's store with 4 messages. The inventory lists 4 IDs;
	// the engine's FetchBatchSize is 8, so all 4 IDs are requested in a
	// single fetch call. The custom handler intercepts the FIRST
	// /internal/sync/messages request and returns a partial-404 (1 missing,
	// 3 present), triggering a retry for the 3 present IDs.
	ids := seedSourceStore(t, source.store, 4)
	removedID := ids[3]

	// Install a race-safe handler that:
	// - delays message-fetch operations by 700ms (both the initial and the
	//   retry; the combined 1.4s exceeds the 1s RequestTimeout budget)
	// - returns a partial-404 on the FIRST /internal/sync/messages request
	//   (identifying removedID as missing, the other 3 as present)
	// - passes through the inventory retrieval and the retry normally
	// The handler is installed through the synchronized pre-Serve wrapper
	// (configured before any request can be served, no race).
	var fetchCount int32
	var firstFetchDone int32
	origHandler := source.productionPeerHandler()
	source.setPeerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/sync/messages" {
			n := atomic.AddInt32(&fetchCount, 1)
			if n == 1 && atomic.CompareAndSwapInt32(&firstFetchDone, 0, 1) {
				// First message-fetch: return a partial-404.
				time.Sleep(700 * time.Millisecond)
				// Read the request body to extract the requested IDs.
				var req struct {
					IDs []string `json:"ids"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				var missing []string
				for _, id := range req.IDs {
					if id == removedID {
						missing = append(missing, id)
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
				w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
				w.Header().Set(lifecycle.HeaderRespEpochID, "e1")
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]any{"missing_ids": missing})
				return
			}
			// Second message-fetch (retry for present IDs): delay then pass through.
			time.Sleep(700 * time.Millisecond)
		}
		origHandler.ServeHTTP(w, r)
	}))

	// Set the requester's peer client requestTimeout to 1s.
	requester.client.SetRequestTimeout(1 * time.Second)

	start := time.Now()
	res := requester.aEngine.FetchBootstrapBaseline(context.Background(), source.peerAddr, sourceIdentity(), "", 4)
	elapsed := time.Since(start)

	if res.Error != nil {
		t.Fatalf("FetchBootstrapBaseline error: %v", res.Error)
	}
	if !res.Completed {
		t.Fatalf("expected page to be Completed, got: Inserted=%d ConfirmedAbsent=%d", len(res.Inserted), len(res.ConfirmedAbsent))
	}
	// Two message-fetch HTTP requests occurred (initial + partial-404 retry).
	// (fetchCount is checked below via the handler.)
	if len(res.ConfirmedAbsent) != 1 || res.ConfirmedAbsent[0] != removedID {
		t.Errorf("expected 1 ConfirmedAbsent (%s), got %v", removedID, res.ConfirmedAbsent)
	}
	if len(res.Inserted) != 3 {
		t.Errorf("expected 3 inserted (the present records), got %d: %v", len(res.Inserted), res.Inserted)
	}
	// Two message-fetch HTTP requests occurred (initial + partial-404 retry).
	if got := atomic.LoadInt32(&fetchCount); got != 2 {
		t.Errorf("expected 2 message-fetch HTTP requests, got %d", got)
	}
	// The combined duration of the two message-fetch operations (2×700ms=1.4s)
	// must exceed 1s (one RequestTimeout), proving per-request independence.
	if elapsed < 1*time.Second {
		t.Errorf("combined duration %v should exceed 1s to prove per-request independence", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Errorf("combined duration %v is unexpectedly long", elapsed)
	}
	t.Logf("per-request timeout ownership: elapsed=%v (2 fetch ops × 700ms > 1s, each op < 1s)", elapsed)
}

// TestBootstrapE2E_InventoryTimeout verifies that a slow bootstrap inventory
// operation honors the configured request timeout. If the inventory retrieval
// takes longer than the client's RequestTimeout, the operation returns a
// context deadline error (the client's per-request timeout fires).
func TestBootstrapE2E_InventoryTimeout(t *testing.T) {
	ca := newTestCA(t, "bootstrap-inv-timeout")
	source, requester := newBootstrapNodePair(t, ca)
	seedSourceStore(t, source.store, 8)

	// Install a handler that delays ALL requests by 2s (exceeding the 1s
	// RequestTimeout). The inventory retrieval is the first HTTP operation;
	// it must time out. The handler is installed through the synchronized
	// pre-Serve wrapper (configured before any request can be served, no
	// race) and is NOT replaced after the test (the server shuts down via
	// t.Cleanup).
	origHandler := source.productionPeerHandler()
	source.setPeerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		origHandler.ServeHTTP(w, r)
	}))

	requester.client.SetRequestTimeout(1 * time.Second)

	start := time.Now()
	res := requester.aEngine.FetchBootstrapBaseline(context.Background(), source.peerAddr, sourceIdentity(), "", 8)
	elapsed := time.Since(start)

	// The inventory retrieval must have timed out (1s request budget).
	if res.Error == nil {
		t.Fatal("expected error from slow inventory retrieval, got nil")
	}
	// The elapsed time should be close to 1s (the request timeout), not 2s
	// (the handler's delay).
	if elapsed > 1500*time.Millisecond {
		t.Errorf("inventory retrieval took %v, expected ~1s (request timeout bound)", elapsed)
	}
	if res.Completed {
		t.Error("page should not be Completed when inventory retrieval times out")
	}
	t.Logf("inventory timeout: elapsed=%v (bounded by 1s request timeout, not 2s handler delay)", elapsed)
}

// --- Test: bootstrap-specific authorization at transport boundary (Correction 3 regression) ---

// TestBootstrapE2E_BootstrapAuthRevokedAtTransport verifies that when the
// lifecycle controller transitions to RECOVERY_REQUIRED between an initial
// authorized HTTP request and a partial-404 retry, the retry is blocked
// (no network I/O) by the dedicated bootstrap authorization decision.
//
// The scenario:
//  1. A SYNCING requester (bootstrap-authorized) sends a bootstrap
//     message-fetch request. The source returns a valid partial-404
//     (one ID missing, one present), which the client accepts as
//     successful reconciliation.
//  2. The requester's controller transitions to RECOVERY_REQUIRED
//     (without changing the accepted epoch or durable binding).
//  3. The client attempts a retry for the present ID. The dedicated
//     bootstrap decision (BootstrapDecision) now returns authorized=false
//     (State != SYNCING). The retry is blocked before any network I/O.
//
// This exercises the REAL production wiring (real controller, real
// BootstrapEpochProvider, real mTLS transport) and proves that the
// partial-404 retry independently repeats the bootstrap authorization
// decision.
func TestBootstrapE2E_BootstrapAuthRevokedAtTransport(t *testing.T) {
	ca := newTestCA(t, "bootstrap-auth-revoke")
	source, requester := newBootstrapNodePair(t, ca)

	// Seed 2 messages. The handler will make 1 present and 1 missing
	// (partial-404), triggering a retry for the present ID.
	ids := seedSourceStore(t, source.store, 2)
	missingID := ids[0]
	presentID := ids[1]

	// Install a handler that returns a partial-404 on the first
	// /internal/sync/messages request (missingID absent, presentID
	// present) and passes through on the second (retry). Count requests.
	// The handler is installed through the synchronized pre-Serve wrapper
	// (configured before any request can be served, no race).
	var fetchCount int32
	origHandler := source.productionPeerHandler()
	source.setPeerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/sync/messages" {
			n := atomic.AddInt32(&fetchCount, 1)
			if n == 1 {
				var req struct {
					IDs []string `json:"ids"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				var missing []string
				for _, id := range req.IDs {
					if id == missingID {
						missing = append(missing, id)
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
				w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
				w.Header().Set(lifecycle.HeaderRespEpochID, "e1")
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]any{"missing_ids": missing})
				return
			}
			// Second call (retry): pass through to the original handler.
		}
		origHandler.ServeHTTP(w, r)
	}))

	// Precondition: the requester is SYNCING and bootstrap-authorized.
	_, authed, err := requester.ctrl.BootstrapAuthorizationSnapshot()
	if err != nil {
		t.Fatalf("BootstrapAuthorizationSnapshot: %v", err)
	}
	if !authed {
		t.Fatalf("precondition: requester should be bootstrap-authorized, but it is not")
	}

	// The initial message-fetch request succeeds: the source returns a
	// valid partial-404 (missingID absent, presentID present). The client
	// accepts the 404 as successful reconciliation and retries for the
	// present ID. Two HTTP requests total (1 404 + 1 retry).
	msgs, fetchErr := requester.client.FetchBootstrapMessages(
		context.Background(),
		source.peerAddr,
		sourceIdentity(),
		ids,
		&lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
	)
	if fetchErr != nil {
		t.Fatalf("initial bootstrap fetch should succeed (partial-404 is valid reconciliation), got: %v", fetchErr)
	}
	if len(msgs) != 1 || msgs[0].ID != presentID {
		t.Fatalf("expected 1 fetched message (presentID), got %d: %v", len(msgs), msgs)
	}
	// The initial call made 2 HTTP requests (1 404 + 1 retry).
	if got := atomic.LoadInt32(&fetchCount); got != 2 {
		t.Fatalf("expected 2 fetch requests after initial call (404 + retry), got %d", got)
	}

	// Now transition the requester to RECOVERY_REQUIRED. The cluster ID,
	// epoch ID, and durable binding all remain unchanged; only the state
	// changes. The general EpochProvider would still return a valid context
	// (metadata is fresh, binding matches), but the dedicated bootstrap
	// decision (bootstrapAuthorized) now returns authorized=false.
	if err := requester.ctrl.SetRecoveryRequired("test transport revocation"); err != nil {
		t.Fatalf("SetRecoveryRequired: %v", err)
	}
	_, authed, err = requester.ctrl.BootstrapAuthorizationSnapshot()
	if err != nil {
		t.Fatalf("BootstrapAuthorizationSnapshot after state change: %v", err)
	}
	if authed {
		t.Fatalf("postcondition: requester should NOT be bootstrap-authorized after RECOVERY_REQUIRED")
	}

	// Now attempt a bootstrap fetch for the present ID. The dedicated
	// bootstrap decision (BootstrapDecision) returns authorized=false
	// (State != SYNCING). The request is blocked before any network I/O.
	_, fetchErr = requester.client.FetchBootstrapMessages(
		context.Background(),
		source.peerAddr,
		sourceIdentity(),
		[]string{presentID},
		&lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
	)
	if fetchErr == nil {
		t.Fatal("expected PeerBlockedError for bootstrap fetch after RECOVERY_REQUIRED, got nil")
	}
	if !isPeerBlockedError(fetchErr) {
		t.Errorf("expected PeerBlockedError (transport boundary), got: %T: %v", fetchErr, fetchErr)
	}
	// The second fetch was NOT transmitted: the fetch count is still 2
	// (only the initial 404 + retry). No unauthorized records were installed.
	if got := atomic.LoadInt32(&fetchCount); got != 2 {
		t.Errorf("expected 2 fetch requests (second call blocked, not transmitted), got %d", got)
	}
}

// isPeerBlockedError returns true if err is or wraps a *peertls.PeerBlockedError.
func isPeerBlockedError(err error) bool {
	if err == nil {
		return false
	}
	var blocked *peertls.PeerBlockedError
	return errors.As(err, &blocked)
}

// --- Test: revocation immediately before a partial-404 retry (retry-boundary regression) ---

// TestBootstrapE2E_BootstrapAuthRevokedAtRetryBoundary verifies that when the
// lifecycle controller transitions to RECOVERY_REQUIRED between the initial
// authorized bootstrap message-fetch (which returns a valid partial-404) and
// the retry transmission, the retry is blocked at the transport boundary (no
// network I/O) by the dedicated bootstrap authorization decision.
//
// The scenario exercises the REAL production wiring (real controller, real
// BootstrapEpochProvider delegation, real mTLS transport):
//
//  1. A SYNCING requester (bootstrap-authorized) sends a bootstrap
//     message-fetch request via the peertls client's FetchBootstrapMessages.
//     The source returns a valid partial-404 (one ID missing, one present).
//     The client accepts the 404 as successful reconciliation.
//  2. The second BootstrapDecision call (the in-flight recheck after the 404
//     response) captures the still-authorized decision from the real
//     controller.
//  3. Before the captured decision is returned, the wrapper transitions the
//     real controller to RECOVERY_REQUIRED.
//  4. The retry's third BootstrapDecision call obtains the new rejected
//     decision from the real controller. The retry is blocked with
//     PeerBlockedError before any network I/O.
//  5. Exactly one HTTP request was transmitted (the initial 404 only; the
//     retry was blocked).
//  6. The cluster ID, epoch ID, and durable binding remain unchanged.
func TestBootstrapE2E_BootstrapAuthRevokedAtRetryBoundary(t *testing.T) {
	ca := newTestCA(t, "bootstrap-retry-revoke")
	source, requester := newBootstrapNodePair(t, ca)

	// Seed 2 messages. The handler returns a partial-404 on the first
	// /internal/sync/messages request (ids[0] absent, ids[1] present),
	// triggering a retry for the present ID.
	ids := seedSourceStore(t, source.store, 2)
	missingID := ids[0]

	// Install a race-safe handler (installed through the synchronized
	// pre-Serve wrapper, configured before any request can be served)
	// that returns a partial-404 on the first /internal/sync/messages
	// request and passes through on subsequent requests. Count requests.
	var fetchCount int32
	origHandler := source.productionPeerHandler()
	source.setPeerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/sync/messages" {
			n := atomic.AddInt32(&fetchCount, 1)
			if n == 1 {
				var req struct {
					IDs []string `json:"ids"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				var missing []string
				for _, id := range req.IDs {
					if id == missingID {
						missing = append(missing, id)
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
				w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
				w.Header().Set(lifecycle.HeaderRespEpochID, "e1")
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]any{"missing_ids": missing})
				return
			}
			// Second call (retry): pass through to the original handler.
		}
		origHandler.ServeHTTP(w, r)
	}))

	// Precondition: the requester is SYNCING and bootstrap-authorized.
	_, authed, err := requester.ctrl.BootstrapAuthorizationSnapshot()
	if err != nil {
		t.Fatalf("BootstrapAuthorizationSnapshot: %v", err)
	}
	if !authed {
		t.Fatalf("precondition: requester should be bootstrap-authorized, but it is not")
	}

	// Create a test-only BootstrapEpochProvider wrapper that delegates
	// authorization decisions to the real controller but transitions the
	// controller to RECOVERY_REQUIRED after the second BootstrapDecision
	// call (the in-flight recheck after the partial-404 response).
	wrapper := &delegatingBootstrapProvider{
		ctrl:        requester.ctrl,
		revokeAfter: 2,
	}

	// Replace the requester's peer client's bootstrap provider with the
	// wrapper. The general EpochProvider remains the real controller's
	// provider (unchanged); only the bootstrap provider is wrapped.
	requester.client.SetBootstrapEpochProvider(wrapper)

	// Attempt a bootstrap message-fetch for both IDs. The flow:
	//   - BootstrapDecision call 1 (pre-transmission check): authorized.
	//   - Initial HTTP request transmitted (count=1).
	//   - Source returns partial-404 (missingID absent).
	//   - BootstrapDecision call 2 (in-flight recheck): captures the
	//     still-authorized decision, then transitions controller to
	//     RECOVERY_REQUIRED before returning.
	//   - BootstrapDecision call 3 (retry pre-transmission check): the
	//     controller is now RECOVERY_REQUIRED; the decision is rejected.
	//   - The retry is blocked with PeerBlockedError (no network I/O).
	msgs, fetchErr := requester.client.FetchBootstrapMessages(
		context.Background(),
		source.peerAddr,
		sourceIdentity(),
		ids,
		&lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
	)

	// The fetch must fail with PeerBlockedError (the retry was blocked at
	// the transport boundary by the rejected bootstrap decision).
	if fetchErr == nil {
		t.Fatalf("expected PeerBlockedError for retry after RECOVERY_REQUIRED transition, got nil (msgs=%v)", msgs)
	}
	if !isPeerBlockedError(fetchErr) {
		t.Fatalf("expected PeerBlockedError (transport boundary), got: %T: %v", fetchErr, fetchErr)
	}

	// Exactly one HTTP request was transmitted (the initial 404 only; the
	// retry was blocked before network I/O).
	if got := atomic.LoadInt32(&fetchCount); got != 1 {
		t.Errorf("expected 1 transmitted HTTP request (initial 404 only; retry blocked), got %d", got)
	}

	// The real controller must have been consulted exactly three times
	// (call 1: initial pre-transmission; call 2: in-flight recheck after the
	// partial-404 response, which captured the still-authorized decision and
	// then transitioned the controller; call 3: retry pre-transmission, which
	// observed the new rejected decision). This proves the revocation landed
	// between the in-flight recheck and the retry, not before the recheck.
	if got := wrapper.decisionsMade(); got != 3 {
		t.Errorf("expected 3 BootstrapDecision calls (initial, in-flight recheck, retry pre-transmission), got %d", got)
	}

	// The cluster ID, epoch ID, and durable binding remain unchanged after
	// the state transition.
	state := requester.ctrl.Machine().State()
	if state != lifecycle.RECOVERY_REQUIRED {
		t.Errorf("requester state = %s, want RECOVERY_REQUIRED", state.String())
	}
	binding := requester.ctrl.LocalBinding()
	if binding == nil {
		t.Fatal("durable binding is nil after state transition (should be preserved)")
	}
	if binding.ClusterID != "c1" || binding.EpochID != "e1" {
		t.Errorf("binding changed: got %s/%s, want c1/e1", binding.ClusterID, binding.EpochID)
	}
	meta := requester.ctrl.AcceptedMetadata()
	if meta == nil {
		t.Fatal("accepted metadata is nil after state transition (should be preserved)")
	}
	if meta.ClusterID != "c1" || meta.EpochID != "e1" {
		t.Errorf("metadata changed: got %s/%s, want c1/e1", meta.ClusterID, meta.EpochID)
	}
	t.Logf("retry-boundary revocation: 3 decisions made, 1 request transmitted, retry blocked with PeerBlockedError, binding=%s/%s unchanged", binding.ClusterID, binding.EpochID)
}

// --- Test: controllable-provider transport test — reject at retry transmission ---

// TestBootstrapE2E_BootstrapAuthRevokedAtRetryTransport verifies that a
// controllable BootstrapEpochProvider that authorizes the first two
// BootstrapDecision calls (pre-transmission check and in-flight recheck) but
// rejects the third (the retry's pre-transmission check) proves rejection
// specifically at retry transmission rather than at response consumption.
//
// The scenario:
//  1. BootstrapDecision call 1 (pre-transmission check for the initial
//     request): authorized. The initial HTTP request is transmitted.
//  2. The source returns a valid partial-404 (one ID missing, one present).
//  3. BootstrapDecision call 2 (in-flight recheck after the 404 response):
//     authorized. The response is consumed (reconciliation proceeds).
//  4. BootstrapDecision call 3 (pre-transmission check for the retry):
//     rejected (authorized=false). The retry is blocked with PeerBlockedError
//     before any network I/O.
//
// This is distinct from the integration test above (which uses the real
// controller and transitions its state between the in-flight recheck and the
// retry): here the rejection is controlled purely by the provider, proving
// the transport-level boundary is at the retry's pre-transmission check, not
// at the response-consumption in-flight recheck.
func TestBootstrapE2E_BootstrapAuthRevokedAtRetryTransport(t *testing.T) {
	ca := newTestCA(t, "bootstrap-retry-transport-revoke")
	source, requester := newBootstrapNodePair(t, ca)

	// Seed 2 messages. The handler returns a partial-404 on the first
	// /internal/sync/messages request (ids[0] absent, ids[1] present).
	ids := seedSourceStore(t, source.store, 2)
	missingID := ids[0]

	// Install a race-safe handler (installed through the synchronized
	// pre-Serve wrapper, configured before any request can be served)
	// that returns a partial-404 on the first /internal/sync/messages
	// request and passes through on subsequent requests. Count requests.
	var fetchCount int32
	origHandler := source.productionPeerHandler()
	source.setPeerHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/sync/messages" {
			n := atomic.AddInt32(&fetchCount, 1)
			if n == 1 {
				var req struct {
					IDs []string `json:"ids"`
				}
				_ = json.NewDecoder(r.Body).Decode(&req)
				var missing []string
				for _, id := range req.IDs {
					if id == missingID {
						missing = append(missing, id)
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
				w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
				w.Header().Set(lifecycle.HeaderRespEpochID, "e1")
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]any{"missing_ids": missing})
				return
			}
			// Second call (retry): pass through to the original handler.
		}
		origHandler.ServeHTTP(w, r)
	}))

	// A controllable bootstrap provider that authorizes the first two
	// BootstrapDecision calls and rejects the third (the retry's
	// pre-transmission check). This proves rejection is at retry
	// transmission, not at response consumption (call 2 is authorized, so
	// the partial-404 response is consumed successfully).
	provider := &controllableBootstrapProvider{
		ctx:        lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		authorizeN: 2,
	}

	requester.client.SetBootstrapEpochProvider(provider)

	// Attempt a bootstrap message-fetch for both IDs. The flow:
	//   - BootstrapDecision call 1 (initial pre-transmission): authorized.
	//   - Initial HTTP request transmitted (count=1).
	//   - Source returns partial-404.
	//   - BootstrapDecision call 2 (in-flight recheck): authorized.
	//   - Response consumed; retry proceeds.
	//   - BootstrapDecision call 3 (retry pre-transmission): rejected.
	//   - The retry is blocked with PeerBlockedError (no network I/O).
	_, fetchErr := requester.client.FetchBootstrapMessages(
		context.Background(),
		source.peerAddr,
		sourceIdentity(),
		ids,
		&lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
	)

	// The fetch must fail with PeerBlockedError (the retry was blocked at
	// the transport boundary by the rejected bootstrap decision).
	if fetchErr == nil {
		t.Fatal("expected PeerBlockedError for retry after provider rejection, got nil")
	}
	if !isPeerBlockedError(fetchErr) {
		t.Fatalf("expected PeerBlockedError (transport boundary), got: %T: %v", fetchErr, fetchErr)
	}

	// Exactly one HTTP request was transmitted (the initial 404 only; the
	// retry was blocked before network I/O).
	if got := atomic.LoadInt32(&fetchCount); got != 1 {
		t.Errorf("expected 1 transmitted HTTP request (initial 404 only; retry blocked), got %d", got)
	}

	// The provider must have been called exactly 3 times (pre-transmission
	// for initial, in-flight recheck, pre-transmission for retry).
	if got := provider.decisionsMade(); got != 3 {
		t.Errorf("expected 3 BootstrapDecision calls, got %d", got)
	}
	t.Logf("retry-transport revocation: 1 request transmitted, retry blocked with PeerBlockedError at call 3, calls=%d", provider.decisionsMade())
}

// --- Test-only providers ---

// delegatingBootstrapProvider is a test-only peertls.BootstrapEpochProvider
// wrapper that delegates authorization decisions to a real lifecycle
// controller but can transition the controller to RECOVERY_REQUIRED after a
// configurable number of BootstrapDecision calls. This is used to test the
// retry-boundary regression: the controller is still SYNCING (authorized) for
// the first N calls, then is transitioned to RECOVERY_REQUIRED, and
// subsequent calls observe the new rejected decision from the real
// controller.
type delegatingBootstrapProvider struct {
	ctrl        *lifecycle.Controller
	revokeAfter int
	calls       int
	mu          sync.Mutex
}

// BootstrapDecision delegates to the controller's BootstrapAuthorizationSnapshot
// and, after the configured number of calls, transitions the controller to
// RECOVERY_REQUIRED before returning. The transition is performed on the REAL
// controller (not a mock), so the subsequent BootstrapDecision calls observe
// the genuine rejected decision from the real lifecycle state machine.
//
// The capture-then-revoke ordering is critical: the current decision is
// captured first, and only after that (on the Nth call) is the controller
// transitioned. This ensures the Nth call returns the still-authorized
// decision (the in-flight recheck at response-consumption time sees
// authorized=true, so the partial-404 response is consumed), and the (N+1)th
// call observes the new rejected decision (the retry pre-transmission check
// is blocked). Revoking before the capture would cause the Nth call to
// return the already-rejected decision, blocking at response-consumption
// rather than at retry transmission.
func (p *delegatingBootstrapProvider) BootstrapDecision() (lifecycle.RequestContext, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.calls++

	ctx, authorized, err := p.ctrl.BootstrapAuthorizationSnapshot()
	if err != nil {
		return ctx, authorized, err
	}

	if p.revokeAfter > 0 && p.calls == p.revokeAfter {
		if err := p.ctrl.SetRecoveryRequired("test retry-boundary revocation"); err != nil {
			return lifecycle.RequestContext{}, false,
				fmt.Errorf("delegatingBootstrapProvider: SetRecoveryRequired: %w", err)
		}
	}

	return ctx, authorized, nil
}

// decisionsMade returns the number of BootstrapDecision calls made. It is
// concurrency-safe: it acquires the same mutex that protects p.calls.
func (p *delegatingBootstrapProvider) decisionsMade() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// controllableBootstrapProvider is a test-only peertls.BootstrapEpochProvider
// that authorizes the first authorizeN BootstrapDecision calls and rejects
// subsequent calls (authorized=false). This is used to prove rejection at
// retry transmission (call N+1) rather than at response consumption
// (call N).
type controllableBootstrapProvider struct {
	ctx        lifecycle.RequestContext
	authorizeN int
	n          int
	mu         sync.Mutex
}

func (p *controllableBootstrapProvider) BootstrapDecision() (lifecycle.RequestContext, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n++
	if p.n <= p.authorizeN {
		return p.ctx, true, nil
	}
	return p.ctx, false, nil
}

// decisionsMade returns the number of BootstrapDecision calls made.
func (p *controllableBootstrapProvider) decisionsMade() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.n
}

// --- Helpers ---

// containsAdmissionError returns true if the error string contains a local
// admission rejection (not a transport-level error).
func containsAdmissionError(s string) bool {
	return strings.Contains(s, "lifecycle bootstrap admission") || strings.Contains(s, "lifecycle admission")
}
