// Tests for the bootstrap baseline fetch admission contract end-to-end
// (Milestone 5B, Stage 2). These tests drive the actual peer HTTP handlers
// (dispatchSyncInventoryHandler / dispatchSyncFetchHandler) with a real
// lifecycle controller, a real mTLS-free httptest request, and a real
// SQLite-backed store. They verify:
//
//  1. A READY or DEGRADED source serves a bootstrap inventory page and
//     message records to an authenticated, same-epoch requester.
//  2. A SYNCING, GENESIS, or RECOVERY_REQUIRED source NEVER serves a
//     bootstrap baseline (the full authorized() decision fails for
//     non-ready states).
//  3. A wrong-epoch or wrong-cluster request is rejected (409).
//  4. A request with no epoch headers is rejected (409).
//  5. A request without the bootstrap request-purpose header falls back to
//     the ordinary handler (a SYNCING source is rejected by the ordinary
//     handler's authorized() check, just as before).
//  6. Privilege isolation: a bootstrap-authorized SYNCING replica cannot
//     accept inbound replication writes, cannot accept public writes, and
//     cannot act as an authoritative source. The bootstrap fetch privilege
//     is narrowly scoped to the two sync endpoints and the guarded local
//     commit; it does not grant any other lifecycle privilege.

package peertls

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/store"
)

// newBootstrapTestController creates a controller with a warm binding cache
// (the authorization snapshot reads bindingVal, not the store, so the cache
// must be set for the bootstrap admission checks to see a matching binding).
// All bootstrap tests use the same cluster/epoch ("c1"/"e1").
func newBootstrapTestController(t *testing.T, state lifecycle.State) *lifecycle.Controller {
	t.Helper()
	const clusterID, epochID = "c1", "e1"
	ctrl := newInboundTestController(t, state, clusterID, epochID)
	ctrl.SetBindingCacheForTest(&lifecycle.DatabaseBinding{
		ClusterID:  clusterID,
		EpochID:    epochID,
		Generation: 1,
		CreatedAt:  time.Now(),
	})
	return ctrl
}

// bootstrapSyncRequest builds an HTTP request for the bootstrap sync endpoints
// with the epoch context headers and the bootstrap request-purpose header.
func bootstrapSyncRequest(t *testing.T, method, path, body string, clusterID, epochID string) *http.Request {
	t.Helper()
	var reader *bytes.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequestWithContext(context.Background(), method, path, reader)
	req.Header.Set(lifecycle.HeaderClusterID, clusterID)
	req.Header.Set(lifecycle.HeaderEpochID, epochID)
	req.Header.Set(lifecycle.HeaderRequestPurpose, lifecycle.PurposeBootstrapBaseline)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
		req.ContentLength = int64(len(body))
	}
	return req
}

// bootstrapSyncRequestNoPurpose builds an HTTP request for the sync endpoints
// with the epoch context headers but WITHOUT the bootstrap request-purpose
// header (the request falls back to the ordinary handler).
func bootstrapSyncRequestNoPurpose(t *testing.T, method, path, body string, clusterID, epochID string) *http.Request {
	t.Helper()
	var reader *bytes.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequestWithContext(context.Background(), method, path, reader)
	req.Header.Set(lifecycle.HeaderClusterID, clusterID)
	req.Header.Set(lifecycle.HeaderEpochID, epochID)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
		req.ContentLength = int64(len(body))
	}
	return req
}

// bootstrapSyncRequestMalformedPurpose builds an HTTP request with a
// malformed/unsupported request-purpose header. The dispatch falls back to the
// ordinary handler (the header is untrusted; a malformed value is never an
// error and never grants a privilege).
func bootstrapSyncRequestMalformedPurpose(t *testing.T, method, path, body string, clusterID, epochID string) *http.Request {
	t.Helper()
	var reader *bytes.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequestWithContext(context.Background(), method, path, reader)
	req.Header.Set(lifecycle.HeaderClusterID, clusterID)
	req.Header.Set(lifecycle.HeaderEpochID, epochID)
	req.Header.Set(lifecycle.HeaderRequestPurpose, "not-a-supported-purpose")
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
		req.ContentLength = int64(len(body))
	}
	return req
}

// --- Bootstrap inventory: READY source ---

// TestBootstrapSyncInventory_ReadySource verifies that a READY source serves a
// bootstrap inventory page to an authenticated, same-epoch requester.
func TestBootstrapSyncInventory_ReadySource(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newBootstrapTestController(t, lifecycle.READY)

	// Insert a message so the inventory is non-empty.
	_, _ = st.InsertReplicatedMessageReturningInserted(&store.Message{
		ID:     "019a0000-0000-7000-0000-000000000001",
		Body:   "test-body",
		Origin: "gossipgrid-0.test",
	})

	handler := dispatchSyncInventoryHandler(st, ctrl, 0, time.Now)
	req := bootstrapSyncRequest(t, "GET", "/internal/sync/ids", "", "c1", "e1")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for bootstrap inventory from READY source, got %d; body: %s", rr.Code, rr.Body.String())
	}
	// The response envelope headers must carry the local cluster/epoch.
	if got := rr.Header().Get(lifecycle.HeaderRespClusterID); got != "c1" {
		t.Errorf("response envelope cluster ID = %q, want %q", got, "c1")
	}
	if got := rr.Header().Get(lifecycle.HeaderRespEpochID); got != "e1" {
		t.Errorf("response envelope epoch ID = %q, want %q", got, "e1")
	}
}

// --- Bootstrap inventory: DEGRADED source ---

// TestBootstrapSyncInventory_DegradedSource verifies that a DEGRADED source
// (which retains valid local authority) serves a bootstrap inventory page to an
// authenticated, same-epoch requester (the documented READY/DEGRADED policy).
func TestBootstrapSyncInventory_DegradedSource(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newBootstrapTestController(t, lifecycle.DEGRADED)

	handler := dispatchSyncInventoryHandler(st, ctrl, 0, time.Now)
	req := bootstrapSyncRequest(t, "GET", "/internal/sync/ids", "", "c1", "e1")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for bootstrap inventory from DEGRADED source, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

// --- Bootstrap inventory: SYNCING source rejected ---

// TestBootstrapSyncInventory_SyncingSourceRejected verifies that a SYNCING
// source NEVER serves a bootstrap baseline: its state is not ready, so the
// full authorized() decision fails and the request is rejected (503).
func TestBootstrapSyncInventory_SyncingSourceRejected(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newBootstrapTestController(t, lifecycle.SYNCING)

	handler := dispatchSyncInventoryHandler(st, ctrl, 0, time.Now)
	req := bootstrapSyncRequest(t, "GET", "/internal/sync/ids", "", "c1", "e1")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code == http.StatusOK {
		t.Fatalf("SYNCING source should NOT serve a bootstrap baseline, got 200; body: %s", rr.Body.String())
	}
	if rr.Code != lifecycle.UnreadyStatus && rr.Code != lifecycle.EpochMismatchStatus {
		t.Errorf("expected 503 or 409 for SYNCING source, got %d", rr.Code)
	}
}

// --- Bootstrap inventory: GENESIS source rejected ---

func TestBootstrapSyncInventory_GenesisSourceRejected(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newBootstrapTestController(t, lifecycle.GENESIS)

	handler := dispatchSyncInventoryHandler(st, ctrl, 0, time.Now)
	req := bootstrapSyncRequest(t, "GET", "/internal/sync/ids", "", "c1", "e1")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code == http.StatusOK {
		t.Fatalf("GENESIS source should NOT serve a bootstrap baseline, got 200")
	}
}

// --- Bootstrap inventory: RECOVERY_REQUIRED source rejected ---

func TestBootstrapSyncInventory_RecoveryRequiredSourceRejected(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newBootstrapTestController(t, lifecycle.RECOVERY_REQUIRED)

	handler := dispatchSyncInventoryHandler(st, ctrl, 0, time.Now)
	req := bootstrapSyncRequest(t, "GET", "/internal/sync/ids", "", "c1", "e1")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code == http.StatusOK {
		t.Fatalf("RECOVERY_REQUIRED source should NOT serve a bootstrap baseline, got 200")
	}
}

// --- Bootstrap inventory: wrong-epoch request rejected ---

func TestBootstrapSyncInventory_WrongEpochRequest(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newBootstrapTestController(t, lifecycle.READY)

	handler := dispatchSyncInventoryHandler(st, ctrl, 0, time.Now)
	req := bootstrapSyncRequest(t, "GET", "/internal/sync/ids", "", "c1", "e2")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != lifecycle.EpochMismatchStatus {
		t.Errorf("expected 409 for wrong-epoch request, got %d; body: %s", rr.Code, rr.Body.String())
	}
}

// --- Bootstrap inventory: wrong-cluster request rejected ---

func TestBootstrapSyncInventory_WrongClusterRequest(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newBootstrapTestController(t, lifecycle.READY)

	handler := dispatchSyncInventoryHandler(st, ctrl, 0, time.Now)
	req := bootstrapSyncRequest(t, "GET", "/internal/sync/ids", "", "c2", "e1")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != lifecycle.EpochMismatchStatus {
		t.Errorf("expected 409 for wrong-cluster request, got %d", rr.Code)
	}
}

// --- Bootstrap inventory: missing epoch headers rejected ---

func TestBootstrapSyncInventory_MissingEpochHeaders(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newBootstrapTestController(t, lifecycle.READY)

	handler := dispatchSyncInventoryHandler(st, ctrl, 0, time.Now)
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/internal/sync/ids", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != lifecycle.EpochMismatchStatus {
		t.Errorf("expected 409 for missing epoch headers, got %d", rr.Code)
	}
}

// --- Bootstrap inventory: malformed purpose header falls back to ordinary ---

// TestBootstrapSyncInventory_MalformedPurposeFallsBack verifies that a
// malformed request-purpose header is treated as untrusted metadata: the
// dispatch falls back to the ordinary handler (a SYNCING source is rejected by
// the ordinary handler's authorized() check). The header never grants a
// privilege.
func TestBootstrapSyncInventory_MalformedPurposeFallsBack(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newBootstrapTestController(t, lifecycle.SYNCING)

	handler := dispatchSyncInventoryHandler(st, ctrl, 0, time.Now)
	req := bootstrapSyncRequestMalformedPurpose(t, "GET", "/internal/sync/ids", "", "c1", "e1")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	// A SYNCING source is not authorized for ordinary sync either; the
	// malformed purpose header must not grant the bootstrap privilege.
	if rr.Code == http.StatusOK {
		t.Fatalf("SYNCING source should NOT serve with a malformed purpose header, got 200")
	}
}

// --- Bootstrap fetch (POST /internal/sync/messages): READY source ---

func TestBootstrapSyncFetch_ReadySource(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newBootstrapTestController(t, lifecycle.READY)

	handler := dispatchSyncFetchHandler(st, ctrl, 0, time.Now)
	// A valid fetch request with an ID list.
	body := `{"ids":["019a0000-0000-7000-0000-000000000001"]}`
	req := bootstrapSyncRequest(t, "POST", "/internal/sync/messages", body, "c1", "e1")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	// The request is admitted (200) or a data-level error (404/409) depending
	// on whether the ID exists. The key assertion: the admission check
	// passes (not a 503 unready).
	if rr.Code == lifecycle.UnreadyStatus {
		t.Fatalf("READY source should not reject bootstrap fetch with 503; got body: %s", rr.Body.String())
	}
}

// --- Bootstrap fetch: SYNCING source rejected ---

func TestBootstrapSyncFetch_SyncingSourceRejected(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newBootstrapTestController(t, lifecycle.SYNCING)

	handler := dispatchSyncFetchHandler(st, ctrl, 0, time.Now)
	body := `{"ids":["019a0000-0000-7000-0000-000000000001"]}`
	req := bootstrapSyncRequest(t, "POST", "/internal/sync/messages", body, "c1", "e1")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code == http.StatusOK {
		t.Fatalf("SYNCING source should NOT serve bootstrap fetch, got 200")
	}
}

// --- Bootstrap fetch: wrong-epoch request rejected ---

func TestBootstrapSyncFetch_WrongEpochRequest(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newBootstrapTestController(t, lifecycle.READY)

	handler := dispatchSyncFetchHandler(st, ctrl, 0, time.Now)
	body := `{"ids":["019a0000-0000-7000-0000-000000000001"]}`
	req := bootstrapSyncRequest(t, "POST", "/internal/sync/messages", body, "c1", "e2")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != lifecycle.EpochMismatchStatus {
		t.Errorf("expected 409 for wrong-epoch bootstrap fetch request, got %d", rr.Code)
	}
}

// --- Privilege isolation: bootstrap-authorized SYNCING replica ---

// TestPrivilegeIsolation_BootstrapAuthorizedSyncingCannotAcceptInboundReplication
// verifies that a bootstrap-authorized SYNCING replica (one that can fetch a
// baseline) CANNOT accept inbound replication writes: the
// CheckReplicationInbound decision requires a ready state, which SYNCING is
// not. The bootstrap fetch privilege does not grant inbound replication
// write access.
func TestPrivilegeIsolation_BootstrapAuthorizedSyncingCannotAcceptInboundReplication(t *testing.T) {
	ctrl := newBootstrapTestController(t, lifecycle.SYNCING)

	// The bootstrap fetch decision: allowed (the replica is bound, fresh, and
	// SYNCING).
	fetchDecision := ctrl.Admission().CheckBootstrapFetch(context.Background(), lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"})
	if !fetchDecision.Allowed {
		t.Fatalf("precondition: bootstrap fetch should be allowed for a bound SYNCING replica, got: %s", fetchDecision.Reason)
	}

	// The inbound replication write decision: rejected (SYNCING is not a
	// ready state).
	repDecision := ctrl.Admission().CheckReplicationInbound(context.Background(), lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"})
	if repDecision.Allowed {
		t.Fatalf("inbound replication should be rejected for a SYNCING replica, got: %s", repDecision.Reason)
	}
}

// TestPrivilegeIsolation_BootstrapAuthorizedSyncingCannotAcceptPublicWrites
// verifies that a bootstrap-authorized SYNCING replica CANNOT accept public
// writes: the CheckWrite decision requires a ready state.
func TestPrivilegeIsolation_BootstrapAuthorizedSyncingCannotAcceptPublicWrites(t *testing.T) {
	ctrl := newBootstrapTestController(t, lifecycle.SYNCING)

	fetchDecision := ctrl.Admission().CheckBootstrapFetch(context.Background(), lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"})
	if !fetchDecision.Allowed {
		t.Fatalf("precondition: bootstrap fetch should be allowed, got: %s", fetchDecision.Reason)
	}

	writeDecision := ctrl.Admission().CheckWrite(context.Background())
	if writeDecision.Allowed {
		t.Fatalf("public writes should be rejected for a SYNCING replica, got: %s", writeDecision.Reason)
	}
}

// TestPrivilegeIsolation_BootstrapAuthorizedSyncingCannotAcceptOutboundReplication
// verifies that a bootstrap-authorized SYNCING replica CANNOT send outbound
// replication (gossip): the CheckReplicationOutbound decision requires a ready
// state.
func TestPrivilegeIsolation_BootstrapAuthorizedSyncingCannotAcceptOutboundReplication(t *testing.T) {
	ctrl := newBootstrapTestController(t, lifecycle.SYNCING)

	fetchDecision := ctrl.Admission().CheckBootstrapFetch(context.Background(), lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"})
	if !fetchDecision.Allowed {
		t.Fatalf("precondition: bootstrap fetch should be allowed, got: %s", fetchDecision.Reason)
	}

	outDecision := ctrl.Admission().CheckReplicationOutbound(context.Background())
	if outDecision.Allowed {
		t.Fatalf("outbound replication should be rejected for a SYNCING replica, got: %s", outDecision.Reason)
	}
}

// TestPrivilegeIsolation_BootstrapAuthorizedSyncingCannotSelfPromoteToReady
// verifies that a bootstrap-authorized SYNCING replica cannot use the bootstrap
// commit path to self-promote to READY: the bootstrap commit decision
// requires exactly SYNCING; after a transition to READY, the decision is
// rejected (the bootstrap path is not a promotion mechanism).
func TestPrivilegeIsolation_BootstrapAuthorizedSyncingCannotSelfPromoteToReady(t *testing.T) {
	ctrl := newBootstrapTestController(t, lifecycle.SYNCING)

	// Bootstrap commit is allowed while SYNCING.
	ran := false
	err := ctrl.CommitUnderBootstrapAuthorization(lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}, func() error {
		ran = true
		return nil
	})
	if err != nil {
		t.Fatalf("precondition: bootstrap commit should be allowed for bound SYNCING replica, got: %v", err)
	}
	if !ran {
		t.Fatalf("precondition: bootstrap commit callback should run")
	}

	// Now transition to READY (a promotion the bootstrap path must NOT
	// enable — only the full synchronization completion can do that).
	if err := ctrl.Machine().TransitionForTest(lifecycle.READY, "test"); err != nil {
		t.Fatalf("transition to READY: %v", err)
	}

	// The bootstrap commit is now rejected (the replica is no longer
	// SYNCING).
	ran = false
	err = ctrl.CommitUnderBootstrapAuthorization(lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}, func() error {
		ran = true
		return nil
	})
	if err == nil {
		t.Fatalf("bootstrap commit should be rejected after transition to READY (not a promotion path)")
	}
	if ran {
		t.Fatalf("bootstrap commit callback must NOT run after transition to READY")
	}
}

// TestPrivilegeIsolation_NoBootstrapPrivilegeWithoutPurposeHeader verifies that
// a SYNCING replica with no bootstrap request-purpose header (a legacy peer)
// cannot access the bootstrap sync endpoints: the dispatch falls back to the
// ordinary handler, which rejects a non-ready source.
func TestPrivilegeIsolation_NoBootstrapPrivilegeWithoutPurposeHeader(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newBootstrapTestController(t, lifecycle.SYNCING)

	handler := dispatchSyncInventoryHandler(st, ctrl, 0, time.Now)
	// No purpose header: the request falls back to the ordinary handler.
	req := bootstrapSyncRequestNoPurpose(t, "GET", "/internal/sync/ids", "", "c1", "e1")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code == http.StatusOK {
		t.Fatalf("SYNCING source should NOT serve without the bootstrap purpose header, got 200")
	}
}
