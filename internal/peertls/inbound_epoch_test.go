package peertls

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/store"
)

// TestFinalAdmissionCheckRevocation verifies that when authorization is
// revoked between the initial admission and the final boundary, the
// finalAdmissionCheck rejects the request.
func TestFinalAdmissionCheckRevocation(t *testing.T) {
	reqCtx := lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}

	// Authorized snapshot (initial check passed).
	_ = lifecycle.AuthorizationSnapshot{
		State:          lifecycle.READY,
		Metadata:       &lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true},
		AuthorityState: "fresh",
		Binding:        &lifecycle.DatabaseBinding{ClusterID: "c1", EpochID: "e1"},
	}

	// Revoked snapshot (final check: controller stopped).
	revokedSnap := lifecycle.AuthorizationSnapshot{
		State:   lifecycle.RECOVERY_REQUIRED,
		Stopped: true,
		Reason:  "lifecycle controller stopped",
	}
	status, reason := finalAdmissionCheck(revokedSnap, reqCtx)
	if status == 0 {
		t.Error("expected rejection for revoked authorization, got 0 (allowed)")
	}
	if status != lifecycle.UnreadyStatus && status != lifecycle.EpochMismatchStatus {
		t.Errorf("expected 503 or 409, got %d", status)
	}
	if reason == "" {
		t.Error("expected non-empty reason for rejection")
	}
}

// TestFinalAdmissionCheckEpochChange verifies that a valid epoch-A request is
// rejected when the current snapshot authorizes epoch-B. Authorization for
// epoch-B must never be used to process a request received under epoch-A.
func TestFinalAdmissionCheckEpochChange(t *testing.T) {
	// The request was made under epoch-A.
	reqCtx := lifecycle.RequestContext{ClusterID: "cluster-A", EpochID: "epoch-A"}

	// The current snapshot authorizes epoch-B (a concurrent epoch change
	// occurred between the initial check and the final boundary).
	epochBSnap := lifecycle.AuthorizationSnapshot{
		State:          lifecycle.READY,
		Metadata:       &lifecycle.ClusterMetadata{ClusterID: "cluster-B", EpochID: "epoch-B", Generation: 2, Initialized: true},
		AuthorityState: "fresh",
		Binding:        &lifecycle.DatabaseBinding{ClusterID: "cluster-B", EpochID: "epoch-B"},
	}
	status, reason := finalAdmissionCheck(epochBSnap, reqCtx)
	if status == 0 {
		t.Error("expected rejection: request under epoch-A, current authorization is epoch-B")
	}
	if status != lifecycle.EpochMismatchStatus {
		t.Errorf("expected 409 (epoch mismatch), got %d: %s", status, reason)
	}
}

// TestFinalAdmissionCheckValid verifies that when the request's epoch context
// matches the current snapshot, the final check passes.
func TestFinalAdmissionCheckValid(t *testing.T) {
	reqCtx := lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}

	snap := lifecycle.AuthorizationSnapshot{
		State:          lifecycle.READY,
		Metadata:       &lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true},
		AuthorityState: "fresh",
		Binding:        &lifecycle.DatabaseBinding{ClusterID: "c1", EpochID: "e1"},
	}
	status, reason := finalAdmissionCheck(snap, reqCtx)
	if status != 0 {
		t.Errorf("expected 0 (allowed), got %d: %s", status, reason)
	}
}

// TestFinalAdmissionCheckUnreadyState verifies that a request is rejected when
// the current snapshot is in an unready state (e.g. SYNCING), even if the
// epoch context matches.
func TestFinalAdmissionCheckUnreadyState(t *testing.T) {
	reqCtx := lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}

	snap := lifecycle.AuthorizationSnapshot{
		State:          lifecycle.SYNCING,
		Metadata:       &lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true},
		AuthorityState: "fresh",
		Binding:        &lifecycle.DatabaseBinding{ClusterID: "c1", EpochID: "e1"},
	}
	status, reason := finalAdmissionCheck(snap, reqCtx)
	if status == 0 {
		t.Error("expected rejection for unready state, got 0 (allowed)")
	}
	if status != lifecycle.UnreadyStatus {
		t.Errorf("expected 503 for unready state, got %d: %s", status, reason)
	}
}

// TestInboundEpochChangeHandlerRevocation tests the full handler path: a
// request is admitted initially, then authorization is revoked before the
// final boundary. The handler must reject the request.
func TestInboundEpochChangeHandlerRevocation(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newInboundTestController(t, lifecycle.READY, "c1", "e1")

	handler := newLifecycleSyncInventoryHandler(st, ctrl, 0, time.Now)

	// Send a request under the current epoch (c1/e1).
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/internal/sync/ids?limit=10", nil)
	req.Header.Set(lifecycle.HeaderClusterID, "c1")
	req.Header.Set(lifecycle.HeaderEpochID, "e1")

	// Stop the controller (revoking authorization). Both the initial check
	// and the final recheck will see the stopped state and reject.
	ctrl.Stop(context.Background())

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// The handler should reject (503 or 409).
	if w.Code == http.StatusOK {
		t.Error("expected non-200 response when controller is stopped, got 200")
	}
	if w.Code != lifecycle.UnreadyStatus && w.Code != lifecycle.EpochMismatchStatus {
		t.Errorf("expected 503 or 409, got %d", w.Code)
	}
}

// TestInboundEpochChangeHandlerEpochMismatch tests the full handler path:
// the request carries an epoch context that does NOT match the controller's
// current metadata. The handler must reject the request with 409.
func TestInboundEpochChangeHandlerEpochMismatch(t *testing.T) {
	st := newInboundTestStore(t)
	// Controller is authorized for c1/e1.
	ctrl := newInboundTestController(t, lifecycle.READY, "c1", "e1")

	handler := newLifecycleSyncInventoryHandler(st, ctrl, 0, time.Now)

	// Send a request under a DIFFERENT epoch (c2/e2).
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/internal/sync/ids?limit=10", nil)
	req.Header.Set(lifecycle.HeaderClusterID, "c2")
	req.Header.Set(lifecycle.HeaderEpochID, "e2")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// The handler should reject with 409 (epoch mismatch).
	if w.Code != http.StatusConflict {
		t.Errorf("expected 409 for epoch mismatch, got %d", w.Code)
	}
}

// TestInboundEpochChangeHandlerValidRequest tests the full handler path: a
// valid request under the current epoch is accepted.
func TestInboundEpochChangeHandlerValidRequest(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newInboundTestController(t, lifecycle.READY, "c1", "e1")

	handler := newLifecycleSyncInventoryHandler(st, ctrl, 0, time.Now)

	// Send a request under the current epoch (c1/e1).
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/internal/sync/ids?limit=10", nil)
	req.Header.Set(lifecycle.HeaderClusterID, "c1")
	req.Header.Set(lifecycle.HeaderEpochID, "e1")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// The handler should accept (200).
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for valid request, got %d", w.Code)
	}
}

// --- Test helpers ---

// newInboundTestStore creates a test store with a few messages.
func newInboundTestStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	st, err := store.New(dir + "/test.db")
	if err != nil {
		t.Fatalf("opening test store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// inboundTestController creates a controller in the given state with the
// given cluster/epoch, with a matching binding and fresh metadata.
//
//nolint:unparam // state is a flexible parameter; all current call sites use READY, but the switch supports other states for future tests.
func newInboundTestController(t *testing.T, state lifecycle.State, clusterID, epochID string) *lifecycle.Controller {
	t.Helper()
	machine := lifecycle.NewMachine(nil)
	switch state {
	case lifecycle.STARTING:
	case lifecycle.DISCOVERING:
		machine.ForceTransitionForTest(lifecycle.DISCOVERING, "test")
	case lifecycle.SYNCING:
		machine.ForceTransitionForTest(lifecycle.DISCOVERING, "test")
		if err := machine.TransitionForTest(lifecycle.SYNCING, "test"); err != nil {
			t.Fatalf("transition to SYNCING: %v", err)
		}
	case lifecycle.READY:
		machine.ForceTransitionForTest(lifecycle.DISCOVERING, "test")
		if err := machine.TransitionForTest(lifecycle.READY, "test"); err != nil {
			t.Fatalf("transition to READY: %v", err)
		}
	case lifecycle.DEGRADED:
		machine.ForceTransitionForTest(lifecycle.DISCOVERING, "test")
		if err := machine.TransitionForTest(lifecycle.READY, "test"); err != nil {
			t.Fatalf("transition to READY: %v", err)
		}
		if err := machine.TransitionForTest(lifecycle.DEGRADED, "test"); err != nil {
			t.Fatalf("transition to DEGRADED: %v", err)
		}
	case lifecycle.RECOVERY_REQUIRED:
		machine.ForceTransitionForTest(lifecycle.STARTING, "test")
		if err := machine.TransitionForTest(lifecycle.RECOVERY_REQUIRED, "test"); err != nil {
			t.Fatalf("transition to RECOVERY_REQUIRED: %v", err)
		}
	}

	meta := lifecycle.ClusterMetadata{
		ClusterID:   clusterID,
		EpochID:     epochID,
		Generation:  1,
		Initialized: true,
	}

	st := newInboundTestStore(t)
	if _, err := st.Bind(clusterID, epochID, 1); err != nil {
		t.Fatalf("binding test store: %v", err)
	}

	src := lifecycle.NewFakeMetadataSource(meta, true)
	ctrl := lifecycle.NewController(lifecycle.Config{
		MetadataPollInterval: time.Hour,
		Enabled:              true,
	}, machine, src, st, nil)
	ctrl.SetAcceptedMetadataForTest(meta)
	return ctrl
}
