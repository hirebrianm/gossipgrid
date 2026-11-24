package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gossipgrid/internal/antientropy"
	"gossipgrid/internal/lifecycle"
)

// This file exercises the combined /readyz handler (combinedReadyHandler) with
// the initial-sync barrier tracker, verifying that the readiness decision is
// the logical combination of lifecycle authorization and barrier completion.
// It covers:
//
//  1. Disabled gate (nil tracker) preserves existing readiness behavior.
//  2. Enabled gate begins NotReady (503, reason initial_sync_pending).
//  3. Successful complete synchronization makes the pod Ready (200).
//  4. A missing or unreachable expected peer prevents readiness (503).
//  5. Lifecycle authorization continues to take precedence: a
//     RECOVERY_REQUIRED (unauthorized) lifecycle yields 503 even when the
//     barrier is complete, and the reason is the lifecycle reason (not
//     initial_sync_pending).
//
// The tracker is the antientropy.InitialSyncTracker (or nil for the disabled
// case). The lifecycle controller is constructed via the existing
// makeControllerWithMetadata helper in lifecycle_test.go.

// readyResponseForTest is the decoded /readyz response shape. It mirrors the
// fields the test asserts on; using a local type keeps the test decoupled from
// the exact response struct (and tolerant of future field additions).
type readyResponseForTest struct {
	Service              string   `json:"service"`
	Ready                bool     `json:"ready"`
	State                string   `json:"state"`
	Reason               string   `json:"reason"`
	InitialSyncComplete  bool     `json:"initial_sync_complete"`
	InitialSyncCompleted int      `json:"initial_sync_completed"`
	InitialSyncExpected  int      `json:"initial_sync_expected"`
	InitialSyncMissing   []string `json:"initial_sync_missing"`
}

// getReadyz issues GET /readyz against the handler and returns the status and
// the decoded response.
func getReadyz(t *testing.T, handler http.Handler) (int, readyResponseForTest) {
	t.Helper()
	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/readyz", nil)
	handler.ServeHTTP(w, req)
	var resp readyResponseForTest
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding /readyz response: %v (body: %s)", err, w.Body.String())
	}
	return w.Code, resp
}

// --- Scenario 1: Disabled gate preserves existing readiness behavior ---

// With a nil tracker (barrier disabled) and a lifecycle that authorizes
// readiness (READY + fresh metadata + matching binding), /readyz returns 200
// (the existing 7A behavior). The initial-sync fields render the disabled
// view: InitialSyncComplete=true, InitialSyncExpected=0.
func TestReadyzDisabledTrackerPreservesBehavior(t *testing.T) {
	ctrl := makeController(t, lifecycle.READY)
	handler := NewWithStoreAndCallbackAndRetentionAndInitialSync(Node{ID: "node-1"}, newTestStore(t), nil, ctrl, 0, nil)

	code, resp := getReadyz(t, handler)
	if code != http.StatusOK {
		t.Errorf("disabled tracker + READY lifecycle: expected 200, got %d", code)
	}
	if !resp.Ready {
		t.Errorf("disabled tracker + READY lifecycle: expected Ready=true, got false")
	}
	if resp.Reason != "" {
		t.Errorf("disabled tracker + READY lifecycle: expected empty reason, got %q", resp.Reason)
	}
	if !resp.InitialSyncComplete {
		t.Errorf("disabled tracker: expected InitialSyncComplete=true (disabled = no constraint), got false")
	}
	if resp.InitialSyncExpected != 0 {
		t.Errorf("disabled tracker: expected InitialSyncExpected=0, got %d", resp.InitialSyncExpected)
	}
}

// --- Scenario 2: Enabled gate begins NotReady ---

// With an enabled tracker (barrier pending) and an authorized lifecycle,
// /readyz returns 503 with reason initial_sync_pending. The pod is NotReady
// until the barrier completes, regardless of lifecycle state.
func TestReadyzEnabledTrackerPending503(t *testing.T) {
	// 5-replica cluster, local gossipgrid-0: 4 expected remote peers.
	tracker := antientropy.NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3", "gossipgrid-4"},
		"gossipgrid-0",
	)
	if tracker.Complete() {
		t.Fatalf("precondition: fresh tracker should be pending")
	}
	ctrl := makeController(t, lifecycle.READY)
	handler := NewWithStoreAndCallbackAndRetentionAndInitialSync(Node{ID: "node-1"}, newTestStore(t), nil, ctrl, 0, tracker)

	code, resp := getReadyz(t, handler)
	if code != http.StatusServiceUnavailable {
		t.Errorf("enabled tracker (pending) + READY lifecycle: expected 503, got %d", code)
	}
	if resp.Ready {
		t.Errorf("enabled tracker (pending): expected Ready=false, got true")
	}
	if resp.Reason != "initial_sync_pending" {
		t.Errorf("enabled tracker (pending): expected reason initial_sync_pending, got %q", resp.Reason)
	}
	if resp.InitialSyncComplete {
		t.Errorf("enabled tracker (pending): expected InitialSyncComplete=false, got true")
	}
	if resp.InitialSyncExpected != 4 || resp.InitialSyncCompleted != 0 {
		t.Errorf("enabled tracker (pending): expected Completed=0 Expected=4, got %d/%d", resp.InitialSyncCompleted, resp.InitialSyncExpected)
	}
}

// --- Scenario 3: Successful complete synchronization makes the pod Ready ---

// Once the barrier completes (all expected peers have a full pass) and the
// lifecycle authorizes readiness, /readyz returns 200.
func TestReadyzEnabledTrackerComplete200(t *testing.T) {
	tracker := antientropy.NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3", "gossipgrid-4"},
		"gossipgrid-0",
	)
	// Complete the barrier (all 4 expected remote peers).
	tracker.RecordComplete("gossipgrid-1")
	tracker.RecordComplete("gossipgrid-2")
	tracker.RecordComplete("gossipgrid-3")
	tracker.RecordComplete("gossipgrid-4")
	if !tracker.Complete() {
		t.Fatalf("precondition: tracker should be complete after all peers recorded")
	}
	ctrl := makeController(t, lifecycle.READY)
	handler := NewWithStoreAndCallbackAndRetentionAndInitialSync(Node{ID: "node-1"}, newTestStore(t), nil, ctrl, 0, tracker)

	code, resp := getReadyz(t, handler)
	if code != http.StatusOK {
		t.Errorf("enabled tracker (complete) + READY lifecycle: expected 200, got %d", code)
	}
	if !resp.Ready {
		t.Errorf("enabled tracker (complete): expected Ready=true, got false")
	}
	if resp.Reason != "" {
		t.Errorf("enabled tracker (complete): expected empty reason, got %q", resp.Reason)
	}
	if !resp.InitialSyncComplete {
		t.Errorf("enabled tracker (complete): expected InitialSyncComplete=true, got false")
	}
	if resp.InitialSyncCompleted != 4 || resp.InitialSyncExpected != 4 {
		t.Errorf("enabled tracker (complete): expected Completed=4 Expected=4, got %d/%d", resp.InitialSyncCompleted, resp.InitialSyncExpected)
	}
}

// --- Scenario 4: A missing or unreachable expected peer prevents readiness ---

// With the barrier still pending (one expected peer outstanding), /readyz
// returns 503 even when the lifecycle authorizes readiness. The missing peer
// is reported in the response (InitialSyncMissing).
func TestReadyzEnabledTrackerPartial503(t *testing.T) {
	tracker := antientropy.NewInitialSyncTracker(
		[]string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		"gossipgrid-0",
	)
	// Only 1 of 2 expected peers has a full pass; gossipgrid-2 is still
	// outstanding (unreachable / not yet synced).
	tracker.RecordComplete("gossipgrid-1")
	if tracker.Complete() {
		t.Fatalf("precondition: tracker should be pending (one peer outstanding)")
	}
	ctrl := makeController(t, lifecycle.READY)
	handler := NewWithStoreAndCallbackAndRetentionAndInitialSync(Node{ID: "node-1"}, newTestStore(t), nil, ctrl, 0, tracker)

	code, resp := getReadyz(t, handler)
	if code != http.StatusServiceUnavailable {
		t.Errorf("enabled tracker (1 of 2 peers): expected 503, got %d", code)
	}
	if resp.Ready {
		t.Errorf("enabled tracker (partial): expected Ready=false, got true")
	}
	if resp.Reason != "initial_sync_pending" {
		t.Errorf("enabled tracker (partial): expected reason initial_sync_pending, got %q", resp.Reason)
	}
	if resp.InitialSyncCompleted != 1 || resp.InitialSyncExpected != 2 {
		t.Errorf("enabled tracker (partial): expected Completed=1 Expected=2, got %d/%d", resp.InitialSyncCompleted, resp.InitialSyncExpected)
	}
	if len(resp.InitialSyncMissing) != 1 || resp.InitialSyncMissing[0] != "gossipgrid-2" {
		t.Errorf("enabled tracker (partial): expected Missing=[gossipgrid-2], got %v", resp.InitialSyncMissing)
	}
}

// --- Scenario 5: Lifecycle authorization continues to take precedence ---

// When the barrier is complete but the lifecycle is NOT authorized (e.g.
// RECOVERY_REQUIRED), /readyz returns 503. The reason is the lifecycle
// authorization reason (not initial_sync_pending), because the barrier is no
// longer the failing condition — lifecycle revocation takes precedence over
// a complete barrier. This is the "the barrier must never override
// metadata/epoch/lockout/recovery/stopped failures" requirement.
func TestReadyzLifecycleRevocationTakesPrecedence(t *testing.T) {
	// A single-replica tracker (vacuously complete) so the barrier is not the
	// failing condition.
	tracker := antientropy.NewInitialSyncTracker(
		[]string{"gossipgrid-0"},
		"gossipgrid-0",
	)
	if !tracker.Complete() {
		t.Fatalf("precondition: single-replica tracker should be vacuously complete")
	}
	// RECOVERY_REQUIRED: the lifecycle is not authorized (recovery lockout).
	ctrl := makeController(t, lifecycle.RECOVERY_REQUIRED)
	handler := NewWithStoreAndCallbackAndRetentionAndInitialSync(Node{ID: "node-1"}, newTestStore(t), nil, ctrl, 0, tracker)

	code, resp := getReadyz(t, handler)
	if code != http.StatusServiceUnavailable {
		t.Errorf("complete barrier + RECOVERY_REQUIRED lifecycle: expected 503, got %d", code)
	}
	if resp.Ready {
		t.Errorf("complete barrier + RECOVERY_REQUIRED: expected Ready=false, got true")
	}
	// The reason must be the lifecycle reason, not initial_sync_pending (the
	// barrier is complete, so it is not the failing condition).
	if resp.Reason == "initial_sync_pending" {
		t.Errorf("complete barrier + RECOVERY_REQUIRED: reason is initial_sync_pending, want the lifecycle reason (lifecycle takes precedence over a complete barrier)")
	}
	if resp.Reason == "" {
		t.Errorf("complete barrier + RECOVERY_REQUIRED: expected a non-empty lifecycle reason, got empty")
	}
	if !resp.InitialSyncComplete {
		t.Errorf("complete barrier: expected InitialSyncComplete=true, got false")
	}
}

// The same precedence holds for a stopped controller: a stopped controller
// must not authorize readiness even when the barrier is complete.
func TestReadyzStoppedControllerTakesPrecedence(t *testing.T) {
	tracker := antientropy.NewInitialSyncTracker(
		[]string{"gossipgrid-0"},
		"gossipgrid-0",
	)
	// Start a controller that authorizes readiness, then stop it. A stopped
	// controller must not authorize (fail-closed).
	ctrl := makeController(t, lifecycle.READY)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	ctrl.Stop(stopCtx)
	stopCancel()

	handler := NewWithStoreAndCallbackAndRetentionAndInitialSync(Node{ID: "node-1"}, newTestStore(t), nil, ctrl, 0, tracker)

	code, resp := getReadyz(t, handler)
	if code != http.StatusServiceUnavailable {
		t.Errorf("complete barrier + stopped controller: expected 503, got %d", code)
	}
	if resp.Ready {
		t.Errorf("complete barrier + stopped controller: expected Ready=false, got true")
	}
	if resp.Reason == "initial_sync_pending" {
		t.Errorf("complete barrier + stopped controller: reason is initial_sync_pending, want the lifecycle reason (a stopped controller takes precedence over a complete barrier)")
	}
}
