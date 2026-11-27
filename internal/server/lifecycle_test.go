package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/store"
)

// jsonBody wraps a JSON string in an io.ReadCloser for httptest requests.
func jsonBody(s string) io.ReadCloser {
	return io.NopCloser(bytes.NewReader([]byte(s)))
}

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	st, err := store.New(dir + "/test.db")
	if err != nil {
		t.Fatalf("opening test store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func makeController(t *testing.T, state lifecycle.State) *lifecycle.Controller {
	t.Helper()
	return makeControllerWithMetadata(t, state, nil)
}

// defaultTestMetadata is the metadata the default server-test controller is
// bound to, so a READY or DEGRADED controller is fully authorized (ready +
// fresh + matching durable binding).
var defaultTestMetadata = lifecycle.ClusterMetadata{
	ClusterID:   "test-cluster",
	EpochID:     "test-epoch",
	Generation:  1,
	Initialized: true,
}

// makeControllerWithMetadata creates a controller in the given state with a
// real (bound) store and an available metadata source. When meta is nil the
// controller uses defaultTestMetadata. The store is bound to the metadata so
// the authorization snapshot (readiness, write admission, peer status) can be
// authorized: ready state + fresh valid observation + matching durable
// binding + no lockout. This mirrors production, where readiness and
// admission read the same decision.
func makeControllerWithMetadata(t *testing.T, state lifecycle.State, meta *lifecycle.ClusterMetadata) *lifecycle.Controller {
	t.Helper()
	if meta == nil {
		m := defaultTestMetadata
		meta = &m
	}
	machine := lifecycle.NewMachine(nil)
	// Force the machine to the desired state.
	switch state {
	case lifecycle.STARTING:
		// Already in STARTING.
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

	st := newTestStore(t)
	// Bind the store to the metadata so the controller can be authorized.
	// The bind is done while the database is empty.
	if _, err := st.Bind(context.Background(), meta.ClusterID, meta.EpochID, meta.Generation, lifecycle.ProvenanceGenesis); err != nil {
		t.Fatalf("binding test store: %v", err)
	}

	src := lifecycle.NewFakeMetadataSource(*meta, true)
	ctrl := lifecycle.NewController(lifecycle.Config{
		MetadataPollInterval: time.Hour,
		Enabled:              true,
	}, machine, src, st, nil)

	// Set the accepted metadata directly (bypassing the metadata worker)
	// so the admission check sees a valid, fresh observation.
	ctrl.SetAcceptedMetadataForTest(*meta)
	return ctrl
}

func TestReadinessDisabled(t *testing.T) {
	st := newTestStore(t)
	handler := NewWithStoreAndCallback(Node{ID: "node-1"}, st, nil)

	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/readyz", nil)
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("lifecycle disabled: expected 200, got %d", w.Code)
	}
}

func TestReadinessEnabledUnready(t *testing.T) {
	st := newTestStore(t)
	ctrl := makeController(t, lifecycle.STARTING)
	handler := NewWithStoreAndCallbackAndLifecycle(Node{ID: "node-1"}, st, nil, ctrl)

	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/readyz", nil)
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("STARTING: expected 503, got %d", w.Code)
	}

	var resp combinedReadyResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if resp.Ready {
		t.Error("STARTING: expected Ready=false")
	}
	if resp.State != "STARTING" {
		t.Errorf("STARTING: expected state STARTING, got %s", resp.State)
	}
}

func TestReadinessEnabledReady(t *testing.T) {
	st := newTestStore(t)
	ctrl := makeController(t, lifecycle.READY)
	handler := NewWithStoreAndCallbackAndLifecycle(Node{ID: "node-1"}, st, nil, ctrl)

	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/readyz", nil)
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("READY: expected 200, got %d", w.Code)
	}

	var resp combinedReadyResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if !resp.Ready {
		t.Error("READY: expected Ready=true")
	}
}

func TestReadinessEnabledDegraded(t *testing.T) {
	st := newTestStore(t)
	ctrl := makeController(t, lifecycle.DEGRADED)
	handler := NewWithStoreAndCallbackAndLifecycle(Node{ID: "node-1"}, st, nil, ctrl)

	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/readyz", nil)
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("DEGRADED: expected 200, got %d", w.Code)
	}
}

func TestReadinessEnabledRecoveryRequired(t *testing.T) {
	st := newTestStore(t)
	ctrl := makeController(t, lifecycle.RECOVERY_REQUIRED)
	handler := NewWithStoreAndCallbackAndLifecycle(Node{ID: "node-1"}, st, nil, ctrl)

	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/readyz", nil)
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("RECOVERY_REQUIRED: expected 503, got %d", w.Code)
	}
}

func TestPostMessageRejectedWhileUnready(t *testing.T) {
	st := newTestStore(t)
	ctrl := makeController(t, lifecycle.STARTING)
	handler := NewWithStoreAndCallbackAndLifecycle(Node{ID: "node-1"}, st, nil, ctrl)

	body := `{"body":"hello"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/api/messages", nil)
	req.Header.Set("Content-Type", "application/json")
	req.Body = jsonBody(body)
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("STARTING: expected 503 for POST, got %d", w.Code)
	}
	// No message should have been inserted.
	count, err := st.CountMessages()
	if err != nil {
		t.Fatalf("CountMessages: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 messages after rejected write, got %d", count)
	}
}

func TestPostMessageAllowedWhenReady(t *testing.T) {
	st := newTestStore(t)
	meta := &lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}
	ctrl := makeControllerWithMetadata(t, lifecycle.READY, meta)
	handler := NewWithStoreAndCallbackAndLifecycle(Node{ID: "node-1"}, st, nil, ctrl)

	body := `{"body":"hello"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/api/messages", nil)
	req.Header.Set("Content-Type", "application/json")
	req.Body = jsonBody(body)
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("READY: expected 201 for POST, got %d", w.Code)
	}
	count, err := st.CountMessages()
	if err != nil {
		t.Fatalf("CountMessages: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 message after accepted write, got %d", count)
	}
}

func TestHealthzIndependentOfLifecycle(t *testing.T) {
	st := newTestStore(t)
	ctrl := makeController(t, lifecycle.RECOVERY_REQUIRED)
	handler := NewWithStoreAndCallbackAndLifecycle(Node{ID: "node-1"}, st, nil, ctrl)

	w := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/healthz", nil)
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("healthz with RECOVERY_REQUIRED: expected 200, got %d", w.Code)
	}
}
