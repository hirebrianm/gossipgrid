package peertls

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
)

// setupSyncTestWithProvider is like setupSyncTest but additionally sets the
// given EpochProvider on the returned client, enabling tests of the
// lifecycle-response-envelope validation path in FetchMessages.
func setupSyncTestWithProvider(t *testing.T, handler http.Handler, p EpochProvider) (addr string, client *Client) {
	t.Helper()
	ca := newTestCA(t, "w")
	cert, key, trust := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, cert, key, trust, "gossipgrid-0", 50*time.Millisecond)
	a, stop := startTestServer(t, mgr, handler)
	t.Cleanup(stop)
	client = NewClient(mgr)
	if p != nil {
		client.SetEpochProvider(p)
	}
	return a, client
}

// TestFetchMessages404AllMissingWithInvalidEnvelope verifies that when all
// requested IDs are reported missing in a 404 response and the response
// carries an invalid lifecycle envelope (wrong epoch), the client rejects the
// response with a PeerAckError rather than accepting it as successful
// reconciliation. This is the regression test for the defect where the 404
// missing-ID handling occurred before validateResponseEnvelope, allowing a
// wrong-epoch peer to confirm message absence and drive the cursor forward.
func TestFetchMessages404AllMissingWithInvalidEnvelope(t *testing.T) {
	ids := genTestUUIDs(2)

	// Lifecycle enforcement enabled locally with a specific epoch context.
	provider := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Set a WRONG-epoch response envelope (lifecycle active, but epoch "e2"
		// instead of the request's "e1").
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
		w.Header().Set(lifecycle.HeaderRespEpochID, "e2")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(missingIDsResponse{
			Error:      "message IDs not found: " + ids[0] + ", " + ids[1],
			MissingIDs: ids,
		})
	})
	addr, client := setupSyncTestWithProvider(t, mux, provider)

	_, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids)
	if err == nil {
		t.Fatal("FetchMessages with all-missing 404 and wrong-epoch envelope: expected error, got nil (envelope must be validated before trusting 404)")
	}
	var ackErr *PeerAckError
	if !errors.As(err, &ackErr) {
		t.Fatalf("FetchMessages: error type = %T, want *PeerAckError (invalid envelope); got: %v", err, err)
	}
}

// TestFetchMessages404PartialMissingWithInvalidEnvelope verifies that when some
// requested IDs are reported missing in a 404 response and the response carries
// a legacy (no-envelope) or contradictory lifecycle envelope, the client
// rejects the response with a PeerAckError before attempting the retry.
func TestFetchMessages404PartialMissingWithInvalidEnvelope(t *testing.T) {
	ids := genTestUUIDs(2)
	missingID := ids[1]

	// Lifecycle enforcement enabled locally; the peer is legacy (no
	// lifecycle envelope headers). A lifecycle-enabled client must reject
	// a legacy peer's 404 before trusting the missing-ID list.
	provider := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}

	var mu sync.Mutex
	requestCount := 0

	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		// Legacy peer: no lifecycle envelope headers at all.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(missingIDsResponse{
			Error:      "message IDs not found: " + missingID,
			MissingIDs: []string{missingID},
		})
	})
	addr, client := setupSyncTestWithProvider(t, mux, provider)

	_, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids)
	if err == nil {
		t.Fatal("FetchMessages with partial-missing 404 and legacy envelope: expected error, got nil (legacy peer must be rejected)")
	}
	var ackErr *PeerAckError
	if !errors.As(err, &ackErr) {
		t.Fatalf("FetchMessages: error type = %T, want *PeerAckError (legacy envelope); got: %v", err, err)
	}
	// The retry must NOT have occurred: the 404 was rejected before the
	// client attempted to fetch the present ID.
	mu.Lock()
	defer mu.Unlock()
	if requestCount != 1 {
		t.Errorf("requestCount = %d, want 1 (404 rejected before retry; no retry should occur)", requestCount)
	}
}

// TestFetchMessages404AllMissingWithValidEnvelope verifies that when all
// requested IDs are reported missing in a 404 response and the response
// carries a valid lifecycle envelope (matching the request's authorized
// epoch), the client accepts the response as successful reconciliation with
// zero fetched messages (empty slice, nil error). This confirms the fix
// does not break the legitimate all-missing path when lifecycle is active
// and both peers are on the same epoch.
func TestFetchMessages404AllMissingWithValidEnvelope(t *testing.T) {
	ids := genTestUUIDs(2)

	// Lifecycle enforcement enabled locally with a specific epoch context.
	provider := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}

	var mu sync.Mutex
	requestCount := 0

	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		// Valid lifecycle envelope: lifecycle active, epoch matches the
		// request's authorized context.
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
		w.Header().Set(lifecycle.HeaderRespEpochID, "e1")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(missingIDsResponse{
			Error:      "message IDs not found: " + ids[0] + ", " + ids[1],
			MissingIDs: ids,
		})
	})
	addr, client := setupSyncTestWithProvider(t, mux, provider)

	msgs, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids)
	if err != nil {
		t.Fatalf("FetchMessages with all-missing 404 and valid envelope: err = %v, want nil (valid 404 is successful reconciliation)", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("FetchMessages: len = %d, want 0 (zero fetched messages); got %+v", len(msgs), msgs)
	}
	// No retry should occur (all IDs are missing).
	mu.Lock()
	defer mu.Unlock()
	if requestCount != 1 {
		t.Errorf("requestCount = %d, want 1 (no retry for all-missing)", requestCount)
	}
}

// TestFetchMessages404PartialMissingWithValidEnvelope verifies that when some
// requested IDs are reported missing in a 404 response and the response
// carries a valid lifecycle envelope, the client proceeds to the retry with
// the present IDs and returns the valid records. This confirms the fix
// preserves the legitimate partial-missing 404-retry path when lifecycle is
// active and the envelope is valid.
func TestFetchMessages404PartialMissingWithValidEnvelope(t *testing.T) {
	ids := genTestUUIDs(2)
	presentID, missingID := ids[0], ids[1]
	timestamp := "2026-11-20T12:00:00Z"

	provider := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}

	var mu sync.Mutex
	requestCount := 0

	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requestCount++
		count := requestCount
		mu.Unlock()

		// Valid lifecycle envelope on every response.
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
		w.Header().Set(lifecycle.HeaderRespEpochID, "e1")

		if count == 1 {
			// First request: structured 404 with one ID missing.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(missingIDsResponse{
				Error:      "message IDs not found: " + missingID,
				MissingIDs: []string{missingID},
			})
			return
		}
		// Retry: 200 with only the present ID's record.
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messages": []map[string]string{
				{"id": presentID, "body": "body-0", "origin": "node-1", "created_at": timestamp},
			},
		})
	})
	addr, client := setupSyncTestWithProvider(t, mux, provider)

	msgs, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids)
	if err != nil {
		t.Fatalf("FetchMessages with partial-missing 404 and valid envelope: err = %v, want nil (404-retry should succeed)", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("FetchMessages: len = %d, want 1 (only the present ID); got %+v", len(msgs), msgs)
	}
	if msgs[0].ID != presentID {
		t.Errorf("FetchMessages: ID = %q, want %q (the present ID)", msgs[0].ID, presentID)
	}
	// The retry must have occurred (2 total requests).
	mu.Lock()
	defer mu.Unlock()
	if requestCount != 2 {
		t.Errorf("requestCount = %d, want 2 (original + retry after 404)", requestCount)
	}
}

// TestFetchMessages404MissingEnvelopeHeader verifies that a 404 response
// that is lifecycle-active but missing the Resp-Cluster-ID header is
// rejected when lifecycle enforcement is enabled. This covers the
// "missing envelope header" requirement.
func TestFetchMessages404MissingEnvelopeHeader(t *testing.T) {
	ids := genTestUUIDs(2)

	provider := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		// Lifecycle active but Resp-Cluster-ID header is missing.
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespEpochID, "e1")
		// No HeaderRespClusterID.
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(missingIDsResponse{
			Error:      "message IDs not found: " + ids[0] + ", " + ids[1],
			MissingIDs: ids,
		})
	})
	addr, client := setupSyncTestWithProvider(t, mux, provider)

	_, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids)
	if err == nil {
		t.Fatal("FetchMessages with missing ClusterID header in 404 envelope: expected error, got nil")
	}
	var ackErr *PeerAckError
	if !errors.As(err, &ackErr) {
		t.Fatalf("FetchMessages: error type = %T, want *PeerAckError (missing header); got: %v", err, err)
	}
}

// TestFetchMessages404LegacyDisabled verifies that when lifecycle
// enforcement is DISABLED (no provider or enforcement=false), a 404 response
// without any lifecycle envelope headers is accepted (legacy behavior
// preserved). This confirms the fix does not change behavior for
// non-lifecycle configurations.
func TestFetchMessages404LegacyDisabled(t *testing.T) {
	ids := genTestUUIDs(2)

	// No provider: lifecycle enforcement disabled, no envelope validation.
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		// Legacy peer: no lifecycle envelope headers.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(missingIDsResponse{
			Error:      "message IDs not found: " + ids[0] + ", " + ids[1],
			MissingIDs: ids,
		})
	})
	addr, client := setupSyncTestWithProvider(t, mux, nil)

	msgs, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids)
	if err != nil {
		t.Fatalf("FetchMessages with all-missing 404 and no lifecycle: err = %v, want nil (legacy behavior preserved)", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("FetchMessages: len = %d, want 0 (zero fetched messages); got %+v", len(msgs), msgs)
	}
}
