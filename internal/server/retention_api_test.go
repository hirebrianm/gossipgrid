package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gossipgrid/internal/store"
)

// newRetentionHandler builds a message-API handler backed by the given store,
// with the given retention TTL and a fixed clock (for deterministic expiration).
// A zero TTL means retention is disabled (legacy behavior).
func newRetentionHandler(t *testing.T, st *store.Store, ttl time.Duration, now time.Time) http.Handler {
	t.Helper()
	h := &messageHandler{store: st, nodeID: "test-node", retentionTTL: ttl, now: func() time.Time { return now }}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/messages", h.getMessages)
	mux.HandleFunc("GET /api/messages/{id}", h.getMessage)
	return mux
}

// retentionCounter is used to generate deterministic, ordered, valid UUIDv7-style
// IDs with a fixed base timestamp (so they are consistent with the test's fixed
// "now" rather than the wall clock).
var retentionCounter int64

// seedMessage inserts a message with an explicit creation timestamp and a
// deterministic ID whose embedded timestamp matches the creation time.
func seedMessage(t *testing.T, st *store.Store, ts time.Time) string {
	t.Helper()
	retentionCounter++
	id := makeFixedID(ts)
	if err := st.InsertReplicatedMessage(&store.Message{
		ID: id, Body: "retention-api", Origin: "node-0", CreatedAt: ts,
	}); err != nil {
		t.Fatalf("InsertReplicatedMessage: %v", err)
	}
	return id
}

// makeFixedID builds a valid canonical UUIDv7-format string with a fixed
// timestamp embedded (for deterministic, ordered IDs in tests).
func makeFixedID(ts time.Time) string {
	tms := uint64(ts.UnixMilli())
	var b [16]byte
	b[0] = byte(tms >> 40)
	b[1] = byte(tms >> 32)
	b[2] = byte(tms >> 24)
	b[3] = byte(tms >> 16)
	b[4] = byte(tms >> 8)
	b[5] = byte(tms)
	b[6] = 0x70
	b[7] = byte(retentionCounter & 0xFF)
	b[8] = 0x80
	b[9] = 0xCD
	b[10] = 0xEF
	b[11] = 0x01
	b[12] = 0x23
	b[13] = 0x45
	b[14] = byte((retentionCounter >> 8) & 0xFF)
	b[15] = 0x89
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

type apiListResp struct {
	Messages []store.Message `json:"messages"`
	Total    int64           `json:"total"`
}

// TestAPIListExcludesExpired verifies that with retention enabled, expired
// messages are excluded from GET /api/messages (both the payload and the total
// count), while fresh messages are listed.
func TestAPIListExcludesExpired(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour

	st, err := store.New(t.TempDir() + "/retention.db")
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	//nolint:errcheck // test cleanup
	t.Cleanup(func() { st.Close() })

	freshID := seedMessage(t, st, now.Add(-time.Hour))
	_ = seedMessage(t, st, now.Add(-72*time.Hour)) // expired

	h := newRetentionHandler(t, st, ttl, now)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/messages", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp apiListResp
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Total != 1 {
		t.Errorf("total = %d, want 1 (expired excluded from count)", resp.Total)
	}
	if len(resp.Messages) != 1 || resp.Messages[0].ID != freshID {
		t.Errorf("messages = %+v, want only the fresh message %q", resp.Messages, freshID)
	}
}

// TestAPIListDisabledPreservesBehavior verifies that with retention disabled
// (zero TTL), GET /api/messages lists all messages (the legacy behavior).
func TestAPIListDisabledPreservesBehavior(t *testing.T) {
	now := time.Now().UTC()

	st, err := store.New(t.TempDir() + "/retention.db")
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	//nolint:errcheck // test cleanup
	t.Cleanup(func() { st.Close() })

	// Seed a mix of fresh and old messages.
	for _, d := range []int{1, 48, 200} {
		_ = seedMessage(t, st, now.Add(-time.Duration(d)*time.Hour))
	}

	h := newRetentionHandler(t, st, 0, now)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/messages", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp apiListResp
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Total != 3 {
		t.Errorf("total = %d, want 3 (retention disabled lists all)", resp.Total)
	}
	if len(resp.Messages) != 3 {
		t.Errorf("len(messages) = %d, want 3", len(resp.Messages))
	}
}

// TestAPISingleFetchExpiredIsNotFound verifies that with retention enabled,
// GET /api/messages/{id} returns 404 for an expired message (indistinguishable
// from an absent one) and 200 for a fresh one.
func TestAPISingleFetchExpiredIsNotFound(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour

	st, err := store.New(t.TempDir() + "/retention.db")
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	//nolint:errcheck // test cleanup
	t.Cleanup(func() { st.Close() })

	freshID := seedMessage(t, st, now.Add(-time.Hour))
	expiredID := seedMessage(t, st, now.Add(-72*time.Hour))

	h := newRetentionHandler(t, st, ttl, now)

	// Fresh message: 200.
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/messages/"+freshID, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("fresh fetch status = %d, want 200", rec.Code)
	}

	// Expired message: 404.
	req = httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/messages/"+expiredID, nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expired fetch status = %d, want 404", rec.Code)
	}
}

// TestAPISingleFetchDisabledPreservesBehavior verifies that with retention
// disabled, GET /api/messages/{id} returns 200 for an old message (the legacy
// behavior: no expiration).
func TestAPISingleFetchDisabledPreservesBehavior(t *testing.T) {
	now := time.Now().UTC()
	st, err := store.New(t.TempDir() + "/retention.db")
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	//nolint:errcheck // test cleanup
	t.Cleanup(func() { st.Close() })

	oldID := seedMessage(t, st, now.Add(-400*24*time.Hour))

	h := newRetentionHandler(t, st, 0, now)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/messages/"+oldID, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("old-message fetch status = %d, want 200 (retention disabled)", rec.Code)
	}
}
