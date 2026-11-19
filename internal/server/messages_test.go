package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gossipgrid/internal/store"
)

// newTestHandler builds an http.Handler with a temp-file SQLite store.
func newTestHandler(t *testing.T) http.Handler {
	t.Helper()
	st, err := store.New(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	//nolint:errcheck // test cleanup: Close errors are not actionable
	t.Cleanup(func() { st.Close() })
	return NewWithStore(Node{ID: "test-node"}, st)
}

// postJSON sends a POST to /api/messages with the given JSON body.
func postJSON(t *testing.T, h http.Handler, body any) *httptest.ResponseRecorder {
	t.Helper()
	var reader *bytes.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		reader = bytes.NewReader(data)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/api/messages", reader)
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func TestPostMessage(t *testing.T) {
	h := newTestHandler(t)

	rr := postJSON(t, h, map[string]string{"body": "hello"})
	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body: %s", rr.Code, http.StatusCreated, rr.Body.String())
	}

	var msg store.Message
	if err := json.NewDecoder(rr.Body).Decode(&msg); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if msg.Body != "hello" {
		t.Errorf("Body = %q, want %q", msg.Body, "hello")
	}
	if msg.Origin != "test-node" {
		t.Errorf("Origin = %q, want %q", msg.Origin, "test-node")
	}
	if msg.ID == "" {
		t.Error("ID is empty")
	}
	if msg.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}
}

func TestPostMessageValidation(t *testing.T) {
	h := newTestHandler(t)

	// Unknown field.
	rr := postJSON(t, h, map[string]string{"body": "hi", "extra": "x"})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("unknown field: status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}

	// Empty body.
	rr = postJSON(t, h, map[string]string{"body": ""})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("empty body: status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}

	// Non-string body.
	rr = postJSON(t, h, map[string]int{"body": 42})
	if rr.Code != http.StatusBadRequest {
		t.Errorf("non-string body: status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}

	// Oversized body (>4096 bytes).
	rr = postJSON(t, h, map[string]string{"body": strings.Repeat("a", 5000)})
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized body: status = %d, want 413; body: %s", rr.Code, rr.Body.String())
	}

	// Malformed JSON.
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/api/messages",
		strings.NewReader("{invalid json"))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("malformed JSON: status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}

	// Multiple JSON values.
	req = httptest.NewRequestWithContext(context.Background(), "POST", "/api/messages",
		strings.NewReader(`{"body":"a"}{"body":"b"}`))
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("multiple values: status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
}

func TestPostErrorHasJSONErrorField(t *testing.T) {
	h := newTestHandler(t)

	rr := postJSON(t, h, map[string]string{"body": ""})
	var errResp errorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response: %v; body: %s", err, rr.Body.String())
	}
	if errResp.Error == "" {
		t.Error("error response has empty Error field")
	}
}

func TestGetMessages(t *testing.T) {
	h := newTestHandler(t)

	// Create 3 messages.
	for i := 0; i < 3; i++ {
		rr := postJSON(t, h, map[string]string{"body": "msg"})
		if rr.Code != http.StatusCreated {
			t.Fatalf("create %d: status = %d", i, rr.Code)
		}
	}

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/api/messages", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}

	var resp messageListResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Total != 3 {
		t.Errorf("Total = %d, want 3", resp.Total)
	}
	if len(resp.Messages) != 3 {
		t.Errorf("len(Messages) = %d, want 3", len(resp.Messages))
	}
}

func TestGetMessagesPagination(t *testing.T) {
	h := newTestHandler(t)

	// Create 5 messages.
	for i := 0; i < 5; i++ {
		postJSON(t, h, map[string]string{"body": "msg"})
	}

	// limit=2, offset=0
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/api/messages?limit=2&offset=0", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	var resp messageListResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Messages) != 2 {
		t.Errorf("limit=2: len = %d, want 2", len(resp.Messages))
	}
	if resp.Limit != 2 {
		t.Errorf("limit = %d, want 2", resp.Limit)
	}

	// limit=2, offset=4 → 1 message
	req = httptest.NewRequestWithContext(context.Background(), "GET", "/api/messages?limit=2&offset=4", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Messages) != 1 {
		t.Errorf("limit=2 offset=4: len = %d, want 1", len(resp.Messages))
	}
}

func TestGetMessageByID(t *testing.T) {
	h := newTestHandler(t)

	// Create a message.
	rr := postJSON(t, h, map[string]string{"body": "find me"})
	var created store.Message
	if err := json.NewDecoder(rr.Body).Decode(&created); err != nil {
		t.Fatalf("decode created: %v", err)
	}

	// Fetch by ID.
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/api/messages/"+created.ID, nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}
	var got store.Message
	if err := json.NewDecoder(rr.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ID != created.ID || got.Body != "find me" {
		t.Errorf("got %+v, want id=%s body=%q", got, created.ID, "find me")
	}
}

func TestGetMessageNotFound(t *testing.T) {
	h := newTestHandler(t)

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/api/messages/nonexistent", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body: %s", rr.Code, rr.Body.String())
	}
	var errResp errorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if errResp.Error == "" {
		t.Error("404 error response has empty Error field")
	}
}

func TestMethodNotAllowedOnMessages(t *testing.T) {
	h := newTestHandler(t)

	// PUT /api/messages → 405
	req := httptest.NewRequestWithContext(context.Background(), "PUT", "/api/messages",
		strings.NewReader(`{"body":"x"}`))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("PUT /api/messages: status = %d, want 405", rr.Code)
	}

	// DELETE /api/messages → 405
	req = httptest.NewRequestWithContext(context.Background(), "DELETE", "/api/messages", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("DELETE /api/messages: status = %d, want 405", rr.Code)
	}
}

func TestExistingHealthEndpointsStillWork(t *testing.T) {
	h := newTestHandler(t)

	for _, path := range []string{"/healthz", "/readyz", "/"} {
		req := httptest.NewRequestWithContext(context.Background(), "GET", path, nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("GET %s: status = %d, want 200", path, rr.Code)
		}
	}
}

// TestPostOversizedJSONString posts a JSON body whose total byte length exceeds
// maxRequestBytes (8192) via the first Decode call and expects 413.
func TestPostOversizedJSONString(t *testing.T) {
	h := newTestHandler(t)

	// Build a JSON object whose body string makes the total exceed 8192 bytes.
	pad := strings.Repeat("a", 9000)
	raw := `{"body":"` + pad + `"}`
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/api/messages",
		strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(raw))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized JSON string: status = %d, want 413; body: %s", rr.Code, rr.Body.String())
	}
}

// TestPostChunkedOversized posts an oversized body with ContentLength = -1
// (as for chunked transfer) and expects 413 via the MaxBytesReader path in
// the first Decode call.
func TestPostChunkedOversized(t *testing.T) {
	h := newTestHandler(t)

	pad := strings.Repeat("b", 9000)
	raw := `{"body":"` + pad + `"}`
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/api/messages",
		strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Transfer-Encoding", "chunked")
	req.ContentLength = -1

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("chunked oversized: status = %d, want 413; body: %s", rr.Code, rr.Body.String())
	}
}

// TestPostValidJSONThenExcessiveWhitespace posts a valid JSON object followed
// by enough whitespace to push the total body past maxRequestBytes. The first
// Decode call succeeds (the JSON object is small); the second Decode call (to
// reject trailing values) reads past the limit and MaxBytesReader returns
// *http.MaxBytesError. This must yield 413, not 400.
func TestPostValidJSONThenExcessiveWhitespace(t *testing.T) {
	h := newTestHandler(t)

	// Valid JSON object (~17 bytes) + whitespace padding to exceed 8192 total.
	obj := `{"body":"ok"}`
	whitespace := strings.Repeat(" ", 8500)
	raw := obj + whitespace
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/api/messages",
		strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(raw))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("valid JSON + excessive whitespace: status = %d, want 413; body: %s", rr.Code, rr.Body.String())
	}
}

// TestPostValidJSONThenMoreJSON posts a valid JSON object followed by another
// JSON object, within the size limit. This is a genuine multi-value error and
// must return 400, not 413.
func TestPostValidJSONThenMoreJSON(t *testing.T) {
	h := newTestHandler(t)

	raw := `{"body":"a"}{"body":"b"}`
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/api/messages",
		strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(raw))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("multiple JSON values within limit: status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
}

// TestPostMalformedJSONWithinLimit posts syntactically invalid JSON that is
// well under the size limit and expects 400 (not 413).
func TestPostMalformedJSONWithinLimit(t *testing.T) {
	h := newTestHandler(t)

	req := httptest.NewRequestWithContext(context.Background(), "POST", "/api/messages",
		strings.NewReader(`{"body":`))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = 9

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("malformed JSON within limit: status = %d, want 400; body: %s", rr.Code, rr.Body.String())
	}
}
