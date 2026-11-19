package server

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestRootHandlerEscapesNodeID verifies that a node ID containing HTML/script
// markup is escaped in the root page output (no raw <script> tag is emitted).
func TestRootHandlerEscapesNodeID(t *testing.T) {
	evilID := `<script>alert("xss")</script>`
	h := NewWithStore(Node{ID: evilID}, nil)

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	// The raw script tag must not appear; it must be HTML-escaped.
	if strings.Contains(body, "<script>") {
		t.Errorf("root page contains unescaped <script>: %q", body)
	}
	if !strings.Contains(body, "&lt;script&gt;") {
		t.Errorf("root page does not contain the escaped node ID: %q", body)
	}
}

// TestPostOversizedRequestWithContentLength posts a body whose Content-Length
// exceeds the limit and expects 413.
func TestPostOversizedRequestWithContentLength(t *testing.T) {
	h := newTestHandler(t)

	// 10 KiB body, well over maxRequestBytes (8192).
	body := strings.Repeat("a", 10240)
	raw := `{"body":"` + body + `"}`
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/api/messages",
		strings.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	// Set ContentLength explicitly (as a real HTTP client would) so the
	// handler's r.ContentLength check fires before reading.
	req.ContentLength = int64(len(raw))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized with Content-Length: status = %d, want 413; body: %s", rr.Code, rr.Body.String())
	}
}

// TestPostOversizedRequestWithoutContentLength posts an oversized body using a
// reader that does not report a length (ContentLength == -1, as for chunked
// requests) and expects 413 via the MaxBytesReader path.
func TestPostOversizedRequestWithoutContentLength(t *testing.T) {
	h := newTestHandler(t)

	body := strings.NewReader(`{"body":"` + strings.Repeat("a", 10240) + `"}`)
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/api/messages", body)
	req.Header.Set("Content-Type", "application/json")
	// Simulate a chunked request: no Content-Length.
	req.ContentLength = -1

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized without Content-Length (chunked): status = %d, want 413; body: %s", rr.Code, rr.Body.String())
	}
}

// TestPostOversizedChunkedBody posts an oversized body read from a bytes
// reader with a manually cleared ContentLength, modelling chunked transfer.
func TestPostOversizedChunkedBody(t *testing.T) {
	h := newTestHandler(t)

	buf := bytes.NewBufferString(`{"body":"` + strings.Repeat("x", 20000) + `"}`)
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/api/messages", buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Transfer-Encoding", "chunked")
	req.ContentLength = -1

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("chunked oversized: status = %d, want 413; body: %s", rr.Code, rr.Body.String())
	}
}
