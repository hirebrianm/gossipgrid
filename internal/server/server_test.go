package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthz(t *testing.T) {
	h := New(Node{ID: "gossipgrid-0"})
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/healthz", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
		t.Errorf("Content-Type = %q, want JSON", ct)
	}
	var body healthResponse
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Service != "gossipgrid" || body.Status != "healthy" {
		t.Errorf("body = %+v, want service=gossipgrid status=healthy", body)
	}
}

func TestReadyz(t *testing.T) {
	h := New(Node{ID: "gossipgrid-0"})
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/readyz", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	var body combinedReadyResponse
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Service != "gossipgrid" || !body.Ready {
		t.Errorf("body = %+v, want service=gossipgrid ready=true", body)
	}
}

func TestRoot(t *testing.T) {
	node := Node{ID: "gossipgrid-2"}
	h := New(node)
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q, want HTML", ct)
	}
	body, _ := io.ReadAll(rr.Body)
	got := string(body)
	if !contains(got, "GossipGrid") {
		t.Errorf("body should mention GossipGrid: %s", got)
	}
	if !contains(got, node.ID) {
		t.Errorf("body should mention node id %s: %s", node.ID, got)
	}
}

// TestMethodNotAllowed verifies that the ServeMux method-parsed patterns reject
// other methods for the exact paths (e.g. POST /healthz).
func TestMethodNotAllowed(t *testing.T) {
	h := New(Node{ID: "gossipgrid-0"})

	cases := []struct {
		method string
		path   string
	}{
		{"POST", "/healthz"},
		{"PUT", "/readyz"},
		{"DELETE", "/"},
		{"PATCH", "/healthz"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req := httptest.NewRequestWithContext(context.Background(), tc.method, tc.path, nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code == http.StatusOK {
				t.Errorf("%s %s returned 200, want a non-200 (method not allowed)", tc.method, tc.path)
			}
		})
	}
}

// TestUnknownPath verifies that paths not explicitly registered (e.g. /nope)
// return 404, since the root route is now "GET /{$}" (exact root only).
func TestUnknownPath(t *testing.T) {
	h := New(Node{ID: "gossipgrid-0"})

	cases := []string{"/nope", "/api", "/healthz/extra", "/v1/messages"}
	for _, path := range cases {
		t.Run("GET "+path, func(t *testing.T) {
			req := httptest.NewRequestWithContext(context.Background(), "GET", path, nil)
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)
			if rr.Code != http.StatusNotFound {
				t.Errorf("GET %s = %d, want 404", path, rr.Code)
			}
		})
	}
}

// TestRootExact verifies that the root path "/" still returns the root page,
// confirming the "GET /{$}" pattern matches the root exactly.
func TestRootExact(t *testing.T) {
	h := New(Node{ID: "gossipgrid-0"})
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("GET / = %d, want 200", rr.Code)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
