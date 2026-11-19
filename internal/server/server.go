// Package server exposes GossipGrid's public HTTP endpoints: health, readiness,
// and a simple root page identifying the service and node.
//
// Handlers are kept separate from main so they can be tested with httptest
// without starting the application. For this milestone the health responses are
// deliberately provisional; later milestones connect readiness to database,
// synchronization, and security-credential state.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Node identifies the serving replica to the browser.
type Node struct {
	ID string
}

// New constructs an http.Handler for the public listener with the given node
// identity. Timeouts are applied by the caller (see cmd/gossipgrid/main.go) so
// this stays focused on routing and responses.
func New(node Node) http.Handler {
	mux := http.NewServeMux()
	// "GET /{$}" matches the root path exactly ({$} = end-of-path), so
	// unrecognized paths fall through to 404 rather than serving the root page.
	// The method-parsed patterns (GET /healthz, GET /readyz) apply method
	// restrictions to those exact paths, returning 405 for other methods.
	mux.HandleFunc("GET /{$}", rootHandler(node))
	mux.HandleFunc("GET /healthz", healthHandler)
	mux.HandleFunc("GET /readyz", readyHandler)
	return mux
}

// healthHandler reports liveness: the process is running and can serve.
// It intentionally does not probe peers, storage, or credentials.
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, healthResponse{
		Service: "gossipgrid",
		Status:  "healthy",
	})
}

// readyHandler reports that the node has completed initialization and may
// participate in the cluster. For this milestone the node is ready once it has
// started; later milestones gate this on sync/DB/TLS readiness.
func readyHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, readyResponse{
		Service: "gossipgrid",
		Ready:   true,
	})
}

// rootHandler returns a small HTML page identifying the service and node.
func rootHandler(node Node) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// Fprintf error is unlikely (write to a valid ResponseWriter) but
		// must be checked to satisfy errcheck; the error is ignored because
		// there is no useful recovery for a broken client connection.
		if _, err := fmt.Fprintf(w, rootTemplate, node.ID); err != nil {
			return
		}
	}
}

type healthResponse struct {
	Service string `json:"service"`
	Status  string `json:"status"`
}

type readyResponse struct {
	Service string `json:"service"`
	Ready   bool   `json:"ready"`
}

const rootTemplate = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>GossipGrid</title>
</head>
<body>
  <h1>GossipGrid</h1>
  <p>Serving node: <code>%s</code></p>
  <p>Endpoints: <a href="/healthz">/healthz</a> · <a href="/readyz">/readyz</a></p>
</body>
</html>
`

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// Server is a thin wrapper around http.Server so callers (and tests) can
// construct one with explicit timeouts without the application's main logic.
type Server struct {
	HTTPServer *http.Server
}

// NewServer builds a Server with the given listener address, handler, and a
// conservative set of timeouts appropriate for a service that must remain
// responsive and shed slow connections.
func NewServer(addr string, handler http.Handler) *Server {
	return &Server{
		HTTPServer: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}
}

// ListenAndServe starts the HTTP server on the configured address.
func (s *Server) ListenAndServe() error {
	return s.HTTPServer.ListenAndServe()
}

// Shutdown gracefully stops the server, giving in-flight requests the supplied
// context's deadline to complete.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.HTTPServer.Shutdown(ctx)
}
