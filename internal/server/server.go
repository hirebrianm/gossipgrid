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
	"html/template"
	"net"
	"net/http"
	"time"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/store"
)

// Node identifies the serving replica to the browser.
type Node struct {
	ID string
}

// New constructs an http.Handler for the public listener with the given node
// identity. Timeouts are applied by the caller (see cmd/gossipgrid/main.go) so
// this stays focused on routing and responses.
func New(node Node) http.Handler {
	return NewWithStore(node, nil)
}

// NewWithStore constructs an http.Handler with the given node identity and
// optional local message store. When store is nil, message endpoints are not
// registered.
func NewWithStore(node Node, s *store.Store) http.Handler {
	return NewWithStoreAndCallback(node, s, nil)
}

// NewWithStoreAndCallback is like NewWithStore but also accepts an optional
// MessageCallback that is invoked after each successful local message commit
// (POST /api/messages). This is used by the gossip engine (Milestone 4B) to
// trigger asynchronous peer fanout. The callback must be non-blocking.
func NewWithStoreAndCallback(node Node, s *store.Store, onMessage MessageCallback) http.Handler {
	return NewWithStoreAndCallbackAndLifecycle(node, s, onMessage, nil)
}

// NewWithStoreAndCallbackAndLifecycle is like NewWithStoreAndCallback but
// also accepts an optional lifecycle.Controller. When the controller is
// non-nil and lifecycle protection is enabled, readiness is gated on the
// controller's state, and public message writes are rejected while the
// replica is unready.
//
// When the controller is nil (or lifecycle protection is disabled), the
// existing Milestone 4 behavior is preserved: /readyz always returns 200
// and POST /api/messages is always accepted.
func NewWithStoreAndCallbackAndLifecycle(node Node, s *store.Store, onMessage MessageCallback, ctrl *lifecycle.Controller) http.Handler {
	return newMux(node, s, onMessage, ctrl, 0, time.Now)
}

// NewWithStoreAndCallbackAndRetention is like NewWithStoreAndCallbackAnd
// Lifecycle but additionally accepts a message retention TTL.
// When ttl is non-zero, expired messages are excluded from the public
// message API (GET /api/messages and GET /api/messages/{id}). When ttl is
// zero (the default), retention is disabled and the behavior is identical to
// NewWithStoreAndCallbackAndLifecycle.
func NewWithStoreAndCallbackAndRetention(node Node, s *store.Store, onMessage MessageCallback, ctrl *lifecycle.Controller, ttl time.Duration) http.Handler {
	return newMux(node, s, onMessage, ctrl, ttl, time.Now)
}

// newMux builds the public message-API mux with an optional lifecycle
// controller and an optional message retention TTL.
func newMux(node Node, s *store.Store, onMessage MessageCallback, ctrl *lifecycle.Controller, ttl time.Duration, now func() time.Time) http.Handler {
	mux := http.NewServeMux()
	// "GET /{$}" matches the root path exactly ({$} = end-of-path), so
	// unrecognized paths fall through to 404 rather than serving the root page.
	// The method-parsed patterns (GET /healthz, GET /readyz) apply method
	// restrictions to those exact paths, returning 405 for other methods.
	mux.HandleFunc("GET /{$}", rootHandler(node))
	mux.HandleFunc("GET /healthz", healthHandler)

	// Readiness: when a lifecycle controller is provided, use the
	// lifecycle-aware ready handler; otherwise, use the legacy handler
	// (always 200).
	if ctrl != nil && ctrl.Admission().Enabled() {
		mux.HandleFunc("GET /readyz", lifecycleReadyHandler(ctrl))
	} else {
		mux.HandleFunc("GET /readyz", readyHandler)
	}

	if s != nil {
		mh := newMessageHandler(s)
		mh.nodeID = node.ID
		mh.onMessage = onMessage
		if ctrl != nil && ctrl.Admission().Enabled() {
			mh.lifecycleCtrl = ctrl
		}
		mh.retentionTTL = ttl
		mh.now = now
		mux.HandleFunc("POST /api/messages", mh.postMessages)
		mux.HandleFunc("GET /api/messages", mh.getMessages)
		mux.HandleFunc("GET /api/messages/{id}", mh.getMessage)
	}

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

// lifecycleReadyHandler reports readiness based on the lifecycle
// controller's single consistent authorization decision. It returns HTTP 200
// only when the replica is authorized (ready state plus a fresh, valid,
// matching authoritative observation plus a matching durable database binding,
// no lockout, not stopped); HTTP 503 otherwise. It includes a structured
// response with readiness, state, and reason.
//
// Using the same authorization decision as the public and private handlers
// guarantees that a replica reported "ready" by /readyz is one that the
// handlers would actually authorize to serve: the two cannot diverge (e.g. a
// stale metadata observation or a missing binding that makes the state READY
// but the authorization fail closed).
//
// It does NOT hold a lifecycle mutex while performing network operations
// (there are none in this handler). It does NOT synchronously probe every
// peer on every readiness request.
func lifecycleReadyHandler(ctrl *lifecycle.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		snap := ctrl.Authorization()
		ready := snap.Authorized()

		status := http.StatusServiceUnavailable
		if ready {
			status = http.StatusOK
		}

		writeJSON(w, status, lifecycleReadyResponse{
			Service: "gossipgrid",
			Ready:   ready,
			State:   snap.State.String(),
			Reason:  snap.Reason,
		})
	}
}

// rootHandler returns a small HTML page identifying the service and node.
// The node ID is rendered through html/template, which escapes dynamic
// values, so a node ID containing HTML/script markup is neutralised.
func rootHandler(node Node) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		// Write error is unlikely (write to a valid ResponseWriter) but must
		// be checked; the error is ignored because there is no useful
		// recovery for a broken client connection.
		if err := rootTmpl.Execute(w, struct {
			NodeID string
		}{NodeID: node.ID}); err != nil {
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

type lifecycleReadyResponse struct {
	Service string `json:"service"`
	Ready   bool   `json:"ready"`
	State   string `json:"state"`
	Reason  string `json:"reason,omitempty"`
}

var rootTmpl = template.Must(template.New("root").Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>GossipGrid</title>
</head>
<body>
  <h1>GossipGrid</h1>
  <p>Serving node: <code>{{.NodeID}}</code></p>
  <p>Endpoints: <a href="/healthz">/healthz</a> · <a href="/readyz">/readyz</a></p>
</body>
</html>
`))

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

// Serve starts the HTTP server on the supplied listener. It is provided so
// tests (and other callers) can bind a listener once and serve through the
// retained net.Listener, avoiding the race of closing a port-reservation
// listener and re-binding the same address.
func (s *Server) Serve(ln net.Listener) error {
	return s.HTTPServer.Serve(ln)
}

// Shutdown gracefully stops the server, giving in-flight requests the supplied
// context's deadline to complete.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.HTTPServer.Shutdown(ctx)
}
