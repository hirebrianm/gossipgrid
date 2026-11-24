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
	"gossipgrid/internal/readiness"
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
	return newMux(node, s, onMessage, ctrl, 0, nil, time.Now)
}

// NewWithStoreAndCallbackAndRetention is like NewWithStoreAndCallbackAnd
// Lifecycle but additionally accepts a message retention TTL.
// When ttl is non-zero, expired messages are excluded from the public
// message API (GET /api/messages and GET /api/messages/{id}). When ttl is
// zero (the default), retention is disabled and the behavior is identical to
// NewWithStoreAndCallbackAndLifecycle.
func NewWithStoreAndCallbackAndRetention(node Node, s *store.Store, onMessage MessageCallback, ctrl *lifecycle.Controller, ttl time.Duration) http.Handler {
	return NewWithStoreAndCallbackAndRetentionAndInitialSync(node, s, onMessage, ctrl, ttl, nil)
}

// NewWithStoreAndCallbackAndRetentionAndInitialSync is like
// NewWithStoreAndCallbackAndRetention but additionally accepts an optional
// initial-sync barrier tracker (Milestone 7A enhancement, synchronization-
// aware Kubernetes readiness). When the tracker is non-nil (the barrier is
// enabled), /readyz returns HTTP 503 until the replica has completed its
// initial anti-entropy synchronization against all expected remote peers,
// and only then returns 200 (subject to any existing lifecycle
// authorization). When the tracker is nil (the default), the behavior is
// identical to NewWithStoreAndCallbackAndRetention.
//
// The readiness handler reads only concurrency-safe in-memory state from the
// tracker; it performs no network requests or SQLite inventory scans.
// /healthz semantics are preserved unchanged (always 200).
func NewWithStoreAndCallbackAndRetentionAndInitialSync(node Node, s *store.Store, onMessage MessageCallback, ctrl *lifecycle.Controller, ttl time.Duration, tracker readiness.TrackerProvider) http.Handler {
	return newMux(node, s, onMessage, ctrl, ttl, tracker, time.Now)
}

// newMux builds the public message-API mux with an optional lifecycle
// controller, an optional message retention TTL, and an optional initial-sync
// barrier tracker.
func newMux(node Node, s *store.Store, onMessage MessageCallback, ctrl *lifecycle.Controller, ttl time.Duration, tracker readiness.TrackerProvider, now func() time.Time) http.Handler {
	mux := http.NewServeMux()
	// "GET /{$}" matches the root path exactly ({$} = end-of-path), so
	// unrecognized paths fall through to 404 rather than serving the root page.
	// The method-parsed patterns (GET /healthz, GET /readyz) apply method
	// restrictions to those exact paths, returning 405 for other methods.
	mux.HandleFunc("GET /{$}", rootHandler(node))
	mux.HandleFunc("GET /healthz", healthHandler)

	// Readiness: the readiness decision is the logical combination of the
	// existing lifecycle authorization (when lifecycle protection is
	// enabled) and the initial-sync barrier (when enabled). This is a single
	// handler, not two independent handlers that can disagree:
	//   - both conditions must hold for 200;
	//   - if either condition fails, 503 is returned with a reason naming
	//     the failing condition (initial_sync_pending takes precedence over
	//     a lifecycle reason when the barrier is still pending, because the
	//     barrier is the startup gate; a lifecycle failure is reported when
	//     the barrier is complete but lifecycle authorization is not).
	mux.HandleFunc("GET /readyz", combinedReadyHandler(ctrl, tracker))

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

// combinedReadyHandler reports readiness as the logical combination of the
// existing lifecycle authorization (when lifecycle protection is enabled) and
// the initial-sync barrier (when enabled). It is a single handler, not two
// independent handlers that can disagree.
//
// Decision (fail-closed):
//   - 200 only when BOTH hold: the lifecycle authorizes readiness (or
//     lifecycle protection is disabled, in which case it is vacuously true)
//     AND the initial-sync barrier is complete (or disabled, in which case it
//     is vacuously true).
//   - 503 otherwise, with a reason naming the failing condition.
//
// Reason precedence: when the initial-sync barrier is enabled and still
// pending, the reason is "initial_sync_pending" (the barrier is the startup
// gate; the pod is NotReady regardless of lifecycle state). When the barrier
// is complete (or disabled) but lifecycle authorization is not, the reason is
// the lifecycle authorization reason. This ordering keeps the startup gate
// visible to the operator while still surfacing lifecycle failures after
// synchronization completes.
//
// The handler reads only concurrency-safe in-memory state (one tracker Status
// call, one controller Authorization snapshot). It performs no network
// requests and no SQLite inventory scans. It does not hold a lifecycle mutex
// across network operations (there are none).
func combinedReadyHandler(ctrl *lifecycle.Controller, tracker readiness.TrackerProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		// Initial-sync barrier status. A nil tracker means the barrier is
		// disabled: the barrier imposes no constraint (treated as complete).
		// Guard against BOTH a nil interface (an untyped nil passed to the
		// interface parameter) and a nil *InitialSyncTracker boxed into the
		// interface (the typed-nil case main.go produces when the barrier is
		// off). Both represent the disabled state.
		var isStatus readiness.Status
		if tracker == nil {
			isStatus = readiness.Status{Complete: true, State: "disabled"}
		} else {
			isStatus = tracker.Status()
		}
		// Lifecycle authorization (nil controller = disabled = authorized).
		var lState, lReason string
		lifecycleAuthorized := true
		if ctrl != nil && ctrl.Admission().Enabled() {
			snap := ctrl.Authorization()
			lifecycleAuthorized = snap.Authorized()
			lState = snap.State.String()
			lReason = snap.Reason
		}

		ready := lifecycleAuthorized && isStatus.Complete
		status := http.StatusServiceUnavailable
		if ready {
			status = http.StatusOK
		}

		// Determine the reason: the failing condition. The barrier takes
		// precedence while pending (it is the startup gate); a lifecycle
		// failure is reported when the barrier is complete but the
		// lifecycle is not.
		reason := ""
		if !ready {
			if !isStatus.Complete {
				reason = "initial_sync_pending"
			} else if !lifecycleAuthorized {
				reason = lReason
			}
		}

		writeJSON(w, status, combinedReadyResponse{
			Service:              "gossipgrid",
			Ready:                ready,
			State:                lState,
			Reason:               reason,
			InitialSyncComplete:  isStatus.Complete,
			InitialSyncCompleted: isStatus.Completed,
			InitialSyncExpected:  isStatus.Expected,
			InitialSyncMissing:   isStatus.MissingPeers,
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

// combinedReadyResponse is the /readyz response. It carries the readiness
// decision plus, when the initial-sync barrier is enabled, concise, safe
// progress information (completed vs expected peer passes and the set of
// peers still outstanding). It never exposes credentials, private keys, or
// message contents. The State field is populated only when lifecycle
// protection is enabled; the InitialSync* fields are populated only when the
// barrier is enabled (a nil tracker renders them as the disabled/complete
// view, which is correct: the barrier imposes no constraint).
type combinedReadyResponse struct {
	Service              string   `json:"service"`
	Ready                bool     `json:"ready"`
	State                string   `json:"state,omitempty"`
	Reason               string   `json:"reason,omitempty"`
	InitialSyncComplete  bool     `json:"initial_sync_complete"`
	InitialSyncCompleted int      `json:"initial_sync_completed"`
	InitialSyncExpected  int      `json:"initial_sync_expected"`
	InitialSyncMissing   []string `json:"initial_sync_missing,omitempty"`
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
