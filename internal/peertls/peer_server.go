// Package peertls: private peer HTTPS server.
//
// The PeerServer wraps an http.Server that listens on the configured peer
// address (default :8443) and serves a small set of private endpoints that
// require authenticated client certificates. For this milestone it exposes
// only GET /internal/status, which returns the authenticated server's node
// identity and application version.
//
// The server:
//   - uses the Manager's TLS configuration (TLS 1.3, mutual TLS, dedicated CA
//     trust, strict identity authorization);
//   - applies sensible read-header, read, write, and idle timeouts;
//   - does NOT expose /internal/status on the public HTTP router;
//   - supports graceful shutdown.
package peertls

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// PeerStatus is the JSON response returned by the private peer server's
// /internal/status endpoint. It contains the authenticated server's node
// identity and application version, plus a safe certificate diagnostic. It
// does NOT report an invented synchronization state, cluster epoch, or
// replicated-data readiness.
type PeerStatus struct {
	NodeID          string `json:"node_id"`
	Version         string `json:"version"`
	PodID           string `json:"pod_id,omitempty"`
	CertFingerprint string `json:"cert_fingerprint,omitempty"`
}

// PeerServer is a private HTTPS server for the peer listener. It is a thin
// wrapper around http.Server with the mTLS configuration from the Manager.
type PeerServer struct {
	mgr *Manager

	HTTPServer *http.Server
}

// NewPeerServer builds a PeerServer with the given listen address, node
// identity, and application version. The Manager must have been Started (its
// credentials loaded) before calling ListenAndServe.
//
// Timeouts are conservative: this is an internal endpoint with small payloads,
// so slow connections are shed quickly.
func NewPeerServer(mgr *Manager, addr, nodeID, version string) *PeerServer {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/status", newStatusHandler(mgr, nodeID, version))

	return &PeerServer{
		mgr: mgr,
		HTTPServer: &http.Server{
			Addr:              addr,
			Handler:           mux,
			TLSConfig:         mgr.TLSConfigForServer(),
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
	}
}

// ListenAndServe starts the peer HTTPS server. It creates a TLS listener on
// the configured address using the Manager's TLS configuration (TLS 1.3,
// mutual TLS, dedicated CA trust) and serves the private peer endpoints
// over HTTPS. Plaintext HTTP is never served on this listener.
//
// It blocks until the server is shut down (returns nil) or a fatal error
// occurs (returns the error). The caller should run this in a goroutine and
// watch the returned error channel for startup failures.
func (s *PeerServer) ListenAndServe() error {
	tlsCfg := s.mgr.TLSConfigForServer()
	tlsListener, err := tls.Listen("tcp", s.HTTPServer.Addr, tlsCfg)
	if err != nil {
		return fmt.Errorf("creating TLS listener on %s: %w", s.HTTPServer.Addr, err)
	}
	// Serve returns nil when Shutdown has been called. A wrapped net.ErrClosed
	// is also treated as a clean shutdown.
	err = s.HTTPServer.Serve(tlsListener)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		// Check if the underlying listener was closed by Shutdown.
		if _, ok := err.(net.Error); ok {
			return err
		}
		return err
	}
	return nil
}

// Handler returns the http.Handler serving the private peer endpoints. It is
// exported so tests (including cross-package integration tests) can build a
// custom TLS listener with the Manager's config and a known handler. In
// production the PeerServer's own http.Server uses this same handler.
func (s *PeerServer) Handler() http.Handler {
	return s.HTTPServer.Handler
}

// Shutdown gracefully stops the peer server, giving in-flight requests the
// supplied context's deadline to complete.
func (s *PeerServer) Shutdown(ctx context.Context) error {
	return s.HTTPServer.Shutdown(ctx)
}

// newStatusHandler returns the handler for GET /internal/status. The handler
// is reached only after the TLS handshake (including mutual authentication and
// the additional identity authorization) has succeeded, so the client is
// guaranteed to be an authorized GossipGrid replica.
func newStatusHandler(mgr *Manager, nodeID, version string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		diag := mgr.Diagnostics()
		status := PeerStatus{
			NodeID:          nodeID,
			Version:         version,
			PodID:           diag.LocalPodID,
			CertFingerprint: diag.CertFingerprint,
		}
		writePeerJSON(w, http.StatusOK, status)
	}
}

// writePeerJSON writes a JSON response with the given status code.
func writePeerJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	// The client may have disconnected; no useful recovery for a write error.
	_ = json.NewEncoder(w).Encode(v)
}
