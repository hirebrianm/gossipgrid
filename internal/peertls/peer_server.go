// Package peertls: private peer HTTPS server.
//
// The PeerServer wraps an http.Server that listens on the configured peer
// address (default :8443) and serves a small set of private endpoints that
// require authenticated client certificates. For Milestone 3B it exposes only
// GET /internal/status. For Milestone 4A it also exposes
// POST /internal/messages when the authenticated peer message exchange is
// enabled (opt-in, requires peer mTLS).
//
// The server:
//   - uses the Manager's TLS configuration (TLS 1.3, mutual TLS, dedicated CA
//     trust, strict identity authorization);
//   - applies sensible read-header, read, write, and idle timeouts;
//   - does NOT expose /internal/status or /internal/messages on the public
//     HTTP router;
//   - supports graceful shutdown.
package peertls

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"gossipgrid/internal/store"
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

	// onNewMessage is an optional callback invoked after a successfully
	// inserted (new row) replicated message. It is NOT called for duplicate
	// no-ops or conflicts. Used by the gossip engine (Milestone 4B) to
	// forward newly received messages to other peers.
	onNewMessage func(msg *store.Message, senderPodID string)
}

// maxPeerRequestBodyBytes bounds the inbound request body for
// POST /internal/messages. The wire envelope (JSON of the message) must not
// exceed this; the store enforces a tighter 4096-byte limit on the message
// body itself.
const maxPeerRequestBodyBytes = 8192

// maxSyncRequestBodyBytes bounds the inbound request body for
// POST /internal/sync/messages. A batch of up to 1024 IDs (36 bytes each in
// canonical UUIDv7 form) plus JSON overhead requires roughly 40 KiB.
// 64 KiB provides headroom while remaining finite.
const maxSyncRequestBodyBytes = 64 * 1024

// maxSyncPageSize is the strict maximum page size for the inventory endpoint.
// It is enforced server-side regardless of the caller's limit parameter.
const maxSyncPageSize = 1024

// maxSyncFetchBatchSize is the strict maximum number of message records the
// POST /internal/sync/messages endpoint will return in a single response.
//
// It is enforced server-side regardless of the number of IDs requested. It
// must be compatible with the client's fetch-batch bound (both 8): a batch
// of 8 records at the per-record wire-size bound (maxSyncMessageWireBytes =
// 32768 = the absolute worst-case JSON-encoded body for a 4096-byte stored
// body, plus the origin/ID/timestamp overhead) is 32768*8 = ~256 KiB,
// within the client's 256 KiB response bound. The server bound prevents an
// authenticated client from requesting a response that would force
// uncontrolled allocations beyond the response limit.
const maxSyncFetchBatchSize = 8

// NewPeerServer builds a PeerServer with the given listen address, node
// identity, and application version. The Manager must have been Started (its
// credentials loaded) before calling ListenAndServe.
//
// When store is nil (or enabledMessageExchange is false), only
// GET /internal/status is registered. When store is non-nil and
// enabledMessageExchange is true, POST /internal/messages is additionally
// registered on the private peer listener. The public router never sees these
// endpoints.
//
// Timeouts are conservative: this is an internal endpoint with small payloads,
// so slow connections are shed quickly.
func NewPeerServer(mgr *Manager, addr, nodeID, version string) *PeerServer {
	return newPeerServer(mgr, addr, nodeID, version, nil, false, false, nil)
}

// NewPeerServerWithStore builds a PeerServer with the given listen address,
// node identity, application version, and optional local message store. When
// store is non-nil and enabledMessageExchange is true,
// POST /internal/messages is registered on the private peer listener for
// Milestone 4A authenticated peer message exchange. The enable flag is a
// hard opt-in: when false, only /internal/status is served.
func NewPeerServerWithStore(mgr *Manager, addr, nodeID, version string, s *store.Store, enabledMessageExchange bool) *PeerServer {
	return newPeerServer(mgr, addr, nodeID, version, s, enabledMessageExchange, false, nil)
}

// NewPeerServerWithStoreAndCallback is like NewPeerServerWithStore but also
// accepts an optional callback invoked after a successfully inserted (new row)
// replicated message. The callback receives the message and the authenticated
// sender's pod identity (extracted from the mTLS client certificate). It is
// NOT called for duplicate no-ops or conflicts.
func NewPeerServerWithStoreAndCallback(mgr *Manager, addr, nodeID, version string, s *store.Store, enabledMessageExchange bool, onNewMessage func(msg *store.Message, senderPodID string)) *PeerServer {
	return newPeerServer(mgr, addr, nodeID, version, s, enabledMessageExchange, false, onNewMessage)
}

// NewPeerServerWithSync builds a PeerServer with the given listen address,
// node identity, application version, local message store, and both the
// message-exchange and anti-entropy sync flags. When enabledAntiEntropy is
// true and a store is supplied, the private sync endpoints
// (GET /internal/sync/ids and POST /internal/sync/messages) are additionally
// registered on the private peer listener.
func NewPeerServerWithSync(mgr *Manager, addr, nodeID, version string, s *store.Store, enabledMessageExchange, enabledAntiEntropy bool, onNewMessage func(msg *store.Message, senderPodID string)) *PeerServer {
	return newPeerServer(mgr, addr, nodeID, version, s, enabledMessageExchange, enabledAntiEntropy, onNewMessage)
}

// newPeerServer is the shared builder for PeerServer variants.
func newPeerServer(mgr *Manager, addr, nodeID, version string, s *store.Store, enabledMessageExchange, enabledAntiEntropy bool, onNewMessage func(msg *store.Message, senderPodID string)) *PeerServer {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/status", newStatusHandler(mgr, nodeID, version))

	// Milestone 4A/4B: register the private message exchange endpoint only
	// when (a) the message-exchange flag is enabled AND (b) a store is
	// supplied. When disabled, the endpoint is NOT registered at all.
	if enabledMessageExchange && s != nil {
		mux.HandleFunc("POST /internal/messages", newMessageExchangeHandler(mgr, s, onNewMessage))
	}

	// Milestone 4C: register the private anti-entropy sync endpoints only
	// when (a) the anti-entropy flag is enabled AND (b) a store is supplied.
	if enabledAntiEntropy && s != nil {
		mux.HandleFunc("GET /internal/sync/ids", newSyncInventoryHandler(s))
		mux.HandleFunc("POST /internal/sync/messages", newSyncFetchHandler(s))
	}

	return &PeerServer{
		mgr:          mgr,
		onNewMessage: onNewMessage,
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

// newMessageExchangeHandler returns the handler for POST /internal/messages.
// It is reached only after the mTLS handshake has succeeded, so the sender is
// an authorized GossipGrid replica. It decodes the JSON envelope, validates
// it (bounded size, single JSON document, no unknown fields, canonical
// UUIDv7, nonempty body/origin, valid timestamp, bounded message body), and
// inserts the message idempotently via the store.
//
// The handler does NOT trust any HTTP header as proof of peer identity; the
// authenticated TLS client identity establishes who transmitted the message.
// The message's origin field is immutable metadata identifying its original
// creator (which may differ from the forwarding peer); mTLS alone does not
// prove the historical origin of a forwarded message.
func newMessageExchangeHandler(mgr *Manager, s *store.Store, onNewMessage func(msg *store.Message, senderPodID string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Reject oversized bodies before reading. MaxBytesReader provides a
		// second line of defense for chunked requests without Content-Length.
		if r.ContentLength > maxPeerRequestBodyBytes {
			writePeerJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxPeerRequestBodyBytes)

		// Validate Content-Type: the endpoint accepts JSON only. A missing
		// or different content type is a client error (415).
		if ct := r.Header.Get("Content-Type"); ct != "" && !isJSONContentType(ct) {
			writePeerJSONError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
			return
		}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()

		var req sendRequest
		if err := dec.Decode(&req); err != nil {
			// A type error (e.g. message.body is a number) is a client error.
			var typeErr *json.UnmarshalTypeError
			if errors.As(err, &typeErr) {
				writePeerJSONError(w, http.StatusBadRequest, "invalid request: message field has wrong type")
				return
			}
			// An empty body is a client error.
			if errors.Is(err, io.EOF) {
				writePeerJSONError(w, http.StatusBadRequest, "invalid request: empty body")
				return
			}
			// MaxBytesReader returns *http.MaxBytesError when the body exceeds the
			// limit; the JSON decoder may also surface this as a syntax error
			// at the offset where the body was truncated.
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				writePeerJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
				return
			}
			var syntaxErr *json.SyntaxError
			if errors.As(err, &syntaxErr) && syntaxErr.Offset > maxPeerRequestBodyBytes {
				writePeerJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
				return
			}
			writePeerJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
			return
		}

		// Reject multiple JSON values (e.g. {} {} ).
		var extra any
		if err := dec.Decode(&extra); err != io.EOF {
			if err != nil {
				var maxErr *http.MaxBytesError
				if errors.As(err, &maxErr) {
					writePeerJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
					return
				}
				var syntaxErr *json.SyntaxError
				if errors.As(err, &syntaxErr) && syntaxErr.Offset > maxPeerRequestBodyBytes {
					writePeerJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
					return
				}
			}
			writePeerJSONError(w, http.StatusBadRequest, "invalid JSON: multiple values in request body")
			return
		}

		// Parse the message's immutable fields into a store.Message. The
		// created_at field is a string in the wire format; parse it as an
		// RFC3339 timestamp.
		var msg store.Message
		msg.ID = req.Message.ID
		msg.Body = req.Message.Body
		msg.Origin = req.Message.Origin
		if req.Message.CreatedAt != "" {
			createdAt, err := time.Parse(time.RFC3339Nano, req.Message.CreatedAt)
			if err != nil {
				// Fall back to plain RFC3339 (no fractional seconds).
				createdAt, err = time.Parse(time.RFC3339, req.Message.CreatedAt)
				if err != nil {
					writePeerJSONError(w, http.StatusBadRequest, "invalid request: created_at must be an RFC3339 timestamp")
					return
				}
			}
			msg.CreatedAt = createdAt
		}

		// Use the store's validation and idempotent insert. The store
		// enforces canonical UUIDv7, nonempty body/origin, non-zero
		// timestamp, bounded body (4096 bytes), and atomic ON CONFLICT DO
		// NOTHING insert with conflict detection.
		// Milestone 4B: use InsertReplicatedMessageReturningInserted to
		// distinguish "new row created" from "duplicate no-op" so that
		// only genuinely new messages trigger gossip forwarding.
		inserted, err := s.InsertReplicatedMessageReturningInserted(&msg)
		if err != nil {
			switch {
			case errors.Is(err, store.ErrInvalidID):
				writePeerJSONError(w, http.StatusBadRequest, "invalid message: id must be a canonical UUIDv7")
			case errors.Is(err, store.ErrEmptyOrigin):
				writePeerJSONError(w, http.StatusBadRequest, "invalid message: origin must not be empty")
			case errors.Is(err, store.ErrMissingTimestamp):
				writePeerJSONError(w, http.StatusBadRequest, "invalid message: created_at must be a valid non-zero timestamp")
			case errors.Is(err, store.ErrEmptyBody):
				writePeerJSONError(w, http.StatusBadRequest, "invalid message: body must not be empty")
			case errors.Is(err, store.ErrBodyTooLarge):
				writePeerJSONError(w, http.StatusRequestEntityTooLarge, "invalid message: body exceeds 4096 bytes")
			case errors.Is(err, store.ErrOriginTooLarge):
				writePeerJSONError(w, http.StatusRequestEntityTooLarge, "invalid message: origin exceeds 253 bytes")
			case errors.Is(err, store.ErrDuplicateIDConflict):
				writePeerJSONError(w, http.StatusConflict, "message ID already exists with conflicting content")
			default:
				writePeerJSONError(w, http.StatusInternalServerError, "failed to store message")
			}
			return
		}

		// Milestone 4B: if a new row was created and a callback is
		// configured, notify the gossip engine to forward this message
		// to other peers. The callback receives the message and the
		// authenticated sender's pod identity (for duplicate suppression:
		// avoid forwarding back to the peer we just received from).
		if inserted && onNewMessage != nil {
			senderPodID := extractSenderPodID(r, mgr)
			onNewMessage(&msg, senderPodID)
		}

		// Echo the stored message for the caller to confirm what was
		// persisted. The peer field is the receiver's pod identity (the
		// authenticated peer the message was sent to), not the sender's.
		resp := sendResponse{
			Message: &MessageWire{
				ID:        msg.ID,
				Body:      msg.Body,
				Origin:    msg.Origin,
				CreatedAt: msg.CreatedAt.UTC().Format(time.RFC3339Nano),
			},
			Peer: mgr.Diagnostics().LocalPodID,
		}
		writePeerJSON(w, http.StatusOK, resp)
	}
}

// isJSONContentType reports whether the given Content-Type header value is a
// JSON media type (application/json, with or without parameters like
// charset). An empty string is accepted (Go's ServeMux does not require a
// Content-Type on POST; the decoder will still reject non-JSON).
func isJSONContentType(ct string) bool {
	if ct == "" {
		return true
	}
	// Strip parameters (e.g. "application/json; charset=utf-8" -> "application/json").
	if idx := indexOfByte(ct, ';'); idx >= 0 {
		ct = ct[:idx]
	}
	// Trim whitespace.
	for len(ct) > 0 && (ct[0] == ' ' || ct[0] == '\t') {
		ct = ct[1:]
	}
	for len(ct) > 0 && (ct[len(ct)-1] == ' ' || ct[len(ct)-1] == '\t') {
		ct = ct[:len(ct)-1]
	}
	return ct == "application/json"
}

// indexOfByte returns the index of the first occurrence of c in s, or -1.
func indexOfByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// writePeerJSON writes a JSON response with the given status code.
func writePeerJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	// The client may have disconnected; no useful recovery for a write error.
	_ = json.NewEncoder(w).Encode(v)
}

// writePeerJSONError writes a JSON error response with the given status code.
func writePeerJSONError(w http.ResponseWriter, status int, msg string) {
	writePeerJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: msg})
}

// extractSenderPodID extracts the authenticated sender's pod identity from
// the mTLS client certificate in the request. The request is only received
// after the mTLS handshake has completed (including the custom
// VerifyPeerCertificate identity authorization), so the client certificate
// is guaranteed to be an authorized GossipGrid replica.
//
// Returns an empty string when the TLS state is unavailable (e.g., in tests
// that bypass the TLS layer) or when the certificate cannot be parsed.
// The empty string is safe: the gossip engine treats it as "unknown sender"
// and simply does not exclude any specific peer from forwarding.
func extractSenderPodID(r *http.Request, mgr *Manager) string {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return ""
	}
	leaf := r.TLS.PeerCertificates[0]
	if len(leaf.DNSNames) == 0 {
		return ""
	}
	// The pod identity is the part before the identity suffix in the DNS SAN.
	san := normalizeDNSName(leaf.DNSNames[0])
	suffix := normalizeDNSName(mgr.cfg.IdentitySuffix)
	if strings.HasSuffix(san, "."+suffix) {
		return san[:len(san)-len(suffix)-1]
	}
	return ""
}

// newSyncInventoryHandler returns the handler for GET /internal/sync/ids.
// It is reached only after the mTLS handshake has succeeded. It accepts
// optional query parameters: after (the exclusive cursor, a canonical UUIDv7)
// and limit (the page size, 1-1024). It returns a bounded page of message IDs
// in strict ascending order using keyset pagination.
func newSyncInventoryHandler(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()

		// Parse and validate the limit parameter.
		limit := 128
		if v := q.Get("limit"); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 || n > maxSyncPageSize {
				writePeerJSONError(w, http.StatusBadRequest, fmt.Sprintf("limit must be an integer between 1 and %d", maxSyncPageSize))
				return
			}
			limit = n
		}

		// Parse and validate the after (cursor) parameter.
		after := ""
		if v := q.Get("after"); v != "" {
			// Validate as canonical UUIDv7.
			u, err := uuid.Parse(v)
			if err != nil || u.Version() != 7 || u.String() != v {
				writePeerJSONError(w, http.StatusBadRequest, "after must be a valid canonical UUIDv7 (lowercase, hyphenated)")
				return
			}
			after = v
		}

		page, err := s.ListIDsAfter(r.Context(), after, limit)
		if err != nil {
			writePeerJSONError(w, http.StatusInternalServerError, "failed to list message IDs")
			return
		}

		resp := InventoryPageResponse{
			IDs:        page.IDs,
			NextCursor: page.NextCursor,
			HasMore:    page.HasMore,
		}
		if page.IDs == nil {
			resp.IDs = []string{}
		}
		writePeerJSON(w, http.StatusOK, resp)
	}
}

// newSyncFetchHandler returns the handler for POST /internal/sync/messages.
// It is reached only after the mTLS handshake has succeeded. It accepts a JSON
// body with a bounded "ids" array (canonical UUIDv7 values, no duplicates,
// max 1024 entries) and returns the complete message records in ascending ID
// order. If any requested ID is missing, an explicit error is returned
// (partial results are not silently presented as complete).
func newSyncFetchHandler(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Reject oversized bodies before reading.
		if r.ContentLength > maxSyncRequestBodyBytes {
			writePeerJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxSyncRequestBodyBytes)

		// Validate Content-Type.
		if ct := r.Header.Get("Content-Type"); ct != "" && !isJSONContentType(ct) {
			writePeerJSONError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
			return
		}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()

		var req fetchMessagesRequest
		if err := dec.Decode(&req); err != nil {
			var typeErr *json.UnmarshalTypeError
			if errors.As(err, &typeErr) {
				writePeerJSONError(w, http.StatusBadRequest, "invalid request: ids field has wrong type")
				return
			}
			if errors.Is(err, io.EOF) {
				writePeerJSONError(w, http.StatusBadRequest, "invalid request: empty body")
				return
			}
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				writePeerJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
				return
			}
			writePeerJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
			return
		}

		// Reject multiple JSON values.
		var extra any
		if err := dec.Decode(&extra); err != io.EOF {
			if err != nil {
				var maxErr *http.MaxBytesError
				if errors.As(err, &maxErr) {
					writePeerJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
					return
				}
			}
			writePeerJSONError(w, http.StatusBadRequest, "invalid JSON: multiple values in request body")
			return
		}

		// Validate the ID count.
		if len(req.IDs) == 0 {
			writePeerJSONError(w, http.StatusBadRequest, "ids must contain at least one entry")
			return
		}
		// The strict maximum number of records returned in a single response.
		// A request exceeding this is rejected even though the IDs are
		// individually valid: it would produce a response that exceeds the
		// client's response-size bound (and force uncontrolled server
		// allocations). The caller (the anti-entropy engine) requests bounded
		// batches, so this only rejects oversized requests.
		if len(req.IDs) > maxSyncFetchBatchSize {
			writePeerJSONError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("ids must not exceed %d entries per batch", maxSyncFetchBatchSize))
			return
		}

		// Validate each ID as canonical UUIDv7 and check for duplicates.
		seen := make(map[string]bool, len(req.IDs))
		for _, id := range req.IDs {
			u, err := uuid.Parse(id)
			if err != nil || u.Version() != 7 || u.String() != id {
				writePeerJSONError(w, http.StatusBadRequest, fmt.Sprintf("id %q is not a valid canonical UUIDv7", id))
				return
			}
			if seen[id] {
				writePeerJSONError(w, http.StatusBadRequest, fmt.Sprintf("duplicate id %q in request", id))
				return
			}
			seen[id] = true
		}

		// Fetch the messages. The store returns an error listing missing IDs.
		msgs, err := s.GetMessagesByIDs(r.Context(), req.IDs)
		if err != nil {
			// The error contains the list of missing IDs.
			writePeerJSONError(w, http.StatusNotFound, err.Error())
			return
		}

		// Build the response.
		wireMsgs := make([]SyncMessageWire, len(msgs))
		for i, m := range msgs {
			wireMsgs[i] = SyncMessageWire{
				ID:        m.ID,
				Body:      m.Body,
				Origin:    m.Origin,
				CreatedAt: m.CreatedAt.UTC().Format(time.RFC3339Nano),
			}
		}
		respBytes, err := json.Marshal(fetchMessagesResponse{Messages: wireMsgs})
		if err != nil {
			writePeerJSONError(w, http.StatusInternalServerError, "failed to encode sync response")
			return
		}
		// Bound server response generation: a valid batch (bounded ID count
		// and per-field length) must serialize within the client's response
		// bound. A response that would exceed it is an explicit error rather
		// than a truncated or uncontrolled allocation. This guards against a
		// legacy or malformed oversized record that slipped past the field
		// bounds (e.g. a body stored before the origin bound existed).
		if len(respBytes) > maxSyncResponseBodyBytes {
			writePeerJSONError(w, http.StatusInternalServerError, fmt.Sprintf("sync response exceeds maximum of %d bytes", maxSyncResponseBodyBytes))
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		// The client may have disconnected; no useful recovery for a write error.
		_, _ = w.Write(respBytes)
	}
}
