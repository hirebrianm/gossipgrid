package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/store"
)

const (
	maxRequestBytes = 8192
	defaultPageSize = 50
	maxPageSize     = 100
)

// MessageCallback is an optional notification invoked after a successful
// local message commit (POST /api/messages). The callback receives the
// committed message. It must not block indefinitely; the HTTP handler does
// not wait for the callback to complete.
type MessageCallback func(msg store.Message)

// messageHandler holds the dependencies for message API endpoints.
type messageHandler struct {
	store         *store.Store
	nodeID        string
	onMessage     MessageCallback
	lifecycleCtrl *lifecycle.Controller

	// retention: when TTL > 0, expired messages are excluded
	// from the public API (GET /api/messages and GET /api/messages/{id}).
	// A zero TTL (the default) disables retention and the read paths behave
	// exactly as before. now is injectable for deterministic tests.
	retentionTTL time.Duration
	now          func() time.Time
}

// newMessageHandler creates a messageHandler with the given store. The clock
// defaults to time.Now; tests may override the retentionTTL and now fields
// directly to drive deterministic expiration scenarios.
func newMessageHandler(s *store.Store) *messageHandler {
	return &messageHandler{store: s, now: time.Now}
}

// postMessages handles POST /api/messages.
// It accepts only {"body": "..."} and rejects unknown fields, multiple JSON
// values, and oversized bodies.
func (h *messageHandler) postMessages(w http.ResponseWriter, r *http.Request) {
	// Reject oversized bodies before reading. MaxBytesReader provides a
	// second line of defense for chunked requests without Content-Length.
	if r.ContentLength > maxRequestBytes {
		writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var req struct {
		Body string `json:"body"`
	}
	if err := dec.Decode(&req); err != nil {
		// A type error (e.g. body is a number) is a client error.
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			writeJSONError(w, http.StatusBadRequest, "invalid request: body must be a string")
			return
		}
		// An empty body is a client error.
		if errors.Is(err, io.EOF) {
			writeJSONError(w, http.StatusBadRequest, "invalid request: empty body")
			return
		}
		// MaxBytesReader returns *http.MaxBytesError when the body exceeds the
		// limit (both for requests with Content-Length and for chunked
		// requests); the JSON decoder may also surface this as a syntax error
		// at the offset where the body was truncated.
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		var syntaxErr *json.SyntaxError
		if errors.As(err, &syntaxErr) && syntaxErr.Offset > maxRequestBytes {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	// Reject multiple JSON values (e.g. {} {} ).
	// A second Decode() may also trip the MaxBytesReader limit when the
	// request body (valid JSON plus trailing data or whitespace) exceeds
	// maxRequestBytes; that is an oversized request, not a multi-value error.
	// When err is nil, a second value was decoded successfully (multi-value
	// error). When err is io.EOF, the body is exhausted (valid). Any other
	// error is checked for MaxBytesError before defaulting to 400.
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
				return
			}
			var syntaxErr *json.SyntaxError
			if errors.As(err, &syntaxErr) && syntaxErr.Offset > maxRequestBytes {
				writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
				return
			}
		}
		writeJSONError(w, http.StatusBadRequest, "invalid JSON: multiple values in request body")
		return
	}

	// Milestone 5A: when lifecycle protection is enabled, check admission
	// before the SQLite insert. An unready or unauthorized replica must not
	// accept ordinary public message writes. The decision is read from the
	// controller's single consistent authorization snapshot (ready state plus
	// a fresh, valid, matching authoritative observation plus a matching
	// durable binding, no lockout, not stopped).
	if h.lifecycleCtrl != nil {
		decision := h.lifecycleCtrl.Admission().CheckWrite(r.Context())
		if !decision.Allowed {
			writeJSONError(w, http.StatusServiceUnavailable, decision.Reason)
			return
		}
		// Recheck admission immediately before the store mutation (Issue 5:
		// the finite in-flight window). The initial check above authorized
		// the request at one point in time; this recheck catches a concurrent
		// revocation (a state transition or metadata change) that occurred
		// while the request body was being decoded. A request blocked before
		// admission cannot reach a store mutation.
		recheck := h.lifecycleCtrl.Admission().CheckWrite(r.Context())
		if !recheck.Allowed {
			writeJSONError(w, http.StatusServiceUnavailable,
				"admission revoked while request was in flight: "+recheck.Reason)
			return
		}
	}

	msg, err := h.store.CreateLocalMessage(h.nodeID, req.Body)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrEmptyBody):
			writeJSONError(w, http.StatusBadRequest, "message body must not be empty")
		case errors.Is(err, store.ErrBodyTooLarge):
			writeJSONError(w, http.StatusRequestEntityTooLarge, "message body exceeds 4096 bytes")
		default:
			writeJSONError(w, http.StatusInternalServerError, "failed to store message")
		}
		return
	}

	// Milestone 4B: notify the gossip engine of a new local message.
	// The callback is non-blocking (it enqueues to a bounded channel).
	// The HTTP response is returned after the local commit; it does not
	// wait for network replication.
	//
	// Milestone 5A: the callback is only invoked when the write passed the
	// lifecycle admission check (above), so a rejected write does not
	// trigger gossip.
	if h.onMessage != nil {
		h.onMessage(*msg)
	}

	writeJSON(w, http.StatusCreated, msg)
}

// authorizationGate checks the lifecycle admission decision for a public read
// operation (GET /api/messages, GET /api/messages/{id}). It returns true when
// the read may proceed (lifecycle protection disabled, or the replica is
// authorized to serve its local message database). When lifecycle protection
// is enabled but the replica is NOT authorized (a stale or invalid or missing
// authoritative observation, a missing or mismatched durable binding, a
// recovery lockout, or the controller is stopped), the read is quarantined:
// the local records remain preserved (never deleted) but are NOT exposed as
// ordinary current-epoch API data; the handler returns 503.
func (h *messageHandler) authorizationGate(w http.ResponseWriter, r *http.Request) bool {
	if h.lifecycleCtrl == nil {
		return true // lifecycle protection disabled: legacy behavior preserved
	}
	decision := h.lifecycleCtrl.Admission().CheckWrite(r.Context())
	if !decision.Allowed {
		writeJSONError(w, http.StatusServiceUnavailable,
			"message database quarantined: "+decision.Reason)
		return false
	}
	return true
}

// getMessages handles GET /api/messages.
// Returns locally stored messages, newest first, with bounded pagination via
// ?limit=N (default 50, max 100) and ?offset=N (default 0).
//
// Milestone 5A: when lifecycle protection is enabled, the read is gated on the
// authorization decision: a replica that is not authorized to serve its local
// message database (e.g. RECOVERY_REQUIRED with a mismatched binding, or a
// stale/missing authoritative observation) returns 503 rather than exposing
// quarantined old-epoch records as ordinary current-epoch data. The records
// remain preserved locally. When lifecycle protection is disabled, the read
// proceeds as before.
func (h *messageHandler) getMessages(w http.ResponseWriter, r *http.Request) {
	if !h.authorizationGate(w, r) {
		return
	}
	limit := defaultPageSize
	offset := 0

	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil && n > 0 && n <= maxPageSize {
			limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil && n >= 0 {
			offset = n
		}
	}

	// Message retention: when enabled, exclude expired messages from
	// the listing and the total count. When disabled (the default), this is
	// identical to the legacy List.
	threshold := store.ExpirationThreshold(h.retentionTTL, h.now())
	msgs, total, err := h.store.ListWithExpiration(limit, offset, threshold)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to list messages")
		return
	}

	writeJSON(w, http.StatusOK, messageListResponse{
		Messages: msgs,
		Total:    total,
		Limit:    limit,
		Offset:   offset,
	})
}

// getMessage handles GET /api/messages/{id}.
// Returns a single message or 404.
//
// Milestone 5A: when lifecycle protection is enabled, the read is gated on the
// authorization decision (same as getMessages): a replica that is not
// authorized to serve its local message database returns 503 rather than
// exposing a quarantined old-epoch record as ordinary current-epoch data. The
// record remains preserved locally. When lifecycle protection is disabled, the
// read proceeds as before.
func (h *messageHandler) getMessage(w http.ResponseWriter, r *http.Request) {
	if !h.authorizationGate(w, r) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "message id is required")
		return
	}

	// Message retention: when enabled, an expired message is
	// treated as not found (404), indistinguishable from an absent message.
	// When disabled (the default), this is identical to GetByID.
	threshold := store.ExpirationThreshold(h.retentionTTL, h.now())
	msg, err := h.store.GetByIDWithExpiration(id, threshold)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeJSONError(w, http.StatusNotFound, "message not found")
			return
		}
		writeJSONError(w, http.StatusInternalServerError, "failed to retrieve message")
		return
	}

	writeJSON(w, http.StatusOK, msg)
}

type messageListResponse struct {
	Messages []store.Message `json:"messages"`
	Total    int64           `json:"total"`
	Limit    int             `json:"limit"`
	Offset   int             `json:"offset"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
