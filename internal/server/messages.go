package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"gossipgrid/internal/store"
)

const (
	maxRequestBytes = 8192
	defaultPageSize = 50
	maxPageSize     = 100
)

// messageHandler holds the dependencies for message API endpoints.
type messageHandler struct {
	store  *store.Store
	nodeID string
}

// newMessageHandler creates a messageHandler with the given store.
func newMessageHandler(s *store.Store) *messageHandler {
	return &messageHandler{store: s}
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

	writeJSON(w, http.StatusCreated, msg)
}

// getMessages handles GET /api/messages.
// Returns locally stored messages, newest first, with bounded pagination via
// ?limit=N (default 50, max 100) and ?offset=N (default 0).
func (h *messageHandler) getMessages(w http.ResponseWriter, r *http.Request) {
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

	msgs, total, err := h.store.List(limit, offset)
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
func (h *messageHandler) getMessage(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSONError(w, http.StatusBadRequest, "message id is required")
		return
	}

	msg, err := h.store.GetByID(id)
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
