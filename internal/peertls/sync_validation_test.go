// Package peertls: real TLS-client regression tests for malformed inventory
// and fetch responses (Milestone 4C corrections).
//
// These tests use real mTLS connections (real TLS listener, real client with
// certificate verification) to verify that malformed peer responses are
// rejected with typed protocol errors and never cause a panic.
package peertls

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"gossipgrid/internal/store"
)

// genTestUUIDs generates n sequential canonical UUIDv7 strings starting
// from the zero-based offset (0 in all current test usages).
func genTestUUIDs(n int) []string {
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 16)
		b[0] = 0x01
		b[1] = 0x9a
		b[6] = 0x70 // version 7
		b[8] = 0x80 // variant
		for j := 0; j < 8; j++ {
			b[15-j] = byte(i >> (j * 8))
		}
		u := uuid.UUID(b)
		ids[i] = u.String()
	}
	return ids
}

// setupSyncTest creates an mTLS test server and client pair. The handler is
// registered on both /internal/sync/ids and /internal/sync/messages.
func setupSyncTest(t *testing.T, handler http.Handler) (addr string, client *Client) {
	t.Helper()
	ca := newTestCA(t, "w")
	cert, key, trust := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, cert, key, trust, "gossipgrid-0", 50*time.Millisecond)
	a, stop := startTestServer(t, mgr, handler)
	t.Cleanup(stop)
	return a, NewClient(mgr)
}

// --- FetchInventoryPage regression tests ---

// TestFetchInventoryPageEmptyIDsWithHasMorePanics is the previously
// independently reproduced panic: has_more=true with an empty ID list caused
// FetchInventoryPage to index IDs[len(IDs)-1] on an empty slice.
func TestFetchInventoryPageEmptyIDsWithHasMorePanics(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/sync/ids", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ids":         []string{},
			"next_cursor": "019a0000-0000-7000-8000-000000000001",
			"has_more":    true,
		})
	})
	addr, client := setupSyncTest(t, mux)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("FetchInventoryPage panicked: %v", r)
		}
	}()

	_, err := client.FetchInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 10)
	if err == nil {
		t.Fatal("expected error for has_more=true with empty ID list")
	}
	var ackErr *PeerAckError
	if !errorsAs(err, &ackErr) {
		t.Errorf("expected *PeerAckError, got %T: %v", err, err)
	}
}

// TestFetchInventoryPageInvalidIDRejected verifies that a non-UUID ID in the
// response is rejected as a protocol error (not silently accepted).
func TestFetchInventoryPageInvalidIDRejected(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/sync/ids", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ids":      []string{"not-a-uuid"},
			"has_more": false,
		})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 10)
	if err == nil {
		t.Fatal("expected error for invalid ID in inventory response")
	}
	var ackErr *PeerAckError
	if !errorsAs(err, &ackErr) {
		t.Errorf("expected *PeerAckError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "not-a-uuid") {
		t.Errorf("error should mention the invalid ID, got: %v", err)
	}
}

// TestFetchInventoryPageNonAscendingOrderRejected verifies that IDs not in
// strict ascending order are rejected.
func TestFetchInventoryPageNonAscendingOrderRejected(t *testing.T) {
	ids := genTestUUIDs(3)
	// Swap the first two to break ascending order.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/sync/ids", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ids":      []string{ids[1], ids[0], ids[2]},
			"has_more": false,
		})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 10)
	if err == nil {
		t.Fatal("expected error for non-ascending ID order")
	}
	var ackErr *PeerAckError
	if !errorsAs(err, &ackErr) {
		t.Errorf("expected *PeerAckError, got %T: %v", err, err)
	}
}

// TestFetchInventoryPageDuplicateIDsRejected verifies that duplicate IDs are
// rejected.
func TestFetchInventoryPageDuplicateIDsRejected(t *testing.T) {
	ids := genTestUUIDs(2)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/sync/ids", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ids":      []string{ids[0], ids[0]},
			"has_more": false,
		})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 10)
	if err == nil {
		t.Fatal("expected error for duplicate IDs")
	}
}

// TestFetchInventoryPageExceedsLimitRejected verifies that a page exceeding
// the requested limit is rejected.
func TestFetchInventoryPageExceedsLimitRejected(t *testing.T) {
	ids := genTestUUIDs(5)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/sync/ids", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ids":      ids,
			"has_more": false,
		})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 3)
	if err == nil {
		t.Fatal("expected error for page exceeding limit")
	}
}

// TestFetchInventoryPageIDNotGreaterThanAfter verifies that a returned ID
// that does not advance past the requested after cursor is rejected.
func TestFetchInventoryPageIDNotGreaterThanAfter(t *testing.T) {
	ids := genTestUUIDs(3)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/sync/ids", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Return IDs that are all <= the after cursor.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ids":      []string{ids[0], ids[1]},
			"has_more": false,
		})
	})
	addr, client := setupSyncTest(t, mux)

	// after = ids[1], so both ids[0] and ids[1] are <= after.
	_, err := client.FetchInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, ids[1], 10)
	if err == nil {
		t.Fatal("expected error for IDs not advancing past after cursor")
	}
}

// TestFetchInventoryPageFinalPageNonEmptyNextCursor verifies that a final
// page (has_more=false) with a non-empty next_cursor is rejected.
func TestFetchInventoryPageFinalPageNonEmptyNextCursor(t *testing.T) {
	ids := genTestUUIDs(2)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/sync/ids", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ids":         ids,
			"next_cursor": ids[1],
			"has_more":    false,
		})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 10)
	if err == nil {
		t.Fatal("expected error for final page with non-empty next_cursor")
	}
}

// TestFetchInventoryPageValidPageSucceeds verifies that a valid inventory
// page (proper UUIDv7 IDs, ascending order, correct cursor semantics) is
// accepted.
func TestFetchInventoryPageValidPageSucceeds(t *testing.T) {
	ids := genTestUUIDs(5)
	// Use only 3 IDs (limit=3) with has_more=true.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/sync/ids", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ids":         ids[:3],
			"next_cursor": ids[2],
			"has_more":    true,
		})
	})
	addr, client := setupSyncTest(t, mux)

	page, err := client.FetchInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 3)
	if err != nil {
		t.Fatalf("valid page rejected: %v", err)
	}
	if len(page.IDs) != 3 {
		t.Errorf("expected 3 IDs, got %d", len(page.IDs))
	}
	if page.NextCursor != ids[2] {
		t.Errorf("expected next_cursor %q, got %q", ids[2], page.NextCursor)
	}
	if !page.HasMore {
		t.Error("expected has_more=true")
	}
}

// TestFetchInventoryPageValidFinalPageSucceeds verifies that a valid final
// page (has_more=false, empty next_cursor) is accepted.
func TestFetchInventoryPageValidFinalPageSucceeds(t *testing.T) {
	ids := genTestUUIDs(3)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/sync/ids", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ids":      ids,
			"has_more": false,
		})
	})
	addr, client := setupSyncTest(t, mux)

	page, err := client.FetchInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 10)
	if err != nil {
		t.Fatalf("valid final page rejected: %v", err)
	}
	if len(page.IDs) != 3 {
		t.Errorf("expected 3 IDs, got %d", len(page.IDs))
	}
	if page.NextCursor != "" {
		t.Errorf("expected empty next_cursor on final page, got %q", page.NextCursor)
	}
	if page.HasMore {
		t.Error("expected has_more=false on final page")
	}
}

// TestFetchInventoryPageEmptyFinalPageSucceeds verifies that an empty final
// page (no IDs, has_more=false) is accepted (the peer has no records).
func TestFetchInventoryPageEmptyFinalPageSucceeds(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/sync/ids", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ids":      []string{},
			"has_more": false,
		})
	})
	addr, client := setupSyncTest(t, mux)

	page, err := client.FetchInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 10)
	if err != nil {
		t.Fatalf("empty final page rejected: %v", err)
	}
	if len(page.IDs) != 0 {
		t.Errorf("expected 0 IDs, got %d", len(page.IDs))
	}
}

// --- FetchMessages regression tests ---

// TestFetchMessagesMalformedTimestampRejected verifies that a message with a
// malformed timestamp in the fetch response is rejected as a protocol error
// rather than returned as a valid result.
func TestFetchMessagesMalformedTimestampRejected(t *testing.T) {
	id := genTestUUIDs(1)[0]
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messages": []map[string]string{
				{"id": id, "body": "test", "origin": "node-1", "created_at": "not-a-timestamp"},
			},
		})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, []string{id})
	if err == nil {
		t.Fatal("expected error for malformed timestamp in fetch response")
	}
	var ackErr *PeerAckError
	if !errorsAs(err, &ackErr) {
		t.Errorf("expected *PeerAckError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "malformed timestamp") {
		t.Errorf("error should mention malformed timestamp, got: %v", err)
	}
}

// TestFetchMessagesEmptyTimestampRejected verifies that a message with an
// empty timestamp is rejected.
func TestFetchMessagesEmptyTimestampRejected(t *testing.T) {
	id := genTestUUIDs(1)[0]
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messages": []map[string]string{
				{"id": id, "body": "test", "origin": "node-1", "created_at": ""},
			},
		})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, []string{id})
	if err == nil {
		t.Fatal("expected error for empty timestamp in fetch response")
	}
}

// TestFetchMessagesInvalidIDRejected verifies that a message with an
// invalid (non-UUIDv7) ID in the fetch response is rejected.
func TestFetchMessagesInvalidIDRejected(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messages": []map[string]string{
				{"id": "not-a-uuid", "body": "test", "origin": "node-1", "created_at": "2026-11-20T12:00:00Z"},
			},
		})
	})
	addr, client := setupSyncTest(t, mux)

	// Request the valid ID; the response contains an invalid one.
	validID := genTestUUIDs(1)[0]
	_, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, []string{validID})
	if err == nil {
		t.Fatal("expected error for invalid ID in fetch response")
	}
}

// TestFetchMessagesValidResponseSucceeds verifies that a valid fetch response
// (matching IDs, valid timestamps, ascending order) is accepted.
func TestFetchMessagesValidResponseSucceeds(t *testing.T) {
	ids := genTestUUIDs(2)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messages": []map[string]string{
				{"id": ids[0], "body": "body-0", "origin": "node-1", "created_at": "2026-11-20T12:00:00Z"},
				{"id": ids[1], "body": "body-1", "origin": "node-1", "created_at": "2026-11-20T12:00:01Z"},
			},
		})
	})
	addr, client := setupSyncTest(t, mux)

	msgs, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids)
	if err != nil {
		t.Fatalf("valid fetch response rejected: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if msgs[0].ID != ids[0] || msgs[1].ID != ids[1] {
		t.Errorf("unexpected message IDs: %q, %q", msgs[0].ID, msgs[1].ID)
	}
}

// TestFetchMessagesWrongCountRejected verifies that a fetch response with a
// different number of messages than requested is rejected.
func TestFetchMessagesWrongCountRejected(t *testing.T) {
	ids := genTestUUIDs(2)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Return only 1 message for 2 requested IDs.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messages": []map[string]string{
				{"id": ids[0], "body": "b", "origin": "o", "created_at": "2026-11-20T12:00:00Z"},
			},
		})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids)
	if err == nil {
		t.Fatal("expected error for wrong message count")
	}
}

// --- Fetch-batch response-size contract tests (real JSON encoding) ---

// TestJSONEncodedBodyLessThan signs verifies the worst-case JSON encoding for
// body fields: a 4096-byte body of "<" characters (each escapes to 6 bytes as
// \u003c) produces 24576 encoded bytes, far exceeding the old assumption of
// 20480 (5 bytes per byte). This is the bug that made the old 12-record
// batch limit incorrect.
func TestJSONEncodedBodyWorstCase(t *testing.T) {
	body := strings.Repeat("<", 4096)
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("encoding body: %v", err)
	}
	// json.Marshal adds quotes around the string, so the payload is len-2.
	payload := len(encoded) - 2
	t.Logf("4096 '<' chars: %d bytes after JSON encoding (old assumption: 20480)", payload)
	if payload != 24576 {
		t.Errorf("4096 '<' chars encode to %d bytes, want 24576 (4096*6)", payload)
	}
	// Also verify control characters (e.g. \x01 → \u0001, 6 bytes).
	ctrlBody := strings.Repeat("\x01", 4096)
	ctrlEncoded, err := json.Marshal(ctrlBody)
	if err != nil {
		t.Fatalf("encoding control body: %v", err)
	}
	ctrlPayload := len(ctrlEncoded) - 2
	t.Logf("4096 0x01 chars: %d bytes after JSON encoding", ctrlPayload)
	if ctrlPayload != 24576 {
		t.Errorf("4096 0x01 chars encode to %d bytes, want 24576 (4096*6)", ctrlPayload)
	}
}

// TestSyncResponseBoundMaxOrigin verifies that a batch of 8 (the maximum
// permitted fetch batch) messages each carrying the maximum permitted origin
// length (253 bytes) and a maximum 4096-byte body serializes to a response
// within the client's 256 KiB response bound, using actual JSON encoding.
func TestSyncResponseBoundMaxOrigin(t *testing.T) {
	const maxOriginLen = maxMessageOriginBytes // 253
	const maxBodyLen = 4096
	const maxBatch = 8

	msgs := make([]SyncMessageWire, maxBatch)
	for i := 0; i < maxBatch; i++ {
		msgs[i] = SyncMessageWire{
			ID:        genTestUUIDs(maxBatch)[i],
			Body:      strings.Repeat("a", maxBodyLen),
			Origin:    strings.Repeat("o", maxOriginLen),
			CreatedAt: "2026-11-20T12:00:00.123456789Z",
		}
	}
	resp, err := json.Marshal(fetchMessagesResponse{Messages: msgs})
	if err != nil {
		t.Fatalf("encoding response: %v", err)
	}
	if len(resp) > maxSyncResponseBodyBytes {
		t.Errorf("8 messages with 253-byte origin + 4096-byte body produce %d bytes, exceeding the %d byte response bound", len(resp), maxSyncResponseBodyBytes)
	}
	t.Logf("8 max-origin/max-body messages: %d bytes (bound %d)", len(resp), maxSyncResponseBodyBytes)
}

// TestSyncResponseBoundWorstCaseEscapedBody verifies the absolute worst case
// for the body field: a 4096-byte body of "<" characters (each escapes to
// 6 bytes as \u003c) produces 24576 encoded bytes per record. A batch of 8
// such records with a 253-byte origin serializes to a response within the
// client's 256 KiB bound. This is the worst case that the 8-record batch
// limit is designed to protect against.
func TestSyncResponseBoundWorstCaseEscapedBody(t *testing.T) {
	const maxBatch = 8

	body := strings.Repeat("<", 4096)
	// Verify the stored body is within the store limit.
	if len(body) > 4096 {
		t.Fatalf("test body %d exceeds store limit 4096", len(body))
	}
	// Verify a public POST with a 1340-"<%s body (whose JSON encoding is
	// 8040 bytes, within the 8192 request limit) works. The 4096-"<%s body
	// (24576 encoded bytes) exceeds the public POST request limit — it can
	// only enter the store through a legacy/malformed path, not the public API.
	smallBody := strings.Repeat("<", 1340)
	single, _ := json.Marshal(sendRequest{Message: MessageWire{
		ID: genTestUUIDs(1)[0], Body: smallBody, Origin: "node", CreatedAt: "2026-11-20T12:00:00Z",
	}})
	if len(single) > maxPeerRequestBodyBytes {
		t.Fatalf("public POST JSON %d exceeds the %d request limit", len(single), maxPeerRequestBodyBytes)
	}
	t.Logf("public POST of 1340-< body: %d bytes (limit %d)", len(single), maxPeerRequestBodyBytes)

	// A batch of 8 worst-case records (4096-byte "<" body + 253-byte origin).
	msgs := make([]SyncMessageWire, maxBatch)
	for i := 0; i < maxBatch; i++ {
		msgs[i] = SyncMessageWire{
			ID:        genTestUUIDs(maxBatch)[i],
			Body:      body,
			Origin:    strings.Repeat("o", maxMessageOriginBytes),
			CreatedAt: "2026-11-20T12:00:00.123456789Z",
		}
	}
	resp, err := json.Marshal(fetchMessagesResponse{Messages: msgs})
	if err != nil {
		t.Fatalf("encoding response: %v", err)
	}
	if len(resp) > maxSyncResponseBodyBytes {
		t.Errorf("8 worst-case escaped-body messages produce %d bytes, exceeding the %d byte response bound", len(resp), maxSyncResponseBodyBytes)
	}
	t.Logf("8 worst-case escaped-body messages: %d bytes (bound %d)", len(resp), maxSyncResponseBodyBytes)

	// The per-record wire-size bound (32768) accounts for the worst-case body
	// encoding (24576) plus origin/ID/timestamp overhead (~8192).
	absWorst8 := 8 * maxSyncMessageWireBytes
	absWorst9 := 9 * maxSyncMessageWireBytes
	t.Logf("per-record bound: 8 records = %d bytes (within %d); 9 records = %d bytes (exceeds %d)",
		absWorst8, maxSyncResponseBodyBytes, absWorst9, maxSyncResponseBodyBytes)
	if absWorst8 > maxSyncResponseBodyBytes || absWorst9 <= maxSyncResponseBodyBytes {
		t.Errorf("batch bound arithmetic invalid: 8*%d=%d, 9*%d=%d, bound=%d", maxSyncMessageWireBytes, absWorst8, maxSyncMessageWireBytes, absWorst9, maxSyncResponseBodyBytes)
	}
}

// TestFetchMessagesOversizedOriginRejected verifies that a fetch response
// containing a record whose origin exceeds the maximum permitted length
// (253) is rejected with an explicit typed error rather than silently
// accepted or producing an oversized response.
func TestFetchMessagesOversizedOriginRejected(t *testing.T) {
	id := genTestUUIDs(1)[0]
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(fetchMessagesResponse{Messages: []SyncMessageWire{
			{ID: id, Body: "test", Origin: strings.Repeat("o", maxMessageOriginBytes+1), CreatedAt: "2026-11-20T12:00:00Z"},
		}})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, []string{id})
	if err == nil {
		t.Fatal("expected error for oversized origin in fetch response")
	}
	var ackErr *PeerAckError
	if !errorsAs(err, &ackErr) {
		t.Errorf("expected *PeerAckError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "origin") {
		t.Errorf("error should mention origin, got: %v", err)
	}
}

// TestFetchMessagesOversizedRecordExplicitError verifies that a fetch response
// containing a record whose full JSON-encoded size exceeds the per-record
// wire-size bound (32768) is rejected with an explicit typed error. This
// simulates a legacy/malformed record that bypassed the field bounds.
func TestFetchMessagesOversizedRecordExplicitError(t *testing.T) {
	id := genTestUUIDs(1)[0]
	// A 60000-byte body (far beyond the 4096 store limit) whose JSON-encoded
	// size exceeds the per-record wire-size bound.
	body := strings.Repeat("x", 60000)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(fetchMessagesResponse{Messages: []SyncMessageWire{
			{ID: id, Body: body, Origin: "node", CreatedAt: "2026-11-20T12:00:00Z"},
		}})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, []string{id})
	if err == nil {
		t.Fatal("expected error for oversized record in fetch response")
	}
	var ackErr *PeerAckError
	if !errorsAs(err, &ackErr) {
		t.Errorf("expected *PeerAckError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "wire-size") {
		t.Errorf("error should mention wire-size, got: %v", err)
	}
}

// TestFetchBatchExceedsMaxRejected verifies the fetch-batch limit (8) is the
// right bound: at the per-record wire-size bound (maxSyncMessageWireBytes =
// 32768 = the absolute worst-case JSON-encoded body for a 4096-byte stored
// body, plus the origin/ID/timestamp overhead), 8 records fit within the
// 256 KiB response bound, but 9 records of the same size would exceed it.
// This is the arithmetic that justifies the 8-record limit.
func TestFetchBatchExceedsMaxRejected(t *testing.T) {
	const perRecordWire = maxSyncMessageWireBytes // 32768

	// 8 records at the per-record wire size fit within the response bound.
	if 8*perRecordWire > maxSyncResponseBodyBytes {
		t.Errorf("8 records at %d bytes each = %d bytes, exceeding the %d bound (the batch limit of 8 must fit)", perRecordWire, 8*perRecordWire, maxSyncResponseBodyBytes)
	}
	// 9 records (a larger batch) of the same size exceed the bound,
	// demonstrating the 8-record limit is required.
	if 9*perRecordWire <= maxSyncResponseBodyBytes {
		t.Errorf("9 records at %d bytes each = %d bytes; expected to EXCEED the %d bound (the reason the batch limit is 8, not 9)", perRecordWire, 9*perRecordWire, maxSyncResponseBodyBytes)
	}
	t.Logf("8 records: %d bytes (within %d); 9 records: %d bytes (exceeds %d)", 8*perRecordWire, maxSyncResponseBodyBytes, 9*perRecordWire, maxSyncResponseBodyBytes)
}

// --- Server-side sync fetch handler: batch bound and response-size contract ---

// postSyncFetch posts a JSON body to the sync fetch handler and returns the
// status code and response body.
func postSyncFetch(t *testing.T, handler http.Handler, body []byte) (int, []byte) {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/internal/sync/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	return rr.Code, rr.Body.Bytes()
}

// TestSyncFetchHandlerBatchBoundRejected verifies that a request for more than
// the maximum permitted fetch batch (8) is rejected server-side with 413,
// even though the IDs are individually valid. This is the server-side bound
// that prevents an authenticated client from requesting an excessive response.
func TestSyncFetchHandlerBatchBoundRejected(t *testing.T) {
	st := newMessageTestStore(t)
	handler := newSyncFetchHandler(st, 0, time.Now)

	// Request 9 IDs (1 more than the max batch of 8).
	body, _ := json.Marshal(fetchMessagesRequest{IDs: genTestUUIDs(9)})
	status, resp := postSyncFetch(t, handler, body)
	if status != http.StatusRequestEntityTooLarge {
		t.Errorf("9-ID request: status = %d, want %d (413); body: %s", status, http.StatusRequestEntityTooLarge, resp)
	}
}

// TestSyncFetchHandlerValidBatchWithinBound verifies that a request for exactly
// the maximum batch (8) of maximum-size records (4096-byte "<" body, 253-byte
// origin) is accepted (200) and the response is within the client's 256 KiB
// bound. The store enforces the per-field bounds (4096 body, 253 origin), so
// the serialized response is finite and within the bound.
func TestSyncFetchHandlerValidBatchWithinBound(t *testing.T) {
	st := newMessageTestStore(t)
	handler := newSyncFetchHandler(st, 0, time.Now)

	const maxBatch = 8
	// Insert 8 max-size messages (4096-byte "<" body, 253-byte origin) into
	// the store. CreateLocalMessage generates its own UUIDv7 IDs.
	body := strings.Repeat("<", 4096)
	origin := strings.Repeat("o", maxMessageOriginBytes)
	for i := 0; i < maxBatch; i++ {
		if _, err := st.CreateLocalMessage(origin, body); err != nil {
			t.Fatalf("CreateLocalMessage: %v", err)
		}
	}
	// Get the 8 stored IDs.
	var stored []store.Message
	if err := st.DB().Order("id ASC").Limit(maxBatch).Find(&stored).Error; err != nil {
		t.Fatalf("reading stored messages: %v", err)
	}
	if len(stored) != maxBatch {
		t.Fatalf("expected %d stored messages, got %d", maxBatch, len(stored))
	}
	fetchIDs := make([]string, maxBatch)
	for i := range stored {
		fetchIDs[i] = stored[i].ID
	}
	reqBody, _ := json.Marshal(fetchMessagesRequest{IDs: fetchIDs})
	status, resp := postSyncFetch(t, handler, reqBody)
	if status != http.StatusOK {
		t.Fatalf("8-ID request: status = %d, want 200; body: %s", status, resp)
	}
	if len(resp) > maxSyncResponseBodyBytes {
		t.Errorf("8 max-size messages: response %d bytes exceeds %d bound", len(resp), maxSyncResponseBodyBytes)
	}
	t.Logf("8 max-size messages (4096 '<' body + 253 origin): response %d bytes (bound %d)", len(resp), maxSyncResponseBodyBytes)
}

// TestSyncFetchHandlerOversizedRecordExplicitError verifies that a batch of 8
// records whose bodies were stored oversized (bypassing validation, simulating
// a legacy record) causes the handler to return an explicit 500 error rather
// than truncating the record or silently returning a response that exceeds
// the bound. The per-field validation is bypassed (direct SQL insert), so the
// response-size bounding in the handler is what catches it.
func TestSyncFetchHandlerOversizedRecordExplicitError(t *testing.T) {
	st := newMessageTestStore(t)
	handler := newSyncFetchHandler(st, 0, time.Now)

	// Directly insert 8 records, each with a 60000-byte body (far beyond the
	// 4096 store limit and beyond what would fit in the response bound).
	// This simulates legacy/malformed records that bypassed the field bounds.
	ids := genTestUUIDs(8)
	now := time.Now().UTC()
	for _, id := range ids {
		if err := st.DB().Table("messages").Create(&store.Message{
			ID:        id,
			Body:      strings.Repeat("x", 60000),
			Origin:    "node",
			CreatedAt: now,
		}).Error; err != nil {
			t.Fatalf("direct insert: %v", err)
		}
	}
	reqBody, _ := json.Marshal(fetchMessagesRequest{IDs: ids})
	status, resp := postSyncFetch(t, handler, reqBody)
	// The serialized response (~8*60000 ≈ 480 KiB) exceeds the 256 KiB bound,
	// so the handler must return an explicit 500 rather than a 200.
	if status != http.StatusInternalServerError {
		t.Errorf("oversized-record batch: status = %d, want 500; body: %.120s", status, resp)
	}
	if len(resp) > maxSyncResponseBodyBytes {
		t.Errorf("handler wrote %d bytes, exceeding the %d bound (must not write oversized responses)", len(resp), maxSyncResponseBodyBytes)
	}
}

// errorsAs wraps errors.As for convenience in test assertions.
func errorsAs(err error, target interface{}) bool {
	return errors.As(err, target)
}

// TestFetchMessages404RetryOnExpiration verifies the client's 404-retry
// behavior: when the peer's sync-fetch endpoint returns a structured 404
// (one or more requested IDs absent or expired, reported in the
// "missing_ids" field), the client validates the missing IDs against the
// original request, retries with only the present IDs, and returns the valid
// records. This prevents one expired ID from stalling the entire batch (and
// the anti-entropy engine's cursor).
//
// The test uses a real mTLS server whose handler returns a structured 404 on
// the first request (listing one ID as missing) and 200 on the retry
// (returning only the present ID's record). This exercises the actual client
// code path.
func TestFetchMessages404RetryOnExpiration(t *testing.T) {
	ids := genTestUUIDs(2) // [presentID, expiredID]
	presentID, expiredID := ids[0], ids[1]
	timestamp := "2026-11-20T12:00:00Z"

	var mu sync.Mutex
	requestCount := 0

	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		count := requestCount
		mu.Unlock()

		if count == 1 {
			// First request: structured 404 with the expired ID in
			// missing_ids. The peer's retention filter excluded the expired
			// message between inventory enumeration and this fetch.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(missingIDsResponse{
				Error:      "message IDs not found: " + expiredID,
				MissingIDs: []string{expiredID},
			})
			return
		}
		// Retry (count >= 2): 200 with only the present ID's record.
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messages": []map[string]string{
				{"id": presentID, "body": "body-0", "origin": "node-1", "created_at": timestamp},
			},
		})
	})
	addr, client := setupSyncTest(t, mux)

	// Request both IDs. The client should handle the 404 by retrying with
	// only the present ID and return the valid record.
	msgs, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids)
	if err != nil {
		t.Fatalf("FetchMessages with 404: err = %v, want nil (404-retry should succeed)", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("FetchMessages: len = %d, want 1 (only the present ID); got %+v", len(msgs), msgs)
	}
	if msgs[0].ID != presentID {
		t.Errorf("FetchMessages: ID = %q, want %q (the present ID)", msgs[0].ID, presentID)
	}
	// The retry must have occurred (2 total requests: original + retry).
	mu.Lock()
	defer mu.Unlock()
	if requestCount != 2 {
		t.Errorf("requestCount = %d, want 2 (original + retry after 404)", requestCount)
	}
}

// TestFetchMessages404AllMissing verifies that when ALL requested IDs are
// confirmed missing/expired (a structured 404 listing every requested ID in
// missing_ids), the client treats this as SUCCESSFUL reconciliation with zero
// fetched messages: it returns an empty slice and nil error (no retry is made;
// there are no present IDs to fetch). The peer's inventory, which excludes
// expired records, will not re-advertise these IDs, so the anti-entropy engine
// can advance its cursor without stalling the batch.
func TestFetchMessages404AllMissing(t *testing.T) {
	ids := genTestUUIDs(2)

	var mu sync.Mutex
	requestCount := 0

	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		// Structured 404: all requested IDs are missing/expired.
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(missingIDsResponse{
			Error:      "message IDs not found: " + ids[0] + ", " + ids[1],
			MissingIDs: ids,
		})
	})
	addr, client := setupSyncTest(t, mux)

	msgs, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids)
	if err != nil {
		t.Fatalf("FetchMessages with all IDs missing: err = %v, want nil (all-missing is successful reconciliation)", err)
	}
	if msgs == nil || len(msgs) != 0 {
		t.Fatalf("FetchMessages: len = %d, want 0 (zero fetched messages); got %+v", len(msgs), msgs)
	}
	// No retry must be made for an all-missing 404.
	mu.Lock()
	defer mu.Unlock()
	if requestCount != 1 {
		t.Errorf("requestCount = %d, want 1 (no retry for all-missing 404)", requestCount)
	}
}

// TestFetchMessages404RetryFailsVerifies that when the 404-retry itself fails
// with a non-404 error (a genuine protocol error, not legitimate expiration),
// the client returns the retry's error (not the original 404). This preserves
// the "do not silently ignore unrelated protocol errors" guarantee.
func TestFetchMessages404RetryFails(t *testing.T) {
	ids := genTestUUIDs(2)
	presentID := ids[0]

	var mu sync.Mutex
	requestCount := 0

	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requestCount++
		count := requestCount
		mu.Unlock()

		if count == 1 {
			// First request: structured 404 (one ID missing).
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(missingIDsResponse{
				Error:      "message IDs not found: " + ids[1],
				MissingIDs: []string{ids[1]},
			})
			return
		}
		// Retry: 500 (a genuine server error, not expiration).
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "internal server error",
		})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids)
	if err == nil {
		t.Fatal("FetchMessages with failed retry: expected error, got nil")
	}
	var httpErr *PeerHTTPStatusError
	if !errors.As(err, &httpErr) {
		t.Fatalf("FetchMessages: error type = %T, want *PeerHTTPStatusError", err)
	}
	// The retry's 500 must be propagated (not the original 404).
	if httpErr.status != http.StatusInternalServerError {
		t.Errorf("FetchMessages: status = %d, want 500 (the retry's error, not the original 404)", httpErr.status)
	}
	_ = presentID
}

// TestFetchMessages404MalformedBodyRejected verifies that a 404 whose body is
// not a valid structured missing-ID response (no "missing_ids" field) is
// rejected as a protocol error (PeerAckError) rather than interpreted as an
// expiration. This prevents an unrelated or malformed 404 from being treated
// as a legitimate expiration and stalling or advancing the cursor incorrectly.
func TestFetchMessages404MalformedBodyRejected(t *testing.T) {
	ids := genTestUUIDs(2)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		// A 404 with only a free-form error string and no missing_ids field:
		// this does not reliably report which IDs are missing, so it must be
		// rejected (not parsed as an expiration).
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids)
	if err == nil {
		t.Fatal("FetchMessages with malformed 404 body: expected error, got nil")
	}
	var ackErr *PeerAckError
	if !errors.As(err, &ackErr) {
		t.Fatalf("FetchMessages: error type = %T, want *PeerAckError (malformed 404)", err)
	}
}

// TestFetchMessages404UnexpectedIDRejected verifies that a structured 404 whose
// missing_ids contains an ID that was NOT part of the original request is
// rejected (the response is unrelated to this fetch and must not be
// interpreted as an expiration).
func TestFetchMessages404UnexpectedIDRejected(t *testing.T) {
	requested := genTestUUIDs(2)
	unexpected := genTestUUIDs(10)[5] // a valid UUIDv7 not in the request

	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(missingIDsResponse{
			Error:      "message IDs not found",
			MissingIDs: []string{requested[0], unexpected},
		})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, requested)
	if err == nil {
		t.Fatal("FetchMessages with unexpected missing ID: expected error, got nil")
	}
	var ackErr *PeerAckError
	if !errors.As(err, &ackErr) {
		t.Fatalf("FetchMessages: error type = %T, want *PeerAckError (unexpected ID)", err)
	}
}

// TestFetchMessages404RepeatedExpirationDuringRetry verifies the bounded-retry
// guarantee: when only some IDs are missing and the single retry (with the
// present IDs) encounters a SECOND 404 (records expiring across successive
// attempts), the client returns the retry's error (a failed page) rather than
// retrying again. This is what prevents unbounded retries as records expire
// during successive attempts: at most one retry is ever made after a 404.
func TestFetchMessages404RepeatedExpirationDuringRetry(t *testing.T) {
	ids := genTestUUIDs(2)
	presentID := ids[0]

	var mu sync.Mutex
	requestCount := 0

	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/sync/messages", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requestCount++
		count := requestCount
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		if count == 1 {
			// First request: one ID missing.
			_ = json.NewEncoder(w).Encode(missingIDsResponse{
				Error:      "message IDs not found: " + ids[1],
				MissingIDs: []string{ids[1]},
			})
			return
		}
		// Retry (count == 2): the present ID has ALSO now expired → second
		// 404. The client must NOT retry again; it returns this 404 error.
		_ = json.NewEncoder(w).Encode(missingIDsResponse{
			Error:      "message IDs not found: " + presentID,
			MissingIDs: []string{presentID},
		})
	})
	addr, client := setupSyncTest(t, mux)

	_, err := client.FetchMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids)
	if err == nil {
		t.Fatal("FetchMessages with repeated 404 during retry: expected error, got nil")
	}
	var httpErr *PeerHTTPStatusError
	if !errors.As(err, &httpErr) {
		t.Fatalf("FetchMessages: error type = %T, want *PeerHTTPStatusError (retry 404)", err)
	}
	if httpErr.status != http.StatusNotFound {
		t.Errorf("FetchMessages: status = %d, want 404 (the retry's 404)", httpErr.status)
	}
	// Exactly two requests: the original + ONE retry (no unbounded retries).
	mu.Lock()
	defer mu.Unlock()
	if requestCount != 2 {
		t.Errorf("requestCount = %d, want 2 (original + single retry, no unbounded retries)", requestCount)
	}
}
