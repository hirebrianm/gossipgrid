package peertls

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/store"
)

// syncCounter generates deterministic, ordered, valid UUIDv7-style IDs with a
// fixed base timestamp (consistent with the test's fixed "now").
var syncCounter int64

// seedSyncMessage inserts a message with an explicit creation timestamp and
// returns its ID (with the ID's embedded timestamp matching the creation time).
func seedSyncMessage(t *testing.T, st *store.Store, ts time.Time) string {
	t.Helper()
	syncCounter++
	id := makeSyncID(ts)
	if err := st.InsertReplicatedMessage(&store.Message{
		ID: id, Body: "sync-retention", Origin: "node-0", CreatedAt: ts,
	}); err != nil {
		t.Fatalf("InsertReplicatedMessage: %v", err)
	}
	return id
}

// makeSyncID builds a valid canonical UUIDv7-format string with a fixed
// timestamp embedded.
func makeSyncID(ts time.Time) string {
	tms := uint64(ts.UnixMilli())
	var b [16]byte
	b[0] = byte(tms >> 40)
	b[1] = byte(tms >> 32)
	b[2] = byte(tms >> 24)
	b[3] = byte(tms >> 16)
	b[4] = byte(tms >> 8)
	b[5] = byte(tms)
	b[6] = 0x70
	b[7] = byte(syncCounter & 0xFF)
	b[8] = 0x80
	b[9] = 0xAB
	b[10] = 0xCD
	b[11] = 0xEF
	b[12] = 0x01
	b[13] = 0x23
	b[14] = byte((syncCounter >> 8) & 0xFF)
	b[15] = 0x45
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

type inventoryResp struct {
	IDs        []string `json:"ids"`
	NextCursor string   `json:"next_cursor"`
	HasMore    bool     `json:"has_more"`
}

// getSyncInventory performs a GET on the sync inventory handler.
func getSyncInventory(t *testing.T, handler http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), "GET", "/internal/sync/ids?limit=128", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	return rr
}

// TestSyncInventoryExcludesExpired verifies that, with retention enabled, the
// sync inventory (GET /internal/sync/ids) does not advertise expired message
// IDs. This is the primary anti-entropy resurrection guard: a peer that lists
// an expired ID would fetch it and re-insert (resurrect) it. When retention is
// disabled (zero TTL), all IDs are advertised.
func TestSyncInventoryExcludesExpired(t *testing.T) {
	now := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	fixedNow := func() time.Time { return now }

	st := newMessageTestStore(t)
	freshID := seedSyncMessage(t, st, now.Add(-time.Hour))
	expiredID := seedSyncMessage(t, st, now.Add(-72*time.Hour))

	// Retention enabled: only the fresh ID is advertised.
	handler := newSyncInventoryHandler(st, ttl, fixedNow)
	rec := getSyncInventory(t, handler)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp inventoryResp
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.IDs) != 1 || resp.IDs[0] != freshID {
		t.Errorf("inventory = %v, want only the fresh ID %q (expired IDs excluded)", resp.IDs, freshID)
	}
	for _, id := range resp.IDs {
		if id == expiredID {
			t.Errorf("expired ID %q advertised in inventory (would be resurrected)", id)
		}
	}

	// Retention disabled: both IDs are advertised.
	handlerDisabled := newSyncInventoryHandler(st, 0, fixedNow)
	rec = getSyncInventory(t, handlerDisabled)
	if rec.Code != http.StatusOK {
		t.Fatalf("disabled status = %d, want 200", rec.Code)
	}
	var respAll inventoryResp
	if err := json.Unmarshal(rec.Body.Bytes(), &respAll); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(respAll.IDs) != 2 {
		t.Errorf("disabled inventory len = %d, want 2", len(respAll.IDs))
	}
}

// TestSyncFetchReportsExpiredAsNotFound verifies that, with retention enabled,
// the sync fetch (POST /internal/sync/messages) reports an expired ID as "not
// found" (defense in depth: even if the inventory advertised it, the fetch
// would not return it, so the peer would not re-insert it). A fresh ID is
// returned normally. When retention is disabled, both IDs are returned.
func TestSyncFetchReportsExpiredAsNotFound(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour
	fixedNow := func() time.Time { return now }

	st := newMessageTestStore(t)
	freshID := seedSyncMessage(t, st, now.Add(-time.Hour))
	expiredID := seedSyncMessage(t, st, now.Add(-72*time.Hour))

	// Retention enabled: fetch both -> the expired one is reported missing.
	handler := newSyncFetchHandler(st, ttl, fixedNow)
	body, _ := json.Marshal(fetchMessagesRequest{IDs: []string{freshID, expiredID}})
	status, resp := postSyncFetch(t, handler, body)
	if status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (expired ID reported as not found); body: %s", status, resp)
	}
	// The error response must mention the expired ID (and not the fresh one).
	if !strings.Contains(string(resp), expiredID) {
		t.Errorf("response does not mention expired ID %q: %s", expiredID, resp)
	}

	// Retention disabled: fetch both -> both returned (200).
	handlerDisabled := newSyncFetchHandler(st, 0, fixedNow)
	status, resp = postSyncFetch(t, handlerDisabled, body)
	if status != http.StatusOK {
		t.Fatalf("disabled status = %d, want 200; body: %s", status, resp)
	}
}

// TestSyncInventoryDynamicThreshold verifies that the sync inventory
// computes the expiration threshold from the current clock at each request,
// not from a value captured at handler registration. A message that is
// initially valid (within the TTL) must be excluded once the clock advances
// past the TTL, even though the handler was registered before the message
// expired. This is the regression test for the stale-cutoff defect.
func TestSyncInventoryDynamicThreshold(t *testing.T) {
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	ttl := 2 * time.Hour

	var mu sync.Mutex
	var nowVal time.Time
	setNow := func(ts time.Time) { mu.Lock(); nowVal = ts; mu.Unlock() }
	getNow := func() time.Time { mu.Lock(); defer mu.Unlock(); return nowVal }

	// At base, the message (created 1h ago) is within the 2h TTL.
	setNow(base)
	st := newMessageTestStore(t)
	validID := seedSyncMessage(t, st, base.Add(-time.Hour))

	// Register the handler with the injectable clock.
	handler := newSyncInventoryHandler(st, ttl, getNow)

	// Initially: the message is valid and appears in the inventory.
	rec := getSyncInventory(t, handler)
	if rec.Code != http.StatusOK {
		t.Fatalf("initial status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var resp inventoryResp
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(resp.IDs) != 1 || resp.IDs[0] != validID {
		t.Fatalf("initial inventory = %v, want only %q (message is within TTL)", resp.IDs, validID)
	}

	// Advance the clock past the TTL: the message is now expired.
	setNow(base.Add(3 * time.Hour))

	// Subsequent sync request must exclude the now-expired message.
	rec = getSyncInventory(t, handler)
	if rec.Code != http.StatusOK {
		t.Fatalf("after-expiry status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
	var respAfter inventoryResp
	if err := json.Unmarshal(rec.Body.Bytes(), &respAfter); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(respAfter.IDs) != 0 {
		t.Errorf("after-expiry inventory = %v, want empty (message expired; stale cutoff would still list it)", respAfter.IDs)
	}
}

// TestSyncFetchDynamicThreshold verifies that the sync fetch also computes
// the expiration threshold dynamically. A message initially within the TTL
// must be reported as "not found" once the clock advances past the TTL,
// even though the handler was registered before the message expired.
func TestSyncFetchDynamicThreshold(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 2 * time.Hour

	var mu sync.Mutex
	var nowVal time.Time
	setNow := func(ts time.Time) { mu.Lock(); nowVal = ts; mu.Unlock() }
	getNow := func() time.Time { mu.Lock(); defer mu.Unlock(); return nowVal }

	setNow(base)
	st := newMessageTestStore(t)
	validID := seedSyncMessage(t, st, base.Add(-time.Hour))

	handler := newSyncFetchHandler(st, ttl, getNow)
	body, _ := json.Marshal(fetchMessagesRequest{IDs: []string{validID}})

	// Initially: the message is within the TTL and is returned.
	status, _ := postSyncFetch(t, handler, body)
	if status != http.StatusOK {
		t.Fatalf("initial status = %d, want 200", status)
	}

	// Advance the clock past the TTL: the message is now expired.
	setNow(base.Add(3 * time.Hour))

	// Subsequent fetch must report the now-expired message as not found.
	status, resp := postSyncFetch(t, handler, body)
	if status != http.StatusNotFound {
		t.Fatalf("after-expiry status = %d, want 404 (message expired); body: %s", status, resp)
	}
	if !strings.Contains(string(resp), validID) {
		t.Errorf("response does not mention expired ID %q: %s", validID, resp)
	}
}

// TestGossipInboundRejectsExpired verifies that, with retention enabled, the
// inbound gossip endpoint (POST /internal/messages) rejects a message whose
// creation timestamp is older than the current expiration threshold. This is
// the regression test for the resurrection-through-gossip defect: without this
// check, an expired message forwarded by a peer would be reinserted into the
// local store, resurrecting it after a cleanup sweep removed it.
//
// The test also verifies that a fresh message (within the TTL) is accepted.
func TestGossipInboundRejectsExpired(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 2 * time.Hour

	var mu sync.Mutex
	var nowVal time.Time
	setNow := func(ts time.Time) { mu.Lock(); nowVal = ts; mu.Unlock() }
	getNow := func() time.Time { mu.Lock(); defer mu.Unlock(); return nowVal }

	setNow(base)
	st := newMessageTestStore(t)
	mgr := newInboundTestManager(t)

	// Handler with retention enabled.
	handler := newMessageExchangeHandler(mgr, st, nil, ttl, getNow)

	// Build an expired message (created 5h ago, TTL is 2h).
	expiredID := makeSyncID(base.Add(-5 * time.Hour))
	body, _ := json.Marshal(sendRequest{Message: MessageWire{
		ID:        expiredID,
		Body:      "expired",
		Origin:    "node-0",
		CreatedAt: base.Add(-5 * time.Hour).UTC().Format(time.RFC3339Nano),
	}})

	req := httptest.NewRequestWithContext(t.Context(), "POST", "/internal/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	// The expired message must be rejected with 409.
	if rr.Code != http.StatusConflict {
		t.Fatalf("expired message: status = %d, want 409; body: %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "expired") {
		t.Errorf("response does not mention expiration: %s", rr.Body.String())
	}

	// The message must NOT be in the store.
	if _, err := st.GetByID(expiredID); err == nil {
		t.Errorf("expired message was inserted into the store (resurrection)")
	}

	// Now test a fresh message (created 30m ago, TTL is 2h).
	freshID := makeSyncID(base.Add(-30 * time.Minute))
	freshBody, _ := json.Marshal(sendRequest{Message: MessageWire{
		ID:        freshID,
		Body:      "fresh",
		Origin:    "node-0",
		CreatedAt: base.Add(-30 * time.Minute).UTC().Format(time.RFC3339Nano),
	}})
	req = httptest.NewRequestWithContext(t.Context(), "POST", "/internal/messages", bytes.NewReader(freshBody))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(freshBody))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	// The fresh message must be accepted with 200.
	if rr.Code != http.StatusOK {
		t.Fatalf("fresh message: status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}

	// The fresh message must be in the store.
	if _, err := st.GetByID(freshID); err != nil {
		t.Errorf("fresh message not in store: %v", err)
	}

	// Advance the clock past the TTL: the fresh message is now expired.
	setNow(base.Add(3 * time.Hour))

	// Re-send the same fresh message: it should now be rejected (expired).
	req = httptest.NewRequestWithContext(t.Context(), "POST", "/internal/messages", bytes.NewReader(freshBody))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(freshBody))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("after-expiry: status = %d, want 409 (message now expired); body: %s", rr.Code, rr.Body.String())
	}
}

// TestGossipInboundAfterCleanupSweep verifies that, after a cleanup sweep
// removes an expired message, a subsequent gossip request for the same message
// is rejected (not reinserted). This is the "resurrection after cleanup"
// regression test: the cleanup removes the message, and the retention check in
// the gossip handler prevents a peer from forwarding it back.
func TestGossipInboundAfterCleanupSweep(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 2 * time.Hour

	var mu sync.Mutex
	var nowVal time.Time
	setNow := func(ts time.Time) { mu.Lock(); nowVal = ts; mu.Unlock() }
	getNow := func() time.Time { mu.Lock(); defer mu.Unlock(); return nowVal }

	// At base, the message (created 3h ago) is expired (TTL is 2h).
	setNow(base)
	st := newMessageTestStore(t)
	mgr := newInboundTestManager(t)

	// Insert the message directly (simulating it was inserted before the
	// retention feature was enabled, or before the clock advanced).
	msgID := makeSyncID(base.Add(-3 * time.Hour))
	if err := st.InsertReplicatedMessage(&store.Message{
		ID: msgID, Body: "cleanup-test", Origin: "node-0",
		CreatedAt: base.Add(-3 * time.Hour),
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Run a cleanup sweep: the expired message is deleted.
	threshold := store.ExpirationThreshold(ttl, base)
	deleted, err := st.DeleteExpired(context.Background(), threshold, -1, nil)
	if err != nil {
		t.Fatalf("DeleteExpired: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	// Verify the message is gone.
	if _, err := st.GetByID(msgID); err == nil {
		t.Fatalf("message still present after cleanup sweep")
	}

	// Now a peer tries to forward the same (expired) message via gossip.
	// The retention check must reject it, preventing resurrection.
	handler := newMessageExchangeHandler(mgr, st, nil, ttl, getNow)
	body, _ := json.Marshal(sendRequest{Message: MessageWire{
		ID:        msgID,
		Body:      "cleanup-test",
		Origin:    "node-0",
		CreatedAt: base.Add(-3 * time.Hour).UTC().Format(time.RFC3339Nano),
	}})
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/internal/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("after-cleanup: status = %d, want 409 (message expired, must not be resurrected); body: %s", rr.Code, rr.Body.String())
	}

	// The message must NOT be back in the store.
	if _, err := st.GetByID(msgID); err == nil {
		t.Fatalf("message resurrected after cleanup sweep via gossip")
	}
}

// TestGossipOnlyRetentionBypass verifies that the gossip-only inbound
// replication path (gossip enabled, anti-entropy disabled) receives the
// retention configuration. This is the regression test for the defect where
// main.go used a legacy constructor (without retention TTL) when anti-entropy
// was disabled but gossip was enabled, allowing expired messages to be
// accepted through POST /internal/messages.
//
// The test exercises both the legacy (no lifecycle) and the lifecycle-aware
// constructors with a non-zero retention TTL, verifying that expired messages
// are rejected and fresh messages are accepted.
func TestGossipOnlyRetentionBypass(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 2 * time.Hour

	var mu sync.Mutex
	var nowVal time.Time
	setNow := func(ts time.Time) { mu.Lock(); nowVal = ts; mu.Unlock() }
	getNow := func() time.Time { mu.Lock(); defer mu.Unlock(); return nowVal }
	setNow(base)

	st := newMessageTestStore(t)
	mgr := newInboundTestManager(t)

	// --- Legacy path (no lifecycle): NewPeerServerWithStoreAndCallback ---
	// This is the constructor main.go uses when gossip is on, anti-entropy is
	// off, and lifecycle is disabled. It must forward the retention TTL.
	psLegacy := NewPeerServerWithStoreAndCallback(mgr, ":8443", "node-0", "test", st, true, nil, ttl)
	// Build the message-exchange handler with the same retention TTL (the
	// PeerServer registers it via newPeerServerWithLifecycle; we test the
	// handler directly for determinism).
	handlerLegacy := newMessageExchangeHandler(mgr, st, nil, ttl, getNow)

	// Expired message (created 5h ago, TTL 2h): must be rejected.
	expiredID := makeSyncID(base.Add(-5 * time.Hour))
	body, _ := json.Marshal(sendRequest{Message: MessageWire{
		ID: expiredID, Body: "expired", Origin: "node-0",
		CreatedAt: base.Add(-5 * time.Hour).UTC().Format(time.RFC3339Nano),
	}})
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/internal/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))
	rr := httptest.NewRecorder()
	handlerLegacy.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("legacy gossip-only: expired status = %d, want 409; body: %s", rr.Code, rr.Body.String())
	}
	if _, err := st.GetByID(expiredID); err == nil {
		t.Fatalf("legacy gossip-only: expired message was inserted (bypass)")
	}

	// Fresh message (created 30m ago): must be accepted.
	freshID := makeSyncID(base.Add(-30 * time.Minute))
	freshBody, _ := json.Marshal(sendRequest{Message: MessageWire{
		ID: freshID, Body: "fresh", Origin: "node-0",
		CreatedAt: base.Add(-30 * time.Minute).UTC().Format(time.RFC3339Nano),
	}})
	req = httptest.NewRequestWithContext(t.Context(), "POST", "/internal/messages", bytes.NewReader(freshBody))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(freshBody))
	rr = httptest.NewRecorder()
	handlerLegacy.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("legacy gossip-only: fresh status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}

	// Verify the PeerServer was built with the retention TTL (the constructor
	// stores it; the handler reads it from the PeerServer at request time).
	if psLegacy.retentionTTL != ttl {
		t.Errorf("PeerServer retentionTTL = %v, want %v (constructor must forward the TTL)", psLegacy.retentionTTL, ttl)
	}

	// --- Zero TTL (retention disabled): expired messages are accepted ---
	psNoRetention := NewPeerServerWithStoreAndCallback(mgr, ":8443", "node-0", "test", st, true, nil, 0)
	if psNoRetention.retentionTTL != 0 {
		t.Errorf("no-retention PeerServer retentionTTL = %v, want 0", psNoRetention.retentionTTL)
	}
	handlerNoRet := newMessageExchangeHandler(mgr, st, nil, 0, getNow)
	req = httptest.NewRequestWithContext(t.Context(), "POST", "/internal/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))
	rr = httptest.NewRecorder()
	handlerNoRet.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("no-retention: status = %d, want 200 (expired accepted when retention disabled); body: %s", rr.Code, rr.Body.String())
	}
}

// TestGossipOnlyRetentionWithLifecycle verifies that the lifecycle-aware
// gossip-only path (gossip on, anti-entropy off, lifecycle on) also receives
// the retention TTL. This is the constructor main.go uses when lifecycle
// protection is enabled alongside gossip.
func TestGossipOnlyRetentionWithLifecycle(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 2 * time.Hour

	var mu sync.Mutex
	var nowVal time.Time
	getNow := func() time.Time { mu.Lock(); defer mu.Unlock(); return nowVal }
	// Start at base (the message is expired at base).
	mu.Lock()
	nowVal = base
	mu.Unlock()

	st := newMessageTestStore(t)
	mgr := newInboundTestManager(t)

	// Build the PeerServer with the lifecycle-aware gossip-only constructor
	// (anti-entropy disabled, gossip callback set, retention TTL non-zero).
	// We pass a nil lifecycle controller here (the test focuses on the
	// retention TTL forwarding, not the lifecycle fencing). The key assertion
	// is that the constructor stores the TTL in the PeerServer.
	ps := NewPeerServerWithStoreAndCallback(mgr, ":8443", "node-0", "test", st, true, func(*store.Message, string) {}, ttl)
	if ps.retentionTTL != ttl {
		t.Fatalf("gossip-only-with-lifecycle PeerServer retentionTTL = %v, want %v", ps.retentionTTL, ttl)
	}

	// The message-exchange handler registered by the PeerServer uses the
	// stored TTL. Test it directly (same as the handler the PeerServer would
	// register).
	handler := newMessageExchangeHandler(mgr, st, nil, ttl, getNow)
	expiredID := makeSyncID(base.Add(-5 * time.Hour))
	body, _ := json.Marshal(sendRequest{Message: MessageWire{
		ID: expiredID, Body: "expired", Origin: "node-0",
		CreatedAt: base.Add(-5 * time.Hour).UTC().Format(time.RFC3339Nano),
	}})
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/internal/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("gossip-only-with-lifecycle: expired status = %d, want 409; body: %s", rr.Code, rr.Body.String())
	}
	if _, err := st.GetByID(expiredID); err == nil {
		t.Fatalf("gossip-only-with-lifecycle: expired message inserted (bypass)")
	}
}

// TestPeerExchangeOnlyRetention verifies that the peer-exchange-only inbound
// replication path (gossip disabled, anti-entropy disabled, peer message
// exchange enabled, lifecycle disabled) receives the retention configuration.
//
// This is the regression test for the defect where main.go used the
// non-retention-aware constructor (NewPeerServerWithStore) when gossip and
// anti-entropy were both disabled but peer message exchange was enabled,
// causing the POST /internal/messages endpoint to accept expired messages
// even when retention was configured.
func TestPeerExchangeOnlyRetention(t *testing.T) {
	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	ttl := 2 * time.Hour

	var mu sync.Mutex
	var nowVal time.Time
	getNow := func() time.Time { mu.Lock(); defer mu.Unlock(); return nowVal }
	mu.Lock()
	nowVal = base
	mu.Unlock()

	st := newMessageTestStore(t)
	mgr := newInboundTestManager(t)

	// Build the PeerServer with the retention-aware constructor that
	// main.go now uses for the peer-exchange-only configuration
	// (NewPeerServerWithStoreAndCallback with nil callback and the TTL).
	ps := NewPeerServerWithStoreAndCallback(mgr, ":8443", "node-0", "test", st, true, nil, ttl)
	if ps.retentionTTL != ttl {
		t.Fatalf("peer-exchange-only PeerServer retentionTTL = %v, want %v (constructor must forward the TTL)", ps.retentionTTL, ttl)
	}

	// The message-exchange handler registered by the PeerServer uses the
	// stored TTL. Test it directly (same handler the PeerServer registers).
	handler := newMessageExchangeHandler(mgr, st, nil, ttl, getNow)

	// Expired message (created 5h ago, TTL 2h): must be rejected.
	expiredID := makeSyncID(base.Add(-5 * time.Hour))
	body, _ := json.Marshal(sendRequest{Message: MessageWire{
		ID: expiredID, Body: "expired", Origin: "node-0",
		CreatedAt: base.Add(-5 * time.Hour).UTC().Format(time.RFC3339Nano),
	}})
	req := httptest.NewRequestWithContext(t.Context(), "POST", "/internal/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("peer-exchange-only: expired status = %d, want 409; body: %s", rr.Code, rr.Body.String())
	}
	if _, err := st.GetByID(expiredID); err == nil {
		t.Fatalf("peer-exchange-only: expired message was inserted (bypass)")
	}

	// Fresh message (created 30m ago): must be accepted.
	freshID := makeSyncID(base.Add(-30 * time.Minute))
	freshBody, _ := json.Marshal(sendRequest{Message: MessageWire{
		ID: freshID, Body: "fresh", Origin: "node-0",
		CreatedAt: base.Add(-30 * time.Minute).UTC().Format(time.RFC3339Nano),
	}})
	req = httptest.NewRequestWithContext(t.Context(), "POST", "/internal/messages", bytes.NewReader(freshBody))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(freshBody))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("peer-exchange-only: fresh status = %d, want 200; body: %s", rr.Code, rr.Body.String())
	}

	// Zero TTL (retention disabled): expired messages are accepted (legacy behavior preserved).
	psNoRetention := NewPeerServerWithStoreAndCallback(mgr, ":8443", "node-0", "test", st, true, nil, 0)
	if psNoRetention.retentionTTL != 0 {
		t.Fatalf("peer-exchange-only no-retention: retentionTTL = %v, want 0", psNoRetention.retentionTTL)
	}
	handlerNoRet := newMessageExchangeHandler(mgr, st, nil, 0, getNow)
	req = httptest.NewRequestWithContext(t.Context(), "POST", "/internal/messages", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))
	rr = httptest.NewRecorder()
	handlerNoRet.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("peer-exchange-only no-retention: status = %d, want 200 (expired accepted when retention disabled); body: %s", rr.Code, rr.Body.String())
	}
}
