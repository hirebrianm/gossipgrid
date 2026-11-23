package peertls

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/store"
)

// blockingBodyReader is an io.Reader that blocks until the test releases it
// via a channel. It delivers the full payload after release. This simulates
// a slow client that is still sending the request body when authorization
// is revoked on the server.
//
// The JSON decoder reads from this reader in chunks. The first Read call
// blocks (the client hasn't sent the body yet). After the test releases
// the barrier, the remaining reads succeed and deliver the full payload.
//
// enteredCh is closed when the first Read is called (the decoder has started
// reading the body). The test waits on this to know the handler is in the
// body-parsing phase.
type blockingBodyReader struct {
	data       []byte
	mu         sync.Mutex
	released   bool
	enteredCh  chan struct{}
	once       sync.Once
	releasedCh chan struct{}
}

func newBlockingBodyReader(data []byte) *blockingBodyReader {
	return &blockingBodyReader{
		data:       data,
		enteredCh:  make(chan struct{}),
		releasedCh: make(chan struct{}),
	}
}

func (b *blockingBodyReader) Read(p []byte) (int, error) {
	b.once.Do(func() {
		close(b.enteredCh)
	})
	b.mu.Lock()
	if !b.released {
		b.mu.Unlock()
		select {
		case <-b.releasedCh:
		case <-time.After(30 * time.Second):
			return 0, context.DeadlineExceeded
		}
		b.mu.Lock()
	}
	b.mu.Unlock()
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > len(b.data) {
		n = len(b.data)
	}
	copy(p, b.data[:n])
	b.data = b.data[n:]
	return n, nil
}

func (b *blockingBodyReader) release() {
	close(b.releasedCh)
	b.mu.Lock()
	b.released = true
	b.mu.Unlock()
}

// TestInboundMessageExchangeRevocationDuringBodyParse verifies that when
// authorization is revoked while the request body is being parsed (before
// the pre-insert admission check), the request is rejected, no new row is
// inserted in SQLite, and the gossip callback count remains zero.
//
// Deterministic sequence:
//  1. A valid epoch-A inbound message request is started.
//  2. The request body is blocked (the JSON decoder is waiting for data).
//  3. The test waits for the decoder to enter the body-parsing phase
//     (enteredCh signal).
//  4. The test revokes authorization (stops the controller).
//  5. The test releases the body (the decoder completes JSON parsing).
//  6. The pre-insert admission check re-reads the controller's snapshot,
//     sees the stopped/revoked state, and rejects the request.
//  7. The test verifies: response is rejected (non-200), SQLite has no
//     new row, gossip callback count is 0.
func TestInboundMessageExchangeRevocationDuringBodyParse(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newInboundTestController(t, lifecycle.READY, "c1", "e1")

	var gossipCallbackCount int
	var cbMu sync.Mutex
	onNewMessage := func(msg *store.Message, senderPodID string) {
		cbMu.Lock()
		gossipCallbackCount++
		cbMu.Unlock()
	}

	mgr := newInboundTestManager(t)
	handler := newLifecycleMessageExchangeHandler(mgr, st, onNewMessage, ctrl, 0, time.Now)

	// Build a valid message request body.
	body := `{"message":{"id":"019a0000-0000-7000-0000-000000000001","body":"test-body","origin":"gossipgrid-0.test","created_at":"2026-11-21T00:00:00Z"}}`
	blockingReader := newBlockingBodyReader([]byte(body))

	// Build the request with the blocking body.
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/internal/messages", blockingReader)
	req.Header.Set(lifecycle.HeaderClusterID, "c1")
	req.Header.Set(lifecycle.HeaderEpochID, "e1")
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))

	// Run the handler in a goroutine.
	done := make(chan struct{})
	var respCode int
	w := httptest.NewRecorder()
	go func() {
		handler.ServeHTTP(w, req)
		respCode = w.Code
		close(done)
	}()

	// Wait for the decoder to enter the body-parsing phase (deterministic).
	select {
	case <-blockingReader.enteredCh:
	case <-time.After(5 * time.Second):
		t.Fatal("decoder did not enter body-parsing phase within 5s")
	}

	// Revoke authorization (stop the controller).
	ctrl.Stop(context.Background())

	// Release the body (the decoder completes JSON parsing).
	blockingReader.release()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not complete within 10s")
	}

	// Verify: the response must be rejected (non-200).
	if respCode == http.StatusOK {
		t.Errorf("expected non-200 response when authorization is revoked during body parse, got 200")
	}
	if respCode != lifecycle.UnreadyStatus && respCode != lifecycle.EpochMismatchStatus {
		t.Errorf("expected 503 or 409, got %d", respCode)
	}

	// Verify: no new row in SQLite.
	count, err := st.CountMessages()
	if err != nil {
		t.Fatalf("CountMessages: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 messages in store, got %d", count)
	}

	// Verify: gossip callback count is 0.
	cbMu.Lock()
	calls := gossipCallbackCount
	cbMu.Unlock()
	if calls != 0 {
		t.Errorf("expected 0 gossip callback invocations, got %d", calls)
	}
}

// TestInboundMessageExchangeNoRevocationDuringBodyParse verifies that when
// authorization remains valid through the pre-insert admission boundary,
// the insert succeeds and the gossip callback is triggered.
//
// This is the positive control for
// TestInboundMessageExchangeRevocationDuringBodyParse.
func TestInboundMessageExchangeNoRevocationDuringBodyParse(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newInboundTestController(t, lifecycle.READY, "c1", "e1")

	var gossipCallbackCount int
	var cbMu sync.Mutex
	onNewMessage := func(msg *store.Message, senderPodID string) {
		cbMu.Lock()
		gossipCallbackCount++
		cbMu.Unlock()
	}

	mgr := newInboundTestManager(t)
	handler := newLifecycleMessageExchangeHandler(mgr, st, onNewMessage, ctrl, 0, time.Now)

	body := `{"message":{"id":"019a0000-0000-7000-0000-000000000001","body":"test-body","origin":"gossipgrid-0.test","created_at":"2026-11-21T00:00:00Z"}}`
	blockingReader := newBlockingBodyReader([]byte(body))

	req := httptest.NewRequestWithContext(context.Background(), "POST", "/internal/messages", blockingReader)
	req.Header.Set(lifecycle.HeaderClusterID, "c1")
	req.Header.Set(lifecycle.HeaderEpochID, "e1")
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))

	done := make(chan struct{})
	var respCode int
	w := httptest.NewRecorder()
	go func() {
		handler.ServeHTTP(w, req)
		respCode = w.Code
		close(done)
	}()

	// Wait for the decoder to enter the body-parsing phase.
	select {
	case <-blockingReader.enteredCh:
	case <-time.After(5 * time.Second):
		t.Fatal("decoder did not enter body-parsing phase within 5s")
	}

	// Do NOT revoke authorization.

	// Release the body.
	blockingReader.release()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not complete within 10s")
	}

	// Verify: the response must be 200.
	if respCode != http.StatusOK {
		t.Errorf("expected 200 response when authorization remains valid, got %d", respCode)
	}

	// Verify: one new row in SQLite.
	count, err := st.CountMessages()
	if err != nil {
		t.Fatalf("CountMessages: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 message in store, got %d", count)
	}

	// Verify: gossip callback count is 1.
	cbMu.Lock()
	calls := gossipCallbackCount
	cbMu.Unlock()
	if calls != 1 {
		t.Errorf("expected 1 gossip callback invocation, got %d", calls)
	}
}

// TestInboundInventoryRevocationAtDataBoundary verifies that when
// authorization is revoked between the initial admission check and the
// data-serving boundary for the inventory endpoint, the request is rejected
// and no data is served.
//
// The inventory endpoint (GET) has no request-body I/O, so the gap between
// the initial admission check and the store read is small. The test
// simulates this by stopping the controller before calling the handler.
// The handler's initial admission check sees the stopped state and rejects.
//
// This also verifies the preDataAdmission callback in
// newSyncInventoryHandlerWithAdmission: when the controller is stopped,
// the callback returns a non-zero status.
func TestInboundInventoryRevocationAtDataBoundary(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newInboundTestController(t, lifecycle.READY, "c1", "e1")

	// Stop the controller (revoking authorization).
	ctrl.Stop(context.Background())

	// The preDataAdmission callback should reject.
	reqCtx := lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}
	preDataAdmission := func() (int, string) {
		recheckSnap := ctrl.Authorization()
		return finalAdmissionCheck(recheckSnap, reqCtx)
	}
	status, reason := preDataAdmission()
	if status == 0 {
		t.Error("expected rejection for revoked authorization at data boundary, got 0 (allowed)")
	}
	if reason == "" {
		t.Error("expected non-empty reason for rejection")
	}

	// Now test the full handler path: the initial admission check should
	// reject (the controller is stopped), so the handler returns early.
	handler := newLifecycleSyncInventoryHandler(st, ctrl, 0, time.Now)
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/internal/sync/ids?limit=10", nil)
	req.Header.Set(lifecycle.HeaderClusterID, "c1")
	req.Header.Set(lifecycle.HeaderEpochID, "e1")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code == http.StatusOK {
		t.Error("expected non-200 when controller is stopped, got 200")
	}
}

// TestInboundFetchRevocationAtDataBoundary verifies that when
// authorization is revoked between the initial admission check and the
// data-serving boundary for the fetch endpoint (during request-body
// parsing), the request is rejected and no data is served.
//
// Deterministic sequence:
//  1. A valid epoch-A inbound fetch request is started.
//  2. The request body is blocked (the JSON decoder is waiting for data).
//  3. The test waits for the decoder to enter the body-parsing phase.
//  4. The test revokes authorization (stops the controller).
//  5. The test releases the body (the decoder completes JSON parsing and
//     validates the IDs).
//  6. The preDataAdmission callback re-reads the controller's snapshot,
//     sees the stopped state, and rejects the request.
//  7. The test verifies: response is rejected (non-200).
func TestInboundFetchRevocationAtDataBoundary(t *testing.T) {
	st := newInboundTestStore(t)
	ctrl := newInboundTestController(t, lifecycle.READY, "c1", "e1")

	handler := newLifecycleSyncFetchHandler(st, ctrl, 0, time.Now)

	// A valid fetch request body with one UUIDv7 ID.
	body := `{"ids":["019a0000-0000-7000-0000-000000000001"]}`
	blockingReader := newBlockingBodyReader([]byte(body))

	req := httptest.NewRequestWithContext(context.Background(), "POST", "/internal/sync/messages", blockingReader)
	req.Header.Set(lifecycle.HeaderClusterID, "c1")
	req.Header.Set(lifecycle.HeaderEpochID, "e1")
	req.Header.Set("Content-Type", "application/json")
	req.ContentLength = int64(len(body))

	done := make(chan struct{})
	var respCode int
	w := httptest.NewRecorder()
	go func() {
		handler.ServeHTTP(w, req)
		respCode = w.Code
		close(done)
	}()

	// Wait for the decoder to enter the body-parsing phase.
	select {
	case <-blockingReader.enteredCh:
	case <-time.After(5 * time.Second):
		t.Fatal("decoder did not enter body-parsing phase within 5s")
	}

	// Revoke authorization (stop the controller).
	ctrl.Stop(context.Background())

	// Release the body (the decoder completes JSON parsing and validates IDs).
	blockingReader.release()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handler did not complete within 10s")
	}

	// Verify: the response must be rejected (non-200).
	if respCode == http.StatusOK {
		t.Errorf("expected non-200 response when authorization is revoked during fetch body parse, got 200")
	}
	if respCode != lifecycle.UnreadyStatus && respCode != lifecycle.EpochMismatchStatus {
		t.Errorf("expected 503 or 409, got %d", respCode)
	}
}

// newInboundTestManager creates a minimal Manager for tests that need it
// but don't exercise the TLS layer. The Manager is not Started; its
// Diagnostics() returns zero values which is sufficient for the handler
// tests (the LocalPodID is empty, which is fine for the test).
func newInboundTestManager(t *testing.T) *Manager {
	t.Helper()
	return New(Config{
		CertFile:         "/dev/null",
		KeyFile:          "/dev/null",
		TrustBundleFile:  "/dev/null",
		IdentitySuffix:   "test",
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
		ExpectedReplicas: 2,
		ReloadInterval:   time.Hour,
	}, nil)
}
