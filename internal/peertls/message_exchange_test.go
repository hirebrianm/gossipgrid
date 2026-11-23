// Package peertls: unit tests for the authenticated peer message exchange
// endpoint (POST /internal/messages, Milestone 4A).
//
// These tests cover the required positive and negative scenarios for the
// private message exchange endpoint. All certificates and CAs are generated
// ephemeral in memory (see testcert_test.go); no test keys are committed.
//
// The tests use actual Go HTTP/TLS behavior (real mTLS listeners, real HTTP
// clients) rather than direct handler invocations, so the full security
// properties (TLS 1.3, client-cert authentication, identity authorization,
// no-redirect, bounded decoding) are exercised end-to-end.
package peertls

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/server"
	"gossipgrid/internal/store"

	"github.com/google/uuid"
)

// newMessageTestStore opens a store backed by a temp-file SQLite database.
func newMessageTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.New(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	//nolint:errcheck // test cleanup: Close errors are not actionable
	t.Cleanup(func() { st.Close() })
	return st
}

// messageTestNode is a self-contained GossipGrid peer (Manager + mTLS listener
// + client + optional local store) for the message exchange tests. It retains
// the shared CA and its credential file paths so tests can rotate certs and
// build raw mTLS clients.
type messageTestNode struct {
	ca        *testCA
	mgr       *Manager
	client    *Client
	addr      string
	podID     string
	certFile  string
	keyFile   string
	trustFile string
	store     *store.Store
}

// newMessageTestNode builds a node with the given pod ID, shared CA, optional
// store, and message-exchange enable flag.
func newMessageTestNode(t *testing.T, ca *testCA, podID string, s *store.Store, enabled bool, reloadInterval time.Duration) (*messageTestNode, func()) {
	t.Helper()
	certFile, keyFile, trustFile := testCredentials(t, ca, podID)
	mgr := newTestManager(t, certFile, keyFile, trustFile, podID, reloadInterval)
	addr, stop := startMessageServer(t, mgr, podID, s, enabled)
	return &messageTestNode{
		ca: ca, mgr: mgr, client: NewClient(mgr), addr: addr,
		podID: podID, certFile: certFile, keyFile: keyFile, trustFile: trustFile,
		store: s,
	}, stop
}

// startMessageServer starts an mTLS listener on a random loopback port using
// the given Manager's server TLS config and the PeerServer's handler.
func startMessageServer(t *testing.T, mgr *Manager, nodeID string, s *store.Store, enabled bool) (addr string, stop func()) {
	t.Helper()
	peerSrv := NewPeerServerWithStore(mgr, ":0", nodeID, "v0.1.0-test", s, enabled)
	cfg := mgr.TLSConfigForServer()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	srv := &http.Server{Handler: peerSrv.Handler()}
	go func() {
		_ = srv.Serve(listener)
	}()
	return listener.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}

// makeValidMessage returns a well-formed MessageWire with a fresh UUIDv7 and
// origin "gossipgrid-0".
func makeValidMessage(t *testing.T, body string) MessageWire {
	t.Helper()
	id, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid.NewV7: %v", err)
	}
	return MessageWire{
		ID:        id.String(),
		Body:      body,
		Origin:    "gossipgrid-0",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// validID returns a fixed canonical UUIDv7 for tests that need a known ID.
func validID() string {
	return "019c0f1e-6c0a-7c00-8000-000000000000"
}

// --- 1. Successfully transmitting a message between authenticated replicas ---

func TestSendMessageSuccess(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	msg := makeValidMessage(t, "Hello from gossipgrid-0")
	stored, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, msg)
	if err != nil {
		t.Fatalf("A -> B SendMessage: %v", err)
	}
	if stored.ID != msg.ID || stored.Body != msg.Body || stored.Origin != msg.Origin {
		t.Errorf("stored = %+v, want %+v", stored, msg)
	}
	got, err := storeB.GetByID(msg.ID)
	if err != nil {
		t.Fatalf("storeB.GetByID: %v", err)
	}
	if got.Body != msg.Body || got.Origin != msg.Origin {
		t.Errorf("persisted = %+v, want body %q origin %q", got, msg.Body, msg.Origin)
	}
}

// --- 2. Receiver preserves UUIDv7, body, origin, and creation timestamp ---

func TestSendMessagePreservesIdentityAndTimestamp(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	original := time.Date(2026, 11, 20, 12, 34, 56, 123456789, time.UTC)
	msg := MessageWire{
		ID:        validID(),
		Body:      "preserve me",
		Origin:    "gossipgrid-5",
		CreatedAt: original.Format(time.RFC3339Nano),
	}
	_, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, msg)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	got, err := storeB.GetByID(msg.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.ID != msg.ID || got.Body != msg.Body || got.Origin != msg.Origin {
		t.Errorf("got %+v, want id %q body %q origin %q", got, msg.ID, msg.Body, msg.Origin)
	}
	if !got.CreatedAt.Equal(original) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, original)
	}
}

// --- 3. Repeated delivery of the same message is idempotent ---

func TestSendMessageIdempotent(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	msg := makeValidMessage(t, "idempotent")
	for i := 0; i < 3; i++ {
		if _, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, msg); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	var count int64
	if err := storeB.DB().Model(&store.Message{}).Count(&count).Error; err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1 (idempotent insert)", count)
	}
}

// --- 4. Conflicting duplicate ID returns HTTP 409 without altering the original row ---

func TestSendMessageConflictReturns409(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	msg := makeValidMessage(t, "original body")
	if _, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, msg); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	conflict := msg
	conflict.Body = "conflicting body"
	_, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, conflict)
	if err == nil {
		t.Fatal("expected 409 conflict error, got success")
	}
	if !strings.Contains(err.Error(), "409") {
		t.Errorf("error should mention 409, got: %v", err)
	}
	got, err := storeB.GetByID(msg.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Body != "original body" {
		t.Errorf("original row body = %q, want %q (conflict must not overwrite)", got.Body, "original body")
	}
}

// --- 5. Invalid UUIDv7 rejected ---

func TestSendMessageInvalidUUIDv7(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	cases := []struct {
		name string
		id   string
	}{
		{"not a uuid", "not-a-uuid"},
		{"uuid v4", "019c0f1e-6c0a-4c00-8000-000000000000"},
		{"uppercase", strings.ToUpper(validID())},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, MessageWire{
				ID:        tc.id,
				Body:      "x",
				Origin:    "gossipgrid-0",
				CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
			})
			if err == nil {
				t.Fatalf("expected invalid UUIDv7 error for %q, got success", tc.id)
			}
		})
	}
}

// --- 6. Missing origin rejected ---

func TestSendMessageMissingOrigin(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	_, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, MessageWire{
		ID:        validID(),
		Body:      "x",
		Origin:    "",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err == nil {
		t.Fatal("expected missing-origin error, got success")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error should mention 400, got: %v", err)
	}
}

// --- 7. Empty message body rejected ---

func TestSendMessageEmptyBody(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	_, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, MessageWire{
		ID:        validID(),
		Body:      "",
		Origin:    "gossipgrid-0",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err == nil {
		t.Fatal("expected empty-body error, got success")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error should mention 400, got: %v", err)
	}
}

// --- 8. Oversized message body rejected (4096 limit) ---

func TestSendMessageOversizedMessageBody(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	_, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, MessageWire{
		ID:        validID(),
		Body:      strings.Repeat("a", 4500),
		Origin:    "gossipgrid-0",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err == nil {
		t.Fatal("expected oversized-body error, got success")
	}
	if !strings.Contains(err.Error(), "413") {
		t.Errorf("error should mention 413, got: %v", err)
	}
}

// --- 9. Oversized HTTP request rejected (8192 limit) ---

func TestSendMessageOversizedHTTPRequest(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	_, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, MessageWire{
		ID:        validID(),
		Body:      strings.Repeat("b", 8200),
		Origin:    "gossipgrid-0",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	})
	if err == nil {
		t.Fatal("expected oversized-HTTP-request error, got success")
	}
	if !strings.Contains(err.Error(), "413") {
		t.Errorf("error should mention 413, got: %v", err)
	}
}

// --- 10. Malformed JSON rejected ---

func TestSendMessageMalformedJSON(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	body := []byte(`{"message": {"id": "` + validID() + `"`)
	if err := rawMTLSPost(t, nodeA, nodeB, body); err == nil {
		t.Fatal("expected malformed-JSON error, got success")
	}
}

// --- 11. Multiple JSON documents rejected ---

func TestSendMessageMultipleJSONDocuments(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	body := []byte(`{"message":{"id":"` + validID() + `","body":"x","origin":"gossipgrid-0","created_at":"2026-11-20T00:00:00Z"}}` +
		`{"message":{"id":"` + validID() + `","body":"y","origin":"gossipgrid-0","created_at":"2026-11-20T00:00:01Z"}}`)
	if err := rawMTLSPost(t, nodeA, nodeB, body); err == nil {
		t.Fatal("expected multi-JSON error, got success")
	}
}

// --- 12. Unknown JSON fields rejected ---

func TestSendMessageUnknownJSONFields(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	body := []byte(`{"message":{"id":"` + validID() + `","body":"x","origin":"gossipgrid-0","created_at":"2026-11-20T00:00:00Z"},"extra":"nope"}`)
	if err := rawMTLSPost(t, nodeA, nodeB, body); err == nil {
		t.Fatal("expected unknown-field error, got success")
	}
}

// --- 13. Unsupported HTTP method rejected ---

func TestSendMessageUnsupportedMethod(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	if err := rawMTLSGet(t, nodeA, nodeB); err == nil {
		t.Fatal("expected method-not-allowed for GET /internal/messages, got success")
	}
	if err := rawMTLSRequest(t, nodeA, nodeB, http.MethodPut, nil); err == nil {
		t.Fatal("expected method-not-allowed for PUT /internal/messages, got success")
	}
}

// --- 14. Missing client certificate rejected ---

func TestSendMessageNoClientCert(t *testing.T) {
	ca := newTestCA(t, "w")
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	if err := rawMTLSPostNoCert(t, nodeB); err == nil {
		t.Fatal("expected handshake failure for client with no cert, got success")
	}
}

// --- 15. Untrusted client CA rejected ---

func TestSendMessageUntrustedClientCA(t *testing.T) {
	caGood := newTestCA(t, "good")
	caBad := newTestCA(t, "bad")
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, caGood, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	clientCert, clientKey := issueLeaf(t, caBad, leafCertSpec{
		dnsNames: []string{"gossipgrid-0." + testSuffix},
	})
	if err := rawMTLSPostCustomCert(t, nodeB, "gossipgrid-1."+testSuffix, clientCert, clientKey, caGood.pem); err == nil {
		t.Fatal("expected handshake failure for client signed by unknown CA, got success")
	}
}

// --- 16. Unauthorized GossipGrid identity rejected ---

func TestSendMessageUnauthorizedIdentity(t *testing.T) {
	ca := newTestCA(t, "w")
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	clientCert, clientKey := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"gossipgrid-5." + testSuffix},
	})
	if err := rawMTLSPostCustomCert(t, nodeB, "gossipgrid-1."+testSuffix, clientCert, clientKey, ca.pem); err == nil {
		t.Fatal("expected handshake failure for out-of-range identity, got success")
	}
}

// --- 17. Wrong expected server identity rejected ---

func TestSendMessageWrongExpectedServerIdentity(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	msg := makeValidMessage(t, "x")
	_, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-2."+testSuffix, msg)
	if err == nil {
		t.Fatal("expected failure for wrong expected peer identity, got success")
	}
}

// --- 18. HTTP redirects rejected without following them ---

func TestSendMessageRejectsRedirect(t *testing.T) {
	var hits int32
	targetSrv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"message":{}}`))
		}),
	}
	lc := net.ListenConfig{}
	targetListener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening for redirect target: %v", err)
	}
	targetAddr := targetListener.Addr().String()
	go func() { _ = targetSrv.Serve(targetListener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = targetSrv.Shutdown(ctx)
	})

	ca := newTestCA(t, "w")
	certA, keyA, trustA := testCredentials(t, ca, "gossipgrid-0")
	certS, keyS, trustS := testCredentials(t, ca, "gossipgrid-1")
	mgrA := newTestManager(t, certA, keyA, trustA, "gossipgrid-0", 50*time.Millisecond)
	mgrB := newTestManager(t, certS, keyS, trustS, "gossipgrid-1", 50*time.Millisecond)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "http://"+targetAddr+"/internal/messages")
		w.WriteHeader(http.StatusFound)
	})
	mtlsAddr, stopMTLS := startTestServer(t, mgrB, mux)
	defer stopMTLS()

	clientA := NewClient(mgrA)
	msg := makeValidMessage(t, "x")
	_, err = clientA.SendMessage(context.Background(), mtlsAddr, "gossipgrid-1."+testSuffix, msg)
	if err == nil {
		t.Fatal("SendMessage succeeded despite redirect; it must reject all redirects")
	}
	if !strings.Contains(err.Error(), "redirect") {
		t.Errorf("error should mention redirect, got: %v", err)
	}
	if got := hits; got != 0 {
		t.Errorf("plaintext target received %d requests; it should have received 0", got)
	}
}

// --- 18a. Successful message acknowledgments are validated by the client ---
//
// The client must reject a 200/201 response that does not contain a
// non-nil message acknowledgment whose ID, body, origin, and creation
// timestamp match the submitted message. These tests use an authenticated
// mTLS server that returns crafted (possibly malformed) responses to
// exercise the client-side validation logic end-to-end.

// ackTestServer is an authenticated mTLS server that returns a fixed
// response (status + raw JSON body) for POST /internal/messages. It is used
// to exercise the client's acknowledgment validation logic.
type ackTestServer struct {
	t      *testing.T
	addr   string
	client *Client
	stop   func()
}

// startAckTestServer builds a node (Manager + mTLS listener) whose handler
// returns the given status and raw body for POST /internal/messages. The node
// is always identified as "gossipgrid-1" (the receiver in every acknowledgment
// validation scenario), so the pod ID is not parameterised.
func startAckTestServer(t *testing.T, ca *testCA, status int, body string) *ackTestServer {
	t.Helper()
	podID := "gossipgrid-1"
	cert, key, trust := testCredentials(t, ca, podID)
	mgr := newTestManager(t, cert, key, trust, podID, 100*time.Millisecond)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/messages", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
	cfg := mgr.TLSConfigForServer()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(listener) }()
	addr := listener.Addr().String()
	return &ackTestServer{
		t:      t,
		addr:   addr,
		client: NewClient(mgr),
		stop: func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = srv.Shutdown(ctx)
		},
	}
}

func (s *ackTestServer) send(t *testing.T, msg MessageWire) (*MessageWire, error) {
	t.Helper()
	return s.client.SendMessage(context.Background(), s.addr, "gossipgrid-1."+testSuffix, msg)
}

// TestSendMessageMissingAcknowledgment: a 200 response with no message
// field (e.g. {"peer":"gossipgrid-1"}) must be rejected.
func TestSendMessageMissingAcknowledgment(t *testing.T) {
	ca := newTestCA(t, "w")
	srv := startAckTestServer(t, ca, http.StatusOK, `{"peer":"gossipgrid-1"}`)
	defer srv.stop()

	msg := makeValidMessage(t, "x")
	got, err := srv.send(t, msg)
	if err == nil {
		t.Fatalf("expected error for missing acknowledgment, got success (message=%+v)", got)
	}
	if got != nil {
		t.Errorf("expected nil message, got %+v", got)
	}
	if !strings.Contains(err.Error(), "without a message acknowledgment") {
		t.Errorf("error should describe missing acknowledgment, got: %v", err)
	}
}

// TestSendMessageNullAcknowledgment: a 200 response with an explicit null
// message field must be rejected.
func TestSendMessageNullAcknowledgment(t *testing.T) {
	ca := newTestCA(t, "w")
	srv := startAckTestServer(t, ca, http.StatusOK, `{"message":null,"peer":"gossipgrid-1"}`)
	defer srv.stop()

	msg := makeValidMessage(t, "x")
	got, err := srv.send(t, msg)
	if err == nil {
		t.Fatalf("expected error for null acknowledgment, got success (message=%+v)", got)
	}
	if got != nil {
		t.Errorf("expected nil message, got %+v", got)
	}
	if !strings.Contains(err.Error(), "without a message acknowledgment") {
		t.Errorf("error should describe null acknowledgment, got: %v", err)
	}
}

// TestSendMessageMismatchedID: a 200 response echoing a different message ID
// must be rejected.
func TestSendMessageMismatchedID(t *testing.T) {
	ca := newTestCA(t, "w")
	original := makeValidMessage(t, "x")
	otherID := validID()
	srv := startAckTestServer(t, ca, http.StatusOK,
		fmt.Sprintf(`{"message":{"id":%q,"body":%q,"origin":%q,"created_at":%q},"peer":"gossipgrid-1"}`,
			otherID, original.Body, original.Origin, original.CreatedAt))
	defer srv.stop()

	got, err := srv.send(t, original)
	if err == nil {
		t.Fatalf("expected error for mismatched ID, got success (message=%+v)", got)
	}
	if got != nil {
		t.Errorf("expected nil message, got %+v", got)
	}
	if !strings.Contains(err.Error(), "acknowledged message ID") {
		t.Errorf("error should describe mismatched ID, got: %v", err)
	}
}

// TestSendMessageMismatchedBody: a 200 response echoing a different body
// must be rejected.
func TestSendMessageMismatchedBody(t *testing.T) {
	ca := newTestCA(t, "w")
	original := makeValidMessage(t, "correct body")
	srv := startAckTestServer(t, ca, http.StatusOK,
		fmt.Sprintf(`{"message":{"id":%q,"body":%q,"origin":%q,"created_at":%q},"peer":"gossipgrid-1"}`,
			original.ID, "wrong body", original.Origin, original.CreatedAt))
	defer srv.stop()

	got, err := srv.send(t, original)
	if err == nil {
		t.Fatalf("expected error for mismatched body, got success (message=%+v)", got)
	}
	if !strings.Contains(err.Error(), "acknowledged message body") {
		t.Errorf("error should describe mismatched body, got: %v", err)
	}
}

// TestSendMessageMismatchedOrigin: a 200 response echoing a different origin
// must be rejected.
func TestSendMessageMismatchedOrigin(t *testing.T) {
	ca := newTestCA(t, "w")
	original := makeValidMessage(t, "x")
	srv := startAckTestServer(t, ca, http.StatusOK,
		fmt.Sprintf(`{"message":{"id":%q,"body":%q,"origin":%q,"created_at":%q},"peer":"gossipgrid-1"}`,
			original.ID, original.Body, "gossipgrid-9", original.CreatedAt))
	defer srv.stop()

	got, err := srv.send(t, original)
	if err == nil {
		t.Fatalf("expected error for mismatched origin, got success (message=%+v)", got)
	}
	if !strings.Contains(err.Error(), "acknowledged message origin") {
		t.Errorf("error should describe mismatched origin, got: %v", err)
	}
}

// TestSendMessageMismatchedTimestamp: a 200 response echoing a different
// creation timestamp (different instant) must be rejected.
func TestSendMessageMismatchedTimestamp(t *testing.T) {
	ca := newTestCA(t, "w")
	original := makeValidMessage(t, "x")
	other := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	srv := startAckTestServer(t, ca, http.StatusOK,
		fmt.Sprintf(`{"message":{"id":%q,"body":%q,"origin":%q,"created_at":%q},"peer":"gossipgrid-1"}`,
			original.ID, original.Body, original.Origin, other.Format(time.RFC3339)))
	defer srv.stop()

	got, err := srv.send(t, original)
	if err == nil {
		t.Fatalf("expected error for mismatched timestamp, got success (message=%+v)", got)
	}
	if !strings.Contains(err.Error(), "mismatched message timestamp") {
		t.Errorf("error should describe mismatched timestamp, got: %v", err)
	}
}

// TestSendMessageMalformedTimestamp: a 200 response with a syntactically
// invalid creation timestamp must be rejected.
func TestSendMessageMalformedTimestamp(t *testing.T) {
	ca := newTestCA(t, "w")
	original := makeValidMessage(t, "x")
	srv := startAckTestServer(t, ca, http.StatusOK,
		fmt.Sprintf(`{"message":{"id":%q,"body":%q,"origin":%q,"created_at":%q},"peer":"gossipgrid-1"}`,
			original.ID, original.Body, original.Origin, "not-a-timestamp"))
	defer srv.stop()

	got, err := srv.send(t, original)
	if err == nil {
		t.Fatalf("expected error for malformed timestamp, got success (message=%+v)", got)
	}
	if !strings.Contains(err.Error(), "malformed") {
		t.Errorf("error should describe malformed timestamp, got: %v", err)
	}
}

// TestSendMessageEquivalentTimestamps: a 200 response whose creation
// timestamp denotes the same instant in a different RFC3339 representation
// (with vs without fractional seconds) must be accepted. The submitted
// timestamp uses RFC3339 (no fractional seconds) and the server echoes the
// same instant in RFC3339Nano (with fractional seconds). Both parse to the
// same instant, so the comparison by instant must accept it.
func TestSendMessageEquivalentTimestamps(t *testing.T) {
	ca := newTestCA(t, "w")
	// A base instant at an exact second (no fractional component).
	base := time.Date(2026, 11, 20, 12, 34, 56, 0, time.UTC)
	// The message is sent with the timestamp in RFC3339 (no fractional
	// seconds): "2026-11-20T12:34:56Z".
	msg := MessageWire{
		ID:        validID(),
		Body:      "equivalent",
		Origin:    "gossipgrid-0",
		CreatedAt: base.Format(time.RFC3339),
	}
	// The server echoes the same instant in RFC3339Nano (with fractional
	// seconds): "2026-11-20T12:34:56.000000000Z". Both parse to the same
	// instant.
	srv := startAckTestServer(t, ca, http.StatusOK,
		fmt.Sprintf(`{"message":{"id":%q,"body":%q,"origin":%q,"created_at":%q},"peer":"gossipgrid-1"}`,
			msg.ID, msg.Body, msg.Origin, base.Format(time.RFC3339Nano)))
	defer srv.stop()

	got, err := srv.send(t, msg)
	if err != nil {
		t.Fatalf("expected success for equivalent timestamp, got: %v", err)
	}
	if got == nil || got.ID != msg.ID {
		t.Errorf("got %+v, want ID %q", got, msg.ID)
	}
}

// TestSendMessageNormal200Acknowledgment: a 200 response with a matching
// message acknowledgment must be accepted.
func TestSendMessageNormal200Acknowledgment(t *testing.T) {
	ca := newTestCA(t, "w")
	original := makeValidMessage(t, "hello")
	srv := startAckTestServer(t, ca, http.StatusOK,
		fmt.Sprintf(`{"message":{"id":%q,"body":%q,"origin":%q,"created_at":%q},"peer":"gossipgrid-1"}`,
			original.ID, original.Body, original.Origin, original.CreatedAt))
	defer srv.stop()

	got, err := srv.send(t, original)
	if err != nil {
		t.Fatalf("expected success for 200 acknowledgment, got: %v", err)
	}
	if got.ID != original.ID || got.Body != original.Body || got.Origin != original.Origin {
		t.Errorf("got %+v, want %+v", got, original)
	}
}

// TestSendMessageNormal201Acknowledgment: a 201 response with a matching
// message acknowledgment must be accepted.
func TestSendMessageNormal201Acknowledgment(t *testing.T) {
	ca := newTestCA(t, "w")
	original := makeValidMessage(t, "created")
	srv := startAckTestServer(t, ca, http.StatusCreated,
		fmt.Sprintf(`{"message":{"id":%q,"body":%q,"origin":%q,"created_at":%q},"peer":"gossipgrid-1"}`,
			original.ID, original.Body, original.Origin, original.CreatedAt))
	defer srv.stop()

	got, err := srv.send(t, original)
	if err != nil {
		t.Fatalf("expected success for 201 acknowledgment, got: %v", err)
	}
	if got.ID != original.ID || got.Body != original.Body {
		t.Errorf("got %+v, want ID %q body %q", got, original.ID, original.Body)
	}
}

// --- 19. Private message endpoint absent from public router ---
//
// The public router (built by server.NewWithStore) must not register
// POST /internal/messages even when the node has a valid store. This test
// uses the real handler from internal/server.NewWithStore rather than a
// synthetic imitation, so it exercises the actual routing code that ships
// in production.
func TestMessageEndpointAbsentFromPublicRouter(t *testing.T) {
	// Build the real public router with a valid store. The store is created
	// so the message endpoints are registered, proving that the absence of
	// /internal/messages is a property of the real router, not of a
	// degenerate no-store configuration.
	st, err := store.New(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	//nolint:errcheck // test cleanup: Close errors are not actionable
	t.Cleanup(func() { st.Close() })
	handler := server.NewWithStore(server.Node{ID: "test-node"}, st)

	req := httptest.NewRequestWithContext(context.Background(), "POST", "/internal/messages", strings.NewReader(`{}`))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code == http.StatusOK {
		t.Errorf("public router served POST /internal/messages with 200; it must be absent (want 404 or 405)")
	}
}

// --- 20. Message exchange disabled by default ---

func TestMessageExchangeDisabledByDefault(t *testing.T) {
	ca := newTestCA(t, "w")
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, false, 100*time.Millisecond)
	defer stopB()
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()

	msg := makeValidMessage(t, "x")
	_, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, msg)
	if err == nil {
		t.Fatal("expected failure when message exchange is disabled, got success")
	}
	if !strings.Contains(err.Error(), "404") && !strings.Contains(err.Error(), "405") {
		t.Errorf("error should reflect 404/405 when disabled, got: %v", err)
	}
	var count int64
	if err := storeB.DB().Model(&store.Message{}).Count(&count).Error; err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("store row count = %d, want 0 (exchange disabled)", count)
	}
}

// --- 22. Request timeout and context cancellation ---

func TestSendMessageContextCancellation(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	msg := makeValidMessage(t, "x")
	_, err := nodeA.client.SendMessage(ctx, nodeB.addr, "gossipgrid-1."+testSuffix, msg)
	if err == nil {
		t.Fatal("expected context-cancellation error, got success")
	}
}

// --- 23. Concurrent duplicate deliveries do not create duplicate rows ---

func TestSendMessageConcurrentDuplicates(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	msg := makeValidMessage(t, "concurrent")
	var wg sync.WaitGroup
	errCh := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, msg)
			if err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent send: %v", err)
	}
	var count int64
	if err := storeB.DB().Model(&store.Message{}).Count(&count).Error; err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1 (concurrent idempotent insert)", count)
	}
}

// --- 24. Concurrent conflicting inserts preserve the immutable winning row ---

func TestSendMessageConcurrentConflicts(t *testing.T) {
	ca := newTestCA(t, "w")
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	storeB := newMessageTestStore(t)
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 100*time.Millisecond)
	defer stopB()

	winner := makeValidMessage(t, "winner body")
	if _, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, winner); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			conflict := winner
			conflict.Body = fmt.Sprintf("loser body %d", i)
			_, _ = nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, conflict)
		}(i)
	}
	wg.Wait()
	got, err := storeB.GetByID(winner.ID)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if got.Body != "winner body" {
		t.Errorf("winner body = %q, want %q (concurrent conflicts must not overwrite)", got.Body, "winner body")
	}
	var count int64
	if err := storeB.DB().Model(&store.Message{}).Count(&count).Error; err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("row count = %d, want 1 (concurrent conflicting inserts)", count)
	}
}

// --- 25. Certificate rotation does not break subsequent new authenticated requests ---

func TestSendMessageAfterCertRotation(t *testing.T) {
	ca := newTestCA(t, "w")
	storeB := newMessageTestStore(t)
	nodeA, stopA := newMessageTestNode(t, ca, "gossipgrid-0", nil, true, 100*time.Millisecond)
	defer stopA()
	nodeB, stopB := newMessageTestNode(t, ca, "gossipgrid-1", storeB, true, 50*time.Millisecond)
	defer stopB()

	msg := makeValidMessage(t, "rotate")
	if _, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, msg); err != nil {
		t.Fatalf("first send (before rotation): %v", err)
	}
	cert, key := issueLeaf(t, ca, leafCertSpec{dnsNames: []string{"gossipgrid-1." + testSuffix}})
	chainPEM, keyPEM := certKeyPEM(t, cert, key, ca)
	// Capture the reload baseline BEFORE writing the rotated material: the
	// reload worker polls every 50ms, so a reload can complete before a
	// post-write baseline is read, and the wait would then target a second
	// reload that never occurs.
	prevRC := nodeB.mgr.Diagnostics().ReloadCount
	//nolint:gosec/G306 // test fixture in temp dir
	if err := os.WriteFile(nodeB.certFile, chainPEM, 0o600); err != nil {
		t.Fatalf("writing rotated cert: %v", err)
	}
	//nolint:gosec/G306 // test fixture in temp dir
	if err := os.WriteFile(nodeB.keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("writing rotated key: %v", err)
	}
	waitForReload(t, nodeB.mgr, prevRC)
	msg2 := makeValidMessage(t, "after-rotate")
	if _, err := nodeA.client.SendMessage(context.Background(), nodeB.addr, "gossipgrid-1."+testSuffix, msg2); err != nil {
		t.Fatalf("second send (after rotation): %v", err)
	}
}

// --- Raw mTLS request helpers ---

func rawMTLSPost(t *testing.T, sender, target *messageTestNode, body []byte) error {
	t.Helper()
	return rawMTLSRequest(t, sender, target, http.MethodPost, body)
}

func rawMTLSGet(t *testing.T, sender, target *messageTestNode) error {
	t.Helper()
	return rawMTLSRequest(t, sender, target, http.MethodGet, nil)
}

func rawMTLSRequest(t *testing.T, sender, target *messageTestNode, method string, body []byte) error {
	t.Helper()
	tlsCfg, err := sender.mgr.TLSConfigForClient(target.addr, "gossipgrid-1."+testSuffix)
	if err != nil {
		return fmt.Errorf("building TLS config: %w", err)
	}
	client := buildClient(tlsCfg)
	var req *http.Request
	if body == nil {
		req, err = http.NewRequestWithContext(context.Background(), method, "https://"+target.addr+"/internal/messages", nil)
	} else {
		req, err = http.NewRequestWithContext(context.Background(), method, "https://"+target.addr+"/internal/messages", bytes.NewReader(body))
	}
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		client.Transport.(*http.Transport).CloseIdleConnections()
		return err
	}
	defer func() {
		_ = resp.Body.Close()
		client.Transport.(*http.Transport).CloseIdleConnections()
	}()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("peer returned status %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

func rawMTLSPostNoCert(t *testing.T, target *messageTestNode) error {
	t.Helper()
	pool := loadPool(t, target.trustFile)
	tlsCfg := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
		ServerName: "gossipgrid-1." + testSuffix,
	}
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
		Timeout:   3 * time.Second,
	}
	body := []byte(`{"message":{"id":"` + validID() + `","body":"x","origin":"gossipgrid-0","created_at":"2026-11-20T00:00:00Z"}}`)
	req, err := http.NewRequestWithContext(context.Background(), "POST", "https://"+target.addr+"/internal/messages", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func rawMTLSPostCustomCert(t *testing.T, target *messageTestNode, serverName string, clientCert *x509.Certificate, clientKey any, caPEM []byte) error {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("no valid CA in trust bundle")
	}
	tlsCfg := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{clientCert.Raw},
			PrivateKey:  clientKey,
		}},
		ServerName: serverName,
	}
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
		Timeout:   3 * time.Second,
	}
	body := []byte(`{"message":{"id":"` + validID() + `","body":"x","origin":"gossipgrid-0","created_at":"2026-11-20T00:00:00Z"}}`)
	req, err := http.NewRequestWithContext(context.Background(), "POST", "https://"+target.addr+"/internal/messages", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}
