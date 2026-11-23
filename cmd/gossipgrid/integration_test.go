// Package main: local multi-process integration test (Milestone 3B).
//
// This test demonstrates two GossipGrid nodes communicating over authenticated
// mTLS using locally generated, short-lived test certificates. Each node has a
// distinct identity, SQLite database path, public HTTP port, private HTTPS
// port, and certificate/key pair. The nodes dial loopback addresses while
// separately verifying the expected certificate identities (the dial address
// and the authenticated peer identity are distinct inputs).
//
// Demonstrates:
//   - successful authenticated GET /internal/status;
//   - failure when the expected peer identity is wrong;
//   - failure with an untrusted client certificate;
//   - successful credential rotation without restarting the services.
//
// No development certificates or private keys are committed: everything is
// generated in memory per test run and discarded when the test ends.
package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/discovery"
	"gossipgrid/internal/gossip"
	"gossipgrid/internal/peertls"
	"gossipgrid/internal/server"
	"gossipgrid/internal/store"
)

const (
	intSuffix      = "gossipgrid-headless.int.svc"
	intStatefulSet = "gossipgrid"
)

// integrationNode is a self-contained GossipGrid peer (Manager + peer server +
// public HTTP server + client) for the integration test. It owns its credential
// files, a private HTTPS listener on a random loopback port, and (when a store
// is supplied) a public HTTP listener serving the real public router.
type integrationNode struct {
	mgr        *peertls.Manager
	client     *peertls.Client
	addr       string // peer listener address (host:port)
	podID      string // e.g. "gossipgrid-0"
	certFile   string
	keyFile    string
	trustFile  string
	store      *store.Store // local SQLite store (nil for status-only nodes)
	publicAddr string       // public listener address (empty when store is nil)

	// Shutdown functions (idempotent; safe to call multiple times).
	peerShutdown   func() // shuts down the peer http.Server + closes the listener
	publicShutdown func() // shuts down the public http.Server + closes the listener
}

// newIntegrationNode starts a GossipGrid peer node with the given pod short
// name (e.g. "gossipgrid-0"). The node uses the supplied workload CA for both
// its own certificate and its trust bundle (so nodes sharing the CA trust
// each other). The node has no local store (status-only) and no public
// listener.
func newIntegrationNode(t *testing.T, ca *testCA, podID string) *integrationNode {
	return newIntegrationNodeWithStore(t, ca, podID, nil, false)
}

// newIntegrationNodeWithStore starts a GossipGrid peer node with a local
// SQLite store and an optional message-exchange endpoint. When store is
// non-nil and enabled is true, POST /internal/messages is registered on the
// private peer listener. When store is non-nil, the node also starts a public
// HTTP listener serving the real public router (server.NewWithStore) so tests
// can create messages through POST /api/messages rather than calling the
// store directly.
//
// Cleanup is idempotent: each listener is closed exactly once (whether
// explicitly stopped by the test or closed by t.Cleanup), and the store is
// closed exactly once (whether explicitly closed by the test or by
// t.Cleanup). This makes explicit mid-test shutdown safe.
func newIntegrationNodeWithStore(t *testing.T, ca *testCA, podID string, s *store.Store, enabled bool) *integrationNode {
	t.Helper()
	dir := t.TempDir()
	cert, key := issueLeafCert(t, ca, podID+"."+intSuffix)
	chainPEM, keyPEM := certKeyPEMTwo(t, cert, key, ca)
	certFile := writePEMFile(t, dir, "cert.pem", chainPEM)
	keyFile := writePEMFile(t, dir, "key.pem", keyPEM)
	trustFile := writePEMFile(t, dir, "ca-bundle.pem", ca.pem)

	mgr := peertls.New(peertls.Config{
		CertFile:         certFile,
		KeyFile:          keyFile,
		TrustBundleFile:  trustFile,
		IdentitySuffix:   intSuffix,
		StatefulSetName:  intStatefulSet,
		NodeID:           podID,
		ExpectedReplicas: 2,
		ReloadInterval:   50 * time.Millisecond,
	}, nil)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("%s: manager Start: %v", podID, err)
	}
	t.Cleanup(mgr.Shutdown)

	// Build a PeerServer with the optional store and message-exchange flag.
	peerSrv := peertls.NewPeerServerWithStore(mgr, ":0", podID, "v0.1.0-int", s, enabled)

	// Listen on a random loopback port with the mTLS config.
	tlsListener, err := tls.Listen("tcp", "127.0.0.1:0", mgr.TLSConfigForServer())
	if err != nil {
		t.Fatalf("%s: tls.Listen: %v", podID, err)
	}
	addr := tlsListener.Addr().String()

	httpSrv := &http.Server{Handler: peerSrv.Handler()}
	go func() { _ = httpSrv.Serve(tlsListener) }()
	peerShutdown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
		_ = tlsListener.Close()
	}
	t.Cleanup(peerShutdown)

	// Optional public HTTP listener (only when a store is supplied). The
	// public server uses the real public router from server.NewWithStore,
	// so tests can create messages through POST /api/messages rather than
	// calling the store directly.
	var publicAddr string
	var publicShutdown func()
	if s != nil {
		publicSrv := server.NewServer(":0", server.NewWithStore(server.Node{ID: podID}, s))
		lc := net.ListenConfig{}
		publicListener, lerr := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
		if lerr != nil {
			t.Fatalf("%s: net.ListenConfig.Listen (public): %v", podID, lerr)
		}
		publicAddr = publicListener.Addr().String()
		go func() { _ = publicSrv.HTTPServer.Serve(publicListener) }()
		publicShutdown = func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = publicSrv.Shutdown(ctx)
			_ = publicListener.Close()
		}
		t.Cleanup(publicShutdown)
	}

	return &integrationNode{
		mgr: mgr, client: peertls.NewClient(mgr), addr: addr,
		podID: podID, certFile: certFile, keyFile: keyFile, trustFile: trustFile,
		store: s, publicAddr: publicAddr,
		peerShutdown: peerShutdown, publicShutdown: publicShutdown,
	}
}

// newIntegrationStore opens a store backed by a temp-file SQLite database.
// The returned close function is idempotent: the test may close the store
// explicitly during the test (to simulate node shutdown) and t.Cleanup will
// not double-close it.
func newIntegrationStore(t *testing.T) (*store.Store, func() error) {
	t.Helper()
	st, err := store.New(filepath.Join(t.TempDir(), "node.db"))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	closed := false
	closeStore := func() error {
		if closed {
			return nil
		}
		closed = true
		return st.Close()
	}
	//nolint:errcheck // test cleanup
	t.Cleanup(func() { _ = closeStore() })
	return st, closeStore
}

// --- Integration scenarios ---

func TestIntegrationTwoNodesMTLS(t *testing.T) {
	// A shared workload CA so the two nodes trust each other.
	sharedCA := newTestCA(t, "workload")
	node0 := newIntegrationNode(t, sharedCA, "gossipgrid-0")
	node1 := newIntegrationNode(t, sharedCA, "gossipgrid-1")

	// (a) Successful authenticated GET /internal/status (node0 -> node1).
	st, err := node0.client.GetStatus(context.Background(), node1.addr, "gossipgrid-1."+intSuffix)
	if err != nil {
		t.Fatalf("node0 -> node1 GetStatus: %v", err)
	}
	if st.NodeID != "gossipgrid-1" || st.Version != "v0.1.0-int" {
		t.Errorf("node1 status = %+v, want node gossipgrid-1 version v0.1.0-int", st)
	}

	// (b) Mutual: node1 -> node0.
	st0, err := node1.client.GetStatus(context.Background(), node0.addr, "gossipgrid-0."+intSuffix)
	if err != nil {
		t.Fatalf("node1 -> node0 GetStatus: %v", err)
	}
	if st0.NodeID != "gossipgrid-0" {
		t.Errorf("node0 status = %+v, want node gossipgrid-0", st0)
	}

	// (c) Failure when the expected peer identity is wrong: node0 connects to
	// node1 but expects gossipgrid-2 (node1's SAN is gossipgrid-1, so the
	// handshake must fail).
	if _, err := node0.client.GetStatus(context.Background(), node1.addr, "gossipgrid-2."+intSuffix); err == nil {
		t.Fatal("expected failure for wrong expected peer identity, got success")
	}

	// (d) Failure with an untrusted client certificate: a client signed by a
	// CA not in node1's trust bundle must be rejected.
	untrustedCA := newTestCA(t, "untrusted")
	if err := untrustedClientGet(t, node1, untrustedCA); err == nil {
		t.Fatal("expected failure for untrusted client certificate, got success")
	}

	// (e) Successful credential rotation without restart: rotate node1's leaf
	// cert (new key, same identity) and verify node0 can still connect to
	// node1 over a NEW connection, and that the new cert is presented.
	fpBefore := node1.mgr.Diagnostics().CertFingerprint
	rcBefore := node1.mgr.Diagnostics().ReloadCount
	rotateNodeCert(t, node1, sharedCA)
	waitForNodeReload(t, node1, rcBefore)
	fpAfter := node1.mgr.Diagnostics().CertFingerprint
	if fpBefore == fpAfter {
		t.Fatal("node1 cert fingerprint did not change after rotation")
	}
	st2, err := node0.client.GetStatus(context.Background(), node1.addr, "gossipgrid-1."+intSuffix)
	if err != nil {
		t.Fatalf("node0 -> node1 after rotation: %v", err)
	}
	if st2.NodeID != "gossipgrid-1" {
		t.Errorf("node1 status after rotation = %+v, want node gossipgrid-1", st2)
	}

	// (f) Rotate node1's trust bundle to include the untrusted CA: after the
	// rotation, a client signed by the untrusted CA must now be accepted.
	rcBeforeTrust := node1.mgr.Diagnostics().ReloadCount
	//nolint:gosec/G306 // test fixture in temp dir
	if err := os.WriteFile(node1.trustFile, trustBundlePEMTwo(sharedCA, untrustedCA), 0o600); err != nil {
		t.Fatalf("writing node1 trust bundle: %v", err)
	}
	waitForNodeReload(t, node1, rcBeforeTrust)
	if err := untrustedClientGet(t, node1, untrustedCA); err != nil {
		t.Fatalf("expected untrusted-CA client to be accepted after trust rotation, got: %v", err)
	}
}

// TestIntegrationTwoNodesMessageExchange demonstrates Milestone 4A:
// authenticated peer message exchange between two independent GossipGrid
// nodes with separate SQLite databases, separate identities, and a shared
// authorized workload CA.
//
// Demonstrates:
//  1. Node 0 creates a message through its public REST API
//     (POST /api/messages on the real public router).
//  2. The returned UUIDv7 and immutable fields are correct.
//  3. The message initially does not exist in node 1's database.
//  4. An explicit authenticated SendMessage transfers it to node 1.
//  5. The resulting database records are identical.
//  6. Repeated delivery does not create another row (idempotent).
//  7. A conflicting duplicate is rejected (409).
//  8. An unauthorized peer cannot submit messages.
//  9. Stopping one node's listener does not corrupt the other node's
//     database, which remains readable and writable.
//
// 10. Both nodes shut down cleanly; cleanup is idempotent (no double-close).
func TestIntegrationTwoNodesMessageExchange(t *testing.T) {
	// A shared workload CA so the two nodes trust each other.
	sharedCA := newTestCA(t, "workload")

	// Node 0: has a local store; creates the message through the public API.
	store0, closeStore0 := newIntegrationStore(t)
	node0 := newIntegrationNodeWithStore(t, sharedCA, "gossipgrid-0", store0, true)
	// Node 1: has a local store; receives the message.
	store1, closeStore1 := newIntegrationStore(t)
	node1 := newIntegrationNodeWithStore(t, sharedCA, "gossipgrid-1", store1, true)

	// (1) Node 0 creates a message through its public REST API
	// (POST /api/messages on the real public router, not a synthetic one).
	status, respBody, err := postPublicJSON(t, node0.publicAddr, `{"body":"Hello from gossipgrid-0"}`)
	if err != nil {
		t.Fatalf("POST /api/messages on node0: %v", err)
	}
	if status != http.StatusCreated {
		t.Fatalf("POST /api/messages status = %d, want %d; body: %s", status, http.StatusCreated, respBody)
	}
	var msg store.Message
	if err := json.Unmarshal([]byte(respBody), &msg); err != nil {
		t.Fatalf("decoding POST /api/messages response: %v", err)
	}
	if msg.ID == "" {
		t.Fatal("POST /api/messages returned an empty ID")
	}
	if msg.Body != "Hello from gossipgrid-0" {
		t.Errorf("POST /api/messages Body = %q, want %q", msg.Body, "Hello from gossipgrid-0")
	}
	if msg.Origin != "gossipgrid-0" {
		t.Errorf("POST /api/messages Origin = %q, want %q", msg.Origin, "gossipgrid-0")
	}
	if msg.CreatedAt.IsZero() {
		t.Error("POST /api/messages CreatedAt is zero")
	}

	// (2) The message now exists in node 0's database with the same ID.
	got0, err := store0.GetByID(msg.ID)
	if err != nil {
		t.Fatalf("store0.GetByID: %v", err)
	}
	if got0.ID != msg.ID || got0.Body != msg.Body || got0.Origin != msg.Origin {
		t.Errorf("node0 record = %+v, want id=%s body=%q origin=%q", got0, msg.ID, msg.Body, msg.Origin)
	}
	if !got0.CreatedAt.Equal(msg.CreatedAt) {
		t.Errorf("node0 CreatedAt = %v, want %v (must match the API response)", got0.CreatedAt, msg.CreatedAt)
	}

	// (3) The message initially does not exist in node 1's database.
	if _, err := store1.GetByID(msg.ID); err == nil {
		t.Fatal("message should not yet exist in node 1's store")
	}

	// (4) An explicit authenticated SendMessage transfers it to node 1.
	wireMsg := peertls.MessageWire{
		ID:        msg.ID,
		Body:      msg.Body,
		Origin:    msg.Origin,
		CreatedAt: msg.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	stored, err := node0.client.SendMessage(context.Background(), node1.addr, "gossipgrid-1."+intSuffix, wireMsg)
	if err != nil {
		t.Fatalf("node0 -> node1 SendMessage: %v", err)
	}

	// (5) The resulting database records are identical on both nodes.
	got1, err := store1.GetByID(msg.ID)
	if err != nil {
		t.Fatalf("store1.GetByID after SendMessage: %v", err)
	}
	if got1.ID != got0.ID {
		t.Errorf("node1 ID = %q, want %q (node0)", got1.ID, got0.ID)
	}
	if got1.Body != got0.Body {
		t.Errorf("node1 Body = %q, want %q (node0)", got1.Body, got0.Body)
	}
	if got1.Origin != got0.Origin {
		t.Errorf("node1 Origin = %q, want %q (node0)", got1.Origin, got0.Origin)
	}
	if !got1.CreatedAt.Equal(got0.CreatedAt) {
		t.Errorf("node1 CreatedAt = %v, want %v (node0)", got1.CreatedAt, got0.CreatedAt)
	}
	if stored.ID != msg.ID {
		t.Errorf("stored echo ID = %q, want %q", stored.ID, msg.ID)
	}

	// (6) Repeated delivery does not create another row (idempotent).
	for i := 0; i < 3; i++ {
		if _, err := node0.client.SendMessage(context.Background(), node1.addr, "gossipgrid-1."+intSuffix, wireMsg); err != nil {
			t.Fatalf("repeated delivery %d: %v", i, err)
		}
	}
	var count1 int64
	if err := store1.DB().Model(&store.Message{}).Count(&count1).Error; err != nil {
		t.Fatalf("count node1: %v", err)
	}
	if count1 != 1 {
		t.Errorf("node1 row count = %d, want 1 (idempotent)", count1)
	}

	// (7) A conflicting duplicate is rejected (409).
	conflict := wireMsg
	conflict.Body = "conflicting body"
	_, err = node0.client.SendMessage(context.Background(), node1.addr, "gossipgrid-1."+intSuffix, conflict)
	if err == nil {
		t.Fatal("expected 409 conflict, got success")
	}
	if !contains(err.Error(), "409") {
		t.Errorf("error should mention 409, got: %v", err)
	}
	// The original row is unaltered.
	got2, err := store1.GetByID(msg.ID)
	if err != nil {
		t.Fatalf("GetByID after conflict: %v", err)
	}
	if got2.Body != msg.Body {
		t.Errorf("original row altered: body = %q, want %q", got2.Body, msg.Body)
	}

	// (8) An unauthorized peer cannot submit messages.
	// A client signed by a different CA (not in node1's trust bundle) is
	// rejected at the mTLS handshake.
	untrustedCA := newTestCA(t, "unauthorized")
	if err := untrustedClientPost(t, node1, untrustedCA, wireMsg); err == nil {
		t.Fatal("expected handshake failure for untrusted-CA client, got success")
	}

	// (9) Stopping one node's listener does not corrupt the other node's
	// database. Stop node 1's public and peer listeners explicitly (the
	// shutdown functions are idempotent; t.Cleanup will not double-close).
	node1.publicShutdown()
	node1.peerShutdown()
	// Node 0 must remain operational: its public router still answers.
	status2, resp2, err := postPublicJSON(t, node0.publicAddr, `{"body":"still alive"}`)
	if err != nil {
		t.Fatalf("node0 POST after node1 stop: %v", err)
	}
	if status2 != http.StatusCreated {
		t.Fatalf("node0 POST after node1 stop: status = %d, want %d; body: %s", status2, http.StatusCreated, resp2)
	}
	// Node 0's local SQLite database remains readable and writable.
	_, err = store0.GetByID(msg.ID)
	if err != nil {
		t.Fatalf("node0 store unreadable after node1 stop: %v", err)
	}
	if _, err := store0.CreateLocalMessage("gossipgrid-0", "post-node1-stop"); err != nil {
		t.Fatalf("node0 store not writable after node1 stop: %v", err)
	}
	// Node 1's SQLite database (its file is intact even with the listener
	// stopped) is still readable: read the replicated message directly.
	reopened1, err := store1.GetByID(msg.ID)
	if err != nil {
		t.Fatalf("node1 store unreadable after stop: %v", err)
	}
	if reopened1.ID != msg.ID {
		t.Errorf("node1 record after stop = %q, want %q", reopened1.ID, msg.ID)
	}

	// (10) Both nodes shut down cleanly; cleanup is idempotent.
	// Stop node 0 explicitly, then call both shutdowns again to prove the
	// guards prevent double-close (no panic, no error).
	node0.publicShutdown()
	node0.peerShutdown()
	// Re-invoking shutdown must be safe (idempotent).
	node0.publicShutdown()
	node0.peerShutdown()
	node1.publicShutdown()
	node1.peerShutdown()

	// Close both stores explicitly. The returned close functions are
	// idempotent; t.Cleanup will not double-close them.
	if err := closeStore0(); err != nil {
		t.Fatalf("closeStore0: %v", err)
	}
	if err := closeStore1(); err != nil {
		t.Fatalf("closeStore1: %v", err)
	}
	// A second close must be a no-op (idempotent), not an error.
	if err := closeStore0(); err != nil {
		t.Errorf("second closeStore0 should be a no-op, got: %v", err)
	}
	if err := closeStore1(); err != nil {
		t.Errorf("second closeStore1 should be a no-op, got: %v", err)
	}
}

// untrustedClientPost performs an mTLS POST with a client cert signed by the
// given (untrusted) CA against the node's /internal/messages endpoint. It
// returns only the error; a non-nil error indicates a rejected handshake.
func untrustedClientPost(t *testing.T, node *integrationNode, ca *testCA, msg peertls.MessageWire) error {
	t.Helper()
	cert, key := issueLeafCert(t, ca, "gossipgrid-0."+intSuffix)
	pool := loadPoolFile(t, node.trustFile)
	body := `{"message":{"id":"` + msg.ID + `","body":"` + msg.Body + `","origin":"` + msg.Origin + `","created_at":"` + msg.CreatedAt + `"}}`
	tlsCfg := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{cert.Raw},
			PrivateKey:  key,
		}},
		ServerName: node.podID + "." + intSuffix,
	}
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
		Timeout:   3 * time.Second,
	}
	req, err := http.NewRequestWithContext(context.Background(), "POST", "https://"+node.addr+"/internal/messages", strings.NewReader(body))
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

// postPublicJSON performs an unauthenticated POST to the node's public
// /api/messages endpoint (the public router has no auth; only the peer
// listener requires mTLS). It returns the response status code and body.
func postPublicJSON(t *testing.T, addr, body string) (int, string, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), "POST", "http://"+addr+"/api/messages", bytes.NewReader([]byte(body)))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	data, rerr := io.ReadAll(io.LimitReader(resp.Body, 8192))
	if rerr != nil {
		return resp.StatusCode, "", rerr
	}
	return resp.StatusCode, string(data), nil
}

// contains reports whether s contains substr.
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || indexOfStr(s, substr) >= 0)
}

// indexOfStr returns the index of substr in s, or -1.
func indexOfStr(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// --- Test helpers ---

// rotateNodeCert replaces the node's leaf cert and key with a new pair for the
// same identity (simulating cert-manager rotation).
func rotateNodeCert(t *testing.T, node *integrationNode, ca *testCA) {
	t.Helper()
	cert, key := issueLeafCert(t, ca, node.podID+"."+intSuffix)
	chainPEM, keyPEM := certKeyPEMTwo(t, cert, key, ca)
	//nolint:gosec/G306 // test fixture in temp dir
	if err := os.WriteFile(node.certFile, chainPEM, 0o600); err != nil {
		t.Fatalf("writing rotated cert: %v", err)
	}
	//nolint:gosec/G306 // test fixture in temp dir
	if err := os.WriteFile(node.keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("writing rotated key: %v", err)
	}
}

// waitForNodeReload waits (bounded) for the node's Manager to complete a
// successful reload (ReloadCount increases beyond prevCount). prevCount MUST be
// captured BEFORE the credential material is modified, not inside this helper:
// the reload worker polls every 50ms, so a reload can complete before a
// post-write baseline is read, and the helper would then wait for a second
// reload that never occurs.
func waitForNodeReload(t *testing.T, node *integrationNode, prevCount int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if node.mgr.Diagnostics().ReloadCount > prevCount {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("node %s: expected a reload within 5s", node.podID)
}

// untrustedClientGet performs an mTLS GET with a client cert signed by the
// given (untrusted or, after rotation, trusted) CA against the node. It
// returns only the error; the response body is closed immediately.
func untrustedClientGet(t *testing.T, node *integrationNode, ca *testCA) error {
	t.Helper()
	cert, key := issueLeafCert(t, ca, "gossipgrid-0."+intSuffix)
	pool := loadPoolFile(t, node.trustFile)
	tlsCfg := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{cert.Raw},
			PrivateKey:  key,
		}},
		ServerName: node.podID + "." + intSuffix,
	}
	client := &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
		Timeout:   3 * time.Second,
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+node.addr+"/internal/status", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	// The close error is ignored: no useful recovery for a broken client
	// connection after the request.
	_ = resp.Body.Close()
	return nil
}

// loadPoolFile loads a CA trust pool from a PEM file path.
func loadPoolFile(t *testing.T, path string) *x509.CertPool {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading trust bundle %s: %v", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		t.Fatalf("trust bundle %s contains no CAs", path)
	}
	return pool
}

// --- Ephemeral test CA and leaf certificate helpers ---
//
// These are re-implemented here (rather than imported from the peertls
// package, whose test helpers are not exported) so the integration test can
// generate short-lived ECDSA P-256 CAs and leaf certificates in memory. No
// test private keys or CA material are committed to the repository.

// testCA is an ephemeral CA generated in memory.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

// newTestCA generates a fresh ECDSA P-256 CA valid from [now-1h, now+720h].
func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating CA key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: name + " Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(720 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("creating CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing CA cert: %v", err)
	}
	return &testCA{cert: cert, key: key, pem: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issueLeafCert signs a leaf certificate with the given CA. The leaf has both
// serverAuth and clientAuth EKUs (a GossipGrid pod cert used for both roles).
func issueLeafCert(t *testing.T, ca *testCA, dns string) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating leaf key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: dns},
		DNSNames:     []string{dns},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(720 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("creating leaf cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing leaf cert: %v", err)
	}
	return cert, key
}

// certKeyPEMTwo returns (certChainPEM, keyPEM) for a leaf cert and its key,
// where certChainPEM is the leaf followed by its issuer CA and keyPEM is the
// PKCS8 key.
func certKeyPEMTwo(t *testing.T, cert *x509.Certificate, key *ecdsa.PrivateKey, ca *testCA) (chainPEM, keyPEM []byte) {
	t.Helper()
	keyDer, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling PKCS8 key: %v", err)
	}
	chainPEM = append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})...)
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDer})
	return chainPEM, keyPEM
}

// trustBundlePEMTwo concatenates the PEM of two CAs (for trust-bundle
// rotation tests).
func trustBundlePEMTwo(ca1, ca2 *testCA) []byte {
	return append(ca1.pem, ca2.pem...)
}

// writePEMFile writes data to a file at dir/name and returns the absolute
// path.
func writePEMFile(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	//nolint:gosec/G306 // test fixtures are ephemeral, per-test temp dirs
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatalf("writing %s: %v", p, err)
	}
	return p
}

// --- Five-node gossip integration test (Milestone 4B) ---

const (
	fiveNodeSuffix      = "gossipgrid-headless.int5.svc"
	fiveNodeStatefulSet = "gossipgrid"
	fiveNodeReplicas    = 5
)

// fiveNode is a self-contained GossipGrid replica for the gossip integration
// test. Each node has its own SQLite DB, peer TLS manager, public HTTP
// listener, private mTLS listener, static discovery manager, and gossip engine.
type fiveNode struct {
	podID      string
	store      *store.Store
	mgr        *peertls.Manager
	client     *peertls.Client
	peerSrv    *peertls.PeerServer
	publicSrv  *server.Server
	publicAddr string
	peerAddr   string
	engine     *gossip.Engine
	discMgr    *discovery.Manager

	// peerListenerMu serializes stop/restart of the peer listener so a
	// concurrent Stop and Restart cannot double-close the listener.
	peerListenerMu sync.Mutex
	peerListener   net.Listener
	peerHTTP       *http.Server
	peerServing    bool

	// Shutdown functions (idempotent).
	peerShutdown   func()
	publicShutdown func()
}

// newFiveNodeBase builds and starts a single GossipGrid node with all
// components except the gossip engine (store, peer TLS, public HTTP, private
// mTLS listener). The peer server is created with a nil onNewMsg callback;
// the caller wires the gossip engine later and can re-create the peer server
// if needed. This allows all nodes to be created first (to learn their
// loopback addresses) before static discovery and gossip are configured.
func newFiveNodeBase(t *testing.T, ca *testCA, podID string) *fiveNode {
	t.Helper()
	dir := t.TempDir()

	// Credentials.
	cert, key := issueLeafCert(t, ca, podID+"."+fiveNodeSuffix)
	chainPEM, keyPEM := certKeyPEMTwo(t, cert, key, ca)
	certFile := writePEMFile(t, dir, "cert.pem", chainPEM)
	keyFile := writePEMFile(t, dir, "key.pem", keyPEM)
	trustFile := writePEMFile(t, dir, "ca-bundle.pem", ca.pem)

	// Store.
	st, err := store.New(filepath.Join(dir, "node.db"))
	if err != nil {
		t.Fatalf("%s: store.New: %v", podID, err)
	}
	t.Cleanup(func() { _ = st.Close() })

	// Peer TLS manager.
	mgr := peertls.New(peertls.Config{
		CertFile:         certFile,
		KeyFile:          keyFile,
		TrustBundleFile:  trustFile,
		IdentitySuffix:   fiveNodeSuffix,
		StatefulSetName:  fiveNodeStatefulSet,
		NodeID:           podID,
		ExpectedReplicas: fiveNodeReplicas,
		ReloadInterval:   50 * time.Millisecond,
	}, nil)
	ctx := context.Background()
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("%s: manager Start: %v", podID, err)
	}
	t.Cleanup(mgr.Shutdown)
	client := peertls.NewClient(mgr)

	// Peer server: the onNewMsg callback references node.engine through a
	// closure over the *fiveNode pointer; node.engine is set later by
	// wireGossipForNode before any messages are received.
	node := &fiveNode{
		podID: podID, store: st, mgr: mgr, client: client,
	}
	onNewMsg := func(msg *store.Message, senderPodID string) {
		if node.engine == nil {
			return
		}
		node.engine.Enqueue(gossip.MessageEvent{
			Message: peertls.MessageWire{
				ID: msg.ID, Body: msg.Body, Origin: msg.Origin,
				CreatedAt: msg.CreatedAt.UTC().Format(time.RFC3339Nano),
			},
			SenderPodID: senderPodID,
		})
	}
	peerSrv := peertls.NewPeerServerWithStoreAndCallback(mgr, ":0", podID, "v0.1.0-5n", st, true, onNewMsg, 0)
	tlsListener, err := tls.Listen("tcp", "127.0.0.1:0", mgr.TLSConfigForServer())
	if err != nil {
		t.Fatalf("%s: tls.Listen: %v", podID, err)
	}
	peerAddr := tlsListener.Addr().String()
	httpSrv := &http.Server{Handler: peerSrv.Handler()}
	go func() { _ = httpSrv.Serve(tlsListener) }()
	node.peerListener = tlsListener
	node.peerHTTP = httpSrv
	node.peerServing = true
	// Idempotent shutdown: only closes the listener if it is still the
	// current one (a restart may have replaced it).
	peerShutdown := func() {
		node.peerListenerMu.Lock()
		defer node.peerListenerMu.Unlock()
		if !node.peerServing {
			return
		}
		node.peerServing = false
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = node.peerHTTP.Shutdown(sctx)
		_ = node.peerListener.Close()
	}
	t.Cleanup(peerShutdown)

	// Public HTTP server: the onMessage callback references node.engine
	// through the same closure pattern as the peer server.
	onMessage := func(msg store.Message) {
		if node.engine == nil {
			return
		}
		node.engine.Enqueue(gossip.MessageEvent{
			Message: peertls.MessageWire{
				ID: msg.ID, Body: msg.Body, Origin: msg.Origin,
				CreatedAt: msg.CreatedAt.UTC().Format(time.RFC3339Nano),
			},
		})
	}
	publicSrv := server.NewServer(":0", server.NewWithStoreAndCallback(server.Node{ID: podID}, st, onMessage))
	lc := net.ListenConfig{}
	publicListener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("%s: net.Listen (public): %v", podID, err)
	}
	publicAddr := publicListener.Addr().String()
	go func() { _ = publicSrv.HTTPServer.Serve(publicListener) }()
	publicShutdown := func() {
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = publicSrv.Shutdown(sctx)
		_ = publicListener.Close()
	}
	t.Cleanup(publicShutdown)

	node.peerSrv = peerSrv
	node.publicSrv = publicSrv
	node.publicAddr = publicAddr
	node.peerAddr = peerAddr
	node.peerShutdown = peerShutdown
	node.publicShutdown = publicShutdown
	return node
}

// stopPeerListener stops the node's peer listener (simulating the replica
// becoming temporarily unavailable). It is idempotent: a second call is a
// no-op. The mTLS manager and store are NOT closed (only the listener), so
// the node can be restored by restartPeerListener.
func (n *fiveNode) stopPeerListener(t *testing.T) {
	t.Helper()
	n.peerListenerMu.Lock()
	defer n.peerListenerMu.Unlock()
	if !n.peerServing {
		return
	}
	n.peerServing = false
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = n.peerHTTP.Shutdown(sctx)
	_ = n.peerListener.Close()
	t.Logf("%s: peer listener stopped (temporarily unavailable)", n.podID)
}

// restartPeerListener restores the node's peer listener on the SAME loopback
// address (n.peerAddr) using the same mTLS config and handler. This simulates
// a temporarily unavailable replica being restored at a reachable, stable
// address within the retry budget. It is safe to call after stopPeerListener.
// The static discovery snapshot (and the AddressOverrides) still points at
// n.peerAddr, so other nodes' gossip engines reach the restored node at its
// original address.
func (n *fiveNode) restartPeerListener(t *testing.T) {
	t.Helper()
	n.peerListenerMu.Lock()
	defer n.peerListenerMu.Unlock()
	if n.peerServing {
		return
	}
	// Rebind the original loopback address (which was freed by
	// stopPeerListener) with the same mTLS config and the same peer handler
	// (which routes to the same store and onNewMsg callback).
	listener, err := tls.Listen("tcp", n.peerAddr, n.mgr.TLSConfigForServer())
	if err != nil {
		t.Fatalf("%s: re-listen on %s: %v", n.podID, n.peerAddr, err)
	}
	n.peerListener = listener
	httpSrv := &http.Server{Handler: n.peerSrv.Handler()}
	n.peerHTTP = httpSrv
	n.peerServing = true
	go func() { _ = httpSrv.Serve(listener) }()
	t.Logf("%s: peer listener restored on %s", n.podID, n.peerAddr)
}

// wireGossipForNode creates a static discovery manager and gossip engine for
// the given node, using the production bridge (discMgr.Snapshot() →
// gossip.PeerSnapshot, the same code as main.go). The static peers are the
// FQDN hostnames of the other nodes; AddressOverrides maps pod identities to
// the actual loopback dial addresses.
func wireGossipForNode(t *testing.T, node *fiveNode, allNodes []*fiveNode, gossipCfg gossip.Config) {
	t.Helper()
	ctx := context.Background()

	// Build static peer list: FQDN hostname + dummy port for each other node.
	// The port here is not used for dialing (AddressOverrides provides the
	// real loopback address); it only needs to be a valid port for config
	// validation.
	staticPeers := make([]string, 0, len(allNodes)-1)
	addrOverrides := make(map[string]string, len(allNodes)-1)
	for _, other := range allNodes {
		if other.podID == node.podID {
			continue
		}
		staticPeers = append(staticPeers, other.podID+"."+fiveNodeSuffix+":8443")
		addrOverrides[other.podID] = other.peerAddr
	}

	// Static discovery: deterministic, no network I/O, snapshot populated
	// synchronously when Start returns.
	discCfg := discovery.Config{
		Mode:             discovery.ModeStatic,
		StaticPeers:      staticPeers,
		StatefulSetName:  fiveNodeStatefulSet,
		NodeID:           node.podID,
		ExpectedReplicas: fiveNodeReplicas,
		DNSName:          fiveNodeSuffix,
	}
	discMgr := discovery.NewManager(discCfg, nil, nil)
	if err := discMgr.Start(ctx); err != nil {
		t.Fatalf("%s: discovery Start: %v", node.podID, err)
	}
	t.Cleanup(discMgr.Shutdown)

	// Gossip engine with the production bridge.
	selector := gossip.NewDefaultPeerSelector(fiveNodeSuffix, addrOverrides)
	delivery := gossip.NewRealDelivery(node.client)
	engine := gossip.New(gossipCfg, selector, delivery, nil)
	// Production bridge: discovery.Snapshot → gossip.PeerSnapshot
	// (identical to main.go).
	engine.SetSnapshotSource(func() gossip.PeerSnapshot {
		ds := discMgr.Snapshot()
		peers := make([]gossip.PeerInfo, len(ds.Peers))
		for i, p := range ds.Peers {
			peers[i] = gossip.PeerInfo{
				Hostname:    p.Hostname,
				PodIdentity: p.PodIdentity,
				Address:     p.Address,
				LastSeen:    p.LastSeen,
			}
		}
		var localNode *gossip.PeerInfo
		if ds.LocalNode != nil {
			localNode = &gossip.PeerInfo{
				Hostname:    ds.LocalNode.Hostname,
				PodIdentity: ds.LocalNode.PodIdentity,
				Address:     ds.LocalNode.Address,
				LastSeen:    ds.LocalNode.LastSeen,
			}
		}
		return gossip.PeerSnapshot{
			Peers:     peers,
			LocalNode: localNode,
			Stale:     ds.Stale,
			Mode:      string(ds.Mode),
		}
	})
	if err := engine.Start(ctx); err != nil {
		t.Fatalf("%s: gossip Start: %v", node.podID, err)
	}
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		engine.Stop(sctx)
	})

	node.engine = engine
	node.discMgr = discMgr
}

// TestIntegrationFiveNodesGossip demonstrates Milestone 4B: automatic
// epidemic gossip between five independent GossipGrid replicas with
// real authenticated mTLS, separate SQLite databases, static peer discovery
// through the production bridge, and the gossip engine's bounded fanout,
// retry, and duplicate-suppression behavior.
//
// Scenarios covered:
//  1. Healthy convergence: all five replicas online; a message posted to one
//     propagates to all 5 with identical immutable records.
//  2. One replica unavailable: node 4 is stopped while the remaining four
//     continue operating; the down node's attempts are exhausted.
//  3. Temporary unavailability + restoration: node 4 is restored on the same
//     loopback address within the retry budget; a real authenticated retry
//     succeeds and node 4 receives the message.
//  4. Multiple message origins + duplicate suppression: messages from
//     different origins propagate with no duplicate rows.
//  5. Permanent exhaustion: a permanently unavailable node exhausts its
//     configured attempt budget; no new retries occur after exhaustion.
//  6. Clean shutdown: all five engines, listeners, and SQLite stores shut
//     down cleanly; repeated Stop is safe; engine stats all read zero.
//
// The test uses real static discovery (discovery.Manager in static mode) and
// the production gossip bridge (discMgr.Snapshot() → gossip.PeerSnapshot).
// It does NOT claim that finite fanout guarantees convergence under
// arbitrary failures; it demonstrates behavior under controlled conditions
// with deterministic fanout selection. Real TLS handshakes are used for all
// network delivery (no fake transport calls).
func TestIntegrationFiveNodesGossip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping five-node gossip integration test in short mode")
	}

	podIDs := []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3", "gossipgrid-4"}
	sharedCA := newTestCA(t, "workload-5n")

	// Gossip configuration: fanout=4 contacts all eligible peers each round,
	// guaranteeing convergence in a healthy 5-node cluster. Fast retries for
	// test speed (RetryMax 200ms, MaxAttempts 3 → total per-peer retry budget
	// ≈ 0.5-1.5s, well within the restoration window).
	gossipCfg := gossip.Config{
		Fanout:        4,
		Rounds:        2,
		Workers:       4,
		QueueCapacity: 64,
		MaxAttempts:   3,
		RetryBase:     50 * time.Millisecond,
		RetryMax:      200 * time.Millisecond,
	}

	// Phase 1: create all 5 nodes (without gossip engines) to learn addresses.
	nodes := make([]*fiveNode, 5)
	for i, podID := range podIDs {
		nodes[i] = newFiveNodeBase(t, sharedCA, podID)
	}

	// Phase 2: wire static discovery + gossip engine for all 5 nodes, using
	// the production bridge. All 5 nodes start online.
	for i := 0; i < 5; i++ {
		wireGossipForNode(t, nodes[i], nodes, gossipCfg)
	}
	t.Log("5 nodes started with real mTLS, static discovery, and gossip engines")

	// Helper: wait until a condition is true or the deadline expires.
	waitUntil := func(timeout time.Duration, cond func() bool) bool {
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			if cond() {
				return true
			}
			time.Sleep(20 * time.Millisecond)
		}
		return cond()
	}

	// Helper: check if a node has a message.
	hasMsg := func(n *fiveNode, id string) bool {
		_, err := n.store.GetByID(id)
		return err == nil
	}

	// Helper: record a message's immutable fields from a node's store.
	recMsg := func(n *fiveNode, id string) store.Message {
		m, err := n.store.GetByID(id)
		if err != nil {
			t.Fatalf("node %s: GetByID(%s): %v", n.podID, id, err)
		}
		return *m
	}

	// --- Scenario 1: healthy convergence among all 5 nodes ---
	t.Run("healthy convergence on 5 nodes", func(t *testing.T) {
		// All 5 DBs start empty.
		for i := 0; i < 5; i++ {
			var count int64
			if err := nodes[i].store.DB().Model(&store.Message{}).Count(&count).Error; err != nil {
				t.Fatalf("node %d: count: %v", i, err)
			}
			if count != 0 {
				t.Fatalf("node %d: expected 0 messages, got %d", i, count)
			}
		}

		// POST a message to node 0.
		status, respBody, err := postPublicJSON(t, nodes[0].publicAddr, `{"body":"Hello five-node cluster"}`)
		if err != nil {
			t.Fatalf("POST to node 0: %v", err)
		}
		if status != http.StatusCreated {
			t.Fatalf("POST to node 0: status %d, want 201; body: %s", status, respBody)
		}
		var msg store.Message
		if err := json.Unmarshal([]byte(respBody), &msg); err != nil {
			t.Fatalf("decoding POST response: %v", err)
		}
		t.Logf("created message %s on node 0", msg.ID)

		// Wait for convergence to all 5 nodes.
		if !waitUntil(15*time.Second, func() bool {
			for i := 0; i < 5; i++ {
				if !hasMsg(nodes[i], msg.ID) {
					return false
				}
			}
			return true
		}) {
			for i := 0; i < 5; i++ {
				t.Errorf("node %d: message %s not present after 15s", i, msg.ID)
			}
			return
		}
		t.Log("message converged on all 5 nodes via static discovery bridge")

		// Verify identical immutable records on all 5 nodes.
		ref := recMsg(nodes[0], msg.ID)
		for i := 1; i < 5; i++ {
			m := recMsg(nodes[i], msg.ID)
			if m.ID != ref.ID || m.Body != ref.Body || m.Origin != ref.Origin {
				t.Errorf("node %d record = %+v, want id=%s body=%q origin=%q (identical to node 0)", i, m, ref.ID, ref.Body, ref.Origin)
			}
			if !m.CreatedAt.Equal(ref.CreatedAt) {
				t.Errorf("node %d CreatedAt = %v, want %v (identical to node 0)", i, m.CreatedAt, ref.CreatedAt)
			}
		}

		// Verify no duplicate rows (exactly 1 message per node).
		for i := 0; i < 5; i++ {
			var count int64
			if err := nodes[i].store.DB().Model(&store.Message{}).Count(&count).Error; err != nil {
				t.Fatalf("node %d: count: %v", i, err)
			}
			if count != 1 {
				t.Errorf("node %d: message count = %d, want 1 (no duplicate forwarding)", i, count)
			}
		}
	})

	// msg2 is defined at function scope so later scenarios (3, 4) can
	// reference it. It is assigned in scenario 2.
	var msg2 store.Message

	// --- Scenario 2: one replica unavailable; the other 4 continue ---
	t.Run("one replica unavailable, others continue", func(t *testing.T) {
		// Stop node 4's peer listener (temporarily unavailable). The node's
		// store and mTLS manager remain open, so it can be restored later.
		nodes[4].stopPeerListener(t)

		// POST a second message to node 2 (node 4 is now down).
		status, respBody, err := postPublicJSON(t, nodes[2].publicAddr, `{"body":"Second message"}`)
		if err != nil {
			t.Fatalf("POST to node 2: %v", err)
		}
		if status != http.StatusCreated {
			t.Fatalf("POST to node 2: status %d, want 201", status)
		}
		if err := json.Unmarshal([]byte(respBody), &msg2); err != nil {
			t.Fatalf("decoding POST response: %v", err)
		}

		// The 4 reachable nodes converge on the second message; node 4 is down.
		if !waitUntil(15*time.Second, func() bool {
			for i := 0; i < 4; i++ {
				if !hasMsg(nodes[i], msg2.ID) {
					return false
				}
			}
			return true
		}) {
			for i := 0; i < 4; i++ {
				t.Errorf("node %d: second message not present after 15s", i)
			}
		} else {
			t.Log("second message converged on all 4 reachable nodes")
		}

		// Verify stored messages are intact on reachable nodes (exactly 2
		// messages each: the scenario-1 message + msg2 from scenario 2).
		for i := 0; i < 4; i++ {
			var count int64
			if err := nodes[i].store.DB().Model(&store.Message{}).Count(&count).Error; err != nil {
				t.Fatalf("node %d: count: %v", i, err)
			}
			if count != 2 {
				t.Errorf("node %d: message count = %d, want 2 (exactly 2 messages)", i, count)
			}
		}

	})

	// --- Scenario 3: temporary unavailability + restoration; a real retry succeeds ---
	t.Run("temporary unavailability then restoration", func(t *testing.T) {
		// Node 4 is currently down (stopped in scenario 2). msg2 was not
		// delivered to node 4. Restore node 4 on the same loopback address
		// within the retry budget.
		nodes[4].restartPeerListener(t)

		// Wait for node 4 to receive msg2 via a real authenticated retry from
		// a peer. The 4 reachable nodes each have msg2 and retry toward node 4;
		// with MaxAttempts=3 and RetryBase=50ms/RetryMax=200ms the per-peer
		// retry budget is ~0.5-1.5s. The restoration must land within that
		// window for a retry to succeed.
		if !waitUntil(15*time.Second, func() bool {
			return hasMsg(nodes[4], msg2.ID)
		}) {
			t.Fatal("node 4 did not receive msg2 after restoration (no successful retry within the retry budget)")
		}
		t.Log("node 4 received msg2 after restoration via a real authenticated retry")

		// Verify the restored record is identical to the other nodes.
		ref := recMsg(nodes[0], msg2.ID)
		m4 := recMsg(nodes[4], msg2.ID)
		if m4.ID != ref.ID || m4.Body != ref.Body || m4.Origin != ref.Origin {
			t.Errorf("node 4 restored record = %+v, want identical to node 0", m4)
		}
		if !m4.CreatedAt.Equal(ref.CreatedAt) {
			t.Errorf("node 4 restored CreatedAt = %v, want %v (identical to node 0)", m4.CreatedAt, ref.CreatedAt)
		}
	})

	// --- Scenario 4: multiple origins + duplicate suppression ---
	t.Run("multiple origins and duplicate suppression", func(t *testing.T) {
		// All 5 nodes are now back online (node 4 restored in scenario 3).
		// POST a third message to node 1 (a different origin than scenarios 1-2).
		status, respBody, err := postPublicJSON(t, nodes[1].publicAddr, `{"body":"Third message from node 1"}`)
		if err != nil {
			t.Fatalf("POST to node 1: %v", err)
		}
		if status != http.StatusCreated {
			t.Fatalf("POST to node 1: status %d, want 201", status)
		}
		var msg3 store.Message
		if err := json.Unmarshal([]byte(respBody), &msg3); err != nil {
			t.Fatalf("decoding POST response: %v", err)
		}

		// Wait for all 5 nodes to converge on the third message.
		if !waitUntil(15*time.Second, func() bool {
			for i := 0; i < 5; i++ {
				if !hasMsg(nodes[i], msg3.ID) {
					return false
				}
			}
			return true
		}) {
			for i := 0; i < 5; i++ {
				t.Errorf("node %d: third message not present after 15s", i)
			}
		} else {
			t.Log("third message converged on all 5 nodes")
		}

		// Verify no duplicate rows: each node has exactly 3 distinct messages
		// (the 3 original messages, one per origin).
		for i := 0; i < 5; i++ {
			var count int64
			if err := nodes[i].store.DB().Model(&store.Message{}).Count(&count).Error; err != nil {
				t.Fatalf("node %d: count: %v", i, err)
			}
			if count != 3 {
				t.Errorf("node %d: message count = %d, want 3 (no duplicate forwarding; 3 distinct messages)", i, count)
			}
		}
		t.Log("duplicate suppression verified: exactly 3 messages per node across 3 origins")
	})

	// --- Scenario 5: permanent exhaustion with no new retries after budget ---
	t.Run("permanent exhaustion with no new retries", func(t *testing.T) {
		// Stop node 4 again (permanently unavailable for this scenario).
		nodes[4].stopPeerListener(t)

		// POST a fourth message to node 3 (node 4 is down).
		status, respBody, err := postPublicJSON(t, nodes[3].publicAddr, `{"body":"Fourth message while node 4 permanently down"}`)
		if err != nil {
			t.Fatalf("POST to node 3: %v", err)
		}
		if status != http.StatusCreated {
			t.Fatalf("POST to node 3: status %d, want 201", status)
		}
		var msg4 store.Message
		if err := json.Unmarshal([]byte(respBody), &msg4); err != nil {
			t.Fatalf("decoding POST response: %v", err)
		}

		// The 4 reachable nodes converge on the fourth message.
		if !waitUntil(15*time.Second, func() bool {
			for i := 0; i < 4; i++ {
				if !hasMsg(nodes[i], msg4.ID) {
					return false
				}
			}
			return true
		}) {
			for i := 0; i < 4; i++ {
				t.Errorf("node %d: fourth message not present after 15s", i)
			}
		} else {
			t.Log("fourth message converged on all 4 reachable nodes")
		}

		// Wait for the per-peer retry budget to fully exhaust. With
		// MaxAttempts=3, RetryBase=50ms, RetryMax=200ms the total per-peer
		// retry window is roughly 0.5-1.5s. Sleep 3s to be safely past it.
		time.Sleep(3 * time.Second)

		// Record RetryAttempts for each reachable node, then sleep and
		// re-check: no new retries should occur after exhaustion.
		before := make([]uint64, 5)
		for i := 0; i < 4; i++ {
			before[i] = nodes[i].engine.Stats().RetryAttempts
		}
		time.Sleep(1 * time.Second)
		for i := 0; i < 4; i++ {
			after := nodes[i].engine.Stats().RetryAttempts
			if after > before[i] {
				t.Errorf("node %d: RetryAttempts increased after exhaustion: before=%d after=%d", i, before[i], after)
			}
		}
		t.Log("permanent exhaustion: no new retries after the attempt budget is exhausted")

		// Restore node 4 so scenario 6 can shut down all 5 nodes cleanly.
		nodes[4].restartPeerListener(t)
	})

	// --- Scenario 6: clean shutdown ---
	t.Run("clean shutdown", func(t *testing.T) {
		// Stop all 5 gossip engines.
		for i := 0; i < 5; i++ {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			nodes[i].engine.Stop(sctx)
			cancel()
			t.Logf("node %d gossip engine stopped", i)
		}

		// Shut down peer and public listeners on all 5 nodes.
		for i := 0; i < 5; i++ {
			nodes[i].peerShutdown()
			nodes[i].publicShutdown()
			t.Logf("node %d listeners shut down", i)
		}

		// Verify repeated Stop is safe (idempotent) — no panics.
		for i := 0; i < 5; i++ {
			sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			nodes[i].engine.Stop(sctx)
			cancel()
		}

		// Verify engine stats are all zero after a completed shutdown
		// (queued, pending, and delayed work all released).
		for i := 0; i < 5; i++ {
			s := nodes[i].engine.Stats()
			if s.QueueDepth != 0 || s.PendingWork != 0 || s.DelayedRetries != 0 {
				t.Errorf("node %d: after shutdown stats = %+v, want all zero", i, s)
			}
		}
		t.Log("all 5 nodes shut down cleanly; repeated Stop is safe; engine stats all zero")
	})
}
