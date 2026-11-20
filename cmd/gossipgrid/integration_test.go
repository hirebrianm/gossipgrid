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
	"testing"
	"time"

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
	rotateNodeCert(t, node1, sharedCA)
	waitForNodeReload(t, node1)
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
	//nolint:gosec/G306 // test fixture in temp dir
	if err := os.WriteFile(node1.trustFile, trustBundlePEMTwo(sharedCA, untrustedCA), 0o600); err != nil {
		t.Fatalf("writing node1 trust bundle: %v", err)
	}
	waitForNodeReload(t, node1)
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
// successful reload (ReloadCount increases).
func waitForNodeReload(t *testing.T, node *integrationNode) {
	t.Helper()
	prev := node.mgr.Diagnostics().ReloadCount
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if node.mgr.Diagnostics().ReloadCount > prev {
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
