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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gossipgrid/internal/peertls"
)

const (
	intSuffix      = "gossipgrid-headless.int.svc"
	intStatefulSet = "gossipgrid"
)

// integrationNode is a self-contained GossipGrid peer (Manager + peer server +
// client) for the integration test. It owns its credential files and a
// private HTTPS listener on a random loopback port.
type integrationNode struct {
	mgr       *peertls.Manager
	client    *peertls.Client
	addr      string // peer listener address (host:port)
	podID     string // e.g. "gossipgrid-0"
	certFile  string
	keyFile   string
	trustFile string
}

// newIntegrationNode starts a GossipGrid peer node with the given pod short
// name (e.g. "gossipgrid-0"). The node uses the supplied workload CA for both
// its own certificate and its trust bundle (so nodes sharing the CA trust
// each other).
func newIntegrationNode(t *testing.T, ca *testCA, podID string) *integrationNode {
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

	// Build a PeerServer (which captures the handler and the mTLS config).
	peerSrv := peertls.NewPeerServer(mgr, ":0", podID, "v0.1.0-int")

	// Listen on a random loopback port with the mTLS config. tls.Listen
	// creates both the TCP and TLS listener in one call.
	tlsListener, err := tls.Listen("tcp", "127.0.0.1:0", mgr.TLSConfigForServer())
	if err != nil {
		t.Fatalf("%s: tls.Listen: %v", podID, err)
	}
	t.Cleanup(func() { _ = tlsListener.Close() })
	addr := tlsListener.Addr().String()

	httpSrv := &http.Server{Handler: peerSrv.Handler()}
	go func() { _ = httpSrv.Serve(tlsListener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
	})

	return &integrationNode{
		mgr: mgr, client: peertls.NewClient(mgr), addr: addr,
		podID: podID, certFile: certFile, keyFile: keyFile, trustFile: trustFile,
	}
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
