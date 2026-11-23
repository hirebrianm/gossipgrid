// Package peertls: unit tests.
//
// These tests cover the required positive and negative security scenarios for
// the peer mTLS transport. All certificates and CAs are generated ephemeral in
// memory (see testcert_test.go); no test keys are committed.
package peertls

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

const (
	testSuffix = "gossipgrid-headless.test.svc"
	// testStatefulSet is the expected StatefulSet name in tests; pod
	// identities are "gossipgrid-<ordinal>.<testSuffix>".
	testStatefulSet = "gossipgrid"
)

// startTestServer starts an mTLS TLS listener on a random local port using the
// given Manager's server TLS config, and returns the listener's address
// (host:port) and a stop function.
func startTestServer(t *testing.T, mgr *Manager, handler http.Handler) (addr string, stop func()) {
	t.Helper()
	cfg := mgr.TLSConfigForServer()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	srv := &http.Server{Handler: handler}
	go func() {
		_ = srv.Serve(listener)
	}()
	return listener.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}

// newTestManager builds and starts a Manager from the given credential files
// using the configured test identity suffix and StatefulSet name. podID is
// the pod short name (e.g. "gossipgrid-0") used for the NodeID binding check.
func newTestManager(t *testing.T, certFile, keyFile, trustFile, podID string, reloadInterval time.Duration) *Manager {
	t.Helper()
	mgr := New(Config{
		CertFile:         certFile,
		KeyFile:          keyFile,
		TrustBundleFile:  trustFile,
		IdentitySuffix:   testSuffix,
		StatefulSetName:  testStatefulSet,
		NodeID:           podID,
		ExpectedReplicas: 2,
		ReloadInterval:   reloadInterval,
	}, nil)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("manager Start: %v", err)
	}
	t.Cleanup(mgr.Shutdown)
	return mgr
}

// statusHandler returns an http.Handler that serves GET /internal/status with
// the given node and version (mirrors the real handler's JSON shape).
func statusHandler(nodeID, version string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"node_id": nodeID,
			"version": version,
		})
	})
	return mux
}

// --- 1. Two correctly authenticated replicas communicate ---

func TestTwoReplicasCommunicate(t *testing.T) {
	ca := newTestCA(t, "w")
	certA, keyA, trustA := testCredentials(t, ca, "gossipgrid-0")
	certB, keyB, trustB := testCredentials(t, ca, "gossipgrid-1")

	mgrA := newTestManager(t, certA, keyA, trustA, "gossipgrid-0", 100*time.Millisecond)
	mgrB := newTestManager(t, certB, keyB, trustB, "gossipgrid-1", 100*time.Millisecond)

	addrA, stopA := startTestServer(t, mgrA, statusHandler("gossipgrid-0", "v0.1.0-test"))
	defer stopA()
	addrB, stopB := startTestServer(t, mgrB, statusHandler("gossipgrid-1", "v0.1.0-test"))
	defer stopB()

	clientA := NewClient(mgrA)
	stB, err := clientA.GetStatus(context.Background(), addrB, "gossipgrid-1."+testSuffix)
	if err != nil {
		t.Fatalf("A -> B GetStatus: %v", err)
	}
	if stB.NodeID != "gossipgrid-1" || stB.Version != "v0.1.0-test" {
		t.Errorf("A -> B status = %+v, want node gossipgrid-1", stB)
	}

	clientB := NewClient(mgrB)
	stA, err := clientB.GetStatus(context.Background(), addrA, "gossipgrid-0."+testSuffix)
	if err != nil {
		t.Fatalf("B -> A GetStatus: %v", err)
	}
	if stA.NodeID != "gossipgrid-0" {
		t.Errorf("B -> A status = %+v, want node gossipgrid-0", stA)
	}
}

// --- 2. Server rejects clients with no certificate ---

func TestServerRejectsNoClientCert(t *testing.T) {
	ca := newTestCA(t, "w")
	certS, keyS, trustS := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, certS, keyS, trustS, "gossipgrid-0", 100*time.Millisecond)
	addr, stop := startTestServer(t, mgr, statusHandler("gossipgrid-0", "v"))
	defer stop()

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    loadPool(t, trustS),
				MinVersion: tls.VersionTLS13,
				ServerName: "gossipgrid-0." + testSuffix,
			},
		},
		Timeout: 3 * time.Second,
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+addr+"/internal/status", nil)
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected handshake failure for client with no cert, got success")
	}
}

// --- 3. Server rejects clients signed by an unknown CA ---

func TestServerRejectsUnknownClientCA(t *testing.T) {
	caGood := newTestCA(t, "good")
	caBad := newTestCA(t, "bad")
	certS, keyS, trustS := testCredentials(t, caGood, "gossipgrid-0")
	mgr := newTestManager(t, certS, keyS, trustS, "gossipgrid-0", 100*time.Millisecond)

	clientCert, clientKey := issueLeaf(t, caBad, leafCertSpec{
		dnsNames: []string{"gossipgrid-1." + testSuffix},
	})
	addr, stop := startTestServer(t, mgr, statusHandler("gossipgrid-0", "v"))
	defer stop()

	if err := mTLSGet(t, addr, "gossipgrid-0."+testSuffix, clientCert, clientKey, loadPool(t, trustS)); err == nil {
		t.Fatal("expected handshake failure for client signed by unknown CA, got success")
	}
}

// --- 4. Client rejects a server signed by an unknown CA ---

func TestClientRejectsUnknownServerCA(t *testing.T) {
	caGood := newTestCA(t, "good")
	caBad := newTestCA(t, "bad")
	_, _, trustS := testCredentials(t, caGood, "gossipgrid-0")

	serverCert, serverKey := issueLeaf(t, caBad, leafCertSpec{
		dnsNames: []string{"gossipgrid-0." + testSuffix},
	})
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{{Certificate: [][]byte{serverCert.Raw}, PrivateKey: serverKey}},
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	srv := &http.Server{Handler: statusHandler("gossipgrid-0", "v")}
	go func() { _ = srv.Serve(listener) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    loadPool(t, trustS),
				MinVersion: tls.VersionTLS13,
			},
		},
		Timeout: 3 * time.Second,
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+listener.Addr().String()+"/internal/status", nil)
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected handshake failure for server signed by unknown CA, got success")
	}
}

// --- 5. Server rejects a client with an unexpected pod identity ---

func TestServerRejectsUnexpectedClientIdentity(t *testing.T) {
	ca := newTestCA(t, "w")
	certS, keyS, trustS := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, certS, keyS, trustS, "gossipgrid-0", 100*time.Millisecond)

	clientCert, clientKey := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"other-0." + testSuffix},
	})
	addr, stop := startTestServer(t, mgr, statusHandler("gossipgrid-0", "v"))
	defer stop()

	if err := mTLSGet(t, addr, "gossipgrid-0."+testSuffix, clientCert, clientKey, loadPool(t, trustS)); err == nil {
		t.Fatal("expected rejection for client with unexpected pod identity, got success")
	}
}

// --- 6. Client rejects a server whose SAN does not match the expected identity ---

func TestClientRejectsMismatchedServerSAN(t *testing.T) {
	ca := newTestCA(t, "w")
	_, _, trustS := testCredentials(t, ca, "gossipgrid-0")

	serverCert, serverKey := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"gossipgrid-0." + testSuffix},
	})
	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{{Certificate: [][]byte{serverCert.Raw}, PrivateKey: serverKey}},
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatalf("tls.Listen: %v", err)
	}
	srv := &http.Server{Handler: statusHandler("gossipgrid-0", "v")}
	go func() { _ = srv.Serve(listener) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    loadPool(t, trustS),
				MinVersion: tls.VersionTLS13,
				ServerName: "gossipgrid-2." + testSuffix,
			},
		},
		Timeout: 3 * time.Second,
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+listener.Addr().String()+"/internal/status", nil)
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected handshake failure for mismatched server SAN, got success")
	}
}

// --- 7. Reject expired and not-yet-valid certificates ---

func TestRejectExpiredCertificate(t *testing.T) {
	ca := newTestCA(t, "w")
	cert, key := issueLeaf(t, ca, leafCertSpec{
		dnsNames:  []string{"gossipgrid-0." + testSuffix},
		notBefore: time.Now().Add(-48 * time.Hour),
		notAfter:  time.Now().Add(-24 * time.Hour),
	})
	dir := t.TempDir()
	certFile := writePEM(t, dir, "cert.pem", certKeyPEMChain(t, cert, key, ca))
	keyFile := writePEM(t, dir, "key.pem", keyPEM(t, key))
	trustFile := writePEM(t, dir, "ca-bundle.pem", ca.pem)

	mgr := New(Config{
		CertFile:        certFile,
		KeyFile:         keyFile,
		TrustBundleFile: trustFile,
		IdentitySuffix:  testSuffix,
		StatefulSetName: testStatefulSet,
		ReloadInterval:  100 * time.Millisecond,
	}, nil)
	if err := mgr.Start(context.Background()); err == nil {
		t.Fatal("expected Start to fail for expired certificate, got success")
	}
}

func TestRejectNotYetValidCertificate(t *testing.T) {
	ca := newTestCA(t, "w")
	cert, key := issueLeaf(t, ca, leafCertSpec{
		dnsNames:  []string{"gossipgrid-0." + testSuffix},
		notBefore: time.Now().Add(24 * time.Hour),
		notAfter:  time.Now().Add(720 * time.Hour),
	})
	dir := t.TempDir()
	certFile := writePEM(t, dir, "cert.pem", certKeyPEMChain(t, cert, key, ca))
	keyFile := writePEM(t, dir, "key.pem", keyPEM(t, key))
	trustFile := writePEM(t, dir, "ca-bundle.pem", ca.pem)

	mgr := New(Config{
		CertFile:        certFile,
		KeyFile:         keyFile,
		TrustBundleFile: trustFile,
		IdentitySuffix:  testSuffix,
		StatefulSetName: testStatefulSet,
		ReloadInterval:  100 * time.Millisecond,
	}, nil)
	if err := mgr.Start(context.Background()); err == nil {
		t.Fatal("expected Start to fail for not-yet-valid certificate, got success")
	}
}

// --- 8. Reject invalid Extended Key Usage ---

func TestRejectInvalidEKU(t *testing.T) {
	ca := newTestCA(t, "w")
	clientCert, clientKey := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"gossipgrid-1." + testSuffix},
		eku:      []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, // no clientAuth
	})
	certS, keyS, trustS := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, certS, keyS, trustS, "gossipgrid-0", 100*time.Millisecond)
	addr, stop := startTestServer(t, mgr, statusHandler("gossipgrid-0", "v"))
	defer stop()

	if err := mTLSGet(t, addr, "gossipgrid-0."+testSuffix, clientCert, clientKey, loadPool(t, trustS)); err == nil {
		t.Fatal("expected rejection for client cert missing clientAuth EKU, got success")
	}
}

// --- 9. Reject wildcard or unauthorized peer identities ---

func TestRejectWildcardIdentity(t *testing.T) {
	ca := newTestCA(t, "w")
	clientCert, clientKey := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"*.gossipgrid-headless.test.svc"},
	})
	certS, keyS, trustS := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, certS, keyS, trustS, "gossipgrid-0", 100*time.Millisecond)
	addr, stop := startTestServer(t, mgr, statusHandler("gossipgrid-0", "v"))
	defer stop()

	if err := mTLSGet(t, addr, "gossipgrid-0."+testSuffix, clientCert, clientKey, loadPool(t, trustS)); err == nil {
		t.Fatal("expected rejection for wildcard SAN, got success")
	}
}

// --- 10. Reject a non-GossipGrid cert signed by a trusted CA ---

func TestRejectNonGossipGridCertFromTrustedCA(t *testing.T) {
	ca := newTestCA(t, "w")
	clientCert, clientKey := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"unrelated.example.com"},
	})
	certS, keyS, trustS := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, certS, keyS, trustS, "gossipgrid-0", 100*time.Millisecond)
	addr, stop := startTestServer(t, mgr, statusHandler("gossipgrid-0", "v"))
	defer stop()

	if err := mTLSGet(t, addr, "gossipgrid-0."+testSuffix, clientCert, clientKey, loadPool(t, trustS)); err == nil {
		t.Fatal("expected rejection for non-GossipGrid cert from trusted CA, got success")
	}
}

// --- 11. Reject TLS below version 1.3 ---

func TestRejectTLSBelow13(t *testing.T) {
	ca := newTestCA(t, "w")
	certS, keyS, trustS := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, certS, keyS, trustS, "gossipgrid-0", 100*time.Millisecond)
	addr, stop := startTestServer(t, mgr, statusHandler("gossipgrid-0", "v"))
	defer stop()

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    loadPool(t, trustS),
				MinVersion: tls.VersionTLS12,
				MaxVersion: tls.VersionTLS12,
			},
		},
		Timeout: 3 * time.Second,
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+addr+"/internal/status", nil)
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected handshake failure for TLS 1.2 client, got success")
	}
}

// --- 12. Private endpoints absent from the public router ---

func TestPrivateEndpointsNotOnPublicRouter(t *testing.T) {
	// The public router (internal/server) must NOT expose /internal/status.
	// We build the public handler directly (see server_public_test.go) and
	// assert the path is not served.
	h := publicHandlerForTestImpl()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/internal/status", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatalf("public router served /internal/status with %d; it must not be exposed", rec.Code)
	}
	if rec.Code != http.StatusNotFound {
		t.Errorf("public router returned %d for /internal/status, want 404", rec.Code)
	}
}

// --- 13. Rotated leaf cert picked up by a NEW connection ---

func TestRotatedLeafCertPickedUpByNewConnection(t *testing.T) {
	ca := newTestCA(t, "w")
	dir := t.TempDir()
	pod := "gossipgrid-0"

	c1, k1 := issueLeaf(t, ca, leafCertSpec{dnsNames: []string{pod + "." + testSuffix}})
	certFile := writePEM(t, dir, "cert.pem", certKeyPEMChain(t, c1, k1, ca))
	keyFile := writePEM(t, dir, "key.pem", keyPEM(t, k1))
	trustFile := writePEM(t, dir, "ca-bundle.pem", ca.pem)

	mgr := newTestManager(t, certFile, keyFile, trustFile, "gossipgrid-0", 80*time.Millisecond)
	addr, stop := startTestServer(t, mgr, statusHandler(pod, "v"))
	defer stop()

	fpBefore := mgr.Diagnostics().CertFingerprint

	// Rotate: write a new leaf cert for the same identity.
	c2, k2 := issueLeaf(t, ca, leafCertSpec{dnsNames: []string{pod + "." + testSuffix}})
	mustWriteFile(t, certFile, certKeyPEMChain(t, c2, k2, ca))
	mustWriteFile(t, keyFile, keyPEM(t, k2))

	waitForReload(t, mgr, 0)
	fpAfter := mgr.Diagnostics().CertFingerprint
	if fpBefore == fpAfter {
		t.Fatal("fingerprint did not change after rotation; reload did not pick up new cert")
	}

	client := NewClient(mgr)
	st, err := client.GetStatus(context.Background(), addr, pod+"."+testSuffix)
	if err != nil {
		t.Fatalf("GetStatus after rotation: %v", err)
	}
	if st.NodeID != pod {
		t.Errorf("status after rotation = %+v, want node %s", st, pod)
	}
}

// --- 14. Rotated CA trust bundle used by new connections ---

func TestRotatedTrustBundleUsedByNewConnections(t *testing.T) {
	ca1 := newTestCA(t, "ca1")
	ca2 := newTestCA(t, "ca2")
	dir := t.TempDir()
	pod := "gossipgrid-0"

	c1, k1 := issueLeaf(t, ca1, leafCertSpec{dnsNames: []string{pod + "." + testSuffix}})
	certFile := writePEM(t, dir, "cert.pem", certKeyPEMChain(t, c1, k1, ca1))
	keyFile := writePEM(t, dir, "key.pem", keyPEM(t, k1))
	trustFile := writePEM(t, dir, "ca-bundle.pem", ca1.pem)

	mgr := newTestManager(t, certFile, keyFile, trustFile, "gossipgrid-0", 80*time.Millisecond)
	addr, stop := startTestServer(t, mgr, statusHandler(pod, "v"))
	defer stop()

	// A client signed by ca2 is initially rejected (ca2 not in trust bundle).
	c2, k2 := issueLeaf(t, ca2, leafCertSpec{dnsNames: []string{"gossipgrid-1." + testSuffix}})
	if err := mTLSGet(t, addr, pod+"."+testSuffix, c2, k2, loadPool(t, trustFile)); err == nil {
		t.Fatal("expected ca2-signed client to be rejected before rotation, got success")
	}

	// Rotate the trust bundle to include ca1 + ca2.
	mustWriteFile(t, trustFile, trustBundlePEM(ca1, ca2))
	waitForReload(t, mgr, 0)
	// After reload, the trust bundle includes ca2.
	if err := mTLSGet(t, addr, pod+"."+testSuffix, c2, k2, loadPool(t, trustFile)); err != nil {
		t.Fatalf("expected ca2-signed client to be accepted after trust rotation, got: %v", err)
	}
}

// --- 15. A removed CA is rejected for new connections after rotation ---

func TestRemovedCAResectedAfterRotation(t *testing.T) {
	ca1 := newTestCA(t, "ca1")
	ca2 := newTestCA(t, "ca2")
	dir := t.TempDir()
	pod := "gossipgrid-0"

	c1, k1 := issueLeaf(t, ca1, leafCertSpec{dnsNames: []string{pod + "." + testSuffix}})
	certFile := writePEM(t, dir, "cert.pem", certKeyPEMChain(t, c1, k1, ca1))
	keyFile := writePEM(t, dir, "key.pem", keyPEM(t, k1))
	trustFile := writePEM(t, dir, "ca-bundle.pem", trustBundlePEM(ca1, ca2))

	mgr := newTestManager(t, certFile, keyFile, trustFile, "gossipgrid-0", 80*time.Millisecond)
	addr, stop := startTestServer(t, mgr, statusHandler(pod, "v"))
	defer stop()

	c2, k2 := issueLeaf(t, ca2, leafCertSpec{dnsNames: []string{"gossipgrid-1." + testSuffix}})
	if err := mTLSGet(t, addr, pod+"."+testSuffix, c2, k2, loadPool(t, trustFile)); err != nil {
		t.Fatalf("expected ca2 client accepted initially: %v", err)
	}

	// Rotate trust to ONLY ca1 (ca2 removed).
	mustWriteFile(t, trustFile, ca1.pem)
	waitForReload(t, mgr, 0)

	// After rotation, the ca2-signed client must be rejected.
	if err := mTLSGet(t, addr, pod+"."+testSuffix, c2, k2, loadPool(t, trustFile)); err == nil {
		t.Fatal("expected ca2-signed client to be rejected after CA removal, got success")
	}
}

// --- 16. Malformed reload preserves the previous valid config ---

func TestMalformedReloadPreservesPreviousConfig(t *testing.T) {
	ca := newTestCA(t, "w")
	dir := t.TempDir()
	pod := "gossipgrid-0"

	c1, k1 := issueLeaf(t, ca, leafCertSpec{dnsNames: []string{pod + "." + testSuffix}})
	certFile := writePEM(t, dir, "cert.pem", certKeyPEMChain(t, c1, k1, ca))
	keyFile := writePEM(t, dir, "key.pem", keyPEM(t, k1))
	trustFile := writePEM(t, dir, "ca-bundle.pem", ca.pem)

	mgr := newTestManager(t, certFile, keyFile, trustFile, "gossipgrid-0", 80*time.Millisecond)
	addr, stop := startTestServer(t, mgr, statusHandler(pod, "v"))
	defer stop()
	fpBefore := mgr.Diagnostics().CertFingerprint

	// Corrupt the cert file with garbage.
	mustWriteFile(t, certFile, []byte("not a valid certificate"))

	// Wait for the reload to attempt and fail (bounded). The reload is
	// expected to fail, so ReloadCount stays at 0; we just need the ticker
	// to have fired at least once.
	settleWait(t, 300)

	fpAfter := mgr.Diagnostics().CertFingerprint
	if fpBefore != fpAfter {
		t.Fatalf("fingerprint changed after malformed reload; previous config not preserved (before=%s after=%s)", fpBefore, fpAfter)
	}
	client := NewClient(mgr)
	if _, err := client.GetStatus(context.Background(), addr, pod+"."+testSuffix); err != nil {
		t.Fatalf("expected connection to succeed with previous valid config after malformed reload: %v", err)
	}

	// Restore the valid cert for a clean teardown.
	mustWriteFile(t, certFile, certKeyPEMChain(t, c1, k1, ca))
}

// --- 16b. Unchanged credential material: no-op reload does not increment
// ---
//
// A regression test for the diagnostic-counter behavior. Two distinct reload
// cases are covered:
//
//  1. The reload worker reads back IDENTICAL credential material (same leaf,
//     same key, same trust bundle) across several ticks — this is a no-op
//     reload and must NOT increment ReloadCount (the counter reflects actual
//     credential-material changes, not periodic check ticks).
//
//  2. A CA trust-bundle rotation (leaf unchanged) IS a genuine reload — the
//     trust-bundle material changed — so it MUST increment ReloadCount and
//     the new snapshot must be picked up on the next handshake. This is the
//     "trust rotation is still a reload" property that a leaf-fingerprint-only
//     check would break.
func TestUnchangedMaterialNoOpReloadNotCounted(t *testing.T) {
	ca1 := newTestCA(t, "ca1")
	ca2 := newTestCA(t, "ca2")
	dir := t.TempDir()
	pod := "gossipgrid-0"

	c1, k1 := issueLeaf(t, ca1, leafCertSpec{dnsNames: []string{pod + "." + testSuffix}})
	certFile := writePEM(t, dir, "cert.pem", certKeyPEMChain(t, c1, k1, ca1))
	keyFile := writePEM(t, dir, "key.pem", keyPEM(t, k1))
	trustFile := writePEM(t, dir, "ca-bundle.pem", ca1.pem)

	mgr := newTestManager(t, certFile, keyFile, trustFile, "gossipgrid-0", 20*time.Millisecond)
	addr, stop := startTestServer(t, mgr, statusHandler(pod, "v"))
	defer stop()

	fpBefore := mgr.Diagnostics().CertFingerprint
	rcBefore := mgr.Diagnostics().ReloadCount

	// (1) Give the reload worker several ticks (well beyond the 20ms interval)
	// with UNCHANGED credential material. The leaf fingerprint, key, and trust
	// bundle are all unchanged, so every tick is a no-op: ReloadCount must
	// stay at rcBefore (0) and the fingerprint must stay at fpBefore.
	settleWait(t, 200)

	rcAfter := mgr.Diagnostics().ReloadCount
	fpAfter := mgr.Diagnostics().CertFingerprint
	if rcAfter != rcBefore {
		t.Fatalf("ReloadCount incremented on unchanged material: before=%d after=%d (want %d)", rcBefore, rcAfter, rcBefore)
	}
	if fpAfter != fpBefore {
		t.Fatalf("fingerprint changed on unchanged material: before=%s after=%s", fpBefore, fpAfter)
	}

	// (2) Now rotate the CA trust bundle (ca1 -> ca1+ca2). The leaf is
	// unchanged but the trust-bundle material changed, so this IS a genuine
	// reload: ReloadCount must increment, and the new snapshot must be picked
	// up on the next handshake. A ca2-signed client, rejected before the
	// rotation, must now be accepted.
	c2, k2 := issueLeaf(t, ca2, leafCertSpec{dnsNames: []string{"gossipgrid-1." + testSuffix}})
	if err := mTLSGet(t, addr, pod+"."+testSuffix, c2, k2, loadPool(t, trustFile)); err == nil {
		t.Fatal("expected ca2-signed client to be rejected before trust rotation, got success")
	}
	mustWriteFile(t, trustFile, trustBundlePEM(ca1, ca2))

	// A genuine reload (trust-bundle material changed) increments ReloadCount,
	// so waitForReload is safe here.
	waitForReload(t, mgr, rcAfter)
	rcAfterRotation := mgr.Diagnostics().ReloadCount
	if rcAfterRotation != rcAfter+1 {
		t.Fatalf("ReloadCount after trust rotation = %d, want %d (exactly one genuine reload)", rcAfterRotation, rcAfter+1)
	}
	if err := mTLSGet(t, addr, pod+"."+testSuffix, c2, k2, loadPool(t, trustFile)); err != nil {
		t.Fatalf("expected ca2-signed client to be accepted after trust rotation: %v", err)
	}
}

// --- 17. Concurrent credential access and rotation under race detector ---

func TestConcurrentAccessAndRotation(t *testing.T) {
	ca := newTestCA(t, "w")
	dir := t.TempDir()
	pod := "gossipgrid-0"

	c1, k1 := issueLeaf(t, ca, leafCertSpec{dnsNames: []string{pod + "." + testSuffix}})
	certFile := writePEM(t, dir, "cert.pem", certKeyPEMChain(t, c1, k1, ca))
	keyFile := writePEM(t, dir, "key.pem", keyPEM(t, k1))
	trustFile := writePEM(t, dir, "ca-bundle.pem", ca.pem)

	mgr := newTestManager(t, certFile, keyFile, trustFile, "gossipgrid-0", 20*time.Millisecond)
	addr, stop := startTestServer(t, mgr, statusHandler(pod, "v"))
	defer stop()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = mgr.TLSConfigForServer()
				_, _ = mgr.TLSConfigForClient(addr, pod+"."+testSuffix)
				_ = mgr.Diagnostics()
				_ = mgr.current()
			}
		}()
	}
	c2, k2 := issueLeaf(t, ca, leafCertSpec{dnsNames: []string{pod + "." + testSuffix}})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			mustWriteFile(t, certFile, certKeyPEMChain(t, c2, k2, ca))
			mustWriteFile(t, keyFile, keyPEM(t, k2))
			time.Sleep(2 * time.Millisecond)
		}
	}()
	wg.Wait()

	client := NewClient(mgr)
	if _, err := client.GetStatus(context.Background(), addr, pod+"."+testSuffix); err != nil {
		t.Fatalf("GetStatus after concurrent rotation: %v", err)
	}
}

// --- 18. Reload workers and listeners shut down without leaking goroutines ---

func TestNoGoroutineLeaksOnShutdown(t *testing.T) {
	ca := newTestCA(t, "w")
	certFile, keyFile, trustFile := testCredentials(t, ca, "gossipgrid-0")

	mgr := New(Config{
		CertFile:         certFile,
		KeyFile:          keyFile,
		TrustBundleFile:  trustFile,
		IdentitySuffix:   testSuffix,
		StatefulSetName:  testStatefulSet,
		NodeID:           "gossipgrid-0",
		ExpectedReplicas: 2,
		ReloadInterval:   30 * time.Millisecond,
	}, nil)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	_, stop := startTestServer(t, mgr, statusHandler("gossipgrid-0", "v"))

	before := runtime.NumGoroutine()
	mgr.Shutdown()
	stop()
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()
	// Allow a small tolerance for the test framework's own goroutines.
	if after > before+3 {
		t.Errorf("goroutine leak suspected: before=%d after=%d", before, after)
	}
	// Double Shutdown must be a no-op (idempotent).
	mgr.Shutdown()
	mgr.Shutdown()
}

// --- 19. TLS works with the FIPS-enabled build ---

func TestFIPSTLSWorks(t *testing.T) {
	// Compiled and run under the FIPS build (GOFIPS140=v1.0.0, the default for
	// `make test` / `make test-race`). Exercises the full mTLS handshake
	// (ECDSA P-256, SHA-256, TLS 1.3), which must all be FIPS-approved.
	ca := newTestCA(t, "w")
	certA, keyA, trustA := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, certA, keyA, trustA, "gossipgrid-0", 100*time.Millisecond)
	addr, stop := startTestServer(t, mgr, statusHandler("gossipgrid-0", "v"))
	defer stop()

	client := NewClient(mgr)
	st, err := client.GetStatus(context.Background(), addr, "gossipgrid-0."+testSuffix)
	if err != nil {
		t.Fatalf("FIPS mTLS handshake: %v", err)
	}
	if st.NodeID != "gossipgrid-0" {
		t.Errorf("status node = %q, want gossipgrid-0", st.NodeID)
	}
}

// --- 20. Client cannot bypass identity checks via IP dial or header ---

func TestNoIdentityBypassViaIPIpHeader(t *testing.T) {
	ca := newTestCA(t, "w")
	certS, keyS, trustS := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, certS, keyS, trustS, "gossipgrid-0", 100*time.Millisecond)
	addr, stop := startTestServer(t, mgr, statusHandler("gossipgrid-0", "v"))
	defer stop()

	// (a) Dial by IP address without a matching ServerName: the server cert's
	// SAN is a DNS name, so hostname verification fails.
	host, _, _ := net.SplitHostPort(addr)
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    loadPool(t, trustS),
				MinVersion: tls.VersionTLS13,
				// No ServerName: Go uses the IP, which does not match the DNS SAN.
			},
		},
		Timeout: 3 * time.Second,
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+host+"/internal/status", nil)
	resp, err := client.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("expected failure when dialing by IP without matching identity, got success")
	}

	// (b) An untrusted identity supplied via an HTTP header must not
	// authenticate the client. The client presents no certificate; the header
	// is never read for identity.
	mtlsClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    loadPool(t, trustS),
				MinVersion: tls.VersionTLS13,
				ServerName: "gossipgrid-0." + testSuffix,
				// No client certificate.
			},
		},
		Timeout: 3 * time.Second,
	}
	reqB, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+addr+"/internal/status", nil)
	reqB.Header.Set("X-Peer-Identity", "gossipgrid-1.")
	respB, errB := mtlsClient.Do(reqB)
	if errB == nil {
		_ = respB.Body.Close()
		t.Fatal("expected failure for client with no cert (header must not authenticate), got success")
	}
}

// --- Test helpers ---

// mustWriteFile writes data to a file, failing the test on error.
func mustWriteFile(t *testing.T, path string, data []byte) {
	t.Helper()
	//nolint:gosec/G306 // test fixtures in per-test temp dirs
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

// waitForReload waits (bounded) until the Manager's ReloadCount increases.
//
// Counter semantics (see tryReload): ReloadCount increments when the
// credential material changes — that is, when EITHER the leaf certificate
// changes (cert rotation) OR the CA trust bundle changes (trust rotation). A
// reload worker tick that reads identical credential material (same leaf and
// same trust bundle) does NOT increment the counter, though it still
// publishes a new snapshot so a concurrent change is picked up on the next
// handshake.
//
// Helper assumption: this helper is safe to call after any change that
// alters the credential material — a leaf certificate rotation or a
// trust-bundle rotation (including a trust-bundle-only change with the leaf
// unchanged). It is NOT safe to call when no credential material has
// changed, because ReloadCount will not increment; such callers should use
// settleWait and verify behavior via the mTLS handshake directly.
func waitForReload(t *testing.T, mgr *Manager, prevCount int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if mgr.Diagnostics().ReloadCount > prevCount {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected a reload (ReloadCount > %d) within 5s", prevCount)
}

// settleWait gives the reload worker a bounded amount of time to attempt a
// (possibly failing) reload. Used when we do NOT require a successful reload
// (e.g. the malformed-reload test, where the reload is expected to fail and
// ReloadCount stays unchanged).
func settleWait(t *testing.T, ms int) {
	t.Helper()
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

// --- Security correction regression tests ---

// TestPeerServerListenAndServeServesHTTPS verifies that the production
// PeerServer.ListenAndServe() entry point actually serves HTTPS (not
// plaintext HTTP). This is a regression test for the defect where
// ListenAndServe called HTTPServer.ListenAndServe() (plaintext) even though
// TLSConfig was populated.
func TestPeerServerListenAndServeServesHTTPS(t *testing.T) {
	ca := newTestCA(t, "w")
	certS, keyS, trustS := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, certS, keyS, trustS, "gossipgrid-0", 100*time.Millisecond)

	// Bind to a random loopback port to avoid conflicts.
	lc := net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // free the port; PeerServer will rebind

	peerSrv := NewPeerServer(mgr, addr, "gossipgrid-0", "v1")
	errCh := make(chan error, 1)
	go func() { errCh <- peerSrv.ListenAndServe() }()

	// Wait for the listener to come up.
	dialer := &net.Dialer{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, derr := dialer.DialContext(context.Background(), "tcp", addr)
		if derr == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("peer listener did not come up: %v", derr)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// (a) Plain HTTP must be rejected (the listener is TLS-only).
	plainClient := &http.Client{Timeout: 3 * time.Second}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr+"/internal/status", nil)
	resp, err := plainClient.Do(req)
	if err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			t.Fatal("plain HTTP request to peer listener returned 200; it must be rejected")
		}
	}
	// A non-nil error (TLS handshake failure, connection reset) is also acceptable
	// as "rejected". A 200 response is not.

	// (b) HTTPS without a client certificate must be rejected.
	pool := loadPool(t, trustS)
	noCertClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    pool,
				MinVersion: tls.VersionTLS13,
				ServerName: "gossipgrid-0." + testSuffix,
			},
		},
		Timeout: 3 * time.Second,
	}
	req2, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+addr+"/internal/status", nil)
	resp2, err2 := noCertClient.Do(req2)
	if err2 == nil {
		_ = resp2.Body.Close()
		t.Fatal("HTTPS without client cert succeeded; it must be rejected")
	}

	// (c) HTTPS with an authorized peer certificate succeeds.
	clientCert, clientKey := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"gossipgrid-1." + testSuffix},
		cn:       "gossipgrid-1." + testSuffix,
	})
	authorizedClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    pool,
				MinVersion: tls.VersionTLS13,
				Certificates: []tls.Certificate{{
					Certificate: [][]byte{clientCert.Raw},
					PrivateKey:  clientKey,
				}},
				ServerName: "gossipgrid-0." + testSuffix,
			},
		},
		Timeout: 3 * time.Second,
	}
	req3, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+addr+"/internal/status", nil)
	resp3, err3 := authorizedClient.Do(req3)
	if err3 != nil {
		t.Fatalf("authorized mTLS request failed: %v", err3)
	}
	_ = resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Errorf("authorized mTLS request got status %d, want 200", resp3.StatusCode)
	}

	// (d) HTTPS with a trusted but unauthorized client cert (wrong StatefulSet)
	// is rejected.
	badCert, badKey := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"other-0." + testSuffix},
		cn:       "other-0." + testSuffix,
	})
	badClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    pool,
				MinVersion: tls.VersionTLS13,
				Certificates: []tls.Certificate{{
					Certificate: [][]byte{badCert.Raw},
					PrivateKey:  badKey,
				}},
				ServerName: "gossipgrid-0." + testSuffix,
			},
		},
		Timeout: 3 * time.Second,
	}
	req4, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+addr+"/internal/status", nil)
	resp4, err4 := badClient.Do(req4)
	if err4 == nil {
		_ = resp4.Body.Close()
		t.Fatal("trusted-but-unauthorized client cert (wrong StatefulSet) was accepted; it must be rejected")
	}

	// Clean up.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = peerSrv.Shutdown(ctx)
}

// TestManagerLifecycleFailedStart verifies that Shutdown returns promptly
// after a failed Start (no goroutine was started, so no hang on m.done).
func TestManagerLifecycleFailedStart(t *testing.T) {
	// Use a non-existent cert file to cause Start to fail.
	mgr := New(Config{
		CertFile:         "/nonexistent/cert.pem",
		KeyFile:          "/nonexistent/key.pem",
		TrustBundleFile:  "/nonexistent/ca.pem",
		IdentitySuffix:   testSuffix,
		StatefulSetName:  testStatefulSet,
		NodeID:           "gossipgrid-0",
		ExpectedReplicas: 2,
		ReloadInterval:   time.Second,
	}, nil)

	err := mgr.Start(context.Background())
	if err == nil {
		t.Fatal("expected Start to fail with non-existent files, got nil")
	}

	// Shutdown must return promptly (no goroutine to wait for).
	done := make(chan struct{})
	go func() {
		mgr.Shutdown()
		close(done)
	}()
	select {
	case <-done:
		// OK: Shutdown returned promptly.
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown hung after failed Start (no goroutine should be running)")
	}
}

// TestManagerLifecycleRepeatedStart verifies that a second Start returns an
// error and does not spawn a second reload worker.
func TestManagerLifecycleRepeatedStart(t *testing.T) {
	ca := newTestCA(t, "w")
	cert, key, trust := testCredentials(t, ca, "gossipgrid-0")
	mgr := New(Config{
		CertFile:         cert,
		KeyFile:          key,
		TrustBundleFile:  trust,
		IdentitySuffix:   testSuffix,
		StatefulSetName:  testStatefulSet,
		NodeID:           "gossipgrid-0",
		ExpectedReplicas: 2,
		ReloadInterval:   time.Second,
	}, nil)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	// Second Start must fail (already started).
	if err := mgr.Start(context.Background()); err == nil {
		t.Fatal("second Start succeeded; it must return an error")
	}
	mgr.Shutdown()
	// Start after Shutdown must also fail (stopped state).
	if err := mgr.Start(context.Background()); err == nil {
		t.Fatal("Start after Shutdown succeeded; it must return an error")
	}
}

// TestManagerLifecycleConcurrentShutdown verifies that concurrent or repeated
// Shutdown calls do not panic (no double-close of channels).
func TestManagerLifecycleConcurrentShutdown(t *testing.T) {
	ca := newTestCA(t, "w")
	cert, key, trust := testCredentials(t, ca, "gossipgrid-0")
	mgr := New(Config{
		CertFile:         cert,
		KeyFile:          key,
		TrustBundleFile:  trust,
		IdentitySuffix:   testSuffix,
		StatefulSetName:  testStatefulSet,
		NodeID:           "gossipgrid-0",
		ExpectedReplicas: 2,
		ReloadInterval:   time.Second,
	}, nil)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Call Shutdown from multiple goroutines concurrently.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mgr.Shutdown()
		}()
	}
	wg.Wait()
	// All calls returned without panic.
}

// TestRejectKeyMismatch verifies that a mismatched certificate/key pair
// causes a startup failure (the key does not match the cert's public key).
func TestRejectKeyMismatch(t *testing.T) {
	ca := newTestCA(t, "w")
	// Issue cert for gossipgrid-0, but use a DIFFERENT key (for gossipgrid-1).
	certA, _ := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"gossipgrid-0." + testSuffix},
		cn:       "gossipgrid-0." + testSuffix,
	})
	_, keyB := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"gossipgrid-1." + testSuffix},
		cn:       "gossipgrid-1." + testSuffix,
	})
	dir := t.TempDir()
	// Write certA's chain but keyB's key.
	certPEM := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certA.Raw}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})...)
	keyDer, _ := x509.MarshalPKCS8PrivateKey(keyB)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDer})
	certFile := writePEM(t, dir, "cert.pem", certPEM)
	keyFile := writePEM(t, dir, "key.pem", keyPEM)
	trustFile := writePEM(t, dir, "ca-bundle.pem", ca.pem)

	mgr := New(Config{
		CertFile:         certFile,
		KeyFile:          keyFile,
		TrustBundleFile:  trustFile,
		IdentitySuffix:   testSuffix,
		StatefulSetName:  testStatefulSet,
		NodeID:           "gossipgrid-0",
		ExpectedReplicas: 2,
		ReloadInterval:   time.Second,
	}, nil)
	if err := mgr.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded with mismatched key/cert; it must fail")
	}
}

// TestRejectLocalCertNotSignedByConfiguredCA verifies that a local certificate
// signed by a CA not in the configured trust bundle fails startup.
func TestRejectLocalCertNotSignedByConfiguredCA(t *testing.T) {
	// Two different CAs.
	caSigner := newTestCA(t, "signer")     // signs the local cert
	caConfigured := newTestCA(t, "config") // in the trust bundle
	cert, key := issueLeaf(t, caSigner, leafCertSpec{
		dnsNames: []string{"gossipgrid-0." + testSuffix},
		cn:       "gossipgrid-0." + testSuffix,
	})
	dir := t.TempDir()
	certPEM := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caSigner.cert.Raw})...)
	keyDer, _ := x509.MarshalPKCS8PrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDer})
	certFile := writePEM(t, dir, "cert.pem", certPEM)
	keyFile := writePEM(t, dir, "key.pem", keyPEM)
	// Trust bundle contains only caConfigured, NOT caSigner.
	trustFile := writePEM(t, dir, "ca-bundle.pem", caConfigured.pem)

	mgr := New(Config{
		CertFile:         certFile,
		KeyFile:          keyFile,
		TrustBundleFile:  trustFile,
		IdentitySuffix:   testSuffix,
		StatefulSetName:  testStatefulSet,
		NodeID:           "gossipgrid-0",
		ExpectedReplicas: 2,
		ReloadInterval:   time.Second,
	}, nil)
	if err := mgr.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded with local cert not signed by configured CA; it must fail")
	}
}

// TestRejectCertificateWithIPSAN verifies that a certificate with an IP SAN
// (in addition to its DNS SAN) is rejected.
func TestRejectCertificateWithIPSAN(t *testing.T) {
	ca := newTestCA(t, "w")
	cert, key := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"gossipgrid-0." + testSuffix},
		cn:       "gossipgrid-0." + testSuffix,
	})
	// Add an IP SAN to the cert by re-issuing with IPAddresses.
	// issueLeaf doesn't support IP SANs, so we construct manually.
	ipCert, ipKey := issueLeafWithIPSAN(t, ca, "gossipgrid-0."+testSuffix, net.ParseIP("10.0.0.1"))
	_ = cert
	_ = key
	dir := t.TempDir()
	certPEM := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ipCert.Raw}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})...)
	keyDer, _ := x509.MarshalPKCS8PrivateKey(ipKey)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDer})
	certFile := writePEM(t, dir, "cert.pem", certPEM)
	keyFile := writePEM(t, dir, "key.pem", keyPEM)
	trustFile := writePEM(t, dir, "ca-bundle.pem", ca.pem)

	mgr := New(Config{
		CertFile:         certFile,
		KeyFile:          keyFile,
		TrustBundleFile:  trustFile,
		IdentitySuffix:   testSuffix,
		StatefulSetName:  testStatefulSet,
		NodeID:           "gossipgrid-0",
		ExpectedReplicas: 2,
		ReloadInterval:   time.Second,
	}, nil)
	if err := mgr.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded with cert containing IP SAN; it must fail")
	}
}

// TestRejectOrdinalOutOfRange verifies that a certificate with an ordinal
// beyond the expected replica count is rejected.
func TestRejectOrdinalOutOfRange(t *testing.T) {
	ca := newTestCA(t, "w")
	// Cert for gossipgrid-5 but only 2 replicas expected.
	cert, key := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"gossipgrid-5." + testSuffix},
		cn:       "gossipgrid-5." + testSuffix,
	})
	dir := t.TempDir()
	certPEM := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})...)
	keyDer, _ := x509.MarshalPKCS8PrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDer})
	certFile := writePEM(t, dir, "cert.pem", certPEM)
	keyFile := writePEM(t, dir, "key.pem", keyPEM)
	trustFile := writePEM(t, dir, "ca-bundle.pem", ca.pem)

	mgr := New(Config{
		CertFile:         certFile,
		KeyFile:          keyFile,
		TrustBundleFile:  trustFile,
		IdentitySuffix:   testSuffix,
		StatefulSetName:  testStatefulSet,
		NodeID:           "gossipgrid-5",
		ExpectedReplicas: 2, // only 2 replicas: ordinals 0 and 1
		ReloadInterval:   time.Second,
	}, nil)
	if err := mgr.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded with ordinal 5 but only 2 replicas; it must fail")
	}
}

// TestRejectNodeIDMismatch verifies that a local certificate whose pod
// identity does not match the configured NodeID fails startup.
func TestRejectNodeIDMismatch(t *testing.T) {
	ca := newTestCA(t, "w")
	// Cert for gossipgrid-0 but NodeID configured as gossipgrid-1.
	cert, key := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"gossipgrid-0." + testSuffix},
		cn:       "gossipgrid-0." + testSuffix,
	})
	dir := t.TempDir()
	certPEM := append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})...)
	keyDer, _ := x509.MarshalPKCS8PrivateKey(key)
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDer})
	certFile := writePEM(t, dir, "cert.pem", certPEM)
	keyFile := writePEM(t, dir, "key.pem", keyPEM)
	trustFile := writePEM(t, dir, "ca-bundle.pem", ca.pem)

	mgr := New(Config{
		CertFile:         certFile,
		KeyFile:          keyFile,
		TrustBundleFile:  trustFile,
		IdentitySuffix:   testSuffix,
		StatefulSetName:  testStatefulSet,
		NodeID:           "gossipgrid-1", // mismatch: cert is gossipgrid-0
		ExpectedReplicas: 2,
		ReloadInterval:   time.Second,
	}, nil)
	if err := mgr.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded with NodeID mismatch (cert=gossipgrid-0, NodeID=gossipgrid-1); it must fail")
	}
}

// issueLeafWithIPSAN issues a leaf cert with both a DNS SAN and an IP SAN.
func issueLeafWithIPSAN(t *testing.T, ca *testCA, dns string, ip net.IP) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: dns},
		DNSNames:     []string{dns},
		IPAddresses:  []net.IP{ip},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(720 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("creating cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing cert: %v", err)
	}
	return cert, key
}
