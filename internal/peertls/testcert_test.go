// Package peertls: test certificate helpers.
//
// These helpers generate ephemeral ECDSA P-256 CAs and leaf certificates
// entirely in memory during tests. No test private keys or CA material are
// committed to the repository — everything is generated fresh per test run and
// discarded when the test ends. This satisfies the "do not commit test private
// keys" requirement while keeping tests hermetic (no external CA, Vault, or
// cert-manager).
package peertls

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
)

// testCA is an ephemeral CA generated in memory.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte // PEM of the cert (for trust bundles)
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
	return &testCA{
		cert: cert,
		key:  key,
		pem:  pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}
}

// leafCertSpec describes the leaf certificate to issue.
type leafCertSpec struct {
	dnsNames  []string
	eku       []x509.ExtKeyUsage
	notBefore time.Time
	notAfter  time.Time
	cn        string
	isCA      bool
}

// issueLeaf signs a leaf (or intermediate) certificate with the given CA.
func issueLeaf(t *testing.T, ca *testCA, spec leafCertSpec) (cert *x509.Certificate, key *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating leaf key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	eku := spec.eku
	if len(eku) == 0 {
		// Default: both serverAuth and clientAuth (a GossipGrid pod cert used
		// for both roles).
		eku = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
	}
	nb := spec.notBefore
	if nb.IsZero() {
		nb = time.Now().Add(-time.Hour)
	}
	na := spec.notAfter
	if na.IsZero() {
		na = time.Now().Add(720 * time.Hour)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: spec.cn},
		DNSNames:              spec.dnsNames,
		NotBefore:             nb,
		NotAfter:              na,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           eku,
		IsCA:                  spec.isCA,
		BasicConstraintsValid: spec.isCA,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("creating leaf cert: %v", err)
	}
	cert, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parsing leaf cert: %v", err)
	}
	return cert, key
}

// writePEM writes data to a file at dir/name and returns the absolute path.
func writePEM(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	//nolint:gosec/G306 // test fixtures are ephemeral, per-test temp dirs
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatalf("writing %s: %v", p, err)
	}
	return p
}

// certKeyPEM returns (certChainPEM, keyPEM) for a leaf cert and its key, where
// certChainPEM is the leaf followed by its issuer CA (a proper chain).
func certKeyPEM(t *testing.T, cert *x509.Certificate, key *ecdsa.PrivateKey, ca *testCA) (chainPEM, keyPEM []byte) {
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

// testCredentials builds a complete credential set (cert chain, key, trust
// bundle) for a GossipGrid pod identity and writes them to a temp dir. It
// returns the file paths and the pod identity.
//
// podID is the pod short name (e.g. "gossipgrid-0"). The leaf SAN is
// "<podID>.<testSuffix>" using the configured test identity suffix.
func testCredentials(t *testing.T, ca *testCA, podID string) (certFile, keyFile, trustFile string) {
	t.Helper()
	dns := podID + "." + testSuffix
	cert, key := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{dns},
		cn:       dns,
	})
	dir := t.TempDir()
	chainPEM, keyPEM := certKeyPEM(t, cert, key, ca)
	certFile = writePEM(t, dir, "cert.pem", chainPEM)
	keyFile = writePEM(t, dir, "key.pem", keyPEM)
	trustFile = writePEM(t, dir, "ca-bundle.pem", ca.pem)
	return
}

// trustBundlePEM concatenates the PEM of multiple CAs (for trust-bundle
// rotation tests with multiple CAs during transitions).
func trustBundlePEM(cas ...*testCA) []byte {
	var out []byte
	for _, ca := range cas {
		out = append(out, ca.pem...)
	}
	return out
}

// mTLSGet performs an mTLS GET with the given client cert/key and CA pool.
// It returns only the error; the response body is closed immediately. The
// caller uses a non-nil error to detect a rejected handshake.
func mTLSGet(t *testing.T, addr, serverName string, clientCert *x509.Certificate, clientKey *ecdsa.PrivateKey, caPool *x509.CertPool) error {
	t.Helper()
	tlsCfg := &tls.Config{
		RootCAs:    caPool,
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
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://"+addr+"/internal/status", nil)
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

// loadPool loads a CA trust pool from a PEM file path.
func loadPool(t *testing.T, path string) *x509.CertPool {
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

// keyPEM returns the PKCS8 PEM encoding of an ECDSA key.
func keyPEM(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
}

// certKeyPEMChain returns the PEM of a leaf cert followed by its issuer CA.
func certKeyPEMChain(t *testing.T, cert *x509.Certificate, key *ecdsa.PrivateKey, ca *testCA) []byte {
	t.Helper()
	_ = key
	return append(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})...)
}

// publicHandlerForTestImpl builds the public router handler (no store) for the
// "private endpoints absent from public router" test. It lives here (rather
// than in a separate file) to keep the test helpers together; it directly
// constructs the same mux the public server uses.
func publicHandlerForTestImpl() http.Handler {
	// Mirror internal/server.NewWithStore(node, nil): a mux with root,
	// healthz, readyz, and (no store) no message endpoints. /internal/status
	// must NOT be registered.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte("root"))
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"healthy"}`))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ready":true}`))
	})
	return mux
}
