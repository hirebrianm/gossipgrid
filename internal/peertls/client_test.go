// Package peertls: redirect rejection tests for the authenticated peer client.
//
// These tests verify that Client.GetStatus never follows an HTTP redirect
// issued by an authenticated peer, whether the redirect target is plaintext
// HTTP or a different HTTPS address. This prevents a security downgrade: a
// malicious or misbehaving peer must not be able to redirect the client to an
// unauthenticated endpoint that supplies a forged status response.
package peertls

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestClientRejectsRedirectToPlaintextHTTP verifies that when an authorized
// mTLS peer responds with a 302 redirect to a plaintext HTTP server, the
// client rejects the redirect without contacting the plaintext server.
func TestClientRejectsRedirectToPlaintextHTTP(t *testing.T) {
	// Start a plaintext HTTP server that counts hits.
	hits := int32(0)
	targetSrv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits++
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"node_id":"forged","version":"forged"}`))
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
	certS, keyS, trustS := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, certS, keyS, trustS, "gossipgrid-0", 50*time.Millisecond)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "http://"+targetAddr+"/internal/status")
		w.WriteHeader(http.StatusFound)
	})
	mtlsAddr, stopMTLS := startTestServer(t, mgr, mux)
	defer stopMTLS()

	client := NewClient(mgr)
	_, err = client.GetStatus(context.Background(), mtlsAddr, "gossipgrid-0."+testSuffix)
	if err == nil {
		t.Fatal("GetStatus succeeded despite redirect; it must reject all redirects")
	}
	if !strings.Contains(err.Error(), "redirect") {
		t.Errorf("error should mention redirect, got: %v", err)
	}

	if got := hits; got != 0 {
		t.Errorf("plaintext target received %d requests; it should have received 0", got)
	}
}

// TestClientRejectsRedirectToOtherHTTPS verifies that when an authorized peer
// redirects to a different HTTPS address, the client rejects it without
// connecting to the target.
func TestClientRejectsRedirectToOtherHTTPS(t *testing.T) {
	ca := newTestCA(t, "w")

	// Start a TLS server on a second port (acts as the redirect target).
	targetCert, targetKey := issueLeaf(t, ca, leafCertSpec{
		dnsNames: []string{"target.example.com"},
	})
	targetCfg := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{{Certificate: [][]byte{targetCert.Raw}, PrivateKey: targetKey}},
	}
	lc := net.ListenConfig{}
	targetListener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening for HTTPS redirect target: %v", err)
	}
	tlsTargetListener := tls.NewListener(targetListener, targetCfg)
	targetAddr := tlsTargetListener.Addr().String()
	targetSrv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"node_id":"forged","version":"forged"}`))
		}),
	}
	go func() { _ = targetSrv.Serve(tlsTargetListener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = targetSrv.Shutdown(ctx)
	})

	// The mTLS peer that issues the redirect.
	certS, keyS, trustS := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, certS, keyS, trustS, "gossipgrid-0", 50*time.Millisecond)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://"+targetAddr+"/internal/status")
		w.WriteHeader(http.StatusFound)
	})
	mtlsAddr, stopMTLS := startTestServer(t, mgr, mux)
	defer stopMTLS()

	client := NewClient(mgr)
	_, err = client.GetStatus(context.Background(), mtlsAddr, "gossipgrid-0."+testSuffix)
	if err == nil {
		t.Fatal("GetStatus succeeded despite HTTPS redirect; it must reject all redirects")
	}
	if !strings.Contains(err.Error(), "redirect") {
		t.Errorf("error should mention redirect, got: %v", err)
	}
}

// TestClientRejectsAllRedirectStatusCodes verifies that 301, 302, 307, and 308
// redirects are all rejected.
func TestClientRejectsAllRedirectStatusCodes(t *testing.T) {
	codes := []int{
		http.StatusMovedPermanently,  // 301
		http.StatusFound,             // 302
		http.StatusTemporaryRedirect, // 307
		http.StatusPermanentRedirect, // 308
	}
	for _, code := range codes {
		t.Run(http.StatusText(code), func(t *testing.T) {
			// Start a plaintext HTTP target.
			lc := net.ListenConfig{}
			targetListener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listening: %v", err)
			}
			targetAddr := targetListener.Addr().String()
			targetSrv := &http.Server{
				Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					_, _ = w.Write([]byte(`{"node_id":"forged"}`))
				}),
			}
			go func() { _ = targetSrv.Serve(targetListener) }()
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = targetSrv.Shutdown(ctx)
			}()

			ca := newTestCA(t, "w")
			certS, keyS, trustS := testCredentials(t, ca, "gossipgrid-0")
			mgr := newTestManager(t, certS, keyS, trustS, "gossipgrid-0", 50*time.Millisecond)

			mux := http.NewServeMux()
			mux.HandleFunc("GET /internal/status", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", "http://"+targetAddr+"/internal/status")
				w.WriteHeader(code)
			})
			mtlsAddr, stopMTLS := startTestServer(t, mgr, mux)
			defer stopMTLS()

			client := NewClient(mgr)
			_, err = client.GetStatus(context.Background(), mtlsAddr, "gossipgrid-0."+testSuffix)
			if err == nil {
				t.Fatalf("GetStatus succeeded despite HTTP %d redirect; it must be rejected", code)
			}
			if !strings.Contains(err.Error(), "redirect") {
				t.Errorf("error should mention redirect, got: %v", err)
			}
		})
	}
}

// TestClientGetStatusSucceedsOnDirect200 verifies that a normal authenticated
// 200 status response continues to work (no false-positive redirect rejection).
func TestClientGetStatusSucceedsOnDirect200(t *testing.T) {
	ca := newTestCA(t, "w")
	certA, keyA, trustA := testCredentials(t, ca, "gossipgrid-0")
	certB, keyB, trustB := testCredentials(t, ca, "gossipgrid-1")

	mgrA := newTestManager(t, certA, keyA, trustA, "gossipgrid-0", 100*time.Millisecond)
	mgrB := newTestManager(t, certB, keyB, trustB, "gossipgrid-1", 100*time.Millisecond)

	_, stopA := startTestServer(t, mgrA, statusHandler("gossipgrid-0", "v0.1.0-test"))
	defer stopA()
	addrB, stopB := startTestServer(t, mgrB, statusHandler("gossipgrid-1", "v0.1.0-test"))
	defer stopB()

	clientA := NewClient(mgrA)
	st, err := clientA.GetStatus(context.Background(), addrB, "gossipgrid-1."+testSuffix)
	if err != nil {
		t.Fatalf("direct 200 GetStatus failed: %v", err)
	}
	if st.NodeID != "gossipgrid-1" || st.Version != "v0.1.0-test" {
		t.Errorf("status = %+v, want node gossipgrid-1 version v0.1.0-test", st)
	}
}
