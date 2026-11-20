package config

import (
	"os"
	"testing"
	"time"
)

// env builds a lookup map for the injectable load function.
func env(vars map[string]string) envFunc {
	return func(key string) (string, bool) {
		v, ok := vars[key]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Setenv(EnvHTTPAddr, "")
	t.Setenv(EnvNodeID, "")
	t.Setenv(EnvLogLevel, "")
	t.Setenv(EnvShutdownTimeout, "")

	host, err := os.Hostname()
	if err != nil {
		t.Fatalf("os.Hostname: %v", err)
	}

	cfg, err := load(env(nil))
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.NodeID != host {
		t.Errorf("NodeID = %q, want %q", cfg.NodeID, host)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want info", cfg.LogLevel)
	}
	if cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 10s", cfg.ShutdownTimeout)
	}
}

func TestLoadEnvOverrides(t *testing.T) {
	cfg, err := load(env(map[string]string{
		EnvHTTPAddr:        "127.0.0.1:9090",
		EnvNodeID:          "gossipgrid-0",
		EnvLogLevel:        "DEBUG",
		EnvShutdownTimeout: "30s",
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.HTTPAddr != "127.0.0.1:9090" {
		t.Errorf("HTTPAddr = %q, want 127.0.0.1:9090", cfg.HTTPAddr)
	}
	if cfg.NodeID != "gossipgrid-0" {
		t.Errorf("NodeID = %q, want gossipgrid-0", cfg.NodeID)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want debug (lower-cased)", cfg.LogLevel)
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 30s", cfg.ShutdownTimeout)
	}
}

func TestLoadInvalidValues(t *testing.T) {
	cases := []struct {
		name string
		vars map[string]string
	}{
		{"bad address missing port", map[string]string{EnvHTTPAddr: "localhost"}},
		{"bad address empty port", map[string]string{EnvHTTPAddr: "localhost:"}},
		{"bad address missing host and port", map[string]string{EnvHTTPAddr: "8080"}},
		{"port out of range (0)", map[string]string{EnvHTTPAddr: ":0"}},
		{"port out of range (65536)", map[string]string{EnvHTTPAddr: ":65536"}},
		{"non-numeric port", map[string]string{EnvHTTPAddr: ":80a"}},
		{"bad log level", map[string]string{EnvLogLevel: "verbose"}},
		{"bad duration", map[string]string{EnvShutdownTimeout: "soon"}},
		{"non-positive duration", map[string]string{EnvShutdownTimeout: "0s"}},
		{"explicitly empty node id", map[string]string{EnvNodeID: ""}},
		// Discovery: zero interval.
		{"zero discovery interval", map[string]string{
			EnvDiscoveryMode:     "dns",
			EnvDiscoveryDNSName:  "gossipgrid-headless.default.svc",
			EnvDiscoveryInterval: "0s",
		}},
		// Discovery: negative interval.
		{"negative discovery interval", map[string]string{
			EnvDiscoveryMode:     "dns",
			EnvDiscoveryDNSName:  "gossipgrid-headless.default.svc",
			EnvDiscoveryInterval: "-5s",
		}},
		// Discovery: zero timeout.
		{"zero discovery timeout", map[string]string{
			EnvDiscoveryMode:    "dns",
			EnvDiscoveryDNSName: "gossipgrid-headless.default.svc",
			EnvDiscoveryTimeout: "0s",
		}},
		// Discovery: negative timeout.
		{"negative discovery timeout", map[string]string{
			EnvDiscoveryMode:    "dns",
			EnvDiscoveryDNSName: "gossipgrid-headless.default.svc",
			EnvDiscoveryTimeout: "-3s",
		}},
		// Static peer: invalid port (out of range).
		{"static peer port 0", map[string]string{
			EnvDiscoveryMode: "static",
			EnvStaticPeers:   "a:0",
		}},
		{"static peer port 65536", map[string]string{
			EnvDiscoveryMode: "static",
			EnvStaticPeers:   "a:65536",
		}},
		{"static peer non-numeric port", map[string]string{
			EnvDiscoveryMode: "static",
			EnvStaticPeers:   "a:80a",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := load(env(tc.vars))
			if err == nil {
				t.Fatalf("expected error, got %+v", cfg)
			}
		})
	}
}

// TestNodeIDUnsetVsEmpty verifies that an unset GOSSIPGRID_NODE_ID defaults to
// the hostname, while an explicitly empty value is rejected.
func TestNodeIDUnsetVsEmpty(t *testing.T) {
	host, err := os.Hostname()
	if err != nil {
		t.Fatalf("os.Hostname: %v", err)
	}

	// Unset: env map without the key → hostname default.
	cfg, err := load(env(map[string]string{}))
	if err != nil {
		t.Fatalf("load (unset): %v", err)
	}
	if cfg.NodeID != host {
		t.Errorf("unset: NodeID = %q, want %q (hostname)", cfg.NodeID, host)
	}

	// Explicitly empty: env map with the key set to "" → error.
	_, err = load(env(map[string]string{EnvNodeID: ""}))
	if err == nil {
		t.Error("explicitly empty: expected error, got nil")
	} else if !contains(err.Error(), EnvNodeID) {
		t.Errorf("error should mention %s: %v", EnvNodeID, err)
	}
}

func TestLoadMultipleErrors(t *testing.T) {
	_, err := load(env(map[string]string{
		EnvHTTPAddr:        "bad",
		EnvLogLevel:        "nope",
		EnvShutdownTimeout: "x",
	}))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// All three errors should be surfaced (errors.Join).
	for _, want := range []string{EnvHTTPAddr, EnvLogLevel, EnvShutdownTimeout} {
		if !contains(err.Error(), want) {
			t.Errorf("error should mention %s: %v", want, err)
		}
	}
}

// TestLoadValidDiscovery verifies that valid discovery configurations are
// accepted, including DNS mode with the headless Service name and static mode
// with bracketed IPv6 addresses.
func TestLoadValidPeerTLS(t *testing.T) {
	cfg, err := load(env(map[string]string{
		EnvPeerTLSEnabled:        "true",
		EnvPeerAddr:              "127.0.0.1:8443",
		EnvPeerCertFile:          "/certs/tls.crt",
		EnvPeerKeyFile:           "/certs/tls.key",
		EnvPeerTrustBundleFile:   "/certs/ca-bundle.pem",
		EnvPeerIdentitySuffix:    "gossipgrid-headless.default.svc.cluster.local",
		EnvPeerTLSReloadInterval: "5s",
	}))
	if err != nil {
		t.Fatalf("expected valid peer TLS config, got error: %v", err)
	}
	if !cfg.PeerTLSEnabled {
		t.Error("PeerTLSEnabled should be true")
	}
	if cfg.PeerAddr != "127.0.0.1:8443" {
		t.Errorf("PeerAddr = %q, want 127.0.0.1:8443", cfg.PeerAddr)
	}
	if cfg.PeerIdentitySuffix != "gossipgrid-headless.default.svc.cluster.local" {
		t.Errorf("PeerIdentitySuffix = %q, want full suffix", cfg.PeerIdentitySuffix)
	}
	if cfg.PeerTLSReloadInterval != "5s" {
		t.Errorf("PeerTLSReloadInterval = %q, want 5s", cfg.PeerTLSReloadInterval)
	}
}

func TestLoadInvalidPeerTLS(t *testing.T) {
	cases := []struct {
		name string
		vars map[string]string
	}{
		{"enabled but missing cert file", map[string]string{
			EnvPeerTLSEnabled:      "true",
			EnvPeerKeyFile:         "/k",
			EnvPeerTrustBundleFile: "/t",
			EnvPeerIdentitySuffix:  "a.b.c",
		}},
		{"enabled but missing key file", map[string]string{
			EnvPeerTLSEnabled:      "true",
			EnvPeerCertFile:        "/c",
			EnvPeerTrustBundleFile: "/t",
			EnvPeerIdentitySuffix:  "a.b.c",
		}},
		{"enabled but missing trust bundle", map[string]string{
			EnvPeerTLSEnabled:     "true",
			EnvPeerCertFile:       "/c",
			EnvPeerKeyFile:        "/k",
			EnvPeerIdentitySuffix: "a.b.c",
		}},
		{"enabled but missing identity suffix", map[string]string{
			EnvPeerTLSEnabled:      "true",
			EnvPeerCertFile:        "/c",
			EnvPeerKeyFile:         "/k",
			EnvPeerTrustBundleFile: "/t",
		}},
		{"invalid identity suffix (single label)", map[string]string{
			EnvPeerTLSEnabled:      "true",
			EnvPeerCertFile:        "/c",
			EnvPeerKeyFile:         "/k",
			EnvPeerTrustBundleFile: "/t",
			EnvPeerIdentitySuffix:  "gossipgrid-headless",
		}},
		{"invalid enable value (typo)", map[string]string{
			EnvPeerTLSEnabled:      "tru",
			EnvPeerCertFile:        "/c",
			EnvPeerKeyFile:         "/k",
			EnvPeerTrustBundleFile: "/t",
			EnvPeerIdentitySuffix:  "a.b.c",
		}},
		{"invalid reload interval", map[string]string{
			EnvPeerTLSEnabled:        "true",
			EnvPeerCertFile:          "/c",
			EnvPeerKeyFile:           "/k",
			EnvPeerTrustBundleFile:   "/t",
			EnvPeerIdentitySuffix:    "a.b.c",
			EnvPeerTLSReloadInterval: "soon",
		}},
		{"non-positive reload interval", map[string]string{
			EnvPeerTLSEnabled:        "true",
			EnvPeerCertFile:          "/c",
			EnvPeerKeyFile:           "/k",
			EnvPeerTrustBundleFile:   "/t",
			EnvPeerIdentitySuffix:    "a.b.c",
			EnvPeerTLSReloadInterval: "0s",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(env(tc.vars))
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestPeerTLSDisabledDefaults(t *testing.T) {
	cfg, err := load(env(nil))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.PeerTLSEnabled {
		t.Error("PeerTLSEnabled should default to false")
	}
	if cfg.PeerAddr != ":8443" {
		t.Errorf("PeerAddr default = %q, want :8443", cfg.PeerAddr)
	}
	if cfg.PeerTLSReloadInterval != "2s" {
		t.Errorf("PeerTLSReloadInterval default = %q, want 2s", cfg.PeerTLSReloadInterval)
	}
}

func TestLoadValidDiscovery(t *testing.T) {
	cases := []struct {
		name string
		vars map[string]string
	}{
		{"valid DNS mode", map[string]string{
			EnvDiscoveryMode:       "dns",
			EnvDiscoveryDNSName:    "gossipgrid-headless.default.svc",
			EnvDiscoverySRVService: "peer",
			EnvDiscoveryInterval:   "10s",
			EnvDiscoveryTimeout:    "3s",
			EnvExpectedReplicas:    "5",
			EnvStatefulSetName:     "gossipgrid",
		}},
		{"valid DNS mode with non-cluster-local domain", map[string]string{
			EnvDiscoveryMode:       "dns",
			EnvDiscoveryDNSName:    "gossipgrid-headless.prod.svc.example.internal",
			EnvDiscoverySRVService: "peer",
			EnvDiscoveryInterval:   "15s",
			EnvDiscoveryTimeout:    "5s",
			EnvExpectedReplicas:    "3",
			EnvStatefulSetName:     "gossipgrid",
		}},
		{"valid static mode with IPv6", map[string]string{
			EnvDiscoveryMode: "static",
			EnvStaticPeers:   "[::1]:8443,127.0.0.1:8443",
		}},
		{"valid static mode with host:port", map[string]string{
			EnvDiscoveryMode: "static",
			EnvStaticPeers:   "gossipgrid-1:8443,gossipgrid-2:8443",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := load(env(tc.vars))
			if err != nil {
				t.Fatalf("expected valid config, got error: %v", err)
			}
			_ = cfg
		})
	}
}

func TestValidateAddr(t *testing.T) {
	// Valid: syntax + port range, no DNS. Includes bracketed IPv6.
	valid := []string{
		":8080",        // all interfaces
		"0.0.0.0:8080", // IPv4 any
		"127.0.0.1:8080",
		"[::1]:8080",     // bracketed IPv6 loopback
		"localhost:8080", // named host (no resolution here)
		":1",             // lowest valid port
		":65535",         // highest valid port
	}
	for _, addr := range valid {
		if err := validateAddr(addr); err != nil {
			t.Errorf("validateAddr(%q) = %v, want nil", addr, err)
		}
	}

	invalid := []string{
		"localhost",   // missing port
		"127.0.0.1",   // missing port
		"8080",        // missing host and port
		":8080:extra", // malformed
		":0",          // port below range
		":65536",      // port above range
		":80a",        // non-numeric port
		":",           // empty port
		"::1:8080",    // unbracketed IPv6 (SplitHostPort fails)
	}
	for _, addr := range invalid {
		if err := validateAddr(addr); err == nil {
			t.Errorf("validateAddr(%q) = nil, want error", addr)
		}
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
