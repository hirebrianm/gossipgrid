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
	if cfg.PeerMessageExchangeEnabled {
		t.Error("PeerMessageExchangeEnabled should default to false (Milestone 4A opt-in)")
	}
}

// TestPeerMessageExchangeEnabledDefaults verifies the default for
// GOSSIPGRID_PEER_MESSAGE_EXCHANGE_ENABLED is false (no automatic replication
// before Milestone 5).
func TestPeerMessageExchangeEnabledDefaults(t *testing.T) {
	cfg, err := load(env(nil))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.PeerMessageExchangeEnabled {
		t.Error("PeerMessageExchangeEnabled should default to false")
	}
}

// TestPeerMessageExchangeRequiresPeerTLS verifies that enabling message
// exchange without peer mTLS fails configuration validation (Milestone 4A:
// message exchange is only available over the authenticated peer listener).
func TestPeerMessageExchangeRequiresPeerTLS(t *testing.T) {
	_, err := load(env(map[string]string{
		EnvPeerMessageExchangeEnabled: "true",
		// PeerTLSEnabled is NOT set (defaults to false).
	}))
	if err == nil {
		t.Fatal("expected error when message exchange enabled without peer mTLS, got nil")
	}
	if !contains(err.Error(), EnvPeerMessageExchangeEnabled) {
		t.Errorf("error should mention %s: %v", EnvPeerMessageExchangeEnabled, err)
	}
}

// TestPeerMessageExchangeValidWithPeerTLS verifies that enabling message
// exchange WITH peer mTLS is accepted.
func TestPeerMessageExchangeValidWithPeerTLS(t *testing.T) {
	cfg, err := load(env(map[string]string{
		EnvPeerTLSEnabled:             "true",
		EnvPeerCertFile:               "/certs/tls.crt",
		EnvPeerKeyFile:                "/certs/tls.key",
		EnvPeerTrustBundleFile:        "/certs/ca-bundle.pem",
		EnvPeerIdentitySuffix:         "gossipgrid-headless.default.svc.cluster.local",
		EnvPeerMessageExchangeEnabled: "true",
	}))
	if err != nil {
		t.Fatalf("expected valid config, got error: %v", err)
	}
	if !cfg.PeerMessageExchangeEnabled {
		t.Error("PeerMessageExchangeEnabled should be true")
	}
	if !cfg.PeerTLSEnabled {
		t.Error("PeerTLSEnabled should be true")
	}
}

// TestPeerMessageExchangeInvalidBool verifies a malformed boolean value for
// GOSSIPGRID_PEER_MESSAGE_EXCHANGE_ENABLED is rejected.
func TestPeerMessageExchangeInvalidBool(t *testing.T) {
	_, err := load(env(map[string]string{
		EnvPeerTLSEnabled:             "true",
		EnvPeerCertFile:               "/certs/tls.crt",
		EnvPeerKeyFile:                "/certs/tls.key",
		EnvPeerTrustBundleFile:        "/certs/ca-bundle.pem",
		EnvPeerIdentitySuffix:         "gossipgrid-headless.default.svc.cluster.local",
		EnvPeerMessageExchangeEnabled: "tru", // typo
	}))
	if err == nil {
		t.Fatal("expected error for invalid boolean, got nil")
	}
	if !contains(err.Error(), EnvPeerMessageExchangeEnabled) {
		t.Errorf("error should mention %s: %v", EnvPeerMessageExchangeEnabled, err)
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

// gossipBaseVars returns the env vars needed to enable gossip with valid
// prerequisites (peer mTLS, message exchange, static discovery).
func gossipBaseVars() map[string]string {
	return map[string]string{
		EnvGossipEnabled:              "true",
		EnvPeerTLSEnabled:             "true",
		EnvPeerCertFile:               "/certs/tls.crt",
		EnvPeerKeyFile:                "/certs/tls.key",
		EnvPeerTrustBundleFile:        "/certs/ca-bundle.pem",
		EnvPeerIdentitySuffix:         "gossipgrid-headless.default.svc.cluster.local",
		EnvPeerMessageExchangeEnabled: "true",
		EnvDiscoveryMode:              "static",
		EnvStaticPeers:                "a:8443,b:8443",
	}
}

func TestGossipConfigDefaults(t *testing.T) {
	cfg, err := load(env(gossipBaseVars()))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !cfg.GossipEnabled {
		t.Fatal("GossipEnabled should be true")
	}
	if cfg.GossipFanout != 2 {
		t.Errorf("GossipFanout = %d, want 2", cfg.GossipFanout)
	}
	if cfg.GossipRounds != 3 {
		t.Errorf("GossipRounds = %d, want 3", cfg.GossipRounds)
	}
	if cfg.GossipWorkers != 4 {
		t.Errorf("GossipWorkers = %d, want 4", cfg.GossipWorkers)
	}
	if cfg.GossipQueueCapacity != 256 {
		t.Errorf("GossipQueueCapacity = %d, want 256", cfg.GossipQueueCapacity)
	}
	if cfg.GossipMaxAttempts != 3 {
		t.Errorf("GossipMaxAttempts = %d, want 3", cfg.GossipMaxAttempts)
	}
	if cfg.GossipRetryBase != 250*time.Millisecond {
		t.Errorf("GossipRetryBase = %v, want 250ms", cfg.GossipRetryBase)
	}
	if cfg.GossipRetryMax != 5*time.Second {
		t.Errorf("GossipRetryMax = %v, want 5s", cfg.GossipRetryMax)
	}
}

func TestGossipDisabledIgnoresMalformedValues(t *testing.T) {
	// When gossip is disabled, malformed gossip values are ignored.
	vars := map[string]string{
		EnvGossipFanout: "not-a-number",
		EnvGossipRounds: "-5",
	}
	cfg, err := load(env(vars))
	if err != nil {
		t.Fatalf("expected no error when gossip disabled, got: %v", err)
	}
	if cfg.GossipEnabled {
		t.Error("GossipEnabled should be false")
	}
}

func TestGossipInvalidInteger(t *testing.T) {
	cases := []struct {
		name   string
		vars   map[string]string
		expect string
	}{
		{"fanout not a number", mergeVars(gossipBaseVars(), EnvGossipFanout, "abc"), EnvGossipFanout},
		{"rounds negative", mergeVars(gossipBaseVars(), EnvGossipRounds, "-1"), EnvGossipRounds},
		{"workers zero", mergeVars(gossipBaseVars(), EnvGossipWorkers, "0"), EnvGossipWorkers},
		{"queue capacity zero", mergeVars(gossipBaseVars(), EnvGossipQueueCapacity, "0"), EnvGossipQueueCapacity},
		{"max attempts zero", mergeVars(gossipBaseVars(), EnvGossipMaxAttempts, "0"), EnvGossipMaxAttempts},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(env(tc.vars))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !contains(err.Error(), tc.expect) {
				t.Errorf("error should mention %s: %v", tc.expect, err)
			}
		})
	}
}

func TestGossipOutOfRange(t *testing.T) {
	cases := []struct {
		name   string
		vars   map[string]string
		expect string
	}{
		{"fanout too high", mergeVars(gossipBaseVars(), EnvGossipFanout, "33"), EnvGossipFanout},
		{"rounds too high", mergeVars(gossipBaseVars(), EnvGossipRounds, "33"), EnvGossipRounds},
		{"workers too high", mergeVars(gossipBaseVars(), EnvGossipWorkers, "65"), EnvGossipWorkers},
		{"queue capacity too high", mergeVars(gossipBaseVars(), EnvGossipQueueCapacity, "1025"), EnvGossipQueueCapacity},
		{"max attempts too high", mergeVars(gossipBaseVars(), EnvGossipMaxAttempts, "11"), EnvGossipMaxAttempts},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(env(tc.vars))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !contains(err.Error(), tc.expect) {
				t.Errorf("error should mention %s: %v", tc.expect, err)
			}
		})
	}
}

func TestGossipInvalidDuration(t *testing.T) {
	cases := []struct {
		name   string
		vars   map[string]string
		expect string
	}{
		{"retry base not a duration", mergeVars(gossipBaseVars(), EnvGossipRetryBase, "soon"), EnvGossipRetryBase},
		{"retry base zero", mergeVars(gossipBaseVars(), EnvGossipRetryBase, "0s"), EnvGossipRetryBase},
		{"retry max not a duration", mergeVars(gossipBaseVars(), EnvGossipRetryMax, "x"), EnvGossipRetryMax},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(env(tc.vars))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !contains(err.Error(), tc.expect) {
				t.Errorf("error should mention %s: %v", tc.expect, err)
			}
		})
	}
}

func TestGossipValidOverrides(t *testing.T) {
	cfg, err := load(env(mergeVars(gossipBaseVars(),
		EnvGossipFanout, "4",
		EnvGossipRounds, "2",
		EnvGossipWorkers, "8",
		EnvGossipQueueCapacity, "512",
		EnvGossipMaxAttempts, "5",
		EnvGossipRetryBase, "100ms",
		EnvGossipRetryMax, "2s",
	)))
	if err != nil {
		t.Fatalf("expected valid config, got: %v", err)
	}
	if cfg.GossipFanout != 4 || cfg.GossipRounds != 2 || cfg.GossipWorkers != 8 {
		t.Errorf("fanout/rounds/workers = %d/%d/%d, want 4/2/8", cfg.GossipFanout, cfg.GossipRounds, cfg.GossipWorkers)
	}
	if cfg.GossipQueueCapacity != 512 || cfg.GossipMaxAttempts != 5 {
		t.Errorf("queue/maxAttempts = %d/%d, want 512/5", cfg.GossipQueueCapacity, cfg.GossipMaxAttempts)
	}
	if cfg.GossipRetryBase != 100*time.Millisecond || cfg.GossipRetryMax != 2*time.Second {
		t.Errorf("retryBase/max = %v/%v, want 100ms/2s", cfg.GossipRetryBase, cfg.GossipRetryMax)
	}
}

// TestMessageRetentionDefaultsDisabled verifies that message retention is
// disabled by default (TTL zero) when the TTL variable is unset, and that no
// cleanup interval is parsed in that case (the default 1h is not applied).
func TestMessageRetentionDefaultsDisabled(t *testing.T) {
	cfg, err := load(env(nil))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.MessageRetentionTTL != 0 {
		t.Errorf("default TTL = %v, want 0 (disabled)", cfg.MessageRetentionTTL)
	}
	// The interval is only set when retention is enabled; when disabled it
	// remains the zero value.
	if cfg.MessageRetentionInterval != 0 {
		t.Errorf("default interval = %v, want 0 (not applied when disabled)", cfg.MessageRetentionInterval)
	}
}

// TestMessageRetentionEnabled verifies that a non-zero TTL enables retention
// with the documented default cleanup interval (1h), and that an explicit
// interval override is honoured.
func TestMessageRetentionEnabled(t *testing.T) {
	cfg, err := load(env(map[string]string{
		EnvMessageRetentionTTL: "168h", // 7 days
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.MessageRetentionTTL != 7*24*time.Hour {
		t.Errorf("TTL = %v, want 168h (7 days)", cfg.MessageRetentionTTL)
	}
	if cfg.MessageRetentionInterval != time.Hour {
		t.Errorf("default interval = %v, want 1h", cfg.MessageRetentionInterval)
	}

	cfg2, err := load(env(map[string]string{
		EnvMessageRetentionTTL:      "168h",
		EnvMessageRetentionInterval: "30m",
	}))
	if err != nil {
		t.Fatalf("load (interval override): %v", err)
	}
	if cfg2.MessageRetentionInterval != 30*time.Minute {
		t.Errorf("interval override = %v, want 30m", cfg2.MessageRetentionInterval)
	}
}

// TestMessageRetentionSweepBudgets verifies that the optional sweep-budget
// env vars (max rows and max duration) are parsed, default to 0 (the "use the
// documented defaults" sentinel) when unset, and are honoured when set. A
// negative value is rejected (it would disable the bound entirely).
func TestMessageRetentionSweepBudgets(t *testing.T) {
	// Defaults: unset budgets are 0 (the cleaner applies the documented
	// defaults).
	cfg, err := load(env(map[string]string{
		EnvMessageRetentionTTL: "168h",
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.MessageRetentionMaxSweepRows != 0 {
		t.Errorf("default MaxSweepRows = %d, want 0 (sentinel for documented default)", cfg.MessageRetentionMaxSweepRows)
	}
	if cfg.MessageRetentionMaxSweepDuration != 0 {
		t.Errorf("default MaxSweepDuration = %v, want 0 (sentinel for documented default)", cfg.MessageRetentionMaxSweepDuration)
	}

	// Overrides are honoured.
	cfg2, err := load(env(map[string]string{
		EnvMessageRetentionTTL:              "168h",
		EnvMessageRetentionMaxSweepRows:     "2000",
		EnvMessageRetentionMaxSweepDuration: "5s",
	}))
	if err != nil {
		t.Fatalf("load (budget overrides): %v", err)
	}
	if cfg2.MessageRetentionMaxSweepRows != 2000 {
		t.Errorf("MaxSweepRows override = %d, want 2000", cfg2.MessageRetentionMaxSweepRows)
	}
	if cfg2.MessageRetentionMaxSweepDuration != 5*time.Second {
		t.Errorf("MaxSweepDuration override = %v, want 5s", cfg2.MessageRetentionMaxSweepDuration)
	}

	// A negative value is rejected (it would disable the bound).
	_, err = load(env(map[string]string{
		EnvMessageRetentionTTL:          "168h",
		EnvMessageRetentionMaxSweepRows: "-5",
	}))
	if err == nil {
		t.Error("negative MaxSweepRows: expected error, got nil")
	}
}

// TestMessageRetentionDisabledExplicitZero verifies that an explicit "0" TTL
// disables retention (the documented off value) without error.
func TestMessageRetentionDisabledExplicitZero(t *testing.T) {
	cfg, err := load(env(map[string]string{
		EnvMessageRetentionTTL: "0",
	}))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.MessageRetentionTTL != 0 {
		t.Errorf("TTL = %v, want 0 (explicit 0 disables)", cfg.MessageRetentionTTL)
	}
}

// TestMessageRetentionInvalidValues verifies that malformed or out-of-range
// retention values are rejected (a typo must not silently change a data
// lifetime policy).
func TestMessageRetentionInvalidValues(t *testing.T) {
	cases := []struct {
		name string
		vars map[string]string
	}{
		{"malformed TTL", map[string]string{EnvMessageRetentionTTL: "soon"}},
		{"negative TTL", map[string]string{EnvMessageRetentionTTL: "-5m"}},
		{"sub-minute TTL", map[string]string{EnvMessageRetentionTTL: "30s"}},
		{"malformed interval", map[string]string{EnvMessageRetentionTTL: "1h", EnvMessageRetentionInterval: "soon"}},
		{"zero interval", map[string]string{EnvMessageRetentionTTL: "1h", EnvMessageRetentionInterval: "0s"}},
		{"interval too small", map[string]string{EnvMessageRetentionTTL: "1h", EnvMessageRetentionInterval: "500ms"}},
		{"interval too large", map[string]string{EnvMessageRetentionTTL: "1h", EnvMessageRetentionInterval: "48h"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := load(env(c.vars))
			if err == nil {
				t.Errorf("expected an error for %s", c.name)
			}
		})
	}
}

// TestMessageRetentionValidationUnit exercises Config-level validation directly
// (the function the cleaner also uses).
func TestMessageRetentionValidationUnit(t *testing.T) {
	valid := Config{MessageRetentionTTL: 7 * 24 * time.Hour, MessageRetentionInterval: time.Hour}
	if err := validateMessageRetentionConfig(&valid); err != nil {
		t.Errorf("valid retention: err = %v, want nil", err)
	}
	disabled := Config{MessageRetentionTTL: 0}
	if err := validateMessageRetentionConfig(&disabled); err != nil {
		t.Errorf("disabled retention: err = %v, want nil", err)
	}
	tooSmall := Config{MessageRetentionTTL: 30 * time.Second, MessageRetentionInterval: time.Hour}
	if err := validateMessageRetentionConfig(&tooSmall); err == nil {
		t.Error("sub-minute TTL: expected an error, got nil")
	}
}

// mergeVars returns a new map with the given key-value pairs added to base.
func mergeVars(base map[string]string, kv ...string) map[string]string {
	out := make(map[string]string, len(base)+len(kv)/2)
	for k, v := range base {
		out[k] = v
	}
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	return out
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
