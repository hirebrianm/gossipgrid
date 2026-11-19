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
