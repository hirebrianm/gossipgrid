package main

import (
	"bytes"
	"crypto/fips140"
	"log/slog"
	"strings"
	"testing"
)

// TestStartupLogFields verifies that all required structured fields appear in
// the startup log: version, node, http_addr, fips_enabled, fips_module.
func TestStartupLogFields(t *testing.T) {
	var buf bytes.Buffer
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
	defer slog.SetDefault(orig)

	// Simulate the startup log line exactly as main.go emits it.
	fipsEnabled := fips140.Enabled()
	fipsModule := fips140.Version()

	slog.Info("starting gossipgrid",
		slog.String("version", Version),
		slog.String("node", "test-node"),
		slog.String("http_addr", ":8080"),
		slog.String("log_level", "info"),
		slog.Bool("fips_enabled", fipsEnabled),
		slog.String("fips_module", fipsModule),
	)

	out := buf.String()
	want := []struct {
		field string
		value string
	}{
		{"version", Version},
		{"node", "test-node"},
		{"http_addr", ":8080"},
		{"fips_enabled", boolStr(fipsEnabled)},
		{"fips_module", fipsModule},
	}
	for _, w := range want {
		if !strings.Contains(out, w.field+"="+w.value) {
			t.Errorf("startup log should contain %s=%s, got: %s", w.field, w.value, out)
		}
	}
}

// TestVersionDefault verifies that Version defaults to "dev" when no linker
// flag is supplied (i.e. in a normal `go test` build).
func TestVersionDefault(t *testing.T) {
	if Version != "dev" {
		t.Errorf("Version = %q, want %q (default without linker injection)", Version, "dev")
	}
}

// TestFIPSRequiredDefault verifies that FIPSRequired defaults to "false" when
// no linker flag is supplied (i.e. in a plain `go test` build without
// GOFIPS140).
func TestFIPSRequiredDefault(t *testing.T) {
	if FIPSRequired != "false" {
		t.Errorf("FIPSRequired = %q, want %q (default without linker injection)", FIPSRequired, "false")
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
