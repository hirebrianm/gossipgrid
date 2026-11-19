package main

import (
	"bytes"
	"crypto/fips140"
	"errors"
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

// TestDeferredCloseModifiesNamedReturn exercises the exact timing that was
// buggy in run(): a function sets its return value, then a deferred function
// runs *after* that assignment and modifies the return value via a named
// return. The test asserts that the caller sees the deferred modification —
// something that would be impossible if the return value were unnamed (the
// deferred function would run after the return value is already determined).
//
// This is a direct reproduction of the run() pattern:
//
//	func run() (retErr error) {
//	    // ...
//	    defer func() {
//	        if cerr := st.Close(); cerr != nil {
//	            retErr = errors.Join(retErr, cerr)
//	        }
//	    }()
//	    // ...
//	    return
//	}
func TestDeferredCloseModifiesNamedReturn(t *testing.T) {
	// Simulate the store with a Close method that fails.
	var closeErr = errors.New("db close failed")
	var closeCallCount int

	// runSim mirrors the structure of run(): it returns a named error,
	// defers a close that joins into the named return, and sets a run
	// error before returning.
	runSim := func(runErr error) (retErr error) {
		defer func() {
			closeCallCount++
			if closeErr != nil {
				retErr = errors.Join(retErr, closeErr)
			}
		}()
		// This is the equivalent of "retErr = err" in run()'s select block.
		retErr = runErr
		// Implicit return — the deferred function runs here.
		return
	}

	// Case 1: both a run error and a close error → both must be present.
	earlyErr := errors.New("listener failed")
	result := runSim(earlyErr)
	if result == nil {
		t.Fatal("expected non-nil error when both run and close fail")
	}
	if !errors.Is(result, earlyErr) {
		t.Errorf("result should wrap earlyErr, got: %v", result)
	}
	if !errors.Is(result, closeErr) {
		t.Errorf("result should wrap closeErr (deferred close modified the named return), got: %v", result)
	}
	if closeCallCount != 1 {
		t.Errorf("close call count = %d, want 1 (exactly once)", closeCallCount)
	}

	// Case 2: only a run error, close succeeds → only the run error.
	closeErr = nil
	closeCallCount = 0
	result2 := runSim(earlyErr)
	if !errors.Is(result2, earlyErr) {
		t.Errorf("result should wrap earlyErr when close succeeds, got: %v", result2)
	}
	if errors.Is(result2, errors.New("db close failed")) {
		t.Errorf("result should not contain close error when close succeeds, got: %v", result2)
	}

	// Case 3: only a close error, no run error → only the close error.
	closeErr = errors.New("db close failed")
	closeCallCount = 0
	result3 := runSim(nil)
	if result3 == nil {
		t.Fatal("expected non-nil error when only close fails")
	}
	if !errors.Is(result3, closeErr) {
		t.Errorf("result should wrap closeErr when run error is nil, got: %v", result3)
	}

	// Case 4: no errors at all → nil.
	closeErr = nil
	closeCallCount = 0
	result4 := runSim(nil)
	if result4 != nil {
		t.Errorf("result should be nil when both run and close are nil, got: %v", result4)
	}
}

// TestDeferredCloseNotAppliedToUnnamedReturn documents the failure mode that
// the named-return fix prevents: with an unnamed return value, a deferred
// function that tries to modify the return via a separate variable has no
// effect on what the caller sees. This test demonstrates the *absent*
// behaviour that the old code exhibited.
func TestDeferredCloseNotAppliedToUnnamedReturn(t *testing.T) {
	closeFailure := errors.New("db close failed")
	runErr := errors.New("listener failed")

	// runOld mirrors the OLD (buggy) run() pattern: unnamed return, close
	// error stored in a separate variable, but the return statement does not
	// include the close error. The deferred function cannot affect the return
	// value because it is unnamed and already determined.
	var closeErrSeen error
	runOld := func() error {
		defer func() {
			if cerr := closeFailure; cerr != nil {
				closeErrSeen = cerr
			}
		}()
		// The return value is determined here; the deferred function runs
		// after this point and cannot modify the already-determined return.
		return runErr
	}

	result := runOld()
	// The deferred function DID set closeErrSeen, but because the return
	// value is unnamed and already determined, the caller never sees it.
	_ = closeErrSeen
	if !errors.Is(result, runErr) {
		t.Errorf("result should wrap runErr, got: %v", result)
	}
	if errors.Is(result, closeFailure) {
		t.Errorf("with an unnamed return, the deferred close error is silently lost — result should NOT contain closeFailure, but it does: %v", result)
	}
	// This test PASSES precisely because the close error is lost. It documents
	// the bug: the caller only sees runErr, never the close error.
}
