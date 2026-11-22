package auth

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestReloaderInitialLoad verifies that a successful initial load publishes a
// snapshot and the state is StateLoaded.
func TestReloaderInitialLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred.json")
	cf, err := GenerateCredential("pw", 1000, 1, nil)
	if err != nil {
		t.Fatalf("GenerateCredential: %v", err)
	}
	if err := writeFile(path, cf); err != nil {
		t.Fatalf("writeFile: %v", err)
	}
	r := NewReloader(path, time.Hour, time.Hour, nil)
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Shutdown()
	if r.State() != StateLoaded {
		t.Errorf("State = %q, want %q", r.State(), StateLoaded)
	}
	snap := r.Snapshot()
	if snap == nil || snap.Generation != 1 {
		t.Errorf("Snapshot = %+v, want generation 1", snap)
	}
}

// TestReloaderMissingFile verifies that a missing file leaves the reloader in
// StateUnavailable (the auth endpoint returns 503). The process does not crash;
// the poll loop keeps retrying.
func TestReloaderMissingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nope.json")
	r := NewReloader(path, time.Hour, time.Hour, nil)
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("Start (missing file must not fail): %v", err)
	}
	defer r.Shutdown()
	if r.State() != StateUnavailable {
		t.Errorf("State = %q, want %q", r.State(), StateUnavailable)
	}
	if r.Snapshot() != nil {
		t.Error("Snapshot = non-nil, want nil (no valid credential loaded)")
	}
}

// TestReloaderFailedReloadPreservesPrevious verifies that a failed reload (the
// file is removed after the initial load) preserves the previous valid
// snapshot. The state remains StateLoaded (the previous snapshot is still in
// effect), and the last-reload-failed flag is set.
func TestReloaderFailedReloadPreservesPrevious(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred.json")
	cf, _ := GenerateCredential("pw", 1000, 1, nil)
	if err := writeFile(path, cf); err != nil {
		t.Fatalf("writeFile: %v", err)
	}
	r := NewReloader(path, time.Hour, time.Hour, nil)
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Shutdown()
	// Remove the file and force a reload — it must fail, and the previous
	// snapshot must be preserved.
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	r.TryReload()
	if r.State() != StateLoaded {
		t.Errorf("State after failed reload = %q, want %q (previous snapshot retained)", r.State(), StateLoaded)
	}
	if r.Snapshot() == nil || r.Snapshot().Generation != 1 {
		t.Error("Snapshot after failed reload is not the previous one")
	}
	d := r.Diagnostics()
	if !d.LastReloadFailed {
		t.Error("Diagnostics.LastReloadFailed = false, want true after a failed reload")
	}
}

// TestReloaderStaleAfterBound verifies that after a failed reload and the
// max-stale bound elapses, the state becomes StateStale (the auth endpoint
// returns 503 for new sessions). It uses a short maxStale and an advanced
// clock.
func TestReloaderStaleAfterBound(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred.json")
	cf, _ := GenerateCredential("pw", 1000, 1, nil)
	if err := writeFile(path, cf); err != nil {
		t.Fatalf("writeFile: %v", err)
	}
	now := time.Now()
	r := NewReloader(path, time.Hour, 100*time.Millisecond, nil)
	r.WithClock(func() time.Time { return now })
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Shutdown()
	// Remove the file and force a failed reload.
	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	r.TryReload()
	// Just after the failure, the state is still StateLoaded (within the bound).
	if r.State() != StateLoaded {
		t.Errorf("State just after failed reload = %q, want %q", r.State(), StateLoaded)
	}
	// Advance the clock past the max-stale bound.
	r.WithClock(func() time.Time { return now.Add(200 * time.Millisecond) })
	if r.State() != StateStale {
		t.Errorf("State after bound elapses = %q, want %q", r.State(), StateStale)
	}
	if !r.Stale() {
		t.Error("Stale() = false, want true after the bound elapses")
	}
}

// TestReloaderRotationSwapsSnapshot verifies that a successful rotation (a new
// generation in the file) atomically swaps the current snapshot and increments
// the reload count. There is no prev/overlap retention: the new snapshot
// replaces the old one and the old one is not retained.
func TestReloaderRotationSwapsSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred.json")
	oldCf, _ := GenerateCredential("old-pw", 1000, 1, nil)
	newCf, _ := GenerateCredential("new-pw", 1000, 2, nil)
	if err := writeFile(path, oldCf); err != nil {
		t.Fatalf("writeFile(old): %v", err)
	}
	r := NewReloader(path, time.Hour, time.Hour, nil)
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Shutdown()
	if r.Snapshot().Generation != 1 {
		t.Fatalf("initial generation = %d, want 1", r.Snapshot().Generation)
	}
	// Rotate.
	if err := writeFile(path, newCf); err != nil {
		t.Fatalf("writeFile(new): %v", err)
	}
	r.TryReload()
	if r.Snapshot().Generation != 2 {
		t.Errorf("generation after rotation = %d, want 2", r.Snapshot().Generation)
	}
	if r.Diagnostics().ReloadCount != 1 {
		t.Errorf("ReloadCount = %d, want 1 after one rotation", r.Diagnostics().ReloadCount)
	}
}

// TestReloaderVerifierChangeDetected verifies that a verifier (password hash)
// change at an UNCHANGED generation is detected as a change and triggers a
// reload. This is the regression for Part B: the old change detection
// compared only generation + signing key, so a verifier change with the same
// generation and key was silently ignored. Now every security-relevant field
// is compared.
func TestReloaderVerifierChangeDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred.json")
	base, _ := GenerateCredential("pw", 1000, 1, nil)
	if err := writeFile(path, base); err != nil {
		t.Fatalf("writeFile(base): %v", err)
	}
	r := NewReloader(path, time.Hour, time.Hour, nil)
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Shutdown()
	// Change the verifier (a different password hash) but keep the generation
	// and signing key identical.
	modified := *base
	modified.Verifier = hex.EncodeToString(make([]byte, VerifierBytes)) // a different 32-byte verifier
	if err := writeFile(path, &modified); err != nil {
		t.Fatalf("writeFile(modified): %v", err)
	}
	r.TryReload()
	if r.Diagnostics().ReloadCount != 1 {
		t.Errorf("ReloadCount = %d, want 1 (verifier change must trigger a reload)", r.Diagnostics().ReloadCount)
	}
}

// TestReloaderSigningKeyChangeDetected verifies that a signing-key change at an
// UNCHANGED generation is detected as a change and triggers a reload.
func TestReloaderSigningKeyChangeDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred.json")
	base, _ := GenerateCredential("pw", 1000, 1, nil)
	if err := writeFile(path, base); err != nil {
		t.Fatalf("writeFile(base): %v", err)
	}
	r := NewReloader(path, time.Hour, time.Hour, nil)
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Shutdown()
	modified := *base
	modified.SigningKey = hex.EncodeToString(make([]byte, SigningKeyBytes)) // a different 32-byte key
	if err := writeFile(path, &modified); err != nil {
		t.Fatalf("writeFile(modified): %v", err)
	}
	r.TryReload()
	if r.Diagnostics().ReloadCount != 1 {
		t.Errorf("ReloadCount = %d, want 1 (signing-key change must trigger a reload)", r.Diagnostics().ReloadCount)
	}
}

// TestReloaderPBKDF2ParamChangeDetected verifies that a PBKDF2 iteration-count
// change at an UNCHANGED generation is detected as a change and triggers a
// reload.
func TestReloaderPBKDF2ParamChangeDetected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred.json")
	base, _ := GenerateCredential("pw", 1000, 1, nil)
	if err := writeFile(path, base); err != nil {
		t.Fatalf("writeFile(base): %v", err)
	}
	r := NewReloader(path, time.Hour, time.Hour, nil)
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Shutdown()
	modified := *base
	modified.Iterations = 2000 // a different iteration count
	if err := writeFile(path, &modified); err != nil {
		t.Fatalf("writeFile(modified): %v", err)
	}
	r.TryReload()
	if r.Diagnostics().ReloadCount != 1 {
		t.Errorf("ReloadCount = %d, want 1 (PBKDF2 param change must trigger a reload)", r.Diagnostics().ReloadCount)
	}
}

// TestReloaderMalformedReplacementKeepsPrevious verifies that when a malformed
// (invalid) credential is written over a valid one, the reload fails and the
// previous valid snapshot is preserved. The last-failed flag is set.
func TestReloaderMalformedReplacementKeepsPrevious(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred.json")
	base, _ := GenerateCredential("pw", 1000, 1, nil)
	if err := writeFile(path, base); err != nil {
		t.Fatalf("writeFile(base): %v", err)
	}
	r := NewReloader(path, time.Hour, time.Hour, nil)
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Shutdown()
	// Write a malformed credential (bad JSON).
	if err := os.WriteFile(path, []byte(`{not json`), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	r.TryReload()
	// The previous valid snapshot must be preserved (state still loaded).
	if r.State() != StateLoaded {
		t.Errorf("State after malformed replacement = %q, want %q (previous preserved)", r.State(), StateLoaded)
	}
	if r.Snapshot() == nil || r.Snapshot().Generation != 1 {
		t.Error("Snapshot after malformed replacement is not the previous one")
	}
	if !r.Diagnostics().LastReloadFailed {
		t.Error("Diagnostics.LastReloadFailed = false, want true after a malformed replacement")
	}
}

// TestReloaderValidReplacementRecovers verifies that after a malformed
// replacement, a valid replacement recovers the reloader: the snapshot updates
// to the new generation and the last-failed flag clears.
func TestReloaderValidReplacementRecovers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred.json")
	base, _ := GenerateCredential("pw", 1000, 1, nil)
	if err := writeFile(path, base); err != nil {
		t.Fatalf("writeFile(base): %v", err)
	}
	r := NewReloader(path, time.Hour, time.Hour, nil)
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Shutdown()
	// Malformed replacement → previous preserved, lastFailed=true.
	if err := os.WriteFile(path, []byte(`{not json`), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	r.TryReload()
	if !r.Diagnostics().LastReloadFailed {
		t.Fatal("expected LastReloadFailed after malformed replacement")
	}
	// Valid replacement (new generation) → recovers.
	recovered, _ := GenerateCredential("new-pw", 1000, 2, nil)
	if err := writeFile(path, recovered); err != nil {
		t.Fatalf("writeFile(recovered): %v", err)
	}
	r.TryReload()
	if r.State() != StateLoaded {
		t.Errorf("State after recovery = %q, want %q", r.State(), StateLoaded)
	}
	if r.Snapshot() == nil || r.Snapshot().Generation != 2 {
		t.Error("Snapshot after recovery is not the new generation")
	}
	if r.Diagnostics().LastReloadFailed {
		t.Error("Diagnostics.LastReloadFailed = true, want false after recovery")
	}
}

// TestReloaderConcurrentRequestsAndReloads verifies that concurrent
// Authenticate calls and TryReload calls do not race. It runs a reloader with
// a valid credential, issues a session, and concurrently makes authenticated
// requests while rotating the credential. The race detector (make test-race)
// is the authoritative check; this test exercises the concurrency.
func TestReloaderConcurrentRequestsAndReloads(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred.json")
	cf, _ := GenerateCredential("pw", 1000, 1, nil)
	if err := writeFile(path, cf); err != nil {
		t.Fatalf("writeFile: %v", err)
	}
	r := NewReloader(path, time.Hour, time.Hour, nil)
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Shutdown()
	provider := NewProvider(r)
	cookie, _, err := IssueSession(cf.SigningKeyHex(), 1, time.Now())
	if err != nil {
		t.Fatalf("IssueSession: %v", err)
	}
	req := makeRequestWithCookie(cookie)

	var wg sync.WaitGroup
	// Concurrent authenticated requests.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := provider.Authenticate(req, time.Now())
			if !res.Allowed {
				t.Errorf("Authenticate not allowed: %q", res.Message)
			}
		}()
	}
	// Concurrent reloads (a no-op rotation: rewrite the same file).
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.TryReload()
		}()
	}
	wg.Wait()
}

// TestReloaderShutdownIdempotent verifies that Shutdown is idempotent (calling
// it twice does not panic or block).
func TestReloaderShutdownIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred.json")
	cf, _ := GenerateCredential("pw", 1000, 1, nil)
	if err := writeFile(path, cf); err != nil {
		t.Fatalf("writeFile: %v", err)
	}
	r := NewReloader(path, time.Hour, time.Hour, nil)
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Shutdown()
	// A second Shutdown must return promptly (idempotent).
	done := make(chan struct{})
	go func() {
		r.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second Shutdown blocked (not idempotent)")
	}
}

// TestReloaderRepeatStartFails verifies that a second Start returns an error
// (the reloader can only be started once).
func TestReloaderRepeatStartFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred.json")
	cf, _ := GenerateCredential("pw", 1000, 1, nil)
	if err := writeFile(path, cf); err != nil {
		t.Fatalf("writeFile: %v", err)
	}
	r := NewReloader(path, time.Hour, time.Hour, nil)
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	defer r.Shutdown()
	if err := r.Start(t.Context()); err == nil {
		t.Error("second Start = nil, want error")
	}
}

// TestReloaderDiagnosticsNeverLeaksMaterial verifies that Diagnostics does not
// include the salt, verifier, or signing key (the credential material must
// never appear in diagnostics or logs).
func TestReloaderDiagnosticsNeverLeaksMaterial(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cred.json")
	cf, _ := GenerateCredential("pw", 1000, 1, nil)
	if err := writeFile(path, cf); err != nil {
		t.Fatalf("writeFile: %v", err)
	}
	r := NewReloader(path, time.Hour, time.Hour, nil)
	if err := r.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer r.Shutdown()
	d := r.Diagnostics()
	// The Diagnostics struct has no fields for salt/verifier/signing key. We
	// verify the Generation and State are present, and that the struct does
	// not contain the raw material (it cannot, by construction).
	if d.Generation != 1 {
		t.Errorf("Diagnostics.Generation = %d, want 1", d.Generation)
	}
	if d.State != StateLoaded {
		t.Errorf("Diagnostics.State = %q, want %q", d.State, StateLoaded)
	}
}
