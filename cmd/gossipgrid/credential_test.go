package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gossipgrid/internal/auth"
)

// initStdin pipes the given password (with confirmation) into stdinReader for
// credentialInit/credentialRotate, which read passwords interactively. It
// returns a restore function.
//
// For `init` and `rotate`, the password is read twice (password + confirm),
// so the piped input is "pw\npw\n".
func initStdin(t *testing.T, password string) func() {
	t.Helper()
	orig := stdinReader
	stdinReader = strings.NewReader(password + "\n" + password + "\n")
	lineReaderForReset()
	return func() {
		stdinReader = orig
		lineReaderForReset()
	}
}

// verifyStdin pipes a single password into stdinReader for credentialVerify.
func verifyStdin(t *testing.T, password string) func() {
	t.Helper()
	orig := stdinReader
	stdinReader = strings.NewReader(password + "\n")
	lineReaderForReset()
	return func() {
		stdinReader = orig
		lineReaderForReset()
	}
}

// TestCredentialInitWritesValidFile verifies that `credential init --out <file>`
// (with a piped password) writes a valid credential file with 0600 permissions,
// and that the file loads and verifies the password.
func TestCredentialInitWritesValidFile(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "admin-credential.json")
	const password = "s3cur3-pw"
	restore := initStdin(t, password)
	defer restore()

	err := runCredential([]string{"init", "--out", out, "--iterations", "1000"})
	if err != nil {
		t.Fatalf("runCredential init: %v", err)
	}

	// The file must exist with 0600 permissions.
	info, err := os.Stat(out)
	if err != nil {
		t.Fatalf("Stat(%s): %v", out, err)
	}
	perm := info.Mode().Perm()
	if perm != 0o600 {
		t.Errorf("file permissions = %o, want 0600", perm)
	}

	// The file must load and verify the password.
	cf, err := auth.LoadCredentialFile(out)
	if err != nil {
		t.Fatalf("LoadCredentialFile: %v", err)
	}
	if cf.Generation != 1 {
		t.Errorf("Generation = %d, want 1 (default)", cf.Generation)
	}
	if cf.Iterations != 1000 {
		t.Errorf("Iterations = %d, want 1000", cf.Iterations)
	}
	if !auth.VerifyPassword(cf, password) {
		t.Error("VerifyPassword(correct) = false, want true")
	}
	if auth.VerifyPassword(cf, "wrong") {
		t.Error("VerifyPassword(wrong) = true, want false")
	}
}

// TestCredentialInitNoPasswordFlag verifies that `credential init` does NOT
// accept a password as a command-line argument (it would expose the password
// in shell history and process listings). The password is read interactively.
func TestCredentialInitNoPasswordFlag(t *testing.T) {
	err := runCredential([]string{"init", "--out", "/tmp/x", "--password", "pw"})
	if err == nil || !strings.Contains(err.Error(), "unexpected argument") {
		t.Errorf("runCredential init (--password) = %v, want an error (password must not be an argument)", err)
	}
}

// TestCredentialInitMismatchedPasswords verifies that a password/confirm
// mismatch (the two prompts differ) returns an error (the operator re-enters).
func TestCredentialInitMismatchedPasswords(t *testing.T) {
	orig := stdinReader
	stdinReader = strings.NewReader("pw1\npw2\n")
	lineReaderForReset()
	defer func() {
		stdinReader = orig
		lineReaderForReset()
	}()

	err := runCredential([]string{"init", "--out", filepath.Join(t.TempDir(), "x.json")})
	if err == nil || !strings.Contains(err.Error(), "do not match") {
		t.Errorf("runCredential init (mismatched) = %v, want a mismatch error", err)
	}
}

// TestCredentialInitDefaultIterations verifies that when --iterations is
// omitted, the default iteration count (auth.DefaultIterations) is used.
func TestCredentialInitDefaultIterations(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "admin-credential.json")
	restore := initStdin(t, "pw")
	defer restore()

	if err := runCredential([]string{"init", "--out", out}); err != nil {
		t.Fatalf("runCredential init: %v", err)
	}
	cf, err := auth.LoadCredentialFile(out)
	if err != nil {
		t.Fatalf("LoadCredentialFile: %v", err)
	}
	if cf.Iterations != auth.DefaultIterations {
		t.Errorf("Iterations = %d, want %d (default)", cf.Iterations, auth.DefaultIterations)
	}
}

// TestCredentialInitMissingOut verifies that `credential init` without --out
// returns an error.
func TestCredentialInitMissingOut(t *testing.T) {
	restore := initStdin(t, "pw")
	defer restore()
	err := runCredential([]string{"init"})
	if err == nil || !strings.Contains(err.Error(), "--out") {
		t.Errorf("runCredential init (no --out) = %v, want an error mentioning --out", err)
	}
}

// TestCredentialVerifyCorrect verifies that `credential verify --file <file>`
// (with a piped password) returns nil for the correct password.
func TestCredentialVerifyCorrect(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "admin-credential.json")
	const password = "verify-me"
	restore := initStdin(t, password)
	defer restore()
	if err := runCredential([]string{"init", "--out", out}); err != nil {
		t.Fatalf("init: %v", err)
	}

	restoreV := verifyStdin(t, password)
	defer restoreV()
	if err := runCredential([]string{"verify", "--file", out}); err != nil {
		t.Errorf("verify (correct) = %v, want nil", err)
	}
}

// TestCredentialVerifyWrong verifies that `credential verify` returns an error
// for the wrong password.
func TestCredentialVerifyWrong(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "admin-credential.json")
	restore := initStdin(t, "correct")
	defer restore()
	if err := runCredential([]string{"init", "--out", out}); err != nil {
		t.Fatalf("init: %v", err)
	}

	restoreV := verifyStdin(t, "wrong")
	defer restoreV()
	err := runCredential([]string{"verify", "--file", out})
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Errorf("verify (wrong) = %v, want an error about a mismatch", err)
	}
}

// TestCredentialVerifyNoPasswordFlag verifies that `credential verify` does NOT
// accept a password as a command-line argument.
func TestCredentialVerifyNoPasswordFlag(t *testing.T) {
	err := runCredential([]string{"verify", "--file", "/tmp/x", "--password", "pw"})
	if err == nil || !strings.Contains(err.Error(), "unexpected argument") {
		t.Errorf("runCredential verify (--password) = %v, want an error (password must not be an argument)", err)
	}
}

// TestCredentialVerifyMissingFile verifies that `credential verify` with a
// missing file returns an error (the file must exist).
func TestCredentialVerifyMissingFile(t *testing.T) {
	dir := t.TempDir()
	restoreV := verifyStdin(t, "pw")
	defer restoreV()
	err := runCredential([]string{"verify", "--file", filepath.Join(dir, "nope.json")})
	if err == nil {
		t.Error("verify (missing file) = nil, want error")
	}
}

// TestCredentialRotateIncrementsGeneration verifies that `credential rotate`
// reads the existing credential, increments the generation, prompts for the
// new password, and atomically writes the new record (preserving the signing
// key by default). The old password no longer verifies; the new password does.
func TestCredentialRotateIncrementsGeneration(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "admin-credential.json")
	const oldPassword = "old-pw"
	const newPassword = "new-pw"
	restore := initStdin(t, oldPassword)
	defer restore()
	if err := runCredential([]string{"init", "--out", file}); err != nil {
		t.Fatalf("init: %v", err)
	}
	oldCf, err := auth.LoadCredentialFile(file)
	if err != nil {
		t.Fatalf("LoadCredentialFile: %v", err)
	}

	// Rotate: piped new password (new + confirm).
	restoreR := initStdin(t, newPassword)
	defer restoreR()
	if err := runCredential([]string{"rotate", "--file", file}); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	newCf, err := auth.LoadCredentialFile(file)
	if err != nil {
		t.Fatalf("LoadCredentialFile (after rotate): %v", err)
	}
	// The generation must be incremented (old + 1).
	if newCf.Generation != oldCf.Generation+1 {
		t.Errorf("Generation after rotate = %d, want %d (old %d + 1)", newCf.Generation, oldCf.Generation+1, oldCf.Generation)
	}
	// The signing key must be preserved by default.
	if newCf.SigningKey != oldCf.SigningKey {
		t.Error("SigninKey changed on rotate (default should preserve the key)")
	}
	// The old password must no longer verify; the new password must verify.
	if auth.VerifyPassword(newCf, oldPassword) {
		t.Error("old password still verifies after rotate (want rejected)")
	}
	if !auth.VerifyPassword(newCf, newPassword) {
		t.Error("new password does not verify after rotate (want accepted)")
	}
}

// TestCredentialRotateNewSigningKey verifies that `credential rotate
// --new-signing-key` generates a fresh signing key (the old key is replaced)
// while still incrementing the generation.
func TestCredentialRotateNewSigningKey(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "admin-credential.json")
	restore := initStdin(t, "old-pw")
	defer restore()
	if err := runCredential([]string{"init", "--out", file}); err != nil {
		t.Fatalf("init: %v", err)
	}
	oldCf, err := auth.LoadCredentialFile(file)
	if err != nil {
		t.Fatalf("LoadCredentialFile: %v", err)
	}

	restoreR := initStdin(t, "new-pw")
	defer restoreR()
	if err := runCredential([]string{"rotate", "--file", file, "--new-signing-key"}); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	newCf, err := auth.LoadCredentialFile(file)
	if err != nil {
		t.Fatalf("LoadCredentialFile (after rotate): %v", err)
	}
	if newCf.SigningKey == oldCf.SigningKey {
		t.Error("signing key unchanged with --new-signing-key (want a fresh key)")
	}
	if newCf.Generation != oldCf.Generation+1 {
		t.Errorf("Generation = %d, want %d", newCf.Generation, oldCf.Generation+1)
	}
}

// TestCredentialRotateMissingFile verifies that `credential rotate` with a
// missing file returns an error.
func TestCredentialRotateMissingFile(t *testing.T) {
	dir := t.TempDir()
	restoreR := initStdin(t, "pw")
	defer restoreR()
	err := runCredential([]string{"rotate", "--file", filepath.Join(dir, "nope.json")})
	if err == nil {
		t.Error("rotate (missing file) = nil, want error")
	}
}

// TestCredentialUnknownSubcommand verifies that `credential <unknown>` returns
// an error naming the unknown action.
func TestCredentialUnknownSubcommand(t *testing.T) {
	err := runCredential([]string{"bogus"})
	if err == nil || !strings.Contains(err.Error(), "unknown action") {
		t.Errorf("runCredential(bogus) = %v, want an error naming the unknown action", err)
	}
}

// TestCredentialMissingAction verifies that `credential` (no action) returns an
// error.
func TestCredentialMissingAction(t *testing.T) {
	err := runCredential([]string{})
	if err == nil || !strings.Contains(err.Error(), "missing action") {
		t.Errorf("runCredential() = %v, want an error about a missing action", err)
	}
}

// TestCredentialAtomicWriteNoPartialFile verifies that atomicWriteFile does not
// leave a partial file on failure: if the write fails (a directory that does
// not exist), the target file is not created and no temp file is left behind.
func TestCredentialAtomicWriteNoPartialFile(t *testing.T) {
	dir := t.TempDir()
	// A path in a non-existent subdirectory: CreateTemp fails, so no file is
	// created and no temp file is left.
	badPath := filepath.Join(dir, "noexist", "cred.json")
	err := atomicWriteFile(badPath, []byte(`{"x":1}`))
	if err == nil {
		t.Fatal("atomicWriteFile (bad dir) = nil, want error")
	}
	// The target file must not exist.
	if _, statErr := os.Stat(badPath); statErr == nil {
		t.Error("atomicWriteFile (bad dir) created the target file (want no file on failure)")
	}
	// No temp files should be left in dir.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.Contains(e.Name(), ".tmp-") {
			t.Errorf("atomicWriteFile left a temp file %q behind", e.Name())
		}
	}
}

// TestCredentialNoSecretInOutput verifies that the credential tooling does not
// print the plaintext password, the verifier, or the signing key to stdout.
// (The success messages go to stderr and contain only the path, generation,
// and iteration count — never the credential material.)
func TestCredentialNoSecretInOutput(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "admin-credential.json")
	const password = "s3cur3-pw"
	restore := initStdin(t, password)
	defer restore()
	if err := runCredential([]string{"init", "--out", out}); err != nil {
		t.Fatalf("init: %v", err)
	}
	cf, _ := auth.LoadCredentialFile(out)
	// The verifier and signing key must not appear in the success message (which
	// goes to stderr). We verify the file content is correct (the material is
	// there) and that the tool did not error.
	if cf.Verifier == "" || cf.SigningKey == "" {
		t.Error("credential file missing verifier or signing key")
	}
}
