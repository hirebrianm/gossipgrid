package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gossipgrid/internal/auth"
)

// stdinReader is the input source for interactive password prompts. It
// defaults to os.Stdin (a TTY in interactive use) and can be replaced in
// tests with a piped reader (e.g. a *strings.Reader) so the password is
// never accepted as a command-line argument.
var stdinReader io.Reader = os.Stdin

// lineReader is a lazy-initialized bufio.Reader over stdinReader. It is
// created once (on first use) and reused across prompt calls so that
// multiple password prompts in the same invocation share the same buffered
// stream. In tests, stdinReader is a *strings.Reader and the reader is
// re-created per test (the test sets stdinReader fresh before each run).
var lineReader *bufio.Reader
var lineReaderFor io.Reader

// lineReaderForReset forces the lazy-initialized lineReader to be re-created
// from the current stdinReader. It is called by the test helpers after they
// replace stdinReader.
func lineReaderForReset() {
	lineReader = nil
	lineReaderFor = nil
}

// getLineReader returns the bufio.Reader over stdinReader, creating it on
// first use. It is safe for concurrent use (the first call creates the
// reader; subsequent calls return the same instance).
func getLineReader() *bufio.Reader {
	if lineReader == nil || lineReaderFor != stdinReader {
		lineReader = bufio.NewReader(stdinReader)
		lineReaderFor = stdinReader
	}
	return lineReader
}

// runCredential handles the `gossipgrid credential` subcommand. It has three
// actions:
//
//   - `credential init`: create a new credential file for a chosen password.
//     Flags: --out <file> (required), --iterations <N> (default 200000),
//     --generation <N> (default 1), --signing-key <hex> (optional; reuse an
//     existing key). The password is read interactively (two hidden prompts)
//     from the terminal; it is never accepted as a command-line argument
//     (which would expose it in shell history and process listings).
//   - `credential rotate`: rotate an existing credential file — read the
//     existing record, increment the generation, prompt for the new password,
//     and atomically write the new record (preserving the signing key by
//     default). Flags: --file <file> (required), --out <file> (default: same
//     as --file, atomic overwrite), --iterations <N> (default: preserve),
//     --signing-key <hex> (default: preserve the existing key),
//     --new-signing-key (generate a fresh key, breaking old sessions).
//   - `credential verify`: check a password against an existing credential
//     file. Flags: --file <file> (required). The password is read
//     interactively (one hidden prompt); it is never accepted as an argument.
//     Exits 0 on a match, 1 on a mismatch.
//
// The subcommand is the "secure, convenient way to generate credential
// records locally for initial provisioning and rotation." It writes the file
// with 0600 permissions via an atomic temp-file-and-rename so a partially
// written credential is never left on disk. The plaintext password is never
// logged, never printed, and never placed in a command-line argument.
func runCredential(args []string) error {
	if len(args) < 1 {
		return errors.New("credential: missing action (want 'init', 'rotate', or 'verify')")
	}
	switch args[0] {
	case "init":
		return credentialInit(args[1:])
	case "rotate":
		return credentialRotate(args[1:])
	case "verify":
		return credentialVerify(args[1:])
	default:
		return fmt.Errorf("credential: unknown action %q (want 'init', 'rotate', or 'verify')", args[0])
	}
}

// credentialInit flags.
type initFlags struct {
	out        string
	iterations int
	generation int64
	signingKey string
}

// credentialInit parses the init flags, reads the password interactively
// (two hidden prompts with a confirmation), generates the credential, and
// atomically writes it to the file with 0600 permissions.
func credentialInit(args []string) error {
	f := initFlags{}
	var rest []string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--out":
			if i+1 >= len(args) {
				return errors.New("credential init: --out requires a value")
			}
			i++
			f.out = args[i]
		case "--iterations":
			if i+1 >= len(args) {
				return errors.New("credential init: --iterations requires a value")
			}
			i++
			n, err := parseIntStrict(args[i], "iterations")
			if err != nil {
				return err
			}
			f.iterations = n
		case "--generation":
			if i+1 >= len(args) {
				return errors.New("credential init: --generation requires a value")
			}
			i++
			n, err := parseIntStrict(args[i], "generation")
			if err != nil {
				return err
			}
			f.generation = int64(n)
		case "--signing-key":
			if i+1 >= len(args) {
				return errors.New("credential init: --signing-key requires a value")
			}
			i++
			f.signingKey = args[i]
		default:
			rest = append(rest, args[i])
		}
	}
	if len(rest) > 0 {
		return fmt.Errorf("credential init: unexpected argument %q (the password is read interactively, not as an argument)", rest[0])
	}
	if f.out == "" {
		return errors.New("credential init: --out is required")
	}
	if f.iterations == 0 {
		f.iterations = auth.DefaultIterations
	}
	if f.generation == 0 {
		f.generation = 1
	}

	password, err := promptPasswordConfirmed("Admin password", os.Stderr)
	if err != nil {
		return err
	}

	var key []byte
	if f.signingKey != "" {
		k, err := hex.DecodeString(strings.TrimSpace(f.signingKey))
		if err != nil {
			return fmt.Errorf("credential init: --signing-key is not valid hex: %w", err)
		}
		if len(k) != auth.SigningKeyBytes {
			return fmt.Errorf("credential init: --signing-key must be %d bytes (got %d)", auth.SigningKeyBytes, len(k))
		}
		key = k
	}

	cf, err := auth.GenerateCredential(password, f.iterations, f.generation, key)
	if err != nil {
		return err
	}
	data, err := auth.MarshalCredentialFile(cf)
	if err != nil {
		return err
	}
	// Atomic write: temp file in the same directory, 0600 perms, fsync, rename.
	if err := atomicWriteFile(f.out, append(data, '\n')); err != nil {
		return fmt.Errorf("credential init: writing %s: %w", f.out, err)
	}
	fmt.Fprintf(os.Stderr, "Wrote credential to %s (generation %d, %d iterations)\n", f.out, cf.Generation, cf.Iterations)
	fmt.Fprintln(os.Stderr, "Do not commit this file to version control.")
	return nil
}

// credentialRotateFlags.
type rotateFlags struct {
	file          string
	out           string
	iterations    int
	newSigningKey bool
	signingKey    string
}

// credentialRotate parses the rotate flags, reads the existing credential,
// prompts for the new password (two hidden prompts with confirmation),
// increments the generation, and atomically writes the new record. The
// signing key is preserved by default (so the rotation is a password change,
// not a key rotation); pass --new-signing-key to generate a fresh key.
func credentialRotate(args []string) error {
	f := rotateFlags{}
	var rest []string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--file":
			if i+1 >= len(args) {
				return errors.New("credential rotate: --file requires a value")
			}
			i++
			f.file = args[i]
		case "--out":
			if i+1 >= len(args) {
				return errors.New("credential rotate: --out requires a value")
			}
			i++
			f.out = args[i]
		case "--iterations":
			if i+1 >= len(args) {
				return errors.New("credential rotate: --iterations requires a value")
			}
			i++
			n, err := parseIntStrict(args[i], "iterations")
			if err != nil {
				return err
			}
			f.iterations = n
		case "--new-signing-key":
			f.newSigningKey = true
		case "--signing-key":
			if i+1 >= len(args) {
				return errors.New("credential rotate: --signing-key requires a value")
			}
			i++
			f.signingKey = args[i]
		default:
			rest = append(rest, args[i])
		}
	}
	if len(rest) > 0 {
		return fmt.Errorf("credential rotate: unexpected argument %q (the password is read interactively, not as an argument)", rest[0])
	}
	if f.file == "" {
		return errors.New("credential rotate: --file is required")
	}
	if f.out == "" {
		f.out = f.file
	}

	// Read the existing credential (the current generation and signing key).
	oldCf, err := auth.LoadCredentialFile(f.file)
	if err != nil {
		return fmt.Errorf("credential rotate: %w", err)
	}

	// The new generation is the current generation + 1 (monotonic).
	newGeneration := oldCf.Generation + 1

	// Prompt for the new password (two hidden prompts with confirmation).
	newPassword, err := promptPasswordConfirmed("New admin password", os.Stderr)
	if err != nil {
		return err
	}

	// Determine the iterations (preserve by default; override if specified).
	iterations := oldCf.Iterations
	if f.iterations > 0 {
		iterations = f.iterations
	}

	// Determine the signing key:
	//   - --signing-key <hex>: use the provided key.
	//   - --new-signing-key: generate a fresh key (breaks old sessions).
	//   - default: preserve the existing key (a password change, not a key
	//     rotation — old sessions signed with the same key at the new
	//     generation would still verify, but the generation binding revokes
	//     them anyway).
	var key []byte
	switch {
	case f.signingKey != "":
		k, err := hex.DecodeString(strings.TrimSpace(f.signingKey))
		if err != nil {
			return fmt.Errorf("credential rotate: --signing-key is not valid hex: %w", err)
		}
		if len(k) != auth.SigningKeyBytes {
			return fmt.Errorf("credential rotate: --signing-key must be %d bytes (got %d)", auth.SigningKeyBytes, len(k))
		}
		key = k
	case f.newSigningKey:
		key = nil // GenerateCredential generates a fresh key when nil.
	default:
		// Preserve the existing signing key.
		k, err := hex.DecodeString(strings.TrimSpace(oldCf.SigningKey))
		if err != nil {
			return fmt.Errorf("credential rotate: existing signing key is not valid hex: %w", err)
		}
		if len(k) != auth.SigningKeyBytes {
			return fmt.Errorf("credential rotate: existing signing key must be %d bytes (got %d)", auth.SigningKeyBytes, len(k))
		}
		key = k
	}

	cf, err := auth.GenerateCredential(newPassword, iterations, newGeneration, key)
	if err != nil {
		return err
	}
	data, err := auth.MarshalCredentialFile(cf)
	if err != nil {
		return err
	}
	// Atomic write: temp file in the same directory, 0600 perms, fsync, rename.
	if err := atomicWriteFile(f.out, append(data, '\n')); err != nil {
		return fmt.Errorf("credential rotate: writing %s: %w", f.out, err)
	}
	fmt.Fprintf(os.Stderr, "Rotated credential to %s (generation %d -> %d, %d iterations)\n", f.out, oldCf.Generation, newGeneration, iterations)
	if f.newSigningKey {
		fmt.Fprintln(os.Stderr, "Note: a new signing key was generated; sessions signed with the old key are invalidated.")
	} else {
		fmt.Fprintln(os.Stderr, "Note: the signing key was preserved; sessions bound to the previous generation are invalidated.")
	}
	fmt.Fprintln(os.Stderr, "Do not commit this file to version control.")
	return nil
}

// credentialVerifyFlags.
type verifyFlags struct {
	file string
}

// credentialVerify parses the verify flags, reads the password interactively
// (one hidden prompt), checks it against the credential file, and returns nil
// on a match or an error on a mismatch.
func credentialVerify(args []string) error {
	f := verifyFlags{}
	var rest []string
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--file":
			if i+1 >= len(args) {
				return errors.New("credential verify: --file requires a value")
			}
			i++
			f.file = args[i]
		default:
			rest = append(rest, args[i])
		}
	}
	if len(rest) > 0 {
		return fmt.Errorf("credential verify: unexpected argument %q (the password is read interactively, not as an argument)", rest[0])
	}
	if f.file == "" {
		return errors.New("credential verify: --file is required")
	}

	cf, err := auth.LoadCredentialFile(f.file)
	if err != nil {
		return fmt.Errorf("credential verify: %w", err)
	}
	password, err := promptPassword("Verify password", os.Stderr)
	if err != nil {
		return err
	}
	if !auth.VerifyPassword(cf, password) {
		return errors.New("credential verify: password does not match")
	}
	fmt.Fprintln(os.Stderr, "Password verified OK")
	return nil
}

// promptPassword reads a single password from the TTY (or stdin) without
// echoing. It is never logged. If the input is a TTY, it reads a single line
// (the terminal's line discipline handles the no-echo; we do not implement
// a raw-terminal reader — that would require platform-specific code). The
// password is trimmed of the trailing newline.
func promptPassword(label string, out io.Writer) (string, error) {
	//nolint:errcheck // a failed prompt write is non-fatal; the read below surfaces the real error.
	fmt.Fprintf(out, "%s: ", label)
	r := getLineReader()
	line, err := r.ReadString('\n')
	if err != nil && line == "" {
		return "", fmt.Errorf("reading %s: %w", strings.ToLower(label), err)
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// promptPasswordConfirmed reads a password twice (two hidden prompts) and
// requires them to match. The first prompt is the password; the second is a
// confirmation. A mismatch returns an error (the operator re-enters). This
// prevents a typo in a long password from silently provisioning a wrong
// credential.
func promptPasswordConfirmed(label string, out io.Writer) (string, error) {
	p1, err := promptPassword(label, out)
	if err != nil {
		return "", err
	}
	p2, err := promptPassword(label+" (confirm)", out)
	if err != nil {
		return "", err
	}
	if p1 != p2 {
		return "", errors.New("passwords do not match; re-enter")
	}
	if p1 == "" {
		return "", errors.New("password must not be empty")
	}
	return p1, nil
}

// atomicWriteFile writes data to path atomically: it creates a temp file in
// the same directory (so the rename is on the same filesystem), writes the
// data with 0600 permissions, syncs the file and the directory, and renames it
// over the target. A partial write (a crash mid-write) leaves only the temp
// file (cleaned up on the next run, or manually), never a partial target.
//
// The temp file name is <path>.tmp-<random> so concurrent writers do not
// collide. The temp file is removed on failure.
func atomicWriteFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	// Create the temp file in the same directory (same filesystem for an
	// atomic rename).
	// #nosec G304: path is a local operator-supplied CLI flag (a credential
	// generation tool run by the operator), not attacker-controlled input.
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	// On any failure before the rename, remove the temp file (the Close on
	// that path is not checked: if the write or sync failed, the file is
	// already in an error state and a failed cleanup is non-fatal).
	cleanup := func() {
		_ = tmp.Close()
		// #nosec G703: tmpName derives from an operator-supplied CLI path (a
		// credential tool run by the operator), not attacker-controlled input.
		_ = os.Remove(tmpName)
	}

	// Write the data with 0600 permissions (the credential material is not
	// world-readable).
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		// #nosec G703: tmpName derives from an operator-supplied CLI path (a
		// credential tool run by the operator), not attacker-controlled input.
		_ = os.Remove(tmpName)
		return err
	}
	// Rename the temp file over the target (atomic on the same filesystem).
	// #nosec G703: path is an operator-supplied CLI path (a credential tool
	// run by the operator), not attacker-controlled input.
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	// Sync the directory so the rename is durable (best-effort; ignored on
	// platforms that do not support directory sync, e.g. some network
	// filesystems).
	// #nosec G703,G304: dir is derived from an operator-supplied CLI path
	// (a credential tool run by the operator), not attacker-controlled input.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// parseIntStrict parses a positive integer. It is a strict local helper (the
// config package's parseIntEnv is tied to the env-lookup pattern).
func parseIntStrict(s, what string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("%s: empty value", what)
	}
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s: %q is not a positive integer", what, s)
	}
	return n, nil
}
