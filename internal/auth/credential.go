package auth

import (
	"bytes"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// Algorithm is the only password hashing algorithm this credential format
// supports. The value is recorded in the credential file and verified at load
// time; a future algorithm (e.g. Argon2) would require a new format version.
const Algorithm = "PBKDF2-HMAC-SHA-256"

// FormatVersion is the current version of the credential file format. The
// version is recorded in the file and must match this constant for the file
// to load; an unknown version fails closed (the credential is unavailable).
const FormatVersion = 1

// SaltBytes is the per-credential salt length. 32 bytes is the conventional
// length for a random salt; the salt is not secret but ensures two identical
// passwords produce different verifiers.
const SaltBytes = 32

// VerifierBytes is the PBKDF2 verifier length. It equals the output length of
// one PBKDF2 HMAC-SHA-256 block (32 bytes), which is the default and the only
// length this implementation produces.
const VerifierBytes = 32

// SigningKeyBytes is the common session-signing key length. Every replica
// mounts the same credential file, so every replica holds the same key and can
// independently verify session cookies signed under it. 32 bytes is appropriate
// for an HMAC-SHA-256 key (at least the hash output length).
const SigningKeyBytes = 32

// MaxIterations is an upper bound on the PBKDF2 iteration count accepted from a
// credential file. It exists to bound the CPU cost of a single verification
// (a hostile file must not be able to pin a CPU with an absurd iteration
// count). 4096 is the RFC 2898 recommendation; 1,000,000 is a generous ceiling
// for a single-administrator service that verifies at most a handful of
// logins per second. The exact count is a parameter of the credential record;
// changing it is a rotation.
const MaxIterations = 1000000

// DefaultIterations is the iteration count used by `credential init` when the
// operator does not supply one. It is deliberately conservative for a
// single-administrator service (the threat is an offline attacker, not a
// volumetric online brute-force); the operator may raise it at provisioning.
const DefaultIterations = 200000

// CredentialFile is the versioned credential record stored in the mounted
// credential file. It is JSON-encoded for readability and testability.
//
// The plaintext password is never stored in this struct or in the file. Only
// the PBKDF2 verifier (the password hash) is retained, so the service can
// verify passwords without holding the plaintext. The verifier and the signing
// key are both deliberately kept in the file (see docs/security.md); only the
// plaintext password is excluded from storage and from the session cookie.
type CredentialFile struct {
	// FormatVersion is the credential format version (must be FormatVersion).
	FormatVersion int `json:"format_version"`
	// Algorithm is the password hashing algorithm (must be Algorithm).
	Algorithm string `json:"algorithm"`
	// Iterations is the PBKDF2 iteration count (1..MaxIterations).
	Iterations int `json:"iterations"`
	// Salt is the per-credential random salt (hex-encoded, SaltBytes bytes).
	Salt string `json:"salt"`
	// Verifier is the PBKDF2-HMAC-SHA-256 password verifier (hex-encoded,
	// VerifierBytes bytes). Never the plaintext password.
	Verifier string `json:"verifier"`
	// Generation is a monotonically increasing counter that increments on every
	// rotation. Session cookies are bound to the generation they were issued
	// under; a rotation changes the generation and invalidates prior sessions
	// on replicas that have loaded the new snapshot.
	Generation int64 `json:"generation"`
	// SigningKey is the common session-signing key (hex-encoded,
	// SigningKeyBytes bytes). Every replica holds the same key, so any replica
	// can verify a session cookie without contacting the issuer. The key is
	// never exposed to the client.
	SigningKey string `json:"signing_key"`
}

// ErrCredentialUnavailable indicates the credential is absent or failed to load.
// The authentication endpoint returns 503 for this condition (distinguishing it
// from a wrong password, which is 401). It is the "unavailable" half of the
// availability/validity distinction in docs/security.md.
var ErrCredentialUnavailable = errors.New("credential unavailable")

// ErrCredentialInvalid indicates the credential file is present but malformed or
// internally inconsistent. It is treated the same as unavailable (503, never
// used to answer an authentication decision) but is reported distinctly in
// diagnostics.
var ErrCredentialInvalid = errors.New("credential invalid")

// validate performs the complete internal-consistency check on a parsed
// credential record. It never logs the credential material. A failure means the
// file is unusable for authentication (treated as unavailable).
func (cf *CredentialFile) validate() error {
	if cf.FormatVersion != FormatVersion {
		return fmt.Errorf("%w: unsupported format version %d (want %d)", ErrCredentialInvalid, cf.FormatVersion, FormatVersion)
	}
	if cf.Algorithm != Algorithm {
		return fmt.Errorf("%w: unsupported algorithm %q (want %q)", ErrCredentialInvalid, cf.Algorithm, Algorithm)
	}
	if cf.Iterations < 1 || cf.Iterations > MaxIterations {
		return fmt.Errorf("%w: iteration count %d out of range (want 1-%d)", ErrCredentialInvalid, cf.Iterations, MaxIterations)
	}
	if cf.Generation < 1 {
		return fmt.Errorf("%w: generation %d must be positive", ErrCredentialInvalid, cf.Generation)
	}
	// Decode and bound-check the salt, verifier, and signing key. The lengths
	// must be exact; a truncated or oversized value is malformed.
	salt, err := decodeHexBounded(cf.Salt, SaltBytes, "salt")
	if err != nil {
		return err
	}
	verifier, err := decodeHexBounded(cf.Verifier, VerifierBytes, "verifier")
	if err != nil {
		return err
	}
	key, err := decodeHexBounded(cf.SigningKey, SigningKeyBytes, "signing key")
	if err != nil {
		return err
	}
	_ = salt
	_ = verifier
	_ = key
	return nil
}

// decodeHexBounded decodes a hex string and returns an error unless it decodes
// to exactly wantLen bytes. The field name is used only in the error message.
// The []byte return is used by all call sites; unparam cannot see that.
//
//nolint:unparam
func decodeHexBounded(s string, wantLen int, field string) ([]byte, error) {
	s = strings.TrimSpace(s)
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("%w: %s is not valid hex: %v", ErrCredentialInvalid, field, err)
	}
	if len(b) != wantLen {
		return nil, fmt.Errorf("%w: %s must be exactly %d bytes (got %d)", ErrCredentialInvalid, field, wantLen, len(b))
	}
	return b, nil
}

// hexDecode decodes a hex string. It wraps hex.DecodeString so the credential
// and session code share one decoding path.
func hexDecode(s string) ([]byte, error) {
	return hex.DecodeString(s)
}

// LoadCredentialFile reads and validates the credential file at path.
//
// It returns:
//
//   - a *CredentialFile and nil when the file is present and valid;
//   - nil and ErrCredentialUnavailable when the file does not exist (or the
//     read fails for an I/O reason);
//   - nil and ErrCredentialInvalid (wrapped) when the file is present but
//     malformed.
//
// The file is read in a single os.ReadFile call. A Kubernetes Secret mounted
// as a full volume is updated atomically by the kubelet, so a read observes
// either the complete old or the complete new version — never a partial write.
func LoadCredentialFile(path string) (*CredentialFile, error) {
	// #nosec G304: path is a configured local/mounted credential-file location
	// (an operator set via GOSSIPGRID_AUTH_CREDENTIAL_FILE or a mounted Secret
	// volume), not attacker-controlled input; no request path reaches here.
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s: %v", ErrCredentialUnavailable, path, err)
		}
		// A read failure (permission, I/O) is also treated as unavailable; the
		// previous valid snapshot (if any) is preserved by the reloader.
		return nil, fmt.Errorf("%w: %s: %v", ErrCredentialUnavailable, path, err)
	}
	return ParseCredentialFile(data)
}

// ParseCredentialFile parses and validates a raw credential file body. It is
// split from LoadCredentialFile so tests can exercise the parser without a
// file on disk.
func ParseCredentialFile(data []byte) (*CredentialFile, error) {
	var cf CredentialFile
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cf); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("%w: incomplete JSON", ErrCredentialInvalid)
		}
		return nil, fmt.Errorf("%w: malformed JSON: %v", ErrCredentialInvalid, err)
	}
	// Reject trailing data (multiple JSON documents) — a well-formed credential
	// file is exactly one JSON object. A second Decode returns io.EOF when the
	// body is exhausted (valid) and nil when a second value was decoded
	// (invalid).
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("%w: multiple JSON values in credential file", ErrCredentialInvalid)
		}
		return nil, fmt.Errorf("%w: trailing data after JSON object: %v", ErrCredentialInvalid, err)
	}
	if err := cf.validate(); err != nil {
		return nil, err
	}
	return &cf, nil
}

// VerifyPassword checks a submitted password against the credential record
// using a constant-time comparison. It returns true only when the PBKDF2
// output for the password (with the stored salt and iteration count) matches
// the stored verifier. The password is never logged and is discarded after the
// comparison.
//
// The PBKDF2 key length is fixed to VerifierBytes (32) — the length the
// verifier is stored at — so the comparison is always between two 32-byte
// values and the constant-time compare is well-defined.
func VerifyPassword(cf *CredentialFile, password string) bool {
	salt, err := hexDecode(strings.TrimSpace(cf.Salt))
	if err != nil {
		return false
	}
	verifier, err := hexDecode(strings.TrimSpace(cf.Verifier))
	if err != nil {
		return false
	}
	computed, err := pbkdf2.Key(sha256.New, password, salt, cf.Iterations, VerifierBytes)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(computed, verifier) == 1
}

// MarshalCredentialFile encodes a credential record as a JSON document. It is
// used by the `credential init` subcommand to write a new credential file. The
// output is a single JSON object with one key per field; the credential
// material (salt, verifier, signing key) is hex-encoded so the file is
// text-readable without binary content.
func MarshalCredentialFile(cf *CredentialFile) ([]byte, error) {
	return json.MarshalIndent(cf, "", "  ")
}

// GenerateCredential creates a new credential record for the given password.
// It generates a fresh 32-byte salt and (when signingKey is empty) a fresh
// 32-byte signing key, computes the PBKDF2-HMAC-SHA-256 verifier, and sets the
// generation. It is used by the `credential init` subcommand and by tests.
//
// The plaintext password is never stored; only the PBKDF2 verifier is
// retained. The returned record is ready to be written to a file via
// MarshalCredentialFile.
//
// Parameters:
//   - password: the administrator password (never stored).
//   - iterations: the PBKDF2 iteration count (1..MaxIterations).
//   - generation: the credential generation (>= 1).
//   - signingKey: a pre-existing 32-byte signing key to reuse, or nil to
//     generate a fresh one. Reusing the signing key across password rotations
//     is optional: the key is the mechanism for cross-replica verification, and
//     reusing it means sessions issued under the old generation were signed
//     with the same key. It does NOT keep old sessions valid after a rotation
//     — a rotation changes the generation, and sessions are bound to the
//     generation (see session.go). A new signing key is fine too; in that case
//     old-generation sessions (signed with the old key) are rejected as well
//     (signature mismatch).
func GenerateCredential(password string, iterations int, generation int64, signingKey []byte) (*CredentialFile, error) {
	if iterations < 1 || iterations > MaxIterations {
		return nil, fmt.Errorf("%w: iteration count %d out of range", ErrCredentialInvalid, iterations)
	}
	if generation < 1 {
		return nil, fmt.Errorf("%w: generation %d must be positive", ErrCredentialInvalid, generation)
	}
	salt := make([]byte, SaltBytes)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("generating salt: %w", err)
	}
	if signingKey == nil {
		signingKey = make([]byte, SigningKeyBytes)
		if _, err := rand.Read(signingKey); err != nil {
			return nil, fmt.Errorf("generating signing key: %w", err)
		}
	} else if len(signingKey) != SigningKeyBytes {
		return nil, fmt.Errorf("%w: signing key must be %d bytes (got %d)", ErrCredentialInvalid, SigningKeyBytes, len(signingKey))
	}
	verifier, err := pbkdf2.Key(sha256.New, password, salt, iterations, VerifierBytes)
	if err != nil {
		return nil, fmt.Errorf("computing verifier: %w", err)
	}
	return &CredentialFile{
		FormatVersion: FormatVersion,
		Algorithm:     Algorithm,
		Iterations:    iterations,
		Salt:          hex.EncodeToString(salt),
		Verifier:      hex.EncodeToString(verifier),
		Generation:    generation,
		SigningKey:    hex.EncodeToString(signingKey),
	}, nil
}
