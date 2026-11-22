package auth

import (
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestGenerateCredentialAndVerify round-trips a credential: generate it for a
// password, marshal it to a file, load it back, and verify the password. A
// wrong password must fail.
func TestGenerateCredentialAndVerify(t *testing.T) {
	const password = "correct horse battery staple"
	cf, err := GenerateCredential(password, DefaultIterations, 1, nil)
	if err != nil {
		t.Fatalf("GenerateCredential: %v", err)
	}
	if cf.FormatVersion != FormatVersion {
		t.Errorf("FormatVersion = %d, want %d", cf.FormatVersion, FormatVersion)
	}
	if cf.Algorithm != Algorithm {
		t.Errorf("Algorithm = %q, want %q", cf.Algorithm, Algorithm)
	}
	if cf.Generation != 1 {
		t.Errorf("Generation = %d, want 1", cf.Generation)
	}

	// Write to a temp file and load it back.
	dir := t.TempDir()
	path := filepath.Join(dir, "credential.json")
	data, err := MarshalCredentialFile(cf)
	if err != nil {
		t.Fatalf("MarshalCredentialFile: %v", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	loaded, err := LoadCredentialFile(path)
	if err != nil {
		t.Fatalf("LoadCredentialFile: %v", err)
	}
	if !VerifyPassword(loaded, password) {
		t.Error("VerifyPassword(correct) = false, want true")
	}
	if VerifyPassword(loaded, "wrong password") {
		t.Error("VerifyPassword(wrong) = true, want false")
	}
}

// TestLoadCredentialFileMissing returns ErrCredentialUnavailable for a missing
// file (the "unavailable" condition — the auth endpoint returns 503).
func TestLoadCredentialFileMissing(t *testing.T) {
	_, err := LoadCredentialFile(filepath.Join(t.TempDir(), "nope.json"))
	if !errors.Is(err, ErrCredentialUnavailable) {
		t.Errorf("err = %v, want ErrCredentialUnavailable", err)
	}
}

// TestParseCredentialFileMalformed covers malformed JSON: bad JSON, unknown
// fields, multiple values, and incomplete JSON. All must return
// ErrCredentialInvalid.
func TestParseCredentialFileMalformed(t *testing.T) {
	cases := map[string]string{
		"bad json":      `{not json`,
		"unknown field": `{"format_version":1,"algorithm":"PBKDF2-HMAC-SHA-256","iterations":1000,"salt":"aa","verifier":"bb","generation":1,"signing_key":"cc","extra":1}`,
		"multiple":      `{} {}`,
		"incomplete":    `{"format_version":1`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseCredentialFile([]byte(body))
			if !errors.Is(err, ErrCredentialInvalid) {
				t.Errorf("ParseCredentialFile(%q) err = %v, want ErrCredentialInvalid", name, err)
			}
		})
	}
}

// TestParseCredentialFileVersionMismatch rejects a wrong format version.
func TestParseCredentialFileVersionMismatch(t *testing.T) {
	// A well-formed JSON object but with format_version 2 (unknown).
	body := `{"format_version":2,"algorithm":"PBKDF2-HMAC-SHA-256","iterations":1000,"salt":"` + hex32() + `","verifier":"` + hex32() + `","generation":1,"signing_key":"` + hex32() + `"}`
	_, err := ParseCredentialFile([]byte(body))
	if !errors.Is(err, ErrCredentialInvalid) {
		t.Errorf("err = %v, want ErrCredentialInvalid (wrong version)", err)
	}
}

// TestParseCredentialFileAlgorithmMismatch rejects a wrong algorithm.
func TestParseCredentialFileAlgorithmMismatch(t *testing.T) {
	body := `{"format_version":1,"algorithm":"MD5","iterations":1000,"salt":"` + hex32() + `","verifier":"` + hex32() + `","generation":1,"signing_key":"` + hex32() + `"}`
	_, err := ParseCredentialFile([]byte(body))
	if !errors.Is(err, ErrCredentialInvalid) {
		t.Errorf("err = %v, want ErrCredentialInvalid (wrong algorithm)", err)
	}
}

// TestParseCredentialFileBadHex rejects a salt/verifier/signing key that is not
// the correct length (hex-decodes to the wrong number of bytes).
func TestParseCredentialFileBadHex(t *testing.T) {
	cases := map[string]string{
		"short salt":      `{"format_version":1,"algorithm":"PBKDF2-HMAC-SHA-256","iterations":1000,"salt":"aa","verifier":"` + hex32() + `","generation":1,"signing_key":"` + hex32() + `"}`,
		"odd hex":         `{"format_version":1,"algorithm":"PBKDF2-HMAC-SHA-256","iterations":1000,"salt":"abc","verifier":"` + hex32() + `","generation":1,"signing_key":"` + hex32() + `"}`,
		"zero generation": `{"format_version":1,"algorithm":"PBKDF2-HMAC-SHA-256","iterations":1000,"salt":"` + hex32() + `","verifier":"` + hex32() + `","generation":0,"signing_key":"` + hex32() + `"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseCredentialFile([]byte(body))
			if !errors.Is(err, ErrCredentialInvalid) {
				t.Errorf("ParseCredentialFile(%q) err = %v, want ErrCredentialInvalid", name, err)
			}
		})
	}
}

// TestParseCredentialFileValid accepts a well-formed credential.
func TestParseCredentialFileValid(t *testing.T) {
	cf, err := GenerateCredential("pw", 1000, 1, nil)
	if err != nil {
		t.Fatalf("GenerateCredential: %v", err)
	}
	data, err := MarshalCredentialFile(cf)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	loaded, err := ParseCredentialFile(data)
	if err != nil {
		t.Fatalf("ParseCredentialFile: %v", err)
	}
	if loaded.Verifier != cf.Verifier || loaded.Salt != cf.Salt || loaded.SigningKey != cf.SigningKey {
		t.Error("parsed credential does not match generated")
	}
}

// TestVerifyPasswordFailsOnBadVerifier does not panic and returns false when
// the verifier is not valid hex (defensive: a corrupt file must not crash the
// service).
func TestVerifyPasswordFailsOnBadVerifier(t *testing.T) {
	cf := &CredentialFile{
		FormatVersion: FormatVersion,
		Algorithm:     Algorithm,
		Iterations:    1000,
		Salt:          "not-hex",
		Verifier:      "also-not-hex",
		Generation:    1,
		SigningKey:    hex32(),
	}
	if VerifyPassword(cf, "pw") {
		t.Error("VerifyPassword with bad verifier = true, want false")
	}
}

// TestGenerateCredentialRejectsBadParams rejects an out-of-range iteration
// count and a non-positive generation.
func TestGenerateCredentialRejectsBadParams(t *testing.T) {
	if _, err := GenerateCredential("pw", 0, 1, nil); err == nil {
		t.Error("GenerateCredential(iterations=0) = nil, want error")
	}
	if _, err := GenerateCredential("pw", MaxIterations+1, 1, nil); err == nil {
		t.Error("GenerateCredential(iterations=Max+1) = nil, want error")
	}
	if _, err := GenerateCredential("pw", 1000, 0, nil); err == nil {
		t.Error("GenerateCredential(generation=0) = nil, want error")
	}
	if _, err := GenerateCredential("pw", 1000, 1, make([]byte, 16)); err == nil {
		t.Error("GenerateCredential(short signing key) = nil, want error")
	}
}

// hex32 returns a hex-encoded 32-byte value (64 hex chars) for tests. The
// credential fields are hex-encoded, so a "32-byte salt" is 64 hex characters.
func hex32() string {
	b := make([]byte, SaltBytes)
	for i := range b {
		b[i] = byte(i)
	}
	return hex.EncodeToString(b)
}
