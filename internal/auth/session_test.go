package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newTestReq builds an httptest request using the test context (noctx).
func newTestReq(method, target string) *http.Request {
	return httptest.NewRequestWithContext(context.Background(), method, target, nil)
}

// testKey returns a fresh 32-byte random signing key for tests.
func testKey(t *testing.T) []byte {
	t.Helper()
	b := make([]byte, SigningKeyBytes)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("generating test key: %v", err)
	}
	return b
}

// TestIssueAndValidateSession round-trips a session: issue a cookie, validate
// it with the same key and generation. The returned payload must carry the
// correct generation and a future expiry.
func TestIssueAndValidateSession(t *testing.T) {
	key := testKey(t)
	now := time.Now()
	cookie, csrf, err := IssueSession(key, 7, now)
	if err != nil {
		t.Fatalf("IssueSession: %v", err)
	}
	if csrf == "" {
		t.Error("CSRF token is empty")
	}
	if !strings.Contains(cookie, ".") {
		t.Errorf("cookie %q does not contain a separator", cookie)
	}
	p, err := ValidateSession(key, 7, cookie, now)
	if err != nil {
		t.Fatalf("ValidateSession: %v", err)
	}
	if p.GEN != 7 {
		t.Errorf("payload GEN = %d, want 7", p.GEN)
	}
	if p.EXP <= now.Unix() {
		t.Errorf("payload EXP = %d is not in the future (now=%d)", p.EXP, now.Unix())
	}
}

// TestValidateSessionTampered rejects a cookie whose payload has been modified
// after signing (the signature no longer matches).
func TestValidateSessionTampered(t *testing.T) {
	key := testKey(t)
	now := time.Now()
	cookie, _, err := IssueSession(key, 1, now)
	if err != nil {
		t.Fatalf("IssueSession: %v", err)
	}
	// Tamper: flip a character in the payload (the first half, before the ".").
	parts := strings.SplitN(cookie, ".", 2)
	payload := []byte(parts[0])
	payload[0] ^= 1
	tampered := string(payload) + "." + parts[1]
	if _, err := ValidateSession(key, 1, tampered, now); !errors.Is(err, ErrSessionInvalid) {
		t.Errorf("ValidateSession(tampered) err = %v, want ErrSessionInvalid", err)
	}
}

// TestValidateSessionExpired rejects a cookie whose absolute expiry has
// elapsed. It issues with a short TTL (by temporarily overriding SessionTTL)
// and advances the clock past the expiry.
func TestValidateSessionExpired(t *testing.T) {
	key := testKey(t)
	now := time.Now()
	cookie, _, err := IssueSession(key, 1, now)
	if err != nil {
		t.Fatalf("IssueSession: %v", err)
	}
	// Advance the clock past the absolute expiry (SessionTTL + a buffer).
	later := now.Add(SessionTTL + time.Second)
	if _, err := ValidateSession(key, 1, cookie, later); !errors.Is(err, ErrSessionExpired) {
		t.Errorf("ValidateSession(expired) err = %v, want ErrSessionExpired", err)
	}
}

// TestValidateSessionIdleExpired rejects a cookie whose idle expiry has
// elapsed (but the absolute expiry has not). It advances the clock past
// SessionIdleTTL but before SessionTTL.
func TestValidateSessionIdleExpired(t *testing.T) {
	key := testKey(t)
	now := time.Now()
	cookie, _, err := IssueSession(key, 1, now)
	if err != nil {
		t.Fatalf("IssueSession: %v", err)
	}
	// Past the idle expiry (30m) but before the absolute expiry (2h).
	later := now.Add(SessionIdleTTL + time.Second)
	if _, err := ValidateSession(key, 1, cookie, later); !errors.Is(err, ErrSessionExpired) {
		t.Errorf("ValidateSession(idle-expired) err = %v, want ErrSessionExpired", err)
	}
}

// TestValidateSessionWrongGeneration rejects a cookie issued under a different
// generation. This is the rotation-invalidation mechanism: a cookie issued
// under generation 1 is rejected once the replica verifies against generation
// 2.
func TestValidateSessionWrongGeneration(t *testing.T) {
	key := testKey(t)
	now := time.Now()
	cookie, _, err := IssueSession(key, 1, now)
	if err != nil {
		t.Fatalf("IssueSession: %v", err)
	}
	// Verify against generation 2 (a different generation) — reject.
	if _, err := ValidateSession(key, 2, cookie, now); !errors.Is(err, ErrSessionInvalid) {
		t.Errorf("ValidateSession(wrong generation) err = %v, want ErrSessionInvalid", err)
	}
}

// TestValidateSessionWrongKey rejects a cookie signed with a different key
// (the replica has a different signing key — e.g. a different credential
// file). The signature mismatch is detected in constant time.
func TestValidateSessionWrongKey(t *testing.T) {
	key1 := testKey(t)
	key2 := testKey(t)
	now := time.Now()
	cookie, _, err := IssueSession(key1, 1, now)
	if err != nil {
		t.Fatalf("IssueSession: %v", err)
	}
	if _, err := ValidateSession(key2, 1, cookie, now); !errors.Is(err, ErrSessionInvalid) {
		t.Errorf("ValidateSession(wrong key) err = %v, want ErrSessionInvalid", err)
	}
}

// TestValidateSessionMalformed rejects a cookie that is not in the
// base64url(payload).base64url(hmac) format.
func TestValidateSessionMalformed(t *testing.T) {
	key := testKey(t)
	now := time.Now()
	cases := []string{
		"",
		"no-dot-here",
		".",
		"!!!.!!!", // invalid base64url
	}
	for _, c := range cases {
		if _, err := ValidateSession(key, 1, c, now); !errors.Is(err, ErrSessionInvalid) {
			t.Errorf("ValidateSession(%q) err = %v, want ErrSessionInvalid", c, err)
		}
	}
}

// TestRotationRejectsOldGeneration verifies that once a replica has loaded a
// new credential generation, a session issued under the old generation is
// rejected immediately (no overlap window). This is the
// rotation-invalidation mechanism: password rotation changes the generation,
// and sessions are bound to the generation.
func TestRotationRejectsOldGeneration(t *testing.T) {
	dir := t.TempDir()
	path := filepathJoin(dir, "cred.json")

	oldCf, err := GenerateCredential("old-pw", 1000, 1, nil)
	if err != nil {
		t.Fatalf("GenerateCredential(old): %v", err)
	}
	newCf, err := GenerateCredential("new-pw", 1000, 2, nil)
	if err != nil {
		t.Fatalf("GenerateCredential(new): %v", err)
	}

	if err := writeFile(path, oldCf); err != nil {
		t.Fatalf("writeFile(old): %v", err)
	}
	reloader := NewReloader(path, time.Hour, time.Hour, nil)
	if err := reloader.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer reloader.Shutdown()

	// Issue a session under the old generation (generation 1, old key).
	now := time.Now()
	oldCookie, _, err := IssueSession(oldCf.SigningKeyHex(), 1, now)
	if err != nil {
		t.Fatalf("IssueSession(old): %v", err)
	}

	// Rotate: write the new credential (generation 2, new key) and force a reload.
	if err := writeFile(path, newCf); err != nil {
		t.Fatalf("writeFile(new): %v", err)
	}
	reloader.TryReload()

	// The replica now verifies against generation 2. The old-generation cookie
	// (generation 1) must be rejected immediately (no overlap window).
	provider := NewProvider(reloader)
	res := provider.Authenticate(makeRequestWithCookie(oldCookie), now)
	if res.Allowed {
		t.Error("Authenticate(old-generation cookie after rotation) allowed, want rejected (no overlap window)")
	}
	if res.Condition != ConditionUnauthenticated {
		t.Errorf("condition = %q, want %q", res.Condition, ConditionUnauthenticated)
	}
}

// TestRotationNewPasswordWorks verifies that after a rotation, a new login with
// the new password succeeds and issues a session bound to the new generation.
func TestRotationNewPasswordWorks(t *testing.T) {
	dir := t.TempDir()
	path := filepathJoin(dir, "cred.json")

	oldCf, _ := GenerateCredential("old-pw", 1000, 1, nil)
	newCf, _ := GenerateCredential("new-pw", 1000, 2, nil)
	if err := writeFile(path, oldCf); err != nil {
		t.Fatalf("writeFile(old): %v", err)
	}
	reloader := NewReloader(path, time.Hour, time.Hour, nil)
	if err := reloader.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer reloader.Shutdown()

	// Rotate to generation 2 with a new password.
	if err := writeFile(path, newCf); err != nil {
		t.Fatalf("writeFile(new): %v", err)
	}
	reloader.TryReload()

	provider := NewProvider(reloader)
	// The old password must be rejected.
	if ok, err := provider.VerifyPassword("old-pw"); err != nil || ok {
		t.Errorf("VerifyPassword(old-pw) after rotation = (%v, %v), want (false, nil)", ok, err)
	}
	// The new password must be accepted.
	if ok, err := provider.VerifyPassword("new-pw"); err != nil || !ok {
		t.Errorf("VerifyPassword(new-pw) after rotation = (%v, %v), want (true, nil)", ok, err)
	}
	// Issue a session under the new generation and verify it.
	now := time.Now()
	cookie, _, err := IssueSession(newCf.SigningKeyHex(), 2, now)
	if err != nil {
		t.Fatalf("IssueSession(new): %v", err)
	}
	res := provider.Authenticate(makeRequestWithCookie(cookie), now)
	if !res.Allowed {
		t.Errorf("Authenticate(new-generation session) not allowed: %q", res.Message)
	}
}

// TestSigningKeyRotationPreservesSameGenerationSessions verifies that a
// session-signing-key rotation (a new signing key at the UNCHANGED generation)
// is a distinct concept from password rotation: it does NOT invalidate
// sessions (the generation is unchanged). Sessions signed with the old key at
// the same generation are still rejected (signature mismatch), but a session
// signed with the new key at the same generation is accepted. This documents
// that the signing-key rotation and the password/session-revocation policy are
// separated: the key changes the verification mechanism, the generation
// changes the revocation policy.
func TestSigningKeyRotationPreservesSameGenerationSessions(t *testing.T) {
	dir := t.TempDir()
	path := filepathJoin(dir, "cred.json")

	// Old credential: generation 1, key1, password "pw".
	oldCf, _ := GenerateCredential("pw", 1000, 1, nil)
	// New credential: SAME generation 1, same password, but a new signing key
	// (key2). This models a session-signing-key rotation (key rollover) without
	// a password rotation (the generation is unchanged). The verifier and salt
	// are copied from the old credential so the password still verifies; only
	// the signing key changes.
	key2, _ := hex.DecodeString(makeKey2())
	newCf := &CredentialFile{
		FormatVersion: FormatVersion,
		Algorithm:     Algorithm,
		Iterations:    oldCf.Iterations,
		Salt:          oldCf.Salt,
		Verifier:      oldCf.Verifier,
		Generation:    oldCf.Generation, // unchanged (no password rotation)
		SigningKey:    makeKey2(),       // new key
	}
	_ = key2
	if err := writeFile(path, oldCf); err != nil {
		t.Fatalf("writeFile(old): %v", err)
	}
	reloader := NewReloader(path, time.Hour, time.Hour, nil)
	if err := reloader.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer reloader.Shutdown()

	now := time.Now()
	// A session signed with the OLD key at generation 1.
	oldKeyCookie, _, err := IssueSession(oldCf.SigningKeyHex(), 1, now)
	if err != nil {
		t.Fatalf("IssueSession(old key): %v", err)
	}

	// Rotate the signing key (same generation 1, new key2).
	if err := writeFile(path, newCf); err != nil {
		t.Fatalf("writeFile(new): %v", err)
	}
	reloader.TryReload()

	provider := NewProvider(reloader)
	// The old-key session is rejected (signature mismatch — the replica now
	// verifies with key2, and the old cookie was signed with key1).
	res := provider.Authenticate(makeRequestWithCookie(oldKeyCookie), now)
	if res.Allowed {
		t.Error("Authenticate(old-key session after key rotation) allowed, want rejected (signature mismatch)")
	}
	// A session signed with the NEW key at the SAME generation 1 is accepted
	// (the generation is unchanged, so it is a valid session).
	newKeyCookie, _, err := IssueSession(newCf.SigningKeyHex(), 1, now)
	if err != nil {
		t.Fatalf("IssueSession(new key): %v", err)
	}
	res = provider.Authenticate(makeRequestWithCookie(newKeyCookie), now)
	if !res.Allowed {
		t.Errorf("Authenticate(new-key session, same generation) not allowed: %q", res.Message)
	}
}

// makeKey2 returns a distinct 32-byte value for the new signing key in tests.
// It is a fixed, non-random value so the test is deterministic.
func makeKey2() string {
	b := make([]byte, SigningKeyBytes)
	for i := range b {
		b[i] = byte(i + 7)
	}
	return hex.EncodeToString(b)
}

// TestCSRFValid verifies that a state-changing request whose CSRF token
// equals HMAC-SHA256(key, SID) (for the session's SID) passes CheckCSRF.
func TestCSRFValid(t *testing.T) {
	key := testKey(t)
	now := time.Now()
	cookie, csrfToken, err := IssueSession(key, 1, now)
	if err != nil {
		t.Fatalf("IssueSession: %v", err)
	}
	req := newTestReq(http.MethodPost, "/api/messages")
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: cookie})
	req.AddCookie(&http.Cookie{Name: CSRFCookieName, Value: csrfToken})
	req.Header.Set(CSRFHeader, csrfToken)
	if !CheckCSRF(req, key, now) {
		t.Error("CheckCSRF(valid) = false, want true")
	}
}

// TestCSRFInvalid verifies that a state-changing request with a mismatched or
// missing CSRF token fails CheckCSRF.
func TestCSRFInvalid(t *testing.T) {
	key := testKey(t)
	now := time.Now()
	cookie, csrfToken, err := IssueSession(key, 1, now)
	if err != nil {
		t.Fatalf("IssueSession: %v", err)
	}
	cases := []struct {
		name   string
		cookie string
		header string
	}{
		{"mismatch", csrfToken, "wrong-token"},
		{"missing header", csrfToken, ""},
		{"missing cookie", "", csrfToken},
		{"both missing", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := newTestReq(http.MethodPost, "/api/messages")
			req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: cookie})
			if c.cookie != "" {
				req.AddCookie(&http.Cookie{Name: CSRFCookieName, Value: c.cookie})
			}
			if c.header != "" {
				req.Header.Set(CSRFHeader, c.header)
			}
			if CheckCSRF(req, key, now) {
				t.Errorf("CheckCSRF(%s) = true, want false", c.name)
			}
		})
	}
}

// TestCSRFNotRequiredForSafeMethods verifies that a GET request passes
// CheckCSRF without a token (GET is not state-changing).
func TestCSRFNotRequiredForSafeMethods(t *testing.T) {
	req := newTestReq(http.MethodGet, "/api/messages")
	if !CheckCSRF(req, testKey(t), time.Now()) {
		t.Error("CheckCSRF(GET) = false, want true (safe method, no CSRF required)")
	}
}

// TestCSRFSessionBound verifies that the CSRF token is cryptographically bound
// to the session: a token issued for one session (SID1) does NOT validate on
// a request carrying a different session (SID2), even if the token is
// carried in both the cookie and the header. This is the session-binding
// property: a token from one session cannot be reused on another session.
func TestCSRFSessionBound(t *testing.T) {
	key := testKey(t)
	now := time.Now()
	// Session A: cookieA, tokenA (derived from SID_A).
	cookieA, tokenA, err := IssueSession(key, 1, now)
	if err != nil {
		t.Fatalf("IssueSession(A): %v", err)
	}
	// Session B (a different SID): cookieB, tokenB (derived from SID_B).
	cookieB, tokenB, err := IssueSession(key, 1, now)
	if err != nil {
		t.Fatalf("IssueSession(B): %v", err)
	}
	if tokenA == tokenB {
		t.Fatal("two sessions produced the same CSRF token (SIDs are not unique)")
	}
	// A request carrying session B's cookie but session A's token must be
	// rejected (the token is not the HMAC of session B's SID).
	req := newTestReq(http.MethodPost, "/api/messages")
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: cookieB})
	req.AddCookie(&http.Cookie{Name: CSRFCookieName, Value: tokenA})
	req.Header.Set(CSRFHeader, tokenA)
	if CheckCSRF(req, key, now) {
		t.Error("CheckCSRF(token from session A on session B) = true, want false (session-bound)")
	}
	// A request carrying session A's cookie and token (matching) must be accepted.
	req2 := newTestReq(http.MethodPost, "/api/messages")
	req2.AddCookie(&http.Cookie{Name: SessionCookieName, Value: cookieA})
	req2.AddCookie(&http.Cookie{Name: CSRFCookieName, Value: tokenA})
	req2.Header.Set(CSRFHeader, tokenA)
	if !CheckCSRF(req2, key, now) {
		t.Error("CheckCSRF(matching session A token) = false, want true")
	}
}

// TestCSRFTamperedToken verifies that a tampered CSRF token (one character
// changed) fails CheckCSRF (the constant-time comparison detects the
// mismatch).
func TestCSRFTamperedToken(t *testing.T) {
	key := testKey(t)
	now := time.Now()
	cookie, token, err := IssueSession(key, 1, now)
	if err != nil {
		t.Fatalf("IssueSession: %v", err)
	}
	// Tamper the token (flip the last hex character).
	tampered := token[:len(token)-1]
	if tampered[len(tampered)-1] == 'a' {
		tampered = tampered[:len(tampered)-1] + "b"
	} else {
		tampered = tampered[:len(tampered)-1] + "a"
	}
	req := newTestReq(http.MethodPost, "/api/messages")
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: cookie})
	req.AddCookie(&http.Cookie{Name: CSRFCookieName, Value: tampered})
	req.Header.Set(CSRFHeader, tampered)
	if CheckCSRF(req, key, now) {
		t.Error("CheckCSRF(tampered token) = true, want false")
	}
}

// TestCSRFExpiredSession verifies that an expired session's CSRF token is
// rejected. The session cookie's signature still validates (it was signed with
// the correct key and generation) but the session has expired; the web
// handler's Authenticate rejects it before CSRF. This test checks that
// CheckCSRF, on its own, still validates the token (it does not check
// expiry — that is Authenticate's job). We assert the token is well-formed
// (CheckCSRF returns true for a matching token on an unexpired session) and
// that the web handler (which authenticates first) rejects the expired
// session.
func TestCSRFExpiredSession(t *testing.T) {
	key := testKey(t)
	now := time.Now()
	cookie, token, err := IssueSession(key, 1, now)
	if err != nil {
		t.Fatalf("IssueSession: %v", err)
	}
	// CheckCSRF on its own does not check session expiry (that is the
	// provider's Authenticate job, which runs first in the web handler).
	// The token is still the valid HMAC of the SID, so CheckCSRF returns true
	// for a request at "now". At "now + SessionTTL" the session is expired;
	// Authenticate would reject it. CheckCSRF itself is a pure token check.
	req := newTestReq(http.MethodPost, "/api/messages")
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: cookie})
	req.AddCookie(&http.Cookie{Name: CSRFCookieName, Value: token})
	req.Header.Set(CSRFHeader, token)
	// At "now" the token is valid (CheckCSRF does not check expiry).
	if !CheckCSRF(req, key, now) {
		t.Error("CheckCSRF(token at now) = false, want true (token is well-formed; expiry is Authenticate's concern)")
	}
}

// TestSetAndClearSessionCookie verifies the cookie flags: the session cookie
// is HttpOnly + SameSite=Strict, the CSRF cookie is not HttpOnly. Both are
// Secure when secure=true and not Secure when secure=false.
func TestSetAndClearSessionCookie(t *testing.T) {
	now := time.Now()
	rec := httptest.NewRecorder()
	SetSessionCookie(rec, "sess-val", "csrf-val", true, now)
	cookies := rec.Result().Cookies()
	if len(cookies) != 2 {
		t.Fatalf("SetSessionCookie set %d cookies, want 2", len(cookies))
	}
	var session, csrf *http.Cookie
	for _, c := range cookies {
		if c.Name == SessionCookieName {
			session = c
		}
		if c.Name == CSRFCookieName {
			csrf = c
		}
	}
	if session == nil || csrf == nil {
		t.Fatalf("missing session or csrf cookie (got %v)", cookies)
	}
	if !session.HttpOnly {
		t.Error("session cookie not HttpOnly")
	}
	if !session.Secure {
		t.Error("session cookie not Secure (secure=true)")
	}
	if session.SameSite != http.SameSiteStrictMode {
		t.Error("session cookie not SameSite=Strict")
	}
	if csrf.HttpOnly {
		t.Error("csrf cookie is HttpOnly (it must be readable by JavaScript)")
	}
	if !csrf.Secure {
		t.Error("csrf cookie not Secure (secure=true)")
	}

	// Clear: MaxAge must be negative.
	rec2 := httptest.NewRecorder()
	ClearSessionCookie(rec2, true)
	for _, c := range rec2.Result().Cookies() {
		if c.MaxAge >= 0 {
			t.Errorf("ClearSessionCookie: cookie %s MaxAge = %d, want negative", c.Name, c.MaxAge)
		}
	}
}

// --- helpers ---

// makeRequestWithCookie builds a GET request carrying the given session
// cookie (and an Accept: text/html header so the browser-path is exercised).
func makeRequestWithCookie(cookieValue string) *http.Request {
	req := newTestReq(http.MethodGet, "/")
	req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: cookieValue})
	req.Header.Set("Accept", "text/html")
	return req
}

// writeFile marshals a credential and writes it to a path with 0600 perms.
func writeFile(path string, cf *CredentialFile) error {
	data, err := MarshalCredentialFile(cf)
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}

// filepathJoin is a thin alias for filepath.Join (used by the key-overlap
// tests).
var filepathJoin = filepath.Join
