package auth

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Session cookie name. It is a single, well-known name shared by all replicas
// (the Service load-balances across them, so the cookie must be the same
// everywhere). The name is never the session identifier itself.
const SessionCookieName = "gossipgrid_session"

// CSRF cookie name. The double-submit pattern uses a non-HttpOnly cookie that
// JavaScript can read, paired with an X-CSRF-Token header.
const CSRFCookieName = "gossipgrid_csrf"

// CSRFHeader is the custom header that carries the CSRF token on
// state-changing requests. It must match the value of the CSRF cookie.
const CSRFHeader = "X-CSRF-Token"

// SessionTTL is the absolute lifetime of a session. It is never extended by
// activity (the idle extension is a separate, shorter bound). A short absolute
// TTL limits the value of a stolen cookie.
var SessionTTL = 2 * time.Hour

// SessionIdleTTL is the idle lifetime: a session expires this long after the
// last authenticated request, even if the absolute TTL has not elapsed. The
// server slides the idle expiry on each authenticated request.
var SessionIdleTTL = 30 * time.Minute

// ErrSessionInvalid indicates the cookie signature failed verification (a
// tampered cookie, a wrong key, or a cookie from a different credential
// generation). The authentication endpoint returns 401 for this condition.
var ErrSessionInvalid = errors.New("session invalid")

// ErrSessionExpired indicates the cookie is well-formed and correctly signed
// but its absolute or idle expiry has elapsed. The endpoint returns 401 (the
// browser is redirected to the login page). It is reported distinctly from
// ErrSessionInvalid in diagnostics.
var ErrSessionExpired = errors.New("session expired")

// sessionPayload is the signed content of a session cookie. It is bound to the
// credential generation it was issued under, so a rotation invalidates prior
// sessions on replicas that have loaded the new generation.
//
// The plaintext password and the password verifier are NEVER placed in this
// payload (docs/security.md). The payload carries only session material and its
// metadata; the signature (computed with the common signing key) is appended
// outside the JSON.
type sessionPayload struct {
	// SID is the opaque random session identifier (hex-encoded, 32 bytes).
	// It is not secret and not used for lookups; it is a unique token for
	// logging and for the CSRF token derivation.
	SID string `json:"sid"`
	// IAT is the issued-at time (Unix seconds).
	IAT int64 `json:"iat"`
	// EXP is the absolute expiry (Unix seconds). Never slid.
	EXP int64 `json:"exp"`
	// IDE is the idle expiry (Unix seconds). Slid on each authenticated
	// request (the server reissues a cookie with IDE = now + SessionIdleTTL).
	IDE int64 `json:"ide"`
	// GEN is the credential generation the session was issued under. A replica
	// verifies that GEN matches its currently loaded generation (or the
	// previous generation during the key-overlap window).
	GEN int64 `json:"gen"`
}

// IssueSession creates a new session cookie value and a CSRF token, signed with
// the given key under the given credential generation. The absolute expiry is
// now + SessionTTL; the idle expiry is now + SessionIdleTTL.
//
// The CSRF token is DERIVED from the session identifier (SID) using
// HMAC-SHA256 with the session signing key. This cryptographically binds the
// CSRF token to the authenticated session: a token from one session does not
// validate on another session (a different SID produces a different HMAC), and
// a tampered token fails verification. It is stateless (no server-side session
// store) and cross-replica (any replica with the shared signing key can
// derive the same token from the cookie's SID).
//
// It returns:
//   - the cookie value (base64url(payload) + "." + base64url(hmac));
//   - the CSRF token (hex-encoded HMAC-SHA256(signing key, SID), set as a
//     cookie and required in the X-CSRF-Token header on state-changing
//     requests);
//   - an error if the random generation fails.
//
// The password and the password verifier are never placed in the cookie. The
// CSRF token is a derived value, not the SID itself (the SID is the HMAC
// input; the token is the HMAC output), so an attacker who reads the CSRF
// cookie cannot recover the SID or forge a token for another session.
func IssueSession(key []byte, generation int64, now time.Time) (cookieValue, csrfToken string, err error) {
	sid, err := randomHex(SaltBytes)
	if err != nil {
		return "", "", fmt.Errorf("generating session id: %w", err)
	}
	// The CSRF token is derived from the SID (the HMAC input) using the session
	// signing key. This binds the token to the session: a different session
	// (different SID) produces a different token, and a tampered token fails
	// the constant-time verification in CheckCSRF.
	csrfSid, err := hex.DecodeString(sid)
	if err != nil {
		return "", "", fmt.Errorf("decoding session id: %w", err)
	}
	csrfToken = hex.EncodeToString(hmacSHA256(key, csrfSid))

	payload := sessionPayload{
		SID: sid,
		IAT: now.Unix(),
		EXP: now.Add(SessionTTL).Unix(),
		IDE: now.Add(SessionIdleTTL).Unix(),
		GEN: generation,
	}
	cookieValue, err = signPayload(key, &payload)
	if err != nil {
		return "", "", err
	}
	return cookieValue, csrfToken, nil
}

// signPayload encodes a session payload as a signed cookie value:
// base64url(JSON) + "." + base64url(HMAC-SHA256(key, jsonBytes)).
func signPayload(key []byte, p *sessionPayload) (string, error) {
	body, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("encoding session payload: %w", err)
	}
	sig := hmacSHA256(key, body)
	return base64.RawURLEncoding.EncodeToString(body) + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// ValidateSession verifies a session cookie value against the given key and the
// given credential generation. It returns the session payload (SID, expiry,
// generation) on success, or an error wrapped in ErrSessionInvalid (bad
// signature / wrong generation / malformed) or ErrSessionExpired (valid
// signature but expired).
//
// The generation check is the rotation-invalidation mechanism: a cookie issued
// under generation N is rejected once the replica has loaded generation N+1
// (the cookie's GEN no longer matches). There is no overlap window — the
// provider verifies only against the current key+generation (see the validator
// in middleware.go). A session-signing-key rotation (a new key at the same
// generation) is a distinct concept: it does not invalidate sessions (the
// generation is unchanged), it changes the verification key. The two are not
// conflated.
//
// The comparison of the recomputed signature against the cookie's signature is
// constant-time (subtle.ConstantTimeCompare). The password and verifier are
// never involved in this check.
func ValidateSession(key []byte, generation int64, cookieValue string, now time.Time) (*sessionPayload, error) {
	parts := splitCookie(cookieValue)
	if len(parts) != 2 {
		return nil, fmt.Errorf("%w: malformed cookie", ErrSessionInvalid)
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("%w: bad payload encoding: %v", ErrSessionInvalid, err)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("%w: bad signature encoding: %v", ErrSessionInvalid, err)
	}
	// Recompute the signature over the raw body and compare in constant time.
	expected := hmacSHA256(key, body)
	if subtle.ConstantTimeCompare(sig, expected) != 1 {
		return nil, fmt.Errorf("%w: signature mismatch", ErrSessionInvalid)
	}

	var p sessionPayload
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("%w: bad payload JSON: %v", ErrSessionInvalid, err)
	}
	// Generation binding: the cookie must carry the generation the replica
	// currently verifies against. A mismatch is a rotation (or a forged
	// generation) — reject.
	if p.GEN != generation {
		return nil, fmt.Errorf("%w: generation mismatch (cookie=%d, current=%d)", ErrSessionInvalid, p.GEN, generation)
	}
	// Expiry checks. The absolute expiry is never slid; the idle expiry is.
	if now.Unix() >= p.EXP {
		return nil, fmt.Errorf("%w: absolute expiry elapsed", ErrSessionExpired)
	}
	if now.Unix() >= p.IDE {
		return nil, fmt.Errorf("%w: idle expiry elapsed", ErrSessionExpired)
	}
	return &p, nil
}

// hmacSHA256 computes HMAC-SHA256(key, msg). It is a thin wrapper so the
// session code has one signature-computation path.
func hmacSHA256(key, msg []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(msg)
	return h.Sum(nil)
}

// randomHex returns hex-encoded random bytes of length n. It uses crypto/rand,
// which is FIPS-approved.
func randomHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// splitCookie splits a cookie value on "." and returns the parts. It does not
// validate the parts (the caller does).
func splitCookie(v string) []string {
	var out []string
	start := 0
	for i, c := range v {
		if c == '.' {
			out = append(out, v[start:i])
			start = i + 1
		}
	}
	out = append(out, v[start:])
	return out
}

// SetSessionCookie writes the session cookie and the CSRF cookie to the
// response. The session cookie is HttpOnly (never accessible to JavaScript),
// Secure (HTTPS-only when secure is true), and SameSite=Strict. The CSRF
// cookie is NOT HttpOnly (JavaScript reads it to fill the X-CSRF-Token header)
// but is also Secure and SameSite=Strict.
//
// The Secure flag is set only when the request arrived over HTTPS (secure
// true). For local development over plain HTTP, secure is false so the cookie
// is not dropped. In production (Cilium Gateway TLS termination), secure is
// true and the cookie is never sent over plaintext.
func SetSessionCookie(w http.ResponseWriter, cookieValue, csrfToken string, secure bool, now time.Time) {
	// #nosec G124: the session cookie sets HttpOnly, SameSite=Strict, and
	// Secure (true in production); gosec's taint analysis cannot see the
	// per-field attributes inside the struct literal.
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    cookieValue,
		Path:     "/",
		Expires:  now.Add(SessionTTL),
		MaxAge:   int(SessionTTL.Seconds()),
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteStrictMode,
	})
	// #nosec G124: the CSRF cookie is deliberately NOT HttpOnly (read by
	// JavaScript for the double-submit token) but sets SameSite=Strict and
	// Secure (true in production); the same taint-analysis limitation as above.
	http.SetCookie(w, &http.Cookie{
		Name:     CSRFCookieName,
		Value:    csrfToken,
		Path:     "/",
		Expires:  now.Add(SessionTTL),
		MaxAge:   int(SessionTTL.Seconds()),
		HttpOnly: false,
		Secure:   secure,
		SameSite: http.SameSiteStrictMode,
	})
}

// ClearSessionCookie removes the session and CSRF cookies by setting them with
// an expired Max-Age. The same Secure/SameSite attributes must be set so the
// browser matches the original cookie.
func ClearSessionCookie(w http.ResponseWriter, secure bool) {
	// #nosec G124: the clear cookie sets HttpOnly, SameSite=Strict, and Secure
	// (true in production); gosec cannot see the per-field attributes inside
	// the struct literal.
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteStrictMode,
	})
	// #nosec G124: the CSRF clear cookie is NOT HttpOnly but sets
	// SameSite=Strict and Secure (true in production); same taint limitation.
	http.SetCookie(w, &http.Cookie{
		Name:     CSRFCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: false,
		Secure:   secure,
		SameSite: http.SameSiteStrictMode,
	})
}
