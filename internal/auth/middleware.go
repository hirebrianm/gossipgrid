package auth

import (
	"bytes"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// Result is the outcome of an authentication decision for a request. It is
// returned by the Provider and consumed by the middleware and the web handler.
// It distinguishes the conditions that map to different HTTP status codes:
//
//   - Allowed: the request is authenticated and (for state-changing methods)
//     the CSRF check passed. The handler proceeds.
//   - Unauthenticated (401): no session cookie, a tampered/expired cookie, or a
//     wrong password on a login attempt. The browser is redirected to /login.
//   - CredentialUnavailable (503): the credential is absent, failed to load, or
//     is stale past the bound. The endpoint is unavailable; /healthz and
//     /readyz remain 200.
//   - CSRFRejected (403): a state-changing request lacked a valid
//     X-CSRF-Token header matching the CSRF cookie.
//
// The Condition field is the authoritative signal; the HTTP code is derived by
// the middleware. The message is a short, non-sensitive diagnostic (never the
// password, verifier, or signing key).
type Result struct {
	// Allowed reports whether the request may proceed to the handler.
	Allowed bool
	// Condition is the reason for a non-allowed result (empty when Allowed).
	Condition Condition
	// Message is a short, non-sensitive diagnostic for logging.
	Message string
}

// Condition classifies a non-allowed authentication result.
type Condition string

// Authentication condition codes.
const (
	// ConditionOK is the allowed condition (no error).
	ConditionOK Condition = ""
	// ConditionUnauthenticated maps to HTTP 401.
	ConditionUnauthenticated Condition = "unauthenticated"
	// ConditionCredentialUnavailable maps to HTTP 503.
	// #nosec G101: this is an authentication-condition code, not a credential.
	ConditionCredentialUnavailable Condition = "credential_unavailable"
	// ConditionCSRFRejected maps to HTTP 403.
	ConditionCSRFRejected Condition = "csrf_rejected"
)

// Provider is the interface the web handler uses to make authentication
// decisions. It is implemented by the Reloader-backed DefaultProvider.
//
// A Provider is safe for concurrent use. It reads the reloader's snapshot
// (an immutable credential) and performs the cryptographic verification. It
// does not hold any lock across the password comparison (the comparison is
// fast and in-memory).
type Provider interface {
	// Authenticate checks the session cookie on the request and returns the
	// result. For a request with a valid session it returns ConditionOK. For
	// a request with no/invalid session it returns ConditionUnauthenticated.
	// When the credential is unavailable or stale it returns
	// ConditionCredentialUnavailable.
	Authenticate(r *http.Request, now time.Time) Result
	// VerifyPassword checks a submitted password against the current credential
	// and returns true on a match. It is used by the /login handler. When the
	// credential is unavailable, it returns (false, ErrCredentialUnavailable).
	VerifyPassword(password string) (bool, error)
	// Diagnostics returns the reloader's non-sensitive diagnostics.
	Diagnostics() Diagnostics
}

// DefaultProvider is the Reloader-backed Provider. It verifies session cookies
// with the current credential's signing key and generation only. A session
// issued under generation N is rejected once the replica has loaded generation
// N+1 (the cookie's GEN no longer matches), with NO overlap window.
//
// This is the rotation-invalidation mechanism: password-credential rotation
// (a new generation with a new password) invalidates old-generation sessions
// on every replica that has loaded the new credential. It is distinct from
// session-signing-key rotation (a new signing key that does not change the
// generation): the key is the mechanism for cross-replica verification, and the
// generation is the session-revocation policy. The two are not conflated.
//
// The "different replicas temporarily observe different Secret generations"
// behavior (Vault/VSO propagation) is eventual consistency, not instantaneous
// global revocation: a replica that has not yet loaded generation N+1 still
// verifies generation N sessions. Once it loads N+1, it immediately rejects
// them. There is no deliberate overlap window that extends old-session or
// old-password validity (docs/security.md).
type DefaultProvider struct {
	reloader *Reloader
	now      func() time.Time
}

// NewProvider creates a DefaultProvider backed by the given reloader. The
// reloader must have been started (or at least have a valid snapshot) for
// authentication to succeed.
func NewProvider(reloader *Reloader) *DefaultProvider {
	return &DefaultProvider{reloader: reloader, now: time.Now}
}

// WithClock replaces the clock. It is provided so tests can drive expiry and
// the key-overlap window deterministically.
func (p *DefaultProvider) WithClock(now func() time.Time) *DefaultProvider {
	p.now = now
	return p
}

// Authenticate implements the Provider interface. It reads the reloader's
// current snapshot and verifies the session cookie against the current
// credential's signing key and generation. It returns the Result.
//
// There is no overlap window: a session issued under a previous generation is
// rejected once the replica has loaded the new generation (the cookie's GEN no
// longer matches). This is the rotation-invalidation mechanism — see the
// type documentation for how this differs from session-signing-key rotation.
//
// The password and the password verifier are never involved in this check (the
// session cookie is a signed token, not a password). Only the signing key and
// the generation are used.
func (p *DefaultProvider) Authenticate(r *http.Request, now time.Time) Result {
	// The credential must be available (loaded, not stale) before any
	// authentication decision is made. An unavailable or stale credential
	// returns 503 — no path to an unauthenticated acceptance.
	if p.reloader.State() != StateLoaded {
		return Result{
			Condition: ConditionCredentialUnavailable,
			Message:   "credential unavailable or stale",
		}
	}

	cookie, err := r.Cookie(SessionCookieName)
	if err != nil || cookie.Value == "" {
		return Result{
			Condition: ConditionUnauthenticated,
			Message:   "no session",
		}
	}

	current := p.reloader.Snapshot()
	if current == nil {
		return Result{
			Condition: ConditionCredentialUnavailable,
			Message:   "no credential snapshot",
		}
	}

	// Verify the cookie against the current key + generation only. SigningKeyHex
	// returns the raw key bytes (decoded from the hex-encoded field).
	payload, verr := ValidateSession(current.SigningKeyHex(), current.Generation, cookie.Value, now)
	if verr == nil && payload != nil {
		return Result{Allowed: true, Condition: ConditionOK}
	}
	// Distinguish expired from invalid for diagnostics.
	if errors.Is(verr, ErrSessionExpired) {
		return Result{
			Condition: ConditionUnauthenticated,
			Message:   "session expired",
		}
	}
	return Result{
		Condition: ConditionUnauthenticated,
		Message:   "session invalid",
	}
}

// VerifyPassword implements the Provider interface. It checks the password
// against the current credential using a constant-time comparison. When the
// credential is unavailable or stale, it returns (false,
// ErrCredentialUnavailable) so the /login handler returns 503 (not 401) —
// a correct password must not be reported "wrong" when the credential is
// stuck in a bad state (docs/security.md, "Distinguishing availability from
// validity").
func (p *DefaultProvider) VerifyPassword(password string) (bool, error) {
	if p.reloader.State() != StateLoaded {
		return false, ErrCredentialUnavailable
	}
	current := p.reloader.Snapshot()
	if current == nil {
		return false, ErrCredentialUnavailable
	}
	return VerifyPassword(current, password), nil
}

// Diagnostics implements the Provider interface.
func (p *DefaultProvider) Diagnostics() Diagnostics {
	return p.reloader.Diagnostics()
}

// Snapshot returns the in-effect credential snapshot from the reloader. It is
// used by the web handler to read the current signing key and generation when
// issuing a session cookie. It returns nil when no valid credential is loaded.
//
// This method is not part of the Provider interface (which keeps the
// interface narrow for the middleware); it is a convenience for the web
// handler, which always uses a DefaultProvider.
func (p *DefaultProvider) Snapshot() *CredentialFile {
	return p.reloader.Snapshot()
}

// SigningKeyHex returns the signing key as raw bytes (decoded from hex). It is
// a convenience for the middleware and tests. It returns an empty slice when
// the key is empty or malformed.
func (cf *CredentialFile) SigningKeyHex() []byte {
	b, err := hexDecode(cf.SigningKey)
	if err != nil {
		return nil
	}
	return b
}

// isStateChanging reports whether the HTTP method is state-changing (requires
// CSRF validation). GET and HEAD are safe; everything else is state-changing.
func isStateChanging(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return false
	default:
		return true
	}
}

// CheckCSRF validates the session-bound CSRF token for state-changing
// requests, using the given signing key to derive the expected token. It is
// the primary CSRF check; the web handler calls it after authentication,
// passing the provider's current signing key.
//
// The token is cryptographically bound to the authenticated session: it is
// HMAC-SHA256(signing key, SID), where SID is the session identifier embedded
// in the session cookie. A token from one session (a different SID) does not
// validate on another session, and a tampered token fails the constant-time
// comparison. It is stateless (no server-side session store) and cross-replica
// (any replica with the shared signing key can derive the same token from the
// cookie's SID).
//
// The flow for a state-changing request:
//  1. Recover the SID from the session cookie payload (the cookie's signature
//     is verified by the provider's Authenticate, which runs first; a forged
//     cookie is rejected before CSRF is reached).
//  2. Compute the expected token = HMAC-SHA256(key, SID).
//  3. The X-CSRF-Token header must equal the CSRF cookie value (the
//     double-submit pattern: JavaScript reads the cookie and fills the header),
//     AND both must equal the expected token. A mismatch (a token from another
//     session, a tampered token, a missing header, or a reflected header-only
//     attack) returns false.
//
// It returns true when the request is not state-changing (GET/HEAD require no
// CSRF), or when the token validates against the session. The comparison is
// constant-time (subtle.ConstantTimeCompare).
func CheckCSRF(r *http.Request, key []byte, now time.Time) bool {
	if !isStateChanging(r) {
		return true
	}
	sessionCookie, err := r.Cookie(SessionCookieName)
	if err != nil || sessionCookie.Value == "" {
		return false
	}
	// Decode the payload to recover the SID (the signature and generation are
	// verified by Authenticate; here we only need the SID).
	parts := splitCookie(sessionCookie.Value)
	if len(parts) != 2 {
		return false
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return false
	}
	var p sessionPayload
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return false
	}
	if p.SID == "" {
		return false
	}
	// The expected token is HMAC-SHA256(signing key, SID).
	sid, err := hex.DecodeString(p.SID)
	if err != nil {
		return false
	}
	expected := hex.EncodeToString(hmacSHA256(key, sid))
	header := r.Header.Get(CSRFHeader)
	if header == "" {
		return false
	}
	// The CSRF cookie must also carry the token (double-submit: JavaScript
	// reads the cookie and fills the header). A header that does not match the
	// cookie is rejected (prevents a reflected attack where the attacker sets
	// only the header).
	csrfCookie, err := r.Cookie(CSRFCookieName)
	if err != nil || csrfCookie.Value == "" {
		return false
	}
	if subtle.ConstantTimeCompare([]byte(header), []byte(csrfCookie.Value)) != 1 {
		return false
	}
	// Finally, the token (header == cookie) must equal the expected HMAC of the
	// SID under the session's signing key. This is the session binding: a token
	// from another session (different SID) or a tampered token fails.
	return subtle.ConstantTimeCompare([]byte(csrfCookie.Value), []byte(expected)) == 1
}

// RequireAuth is an http middleware that authenticates the request using the
// given Provider. On an allowed result it calls the next handler. On a
// non-allowed result it writes the appropriate status (401/503/403) and does
// NOT call next.
//
// The onRejected callback (optional) is invoked instead of the default status
// write; the web handler uses it to redirect browser requests to /login and to
// write a JSON error for API requests.
func RequireAuth(p Provider, onRejected func(w http.ResponseWriter, r *http.Request, res Result)) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			res := p.Authenticate(r, time.Now())
			if res.Allowed {
				next.ServeHTTP(w, r)
				return
			}
			if onRejected != nil {
				onRejected(w, r, res)
				return
			}
			switch res.Condition {
			case ConditionCredentialUnavailable:
				http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			case ConditionCSRFRejected:
				http.Error(w, "forbidden", http.StatusForbidden)
			default:
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			}
		})
	}
}

// RequireCSRF is an http middleware that validates the session-bound CSRF
// token for state-changing requests using the given signing key. It must run
// AFTER RequireAuth (so the session is established and the CSRF cookie is
// present). On a valid token (or a safe method) it calls next; on a
// missing/mismatched/tampered token it writes 403 and does NOT call next.
//
// The key is the session signing key (from the provider's snapshot); it is
// used to derive the expected token from the session's SID. The
// onRejected callback (optional) is invoked instead of the default 403.
func RequireCSRF(key []byte, onRejected func(w http.ResponseWriter, r *http.Request)) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if CheckCSRF(r, key, time.Now()) {
				next.ServeHTTP(w, r)
				return
			}
			if onRejected != nil {
				onRejected(w, r)
				return
			}
			http.Error(w, "forbidden: CSRF token required", http.StatusForbidden)
		})
	}
}
