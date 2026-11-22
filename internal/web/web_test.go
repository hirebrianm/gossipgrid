package web

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gossipgrid/internal/auth"
	"gossipgrid/internal/server"
	"gossipgrid/internal/store"
)

// newTestServer builds a single-replica web.Handler with a valid credential
// and returns the httptest.Server and a cleanup function. It is the common
// setup for the single-replica web tests.
func newTestServer(t *testing.T, password string) (*httptest.Server, func()) {
	t.Helper()
	dir := t.TempDir()
	credPath := filepath.Join(dir, "admin-credential.json")
	cf, err := auth.GenerateCredential(password, 1000, 1, nil)
	if err != nil {
		t.Fatalf("GenerateCredential: %v", err)
	}
	if err := writeCredFile(credPath, cf); err != nil {
		t.Fatalf("writeCredFile: %v", err)
	}
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	rel := auth.NewReloader(credPath, time.Hour, time.Hour, nil)
	if err := rel.Start(t.Context()); err != nil {
		t.Fatalf("reloader.Start: %v", err)
	}
	provider := auth.NewProvider(rel)
	h := NewHandler(Config{
		Node:         server.Node{ID: "gossipgrid-0"},
		Store:        st,
		AuthProvider: provider,
		CookieSecure: false,
	})
	srv := httptest.NewServer(h)
	cleanup := func() {
		srv.Close()
		rel.Shutdown()
		_ = st.Close()
	}
	return srv, cleanup
}

// noRedirectClient is a client that does not follow redirects, so a 3xx
// response is observed as-is (the tests assert on the first response's status
// code and Location header).
func noRedirectClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// doRequest performs a request with the given method, url, and optional body
// bytes, returning the response (without following redirects). It uses the test
// context (noctx) and does not close the response body; callers close it via
// drainBody when needed.
func doRequest(t *testing.T, method, url string, body []byte, extra func(*http.Request)) *http.Response {
	t.Helper()
	var rd io.Reader
	if body != nil {
		rd = strings.NewReader(string(body))
	}
	req, err := http.NewRequestWithContext(t.Context(), method, url, rd)
	if err != nil {
		t.Fatalf("NewRequestWithContext %s %s: %v", method, url, err)
	}
	if extra != nil {
		extra(req)
	}
	res, err := noRedirectClient().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return res
}

// doLogin performs a POST /login with the given password and a client that does
// not follow redirects. It returns the login status code and the session and
// CSRF cookies (the body is drained and closed internally, so callers never
// hold a *http.Response).
func doLogin(t *testing.T, base, password string) (int, *http.Cookie, *http.Cookie) {
	t.Helper()
	client := &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+"/login",
		strings.NewReader("password="+password))
	if err != nil {
		t.Fatalf("NewRequestWithContext POST /login: %v", err)
	}
	// ParseForm only treats the body as form data when the Content-Type is
	// application/x-www-form-urlencoded.
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST /login: %v", err)
	}
	defer drainBody(t, res)
	return res.StatusCode, findCookie(res, auth.SessionCookieName), findCookie(res, auth.CSRFCookieName)
}

// drainBody closes a response body (and discards any unread content).
func drainBody(t *testing.T, res *http.Response) {
	t.Helper()
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
}

// findCookie returns the first cookie with the given name, or nil.
func findCookie(res *http.Response, name string) *http.Cookie {
	for _, c := range res.Cookies() {
		if c.Name == name {
			return c
		}
	}
	return nil
}

// TestLoginSuccess verifies that a correct password on POST /login returns 303
// (redirect to /) and sets the session + CSRF cookies.
func TestLoginSuccess(t *testing.T) {
	const password = "admin-pw"
	srv, cleanup := newTestServer(t, password)
	defer cleanup()

	status, session, csrf := doLogin(t, srv.URL, password)
	if status != http.StatusSeeOther {
		t.Fatalf("login status = %d, want 303", status)
	}
	if session == nil || csrf == nil {
		t.Error("login did not set both cookies (session and/or csrf missing)")
	}
}

// TestLoginFailure verifies that a wrong password on POST /login returns 401
// and does NOT set a session cookie.
func TestLoginFailure(t *testing.T) {
	srv, cleanup := newTestServer(t, "correct-pw")
	defer cleanup()

	status, session, _ := doLogin(t, srv.URL, "wrong-pw")
	if status != http.StatusUnauthorized {
		t.Fatalf("login (wrong password) status = %d, want 401", status)
	}
	if session != nil {
		t.Error("login (wrong password) set a session cookie")
	}
}

// TestLoginMissingPassword verifies that a POST /login with no password
// returns 400.
func TestLoginMissingPassword(t *testing.T) {
	srv, cleanup := newTestServer(t, "pw")
	defer cleanup()

	status, _, _ := doLogin(t, srv.URL, "")
	if status != http.StatusBadRequest {
		t.Fatalf("login (no password) status = %d, want 400", status)
	}
}

// TestUnauthenticatedAPIRequest verifies that a GET /api/messages without a
// session cookie returns 401 (for an API request) and does not serve the
// messages.
func TestUnauthenticatedAPIRequest(t *testing.T) {
	srv, cleanup := newTestServer(t, "pw")
	defer cleanup()

	res := doRequest(t, http.MethodGet, srv.URL+"/api/messages", nil, nil)
	defer drainBody(t, res)
	// No Accept: text/html → API request → JSON 401.
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /api/messages (unauthenticated) status = %d, want 401", res.StatusCode)
	}
}

// TestUnauthenticatedBrowserRequest verifies that a GET / (the root UI page)
// without a session cookie returns a redirect to /login (for a browser
// request with Accept: text/html).
func TestUnauthenticatedBrowserRequest(t *testing.T) {
	srv, cleanup := newTestServer(t, "pw")
	defer cleanup()

	res := doRequest(t, http.MethodGet, srv.URL+"/", nil, func(req *http.Request) {
		req.Header.Set("Accept", "text/html")
	})
	defer drainBody(t, res)
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("GET / (unauthenticated browser) status = %d, want 303 (redirect to /login)", res.StatusCode)
	}
	if loc := res.Header.Get("Location"); loc != "/login" {
		t.Errorf("redirect location = %q, want /login", loc)
	}
}

// TestAuthenticatedMessageFlow verifies the full authenticated flow: log in,
// submit a message (with the CSRF header), and read it back. The message
// submit requires the CSRF token (the cookie + header must match).
func TestAuthenticatedMessageFlow(t *testing.T) {
	const password = "admin-pw"
	srv, cleanup := newTestServer(t, password)
	defer cleanup()

	status, session, csrf := doLogin(t, srv.URL, password)
	if status != http.StatusSeeOther {
		t.Fatalf("login status = %d, want 303", status)
	}
	if session == nil || csrf == nil {
		t.Fatal("login did not set session+csrf cookies")
	}

	// A client that carries both cookies.
	jar := &cookieJar{cookies: map[string]*http.Cookie{
		auth.SessionCookieName: session,
		auth.CSRFCookieName:    csrf,
	}}
	jarClient := &http.Client{Jar: jar}

	// Submit a message (POST, requires the CSRF header).
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/api/messages",
		strings.NewReader(`{"body":"hello world"}`))
	if err != nil {
		t.Fatalf("NewRequestWithContext POST /api/messages: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(auth.CSRFHeader, csrf.Value)
	res2, err := jarClient.Do(req)
	if err != nil {
		t.Fatalf("POST /api/messages: %v", err)
	}
	drainBody(t, res2)
	if res2.StatusCode != http.StatusCreated {
		t.Fatalf("POST /api/messages (authenticated) status = %d, want 201", res2.StatusCode)
	}

	// Read it back.
	req3, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/api/messages?limit=10", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext GET /api/messages: %v", err)
	}
	res3, err := jarClient.Do(req3)
	if err != nil {
		t.Fatalf("GET /api/messages: %v", err)
	}
	drainBody(t, res3)
	if res3.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/messages (authenticated) status = %d, want 200", res3.StatusCode)
	}
}

// TestCSRFRequiredForPost verifies that a POST /api/messages with a valid
// session but a MISSING or MISMATCHED CSRF token returns 403 (the CSRF check
// fails). The auth passes (valid session) but the CSRF check rejects.
func TestCSRFRequiredForPost(t *testing.T) {
	const password = "admin-pw"
	srv, cleanup := newTestServer(t, password)
	defer cleanup()

	_, session, _ := doLogin(t, srv.URL, password)
	if session == nil {
		t.Fatal("no session cookie")
	}

	// Case 1: session cookie present but NO csrf cookie and NO header.
	t.Run("missing csrf token", func(t *testing.T) {
		r := doRequest(t, http.MethodPost, srv.URL+"/api/messages", []byte(`{"body":"x"}`), func(req *http.Request) {
			req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: session.Value})
		})
		drainBody(t, r)
		if r.StatusCode != http.StatusForbidden {
			t.Errorf("POST (no csrf) status = %d, want 403", r.StatusCode)
		}
	})

	// Case 2: session + csrf cookie present but the header is MISMATCHED.
	t.Run("mismatched csrf token", func(t *testing.T) {
		r := doRequest(t, http.MethodPost, srv.URL+"/api/messages", []byte(`{"body":"x"}`), func(req *http.Request) {
			req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: session.Value})
			req.AddCookie(&http.Cookie{Name: auth.CSRFCookieName, Value: "correct-token"})
			req.Header.Set(auth.CSRFHeader, "wrong-token")
		})
		drainBody(t, r)
		if r.StatusCode != http.StatusForbidden {
			t.Errorf("POST (mismatched csrf) status = %d, want 403", r.StatusCode)
		}
	})
}

// TestHealthzReadyzUnauthenticated verifies that /healthz and /readyz return
// 200 without a session cookie (Kubernetes probes must always reach them).
func TestHealthzReadyzUnauthenticated(t *testing.T) {
	srv, cleanup := newTestServer(t, "pw")
	defer cleanup()

	for _, path := range []string{"/healthz", "/readyz"} {
		res := doRequest(t, http.MethodGet, srv.URL+path, nil, nil)
		drainBody(t, res)
		if res.StatusCode != http.StatusOK {
			t.Errorf("GET %s (unauthenticated) status = %d, want 200", path, res.StatusCode)
		}
	}
}

// TestDiagnosticsAuthenticated verifies that GET /api/diagnostics returns 200
// with a valid session. It is authenticated (not in the unauthenticated set).
func TestDiagnosticsAuthenticated(t *testing.T) {
	const password = "admin-pw"
	srv, cleanup := newTestServer(t, password)
	defer cleanup()

	_, session, _ := doLogin(t, srv.URL, password)
	if session == nil {
		t.Fatal("no session cookie")
	}

	r := doRequest(t, http.MethodGet, srv.URL+"/api/diagnostics", nil, func(req *http.Request) {
		req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: session.Value})
	})
	drainBody(t, r)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/diagnostics (authenticated) status = %d, want 200", r.StatusCode)
	}
}

// TestDiagnosticsUnauthenticated verifies that GET /api/diagnostics without a
// session returns 401 (it is NOT in the unauthenticated set).
func TestDiagnosticsUnauthenticated(t *testing.T) {
	srv, cleanup := newTestServer(t, "pw")
	defer cleanup()

	res := doRequest(t, http.MethodGet, srv.URL+"/api/diagnostics", nil, nil)
	drainBody(t, res)
	if res.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET /api/diagnostics (unauthenticated) status = %d, want 401", res.StatusCode)
	}
}

// TestTamperedSessionCookie verifies that a tampered session cookie (the
// signature no longer matches) returns 401. The cookie's payload is modified
// after signing.
func TestTamperedSessionCookie(t *testing.T) {
	const password = "admin-pw"
	srv, cleanup := newTestServer(t, password)
	defer cleanup()

	_, session, _ := doLogin(t, srv.URL, password)
	if session == nil {
		t.Fatal("no session cookie")
	}
	// Tamper the payload (flip a character before the ".").
	parts := strings.SplitN(session.Value, ".", 2)
	payload := []byte(parts[0])
	payload[0] ^= 1
	tampered := string(payload) + "." + parts[1]

	r := doRequest(t, http.MethodGet, srv.URL+"/api/messages", nil, func(req *http.Request) {
		req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: tampered})
	})
	drainBody(t, r)
	if r.StatusCode != http.StatusUnauthorized {
		t.Errorf("GET /api/messages (tampered cookie) status = %d, want 401", r.StatusCode)
	}
}

// TestPeerMTLSNotOnPublicRouter verifies that the /internal/* routes are NOT
// registered on the public HTTP router (the peer mTLS separation). A request
// to /internal/status on the public handler returns 404 (not found).
func TestPeerMTLSNotOnPublicRouter(t *testing.T) {
	srv, cleanup := newTestServer(t, "pw")
	defer cleanup()

	res := doRequest(t, http.MethodGet, srv.URL+"/internal/status", nil, nil)
	drainBody(t, res)
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("GET /internal/status on public router status = %d, want 404 (peer mTLS routes are on the peer listener only)", res.StatusCode)
	}
}

// TestLogoutClearsCookies verifies that POST /logout (with a valid session and
// CSRF token) clears the session and CSRF cookies (MaxAge negative) and
// redirects to /login.
func TestLogoutClearsCookies(t *testing.T) {
	const password = "admin-pw"
	srv, cleanup := newTestServer(t, password)
	defer cleanup()

	status, session, csrf := doLogin(t, srv.URL, password)
	if status != http.StatusSeeOther {
		t.Fatalf("login status = %d, want 303", status)
	}
	if session == nil || csrf == nil {
		t.Fatal("login did not set both cookies")
	}

	// POST /logout with the session + csrf cookie and the CSRF header.
	r := doRequest(t, http.MethodPost, srv.URL+"/logout", nil, func(req *http.Request) {
		req.AddCookie(&http.Cookie{Name: auth.SessionCookieName, Value: session.Value})
		req.AddCookie(&http.Cookie{Name: auth.CSRFCookieName, Value: csrf.Value})
		req.Header.Set(auth.CSRFHeader, csrf.Value)
	})
	drainBody(t, r)
	if r.StatusCode != http.StatusSeeOther {
		t.Fatalf("logout status = %d, want 303 (redirect to /login)", r.StatusCode)
	}
	if loc := r.Header.Get("Location"); loc != "/login" {
		t.Errorf("logout redirect location = %q, want /login", loc)
	}
	// Verify the cookies are cleared (MaxAge negative).
	if findCookie(r, auth.SessionCookieName) == nil || findCookie(r, auth.CSRFCookieName) == nil {
		t.Errorf("logout did not return both cookies to clear them")
	}
	for _, c := range r.Cookies() {
		if c.Name == auth.SessionCookieName || c.Name == auth.CSRFCookieName {
			if c.MaxAge >= 0 {
				t.Errorf("logout cookie %s MaxAge = %d, want negative", c.Name, c.MaxAge)
			}
		}
	}
}

// TestCredentialUnavailableReturns503 verifies that when the credential file is
// missing (the reloader is in StateUnavailable), POST /login returns 503 (not
// 401) and authenticated requests return 503 (fail-closed).
func TestCredentialUnavailableReturns503(t *testing.T) {
	dir := t.TempDir()
	credPath := filepath.Join(dir, "missing.json")
	dbPath := filepath.Join(t.TempDir(), "test.db")
	st, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	rel := auth.NewReloader(credPath, time.Hour, time.Hour, nil)
	if err := rel.Start(t.Context()); err != nil {
		t.Fatalf("reloader.Start: %v", err)
	}
	provider := auth.NewProvider(rel)
	h := NewHandler(Config{
		Node:         server.Node{ID: "gossipgrid-0"},
		Store:        st,
		AuthProvider: provider,
		CookieSecure: false,
	})
	srv := httptest.NewServer(h)
	defer func() {
		srv.Close()
		rel.Shutdown()
		_ = st.Close()
	}()

	// POST /login → 503 (credential unavailable, not a wrong password).
	status, _, _ := doLogin(t, srv.URL, "any")
	if status != http.StatusServiceUnavailable {
		t.Errorf("login (credential unavailable) status = %d, want 503", status)
	}

	// GET /api/messages → 503.
	r := doRequest(t, http.MethodGet, srv.URL+"/api/messages", nil, nil)
	drainBody(t, r)
	if r.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("GET /api/messages (credential unavailable) status = %d, want 503", r.StatusCode)
	}

	// /healthz and /readyz remain 200 (unauthenticated, always reachable).
	for _, path := range []string{"/healthz", "/readyz"} {
		hz := doRequest(t, http.MethodGet, srv.URL+path, nil, nil)
		drainBody(t, hz)
		if hz.StatusCode != http.StatusOK {
			t.Errorf("GET %s (credential unavailable) status = %d, want 200", path, hz.StatusCode)
		}
	}
}
