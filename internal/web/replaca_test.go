package web

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/auth"
	"gossipgrid/internal/server"
	"gossipgrid/internal/store"
)

// buildReplica constructs a single replica's web.Handler backed by a fresh
// in-memory SQLite store and a shared credential reloader. All replicas share
// the same credential file (the same signing key and generation), which is
// what makes a single login valid across all of them (no server-side session
// store).
func buildReplica(t *testing.T, credPath string, nodeID string) (*Handler, *auth.Reloader, func()) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "replica.db")
	st, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	rel := auth.NewReloader(credPath, time.Hour, time.Hour, nil)
	if err := rel.Start(t.Context()); err != nil {
		t.Fatalf("reloader.Start: %v", err)
	}
	cleanup := func() {
		rel.Shutdown()
		_ = st.Close()
	}
	provider := auth.NewProvider(rel)
	cfg := Config{
		Node:         server.Node{ID: nodeID},
		Store:        st,
		AuthProvider: provider,
		CookieSecure: false, // local dev over HTTP; no Secure flag.
	}
	return NewHandler(cfg), rel, cleanup
}

// newLoginClient returns a client that does not follow redirects (so a login
// response with a 303 is observed as-is).
func newLoginClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// loginReplica performs a POST /login on the given base URL with the password,
// returning the response.
func loginReplica(t *testing.T, base, password string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, base+"/login",
		strings.NewReader("password="+password))
	if err != nil {
		t.Fatalf("NewRequestWithContext POST /login: %v", err)
	}
	// ParseForm only treats the body as form data when the Content-Type is
	// application/x-www-form-urlencoded.
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := newLoginClient().Do(req)
	if err != nil {
		t.Fatalf("POST /login: %v", err)
	}
	return res
}

// getOnReplica performs a GET on the given URL with a client that carries the
// cookies in the jar, returning the response.
func getOnReplica(t *testing.T, jar *cookieJar, target string, extra func(*http.Request)) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext GET %s: %v", target, err)
	}
	if extra != nil {
		extra(req)
	}
	res, err := (&http.Client{Jar: jar}).Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", target, err)
	}
	return res
}

// TestFiveReplicaSingleLogin verifies the core cross-replica guarantee (Part
// F): a user logs in ONCE (on replica 0) and can make authenticated requests
// on all five replicas. The session cookie is signed with the common signing
// key, so every replica can verify it independently. No server-side session
// store, no Redis, no sticky sessions.
func TestFiveReplicaSingleLogin(t *testing.T) {
	dir := t.TempDir()
	credPath := filepath.Join(dir, "admin-credential.json")
	const password = "s3cur3-admin-pw"
	cf, err := auth.GenerateCredential(password, 1000, 1, nil)
	if err != nil {
		t.Fatalf("GenerateCredential: %v", err)
	}
	if err := writeCredFile(credPath, cf); err != nil {
		t.Fatalf("writeCredFile: %v", err)
	}

	const n = 5
	replicas := make([]*httptest.Server, n)
	cleanups := make([]func(), n)
	for i := 0; i < n; i++ {
		h, _, cleanup := buildReplica(t, credPath, "gossipgrid-"+itoa(i))
		srv := httptest.NewServer(h)
		replicas[i] = srv
		cleanups[i] = cleanup
	}
	defer func() {
		for i := n - 1; i >= 0; i-- {
			replicas[i].Close()
			cleanups[i]()
		}
	}()

	// Log in on replica 0.
	res := loginReplica(t, replicas[0].URL, password)
	var sessionCookie, csrfCookie *http.Cookie
	for _, c := range res.Cookies() {
		if c.Name == auth.SessionCookieName {
			sessionCookie = c
		}
		if c.Name == auth.CSRFCookieName {
			csrfCookie = c
		}
	}
	drainBody(t, res)
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("login on replica 0: status %d, want 303 (redirect to /)", res.StatusCode)
	}
	if sessionCookie == nil || csrfCookie == nil {
		t.Fatalf("login did not set session+csrf cookies (got %v)", res.Cookies())
	}

	jar := &cookieJar{cookies: map[string]*http.Cookie{
		auth.SessionCookieName: sessionCookie,
		auth.CSRFCookieName:    csrfCookie,
	}}

	// Make authenticated requests on ALL five replicas.
	for i := 0; i < n; i++ {
		r2 := getOnReplica(t, jar, replicas[i].URL+"/api/messages", nil)
		drainBody(t, r2)
		if r2.StatusCode != http.StatusOK {
			t.Errorf("GET /api/messages on replica %d: status %d, want 200 (single login valid across all replicas)", i, r2.StatusCode)
		}
	}

	// Also verify the UI root page (GET /) is accessible on all replicas.
	for i := 0; i < n; i++ {
		r2 := getOnReplica(t, jar, replicas[i].URL+"/", func(req *http.Request) {
			req.Header.Set("Accept", "text/html")
		})
		drainBody(t, r2)
		if r2.StatusCode != http.StatusOK {
			t.Errorf("GET / on replica %d: status %d, want 200", i, r2.StatusCode)
		}
	}
}

// TestFiveReplicaRotationInvalidatesOldSessions verifies the rotation
// guarantee: after a credential rotation (a new generation in the shared
// file), the old PASSWORD is rejected on all replicas, a new login with the
// new password succeeds, and the old GENERATION's sessions are rejected
// immediately on every replica that has loaded the new generation (no overlap
// window). This is the strong, immediate revocation: a rotation changes the
// generation, and sessions are bound to the generation.
func TestFiveReplicaRotationInvalidatesOldSessions(t *testing.T) {
	dir := t.TempDir()
	credPath := filepath.Join(dir, "admin-credential.json")
	const oldPassword = "old-admin-pw"
	const newPassword = "new-admin-pw"
	oldCf, err := auth.GenerateCredential(oldPassword, 1000, 1, nil)
	if err != nil {
		t.Fatalf("GenerateCredential(old): %v", err)
	}
	if err := writeCredFile(credPath, oldCf); err != nil {
		t.Fatalf("writeCredFile(old): %v", err)
	}

	const n = 5
	type replica struct {
		srv     *httptest.Server
		rel     *auth.Reloader
		cleanup func()
	}
	replicas := make([]replica, n)
	for i := 0; i < n; i++ {
		h, rel, cleanup := buildReplica(t, credPath, "gossipgrid-"+itoa(i))
		srv := httptest.NewServer(h)
		replicas[i] = replica{srv: srv, rel: rel, cleanup: cleanup}
	}
	defer func() {
		for i := n - 1; i >= 0; i-- {
			replicas[i].srv.Close()
			replicas[i].cleanup()
		}
	}()

	// Log in on replica 0 with the old password.
	res := loginReplica(t, replicas[0].srv.URL, oldPassword)
	var sessionCookie *http.Cookie
	for _, c := range res.Cookies() {
		if c.Name == auth.SessionCookieName {
			sessionCookie = c
		}
	}
	drainBody(t, res)
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("login (old) status %d, want 303", res.StatusCode)
	}
	if sessionCookie == nil {
		t.Fatal("no session cookie from old login")
	}
	jar := &cookieJar{cookies: map[string]*http.Cookie{auth.SessionCookieName: sessionCookie}}

	// Verify the old session works on all replicas before rotation.
	for i := 0; i < n; i++ {
		r2 := getOnReplica(t, jar, replicas[i].srv.URL+"/api/messages", nil)
		drainBody(t, r2)
		if r2.StatusCode != http.StatusOK {
			t.Fatalf("GET before rotation on replica %d: %d, want 200", i, r2.StatusCode)
		}
	}

	// Rotate: write the new credential (generation 2, new key) and force every
	// replica to reload it.
	newCf, err := auth.GenerateCredential(newPassword, 1000, 2, nil)
	if err != nil {
		t.Fatalf("GenerateCredential(new): %v", err)
	}
	if err := writeCredFile(credPath, newCf); err != nil {
		t.Fatalf("writeCredFile(new): %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			replicas[i].rel.TryReload()
		}(i)
	}
	wg.Wait()

	// After rotation, the old PASSWORD is rejected on all replicas (the
	// verifier changed). This is the strong, immediate guarantee of a rotation.
	for i := 0; i < n; i++ {
		res3 := loginReplica(t, replicas[i].srv.URL, oldPassword)
		drainBody(t, res3)
		if res3.StatusCode != http.StatusUnauthorized {
			t.Errorf("old password post-rotation on replica %d: %d, want 401 (verifier changed)", i, res3.StatusCode)
		}
	}

	// A new login with the new password succeeds on all replicas.
	for i := 0; i < n; i++ {
		res2 := loginReplica(t, replicas[i].srv.URL, newPassword)
		drainBody(t, res2)
		if res2.StatusCode != http.StatusSeeOther {
			t.Errorf("login (new) on replica %d: %d, want 303", i, res2.StatusCode)
		}
	}

	// The old-GENERATION session cookie is rejected on all replicas (no overlap
	// window — the generation changed, so the old session's GEN no longer
	// matches the replica's current generation).
	for i := 0; i < n; i++ {
		r2 := getOnReplica(t, jar, replicas[i].srv.URL+"/api/messages", nil)
		drainBody(t, r2)
		if r2.StatusCode != http.StatusUnauthorized {
			t.Errorf("old-generation session post-rotation on replica %d: %d, want 401 (no overlap window)", i, r2.StatusCode)
		}
	}
}

// TestFiveReplicaRotationNewSessionWorksCrossReplica verifies that after a
// rotation and a new login (on replica 0 with the new password), the new
// session (bound to the new generation) works on ALL replicas after they have
// all loaded the new generation. This confirms that the rotation does not
// break the cross-replica single-login guarantee: a new login is valid across
// all replicas (shared signing key, shared generation).
func TestFiveReplicaRotationNewSessionWorksCrossReplica(t *testing.T) {
	dir := t.TempDir()
	credPath := filepath.Join(dir, "admin-credential.json")
	const oldPassword = "old-admin-pw"
	const newPassword = "new-admin-pw"
	oldCf, _ := auth.GenerateCredential(oldPassword, 1000, 1, nil)
	newCf, _ := auth.GenerateCredential(newPassword, 1000, 2, nil)
	if err := writeCredFile(credPath, oldCf); err != nil {
		t.Fatalf("writeCredFile(old): %v", err)
	}

	const n = 5
	type replica struct {
		srv     *httptest.Server
		rel     *auth.Reloader
		cleanup func()
	}
	replicas := make([]replica, n)
	for i := 0; i < n; i++ {
		h, rel, cleanup := buildReplica(t, credPath, "gossipgrid-"+itoa(i))
		srv := httptest.NewServer(h)
		replicas[i] = replica{srv: srv, rel: rel, cleanup: cleanup}
	}
	defer func() {
		for i := n - 1; i >= 0; i-- {
			replicas[i].srv.Close()
			replicas[i].cleanup()
		}
	}()

	// Rotate: write the new credential and force every replica to reload it.
	if err := writeCredFile(credPath, newCf); err != nil {
		t.Fatalf("writeCredFile(new): %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); replicas[i].rel.TryReload() }(i)
	}
	wg.Wait()

	// New login on replica 0 with the new password.
	res := loginReplica(t, replicas[0].srv.URL, newPassword)
	var sessionCookie, csrfCookie *http.Cookie
	for _, c := range res.Cookies() {
		if c.Name == auth.SessionCookieName {
			sessionCookie = c
		}
		if c.Name == auth.CSRFCookieName {
			csrfCookie = c
		}
	}
	drainBody(t, res)
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("new login on replica 0: %d, want 303", res.StatusCode)
	}
	if sessionCookie == nil || csrfCookie == nil {
		t.Fatal("new login did not set session+csrf cookies")
	}
	jar := &cookieJar{cookies: map[string]*http.Cookie{
		auth.SessionCookieName: sessionCookie,
		auth.CSRFCookieName:    csrfCookie,
	}}

	// The new session (bound to the new generation) works on all replicas
	// after convergence (all loaded generation 2).
	for i := 0; i < n; i++ {
		r2 := getOnReplica(t, jar, replicas[i].srv.URL+"/api/messages", nil)
		drainBody(t, r2)
		if r2.StatusCode != http.StatusOK {
			t.Errorf("new session on replica %d after convergence: %d, want 200", i, r2.StatusCode)
		}
	}
}

// TestFiveReplicaMixedGenerationEventualConsistency verifies the documented
// eventual-consistency limitation: during Vault/VSO propagation, different
// replicas may temporarily observe different generations. A session issued
// under generation N is valid on a replica that has loaded generation N but
// rejected on a replica that has loaded generation N+1. This is NOT
// instantaneous global revocation — it is bounded by the time it takes for all
// replicas to converge on the new Secret version.
func TestFiveReplicaMixedGenerationEventualConsistency(t *testing.T) {
	dir := t.TempDir()
	credPath := filepath.Join(dir, "admin-credential.json")
	const oldPassword = "old-admin-pw"
	const newPassword = "new-admin-pw"
	oldCf, _ := auth.GenerateCredential(oldPassword, 1000, 1, nil)
	newCf, _ := auth.GenerateCredential(newPassword, 1000, 2, nil)
	if err := writeCredFile(credPath, oldCf); err != nil {
		t.Fatalf("writeCredFile(old): %v", err)
	}

	const n = 5
	type replica struct {
		srv     *httptest.Server
		rel     *auth.Reloader
		cleanup func()
	}
	replicas := make([]replica, n)
	for i := 0; i < n; i++ {
		h, rel, cleanup := buildReplica(t, credPath, "gossipgrid-"+itoa(i))
		srv := httptest.NewServer(h)
		replicas[i] = replica{srv: srv, rel: rel, cleanup: cleanup}
	}
	defer func() {
		for i := n - 1; i >= 0; i-- {
			replicas[i].srv.Close()
			replicas[i].cleanup()
		}
	}()

	// Log in on replica 0 (still generation 1).
	res := loginReplica(t, replicas[0].srv.URL, oldPassword)
	var sessionCookie *http.Cookie
	for _, c := range res.Cookies() {
		if c.Name == auth.SessionCookieName {
			sessionCookie = c
		}
	}
	// Close the body immediately (the only remaining use of res is its body).
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	if sessionCookie == nil {
		t.Fatal("no session cookie")
	}
	jar := &cookieJar{cookies: map[string]*http.Cookie{auth.SessionCookieName: sessionCookie}}

	// Write the new credential FIRST so the reloads pick it up.
	if err := writeCredFile(credPath, newCf); err != nil {
		t.Fatalf("writeCredFile(new): %v", err)
	}
	// Partially propagate the new generation: only replicas 0, 1, 2 reload
	// (they observe generation 2); replicas 3, 4 still observe generation 1.
	// This models the eventual-consistency window of Vault/VSO propagation.
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); replicas[i].rel.TryReload() }(i)
	}
	wg.Wait()

	// On replicas 0-2 (loaded generation 2), the old-generation session is
	// rejected (no overlap window). On replicas 3-4 (still generation 1), the
	// old-generation session is still valid (they have not converged yet).
	for i := 0; i < 3; i++ {
		r2 := getOnReplica(t, jar, replicas[i].srv.URL+"/api/messages", nil)
		drainBody(t, r2)
		if r2.StatusCode != http.StatusUnauthorized {
			t.Errorf("replica %d (loaded gen 2): old session status %d, want 401", i, r2.StatusCode)
		}
	}
	for i := 3; i < n; i++ {
		r2 := getOnReplica(t, jar, replicas[i].srv.URL+"/api/messages", nil)
		drainBody(t, r2)
		if r2.StatusCode != http.StatusOK {
			t.Errorf("replica %d (still gen 1): old session status %d, want 200 (eventual consistency)", i, r2.StatusCode)
		}
	}
}

// writeCredFile marshals a credential and writes it to a path.
func writeCredFile(path string, cf *auth.CredentialFile) error {
	data, err := auth.MarshalCredentialFile(cf)
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}

// itoa is a small int-to-string helper (avoids importing strconv in this
// test file).
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	digits := ""
	negative := i < 0
	if negative {
		i = -i
	}
	for i > 0 {
		digits = string(rune('0'+i%10)) + digits
		i /= 10
	}
	if negative {
		digits = "-" + digits
	}
	return digits
}

// cookieJar is a minimal http.CookieJar for the 5-replica tests. It carries the
// same cookies to every host (a "browser" that has logged in once and visits
// multiple replicas), modelling the cross-replica single-login behavior without
// a real browser.
type cookieJar struct {
	mu      sync.Mutex
	cookies map[string]*http.Cookie
}

func (j *cookieJar) SetCookies(_ *url.URL, cookies []*http.Cookie) {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, c := range cookies {
		j.cookies[c.Name] = c
	}
}

func (j *cookieJar) Cookies(_ *url.URL) []*http.Cookie {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]*http.Cookie, 0, len(j.cookies))
	for _, c := range j.cookies {
		out = append(out, c)
	}
	return out
}
