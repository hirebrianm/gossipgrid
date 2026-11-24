// Package web provides GossipGrid's browser interface (Milestone 6): an
// embedded single-page UI, the login/logout handlers, the diagnostics
// endpoint, and the authentication/CSRF middleware that protects the public
// message API and the UI.
//
// The handler reuses the existing public mux (built by
// server.NewWithStoreAndCallbackAndLifecycle) for the message API routes, so
// the message validation, the lifecycle admission checks, and the readiness
// gating are preserved unchanged. The web layer wraps that mux with
// authentication and CSRF, and adds the UI routes (/, /login, /logout,
// /api/diagnostics, /styles.css, /app.js, /index.html, /login.html).
//
// The peer mTLS listener is independent and unchanged: the /internal/* routes
// are served only on the peer listener (mTLS 1.3, mutual TLS, strict identity
// authorization) and are NOT registered on the public HTTP router (verified by
// a negative test in web_test.go).
package web

import (
	"encoding/json"
	"net/http"
	"time"

	"gossipgrid/internal/auth"
	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/readiness"
	"gossipgrid/internal/server"
	"gossipgrid/internal/store"
)

// Diagnostics is the non-sensitive diagnostic payload returned by
// GET /api/diagnostics. It is what the UI's "Diagnostics" panel renders.
// It never includes the credential material (verifier, signing key), the
// password, or the peer private key.
type Diagnostics struct {
	NodeID      string `json:"node_id"`
	Version     string `json:"version"`
	FIPSModule  string `json:"fips_module,omitempty"`
	FIPSEnabled bool   `json:"fips_enabled"`

	// Readiness.
	Ready            bool   `json:"ready"`
	LifecycleEnabled bool   `json:"lifecycle_enabled"`
	LifecycleState   string `json:"lifecycle_state,omitempty"`
	LifecycleReason  string `json:"lifecycle_reason,omitempty"`

	// Initial synchronization barrier (Milestone 7A enhancement, when the
	// barrier is enabled; the fields are omitted when disabled).
	InitialSyncEnabled   bool     `json:"initial_sync_enabled"`
	InitialSyncComplete  bool     `json:"initial_sync_complete"`
	InitialSyncCompleted int      `json:"initial_sync_completed"`
	InitialSyncExpected  int      `json:"initial_sync_expected"`
	InitialSyncMissing   []string `json:"initial_sync_missing,omitempty"`

	// Peers (from discovery).
	Peers            []PeerInfo `json:"peers"`
	PeerCount        int        `json:"peer_count"`
	ExpectedReplicas int        `json:"expected_replicas"`
	DiscoveryStale   bool       `json:"discovery_stale"`

	// Peer mTLS (when enabled).
	PeerTLSEnabled      bool   `json:"peer_tls_enabled"`
	PeerCertFingerprint string `json:"peer_cert_fingerprint,omitempty"`
	PeerReloadCount     int    `json:"peer_reload_count"`

	// Browser credential (when authentication is enabled).
	AuthEnabled          bool   `json:"auth_enabled"`
	CredentialState      string `json:"credential_state,omitempty"`
	CredentialGeneration int64  `json:"credential_generation,omitempty"`
}

// PeerInfo is a single discovered peer for the diagnostics payload.
type PeerInfo struct {
	Hostname string `json:"hostname"`
	Identity string `json:"identity,omitempty"`
	State    string `json:"state"`
}

// Config holds the web handler's dependencies. It is built by the caller
// (cmd/gossipgrid/main.go) from the existing in-process components.
type Config struct {
	Node      server.Node
	Store     *store.Store
	OnMessage server.MessageCallback
	Lifecycle *lifecycle.Controller

	// Version and FIPS state (from the build).
	Version     string
	FIPSModule  string
	FIPSEnabled bool

	// Discovery (optional; nil when discovery is not configured).
	DiscoverySnapshot func() DiscoverySnapshot

	// Peer mTLS diagnostics (optional; nil when peer TLS is not enabled).
	PeerTLDiagnostics func() PeerTLDiagnostics

	// Auth (optional; nil when authentication is disabled — the existing
	// behavior is preserved, the message API is unauthenticated).
	AuthProvider auth.Provider
	CookieSecure bool

	// Message retention TTL. When non-zero, expired messages
	// are excluded from the public message API (GET /api/messages and GET
	// /api/messages/{id}). When zero (the default), retention is disabled and
	// the message API behaves exactly as before.
	RetentionTTL time.Duration

	// InitialSyncTracker is the opt-in initial anti-entropy synchronization
	// barrier tracker (Milestone 7A enhancement). When non-nil (the barrier
	// is enabled), the web handler's /readyz is gated on it in combination
	// with any existing lifecycle authorization. When nil (the default), the
	// existing readiness behavior is preserved exactly.
	InitialSyncTracker readiness.TrackerProvider

	// Clock (for tests; nil uses time.Now).
	Now func() time.Time
}

// DiscoverySnapshot is the web handler's view of the discovery state. It is a
// narrow struct (not the discovery.Snapshot directly) to avoid an import
// cycle and to keep the web package decoupled from the discovery package's
// internal types. The caller (main.go) bridges discovery.Snapshot into this.
type DiscoverySnapshot struct {
	Peers            []PeerInfo
	PeerCount        int
	ExpectedReplicas int
	Stale            bool
}

// PeerTLDiagnostics is the web handler's view of the peer mTLS diagnostics.
// It is a narrow struct (not the peertls.Diagnostics directly) to avoid an
// import cycle. The caller bridges peertls.Diagnostics into this.
type PeerTLDiagnostics struct {
	CertFingerprint string
	ReloadCount     int
	Enabled         bool
}

// Handler is the web http.Handler. It is safe for concurrent use.
type Handler struct {
	cfg    Config
	mux    *http.ServeMux
	authOn bool
}

// NewHandler builds the web Handler. It internally builds the public mux
// (reusing server.NewWithStoreAndCallbackAndLifecycle for the message API
// routes) and wraps it with authentication and CSRF when the AuthProvider is
// non-nil. When AuthProvider is nil, the existing behavior is preserved (no
// browser authentication; the message API is unauthenticated).
//
// The peer mTLS listener is NOT touched by this handler — the /internal/*
// routes remain on the peer listener only.
func NewHandler(cfg Config) *Handler {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	h := &Handler{cfg: cfg, mux: http.NewServeMux()}

	// Build the public mux for the message API routes. This reuses the
	// existing server.NewWithStoreAndCallbackAndLifecycle, so the message
	// validation, the lifecycle admission checks, and the readiness gating
	// are preserved unchanged.
	// Build the public mux for the message API routes. Pass the initial-sync
	// tracker and the lifecycle controller so the public mux's own /readyz
	// (reachable if the mux is used directly) agrees with this handler's
	// /readyz: both are the same logical combination of the lifecycle
	// authorization and the barrier. A nil tracker (the default) preserves
	// the existing behavior exactly.
	var publicMux http.Handler
	if cfg.Lifecycle != nil {
		publicMux = server.NewWithStoreAndCallbackAndRetentionAndInitialSync(cfg.Node, cfg.Store, cfg.OnMessage, cfg.Lifecycle, cfg.RetentionTTL, cfg.InitialSyncTracker)
	} else {
		publicMux = server.NewWithStoreAndCallbackAndRetentionAndInitialSync(cfg.Node, cfg.Store, cfg.OnMessage, nil, cfg.RetentionTTL, cfg.InitialSyncTracker)
	}

	h.authOn = cfg.AuthProvider != nil

	// Static assets (unauthenticated — they are public UI resources).
	h.mux.HandleFunc("GET /styles.css", h.staticAsset("static/styles.css", "text/css; charset=utf-8"))
	h.mux.HandleFunc("GET /app.js", h.staticAsset("static/app.js", "text/javascript; charset=utf-8"))
	h.mux.HandleFunc("GET /index.html", h.staticAsset("static/index.html", "text/html; charset=utf-8"))
	h.mux.HandleFunc("GET /login.html", h.staticAsset("static/login.html", "text/html; charset=utf-8"))

	// Health/readiness (unauthenticated — Kubernetes probes).
	h.mux.HandleFunc("GET /healthz", healthHandler)
	h.mux.HandleFunc("GET /readyz", h.readyHandler())

	// Login (GET: serve the login page; POST: authenticate).
	h.mux.HandleFunc("GET /login", h.loginPage)
	h.mux.HandleFunc("POST /login", h.login)
	h.mux.HandleFunc("POST /logout", h.logout)

	// Diagnostics (authenticated when auth is on; unauthenticated otherwise).
	if h.authOn {
		h.mux.Handle("GET /api/diagnostics", h.requireAuthCSRF(http.HandlerFunc(h.diagnostics)))
	} else {
		h.mux.HandleFunc("GET /api/diagnostics", h.diagnostics)
	}

	// The message API and the root UI page are authenticated when auth is on.
	// The root page (GET /) serves the embedded index.html; it is wrapped with
	// auth so an unauthenticated browser is redirected to /login.
	if h.authOn {
		// The public mux's message routes (POST/GET /api/messages, GET
		// /api/messages/{id}) are wrapped with auth + CSRF. The root page is
		// served by the auth-wrapped index handler (not the public mux's root
		// handler, which would bypass auth).
		h.mux.Handle("POST /api/messages", h.requireAuthCSRF(messagePostAdapter(publicMux)))
		h.mux.Handle("GET /api/messages", h.requireAuth(messageGetAdapter(publicMux)))
		h.mux.Handle("GET /api/messages/{id}", h.requireAuth(messageGetByIDAdapter(publicMux)))
		h.mux.Handle("GET /{$}", h.requireAuth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.rootPage(w, r) })))
	} else {
		// Auth disabled: use the public mux's handlers directly (existing
		// behavior preserved).
		h.mux.Handle("POST /api/messages", messagePostAdapter(publicMux))
		h.mux.Handle("GET /api/messages", messageGetAdapter(publicMux))
		h.mux.Handle("GET /api/messages/{id}", messageGetByIDAdapter(publicMux))
		h.mux.Handle("GET /{$}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.rootPage(w, r) }))
	}

	return h
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

// staticAsset serves an embedded static file with the given content type.
func (h *Handler) staticAsset(path, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		data, err := staticFS.ReadFile(path)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(data)
	}
}

// rootPage serves the embedded index.html (the main app UI). It is the
// authenticated root: an unauthenticated browser is redirected to /login by the
// requireAuth wrapper.
func (h *Handler) rootPage(w http.ResponseWriter, _ *http.Request) {
	data, err := staticFS.ReadFile("static/index.html")
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

// loginPage serves the embedded login.html when the request is unauthenticated.
// When the request is already authenticated (a valid session cookie), it
// redirects to / (the browser is already signed in).
func (h *Handler) loginPage(w http.ResponseWriter, r *http.Request) {
	if h.authOn {
		res := h.cfg.AuthProvider.Authenticate(r, h.cfg.Now())
		if res.Allowed {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
	}
	data, err := staticFS.ReadFile("static/login.html")
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

// login handles POST /login. It reads the password from the form body,
// verifies it against the credential, and on success sets the session and CSRF
// cookies and redirects to /. On failure it returns 401 (wrong password) or
// 503 (credential unavailable).
func (h *Handler) login(w http.ResponseWriter, r *http.Request) {
	if !h.authOn {
		// Auth disabled: redirect to / (no login required).
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	// Bound the form body.
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := r.ParseForm(); err != nil {
		writeLoginError(w, http.StatusBadRequest, "invalid request")
		return
	}
	password := r.FormValue("password")
	if password == "" {
		writeLoginError(w, http.StatusBadRequest, "password is required")
		return
	}

	ok, err := h.cfg.AuthProvider.VerifyPassword(password)
	if err != nil {
		if err == auth.ErrCredentialUnavailable {
			writeLoginError(w, http.StatusServiceUnavailable, "credential unavailable")
			return
		}
		writeLoginError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !ok {
		writeLoginError(w, http.StatusUnauthorized, "invalid password")
		return
	}

	// Success: issue the session + CSRF cookies and redirect to /.
	cred := h.cfg.AuthProvider.Diagnostics()
	// Re-read the snapshot for the generation (Diagnostics carries it).
	generation := cred.Generation
	cookieValue, csrfToken, err := issueSessionFor(h.cfg.AuthProvider, generation, h.cfg.Now())
	if err != nil {
		writeLoginError(w, http.StatusInternalServerError, "internal error")
		return
	}
	auth.SetSessionCookie(w, cookieValue, csrfToken, h.cfg.CookieSecure, h.cfg.Now())
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// logout handles POST /logout. It validates the session-bound CSRF token
// (state-changing), clears the session and CSRF cookies, and redirects to
// /login. The CSRF token is validated against the session's SID using the
// provider's current signing key; a token from another session or a tampered
// token is rejected before the cookies are cleared.
func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	if h.authOn {
		// CSRF check (state-changing) using the session-bound token.
		key := h.authSigningKey()
		if key == nil || !auth.CheckCSRF(r, key, h.cfg.Now()) {
			http.Error(w, "forbidden: CSRF token required", http.StatusForbidden)
			return
		}
		auth.ClearSessionCookie(w, h.cfg.CookieSecure)
	}
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// diagnostics handles GET /api/diagnostics. It assembles the Diagnostics
// payload from the in-process components and returns it as JSON. It never
// includes the credential material or the peer private key.
func (h *Handler) diagnostics(w http.ResponseWriter, _ *http.Request) {
	d := Diagnostics{
		NodeID:           h.cfg.Node.ID,
		Version:          h.cfg.Version,
		FIPSModule:       h.cfg.FIPSModule,
		FIPSEnabled:      h.cfg.FIPSEnabled,
		Peers:            []PeerInfo{},
		PeerCount:        0,
		ExpectedReplicas: 0,
	}

	// Readiness + lifecycle.
	if h.cfg.Lifecycle != nil {
		d.LifecycleEnabled = true
		snap := h.cfg.Lifecycle.Authorization()
		d.LifecycleState = snap.State.String()
		d.LifecycleReason = snap.Reason
	}
	// Initial-sync barrier (Milestone 7A enhancement).
	isEnabled := h.trackerEnabled()
	isStatus := h.trackerStatus()
	d.InitialSyncEnabled = isEnabled
	d.InitialSyncComplete = isStatus.Complete
	d.InitialSyncCompleted = isStatus.Completed
	d.InitialSyncExpected = isStatus.Expected
	if isEnabled {
		d.InitialSyncMissing = isStatus.MissingPeers
	}
	// The readiness decision is the combination: lifecycle authorization
	// (vacuously true when lifecycle is disabled) AND the barrier (vacuously
	// true when disabled).
	lifecycleAuthorized := true
	if h.cfg.Lifecycle != nil && h.cfg.Lifecycle.Admission().Enabled() {
		lifecycleAuthorized = h.cfg.Lifecycle.Authorization().Authorized()
	}
	d.Ready = lifecycleAuthorized && isStatus.Complete

	// Peers (from discovery, when configured).
	if h.cfg.DiscoverySnapshot != nil {
		ds := h.cfg.DiscoverySnapshot()
		d.Peers = ds.Peers
		d.PeerCount = ds.PeerCount
		d.ExpectedReplicas = ds.ExpectedReplicas
		d.DiscoveryStale = ds.Stale
	}

	// Peer mTLS diagnostics (when enabled).
	if h.cfg.PeerTLDiagnostics != nil {
		p := h.cfg.PeerTLDiagnostics()
		d.PeerTLSEnabled = p.Enabled
		d.PeerCertFingerprint = p.CertFingerprint
		d.PeerReloadCount = p.ReloadCount
	}

	// Browser credential (when auth is on).
	if h.authOn {
		d.AuthEnabled = true
		cd := h.cfg.AuthProvider.Diagnostics()
		d.CredentialState = string(cd.State)
		d.CredentialGeneration = cd.Generation
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(d)
}

// trackerEnabled reports whether the initial-sync barrier is active. It is
// nil-safe: a nil tracker (either a nil interface, as in a zero-valued
// web.Config, or a nil *InitialSyncTracker boxed into the interface, as
// main.go produces when the barrier is off) means the barrier is disabled.
func (h *Handler) trackerEnabled() bool {
	tr := h.cfg.InitialSyncTracker
	if tr == nil {
		return false
	}
	return tr.Enabled()
}

// trackerStatus returns a concurrency-safe, nil-safe point-in-time view of
// the initial-sync barrier. A nil tracker (nil interface or nil pointer) is
// treated as the disabled/complete view: the barrier imposes no constraint.
// No network or SQLite access is performed.
func (h *Handler) trackerStatus() readiness.Status {
	tr := h.cfg.InitialSyncTracker
	if tr == nil {
		return readiness.Status{Complete: true, State: "disabled"}
	}
	return tr.Status()
}

// readyHandler returns the readiness handler. It reports readiness as the
// logical combination of the existing lifecycle authorization (when lifecycle
// protection is enabled) and the initial-sync barrier (when enabled). This is
// a single handler, not two independent handlers that can disagree: 200 only
// when both conditions hold (or both are disabled, in which case both are
// vacuously true); 503 otherwise, with a reason naming the failing condition.
//
// The handler reads only concurrency-safe in-memory state (one tracker Status
// call, one controller Authorization snapshot). It performs no network
// requests and no SQLite inventory scans. /healthz semantics are unchanged
// (always 200, unauthenticated).
func (h *Handler) readyHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		// Initial-sync barrier status (nil tracker = disabled = complete).
		isStatus := h.trackerStatus()
		// Lifecycle authorization (nil/disabled controller = authorized).
		var lState, lReason string
		lifecycleAuthorized := true
		if h.cfg.Lifecycle != nil && h.cfg.Lifecycle.Admission().Enabled() {
			snap := h.cfg.Lifecycle.Authorization()
			lifecycleAuthorized = snap.Authorized()
			lState = snap.State.String()
			lReason = snap.Reason
		}

		ready := lifecycleAuthorized && isStatus.Complete
		status := http.StatusServiceUnavailable
		if ready {
			status = http.StatusOK
		}

		// Reason precedence: the barrier (initial_sync_pending) takes
		// precedence while pending (it is the startup gate); a lifecycle
		// failure is reported when the barrier is complete but lifecycle
		// authorization is not.
		reason := ""
		if !ready {
			if !isStatus.Complete {
				reason = "initial_sync_pending"
			} else if !lifecycleAuthorized {
				reason = lReason
			}
		}

		writeJSON(w, status, struct {
			Service              string   `json:"service"`
			Ready                bool     `json:"ready"`
			State                string   `json:"state,omitempty"`
			Reason               string   `json:"reason,omitempty"`
			InitialSyncComplete  bool     `json:"initial_sync_complete"`
			InitialSyncCompleted int      `json:"initial_sync_completed"`
			InitialSyncExpected  int      `json:"initial_sync_expected"`
			InitialSyncMissing   []string `json:"initial_sync_missing,omitempty"`
		}{
			Service:              "gossipgrid",
			Ready:                ready,
			State:                lState,
			Reason:               reason,
			InitialSyncComplete:  isStatus.Complete,
			InitialSyncCompleted: isStatus.Completed,
			InitialSyncExpected:  isStatus.Expected,
			InitialSyncMissing:   isStatus.MissingPeers,
		})
	}
}

// requireAuth wraps a handler with authentication. On an allowed result it
// calls the handler; on a non-allowed result it writes the appropriate status
// (401/503) and does NOT call the handler. For browser requests (Accept:
// text/html) a 401 is a redirect to /login; for API requests it is a JSON 401.
func (h *Handler) requireAuth(next http.Handler) http.Handler {
	return h.requireAuthCSRF(next)
}

// requireAuthCSRF wraps a handler with authentication AND session-bound CSRF
// validation (for state-changing methods). The CSRF check runs only after
// authentication succeeds (so the session is established and the CSRF cookie is
// present). The CSRF token is validated against the session's SID using the
// provider's current signing key (HMAC-SHA256(key, SID) == the token), so a
// token from another session or a tampered token is rejected.
func (h *Handler) requireAuthCSRF(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := h.cfg.AuthProvider.Authenticate(r, h.cfg.Now())
		if !res.Allowed {
			h.writeAuthError(w, r, res)
			return
		}
		// CSRF: validate the session-bound token for state-changing methods,
		// using the provider's current signing key.
		key := h.authSigningKey()
		if key == nil {
			// No signing key (should not happen when auth is on and loaded);
			// fail closed.
			h.writeCSRFError(w, r)
			return
		}
		if !auth.CheckCSRF(r, key, h.cfg.Now()) {
			h.writeCSRFError(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// authSigningKey returns the provider's current signing key (from the
// reloader's snapshot), or nil when no valid credential is loaded. It is used
// by the CSRF check to derive the expected token from the session's SID.
func (h *Handler) authSigningKey() []byte {
	if dp, ok := h.cfg.AuthProvider.(*auth.DefaultProvider); ok {
		if cf := dp.Snapshot(); cf != nil {
			return cf.SigningKeyHex()
		}
	}
	return nil
}

// writeAuthError writes the response for a non-allowed auth result. It
// distinguishes the conditions (401 unauthenticated, 503 credential
// unavailable) and chooses a redirect (browser) or a JSON error (API) based on
// the request's Accept header.
func (h *Handler) writeAuthError(w http.ResponseWriter, r *http.Request, res auth.Result) {
	// Browser navigation (Accept: text/html) → redirect to /login.
	if isBrowserRequest(r) {
		switch res.Condition {
		case auth.ConditionCredentialUnavailable:
			// A 503 is not a redirect (the credential is stuck; the user
			// should not be sent to a login form that will also 503).
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		default:
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
	}
	// API request → JSON error.
	switch res.Condition {
	case auth.ConditionCredentialUnavailable:
		writeJSONError(w, http.StatusServiceUnavailable, res.Message)
	default:
		writeJSONError(w, http.StatusUnauthorized, res.Message)
	}
}

// writeCSRFError writes the response for a CSRF failure. Browser → a redirect
// to / (the user should refresh to get a fresh token); API → JSON 403.
func (h *Handler) writeCSRFError(w http.ResponseWriter, r *http.Request) {
	if isBrowserRequest(r) {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	writeJSONError(w, http.StatusForbidden, "CSRF token required")
}

// isBrowserRequest reports whether the request is a browser navigation (as
// opposed to an API/fetch request). It checks the Accept header for
// text/html. A fetch() request with no Accept header (or Accept:
// application/json) is an API request.
func isBrowserRequest(r *http.Request) bool {
	accept := r.Header.Get("Accept")
	return accept != "" && contains(accept, "text/html")
}

// contains is a small string-contains helper (avoids importing strings for a
// single check).
func contains(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// healthHandler reports liveness (always 200, unauthenticated).
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, struct {
		Service string `json:"service"`
		Status  string `json:"status"`
	}{Service: "gossipgrid", Status: "healthy"})
}

// writeJSON writes a JSON response.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// writeJSONError writes a JSON error response.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: msg})
}

// writeLoginError writes a login error response as JSON. The login POST is
// always from the browser (the login page's form), and the form's JavaScript
// reads the status code and the JSON error body to display the message. A
// 401 means wrong password; a 503 means the credential is unavailable.
func writeLoginError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: msg})
}

// --- Adapters that bridge the public mux's handlers into the web handler ---
//
// The public mux (server.NewWithStoreAndCallbackAndLifecycle) is an
// http.Handler. To wrap individual routes with auth, we extract the handlers
// by making a synthetic request to the public mux and capturing the handler
// via a wrapper. A simpler approach: re-create the message handlers here.
// But the cleanest approach is to use the public mux as a fallback: for the
// message routes, we delegate to the public mux (which has the same handler
// logic) by calling publicMux.ServeHTTP with the original request. This
// preserves the exact handler behavior (validation, lifecycle checks, etc.)
// without duplicating it.

// messagePostAdapter delegates POST /api/messages to the public mux.
func messagePostAdapter(publicMux http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		publicMux.ServeHTTP(w, r)
	}
}

// messageGetAdapter delegates GET /api/messages to the public mux.
func messageGetAdapter(publicMux http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		publicMux.ServeHTTP(w, r)
	}
}

// messageGetByIDAdapter delegates GET /api/messages/{id} to the public mux.
func messageGetByIDAdapter(publicMux http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		publicMux.ServeHTTP(w, r)
	}
}

// issueSessionFor issues a session cookie using the provider's current
// credential. It re-reads the snapshot to get the current signing key and
// generation, then calls auth.IssueSession.
func issueSessionFor(p auth.Provider, generation int64, now time.Time) (string, string, error) {
	// The provider's Diagnostics carries the generation but not the raw key.
	// We need the raw key to sign. The DefaultProvider exposes it via the
	// reloader's snapshot; but the Provider interface does not. So we cast.
	if dp, ok := p.(*auth.DefaultProvider); ok {
		if cf := dp.Snapshot(); cf != nil {
			return auth.IssueSession(cf.SigningKeyHex(), generation, now)
		}
	}
	// Fallback: if the provider does not expose the snapshot, we cannot sign.
	// This should not happen in practice (the web handler always uses a
	// DefaultProvider).
	return "", "", auth.ErrCredentialUnavailable
}
