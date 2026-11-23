// Package peertls provides the authenticated peer-to-peer mTLS transport for
// GossipGrid nodes.
//
// Each replica has a predictable StatefulSet identity (gossipgrid-0,
// gossipgrid-1, ...). Its certificate DNS identity is
//
//	<replica>.<configured-identity-suffix>
//
// e.g. "gossipgrid-0.gossipgrid-headless.default.svc.cluster.local" where the
// configured suffix is "gossipgrid-headless.default.svc.cluster.local". The
// cluster DNS domain is NOT hard-coded; the operator configures the full
// suffix.
//
// This package:
//   - loads the certificate/key and the workload CA trust bundle;
//   - validates the local certificate's identity and usages;
//   - builds secure TLS server and client configurations (TLS 1.3 only,
//     mutual TLS, dedicated CA trust, strict identity authorization);
//   - hot-reloads credentials on file change without replacing valid material
//     on failure;
//   - performs additional peer identity authorization after the normal TLS
//     handshake (the TLS layer verifies the chain/hostname; we verify the
//     authorized GossipGrid pod identity).
//
// The network dial address and the expected authenticated peer identity are
// distinct inputs: a client may dial 127.0.0.1:8443 while verifying a
// certificate for "gossipgrid-1.gossipgrid-headless.default.svc.cluster.local".
// This is essential for local multi-process testing.
package peertls

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrPeerIdentityRejected is returned when a peer certificate passes the TLS
// chain/hostname verification but fails the additional GossipGrid identity
// authorization (wrong pod, wrong suffix, wildcard, etc.).
var ErrPeerIdentityRejected = errors.New("peer identity not authorized by GossipGrid policy")

// Config holds the file paths and policy parameters for the peer mTLS transport.
// It is independent of the environment configuration (which is parsed and
// validated separately); this struct is what the peertls package consumes.
type Config struct {
	// CertFile is the path to the mounted certificate chain (PEM).
	CertFile string
	// KeyFile is the path to the mounted private key (PEM).
	KeyFile string
	// TrustBundleFile is the path to the dedicated workload CA trust bundle
	// (PEM, may contain multiple CAs during transitions).
	TrustBundleFile string
	// IdentitySuffix is the DNS suffix that follows the pod short name in
	// the certificate SAN, e.g. "gossipgrid-headless.default.svc.cluster.local".
	IdentitySuffix string
	// StatefulSetName is the expected StatefulSet name (e.g. "gossipgrid").
	// The peer identity authorization enforces that a certificate's pod
	// identity has the exact prefix "<StatefulSetName>-". This prevents a
	// certificate from a different StatefulSet (e.g. "other-0.<suffix>") from
	// being authorized, even when signed by the same workload CA.
	StatefulSetName string
	// NodeID is the configured node identity (e.g. "gossipgrid-0"). The local
	// certificate's pod identity must match this value (or its FQDN form
	// "<NodeID>.<IdentitySuffix>") so a mismatch between the configured
	// identity and the certificate fails startup.
	NodeID string
	// ExpectedReplicas is the number of replicas in the StatefulSet. Peer
	// ordinals must be in the range [0, ExpectedReplicas-1].
	ExpectedReplicas int
	// ReloadInterval is the period for polling credential file changes.
	ReloadInterval time.Duration
}

// Validate checks the configuration for required fields and syntactic
// correctness. It does not open the files (that is the Manager's job at
// load time); it only checks that the fields are non-empty and well-formed.
func (c Config) Validate() error {
	if c.CertFile == "" {
		return errors.New("cert file path is empty")
	}
	if c.KeyFile == "" {
		return errors.New("key file path is empty")
	}
	if c.TrustBundleFile == "" {
		return errors.New("trust bundle file path is empty")
	}
	if c.IdentitySuffix == "" {
		return errors.New("identity suffix is empty")
	}
	if c.StatefulSetName == "" {
		return errors.New("stateful set name is empty")
	}
	if c.NodeID == "" {
		return errors.New("node id is empty")
	}
	if c.ExpectedReplicas < 1 {
		return fmt.Errorf("expected replicas must be at least 1 (got %d)", c.ExpectedReplicas)
	}
	if c.ReloadInterval <= 0 {
		return fmt.Errorf("reload interval must be positive (got %v)", c.ReloadInterval)
	}
	return nil
}

// credentialSnapshot is an immutable, validated set of TLS credentials. It is
// published atomically so concurrent readers always see either the old or the
// new complete set, never a partially loaded mix.
type credentialSnapshot struct {
	cert       tls.Certificate
	caPool     *x509.CertPool
	localPodID string
	loadedAt   time.Time
	certSHA256 [32]byte
	// trustSHA256 is the SHA-256 of the raw trust-bundle PEM that produced
	// caPool. It is used by tryReload to detect a genuine reload (a
	// trust-bundle rotation) even when the leaf certificate is unchanged,
	// while still treating an identical-material tick as a no-op.
	trustSHA256 [32]byte
}

// Manager loads and manages peer mTLS credentials with hot reload. It is safe
// for concurrent use: TLSConfigForServer and TLSConfigForClient may be called
// from multiple goroutines while a reload is in progress.
//
// A failed reload never replaces the current valid snapshot; the previous
// credentials continue to be used for new handshakes until the next successful
// reload or Shutdown.
//
// Lifecycle states:
//
//	created → started → stopped
//
// - created: New() returned; no goroutine running.
// - started: Start() succeeded; reload goroutine running.
// - stopped: Shutdown() completed; no goroutine running.
//
// Repeated Start from created returns an error if already started or stopped.
// Repeated or concurrent Shutdown is idempotent and never double-closes channels.
type Manager struct {
	cfg    Config
	logger *slog.Logger

	mu          sync.RWMutex
	cred        *credentialSnapshot
	reloadCount int
	state       int // 0=created, 1=started, 2=stopped
	stopCh      chan struct{}
	done        chan struct{}
}

// New creates a Manager. The first call to Start performs the initial
// credential load; if that load fails, Start returns an error and no listener
// should be started.
func New(cfg Config, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		cfg:    cfg,
		logger: logger,
		stopCh: make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// Start performs the initial credential load and starts the reload worker.
// It returns an error if the initial load fails (missing/malformed files),
// which must cause startup to fail — no silent fallback to unauthenticated
// communication.
//
// Start can only be called once, from the created state. A failed Start
// leaves the Manager in the created state (no goroutine started), so a
// subsequent Shutdown returns promptly.
func (m *Manager) Start(ctx context.Context) error {
	if err := m.cfg.Validate(); err != nil {
		return fmt.Errorf("invalid peer TLS config: %w", err)
	}

	m.mu.Lock()
	if m.state != 0 { // not in created state
		m.mu.Unlock()
		return fmt.Errorf("peer TLS manager already %s", stateName(m.state))
	}
	m.mu.Unlock()

	// Initial load (synchronous; must succeed for the listener to start).
	snap, err := m.loadCredentials()
	if err != nil {
		// Stay in created state: no goroutine was started, Shutdown will
		// return promptly.
		return fmt.Errorf("initial credential load: %w", err)
	}

	m.mu.Lock()
	if m.state != 0 {
		m.mu.Unlock()
		return fmt.Errorf("peer TLS manager already %s", stateName(m.state))
	}
	m.cred = snap
	m.state = 1 // started
	m.mu.Unlock()

	m.logger.Info("peer TLS credentials loaded",
		slog.String("cert_fingerprint", fingerprintHex(snap.certSHA256)),
		slog.String("local_pod_id", snap.localPodID),
	)

	// Start the reload worker. The derived context is cancelled by Shutdown
	// (via the stop channel) and when the caller's context is cancelled.
	reloadCtx, cancel := context.WithCancel(ctx)
	//nolint:contextcheck // reloadCtx is a derived context from the caller's ctx; the loop exits on m.stopCh (closed by Shutdown) or when reloadCtx is cancelled.
	go m.runReloadLoop(reloadCtx, cancel)
	return nil
}

// runReloadLoop polls credential files at the configured interval and atomically
// replaces the snapshot on a successful load. A failed load logs a warning and
// preserves the previous valid snapshot.
//
// After each reload attempt the loop re-checks the stop signal so a Shutdown
// that fires while a reload is in progress is not missed; the worker then
// exits and closes m.done, which Shutdown waits on.
//
//nolint:contextcheck // ctx is a derived context from Start's caller; the loop exits on m.stopCh (closed by Shutdown) or when ctx is cancelled.
func (m *Manager) runReloadLoop(ctx context.Context, cancel context.CancelFunc) {
	defer cancel()
	defer close(m.done)

	ticker := time.NewTicker(m.cfg.ReloadInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			m.logger.Debug("peer TLS reload loop stopped")
			return
		case <-ctx.Done():
			m.logger.Debug("peer TLS reload loop stopped (context cancelled)")
			return
		case <-ticker.C:
			m.tryReload()
			select {
			case <-m.stopCh:
				m.logger.Debug("peer TLS reload loop stopped")
				return
			default:
			}
		}
	}
}

// tryReload attempts to load credentials and atomically publishes the new
// snapshot. A failed load preserves the previous valid snapshot.
//
// A reload is only counted when the loaded credential material actually
// differs from the currently published snapshot — either the leaf
// certificate changed (a certificate rotation) or the CA trust bundle
// changed (a trust rotation). When the material is unchanged, the new
// snapshot is still published (so a concurrent change that lands between
// ticks is picked up on the next handshake) but the reload counter is left
// alone and no "reloaded" log line is emitted. This is the correct behavior
// for the diagnostics "reload count" metric: it should reflect how many
// times the credential material actually changed, not how many times the
// reload worker ran a periodic check.
func (m *Manager) tryReload() {
	snap, err := m.loadCredentials()
	if err != nil {
		m.logger.Warn("peer TLS credential reload failed; previous credentials retained",
			slog.Any("error", err),
		)
		return
	}
	m.mu.Lock()
	prev := m.cred
	m.cred = snap
	if prev == nil || prev.certSHA256 != snap.certSHA256 || prev.trustSHA256 != snap.trustSHA256 {
		m.reloadCount++
		m.mu.Unlock()
		m.logger.Info("peer TLS credentials reloaded",
			slog.String("cert_fingerprint", fingerprintHex(snap.certSHA256)),
			slog.String("local_pod_id", snap.localPodID),
		)
		return
	}
	m.mu.Unlock()
	// Identical credential material (same leaf and same trust bundle): publish
	// the new snapshot without incrementing the reload counter.
	m.logger.Debug("peer TLS reload tick: credential material unchanged; snapshot updated",
		slog.String("cert_fingerprint", fingerprintHex(snap.certSHA256)),
	)
}

// Shutdown stops the reload worker and waits for it to exit. It is idempotent
// and safe to call from concurrent goroutines.
//
//   - If the Manager is in the created state (Start was never called, or Start
//     failed before starting the goroutine), Shutdown transitions to stopped
//     and returns promptly (no goroutine to wait for).
//   - If the Manager is in the started state, Shutdown signals the reload
//     worker via stopCh, transitions to stopped, and then waits for the worker
//     to close done.
//   - If the Manager is already stopped, Shutdown still waits for done. If the
//     reload worker was started it must have exited (or be exiting) for done
//     to be closed, so this wait is bounded; if no worker was ever started
//     (created-state Shutdown) done is already closed and the wait returns
//     immediately.
func (m *Manager) Shutdown() {
	m.mu.Lock()
	switch m.state {
	case 0: // created (Start never succeeded, or failed before goroutine start)
		m.state = 2
		close(m.done) // no worker was started; close immediately
		m.mu.Unlock()
		return
	case 1: // started
		m.state = 2
		close(m.stopCh) // signal the reload worker to stop
		m.mu.Unlock()
		<-m.done // wait for the reload worker to exit
		return
	case 2: // already stopped (a concurrent or prior Shutdown transitioned it)
		m.mu.Unlock()
		<-m.done // still wait: if a worker was started, ensure it has exited
		return
	}
}

// stateName returns a human-readable name for the lifecycle state.
func stateName(s int) string {
	switch s {
	case 0:
		return "created"
	case 1:
		return "started"
	case 2:
		return "stopped"
	default:
		return "unknown"
	}
}

// current returns the current valid credential snapshot (nil if never loaded).
func (m *Manager) current() *credentialSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cred
}

// loadCredentials reads the certificate, key, and trust bundle from disk and
// validates the complete credential set. It returns an immutable snapshot.
// All validation is done here; a returned snapshot is guaranteed to be
// complete and valid:
//
//   - the private key matches the leaf certificate (tls.X509KeyPair);
//   - the local certificate chain verifies against the configured workload
//     CA trust bundle (x509.Certificate.Verify with the CA pool as roots);
//   - the local certificate is within its validity period;
//   - the local certificate's identity is authorized (exactly one DNS SAN,
//     correct suffix, StatefulSet name prefix, ordinal in range);
//   - the local certificate has both serverAuth and clientAuth EKUs.
func (m *Manager) loadCredentials() (*credentialSnapshot, error) {
	certPEM, err := os.ReadFile(m.cfg.CertFile)
	if err != nil {
		return nil, fmt.Errorf("reading certificate %q: %w", m.cfg.CertFile, err)
	}
	keyPEM, err := os.ReadFile(m.cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("reading key %q: %w", m.cfg.KeyFile, err)
	}
	trustPEM, err := os.ReadFile(m.cfg.TrustBundleFile)
	if err != nil {
		return nil, fmt.Errorf("reading trust bundle %q: %w", m.cfg.TrustBundleFile, err)
	}

	// Parse the certificate chain.
	certs, err := parseCertChain(certPEM)
	if err != nil {
		return nil, fmt.Errorf("parsing certificate chain: %w", err)
	}
	leaf := certs[0]

	// Parse the private key.
	privKey, err := parsePrivateKey(keyPEM)
	if err != nil {
		return nil, fmt.Errorf("parsing private key: %w", err)
	}

	// Verify that the private key matches the leaf certificate. This catches
	// a mismatched cert/key pair (e.g. the key was rotated but the cert was
	// not, or vice versa) before it is published.
	if err := verifyKeyMatchesCert(leaf, privKey); err != nil {
		return nil, fmt.Errorf("private key does not match certificate: %w", err)
	}

	// Build the CA pool from the trust bundle.
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(trustPEM) {
		return nil, errors.New("trust bundle contains no valid CA certificates")
	}

	// Verify the local certificate chain against the configured workload CA
	// trust bundle. This catches a local cert signed by an unknown or
	// untrusted CA before it is published.
	intermediates := x509.NewCertPool()
	for _, c := range certs[1:] {
		intermediates.AddCert(c)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:         caPool,
		Intermediates: intermediates,
	}); err != nil {
		return nil, fmt.Errorf("local certificate does not verify against workload CA trust bundle: %w", err)
	}

	// Validate the local certificate's identity and usages.
	if err := m.validateLocalCertificate(leaf); err != nil {
		return nil, fmt.Errorf("local certificate validation: %w", err)
	}

	// Compute the SHA-256 fingerprint of the leaf for diagnostics.
	certSHA256 := sha256.Sum256(leaf.Raw)
	// Hash the raw trust-bundle PEM so tryReload can detect a trust rotation
	// (a genuine reload) without parsing the pool into a comparable form.
	trustSHA256 := sha256.Sum256(trustPEM)

	podID, err := m.authorizePeerIdentity(leaf)
	if err != nil {
		return nil, fmt.Errorf("extracting pod identity: %w", err)
	}

	// tls.Certificate.Certificate is [][]byte (the DER bytes of the chain),
	// not []*x509.Certificate.
	derCerts := make([][]byte, len(certs))
	for i, c := range certs {
		derCerts[i] = c.Raw
	}

	return &credentialSnapshot{
		cert:        tls.Certificate{Certificate: derCerts, PrivateKey: privKey},
		caPool:      caPool,
		localPodID:  podID,
		loadedAt:    time.Now(),
		certSHA256:  certSHA256,
		trustSHA256: trustSHA256,
	}, nil
}

// Diagnostics is a non-sensitive summary of the current credentials for
// logging and the /internal/status endpoint. It never includes the private key
// or the certificate PEM content.
type Diagnostics struct {
	CertFingerprint string `json:"cert_fingerprint"`
	LocalPodID      string `json:"local_pod_id"`
	LoadedAt        string `json:"loaded_at"`
	// ReloadCount is the number of times the local credential material
	// actually changed since Start (0 after the initial load). A "genuine"
	// reload — a leaf certificate rotation OR a CA trust-bundle rotation —
	// increments this counter. A reload worker tick that reads back identical
	// credential material (same leaf, same trust bundle) does NOT increment
	// it: the new snapshot is still published (so a concurrent change is
	// picked up on the next handshake) but the counter only reflects actual
	// material changes, not periodic check ticks. Exposed for tests to detect
	// that a genuine reload occurred and for diagnostics to report material
	// changes without conflating them with periodic check ticks.
	ReloadCount int `json:"reload_count"`
}

// Diagnostics returns a safe summary of the current credentials.
func (m *Manager) Diagnostics() Diagnostics {
	snap := m.current()
	if snap == nil {
		return Diagnostics{}
	}
	m.mu.RLock()
	rc := m.reloadCount
	m.mu.RUnlock()
	return Diagnostics{
		CertFingerprint: fingerprintHex(snap.certSHA256),
		LocalPodID:      snap.localPodID,
		LoadedAt:        snap.loadedAt.Format(time.RFC3339),
		ReloadCount:     rc,
	}
}

// TLSConfigForServer returns the base *tls.Config for the peer HTTPS server.
// It uses GetConfigForClient to return a fresh per-connection config from the
// current immutable credential snapshot on each new client handshake. This
// avoids any in-place mutation of a captured config (which would race with
// the TLS handshake goroutine) and ensures that reloaded CA bundles and leaf
// certificates apply to the next new handshake without a process restart.
//
// The base config sets the TLS version bounds and session ticket policy.
// The per-connection config (returned by getConfigForClient) adds the
// concrete ClientCAs, certificate, and identity authorization.
func (m *Manager) TLSConfigForServer() *tls.Config {
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
		// GetConfigForClient is called after the ClientHello is received and
		// returns a per-connection *tls.Config built from the latest
		// immutable credential snapshot. This is the standard Go pattern for
		// hot-reloaded credentials: the base config is never mutated, and
		// each handshake sees the current (possibly reloaded) credentials.
		GetConfigForClient: m.getConfigForClient,
		// Disable TLS session resumption: resumed sessions can bypass the
		// per-connection identity authorization check (gosec G123).
		SessionTicketsDisabled: true,
	}
}

// getConfigForClient returns a per-connection *tls.Config for an incoming
// client TLS handshake. It reads the current immutable credential snapshot
// (which is never mutated after publication) and builds a fresh config with
// the concrete ClientCAs, certificate, and identity authorization.
//
// This is called by the TLS library on every new client handshake, so a
// reload is picked up on the next new connection without a process restart.
func (m *Manager) getConfigForClient(_ *tls.ClientHelloInfo) (*tls.Config, error) {
	snap := m.current()
	if snap == nil {
		return nil, errors.New("peer TLS credentials not yet loaded")
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		MaxVersion: tls.VersionTLS13,
		// ClientCAs is the dedicated workload CA trust bundle. Without it,
		// the server falls back to the system root store, which does not
		// trust the workload CA. Using ClientCAs + RequireAndVerifyClientCert
		// is the standard Go approach for custom CA verification — no
		// InsecureSkipVerify, no custom chain verification framework.
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  snap.caPool,
		// No NextProtos (ALPN): all GossipGrid peers share a single trusted
		// workload identity and a single protocol. Multiplexing multiple
		// ALPN protocols is not needed and, in this Go version, an ALPN
		// negotiation failure on one side can drop the whole handshake with
		// a bare "EOF". Omitting ALPN keeps the mTLS handshake robust.
		Certificates:          []tls.Certificate{snap.cert},
		VerifyPeerCertificate: m.verifyClientPeerCertificate,
		// Disable TLS session resumption: resumed sessions can bypass the
		// per-connection identity authorization check (gosec G123).
		SessionTicketsDisabled: true,
	}, nil
}

//   - The leaf must have exactly one DNS SAN.
//   - The DNS SAN must be of the form "<statefulset-name>-<ordinal>.<suffix>"
//     where <suffix> matches the configured IdentitySuffix exactly.
//   - <ordinal> must be a non-negative integer.
//   - Wildcard SANs, IP SANs, URI SANs, and multiple SANs are rejected.
//   - The Common Name is NOT used as a fallback (SAN-only validation).
//   - The client certificate must include the clientAuth EKU.
//
// verifyClientPeerCertificate performs the additional GossipGrid identity
// authorization after the standard TLS chain verification (which is handled
// by the TLS library via ClientCAs + RequireAndVerifyClientCert).
//
// Authorization rules:
//   - The leaf must have exactly one DNS SAN with no other SAN types
//     (no IP, URI, or email SANs).
//   - The DNS SAN must be of the form "<statefulset-name>-<ordinal>.<suffix>"
//     where <suffix> matches the configured IdentitySuffix exactly.
//   - <ordinal> must be a non-negative integer within the expected replica
//     range (0 to ExpectedReplicas-1).
//   - Wildcard SANs are rejected.
//   - The Common Name is NOT used as a fallback (SAN-only validation).
//   - The client certificate must include the clientAuth EKU.
func (m *Manager) verifyClientPeerCertificate(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if len(rawCerts) == 0 {
		return errors.New("no client certificate presented")
	}
	leaf, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return fmt.Errorf("parsing client certificate: %w", err)
	}
	if _, err := m.authorizePeerIdentity(leaf); err != nil {
		return fmt.Errorf("%w: %v", ErrPeerIdentityRejected, err)
	}
	if !containsEKU(leaf, x509.ExtKeyUsageClientAuth) {
		return fmt.Errorf("client certificate missing clientAuth Extended Key Usage")
	}
	m.logger.Debug("client peer authorized", slog.String("pod_id", podFromSAN(leaf.DNSNames[0], m.cfg.IdentitySuffix)))
	return nil
}

// TLSConfigForClient returns a *tls.Config for a peer HTTPS client that
// connects to a specific expected peer identity.
//
// dialAddr is the network address to connect to (e.g. "127.0.0.1:8443" for
// local testing, or "gossipgrid-1.gossipgrid-headless.default.svc:8443" for
// in-cluster). expectedIdentity is the authorized GossipGrid DNS identity the
// server certificate must present (e.g.
// "gossipgrid-1.gossipgrid-headless.default.svc.cluster.local"). These are
// distinct: the client may dial a loopback address while verifying a
// certificate for an in-cluster FQDN.
func (m *Manager) TLSConfigForClient(dialAddr, expectedIdentity string) (*tls.Config, error) {
	if expectedIdentity == "" {
		return nil, errors.New("expected peer identity is empty")
	}
	if _, err := m.validateExpectedIdentity(expectedIdentity); err != nil {
		return nil, fmt.Errorf("invalid expected peer identity %q: %w", expectedIdentity, err)
	}
	caPool := m.current()
	if caPool == nil {
		return nil, errors.New("peer TLS credentials not yet loaded")
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		RootCAs:      caPool.caPool,
		Certificates: []tls.Certificate{caPool.cert},
		// No NextProtos (ALPN): see TLSConfigForServer for the rationale.
		ServerName:            expectedIdentity,
		VerifyPeerCertificate: m.verifyServerPeerCertificate(expectedIdentity),
		// Disable TLS session resumption: resumed sessions can bypass the
		// custom VerifyPeerCertificate identity authorization check (gosec G123).
		SessionTicketsDisabled: true,
	}, nil
}

// verifyServerPeerCertificate returns a VerifyPeerCertificate function that
// enforces the additional GossipGrid identity authorization for a specific
// expected server identity. The standard TLS layer already verified the chain
// and that the hostname matches ServerName; this check enforces the GossipGrid
// policy: no non-DNS SAN types, exactly one DNS SAN matching expectedIdentity,
// and serverAuth EKU.
func (m *Manager) verifyServerPeerCertificate(expectedIdentity string) func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(rawCerts) == 0 {
			return errors.New("no server certificate presented")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return fmt.Errorf("parsing server certificate: %w", err)
		}
		// Reject non-DNS SAN types (IP, URI, email).
		if err := validateSANs(leaf); err != nil {
			return err
		}
		if len(leaf.DNSNames) != 1 {
			return fmt.Errorf("%w: expected exactly 1 DNS SAN, got %d", ErrPeerIdentityRejected, len(leaf.DNSNames))
		}
		if leaf.DNSNames[0] != expectedIdentity {
			return fmt.Errorf("%w: SAN %q does not match expected identity %q", ErrPeerIdentityRejected, leaf.DNSNames[0], expectedIdentity)
		}
		if !containsEKU(leaf, x509.ExtKeyUsageServerAuth) {
			return fmt.Errorf("server certificate missing serverAuth Extended Key Usage")
		}
		m.logger.Debug("server peer authorized", slog.String("identity", expectedIdentity))
		return nil
	}
}

// --- Certificate parsing and validation helpers ---

// parseCertChain parses a PEM certificate chain and returns the parsed
// certificates. It requires at least one certificate.
func parseCertChain(data []byte) ([]*x509.Certificate, error) {
	var certs []*x509.Certificate
	rest := data
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing certificate in chain: %w", err)
		}
		certs = append(certs, cert)
	}
	if len(certs) == 0 {
		return nil, errors.New("certificate file contains no certificates")
	}
	return certs, nil
}

// parsePrivateKey parses a PEM private key, supporting PKCS8 (the cert-manager
// default) with ECDSA/RSA fallbacks. The returned key satisfies
// crypto.Signer as required by tls.Certificate.
//
// The PEM block is decoded to DER first, then the appropriate DER parser is
// used. This is required because in this Go version the x509 PEM-accepting
// helpers (e.g. x509.ParsePKCS8PrivateKey) do not reliably base64-decode a
// "PRIVATE KEY" PEM block — they expect DER. Decoding explicitly avoids that
// quirk and works for all three key types.
func parsePrivateKey(pemBytes []byte) (crypto.Signer, error) {
	block, rest := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("no PEM block found in private key file")
	}
	switch block.Type {
	case "PRIVATE KEY":
		// PKCS8 (the common cert-manager output).
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing PKCS8 private key: %w", err)
		}
		signer, ok := key.(crypto.Signer)
		if !ok {
			return nil, errors.New("PKCS8 private key is not a usable crypto.Signer")
		}
		return signer, nil
	case "EC PRIVATE KEY":
		key, err := x509.ParseECPrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing EC private key: %w", err)
		}
		return key, nil
	case "RSA PRIVATE KEY":
		key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing RSA private key: %w", err)
		}
		return key, nil
	default:
		return nil, fmt.Errorf("unrecognized private key PEM type %q (want PRIVATE KEY, EC PRIVATE KEY, or RSA PRIVATE KEY); remaining %d bytes", block.Type, len(rest))
	}
}

// authorizePeerIdentity performs the complete GossipGrid peer identity
// authorization for a certificate. It is used for both the local certificate
// (at load time) and remote peer certificates (at handshake time).
//
// Rules enforced:
//   - Exactly one DNS SAN (no other SAN types: no IP, URI, or email SANs).
//   - No wildcard identities.
//   - The SAN must end with "." + IdentitySuffix (exact match).
//   - The pod part must be "<StatefulSetName>-<ordinal>" where the name
//     matches the configured StatefulSet name EXACTLY.
//   - <ordinal> must be a non-negative integer in [0, ExpectedReplicas-1].
//   - The Common Name is never used as a fallback.
//
// Returns the pod short name (e.g. "gossipgrid-0") on success.
func (m *Manager) authorizePeerIdentity(leaf *x509.Certificate) (string, error) {
	// Reject any non-DNS SAN types (IP, URI, email).
	if err := validateSANs(leaf); err != nil {
		return "", err
	}
	// Must have exactly one DNS SAN.
	if len(leaf.DNSNames) != 1 {
		return "", fmt.Errorf("%w: expected exactly 1 DNS SAN, got %d", ErrPeerIdentityRejected, len(leaf.DNSNames))
	}
	san := normalizeDNSName(leaf.DNSNames[0])
	suffix := normalizeDNSName(m.cfg.IdentitySuffix)
	statefulsetName := normalizeDNSName(m.cfg.StatefulSetName)

	if strings.Contains(san, "*") {
		return "", fmt.Errorf("%w: wildcard DNS SAN %q is not authorized", ErrPeerIdentityRejected, san)
	}
	if !strings.HasSuffix(san, "."+suffix) {
		return "", fmt.Errorf("%w: DNS SAN %q does not match identity suffix %q", ErrPeerIdentityRejected, san, suffix)
	}
	podPart := san[:len(san)-len(suffix)-1]
	expectedPrefix := statefulsetName + "-"
	if !strings.HasPrefix(podPart, expectedPrefix) {
		return "", fmt.Errorf("%w: pod identity %q does not start with expected StatefulSet name %q", ErrPeerIdentityRejected, podPart, statefulsetName)
	}
	ordinal := podPart[len(expectedPrefix):]
	n, err := strconv.Atoi(ordinal)
	if err != nil || n < 0 {
		return "", fmt.Errorf("%w: ordinal %q is not a valid non-negative integer", ErrPeerIdentityRejected, ordinal)
	}
	if n >= m.cfg.ExpectedReplicas {
		return "", fmt.Errorf("%w: ordinal %d is out of range (want 0-%d for %d replicas)", ErrPeerIdentityRejected, n, m.cfg.ExpectedReplicas-1, m.cfg.ExpectedReplicas)
	}
	return podPart, nil
}

// validateSANs rejects any certificate that has non-DNS SAN types (IP, URI,
// or email addresses). A GossipGrid peer certificate must have only DNS SANs.
func validateSANs(leaf *x509.Certificate) error {
	if len(leaf.IPAddresses) > 0 {
		return fmt.Errorf("%w: certificate contains %d IP SAN(s); only DNS SANs are authorized", ErrPeerIdentityRejected, len(leaf.IPAddresses))
	}
	if len(leaf.URIs) > 0 {
		return fmt.Errorf("%w: certificate contains %d URI SAN(s); only DNS SANs are authorized", ErrPeerIdentityRejected, len(leaf.URIs))
	}
	if len(leaf.EmailAddresses) > 0 {
		return fmt.Errorf("%w: certificate contains %d email SAN(s); only DNS SANs are authorized", ErrPeerIdentityRejected, len(leaf.EmailAddresses))
	}
	return nil
}

// validateLocalCertificate validates the local certificate's identity, EKUs,
// validity period, and binding to the configured NodeID.
func (m *Manager) validateLocalCertificate(leaf *x509.Certificate) error {
	// Validity period.
	now := time.Now()
	if now.Before(leaf.NotBefore) {
		return fmt.Errorf("certificate not yet valid (NotBefore=%s)", leaf.NotBefore.Format(time.RFC3339))
	}
	if now.After(leaf.NotAfter) {
		return fmt.Errorf("certificate expired (NotAfter=%s)", leaf.NotAfter.Format(time.RFC3339))
	}
	// Identity authorization (covers SAN types, wildcard, suffix, prefix, ordinal range).
	podID, err := m.authorizePeerIdentity(leaf)
	if err != nil {
		return err
	}
	// EKUs: the local cert is used for both server and client roles.
	for _, eku := range []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth} {
		if !containsEKU(leaf, eku) {
			return fmt.Errorf("certificate missing required Extended Key Usage %v", eku)
		}
	}
	// Bind the local certificate's pod identity to the configured NodeID.
	// The NodeID may be the short pod name (e.g. "gossipgrid-0") or the full
	// FQDN (e.g. "gossipgrid-0.gossipgrid-headless.default.svc"). Compare
	// against both forms.
	nodeID := normalizeDNSName(m.cfg.NodeID)
	if nodeID != podID {
		// Try the FQDN form: "<NodeID>.<suffix>".
		fqdn := podID + "." + normalizeDNSName(m.cfg.IdentitySuffix)
		if nodeID != fqdn {
			return fmt.Errorf("local certificate identity %q does not match configured node id %q", podID, m.cfg.NodeID)
		}
	}
	return nil
}

// containsEKU reports whether the certificate has the given EKU.
func containsEKU(leaf *x509.Certificate, eku x509.ExtKeyUsage) bool {
	for _, e := range leaf.ExtKeyUsage {
		if e == eku {
			return true
		}
	}
	return false
}

// verifyKeyMatchesCert verifies that the private key corresponds to the
// public key embedded in the certificate. It uses the standard Go approach
// of comparing the public key from the certificate with the public key
// derived from the private key.
func verifyKeyMatchesCert(leaf *x509.Certificate, privKey crypto.Signer) error {
	pubKey := privKey.Public()
	// Compare the public key bytes. For ECDSA and RSA, the PublicKey
	// interfaces have consistent string representations for equal keys.
	if !publicKeysEqual(leaf.PublicKey, pubKey) {
		return errors.New("private key does not match the certificate's public key")
	}
	return nil
}

// publicKeysEqual reports whether two crypto.PublicKey values represent the
// same key. It handles ECDSA and RSA (the two key types GossipGrid supports).
func publicKeysEqual(a, b crypto.PublicKey) bool {
	switch ak := a.(type) {
	case *ecdsa.PublicKey:
		bk, ok := b.(*ecdsa.PublicKey)
		return ok && ak.Equal(bk)
	case *rsa.PublicKey:
		bk, ok := b.(*rsa.PublicKey)
		return ok && ak.Equal(bk)
	default:
		// Fallback: compare string representations.
		return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
	}
}

// podFromSAN returns the pod identity (the part before the suffix) from a SAN
// for logging only. Returns the raw SAN on failure.
func podFromSAN(san, suffix string) string {
	san = normalizeDNSName(san)
	suffix = normalizeDNSName(suffix)
	if strings.HasSuffix(san, "."+suffix) {
		return san[:len(san)-len(suffix)-1]
	}
	return san
}

// validateExpectedIdentity validates that expectedIdentity is a well-formed
// GossipGrid DNS identity for the configured suffix, StatefulSet name, and
// replica range. It enforces the same rules as authorizePeerIdentity.
func (m *Manager) validateExpectedIdentity(identity string) (string, error) {
	identity = normalizeDNSName(identity)
	suffix := normalizeDNSName(m.cfg.IdentitySuffix)
	statefulsetName := normalizeDNSName(m.cfg.StatefulSetName)
	if identity == "" {
		return "", fmt.Errorf("%w: identity is empty", ErrPeerIdentityRejected)
	}
	if strings.Contains(identity, "*") {
		return "", fmt.Errorf("%w: wildcard identity %q is not authorized", ErrPeerIdentityRejected, identity)
	}
	if !strings.HasSuffix(identity, "."+suffix) {
		return "", fmt.Errorf("%w: identity %q does not match identity suffix %q", ErrPeerIdentityRejected, identity, suffix)
	}
	podPart := identity[:len(identity)-len(suffix)-1]
	expectedPrefix := statefulsetName + "-"
	if !strings.HasPrefix(podPart, expectedPrefix) {
		return "", fmt.Errorf("%w: identity %q does not start with expected StatefulSet name %q", ErrPeerIdentityRejected, podPart, statefulsetName)
	}
	ordinal := podPart[len(expectedPrefix):]
	n, err := strconv.Atoi(ordinal)
	if err != nil || n < 0 {
		return "", fmt.Errorf("%w: ordinal %q is not a valid non-negative integer", ErrPeerIdentityRejected, ordinal)
	}
	if n >= m.cfg.ExpectedReplicas {
		return "", fmt.Errorf("%w: ordinal %d is out of range (want 0-%d)", ErrPeerIdentityRejected, n, m.cfg.ExpectedReplicas-1)
	}
	return podPart, nil
}

// normalizeDNSName lowercases and strips a trailing dot from a DNS name.
func normalizeDNSName(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimSuffix(s, ".")
	return s
}

// fingerprintHex formats a SHA-256 digest as a hex string for logging.
func fingerprintHex(h [32]byte) string {
	return fmt.Sprintf("%x", h)
}

// ExpectedPeerDNSIdentity returns the full DNS identity for a given pod short
// name (e.g. "gossipgrid-1") and the configured identity suffix. It is the
// function callers use to compute the expected identity to pass to
// TLSConfigForClient / the peer client.
func ExpectedPeerDNSIdentity(podID, suffix string) string {
	return podID + "." + normalizeDNSName(suffix)
}
