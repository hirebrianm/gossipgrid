// Package discovery provides peer discovery and membership tracking for
// GossipGrid nodes.
//
// It supports three modes:
//
//   - off: no discovery (single-instance development)
//   - static: peers from a configured host:port list (local multi-process testing)
//   - dns: peers from Kubernetes headless Service DNS SRV records
//
// A Manager performs initial discovery, periodic DNS refresh, and maintains a
// concurrency-safe membership snapshot. DNS is not proof that a peer is
// healthy, reachable, authenticated, synchronized, or ready to supply data;
// those properties are established by later mTLS and lifecycle implementations.
package discovery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Mode selects the peer discovery mechanism.
type Mode string

// Discovery modes.
const (
	ModeOff    Mode = "off"
	ModeStatic Mode = "static"
	ModeDNS    Mode = "dns"
)

// Source identifies where a peer's address was learned.
type Source string

// Peer discovery sources.
const (
	SourceStatic Source = "static"
	SourceDNS    Source = "dns"
	SourceLocal  Source = "local"
)

// Peer is a discovered peer with its identity, address, and discovery metadata.
type Peer struct {
	// Hostname is the normalized DNS hostname. For Kubernetes SRV responses
	// this is the full FQDN (e.g. "gossipgrid-0.gossipgrid-headless.default.svc.cluster.local");
	// for static peers it is the configured host (e.g. "gossipgrid-1").
	// Normalized: lowercase, trailing dot stripped.
	Hostname string
	// PodIdentity is the canonical StatefulSet pod identity (e.g. "gossipgrid-0")
	// derived from the hostname and the configured headless Service DNS suffix.
	// Empty when the hostname does not match the expected suffix (unrelated host).
	PodIdentity string
	// Address is the network address to connect to (e.g. "gossipgrid-0.gossipgrid-headless.default.svc:8443").
	Address string
	// Source is how the peer was discovered.
	Source Source
	// LastSeen is the time this peer was last observed in a successful
	// discovery pass.
	LastSeen time.Time
}

// LookupFunc is the signature of a DNS SRV lookup. It mirrors
// net.Resolver.LookupSRV: the three arguments are the service name
// (e.g. "peer"), the protocol (e.g. "tcp"), and the DNS query name
// (e.g. "gossipgrid-headless.default.svc"). The return values are the
// FQDN targets and ports from the SRV records, in order.
type LookupFunc func(ctx context.Context, service, proto, name string) (host []string, port []int, err error)

// LookupSRV delegates to Go's standard net.Resolver and normalizes the
// *net.SRV results into plain FQDN/port slices.
func LookupSRV(ctx context.Context, service, proto, name string) (host []string, port []int, err error) {
	_, srvs, err := net.DefaultResolver.LookupSRV(ctx, service, proto, name)
	if err != nil {
		return nil, nil, err
	}
	host = make([]string, 0, len(srvs))
	port = make([]int, 0, len(srvs))
	for _, s := range srvs {
		host = append(host, s.Target)
		port = append(port, int(s.Port))
	}
	return host, port, nil
}

// Config holds the validated discovery parameters for a single node.
type Config struct {
	// Mode selects the discovery mechanism.
	Mode Mode
	// StaticPeers is the list of host:port addresses for static mode.
	StaticPeers []string
	// DNSName is the headless Service DNS name (e.g. "gossipgrid-headless.default.svc").
	DNSName string
	// SRVService is the named port to query in SRV records (default "peer").
	SRVService string
	// Interval is the DNS refresh period.
	Interval time.Duration
	// Timeout bounds each individual DNS lookup.
	Timeout time.Duration
	// ExpectedReplicas is the configured cluster size (StatefulSet replicas).
	ExpectedReplicas int
	// StatefulSetName is the StatefulSet name used to derive expected identities.
	StatefulSetName string
	// NodeID is the local node identity (e.g. "gossipgrid-0" or a FQDN).
	NodeID string
}

// ExpectedIdentity is a single expected replica identity (e.g. "gossipgrid-2").
type ExpectedIdentity struct {
	Hostname string
}

// Snapshot is an immutable point-in-time view of the membership state.
type Snapshot struct {
	// Peers are the deduplicated, self-excluded discovered peers.
	Peers []Peer
	// LocalNode is the canonical local pod identity when it could be
	// established from the configured NodeID. Nil when the local node's
	// identity cannot be determined (e.g. an arbitrary hostname that does not
	// match the StatefulSet prefix). Reconcile uses this field to report the
	// local replica as MemberLocal rather than MemberMissing.
	LocalNode *Peer
	// ExpectedIdentities are the StatefulSet ordinal identities for the
	// configured replica count.
	ExpectedIdentities []ExpectedIdentity
	// Stale is true when the most recent refresh failed and the data reflects
	// the last successful discovery pass.
	Stale bool
	// LastUpdated is the time of the most recent successful discovery pass.
	// Zero if no pass has ever succeeded.
	LastUpdated time.Time
	// LastError is the error from the most recent failed refresh (nil if the
	// last pass succeeded).
	LastError error
	// Mode is the discovery mode in effect.
	Mode Mode
}

// MemberState describes the relationship between an expected identity and
// discovered membership.
type MemberState string

// Member states.
const (
	MemberFound      MemberState = "found"
	MemberMissing    MemberState = "missing"
	MemberLocal      MemberState = "local"
	MemberUnexpected MemberState = "unexpected"
)

// member is an internal membership record used by the Manager.
type member struct {
	peer     Peer
	discover time.Time
}

// errAlreadyStarted is returned by Start when the manager is already running.
var errAlreadyStarted = errors.New("discovery manager already started")

// Manager maintains peer membership through discovery and periodic refresh.
//
// A Manager is safe for concurrent use: Snapshot() and Refresh() may be called
// from multiple goroutines. Shutdown() stops the refresh loop and cancels any
// in-flight DNS lookup via a derived context, then waits for the goroutine to
// exit. Start/Shutdown lifecycle is guarded by a mutex and a done channel:
// repeated Start returns an error, repeated or concurrent Shutdown is safe and
// non-blocking.
type Manager struct {
	cfg      Config
	logger   *slog.Logger
	lookupFn LookupFunc

	mu         sync.RWMutex
	members    map[string]*member
	stale      bool
	lastErr    error
	lastUpdate time.Time

	// Lifecycle state.
	startMu   sync.Mutex
	started   bool
	initial   bool
	stopCh    chan struct{}
	done      chan struct{}
	initDone  chan struct{}
	mgrCtx    context.Context
	mgrCancel context.CancelFunc
}

// NewManager creates a Manager with the given configuration, logger, and
// lookup function. The lookup function defaults to LookupSRV when nil.
func NewManager(cfg Config, logger *slog.Logger, lookupFn LookupFunc) *Manager {
	if lookupFn == nil {
		lookupFn = LookupSRV
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		cfg:      cfg,
		logger:   logger,
		lookupFn: lookupFn,
		members:  make(map[string]*member),
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
		initDone: make(chan struct{}),
	}
}

// Validate checks the configuration for the active mode. It returns an error
// describing the first problem found, or nil if the configuration is valid.
// This must be called before Start to prevent invalid values from reaching
// time.NewTicker or other runtime components.
func (c Config) Validate() error {
	switch c.Mode {
	case ModeOff:
		// No mode-specific requirements.
	case ModeStatic:
		if len(c.StaticPeers) == 0 {
			return errors.New("static discovery mode requires at least one peer address")
		}
		for _, addr := range c.StaticPeers {
			if err := validatePeerAddr(addr); err != nil {
				return err
			}
		}
	case ModeDNS:
		if c.DNSName == "" {
			return errors.New("dns discovery mode requires a non-empty DNS service name")
		}
		if c.SRVService == "" {
			return errors.New("dns discovery mode requires a non-empty SRV service name")
		}
		if c.Interval <= 0 {
			return errors.New("dns discovery interval must be positive (got " + strconv.FormatInt(c.Interval.Nanoseconds(), 10) + "ns)")
		}
		if c.Timeout <= 0 {
			return errors.New("dns discovery timeout must be positive (got " + strconv.FormatInt(c.Timeout.Nanoseconds(), 10) + "ns)")
		}
	default:
		return fmt.Errorf("unknown discovery mode %q (want off, static, or dns)", c.Mode)
	}
	if c.ExpectedReplicas < 1 {
		return errors.New("expected replicas must be at least 1")
	}
	if c.ExpectedReplicas > 1 && c.StatefulSetName == "" {
		return errors.New("stateful set name is required when expected replicas exceeds 1")
	}
	return nil
}

// validatePeerAddr checks host:port syntax and the numeric port range (1-65535)
// without DNS resolution.
func validatePeerAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("static peer %q: %w", addr, err)
	}
	if host == "" {
		return fmt.Errorf("static peer %q: missing host", addr)
	}
	if port == "" {
		return fmt.Errorf("static peer %q: missing port", addr)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("static peer %q: port %q is not a valid number: %w", addr, port, err)
	}
	if n < 1 || n > 65535 {
		return fmt.Errorf("static peer %q: port %d is out of range (want 1-65535)", addr, n)
	}
	return nil
}

// Start performs an initial discovery pass and, for DNS mode, starts the
// periodic refresh loop.
//
// For DNS mode the initial discovery pass and the periodic refresh loop run
// in a single goroutine. This serializes discovery passes in execution order:
// a slow initial lookup cannot finish after a newer periodic lookup and
// overwrite newer membership with older data.
//
// For static mode the initial discovery pass is completed synchronously
// before Start returns. Static discovery involves no network I/O, so this
// does not block on external dependencies. The snapshot is immediately
// populated when Start returns.
//
// Repeated Start calls return errAlreadyStarted.
func (m *Manager) Start(ctx context.Context) error {
	// Validate the configuration before any runtime work. This prevents
	// malformed values from reaching time.NewTicker (which panics on
	// non-positive durations).
	if err := m.cfg.Validate(); err != nil {
		return fmt.Errorf("invalid discovery config: %w", err)
	}

	m.startMu.Lock()
	defer m.startMu.Unlock()

	if m.started {
		return errAlreadyStarted
	}

	if m.cfg.Mode == ModeOff {
		m.logger.Debug("discovery disabled", "mode", "off")
		m.started = true
		// No discovery goroutine; close done so Shutdown() returns promptly.
		// No derived context needed (no lookups to cancel).
		close(m.done)
		close(m.initDone)
		return nil
	}

	// Create a derived context that Shutdown() can cancel to abort any
	// in-flight DNS lookup. The derived context is also cancelled when the
	// parent context is cancelled, so both paths are covered.
	m.mgrCtx, m.mgrCancel = context.WithCancel(ctx)

	if m.cfg.Mode == ModeStatic {
		// Static discovery is deterministic and involves no network I/O.
		// Perform it synchronously so the snapshot is immediately populated
		// when Start returns. No background goroutine is needed.
		m.started = true
		m.initial = true
		//nolint:contextcheck // m.mgrCtx is a derived context created above from ctx via context.WithCancel; it inherits cancellation from ctx and is additionally cancellable via Shutdown().
		err := m.refresh(m.mgrCtx)
		close(m.initDone)
		if err != nil {
			m.logger.Warn("initial static discovery failed",
				slog.Any("error", err),
			)
		}
		// No background goroutine for static mode; close done and cancel the
		// derived context so Shutdown returns promptly.
		m.mgrCancel()
		close(m.done)
		return nil
	}

	// DNS mode: a single goroutine performs the initial discovery pass and
	// then enters the periodic refresh loop. This serializes all discovery
	// passes: the periodic loop only begins after the initial pass completes,
	// so a slow initial lookup cannot finish after a newer periodic lookup.
	m.started = true
	m.initial = true
	//nolint:contextcheck // m.mgrCtx is a derived context created above from ctx via context.WithCancel; it inherits cancellation from ctx and is additionally cancellable via Shutdown().
	go m.runDNSTask(m.mgrCtx)

	return nil
}

// runDNSTask performs the initial discovery pass and then enters the periodic
// refresh loop. It runs in a single goroutine so that discovery passes are
// serialized in execution order: the periodic loop only begins after the
// initial pass completes, preventing a slow initial lookup from overwriting
// newer membership data from a periodic pass.
//
// The initDone channel is closed after the initial pass completes (success or
// failure), allowing Shutdown to wait for the initial work to finish. The done
// channel is closed when the goroutine exits entirely.
//
//nolint:contextcheck // ctx is m.mgrCtx, a derived context created in Start from the caller's context via context.WithCancel; it inherits cancellation from the caller and is additionally cancellable via Shutdown().
func (m *Manager) runDNSTask(ctx context.Context) {
	defer close(m.done)

	// Initial discovery pass.
	err := m.refresh(ctx)
	close(m.initDone)

	if err != nil {
		m.logger.Warn("initial discovery failed",
			slog.Any("error", err),
			slog.String("mode", string(m.cfg.Mode)),
		)
	}

	// Periodic refresh loop. Only reached after the initial pass completes.
	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			m.logger.Debug("discovery refresh loop stopped")
			return
		case <-ctx.Done():
			m.logger.Debug("discovery refresh loop stopped (context cancelled)")
			return
		case <-ticker.C:
			// Errors are logged inside refresh; the loop continues.
			_ = m.refresh(ctx)
		}
	}
}

// Shutdown stops the discovery goroutine and cancels any in-flight DNS lookup
// via the manager's derived context. It is safe to call multiple times and from
// concurrent goroutines.
//
// Calling Shutdown before Start returns immediately (the done channel is
// never closed for a manager that has not started, so we simply return).
// After Start, Shutdown cancels the derived context (aborting any active
// lookup), closes the stop channel, and waits for the discovery goroutine to
// finish.
func (m *Manager) Shutdown() {
	m.startMu.Lock()
	defer m.startMu.Unlock()

	if !m.started {
		// Never started: no goroutine to wait for, no stop to signal.
		return
	}

	// Cancel the derived context to abort any in-flight DNS lookup, whether
	// it is the initial discovery or a periodic refresh.
	// context.CancelFunc is idempotent, so repeated calls are safe.
	if m.mgrCancel != nil {
		m.mgrCancel()
	}

	// Close the stop channel once to signal the refresh loop to exit.
	select {
	case <-m.stopCh:
		// Already stopped.
	default:
		close(m.stopCh)
	}

	// Wait for the initial discovery work to finish (if any was started).
	// For off and static modes, initDone is closed in Start. For DNS mode it
	// is closed by runDNSTask after the initial pass completes.
	if m.initial {
		<-m.initDone
	}

	// Wait for the discovery goroutine to finish. For off/static modes, done
	// is closed in Start. For DNS mode, done is closed by runDNSTask.
	<-m.done
}

// refresh performs a single discovery pass and updates the membership state.
// It returns the error from the lookup (if any) but never panics.
func (m *Manager) refresh(ctx context.Context) error {
	var peers []Peer
	var err error

	switch m.cfg.Mode {
	case ModeStatic:
		peers, err = m.discoverStatic(ctx)
	case ModeDNS:
		peers, err = m.discoverDNS(ctx)
	default:
		return fmt.Errorf("unknown discovery mode %q", m.cfg.Mode)
	}

	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()

	if err != nil {
		// Mark the membership stale; preserve last-known data.
		m.stale = true
		m.lastErr = err
		m.logger.Warn("discovery refresh failed; membership marked stale",
			slog.Any("error", err),
			slog.String("mode", string(m.cfg.Mode)),
		)
		return err
	}

	peers = dedup(peers)

	// Rebuild the membership map from the new peer list.
	newMembers := make(map[string]*member, len(peers))
	for _, p := range peers {
		key := membershipKey(p)
		if key == "" {
			continue
		}
		newMembers[key] = &member{
			peer:     p,
			discover: now,
		}
	}

	// Detect additions and removals by comparing with the current map.
	added := make([]string, 0)
	removed := make([]string, 0)
	for key, mbr := range newMembers {
		if old, ok := m.members[key]; !ok || old.peer.Address != mbr.peer.Address {
			added = append(added, mbr.peer.Address)
		}
	}
	for key, mbr := range m.members {
		if _, ok := newMembers[key]; !ok {
			removed = append(removed, mbr.peer.Address)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)

	if len(added) > 0 {
		m.logger.Info("peers added", "peers", added, "mode", string(m.cfg.Mode))
	}
	if len(removed) > 0 {
		m.logger.Info("peers removed", "peers", removed, "mode", string(m.cfg.Mode))
	}

	m.members = newMembers
	m.stale = false
	m.lastErr = nil
	m.lastUpdate = now

	return nil
}

// discoverStatic returns the configured static peers (deterministic, no
// network I/O).
func (m *Manager) discoverStatic(_ context.Context) ([]Peer, error) {
	now := time.Now()
	peers := make([]Peer, 0, len(m.cfg.StaticPeers))
	for _, addr := range m.cfg.StaticPeers {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			// Configuration was validated at load time; this is a defensive check.
			return nil, fmt.Errorf("parsing static peer %q: %w", addr, err)
		}
		hostname := normalizeHostname(host)
		peers = append(peers, Peer{
			Hostname:    hostname,
			PodIdentity: m.localPodIdentity(hostname),
			Address:     addr,
			Source:      SourceStatic,
			LastSeen:    now,
		})
	}
	return m.filterSelf(peers), nil
}

// discoverDNS performs a single SRV lookup and converts the results to peers.
//
// The SRV query is for the configured service name and protocol "tcp" under
// the configured headless Service DNS name, e.g.:
//
//	_peer._tcp.gossipgrid-headless.default.svc.cluster.local
func (m *Manager) discoverDNS(ctx context.Context) ([]Peer, error) {
	// Bound the lookup with a timeout.
	lookupCtx, cancel := context.WithTimeout(ctx, m.cfg.Timeout)
	defer cancel()

	name := normalizeServiceName(m.cfg.DNSName)
	hosts, ports, err := m.lookupFn(lookupCtx, m.cfg.SRVService, "tcp", name)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	peers := make([]Peer, 0, len(hosts))
	for i, host := range hosts {
		if i >= len(ports) {
			break // Malformed response: fewer ports than hosts.
		}
		hostname := normalizeHostname(host)
		if hostname == "" {
			continue
		}
		addr := fmt.Sprintf("%s:%d", hostname, ports[i])
		peers = append(peers, Peer{
			Hostname:    hostname,
			PodIdentity: m.localPodIdentity(hostname),
			Address:     addr,
			Source:      SourceDNS,
			LastSeen:    now,
		})
	}

	return m.filterSelf(peers), nil
}

// localPodIdentity derives the canonical StatefulSet pod identity from a
// hostname (which may be a FQDN or a short name) using the configured
// headless Service DNS suffix.
//
// The headless Service DNS name (e.g. "gossipgrid-headless.default.svc") is
// the suffix that follows the pod short name in SRV FQDN targets:
//
//	gossipgrid-0.gossipgrid-headless.default.svc.cluster.local
//	        ^^^^^^^^^^^^  ^^^^^^^^^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^^^
//	        pod short name   headless Service DNS name      cluster domain
//
// The FQDN is formed as "<short>.<dnsSuffix>.<clusterDomain>" where the
// cluster domain (e.g. "cluster.local") is not known in advance and must
// not be hard-coded. The pod identity is extracted by:
//  1. Checking the hostname starts with "<statefulset-name>-".
//  2. Splitting the remainder at the first dot.
//  3. For FQDN form: verifying the DNS part starts with the configured
//     suffix (the remainder after the suffix is the cluster domain, which
//     we accept without knowing its value).
//  4. For short name form (no dot): verifying the remainder is a valid ordinal.
//
// This avoids substring matching and prevents unrelated hosts from being
// identified as GossipGrid replicas.
func (m *Manager) localPodIdentity(hostname string) string {
	if m.cfg.StatefulSetName == "" {
		return ""
	}
	stsPrefix := m.cfg.StatefulSetName + "-"
	// The hostname must start with the StatefulSet name prefix.
	if !strings.HasPrefix(hostname, stsPrefix) {
		return ""
	}
	// Strip the prefix; the remainder should be "<ordinal>.<dnsSuffix>[.clusterDomain]"
	// (for FQDNs) or "<ordinal>" (for short names).
	remainder := hostname[len(stsPrefix):]
	// Check for a dot separator: FQDN form.
	if dotIdx := strings.IndexByte(remainder, '.'); dotIdx > 0 {
		ordinal := remainder[:dotIdx]
		dnsPart := remainder[dotIdx+1:] // the part after the first dot
		if m.cfg.DNSName == "" {
			// No DNS suffix configured: cannot verify FQDN belongs to our
			// StatefulSet. Do not map to a pod identity.
			return ""
		}
		dnsSuffix := normalizeServiceName(m.cfg.DNSName)
		// The DNS part must start with the configured suffix. The FQDN
		// format is <short>.<dnsSuffix>.<clusterDomain>, so dnsPart =
		// <dnsSuffix>.<clusterDomain>. We check that dnsPart starts with
		// dnsSuffix followed by either nothing (exact match) or a dot
		// (cluster domain follows).
		if !strings.HasPrefix(dnsPart, dnsSuffix) {
			return ""
		}
		afterSuffix := dnsPart[len(dnsSuffix):]
		// afterSuffix must be either "" (exact match, no cluster domain)
		// or start with "." (cluster domain present).
		if afterSuffix != "" && !strings.HasPrefix(afterSuffix, ".") {
			return ""
		}
		// Validate ordinal is a non-negative integer.
		if _, err := strconv.Atoi(ordinal); err != nil {
			return ""
		}
		return stsPrefix + ordinal
	}
	// No dot: short name form. Verify the remainder is a valid ordinal.
	if _, err := strconv.Atoi(remainder); err != nil {
		return ""
	}
	return stsPrefix + remainder
}

// filterSelf removes the local node's entry from the peer list when its
// identity can be reliably determined. It compares both the normalized
// hostname (FQDN-level) and the derived pod identity (short-name-level) so
// that a local node identified by a short name matches its own FQDN in
// SRV responses, and vice versa. Exact comparison only; no substring matching.
func (m *Manager) filterSelf(peers []Peer) []Peer {
	localHostname := normalizeHostname(m.cfg.NodeID)
	localPodID := m.localPodIdentity(localHostname)

	var localIDs map[string]bool
	if localHostname != "" {
		localIDs = make(map[string]bool, 2)
		localIDs[localHostname] = true
	}
	if localPodID != "" {
		if localIDs == nil {
			localIDs = make(map[string]bool, 1)
		}
		localIDs[localPodID] = true
	}
	if len(localIDs) == 0 {
		// Cannot determine identity; keep all peers.
		return peers
	}

	result := make([]Peer, 0, len(peers))
	for _, p := range peers {
		if localIDs[p.Hostname] || localIDs[p.PodIdentity] {
			m.logger.Debug("excluding self from peer list", "hostname", p.Hostname, "pod_identity", p.PodIdentity)
			continue
		}
		result = append(result, p)
	}
	return result
}

// dedup removes duplicate peers by membership key, keeping the first
// occurrence.
func dedup(peers []Peer) []Peer {
	seen := make(map[string]bool, len(peers))
	result := make([]Peer, 0, len(peers))
	for _, p := range peers {
		key := membershipKey(p)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, p)
	}
	return result
}

// membershipKey returns a canonical key for a peer, used for deduplication and
// the membership map. The key is the normalized hostname + port.
func membershipKey(p Peer) string {
	host, port, err := net.SplitHostPort(p.Address)
	if err != nil {
		return ""
	}
	return normalizeHostname(host) + ":" + port
}

// normalizeHostname lowercases and strips a trailing dot from a DNS hostname.
// An empty result is returned for an empty or dot-only input.
func normalizeHostname(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.TrimSuffix(host, ".")
	return host
}

// normalizeServiceName lowercases and strips a trailing dot from a service DNS
// name. It does NOT append "cluster.local" — the actual cluster DNS domain may
// differ, and the operator is expected to configure the full name.
func normalizeServiceName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	name = strings.TrimSuffix(name, ".")
	return name
}

// buildLocalNode returns a Peer representing the local node's canonical
// identity, or nil when the identity cannot be established. It derives the
// pod identity from the configured NodeID (which may be a short name like
// "gossipgrid-0" or a FQDN). The returned Peer has SourceLocal and the
// pod identity as both Hostname and PodIdentity.
//
// The node is only reported when localPodIdentity returns a non-empty value,
// which requires the StatefulSet name prefix to match. This prevents false
// identification when the NodeID is an arbitrary hostname (e.g. a local
// machine name in off mode).
func (m *Manager) buildLocalNode() *Peer {
	nodeID := normalizeHostname(m.cfg.NodeID)
	podID := m.localPodIdentity(nodeID)
	if podID == "" {
		return nil
	}
	return &Peer{
		Hostname:    podID,
		PodIdentity: podID,
		Address:     podID,
		Source:      SourceLocal,
		LastSeen:    time.Now(),
	}
}

// Snapshot returns an immutable copy of the current membership state.
// The returned Snapshot is safe to read without holding a lock.
func (m *Manager) Snapshot() Snapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := make([]Peer, 0, len(m.members))
	for _, mbr := range m.members {
		peers = append(peers, mbr.peer)
	}
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].Address < peers[j].Address
	})

	expected := expectedIdentities(m.cfg.StatefulSetName, m.cfg.ExpectedReplicas)

	return Snapshot{
		Peers:              peers,
		LocalNode:          m.buildLocalNode(),
		ExpectedIdentities: expected,
		Stale:              m.stale,
		LastUpdated:        m.lastUpdate,
		LastError:          m.lastErr,
		Mode:               m.cfg.Mode,
	}
}

// expectedIdentities returns the StatefulSet ordinal identities for the
// configured replica count.
func expectedIdentities(statefulsetName string, replicas int) []ExpectedIdentity {
	result := make([]ExpectedIdentity, 0, replicas)
	for i := 0; i < replicas; i++ {
		result = append(result, ExpectedIdentity{
			Hostname: fmt.Sprintf("%s-%d", statefulsetName, i),
		})
	}
	return result
}

// Reconcile compares expected identities against discovered peers and returns
// the state of each expected replica plus any unexpected peers.
//
// An expected identity that is the local node (as reported by Snapshot.LocalNode)
// is reported as MemberLocal. An expected identity that is missing from the
// discovered peer list is reported as MemberMissing (it may be a transient DNS
// gap or a pod that has not started yet). A discovered peer whose pod identity
// does not match any expected identity is reported as unexpected (it may be
// from a different StatefulSet or a leftover DNS record from a scaled-down
// cluster).
//
// For peers with an empty PodIdentity (unrelated hosts whose hostname does not
// match the configured headless Service suffix), the full hostname is used as
// the comparison key against expected identities. This ensures that unrelated
// hosts are never silently matched to expected StatefulSet identities.
//
// Reconcile is a pure function: it reads only the Snapshot argument and returns
// a new map and slice. It does not access any Manager state.
func Reconcile(snapshot Snapshot) (expected map[string]MemberState, unexpected []string) {
	expected = make(map[string]MemberState, len(snapshot.ExpectedIdentities))

	// Determine the local node's identity (nil when it cannot be established).
	var localID string
	if snapshot.LocalNode != nil {
		localID = snapshot.LocalNode.PodIdentity
	}

	for _, id := range snapshot.ExpectedIdentities {
		// The local replica is reported as MemberLocal, not MemberMissing or
		// MemberFound. This is distinct from a discovered peer: the local node
		// is known by configuration, not by DNS observation.
		if id.Hostname == localID {
			expected[id.Hostname] = MemberLocal
			continue
		}
		found := false
		for _, p := range snapshot.Peers {
			// Compare by pod identity if available; otherwise by hostname.
			peerID := p.PodIdentity
			if peerID == "" {
				peerID = p.Hostname
			}
			if peerID == id.Hostname {
				found = true
				break
			}
		}
		if found {
			expected[id.Hostname] = MemberFound
		} else {
			expected[id.Hostname] = MemberMissing
		}
	}

	// Build the set of expected pod identities for unexpected-peer detection.
	// The local node is not a "discovered" peer, so it is not included here.
	expectedSet := make(map[string]bool, len(snapshot.ExpectedIdentities))
	for _, id := range snapshot.ExpectedIdentities {
		expectedSet[id.Hostname] = true
	}
	for _, p := range snapshot.Peers {
		peerID := p.PodIdentity
		if peerID == "" {
			peerID = p.Hostname
		}
		if !expectedSet[peerID] {
			unexpected = append(unexpected, p.Address)
		}
	}
	sort.Strings(unexpected)
	return expected, unexpected
}
