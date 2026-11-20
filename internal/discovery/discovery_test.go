package discovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// testLogger returns a discard logger for tests that don't inspect log output.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeLookup is an injectable DNS lookup for tests. It records the arguments
// it receives so tests can assert on the service, protocol, and DNS name
// passed to the resolver. It must NOT silently ignore these parameters:
// tests that assert on the query arguments rely on them being captured.
type fakeLookup struct {
	mu         sync.Mutex
	hosts      []string
	ports      []int
	err        error
	gotService string
	gotProto   string
	gotName    string
	callCount  int
}

func (f *fakeLookup) call(ctx context.Context, service, proto, name string) (host []string, port []int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Record the arguments for assertion; do not ignore them.
	f.gotService = service
	f.gotProto = proto
	f.gotName = name
	f.callCount++
	return f.hosts, f.ports, f.err
}

func (f *fakeLookup) lastArgs() (service, proto, name string, count int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gotService, f.gotProto, f.gotName, f.callCount
}

func (f *fakeLookup) setHosts(hosts []string, ports []int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hosts = hosts
	f.ports = ports
}

func (f *fakeLookup) setErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// 1. Static peer configuration and discovery.
func TestStaticDiscovery(t *testing.T) {
	cfg := Config{
		Mode:             ModeStatic,
		StaticPeers:      []string{"gossipgrid-1:8443", "gossipgrid-2:8443", "gossipgrid-3:8443"},
		ExpectedReplicas: 5,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), nil)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	if len(snap.Peers) != 3 {
		t.Fatalf("expected 3 peers, got %d: %+v", len(snap.Peers), snap.Peers)
	}
	if snap.Stale {
		t.Error("snapshot should not be stale after successful static discovery")
	}
	for _, p := range snap.Peers {
		if p.Source != SourceStatic {
			t.Errorf("peer %s: source = %q, want %q", p.Address, p.Source, SourceStatic)
		}
	}
}

// 1b. Static discovery with bracketed IPv6 address.
func TestStaticDiscoveryIPv6(t *testing.T) {
	cfg := Config{
		Mode:             ModeStatic,
		StaticPeers:      []string{"[::1]:8443", "127.0.0.1:8443"},
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), nil)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	if len(snap.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d: %+v", len(snap.Peers), snap.Peers)
	}
	// The IPv6 address should be preserved as-is in the peer list.
	foundV6 := false
	for _, p := range snap.Peers {
		if p.Address == "[::1]:8443" {
			foundV6 = true
		}
	}
	if !foundV6 {
		t.Error("bracketed IPv6 address not found in peers")
	}
}

// 2. DNS SRV response parsing.
func TestDNSSRVParsing(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0.", "gossipgrid-1", "gossipgrid-2"},
		ports: []int{8443, 8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour, // long interval so the refresh loop doesn't interfere
		Timeout:          2 * time.Second,
		ExpectedReplicas: 5,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	// gossipgrid-0 is excluded (self), so expect 2 peers.
	if len(snap.Peers) != 2 {
		t.Fatalf("expected 2 peers (self excluded), got %d: %+v", len(snap.Peers), snap.Peers)
	}
	for _, p := range snap.Peers {
		if p.Source != SourceDNS {
			t.Errorf("peer %s: source = %q, want %q", p.Address, p.Source, SourceDNS)
		}
		// Trailing dot should be stripped from hostnames.
		if p.Hostname == "gossipgrid-0." || p.Hostname == "gossipgrid-1." {
			t.Errorf("peer %s: trailing dot not stripped", p.Hostname)
		}
	}
}

// 2b. DNS SRV query argument assertion.
// Verifies that the resolver is queried with the correct service, protocol,
// and DNS name arguments. The fake lookup records the arguments; this test
// asserts they match the configured values.
func TestDNSSRVQueryArguments(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0", "gossipgrid-1"},
		ports: []int{8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.prod.svc.example.internal",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 2,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	svc, proto, name, count := f.lastArgs()
	if count == 0 {
		t.Fatal("lookup function was never called")
	}
	if svc != "peer" {
		t.Errorf("service = %q, want %q", svc, "peer")
	}
	if proto != "tcp" {
		t.Errorf("protocol = %q, want %q", proto, "tcp")
	}
	wantName := "gossipgrid-headless.prod.svc.example.internal"
	if name != wantName {
		t.Errorf("DNS name = %q, want %q", name, wantName)
	}
}

// 3. Peer deduplication.
func TestPeerDeduplication(t *testing.T) {
	// Two peers with the same hostname and port but different case.
	peers := []Peer{
		{Hostname: "gossipgrid-1", Address: "gossipgrid-1:8443", Source: SourceDNS, LastSeen: time.Now()},
		{Hostname: "GOSSIPGRID-1", Address: "GOSSIPGRID-1:8443", Source: SourceDNS, LastSeen: time.Now()},
	}
	deduped := dedup(peers)
	if len(deduped) != 1 {
		t.Fatalf("dedup: expected 1 peer, got %d: %+v", len(deduped), deduped)
	}
}

// 4. Self-exclusion.
func TestSelfExclusion(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		ports: []int{8443, 8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	for _, p := range snap.Peers {
		if p.Hostname == "gossipgrid-0" {
			t.Fatalf("self (gossipgrid-0) should be excluded from peers: %+v", snap.Peers)
		}
	}
	if len(snap.Peers) != 2 {
		t.Fatalf("expected 2 peers after self-exclusion, got %d", len(snap.Peers))
	}
}

// 4b. Self-exclusion with trailing dot normalization.
func TestSelfExclusionTrailingDot(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0.", "gossipgrid-1"},
		ports: []int{8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 2,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	if len(snap.Peers) != 1 {
		t.Fatalf("expected 1 peer (self excluded via normalized hostname), got %d: %+v", len(snap.Peers), snap.Peers)
	}
}

// 4c. Self-exclusion with FQDN: short NodeID matches FQDN in SRV response.
// The local node is identified by "gossipgrid-0" but DNS returns
// "gossipgrid-0.gossipgrid-headless.default.svc.cluster.local." — the
// pod identity derivation must match these as the same node.
func TestSelfExclusionFQDN(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{
			"gossipgrid-0.gossipgrid-headless.default.svc.cluster.local.",
			"gossipgrid-1.gossipgrid-headless.default.svc.cluster.local.",
			"gossipgrid-2.gossipgrid-headless.default.svc.cluster.local.",
		},
		ports: []int{8443, 8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0", // short name
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	// gossipgrid-0 should be excluded even though DNS returns it as a FQDN.
	if len(snap.Peers) != 2 {
		t.Fatalf("expected 2 peers (FQDN self excluded), got %d: %+v", len(snap.Peers), snap.Peers)
	}
	for _, p := range snap.Peers {
		if p.PodIdentity == "gossipgrid-0" {
			t.Fatalf("self pod identity gossipgrid-0 not excluded: %+v", p)
		}
	}
}

// 4d. Self-exclusion with FQDN: FQDN NodeID matches FQDN in SRV response.
func TestSelfExclusionFQDNNodeID(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{
			"gossipgrid-0.gossipgrid-headless.default.svc.cluster.local.",
			"gossipgrid-1.gossipgrid-headless.default.svc.cluster.local.",
		},
		ports: []int{8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 2,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0.gossipgrid-headless.default.svc.cluster.local", // FQDN
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	if len(snap.Peers) != 1 {
		t.Fatalf("expected 1 peer (FQDN NodeID self excluded), got %d: %+v", len(snap.Peers), snap.Peers)
	}
}

// 5. Expected versus discovered membership.
func TestExpectedVsDiscovered(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0", "gossipgrid-1"},
		ports: []int{8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 5,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	if len(snap.ExpectedIdentities) != 5 {
		t.Fatalf("expected 5 identities, got %d", len(snap.ExpectedIdentities))
	}
	for i, id := range snap.ExpectedIdentities {
		want := fmt.Sprintf("gossipgrid-%d", i)
		if id.Hostname != want {
			t.Errorf("identity[%d] = %q, want %q", i, id.Hostname, want)
		}
	}

	// The local node should be reported in the snapshot.
	if snap.LocalNode == nil {
		t.Fatal("LocalNode should be set when NodeID matches StatefulSet prefix")
	}
	if snap.LocalNode.PodIdentity != "gossipgrid-0" {
		t.Errorf("LocalNode.PodIdentity = %q, want %q", snap.LocalNode.PodIdentity, "gossipgrid-0")
	}
	if snap.LocalNode.Source != SourceLocal {
		t.Errorf("LocalNode.Source = %q, want %q", snap.LocalNode.Source, SourceLocal)
	}

	expected, unexpected := Reconcile(snap)
	if len(expected) != 5 {
		t.Fatalf("expected 5 member states, got %d", len(expected))
	}
	// gossipgrid-0 is the local node, reported as MemberLocal (not MemberMissing).
	if expected["gossipgrid-0"] != MemberLocal {
		t.Errorf("gossipgrid-0 state = %q, want %q (local node)", expected["gossipgrid-0"], MemberLocal)
	}
	if expected["gossipgrid-1"] != MemberFound {
		t.Errorf("gossipgrid-1 state = %q, want %q", expected["gossipgrid-1"], MemberFound)
	}
	for i := 2; i <= 4; i++ {
		key := fmt.Sprintf("gossipgrid-%d", i)
		if expected[key] != MemberMissing {
			t.Errorf("%s state = %q, want %q", key, expected[key], MemberMissing)
		}
	}
	if len(unexpected) != 0 {
		t.Errorf("unexpected peers: %v, want none", unexpected)
	}
}

// 5b. Unexpected peer detection.
func TestUnexpectedPeer(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0", "gossipgrid-1", "other-service-0"},
		ports: []int{8443, 8443, 9999},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 2,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	_, unexpected := Reconcile(snap)
	if len(unexpected) != 1 {
		t.Fatalf("expected 1 unexpected peer, got %d: %v", len(unexpected), unexpected)
	}
	if unexpected[0] != "other-service-0:9999" {
		t.Errorf("unexpected[0] = %q, want %q", unexpected[0], "other-service-0:9999")
	}
}

// 5c. FQDN expected vs discovered: reconciliation with real Kubernetes FQDNs.
// SRV returns FQDNs; expected identities are short names. Reconcile must
// match them via the pod identity derivation.
func TestExpectedVsDiscoveredFQDN(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{
			"gossipgrid-0.gossipgrid-headless.default.svc.cluster.local.",
			"gossipgrid-1.gossipgrid-headless.default.svc.cluster.local.",
			"gossipgrid-2.gossipgrid-headless.default.svc.cluster.local.",
		},
		ports: []int{8443, 8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	// gossipgrid-0 is self-excluded; gossipgrid-1 and gossipgrid-2 remain.
	if len(snap.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d: %+v", len(snap.Peers), snap.Peers)
	}

	expected, unexpected := Reconcile(snap)
	// gossipgrid-0 is the local node (FQDN in DNS, short NodeID) -> MemberLocal.
	if expected["gossipgrid-0"] != MemberLocal {
		t.Errorf("gossipgrid-0 state = %q, want %q (local node)", expected["gossipgrid-0"], MemberLocal)
	}
	// gossipgrid-1 and gossipgrid-2 are discovered as FQDNs -> found.
	if expected["gossipgrid-1"] != MemberFound {
		t.Errorf("gossipgrid-1 state = %q, want %q", expected["gossipgrid-1"], MemberFound)
	}
	if expected["gossipgrid-2"] != MemberFound {
		t.Errorf("gossipgrid-2 state = %q, want %q", expected["gossipgrid-2"], MemberFound)
	}
	if len(unexpected) != 0 {
		t.Errorf("unexpected peers: %v, want none", unexpected)
	}
}

// 5d. Unrelated hostname suffix: a host that does not match the headless
// Service suffix must not be identified as a GossipGrid replica.
func TestUnrelatedHostnameSuffix(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{
			"gossipgrid-0.gossipgrid-headless.default.svc.cluster.local.",
			"gossipgrid-1.other-service.default.svc.cluster.local.",
			"gossipgrid-1.gossipgrid-headless.default.svc.cluster.local.",
		},
		ports: []int{8443, 8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 2,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	// gossipgrid-0 is self-excluded.
	// gossipgrid-1.other-service... has a different suffix -> PodIdentity=""
	//    -> it should appear as an unexpected peer (not matched to expected).
	// gossipgrid-1.gossipgrid-headless... -> PodIdentity="gossipgrid-1" -> found.
	// After dedup by membership key (full FQDN + port), both gossipgrid-1
	// entries survive (different hostnames). We expect 2 peers.
	if len(snap.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d: %+v", len(snap.Peers), snap.Peers)
	}

	expected, unexpected := Reconcile(snap)
	if expected["gossipgrid-1"] != MemberFound {
		t.Errorf("gossipgrid-1 state = %q, want %q", expected["gossipgrid-1"], MemberFound)
	}
	// The other-service host is unexpected (no matching expected identity).
	if len(unexpected) != 1 {
		t.Fatalf("expected 1 unexpected peer, got %d: %v", len(unexpected), unexpected)
	}
}

// 6. Dynamic membership additions and removals.
func TestDynamicMembershipChanges(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0", "gossipgrid-1"},
		ports: []int{8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         100 * time.Millisecond, // short interval for test
		Timeout:          2 * time.Second,
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	// Initial state: gossipgrid-1 is the only peer (gossipgrid-0 excluded).
	snap := m.Snapshot()
	if len(snap.Peers) != 1 {
		t.Fatalf("initial: expected 1 peer, got %d", len(snap.Peers))
	}

	// Add a new peer.
	f.setHosts([]string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"}, []int{8443, 8443, 8443})

	waitForCondition(t, "peer added", func() bool {
		return len(m.Snapshot().Peers) == 2
	})

	// Remove a peer.
	f.setHosts([]string{"gossipgrid-0"}, []int{8443})

	waitForCondition(t, "peer removed", func() bool {
		return len(m.Snapshot().Peers) == 0
	})
}

// 7. Empty DNS responses.
func TestEmptyDNSResponse(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{},
		ports: []int{},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 5,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	if len(snap.Peers) != 0 {
		t.Fatalf("expected 0 peers for empty DNS response, got %d", len(snap.Peers))
	}
	if snap.Stale {
		t.Error("snapshot should not be stale for a successful (empty) DNS response")
	}
}

// 8. DNS lookup errors and timeouts.
func TestDNSLookupError(t *testing.T) {
	f := &fakeLookup{
		err: errors.New("DNS server not found"),
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 5,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	// Start should not return an error for DNS mode failures.
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start should not fail on DNS lookup error, got: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	if len(snap.Peers) != 0 {
		t.Errorf("expected 0 peers after DNS failure, got %d", len(snap.Peers))
	}
	if !snap.Stale {
		t.Error("snapshot should be marked stale after DNS failure")
	}
	if snap.LastError == nil {
		t.Error("LastError should be set after DNS failure")
	}
}

// 8b. DNS lookup timeout (context cancellation).
func TestDNSLookupTimeout(t *testing.T) {
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          1 * time.Millisecond, // extremely short timeout
		ExpectedReplicas: 5,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	// Use a lookup that blocks until context is done, simulating a slow DNS.
	slowLookup := func(ctx context.Context, service, proto, name string) ([]string, []int, error) {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return []string{"gossipgrid-1"}, []int{8443}, nil
		}
	}
	m := NewManager(cfg, testLogger(), slowLookup)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start should not fail on DNS timeout, got: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	if !snap.Stale {
		t.Error("snapshot should be marked stale after DNS timeout")
	}
}

// 9. Stale membership after a failed refresh.
func TestStaleMembershipAfterFailedRefresh(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		ports: []int{8443, 8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         50 * time.Millisecond,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	// Verify initial success.
	snap := m.Snapshot()
	if snap.Stale {
		t.Fatal("snapshot should not be stale after initial success")
	}
	if len(snap.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(snap.Peers))
	}
	lastUpdated := snap.LastUpdated

	// Now make the DNS lookup fail.
	f.setErr(errors.New("connection refused"))

	// Wait for a failed refresh to mark the snapshot stale.
	waitForCondition(t, "stale after failure", func() bool {
		s := m.Snapshot()
		return s.Stale && s.LastUpdated.Equal(lastUpdated)
	})

	snap = m.Snapshot()
	if !snap.Stale {
		t.Error("snapshot should be stale after failed refresh")
	}
	// Peers from the last successful pass should still be present.
	if len(snap.Peers) != 2 {
		t.Errorf("stale snapshot should retain last-known peers, got %d", len(snap.Peers))
	}
}

// 10. Context cancellation and clean shutdown.
func TestContextCancellationAndShutdown(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0", "gossipgrid-1"},
		ports: []int{8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         10 * time.Millisecond,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 2,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx, cancel := context.WithCancel(context.Background())
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Let a few refresh cycles run.
	time.Sleep(50 * time.Millisecond)

	// Cancel the context; the refresh loop should stop.
	cancel()
	// Shutdown should return promptly (no goroutine leak).
	done := make(chan struct{})
	go func() {
		m.Shutdown()
		close(done)
	}()
	select {
	case <-done:
		// Clean shutdown.
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return within 2s; goroutine leak suspected")
	}

	// Second Shutdown should not block (idempotent).
	done2 := make(chan struct{})
	go func() {
		m.Shutdown()
		close(done2)
	}()
	select {
	case <-done2:
	case <-time.After(1 * time.Second):
		t.Fatal("second Shutdown did not return promptly")
	}
}

// 10b. Shutdown with off mode (no goroutine started).
func TestShutdownOffMode(t *testing.T) {
	cfg := Config{
		Mode:             ModeOff,
		ExpectedReplicas: 1,
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), nil)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start (off): %v", err)
	}
	m.Shutdown()
	// Should not deadlock since no refresh goroutine was started.
}

// 11. Configuration validation.
func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"valid off mode", Config{Mode: ModeOff, ExpectedReplicas: 1}, false},
		{"valid static mode", Config{Mode: ModeStatic, StaticPeers: []string{"a:8443"}, ExpectedReplicas: 1}, false},
		{"valid dns mode", Config{
			Mode: ModeDNS, DNSName: "gossipgrid-headless.ns.svc", SRVService: "peer",
			Interval: 10 * time.Second, Timeout: 3 * time.Second,
			ExpectedReplicas: 5, StatefulSetName: "gossipgrid",
		}, false},
		{"valid static mode with IPv6", Config{Mode: ModeStatic, StaticPeers: []string{"[::1]:8443"}, ExpectedReplicas: 1}, false},
		{"static mode with no peers", Config{Mode: ModeStatic, ExpectedReplicas: 1}, true},
		{"static mode with bad address", Config{Mode: ModeStatic, StaticPeers: []string{"localhost"}, ExpectedReplicas: 1}, true},
		{"static mode with empty host", Config{Mode: ModeStatic, StaticPeers: []string{":8443"}, ExpectedReplicas: 1}, true},
		{"static mode with port 0", Config{Mode: ModeStatic, StaticPeers: []string{"a:0"}, ExpectedReplicas: 1}, true},
		{"static mode with port 65536", Config{Mode: ModeStatic, StaticPeers: []string{"a:65536"}, ExpectedReplicas: 1}, true},
		{"static mode with non-numeric port", Config{Mode: ModeStatic, StaticPeers: []string{"a:80a"}, ExpectedReplicas: 1}, true},
		{"dns mode with empty DNS name", Config{
			Mode: ModeDNS, SRVService: "peer",
			Interval: 10 * time.Second, Timeout: 3 * time.Second,
			ExpectedReplicas: 1,
		}, true},
		{"dns mode with zero interval", Config{
			Mode: ModeDNS, DNSName: "svc", SRVService: "peer",
			Interval: 0, Timeout: 3 * time.Second,
			ExpectedReplicas: 1,
		}, true},
		{"dns mode with negative interval", Config{
			Mode: ModeDNS, DNSName: "svc", SRVService: "peer",
			Interval: -1 * time.Second, Timeout: 3 * time.Second,
			ExpectedReplicas: 1,
		}, true},
		{"dns mode with zero timeout", Config{
			Mode: ModeDNS, DNSName: "svc", SRVService: "peer",
			Interval: 10 * time.Second, Timeout: 0,
			ExpectedReplicas: 1,
		}, true},
		{"dns mode with negative timeout", Config{
			Mode: ModeDNS, DNSName: "svc", SRVService: "peer",
			Interval: 10 * time.Second, Timeout: -2 * time.Second,
			ExpectedReplicas: 1,
		}, true},
		{"unknown mode", Config{Mode: Mode("unknown"), ExpectedReplicas: 1}, true},
		{"zero replicas", Config{Mode: ModeOff, ExpectedReplicas: 0}, true},
		{"negative replicas", Config{Mode: ModeOff, ExpectedReplicas: -1}, true},
		{"replicas > 1 without statefulset name", Config{Mode: ModeOff, ExpectedReplicas: 3}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

// 11b. Start with invalid config must not panic.
// A zero or negative interval would cause time.NewTicker to panic.
// Start must validate the config first and return an error.
func TestStartWithInvalidConfigNoPanic(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{"zero interval", Config{
			Mode: ModeDNS, DNSName: "svc", SRVService: "peer",
			Interval: 0, Timeout: 3 * time.Second,
			ExpectedReplicas: 1,
		}},
		{"negative timeout", Config{
			Mode: ModeDNS, DNSName: "svc", SRVService: "peer",
			Interval: 10 * time.Second, Timeout: -1 * time.Second,
			ExpectedReplicas: 1,
		}},
		{"invalid static port", Config{
			Mode: ModeStatic, StaticPeers: []string{"a:99999"},
			ExpectedReplicas: 1,
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := NewManager(tc.cfg, testLogger(), nil)
			ctx := context.Background()
			err := m.Start(ctx)
			if err == nil {
				t.Fatal("Start() should return an error for invalid config")
			}
			// After a failed Start, Shutdown must not hang.
			done := make(chan struct{})
			go func() {
				m.Shutdown()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(1 * time.Second):
				t.Fatal("Shutdown hung after failed Start")
			}
		})
	}
}

// 12. Race-safe concurrent membership reads and updates.
func TestConcurrentAccess(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		ports: []int{8443, 8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         5 * time.Millisecond, // very fast refresh
		Timeout:          2 * time.Second,
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	// Concurrent readers.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				snap := m.Snapshot()
				_, _ = Reconcile(snap)
			}
		}()
	}

	// Concurrent writer: periodically change the peer set.
	stopWriter := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		i := 0
		for {
			select {
			case <-stopWriter:
				return
			case <-ticker.C:
				if i%2 == 0 {
					f.setHosts([]string{"gossipgrid-0", "gossipgrid-1"}, []int{8443, 8443})
				} else {
					f.setHosts([]string{"gossipgrid-0", "gossipgrid-2"}, []int{8443, 8443})
				}
				i++
			}
		}
	}()

	close(stopWriter)
	wg.Wait()
}

// 12b. Concurrent Start and Shutdown (race detector test).
// Multiple goroutines call Start and Shutdown concurrently on the SAME
// manager instance. The lifecycle must not race: Start is guarded by a mutex,
// Shutdown is idempotent, and the derived context cancellation is safe under
// concurrent access.
func TestConcurrentStartShutdown(t *testing.T) {
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         100 * time.Millisecond,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 2,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), nil)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.Start(ctx)
			m.Shutdown()
			// Second shutdown (concurrent safety).
			m.Shutdown()
		}()
	}
	wg.Wait()
}

// 12c. Repeated Start returns an error (not a panic) in DNS mode.
func TestRepeatedStartDNSMode(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0", "gossipgrid-1"},
		ports: []int{8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 2,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)
	ctx := context.Background()

	if err := m.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	err := m.Start(ctx)
	if err == nil {
		t.Fatal("second Start should return an error, got nil")
	}
	m.Shutdown()
}

// 12d. Shutdown before Start (never started) must not hang.
func TestShutdownBeforeStart(t *testing.T) {
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         10 * time.Second,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 2,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), nil)

	// Shutdown before Start: must return immediately, no hang.
	done := make(chan struct{})
	go func() {
		m.Shutdown()
		close(done)
	}()
	select {
	case <-done:
		// Correct: returned immediately.
	case <-time.After(1 * time.Second):
		t.Fatal("Shutdown before Start hung")
	}
}

// 12e. Repeated and concurrent Shutdown in off mode.
func TestRepeatedConcurrentShutdownOffMode(t *testing.T) {
	cfg := Config{
		Mode:             ModeOff,
		ExpectedReplicas: 1,
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), nil)
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start (off): %v", err)
	}

	// Concurrent shutdowns.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.Shutdown()
			m.Shutdown() // double shutdown
		}()
	}
	wg.Wait()
}

// 12f. Shutdown waits for the running refresh loop (no goroutine leak).
func TestShutdownWaitsForRefreshLoop(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0", "gossipgrid-1"},
		ports: []int{8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         10 * time.Millisecond,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 2,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)
	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Let the refresh loop run a few cycles.
	time.Sleep(50 * time.Millisecond)

	// Shutdown should block until the loop goroutine finishes.
	// With a 10ms interval and a fast fake lookup, this should complete quickly.
	done := make(chan struct{})
	go func() {
		m.Shutdown()
		close(done)
	}()
	select {
	case <-done:
		// Loop stopped.
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return; refresh loop goroutine leak suspected")
	}
}

// TestNormalizeHostname verifies DNS hostname normalization.
func TestNormalizeHostname(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"gossipgrid-0", "gossipgrid-0"},
		{"gossipgrid-0.", "gossipgrid-0"},
		{"GOSSIPGRID-0", "gossipgrid-0"},
		{"  gossipgrid-0  ", "gossipgrid-0"},
		{"GossipGrid-1.", "gossipgrid-1"},
		{"", ""},
		{".", ""},
	}
	for _, tc := range cases {
		if got := normalizeHostname(tc.input); got != tc.want {
			t.Errorf("normalizeHostname(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// TestNormalizeServiceName verifies service name normalization.
func TestNormalizeServiceName(t *testing.T) {
	cases := []struct {
		input string
		want  string
	}{
		{"gossipgrid-headless.default.svc", "gossipgrid-headless.default.svc"},
		{"gossipgrid-headless.default.svc.", "gossipgrid-headless.default.svc"},
		{"GossipGrid-Headless.DEFAULT.SVC", "gossipgrid-headless.default.svc"},
		{"", ""},
	}
	for _, tc := range cases {
		if got := normalizeServiceName(tc.input); got != tc.want {
			t.Errorf("normalizeServiceName(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// TestMalformedSRVResponse verifies that a response with fewer ports than
// hosts is handled gracefully.
func TestMalformedSRVResponse(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		ports: []int{8443}, // only 1 port for 3 hosts
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	// Only the first host (gossipgrid-0) has a matching port; it's self-excluded.
	if len(snap.Peers) != 0 {
		t.Fatalf("expected 0 peers for malformed SRV response, got %d: %+v", len(snap.Peers), snap.Peers)
	}
}

// TestDNSRefreshRecoversAfterFailure verifies that membership is no longer
// stale after a successful refresh following a failure.
func TestDNSRefreshRecoversAfterFailure(t *testing.T) {
	f := &fakeLookup{
		err: errors.New("initial failure"),
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         50 * time.Millisecond,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 2,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	// Wait for the initial failure to mark stale.
	waitForCondition(t, "stale after initial failure", func() bool {
		return m.Snapshot().Stale
	})

	// Now recover the DNS lookup.
	f.setErr(nil)
	f.setHosts([]string{"gossipgrid-0", "gossipgrid-1"}, []int{8443, 8443})

	// Wait for recovery.
	waitForCondition(t, "recovered from failure", func() bool {
		s := m.Snapshot()
		return !s.Stale && len(s.Peers) == 1
	})
}

// TestLocalPodIdentityFQDN verifies the FQDN-to-pod-identity mapping.
func TestLocalPodIdentityFQDN(t *testing.T) {
	cfg := Config{
		DNSName:         "gossipgrid-headless.default.svc",
		StatefulSetName: "gossipgrid",
	}
	m := NewManager(cfg, testLogger(), nil)

	cases := []struct {
		hostname string
		want     string
	}{
		// FQDN with cluster.local suffix.
		{"gossipgrid-0.gossipgrid-headless.default.svc.cluster.local", "gossipgrid-0"},
		{"gossipgrid-1.gossipgrid-headless.default.svc.cluster.local", "gossipgrid-1"},
		{"gossipgrid-42.gossipgrid-headless.default.svc.cluster.local", "gossipgrid-42"},
		// FQDN without cluster.local (different cluster domain).
		{"gossipgrid-0.gossipgrid-headless.default.svc.example.internal", "gossipgrid-0"},
		// Short name (no dot after ordinal).
		{"gossipgrid-0", "gossipgrid-0"},
		{"gossipgrid-3", "gossipgrid-3"},
		// Trailing dot FQDN.
		{"gossipgrid-0.gossipgrid-headless.default.svc.cluster.local.", "gossipgrid-0"},
		// Uppercase FQDN (normalized to lowercase before comparison).
		{"GOSSIPGRID-0.GOSSIPGRID-HEADLESS.DEFAULT.SVC", "gossipgrid-0"},
		// Unrelated host: wrong suffix.
		{"gossipgrid-0.other-service.default.svc.cluster.local", ""},
		{"gossipgrid-0.gossipgrid-headless.other-ns.svc.cluster.local", ""},
		// Non-ordinal remainder.
		{"gossipgrid-abc.gossipgrid-headless.default.svc.cluster.local", ""},
		// Host that does not start with the StatefulSet name.
		{"other-0.gossipgrid-headless.default.svc.cluster.local", ""},
		// Empty.
		{"", ""},
	}
	for _, tc := range cases {
		t.Run(tc.hostname, func(t *testing.T) {
			got := m.localPodIdentity(normalizeHostname(tc.hostname))
			if got != tc.want {
				t.Errorf("localPodIdentity(%q) = %q, want %q", tc.hostname, got, tc.want)
			}
		})
	}
}

// TestLocalPodIdentityNoSuffix verifies that without a configured DNS suffix,
// FQDNs are not mapped to pod identities (no way to verify the FQDN belongs
// to our StatefulSet), but short names are still mappable.
func TestLocalPodIdentityNoSuffix(t *testing.T) {
	cfg := Config{
		DNSName:         "", // no DNS suffix configured
		StatefulSetName: "gossipgrid",
	}
	m := NewManager(cfg, testLogger(), nil)

	// FQDN should not be mapped to a pod identity without a DNS suffix.
	got := m.localPodIdentity("gossipgrid-0.gossipgrid-headless.default.svc.cluster.local")
	if got != "" {
		t.Errorf("expected empty pod identity for FQDN without DNS suffix, got %q", got)
	}
	// Short name should still be mappable (no DNS suffix needed for short names).
	got = m.localPodIdentity("gossipgrid-0")
	if got != "gossipgrid-0" {
		t.Errorf("short name without DNS suffix: expected %q, got %q", "gossipgrid-0", got)
	}
}

// waitForCondition polls fn until it returns true or 2 seconds elapse.
func waitForCondition(t *testing.T, desc string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition %q not met within 2s", desc)
}

// waitForInitialDiscovery waits until the initial discovery pass has completed
// (either successfully or with an error), or until the timeout elapses.
// It is needed because the initial discovery runs in a goroutine (not inline
// in Start), so Snapshot() may return empty peers immediately after Start
// returns. For static and DNS modes with a fast fake lookup, this returns
// promptly. For tests where the lookup is intentionally slow or blocking,
// this returns after the timeout.
func waitForInitialDiscovery(t *testing.T, m *Manager) {
	t.Helper()
	// Poll until the snapshot has peers, or LastError is set (initial
	// discovery failed), or the timeout elapses.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s := m.Snapshot()
		if len(s.Peers) > 0 || s.LastError != nil || s.Stale {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	// If we get here, the initial discovery did not complete. This is not
	// necessarily an error (the lookup may be blocking), but the test should
	// handle it. We don't Fatal here because some tests expect the lookup to
	// still be in flight.
}

// 13. Healthy three-node membership view: local vs discovered vs missing.
// Regression test: a healthy three-node cluster must distinguish the local
// node from discovered peers. The local node is reported as MemberLocal,
// discovered peers as MemberFound, and all replicas are accounted for
// (no false missing, no false unexpected).
func TestHealthyThreeNodeMembership(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{
			"gossipgrid-0.gossipgrid-headless.default.svc.cluster.local.",
			"gossipgrid-1.gossipgrid-headless.default.svc.cluster.local.",
			"gossipgrid-2.gossipgrid-headless.default.svc.cluster.local.",
		},
		ports: []int{8443, 8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour, // no periodic refresh needed for this test
		Timeout:          2 * time.Second,
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0", // short name
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()

	// The local node must be present in the snapshot.
	if snap.LocalNode == nil {
		t.Fatal("LocalNode should be set for a healthy three-node cluster")
	}
	if snap.LocalNode.PodIdentity != "gossipgrid-0" {
		t.Fatalf("LocalNode.PodIdentity = %q, want %q", snap.LocalNode.PodIdentity, "gossipgrid-0")
	}

	// Two discovered peers (self excluded from the peer list).
	if len(snap.Peers) != 2 {
		t.Fatalf("expected 2 discovered peers, got %d: %+v", len(snap.Peers), snap.Peers)
	}
	for _, p := range snap.Peers {
		if p.Source != SourceDNS {
			t.Errorf("peer %s: source = %q, want %q", p.Address, p.Source, SourceDNS)
		}
	}

	expected, unexpected := Reconcile(snap)

	// All three replicas must be accounted for: local + two discovered.
	if expected["gossipgrid-0"] != MemberLocal {
		t.Errorf("gossipgrid-0 state = %q, want %q (local)", expected["gossipgrid-0"], MemberLocal)
	}
	if expected["gossipgrid-1"] != MemberFound {
		t.Errorf("gossipgrid-1 state = %q, want %q (discovered)", expected["gossipgrid-1"], MemberFound)
	}
	if expected["gossipgrid-2"] != MemberFound {
		t.Errorf("gossipgrid-2 state = %q, want %q (discovered)", expected["gossipgrid-2"], MemberFound)
	}
	if len(unexpected) != 0 {
		t.Errorf("unexpected peers: %v, want none in a healthy cluster", unexpected)
	}

	// No missing members in a healthy cluster.
	for id, state := range expected {
		if state == MemberMissing {
			t.Errorf("%s: state = %q, want not missing in a healthy cluster", id, state)
		}
	}
}

// 13b. Local node with FQDN NodeID: healthy three-node membership.
func TestHealthyThreeNodeMembershipFQDNNodeID(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{
			"gossipgrid-0.gossipgrid-headless.default.svc.cluster.local.",
			"gossipgrid-1.gossipgrid-headless.default.svc.cluster.local.",
			"gossipgrid-2.gossipgrid-headless.default.svc.cluster.local.",
		},
		ports: []int{8443, 8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0.gossipgrid-headless.default.svc.cluster.local", // FQDN
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	if snap.LocalNode == nil {
		t.Fatal("LocalNode should be set for FQDN NodeID")
	}
	if snap.LocalNode.PodIdentity != "gossipgrid-0" {
		t.Fatalf("LocalNode.PodIdentity = %q, want %q", snap.LocalNode.PodIdentity, "gossipgrid-0")
	}

	expected, unexpected := Reconcile(snap)
	if expected["gossipgrid-0"] != MemberLocal {
		t.Errorf("gossipgrid-0 state = %q, want %q (local)", expected["gossipgrid-0"], MemberLocal)
	}
	if expected["gossipgrid-1"] != MemberFound {
		t.Errorf("gossipgrid-1 state = %q, want %q", expected["gossipgrid-1"], MemberFound)
	}
	if expected["gossipgrid-2"] != MemberFound {
		t.Errorf("gossipgrid-2 state = %q, want %q", expected["gossipgrid-2"], MemberFound)
	}
	if len(unexpected) != 0 {
		t.Errorf("unexpected peers: %v, want none", unexpected)
	}
}

// 13c. Local node identity cannot be established (arbitrary NodeID).
// When the NodeID does not match the StatefulSet prefix, LocalNode is nil
// and the expected replica is reported as MemberMissing (not MemberLocal).
func TestLocalNodeNotEstablished(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		ports: []int{8443, 8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         time.Hour,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "my-laptop", // arbitrary hostname, not a StatefulSet pod
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer m.Shutdown()

	waitForInitialDiscovery(t, m)

	snap := m.Snapshot()
	// LocalNode must be nil when the NodeID cannot be identified as a pod.
	if snap.LocalNode != nil {
		t.Fatalf("LocalNode should be nil for arbitrary NodeID, got %+v", snap.LocalNode)
	}

	expected, _ := Reconcile(snap)
	// Since the local node identity cannot be established, gossipgrid-0 is
	// discovered as a regular peer (not excluded) and reported as MemberFound.
	// All three are found (no self-exclusion occurred).
	if expected["gossipgrid-0"] != MemberFound {
		t.Errorf("gossipgrid-0 state = %q, want %q (no self-exclusion, found via DNS)", expected["gossipgrid-0"], MemberFound)
	}
	if expected["gossipgrid-1"] != MemberFound {
		t.Errorf("gossipgrid-1 state = %q, want %q", expected["gossipgrid-1"], MemberFound)
	}
	if expected["gossipgrid-2"] != MemberFound {
		t.Errorf("gossipgrid-2 state = %q, want %q", expected["gossipgrid-2"], MemberFound)
	}
}

// 14. Shutdown cancels an in-flight DNS lookup.
//
// Tests two scenarios:
//  1. Shutdown during the initial DNS lookup (called before Start returns).
//  2. Shutdown during a periodic DNS refresh.
//
// Both use a blocking resolver that waits on ctx.Done() and signal channels
// to synchronize with the test — no arbitrary sleeps for determining whether
// the lookup is in flight.
func TestShutdownCancelsInFlightLookup(t *testing.T) {
	t.Run("initial lookup", func(t *testing.T) {
		// entered signals when the resolver is called (initial discovery).
		// cancelled signals when the resolver's context is cancelled.
		entered := make(chan struct{}, 1)
		cancelled := make(chan struct{})

		blockingLookup := func(ctx context.Context, service, proto, name string) ([]string, []int, error) {
			entered <- struct{}{} // non-blocking (buffered)
			<-ctx.Done()
			close(cancelled)
			return nil, nil, ctx.Err()
		}

		cfg := Config{
			Mode:             ModeDNS,
			DNSName:          "gossipgrid-headless.default.svc",
			SRVService:       "peer",
			Interval:         time.Hour, // long so no periodic refresh interferes
			Timeout:          10 * time.Second,
			ExpectedReplicas: 2,
			StatefulSetName:  "gossipgrid",
			NodeID:           "gossipgrid-0",
		}
		m := NewManager(cfg, testLogger(), blockingLookup)

		ctx := context.Background() // NOT cancellable — Shutdown must cancel the lookup.

		// Start the manager; the initial discovery runs in a goroutine and
		// calls the blocking lookup, which blocks on ctx.Done().
		startDone := make(chan error)
		go func() { startDone <- m.Start(ctx) }()

		// Wait for the resolver to actually be entered (lookup is in flight).
		select {
		case <-entered:
			// The lookup is running.
		case <-time.After(2 * time.Second):
			t.Fatal("initial lookup was not entered within 2s")
		}

		// Call Shutdown WITHOUT cancelling the original context.
		// Shutdown must cancel the derived context, which unblocks the lookup.
		// This is called before Start has returned (Start is blocked on the
		// lookup goroutine, but since the lookup is in a goroutine, Start
		// should have already returned by now — the key point is that the
		// lookup is in flight).
		shutdownDone := make(chan struct{})
		go func() {
			m.Shutdown()
			close(shutdownDone)
		}()
		select {
		case <-shutdownDone:
			// Shutdown completed.
		case <-time.After(2 * time.Second):
			t.Fatal("Shutdown did not return within 2s; in-flight initial lookup not cancelled")
		}

		// Verify the lookup was actually cancelled.
		select {
		case <-cancelled:
			// The lookup was cancelled.
		case <-time.After(2 * time.Second):
			t.Fatal("in-flight initial lookup was not cancelled by Shutdown")
		}

		// Wait for Start to complete (it should complete promptly now that
		// the lookup returned).
		select {
		case err := <-startDone:
			if err != nil {
				t.Errorf("Start returned error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Start did not return within 2s")
		}
	})

	t.Run("periodic refresh", func(t *testing.T) {
		// The first call (initial discovery) succeeds immediately.
		// Subsequent calls (periodic refresh) block until context cancellation.
		// This verifies that:
		//  1. The periodic refresh loop only begins after the initial pass
		//     completes (serialized passes).
		//  2. Shutdown cancels an in-flight periodic lookup.
		entered := make(chan struct{}, 4)
		cancelled := make(chan struct{})
		var cancelOnce sync.Once
		var callMu sync.Mutex
		callCount := 0

		blockingLookup := func(ctx context.Context, service, proto, name string) ([]string, []int, error) {
			callMu.Lock()
			callCount++
			isFirst := callCount == 1
			callMu.Unlock()

			select {
			case entered <- struct{}{}:
			default:
			}

			if isFirst {
				// Initial discovery: succeed immediately.
				return []string{"gossipgrid-0", "gossipgrid-1"}, []int{8443, 8443}, nil
			}
			// Periodic refresh: block until context is cancelled.
			<-ctx.Done()
			cancelOnce.Do(func() { close(cancelled) })
			return nil, nil, ctx.Err()
		}

		cfg := Config{
			Mode:             ModeDNS,
			DNSName:          "gossipgrid-headless.default.svc",
			SRVService:       "peer",
			Interval:         5 * time.Millisecond, // fast so periodic refresh enters the resolver quickly
			Timeout:          10 * time.Second,
			ExpectedReplicas: 2,
			StatefulSetName:  "gossipgrid",
			NodeID:           "gossipgrid-0",
		}
		m := NewManager(cfg, testLogger(), blockingLookup)

		ctx := context.Background() // NOT cancellable — Shutdown must cancel the lookup.
		if err := m.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Wait for the initial discovery to complete (first entry).
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("initial lookup was not entered within 2s")
		}

		// Wait for a periodic refresh to enter the resolver (second entry).
		// This confirms the periodic loop has begun (only possible after the
		// initial pass completed).
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("periodic refresh was not entered within 2s")
		}

		// Call Shutdown WITHOUT cancelling the original context.
		shutdownDone := make(chan struct{})
		go func() {
			m.Shutdown()
			close(shutdownDone)
		}()
		select {
		case <-shutdownDone:
			// Shutdown completed.
		case <-time.After(2 * time.Second):
			t.Fatal("Shutdown did not return within 2s; in-flight periodic lookup not cancelled")
		}

		// Verify the periodic lookup was actually cancelled.
		select {
		case <-cancelled:
			// The lookup was cancelled.
		case <-time.After(2 * time.Second):
			t.Fatal("in-flight periodic lookup was not cancelled by Shutdown")
		}
	})

	t.Run("blocking initial prevents periodic overlap", func(t *testing.T) {
		// Verify that when the initial lookup blocks, no periodic refresh
		// starts (serialized passes: periodic only begins after initial
		// completes). This is the core guarantee of Defect 1.
		entered := make(chan struct{}, 2)
		calls := 0
		var callMu sync.Mutex

		blockingLookup := func(ctx context.Context, service, proto, name string) ([]string, []int, error) {
			callMu.Lock()
			calls++
			callMu.Unlock()
			select {
			case entered <- struct{}{}:
			default:
			}
			// Block indefinitely (until context cancellation by Shutdown).
			<-ctx.Done()
			return nil, nil, ctx.Err()
		}

		cfg := Config{
			Mode:             ModeDNS,
			DNSName:          "gossipgrid-headless.default.svc",
			SRVService:       "peer",
			Interval:         5 * time.Millisecond,
			Timeout:          10 * time.Second,
			ExpectedReplicas: 2,
			StatefulSetName:  "gossipgrid",
			NodeID:           "gossipgrid-0",
		}
		m := NewManager(cfg, testLogger(), blockingLookup)

		ctx := context.Background()
		if err := m.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Wait for the initial lookup to enter (blocking).
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("initial lookup was not entered within 2s")
		}

		// Wait longer than one discovery interval. No second entry should
		// occur because the periodic loop cannot start until the initial
		// pass completes.
		select {
		case <-entered:
			t.Fatal("periodic refresh started while initial lookup is still in progress (overlap detected)")
		case <-time.After(100 * time.Millisecond):
			// Correct: no periodic refresh started.
		}

		// Shutdown must cancel the in-flight initial lookup.
		shutdownDone := make(chan struct{})
		go func() {
			m.Shutdown()
			close(shutdownDone)
		}()
		select {
		case <-shutdownDone:
		case <-time.After(2 * time.Second):
			t.Fatal("Shutdown did not return within 2s")
		}

		callMu.Lock()
		defer callMu.Unlock()
		if calls != 1 {
			t.Errorf("expected exactly 1 lookup call, got %d", calls)
		}
	})
}

// 14b. Parent context cancellation still stops the refresh loop.
// Verify that cancelling the external Start context (without calling Shutdown)
// still stops the manager, and that a subsequent Shutdown returns promptly.
func TestParentContextCancellationStopsLoop(t *testing.T) {
	f := &fakeLookup{
		hosts: []string{"gossipgrid-0", "gossipgrid-1"},
		ports: []int{8443, 8443},
	}
	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         5 * time.Millisecond,
		Timeout:          2 * time.Second,
		ExpectedReplicas: 2,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), f.call)

	ctx, cancel := context.WithCancel(context.Background())
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Let a few refresh cycles run.
	time.Sleep(30 * time.Millisecond)

	// Cancel the parent context. The refresh loop should stop.
	cancel()

	// Shutdown should return promptly (the loop has stopped or is stopping).
	done := make(chan struct{})
	go func() {
		m.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after parent context cancellation")
	}
}

// 15. Deterministic regression: DNS discovery passes are serialized.
// A slow initial lookup that finishes after a newer periodic lookup must not
// overwrite newer membership with older data. The design guarantees this by
// ensuring the periodic loop only begins after the initial pass completes.
func TestDNSSerializationRegression(t *testing.T) {
	// gate blocks the initial lookup until released.
	// entered signals each lookup call.
	gate := make(chan struct{})
	entered := make(chan struct{}, 4)
	var callMu sync.Mutex
	callCount := 0
	// results returned by each call (set dynamically).
	var resultMu sync.Mutex
	var results []struct {
		hosts []string
		ports []int
	}

	blockingLookup := func(ctx context.Context, service, proto, name string) ([]string, []int, error) {
		callMu.Lock()
		callCount++
		n := callCount
		callMu.Unlock()

		entered <- struct{}{}

		switch n {
		case 1:
			// Initial lookup: block until gate is released.
			select {
			case <-gate:
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
			resultMu.Lock()
			r := results[0]
			resultMu.Unlock()
			return r.hosts, r.ports, nil
		default:
			// Periodic lookups: return immediately with current results.
			resultMu.Lock()
			r := results[callCount-1]
			resultMu.Unlock()
			return r.hosts, r.ports, nil
		}
	}

	cfg := Config{
		Mode:             ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         20 * time.Millisecond,
		Timeout:          10 * time.Second,
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), blockingLookup)

	// Initial result: only gossipgrid-0 (self) -> 0 peers after self-exclusion.
	// Periodic result: gossipgrid-0 + gossipgrid-1 + gossipgrid-2 -> 2 peers.
	resultMu.Lock()
	results = []struct {
		hosts []string
		ports []int
	}{
		{hosts: []string{"gossipgrid-0"}, ports: []int{8443}},
		{hosts: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"}, ports: []int{8443, 8443, 8443}},
		{hosts: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"}, ports: []int{8443, 8443, 8443}},
		{hosts: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"}, ports: []int{8443, 8443, 8443}},
	}
	resultMu.Unlock()

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Step 1: Wait for the initial lookup to be entered (it's now blocked).
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("initial lookup was not entered within 2s")
	}

	// Step 2: Wait longer than one discovery interval. Verify no second
	// lookup has started (periodic loop cannot begin while initial is in progress).
	select {
	case <-entered:
		t.Fatal("second lookup started while initial is still in progress (overlap)")
	case <-time.After(100 * time.Millisecond):
		// Correct: no overlap.
	}

	// Step 3: Release the initial lookup. It returns the "old" (smaller) membership.
	close(gate)

	// Wait for the initial pass to complete.
	select {
	case <-m.initDone:
	case <-time.After(2 * time.Second):
		t.Fatal("initial discovery did not complete after gate release")
	}

	// Step 4: Wait for a periodic lookup to run and its newer membership to
	// become authoritative.
	waitForCondition(t, "periodic lookup updates membership", func() bool {
		return len(m.Snapshot().Peers) == 2
	})

	snap := m.Snapshot()
	if len(snap.Peers) != 2 {
		t.Fatalf("expected 2 peers after periodic refresh, got %d: %+v", len(snap.Peers), snap.Peers)
	}
	if snap.Stale {
		t.Error("snapshot should not be stale after successful periodic refresh")
	}

	// Step 5: Verify cancellation and shutdown still work.
	done := make(chan struct{})
	go func() {
		m.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return within 2s")
	}
}

// 16. Static discovery is synchronous: Snapshot immediately after Start
// contains the configured, deduplicated, self-excluded peers. No polling
// or waiting on initDone is needed.
func TestStaticDiscoverySynchronous(t *testing.T) {
	cfg := Config{
		Mode:             ModeStatic,
		StaticPeers:      []string{"gossipgrid-1:8443", "gossipgrid-2:8443", "gossipgrid-1:8443"},
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	m := NewManager(cfg, testLogger(), nil)

	ctx := context.Background()
	if err := m.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Immediately after Start (no wait, no polling): snapshot must contain
	// the deduplicated, self-excluded peers.
	snap := m.Snapshot()
	// gossipgrid-1 appears twice in config -> deduped to 1. gossipgrid-2 once.
	// gossipgrid-0 is not in the list (self-exclusion is a no-op here).
	if len(snap.Peers) != 2 {
		t.Fatalf("expected 2 peers immediately after Start, got %d: %+v", len(snap.Peers), snap.Peers)
	}
	if snap.Stale {
		t.Error("snapshot should not be stale after successful static discovery")
	}
	for _, p := range snap.Peers {
		if p.Source != SourceStatic {
			t.Errorf("peer %s: source = %q, want %q", p.Address, p.Source, SourceStatic)
		}
	}

	// Repeated Start returns an error.
	if err := m.Start(ctx); err == nil {
		t.Fatal("second Start should return an error")
	}

	// Shutdown is safe and non-blocking.
	done := make(chan struct{})
	go func() {
		m.Shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return promptly")
	}

	// Second Shutdown is also safe.
	done2 := make(chan struct{})
	go func() {
		m.Shutdown()
		close(done2)
	}()
	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("second Shutdown did not return promptly")
	}
}
