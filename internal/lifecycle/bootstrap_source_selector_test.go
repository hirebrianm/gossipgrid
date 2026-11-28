// Package lifecycle: tests for the deterministic bootstrap-source discovery
// and selection mechanism (Milestone 5B, Stage 3B).
//
// These tests verify:
//   - candidate construction and determinism (fresh DNS, static, stale,
//     no-initial-refresh, disabled, empty, unexpected, invalid, local
//     exclusion, duplicates, stable ordering);
//   - eligibility and fallback (first eligible, first unavailable/second
//     eligible, ineligible state, mismatched epoch, READY/DEGRADED, all
//     rejected, no further observations after selection, expired
//     observation);
//   - authorization and membership changes (authority invalidated during
//     observation, epoch change, controller stopped, caller cancelled,
//     local state change, observation expiry, DNS stale, candidate removed);
//   - bounded execution (observation timeout, budget exhaustion,
//     candidate-attempt budget, caller deadline shorter than budget,
//     cancellation before/during observation);
//   - that selection alone does not create a binding or alter lifecycle
//     state.

package lifecycle

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/discovery"
)

// testSnapshotObserver is a test double for the BootstrapSourceObserver
// interface. It records the call order (dial addresses) and returns
// pre-configured results for specific candidates.
type testSnapshotObserver struct {
	mu sync.Mutex
	// results maps dialAddr to the result/err to return.
	results map[string]struct {
		result SourceObservationResult
		err    error
	}
	// calls records the actual ordered observation calls.
	calls []string
	// blockAddr is the address that blocks for the given duration (for
	// timeout tests).
	blockAddr     string
	blockDuration time.Duration
	// blockStarted is closed when the observer starts blocking (for
	// deterministic test synchronization).
	blockStarted chan struct{}
}

func (o *testSnapshotObserver) Observe(ctx context.Context, dialAddr, expectedIdentity string, expected RequestContext, localNodeID string) (SourceObservationResult, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, dialAddr)

	// Check context cancellation during observation.
	if err := ctx.Err(); err != nil {
		return SourceObservationResult{}, err
	}

	// Simulate a blocking peer (for timeout tests).
	if dialAddr == o.blockAddr {
		if o.blockStarted != nil {
			close(o.blockStarted)
		}
		select {
		case <-time.After(o.blockDuration):
		case <-ctx.Done():
			return SourceObservationResult{}, ctx.Err()
		}
	}

	entry, ok := o.results[dialAddr]
	if !ok {
		return SourceObservationResult{}, errors.New("observer: no result configured for " + dialAddr)
	}
	return entry.result, entry.err
}

func (o *testSnapshotObserver) callList() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	result := make([]string, len(o.calls))
	copy(result, o.calls)
	return result
}

// helper to build an eligible observation for a given peer.
func eligibleObs(peerNodeID, state string) SourceObservationResult {
	return NewSourceObservationResult(
		time.Now(),
		MaxSourceObservationTTL,
		RequestContext{ClusterID: testClusterID, EpochID: testEpochID},
		peerNodeID+".test.svc",
		peerNodeID,
		state,
		testClusterID,
		testEpochID,
		true,
		true,
	)
}

// testSelectorConfig returns a valid SelectorConfig for tests.
func testSelectorConfig() SelectorConfig {
	return SelectorConfig{
		IdentitySuffix:    "test.svc",
		MaxCandidates:     10,
		SelectionBudget:   5 * time.Second,
		ObservationBudget: 1 * time.Second,
	}
}

// --- Candidate construction and determinism ---

// TestSelector_FreshDNSMembership verifies that a fresh DNS snapshot with
// multiple expected peers produces the correct ordered candidate list.
func TestSelector_FreshDNSMembership(t *testing.T) {
	cfg := testSelectorConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config: %v", err)
	}

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
			"gossipgrid-2.test.svc:8443": {result: eligibleObs("gossipgrid-2", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected; got %s", result.Outcome)
	}
	// The first candidate in lexicographic order is gossipgrid-0.
	if result.Candidate.PodIdentity != "gossipgrid-0" {
		t.Fatalf("expected gossipgrid-0 first; got %s", result.Candidate.PodIdentity)
	}
}

// TestSelector_StaticMembership verifies that static discovery mode works
// (static mode is deterministic and never stale).
func TestSelector_StaticMembership(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-2", PodIdentity: "gossipgrid-2", Address: "10.0.0.2:8443"},
			{Hostname: "gossipgrid-0", PodIdentity: "gossipgrid-0", Address: "10.0.0.0:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "static",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"10.0.0.0:8443": {result: eligibleObs("gossipgrid-0", "READY")},
			"10.0.0.2:8443": {result: eligibleObs("gossipgrid-2", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected; got %s", result.Outcome)
	}
	if result.Candidate.PodIdentity != "gossipgrid-0" {
		t.Fatalf("expected gossipgrid-0 first; got %s", result.Candidate.PodIdentity)
	}
}

// TestSelector_StaleDNS returns OutcomeDiscoveryStale for a stale DNS snapshot
// even when previously discovered peers exist.
func TestSelector_StaleDNS(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              true,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap },
		newTestSourceObserver(testEpochID), ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeDiscoveryStale {
		t.Fatalf("expected OutcomeDiscoveryStale; got %s", result.Outcome)
	}
}

// TestSelector_NoInitialRefresh returns OutcomeNoInitialRefresh for a DNS
// snapshot that has never had a successful refresh.
func TestSelector_NoInitialRefresh(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers:              nil,
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Time{}, // zero: no initial refresh
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap },
		newTestSourceObserver(testEpochID), ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeNoInitialRefresh {
		t.Fatalf("expected OutcomeNoInitialRefresh; got %s", result.Outcome)
	}
}

// TestSelector_DiscoveryDisabled returns OutcomeDiscoveryUnavailable when
// discovery mode is "off".
func TestSelector_DiscoveryDisabled(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers:              nil,
		LocalNode:          nil,
		ExpectedIdentities: nil,
		Stale:              false,
		LastUpdated:        time.Time{},
		Mode:               "off",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap },
		newTestSourceObserver(testEpochID), ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeDiscoveryUnavailable {
		t.Fatalf("expected OutcomeDiscoveryUnavailable; got %s", result.Outcome)
	}
}

// TestSelector_EmptyMembership returns OutcomeNoCandidates when the membership
// is fresh but has no usable candidates (only the local replica is present).
func TestSelector_EmptyMembership(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers:              nil, // only local node, no peers
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-0", PodIdentity: "gossipgrid-0"},
		ExpectedIdentities: []string{"gossipgrid-0"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "static",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-0", finalizedMeta())
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap },
		newTestSourceObserver(testEpochID), ctrl, "gossipgrid-0")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeNoCandidates {
		t.Fatalf("expected OutcomeNoCandidates; got %s", result.Outcome)
	}
}

// TestSelector_LocalExclusion verifies that the local replica is excluded
// from the candidate list using both the snapshot's LocalNode and the
// selector's localNodeID.
func TestSelector_LocalExclusion(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-2.test.svc:8443": {result: eligibleObs("gossipgrid-2", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected; got %s", result.Outcome)
	}
	if result.Candidate.PodIdentity != "gossipgrid-2" {
		t.Fatalf("expected gossipgrid-2 (local excluded); got %s", result.Candidate.PodIdentity)
	}
}

// TestSelector_UnexpectedIdentityExcluded verifies that peers with pod
// identities not in the expected set are excluded.
func TestSelector_UnexpectedIdentityExcluded(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-99.test.svc", PodIdentity: "gossipgrid-99", Address: "gossipgrid-99.test.svc:8443"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected; got %s", result.Outcome)
	}
	if result.Candidate.PodIdentity != "gossipgrid-0" {
		t.Fatalf("expected gossipgrid-0; got %s", result.Candidate.PodIdentity)
	}
	// Only gossipgrid-0 should have been observed (gossipgrid-99 excluded).
	calls := obs.callList()
	if len(calls) != 1 || calls[0] != "gossipgrid-0.test.svc:8443" {
		t.Fatalf("expected only gossipgrid-0 to be observed; got %v", calls)
	}
}

// TestSelector_InvalidIdentityExcluded verifies that peers with empty PodIdentity
// are excluded.
func TestSelector_InvalidIdentityExcluded(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "unrelated-host", PodIdentity: "", Address: "unrelated-host:8443"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected; got %s", result.Outcome)
	}
	if result.Candidate.PodIdentity != "gossipgrid-0" {
		t.Fatalf("expected gossipgrid-0; got %s", result.Candidate.PodIdentity)
	}
}

// TestSelector_DuplicateAddresses verifies that multiple addresses for the
// same pod identity are deduplicated (only the first is kept, in address
// order). They are NOT counted as distinct replicas.
func TestSelector_DuplicateAddresses(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:9443"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected; got %s", result.Outcome)
	}
	// Only 2 candidates (gossipgrid-0 and gossipgrid-2, deduplicated).
	// The first is gossipgrid-0.
	if result.Candidate.PodIdentity != "gossipgrid-0" {
		t.Fatalf("expected gossipgrid-0; got %s", result.Candidate.PodIdentity)
	}
}

// TestSelector_StableOrdering verifies that the candidate ordering is stable
// across permutations of identical discovery data (not dependent on map
// iteration order or DNS response order).
func TestSelector_StableOrdering(t *testing.T) {
	cfg := testSelectorConfig()

	// Two snapshots with the same peers in different orders.
	snapA := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
			{Hostname: "gossipgrid-3.test.svc", PodIdentity: "gossipgrid-3", Address: "gossipgrid-3.test.svc:8443"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-4", PodIdentity: "gossipgrid-4"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3", "gossipgrid-4"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}
	snapB := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-3.test.svc", PodIdentity: "gossipgrid-3", Address: "gossipgrid-3.test.svc:8443"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-4", PodIdentity: "gossipgrid-4"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3", "gossipgrid-4"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-4", finalizedMeta())

	// Make all candidates ineligible so all are observed.
	mkObs := func() *testSnapshotObserver {
		return &testSnapshotObserver{
			results: map[string]struct {
				result SourceObservationResult
				err    error
			}{
				"gossipgrid-0.test.svc:8443": {err: errors.New("unreachable")},
				"gossipgrid-1.test.svc:8443": {err: errors.New("unreachable")},
				"gossipgrid-2.test.svc:8443": {err: errors.New("unreachable")},
				"gossipgrid-3.test.svc:8443": {err: errors.New("unreachable")},
			},
		}
	}

	selA := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snapA }, mkObs(), ctrl, "gossipgrid-4")
	selB := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snapB }, mkObs(), ctrl, "gossipgrid-4")

	// Run both; the observation call order must be identical.
	_ = selA.Select(context.Background())
	_ = selB.Select(context.Background())

	// We can't access the call list from selA/selB directly (they're separate
	// observer instances). Instead, verify by checking the rejections order.
	resultA := selA.Select(context.Background())
	resultB := selB.Select(context.Background())

	if len(resultA.Rejections) != len(resultB.Rejections) {
		t.Fatalf("rejection count mismatch: %d vs %d", len(resultA.Rejections), len(resultB.Rejections))
	}
	for i := range resultA.Rejections {
		if resultA.Rejections[i].Candidate.PodIdentity != resultB.Rejections[i].Candidate.PodIdentity {
			t.Fatalf("rejection order mismatch at %d: %s vs %s", i,
				resultA.Rejections[i].Candidate.PodIdentity, resultB.Rejections[i].Candidate.PodIdentity)
		}
	}
}

// TestSelector_SnapshotUnchanged verifies that the caller's original discovery
// snapshot is not mutated by the selection.
func TestSelector_SnapshotUnchanged(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-2", PodIdentity: "gossipgrid-2"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	// Save a copy of the snapshot before selection.
	origPeers := make([]DiscoveryPeerView, len(snap.Peers))
	copy(origPeers, snap.Peers)
	origExpected := make([]string, len(snap.ExpectedIdentities))
	copy(origExpected, snap.ExpectedIdentities)

	ctrl := newTestSelectorController(t, "gossipgrid-2", finalizedMeta())
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-2")

	_ = sel.Select(context.Background())

	// Verify the snapshot is unchanged.
	if len(snap.Peers) != len(origPeers) {
		t.Fatal("snapshot peers length changed")
	}
	for i := range origPeers {
		if snap.Peers[i] != origPeers[i] {
			t.Fatalf("snapshot peer %d changed: %v vs %v", i, snap.Peers[i], origPeers[i])
		}
	}
	if len(snap.ExpectedIdentities) != len(origExpected) {
		t.Fatal("snapshot expected identities length changed")
	}
}

// --- Eligibility and fallback ---

// TestSelector_FirstCandidateEligible verifies that the first eligible
// candidate is selected and no further observations are made.
func TestSelector_FirstCandidateEligible(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-2", PodIdentity: "gossipgrid-2"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-2", finalizedMeta())
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
			"gossipgrid-1.test.svc:8443": {result: eligibleObs("gossipgrid-1", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-2")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected; got %s", result.Outcome)
	}
	if result.Candidate.PodIdentity != "gossipgrid-0" {
		t.Fatalf("expected gossipgrid-0; got %s", result.Candidate.PodIdentity)
	}
	// Only one observation should have been made (the first candidate).
	calls := obs.callList()
	if len(calls) != 1 {
		t.Fatalf("expected 1 observation; got %d (%v)", len(calls), calls)
	}
}

// TestSelector_FirstUnreachableSecondEligible verifies that a failed first
// candidate leads to observation of the next candidate, which is then selected.
func TestSelector_FirstUnreachableSecondEligible(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-2", PodIdentity: "gossipgrid-2"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-2", finalizedMeta())
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {err: errors.New("connection refused")},
			"gossipgrid-1.test.svc:8443": {result: eligibleObs("gossipgrid-1", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-2")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected; got %s", result.Outcome)
	}
	if result.Candidate.PodIdentity != "gossipgrid-1" {
		t.Fatalf("expected gossipgrid-1; got %s", result.Candidate.PodIdentity)
	}
	// Two observations: gossipgrid-0 failed, gossipgrid-1 succeeded.
	calls := obs.callList()
	if len(calls) != 2 {
		t.Fatalf("expected 2 observations; got %d (%v)", len(calls), calls)
	}
}

// TestSelector_IneligibleStateRejects verifies that a candidate reporting an
// ineligible lifecycle state (e.g. SYNCING) is rejected.
func TestSelector_IneligibleStateRejects(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	// The observer returns an observation with an ineligible state (SYNCING).
	// The controller-side validation will reject it.
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {
				result: NewSourceObservationResult(
					time.Now(), MaxSourceObservationTTL,
					RequestContext{ClusterID: testClusterID, EpochID: testEpochID},
					"gossipgrid-0.test.svc", "gossipgrid-0", "SYNCING",
					testClusterID, testEpochID, true, true,
				),
			},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeNoEligibleSource {
		t.Fatalf("expected OutcomeNoEligibleSource; got %s", result.Outcome)
	}
	if len(result.Rejections) == 0 {
		t.Fatal("expected a rejection record")
	}
}

// TestSelector_MismatchedEpochRejects verifies that a candidate reporting a
// mismatched epoch is rejected by the controller-side validation.
func TestSelector_MismatchedEpochRejects(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	// The observer returns an observation with a different epoch.
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {
				result: NewSourceObservationResult(
					time.Now(), MaxSourceObservationTTL,
					RequestContext{ClusterID: testClusterID, EpochID: "wrong-epoch"},
					"gossipgrid-0.test.svc", "gossipgrid-0", "READY",
					testClusterID, "wrong-epoch", true, true,
				),
			},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeNoEligibleSource {
		t.Fatalf("expected OutcomeNoEligibleSource; got %s", result.Outcome)
	}
}

// TestSelector_DEGRADEDEligible verifies that a DEGRADED source is eligible
// (no preference for READY over DEGRADED in this stage).
func TestSelector_DEGRADEDEligible(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {
				result: eligibleObs("gossipgrid-0", "DEGRADED"),
			},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected (DEGRADED eligible); got %s", result.Outcome)
	}
	if result.Candidate.PodIdentity != "gossipgrid-0" {
		t.Fatalf("expected gossipgrid-0; got %s", result.Candidate.PodIdentity)
	}
}

// TestSelector_AllRejected verifies that when all candidates are rejected,
// the outcome is OutcomeNoEligibleSource.
func TestSelector_AllRejected(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-2", PodIdentity: "gossipgrid-2"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-2", finalizedMeta())
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {err: errors.New("unreachable")},
			"gossipgrid-1.test.svc:8443": {err: errors.New("unreachable")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-2")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeNoEligibleSource {
		t.Fatalf("expected OutcomeNoEligibleSource; got %s", result.Outcome)
	}
	if len(result.Rejections) != 2 {
		t.Fatalf("expected 2 rejections; got %d", len(result.Rejections))
	}
}

// TestSelector_ExpiredObservationRejects verifies that an observation that is
// expired (outside its bounded lifetime) is rejected by the controller-side
// validation.
func TestSelector_ExpiredObservationRejects(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	// The observer returns an observation that is already expired (observedAt
	// is far in the past, so now > observedAt + TTL).
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {
				result: NewSourceObservationResult(
					time.Now().Add(-2*MaxSourceObservationTTL), MaxSourceObservationTTL,
					RequestContext{ClusterID: testClusterID, EpochID: testEpochID},
					"gossipgrid-0.test.svc", "gossipgrid-0", "READY",
					testClusterID, testEpochID, true, true,
				),
			},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeNoEligibleSource {
		t.Fatalf("expected OutcomeNoEligibleSource (expired observation); got %s", result.Outcome)
	}
}

// TestSelector_NoSourceDoesNotCreateBinding verifies that a selection that
// finds no eligible source does NOT create a durable binding or alter the
// lifecycle state.
func TestSelector_NoSourceDoesNotCreateBinding(t *testing.T) {
	cfg := testSelectorConfig()

	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {err: errors.New("unreachable")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	stateBefore := ctrl.State()
	_ = sel.Select(context.Background())

	if binding.binding != nil {
		t.Fatal("selection created a binding (it must not)")
	}
	if ctrl.State() != stateBefore {
		t.Fatalf("selection changed lifecycle state: %s → %s", stateBefore, ctrl.State())
	}
}

// --- Authorization and membership changes ---

// TestSelector_AuthorityChangedDuringObservation verifies that an epoch change
// during observation produces OutcomeAuthorizationChanged.
func TestSelector_AuthorityChangedDuringObservation(t *testing.T) {
	cfg := testSelectorConfig()

	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	// The observer blocks briefly, during which we change the metadata.
	obs := &testSnapshotObserver{
		blockAddr:     "gossipgrid-0.test.svc:8443",
		blockDuration: 50 * time.Millisecond,
		blockStarted:  make(chan struct{}),
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}

	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	// Start the selection in a goroutine; wait for the observer to start
	// blocking, then change the metadata to a different epoch.
	resultCh := make(chan SelectionResult, 1)
	go func() {
		resultCh <- sel.Select(context.Background())
	}()

	// Wait for the observer to start blocking, then change the metadata.
	<-obs.blockStarted
	changedMeta := finalizedMeta()
	changedMeta.EpochID = "changed-epoch"
	ctrl.AcceptMetadataForTest(changedMeta)

	result := <-resultCh
	if result.Outcome != OutcomeAuthorizationChanged {
		t.Fatalf("expected OutcomeAuthorizationChanged; got %s", result.Outcome)
	}
}

// TestSelector_ControllerStoppedDuringObservation verifies that a controller
// stop during observation produces a non-selected outcome (the authorization
// becomes unavailable).
func TestSelector_ControllerStoppedDuringObservation(t *testing.T) {
	cfg := testSelectorConfig()

	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		blockAddr:     "gossipgrid-0.test.svc:8443",
		blockDuration: 50 * time.Millisecond,
		blockStarted:  make(chan struct{}),
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}

	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	resultCh := make(chan SelectionResult, 1)
	go func() {
		resultCh <- sel.Select(context.Background())
	}()

	<-obs.blockStarted
	ctrl.Stop(context.Background())

	result := <-resultCh
	if result.Outcome == OutcomeSelected {
		t.Fatal("expected a non-selected outcome when controller stopped during observation")
	}
}

// TestSelector_CallerCancelledDuringObservation verifies that caller
// cancellation during observation produces OutcomeCancelled.
func TestSelector_CallerCancelledDuringObservation(t *testing.T) {
	cfg := testSelectorConfig()

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		blockAddr:     "gossipgrid-0.test.svc:8443",
		blockDuration: 100 * time.Millisecond,
		blockStarted:  make(chan struct{}),
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}

	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan SelectionResult, 1)
	go func() {
		resultCh <- sel.Select(ctx)
	}()

	<-obs.blockStarted
	cancel()

	result := <-resultCh
	if result.Outcome != OutcomeCancelled {
		t.Fatalf("expected OutcomeCancelled; got %s", result.Outcome)
	}
}

// TestSelector_MembershipChangedAfterObservation verifies that a DNS snapshot
// that becomes stale after the observation produces OutcomeMembershipChanged.
func TestSelector_MembershipChangedAfterObservation(t *testing.T) {
	cfg := testSelectorConfig()

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	// Mutable snapshot: fresh during the observation, stale after.
	mu := sync.Mutex{}
	stale := false
	snapFn := func() DiscoverySnapshotView {
		mu.Lock()
		defer mu.Unlock()
		return DiscoverySnapshotView{
			Peers: []DiscoveryPeerView{
				{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			},
			LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
			ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
			Stale:              stale,
			LastUpdated:        time.Now(),
			Mode:               "dns",
		}
	}

	obs := &testSnapshotObserver{
		blockAddr:     "gossipgrid-0.test.svc:8443",
		blockDuration: 50 * time.Millisecond,
		blockStarted:  make(chan struct{}),
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}

	sel := newTestSelector(t, cfg, snapFn, obs, ctrl, "gossipgrid-1")

	resultCh := make(chan SelectionResult, 1)
	go func() {
		resultCh <- sel.Select(context.Background())
	}()

	<-obs.blockStarted
	mu.Lock()
	stale = true
	mu.Unlock()

	result := <-resultCh
	if result.Outcome != OutcomeMembershipChanged {
		t.Fatalf("expected OutcomeMembershipChanged; got %s", result.Outcome)
	}
}

// TestSelector_StateChangeDuringObservation verifies that a local state change
// to an ineligible state (e.g. RECOVERY_REQUIRED) during observation produces
// a non-selected outcome.
func TestSelector_StateChangeDuringObservation(t *testing.T) {
	cfg := testSelectorConfig()

	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		blockAddr:     "gossipgrid-0.test.svc:8443",
		blockDuration: 50 * time.Millisecond,
		blockStarted:  make(chan struct{}),
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}

	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	resultCh := make(chan SelectionResult, 1)
	go func() {
		resultCh <- sel.Select(context.Background())
	}()

	<-obs.blockStarted
	// Force the state to RECOVERY_REQUIRED (an ineligible state for source
	// selection).
	ctrl.Machine().ForceTransitionForTest(RECOVERY_REQUIRED, "test: forced recovery")

	result := <-resultCh
	if result.Outcome == OutcomeSelected {
		t.Fatal("expected a non-selected outcome when state changed to RECOVERY_REQUIRED")
	}
}

// --- Bounded execution ---

// TestSelector_CandidateObservationTimeout verifies that a peer that is
// unreachable (blocks beyond the observation budget) does not prevent the
// selection from terminating within its budget.
func TestSelector_CandidateObservationTimeout(t *testing.T) {
	cfg := testSelectorConfig()
	cfg.ObservationBudget = 50 * time.Millisecond
	cfg.SelectionBudget = 2 * time.Second

	// The controller's NodeID is "gossipgrid-2" (a different replica from
	// the peers being observed). The shared validator's self-exclusion
	// check uses c.cfg.NodeID, which must not match the observed peers.
	ctrl := newTestSelectorController(t, "gossipgrid-2", finalizedMeta())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-2", PodIdentity: "gossipgrid-2"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	// gossipgrid-0 blocks for 5s (exceeds the 50ms observation budget);
	// gossipgrid-1 is eligible.
	obs := &testSnapshotObserver{
		blockAddr:     "gossipgrid-0.test.svc:8443",
		blockDuration: 5 * time.Second,
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
			"gossipgrid-1.test.svc:8443": {result: eligibleObs("gossipgrid-1", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-2")

	start := time.Now()
	result := sel.Select(context.Background())
	elapsed := time.Since(start)

	// The selection must terminate within the selection budget (2s).
	if elapsed > 2*time.Second {
		t.Fatalf("selection took too long: %v (budget is 2s)", elapsed)
	}

	// gossipgrid-0 should have timed out; gossipgrid-1 should be selected.
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected; got %s", result.Outcome)
	}
	if result.Candidate.PodIdentity != "gossipgrid-1" {
		t.Fatalf("expected gossipgrid-1 (gossipgrid-0 timed out); got %s", result.Candidate.PodIdentity)
	}
}

// TestSelector_TotalBudgetExhaustion verifies that when the total selection
// budget is exhausted (all candidates block beyond it), the outcome is
// OutcomeBudgetExhausted or OutcomeCancelled.
func TestSelector_TotalBudgetExhaustion(t *testing.T) {
	cfg := testSelectorConfig()
	cfg.SelectionBudget = 100 * time.Millisecond
	cfg.ObservationBudget = 50 * time.Millisecond

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-2", PodIdentity: "gossipgrid-2"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	// Both candidates block for 5s (exceeds the 100ms selection budget).
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {err: errors.New("timeout")},
			"gossipgrid-1.test.svc:8443": {err: errors.New("timeout")},
		},
	}
	// Make both block.
	obs.blockAddr = "gossipgrid-0.test.svc:8443"
	obs.blockDuration = 5 * time.Second

	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-2")

	start := time.Now()
	result := sel.Select(context.Background())
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Fatalf("selection took too long: %v (budget is 100ms)", elapsed)
	}
	if result.Outcome == OutcomeSelected {
		t.Fatal("expected a non-selected outcome when budget exhausted")
	}
}

// TestSelector_CallerDeadlineShorterThanBudget verifies that a caller context
// with a deadline shorter than the configured selection budget is respected.
func TestSelector_CallerDeadlineShorterThanBudget(t *testing.T) {
	cfg := testSelectorConfig()
	cfg.SelectionBudget = 10 * time.Second // much longer than caller deadline
	cfg.ObservationBudget = 5 * time.Second

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	// The candidate blocks for 5s.
	obs := &testSnapshotObserver{
		blockAddr:     "gossipgrid-0.test.svc:8443",
		blockDuration: 5 * time.Second,
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	// Caller deadline is 100ms (shorter than the 10s selection budget).
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	result := sel.Select(ctx)
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Fatalf("selection took too long: %v (caller deadline is 100ms)", elapsed)
	}
	if result.Outcome == OutcomeSelected {
		t.Fatal("expected a non-selected outcome when caller deadline expires")
	}
}

// TestSelector_CancellationBeforeAnyObservation verifies that a pre-cancelled
// context produces OutcomeCancelled without any observations.
func TestSelector_CancellationBeforeAnyObservation(t *testing.T) {
	cfg := testSelectorConfig()

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Pre-cancel.

	result := sel.Select(ctx)
	if result.Outcome != OutcomeCancelled {
		t.Fatalf("expected OutcomeCancelled; got %s", result.Outcome)
	}
	// No observations should have been made.
	calls := obs.callList()
	if len(calls) != 0 {
		t.Fatalf("expected 0 observations; got %d", len(calls))
	}
}

// --- Selection result properties ---

// TestSelector_SelectionResultCarriesTarget verifies that a successful
// selection result carries the pinned target and the authenticated
// observation.
func TestSelector_SelectionResultCarriesTarget(t *testing.T) {
	cfg := testSelectorConfig()

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected; got %s", result.Outcome)
	}
	if result.Target.ClusterID != testClusterID || result.Target.EpochID != testEpochID {
		t.Fatalf("target mismatch: %s/%s", result.Target.ClusterID, result.Target.EpochID)
	}
	if result.Candidate == nil {
		t.Fatal("candidate is nil in a successful result")
	}
	if result.Observation == nil {
		t.Fatal("observation is nil in a successful result")
	}
	if !result.Observation.Observed() {
		t.Fatal("observation is not marked as observed")
	}
}

// --- Helper ---

// newTestSelectorController creates a controller in DISCOVERING state with
// fresh, finalized metadata, suitable for pre-join source selection tests.
func newTestSelectorController(t *testing.T, nodeID string, meta ClusterMetadata) *Controller {
	t.Helper()
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(meta, true)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               nodeID,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { ctrl.Stop(context.Background()) })
	return ctrl
}

// newTestSelector is a convenience wrapper around NewBootstrapSourceSelector
// for tests: it panics on a construction error (invalid config or nil deps),
// which is appropriate for test setup where a construction failure is a test
// bug, not a runtime condition.
func newTestSelector(t *testing.T, cfg SelectorConfig, snapFn DiscoverySnapshotFunc, obs BootstrapSourceObserver, ctrl *Controller, localNodeID string) *BootstrapSourceSelector {
	t.Helper()
	sel, err := NewBootstrapSourceSelector(cfg, snapFn, obs, ctrl, localNodeID)
	if err != nil {
		t.Fatalf("NewBootstrapSourceSelector: %v", err)
	}
	return sel
}

// --- Stage 3B Correction: Candidate normalization and deterministic ordering ---

// TestSelector_SamePodIDTwoAddresses verifies that the same PodID with two
// different addresses produces two candidates (one per distinct
// identity/address pair), ordered deterministically by address.
func TestSelector_SamePodIDTwoAddresses(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:9443"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	// Make the first address (8443) fail; the second (9443) is eligible.
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {err: errors.New("connection refused")},
			"gossipgrid-0.test.svc:9443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected (fallback to alternate address); got %s (rejections=%+v)", result.Outcome, result.Rejections)
	}
	if result.Candidate.PodIdentity != "gossipgrid-0" {
		t.Fatalf("expected gossipgrid-0; got %s", result.Candidate.PodIdentity)
	}
	if result.Candidate.DialAddr != "gossipgrid-0.test.svc:9443" {
		t.Fatalf("expected fallback to 9443; got %s", result.Candidate.DialAddr)
	}
	// Both addresses must have been observed (first failed, second succeeded).
	calls := obs.callList()
	if len(calls) != 2 {
		t.Fatalf("expected 2 observation calls (both addresses); got %d (%v)", len(calls), calls)
	}
	// The first call must be the lower address (deterministic ordering).
	if calls[0] != "gossipgrid-0.test.svc:8443" {
		t.Fatalf("expected first call to be :8443; got %s", calls[0])
	}
	if calls[1] != "gossipgrid-0.test.svc:9443" {
		t.Fatalf("expected second call to be :9443; got %s", calls[1])
	}
}

// TestSelector_ExactDuplicatePairs verifies that exact duplicate
// identity/address pairs are deduplicated (only one candidate per pair).
func TestSelector_ExactDuplicatePairs(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected; got %s", result.Outcome)
	}
	// Only one observation (the duplicate pairs are deduplicated).
	calls := obs.callList()
	if len(calls) != 1 {
		t.Fatalf("expected 1 observation (duplicates deduplicated); got %d (%v)", len(calls), calls)
	}
}

// TestSelector_PermutationsDeterministic verifies that permutations of the same
// membership (with duplicate identities) produce the same observation order.
func TestSelector_PermutationsDeterministic(t *testing.T) {
	cfg := testSelectorConfig()

	// Three permutations of the same membership with a duplicate address
	// for gossipgrid-0.
	snapA := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:9443"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-3", PodIdentity: "gossipgrid-3"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}
	snapB := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:9443"},
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-3", PodIdentity: "gossipgrid-3"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}
	snapC := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:9443"},
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-3", PodIdentity: "gossipgrid-3"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-3", finalizedMeta())

	mkObs := func() *testSnapshotObserver {
		return &testSnapshotObserver{
			results: map[string]struct {
				result SourceObservationResult
				err    error
			}{
				"gossipgrid-0.test.svc:8443": {err: errors.New("fail")},
				"gossipgrid-0.test.svc:9443": {err: errors.New("fail")},
				"gossipgrid-2.test.svc:8443": {err: errors.New("fail")},
			},
		}
	}

	obsA, obsB, obsC := mkObs(), mkObs(), mkObs()
	selA := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snapA }, obsA, ctrl, "gossipgrid-3")
	selB := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snapB }, obsB, ctrl, "gossipgrid-3")
	selC := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snapC }, obsC, ctrl, "gossipgrid-3")

	_ = selA.Select(context.Background())
	_ = selB.Select(context.Background())
	_ = selC.Select(context.Background())

	expected := []string{"gossipgrid-0.test.svc:8443", "gossipgrid-0.test.svc:9443", "gossipgrid-2.test.svc:8443"}
	for _, tc := range []struct {
		name  string
		calls []string
	}{
		{"A", obsA.callList()},
		{"B", obsB.callList()},
		{"C", obsC.callList()},
	} {
		if len(tc.calls) != len(expected) {
			t.Fatalf("permutation %s: expected %d calls; got %d (%v)", tc.name, len(expected), len(tc.calls), tc.calls)
		}
		for i, want := range expected {
			if tc.calls[i] != want {
				t.Fatalf("permutation %s: call %d expected %s; got %s", tc.name, i, want, tc.calls[i])
			}
		}
	}
}

// TestSelector_InvalidAddressesExcluded verifies that peers with invalid
// dial addresses (missing host, missing port, invalid port, empty address)
// are excluded from the candidate list.
func TestSelector_InvalidAddressesExcluded(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: ""},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:0"},
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:99999"},
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-2", PodIdentity: "gossipgrid-2"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-2", finalizedMeta())
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-1.test.svc:8443": {result: eligibleObs("gossipgrid-1", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-2")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected (only valid address); got %s (rejections=%+v)", result.Outcome, result.Rejections)
	}
	if result.Candidate.PodIdentity != "gossipgrid-1" {
		t.Fatalf("expected gossipgrid-1 (all gossipgrid-0 addresses invalid); got %s", result.Candidate.PodIdentity)
	}
	// Only one observation (gossipgrid-1).
	calls := obs.callList()
	if len(calls) != 1 {
		t.Fatalf("expected 1 observation; got %d (%v)", len(calls), calls)
	}
}

// TestSelector_UnknownDiscoveryMode verifies that an unknown discovery mode
// is rejected (not accepted as usable discovery).
func TestSelector_UnknownDiscoveryMode(t *testing.T) {
	cfg := testSelectorConfig()

	snap := DiscoverySnapshotView{
		Peers:              []DiscoveryPeerView{{Hostname: "gossipgrid-0", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0:8443"}},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "carrier-pigeon",
	}

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, newTestSourceObserver(testEpochID), ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeDiscoveryUnavailable {
		t.Fatalf("expected OutcomeDiscoveryUnavailable (unknown mode); got %s", result.Outcome)
	}
}

// --- Stage 3B Correction: Membership revalidation ---

// TestSelector_SelectedAddressDisappears verifies that when the selected
// candidate's specific address disappears from the membership (but the PodID
// remains expected), the outcome is OutcomeMembershipChanged.
func TestSelector_SelectedAddressDisappears(t *testing.T) {
	cfg := testSelectorConfig()

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	// Post-observation snapshot: gossipgrid-0's address is gone.
	postSnap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}
	// Pre-observation snapshot: gossipgrid-0 is present.
	preSnap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	barrier := &revalidationBarrier{
		inner:    obs,
		preSnap:  preSnap,
		postSnap: postSnap,
		obsDone:  make(chan struct{}),
		flipCh:   make(chan struct{}),
	}

	sel := newTestSelector(t, cfg, barrier.snapshot, barrier, ctrl, "gossipgrid-1")

	resultCh := make(chan SelectionResult, 1)
	go func() {
		resultCh <- sel.Select(context.Background())
	}()

	// Wait for the observation to complete, then flip the snapshot.
	<-barrier.obsDone
	barrier.flip()

	result := <-resultCh
	if result.Outcome != OutcomeMembershipChanged {
		t.Fatalf("expected OutcomeMembershipChanged (address disappeared); got %s (rejections=%+v)", result.Outcome, result.Rejections)
	}
}

// TestSelector_SelectedPodIDAtDifferentAddress verifies that when the selected
// PodID remains present but at a different address, the specific
// identity/address pair is invalidated (OutcomeMembershipChanged).
func TestSelector_SelectedPodIDAtDifferentAddress(t *testing.T) {
	cfg := testSelectorConfig()

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	postSnap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:9999"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}
	preSnap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	barrier := &revalidationBarrier{
		inner:    obs,
		preSnap:  preSnap,
		postSnap: postSnap,
		obsDone:  make(chan struct{}),
		flipCh:   make(chan struct{}),
	}

	sel := newTestSelector(t, cfg, barrier.snapshot, barrier, ctrl, "gossipgrid-1")

	resultCh := make(chan SelectionResult, 1)
	go func() {
		resultCh <- sel.Select(context.Background())
	}()

	<-barrier.obsDone
	barrier.flip()

	result := <-resultCh
	if result.Outcome != OutcomeMembershipChanged {
		t.Fatalf("expected OutcomeMembershipChanged (address changed); got %s", result.Outcome)
	}
}

// TestSelector_SelectedIdentityRemovedFromExpected verifies that when the
// selected identity is removed from the expected membership, the outcome is
// OutcomeMembershipChanged.
func TestSelector_SelectedIdentityRemovedFromExpected(t *testing.T) {
	cfg := testSelectorConfig()

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	postSnap := DiscoverySnapshotView{
		Peers:              []DiscoveryPeerView{{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"}},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-1"}, // gossipgrid-0 removed
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}
	preSnap := DiscoverySnapshotView{
		Peers:              []DiscoveryPeerView{{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"}},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	barrier := &revalidationBarrier{
		inner:    obs,
		preSnap:  preSnap,
		postSnap: postSnap,
		obsDone:  make(chan struct{}),
		flipCh:   make(chan struct{}),
	}

	sel := newTestSelector(t, cfg, barrier.snapshot, barrier, ctrl, "gossipgrid-1")

	resultCh := make(chan SelectionResult, 1)
	go func() {
		resultCh <- sel.Select(context.Background())
	}()

	<-barrier.obsDone
	barrier.flip()

	result := <-resultCh
	if result.Outcome != OutcomeMembershipChanged {
		t.Fatalf("expected OutcomeMembershipChanged (identity removed); got %s", result.Outcome)
	}
}

// TestSelector_DiscoveryStaleAfterObservation verifies that when discovery
// becomes stale after the observation, the outcome is OutcomeMembershipChanged.
func TestSelector_DiscoveryStaleAfterObservation(t *testing.T) {
	cfg := testSelectorConfig()

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	preSnap := DiscoverySnapshotView{
		Peers:              []DiscoveryPeerView{{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"}},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}
	postSnap := DiscoverySnapshotView{
		Peers:              []DiscoveryPeerView{{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"}},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              true,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	barrier := &revalidationBarrier{
		inner:    obs,
		preSnap:  preSnap,
		postSnap: postSnap,
		obsDone:  make(chan struct{}),
		flipCh:   make(chan struct{}),
	}

	sel := newTestSelector(t, cfg, barrier.snapshot, barrier, ctrl, "gossipgrid-1")

	resultCh := make(chan SelectionResult, 1)
	go func() {
		resultCh <- sel.Select(context.Background())
	}()

	<-barrier.obsDone
	barrier.flip()

	result := <-resultCh
	if result.Outcome != OutcomeMembershipChanged {
		t.Fatalf("expected OutcomeMembershipChanged (discovery stale); got %s", result.Outcome)
	}
}

// TestSelector_DiscoveryDisabledAfterObservation verifies that when discovery
// becomes disabled after the observation, the outcome is OutcomeMembershipChanged.
func TestSelector_DiscoveryDisabledAfterObservation(t *testing.T) {
	cfg := testSelectorConfig()

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	preSnap := DiscoverySnapshotView{
		Peers:              []DiscoveryPeerView{{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"}},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}
	postSnap := DiscoverySnapshotView{
		Peers:              nil,
		LocalNode:          nil,
		ExpectedIdentities: nil,
		Stale:              false,
		LastUpdated:        time.Time{},
		Mode:               "off",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	barrier := &revalidationBarrier{
		inner:    obs,
		preSnap:  preSnap,
		postSnap: postSnap,
		obsDone:  make(chan struct{}),
		flipCh:   make(chan struct{}),
	}

	sel := newTestSelector(t, cfg, barrier.snapshot, barrier, ctrl, "gossipgrid-1")

	resultCh := make(chan SelectionResult, 1)
	go func() {
		resultCh <- sel.Select(context.Background())
	}()

	<-barrier.obsDone
	barrier.flip()

	result := <-resultCh
	if result.Outcome != OutcomeMembershipChanged {
		t.Fatalf("expected OutcomeMembershipChanged (discovery disabled); got %s", result.Outcome)
	}
}

// TestSelector_HarmlessRefreshPreservesCandidate verifies that a routine
// DNS refresh that preserves the candidate (same peers, same addresses)
// does NOT invalidate the selection result solely because LastUpdated changed.
func TestSelector_HarmlessRefreshPreservesCandidate(t *testing.T) {
	cfg := testSelectorConfig()

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	// The snapshot is the same after observation (same peers, same addresses),
	// but with a slightly later LastUpdated (simulating a routine refresh).
	preSnap := DiscoverySnapshotView{
		Peers:              []DiscoveryPeerView{{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"}},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}
	postSnap := DiscoverySnapshotView{
		Peers:              []DiscoveryPeerView{{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"}},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now().Add(1 * time.Second),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	barrier := &revalidationBarrier{
		inner:    obs,
		preSnap:  preSnap,
		postSnap: postSnap,
		obsDone:  make(chan struct{}),
		flipCh:   make(chan struct{}),
	}

	sel := newTestSelector(t, cfg, barrier.snapshot, barrier, ctrl, "gossipgrid-1")

	resultCh := make(chan SelectionResult, 1)
	go func() {
		resultCh <- sel.Select(context.Background())
	}()

	<-barrier.obsDone
	barrier.flip()

	result := <-resultCh
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected (harmless refresh preserves candidate); got %s (rejections=%+v)", result.Outcome, result.Rejections)
	}
}

// revalidationBarrier is a test helper for deterministic membership
// revalidation tests. It provides a two-phase barrier:
//
//  1. The first snapshot call (initial read) returns the pre-observation
//     snapshot immediately (no blocking).
//  2. The inner observer completes; the wrapper signals obsDone.
//  3. The test flips the flag and calls flip().
//  4. The second snapshot call (revalidation) blocks until flip() is called,
//     then returns the post-observation snapshot.
//
// This guarantees the revalidation reads the post-observation snapshot
// deterministically, without arbitrary sleeps.
type revalidationBarrier struct {
	inner     BootstrapSourceObserver
	preSnap   DiscoverySnapshotView
	postSnap  DiscoverySnapshotView
	obsDone   chan struct{}
	flipCh    chan struct{}
	obsOnce   sync.Once
	flipOnce  sync.Once
	mu        sync.Mutex
	callCount int
}

func (b *revalidationBarrier) Observe(ctx context.Context, dialAddr, expectedIdentity string, expected RequestContext, localNodeID string) (SourceObservationResult, error) {
	res, err := b.inner.Observe(ctx, dialAddr, expectedIdentity, expected, localNodeID)
	b.obsOnce.Do(func() { close(b.obsDone) })
	return res, err
}

func (b *revalidationBarrier) snapshot() DiscoverySnapshotView {
	b.mu.Lock()
	b.callCount++
	firstCall := b.callCount == 1
	b.mu.Unlock()
	if firstCall {
		return b.preSnap
	}
	// Second and subsequent calls (revalidation): block until flip().
	<-b.flipCh
	return b.postSnap
}

func (b *revalidationBarrier) flip() {
	b.flipOnce.Do(func() { close(b.flipCh) })
}

// --- Stage 3B Correction: Exact budget and cancellation outcomes ---

// TestSelector_CallerDeadlineExhausted verifies that when the caller's deadline
// is shorter than the selection budget and expires, the outcome is
// OutcomeCancelled (not OutcomeBudgetExhausted).
func TestSelector_CallerDeadlineExhausted(t *testing.T) {
	cfg := testSelectorConfig()
	cfg.SelectionBudget = 10 * time.Second
	cfg.ObservationBudget = 5 * time.Second

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		blockAddr:     "gossipgrid-0.test.svc:8443",
		blockDuration: 5 * time.Second,
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	// Caller deadline is 50ms (much shorter than the 10s selection budget).
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	result := sel.Select(ctx)
	if result.Outcome != OutcomeCancelled {
		t.Fatalf("expected OutcomeCancelled (caller deadline); got %s", result.Outcome)
	}
}

// TestSelector_CandidateAttemptBudget verifies that when the candidate-attempt
// limit (MaxCandidates) is reached while additional usable candidates remain,
// the outcome is OutcomeBudgetExhausted. This is the execution-budget
// (attempt-limit) exhaustion, distinct from a wall-clock deadline expiry.
func TestSelector_CandidateAttemptBudget(t *testing.T) {
	cfg := testSelectorConfig()
	cfg.MaxCandidates = 2
	cfg.SelectionBudget = 10 * time.Second
	cfg.ObservationBudget = 5 * time.Second

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
			{Hostname: "gossipgrid-3.test.svc", PodIdentity: "gossipgrid-3", Address: "gossipgrid-3.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-4", PodIdentity: "gossipgrid-4"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3", "gossipgrid-4"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	// All candidates are rejected (unreachable). With MaxCandidates=2, only
	// 2 of 4 are attempted; the remaining 2 make this budget-exhausted.
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {err: errors.New("unreachable")},
			"gossipgrid-1.test.svc:8443": {err: errors.New("unreachable")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-4")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeBudgetExhausted {
		t.Fatalf("expected OutcomeBudgetExhausted (attempt limit, candidates remain); got %s (rejections=%+v)", result.Outcome, result.Rejections)
	}
}

// TestSelector_CandidateAttemptBudgetExhausted verifies that when the
// candidate-attempt limit is reached while additional usable candidates remain,
// the outcome is OutcomeBudgetExhausted.
func TestSelector_CandidateAttemptBudgetExhausted(t *testing.T) {
	cfg := testSelectorConfig()
	cfg.MaxCandidates = 1 // only 1 attempt allowed

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	// Three usable candidates, but MaxCandidates=1 limits to 1 attempt.
	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-3", PodIdentity: "gossipgrid-3"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2", "gossipgrid-3"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	// The first candidate is rejected; additional candidates remain unobserved.
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {err: errors.New("unreachable")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-3")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeBudgetExhausted {
		t.Fatalf("expected OutcomeBudgetExhausted (attempt limit, candidates remain); got %s", result.Outcome)
	}
	if len(result.Rejections) != 1 {
		t.Fatalf("expected 1 rejection (only 1 attempt made); got %d", len(result.Rejections))
	}
}

// TestSelector_AllCandidatesGenuinelyExhausted verifies that when all usable
// candidates are attempted and rejected, the outcome is OutcomeNoEligibleSource.
func TestSelector_AllCandidatesGenuinelyExhausted(t *testing.T) {
	cfg := testSelectorConfig()
	cfg.MaxCandidates = 10 // generous budget

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-1.test.svc", PodIdentity: "gossipgrid-1", Address: "gossipgrid-1.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-2", PodIdentity: "gossipgrid-2"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {err: errors.New("unreachable")},
			"gossipgrid-1.test.svc:8443": {err: errors.New("unreachable")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-2")

	result := sel.Select(context.Background())
	if result.Outcome != OutcomeNoEligibleSource {
		t.Fatalf("expected OutcomeNoEligibleSource (all exhausted); got %s", result.Outcome)
	}
	if len(result.Rejections) != 2 {
		t.Fatalf("expected 2 rejections; got %d", len(result.Rejections))
	}
}

// TestSelector_ObserverReturnsAfterCancellation verifies that when the caller
// cancels during observation and the observer returns a nominally valid result
// after the cancellation, the outcome is OutcomeCancelled (not OutcomeSelected).
func TestSelector_ObserverReturnsAfterCancellation(t *testing.T) {
	cfg := testSelectorConfig()

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	// The observer blocks, then returns a valid result even after ctx is done.
	obsCalled := make(chan struct{})

	// Custom observer that blocks until cancelled, then returns a valid result.
	lateObs := &lateObserver{
		blockDone: obsCalled,
		result:    eligibleObs("gossipgrid-0", "READY"),
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, lateObs, ctrl, "gossipgrid-1")

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan SelectionResult, 1)
	go func() {
		resultCh <- sel.Select(ctx)
	}()

	// Wait for the observer to start blocking, then cancel.
	<-obsCalled
	cancel()

	result := <-resultCh
	if result.Outcome != OutcomeCancelled {
		t.Fatalf("expected OutcomeCancelled (observer returned after cancel); got %s", result.Outcome)
	}
}

// lateObserver blocks until its context is cancelled, then returns a
// nominally valid result (to test that a late return after cancellation is
// not accepted as a successful selection).
type lateObserver struct {
	blockDone chan struct{}
	result    SourceObservationResult
	done      sync.Once
}

func (o *lateObserver) Observe(ctx context.Context, _, _ string, _ RequestContext, _ string) (SourceObservationResult, error) {
	o.done.Do(func() { close(o.blockDone) })
	// Block until context is done, then return the result.
	<-ctx.Done()
	return o.result, nil
}

// blockingObserver blocks on a caller-controlled channel before returning
// a result. It is used for deterministic interleaving tests (e.g., advancing
// a controlled clock while the observer is blocked). The called channel is
// closed exactly once when Observe is invoked (the first invocation only;
// subsequent invocations do not re-close).
type blockingObserver struct {
	blockCh chan struct{}
	called  chan struct{}
	result  SourceObservationResult
	done    sync.Once
}

// immediatelyValidObserver returns a valid, immediately-successful
// observation for every invocation. It is used with a snapshot function that
// changes between calls, so that the first snapshot yields a valid candidate
// observation and the second (final-membership-validation) snapshot yields a
// membership change.
type immediatelyValidObserver struct {
	result SourceObservationResult
}

func (o *immediatelyValidObserver) Observe(_ context.Context, _, _ string, _ RequestContext, _ string) (SourceObservationResult, error) {
	return o.result, nil
}

func (o *blockingObserver) Observe(ctx context.Context, _, _ string, _ RequestContext, _ string) (SourceObservationResult, error) {
	o.done.Do(func() { close(o.called) })
	// Block until unblocked by the test (or the context is cancelled).
	select {
	case <-o.blockCh:
		// Test unblocked the observer; return the result.
		return o.result, nil
	case <-ctx.Done():
		// Context cancelled (e.g., observation budget or selection budget
		// expired); return the result (a late result; the caller rejects
		// it via the obsCtx check).
		return o.result, nil
	}
}

// --- Stage 3B Correction: Production discovery adapter ---

// TestDiscoveryAdapter_StaticMode verifies that NewDiscoverySnapshotFunc
// correctly maps a real discovery.Manager.Snapshot() (static mode) into the
// selector's DiscoverySnapshotView.
func TestDiscoveryAdapter_StaticMode(t *testing.T) {
	cfg := discovery.Config{
		Mode:             discovery.ModeStatic,
		StaticPeers:      []string{"gossipgrid-0:8443", "gossipgrid-2:8443"},
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-1",
	}
	mgr := discovery.NewManager(cfg, nil, nil)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("manager Start: %v", err)
	}
	defer mgr.Shutdown()

	snapFn := NewDiscoverySnapshotFunc(mgr)
	view := snapFn()

	// Verify the mapping: peers, local node, expected identities, mode.
	if view.Mode != "static" {
		t.Fatalf("expected mode static; got %s", view.Mode)
	}
	if len(view.Peers) != 2 {
		t.Fatalf("expected 2 peers (self-excluded); got %d", len(view.Peers))
	}
	if view.LocalNode == nil {
		t.Fatal("expected local node to be set")
	}
	if view.LocalNode.PodIdentity != "gossipgrid-1" {
		t.Fatalf("expected local node gossipgrid-1; got %s", view.LocalNode.PodIdentity)
	}
	if len(view.ExpectedIdentities) != 3 {
		t.Fatalf("expected 3 expected identities; got %d", len(view.ExpectedIdentities))
	}
	if !view.LastUpdated.After(time.Time{}) {
		t.Fatal("expected LastUpdated to be set (static mode is immediate)")
	}
	if view.Stale {
		t.Fatal("expected non-stale (static mode)")
	}

	// Verify peer mapping: PodIdentity and Address are correctly mapped.
	for _, p := range view.Peers {
		if p.PodIdentity == "" {
			t.Fatalf("peer has empty PodIdentity: %+v", p)
		}
		if p.Address == "" {
			t.Fatalf("peer has empty Address: %+v", p)
		}
	}
}

// TestDiscoveryAdapter_DNSMode verifies that NewDiscoverySnapshotFunc
// correctly maps a real discovery.Manager.Snapshot() (DNS mode with a
// controlled lookup function) into the selector's DiscoverySnapshotView.
func TestDiscoveryAdapter_DNSMode(t *testing.T) {
	// A controlled DNS lookup that returns two SRV records.
	lookupFn := func(_ context.Context, _, _ string, _ string) ([]string, []int, error) {
		return []string{
			"gossipgrid-0.gossipgrid-headless.default.svc",
			"gossipgrid-2.gossipgrid-headless.default.svc",
		}, []int{8443, 8443}, nil
	}

	cfg := discovery.Config{
		Mode:             discovery.ModeDNS,
		DNSName:          "gossipgrid-headless.default.svc",
		SRVService:       "peer",
		Interval:         30 * time.Second,
		Timeout:          5 * time.Second,
		ExpectedReplicas: 3,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-1",
	}
	mgr := discovery.NewManager(cfg, nil, lookupFn)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("manager Start: %v", err)
	}
	defer mgr.Shutdown()

	// Wait for the initial DNS refresh to complete.
	deadline := time.After(5 * time.Second)
	for {
		snap := mgr.Snapshot()
		if !snap.LastUpdated.IsZero() {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for initial DNS refresh")
		case <-time.After(10 * time.Millisecond):
		}
	}

	snapFn := NewDiscoverySnapshotFunc(mgr)
	view := snapFn()

	if view.Mode != "dns" {
		t.Fatalf("expected mode dns; got %s", view.Mode)
	}
	// Two peers (gossipgrid-0 and gossipgrid-2; self gossipgrid-1 excluded).
	if len(view.Peers) != 2 {
		t.Fatalf("expected 2 peers; got %d (%+v)", len(view.Peers), view.Peers)
	}
	if view.LocalNode == nil || view.LocalNode.PodIdentity != "gossipgrid-1" {
		t.Fatalf("expected local node gossipgrid-1; got %+v", view.LocalNode)
	}
	if len(view.ExpectedIdentities) != 3 {
		t.Fatalf("expected 3 expected identities; got %d", len(view.ExpectedIdentities))
	}
	// Verify peer mapping: PodIdentity is derived from the FQDN.
	for _, p := range view.Peers {
		if p.PodIdentity == "" {
			t.Fatalf("peer has empty PodIdentity: %+v", p)
		}
		// The Address should be the FQDN:port.
		if p.Address == "" {
			t.Fatalf("peer has empty Address: %+v", p)
		}
	}
}

// TestDiscoveryAdapter_OffMode verifies that NewDiscoverySnapshotFunc
// correctly maps a discovery.Manager in off mode.
func TestDiscoveryAdapter_OffMode(t *testing.T) {
	cfg := discovery.Config{
		Mode:             discovery.ModeOff,
		ExpectedReplicas: 1,
		StatefulSetName:  "gossipgrid",
		NodeID:           "gossipgrid-0",
	}
	mgr := discovery.NewManager(cfg, nil, nil)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("manager Start: %v", err)
	}
	defer mgr.Shutdown()

	snapFn := NewDiscoverySnapshotFunc(mgr)
	view := snapFn()

	if view.Mode != "off" {
		t.Fatalf("expected mode off; got %s", view.Mode)
	}
	if len(view.Peers) != 0 {
		t.Fatalf("expected 0 peers in off mode; got %d", len(view.Peers))
	}
}

// TestSelector_ObservationExpiresDuringRevalidation verifies that when the
// observation's bounded lifetime expires BETWEEN the observation validation
// and the final recheck (i.e., the observation was valid when the selector
// initially received and validated it, but expires during the
// revalidation window), the candidate is rejected (not selected).
//
// The test uses a deterministic controlled clock installed BEFORE the
// controller starts. The observer returns evidence that is valid when the
// selector initially receives and validates it (the controlled clock is at
// base). The snapshot function counts its calls: on the SECOND call (the
// deterministic final-membership-validation boundary inside
// revalidateAfterObservation), the test advances the controlled clock past
// the observation's expiry. The metadata poll interval is set to one hour
// so the metadata remains fresh throughout (the expiration is driven by
// the observation's bounded lifetime, not by metadata staleness).
//
// The test verifies:
//   - the rejection reason is specifically expiration (not membership-changed
//     or authorization-changed);
//   - a second eligible candidate is successfully selected (fallback);
//   - when the expired candidate is the last usable candidate, the outcome
//     is OutcomeNoEligibleSource.
func TestSelector_ObservationExpiresDuringRevalidation(t *testing.T) {
	cfg := testSelectorConfig()
	cfg.SelectionBudget = 30 * time.Second
	cfg.ObservationBudget = 10 * time.Second

	// Deterministic controlled clock.
	base := time.Unix(1000000, 0)
	var clockMu sync.Mutex
	now := base
	ctrlClock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	advanceClock := func(d time.Duration) {
		clockMu.Lock()
		now = now.Add(d)
		clockMu.Unlock()
	}

	// Controller with the controlled clock installed BEFORE Start.
	// The metadata poll interval is set to one hour so the metadata
	// remains fresh throughout (the expiration is driven by the
	// observation's bounded lifetime, not by metadata staleness).
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	// Install the controlled clock before starting the controller so the
	// initial metadata read uses the controlled clock (the metadata
	// freshness check uses c.now(), which is the controlled clock).
	ctrl = ctrl.WithClock(ctrlClock)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { ctrl.Stop(context.Background()) })

	// Verify that metadata remains fresh (the controlled clock is at base;
	// the metadata was read at base with a 1-hour poll interval, so the
	// freshness threshold is base + 2 hours, which is far in the future).
	snap := ctrl.Authorization()
	if snap.AuthorityState != "fresh" || snap.Stale {
		t.Fatalf("expected metadata to be fresh (authority=%s, stale=%v); the 1-hour poll interval must keep metadata fresh at base", snap.AuthorityState, snap.Stale)
	}

	// Snapshot function that counts its calls and advances the clock on the
	// SECOND call (the deterministic final-membership-validation boundary
	// inside revalidateAfterObservation, after initial observation
	// validation has passed).
	var snapCallMu sync.Mutex
	snapCallCount := 0
	snapFn := func() DiscoverySnapshotView {
		snapCallMu.Lock()
		snapCallCount++
		n := snapCallCount
		snapCallMu.Unlock()
		if n == 2 {
			// Second call: advance the clock past the observation's expiry.
			// The observation was created at base with MaxSourceObservationTTL;
			// advancing past base + MaxSourceObservationTTL makes it expired.
			advanceClock(MaxSourceObservationTTL + time.Second)
		}
		return DiscoverySnapshotView{
			Peers: []DiscoveryPeerView{
				{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
				{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
			},
			LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
			ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
			Stale:              false,
			LastUpdated:        base,
			Mode:               "dns",
		}
	}

	// The observer returns a FRESH observation (valid at base, the time
	// when the selector initially receives and validates it). It returns
	// immediately (no blocking), so the observation is valid when the
	// selector receives it. The expiration happens LATER, during the
	// second snapshot call (inside revalidateAfterObservation).
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {
				result: NewSourceObservationResult(
					base, MaxSourceObservationTTL,
					RequestContext{ClusterID: testClusterID, EpochID: testEpochID},
					"gossipgrid-0.test.svc", "gossipgrid-0", "READY",
					testClusterID, testEpochID, true, true,
				),
			},
			"gossipgrid-2.test.svc:8443": {
				// The second candidate's observation is created at the
				// ADVANCED clock (after the first candidate's clock
				// advance), so it is valid at the advanced time.
				result: NewSourceObservationResult(
					base.Add(MaxSourceObservationTTL+time.Second), MaxSourceObservationTTL,
					RequestContext{ClusterID: testClusterID, EpochID: testEpochID},
					"gossipgrid-2.test.svc", "gossipgrid-2", "READY",
					testClusterID, testEpochID, true, true,
				),
			},
		},
	}

	sel := newTestSelector(t, cfg, snapFn, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())

	// The first candidate (gossipgrid-0) was rejected because its
	// observation expired during the final revalidation window (the clock
	// advanced during the second snapshot call). The second candidate
	// (gossipgrid-2) was selected successfully (fallback).
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected (fallback to second candidate); got %s (rejections=%+v)", result.Outcome, result.Rejections)
	}
	if result.Candidate == nil || result.Candidate.PodIdentity != "gossipgrid-2" {
		t.Fatalf("expected gossipgrid-2 selected (fallback); got %+v", result.Candidate)
	}
	// The first candidate must have been rejected with a reason
	// specifically mentioning expiration (not membership-changed or
	// authorization-changed).
	if len(result.Rejections) < 1 {
		t.Fatalf("expected at least 1 rejection (expired observation for gossipgrid-0); got %d", len(result.Rejections))
	}
	if result.Rejections[0].Candidate.PodIdentity != "gossipgrid-0" {
		t.Fatalf("expected first rejection to be gossipgrid-0; got %s", result.Rejections[0].Candidate.PodIdentity)
	}
	reason := result.Rejections[0].Reason
	if !strings.Contains(reason, "expired") && !strings.Contains(reason, "not valid") && !strings.Contains(reason, "lifetime") {
		t.Fatalf("expected rejection reason to mention observation expiration; got %q", reason)
	}

	// Verify the snapshot function was called at least twice: once for
	// buildCandidates, and once per candidate that reaches
	// revalidateAfterObservation. With two candidates (the first rejected
	// due to expiration, the second selected), the snapshot is called
	// at least 3 times (1 initial + 2 revalidations).
	snapCallMu.Lock()
	defer snapCallMu.Unlock()
	if snapCallCount < 2 {
		t.Fatalf("expected snapshot function to be called at least twice; got %d", snapCallCount)
	}
}

// TestSelector_ObservationExpiresLastCandidate verifies that when the
// expired candidate is the LAST usable candidate, the outcome is
// OutcomeNoEligibleSource (all usable candidates attempted and rejected).
func TestSelector_ObservationExpiresLastCandidate(t *testing.T) {
	cfg := testSelectorConfig()
	cfg.SelectionBudget = 30 * time.Second
	cfg.ObservationBudget = 10 * time.Second

	// Deterministic controlled clock.
	base := time.Unix(1000000, 0)
	var clockMu sync.Mutex
	now := base
	ctrlClock := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	advanceClock := func(d time.Duration) {
		clockMu.Lock()
		now = now.Add(d)
		clockMu.Unlock()
	}

	// Controller with the controlled clock installed BEFORE Start.
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	ctrl = ctrl.WithClock(ctrlClock)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { ctrl.Stop(context.Background()) })

	// Snapshot function that advances the clock on the second call.
	// Only ONE candidate (gossipgrid-0) is present: when its observation
	// expires during revalidation, there is no fallback candidate.
	var snapCallMu sync.Mutex
	snapCallCount := 0
	snapFn := func() DiscoverySnapshotView {
		snapCallMu.Lock()
		snapCallCount++
		n := snapCallCount
		snapCallMu.Unlock()
		if n == 2 {
			advanceClock(MaxSourceObservationTTL + time.Second)
		}
		return DiscoverySnapshotView{
			Peers: []DiscoveryPeerView{
				{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			},
			LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
			ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
			Stale:              false,
			LastUpdated:        base,
			Mode:               "dns",
		}
	}

	// The observer returns a FRESH observation (valid at base).
	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {
				result: NewSourceObservationResult(
					base, MaxSourceObservationTTL,
					RequestContext{ClusterID: testClusterID, EpochID: testEpochID},
					"gossipgrid-0.test.svc", "gossipgrid-0", "READY",
					testClusterID, testEpochID, true, true,
				),
			},
		},
	}

	sel := newTestSelector(t, cfg, snapFn, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())

	// The single candidate (gossipgrid-0) was rejected because its
	// observation expired during the final revalidation window. There is
	// no fallback candidate, so the outcome is OutcomeNoEligibleSource.
	if result.Outcome != OutcomeNoEligibleSource {
		t.Fatalf("expected OutcomeNoEligibleSource (expired last candidate); got %s (rejections=%+v)", result.Outcome, result.Rejections)
	}
	if len(result.Rejections) < 1 {
		t.Fatal("expected at least 1 rejection (expired observation)")
	}
	reason := result.Rejections[0].Reason
	if !strings.Contains(reason, "expired") && !strings.Contains(reason, "not valid") && !strings.Contains(reason, "lifetime") {
		t.Fatalf("expected rejection reason to mention observation expiration; got %q", reason)
	}
}

// TestSelector_InternalDeadlineExhausted verifies that when the
// selector-owned total deadline (SelectionBudget) expires during the
// selection, the outcome is OutcomeBudgetExhausted (distinct from
// OutcomeCancelled, which is for caller-context cancellation).
//
// The test uses a deterministic controlled clock for the controller
// (so the observation's Valid check uses a predictable time) and a
// very short SelectionBudget that expires while the observer is still
// blocking. The ObservationBudget is set to the same value as the
// SelectionBudget so that the selection budget expires first (a
// per-candidate budget cannot exceed the total budget; when they are
// equal, the total budget's deadline is the same or earlier, and the
// derived context's deadline is the minimum of both).
func TestSelector_InternalDeadlineExhausted(t *testing.T) {
	cfg := testSelectorConfig()
	cfg.SelectionBudget = 50 * time.Millisecond
	cfg.ObservationBudget = 50 * time.Millisecond

	// Controller with a fixed clock (the observation is valid at the
	// fixed time; the SelectionBudget expiry is what drives the
	// budget-exhausted outcome).
	base := time.Unix(1000000, 0)
	ctrlClock := func() time.Time { return base }

	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctrl = ctrl.WithClock(ctrlClock)
	t.Cleanup(func() { ctrl.Stop(context.Background()) })

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        base,
		Mode:               "dns",
	}

	// The observer blocks until the test unblocks it (or the context is
	// cancelled). The SelectionBudget (100ms) expires while the observer
	// is blocked, causing the selector's derived context to expire.
	blockCh := make(chan struct{})
	obs := &blockingObserver{
		result:  eligibleObs("gossipgrid-0", "READY"),
		blockCh: blockCh,
		called:  make(chan struct{}),
	}
	t.Cleanup(func() { close(blockCh) })
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	// The selector's total deadline expired during the observation: the
	// outcome must be OutcomeBudgetExhausted (not OutcomeCancelled, since
	// the caller context was not cancelled; not OutcomeNoEligibleSource,
	// since the budget was exhausted, not the candidate set).
	if result.Outcome != OutcomeBudgetExhausted {
		t.Fatalf("expected OutcomeBudgetExhausted (selector deadline expired); got %s (rejections=%+v)", result.Outcome, result.Rejections)
	}
}

// TestSelector_CancellationDuringFinalMembershipValidation verifies that
// caller cancellation during the final membership revalidation (the second
// snapshot invocation inside revalidateAfterObservation) returns
// OutcomeCancelled (not OutcomeMembershipChanged). The caller context is
// checked immediately after revalidateAfterObservation returns and before
// classifying its result, so a caller cancellation that linearized during
// the revalidation takes precedence over the membership-changed outcome.
//
// The test is deterministic: the observer returns a valid observation
// immediately (no blocking, no sleeps). The snapshot function returns a
// stable membership on the first invocation (candidate construction) and
// blocks on a channel during the second invocation (the
// final-membership-validation boundary). The test cancels the caller
// context once the second snapshot invocation is reached.
func TestSelector_CancellationDuringFinalMembershipValidation(t *testing.T) {
	cfg := testSelectorConfig()
	cfg.SelectionBudget = 30 * time.Second
	cfg.ObservationBudget = 10 * time.Second

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	// First snapshot: stable membership (gossipgrid-0 present and expected).
	stableSnap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}
	// The snapshot function: first invocation returns the stable snapshot
	// (candidate construction succeeds, the observer returns a valid
	// observation). The second invocation (the
	// final-membership-validation boundary inside revalidateAfterObservation)
	// signals via secondCallCh and blocks on unblockCh until the test
	// closes it (after cancelling the caller context). It then returns
	// changedSnap (gossipgrid-0 removed from the membership), which would
	// produce OutcomeMembershipChanged if the caller context were not
	// cancelled. The selector's terminal check after
	// revalidateAfterObservation returns sees the caller context cancelled
	// and returns OutcomeCancelled — demonstrating that cancellation takes
	// precedence over a membership-change result.
	secondCallCh := make(chan struct{})
	unblockCh := make(chan struct{})
	var callCount int32
	var mu sync.Mutex
	snapFn := func() DiscoverySnapshotView {
		mu.Lock()
		callCount++
		n := callCount
		mu.Unlock()
		if n == 1 {
			return stableSnap
		}
		// Second invocation: signal that we've reached the
		// final-membership-validation boundary, then block until the test
		// unblocks us (after cancelling the caller context).
		close(secondCallCh)
		<-unblockCh
		// Return a snapshot with gossipgrid-0 removed from the membership:
		// revalidateAfterObservation would classify this as
		// OutcomeMembershipChanged. But the caller context is already
		// cancelled, so the selector's terminal check returns
		// OutcomeCancelled first.
		return DiscoverySnapshotView{
			Peers:              nil, // gossipgrid-0 removed from the membership
			LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
			ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
			Stale:              false,
			LastUpdated:        time.Now(),
			Mode:               "dns",
		}
	}

	obs := &immediatelyValidObserver{result: eligibleObs("gossipgrid-0", "READY")}
	sel := newTestSelector(t, cfg, snapFn, obs, ctrl, "gossipgrid-1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := make(chan SelectionResult, 1)
	go func() {
		resultCh <- sel.Select(ctx)
	}()

	// Wait for the second snapshot invocation (the
	// final-membership-validation boundary), then cancel the caller context
	// and unblock the snapshot function so the selection can complete.
	select {
	case <-secondCallCh:
		// The selector is now inside revalidateAfterObservation's second
		// snapshot invocation. Cancel the caller context, then unblock the
		// snapshot function: revalidateAfterObservation returns
		// errMembershipChanged, but the selector's terminal check immediately
		// after sees the caller context cancelled and returns
		// OutcomeCancelled (cancellation takes precedence over the
		// membership-changed result).
		cancel()
		close(unblockCh)
		select {
		case result := <-resultCh:
			if result.Outcome != OutcomeCancelled {
				t.Fatalf("expected OutcomeCancelled (caller cancellation during final membership validation); got %s (rejections=%+v)", result.Outcome, result.Rejections)
			}
			if result.Candidate != nil {
				t.Fatalf("expected no selected candidate on OutcomeCancelled; got %+v", result.Candidate)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for selection result after cancelling during final membership validation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the second snapshot invocation (final-membership-validation boundary)")
	}
}

// TestSelector_LateResultThenFallback verifies that a valid observer result
// returned after the individual observation deadline is rejected (late
// result is not eligible), and the next candidate is attempted successfully
// when the total budget remains available.
func TestSelector_LateResultThenFallback(t *testing.T) {
	cfg := testSelectorConfig()
	cfg.SelectionBudget = 30 * time.Second
	cfg.ObservationBudget = 50 * time.Millisecond

	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
			{Hostname: "gossipgrid-2.test.svc", PodIdentity: "gossipgrid-2", Address: "gossipgrid-2.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	// The observer for gossipgrid-0 blocks until the observation deadline
	// (50ms) expires, then returns a valid result (a late result, not
	// eligible). The observer for gossipgrid-2 returns a valid result
	// immediately.
	obs := &lateThenFastObserver{
		lateAddr:     "gossipgrid-0.test.svc:8443",
		lateResult:   eligibleObs("gossipgrid-0", "READY"),
		fastAddr:     "gossipgrid-2.test.svc:8443",
		fastResult:   eligibleObs("gossipgrid-2", "READY"),
		lateCalled:   make(chan struct{}),
		lateDoneOnce: &sync.Once{},
	}

	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	// The first candidate (gossipgrid-0) was rejected (late result), and
	// the second candidate (gossipgrid-2) was selected successfully.
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected (late result rejected, fallback to second candidate); got %s (rejections=%+v)", result.Outcome, result.Rejections)
	}
	if result.Candidate == nil || result.Candidate.PodIdentity != "gossipgrid-2" {
		t.Fatalf("expected gossipgrid-2 selected; got %+v", result.Candidate)
	}
	// The first candidate must have been rejected with a reason mentioning
	// the individual deadline.
	if len(result.Rejections) < 1 {
		t.Fatalf("expected at least 1 rejection (late result for gossipgrid-0); got %d", len(result.Rejections))
	}
	if result.Rejections[0].Candidate.PodIdentity != "gossipgrid-0" {
		t.Fatalf("expected first rejection to be gossipgrid-0; got %s", result.Rejections[0].Candidate.PodIdentity)
	}
	if !strings.Contains(result.Rejections[0].Reason, "deadline") {
		t.Fatalf("expected rejection reason to mention deadline; got %q", result.Rejections[0].Reason)
	}
}

// lateThenFastObserver is a test observer that blocks for the first candidate
// (past the observation deadline) and returns immediately for the second.
type lateThenFastObserver struct {
	lateAddr     string
	lateResult   SourceObservationResult
	fastAddr     string
	fastResult   SourceObservationResult
	lateCalled   chan struct{}
	lateDoneOnce *sync.Once
}

func (o *lateThenFastObserver) Observe(ctx context.Context, dialAddr, _ string, _ RequestContext, _ string) (SourceObservationResult, error) {
	if dialAddr == o.lateAddr {
		o.lateDoneOnce.Do(func() { close(o.lateCalled) })
		// Block until the observation deadline expires (or the context is
		// cancelled). The observation context's deadline (ObservationBudget)
		// will fire while we are blocked, making the returned result "late".
		<-ctx.Done()
		// The observation deadline expired; return the result (late).
		return o.lateResult, nil
	}
	if dialAddr == o.fastAddr {
		return o.fastResult, nil
	}
	return SourceObservationResult{}, errors.New("observer: no result configured for " + dialAddr)
}

// TestSelector_BindingReadErrorPreJoin verifies that a binding-read error
// during pre-join selection is rejected fail-closed (distinct from an
// absent binding). The 3-way binding distinction: a failed store read is
// NOT evidence that the database is unbound.
func TestSelector_BindingReadErrorPreJoin(t *testing.T) {
	cfg := testSelectorConfig()
	cfg.SelectionBudget = 10 * time.Second
	cfg.ObservationBudget = 5 * time.Second

	// Controller with a binding store that returns an error from GetBinding.
	binding := &fakeBindingStoreWithMessages{}
	binding.SetGetErr(errors.New("simulated SQLite I/O error"))
	src := NewFakeMetadataSource(finalizedMeta(), true)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { ctrl.Stop(context.Background()) })

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	// The binding-read error is a distinct, fail-closed condition caught
	// early by authorizationTarget (the snapshot's Binding is nil and
	// BindingReadError is non-nil, captured during the existing read-through
	// inside Controller.authorization under the established lock order).
	// The outcome must be OutcomeAuthorizationUnavailable (the authorization
	// target could not be established because the binding store read failed —
	// it is NOT conflated with an absent binding, which would allow the
	// target to be established). It is NOT OutcomeSelected, NOT
	// OutcomeAuthorizationChanged (the authorization did not change; the
	// store read failed), and NOT OutcomeNoEligibleSource.
	if result.Outcome != OutcomeAuthorizationUnavailable {
		t.Fatalf("expected OutcomeAuthorizationUnavailable (binding-read error is fail-closed); got %s (rejections=%+v)", result.Outcome, result.Rejections)
	}
	// No candidates were observed (the selection failed before the
	// observation loop).
	if len(result.Rejections) != 0 {
		t.Fatalf("expected no rejections (selection failed before observation); got %+v", result.Rejections)
	}
}

// TestSelector_NilBindingStoreDependency verifies that the selector rejects
// a controller with a nil binding store before attempting selection (a
// programming error, rejected fail-closed).
func TestSelector_NilBindingStoreDependency(t *testing.T) {
	cfg := testSelectorConfig()

	// A controller with a nil binding store.
	src := NewFakeMetadataSource(finalizedMeta(), true)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, nil, nil)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { ctrl.Stop(context.Background()) })

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	// The nil binding store is a missing required dependency; the selector
	// rejects it before attempting selection.
	if result.Outcome != OutcomeAuthorizationUnavailable {
		t.Fatalf("expected OutcomeAuthorizationUnavailable (nil binding store); got %s", result.Outcome)
	}
	if len(result.Rejections) != 0 {
		t.Fatalf("expected no rejections; got %+v", result.Rejections)
	}
}

// TestSelector_NilMetadataSourceDependency verifies that the selector rejects
// a controller with a nil metadata source before attempting selection.
func TestSelector_NilMetadataSourceDependency(t *testing.T) {
	cfg := testSelectorConfig()

	// A controller with a nil metadata source. Start is NOT called (it
	// would panic on the nil source); the controller is in STARTING state.
	// The nil-dependency check in authorizationTarget fires before any
	// snapshot read, so the selector rejects it before attempting
	// selection.
	binding := &fakeBindingStoreWithMessages{}
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), nil, binding, nil)
	t.Cleanup(func() { ctrl.Stop(context.Background()) })

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	// The nil metadata source is a missing required dependency; the selector
	// rejects it before attempting selection.
	if result.Outcome != OutcomeAuthorizationUnavailable {
		t.Fatalf("expected OutcomeAuthorizationUnavailable (nil metadata source); got %s", result.Outcome)
	}
	if len(result.Rejections) != 0 {
		t.Fatalf("expected no rejections; got %+v", result.Rejections)
	}
}

// TestSelector_UnboundReplicaCanSelect verifies that a genuinely unbound
// replica (binding store returns nil, no error) can still select a
// bootstrap source (the 3-way binding distinction: absent binding is
// NOT a binding-read error).
func TestSelector_UnboundReplicaCanSelect(t *testing.T) {
	cfg := testSelectorConfig()

	// A controller with a binding store that returns nil, no error
	// (genuinely unbound).
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { ctrl.Stop(context.Background()) })

	snap := DiscoverySnapshotView{
		Peers: []DiscoveryPeerView{
			{Hostname: "gossipgrid-0.test.svc", PodIdentity: "gossipgrid-0", Address: "gossipgrid-0.test.svc:8443"},
		},
		LocalNode:          &DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	obs := &testSnapshotObserver{
		results: map[string]struct {
			result SourceObservationResult
			err    error
		}{
			"gossipgrid-0.test.svc:8443": {result: eligibleObs("gossipgrid-0", "READY")},
		},
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, obs, ctrl, "gossipgrid-1")

	result := sel.Select(context.Background())
	// A genuinely unbound replica (absent binding, no error) CAN select:
	// the 3-way binding distinction treats absent binding as a valid
	// pre-join condition, NOT as a binding-read error.
	if result.Outcome != OutcomeSelected {
		t.Fatalf("expected OutcomeSelected (unbound replica can select); got %s (rejections=%+v)", result.Outcome, result.Rejections)
	}
	if result.Candidate == nil || result.Candidate.PodIdentity != "gossipgrid-0" {
		t.Fatalf("expected gossipgrid-0 selected; got %+v", result.Candidate)
	}
}

// TestSelector_Normalization verifies that candidate normalization
// (normalizeDialAddr) handles:
//   - normal host:port (identity);
//   - DNS case normalization (uppercase → lowercase);
//   - trailing dot stripping;
//   - whitespace rejection;
//   - invalid port rejection;
//   - IPv6 bracket preservation;
//   - empty host rejection.
func TestSelector_Normalization(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		want    string
		wantErr bool
	}{
		{"normal", "gossipgrid-0.test.svc:8443", "gossipgrid-0.test.svc:8443", false},
		{"dns uppercase", "GOSSIPGRID-0.TEST.SVC:8443", "gossipgrid-0.test.svc:8443", false},
		{"trailing dot", "gossipgrid-0.test.svc.:8443", "gossipgrid-0.test.svc:8443", false},
		{"whitespace", "gossipgrid-0.test.svc :8443", "", true},
		{"invalid port", "gossipgrid-0.test.svc:0", "", true},
		{"port too high", "gossipgrid-0.test.svc:99999", "", true},
		{"non-numeric port", "gossipgrid-0.test.svc:abc", "", true},
		{"ipv6", "[::1]:8443", "[::1]:8443", false},
		{"ipv6 uppercase", "[::FFFF]:8443", "[::ffff]:8443", false},
		{"empty host", ":8443", "", true},
		{"no port", "gossipgrid-0.test.svc", "", true},
		{"leading whitespace", " gossipgrid-0.test.svc:8443", "gossipgrid-0.test.svc:8443", false},
		{"trailing whitespace", "gossipgrid-0.test.svc:8443 ", "gossipgrid-0.test.svc:8443", false},
		{"ipv4 normal", "127.0.0.1:443", "127.0.0.1:443", false},
		{"ipv4 leading zeros treated as dns", "010.001.000.001:443", "010.001.000.001:443", false},
		{"ipv4-mapped ipv6", "[::ffff:192.0.2.1]:8443", "192.0.2.1:8443", false},
		{"path in host", "gossipgrid-0.test.svc/foo:8443", "", true},
		{"invalid ipv6 literal", "[::zzz]:8443", "", true},
		{"invalid dns label leading hyphen", "-gossipgrid-0.test.svc:8443", "", true},
		{"invalid dns label trailing hyphen", "gossipgrid-0.test.svc-:8443", "", true},
		{"invalid dns label with underscore", "gossipgrid_0.test.svc:8443", "", true},
		{"empty label double dot", "gossipgrid-0..test.svc:8443", "", true},
		{"label too long", strings.Repeat("a", 64) + ".test.svc:8443", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := normalizeDialAddr(tc.addr)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error; got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected %q; got %q", tc.want, got)
			}
		})
	}
}

// --- Trusted-observer result validation tests (shared validateSourceObservation) ---
//
// These tests verify that the shared, strict, fail-closed observation
// validator (validateSourceObservation, used by both the controller's join
// path and the bootstrap-source selector) correctly rejects observations
// that fail each individual check: epoch mismatch, target mismatch,
// identity mismatch, PodID mismatch, future timestamp, and readiness
// inconsistency. The validator is the single consistent observation
// contract; a trusted-boundary result that carries a mismatched field is
// rejected fail-closed.

func TestValidateSourceObservation_EpochMismatch(t *testing.T) {
	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	now := time.Now()

	// An observation reporting a different epoch than the target metadata.
	obs := NewSourceObservationResult(
		now, MaxSourceObservationTTL,
		RequestContext{ClusterID: testClusterID, EpochID: "wrong-epoch"},
		"gossipgrid-0.test.svc", "gossipgrid-0", "READY",
		testClusterID, "wrong-epoch", true, true,
	)
	meta := &ClusterMetadata{ClusterID: testClusterID, EpochID: testEpochID}
	target := RequestContext{ClusterID: testClusterID, EpochID: testEpochID}
	err := validateSourceObservation(ctrl, obs, meta, target, "gossipgrid-0.test.svc")
	if err == nil {
		t.Fatal("expected an error (epoch mismatch); got nil")
	}
	if !strings.Contains(err.Error(), "epoch") {
		t.Fatalf("expected an epoch-mismatch error; got %q", err)
	}
}

func TestValidateSourceObservation_TargetMismatch(t *testing.T) {
	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	now := time.Now()

	// An observation whose pinned target does not match the requested target.
	obs := NewSourceObservationResult(
		now, MaxSourceObservationTTL,
		RequestContext{ClusterID: "pinned-cluster", EpochID: "pinned-epoch"},
		"gossipgrid-0.test.svc", "gossipgrid-0", "READY",
		testClusterID, testEpochID, true, true,
	)
	meta := &ClusterMetadata{ClusterID: testClusterID, EpochID: testEpochID}
	target := RequestContext{ClusterID: testClusterID, EpochID: testEpochID}
	err := validateSourceObservation(ctrl, obs, meta, target, "gossipgrid-0.test.svc")
	if err == nil {
		t.Fatal("expected an error (target mismatch); got nil")
	}
	if !strings.Contains(err.Error(), "pinned target") {
		t.Fatalf("expected a pinned-target-mismatch error; got %q", err)
	}
}

func TestValidateSourceObservation_IdentityMismatch(t *testing.T) {
	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	now := time.Now()

	// An observation whose authenticated peer identity does not match the
	// expected identity the observation was requested for.
	obs := NewSourceObservationResult(
		now, MaxSourceObservationTTL,
		RequestContext{ClusterID: testClusterID, EpochID: testEpochID},
		"gossipgrid-99.test.svc", "gossipgrid-0", "READY",
		testClusterID, testEpochID, true, true,
	)
	meta := &ClusterMetadata{ClusterID: testClusterID, EpochID: testEpochID}
	target := RequestContext{ClusterID: testClusterID, EpochID: testEpochID}
	err := validateSourceObservation(ctrl, obs, meta, target, "gossipgrid-0.test.svc")
	if err == nil {
		t.Fatal("expected an error (identity mismatch); got nil")
	}
	if !strings.Contains(err.Error(), "authenticated peer identity") {
		t.Fatalf("expected an identity-mismatch error; got %q", err)
	}
}

func TestValidateSourceObservation_PodIDMismatch(t *testing.T) {
	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	now := time.Now()

	// The shared validator does not check PodID (it has no access to the
	// candidate); the selector-specific check (validateObservation) does.
	// Test the selector-specific check via the selector's validateObservation.
	cfg := testSelectorConfig()
	snap := DiscoverySnapshotView{
		Peers:              []DiscoveryPeerView{},
		LocalNode:          &DiscoveryPeerView{PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1"},
		LastUpdated:        now,
		Mode:               "dns",
	}
	sel := newTestSelector(t, cfg, func() DiscoverySnapshotView { return snap }, &testSnapshotObserver{}, ctrl, "gossipgrid-1")

	// An observation whose peerNodeID does not match the candidate's PodID.
	obs := NewSourceObservationResult(
		now, MaxSourceObservationTTL,
		RequestContext{ClusterID: testClusterID, EpochID: testEpochID},
		"gossipgrid-0.test.svc", "gossipgrid-99", "READY",
		testClusterID, testEpochID, true, true,
	)
	target := RequestContext{ClusterID: testClusterID, EpochID: testEpochID}
	cand := Candidate{PodIdentity: "gossipgrid-0", DialAddr: "gossipgrid-0.test.svc:8443", ExpectedIdentity: "gossipgrid-0.test.svc"}
	err := sel.validateObservation(obs, target, cand)
	if err == nil {
		t.Fatal("expected an error (PodID mismatch); got nil")
	}
	if !strings.Contains(err.Error(), "PodID") {
		t.Fatalf("expected a PodID-mismatch error; got %q", err)
	}
}

func TestValidateSourceObservation_FutureTimestamp(t *testing.T) {
	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	now := time.Now()

	// An observation with a future timestamp (observedAt after now): an
	// observation that has not yet occurred is not evidence (a clock skew
	// or a fabricated value). The Valid() check rejects it.
	obs := NewSourceObservationResult(
		now.Add(100*time.Second), MaxSourceObservationTTL,
		RequestContext{ClusterID: testClusterID, EpochID: testEpochID},
		"gossipgrid-0.test.svc", "gossipgrid-0", "READY",
		testClusterID, testEpochID, true, true,
	)
	meta := &ClusterMetadata{ClusterID: testClusterID, EpochID: testEpochID}
	target := RequestContext{ClusterID: testClusterID, EpochID: testEpochID}
	err := validateSourceObservation(ctrl, obs, meta, target, "gossipgrid-0.test.svc")
	if err == nil {
		t.Fatal("expected an error (future timestamp); got nil")
	}
	if !strings.Contains(err.Error(), "not valid") {
		t.Fatalf("expected a validity error (future timestamp); got %q", err)
	}
}

func TestValidateSourceObservation_ReadinessInconsistent(t *testing.T) {
	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	now := time.Now()

	// An observation with readiness=false but BootstrapSourceEligible=true:
	// inconsistent with an eligible READY/DEGRADED source.
	obs := NewSourceObservationResult(
		now, MaxSourceObservationTTL,
		RequestContext{ClusterID: testClusterID, EpochID: testEpochID},
		"gossipgrid-0.test.svc", "gossipgrid-0", "READY",
		testClusterID, testEpochID, true, false, // readiness=false
	)
	meta := &ClusterMetadata{ClusterID: testClusterID, EpochID: testEpochID}
	target := RequestContext{ClusterID: testClusterID, EpochID: testEpochID}
	err := validateSourceObservation(ctrl, obs, meta, target, "gossipgrid-0.test.svc")
	if err == nil {
		t.Fatal("expected an error (readiness inconsistent); got nil")
	}
	if !strings.Contains(err.Error(), "readiness") {
		t.Fatalf("expected a readiness error; got %q", err)
	}
}

func TestValidateSourceObservation_Valid(t *testing.T) {
	ctrl := newTestSelectorController(t, "gossipgrid-1", finalizedMeta())
	now := time.Now()

	// A fully valid observation: all checks pass.
	obs := NewSourceObservationResult(
		now, MaxSourceObservationTTL,
		RequestContext{ClusterID: testClusterID, EpochID: testEpochID},
		"gossipgrid-0.test.svc", "gossipgrid-0", "READY",
		testClusterID, testEpochID, true, true,
	)
	meta := &ClusterMetadata{ClusterID: testClusterID, EpochID: testEpochID}
	target := RequestContext{ClusterID: testClusterID, EpochID: testEpochID}
	err := validateSourceObservation(ctrl, obs, meta, target, "gossipgrid-0.test.svc")
	if err != nil {
		t.Fatalf("expected no error (valid observation); got %q", err)
	}
}
