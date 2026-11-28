// Package peertls: real-mTLS integration test for the deterministic
// bootstrap-source selector (Milestone 5B, Stage 3B).
//
// These tests verify, end-to-end with a real mTLS handshake, that:
//   - an unbound requester in DISCOVERING with fresh, finalized metadata
//     selects an eligible, same-epoch bootstrap source from a discovery
//     membership containing multiple candidates (some ineligible);
//   - the production observer actually contacts and rejects the first
//     candidate (authenticated but ineligible) before selecting the second
//     candidate (authenticated and eligible);
//   - the selection alone does not create a binding or alter the lifecycle
//     state (selection is advisory, not a join);
//   - the selected source, observed through the production observer, is
//     accepted by AuthorizeJoin and transitions the replica to SYNCING with
//     a real durable join binding;
//   - a SYNCING replica has ordinary replication and public writes denied
//     but bootstrap fetch permitted;
//   - the selector can select a bootstrap source for a SYNCING requester;
//   - a fully-unsuccessful selection (all candidates ineligible) leaves a
//     new, unbound replica unbound and preserves its state.
//
// The real mTLS handshake is performed by the production
// peertls.BootstrapSourceObserver against real peertls managers with
// ephemeral certificates. The discovery membership is supplied through a
// deterministic snapshot function (the production adapter
// lifecycle.NewDiscoverySnapshotFunc performs the same mapping from
// discovery.Manager.Snapshot()).
//
// The peer client is configured with both the ordinary EpochProvider and
// the dedicated BootstrapEpochProvider, mirroring the production
// cmd/gossipgrid/main.go wiring.
//
// This test lives in the peertls package (not lifecycle) because the
// peertls package's test helpers (newTestCA, testCredentials,
// newTestManager, startTestServer) are in _test files and are only
// accessible to peertls package tests. The peertls package already imports
// lifecycle (in production code), so a peertls test can use both the
// production observer and the lifecycle controller.

package peertls

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/store"
)

// finalizedTestMeta returns the authoritative metadata for a finalized cluster
// (Initialized=true, GenesisAuthorized=false, GenesisConsumer="gossipgrid-0",
// DataLossProtected=true), suitable for pre-join and join authorization.
func finalizedTestMeta() lifecycle.ClusterMetadata {
	return lifecycle.ClusterMetadata{
		ClusterID:            "0190e8a2-6b0c-7000-8000-000000000000",
		EpochID:              "0190e8a3-1c2d-7000-9000-111111111111",
		Generation:           1,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      "gossipgrid-0",
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
}

// testMetaSource is a deterministic, concurrency-safe fake for
// lifecycle.MetadataSource.
type testMetaSource struct {
	mu    sync.RWMutex
	meta  lifecycle.ClusterMetadata
	avail bool
}

func (s *testMetaSource) Read(_ context.Context) (lifecycle.ClusterMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.avail {
		return lifecycle.ClusterMetadata{}, errors.New("metadata unavailable")
	}
	return s.meta, nil
}

func (s *testMetaSource) SetMeta(m lifecycle.ClusterMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta = m
	s.avail = true
}

// newRealBindingStore creates a real SQLite-backed binding store using
// store.New. The store is closed when the test completes. It is used in
// place of the in-memory fakeBindingStore to verify that the production
// SQLite binding store correctly distinguishes "absent binding" from
// "failed store read" (the 3-way binding distinction), and that the
// durable binding is correctly written and read by the controller.
func newRealBindingStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.New(t.TempDir() + "/binding.db")
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Logf("store close: %v", err)
		}
	})
	return s
}

// selectorTestEpochProvider adapts a lifecycle controller to the peertls
// EpochProvider interface, mirroring the production cmd/gossipgrid/main.go
// wiring. It returns the controller's currently authorized cluster/epoch
// when authorized (and not stopped), and an error otherwise.
type selectorTestEpochProvider struct {
	ctrl *lifecycle.Controller
}

func (p *selectorTestEpochProvider) Context() (lifecycle.RequestContext, error) {
	snap := p.ctrl.Authorization()
	if snap.Stopped {
		return lifecycle.RequestContext{}, fmt.Errorf("lifecycle controller stopped")
	}
	if snap.Metadata == nil {
		return lifecycle.RequestContext{}, fmt.Errorf("no authoritative metadata observation")
	}
	if snap.AuthorityState != "fresh" || snap.Stale {
		return lifecycle.RequestContext{}, fmt.Errorf("authoritative metadata not fresh")
	}
	if snap.Binding == nil || !snap.Binding.MatchesMetadata(*snap.Metadata) {
		return lifecycle.RequestContext{}, fmt.Errorf("local database binding does not match authorized epoch")
	}
	return lifecycle.RequestContext{
		ClusterID: snap.Metadata.ClusterID,
		EpochID:   snap.Metadata.EpochID,
	}, nil
}

func (p *selectorTestEpochProvider) Enforcement() bool { return true }

// selectorTestBootstrapProvider adapts the lifecycle controller to the
// peertls.BootstrapEpochProvider interface, mirroring the production
// cmd/gossipgrid/main.go wiring. It delegates to the controller's
// BootstrapAuthorizationSnapshot.
type selectorTestBootstrapProvider struct {
	ctrl *lifecycle.Controller
}

func (p *selectorTestBootstrapProvider) BootstrapDecision() (lifecycle.RequestContext, bool, error) {
	return p.ctrl.BootstrapAuthorizationSnapshot()
}

// sourceStatusHandler returns an http.Handler that serves GET
// /internal/status with the given lifecycle fields, mirroring the real peer
// handler's JSON shape and the response epoch headers that the production
// observer requires.
func sourceStatusHandler(nodeID, podID, clusterID, epochID, state string, readiness, eligible bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, clusterID)
		w.Header().Set(lifecycle.HeaderRespEpochID, epochID)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node_id":                   nodeID,
			"version":                   "v1",
			"pod_id":                    podID,
			"cert_fingerprint":          "test-fingerprint",
			"cluster_id":                clusterID,
			"epoch_id":                  epochID,
			"generation":                1,
			"lifecycle_state":           state,
			"readiness":                 readiness,
			"bootstrap_source_eligible": eligible,
		})
	})
	return mux
}

// countingObserver wraps a BootstrapSourceObserver and counts the number of
// Observe calls. It is used to verify that AuthorizeJoin independently
// observes the source (a separate observation from the selector's own).
type countingObserver struct {
	inner lifecycle.BootstrapSourceObserver
	count *int
}

func (o *countingObserver) Observe(ctx context.Context, dialAddr, expectedIdentity string, expected lifecycle.RequestContext, localNodeID string) (lifecycle.SourceObservationResult, error) {
	*(o.count)++
	return o.inner.Observe(ctx, dialAddr, expectedIdentity, expected, localNodeID)
}

// mTLSSource is a real mTLS peer used as a bootstrap-source candidate in the
// selector integration tests.
type mTLSSource struct {
	mgr      *Manager
	peerAddr string
	podID    string
	stop     func()
}

// newSelectorTestManager builds and starts a peertls.Manager from the given
// credential files.
func newSelectorTestManager(t *testing.T, certFile, keyFile, trustFile, podID string) *Manager {
	t.Helper()
	mgr := New(Config{
		CertFile:         certFile,
		KeyFile:          keyFile,
		TrustBundleFile:  trustFile,
		IdentitySuffix:   testSuffix,
		StatefulSetName:  testStatefulSet,
		NodeID:           podID,
		ExpectedReplicas: 10,
		ReloadInterval:   100 * time.Millisecond,
	}, nil)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("manager Start for %s: %v", podID, err)
	}
	t.Cleanup(mgr.Shutdown)
	return mgr
}

// newmTLSSource creates and starts a real mTLS peer with the given pod
// identity and lifecycle fields (state, readiness, bootstrapSourceEligible).
func newmTLSSource(t *testing.T, ca *testCA, podID, clusterID, epochID, state string, readiness, eligible bool) *mTLSSource {
	t.Helper()
	cert, key, trust := testCredentials(t, ca, podID)
	mgr := newSelectorTestManager(t, cert, key, trust, podID)
	handler := sourceStatusHandler(podID, podID, clusterID, epochID, state, readiness, eligible)
	addr, stop := startTestServer(t, mgr, handler)
	return &mTLSSource{mgr: mgr, peerAddr: addr, podID: podID, stop: stop}
}

// newSelectorTestController creates a lifecycle controller in the given
// initial state with fresh, finalized metadata, suitable for pre-join source
// selection and join tests. It uses a real SQLite-backed binding store
// (store.New) in place of the in-memory fake, so the 3-way binding
// distinction (absent vs. present vs. read-failed) is exercised with the
// production store. It returns both the controller and the real store so
// tests can verify the persisted binding directly via store.GetBinding.
func newSelectorTestController(t *testing.T, nodeID, seedIdentity string, meta lifecycle.ClusterMetadata) (*lifecycle.Controller, *store.Store) {
	t.Helper()
	metaSrc := &testMetaSource{meta: meta, avail: true}
	bindingStore := newRealBindingStore(t)
	ctrl := lifecycle.NewController(lifecycle.Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               nodeID,
		SeedIdentity:         seedIdentity,
		Enabled:              true,
	}, lifecycle.NewMachine(nil), metaSrc, bindingStore, nil)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("controller Start for %s: %v", nodeID, err)
	}
	t.Cleanup(func() { ctrl.Stop(context.Background()) })
	return ctrl, bindingStore
}

// TestSelectorIntegration_RealMTLS verifies, end-to-end with a real mTLS
// handshake, the complete selection-to-join scenario:
//
//  1. Requester is unbound and DISCOVERING.
//  2. Ordinary replication and bootstrap fetching are denied before joining.
//  3. First candidate (gossipgrid-0) is rejected by the production observer
//     (authenticated but ineligible: DEGRADED, not eligible).
//  4. Subsequent eligible source (gossipgrid-2, READY, eligible) is selected.
//  5. Selection creates no binding and changes no lifecycle state.
//  6. AuthorizeJoin independently observes the selected source.
//  7. AuthorizeJoin creates an actual durable join binding and transitions
//     to SYNCING.
//  8. Public writes and ordinary replication remain denied.
//  9. Dedicated bootstrap-fetch admission permits the matching context.
//  10. Bootstrap-source admission and premature SetReady remain denied.
//  11. Selector succeeds for the SYNCING requester.
//  12. An unsuccessful subsequent selection preserves the matching durable
//     binding and state.
//
// The primary success scenario is configured as:
//
//	Requester: gossipgrid-1
//	First candidate: gossipgrid-0, authenticated but ineligible
//	Second candidate: gossipgrid-2, authenticated and eligible
func TestSelectorIntegration_RealMTLS(t *testing.T) {
	ca := newTestCA(t, "selector-integration-ca")
	meta := finalizedTestMeta()
	clusterID := meta.ClusterID
	epochID := meta.EpochID

	// --- First candidate: gossipgrid-0 (DEGRADED, not eligible) ---
	// The mTLS handshake succeeds (the certificate is valid) but the
	// observer rejects the observation (ineligible state/readiness).
	src0 := newmTLSSource(t, ca, "gossipgrid-0", clusterID, epochID, "DEGRADED", false, false)
	defer src0.stop()

	// --- Second candidate: gossipgrid-2 (READY, eligible) ---
	src2 := newmTLSSource(t, ca, "gossipgrid-2", clusterID, epochID, "READY", true, true)
	defer src2.stop()

	// --- Requester: gossipgrid-1 (unbound, DISCOVERING) ---
	reqCert, reqKey, reqTrust := testCredentials(t, ca, "gossipgrid-1")
	reqMgr := newSelectorTestManager(t, reqCert, reqKey, reqTrust, "gossipgrid-1")
	reqClient := NewClient(reqMgr)

	// Configure the peer client with both the ordinary EpochProvider and the
	// dedicated BootstrapEpochProvider, mirroring the production wiring.
	reqClient.SetEpochProvider(&selectorTestEpochProvider{ctrl: nil})              // placeholder, set after controller
	reqClient.SetBootstrapEpochProvider(&selectorTestBootstrapProvider{ctrl: nil}) // placeholder

	// The controller is in DISCOVERING (no binding, initialized metadata).
	ctrl, bindingStore := newSelectorTestController(t, "gossipgrid-1", "gossipgrid-0", meta)
	if ctrl.State() != lifecycle.DISCOVERING {
		t.Fatalf("expected DISCOVERING after Start; got %s", ctrl.State())
	}

	// Reconfigure the peer client providers with the actual controller.
	reqClient.SetEpochProvider(&selectorTestEpochProvider{ctrl: ctrl})
	reqClient.SetBootstrapEpochProvider(&selectorTestBootstrapProvider{ctrl: ctrl})

	// --- Step 2: Ordinary replication and bootstrap fetch denied before joining ---
	admission := ctrl.Admission()
	if d := admission.CheckReplicationOutbound(context.Background()); d.Allowed {
		t.Fatal("expected ordinary replication to be denied before joining (unbound, DISCOVERING)")
	}
	if d := admission.CheckBootstrapFetch(context.Background(), lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID}); d.Allowed {
		t.Fatal("expected bootstrap fetch to be denied before joining (unbound, DISCOVERING)")
	}

	// --- Discovery snapshot: two candidates (gossipgrid-0 first, gossipgrid-2 second) ---
	snap := lifecycle.DiscoverySnapshotView{
		Peers: []lifecycle.DiscoveryPeerView{
			{Hostname: "gossipgrid-0." + testSuffix, PodIdentity: "gossipgrid-0", Address: src0.peerAddr},
			{Hostname: "gossipgrid-2." + testSuffix, PodIdentity: "gossipgrid-2", Address: src2.peerAddr},
			{Hostname: "gossipgrid-1." + testSuffix, PodIdentity: "gossipgrid-1", Address: "127.0.0.1:9999"},
		},
		LocalNode:          &lifecycle.DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	cfg := lifecycle.SelectorConfig{
		IdentitySuffix:    testSuffix,
		MaxCandidates:     10,
		SelectionBudget:   5 * time.Second,
		ObservationBudget: 2 * time.Second,
	}
	observer := NewBootstrapSourceObserver(reqClient)

	// --- Assert: the real SQLite store reports no binding before selection ---
	// The production store (store.New) must report nil binding for a
	// fresh, unbound database. Use the actual store.GetBinding result AND
	// error to verify the database is genuinely unbound (not a failed read).
	dbBinding, dbErr := bindingStore.GetBinding()
	if dbErr != nil {
		t.Fatalf("store.GetBinding before selection: unexpected error: %v", dbErr)
	}
	if dbBinding != nil {
		t.Fatalf("expected no binding before selection (unbound database); got %+v", dbBinding)
	}

	// --- Phase 1: select a bootstrap source ---
	sel, selErr := lifecycle.NewBootstrapSourceSelector(cfg, func() lifecycle.DiscoverySnapshotView { return snap }, observer, ctrl, "gossipgrid-1")
	if selErr != nil {
		t.Fatalf("NewBootstrapSourceSelector: %v", selErr)
	}
	result := sel.Select(context.Background())
	if result.Outcome != lifecycle.OutcomeSelected {
		t.Fatalf("expected OutcomeSelected; got %s (rejections=%+v)", result.Outcome, result.Rejections)
	}
	if result.Candidate == nil {
		t.Fatal("selected result has no candidate")
	}
	// Step 4: The eligible source (gossipgrid-2) must be selected.
	// gossipgrid-0 (DEGRADED, not eligible) must have been rejected first.
	if result.Candidate.PodIdentity != "gossipgrid-2" {
		t.Fatalf("expected gossipgrid-2 selected; got %s", result.Candidate.PodIdentity)
	}
	if result.Candidate.ExpectedIdentity != "gossipgrid-2."+testSuffix {
		t.Fatalf("expected identity gossipgrid-2.%s; got %s", testSuffix, result.Candidate.ExpectedIdentity)
	}
	// Step 3: Verify the rejection of gossipgrid-0 (authenticated but
	// ineligible). The mTLS handshake succeeds (the certificate is valid
	// and matches the expected identity); the rejection is for
	// INELIGIBILITY (DEGRADED state, not eligible, no readiness), NOT a
	// certificate-identity mismatch. A cert mismatch would produce an
	// error from the observer (the handshake fails), not a validation
	// rejection of an observed result.
	if len(result.Rejections) < 1 {
		t.Fatalf("expected at least 1 rejection (gossipgrid-0 ineligible); got %d", len(result.Rejections))
	}
	if result.Rejections[0].Candidate.PodIdentity != "gossipgrid-0" {
		t.Fatalf("expected first rejection to be gossipgrid-0; got %s", result.Rejections[0].Candidate.PodIdentity)
	}
	// The rejection reason must mention the ineligible state or readiness
	// (e.g., "DEGRADED", "not report bootstrap source eligibility",
	// "readiness"), NOT a certificate-identity mismatch (which would
	// indicate a different error path: "certificate is valid for" or
	// "x509").
	reason := result.Rejections[0].Reason
	t.Logf("gossipgrid-0 rejected: %s", reason)
	isIneligibility := false
	for _, kw := range []string{"DEGRADED", "eligibility", "readiness", "state", "not report"} {
		if strings.Contains(reason, kw) {
			isIneligibility = true
			break
		}
	}
	isCertMismatch := false
	for _, kw := range []string{"certificate is valid for", "x509", "certificate mismatch"} {
		if strings.Contains(reason, kw) {
			isCertMismatch = true
			break
		}
	}
	if !isIneligibility || isCertMismatch {
		t.Fatalf("expected an ineligibility rejection (not a cert mismatch); got %q", reason)
	}

	// --- Step 5: Selection alone must not create a binding or change state ---
	// Use the actual store.GetBinding result AND error to verify the
	// database is still unbound after selection alone.
	dbBinding2, dbErr2 := bindingStore.GetBinding()
	if dbErr2 != nil {
		t.Fatalf("store.GetBinding after selection: unexpected error: %v", dbErr2)
	}
	if dbBinding2 != nil {
		t.Fatal("selection created a binding in the real SQLite store (it must not)")
	}
	if ctrl.State() != lifecycle.DISCOVERING {
		t.Fatalf("selection changed lifecycle state: %s → %s", lifecycle.DISCOVERING, ctrl.State())
	}

	// --- Steps 6 & 7: AuthorizeJoin using the selected source ---
	// Wrap the observer with a request counter to verify that AuthorizeJoin
	// independently observes the source (a separate observation from the
	// selector's own observation).
	var joinObsCount int
	countingObserver := &countingObserver{
		inner: observer,
		count: &joinObsCount,
	}
	if err := ctrl.AuthorizeJoin(context.Background(), countingObserver, result.Candidate.DialAddr, result.Candidate.ExpectedIdentity); err != nil {
		t.Fatalf("AuthorizeJoin: %v", err)
	}
	if ctrl.State() != lifecycle.SYNCING {
		t.Fatalf("expected SYNCING after AuthorizeJoin; got %s", ctrl.State())
	}
	// Verify AuthorizeJoin independently observed the source (the counter
	// must be incremented).
	if joinObsCount != 1 {
		t.Fatalf("expected AuthorizeJoin to independently observe the source (count=%d); got %d", 1, joinObsCount)
	}
	// Use the actual store.GetBinding result AND error to verify the
	// correct persisted join binding after AuthorizeJoin.
	dbBinding3, dbErr3 := bindingStore.GetBinding()
	if dbErr3 != nil {
		t.Fatalf("store.GetBinding after AuthorizeJoin: unexpected error: %v", dbErr3)
	}
	if dbBinding3 == nil {
		t.Fatal("expected a join binding in the real SQLite store after AuthorizeJoin")
	}
	if dbBinding3.Provenance != lifecycle.ProvenanceJoin {
		t.Fatalf("expected join provenance in the real SQLite store; got %s", dbBinding3.Provenance)
	}
	if dbBinding3.ClusterID != clusterID || dbBinding3.EpochID != epochID {
		t.Fatalf("binding cluster/epoch mismatch in the real SQLite store: %s/%s", dbBinding3.ClusterID, dbBinding3.EpochID)
	}

	// --- Step 8: Public writes and ordinary replication remain denied ---
	if d := admission.CheckReplicationOutbound(context.Background()); d.Allowed {
		t.Fatal("expected ordinary replication (outbound) to be denied (SYNCING state)")
	}
	if d := admission.CheckWrite(context.Background()); d.Allowed {
		t.Fatal("expected public writes to be denied (SYNCING state)")
	}

	// --- Step 9: Dedicated bootstrap-fetch admission permits the matching context ---
	bootstrapReqCtx := lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID}
	if d := admission.CheckBootstrapFetch(context.Background(), bootstrapReqCtx); !d.Allowed {
		t.Fatalf("expected bootstrap fetch to be permitted (SYNCING, matching join binding); got denied: %s", d.Reason)
	}

	// --- Step 10: Bootstrap-source admission, inbound replication, and premature SetReady denied ---
	if d := admission.CheckBootstrapSourceInbound(context.Background(), bootstrapReqCtx); d.Allowed {
		t.Fatal("expected bootstrap-source admission to be denied (SYNCING, not READY)")
	}
	// Inbound replication is denied for a SYNCING replica (not authorized
	// to serve authoritative data).
	if d := admission.CheckReplicationInbound(context.Background(), bootstrapReqCtx); d.Allowed {
		t.Fatal("expected inbound replication to be denied (SYNCING state, not authorized to serve)")
	}
	// Premature SetReady (initial promotion from non-DEGRADED) must fail:
	// the replica is in SYNCING (not DEGRADED), and the initial promotion
	// requires genesis provenance (a join-provenance binding does not
	// carry genesis provenance). The SetReady call must return an error.
	if err := ctrl.SetReady("integration-test"); err == nil {
		t.Fatal("expected SetReady to return an error (SYNCING, not DEGRADED, no genesis provenance)")
	}
	// After the premature SetReady rejection, the requester must remain
	// in SYNCING (the rejected SetReady must not change the state).
	if ctrl.State() != lifecycle.SYNCING {
		t.Fatalf("expected SYNCING after premature SetReady rejection; got %s", ctrl.State())
	}

	// --- Step 11: Selector succeeds for the SYNCING requester ---
	sel2, selErr2 := lifecycle.NewBootstrapSourceSelector(cfg, func() lifecycle.DiscoverySnapshotView { return snap }, observer, ctrl, "gossipgrid-1")
	if selErr2 != nil {
		t.Fatalf("NewBootstrapSourceSelector (sel2): %v", selErr2)
	}
	result2 := sel2.Select(context.Background())
	if result2.Outcome != lifecycle.OutcomeSelected {
		t.Fatalf("expected OutcomeSelected for SYNCING requester; got %s (rejections=%+v)", result2.Outcome, result2.Rejections)
	}
	if result2.Candidate == nil || result2.Candidate.PodIdentity != "gossipgrid-2" {
		t.Fatalf("expected gossipgrid-2 selected for SYNCING requester; got %+v", result2.Candidate)
	}

	// --- Step 12: Unsuccessful subsequent selection preserves binding and state ---
	// A snapshot where all candidates are ineligible: the selection fails but
	// the existing binding and SYNCING state are preserved.
	snapAllIneligible := lifecycle.DiscoverySnapshotView{
		Peers: []lifecycle.DiscoveryPeerView{
			{Hostname: "gossipgrid-0." + testSuffix, PodIdentity: "gossipgrid-0", Address: src0.peerAddr},
		},
		LocalNode:          &lifecycle.DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}
	sel3, selErr3 := lifecycle.NewBootstrapSourceSelector(cfg, func() lifecycle.DiscoverySnapshotView { return snapAllIneligible }, observer, ctrl, "gossipgrid-1")
	if selErr3 != nil {
		t.Fatalf("NewBootstrapSourceSelector (sel3): %v", selErr3)
	}
	result3 := sel3.Select(context.Background())
	// The unsuccessful subsequent selection must return exactly
	// OutcomeNoEligibleSource (all usable candidates attempted and rejected).
	if result3.Outcome != lifecycle.OutcomeNoEligibleSource {
		t.Fatalf("expected OutcomeNoEligibleSource (all candidates ineligible); got %s (rejections=%+v)", result3.Outcome, result3.Rejections)
	}
	// The entire persisted binding must be unchanged after the unsuccessful
	// selection (verified through the real SQLite store).
	dbBinding4, dbErr4 := bindingStore.GetBinding()
	if dbErr4 != nil {
		t.Fatalf("store.GetBinding after unsuccessful selection: unexpected error: %v", dbErr4)
	}
	if dbBinding4 == nil {
		t.Fatal("unsuccessful selection removed the persisted binding from the real SQLite store")
	}
	if dbBinding4.ClusterID != clusterID || dbBinding4.EpochID != epochID || dbBinding4.Provenance != lifecycle.ProvenanceJoin {
		t.Fatalf("unsuccessful selection changed the persisted binding: %+v", dbBinding4)
	}
	// Compare Generation and CreatedAt in addition to ClusterID/EpochID/Provenance.
	if dbBinding4.Generation != dbBinding3.Generation {
		t.Fatalf("unsuccessful selection changed the persisted binding Generation: %d → %d", dbBinding3.Generation, dbBinding4.Generation)
	}
	if !dbBinding4.CreatedAt.Equal(dbBinding3.CreatedAt) {
		t.Fatalf("unsuccessful selection changed the persisted binding CreatedAt: %v → %v", dbBinding3.CreatedAt, dbBinding4.CreatedAt)
	}
	if ctrl.State() != lifecycle.SYNCING {
		t.Fatalf("unsuccessful selection changed lifecycle state: %s", ctrl.State())
	}
}

// TestSelectorIntegration_UnsuccessfulUnbound verifies that a fully-unsuccessful
// selection (all candidates ineligible) for a new, unbound replica leaves it
// unbound and preserves its state.
func TestSelectorIntegration_UnsuccessfulUnbound(t *testing.T) {
	ca := newTestCA(t, "selector-unsuccessful-ca")
	meta := finalizedTestMeta()
	clusterID := meta.ClusterID
	epochID := meta.EpochID

	// An ineligible source for the "all candidates ineligible" case.
	srcIneligible := newmTLSSource(t, ca, "gossipgrid-2", clusterID, epochID, "DEGRADED", false, false)
	defer srcIneligible.stop()

	// --- Requester: gossipgrid-1 (unbound, DISCOVERING) ---
	reqCert, reqKey, reqTrust := testCredentials(t, ca, "gossipgrid-1")
	reqMgr := newSelectorTestManager(t, reqCert, reqKey, reqTrust, "gossipgrid-1")
	reqClient := NewClient(reqMgr)

	ctrl, bindingStore := newSelectorTestController(t, "gossipgrid-1", "gossipgrid-0", meta)
	if ctrl.State() != lifecycle.DISCOVERING {
		t.Fatalf("expected DISCOVERING after Start; got %s", ctrl.State())
	}
	reqClient.SetEpochProvider(&selectorTestEpochProvider{ctrl: ctrl})
	reqClient.SetBootstrapEpochProvider(&selectorTestBootstrapProvider{ctrl: ctrl})

	// All candidates point at the ineligible source. The mTLS handshake
	// succeeds (the certificate is valid) but the observer rejects the
	// observation (ineligible state/readiness).
	snapAllIneligible := lifecycle.DiscoverySnapshotView{
		Peers: []lifecycle.DiscoveryPeerView{
			{Hostname: "gossipgrid-2." + testSuffix, PodIdentity: "gossipgrid-2", Address: srcIneligible.peerAddr},
		},
		LocalNode:          &lifecycle.DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	cfg := lifecycle.SelectorConfig{
		IdentitySuffix:    testSuffix,
		MaxCandidates:     10,
		SelectionBudget:   5 * time.Second,
		ObservationBudget: 2 * time.Second,
	}
	observer := NewBootstrapSourceObserver(reqClient)

	sel, selErr := lifecycle.NewBootstrapSourceSelector(cfg, func() lifecycle.DiscoverySnapshotView { return snapAllIneligible }, observer, ctrl, "gossipgrid-1")
	if selErr != nil {
		t.Fatalf("NewBootstrapSourceSelector: %v", selErr)
	}
	result := sel.Select(context.Background())
	if result.Outcome != lifecycle.OutcomeNoEligibleSource {
		t.Fatalf("expected OutcomeNoEligibleSource; got %s (rejections=%+v)", result.Outcome, result.Rejections)
	}
	// The replica must remain unbound and in DISCOVERING.
	// Verify through the real SQLite store (not just the controller's cache).
	dbBinding, dbErr := bindingStore.GetBinding()
	if dbErr != nil {
		t.Fatalf("store.GetBinding after unsuccessful unbound selection: unexpected error: %v", dbErr)
	}
	if dbBinding != nil {
		t.Fatal("fully-unsuccessful selection created a binding in the real SQLite store (it must not)")
	}
	if ctrl.State() != lifecycle.DISCOVERING {
		t.Fatalf("fully-unsuccessful selection changed lifecycle state: %s → %s", lifecycle.DISCOVERING, ctrl.State())
	}
}

// TestSelectorIntegration_IdentityMismatch verifies that a real TLS handshake
// with a certificate-identity mismatch (the peer presents a certificate for a
// different identity than expected) is a distinct negative case from an
// ineligible-but-authenticated peer.
func TestSelectorIntegration_IdentityMismatch(t *testing.T) {
	ca := newTestCA(t, "selector-identity-mismatch-ca")
	meta := finalizedTestMeta()
	clusterID := meta.ClusterID
	epochID := meta.EpochID

	// A peer with a valid certificate for gossipgrid-2, but the selector
	// expects gossipgrid-0's identity: the mTLS handshake fails.
	src := newmTLSSource(t, ca, "gossipgrid-2", clusterID, epochID, "READY", true, true)
	defer src.stop()

	reqCert, reqKey, reqTrust := testCredentials(t, ca, "gossipgrid-1")
	reqMgr := newSelectorTestManager(t, reqCert, reqKey, reqTrust, "gossipgrid-1")
	reqClient := NewClient(reqMgr)

	ctrl, _ := newSelectorTestController(t, "gossipgrid-1", "gossipgrid-0", meta)
	reqClient.SetEpochProvider(&selectorTestEpochProvider{ctrl: ctrl})
	reqClient.SetBootstrapEpochProvider(&selectorTestBootstrapProvider{ctrl: ctrl})

	// The snapshot lists the peer under gossipgrid-0's identity, but the
	// actual certificate is for gossipgrid-2: identity mismatch.
	snap := lifecycle.DiscoverySnapshotView{
		Peers: []lifecycle.DiscoveryPeerView{
			{Hostname: "gossipgrid-0." + testSuffix, PodIdentity: "gossipgrid-0", Address: src.peerAddr},
		},
		LocalNode:          &lifecycle.DiscoveryPeerView{Hostname: "gossipgrid-1", PodIdentity: "gossipgrid-1"},
		ExpectedIdentities: []string{"gossipgrid-0", "gossipgrid-1", "gossipgrid-2"},
		Stale:              false,
		LastUpdated:        time.Now(),
		Mode:               "dns",
	}

	cfg := lifecycle.SelectorConfig{
		IdentitySuffix:    testSuffix,
		MaxCandidates:     10,
		SelectionBudget:   5 * time.Second,
		ObservationBudget: 2 * time.Second,
	}
	observer := NewBootstrapSourceObserver(reqClient)

	sel, selErr := lifecycle.NewBootstrapSourceSelector(cfg, func() lifecycle.DiscoverySnapshotView { return snap }, observer, ctrl, "gossipgrid-1")
	if selErr != nil {
		t.Fatalf("NewBootstrapSourceSelector: %v", selErr)
	}
	result := sel.Select(context.Background())
	if result.Outcome == lifecycle.OutcomeSelected {
		t.Fatal("expected a non-selected outcome (certificate identity mismatch)")
	}
	// The rejection reason should mention the identity mismatch.
	if len(result.Rejections) == 0 {
		t.Fatal("expected a rejection record for the identity mismatch")
	}
	t.Logf("identity mismatch rejected: %s", result.Rejections[0].Reason)
	// No binding created.
	if b := ctrl.LocalBinding(); b != nil {
		t.Fatal("identity-mismatch selection created a binding (it must not)")
	}
}
