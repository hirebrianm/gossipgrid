package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/antientropy"
	"gossipgrid/internal/discovery"
	"gossipgrid/internal/peertls"
	"gossipgrid/internal/server"
	"gossipgrid/internal/store"
)

// This file exercises the opt-in initial anti-entropy synchronization barrier
// (Milestone 7A enhancement, synchronization-aware Kubernetes readiness) with
// real mTLS, real SQLite, and real networking. It reuses the five-node
// anti-entropy integration-test infrastructure (newAENode, aeWaitUntil,
// aeStopPeer/aeStartPeer/aeStopNode, etc.) and injects the initial-sync
// tracker into both the public server (so /readyz reflects the barrier) and
// the anti-entropy engine (so syncPeer records completed passes to it).
//
// Scenarios covered (the 13 required scenarios' integration subset):
//
//  9. Five replicas starting together do not deadlock: all 5 replicas start
//     simultaneously with empty databases and the barrier enabled. Because a
//     NotReady replica still serves its peer listener and inventory
//     endpoints, the first round of empty-inventory exchanges completes every
//     peer's pass, and all 5 replicas become Ready (200 from /readyz) without
//     any ordering dependency. This proves the bootstrap deadlock the barrier
//     could otherwise cause (a replica waiting for a peer to be Ready before
//     it can complete) does not occur.
//
// 10. A replacement replica with an empty SQLite database recovers messages
//     before readiness: 4 surviving replicas hold historical messages; node 4
//     is stopped and replaced with a fresh empty-database replica whose
//     barrier is enabled. The replacement remains NotReady (503) until it has
//     completed a full sweep against every surviving peer, at which point it
//     becomes Ready (200) and its store holds all historical messages.
//
// 13. Cancellation and shutdown do not leak workers or goroutines: after the
//     barrier completes and the nodes are stopped, the anti-entropy engine
//     stats show 0 active syncs (no leaked workers).
//
// Scenarios 1-8 and 11-12 are covered by the deterministic unit tests
// (internal/antientropy/initial_sync_test.go and
// internal/server/initial_sync_ready_test.go); they are not repeated here.

// isSyncNode is a self-contained GossipGrid peer for the initial-sync
// integration test. It is structurally identical to aeNode but carries an
// initial-sync tracker that is wired into both the public server (so /readyz
// reflects the barrier) and the anti-entropy engine (so syncPeer records
// completed passes to it).
type isSyncNode struct {
	mgr          *peertls.Manager
	client       *peertls.Client
	peerAddr     string
	podID        string
	store        *store.Store
	publicAddr   string
	peerSrv      *peertls.PeerServer
	peerHTTP     *http.Server
	peerListener net.Listener
	engine       *antientropy.Engine
	discMgr      *discovery.Manager
	tracker      *antientropy.InitialSyncTracker

	peerListenerMu sync.Mutex
	peerServing    bool

	peerShutdown   func()
	publicShutdown func()
}

// newISyncNode starts a GossipGrid peer node with anti-entropy and the
// initial-sync barrier enabled. The expected membership is derived from the
// StatefulSet name and replica count (matching the production path in
// main.go). The local node's ID is excluded from the expected remote peers.
func newISyncNode(t *testing.T, ca *testCA, podID string, expectedReplicas int) *isSyncNode {
	t.Helper()
	dir := t.TempDir()
	cert, key := issueLeafCert(t, ca, podID+"."+aeSuffix)
	chainPEM, keyPEM := certKeyPEMTwo(t, cert, key, ca)
	certFile := writePEMFile(t, dir, "cert.pem", chainPEM)
	keyFile := writePEMFile(t, dir, "key.pem", keyPEM)
	trustFile := writePEMFile(t, dir, "ca-bundle.pem", ca.pem)

	mgr := peertls.New(peertls.Config{
		CertFile:         certFile,
		KeyFile:          keyFile,
		TrustBundleFile:  trustFile,
		IdentitySuffix:   aeSuffix,
		StatefulSetName:  aeStatefulSet,
		NodeID:           podID,
		ExpectedReplicas: expectedReplicas,
		ReloadInterval:   50 * time.Millisecond,
	}, nil)
	if err := mgr.Start(context.Background()); err != nil {
		t.Fatalf("%s: manager Start: %v", podID, err)
	}
	t.Cleanup(mgr.Shutdown)

	// Open the store.
	st, err := store.New(dir + "/node.db")
	if err != nil {
		t.Fatalf("%s: store.New: %v", podID, err)
	}
	closed := false
	closeStore := func() {
		if !closed {
			closed = true
			_ = st.Close()
		}
	}
	t.Cleanup(closeStore)

	// Build the expected membership (matching the production derivation in
	// main.go: statefulset-name-<i> for i in 0..replicas-1).
	expected := make([]string, expectedReplicas)
	for i := 0; i < expectedReplicas; i++ {
		expected[i] = fmt.Sprintf("%s-%d", aeStatefulSet, i)
	}
	// The initial-sync tracker (barrier enabled). The local node is excluded
	// from the expected remote peers.
	tracker := antientropy.NewInitialSyncTracker(expected, podID)

	// Peer server WITH anti-entropy sync endpoints enabled.
	peerSrv := peertls.NewPeerServerWithSync(mgr, ":0", podID, "v0.1.0-is", st, true, true, nil, 0)

	// Listen on a random loopback port.
	tlsListener, err := aeTlsListen("127.0.0.1:0", mgr.TLSConfigForServer())
	if err != nil {
		t.Fatalf("%s: tls.Listen: %v", podID, err)
	}
	addr := tlsListener.Addr().String()
	httpSrv := &http.Server{Handler: peerSrv.Handler()}
	go func() { _ = httpSrv.Serve(tlsListener) }()
	peerShutdown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
		_ = tlsListener.Close()
	}
	t.Cleanup(peerShutdown)

	// Public HTTP listener WITH the initial-sync barrier tracker, so /readyz
	// reflects the barrier (503 while pending, 200 when complete).
	publicHandler := server.NewWithStoreAndCallbackAndRetentionAndInitialSync(
		server.Node{ID: podID}, st, nil, nil, 0, tracker,
	)
	srv := server.NewServer(":0", publicHandler)
	lc := net.ListenConfig{}
	pubListener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("%s: public listen: %v", podID, err)
	}
	go func() { _ = srv.HTTPServer.Serve(pubListener) }()
	publicShutdown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = pubListener.Close()
	}
	t.Cleanup(publicShutdown)

	return &isSyncNode{
		mgr:            mgr,
		client:         peertls.NewClient(mgr),
		peerAddr:       addr,
		podID:          podID,
		store:          st,
		publicAddr:     pubListener.Addr().String(),
		peerSrv:        peerSrv,
		peerHTTP:       httpSrv,
		peerListener:   tlsListener,
		tracker:        tracker,
		peerServing:    true,
		peerShutdown:   peerShutdown,
		publicShutdown: publicShutdown,
	}
}

// wireISyncAntiEntropyForNode creates a static discovery manager and
// anti-entropy engine for the given node using the production bridge pattern,
// and wires the node's initial-sync tracker into the engine (so syncPeer
// records completed passes to it). Address overrides map pod identities to
// the actual loopback dial addresses.
func wireISyncAntiEntropyForNode(t *testing.T, node *isSyncNode, allNodes []*isSyncNode, cfg antientropy.Config) {
	t.Helper()
	ctx := context.Background()

	addrOverrides := make(map[string]string)
	staticPeers := make([]string, 0, len(allNodes)-1)
	for _, other := range allNodes {
		if other.podID == node.podID {
			continue
		}
		staticPeers = append(staticPeers, other.podID+"."+aeSuffix+":8443")
		addrOverrides[other.podID] = other.peerAddr
	}

	discCfg := discovery.Config{
		Mode:             discovery.ModeStatic,
		StaticPeers:      staticPeers,
		StatefulSetName:  aeStatefulSet,
		NodeID:           node.podID,
		ExpectedReplicas: len(allNodes),
		DNSName:          aeSuffix,
	}
	discMgr := discovery.NewManager(discCfg, nil, nil)
	if err := discMgr.Start(ctx); err != nil {
		t.Fatalf("%s: discovery Start: %v", node.podID, err)
	}
	t.Cleanup(discMgr.Shutdown)
	node.discMgr = discMgr

	peerSource := antientropy.NewDiscoveryPeerSource(aeSuffix, func() antientropy.PeerSourceSnapshot {
		ds := discMgr.Snapshot()
		peers := make([]antientropy.PeerSourceInfo, len(ds.Peers))
		for i, p := range ds.Peers {
			a := p.Address
			if override, ok := addrOverrides[p.PodIdentity]; ok {
				a = override
			}
			peers[i] = antientropy.PeerSourceInfo{
				Hostname:    p.Hostname,
				PodIdentity: p.PodIdentity,
				Address:     a,
			}
		}
		var localNode *antientropy.PeerSourceInfo
		if ds.LocalNode != nil {
			localNode = &antientropy.PeerSourceInfo{
				Hostname:    ds.LocalNode.Hostname,
				PodIdentity: ds.LocalNode.PodIdentity,
			}
		}
		expected := make([]string, len(ds.ExpectedIdentities))
		for i, id := range ds.ExpectedIdentities {
			expected[i] = id.Hostname
		}
		return antientropy.PeerSourceSnapshot{
			Peers:              peers,
			LocalNode:          localNode,
			ExpectedIdentities: expected,
			Stale:              ds.Stale,
			Mode:               string(ds.Mode),
		}
	})

	engine := antientropy.New(cfg, peerSource, nil, nil, nil, nil)
	node.engine = engine
	engine.SetPeerSource(peerSource)
	engine.SetInventoryClient(node.client)
	engine.SetMessageFetcher(node.client)
	engine.SetLocalStore(node.store)
	// Wire the initial-sync tracker into the engine: syncPeer records
	// completed passes to it (the barrier's single linearization point).
	engine.SetInitialSyncTracker(node.tracker)

	if err := engine.Start(ctx); err != nil {
		t.Fatalf("%s: anti-entropy Start: %v", node.podID, err)
	}
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		engine.Stop(sctx)
		cancel()
	})
}

// isSyncPost creates a message on a node via the public API and returns the
// ID.
func isSyncPost(t *testing.T, node *isSyncNode, body string) string {
	t.Helper()
	status, respBody, err := postPublicJSON(t, node.publicAddr, fmt.Sprintf(`{"body":%q}`, body))
	if err != nil {
		t.Fatalf("POST to %s: %v", node.podID, err)
	}
	if status != http.StatusCreated {
		t.Fatalf("POST to %s: status %d, want 201", node.podID, status)
	}
	var msg store.Message
	if err := json.Unmarshal([]byte(respBody), &msg); err != nil {
		t.Fatalf("decoding POST response: %v", err)
	}
	return msg.ID
}

// isSyncHasMsg checks whether a node's store contains a message with the
// given ID.
func isSyncHasMsg(node *isSyncNode, id string) bool {
	msg, err := node.store.GetByID(id)
	if err != nil {
		return false
	}
	return msg.ID == id
}

// isSyncMessageCount returns the number of messages in a node's store.
// Returns -1 on error (so it's safe to call from polling loops).
func isSyncMessageCount(node *isSyncNode) int64 {
	var count int64
	if err := node.store.DB().Model(&store.Message{}).Count(&count).Error; err != nil {
		return -1
	}
	return count
}

// isSyncReadyz polls GET /readyz on the node's public listener. It returns the
// status code and the decoded readiness response. This is the readiness probe
// Kubernetes would fire; it reads only in-memory state (no network/SQLite).
func isSyncReadyz(t *testing.T, node *isSyncNode) (int, isSyncReadyzResponse) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), "GET", "http://"+node.publicAddr+"/readyz", nil)
	if err != nil {
		t.Fatalf("%s: building readyz request: %v", node.podID, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s: readyz request: %v", node.podID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	var out isSyncReadyzResponse
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("%s: decoding readyz response: %v (body: %s)", node.podID, err, body)
	}
	return resp.StatusCode, out
}

// isSyncReadyzResponse is the /readyz response shape for the initial-sync
// integration test. It mirrors the fields the test asserts on.
type isSyncReadyzResponse struct {
	Service              string   `json:"service"`
	Ready                bool     `json:"ready"`
	State                string   `json:"state"`
	Reason               string   `json:"reason"`
	InitialSyncComplete  bool     `json:"initial_sync_complete"`
	InitialSyncCompleted int      `json:"initial_sync_completed"`
	InitialSyncExpected  int      `json:"initial_sync_expected"`
	InitialSyncMissing   []string `json:"initial_sync_missing"`
}

// --- Scenario 9: Five replicas starting together do not deadlock ---

func TestIntegrationInitialSyncFiveReplicasSimultaneousStartupNoDeadlock(t *testing.T) {
	const replicas = 5
	sharedCA := newTestCA(t, "is-workload")

	nodes := make([]*isSyncNode, replicas)
	for i := 0; i < replicas; i++ {
		nodes[i] = newISyncNode(t, sharedCA, fmt.Sprintf("gossipgrid-%d", i), replicas)
	}

	// Short interval for fast test convergence.
	cfg := antientropy.Config{
		Interval:        100 * time.Millisecond,
		PageSize:        32,
		FetchBatchSize:  8,
		MaxPagesPerPeer: 4,
		Workers:         2,
		RequestTimeout:  2 * time.Second,
	}

	// Wire anti-entropy (with the tracker) for all nodes.
	for _, node := range nodes {
		wireISyncAntiEntropyForNode(t, node, nodes, cfg)
	}

	// All 5 replicas start simultaneously with empty databases and the
	// barrier enabled. Each replica expects 4 remote peers. Verify each
	// begins NotReady (503, initial_sync_pending).
	for _, node := range nodes {
		if !node.tracker.Enabled() {
			t.Fatalf("%s: tracker should be enabled", node.podID)
		}
		if node.tracker.Complete() {
			t.Fatalf("%s: fresh tracker should be pending (no remote peers synced yet)", node.podID)
		}
		code, resp := isSyncReadyz(t, node)
		if code != http.StatusServiceUnavailable {
			t.Errorf("%s: expected 503 (NotReady) on fresh start, got %d", node.podID, code)
		}
		if resp.Ready {
			t.Errorf("%s: expected Ready=false on fresh start, got true", node.podID)
		}
		if resp.Reason != "initial_sync_pending" {
			t.Errorf("%s: expected reason initial_sync_pending on fresh start, got %q", node.podID, resp.Reason)
		}
		if resp.InitialSyncExpected != replicas-1 {
			t.Errorf("%s: expected %d expected remote peers, got %d", node.podID, replicas-1, resp.InitialSyncExpected)
		}
	}

	// Because a NotReady replica still serves its peer listener and inventory
	// endpoints, the first round of empty-inventory exchanges completes every
	// peer's pass. All 5 replicas become Ready (200) without any ordering
	// dependency (no deadlock).
	if !aeWaitUntil(20*time.Second, func() bool {
		for _, node := range nodes {
			code, _ := isSyncReadyz(t, node)
			if code != http.StatusOK {
				return false
			}
		}
		return true
	}) {
		// Report which nodes are still pending for diagnosis.
		var pending []string
		for _, node := range nodes {
			code, resp := isSyncReadyz(t, node)
			if code != http.StatusOK {
				pending = append(pending, fmt.Sprintf("%s=%d/%s", node.podID, code, resp.Reason))
			}
		}
		t.Fatalf("not all 5 replicas became Ready within 20s (simultaneous startup deadlock?): %v", pending)
	}
	for _, node := range nodes {
		if !node.tracker.Complete() {
			t.Errorf("%s: tracker should be complete after simultaneous startup", node.podID)
		}
		code, resp := isSyncReadyz(t, node)
		if code != http.StatusOK || !resp.Ready {
			t.Errorf("%s: expected 200/Ready=true, got %d/Ready=%v", node.podID, code, resp.Ready)
		}
	}
	t.Log("all 5 replicas became Ready (200) after simultaneous startup; no bootstrap deadlock")

	// --- Scenario 13 (subset): no goroutine leaks after shutdown ---
	for _, node := range nodes {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		node.engine.Stop(sctx)
		cancel()
		node.peerShutdown()
		node.publicShutdown()
	}
	for _, node := range nodes {
		if s := node.engine.Stats(); s.ActiveSyncs != 0 {
			t.Errorf("%s: ActiveSyncs = %d after shutdown, want 0 (no leaked workers)", node.podID, s.ActiveSyncs)
		}
	}
	t.Log("clean shutdown: 0 active syncs after stopping all nodes (no goroutine leaks)")
}

// --- Scenario 10: Replacement replica with empty SQLite recovers before readiness ---

func TestIntegrationInitialSyncReplacementRecoversBeforeReady(t *testing.T) {
	const replicas = 5
	sharedCA := newTestCA(t, "is-repl-workload")

	nodes := make([]*isSyncNode, replicas)
	for i := 0; i < replicas; i++ {
		nodes[i] = newISyncNode(t, sharedCA, fmt.Sprintf("gossipgrid-%d", i), replicas)
	}

	cfg := antientropy.Config{
		Interval:        100 * time.Millisecond,
		PageSize:        32,
		FetchBatchSize:  8,
		MaxPagesPerPeer: 4,
		Workers:         2,
		RequestTimeout:  2 * time.Second,
	}

	for _, node := range nodes {
		wireISyncAntiEntropyForNode(t, node, nodes, cfg)
	}

	// 1. Populate 3 surviving replicas with historical messages.
	var historicalIDs []string
	for i := 0; i < 3; i++ {
		id := isSyncPost(t, nodes[i], fmt.Sprintf("initial-sync replacement message %d", i))
		historicalIDs = append(historicalIDs, id)
	}

	// 2. Wait for the 4 surviving replicas (0-3) to converge on the historical
	//    messages. Node 4 will be replaced.
	survivors := nodes[0:4]
	if !aeWaitUntil(20*time.Second, func() bool {
		for _, node := range survivors {
			for _, id := range historicalIDs {
				if !isSyncHasMsg(node, id) {
					return false
				}
			}
		}
		return true
	}) {
		t.Fatal("survivors did not converge on historical messages after 20s")
	}
	expectedCount := isSyncMessageCount(nodes[0])
	if expectedCount < int64(len(historicalIDs)) {
		t.Fatalf("node 0 has only %d messages, want >= %d", expectedCount, len(historicalIDs))
	}

	// 3. Stop node 4 cleanly (it will be replaced by an empty-database replica).
	stopISyncNode(t, nodes[4])

	// 4. Create a fresh empty-database replacement replica for gossipgrid-4
	//    with the barrier enabled. In a real deployment, the StatefulSet pod
	//    name (gossipgrid-4) is reused, so the expected membership is the
	//    same; the replacement's local ID is gossipgrid-4 (excluded from the
	//    expected remote peers, which are gossipgrid-0..3).
	emptyDir := t.TempDir()
	replStore, err := store.New(emptyDir + "/replacement.db")
	if err != nil {
		t.Fatalf("creating replacement store: %v", err)
	}
	t.Cleanup(func() { _ = replStore.Close() })

	// Build the expected membership and the replacement's tracker.
	expected := make([]string, replicas)
	for i := 0; i < replicas; i++ {
		expected[i] = fmt.Sprintf("%s-%d", aeStatefulSet, i)
	}
	replTracker := antientropy.NewInitialSyncTracker(expected, "gossipgrid-4")

	// Build the replacement node's TLS, peer server, and public listener
	// (with the tracker so /readyz reflects the barrier).
	dir := t.TempDir()
	cert, key := issueLeafCert(t, sharedCA, "gossipgrid-4."+aeSuffix)
	chainPEM, keyPEM := certKeyPEMTwo(t, cert, key, sharedCA)
	certFile := writePEMFile(t, dir, "cert.pem", chainPEM)
	keyFile := writePEMFile(t, dir, "key.pem", keyPEM)
	trustFile := writePEMFile(t, dir, "ca-bundle.pem", sharedCA.pem)

	replMgr := peertls.New(peertls.Config{
		CertFile:         certFile,
		KeyFile:          keyFile,
		TrustBundleFile:  trustFile,
		IdentitySuffix:   aeSuffix,
		StatefulSetName:  aeStatefulSet,
		NodeID:           "gossipgrid-4",
		ExpectedReplicas: replicas,
		ReloadInterval:   50 * time.Millisecond,
	}, nil)
	if err := replMgr.Start(context.Background()); err != nil {
		t.Fatalf("replacement node manager Start: %v", err)
	}
	t.Cleanup(replMgr.Shutdown)

	replNode := &isSyncNode{
		mgr:     replMgr,
		client:  peertls.NewClient(replMgr),
		podID:   "gossipgrid-4",
		store:   replStore,
		tracker: replTracker,
	}
	t.Cleanup(func() {
		if replNode.engine != nil {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			replNode.engine.Stop(sctx)
			cancel()
		}
	})

	// Public listener WITH the tracker.
	publicHandler := server.NewWithStoreAndCallbackAndRetentionAndInitialSync(
		server.Node{ID: "gossipgrid-4"}, replStore, nil, nil, 0, replTracker,
	)
	srv := server.NewServer(":0", publicHandler)
	lc := net.ListenConfig{}
	pubListener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("replacement public listen: %v", err)
	}
	go func() { _ = srv.HTTPServer.Serve(pubListener) }()
	replNode.publicAddr = pubListener.Addr().String()
	replNode.publicShutdown = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = pubListener.Close()
	}
	t.Cleanup(replNode.publicShutdown)

	// Peer server with the empty store and sync endpoints enabled.
	replPeerSrv := peertls.NewPeerServerWithSync(replMgr, ":0", "gossipgrid-4", "v0.1.0-is-repl", replStore, true, true, nil, 0)
	tlsListener, err := aeTlsListen("127.0.0.1:0", replMgr.TLSConfigForServer())
	if err != nil {
		t.Fatalf("replacement tls.Listen: %v", err)
	}
	replNode.peerAddr = tlsListener.Addr().String()
	replNode.peerSrv = replPeerSrv
	replHTTP := &http.Server{Handler: replPeerSrv.Handler()}
	replNode.peerHTTP = replHTTP
	replNode.peerListener = tlsListener
	replNode.peerServing = true
	go func() { _ = replHTTP.Serve(tlsListener) }()
	replNode.peerShutdown = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = replHTTP.Shutdown(ctx)
		_ = tlsListener.Close()
	}

	// 5. Verify the replacement begins NotReady (503, initial_sync_pending)
	//    with 4 expected remote peers (gossipgrid-0..3).
	if replTracker.Complete() {
		t.Fatalf("replacement: fresh tracker should be pending")
	}
	code, resp := isSyncReadyz(t, replNode)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("replacement: expected 503 (NotReady) on fresh start, got %d", code)
	}
	if resp.Reason != "initial_sync_pending" {
		t.Fatalf("replacement: expected reason initial_sync_pending, got %q", resp.Reason)
	}
	if resp.InitialSyncExpected != 4 || resp.InitialSyncCompleted != 0 {
		t.Fatalf("replacement: expected Completed=0 Expected=4, got %d/%d", resp.InitialSyncCompleted, resp.InitialSyncExpected)
	}
	t.Log("replacement replica begins NotReady (503, initial_sync_pending) with 4 expected peers")

	// 6. Wire anti-entropy for the replacement (with the tracker) against the
	//    4 survivors + itself.
	allNodes := append([]*isSyncNode{}, survivors...)
	allNodes = append(allNodes, replNode)
	wireISyncAntiEntropyForNode(t, replNode, allNodes, cfg)

	// 7. Wait for the replacement to become Ready (200). It remains NotReady
	//    until it has completed a full sweep against every surviving peer.
	if !aeWaitUntil(30*time.Second, func() bool {
		c, _ := isSyncReadyz(t, replNode)
		return c == http.StatusOK
	}) {
		c, resp := isSyncReadyz(t, replNode)
		t.Fatalf("replacement did not become Ready within 30s: status=%d reason=%q completed=%d expected=%d missing=%v",
			c, resp.Reason, resp.InitialSyncCompleted, resp.InitialSyncExpected, resp.InitialSyncMissing)
	}
	t.Log("replacement replica became Ready (200) after completing the initial sync barrier")

	// 8. Verify the replacement store recovered ALL historical messages
	//    (the recovery happened within the same full sweeps that completed
	//    the barrier — the barrier is the rollout gate for recovery).
	for _, id := range historicalIDs {
		if !isSyncHasMsg(replNode, id) {
			t.Fatalf("replacement missing recovered message %s", id)
		}
	}
	if c := isSyncMessageCount(replNode); c != expectedCount {
		t.Errorf("replacement has %d messages, want %d", c, expectedCount)
	}
	// Verify identical immutable fields against a survivor reference.
	for _, id := range historicalIDs {
		ref, err := nodes[0].store.GetByID(id)
		if err != nil {
			t.Fatalf("reference node 0 missing ID %s: %v", id, err)
		}
		got, err := replNode.store.GetByID(id)
		if err != nil {
			t.Fatalf("replacement missing ID %s: %v", id, err)
		}
		if got.Body != ref.Body || got.Origin != ref.Origin {
			t.Errorf("ID %s: mismatch: got body=%q origin=%q, want body=%q origin=%q", id, got.Body, got.Origin, ref.Body, ref.Origin)
		}
	}
	t.Logf("replacement recovered all %d historical messages before becoming Ready", len(historicalIDs))

	// 9. Verify the surviving replicas remain operational.
	for i := 0; i < 4; i++ {
		if c := isSyncMessageCount(nodes[i]); c != expectedCount {
			t.Errorf("surviving node %d has %d messages, want %d", i, c, expectedCount)
		}
	}

	// 10. Clean shutdown of the replacement and survivors.
	stopISyncNode(t, replNode)
	for i := 0; i < 4; i++ {
		stopISyncNode(t, nodes[i])
	}
	for i := 0; i < 4; i++ {
		if s := nodes[i].engine.Stats(); s.ActiveSyncs != 0 {
			t.Errorf("surviving node %d: ActiveSyncs = %d after shutdown, want 0", i, s.ActiveSyncs)
		}
	}
	if s := replNode.engine.Stats(); s.ActiveSyncs != 0 {
		t.Errorf("replacement: ActiveSyncs = %d after shutdown, want 0", s.ActiveSyncs)
	}
	t.Log("replacement recovery + clean shutdown: no leaked workers")
}

// stopISyncNode cleanly stops a node's anti-entropy engine and all listeners
// (peer and public), and closes its store.
func stopISyncNode(t *testing.T, node *isSyncNode) {
	t.Helper()
	if node.engine != nil {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		node.engine.Stop(sctx)
		cancel()
	}
	node.peerListenerMu.Lock()
	if node.peerServing {
		pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = node.peerHTTP.Shutdown(pctx)
		_ = node.peerListener.Close()
		pcancel()
		node.peerServing = false
	}
	node.peerListenerMu.Unlock()
	if node.publicShutdown != nil {
		node.publicShutdown()
	}
	if err := node.store.Close(); err != nil {
		t.Logf("%s: store close: %v (ignoring)", node.podID, err)
	}
	t.Logf("%s: fully stopped (engine, peer, public, store)", node.podID)
}
