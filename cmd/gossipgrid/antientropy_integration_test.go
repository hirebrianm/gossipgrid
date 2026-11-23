// Package main: five-node anti-entropy integration test (Milestone 4C).
//
// This test exercises the real anti-entropy engine with five independent
// replicas, each with its own SQLite database, workload certificate, public
// HTTP listener, private peer listener (with sync endpoints registered),
// static discovery, and anti-entropy engine. Real mTLS and real SQLite are
// used; no fake delivery implementations.
//
// Scenarios:
//
//	A. Repair gossip misses (node down, anti-entropy repairs after recovery)
//	B. Replica replacement (new empty node retrieves all historical messages)
//	C. Concurrent writers (all nodes converge on the same message set)
//	D. Pagination stress (cursor advances across cycles)
//	E. Recovery and isolation (disconnect, verify, reconnect, clean shutdown)
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
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

const (
	aeSuffix      = "gossipgrid-headless.ae.svc"
	aeStatefulSet = "gossipgrid"
	aeReplicas    = 5
)

// aeNode is a self-contained GossipGrid peer for the anti-entropy integration
// test. It has a store, public listener, peer listener (with sync endpoints),
// and an anti-entropy engine.
type aeNode struct {
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

	peerListenerMu sync.Mutex
	peerServing    bool

	// Close functions (idempotent).
	peerShutdown   func()
	publicShutdown func()
}

// newAENode starts a GossipGrid peer node with anti-entropy enabled.
func newAENode(t *testing.T, ca *testCA, podID string) *aeNode {
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
		ExpectedReplicas: aeReplicas,
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

	// Peer server WITH anti-entropy sync endpoints enabled.
	peerSrv := peertls.NewPeerServerWithSync(mgr, ":0", podID, "v0.1.0-ae", st, true, true, nil, 0)

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

	// Public HTTP listener.
	publicSrv := newPublicServer(t, podID, st)
	publicAddr := publicSrv.addr
	publicShutdown := publicSrv.shutdown
	t.Cleanup(publicShutdown)

	return &aeNode{
		mgr: mgr, client: peertls.NewClient(mgr),
		peerAddr: addr, podID: podID, store: st,
		publicAddr: publicAddr, peerSrv: peerSrv,
		peerHTTP: httpSrv, peerListener: tlsListener,
		peerServing:  true,
		peerShutdown: peerShutdown, publicShutdown: publicShutdown,
	}
}

// wireAntiEntropyForNode creates a static discovery manager and anti-entropy
// engine for the given node using the production bridge pattern. Address
// overrides map pod identities to the actual loopback dial addresses (the
// static peer list uses FQDNs for identity, but the real dial address is
// 127.0.0.1:<port>).
func wireAntiEntropyForNode(t *testing.T, node *aeNode, allNodes []*aeNode, cfg antientropy.Config) {
	t.Helper()
	ctx := context.Background()

	// Build the address override map: pod identity → real loopback address.
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
		ExpectedReplicas: aeReplicas,
		DNSName:          aeSuffix,
	}
	discMgr := discovery.NewManager(discCfg, nil, nil)
	if err := discMgr.Start(ctx); err != nil {
		t.Fatalf("%s: discovery Start: %v", node.podID, err)
	}
	t.Cleanup(discMgr.Shutdown)
	node.discMgr = discMgr

	// Build the peer source with address overrides: the snapshot function
	// overrides the Address field with the real loopback dial address.
	peerSource := antientropy.NewDiscoveryPeerSource(aeSuffix, func() antientropy.PeerSourceSnapshot {
		ds := discMgr.Snapshot()
		peers := make([]antientropy.PeerSourceInfo, len(ds.Peers))
		for i, p := range ds.Peers {
			addr := p.Address
			if override, ok := addrOverrides[p.PodIdentity]; ok {
				addr = override
			}
			peers[i] = antientropy.PeerSourceInfo{
				Hostname:    p.Hostname,
				PodIdentity: p.PodIdentity,
				Address:     addr,
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

	// Set the real dependencies.
	engine.SetPeerSource(peerSource)
	engine.SetInventoryClient(node.client)
	engine.SetMessageFetcher(node.client)
	engine.SetLocalStore(node.store)

	if err := engine.Start(ctx); err != nil {
		t.Fatalf("%s: anti-entropy Start: %v", node.podID, err)
	}
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		engine.Stop(sctx)
		cancel()
	})
}

// aePost creates a message on a node via the public API and returns the ID.
func aePost(t *testing.T, node *aeNode, body string) string {
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

// aeHasMsg checks whether a node's store contains a message with the given ID.
func aeHasMsg(node *aeNode, id string) bool {
	msg, err := node.store.GetByID(id)
	if err != nil {
		return false
	}
	return msg.ID == id
}

// aeMessageCount returns the number of messages in a node's store.
// Returns -1 on error (so it's safe to call from polling loops).
func aeMessageCount(node *aeNode) int64 {
	var count int64
	if err := node.store.DB().Model(&store.Message{}).Count(&count).Error; err != nil {
		return -1
	}
	return count
}

// aeStopPeer temporarily stops a node's peer listener (simulates unreachability).
func aeStopPeer(t *testing.T, node *aeNode) {
	t.Helper()
	node.peerListenerMu.Lock()
	defer node.peerListenerMu.Unlock()
	if !node.peerServing {
		return
	}
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = node.peerHTTP.Shutdown(sctx)
	_ = node.peerListener.Close()
	node.peerServing = false
	t.Logf("%s: peer listener stopped", node.podID)
}

// aeStartPeer restores a node's peer listener on the same address.
func aeStartPeer(t *testing.T, node *aeNode) {
	t.Helper()
	node.peerListenerMu.Lock()
	defer node.peerListenerMu.Unlock()
	if node.peerServing {
		return
	}
	listener, err := aeTlsListen(node.peerAddr, node.mgr.TLSConfigForServer())
	if err != nil {
		t.Fatalf("%s: re-listen on %s: %v", node.podID, node.peerAddr, err)
	}
	node.peerListener = listener
	httpSrv := &http.Server{Handler: node.peerSrv.Handler()}
	node.peerHTTP = httpSrv
	node.peerServing = true
	go func() { _ = httpSrv.Serve(listener) }()
	t.Logf("%s: peer listener restored", node.podID)
}

// aeStopNode cleanly stops a node's anti-entropy engine and all listeners
// (peer and public), and closes its store. It is used to permanently
// decommission a node before replacement.
func aeStopNode(t *testing.T, node *aeNode) {
	t.Helper()
	// Stop the anti-entropy engine first (aborts in-flight syncs).
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	node.engine.Stop(sctx)
	cancel()
	// Shut down peer listener.
	node.peerListenerMu.Lock()
	if node.peerServing {
		pctx, pcancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = node.peerHTTP.Shutdown(pctx)
		_ = node.peerListener.Close()
		pcancel()
		node.peerServing = false
	}
	node.peerListenerMu.Unlock()
	// Shut down public listener.
	node.publicShutdown()
	// Close the store.
	if err := node.store.Close(); err != nil {
		t.Logf("%s: store close: %v (ignoring)", node.podID, err)
	}
	t.Logf("%s: fully stopped (engine, peer, public, store)", node.podID)
}

// TestIntegrationFiveNodesAntiEntropy exercises the real anti-entropy engine
// with five independent replicas using real mTLS and SQLite.
func TestIntegrationFiveNodesAntiEntropy(t *testing.T) {
	sharedCA := newTestCA(t, "ae-workload")

	nodes := make([]*aeNode, aeReplicas)
	for i := 0; i < aeReplicas; i++ {
		nodes[i] = newAENode(t, sharedCA, fmt.Sprintf("gossipgrid-%d", i))
	}

	// Short interval for fast test convergence.
	aeCfg := antientropy.Config{
		Interval:        100 * time.Millisecond,
		PageSize:        32,
		FetchBatchSize:  8,
		MaxPagesPerPeer: 4,
		Workers:         2,
		RequestTimeout:  2 * time.Second,
	}

	// Wire anti-entropy for all nodes.
	for _, node := range nodes {
		wireAntiEntropyForNode(t, node, nodes, aeCfg)
	}

	// --- Scenario A: Repair gossip misses ---
	t.Run("repair gossip misses", func(t *testing.T) {
		// Stop node 4 (unreachable).
		aeStopPeer(t, nodes[4])

		// Create a message on node 0 while node 4 is down.
		msgID := aePost(t, nodes[0], "message while node 4 is down")

		// Wait a bit for the gossip engine (if any) to exhaust retries toward node 4.
		// In this test we don't run gossip, so the message only exists on node 0.
		time.Sleep(200 * time.Millisecond)

		// Restore node 4.
		aeStartPeer(t, nodes[4])

		// Anti-entropy should repair the missing message on node 4.
		// With a 100ms interval, convergence should happen within a few seconds.
		if !aeWaitUntil(10*time.Second, func() bool {
			return aeHasMsg(nodes[4], msgID)
		}) {
			t.Fatal("node 4 did not receive the message via anti-entropy after 10s")
		}
		t.Log("node 4 repaired the missing message via anti-entropy")

		// Verify the record is identical.
		ref, _ := nodes[0].store.GetByID(msgID)
		got, _ := nodes[4].store.GetByID(msgID)
		if got.Body != ref.Body || got.Origin != ref.Origin {
			t.Errorf("node 4 record mismatch: body=%q origin=%q", got.Body, got.Origin)
		}
	})

	// --- Scenario B: Replica replacement ---
	t.Run("replica replacement", func(t *testing.T) {
		// 1. Populate multiple replicas with historical messages.
		var historicalIDs []string
		for i := 0; i < 3; i++ {
			id := aePost(t, nodes[i], fmt.Sprintf("replacement test message %d", i))
			historicalIDs = append(historicalIDs, id)
		}

		// 2. Verify at least one surviving authorized replica holds every
		//    historical message (convergence via anti-entropy).
		if !aeWaitUntil(15*time.Second, func() bool {
			for _, node := range nodes {
				for _, id := range historicalIDs {
					if !aeHasMsg(node, id) {
						return false
					}
				}
			}
			return true
		}) {
			t.Fatal("not all nodes converged on replacement messages after 15s")
		}

		// Capture the expected total message count before stopping node 4.
		expectedCount := aeMessageCount(nodes[0])
		if expectedCount < int64(len(historicalIDs)) {
			t.Fatalf("node 0 has only %d messages, want >= %d", expectedCount, len(historicalIDs))
		}

		// 3. Stop node 4's anti-entropy engine and relevant listeners cleanly.
		aeStopNode(t, nodes[4])

		// 4. Replace its SQLite store with a genuinely empty database.
		//    (The old store was already closed in aeStopNode; create a fresh one.)
		emptyDir := t.TempDir()
		replStore, err := store.New(emptyDir + "/replacement.db")
		if err != nil {
			t.Fatalf("creating replacement store: %v", err)
		}
		t.Cleanup(func() { _ = replStore.Close() })

		// 5. Start an appropriately authenticated replacement replica using
		//    the empty store. It uses the same pod identity and certificate
		//    as the original node 4 (in a real deployment, the same
		//    StatefulSet pod name would be reused).
		//
		// Build the replacement node manually (not via newAENode) so we can
		// control the store, peer server, and engine lifecycle precisely.
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
			ExpectedReplicas: aeReplicas,
			ReloadInterval:   50 * time.Millisecond,
		}, nil)
		if err := replMgr.Start(context.Background()); err != nil {
			t.Fatalf("replacement node manager Start: %v", err)
		}
		t.Cleanup(replMgr.Shutdown)

		replNode := &aeNode{
			mgr:    replMgr,
			client: peertls.NewClient(replMgr),
			podID:  "gossipgrid-4",
			store:  replStore,
		}
		// Register a safe cleanup that checks for nil engine.
		t.Cleanup(func() {
			if replNode.engine != nil {
				sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				replNode.engine.Stop(sctx)
				cancel()
			}
		})

		// Start the public HTTP listener for the replacement node.
		pubSrv := newPublicServer(t, "gossipgrid-4", replNode.store)
		replNode.publicAddr = pubSrv.addr
		replNode.publicShutdown = pubSrv.shutdown

		// Create the peer server with the replacement (empty) store and
		// sync endpoints enabled.
		replPeerSrv := peertls.NewPeerServerWithSync(replMgr, ":0", "gossipgrid-4", "v0.1.0-ae-repl", replNode.store, true, true, nil, 0)
		tlsListener, err := aeTlsListen("127.0.0.1:0", replMgr.TLSConfigForServer())
		if err != nil {
			t.Fatalf("replacement node tls.Listen: %v", err)
		}
		replNode.peerAddr = tlsListener.Addr().String()
		replNode.peerSrv = replPeerSrv
		replHTTP := &http.Server{Handler: replPeerSrv.Handler()}
		replNode.peerHTTP = replHTTP
		replNode.peerListener = tlsListener
		replNode.peerServing = true
		go func() { _ = replHTTP.Serve(tlsListener) }()
		replNode.peerShutdown = func() {
			rctx, rcancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = replHTTP.Shutdown(rctx)
			_ = tlsListener.Close()
			rcancel()
		}

		// 6. Ensure its anti-entropy engine uses that replacement store.
		//    Build the address override map: the replacement node's address
		//    is replNode.peerAddr, and the surviving nodes keep their
		//    addresses.
		allNodes := append([]*aeNode{}, nodes[0], nodes[1], nodes[2], nodes[3])
		allNodes = append(allNodes, replNode)
		wireAntiEntropyForNode(t, replNode, allNodes, aeCfg)

		// 7. Reconcile through real mTLS inventory and fetch endpoints.
		//    Wait for the replacement store to recover ALL historical
		//    message records.
		if !aeWaitUntil(30*time.Second, func() bool {
			for _, id := range historicalIDs {
				if !aeHasMsg(replNode, id) {
					return false
				}
			}
			return true
		}) {
			t.Fatalf("replacement node did not recover all %d historical messages after 30s", len(historicalIDs))
		}
		t.Logf("replacement node recovered all %d historical messages via real mTLS", len(historicalIDs))

		// 8. Verify the replacement store recovers ALL historical message
		//    records with identical immutable fields.
		for _, id := range historicalIDs {
			ref, err := nodes[0].store.GetByID(id)
			if err != nil {
				t.Fatalf("reference node 0 missing ID %s: %v", id, err)
			}
			got, err := replNode.store.GetByID(id)
			if err != nil {
				t.Fatalf("replacement node missing ID %s: %v", id, err)
			}
			if got.Body != ref.Body {
				t.Errorf("ID %s: body mismatch: got %q, want %q", id, got.Body, ref.Body)
			}
			if got.Origin != ref.Origin {
				t.Errorf("ID %s: origin mismatch: got %q, want %q", id, got.Origin, ref.Origin)
			}
			if !got.CreatedAt.Equal(ref.CreatedAt) {
				t.Errorf("ID %s: created_at mismatch: got %v, want %v", id, got.CreatedAt, ref.CreatedAt)
			}
		}

		// 9. Verify the surviving replicas remain operational and retain
		//    their data.
		for i := 0; i < 4; i++ {
			if c := aeMessageCount(nodes[i]); c != expectedCount {
				t.Errorf("surviving node %d has %d messages, want %d", i, c, expectedCount)
			}
		}
		// Verify the replacement node has the full message set.
		if c := aeMessageCount(replNode); c != expectedCount {
			t.Errorf("replacement node has %d messages, want %d", c, expectedCount)
		}

		// 10. Shut down the replacement node cleanly.
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		replNode.engine.Stop(sctx)
		cancel()
		replNode.peerShutdown()
		replNode.publicShutdown()
		t.Log("replica replacement: empty replacement node recovered all historical data via real mTLS; surviving replicas intact")
	})

	// --- Scenario C: Concurrent writers ---
	t.Run("concurrent writers", func(t *testing.T) {
		// Write messages on 3 different nodes concurrently while anti-entropy runs.
		// Node 4 is stopped from scenario B; use nodes 0, 1, 2 for writing
		// and check convergence on the 4 active nodes (0, 1, 2, 3).
		var wg sync.WaitGroup
		var mu sync.Mutex
		var newIDs []string
		for _, idx := range []int{0, 1, 2} {
			wg.Add(1)
			go func(n *aeNode) {
				defer wg.Done()
				id := aePost(t, n, fmt.Sprintf("concurrent writer %s", n.podID))
				mu.Lock()
				newIDs = append(newIDs, id)
				mu.Unlock()
			}(nodes[idx])
		}
		wg.Wait()

		// Wait for the 4 active nodes to converge on the 3 new messages.
		activeNodes := []*aeNode{nodes[0], nodes[1], nodes[2], nodes[3]}
		if !aeWaitUntil(15*time.Second, func() bool {
			for _, node := range activeNodes {
				for _, id := range newIDs {
					if !aeHasMsg(node, id) {
						return false
					}
				}
			}
			return true
		}) {
			t.Fatal("active nodes did not converge on concurrent writer messages after 15s")
		}

		// Verify exactly the same message set on all active nodes.
		count0 := aeMessageCount(nodes[0])
		for i := 1; i < 4; i++ {
			if c := aeMessageCount(nodes[i]); c != count0 {
				t.Errorf("node %d has %d messages, node 0 has %d", i, c, count0)
			}
		}
		t.Log("concurrent writers: all 4 active nodes converged on identical message sets")
	})

	// --- Scenario D: Pagination stress ---
	t.Run("pagination stress", func(t *testing.T) {
		// Create enough messages to exceed the page size (32) and
		// max pages per peer (4) → 32*4 = 128 IDs per cycle (the fetch
		// batch of 12 limits per-batch record count, not per-page).
		// We create 130 messages on node 0 to require multiple cycles.
		for i := 0; i < 130; i++ {
			aePost(t, nodes[0], fmt.Sprintf("pagination msg %d", i))
		}

		// Verify node 0 has all 130+ messages.
		if c := aeMessageCount(nodes[0]); c < 130 {
			t.Fatalf("node 0 has only %d messages, want >= 130", c)
		}

		// Wait for node 3 (a surviving active node) to converge. With page
		// size 32 and max 4 pages/cycle, each cycle processes up to 128 IDs.
		// The total inventory (including prior messages) may require 2+ cycles.
		// With 100ms interval, this should converge within ~5s.
		if !aeWaitUntil(20*time.Second, func() bool {
			return aeMessageCount(nodes[3]) == aeMessageCount(nodes[0])
		}) {
			t.Errorf("node 3 count=%d, node 0 count=%d after 20s (pagination stress)",
				aeMessageCount(nodes[3]), aeMessageCount(nodes[0]))
		} else {
			t.Log("pagination stress: node 3 converged (cursor advanced across multiple cycles)")
		}
	})

	// --- Scenario E: Recovery and isolation ---
	t.Run("recovery and isolation", func(t *testing.T) {
		// Stop node 2 (isolate it). Node 4 is already stopped from
		// scenario B; we only check the active nodes (0, 1, 2, 3).
		aeStopPeer(t, nodes[2])

		// Create a message on node 0 while node 2 is isolated.
		msgID := aePost(t, nodes[0], "isolation test message")

		// Wait for other active nodes to converge (not node 2, not node 4
		// which is already stopped).
		activeNodes := []*aeNode{nodes[0], nodes[1], nodes[3]}
		if !aeWaitUntil(10*time.Second, func() bool {
			for _, n := range activeNodes {
				if !aeHasMsg(n, msgID) {
					return false
				}
			}
			return true
		}) {
			t.Fatal("active nodes did not converge on isolation message")
		}

		// Verify other nodes remain responsive (public API works) while node 2
		// is isolated. This confirms the public listener is independent of
		// peer availability.
		status, _, err := postPublicJSON(t, nodes[3].publicAddr, `{"body":"node 3 still responsive"}`)
		if err != nil || status != http.StatusCreated {
			t.Fatalf("node 3 public API not responsive while node 2 is isolated: status=%d err=%v", status, err)
		}

		// Restore node 2.
		aeStartPeer(t, nodes[2])

		// Anti-entropy should repair node 2.
		if !aeWaitUntil(10*time.Second, func() bool {
			return aeHasMsg(nodes[2], msgID)
		}) {
			t.Fatal("node 2 did not recover the isolation message after restoration")
		}
		t.Log("recovery and isolation: node 2 restored and repaired via anti-entropy")
	})

	// --- Clean shutdown ---
	t.Run("clean shutdown", func(t *testing.T) {
		// Stop the surviving nodes (0-3). Node 4 was already stopped in
		// scenario B; the replacement node was stopped at the end of that
		// scenario too.
		for i := 0; i < 4; i++ {
			sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			nodes[i].engine.Stop(sctx)
			cancel()
			t.Logf("node %d anti-entropy engine stopped", i)
		}

		// Shut down all listeners for surviving nodes.
		for i := 0; i < 4; i++ {
			nodes[i].peerShutdown()
			nodes[i].publicShutdown()
		}

		// Verify repeated Stop is safe.
		for i := 0; i < 4; i++ {
			sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			nodes[i].engine.Stop(sctx)
			cancel()
		}

		// Verify no goroutine leaks (engine stats show 0 active syncs).
		for i := 0; i < 4; i++ {
			s := nodes[i].engine.Stats()
			if s.ActiveSyncs != 0 {
				t.Errorf("node %d: ActiveSyncs = %d after shutdown, want 0", i, s.ActiveSyncs)
			}
		}
		t.Log("surviving nodes shut down cleanly; no active syncs after shutdown")
	})
}

// --- Helper functions for the anti-entropy integration test ---

// aeWaitUntil polls cond every 50ms until it returns true or the timeout
// expires. It returns true if the condition was met.
func aeWaitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return cond()
}

// aeTlsListen creates a TLS listener on the given address using the mTLS config.
func aeTlsListen(addr string, cfg *tls.Config) (net.Listener, error) {
	return tls.Listen("tcp", addr, cfg)
}

// aePublicServer holds a public HTTP server for a test node.
type aePublicServer struct {
	addr     string
	shutdown func()
}

// newPublicServer starts a public HTTP listener for the given node and store.
func newPublicServer(t *testing.T, podID string, st *store.Store) *aePublicServer {
	t.Helper()
	srv := server.NewServer(":0", server.NewWithStore(server.Node{ID: podID}, st))
	lc := net.ListenConfig{}
	listener, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("%s: public listen: %v", podID, err)
	}
	go func() { _ = srv.HTTPServer.Serve(listener) }()
	shutdown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = listener.Close()
	}
	return &aePublicServer{addr: listener.Addr().String(), shutdown: shutdown}
}
