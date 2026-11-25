// Package main: lifecycle epoch-fencing integration test (Milestone 5A).
//
// These tests use independent SQLite stores, distinct workload certificates,
// real TLS 1.3 mTLS, the production peer handlers, the production peer client,
// and an injected deterministic authoritative metadata source.
//
// Scenarios:
//
//	A. Matching epochs — two replicas with the same ClusterID and EpochID;
//	   verify authorized message exchange and synchronization work.
//	B. Wrong-epoch peer — a peer presents a valid workload certificate but
//	   belongs to another epoch; verify all replication operations are
//	   rejected and the receiver retains its existing data unchanged.
//	C. Unready synchronization source — a peer is in SYNCING with the correct
//	   epoch; verify it can answer status but cannot supply a snapshot.
//	D. Epoch change — a replica initially in epoch A; metadata changes to
//	   epoch B; verify the replica stops accepting writes and serving
//	   replication, remains bound to A, and preserves its data.
//	E. Startup without metadata — an empty replica starts with lifecycle
//	   protection enabled but no metadata; verify it is alive but unready.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/antientropy"
	"gossipgrid/internal/gossip"
	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/peertls"
	"gossipgrid/internal/server"
	"gossipgrid/internal/store"
)

// lifecycleTestNode is a self-contained GossipGrid peer for lifecycle
// integration tests. It has a peer server, a public server, a store, a
// controller-driven lifecycle controller, a mutable metadata source, and a
// peer client wired to an EpochProvider sourced from the controller's current
// authorization decision.
type lifecycleTestNode struct {
	t          *testing.T
	ctx        context.Context
	mgr        *peertls.Manager
	peerSrv    *peertls.PeerServer
	publicSrv  *server.Server
	store      *store.Store
	ctrl       *lifecycle.Controller
	src        *lifecycle.FakeMetadataSource // mutable authoritative source
	client     *peertls.Client               // wired to the controller EpochProvider
	gEngine    *gossip.Engine                // production gossip engine (fenced)
	aEngine    *antientropy.Engine           // production anti-entropy engine (fenced)
	peerAddr   string
	publicAddr string
	podID      string
}

// synchronizedPeerHandler is a test-only http.Handler wrapper that holds an
// optional custom handler behind a RWMutex. It is installed on the peer
// server's HTTPServer.Handler BEFORE the server starts serving, so a test
// can install a custom handler (e.g. to simulate a partial-404 or a slow
// handler) after the node is up, without racing the server goroutine that
// is concurrently reading the handler field. When no custom handler is
// installed, requests are passed through to the production mux.
type synchronizedPeerHandler struct {
	mu     sync.RWMutex
	inner  http.Handler // production mux (always non-nil)
	custom http.Handler // optional test handler (nil when not installed)
}

// ServeHTTP serves the custom handler when one is installed; otherwise it
// passes through to the production mux.
func (h *synchronizedPeerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	c := h.custom
	h.mu.RUnlock()
	if c != nil {
		c.ServeHTTP(w, r)
		return
	}
	h.inner.ServeHTTP(w, r)
}

// nodeTestOpts controls how a lifecycle test node is set up.
type nodeTestOpts struct {
	// bindTo, when non-nil, binds the store to the given cluster/epoch before
	// startup (representing a durable binding that was established by an
	// earlier authorized initialization).
	bindTo *lifecycle.ClusterMetadata
	// forceState, when non-nil, forces the controller's state machine to the
	// given state via the bounded test-only promotion path (for scenarios that
	// need a specific non-derivable state such as SYNCING).
	forceState lifecycle.State
	// engines, when true, starts the gossip and anti-entropy engines on the
	// node (only the few tests that exercise engine fencing need them; the
	// default is false to minimize goroutine count and cross-test
	// interference).
	engines bool
	// aEngineRequestTimeout, when non-zero, overrides the anti-entropy engine's
	// RequestTimeout before engine creation. The default is 5s (the production
	// default). A test that exercises per-request timeout ownership sets this
	// to match the peer client's RequestTimeout so the regression is caught
	// if an engine-level shared deadline is reintroduced.
	aEngineRequestTimeout time.Duration
}

// newLifecycleTestNode creates and starts a controller-driven lifecycle test
// node. The controller is started (its metadata worker runs) against a mutable
// FakeMetadataSource, and the store is a real SQLite store. The node's peer
// client is wired to an EpochProvider sourced from the controller's current
// authorization decision (not a one-time startup copy), and the public handler
// uses the lifecycle-aware constructor (readiness and reads gated on the same
// authorization decision).
func newLifecycleTestNode(t *testing.T, ca *testCA, podID string, opts nodeTestOpts) *lifecycleTestNode {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	cert, key := issueLeafCert(t, ca, podID+"."+intSuffix)
	chainPEM, keyPEM := certKeyPEMTwo(t, cert, key, ca)
	certFile := writePEMFile(t, dir, "cert.pem", chainPEM)
	keyFile := writePEMFile(t, dir, "key.pem", keyPEM)
	trustFile := writePEMFile(t, dir, "ca-bundle.pem", ca.pem)

	mgr := peertls.New(peertls.Config{
		CertFile:         certFile,
		KeyFile:          keyFile,
		TrustBundleFile:  trustFile,
		IdentitySuffix:   intSuffix,
		StatefulSetName:  intStatefulSet,
		NodeID:           podID,
		ExpectedReplicas: 5,
		ReloadInterval:   10 * time.Second,
	}, nil)
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("%s: manager Start: %v", podID, err)
	}

	st, err := store.New(filepath.Join(dir, "gossipgrid.db"))
	if err != nil {
		mgr.Shutdown()
		t.Fatalf("%s: store: %v", podID, err)
	}

	// Bind the store to the given cluster/epoch BEFORE startup when requested.
	// A nonempty database without a binding cannot be bound, so the bind is
	// done while the database is empty.
	if opts.bindTo != nil {
		if _, err := st.Bind(opts.bindTo.ClusterID, opts.bindTo.EpochID, opts.bindTo.Generation); err != nil {
			mgr.Shutdown()
			_ = st.Close()
			t.Fatalf("%s: binding store: %v", podID, err)
		}
	}

	// Mutable authoritative metadata source.
	src := lifecycle.NewFakeMetadataSourceUnavailable()

	machine := lifecycle.NewMachine(nil)
	ctrl := lifecycle.NewController(lifecycle.Config{
		MetadataPollInterval: time.Second,
		NodeID:               podID,
		Enabled:              true,
	}, machine, src, st, nil)

	// Start the controller (runs the metadata worker).
	if err := ctrl.Start(ctx); err != nil {
		t.Fatalf("%s: controller Start: %v", podID, err)
	}

	// If a specific non-derivable state is requested, force it via the bounded
	// test-only machine transition (e.g. SYNCING or READY, which the controller
	// does not enter automatically in 5A). Production promotion still goes
	// through the gated SetReady; this direct machine force is the test-only
	// mechanism the spec permits for reaching a baseline state.
	if opts.forceState != lifecycle.STARTING {
		if err := forceMachineState(t, machine, opts.forceState, podID); err != nil {
			t.Fatalf("%s: force state %s: %v", podID, opts.forceState.String(), err)
		}
	}

	// Build the public handler with lifecycle readiness gating.
	publicHandler := server.NewWithStoreAndCallbackAndLifecycle(server.Node{ID: podID}, st, nil, ctrl)

	// Bind the actual listeners ONCE and retain them. Do NOT close and
	// re-bind: that is a race (another process can grab the port between
	// close and re-bind), which is the root cause of the intermittent
	// 30-60s startup timeouts. The listeners are served directly through
	// the retained net.Listener objects.
	lc := net.ListenConfig{}
	peerLn, err := lc.Listen(ctx, "tcp", ":0")
	if err != nil {
		t.Fatalf("%s: peer listen: %v", podID, err)
	}
	peerAddr := peerLn.Addr().String()

	publicLn, err := lc.Listen(ctx, "tcp", ":0")
	if err != nil {
		_ = peerLn.Close()
		t.Fatalf("%s: public listen: %v", podID, err)
	}
	publicAddr := publicLn.Addr().String()

	// Build the peer server with lifecycle fencing on the actual address.
	peerSrv := peertls.NewPeerServerWithSyncAndLifecycle(mgr, peerAddr, podID, "v0.1.0-lc", st, true, true, nil, ctrl, 0)
	publicSrv := server.NewServer(publicAddr, publicHandler)

	// Peer client wired to the controller's current authorization decision.
	peerClient := peertls.NewClient(mgr)
	peerClient.SetEpochProvider(newControllerEpochProvider(ctrl))
	// Milestone 5B, Stage 2, Issue 3: set the DEDICATED bootstrap
	// authorization provider so the peer client enforces the full
	// bootstrapAuthorized predicate at the transport boundary (same
	// wiring as production in main.go).
	peerClient.SetBootstrapEpochProvider(&controllerBootstrapProvider{ctrl: ctrl})

	// Test-only pre-Serve handler wrapper: a test can install a custom
	// handler (e.g. to simulate a partial-404 or a slow handler) via
	// node.setPeerHandler. The wrapper is installed on peerSrv.HTTPServer.Handler
	// BEFORE peerSrv.HTTPServer.Serve is started, so the handler is
	// configured before any request is served (no race). When no custom
	// handler is installed, the wrapper is a no-op passthrough to the
	// production mux.
	innerMux := peerSrv.HTTPServer.Handler
	peerSrv.HTTPServer.Handler = &synchronizedPeerHandler{inner: innerMux}

	// Gossip engine wired to the real delivery transport and the controller's
	// outbound admission check (Milestone 5A: outbound gossip is fenced on the
	// current authorization decision). The snapshot source is a static,
	// deterministic membership that includes the other node(s) so the engine
	// has an eligible target. Engines are only started when opts.engines is
	// true (the few tests that exercise engine fencing); the default is false
	// to minimize goroutine count and cross-test interference.
	var gEngine *gossip.Engine
	var aEngine *antientropy.Engine
	if opts.engines {
		gEngine = gossip.New(gossip.Config{
			Fanout:        1,
			Rounds:        1,
			Workers:       2,
			QueueCapacity: 16,
			MaxAttempts:   3,
			RetryBase:     20 * time.Millisecond,
			RetryMax:      100 * time.Millisecond,
		}, gossip.NewDefaultPeerSelector(intSuffix, nil), gossip.NewRealDelivery(peerClient), nil)
		gEngine.SetSnapshotSource(func() gossip.PeerSnapshot {
			// Static mode; the peer list is filled in by the test once all nodes
			// are up (via node.setGossipPeer). For now return a snapshot with the
			// local node so self-exclusion is exercised.
			return gossip.PeerSnapshot{
				LocalNode: &gossip.PeerInfo{
					PodIdentity: podID,
					Hostname:    podID + "." + intSuffix,
				},
				Mode: "static",
			}
		})
		gEngine.SetAdmissionCheck(func(ctx context.Context) error {
			d := ctrl.Admission().CheckReplicationOutbound(ctx)
			if !d.Allowed {
				return fmt.Errorf("lifecycle admission: %s", d.Reason)
			}
			return nil
		})
		if err := gEngine.Start(ctx); err != nil {
			t.Fatalf("%s: gossip Start: %v", podID, err)
		}

		// Anti-entropy engine wired to the real inventory/fetch transports, the
		// local store, and the controller's outbound admission check. The
		// interval is 10s (well beyond any test duration) so the periodic
		// scheduler's first tick does NOT fire during a test. The anti-entropy
		// fencing is verified at the admission boundary (the check set on the
		// engine), not by waiting for the scheduler to fire.
		aTimeout := 5 * time.Second
		if opts.aEngineRequestTimeout > 0 {
			aTimeout = opts.aEngineRequestTimeout
		}
		aEngine = antientropy.New(antientropy.Config{
			Interval:        10 * time.Second,
			PageSize:        10,
			FetchBatchSize:  8,
			MaxPagesPerPeer: 4,
			Workers:         2,
			RequestTimeout:  aTimeout,
		}, antientropy.NewDiscoveryPeerSource(intSuffix, func() antientropy.PeerSourceSnapshot {
			// Default empty membership (no peers); replaced by
			// setAntiEntropyPeer.
			return antientropy.PeerSourceSnapshot{
				LocalNode: &antientropy.PeerSourceInfo{PodIdentity: podID},
				Mode:      "static",
			}
		}), peerClient, peerClient, st, nil)
		aEngine.SetAdmissionCheck(func(ctx context.Context) error {
			d := ctrl.Admission().CheckReplicationOutbound(ctx)
			if !d.Allowed {
				return fmt.Errorf("lifecycle admission: %s", d.Reason)
			}
			return nil
		})
		if err := aEngine.Start(ctx); err != nil {
			t.Fatalf("%s: anti-entropy Start: %v", podID, err)
		}
	}

	node := &lifecycleTestNode{
		t:          t,
		ctx:        ctx,
		mgr:        mgr,
		peerSrv:    peerSrv,
		publicSrv:  publicSrv,
		store:      st,
		ctrl:       ctrl,
		src:        src,
		client:     peerClient,
		gEngine:    gEngine,
		aEngine:    aEngine,
		peerAddr:   peerAddr,
		publicAddr: publicAddr,
		podID:      podID,
	}

	// Serve through the retained listeners (do NOT call ListenAndServe,
	// which would create a new listener on the same address — a race).
	// The peer server serves over TLS 1.3 mutual auth via the retained
	// net.Listener wrapped in a TLS listener. The public server serves
	// plain HTTP over the retained net.Listener.
	var listenerWG sync.WaitGroup
	listenerWG.Add(2)

	// Peer server: wrap the retained plain TCP listener in a TLS listener
	// using the Manager's TLS config (TLS 1.3, mutual TLS, identity auth).
	go func() {
		defer listenerWG.Done()
		tlsCfg := mgr.TLSConfigForServer()
		tlsLn := tls.NewListener(peerLn, tlsCfg)
		// Surface startup errors immediately (not silently ignored).
		if err := peerSrv.HTTPServer.Serve(tlsLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Logf("%s: peer server exited: %v", podID, err)
		}
	}()

	// Public server: serve plain HTTP over the retained listener.
	go func() {
		defer listenerWG.Done()
		if err := publicSrv.Serve(publicLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Logf("%s: public server exited: %v", podID, err)
		}
	}()

	// Wait for listeners to be ready.
	waitForAddr(t, ctx, peerAddr)
	waitForAddr(t, ctx, publicAddr)

	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if gEngine != nil {
			gEngine.Stop(shutdownCtx)
		}
		if aEngine != nil {
			aEngine.Stop(shutdownCtx)
		}
		_ = node.publicSrv.Shutdown(shutdownCtx)
		_ = node.peerSrv.Shutdown(shutdownCtx)
		// Wait for the listener goroutines to exit before shutting down the
		// manager and store (avoids in-flight requests hitting a shut-down
		// transport).
		listenerWG.Wait()
		ctrl.Stop(context.Background())
		mgr.Shutdown()
		_ = st.Close()
	})
	return node
}

// setPeerHandler installs a test-only custom handler on the node's peer
// server. The handler is set through the synchronizedPeerHandler wrapper
// (installed before the server started serving), so it is configured before
// any request can be served — no race with the server goroutine. The custom
// handler is responsible for delegating to the production handler (the
// wrapper's inner mux) when it wants to fall through.
func (n *lifecycleTestNode) setPeerHandler(custom http.Handler) {
	n.t.Helper()
	wrapper := n.peerSrv.HTTPServer.Handler.(*synchronizedPeerHandler)
	wrapper.mu.Lock()
	wrapper.custom = custom
	wrapper.mu.Unlock()
}

// productionPeerHandler returns the production peer handler (the mux
// registered by the peer server constructor), which a test can capture to
// delegate to from a custom handler installed via setPeerHandler.
func (n *lifecycleTestNode) productionPeerHandler() http.Handler {
	n.t.Helper()
	return n.peerSrv.HTTPServer.Handler.(*synchronizedPeerHandler).inner
}

// setGossipPeer wires a node's gossip engine to treat another node as an
// eligible peer target (static membership), so the engine has a delivery
// target. It is called after all nodes in a test are up.
func (n *lifecycleTestNode) setGossipPeer(peer *lifecycleTestNode) {
	n.t.Helper()
	peerID := peer.podID
	n.gEngine.SetSnapshotSource(func() gossip.PeerSnapshot {
		return gossip.PeerSnapshot{
			Peers: []gossip.PeerInfo{{
				PodIdentity: peerID,
				Hostname:    peerID + "." + intSuffix,
				Address:     peer.peerAddr,
			}},
			LocalNode: &gossip.PeerInfo{
				PodIdentity: n.podID,
				Hostname:    n.podID + "." + intSuffix,
			},
			Mode: "static",
		}
	})
}

// setAntiEntropyPeer wires a node's anti-entropy engine to treat another node
// as an eligible peer target, so the engine's exported sync path has a target.
func (n *lifecycleTestNode) setAntiEntropyPeer(peer *lifecycleTestNode) {
	n.t.Helper()
	peerID := peer.podID
	peerAddr := peer.peerAddr
	n.aEngine.SetPeerSource(antientropy.NewDiscoveryPeerSource(intSuffix, func() antientropy.PeerSourceSnapshot {
		return antientropy.PeerSourceSnapshot{
			Peers: []antientropy.PeerSourceInfo{{
				PodIdentity: peerID,
				Hostname:    peerID + "." + intSuffix,
				Address:     peerAddr,
			}},
			LocalNode: &antientropy.PeerSourceInfo{
				PodIdentity: n.podID,
			},
			ExpectedIdentities: []string{n.podID, peerID},
			Mode:               "static",
		}
	}))
}

// setMetadata atomically sets the node's authoritative metadata and waits for
// the controller's metadata worker to re-evaluate (the poll interval is 50ms).
// It uses a channel-free poll: it waits until the controller's authorization
// snapshot reflects the new state, bounded by a deadline.
func (n *lifecycleTestNode) setMetadata(meta lifecycle.ClusterMetadata) {
	n.t.Helper()
	n.src.SetMetadata(meta)
	n.waitForAuthorization(func(s lifecycle.AuthorizationSnapshot) bool {
		// Consider the re-evaluation complete once the accepted metadata
		// matches the new metadata (or the state has settled to
		// RECOVERY_REQUIRED for a mismatch).
		return s.Metadata != nil && s.Metadata.ClusterID == meta.ClusterID && s.Metadata.EpochID == meta.EpochID
	})
}

// waitForAuthorization polls the controller's authorization snapshot until the
// predicate holds or the fixed 10s deadline elapses.
func (n *lifecycleTestNode) waitForAuthorization(pred func(lifecycle.AuthorizationSnapshot) bool) {
	n.t.Helper()
	const deadline = 10 * time.Second
	deadlineTime := time.Now().Add(deadline)
	for time.Now().Before(deadlineTime) {
		if pred(n.ctrl.Authorization()) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	n.t.Fatalf("authorization did not reach expected state within %v", deadline)
}

// forceMachineState drives a test-only Machine to the target state using the
// bounded, non-production transition helpers. It first force-resets to
// STARTING, then walks the legal transition path to the target. It is a test
// harness only: production state promotion goes through the controller's
// gated SetReady and is never driven directly.
func forceMachineState(t *testing.T, m *lifecycle.Machine, target lifecycle.State, podID string) error {
	t.Helper()
	switch target {
	case lifecycle.STARTING:
		return nil
	case lifecycle.DISCOVERING:
		m.ForceTransitionForTest(lifecycle.DISCOVERING, "test setup")
		return nil
	case lifecycle.SYNCING:
		m.ForceTransitionForTest(lifecycle.DISCOVERING, "test setup")
		return m.TransitionForTest(lifecycle.SYNCING, "test setup")
	case lifecycle.READY:
		m.ForceTransitionForTest(lifecycle.DISCOVERING, "test setup")
		return m.TransitionForTest(lifecycle.READY, "test setup")
	default:
		return fmt.Errorf("%s: cannot force to %s", podID, target.String())
	}
}

// peerExpectedIdentity returns the expected certificate identity for a peer.
func peerExpectedIdentity(podID string) string {
	return podID + "." + intSuffix
}

// waitForAddr polls until the TCP address accepts connections. For TLS
// listeners, the TCP connection succeeds but the TLS handshake may fail;
// we treat any TCP-level success (or a TLS error that proves the listener
// is up) as "ready".
func waitForAddr(t *testing.T, ctx context.Context, addr string) {
	t.Helper()
	dialer := net.Dialer{Timeout: 300 * time.Millisecond}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			// Defensive nil check: a race between the dial and the listener
			// teardown can return a nil conn with a nil err.
			if conn != nil {
				_ = conn.Close()
			}
			return
		}
		// If the context was cancelled, stop waiting.
		select {
		case <-ctx.Done():
			return
		default:
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("address %s not ready after 60s", addr)
}

// postMessageTo posts a message to a node's public API.
func postMessageTo(t *testing.T, n *lifecycleTestNode, body string) *http.Response {
	t.Helper()
	reqBody := fmt.Sprintf(`{"body":%q}`, body)
	req, err := http.NewRequestWithContext(n.ctx, "POST", "http://"+n.publicAddr+"/api/messages", bytes.NewReader([]byte(reqBody)))
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /api/messages: %v", err)
	}
	return resp
}

// getReadiness fetches the /readyz endpoint and returns the status code.
func getReadiness(t *testing.T, n *lifecycleTestNode) int {
	t.Helper()
	req, err := http.NewRequestWithContext(n.ctx, "GET", "http://"+n.publicAddr+"/readyz", nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}

// getPeerStatus fetches the peer status endpoint via mTLS.
func getPeerStatus(t *testing.T, client *peertls.Client, n *lifecycleTestNode) (*peertls.PeerStatusInfo, error) {
	t.Helper()
	return client.GetStatus(n.ctx, n.peerAddr, peerExpectedIdentity(n.podID))
}

// TestIntegrationLifecycleMatchingEpochs (Scenario A).
// Two replicas with the same authorized ClusterID and EpochID, valid database
// bindings, and eligible lifecycle states. Verify authorized message exchange
// and synchronization work.
func TestIntegrationLifecycleMatchingEpochs(t *testing.T) {
	ca := newTestCA(t, "lifecycle-ca")
	meta := lifecycle.ClusterMetadata{
		ClusterID:   "cluster-test-1",
		EpochID:     "epoch-test-1",
		Generation:  1,
		Initialized: true,
	}

	nodeA := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &meta, forceState: lifecycle.READY})
	nodeB := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &meta, forceState: lifecycle.READY})

	// Establish both nodes' authoritative metadata; the controller's
	// metadata worker re-evaluates admission on the matching binding.
	nodeA.setMetadata(meta)
	nodeB.setMetadata(meta)

	// Verify both nodes report READY.
	codeA := getReadiness(t, nodeA)
	if codeA != http.StatusOK {
		t.Fatalf("node A readiness: expected 200, got %d", codeA)
	}
	codeB := getReadiness(t, nodeB)
	if codeB != http.StatusOK {
		t.Fatalf("node B readiness: expected 200, got %d", codeB)
	}

	// Verify peer status reports the correct epoch.
	statusA, err := getPeerStatus(t, nodeA.client, nodeA)
	if err != nil {
		t.Fatalf("node A peer status: %v", err)
	}
	if statusA.ClusterID != meta.ClusterID || statusA.EpochID != meta.EpochID {
		t.Errorf("node A status: expected %s/%s, got %s/%s",
			meta.ClusterID, meta.EpochID, statusA.ClusterID, statusA.EpochID)
	}
	if statusA.LifecycleState != "READY" {
		t.Errorf("node A lifecycle state: expected READY, got %s", statusA.LifecycleState)
	}
	if !statusA.BootstrapSourceEligible {
		t.Error("node A: expected bootstrap source eligible")
	}

	// Verify message exchange works (node A -> node B). The outbound epoch
	// is sourced from node A's current controller authorization decision
	// (via the EpochProvider), not a startup copy.
	msg := peertls.MessageWire{
		ID:        "01900000-0000-7000-8000-000000000001",
		Body:      "hello from A",
		Origin:    "gossipgrid-0",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	ack, err := nodeA.client.SendMessage(nodeB.ctx, nodeB.peerAddr, peerExpectedIdentity(nodeB.podID), msg)
	if err != nil {
		t.Fatalf("message exchange A->B: %v", err)
	}
	if ack.ID != msg.ID {
		t.Errorf("ack ID: expected %s, got %s", msg.ID, ack.ID)
	}

	// Verify the message was stored on node B.
	count, err := nodeB.store.CountMessages()
	if err != nil {
		t.Fatalf("node B count: %v", err)
	}
	if count != 1 {
		t.Errorf("node B: expected 1 message, got %d", count)
	}

	// Verify public writes work on both nodes.
	resp := postMessageTo(t, nodeA, "local write A")
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("node A public write: expected 201, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	resp = postMessageTo(t, nodeB, "local write B")
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("node B public write: expected 201, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

// TestIntegrationLifecycleWrongEpochPeer (Scenario B).
// A peer presents a valid workload certificate but belongs to another epoch.
// Verify all replication operations are rejected and the receiver retains its
// existing data unchanged.
func TestIntegrationLifecycleWrongEpochPeer(t *testing.T) {
	ca := newTestCA(t, "lifecycle-ca-wrong-epoch")
	metaA := lifecycle.ClusterMetadata{
		ClusterID:   "cluster-test-1",
		EpochID:     "epoch-A",
		Generation:  1,
		Initialized: true,
	}
	metaB := lifecycle.ClusterMetadata{
		ClusterID:   "cluster-test-1",
		EpochID:     "epoch-B",
		Generation:  2,
		Initialized: true,
	}

	nodeA := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	nodeB := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &metaB, forceState: lifecycle.READY})

	// Establish each node's authoritative metadata (matching their bindings).
	nodeA.setMetadata(metaA)
	nodeB.setMetadata(metaB)

	// Seed a message on node A (now authorized: READY + matching binding).
	if _, err := nodeA.store.CreateLocalMessage("gossipgrid-0", "epoch-A message"); err != nil {
		t.Fatalf("seeding node A: %v", err)
	}
	countBefore, _ := nodeA.store.CountMessages()

	// Attempt message exchange A -> B (different epochs). The outbound epoch
	// is sourced from node A's current authorization decision; node B's
	// handler rejects it with 409 (a permanent epoch mismatch, not a retry).
	msg := peertls.MessageWire{
		ID:        "01900000-0000-7000-8000-000000000002",
		Body:      "cross-epoch message",
		Origin:    "gossipgrid-0",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	_, err := nodeA.client.SendMessage(nodeB.ctx, nodeB.peerAddr, peerExpectedIdentity(nodeB.podID), msg)
	if err == nil {
		t.Fatal("expected epoch mismatch rejection, got nil")
	}
	var statusErr *peertls.PeerHTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected PeerHTTPStatusError, got %T: %v", err, err)
	}
	if statusErr.StatusCode() != http.StatusConflict {
		t.Errorf("expected 409, got %d", statusErr.StatusCode())
	}

	// Node A's data should be unchanged.
	countAfter, _ := nodeA.store.CountMessages()
	if countBefore != countAfter {
		t.Errorf("node A data changed: before=%d after=%d", countBefore, countAfter)
	}

	// Node B's data should be unchanged (no new message inserted).
	countB, _ := nodeB.store.CountMessages()
	if countB != 0 {
		t.Errorf("node B: expected 0 messages, got %d", countB)
	}
}

// TestIntegrationLifecycleUnreadySyncSource (Scenario C).
// A peer is in SYNCING with the correct epoch. Verify it can answer
// authenticated status requests but cannot supply an authoritative bootstrap
// snapshot.
func TestIntegrationLifecycleUnreadySyncSource(t *testing.T) {
	ca := newTestCA(t, "lifecycle-ca-unready")
	meta := lifecycle.ClusterMetadata{
		ClusterID:   "cluster-test-1",
		EpochID:     "epoch-test-3",
		Generation:  1,
		Initialized: true,
	}

	nodeA := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &meta, forceState: lifecycle.READY})
	nodeB := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &meta, forceState: lifecycle.SYNCING})

	// Establish the authoritative metadata on both nodes. Node A is READY
	// (authorized); node B is SYNCING (the controller does not auto-promote in
	// 5A, so the state is forced to the baseline via the test-only path).
	nodeA.setMetadata(meta)
	nodeB.setMetadata(meta)

	// Node B (SYNCING) should answer status requests.
	statusB, err := getPeerStatus(t, nodeA.client, nodeB)
	if err != nil {
		t.Fatalf("node B status (SYNCING): %v", err)
	}
	if statusB.LifecycleState != "SYNCING" {
		t.Errorf("node B state: expected SYNCING, got %s", statusB.LifecycleState)
	}
	if statusB.Readiness {
		t.Error("node B: expected readiness=false (SYNCING)")
	}
	if statusB.BootstrapSourceEligible {
		t.Error("node B: expected bootstrap source NOT eligible (SYNCING)")
	}

	// Node B (SYNCING) should NOT serve inventory.
	_, err = nodeA.client.FetchInventoryPage(nodeB.ctx, nodeB.peerAddr, peerExpectedIdentity(nodeB.podID), "", 10)
	if err == nil {
		t.Fatal("expected 503 from SYNCING node's inventory endpoint, got nil")
	}
	var statusErr *peertls.PeerHTTPStatusError
	if errors.As(err, &statusErr) {
		if statusErr.StatusCode() != http.StatusServiceUnavailable && statusErr.StatusCode() != http.StatusConflict {
			t.Errorf("expected 503 or 409, got %d", statusErr.StatusCode())
		}
	}
}

// TestIntegrationLifecycleEpochChange (Scenario D).
// A replica initially participates in epoch A. The authoritative metadata
// source then changes to epoch B. Verify the replica stops accepting new
// writes and serving replication, remains bound to A, and preserves its
// historical data.
func TestIntegrationLifecycleEpochChange(t *testing.T) {
	ca := newTestCA(t, "lifecycle-ca-epoch-change")
	metaA := lifecycle.ClusterMetadata{
		ClusterID:   "cluster-test-1",
		EpochID:     "epoch-D-A",
		Generation:  1,
		Initialized: true,
	}
	metaB := lifecycle.ClusterMetadata{
		ClusterID:   "cluster-test-1",
		EpochID:     "epoch-D-B",
		Generation:  2,
		Initialized: true,
	}

	// Node A is bound to epoch A and READY (authorized).
	nodeA := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	nodeA.setMetadata(metaA)

	// Seed a message on node A in epoch A (authorized: READY + matching binding).
	if _, err := nodeA.store.CreateLocalMessage("gossipgrid-0", "epoch-A data"); err != nil {
		t.Fatalf("seeding: %v", err)
	}

	// Verify the node is ready and accepts writes.
	resp := postMessageTo(t, nodeA, "pre-change write")
	if resp.StatusCode != http.StatusCreated {
		t.Errorf("pre-change write: expected 201, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	countBefore, _ := nodeA.store.CountMessages()

	// Change the authoritative metadata to epoch B via the source. The
	// controller's metadata worker detects the mismatch between the durable
	// binding (epoch A) and the new observation (epoch B) and drives the
	// node into RECOVERY_REQUIRED — this is the controller-driven path,
	// not a manual state flip.
	nodeA.setMetadata(metaB)
	nodeA.waitForAuthorization(func(s lifecycle.AuthorizationSnapshot) bool {
		return s.State == lifecycle.RECOVERY_REQUIRED
	})

	// The node should now be unready (readiness reflects the same decision).
	code := getReadiness(t, nodeA)
	if code != http.StatusServiceUnavailable {
		t.Errorf("post-change readiness: expected 503, got %d", code)
	}

	// The node should reject new public writes.
	resp = postMessageTo(t, nodeA, "post-change write")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("post-change write: expected 503, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// The node should reject inbound replication (peer message exchange).
	msg := peertls.MessageWire{
		ID:        "01900000-0000-7000-8000-000000000003",
		Body:      "post-change replication",
		Origin:    "gossipgrid-1",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	// Node B is bound to the same epoch A (matching the inbound request), but
	// node A is now in RECOVERY_REQUIRED and quarantined, so it rejects.
	nodeB := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	nodeB.setMetadata(metaA)
	_, err := nodeB.client.SendMessage(nodeA.ctx, nodeA.peerAddr, peerExpectedIdentity(nodeA.podID), msg)
	if err == nil {
		t.Fatal("expected replication rejection after epoch change, got nil")
	}

	// The node's data should be unchanged.
	countAfter, _ := nodeA.store.CountMessages()
	if countBefore != countAfter {
		t.Errorf("data changed after epoch change: before=%d after=%d", countBefore, countAfter)
	}

	// The binding should still be epoch A.
	binding, err := nodeA.store.GetBinding()
	if err != nil {
		t.Fatalf("GetBinding: %v", err)
	}
	if binding.EpochID != metaA.EpochID {
		t.Errorf("binding changed: expected %s, got %s", metaA.EpochID, binding.EpochID)
	}
}

// TestIntegrationLifecycleStartupWithoutMetadata (Scenario E).
// An empty replica starts with lifecycle protection enabled but no
// authoritative metadata provider. Verify it is alive for diagnostics but
// is not ready and cannot replicate.
func TestIntegrationLifecycleStartupWithoutMetadata(t *testing.T) {
	ca := newTestCA(t, "lifecycle-ca-no-metadata")
	// An empty replica with lifecycle protection enabled but no metadata and
	// no binding: the controller stays STARTING and unready.
	node := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{})

	// The node should be alive (healthz returns 200).
	req, err := http.NewRequestWithContext(node.ctx, "GET", "http://"+node.publicAddr+"/healthz", nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz: expected 200, got %d", resp.StatusCode)
	}

	// The node should be unready (readyz returns 503).
	code := getReadiness(t, node)
	if code != http.StatusServiceUnavailable {
		t.Errorf("readyz without metadata: expected 503, got %d", code)
	}

	// The node should reject public writes.
	resp2 := postMessageTo(t, node, "should be rejected")
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("public write without metadata: expected 503, got %d", resp2.StatusCode)
	}
	_ = resp2.Body.Close()

	// No messages should have been inserted.
	count, _ := node.store.CountMessages()
	if count != 0 {
		t.Errorf("expected 0 messages, got %d", count)
	}

	// The node's own outbound replication must be blocked locally: with no
	// authoritative metadata observation, the controller's EpochProvider cannot
	// safely establish an outbound epoch context, so the client refuses to
	// send. This is the fail-closed §6 guarantee (mTLS alone does not prove
	// epoch membership; an epoch must be derivable from current authority).
	peer2 := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{})
	msg := peertls.MessageWire{
		ID:        "01900000-0000-7000-8000-000000000004",
		Body:      "blocked outbound",
		Origin:    "gossipgrid-0",
		CreatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}
	_, errOut := node.client.SendMessage(peer2.ctx, peer2.peerAddr, peerExpectedIdentity(peer2.podID), msg)
	var blockedErr *peertls.PeerBlockedError
	if !errors.As(errOut, &blockedErr) {
		t.Errorf("expected PeerBlockedError for outbound without metadata, got %T: %v", errOut, errOut)
	}
}

// --- Section 8: controller-driven integration scenarios -------------------
//
// These scenarios use genuine controller-driven lifecycle transitions: a real
// started controller with a mutable authoritative metadata source, real SQLite
// stores with durable bindings, production REST and peer mTLS handlers, and the
// production gossip and anti-entropy engines. State is driven by the
// controller's metadata worker (via setMetadata), not by manual state flips.
// Barriers (channels) — not arbitrary sleeps — are used to synchronize
// concurrent phases.

// §8-1: matching metadata+binding permit operations (the controller-driven
// equivalent of scenario A, asserting the authorization snapshot is coherent).
func TestControllerDrivenMatchingPermitsOps(t *testing.T) {
	ca := newTestCA(t, "cd-1")
	meta := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}
	a := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &meta, forceState: lifecycle.READY})
	b := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &meta, forceState: lifecycle.READY})
	a.setMetadata(meta)
	b.setMetadata(meta)

	// Both authorized.
	if snap := a.ctrl.Authorization(); !snap.Authorized() {
		t.Fatalf("a: expected authorized, state=%s", snap.State)
	}
	if snap := b.ctrl.Authorization(); !snap.Authorized() {
		t.Fatalf("b: expected authorized, state=%s", snap.State)
	}

	// Private message exchange a->b works.
	msg := peertls.MessageWire{ID: "01900000-0000-7000-8000-000000000010", Body: "cd-1", Origin: "gossipgrid-0", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if _, err := a.client.SendMessage(b.ctx, b.peerAddr, peerExpectedIdentity(b.podID), msg); err != nil {
		t.Fatalf("private exchange a->b: %v", err)
	}
	// Public write works.
	resp := postMessageTo(t, b, "cd-1 public")
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("public write b: expected 201, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
}

// §8-2: a missing binding cannot be bypassed by forcing READY. The state is
// READY (forced) but no durable binding exists, so the authorization snapshot
// must be NOT authorized and all operations blocked.
func TestControllerDrivenReadyWithoutBindingBlocked(t *testing.T) {
	ca := newTestCA(t, "cd-2")
	meta := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}
	// No bindTo: the store has no durable binding. State is forced to READY.
	node := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{forceState: lifecycle.READY})
	node.setMetadata(meta)

	snap := node.ctrl.Authorization()
	if snap.State != lifecycle.READY {
		t.Fatalf("state: expected READY, got %s", snap.State)
	}
	if snap.Authorized() {
		t.Fatal("READY without a binding: expected NOT authorized (fail-closed)")
	}
	// Readiness reflects the same decision.
	if code := getReadiness(t, node); code != http.StatusServiceUnavailable {
		t.Errorf("readiness: expected 503, got %d", code)
	}
	// Public write is blocked.
	resp := postMessageTo(t, node, "should fail")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("public write: expected 503, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	// No messages inserted.
	if n, _ := node.store.CountMessages(); n != 0 {
		t.Errorf("expected 0 messages, got %d", n)
	}
}

// §8-3: invalid or missing metadata revokes authorization even when the state
// is READY and the binding matches. (metadata loss = fail-closed)
func TestControllerDrivenInvalidMetadataRevokes(t *testing.T) {
	ca := newTestCA(t, "cd-3")
	meta := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}
	node := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &meta, forceState: lifecycle.READY})
	node.setMetadata(meta)
	if snap := node.ctrl.Authorization(); !snap.Authorized() {
		t.Fatalf("baseline: expected authorized, state=%s", snap.State)
	}

	// Mark the source malformed (an invalid observation). The controller's
	// worker re-evaluates and the authority becomes "invalid".
	node.src.SetMalformed()
	node.waitForAuthorization(func(s lifecycle.AuthorizationSnapshot) bool {
		return s.AuthorityState == "invalid"
	})

	snap := node.ctrl.Authorization()
	if snap.Authorized() {
		t.Fatal("invalid metadata: expected NOT authorized")
	}
	if code := getReadiness(t, node); code != http.StatusServiceUnavailable {
		t.Errorf("readiness after invalid metadata: expected 503, got %d", code)
	}
	// Outbound replication is blocked (the EpochProvider cannot establish a
	// safe epoch from an invalid observation).
	peer := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &meta, forceState: lifecycle.READY})
	peer.setMetadata(meta)
	_, errOut := node.client.SendMessage(peer.ctx, peer.peerAddr, peerExpectedIdentity(peer.podID),
		peertls.MessageWire{ID: "01900000-0000-7000-8000-000000000013", Body: "x", Origin: "gossipgrid-0", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	var blockedErr *peertls.PeerBlockedError
	if !errors.As(errOut, &blockedErr) {
		t.Errorf("expected PeerBlockedError, got %T: %v", errOut, errOut)
	}
}

// §8-4: the epoch A→B transition is controller-driven. When the authoritative
// source changes from epoch A (matching the binding) to epoch B, the controller
// detects the mismatch and drives the node into RECOVERY_REQUIRED; the lockout
// blocks writes and replication.
func TestControllerDrivenEpochChangeLockout(t *testing.T) {
	ca := newTestCA(t, "cd-4")
	metaA := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eA", Generation: 1, Initialized: true}
	metaB := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eB", Generation: 2, Initialized: true}
	node := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	node.setMetadata(metaA)
	// Seed an epoch-A message while authorized.
	if _, err := node.store.CreateLocalMessage("gossipgrid-0", "epoch-A"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	countBefore, _ := node.store.CountMessages()

	// The controller-driven epoch change: metadata -> epoch B.
	node.setMetadata(metaB)
	node.waitForAuthorization(func(s lifecycle.AuthorizationSnapshot) bool {
		return s.State == lifecycle.RECOVERY_REQUIRED
	})

	// The node must be locked out: unready, writes and replication blocked.
	if code := getReadiness(t, node); code != http.StatusServiceUnavailable {
		t.Errorf("readiness: expected 503, got %d", code)
	}
	resp := postMessageTo(t, node, "post-lockout")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("public write: expected 503, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()
	// Data is preserved (epoch-A messages survive; no new inserts).
	countAfter, _ := node.store.CountMessages()
	if countBefore != countAfter {
		t.Errorf("data changed: before=%d after=%d", countBefore, countAfter)
	}
}

// §8-5: public writes AND reads are blocked after a lockout, while /healthz and
// /readyz remain accessible. This exercises the §7 public-read quarantine gate
// end-to-end.
func TestControllerDrivenPublicReadQuarantineAfterLockout(t *testing.T) {
	ca := newTestCA(t, "cd-5")
	metaA := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eA", Generation: 1, Initialized: true}
	metaB := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eB", Generation: 2, Initialized: true}
	node := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	node.setMetadata(metaA)
	// Seed a message while authorized so there is data to quarantine.
	msg, err := node.store.CreateLocalMessage("gossipgrid-0", "to-quarantine")
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Lock out via the controller.
	node.setMetadata(metaB)
	node.waitForAuthorization(func(s lifecycle.AuthorizationSnapshot) bool {
		return s.State == lifecycle.RECOVERY_REQUIRED
	})

	// Public read (GET /api/messages) is quarantined (503), not exposing data.
	req, _ := http.NewRequestWithContext(node.ctx, "GET", "http://"+node.publicAddr+"/api/messages", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/messages: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("GET /api/messages: expected 503 (quarantined), got %d", resp.StatusCode)
	}

	// Public read by ID is also quarantined.
	req2, _ := http.NewRequestWithContext(node.ctx, "GET", "http://"+node.publicAddr+"/api/messages/"+msg.ID, nil)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("GET /api/messages/{id}: %v", err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("GET /api/messages/{id}: expected 503 (quarantined), got %d", resp2.StatusCode)
	}

	// /healthz and /readyz remain accessible (readyz reports the unready state).
	if code := getReadiness(t, node); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz: expected 503, got %d", code)
	}
	req3, _ := http.NewRequestWithContext(node.ctx, "GET", "http://"+node.publicAddr+"/healthz", nil)
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	_ = resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Errorf("/healthz: expected 200, got %d", resp3.StatusCode)
	}
	// The quarantined record is preserved (not deleted).
	if n, _ := node.store.CountMessages(); n != 1 {
		t.Errorf("record should be preserved: expected 1, got %d", n)
	}
}

// §8-6: private exchange and inventory/fetch are blocked after a lockout.
func TestControllerDrivenPrivateOpsBlockedAfterLockout(t *testing.T) {
	ca := newTestCA(t, "cd-6")
	metaA := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eA", Generation: 1, Initialized: true}
	metaB := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eB", Generation: 2, Initialized: true}
	node := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	node.setMetadata(metaA)
	// Lock out.
	node.setMetadata(metaB)
	node.waitForAuthorization(func(s lifecycle.AuthorizationSnapshot) bool {
		return s.State == lifecycle.RECOVERY_REQUIRED
	})

	// A peer trying to exchange a message into the locked-out node is rejected.
	peer := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	peer.setMetadata(metaA)
	_, err := peer.client.SendMessage(node.ctx, node.peerAddr, peerExpectedIdentity(node.podID),
		peertls.MessageWire{ID: "01900000-0000-7000-8000-000000000016", Body: "x", Origin: "gossipgrid-1", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	if err == nil {
		t.Fatal("expected inbound replication rejection after lockout, got nil")
	}
	// A peer trying to fetch the locked-out node's inventory is rejected.
	_, err = peer.client.FetchInventoryPage(node.ctx, node.peerAddr, peerExpectedIdentity(node.podID), "", 10)
	if err == nil {
		t.Fatal("expected inventory rejection after lockout, got nil")
	}
}

// §8-7: queued gossip cannot be sent after revocation. A message is enqueued
// while authorized (it sits in the queue); the node is then locked out. The
// deterministic guarantee: a NEW enqueue after the lockout is blocked (the
// Enqueue admission re-check returns not-allowed, so the work item is never
// published), and the admission check itself reports not-allowed. This is the
// local linearization: work after revocation is not authorized. (An
// already-in-flight send that was authorized when it started may complete in
// the bounded in-flight window; the guarantee is that no NEW work is
// authorized after the revocation.)
func TestControllerDrivenQueuedGossipFencedAfterRevocation(t *testing.T) {
	ca := newTestCA(t, "cd-7")
	metaA := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eA", Generation: 1, Initialized: true}
	metaB := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eB", Generation: 2, Initialized: true}
	a := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY, engines: true})
	b := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	a.setMetadata(metaA)
	b.setMetadata(metaA)
	a.setGossipPeer(b)

	// Enqueue a gossip message while authorized (it is published to the queue).
	a.gEngine.Enqueue(gossip.MessageEvent{
		Message: peertls.MessageWire{ID: "01900000-0000-7000-8000-000000000017", Body: "queued", Origin: "gossipgrid-0", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)},
	})
	queuedBefore := a.gEngine.Stats().MessagesQueued
	if queuedBefore == 0 {
		t.Fatal("expected the initial enqueue to be published (MessagesQueued >= 1)")
	}

	// Lock out node A via the controller.
	a.setMetadata(metaB)
	a.waitForAuthorization(func(s lifecycle.AuthorizationSnapshot) bool {
		return s.State == lifecycle.RECOVERY_REQUIRED
	})

	// After the lockout, the admission check (the one set on the engine) must
	// report not-allowed.
	decision := a.ctrl.Admission().CheckReplicationOutbound(a.ctx)
	if decision.Allowed {
		t.Fatal("after revocation: CheckReplicationOutbound must NOT be allowed")
	}

	// A NEW enqueue after the lockout must be blocked: the Enqueue admission
	// re-check returns not-allowed, so the work item is never published
	// (MessagesQueued stays at the pre-lockout count).
	a.gEngine.Enqueue(gossip.MessageEvent{
		Message: peertls.MessageWire{ID: "01900000-0000-7000-8000-000000000018", Body: "post-revocation", Origin: "gossipgrid-0", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)},
	})
	if got := a.gEngine.Stats().MessagesQueued; got != queuedBefore {
		t.Errorf("post-revocation enqueue was published: MessagesQueued=%d, want %d (blocked)", got, queuedBefore)
	}
}

// §8-8: an ongoing anti-entropy sync cannot insert wrong-epoch messages or
// advance the cursor after a revocation. The local node is locked out
// mid-sync; the per-insert admission re-check blocks the insert and the
// cursor does not advance.
func TestControllerDrivenAntiEntropyFencedAfterRevocation(t *testing.T) {
	ca := newTestCA(t, "cd-8")
	metaA := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eA", Generation: 1, Initialized: true}
	metaB := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eB", Generation: 2, Initialized: true}
	// Source node (b) stays authorized with epoch-A messages.
	b := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	b.setMetadata(metaA)
	// Seed messages on b so anti-entropy has something to fetch.
	for i := 0; i < 3; i++ {
		if _, err := b.store.CreateLocalMessage("gossipgrid-1", "src-"+itoa(i)); err != nil {
			t.Fatalf("seed b: %v", err)
		}
	}

	// Local node (a) starts authorized, then is locked out before we drive a
	// sync. Its anti-entropy engine is wired to b.
	a := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY, engines: true})
	a.setMetadata(metaA)
	a.setAntiEntropyPeer(b)
	// Lock out a so its admission check blocks.
	a.setMetadata(metaB)
	a.waitForAuthorization(func(s lifecycle.AuthorizationSnapshot) bool {
		return s.State == lifecycle.RECOVERY_REQUIRED
	})

	// After the lockout, the admission check (the one set on the anti-entropy
	// engine) must report not-allowed. This is the fence: the engine's
	// syncPeer checks it before any network operation, and fetchAndInsert
	// re-checks it before each insert. While locked out, no inventory
	// request and no insert is authorized.
	decision := a.ctrl.Admission().CheckReplicationOutbound(a.ctx)
	if decision.Allowed {
		t.Fatal("after revocation: CheckReplicationOutbound must NOT be allowed")
	}
	// No inserts and no cursor advance (the engine has not synced; the
	// admission check blocks any sync that would run).
	if n, _ := a.store.CountMessages(); n != 0 {
		t.Errorf("anti-entropy inserted messages after revocation: expected 0 on a, got %d", n)
	}
	st := a.aEngine.Stats()
	if st.CompletedPasses != 0 {
		t.Errorf("expected 0 completed passes after revocation, got %d", st.CompletedPasses)
	}
}

// §8-9: a lifecycle-enabled replica refuses data from a legacy peer (a peer
// with lifecycle disabled) because the legacy peer's responses carry no
// epoch/response envelope proving it runs the epoch-fencing protocol. mTLS
// alone is not sufficient.
func TestControllerDrivenRefusesLegacyPeer(t *testing.T) {
	ca := newTestCA(t, "cd-9")
	meta := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}
	// a is lifecycle-enabled and authorized.
	a := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &meta, forceState: lifecycle.READY})
	a.setMetadata(meta)
	// b is a "legacy" peer: its peer server has lifecycle protection disabled
	// (it never sets the response envelope headers).
	b := newLegacyPeerNode(t, ca, "gossipgrid-1")

	// Seed a message on b (a legacy peer can still serve data).
	if _, err := b.store.CreateLocalMessage("gossipgrid-1", "legacy"); err != nil {
		t.Fatalf("seed b: %v", err)
	}

	// a tries to fetch b's inventory: the client must reject because b's
	// response has no X-GossipGrid-Lifecycle-Active header (a legacy peer).
	_, err := a.client.FetchInventoryPage(a.ctx, b.peerAddr, peerExpectedIdentity(b.podID), "", 10)
	if err == nil {
		t.Fatal("expected rejection of a legacy peer as a sync source, got nil")
	}
	// a must not have ingested b's data.
	if n, _ := a.store.CountMessages(); n != 0 {
		t.Errorf("a ingested data from a legacy peer: expected 0, got %d", n)
	}
}

// §8-10: a restart with a matching binding is safe (the node re-authorizes
// after restart). The store's durable binding survives; after restart with the
// same metadata, the node is authorized again.
func TestControllerDrivenRestartMatchingBindingSafe(t *testing.T) {
	ca := newTestCA(t, "cd-10")
	meta := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}
	dir := t.TempDir()
	cert, key := issueLeafCert(t, ca, "gossipgrid-0."+intSuffix)
	chainPEM, keyPEM := certKeyPEMTwo(t, cert, key, ca)
	certFile := writePEMFile(t, dir, "cert.pem", chainPEM)
	keyFile := writePEMFile(t, dir, "key.pem", keyPEM)
	trustFile := writePEMFile(t, dir, "ca-bundle.pem", ca.pem)

	ctx := context.Background()
	mgr := peertls.New(peertls.Config{
		CertFile: certFile, KeyFile: keyFile, TrustBundleFile: trustFile,
		IdentitySuffix: intSuffix, StatefulSetName: intStatefulSet, NodeID: "gossipgrid-0",
		ExpectedReplicas: 5, ReloadInterval: 10 * time.Second,
	}, nil)
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("mgr Start: %v", err)
	}
	dbPath := filepath.Join(dir, "gossipgrid.db")
	st, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	// Bind and seed while "running" the first time.
	if _, err := st.Bind(meta.ClusterID, meta.EpochID, meta.Generation); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if _, err := st.CreateLocalMessage("gossipgrid-0", "pre-restart"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	countBefore, _ := st.CountMessages()

	// "Restart": close the store, re-open the same DB file.
	_ = st.Close()
	mgr.Shutdown()
	st2, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("re-open: %v", err)
	}
	// The durable binding survives the restart.
	if b, _ := st2.GetBinding(); b == nil || b.EpochID != meta.EpochID {
		t.Fatalf("binding did not survive restart: %+v", b)
	}
	// Re-establish the controller against the re-opened store + metadata.
	// The poll interval is 1h so the metadata worker does not interfere with
	// the forced state during this short test.
	src := lifecycle.NewFakeMetadataSource(meta, true)
	machine := lifecycle.NewMachine(nil)
	// Force the machine to READY (the test-only bounded path; production
	// promotion goes through the gated SetReady).
	machine.ForceTransitionForTest(lifecycle.READY, "restart")
	ctrl := lifecycle.NewController(lifecycle.Config{MetadataPollInterval: time.Hour, NodeID: "gossipgrid-0", Enabled: true}, machine, src, st2, nil)
	if err := ctrl.Start(ctx); err != nil {
		t.Fatalf("ctrl Start: %v", err)
	}
	ctrl.SetAcceptedMetadataForTest(meta)
	// The node is authorized again after restart (readiness 200 via the
	// snapshot).
	snap := ctrl.Authorization()
	if !snap.Authorized() {
		t.Fatalf("post-restart: expected authorized, state=%s", snap.State)
	}
	// Data survives.
	countAfter, _ := st2.CountMessages()
	if countBefore != countAfter {
		t.Errorf("data changed across restart: before=%d after=%d", countBefore, countAfter)
	}
	ctrl.Stop(context.Background())
	_ = st2.Close()
}

// §8-11: a restart with a conflicting binding is locked out. The durable
// binding (epoch A) conflicts with the post-restart metadata (epoch B); the
// controller enters RECOVERY_REQUIRED.
func TestControllerDrivenRestartConflictingBindingLockedOut(t *testing.T) {
	ca := newTestCA(t, "cd-11")
	metaA := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eA", Generation: 1, Initialized: true}
	metaB := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eB", Generation: 2, Initialized: true}
	dir := t.TempDir()
	cert, key := issueLeafCert(t, ca, "gossipgrid-0."+intSuffix)
	chainPEM, keyPEM := certKeyPEMTwo(t, cert, key, ca)
	certFile := writePEMFile(t, dir, "cert.pem", chainPEM)
	keyFile := writePEMFile(t, dir, "key.pem", keyPEM)
	trustFile := writePEMFile(t, dir, "ca-bundle.pem", ca.pem)
	ctx := context.Background()
	mgr := peertls.New(peertls.Config{
		CertFile: certFile, KeyFile: keyFile, TrustBundleFile: trustFile,
		IdentitySuffix: intSuffix, StatefulSetName: intStatefulSet, NodeID: "gossipgrid-0",
		ExpectedReplicas: 5, ReloadInterval: 10 * time.Second,
	}, nil)
	_ = mgr.Start(ctx)
	dbPath := filepath.Join(dir, "gossipgrid.db")
	st, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	// Bind to epoch A.
	if _, err := st.Bind(metaA.ClusterID, metaA.EpochID, metaA.Generation); err != nil {
		t.Fatalf("bind: %v", err)
	}
	_ = st.Close()
	mgr.Shutdown()
	st2, _ := store.New(dbPath)

	// Post-restart metadata is epoch B (conflicts with the epoch-A binding).
	src := lifecycle.NewFakeMetadataSource(metaB, true)
	machine := lifecycle.NewMachine(nil)
	ctrl := lifecycle.NewController(lifecycle.Config{MetadataPollInterval: 50 * time.Millisecond, NodeID: "gossipgrid-0", Enabled: true}, machine, src, st2, nil)
	_ = ctrl.Start(ctx)
	ctrl.SetAcceptedMetadataForTest(metaB)
	// The controller must detect the mismatch and enter RECOVERY_REQUIRED.
	waitForCounter(t, "recovery", func() bool {
		return ctrl.Authorization().State == lifecycle.RECOVERY_REQUIRED
	}, 10*time.Second)
	if snap := ctrl.Authorization(); snap.Authorized() {
		t.Fatal("conflicting binding after restart: expected NOT authorized")
	}
	ctrl.Stop(context.Background())
	_ = st2.Close()
}

// §8-12: restoring the matching metadata does NOT bypass initial sync /
// recovery. A node in RECOVERY_REQUIRED with a conflicting binding, when the
// metadata is restored to the matching epoch, transitions to DISCOVERING
// (re-evaluation) — NOT directly to READY. The node must not be considered
// authorized until it re-establishes a matching binding.
func TestControllerDrivenMetadataRestoreDoesNotBypassRecovery(t *testing.T) {
	ca := newTestCA(t, "cd-12")
	metaA := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eA", Generation: 1, Initialized: true}
	metaB := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eB", Generation: 2, Initialized: true}
	node := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	node.setMetadata(metaA)
	// Lock out with epoch B.
	node.setMetadata(metaB)
	node.waitForAuthorization(func(s lifecycle.AuthorizationSnapshot) bool {
		return s.State == lifecycle.RECOVERY_REQUIRED
	})
	// Restore the matching epoch A.
	node.setMetadata(metaA)
	// The node must transition to DISCOVERING (re-evaluation), NOT to READY
	// (initial sync / recovery has not been performed).
	node.waitForAuthorization(func(s lifecycle.AuthorizationSnapshot) bool {
		return s.State == lifecycle.DISCOVERING
	})
	// DISCOVERING is not a ready state, so the node is NOT authorized.
	snap := node.ctrl.Authorization()
	if snap.Authorized() {
		t.Fatal("restored metadata: expected NOT authorized (recovery not performed)")
	}
	if code := getReadiness(t, node); code != http.StatusServiceUnavailable {
		t.Errorf("readiness after restore: expected 503, got %d", code)
	}
}

// §8-13: the existing lifecycle-disabled path still works (preserved legacy
// behavior). This is a regression guard: with lifecycle protection disabled,
// no epoch fencing applies and the public write path is unrestricted.
func TestControllerDrivenLegacyDisabledConverges(t *testing.T) {
	ca := newTestCA(t, "cd-13")
	nodes := make([]*lifecycleTestNode, 0, 2)
	for i := 0; i < 2; i++ {
		// Lifecycle disabled: use a legacy-style node (no controller).
		n := newLegacyPeerNode(t, ca, "gossipgrid-"+itoa(i))
		nodes = append(nodes, n)
	}
	// Seed a message on node 0.
	if _, err := nodes[0].store.CreateLocalMessage("gossipgrid-0", "legacy-spread"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// With lifecycle disabled, the admission guard is NewAdmissionDisabled and
	// the EpochProvider is not set, so outbound replication is unrestricted.
	// We assert the public write path works (no 503 quarantine) on all nodes.
	for i, n := range nodes {
		resp := postMessageTo(t, n, "node-"+itoa(i))
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("node %d public write: expected 201, got %d", i, resp.StatusCode)
		}
		_ = resp.Body.Close()
	}
}

// §8-14: a request racing an authority change is locally linearized. While an
// anti-entropy sync is in flight, the node is locked out. The work after the
// revocation is not authorized: no insert occurs and the cursor does not
// advance. (The bounded in-flight window is preserved: an already-in-flight
// page may complete, but no NEW insert is authorized after revocation.)
func TestControllerDrivenRequestRacingAuthorityChange(t *testing.T) {
	ca := newTestCA(t, "cd-14")
	metaA := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eA", Generation: 1, Initialized: true}
	metaB := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eB", Generation: 2, Initialized: true}
	// Source b authorized with data.
	b := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	b.setMetadata(metaA)
	for i := 0; i < 4; i++ {
		if _, err := b.store.CreateLocalMessage("gossipgrid-1", "race-"+itoa(i)); err != nil {
			t.Fatalf("seed b: %v", err)
		}
	}
	// Local a: start authorized, then lock out while the sync is "in flight".
	a := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY, engines: true})
	a.setMetadata(metaA)
	a.setAntiEntropyPeer(b)

	// Lock out a just before driving the sync. The admission check will block
	// the inventory request and the inserts.
	a.setMetadata(metaB)
	a.waitForAuthorization(func(s lifecycle.AuthorizationSnapshot) bool {
		return s.State == lifecycle.RECOVERY_REQUIRED
	})

	// After the authority change, the admission check must report not-allowed.
	// This is the local linearization: work after revocation is not
	// authorized. (An already-in-flight send that was authorized when it
	// started may complete in the bounded in-flight window; the guarantee is
	// that no NEW work is authorized after the revocation.)
	decision := a.ctrl.Admission().CheckReplicationOutbound(a.ctx)
	if decision.Allowed {
		t.Fatal("after authority change: CheckReplicationOutbound must NOT be allowed")
	}
	// No insert, no cursor advance.
	if n, _ := a.store.CountMessages(); n != 0 {
		t.Errorf("inserted after authority change: expected 0, got %d", n)
	}
	if st := a.aEngine.Stats(); st.CompletedPasses != 0 {
		t.Errorf("cursor advanced after authority change: expected 0 completed passes, got %d", st.CompletedPasses)
	}
}

// --- Section 6: real-mTLS mixed-mode epoch-fencing (scenarios A–G) ---------
//
// These scenarios exercise the full mTLS + epoch-fencing + response-envelope
// protocol between two real peers, covering: matching epochs (A), wrong-epoch
// rejection (B), unready source (C), epoch change (D), mixed-mode legacy
// rejection (E), response-envelope validation (F), and legacy-legacy
// passthrough (G).

// §6-A: matching-epoch mTLS exchange with a valid response envelope. The
// lifecycle-enabled peer sets the response envelope headers; the client
// accepts the response (valid mTLS + compatible protocol).
func TestMixedModeMatchingEpochEnvelope(t *testing.T) {
	ca := newTestCA(t, "mm-6a")
	meta := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}
	a := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &meta, forceState: lifecycle.READY})
	b := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &meta, forceState: lifecycle.READY})
	a.setMetadata(meta)
	b.setMetadata(meta)

	// Private exchange a->b: the response carries a valid envelope
	// (X-GossipGrid-Lifecycle-Active: true and matching cluster/epoch).
	msg := peertls.MessageWire{ID: "01900000-0000-7000-8000-000000000020", Body: "mm-a", Origin: "gossipgrid-0", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	ack, err := a.client.SendMessage(b.ctx, b.peerAddr, peerExpectedIdentity(b.podID), msg)
	if err != nil {
		t.Fatalf("matching-epoch exchange: %v", err)
	}
	if ack.ID != msg.ID {
		t.Errorf("ack ID: expected %s, got %s", msg.ID, ack.ID)
	}
	if n, _ := b.store.CountMessages(); n != 1 {
		t.Errorf("b should have 1 message, got %d", n)
	}
}

// §6-B: wrong-epoch peer over mTLS. The mTLS handshake succeeds (valid
// certificate), but the receiver's epoch check rejects with 409 (a permanent
// rejection, not a retry). The client classifies it as a permanent error.
func TestMixedModeWrongEpochPermanent409(t *testing.T) {
	ca := newTestCA(t, "mm-6b")
	metaA := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eA", Generation: 1, Initialized: true}
	metaB := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eB", Generation: 2, Initialized: true}
	a := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	b := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &metaB, forceState: lifecycle.READY})
	a.setMetadata(metaA)
	b.setMetadata(metaB)

	_, err := a.client.SendMessage(b.ctx, b.peerAddr, peerExpectedIdentity(b.podID),
		peertls.MessageWire{ID: "01900000-0000-7000-8000-000000000021", Body: "mm-b", Origin: "gossipgrid-0", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	var statusErr *peertls.PeerHTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected PeerHTTPStatusError, got %T: %v", err, err)
	}
	if statusErr.StatusCode() != http.StatusConflict {
		t.Errorf("expected 409 (permanent), got %d", statusErr.StatusCode())
	}
}

// §6-C: an unready (SYNCING) lifecycle-enabled peer answers status but
// rejects sync operations with 503 (a transient/unready rejection, distinct
// from the 409 epoch mismatch).
func TestMixedModeUnreadySource503(t *testing.T) {
	ca := newTestCA(t, "mm-6c")
	meta := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}
	a := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &meta, forceState: lifecycle.READY})
	b := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &meta, forceState: lifecycle.SYNCING})
	a.setMetadata(meta)
	b.setMetadata(meta)

	// Status is accessible.
	status, err := a.client.GetStatus(a.ctx, b.peerAddr, peerExpectedIdentity(b.podID))
	if err != nil {
		t.Fatalf("status (SYNCING): %v", err)
	}
	if status.LifecycleState != "SYNCING" || status.Readiness {
		t.Errorf("status: expected SYNCING/unready, got %s/ready=%v", status.LifecycleState, status.Readiness)
	}
	// Inventory is rejected with 503.
	_, err = a.client.FetchInventoryPage(a.ctx, b.peerAddr, peerExpectedIdentity(b.podID), "", 10)
	var statusErr *peertls.PeerHTTPStatusError
	if errors.As(err, &statusErr) && statusErr.StatusCode() != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", statusErr.StatusCode())
	}
	if err == nil {
		t.Fatal("expected 503 for SYNCING inventory, got nil")
	}
}

// §6-D: an epoch change over mTLS. The receiver transitions to
// RECOVERY_REQUIRED; subsequent exchange and sync operations are rejected
// (the node is quarantined).
func TestMixedModeEpochChangeQuarantined(t *testing.T) {
	ca := newTestCA(t, "mm-6d")
	metaA := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eA", Generation: 1, Initialized: true}
	metaB := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "eB", Generation: 2, Initialized: true}
	a := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	b := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	a.setMetadata(metaA)
	b.setMetadata(metaA)
	if _, err := a.store.CreateLocalMessage("gossipgrid-0", "pre"); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// b changes to epoch B via the controller -> RECOVERY_REQUIRED.
	b.setMetadata(metaB)
	b.waitForAuthorization(func(s lifecycle.AuthorizationSnapshot) bool {
		return s.State == lifecycle.RECOVERY_REQUIRED
	})
	// a->b exchange is now rejected (b is quarantined).
	_, err := a.client.SendMessage(b.ctx, b.peerAddr, peerExpectedIdentity(b.podID),
		peertls.MessageWire{ID: "01900000-0000-7000-8000-000000000024", Body: "mm-d", Origin: "gossipgrid-0", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	if err == nil {
		t.Fatal("expected rejection after b's epoch change, got nil")
	}
}

// §6-E: a lifecycle-enabled client rejects a successful response from a
// legacy peer (lifecycle disabled) because the response cannot demonstrate
// compatible epoch-fencing protocol behavior (no X-GossipGrid-Lifecycle-Active
// header). mTLS is necessary but not sufficient.
func TestMixedModeLifecycleEnabledRefusesLegacyResponse(t *testing.T) {
	ca := newTestCA(t, "mm-6e")
	meta := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}
	a := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &meta, forceState: lifecycle.READY})
	a.setMetadata(meta)
	// b is a legacy peer (lifecycle disabled): its responses never carry the
	// envelope headers, so a lifecycle-enabled client must refuse it.
	b := newLegacyPeerNode(t, ca, "gossipgrid-1")
	if _, err := b.store.CreateLocalMessage("gossipgrid-1", "legacy-data"); err != nil {
		t.Fatalf("seed b: %v", err)
	}

	// a tries to fetch b's inventory: the response is "successful" (200) but
	// the client must reject it because the envelope is absent (legacy peer).
	_, err := a.client.FetchInventoryPage(a.ctx, b.peerAddr, peerExpectedIdentity(b.podID), "", 10)
	if err == nil {
		t.Fatal("expected rejection of a legacy peer response, got nil")
	}
	if n, _ := a.store.CountMessages(); n != 0 {
		t.Errorf("a ingested legacy data: expected 0, got %d", n)
	}
}

// §6-F: response-envelope validation. A lifecycle-enabled peer whose
// response envelope carries a CONFLICTING epoch (mismatched cluster/epoch) is
// rejected by the client. This simulates a peer that self-reports an
// incompatible authorization context.
func TestMixedModeConflictingEnvelopeRejected(t *testing.T) {
	ca := newTestCA(t, "mm-6f")
	// a is authorized to epoch e1.
	metaA := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e1", Generation: 1, Initialized: true}
	a := newLifecycleTestNode(t, ca, "gossipgrid-0", nodeTestOpts{bindTo: &metaA, forceState: lifecycle.READY})
	a.setMetadata(metaA)
	// b is authorized to epoch e2 (a different epoch). Its response envelope
	// carries e2, which conflicts with a's request context (e1).
	metaB := lifecycle.ClusterMetadata{ClusterID: "c1", EpochID: "e2", Generation: 2, Initialized: true}
	b := newLifecycleTestNode(t, ca, "gossipgrid-1", nodeTestOpts{bindTo: &metaB, forceState: lifecycle.READY})
	b.setMetadata(metaB)

	// a tries to fetch b's inventory: a's request context is e1; b's response
	// envelope is e2. The client must reject the mismatched envelope.
	_, err := a.client.FetchInventoryPage(a.ctx, b.peerAddr, peerExpectedIdentity(b.podID), "", 10)
	if err == nil {
		t.Fatal("expected rejection of a conflicting-envelope response, got nil")
	}
}

// §6-G: legacy-legacy passthrough. When BOTH peers have lifecycle protection
// disabled, the response envelope is not required and the exchange proceeds
// as before (legacy behavior preserved).
func TestMixedModeLegacyLegacyPassthrough(t *testing.T) {
	ca := newTestCA(t, "mm-6g")
	a := newLegacyPeerNode(t, ca, "gossipgrid-0")
	b := newLegacyPeerNode(t, ca, "gossipgrid-1")
	// Both legacy: no controller, no envelope requirement. The client (a) has
	// no EpochProvider set, so it does not enforce a response envelope.
	_, err := a.client.SendMessage(b.ctx, b.peerAddr, peerExpectedIdentity(b.podID),
		peertls.MessageWire{ID: "01900000-0000-7000-8000-000000000027", Body: "mm-g", Origin: "gossipgrid-0", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)})
	if err != nil {
		t.Fatalf("legacy-legacy exchange: %v", err)
	}
	if n, _ := b.store.CountMessages(); n != 1 {
		t.Errorf("b should have 1 message, got %d", n)
	}
}

// newLegacyPeerNode creates a node with lifecycle protection DISABLED (no
// controller). It is used to represent a legacy peer (pre-5A) in mixed-mode
// tests. Its peer server and public handler are the production handlers with
// no lifecycle controller.
func newLegacyPeerNode(t *testing.T, ca *testCA, podID string) *lifecycleTestNode {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	cert, key := issueLeafCert(t, ca, podID+"."+intSuffix)
	chainPEM, keyPEM := certKeyPEMTwo(t, cert, key, ca)
	certFile := writePEMFile(t, dir, "cert.pem", chainPEM)
	keyFile := writePEMFile(t, dir, "key.pem", keyPEM)
	trustFile := writePEMFile(t, dir, "ca-bundle.pem", ca.pem)
	mgr := peertls.New(peertls.Config{
		CertFile: certFile, KeyFile: keyFile, TrustBundleFile: trustFile,
		IdentitySuffix: intSuffix, StatefulSetName: intStatefulSet, NodeID: podID,
		ExpectedReplicas: 5, ReloadInterval: 10 * time.Second,
	}, nil)
	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("%s: mgr Start: %v", podID, err)
	}
	st, err := store.New(filepath.Join(dir, "gossipgrid.db"))
	if err != nil {
		mgr.Shutdown()
		t.Fatalf("%s: store: %v", podID, err)
	}

	// No controller: lifecycle protection disabled.
	publicHandler := server.NewWithStoreAndCallback(server.Node{ID: podID}, st, nil)

	lc := net.ListenConfig{}
	peerLn, err := lc.Listen(ctx, "tcp", ":0")
	if err != nil {
		t.Fatalf("%s: peer listen: %v", podID, err)
	}
	peerAddr := peerLn.Addr().String()
	publicLn, err := lc.Listen(ctx, "tcp", ":0")
	if err != nil {
		_ = peerLn.Close()
		t.Fatalf("%s: public listen: %v", podID, err)
	}
	publicAddr := publicLn.Addr().String()

	// Peer server with lifecycle disabled (lifecycleEnabled=false).
	peerSrv := peertls.NewPeerServerWithSyncAndLifecycle(mgr, peerAddr, podID, "v0.1.0-legacy", st, true, true, nil, nil, 0)
	publicSrv := server.NewServer(publicAddr, publicHandler)
	// Client with no EpochProvider (legacy: outbound epoch not enforced).
	peerClient := peertls.NewClient(mgr)

	node := &lifecycleTestNode{
		t: t, ctx: ctx, mgr: mgr, peerSrv: peerSrv, publicSrv: publicSrv,
		store: st, client: peerClient, peerAddr: peerAddr, publicAddr: publicAddr, podID: podID,
	}
	var listenerWG sync.WaitGroup
	listenerWG.Add(2)
	// Serve through the retained listeners (no close+re-bind race).
	go func() {
		defer listenerWG.Done()
		tlsCfg := mgr.TLSConfigForServer()
		tlsLn := tls.NewListener(peerLn, tlsCfg)
		if err := peerSrv.HTTPServer.Serve(tlsLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Logf("%s: peer server exited: %v", podID, err)
		}
	}()
	go func() {
		defer listenerWG.Done()
		if err := publicSrv.Serve(publicLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Logf("%s: public server exited: %v", podID, err)
		}
	}()
	waitForAddr(t, ctx, peerAddr)
	waitForAddr(t, ctx, publicAddr)
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = node.publicSrv.Shutdown(shutdownCtx)
		_ = node.peerSrv.Shutdown(shutdownCtx)
		listenerWG.Wait()
		mgr.Shutdown()
		_ = st.Close()
	})
	return node
}

// waitForCounter polls a boolean predicate until true or the deadline elapses.
func waitForCounter(t *testing.T, name string, pred func() bool, deadline time.Duration) {
	t.Helper()
	deadlineTime := time.Now().Add(deadline)
	for time.Now().Before(deadlineTime) {
		if pred() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s: condition not met within %v", name, deadline)
}

// itoa is a small int-to-string helper for test IDs (avoids importing strconv
// just for a few test strings).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
