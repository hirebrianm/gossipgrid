// Command gossipgrid is a single-instance node of the GossipGrid distributed
// application. For the first milestone it exposes public health/ready/root
// HTTP endpoints, reads configuration from the environment, and shuts down
// gracefully on SIGINT/SIGTERM.
//
// Later milestones add the local message store, peer mTLS, gossip, and
// synchronization state machine; those do not alter the shape of this
// bootstrap/shutdown skeleton.
package main

import (
	"context"
	"crypto/fips140"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gossipgrid/internal/antientropy"
	"gossipgrid/internal/config"
	"gossipgrid/internal/discovery"
	"gossipgrid/internal/gossip"
	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/peertls"
	"gossipgrid/internal/server"
	"gossipgrid/internal/store"
)

// Version is the build version injected by the linker (see Makefile/Dockerfile
// -X flag). It defaults to "dev" for local builds.
var Version = "dev"

// FIPSRequired is set at compile time by the linker flag
// -X main.FIPSRequired=true|false. It records whether the build was compiled
// with a FIPS 140-3 module (GOFIPS140=<version>) or not (GOFIPS140=off).
// The default "false" matches a plain `go build` without the flag.
// It is a string (not bool) because the Go linker -X flag only supports
// string-typed variables.
var FIPSRequired = "false"

// fipsRequired parses the FIPSRequired linker-injected string into a bool.
func fipsRequired() bool {
	return FIPSRequired == "true"
}

func main() {
	if err := run(); err != nil {
		slog.Error("gossipgrid stopped", "error", err)
		os.Exit(1)
	}
}

// run returns a named error so that deferred functions can modify the return
// value after the function body has set it. Without a named return, a deferred
// function that sets a close error would run after the return value is already
// determined, and that error would be silently lost.
func run() (retErr error) {
	cfg, err := config.Load()
	if err != nil {
		// The slog handler is not yet configured, so the error is returned to
		// main where it is logged with a default handler.
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: levelFromName(cfg.LogLevel),
	}))
	slog.SetDefault(logger)

	fipsEnabled := fips140.Enabled()
	fipsModule := fips140.Version()

	// If this build was compiled with FIPS required (FIPSRequired="true") but
	// FIPS is not currently enabled at runtime (e.g. via GODEBUG=fips140=off),
	// fail startup. A runtime override must not silently disable a required
	// security configuration.
	if fipsRequired() && !fipsEnabled {
		return fmt.Errorf("FIPS 140-3 is required (module %s) but is disabled at runtime; refusing to start", fipsModule)
	}

	// Initialize local message storage before reporting readiness.
	// Storage must be ready before the HTTP listener starts accepting traffic.
	st, err := store.New(cfg.DBPath)
	if err != nil {
		return fmt.Errorf("initializing local storage at %q: %w", cfg.DBPath, err)
	}
	// Close the store exactly once on every exit path (listener failure,
	// signal shutdown, or return). The deferred function captures the *Store
	// pointer at defer time so it calls st.Close() with the live value, not a
	// stale one. A close error is joined into retErr (a named return) so that
	// it propagates to the caller without masking any earlier startup or
	// shutdown error.
	defer func() {
		if cerr := st.Close(); cerr != nil {
			logger.Error("closing database failed", slog.Any("error", cerr))
			retErr = errors.Join(retErr, cerr)
		}
	}()

	logger.Info("starting gossipgrid",
		slog.String("version", Version),
		slog.String("node", cfg.NodeID),
		slog.String("http_addr", cfg.HTTPAddr),
		slog.String("db_path", cfg.DBPath),
		slog.String("log_level", cfg.LogLevel),
		slog.String("discovery_mode", cfg.DiscoveryMode),
		slog.Bool("fips_enabled", fipsEnabled),
		slog.String("fips_module", fipsModule),
	)

	// Initialize peer discovery (Milestone 3A).
	// Discovery is observational for this stage: a failed DNS lookup does not
	// affect the public HTTP service's readiness. Peer connections are
	// established in later milestones (mTLS, gossip, synchronization).
	discCfg, err := buildDiscoveryConfig(cfg)
	if err != nil {
		return fmt.Errorf("building discovery config: %w", err)
	}
	discMgr := discovery.NewManager(discCfg, logger, nil)
	// Deferred shutdown ensures the refresh loop goroutine is stopped on every
	// exit path. Shutdown is idempotent and returns promptly for off/static
	// modes (no goroutine was started).
	defer discMgr.Shutdown()

	// Interrupt and terminate signals from the OS or a Kubernetes liveness
	// probe. signal.NotifyContext cancels the parent context on first signal.
	// This must be created before peer TLS Start (which needs the context to
	// derive its reload-loop context) and before the listeners start.
	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Milestone 4B: when gossip is enabled, build the gossip engine
	// before the HTTP servers so the callbacks can reference it.
	var gEngine *gossip.Engine
	var gStop func(ctx context.Context)
	if cfg.GossipEnabled {
		gEngine = buildGossipEngine(cfg, logger)
		if gEngine == nil {
			return fmt.Errorf("gossip engine initialization failed (see logs)")
		}
		// Defer gossip engine shutdown (after the HTTP servers are stopped).
		gStop = gEngine.Stop
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			defer cancel()
			gStop(shutdownCtx)
		}()
	}

	// Build the public server handler. When gossip is enabled, the
	// onMessage callback enqueues new local messages for peer fanout.
	var onMessage server.MessageCallback
	if gEngine != nil {
		onMessage = func(msg store.Message) {
			gEngine.Enqueue(gossip.MessageEvent{
				Message: peertls.MessageWire{
					ID:        msg.ID,
					Body:      msg.Body,
					Origin:    msg.Origin,
					CreatedAt: msg.CreatedAt.UTC().Format(time.RFC3339Nano),
				},
			})
		}
	}

	// Initialize the private peer mTLS transport (Milestone 3B). When peer TLS
	// is enabled, load credentials and create the peer server. A credential
	// load failure must fail startup clearly — no silent fallback to
	// unauthenticated communication, and the public server must not be left
	// running indefinitely.
	var peerMgr *peertls.Manager
	var peerSrv *peertls.PeerServer
	var lifeCtrl *lifecycle.Controller
	if cfg.PeerTLSEnabled {
		peerCfg, perr := buildPeerTLSConfig(cfg)
		if perr != nil {
			return fmt.Errorf("building peer TLS config: %w", perr)
		}
		peerMgr = peertls.New(peerCfg, logger)
		// Defer the reload-worker shutdown on every exit path. Shutdown is
		// idempotent and returns promptly.
		defer peerMgr.Shutdown()
		// Start loads credentials synchronously and starts the reload worker.
		// This must happen before we start listening so a credential failure
		// fails startup, not a late handshake.
		if err := peerMgr.Start(ctx); err != nil {
			return fmt.Errorf("starting peer TLS (initial credential load): %w", err)
		}

		// Milestone 5A: when lifecycle protection is enabled, create the
		// lifecycle controller. The controller owns the state machine, the
		// accepted metadata, and the local database binding. It runs a
		// metadata worker that periodically reads the authoritative source.
		//
		// In Milestone 5A there is no production Kubernetes metadata
		// provider. The metadata source is a no-op that always returns
		// ErrMetadataUnavailable. This is the correct fail-closed behavior:
		// without an authoritative observation, the replica cannot establish
		// its epoch and must remain unready.
		if cfg.LifecycleEnabled {
			machine := lifecycle.NewMachine(nil)
			// A no-op metadata source: always unavailable. The future
			// Kubernetes provider will replace this with a real
			// resourceVersion-based reader.
			metadataSource := lifecycle.NewFakeMetadataSourceUnavailable()
			lcCfg := lifecycle.Config{
				MetadataPollInterval: 30 * time.Second,
				NodeID:               cfg.NodeID,
				Enabled:              true,
			}
			lifeCtrl = lifecycle.NewController(lcCfg, machine, metadataSource, st, logger)
			// Defer the controller shutdown on every exit path.
			defer func() {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
				defer cancel()
				lifeCtrl.Stop(shutdownCtx)
			}()
			if err := lifeCtrl.Start(ctx); err != nil {
				return fmt.Errorf("starting lifecycle controller: %w", err)
			}
			lSnap := lifeCtrl.Snapshot()
			logger.Info("lifecycle controller started",
				slog.String("state", lSnap.State.String()),
				slog.String("reason", lSnap.Reason),
				slog.String("authority_state", lSnap.AuthorityState),
			)
		}

		// Milestone 4A/4B: pass the local store and the opt-in flag so the
		// private peer listener can register POST /internal/messages when
		// the operator has explicitly enabled authenticated peer message
		// exchange. When gossip is also enabled, the onNewMessage callback
		// enqueues newly received messages for further fanout.
		var onNewMsg func(msg *store.Message, senderPodID string)
		if gEngine != nil {
			onNewMsg = func(msg *store.Message, senderPodID string) {
				gEngine.Enqueue(gossip.MessageEvent{
					Message: peertls.MessageWire{
						ID:        msg.ID,
						Body:      msg.Body,
						Origin:    msg.Origin,
						CreatedAt: msg.CreatedAt.UTC().Format(time.RFC3339Nano),
					},
					SenderPodID: senderPodID,
				})
			}
		}
		// Milestone 4C/5A: when anti-entropy is enabled, use the sync-aware
		// constructor which also registers the private sync endpoints.
		// When lifecycle protection is enabled (lifeCtrl != nil), use the
		// lifecycle-aware constructor which fences all replication
		// endpoints on the controller's accepted metadata and state.
		if cfg.AntiEntropyEnabled {
			if onNewMsg != nil {
				if lifeCtrl != nil {
					peerSrv = peertls.NewPeerServerWithSyncAndLifecycle(peerMgr, cfg.PeerAddr, cfg.NodeID, Version, st, cfg.PeerMessageExchangeEnabled, true, onNewMsg, lifeCtrl)
				} else {
					peerSrv = peertls.NewPeerServerWithSync(peerMgr, cfg.PeerAddr, cfg.NodeID, Version, st, cfg.PeerMessageExchangeEnabled, true, onNewMsg)
				}
			} else {
				if lifeCtrl != nil {
					peerSrv = peertls.NewPeerServerWithSyncAndLifecycle(peerMgr, cfg.PeerAddr, cfg.NodeID, Version, st, cfg.PeerMessageExchangeEnabled, true, nil, lifeCtrl)
				} else {
					peerSrv = peertls.NewPeerServerWithSync(peerMgr, cfg.PeerAddr, cfg.NodeID, Version, st, cfg.PeerMessageExchangeEnabled, true, nil)
				}
			}
		} else if onNewMsg != nil {
			if lifeCtrl != nil {
				peerSrv = peertls.NewPeerServerWithSyncAndLifecycle(peerMgr, cfg.PeerAddr, cfg.NodeID, Version, st, cfg.PeerMessageExchangeEnabled, false, onNewMsg, lifeCtrl)
			} else {
				peerSrv = peertls.NewPeerServerWithStoreAndCallback(peerMgr, cfg.PeerAddr, cfg.NodeID, Version, st, cfg.PeerMessageExchangeEnabled, onNewMsg)
			}
		} else {
			if lifeCtrl != nil {
				peerSrv = peertls.NewPeerServerWithSyncAndLifecycle(peerMgr, cfg.PeerAddr, cfg.NodeID, Version, st, cfg.PeerMessageExchangeEnabled, false, nil, lifeCtrl)
			} else {
				peerSrv = peertls.NewPeerServerWithStore(peerMgr, cfg.PeerAddr, cfg.NodeID, Version, st, cfg.PeerMessageExchangeEnabled)
			}
		}
	}

	// Build the public server handler. When lifecycle protection is enabled
	// (lifeCtrl != nil), use the lifecycle-aware constructor which gates
	// readiness on the controller's state and rejects public writes while
	// unready. Otherwise, use the legacy constructor (always ready).
	var publicHandler http.Handler
	if lifeCtrl != nil {
		publicHandler = server.NewWithStoreAndCallbackAndLifecycle(server.Node{ID: cfg.NodeID}, st, onMessage, lifeCtrl)
	} else {
		publicHandler = server.NewWithStoreAndCallback(server.Node{ID: cfg.NodeID}, st, onMessage)
	}
	srv := server.NewServer(cfg.HTTPAddr, publicHandler)

	// Start listening in a goroutine so we can watch for OS signals on the
	// main goroutine. ListenAndServe returns nil when Shutdown has been called.
	errCh := make(chan error, 2)
	go func() {
		logger.Info("http listener starting", slog.String("addr", cfg.HTTPAddr))
		errCh <- srv.ListenAndServe()
	}()
	if peerSrv != nil {
		go func() {
			logger.Info("peer https listener starting", slog.String("addr", cfg.PeerAddr))
			errCh <- peerSrv.ListenAndServe()
		}()
	}

	// Start peer discovery. The context is cancelled on signal, which stops
	// the DNS refresh loop. For off and static modes, Start is a no-op or a
	// single deterministic pass with no background goroutine.
	if err := discMgr.Start(ctx); err != nil {
		return fmt.Errorf("starting peer discovery: %w", err)
	}
	snap := discMgr.Snapshot()
	logger.Info("peer discovery initialized",
		slog.String("mode", string(snap.Mode)),
		slog.Int("peers", len(snap.Peers)),
		slog.Int("expected_replicas", discCfg.ExpectedReplicas),
		slog.Bool("stale", snap.Stale),
	)
	if peerMgr != nil {
		logger.Info("peer mTLS enabled",
			slog.String("addr", cfg.PeerAddr),
			slog.String("identity_suffix", cfg.PeerIdentitySuffix),
			slog.Bool("peer_message_exchange", cfg.PeerMessageExchangeEnabled),
		)
	}

	// Milestone 4B/5A: create the peer client (shared by gossip and
	// anti-entropy). When lifecycle protection is enabled, set the
	// client's local epoch context so that all replication requests carry
	// the caller's accepted cluster and epoch headers.
	var peerClient *peertls.Client
	if peerMgr != nil {
		peerClient = peertls.NewClient(peerMgr)
		// Milestone 5A: source the outbound epoch context from the lifecycle
		// controller's CURRENT authorization decision, not a one-time startup
		// copy. The provider returns the authorized cluster/epoch when the
		// snapshot is authorized (ready state + fresh, valid, matching
		// observation + matching durable binding, no lockout, not stopped),
		// and an error otherwise (a lockout or no fresh observation blocks
		// outbound requests). A metadata change between two requests is
		// reflected in the next request (or the request is blocked), rather
		// than using a stale startup copy.
		if lifeCtrl != nil {
			peerClient.SetEpochProvider(newControllerEpochProvider(lifeCtrl))
		}
	}

	// Milestone 4B: start the gossip engine now that the peer client and
	// discovery are available. Set the delivery transport and snapshot
	// source, then start the worker pool.
	if gEngine != nil && peerMgr != nil {
		gEngine.SetDelivery(gossip.NewRealDelivery(peerClient))
		// Milestone 5A: set the admission check for outbound gossip.
		// When lifecycle protection is enabled, the check blocks Enqueue
		// when the replica is unready or has no accepted metadata.
		if lifeCtrl != nil {
			admission := lifeCtrl.Admission()
			gEngine.SetAdmissionCheck(func(ctx context.Context) error {
				decision := admission.CheckReplicationOutbound(ctx)
				if !decision.Allowed {
					return fmt.Errorf("lifecycle admission: %s", decision.Reason)
				}
				return nil
			})
		}
		// Set the snapshot source to bridge discovery.Snapshot into
		// gossip.PeerSnapshot (avoiding a circular import).
		gEngine.SetSnapshotSource(func() gossip.PeerSnapshot {
			ds := discMgr.Snapshot()
			peers := make([]gossip.PeerInfo, len(ds.Peers))
			for i, p := range ds.Peers {
				peers[i] = gossip.PeerInfo{
					Hostname:    p.Hostname,
					PodIdentity: p.PodIdentity,
					Address:     p.Address,
					LastSeen:    p.LastSeen,
				}
			}
			var localNode *gossip.PeerInfo
			if ds.LocalNode != nil {
				localNode = &gossip.PeerInfo{
					Hostname:    ds.LocalNode.Hostname,
					PodIdentity: ds.LocalNode.PodIdentity,
					Address:     ds.LocalNode.Address,
					LastSeen:    ds.LocalNode.LastSeen,
				}
			}
			return gossip.PeerSnapshot{
				Peers:     peers,
				LocalNode: localNode,
				Stale:     ds.Stale,
				Mode:      string(ds.Mode),
			}
		})
		if err := gEngine.Start(ctx); err != nil {
			return fmt.Errorf("starting gossip engine: %w", err)
		}
		logger.Info("gossip engine started",
			slog.Int("fanout", cfg.GossipFanout),
			slog.Int("rounds", cfg.GossipRounds),
			slog.Int("workers", cfg.GossipWorkers),
			slog.Int("queue_capacity", cfg.GossipQueueCapacity),
			slog.Int("max_attempts", cfg.GossipMaxAttempts),
		)
	}

	// Milestone 4C: when anti-entropy is enabled, build and start the
	// anti-entropy engine. It operates independently from the gossip engine
	// and can run with gossip disabled.
	if cfg.AntiEntropyEnabled && peerMgr != nil {
		aEngine := buildAntiEntropyEngine(cfg, logger)
		if aEngine == nil {
			return fmt.Errorf("anti-entropy engine initialization failed (see logs)")
		}
		// Defer anti-entropy engine shutdown.
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
			defer cancel()
			aEngine.Stop(shutdownCtx)
		}()

		aEngine.SetInventoryClient(peerClient)
		aEngine.SetMessageFetcher(peerClient)
		aEngine.SetLocalStore(st)
		// Milestone 5A: set the admission check for outbound anti-entropy.
		if lifeCtrl != nil {
			admission := lifeCtrl.Admission()
			aEngine.SetAdmissionCheck(func(ctx context.Context) error {
				decision := admission.CheckReplicationOutbound(ctx)
				if !decision.Allowed {
					return fmt.Errorf("lifecycle admission: %s", decision.Reason)
				}
				return nil
			})
			// Milestone 5A: set the commit guard so that the cursor commit
			// (advancement or full-sweep completion) is executed under the
			// controller's authorization read lock. The guard evaluates the
			// complete authorization decision ONCE, from a single non-
			// recursive snapshot (no second c.mu acquisition, no SQLite
			// read while the lock is held), and only runs the short
			// in-memory cursor mutation when that decision is authorized.
			// A revocation that completes before the guard enters is
			// observed and the cursor is not advanced; a revocation after
			// the commit linearizes is a normal, permitted revocation. The
			// non-recursive evaluation (one authorization evaluation, one
			// protected commit boundary) prevents the deadlock that a
			// recursive RLock (a callback re-reading the admission check
			// under the controller's read lock) would cause when a writer
			// (Stop or a metadata update) is waiting for the write lock.
			aEngine.SetCommitGuard(lifeCtrl)
		}
		// Set the peer source to bridge discovery.Snapshot into
		// antientropy.PeerSourceSnapshot (avoiding a circular import).
		aEngine.SetPeerSource(antientropy.NewDiscoveryPeerSource(cfg.PeerIdentitySuffix, func() antientropy.PeerSourceSnapshot {
			ds := discMgr.Snapshot()
			peers := make([]antientropy.PeerSourceInfo, len(ds.Peers))
			for i, p := range ds.Peers {
				peers[i] = antientropy.PeerSourceInfo{
					Hostname:    p.Hostname,
					PodIdentity: p.PodIdentity,
					Address:     p.Address,
				}
			}
			var localNode *antientropy.PeerSourceInfo
			if ds.LocalNode != nil {
				localNode = &antientropy.PeerSourceInfo{
					Hostname:    ds.LocalNode.Hostname,
					PodIdentity: ds.LocalNode.PodIdentity,
					Address:     ds.LocalNode.Address,
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
		}))
		if err := aEngine.Start(ctx); err != nil {
			return fmt.Errorf("starting anti-entropy engine: %w", err)
		}
		logger.Info("anti-entropy engine started",
			slog.Duration("interval", cfg.AntiEntropyInterval),
			slog.Int("page_size", cfg.AntiEntropyPageSize),
			slog.Int("fetch_batch_size", cfg.AntiEntropyFetchBatchSize),
			slog.Int("max_pages_per_peer", cfg.AntiEntropyMaxPagesPerPeer),
			slog.Int("workers", cfg.AntiEntropyWorkers),
			slog.Duration("request_timeout", cfg.AntiEntropyRequestTimeout),
		)
	}

	// Wait for a startup failure or a signal.
	select {
	case err := <-errCh:
		// A listener failed to start (e.g. address in use) before a signal
		// was received. Drain the other listener's error if it is already
		// available (non-blocking) so we don't lose it, then shut down both.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("listener failed to start", slog.Any("error", err))
			retErr = err
		}
		select {
		case otherErr := <-errCh:
			if otherErr != nil && !errors.Is(otherErr, http.ErrServerClosed) {
				retErr = errors.Join(retErr, otherErr)
			}
		default:
		}
		// Shut down the other listener (it may have started or not).
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		if peerSrv != nil {
			_ = peerSrv.Shutdown(shutdownCtx)
		}
	case <-ctx.Done():
		logger.Info("shutdown initiated",
			slog.String("node", cfg.NodeID),
			slog.String("reason", "signal received"),
		)
		// Give in-flight requests the configured grace period to complete.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("graceful shutdown failed (public)", slog.Any("error", err))
			retErr = err
		}
		if peerSrv != nil {
			if err := peerSrv.Shutdown(shutdownCtx); err != nil {
				logger.Error("graceful shutdown failed (peer)", slog.Any("error", err))
				if retErr == nil {
					retErr = err
				} else {
					retErr = errors.Join(retErr, err)
				}
			}
		}
		logger.Info("shutdown complete", slog.String("node", cfg.NodeID))
	}
	// retErr is returned implicitly. The deferred close function above may
	// modify it after this point, so a close error is propagated to the caller
	// without masking any earlier startup or shutdown error.
	return
}

// controllerEpochProvider adapts a lifecycle controller to the peertls
// EpochProvider interface. It returns the controller's currently authorized
// cluster/epoch from its Authorization() snapshot when authorized (and not
// stopped), and an error otherwise, so the peer client blocks outbound
// requests when the current epoch cannot be established safely. This keeps the
// outbound epoch context in sync with metadata changes instead of using a
// one-time startup copy.
type controllerEpochProvider struct {
	ctrl     *lifecycle.Controller
	enforced bool
}

func (p controllerEpochProvider) Context() (lifecycle.RequestContext, error) {
	snap := p.ctrl.Authorization()
	if snap.Stopped {
		return lifecycle.RequestContext{}, fmt.Errorf("lifecycle controller stopped")
	}
	if snap.Metadata == nil {
		return lifecycle.RequestContext{}, fmt.Errorf("no authoritative metadata observation")
	}
	if snap.AuthorityState != "fresh" || snap.Stale {
		return lifecycle.RequestContext{}, fmt.Errorf("authoritative metadata not fresh (authority=%s)", snap.AuthorityState)
	}
	if snap.Binding == nil || !snap.Binding.MatchesMetadata(*snap.Metadata) {
		return lifecycle.RequestContext{}, fmt.Errorf("local database binding does not match authorized epoch")
	}
	return lifecycle.RequestContext{
		ClusterID: snap.Metadata.ClusterID,
		EpochID:   snap.Metadata.EpochID,
	}, nil
}

func (p controllerEpochProvider) Enforcement() bool { return p.enforced }

func newControllerEpochProvider(ctrl *lifecycle.Controller) controllerEpochProvider {
	return controllerEpochProvider{ctrl: ctrl, enforced: true}
}

func levelFromName(name string) slog.Level {
	switch name {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// buildDiscoveryConfig converts the validated environment configuration into
// the discovery.Config struct, parsing durations and integers and splitting
// the static peer list.
func buildDiscoveryConfig(cfg *config.Config) (discovery.Config, error) {
	var dc discovery.Config
	dc.Mode = discovery.Mode(cfg.DiscoveryMode)
	dc.NodeID = cfg.NodeID
	dc.DNSName = cfg.DiscoveryDNSName
	dc.SRVService = cfg.DiscoverySRVService
	dc.StatefulSetName = cfg.StatefulSetName

	n, err := strconv.Atoi(cfg.ExpectedReplicas)
	if err != nil {
		return dc, fmt.Errorf("expected replicas %q: %w", cfg.ExpectedReplicas, err)
	}
	dc.ExpectedReplicas = n

	if cfg.DiscoveryMode == "static" {
		dc.StaticPeers = splitTrimPeers(cfg.StaticPeers)
	}

	if cfg.DiscoveryMode == "dns" {
		dc.Interval, err = time.ParseDuration(cfg.DiscoveryInterval)
		if err != nil {
			return dc, fmt.Errorf("discovery interval %q: %w", cfg.DiscoveryInterval, err)
		}
		dc.Timeout, err = time.ParseDuration(cfg.DiscoveryTimeout)
		if err != nil {
			return dc, fmt.Errorf("discovery timeout %q: %w", cfg.DiscoveryTimeout, err)
		}
	}

	// Enforce centralized validation before the config is handed to the
	// discovery manager. This catches malformed durations and port values
	// early, before they reach time.NewTicker (which panics on non-positive
	// durations) or the network layer.
	if err := dc.Validate(); err != nil {
		return dc, fmt.Errorf("discovery config validation: %w", err)
	}

	return dc, nil
}

// splitTrimPeers splits a comma-separated peer list, trimming whitespace and
// removing empty entries.
func splitTrimPeers(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// buildGossipEngine constructs and returns the gossip engine for the
// configured parameters. It does NOT start the engine (Start is called
// after discovery is initialized, so the snapshot source can be set).
// Returns nil on failure (the error is logged).
func buildGossipEngine(cfg *config.Config, logger *slog.Logger) *gossip.Engine {
	gossipCfg := gossip.Config{
		Fanout:        cfg.GossipFanout,
		Rounds:        cfg.GossipRounds,
		Workers:       cfg.GossipWorkers,
		QueueCapacity: cfg.GossipQueueCapacity,
		MaxAttempts:   cfg.GossipMaxAttempts,
		RetryBase:     cfg.GossipRetryBase,
		RetryMax:      cfg.GossipRetryMax,
	}
	if err := gossipCfg.Validate(); err != nil {
		logger.Error("gossip config validation failed", "error", err)
		return nil
	}

	// Build the peer selector with the configured identity suffix.
	selector := gossip.NewDefaultPeerSelector(cfg.PeerIdentitySuffix, nil)

	// The delivery transport is set after the peer client is available.
	// For now, create the engine with a placeholder; the real delivery
	// is set via SetDelivery after the peer client exists.
	return gossip.New(gossipCfg, selector, nil, logger)
}

// buildAntiEntropyEngine constructs and returns the anti-entropy engine for
// the configured parameters. It does NOT start the engine (Start is called
// after discovery is initialized, so the peer source can be set).
// Returns nil on failure (the error is logged).
func buildAntiEntropyEngine(cfg *config.Config, logger *slog.Logger) *antientropy.Engine {
	aCfg := antientropy.Config{
		Interval:        cfg.AntiEntropyInterval,
		PageSize:        cfg.AntiEntropyPageSize,
		FetchBatchSize:  cfg.AntiEntropyFetchBatchSize,
		MaxPagesPerPeer: cfg.AntiEntropyMaxPagesPerPeer,
		Workers:         cfg.AntiEntropyWorkers,
		RequestTimeout:  cfg.AntiEntropyRequestTimeout,
	}
	if err := aCfg.Validate(); err != nil {
		logger.Error("anti-entropy config validation failed", "error", err)
		return nil
	}
	// The peer source, inventory client, message fetcher, and local store
	// are set via setters after the peer client is available.
	return antientropy.New(aCfg, nil, nil, nil, nil, logger)
}

// buildPeerTLSConfig converts the validated environment configuration into the
// peertls.Config struct, parsing the reload interval. It is only called when
// peer TLS is enabled; the field presence and suffix syntax are already
// validated by config.validatePeerTLSConfig at load time.
func buildPeerTLSConfig(cfg *config.Config) (peertls.Config, error) {
	var pc peertls.Config
	pc.CertFile = cfg.PeerCertFile
	pc.KeyFile = cfg.PeerKeyFile
	pc.TrustBundleFile = cfg.PeerTrustBundleFile
	pc.IdentitySuffix = cfg.PeerIdentitySuffix
	// The StatefulSet name is shared with the discovery config (the same
	// StatefulSet provides both peer discovery and the pod identity prefix).
	pc.StatefulSetName = cfg.StatefulSetName
	pc.NodeID = cfg.NodeID

	n, err := strconv.Atoi(cfg.ExpectedReplicas)
	if err != nil {
		return pc, fmt.Errorf("expected replicas %q: %w", cfg.ExpectedReplicas, err)
	}
	pc.ExpectedReplicas = n

	interval, err := time.ParseDuration(cfg.PeerTLSReloadInterval)
	if err != nil {
		return pc, fmt.Errorf("peer TLS reload interval %q: %w", cfg.PeerTLSReloadInterval, err)
	}
	pc.ReloadInterval = interval

	// Enforce centralized validation before handing the config to the peer
	// TLS manager.
	if err := pc.Validate(); err != nil {
		return pc, fmt.Errorf("peer TLS config validation: %w", err)
	}
	return pc, nil
}
