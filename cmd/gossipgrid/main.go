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

	"gossipgrid/internal/config"
	"gossipgrid/internal/discovery"
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

	srv := server.NewServer(cfg.HTTPAddr, server.NewWithStore(server.Node{ID: cfg.NodeID}, st))

	// Initialize the private peer mTLS transport (Milestone 3B). When peer TLS
	// is enabled, load credentials and create the peer server. A credential
	// load failure must fail startup clearly — no silent fallback to
	// unauthenticated communication, and the public server must not be left
	// running indefinitely.
	var peerMgr *peertls.Manager
	var peerSrv *peertls.PeerServer
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
		peerSrv = peertls.NewPeerServer(peerMgr, cfg.PeerAddr, cfg.NodeID, Version)
	}

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
