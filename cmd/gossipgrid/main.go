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
	"syscall"

	"gossipgrid/internal/config"
	"gossipgrid/internal/server"
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

func run() error {
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

	logger.Info("starting gossipgrid",
		slog.String("version", Version),
		slog.String("node", cfg.NodeID),
		slog.String("http_addr", cfg.HTTPAddr),
		slog.String("log_level", cfg.LogLevel),
		slog.Bool("fips_enabled", fipsEnabled),
		slog.String("fips_module", fipsModule),
	)

	srv := server.NewServer(cfg.HTTPAddr, server.New(server.Node{ID: cfg.NodeID}))

	// Start listening in a goroutine so we can watch for OS signals on the
	// main goroutine. ListenAndServe returns nil when Shutdown has been called.
	errCh := make(chan error, 1)
	go func() {
		logger.Info("http listener starting", slog.String("addr", cfg.HTTPAddr))
		errCh <- srv.ListenAndServe()
	}()

	// Interrupt and terminate signals from the OS or a Kubernetes liveness
	// probe. signal.NotifyContext cancels the parent context on first signal.
	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		// The server failed to start (e.g. address in use) before a signal
		// was received.
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
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
			logger.Error("graceful shutdown failed", slog.Any("error", err))
			return err
		}
		logger.Info("shutdown complete", slog.String("node", cfg.NodeID))
	}
	return nil
}

func levelFromName(name string) slog.Level {
	switch name {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
