// Package config loads GossipGrid configuration from environment variables.
//
// It is deliberately small and flat: each field maps to one environment
// variable with a default, and Validate() reports the first invalid value.
// Additional fields for the database, discovery, gossip, and TLS milestones
// are added here as they are implemented, without a generic config framework.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Env vars read by Load.
const (
	EnvHTTPAddr            = "GOSSIPGRID_HTTP_ADDR"
	EnvNodeID              = "GOSSIPGRID_NODE_ID"
	EnvLogLevel            = "GOSSIPGRID_LOG_LEVEL"
	EnvShutdownTimeout     = "GOSSIPGRID_SHUTDOWN_TIMEOUT"
	EnvDBPath              = "GOSSIPGRID_DB_PATH"
	EnvDiscoveryMode       = "GOSSIPGRID_DISCOVERY_MODE"
	EnvStaticPeers         = "GOSSIPGRID_STATIC_PEERS"
	EnvDiscoveryDNSName    = "GOSSIPGRID_DISCOVERY_DNS_NAME"
	EnvDiscoverySRVService = "GOSSIPGRID_DISCOVERY_SRV_SERVICE"
	EnvDiscoveryInterval   = "GOSSIPGRID_DISCOVERY_INTERVAL"
	EnvDiscoveryTimeout    = "GOSSIPGRID_DISCOVERY_TIMEOUT"
	EnvExpectedReplicas    = "GOSSIPGRID_EXPECTED_REPLICAS"
	EnvStatefulSetName     = "GOSSIPGRID_STATEFULSET_NAME"
)

// Config holds the validated, runtime configuration for a GossipGrid node.
type Config struct {
	HTTPAddr        string
	NodeID          string
	LogLevel        string
	ShutdownTimeout time.Duration
	DBPath          string

	// Discovery configuration (Milestone 3A).
	DiscoveryMode       string
	StaticPeers         string
	DiscoveryDNSName    string
	DiscoverySRVService string
	DiscoveryInterval   string
	DiscoveryTimeout    string
	ExpectedReplicas    string
	StatefulSetName     string
}

// envFunc is the lookup used to read environment values. It is a function field
// (not an interface) so tests can inject values without a framework.
type envFunc func(string) (string, bool)

// Load reads configuration from the process environment, applies defaults,
// and returns a validated Config. It fails with a descriptive error naming the
// offending variable when any value is invalid.
func Load() (*Config, error) {
	return load(os.LookupEnv)
}

func load(lookup envFunc) (*Config, error) {
	cfg := &Config{}
	var errs []error

	cfg.HTTPAddr = getEnv(lookup, EnvHTTPAddr, ":8080")
	if err := validateAddr(cfg.HTTPAddr); err != nil {
		errs = append(errs, err)
	}

	// Distinguish "unset" (ok == false) from "set to empty string" (ok == true,
	// value == ""). The hostname default applies only when the variable is
	// unset; an explicitly empty value is an error the operator should fix.
	if v, ok := lookup(EnvNodeID); ok {
		cfg.NodeID = v
	} else {
		host, err := os.Hostname()
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: defaulting to hostname: %w", EnvNodeID, err))
		} else {
			cfg.NodeID = host
		}
	}
	if err := validateNodeID(cfg.NodeID); err != nil {
		errs = append(errs, err)
	}

	cfg.LogLevel = strings.ToLower(getEnv(lookup, EnvLogLevel, "info"))
	if !validLogLevel(cfg.LogLevel) {
		errs = append(errs, fmt.Errorf("%s: %q is not a valid log level (want debug, info, warn, or error)", EnvLogLevel, cfg.LogLevel))
	}

	duration, err := time.ParseDuration(getEnv(lookup, EnvShutdownTimeout, "10s"))
	if err != nil {
		errs = append(errs, fmt.Errorf("%s: %w", EnvShutdownTimeout, err))
	} else if duration <= 0 {
		errs = append(errs, fmt.Errorf("%s: %s is not a positive duration", EnvShutdownTimeout, duration))
	} else {
		cfg.ShutdownTimeout = duration
	}

	cfg.DBPath = getEnv(lookup, EnvDBPath, "./data/gossipgrid.db")

	// Discovery configuration (Milestone 3A).
	cfg.DiscoveryMode = strings.ToLower(getEnv(lookup, EnvDiscoveryMode, "off"))
	cfg.StaticPeers = getEnv(lookup, EnvStaticPeers, "")
	cfg.DiscoveryDNSName = getEnv(lookup, EnvDiscoveryDNSName, "")
	cfg.DiscoverySRVService = getEnv(lookup, EnvDiscoverySRVService, "peer")
	cfg.DiscoveryInterval = getEnv(lookup, EnvDiscoveryInterval, "10s")
	cfg.DiscoveryTimeout = getEnv(lookup, EnvDiscoveryTimeout, "3s")
	cfg.ExpectedReplicas = getEnv(lookup, EnvExpectedReplicas, "1")
	cfg.StatefulSetName = getEnv(lookup, EnvStatefulSetName, "gossipgrid")

	if err := validateDiscoveryConfig(cfg); err != nil {
		errs = append(errs, err)
	}

	return cfg, errors.Join(errs...)
}

func getEnv(lookup envFunc, key, fallback string) string {
	if v, ok := lookup(key); ok && v != "" {
		return v
	}
	return fallback
}

// validateAddr checks host/port syntax and the numeric port range without
// performing DNS resolution or any network operation. It accepts the normal
// Go listener forms: ":8080" (all interfaces), "127.0.0.1:8080", "0.0.0.0:8080",
// "[::1]:8080" (bracketed IPv6), and named hosts like "localhost:8080".
//
// Only syntax and range are validated here; reachability is the listener's
// concern at start time, not the config loader's.
func validateAddr(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s: %q is not a valid listen address (want e.g. \":8080\"): %w", EnvHTTPAddr, addr, err)
	}
	// The port must be a non-empty string of ASCII digits; strconv.Atoi will
	// reject leading zeros, "+", "-", and any non-digit character.
	if port == "" {
		return fmt.Errorf("%s: %q is missing a port", EnvHTTPAddr, addr)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("%s: port %q is not a valid number: %w", EnvHTTPAddr, port, err)
	}
	if n < 1 || n > 65535 {
		return fmt.Errorf("%s: port %d is out of range (want 1-65535)", EnvHTTPAddr, n)
	}
	// An empty host (e.g. ":8080") means all interfaces and is valid.
	// A numeric IP (e.g. "127.0.0.1", "::1") is valid.
	// A named host (e.g. "localhost") is accepted without resolution; the
	// listener will fail at start time if it cannot be resolved.
	return nil
}

func validateNodeID(id string) error {
	if id == "" {
		return fmt.Errorf("%s: node id must not be empty", EnvNodeID)
	}
	if len(id) > 253 {
		return fmt.Errorf("%s: node id must be at most 253 bytes", EnvNodeID)
	}
	return nil
}

func validLogLevel(level string) bool {
	switch level {
	case "debug", "info", "warn", "error":
		return true
	default:
		return false
	}
}

// validatePeerAddr checks host:port syntax and the numeric port range
// (1-65535) for a static peer address without performing DNS resolution or
// any network operation. It accepts bracketed IPv6 addresses (e.g.
// "[::1]:8443") via net.SplitHostPort.
func validatePeerAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s: %q is not a valid peer address (want host:port): %w", EnvStaticPeers, addr, err)
	}
	if host == "" {
		return fmt.Errorf("%s: %q is missing a host", EnvStaticPeers, addr)
	}
	if port == "" {
		return fmt.Errorf("%s: %q is missing a port", EnvStaticPeers, addr)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("%s: port %q in %q is not a valid number: %w", EnvStaticPeers, port, addr, err)
	}
	if n < 1 || n > 65535 {
		return fmt.Errorf("%s: port %d in %q is out of range (want 1-65535)", EnvStaticPeers, n, addr)
	}
	return nil
}

// validateDiscoveryConfig checks the discovery fields for the active mode.
// It returns the first error found, or nil if all values are valid.
func validateDiscoveryConfig(cfg *Config) error {
	switch cfg.DiscoveryMode {
	case "off":
		// No mode-specific requirements.
	case "static":
		if cfg.StaticPeers == "" {
			return fmt.Errorf("%s: static discovery mode requires a non-empty peer list", EnvStaticPeers)
		}
		for _, addr := range splitPeers(cfg.StaticPeers) {
			if err := validatePeerAddr(addr); err != nil {
				return err
			}
		}
	case "dns":
		if cfg.DiscoveryDNSName == "" {
			return fmt.Errorf("%s: dns discovery mode requires a non-empty DNS service name", EnvDiscoveryDNSName)
		}
		if cfg.DiscoverySRVService == "" {
			return fmt.Errorf("%s: SRV service name must not be empty", EnvDiscoverySRVService)
		}
		interval, err := time.ParseDuration(cfg.DiscoveryInterval)
		if err != nil {
			return fmt.Errorf("%s: %q is not a valid duration: %w", EnvDiscoveryInterval, cfg.DiscoveryInterval, err)
		}
		if interval <= 0 {
			return fmt.Errorf("%s: %q must be a positive duration", EnvDiscoveryInterval, cfg.DiscoveryInterval)
		}
		timeout, err := time.ParseDuration(cfg.DiscoveryTimeout)
		if err != nil {
			return fmt.Errorf("%s: %q is not a valid duration: %w", EnvDiscoveryTimeout, cfg.DiscoveryTimeout, err)
		}
		if timeout <= 0 {
			return fmt.Errorf("%s: %q must be a positive duration", EnvDiscoveryTimeout, cfg.DiscoveryTimeout)
		}
	default:
		return fmt.Errorf("%s: %q is not a valid discovery mode (want off, static, or dns)", EnvDiscoveryMode, cfg.DiscoveryMode)
	}

	// Expected replicas must be a positive integer.
	n, err := strconv.Atoi(cfg.ExpectedReplicas)
	if err != nil {
		return fmt.Errorf("%s: %q is not a valid integer: %w", EnvExpectedReplicas, cfg.ExpectedReplicas, err)
	}
	if n < 1 {
		return fmt.Errorf("%s: must be at least 1 (got %d)", EnvExpectedReplicas, n)
	}

	// StatefulSet name is required when replicas > 1.
	if n > 1 && cfg.StatefulSetName == "" {
		return fmt.Errorf("%s: stateful set name is required when expected replicas exceeds 1", EnvStatefulSetName)
	}

	return nil
}

// splitPeers splits a comma-separated peer list, trimming whitespace.
func splitPeers(s string) []string {
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
