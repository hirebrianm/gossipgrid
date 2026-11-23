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

	// Peer mTLS configuration (Milestone 3B).
	EnvPeerTLSEnabled        = "GOSSIPGRID_PEER_TLS_ENABLED"
	EnvPeerAddr              = "GOSSIPGRID_PEER_ADDR"
	EnvPeerCertFile          = "GOSSIPGRID_PEER_CERT_FILE"
	EnvPeerKeyFile           = "GOSSIPGRID_PEER_KEY_FILE"
	EnvPeerTrustBundleFile   = "GOSSIPGRID_PEER_TRUST_BUNDLE_FILE"
	EnvPeerIdentitySuffix    = "GOSSIPGRID_PEER_IDENTITY_SUFFIX"
	EnvPeerTLSReloadInterval = "GOSSIPGRID_PEER_TLS_RELOAD_INTERVAL"

	// Authenticated peer message exchange (Milestone 4A).
	// When enabled, the private peer listener accepts POST /internal/messages
	// from authenticated replicas. It requires peer mTLS to be enabled; it
	// does NOT enable automatic gossip, fanout, anti-entropy, or readiness
	// gating (those belong to Milestones 4B/4C/5).
	EnvPeerMessageExchangeEnabled = "GOSSIPGRID_PEER_MESSAGE_EXCHANGE_ENABLED"

	// Epidemic gossip (Milestone 4B). When enabled, local and replicated
	// message commits trigger asynchronous fanout to authenticated peers.
	// Requires peer mTLS, peer message exchange, and static or DNS discovery.
	EnvGossipEnabled       = "GOSSIPGRID_GOSSIP_ENABLED"
	EnvGossipFanout        = "GOSSIPGRID_GOSSIP_FANOUT"
	EnvGossipRounds        = "GOSSIPGRID_GOSSIP_ROUNDS"
	EnvGossipWorkers       = "GOSSIPGRID_GOSSIP_WORKERS"
	EnvGossipQueueCapacity = "GOSSIPGRID_GOSSIP_QUEUE_CAPACITY"
	EnvGossipMaxAttempts   = "GOSSIPGRID_GOSSIP_MAX_ATTEMPTS"
	EnvGossipRetryBase     = "GOSSIPGRID_GOSSIP_RETRY_BASE"
	EnvGossipRetryMax      = "GOSSIPGRID_GOSSIP_RETRY_MAX"

	// Anti-entropy (Milestone 4C). When enabled, replicas periodically
	// compare bounded message ID inventories with authenticated peers and
	// fetch missing records. Requires peer mTLS, peer message exchange,
	// and static or DNS discovery. Gossip need not be enabled.
	EnvAntiEntropyEnabled         = "GOSSIPGRID_ANTI_ENTROPY_ENABLED"
	EnvAntiEntropyInterval        = "GOSSIPGRID_ANTI_ENTROPY_INTERVAL"
	EnvAntiEntropyPageSize        = "GOSSIPGRID_ANTI_ENTROPY_PAGE_SIZE"
	EnvAntiEntropyFetchBatchSize  = "GOSSIPGRID_ANTI_ENTROPY_FETCH_BATCH_SIZE"
	EnvAntiEntropyMaxPagesPerPeer = "GOSSIPGRID_ANTI_ENTROPY_MAX_PAGES_PER_PEER"
	EnvAntiEntropyWorkers         = "GOSSIPGRID_ANTI_ENTROPY_WORKERS"
	EnvAntiEntropyRequestTimeout  = "GOSSIPGRID_ANTI_ENTROPY_REQUEST_TIMEOUT"

	// Lifecycle (Milestone 5A). When enabled, the replica lifecycle state
	// machine is active, public writes are gated on readiness, and
	// replication requires epoch fencing. When disabled (the default),
	// all existing Milestone 4 behavior is preserved.
	EnvLifecycleEnabled = "GOSSIPGRID_LIFECYCLE_ENABLED"

	// Browser authentication and credential management (Milestone 6). When
	// AuthCredentialFile is set, the public API and the browser UI are
	// protected by single-administrator password authentication using a
	// mounted versioned credential file. When unset (the default), the
	// existing behavior is preserved (no browser authentication; the message
	// API is unauthenticated). The credential is never delivered through
	// environment variables; the file is the sole source of credential
	// material.
	// #nosec G101: this is an environment-variable name, not a credential.
	EnvAuthEnabled         = "GOSSIPGRID_AUTH_ENABLED"
	EnvAuthInsecureDevMode = "GOSSIPGRID_AUTH_INSECURE_DEV_MODE"
	// #nosec G101: this is an environment-variable name (the path to the
	// credential file), not a credential value.
	EnvAuthCredentialFile = "GOSSIPGRID_AUTH_CREDENTIAL_FILE"
	EnvAuthReloadInterval = "GOSSIPGRID_AUTH_RELOAD_INTERVAL"
	EnvAuthMaxStale       = "GOSSIPGRID_AUTH_MAX_STALE"
	EnvAuthSessionTTL     = "GOSSIPGRID_AUTH_SESSION_TTL"
	EnvAuthSessionIdleTTL = "GOSSIPGRID_AUTH_SESSION_IDLE_TTL"
	EnvAuthCookieSecure   = "GOSSIPGRID_AUTH_COOKIE_SECURE"

	// Message retention. When a non-zero TTL is configured,
	// messages older than the TTL are expired: they are excluded from API
	// listings, not propagated through gossip, not synced via anti-entropy,
	// and physically removed from SQLite by a periodic cleaner. The feature
	// is DISABLED by default (TTL = 0): when unset, all existing message
	// behavior is preserved and no cleaner is started. It is independent of
	// peer mTLS, gossip, and anti-entropy (it can be enabled with any of them
	// off).
	EnvMessageRetentionTTL      = "GOSSIPGRID_MESSAGE_RETENTION_TTL"
	EnvMessageRetentionInterval = "GOSSIPGRID_MESSAGE_RETENTION_INTERVAL"
	// GOSSIPGRID_MESSAGE_RETENTION_MAX_SWEEP_ROWS caps the total rows a single
	// scheduled retention sweep may delete (0/unset = default 5000; the
	// backlog resumes on the next sweep).
	EnvMessageRetentionMaxSweepRows = "GOSSIPGRID_MESSAGE_RETENTION_MAX_SWEEP_ROWS"
	// GOSSIPGRID_MESSAGE_RETENTION_MAX_SWEEP_DURATION caps the wall-clock time
	// a single scheduled retention sweep's delete phase may take (0/unset =
	// default 10s; the backlog resumes on the next sweep).
	EnvMessageRetentionMaxSweepDuration = "GOSSIPGRID_MESSAGE_RETENTION_MAX_SWEEP_DURATION"
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

	// Peer mTLS configuration (Milestone 3B). The private peer listener is
	// only started when PeerTLSEnabled is true; otherwise local single-instance
	// development and observational discovery are unchanged.
	PeerTLSEnabled        bool
	PeerAddr              string
	PeerCertFile          string
	PeerKeyFile           string
	PeerTrustBundleFile   string
	PeerIdentitySuffix    string
	PeerTLSReloadInterval string

	// Authenticated peer message exchange (Milestone 4A). When true, the
	// private peer listener registers POST /internal/messages and accepts
	// idempotent replicated message inserts from authenticated replicas.
	// Requires PeerTLSEnabled to be true (validated).
	PeerMessageExchangeEnabled bool

	// Epidemic gossip (Milestone 4B). When GossipEnabled is true, local and
	// replicated message commits trigger asynchronous fanout to authenticated
	// peers using bounded queues, workers, and exponential-backoff retries.
	// Requires PeerTLSEnabled, PeerMessageExchangeEnabled, and discovery mode
	// static or dns (all validated).
	GossipEnabled       bool
	GossipFanout        int
	GossipRounds        int
	GossipWorkers       int
	GossipQueueCapacity int
	GossipMaxAttempts   int
	GossipRetryBase     time.Duration
	GossipRetryMax      time.Duration

	// Anti-entropy (Milestone 4C). When AntiEntropyEnabled is true, replicas
	// periodically compare bounded message ID inventories with authenticated
	// peers and fetch missing records. Requires PeerTLSEnabled,
	// PeerMessageExchangeEnabled, and discovery mode static or dns.
	// Gossip need not be enabled.
	AntiEntropyEnabled         bool
	AntiEntropyInterval        time.Duration
	AntiEntropyPageSize        int
	AntiEntropyFetchBatchSize  int
	AntiEntropyMaxPagesPerPeer int
	AntiEntropyWorkers         int
	AntiEntropyRequestTimeout  time.Duration

	// Lifecycle (Milestone 5A). When LifecycleEnabled is true, the replica
	// lifecycle state machine is active. Requires PeerTLSEnabled (validates
	// peer identity configuration). When false (default), all existing
	// behavior is preserved and no epoch protection is claimed.
	LifecycleEnabled bool

	// Browser authentication (Milestone 6). AuthEnabled is true by default:
	// the public message API and the web UI are protected by
	// single-administrator password authentication. A missing or invalid
	// credential file does NOT silently enable anonymous access — the
	// process starts, /healthz and /readyz remain 200, and authenticated
	// requests return 503 (fail-closed) until a valid credential is loaded.
	// AuthInsecureDevMode is an opt-in, clearly-named escape hatch for local
	// development that disables authentication entirely. It defaults to
	// false, is documented as insecure, and is unsuitable for
	// production/Kubernetes deployment. AuthCredentialFile is the path to the
	// mounted versioned credential file. AuthCookieSecure controls the Secure
	// cookie flag; it defaults to true in production (HTTPS) and is set to
	// false for local development over plain HTTP.
	AuthEnabled         bool
	AuthInsecureDevMode bool
	AuthCredentialFile  string
	AuthReloadInterval  string
	AuthMaxStale        string
	AuthSessionTTL      string
	AuthSessionIdleTTL  string
	AuthCookieSecure    bool

	// Message retention. MessageRetentionTTL == 0 disables
	// retention (the default): no expiration, no cleaner. When non-zero,
	// messages older than the TTL are expired. MessageRetentionInterval is
	// the period between cleaner sweeps (it only matters when retention is
	// enabled; it is still validated when retention is on).
	//
	// MessageRetentionMaxSweepRows caps the total rows a single scheduled
	// sweep may delete (0 = default 5000; the backlog resumes on the next
	// sweep). MessageRetentionMaxSweepDuration caps the delete-phase
	// wall-clock time of a single sweep (0 = default 10s). Both bound a
	// single sweep in work and time so a large backlog cannot keep one sweep
	// running indefinitely.
	MessageRetentionTTL              time.Duration
	MessageRetentionInterval         time.Duration
	MessageRetentionMaxSweepRows     int64
	MessageRetentionMaxSweepDuration time.Duration
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

	// Peer mTLS configuration (Milestone 3B).
	cfg.PeerAddr = getEnv(lookup, EnvPeerAddr, ":8443")
	if err := validateAddrWithVar(EnvPeerAddr, cfg.PeerAddr); err != nil {
		errs = append(errs, err)
	}
	cfg.PeerCertFile = getEnv(lookup, EnvPeerCertFile, "")
	cfg.PeerKeyFile = getEnv(lookup, EnvPeerKeyFile, "")
	cfg.PeerTrustBundleFile = getEnv(lookup, EnvPeerTrustBundleFile, "")
	cfg.PeerIdentitySuffix = strings.ToLower(strings.TrimSpace(getEnv(lookup, EnvPeerIdentitySuffix, "")))
	cfg.PeerTLSReloadInterval = getEnv(lookup, EnvPeerTLSReloadInterval, "2s")
	// When the enable flag is set, parse it strictly; an unrecognised value is
	// an error (a typo must not silently disable a security feature).
	if v, ok := lookup(EnvPeerTLSEnabled); ok {
		b, perr := parseBoolEnv(v, EnvPeerTLSEnabled)
		if perr != nil {
			errs = append(errs, perr)
		}
		cfg.PeerTLSEnabled = b
	}
	if err := validatePeerTLSConfig(cfg); err != nil {
		errs = append(errs, err)
	}

	// Authenticated peer message exchange (Milestone 4A). Default false:
	// when unset, the private peer listener does NOT register
	// POST /internal/messages (no automatic replication before Milestone 5).
	if v, ok := lookup(EnvPeerMessageExchangeEnabled); ok {
		b, perr := parseBoolEnv(v, EnvPeerMessageExchangeEnabled)
		if perr != nil {
			errs = append(errs, perr)
		}
		cfg.PeerMessageExchangeEnabled = b
	}
	if err := validatePeerMessageExchangeConfig(cfg); err != nil {
		errs = append(errs, err)
	}

	// Epidemic gossip (Milestone 4B). Default disabled: when unset, no
	// automatic peer message fanout occurs.
	if v, ok := lookup(EnvGossipEnabled); ok {
		b, perr := parseBoolEnv(v, EnvGossipEnabled)
		if perr != nil {
			errs = append(errs, perr)
		}
		cfg.GossipEnabled = b
	}
	// When gossip is enabled, parse and validate the gossip parameters.
	// When disabled, the fields retain their defaults and malformed values
	// are ignored (gossip is a best-effort, opt-in feature).
	if cfg.GossipEnabled {
		var gerr error
		cfg.GossipFanout, gerr = parseIntEnvGossip(lookup, EnvGossipFanout, 2)
		if gerr != nil {
			errs = append(errs, gerr)
		}
		cfg.GossipRounds, gerr = parseIntEnvGossip(lookup, EnvGossipRounds, 3)
		if gerr != nil {
			errs = append(errs, gerr)
		}
		cfg.GossipWorkers, gerr = parseIntEnvGossip(lookup, EnvGossipWorkers, 4)
		if gerr != nil {
			errs = append(errs, gerr)
		}
		cfg.GossipQueueCapacity, gerr = parseIntEnvGossip(lookup, EnvGossipQueueCapacity, 256)
		if gerr != nil {
			errs = append(errs, gerr)
		}
		cfg.GossipMaxAttempts, gerr = parseIntEnvGossip(lookup, EnvGossipMaxAttempts, 3)
		if gerr != nil {
			errs = append(errs, gerr)
		}
		cfg.GossipRetryBase, gerr = parseDurationEnvGossip(lookup, EnvGossipRetryBase, 250*time.Millisecond)
		if gerr != nil {
			errs = append(errs, gerr)
		}
		cfg.GossipRetryMax, gerr = parseDurationEnvGossip(lookup, EnvGossipRetryMax, 5*time.Second)
		if gerr != nil {
			errs = append(errs, gerr)
		}
		if err := validateGossipConfig(cfg); err != nil {
			errs = append(errs, err)
		}
	}

	// Anti-entropy (Milestone 4C). Default disabled: when unset, no periodic
	// inventory comparison or fetch occurs.
	if v, ok := lookup(EnvAntiEntropyEnabled); ok {
		b, perr := parseBoolEnv(v, EnvAntiEntropyEnabled)
		if perr != nil {
			errs = append(errs, perr)
		}
		cfg.AntiEntropyEnabled = b
	}
	if cfg.AntiEntropyEnabled {
		var aerr error
		cfg.AntiEntropyInterval, aerr = parseDurationEnv(lookup, EnvAntiEntropyInterval, 30*time.Second)
		if aerr != nil {
			errs = append(errs, aerr)
		}
		cfg.AntiEntropyPageSize, aerr = parseIntEnv(lookup, EnvAntiEntropyPageSize, 128)
		if aerr != nil {
			errs = append(errs, aerr)
		}
		cfg.AntiEntropyFetchBatchSize, aerr = parseIntEnv(lookup, EnvAntiEntropyFetchBatchSize, 8)
		if aerr != nil {
			errs = append(errs, aerr)
		}
		cfg.AntiEntropyMaxPagesPerPeer, aerr = parseIntEnv(lookup, EnvAntiEntropyMaxPagesPerPeer, 4)
		if aerr != nil {
			errs = append(errs, aerr)
		}
		cfg.AntiEntropyWorkers, aerr = parseIntEnv(lookup, EnvAntiEntropyWorkers, 2)
		if aerr != nil {
			errs = append(errs, aerr)
		}
		cfg.AntiEntropyRequestTimeout, aerr = parseDurationEnv(lookup, EnvAntiEntropyRequestTimeout, 5*time.Second)
		if aerr != nil {
			errs = append(errs, aerr)
		}
		if err := validateAntiEntropyConfig(cfg); err != nil {
			errs = append(errs, err)
		}
	}

	// Lifecycle (Milestone 5A). Default disabled: when unset, the
	// lifecycle state machine is not active and all existing Milestone 4
	// behavior is preserved.
	if v, ok := lookup(EnvLifecycleEnabled); ok {
		b, perr := parseBoolEnv(v, EnvLifecycleEnabled)
		if perr != nil {
			errs = append(errs, perr)
		}
		cfg.LifecycleEnabled = b
	}
	if err := validateLifecycleConfig(cfg); err != nil {
		errs = append(errs, err)
	}

	// Browser authentication (Milestone 6). Authentication is enabled by
	// default (fail-closed): the message API and web UI require a valid
	// credential. AuthEnabled defaults to true; an explicit false disables
	// authentication for the (documented) unauthenticated mode. An
	// unrecognised value is an error (a typo must not silently change a
	// security feature).
	if v, ok := lookup(EnvAuthEnabled); ok {
		b, perr := parseBoolEnv(v, EnvAuthEnabled)
		if perr != nil {
			errs = append(errs, perr)
		}
		cfg.AuthEnabled = b
	} else {
		cfg.AuthEnabled = true
	}
	// AuthInsecureDevMode is an opt-in escape hatch for local development.
	// It defaults to false; when true, authentication is disabled entirely
	// (regardless of AuthEnabled). It is documented as insecure and
	// unsuitable for production/Kubernetes. An unrecognised value is an
	// error.
	if v, ok := lookup(EnvAuthInsecureDevMode); ok {
		b, perr := parseBoolEnv(v, EnvAuthInsecureDevMode)
		if perr != nil {
			errs = append(errs, perr)
		}
		cfg.AuthInsecureDevMode = b
	} else {
		cfg.AuthInsecureDevMode = false
	}
	// AuthCredentialFile is a path; when empty, the reloader stays
	// "unavailable" and authenticated requests return 503 (fail-closed) —
	// never anonymous access. The other fields have defaults and are
	// validated when authentication is on (not in insecure dev mode).
	cfg.AuthCredentialFile = getEnv(lookup, EnvAuthCredentialFile, "")
	cfg.AuthReloadInterval = getEnv(lookup, EnvAuthReloadInterval, "2s")
	cfg.AuthMaxStale = getEnv(lookup, EnvAuthMaxStale, "5m")
	cfg.AuthSessionTTL = getEnv(lookup, EnvAuthSessionTTL, "2h")
	cfg.AuthSessionIdleTTL = getEnv(lookup, EnvAuthSessionIdleTTL, "30m")
	// The Secure cookie flag defaults to true (production HTTPS). It is set
	// explicitly only when the operator overrides it; an unrecognised value is
	// an error (a typo must not silently weaken the cookie).
	if v, ok := lookup(EnvAuthCookieSecure); ok {
		b, perr := parseBoolEnv(v, EnvAuthCookieSecure)
		if perr != nil {
			errs = append(errs, perr)
		}
		cfg.AuthCookieSecure = b
	} else {
		cfg.AuthCookieSecure = true
	}
	if err := validateAuthConfig(cfg); err != nil {
		errs = append(errs, err)
	}

	// Message retention. Disabled by default: when the TTL is
	// unset, retention is off and the cleaner is not started. An explicit "0"
	// also disables retention (the documented "off" value). A non-zero TTL
	// must be a valid positive duration (a typo is an error, not a silent
	// default). The cleanup interval defaults to 1h and is only meaningful
	// (and validated) when retention is enabled.
	if v, ok := lookup(EnvMessageRetentionTTL); ok && v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %q is not a valid duration: %w", EnvMessageRetentionTTL, v, err))
		} else if d < 0 {
			errs = append(errs, fmt.Errorf("%s: %v must not be negative (use 0 to disable)", EnvMessageRetentionTTL, d))
		} else {
			cfg.MessageRetentionTTL = d
			if d > 0 {
				cfg.MessageRetentionInterval, err = parseDurationEnv(lookup, EnvMessageRetentionInterval, time.Hour)
				if err != nil {
					errs = append(errs, err)
				}
				// Sweep budgets (optional; 0/unset = documented defaults).
				cfg.MessageRetentionMaxSweepRows, err = parseInt64Env(lookup, EnvMessageRetentionMaxSweepRows, 0)
				if err != nil {
					errs = append(errs, err)
				}
				cfg.MessageRetentionMaxSweepDuration, err = parseDurationEnv(lookup, EnvMessageRetentionMaxSweepDuration, 0)
				if err != nil {
					errs = append(errs, err)
				}
				if err := validateMessageRetentionConfig(cfg); err != nil {
					errs = append(errs, err)
				}
			}
		}
	}

	return cfg, errors.Join(errs...)
}

func getEnv(lookup envFunc, key, fallback string) string {
	if v, ok := lookup(key); ok && v != "" {
		return v
	}
	return fallback
}

// parseIntEnvGossip reads a gossip integer environment variable. Unset values
// use the documented fallback; an explicitly supplied malformed value is an
// error (a typo must not silently change a resource limit).
func parseIntEnvGossip(lookup envFunc, key string, fallback int) (int, error) {
	if v, ok := lookup(key); ok && v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("%s: %q is not a valid integer: %w", key, v, err)
		}
		return n, nil
	}
	return fallback, nil
}

// parseIntEnv reads an integer environment variable. Unset values use the
// documented fallback; an explicitly supplied malformed value is an error.
func parseIntEnv(lookup envFunc, key string, fallback int) (int, error) {
	if v, ok := lookup(key); ok && v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, fmt.Errorf("%s: %q is not a valid integer: %w", key, v, err)
		}
		return n, nil
	}
	return fallback, nil
}

// parseInt64Env reads a 64-bit integer environment variable. Unset values use
// the documented fallback; an explicitly supplied malformed value is an error.
func parseInt64Env(lookup envFunc, key string, fallback int64) (int64, error) {
	if v, ok := lookup(key); ok && v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%s: %q is not a valid integer: %w", key, v, err)
		}
		return n, nil
	}
	return fallback, nil
}

// parseDurationEnv reads a duration environment variable. Unset values use the
// documented fallback; an explicitly supplied malformed value is an error.
func parseDurationEnv(lookup envFunc, key string, fallback time.Duration) (time.Duration, error) {
	if v, ok := lookup(key); ok && v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("%s: %q is not a valid duration: %w", key, v, err)
		}
		return d, nil
	}
	return fallback, nil
}

// parseDurationEnvGossip reads a gossip duration environment variable. Unset
// values use the documented fallback; an explicitly supplied malformed value is
// an error (a typo must not silently change a backoff bound).
func parseDurationEnvGossip(lookup envFunc, key string, fallback time.Duration) (time.Duration, error) {
	if v, ok := lookup(key); ok && v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("%s: %q is not a valid duration: %w", key, v, err)
		}
		return d, nil
	}
	return fallback, nil
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

// parseBoolEnv parses a boolean environment variable. It accepts the common
// true/false spellings case-insensitively and returns an error for anything
// else so the operator notices a typo instead of silently disabling a
// security feature. The env name is supplied for the error message.
func parseBoolEnv(v, envName string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "1", "yes", "on":
		return true, nil
	case "false", "0", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("%s: %q is not a valid boolean (want true or false)", envName, v)
	}
}

// validateAddrWithVar is validateAddr with a configurable env var name in the
// error message (validateAddr hard-codes EnvHTTPAddr).
func validateAddrWithVar(env, addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s: %q is not a valid listen address (want e.g. \":8443\"): %w", env, addr, err)
	}
	if port == "" {
		return fmt.Errorf("%s: %q is missing a port", env, addr)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("%s: port %q is not a valid number: %w", env, addr, err)
	}
	if n < 1 || n > 65535 {
		return fmt.Errorf("%s: port %d in %q is out of range (want 1-65535)", env, n, addr)
	}
	return nil
}

// validatePeerTLSConfig checks the peer mTLS fields. When peer TLS is disabled
// the peer fields are ignored entirely (local single-instance development and
// observational discovery are preserved). When enabled, the required file
// paths, identity suffix, and reload interval must all be present and valid,
// and the reload interval must be positive.
func validatePeerTLSConfig(cfg *Config) error {
	if !cfg.PeerTLSEnabled {
		return nil
	}
	if cfg.PeerCertFile == "" {
		return fmt.Errorf("%s: peer TLS is enabled but the certificate file path is empty", EnvPeerCertFile)
	}
	if cfg.PeerKeyFile == "" {
		return fmt.Errorf("%s: peer TLS is enabled but the private key file path is empty", EnvPeerKeyFile)
	}
	if cfg.PeerTrustBundleFile == "" {
		return fmt.Errorf("%s: peer TLS is enabled but the trust bundle file path is empty", EnvPeerTrustBundleFile)
	}
	if cfg.PeerIdentitySuffix == "" {
		return fmt.Errorf("%s: peer TLS is enabled but the peer identity suffix is empty", EnvPeerIdentitySuffix)
	}
	// The identity suffix must be a non-empty DNS name with at least two
	// labels (e.g. "gossipgrid-headless.default.svc" or
	// "gossipgrid-headless.default.svc.cluster.local"). This prevents a
	// malformed suffix from silently authorising every host.
	if !isValidDNSSuffix(cfg.PeerIdentitySuffix) {
		return fmt.Errorf("%s: %q is not a valid DNS suffix (want e.g. \"gossipgrid-headless.default.svc.cluster.local\")", EnvPeerIdentitySuffix, cfg.PeerIdentitySuffix)
	}
	reloadInterval, err := time.ParseDuration(cfg.PeerTLSReloadInterval)
	if err != nil {
		return fmt.Errorf("%s: %q is not a valid duration: %w", EnvPeerTLSReloadInterval, cfg.PeerTLSReloadInterval, err)
	}
	if reloadInterval <= 0 {
		return fmt.Errorf("%s: %q must be a positive duration", EnvPeerTLSReloadInterval, cfg.PeerTLSReloadInterval)
	}
	return nil
}

// validatePeerMessageExchangeConfig checks the authenticated peer message
// exchange flag (Milestone 4A). Message exchange is a provisional, opt-in
// direct-exchange capability: it requires peer mTLS to be enabled (the private
// peer listener is what enforces the mTLS identity that authorises each
// inbound message). It does NOT enable automatic replication or readiness
// gating; those belong to later milestones.
func validatePeerMessageExchangeConfig(cfg *Config) error {
	if !cfg.PeerMessageExchangeEnabled {
		return nil
	}
	if !cfg.PeerTLSEnabled {
		return fmt.Errorf("%s: peer message exchange requires peer mTLS to be enabled (%s=true)", EnvPeerMessageExchangeEnabled, EnvPeerTLSEnabled)
	}
	return nil
}

// validateGossipConfig checks the epidemic gossip fields (Milestone 4B).
// Gossip is a best-effort, opt-in feature: it requires peer mTLS, peer
// message exchange, and a discovery mode that can produce peer addresses
// (static or dns). When disabled, all other gossip fields are ignored.
// When enabled, all resource limits must be within documented bounds.
func validateGossipConfig(cfg *Config) error {
	if !cfg.GossipEnabled {
		return nil
	}
	// Gossip requires peer mTLS (the transport for authenticated delivery).
	if !cfg.PeerTLSEnabled {
		return fmt.Errorf("%s: gossip requires peer mTLS to be enabled (%s=true)", EnvGossipEnabled, EnvPeerTLSEnabled)
	}
	// Gossip requires peer message exchange (the endpoint that receives
	// forwarded messages over mTLS).
	if !cfg.PeerMessageExchangeEnabled {
		return fmt.Errorf("%s: gossip requires peer message exchange to be enabled (%s=true)", EnvGossipEnabled, EnvPeerMessageExchangeEnabled)
	}
	// Gossip requires a discovery mode that produces peer addresses.
	if cfg.DiscoveryMode != "static" && cfg.DiscoveryMode != "dns" {
		return fmt.Errorf("%s: gossip requires discovery mode static or dns (got %q)", EnvGossipEnabled, cfg.DiscoveryMode)
	}
	// Resource bounds (documented in docs/gossip-and-bootstrap.md).
	if cfg.GossipFanout < 1 || cfg.GossipFanout > 32 {
		return fmt.Errorf("%s: gossip fanout must be between 1 and 32 (got %d)", EnvGossipFanout, cfg.GossipFanout)
	}
	if cfg.GossipRounds < 1 || cfg.GossipRounds > 32 {
		return fmt.Errorf("%s: gossip rounds must be between 1 and 32 (got %d)", EnvGossipRounds, cfg.GossipRounds)
	}
	if cfg.GossipWorkers < 1 || cfg.GossipWorkers > 64 {
		return fmt.Errorf("%s: gossip workers must be between 1 and 64 (got %d)", EnvGossipWorkers, cfg.GossipWorkers)
	}
	if cfg.GossipQueueCapacity < 1 || cfg.GossipQueueCapacity > 1024 {
		return fmt.Errorf("%s: gossip queue capacity must be between 1 and 1024 (got %d)", EnvGossipQueueCapacity, cfg.GossipQueueCapacity)
	}
	if cfg.GossipMaxAttempts < 1 || cfg.GossipMaxAttempts > 10 {
		return fmt.Errorf("%s: gossip max attempts must be between 1 and 10 (got %d)", EnvGossipMaxAttempts, cfg.GossipMaxAttempts)
	}
	if cfg.GossipRetryBase <= 0 {
		return fmt.Errorf("%s: gossip retry base must be a positive duration (got %v)", EnvGossipRetryBase, cfg.GossipRetryBase)
	}
	if cfg.GossipRetryMax < cfg.GossipRetryBase {
		return fmt.Errorf("%s: gossip retry max (%v) must be >= retry base (%v)", EnvGossipRetryMax, cfg.GossipRetryMax, cfg.GossipRetryBase)
	}
	return nil
}

// validateAntiEntropyConfig checks the anti-entropy fields (Milestone 4C).
// Anti-entropy is an opt-in feature: it requires peer mTLS, peer message
// exchange, and a discovery mode that can produce peer addresses (static or
// dns). Gossip need not be enabled. When enabled, all resource limits must
// be within documented bounds.
func validateAntiEntropyConfig(cfg *Config) error {
	if !cfg.AntiEntropyEnabled {
		return nil
	}
	// Anti-entropy requires peer mTLS (the transport for authenticated sync).
	if !cfg.PeerTLSEnabled {
		return fmt.Errorf("%s: anti-entropy requires peer mTLS to be enabled (%s=true)", EnvAntiEntropyEnabled, EnvPeerTLSEnabled)
	}
	// Anti-entropy requires peer message exchange (the endpoint that provides
	// the inventory and fetch operations).
	if !cfg.PeerMessageExchangeEnabled {
		return fmt.Errorf("%s: anti-entropy requires peer message exchange to be enabled (%s=true)", EnvAntiEntropyEnabled, EnvPeerMessageExchangeEnabled)
	}
	// Anti-entropy requires a discovery mode that produces peer addresses.
	if cfg.DiscoveryMode != "static" && cfg.DiscoveryMode != "dns" {
		return fmt.Errorf("%s: anti-entropy requires discovery mode static or dns (got %q)", EnvAntiEntropyEnabled, cfg.DiscoveryMode)
	}
	// Resource bounds (documented in docs/gossip-and-bootstrap.md).
	if cfg.AntiEntropyInterval <= 0 {
		return fmt.Errorf("%s: anti-entropy interval must be a positive duration (got %v)", EnvAntiEntropyInterval, cfg.AntiEntropyInterval)
	}
	if cfg.AntiEntropyInterval < 1*time.Second {
		return fmt.Errorf("%s: anti-entropy interval must be at least 1s (got %v)", EnvAntiEntropyInterval, cfg.AntiEntropyInterval)
	}
	if cfg.AntiEntropyInterval > 10*time.Minute {
		return fmt.Errorf("%s: anti-entropy interval must be at most 10m (got %v)", EnvAntiEntropyInterval, cfg.AntiEntropyInterval)
	}
	if cfg.AntiEntropyPageSize < 1 || cfg.AntiEntropyPageSize > 1024 {
		return fmt.Errorf("%s: anti-entropy page size must be between 1 and 1024 (got %d)", EnvAntiEntropyPageSize, cfg.AntiEntropyPageSize)
	}
	// Fetch batch size is bounded to 8: a batch of 8 records at the
	// per-record wire-size bound (32768 = the absolute worst-case
	// JSON-encoded body for a 4096-byte stored body, plus the origin/ID/
	// timestamp overhead) produces ~256 KiB of JSON, within the client's
	// 256 KiB response bound. The peer server enforces the same 8-record
	// bound server-side. Larger batches risk exceeding the response-size
	// limit and are rejected.
	if cfg.AntiEntropyFetchBatchSize < 1 || cfg.AntiEntropyFetchBatchSize > 8 {
		return fmt.Errorf("%s: anti-entropy fetch batch size must be between 1 and 8 (got %d); larger batches may exceed the 256 KiB response bound", EnvAntiEntropyFetchBatchSize, cfg.AntiEntropyFetchBatchSize)
	}
	// Fetch batch must not exceed page size (a batch is a subset of a page).
	if cfg.AntiEntropyFetchBatchSize > cfg.AntiEntropyPageSize {
		return fmt.Errorf("%s: fetch batch size (%d) must not exceed page size (%d)", EnvAntiEntropyFetchBatchSize, cfg.AntiEntropyFetchBatchSize, cfg.AntiEntropyPageSize)
	}
	if cfg.AntiEntropyMaxPagesPerPeer < 1 || cfg.AntiEntropyMaxPagesPerPeer > 64 {
		return fmt.Errorf("%s: anti-entropy max pages per peer must be between 1 and 64 (got %d)", EnvAntiEntropyMaxPagesPerPeer, cfg.AntiEntropyMaxPagesPerPeer)
	}
	if cfg.AntiEntropyWorkers < 1 || cfg.AntiEntropyWorkers > 16 {
		return fmt.Errorf("%s: anti-entropy workers must be between 1 and 16 (got %d)", EnvAntiEntropyWorkers, cfg.AntiEntropyWorkers)
	}
	if cfg.AntiEntropyRequestTimeout <= 0 {
		return fmt.Errorf("%s: anti-entropy request timeout must be a positive duration (got %v)", EnvAntiEntropyRequestTimeout, cfg.AntiEntropyRequestTimeout)
	}
	if cfg.AntiEntropyRequestTimeout > 60*time.Second {
		return fmt.Errorf("%s: anti-entropy request timeout must be at most 60s (got %v)", EnvAntiEntropyRequestTimeout, cfg.AntiEntropyRequestTimeout)
	}
	return nil
}

// isValidDNSSuffix reports whether s is a syntactically plausible DNS name
// with at least two dot-separated labels. It does not perform name resolution;
// it only checks syntax so a malformed suffix is rejected at load time.
func isValidDNSSuffix(s string) bool {
	labels := strings.Split(s, ".")
	if len(labels) < 2 {
		return false
	}
	for _, l := range labels {
		if l == "" || len(l) > 63 {
			return false
		}
		for i := 0; i < len(l); i++ {
			c := l[i]
			if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
				return false
			}
		}
	}
	return true
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

// validateLifecycleConfig checks the lifecycle protection flag (Milestone 5A).
// Lifecycle protection requires peer mTLS to be enabled (the peer identity
// configuration must be valid). It does NOT require gossip or anti-entropy to
// be enabled; those are independent opt-in features.
//
// When lifecycle protection is disabled (the default), no additional
// validation is performed and all existing Milestone 4 behavior is preserved.
func validateLifecycleConfig(cfg *Config) error {
	if !cfg.LifecycleEnabled {
		return nil
	}
	// Lifecycle protection requires peer mTLS (the transport for
	// authenticated peer identity).
	if !cfg.PeerTLSEnabled {
		return fmt.Errorf("%s: lifecycle protection requires peer mTLS to be enabled (%s=true)", EnvLifecycleEnabled, EnvPeerTLSEnabled)
	}
	return nil
}

// validateAuthConfig checks the browser authentication fields (Milestone 6).
//
// Fail-closed default: when AuthEnabled is true (the default), authentication
// is always on. A missing or invalid credential file does NOT silently enable
// anonymous access — the reloader stays "unavailable" and authenticated
// requests return 503 until a valid credential is loaded. The insecure
// development mode (AuthInsecureDevMode) is the only path that disables
// authentication, and it is a clearly-named opt-in documented as unsuitable for
// production/Kubernetes.
//
// When authentication is off (insecure dev mode, or AuthEnabled explicitly
// false), no duration validation is performed. When on, the reload interval,
// max-stale bound, and session TTLs must all be positive durations within
// documented bounds. The credential file path is not validated for existence
// here (that is the reloader's job at load time, which fails closed when the
// file is absent).
func validateAuthConfig(cfg *Config) error {
	if !cfg.AuthEnabled || cfg.AuthInsecureDevMode {
		return nil
	}
	reloadInterval, err := time.ParseDuration(cfg.AuthReloadInterval)
	if err != nil {
		return fmt.Errorf("%s: %q is not a valid duration: %w", EnvAuthReloadInterval, cfg.AuthReloadInterval, err)
	}
	if reloadInterval <= 0 {
		return fmt.Errorf("%s: %q must be a positive duration", EnvAuthReloadInterval, cfg.AuthReloadInterval)
	}
	if reloadInterval < time.Second {
		return fmt.Errorf("%s: %q must be at least 1s (polling more frequently than 1s is not useful and risks excessive file reads)", EnvAuthReloadInterval, cfg.AuthReloadInterval)
	}
	maxStale, err := time.ParseDuration(cfg.AuthMaxStale)
	if err != nil {
		return fmt.Errorf("%s: %q is not a valid duration: %w", EnvAuthMaxStale, cfg.AuthMaxStale, err)
	}
	if maxStale <= 0 {
		return fmt.Errorf("%s: %q must be a positive duration", EnvAuthMaxStale, cfg.AuthMaxStale)
	}
	sessionTTL, err := time.ParseDuration(cfg.AuthSessionTTL)
	if err != nil {
		return fmt.Errorf("%s: %q is not a valid duration: %w", EnvAuthSessionTTL, cfg.AuthSessionTTL, err)
	}
	if sessionTTL <= 0 {
		return fmt.Errorf("%s: %q must be a positive duration", EnvAuthSessionTTL, cfg.AuthSessionTTL)
	}
	if sessionTTL > 24*time.Hour {
		return fmt.Errorf("%s: %q must be at most 24h (a session longer than a day defeats the short-lived cookie model)", EnvAuthSessionTTL, cfg.AuthSessionTTL)
	}
	idleTTL, err := time.ParseDuration(cfg.AuthSessionIdleTTL)
	if err != nil {
		return fmt.Errorf("%s: %q is not a valid duration: %w", EnvAuthSessionIdleTTL, cfg.AuthSessionIdleTTL, err)
	}
	if idleTTL <= 0 {
		return fmt.Errorf("%s: %q must be a positive duration", EnvAuthSessionIdleTTL, cfg.AuthSessionIdleTTL)
	}
	// The idle TTL must not exceed the absolute TTL (the idle expiry is a
	// subset of the absolute lifetime).
	if idleTTL > sessionTTL {
		return fmt.Errorf("%s: idle TTL (%v) must not exceed the absolute session TTL (%v)", EnvAuthSessionIdleTTL, idleTTL, sessionTTL)
	}
	return nil
}

// validateMessageRetentionConfig checks the message retention fields
// (message retention). The cleanup interval only matters when retention is enabled
// (a non-zero TTL); when the TTL is zero (the default), no validation is
// performed and no cleaner is started. When enabled, the TTL must be positive
// and the cleanup interval must be a positive duration within documented
// bounds. Retention is independent of peer mTLS, gossip, and anti-entropy.
func validateMessageRetentionConfig(cfg *Config) error {
	if cfg.MessageRetentionTTL <= 0 {
		return nil
	}
	if cfg.MessageRetentionTTL < 1*time.Minute {
		return fmt.Errorf("%s: message retention TTL must be at least 1m (got %v); sub-minute TTLs would delete messages before they are meaningfully usable", EnvMessageRetentionTTL, cfg.MessageRetentionTTL)
	}
	if cfg.MessageRetentionInterval <= 0 {
		return fmt.Errorf("%s: message retention cleanup interval must be a positive duration (got %v)", EnvMessageRetentionInterval, cfg.MessageRetentionInterval)
	}
	if cfg.MessageRetentionInterval < 1*time.Second {
		return fmt.Errorf("%s: message retention cleanup interval must be at least 1s (got %v)", EnvMessageRetentionInterval, cfg.MessageRetentionInterval)
	}
	if cfg.MessageRetentionInterval > 24*time.Hour {
		return fmt.Errorf("%s: message retention cleanup interval must be at most 24h (got %v)", EnvMessageRetentionInterval, cfg.MessageRetentionInterval)
	}
	// Sweep budgets (0/unset = documented defaults, validated in retention).
	// A negative value is an error: a non-positive budget would disable the
	// bound entirely, defeating the purpose of a bounded sweep.
	if cfg.MessageRetentionMaxSweepRows < 0 {
		return fmt.Errorf("%s: max sweep rows must be a non-negative integer (got %d); use 0 for the default", EnvMessageRetentionMaxSweepRows, cfg.MessageRetentionMaxSweepRows)
	}
	if cfg.MessageRetentionMaxSweepDuration < 0 {
		return fmt.Errorf("%s: max sweep duration must be a non-negative duration (got %v); use 0 for the default", EnvMessageRetentionMaxSweepDuration, cfg.MessageRetentionMaxSweepDuration)
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
