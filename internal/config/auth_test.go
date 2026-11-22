package config

import (
	"strings"
	"testing"
)

// TestLoadAuthConfigEnabled verifies that setting GOSSIPGRID_AUTH_CREDENTIAL_FILE
// enables browser authentication and that the default values for the other
// auth fields are applied.
func TestLoadAuthConfigEnabled(t *testing.T) {
	env := map[string]string{
		EnvAuthCredentialFile: "/creds/admin",
	}
	cfg, err := load(envGet(env))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.AuthCredentialFile != "/creds/admin" {
		t.Errorf("AuthCredentialFile = %q, want /creds/admin", cfg.AuthCredentialFile)
	}
	if cfg.AuthReloadInterval != "2s" {
		t.Errorf("AuthReloadInterval = %q, want 2s (default)", cfg.AuthReloadInterval)
	}
	if cfg.AuthMaxStale != "5m" {
		t.Errorf("AuthMaxStale = %q, want 5m (default)", cfg.AuthMaxStale)
	}
	if cfg.AuthSessionTTL != "2h" {
		t.Errorf("AuthSessionTTL = %q, want 2h (default)", cfg.AuthSessionTTL)
	}
	if cfg.AuthSessionIdleTTL != "30m" {
		t.Errorf("AuthSessionIdleTTL = %q, want 30m (default)", cfg.AuthSessionIdleTTL)
	}
	// CookieSecure defaults to true when unset.
	if !cfg.AuthCookieSecure {
		t.Error("AuthCookieSecure = false, want true (default)")
	}
}

// TestLoadAuthConfigEnabledByDefault verifies the fail-closed default: when no
// auth variables are set, AuthEnabled is true (authentication on) and the
// credential file is empty (the reloader stays "unavailable"; authenticated
// requests return 503, never anonymous access).
func TestLoadAuthConfigEnabledByDefault(t *testing.T) {
	cfg, err := load(envGet(nil))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !cfg.AuthEnabled {
		t.Errorf("AuthEnabled = false, want true (fail-closed default)")
	}
	if cfg.AuthInsecureDevMode {
		t.Errorf("AuthInsecureDevMode = true, want false (opt-in default off)")
	}
	if cfg.AuthCredentialFile != "" {
		t.Errorf("AuthCredentialFile = %q, want empty", cfg.AuthCredentialFile)
	}
}

// TestLoadAuthConfigInsecureDevMode verifies that the explicitly-named
// opt-in GOSSIPGRID_AUTH_INSECURE_DEV_MODE=true disables authentication
// (AuthEnabled is still true, but the insecure flag turns it off).
func TestLoadAuthConfigInsecureDevMode(t *testing.T) {
	env := map[string]string{
		EnvAuthInsecureDevMode: "true",
	}
	cfg, err := load(envGet(env))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !cfg.AuthInsecureDevMode {
		t.Errorf("AuthInsecureDevMode = false, want true (explicit opt-in)")
	}
}

// TestLoadAuthConfigExplicitDisable verifies that an explicit
// GOSSIPGRID_AUTH_ENABLED=false disables authentication.
func TestLoadAuthConfigExplicitDisable(t *testing.T) {
	env := map[string]string{
		EnvAuthEnabled: "false",
	}
	cfg, err := load(envGet(env))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.AuthEnabled {
		t.Errorf("AuthEnabled = true, want false (explicit disable)")
	}
}

// TestLoadAuthConfigBadAuthEnabled verifies that an unrecognised
// GOSSIPGRID_AUTH_ENABLED value is an error (a typo must not silently change
// a security feature).
func TestLoadAuthConfigBadAuthEnabled(t *testing.T) {
	env := map[string]string{
		EnvAuthEnabled: "maybe",
	}
	_, err := load(envGet(env))
	if err == nil || !strings.Contains(err.Error(), EnvAuthEnabled) {
		t.Errorf("load (bad auth enabled) err = %v, want an error naming %s", err, EnvAuthEnabled)
	}
}

// TestLoadAuthConfigCookieSecureOverride verifies that an explicit
// GOSSIPGRID_AUTH_COOKIE_SECURE=false sets the flag to false (local dev over
// HTTP).
func TestLoadAuthConfigCookieSecureOverride(t *testing.T) {
	env := map[string]string{
		EnvAuthCredentialFile: "/creds/admin",
		EnvAuthCookieSecure:   "false",
	}
	cfg, err := load(envGet(env))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.AuthCookieSecure {
		t.Error("AuthCookieSecure = true, want false (explicit override)")
	}
}

// TestLoadAuthConfigBadCookieSecure verifies that an unrecognised
// GOSSIPGRID_AUTH_COOKIE_SECURE value is an error (a typo must not silently
// weaken the cookie).
func TestLoadAuthConfigBadCookieSecure(t *testing.T) {
	env := map[string]string{
		EnvAuthCredentialFile: "/creds/admin",
		EnvAuthCookieSecure:   "maybe",
	}
	_, err := load(envGet(env))
	if err == nil || !strings.Contains(err.Error(), EnvAuthCookieSecure) {
		t.Errorf("load (bad cookie secure) err = %v, want an error naming %s", err, EnvAuthCookieSecure)
	}
}

// TestValidateAuthConfigBadDurations verifies that, when authentication is
// enabled, a malformed or out-of-range duration is an error.
func TestValidateAuthConfigBadDurations(t *testing.T) {
	cases := map[string]map[string]string{
		"bad reload interval": {
			EnvAuthCredentialFile: "/creds/admin",
			EnvAuthReloadInterval: "not-a-duration",
		},
		"reload interval too small": {
			EnvAuthCredentialFile: "/creds/admin",
			EnvAuthReloadInterval: "100ms",
		},
		"bad max stale": {
			EnvAuthCredentialFile: "/creds/admin",
			EnvAuthMaxStale:       "xyz",
		},
		"session TTL too large": {
			EnvAuthCredentialFile: "/creds/admin",
			EnvAuthSessionTTL:     "48h",
		},
		"idle TTL exceeds absolute TTL": {
			EnvAuthCredentialFile: "/creds/admin",
			EnvAuthSessionTTL:     "1h",
			EnvAuthSessionIdleTTL: "2h",
		},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := load(envGet(env))
			if err == nil {
				t.Errorf("load(%s) = nil, want error", name)
			}
		})
	}
}

// TestValidateAuthConfigValid verifies that a valid set of auth durations is
// accepted.
func TestValidateAuthConfigValid(t *testing.T) {
	env := map[string]string{
		EnvAuthCredentialFile: "/creds/admin",
		EnvAuthReloadInterval: "5s",
		EnvAuthMaxStale:       "10m",
		EnvAuthSessionTTL:     "4h",
		EnvAuthSessionIdleTTL: "30m",
		EnvAuthCookieSecure:   "true",
	}
	cfg, err := load(envGet(env))
	if err != nil {
		t.Fatalf("load (valid) = %v, want nil", err)
	}
	if cfg.AuthCredentialFile == "" {
		t.Error("auth disabled, want enabled")
	}
}

// envGet returns an envFunc (the lookup signature used by load) that reads
// from a map. A nil map means no env vars are set.
func envGet(m map[string]string) envFunc {
	return func(key string) (string, bool) {
		v, ok := m[key]
		return v, ok
	}
}
