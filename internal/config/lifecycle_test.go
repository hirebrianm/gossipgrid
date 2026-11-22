package config

import (
	"testing"
)

func TestLifecycleDisabledByDefault(t *testing.T) {
	lookup := func(key string) (string, bool) {
		return "", false
	}
	cfg, err := load(lookup)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.LifecycleEnabled {
		t.Error("lifecycle should be disabled by default")
	}
}

func TestLifecycleEnabledRequiresPeerTLS(t *testing.T) {
	lookup := func(key string) (string, bool) {
		switch key {
		case EnvLifecycleEnabled:
			return "true", true
		case EnvPeerTLSEnabled:
			return "false", true
		}
		return "", false
	}
	_, err := load(lookup)
	if err == nil {
		t.Error("expected error when lifecycle enabled without peer TLS")
	}
}

func TestLifecycleEnabledWithPeerTLS(t *testing.T) {
	lookup := func(key string) (string, bool) {
		switch key {
		case EnvLifecycleEnabled:
			return "true", true
		case EnvPeerTLSEnabled:
			return "true", true
		case EnvPeerCertFile:
			return "/certs/tls.crt", true
		case EnvPeerKeyFile:
			return "/certs/tls.key", true
		case EnvPeerTrustBundleFile:
			return "/certs/ca-bundle.pem", true
		case EnvPeerIdentitySuffix:
			return "gossipgrid-headless.test.svc", true
		}
		return "", false
	}
	cfg, err := load(lookup)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !cfg.LifecycleEnabled {
		t.Error("lifecycle should be enabled")
	}
}

func TestLifecycleMalformedBoolean(t *testing.T) {
	lookup := func(key string) (string, bool) {
		switch key {
		case EnvLifecycleEnabled:
			return "maybe", true
		}
		return "", false
	}
	_, err := load(lookup)
	if err == nil {
		t.Error("expected error for malformed boolean")
	}
}
