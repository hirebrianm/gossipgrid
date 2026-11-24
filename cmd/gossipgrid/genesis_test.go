package main

import (
	"context"
	"errors"
	"os"
	"testing"

	"gossipgrid/internal/lifecycle/k8smetadata"
)

// testGenesisClient is a deterministic k8sClient for runGenesis tests. It
// models the Kubernetes API outcomes (absent object, existing object,
// permission denial, unavailability) with typed errors. The grant logic itself
// is unit-tested in internal/lifecycle/genesis (GrantOperation with an
// injectable writer); this test exercises the runGenesis subcommand's flag
// parsing, the in-cluster namespace resolution, and the complete CLI handling
// of an existing grant.
type testGenesisClient struct {
	getErr      error
	createErr   error
	existingCM  *k8smetadata.ConfigMap
	getCalls    int
	createCalls int
}

func (c *testGenesisClient) Get(_ context.Context, _, _ string) (*k8smetadata.ConfigMap, error) {
	c.getCalls++
	if c.getErr != nil {
		return nil, c.getErr
	}
	if c.existingCM != nil {
		return c.existingCM, nil
	}
	return nil, k8smetadata.NewConfigMapNotFound(errors.New("configmap not found"))
}
func (c *testGenesisClient) Create(_ context.Context, _, _ string, _ map[string]string) (*k8smetadata.ConfigMap, error) {
	c.createCalls++
	return nil, c.createErr
}

// TestRunGenesisFlagParsing verifies that runGenesis rejects unknown arguments
// and missing flag values.
func TestRunGenesisFlagParsing(t *testing.T) {
	if err := runGenesis([]string{"--bogus"}); err == nil {
		t.Error("expected error for unknown flag")
	}
	if err := runGenesis([]string{"--namespace"}); err == nil {
		t.Error("expected error for --namespace without a value")
	}
	if err := runGenesis([]string{"--configmap"}); err == nil {
		t.Error("expected error for --configmap without a value")
	}
}

// TestRunGenesisNonCanonicalConfigMap verifies that runGenesis rejects a
// --configmap value different from the canonical gossipgrid-cluster-metadata.
// The flag is accepted only for the canonical name; any other value has no
// effect and is rejected rather than silently accepted.
func TestRunGenesisNonCanonicalConfigMap(t *testing.T) {
	err := runGenesis([]string{"--namespace", "test-ns", "--configmap", "some-other-name"})
	if err == nil {
		t.Fatal("expected an error for a non-canonical --configmap value")
	}
	if !containsSubstr(err.Error(), "must be") {
		t.Errorf("error = %q, want it to mention the canonical name requirement", err.Error())
	}
}

// TestRunGenesisCanonicalConfigMapAccepted verifies that runGenesis accepts the
// canonical --configmap value (gossipgrid-cluster-metadata) and proceeds to the
// client build (which fails outside a cluster, but the flag parsing succeeds).
func TestRunGenesisCanonicalConfigMapAccepted(t *testing.T) {
	err := runGenesis([]string{"--namespace", "test-ns", "--configmap", "gossipgrid-cluster-metadata"})
	// Outside a cluster, the error is about the client build, not the flag.
	if err == nil {
		t.Fatal("expected an error outside a cluster")
	}
	// The error must NOT be the canonical-name rejection (which mentions
	// "the canonical cluster-metadata ConfigMap name").
	if containsSubstr(err.Error(), "canonical cluster-metadata ConfigMap name") {
		t.Errorf("error = %q, the canonical configmap value should not be rejected", err.Error())
	}
	if !containsSubstr(err.Error(), "building Kubernetes client") {
		t.Errorf("error = %q, want the client build error (flag parsing succeeded)", err.Error())
	}
}

// TestRunGenesisOutsideCluster verifies that runGenesis with valid flags but
// no in-cluster configuration fails closed (a "building Kubernetes client"
// error). Outside a cluster (the test environment), rest.InClusterConfig fails
// because there is no service account or API server; the grant must not
// proceed without a working Kubernetes API (fail-closed, no safety inferred).
func TestRunGenesisOutsideCluster(t *testing.T) {
	err := runGenesis([]string{"--namespace", "test-ns", "--configmap", "gossipgrid-cluster-metadata"})
	if err == nil {
		t.Fatal("expected an error outside a cluster (no in-cluster config)")
	}
	if !containsSubstr(err.Error(), "building Kubernetes client") {
		t.Errorf("error = %q, want it to mention the client build failure", err.Error())
	}
}

// TestInClusterNamespaceOutsideCluster verifies that inClusterNamespace returns
// "" (the fail-closed condition) when the in-cluster namespace file is missing
// (outside a cluster). The grant cannot create a ConfigMap without a
// namespace; an empty namespace is an error.
func TestInClusterNamespaceOutsideCluster(t *testing.T) {
	// Point at a non-existent path (the default, outside a cluster).
	origPath := inClusterNamespacePath
	inClusterNamespacePath = "/nonexistent/namespace/file"
	defer func() { inClusterNamespacePath = origPath }()
	if ns := inClusterNamespace(); ns != "" {
		t.Errorf("inClusterNamespace = %q, want empty outside a cluster", ns)
	}
}

// TestInClusterNamespaceFromMountedFile verifies that inClusterNamespace reads
// the namespace from a plain-text file (the way Kubernetes mounts it at
// /var/run/secrets/kubernetes.io/serviceaccount/namespace). It writes a
// temporary file, points the path variable at it, and verifies the namespace
// is read correctly with whitespace trimmed.
func TestInClusterNamespaceFromMountedFile(t *testing.T) {
	tmpDir := t.TempDir()
	nsFile := tmpDir + "/namespace"
	if err := os.WriteFile(nsFile, []byte("gossipgrid\n"), 0o600); err != nil {
		t.Fatalf("writing temp namespace file: %v", err)
	}
	origPath := inClusterNamespacePath
	inClusterNamespacePath = nsFile
	defer func() { inClusterNamespacePath = origPath }()
	if ns := inClusterNamespace(); ns != "gossipgrid" {
		t.Errorf("inClusterNamespace = %q, want %q", ns, "gossipgrid")
	}
}

// TestInClusterNamespaceTrimsWhitespace verifies that inClusterNamespace trims
// leading and trailing whitespace from the namespace file content.
func TestInClusterNamespaceTrimsWhitespace(t *testing.T) {
	tmpDir := t.TempDir()
	nsFile := tmpDir + "/namespace"
	if err := os.WriteFile(nsFile, []byte("  prod-ns  \n"), 0o600); err != nil {
		t.Fatalf("writing temp namespace file: %v", err)
	}
	origPath := inClusterNamespacePath
	inClusterNamespacePath = nsFile
	defer func() { inClusterNamespacePath = origPath }()
	if ns := inClusterNamespace(); ns != "prod-ns" {
		t.Errorf("inClusterNamespace = %q, want %q", ns, "prod-ns")
	}
}

// TestRunGenesisExistingGrantCLI verifies the complete CLI handling of an
// existing grant using an injectable client. When the ConfigMap already exists
// with a valid pending genesis authorization, runGenesis must:
//   - return the existing authoritative identity (not generate a new one)
//   - not attempt a Create
//   - exit 0 (return nil error)
//
// This is the documented contract: an already-granted result is a successful
// idempotent outcome, not a failure.
func TestRunGenesisExistingGrantCLI(t *testing.T) {
	// Build a fake client that returns the existing valid pending grant.
	fake := &testGenesisClient{
		existingCM: &k8smetadata.ConfigMap{
			Data: map[string]string{
				"metadata": `{"schemaVersion":1,"clusterId":"0190e8a2-6b0c-7000-8000-000000000000","epochId":"0190e8a3-1c2d-7000-9000-111111111111","generation":1,"initialized":false,"genesisAuthorized":true,"genesisConsumer":"","everAcceptedMessages":false,"dataLossProtected":false}`,
			},
		},
	}
	origFactory := newGenesisClient
	newGenesisClient = func() (k8sClient, error) { return fake, nil }
	defer func() { newGenesisClient = origFactory }()

	// Also override the namespace path so we don't need a live cluster.
	origNSPath := inClusterNamespacePath
	inClusterNamespacePath = "/nonexistent/namespace"
	defer func() { inClusterNamespacePath = origNSPath }()

	err := runGenesis([]string{"--namespace", "test-ns"})
	if err != nil {
		t.Fatalf("runGenesis = %v, want nil (an existing valid pending grant is a successful idempotent outcome, exit 0)", err)
	}
	// The client must not have attempted a Create (the existing identity was
	// returned directly).
	if fake.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (an existing valid pending grant must not trigger a Create)", fake.createCalls)
	}
	// The client must have performed at least one Get (preflight + idempotency check).
	if fake.getCalls < 2 {
		t.Errorf("getCalls = %d, want >= 2 (preflight read + idempotency check read)", fake.getCalls)
	}
}

// containsSubstr reports whether sub appears in s (a test helper; the
// standard library's strings.Contains is not otherwise used in this package).
func containsSubstr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// _ is a compile-time assertion that testGenesisClient satisfies k8sClient.
var _ k8sClient = (*testGenesisClient)(nil)
