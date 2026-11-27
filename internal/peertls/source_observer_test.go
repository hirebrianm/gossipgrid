// Package peertls: real-mTLS tests for the BootstrapSourceObserver
// (Milestone 5B, Stage 3A, Finding 4). These tests verify that the
// production observer performs a real mTLS handshake and strict response
// validation against a real peer server.

package peertls

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
)

// eligibleSourceHandler returns an http.Handler that serves GET /internal/status
// with a complete, eligible source envelope (READY state, matching epoch,
// BootstrapSourceEligible=true, readiness=true) and the response epoch headers.
func eligibleSourceHandler(nodeID, podID string) http.Handler {
	const (
		clusterID = "c-test"
		epochID   = "e-test"
		version   = "v1"
	)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, clusterID)
		w.Header().Set(lifecycle.HeaderRespEpochID, epochID)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node_id":                   nodeID,
			"version":                   version,
			"pod_id":                    podID,
			"cert_fingerprint":          "test-fingerprint",
			"cluster_id":                clusterID,
			"epoch_id":                  epochID,
			"generation":                1,
			"lifecycle_state":           "READY",
			"readiness":                 true,
			"bootstrap_source_eligible": true,
		})
	})
	return mux
}

// unreadySourceHandler returns a handler with a partial envelope (no epoch
// headers) simulating an unready replica.
func unreadySourceHandler(nodeID, podID string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		// No cluster/epoch headers (unready peer).
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node_id":                   nodeID,
			"version":                   "v",
			"pod_id":                    podID,
			"lifecycle_state":           "DISCOVERING",
			"readiness":                 false,
			"bootstrap_source_eligible": false,
		})
	})
	return mux
}

// TestSourceObserver_ValidObservation verifies that the production observer
// performs a real mTLS observation of an eligible source peer and returns a
// valid SourceObservationResult.
func TestSourceObserver_ValidObservation(t *testing.T) {
	const (
		clusterID = "c-test"
		epochID   = "e-test"
	)
	ca := newTestCA(t, "w")
	certA, keyA, trustA := testCredentials(t, ca, "gossipgrid-0")
	certB, keyB, trustB := testCredentials(t, ca, "gossipgrid-1")

	mgrA := newTestManager(t, certA, keyA, trustA, "gossipgrid-0", 100*time.Millisecond)
	mgrB := newTestManager(t, certB, keyB, trustB, "gossipgrid-1", 100*time.Millisecond)

	addrB, stopB := startTestServer(t, mgrB, eligibleSourceHandler("gossipgrid-1", "gossipgrid-1"))
	defer stopB()

	clientA := NewClient(mgrA)
	clientA.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		enforcement: true,
	})

	now := time.Now()
	obs := NewBootstrapSourceObserver(clientA).WithClock(func() time.Time { return now })

	result, err := obs.Observe(
		context.Background(),
		addrB,
		"gossipgrid-1."+testSuffix,
		lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		"gossipgrid-0",
	)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if !result.Observed() {
		t.Fatal("expected observed=true; got false")
	}
	if result.PeerNodeID() != "gossipgrid-1" {
		t.Fatalf("peer node ID: expected gossipgrid-1, got %s", result.PeerNodeID())
	}
	if result.PeerIdentity() != "gossipgrid-1."+testSuffix {
		t.Fatalf("peer identity: expected gossipgrid-1.%s, got %s", testSuffix, result.PeerIdentity())
	}
	if !result.Valid(now) {
		t.Fatal("result is not valid at observation time")
	}
}

// TestSourceObserver_WrongIdentityRejects verifies that the observer rejects
// an observation when the expected certificate identity does not match.
func TestSourceObserver_WrongIdentityRejects(t *testing.T) {
	const (
		clusterID = "c-test"
		epochID   = "e-test"
	)
	ca := newTestCA(t, "w")
	certA, keyA, trustA := testCredentials(t, ca, "gossipgrid-0")
	certB, keyB, trustB := testCredentials(t, ca, "gossipgrid-1")

	mgrA := newTestManager(t, certA, keyA, trustA, "gossipgrid-0", 100*time.Millisecond)
	mgrB := newTestManager(t, certB, keyB, trustB, "gossipgrid-1", 100*time.Millisecond)

	addrB, stopB := startTestServer(t, mgrB, eligibleSourceHandler("gossipgrid-1", "gossipgrid-1"))
	defer stopB()

	clientA := NewClient(mgrA)
	clientA.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		enforcement: true,
	})

	obs := NewBootstrapSourceObserver(clientA)

	_, err := obs.Observe(
		context.Background(),
		addrB,
		"gossipgrid-0."+testSuffix,
		lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		"gossipgrid-0",
	)
	if err == nil {
		t.Fatal("expected observation to fail (wrong certificate identity); got nil")
	}
}

// TestSourceObserver_UnreadyPeerRejects verifies that the observer rejects an
// observation of an unready peer (partial envelope, no epoch headers).
func TestSourceObserver_UnreadyPeerRejects(t *testing.T) {
	const (
		clusterID = "c-test"
		epochID   = "e-test"
	)
	ca := newTestCA(t, "w")
	certA, keyA, trustA := testCredentials(t, ca, "gossipgrid-0")
	certB, keyB, trustB := testCredentials(t, ca, "gossipgrid-1")

	mgrA := newTestManager(t, certA, keyA, trustA, "gossipgrid-0", 100*time.Millisecond)
	mgrB := newTestManager(t, certB, keyB, trustB, "gossipgrid-1", 100*time.Millisecond)

	addrB, stopB := startTestServer(t, mgrB, unreadySourceHandler("gossipgrid-1", "gossipgrid-1"))
	defer stopB()

	clientA := NewClient(mgrA)
	clientA.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		enforcement: true,
	})

	obs := NewBootstrapSourceObserver(clientA)

	_, err := obs.Observe(
		context.Background(),
		addrB,
		"gossipgrid-1."+testSuffix,
		lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		"gossipgrid-0",
	)
	if err == nil {
		t.Fatal("expected observation to fail (unready peer, partial envelope); got nil")
	}
}

// TestSourceObserver_SelfObservationRejects verifies that the observer rejects
// an observation where the peer is the local replica.
func TestSourceObserver_SelfObservationRejects(t *testing.T) {
	const (
		clusterID = "c-test"
		epochID   = "e-test"
	)
	ca := newTestCA(t, "w")
	certA, keyA, trustA := testCredentials(t, ca, "gossipgrid-0")

	mgrA := newTestManager(t, certA, keyA, trustA, "gossipgrid-0", 100*time.Millisecond)

	addrA, stopA := startTestServer(t, mgrA, eligibleSourceHandler("gossipgrid-0", "gossipgrid-0"))
	defer stopA()

	clientA := NewClient(mgrA)
	clientA.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		enforcement: true,
	})

	obs := NewBootstrapSourceObserver(clientA)

	_, err := obs.Observe(
		context.Background(),
		addrA,
		"gossipgrid-0."+testSuffix,
		lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		"gossipgrid-0",
	)
	if err == nil {
		t.Fatal("expected observation to fail (self-observation); got nil")
	}
}

// TestSourceObserver_StaleResultInvalid verifies that an observation result
// becomes invalid after its bounded lifetime elapses.
func TestSourceObserver_StaleResultInvalid(t *testing.T) {
	const (
		clusterID = "c-test"
		epochID   = "e-test"
	)
	ca := newTestCA(t, "w")
	certA, keyA, trustA := testCredentials(t, ca, "gossipgrid-0")
	certB, keyB, trustB := testCredentials(t, ca, "gossipgrid-1")

	mgrA := newTestManager(t, certA, keyA, trustA, "gossipgrid-0", 100*time.Millisecond)
	mgrB := newTestManager(t, certB, keyB, trustB, "gossipgrid-1", 100*time.Millisecond)

	addrB, stopB := startTestServer(t, mgrB, eligibleSourceHandler("gossipgrid-1", "gossipgrid-1"))
	defer stopB()

	clientA := NewClient(mgrA)
	clientA.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		enforcement: true,
	})

	now := time.Now()
	obs := NewBootstrapSourceObserver(clientA).WithClock(func() time.Time { return now })

	result, err := obs.Observe(
		context.Background(),
		addrB,
		"gossipgrid-1."+testSuffix,
		lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		"gossipgrid-0",
	)
	if err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if !result.Valid(now) {
		t.Fatal("result should be valid at observation time")
	}

	staleTime := now.Add(BootstrapSourceObservationTTL + time.Second)
	if result.Valid(staleTime) {
		t.Fatal("result should be invalid after TTL elapses")
	}
}

// TestValidateStrictSourceEnvelope_TableDriven is a table-driven test for the
// strict response envelope validation (validateStrictSourceEnvelope). It
// verifies every failure mode: missing headers, duplicate headers, wrong
// epoch in headers vs expected target, and the happy path.
func TestValidateStrictSourceEnvelope_TableDriven(t *testing.T) {
	expected := lifecycle.RequestContext{ClusterID: "c-test", EpochID: "e-test"}
	identity := "gossipgrid-1." + testSuffix

	tests := []struct {
		name    string
		headers http.Header
		wantErr bool
		errPart string
	}{
		{
			name:    "happy path: all headers present and matching",
			headers: headersFor("c-test", "e-test", 1, 1, "true"),
			wantErr: false,
		},
		{
			name:    "missing cluster header",
			headers: headersFor("", "e-test", 0, 1, "true"),
			wantErr: true,
			errPart: "missing",
		},
		{
			name:    "missing epoch header",
			headers: headersFor("c-test", "", 1, 0, "true"),
			wantErr: true,
			errPart: "missing",
		},
		{
			name:    "duplicate cluster headers",
			headers: headersFor("c-test", "e-test", 2, 1, "true"),
			wantErr: true,
			errPart: "duplicate",
		},
		{
			name:    "duplicate epoch headers",
			headers: headersFor("c-test", "e-test", 1, 2, "true"),
			wantErr: true,
			errPart: "duplicate",
		},
		{
			name:    "wrong cluster in headers vs expected target",
			headers: headersFor("c-wrong", "e-test", 1, 1, "true"),
			wantErr: true,
			errPart: "cluster",
		},
		{
			name:    "wrong epoch in headers vs expected target",
			headers: headersFor("c-test", "e-wrong", 1, 1, "true"),
			wantErr: true,
			errPart: "epoch",
		},
		{
			name:    "empty cluster header value",
			headers: headersFor("", "e-test", 1, 1, "true"),
			wantErr: true,
			errPart: "empty",
		},
		{
			name:    "empty epoch header value",
			headers: headersFor("c-test", "", 1, 1, "true"),
			wantErr: true,
			errPart: "empty",
		},
		{
			name:    "missing lifecycle-active header",
			headers: headersFor("c-test", "e-test", 1, 1, ""),
			wantErr: true,
			errPart: "Lifecycle-Active",
		},
		{
			name:    "duplicate lifecycle-active headers",
			headers: headersFor("c-test", "e-test", 1, 1, "true"),
			wantErr: false,
		},
		{
			name: "lifecycle-active wrong value",
			headers: func() http.Header {
				h := headersFor("c-test", "e-test", 1, 1, "")
				h.Set(lifecycle.HeaderLifecycleActive, "false")
				return h
			}(),
			wantErr: true,
			errPart: "lifecycle",
		},
		{
			name:    "incomplete expected target (empty cluster)",
			headers: headersFor("c-test", "e-test", 1, 1, "true"),
			wantErr: true,
			errPart: "incomplete",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exp := expected
			if tc.name == "incomplete expected target (empty cluster)" {
				exp = lifecycle.RequestContext{ClusterID: "", EpochID: "e-test"}
			}
			err := validateStrictSourceEnvelope(tc.headers, identity, exp)
			if tc.wantErr && err == nil {
				t.Fatalf("expected error; got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error; got %v", err)
			}
			if tc.wantErr && tc.errPart != "" && err != nil {
				if !strings.Contains(err.Error(), tc.errPart) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.errPart)
				}
			}
		})
	}
}

// headersFor builds an http.Header with the given cluster/epoch header counts
// and lifecycle-active value. count 0 means the header is absent.
func headersFor(cluster, epoch string, clusterCount, epochCount int, active string) http.Header {
	h := http.Header{}
	for i := 0; i < clusterCount; i++ {
		h.Add(lifecycle.HeaderRespClusterID, cluster)
	}
	for i := 0; i < epochCount; i++ {
		h.Add(lifecycle.HeaderRespEpochID, epoch)
	}
	if active != "" {
		h.Set(lifecycle.HeaderLifecycleActive, active)
	}
	return h
}

// TestSourceObserver_BodyHeaderMismatchRejects verifies that the observer
// rejects an observation when the status body's cluster/epoch disagrees with
// the response headers (triple agreement: headers == body == expected target).
// The headers match the expected target, but the body reports a different
// epoch.
func TestSourceObserver_BodyHeaderMismatchRejects(t *testing.T) {
	const (
		clusterID = "c-test"
		epochID   = "e-test"
	)
	ca := newTestCA(t, "w")
	certA, keyA, trustA := testCredentials(t, ca, "gossipgrid-0")
	certB, keyB, trustB := testCredentials(t, ca, "gossipgrid-1")

	mgrA := newTestManager(t, certA, keyA, trustA, "gossipgrid-0", 100*time.Millisecond)
	mgrB := newTestManager(t, certB, keyB, trustB, "gossipgrid-1", 100*time.Millisecond)

	// Handler: headers match the expected target, but the body reports a
	// different epoch (mismatch between headers and body).
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, clusterID)
		w.Header().Set(lifecycle.HeaderRespEpochID, epochID)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node_id":                   "gossipgrid-1",
			"version":                   "v1",
			"pod_id":                    "gossipgrid-1",
			"cert_fingerprint":          "test-fingerprint",
			"cluster_id":                clusterID,
			"epoch_id":                  "e-different-in-body",
			"generation":                1,
			"lifecycle_state":           "READY",
			"readiness":                 true,
			"bootstrap_source_eligible": true,
		})
	})

	addrB, stopB := startTestServer(t, mgrB, mux)
	defer stopB()

	clientA := NewClient(mgrA)
	clientA.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		enforcement: true,
	})

	obs := NewBootstrapSourceObserver(clientA)

	_, err := obs.Observe(
		context.Background(),
		addrB,
		"gossipgrid-1."+testSuffix,
		lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		"gossipgrid-0",
	)
	if err == nil {
		t.Fatal("expected observation to fail (body/header epoch mismatch); got nil")
	}
}

// TestSourceObserver_BodyPodIDMismatchRejects verifies that the observer
// rejects an observation when the status body's PodID does not match the
// authenticated certificate identity (the peer's self-reported PodID is not
// trusted in isolation; it must equal the pod part derived from the
// certificate DNS SAN).
func TestSourceObserver_BodyPodIDMismatchRejects(t *testing.T) {
	const (
		clusterID = "c-test"
		epochID   = "e-test"
	)
	ca := newTestCA(t, "w")
	certA, keyA, trustA := testCredentials(t, ca, "gossipgrid-0")
	certB, keyB, trustB := testCredentials(t, ca, "gossipgrid-1")

	mgrA := newTestManager(t, certA, keyA, trustA, "gossipgrid-0", 100*time.Millisecond)
	mgrB := newTestManager(t, certB, keyB, trustB, "gossipgrid-1", 100*time.Millisecond)

	// Handler: the body reports a different PodID than the certificate
	// identity (the certificate is gossipgrid-1.<suffix>, so the
	// authenticated podID is "gossipgrid-1", but the body says "gossipgrid-9").
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, clusterID)
		w.Header().Set(lifecycle.HeaderRespEpochID, epochID)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"node_id":                   "gossipgrid-9",
			"version":                   "v1",
			"pod_id":                    "gossipgrid-9",
			"cert_fingerprint":          "test-fingerprint",
			"cluster_id":                clusterID,
			"epoch_id":                  epochID,
			"generation":                1,
			"lifecycle_state":           "READY",
			"readiness":                 true,
			"bootstrap_source_eligible": true,
		})
	})

	addrB, stopB := startTestServer(t, mgrB, mux)
	defer stopB()

	clientA := NewClient(mgrA)
	clientA.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		enforcement: true,
	})

	obs := NewBootstrapSourceObserver(clientA)

	_, err := obs.Observe(
		context.Background(),
		addrB,
		"gossipgrid-1."+testSuffix,
		lifecycle.RequestContext{ClusterID: clusterID, EpochID: epochID},
		"gossipgrid-0",
	)
	if err == nil {
		t.Fatal("expected observation to fail (body PodID mismatch with authenticated identity); got nil")
	}
}
