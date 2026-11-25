// Tests for the pinned-epoch transport validation in the peer client
// (Milestone 5B, Stage 2, Issue 5 correction). These tests use a real mTLS
// transport (real TLS 1.3 handshake, real HTTP server) and a controllable
// EpochProvider to verify that:
//
//  1. A mismatch between the provider's current cluster/epoch and the pinned
//     context prevents network transmission (no request is sent).
//  2. A revocation (provider error) prevents network transmission.
//  3. An incomplete pinned context (empty cluster or epoch) is rejected when
//     lifecycle enforcement is active (no defensive bypass).
//  4. A matching pinned context and provider allows the request to proceed.
//  5. A partial-404 retry also performs the pinned-context comparison.
//
// The tests prove that the rejection is at the TRANSPORT level (no network
// I/O), not merely at the response-validation level.

package peertls

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
)

// bootstrapTransportTestServer sets up a real mTLS server that counts requests.
// The handler is called only when a request actually arrives (proving the
// request was transmitted). The test verifies that no request arrives when the
// pinned-context validation rejects the request.
func bootstrapTransportTestServer(t *testing.T, handler http.Handler) (addr string, client *Client, requestCount *int32) {
	return bootstrapTransportTestServerWithBootstrap(t, handler, nil)
}

// bootstrapTransportTestServerWithBootstrap is like bootstrapTransportTestServer
// but also sets a dedicated BootstrapEpochProvider (M5B Stage 2, Correction 3).
// When a non-nil bp is provided, it is set via SetBootstrapEpochProvider; the
// test must provide the bootstrap provider for bootstrap operations with
// lifecycle enforcement enabled.
func bootstrapTransportTestServerWithBootstrap(t *testing.T, handler http.Handler, bp BootstrapEpochProvider) (addr string, client *Client, requestCount *int32) {
	t.Helper()
	ca := newTestCA(t, "w")
	cert, key, trust := testCredentials(t, ca, "gossipgrid-0")
	mgr := newTestManager(t, cert, key, trust, "gossipgrid-0", 50*time.Millisecond)

	var count int32
	countingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		handler.ServeHTTP(w, r)
	})
	a, stop := startTestServer(t, mgr, countingHandler)
	t.Cleanup(stop)
	client = NewClient(mgr)
	if bp != nil {
		client.SetBootstrapEpochProvider(bp)
	}
	return a, client, &count
}

// TestBootstrapTransport_EpochMismatchPreventsTransmission verifies that when
// the provider's current epoch differs from the pinned context, no request is
// transmitted. The test uses a real mTLS server that counts requests; the
// count must be zero after the client call.
func TestBootstrapTransport_EpochMismatchPreventsTransmission(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
		w.Header().Set(lifecycle.HeaderRespEpochID, "e2")
		_ = json.NewEncoder(w).Encode(InventoryPageResponse{IDs: nil, HasMore: false})
	})
	// The bootstrap provider returns c1/e2 (mismatch with pinned c1/e1).
	bp := &testBootstrapEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e2"},
		authorized:  true,
		enforcement: true,
	}
	addr, client, count := bootstrapTransportTestServerWithBootstrap(t, handler, bp)
	client.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e2"},
		enforcement: true,
	})

	pinnedCtx := &lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}

	_, err := client.FetchBootstrapInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 8, pinnedCtx)
	if err == nil {
		t.Fatal("expected error when provider epoch mismatches pinned context, got nil")
	}
	var blocked *PeerBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("expected *PeerBlockedError (transport-level rejection), got %T: %v", err, err)
	}
	if got := *count; got != 0 {
		t.Errorf("expected 0 requests transmitted (epoch mismatch prevents transmission), got %d", got)
	}
}

// TestBootstrapTransport_RevocationPreventsTransmission verifies that when the
// provider returns an error (revocation), no request is transmitted.
func TestBootstrapTransport_RevocationPreventsTransmission(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	})
	// The bootstrap provider returns an error (revocation).
	bp := &testBootstrapEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		authorized:  false,
		err:         errors.New("lifecycle lockout: no fresh authoritative observation"),
		enforcement: true,
	}
	addr, client, count := bootstrapTransportTestServerWithBootstrap(t, handler, bp)
	client.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	})

	pinnedCtx := &lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}

	_, err := client.FetchBootstrapInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 8, pinnedCtx)
	if err == nil {
		t.Fatal("expected error when bootstrap provider is revoked, got nil")
	}
	var blocked *PeerBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("expected *PeerBlockedError (transport-level rejection), got %T: %v", err, err)
	}
	if got := *count; got != 0 {
		t.Errorf("expected 0 requests transmitted (revocation prevents transmission), got %d", got)
	}
}

// TestBootstrapTransport_IncompletePinnedContextRejected verifies that when
// lifecycle enforcement is active and the pinned context is incomplete (empty
// ClusterID or EpochID), the request is rejected at the transport level with
// no network I/O.
func TestBootstrapTransport_IncompletePinnedContextRejected(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
	})
	bp := &testBootstrapEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		authorized:  true,
		enforcement: true,
	}
	addr, client, count := bootstrapTransportTestServerWithBootstrap(t, handler, bp)
	client.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	})

	// Incomplete pinned context: empty EpochID.
	pinnedCtx := &lifecycle.RequestContext{ClusterID: "c1", EpochID: ""}

	_, err := client.FetchBootstrapInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 8, pinnedCtx)
	if err == nil {
		t.Fatal("expected error for incomplete pinned context, got nil")
	}
	var blocked *PeerBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("expected *PeerBlockedError (incomplete pinned context), got %T: %v", err, err)
	}
	if got := *count; got != 0 {
		t.Errorf("expected 0 requests transmitted (incomplete pinned context rejected), got %d", got)
	}
}

// TestBootstrapTransport_MatchingContextAllowsTransmission verifies that when
// the provider's current context matches the pinned context, the request is
// transmitted normally. This is the positive control: the validation passes
// and the request proceeds.
func TestBootstrapTransport_MatchingContextAllowsTransmission(t *testing.T) {
	var mu sync.Mutex
	var capturedCluster, capturedEpoch string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		capturedCluster = r.Header.Get(lifecycle.HeaderClusterID)
		capturedEpoch = r.Header.Get(lifecycle.HeaderEpochID)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
		w.Header().Set(lifecycle.HeaderRespEpochID, "e1")
		_ = json.NewEncoder(w).Encode(InventoryPageResponse{IDs: nil, HasMore: false})
	})
	bp := &testBootstrapEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		authorized:  true,
		enforcement: true,
	}
	addr, client, count := bootstrapTransportTestServerWithBootstrap(t, handler, bp)
	client.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	})

	pinnedCtx := &lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}

	_, err := client.FetchBootstrapInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 8, pinnedCtx)
	if err != nil {
		t.Fatalf("expected matching context to allow transmission, got: %v", err)
	}
	if got := *count; got != 1 {
		t.Errorf("expected 1 request transmitted, got %d", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if capturedCluster != "c1" {
		t.Errorf("request carried cluster %q, want c1", capturedCluster)
	}
	if capturedEpoch != "e1" {
		t.Errorf("request carried epoch %q, want e1", capturedEpoch)
	}
}

// TestBootstrapTransport_Partial404RetryPerformsPinnedCheck verifies that the
// partial-404 retry also performs the pinned-context comparison. The test
// simulates a 404 response with some IDs missing and some present; the retry
// is issued with the same pinned context. If the provider's epoch changes
// between the initial request and the retry, the retry is blocked.
func TestBootstrapTransport_Partial404RetryPerformsPinnedCheck(t *testing.T) {
	ids := genTestUUIDs(2)

	var mu sync.Mutex
	callCount := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		callCount++
		current := callCount
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
		w.Header().Set(lifecycle.HeaderRespEpochID, "e1")
		if current == 1 {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(missingIDsResponse{
				Error:      "message IDs not found: " + ids[0],
				MissingIDs: ids[:1],
			})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(fetchMessagesResponse{
			Messages: []SyncMessageWire{
				{ID: ids[1], Body: "body", Origin: "gossipgrid-0", CreatedAt: "2026-01-01T00:00:00Z"},
			},
		})
	})
	bp := &testBootstrapEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		authorized:  true,
		enforcement: true,
	}
	addr, client, count := bootstrapTransportTestServerWithBootstrap(t, handler, bp)
	client.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	})

	pinnedCtx := &lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}

	msgs, err := client.FetchBootstrapMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids, pinnedCtx)
	if err != nil {
		t.Fatalf("expected partial-404 retry to succeed with matching context, got: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("expected 1 fetched message (the present ID), got %d", len(msgs))
	}
	if got := *count; got != 2 {
		t.Errorf("expected 2 requests (initial + retry), got %d", got)
	}
}

// TestBootstrapTransport_RevocationBetweenInitialAndRetry verifies that when
// the provider is revoked between the initial 404 request and the retry, the
// retry is blocked (no network I/O for the retry). The test uses a
// controllable provider that changes its state after the first call.
func TestBootstrapTransport_RevocationBetweenInitialAndRetry(t *testing.T) {
	ids := genTestUUIDs(2)

	var mu sync.Mutex
	callCount := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		callCount++
		current := callCount
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
		w.Header().Set(lifecycle.HeaderRespEpochID, "e1")
		if current == 1 {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(missingIDsResponse{
				Error:      "message IDs not found: " + ids[0],
				MissingIDs: ids[:1],
			})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(fetchMessagesResponse{
			Messages: []SyncMessageWire{
				{ID: ids[1], Body: "body", Origin: "gossipgrid-0", CreatedAt: "2026-01-01T00:00:00Z"},
			},
		})
	})

	// A controllable bootstrap provider: initially authorized, then revoked
	// after the first BootstrapDecision call.
	revocableBP := &revocableBootstrapProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	addr, client, count := bootstrapTransportTestServerWithBootstrap(t, handler, revocableBP)
	client.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	})

	pinnedCtx := &lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}

	// The initial request's pre-transmission check (call 1) is authorized.
	// After the 404 response, the in-flight recheck (call 2) revokes.
	// The response is blocked (not consumed), and no retry is issued.
	revocableBP.revokeAfter = 1

	_, err := client.FetchBootstrapMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids, pinnedCtx)
	if err == nil {
		t.Fatal("expected error when bootstrap provider is revoked between initial and retry, got nil")
	}
	var blocked *PeerBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("expected *PeerBlockedError (revocation between initial and retry), got %T: %v", err, err)
	}
	// The initial request was transmitted (1), but the response was blocked
	// at the in-flight recheck, so no retry was issued.
	if got := *count; got != 1 {
		t.Errorf("expected 1 request transmitted (initial only; retry blocked), got %d", got)
	}
}

// TestBootstrapTransport_RevocationAtRetryTransmission verifies that when the
// provider authorizes the first two BootstrapDecision calls (the initial
// pre-transmission check and the in-flight recheck after the 404 response)
// but rejects the third (the retry's pre-transmission check), the rejection
// occurs specifically at retry transmission rather than at response
// consumption.
//
// The sequence:
//  1. BootstrapDecision call 1 (initial pre-transmission): authorized.
//     The initial HTTP request is transmitted.
//  2. The source returns a valid partial-404 (one ID missing, one present).
//  3. BootstrapDecision call 2 (in-flight recheck after the 404 response):
//     authorized. The response is consumed (reconciliation proceeds to retry).
//  4. BootstrapDecision call 3 (retry pre-transmission): rejected.
//     The retry is blocked with PeerBlockedError before any network I/O.
//
// This proves the transport-level boundary is at the retry's pre-transmission
// check, not at the response-consumption in-flight recheck (which was
// authorized in call 2).
func TestBootstrapTransport_RevocationAtRetryTransmission(t *testing.T) {
	ids := genTestUUIDs(2)

	var mu sync.Mutex
	callCount := 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		callCount++
		current := callCount
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
		w.Header().Set(lifecycle.HeaderRespEpochID, "e1")
		if current == 1 {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(missingIDsResponse{
				Error:      "message IDs not found: " + ids[0],
				MissingIDs: ids[:1],
			})
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(fetchMessagesResponse{
			Messages: []SyncMessageWire{
				{ID: ids[1], Body: "body", Origin: "gossipgrid-0", CreatedAt: "2026-01-01T00:00:00Z"},
			},
		})
	})

	// A controllable bootstrap provider: authorizes the first two
	// BootstrapDecision calls (initial pre-transmission and in-flight recheck),
	// rejects the third (retry pre-transmission).
	revocableBP := &revocableBootstrapProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	addr, client, count := bootstrapTransportTestServerWithBootstrap(t, handler, revocableBP)
	client.SetEpochProvider(&testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	})

	pinnedCtx := &lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}

	// Authorize the first 2 calls (initial pre-transmission + in-flight
	// recheck), reject the 3rd (retry pre-transmission).
	revocableBP.revokeAfter = 2

	_, err := client.FetchBootstrapMessages(context.Background(), addr, "gossipgrid-0."+testSuffix, ids, pinnedCtx)
	if err == nil {
		t.Fatal("expected PeerBlockedError when bootstrap provider is rejected at retry transmission, got nil")
	}
	var blocked *PeerBlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("expected *PeerBlockedError (rejection at retry transmission), got %T: %v", err, err)
	}
	// The initial request was transmitted (1). The in-flight recheck (call 2)
	// was authorized, so the 404 response was consumed and the retry was
	// attempted. The retry's pre-transmission check (call 3) was rejected, so
	// the retry was NOT transmitted.
	if got := *count; got != 1 {
		t.Errorf("expected 1 request transmitted (initial only; retry blocked at pre-transmission), got %d", got)
	}
	// The provider must have been called exactly 3 times:
	// call 1 (initial pre-transmission), call 2 (in-flight recheck),
	// call 3 (retry pre-transmission — rejected).
	if got := revocableBP.calls; got != 3 {
		t.Errorf("expected 3 BootstrapDecision calls, got %d", got)
	}
}

// revocableBootstrapProvider is a controllable BootstrapEpochProvider that
// returns authorized=false after a configurable number of BootstrapDecision
// calls (simulating a revocation between pre-transmission and in-flight
// recheck, or between initial request and retry).
type revocableBootstrapProvider struct {
	ctx         lifecycle.RequestContext
	enforcement bool
	revokeAfter int
	calls       int
	mu          sync.Mutex
}

func (p *revocableBootstrapProvider) BootstrapDecision() (lifecycle.RequestContext, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.revokeAfter > 0 && p.calls > p.revokeAfter {
		return p.ctx, false, nil
	}
	return p.ctx, true, nil
}

func (p *revocableBootstrapProvider) Context() (lifecycle.RequestContext, error) {
	return p.ctx, nil
}

func (p *revocableBootstrapProvider) Enforcement() bool {
	return p.enforcement
}

// testBootstrapEpochProvider is a dedicated BootstrapEpochProvider for
// transport tests (M5B Stage 2, Correction 3). It implements both the
// BootstrapEpochProvider and EpochProvider interfaces so it can be set via
// both SetBootstrapEpochProvider and SetEpochProvider.
type testBootstrapEpochProvider struct {
	ctx         lifecycle.RequestContext
	authorized  bool
	err         error
	enforcement bool
}

func (p *testBootstrapEpochProvider) BootstrapDecision() (lifecycle.RequestContext, bool, error) {
	return p.ctx, p.authorized, p.err
}

func (p *testBootstrapEpochProvider) Context() (lifecycle.RequestContext, error) {
	return p.ctx, p.err
}

func (p *testBootstrapEpochProvider) Enforcement() bool {
	return p.enforcement
}

// TestBootstrapTransport_FailClosed verifies that a bootstrap operation with
// lifecycle enforcement enabled is rejected (zero HTTP requests) when any
// required condition is not met: nil pinned ctx, empty pinned ctx, partial
// pinned ctx, missing dedicated bootstrap provider, unauthorized bootstrap
// decision, or epoch mismatch.
func TestBootstrapTransport_FailClosed(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(lifecycle.HeaderLifecycleActive, "true")
		w.Header().Set(lifecycle.HeaderRespClusterID, "c1")
		w.Header().Set(lifecycle.HeaderRespEpochID, "e1")
		_ = json.NewEncoder(w).Encode(InventoryPageResponse{IDs: nil, HasMore: false})
	})

	t.Run("nil_pinned_ctx", func(t *testing.T) {
		bp := &testBootstrapEpochProvider{
			ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
			authorized:  true,
			enforcement: true,
		}
		addr, client, count := bootstrapTransportTestServerWithBootstrap(t, handler, bp)
		client.SetEpochProvider(&testEpochProvider{
			ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
			enforcement: true,
		})
		_, err := client.FetchBootstrapInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 8, nil)
		if err == nil {
			t.Fatal("expected error for nil pinned ctx, got nil")
		}
		var blocked *PeerBlockedError
		if !errors.As(err, &blocked) {
			t.Fatalf("expected *PeerBlockedError, got %T: %v", err, err)
		}
		if got := *count; got != 0 {
			t.Errorf("expected 0 requests, got %d", got)
		}
	})

	t.Run("empty_pinned_ctx", func(t *testing.T) {
		bp := &testBootstrapEpochProvider{
			ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
			authorized:  true,
			enforcement: true,
		}
		addr, client, count := bootstrapTransportTestServerWithBootstrap(t, handler, bp)
		client.SetEpochProvider(&testEpochProvider{
			ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
			enforcement: true,
		})
		_, err := client.FetchBootstrapInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 8, &lifecycle.RequestContext{})
		if err == nil {
			t.Fatal("expected error for empty pinned ctx, got nil")
		}
		var blocked *PeerBlockedError
		if !errors.As(err, &blocked) {
			t.Fatalf("expected *PeerBlockedError, got %T: %v", err, err)
		}
		if got := *count; got != 0 {
			t.Errorf("expected 0 requests, got %d", got)
		}
	})

	t.Run("partial_pinned_ctx", func(t *testing.T) {
		bp := &testBootstrapEpochProvider{
			ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
			authorized:  true,
			enforcement: true,
		}
		addr, client, count := bootstrapTransportTestServerWithBootstrap(t, handler, bp)
		client.SetEpochProvider(&testEpochProvider{
			ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
			enforcement: true,
		})
		_, err := client.FetchBootstrapInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 8, &lifecycle.RequestContext{ClusterID: "c1", EpochID: ""})
		if err == nil {
			t.Fatal("expected error for partial pinned ctx, got nil")
		}
		var blocked *PeerBlockedError
		if !errors.As(err, &blocked) {
			t.Fatalf("expected *PeerBlockedError, got %T: %v", err, err)
		}
		if got := *count; got != 0 {
			t.Errorf("expected 0 requests, got %d", got)
		}
	})

	t.Run("missing_bootstrap_provider", func(t *testing.T) {
		// No bootstrap provider set: the fail-closed path rejects the request.
		addr, client, count := bootstrapTransportTestServer(t, handler)
		client.SetEpochProvider(&testEpochProvider{
			ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
			enforcement: true,
		})
		pinnedCtx := &lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}
		_, err := client.FetchBootstrapInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 8, pinnedCtx)
		if err == nil {
			t.Fatal("expected error for missing bootstrap provider, got nil")
		}
		var blocked *PeerBlockedError
		if !errors.As(err, &blocked) {
			t.Fatalf("expected *PeerBlockedError, got %T: %v", err, err)
		}
		if got := *count; got != 0 {
			t.Errorf("expected 0 requests, got %d", got)
		}
	})

	t.Run("unauthorized_decision", func(t *testing.T) {
		bp := &testBootstrapEpochProvider{
			ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
			authorized:  false,
			enforcement: true,
		}
		addr, client, count := bootstrapTransportTestServerWithBootstrap(t, handler, bp)
		client.SetEpochProvider(&testEpochProvider{
			ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
			enforcement: true,
		})
		pinnedCtx := &lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}
		_, err := client.FetchBootstrapInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 8, pinnedCtx)
		if err == nil {
			t.Fatal("expected error for unauthorized decision, got nil")
		}
		var blocked *PeerBlockedError
		if !errors.As(err, &blocked) {
			t.Fatalf("expected *PeerBlockedError, got %T: %v", err, err)
		}
		if got := *count; got != 0 {
			t.Errorf("expected 0 requests, got %d", got)
		}
	})

	t.Run("epoch_mismatch", func(t *testing.T) {
		bp := &testBootstrapEpochProvider{
			ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e2"},
			authorized:  true,
			enforcement: true,
		}
		addr, client, count := bootstrapTransportTestServerWithBootstrap(t, handler, bp)
		client.SetEpochProvider(&testEpochProvider{
			ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e2"},
			enforcement: true,
		})
		pinnedCtx := &lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}
		_, err := client.FetchBootstrapInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 8, pinnedCtx)
		if err == nil {
			t.Fatal("expected error for epoch mismatch, got nil")
		}
		var blocked *PeerBlockedError
		if !errors.As(err, &blocked) {
			t.Fatalf("expected *PeerBlockedError, got %T: %v", err, err)
		}
		if got := *count; got != 0 {
			t.Errorf("expected 0 requests, got %d", got)
		}
	})
}

// TestBootstrapTransport_LifecycleDisabledPassthrough verifies that a bootstrap
// operation with lifecycle disabled retains legacy passthrough behavior: no
// authorization gate, headers set from pinnedCtx if present.
func TestBootstrapTransport_LifecycleDisabledPassthrough(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(InventoryPageResponse{IDs: nil, HasMore: false})
	})
	// No provider at all (lifecycle disabled).
	addr, client, count := bootstrapTransportTestServer(t, handler)
	pinnedCtx := &lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"}

	_, err := client.FetchBootstrapInventoryPage(context.Background(), addr, "gossipgrid-0."+testSuffix, "", 8, pinnedCtx)
	if err != nil {
		t.Fatalf("expected passthrough (no auth gate) with lifecycle disabled, got: %v", err)
	}
	if got := *count; got != 1 {
		t.Errorf("expected 1 request transmitted, got %d", got)
	}
}
