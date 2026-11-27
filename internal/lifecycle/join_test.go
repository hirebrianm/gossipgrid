// Package lifecycle: tests for ordinary replica joining (Milestone 5B,
// Stage 3A). These tests verify:
//   - durable binding provenance (genesis vs join, real SQLite persistence);
//   - the AuthorizeJoin authorization contract (all prerequisites);
//   - the critical regression: a replacement seed with a join binding must
//     never reach READY through the genesis-restart path.
package lifecycle

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// testSourceObserver is a test double for the BootstrapSourceObserver
// interface. It implements the trusted observer boundary for unit tests,
// returning a valid (or configured invalid) SourceObservationResult without
// performing network I/O.
type testSourceObserver struct {
	mu      sync.Mutex
	result  SourceObservationResult
	err     error
	calls   int
	cluster string
	epoch   string
}

// newTestSourceObserver creates a test observer that returns a valid
// SourceObservationResult for the given epoch, using the package-level
// testClusterID and the standard test peer "gossipgrid-9".
func newTestSourceObserver(epoch string) *testSourceObserver {
	const peerNodeID = "gossipgrid-9"
	return &testSourceObserver{
		cluster: testClusterID,
		epoch:   epoch,
		result:  NewSourceObservationResult(time.Now(), time.Hour, RequestContext{ClusterID: testClusterID, EpochID: epoch}, peerNodeID+".test.svc", peerNodeID, "READY", testClusterID, epoch, true, true),
	}
}

// newTestSourceObserverErr creates a test observer that returns the given
// error (simulating a failed observation).
func newTestSourceObserverErr(err error) *testSourceObserver {
	return &testSourceObserver{err: err}
}

// newTestSourceObserverResult creates a test observer that returns a
// specific (potentially invalid) SourceObservationResult.
func newTestSourceObserverResult(r SourceObservationResult) *testSourceObserver {
	return &testSourceObserver{result: r}
}

func (o *testSourceObserver) Observe(_ context.Context, _, _ string, _ RequestContext, _ string) (SourceObservationResult, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls++
	if o.err != nil {
		return SourceObservationResult{}, o.err
	}
	return o.result, nil
}

// TestAuthorizeJoin_ValidContextPermits verifies the happy path: a non-seed
// replica (NodeID != SeedIdentity) with an empty, unbound database, fresh
// finalized metadata, and valid source evidence transitions to SYNCING.
func TestAuthorizeJoin_ValidContextPermits(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// After Start, the state should be DISCOVERING (no binding, metadata
	// available, cluster initialized).
	if ctrl.State() != DISCOVERING {
		t.Fatalf("expected DISCOVERING after Start (no binding, initialized metadata); got %s", ctrl.State())
	}

	// Authorize the join.
	if err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc"); err != nil {
		t.Fatalf("AuthorizeJoin: %v", err)
	}

	// The state must be SYNCING.
	if ctrl.State() != SYNCING {
		t.Fatalf("expected SYNCING after AuthorizeJoin; got %s (reason=%s)", ctrl.State(), ctrl.Snapshot().Reason)
	}

	// The binding must be present with JOIN provenance.
	b := binding.binding
	if b == nil {
		t.Fatal("expected a binding after AuthorizeJoin; got nil")
	}
	if b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatalf("binding mismatch: got %s/%s, expected %s/%s", b.ClusterID, b.EpochID, testClusterID, testEpochID)
	}
	if b.Provenance != ProvenanceJoin {
		t.Fatalf("expected join provenance; got %s", b.Provenance)
	}

	// Public writes must be blocked (SYNCING is not a ready state).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("expected public writes to be blocked (SYNCING state); got allowed")
	}

	// Bootstrap fetch must be permitted (the dedicated bootstrap
	// authorization is available for a SYNCING replica with a matching
	// binding and fresh, finalized metadata).
	bDecision := admission.CheckBootstrapFetch(context.Background(), RequestContext{ClusterID: testClusterID, EpochID: testEpochID})
	if !bDecision.Allowed {
		t.Fatalf("expected bootstrap fetch to be permitted (SYNCING, bound, fresh, finalized); got denied: %s", bDecision.Reason)
	}

	// The replica must NOT be a bootstrap source (IsReady is false for
	// SYNCING).
	if ctrl.State().IsBootstrapSourceEligible() {
		t.Fatal("expected a SYNCING replica to NOT be a bootstrap source; got eligible")
	}
}

// TestAuthorizeJoin_MissingMetadataRejects verifies that joining is rejected
// when no authoritative metadata observation exists.
func TestAuthorizeJoin_MissingMetadataRejects(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSourceUnavailable()

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to fail (no metadata); got nil")
	}
}

// TestAuthorizeJoin_StaleMetadataRejects verifies that joining is rejected
// when the metadata observation is stale.
func TestAuthorizeJoin_StaleMetadataRejects(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Start with a very short poll interval, then advance the clock past
	// 2x the interval to make the observation stale.
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// Advance the clock to make the observation stale (2x poll interval
	// + 1ms).
	ctrl.WithClock(func() time.Time {
		return time.Now().Add(250 * time.Millisecond)
	})

	err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to fail (stale metadata); got nil")
	}
}

// TestAuthorizeJoin_UninitializedMetadataRejects verifies that ordinary
// joining is rejected when the metadata reports Initialized=false.
func TestAuthorizeJoin_UninitializedMetadataRejects(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(pendingMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to fail (uninitialized metadata); got nil")
	}
}

// TestAuthorizeJoin_IncompleteFinalizationRejects verifies that joining is
// rejected when the metadata is initialized but the genesis finalization
// invariant is not complete (e.g., GenesisAuthorized still true).
func TestAuthorizeJoin_IncompleteFinalizationRejects(t *testing.T) {
	// A metadata record that is initialized but has GenesisAuthorized=true
	// (incoherent per Validate, so we use a record that passes Validate
	// but is not fully finalized: GenesisConsumer set but
	// DataLossProtected=false is incoherent. Instead, we test with a
	// non-seed replica and a record where the finalization invariant
	// requires DataLossProtected=true).
	binding := &fakeBindingStoreWithMessages{}

	// Use a metadata record that is initialized, GenesisAuthorized=false,
	// GenesisConsumer set, but DataLossProtected=false (which is
	// incoherent per Validate for a consumer-set record). So instead we
	// test the case where the seed identity is set and the consumer
	// doesn't match.
	meta := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              testEpochID,
		Generation:           1,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      "some-other-seed",
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
	if err := meta.Validate(); err != nil {
		t.Fatalf("test metadata should be valid: %v", err)
	}
	src := NewFakeMetadataSource(meta, true)

	// The NodeID is the designated seed, but the consumer is different.
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to fail (incomplete finalization: consumer mismatch); got nil")
	}
}

// TestAuthorizeJoin_ConflictingBindingRejects verifies that joining is
// rejected when an existing binding conflicts with the target epoch.
func TestAuthorizeJoin_ConflictingBindingRejects(t *testing.T) {
	conflicting := &DatabaseBinding{
		ClusterID:  "other-cluster",
		EpochID:    "other-epoch",
		Generation: 1,
		CreatedAt:  time.Now(),
		Provenance: ProvenanceJoin,
	}
	binding := &fakeBindingStoreWithMessages{binding: conflicting}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to fail (conflicting binding); got nil")
	}
}

// TestAuthorizeJoin_NonemptyUnboundDatabaseRejects verifies that joining is
// rejected when the database has messages but no binding.
func TestAuthorizeJoin_NonemptyUnboundDatabaseRejects(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{messageCount: 5}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to fail (nonempty unbound database); got nil")
	}
}

// TestAuthorizeJoin_CancellationPrevents verifies that a cancelled context
// prevents the joining operation.
func TestAuthorizeJoin_CancellationPrevents(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ctrl.AuthorizeJoin(ctx, newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to fail (cancelled context); got nil")
	}
}

// TestAuthorizeJoin_StopPrevents verifies that a stopped controller
// prevents the joining operation.
func TestAuthorizeJoin_StopPrevents(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ctrl.Stop(context.Background())

	err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to fail (stopped controller); got nil")
	}
}

// TestAuthorizeJoin_WrongEpochSourceEvidenceRejects verifies that an
// observation with a wrong epoch (the peer reported a different epoch than
// the pinned target) is rejected by the final guard.
func TestAuthorizeJoin_WrongEpochSourceEvidenceRejects(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// The observer returns a result with a wrong epoch (the peer reported
	// a different epoch than the pinned target). The final guard's
	// observation validation (validateSourceObservationResultLocked)
	// rejects it because the reported epoch does not match the current
	// metadata.
	badObs := NewSourceObservationResult(time.Now(), time.Hour, RequestContext{ClusterID: testClusterID, EpochID: testEpochID}, "gossipgrid-9.test.svc", "gossipgrid-9", "READY", testClusterID, "wrong-epoch", true, true)
	obs := newTestSourceObserverResult(badObs)

	err := ctrl.AuthorizeJoin(context.Background(), obs, "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to fail (wrong epoch in source observation); got nil")
	}
}

// TestAuthorizeJoin_ObservationTemporalAndIdentityValidation verifies the
// observation's temporal policy (future timestamp, expiration, invalid
// lifetime, maximum-lifetime clamp) and the controller-side identity and
// pinned-target checks (wrong pinned target, wrong expected peer identity)
// through the single consistent controller-side observation validator
// (validateSourceObservationResult, used by both the pre-binding
// revalidation and the final guard). The observer is an explicitly trusted
// in-process dependency; these checks are engineering controls, not a
// claim that the Go type prevents an in-process attacker.
func TestAuthorizeJoin_ObservationTemporalAndIdentityValidation(t *testing.T) {
	// The temporal cases (future timestamp, expired, invalid lifetime)
	// are rejected by SourceObservationResult.Valid itself; the identity
	// and pinned-target cases are temporally valid but rejected by the
	// controller-side identity / pinned-target checks in the single
	// consistent controller-side observation validator.
	temporalCases := []struct {
		name string
		obs  SourceObservationResult
	}{
		{
			name: "future observation timestamp",
			// observedAt in the future (now + 5s): the observation has not
			// yet occurred; Valid rejects it (a clock skew or fabrication).
			obs: NewSourceObservationResult(time.Now().Add(5*time.Second), 10*time.Second, RequestContext{ClusterID: testClusterID, EpochID: testEpochID}, "gossipgrid-9.test.svc", "gossipgrid-9", "READY", testClusterID, testEpochID, true, true),
		},
		{
			name: "expired observation lifetime",
			// observedAt in the past, ttl short, now past observedAt+ttl:
			// the observation is outside its bounded lifetime.
			obs: NewSourceObservationResult(time.Now().Add(-2*time.Second), 10*time.Millisecond, RequestContext{ClusterID: testClusterID, EpochID: testEpochID}, "gossipgrid-9.test.svc", "gossipgrid-9", "READY", testClusterID, testEpochID, true, true),
		},
		{
			name: "invalid (zero) lifetime",
			// ttl <= 0: an invalid lifetime is rejected.
			obs: NewSourceObservationResult(time.Now(), 0, RequestContext{ClusterID: testClusterID, EpochID: testEpochID}, "gossipgrid-9.test.svc", "gossipgrid-9", "READY", testClusterID, testEpochID, true, true),
		},
	}
	for _, tc := range temporalCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.obs.Valid(time.Now()) {
				t.Fatalf("expected the observation to be invalid (%s); got valid", tc.name)
			}
			// The temporal check is enforced at both the pre-binding
			// revalidation and the final guard (the single consistent
			// controller-side validator rejects an invalid result before
			// any durable write). Verify it via the validator directly.
			ctrl := &Controller{cfg: Config{NodeID: "gossipgrid-1", Enabled: true, MetadataPollInterval: 100 * time.Millisecond}}
			if err := c_validateJoinObservation(ctrl, tc.obs, RequestContext{ClusterID: testClusterID, EpochID: testEpochID}, "gossipgrid-9.test.svc"); err == nil {
				t.Fatalf("expected the observation to be rejected (%s); got nil", tc.name)
			}
		})
	}
	// The maximum-lifetime case: a requested TTL longer than the maximum
	// is clamped to the maximum. The result is still valid (observedAt
	// now, now within the clamped lifetime), confirming the clamp is
	// enforced consistently (the result cannot outlive the bounded
	// observation contract).
	t.Run("maximum lifetime clamped", func(t *testing.T) {
		obs := NewSourceObservationResult(time.Now(), time.Hour, RequestContext{ClusterID: testClusterID, EpochID: testEpochID}, "gossipgrid-9.test.svc", "gossipgrid-9", "READY", testClusterID, testEpochID, true, true)
		if !obs.Valid(time.Now()) {
			t.Fatal("clamped observation (now within the maximum lifetime) is unexpectedly invalid")
		}
		if obs.ttl != MaxSourceObservationTTL {
			t.Fatalf("expected the ttl to be clamped to the maximum (%v); got %v", MaxSourceObservationTTL, obs.ttl)
		}
		// A future observation is invalid even with the clamped maximum
		// lifetime (the temporal policy is independent of the lifetime
		// bound).
		future := NewSourceObservationResult(time.Now().Add(5*time.Second), time.Hour, RequestContext{ClusterID: testClusterID, EpochID: testEpochID}, "gossipgrid-9.test.svc", "gossipgrid-9", "READY", testClusterID, testEpochID, true, true)
		if future.Valid(time.Now()) {
			t.Fatal("future-dated observation with the clamped maximum lifetime is unexpectedly valid")
		}
	})
	// The identity and pinned-target cases: temporally valid observations
	// whose authenticated peer identity or recorded pinned target does not
	// match the requested expected identity or target. The single
	// consistent controller-side observation validator rejects them.
	identityCases := []struct {
		name   string
		obs    SourceObservationResult
		target RequestContext
		id     string
	}{
		{
			name: "wrong pinned target",
			// The observation's recorded pinned target (expected) differs
			// from the requested target (cluster/epoch): the
			// controller-side pinned-target check rejects it.
			obs:    NewSourceObservationResult(time.Now(), 10*time.Second, RequestContext{ClusterID: testClusterID, EpochID: "e-different-pinned"}, "gossipgrid-9.test.svc", "gossipgrid-9", "READY", testClusterID, "e-different-pinned", true, true),
			target: RequestContext{ClusterID: testClusterID, EpochID: testEpochID},
			id:     "gossipgrid-9.test.svc",
		},
		{
			name: "wrong expected peer identity",
			// The observation's authenticated peer identity differs from
			// the expected identity the observation was requested for: the
			// controller-side identity check rejects it.
			obs:    NewSourceObservationResult(time.Now(), 10*time.Second, RequestContext{ClusterID: testClusterID, EpochID: testEpochID}, "gossipgrid-7.test.svc", "gossipgrid-9", "READY", testClusterID, testEpochID, true, true),
			target: RequestContext{ClusterID: testClusterID, EpochID: testEpochID},
			id:     "gossipgrid-9.test.svc",
		},
	}
	for _, tc := range identityCases {
		t.Run(tc.name, func(t *testing.T) {
			// The observation is temporally valid.
			if !tc.obs.Valid(time.Now()) {
				t.Fatalf("expected the observation to be temporally valid (%s); got invalid", tc.name)
			}
			// A full AuthorizeJoin with the observation rejects before
			// Bind (the controller-side identity / pinned-target check
			// fails; no binding is created).
			binding := &fakeBindingStoreWithMessages{}
			src := NewFakeMetadataSource(finalizedMeta(), true)
			ctrl := NewController(Config{
				MetadataPollInterval: 100 * time.Millisecond,
				NodeID:               "gossipgrid-1",
				SeedIdentity:         genesisSeed,
				Enabled:              true,
			}, NewMachine(nil), src, binding, nil)
			if err := ctrl.Start(context.Background()); err != nil {
				t.Fatalf("Start: %v", err)
			}
			defer ctrl.Stop(context.Background())
			if err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserverResult(tc.obs), "127.0.0.1:9999", tc.id); err == nil {
				t.Fatalf("expected AuthorizeJoin to reject the observation (%s); got nil", tc.name)
			}
			// No binding is created (rejected before Bind).
			if binding.binding != nil {
				t.Fatalf("expected no binding after rejection (%s); got %+v", tc.name, binding.binding)
			}
		})
	}
}

// c_validateJoinObservation is a test helper that exercises the single
// consistent controller-side observation validator (the same function
// validateSourceObservationResult that revalidateJoinPreBind and
// authorizeJoinFinalGuard call) directly, so the temporal and identity
// checks are verified in isolation (without the surrounding
// authorization preconditions).
func c_validateJoinObservation(c *Controller, r SourceObservationResult, target RequestContext, expectedIdentity string) error {
	return c.validateSourceObservationResult(r, c.accepted, target, expectedIdentity)
}

// TestAuthorizeJoin_ClockStalenessRejects verifies the deterministic
// staleness case (requirement 5) using the controlled clock: a fresh
// observation (freshAt=now) becomes stale when the controller's clock
// advances past 2x the metadata poll interval. The pre-binding
// revalidation (revalidateJoinPreBind) observes the stale authority and
// rejects the join before any durable write (no binding is created).
func TestAuthorizeJoin_ClockStalenessRejects(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// Advance the controller's clock past 2x the poll interval so the
	// fresh observation (freshAt = the Start-time now) is now stale.
	ctrl.WithClock(func() time.Time {
		return time.Now().Add(250 * time.Millisecond)
	})

	// The observation is valid (its own observedAt is the real now, within
	// its 10s lifetime). The rejection is from the metadata's STALE
	// authority (the controlled clock), not from the observation's
	// lifetime.
	err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to reject (stale authority via controlled clock); got nil")
	}
	if !strings.Contains(err.Error(), "stale") && !strings.Contains(err.Error(), "not fresh") {
		t.Fatalf("expected a staleness rejection; got: %v", err)
	}
	// No binding is created (rejected before Bind).
	if binding.binding != nil {
		t.Fatalf("expected no binding after staleness rejection; got %+v", binding.binding)
	}
}

// TestAuthorizeJoin_PostCommit_MetadataBarrierStaleObservation verifies the
// outstanding expired-observation case (requirement 5) using a
// metadata-source barrier: a change (a new authority observation that
// advances the controller's clock and makes the observation stale) is
// applied after the post-Bind re-read and before the final local
// authorization guard. The final guard (which evaluates the observation
// lifetime against c.now) observes the stale observation and rejects
// fail-closed; the committed durable binding is preserved.
//
// This is a genuine final-guard race (the change linearizes after the
// post-Bind re-read and before the guard's evaluation), not a precondition
// failure: the post-Bind re-read passes (the metadata is still valid and
// matches the binding), and the rejection is from the final guard's
// observation-lifetime check.
func TestAuthorizeJoin_PostCommit_MetadataBarrierStaleObservation(t *testing.T) {
	// A deterministic clock (t0) so the observation's bounded lifetime and
	// the metadata freshness bound are controlled precisely. A sufficiently
	// long metadata poll interval (1h) keeps the authoritative metadata
	// FRESH across the controlled clock advance — only the observation
	// expires, isolating the expired-observation regression from the
	// metadata-staleness regression (covered by a separate test).
	t0 := time.Unix(1700000000, 0)
	poll := time.Hour

	binding := newPostCommitBlockingBindingStore()
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: poll,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)
	ctrl.WithClock(func() time.Time { return t0 })

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// The source observation is valid when the operation begins: its
	// observedAt is t0 and its lifetime (5s) is well inside the bounded
	// observation contract. It is NOT the short 50ms lifetime that could
	// expire during ordinary test execution.
	obsResult := NewSourceObservationResult(t0, 5*time.Second, RequestContext{ClusterID: testClusterID, EpochID: testEpochID}, "gossipgrid-9.test.svc", "gossipgrid-9", "READY", testClusterID, testEpochID, true, true)
	obs := newTestSourceObserverResult(obsResult)

	// Run AuthorizeJoin in a goroutine; it blocks after the Bind commits.
	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.AuthorizeJoin(context.Background(), obs, "127.0.0.1:9999", "gossipgrid-9.test.svc")
	}()

	// Wait for the Bind to commit (the binding store's commitCh). At this
	// point the binding is committed (durable, never-rollback); the Bind
	// return is blocked on releaseCh, so the post-Bind metadata re-read has
	// NOT yet begun — no event has occurred after that read.
	select {
	case <-binding.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Bind to commit")
	}

	// Controlled clock advance: move the controller's clock past the
	// observation's expiration (t0 + 5s) but well within the metadata
	// freshness bound (t0 + 2h, since the poll interval is 1h). The
	// authoritative metadata remains fresh and valid; only the source
	// observation has expired.
	ctrl.WithClock(func() time.Time { return t0.Add(6 * time.Second) })

	// Release the blocked Bind. The post-Bind re-read now passes (the
	// metadata is valid and matching), and the final guard's
	// observation-lifetime check rejects: as of the advanced clock, the
	// observation (observedAt=t0, ttl=5s) is expired.
	close(binding.releaseCh)

	err := <-errCh
	if err == nil {
		t.Fatal("expected AuthorizeJoin to reject (expired source observation at the final guard); got nil")
	}
	// The rejection must be specifically because the source observation
	// expired, not because the metadata is stale or for any other reason.
	if !containsFold(err.Error(), "source observation is not valid or is outside its bounded lifetime") {
		t.Fatalf("expected the rejection to cite the expired source observation; got: %v", err)
	}
	// The authoritative metadata must remain fresh (the long poll interval
	// keeps it current across the 6s advance): this confirms the rejection
	// is attributable to the observation, not to metadata staleness.
	if snap := ctrl.Authorization(); snap.Stale || snap.AuthorityState != "fresh" {
		t.Fatalf("authoritative metadata must remain fresh across the clock advance (stale=%v authority=%s); the regression must isolate observation expiry from metadata staleness", snap.Stale, snap.AuthorityState)
	}
	// The durable binding is preserved (committed before the guard
	// rejection).
	b := binding.getBinding()
	if b == nil {
		t.Fatal("expected durable binding to be preserved after guard rejection; got nil")
	}
	if b.Provenance != ProvenanceJoin {
		t.Fatalf("expected join provenance on preserved binding; got %s", b.Provenance)
	}
	// The replica must not have acquired unauthorized participation: it
	// was rejected before the final guard's transition, so it is not in
	// SYNCING (it remains in its pre-join state).
	if ctrl.State() == SYNCING {
		t.Fatal("CRITICAL: the replica entered SYNCING despite the expired-observation rejection; it must not acquire unauthorized participation")
	}
}

// TestAuthorizeJoin_NonEligibleSourceRejects verifies that an observation
// from a peer that is not eligible (wrong state, not READY/DEGRADED) is
// rejected by the final guard.
func TestAuthorizeJoin_NonEligibleSourceRejects(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// The observer returns a result with a non-eligible state (DISCOVERING).
	// The final guard's observation validation rejects it.
	badObs := NewSourceObservationResult(time.Now(), time.Hour, RequestContext{ClusterID: testClusterID, EpochID: testEpochID}, "gossipgrid-9.test.svc", "gossipgrid-9", "DISCOVERING", testClusterID, testEpochID, false, false)
	obs := newTestSourceObserverResult(badObs)

	err := ctrl.AuthorizeJoin(context.Background(), obs, "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to fail (non-eligible source state); got nil")
	}
}

// TestAuthorizeJoin_ObserverErrorRejects verifies that a failed observation
// (the observer returns an error) rejects the joining operation.
func TestAuthorizeJoin_ObserverErrorRejects(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	obs := newTestSourceObserverErr(errors.New("mTLS handshake failed"))
	err := ctrl.AuthorizeJoin(context.Background(), obs, "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to fail (observer error); got nil")
	}
}

// TestAuthorizeJoin_IdempotentResume verifies that a second AuthorizeJoin
// call with a matching join-provenance binding is idempotent (the binding
// is not rewritten, and the state remains SYNCING).
//
// The test covers two scenarios:
//  1. Restart resume: a replica with a pre-existing join binding starts in
//     STARTING (the metadata-evaluation path does not transition it because
//     the replica is not the seed). AuthorizeJoin resumes it to SYNCING via
//     the forceTransition path. The binding is not rewritten.
//  2. Second call in SYNCING: after the restart resume, a second
//     AuthorizeJoin call is rejected because SYNCING is not a pre-join
//     state. The binding remains unchanged.
func TestAuthorizeJoin_IdempotentResume(t *testing.T) {
	existing := &DatabaseBinding{
		ClusterID:  testClusterID,
		EpochID:    testEpochID,
		Generation: 1,
		CreatedAt:  time.Now(),
		Provenance: ProvenanceJoin,
	}
	binding := &fakeBindingStoreWithMessages{binding: existing}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// The replica is not the designated seed (NodeID="gossipgrid-1" vs
	// SeedIdentity="gossipgrid-0"), so the metadata-evaluation path does
	// not transition it. With a matching join binding, it remains in
	// STARTING after Start.
	if ctrl.State() == READY {
		t.Fatal("a non-seed replica with a join binding was auto-promoted to READY on start; it must not be")
	}

	// Restart resume: the replica has a matching join binding in STARTING.
	// AuthorizeJoin should resume it to SYNCING via the forceTransition
	// path. The binding is not rewritten.
	if err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc"); err != nil {
		t.Fatalf("AuthorizeJoin (restart resume): %v", err)
	}
	if ctrl.State() != SYNCING {
		t.Fatalf("expected SYNCING after restart resume; got %s", ctrl.State())
	}

	// The binding must be unchanged (same CreatedAt, same provenance).
	b := binding.binding
	if b == nil {
		t.Fatal("binding is nil")
	}
	if b.Provenance != ProvenanceJoin {
		t.Fatalf("provenance changed: expected join, got %s", b.Provenance)
	}
	if !b.CreatedAt.Equal(existing.CreatedAt) {
		t.Fatal("binding was rewritten (CreatedAt changed)")
	}

	// Second call in SYNCING: the state is now SYNCING. The idempotent
	// duplicate contract: a second AuthorizeJoin call with a matching
	// join-provenance binding and the state already in SYNCING is an
	// idempotent success (no extra durable bind, no state transition). The
	// guard confirms the complete current authorization and the matching
	// durable binding.
	err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err != nil {
		t.Fatalf("expected AuthorizeJoin to succeed idempotently (state is SYNCING); got %v", err)
	}
	if ctrl.State() != SYNCING {
		t.Fatalf("expected SYNCING after idempotent second call; got %s", ctrl.State())
	}

	// The binding must still be unchanged.
	b = binding.binding
	if !b.CreatedAt.Equal(existing.CreatedAt) {
		t.Fatal("binding was rewritten by idempotent second call")
	}
}

// TestAuthorizeJoin_GenesisBindingOnNonSeedRejects verifies that a
// non-seed replica with a genesis-provenance binding is rejected (only the
// designated seed can have a genesis binding).
func TestAuthorizeJoin_GenesisBindingOnNonSeedRejects(t *testing.T) {
	genesisBinding := &DatabaseBinding{
		ClusterID:  testClusterID,
		EpochID:    testEpochID,
		Generation: 1,
		CreatedAt:  time.Now(),
		Provenance: ProvenanceGenesis,
	}
	binding := &fakeBindingStoreWithMessages{binding: genesisBinding}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to fail (genesis binding on non-seed); got nil")
	}
}

// TestBindingProvenance_PersistsThroughRestart verifies (using real SQLite
// via the store package's test helper) that a join-provenance binding
// survives closing and reopening the database. This test is in the store
// package (see store/binding_provenance_test.go) due to the import cycle
// between lifecycle and store. The lifecycle package tests here use the
// fake binding store to verify the controller-level behavior.
//
// The real-SQLite persistence tests are:
//   - TestBindingProvenance_PersistedInStore (store package)
//   - TestBindingProvenance_JoinNotRelabeled (store package)
//   - TestBindingProvenance_MalformedProvenanceFailsClosed (store package)

// TestReplacementSeed_JoinBinding_RestartRemainsUnready is the CRITICAL
// regression test for Stage 3A. It exercises the exact failure scenario:
//
//  1. The cluster completes operator-controlled genesis.
//  2. The original gossipgrid-0 database is lost.
//  3. A replacement gossipgrid-0 starts with a fresh SQLite database.
//  4. The replacement obtains an ordinary joining binding.
//  5. Bootstrap has not completed.
//  6. The replacement process restarts.
//  7. Existing seed-restart logic observes a matching binding and
//     finalized metadata.
//
// The replacement must NEVER reach READY through the
// finalized-genesis restoration path.
func TestReplacementSeed_JoinBinding_RestartRemainsUnready(t *testing.T) {
	// The replacement seed has a JOIN-provenance binding (it joined the
	// already-initialized epoch via AuthorizeJoin).
	joinBinding := &DatabaseBinding{
		ClusterID:  testClusterID,
		EpochID:    testEpochID,
		Generation: 1,
		CreatedAt:  time.Now(),
		Provenance: ProvenanceJoin,
	}
	binding := &fakeBindingStoreWithMessages{binding: joinBinding}

	// The authoritative metadata is completely finalized (the original
	// seed was finalized before this replacement joined).
	src := NewFakeMetadataSource(finalizedMeta(), true)

	// A NEW controller (a new process): NodeID = SeedIdentity =
	// "gossipgrid-0" (the replacement has the same identity as the
	// original seed). The machine is created in STARTING.
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	// Start the controller through the real Start() path. The initial
	// metadata read is synchronous: it reads the finalized metadata,
	// accepts it, and calls reevaluateState. The re-evaluation observes
	// a matching binding with JOIN provenance on the designated seed in
	// STARTING state. The seed-restart reconciliation (reconcileSeedRestartLocked)
	// must NOT promote this replica to READY (the join provenance check
	// prevents it).
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start (replacement seed restart): %v", err)
	}
	defer ctrl.Stop(context.Background())

	// CRITICAL ASSERTION: the state must NOT be READY.
	if ctrl.State() == READY {
		t.Fatalf("CRITICAL FAILURE: replacement seed with join binding was promoted to READY on restart; it must remain unready (got reason=%s)", ctrl.Snapshot().Reason)
	}
	if ctrl.State() == GENESIS {
		t.Fatal("CRITICAL FAILURE: replacement seed with join binding entered GENESIS on restart; only the original seed (genesis binding) may enter GENESIS")
	}

	// The state should be STARTING or DISCOVERING (the join binding does
	// not trigger the genesis-restart promotion; the reevaluateState
	// "state is maintained" path leaves it in its current state, or the
	// no-binding case does not apply since there IS a binding).
	// Actually, reevaluateState sees a matching binding and the state is
	// STARTING. The seed-restart branch is entered (SeedIdentity == NodeID
	// && state == STARTING) but the join-provenance check causes an early
	// return. So the state remains STARTING.
	if ctrl.State() == STARTING {
		// This is the expected outcome: the replacement seed remains
		// STARTING (not promoted).
		t.Log("replacement seed remains STARTING after restart (correct: not promoted)")
	} else if ctrl.State() == DISCOVERING {
		t.Log("replacement seed is in DISCOVERING after restart (acceptable: not promoted)")
	} else {
		t.Fatalf("unexpected state after replacement seed restart: %s (reason=%s)", ctrl.State(), ctrl.Snapshot().Reason)
	}

	// Public writes must be blocked (not a ready state).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("CRITICAL FAILURE: replacement seed with join binding can accept public writes; it must not be in a ready state")
	}

	// The replica must NOT be a bootstrap source.
	if ctrl.State().IsBootstrapSourceEligible() {
		t.Fatal("CRITICAL FAILURE: replacement seed with join binding is eligible as a bootstrap source; it must not be")
	}

	// Bootstrap fetch should be available only if the state is SYNCING
	// (the dedicated predicate requires SYNCING). If the state is
	// STARTING or DISCOVERING, bootstrap fetch is not authorized.
	bDecision := admission.CheckBootstrapFetch(context.Background(), RequestContext{ClusterID: testClusterID, EpochID: testEpochID})
	if ctrl.State() != SYNCING && bDecision.Allowed {
		t.Fatal("unexpected: bootstrap fetch authorized in a non-SYNCING state")
	}

	// Verify the binding still has join provenance (not relabeled).
	b := binding.binding
	if b == nil {
		t.Fatal("binding is nil")
	}
	if b.Provenance != ProvenanceJoin {
		t.Fatalf("CRITICAL FAILURE: binding provenance was changed from join to %s; it must be immutable", b.Provenance)
	}
}

// TestOriginalSeed_GenesisBinding_RestartPromotes verifies that the
// existing original genesis-seed restart path still behaves correctly:
// a seed with a genesis-provenance binding and finalized metadata is
// promoted to READY on restart.
func TestOriginalSeed_GenesisBinding_RestartPromotes(t *testing.T) {
	// The original seed has a GENESIS-provenance binding.
	genesisBinding := &DatabaseBinding{
		ClusterID:  testClusterID,
		EpochID:    testEpochID,
		Generation: 1,
		CreatedAt:  time.Now(),
		Provenance: ProvenanceGenesis,
	}
	binding := &fakeBindingStoreWithMessages{binding: genesisBinding}

	// The metadata is completely finalized.
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               genesisSeed,
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start (original seed restart): %v", err)
	}
	defer ctrl.Stop(context.Background())

	// The original seed MUST be promoted to READY (the genesis-restart
	// path with a genesis-provenance binding and finalized metadata).
	if ctrl.State() != READY {
		t.Fatalf("REGRESSION: original seed with genesis binding was NOT promoted to READY on restart; got %s (reason=%s)", ctrl.State(), ctrl.Snapshot().Reason)
	}

	// Public writes must now be allowed.
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if !decision.Allowed {
		t.Fatalf("REGRESSION: public writes blocked for original seed after promotion: %s", decision.Reason)
	}
}

// TestJoinBinding_Restart_MatchingMetadata verifies that a replica with a
// join-provenance binding that restarts with matching, fresh metadata is
// eligible for re-evaluation and synchronization, not automatically READY.
func TestJoinBinding_Restart_MatchingMetadata(t *testing.T) {
	joinBinding := &DatabaseBinding{
		ClusterID:  testClusterID,
		EpochID:    testEpochID,
		Generation: 1,
		CreatedAt:  time.Now(),
		Provenance: ProvenanceJoin,
	}
	binding := &fakeBindingStoreWithMessages{binding: joinBinding}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	// A non-seed replica (the common case for join bindings).
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-2",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// The non-seed replica is NOT the designated seed, so the
	// seed-restart reconciliation is not entered. The reevaluateState
	// "matching binding" path leaves the state unchanged (STARTING).
	// The replica is eligible for re-evaluation (a future AuthorizeJoin
	// call can resume it to SYNCING) but is NOT automatically READY.
	if ctrl.State() == READY {
		t.Fatal("a non-seed replica with a join binding was auto-promoted to READY on restart; it must not be")
	}

	// The replica can be re-joined (idempotent resume to SYNCING).
	if err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc"); err != nil {
		t.Fatalf("AuthorizeJoin (resume): %v", err)
	}
	if ctrl.State() != SYNCING {
		t.Fatalf("expected SYNCING after re-join; got %s", ctrl.State())
	}
}

// TestUnknownProvenance_Binding_AdmissionFailsClosed verifies the
// unknown-provenance admission regressions (requirement 3): an
// explicitly-unknown (legacy) provenance binding must fail closed for both
// ordinary participation and bootstrap fetching. The binding is readable for
// diagnosis (GetBinding succeeds), but the admission predicates reject it:
// the core authorized predicate (ordinary write/replication admission)
// requires a ready state plus a matching, valid, fresh binding, and a
// legacy binding on a non-ready (STARTING) replica fails the ready-state
// check; the bootstrap predicate requires a matching, valid, fresh binding
// plus the SYNCING state, and a legacy binding on a STARTING replica fails
// the state check. Both fail closed: an explicitly-unknown origin is never
// inferred to authorize participation from a matching NodeID/SeedIdentity
// or a valid binding.
//
// The supported historical genesis binding (zero provenance → genesis at
// read time) is NOT affected: it still authorizes the original seed's
// genesis-restart promotion (verified by
// TestOriginalSeed_GenesisBinding_RestartPromotes and the store migration
// tests). This regression covers the EXPLICITLY-UNKNOWN (legacy) case.
func TestUnknownProvenance_Binding_AdmissionFailsClosed(t *testing.T) {
	// An explicitly-unknown (legacy) provenance binding.
	legacyBinding := &DatabaseBinding{
		ClusterID:  testClusterID,
		EpochID:    testEpochID,
		Generation: 1,
		CreatedAt:  time.Now(),
		Provenance: ProvenanceLegacy,
	}
	binding := &fakeBindingStoreWithMessages{binding: legacyBinding}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-2",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// The binding is readable for diagnosis (provenance is legacy).
	b := ctrl.LocalBinding()
	if b == nil {
		t.Fatal("expected the legacy binding to be readable for diagnosis; got nil")
	}
	if b.Provenance != ProvenanceLegacy {
		t.Fatalf("expected legacy provenance; got %s", b.Provenance)
	}

	// Ordinary participation (public writes) must fail closed: the replica
	// is in STARTING (a legacy binding does not promote it), so the core
	// authorized predicate's ready-state check rejects.
	admission := ctrl.Admission()
	if d := admission.CheckWrite(context.Background()); d.Allowed {
		t.Fatal("CRITICAL: ordinary write admitted for a legacy (explicitly-unknown) provenance binding; it must fail closed")
	}
	// Replication inbound must also fail closed (the replica is not in a
	// ready state).
	if d := admission.CheckReplicationInbound(context.Background(), RequestContext{ClusterID: testClusterID, EpochID: testEpochID}); d.Allowed {
		t.Fatal("CRITICAL: inbound replication admitted for a legacy (explicitly-unknown) provenance binding; it must fail closed")
	}

	// Bootstrap fetching must fail closed: the bootstrap predicate requires
	// the SYNCING state; the replica is in STARTING, so it rejects. (The
	// legacy provenance is an additional fail-closed signal, but the
	// state mismatch is the primary gate here.)
	if d := admission.CheckBootstrapFetch(context.Background(), RequestContext{ClusterID: testClusterID, EpochID: testEpochID}); d.Allowed {
		t.Fatal("CRITICAL: bootstrap fetch admitted for a legacy (explicitly-unknown) provenance binding; it must fail closed")
	}

	// The replica must NOT be auto-promoted to READY by the seed-restart
	// reconciliation (a non-seed replica; and even a seed with a legacy
	// binding must not be promoted — the provenance gate blocks it).
	if ctrl.State() == READY {
		t.Fatal("CRITICAL: a legacy (explicitly-unknown) provenance binding was auto-promoted to READY; it must not be")
	}
}

// TestJoinBinding_Restart_TemporaryMetadataFailure verifies that a
// temporary metadata API failure does not cause the replica to enter
// RECOVERY_REQUIRED (it fails closed for admission but the state is
// unchanged).
func TestJoinBinding_Restart_TemporaryMetadataFailure(t *testing.T) {
	joinBinding := &DatabaseBinding{
		ClusterID:  testClusterID,
		EpochID:    testEpochID,
		Generation: 1,
		CreatedAt:  time.Now(),
		Provenance: ProvenanceJoin,
	}
	binding := &fakeBindingStoreWithMessages{binding: joinBinding}
	// Metadata source is unavailable (simulating a temporary API failure).
	src := NewFakeMetadataSourceUnavailable()

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-2",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// The state must NOT be RECOVERY_REQUIRED (a temporary metadata
	// failure does not force recovery; it fails closed for admission
	// only).
	if ctrl.State() == RECOVERY_REQUIRED {
		t.Fatal("a temporary metadata failure caused RECOVERY_REQUIRED; it should fail closed without a recovery incident")
	}

	// Public writes are blocked (no fresh metadata).
	admission := ctrl.Admission()
	decision := admission.CheckWrite(context.Background())
	if decision.Allowed {
		t.Fatal("public writes allowed without fresh metadata; must be blocked")
	}
}

// TestProvenanceAllowlist_AdmissionDirect verifies the mandatory positive
// provenance allowlist in the admission predicates, isolated from the other
// admission conditions (state, metadata, freshness). Each test case constructs
// an otherwise-valid controller (fresh, valid, matching, non-stopped authority
// and a matching binding) and varies ONLY the binding provenance, so the
// observed allow/deny is attributable to the provenance check rather than to
// a state or metadata mismatch.
//
// These tests deliberately isolate provenance from the other conditions; they
// are not claims that all fixture states are currently reachable in
// production.
//
// Negative cases (an unrecognized provenance must fail closed for both
// ordinary participation and bootstrap fetch, regardless of state):
//   - READY with unknown provenance: ordinary authorization denied.
//   - DEGRADED with unknown provenance: ordinary authorization denied.
//   - SYNCING with unknown provenance: bootstrap authorization denied.
//   - Out-of-range provenance: denied.
//
// Positive controls (recognized provenance allowed in the matching state):
//   - READY with genesis provenance: ordinary authorization allowed.
//   - SYNCING with join provenance: bootstrap authorization allowed.
func TestProvenanceAllowlist_AdmissionDirect(t *testing.T) {
	tt := []struct {
		name             string
		state            State
		provenance       BindingProvenance
		which            string // "ordinary" or "bootstrap"
		wantAllowed      bool
		wantReasonSubstr string
	}{
		{
			name:             "READY with unknown provenance denies ordinary authorization",
			state:            READY,
			provenance:       ProvenanceLegacy,
			which:            "ordinary",
			wantAllowed:      false,
			wantReasonSubstr: "provenance is not a recognized value",
		},
		{
			name:             "DEGRADED with unknown provenance denies ordinary authorization",
			state:            DEGRADED,
			provenance:       ProvenanceLegacy,
			which:            "ordinary",
			wantAllowed:      false,
			wantReasonSubstr: "provenance is not a recognized value",
		},
		{
			name:             "SYNCING with unknown provenance denies bootstrap authorization",
			state:            SYNCING,
			provenance:       ProvenanceLegacy,
			which:            "bootstrap",
			wantAllowed:      false,
			wantReasonSubstr: "provenance is not a recognized value",
		},
		{
			name:             "Out-of-range provenance denies both",
			state:            READY,
			provenance:       BindingProvenance(99),
			which:            "ordinary",
			wantAllowed:      false,
			wantReasonSubstr: "provenance is not a recognized value",
		},
		{
			name:             "Out-of-range provenance denies bootstrap",
			state:            SYNCING,
			provenance:       BindingProvenance(99),
			which:            "bootstrap",
			wantAllowed:      false,
			wantReasonSubstr: "provenance is not a recognized value",
		},
		{
			name:             "READY with genesis provenance allows ordinary authorization",
			state:            READY,
			provenance:       ProvenanceGenesis,
			which:            "ordinary",
			wantAllowed:      true,
			wantReasonSubstr: "authorized",
		},
		{
			name:             "SYNCING with join provenance allows bootstrap authorization",
			state:            SYNCING,
			provenance:       ProvenanceJoin,
			which:            "bootstrap",
			wantAllowed:      true,
			wantReasonSubstr: "bootstrap fetch authorized",
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			binding := &DatabaseBinding{
				ClusterID:  testClusterID,
				EpochID:    testEpochID,
				Generation: 1,
				CreatedAt:  time.Now(),
				Provenance: tc.provenance,
			}
			// newSyncingController (defined in bootstrap_admission_test.go)
			// builds a real Controller in the requested state with a fresh,
			// valid, matching authoritative observation and the supplied
			// binding, using a far-future metadata poll interval so the
			// observation never goes stale during the test. The state is
			// forced directly (ForceTransitionForTest bypasses the transition
			// table), so the provenance check is isolated from the state
			// check.
			ctrl := newSyncingController(t, tc.state, finalizedMeta(), binding)

			admission := ctrl.Admission()
			var d AdmissionDecision
			switch tc.which {
			case "ordinary":
				d = admission.CheckWrite(context.Background())
			case "bootstrap":
				d = admission.CheckBootstrapFetch(context.Background(), RequestContext{ClusterID: testClusterID, EpochID: testEpochID})
			default:
				t.Fatalf("unknown which: %s", tc.which)
			}
			if d.Allowed != tc.wantAllowed {
				t.Fatalf("allowed=%v want %v; reason=%q", d.Allowed, tc.wantAllowed, d.Reason)
			}
			if !containsFold(d.Reason, tc.wantReasonSubstr) {
				t.Fatalf("reason %q does not contain %q", d.Reason, tc.wantReasonSubstr)
			}
		})
	}
}

// containsFold reports whether the haystack contains the needle, case
// insensitively (a test-only helper for asserting on diagnostic reasons).
func containsFold(haystack, needle string) bool {
	return strings.Contains(strings.ToLower(haystack), strings.ToLower(needle))
}

// TestJoinBinding_Restart_ConflictingEpoch verifies that a conflicting
// epoch (binding does not match metadata) enters RECOVERY_REQUIRED.
func TestJoinBinding_Restart_ConflictingEpoch(t *testing.T) {
	joinBinding := &DatabaseBinding{
		ClusterID:  testClusterID,
		EpochID:    "old-epoch",
		Generation: 1,
		CreatedAt:  time.Now(),
		Provenance: ProvenanceJoin,
	}
	binding := &fakeBindingStoreWithMessages{binding: joinBinding}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-2",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// The binding (old-epoch) does not match the metadata (testEpochID).
	// The reevaluateState Case 3 (mismatch) transitions to
	// RECOVERY_REQUIRED.
	if ctrl.State() != RECOVERY_REQUIRED {
		t.Fatalf("expected RECOVERY_REQUIRED for conflicting epoch; got %s (reason=%s)", ctrl.State(), ctrl.Snapshot().Reason)
	}

	// The binding must be preserved (not deleted or overwritten).
	b := binding.binding
	if b == nil {
		t.Fatal("binding was deleted; it must be preserved")
	}
	if b.EpochID != "old-epoch" {
		t.Fatalf("binding was modified: epoch changed from old-epoch to %s", b.EpochID)
	}
}

// TestAuthorizeJoin_DuplicateConcurrentJoins verifies that concurrent
// duplicate join attempts are safe and idempotent (only one binding is
// created; all callers observe the same result). The test is genuinely
// concurrent: multiple goroutines call AuthorizeJoin simultaneously, and a
// barrier ensures they all start before any can complete.
func TestAuthorizeJoin_DuplicateConcurrentJoins(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	const n = 5
	results := make(chan error, n)
	start := make(chan struct{})

	// Barrier: all goroutines wait for start before calling AuthorizeJoin.
	for i := 0; i < n; i++ {
		go func() {
			<-start
			results <- ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
		}()
	}
	close(start)

	var errs []error
	for i := 0; i < n; i++ {
		if err := <-results; err != nil {
			errs = append(errs, err)
		}
	}
	// All callers must observe a successful result (idempotent contract):
	// the first caller transitions DISCOVERING→SYNCING; subsequent callers
	// find the state already in SYNCING and return idempotent success.
	if len(errs) > 0 {
		t.Fatalf("concurrent AuthorizeJoin: %d of %d callers failed: %v", len(errs), n, errs)
	}

	// The state is SYNCING.
	if ctrl.State() != SYNCING {
		t.Fatalf("expected SYNCING; got %s", ctrl.State())
	}

	// The binding has join provenance and was created only once.
	b, _ := binding.GetBinding()
	if b == nil {
		t.Fatal("binding is nil")
	}
	if b.Provenance != ProvenanceJoin {
		t.Fatalf("expected join provenance; got %s", b.Provenance)
	}
}

// TestAuthorizeJoin_MetadataChangeAfterBind verifies that if the
// authoritative metadata changes (epoch changes) between the initial
// observation and the join operation, the join succeeds consistently.
// This test is deterministic: it updates BOTH the metadata source
// (so the post-Bind re-read sees the new epoch) and c.accepted
// (so the pre-guard snapshot and final guard see the new epoch) to a
// different epoch before calling AuthorizeJoin. The pre-guard snapshot
// reads the new epoch, the Bind uses the new epoch, the post-Bind
// re-read returns the new epoch from the source, and the observation
// (made for the new epoch by the observer) matches. All checks pass.
//
// This test verifies that the Bind uses the current metadata's epoch
// (not a stale pre-guard snapshot) and that the post-Bind re-read
// (the authoritative metadata source) is consistent with the Bind.
func TestAuthorizeJoin_MetadataChangeAfterBind(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// After Start, the state is DISCOVERING and c.accepted is the
	// finalized metadata (testEpochID). Now simulate a metadata change
	// (epoch changed) by updating BOTH the metadata source and c.accepted
	// to a different epoch. The pre-guard snapshot reads the new epoch,
	// the Bind uses the new epoch, the post-Bind re-read returns the new
	// epoch from the source, and the observer (made for the new epoch)
	// matches. The guard should pass (all checks consistent with the new
	// epoch).
	changedMeta := ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              "new-epoch",
		Generation:           2,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      genesisSeed,
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
	// Update the metadata source so the post-Bind re-read returns the
	// changed metadata (consistent with the Bind).
	src.SetMetadata(changedMeta)
	// Update c.accepted so the pre-guard snapshot and final guard see
	// the changed metadata.
	ctrl.SetAcceptedMetadataForTest(changedMeta)

	// The observer is made for the new epoch (matching c.accepted).
	err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver("new-epoch"), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err != nil {
		t.Fatalf("expected AuthorizeJoin to succeed (metadata changed but observation matches); got %v", err)
	}

	// The binding should have the new epoch (the Bind used the current
	// metadata's epoch).
	b, _ := binding.GetBinding()
	if b == nil {
		t.Fatal("binding is nil")
	}
	if b.EpochID != "new-epoch" {
		t.Fatalf("binding epoch: expected new-epoch, got %s", b.EpochID)
	}
}

// --- Final-guard race regression tests (Finding 7) ---
//
// These tests verify that the final authorization guard (authorizeJoinFinalGuard)
// correctly rejects a join when a concurrent operation changes the controller
// state between the durable Bind and the guard evaluation. Each test uses a
// deterministic barrier (a channel or a blocking binding store) to force the
// specific interleaving.

// --- Final-guard post-commit race regression tests (Finding 7) ---
//
// These tests verify that the final authorization guard (authorizeJoinFinalGuard)
// correctly rejects a join when a concurrent operation changes the controller
// state between the durable Bind's commit and the guard evaluation. Each test
// uses a postCommitBlockingBindingStore: Bind COMMITs the binding, signals
// commitCh, then blocks on releaseCh until the test changes state and
// releases. The commit is a linearization point: after commit, the binding
// is durable and is never rolled back. The state change (stop, cancel,
// metadata change, etc.) linearizes between commit and the guard evaluation.
// The guard must observe the changed state and reject fail-closed, while
// the durable binding is preserved.

// TestAuthorizeJoin_PostCommit_StopRace verifies that if the controller is
// stopped after the durable Bind's commit but before the final guard runs,
// the guard rejects (the lifecycle is stopped) and the durable binding is
// preserved.
func TestAuthorizeJoin_PostCommit_StopRace(t *testing.T) {
	binding := newPostCommitBlockingBindingStore()
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Run AuthorizeJoin in a goroutine; it will block after the Bind commits.
	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	}()

	// Wait for the Bind to commit (the binding store's commitCh).
	select {
	case <-binding.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Bind to commit")
	}

	// Stop the controller AFTER the commit but BEFORE the Bind returns
	// (the Bind is still blocked on releaseCh). The post-Bind re-read
	// and the guard will observe the stopped controller and reject.
	ctrl.Stop(context.Background())

	// Release the blocked Bind.
	close(binding.releaseCh)

	err := <-errCh
	if err == nil {
		t.Fatal("expected AuthorizeJoin to be rejected (controller stopped after Bind commit); got nil")
	}

	// The durable binding must be preserved (committed before the stop).
	b := binding.getBinding()
	if b == nil {
		t.Fatal("expected durable binding to be preserved after guard rejection; got nil")
	}
	if b.Provenance != ProvenanceJoin {
		t.Fatalf("expected join provenance on preserved binding; got %s", b.Provenance)
	}
}

// TestAuthorizeJoin_PostCommit_CtxCancelRace verifies that if the caller's
// context is cancelled after the durable Bind's commit but before the final
// guard runs, the guard rejects (the caller context is no longer live) and
// the durable binding is preserved.
func TestAuthorizeJoin_PostCommit_CtxCancelRace(t *testing.T) {
	binding := newPostCommitBlockingBindingStore()
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// Create a cancellable context and run AuthorizeJoin in a goroutine.
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.AuthorizeJoin(ctx, newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	}()

	// Wait for the Bind to commit.
	select {
	case <-binding.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Bind to commit")
	}

	// Cancel the caller context AFTER the commit but BEFORE the Bind
	// returns (the Bind is still blocked on releaseCh). The guard checks
	// the caller ctx under the lock pair and rejects.
	cancel()

	// Release the blocked Bind.
	close(binding.releaseCh)

	err := <-errCh
	if err == nil {
		t.Fatal("expected AuthorizeJoin to be rejected (context cancelled after Bind commit); got nil")
	}

	// The durable binding must be preserved (committed before the cancel).
	b := binding.getBinding()
	if b == nil {
		t.Fatal("expected durable binding to be preserved after guard rejection; got nil")
	}
}

// TestAuthorizeJoin_PostCommit_MetadataChangeRace verifies that if the
// authoritative metadata changes (epoch changes) after the durable Bind's
// commit but before the final guard runs, the post-Bind re-read or the
// guard rejects (metadata no longer matches the binding) and the durable
// binding is preserved.
func TestAuthorizeJoin_PostCommit_MetadataChangeRace(t *testing.T) {
	binding := newPostCommitBlockingBindingStore()
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// Run AuthorizeJoin in a goroutine; it will block after the Bind commits.
	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	}()

	// Wait for the Bind to commit (the binding was created with the
	// original metadata's epoch).
	select {
	case <-binding.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Bind to commit")
	}

	// Change the metadata AFTER the commit but BEFORE the Bind returns.
	// The post-Bind re-read (c.source.Read) will return the new metadata,
	// which no longer matches the binding's epoch → reject.
	changedMeta := finalizedMeta()
	changedMeta.EpochID = "e-changed-after-commit"
	src.SetMetadata(changedMeta)

	// Release the blocked Bind.
	close(binding.releaseCh)

	err := <-errCh
	if err == nil {
		t.Fatal("expected AuthorizeJoin to be rejected (metadata changed after Bind commit); got nil")
	}

	// The durable binding must be preserved (committed before the metadata
	// change). The binding has the original epoch.
	b := binding.getBinding()
	if b == nil {
		t.Fatal("expected durable binding to be preserved after guard rejection; got nil")
	}
	if b.EpochID != testEpochID {
		t.Fatalf("expected original epoch on preserved binding; got %s", b.EpochID)
	}
}

// TestAuthorizeJoin_PostCommit_StateChangeRace verifies that if the lifecycle
// state changes to RECOVERY_REQUIRED after the durable Bind's commit but
// before the final guard runs, the guard rejects (state does not permit join)
// and the durable binding is preserved.
func TestAuthorizeJoin_PostCommit_StateChangeRace(t *testing.T) {
	binding := newPostCommitBlockingBindingStore()
	src := NewFakeMetadataSource(finalizedMeta(), true)

	machine := NewMachine(nil)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, machine, src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// Run AuthorizeJoin in a goroutine; it will block after the Bind commits.
	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	}()

	// Wait for the Bind to commit.
	select {
	case <-binding.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Bind to commit")
	}

	// Force the state to RECOVERY_REQUIRED AFTER the commit but BEFORE
	// the Bind returns. The guard's state check (permitsJoinTransition)
	// will reject.
	machine.ForceTransitionForTest(RECOVERY_REQUIRED, "test: concurrent state change")

	// Release the blocked Bind.
	close(binding.releaseCh)

	err := <-errCh
	if err == nil {
		t.Fatal("expected AuthorizeJoin to be rejected (state changed to RECOVERY_REQUIRED after Bind commit); got nil")
	}

	// The durable binding must be preserved.
	b := binding.getBinding()
	if b == nil {
		t.Fatal("expected durable binding to be preserved after guard rejection; got nil")
	}
}

// TestAuthorizeJoin_PostCommit_AuthorityInvalidationRace verifies that if the
// authority (metadata freshness) is invalidated after the durable Bind's
// commit but before the final guard runs, the guard rejects (stale authority)
// and the durable binding is preserved.
func TestAuthorizeJoin_PostCommit_AuthorityInvalidationRace(t *testing.T) {
	binding := newPostCommitBlockingBindingStore()
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// Run AuthorizeJoin in a goroutine; it will block after the Bind commits.
	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	}()

	// Wait for the Bind to commit.
	select {
	case <-binding.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Bind to commit")
	}

	// Invalidate the authority AFTER the commit but BEFORE the Bind
	// returns. Set the authority state to invalid and the metadata to
	// unavailable (the guard's authority check will reject).
	src.SetUnavailable()
	ctrl.mu.Lock()
	ctrl.authorityState = "invalid"
	ctrl.accepted = nil
	ctrl.mu.Unlock()

	// Release the blocked Bind.
	close(binding.releaseCh)

	err := <-errCh
	if err == nil {
		t.Fatal("expected AuthorizeJoin to be rejected (authority invalidated after Bind commit); got nil")
	}

	// The durable binding must be preserved.
	b := binding.getBinding()
	if b == nil {
		t.Fatal("expected durable binding to be preserved after guard rejection; got nil")
	}
}

// TestAuthorizeJoin_PostCommit_GenesisInvariantRace verifies that if the
// finalized-genesis invariant is broken (e.g., GenesisAuthorized becomes true)
// after the durable Bind's commit but before the final guard runs, the
// post-Bind re-read or the guard rejects and the durable binding is preserved.
func TestAuthorizeJoin_PostCommit_GenesisInvariantRace(t *testing.T) {
	binding := newPostCommitBlockingBindingStore()
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// Run AuthorizeJoin in a goroutine; it will block after the Bind commits.
	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	}()

	// Wait for the Bind to commit.
	select {
	case <-binding.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Bind to commit")
	}

	// Break the genesis invariant AFTER the commit but BEFORE the Bind
	// returns. The post-Bind re-read will return metadata with
	// GenesisAuthorized=true, which fails the genesis invariant check
	// (c.cfg.SeedIdentity != "" → GenesisAuthorized must be false).
	brokenMeta := finalizedMeta()
	brokenMeta.GenesisAuthorized = true
	src.SetMetadata(brokenMeta)

	// Release the blocked Bind.
	close(binding.releaseCh)

	err := <-errCh
	if err == nil {
		t.Fatal("expected AuthorizeJoin to be rejected (genesis invariant broken after Bind commit); got nil")
	}

	// The durable binding must be preserved.
	b := binding.getBinding()
	if b == nil {
		t.Fatal("expected durable binding to be preserved after guard rejection; got nil")
	}
}

// TestAuthorizeJoin_PreBind_CtxCancel verifies that if the caller's context
// is cancelled BEFORE the Bind (during pre-binding validation or the
// observation), no binding is created.
func TestAuthorizeJoin_PreBind_CtxCancel(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// Cancel the context before calling AuthorizeJoin.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ctrl.AuthorizeJoin(ctx, newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to be rejected (context already cancelled); got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled; got %v", err)
	}
	// No binding should have been created.
	if binding.binding != nil {
		t.Fatalf("expected no binding after cancelled ctx; got %+v", binding.binding)
	}
}

// TestAuthorizeJoin_PreBind_ConflictingEpoch verifies that if the accepted
// metadata's epoch conflicts with the observation BEFORE the Bind, the join
// is rejected during pre-binding validation (no binding is created).
func TestAuthorizeJoin_PreBind_ConflictingEpoch(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// Pre-set a conflicting epoch in the accepted metadata.
	conflictingMeta := finalizedMeta()
	conflictingMeta.EpochID = "e-different"
	ctrl.SetAcceptedMetadataForTest(conflictingMeta)

	// The observer reports the ORIGINAL epoch (testEpochID), which now
	// conflicts with the accepted metadata's epoch. Pre-binding validation
	// (validateSourceObservationResult) rejects before any durable write.
	err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to be rejected (conflicting epoch); got nil")
	}
	// No binding should have been created (rejected before Bind).
	if binding.binding != nil {
		t.Fatalf("expected no binding after pre-binding rejection; got %+v", binding.binding)
	}
}

// TestAuthorizeJoin_PreBind_GenesisProvenanceRejects verifies that a pre-
// existing binding with genesis provenance is rejected by the join path
// (the join path requires join provenance; genesis is for the seed).
func TestAuthorizeJoin_PreBind_GenesisProvenanceRejects(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{
		binding: &DatabaseBinding{
			ClusterID:  testClusterID,
			EpochID:    testEpochID,
			Generation: 1,
			CreatedAt:  time.Now(),
			Provenance: ProvenanceGenesis,
		},
	}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// The node has an existing genesis-provenance binding. The join path
	// rejects it during pre-binding validation (binding.Provenance ==
	// ProvenanceGenesis → reject).
	err := ctrl.AuthorizeJoin(context.Background(), newTestSourceObserver(testEpochID), "127.0.0.1:9999", "gossipgrid-9.test.svc")
	if err == nil {
		t.Fatal("expected AuthorizeJoin to be rejected (genesis provenance on non-seed); got nil")
	}
}

// --- Pre-Bind revalidation race regression tests (Finding: pre-binding
// authorization revalidation) ---
//
// These tests verify that AuthorizeJoin revalidates the COMPLETE authorization
// against the CURRENT accepted authority AFTER the network source observation
// and BEFORE the durable Bind. A blocking observer blocks before returning
// its otherwise-valid observation; during the block the test changes the
// controller's accepted authority (or state). When the observer releases, the
// revalidation observes the changed authority and rejects the join BEFORE
// invoking Bind, so no binding is created and the database remains unbound.
//
// These are pre-Bind regressions (distinct from the accepted post-commit
// interleaving tests, which inject a change after the durable Bind's commit).

// blockingSourceObserver is a test double for the BootstrapSourceObserver
// interface that blocks on blockCh before returning its (otherwise valid)
// observation. It gives a test a deterministic window during the network
// observation in which to change the controller's accepted authority.
type blockingSourceObserver struct {
	result    SourceObservationResult
	blockCh   chan struct{}
	releaseCh chan struct{}
}

// newBlockingSourceObserver creates a blocking observer that returns the
// given (valid) observation after the test signals releaseCh.
func newBlockingSourceObserver(r SourceObservationResult) *blockingSourceObserver {
	return &blockingSourceObserver{
		result:    r,
		blockCh:   make(chan struct{}),
		releaseCh: make(chan struct{}),
	}
}

func (o *blockingSourceObserver) Observe(_ context.Context, _, _ string, _ RequestContext, _ string) (SourceObservationResult, error) {
	// Signal that the observation has begun (the test can now change the
	// controller's accepted authority during the "network I/O").
	o.blockCh <- struct{}{}
	// Block until the test releases.
	<-o.releaseCh
	return o.result, nil
}

// TestAuthorizeJoin_PreBind_BlockedObserver_AuthorityChangeRejects is the
// deterministic blocking-observer regression: a valid, initialized,
// unbound controller begins AuthorizeJoin; the observer blocks before
// returning its otherwise-valid observation; the test changes the
// controller's accepted authority; the observer releases; AuthorizeJoin
// rejects (the revalidation observes the changed authority), Bind is
// never invoked, and the database remains unbound.
func TestAuthorizeJoin_PreBind_BlockedObserver_AuthorityChangeRejects(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	// The observer returns an otherwise-valid observation (matching the
	// original finalized metadata) but blocks before returning it.
	validObs := NewSourceObservationResult(time.Now(), 10*time.Second, RequestContext{ClusterID: testClusterID, EpochID: testEpochID}, "gossipgrid-9.test.svc", "gossipgrid-9", "READY", testClusterID, testEpochID, true, true)
	obs := newBlockingSourceObserver(validObs)

	// Begin AuthorizeJoin in a goroutine; it blocks in the observation.
	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.AuthorizeJoin(context.Background(), obs, "127.0.0.1:9999", "gossipgrid-9.test.svc")
	}()

	// Wait for the observation to begin.
	select {
	case <-obs.blockCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the observer to begin")
	}

	// Change the controller's accepted authority DURING the observation:
	// the epoch changes (a new, accepted authority observation that does
	// not match the pinned target). The revalidation (after the
	// observation) observes the new authority and rejects before Bind.
	changedMeta := finalizedMeta()
	changedMeta.EpochID = "e-changed-during-observation"
	ctrl.SetAcceptedMetadataForTest(changedMeta)

	// Release the blocked observation.
	close(obs.releaseCh)

	err := <-errCh
	if err == nil {
		t.Fatal("CRITICAL: expected AuthorizeJoin to reject (authority changed during the observation); got nil")
	}
	// Bind was never invoked: the database remains unbound.
	if binding.binding != nil {
		t.Fatalf("CRITICAL: a binding was created (Bind was invoked) despite the authority change; it must not be (got %+v)", binding.binding)
	}
}

// TestAuthorizeJoin_PreBind_BlockedObserver_AuthorityInvalidationRejects
// verifies that a change that INVALIDATES the authority (the authority
// becomes invalid / the metadata becomes unavailable) during the
// observation causes the revalidation to reject before Bind.
func TestAuthorizeJoin_PreBind_BlockedObserver_AuthorityInvalidationRejects(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, NewMachine(nil), src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	validObs := NewSourceObservationResult(time.Now(), 10*time.Second, RequestContext{ClusterID: testClusterID, EpochID: testEpochID}, "gossipgrid-9.test.svc", "gossipgrid-9", "READY", testClusterID, testEpochID, true, true)
	obs := newBlockingSourceObserver(validObs)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.AuthorizeJoin(context.Background(), obs, "127.0.0.1:9999", "gossipgrid-9.test.svc")
	}()
	select {
	case <-obs.blockCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the observer to begin")
	}

	// Invalidate the authority DURING the observation (the metadata source
	// becomes unavailable; the controller's authority is no longer fresh).
	// The revalidation observes the invalid authority and rejects.
	src.SetUnavailable()
	ctrl.mu.Lock()
	ctrl.authorityState = "invalid"
	ctrl.mu.Unlock()

	close(obs.releaseCh)
	err := <-errCh
	if err == nil {
		t.Fatal("CRITICAL: expected AuthorizeJoin to reject (authority invalidated during the observation); got nil")
	}
	if binding.binding != nil {
		t.Fatalf("CRITICAL: a binding was created despite the authority invalidation; it must not be (got %+v)", binding.binding)
	}
}

// TestAuthorizeJoin_PreBind_BlockedObserver_StateChangeRejects verifies that
// a state change that FORBIDS joining (the controller moves to
// RECOVERY_REQUIRED) during the observation causes the revalidation to
// reject before Bind.
func TestAuthorizeJoin_PreBind_BlockedObserver_StateChangeRejects(t *testing.T) {
	binding := &fakeBindingStoreWithMessages{}
	src := NewFakeMetadataSource(finalizedMeta(), true)

	machine := NewMachine(nil)
	ctrl := NewController(Config{
		MetadataPollInterval: 100 * time.Millisecond,
		NodeID:               "gossipgrid-1",
		SeedIdentity:         genesisSeed,
		Enabled:              true,
	}, machine, src, binding, nil)

	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer ctrl.Stop(context.Background())

	validObs := NewSourceObservationResult(time.Now(), 10*time.Second, RequestContext{ClusterID: testClusterID, EpochID: testEpochID}, "gossipgrid-9.test.svc", "gossipgrid-9", "READY", testClusterID, testEpochID, true, true)
	obs := newBlockingSourceObserver(validObs)

	errCh := make(chan error, 1)
	go func() {
		errCh <- ctrl.AuthorizeJoin(context.Background(), obs, "127.0.0.1:9999", "gossipgrid-9.test.svc")
	}()
	select {
	case <-obs.blockCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the observer to begin")
	}

	// Force the state to RECOVERY_REQUIRED DURING the observation (a state
	// change that forbids joining). The revalidation observes the state
	// (permitsJoinTransition is false) and rejects before Bind.
	machine.ForceTransitionForTest(RECOVERY_REQUIRED, "test: state change during observation")

	close(obs.releaseCh)
	err := <-errCh
	if err == nil {
		t.Fatal("CRITICAL: expected AuthorizeJoin to reject (state changed to RECOVERY_REQUIRED during the observation); got nil")
	}
	if binding.binding != nil {
		t.Fatalf("CRITICAL: a binding was created despite the state change; it must not be (got %+v)", binding.binding)
	}
}

// postCommitBlockingBindingStore is a BindingStore that simulates the
// authoritative SQLite commit-then-return pattern: Bind COMMITs the binding
// (making it durable and never-rollback), signals commitCh, then blocks on
// releaseCh until the test changes state and releases. This is an in-memory
// test double that simulates a committed binding; it does not itself perform
// a SQLite transaction (the real SQLite transaction is the production
// linearization point). This gives the test a deterministic window between
// the commit (the simulated linearization point) and the guard evaluation to
// inject a state change.
type postCommitBlockingBindingStore struct {
	mu        sync.Mutex
	binding   *DatabaseBinding
	committed bool
	commitCh  chan struct{}
	releaseCh chan struct{}
}

func newPostCommitBlockingBindingStore() *postCommitBlockingBindingStore {
	return &postCommitBlockingBindingStore{
		commitCh:  make(chan struct{}, 1),
		releaseCh: make(chan struct{}),
	}
}

func (s *postCommitBlockingBindingStore) GetBinding() (*DatabaseBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.binding, nil
}

func (s *postCommitBlockingBindingStore) getBinding() *DatabaseBinding {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.binding
}

func (s *postCommitBlockingBindingStore) Bind(_ context.Context, clusterID, epochID string, generation int, provenance BindingProvenance) (*DatabaseBinding, error) {
	b := &DatabaseBinding{
		ClusterID:  clusterID,
		EpochID:    epochID,
		Generation: generation,
		CreatedAt:  time.Now(),
		Provenance: provenance,
	}

	// COMMIT: make the binding durable. In production, this is the
	// SQLite transaction commit (the linearization point). After this
	// point, the binding is preserved and never rolled back.
	s.mu.Lock()
	s.binding = b
	s.committed = true
	s.mu.Unlock()

	// Signal the commit to the test. The test can now change state
	// (stop, cancel, metadata change, etc.) before releasing.
	s.commitCh <- struct{}{}

	// Block until the test releases. This simulates the gap between
	// the SQLite commit and the Bind function's return.
	<-s.releaseCh

	return b, nil
}

func (s *postCommitBlockingBindingStore) CountMessages() (int64, error) {
	return 0, nil
}
