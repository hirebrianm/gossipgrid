// Tests for the narrowly scoped bootstrap baseline fetch admission (Milestone
// 5B, Stage 2): CheckBootstrapFetch (requester side), CheckBootstrapSourceInbound
// (serving side), and CommitUnderBootstrapAuthorization (commit boundary).
//
// These tests exercise the ACTUAL admission decisions (real controller + real
// authorization snapshot) for a joining, epoch-bound SYNCING replica, not
// forced or mocked results. The predicate is the dedicated bootstrap decision
// (bootstrapAuthorized), which is intentionally distinct from the broad
// ordinary-replication decision (authorized, which requires a ready state).

package lifecycle

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newSyncingController creates a real Controller in the given lifecycle state
// with the supplied authoritative metadata and a durable binding that either
// matches the metadata (binding != nil and matching) or is absent/conflicting.
// The metadata poll interval is far in the future so the observation never
// goes stale during the test. It returns the controller and its Admission
// guard (the production guard, enabled=true).
func newSyncingController(t *testing.T, state State, meta ClusterMetadata, binding *DatabaseBinding) *Controller {
	t.Helper()
	src := NewFakeMetadataSourceUnavailable()
	bstore := &blockingBindingStore{binding: binding}
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "gossipgrid-1",
		Enabled:              true,
	}, NewMachine(nil), src, bstore, nil)
	// Force the target state directly. ForceTransitionForTest bypasses the
	// transition table, so any state can be set (needed for DEGRADED, which
	// is only reachable via READY → DEGRADED in the legal table, and for
	// STARTING, which is the initial state and not a transition target).
	ctrl.machine.ForceTransitionForTest(state, "test")
	if meta.ClusterID != "" {
		ctrl.SetAcceptedMetadataForTest(meta)
	}
	if binding != nil {
		ctrl.SetBindingCacheForTest(binding)
	}
	return ctrl
}

// matchingSyncingMeta returns the canonical fresh, valid, initialized metadata
// used throughout the bootstrap admission tests.
func matchingSyncingMeta() ClusterMetadata {
	return ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e1",
		Generation:  1,
		Initialized: true,
	}
}

// matchingBinding returns a durable binding matching matchingSyncingMeta.
func matchingBinding() *DatabaseBinding {
	return &DatabaseBinding{
		ClusterID:  "c1",
		EpochID:    "e1",
		Generation: 1,
		CreatedAt:  time.Now(),
	}
}

func validReqCtx() RequestContext {
	return RequestContext{ClusterID: "c1", EpochID: "e1"}
}

// --- Requester admission: CheckBootstrapFetch ---

func TestCheckBootstrapFetch_ValidBoundSyncing(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	g := ctrl.Admission()
	d := g.CheckBootstrapFetch(context.Background(), validReqCtx())
	if !d.Allowed {
		t.Fatalf("expected bootstrap fetch allowed for valid bound SYNCING replica, got: %s", d.Reason)
	}
}

func TestCheckBootstrapFetch_UnboundSyncing(t *testing.T) {
	// No durable binding: the replica must be bound before fetching a
	// baseline.
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), nil)
	g := ctrl.Admission()
	d := g.CheckBootstrapFetch(context.Background(), validReqCtx())
	if d.Allowed {
		t.Fatalf("expected bootstrap fetch rejected for unbound SYNCING replica, got: %s", d.Reason)
	}
}

func TestCheckBootstrapFetch_ConflictingBinding(t *testing.T) {
	// The binding matches a DIFFERENT epoch than the metadata: a conflicting
	// (wrong-epoch) authority must not authorize a fetch.
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), &DatabaseBinding{
		ClusterID:  "c1",
		EpochID:    "e2",
		Generation: 1,
		CreatedAt:  time.Now(),
	})
	g := ctrl.Admission()
	d := g.CheckBootstrapFetch(context.Background(), validReqCtx())
	if d.Allowed {
		t.Fatalf("expected bootstrap fetch rejected for conflicting binding, got: %s", d.Reason)
	}
}

func TestCheckBootstrapFetch_StaleMetadata(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	// Age the observation past the freshness bound (2x the poll interval).
	ctrl.mu.Lock()
	ctrl.freshAt = time.Now().Add(-3 * time.Hour)
	ctrl.mu.Unlock()
	g := ctrl.Admission()
	d := g.CheckBootstrapFetch(context.Background(), validReqCtx())
	if d.Allowed {
		t.Fatalf("expected bootstrap fetch rejected for stale metadata, got: %s", d.Reason)
	}
}

func TestCheckBootstrapFetch_InvalidAuthority(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	// Force the authority state to invalid (a metadata read failure).
	ctrl.mu.Lock()
	ctrl.authorityState = "invalid"
	ctrl.mu.Unlock()
	g := ctrl.Admission()
	d := g.CheckBootstrapFetch(context.Background(), validReqCtx())
	if d.Allowed {
		t.Fatalf("expected bootstrap fetch rejected for invalid authority, got: %s", d.Reason)
	}
}

func TestCheckBootstrapFetch_UninitializedMetadata(t *testing.T) {
	// Initialized=false: there is no baseline to fetch.
	meta := matchingSyncingMeta()
	meta.Initialized = false
	ctrl := newSyncingController(t, SYNCING, meta, matchingBinding())
	g := ctrl.Admission()
	d := g.CheckBootstrapFetch(context.Background(), validReqCtx())
	if d.Allowed {
		t.Fatalf("expected bootstrap fetch rejected for uninitialized metadata, got: %s", d.Reason)
	}
}

func TestCheckBootstrapFetch_MissingMetadata(t *testing.T) {
	// No authoritative observation at all.
	ctrl := newSyncingController(t, SYNCING, ClusterMetadata{}, nil)
	g := ctrl.Admission()
	d := g.CheckBootstrapFetch(context.Background(), validReqCtx())
	if d.Allowed {
		t.Fatalf("expected bootstrap fetch rejected for missing metadata, got: %s", d.Reason)
	}
}

func TestCheckBootstrapFetch_GenesisManagedUnfinalized(t *testing.T) {
	// A genesis-managed lifecycle (a designated seed) whose record is
	// initialized but NOT completely finalized must not authorize a joining
	// replica to fetch.
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	ctrl.cfg.SeedIdentity = "gossipgrid-0"
	// The record is initialized but DataLossProtected is not armed:
	// finalizationComplete() is false for a genesis-managed configuration.
	g := ctrl.Admission()
	d := g.CheckBootstrapFetch(context.Background(), validReqCtx())
	if d.Allowed {
		t.Fatalf("expected bootstrap fetch rejected for genesis-managed unfinalized record, got: %s", d.Reason)
	}
}

func TestCheckBootstrapFetch_GenesisManagedFinalized(t *testing.T) {
	// A genesis-managed lifecycle with a COMPLETELY finalized record (the
	// data-loss protection armed and the one-time authorization consumed by
	// the designated seed) DOES authorize a joining replica to fetch.
	meta := matchingSyncingMeta()
	meta.GenesisConsumer = "gossipgrid-0"
	meta.DataLossProtected = true
	ctrl := newSyncingController(t, SYNCING, meta, matchingBinding())
	ctrl.cfg.SeedIdentity = "gossipgrid-0"
	g := ctrl.Admission()
	d := g.CheckBootstrapFetch(context.Background(), validReqCtx())
	if !d.Allowed {
		t.Fatalf("expected bootstrap fetch allowed for genesis-managed finalized record, got: %s", d.Reason)
	}
}

func TestCheckBootstrapFetch_StoppedController(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	ctrl.mu.Lock()
	ctrl.stopped = true
	ctrl.mu.Unlock()
	g := ctrl.Admission()
	d := g.CheckBootstrapFetch(context.Background(), validReqCtx())
	if d.Allowed {
		t.Fatalf("expected bootstrap fetch rejected for stopped controller, got: %s", d.Reason)
	}
}

func TestCheckBootstrapFetch_WrongEpochRequestContext(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	g := ctrl.Admission()
	// A request carrying a different epoch must be rejected.
	d := g.CheckBootstrapFetch(context.Background(), RequestContext{ClusterID: "c1", EpochID: "e2"})
	if d.Allowed {
		t.Fatalf("expected bootstrap fetch rejected for wrong-epoch request, got: %s", d.Reason)
	}
	// A request with a different cluster must be rejected.
	d = g.CheckBootstrapFetch(context.Background(), RequestContext{ClusterID: "c2", EpochID: "e1"})
	if d.Allowed {
		t.Fatalf("expected bootstrap fetch rejected for wrong-cluster request, got: %s", d.Reason)
	}
}

func TestCheckBootstrapFetch_MissingEpochRequestContext(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	g := ctrl.Admission()
	d := g.CheckBootstrapFetch(context.Background(), RequestContext{})
	if d.Allowed {
		t.Fatalf("expected bootstrap fetch rejected for missing epoch context, got: %s", d.Reason)
	}
}

// TestCheckBootstrapFetch_OtherStates verifies that no other lifecycle state
// (not just SYNCING) can use the bootstrap fetch privilege. A ready state uses
// the ordinary authorization; a recovery or genesis state is not fetching a
// baseline.
func TestCheckBootstrapFetch_OtherStates(t *testing.T) {
	states := []State{STARTING, DISCOVERING, GENESIS, READY, DEGRADED, RECOVERY_REQUIRED}
	for _, st := range states {
		t.Run(st.String(), func(t *testing.T) {
			ctrl := newSyncingController(t, st, matchingSyncingMeta(), matchingBinding())
			g := ctrl.Admission()
			d := g.CheckBootstrapFetch(context.Background(), validReqCtx())
			if d.Allowed {
				t.Fatalf("expected bootstrap fetch rejected for state %s, got: %s", st, d.Reason)
			}
		})
	}
}

// --- Serving-side admission: CheckBootstrapSourceInbound ---

func TestCheckBootstrapSourceInbound_EligibleReadySource(t *testing.T) {
	ctrl := newSyncingController(t, READY, matchingSyncingMeta(), matchingBinding())
	g := ctrl.Admission()
	d := g.CheckBootstrapSourceInbound(context.Background(), validReqCtx())
	if !d.Allowed {
		t.Fatalf("expected bootstrap source inbound allowed for eligible READY source, got: %s", d.Reason)
	}
}

func TestCheckBootstrapSourceInbound_EligibleDegradedSource(t *testing.T) {
	// A DEGRADED replica retains valid local authority and is eligible to
	// serve a bootstrap baseline (the documented READY/DEGRADED policy).
	ctrl := newSyncingController(t, DEGRADED, matchingSyncingMeta(), matchingBinding())
	g := ctrl.Admission()
	d := g.CheckBootstrapSourceInbound(context.Background(), validReqCtx())
	if !d.Allowed {
		t.Fatalf("expected bootstrap source inbound allowed for eligible DEGRADED source, got: %s", d.Reason)
	}
}

func TestCheckBootstrapSourceInbound_SyncingSourceRejected(t *testing.T) {
	// A SYNCING replica is never an authoritative bootstrap source: its state
	// is not ready, so the full authorized() decision fails.
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	g := ctrl.Admission()
	d := g.CheckBootstrapSourceInbound(context.Background(), validReqCtx())
	if d.Allowed {
		t.Fatalf("expected bootstrap source inbound rejected for SYNCING source, got: %s", d.Reason)
	}
}

func TestCheckBootstrapSourceInbound_GenesisSourceRejected(t *testing.T) {
	ctrl := newSyncingController(t, GENESIS, matchingSyncingMeta(), matchingBinding())
	g := ctrl.Admission()
	d := g.CheckBootstrapSourceInbound(context.Background(), validReqCtx())
	if d.Allowed {
		t.Fatalf("expected bootstrap source inbound rejected for GENESIS source, got: %s", d.Reason)
	}
}

func TestCheckBootstrapSourceInbound_RecoveryRequiredSourceRejected(t *testing.T) {
	ctrl := newSyncingController(t, RECOVERY_REQUIRED, matchingSyncingMeta(), matchingBinding())
	g := ctrl.Admission()
	d := g.CheckBootstrapSourceInbound(context.Background(), validReqCtx())
	if d.Allowed {
		t.Fatalf("expected bootstrap source inbound rejected for RECOVERY_REQUIRED source, got: %s", d.Reason)
	}
}

func TestCheckBootstrapSourceInbound_WrongEpochSource(t *testing.T) {
	// A source whose metadata is a different epoch than the request: the
	// request's epoch context does not match the local authorized metadata.
	ctrl := newSyncingController(t, READY, matchingSyncingMeta(), matchingBinding())
	g := ctrl.Admission()
	d := g.CheckBootstrapSourceInbound(context.Background(), RequestContext{ClusterID: "c1", EpochID: "e2"})
	if d.Allowed {
		t.Fatalf("expected bootstrap source inbound rejected for wrong-epoch request, got: %s", d.Reason)
	}
}

func TestCheckBootstrapSourceInbound_WrongClusterSource(t *testing.T) {
	ctrl := newSyncingController(t, READY, matchingSyncingMeta(), matchingBinding())
	g := ctrl.Admission()
	d := g.CheckBootstrapSourceInbound(context.Background(), RequestContext{ClusterID: "c2", EpochID: "e1"})
	if d.Allowed {
		t.Fatalf("expected bootstrap source inbound rejected for wrong-cluster request, got: %s", d.Reason)
	}
}

func TestCheckBootstrapSourceInbound_MissingEpochContext(t *testing.T) {
	ctrl := newSyncingController(t, READY, matchingSyncingMeta(), matchingBinding())
	g := ctrl.Admission()
	d := g.CheckBootstrapSourceInbound(context.Background(), RequestContext{})
	if d.Allowed {
		t.Fatalf("expected bootstrap source inbound rejected for missing epoch context, got: %s", d.Reason)
	}
}

// TestCheckBootstrapSourceInbound_UnboundSource verifies that a ready-state
// source with no durable binding is not authorized (the full authorized()
// decision requires a matching binding), so it cannot serve a bootstrap
// baseline.
func TestCheckBootstrapSourceInbound_UnboundSource(t *testing.T) {
	ctrl := newSyncingController(t, READY, matchingSyncingMeta(), nil)
	g := ctrl.Admission()
	d := g.CheckBootstrapSourceInbound(context.Background(), validReqCtx())
	if d.Allowed {
		t.Fatalf("expected bootstrap source inbound rejected for unbound READY source, got: %s", d.Reason)
	}
}

// --- Commit boundary: CommitUnderBootstrapAuthorization ---

// TestCommitUnderBootstrapAuthorization_ValidSyncing verifies that a bound,
// epoch-bound, fresh, finalized SYNCING replica may install a fetched baseline
// through the guarded bootstrap commit path.
func TestCommitUnderBootstrapAuthorization_ValidSyncing(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	ran := false
	err := ctrl.CommitUnderBootstrapAuthorization(validReqCtx(), func() error {
		ran = true
		return nil
	})
	if err != nil {
		t.Fatalf("expected bootstrap commit to succeed for valid bound SYNCING replica, got: %v", err)
	}
	if !ran {
		t.Fatalf("expected the bootstrap commit callback to run")
	}
}

// TestCommitUnderBootstrapAuthorization_RevokedState verifies that if the
// requester leaves SYNCING (e.g. promoted to READY) between the fetch and the
// local insert, the bootstrap commit is rejected and no data is installed.
func TestCommitUnderBootstrapAuthorization_RevokedState(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	// Simulate a state change out of SYNCING before the commit.
	if err := ctrl.machine.TransitionForTest(READY, "test"); err != nil {
		t.Fatalf("transition to READY: %v", err)
	}
	ran := false
	err := ctrl.CommitUnderBootstrapAuthorization(validReqCtx(), func() error {
		ran = true
		return nil
	})
	if err == nil {
		t.Fatalf("expected bootstrap commit rejected after state change out of SYNCING")
	}
	if ran {
		t.Fatalf("expected the bootstrap commit callback NOT to run after revocation")
	}
}

// TestCommitUnderBootstrapAuthorization_EpochChanged verifies that if the
// authoritative epoch changes (the binding no longer matches the metadata)
// between the fetch and the insert, the bootstrap commit is rejected.
func TestCommitUnderBootstrapAuthorization_EpochChanged(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	// Simulate an epoch change: the authoritative metadata moves to a new
	// epoch that the durable binding does not match.
	ctrl.SetAcceptedMetadataForTest(ClusterMetadata{
		ClusterID:   "c1",
		EpochID:     "e2",
		Generation:  2,
		Initialized: true,
	})
	ran := false
	err := ctrl.CommitUnderBootstrapAuthorization(validReqCtx(), func() error {
		ran = true
		return nil
	})
	if err == nil {
		t.Fatalf("expected bootstrap commit rejected after epoch change (binding/metadata mismatch)")
	}
	if ran {
		t.Fatalf("expected the bootstrap commit callback NOT to run after an epoch change")
	}
}

// TestCommitUnderBootstrapAuthorization_MetadataRevoked verifies that a
// metadata revocation (the authority becomes invalid) between the fetch and the
// insert prevents installation.
func TestCommitUnderBootstrapAuthorization_MetadataRevoked(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	ctrl.mu.Lock()
	ctrl.authorityState = "invalid"
	ctrl.mu.Unlock()
	ran := false
	err := ctrl.CommitUnderBootstrapAuthorization(validReqCtx(), func() error {
		ran = true
		return nil
	})
	if err == nil {
		t.Fatalf("expected bootstrap commit rejected after metadata revocation (authority invalid)")
	}
	if ran {
		t.Fatalf("expected the bootstrap commit callback NOT to run after metadata revocation")
	}
}

// TestCommitUnderBootstrapAuthorization_Stopped verifies that a stopped
// controller rejects the bootstrap commit (no data installed).
func TestCommitUnderBootstrapAuthorization_Stopped(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	ctrl.mu.Lock()
	ctrl.stopped = true
	ctrl.mu.Unlock()
	ran := false
	err := ctrl.CommitUnderBootstrapAuthorization(validReqCtx(), func() error {
		ran = true
		return nil
	})
	if err == nil {
		t.Fatalf("expected bootstrap commit rejected for stopped controller")
	}
	if ran {
		t.Fatalf("expected the bootstrap commit callback NOT to run for a stopped controller")
	}
}

// TestCommitUnderBootstrapAuthorization_Unbound verifies that an unbound
// replica (no durable binding) cannot install a fetched baseline: the
// bootstrap decision requires a matching durable binding.
func TestCommitUnderBootstrapAuthorization_Unbound(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), nil)
	ran := false
	err := ctrl.CommitUnderBootstrapAuthorization(validReqCtx(), func() error {
		ran = true
		return nil
	})
	if err == nil {
		t.Fatalf("expected bootstrap commit rejected for unbound replica")
	}
	if ran {
		t.Fatalf("expected the bootstrap commit callback NOT to run for an unbound replica")
	}
}

// TestCommitUnderBootstrapAuthorization_NonSyncingState verifies that a
// replica in a non-SYNCING state cannot install a fetched baseline through the
// bootstrap commit path (the dedicated bootstrap decision requires exactly
// SYNCING). A ready-state replica uses the ordinary commit path instead.
func TestCommitUnderBootstrapAuthorization_NonSyncingState(t *testing.T) {
	for _, st := range []State{READY, DEGRADED, DISCOVERING, GENESIS, RECOVERY_REQUIRED} {
		t.Run(st.String(), func(t *testing.T) {
			ctrl := newSyncingController(t, st, matchingSyncingMeta(), matchingBinding())
			ran := false
			err := ctrl.CommitUnderBootstrapAuthorization(validReqCtx(), func() error {
				ran = true
				return nil
			})
			if err == nil {
				t.Fatalf("expected bootstrap commit rejected for non-SYNCING state %s", st)
			}
			if ran {
				t.Fatalf("expected the bootstrap commit callback NOT to run for non-SYNCING state %s", st)
			}
		})
	}
}

// TestCommitUnderBootstrapAuthorization_CallbackErrorPropagates verifies that a
// non-nil error returned by the commit callback (e.g. an insert conflict) is
// propagated to the caller.
func TestCommitUnderBootstrapAuthorization_CallbackErrorPropagates(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	boom := errors.New("insert conflict")
	err := ctrl.CommitUnderBootstrapAuthorization(validReqCtx(), func() error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("expected the callback error to propagate, got: %v", err)
	}
}

// TestCommitUnderBootstrapAuthorization_DisabledPassthrough verifies that when
// lifecycle protection is disabled, the bootstrap commit guard is a passthrough
// (legacy behavior preserved): the callback runs unconditionally.
func TestCommitUnderBootstrapAuthorization_DisabledPassthrough(t *testing.T) {
	src := NewFakeMetadataSourceUnavailable()
	ctrl := NewController(Config{
		MetadataPollInterval: time.Hour,
		NodeID:               "gossipgrid-1",
		Enabled:              false, // lifecycle protection disabled
	}, NewMachine(nil), src, &blockingBindingStore{}, nil)
	ran := false
	err := ctrl.CommitUnderBootstrapAuthorization(RequestContext{}, func() error {
		ran = true
		return nil
	})
	if err != nil {
		t.Fatalf("expected bootstrap commit passthrough when lifecycle disabled, got: %v", err)
	}
	if !ran {
		t.Fatalf("expected the bootstrap commit callback to run when lifecycle disabled")
	}
}

// --- C3 regression tests: pinned epoch throughout transport ---

// TestCommitUnderBootstrapAuthorization_EmptyExpectedRejected verifies that
// when lifecycle is enabled, an empty expected RequestContext (no ClusterID
// or EpochID) is rejected (fail-closed). The bootstrap commit must NOT accept
// a commit with no pinned epoch (Issue 5: the commit is bound to the pinned
// cluster/epoch; an empty context is a protocol violation).
func TestCommitUnderBootstrapAuthorization_EmptyExpectedRejected(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	ran := false
	err := ctrl.CommitUnderBootstrapAuthorization(RequestContext{}, func() error {
		ran = true
		return nil
	})
	if err == nil {
		t.Fatal("expected empty expected context to be rejected when lifecycle enabled")
	}
	if ran {
		t.Fatal("expected the callback NOT to run when the expected context is empty")
	}
}

// TestCommitUnderBootstrapAuthorization_PartialExpectedRejected verifies that
// when lifecycle is enabled, a partially-populated expected RequestContext
// (only ClusterID set, no EpochID) is rejected (fail-closed). Both fields
// are required for a valid pinned context.
func TestCommitUnderBootstrapAuthorization_PartialExpectedRejected(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	ran := false
	err := ctrl.CommitUnderBootstrapAuthorization(RequestContext{ClusterID: "c1"}, func() error {
		ran = true
		return nil
	})
	if err == nil {
		t.Fatal("expected partial expected context (no EpochID) to be rejected")
	}
	if ran {
		t.Fatal("expected the callback NOT to run when the expected context is incomplete")
	}
}

// TestCommitUnderBootstrapAuthorization_EpochChangedBeforeCommit verifies that
// when the current epoch changes (metadata updated to a new epoch) after the
// pinned context was captured, the commit is rejected (the pinned context no
// longer matches the current snapshot). This is the "epoch change before
// local installation" regression test (Issue 5).
func TestCommitUnderBootstrapAuthorization_EpochChangedBeforeCommit(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	// The pinned context (c1/e1) was captured when the metadata was
	// matchingSyncingMeta (c1/e1). Now the metadata is updated to a new
	// epoch (c1/e2) — the controller's snapshot now reflects e2.
	newMeta := matchingSyncingMeta()
	newMeta.EpochID = "e2"
	newBinding := matchingBinding()
	newBinding.EpochID = "e2"
	ctrl.SetAcceptedMetadataForTest(newMeta)
	ctrl.SetBindingCacheForTest(newBinding)

	// The pinned context (c1/e1) no longer matches the current snapshot
	// (c1/e2): the commit must be rejected.
	ran := false
	err := ctrl.CommitUnderBootstrapAuthorization(RequestContext{ClusterID: "c1", EpochID: "e1"}, func() error {
		ran = true
		return nil
	})
	if err == nil {
		t.Fatal("expected commit to be rejected when the epoch changed before installation")
	}
	if ran {
		t.Fatal("expected the callback NOT to run when the epoch changed before installation")
	}
}

// TestCommitUnderBootstrapAuthorization_ClusterChangedBeforeCommit verifies that
// when the current cluster ID changes after the pinned context was captured,
// the commit is rejected. This is the "cluster change before local
// installation" regression test.
func TestCommitUnderBootstrapAuthorization_ClusterChangedBeforeCommit(t *testing.T) {
	ctrl := newSyncingController(t, SYNCING, matchingSyncingMeta(), matchingBinding())
	// Update the metadata to a different cluster (c2/e1).
	newMeta := matchingSyncingMeta()
	newMeta.ClusterID = "c2"
	newBinding := matchingBinding()
	newBinding.ClusterID = "c2"
	ctrl.SetAcceptedMetadataForTest(newMeta)
	ctrl.SetBindingCacheForTest(newBinding)

	// The pinned context (c1/e1) no longer matches the current snapshot
	// (c2/e1): the commit must be rejected.
	ran := false
	err := ctrl.CommitUnderBootstrapAuthorization(RequestContext{ClusterID: "c1", EpochID: "e1"}, func() error {
		ran = true
		return nil
	})
	if err == nil {
		t.Fatal("expected commit to be rejected when the cluster changed before installation")
	}
	if ran {
		t.Fatal("expected the callback NOT to run when the cluster changed before installation")
	}
}
