// Package lifecycle_test: integration test for the full genesis lifecycle
// with a real SQLite store (Milestone 5B, Stage 1B). This test covers the
// 8-step scenario: fresh genesis authorization → first durable binding →
// restart before finalization → finalization metadata observation →
// promotion to READY → restart after finalization → conflicting/revoked
// authority → no unauthorized public writes.
//
// The test uses a real temp SQLite store (store.Store) for the durable
// binding and a fake metadata source (lifecycle.FakeMetadataSource) for the
// authoritative metadata (no real K8s). The controller is started through
// the real Start() path (not ForceTransitionForTest).
//
// This test is in the EXTERNAL test package (lifecycle_test) to avoid an
// import cycle: lifecycle imports store (via BindingStore), and store
// imports lifecycle (for ClusterMetadata and DatabaseBinding). An external
// test package can import both without a cycle.
package lifecycle_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/store"
)

const (
	// genesisSeed is the designated seed identity used by the genesis
	// tests (a StatefulSet ordinal 0 pod, e.g. "gossipgrid-0").
	// This must match the value used in the lifecycle package's tests.
	seedIdentity = "gossipgrid-0"
	// testClusterID / testEpochID are the authoritative identities for the
	// genesis tests (matching the metadata fixtures).
	testClusterID = "0190e8a2-6b0c-7000-8000-000000000000"
	testEpochID   = "0190e8a3-1c2d-7000-9000-111111111111"
)

// pendingMeta returns the authoritative metadata for an outstanding,
// unconsumed genesis authorization.
func pendingMeta() lifecycle.ClusterMetadata {
	return lifecycle.ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              testEpochID,
		Generation:           1,
		Initialized:          false,
		GenesisAuthorized:    true,
		GenesisConsumer:      "",
		EverAcceptedMessages: false,
		DataLossProtected:    false,
	}
}

// finalizedMeta returns the authoritative metadata for a finalized cluster.
func finalizedMeta() lifecycle.ClusterMetadata {
	return lifecycle.ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              testEpochID,
		Generation:           1,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      seedIdentity,
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
}

// TestGenesisLifecycle_Integration is the 8-step integration test.
func TestGenesisLifecycle_Integration(t *testing.T) {
	// Create a real SQLite store in a temp directory.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gossipgrid.db")
	st, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("opening real SQLite store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	// The metadata source: starts with the PENDING authorization.
	src := lifecycle.NewFakeMetadataSource(pendingMeta(), true)

	cfg := lifecycle.Config{
		MetadataPollInterval: 10 * time.Millisecond,
		NodeID:               seedIdentity,
		SeedIdentity:         seedIdentity,
		Enabled:              true,
	}

	// -------------------------------------------------------------------
	// Step 1: Fresh genesis authorization (empty DB, no binding).
	// -------------------------------------------------------------------
	t.Log("=== Step 1: Fresh genesis authorization (empty DB) ===")
	ctrl1 := lifecycle.NewController(cfg, lifecycle.NewMachine(nil), src, st, nil)
	if err := ctrl1.Start(context.Background()); err != nil {
		t.Fatalf("Start (step 1): %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ctrl1.Snapshot().State == lifecycle.DISCOVERING {
			break
		}
		time.Sleep(time.Millisecond)
	}
	snap := ctrl1.Snapshot()
	if snap.State != lifecycle.DISCOVERING {
		t.Fatalf("step 1: expected DISCOVERING; got %s (reason=%s)", snap.State, snap.Reason)
	}
	if b, _ := st.GetBinding(); b != nil {
		t.Fatal("step 1: expected no binding; got one")
	}

	// -------------------------------------------------------------------
	// Step 2: First durable binding (AuthorizeGenesis).
	// -------------------------------------------------------------------
	t.Log("=== Step 2: First durable binding (AuthorizeGenesis) ===")
	if err := ctrl1.AuthorizeGenesis(context.Background()); err != nil {
		t.Fatalf("AuthorizeGenesis (step 2): %v", err)
	}
	snap = ctrl1.Snapshot()
	if snap.State != lifecycle.GENESIS {
		t.Fatalf("step 2: expected GENESIS; got %s (reason=%s)", snap.State, snap.Reason)
	}
	b, err := st.GetBinding()
	if err != nil {
		t.Fatalf("step 2: reading binding: %v", err)
	}
	if b == nil {
		t.Fatal("step 2: expected a binding; got nil")
	}
	if b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatalf("step 2: binding mismatch; got %s/%s", b.ClusterID, b.EpochID)
	}
	if d := ctrl1.Admission().CheckWrite(context.Background()); d.Allowed {
		t.Fatal("step 2: expected writes blocked (GENESIS); got allowed")
	}
	ctrl1.Stop(context.Background())

	// -------------------------------------------------------------------
	// Step 3: Restart before finalization.
	// -------------------------------------------------------------------
	t.Log("=== Step 3: Restart before finalization ===")
	ctrl2 := lifecycle.NewController(cfg, lifecycle.NewMachine(nil), src, st, nil)
	if err := ctrl2.Start(context.Background()); err != nil {
		t.Fatalf("Start (step 3): %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ctrl2.Snapshot().State == lifecycle.GENESIS {
			break
		}
		time.Sleep(time.Millisecond)
	}
	snap = ctrl2.Snapshot()
	if snap.State != lifecycle.GENESIS {
		t.Fatalf("step 3: expected GENESIS; got %s (reason=%s)", snap.State, snap.Reason)
	}
	if d := ctrl2.Admission().CheckWrite(context.Background()); d.Allowed {
		t.Fatal("step 3: expected writes blocked (GENESIS); got allowed")
	}

	// -------------------------------------------------------------------
	// Step 4: Finalization metadata observation.
	// -------------------------------------------------------------------
	t.Log("=== Step 4: Finalization metadata observation ===")
	src.SetMetadata(finalizedMeta())
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if snap := ctrl2.Snapshot(); snap.State == lifecycle.READY || snap.State == lifecycle.RECOVERY_REQUIRED {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctrl2.EvaluateMetadataForTest(context.Background())

	// -------------------------------------------------------------------
	// Step 5: Promotion to READY.
	// -------------------------------------------------------------------
	t.Log("=== Step 5: Promotion to READY ===")
	snap = ctrl2.Snapshot()
	if snap.State != lifecycle.READY {
		t.Fatalf("step 5: expected READY; got %s (reason=%s)", snap.State, snap.Reason)
	}
	if d := ctrl2.Admission().CheckWrite(context.Background()); !d.Allowed {
		t.Fatalf("step 5: expected writes allowed (READY); got denied (reason=%s)", d.Reason)
	}
	ctrl2.Stop(context.Background())

	// -------------------------------------------------------------------
	// Step 6: Restart after finalization.
	// -------------------------------------------------------------------
	t.Log("=== Step 6: Restart after finalization ===")
	ctrl3 := lifecycle.NewController(cfg, lifecycle.NewMachine(nil), src, st, nil)
	if err := ctrl3.Start(context.Background()); err != nil {
		t.Fatalf("Start (step 6): %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ctrl3.Snapshot().State == lifecycle.READY {
			break
		}
		time.Sleep(time.Millisecond)
	}
	snap = ctrl3.Snapshot()
	if snap.State != lifecycle.READY {
		t.Fatalf("step 6: expected READY; got %s (reason=%s)", snap.State, snap.Reason)
	}
	if d := ctrl3.Admission().CheckWrite(context.Background()); !d.Allowed {
		t.Fatalf("step 6: expected writes allowed (READY); got denied (reason=%s)", d.Reason)
	}

	// -------------------------------------------------------------------
	// Step 7: Conflicting/revoked authority.
	// -------------------------------------------------------------------
	t.Log("=== Step 7: Conflicting/revoked authority ===")
	revokedMeta := lifecycle.ClusterMetadata{
		ClusterID:            testClusterID,
		EpochID:              "a-different-epoch",
		Generation:           2,
		Initialized:          true,
		GenesisAuthorized:    false,
		GenesisConsumer:      "another-seed",
		EverAcceptedMessages: true,
		DataLossProtected:    true,
	}
	src.SetMetadata(revokedMeta)
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if snap := ctrl3.Snapshot(); snap.State == lifecycle.RECOVERY_REQUIRED {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctrl3.EvaluateMetadataForTest(context.Background())
	snap = ctrl3.Snapshot()
	if snap.State != lifecycle.RECOVERY_REQUIRED {
		t.Fatalf("step 7: expected RECOVERY_REQUIRED; got %s (reason=%s)", snap.State, snap.Reason)
	}
	b, _ = st.GetBinding()
	if b == nil || b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatal("step 7: the binding was not preserved on revocation")
	}
	if d := ctrl3.Admission().CheckWrite(context.Background()); d.Allowed {
		t.Fatal("step 7: expected writes blocked (RECOVERY_REQUIRED); got allowed")
	}

	// -------------------------------------------------------------------
	// Step 8: No unauthorized public writes (final verification).
	// -------------------------------------------------------------------
	t.Log("=== Step 8: No unauthorized public writes ===")
	snap = ctrl3.Snapshot()
	if snap.State != lifecycle.RECOVERY_REQUIRED {
		t.Fatalf("step 8: expected RECOVERY_REQUIRED; got %s", snap.State)
	}
	if d := ctrl3.Admission().CheckWrite(context.Background()); d.Allowed {
		t.Fatal("step 8: expected writes blocked (RECOVERY_REQUIRED); got allowed")
	}
	t.Log("step 8: public writes correctly blocked in RECOVERY_REQUIRED")
	ctrl3.Stop(context.Background())
}
