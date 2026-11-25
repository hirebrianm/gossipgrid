// Package lifecycle_test: integration test for the actual
// finalization-to-promotion sequence (Milestone 5B, Stage 1B, Fix 4).
//
// This test uses the REAL FinalizeOperation (not a FakeMetadataSource.Set
// shortcut) against a fake Kubernetes ConfigMap source/updater that backs
// both the operator's finalization Job (ConfigMapUpdater) and the seed
// controller's metadata polling (MetadataSource). The fake is a single
// ConfigMap that advances its resourceVersion on Update, so the
// FinalizeOperation's CAS and the controller's Read observe the same
// authoritative record.
//
// Verification points:
//  1. Genesis authorization is pending (Initialized=false,
//     GenesisAuthorized=true, GenesisConsumer="", DataLossProtected=false).
//  2. The seed binds durably (AuthorizeGenesis).
//  3. The operator finalizes using matching expected identities (the real
//     FinalizeOperation performs a CAS and the ConfigMap is updated).
//  4. The seed observes the finalized metadata and reaches READY.
//  5. A mismatched finalization request (wrong ClusterID) never updates the
//     ConfigMap (0 Update calls; the record is unchanged).
//  6. A revoked finalization (the pending record is replaced by a revoked
//     record) cannot authorize public writes (the seed does not enter
//     GENESIS; writes are blocked).
//  7. Restart after successful finalization remains safe (the seed reaches
//     READY and writes are allowed).
package lifecycle_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gossipgrid/internal/lifecycle"
	"gossipgrid/internal/lifecycle/genesis"
	"gossipgrid/internal/lifecycle/k8smetadata"
	"gossipgrid/internal/store"
)

// cmDoc is the JSON document stored in the fake ConfigMap's "metadata" key.
// It is the same schema the k8smetadata package parses.
type cmDoc struct {
	SchemaVersion        *int    `json:"schemaVersion"`
	ClusterID            *string `json:"clusterId"`
	EpochID              *string `json:"epochId"`
	Generation           *int    `json:"generation"`
	Initialized          *bool   `json:"initialized"`
	GenesisAuthorized    *bool   `json:"genesisAuthorized"`
	GenesisConsumer      *string `json:"genesisConsumer"`
	EverAcceptedMessages *bool   `json:"everAcceptedMessages"`
	DataLossProtected    *bool   `json:"dataLossProtected"`
	LastReset            *string `json:"lastReset,omitempty"`
}

// marshalCMDoc renders the document as the JSON string stored in the
// ConfigMap's "metadata" key.
func marshalCMDoc(d cmDoc) string {
	b, err := json.Marshal(d)
	if err != nil {
		panic(err) // unreachable: the struct is well-formed
	}
	return string(b)
}

// cmPendingDoc / cmFinalizedDoc / cmRevokedDoc are the authoritative
// documents for the three metadata states the test exercises.
var (
	schemaVer  = 1
	clusterID  = testClusterID
	epochID    = testEpochID
	seedID     = seedIdentity
	everAccept = false
	dataLoss   = false

	cmpBoolT, cmpBoolF = true, false

	cmPendingDoc = cmDoc{
		SchemaVersion:        &schemaVer,
		ClusterID:            &clusterID,
		EpochID:              &epochID,
		Generation:           intPtr(1),
		Initialized:          &cmpBoolF,
		GenesisAuthorized:    &cmpBoolT,
		GenesisConsumer:      &emptyStr,
		EverAcceptedMessages: &everAccept,
		DataLossProtected:    &dataLoss,
	}

	cmFinalizedDoc = cmDoc{
		SchemaVersion:        &schemaVer,
		ClusterID:            &clusterID,
		EpochID:              &epochID,
		Generation:           intPtr(1),
		Initialized:          &cmpBoolT,
		GenesisAuthorized:    &cmpBoolF,
		GenesisConsumer:      &seedID,
		EverAcceptedMessages: &cmpBoolT,
		DataLossProtected:    &cmpBoolT,
	}

	cmRevokedDoc = cmDoc{
		SchemaVersion:        &schemaVer,
		ClusterID:            &clusterID,
		EpochID:              &otherEpoch,
		Generation:           intPtr(2),
		Initialized:          &cmpBoolT,
		GenesisAuthorized:    &cmpBoolF,
		GenesisConsumer:      &otherSeed,
		EverAcceptedMessages: &cmpBoolT,
		DataLossProtected:    &cmpBoolT,
	}
)

var (
	emptyStr   = ""
	otherEpoch = "a-different-epoch"
	otherSeed  = "another-seed"
)

func intPtr(n int) *int { return &n }

// cmBackedSource is a fake Kubernetes ConfigMap that implements
// genesis.ConfigMapUpdater (for the operator's FinalizeOperation CAS) and
// k8smetadata.ConfigMapGetter (for the metadata Provider). It is wrapped in a
// k8smetadata.Provider to provide the lifecycle.MetadataSource interface for
// the controller's metadata polling. Both the Provider and the
// FinalizeOperation observe the same ConfigMap (s.cm), so a finalization CAS
// performed by the operator is visible to the controller on its next Read.
//
// The resourceVersion advances on Update, simulating the API server
// assigning a new revision. A CAS that does not match the current
// resourceVersion returns a typed 409 conflict.
type cmBackedSource struct {
	mu                  sync.Mutex
	cm                  *k8smetadata.ConfigMap
	nextResourceVersion string
	updateCalls         int
}

// newCmBackedSource creates a cmBackedSource with the given document and
// returns a lifecycle.MetadataSource (a k8smetadata.Provider wrapping the
// source) along with the raw source (for UpdateCalls/Document assertions).
func newCmBackedSource(doc cmDoc) (*k8smetadata.Provider, *cmBackedSource) {
	src := &cmBackedSource{
		cm: &k8smetadata.ConfigMap{
			ResourceVersion: "rv-1",
			Data:            map[string]string{k8smetadata.MetadataKey: marshalCMDoc(doc)},
		},
		nextResourceVersion: "rv-2",
	}
	provider, err := k8smetadata.NewProvider(k8smetadata.ProviderConfig{
		Namespace: "gossipgrid",
		Name:      "gossipgrid-cluster-metadata",
	}, src)
	if err != nil {
		panic(err) // unreachable: the config is well-formed
	}
	return provider, src
}

// SetDocument atomically replaces the ConfigMap's document (and advances the
// resourceVersion, simulating an operator or concurrent writer).
func (s *cmBackedSource) SetDocument(doc cmDoc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cm = &k8smetadata.ConfigMap{
		ResourceVersion: s.nextResourceVersion,
		Data:            map[string]string{k8smetadata.MetadataKey: marshalCMDoc(doc)},
	}
	s.nextResourceVersion = advanceRV(s.nextResourceVersion)
}

// Get implements genesis.ConfigMapUpdater.
func (s *cmBackedSource) Get(_ context.Context, _, _ string) (*k8smetadata.ConfigMap, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cm == nil {
		return nil, k8smetadata.NewConfigMapNotFound(errors.New("configmap not found"))
	}
	// Return a shallow copy so the caller cannot mutate the stored object.
	cp := *s.cm
	return &cp, nil
}

// Update implements genesis.ConfigMapUpdater (a resourceVersion CAS).
func (s *cmBackedSource) Update(_ context.Context, _, _, expectedRV string, data map[string]string) (*k8smetadata.ConfigMap, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateCalls++
	if s.cm == nil || s.cm.ResourceVersion != expectedRV {
		return nil, k8smetadata.NewAPIConflict(errors.New("resourceVersion mismatch (409)"))
	}
	updated := &k8smetadata.ConfigMap{ResourceVersion: s.nextResourceVersion, Data: data}
	s.cm = updated
	s.nextResourceVersion = advanceRV(s.nextResourceVersion)
	return updated, nil
}

// UpdateCalls returns the number of Update calls made (for test assertions).
func (s *cmBackedSource) UpdateCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateCalls
}

// ResetUpdateCalls resets the Update call counter to 0 (for test assertions
// that need to count updates in a sub-scenario).
func (s *cmBackedSource) ResetUpdateCalls() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateCalls = 0
}

// Document returns the current ConfigMap's document (for test assertions).
func (s *cmBackedSource) Document() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cm == nil {
		return ""
	}
	return s.cm.Data[k8smetadata.MetadataKey]
}

// advanceRV is a simple resourceVersion advancer (rv-1 → rv-2 → rv-3 → ...).
func advanceRV(rv string) string {
	for i := 1; ; i++ {
		if rv == "rv-"+itoa(i) {
			return "rv-" + itoa(i+1)
		}
	}
}

// itoa is a minimal int-to-string (the resourceVersions in the test are
// small positive integers).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := ""
	for n > 0 {
		digits = string(rune('0'+n%10)) + digits
		n /= 10
	}
	return digits
}

// TestFinalizationToPromotion_Integration is the end-to-end test of the
// actual finalization-to-promotion sequence using the real FinalizeOperation
// and a real SQLite store.
func TestFinalizationToPromotion_Integration(t *testing.T) {
	// Create a real SQLite store in a temp directory.
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "gossipgrid.db")
	st, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("opening real SQLite store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	// The shared ConfigMap-backed source (starts with the PENDING document).
	// The Provider wraps the source as a lifecycle.MetadataSource; the source
	// is also passed directly to the FinalizeOperation as a
	// genesis.ConfigMapUpdater.
	srcMeta, src := newCmBackedSource(cmPendingDoc)

	cfg := lifecycle.Config{
		MetadataPollInterval: 10 * time.Millisecond,
		NodeID:               seedIdentity,
		SeedIdentity:         seedIdentity,
		Enabled:              true,
	}

	// -------------------------------------------------------------------
	// Verification 1: Genesis authorization is pending.
	// -------------------------------------------------------------------
	t.Log("=== Verification 1: Genesis authorization is pending ===")
	meta, err := srcMeta.Read(context.Background())
	if err != nil {
		t.Fatalf("reading metadata: %v", err)
	}
	if meta.Initialized || !meta.GenesisAuthorized || meta.GenesisConsumer != "" || meta.DataLossProtected {
		t.Fatalf("expected a pending authorization (Initialized=false, GenesisAuthorized=true, GenesisConsumer='', DataLossProtected=false); got %+v", meta)
	}

	// -------------------------------------------------------------------
	// Verification 2: The seed binds durably (AuthorizeGenesis).
	// -------------------------------------------------------------------
	t.Log("=== Verification 2: The seed binds durably ===")
	ctrl := lifecycle.NewController(cfg, lifecycle.NewMachine(nil), srcMeta, st, nil)
	if err := ctrl.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ctrl.Snapshot().State == lifecycle.DISCOVERING {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if snap := ctrl.Snapshot(); snap.State != lifecycle.DISCOVERING {
		t.Fatalf("expected DISCOVERING; got %s (reason=%s)", snap.State, snap.Reason)
	}
	if err := ctrl.AuthorizeGenesis(context.Background()); err != nil {
		t.Fatalf("AuthorizeGenesis: %v", err)
	}
	if snap := ctrl.Snapshot(); snap.State != lifecycle.GENESIS {
		t.Fatalf("expected GENESIS after AuthorizeGenesis; got %s (reason=%s)", snap.State, snap.Reason)
	}
	b, err := st.GetBinding()
	if err != nil {
		t.Fatalf("reading binding: %v", err)
	}
	if b == nil || b.ClusterID != testClusterID || b.EpochID != testEpochID {
		t.Fatalf("expected a binding to %s/%s; got %+v", testClusterID, testEpochID, b)
	}
	if d := ctrl.Admission().CheckWrite(context.Background()); d.Allowed {
		t.Fatal("expected writes blocked (GENESIS); got allowed")
	}

	// -------------------------------------------------------------------
	// Verification 3: The operator finalizes using matching identities
	// (the real FinalizeOperation performs a CAS).
	// -------------------------------------------------------------------
	t.Log("=== Verification 3: The operator finalizes (real FinalizeOperation CAS) ===")
	op, err := genesis.NewFinalizeOperation("gossipgrid", src)
	if err != nil {
		t.Fatalf("NewFinalizeOperation: %v", err)
	}
	fcfg := genesis.FinalizeConfig{
		ClusterID:        testClusterID,
		EpochID:          testEpochID,
		GenesisConsumer:  seedIdentity,
		ConfirmSeedBound: true,
	}
	res, err := op.Finalize(context.Background(), fcfg)
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if !res.Finalized {
		t.Fatalf("expected Finalized=true (a fresh CAS); got AlreadyFinalized=%v", res.AlreadyFinalized)
	}
	if src.UpdateCalls() != 1 {
		t.Fatalf("expected exactly 1 Update call; got %d", src.UpdateCalls())
	}
	// The ConfigMap is now the finalized document.
	finalMeta, err := srcMeta.Read(context.Background())
	if err != nil {
		t.Fatalf("reading metadata after finalization: %v", err)
	}
	if !finalMeta.Initialized || finalMeta.GenesisAuthorized || finalMeta.GenesisConsumer != seedIdentity || !finalMeta.DataLossProtected {
		t.Fatalf("expected a coherent finalized record; got %+v", finalMeta)
	}

	// -------------------------------------------------------------------
	// Verification 4: The seed observes the finalized metadata and reaches
	// READY.
	// -------------------------------------------------------------------
	t.Log("=== Verification 4: The seed reaches READY ===")
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if snap := ctrl.Snapshot(); snap.State == lifecycle.READY || snap.State == lifecycle.RECOVERY_REQUIRED {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctrl.EvaluateMetadataForTest(context.Background())
	snap := ctrl.Snapshot()
	if snap.State != lifecycle.READY {
		t.Fatalf("expected READY after finalization; got %s (reason=%s)", snap.State, snap.Reason)
	}
	if d := ctrl.Admission().CheckWrite(context.Background()); !d.Allowed {
		t.Fatalf("expected writes allowed (READY); got denied (reason=%s)", d.Reason)
	}
	ctrl.Stop(context.Background())

	// -------------------------------------------------------------------
	// Verification 5: A mismatched finalization request (wrong ClusterID)
	// never updates the ConfigMap.
	// -------------------------------------------------------------------
	t.Log("=== Verification 5: A mismatched finalization request never updates the ConfigMap ===")
	// Reset the ConfigMap to the PENDING document (a fresh scenario) and reset
	// the Update call counter (Verification 3 already performed 1 CAS).
	src.SetDocument(cmPendingDoc)
	src.ResetUpdateCalls()
	mismatchedCfg := genesis.FinalizeConfig{
		ClusterID:        "a-different-cluster",
		EpochID:          testEpochID,
		GenesisConsumer:  seedIdentity,
		ConfirmSeedBound: true,
	}
	mOp, err := genesis.NewFinalizeOperation("gossipgrid", src)
	if err != nil {
		t.Fatalf("NewFinalizeOperation (mismatched): %v", err)
	}
	if _, err := mOp.Finalize(context.Background(), mismatchedCfg); err == nil {
		t.Fatal("expected an error for a mismatched ClusterID; got nil")
	}
	if src.UpdateCalls() != 0 {
		t.Fatalf("expected 0 Update calls for a mismatched finalization; got %d", src.UpdateCalls())
	}
	// The ConfigMap is unchanged (still the PENDING document).
	unchanged, err := srcMeta.Read(context.Background())
	if err != nil {
		t.Fatalf("reading metadata after mismatched finalization: %v", err)
	}
	if changed := unchanged.Initialized; changed {
		t.Fatal("the ConfigMap was modified by a mismatched finalization")
	}

	// -------------------------------------------------------------------
	// Verification 6: A revoked finalization cannot authorize public writes.
	// -------------------------------------------------------------------
	t.Log("=== Verification 6: A revoked finalization cannot authorize public writes ===")
	// The pending record is replaced by a REVOKED record (a different epoch,
	// Initialized=true, GenesisAuthorized=false) BEFORE the seed attempts
	// AuthorizeGenesis. The seed must NOT enter GENESIS.
	src.SetDocument(cmRevokedDoc)
	ctrl2 := lifecycle.NewController(cfg, lifecycle.NewMachine(nil), srcMeta, st, nil)
	if err := ctrl2.Start(context.Background()); err != nil {
		t.Fatalf("Start (revoked scenario): %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if snap := ctrl2.Snapshot(); snap.State == lifecycle.RECOVERY_REQUIRED || snap.State == lifecycle.GENESIS {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctrl2.EvaluateMetadataForTest(context.Background())
	snap = ctrl2.Snapshot()
	if snap.State == lifecycle.GENESIS {
		t.Fatal("seed entered GENESIS despite the revoked authorization; the pending-grant invariant must be checked before GENESIS entry")
	}
	if snap.State != lifecycle.RECOVERY_REQUIRED {
		t.Fatalf("expected RECOVERY_REQUIRED for a revoked authorization; got %s (reason=%s)", snap.State, snap.Reason)
	}
	if d := ctrl2.Admission().CheckWrite(context.Background()); d.Allowed {
		t.Fatal("expected writes blocked (revoked authorization); got allowed")
	}
	ctrl2.Stop(context.Background())

	// -------------------------------------------------------------------
	// Verification 7: Restart after successful finalization remains safe.
	// -------------------------------------------------------------------
	t.Log("=== Verification 7: Restart after successful finalization ===")
	// Restore the finalized document (the seed was already finalized in
	// Verification 4; this is a fresh controller with the same SQLite DB).
	src.SetDocument(cmFinalizedDoc)
	ctrl3 := lifecycle.NewController(cfg, lifecycle.NewMachine(nil), srcMeta, st, nil)
	if err := ctrl3.Start(context.Background()); err != nil {
		t.Fatalf("Start (restart after finalization): %v", err)
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if snap := ctrl3.Snapshot(); snap.State == lifecycle.READY || snap.State == lifecycle.RECOVERY_REQUIRED {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctrl3.EvaluateMetadataForTest(context.Background())
	snap = ctrl3.Snapshot()
	if snap.State != lifecycle.READY {
		t.Fatalf("expected READY on restart after finalization; got %s (reason=%s)", snap.State, snap.Reason)
	}
	if d := ctrl3.Admission().CheckWrite(context.Background()); !d.Allowed {
		t.Fatalf("expected writes allowed (READY on restart); got denied (reason=%s)", d.Reason)
	}
	ctrl3.Stop(context.Background())
}
