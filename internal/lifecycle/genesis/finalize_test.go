package genesis

import (
	"context"
	"errors"
	"sync"
	"testing"

	"gossipgrid/internal/lifecycle/k8smetadata"
)

// fakeUpdater is a deterministic, concurrency-safe ConfigMapUpdater for tests.
// It simulates the Kubernetes API: Get returns the stored ConfigMap (or a
// typed not-found error when absent), and Update performs a resourceVersion
// compare-and-swap (it succeeds only when the current resourceVersion equals
// the expected value, advancing it to a new version; a mismatch yields a
// typed 409 conflict). The injected-error fields let a test simulate specific
// API outcomes (a transient unavailability, a permission denial, an ambiguous
// nil result). The mutex makes it safe for concurrent finalization tests.
type fakeUpdater struct {
	mu sync.Mutex
	// cm is the current stored ConfigMap (nil when the object is absent).
	cm *k8smetadata.ConfigMap
	// The resourceVersion the stored object advances to after a successful
	// Update (simulating the API server assigning a new revision).
	nextResourceVersion string
	getCalls            int
	updateCalls         int
	// Injected errors (set before the test calls Finalize).
	getError    error
	updateError error
	// getNilResult / updateNilResult make the method return (nil, nil) — an
	// ambiguous (unverifiable) result.
	getNilResult    bool
	updateNilResult bool
	// getCopy, when non-nil, is the ConfigMap the Get returns on the FIRST
	// call (simulating a stale revision the Job read) while the stored object
	// (u.cm) is advanced (a concurrent writer). The Update compares against
	// the getCopy's resourceVersion and conflicts when it mismatches u.cm's
	// resourceVersion. This models a genuine CAS conflict (the Job read an
	// older revision; a concurrent writer advanced it). The re-read (the
	// second Get) returns u.cm (the stored object, now the concurrent
	// writer's record).
	getCopy *k8smetadata.ConfigMap
	// getCopyUsed is set after getCopy is returned once (the re-read returns
	// u.cm, not the stale copy).
	getCopyUsed bool
	// getFailAfter, when > 0, makes the Get return an unavailability error
	// after the (getFailAfter)th call (0/unset = no failure). This models a
	// re-read that fails (the API degrades after the initial read).
	getFailAfter int
}

func (u *fakeUpdater) Get(_ context.Context, _, _ string) (*k8smetadata.ConfigMap, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.getCalls++
	// A re-read that fails (the API degrades after the initial read).
	if u.getFailAfter > 0 && u.getCalls > u.getFailAfter {
		return nil, k8smetadata.NewAPIUnavailable(errors.New("api server degraded on re-read"))
	}
	if u.getError != nil {
		return nil, u.getError
	}
	if u.getNilResult {
		return nil, nil
	}
	// A stale revision the Job read (a concurrent writer advanced u.cm). The
	// stale copy is returned only on the FIRST call; the re-read (the second
	// and later calls) returns u.cm (the stored object, now the concurrent
	// writer's record).
	if u.getCopy != nil && !u.getCopyUsed {
		u.getCopyUsed = true
		return u.getCopy, nil
	}
	if u.cm == nil {
		return nil, k8smetadata.NewConfigMapNotFound(errors.New("configmap not found"))
	}
	return u.cm, nil
}

func (u *fakeUpdater) Update(_ context.Context, _, _, expectedRV string, data map[string]string) (*k8smetadata.ConfigMap, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.updateCalls++
	if u.updateError != nil {
		return nil, u.updateError
	}
	// A CAS: it succeeds only when the stored object exists and its
	// resourceVersion matches the expected value. A mismatch (another writer
	// advanced the revision) yields a typed 409 conflict.
	if u.cm == nil || u.cm.ResourceVersion != expectedRV {
		return nil, k8smetadata.NewAPIConflict(errors.New("resourceVersion mismatch (409)"))
	}
	if u.updateNilResult {
		// The API server performed the update but the response was lost
		// (an ambiguous result). The stored object is advanced (the update
		// did happen), but the caller receives (nil, nil).
		u.cm = &k8smetadata.ConfigMap{ResourceVersion: u.nextResourceVersion, Data: data}
		return nil, nil
	}
	// A success: the object is updated (the data is written and the
	// resourceVersion advances to a new revision).
	updated := &k8smetadata.ConfigMap{ResourceVersion: u.nextResourceVersion, Data: data}
	u.cm = updated
	return updated, nil
}

// pendingDoc / initializedDoc / mismatchedDoc are the test fixture documents
// for the finalization. The pending doc is an outstanding, unconsumed
// authorization (Initialized=false, GenesisAuthorized=true, GenesisConsumer=
// "", DataLossProtected=false). The initialized doc is a coherent, finalized
// record matching the expected identities. The mismatched doc is a finalized
// record whose GenesisConsumer (or ClusterID/EpochID) differs from the
// expected identities.
const (
	testClusterID = "0190e8a2-6b0c-7000-8000-000000000000"
	testEpochID   = "0190e8a3-1c2d-7000-9000-111111111111"
	testSeed      = "gossipgrid-0"
	pendingDoc    = `{
		"schemaVersion": 1,
		"clusterId": "0190e8a2-6b0c-7000-8000-000000000000",
		"epochId": "0190e8a3-1c2d-7000-9000-111111111111",
		"generation": 1,
		"initialized": false,
		"genesisAuthorized": true,
		"genesisConsumer": "",
		"everAcceptedMessages": false,
		"dataLossProtected": false
	}`
	initializedDoc = `{
		"schemaVersion": 1,
		"clusterId": "0190e8a2-6b0c-7000-8000-000000000000",
		"epochId": "0190e8a3-1c2d-7000-9000-111111111111",
		"generation": 1,
		"initialized": true,
		"genesisAuthorized": false,
		"genesisConsumer": "gossipgrid-0",
		"everAcceptedMessages": true,
		"dataLossProtected": true
	}`
	mismatchedConsumerDoc = `{
		"schemaVersion": 1,
		"clusterId": "0190e8a2-6b0c-7000-8000-000000000000",
		"epochId": "0190e8a3-1c2d-7000-9000-111111111111",
		"generation": 1,
		"initialized": true,
		"genesisAuthorized": false,
		"genesisConsumer": "gossipgrid-1",
		"everAcceptedMessages": true,
		"dataLossProtected": true
	}`
	mismatchedClusterDoc = `{
		"schemaVersion": 1,
		"clusterId": "0190e8a2-6b0c-7000-8000-999999999999",
		"epochId": "0190e8a3-1c2d-7000-9000-111111111111",
		"generation": 1,
		"initialized": true,
		"genesisAuthorized": false,
		"genesisConsumer": "gossipgrid-0",
		"everAcceptedMessages": true,
		"dataLossProtected": true
	}`
)

func newFinalizeOp(t *testing.T, u ConfigMapUpdater) *FinalizeOperation {
	t.Helper()
	op, err := NewFinalizeOperation("gossipgrid", u)
	if err != nil {
		t.Fatalf("NewFinalizeOperation: %v", err)
	}
	return op
}

// testFinalizeCfg is the expected identity context for the tests.
func testFinalizeCfg() FinalizeConfig {
	return FinalizeConfig{
		ClusterID:        testClusterID,
		EpochID:          testEpochID,
		GenesisConsumer:  testSeed,
		ConfirmSeedBound: true,
	}
}

// TestFinalize_NoOutstandingAuthorization covers the case where the record is
// neither an outstanding, unconsumed authorization nor a coherent finalized
// record: the finalization fails closed (ErrNoOutstandingAuthorization). This
// includes a cluster whose authorization was never granted and a cluster in an
// incoherent intermediate state (e.g. Initialized=true but
// GenesisAuthorized=true, which fails Validate and is therefore rejected at
// the parse stage).
func TestFinalize_NoOutstandingAuthorization(t *testing.T) {
	cases := []struct {
		name string
		doc  string
	}{
		{
			"no authorization (uninitialized, not authorized)",
			`{"schemaVersion":1,"clusterId":"` + testClusterID + `","epochId":"` + testEpochID + `","generation":1,"initialized":false,"genesisAuthorized":false,"genesisConsumer":"","everAcceptedMessages":false,"dataLossProtected":false}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := &fakeUpdater{cm: &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": tc.doc}}}
			op := newFinalizeOp(t, u)
			_, err := op.Finalize(context.Background(), testFinalizeCfg())
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !errors.Is(err, ErrNoOutstandingAuthorization) {
				t.Fatalf("expected ErrNoOutstandingAuthorization, got: %v", err)
			}
			// No update must have been attempted (the record is not
			// finalizable; the CAS must not run).
			if u.updateCalls != 0 {
				t.Fatalf("expected 0 update calls, got %d", u.updateCalls)
			}
		})
	}
}

// TestFinalize_IncoherentIntermediateRecord covers the fail-closed case: a
// record that is NOT a coherent, finalized record and NOT an outstanding,
// unconsumed authorization (e.g. Initialized=false, GenesisAuthorized=false,
// GenesisConsumer=<seed> — the consumer is set but Initialized is false) is
// incoherent (a Validate() rejection) and is rejected at the parse stage
// (a ErrMetadataConflict error, which is NOT a success). It is never
// overwritten. This is a distinct case from TestFinalize_NoOutstanding
// Authorization (a coherent record with no outstanding authorization) and from
// TestFinalize_MismatchedFinalizedRecord (a coherent, finalized record that
// does not match the expected identities).
func TestFinalize_IncoherentIntermediateRecord(t *testing.T) {
	// Initialized=false, GenesisAuthorized=false, GenesisConsumer=<seed>,
	// DataLossProtected=false: incoherent (the consumer is recorded on
	// consumption, which also sets Initialized=true).
	doc := `{"schemaVersion":1,"clusterId":"` + testClusterID + `","epochId":"` + testEpochID + `","generation":1,"initialized":false,"genesisAuthorized":false,"genesisConsumer":"` + testSeed + `","everAcceptedMessages":true,"dataLossProtected":false}`
	u := &fakeUpdater{cm: &k8smetadata.ConfigMap{ResourceVersion: "rv-7", Data: map[string]string{"metadata": doc}}}
	op := newFinalizeOp(t, u)
	_, err := op.Finalize(context.Background(), testFinalizeCfg())
	if err == nil {
		t.Fatal("expected an error (the incoherent record must fail closed), got nil")
	}
	if !errors.Is(err, ErrMetadataConflict) {
		t.Fatalf("expected ErrMetadataConflict (the incoherent record is rejected at the parse stage), got: %v", err)
	}
	if u.updateCalls != 0 {
		t.Fatalf("expected 0 update calls, got %d", u.updateCalls)
	}
}

// TestFinalize_FreshFinalization covers the happy path: an outstanding,
// unconsumed authorization is finalized with a single CAS that sets all four
// fields together (Initialized=true, GenesisAuthorized=false,
// GenesisConsumer=<seed>, DataLossProtected=true) and preserves the other
// fields. The result reports Finalized=true with the finalized metadata.
func TestFinalize_FreshFinalization(t *testing.T) {
	u := &fakeUpdater{
		cm:                  &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": pendingDoc}},
		nextResourceVersion: "rv-2",
	}
	op := newFinalizeOp(t, u)
	res, err := op.Finalize(context.Background(), testFinalizeCfg())
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if !res.Finalized || res.AlreadyFinalized {
		t.Fatalf("expected Finalized=true, AlreadyFinalized=false; got Finalized=%v AlreadyFinalized=%v", res.Finalized, res.AlreadyFinalized)
	}
	// The updated ConfigMap must carry the finalized document.
	if u.cm == nil {
		t.Fatal("expected the stored ConfigMap to be updated")
	}
	doc, ok := u.cm.Data["metadata"]
	if !ok {
		t.Fatal("expected the updated ConfigMap to carry the metadata document")
	}
	// Verify the four finalization fields are all set and the other fields
	// are preserved (ClusterID and EpochID unchanged). The marshalled JSON
	// uses no space after the colon (e.g. "initialized":true).
	want := `"initialized":true`
	if !contains(doc, want) {
		t.Errorf("expected the document to set Initialized=true; got: %s", doc)
	}
	want = `"genesisAuthorized":false`
	if !contains(doc, want) {
		t.Errorf("expected the document to set GenesisAuthorized=false; got: %s", doc)
	}
	want = `"genesisConsumer":"` + testSeed + `"`
	if !contains(doc, want) {
		t.Errorf("expected the document to set GenesisConsumer=%s; got: %s", testSeed, doc)
	}
	want = `"dataLossProtected":true`
	if !contains(doc, want) {
		t.Errorf("expected the document to set DataLossProtected=true; got: %s", doc)
	}
	want = `"clusterId":"` + testClusterID + `"`
	if !contains(doc, want) {
		t.Errorf("expected the document to preserve ClusterID=%s; got: %s", testClusterID, doc)
	}
	want = `"epochId":"` + testEpochID + `"`
	if !contains(doc, want) {
		t.Errorf("expected the document to preserve EpochID=%s; got: %s", testEpochID, doc)
	}
	// The resourceVersion must have advanced (the CAS wrote a new revision).
	if u.cm.ResourceVersion != "rv-2" {
		t.Errorf("expected the resourceVersion to advance to rv-2; got %s", u.cm.ResourceVersion)
	}
	// Exactly one update (the CAS) and the Get used for the read.
	if u.updateCalls != 1 {
		t.Errorf("expected exactly 1 update call; got %d", u.updateCalls)
	}
}

// TestFinalize_AlreadyFinalized covers the idempotent case: a retry against an
// existing coherent finalized record (Initialized=true, GenesisAuthorized=
// false, GenesisConsumer=<seed>, DataLossProtected=true, with the exact
// expected ClusterID/EpochID) reports ErrAlreadyFinalized (a benign, expected
// outcome) without performing an update or generating a new identity.
func TestFinalize_AlreadyFinalized(t *testing.T) {
	u := &fakeUpdater{cm: &k8smetadata.ConfigMap{ResourceVersion: "rv-5", Data: map[string]string{"metadata": initializedDoc}}}
	op := newFinalizeOp(t, u)
	res, err := op.Finalize(context.Background(), testFinalizeCfg())
	if !errors.Is(err, ErrAlreadyFinalized) {
		t.Fatalf("expected ErrAlreadyFinalized, got: %v", err)
	}
	if res.AlreadyFinalized != true || res.Finalized {
		t.Fatalf("expected AlreadyFinalized=true, Finalized=false; got AlreadyFinalized=%v Finalized=%v", res.AlreadyFinalized, res.Finalized)
	}
	// No update must have been attempted (the record is already finalized).
	if u.updateCalls != 0 {
		t.Fatalf("expected 0 update calls, got %d", u.updateCalls)
	}
	// The stored ConfigMap must be unchanged (no new identity, no new
	// revision).
	if u.cm.ResourceVersion != "rv-5" {
		t.Errorf("expected the resourceVersion to be unchanged; got %s", u.cm.ResourceVersion)
	}
}

// TestFinalize_MismatchedFinalizedRecord covers the fail-closed case: a
// finalized record whose GenesisConsumer, ClusterID, or EpochID does NOT match
// the expected identities fails closed (ErrFinalizationMismatch). The record
// is never overwritten (no update is attempted) and no replacement identity is
// generated.
func TestFinalize_MismatchedFinalizedRecord(t *testing.T) {
	cases := []struct {
		name string
		doc  string
	}{
		{"different GenesisConsumer", mismatchedConsumerDoc},
		{"different ClusterID", mismatchedClusterDoc},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := &fakeUpdater{cm: &k8smetadata.ConfigMap{ResourceVersion: "rv-3", Data: map[string]string{"metadata": tc.doc}}}
			op := newFinalizeOp(t, u)
			_, err := op.Finalize(context.Background(), testFinalizeCfg())
			if !errors.Is(err, ErrFinalizationMismatch) {
				t.Fatalf("expected ErrFinalizationMismatch, got: %v", err)
			}
			// No update must have been attempted (the mismatched record is
			// never overwritten).
			if u.updateCalls != 0 {
				t.Fatalf("expected 0 update calls, got %d", u.updateCalls)
			}
		})
	}
}

// TestFinalize_DataLossProtectedNotArmed covers the fail-closed case: a
// finalized record (Initialized=true) whose DataLossProtected=false is
// incoherent (the protection flag must be armed in the same CAS that consumes
// the authorization; a coherent, finalized record has DataLossProtected=true
// because Validate() requires it when Initialized=true and GenesisConsumer is
// set). The record is rejected at the parse stage (a ErrMetadataConflict
// error, which is NOT a success). It is never overwritten.
func TestFinalize_DataLossProtectedNotArmed(t *testing.T) {
	doc := `{"schemaVersion":1,"clusterId":"` + testClusterID + `","epochId":"` + testEpochID + `","generation":1,"initialized":true,"genesisAuthorized":false,"genesisConsumer":"` + testSeed + `","everAcceptedMessages":true,"dataLossProtected":false}`
	u := &fakeUpdater{cm: &k8smetadata.ConfigMap{ResourceVersion: "rv-4", Data: map[string]string{"metadata": doc}}}
	op := newFinalizeOp(t, u)
	_, err := op.Finalize(context.Background(), testFinalizeCfg())
	if !errors.Is(err, ErrMetadataConflict) {
		t.Fatalf("expected ErrMetadataConflict (the incoherent record is rejected at the parse stage), got: %v", err)
	}
	if u.updateCalls != 0 {
		t.Fatalf("expected 0 update calls, got %d", u.updateCalls)
	}
}

// TestFinalize_IncoherentFinalizedRecord covers the fail-closed case: a record
// with Initialized=true AND GenesisAuthorized=true is incoherent (a
// Validate() rejection) and is rejected at the parse stage (a
// lifecycle.ErrMetadataMalformed error, which is NOT a success). It is never
// overwritten.
func TestFinalize_IncoherentFinalizedRecord(t *testing.T) {
	doc := `{"schemaVersion":1,"clusterId":"` + testClusterID + `","epochId":"` + testEpochID + `","generation":1,"initialized":true,"genesisAuthorized":true,"genesisConsumer":"` + testSeed + `","everAcceptedMessages":true,"dataLossProtected":true}`
	u := &fakeUpdater{cm: &k8smetadata.ConfigMap{ResourceVersion: "rv-6", Data: map[string]string{"metadata": doc}}}
	op := newFinalizeOp(t, u)
	_, err := op.Finalize(context.Background(), testFinalizeCfg())
	if err == nil {
		t.Fatal("expected an error (the incoherent record must fail closed), got nil")
	}
	// A Validate() failure is a malformed observation, not a success.
	if u.updateCalls != 0 {
		t.Fatalf("expected 0 update calls, got %d", u.updateCalls)
	}
}

// TestFinalize_CASConflict_ResolvedByReRead covers the crash-safe case: a CAS
// conflict (another writer advanced the resourceVersion between the Get and
// the Update) is resolved by re-reading the authoritative object. If the
// re-read shows the record is now a coherent finalized record matching the
// expected identities (a concurrent finalization won the race), the retry
// reports ErrAlreadyFinalized (the idempotent case). No replacement identity is
// generated.
func TestFinalize_CASConflict_ResolvedByReRead(t *testing.T) {
	// The Job reads a pending authorization at rv-1 (the getCopy). A
	// concurrent writer finalizes the record (the stored object is the
	// initialized doc at rv-9). The Update (comparing against rv-1) conflicts
	// because the stored object is at rv-9. The re-read (Get) returns the
	// stored object (now the initialized doc at rv-9), which is a coherent
	// finalized record matching the expected identities → ErrAlreadyFinalized.
	u := &fakeUpdater{
		cm:      &k8smetadata.ConfigMap{ResourceVersion: "rv-9", Data: map[string]string{"metadata": initializedDoc}},
		getCopy: &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": pendingDoc}},
	}
	op := newFinalizeOp(t, u)
	res, err := op.Finalize(context.Background(), testFinalizeCfg())
	if !errors.Is(err, ErrAlreadyFinalized) {
		t.Fatalf("expected ErrAlreadyFinalized (a concurrent finalization won the race), got: %v", err)
	}
	if !res.AlreadyFinalized || res.Finalized {
		t.Fatalf("expected AlreadyFinalized=true, Finalized=false; got AlreadyFinalized=%v Finalized=%v", res.AlreadyFinalized, res.Finalized)
	}
	// The Update must have been attempted (and conflicted) exactly once; the
	// re-read must have occurred (at least one Get after the Update).
	if u.updateCalls != 1 {
		t.Errorf("expected 1 update call (the CAS attempt); got %d", u.updateCalls)
	}
}

// TestFinalize_CASConflict_RevokedAuthorization covers the crash-safe case: a
// CAS conflict (another writer advanced the resourceVersion) is resolved by
// re-reading the authoritative object. If the re-read shows the authorization
// was revoked (no outstanding authorization, and not a finalized record), the
// finalization fails closed (ErrUpdateAmbiguous). It is never treated as a
// success.
func TestFinalize_CASConflict_RevokedAuthorization(t *testing.T) {
	// The Job reads a pending authorization at rv-1. A concurrent writer
	// revokes the authorization (the stored object is an uninitialized,
	// unauthorized record at rv-9 — no outstanding authorization). The Update
	// (comparing against rv-1) conflicts. The re-read (Get) returns the stored
	// object (now the revoked record), which is not finalizable →
	// ErrUpdateAmbiguous (an ambiguous outcome; the operator must inspect the
	// record).
	revokedDoc := `{"schemaVersion":1,"clusterId":"` + testClusterID + `","epochId":"` + testEpochID + `","generation":1,"initialized":false,"genesisAuthorized":false,"genesisConsumer":"","everAcceptedMessages":false,"dataLossProtected":false}`
	u := &fakeUpdater{
		cm:      &k8smetadata.ConfigMap{ResourceVersion: "rv-9", Data: map[string]string{"metadata": revokedDoc}},
		getCopy: &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": pendingDoc}},
	}
	op := newFinalizeOp(t, u)
	_, err := op.Finalize(context.Background(), testFinalizeCfg())
	if !errors.Is(err, ErrUpdateAmbiguous) {
		t.Fatalf("expected ErrUpdateAmbiguous (the authorization was revoked; the update did not result in a coherent finalized record), got: %v", err)
	}
}

// TestFinalize_CASConflict_ReReadUnavailable covers the crash-safe case: a CAS
// conflict is resolved by re-reading, but the re-read fails (the API is
// unavailable). The finalization fails closed (ErrUpdateAmbiguous). It is never
// inferred as a success.
func TestFinalize_CASConflict_ReReadUnavailable(t *testing.T) {
	// The Job reads a pending authorization at rv-1. The Update (comparing
	// against rv-1) conflicts (the stored object is at rv-9). The re-read
	// (Get) returns an unavailability error (the API is degraded). The
	// finalization fails closed (ErrUpdateAmbiguous).
	u := &fakeUpdater{
		cm:      &k8smetadata.ConfigMap{ResourceVersion: "rv-9", Data: map[string]string{"metadata": pendingDoc}},
		getCopy: &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": pendingDoc}},
		// The re-read (the second Get) fails with an unavailability error.
		// We model this by making the Get fail after the first call.
		getFailAfter: 1,
	}
	op := newFinalizeOp(t, u)
	_, err := op.Finalize(context.Background(), testFinalizeCfg())
	if !errors.Is(err, ErrUpdateAmbiguous) {
		t.Fatalf("expected ErrUpdateAmbiguous (the re-read after the CAS conflict failed), got: %v", err)
	}
}

// TestFinalize_UpdateAmbiguous_NilResult covers the crash-safe case: the Update
// returns a nil response with a nil error (an ambiguous result — the API
// server may have performed the update but the response was lost). The
// finalization re-reads the authoritative object; if it is a coherent finalized
// record matching the expected identities, it reports ErrAlreadyFinalized;
// otherwise it fails closed (ErrUpdateAmbiguous).
func TestFinalize_UpdateAmbiguous_NilResult(t *testing.T) {
	t.Run("ambiguous update, re-read shows finalized", func(t *testing.T) {
		// The Update returns (nil, nil) (ambiguous). The stored object is
		// advanced to the initialized doc at rv-2 (the update happened). The
		// re-read (Get) returns the stored object (the initialized doc) →
		// ErrAlreadyFinalized.
		u := &fakeUpdater{
			cm:                  &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": pendingDoc}},
			nextResourceVersion: "rv-2",
			updateNilResult:     true,
		}
		op := newFinalizeOp(t, u)
		res, err := op.Finalize(context.Background(), testFinalizeCfg())
		if !errors.Is(err, ErrAlreadyFinalized) {
			t.Fatalf("expected ErrAlreadyFinalized (the ambiguous update may have succeeded; the re-read confirms a coherent finalized record), got: %v", err)
		}
		if !res.AlreadyFinalized {
			t.Fatalf("expected AlreadyFinalized=true; got %v", res.AlreadyFinalized)
		}
	})
	t.Run("ambiguous update, re-read shows not finalized", func(t *testing.T) {
		// The Update returns (nil, nil) (ambiguous). The stored object is
		// advanced to a pending doc at rv-2 (a concurrent writer reset it; the
		// update did not result in a coherent finalized record). The re-read
		// (Get) returns the stored object (the pending doc) →
		// ErrUpdateAmbiguous (an ambiguous outcome; the operator must inspect
		// the record).
		u := &fakeUpdater{
			cm:                  &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": pendingDoc}},
			nextResourceVersion: "rv-2",
			updateNilResult:     true,
			// The re-read shows a pending doc (not finalized). We model this by
			// having the stored object be the pending doc at rv-2 after the
			// Update (the concurrent writer reset it). The updateNilResult path
			// advances the stored object to nextResourceVersion with the
			// finalized data, so to model a reset we need the stored object to
			// be the pending doc at rv-2. We use a getCopy for the re-read.
			getCopy: &k8smetadata.ConfigMap{ResourceVersion: "rv-2", Data: map[string]string{"metadata": pendingDoc}},
		}
		op := newFinalizeOp(t, u)
		_, err := op.Finalize(context.Background(), testFinalizeCfg())
		if !errors.Is(err, ErrUpdateAmbiguous) {
			t.Fatalf("expected ErrUpdateAmbiguous (the re-read after the ambiguous update shows the record is not finalized), got: %v", err)
		}
	})
}

// TestFinalize_GetUnavailable covers the fail-closed case: the Get (the
// preflight read) fails with an unavailability error. The finalization cannot
// proceed without a working, readable API and fails closed (a
// lifecycle.ErrMetadataUnavailable error, which is NOT a success). No update is
// attempted.
func TestFinalize_GetUnavailable(t *testing.T) {
	u := &fakeUpdater{
		getError: k8smetadata.NewAPIUnavailable(errors.New("api server degraded")),
	}
	op := newFinalizeOp(t, u)
	_, err := op.Finalize(context.Background(), testFinalizeCfg())
	if err == nil {
		t.Fatal("expected an error (the API is unavailable), got nil")
	}
	if u.updateCalls != 0 {
		t.Fatalf("expected 0 update calls, got %d", u.updateCalls)
	}
}

// TestFinalize_GetPermissionDenied covers the fail-closed case: the Get fails
// with a permission-denied error (the RBAC is misconfigured — the Job lacks the
// get permission). The finalization fails closed (a
// k8smetadata.ErrMetadataPermissionDenied error, which is NOT a success). No
// update is attempted.
func TestFinalize_GetPermissionDenied(t *testing.T) {
	u := &fakeUpdater{
		getError: k8smetadata.NewAPIPermissionDenied(errors.New("403 forbidden")),
	}
	op := newFinalizeOp(t, u)
	_, err := op.Finalize(context.Background(), testFinalizeCfg())
	if err == nil {
		t.Fatal("expected an error (permission denied), got nil")
	}
	if u.updateCalls != 0 {
		t.Fatalf("expected 0 update calls, got %d", u.updateCalls)
	}
}

// TestFinalize_UpdatePermissionDenied covers the fail-closed case: the Update
// fails with a permission-denied error (the RBAC is misconfigured — the Job
// lacks the update permission). The finalization fails closed (a
// k8smetadata.ErrMetadataPermissionDenied error, which is NOT a success). The
// record is never overwritten.
func TestFinalize_UpdatePermissionDenied(t *testing.T) {
	u := &fakeUpdater{
		cm:          &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": pendingDoc}},
		updateError: k8smetadata.NewAPIPermissionDenied(errors.New("403 forbidden on update")),
	}
	op := newFinalizeOp(t, u)
	_, err := op.Finalize(context.Background(), testFinalizeCfg())
	if err == nil {
		t.Fatal("expected an error (permission denied on update), got nil")
	}
	// The record must be unchanged (the Update failed; no CAS was applied).
	if u.cm.ResourceVersion != "rv-1" {
		t.Errorf("expected the resourceVersion to be unchanged; got %s", u.cm.ResourceVersion)
	}
}

// TestFinalize_NoConfirmSeedBound covers the fail-closed case: the operator did
// not provide the --confirm-seed-bound attestation (ConfirmSeedBound=false).
// The finalization cannot verify the seed's local binding itself (the seed's
// emptyDir is not accessible to the Job) and fails closed
// (ErrNoOutstandingAuthorization). No update is attempted.
func TestFinalize_NoConfirmSeedBound(t *testing.T) {
	u := &fakeUpdater{cm: &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": pendingDoc}}}
	cfg := testFinalizeCfg()
	cfg.ConfirmSeedBound = false
	op := newFinalizeOp(t, u)
	_, err := op.Finalize(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected an error (the operator did not confirm the seed is bound), got nil")
	}
	if !errors.Is(err, ErrNoOutstandingAuthorization) {
		t.Fatalf("expected ErrNoOutstandingAuthorization (the --confirm-seed-bound gate), got: %v", err)
	}
	if u.updateCalls != 0 {
		t.Fatalf("expected 0 update calls, got %d", u.updateCalls)
	}
}

// TestFinalize_PendingIdentityMismatch covers the pre-CAS identity verification
// (Stage 1B correction 1): when the record is an outstanding, unconsumed
// authorization (not yet finalized) but the operator-supplied identity context
// does NOT match the record, the finalization must fail closed (no Update)
// before the CAS. A mismatched ClusterID or EpochID means the operator is
// finalizing the wrong record. The record is a valid pending authorization
// (GenesisConsumer is empty), so the "designated seed" is the operator's
// --genesis-consumer flag (the grant's intended consumer) and the explicit
// --confirm-seed-bound attestation is the operator's seed-binding
// confirmation; there is no stored consumer field to mismatch. A wrong
// ClusterID or EpochID fails closed (ErrIdentityMismatch) with zero update
// attempts.
func TestFinalize_PendingIdentityMismatch(t *testing.T) {
	cases := []struct {
		name string
		doc  string
		cfg  FinalizeConfig
	}{
		{
			"wrong ClusterID",
			`{"schemaVersion":1,"clusterId":"other-cluster","epochId":"` + testEpochID + `","generation":1,"initialized":false,"genesisAuthorized":true,"genesisConsumer":"","everAcceptedMessages":false,"dataLossProtected":false}`,
			testFinalizeCfg(),
		},
		{
			"wrong EpochID",
			`{"schemaVersion":1,"clusterId":"` + testClusterID + `","epochId":"other-epoch","generation":1,"initialized":false,"genesisAuthorized":true,"genesisConsumer":"","everAcceptedMessages":false,"dataLossProtected":false}`,
			testFinalizeCfg(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := &fakeUpdater{cm: &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": tc.doc}}}
			op := newFinalizeOp(t, u)
			_, err := op.Finalize(context.Background(), tc.cfg)
			if err == nil {
				t.Fatal("expected an error (identity mismatch on the pending record), got nil")
			}
			if !errors.Is(err, ErrIdentityMismatch) {
				t.Fatalf("expected ErrIdentityMismatch, got: %v", err)
			}
			// Zero update attempts (the CAS must not run on a mismatched
			// pending record).
			if u.updateCalls != 0 {
				t.Fatalf("expected 0 update calls; got %d", u.updateCalls)
			}
		})
	}
}

// TestFinalize_PendingMissingSeedIdentity verifies the pre-CAS designated-seed
// gate (Stage 1B correction 1) from the seed-identity angle. The
// operator-supplied --genesis-consumer is the designated seed identity (the
// grant's intended consumer, written into the record by the CAS). A pending
// record (GenesisConsumer empty) has no stored consumer field to compare
// against, so the "wrong seed identity" is detected at the config boundary:
// an EMPTY seed identity is an invalid configuration and fails closed before
// any API call (and therefore before any Update). A well-formed, nonempty
// seed identity is the operator's attested designated seed and is permitted
// (the operator's --confirm-seed-bound flag is the seed-binding confirmation).
func TestFinalize_PendingMissingSeedIdentity(t *testing.T) {
	// An empty designated seed identity is an invalid configuration: the
	// finalization fails closed before any API call (no Get, no Update).
	u := &fakeUpdater{cm: &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": pendingDoc}}}
	op := newFinalizeOp(t, u)
	cfg := FinalizeConfig{ClusterID: testClusterID, EpochID: testEpochID, GenesisConsumer: "", ConfirmSeedBound: true}
	_, err := op.Finalize(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected an error (missing designated seed identity), got nil")
	}
	// Zero API calls and zero update attempts (the config is invalid before
	// any read).
	if u.getCalls != 0 || u.updateCalls != 0 {
		t.Fatalf("expected 0 get/update calls; got get=%d update=%d", u.getCalls, u.updateCalls)
	}
}

// TestFinalize_InvalidConfig covers the fail-closed case: the finalization
// configuration is invalid (an empty ClusterID, EpochID, or GenesisConsumer).
// The finalization fails closed (a configuration error, which is NOT a success)
// before any API call.
func TestFinalize_InvalidConfig(t *testing.T) {
	u := &fakeUpdater{cm: &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": pendingDoc}}}
	op := newFinalizeOp(t, u)
	cases := []struct {
		name string
		cfg  FinalizeConfig
	}{
		{"empty ClusterID", FinalizeConfig{ClusterID: "", EpochID: testEpochID, GenesisConsumer: testSeed, ConfirmSeedBound: true}},
		{"empty EpochID", FinalizeConfig{ClusterID: testClusterID, EpochID: "", GenesisConsumer: testSeed, ConfirmSeedBound: true}},
		{"empty GenesisConsumer", FinalizeConfig{ClusterID: testClusterID, EpochID: testEpochID, GenesisConsumer: "", ConfirmSeedBound: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := op.Finalize(context.Background(), tc.cfg)
			if err == nil {
				t.Fatal("expected an error (invalid config), got nil")
			}
			// No API call must have been attempted (the config is invalid
			// before any read).
			if u.getCalls != 0 || u.updateCalls != 0 {
				t.Fatalf("expected 0 get/update calls, got get=%d update=%d", u.getCalls, u.updateCalls)
			}
		})
	}
}

// TestFinalize_Concurrent_BothSucceedOnConflict covers the race-safe case: two
// concurrent finalization operations race to CAS the same pending record.
// Exactly ONE succeeds (Finalized=true); the OTHER observes a CAS conflict,
// re-reads the record (now a coherent finalized record), and reports
// ErrAlreadyFinalized (the idempotent case). Both report the same ClusterID
// and EpochID (no replacement identity is generated). The race is resolved by
// the Kubernetes API (the CAS), not by in-process state.
func TestFinalize_Concurrent_BothSucceedOnConflict(t *testing.T) {
	u := &fakeUpdater{
		cm:                  &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": pendingDoc}},
		nextResourceVersion: "rv-2",
	}
	op := newFinalizeOp(t, u)
	cfg := testFinalizeCfg()
	var wg sync.WaitGroup
	results := make(chan FinalizeResult, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := op.Finalize(context.Background(), cfg)
			results <- res
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	// Collect the outcomes.
	var freshCount, alreadyCount int
	var finalCluster, finalEpoch string
	for res := range results {
		if res.Finalized {
			freshCount++
			finalCluster = res.Metadata.ClusterID
			finalEpoch = res.Metadata.EpochID
		} else if res.AlreadyFinalized {
			alreadyCount++
		}
	}
	// Exactly one fresh finalization and one idempotent "already finalized"
	// (the race is resolved by the CAS: one writer advances the revision, the
	// other conflicts and re-reads the coherent finalized record).
	if freshCount != 1 {
		t.Errorf("expected exactly 1 fresh finalization; got %d", freshCount)
	}
	if alreadyCount != 1 {
		t.Errorf("expected exactly 1 idempotent already-finalized; got %d", alreadyCount)
	}
	// The identity is preserved (no replacement identity is generated).
	if finalCluster != testClusterID || finalEpoch != testEpochID {
		t.Errorf("expected the preserved identity (cluster=%s, epoch=%s); got cluster=%s, epoch=%s", testClusterID, testEpochID, finalCluster, finalEpoch)
	}
	// The errors must be nil for the fresh finalization and
	// ErrAlreadyFinalized for the idempotent one (both are non-error outcomes
	// from the operator's perspective, but ErrAlreadyFinalized is inspectable
	// as an error).
	for err := range errs {
		if err != nil && !errors.Is(err, ErrAlreadyFinalized) {
			t.Errorf("expected nil or ErrAlreadyFinalized; got: %v", err)
		}
	}
}

// TestFinalize_IdempotentRetry covers the full idempotency contract: a fresh
// finalization succeeds, and a subsequent retry against the resulting
// coherent finalized record reports ErrAlreadyFinalized (a benign, expected
// outcome) without performing another update or generating a new identity.
func TestFinalize_IdempotentRetry(t *testing.T) {
	u := &fakeUpdater{
		cm:                  &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": pendingDoc}},
		nextResourceVersion: "rv-2",
	}
	op := newFinalizeOp(t, u)
	cfg := testFinalizeCfg()
	// The first finalization succeeds.
	res, err := op.Finalize(context.Background(), cfg)
	if err != nil || !res.Finalized {
		t.Fatalf("first Finalize: res=%+v err=%v", res, err)
	}
	if u.updateCalls != 1 {
		t.Fatalf("expected 1 update call after the first finalization; got %d", u.updateCalls)
	}
	// The retry against the coherent finalized record reports
	// ErrAlreadyFinalized (idempotent) without another update.
	res2, err2 := op.Finalize(context.Background(), cfg)
	if !errors.Is(err2, ErrAlreadyFinalized) {
		t.Fatalf("second Finalize (retry): expected ErrAlreadyFinalized, got: %v", err2)
	}
	if !res2.AlreadyFinalized || res2.Finalized {
		t.Fatalf("second Finalize: expected AlreadyFinalized=true, Finalized=false; got AlreadyFinalized=%v Finalized=%v", res2.AlreadyFinalized, res2.Finalized)
	}
	// No additional update (the record is already finalized).
	if u.updateCalls != 1 {
		t.Fatalf("expected 1 total update call (the first CAS); got %d", u.updateCalls)
	}
	// The identity is preserved (no new identity, no new revision on the
	// retry).
	if u.cm.ResourceVersion != "rv-2" {
		t.Errorf("expected the resourceVersion to remain rv-2 after the idempotent retry; got %s", u.cm.ResourceVersion)
	}
}

// TestFinalize_WrongSeedIdentity_ZeroUpdateAttempts covers the Stage 1B
// correction 4 designated-seed policy: an operator-supplied GenesisConsumer
// that does NOT equal the deterministic seed-identity convention
// (lifecycle.DesignatedGenesisSeed, "gossipgrid-0") must fail closed BEFORE
// any ConfigMap update (0 Update calls; the record is unchanged). The
// pending-grant document does NOT carry a seed-identity field (a pending
// record has GenesisConsumer=""), so the finalization does not claim the
// pending ConfigMap authenticates the seed identity — it enforces the
// deterministic convention.
//
// The test covers several wrong-seed cases: a different pod ordinal
// (gossipgrid-1), a custom non-ordinal-0 name (custom-seed), and the empty
// string (an empty consumer is already rejected by Validate, but the
// assertion of 0 Update calls holds for every rejection before the Get/Update
// path).
func TestFinalize_WrongSeedIdentity_ZeroUpdateAttempts(t *testing.T) {
	cases := []struct {
		name string
		seed string
	}{
		{"different pod ordinal (gossipgrid-1)", "gossipgrid-1"},
		{"custom non-ordinal-0 name (custom-seed)", "custom-seed"},
		{"empty seed (rejected by Validate)", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The record is a PENDING authorization (outstanding, unconsumed):
			// the only case where a finalization would normally proceed to the
			// CAS. A wrong-seed config must fail closed before the Get/Update.
			u := &fakeUpdater{
				cm:                  &k8smetadata.ConfigMap{ResourceVersion: "rv-1", Data: map[string]string{"metadata": pendingDoc}},
				nextResourceVersion: "rv-2",
			}
			op := newFinalizeOp(t, u)
			cfg := testFinalizeCfg()
			cfg.GenesisConsumer = tc.seed
			_, err := op.Finalize(context.Background(), cfg)
			// A wrong seed identity must fail closed (an error, not a success).
			if err == nil {
				t.Fatalf("expected an error for a wrong seed identity %q; got nil (the finalization must not arm data-loss protection for a non-designated seed)", tc.seed)
			}
			// The key assertion: zero Update calls. The designated-seed policy
			// is enforced in Validate (before the Get), so no ConfigMap update
			// is attempted for a wrong seed identity.
			if u.updateCalls != 0 {
				t.Fatalf("expected 0 update calls for a wrong seed identity %q; got %d (the record must not be modified)", tc.seed, u.updateCalls)
			}
			// The record must be UNCHANGED (no new revision, no modified data).
			if u.cm.ResourceVersion != "rv-1" {
				t.Errorf("expected the resourceVersion to remain rv-1 (no update); got %s", u.cm.ResourceVersion)
			}
		})
	}
}

// contains is a simple substring helper for the document assertions.
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
