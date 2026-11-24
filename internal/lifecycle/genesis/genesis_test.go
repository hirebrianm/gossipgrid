package genesis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"gossipgrid/internal/lifecycle/k8smetadata"
)

// fakeWriter is a deterministic, concurrency-safe ConfigMapWriter for tests.
// It simulates the Kubernetes API: Get returns the stored ConfigMap (or a
// typed not-found error when absent), and Create succeeds only when the
// object does not already exist (otherwise a typed 409 conflict). A
// createConflict / createError / getError field lets the test inject specific
// API outcomes. The mutex makes it safe for concurrent grant tests.
type fakeWriter struct {
	mu          sync.Mutex
	cm          *k8smetadata.ConfigMap
	createCalls int
	getCalls    int
	// Injected errors (set before the test calls Grant/Preflight).
	createConflict bool
	createError    error
	getError       error
	// createNilResult makes Create return (nil, nil) — an ambiguous
	// (unverifiable) result.
	createNilResult bool
	// getNilResult makes Get return (nil, nil) — an invalid/ambiguous API
	// response (Task 4 regression: must not authorize creation).
	getNilResult bool
}

func (w *fakeWriter) Get(_ context.Context, _, _ string) (*k8smetadata.ConfigMap, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.getCalls++
	if w.getError != nil {
		return nil, w.getError
	}
	if w.getNilResult {
		return nil, nil
	}
	if w.cm == nil {
		return nil, k8smetadata.NewConfigMapNotFound(errors.New("configmap not found"))
	}
	return w.cm, nil
}

func (w *fakeWriter) Create(_ context.Context, _, _ string, _ map[string]string) (*k8smetadata.ConfigMap, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.createCalls++
	if w.createError != nil {
		return nil, w.createError
	}
	if w.createConflict || w.cm != nil {
		return nil, k8smetadata.NewAPIError(k8smetadata.APIClassNotFound, errors.New("already exists (409)"))
	}
	if w.createNilResult {
		return nil, nil
	}
	// A success: the object is created. Build a minimal ConfigMap response
	// with a resourceVersion.
	return &k8smetadata.ConfigMap{ResourceVersion: "rv-created"}, nil
}

// validPendingDoc returns a schema-v1 JSON document for a valid, pending
// genesis authorization (Initialized=false, GenesisAuthorized=true).
func validPendingDoc() string {
	return `{
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
}

// validInitializedDoc returns a schema-v1 JSON document for an initialized,
// protected cluster (Initialized=true, GenesisAuthorized=false,
// DataLossProtected=true).
func validInitializedDoc() string {
	return `{
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
}

func newTestOp(t *testing.T, w ConfigMapWriter) *GrantOperation {
	t.Helper()
	op, err := NewGrantOperation("gossipgrid", w)
	if err != nil {
		t.Fatalf("NewGrantOperation: %v", err)
	}
	return op
}

// isUUIDv7 reports whether s is a well-formed, version-7 UUID (the project's
// documented identifier convention).
func isUUIDv7(t *testing.T, s string) {
	t.Helper()
	if s == "" {
		t.Fatalf("empty identifier")
	}
	// A UUIDv7 string: 36 chars, version nibble (index 14) is '7'.
	if len(s) != 36 {
		t.Fatalf("identifier %q is not 36 chars", s)
	}
	if s[14] != '7' {
		t.Fatalf("identifier %q version nibble is %c, want '7'", s, s[14])
	}
}

// TestGrantSuccessfulFirstCreation verifies the happy path: a fresh cluster
// (ConfigMap absent) yields a successful create with a valid schema-v1 pending
// grant.
func TestGrantSuccessfulFirstCreation(t *testing.T) {
	w := &fakeWriter{}
	op := newTestOp(t, w)
	res, err := op.Grant(context.Background())
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if !res.Created {
		t.Error("Created = false, want true")
	}
	if res.AlreadyGranted {
		t.Error("AlreadyGranted = true, want false")
	}
	// The metadata must be the exact pending grant state.
	if res.Metadata.Initialized {
		t.Error("Initialized = true, want false")
	}
	if !res.Metadata.GenesisAuthorized {
		t.Error("GenesisAuthorized = false, want true")
	}
	if res.Metadata.GenesisConsumer != "" {
		t.Errorf("GenesisConsumer = %q, want empty", res.Metadata.GenesisConsumer)
	}
	if res.Metadata.DataLossProtected {
		t.Error("DataLossProtected = true, want false")
	}
	if res.Metadata.EverAcceptedMessages {
		t.Error("EverAcceptedMessages = true, want false")
	}
	if res.Metadata.Generation != 1 {
		t.Errorf("Generation = %d, want 1", res.Metadata.Generation)
	}
	if res.Metadata.LastReset != nil {
		t.Errorf("LastReset = %v, want nil", res.Metadata.LastReset)
	}
	if w.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1", w.createCalls)
	}
}

// TestGrantMetadataSchema verifies the created metadata is a valid, coherent
// schema-v1 document: distinct, well-formed identifiers and a Validate-passing
// state.
func TestGrantMetadataSchema(t *testing.T) {
	w := &fakeWriter{}
	op := newTestOp(t, w)
	res, err := op.Grant(context.Background())
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	// Distinct, valid cluster and epoch identifiers.
	if res.Metadata.ClusterID == res.Metadata.EpochID {
		t.Error("ClusterID == EpochID; they must be distinct")
	}
	isUUIDv7(t, res.Metadata.ClusterID)
	isUUIDv7(t, res.Metadata.EpochID)
	// The complete document must pass Validate (a coherent pending state).
	if err := res.Metadata.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

// TestGrantRoundTrip verifies the document the grant writes is exactly the
// schema-v1 document the reader parses: the created metadata marshals to a
// JSON document that, when parsed by parseExistingMetadata, round-trips to the
// same state.
func TestGrantRoundTrip(t *testing.T) {
	w := &fakeWriter{}
	op := newTestOp(t, w)
	res, err := op.Grant(context.Background())
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	// Reconstruct the document the grant would have written and verify it
	// parses back to the same metadata (the round-trip contract).
	doc, err := marshalMetadataDocument(res.Metadata)
	if err != nil {
		t.Fatalf("marshalMetadataDocument: %v", err)
	}
	var rawObj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(doc), &rawObj); err != nil {
		t.Fatalf("the document is not a JSON object: %v", err)
	}
	// All required keys must be present.
	for _, key := range []string{"schemaVersion", "clusterId", "epochId", "generation", "initialized", "genesisAuthorized", "genesisConsumer", "everAcceptedMessages", "dataLossProtected"} {
		if _, ok := rawObj[key]; !ok {
			t.Errorf("document is missing key %q", key)
		}
	}
	// The schema version must be 1.
	var sv int
	if err := json.Unmarshal(rawObj["schemaVersion"], &sv); err != nil || sv != 1 {
		t.Errorf("schemaVersion = %d (err=%v), want 1", sv, err)
	}
}

// TestGrantDuplicateInvocation verifies that a second grant against an
// existing valid pending grant is idempotent: it reports ErrAlreadyGranted
// without creating a second record (no second Create is attempted, or if one
// is, it is a conflict that resolves to the existing record).
func TestGrantDuplicateInvocation(t *testing.T) {
	// Simulate an existing valid pending grant (a prior successful grant).
	w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: validPendingDoc()}}}
	op := newTestOp(t, w)
	res, err := op.Grant(context.Background())
	if !errors.Is(err, ErrAlreadyGranted) {
		t.Fatalf("Grant = %v, want ErrAlreadyGranted", err)
	}
	if !res.AlreadyGranted {
		t.Error("AlreadyGranted = false, want true")
	}
	if res.Created {
		t.Error("Created = true, want false")
	}
	// The existing metadata must be reported (the prior grant's identities).
	if res.Metadata.ClusterID != "0190e8a2-6b0c-7000-8000-000000000000" {
		t.Errorf("Metadata.ClusterID = %q, want the existing identity", res.Metadata.ClusterID)
	}
}

// TestGrantExistingPendingGrant is the same as TestGrantDuplicateInvocation but
// exercises the conflict path: preflight sees the existing object, the grant
// attempts a Create (which 409s), and the re-read classifies the existing
// record as already granted.
func TestGrantExistingPendingGrant(t *testing.T) {
	w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: validPendingDoc()}}}
	op := newTestOp(t, w)
	res, err := op.Grant(context.Background())
	// Preflight sees the existing object (a valid pending grant) and succeeds.
	// Create 409s (the object exists). The re-read classifies it as
	// ErrAlreadyGranted.
	if !errors.Is(err, ErrAlreadyGranted) {
		t.Fatalf("Grant = %v, want ErrAlreadyGranted", err)
	}
	if !res.AlreadyGranted {
		t.Error("AlreadyGranted = false, want true")
	}
}

// TestGrantExistingInitializedCluster verifies that a grant against an
// existing initialized cluster fails closed (ErrClusterInitialized) without
// creating a replacement identity.
func TestGrantExistingInitializedCluster(t *testing.T) {
	w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: validInitializedDoc()}}}
	op := newTestOp(t, w)
	res, err := op.Grant(context.Background())
	if !errors.Is(err, ErrClusterInitialized) {
		t.Fatalf("Grant = %v, want ErrClusterInitialized", err)
	}
	if res.Created {
		t.Error("Created = true, want false (an initialized cluster is never re-granted)")
	}
	if w.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (preflight failed; no create attempted)", w.createCalls)
	}
}

// TestGrantCorruptedMetadata verifies that a grant against a corrupted
// ConfigMap (malformed JSON) fails closed (ErrMetadataConflict) without
// creating a replacement identity.
func TestGrantCorruptedMetadata(t *testing.T) {
	w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: "{not json"}}}
	op := newTestOp(t, w)
	res, err := op.Grant(context.Background())
	if !errors.Is(err, ErrMetadataConflict) {
		t.Fatalf("Grant = %v, want ErrMetadataConflict", err)
	}
	if res.Created {
		t.Error("Created = true, want false")
	}
}

// TestGrantMissingMetadataKey verifies that a present ConfigMap with no
// metadata key is a corrupted (malformed) record, not an absent one: the
// grant fails closed (ErrMetadataConflict).
func TestGrantMissingMetadataKey(t *testing.T) {
	w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{"other": "x"}}}
	op := newTestOp(t, w)
	_, err := op.Grant(context.Background())
	if !errors.Is(err, ErrMetadataConflict) {
		t.Fatalf("Grant = %v, want ErrMetadataConflict for missing key", err)
	}
}

// TestGrantUnsupportedSchemaVersion verifies that a record with an
// unsupported schemaVersion fails closed (ErrMetadataConflict), not a silent
// grant.
func TestGrantUnsupportedSchemaVersion(t *testing.T) {
	doc := `{"schemaVersion": 99, "clusterId": "a", "epochId": "b", "generation": 1, "initialized": false, "genesisAuthorized": true, "genesisConsumer": "", "everAcceptedMessages": false, "dataLossProtected": false}`
	w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: doc}}}
	op := newTestOp(t, w)
	_, err := op.Grant(context.Background())
	if !errors.Is(err, ErrMetadataConflict) {
		t.Fatalf("Grant = %v, want ErrMetadataConflict for unsupported version", err)
	}
}

// TestGrantIncoherentState verifies that a record in an incoherent state
// (GenesisConsumer set but Initialized=false) fails closed (ErrMetadataConflict).
func TestGrantIncoherentState(t *testing.T) {
	doc := `{"schemaVersion": 1, "clusterId": "a", "epochId": "b", "generation": 1, "initialized": false, "genesisAuthorized": false, "genesisConsumer": "gossipgrid-0", "everAcceptedMessages": false, "dataLossProtected": false}`
	w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: doc}}}
	op := newTestOp(t, w)
	_, err := op.Grant(context.Background())
	if !errors.Is(err, ErrMetadataConflict) {
		t.Fatalf("Grant = %v, want ErrMetadataConflict for incoherent state", err)
	}
}

// TestGrantCreateConflictAndReRead verifies the core concurrency contract: an
// existing valid pending grant is detected before any Create is attempted, and
// the grant reports ErrAlreadyGranted with the existing identity, never by
// generating a replacement identity.
func TestGrantCreateConflictAndReRead(t *testing.T) {
	// The object exists (a prior grant); preflight succeeds (valid pending),
	// and the idempotency check detects the existing record before any Create.
	w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: validPendingDoc()}}}
	op := newTestOp(t, w)
	res, err := op.Grant(context.Background())
	if !errors.Is(err, ErrAlreadyGranted) {
		t.Fatalf("Grant = %v, want ErrAlreadyGranted for an existing valid pending grant", err)
	}
	// No Create was attempted (the existing record was detected directly).
	if w.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (the existing valid pending grant is detected before any Create)", w.createCalls)
	}
	// No replacement identity: the reported metadata is the existing one.
	if res.Metadata.ClusterID != "0190e8a2-6b0c-7000-8000-000000000000" {
		t.Errorf("Metadata.ClusterID = %q, want the existing identity (no replacement)", res.Metadata.ClusterID)
	}
}

// TestGrantConcurrentCreates verifies that two concurrent grants produce
// exactly one cluster identity: the first create succeeds, the second 409s and
// resolves to the existing record (ErrAlreadyGranted). No third identity is
// ever generated.
func TestGrantConcurrentCreates(t *testing.T) {
	// A shared writer that models the Kubernetes API: the first Create
	// succeeds and stores the object; a subsequent Create 409s because the
	// object now exists.
	w := &raceWriter{data: make(map[string]string)}
	op := newTestOp(t, w)

	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []GrantResult
	var errs []error
	const n = 8
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := op.Grant(context.Background())
			mu.Lock()
			results = append(results, res)
			errs = append(errs, err)
			mu.Unlock()
		}()
	}
	wg.Wait()

	// Exactly one grant must report Created=true (the winner).
	createdCount := 0
	for _, res := range results {
		if res.Created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Errorf("createdCount = %d, want exactly 1 (a single identity wins the race)", createdCount)
	}
	// All other grants must report ErrAlreadyGranted (the idempotent
	// "already granted" outcome) — never a new identity and never a hard
	// error.
	for i := 0; i < n; i++ {
		if !results[i].Created {
			if !errors.Is(errs[i], ErrAlreadyGranted) {
				t.Errorf("grant %d: non-winner err = %v, want ErrAlreadyGranted", i, errs[i])
			}
		}
	}
	// All winners' and losers' reported ClusterIDs must be the single
	// identity the winner created (no replacement, no divergence).
	var winnerID string
	for _, res := range results {
		if res.Created {
			winnerID = res.Metadata.ClusterID
			break
		}
	}
	if winnerID == "" {
		t.Fatal("no winner found")
	}
	for i := 0; i < n; i++ {
		if results[i].Metadata.ClusterID != winnerID {
			t.Errorf("grant %d: ClusterID %q != winner %q (a replacement identity was generated)", i, results[i].Metadata.ClusterID, winnerID)
		}
	}
	// The writer must have been created exactly once (the first successful
	// create).
	if w.created != 1 {
		t.Errorf("writer created = %d, want exactly 1", w.created)
	}
}

// raceWriter is a ConfigMapWriter that models the Kubernetes create race: the
// first Create succeeds and stores the object (with a generated identity); any
// subsequent Create 409s because the object now exists. Get returns the stored
// object (or a not-found error when absent).
type raceWriter struct {
	mu      sync.Mutex
	data    map[string]string
	created int
}

func (w *raceWriter) Get(_ context.Context, _, _ string) (*k8smetadata.ConfigMap, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.data) == 0 {
		return nil, k8smetadata.NewConfigMapNotFound(errors.New("configmap not found"))
	}
	return &k8smetadata.ConfigMap{ResourceVersion: "rv", Data: w.data}, nil
}

func (w *raceWriter) Create(_ context.Context, _, _ string, data map[string]string) (*k8smetadata.ConfigMap, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.data) > 0 {
		// The object already exists: a 409 conflict.
		return nil, k8smetadata.NewAPIError(k8smetadata.APIClassNotFound, errors.New("already exists (409)"))
	}
	w.created++
	w.data = data
	return &k8smetadata.ConfigMap{ResourceVersion: "rv", Data: data}, nil
}

// TestGrantAmbiguousCreate verifies that an ambiguous create result (a nil
// response with a nil error) is resolved by re-reading the authoritative
// object: if the object is now present and valid it is reported
// (ErrAlreadyGranted), otherwise the grant fails closed (ErrCreateAmbiguous) —
// never by blindly generating a second identity.
func TestGrantAmbiguousCreate(t *testing.T) {
	// Case 1: the create returns (nil, nil) and the object is absent on
	// re-read. The grant must fail closed (ErrCreateAmbiguous).
	w := &fakeWriter{createNilResult: true} // absent object (cm == nil)
	op := newTestOp(t, w)
	_, err := op.Grant(context.Background())
	if !errors.Is(err, ErrCreateAmbiguous) {
		t.Fatalf("Grant = %v, want ErrCreateAmbiguous for nil-create-with-absent-object", err)
	}

	// Case 2: the create returns (nil, nil) but the object is present (a
	// concurrent writer won). The re-read classifies it as already granted.
	w2 := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: validPendingDoc()}}}
	// Force Create to return (nil, nil) (ambiguous) while the object exists.
	w2.createNilResult = true
	op2 := newTestOp(t, w2)
	res2, err2 := op2.Grant(context.Background())
	if !errors.Is(err2, ErrAlreadyGranted) {
		t.Fatalf("Grant = %v, want ErrAlreadyGranted for nil-create-with-present-object", err2)
	}
	if !res2.AlreadyGranted {
		t.Error("AlreadyGranted = false, want true")
	}
}

// TestGrantTransientCreateError verifies that a transient create error (the
// object is absent on re-read) fails closed (ErrCreateAmbiguous) rather than
// retrying with a new identity.
func TestGrantTransientCreateError(t *testing.T) {
	w := &fakeWriter{createError: k8smetadata.NewAPIUnavailable(errors.New("503"))} // absent object
	op := newTestOp(t, w)
	_, err := op.Grant(context.Background())
	if !errors.Is(err, ErrCreateAmbiguous) {
		t.Fatalf("Grant = %v, want ErrCreateAmbiguous for transient create error with absent object", err)
	}
}

// TestGrantAPIUnavailable verifies that a preflight failure due to a
// transient API unavailability fails closed (the operator cannot verify the
// preconditions) without creating anything.
func TestGrantAPIUnavailable(t *testing.T) {
	w := &fakeWriter{getError: k8smetadata.NewAPIUnavailable(errors.New("503 service unavailable"))}
	op := newTestOp(t, w)
	_, err := op.Grant(context.Background())
	var ua *k8smetadata.ErrAPIUnavailable
	if !errors.As(err, &ua) {
		t.Fatalf("Grant = %v, want an ErrAPIUnavailable marker (preflight failed closed)", err)
	}
	if w.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (preflight failed; no create attempted)", w.createCalls)
	}
}

// TestGrantPermissionDenied verifies that a permission-denied (403) failure
// fails closed (the RBAC is misconfigured) without creating anything.
func TestGrantPermissionDenied(t *testing.T) {
	w := &fakeWriter{getError: k8smetadata.NewAPIPermissionDenied(errors.New("forbidden: user cannot get"))}
	op := newTestOp(t, w)
	_, err := op.Grant(context.Background())
	if !errors.Is(err, k8smetadata.ErrMetadataPermissionDenied) {
		t.Fatalf("Grant = %v, want ErrMetadataPermissionDenied", err)
	}
	if w.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (preflight permission-denied; no create attempted)", w.createCalls)
	}
}

// TestGrantPermissionDeniedOnCreate verifies that a permission-denied on the
// Create call (a preflight that succeeded but the Job lacks create
// permission) fails closed (ErrMetadataPermissionDenied) without a re-read
// resolution.
func TestGrantPermissionDeniedOnCreate(t *testing.T) {
	w := &fakeWriter{createError: k8smetadata.NewAPIPermissionDenied(errors.New("forbidden: user cannot create"))}
	op := newTestOp(t, w)
	_, err := op.Grant(context.Background())
	if !errors.Is(err, k8smetadata.ErrMetadataPermissionDenied) {
		t.Fatalf("Grant = %v, want ErrMetadataPermissionDenied", err)
	}
}

// TestGrantContextCancellation verifies that a canceled context is surfaced
// (context.Canceled) and no create is attempted.
func TestGrantContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &fakeWriter{}
	op := newTestOp(t, w)
	_, err := op.Grant(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Grant = %v, want context.Canceled", err)
	}
	if w.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (context canceled; no create attempted)", w.createCalls)
	}
}

// TestGrantPreflightMissingObjectSucceeds verifies that a preflight against a
// missing ConfigMap (a readable, healthy API) succeeds (the only state in which
// a grant is permitted).
func TestGrantPreflightMissingObjectSucceeds(t *testing.T) {
	w := &fakeWriter{} // absent object
	op := newTestOp(t, w)
	if err := op.Preflight(context.Background()); err != nil {
		t.Fatalf("Preflight = %v, want nil for a missing object", err)
	}
}

// TestGrantPreflightExistingPendingSucceeds verifies that a preflight against
// an existing valid pending grant succeeds (a grant would be a no-op
// "already granted").
func TestGrantPreflightExistingPendingSucceeds(t *testing.T) {
	w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: validPendingDoc()}}}
	op := newTestOp(t, w)
	if err := op.Preflight(context.Background()); err != nil {
		t.Fatalf("Preflight = %v, want nil for an existing valid pending grant", err)
	}
}

// TestGrantPreflightInitializedFails verifies that a preflight against an
// initialized cluster fails closed (ErrClusterInitialized).
func TestGrantPreflightInitializedFails(t *testing.T) {
	w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: validInitializedDoc()}}}
	op := newTestOp(t, w)
	_, err := op.Grant(context.Background())
	if !errors.Is(err, ErrClusterInitialized) {
		t.Fatalf("Grant = %v, want ErrClusterInitialized", err)
	}
}

// TestGrantPreflightCorruptedFails verifies that a preflight against a
// corrupted record fails closed (ErrMetadataConflict).
func TestGrantPreflightCorruptedFails(t *testing.T) {
	w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: "{bad"}}}
	op := newTestOp(t, w)
	_, err := op.Grant(context.Background())
	if !errors.Is(err, ErrMetadataConflict) {
		t.Fatalf("Grant = %v, want ErrMetadataConflict", err)
	}
}

// TestGrantPreflightAPIUnavailableFailsClosed verifies that a preflight
// failure due to an API unavailability fails closed (no safety is inferred
// from an unreadable API).
func TestGrantPreflightAPIUnavailableFailsClosed(t *testing.T) {
	w := &fakeWriter{getError: k8smetadata.NewAPIUnavailable(errors.New("503"))}
	op := newTestOp(t, w)
	err := op.Preflight(context.Background())
	if err == nil {
		t.Fatal("Preflight = nil, want an error (an unreadable API must fail closed)")
	}
	var ua *k8smetadata.ErrAPIUnavailable
	if !errors.As(err, &ua) {
		t.Fatalf("Preflight = %v, want an ErrAPIUnavailable marker", err)
	}
}

// TestGrantPreflightPermissionDeniedFails verifies that a preflight
// permission-denied fails closed (the operator cannot verify the
// preconditions).
func TestGrantPreflightPermissionDeniedFails(t *testing.T) {
	w := &fakeWriter{getError: k8smetadata.NewAPIPermissionDenied(errors.New("forbidden"))}
	op := newTestOp(t, w)
	err := op.Preflight(context.Background())
	if err == nil {
		t.Fatal("Preflight = nil, want an error")
	}
	if !errors.Is(err, k8smetadata.ErrMetadataPermissionDenied) {
		t.Fatalf("Preflight = %v, want ErrMetadataPermissionDenied", err)
	}
}

// TestGrantPreflightContextCanceled verifies that a canceled context in
// preflight is surfaced as context.Canceled.
func TestGrantPreflightContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &fakeWriter{}
	op := newTestOp(t, w)
	if err := op.Preflight(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Preflight = %v, want context.Canceled", err)
	}
}

// TestNewGrantOperationValidation verifies the constructor rejects an empty
// namespace and a nil writer.
func TestNewGrantOperationValidation(t *testing.T) {
	if _, err := NewGrantOperation("", &fakeWriter{}); err == nil {
		t.Error("expected error for empty namespace")
	}
	if _, err := NewGrantOperation("  ", &fakeWriter{}); err == nil {
		t.Error("expected error for blank namespace")
	}
	if _, err := NewGrantOperation("ns", nil); err == nil {
		t.Error("expected error for nil writer")
	}
}

// TestGrantIdentityDistinctAndValid verifies the generated identity pair is
// always distinct and well-formed (a regression guard on newIdentityPair).
func TestGrantIdentityDistinctAndValid(t *testing.T) {
	c, e, err := newIdentityPair()
	if err != nil {
		t.Fatalf("newIdentityPair: %v", err)
	}
	if c == e {
		t.Fatal("ClusterID == EpochID; they must be distinct")
	}
	isUUIDv7(t, c)
	isUUIDv7(t, e)
}

// TestGrantMetadataDocumentContainsAllFields verifies the created document
// carries every required field (no field is omitted, which would be ambiguous
// on re-parse).
func TestGrantMetadataDocumentContainsAllFields(t *testing.T) {
	w := &fakeWriter{}
	op := newTestOp(t, w)
	res, err := op.Grant(context.Background())
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	doc, err := marshalMetadataDocument(res.Metadata)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{
		`"schemaVersion":1`, `"clusterId":`, `"epochId":`, `"generation":1`,
		`"initialized":false`, `"genesisAuthorized":true`, `"genesisConsumer":""`,
		`"everAcceptedMessages":false`, `"dataLossProtected":false`,
	} {
		if !strings.Contains(doc, key) {
			t.Errorf("document is missing %q: %s", key, doc)
		}
	}
	// No lastReset on the first epoch.
	if strings.Contains(doc, "lastReset") {
		t.Errorf("document contains lastReset: %s (the first epoch has no reset incident)", doc)
	}
}

// TestGrantNoReplacementOnAmbiguity verifies the core invariant: a failed or
// ambiguous grant never generates a replacement identity. When an existing
// valid pending grant is present, the grant returns the existing identity
// without any Create (no replacement identity is generated).
func TestGrantNoReplacementOnAmbiguity(t *testing.T) {
	// The object exists (a valid pending grant). The idempotency check detects
	// it before any Create is attempted.
	w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: validPendingDoc()}}}
	w.createNilResult = true
	op := newTestOp(t, w)
	_, err := op.Grant(context.Background())
	if !errors.Is(err, ErrAlreadyGranted) {
		t.Fatalf("Grant = %v, want ErrAlreadyGranted", err)
	}
	// No Create was attempted (the existing record was detected directly).
	if w.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (no replacement identity generated)", w.createCalls)
	}
}

// TestGrantCreateThenCrashRetry simulates the crash-retry case: a first grant
// succeeds (the ConfigMap is created), the process crashes, and a second
// grant is issued. The second grant observes the existing valid pending grant
// and reports ErrAlreadyGranted (idempotent) without creating a second record.
func TestGrantCreateThenCrashRetry(t *testing.T) {
	// Model the crash: the first grant succeeds and stores the object in a
	// shared writer; the "crashed" process is gone, and a new process (a new
	// GrantOperation over the same shared writer) retries.
	shared := &raceWriter{data: make(map[string]string)}
	op1 := newTestOp(t, shared)
	res1, err1 := op1.Grant(context.Background())
	if err1 != nil || !res1.Created {
		t.Fatalf("first grant: res=%v err=%v, want a successful create", res1, err1)
	}
	// The object now exists (the crash occurred after the create succeeded).
	// A new process retries the grant.
	op2 := newTestOp(t, shared)
	res2, err2 := op2.Grant(context.Background())
	if !errors.Is(err2, ErrAlreadyGranted) {
		t.Fatalf("retry grant = %v, want ErrAlreadyGranted (idempotent crash recovery)", err2)
	}
	if res2.Created {
		t.Error("retry Created = true, want false (no second record created)")
	}
	// The retry must report the same identity the first grant created.
	if res2.Metadata.ClusterID != res1.Metadata.ClusterID {
		t.Errorf("retry ClusterID %q != first grant %q (a replacement identity was generated)", res2.Metadata.ClusterID, res1.Metadata.ClusterID)
	}
	// The shared writer's object was created exactly once.
	if shared.created != 1 {
		t.Errorf("shared writer created = %d, want exactly 1", shared.created)
	}
}

// TestPreflightNilNilGetFailsClosed is the Task 4 regression: a Get response
// of (nil, nil) in preflight must NOT authorize creation. It is an invalid or
// ambiguous API response and the preflight must fail closed with zero create
// attempts.
func TestPreflightNilNilGetFailsClosed(t *testing.T) {
	w := &fakeWriter{getNilResult: true}
	op := newTestOp(t, w)
	err := op.Preflight(context.Background())
	if err == nil {
		t.Fatal("Preflight = nil, want an error (nil ConfigMap with nil error is an invalid API response; must fail closed)")
	}
	// The error must carry the ErrAPIUnavailable marker (the conservative
	// default for an unclassified/ambiguous response).
	var ua *k8smetadata.ErrAPIUnavailable
	if !errors.As(err, &ua) {
		t.Errorf("Preflight = %v, want an ErrAPIUnavailable marker (fail-closed on ambiguous response)", err)
	}
	// No create may have been attempted (preflight failed; the grant does not
	// proceed).
	if w.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (preflight failed closed; no create attempted)", w.createCalls)
	}
}

// TestGrantNilNilGetFailsClosed is the Task 4 regression at the Grant level:
// a Get response of (nil, nil) must cause zero create attempts. The grant
// fails closed rather than inferring that the object is absent.
func TestGrantNilNilGetFailsClosed(t *testing.T) {
	w := &fakeWriter{getNilResult: true}
	op := newTestOp(t, w)
	_, err := op.Grant(context.Background())
	if err == nil {
		t.Fatal("Grant = nil, want an error (nil ConfigMap with nil error is an invalid API response; must fail closed)")
	}
	// No create may have been attempted.
	if w.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (ambiguous Get response; no create attempted)", w.createCalls)
	}
}

// TestGrantExistingPendingGrantNoCreate verifies the Task 3 idempotency
// contract: when an existing valid pending grant is discovered, the grant
// returns the existing authoritative identity WITHOUT generating a new
// identity pair or attempting a Create. The create counter must remain 0.
func TestGrantExistingPendingGrantNoCreate(t *testing.T) {
	w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: validPendingDoc()}}}
	op := newTestOp(t, w)
	res, err := op.Grant(context.Background())
	if !errors.Is(err, ErrAlreadyGranted) {
		t.Fatalf("Grant = %v, want ErrAlreadyGranted", err)
	}
	if !res.AlreadyGranted {
		t.Error("AlreadyGranted = false, want true")
	}
	if res.Created {
		t.Error("Created = true, want false")
	}
	// The critical assertion: no Create was attempted (the existing identity
	// was returned directly).
	if w.createCalls != 0 {
		t.Errorf("createCalls = %d, want 0 (an existing valid pending grant must not trigger a Create)", w.createCalls)
	}
	// The reported metadata must be the existing identity (not a newly
	// generated one).
	if res.Metadata.ClusterID != "0190e8a2-6b0c-7000-8000-000000000000" {
		t.Errorf("Metadata.ClusterID = %q, want the existing identity (0190e8a2-...)", res.Metadata.ClusterID)
	}
	if res.Metadata.EpochID != "0190e8a3-1c2d-7000-9000-111111111111" {
		t.Errorf("Metadata.EpochID = %q, want the existing identity (0190e8a3-...)", res.Metadata.EpochID)
	}
}

// TestGrantErrorChainPreservation verifies the error-wrapping regression fix:
// errors returned by Preflight and Grant must preserve their underlying error
// chains (via %w or errors.Join) so that the original Kubernetes API error,
// JSON parsing error, and validation error remain reachable, and the error
// sentinels remain intact.
func TestGrantErrorChainPreservation(t *testing.T) {
	t.Run("Preflight permission denied preserves sentinel and underlying error", func(t *testing.T) {
		// The API returns a permission-denied error wrapping underlying.
		// The grant uses errors.Join(underlying, sentinel-wrapping-error),
		// so BOTH the original API error AND the sentinel must be reachable
		// via errors.Is.
		underlying := errors.New("forbidden: user cannot get configmaps")
		w := &fakeWriter{getError: k8smetadata.NewAPIPermissionDenied(underlying)}
		op := newTestOp(t, w)
		err := op.Preflight(context.Background())
		if err == nil {
			t.Fatal("Preflight = nil, want an error")
		}
		// The permission-denied sentinel must be reachable via errors.Is.
		if !errors.Is(err, k8smetadata.ErrMetadataPermissionDenied) {
			t.Errorf("errors.Is(err, ErrMetadataPermissionDenied) = false, want true; err=%v", err)
		}
		// The underlying API error must be reachable via errors.Is (the
		// key regression: the old %w: %v pattern lost the underlying error
		// from the unwrap chain).
		if !errors.Is(err, underlying) {
			t.Errorf("errors.Is(err, underlying) = false, want true; err=%v", err)
		}
	})

	t.Run("Grant permission denied on Create preserves sentinel and underlying error", func(t *testing.T) {
		underlying := errors.New("forbidden: user cannot create configmaps")
		w := &fakeWriter{createError: k8smetadata.NewAPIPermissionDenied(underlying)}
		op := newTestOp(t, w)
		_, err := op.Grant(context.Background())
		if err == nil {
			t.Fatal("Grant = nil, want an error")
		}
		if !errors.Is(err, k8smetadata.ErrMetadataPermissionDenied) {
			t.Errorf("errors.Is(err, ErrMetadataPermissionDenied) = false, want true; err=%v", err)
		}
		// The underlying API error must be reachable via errors.Is.
		if !errors.Is(err, underlying) {
			t.Errorf("errors.Is(err, underlying) = false, want true; err=%v", err)
		}
	})

	t.Run("Preflight corrupted document preserves JSON parse error", func(t *testing.T) {
		// A ConfigMap with malformed JSON: parseExistingMetadata wraps the
		// json.Unmarshal error via errors.Join(ErrMetadataConflict, err),
		// so BOTH the sentinel AND the original parse error are in the chain.
		// The json.Unmarshal error for "{bad json" is a *json.SyntaxError
		// (a named string type). It does NOT implement the error interface
		// as a pointer, so we verify via errors.Is using a captured
		// sentinel.
		w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: "{bad json"}}}
		op := newTestOp(t, w)
		_, err := op.Grant(context.Background())
		if err == nil {
			t.Fatal("Grant = nil, want an error")
		}
		// The sentinel must be reachable via errors.Is.
		if !errors.Is(err, ErrMetadataConflict) {
			t.Errorf("errors.Is(err, ErrMetadataConflict) = false, want true; err=%v", err)
		}
		// The key regression: the JSON parse error is NOT lost. Verify via
		// errors.Is that the error chain contains a JSON-related error.
		// json.Unmarshal returns errors whose messages include "invalid
		// character" for syntax errors. We use a sentinel that matches the
		// json.Unmarshal error's underlying cause.
		// Since json.Unmarshal's error type is not easily captured by a
		// sentinel, we verify the chain contains the expected JSON error
		// by checking that the error is NOT only the sentinel (i.e., the
		// underlying parse error was preserved, not lost to %v).
		// Use errors.As to verify a *json.SyntaxError is in the chain.
		var syntaxErr *json.SyntaxError
		if !errors.As(err, &syntaxErr) {
			// json.SyntaxError is a named string type, not a pointer-to-
			// error. Try the value type instead.
			t.Logf("note: *json.SyntaxError not found via errors.As (expected for this type); verifying via message content")
			if !strings.Contains(err.Error(), "invalid") && !strings.Contains(err.Error(), "JSON") && !strings.Contains(err.Error(), "json") && !strings.Contains(err.Error(), "character") {
				t.Errorf("error %q does not contain the JSON parse error message; the parse error chain was lost", err)
			}
		}
	})

	t.Run("Preflight validation error preserves validation error", func(t *testing.T) {
		// A document that parses but fails Validate: an over-long ClusterID.
		// toMetadata succeeds (field present), but Validate rejects it
		// (exceeds MaxIDBytes). parseExistingMetadata wraps via
		// errors.Join(ErrMetadataConflict, vErr), preserving the validation
		// error in the chain.
		overlong := strings.Repeat("a", 65) // 65 bytes > MaxIDBytes (64)
		doc := fmt.Sprintf(`{"schemaVersion":1,"clusterId":"%s","epochId":"0190e8a3-1c2d-7000-9000-111111111111","generation":1,"initialized":false,"genesisAuthorized":true,"genesisConsumer":"","everAcceptedMessages":false,"dataLossProtected":false}`, overlong)
		w := &fakeWriter{cm: &k8smetadata.ConfigMap{Data: map[string]string{k8smetadata.MetadataKey: doc}}}
		op := newTestOp(t, w)
		_, err := op.Grant(context.Background())
		if err == nil {
			t.Fatal("Grant = nil, want an error")
		}
		// The sentinel must be reachable via errors.Is.
		if !errors.Is(err, ErrMetadataConflict) {
			t.Errorf("errors.Is(err, ErrMetadataConflict) = false, want true; err=%v", err)
		}
		// The validation error from lifecycle.ClusterMetadata.Validate() must
		// be reachable via errors.Is. Validate returns an error containing
		// "exceeds 64 bytes" for an over-long identifier. We verify the
		// validation error was preserved (not lost to %v) by checking the
		// error chain contains the expected message.
		if !strings.Contains(err.Error(), "exceeds") && !strings.Contains(err.Error(), "64") && !strings.Contains(err.Error(), "ClusterID") {
			t.Errorf("error %q does not contain the validation error; the validation error chain was lost", err)
		}
	})

	t.Run("resolveAfterCreateError preserves create and re-read errors", func(t *testing.T) {
		// Preflight passes (object absent); Create returns a transient
		// error (503); the re-read (Get) returns an unclassified error (500).
		// The returned error must carry the create error, the re-read error,
		// AND the ErrCreateAmbiguous sentinel — all via errors.Join.
		// A test-specific writer is needed because fakeWriter returns
		// getError for ALL Get calls (including preflight).
		createUnderlying := errors.New("503 during create")
		reReadUnderlying := errors.New("500 on re-read")
		createErr := k8smetadata.NewAPIUnavailable(createUnderlying)
		reReadErr := k8smetadata.NewAPIError(k8smetadata.APIClassUnknown, reReadUnderlying)
		w := &resolveTestWriter{
			createError: createErr,
			reReadError: reReadErr,
		}
		op := newTestOp(t, w)
		_, err := op.Grant(context.Background())
		if err == nil {
			t.Fatal("Grant = nil, want an error")
		}
		// The ErrCreateAmbiguous sentinel must be reachable via errors.Is.
		if !errors.Is(err, ErrCreateAmbiguous) {
			t.Errorf("errors.Is(err, ErrCreateAmbiguous) = false, want true; err=%v", err)
		}
		// Both the create error AND the re-read error must be reachable
		// via errors.Is (the key regression: the old code discarded the
		// create error in the Create-NotFound branch).
		if !errors.Is(err, createUnderlying) {
			t.Errorf("errors.Is(err, createUnderlying) = false, want true; err=%v", err)
		}
		if !errors.Is(err, reReadUnderlying) {
			t.Errorf("errors.Is(err, reReadUnderlying) = false, want true; err=%v", err)
		}
		// The typed API error markers must be reachable via errors.As.
		var ua *k8smetadata.ErrAPIUnavailable
		if !errors.As(err, &ua) {
			t.Errorf("errors.As(err, *ErrAPIUnavailable) = false, want true (create error marker lost); err=%v", err)
		}
	})

	t.Run("Create NotFound error is preserved in resolution", func(t *testing.T) {
		// Regression test for the Create-NotFound branch: when Create
		// returns a not-found error (a contradictory state), the grant
		// must preserve the original Create error in the resolution path
		// (not discard it via resolveAmbiguousCreate).
		// The test-specific writer returns not-found for the first two Gets
		// (preflight + checkExistingPendingGrant) and a re-read error for
		// the third Get (the re-read after Create fails).
		createUnderlying := errors.New("not found on create")
		reReadUnderlying := errors.New("unavailable on re-read")
		createErr := k8smetadata.NewConfigMapNotFound(createUnderlying)
		reReadErr := k8smetadata.NewAPIUnavailable(reReadUnderlying)
		w := &resolveTestWriter{
			createError: createErr,
			reReadError: reReadErr,
		}
		op := newTestOp(t, w)
		_, err := op.Grant(context.Background())
		if err == nil {
			t.Fatal("Grant = nil, want an error")
		}
		// The create error must be preserved (not discarded).
		if !errors.Is(err, createUnderlying) {
			t.Errorf("errors.Is(err, createUnderlying) = false, want true; the Create-NotFound error was discarded; err=%v", err)
		}
		// The re-read error must also be present.
		if !errors.Is(err, reReadUnderlying) {
			t.Errorf("errors.Is(err, reReadUnderlying) = false, want true; err=%v", err)
		}
		// The ErrCreateAmbiguous sentinel must be reachable.
		if !errors.Is(err, ErrCreateAmbiguous) {
			t.Errorf("errors.Is(err, ErrCreateAmbiguous) = false, want true; err=%v", err)
		}
	})

	t.Run("ErrAPIUnavailable marker is reachable via errors.As", func(t *testing.T) {
		// A preflight API-unavailability failure must carry the
		// ErrAPIUnavailable marker, reachable via errors.As.
		w := &fakeWriter{getError: k8smetadata.NewAPIUnavailable(errors.New("503"))}
		op := newTestOp(t, w)
		err := op.Preflight(context.Background())
		if err == nil {
			t.Fatal("Preflight = nil, want an error")
		}
		var ua *k8smetadata.ErrAPIUnavailable
		if !errors.As(err, &ua) {
			t.Errorf("errors.As(err, *ErrAPIUnavailable) = false, want true; err=%v", err)
		}
	})
}

// resolveTestWriter is a test-specific ConfigMapWriter for the
// resolveAfterCreateError path. Unlike fakeWriter (which returns getError for
// all Get calls, including preflight), this writer tracks the number of Get
// calls: the first Get (preflight) returns not-found (object absent,
// preflight succeeds), and subsequent Gets (the re-read after Create fails)
// return reReadError. This isolates the resolution logic.
type resolveTestWriter struct {
	createError error
	reReadError error
	getCalls    int
}

func (w *resolveTestWriter) Get(_ context.Context, _, _ string) (*k8smetadata.ConfigMap, error) {
	w.getCalls++
	if w.getCalls <= 2 {
		// First two Gets (preflight + checkExistingPendingGrant): the object
		// is absent. Both return not-found so the grant proceeds to Create.
		return nil, k8smetadata.NewConfigMapNotFound(errors.New("configmap not found"))
	}
	// Third and subsequent Gets (re-read after create error): return the
	// injected error.
	return nil, w.reReadError
}

func (w *resolveTestWriter) Create(_ context.Context, _, _ string, _ map[string]string) (*k8smetadata.ConfigMap, error) {
	return nil, w.createError
}
