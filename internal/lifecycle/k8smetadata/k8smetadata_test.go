package k8smetadata

import (
	"context"
	"errors"
	"strings"
	"testing"

	"gossipgrid/internal/lifecycle"
)

// typedGetter is a deterministic, concurrency-safe ConfigMapGetter for tests.
// It returns a typed API error (via the convenience constructors) so the
// provider's typed classification is exercised.
type typedGetter struct {
	cm    *ConfigMap
	err   error
	calls int
}

func (f *typedGetter) Get(_ context.Context, _, _ string) (*ConfigMap, error) {
	f.calls++
	return f.cm, f.err
}

func validDoc() string {
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

func newTestProvider(t *testing.T, getter ConfigMapGetter) *Provider {
	t.Helper()
	p, err := NewProvider(ProviderConfig{Namespace: "gossipgrid", Name: "gossipgrid-cluster-metadata"}, getter)
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}
	return p
}

func TestNewProviderRejectsInvalidConfig(t *testing.T) {
	if _, err := NewProvider(ProviderConfig{}, &typedGetter{}); err == nil {
		t.Error("expected error for empty config")
	}
	if _, err := NewProvider(ProviderConfig{Namespace: "ns"}, &typedGetter{}); err == nil {
		t.Error("expected error for empty name")
	}
	if _, err := NewProvider(ProviderConfig{Namespace: "ns", Name: "cm"}, nil); err == nil {
		t.Error("expected error for nil getter")
	}
}

func TestReadValidMetadata(t *testing.T) {
	getter := &typedGetter{cm: &ConfigMap{
		ResourceVersion: "rv-12345",
		Data:            map[string]string{metadataKey: validDoc()},
	}}
	p := newTestProvider(t, getter)
	meta, err := p.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if meta.ClusterID != "0190e8a2-6b0c-7000-8000-000000000000" {
		t.Errorf("ClusterID = %q", meta.ClusterID)
	}
	if meta.EpochID != "0190e8a3-1c2d-7000-9000-111111111111" {
		t.Errorf("EpochID = %q", meta.EpochID)
	}
	if meta.Generation != 1 {
		t.Errorf("Generation = %d", meta.Generation)
	}
	if !meta.Initialized {
		t.Error("Initialized = false, want true")
	}
	// The genesis fields must be parsed and preserved (not discarded).
	if meta.GenesisAuthorized {
		t.Error("GenesisAuthorized = true, want false")
	}
	if meta.GenesisConsumer != "gossipgrid-0" {
		t.Errorf("GenesisConsumer = %q, want gossipgrid-0", meta.GenesisConsumer)
	}
	if !meta.EverAcceptedMessages {
		t.Error("EverAcceptedMessages = false, want true")
	}
	if meta.Revision != "rv-12345" {
		t.Errorf("Revision = %q, want %q", meta.Revision, "rv-12345")
	}
	if getter.calls != 1 {
		t.Errorf("getter calls = %d, want 1", getter.calls)
	}
}

func TestReadAbsentConfigMap(t *testing.T) {
	// A typed not-found error (404 / IsNotFound).
	notFound := NewConfigMapNotFound(errors.New("configmaps \"gossipgrid-cluster-metadata\" not found"))
	getter := &typedGetter{err: notFound}
	p := newTestProvider(t, getter)
	_, err := p.Read(context.Background())
	// Must satisfy the existing lifecycle.ErrMetadataUnavailable contract.
	if !errors.Is(err, lifecycle.ErrMetadataUnavailable) {
		t.Fatalf("Read = %v, want ErrMetadataUnavailable", err)
	}
	// The distinct missing-object marker must be independently inspectable.
	var nf *ErrConfigMapNotFound
	if !errors.As(err, &nf) {
		t.Fatalf("Read = %v, want distinct ErrConfigMapNotFound marker", err)
	}
	// The original error must be preserved as the leaf of the unwrap chain.
	// (errors.Is cannot match a leaf errors.New by identity, so we walk the
	// unwrap chain to confirm the original message is present.)
	if !containsInChain(nf, "configmaps \"gossipgrid-cluster-metadata\" not found") {
		t.Errorf("Read = %v, original error not preserved in the chain", err)
	}
}

// containsInChain reports whether the message appears anywhere in the
// unwrap chain rooted at err.
func containsInChain(err error, msg string) bool {
	for err != nil {
		if strings.Contains(err.Error(), msg) {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

func TestReadNilConfigMap(t *testing.T) {
	// A nil ConfigMap response with no error is treated as a missing object.
	getter := &typedGetter{cm: nil}
	p := newTestProvider(t, getter)
	_, err := p.Read(context.Background())
	if !errors.Is(err, lifecycle.ErrMetadataUnavailable) {
		t.Fatalf("Read = %v, want ErrMetadataUnavailable", err)
	}
	var nf *ErrConfigMapNotFound
	if !errors.As(err, &nf) {
		t.Errorf("Read = %v, want distinct ErrConfigMapNotFound marker", err)
	}
}

func TestReadNoMetadataKey(t *testing.T) {
	// ConfigMap exists but the "metadata" key is absent. A present object with
	// a missing document is MALFORMED (corrupted), not absent.
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{"other": "x"}}}
	p := newTestProvider(t, getter)
	_, err := p.Read(context.Background())
	if !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for missing key", err)
	}
	if !errors.Is(err, ErrMetadataMissingDocument) {
		t.Errorf("Read = %v, want distinct ErrMetadataMissingDocument sentinel", err)
	}
	// It must NOT be classified as unavailable (the object is present).
	if errors.Is(err, lifecycle.ErrMetadataUnavailable) {
		t.Error("a present ConfigMap with no document should not be ErrMetadataUnavailable")
	}
}

func TestReadEmptyMetadataKey(t *testing.T) {
	// A blank document is a present object with a missing document: malformed,
	// not absent.
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: "   "}}}
	p := newTestProvider(t, getter)
	_, err := p.Read(context.Background())
	if !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for blank key", err)
	}
	if !errors.Is(err, ErrMetadataMissingDocument) {
		t.Errorf("Read = %v, want distinct ErrMetadataMissingDocument sentinel", err)
	}
}

func TestReadMalformedJSON(t *testing.T) {
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: "{not json"}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed", err)
	}
}

func TestReadNonObjectDocument(t *testing.T) {
	// A top-level JSON array is a malformed document.
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: `[1,2,3]`}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for non-object", err)
	}
}

func TestReadWrongFieldType(t *testing.T) {
	// generation is a string, not an int.
	doc := `{
		"schemaVersion": 1,
		"clusterId": "abc",
		"epochId": "def",
		"generation": "not-an-int",
		"initialized": true,
		"genesisAuthorized": false,
		"genesisConsumer": "",
		"everAcceptedMessages": false
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for wrong field type", err)
	}
}

func TestReadMissingBoolField(t *testing.T) {
	// initialized is omitted. encoding/json would silently default a missing
	// bool to false; the pointer field makes its absence a validation failure.
	doc := `{
		"schemaVersion": 1,
		"clusterId": "abc",
		"epochId": "def",
		"generation": 1,
		"genesisAuthorized": false,
		"genesisConsumer": "",
		"everAcceptedMessages": false
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for missing initialized", err)
	}
}

func TestReadMissingBoolFieldFalse(t *testing.T) {
	// genesisAuthorized is omitted. A missing bool is NOT the same as a
	// false value; it is malformed.
	doc := `{
		"schemaVersion": 1,
		"clusterId": "abc",
		"epochId": "def",
		"generation": 1,
		"initialized": false,
		"genesisConsumer": "",
		"everAcceptedMessages": false
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for missing genesisAuthorized", err)
	}
}

func TestReadMissingStringField(t *testing.T) {
	// clusterId is omitted (a missing string, not an empty one).
	doc := `{
		"schemaVersion": 1,
		"epochId": "def",
		"generation": 1,
		"initialized": false,
		"genesisAuthorized": true,
		"genesisConsumer": "",
		"everAcceptedMessages": false
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for missing clusterId", err)
	}
}

func TestReadMissingGenesisConsumer(t *testing.T) {
	// genesisConsumer is omitted. It is required (may be empty, but must be
	// present).
	doc := `{
		"schemaVersion": 1,
		"clusterId": "abc",
		"epochId": "def",
		"generation": 1,
		"initialized": false,
		"genesisAuthorized": true,
		"everAcceptedMessages": false
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for missing genesisConsumer", err)
	}
}

func TestReadMissingEverAccepted(t *testing.T) {
	// everAcceptedMessages is omitted.
	doc := `{
		"schemaVersion": 1,
		"clusterId": "abc",
		"epochId": "def",
		"generation": 1,
		"initialized": true,
		"genesisAuthorized": false,
		"genesisConsumer": "gossipgrid-0",
		"dataLossProtected": true
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for missing everAcceptedMessages", err)
	}
}

func TestReadMissingDataLossProtected(t *testing.T) {
	// dataLossProtected is omitted. It is required in schema v1 (the
	// conservative data-loss protection flag).
	doc := `{
		"schemaVersion": 1,
		"clusterId": "abc",
		"epochId": "def",
		"generation": 1,
		"initialized": true,
		"genesisAuthorized": false,
		"genesisConsumer": "",
		"everAcceptedMessages": false
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for missing dataLossProtected", err)
	}
}

func TestReadFailedValidation(t *testing.T) {
	// Valid JSON, supported version, but empty clusterId → validation fails.
	doc := `{
		"schemaVersion": 1,
		"clusterId": "",
		"epochId": "def",
		"generation": 1,
		"initialized": false,
		"genesisAuthorized": true,
		"genesisConsumer": "",
		"everAcceptedMessages": false
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for failed validation", err)
	}
}

func TestReadIDTooLong(t *testing.T) {
	// clusterId exceeds the documented byte-length limit (64 bytes).
	longID := make([]byte, 65)
	for i := range longID {
		longID[i] = 'a'
	}
	doc := `{
		"schemaVersion": 1,
		"clusterId": "` + string(longID) + `",
		"epochId": "def",
		"generation": 1,
		"initialized": true,
		"genesisAuthorized": false,
		"genesisConsumer": "",
		"everAcceptedMessages": true
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for oversized clusterId", err)
	}
}

func TestReadUnsupportedSchemaVersion(t *testing.T) {
	doc := `{
		"schemaVersion": 99,
		"clusterId": "abc",
		"epochId": "def",
		"generation": 1,
		"initialized": true,
		"genesisAuthorized": false,
		"genesisConsumer": "",
		"everAcceptedMessages": true
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, ErrMetadataUnsupportedVersion) {
		t.Fatalf("Read = %v, want ErrMetadataUnsupportedVersion", err)
	}
}

func TestReadMissingSchemaVersion(t *testing.T) {
	// A document with no schemaVersion field is unsupported (not merely
	// malformed): the version is required and 0 is not a supported version.
	doc := `{
		"clusterId": "abc",
		"epochId": "def",
		"generation": 1,
		"initialized": true,
		"genesisAuthorized": false,
		"genesisConsumer": "",
		"everAcceptedMessages": true
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, ErrMetadataUnsupportedVersion) {
		t.Fatalf("Read = %v, want ErrMetadataUnsupportedVersion for missing schema", err)
	}
}

func TestReadIncoherentState(t *testing.T) {
	// GenesisConsumer is set but Initialized is false: incoherent (a consumed
	// authorization implies the cluster was initialized).
	doc := `{
		"schemaVersion": 1,
		"clusterId": "abc",
		"epochId": "def",
		"generation": 1,
		"initialized": false,
		"genesisAuthorized": false,
		"genesisConsumer": "gossipgrid-0",
		"everAcceptedMessages": false
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for incoherent state", err)
	}
}

func TestReadAPIUnavailable(t *testing.T) {
	// A typed transient-unavailability error (5xx / network).
	apiErr := NewAPIUnavailable(errors.New("503 service unavailable"))
	getter := &typedGetter{err: apiErr}
	p := newTestProvider(t, getter)
	_, err := p.Read(context.Background())
	if !errors.Is(err, lifecycle.ErrMetadataUnavailable) {
		t.Fatalf("Read = %v, want ErrMetadataUnavailable for API error", err)
	}
	// The distinct unavailability marker must be independently inspectable.
	var ua *ErrAPIUnavailable
	if !errors.As(err, &ua) {
		t.Errorf("Read = %v, want distinct ErrAPIUnavailable marker", err)
	}
}

func TestReadUnclassifiedAPIError(t *testing.T) {
	// A getter that returns an error with no API classification (a plain
	// errors.New) is conservatively treated as an unavailability, never as a
	// permission error.
	plain := errors.New("some unclassified failure")
	getter := &typedGetter{err: plain}
	p := newTestProvider(t, getter)
	_, err := p.Read(context.Background())
	if !errors.Is(err, lifecycle.ErrMetadataUnavailable) {
		t.Fatalf("Read = %v, want ErrMetadataUnavailable for unclassified error", err)
	}
	var ua *ErrAPIUnavailable
	if !errors.As(err, &ua) {
		t.Errorf("Read = %v, want ErrAPIUnavailable marker for unclassified error", err)
	}
	if errors.Is(err, ErrMetadataPermissionDenied) {
		t.Error("an unclassified error must never be classified as permission denied")
	}
}

func TestReadPermissionDenied(t *testing.T) {
	// A typed 403 forbidden error.
	forbidden := NewAPIPermissionDenied(errors.New("forbidden: user cannot get"))
	getter := &typedGetter{err: forbidden}
	p := newTestProvider(t, getter)
	_, err := p.Read(context.Background())
	if !errors.Is(err, ErrMetadataPermissionDenied) {
		t.Fatalf("Read = %v, want ErrMetadataPermissionDenied", err)
	}
	// It must NOT be classified as unavailable.
	if errors.Is(err, lifecycle.ErrMetadataUnavailable) {
		t.Error("permission denied should not be classified as ErrMetadataUnavailable")
	}
}

func TestReadPermissionDeniedNotByText(t *testing.T) {
	// Regression: the provider must NOT classify a permission error by
	// searching the message text. A not-found typed error whose message
	// happens to contain "forbidden" must still be classified as not-found,
	// not permission-denied.
	notFound := NewConfigMapNotFound(errors.New("forbidden in message text but really a 404"))
	getter := &typedGetter{err: notFound}
	p := newTestProvider(t, getter)
	_, err := p.Read(context.Background())
	if errors.Is(err, ErrMetadataPermissionDenied) {
		t.Errorf("Read = %v; must not classify by message text as permission denied", err)
	}
	var nf *ErrConfigMapNotFound
	if !errors.As(err, &nf) {
		t.Errorf("Read = %v; the typed not-found classification must win over message text", err)
	}
}

func TestReadContextCanceled(t *testing.T) {
	// The context is canceled; the provider must surface the context error
	// promptly, even if the getter's error is a wrapped context error.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	getter := &typedGetter{err: NewAPIUnavailable(ctx.Err())}
	p := newTestProvider(t, getter)
	_, err := p.Read(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Read = %v, want context.Canceled", err)
	}
}

func TestReadContextAlreadyCanceled(t *testing.T) {
	// The context is already canceled before the getter is even called.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: validDoc()}}}
	p := newTestProvider(t, getter)
	_, err := p.Read(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Read = %v, want context.Canceled", err)
	}
	if getter.calls != 0 {
		t.Errorf("getter should not be called when context is pre-canceled; calls = %d", getter.calls)
	}
}

func TestReadResourceVersionEmpty(t *testing.T) {
	// A ConfigMap with an empty resourceVersion: Revision is empty, no error.
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: validDoc()}}}
	p := newTestProvider(t, getter)
	meta, err := p.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if meta.Revision != "" {
		t.Errorf("Revision = %q, want empty", meta.Revision)
	}
}

func TestReadWithLastResetIncident(t *testing.T) {
	// A document with a lastReset incident present (schema v1, optional field).
	// lastReset.epochId must match the current epochId.
	doc := `{
		"schemaVersion": 1,
		"clusterId": "abc",
		"epochId": "def",
		"generation": 2,
		"initialized": true,
		"genesisAuthorized": false,
		"genesisConsumer": "gossipgrid-0",
		"everAcceptedMessages": true,
		"dataLossProtected": true,
		"lastReset": {
			"epochId": "def",
			"cause": "operator",
			"acknowledged": false,
			"timestamp": "2026-11-23T00:00:00Z"
		}
	}`
	getter := &typedGetter{cm: &ConfigMap{ResourceVersion: "rv-9", Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	meta, err := p.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if meta.Generation != 2 {
		t.Errorf("Generation = %d, want 2", meta.Generation)
	}
	if meta.Revision != "rv-9" {
		t.Errorf("Revision = %q, want rv-9", meta.Revision)
	}
	// The lastReset incident must be preserved.
	if meta.LastReset == nil {
		t.Fatal("LastReset = nil, want an incident")
	}
	if meta.LastReset.EpochID != "def" {
		t.Errorf("LastReset.EpochID = %q, want def", meta.LastReset.EpochID)
	}
	if meta.LastReset.Cause != "operator" {
		t.Errorf("LastReset.Cause = %q, want operator", meta.LastReset.Cause)
	}
}

func TestReadLastResetMissingSubfield(t *testing.T) {
	// A lastReset incident present but with a missing required subfield
	// (acknowledged) → malformed.
	doc := `{
		"schemaVersion": 1,
		"clusterId": "abc",
		"epochId": "def",
		"generation": 2,
		"initialized": true,
		"genesisAuthorized": false,
		"genesisConsumer": "gossipgrid-0",
		"everAcceptedMessages": true,
		"lastReset": {
			"epochId": "def",
			"cause": "operator",
			"timestamp": "2026-11-23T00:00:00Z"
		}
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for lastReset missing subfield", err)
	}
}

func TestReadLastResetEpochMismatch(t *testing.T) {
	// A lastReset incident whose epochId does not match the current epochId →
	// incoherent (malformed).
	doc := `{
		"schemaVersion": 1,
		"clusterId": "abc",
		"epochId": "def",
		"generation": 2,
		"initialized": true,
		"genesisAuthorized": false,
		"genesisConsumer": "gossipgrid-0",
		"everAcceptedMessages": true,
		"lastReset": {
			"epochId": "other",
			"cause": "operator",
			"acknowledged": false,
			"timestamp": "2026-11-23T00:00:00Z"
		}
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for lastReset epoch mismatch", err)
	}
}

// TestReadLastResetNonProtectedCluster is the regression test for the
// parse-ordering bug where the lastReset incident was not populated on the
// metadata before meta.Validate() ran. A valid-looking lastReset incident
// (well-typed, epochId matching the current epochId) on a
// dataLossProtected=false cluster is incoherent: a 5C reset records the
// consumer and arms the protection in the same CAS, so a reset incident on
// a non-protected cluster must be rejected as lifecycle.ErrMetadataMalformed.
// Before the fix, the incident was applied to the metadata after Validate,
// so the !DataLossProtected / LastReset!=nil rule in Validate never fired and
// the document was incorrectly accepted.
func TestReadLastResetNonProtectedCluster(t *testing.T) {
	doc := `{
		"schemaVersion": 1,
		"clusterId": "abc",
		"epochId": "def",
		"generation": 2,
		"initialized": true,
		"genesisAuthorized": false,
		"genesisConsumer": "",
		"everAcceptedMessages": true,
		"dataLossProtected": false,
		"lastReset": {
			"epochId": "def",
			"cause": "operator",
			"acknowledged": false,
			"timestamp": "2026-11-23T00:00:00Z"
		}
	}`
	getter := &typedGetter{cm: &ConfigMap{Data: map[string]string{metadataKey: doc}}}
	p := newTestProvider(t, getter)
	if _, err := p.Read(context.Background()); !errors.Is(err, lifecycle.ErrMetadataMalformed) {
		t.Fatalf("Read = %v, want ErrMetadataMalformed for lastReset on non-protected cluster", err)
	}
}

func TestProviderImplementsMetadataSource(t *testing.T) {
	// Compile-time check: *Provider satisfies lifecycle.MetadataSource.
	var _ lifecycle.MetadataSource = (*Provider)(nil)
}

func TestClassifyAPIError(t *testing.T) {
	ctx := context.Background()

	if err := classifyAPIError(ctx, NewAPIPermissionDenied(errors.New("403"))); !errors.Is(err, ErrMetadataPermissionDenied) {
		t.Errorf("typed 403 should map to ErrMetadataPermissionDenied, got %v", err)
	}
	// typed not-found should map to a missing-object marker.
	var nf *ErrConfigMapNotFound
	if err := classifyAPIError(ctx, NewConfigMapNotFound(errors.New("not found"))); !errors.As(err, &nf) {
		t.Errorf("typed not-found should map to an ErrConfigMapNotFound marker, got %v", err)
	}
	// typed 500 should map to an unavailability marker.
	var ua *ErrAPIUnavailable
	if err := classifyAPIError(ctx, NewAPIUnavailable(errors.New("500"))); !errors.As(err, &ua) {
		t.Errorf("typed 500 should map to an ErrAPIUnavailable marker, got %v", err)
	}
	// Unclassified error → conservative unavailability marker.
	var ua2 *ErrAPIUnavailable
	if err := classifyAPIError(ctx, errors.New("some error")); !errors.As(err, &ua2) {
		t.Errorf("unclassified error should map to an ErrAPIUnavailable marker, got %v", err)
	}
	if err := classifyAPIError(ctx, nil); err != nil {
		t.Errorf("nil error should map to nil, got %v", err)
	}
}

func TestClassOf(t *testing.T) {
	cases := []struct {
		err    error
		expect APIErrorClass
	}{
		{nil, APIClassUnknown},
		{errors.New("plain"), APIClassUnknown},
		{NewConfigMapNotFound(errors.New("nf")), APIClassNotFound},
		{NewAPIPermissionDenied(errors.New("403")), APIClassForbidden},
		{NewAPIUnavailable(errors.New("500")), APIClassUnavailable},
		{NewAPIError(APIClassUnknown, errors.New("x")), APIClassUnknown},
	}
	for i, tc := range cases {
		if got := ClassOf(tc.err); got != tc.expect {
			t.Errorf("case %d: ClassOf(%v) = %v, want %v", i, tc.err, got, tc.expect)
		}
	}
}

func TestAPIErrorUnwrapPreservesOriginal(t *testing.T) {
	orig := errors.New("root cause")
	err := NewConfigMapNotFound(orig)
	if !errors.Is(err, orig) {
		t.Errorf("Unwrap should preserve the original error, got %v", err)
	}
}

// TestClassifyNewAPINotFound is the regression test for the typed
// NewAPIError(APIClassNotFound, original) constructor: the not-found
// classification must reliably expose the distinct ErrConfigMapNotFound
// marker AND preserve the original error, without relying on the getter
// having used the dedicated NewConfigMapNotFound constructor.
func TestClassifyNewAPINotFound(t *testing.T) {
	orig := errors.New(`configmaps "gossipgrid-cluster-metadata" not found`)
	classified := NewAPIError(APIClassNotFound, orig)
	// The typed classification must be preserved.
	if ClassOf(classified) != APIClassNotFound {
		t.Fatalf("ClassOf(NewAPIError(APIClassNotFound, ...)) = %v, want APIClassNotFound", ClassOf(classified))
	}
	// The distinct missing-object marker must be independently inspectable
	// alongside lifecycle.ErrMetadataUnavailable.
	var nf *ErrConfigMapNotFound
	// The original error must also remain reachable as a leaf.
	if !containsInChain(classified, orig.Error()) {
		t.Errorf("NewAPIError(APIClassNotFound, original): original not preserved in chain, got %v", classified)
	}
	got := classifyAPIError(context.Background(), classified)
	if !errors.Is(got, lifecycle.ErrMetadataUnavailable) {
		t.Errorf("classifyAPIError(NewAPIError(APIClassNotFound, ...)) = %v, want ErrMetadataUnavailable", got)
	}
	if !errors.As(got, &nf) {
		t.Errorf("classifyAPIError(NewAPIError(APIClassNotFound, ...)) = %v, want ErrConfigMapNotFound marker", got)
	}
	if !containsInChain(got, orig.Error()) {
		t.Errorf("classifyAPIError(NewAPIError(APIClassNotFound, ...)) = %v, original not preserved in chain", got)
	}
}

// TestClassifyNewAPIUnavailable is the regression test for the typed
// NewAPIError(APIClassUnavailable, original) constructor: the unavailability
// classification must reliably expose the distinct ErrAPIUnavailable marker
// AND preserve the original error.
func TestClassifyNewAPIUnavailable(t *testing.T) {
	orig := errors.New("503 service unavailable")
	classified := NewAPIError(APIClassUnavailable, orig)
	if ClassOf(classified) != APIClassUnavailable {
		t.Fatalf("ClassOf(NewAPIError(APIClassUnavailable, ...)) = %v, want APIClassUnavailable", ClassOf(classified))
	}
	if !containsInChain(classified, orig.Error()) {
		t.Errorf("NewAPIError(APIClassUnavailable, original): original not preserved in chain, got %v", classified)
	}
	got := classifyAPIError(context.Background(), classified)
	if !errors.Is(got, lifecycle.ErrMetadataUnavailable) {
		t.Errorf("classifyAPIError(NewAPIError(APIClassUnavailable, ...)) = %v, want ErrMetadataUnavailable", got)
	}
	var ua *ErrAPIUnavailable
	if !errors.As(got, &ua) {
		t.Errorf("classifyAPIError(NewAPIError(APIClassUnavailable, ...)) = %v, want ErrAPIUnavailable marker", got)
	}
	if !containsInChain(got, orig.Error()) {
		t.Errorf("classifyAPIError(NewAPIError(APIClassUnavailable, ...)) = %v, original not preserved in chain", got)
	}
}

// TestClassifyPermissionDeniedPreservesOriginal is the regression test for
// the %w (not %v) fix in the APIClassForbidden branch of classifyAPIError:
// the original 403 error's unwrap chain must remain reachable via
// errors.Is / errors.Unwalk, not just via the message text.
func TestClassifyPermissionDeniedPreservesOriginal(t *testing.T) {
	// A typed 403 wrapping a distinct sentinel (simulating a 7B adapter that
	// wraps the k8s.io/apimachinery StatusError in a typed error whose own
	// Unwrap chain includes a recognizable sentinel).
	sentinel := errors.New("forbidden: user \"system:serviceaccount:gossipgrid:default\" cannot get configmaps")
	classified := NewAPIPermissionDenied(sentinel)
	got := classifyAPIError(context.Background(), classified)
	if !errors.Is(got, ErrMetadataPermissionDenied) {
		t.Fatalf("classifyAPIError(NewAPIPermissionDenied(...)) = %v, want ErrMetadataPermissionDenied", got)
	}
	// The original 403 error's message must be reachable as the leaf of the
	// unwrap chain. With %w, errors.Is(got, sentinel) holds; with %v it would
	// not.
	if !errors.Is(got, sentinel) {
		t.Errorf("classifyAPIError(NewAPIPermissionDenied(...)): original 403 sentinel not preserved via %%w; got %v", got)
	}
	// The original must also NOT be classified as unavailable.
	if errors.Is(got, lifecycle.ErrMetadataUnavailable) {
		t.Error("a permission error must not be classified as ErrMetadataUnavailable")
	}
}

// TestReadGenericAPINotFound is the end-to-end regression: a getter that
// returns the typed NewAPIError(APIClassNotFound, original) constructor (a
// 7B adapter shape) must produce an error that exposes the missing-object
// marker and preserves the original.
func TestReadGenericAPINotFound(t *testing.T) {
	orig := errors.New(`configmaps "gossipgrid-cluster-metadata" not found`)
	getter := &typedGetter{err: NewAPIError(APIClassNotFound, orig)}
	p := newTestProvider(t, getter)
	_, err := p.Read(context.Background())
	if !errors.Is(err, lifecycle.ErrMetadataUnavailable) {
		t.Fatalf("Read = %v, want ErrMetadataUnavailable", err)
	}
	var nf *ErrConfigMapNotFound
	if !errors.As(err, &nf) {
		t.Fatalf("Read = %v, want distinct ErrConfigMapNotFound marker", err)
	}
	if !containsInChain(err, orig.Error()) {
		t.Errorf("Read = %v, original not preserved in chain", err)
	}
}

// TestReadGenericAPIUnavailable is the end-to-end regression for
// NewAPIError(APIClassUnavailable, original).
func TestReadGenericAPIUnavailable(t *testing.T) {
	orig := errors.New("500 internal server error")
	getter := &typedGetter{err: NewAPIError(APIClassUnavailable, orig)}
	p := newTestProvider(t, getter)
	_, err := p.Read(context.Background())
	if !errors.Is(err, lifecycle.ErrMetadataUnavailable) {
		t.Fatalf("Read = %v, want ErrMetadataUnavailable", err)
	}
	var ua *ErrAPIUnavailable
	if !errors.As(err, &ua) {
		t.Fatalf("Read = %v, want distinct ErrAPIUnavailable marker", err)
	}
	if !containsInChain(err, orig.Error()) {
		t.Errorf("Read = %v, original not preserved in chain", err)
	}
}
