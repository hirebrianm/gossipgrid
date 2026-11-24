// Package k8smetadata: a read-only Kubernetes ConfigMap implementation of the
// lifecycle.MetadataSource interface (Milestone 5B, Stage 0).
//
// The Provider reads the explicitly configured named ConfigMap in the
// configured namespace, parses the versioned JSON metadata document stored at
// data["metadata"], and returns the resulting lifecycle.ClusterMetadata with
// the Kubernetes resourceVersion populated in the Revision field.
//
// The Provider is read-only: it performs a single Get on the named ConfigMap
// and has no write methods. It is inactive in the running application in
// Stage 0 (lifecycle protection remains disabled and the provider is not
// wired into cmd/gossipgrid/main.go). Enabling the provider and the lifecycle
// layer together, along with the Helm changes, belong to Milestone 7B.
//
// The Provider takes an injectable ConfigMapGetter interface so unit tests can
// supply a deterministic fake without the official Kubernetes client
// libraries. The official client-go ConfigMap client is only needed at the
// wiring site (7B); it is not a dependency of this package in Stage 0.
//
// Error classification (all distinct, inspectable via errors.Is):
//
//   - the ConfigMap does not exist            → lifecycle.ErrMetadataUnavailable
//     (a distinct missing-object sentinel is wrapped, see ErrConfigMapNotFound)
//   - the Kubernetes API is unavailable      → lifecycle.ErrMetadataUnavailable
//     (network/timeout/5xx; a distinct sentinel is wrapped, see ErrAPIUnavailable)
//   - the API denies access (403)            → ErrMetadataPermissionDenied
//   - the data["metadata"] field is missing  → lifecycle.ErrMetadataMalformed
//     (a present ConfigMap with no document is corrupted, not absent)
//   - the JSON is malformed or a field type  → lifecycle.ErrMetadataMalformed
//     is wrong, or validation fails
//   - the schema version is not supported    → ErrMetadataUnsupportedVersion
//   - the caller's context is canceled       → the context error (ctx.Err())
//
// The API error classification is TYPED: the getter returns an error whose
// classification is carried by an inspectable sentinel (APIErrorClass, or the
// convenience constructors NewAPIError / NewAPIUnavailable / NewConfigMapNotFound).
// The Provider never classifies an API status by searching the error's message
// text. A getter that reports no classification is mapped to
// ErrAPIUnavailable (the conservative default), never to a permission error.
package k8smetadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"gossipgrid/internal/lifecycle"
)

// APIErrorClass is the classification of a Kubernetes API error returned by a
// ConfigMapGetter. It carries the error classification so the Provider can
// classify failures without inspecting the error's message text.
type APIErrorClass int

const (
	// APIClassUnknown is an unclassified API error. It is treated as
	// ErrAPIUnavailable (the conservative default).
	APIClassUnknown APIErrorClass = iota
	// APIClassNotFound is a missing-object (HTTP 404 / IsNotFound) error.
	APIClassNotFound
	// APIClassForbidden is a permission-denied (HTTP 403) error.
	APIClassForbidden
	// APIClassUnavailable is a transient unavailability (network/timeout/5xx)
	// error.
	APIClassUnavailable
)

// apiError is a typed error carrying an API error classification and the
// original error it wraps. It is the canonical shape of a ConfigMapGetter.Get
// error. In production (7B) it is produced by an adapter over the official
// k8s.io/apimachinery StatusError, using apierrors.IsNotFound / IsForbidden —
// not message-text matching. In Stage 0 unit tests it is produced by the
// convenience constructors.
type apiError struct {
	class APIErrorClass
	err   error
}

func (e *apiError) Error() string {
	label := "unknown"
	switch e.class {
	case APIClassNotFound:
		label = "not found"
	case APIClassForbidden:
		label = "permission denied"
	case APIClassUnavailable:
		label = "unavailable"
	}
	if e.err != nil {
		return "kubernetes api error (" + label + "): " + e.err.Error()
	}
	return "kubernetes api error (" + label + ")"
}

func (e *apiError) Unwrap() error { return e.err }

// NewAPIError returns a typed API error of the given class wrapping the
// original error. This is the constructor the 7B adapter uses after classifying
// a k8s.io/apimachinery StatusError.
func NewAPIError(class APIErrorClass, err error) error { return &apiError{class: class, err: err} }

// NewAPIPermissionDenied wraps err as a typed permission-denied (403) API error.
func NewAPIPermissionDenied(err error) error { return NewAPIError(APIClassForbidden, err) }

// ClassOf returns the APIErrorClass of err, or APIClassUnknown when err is not
// a typed API error. It recognizes all three typed API error shapes (the
// generic apiError and the two dedicated markers). It is a pure function and
// is exported for test convenience.
func ClassOf(err error) APIErrorClass {
	var e *apiError
	if errors.As(err, &e) {
		return e.class
	}
	var nf *ErrConfigMapNotFound
	if errors.As(err, &nf) {
		return APIClassNotFound
	}
	var ua *ErrAPIUnavailable
	if errors.As(err, &ua) {
		return APIClassUnavailable
	}
	return APIClassUnknown
}

// Provider sentinels.
//
//   - ErrMetadataPermissionDenied is returned when the API denies access (403).
//     It is distinct from a transient API unavailability so callers can
//     distinguish a persistent RBAC misconfiguration from a temporary network or
//     server error. It is inspectable via errors.Is (it is the top-level error
//     for permission failures).
//   - ErrMetadataUnsupportedVersion is returned when the metadata document's
//     schemaVersion is not one this Provider understands. It is distinct from a
//     malformed document (a known version with a bad field) so the operator can
//     distinguish an out-of-date client from a corrupted document. It is
//     inspectable via errors.Is.
//   - A missing ConfigMap (the object does not exist) and a transient API
//     unavailability both map to the existing lifecycle.ErrMetadataUnavailable
//     contract, but their distinct condition is carried by an inspectable
//     marker: ErrConfigMapNotFound and ErrAPIUnavailable. The original error is
//     preserved through wrapping so the full chain is reachable via
//     errors.Is / errors.Unwrap.
//
// The missing-object and unavailability conditions are marker-based (they
// implement Unwrap) so that BOTH the marker and the wrapped original error
// remain inspectable through the same error value. A plain errors.New sentinel
// cannot serve this purpose because it does not implement Unwrap and would
// sever the error chain.
var (
	ErrMetadataPermissionDenied   = errors.New("kubernetes configmap metadata: permission denied")
	ErrMetadataUnsupportedVersion = errors.New("kubernetes configmap metadata: unsupported schema version")
	ErrMetadataMissingDocument    = errors.New("kubernetes configmap metadata document missing")
)

// ErrConfigMapNotFound is an inspectable marker (it implements Unwrap,
// wrapping the original API error) that indicates the named ConfigMap does not
// exist. It is reachable via errors.Is alongside the wrapped original error.
type ErrConfigMapNotFound struct{ err error }

// NewConfigMapNotFound wraps err as a distinct missing-object marker.
func NewConfigMapNotFound(err error) *ErrConfigMapNotFound { return &ErrConfigMapNotFound{err: err} }

func (e *ErrConfigMapNotFound) Error() string {
	if e.err != nil {
		return "kubernetes configmap not found: " + e.err.Error()
	}
	return "kubernetes configmap not found"
}
func (e *ErrConfigMapNotFound) Unwrap() error { return e.err }

// ErrAPIUnavailable is an inspectable marker (it implements Unwrap, wrapping
// the original API error) that indicates a transient API unavailability
// (network/timeout/5xx or an unclassified API error). It is reachable via
// errors.Is alongside the wrapped original error.
type ErrAPIUnavailable struct{ err error }

// NewAPIUnavailable wraps err as a distinct unavailability marker.
func NewAPIUnavailable(err error) *ErrAPIUnavailable { return &ErrAPIUnavailable{err: err} }

func (e *ErrAPIUnavailable) Error() string {
	if e.err != nil {
		return "kubernetes api unavailable: " + e.err.Error()
	}
	return "kubernetes api unavailable"
}
func (e *ErrAPIUnavailable) Unwrap() error { return e.err }

// ensureConfigMapNotFoundMarker returns an error that is guaranteed to
// carry the ErrConfigMapNotFound marker. If the input already carries
// that marker (detected via errors.As) the input is returned unchanged;
// otherwise the input is wrapped in the marker.
//
// This is the single point at which the not-found marker is applied, so
// that the typed API error classification (APIClassNotFound) reliably
// exposes the distinct inspectable marker in BOTH the dedicated-constructor
// case (NewConfigMapNotFound) and the generic NewAPIError case (a 7B
// adapter that classifies via ClassOf and calls NewAPIError directly).
func ensureConfigMapNotFoundMarker(err error) error {
	if err == nil {
		return nil
	}
	var nf *ErrConfigMapNotFound
	if errors.As(err, &nf) {
		return err
	}
	return NewConfigMapNotFound(err)
}

// ensureAPIUnavailableMarker returns an error that is guaranteed to carry
// the ErrAPIUnavailable marker. If the input already carries that marker
// (detected via errors.As) the input is returned unchanged; otherwise the
// input is wrapped in the marker. Same guarantee as
// ensureConfigMapNotFoundMarker for the unavailability condition.
func ensureAPIUnavailableMarker(err error) error {
	if err == nil {
		return nil
	}
	var ua *ErrAPIUnavailable
	if errors.As(err, &ua) {
		return err
	}
	return NewAPIUnavailable(err)
}

// metadataKey is the key in the ConfigMap's data map that holds the JSON
// metadata document.
const metadataKey = "metadata"

// MetadataKey is the exported name of the ConfigMap data key that holds the
// JSON metadata document. It is the same value as the unexported metadataKey
// and exists so that the genesis grant (a separate package that writes the
// document) and other writers can reference the key without redefining it.
// The value is "metadata".
const MetadataKey = "metadata"

// supportedSchemaVersion is the schema version this Provider understands.
const supportedSchemaVersion = 1

// maxIDBytes is the documented byte-length limit for clusterId and epochId
// (the epoch-header bound, lifecycle.MaxEpochHeaderBytes is 64).
const maxIDBytes = 64

// ConfigMapGetter is the narrow, read-only adapter contract the Provider uses
// to obtain the ConfigMap. It is implemented by an adapter over the official
// client-go ConfigMap client (at the 7B wiring site) and by a deterministic
// fake in unit tests.
//
// The returned error must carry its API classification as a typed error (see
// APIErrorClass and the convenience constructors). The Provider classifies the
// error by its type, never by the error's message text. A nil *ConfigMap with a
// nil error is treated as a missing object.
type ConfigMapGetter interface {
	// Get returns the named ConfigMap in the given namespace. On success it
	// returns the ConfigMap and a nil error. On failure it returns a typed
	// error whose classification is carried by an inspectable sentinel
	// (NewAPIError / NewConfigMapNotFound / NewAPIUnavailable /
	// NewAPIPermissionDenied), and a nil ConfigMap. The context supports
	// cancellation and request deadlines.
	Get(ctx context.Context, name, namespace string) (*ConfigMap, error)
}

// ConfigMap is the minimal subset of corev1.ConfigMap the Provider needs. It is
// a value type (not a pointer to the K8s type) so the Provider's package has
// no dependency on k8s.io/api; the 7B wiring site converts a *corev1.ConfigMap
// to this type before calling Read.
type ConfigMap struct {
	// ResourceVersion is the Kubernetes resourceVersion of the ConfigMap.
	// It is populated in ClusterMetadata.Revision for future CAS use.
	ResourceVersion string
	// Data is the ConfigMap's data map. The Provider reads the "metadata"
	// key from this map. A nil Data is treated as a missing document.
	Data map[string]string
}

// ProviderConfig holds the Provider's configuration.
type ProviderConfig struct {
	// Namespace is the namespace of the ConfigMap. It must be nonempty.
	Namespace string
	// Name is the name of the ConfigMap. It must be nonempty.
	Name string
}

// Validate checks the provider configuration for well-formedness.
func (c ProviderConfig) Validate() error {
	if strings.TrimSpace(c.Namespace) == "" {
		return errors.New("kubernetes configmap metadata: namespace is empty")
	}
	if strings.TrimSpace(c.Name) == "" {
		return errors.New("kubernetes configmap metadata: name is empty")
	}
	return nil
}

// Provider is a read-only, ConfigMap-backed implementation of
// lifecycle.MetadataSource. It is safe for concurrent use.
type Provider struct {
	cfg    ProviderConfig
	getter ConfigMapGetter
}

// NewProvider creates a Provider. cfg must be valid and getter must be
// non-nil; both are validated and a non-nil error is returned otherwise. The
// provider has no write capability.
func NewProvider(cfg ProviderConfig, getter ConfigMapGetter) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if getter == nil {
		return nil, errors.New("kubernetes configmap metadata: configmap getter is nil")
	}
	return &Provider{cfg: cfg, getter: getter}, nil
}

// Read implements lifecycle.MetadataSource. It reads the configured named
// ConfigMap, parses the versioned JSON metadata document, and returns the
// resulting lifecycle.ClusterMetadata with the Kubernetes resourceVersion in
// the Revision field. It returns a distinct, inspectable error for each
// failure class (see the package documentation). The caller's context
// cancellation and request deadline are respected.
func (p *Provider) Read(ctx context.Context) (lifecycle.ClusterMetadata, error) {
	if err := ctx.Err(); err != nil {
		return lifecycle.ClusterMetadata{}, err
	}

	cm, err := p.getter.Get(ctx, p.cfg.Name, p.cfg.Namespace)
	if err != nil {
		return lifecycle.ClusterMetadata{}, classifyAPIError(ctx, err)
	}
	// A nil ConfigMap response with no error is treated as a missing object
	// (a safe default; a real getter returns an error on a 404, but a fake or
	// a misbehaving adapter may return a nil pointer).
	if cm == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: %w", lifecycle.ErrMetadataUnavailable, NewConfigMapNotFound(errors.New("nil configmap response")))
	}

	raw, ok := cm.Data[metadataKey]
	if !ok {
		// The ConfigMap exists but has no "metadata" key. This is a present
		// object with a missing/blank document: it is malformed (corrupted
		// authoritative metadata), NOT evidence that the object is absent.
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: %w", lifecycle.ErrMetadataMalformed, ErrMetadataMissingDocument)
	}
	if strings.TrimSpace(raw) == "" {
		// A blank document is likewise a present object with a missing
		// document: malformed, not absent.
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: %w", lifecycle.ErrMetadataMalformed, ErrMetadataMissingDocument)
	}

	meta, err := parseMetadataDocument([]byte(raw))
	if err != nil {
		return lifecycle.ClusterMetadata{}, err
	}

	// Populate the Revision from the ConfigMap's resourceVersion. An empty
	// resourceVersion (e.g. a fake or a non-K8s getter) is permitted; the
	// Revision is simply empty, which is safe (a future CAS would re-read
	// before updating).
	meta.Revision = cm.ResourceVersion

	return meta, nil
}

// classifyAPIError maps a ConfigMapGetter.Get error to the Provider's error
// classification. It is a pure function (no shared state) and is exported for
// test convenience.
//
// The classification is based on the error's TYPED shape (APIErrorClass), never
// on the error's message text:
//   - a context cancellation/timeout error → the context error (so callers can
//     distinguish a canceled read from an API failure)
//   - a permission-denied error (403)       → ErrMetadataPermissionDenied
//   - a not-found error                    → lifecycle.ErrMetadataUnavailable
//     (wrapping the distinct ErrConfigMapNotFound sentinel and the original
//     error)
//   - an unavailable or unclassified error  → lifecycle.ErrMetadataUnavailable
//     (wrapping the distinct ErrAPIUnavailable sentinel and the original error)
func classifyAPIError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	// Context cancellation takes priority: if the caller canceled, surface
	// the context error so the caller can distinguish a cancel from an API
	// failure (an API error can also carry a wrapped context error; we check
	// the context first).
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	switch ClassOf(err) {
	case APIClassForbidden:
		// The permission-denied case. %w preserves the original error's
		// unwrap chain, so the original 403 error remains reachable via
		// errors.Is / errors.Unwalk alongside ErrMetadataPermissionDenied.
		return fmt.Errorf("%w: %w", ErrMetadataPermissionDenied, err)
	case APIClassNotFound:
		// A missing-object condition. Whether the getter produced the
		// marker via NewConfigMapNotFound or via NewAPIError(APIClassNotFound,
		// original), the typed classification (ClassOf) is what we rely on.
		// The helper guarantees the distinct inspectable marker is present
		// and preserves the original error via %w, so both the marker and
		// the original are reachable via errors.Is / errors.As.
		return fmt.Errorf("%w: %w", lifecycle.ErrMetadataUnavailable, ensureConfigMapNotFoundMarker(err))
	case APIClassUnavailable:
		// A transient-unavailability condition. Same marker logic as
		// APIClassNotFound, using the ErrAPIUnavailable marker.
		return fmt.Errorf("%w: %w", lifecycle.ErrMetadataUnavailable, ensureAPIUnavailableMarker(err))
	default:
		// An unclassified API error is conservatively treated as an
		// unavailability (never as a permission error or a missing object).
		// A plain untyped error carries no ErrAPIUnavailable marker, so the
		// helper wraps it explicitly to make the condition inspectable.
		return fmt.Errorf("%w: %w", lifecycle.ErrMetadataUnavailable, ensureAPIUnavailableMarker(err))
	}
}

// metadataDocument is the strict, schema-v1 JSON document stored in the
// ConfigMap. Required fields use *bool / *int / *string so that a missing
// field is distinguishable (nil) from a zero value (false / 0 / ""). encoding/
// json silently converts a missing boolean field to false; the pointers make
// the presence of every required field explicit.
type metadataDocument struct {
	// Required fields (all must be present and well-typed).
	SchemaVersion     *int    `json:"schemaVersion"`
	ClusterID         *string `json:"clusterId"`
	EpochID           *string `json:"epochId"`
	Generation        *int    `json:"generation"`
	Initialized       *bool   `json:"initialized"`
	GenesisAuthorized *bool   `json:"genesisAuthorized"`
	GenesisConsumer   *string `json:"genesisConsumer"`
	EverAccepted      *bool   `json:"everAcceptedMessages"`
	DataLossProtected *bool   `json:"dataLossProtected"`
	// Optional fields.
	LastReset *resetIncident `json:"lastReset,omitempty"`
}

// resetIncident is the optional last-reset incident record in the metadata
// document. It is present only after a reset has occurred. When present, its
// required subfields must be well-typed (presence is validated with the same
// pointer discipline as the top-level required fields).
type resetIncident struct {
	EpochID      *string `json:"epochId"`
	Cause        *string `json:"cause"`
	Acknowledged *bool   `json:"acknowledged"`
	Timestamp    *string `json:"timestamp"`
}

// parseMetadataDocument parses and validates the JSON metadata document. It
// returns a distinct error for each failure class:
//   - ErrMetadataUnsupportedVersion when the schemaVersion is missing or not
//     supported
//   - lifecycle.ErrMetadataMalformed when the JSON is malformed, a field type
//     is wrong, a required field is missing, or the document fails
//     validation (length limits or coherent metadata states)
func parseMetadataDocument(raw []byte) (lifecycle.ClusterMetadata, error) {
	// Reject a top-level that is not a JSON object (e.g. a JSON array or
	// scalar). A non-object document is malformed, not an unsupported version
	// (an unsupported version is a present object whose schemaVersion field is
	// out of range).
	var rawObj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rawObj); err != nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: %v", lifecycle.ErrMetadataMalformed, err)
	}
	if rawObj == nil {
		// A top-level null is a missing/malformed document.
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: %v", lifecycle.ErrMetadataMalformed, errors.New("document is not a JSON object"))
	}

	var doc metadataDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: %v", lifecycle.ErrMetadataMalformed, err)
	}

	// schemaVersion: required and must be a supported version.
	if doc.SchemaVersion == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: schemaVersion is required", ErrMetadataUnsupportedVersion)
	}
	if *doc.SchemaVersion != supportedSchemaVersion {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: got %d, supported %d", ErrMetadataUnsupportedVersion, *doc.SchemaVersion, supportedSchemaVersion)
	}

	// clusterId: required, nonempty, length-bounded.
	if doc.ClusterID == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: clusterId is required", lifecycle.ErrMetadataMalformed)
	}
	if err := validateIDField("clusterId", *doc.ClusterID); err != nil {
		return lifecycle.ClusterMetadata{}, err
	}

	// epochId: required, nonempty, length-bounded.
	if doc.EpochID == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: epochId is required", lifecycle.ErrMetadataMalformed)
	}
	if err := validateIDField("epochId", *doc.EpochID); err != nil {
		return lifecycle.ClusterMetadata{}, err
	}

	// generation: required and positive.
	if doc.Generation == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: generation is required", lifecycle.ErrMetadataMalformed)
	}
	if *doc.Generation < 1 {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: generation must be positive (got %d)", lifecycle.ErrMetadataMalformed, *doc.Generation)
	}

	// initialized: required.
	if doc.Initialized == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: initialized is required", lifecycle.ErrMetadataMalformed)
	}

	// genesisAuthorized: required.
	if doc.GenesisAuthorized == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: genesisAuthorized is required", lifecycle.ErrMetadataMalformed)
	}

	// genesisConsumer: required; may be empty (not yet consumed).
	if doc.GenesisConsumer == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: genesisConsumer is required", lifecycle.ErrMetadataMalformed)
	}
	if err := validateIDField("genesisConsumer", *doc.GenesisConsumer); err != nil {
		return lifecycle.ClusterMetadata{}, err
	}

	// everAcceptedMessages: required.
	if doc.EverAccepted == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: everAcceptedMessages is required", lifecycle.ErrMetadataMalformed)
	}

	// dataLossProtected: required. The conservative data-loss protection flag,
	// armed during genesis finalization.
	if doc.DataLossProtected == nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: dataLossProtected is required", lifecycle.ErrMetadataMalformed)
	}

	// lastReset: optional. When present, its subfields must be well-typed,
	// and the incident's epochId must be coherent with the current epochId
	// (a reset establishes the current epoch; a mismatched incident is
	// incoherent / corrupted).
	if doc.LastReset != nil {
		if err := validateResetIncident(doc.LastReset); err != nil {
			return lifecycle.ClusterMetadata{}, err
		}
		if *doc.LastReset.EpochID != *doc.EpochID {
			return lifecycle.ClusterMetadata{}, fmt.Errorf(
				"%w: lastReset.epochId %q does not match current epochId %q",
				lifecycle.ErrMetadataMalformed, *doc.LastReset.EpochID, *doc.EpochID)
		}
	}

	meta := lifecycle.ClusterMetadata{
		ClusterID:            *doc.ClusterID,
		EpochID:              *doc.EpochID,
		Generation:           *doc.Generation,
		Initialized:          *doc.Initialized,
		GenesisAuthorized:    *doc.GenesisAuthorized,
		GenesisConsumer:      *doc.GenesisConsumer,
		EverAcceptedMessages: *doc.EverAccepted,
		DataLossProtected:    *doc.DataLossProtected,
	}
	// Populate the LastReset incident BEFORE calling Validate: the
	// lifecycle.ClusterMetadata.Validate state-consistency rules inspect
	// LastReset (e.g. a lastReset incident is incoherent on a
	// non-protected cluster), so the incident must be present on the
	// metadata before the validation runs.
	if doc.LastReset != nil {
		meta.LastReset = &lifecycle.ResetIncident{
			EpochID:      *doc.LastReset.EpochID,
			Cause:        *doc.LastReset.Cause,
			Acknowledged: *doc.LastReset.Acknowledged,
			Timestamp:    *doc.LastReset.Timestamp,
		}
	}
	if err := meta.Validate(); err != nil {
		return lifecycle.ClusterMetadata{}, fmt.Errorf("%w: %v", lifecycle.ErrMetadataMalformed, err)
	}

	return meta, nil
}

// validateIDField checks an identifier field for the documented byte-length
// limit (and, for the required identifiers, nonemptiness is already checked by
// the caller; this guards the optional/derived ones too).
func validateIDField(name, value string) error {
	if len([]byte(value)) > maxIDBytes {
		return fmt.Errorf("%w: %s exceeds %d bytes (got %d)", lifecycle.ErrMetadataMalformed, name, maxIDBytes, len([]byte(value)))
	}
	return nil
}

// validateResetIncident checks the required subfields of an optional lastReset
// incident for presence and type.
func validateResetIncident(r *resetIncident) error {
	if r.EpochID == nil {
		return fmt.Errorf("%w: lastReset.epochId is required", lifecycle.ErrMetadataMalformed)
	}
	if err := validateIDField("lastReset.epochId", *r.EpochID); err != nil {
		return err
	}
	if r.Cause == nil {
		return fmt.Errorf("%w: lastReset.cause is required", lifecycle.ErrMetadataMalformed)
	}
	if *r.Cause == "" {
		return fmt.Errorf("%w: lastReset.cause must be nonempty", lifecycle.ErrMetadataMalformed)
	}
	if r.Acknowledged == nil {
		return fmt.Errorf("%w: lastReset.acknowledged is required", lifecycle.ErrMetadataMalformed)
	}
	if r.Timestamp == nil {
		return fmt.Errorf("%w: lastReset.timestamp is required", lifecycle.ErrMetadataMalformed)
	}
	if *r.Timestamp == "" {
		return fmt.Errorf("%w: lastReset.timestamp must be nonempty", lifecycle.ErrMetadataMalformed)
	}
	return nil
}
