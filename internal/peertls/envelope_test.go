package peertls

import (
	"net/http"
	"testing"

	"gossipgrid/internal/lifecycle"
)

// testEpochProvider is a controllable EpochProvider for envelope validation
// tests. It allows the test to set the current context and enforcement
// state independently.
type testEpochProvider struct {
	ctx         lifecycle.RequestContext
	err         error
	enforcement bool
}

func (p *testEpochProvider) Context() (lifecycle.RequestContext, error) {
	return p.ctx, p.err
}

func (p *testEpochProvider) Enforcement() bool {
	return p.enforcement
}

// --- validateResponseEnvelope tests ---

func TestValidateResponseEnvelopeLegacyPeer(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	// No Lifecycle-Active header: legacy peer.
	headers := http.Header{}
	if err := validateResponseEnvelope(headers, "test-peer", p, p.ctx); err == nil {
		t.Error("expected error for legacy peer (no Lifecycle-Active header)")
	}
}

func TestValidateResponseEnvelopeMissingClusterID(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	// Missing ClusterID.
	if err := validateResponseEnvelope(headers, "test-peer", p, p.ctx); err == nil {
		t.Error("expected error for missing ClusterID header")
	}
}

func TestValidateResponseEnvelopeMissingEpochID(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	headers.Set(lifecycle.HeaderRespClusterID, "c1")
	// Missing EpochID.
	if err := validateResponseEnvelope(headers, "test-peer", p, p.ctx); err == nil {
		t.Error("expected error for missing EpochID header")
	}
}

func TestValidateResponseEnvelopeDuplicateClusterID(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	headers.Add(lifecycle.HeaderRespClusterID, "c1")
	headers.Add(lifecycle.HeaderRespClusterID, "c2") // duplicate, contradictory
	headers.Set(lifecycle.HeaderRespEpochID, "e1")
	if err := validateResponseEnvelope(headers, "test-peer", p, p.ctx); err == nil {
		t.Error("expected error for duplicate ClusterID headers")
	}
}

func TestValidateResponseEnvelopeDuplicateEpochID(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	headers.Set(lifecycle.HeaderRespClusterID, "c1")
	headers.Add(lifecycle.HeaderRespEpochID, "e1")
	headers.Add(lifecycle.HeaderRespEpochID, "e2") // duplicate
	if err := validateResponseEnvelope(headers, "test-peer", p, p.ctx); err == nil {
		t.Error("expected error for duplicate EpochID headers")
	}
}

func TestValidateResponseEnvelopeEmptyClusterID(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	headers.Set(lifecycle.HeaderRespClusterID, "") // empty
	headers.Set(lifecycle.HeaderRespEpochID, "e1")
	if err := validateResponseEnvelope(headers, "test-peer", p, p.ctx); err == nil {
		t.Error("expected error for empty ClusterID header")
	}
}

func TestValidateResponseEnvelopeOversizedClusterID(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	headers.Set(lifecycle.HeaderRespClusterID, "x")
	for i := 0; i < lifecycle.MaxEpochHeaderBytes+1; i++ {
		headers.Set(lifecycle.HeaderRespClusterID, "x")
	}
	// Use a long value instead.
	headers = http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	oversized := make([]byte, lifecycle.MaxEpochHeaderBytes+1)
	for i := range oversized {
		oversized[i] = 'a'
	}
	headers.Set(lifecycle.HeaderRespClusterID, string(oversized))
	headers.Set(lifecycle.HeaderRespEpochID, "e1")
	if err := validateResponseEnvelope(headers, "test-peer", p, p.ctx); err == nil {
		t.Error("expected error for oversized ClusterID header")
	}
}

func TestValidateResponseEnvelopeWrongClusterID(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	headers.Set(lifecycle.HeaderRespClusterID, "c2") // wrong cluster
	headers.Set(lifecycle.HeaderRespEpochID, "e1")
	if err := validateResponseEnvelope(headers, "test-peer", p, p.ctx); err == nil {
		t.Error("expected error for wrong ClusterID in response")
	}
}

func TestValidateResponseEnvelopeWrongEpochID(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	headers.Set(lifecycle.HeaderRespClusterID, "c1")
	headers.Set(lifecycle.HeaderRespEpochID, "e2") // wrong epoch
	if err := validateResponseEnvelope(headers, "test-peer", p, p.ctx); err == nil {
		t.Error("expected error for wrong EpochID in response")
	}
}

func TestValidateResponseEnvelopeValid(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	headers.Set(lifecycle.HeaderRespClusterID, "c1")
	headers.Set(lifecycle.HeaderRespEpochID, "e1")
	if err := validateResponseEnvelope(headers, "test-peer", p, p.ctx); err != nil {
		t.Errorf("expected no error for valid envelope, got: %v", err)
	}
}

func TestValidateResponseEnvelopeDisabled(t *testing.T) {
	// Enforcement disabled: no validation.
	p := &testEpochProvider{enforcement: false}
	headers := http.Header{} // no headers at all
	if err := validateResponseEnvelope(headers, "test-peer", p, lifecycle.RequestContext{}); err != nil {
		t.Errorf("expected no error when enforcement disabled, got: %v", err)
	}
}

func TestValidateResponseEnvelopeNilProvider(t *testing.T) {
	// Nil provider: no validation (legacy).
	headers := http.Header{}
	if err := validateResponseEnvelope(headers, "test-peer", nil, lifecycle.RequestContext{}); err != nil {
		t.Errorf("expected no error with nil provider, got: %v", err)
	}
}

func TestValidateResponseEnvelopeDuplicateLifecycleActive(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	headers.Add(lifecycle.HeaderLifecycleActive, "true") // duplicate
	headers.Set(lifecycle.HeaderRespClusterID, "c1")
	headers.Set(lifecycle.HeaderRespEpochID, "e1")
	if err := validateResponseEnvelope(headers, "test-peer", p, p.ctx); err == nil {
		t.Error("expected error for duplicate Lifecycle-Active headers")
	}
}

// --- validateDiagnosticEnvelope tests ---

func TestValidateDiagnosticEnvelopeUnreadyPeer(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	// An unready peer has Lifecycle-Active=true but no ClusterID/EpochID
	// headers (it has no accepted metadata yet). This must be ACCEPTED
	// (diagnostic status must remain accessible for uninitialized peers).
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	if err := validateDiagnosticEnvelope(headers, "test-peer", p, p.ctx); err != nil {
		t.Errorf("expected no error for unready peer (no epoch headers), got: %v", err)
	}
}

func TestValidateDiagnosticEnvelopeLegacyPeer(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	// Legacy peer: no Lifecycle-Active header. Must be REJECTED.
	headers := http.Header{}
	if err := validateDiagnosticEnvelope(headers, "test-peer", p, p.ctx); err == nil {
		t.Error("expected error for legacy peer in diagnostic status")
	}
}

func TestValidateDiagnosticEnvelopeWrongCluster(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	headers.Set(lifecycle.HeaderRespClusterID, "c2") // wrong cluster
	headers.Set(lifecycle.HeaderRespEpochID, "e1")
	if err := validateDiagnosticEnvelope(headers, "test-peer", p, p.ctx); err == nil {
		t.Error("expected error for wrong cluster in diagnostic status")
	}
}

func TestValidateDiagnosticEnvelopeValid(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	headers.Set(lifecycle.HeaderRespClusterID, "c1")
	headers.Set(lifecycle.HeaderRespEpochID, "e1")
	if err := validateDiagnosticEnvelope(headers, "test-peer", p, p.ctx); err != nil {
		t.Errorf("expected no error for valid diagnostic envelope, got: %v", err)
	}
}

func TestValidateDiagnosticEnvelopePartialEpoch(t *testing.T) {
	p := &testEpochProvider{
		ctx:         lifecycle.RequestContext{ClusterID: "c1", EpochID: "e1"},
		enforcement: true,
	}
	// Only ClusterID present, EpochID absent. Tolerated (partial envelope
	// from an unready replica).
	headers := http.Header{}
	headers.Set(lifecycle.HeaderLifecycleActive, "true")
	headers.Set(lifecycle.HeaderRespClusterID, "c1")
	if err := validateDiagnosticEnvelope(headers, "test-peer", p, p.ctx); err != nil {
		t.Errorf("expected no error for partial diagnostic envelope (ClusterID only), got: %v", err)
	}
}
