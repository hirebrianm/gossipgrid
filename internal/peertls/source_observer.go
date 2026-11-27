// Package peertls: production implementation of the lifecycle
// BootstrapSourceObserver (Milestone 5B, Stage 3A, Finding 4).
//
// BootstrapSourceObserver is the narrow, trusted, dependency-injected boundary
// through which the local joining operation (lifecycle.Controller.AuthorizeJoin)
// obtains authenticated bootstrap-source evidence. This file provides the
// production implementation (BootstrapSourceObserver struct) that wraps the
// existing peer transport and the GET /internal/status endpoint.
//
// The observation is STRICT: it requires a complete, valid, nonempty
// response epoch envelope (both cluster and epoch headers present and
// matching the status body and the expected target), a READY or DEGRADED
// lifecycle state, BootstrapSourceEligible=true, and a peer identity that is
// not the local replica. A peer that reports a partial envelope (an
// unready or uninitialized replica) is NOT an eligible bootstrap source.
//
// The observation uses a purpose-specific transport path
// (doStatusObservationRequest) that does NOT require the local EpochProvider
// to be configured. This is the production-compatible pre-binding
// observation: the local database is unbound (no durable binding exists), so
// the normal EpochProvider (which requires a matching durable binding)
// cannot establish an outbound context. The status-observation operation is
// restricted to the GET /internal/status endpoint, uses the explicitly pinned
// authoritative target for strict response validation, preserves the normal
// diagnostic accessibility of the status endpoint, requires authenticated
// expected peer identity, preserves request timeout and response-size
// limits, and never authorizes ordinary message replication, bootstrap
// inventory, or message retrieval.
package peertls

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"gossipgrid/internal/lifecycle"
)

// BootstrapSourceObservationTTL is the bounded lifetime of a bootstrap-source
// observation result. After this duration elapses after the observation, the
// result is no longer current and must not authorize joining. The value is
// intentionally short: a bootstrap source that was READY or DEGRADED a few
// seconds ago may no longer be; the joining operation must re-observe.
// This matches lifecycle.MaxSourceObservationTTL.
const BootstrapSourceObservationTTL = lifecycle.MaxSourceObservationTTL

// BootstrapSourceObserver is the production implementation of the
// lifecycle.BootstrapSourceObserver interface. It wraps a peertls Client and
// performs an authenticated, same-epoch observation of a single candidate
// peer.
//
// The observer does NOT cache results: each Observe call performs a fresh
// mTLS handshake and a fresh HTTP request. The result is valid only for
// BootstrapSourceObservationTTL after the observation.
//
// The observer is not safe for concurrent use by multiple goroutines for the
// same candidate (the underlying Client's per-connection transport is
// discarded after each request); however, concurrent observations of
// different candidates are safe (each call builds its own transport).
type BootstrapSourceObserver struct {
	client *Client
	now    func() time.Time
}

// NewBootstrapSourceObserver creates a BootstrapSourceObserver that uses the
// given peer client. The client must be configured with mTLS (a trust bundle
// and an identity suffix) for the observation to be authenticated.
//
// The observer does NOT require the client's EpochProvider to be configured:
// it uses a purpose-specific transport path (doStatusObservationRequest)
// that pins the explicitly supplied target context for strict response
// validation. This is the production-compatible pre-binding observation:
// the local database is unbound, so the normal EpochProvider cannot
// establish an outbound context.
func NewBootstrapSourceObserver(client *Client) *BootstrapSourceObserver {
	return &BootstrapSourceObserver{client: client, now: time.Now}
}

// WithClock returns a copy of the observer with an injectable clock (for
// tests).
func (o *BootstrapSourceObserver) WithClock(now func() time.Time) *BootstrapSourceObserver {
	return &BootstrapSourceObserver{client: o.client, now: now}
}

// Observe performs a single authenticated observation of the given candidate
// and returns a lifecycle.SourceObservationResult. It implements the
// lifecycle.BootstrapSourceObserver interface.
//
// The observation verifies:
//   - the mTLS handshake succeeds and the server presents the expected
//     certificate DNS identity (expectedIdentity);
//   - the response carries exactly one X-GossipGrid-Lifecycle-Active header
//     with value "true";
//   - the response carries complete, nonempty, bounded cluster and epoch
//     headers (both X-GossipGrid-Resp-Cluster-ID and
//     X-GossipGrid-Resp-Epoch-ID present, nonempty, within the byte bound);
//   - the response headers agree UNCONDITIONALLY with the explicitly pinned
//     expected target (not merely with the request context, which may be
//     empty);
//   - the status body's cluster/epoch fields agree with both the response
//     headers and the expected target;
//   - the reported lifecycle state is READY or DEGRADED;
//   - BootstrapSourceEligible is true;
//   - readiness is true (consistent with an eligible READY/DEGRADED source);
//   - the peer is not the local replica (the authenticated peer's short
//     logical PodID, derived from the certificate DNS identity, is compared
//     against localNodeID — not merely the peer's self-reported PodID);
//   - the status body's PodID equals the authenticated peer's short logical
//     identity (derived from the certificate DNS SAN using the existing
//     identity contract; the peer's self-reported PodID is not trusted in
//     isolation).
//
// On success it returns a SourceObservationResult that is valid for
// BootstrapSourceObservationTTL after the observation. On any failure it
// returns an error and a zero-value SourceObservationResult (Observed=false).
func (o *BootstrapSourceObserver) Observe(ctx context.Context, dialAddr, expectedIdentity string, expected lifecycle.RequestContext, localNodeID string) (lifecycle.SourceObservationResult, error) {
	if err := ctx.Err(); err != nil {
		return lifecycle.SourceObservationResult{}, err
	}
	if o.client == nil {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observer: no peer client configured")
	}
	if expected.ClusterID == "" || expected.EpochID == "" {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observer: expected target is incomplete (cluster=%q, epoch=%q)", expected.ClusterID, expected.EpochID)
	}

	// Derive the authenticated peer's short logical PodID from the
	// expected certificate DNS identity using the existing identity
	// contract (validateExpectedIdentity returns the pod part, e.g.
	// "gossipgrid-1" from "gossipgrid-1.<suffix>"). This is the
	// authenticated identity, not the peer's self-reported PodID.
	podID, err := o.client.mgr.validateExpectedIdentity(expectedIdentity)
	if err != nil {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observer: invalid expected peer identity %q: %w", expectedIdentity, err)
	}

	// Perform the mTLS-validated status observation request using the
	// purpose-specific transport path (no EpochProvider required). The
	// mTLS handshake verifies the server's certificate DNS identity
	// against expectedIdentity; a mismatch causes the handshake to fail.
	respBody, status, respHeaders, err := o.client.doStatusObservationRequest(ctx, dialAddr, expectedIdentity, expected)
	if err != nil {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: mTLS request to %s (expected identity %s) failed: %w", dialAddr, expectedIdentity, err)
	}
	if status != http.StatusOK {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: peer %s returned HTTP %d (expected 200)", expectedIdentity, status)
	}

	// Decode the status body.
	var out peerStatusResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: decoding peer status body: %w", err)
	}

	// STRICT response envelope validation: both the cluster and epoch
	// response headers must be present, exactly one each, nonempty, within
	// the byte bound, and UNCONDITIONALLY agree with the explicitly pinned
	// expected target (not merely with the request context, which may be
	// empty in the pre-binding observation path).
	if err := validateStrictSourceEnvelope(respHeaders, expectedIdentity, expected); err != nil {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: %w", err)
	}

	// The status body's cluster/epoch must be nonempty and agree with the
	// response headers AND the expected target (triple agreement: headers
	// == body == expected target).
	if out.ClusterID == "" || out.EpochID == "" {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: peer %s status body reports incomplete cluster/epoch (cluster=%q, epoch=%q)", expectedIdentity, out.ClusterID, out.EpochID)
	}
	if out.ClusterID != expected.ClusterID || out.EpochID != expected.EpochID {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: peer %s reports cluster/epoch %s/%s but the expected target is %s/%s",
			expectedIdentity, out.ClusterID, out.EpochID, expected.ClusterID, expected.EpochID)
	}
	rcVals := respHeaders.Values(lifecycle.HeaderRespClusterID)
	reVals := respHeaders.Values(lifecycle.HeaderRespEpochID)
	if rcVals[0] != out.ClusterID || reVals[0] != out.EpochID {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: peer %s response headers (cluster=%s epoch=%s) disagree with status body (cluster=%s epoch=%s)",
			expectedIdentity, rcVals[0], reVals[0], out.ClusterID, out.EpochID)
	}

	// The lifecycle state must be READY or DEGRADED.
	if out.LifecycleState != "READY" && out.LifecycleState != "DEGRADED" {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: peer %s reports lifecycle state %q (must be READY or DEGRADED to be an eligible bootstrap source)", expectedIdentity, out.LifecycleState)
	}

	// BootstrapSourceEligible must be true.
	if !out.BootstrapSourceEligible {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: peer %s does not report bootstrap source eligibility (BootstrapSourceEligible=false)", expectedIdentity)
	}

	// Readiness must be true (consistent with an eligible READY/DEGRADED source).
	if !out.Readiness {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: peer %s reported readiness=false (inconsistent with an eligible READY/DEGRADED source)", expectedIdentity)
	}

	// The peer must not be the local replica: compare the AUTHENTICATED
	// peer's short logical PodID (derived from the certificate DNS
	// identity) against localNodeID. Do NOT rely solely on the peer's
	// self-reported PodID.
	if localNodeID != "" && podID == localNodeID {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: the observed peer %q (authenticated identity) is the local replica; self-observation is not a bootstrap source", podID)
	}

	// The status body's PodID must equal the authenticated peer's short
	// logical identity (the pod part derived from the certificate DNS SAN).
	// A nonempty reported PodID does not establish agreement with the
	// authenticated certificate identity; the two must match explicitly.
	if out.PodID == "" {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: peer %s did not report a PodID (cannot verify agreement with authenticated identity %s)", expectedIdentity, podID)
	}
	if out.PodID != podID {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: peer %s status body reports PodID %q but the authenticated certificate identity is %q (mismatch); the peer's self-reported identity is not trusted in isolation",
			expectedIdentity, out.PodID, podID)
	}

	// The status body's NodeID, if present, must be consistent with the
	// authenticated peer identity (the existing supported NodeID
	// conventions: the NodeID is the same as the PodID for a
	// StatefulSet deployment).
	if out.NodeID != "" && out.NodeID != podID {
		return lifecycle.SourceObservationResult{}, fmt.Errorf("source observation: peer %s status body reports NodeID %q inconsistent with authenticated identity %q", expectedIdentity, out.NodeID, podID)
	}

	// Build the result with the bounded observation lifetime.
	now := o.now()
	return lifecycle.NewSourceObservationResult(now, BootstrapSourceObservationTTL, expected, expectedIdentity, out.PodID, out.LifecycleState, out.ClusterID, out.EpochID, out.BootstrapSourceEligible, out.Readiness), nil
}

// doStatusObservationRequest performs the purpose-specific, authenticated
// status-observation HTTP request. It is a narrow transport path that:
//
//   - uses the mTLS-validated peer client (TLS 1.3, full chain verification,
//     exact expectedIdentity DNS SAN match, serverAuth EKU);
//   - pins the explicitly supplied expected context for the request headers
//     (X-GossipGrid-Cluster-ID and X-GossipGrid-Epoch-ID) and for the
//     response validation; it does NOT consult the local EpochProvider
//     (which requires a matching durable binding that does not exist in the
//     pre-binding observation);
//   - preserves the request timeout and response-size limits;
//   - performs NO in-flight or post-response revalidation against the local
//     EpochProvider (the local authorization state is irrelevant for a
//     diagnostic status observation; the strict response envelope
//     validation against the pinned target is the sole authorization gate);
//   - never authorizes ordinary message replication, bootstrap inventory,
//     or message retrieval (the path is restricted to GET /internal/status).
//
// This is NOT a general-purpose authorization bypass: it is a
// purpose-specific transport path for the status-observation operation only.
// The normal replication and Stage 2 bootstrap paths retain their existing
// authorization checks (including response-consumption revalidation and the
// partial-404 retry boundary) through doRequest.
func (c *Client) doStatusObservationRequest(ctx context.Context, dialAddr, expectedIdentity string, pinnedCtx lifecycle.RequestContext) ([]byte, int, http.Header, error) {
	// Build the mTLS-validated TLS config (exact identity match).
	tlsCfg, err := c.mgr.TLSConfigForClient(dialAddr, expectedIdentity)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("building peer TLS client config: %w", err)
	}

	// Build the request.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+dialAddr+"/internal/status", nil)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	// Set the request epoch headers from the PINNED context (not the local
	// EpochProvider). The server uses these for the epoch-fencing response
	// headers; the client validates the response against the same pinned
	// context.
	req.Header.Set(lifecycle.HeaderClusterID, pinnedCtx.ClusterID)
	req.Header.Set(lifecycle.HeaderEpochID, pinnedCtx.EpochID)

	// Use the per-request timeout (if configured) for the request context.
	reqCtx, cancel := c.withRequestTimeout(ctx)
	defer cancel()
	req = req.WithContext(reqCtx)

	client := buildClient(tlsCfg)

	resp, err := client.Do(req)
	if err != nil {
		client.Transport.(*http.Transport).CloseIdleConnections()
		return nil, 0, nil, fmt.Errorf("requesting peer %s at %s: %w", expectedIdentity, dialAddr, err)
	}
	defer func() {
		_ = resp.Body.Close()
		client.Transport.(*http.Transport).CloseIdleConnections()
	}()

	// Read the response body bounded.
	lr := io.LimitReader(resp.Body, maxResponseBodyBytes+1)
	respBody, err := io.ReadAll(lr)
	if err != nil {
		return nil, resp.StatusCode, nil, fmt.Errorf("reading peer response body: %w", err)
	}
	if int64(len(respBody)) > maxResponseBodyBytes {
		return nil, resp.StatusCode, nil, fmt.Errorf("peer response exceeds maximum of %d bytes", maxResponseBodyBytes)
	}

	return respBody, resp.StatusCode, resp.Header.Clone(), nil
}

// validateStrictSourceEnvelope is the strict response envelope validation for
// bootstrap-source observations. Unlike validateDiagnosticEnvelope (which
// tolerates absent epoch headers for unready peers), this validator requires
// BOTH the cluster and epoch response headers to be present, exactly one
// each, nonempty, within the byte bound, and UNCONDITIONALLY consistent with
// the explicitly pinned expected target.
//
// The expected target is the pinned authoritative cluster/epoch (from the
// controller's current accepted metadata). An empty expected target is
// rejected (the caller must always supply a pinned target; the pre-binding
// observation path always pins the target from the metadata). This prevents
// an empty request context from being treated as permission to skip the
// strict expected-target checks.
//
// It also requires exactly one X-GossipGrid-Lifecycle-Active header with
// value "true" (the peer must be lifecycle-capable).
func validateStrictSourceEnvelope(respHeaders http.Header, identity string, expected lifecycle.RequestContext) error {
	// The expected target must be complete (nonempty cluster and epoch).
	// This is the pinned authoritative target; an empty target is a
	// programming error (the caller must always pin a target).
	if expected.ClusterID == "" || expected.EpochID == "" {
		return fmt.Errorf("peer %s observation: the pinned expected target is incomplete (cluster=%q, epoch=%q); a complete target is required", identity, expected.ClusterID, expected.EpochID)
	}
	// Lifecycle-Active: exactly one, value "true".
	activeVals := respHeaders.Values(lifecycle.HeaderLifecycleActive)
	if len(activeVals) != 1 {
		return fmt.Errorf("peer %s status response carries %d Lifecycle-Active headers (expected exactly 1)", identity, len(activeVals))
	}
	if activeVals[0] != "true" {
		return fmt.Errorf("peer %s does not enforce the lifecycle/epoch protocol (Lifecycle-Active=%q); not an eligible bootstrap source", identity, activeVals[0])
	}
	// ClusterID: exactly one, nonempty, within bound, matching the expected
	// target UNCONDITIONALLY.
	rcVals := respHeaders.Values(lifecycle.HeaderRespClusterID)
	if len(rcVals) == 0 {
		return fmt.Errorf("peer %s status response is missing the %s header (a complete envelope is required for a bootstrap source)", identity, lifecycle.HeaderRespClusterID)
	}
	if len(rcVals) > 1 {
		return fmt.Errorf("peer %s status response carries %d duplicate %s headers", identity, len(rcVals), lifecycle.HeaderRespClusterID)
	}
	if rcVals[0] == "" {
		return fmt.Errorf("peer %s status response carries an empty %s header", identity, lifecycle.HeaderRespClusterID)
	}
	if len(rcVals[0]) > lifecycle.MaxEpochHeaderBytes {
		return fmt.Errorf("peer %s status response %s header exceeds %d bytes", identity, lifecycle.HeaderRespClusterID, lifecycle.MaxEpochHeaderBytes)
	}
	if rcVals[0] != expected.ClusterID {
		return fmt.Errorf("peer %s reports cluster %q in response headers but the pinned expected target is %q", identity, rcVals[0], expected.ClusterID)
	}
	// EpochID: exactly one, nonempty, within bound, matching the expected
	// target UNCONDITIONALLY.
	reVals := respHeaders.Values(lifecycle.HeaderRespEpochID)
	if len(reVals) == 0 {
		return fmt.Errorf("peer %s status response is missing the %s header (a complete envelope is required for a bootstrap source)", identity, lifecycle.HeaderRespEpochID)
	}
	if len(reVals) > 1 {
		return fmt.Errorf("peer %s status response carries %d duplicate %s headers", identity, len(reVals), lifecycle.HeaderRespEpochID)
	}
	if reVals[0] == "" {
		return fmt.Errorf("peer %s status response carries an empty %s header", identity, lifecycle.HeaderRespEpochID)
	}
	if len(reVals[0]) > lifecycle.MaxEpochHeaderBytes {
		return fmt.Errorf("peer %s status response %s header exceeds %d bytes", identity, lifecycle.HeaderRespEpochID, lifecycle.MaxEpochHeaderBytes)
	}
	if reVals[0] != expected.EpochID {
		return fmt.Errorf("peer %s reports epoch %q in response headers but the pinned expected target is %q", identity, reVals[0], expected.EpochID)
	}
	return nil
}
