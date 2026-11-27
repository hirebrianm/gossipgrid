// Package peertls: authenticated peer HTTP client.
//
// The Client provides a reusable, narrowly-scoped HTTP client for the private
// peer endpoints. A caller supplies the dial address (network address to
// connect to) and the expected GossipGrid peer identity (the certificate
// DNS SAN the server must present). These are distinct inputs: the client may
// dial 127.0.0.1:8443 while verifying a certificate for an in-cluster FQDN.
//
// The client:
//   - uses TLS 1.3 only, verifying the full server chain against the workload
//     CA trust bundle;
//   - presents the local authorized workload certificate;
//   - performs additional peer identity authorization (exact expectedIdentity
//     DNS SAN match, serverAuth EKU);
//   - bounds connection, handshake, and request timeouts;
//   - supports context cancellation;
//   - applies reloaded credentials/trust to new handshakes;
//   - rejects all HTTP redirects (no plaintext fallback, no unauthenticated
//     endpoint substitution);
//   - uses bounded response decoding.
//
// For Milestone 4A, SendMessage posts an existing message to a peer over
// mTLS. The receiver validates and stores the message idempotently.
package peertls

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"gossipgrid/internal/lifecycle"
)

// maxResponseBodyBytes bounds the response body read from a peer (for
// meaningful error reporting on non-success responses). Message responses
// are small; 8 KiB is generous without risking unbounded memory.
const maxResponseBodyBytes = 8192

// maxSyncResponseBodyBytes bounds the response body for anti-entropy sync
// requests. 256 KiB provides headroom over the worst-case valid batch while
// remaining finite. The worst case is 8 records at the per-record wire-size
// bound (maxSyncMessageWireBytes = 32768 = the absolute worst-case JSON-encoded
// body for a 4096-byte stored body, plus the origin/ID/timestamp overhead),
// i.e. ~256 KiB; see the fetch-batch bounds.
const maxSyncResponseBodyBytes = 256 * 1024

// maxMessageOriginBytes bounds the origin field of a message in a sync
// response. It mirrors the store's origin bound (store.maxMessageOriginBytes,
// 253) and is enforced when validating sync fetch responses so a legacy or
// malformed oversized origin fails explicitly rather than producing a
// response that exceeds maxSyncResponseBodyBytes.
const maxMessageOriginBytes = 253

// maxSyncMessageWireBytes is the absolute worst-case JSON-encoded size of a
// single message record in a sync fetch response. A stored body of at most
// 4096 bytes (store.maxMessageBodyBytes) can serialize to up to 24576 bytes
// when every byte is escaped as a 6-byte \uXXXX sequence (e.g. "<" →
// \u003c). The origin (253 stored bytes) can expand to ~3000 bytes when
// escaped. Adding the fixed per-record overhead (ID, timestamp, field names,
// JSON punctuation) gives ~32768 bytes as the conservative per-record bound.
// A batch of 8 such records is ~256 KiB, within the response limit.
const maxSyncMessageWireBytes = 32768

// PeerHTTPStatusError is returned by Client.SendMessage (and GetStatus) when a
// peer responds with a non-success HTTP status. The exact status code is
// carried as a typed field so callers (e.g. the gossip engine) can classify the
// failure (e.g. a 409 conflict is permanent, a 503 is transient) without
// string-matching the error, which is unreliable (a network error such as
// "dial tcp 127.0.0.1:4090: connection refused" contains "409" but is a
// transient transport failure, not an HTTP 409).
type PeerHTTPStatusError struct {
	identity string
	status   int
	body     string
}

func (e *PeerHTTPStatusError) Error() string {
	if e.body == "" {
		return fmt.Sprintf("peer %s rejected request: status %d", e.identity, e.status)
	}
	return fmt.Sprintf("peer %s rejected request: status %d: %s", e.identity, e.status, e.body)
}

// StatusCode returns the HTTP status code of the peer's non-success response.
func (e *PeerHTTPStatusError) StatusCode() int { return e.status }

// NewPeerHTTPStatusError returns a PeerHTTPStatusError for the given peer
// identity, HTTP status code, and response body. It is exported so callers and
// tests can construct the typed error without relying on a real HTTP exchange.
func NewPeerHTTPStatusError(identity string, status int, body string) *PeerHTTPStatusError {
	return &PeerHTTPStatusError{identity: identity, status: status, body: body}
}

// NewPeerAckError returns a PeerAckError for the given peer identity and
// reason. It is exported so callers and tests can construct the typed error
// without relying on a real HTTP exchange.
func NewPeerAckError(identity, reason string) *PeerAckError {
	return &PeerAckError{identity: identity, reason: reason}
}

// PeerBlockedError is returned by Client methods when the LOCAL EpochProvider
// refuses to authorize the outbound request (the current epoch cannot be
// established safely: a lockout, no fresh authoritative observation, no
// matching durable binding, or the controller is stopped). It is distinct from
// a peer rejection: no request was sent to the remote peer. It is a permanent,
// non-retried condition (retried it would not succeed until the local
// authorization recovers) and is classified as permanent by callers (the gossip
// and anti-entropy engines).
type PeerBlockedError struct {
	identity string
	reason   string
}

func (e *PeerBlockedError) Error() string {
	return fmt.Sprintf("outbound request to %s blocked locally: %s", e.identity, e.reason)
}

// NewPeerBlockedError returns a PeerBlockedError for the given peer identity
// and reason. It is exported so callers and tests can construct the typed
// error without a real HTTP exchange.
func NewPeerBlockedError(identity, reason string) *PeerBlockedError {
	return &PeerBlockedError{identity: identity, reason: reason}
}

// PeerAckError is returned by Client.SendMessage when a successful response
// (200/201) carries an invalid message acknowledgment: a missing or null
// message, or a mismatched/malformed ID, body, origin, or creation timestamp.
// These are protocol defects (not transient network failures) and are
// classified as permanent by callers.
type PeerAckError struct {
	identity string
	reason   string
}

func (e *PeerAckError) Error() string {
	return fmt.Sprintf("peer %s returned a successful response with an invalid acknowledgment: %s", e.identity, e.reason)
}

// EpochProvider supplies the local, currently authorized cluster and epoch
// context for outbound replication requests. It is the narrow interface the
// client uses to source its outbound epoch headers from the LIFECYCLE
// CONTROLLER'S CURRENT AUTHORIZATION DECISION, not a one-time startup copy.
//
//   - Context returns the currently authorized cluster/epoch context, or an
//     error when the current epoch cannot be established safely (no fresh
//     authoritative observation, no matching durable binding, a lockout, or
//     the controller is stopped). When an error is returned, the client must
//     NOT send the request (outbound replication is blocked).
//   - Enforcement reports whether lifecycle protection is enabled locally.
//     When false, the client sends no epoch headers and performs no
//     response-envelope validation (legacy behavior preserved).
//
// In production, main.go wires this to a closure that reads the lifecycle
// controller's Authorization() snapshot: the context is the snapshot's
// authorized metadata when the snapshot is authorized (and not stopped), and
// an error otherwise. This keeps the outbound context in sync with metadata
// changes: a metadata change between two requests is reflected in the next
// request (or the request is blocked), rather than using a stale startup copy.
type EpochProvider interface {
	// Context returns the currently authorized cluster/epoch context, or an
	// error when the current epoch cannot be established safely.
	Context() (lifecycle.RequestContext, error)
	// Enforcement reports whether local lifecycle protection is enabled.
	Enforcement() bool
}

// BootstrapEpochProvider is the narrow interface the peer client uses to
// enforce the DEDICATED bootstrap authorization decision at the transport
// boundary (Milestone 5B, Stage 2, Issue 3). It is separate from EpochProvider
// because a general valid epoch context (authorized: READY/DEGRADED state) is
// NOT equivalent to authorization to perform a bootstrap fetch
// (bootstrapAuthorized: SYNCING-only, epoch-bound, narrowly scoped). A
// replica that leaves SYNCING (e.g., transitions to RECOVERY_REQUIRED)
// cannot issue subsequent bootstrap requests even if its cluster ID, epoch
// ID, metadata freshness, and local binding remain unchanged.
//
// BootstrapDecision returns the current bootstrap authorization decision:
// the pinned cluster/epoch context (from a consistent authorized snapshot),
// an authorized flag (true when the replica is authorized to fetch bootstrap
// data), and an error (when the decision cannot be established). When the
// controller is disabled (lifecycle protection off), it returns a zero context
// and authorized=true (passthrough). When not authorized (revocation, state
// change to non-SYNCING, stop, stale/invalid observation, binding change),
// it returns authorized=false with the zero context.
type BootstrapEpochProvider interface {
	// BootstrapDecision returns the current bootstrap authorization decision.
	BootstrapDecision() (lifecycle.RequestContext, bool, error)
}

// Client is an authenticated peer HTTP client for the private peer endpoints.
// It wraps the Manager's credential snapshot for the mTLS transport. When an
// EpochProvider is configured (lifecycle protection enabled), outbound
// replication requests carry the locally authorized epoch headers and
// successful responses are validated for the response epoch envelope (mixed-mode
// fencing). When no provider is configured (lifecycle disabled), legacy
// behavior is preserved: no epoch headers, no response validation.
type Client struct {
	mgr *Manager

	// mu protects provider, bootstrapProvider, and requestTimeout (set by
	// SetEpochProvider / SetBootstrapEpochProvider / SetRequestTimeout, read
	// by doRequest, fetchMessages, and the response validators).
	mu                sync.RWMutex
	provider          EpochProvider
	bootstrapProvider BootstrapEpochProvider
	requestTimeout    time.Duration
}

// NewClient creates a Client bound to the given Manager. The Manager must have
// been Started (credentials loaded) before making requests.
func NewClient(mgr *Manager) *Client {
	return &Client{mgr: mgr}
}

// SetEpochProvider configures the EpochProvider that supplies the local,
// currently authorized epoch context for outbound replication requests. When
// enforcement is true, requests carry the epoch headers and successful
// responses are validated for the response epoch envelope. When nil, legacy
// behavior is preserved (no epoch headers, no response validation).
func (c *Client) SetEpochProvider(p EpochProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.provider = p
}

// SetBootstrapEpochProvider configures the BootstrapEpochProvider that
// supplies the dedicated bootstrap authorization decision for outbound
// bootstrap fetches (Milestone 5B, Stage 2, Issue 3). When set, the
// doRequest method uses this provider (not the general EpochProvider)
// for the pre-transmission and in-flight checks on bootstrap fetches
// (pinnedCtx != nil). When nil, the general EpochProvider is used
// (legacy behavior; the general provider does not enforce the full
// bootstrapAuthorized predicate).
func (c *Client) SetBootstrapEpochProvider(p BootstrapEpochProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.bootstrapProvider = p
}

// getProvider returns the configured EpochProvider, or nil when none is set.
func (c *Client) getProvider() EpochProvider {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.provider
}

// getBootstrapProvider returns the configured BootstrapEpochProvider, or nil
// when none is set.
func (c *Client) getBootstrapProvider() BootstrapEpochProvider {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.bootstrapProvider
}

// SetRequestTimeout sets the per-request timeout applied to individual bootstrap
// HTTP operations (initial fetch, partial-404 retry). Each operation gets its own
// timeout derived from the caller's context; the overall caller deadline still
// bounds the total. When d <= 0, no per-request timeout is applied (the caller's
// context deadline is the only bound).
func (c *Client) SetRequestTimeout(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requestTimeout = d
}

// withRequestTimeout returns a context derived from ctx with the configured
// per-request timeout applied. When requestTimeout <= 0, ctx is returned
// unchanged. The caller must call the returned cancel function.
func (c *Client) withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	c.mu.RLock()
	d := c.requestTimeout
	c.mu.RUnlock()
	if d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

// peerStatusResponse is the JSON response from GET /internal/status. It
// carries the existing fields plus the lifecycle fields (Milestone 5A).
// The lifecycle fields are empty/zero when the remote peer has lifecycle
// protection disabled.
type peerStatusResponse struct {
	NodeID          string `json:"node_id"`
	Version         string `json:"version"`
	PodID           string `json:"pod_id,omitempty"`
	CertFingerprint string `json:"cert_fingerprint,omitempty"`

	// Lifecycle fields (Milestone 5A).
	ClusterID               string `json:"cluster_id,omitempty"`
	EpochID                 string `json:"epoch_id,omitempty"`
	Generation              int    `json:"generation,omitempty"`
	LifecycleState          string `json:"lifecycle_state,omitempty"`
	Readiness               bool   `json:"readiness,omitempty"`
	BootstrapSourceEligible bool   `json:"bootstrap_source_eligible,omitempty"`
	DiagnosticReason        string `json:"diagnostic_reason,omitempty"`
}

// PeerStatusInfo is the exported form of the peer status response. It is
// returned by Client.GetStatus and carries both the existing fields and the
// lifecycle fields (when present).
type PeerStatusInfo struct {
	NodeID          string
	Version         string
	PodID           string
	CertFingerprint string

	// Lifecycle fields (Milestone 5A). Empty/zero when the remote peer has
	// lifecycle protection disabled.
	ClusterID               string
	EpochID                 string
	Generation              int
	LifecycleState          string
	Readiness               bool
	BootstrapSourceEligible bool
	DiagnosticReason        string
}

// validateResponseEnvelope checks the response epoch envelope carried in the
// response headers against the local EpochProvider and the captured request
// epoch context when local lifecycle enforcement is enabled. It returns an
// error (to be wrapped in a PeerAckError by the caller) when:
//
//   - the remote peer is legacy (lifecycle disabled — it did not set the
//     X-GossipGrid-Lifecycle-Active header): a lifecycle-enabled client must
//     reject it as a synchronization source, because the peer cannot
//     demonstrate compatible epoch-fencing protocol behavior. A valid mTLS
//     certificate is necessary but not sufficient for epoch authorization.
//   - the response does not carry exactly one nonempty ClusterID header.
//   - the response does not carry exactly one nonempty EpochID header.
//   - the response carries duplicate or contradictory epoch headers.
//   - the response reports a cluster/epoch that contradicts the captured
//     request context (a wrong-epoch or conflicting peer).
//
// reqCtx is the immutable authorized context captured when the request was
// sent. The response is validated against this captured context (not a fresh
// re-read of the EpochProvider), so a response from epoch B is never
// accepted as the acknowledgment for a request authorized under epoch A even
// if the local epoch changed while the request was in flight.
//
// When local enforcement is disabled (no provider or provider.Enforcement()
// false), no validation is performed and the response is accepted (legacy
// behavior preserved). The envelope is auditable: the headers are bounded and
// the values are compared against the local authorized metadata, not treated
// as bearer credentials.
func validateResponseEnvelope(respHeaders http.Header, identity string, p EpochProvider, reqCtx lifecycle.RequestContext) error {
	if p == nil || !p.Enforcement() {
		return nil // local lifecycle disabled: legacy behavior preserved
	}
	// Determine whether the remote peer is lifecycle-capable.
	activeVals := respHeaders.Values(lifecycle.HeaderLifecycleActive)
	if len(activeVals) != 1 {
		return fmt.Errorf("peer %s response carries %d Lifecycle-Active headers (expected exactly 1)", identity, len(activeVals))
	}
	if activeVals[0] != "true" {
		return fmt.Errorf("peer %s does not enforce the lifecycle/epoch protocol (Lifecycle-Active=%q); rejected as a synchronization source", identity, activeVals[0])
	}
	// The response must carry exactly one nonempty ClusterID header.
	rcVals := respHeaders.Values(lifecycle.HeaderRespClusterID)
	if len(rcVals) == 0 {
		return fmt.Errorf("peer %s response is missing the %s header", identity, lifecycle.HeaderRespClusterID)
	}
	if len(rcVals) > 1 {
		return fmt.Errorf("peer %s response carries %d duplicate %s headers", identity, len(rcVals), lifecycle.HeaderRespClusterID)
	}
	if rcVals[0] == "" {
		return fmt.Errorf("peer %s response carries an empty %s header", identity, lifecycle.HeaderRespClusterID)
	}
	if len(rcVals[0]) > lifecycle.MaxEpochHeaderBytes {
		return fmt.Errorf("peer %s response %s header exceeds %d bytes", identity, lifecycle.HeaderRespClusterID, lifecycle.MaxEpochHeaderBytes)
	}
	// The response must carry exactly one nonempty EpochID header.
	reVals := respHeaders.Values(lifecycle.HeaderRespEpochID)
	if len(reVals) == 0 {
		return fmt.Errorf("peer %s response is missing the %s header", identity, lifecycle.HeaderRespEpochID)
	}
	if len(reVals) > 1 {
		return fmt.Errorf("peer %s response carries %d duplicate %s headers", identity, len(reVals), lifecycle.HeaderRespEpochID)
	}
	if reVals[0] == "" {
		return fmt.Errorf("peer %s response carries an empty %s header", identity, lifecycle.HeaderRespEpochID)
	}
	if len(reVals[0]) > lifecycle.MaxEpochHeaderBytes {
		return fmt.Errorf("peer %s response %s header exceeds %d bytes", identity, lifecycle.HeaderRespEpochID, lifecycle.MaxEpochHeaderBytes)
	}
	// Compare the response's reported epoch against the captured request
	// context. A mismatch indicates the response is from a different cluster
	// or epoch than the one that authorized the request.
	if reqCtx.ClusterID != "" && rcVals[0] != reqCtx.ClusterID {
		return fmt.Errorf("peer %s reports cluster ID %q in response but request was authorized under cluster %q", identity, rcVals[0], reqCtx.ClusterID)
	}
	if reqCtx.EpochID != "" && reVals[0] != reqCtx.EpochID {
		return fmt.Errorf("peer %s reports epoch ID %q in response but request was authorized under epoch %q", identity, reVals[0], reqCtx.EpochID)
	}
	return nil
}

// validateDiagnosticEnvelope is the validation used for GET /internal/status
// responses. It is less strict than validateResponseEnvelope because the
// diagnostic status endpoint must remain accessible to diagnose an unready
// replica that has no accepted metadata (and therefore cannot yet report
// epoch information). It requires:
//
//   - exactly one Lifecycle-Active header with value "true" (the peer must
//     be lifecycle-capable);
//   - if ClusterID or EpochID headers are present, they must be nonempty,
//     non-duplicate, and within the byte bound;
//   - if both are present, they must be consistent with the local authorized
//     context (a mismatch is an error); if only one is present, it is
//     tolerated (an unready replica may report a partial envelope).
//
// This preserves diagnostic-status accessibility for uninitialized peers
// while still rejecting contradictory or malformed envelopes.
func validateDiagnosticEnvelope(respHeaders http.Header, identity string, p EpochProvider, reqCtx lifecycle.RequestContext) error {
	if p == nil || !p.Enforcement() {
		return nil
	}
	activeVals := respHeaders.Values(lifecycle.HeaderLifecycleActive)
	if len(activeVals) != 1 {
		return fmt.Errorf("peer %s status response carries %d Lifecycle-Active headers (expected exactly 1)", identity, len(activeVals))
	}
	if activeVals[0] != "true" {
		return fmt.Errorf("peer %s does not enforce the lifecycle/epoch protocol (Lifecycle-Active=%q); rejected", identity, activeVals[0])
	}
	// Check ClusterID: if present, must be nonempty, non-duplicate, within
	// bounds, and consistent with the request context.
	rcVals := respHeaders.Values(lifecycle.HeaderRespClusterID)
	if len(rcVals) > 1 {
		return fmt.Errorf("peer %s status response carries %d duplicate %s headers", identity, len(rcVals), lifecycle.HeaderRespClusterID)
	}
	if len(rcVals) == 1 {
		if rcVals[0] == "" {
			return fmt.Errorf("peer %s status response carries an empty %s header", identity, lifecycle.HeaderRespClusterID)
		}
		if len(rcVals[0]) > lifecycle.MaxEpochHeaderBytes {
			return fmt.Errorf("peer %s status response %s header exceeds %d bytes", identity, lifecycle.HeaderRespClusterID, lifecycle.MaxEpochHeaderBytes)
		}
		if reqCtx.ClusterID != "" && rcVals[0] != reqCtx.ClusterID {
			return fmt.Errorf("peer %s status reports cluster %q but request context is %q", identity, rcVals[0], reqCtx.ClusterID)
		}
	}
	// Check EpochID: if present, must be nonempty, non-duplicate, within
	// bounds, and consistent with the request context.
	reVals := respHeaders.Values(lifecycle.HeaderRespEpochID)
	if len(reVals) > 1 {
		return fmt.Errorf("peer %s status response carries %d duplicate %s headers", identity, len(reVals), lifecycle.HeaderRespEpochID)
	}
	if len(reVals) == 1 {
		if reVals[0] == "" {
			return fmt.Errorf("peer %s status response carries an empty %s header", identity, lifecycle.HeaderRespEpochID)
		}
		if len(reVals[0]) > lifecycle.MaxEpochHeaderBytes {
			return fmt.Errorf("peer %s status response %s header exceeds %d bytes", identity, lifecycle.HeaderRespEpochID, lifecycle.MaxEpochHeaderBytes)
		}
		if reqCtx.EpochID != "" && reVals[0] != reqCtx.EpochID {
			return fmt.Errorf("peer %s status reports epoch %q but request context is %q", identity, reVals[0], reqCtx.EpochID)
		}
	}
	return nil
}

// sendResponse is the JSON response from POST /internal/messages on a
// successful (200 or 201) insert. It echoes the stored message for the caller
// to confirm what was persisted. The peer field is the authenticated
// receiver's pod identity — not the sender's identity, which the caller
// already knows. The peer field is NOT treated as authentication evidence:
// peer identity is established solely by the mTLS handshake.
type sendResponse struct {
	Message *MessageWire `json:"message,omitempty"`
	Peer    string       `json:"peer,omitempty"`
}

// MessageWire is the wire form of a message. It mirrors the store.Message
// JSON contract exactly (id, body, origin, created_at). The receiver preserves
// all fields as received; no new ID or timestamp is generated during
// replication.
type MessageWire struct {
	ID        string `json:"id"`
	Body      string `json:"body"`
	Origin    string `json:"origin"`
	CreatedAt string `json:"created_at"`
}

// sendRequest is the JSON envelope for POST /internal/messages.
type sendRequest struct {
	Message MessageWire `json:"message"`
}

// buildClient constructs a per-connection http.Client with the given TLS
// config, bounded timeouts, and redirect rejection. It is created per call
// (not shared across calls) so the mTLS config — which embeds the Manager's
// current credential snapshot — is captured at request time, allowing
// credential rotation to be picked up on new connections.
func buildClient(tlsCfg *tls.Config) *http.Client {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:     tlsCfg,
			DialContext:         dialer.DialContext,
			MaxIdleConns:        4,
			IdleConnTimeout:     30 * time.Second,
			TLSHandshakeTimeout: 5 * time.Second,
		},
		// Overall request timeout bounds the whole request including body
		// read. Context cancellation is honoured through
		// NewRequestWithContext.
		Timeout: 10 * time.Second,
		// Peer communication must never fall back to plaintext or accept
		// responses from an unauthenticated endpoint. An authenticated mTLS
		// peer must not redirect the client to any other address (HTTP or
		// HTTPS), so all redirects are rejected.
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return errors.New("peer redirect is not permitted for authenticated peer requests")
		},
	}
}

// doRequest performs an authenticated request against a private peer endpoint.
// It is the shared plumbing for GetStatus, SendMessage, FetchInventoryPage,
// and FetchMessages.
//
// method is the HTTP method (GET or POST). body is the request body (nil for
// GET). The caller supplies the exact expected peer identity; the TLS layer
// verifies the server presents a certificate whose single DNS SAN exactly
// matches expectedIdentity. responseLimit bounds the response body read.
//
// Milestone 5A mixed-mode fencing: when an EpochProvider with enforcement
// enabled is configured, the request carries the locally authorized epoch
// headers and the caller is expected to validate the response envelope (via
// validateResponseEnvelope). When the provider's Context() returns an error
// (the current epoch cannot be established safely — a lockout, no fresh
// observation, or no matching binding), the request is NOT sent: a block is
// returned as a typed error so the caller does not proceed. When no provider
// is configured (lifecycle disabled), no epoch headers are sent and no
// response validation is performed (legacy behavior preserved).
// doRequest performs an authenticated request against a private peer endpoint.
// It is the shared plumbing for GetStatus, SendMessage, FetchInventoryPage,
// and FetchMessages.
//
// method is the HTTP method (GET or POST). body is the request body (nil for
// GET). The caller supplies the exact expected peer identity; the TLS layer
// verifies the server presents a certificate whose single DNS SAN exactly
// matches expectedIdentity. responseLimit bounds the response body read.
//
// Milestone 5A mixed-mode fencing: when an EpochProvider with enforcement
// enabled is configured, the request carries the locally authorized epoch
// headers and the caller is expected to validate the response envelope (via
// validateResponseEnvelope). The epoch context is captured ONCE at request
// construction time (before the network I/O) and returned to the caller so
// the response can be validated against the SAME context that authorized the
// request — not a fresh re-read that could reflect a concurrent epoch
// change. After reading the response body, doRequest rechecks the current
// authorization: if the local authorization was revoked or the epoch changed
// while the request was in flight, the response is rejected (the data must
// not be consumed after authority is invalidated). When no provider is
// configured (lifecycle disabled), no epoch headers are sent and no response
// validation is performed (legacy behavior preserved).
//
// The returned reqCtx is the immutable authorized context captured when the
// request was sent. The caller must pass it to validateResponseEnvelope (or
// validateDiagnosticEnvelope for status) so the response is validated against
// the original request epoch, not a potentially-changed current epoch.
//
// requestPurpose is the optional request-purpose header value (empty for
// ordinary requests; lifecycle.PurposeBootstrapBaseline for a bootstrap
// baseline fetch). It is UNTRUSTED metadata — it only signals intent to the
// serving replica and never grants a privilege.
//
// pinnedCtx is an optional pinned epoch context (non-nil for bootstrap
// fetches; nil for ordinary requests). When non-nil, the outbound epoch
// headers use the pinned context, and the provider's current cluster/epoch
// is compared against the pinned context both BEFORE transmission and in
// flight (Milestone 5B, Stage 2, Issue 5 correction: the transport must NOT
// silently substitute a newer epoch from its current provider). A mismatch
// or a revocation blocks the request before network I/O, or rejects the
// response, respectively. When lifecycle enforcement is active, an
// incomplete pinned context (empty ClusterID or EpochID) is rejected: the
// bootstrap fetch is bound to a specific epoch and there is no defensive
// bypass that permits an empty context.
func (c *Client) doRequest(ctx context.Context, method, path, dialAddr, expectedIdentity string, body []byte, responseLimit int64, requestPurpose string, pinnedCtx *lifecycle.RequestContext) ([]byte, int, http.Header, lifecycle.RequestContext, error) {
	tlsCfg, err := c.mgr.TLSConfigForClient(dialAddr, expectedIdentity)
	if err != nil {
		return nil, 0, nil, lifecycle.RequestContext{}, fmt.Errorf("building peer TLS client config: %w", err)
	}

	var req *http.Request
	if body == nil {
		req, err = http.NewRequestWithContext(ctx, method, "https://"+dialAddr+path, nil)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, "https://"+dialAddr+path, bytes.NewReader(body))
	}
	if err != nil {
		return nil, 0, nil, lifecycle.RequestContext{}, fmt.Errorf("building request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	// Milestone 5B, Stage 2: optional request-purpose header (untrusted
	// metadata). Set only when nonempty; it does not gate authorization.
	if requestPurpose != "" {
		req.Header.Set(lifecycle.HeaderRequestPurpose, requestPurpose)
	}

	// Milestone 5A: source the outbound epoch context from the current
	// EpochProvider (the lifecycle controller's authorization decision), not
	// a one-time startup copy. The context is captured ONCE here (before any
	// network I/O) and returned to the caller for response validation. A
	// metadata change between two requests is reflected in the next request.
	// When the provider is configured with enforcement enabled but cannot
	// establish the current epoch safely, the request is blocked (not sent) —
	// a block is a typed, permanent error so the caller does not retry
	// indefinitely.
	//
	// Milestone 5B, Stage 2 (Correction 3): bootstrap operations are
	// identified by the request purpose (PurposeBootstrapBaseline), not by
	// pinnedCtx != nil. For a bootstrap operation with lifecycle enforcement
	// enabled, the DEDICATED bootstrap authorization decision
	// (BootstrapEpochProvider.BootstrapDecision) is required: the pinned
	// context must be non-nil and complete, the bootstrap provider must be
	// configured, the decision must be authorized with no error, and the
	// returned cluster/epoch must match the pinned context. Any failed
	// condition is rejected with PeerBlockedError BEFORE network
	// transmission. There is NO fallback to the general EpochProvider for
	// lifecycle-enabled bootstrap requests. The general provider remains
	// appropriate for ordinary operations. Lifecycle-disabled bootstrap
	// retains legacy passthrough behavior (headers set from pinnedCtx if
	// present, no authorization gate).
	var reqCtx lifecycle.RequestContext
	p := c.getProvider()
	isBootstrap := requestPurpose == lifecycle.PurposeBootstrapBaseline
	if isBootstrap && p != nil && p.Enforcement() {
		// Bootstrap fetch with lifecycle enforcement: fail-closed.
		// Require the dedicated bootstrap provider and a complete pinned
		// context. No fallback to the general EpochProvider.
		if pinnedCtx == nil {
			return nil, 0, nil, lifecycle.RequestContext{}, NewPeerBlockedError(expectedIdentity,
				"bootstrap fetch with lifecycle enforcement requires a pinned context")
		}
		if pinnedCtx.ClusterID == "" || pinnedCtx.EpochID == "" {
			return nil, 0, nil, lifecycle.RequestContext{}, NewPeerBlockedError(expectedIdentity,
				fmt.Sprintf("incomplete pinned context for bootstrap fetch (cluster=%q, epoch=%q); lifecycle enforcement is active",
					pinnedCtx.ClusterID, pinnedCtx.EpochID))
		}
		bp := c.getBootstrapProvider()
		if bp == nil {
			return nil, 0, nil, lifecycle.RequestContext{}, NewPeerBlockedError(expectedIdentity,
				"bootstrap fetch with lifecycle enforcement requires a configured BootstrapEpochProvider")
		}
		// Check the DEDICATED bootstrap authorization decision. The
		// BootstrapDecision method evaluates bootstrapAuthorized
		// (State == SYNCING, not stopped, metadata present, Initialized,
		// genesis finalization, fresh, not stale, binding matches).
		currentCtx, authorized, derr := bp.BootstrapDecision()
		if derr != nil {
			return nil, 0, nil, lifecycle.RequestContext{}, NewPeerBlockedError(expectedIdentity,
				fmt.Sprintf("local bootstrap authorization decision failed before request transmission: %s", derr.Error()))
		}
		if !authorized {
			return nil, 0, nil, lifecycle.RequestContext{}, NewPeerBlockedError(expectedIdentity,
				fmt.Sprintf("local bootstrap authorization was revoked before request transmission (pinned=%s/%s); request is not authorized",
					pinnedCtx.ClusterID, pinnedCtx.EpochID))
		}
		// Compare the bootstrap provider's current cluster/epoch against
		// the pinned context. An epoch change (mismatch) blocks the request.
		if currentCtx.ClusterID != pinnedCtx.ClusterID || currentCtx.EpochID != pinnedCtx.EpochID {
			return nil, 0, nil, lifecycle.RequestContext{}, NewPeerBlockedError(expectedIdentity,
				fmt.Sprintf("local epoch changed before bootstrap request transmission (pinned=%s/%s, current=%s/%s); request is not authorized",
					pinnedCtx.ClusterID, pinnedCtx.EpochID, currentCtx.ClusterID, currentCtx.EpochID))
		}
		reqCtx = *pinnedCtx
		req.Header.Set(lifecycle.HeaderClusterID, reqCtx.ClusterID)
		req.Header.Set(lifecycle.HeaderEpochID, reqCtx.EpochID)
	} else if isBootstrap {
		// Bootstrap fetch with lifecycle disabled: legacy passthrough.
		// Headers are set from the pinned context if present.
		if pinnedCtx != nil {
			reqCtx = *pinnedCtx
			req.Header.Set(lifecycle.HeaderClusterID, reqCtx.ClusterID)
			req.Header.Set(lifecycle.HeaderEpochID, reqCtx.EpochID)
		}
	} else if p != nil && p.Enforcement() {
		// Ordinary request: use the current provider's context.
		localCtx, cerr := p.Context()
		if cerr != nil {
			return nil, 0, nil, lifecycle.RequestContext{}, NewPeerBlockedError(expectedIdentity, cerr.Error())
		}
		reqCtx = lifecycle.RequestContext{ClusterID: localCtx.ClusterID, EpochID: localCtx.EpochID}
		if localCtx.ClusterID != "" {
			req.Header.Set(lifecycle.HeaderClusterID, localCtx.ClusterID)
		}
		if localCtx.EpochID != "" {
			req.Header.Set(lifecycle.HeaderEpochID, localCtx.EpochID)
		}
	}

	client := buildClient(tlsCfg)

	resp, err := client.Do(req)
	if err != nil {
		// Close idle connections on the per-request transport to avoid
		// leaking idle TLS connections. The transport is discarded after
		// this call, so any idle connections would otherwise be abandoned.
		client.Transport.(*http.Transport).CloseIdleConnections()
		return nil, 0, nil, lifecycle.RequestContext{}, fmt.Errorf("requesting peer %s at %s: %w", expectedIdentity, dialAddr, err)
	}
	// The close error is ignored: after the response body is read (or the
	// request fails) there is no useful recovery for a broken client
	// connection.
	defer func() {
		_ = resp.Body.Close()
		// Close idle connections on the per-request transport to avoid
		// leaking idle TLS connections. The transport is discarded after
		// this call, so any idle connections must be closed explicitly.
		client.Transport.(*http.Transport).CloseIdleConnections()
	}()

	// Read the response body bounded to responseLimit. If the body exceeds
	// the limit, detect it explicitly rather than silently accepting
	// truncated JSON.
	lr := io.LimitReader(resp.Body, responseLimit+1)
	respBody, err := io.ReadAll(lr)
	if err != nil {
		return nil, resp.StatusCode, nil, reqCtx, fmt.Errorf("reading peer response body: %w", err)
	}
	if int64(len(respBody)) > responseLimit {
		return nil, resp.StatusCode, nil, reqCtx, fmt.Errorf("peer response exceeds maximum of %d bytes", responseLimit)
	}
	respHeaders := resp.Header.Clone()

	// Milestone 5A (Issue 3): recheck the current authorization BEFORE
	// returning the response. If the local authorization was revoked or the
	// epoch changed while the request was in flight, the response must NOT be
	// consumed (the data is from an epoch that no longer has authority).
	// This prevents accepting a response from epoch B as the acknowledgment
	// for a request authorized under epoch A.
	//
	// Milestone 5B, Stage 2 (Correction 3): for a bootstrap operation
	// (identified by requestPurpose == PurposeBootstrapBaseline), the
	// in-flight recheck applies the DEDICATED bootstrap authorization
	// decision (BootstrapDecision) at response-consumption time, not the
	// general EpochProvider. Every partial-404 retry independently repeats
	// this bootstrap authorization decision.
	if isBootstrap && p != nil && p.Enforcement() {
		bp := c.getBootstrapProvider()
		if bp == nil {
			return nil, resp.StatusCode, respHeaders, reqCtx, NewPeerBlockedError(expectedIdentity,
				"bootstrap fetch with lifecycle enforcement requires a configured BootstrapEpochProvider")
		}
		currentCtx, authorized, derr := bp.BootstrapDecision()
		if derr != nil {
			return nil, resp.StatusCode, respHeaders, reqCtx, NewPeerBlockedError(expectedIdentity,
				"local bootstrap authorization decision failed while the request was in flight: "+derr.Error())
		}
		if !authorized {
			return nil, resp.StatusCode, respHeaders, reqCtx, NewPeerBlockedError(expectedIdentity,
				fmt.Sprintf("local bootstrap authorization was revoked while the request was in flight (request=%s/%s); response is not authorized",
					reqCtx.ClusterID, reqCtx.EpochID))
		}
		if currentCtx.ClusterID != reqCtx.ClusterID || currentCtx.EpochID != reqCtx.EpochID {
			return nil, resp.StatusCode, respHeaders, reqCtx, NewPeerBlockedError(expectedIdentity,
				fmt.Sprintf("local epoch changed while the request was in flight (request=%s/%s, current=%s/%s); response is not authorized",
					reqCtx.ClusterID, reqCtx.EpochID, currentCtx.ClusterID, currentCtx.EpochID))
		}
	} else if p != nil && p.Enforcement() {
		// Ordinary request: recheck the general provider's context.
		currentCtx, cerr := p.Context()
		if cerr != nil {
			return nil, resp.StatusCode, respHeaders, reqCtx, NewPeerBlockedError(expectedIdentity, "local authorization was revoked while the request was in flight: "+cerr.Error())
		}
		if currentCtx.ClusterID != reqCtx.ClusterID || currentCtx.EpochID != reqCtx.EpochID {
			return nil, resp.StatusCode, respHeaders, reqCtx, NewPeerBlockedError(expectedIdentity,
				fmt.Sprintf("local epoch changed while the request was in flight (request=%s/%s, current=%s/%s); response is not authorized",
					reqCtx.ClusterID, reqCtx.EpochID, currentCtx.ClusterID, currentCtx.EpochID))
		}
	}

	// Return the response headers and the captured request context so the
	// caller can validate the response epoch envelope (validateResponseEnvelope
	// or validateDiagnosticEnvelope) against the original request epoch.
	return respBody, resp.StatusCode, respHeaders, reqCtx, nil
}

// GetStatus requests the private status endpoint from a specific expected peer.
//
// dialAddr is the network address to connect to (e.g. "127.0.0.1:8443" for
// local testing, or "gossipgrid-1.gossipgrid-headless.default.svc:8443" in
// cluster). expectedIdentity is the authorized GossipGrid DNS identity the
// server certificate must present (e.g. "gossipgrid-1.<suffix>").
//
// The request is made over mTLS with full verification. A wrong expected
// identity, an untrusted client certificate, or a server whose SAN does not
// match expectedIdentity all cause the handshake to fail and this method to
// return an error.
func (c *Client) GetStatus(ctx context.Context, dialAddr, expectedIdentity string) (*PeerStatusInfo, error) {
	respBody, status, respHeaders, reqCtx, err := c.doRequest(ctx, http.MethodGet, "/internal/status", dialAddr, expectedIdentity, nil, maxResponseBodyBytes, "", nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, &PeerHTTPStatusError{identity: expectedIdentity, status: status, body: string(respBody)}
	}
	var out peerStatusResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, fmt.Errorf("decoding peer status: %w", err)
	}

	// Validate the diagnostic-status response envelope (Milestone 5A). The
	// diagnostic status endpoint must remain accessible to diagnose an
	// unready replica that has no accepted metadata (and therefore cannot
	// yet report a full epoch envelope). The diagnostic validator is less
	// strict than the replication-response validator: it requires the
	// Lifecycle-Active header to be present and "true", but tolerates
	// absent ClusterID/EpochID headers (an uninitialized peer may not be
	// able to report them yet). A contradictory or malformed envelope is
	// still rejected.
	if err := validateDiagnosticEnvelope(respHeaders, expectedIdentity, c.getProvider(), reqCtx); err != nil {
		return nil, &PeerAckError{identity: expectedIdentity, reason: err.Error()}
	}

	return &PeerStatusInfo{
		NodeID:                  out.NodeID,
		Version:                 out.Version,
		PodID:                   out.PodID,
		CertFingerprint:         out.CertFingerprint,
		ClusterID:               out.ClusterID,
		EpochID:                 out.EpochID,
		Generation:              out.Generation,
		LifecycleState:          out.LifecycleState,
		Readiness:               out.Readiness,
		BootstrapSourceEligible: out.BootstrapSourceEligible,
		DiagnosticReason:        out.DiagnosticReason,
	}, nil
}

// SendMessage posts an existing message to a specific expected peer over mTLS.
//
// The receiver validates the message (canonical UUIDv7, nonempty body/origin,
// valid timestamp, bounded size) and inserts it idempotently:
//
//   - new valid message: stored, HTTP 200;
//   - identical duplicate (same ID and content): HTTP 200, no modification;
//   - duplicate ID with conflicting content: HTTP 409, original row preserved.
//
// The caller supplies the dial address, the expected peer certificate DNS
// identity, and the complete message. The authenticated peer identity is
// established by the mTLS handshake (the receiver's certificate), NOT by any
// HTTP header or field in the JSON body. The message's origin field is
// immutable metadata identifying the original creator, which may differ from
// the forwarding peer; mTLS alone does not cryptographically prove the
// historical origin of a forwarded message.
//
// Returns the stored message echoed by the receiver, or a descriptive error
// for a non-success HTTP status (including 409 conflict).
//
// For a successful response (200 or 201), the response must contain a
// non-nil message acknowledgment whose ID, body, origin, and creation
// timestamp all match the submitted message. The creation timestamp is
// compared by instant (parsed as RFC3339), so equivalent representations
// (e.g. with or without fractional seconds) are accepted. A missing,
// malformed, or mismatched acknowledgment is rejected with a descriptive
// error rather than returned as a nil result.
func (c *Client) SendMessage(ctx context.Context, dialAddr, expectedIdentity string, msg MessageWire) (*MessageWire, error) {
	body, err := json.Marshal(sendRequest{Message: msg})
	if err != nil {
		return nil, fmt.Errorf("encoding send request: %w", err)
	}
	respBody, status, respHeaders, reqCtx, err := c.doRequest(ctx, http.MethodPost, "/internal/messages", dialAddr, expectedIdentity, body, maxResponseBodyBytes, "", nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return nil, &PeerHTTPStatusError{identity: expectedIdentity, status: status, body: string(respBody)}
	}
	// Validate the response epoch envelope against the captured request
	// context (Milestone 5A mixed-mode fencing). A legacy peer (no envelope),
	// a missing/duplicate/contradictory epoch header, or an epoch mismatch
	// with the request's authorized context is rejected. The response is
	// validated against the SAME context that authorized the request (not a
	// fresh re-read), so a concurrent epoch change cannot cause a
	// wrong-epoch response to be accepted.
	if err := validateResponseEnvelope(respHeaders, expectedIdentity, c.getProvider(), reqCtx); err != nil {
		return nil, &PeerAckError{identity: expectedIdentity, reason: err.Error()}
	}
	var out sendResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("decoding send response: %v", err)}
	}
	// A successful response must echo back a message acknowledgment. A 200/201
	// without one is a protocol defect: the receiver must confirm what it
	// persisted, and the caller cannot assume the insert happened.
	if out.Message == nil {
		return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("status %d returned without a message acknowledgment", status)}
	}
	ack := out.Message
	if ack.ID != msg.ID {
		return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("acknowledged message ID %q but submitted ID was %q", ack.ID, msg.ID)}
	}
	if ack.Body != msg.Body {
		return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("acknowledged message body %q but submitted body was %q", ack.Body, msg.Body)}
	}
	if ack.Origin != msg.Origin {
		return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("acknowledged message origin %q but submitted origin was %q", ack.Origin, msg.Origin)}
	}
	if err := verifyAcknowledgedTimestamp(ack.CreatedAt, msg.CreatedAt); err != nil {
		return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("acknowledged a mismatched message timestamp: %v", err)}
	}
	return ack, nil
}

// InventoryPageResponse is the JSON response from GET /internal/sync/ids.
// It carries a bounded page of message IDs in strict ascending order, the
// cursor for the next page, and a has_more indicator.
//
// NextCursor semantics:
//   - When HasMore is true: NextCursor equals the last ID in this page and
//     is the exclusive cursor to pass as after for the next request.
//   - When HasMore is false: NextCursor must be empty (the final page carries
//     no further cursor). The caller resets its local cursor to empty to
//     detect a completed full sweep.
type InventoryPageResponse struct {
	IDs        []string `json:"ids"`
	NextCursor string   `json:"next_cursor"`
	HasMore    bool     `json:"has_more"`
}

// SyncMessageWire is the wire form of a message in a sync fetch response.
// It mirrors the store.Message JSON contract.
type SyncMessageWire struct {
	ID        string `json:"id"`
	Body      string `json:"body"`
	Origin    string `json:"origin"`
	CreatedAt string `json:"created_at"`
}

// fetchMessagesRequest is the JSON request for POST /internal/sync/messages.
type fetchMessagesRequest struct {
	IDs []string `json:"ids"`
}

// fetchMessagesResponse is the JSON response for POST /internal/sync/messages.
type fetchMessagesResponse struct {
	Messages []SyncMessageWire `json:"messages"`
}

// missingIDsResponse is the structured 404 response body returned by the peer's
// sync-fetch endpoint when one or more requested IDs are absent or have expired
// on the peer. It carries the exact IDs (in request order) that the peer could
// not serve, in a machine-readable "missing_ids" field, so the client can
// validate that the reported missing IDs are a subset of the original request
// instead of parsing a human-readable error string.
//
// The "error" field is retained for backward compatibility with a legacy peer
// that emits the old free-form 404 error body: a client that cannot find a
// valid "missing_ids" field falls back to the legacy text parse (or, when that
// also fails, rejects the 404 as malformed).
type missingIDsResponse struct {
	Error      string   `json:"error,omitempty"`
	MissingIDs []string `json:"missing_ids"`
}

// FetchInventoryPage requests a bounded page of message IDs from a specific
// expected peer over mTLS. The after parameter is the exclusive cursor
// (empty string for the first page). The limit is the maximum page size.
//
// The response is validated:
//   - The page must not exceed limit entries;
//   - Every ID must be a canonical UUIDv7;
//   - IDs must be in strict ascending order with no duplicates;
//   - Every returned ID must be greater than the requested after cursor;
//   - When HasMore is true: the ID list must be non-empty, and NextCursor
//     must equal the last ID in the page and be greater than after;
//   - When HasMore is false: NextCursor must be empty (the final page
//     carries no further cursor; the caller resets its local cursor to
//     empty so that a full sweep is detected);
//   - An empty ID list is only valid when HasMore is false (the peer has
//     no more records).
//
// All protocol violations return a typed *PeerAckError; no panic occurs
// regardless of response shape.
func (c *Client) FetchInventoryPage(ctx context.Context, dialAddr, expectedIdentity, after string, limit int) (*InventoryPageResponse, error) {
	return c.fetchInventoryPage(ctx, dialAddr, expectedIdentity, after, limit, "", nil)
}

// FetchBootstrapInventoryPage is the bootstrap-fetch variant of
// FetchInventoryPage (Milestone 5B, Stage 2): it requests a bounded inventory
// page from an eligible authoritative source, carrying the bootstrap
// request-purpose header to signal intent to the serving replica. The header
// is UNTRUSTED metadata — it only selects the serving-side admission policy;
// mTLS identity and the response epoch envelope remain the sole basis for
// source authorization. All response validation is identical to
// FetchInventoryPage.
//
// pinnedCtx is the pinned epoch context (Milestone 5B, Stage 2, Issue 5):
// the outbound epoch headers use the pinned cluster/epoch, not the current
// EpochProvider. With lifecycle enforcement enabled, a nil or incomplete
// pinnedCtx is rejected (PeerBlockedError) before network transmission —
// there is no fallback to the general EpochProvider. With lifecycle
// disabled, a nil pinnedCtx is a legacy passthrough (no epoch headers set).
func (c *Client) FetchBootstrapInventoryPage(ctx context.Context, dialAddr, expectedIdentity, after string, limit int, pinnedCtx *lifecycle.RequestContext) (*InventoryPageResponse, error) {
	return c.fetchInventoryPage(ctx, dialAddr, expectedIdentity, after, limit, lifecycle.PurposeBootstrapBaseline, pinnedCtx)
}

// fetchInventoryPage is the shared body of FetchInventoryPage and
// FetchBootstrapInventoryPage. requestPurpose is the optional request-purpose
// header value (empty for ordinary anti-entropy;
// lifecycle.PurposeBootstrapBaseline for a bootstrap baseline fetch).
// pinnedCtx is the pinned epoch context (non-nil for bootstrap; nil for
// ordinary).
func (c *Client) fetchInventoryPage(ctx context.Context, dialAddr, expectedIdentity, after string, limit int, requestPurpose string, pinnedCtx *lifecycle.RequestContext) (*InventoryPageResponse, error) {
	path := fmt.Sprintf("/internal/sync/ids?limit=%d", limit)
	if after != "" {
		path += "&after=" + after
	}
	// Give the inventory retrieval its own timeout budget (M5B Stage 2,
	// Correction 2): the peer client owns the per-HTTP-operation timeout;
	// the caller's parent context is the cancellation/overall-deadline bound.
	invCtx, invCancel := c.withRequestTimeout(ctx)
	defer invCancel()
	respBody, status, respHeaders, reqCtx, err := c.doRequest(invCtx, http.MethodGet, path, dialAddr, expectedIdentity, nil, maxSyncResponseBodyBytes, requestPurpose, pinnedCtx)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, &PeerHTTPStatusError{identity: expectedIdentity, status: status, body: string(respBody)}
	}
	// Validate the response epoch envelope against the captured request
	// context (Milestone 5A mixed-mode fencing). A legacy peer (no envelope),
	// a missing/duplicate/contradictory epoch header, or an epoch mismatch
	// with the request's authorized context is rejected, so inventory from a
	// peer that cannot demonstrate compatible protocol behavior is not
	// accepted as a synchronization source.
	if err := validateResponseEnvelope(respHeaders, expectedIdentity, c.getProvider(), reqCtx); err != nil {
		return nil, &PeerAckError{identity: expectedIdentity, reason: err.Error()}
	}
	var out InventoryPageResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("decoding inventory page response: %v", err)}
	}

	// Enforce the requested page limit.
	if len(out.IDs) > limit {
		return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("inventory page has %d IDs, exceeding limit %d", len(out.IDs), limit)}
	}

	// has_more=true with an empty ID list is a protocol contradiction: the
	// peer claims there are more records but returned none to continue from.
	if out.HasMore && len(out.IDs) == 0 {
		return nil, &PeerAckError{identity: expectedIdentity, reason: "has_more is true but the ID list is empty"}
	}

	// Validate every returned ID as canonical UUIDv7, enforce strict
	// ascending order, no duplicates, and that all IDs advance past the
	// requested cursor.
	for i, id := range out.IDs {
		if err := validateCanonicalUUIDv7(id); err != nil {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("ID at position %d: %v", i, err)}
		}
		if i > 0 && id <= out.IDs[i-1] {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("inventory IDs not in strict ascending order at position %d: %q <= %q", i, id, out.IDs[i-1])}
		}
		if after != "" && id <= after {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("ID %q at position %d does not advance past the supplied after %q", id, i, after)}
		}
	}

	// Validate next_cursor semantics.
	if out.HasMore {
		if out.NextCursor == "" {
			return nil, &PeerAckError{identity: expectedIdentity, reason: "has_more is true but next_cursor is empty"}
		}
		if err := validateCanonicalUUIDv7(out.NextCursor); err != nil {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("next_cursor: %v", err)}
		}
		if out.NextCursor != out.IDs[len(out.IDs)-1] {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("next_cursor %q does not match last ID %q", out.NextCursor, out.IDs[len(out.IDs)-1])}
		}
		if after != "" && out.NextCursor <= after {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("next_cursor %q does not advance past the supplied after %q", out.NextCursor, after)}
		}
	} else {
		// Final page: next_cursor must be empty. The caller resets its
		// local cursor to empty to detect a completed full sweep.
		if out.NextCursor != "" {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("has_more is false but next_cursor is %q (must be empty on the final page)", out.NextCursor)}
		}
	}

	return &out, nil
}

// FetchMessages requests a bounded set of complete message records by ID from
// a specific expected peer over mTLS. The IDs must be validated as canonical
// UUIDv7 before the call (the server enforces this too).
//
// Success (HTTP 200): the response is validated — the number of returned
// messages must exactly match the number of requested IDs (no extra
// unsolicited records, no missing records), messages must be in
// deterministic ID-ascending order, each message ID must match one of the
// requested IDs, and timestamps must be valid RFC3339.
//
// A 404 (HTTP 404) from the peer's sync-fetch endpoint indicates that one or
// more requested IDs are absent or have expired on the peer. This is a
// legitimate, expected outcome when a message expires between the inventory
// enumeration and the batch fetch: the peer's inventory advertised the ID, but
// by the time the fetch arrived the message had been expired (and excluded by
// the peer's retention filter). The 404 response is a structured missing-ID
// response (a "missing_ids" field carrying the exact IDs the peer could not
// serve, in request order). FetchMessages validates it against the original
// request:
//
//   - every reported missing ID must be a canonical UUIDv7 that was in the
//     original request (an unexpected, duplicated, or malformed ID is rejected
//     — the response is malformed, not a legitimate expiration);
//   - when every requested ID is missing, this is SUCCESSFUL reconciliation
//     with zero fetched messages: an empty slice and nil error is returned.
//     The peer reliably confirmed those IDs are absent/expired, and its
//     inventory (which excludes expired records) will not re-advertise them,
//     so the caller can advance its cursor without stalling the batch.
//   - when only some IDs are missing, a single retry is made with the
//     remaining present IDs; the retry must return 200 with exactly those
//     records, which are returned.
//
// A 404 whose body is not a valid missing-ID response (no "missing_ids"
// field, or a legacy text parse that yields no valid request IDs) is rejected
// as malformed rather than interpreted as an expiration. No retry is ever
// made for an unexpected ID, so records expiring across successive attempts
// cannot drive unbounded retries: the anti-entropy cursor is not advanced on
// a rejected 404 and the IDs are not re-advertised, so the same page is not
// re-fetched in a loop.
func (c *Client) FetchMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string) ([]SyncMessageWire, error) {
	return c.fetchMessages(ctx, dialAddr, expectedIdentity, ids, "", nil)
}

// FetchBootstrapMessages is the bootstrap-fetch variant of FetchMessages
// (Milestone 5B, Stage 2): it requests a bounded set of complete message
// records by ID from an eligible authoritative source, carrying the bootstrap
// request-purpose header. The header is UNTRUSTED metadata that only selects
// the serving-side admission policy; mTLS identity and the response epoch
// envelope remain the sole basis for source authorization. All response
// validation (including the 404 missing-ID reconciliation and the per-record
// wire-size bounds) is identical to FetchMessages.
//
// pinnedCtx is the pinned epoch context (Milestone 5B, Stage 2, Issue 5):
// the outbound epoch headers use the pinned cluster/epoch, not the current
// EpochProvider. With lifecycle enforcement enabled, a nil or incomplete
// pinnedCtx is rejected (PeerBlockedError) before network transmission —
// there is no fallback to the general EpochProvider. With lifecycle
// disabled, a nil pinnedCtx is a legacy passthrough (no epoch headers set).
func (c *Client) FetchBootstrapMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string, pinnedCtx *lifecycle.RequestContext) ([]SyncMessageWire, error) {
	return c.fetchMessages(ctx, dialAddr, expectedIdentity, ids, lifecycle.PurposeBootstrapBaseline, pinnedCtx)
}

// fetchMessages is the shared body of FetchMessages and
// FetchBootstrapMessages. requestPurpose is the optional request-purpose
// header value (empty for ordinary anti-entropy;
// lifecycle.PurposeBootstrapBaseline for a bootstrap baseline fetch).
// pinnedCtx is the pinned epoch context (non-nil for bootstrap; nil for
// ordinary).
//
// Request timeout semantics (Milestone 5B, Stage 2, Issue 3 correction): the
// initial message-fetch request and its partial-404 retry each get their own
// configured RequestTimeout budget, subject to the caller's parent context
// (cancellation and the overall parent deadline). The timeout is NOT shared
// between the initial request and the retry: each network operation has its
// own wall-clock budget. The per-request timeout is applied via a derived
// context (context.WithTimeout) from the caller's ctx, so the caller's
// cancellation is honored and the overall parent deadline is preserved.
func (c *Client) fetchMessages(ctx context.Context, dialAddr, expectedIdentity string, ids []string, requestPurpose string, pinnedCtx *lifecycle.RequestContext) ([]SyncMessageWire, error) {
	body, err := json.Marshal(fetchMessagesRequest{IDs: ids})
	if err != nil {
		return nil, fmt.Errorf("encoding fetch request: %w", err)
	}
	// Give the initial request its own timeout budget.
	initialCtx, initialCancel := c.withRequestTimeout(ctx)
	defer initialCancel()
	respBody, status, respHeaders, reqCtx, err := c.doRequest(initialCtx, http.MethodPost, "/internal/sync/messages", dialAddr, expectedIdentity, body, maxSyncResponseBodyBytes, requestPurpose, pinnedCtx)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		// Validate the response epoch envelope BEFORE trusting the 404
		// missing-ID response. A 404 carrying missing_ids can be accepted as
		// successful reconciliation without envelope validation when lifecycle
		// enforcement is active; this would allow a wrong-epoch or legacy
		// peer to confirm message absence (or partial absence) and drive the
		// cursor forward. Apply the same epoch validation guarantees used
		// for HTTP 200: reject missing, contradictory, or wrong-epoch
		// envelopes. When lifecycle enforcement is disabled, the validator
		// returns nil (legacy behavior preserved).
		if err := validateResponseEnvelope(respHeaders, expectedIdentity, c.getProvider(), reqCtx); err != nil {
			return nil, &PeerAckError{identity: expectedIdentity, reason: err.Error()}
		}
		// A 404 is only meaningful if it reliably reports which of the
		// requested IDs are absent/expired. Validate the structured response
		// against the original request; a malformed or contradictory 404 is
		// rejected as a protocol error (not a legitimate expiration).
		missingSet, ok := parseMissingIDsResponse(respBody, ids)
		if !ok {
			return nil, &PeerAckError{identity: expectedIdentity,
				reason: "404 response is not a valid missing-ID response (no missing_ids field, or missing IDs do not match the request); rejected as malformed"}
		}
		var presentIDs []string
		for _, id := range ids {
			if !missingSet[id] {
				presentIDs = append(presentIDs, id)
			}
		}
		if len(presentIDs) == 0 {
			// All requested IDs are confirmed missing/expired. This is
			// successful reconciliation with zero fetched messages: the peer
			// reliably confirmed their absence and its inventory will not
			// re-advertise them, so the caller can advance its cursor.
			return []SyncMessageWire{}, nil
		}
		// Only some IDs are missing: retry once with the present IDs.
		// The retry gets its OWN timeout budget (Issue 3): the initial
		// request's timeout is not shared with the retry.
		retryBody, rerr := json.Marshal(fetchMessagesRequest{IDs: presentIDs})
		if rerr != nil {
			return nil, fmt.Errorf("encoding retry fetch request: %w", rerr)
		}
		retryCtx, retryCancel := c.withRequestTimeout(ctx)
		defer retryCancel()
		respBody, status, respHeaders, reqCtx, err = c.doRequest(retryCtx, http.MethodPost, "/internal/sync/messages", dialAddr, expectedIdentity, retryBody, maxSyncResponseBodyBytes, requestPurpose, pinnedCtx)
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			// The retry failed with a non-200 (genuine protocol error,
			// network failure, or a second 404). Return the error; the caller
			// treats this as a failed page and does not advance the cursor.
			return nil, &PeerHTTPStatusError{identity: expectedIdentity, status: status, body: string(respBody)}
		}
		ids = presentIDs // effective ID list after the 404-retry
	} else if status != http.StatusOK {
		return nil, &PeerHTTPStatusError{identity: expectedIdentity, status: status, body: string(respBody)}
	}
	// Validate the response epoch envelope against the captured request
	// context (Milestone 5A mixed-mode fencing). A legacy peer (no envelope),
	// a missing/duplicate/contradictory epoch header, or an epoch mismatch
	// with the request's authorized context is rejected, so fetched records
	// from a peer that cannot demonstrate compatible protocol behavior are
	// not inserted as a synchronization source.
	if err := validateResponseEnvelope(respHeaders, expectedIdentity, c.getProvider(), reqCtx); err != nil {
		return nil, &PeerAckError{identity: expectedIdentity, reason: err.Error()}
	}
	var out fetchMessagesResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("decoding fetch response: %v", err)}
	}
	// The response must contain exactly the number of effective requested
	// messages (after any 404-retry that reduced the ID list).
	if len(out.Messages) != len(ids) {
		return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("expected %d messages but received %d", len(ids), len(out.Messages))}
	}
	// Validate: messages must be in ascending order, each ID must be an
	// effective requested ID, and no duplicates.
	requested := make(map[string]bool, len(ids))
	for _, id := range ids {
		requested[id] = true
	}
	for i := 1; i < len(out.Messages); i++ {
		if out.Messages[i].ID <= out.Messages[i-1].ID {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("fetched messages not in ascending order at position %d", i)}
		}
	}
	for i, m := range out.Messages {
		if !requested[m.ID] {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("unsolicited message ID %q at position %d", m.ID, i)}
		}
		if err := validateCanonicalUUIDv7(m.ID); err != nil {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("message ID %q at position %d: %v", m.ID, i, err)}
		}
		if m.CreatedAt == "" {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("message %q has empty timestamp", m.ID)}
		}
		if _, err := parseRFC3339(m.CreatedAt); err != nil {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("message %q has malformed timestamp %q: %v", m.ID, m.CreatedAt, err)}
		}
		// Enforce the per-record wire-size contract. A legacy or malformed
		// record with an oversized origin or body would otherwise serialize
		// to a response exceeding maxSyncResponseBodyBytes and be rejected by
		// doRequest; rejecting it here gives a precise, explicit error and
		// guarantees the response stays within the response bound. The
		// encoded size is measured with json.Marshal (the same encoder the
		// server uses), so it is the true serialized size of the record in a
		// sync response. The 253-byte origin limit and 4096-byte body limit
		// guarantee that every valid record fits within the per-record
		// wire-size bound; an oversized legacy record is rejected explicitly.
		if len(m.Origin) > maxMessageOriginBytes {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("message %q has an origin of %d bytes, exceeding the maximum of %d", m.ID, len(m.Origin), maxMessageOriginBytes)}
		}
		if encoded, err := json.Marshal(m); err == nil && len(encoded) > maxSyncMessageWireBytes {
			return nil, &PeerAckError{identity: expectedIdentity, reason: fmt.Sprintf("message %q serializes to %d bytes, exceeding the per-record wire-size maximum of %d", m.ID, len(encoded), maxSyncMessageWireBytes)}
		}
	}
	return out.Messages, nil
}

// parseMissingIDsResponse parses a peer's 404 sync-fetch body and returns the
// set of message IDs reliably reported as missing (or expired), validated
// against the original request. It returns ok=false when the body does not
// establish a legitimate missing-ID set:
//
//   - the body is not valid JSON, or
//   - it carries no "missing_ids" field (a malformed or unrelated 404 error
//     body that does not report which IDs are missing), or
//   - a reported missing ID is not a canonical UUIDv7, is duplicated, or was
//     NOT part of the original request (an unrelated or contradictory
//     response must not be interpreted as an expiration).
//
// The returned map contains exactly the validated, request-member missing IDs.
func parseMissingIDsResponse(body []byte, requested []string) (map[string]bool, bool) {
	var m missingIDsResponse
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, false
	}
	if m.MissingIDs == nil {
		// No "missing_ids" field: the body does not report missing IDs in a
		// structured, verifiable form. Reject as malformed (do not fall back
		// to a legacy text parse: a human-readable error string cannot
		// establish that the reported IDs belong to the original request).
		return nil, false
	}
	requestedSet := make(map[string]bool, len(requested))
	for _, id := range requested {
		requestedSet[id] = true
	}
	seen := make(map[string]bool, len(m.MissingIDs))
	result := make(map[string]bool, len(m.MissingIDs))
	for _, id := range m.MissingIDs {
		if validateCanonicalUUIDv7(id) != nil {
			return nil, false
		}
		if !requestedSet[id] {
			// A reported missing ID that was not in the original request:
			// the response is unrelated to this fetch. Reject.
			return nil, false
		}
		if seen[id] {
			// A duplicated missing ID: a well-formed 404 reports each missing
			// ID at most once. Reject as malformed.
			return nil, false
		}
		seen[id] = true
		result[id] = true
	}
	return result, true
}

// validateCanonicalUUIDv7 checks that s is a valid canonical UUIDv7 in
// lowercase hyphenated form.
func validateCanonicalUUIDv7(s string) error {
	u, err := uuid.Parse(s)
	if err != nil {
		return fmt.Errorf("%q is not a valid UUID", s)
	}
	if u.Version() != 7 {
		return fmt.Errorf("%q is not a UUIDv7 (version %d)", s, u.Version())
	}
	if u.String() != s {
		return fmt.Errorf("%q is not in canonical UUIDv7 form", s)
	}
	return nil
}

// verifyAcknowledgedTimestamp parses the submitted and acknowledged creation
// timestamps and confirms they denote the same instant. It accepts equivalent
// RFC3339 representations (e.g. "2026-11-20T12:00:00Z" and
// "2026-11-20T12:00:00.000000000Z" both parse to the same instant) and
// rejects malformed or mismatched timestamps.
func verifyAcknowledgedTimestamp(ackTS, sentTS string) error {
	ackTime, err := parseRFC3339(ackTS)
	if err != nil {
		return fmt.Errorf("acknowledged timestamp %q is malformed: %v", ackTS, err)
	}
	sentTime, err := parseRFC3339(sentTS)
	if err != nil {
		return fmt.Errorf("submitted timestamp %q is malformed: %v", sentTS, err)
	}
	if !ackTime.Equal(sentTime) {
		return fmt.Errorf("acknowledged timestamp %q does not match submitted timestamp %q", ackTS, sentTS)
	}
	return nil
}

// parseRFC3339 parses an RFC3339 timestamp, accepting both with and without
// fractional seconds.
func parseRFC3339(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, errors.New("timestamp is empty")
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

// Close releases any remaining idle connections. In the current implementation
// each request creates a per-request Transport and closes its idle connections
// after the response is read, so Close is typically a no-op.
// It is provided for interface compatibility and future reuse of a shared
// transport.
func (c *Client) Close() error {
	return nil
}
