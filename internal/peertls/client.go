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
	"time"
)

// maxResponseBodyBytes bounds the response body read from a peer (for
// meaningful error reporting on non-success responses). Message responses
// are small; 8 KiB is generous without risking unbounded memory.
const maxResponseBodyBytes = 8192

// Client is an authenticated peer HTTP client for the private peer endpoints.
// It wraps the Manager's credential snapshot for the mTLS transport.
type Client struct {
	mgr *Manager
}

// NewClient creates a Client bound to the given Manager. The Manager must have
// been Started (credentials loaded) before making requests.
func NewClient(mgr *Manager) *Client {
	return &Client{mgr: mgr}
}

// peerStatusResponse is the JSON response from GET /internal/status.
type peerStatusResponse struct {
	NodeID          string `json:"node_id"`
	Version         string `json:"version"`
	PodID           string `json:"pod_id,omitempty"`
	CertFingerprint string `json:"cert_fingerprint,omitempty"`
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
// It is the shared plumbing for GetStatus and SendMessage.
//
// method is the HTTP method (GET or POST). body is the request body (nil for
// GET). The caller supplies the exact expected peer identity; the TLS layer
// verifies the server presents a certificate whose single DNS SAN exactly
// matches expectedIdentity.
func (c *Client) doRequest(ctx context.Context, method, path, dialAddr, expectedIdentity string, body []byte) ([]byte, int, error) {
	tlsCfg, err := c.mgr.TLSConfigForClient(dialAddr, expectedIdentity)
	if err != nil {
		return nil, 0, fmt.Errorf("building peer TLS client config: %w", err)
	}

	var req *http.Request
	if body == nil {
		req, err = http.NewRequestWithContext(ctx, method, "https://"+dialAddr+path, nil)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, "https://"+dialAddr+path, bytes.NewReader(body))
	}
	if err != nil {
		return nil, 0, fmt.Errorf("building request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	client := buildClient(tlsCfg)

	resp, err := client.Do(req)
	if err != nil {
		// Close idle connections on the per-request transport to avoid
		// leaking idle TLS connections. The transport is discarded after
		// this call, so any idle connections would otherwise be abandoned.
		client.Transport.(*http.Transport).CloseIdleConnections()
		return nil, 0, fmt.Errorf("requesting peer %s at %s: %w", expectedIdentity, dialAddr, err)
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

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("reading peer response body: %w", err)
	}
	return respBody, resp.StatusCode, nil
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
func (c *Client) GetStatus(ctx context.Context, dialAddr, expectedIdentity string) (*peerStatusResponse, error) {
	respBody, status, err := c.doRequest(ctx, http.MethodGet, "/internal/status", dialAddr, expectedIdentity, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("peer %s returned status %d: %s", expectedIdentity, status, string(respBody))
	}
	var out peerStatusResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, fmt.Errorf("decoding peer status: %w", err)
	}
	return &out, nil
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
	respBody, status, err := c.doRequest(ctx, http.MethodPost, "/internal/messages", dialAddr, expectedIdentity, body)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK && status != http.StatusCreated {
		return nil, fmt.Errorf("peer %s rejected message: status %d: %s", expectedIdentity, status, string(respBody))
	}
	var out sendResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return nil, fmt.Errorf("decoding send response from %s: %w", expectedIdentity, err)
	}
	// A successful response must echo back a message acknowledgment. A 200/201
	// without one is a protocol defect: the receiver must confirm what it
	// persisted, and the caller cannot assume the insert happened.
	if out.Message == nil {
		return nil, fmt.Errorf("peer %s returned a successful response (status %d) without a message acknowledgment", expectedIdentity, status)
	}
	ack := out.Message
	if ack.ID != msg.ID {
		return nil, fmt.Errorf("peer %s acknowledged message ID %q but submitted ID was %q", expectedIdentity, ack.ID, msg.ID)
	}
	if ack.Body != msg.Body {
		return nil, fmt.Errorf("peer %s acknowledged message body %q but submitted body was %q", expectedIdentity, ack.Body, msg.Body)
	}
	if ack.Origin != msg.Origin {
		return nil, fmt.Errorf("peer %s acknowledged message origin %q but submitted origin was %q", expectedIdentity, ack.Origin, msg.Origin)
	}
	if err := verifyAcknowledgedTimestamp(ack.CreatedAt, msg.CreatedAt); err != nil {
		return nil, fmt.Errorf("peer %s acknowledged a mismatched message timestamp: %w", expectedIdentity, err)
	}
	return ack, nil
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
