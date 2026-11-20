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
//   - reuses connections safely;
//   - applies reloaded credentials/trust to new handshakes.
package peertls

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

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
	tlsCfg, err := c.mgr.TLSConfigForClient(dialAddr, expectedIdentity)
	if err != nil {
		return nil, fmt.Errorf("building peer TLS client config: %w", err)
	}

	dialer := &net.Dialer{Timeout: 5 * time.Second}
	transport := &http.Transport{
		TLSClientConfig:     tlsCfg,
		DialContext:         dialer.DialContext,
		MaxIdleConns:        4,
		IdleConnTimeout:     30 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
	}
	client := &http.Client{
		Transport: transport,
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

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+dialAddr+"/internal/status", nil)
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		// Close idle connections on the per-request transport to avoid
		// leaking idle TLS connections. The transport is discarded after
		// this call, so any idle connections would otherwise be abandoned.
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("requesting peer %s at %s: %w", expectedIdentity, dialAddr, err)
	}
	// The close error is ignored: after the response body is read (or the
	// request fails) there is no useful recovery for a broken client
	// connection.
	defer func() {
		_ = resp.Body.Close()
		// Close idle connections on the per-request transport to avoid
		// leaking idle TLS connections. The transport is discarded after
		// this call, so any idle connections must be closed explicitly.
		transport.CloseIdleConnections()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("peer %s returned status %d: %s", expectedIdentity, resp.StatusCode, string(body))
	}

	var out peerStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decoding peer status: %w", err)
	}
	return &out, nil
}

// Close releases any remaining idle connections. In the current implementation
// each GetStatus call creates a per-request Transport and closes its idle
// connections after the response is read, so Close is typically a no-op.
// It is provided for interface compatibility and future reuse of a shared
// transport.
func (c *Client) Close() error {
	return nil
}
