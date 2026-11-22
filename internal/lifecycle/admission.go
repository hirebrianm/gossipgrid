// Package lifecycle: admission guard.
//
// The Admission guard provides a defensible in-process boundary for
// authorizing replication and public write operations. It is used by:
//   - the public HTTP handlers (POST /api/messages, GET /api/messages);
//   - the private peer handlers (POST /internal/messages, sync endpoints);
//   - the gossip engine (outbound send admission);
//   - the anti-entropy engine (outbound fetch/send admission).
//
// The guard is concurrency-safe: it reads the current lifecycle state and
// accepted metadata under a single lock, so a state transition cannot
// interleave between the check and the operation.
//
// Design: the guard does NOT hold a lifecycle mutex across network
// operations. It performs a fast, in-process check and returns a decision.
// The caller is responsible for performing the operation only when the
// decision is permitted. This is a defensible in-process admission boundary
// (not a superficial check that is easily bypassed by concurrent state
// changes) because:
//   - The check and the decision are atomic (single RLock).
//   - Once a decision is made, the state can change, but the operation
//     is already committed to proceed. This is the intended linearization:
//     the operation is authorized at the moment the check passes, and the
//     receiver's idempotent insert (or the epoch check on the remote side)
//     provides the ultimate safety.
//   - The store's `Bind` operation validates and establishes the durable
//     local epoch binding (a one-time, exclusive operation). Normal message
//     insertion does NOT independently revalidate the epoch binding; the
//     fail-closed admission guard is the line of defense before the insert.
//     In-flight admission checks have a precisely defined local
//     linearization boundary: the admission recheck is performed immediately
//     before the SQLite mutation (or data-serving read) in a short critical
//     section, so a concurrent revocation that happens-before the recheck is
//     observed and the operation is rejected.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
)

// AdmissionDecision is the result of an admission check.
type AdmissionDecision struct {
	// Allowed reports whether the operation may proceed.
	Allowed bool
	// Reason is a stable diagnostic string explaining the decision.
	Reason string
	// State is the lifecycle state at the time of the check.
	State State
}

// Admission is a concurrency-safe guard that checks whether an operation is
// permitted by the current authorization decision. When lifecycle protection
// is enabled, every check reads the controller's single, internally
// consistent AuthorizationSnapshot (one read lock) and applies the full
// fail-closed policy: ready state plus a fresh, valid, matching authoritative
// observation plus a matching durable database binding, with no recovery
// lockout and not stopped. A state, metadata, or binding change therefore
// cannot interleave between reading the state and reading the metadata or
// binding.
//
// When enabled is false (lifecycle protection disabled), all checks are
// permitted, preserving the existing Milestone 4 behavior.
type Admission struct {
	// enabled reports whether lifecycle protection is active.
	enabled bool
	// ctrl is the controller whose authorization snapshot is checked. It is
	// nil when protection is disabled.
	ctrl *Controller
}

// NewAdmission creates an Admission guard. enabled controls whether the
// guard is active (true when lifecycle protection is enabled); when enabled,
// ctrl must be non-nil (its authorization snapshot is the single source of
// the decision). A nil ctrl with enabled true is treated as a programming
// error: all checks fail closed rather than silently authorizing work.
func NewAdmission(enabled bool, ctrl *Controller) *Admission {
	return &Admission{enabled: enabled, ctrl: ctrl}
}

// NewAdmissionDisabled creates a guard that permits all operations (lifecycle
// protection disabled). It exists so callers can construct a guard without a
// controller in the disabled path.
func NewAdmissionDisabled() *Admission {
	return &Admission{enabled: false}
}

// Enabled reports whether the admission guard is active.
func (a *Admission) Enabled() bool {
	return a.enabled
}

// snapshot returns the controller's current authorization snapshot, or a
// zero snapshot (not authorized) when protection is enabled but no controller
// is present (a fail-closed programming-error path).
func (a *Admission) snapshot() AuthorizationSnapshot {
	if a.ctrl == nil {
		return AuthorizationSnapshot{}
	}
	return a.ctrl.authorization()
}

// CheckWrite checks whether a public message write (POST /api/messages) is
// permitted. It applies the full fail-closed authorization policy via the
// controller's single consistent snapshot: ready state plus a fresh, valid,
// matching authoritative observation plus a matching durable database
// binding, with no recovery lockout and not stopped.
func (a *Admission) CheckWrite(ctx context.Context) AdmissionDecision {
	if !a.enabled {
		return AdmissionDecision{Allowed: true, Reason: "lifecycle protection disabled"}
	}
	snap := a.snapshot()
	if !snap.authorized() {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "write not authorized: " + snap.authorizationReason(),
			State:   snap.State,
		}
	}
	return AdmissionDecision{
		Allowed: true,
		Reason:  "authorized",
		State:   snap.State,
	}
}

// CheckReplicationInbound checks whether an inbound replication request
// (POST /internal/messages, GET /internal/sync/ids, POST /internal/sync/messages)
// may be served. It applies the full fail-closed authorization policy via the
// controller's single consistent snapshot, and additionally requires the
// request's epoch context to match the local authorized metadata.
//
// Inbound replication is permitted only when:
//   - the replica is authorized (ready state + fresh, valid, matching
//     authoritative observation + matching durable binding, no lockout, not
//     stopped), AND
//   - the request carries a nonempty epoch context that matches the local
//     authorized metadata's ClusterID and EpochID.
//
// It is NOT permitted when the replica is not authorized, or when the
// request's epoch context is missing or does not match (a 409 mismatch,
// distinct from a 503 unready/no-metadata rejection).
func (a *Admission) CheckReplicationInbound(ctx context.Context, reqCtx RequestContext) AdmissionDecision {
	if !a.enabled {
		return AdmissionDecision{Allowed: true, Reason: "lifecycle protection disabled"}
	}

	snap := a.snapshot()

	// The replica must be authorized to serve authoritative data at all.
	if !snap.authorized() {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "inbound replication not authorized: " + snap.authorizationReason(),
			State:   snap.State,
		}
	}

	// The request's epoch context must match the local authorized metadata.
	meta := snap.Metadata
	if reqCtx.ClusterID == "" || reqCtx.EpochID == "" {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "missing epoch context in replication request",
			State:   snap.State,
		}
	}
	if reqCtx.ClusterID != meta.ClusterID {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "cluster ID mismatch: request=" + reqCtx.ClusterID + " local=" + meta.ClusterID,
			State:   snap.State,
		}
	}
	if reqCtx.EpochID != meta.EpochID {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "epoch ID mismatch: request=" + reqCtx.EpochID + " local=" + meta.EpochID,
			State:   snap.State,
		}
	}

	return AdmissionDecision{
		Allowed: true,
		Reason:  "authorized and epoch context matches",
		State:   snap.State,
	}
}

// CheckReplicationOutbound checks whether an outbound replication operation
// (gossip send, anti-entropy fetch/send) may proceed. It applies the full
// fail-closed authorization policy via the controller's single consistent
// snapshot: ready state plus a fresh, valid, matching authoritative
// observation plus a matching durable database binding, with no recovery
// lockout and not stopped.
//
// It is NOT permitted when the replica is not authorized (unready, no or
// invalid or stale metadata, no binding, a binding/metadata mismatch, a
// recovery lockout, or the controller has been stopped).
func (a *Admission) CheckReplicationOutbound(ctx context.Context) AdmissionDecision {
	if !a.enabled {
		return AdmissionDecision{Allowed: true, Reason: "lifecycle protection disabled"}
	}
	snap := a.snapshot()
	if !snap.authorized() {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "outbound replication not authorized: " + snap.authorizationReason(),
			State:   snap.State,
		}
	}
	return AdmissionDecision{
		Allowed: true,
		Reason:  "authorized",
		State:   snap.State,
	}
}

// RequestContext carries the caller's accepted cluster and epoch context in
// an inbound replication request. It is parsed from bounded HTTP headers
// (see the header constants in this package).
type RequestContext struct {
	ClusterID string
	EpochID   string
}

// Header names for the epoch fencing protocol. These are bounded,
// non-sensitive identifiers carried in HTTP headers. They are NOT proof of
// peer identity — mTLS is the sole basis for peer identity. The headers
// carry the caller's accepted cluster and epoch context for the receiver to
// compare against its own authorized metadata.
const (
	// HeaderClusterID carries the caller's accepted ClusterID.
	HeaderClusterID = "X-GossipGrid-Cluster-ID"

	// HeaderEpochID carries the caller's accepted EpochID.
	HeaderEpochID = "X-GossipGrid-Epoch-ID"
)

// MaxEpochHeaderBytes bounds the length of each epoch header value.
// ClusterID and EpochID are UUIDv7 strings (36 characters); 64 bytes
// provides headroom while remaining finite.
const MaxEpochHeaderBytes = 64

// Response envelope headers (Milestone 5A mixed-mode fencing). When a peer has
// lifecycle protection enabled, it sets these headers on its replication
// responses (status, inventory, message-fetch, message-exchange). They let a
// lifecycle-enabled client confirm that the remote peer enforces the
// lifecycle/epoch protocol: a peer that returns a successful response but
// cannot demonstrate compatible protocol behavior (i.e. a legacy peer with
// lifecycle disabled, which never sets these headers) is rejected as a
// synchronization source. A valid mTLS certificate is necessary but not
// sufficient: the response envelope is what proves the peer is running the
// epoch-fencing protocol.
//
//   - HeaderRespClusterID / HeaderRespEpochID carry the peer's authorized
//     cluster/epoch from its authorization snapshot.
//   - HeaderLifecycleActive is "true" when the peer has lifecycle protection
//     enabled. Its presence (regardless of value) marks the peer as a
//     lifecycle-capable peer; a legacy peer never sets it.
const (
	// HeaderRespClusterID carries the peer's authorized ClusterID.
	HeaderRespClusterID = "X-GossipGrid-Resp-Cluster-ID"

	// HeaderRespEpochID carries the peer's authorized EpochID.
	HeaderRespEpochID = "X-GossipGrid-Resp-Epoch-ID"

	// HeaderLifecycleActive marks the peer as lifecycle-capable.
	HeaderLifecycleActive = "X-GossipGrid-Lifecycle-Active"
)

// ResponseEnvelope is the peer's self-reported authorization context carried
// in its replication response headers. A nil/absent envelope indicates a
// legacy peer (lifecycle disabled) that does not demonstrate epoch-fencing
// protocol behavior.
type ResponseEnvelope struct {
	ClusterID       string
	EpochID         string
	LifecycleActive bool
}

// ParseRequestContext extracts the caller's epoch context from HTTP headers.
// It returns an error when the headers are malformed (duplicate, oversized,
// or empty when required).
//
// The headers are NOT trusted as proof of peer identity; they are the
// caller's self-reported epoch context that the receiver compares against
// its own authorized metadata.
func ParseRequestContext(getHeader func(string) []string, require bool) (RequestContext, error) {
	var rc RequestContext

	clusterVals := getHeader(HeaderClusterID)
	if len(clusterVals) == 0 {
		if require {
			return rc, errors.New("missing " + HeaderClusterID + " header")
		}
		return rc, nil
	}
	if len(clusterVals) > 1 {
		return rc, fmt.Errorf("duplicate %s header", HeaderClusterID)
	}
	if len(clusterVals[0]) > MaxEpochHeaderBytes {
		return rc, fmt.Errorf("%s header exceeds %d bytes", HeaderClusterID, MaxEpochHeaderBytes)
	}
	rc.ClusterID = clusterVals[0]

	epochVals := getHeader(HeaderEpochID)
	if len(epochVals) == 0 {
		if require {
			return rc, errors.New("missing " + HeaderEpochID + " header")
		}
		return rc, nil
	}
	if len(epochVals) > 1 {
		return rc, fmt.Errorf("duplicate %s header", HeaderEpochID)
	}
	if len(epochVals[0]) > MaxEpochHeaderBytes {
		return rc, fmt.Errorf("%s header exceeds %d bytes", HeaderEpochID, MaxEpochHeaderBytes)
	}
	rc.EpochID = epochVals[0]

	// If one is present, both must be present (when require is true).
	if require && (rc.ClusterID == "" || rc.EpochID == "") {
		return rc, errors.New("both " + HeaderClusterID + " and " + HeaderEpochID + " headers must be present")
	}

	return rc, nil
}

// EpochMismatchStatus is the HTTP status code returned by the receiver when
// an inbound replication request carries an epoch context that does not match
// the receiver's authorized metadata. It is distinct from ordinary transient
// network failures (5xx, 429) so clients can classify it as a permanent
// rejection rather than retrying indefinitely.
const EpochMismatchStatus = 409

// UnreadyStatus is the HTTP status code returned by the receiver when it
// cannot safely serve replication (unready state or no metadata).
const UnreadyStatus = 503
