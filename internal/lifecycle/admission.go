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

// CheckBootstrapSourceInbound checks whether the serving replica may serve a
// bootstrap baseline FETCH (serving replica side) for a request that carries
// the bootstrap request-purpose header: GET /internal/sync/ids and
// POST /internal/sync/messages. It is the serving-side counterpart to the
// requesting replica's CheckBootstrapFetch.
//
// It is the ONLY path by which an otherwise-unready serving replica would
// answer the sync endpoints, and it is strictly more restrictive than the
// ordinary CheckReplicationInbound in state: the serving replica must be in a
// ready state (READY or DEGRADED) AND fully authorized (a fresh, valid,
// matching authoritative observation plus a matching durable binding, no
// recovery lockout, not stopped), AND the request's epoch context must match
// the local authorized metadata.
//
// It does NOT relax any existing rule:
//
//   - a serving replica in SYNCING, GENESIS, DISCOVERING, STARTING, or
//     RECOVERY_REQUIRED is never authorized (IsReady is false), so it is
//     rejected;
//   - a wrong-epoch or headerless request is rejected (409) exactly as for
//     ordinary replication;
//   - the response envelope (lifecycle/epoch headers) is still required, so
//     a legacy peer is never accepted as a source by a lifecycle-enabled
//     requester (enforced client-side via validateResponseEnvelope).
//
// The header that selected this check is UNTRUSTED metadata: it only chooses
// the serving-side policy. The actual authorization is the full
// snapshot.Authorized() decision, which a non-ready replica cannot pass.
func (a *Admission) CheckBootstrapSourceInbound(ctx context.Context, reqCtx RequestContext) AdmissionDecision {
	if !a.enabled {
		return AdmissionDecision{Allowed: true, Reason: "lifecycle protection disabled"}
	}
	snap := a.snapshot()
	// The serving replica must be fully authorized to serve current-epoch
	// data at all (ready state + fresh, valid, matching observation +
	// matching binding, no lockout, not stopped). This is the same
	// fail-closed decision the ordinary replication check requires; a
	// non-ready serving replica (including SYNCING) can never pass it.
	if !snap.authorized() {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "bootstrap source inbound not authorized: " + snap.authorizationReason(),
			State:   snap.State,
		}
	}
	// The serving replica must be eligible to act as a bootstrap source in
	// its current state (READY or DEGRADED). authorized() already requires a
	// ready state, so this is a belt-and-suspenders check that the serving
	// replica's state is the documented eligible set; it never broadens.
	if !snap.State.IsBootstrapSourceEligible() {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "replica state " + snap.State.String() + " is not eligible to serve a bootstrap baseline",
			State:   snap.State,
		}
	}
	// The request's epoch context must match the local authorized metadata.
	meta := snap.Metadata
	if reqCtx.ClusterID == "" || reqCtx.EpochID == "" {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "missing epoch context in bootstrap fetch request",
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
		Reason:  "bootstrap source authorized and epoch context matches",
		State:   snap.State,
	}
}

// checkBootstrapSourceFromSnapshot evaluates the bootstrap source-serving
// admission decision from a GIVEN AuthorizationSnapshot (not a fresh
// snapshot). It is used by the peer server handlers (Milestone 5B, Stage 2,
// Issue 4) so the admission decision and the response envelope are both
// derived from the SAME consistent snapshot. The logic is identical to
// CheckBootstrapSourceInbound, except the snapshot is supplied rather than
// read from the controller.
func (a *Admission) CheckBootstrapSourceFromSnapshot(snap AuthorizationSnapshot, reqCtx RequestContext) AdmissionDecision {
	if !a.enabled {
		return AdmissionDecision{Allowed: true, Reason: "lifecycle protection disabled"}
	}
	if !snap.authorized() {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "bootstrap source inbound not authorized: " + snap.authorizationReason(),
			State:   snap.State,
		}
	}
	if !snap.State.IsBootstrapSourceEligible() {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "replica state " + snap.State.String() + " is not eligible to serve a bootstrap baseline",
			State:   snap.State,
		}
	}
	meta := snap.Metadata
	if reqCtx.ClusterID == "" || reqCtx.EpochID == "" {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "missing epoch context in bootstrap fetch request",
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
		Reason:  "bootstrap source authorized and epoch context matches",
		State:   snap.State,
	}
}

// checkReplicationInboundFromSnapshot evaluates the ordinary inbound
// replication admission decision from a GIVEN AuthorizationSnapshot (not a
// fresh snapshot). It is used by the peer server handlers (Milestone 5B,
// Stage 2, Issue 4) so the admission decision and the response envelope are
// both derived from the SAME consistent snapshot. The logic is identical to
// CheckReplicationInbound, except the snapshot is supplied rather than read
// from the controller.
func (a *Admission) CheckReplicationInboundFromSnapshot(snap AuthorizationSnapshot, reqCtx RequestContext) AdmissionDecision {
	if !a.enabled {
		return AdmissionDecision{Allowed: true, Reason: "lifecycle protection disabled"}
	}
	if !snap.authorized() {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "inbound replication not authorized: " + snap.authorizationReason(),
			State:   snap.State,
		}
	}
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
		Reason:  "inbound replication authorized and epoch context matches",
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

// CheckBootstrapFetch checks whether a bootstrap baseline FETCH (requesting
// replica side) is permitted: GET /internal/sync/ids and
// POST /internal/sync/messages. It authorizes ONLY an epoch-bound SYNCING
// replica to retrieve an initial baseline from an eligible authoritative
// source. It is a dedicated, narrowly scoped decision; it does NOT reuse the
// broad ordinary-replication decision (authorized, which requires a ready
// state) and it does NOT grant any other privilege:
//
//   - It does NOT authorize public writes (CheckWrite / authorized still
//     require a ready state).
//   - It does NOT authorize ordinary outbound gossip or ordinary inbound
//     replication writes (CheckReplicationOutbound / CheckWrite still gate
//     those).
//   - It does NOT authorize acting as an authoritative bootstrap source
//     (that is the serving-side CheckBootstrapSourceInbound, gated on the
//     serving replica being ready).
//   - It does NOT authorize READY promotion (Stage 3).
//   - It authorizes only the two sync fetch endpoints; the caller must pass
//     reqCtx so the epoch context is compared.
//
// The replica may fetch a bootstrap baseline only when ALL of the following
// hold (the bootstrap admission policy; see bootstrapAuthorized):
//
//   - the lifecycle state is exactly SYNCING (not READY, DEGRADED, GENESIS,
//     DISCOVERING, STARTING, or RECOVERY_REQUIRED);
//   - the controller is not stopped;
//   - the authoritative metadata is present and FRESH (not stale) and valid;
//   - the authoritative metadata is initialized (Initialized=true: the
//     cluster has a baseline to fetch);
//   - for a genesis-managed lifecycle configuration (a designated seed), the
//     authoritative record is COMPLETELY finalized (the data-loss protection
//     is armed and the one-time genesis authorization is consumed) — an
//     initialized-but-unfinalized record must not authorize a joining
//     replica to fetch;
//   - a durable local database binding exists;
//   - the binding's ClusterID and EpochID match the authoritative metadata
//     (no conflicting or wrong-epoch authority);
//   - the request's epoch context is present and matches the authoritative
//     metadata (a missing or mismatched epoch context is rejected).
//
// When lifecycle protection is disabled (the guard is not enabled), all
// checks are permitted, preserving the existing Milestone 4 behavior.
func (a *Admission) CheckBootstrapFetch(ctx context.Context, reqCtx RequestContext) AdmissionDecision {
	if !a.enabled {
		return AdmissionDecision{Allowed: true, Reason: "lifecycle protection disabled"}
	}
	snap := a.snapshot()
	if !snap.bootstrapAuthorized() {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "bootstrap fetch not authorized: " + snap.bootstrapAuthorizationReason(),
			State:   snap.State,
		}
	}
	// The request must carry a nonempty epoch context matching the local
	// authorized metadata. A missing or mismatched context is rejected so a
	// wrong-epoch or legacy (headerless) bootstrap request cannot be served.
	if reqCtx.ClusterID == "" || reqCtx.EpochID == "" {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "missing epoch context in bootstrap fetch request",
			State:   snap.State,
		}
	}
	meta := snap.Metadata
	if reqCtx.ClusterID != meta.ClusterID {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "cluster ID mismatch in bootstrap fetch request: request=" + reqCtx.ClusterID + " local=" + meta.ClusterID,
			State:   snap.State,
		}
	}
	if reqCtx.EpochID != meta.EpochID {
		return AdmissionDecision{
			Allowed: false,
			Reason:  "epoch ID mismatch in bootstrap fetch request: request=" + reqCtx.EpochID + " local=" + meta.EpochID,
			State:   snap.State,
		}
	}
	return AdmissionDecision{
		Allowed: true,
		Reason:  "bootstrap fetch authorized: SYNCING replica with fresh, finalized, matching binding and matching epoch context",
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

// Request-purpose header (Milestone 5B, Stage 2 bootstrap admission). A
// joining, epoch-bound SYNCING replica announces that a request to the sync
// endpoints (GET /internal/sync/ids, POST /internal/sync/messages) is a
// bootstrap baseline fetch rather than ordinary anti-entropy reconciliation.
//
// Security properties (see the CheckBootstrapFetch docs and the peer server
// bootstrap handlers):
//
//   - It is UNTRUSTED request metadata, NOT authentication. It never grants
//     any privilege on its own. mTLS identity and the epoch fencing headers
//     remain the sole basis for peer identity and epoch authorization.
//   - It is restricted in effect to the two sync endpoints. The serving
//     replica uses it only to select the serving-side admission policy
//     (CheckBootstrapSourceInbound vs. CheckReplicationInbound). Supplying
//     the header on any other endpoint (POST /internal/messages,
//     GET /internal/status, public API) has no effect.
//   - A lifecycle-enabled serving replica that does NOT recognize the
//     purpose (or receives an unsupported value) applies the ordinary
//     replication policy. It is never rejected solely because the header is
//     present or absent; the only way the header changes the decision is to
//     route a SYNCING requester's read to the bootstrap source-eligibility
//     check on a serving replica that is itself eligible to serve.
//   - A legacy serving replica (lifecycle disabled) ignores the header
//     entirely (legacy behavior preserved).
//
// The value is bounded and must be one of the supported purpose constants.
const HeaderRequestPurpose = "X-GossipGrid-Request-Purpose"

// Supported request-purpose values.
const (
	// PurposeBootstrapBaseline marks a bootstrap baseline fetch: a joining,
	// epoch-bound SYNCING requester retrieving its initial baseline from an
	// eligible authoritative source (READY or DEGRADED) via the sync
	// inventory/fetch endpoints. This is a READ from the serving replica's
	// perspective; it is distinct from inbound message insertion
	// (POST /internal/messages) and from ordinary anti-entropy.
	PurposeBootstrapBaseline = "bootstrap-baseline"

	// PurposeOrdinarySync marks ordinary anti-entropy reconciliation
	// (the default when the header is absent). A serving replica applies the
	// ordinary replication policy (CheckReplicationInbound).
	PurposeOrdinarySync = "ordinary-sync"
)

// maxRequestPurposeBytes bounds the request-purpose header value. It must fit
// the longest supported constant ("bootstrap-baseline" = 18 bytes) with
// headroom while remaining finite.
const maxRequestPurposeBytes = 64

// ParseRequestPurpose extracts the request-purpose header value and
// normalizes it. It returns the purpose constant and a boolean reporting
// whether the header was present and well-formed. An absent header yields
// (PurposeOrdinarySync, false). A malformed or unsupported value yields
// (PurposeOrdinarySync, false): the caller falls back to the ordinary
// policy (the header is never an error and never grants a privilege).
func ParseRequestPurpose(getHeader func(string) []string) (purpose string, present bool) {
	vals := getHeader(HeaderRequestPurpose)
	if len(vals) == 0 {
		return PurposeOrdinarySync, false
	}
	if len(vals) > 1 {
		return PurposeOrdinarySync, false
	}
	v := vals[0]
	if v == "" || len(v) > maxRequestPurposeBytes {
		return PurposeOrdinarySync, false
	}
	if v == PurposeBootstrapBaseline || v == PurposeOrdinarySync {
		return v, true
	}
	return PurposeOrdinarySync, false
}

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
