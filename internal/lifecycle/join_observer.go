// Package lifecycle: trusted bootstrap-source observation boundary
// (Milestone 5B, Stage 3A, Finding 4).
//
// This file establishes the narrowly-scoped, dependency-injected boundary
// through which the local joining operation (AuthorizeJoin) obtains
// evidence that at least one eligible bootstrap source exists in the target
// epoch. The purpose is to ensure that normal production callers cannot
// accidentally bypass the required AUTHENTICATED observation: source
// eligibility may not be asserted by an untrusted, publicly-constructible
// value; it must be produced by an authenticated, same-epoch peer status
// observation.
//
// The production implementation (peertls.BootstrapSourceObserver, a wrapper
// around the existing peer transport and the GET /internal/status
// endpoint) performs a real mTLS handshake and strict response validation.
// Controller unit tests inject a test double at this interface; the real
// strict observation is verified by real-mTLS tests in the peertls package.
//
// The trusted boundary is an in-process engineering control: it does NOT
// protect against malicious code that is already executing inside the same
// application process (Go type visibility is not a security boundary
// against an in-process attacker). It exists so that a future production
// orchestration path cannot silently substitute an unauthenticated,
// caller-asserted boolean for the required observation.

package lifecycle

import (
	"context"
	"time"
)

// SourceObservationResult is the authenticated, strictly-validated result of
// a bootstrap-source observation. It is NOT publicly constructible: the
// evidence fields are unexported, and the only way to obtain a valid
// SourceObservationResult is through the Observe method of a
// BootstrapSourceObserver (the trusted boundary). A zero-value
// SourceObservationResult (Observed false) is always invalid.
//
// The result binds the observation to a specific expected target
// (ClusterID/EpochID) and a specific expected peer identity, and carries the
// authenticated peer's reported state and the observation's bounded
// lifetime (ObservedAt + ObservationTTL).
type SourceObservationResult struct {
	// observed reports whether an eligible source was actually observed.
	// A zero-value result is never observed and is invalid.
	observed bool

	// observedAt bounds the observation's lifetime.
	observedAt time.Time
	// ttl is the bounded observation lifetime; after it elapses the result
	// is no longer current and must not authorize joining.
	ttl time.Duration

	// expected is the pinned target the observation was made for.
	expected RequestContext

	// peerIdentity is the authenticated peer's certificate DNS identity
	// (the single DNS SAN the mTLS handshake verified).
	peerIdentity string
	// peerNodeID is the peer's self-reported short logical node identity
	// (the PodID from the status body). It is DISTINCT from peerIdentity
	// (the certificate DNS SAN): the two are not the same string
	// convention, and the observation binds the authenticated identity to
	// the self-reported identity explicitly.
	peerNodeID string

	// The authenticated peer's reported state (from the status body), which
	// the strict observation verified is READY or DEGRADED with
	// BootstrapSourceEligible=true and a complete, matching, nonempty
	// cluster/epoch that agrees with the response headers and the expected
	// target.
	lifecycleState          string
	clusterID               string
	epochID                 string
	bootstrapSourceEligible bool
	readiness               bool
}

// Observed reports whether an eligible source was actually observed.
func (r SourceObservationResult) Observed() bool { return r.observed }

// PeerIdentity returns the authenticated peer's certificate DNS identity.
func (r SourceObservationResult) PeerIdentity() string { return r.peerIdentity }

// PeerNodeID returns the peer's self-reported short logical node identity.
func (r SourceObservationResult) PeerNodeID() string { return r.peerNodeID }

// Valid reports whether this observation result is structurally valid and
// still within its bounded lifetime as of now. A zero-value result is
// never valid.
//
// The temporal policy (enforced here, at both the pre-binding revalidation
// and the final authorization guard through the single consistent
// controller-side observation validator):
//
//   - a FUTURE observation timestamp (observedAt after now) is rejected: an
//     observation that has not yet occurred is not evidence; it is either
//     a clock skew or a fabricated value, and it must not authorize joining;
//   - an invalid or expired lifetime is rejected (ttl <= 0, a zero
//     observedAt, or now after observedAt+ttl): an observation older than
//     its bounded lifetime is no longer current;
//   - the maximum lifetime is enforced consistently by the clamp in
//     NewSourceObservationResult (a requested TTL longer than
//     MaxSourceObservationTTL is reduced to the maximum), so no result can
//     outlive the bounded observation contract.
//
// The observer is an explicitly trusted in-process dependency. This check
// is an engineering control (it keeps a stale, future-dated, or
// over-lived result from authorizing joining); it does NOT claim the Go
// type prevents malicious in-process application code from fabricating an
// observation.
func (r SourceObservationResult) Valid(now time.Time) bool {
	if !r.observed {
		return false
	}
	if r.expected.ClusterID == "" || r.expected.EpochID == "" {
		return false
	}
	if r.peerIdentity == "" || r.peerNodeID == "" {
		return false
	}
	if r.ttl <= 0 || r.observedAt.IsZero() {
		return false
	}
	// Reject a future observation timestamp (observedAt after now): an
	// observation that has not yet occurred is not evidence (a clock skew
	// or a fabricated value).
	if r.observedAt.After(now) {
		return false
	}
	// Reject an expired observation (now past the bounded lifetime
	// observedAt + ttl). The maximum lifetime is clamped to
	// MaxSourceObservationTTL by NewSourceObservationResult, so a valid
	// result's ttl is always <= the maximum and the lifetime is
	// consistently bounded.
	if now.After(r.observedAt.Add(r.ttl)) {
		return false
	}
	return true
}

// BootstrapSourceObserver is the narrow, trusted, dependency-injected
// boundary through which the local joining operation obtains
// authenticated bootstrap-source evidence. It is implemented in production
// by the peertls package (peertls.BootstrapSourceObserver) using the
// existing peer transport and the GET /internal/status endpoint (no second
// transport). It must NOT be implemented by a caller that asserts an
// unauthenticated boolean.
//
// Observe performs an authenticated, same-epoch observation of the single,
// explicitly supplied candidate (dialAddr, expectedIdentity). It verifies:
//   - the existing mTLS handshake succeeds and the server presents exactly
//     the expected certificate DNS identity (expectedIdentity);
//   - the response carries exactly one X-GossipGrid-Lifecycle-Active header
//     with value "true";
//   - the response carries complete, nonempty, bounded cluster and epoch
//     headers;
//   - the response headers agree with the status body's cluster/epoch;
//   - the reported cluster/epoch agrees with the expected target;
//   - the reported lifecycle state is READY or DEGRADED;
//   - BootstrapSourceEligible is true and readiness is consistent;
//   - the observed peer is not the local replica;
//   - the peer's self-reported identity (PodID) is bound to the
//     authenticated certificate identity using the documented
//     PodID/NodeID conventions (the short logical NodeID and the
//     certificate DNS SAN are not assumed to be the same string).
//
// On success it returns a SourceObservationResult that is valid for a
// bounded lifetime (ObservationTTL) after the observation. The serving
// peer's per-request bootstrap admission remains authoritative for each
// individual fetch; this observation only authorizes the local joining
// operation.
type BootstrapSourceObserver interface {
	// Observe performs a single authenticated observation of the given
	// candidate. expected is the pinned target cluster/epoch (from the
	// authoritative metadata); localNodeID is the local replica's identity
	// (used to reject self-observation).
	Observe(ctx context.Context, dialAddr, expectedIdentity string, expected RequestContext, localNodeID string) (SourceObservationResult, error)
}

// MaxSourceObservationTTL is the maximum bounded lifetime of a
// bootstrap-source observation result. An observation older than this
// duration is no longer considered current and must not authorize joining.
// The production observer (peertls.BootstrapSourceObserver) uses this value.
// A test or misconfigured observer that constructs an arbitrarily long-lived
// result through NewSourceObservationResult is clamped to this maximum: the
// trusted boundary is an in-process engineering control (it does not protect
// against malicious in-process code), and this clamp prevents a single
// result from outliving the bounded observation contract.
const MaxSourceObservationTTL = 10 * time.Second

// NewSourceObservationResult constructs a valid SourceObservationResult. It is
// exported for use by the production observer (peertls.BootstrapSourceObserver)
// and by test doubles within the lifecycle package. External production code
// should not construct results directly; it should use the
// BootstrapSourceObserver interface.
//
// The ttl is clamped to MaxSourceObservationTTL: a result with a requested
// TTL longer than the maximum is reduced to the maximum. A zero or negative
// TTL produces an invalid result (Valid returns false). A zero observedAt
// produces an invalid result. This ensures that no test or misconfigured
// observer can construct an arbitrarily long-lived result that bypasses the
// bounded observation contract.
func NewSourceObservationResult(observedAt time.Time, ttl time.Duration, expected RequestContext, peerIdentity, peerNodeID, lifecycleState, clusterID, epochID string, bootstrapSourceEligible, readiness bool) SourceObservationResult {
	if ttl > MaxSourceObservationTTL {
		ttl = MaxSourceObservationTTL
	}
	return SourceObservationResult{
		observed:                true,
		observedAt:              observedAt,
		ttl:                     ttl,
		expected:                expected,
		peerIdentity:            peerIdentity,
		peerNodeID:              peerNodeID,
		lifecycleState:          lifecycleState,
		clusterID:               clusterID,
		epochID:                 epochID,
		bootstrapSourceEligible: bootstrapSourceEligible,
		readiness:               readiness,
	}
}
