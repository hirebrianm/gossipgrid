// Package lifecycle: deterministic bootstrap-source discovery and selection
// (Milestone 5B, Stage 3B).
//
// BootstrapSourceSelector is the narrow, deterministic mechanism that selects
// an eligible, authenticated bootstrap source from the current discovery
// membership. It is used by:
//   - an unbound replica preparing to call AuthorizeJoin;
//   - a replica with an incomplete, matching join binding preparing to resume
//     joining;
//   - an authorized SYNCING replica seeking a bootstrap source.
//
// The selector does NOT perform the joining operation, create a binding,
// transfer messages, or promote the replica. It selects a candidate; the
// caller (the future Stage 3C orchestration layer) decides when to act on it.
//
// A no-source result does NOT mean authoritative-data loss. An unavailable,
// unready, stale, mismatched, or unreachable peer is not evidence that its
// retained records are permanently lost. The local lifecycle state remains
// governed by the existing controller.

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"gossipgrid/internal/discovery"
)

// DiscoverySnapshotFunc is the narrow, read-only interface through which the
// selector obtains a coherent, immutable discovery snapshot. The production
// implementation (NewDiscoverySnapshotFunc) adapts discovery.Manager.Snapshot()
// to this interface.
//
// The snapshot is a single, immutable point-in-time view: peers, expected
// identities, local node, staleness, and mode are all derived from the same
// discovery generation, so they cannot disagree.
type DiscoverySnapshotFunc func() DiscoverySnapshotView

// DiscoverySnapshotView is a minimal read-only view of the discovery snapshot
// that the selector needs. It mirrors the relevant fields of
// discovery.Snapshot.
type DiscoverySnapshotView struct {
	// Peers are the discovered peers (deduplicated, self-excluded).
	Peers []DiscoveryPeerView
	// LocalNode is the canonical local pod identity, or nil when it cannot
	// be established.
	LocalNode *DiscoveryPeerView
	// ExpectedIdentities are the StatefulSet ordinal identities for the
	// configured replica count.
	ExpectedIdentities []string
	// Stale is true when the most recent DNS refresh failed and the data
	// reflects the last successful discovery pass.
	Stale bool
	// LastUpdated is the time of the most recent successful discovery pass.
	// Zero if no pass has ever succeeded.
	LastUpdated time.Time
	// Mode is the discovery mode in effect ("off", "static", or "dns").
	Mode string
}

// DiscoveryPeerView is a single peer entry in the discovery snapshot.
type DiscoveryPeerView struct {
	Hostname    string
	PodIdentity string
	Address     string
}

// NewDiscoverySnapshotFunc is the production adapter that maps a
// discovery.Manager's Snapshot() into the selector's DiscoverySnapshotView.
// It is the narrow composition boundary between the discovery package and
// the bootstrap-source selector, usable by the future Stage 3C orchestrator.
//
// The adapter maps:
//   - discovered peers (discovery.Peer → DiscoveryPeerView);
//   - canonical PodIDs (discovery.Peer.PodIdentity);
//   - dial addresses (discovery.Peer.Address);
//   - local identity (discovery.Snapshot.LocalNode → *DiscoveryPeerView);
//   - expected replica identities (discovery.ExpectedIdentity.Hostname → string);
//   - discovery mode (discovery.Mode → string);
//   - staleness (discovery.Snapshot.Stale);
//   - last successful refresh (discovery.Snapshot.LastUpdated).
func NewDiscoverySnapshotFunc(mgr *discovery.Manager) DiscoverySnapshotFunc {
	return func() DiscoverySnapshotView {
		// A nil manager is a programming error; it must not panic when the
		// callback is invoked. Return an empty, usable view (no peers) so the
		// selector's own validation decides the outcome (no-candidates or
		// discovery-unavailable) rather than crashing the caller.
		if mgr == nil {
			return DiscoverySnapshotView{}
		}
		snap := mgr.Snapshot()
		peers := make([]DiscoveryPeerView, 0, len(snap.Peers))
		for _, p := range snap.Peers {
			peers = append(peers, DiscoveryPeerView{
				Hostname:    p.Hostname,
				PodIdentity: p.PodIdentity,
				Address:     p.Address,
			})
		}
		var localNode *DiscoveryPeerView
		if snap.LocalNode != nil {
			localNode = &DiscoveryPeerView{
				Hostname:    snap.LocalNode.Hostname,
				PodIdentity: snap.LocalNode.PodIdentity,
				Address:     snap.LocalNode.Address,
			}
		}
		expected := make([]string, 0, len(snap.ExpectedIdentities))
		for _, id := range snap.ExpectedIdentities {
			expected = append(expected, id.Hostname)
		}
		return DiscoverySnapshotView{
			Peers:              peers,
			LocalNode:          localNode,
			ExpectedIdentities: expected,
			Stale:              snap.Stale,
			LastUpdated:        snap.LastUpdated,
			Mode:               string(snap.Mode),
		}
	}
}

// Candidate is a normalized, validated bootstrap-source candidate derived
// from a single discovery snapshot. It carries the canonical replica
// identity, the network dial address, and the expected authenticated
// certificate DNS identity.
//
// A candidate is distinct from a raw DNS record: it has been validated
// (expected replica identity, usable address, not the local replica) and
// deduplicated (equivalent identity/address pairs are collapsed).
//
// Multiple addresses for the same expected replica identity are NOT separate
// replicas or independent evidence of data availability. Each distinct
// identity/address pair consumes one candidate-attempt budget entry.
type Candidate struct {
	// PodIdentity is the canonical StatefulSet pod identity (e.g.
	// "gossipgrid-1"). This is the short logical identity.
	PodIdentity string
	// DialAddr is the network address to connect to (e.g.
	// "gossipgrid-1.gossipgrid-headless.default.svc:8443").
	DialAddr string
	// ExpectedIdentity is the expected certificate DNS identity (e.g.
	// "gossipgrid-1.<identitySuffix>"), derived from the pod identity using
	// the configured identity suffix.
	ExpectedIdentity string
}

// SelectorConfig holds the validated configuration for the BootstrapSourceSelector.
type SelectorConfig struct {
	// IdentitySuffix is the configured identity suffix used to derive the
	// expected certificate DNS identity from a pod identity (e.g.
	// "gossipgrid-headless.default.svc"). The expected identity is
	// "<podIdentity>.<IdentitySuffix>" (using the peertls normalization
	// convention: lowercase, trailing dot stripped).
	IdentitySuffix string

	// MaxCandidates is the maximum number of candidate observations a single
	// selection attempt may perform. It must be positive and finite.
	MaxCandidates int

	// SelectionBudget is the total selector-owned wall-clock budget for a
	// single selection attempt. It must be positive and finite. The effective
	// budget is the minimum of this value and the caller's context deadline.
	SelectionBudget time.Duration

	// ObservationBudget is the per-candidate observation deadline. It must be
	// positive and finite. Each candidate observation is bounded by this
	// deadline (derived from the selector's context).
	ObservationBudget time.Duration
}

// Validate checks the selector configuration.
func (c SelectorConfig) Validate() error {
	if c.IdentitySuffix == "" {
		return errors.New("selector: identity suffix must be nonempty")
	}
	// The configured identity suffix must survive normalization: a suffix that
	// normalizes to an empty string (e.g. "." or "  ") would make
	// expectedPeerDNSIdentity produce an incomplete or empty DNS identity,
	// which is not a usable certificate identity.
	if normalizeDNSSuffix(c.IdentitySuffix) == "" {
		return errors.New("selector: identity suffix normalizes to an empty string (not a usable DNS identity)")
	}
	if c.MaxCandidates < 1 {
		return fmt.Errorf("selector: max candidates must be at least 1 (got %d)", c.MaxCandidates)
	}
	if c.MaxCandidates > 32 {
		return fmt.Errorf("selector: max candidates must not exceed 32 (got %d)", c.MaxCandidates)
	}
	if c.SelectionBudget <= 0 {
		return fmt.Errorf("selector: selection budget must be positive (got %v)", c.SelectionBudget)
	}
	if c.ObservationBudget <= 0 {
		return fmt.Errorf("selector: observation budget must be positive (got %v)", c.ObservationBudget)
	}
	if c.ObservationBudget > c.SelectionBudget {
		return fmt.Errorf("selector: observation budget (%v) must not exceed the selection budget (%v)", c.ObservationBudget, c.SelectionBudget)
	}
	return nil
}

// SelectionOutcome is the typed outcome of a selection attempt. It distinguishes
// the different terminal conditions so that future orchestration (Stage 3C)
// can make decisions without parsing error-message strings.
type SelectionOutcome int

// Selection outcomes.
const (
	// OutcomeSelected: an eligible source was found. The result carries the
	// selected candidate and the authenticated observation.
	OutcomeSelected SelectionOutcome = iota

	// OutcomeDiscoveryUnavailable: discovery is disabled or the snapshot is
	// not available (e.g. the manager has not been started, or the snapshot
	// function returned an error).
	OutcomeDiscoveryUnavailable

	// OutcomeDiscoveryStale: the DNS membership is stale (a failed refresh
	// after a prior success). The selector does not use stale DNS membership
	// as authoritative.
	OutcomeDiscoveryStale

	// OutcomeNoInitialRefresh: the DNS snapshot has no successful initial
	// refresh (LastUpdated is zero). It is not authoritative.
	OutcomeNoInitialRefresh

	// OutcomeNoCandidates: the membership is fresh but no usable candidates
	// were found (e.g. the cluster has only the local replica, or all
	// discovered peers are invalid or unexpected).
	OutcomeNoCandidates

	// OutcomeNoEligibleSource: all usable candidates were attempted and
	// rejected. Every distinct identity/address pair in the membership was
	// observed and failed.
	OutcomeNoEligibleSource

	// OutcomeAuthorizationUnavailable: the controller's authorization target
	// could not be established (lifecycle not enabled, no metadata, stale,
	// stopped, etc.).
	OutcomeAuthorizationUnavailable

	// OutcomeAuthorizationChanged: the authorization target changed during the
	// selection (an epoch or cluster change linearized during the observation),
	// or the authorization became unavailable after a target was established.
	OutcomeAuthorizationChanged

	// OutcomeMembershipChanged: the selected candidate was invalidated by a
	// membership change after the observation (the candidate's identity/address
	// pair was removed from the current membership, or the candidate is no
	// longer an expected replica).
	OutcomeMembershipChanged

	// OutcomeBudgetExhausted: the selector-owned total deadline expired, or the
	// candidate-attempt limit was reached while additional usable candidates
	// remain.
	OutcomeBudgetExhausted

	// OutcomeCancelled: the caller's context was cancelled or the caller's
	// deadline expired during the selection.
	OutcomeCancelled
)

// String returns a human-readable name for the outcome.
func (o SelectionOutcome) String() string {
	switch o {
	case OutcomeSelected:
		return "selected"
	case OutcomeDiscoveryUnavailable:
		return "discovery-unavailable"
	case OutcomeDiscoveryStale:
		return "discovery-stale"
	case OutcomeNoInitialRefresh:
		return "no-initial-refresh"
	case OutcomeNoCandidates:
		return "no-candidates"
	case OutcomeNoEligibleSource:
		return "no-eligible-source"
	case OutcomeAuthorizationUnavailable:
		return "authorization-unavailable"
	case OutcomeAuthorizationChanged:
		return "authorization-changed"
	case OutcomeMembershipChanged:
		return "membership-changed"
	case OutcomeBudgetExhausted:
		return "budget-exhausted"
	case OutcomeCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("unknown(%d)", int(o))
	}
}

// CandidateRejection is a bounded diagnostic record of why a candidate was
// rejected. It does not carry sensitive data (credentials, private keys, or
// message contents).
type CandidateRejection struct {
	// Candidate is the rejected candidate.
	Candidate Candidate
	// Reason is a short, stable diagnostic string explaining the rejection.
	Reason string
}

// SelectionResult is the structured result of a selection attempt.
type SelectionResult struct {
	// Outcome is the typed terminal outcome.
	Outcome SelectionOutcome

	// Candidate is the selected candidate (set only when OutcomeSelected).
	Candidate *Candidate

	// Observation is the authenticated observation of the selected source
	// (set only when OutcomeSelected). The observation is valid for its
	// bounded lifetime (MaxSourceObservationTTL) after the selection returns.
	Observation *SourceObservationResult

	// Target is the pinned cluster/epoch target the selection was made for.
	Target RequestContext

	// Rejections records the bounded diagnostic reasons for each rejected
	// candidate (up to MaxCandidates entries). This is for diagnostics only;
	// it does not affect the outcome.
	Rejections []CandidateRejection
}

// BootstrapSourceSelector performs deterministic, bounded bootstrap-source
// selection from the current discovery membership. It is safe for concurrent
// use (each Select call is independent and does not mutate shared state).
//
// The selector does NOT hold any discovery lock during peer network requests:
// it reads the discovery snapshot via the DiscoverySnapshotFunc and processes
// the result without holding any lock.
//
// The selector does NOT create or modify a durable binding, does NOT transition
// the lifecycle state, does NOT transfer records, and does NOT grant
// participation privileges. Selection is advisory: the selected candidate must
// pass the complete AuthorizeJoin authorization sequence (or the Stage 2
// bootstrap-fetch authorization for a SYNCING replica) before any operation
// is performed.
//
// Time budget semantics: the selection relies on context-aware dependencies.
// The total budget and per-candidate budget are enforced through context
// deadlines. A blocking callback that ignores its context (does not select
// on ctx.Done()) is not bounded by the selection's deadline; the selector
// cannot force an uncooperative goroutine to terminate.
type BootstrapSourceSelector struct {
	cfg         SelectorConfig
	snapshotFn  DiscoverySnapshotFunc
	observer    BootstrapSourceObserver
	ctrl        *Controller
	localNodeID string
}

// NewBootstrapSourceSelector creates a BootstrapSourceSelector. The snapshot
// function must be non-nil (it supplies the discovery snapshot); the observer
// must be non-nil (it performs the authenticated source observation); the
// controller must be non-nil (it supplies the authorization target). The
// localNodeID is the local replica's canonical identity (used to exclude
// self-observation). The configuration must be valid.
//
// It returns an error when the configuration is invalid or a required
// dependency is nil.
func NewBootstrapSourceSelector(
	cfg SelectorConfig,
	snapshotFn DiscoverySnapshotFunc,
	observer BootstrapSourceObserver,
	ctrl *Controller,
	localNodeID string,
) (*BootstrapSourceSelector, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if snapshotFn == nil {
		return nil, errors.New("selector: snapshot function must be non-nil")
	}
	if observer == nil {
		return nil, errors.New("selector: observer must be non-nil")
	}
	if ctrl == nil {
		return nil, errors.New("selector: controller must be non-nil")
	}
	return &BootstrapSourceSelector{
		cfg:         cfg,
		snapshotFn:  snapshotFn,
		observer:    observer,
		ctrl:        ctrl,
		localNodeID: localNodeID,
	}, nil
}

// Select performs a single, bounded, deterministic bootstrap-source selection
// attempt. It returns a SelectionResult with a typed outcome.
//
// The deadline and expiration semantics are context-aware: the total budget
// and per-candidate budget are enforced through context deadlines. A
// blocking callback that ignores its context (does not select on ctx.Done())
// is not bounded by the selection's deadline; the selector cannot force an
// uncooperative goroutine to terminate.
//
// The selection sequence:
//  1. Check the caller context. If already done, return OutcomeCancelled.
//  2. Apply the total selection budget as a derived context deadline.
//  3. Read the authorization target from the controller (pinned cluster/epoch).
//     If the target is not available, return OutcomeAuthorizationUnavailable.
//  4. Read the discovery snapshot. Validate it (fresh, not stale, has an
//     initial refresh, discovery enabled, known mode). If invalid, return the
//     appropriate discovery outcome.
//  5. Build the candidate list from the snapshot (membership filtering,
//     address validation, deduplication, deterministic ordering).
//  6. Observe candidates sequentially in the established order. Select the
//     first candidate that passes the complete authenticated source-
//     observation contract. A nominally successful observation that is
//     returned AFTER its individual observation deadline is rejected (a
//     late result is not eligible), and the next candidate is attempted when
//     the caller context and the total budget remain valid.
//  7. Revalidate the membership and the ORIGINAL pinned authorization
//     target after the observation. If the membership changed, the candidate
//     is rejected and the next candidate is attempted when the budgets
//     permit. If the authorization changed or became unavailable, return
//     OutcomeAuthorizationChanged.
//  8. Final recheck: verify the caller context, selector-owned deadline,
//     and observation lifetime as close as practical to the successful
//     return. If any check fails, the candidate is rejected and the next
//     candidate is attempted when the budgets permit (the terminal
//     outcome is determined only after all usable candidates have been
//     exhausted).
//
// Cancellation and budget semantics:
//   - Caller cancellation or caller deadline expiration: OutcomeCancelled
//     (checked at the start, before and after every observation, and before
//     every terminal return; the caller context is checked before the
//     selector-owned deadline so a simultaneous caller cancellation is
//     reported as cancelled, not budget-exhausted).
//   - Selector-owned total deadline expiration: OutcomeBudgetExhausted (an
//     execution-budget outcome, distinct from a candidate being rejected).
//   - Per-candidate observation deadline: the candidate is rejected (a
//     late or timed-out result is not eligible) and the next candidate is
//     attempted when the total budget and caller context permit. An expired
//     observation is NOT conflated with an exhausted execution budget.
//   - Candidate-attempt limit reached while additional usable candidates
//     remain: OutcomeBudgetExhausted.
//   - All usable candidates attempted and rejected: OutcomeNoEligibleSource.
func (s *BootstrapSourceSelector) Select(ctx context.Context) SelectionResult {
	// Preserve the original caller context separately from the derived
	// selection context. The caller context is the sole authority for the
	// OutcomeCancelled outcome; the selector-owned context (derived from the
	// caller with the SelectionBudget timeout) is the sole authority for the
	// OutcomeBudgetExhausted outcome.
	callerCtx := ctx
	selectCtx, cancel := context.WithTimeout(callerCtx, s.cfg.SelectionBudget)
	defer cancel()

	// The pinned target (zero until established) and the bounded rejection
	// record (nil until the first rejection) are declared up front so the
	// terminal helper below can carry them into every outcome.
	var target RequestContext
	var rejections []CandidateRejection

	// terminal returns the appropriate terminal outcome for the current
	// context state, or nil when the selection may continue. The caller
	// context is checked before the selector-owned deadline so that a
	// simultaneous caller cancellation is reported as cancelled rather than
	// budget-exhausted. A cancelled caller always dominates: a caller
	// cancellation is not conflated with an exhausted execution budget.
	terminal := func() *SelectionResult {
		if err := callerCtx.Err(); err != nil {
			return &SelectionResult{Outcome: OutcomeCancelled, Target: target, Rejections: rejections}
		}
		if selectCtx.Err() != nil {
			return &SelectionResult{Outcome: OutcomeBudgetExhausted, Target: target, Rejections: rejections}
		}
		return nil
	}

	// 1. Check the caller context.
	if err := callerCtx.Err(); err != nil {
		return SelectionResult{Outcome: OutcomeCancelled}
	}

	// 3. Read the authorization target (a potentially blocking dependency
	// call: check the contexts before returning the early outcome).
	t, authErr := s.authorizationTarget(callerCtx)
	if authErr != nil {
		if term := terminal(); term != nil {
			return *term
		}
		return SelectionResult{Outcome: OutcomeAuthorizationUnavailable}
	}
	target = t

	// 4. Read and validate the discovery snapshot (a potentially blocking
	// dependency call: check the contexts before returning the early
	// outcome).
	snap := s.snapshotFn()
	if err := s.validateSnapshot(&snap); err != nil {
		if term := terminal(); term != nil {
			return *term
		}
		switch {
		case errors.Is(err, errDiscoveryDisabled):
			return SelectionResult{Outcome: OutcomeDiscoveryUnavailable}
		case errors.Is(err, errDiscoveryStale):
			return SelectionResult{Outcome: OutcomeDiscoveryStale}
		case errors.Is(err, errNoInitialRefresh):
			return SelectionResult{Outcome: OutcomeNoInitialRefresh}
		default:
			return SelectionResult{Outcome: OutcomeDiscoveryUnavailable}
		}
	}

	// 5. Build the candidate list.
	candidates := s.buildCandidates(&snap)
	if len(candidates) == 0 {
		if term := terminal(); term != nil {
			return *term
		}
		return SelectionResult{
			Outcome: OutcomeNoCandidates,
			Target:  target,
		}
	}

	// Preserve the original total candidate count for budget-exhaustion
	// determination (before MaxCandidates truncation).
	totalCandidates := len(candidates)

	// Enforce the candidate-attempt budget: truncate the slice to MaxCandidates.
	if len(candidates) > s.cfg.MaxCandidates {
		candidates = candidates[:s.cfg.MaxCandidates]
	}

	// 6. Observe candidates sequentially in the established order.
	for _, cand := range candidates {
		// Check the caller context and the selector-owned deadline before
		// each observation (a failed or timed-out observation has already
		// advanced both deadlines; the check here bounds the NEXT attempt).
		if term := terminal(); term != nil {
			return *term
		}

		// Per-candidate observation (the observation budget is applied
		// inside observeCandidate, derived from the selector context).
		// observeCandidate checks the observation context's error BEFORE
		// cancelling: a result returned after the individual deadline is
		// rejected as a late result (not eligible).
		obs, obsErr := s.observeCandidate(selectCtx, cand, target)

		// Check the contexts after the observation, including a failed
		// observation: a caller cancellation or an expired total deadline
		// that linearized during the observation dominates over the
		// per-candidate result.
		if term := terminal(); term != nil {
			return *term
		}

		if obsErr != nil {
			rejections = append(rejections, CandidateRejection{
				Candidate: cand,
				Reason:    obsErr.Error(),
			})
			continue
		}

		// Validate the observation against the ORIGINAL target (the shared
		// controller-side observation contract, plus the selector-specific
		// PodID agreement check).
		if valErr := s.validateObservation(obs, target, cand); valErr != nil {
			rejections = append(rejections, CandidateRejection{
				Candidate: cand,
				Reason:    valErr.Error(),
			})
			continue
		}

		// 7. Revalidate the membership and the ORIGINAL pinned
		// authorization target after the observation (as close as practical
		// to the successful return).
		reErr := s.revalidateAfterObservation(callerCtx, cand, target)

		// Check the caller context and the selector-owned deadline
		// immediately after revalidateAfterObservation returns and BEFORE
		// classifying its result: caller cancellation and total-budget
		// expiration must take precedence over membership-changed and
		// authorization-changed outcomes when the corresponding context
		// is already done.
		if term := terminal(); term != nil {
			return *term
		}

		switch {
		case reErr == nil:
			// Membership and authorization hold; proceed to the final recheck.
		case errors.Is(reErr, errAuthorizationChanged):
			// The authorization changed or became unavailable: a terminal
			// outcome (the original pinned authority no longer holds).
			return SelectionResult{
				Outcome:    OutcomeAuthorizationChanged,
				Target:     target,
				Rejections: rejections,
			}
		case errors.Is(reErr, errMembershipChanged):
			// Membership changed (the candidate was removed or invalidated
			// by a membership change, or discovery became unavailable): a
			// terminal outcome (the membership changed; the candidate can
			// no longer be confirmed as present).
			return SelectionResult{
				Outcome:    OutcomeMembershipChanged,
				Target:     target,
				Rejections: rejections,
			}
		case errors.Is(reErr, errBindingReadFailed):
			// A binding-read error during final revalidation: a local
			// authorization failure, NOT evidence that all remote sources
			// are ineligible. Return the applicable typed authorization
			// outcome (OutcomeAuthorizationUnavailable).
			rejections = append(rejections, CandidateRejection{
				Candidate: cand,
				Reason:    reErr.Error(),
			})
			return SelectionResult{
				Outcome:    OutcomeAuthorizationUnavailable,
				Target:     target,
				Rejections: rejections,
			}
		default:
			// An unexpected error: reject this candidate and continue when
			// the budgets permit.
			rejections = append(rejections, CandidateRejection{
				Candidate: cand,
				Reason:    reErr.Error(),
			})
			continue
		}

		// 8. Final recheck, as close as practical to the successful return:
		// caller context, selector-owned deadline, and the observation's
		// bounded lifetime. A failure here rejects the candidate and
		// continues (it is NOT a terminal budget-exhausted outcome; the
		// terminal outcome is determined only after all usable candidates
		// have been exhausted).
		if term := terminal(); term != nil {
			return *term
		}
		if !obs.Valid(s.ctrl.now()) {
			rejections = append(rejections, CandidateRejection{
				Candidate: cand,
				Reason:    "observation expired before selection returned",
			})
			continue
		}

		// Selection succeeded.
		obsCopy := obs
		candCopy := cand
		return SelectionResult{
			Outcome:     OutcomeSelected,
			Candidate:   &candCopy,
			Observation: &obsCopy,
			Target:      target,
			Rejections:  rejections,
		}
	}

	// All attempted candidates were rejected (or none could be attempted).
	// Check the contexts before returning the terminal outcome.
	if term := terminal(); term != nil {
		return *term
	}
	if len(candidates) < totalCandidates {
		// The candidate-attempt limit was reached while additional usable
		// candidates remain: the execution budget (the attempt limit) is
		// exhausted, not the candidate set.
		return SelectionResult{
			Outcome:    OutcomeBudgetExhausted,
			Target:     target,
			Rejections: rejections,
		}
	}
	// Every usable candidate was genuinely attempted and rejected.
	return SelectionResult{
		Outcome:    OutcomeNoEligibleSource,
		Target:     target,
		Rejections: rejections,
	}
}

// authorizationTarget reads the current, pinned bootstrap-selection target
// from the controller using a coherent AuthorizationSnapshot.
//
// For pre-join selection (unbound or incomplete-binding replicas), it requires:
//   - lifecycle enabled;
//   - non-nil binding store and metadata source dependencies;
//   - controller not stopped;
//   - valid, fresh, initialized authoritative metadata;
//   - complete finalized-genesis invariant where applicable;
//   - appropriate local lifecycle state (STARTING, DISCOVERING, or SYNCING);
//   - no conflicting or inappropriate local binding (validated provenance);
//   - a coherent 3-way binding distinction: a valid binding (Binding != nil),
//     an absent binding (Binding == nil, BindingReadError == nil), or a
//     failed binding observation (Binding == nil, BindingReadError != nil —
//     a fail-closed condition, NOT an absent binding).
//
// For an already-SYNCING replica, it reuses the accepted
// AuthorizationSnapshot.bootstrapAuthorized predicate (which requires the
// durable binding).
//
// The binding is read from the coherent AuthorizationSnapshot (not from a
// separate LocalBinding() call), so metadata and binding data belong to the
// same authorization generation. The snapshot's BindingReadError field
// (captured during the existing read-through inside Controller.authorization
// under the established lock order) disambiguates "absent binding" from
// "failed store read" without a supplementary, incoherent second read.
func (s *BootstrapSourceSelector) authorizationTarget(callerCtx context.Context) (RequestContext, error) {
	if s.ctrl == nil {
		return RequestContext{}, errors.New("selector: no controller configured")
	}
	if !s.ctrl.cfg.Enabled {
		return RequestContext{}, errors.New("selector: lifecycle protection is not enabled")
	}
	if err := callerCtx.Err(); err != nil {
		return RequestContext{}, err
	}
	// Reject missing required dependencies before attempting selection.
	// The binding store and metadata source are mandatory for a coherent
	// authorization observation; a nil dependency is a programming error and
	// is rejected fail-closed.
	if s.ctrl.binding == nil {
		return RequestContext{}, errors.New("selector: controller has no binding store configured (required dependency)")
	}
	if s.ctrl.source == nil {
		return RequestContext{}, errors.New("selector: controller has no metadata source configured (required dependency)")
	}

	// Read the coherent authorization snapshot (state, metadata, and binding
	// all captured under one read lock; the binding-read error is captured
	// during the existing read-through inside Controller.authorization).
	snap := s.ctrl.Authorization()

	if snap.Stopped {
		return RequestContext{}, errors.New("selector: controller is stopped")
	}
	if snap.Metadata == nil {
		return RequestContext{}, errors.New("selector: no authoritative metadata observation")
	}
	if snap.AuthorityState != "fresh" || snap.Stale {
		return RequestContext{}, fmt.Errorf("selector: authoritative metadata is not fresh (authority=%s, stale=%v)", snap.AuthorityState, snap.Stale)
	}
	meta := *snap.Metadata
	if vErr := meta.Validate(); vErr != nil {
		return RequestContext{}, fmt.Errorf("selector: authoritative metadata is malformed: %w", vErr)
	}
	if !meta.Initialized {
		return RequestContext{}, errors.New("selector: authoritative metadata reports Initialized=false")
	}
	if snap.SeedIdentity != "" && !snap.finalizationComplete() {
		return RequestContext{}, errors.New("selector: metadata is not a completely finalized record for the designated seed")
	}

	// The current lifecycle state must permit source selection.
	switch snap.State {
	case STARTING, DISCOVERING:
		// Pre-join: no durable binding, or a matching join-provenance binding.
		// The snapshot provides a coherent 3-way binding distinction:
		//   - Binding != nil: a valid binding was observed; validate it.
		//   - Binding == nil, BindingReadError == nil: an absent binding was
		//     successfully observed (the database is genuinely unbound).
		//   - Binding == nil, BindingReadError != nil: the binding observation
		//     failed (a fail-closed condition, NOT an absent binding).
		if snap.Binding != nil {
			b := *snap.Binding
			if vErr := b.Validate(); vErr != nil {
				return RequestContext{}, fmt.Errorf("selector: local binding is malformed: %w", vErr)
			}
			// Positive provenance allowlist: only join provenance is
			// permitted for pre-join selection. Genesis provenance belongs
			// to the seed; legacy and out-of-range values fail closed.
			if !b.Provenance.isRecognized() {
				return RequestContext{}, fmt.Errorf("selector: local binding has unrecognized provenance %s; fail-closed", b.Provenance)
			}
			if b.Provenance == ProvenanceGenesis {
				return RequestContext{}, errors.New("selector: local binding has genesis provenance; pre-join source selection requires join provenance or no binding")
			}
			if !b.MatchesMetadata(meta) {
				return RequestContext{}, fmt.Errorf("selector: local binding (%s/%s) conflicts with the authorized epoch (%s/%s)", b.ClusterID, b.EpochID, meta.ClusterID, meta.EpochID)
			}
		} else if snap.BindingReadError != nil {
			// The binding observation failed: a fail-closed, distinct
			// condition. A binding-read error is NOT evidence that the
			// database is unbound; it must not be conflated with an
			// absent binding.
			return RequestContext{}, fmt.Errorf("%w: %v", errBindingReadFailed, snap.BindingReadError)
		}
		// else: Binding == nil, BindingReadError == nil — genuinely unbound; proceed.
	case SYNCING:
		// Already-SYNCING: reuse the accepted bootstrapAuthorized predicate.
		// bootstrapAuthorized requires the durable binding to exist and match;
		// a binding-read error that produces a nil binding is therefore
		// rejected by bootstrapAuthorized (fail-closed).
		if !snap.bootstrapAuthorized() {
			return RequestContext{}, errors.New("selector: " + snap.bootstrapAuthorizationReason())
		}
	default:
		return RequestContext{}, fmt.Errorf("selector: lifecycle state %s does not permit bootstrap-source selection (only STARTING, DISCOVERING, or SYNCING are permitted)", snap.State)
	}

	return RequestContext{ClusterID: meta.ClusterID, EpochID: meta.EpochID}, nil
}

// validateSnapshot checks the discovery snapshot for validity. It returns an
// error (one of the typed sentinel errors) when the snapshot is not usable.
func (s *BootstrapSourceSelector) validateSnapshot(snap *DiscoverySnapshotView) error {
	// Validate the discovery mode explicitly. Unknown modes must not be
	// accepted as usable discovery.
	switch snap.Mode {
	case "off":
		return errDiscoveryDisabled
	case "static", "dns":
		// Valid modes; continue.
	default:
		return fmt.Errorf("%w: unknown discovery mode %q", errDiscoveryDisabled, snap.Mode)
	}

	// Stale DNS membership.
	if snap.Stale && snap.Mode == "dns" {
		return errDiscoveryStale
	}

	// DNS membership before a successful initial refresh.
	if snap.Mode == "dns" && snap.LastUpdated.IsZero() {
		return errNoInitialRefresh
	}

	return nil
}

// buildCandidates normalizes the discovery snapshot into an ordered list of
// validated candidates. It applies the membership filtering rules:
//  1. Validate and normalize candidate identities and dial addresses.
//  2. Exclude local and unexpected replica identities.
//  3. Deduplicate equivalent identity/address pairs.
//  4. Sort by canonical replica identity and then normalized address.
//  5. Preserve distinct usable addresses for the same expected identity as
//     deterministic fallback candidates.
//
// Each distinct identity/address pair consumes one candidate-attempt budget
// entry. Multiple addresses for one replica are NOT separate replicas or
// independent evidence of data availability.
//
// The original snapshot is not mutated.
func (s *BootstrapSourceSelector) buildCandidates(snap *DiscoverySnapshotView) []Candidate {
	// Build the set of expected replica identities.
	expectedSet := make(map[string]bool, len(snap.ExpectedIdentities))
	for _, id := range snap.ExpectedIdentities {
		expectedSet[id] = true
	}

	// Determine the local node's identity for exclusion.
	localID := ""
	if snap.LocalNode != nil {
		localID = snap.LocalNode.PodIdentity
	}

	// Deduplicate equivalent identity/address pairs while preserving distinct
	// addresses for the same identity.
	type pairKey struct {
		podIdentity string
		dialAddr    string
	}
	seen := make(map[pairKey]bool, len(snap.Peers))

	var candidates []Candidate
	for _, p := range snap.Peers {
		peerID := strings.TrimSpace(p.PodIdentity)
		if peerID == "" {
			continue
		}
		// Exclude the local replica.
		if peerID == localID || peerID == s.localNodeID {
			continue
		}
		// Consider only expected replica identities.
		if !expectedSet[peerID] {
			continue
		}
		// Validate and normalize the dial address (host:port with nonempty
		// host, valid port, DNS-name normalization for the host). The
		// same normalization is used in revalidateAfterObservation, so an
		// address that is valid in one is valid in the other.
		normAddr, normErr := normalizeDialAddr(p.Address)
		if normErr != nil {
			continue
		}
		// Deduplicate equivalent identity/address pairs (using the
		// normalized address).
		key := pairKey{podIdentity: peerID, dialAddr: normAddr}
		if seen[key] {
			continue
		}
		seen[key] = true

		candidates = append(candidates, Candidate{
			PodIdentity:      peerID,
			DialAddr:         normAddr,
			ExpectedIdentity: expectedPeerDNSIdentity(peerID, s.cfg.IdentitySuffix),
		})
	}

	// Deterministic ordering: by canonical replica identity (lexicographic),
	// then by normalized dial address. This preserves distinct addresses for
	// the same identity in a stable order (deterministic fallback candidates).
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].PodIdentity != candidates[j].PodIdentity {
			return candidates[i].PodIdentity < candidates[j].PodIdentity
		}
		return candidates[i].DialAddr < candidates[j].DialAddr
	})

	return candidates
}

// normalizeDialAddr validates and normalizes a dial address (host:port) for
// candidate construction and membership revalidation. It applies the same
// normalization convention for both construction and revalidation, so an
// address that is valid in one is valid in the other.
//
// The rules:
//   - the address must be a valid host:port (net.SplitHostPort);
//   - the host must be nonempty after trimming;
//   - the host must contain no whitespace (a whitespace-padded host is
//     malformed; TrimSpace on the full address would mask an invalid
//     internal space);
//   - the port must be a valid unsigned integer in the range 1-65535;
//   - a leading/trailing space on the full address is trimmed before
//     splitting (a conservative tolerance for DNS resolution output);
//   - the host is validated: a numeric IP address is parsed with
//     net.ParseIP (canonical form preserved; equivalent representations
//     normalized); a DNS hostname is validated as a syntactically valid
//     hostname (lowercase, no path characters, no invalid labels) without
//     performing DNS resolution;
//   - malformed hosts (path-containing strings, invalid IPv6 literals,
//     invalid DNS labels) are rejected.
//
// The returned string is the canonical, normalized form (e.g.
// "example.com:8443", "[::1]:8443", "127.0.0.1:443").
func normalizeDialAddr(addr string) (string, error) {
	// Trim leading/trailing whitespace on the full address (a conservative
	// tolerance; internal whitespace is still rejected).
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", errors.New("dial address is empty")
	}
	// Reject internal whitespace: a host or port containing a space is
	// malformed. (TrimSpace only handles leading/trailing; an internal
	// space would survive and produce a malformed host or port.)
	if strings.ContainsAny(addr, " \t\n\r") {
		return "", fmt.Errorf("dial address %q contains whitespace", addr)
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("dial address %q is not a valid host:port: %w", addr, err)
	}
	if host == "" {
		return "", fmt.Errorf("dial address %q has an empty host", addr)
	}

	// Validate the port: must be a valid unsigned integer in 1-65535.
	portNum, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return "", fmt.Errorf("dial address %q has an invalid port %q: %w", addr, port, err)
	}
	if portNum < 1 || portNum > 65535 {
		return "", fmt.Errorf("dial address %q has a port %q out of range (1-65535)", addr, port)
	}

	// Validate and normalize the host.
	normalizedHost, err := validateAndNormalizeHost(host)
	if err != nil {
		return "", fmt.Errorf("dial address %q: %w", addr, err)
	}
	return normalizedHost + ":" + strconv.FormatUint(portNum, 10), nil
}

// validateAndNormalizeHost validates and normalizes the host component of a
// dial address. It distinguishes numeric IP addresses from DNS hostnames:
//
//   - numeric IP addresses: parsed with net.ParseIP; the canonical string
//     form is used (net.ParseIP normalizes equivalent representations,
//     e.g. "010.001.000.001" → "10.1.0.1", "0:0:0:0:0:ffff:1:1" →
//     "::ffff:1:1"). IPv6 addresses are re-wrapped in brackets for the
//     dial-address form.
//   - DNS hostnames: validated as a syntactically valid hostname (lowercase,
//     no path characters like '/' or '\\', no invalid labels like leading
//     hyphens or over-length labels) without performing DNS resolution.
//     The trailing dot is stripped (matching the peertls normalizeDNSName
//     convention).
//
// Malformed hosts (path-containing strings, invalid IPv6 literals, invalid
// DNS labels, empty labels) are rejected.
func validateAndNormalizeHost(host string) (string, error) {
	// Try numeric IP first. net.ParseIP handles both IPv4 and IPv6.
	if ip := net.ParseIP(host); ip != nil {
		// net.ParseIP returns the canonical form:
		//   - IPv4: dotted-decimal (e.g. "10.1.0.1");
		//   - IPv6: compressed lowercase (e.g. "::1", "::ffff:1:1").
		canon := ip.String()
		if ip.To4() != nil {
			// IPv4 (or IPv4-mapped IPv6): no brackets needed.
			return canon, nil
		}
		// IPv6: re-wrap in brackets for the dial-address form.
		return "[" + canon + "]", nil
	}

	// Not a numeric IP: treat as a DNS hostname and validate its syntax.
	// Lowercase and strip a trailing dot (matching the peertls
	// normalizeDNSName convention).
	normalized := strings.ToLower(strings.TrimSpace(host))
	normalized = strings.TrimSuffix(normalized, ".")
	if normalized == "" {
		return "", errors.New("host normalizes to an empty string")
	}
	// Reject path characters (a hostname must not contain '/', '\\', or
	// other URL path characters).
	if strings.ContainsAny(normalized, "/\\") {
		return "", fmt.Errorf("host %q contains path characters", host)
	}
	// Reject empty labels (a hostname must not contain consecutive dots,
	// or start/end with a dot after normalization).
	if strings.HasPrefix(normalized, ".") || strings.HasSuffix(normalized, ".") || strings.Contains(normalized, "..") {
		return "", fmt.Errorf("host %q has empty labels", host)
	}
	// Validate each label: 1-63 characters, alphanumeric or hyphens,
	// must not start or end with a hyphen.
	for _, label := range strings.Split(normalized, ".") {
		if len(label) == 0 || len(label) > 63 {
			return "", fmt.Errorf("host %q has an invalid label (length %d)", host, len(label))
		}
		for i, c := range label {
			isAlnum := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
			isHyphen := c == '-'
			if !isAlnum && !isHyphen {
				return "", fmt.Errorf("host %q has an invalid character %q in label %q", host, c, label)
			}
			if i == 0 && c == '-' {
				return "", fmt.Errorf("host %q label %q starts with a hyphen", host, label)
			}
			if i == len(label)-1 && c == '-' {
				return "", fmt.Errorf("host %q label %q ends with a hyphen", host, label)
			}
		}
	}
	return normalized, nil
}

// expectedPeerDNSIdentity derives the expected certificate DNS identity from
// a pod identity and the configured suffix. It reuses the peertls package's
// normalization convention (lowercase, trailing dot stripped) via a local
// helper to respect package boundaries.
func expectedPeerDNSIdentity(podID, suffix string) string {
	return podID + "." + normalizeDNSSuffix(suffix)
}

// normalizeDNSSuffix lowercases and strips a trailing dot from the identity
// suffix, matching the peertls package's normalizeDNSName convention.
func normalizeDNSSuffix(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimSuffix(s, ".")
	return s
}

// observeCandidate performs a single authenticated observation of the given
// candidate. It uses the trusted observer boundary (BootstrapSourceObserver)
// and applies the per-candidate observation budget.
//
// The per-candidate observation context is derived from the selector context
// with the ObservationBudget timeout. After Observe returns, the observation
// context's error is checked BEFORE cancelling: a result returned after the
// individual deadline (obsCtx.Err() == context.DeadlineExceeded) is rejected
// as a late result (not eligible). The observation and error are returned
// WITHOUT returning the cancelled context (the caller cannot use a cancelled
// context to classify the result; the late-result rejection is based on the
// pre-cancellation error check).
func (s *BootstrapSourceSelector) observeCandidate(ctx context.Context, cand Candidate, target RequestContext) (SourceObservationResult, error) {
	// Apply the per-candidate observation budget.
	obsCtx, cancel := context.WithTimeout(ctx, s.cfg.ObservationBudget)

	obs, obsErr := s.observer.Observe(obsCtx, cand.DialAddr, cand.ExpectedIdentity, target, s.localNodeID)

	// Check the observation context's error BEFORE cancelling: a result
	// returned after the individual deadline is rejected as a late result.
	// The check is performed before cancel so that a result returned exactly
	// AT the deadline (without exceeding it) is still eligible.
	late := obsCtx.Err() == context.DeadlineExceeded
	cancel()

	if late && obsErr == nil {
		// The observer returned a nominally valid result AFTER its
		// individual observation deadline: a late result is NOT eligible.
		return obs, errors.New("observation returned after its individual deadline")
	}
	return obs, obsErr
}

// validateObservation performs the complete, strict, fail-closed validation
// of an authenticated observation against the pinned target. It reuses the
// shared controller-side observation validator (validateSourceObservation)
// for the common checks, then applies the selector-specific PodID
// agreement check (the peer's self-reported node ID must match the
// selected candidate's canonical PodID).
//
// The shared validator is called with a metadata view whose cluster/epoch
// match the ORIGINAL pinned target (not the current metadata), so a metadata
// change is not misreported as an observation validation failure. The
// shared validator's self-exclusion check uses the controller's configured
// NodeID; the selector applies the same check through the shared validator
// (s.ctrl.cfg.NodeID == s.localNodeID by construction).
//
// The complete checks:
//   - observation validity (bounded lifetime, not future-dated);
//   - eligible lifecycle state (READY or DEGRADED);
//   - BootstrapSourceEligible and readiness;
//   - reported cluster/epoch agreement with the original target;
//   - peer identity non-empty;
//   - local self-exclusion;
//   - pinned target agreement;
//   - authenticated peer identity agreement with the expected identity;
//   - selected canonical PodID agreement (selector-specific).
func (s *BootstrapSourceSelector) validateObservation(obs SourceObservationResult, target RequestContext, cand Candidate) error {
	// The metadata view for the shared validator carries the ORIGINAL pinned
	// target's cluster/epoch (not the current metadata), so the shared
	// validator's cluster/epoch check validates against the original target.
	// The remaining metadata fields (Initialized, Generation, etc.) are zero;
	// they are not used by the shared validator.
	meta := ClusterMetadata{
		ClusterID: target.ClusterID,
		EpochID:   target.EpochID,
	}
	if err := validateSourceObservation(s.ctrl, obs, &meta, target, cand.ExpectedIdentity); err != nil {
		return err
	}
	// Selector-specific check: the peer's self-reported node ID must match
	// the selected candidate's canonical PodID (the controller-side
	// validator does not have access to the candidate).
	if obs.PeerNodeID() != cand.PodIdentity {
		return fmt.Errorf("source observation: peer reported PodID %q does not match the selected candidate %q", obs.PeerNodeID(), cand.PodIdentity)
	}
	return nil
}

// validateSourceObservation is the shared, strict, fail-closed validation of
// an authenticated source observation. It is the single consistent
// observation contract used by both the controller's join path
// (Controller.validateSourceObservationResult) and the bootstrap-source
// selector (BootstrapSourceSelector.validateObservation). The
// expectedIdentity parameter is the authenticated peer identity the
// observation was requested for; it must match the observation's
// authenticated identity.
//
// The checks include:
//   - observation validity (bounded lifetime, not future-dated);
//   - eligible lifecycle state (READY or DEGRADED);
//   - BootstrapSourceEligible and readiness;
//   - reported cluster/epoch agreement with the target metadata;
//   - peer identity non-empty;
//   - local self-exclusion (the peer must not be the local replica);
//   - pinned target agreement (the observation's recorded target must match
//     the requested target);
//   - authenticated peer identity agreement with the expected identity.
//
// The temporal policy (future timestamps, invalid or expired lifetimes, the
// maximum lifetime clamp) is enforced in SourceObservationResult.Valid.
func validateSourceObservation(c *Controller, r SourceObservationResult, meta *ClusterMetadata, target RequestContext, expectedIdentity string) error {
	if meta == nil {
		return errors.New("source observation: no target metadata to validate against")
	}
	if !r.Valid(c.now()) {
		return fmt.Errorf("source observation is not valid or is outside its bounded lifetime (peer=%s)", r.peerIdentity)
	}
	if r.lifecycleState != "READY" && r.lifecycleState != "DEGRADED" {
		return fmt.Errorf("source observation: peer %q reported state %q (must be READY or DEGRADED to be an eligible bootstrap source)", r.peerNodeID, r.lifecycleState)
	}
	if !r.bootstrapSourceEligible {
		return fmt.Errorf("source observation: peer %q does not report bootstrap source eligibility", r.peerNodeID)
	}
	if !r.readiness {
		return fmt.Errorf("source observation: peer %q reported readiness=false (inconsistent with an eligible READY/DEGRADED source)", r.peerNodeID)
	}
	if r.clusterID != meta.ClusterID || r.epochID != meta.EpochID {
		return fmt.Errorf("source observation: peer %q reported cluster/epoch %s/%s does not match the target %s/%s", r.peerNodeID, r.clusterID, r.epochID, meta.ClusterID, meta.EpochID)
	}
	if r.peerIdentity == "" || r.peerNodeID == "" {
		return errors.New("source observation: peer identity is missing (authenticated identity or self-reported node ID is empty)")
	}
	if c.cfg.NodeID != "" && r.peerNodeID == c.cfg.NodeID {
		return fmt.Errorf("source observation: the observed peer %q is the local replica; self-observation is not a bootstrap source", r.peerNodeID)
	}
	if r.expected.ClusterID != target.ClusterID || r.expected.EpochID != target.EpochID {
		return fmt.Errorf("source observation: pinned target %s/%s does not match the requested target %s/%s", r.expected.ClusterID, r.expected.EpochID, target.ClusterID, target.EpochID)
	}
	if r.peerIdentity != expectedIdentity {
		return fmt.Errorf("source observation: authenticated peer identity %q does not match the expected identity %q", r.peerIdentity, expectedIdentity)
	}
	return nil
}

// revalidateAfterObservation revalidates the membership and the ORIGINAL
// pinned authorization after the network observation. The order matters:
// membership is checked FIRST (a failed store read must not be misreported
// as an authorization change), then the pinned authorization is
// revalidated (a changed or unavailable authorization is a terminal
// condition), then the binding's 3-way state is checked (a binding-read
// error is a distinct, fail-closed condition for pre-join selection).
//
// It checks:
//   - a fresh coherent discovery snapshot is available and usable;
//   - the selected canonical identity remains an expected replica;
//   - the selected normalized identity/address pair remains present and
//     usable in the current membership;
//   - the selected identity is not now identified as local;
//   - the authorization target has not changed (cluster/epoch match);
//   - the authorization has not become unavailable;
//   - the local binding's 3-way state permits selection (a binding-read
//     error is rejected fail-closed during pre-join selection).
//
// A successful refresh that preserves the candidate does NOT invalidate the
// result solely because LastUpdated changed.
//
// It returns errAuthorizationChanged when the cluster/epoch changed or the
// authorization became unavailable, errMembershipChanged when the candidate
// was removed or invalidated by a membership change, or
// errBindingReadFailed when the local binding store read failed during
// pre-join revalidation.
func (s *BootstrapSourceSelector) revalidateAfterObservation(ctx context.Context, cand Candidate, target RequestContext) error {
	// 1. MEMBERSHIP FIRST: obtain a fresh coherent discovery snapshot.
	freshSnap := s.snapshotFn()

	// Validate the fresh snapshot's mode, freshness, and initial-refresh.
	if err := s.validateSnapshot(&freshSnap); err != nil {
		// Discovery became disabled or unavailable: the candidate can no
		// longer be confirmed as present (membership-changed).
		return errMembershipChanged
	}

	// Verify the selected identity is not now identified as local.
	if freshSnap.LocalNode != nil && freshSnap.LocalNode.PodIdentity == cand.PodIdentity {
		return errMembershipChanged
	}

	// Build the set of expected replica identities from the fresh snapshot.
	expectedSet := make(map[string]bool, len(freshSnap.ExpectedIdentities))
	for _, id := range freshSnap.ExpectedIdentities {
		expectedSet[id] = true
	}
	if !expectedSet[cand.PodIdentity] {
		return errMembershipChanged
	}

	// Verify the selected normalized identity/address pair remains present
	// and usable in the current membership. The address is normalized
	// (same convention as buildCandidates) before comparison.
	normAddr, normErr := normalizeDialAddr(cand.DialAddr)
	if normErr != nil {
		// The candidate's dial address is malformed: it should have been
		// rejected in buildCandidates; this is a defensive check.
		return errMembershipChanged
	}
	found := false
	for _, p := range freshSnap.Peers {
		if p.PodIdentity != cand.PodIdentity {
			continue
		}
		pAddr, pErr := normalizeDialAddr(p.Address)
		if pErr != nil {
			continue
		}
		if pAddr == normAddr {
			found = true
			break
		}
	}
	if !found {
		return errMembershipChanged
	}

	// 2. Pinned AUTHORIZATION AFTER membership: revalidate the authorization
	// target. A failed store read (errBindingReadFailed) is a local
	// authorization failure (the binding observation failed), NOT evidence
	// that all remote sources are ineligible and NOT an authorization change.
	// It is returned as a distinct, fail-closed condition that the caller
	// maps to a typed authorization outcome.
	newTarget, authErr := s.authorizationTarget(ctx)
	if authErr != nil {
		if errors.Is(authErr, errBindingReadFailed) {
			// A binding-read error during final revalidation: a local
			// authorization failure. The caller maps this to
			// OutcomeAuthorizationUnavailable (not OutcomeNoEligibleSource).
			return errBindingReadFailed
		}
		// Authorization changed or became unavailable: a terminal
		// condition (the original pinned authority no longer holds).
		return errAuthorizationChanged
	}
	if newTarget.ClusterID != target.ClusterID || newTarget.EpochID != target.EpochID {
		return errAuthorizationChanged
	}

	return nil
}

// Typed sentinel errors for snapshot validation and revalidation.
var (
	errDiscoveryDisabled    = errors.New("discovery disabled or unavailable")
	errDiscoveryStale       = errors.New("discovery membership is stale")
	errNoInitialRefresh     = errors.New("no successful initial discovery refresh")
	errAuthorizationChanged = errors.New("authorization target changed during selection")
	errMembershipChanged    = errors.New("membership changed during selection")
	errBindingReadFailed    = errors.New("local binding store read failed (fail-closed; a binding-read error is not an absent binding)")
)
