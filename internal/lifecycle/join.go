// Package lifecycle: ordinary replica joining (Milestone 5B, Stage 3A).
//
// AuthorizeJoin is the narrow, explicitly invoked controller operation for
// beginning an ordinary replica join to an already-initialized cluster epoch.
// It is distinct from AuthorizeGenesis (which is for the one-time,
// operator-authorized first-epoch establishment). A replica in an
// already-initialized epoch that needs to join does so through AuthorizeJoin,
// which creates a join-provenance binding and transitions the replica to
// SYNCING through the legal state-machine path.
//
// The joining operation does NOT automatically select a source peer, schedule
// a bootstrap sweep, or promote the replica to READY (those belong to Stage 3B
// through 3D). The joining mechanism is a controlled, explicitly invoked
// operation that establishes the local replica's authorized epoch membership
// and places it in the correct lifecycle state for subsequent synchronization.
//
// Source evidence is obtained through the BootstrapSourceObserver interface
// (the narrow, trusted, dependency-injected boundary in join_observer.go),
// NOT through a publicly constructible, caller-asserted value. In production
// the observer performs a real mTLS-validated peer observation of a single,
// explicitly supplied candidate; the controller revalidates the result
// fail-closed before using it.

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// AuthorizeJoin is the explicit, narrowly scoped controller operation for
// beginning an ordinary replica join to an already-initialized cluster epoch.
// It is the production path by which a replica with an EMPTY, UNBOUND local
// database (a fresh or replacement pod) that is NOT the designated seed
// (or a replacement seed after the epoch is already initialized) binds the
// database to the authorized epoch with JOIN provenance and transitions to
// SYNCING through the legal state-machine path.
//
// It is DISTINCT from AuthorizeGenesis:
//   - AuthorizeGenesis is for the one-time, operator-authorized first-epoch
//     establishment (the designated seed only, GENESIS state, genesis
//     provenance).
//   - AuthorizeJoin is for ordinary replicas joining an already-initialized
//     epoch (any replica, SYNCING state, join provenance). A replacement
//     canonical seed follows the ordinary joining contract once the epoch
//     has been initialized; its ordinal does not confer genesis privileges.
//
// The authorization is GATED, fail-closed. The complete authorization
// sequence is:
//
//  1. PRE-BINDING VALIDATION (no I/O, no locks held): the caller context is
//     live, the lifecycle is enabled, the controller is not stopped, the
//     current accepted metadata is present, valid, fresh, not stale, reports
//     an initialized epoch, and (for a genesis-managed configuration)
//     satisfies the complete finalized-genesis invariant. The current
//     lifecycle state permits joining (STARTING, DISCOVERING, or SYNCING).
//     The local database has no conflicting or malformed binding.
//
//  2. SOURCE OBSERVATION (network I/O, no controller/machine lock held):
//     the trusted observer performs an authenticated, mTLS-validated
//     observation of the single supplied candidate. The result is revalidated
//     (bounded lifetime, complete source evidence, consistency with the
//     current metadata) before proceeding.
//
//  3. DURABLE BIND (SQLite I/O, no controller/machine lock held): the
//     atomic, idempotent, context-aware Bind with JOIN provenance. The store
//     transaction is authoritative for the atomic empty-database requirement.
//
//  4. POST-BIND AUTHORITATIVE RE-READ: the authoritative metadata is
//     re-read directly (not the pre-guard snapshot). The re-read observation
//     is validated (valid, complete finalized-genesis invariant where
//     applicable, matching pinned cluster/epoch). This re-read does NOT
//     overwrite newer controller-accepted metadata; it is used only to
//     confirm the authorization was not revoked between the pre-guard
//     snapshot and the Bind.
//
//  5. FINAL LOCAL GUARD (c.mu.Lock + m.mu.Lock, no I/O): the complete
//     authorization is revalidated under the held lock pair: caller context
//     live, controller not stopped, controller context live, current accepted
//     metadata present/valid/fresh, initialized epoch, finalized-genesis
//     invariant, pinned target match, durable binding match, binding
//     provenance join (not genesis, not legacy/unknown), complete source
//     observation evidence, legal transition into SYNCING. Only when all
//     checks pass does the method warm the binding cache and transition.
//
// If the durable Bind commits but the final guard rejects (for example, the
// controller was stopped, the context was cancelled, the authoritative
// metadata changed, or the state moved concurrently between the pre-guard
// read and the guard), the method returns an error and the durable binding
// is PRESERVED: a committed binding is never rolled back, and a subsequent
// AuthorizeJoin call with a restored authorization will find the matching
// join-provenance binding and succeed idempotently. The binding and local
// messages are never deleted or modified on a join rejection.
//
// If prerequisites were already invalid before the binding (a pre-binding
// validation failure), no binding is created: the operation rejects before
// any durable write.
//
// Lock-order contract: the method performs pre-binding validation and the
// durable Bind (no controller/machine lock held across the network or
// SQLite write), then acquires c.mu.Lock followed by m.mu.Lock (the
// established write-lock order) and performs the final guard and transition
// with no I/O, no network calls, and no SQLite writes inside the critical
// section. The Bind is the only operation that may block on the database;
// it is not held across any network operation, and no lock is held across
// the Bind.
//
// A successful joining operation:
//   - creates or reuses a durable same-epoch binding with JOIN provenance;
//   - revalidates current authorization through the final guard;
//   - transitions through legal lifecycle states into SYNCING.
//
// It does NOT:
//   - transition to GENESIS;
//   - transition to READY;
//   - enable ordinary replication;
//   - enable public writes;
//   - enable bootstrap-source eligibility;
//   - consume or modify operator genesis authorization;
//   - modify authoritative cluster metadata;
//   - use a forced state transition that bypasses the legal transition
//     table.
func (c *Controller) AuthorizeJoin(ctx context.Context, observer BootstrapSourceObserver, dialAddr, expectedIdentity string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !c.cfg.Enabled {
		return errors.New("cannot authorize join: lifecycle protection is not enabled")
	}
	if observer == nil {
		return errors.New("cannot authorize join: no bootstrap source observer supplied (source evidence must be obtained through the trusted observer boundary)")
	}

	// 1. PRE-BINDING VALIDATION (no I/O, no locks held).
	snap := c.authorization()
	if snap.Stopped {
		return errors.New("cannot authorize join: controller is stopped")
	}
	if snap.Metadata == nil {
		return errors.New("cannot authorize join: no authoritative metadata observation")
	}
	if snap.AuthorityState != "fresh" || snap.Stale {
		return fmt.Errorf("cannot authorize join: authoritative metadata is not fresh (authority=%s, stale=%v)", snap.AuthorityState, snap.Stale)
	}
	meta := *snap.Metadata
	if vErr := meta.Validate(); vErr != nil {
		return fmt.Errorf("cannot authorize join: authoritative metadata is malformed: %w", vErr)
	}
	if !meta.Initialized {
		return errors.New("cannot authorize join: authoritative metadata reports Initialized=false (the cluster must be initialized before joining)")
	}
	if c.cfg.SeedIdentity != "" && !c.finalizationComplete(&meta) {
		return fmt.Errorf("cannot authorize join: authoritative metadata is not a completely finalized record for the designated seed (Initialized=%v GenesisAuthorized=%v GenesisConsumer=%q DataLossProtected=%v)",
			meta.Initialized, meta.GenesisAuthorized, meta.GenesisConsumer, meta.DataLossProtected)
	}

	// The current lifecycle state must permit joining.
	if !snap.State.permitsJoinTransition() {
		return fmt.Errorf("cannot authorize join: lifecycle state %s does not permit a join transition (only STARTING, DISCOVERING, or SYNCING are permitted)", snap.State)
	}

	// The local database must have no conflicting or malformed binding.
	binding := c.LocalBinding()
	if binding != nil {
		if binding.Provenance == ProvenanceGenesis {
			return errors.New("cannot authorize join: existing local binding has genesis provenance (the original seed's binding); the join path requires join provenance")
		}
		if binding.Provenance == ProvenanceLegacy {
			return errors.New("cannot authorize join: existing local binding has explicitly-unknown (legacy) provenance (a fail-closed historical binding does not authorize joining)")
		}
		if !binding.MatchesMetadata(meta) {
			return fmt.Errorf("cannot authorize join: existing local binding (%s/%s) conflicts with the authorized epoch (%s/%s); fail-closed, the binding is preserved",
				binding.ClusterID, binding.EpochID, meta.ClusterID, meta.EpochID)
		}
	}

	// Reject missing required dependencies without panicking (the durable
	// binding store and the authoritative metadata source are required for
	// joining; a nil dependency is a programming error and is rejected
	// fail-closed rather than causing a nil-pointer panic).
	if c.binding == nil {
		return errors.New("cannot authorize join: no binding store configured (the durable binding store is a required dependency)")
	}
	if c.source == nil {
		return errors.New("cannot authorize join: no metadata source configured (the authoritative metadata source is a required dependency)")
	}

	// Pin the join target from the authoritative metadata.
	target := RequestContext{ClusterID: meta.ClusterID, EpochID: meta.EpochID}

	// 2. SOURCE OBSERVATION (network I/O, no controller/machine lock held).
	obs, obsErr := observer.Observe(ctx, dialAddr, expectedIdentity, target, c.cfg.NodeID)
	if obsErr != nil {
		return fmt.Errorf("cannot authorize join: bootstrap source observation failed: %w", obsErr)
	}
	if !obs.Observed() {
		return errors.New("cannot authorize join: bootstrap source observation reported no eligible source")
	}
	// Validate the complete observation against the pre-observation
	// snapshot (a cheap, early fail-closed gate: an observation that is
	// structurally invalid, outside its bounded lifetime, or inconsistent
	// with the snapshot is rejected before the revalidation below). The
	// authoritative revalidation (against the CURRENT accepted authority)
	// is performed in revalidateJoinPreBind after the observation.
	if obsErr := c.validateSourceObservationResult(obs, &meta, target, expectedIdentity); obsErr != nil {
		return fmt.Errorf("cannot authorize join: %w", obsErr)
	}
	// Revalidate the complete observation against the CURRENT accepted
	// authority (NOT the pre-observation snapshot). The observation is
	// network I/O: the controller's accepted metadata, authority state,
	// or lifecycle state may have changed during it (an authority change
	// accepted by the metadata worker, a concurrent state change, a stop).
	// The revalidation reads a FRESH, coherent authorization snapshot and
	// rejects fail-closed when the authorization that the join was based
	// on no longer holds. This is a fail-closed gate before any durable
	// write: a validly initiated join whose authorization has changed is
	// rejected before Bind, so no binding is created. This is NOT a
	// distributed atomic transaction: a change that linearizes after this
	// re-read (between the re-read and the final guard) is detected by the
	// final guard (the post-Bind authoritative re-read and the final local
	// guard, which remain authoritative).
	if reErr := c.revalidateJoinPreBind(ctx, obs, expectedIdentity, target); reErr != nil {
		return fmt.Errorf("cannot authorize join: %w", reErr)
	}

	// 3. DURABLE BIND (SQLite I/O, no controller/machine lock held).
	b, bindErr := c.binding.Bind(ctx, meta.ClusterID, meta.EpochID, meta.Generation, ProvenanceJoin)
	if bindErr != nil {
		return fmt.Errorf("cannot authorize join: binding local database to authorized epoch: %w", bindErr)
	}
	if b == nil {
		return errors.New("cannot authorize join: binding transaction returned a nil result (fail-closed; the database is unchanged)")
	}
	// Validate the actual returned binding (an idempotent Bind can return
	// the winner of a concurrent binding operation with different provenance).
	if vErr := b.Validate(); vErr != nil {
		return fmt.Errorf("cannot authorize join: returned binding is malformed: %w", vErr)
	}
	if b.ClusterID != meta.ClusterID || b.EpochID != meta.EpochID {
		return fmt.Errorf("cannot authorize join: returned binding (%s/%s) does not match the authorized epoch (%s/%s); fail-closed",
			b.ClusterID, b.EpochID, meta.ClusterID, meta.EpochID)
	}
	if b.Provenance != ProvenanceJoin {
		return fmt.Errorf("cannot authorize join: returned binding has provenance %s (expected join; a concurrent binding operation may have created a different-provenance binding); fail-closed", b.Provenance)
	}

	// 4. POST-BIND AUTHORITATIVE RE-READ (direct, not the pre-guard
	// snapshot). The pre-guard snapshot and the durable Bind are NOT atomic
	// across the metadata source and SQLite: a metadata change (the operator
	// revoking the authorization or changing the epoch) may linearize between
	// the snapshot read and the Bind. Re-reading the authoritative record
	// after the Bind ensures that a revoked or changed authorization is
	// observed before the final guard. This re-read does NOT overwrite newer
	// controller-accepted metadata; it is used only to confirm the
	// authorization was not revoked.
	reMeta, reErr := c.source.Read(ctx)
	if reErr != nil {
		return fmt.Errorf("cannot authorize join: authoritative metadata became unavailable after the local binding was written (fail-closed; the binding is preserved): %w", reErr)
	}
	if vErr := reMeta.Validate(); vErr != nil {
		return fmt.Errorf("cannot authorize join: authoritative metadata is malformed after the local binding was written (fail-closed; the binding is preserved): %w", vErr)
	}
	// The cluster must still be initialized (an explicit Initialized=true
	// requirement, applied for ALL configurations — including those without
	// a designated SeedIdentity). The post-Bind authoritative re-read is
	// the authoritative gate: a record that no longer reports an
	// initialized epoch after the Bind is rejected fail-closed and the
	// committed binding is preserved.
	if !reMeta.Initialized {
		return fmt.Errorf("cannot authorize join: authoritative metadata reports Initialized=false after the local binding was written (the cluster is no longer initialized); fail-closed, the binding is preserved")
	}
	if !reMeta.MatchesBinding(b) {
		return fmt.Errorf("cannot authorize join: the local binding (%s/%s) no longer matches the authoritative metadata (%s/%s) after the Bind (the epoch changed or was revoked); fail-closed, the binding is preserved",
			b.ClusterID, b.EpochID, reMeta.ClusterID, reMeta.EpochID)
	}
	if c.cfg.SeedIdentity != "" {
		if reMeta.GenesisAuthorized {
			return fmt.Errorf("cannot authorize join: the authoritative metadata now reports GenesisAuthorized=true (the cluster is no longer finalized); fail-closed, the binding is preserved")
		}
		if reMeta.GenesisConsumer != c.cfg.SeedIdentity {
			return fmt.Errorf("cannot authorize join: the authoritative metadata genesis consumer is %q (expected %q); fail-closed, the binding is preserved",
				reMeta.GenesisConsumer, c.cfg.SeedIdentity)
		}
		if !reMeta.DataLossProtected {
			return fmt.Errorf("cannot authorize join: the authoritative metadata reports DataLossProtected=false (data-loss protection is not armed); fail-closed, the binding is preserved")
		}
	}

	// 5. FINAL LOCAL GUARD (c.mu.Lock + m.mu.Lock, no I/O).
	if guardErr := c.authorizeJoinFinalGuard(ctx, b, obs, target, expectedIdentity); guardErr != nil {
		c.logger.Warn("join final authorization guard rejected; durable binding preserved",
			slog.String("cluster", meta.ClusterID),
			slog.String("epoch", meta.EpochID),
			slog.String("node", c.cfg.NodeID),
			slog.Any("error", guardErr),
		)
		return guardErr
	}

	c.logger.Info("join: local database bound to authorized epoch",
		slog.String("cluster", meta.ClusterID),
		slog.String("epoch", meta.EpochID),
		slog.Int("generation", meta.Generation),
		slog.String("node", c.cfg.NodeID),
		slog.String("provenance", "join"),
	)
	return nil
}

// authorizeJoinFinalGuard is the dedicated, fail-closed join authorization
// guard evaluated while holding both c.mu.Lock and m.mu.Lock (the
// established write-lock order). It revalidates every join prerequisite
// against the CURRENT controller and machine state (not the pre-guard
// snapshot) and performs the idempotent warm-up and legal state transition
// when all checks pass. It performs no I/O: no network calls, no SQLite
// writes, and no blocking context reads.
//
// The guard is the sole final authority for the join transition. It
// validates, in order:
//
//  1. the caller's context is live (checked under the held lock pair, so a
//     cancellation that linearizes before the guard is observed and the
//     operation is rejected; a cancellation after this point does not
//     retrospectively revoke a completed operation);
//  2. the lifecycle is enabled (Config.Enabled == true);
//  3. the controller is not stopped;
//  4. the controller's context is still live (a non-blocking check that
//     detects a Stop-initiated cancellation);
//  5. a current accepted metadata observation exists (c.accepted != nil);
//  6. the current authority state is fresh and not stale;
//  7. the current metadata is valid (Validate() succeeds);
//  8. the current metadata reports an initialized epoch (Initialized=true);
//  9. for a genesis-managed configuration (a designated SeedIdentity), the
//     complete finalized-genesis invariant is satisfied
//     (GenesisAuthorized=false, GenesisConsumer=<seed>, DataLossProtected=true);
//  10. the current metadata's cluster/epoch match the pinned join target;
//  11. the current metadata matches the actual durable binding;
//  12. the binding's provenance is join (the only provenance appropriate for
//     the join path; genesis and explicitly-unknown/legacy provenance are
//     rejected);
//  13. the source observation is valid, within its bounded lifetime, and
//     consistent with the current metadata (state READY/DEGRADED,
//     BootstrapSourceEligible, readiness, cluster/epoch agreement, peer not
//     local, and the observation's recorded target and authenticated peer
//     identity matching the ORIGINAL pinned target and expected identity
//     supplied to AuthorizeJoin — the same single validator as the
//     pre-binding guard, enforced against the original request parameters);
//  14. the current lifecycle state permits the transition into SYNCING
//     (SYNCING: idempotent no-op; DISCOVERING: direct; STARTING: via the
//     legal STARTING→DISCOVERING→SYNCING path); any other state is rejected.
//
// On success it warms the binding cache (c.bindingVal) and advances the
// authorization generation counter (c.authGen) under the held c.mu write
// lock, then performs the legal state transition under the held m.mu write
// lock. A newer controller observation (a metadata change that advanced
// authGen after this guard's read) is detected by the authority-state and
// metadata checks and rejects the join fail-closed; this guard does not
// overwrite a newer observation with a stale join-specific read.
func (c *Controller) authorizeJoinFinalGuard(ctx context.Context, b *DatabaseBinding, obs SourceObservationResult, target RequestContext, expectedIdentity string) error {
	if b == nil {
		return errors.New("join guard: no durable binding to validate")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.machine
	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Caller context live (checked under the held lock pair: a
	// cancellation that linearizes before the guard is observed and the
	// operation is rejected; cancellation after this point does not
	// retrospectively revoke a completed operation).
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("join guard: caller context is no longer live (%v); the join is rejected (a committed binding is preserved and is not rolled back)", ctxErr)
	}
	// 2. Lifecycle enabled.
	if !c.cfg.Enabled {
		return errors.New("join guard: lifecycle protection is not enabled")
	}
	// 3. Controller not stopped.
	if c.stopped {
		return errors.New("join guard: controller is stopped")
	}
	// 4. Controller context live (non-blocking).
	if c.ctrlCtx != nil && c.ctrlCtx.Err() != nil {
		return errors.New("join guard: controller context is no longer live (controller was stopped or its parent context was cancelled)")
	}
	// 5. Current accepted metadata exists.
	if c.accepted == nil {
		return errors.New("join guard: no current accepted metadata observation")
	}
	// 6. Current authority fresh, not stale.
	stale := false
	if !c.freshAt.IsZero() {
		stale = c.now().After(c.freshAt.Add(c.cfg.MetadataPollInterval * 2))
	}
	if c.authorityState != "fresh" || stale {
		return fmt.Errorf("join guard: current authority is not fresh (authority=%s, stale=%v)", c.authorityState, stale)
	}
	// 7. Metadata valid.
	if vErr := c.accepted.Validate(); vErr != nil {
		return fmt.Errorf("join guard: current metadata is malformed: %w", vErr)
	}
	// 8. Initialized epoch.
	if !c.accepted.Initialized {
		return errors.New("join guard: current metadata reports Initialized=false")
	}
	// 9. Complete genesis-finalization invariant (genesis-managed config).
	if c.cfg.SeedIdentity != "" {
		if c.accepted.GenesisAuthorized {
			return errors.New("join guard: genesis authorization is still outstanding (cluster not finalized)")
		}
		if c.accepted.GenesisConsumer != c.cfg.SeedIdentity {
			return fmt.Errorf("join guard: genesis consumer is %q (expected %q)", c.accepted.GenesisConsumer, c.cfg.SeedIdentity)
		}
		if !c.accepted.DataLossProtected {
			return errors.New("join guard: data-loss protection is not armed (cluster not fully finalized)")
		}
	}
	// 10. Cluster/epoch match the pinned join target.
	if c.accepted.ClusterID != target.ClusterID || c.accepted.EpochID != target.EpochID {
		return fmt.Errorf("join guard: current metadata cluster/epoch %s/%s does not match the pinned join target %s/%s",
			c.accepted.ClusterID, c.accepted.EpochID, target.ClusterID, target.EpochID)
	}
	// 11. Current metadata matches the actual durable binding.
	if !c.accepted.MatchesBinding(b) {
		return fmt.Errorf("join guard: current metadata %s/%s does not match the durable binding %s/%s",
			c.accepted.ClusterID, c.accepted.EpochID, b.ClusterID, b.EpochID)
	}
	// 12. Binding provenance is join (the only appropriate provenance for the
	// join path). Genesis provenance is the original seed's binding;
	// explicitly-unknown (legacy) provenance authorizes neither genesis nor
	// joining.
	if b.Provenance != ProvenanceJoin {
		return fmt.Errorf("join guard: binding provenance %s is not appropriate for joining (only join provenance is permitted; genesis and explicitly-unknown/legacy provenance are rejected)", b.Provenance)
	}
	// 13. Source observation valid and consistent with current metadata.
	// The same single consistent controller-side validator is used as in the
	// pre-binding guard (validateSourceObservationResult), passing the
	// ORIGINAL pinned target and ORIGINAL expected identity from
	// AuthorizeJoin so the final authorization contract accurately enforces
	// the original request parameters (not an identity compared against
	// itself).
	if obsErr := c.validateSourceObservationResult(obs, c.accepted, target, expectedIdentity); obsErr != nil {
		return fmt.Errorf("join guard: %w", obsErr)
	}
	// 14. Legal state transition into SYNCING.
	switch m.state {
	case SYNCING:
		// Idempotent: already in the target state. No extra durable bind,
		// no state transition. The guard above confirmed the complete
		// current authorization and the matching durable binding.
	case DISCOVERING:
		if err := m.transitionLocked(SYNCING, joinTransitionReason(c.cfg.NodeID, target, "join authorized")); err != nil {
			return fmt.Errorf("join guard: transition DISCOVERING→SYNCING: %w", err)
		}
	case STARTING:
		if err := m.transitionLocked(DISCOVERING, fmt.Sprintf("join: discovery for cluster=%s epoch=%s", target.ClusterID, target.EpochID)); err != nil {
			return fmt.Errorf("join guard: transition STARTING→DISCOVERING: %w", err)
		}
		if err := m.transitionLocked(SYNCING, joinTransitionReason(c.cfg.NodeID, target, "join authorized")); err != nil {
			return fmt.Errorf("join guard: transition DISCOVERING→SYNCING (from STARTING): %w", err)
		}
	default:
		return fmt.Errorf("join guard: lifecycle state %s does not permit transition into SYNCING (only STARTING, DISCOVERING, or already-SYNCING are permitted)", m.state)
	}

	// Warm the binding cache and advance the authorization generation under
	// the held c.mu write lock.
	c.bindingVal = b
	c.authGen++
	return nil
}

// revalidateJoinPreBind revalidates the complete join authorization after the
// network source observation and before the durable Bind. The observation is
// network I/O; during it the controller's accepted metadata, authority state,
// or lifecycle state may have changed. This revalidation obtains a FRESH,
// coherent authorization snapshot (c.authorization) and revalidates every
// join prerequisite against the CURRENT accepted authority — not the
// pre-observation snapshot. Any change that linearized during the observation
// (an authority change, a state change that forbids joining, a stop, a
// conflicting epoch, or a revoked finalization invariant) is observed and the
// join is rejected before any durable write.
//
// It performs no I/O (the snapshot read is a fast, in-memory read; the
// observation is already complete) and holds no controller or machine lock.
// This is NOT a distributed atomic transaction: a change that linearizes after
// this re-read (between the re-read and the final guard) is detected by the
// final guard (the post-Bind authoritative re-read and the final local guard
// remain the authoritative gates).
func (c *Controller) revalidateJoinPreBind(ctx context.Context, obs SourceObservationResult, expectedIdentity string, target RequestContext) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.binding == nil {
		return errors.New("no binding store configured (cannot authorize join: the durable binding store is a required dependency)")
	}
	if c.source == nil {
		return errors.New("no metadata source configured (cannot authorize join: the authoritative metadata source is a required dependency)")
	}
	snap := c.authorization()
	// The controller must not be stopped and its context must remain live.
	if snap.Stopped {
		return errors.New("controller is stopped after the source observation (the join is rejected before any durable write)")
	}
	if c.ctrlCtx != nil && c.ctrlCtx.Err() != nil {
		return errors.New("controller context is no longer live after the source observation (the controller was stopped)")
	}
	// The current accepted metadata must exist and be fresh (not stale).
	// A change that linearized during the observation is observed here: an
	// authority change (the metadata worker accepted a different or revoked
	// record) makes the current observation not fresh or not matching the
	// pinned target.
	if snap.Metadata == nil {
		return errors.New("no current accepted metadata observation after the source observation (the join is rejected before any durable write)")
	}
	if snap.AuthorityState != "fresh" || snap.Stale {
		return fmt.Errorf("current authority is not fresh after the source observation (authority=%s, stale=%v); the join is rejected before any durable write", snap.AuthorityState, snap.Stale)
	}
	meta := *snap.Metadata
	if vErr := meta.Validate(); vErr != nil {
		return fmt.Errorf("current metadata is malformed after the source observation: %w", vErr)
	}
	// The cluster must still be initialized (an explicit Initialized=true
	// requirement; the metadata may have changed during the observation).
	if !meta.Initialized {
		return errors.New("current metadata reports Initialized=false after the source observation (the cluster is no longer initialized; the join is rejected before any durable write)")
	}
	// The complete finalized-genesis invariant where applicable (a
	// genesis-managed configuration).
	if c.cfg.SeedIdentity != "" && !c.finalizationComplete(&meta) {
		return fmt.Errorf("current metadata is not a completely finalized record for the designated seed after the source observation (Initialized=%v GenesisAuthorized=%v GenesisConsumer=%q DataLossProtected=%v); the join is rejected before any durable write",
			meta.Initialized, meta.GenesisAuthorized, meta.GenesisConsumer, meta.DataLossProtected)
	}
	// The current cluster and epoch must still match the pinned join target
	// (a conflicting epoch that linearized during the observation is
	// rejected here, before any durable write).
	if meta.ClusterID != target.ClusterID || meta.EpochID != target.EpochID {
		return fmt.Errorf("current metadata cluster/epoch %s/%s no longer matches the pinned join target %s/%s after the source observation; the join is rejected before any durable write",
			meta.ClusterID, meta.EpochID, target.ClusterID, target.EpochID)
	}
	// The current lifecycle state must still permit joining.
	if !snap.State.permitsJoinTransition() {
		return fmt.Errorf("lifecycle state %s no longer permits a join transition after the source observation; the join is rejected before any durable write", snap.State)
	}
	// The existing local binding must still be absent or suitable for
	// ordinary joining (a concurrent binding operation may have created a
	// binding during the observation; a conflicting or inappropriate
	// provenance is rejected).
	if snap.Binding != nil {
		if snap.Binding.Provenance == ProvenanceGenesis {
			return errors.New("a genesis-provenance binding now exists (created during the source observation); the join path requires join provenance (rejected before any durable write)")
		}
		if snap.Binding.Provenance == ProvenanceLegacy {
			return errors.New("an explicitly-unknown (legacy) provenance binding now exists; a fail-closed historical binding does not authorize joining (rejected before any durable write)")
		}
		if !snap.Binding.MatchesMetadata(meta) {
			return fmt.Errorf("existing local binding (%s/%s) conflicts with the authorized epoch (%s/%s) after the source observation; fail-closed, the binding is preserved",
				snap.Binding.ClusterID, snap.Binding.EpochID, meta.ClusterID, meta.EpochID)
		}
	}
	// The complete source observation must still be valid, and its pinned
	// target and authenticated peer identity must match the requested target
	// and expected identity (the single consistent controller-side
	// observation validator; the metadata may have changed during the
	// observation, so it is validated against the current metadata).
	if obsErr := c.validateSourceObservationResult(obs, &meta, target, expectedIdentity); obsErr != nil {
		return obsErr
	}
	return nil
}

// validateSourceObservationResult applies the strict, fail-closed validation
// to an authenticated observation outside the final guard's lock pair (used
// in the pre-binding revalidation before the durable Bind). It is the
// unlocked counterpart of validateSourceObservationResultLocked.
//
// target is the pinned join target (the expected cluster/epoch) and
// expectedIdentity is the authenticated peer identity the observation was
// requested for; both must match the observation's recorded pinned target and
// authenticated identity (a single consistent controller-side identity and
// target check; see the SourceObservationResult.Valid future-timestamp and
// bounded-lifetime checks for the temporal policy).
func (c *Controller) validateSourceObservationResult(r SourceObservationResult, meta *ClusterMetadata, target RequestContext, expectedIdentity string) error {
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
		return fmt.Errorf("source observation: peer %q reported cluster/epoch %s/%s does not match the current metadata %s/%s", r.peerNodeID, r.clusterID, r.epochID, meta.ClusterID, meta.EpochID)
	}
	if r.peerIdentity == "" || r.peerNodeID == "" {
		return errors.New("source observation: peer identity is missing (authenticated identity or self-reported node ID is empty)")
	}
	if c.cfg.NodeID != "" && r.peerNodeID == c.cfg.NodeID {
		return fmt.Errorf("source observation: the observed peer %q is the local replica; self-observation is not a bootstrap source", r.peerNodeID)
	}
	// The observation's pinned target must match the requested target
	// (cluster/epoch), and its authenticated peer identity must match the
	// expected identity the observation was requested for. The
	// production observer records these from the request, but the
	// controller revalidates them here so a trusted-boundary result that
	// carries a mismatched target or identity is rejected (a single
	// consistent controller-side check; the temporal policy — future
	// timestamps, invalid/expired lifetimes, the maximum lifetime — is
	// enforced in SourceObservationResult.Valid).
	if r.expected.ClusterID != target.ClusterID || r.expected.EpochID != target.EpochID {
		return fmt.Errorf("source observation: pinned target %s/%s does not match the requested target %s/%s", r.expected.ClusterID, r.expected.EpochID, target.ClusterID, target.EpochID)
	}
	if r.peerIdentity != expectedIdentity {
		return fmt.Errorf("source observation: authenticated peer identity %q does not match the expected identity %q", r.peerIdentity, expectedIdentity)
	}
	return nil
}

// joinTransitionReason builds the transition reason for the join path.
func joinTransitionReason(nodeID string, target RequestContext, prefix string) string {
	return fmt.Sprintf("%s: replica %s bound to cluster=%s epoch=%s (join provenance; initial synchronization pending)",
		prefix, nodeID, target.ClusterID, target.EpochID)
}

// permitsJoinTransition reports whether the given lifecycle state permits a
// join transition into SYNCING. Only STARTING, DISCOVERING, and SYNCING
// (idempotent) are permitted; all other states (GENESIS, READY, DEGRADED,
// RECOVERY_REQUIRED) reject the join.
func (s State) permitsJoinTransition() bool {
	switch s {
	case STARTING, DISCOVERING, SYNCING:
		return true
	default:
		return false
	}
}
