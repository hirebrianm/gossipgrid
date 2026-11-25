// Package main: production bootstrap wiring helper (Milestone 5B, Stage 2).
//
// wireBootstrapEngine sets the DEDICATED bootstrap outbound admission check,
// the bootstrap authorization source, and the bootstrap commit guard on an
// anti-entropy engine, using the given lifecycle controller. It is shared
// between app startup (main.go) and the integration tests
// (bootstrap_integration_test.go) so the production wiring path is exercised
// end-to-end without duplication.
//
// The bootstrap admission check (SetBootstrapAdmissionCheck) is SEPARATE from
// the ordinary anti-entropy admission check (SetAdmissionCheck, wired to
// CheckReplicationOutbound, which requires a ready state). A joining SYNCING
// replica is authorized for bootstrap fetch (CheckBootstrapFetch: a
// SYNCING-only, epoch-bound, narrowly scoped decision) but NOT for ordinary
// replication, so the two admission paths must be independent.
//
// The bootstrap authorization source (SetBootstrapAuthorizationSource) pins
// the expected cluster/epoch from a consistent authorized snapshot and
// revalidates it at every boundary. The controller implements
// antientropy.BootstrapAuthorizationSource (BootstrapAuthorizationSnapshot),
// returning the same snapshot the admission decision is based on, so the
// pinned context and the decision are always consistent.
//
// The bootstrap commit guard (SetBootstrapGuard) revalidates the pinned
// expected cluster/epoch against the current snapshot under the same locks
// at the local write boundary. This is separate from the ordinary commit
// guard (CommitUnderAuthorization) and does not authorize public writes,
// gossip, or acting as a source.

package main

import (
	"context"
	"fmt"

	"gossipgrid/internal/antientropy"
	"gossipgrid/internal/lifecycle"
)

// wireBootstrapEngine wires an anti-entropy engine with the production
// bootstrap wiring. See the package-level doc comment for the full
// rationale.
func wireBootstrapEngine(engine *antientropy.Engine, ctrl *lifecycle.Controller) {
	admission := ctrl.Admission()
	bootstrapCtxProvider := newControllerEpochProvider(ctrl)

	engine.SetBootstrapAdmissionCheck(func(ctx context.Context) error {
		reqCtx, cerr := bootstrapCtxProvider.Context()
		if cerr != nil {
			return fmt.Errorf("lifecycle bootstrap admission: cannot establish authorized context: %s", cerr)
		}
		decision := admission.CheckBootstrapFetch(ctx, reqCtx)
		if !decision.Allowed {
			return fmt.Errorf("lifecycle bootstrap admission: %s", decision.Reason)
		}
		return nil
	})
	engine.SetBootstrapAuthorizationSource(ctrl)
	engine.SetBootstrapGuard(ctrl)
}
