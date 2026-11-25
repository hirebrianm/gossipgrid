# ADR-0004: Cluster view as a composed read-only aggregation

**Status:** Proposed · **Date:** 2026-11-24

## Context

The M6 browser UI provides a per-replica diagnostics panel. For a multi-replica cluster, an
operator must open one browser session per pod to see the full picture: which replicas are
ready, which are syncing, whether all are on the same epoch, and whether message counts
converge. There is no single view that composes this information from all reachable replicas.

The existing `GET /internal/status` peer endpoint (M4A) already returns the pod identity,
node ID, version, lifecycle state, readiness, bootstrap-source eligibility, and (when
lifecycle is enabled) the epoch envelope headers (cluster ID, epoch ID, generation) for each
peer over mTLS. The `PeerStatusInfo` struct does **not** include message counts or
initial-sync progress — those are proposed for the M6B Stage 2 telemetry foundation. The
local replica's `peertls.Client` already has a `GetStatus` method. The discovery snapshot
already lists the expected peers. The lifecycle state, reachability, and epoch data are
already available; the message-count and sync-progress data will require the Stage 2
telemetry infrastructure.

## Decision

Add a **read-only cluster view** page to the existing M6 browser UI. The web handler (on the
local replica) aggregates the local diagnostics (in-process) with a bounded, per-peer
`GET /internal/status` request to each peer in the discovery snapshot, and renders a composed
`ClusterView` document. The browser never connects to peer mTLS endpoints directly; the
aggregation happens server-side on the local replica.

Key constraints:

- **Read-only.** The cluster view does not expose any mutation endpoint. It reads the same data
  the existing diagnostics and peer status endpoints already expose.
- **Bounded.** A per-peer timeout (3 s) and a total aggregation timeout (10 s) prevent the web
  handler from blocking indefinitely. An unreachable peer is reported as `unreachable`, not an
  error for the page as a whole.
- **Eventual, not continuous.** The view reflects the state at the moment of aggregation. A
  page reload or a bounded auto-refresh (15 s, vanilla JS `setTimeout`) is the update mechanism.
  No WebSocket, no SSE, no polling tighter than 5 s.
- **No new transport, no new authorization, no metrics export, no Kubernetes API integration.**
  The cluster view reuses the existing `peertls.Client`, the existing `GET /internal/status`
  endpoint, the existing M6 authentication, and the existing discovery snapshot.

## Consequences

- A new `ClusterView` struct and `GET /api/clusterview` handler in `internal/web`. The browser
  template is a new tab in the existing UI (vanilla JS, no external assets, no build pipeline).
- The aggregation is best-effort: a peer that is unreachable, in a different epoch, or in
  `RECOVERY_REQUIRED` is reported as such. The view does not attempt to "fix" or "promote" any
  replica.
- The convergence indicator (all replicas report the same observed message count) is a
  **heuristic**, not a proof: ID-only anti-entropy cannot detect same-ID content divergence
  (M4C limitation). The view must be labelled accordingly. The message count is an observed
  telemetry value (M6B Stage 2), not a field in the current `PeerStatusInfo` struct.
- The cluster view works identically in local development (static discovery) and in-cluster
  (DNS discovery). No new Helm values are required.
- The cluster view is Stage 1 of Milestone 6B (the complete M6B feature roadmap: Stage 1
  composed status aggregation, Stage 2 bounded telemetry event buffers, Stage 3
  propagation/reconciliation visualization, Stage 4 lifecycle/resilience visualization; see
  [cluster-visualization.md](../cluster-visualization.md)). It is a sub-milestone after 6,
  before 7B. It does not block 7B: the lifecycle enforcement and in-cluster recovery tests
  do not depend on a cluster view.
- Metrics export (Prometheus `/metrics`, OpenTelemetry) is **not** part of 6B. It is a
  candidate for Milestone 9.

References: [cluster-visualization.md](../cluster-visualization.md),
[architecture.md](../architecture.md), [M4C anti-entropy limitations](../gossip-and-bootstrap.md#anti-entropy-milestone-4c--implemented-2026-11-20).
