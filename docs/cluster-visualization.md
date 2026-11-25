# Cluster Visualization and Observability (Milestone 6B — proposed design)

**Status:** Proposed. No application code is implemented. This document is a design proposal for a
cluster-wide view that composes per-replica diagnostics into a single operator-facing dashboard. It is
**not** a new authorization mechanism, **not** a new transport, and **not** a new synchronization
engine — it reuses the existing `GET /api/diagnostics` endpoint, the private peer
`GET /internal/status` endpoint, and the existing discovery snapshot.

## Motivation

Milestone 6 (the browser UI) provides a **per-replica** diagnostics panel: a single pod's node ID,
lifecycle state, peer list, message count, credential state, and FIPS state. For a five-replica
cluster, an operator must open five separate browser sessions (one per pod) to assemble the full
cluster picture. There is no way to see, from a single view:

- Which replicas are `READY`, which are `SYNCING`, and which are `RECOVERY_REQUIRED`.
- Whether the cluster is in a single epoch or split across epochs (a stale-epoch pod would be
  invisible to per-replica views that do not compare epoch IDs).
- Which replicas have completed their initial sync barrier (M7A) or are still missing peers.
- The convergence state: are all replicas reporting the same message count, or is one lagging?
- The discovery snapshot: is the local replica seeing all five peers, or is discovery stale?

Milestone 6B adds a **cluster view** page to the existing browser UI that aggregates this information
from all reachable replicas. It is an **observability** feature: it reads existing endpoints and
renders a composed view. It does not write, reset, or modify any replica state.

## Design

### Data sources (all existing, no new endpoints)

The cluster view composes three existing data sources:

1. **Local replica diagnostics** — `GET /api/diagnostics` (already authenticated, already exposed
   in the M6 UI). Provides the local node ID, version, FIPS state, lifecycle state, genesis
   status, initial sync barrier state, peer list, peer mTLS state, and credential state.

2. **Remote peer status** — `GET /internal/status` on each reachable peer (the existing private peer
   endpoint, already used by the M4A/M5A peer client). The `peertls.Client` already has a
   `GetStatus` method (added in M4A). Each peer's status response (`PeerStatusInfo`) carries the
   pod identity, node ID, version, certificate fingerprint, lifecycle state, readiness,
   bootstrap-source eligibility, diagnostic reason, and (when lifecycle is enabled) the epoch
   envelope headers (cluster ID, epoch ID, generation). A lifecycle-enabled client validates the
   response envelope before accepting the status.

   **Currently available vs proposed telemetry:** The existing `PeerStatusInfo` struct does
   **not** include message counts or initial-sync progress. Those fields are proposed for the
   M6B Stage 2 telemetry foundation (observed application events with source identity and
   event sequencing). The Stage 1 data model below distinguishes between data that is
   currently available from `PeerStatusInfo` and data that will require the Stage 2 telemetry
   infrastructure.

3. **Discovery snapshot** — the local replica's discovery manager `Snapshot()` (already available
   in the M6 diagnostics). Provides the list of expected peers (hostname, identity, state) and
   whether the snapshot is stale (DNS mode) or fixed (static mode).

### Aggregation protocol

The cluster view page (rendered by the local replica's web handler) performs the following:

1. Read the local diagnostics (in-process; no network).
2. For each peer in the discovery snapshot (excluding self), issue `GET /internal/status` over mTLS
   using the existing `peertls.Client.GetStatus`. Use a bounded per-peer timeout (e.g. 3 seconds)
   and a bounded total aggregation timeout (e.g. 10 seconds for all peers). A peer that does not
   respond within the timeout is reported as `unreachable` (not an error for the page as a whole).
3. Compose a **cluster state document** (a new `ClusterView` struct, see below) from the local
   diagnostics and the remote peer status responses.
4. Render the cluster view in the browser.

The aggregation is **eventual and bounded**: it does not block the web handler indefinitely, does not
retry beyond the per-peer timeout, and does not persist anything. A refresh (page reload or a
bounded auto-refresh, e.g. every 15 seconds) re-issues the peer status requests. The view reflects
the state **at the moment of aggregation**, not a continuous stream.

### Cluster state document (`ClusterView`)

```text
ClusterView {
  // Cluster identity (from local diagnostics; all peers should agree).
  ClusterID     string
  EpochID       string
  Generation    int        // from metadata, if available

  // Per-replica state.
  Replicas      []ReplicaView

  // Cluster-level derived state.
  ReadyCount        int
  SyncingCount      int
  DegradedCount     int
  RecoveryRequired  int
  UnreachableCount  int
  AllSameEpoch      bool
  AllSameMessageCount bool  // convergence indicator (not a proof)

  // Discovery.
  ExpectedReplicas  int
  DiscoveryStale    bool
  SelfID            string

  // Timestamp.
  AggregatedAt    time.Time
}

ReplicaView {
  // Currently available from PeerStatusInfo:
  NodeID        string
  Identity      string   // mTLS identity (from peer status or discovery)
  State         string   // lifecycle state (READY, SYNCING, ...) or "unknown"
  Ready         bool
  Reachable     bool
  Error         string   // nonempty if unreachable (timeout, 503, 409, etc.)
  // When lifecycle is enabled and the peer is reachable:
  PeerClusterID string
  PeerEpochID   string
  PeerGeneration int
  // Proposed (M6B Stage 2 telemetry; NOT currently in PeerStatusInfo):
  MessageCount     int64  // observed, not inferred; requires Stage 2 telemetry
  InitialSyncComplete  bool
  InitialSyncCompleted int
  InitialSyncExpected  int
}
```

### Web UI

A new page tab ("Cluster View") in the existing browser UI, alongside the existing "Messages" and
"Diagnostics" tabs. The cluster view renders:

- A **header** showing the cluster ID, epoch ID, generation, and aggregation timestamp.
- A **grid** of replica cards (one per expected peer), each showing: node ID, lifecycle state
  (color-coded: green = READY, yellow = SYNCING/DEGRADED, red = RECOVERY_REQUIRED, grey =
  unreachable), message count, and readiness.
- A **convergence indicator**: a simple "All replicas report the same message count" badge (green
  when true, amber when false) — this is a **heuristic**, not a proof of convergence (ID-only
  anti-entropy cannot detect same-ID content divergence; see the M4C limitations).
- An **epoch consistency indicator**: "All reachable replicas report the same epoch" badge.
- A **discovery status** line: expected replicas, discovered peers, stale flag.

The page uses the same vanilla JS / embedded Go template pattern as the existing M6 UI. No
frontend build pipeline, no external assets, no CDN.

### Authentication and authorization

The cluster view page is served on the **public** web router (port 8080) under the existing M6
authentication (single-administrator password, session-bound CSRF). It does **not** expose the
private peer endpoints directly to the browser; instead, the **web handler** (on the local replica)
makes the peer status requests server-side over mTLS and returns the composed `ClusterView`
as JSON. The browser never connects to peer mTLS endpoints.

The peer status requests are **read-only**: `GET /internal/status` returns a status snapshot and
does not mutate state. The existing M5A admission check for the status endpoint (which requires
the requesting peer to carry a valid epoch context when lifecycle is enabled) still applies: the
local replica's `peertls.Client` carries its own epoch context, and a peer that rejects the
request (409, 503) is reported as `unreachable` with the error reason.

### What is NOT in scope for 6B

- **No new transport.** The cluster view reuses the existing `peertls.Client` and the existing
  `GET /internal/status` endpoint. No new peer-to-peer protocol, no new multiplexing.
- **No new authorization.** The cluster view does not grant any new privilege. It reads the same
  data the existing diagnostics and peer status endpoints already expose. A `SYNCING` replica
  cannot access the cluster view (the M6 web handler is only registered when the replica is
  ready, or the web router is configured to require authentication regardless of lifecycle
  state — the cluster view does not change this).
- **No write path.** The cluster view does not expose any mutation endpoint. It is read-only
  observability.
- **No metrics export.** Prometheus `/metrics` endpoints, OpenTelemetry, or structured log
  aggregation are **not** part of 6B. They are a candidate for Milestone 9 (the
  resilience/security lab, which includes metrics as a deliverable).
- **No continuous streaming.** The cluster view is a bounded, on-demand aggregation. It does not
  push updates via WebSocket or Server-Sent Events. A page reload or a bounded auto-refresh
  (15 seconds, implemented in vanilla JS with `setTimeout`) is the update mechanism.
- **No Kubernetes API integration.** The cluster view uses the application's own discovery
  snapshot and peer status, not the Kubernetes API. It works identically in local development
  (static discovery) and in-cluster (DNS discovery).

### M6B feature stages (approved roadmap)

M6B is delivered in four feature stages. Each stage has dependencies, deliverables, and
measurable acceptance criteria. The existing Go web infrastructure, embedded assets, vanilla JS,
and native SVG are used throughout. Bounded polling is acceptable; WebSockets, SSE, external
observability systems, frontend frameworks, and Kubernetes API access are **not** required. The
visualization must support both lifecycle-enabled and lifecycle-disabled operation.

#### Stage 1 — Cluster status and interactive topology

- **Depends on:** M6 (web UI, authentication), M4A (peer status endpoint), M5A (lifecycle state
  machine, epoch fencing).
- **Deliverables:** Authenticated cluster-wide diagnostics endpoint (`GET /api/clusterview`);
  interactive native SVG topology (clickable nodes, hover tooltips, drag-to-pan); observed
  discovery and reachability per peer; readiness and lifecycle state per replica; epoch
  consistency indicator (all reachable replicas in the same cluster/epoch); diagnostic
  uncertainty indicators (unreachable peers, stale discovery, missing metadata).
- **Acceptance criteria:** A browser operator sees all five replicas (or the configured count)
  in a single view with correct lifecycle states; an unreachable peer is shown as grey with an
  error reason (not omitted); epoch mismatch across replicas is visible; the view refreshes via
  bounded polling (≤ 15 s) without blocking the web handler; no peer data is fabricated — a
  field is shown as "unknown" when the peer did not report it. The existing aggregation, data
  model, tests, template, and wiring design (documented above) are implementation tasks within
  this stage.

#### Stage 2 — Native telemetry foundation

- **Depends on:** Stage 1 (the cluster view page and its data model).
- **Deliverables:** Bounded, concurrency-safe in-memory event buffers (ring buffer or
  bounded slice with atomic counters) per replica; actual observed application events
  (message received, message inserted, anti-entropy batch committed, bootstrap batch
  committed, lifecycle state transition) with timestamps, event sequence numbers, and source
  identity (node ID); dropped-event accounting (total dropped since startup, per-event-type
  counters); bounded collection (the telemetry must not block or interfere with replication
  correctness — a slow consumer is dropped, not queued unboundedly); slow-consumer handling
  (when the buffer is full, new events are dropped and the counter increments, no back-pressure
  on the replication path); authenticated read-only access (telemetry is served under the
  existing M6 authentication); explicit resource budgets (max buffer size, max event size,
  max retention window — all configurable and bounded); graceful degradation (when telemetry
  is unavailable or the buffer is full, the UI shows "telemetry unavailable" rather than
  fabricating data).
- **Acceptance criteria:** No event buffer exceeds its configured size under sustained load;
  dropped-event count is accurate (unit test: fill the buffer, verify the drop counter);
  no message bodies or credentials are ever included in telemetry events (code review +
  static analysis); the replication path's p99 latency is not measurably degraded by telemetry
  collection (benchmarks before/after); the UI correctly shows dropped events and "unknown"
  when data is unavailable.

#### Stage 3 — Replication visualization

- **Depends on:** Stage 2 (the telemetry event buffers and event types).
- **Deliverables:** Visualization of actual gossip message propagation (which peer sent
  which message to which peer, observed via telemetry events); anti-entropy reconciliation
  and transfer (inventory page fetched, message batch committed, cursor advanced — all from
  actual observed events, not inferred); bootstrap progress (batch committed, page completed,
  remaining pages); baseline installation (records installed, conflicts, expirations);
  convergence evidence (per-replica message counts from Stage 2 telemetry, with explicit
  "not a proof of convergence" caveat).
- **Acceptance criteria:** Every visualized event traces to an actual telemetry record (no
  fabricated or inferred events); equal message counts across replicas are shown as a
  heuristic with an explicit "not a proof of convergence" label (ID-only anti-entropy cannot
  detect same-ID content divergence); the bootstrap progress view shows the actual
  FetchBootstrapBaseline result (inserted, already-present, expired, conflict counts); the
  visualization works identically when lifecycle is disabled (no lifecycle-specific fields
  are required).

#### Stage 4 — Lifecycle and resilience visualization

- **Depends on:** Stage 3 (the replication visualization and telemetry), M5B (lifecycle
  metadata, bootstrap admission — Stage 2 minimum for the visualization to be meaningful).
- **Deliverables:** Lifecycle transition visualization (state changes over time, with
  timestamps and reasons from telemetry); epoch/authority transitions (epoch change
  observed, binding accepted, authorization revoked); failure and replacement (a replica
  becomes unreachable, a replacement pod appears in discovery); recovery (a
  RECOVERY_REQUIRED replica transitions back to READY after operator action);
  retention and expiration (messages expired by retention TTL, shown with the threshold);
  integration with planned M9 resilience experiments (the visualization serves as the
  operator-facing dashboard for fault-injection experiments: pod loss, partition, network
  impairment).
- **Acceptance criteria:** Every lifecycle state transition is visualized with its timestamp
  and reason (from telemetry, not inferred); epoch change is visible (the epoch consistency
  indicator flips from green to red); a replacement pod after pod loss is visible in the
  topology within one polling interval; the retention/expiration view shows the actual
  ExpirationThreshold and the count of expired records; the visualization degrades gracefully
  when lifecycle is disabled (lifecycle-specific panels are hidden, not shown as "unknown").

### Dependencies

- **M6** (browser UI, authentication) — the cluster view extends the existing web handler and
  reuses its authentication.
- **M4A** (peer status endpoint) — `GET /internal/status` must exist and be reachable over mTLS.
- **M5A** (lifecycle state machine, epoch fencing) — the cluster view displays lifecycle state
  and validates the epoch envelope on peer status responses (already implemented in the
  `peertls.Client`).
- **M5B Stage 2** (bootstrap admission) — **not** a dependency. The cluster view does not
  interact with bootstrap; it is pure observability.
- **M7A** (Kubernetes deployment) — the cluster view works in the staging cluster without any
  additional configuration; no new Helm values are required.

## ADR

See [ADR-0004: Cluster view as a composed read-only aggregation](decisions/0004-cluster-view.md)
for the decision record.
