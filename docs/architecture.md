# Architecture

**Status:** Approved direction; protocol details may evolve during implementation review.

## Purpose

GossipGrid is a Kubernetes distributed systems testbed: a Go-based application for evaluating Kubernetes orchestration,
service discovery, gossip protocols, mTLS, fault tolerance, and automated deployments. It is not a production message
service.

## Goals

1. Evaluate Kubernetes concepts (DNS, EndpointSlices, readiness, pod scheduling, PKI, cert rotation, LB/Gateway,
   ephemeral storage, CI/CD) through a real, minimal application.
2. Test dynamic service discovery, secure mTLS, certificate rotation, eventual consistency, readiness, failure
   recovery, and deployment automation.
3. Keep the full application testable as five independent local processes without Kubernetes.
4. Provide a serious engineering reference for distributed replication and convergence patterns.

## Non-goals

- Strong consistency, distributed transactions, quorum writes, or a durable message service.
- Production-grade disaster recovery of message bodies.
- Installing and configuring cluster-wide components in early milestones.
- Claiming formal application-level FIPS 140-3 validation or production-readiness.

## Logical components

```mermaid
flowchart TB
  B[Browser] -->|HTTPS, browser CA| GW[Cilium Gateway]
  GW --> WEB[ClusterIP web Service]
  WEB --> A[Go Pod A]
  WEB --> C[Go Pod B]
  WEB --> D[Go Pod C]
  A <-->|HTTPS mTLS gossip / sync| C
  C <-->|HTTPS mTLS gossip / sync| D
  A <-->|HTTPS mTLS gossip / sync| D
  DNS[Headless Service / Kubernetes DNS] -. peer discovery .-> A
  DNS -. peer discovery .-> C
  DNS -. peer discovery .-> D
  A --- DB1[(SQLite emptyDir)]
  C --- DB2[(SQLite emptyDir)]
  D --- DB3[(SQLite emptyDir)]
  VAULT[Vault PKI] --> CM[cert-manager]
  CM --> GW
  CM --> CSI[CSI driver: pod certificates]
  CSI --> A
  CSI --> C
  CSI --> D
```

Three pods illustrated; default target is five, arbitrary count is configurable. All peer gossip paths are independent
of the public HTTP Service.

## Processes, storage, identity

- One process per pod, two listeners: public HTTP port `8080` (behind Gateway HTTPS) and private HTTPS peer port `8443`
  (mandatory mTLS).
- StatefulSet is used for predictable identity (`gossipgrid-0`, `gossipgrid-1`, ...), not durable data.
- One SQLite file per pod at `/data/gossipgrid.db` on disk-backed `emptyDir`. A container restart within the same pod
  retains the volume; pod replacement does not.
- GORM manages the schema; use WAL, busy timeout, transactions where appropriate, and unique message IDs.
- Web interface rendered by embedded Go templates and minimal JS; no Node/frontend pipeline.
- Health and security diagnostics do not need a ready peer to report useful information.

## Minimal message schema

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | UUIDv7 text, primary key | Stable identity for idempotent replication |
| `body` | Text, max 4096 bytes | Original message |
| `origin` | Text | Pod identity at creation |
| `created_at` | UTC timestamp | Original creation time, unchanged by replication |

Primary key conflict on replicated insert is a successful no-op. Message deletion and mutation are outside v1 scope.

## Application boundaries

**Public side:** `GET /`, `GET /api/messages`, `POST /api/messages`, `GET /api/messages/{id}/status`,
`GET /api/cluster`, `GET /healthz`, `GET /readyz`. API paths and response formats are provisional until implemented.

**Private side:** `GET /internal/status`, bounded message/snapshot endpoints, inbound gossip, and membership/probe
functionality. All private methods require authenticated client certificates and peer identity validation. Unready
pods answer status requests but do not supply authoritative synchronization snapshots.

## Expected topology

- Headless peer Service: DNS/SRV discovery; `publishNotReadyAddresses: true` for initialization.
- Web ClusterIP Service: routes to ready pods only.
- Gateway + LoadBalancer IP for browser access over HTTPS (LAN); exact Cilium capabilities to be verified in cluster
  setup.
- `podManagementPolicy: Parallel` avoids StatefulSet bootstrap deadlock caused by ordered readiness.

## Consistency, failure, and observability

Eventual convergence of messages is the target under stable connectivity and surviving source data. Posting a message
to one pod confirms a local write, **not replication durability**. Per-message status must report `present`,
`missing`, or `unreachable` for every expected replica, not just members presently discovered in DNS.

Readiness means the replica is initialized and can participate, not that every replica has identical messages right
now. A temporary peer outage degrades convergence but does not automatically fail the readiness probe of an otherwise
healthy initialized pod.

Metrics (later): gossip attempts/failures, convergence lag, sync state, peer count, SQLite failures, cert expiry, reset
incidents. Logs use Go `slog` with trace/correlation IDs where useful.

## Risks and open implementation questions

- Define a safe multi-page snapshot protocol and follow-up reconciliation in the presence of concurrent posts.
  Bounded scans of immutable messages are sufficient for the small target size; design tests for races.
- Peer membership changes when scaling down or up; distinguish desired replicas from reachable DNS records.
- The cluster metadata control path must safely prevent two competing reset epochs (Kubernetes resourceVersion
  compare-and-swap, restricted RBAC, no secret content).
- Choose DNS/URI SAN conventions based on deployed cert-manager CSI behavior; unit-test hostile identities.
- FIPS testing requires a pinned toolchain and module compatible with our supported build architectures; do not claim
  whole-cluster compliance.
