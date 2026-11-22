# GossipGrid

**A Kubernetes distributed systems testbed.**

A Go-based distributed systems testbed for evaluating Kubernetes orchestration, service discovery, gossip protocols,
mTLS, fault tolerance, and automated deployments.

> **Status: Milestones 4A, 4B, 4C, and 5A implemented (November 2026).** A locally runnable single-instance Go service with
> configuration, HTTP health/ready/root endpoints, a GORM/SQLite local message store with REST API, peer discovery
> (static and Kubernetes DNS SRV), **authenticated peer-to-peer mTLS transport with certificate hot-reload**
> (Milestone 3B), **authenticated peer message exchange (POST /internal/messages, opt-in)** (Milestone 4A),
> **automatic epidemic gossip fanout with bounded queues, exponential-backoff retries, and duplicate suppression
> (opt-in)** (Milestone 4B), **periodic anti-entropy synchronization with inventory comparison, keyset pagination,
> and cursor progression (opt-in)** (Milestone 4C), and a **lifecycle state machine with durable SQLite epoch
> binding, opt-in readiness gating, epoch fencing (request headers + response envelope), single-snapshot
> fail-closed admission, a non-recursive commit guard with a fixed lock order, outbound epoch provider (per-send
> re-check), and legacy-peer rejection (opt-in)** (Milestone 5A). Gossip and anti-entropy are implemented and
> operational in standalone multi-process deployments. Structured logging, graceful shutdown, a Makefile, a static
> `scratch` Docker image, GitLab CI, GitHub Actions, and tests are in place. Milestone 5A provides the **fencing
> and admission framework** and an **implemented RECOVERY_REQUIRED lockout** (a fail-closed state on epoch
> mismatch that preserves data and rejects all writes/replication); it does **not** provide the **operator-
> authorized recovery/reset procedure** that returns a locked-out replica to READY, nor automatic cluster genesis,
> initial snapshot transfer, or the Kubernetes metadata provider — those are planned milestones (Milestones 5B, 6,
> 7, and 9). 5A alone does not make GossipGrid safe for catastrophic cluster recovery. No application-level FIPS
> certification or production-readiness claims are made.

## What it does (planned)

Run a configurable number of identical Go replicas (target: five). Each replica stores string messages in a local
**GORM + SQLite** database, exposes a minimal web interface and REST API, discovers its peers, and gossips messages to
other replicas over **mutually authenticated TLS**. Newly created pods initialize their local databases by
synchronizing from a previously synchronized peer. The UI shows the serving pod, cluster membership, message
availability, and security diagnostics.

There is intentionally **no central message broker, no centralized message database, and no consensus-based guarantee
of durable writes**. Data is *eventually consistent*, and it is possible to lose messages. These tradeoffs are a
deliberate part of the testbed design.

## What is implemented

| Area | Status |
| --- | --- |
| Single-replica Go service | ✅ |
| Environment-based configuration | ✅ |
| HTTP health/ready/root endpoints | ✅ |
| GORM + SQLite local message store | ✅ |
| REST API (POST/GET /api/messages, GET /api/messages/{id}) | ✅ |
| UUIDv7 message identifiers | ✅ |
| Idempotent replicated message insertion | ✅ |
| Structured logging (slog) | ✅ |
| Graceful shutdown | ✅ |
| FIPS 140-3 build enforcement | ✅ |
| Git-based build versioning | ✅ |
| Static `scratch` Docker image (non-root) | ✅ |
| Makefile (build, test, QA, image) | ✅ |
| GitLab CI pipeline | ✅ |
| GitHub Actions CI | ✅ |
| Peer discovery (static mode) | ✅ |
| Peer discovery (DNS SRV mode) | ✅ |
| Membership tracking and reconciliation | ✅ |
| Peer mTLS transport (private HTTPS listener) | ✅ |
| Peer certificate identity authorization (SAN, EKU, StatefulSet name) | ✅ |
| Certificate and trust-bundle hot-reload without restart | ✅ |
| Authenticated peer HTTP client (dial address vs expected identity) | ✅ |
| Private `/internal/status` endpoint (mTLS-protected) | ✅ |
| Authenticated peer message exchange (POST /internal/messages, opt-in) | ✅ |
| Epidemic gossip (automatic fanout, bounded queues, retries, duplicate suppression) | ✅ |
| Anti-entropy (periodic inventory comparison, keyset pagination, cursor progression) | ✅ |
| Lifecycle state machine (7 states, concurrency-safe, opt-in) | ✅ |
| Durable SQLite epoch binding (local_metadata singleton row) | ✅ |
| Readiness gating (503 when unready, 200 for READY/DEGRADED) | ✅ |
| Epoch fencing (request headers + response envelope, 409 on mismatch, permanent) | ✅ |
| Single-snapshot fail-closed admission (reads, writes, replication; per-send re-check) | ✅ |
| Outbound epoch provider (current authorization, not stale) | ✅ |
| Legacy-peer rejection (response envelope validation; mTLS alone does not prove epoch membership) | ✅ |
| Initial snapshot transfer and automatic genesis | 🔜 Milestone 5B |
| Web UI and diagnostics | 🔜 Milestone 6 |
| Kubernetes packaging (Helm) | 🔜 Milestone 7 |
| Fault injection and resilience lab | 🔜 Milestone 9 |

## Architecture at a glance

```text
Browser (internal-CA HTTPS)
          |
   Cilium Gateway (TLS termination / LAN LoadBalancer)
          |
   gossipgrid-web Service (ready pods only)
          |
    Go HTTP API + minimal HTML UI (port 8080)
          |
  ---------------------------------------------
  | Pod 0 | Pod 1 | Pod 2 | Pod 3 | Pod 4 |  ...
  | SQLite emptyDir databases, independent    |
  | HTTPS gossip + peer sync over mTLS :8443  |
  ---------------------------------------------
          |
  Headless peer Service / Kubernetes DNS

Vault PKI -> cert-manager -> Gateway HTTPS certificate
Separate workload PKI -> cert-manager CSI -> per-pod mTLS certificates
```

The workload is planned as a **StatefulSet without PVCs**: stable ordinal identities, but data stored in pod-lifetime
`emptyDir` volumes. The peer Service will publish unready pod addresses to avoid bootstrap deadlock; only ready pods
enter the user-facing Service.

## Principal design choices

| Area | Approach |
| --- | --- |
| Language | Go, standard library where practical |
| Database | GORM with the conventional CGO-backed SQLite driver |
| Local storage | `emptyDir`; survives container restart but not pod replacement |
| Scaling | StatefulSet, initially five replicas, `podManagementPolicy: Parallel` |
| Service discovery | Kubernetes headless Service DNS, dynamic refresh |
| Replication | Small custom epidemic gossip protocol plus periodic anti-entropy |
| Bootstrap | Explicit `STARTING -> DISCOVERING -> SYNCING -> READY` lifecycle |
| Security | Separate Vault-managed browser and workload PKI; peer mTLS required |
| Certificate delivery | cert-manager Vault issuers, CSI per-pod keys/certs, trust bundle distribution |
| Network entry | Cilium Gateway API and LAN LoadBalancer |
| Container | Multi-stage static build; final image `scratch`, non-root user |
| FIPS | Go native FIPS 140-3 module; verify, don't assert app certification |
| CI/CD | GitLab CI (primary), GitHub Actions (public mirror) |

## Project documentation

- [Architecture and components](docs/architecture.md)
- [Gossip, membership and synchronization](docs/gossip-and-bootstrap.md)
- [Data loss and recovery](docs/recovery.md)
- [Security, certificates and FIPS](docs/security.md)
- [Kubernetes and delivery design](docs/kubernetes-deployment.md)
- [Implementation milestones and review protocol](docs/development-plan.md)
- Architecture decision records: [ephemeral storage](docs/decisions/0001-ephemeral-storage.md),
  [peer discovery](docs/decisions/0002-native-peer-discovery.md),
  [static/FIPS build](docs/decisions/0003-static-and-fips.md).

## Local development

Prerequisites: a Go 1.27+ toolchain, `golangci-lint` v2, `gosec` v2, and (for the Docker image) Docker with a working
daemon.

```sh
make build       # compile the binary into ./bin/gossipgrid (FIPS-enabled by default)
make run         # build and run locally (default config: :8080, hostname node)
make qa          # verify fmt, vet, lint, security, unit tests, race detection
make lint        # run golangci-lint (fails on findings)
make security    # run gosec security scan (fails on actionable findings)
make test        # unit tests only
make test-race   # unit tests with race detection
make fmt         # format Go sources
make image       # build the static Docker image
make version     # show the resolved build version, FIPS module, and image reference
make clean       # remove build outputs
```

The `make qa` target runs: `fmt-check` → `go vet` → `golangci-lint` → `gosec` → `docs-lint` → `go test` → `go test -race`.

### FIPS 140-3 builds

Builds are **FIPS-enabled by default** using Go's native FIPS 140-3 support (`FIPS_MODULE=v1.0.0`). The startup log
reports `fips_enabled` and `fips_module`. If a FIPS-required build is run with FIPS disabled at runtime, the
application **refuses to start**.

```sh
make build FIPS_MODULE=off   # non-FIPS development build
```

### Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `GOSSIPGRID_HTTP_ADDR` | `:8080` | HTTP listener address |
| `GOSSIPGRID_NODE_ID` | hostname | Node identity |
| `GOSSIPGRID_LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |
| `GOSSIPGRID_SHUTDOWN_TIMEOUT` | `10s` | Graceful shutdown grace period |
| `GOSSIPGRID_DB_PATH` | `./data/gossipgrid.db` | SQLite database path |
| `GOSSIPGRID_DISCOVERY_MODE` | `off` | Discovery mode: `off`, `static`, or `dns` |
| `GOSSIPGRID_STATIC_PEERS` | (empty) | Comma-separated `host:port` peer list (static mode) |
| `GOSSIPGRID_DISCOVERY_DNS_NAME` | (empty) | Headless Service DNS name (dns mode, e.g. `gossipgrid-headless.default.svc`) |
| `GOSSIPGRID_DISCOVERY_SRV_SERVICE` | `peer` | Named port in SRV records (dns mode) |
| `GOSSIPGRID_DISCOVERY_INTERVAL` | `10s` | DNS refresh period (dns mode) |
| `GOSSIPGRID_DISCOVERY_TIMEOUT` | `3s` | Per-lookup DNS timeout (dns mode) |
| `GOSSIPGRID_EXPECTED_REPLICAS` | `1` | Configured cluster size (StatefulSet replicas) |
| `GOSSIPGRID_STATEFULSET_NAME` | `gossipgrid` | StatefulSet name for deriving expected replica identities |
| `GOSSIPGRID_PEER_TLS_ENABLED` | `false` | Enable the private peer mTLS listener |
| `GOSSIPGRID_PEER_ADDR` | `:8443` | Private peer HTTPS listener address |
| `GOSSIPGRID_PEER_CERT_FILE` | (empty) | Path to the mounted certificate chain (PEM) |
| `GOSSIPGRID_PEER_KEY_FILE` | (empty) | Path to the mounted private key (PEM) |
| `GOSSIPGRID_PEER_TRUST_BUNDLE_FILE` | (empty) | Path to the dedicated workload CA trust bundle (PEM) |
| `GOSSIPGRID_PEER_IDENTITY_SUFFIX` | (empty) | DNS suffix following the pod name in the cert SAN (e.g. `gossipgrid-headless.default.svc.cluster.local`) |
| `GOSSIPGRID_PEER_TLS_RELOAD_INTERVAL` | `2s` | Polling interval for credential file changes |
| `GOSSIPGRID_PEER_MESSAGE_EXCHANGE_ENABLED` | `false` | Enable the private peer message exchange endpoint (POST /internal/messages). Requires peer mTLS |
| `GOSSIPGRID_GOSSIP_ENABLED` | `false` | Enable automatic epidemic gossip fanout (Milestone 4B). Requires peer mTLS, message exchange, and static or dns discovery |
| `GOSSIPGRID_GOSSIP_FANOUT` | `2` | Peers contacted per propagation round (range 1-32) |
| `GOSSIPGRID_GOSSIP_ROUNDS` | `3` | Fanout rounds per message (range 1-32) |
| `GOSSIPGRID_GOSSIP_WORKERS` | `4` | Concurrent delivery workers (range 1-64) |
| `GOSSIPGRID_GOSSIP_QUEUE_CAPACITY` | `256` | Work queue capacity (range 1-1024) |
| `GOSSIPGRID_GOSSIP_MAX_ATTEMPTS` | `3` | Total delivery attempts per peer, including initial (range 1-10) |
| `GOSSIPGRID_GOSSIP_RETRY_BASE` | `250ms` | Base duration for exponential backoff (must be positive) |
| `GOSSIPGRID_GOSSIP_RETRY_MAX` | `5s` | Maximum backoff duration (must be >= retry base) |
| `GOSSIPGRID_ANTI_ENTROPY_ENABLED` | `false` | Enable periodic anti-entropy reconciliation (Milestone 4C). Requires peer mTLS, message exchange, and static or dns discovery |
| `GOSSIPGRID_ANTI_ENTROPY_INTERVAL` | `30s` | Synchronization cycle period (range 1s–10m) |
| `GOSSIPGRID_ANTI_ENTROPY_PAGE_SIZE` | `128` | Maximum IDs per inventory page (range 1–1024) |
| `GOSSIPGRID_ANTI_ENTROPY_FETCH_BATCH_SIZE` | `8` | Maximum messages fetched per batch (range 1–8, ≤ page size) |
| `GOSSIPGRID_ANTI_ENTROPY_MAX_PAGES_PER_PEER` | `4` | Maximum inventory pages per peer per cycle (range 1–64) |
| `GOSSIPGRID_ANTI_ENTROPY_WORKERS` | `2` | Concurrent synchronization workers (range 1–16) |
| `GOSSIPGRID_ANTI_ENTROPY_REQUEST_TIMEOUT` | `5s` | Per-request timeout for sync HTTP calls (range 1ms–60s) |
| `GOSSIPGRID_LIFECYCLE_ENABLED` | `false` | Enable lifecycle state machine, epoch fencing, and fail-closed admission (Milestone 5A). Requires peer mTLS |

When `GOSSIPGRID_PEER_TLS_ENABLED` is `true`, all of the peer TLS file paths and the identity suffix are **required**; a
missing or malformed value fails startup clearly (no silent fallback to unauthenticated communication). When it is
`false` (the default), the peer listener is not started and local single-instance development is unchanged.

When `GOSSIPGRID_PEER_MESSAGE_EXCHANGE_ENABLED` is `true`, the private peer listener additionally registers
`POST /internal/messages`, accepting idempotent replicated message inserts from authenticated replicas. This is a
provisional, opt-in, direct-exchange capability: it does **not** by itself enable readiness gating. It
**requires** `GOSSIPGRID_PEER_TLS_ENABLED=true`; enabling it without peer mTLS fails configuration validation.
Automatic epidemic gossip fanout is enabled separately via `GOSSIPGRID_GOSSIP_ENABLED` (Milestone 4B, implemented).
Periodic anti-entropy synchronization is enabled separately via `GOSSIPGRID_ANTI_ENTROPY_ENABLED` (Milestone 4C,
implemented); it uses the message-exchange endpoint for inventory comparison and batched fetches.

### Lifecycle and epoch fencing (Milestone 5A)

When `GOSSIPGRID_LIFECYCLE_ENABLED=true` (and peer mTLS is enabled), the application enforces a lifecycle state
machine that gates readiness, admission, and epoch fencing. The feature is **opt-in**; when disabled (the default),
all existing behavior is preserved.

**What it provides:**

- A **concurrency-safe state machine** with 7 states (`STARTING`, `DISCOVERING`, `GENESIS`, `SYNCING`, `READY`,
  `DEGRADED`, `RECOVERY_REQUIRED`). Invalid transitions are rejected; no caller can silently force `READY`.
- A **durable SQLite epoch binding** (singleton row in `local_metadata`) that ties the database to a specific
  cluster ID and epoch ID. Rebinding to a different epoch fails; a nonempty database without a binding is rejected.
- **Readiness gating**: `GET /readyz` returns 503 when the replica is not in `READY` or `DEGRADED` state.
  `GET /healthz` remains 200.
- **Fail-closed admission**: `POST /api/messages` returns 503 (no message inserted) when the replica is unready.
  Private replication (inbound gossip, anti-entropy) is rejected when unready.
- **Epoch fencing**: peer status and sync endpoints carry request headers
  (`X-GossipGrid-Cluster-ID`, `X-GossipGrid-Epoch-ID`) and a response envelope
  (`X-GossipGrid-Resp-Cluster-ID`, `X-GossipGrid-Resp-Epoch-ID`,
  `X-GossipGrid-Lifecycle-Active`). A replica whose local binding does not match the peer's epoch receives 409
  (permanent, no retry) and does not merge data. A lifecycle-enabled client additionally validates the response
  envelope: a legacy peer (lifecycle disabled) that omits `X-GossipGrid-Lifecycle-Active: true` is rejected as a
  synchronization source, even with a valid mTLS certificate.
- **RECOVERY_REQUIRED lockout (implemented)**: when the local database binding does not match the authoritative
  metadata (an epoch mismatch), the replica transitions to `RECOVERY_REQUIRED` — a **fail-closed lockout**. All
  writes and replication are rejected (503 unready / 409 epoch mismatch), local data is **preserved** (never
  deleted), and the binding row is intact. This is the lockout *itself*; it is distinct from the **operator-
  authorized recovery/reset procedure** that actually returns the replica to `READY`, which is **not** implemented
  in 5A (deferred to 5B and later). A `RECOVERY_REQUIRED` replica cannot automatically return to `READY`.
- **Outbound epoch provider**: the peer client sources its outbound epoch context from the controller's current
  authorization decision (via the `EpochProvider`), not a one-time startup copy. A revoked node blocks its outbound
  requests rather than using a stale epoch. Gossip and anti-entropy re-check admission at send/sync time; there is
  no retry on a revocation.
- **Commit authorization (lock-order guarantee)**: the anti-entropy cursor commit
  (`Controller.CommitUnderAuthorization`) evaluates the complete authorization decision — ready state,
  not stopped, fresh valid metadata with `Initialized=true`, and a matching durable binding with no
  recovery lockout — from a single locked snapshot that reads all authorization fields under one
  controller read lock and one state-machine read lock (no second lock acquisition, no SQLite read
  while the locks are held). The RLocks are held until the in-memory cursor mutation completes, so a
  revocation (Stop or a state/metadata change) that linearizes before the commit is observed and the
  callback is not executed; a revocation that linearizes after the callback completes is a normal,
  permitted revocation. All lifecycle state transitions acquire the controller read lock before the
  state-machine write lock, matching the commit's lock order, so a readiness-revoking transition cannot
  linearize while a cursor commit holds the RLocks: the transition is queued behind the commit. Lock
  order is `Controller.mu` (RLock) → `Machine.mu` (RLock) → `Engine.mu` (Lock). A cached binding that
  is absent or does not match the accepted metadata fails closed (no database read inside the protected
  commit). A completed revocation prevents a subsequent commit; a commit that linearizes before a
  revocation may complete normally.

**What it does NOT provide (deferred to Milestone 5B and later):**

- No production Kubernetes metadata provider (the controller uses a no-op source that always reports metadata
  unavailable; replicas with lifecycle enabled fail closed).
- No automatic cluster genesis (a fresh replica with no binding cannot automatically initialize an empty epoch).
- No initial snapshot transfer (the path from `SYNCING` to `READY` is not yet implemented).
- No automatic epoch reset or operator reset endpoints.
- mTLS alone does **not** prove a peer's epoch membership; the response envelope is a second, independent check.
- 5A alone does **not** make GossipGrid safe for catastrophic cluster recovery.

**Local testing.** The `cmd/gossipgrid` package includes controller-driven integration tests
(`TestControllerDriven*` — 14 scenarios covering matching/missing/invalid metadata, epoch A→B transitions,
revocation fencing, legacy-peer rejection, and lifecycle-disabled convergence) and real-mTLS mixed-mode tests
(`TestMixedMode*` — 7 scenarios) that start GossipGrid nodes with lifecycle enabled, distinct identities, and
loopback mTLS listeners. All scenarios pass under the race detector.

**Security note.** mTLS remains the sole basis for peer identity. Epoch headers are fencing metadata, not
credentials: possessing an epoch ID does not grant access to any resource. A peer with a valid mTLS certificate
but a different epoch is rejected (409) — its data is not merged. mTLS alone does not prove epoch membership;
the response envelope provides a second, independent validation layer.

### Endpoints

- `GET /` — HTML page identifying GossipGrid and the serving node
- `GET /healthz` — liveness
- `GET /readyz` — readiness
- `POST /api/messages` — create a local message (body only)
- `GET /api/messages` — list local messages (newest first, paginated)
- `GET /api/messages/{id}` — retrieve a single message

### Peer discovery

GossipGrid supports three discovery modes. Discovery is **observational**: a failed DNS
lookup does not affect the public HTTP service's readiness. When gossip is enabled
(`GOSSIPGRID_GOSSIP_ENABLED=true`), the discovery snapshot feeds the gossip engine's
peer selector through the production bridge. When anti-entropy is enabled
(`GOSSIPGRID_ANTI_ENTROPY_ENABLED=true`), the same snapshot feeds the anti-entropy
synchronization worker's peer selection (Milestone 4C, implemented).

| Mode | Use case | Behavior |
| --- | --- | --- |
| `off` | Single-instance development | No discovery; no background activity |
| `static` | Local multi-process testing | Peers read from `GOSSIPGRID_STATIC_PEERS`; deterministic, no background network I/O |
| `dns` | Kubernetes deployment | Peers discovered from headless Service DNS SRV records; periodic refresh with bounded timeouts |

**Static mode example (five local processes):**

```sh
GOSSIPGRID_NODE_ID=gossipgrid-0 GOSSIPGRID_DISCOVERY_MODE=static \
  GOSSIPGRID_STATIC_PEERS="gossipgrid-1:8443,gossipgrid-2:8443,gossipgrid-3:8443,gossipgrid-4:8443" \
  GOSSIPGRID_EXPECTED_REPLICAS=5 gossipgrid

GOSSIPGRID_NODE_ID=gossipgrid-1 GOSSIPGRID_DISCOVERY_MODE=static \
  GOSSIPGRID_STATIC_PEERS="gossipgrid-0:8443,gossipgrid-2:8443,gossipgrid-3:8443,gossipgrid-4:8443" \
  GOSSIPGRID_EXPECTED_REPLICAS=5 gossipgrid
```

**DNS mode example (Kubernetes):**

```sh
GOSSIPGRID_NODE_ID=gossipgrid-0 GOSSIPGRID_DISCOVERY_MODE=dns \
  GOSSIPGRID_DISCOVERY_DNS_NAME="gossipgrid-headless.default.svc" \
  GOSSIPGRID_EXPECTED_REPLICAS=5 gossipgrid
```

The headless Service must have a named TCP port called `peer` on port 8443 and
`publishNotReadyAddresses: true`. No Kubernetes API permissions are required for peer discovery.
DNS records are not proof that a peer is healthy, reachable, authenticated, synchronized, or ready to
supply data; those properties are established by the mTLS and lifecycle implementations in later milestones.

### Peer mTLS

GossipGrid runs a **separate private HTTPS peer listener** (default `:8443`) in addition to the public HTTP listener
(`:8080`). The peer listener:

- enforces **TLS 1.3 only** (no 1.2 or lower);
- **requires and verifies client certificates** (mutual TLS) signed by the dedicated workload CA trust bundle;
- performs **additional peer identity authorization** after the standard TLS chain/hostname verification: the
  certificate must have exactly one DNS SAN of the form `<statefulset-name>-<ordinal>.<identity-suffix>` with a
  non-negative ordinal, and the correct Extended Key Usage (`clientAuth` for clients, `serverAuth` for servers);
- rejects wildcard SANs, IP SANs, multiple SANs, and the Common Name (SAN-only validation).

The **network dial address and the expected authenticated peer identity are distinct inputs**. A client may dial
`127.0.0.1:8443` (a loopback address for local testing) while verifying a certificate for
`gossipgrid-1.gossipgrid-headless.default.svc.cluster.local` (the in-cluster FQDN). This is the mechanism that lets the
application be tested as independent local processes without Kubernetes.

The peer listener exposes the following private endpoints (all mTLS-protected, none exposed on the public HTTP
router):

- `GET /internal/status` — returns the authenticated server's node identity, application version, pod identity, and a
  safe certificate fingerprint (no private key or PEM content). Always available (200 even when unready).
- `POST /internal/messages` — (opt-in via `GOSSIPGRID_PEER_MESSAGE_EXCHANGE_ENABLED`) accepts an authenticated peer's
  message for idempotent replicated storage.
- `GET /internal/sync/ids` — (opt-in via `GOSSIPGRID_ANTI_ENTROPY_ENABLED`) returns a bounded, keyset-paginated page
  of message IDs for inventory comparison.
- `POST /internal/sync/messages` — (opt-in via `GOSSIPGRID_ANTI_ENTROPY_ENABLED`) fetches a bounded batch of messages
  by ID for anti-entropy reconciliation.

When lifecycle protection is enabled (`GOSSIPGRID_LIFECYCLE_ENABLED=true`), all private replication endpoints carry
epoch fencing headers (`X-GossipGrid-Cluster-ID`, `X-GossipGrid-Epoch-ID`) and a response envelope
(`X-GossipGrid-Resp-Cluster-ID`, `X-GossipGrid-Resp-Epoch-ID`, `X-GossipGrid-Lifecycle-Active`); a 409 epoch mismatch
is permanent (no retry).

**Certificate identity convention.** Each replica's certificate DNS identity is
`<replica>.<GOSSIPGRID_PEER_IDENTITY_SUFFIX>`, for example
`gossipgrid-0.gossipgrid-headless.default.svc.cluster.local`. The cluster DNS domain is **not** hard-coded; the
operator configures the full suffix. The same certificate is used for both the server and client roles, so it must
carry both `serverAuth` and `clientAuth` Extended Key Usages.

**Certificate rotation (hot-reload).** The application polls the certificate, key, and trust-bundle files at
`GOSSIPGRID_PEER_TLS_RELOAD_INTERVAL` (default `2s`). On a successful reload the new credentials are published
atomically and used for **new** handshakes without a process restart. A failed reload (malformed or missing file)
preserves the previous valid configuration; it never exposes partially loaded credentials or replaces a valid trust
bundle with malformed material. The trust bundle may contain multiple CAs during a CA transition. Existing
established connections are not forcibly terminated during normal leaf-certificate rotation. Reload outcomes are
reported through structured logs and the `Diagnostics` API (cert fingerprint, pod ID, load time, reload count — no
secrets). This mirrors the cert-manager CSI + trust-manager rotation flow planned for the Kubernetes deployment.

**Local testing.** The `cmd/gossipgrid` package includes a repeatable integration test
(`TestIntegrationTwoNodesMTLS`) that starts two GossipGrid nodes with distinct identities, SQLite databases, and
loopback mTLS listeners, and demonstrates: successful authenticated `GET /internal/status`, failure on a wrong
expected peer identity, failure with an untrusted client certificate, and successful credential rotation without
restart. Run it with:

```sh
make test-race
# or just the integration test:
GOFIPS140=v1.0.0 CGO_ENABLED=1 go test -tags sqlite_omit_load_extension -race -run TestIntegration ./cmd/gossipgrid/
```

No development certificates or private keys are committed: all test material is generated in memory per run.

### Authenticated peer message exchange (Milestone 4A)

When `GOSSIPGRID_PEER_MESSAGE_EXCHANGE_ENABLED=true` (and peer mTLS is enabled), the private peer listener registers:

- `POST /internal/messages` — accepts an authenticated peer's message for idempotent replicated storage.

**Wire contract.** The request body is a JSON envelope containing a complete message:

```json
{
  "message": {
    "id": "<uuidv7>",
    "body": "Hello from gossipgrid-0",
    "origin": "gossipgrid-0",
    "created_at": "2026-11-20T12:34:56.789Z"
  }
}
```

The receiver preserves the original UUIDv7 ID, body, origin, and creation timestamp — it does **not** generate a new
ID or timestamp during replication. Validation enforces:

- Maximum request body size of 8192 bytes.
- Maximum message body size of 4096 bytes (the store's limit).
- Valid canonical UUIDv7 (lowercase, hyphenated).
- Nonempty origin and message body.
- Valid nonzero timestamp (RFC3339).
- Exactly one JSON document; no unknown JSON fields.
- Appropriate Content-Type handling (application/json).

**Idempotency.** The receiver uses the existing atomic SQLite `ON CONFLICT DO NOTHING` insert:

- New valid message: inserted successfully (HTTP 200).
- Identical duplicate ID and content: success, no modification (HTTP 200).
- Duplicate ID with conflicting content: HTTP 409 Conflict; the original row is never overwritten.
- Invalid message: HTTP 400 or 413 as appropriate.
- Storage failure: HTTP 500.

**Security.** The authenticated TLS client identity establishes who transmitted a message; the receiver does **not**
accept a supplied HTTP header as proof of peer identity. The message's `origin` field is immutable metadata
identifying its original creator, which may differ from the forwarding peer. mTLS alone does **not**
cryptographically prove the historical origin of a forwarded message.

**Scope of 4A.** This is a provisional, direct, caller-initiated exchange. At the time of Milestone 4A, automatic
gossip fanout, bounded queues, retry scheduling, anti-entropy, snapshot transfer, epoch management, and
production cluster-reset guarantees were not yet implemented. Milestones 4B and 4C (both completed 2026-11-20)
subsequently implemented automatic epidemic gossip fanout and periodic anti-entropy reconciliation on top of this
endpoint. When `GOSSIPGRID_GOSSIP_ENABLED=true` (Milestone 4B, opt-in), successful local writes
(`POST /api/messages`) **do** initiate automatic propagation to eligible peers. When
`GOSSIPGRID_ANTI_ENTROPY_ENABLED=true` (Milestone 4C, opt-in), periodic inventory comparison and record retrieval
repair missed gossip. Both features are opt-in and require this endpoint plus peer mTLS.

**Local testing.** The `cmd/gossipgrid` package includes a repeatable integration test
(`TestIntegrationTwoNodesMessageExchange`) that starts two GossipGrid nodes with distinct identities, SQLite
databases, and loopback mTLS listeners, and demonstrates the full message-exchange flow including idempotency,
conflict rejection, and unauthorized-peer rejection. Run it with:

```sh
GOFIPS140=v1.0.0 CGO_ENABLED=1 go test -tags sqlite_omit_load_extension -race -run TestIntegrationTwoNodesMessageExchange ./cmd/gossipgrid/
```

The Docker image is a multistage static build with a final `scratch` stage and a non-root user (UID 65534):

```sh
make image
docker run --rm -p 8080:8080 --name gossipgrid gossipgrid:dev
```

## CI/CD

- **GitLab CI** (`.gitlab-ci.yml`): primary pipeline — QA (fmt, vet, lint, security, tests, race detection) and binary
  build. FIPS-enabled by default. Optional manual Docker image build for tags.
- **GitHub Actions** (`.github/workflows/ci.yml`): public mirror pipeline — same QA and build steps. No Docker image
  builds.

## Scope

GossipGrid is a **distributed systems testbed**, not a durable production message store. It is designed for
infrastructure testing, Kubernetes validation, fault-tolerance testing, and security validation. No uptime or FIPS
compliance certification claims are made. The project is not production-ready.

## License

[Apache License, Version 2.0](LICENSE). See the [LICENSE](LICENSE) file for the full text.
