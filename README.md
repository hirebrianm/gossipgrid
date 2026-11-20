# GossipGrid

**A Kubernetes distributed systems testbed.**

A Go-based distributed systems testbed for evaluating Kubernetes orchestration, service discovery, gossip protocols,
mTLS, fault tolerance, and automated deployments.

> **Status: Milestone 4A implemented (November 2026).** A locally runnable single-instance Go service with configuration,
> HTTP health/ready/root endpoints, a GORM/SQLite local message store with REST API, peer discovery (static and
> Kubernetes DNS SRV), **authenticated peer-to-peer mTLS transport with certificate hot-reload**, **authenticated
> peer message exchange (POST /internal/messages, opt-in)**, structured logging, graceful shutdown, a Makefile, a
> static `scratch` Docker image, GitLab CI, GitHub Actions, and tests. Automatic gossip fanout, anti-entropy,
> multi-node synchronization, and Kubernetes deployment are **not yet implemented** and remain planned milestones
> (Milestones 4B, 4C, 5, and 7). No application-level FIPS certification or production-readiness claims are made.

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
| Gossip replication protocol (automatic fanout, retries, anti-entropy) | 🔜 Milestone 4B/4C |
| Bootstrap/sync lifecycle | 🔜 Milestone 5 |
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

When `GOSSIPGRID_PEER_TLS_ENABLED` is `true`, all of the peer TLS file paths and the identity suffix are **required**; a
missing or malformed value fails startup clearly (no silent fallback to unauthenticated communication). When it is
`false` (the default), the peer listener is not started and local single-instance development is unchanged.

When `GOSSIPGRID_PEER_MESSAGE_EXCHANGE_ENABLED` is `true`, the private peer listener additionally registers
`POST /internal/messages`, accepting idempotent replicated message inserts from authenticated replicas. This is a
provisional, opt-in, direct-exchange capability: it does **not** enable automatic gossip, fanout, anti-entropy, or
readiness gating (those belong to Milestones 4B, 4C, and 5). It **requires** `GOSSIPGRID_PEER_TLS_ENABLED=true`;
enabling it without peer mTLS fails configuration validation.

### Endpoints

- `GET /` — HTML page identifying GossipGrid and the serving node
- `GET /healthz` — liveness
- `GET /readyz` — readiness
- `POST /api/messages` — create a local message (body only)
- `GET /api/messages` — list local messages (newest first, paginated)
- `GET /api/messages/{id}` — retrieve a single message

### Peer discovery

GossipGrid supports three discovery modes. Discovery is **observational** in this milestone: a failed DNS
lookup does not affect the public HTTP service's readiness, and no peer connections are initiated.
Peer connections (mTLS, gossip, synchronization) are implemented in subsequent milestones.

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

For this milestone the peer listener exposes a single private endpoint:

- `GET /internal/status` — returns the authenticated server's node identity, application version, pod identity, and a
  safe certificate fingerprint (no private key or PEM content). This endpoint is **not** exposed on the public HTTP
  router and requires a successful client-certificate handshake.

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

**Scope limitation.** This is a provisional, direct, caller-initiated exchange. It does **not** implement automatic
gossip fanout, bounded queues, retry scheduling, anti-entropy, snapshot transfer, epoch management, or production
cluster-reset and epoch-isolation guarantees (those belong to Milestones 4B, 4C, and 5). `POST /api/messages` does
**not** automatically replicate in this milestone.

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
