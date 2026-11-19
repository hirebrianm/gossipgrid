# GossipGrid

**A Kubernetes distributed systems testbed.**

A Go-based distributed systems testbed for evaluating Kubernetes orchestration, service discovery, gossip protocols,
mTLS, fault tolerance, and automated deployments.

> **Status: Milestone 2 implemented (November 2026).** A locally runnable single-instance Go service with configuration,
> HTTP health/ready/root endpoints, a GORM/SQLite local message store with REST API, structured logging, graceful
> shutdown, a Makefile, a static `scratch` Docker image, GitLab CI, GitHub Actions, and tests. Gossip replication, peer
> mTLS, multi-node synchronization, and Kubernetes deployment are **not yet implemented** and remain planned milestones.
> No application-level FIPS certification or production-readiness claims are made.

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
| Peer mTLS transport | 🔜 Milestone 3 |
| Gossip replication protocol | 🔜 Milestone 4 |
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

### Endpoints

- `GET /` — HTML page identifying GossipGrid and the serving node
- `GET /healthz` — liveness
- `GET /readyz` — readiness
- `POST /api/messages` — create a local message (body only)
- `GET /api/messages` — list local messages (newest first, paginated)
- `GET /api/messages/{id}` — retrieve a single message

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
