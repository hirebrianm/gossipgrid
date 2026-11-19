# GossipGrid

**GossipGrid** is an intentionally small, distributed Go application for learning and demonstrating Kubernetes operations, secure service-to-service communication, and eventually consistent data replication.

> **Status: Milestone 1 implemented (November 2026).** A locally runnable single-instance Go service with configuration, HTTP health/ready/root endpoints, structured logging, graceful shutdown, a Makefile, a static `scratch` Docker image, and tests. The distributed behaviors (messages, SQLite, gossip, mTLS, synchronization, Kubernetes deployment, CI) are described in the design documents below and are **not yet implemented**. No application-level FIPS or production-readiness claims are made.

## What it will do

Run a configurable number of identical Go replicas (start with five). Each replica stores string messages in a local **GORM + SQLite** database, exposes a minimal web interface and REST API, discovers its peers, and gossips messages to other replicas over **mutually authenticated TLS**. Newly created pods initialize their local databases by synchronizing from a previously synchronized peer. The UI shows the serving pod, cluster membership, message availability, and security diagnostics.

There is intentionally **no central message broker, no centralized message database, and no consensus-based guarantee of durable writes**. Data is *eventually consistent*, and it is possible to lose messages. Understanding these tradeoffs is part of the project.

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

The workload is planned as a **StatefulSet without PVCs**: stable ordinal identities, but data stored in pod-lifetime `emptyDir` volumes. The peer Service will publish unready pod addresses to avoid bootstrap deadlock; only ready pods enter the user-facing Service.

## Principal design choices

| Area | Planned approach |
| --- | --- |
| Language | Go, standard library where practical |
| Database | GORM with the conventional CGO-backed SQLite driver |
| Local storage | `emptyDir`; survives container restart but not pod replacement |
| Scaling | StatefulSet, initially five replicas, `podManagementPolicy: Parallel` |
| Service discovery | Kubernetes headless Service DNS, dynamic refresh; API/EndpointSlice watch may be a later exercise |
| Replication | Small custom epidemic gossip protocol plus periodic anti-entropy |
| Bootstrap | Explicit `STARTING -> DISCOVERING -> SYNCING -> READY` lifecycle |
| Security | Separate Vault-managed browser and workload PKI; peer mTLS required |
| Certificate delivery | cert-manager Vault issuers, CSI per-pod keys/certs, trust bundle distribution |
| Network entry | Cilium Gateway API and LAN LoadBalancer, configured separately |
| Container | Multi-stage static build using Alpine/musl; final image `scratch` |
| FIPS | Pin a supported Go native FIPS module for the FIPS build profile; verify, don't assert app certification |
| Delivery | Makefile-based QA, GitLab CI, Helm deployment later |

## Project documentation

- [Architecture and components](docs/architecture.md)
- [Gossip, membership and synchronization](docs/gossip-and-bootstrap.md)
- [Data loss and recovery](docs/recovery.md)
- [Security, certificates and FIPS](docs/security.md)
- [Kubernetes and delivery design](docs/kubernetes-deployment.md)
- [Implementation milestones and review protocol](docs/development-plan.md)
- Architecture decision records: [ephemeral storage](docs/decisions/0001-ephemeral-storage.md), [peer discovery](docs/decisions/0002-native-peer-discovery.md), [static/FIPS build](docs/decisions/0003-static-and-fips.md).

## Development approach

Development is **incremental and reviewed**. One implementation prompt at a time will be sent to a locally running AI; its code and test results will be reviewed before the next prompt. Do not generate full application scaffolding or unreviewed deployment manifests as part of this documentation-only first commit.

## Local development

Prerequisites: a Go 1.27+ toolchain, `golangci-lint` v2, `gosec` v2, and (for the Docker image) Docker with a working daemon. Install compatible versions of the linters on `PATH`; `make qa` will fail with an installation message if a required tool is missing.

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

The `make qa` target runs: `fmt-check` → `go vet` → `golangci-lint` → `gosec` → `go test` → `go test -race`. It fails if any check fails or if a required tool is missing.

Investigating findings:

- **golangci-lint** prints each finding with file, line, linter name, and description. Fix genuine issues directly. For false positives, add a `//nolint:lintername` comment on the affected line with a one-line explanation.
- **gosec** prints each finding with the rule ID (e.g. `G104: Err returned`), file, line, and description. Fix actionable issues. For justified false positives, add a `#nosec <rule-id>: <reason>` comment on the affected line.

The linter configuration lives in `.golangci.yml` (golangci-lint v2 format). gosec runs with its default rule set; no custom config file is used.

### FIPS 140-3 builds

Builds are **FIPS-enabled by default** using Go's native FIPS 140-3 support (`FIPS_MODULE=v1.0.0`). The startup log reports `fips_enabled` and `fips_module` so the actual runtime state is visible. If a FIPS-required build is run with FIPS disabled at runtime (e.g. `GODEBUG=fips140=off`), the application **refuses to start** — a runtime override must not silently disable a required security configuration.

To produce an ordinary non-FIPS development build:

```sh
make build FIPS_MODULE=off
make run   FIPS_MODULE=off
```

The `fips_enabled` field in the startup log will be `false` and `fips_module` will be `latest` for non-FIPS builds.

Run with custom configuration via environment variables:

```sh
GOSSIPGRID_HTTP_ADDR=:8080 \
GOSSIPGRID_NODE_ID=gossipgrid-0 \
GOSSIPGRID_LOG_LEVEL=debug \
GOSSIPGRID_SHUTDOWN_TIMEOUT=10s \
make run
```

Endpoints once running (default `http://127.0.0.1:8080`):

- `GET /` — HTML page identifying GossipGrid and the serving node
- `GET /healthz` — liveness (`{"service":"gossipgrid","status":"healthy"}`)
- `GET /readyz` — readiness (`{"service":"gossipgrid","ready":true}`)

The Docker image is a multistage static build with a final `scratch` stage and a non-root user:

```sh
make image
docker run --rm -p 8080:8080 --name gossipgrid gossipgrid:dev
```

## Next milestone

The single-replica foundation is in place. The next milestone is **local storage**: the GORM/SQLite message schema, the local REST API (`POST /api/messages`, `GET /api/messages`), and idempotent insert, verified by a test that a message survives a process restart with the same data directory. Remaining architecture questions (catastrophic-loss reset arbitration, certificate identity format, snapshot page boundaries, FIPS toolchain pinning) will be resolved in subsequent reviews.

## Intended scope

This is a **learning and portfolio demonstration**, not a durable production message store. No uptime or FIPS compliance certification claims are made. Future experiments include DNS/EndpointSlice discovery, pod rollouts, partitions, cert rotation, RBAC, network policies, load balancing, scaling, and recovery.

## License

Not chosen yet; no license is granted by the presence of this repository. Select a license before public distribution.
