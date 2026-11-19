# GossipGrid

**GossipGrid** is an intentionally small, distributed Go application for learning and demonstrating Kubernetes operations, secure service-to-service communication, and eventually consistent data replication.

> **Status: design baseline only (November 2026).** No application, Helm chart, or CI pipeline has been implemented. Documentation describes the target behavior; it is not a claim of working functionality or production readiness.

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

## Next milestone

Agree on remaining implementation details (notably catastrophic-loss reset arbitration and exact certificate identity format), then implement a locally runnable single-instance Go service with health probes, configuration, Makefile, and tests. Kubernetes installation and configuration will be addressed in a separate discussion.

## Intended scope

This is a **learning and portfolio demonstration**, not a durable production message store. No uptime or FIPS compliance certification claims are made. Future experiments include DNS/EndpointSlice discovery, pod rollouts, partitions, cert rotation, RBAC, network policies, load balancing, scaling, and recovery.

## License

Not chosen yet; no license is granted by the presence of this repository. Select a license before public distribution.
