# Incremental development plan

**Working agreement:** The architecture is written first. Implementation proceeds one milestone at a time. After every
milestone the developer shares the result (code, logs, diffs, tests); architecture and QA are reviewed before
continuing.

## Milestones

| Phase | Deliverables | Acceptance signal |
| --- | --- | --- |
| 0. Documentation | README, architecture, ADRs, recovery and security models | Review and commit; no application code |
| 1. Single replica | Go main, env config, HTTP health probes, Makefile, static build, tests | `make qa` and local run |
| 2. Local storage | GORM/SQLite schema, local REST API, idempotent insert, tests | Message survives process restart with same data directory |
| 3A. Peer discovery and membership (✅ 2026-11-19) | Static peer discovery, DNS SRV resolver, membership tracking, reconciliation, tests | Local peer addresses discoverable; DNS errors observable without crash; race-safe |
| 3B. Peer mTLS (✅ 2026-11-19) | mTLS transport, cert hot reload, identity validation, private status endpoint, peer client | Negative TLS tests (20 scenarios); rotated certs/trust picked up by new connections; FIPS + race |
| 4A. Authenticated peer message exchange (✅ 2026-11-20) | POST /internal/messages on private peer listener, idempotent insert, SendMessage client, opt-in flag, two-node integration test | Explicit authenticated transfer between two local stores; idempotent; conflicts rejected; unauthorized peers rejected |
| 4B. Gossip fanout and retries (✅ 2026-11-20) | Epidemic fanout, bounded queues, duplicate suppression, exponential-backoff retries, five-node integration test | Five independent local stores converge under controlled healthy conditions; no unbounded forwarding loops |
| 4C. Anti-entropy (✅ 2026-11-20) | Periodic inventory comparison, bounded ID sets, keyset pagination, cursor progression, five-node integration test | Converges after partition recovery; no unbounded forwarding loops |
| 5. Lifecycle/bootstrap | Explicit state machine, initial snapshot, readiness, recovery lockout | Fresh process joins; unsynced pods never source snapshots |
| 6. UI and diagnostics | HTML templates, current pod, message status, cert status, incidents | Accurate displayed state under network failures |
| 7. Kubernetes packaging | Helm StatefulSet, Services, cert CSI, probes, Gateway resource integration | On-cluster smoke tests |
| 8. GitLab CI/CD | QA, image registry, lint, manual Helm deploy | Repeatable delivery with immutably tagged image |
| 9. Resilience/security lab | Fault injection, pod loss, reset authorization, metrics, FIPS profile | Logged and reproducible experiments |

The sequence is intentionally flexible: the exact bootstrap/recovery control path may need prototype tests before
Kubernetes packaging. No phase should compromise secure peer authentication merely to simplify debugging.

## Rules for local-AI implementation reviews

- Inspect the generated diff and explain what each package does; avoid architectural drift and framework sprawl.
- Run `go test`, `go test -race`, `go vet`, and integration tests as relevant; share failing output in full.
- Keep the Makefile authoritative for repeatable build/test operations and GitLab CI.
- No unreviewed addition of brokers, databases, cluster-wide operators, alternate SQLite drivers, or service meshes.
- Verify security properties with negative tests, not merely configuration inspection.
- Document changes to architecture via short ADR updates.

## Open items to resolve before implementation

1. Exact first-install genesis protocol and reset authorization/locking contract, including scale-down and lost
   metadata behavior.
2. Final pod certificate identity SAN conventions and Go-side authorization logic.
3. Snapshot page boundaries, handling messages arriving while scanning, queue and retry caps.
4. Pinned Go toolchain/FIPS module version and static build validation for chosen architectures.
5. Helm values and the local cluster's actual Cilium and certificate-management capabilities.
6. License and repository visibility.

## Purpose

GossipGrid is a distributed systems testbed for evaluating Kubernetes orchestration, service discovery, gossip
protocols, mTLS, fault tolerance, and automated deployments. It provides a real, minimal application for
infrastructure testing and Kubernetes validation — not a synthetic collection of infrastructure components, and not a
production service.

It exercises: Kubernetes DNS, EndpointSlices, readiness, pod scheduling, internal PKI, cert rotation, LB/Gateway,
ephemeral storage, CI/CD, security testing, and eventual consistency. It is also a deliberately honest example of
where availability, identity, and data durability are different problems.
