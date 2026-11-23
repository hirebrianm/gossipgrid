# Incremental development plan

**Working agreement:** The architecture is written first. Implementation proceeds one milestone at a time. After every
milestone the developer shares the result (code, logs, diffs, tests); architecture and QA are reviewed before
continuing.

## Status classification

Every milestone and feature in this plan is described using one of four labels. Do not describe planned
functionality as operational, and do not describe locally tested code as cluster-verified.

| Label | Meaning |
| --- | --- |
| **Implemented and locally tested** | Go code exists, is covered by unit and integration tests that pass under the FIPS build and the race detector. May or may not be deployed to Kubernetes (see the per-milestone status). |
| **Deployed and initially smoke-tested; acceptance tests pending** | The resource is deployed in the target cluster and initial smoke tests (replicas running, peer discovery, HTTPS access, login, FIPS, mTLS) are confirmed. The full acceptance test suite (cross-replica convergence, recovery after pod replacement, live password rotation, fault tolerance) has not all been run. This is the current status of Milestone 7A. |
| **Partial** | Some components work (e.g., immutable image publishing, Argo CD digest-pinned reconciliation); the full automation (automated digest recording, promotion) is not complete. This is the current status of Milestone 8. |
| **Designed but not implemented** | The design is documented; the code, manifests, or infrastructure do not yet exist. This is the current status of Milestones 5B, 5C, 7B, and 9. |

## Milestones

| Phase | Deliverables | Acceptance signal | Status |
| --- | --- | --- | --- |
| 0. Documentation | README, architecture, ADRs, recovery and security models | Review and commit; no application code | ✅ Committed |
| 1. Single replica | Go main, env config, HTTP health probes, Makefile, static build, tests | `make qa` and local run | ✅ Implemented and locally tested |
| 2. Local storage | GORM/SQLite schema, local REST API, idempotent insert, tests | Message survives process restart with same data directory | ✅ Implemented and locally tested |
| 3A. Peer discovery and membership (2026-11-19) | Static peer discovery, DNS SRV resolver, membership tracking, reconciliation, tests | Local peer addresses discoverable; DNS errors observable without crash; race-safe | ✅ Implemented and locally tested |
| 3B. Peer mTLS (2026-11-19) | mTLS transport, cert hot reload, identity validation, private status endpoint, peer client | Negative TLS tests (20 scenarios); rotated certs/trust picked up by new connections; FIPS + race | ✅ Implemented and locally tested |
| 4A. Authenticated peer message exchange (2026-11-20) | POST /internal/messages on private peer listener, idempotent insert, SendMessage client, opt-in flag, two-node integration test | Explicit authenticated transfer between two local stores; idempotent; conflicts rejected; unauthorized peers rejected | ✅ Implemented and locally tested |
| 4B. Gossip fanout and retries (2026-11-20) | Epidemic fanout, bounded queues, duplicate suppression, exponential-backoff retries, five-node integration test | Five independent local stores converge under controlled healthy conditions; no unbounded forwarding loops | ✅ Implemented and locally tested |
| 4C. Anti-entropy (2026-11-20) | Periodic inventory comparison, bounded ID sets, keyset pagination, cursor progression, five-node integration test | Converges after partition recovery; no unbounded forwarding loops | ✅ Implemented and locally tested |
| 5A. Lifecycle state machine and epoch fencing (2026-11-21, `v0.1.0-alpha.5`) | Concurrency-safe state machine (7 states), durable SQLite epoch binding, opt-in readiness gating, epoch request headers + response envelope in peer status/sync, single-snapshot fail-closed admission for reads/writes/replication, outbound epoch provider (per-send re-check), legacy-peer rejection, controller-driven integration tests (14 scenarios) + real-mTLS mixed-mode tests (7 scenarios) | Unready replicas reject writes and replication; epoch mismatch returns 409 (no merge); legacy peers rejected as sync sources; revoked node blocks outbound replication; all existing M4 behavior preserved when lifecycle is disabled | ✅ Implemented and locally tested |
| 5B. Authoritative metadata, initial synchronization, and recovery authorization (🔜) | Kubernetes ConfigMap-backed authoritative lifecycle metadata (cluster ID, epoch ID, generation, initialized flag, reset incident); cluster epoch and membership authorization; safe genesis (control-plane-coordinated, empty SQLite state alone is never sufficient); initial snapshot transfer from eligible peers (SYNCING → READY); snapshot-source eligibility and readiness (unsynced pods never source snapshots); fail-closed behavior when authority is unavailable | Fresh process joins; unsynced pods never source snapshots; a `RECOVERY_REQUIRED` replica cannot auto-return to READY; fail-closed when ConfigMap is absent or unwatchable | Designed but not implemented |
| 5C. Controlled recovery (🔜) | Explicit operator recovery/reset authorization (no automatic destructive reset by default); catastrophic data-loss handling (preserving data, creating a new epoch only when every expected identity conclusively reports loss and the control-plane permits); epoch transitions and stale-data fencing (old-epoch replicas quarantined, never silently rejoin); incident and recovery audit information (persistent, dismissible incident banner; metadata records reset cause and acknowledgment state) | A `RECOVERY_REQUIRED` replica returns to READY only after explicit operator action; catastrophic reset preserves the loss incident in metadata; no automatic reset without an opt-in flag; stale pods are rejected as sync sources | Designed but not implemented |
| 6. Web UI and authentication (2026-11-21, `v0.1.0-alpha.5`) | Minimal functional browser interface (embedded Go assets via `go:embed`, vanilla JS; no frontend build pipeline, no CDNs, no external assets); message viewing and submission; peer, synchronization, and diagnostic information (version, FIPS state, lifecycle state, peer list); single-administrator password authentication (PBKDF2-HMAC-SHA-256, versioned credential file format v1; Vault KV v2 → VSO → K8s Secret → mounted read-only file in 7A; local file in development); shared HMAC-SHA256-signed session cookies (stateless across replicas, no server-side store, no sticky sessions); double-submit CSRF; `HttpOnly`/`Secure`/`SameSite=Strict` cookies; session absolute + idle expiry (Go default `2h`; Helm chart default `24h`); authentication enabled by default (fail-closed; `GOSSIPGRID_AUTH_INSECURE_DEV_MODE` is an insecure local-dev opt-out); change-detecting poll-based credential reload without restart (default 2s, bounded stale); password rotation invalidates the previous credential and existing sessions with **no overlap window** (generation binding; a session under the old generation is rejected immediately once a replica loads the new generation); invalid or unreadable credential material fails closed (503, not a crash); session-bound CSRF (`HMAC-SHA256(signing key, session SID)`); `credential` CLI subcommand (init/rotate/verify; interactive hidden prompts, no password in args); `/healthz` and `/readyz` remain unauthenticated; peer mTLS remains independent of browser authentication; `X-CSRF-Token` on state-changing requests | A browser user authenticates with the supplied password once and is valid on all five replicas (verified in the staging cluster); the credential file is mounted read-only and never delivered via environment variables (verified in the staging cluster); a credential rotation is picked up without a pod restart (eventual across replicas; a loaded replica rejects old-generation sessions immediately, with no overlap window); an invalid credential file fails closed (503, not a crash); health/readyz remain unauthenticated; `/internal/*` is not reachable on the public router (peer-mTLS separation verified); CSRF is required on state-changing requests | ✅ Implemented and locally tested; in-cluster delivery deployed and verified in M7A |
| 7A. Initial Kubernetes staging (2026-11-22, `v0.1.0-alpha.6`) | Five configurable StatefulSet replicas; independent SQLite databases on `emptyDir`; headless peer Service with `publishNotReadyAddresses: true`; ready-only public web Service; Kubernetes DNS peer discovery; separate workload PKI and per-pod peer mTLS; cert-manager integration with external Vault PKI; browser HTTPS through the existing shared Cilium Gateway (chart sets `gateway.enabled=false`; HTTPRoutes managed in k8s-gitops); VSO-managed authentication Secret and read-only mount; Helm packaging and Argo CD ownership; non-root containers and appropriate resource/probe configuration; gossip and anti-entropy convergence tests; live password-rotation demonstration without pod restarts; **deliverables: Helm chart (StatefulSet, headless + web Services, cert-manager browser Certificate, CiliumNetworkPolicy, NetworkPolicy, PDB), Argo CD Application (reusable example; actual staging Application in k8s-gitops), Vault provisioning doc, staging runbook (14 acceptance tests)** | Initial smoke tests confirmed: five replicas running and ready with zero restarts; replicas on two worker nodes; DNS peer discovery (4 remote + local); HTTPS access at `gossipgrid.k8s.lan` through shared Cilium Gateway; working browser login; Vault credentials loaded; peer mTLS with per-pod certs; Go FIPS module v1.0.0 enabled; immutable image from private GitLab registry via Argo CD. Remaining acceptance tests pending: cross-replica convergence, recovery after pod replacement, live password rotation, fault tolerance. Lifecycle enforcement remains **disabled** in 7A (M5B incomplete), so bootstrap, initial snapshot transfer, and recovery are validated only in 7B | ✅ Deployed and smoke-tested; full acceptance tests pending |
| 7B. Lifecycle-aware Kubernetes staging (🔜) | Enable lifecycle enforcement (`GOSSIPGRID_LIFECYCLE_ENABLED=true`); exercise authoritative metadata and epoch fencing in-cluster; verify safe genesis and initial synchronization; validate recovery authorization and stale-state rejection; run fail-closed, partition, and catastrophic-recovery tests | On-cluster: a fresh pod joins via snapshot transfer; a partition causes missing/unreachable status then heals via anti-entropy; all-pods-loss fails closed until 5C operator reset; stale-epoch pods are rejected as sync sources | Designed but not implemented |
| 8. GitOps delivery automation (🔜 partial) | GitLab CI performs QA, security checks, and container builds; publish immutable container images (by commit SHA / digest); Argo CD reconciles Kubernetes deployment state (watches k8s-gitops repository); no direct CI deployments that compete with Argo CD; record and deploy images by immutable digest in the k8s-gitops repository (automated); update the GitOps desired application revision (automated); document release promotion, rollback, and failure handling; clear responsibility boundaries between Ansible, GitLab CI, Helm, Vault, and Argo CD | An immutable image built with `make image` is published to the private GitLab Container Registry by digest (✅ done in staging — manual, not triggered by a tag push); the GitOps Application in k8s-gitops references the published digest and Argo CD reconciles the cluster to it (✅ done in staging — manual digest update, not automated); automated digest recording (CI commits the digest to k8s-gitops) (🔜 pending); automated revision updates (🔜 pending); a failed Argo CD sync is observable and reversible; CI never deploys directly to the cluster | Partial: manual image build, publish to private registry, digest-pinned Argo CD reconciliation; automated delivery and promotion not complete |
| 9. Resilience/security lab (🔜) | Fault injection (pod loss, partition, network impairment); reset authorization (5C); metrics (gossip attempts, convergence lag, sync state, peer count, SQLite failures, cert expiry, reset incidents); FIPS profile verification | Logged and reproducible experiments; a full-pod-loss scenario fails closed, is manually reset, and the incident persists in metadata | Designed but not implemented |

The sequence is intentionally flexible: the exact bootstrap/recovery control path may need prototype tests before
Kubernetes packaging. No phase should compromise secure peer authentication merely to simplify debugging.

## Milestone dependencies

- **5B** depends on 5A (the state machine, binding, and fencing framework). It adds the Kubernetes metadata
  authority, snapshot-source eligibility, and the initial transfer.
- **5C** depends on 5B (authoritative metadata) and 5A (the `RECOVERY_REQUIRED` lockout). It adds the operator
  recovery procedure and the catastrophic-loss reset.
- **6** is independent of 5B/5C (it adds browser authentication and the UI, not cluster lifecycle). It depends on
  the external Vault and VSO being present in the target cluster.
- **7A** depends on 3B, 4A, 4B, 4C, and 6 (peer mTLS, message exchange, gossip, anti-entropy, and browser auth are
  all required for the staging). Lifecycle enforcement is deliberately **disabled** in 7A because 5B is incomplete.
- **7B** depends on 7A, 5B, and 5C. It enables lifecycle enforcement and exercises the recovery path in-cluster.
- **8** depends on 7A (a working staging deployment, ✅ deployed) and the k8s-gitops repository and Argo CD
  (✅ present in the staging cluster).
- **9** depends on 7B (a full lifecycle-aware deployment) to run the fault-injection experiments.

## Rules for local-AI implementation reviews

- Inspect the generated diff and explain what each package does; avoid architectural drift and framework sprawl.
- Run `go test`, `go test -race`, `go vet`, and integration tests as relevant; share failing output in full.
- Keep the Makefile authoritative for repeatable build/test operations and GitLab CI.
- No unreviewed addition of brokers, databases, cluster-wide operators, alternate SQLite drivers, or service meshes.
- Verify security properties with negative tests, not merely configuration inspection.
- Document changes to architecture via short ADR updates.
- **Build efficiency:** Docker image builds are expensive. For documentation, Mermaid diagram, and Makefile-only
  changes, do **not** invoke `docker build` or `make image` — run targeted validation appropriate to the changed
  files (e.g. `make architecture`, `make docs-lint`, `make qa`). Rebuild container images only when image
  contents, application code, dependencies, build configuration, or deployment validation requires it.

## Open items to resolve before implementation

Resolved (2026-11-21, M5A committed as `v0.1.0-alpha.5`):

- Pod certificate identity SAN conventions and Go-side authorization logic (M3B, implemented).
- Snapshot page boundaries, handling messages arriving while scanning, queue and retry caps (M4C, implemented).
- Pinned Go toolchain (1.27) and FIPS module (v1.0.0) for the current build architecture (M1, implemented).
- License (Apache 2.0) and repository visibility (M2, implemented).

Still open, to be resolved as their milestones are implemented:

1. **M5B — Genesis protocol.** The exact control-plane-coordinated first-install genesis contract: which pod is
   designated seed, how the `Initialized` flag is set atomically, and the behavior when the ConfigMap is deleted or
   stale. Scale-down and lost-metadata behavior are part of the same contract.
2. **M5C — Reset authorization/locking.** The operator recovery/reset procedure: the endpoint or mechanism, the RBAC
   scope, the conditional atomic epoch change, and the incident audit format. The default is no automatic reset.
3. **M6 — Authentication design (✅ resolved, implemented 2026-11-21).** The password verification scheme
   (PBKDF2-HMAC-SHA-256, default 200,000 iterations / max 1,000,000, verified under the `GOFIPS140` build
   profile, per-credential 32-byte salt, versioned credential file format v1, constant-time verifier
   comparison), the HMAC-SHA256-signed session cookie (24h absolute / 30m idle, default), the common
   session-signing key, session invalidation on credential generation change with **no overlap window**
   (a session under the old generation is rejected immediately once a replica has loaded the new
   generation; the `MAX_STALE` policy (default 5m) governs the "stuck in a bad state" → 503 signal, not a
   session overlap), the session-bound CSRF token (`HMAC-SHA256(signing key, session SID)`), the
   authentication-enabled-by-default fail-closed behavior, and the local-development filesystem path
   convention (`GOSSIPGRID_AUTH_CREDENTIAL_FILE`; full Secret volume, no `subPath`) — see
   [security.md](security.md) and [kubernetes-deployment.md](kubernetes-deployment.md).
4. **M7A — Remaining acceptance tests (✅ deployed, smoke-tested).** The Helm chart, Argo CD Application, Vault
   provisioning document, and staging runbook are committed (`v0.1.0-alpha.6`) and validated by `make qa`.
   The staging deployment is confirmed: five replicas running and ready, peer discovery, HTTPS access, browser
   login, Vault credentials, peer mTLS, FIPS, and immutable image via Argo CD. What remains: the 14 acceptance
   tests in [deploy/STAGING-RUNBOOK.md](../deploy/STAGING-RUNBOOK.md) (cross-replica convergence, recovery after
   pod replacement, live password rotation, fault tolerance) must be run on-cluster.
5. **M8 — Automated GitOps delivery (🔜 partial).** An immutable image was built manually with `make image`,
   pushed to the private GitLab Container Registry, and the GitOps Application in k8s-gitops was updated to
   reference the published SHA256 digest; Argo CD reconciled the Kubernetes deployment. What remains:
   automated image publishing (CI commits the digest to k8s-gitops), automated GitOps revision updates,
   the Argo CD sync policy (pruning, self-heal), and the release promotion / rollback procedure.
6. **M9 — Fault-injection harness.** The exact fault-injection tooling and the set of experiments to run (pod loss,
   partition, CA rotation, concurrent resets).

## Purpose

GossipGrid is a distributed systems testbed for evaluating Kubernetes orchestration, service discovery, gossip
protocols, mTLS, fault tolerance, and automated deployments. It provides a real, minimal application for
infrastructure testing and Kubernetes validation — not a synthetic collection of infrastructure components, and not a
production service.

It exercises: Kubernetes DNS, EndpointSlices, readiness, pod scheduling, internal PKI, cert rotation, LB/Gateway,
ephemeral storage, CI/CD, security testing, and eventual consistency. It is also a deliberately honest example of
where availability, identity, and data durability are different problems.
