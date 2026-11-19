# Data loss, recovery, and cluster epochs

## Why this matters

Each pod stores SQLite in `emptyDir`, so replacing a pod discards its messages. The application intentionally tests self-healing from surviving peers. **No design without durable copies can restore data after every copy is lost.** Recovery of *availability* is not recovery of *lost data*.

## Failure cases

| Situation | Required behavior |
| --- | --- |
| One pod replaced, at least one synced source survives | Join current epoch; synchronize; become ready |
| Gossip missed during temporary outage | Reconcile on reconnection |
| No synced peer currently reachable | Stay unready/waiting; diagnose; do not assume loss |
| All expected replicas report empty data in current epoch | Candidate catastrophic loss; perform strict safety checks before reset |
| Any expected replica unreachable or epoch uncertain | No automatic epoch reset; operator intervention or wait |
| New install with no prior epoch | Authorized first genesis; no data-loss alarm |
| Current epoch is known to have messages, but sources lost | Preserve the loss incident even if service resumes empty |

## Cluster metadata (not message storage)

A narrowly scoped Kubernetes record (provisionally a ConfigMap) tracks a globally unique cluster ID, an unpredictable epoch ID (plus display generation number), the fact that the cluster was initialized, whether messages were ever accepted in that epoch, and the last reset incident with acknowledgment status. **Do not use just a predictable integer epoch as an authority token.** Atomic Kubernetes resourceVersion updates gate any transition to a new epoch and prevent competing reset attempts.

This means GossipGrid uses Kubernetes API access for the *metadata/reset control path*, while regular peer discovery remains DNS-based. The service account must be restricted to a named ConfigMap and the minimum necessary get/update permissions; initial provisioning can be handled by Helm or a controlled job.

## Conservative automatic recovery policy

1. Query the authoritative cluster metadata and its expected member roster/epoch.
2. Probe every expected member with verified mTLS identity; distinguish empty, missing, initializing, ready, and unreachable.
3. Prefer any valid synchronized source with the current epoch, regardless of whether it is currently discoverable by DNS; reconcile instead of resetting.
4. If a member is unreachable, mismatched, or ambiguously initialized, **fail closed**. Do not claim all data is lost.
5. Only if every expected identity responds conclusively that all eligible data sources are gone, and the control-plane metadata allows a reset, attempt a conditional atomic epoch change. Recheck preconditions immediately before the update.
6. Establish a new empty epoch and allow healthy replicas to initialize under it; persist an unacknowledged incident marked `TOTAL_DATA_LOSS` or `POSSIBLE_DATA_LOSS` according to evidence.
7. Old-epoch replicas must never silently join or inject messages into the new epoch. An old-epoch return requires quarantine and explicit operator inspection.

**Safety note:** Even unanimous observations of currently empty ephemeral stores cannot mathematically prove that no old copies exist outside the observed membership, or that no messages were in flight. Therefore automatic reset is **a lab-only, opt-in policy** with a prominently disclosed risk; the default safe policy should be to **halt for operator authorization**. We can implement the automation after testing the stricter manual path. This is a deliberate improvement over treating `all currently reachable pods are empty` as a sufficient proof of total loss.

## UX and operations

- A completed reset does not hide history: a persistent, dismissible incident banner reports previous/current epoch, detection time, mode (manual/automatic), and an explicit message-loss warning.
- Status API and structured logs include epoch, readiness reason, reset cause, and acknowledgment state.
- Diagnostics should remain available via direct port-forward when the public Service has no ready endpoints.
- A safe test will deliberately delete all pods with ephemeral volumes, validate fail-closed behavior, then perform a manual reset and confirm the incident persists in metadata.
- Later opt-in experiment: strict automatic reset gate with explicit operator-configured threshold, conditional control-plane update, and tests for partitions, racing resets, stale pods, and uncommitted messages.

## Limitations

The metadata resource is not a backup of messages. It is a coordination and audit mechanism. Its own deletion or restoration from an out-of-date snapshot is an administrative incident, not a signal to create a new cluster. Cluster disaster recovery and safe split-brain prevention would require stronger assumptions and/or durable storage than this educational project intentionally provides.
