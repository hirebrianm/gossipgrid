# ADR-0001: Ephemeral SQLite on a StatefulSet

**Status:** Accepted in principle · **Date:** 2026-11-18

## Context

We want predictable pod identities, independent GORM/SQLite storage, and a realistic need to synchronize after pod
replacement. PVCs would hide the most interesting recovery path; a stateless Deployment would not give as clear a
stable ordinal identity for peer discovery.

## Decision

Use a StatefulSet with `podManagementPolicy: Parallel`, `emptyDir` for `/data`, and **no** `volumeClaimTemplates` or
persistent SQLite volumes. Every pod owns a local store that is rebuilt when its pod is replaced. A synchronized peer
must be the source for initial data; peers still synchronizing cannot be used as sources. Data can be irrecoverably
lost if all copies are destroyed.

## Consequences

Positive: simple storage operations, observable bootstrap, stable pod DNS, easy pod replacement experiments. Negative:
not durable, possible loss of locally committed unsent messages, reset workflow required when every source
disappears. A container restart within the same pod may retain `emptyDir`; a new pod does not.
See [recovery design](../recovery.md).
