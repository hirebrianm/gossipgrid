# ADR-0002: Kubernetes-native peer discovery

**Status:** Accepted in principle · **Date:** 2026-11-18

## Context

Discover replicas without managing Consul, etcd, or a separate membership server. We also need to avoid a bootstrap
cycle in which unready pods cannot discover each other.

## Decision

Use a headless Service and Kubernetes DNS SRV/A queries with `publishNotReadyAddresses: true`; refresh dynamically.
Each peer separately authenticates over mTLS and checks readiness/synchronization state. Track expected StatefulSet
ordinals distinctly from records returned by DNS, especially when reporting all-replica message status. Use a static
peer list for local five-process testing. EndpointSlice watch is a later learning enhancement, not a v1 dependency.

## Consequences

No application RBAC necessary for peer discovery. DNS records may be cached, stale, or contain unready endpoints, so
actual peer health must be probed; DNS is not proof that a source can synchronize. Separate narrow API access may
still be needed for cluster epoch/reset metadata.
