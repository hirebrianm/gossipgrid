# GossipGrid concept library

This library explains the distributed-systems concepts that GossipGrid is built to teach. Each document
explains **the problem the concept solves, the underlying principle, why GossipGrid uses it, how
GossipGrid currently implements it, and the important failure scenarios** — with a clear separation
between *conceptual examples* (illustrative) and *behavior observed in the actual application*
(implemented and tested).

These documents are **educational explanations**, not replacements for the authoritative architecture
documents. Where a concept's full contract is specified, the document links to the authoritative
design document (e.g. [architecture](../architecture.md),
[gossip and bootstrap](../gossip-and-bootstrap.md),
[lifecycle metadata and genesis](../lifecycle-metadata-and-genesis.md),
[recovery](../recovery.md), [security](../security.md)) rather than duplicating it.

The [GossipGrid visual design system](../visual-design.md) defines the color, shape, and line-style
conventions used in the diagrams below and (eventually) the native visualization interface
([Milestone 6B](../cluster-visualization.md)).

## Learning progression

Read the documents in this order. Each step builds on the previous one.

1. **How replicas communicate** — [Gossip protocols](gossip-protocols.md). The basic mechanism by which
   a message written on one replica spreads through the peer network.
2. **Why replicas can temporarily disagree** — [Eventual consistency](eventual-consistency.md). Why
   a gossip system does not offer immediate consistency, and what "eventual" actually means (and does
   not mean).
3. **How replicas reconcile differences** — [Anti-entropy](anti-entropy.md). The PULL-based mechanism
   that repairs the gaps gossip leaves behind. Gossip and anti-entropy are complementary, not
   competing.
4. **How distributed authority constrains operations** — [Distributed authority](distributed-authority.md).
   How a cluster without a leader decides who is allowed to write, read, and replicate.
5. **How epochs fence stale participants** — [Epoch fencing](epoch-fencing.md). How an old-epoch
   replica is prevented from reintroducing stale data into a newer epoch.
6. **How concurrency affects correctness** — [Concurrency and linearization](concurrency-and-linearization.md).
   How the lock order and the commit boundary make a concurrent revocation and commit safe.
7. **How ephemeral replicas recover from failures** — [Ephemeral state and recovery](ephemeral-state-and-recovery.md).
   What happens when a pod's local SQLite is lost, and why complete loss requires operator-controlled
   recovery rather than ordinary bootstrap.

## How to read these documents

- **Concept vs. implementation.** Each document has a "How GossipGrid currently implements it" section.
  That section describes *implemented, committed, and locally tested* behavior. Sections marked
  *planned* or *proposed* describe behavior that is **not yet implemented** (e.g. Milestone 5B Stages 3C–3E,
  Milestone 5C, Milestone 6B). Stages 3A and 3B of Milestone 5B are implemented and locally tested; do not read
  a planned section as a description of current behavior.
- **Conceptual examples are illustrative.** Diagrams and examples that show a "typical" gossip spread or
  a "typical" partition are *illustrative*; they are not a record of a specific observed run. Where a
  diagram reflects actual application behavior (e.g. the bootstrap admission sequence, the transactional
  install), the document says so explicitly.
- **Links are the path to depth.** Each document ends with "Related documentation and concepts" linking
  to the authoritative architecture document, the Go implementation, and the regression tests for the
  concept. Use those links to go from a concept explanation to the actual code and tests.

## Visual conventions used in this library

The diagrams in this library follow the [visual design system](../visual-design.md):

- **Green** — healthy, ready, authorized, successful.
- **Blue** — communication, replication, data transfer (the data plane).
- **Amber** — transition, synchronization, degraded, waiting.
- **Red** — failure, rejection, revocation, fencing.
- **Violet** — authority, identity, security, trust boundaries (the control plane).
- **Slate** — infrastructure, storage, neutral components.

A **dashed, slate, `planned`** edge marks functionality that is designed but not yet implemented. A
**dotted, violet** edge marks an mTLS transport. A **dotted, blue** edge marks an asynchronous event.
Solid edges mark implemented, observed relationships.

## Diagrams in this library

| Concept | Diagram | Purpose |
| --- | --- | --- |
| Gossip propagation | [Gossip propagation](../diagrams/generated/concept-gossip-propagation.svg) ([PNG](../diagrams/generated/concept-gossip-propagation.png)) | A message spreading through a bounded peer network; distinguishes actual GossipGrid fanout from an illustrative example |
| Eventual consistency | [Eventual consistency](../diagrams/generated/concept-eventual-consistency.svg) ([PNG](../diagrams/generated/concept-eventual-consistency.png)) | Replicas temporarily holding different message sets, then converging |
| Gossip vs. anti-entropy | [Gossip vs. anti-entropy](../diagrams/generated/concept-gossip-vs-antientropy.svg) ([PNG](../diagrams/generated/concept-gossip-vs-antientropy.png)) | Push-based dissemination and PULL-based reconciliation as complementary mechanisms |
| Epoch fencing | [Epoch fencing](../diagrams/generated/concept-epoch-fencing.svg) ([PNG](../diagrams/generated/concept-epoch-fencing.png)) | An obsolete-epoch request rejected by a newer-epoch replica |
| Operator-controlled genesis | [Operator-controlled genesis](../diagrams/generated/concept-operator-genesis.svg) ([PNG](../diagrams/generated/concept-operator-genesis.png)) | The authoritative metadata, operator authorization, one-time genesis, and finalization boundaries (Stages 1A/1B) |
| Bootstrap replication admission | [Bootstrap admission](../diagrams/generated/concept-bootstrap-admission.svg) ([PNG](../diagrams/generated/concept-bootstrap-admission.png)) | The focused sequence: an authenticated SYNCING requester, dedicated bootstrap admission, bounded fetch, guarded commit, remaining SYNCING |
| Bootstrap privilege isolation | [Bootstrap privilege isolation](../diagrams/generated/concept-bootstrap-privilege-isolation.svg) ([PNG](../diagrams/generated/concept-bootstrap-privilege-isolation.png)) | Which operations a SYNCING replica may and may not perform |
| Transactional installation and retention | [Transactional installation](../diagrams/generated/concept-transactional-install.svg) ([PNG](../diagrams/generated/concept-transactional-install.png)) | The accepted single-transaction sequence and the SQLite busy-timeout limitation |
| Authorization revocation and linearization | [Revocation and linearization](../diagrams/generated/concept-revocation-linearization.svg) ([PNG](../diagrams/generated/concept-revocation-linearization.png)) | Both legal orderings of a concurrent revocation and commit, and the partial-404 retry regression |
| Ephemeral replica recovery | [Ephemeral replica recovery](../diagrams/generated/concept-ephemeral-recovery.svg) ([PNG](../diagrams/generated/concept-ephemeral-recovery.png)) | A replica losing its SQLite, obtaining a new store, and reconstructing retained state from surviving authoritative replicas |
| GitOps rollback | [GitOps rollback](../diagrams/generated/concept-gitops-rollback.svg) ([PNG](../diagrams/generated/concept-gitops-rollback.png)) | The distinction between Kubernetes workload revision history and SQLite contents |

Regenerate all diagrams (existing and concept) with `make architecture`. The concept diagrams are
registered in the Makefile's `DIAGRAM_BASES` and `DIAGRAM_DOC_MAP` (see the [Makefile](../../Makefile))
and are validated by the same three-level pipeline as the architecture diagrams
([diagrams-check](../../Makefile#L330), [diagrams-render](../../Makefile#L350),
[diagrams-contact-sheet](../../Makefile#L360)).
