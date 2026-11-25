# GossipGrid genesis runbook (Milestone 5B, Stages 1A and 1B)

This runbook documents the **explicit, operator-authorized** establishment of
GossipGrid's initial cluster: the grant (Stage 1A), the seed binding (Stage 1B),
and the finalization (Stage 1B). It is a guide for the operator; it does **not**
automatically run the grant, bind, or finalize anything, and it does **not**
deploy anything.

**The single most important rule:** genesis is **never automatic**. The absence
of the ConfigMap (an empty SQLite database, no reachable peers, a failed DNS
lookup, a first application start, a Kubernetes/Argo CD deployment) is **not**
evidence that creating a new cluster is safe. The only path to genesis is an
explicit, operator-invoked grant that has **verified, via the preflight below,
that there is no existing authoritative epoch and no recoverable data**. A
command that cannot verify its preconditions **fails closed** rather than
inferring safety.

**The existing 7A staging deployment is data-bearing and must NOT be treated as
a fresh cluster.** A grant against it would either find an existing record
(fail-closed) or, if the record were somehow deleted, create a **replacement**
identity that discards the incident. Do **not** run this workflow against the
existing 7A deployment.

**The complete manual sequence** (all steps are explicit, operator-invoked; none
is automatic):

1. **Grant** (Stage 1A): the operator launches the genesis Job, which creates
   the ConfigMap `gossipgrid-cluster-metadata` with `Initialized=false` and
   `GenesisAuthorized=true` (a one-time token). Sections 0–9.
2. **Seed binding** (Stage 1B): the canonical seed `gossipgrid-0` (with
   lifecycle protection enabled and the seed identity configured) observes the
   grant's metadata, binds its local SQLite to the cluster/epoch, and enters
   the `GENESIS` state. Sections 10–13.
3. **Finalization** (Stage 1B): the operator verifies the seed's binding, then
   launches the finalization Job, which atomically sets `Initialized=true`,
   `GenesisAuthorized=false`, `GenesisConsumer=gossipgrid-0`, and
   `dataLossProtected=true` in a single CAS. The seed observes the finalization
   and transitions to `READY`, opening public writes. Sections 14–19.

**Never instruct an operator to delete SQLite, bindings, or metadata.** If a
step fails, the operator inspects the authoritative state and decides the next
action. A delete-and-recreate of the ConfigMap, a deletion of a replica's
SQLite, or a deletion of a binding is **forbidden** — it discards the
authoritative record or the local binding and is not a safe recovery.

## 0. What the grant does

The grant (the `gossipgrid genesis` subcommand, run by the operator-triggered
genesis Job) performs a **fail-closed preflight** and then a
**single create** of the ConfigMap `gossipgrid-cluster-metadata` in the
application namespace. On success it creates exactly one immutable record
carrying:

- `schemaVersion: 1`
- a newly generated unique `clusterId` (UUIDv7)
- a newly generated unique `epochId` (UUIDv7, distinct from `clusterId`)
- `generation: 1`
- `initialized: false`
- `genesisAuthorized: true`
- `genesisConsumer: ""`
- `everAcceptedMessages: false`
- `dataLossProtected: false`
- no reset incident

The one-time `genesisAuthorized` token is what a canonical seed (Milestone 5B,
Stage 1B) later consumes when it binds. **No public write is accepted before
finalization** (Milestone 5B/5C): the grant sets `initialized: false`, and the
5A admission gate rejects writes while `initialized` is false.

## 1. Preconditions (verify first — all must hold before a grant is issued)

Run each check against the **actual cluster** (do not assume). The grant's
preflight re-verifies the ConfigMap state at invocation time, but **the
operator must independently confirm the cluster is fresh and empty before
launching the grant** — the preflight is a safety net, not a substitute for
operator due diligence.

- [ ] **The cluster is fresh and empty.** The ConfigMap
      `gossipgrid-cluster-metadata` does **not** exist in the application
      namespace:
      ```
      kubectl get configmap gossipgrid-cluster-metadata -n gossipgrid
      # expected: Error from server (NotFound) — the object is absent
      ```
      If the object exists, **stop**. Inspect it (section 4). If it reports
      `initialized: true`, the cluster already has a completed epoch and a
      grant is **not** permitted (the grant fails closed with
      `ErrClusterInitialized`).
- [ ] **No surviving replica holds a recoverable binding.** If any GossipGrid
      pod is running with a `local_metadata` SQLite binding (a prior epoch),
      the cluster is **not** fresh — a grant would discard that data. Verify
      no GossipGrid pods are running, or (if the StatefulSet is already
      deployed) that the pods' `/data` volumes are empty (no prior
      `local_metadata` row). **A previously initialized or data-bearing
      cluster must never become genesis-eligible merely because its replicas
      were deliberately drained or its last data copy was destroyed.** Do
      **not** delete surviving data or bindings to satisfy genesis
      preconditions.
- [ ] **The genesis RBAC is in place.** The ServiceAccount, Role, and
      RoleBinding rendered by the genesis Helm template
      (`deploy/helm/gossipgrid/templates/genesis-rbac.yaml`) exist in the
      application namespace:
      ```
      kubectl get serviceaccount gossipgrid-genesis -n gossipgrid
      kubectl get role gossipgrid-genesis -n gossipgrid
      kubectl get rolebinding gossipgrid-genesis -n gossipgrid
      ```
      The RBAC is **disabled by default** (`genesis.enabled: false` in
      `values.yaml`). To prepare a cluster for a grant, an operator sets
      `genesis.enabled: true` via an **override values file** (not the 7A
      staging values) and lets Argo CD reconcile the RBAC, then verifies it as
      above. **Do not** enable the RBAC in the 7A staging values (the 7A
      deployment is data-bearing).
- [ ] **The gossipgrid image (with the `genesis` subcommand) is available** at
      an **immutable image reference** (a digest, e.g.
      `registry.gitlab.example.internal/gossipgrid@sha256:<digest>`) in the
      private registry. The Job manifest (`deploy/genesis/job.yaml`) must be
      updated with this reference before applying. **Do not** use a mutable
      tag (e.g. `:latest`) — the image reference must be immutable so the
      grant's behavior is reproducible and auditable.
- [ ] **The private registry pull credentials are configured.** The Job's pod
      must be able to pull the image from the private registry. The Job
      manifest does **not** inherit the application's `imagePullSecrets`. If
      the registry requires authentication, add an `imagePullSecrets` entry to
      the Job's pod spec (under `spec.template.spec.imagePullSecrets`)
      referencing a Secret that holds the registry pull credentials. Verify the
      Secret exists in the application namespace:
      ```
      kubectl get secret <pull-secret-name> -n gossipgrid
      ```
- [ ] **The operator has explicit authority** to establish a new cluster
      (this is a deliberate, auditable decision, not an automated recovery).

**Genesis is only for a verified, genuinely new cluster.** Missing ConfigMap
metadata or absent pods do **not** establish that the cluster is new.
Existing clusters require **authorized migration or recovery** (Milestone
5C), not a fresh genesis grant. The grant's API preflight only verifies
ConfigMap state; **the operator remains responsible for proving the
environment is genuinely new** — the preflight is a safety net, not a
substitute for operator due diligence.

**If any precondition fails, stop.** Do not launch the grant. A command that
cannot verify its preconditions fails closed.

## 2. How to explicitly launch the grant

The grant is performed by the operator-triggered genesis Job
(`deploy/genesis/job.yaml`). The Job is **not** a Helm hook and is **not**
created by Argo CD reconciliation — it is applied **explicitly** by the
operator.

**Before applying the Job:**

- **Update the image reference** in `deploy/genesis/job.yaml` to the
  immutable digest (see section 1). The Job's container runs the
  `/gossipgrid` binary (the executable path in the scratch image) with
  the `genesis` subcommand.
- **Handle any existing Job.** If a previous Job with the same name
  (`gossipgrid-genesis`) exists in the namespace, it must be **deleted**
  before a new Job can be applied (Kubernetes does not allow two Jobs with
  the same name in the same namespace). Deleting the Job does **not**
  delete the ConfigMap (the Job and the ConfigMap are independent
  resources). If the previous Job completed successfully, the ConfigMap
  already exists and a new Job will report "already granted" (exit 0,
  idempotent). If the previous Job failed, inspect the ConfigMap (section
  6) before re-applying.

  Check for and delete an existing Job:

  ```bash
  # Check for an existing Job.
  kubectl get job gossipgrid-genesis -n gossipgrid
  # If present and you are re-attempting the grant:
  kubectl delete job gossipgrid-genesis -n gossipgrid
  ```

**Apply the Job:**

```bash
# Apply the Job (runs in the application namespace; the Job's serviceAccount
# is gossipgrid-genesis, which has create on configmaps in this namespace).
kubectl apply -f deploy/genesis/job.yaml

# Watch the Job to completion (the grant is a one-shot, fail-closed operation).
kubectl logs job/gossipgrid-genesis -n gossipgrid -f
```

The Job's container runs `/gossipgrid genesis --namespace gossipgrid
--configmap gossipgrid-cluster-metadata`. The `--namespace` flag supplies
the application namespace; when omitted, the namespace is read from the
in-cluster service account's mounted namespace file
(`/var/run/secrets/kubernetes.io/serviceaccount/namespace`). A missing or
empty namespace is a fail-closed condition.

## 3. How to observe successful creation

A successful grant prints to stderr:

```text
genesis: cluster metadata created: cluster=<uuid> epoch=<uuid> generation=1
genesis: the one-time genesis authorization is in place (Initialized=false, GenesisAuthorized=true).
genesis: do NOT run this grant again; the record is immutable and the cluster identity is established.
```

The Job's pod exits 0. Verify the Job succeeded:

```bash
kubectl get job gossipgrid-genesis -n gossipgrid
# STATUS: Complete (1/1)
```

## 4. How to inspect the resulting ConfigMap

```bash
kubectl get configmap gossipgrid-cluster-metadata -n gossipgrid -o yaml
```

The `data.metadata` field holds the JSON metadata document. Verify the
identities and genesis authorization:

```bash
kubectl get configmap gossipgrid-cluster-metadata -n gossipgrid \
  -o jsonpath='{.data.metadata}' | python3 -m json.tool
```

Confirm:

- `schemaVersion` is `1`
- `clusterId` and `epochId` are present, nonempty, and **distinct** UUIDv7
  values (36 characters each, version nibble `7` at index 14)
- `generation` is `1`
- `initialized` is `false`
- `genesisAuthorized` is `true`
- `genesisConsumer` is `""`
- `everAcceptedMessages` is `false`
- `dataLossProtected` is `false`
- there is no `lastReset` field

**Record the `clusterId` and `epochId`** (e.g., in the operator's incident
log). They are the authoritative identities for the first epoch and are
referenced by the finalization Job (Milestone 5B/5C) and by peer fencing
headers.

## 5. How to verify the new identities and genesis authorization

The identities are validated by the grant before creation (`Validate` on the
metadata document). To re-verify after the fact, re-read the ConfigMap and
confirm the fields in section 4. The `resourceVersion` of the ConfigMap is
the CAS revision (a future finalization Job uses it for compare-and-swap).

## 6. What happens when the Job fails

A failed grant exits nonzero. **An ambiguous failure may have occurred after
the ConfigMap was created** (e.g., the API server creates the object but times
out before returning the response). The grant **never generated a replacement
identity**, but the operator **must inspect the authoritative ConfigMap
metadata before retrying** (section 4) to determine whether the object was
created. Common failure modes (each is a distinct, inspectable error — see the
package documentation in `internal/lifecycle/genesis`):

- **The ConfigMap already exists with `initialized: true`** →
  `ErrClusterInitialized`. The cluster has a completed epoch; genesis is not
  permitted. Inspect the ConfigMap; if a reset is genuinely needed, follow the
  recovery procedure (Milestone 5C), **not** a grant.
- **The ConfigMap exists but is corrupted or in an incoherent state** →
  `ErrMetadataConflict`. The record is broken. Inspect and repair it (e.g.,
  re-apply the correct document) before retrying. **Do not** delete and
  recreate the ConfigMap to "fix" it — a delete-and-recreate would discard the
  authoritative record and is forbidden.
- **The create result is ambiguous and the authoritative object could not be
  re-read** → `ErrCreateAmbiguous`. An ambiguous result occurs when the
  Create call returns an error (a transient API error, a 409 conflict, or an
  unclassified error) or a nil response with a nil error, and the subsequent
  re-read cannot establish a definitive state. **A failure may occur after a
  successful creation** (e.g., the API server creates the object but times out
  before returning the response). **Do not** re-launch the grant blindly.
  Re-read the ConfigMap manually; if it is absent, the create failed and the
  operator may retry; if it is present, verify it (section 4) and proceed.
  A 409 conflict is classified as `APIClassUnknown` (not NotFound) and is
  resolved by re-reading the authoritative object.
- **The RBAC denies access (403)** → `ErrMetadataPermissionDenied`. The
  ServiceAccount lacks the create (or get) permission. Verify the RBAC
  (section 1) and the Role's rules (create on configmaps; get on the named
  ConfigMap). Fix the RBAC and re-apply the Job.
- **The Kubernetes API is unavailable (5xx / timeout)** → the grant fails
  closed (an `ErrAPIUnavailable` marker). Wait for the API to recover and
  re-apply the Job (a safe, idempotent retry).

In every failure case, **inspect the ConfigMap** before any retry:

```bash
kubectl get configmap gossipgrid-cluster-metadata -n gossipgrid -o yaml
```

## 7. How to handle a retry safely

The grant is **idempotent**. A retry against an **existing valid pending grant**
( `initialized: false`, `genesisAuthorized: true` ) reports
`ErrAlreadyGranted` (a benign, expected outcome) and creates **nothing**:

```text
genesis: cluster metadata already exists with a pending genesis authorization (already granted): cluster=<uuid> epoch=<uuid> generation=1
```

A retry against an **initialized** or **corrupted** record fails closed
(sections 6). A retry after a **failed create** (the ConfigMap is absent)
creates a **new** record — but only after the operator has **confirmed the
ConfigMap is genuinely absent** (a re-read, not a blind re-launch). The Job
uses `backoffLimit: 0` and `restartPolicy: Never` (no automatic retries): a
failure is surfaced to the operator, who decides whether to re-apply.

**A retry cannot generate a replacement identity for an existing record.**
When an existing valid pending grant is detected (before any Create is
attempted), the grant returns the existing authoritative identity directly
(ErrAlreadyGranted, exit 0) without generating a new identity pair or
attempting a Create. If a Create is attempted and returns a 409 conflict
(classified as `APIClassUnknown`, not NotFound), the grant re-reads the
authoritative object and reports it (already granted / initialized /
corrupted), never by generating a second identity.

## 8. What must NOT be done after metadata corruption or data loss

- **Do NOT delete and recreate the ConfigMap** to "reset" the cluster. A
  delete-and-recreate discards the authoritative record and is forbidden.
  Recovery from a corrupted or lost record is the **recovery procedure**
  (Milestone 5C), which is operator-authorized and auditable, **not** a grant.
- **Do NOT run the grant against an existing initialized cluster.** A grant
  against a record with `initialized: true` fails closed. A reset of an
  initialized cluster requires the 5C recovery procedure (a new epoch, a new
  `epochId`, re-armed `dataLossProtected`).
- **Do NOT infer safety from an empty SQLite database, a missing ConfigMap, or
  a failed DNS lookup.** None of these is evidence that creating a new cluster
  is safe. The operator's preflight (section 1) is the required due diligence.
- **Do NOT enable the genesis RBAC or launch the grant from the 7A staging
  values or Argo CD reconciliation.** The RBAC is disabled by default; the Job
  is operator-managed. There is **no automatic cluster reset or recovery
  procedure** in this milestone.

## 9. What must NOT be done (seed binding and finalization)

- **Do NOT instruct the operator to delete the seed's SQLite database or its
  `local_metadata` binding** to "reset" the seed. The binding is the durable
  proof that the local database belongs to the authorized epoch. Deleting it
  discards the binding and is forbidden.
- **Do NOT delete and recreate the ConfigMap** to "reset" the cluster (the
  finalization Job's RBAC has no `delete` permission, and a manual delete would
  discard the authoritative record).
- **Do NOT enable lifecycle protection, the seed identity, or the finalization
  RBAC in the 7A staging values or Argo CD reconciliation.** These features are
  **inactive by default** (all `enabled` fields are `false`); the operator
  enables them via an **override values file** for a new cluster only.
- **Do NOT run the finalization Job until the seed's binding has been
  verified** (sections 12–13). The `--confirm-seed-bound` flag is an explicit
  operator attestation that the seed is bound; it does not probe the peer
  network (a network probe would be a fail-open inference and is forbidden).
- **Do NOT infer the seed's binding state from a failed peer-status request.**
  A failed request is **unverified**, not "unbound." The operator must confirm
  the seed's binding via a **reachable** peer-status or diagnostics endpoint
  (section 13), not by the absence of an error.

## 10. Enabling lifecycle protection and the seed identity

Seed binding (Stage 1B) requires the application's **lifecycle protection** to
be **enabled** and the **seed identity** to be **configured**. Both are
**disabled by default** and must be set via an **override values file** (not the
7A staging values) for a new cluster.

**The override values file** (e.g. `values-7b-genesis.yaml`) must set:

```yaml
lifecycle:
  enabled: true
  # The Kubernetes cluster-metadata provider. Note: this is a MAPPED value
  # (lifecycle.metadataKubernetes.enabled), not a boolean
  # (lifecycle.metadataKubernetes) — the chart uses the .enabled sub-key to
  # gate the provider and its RBAC.
  metadataKubernetes:
    enabled: true
    # The name of the authoritative cluster-metadata ConfigMap the
    # application reads (defaults to "gossipgrid-cluster-metadata"; set it
    # explicitly if the grant used a different name).
    configMapName: gossipgrid-cluster-metadata
    # The namespace of the authoritative cluster-metadata ConfigMap. Emitted
    # as an env var ONLY when nonempty; when empty (the default), the
    # application falls back to the in-cluster service account's mounted
    # namespace file. Set it explicitly here for clarity (it must match the
    # application namespace).
    metadataNamespace: gossipgrid
  # The designated seed identity for operator-authorized genesis (the
  # StatefulSet ordinal-0 pod, "gossipgrid-0"). Emitted as an env var ONLY
  # when nonempty AND lifecycle.enabled is true. Must equal the
  # deterministic seed-identity convention (lifecycle.DesignatedGenesisSeed):
  # the genesis finalization enforces this convention (a finalization with a
  # different consumer fails closed before any ConfigMap update).
  seedIdentity: gossipgrid-0
```

These map to the environment variables:

- `GOSSIPGRID_LIFECYCLE_ENABLED=true` (from `lifecycle.enabled`)
- `GOSSIPGRID_LIFECYCLE_METADATA_KUBERNETES=true` (from
  `lifecycle.metadataKubernetes.enabled`)
- `GOSSIPGRID_LIFECYCLE_METADATA_CONFIGMAP_NAME=gossipgrid-cluster-metadata`
  (from `lifecycle.metadataKubernetes.configMapName`)
- `GOSSIPGRID_LIFECYCLE_METADATA_NAMESPACE=gossipgrid` (from
  `lifecycle.metadataKubernetes.metadataNamespace`; emitted only when
  nonempty)
- `GOSSIPGRID_GENESIS_SEED_IDENTITY=gossipgrid-0` (from
  `lifecycle.seedIdentity`; emitted only when nonempty and
  `lifecycle.enabled` is true)

**The default Helm output is unchanged:** when `lifecycle.enabled` is false
(the 7A staging default) or `lifecycle.seedIdentity` is empty (the default),
no seed-identity env var is emitted. When
`lifecycle.metadataKubernetes.enabled` is false (the default) or
`lifecycle.metadataKubernetes.metadataNamespace` is empty (the default), no
metadata-namespace env var is emitted.

**The seed identity must be an exact, case-sensitive match** to the seed pod's
`NodeID` (in a StatefulSet, the ordinal 0 pod, e.g. `gossipgrid-0`). Only the
replica whose `NodeID` exactly equals `seedIdentity` may take the genesis
authorization path; all other replicas are no-ops (they cannot enter `GENESIS`).

**The application ServiceAccount** needs `get` on the named ConfigMap
(`gossipgrid-cluster-metadata`, with `resourceNames`) to read the metadata.
This is the read-only metadata path; the app SA has **no** `create`, `update`,
`patch`, or `delete`. The finalization Job has its **own** ServiceAccount
(section 15) with `get` + `update` on the named ConfigMap.

## 11. What the seed does (the AuthorizeGenesis path)

When lifecycle protection is enabled and the seed identity is configured, the
application runs a **genesis-entry loop** for the designated seed: it periodically
attempts `AuthorizeGenesis` until the seed is bound or the state becomes
`RECOVERY_REQUIRED`. The loop stops when the seed has a local binding (it is
bound) or when the state becomes `RECOVERY_REQUIRED` (a fail-closed lockout).

The `AuthorizeGenesis` path is **gated**: it is permitted only when **all** of
the following hold (the fail-closed policy; an empty database or missing peers
alone never authorize genesis):

- The local replica identity is **exactly** the designated seed
  (`seedIdentity == NodeID`); any other replica is a no-op.
- The controller has not been stopped.
- A **fresh, valid** authoritative metadata observation exists (authority
  "fresh", not stale) — an expired or invalid observation cannot authorize
  genesis.
- The metadata reports an **outstanding, unconsumed** genesis authorization:
  `Initialized=false`, `GenesisAuthorized=true`, `GenesisConsumer=""` (the
  one-time token is present and unspent).
- `DataLossProtected=false` (the cluster is pre-finalization; a
  data-loss-protected cluster cannot re-enter genesis).
- The local database has **no existing binding and no messages** (a nonempty
  legacy database must not silently acquire a binding). An existing binding is
  handled by the idempotent-restart case (section 13); a conflicting binding
  (a different cluster/epoch) is a fail-closed mismatch.

On success, the seed performs the **atomic, idempotent** SQLite `Bind` and
transitions `DISCOVERING → GENESIS`. The seed **remains in `GENESIS`** while
`Initialized=false`: public writes stay blocked (the 5A admission gate rejects
non-ready states and `Initialized=false`) and `SetReady` fails (the 5A `SetReady`
gate rejects `Initialized=false`). The seed is promoted to `READY` only after it
observes the finalized metadata (section 16).

## 12. How to observe the seed entering GENESIS

The seed's peer-status and diagnostics endpoints report the genesis state. Verify
the seed has bound and entered `GENESIS`:

```bash
# The seed's peer-status endpoint (authenticated peer transport).
curl -sk --cert <peer-cert> --key <peer-key> \
  https://gossipgrid-0.gossipgrid:8443/internal/peers/status | python3 -m json.tool
```

Confirm (from the peer-status response):

- `localBindingPresent` is `true`
- `bindingMatchesMetadata` is `true` (the binding matches the authoritative
  `ClusterID` and `EpochID`)
- `genesisAuthorizationOutstanding` is `true` (the metadata reports
  `Initialized=false`, `GenesisAuthorized=true`, `GenesisConsumer=""`)
- `finalized` is `false`
- `dataLossProtected` is `false`

The seed's **lifecycle state** is `GENESIS` (not `READY`). `/readyz` reports
503 (the seed is not ready) — this is **expected** and **not** a failure signal;
do not use `/readyz` as the verification signal (it would report 503, which is
expected and not informative).

**The seed's lifecycle state and binding are the verification signal, not
`/readyz`.** The peer-status endpoint's `localBindingPresent` and
`bindingMatchesMetadata` fields, plus the diagnostics endpoint's lifecycle
state, are the authoritative evidence that the seed has bound.

## 13. Idempotent restart and the "already bound" case

If the seed pod **restarts** (e.g., a liveness probe kills it, or the node is
drained) **after** it has bound but **before** the operator finalizes, the
`AuthorizeGenesis` path is **idempotent**:

- The seed re-reads the metadata (`GenesisAuthorized` still `true`,
  `Initialized=false`).
- The local database **already has a binding** that matches the authorized
  cluster/epoch.
- The `AuthorizeGenesis` path detects the matching binding and **skips the
  `Bind` call** (the durable `Bind` would be a no-op) and reuses the existing
  binding. The seed is already in `GENESIS`, so the method returns a no-op
  (it does not re-transition).

**The binding is never deleted or modified on restart.** A conflicting binding
(a different cluster/epoch) is a fail-closed mismatch: the seed does not enter
`GENESIS` and the existing binding is preserved.

**If the seed's `emptyDir` volume is lost** (a node failure that wipes the
volume), the local SQLite is empty and unbound. On restart, the seed re-reads
the metadata (still `GenesisAuthorized=true`) and re-binds (an idempotent `Bind`
for the same values) and re-enters `GENESIS`. This is safe **because the
cluster was empty at finalization time** (no data was at risk). If the cluster
had data (a data-loss incident), the operator must follow the recovery procedure
(Milestone 5C), **not** re-bind and re-finalize.

## 14. The finalization Job

The finalization is performed by a **dedicated, operator-triggered Job**
(`deploy/genesis/finalize-job.yaml`) — *not* by the seed pod and *not* by a Helm
hook. The Job runs the `/gossipgrid genesis finalize` subcommand, which:

1. Reads the ConfigMap `gossipgrid-cluster-metadata` (the finalization
   ServiceAccount has `get` on the named ConfigMap).
2. Validates the metadata document (a strict, versioned schema).
3. Verifies the preconditions: the metadata reports an **outstanding
   authorization** (`Initialized=false`, `GenesisAuthorized=true`,
   `GenesisConsumer=""`).
4. Requires the operator's **explicit confirmation** that the seed is bound
   (the `--confirm-seed-bound` flag is an operator attestation, not a network
   probe).
5. Performs the **atomic CAS** on the named ConfigMap: a single update (using
   the current `resourceVersion`) that sets `Initialized=true`,
   `GenesisAuthorized=false`, `GenesisConsumer=gossipgrid-0`, and
   `dataLossProtected=true` **together**, preserving all other metadata fields
   (no delete/recreate).

The Job is **disabled by default** (`finalize.enabled: false` in `values.yaml`).
It is **not** a Helm hook and is **not** created by Argo CD reconciliation — it
is applied **explicitly** by the operator.

**The finalization ServiceAccount** (rendered by
`deploy/helm/gossipgrid/templates/finalize-rbac.yaml`) has **only** `get` +
`update` on the **named** ConfigMap (with `resourceNames`). It has **no**
`create`, `delete`, `patch`, `list`, or `watch`, and **no** ClusterRole. The
`update` is scoped to the named object via `resourceNames`, which **is** honored
for `update`.

## 15. How to verify the seed's binding before finalizing

**Before launching the finalization Job, the operator must independently confirm
the seed's binding** (sections 12–13). This is the **explicit operator gate** —
the finalization Job does **not** probe the peer network (a network probe would
be a fail-open inference and is forbidden). The operator's verification is:

1. **The seed is in `GENESIS`** (not `READY`, not `RECOVERY_REQUIRED`).
2. **The seed's local binding is present and matches** the authoritative
   `ClusterID` and `EpochID` (the peer-status endpoint's
   `localBindingPresent` and `bindingMatchesMetadata` fields, or the
   diagnostics endpoint's binding fields).
3. **The metadata reports an outstanding authorization** (`Initialized=false`,
   `GenesisAuthorized=true`, `GenesisConsumer=""`) — the operator re-reads the
   ConfigMap (section 4) and confirms.

**The operator records the `clusterId` and `epochId`** from the ConfigMap
(section 4) and passes them to the finalization Job (the Job validates them
against the metadata; a mismatch fails closed). The `--genesis-consumer` flag
is the seed's identity (e.g. `gossipgrid-0`); it must match the seed's
`NodeID` (the Job validates it against the metadata).

## 16. How to explicitly launch the finalization

**Before applying the finalization Job:**

- **Update the image reference** in `deploy/genesis/finalize-job.yaml` to the
  immutable digest (section 1). The Job's container runs the `/gossipgrid`
  binary with the `genesis finalize` subcommand.
- **Set the flags** to the grant's identities (section 4) and the seed's
  identity:
  - `--cluster-id <grant-cluster-id>`
  - `--epoch-id <grant-epoch-id>`
  - `--genesis-consumer gossipgrid-0`
  - `--confirm-seed-bound` (the explicit operator attestation that the seed is
    bound; required — without it the Job fails closed)
- **Handle any existing Job.** If a previous Job with the same name
  (`gossipgrid-genesis-finalize`) exists in the namespace, it must be
  **deleted** before a new Job can be applied.

**Apply the Job:**

```bash
kubectl apply -f deploy/genesis/finalize-job.yaml

# Watch the Job to completion.
kubectl logs job/gossipgrid-genesis-finalize -n gossipgrid -f
```

The Job's container runs `/gossipgrid genesis finalize --namespace gossipgrid
--configmap gossipgrid-cluster-metadata --cluster-id <grant-cluster-id>
--epoch-id <grant-epoch-id> --genesis-consumer gossipgrid-0 --confirm-seed-bound`.

## 17. How to observe the seed reaching READY

A successful finalization exits 0. The seed's metadata worker reads the
finalized metadata on its next poll and **transitions `GENESIS → READY`** (the
`promoteFromGenesis` path; the 5A `SetReady` gate is now satisfied because
`Initialized=true` and the binding matches). Public writes are **opened** (the
admission gate now admits writes because the seed is in `READY` with a fresh,
valid, matching authoritative observation).

Verify the seed reached `READY`:

```bash
# The seed's /readyz endpoint (now 200).
curl -sk -o /dev/null -w '%{http_code}' --cert <peer-cert> --key <peer-key> \
  https://gossipgrid-0.gossipgrid:8443/readyz
# expected: 200

# The seed's peer-status endpoint (now reports finalized).
curl -sk --cert <peer-cert> --key <peer-key> \
  https://gossipgrid-0.gossipgrid:8443/internal/peers/status | python3 -m json.tool
```

Confirm (from the peer-status response):

- `finalized` is `true`
- `dataLossProtected` is `true`
- `genesisAuthorizationOutstanding` is `false`
- `localBindingPresent` is `true`
- `bindingMatchesMetadata` is `true`

The seed's **lifecycle state** is `READY`. The ConfigMap's `data.metadata` field
now reports `initialized: true`, `genesisAuthorized: false`,
`genesisConsumer: "gossipgrid-0"`, and `dataLossProtected: true` (re-read and
verify, section 4).

## 18. What happens when the finalization Job fails

A failed finalization exits nonzero. Common failure modes (each is a distinct,
inspectable error — see the package documentation in
`internal/lifecycle/genesis`):

- **The metadata reports `Initialized=true` (already finalized)** →
  `ErrAlreadyFinalized` (a benign, expected outcome; exit 0, idempotent). The
  finalization is a no-op.
- **The metadata does not report an outstanding authorization** (e.g.,
  `GenesisAuthorized=false`, or `GenesisConsumer` is set, or `Initialized=true`
  with a mismatched consumer) → `ErrNoOutstandingAuthorization`. The
  authorization has been consumed or the record is in an unexpected state.
  Inspect the ConfigMap (section 4) and determine the next action.
- **The metadata's `ClusterID`, `EpochID`, or `GenesisConsumer` does not match
  the Job's flags** → `ErrFinalizationMismatch`. The operator passed incorrect
  identities or the metadata has been changed. Inspect the ConfigMap and
  re-launch the Job with the correct flags (if the metadata is still
  outstanding).
- **The operator did not pass `--confirm-seed-bound`** → the Job fails closed
  (the explicit operator gate is not satisfied). Re-launch with the flag.
- **The CAS conflicts** (another writer advanced `resourceVersion` between the
  read and the update) → the Job **re-reads** the authoritative object,
  re-checks the preconditions, and resolves: if the record is now finalized
  (matching), it reports `ErrAlreadyFinalized` (exit 0, idempotent); if the
  authorization was revoked or the record is not finalized, it reports
  `ErrUpdateAmbiguous` (the operator must inspect and decide).
- **The RBAC denies access (403)** → the finalization fails
  (`ErrMetadataPermissionDenied`). Verify the finalization RBAC (section 14)
  and the Role's rules (get + update on the named ConfigMap). Fix the RBAC and
  re-apply the Job.
- **The Kubernetes API is unavailable (5xx / timeout)** → the finalization fails
  closed. Wait for the API to recover and re-apply the Job (a safe, idempotent
  retry).

In every failure case, **inspect the ConfigMap** before any retry:

```bash
kubectl get configmap gossipgrid-cluster-metadata -n gossipgrid -o yaml
```

## 19. Scope boundaries (what this runbook does NOT cover)

This runbook covers the **grant** (Stage 1A) and the **seed binding and
finalization** (Stage 1B). It does **not** cover:

- Bootstrap synchronization orchestration (Stage 2) — the seed's initial data
  baseline fetch from an eligible `READY` source.
- A 7A-to-7B migration (the existing 7A staging deployment is data-bearing and
  is **not** touched by this workflow).
- 5C recovery or epoch reset.
- Any change to live Kubernetes resources (this runbook does not run against
  the existing 7A deployment).
- Peer network probing (the explicit operator gate is a verification via a
  reachable endpoint, not a network probe; a failed request is **unverified**,
  not "unbound").
