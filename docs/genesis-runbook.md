# GossipGrid genesis grant runbook (Milestone 5B, Stage 1A)

This runbook documents the **explicit, operator-authorized** creation of
GossipGrid's initial cluster metadata. It is a guide for the operator; it does
**not** automatically run the grant, create anything, or deploy anything.

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

## 9. Scope boundaries (what this runbook does NOT cover)

This runbook covers the **grant** only (Milestone 5B, Stage 1A). It does **not**
cover:

- Seed `Bind()` integration (Stage 1B).
- `AuthorizeGenesis()` lifecycle transitions (Stage 1B).
- Genesis finalization (the CAS that sets `initialized: true`,
  `genesisAuthorized: false`, `genesisConsumer: gossipgrid-0`, and
  `dataLossProtected: true`) — Milestone 5B/5C.
- Bootstrap synchronization — Milestone 5B.
- A 7A-to-7B migration.
- 5C recovery or epoch reset.
- Any change to live Kubernetes resources (this runbook does not run against
  the existing 7A deployment).
