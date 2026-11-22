# Vault and cert-manager provisioning for GossipGrid (Milestone 7A)

This document describes the **safe, explicit** provisioning process for the
Vault secrets and cert-manager Issuers that the GossipGrid Helm chart
references. These are **cluster-wide prerequisites** owned by the cluster
operations track — the GossipGrid Helm chart does **not** create them.

The provisioning path for the browser authentication credential is:

```text
Vault KV v2 (mount "apps", secret path: gossipgrid/admin)
    |
    |  Vault Secrets Operator (VSO) watches and synchronizes
    v
Kubernetes Secret (gossipgrid-auth) in the gossipgrid namespace
    |
    |  mounted read-only into GossipGrid pods (a full Secret volume, never subPath)
    v
GossipGrid credential file (poll-based reload, same mechanism as peer mTLS)
```

## Prerequisites (verify against the actual cluster before provisioning)

The following are documented as cluster prerequisites but **must be verified
against the actual cluster** — do not assume they are installed merely because
they are documented. For each, run the `kubectl` command and confirm the
resource exists and is Ready.

| Prerequisite | Verify with | What to confirm |
| --- | --- | --- |
| **Vault** (KV v2 secrets engine, PKI secrets engine) | `kubectl -n vault get secret gossipgrid/admin` (if VSO is wired) or via the `vault` CLI | The KV v2 path `gossipgrid/admin` exists (or will be created below); the PKI secrets engine is enabled (for the Issuers below) |
| **Vault Secrets Operator (VSO)** | `kubectl get crd vaultstaticsecrets.secrets.hashicorp.com` | The VSO `VaultStaticSecret` CRD exists (and VSO is running in its namespace). |
| **cert-manager** | `kubectl get crd certificates.cert-manager.io` | The cert-manager CRDs (`Certificate`, `Issuer`, `ClusterIssuer`) exist; cert-manager is running |
| **cert-manager CSI driver** | `kubectl get csidriver csi.cert-manager.io` | The CSI driver `csi.cert-manager.io` is registered (required for per-pod peer mTLS via the Helm chart's `csi` volume). **If this is missing, the per-pod peer mTLS mount will fail — report the exact gap instead of inventing an installation.** |
| **Cilium Gateway API** | `kubectl get gatewayclass` | A `GatewayClass` named `cilium` (or the name you set in `gateway.gatewayClassName`) exists; Cilium is running with Gateway API support enabled |
| **MetalLB** (or L2 announcements) | `kubectl -n metallb get config` or check Cilium's LB IPAM | The LoadBalancer IP allocation mechanism is configured (for the Gateway's `LoadBalancer` type). If using L2 announcements, confirm the L2 mode is enabled in Cilium |
| **Cluster DNS domain** | `kubectl get --raw '/api/v1/namespaces/kube-system/configmaps/coredns' -o jsonpath='{.data.'\''clusters'\''}'` | The cluster DNS domain (default `cluster.local`). If it differs, update `clusterDomain` in the Helm values |

## 1. Browser authentication credential (Vault KV v2)

The credential file is a single JSON document (format version 1, algorithm
`PBKDF2-HMAC-SHA-256`) with hex-encoded binary fields. It is produced by the
`gossipgrid credential` CLI (Milestone 6, implemented). **Do not hand-author
the credential file** — use the CLI so the PBKDF2 parameters, salt, verifier,
generation, and signing key are correctly generated and the file is 0600.

### 1a. Produce the credential file with the CLI

Run the CLI on a trusted machine (not inside the cluster). The CLI uses
interactive hidden prompts (no password in process arguments) and writes the
file atomically with 0600 permissions.

```sh
# Initial provisioning (create the credential file).
gossipgrid credential init \
  --out /tmp/gossipgrid-credential.json

# The CLI prompts for the admin password (hidden, with confirmation).
# The resulting file contains: format_version, algorithm, iterations,
# salt (32 bytes), verifier (32 bytes, the PBKDF2 output), generation
# (a counter), signing_key (32 bytes, the shared HMAC-SHA256
# session-signing key). The plaintext password is NEVER stored in the
# file.
```

Verify the file is well-formed before writing it to Vault:

```sh
gossipgrid credential verify \
  --file /tmp/gossipgrid-credential.json
# Prompts for the password; confirms the verifier matches.
```

### 1b. Write the credential file to Vault KV v2

The Vault KV v2 secret lives on the **`apps` mount** (the mount reserved for
application secrets), at the path `gossipgrid/admin` *within that mount* —
i.e. the full Vault path is `apps/gossipgrid/admin`. The secret has a single
data key, `admin`, whose value is the JSON credential file. VSO projects this
key into the Kubernetes Secret `gossipgrid-auth` as a key named `admin`
(matching `auth.secretKey` in the Helm values).

The mount is a **planned default, not a fixed one**: this document and the
`VaultStaticSecret` example below use `apps` consistently, but the `mount`
field on the `VaultStaticSecret` (and the mount in the `vault kv` commands)
must be set to whatever KV v2 mount your cluster actually uses for
application secrets. The full Vault path (mount + path within the mount) is
shown in each command so the two stay consistent.

```sh
# Read the file into a variable (the file is 0600; avoid echoing its
# contents to the terminal or to logs).
CRED_JSON=$(cat /tmp/gossipgrid-credential.json)

# Write to Vault KV v2. The full path is <mount>/<path-within-mount>
# (the mount "apps" is the planned default; adjust it if your cluster uses
# a different application-secrets mount). The "admin" after the path is the
# data key name, not part of the Vault path.
vault kv put apps/gossipgrid/admin admin="$CRED_JSON"

# Verify (this prints the data key names, NOT the values — safe).
vault kv metadata get apps/gossipgrid/admin
```

**Security notes:**

- The plaintext password is never stored in Vault, in the Kubernetes Secret,
  or in environment variables. Only the versioned credential record
  (verifier, salt, algorithm, version, generation, signing key) is in the
  Secret.
- The Vault KV v2 secret is versioned; each rotation creates a new version.
  VSO tracks the latest version by default (configure the VSO
  `VaultStaticSecret` `version` field to track the latest version — `0` —
  or a specific version if you need pinning).
- The credential file is never committed to Git. The CLI's `--out` path and
  the Vault secret are the only places the file exists.

### 1c. Vault Secrets Operator (VSO) resource

The VSO `VaultStaticSecret` resource (in the `gossipgrid` namespace) tells VSO
to synchronize the Vault KV v2 secret into a Kubernetes Secret named
`gossipgrid-auth`. This is a cluster-wide resource (owned by the cluster
operations track); the GossipGrid Helm chart does not create it. The
`auth.secretName` in the Helm values must match the `destination.name` here.

The `VaultStaticSecret` CRD (apiVersion `secrets.hashicorp.com/v1beta1`)
projects the Vault KV v2 secret's data keys into the destination Kubernetes
Secret with the same key names. The Vault KV v2 secret
(`apps/gossipgrid/admin` — the `apps` mount, path `gossipgrid/admin` within it)
has a single data key `admin` (the JSON credential file), which VSO projects
as the K8s Secret key `admin` (matching `auth.secretKey` in the Helm values).

**The `mount` field is required** for a KV v2 secret: it names the Vault KV v2
mount the secret lives on. The `path` field is the path *within* that mount
(the two fields combine to the full Vault path `apps/gossipgrid/admin`). Do
not bake the mount into `path` and leave `mount` empty — that is the common
mistake, and the `path` field alone is not enough to locate a KV v2 secret.
This example uses the planned `apps` mount; set `mount` to your cluster's
application-secrets mount if it differs.

```yaml
apiVersion: secrets.hashicorp.com/v1beta1
kind: VaultStaticSecret
metadata:
  name: gossipgrid-auth
  namespace: gossipgrid
spec:
  # The Vault KV v2 mount the secret lives on. The planned default is "apps"
  # (the application-secrets mount); set this to your cluster's mount if it
  # differs. This field is REQUIRED for a KV v2 secret and is NOT inferred
  # from the path.
  mount: apps
  # The path WITHIN the mount (the part after the mount point). Do not
  # include the mount in this field (it is taken from the mount field above).
  path: gossipgrid/admin
  # The version to track. 0 means "latest" (the most recent version — the
  # default for rotation). Set a specific version number for pinning.
  version: 0
  # The type of Vault secret. "kv-v2" for KV v2.
  type: kv-v2
  # How often VSO refreshes the secret from Vault. A 30s refresh interval
  # ensures rotation is detected promptly (the GossipGrid poll-based
  # reloader, default 2s, picks up the kubelet's atomic file swap shortly
  # after VSO updates the K8s Secret).
  refreshAfter: 30s
  # The VaultAuth resource VSO uses to authenticate to Vault.
  # This is a cluster-wide resource owned by the cluster operations track;
  # adjust to match your VSO VaultAuth name. If the VaultAuth is in the
  # VSO operator's namespace and its name is "default", this field can be
  # omitted (VSO defaults to the "default" VaultAuth in the operator's
  # namespace). For a named VaultAuth in a specific namespace, use the
  # "namespace/name" form (e.g. "vault/vault-auth-gossipgrid").
  vaultAuthRef: <your-vso-vaultauth-name>
  # The destination Kubernetes Secret that VSO creates or updates.
  destination:
    # The K8s Secret name (must match auth.secretName in the Helm values).
    name: gossipgrid-auth
    # Whether VSO creates the Secret if it does not exist.
    # Set to true for initial provisioning; set to false if the Secret is
    # pre-created (e.g. by the operator) and VSO should only update it.
    create: true
    # The K8s Secret type. Opaque is the default and is correct for this use.
    type: Opaque
```

**How VSO projects the data keys:**

VSO reads the Vault KV v2 secret at `apps/gossipgrid/admin` (the `apps` mount,
path `gossipgrid/admin` within it — the combination of the `mount` and `path`
fields above). The secret has a single data key `admin` whose value is the
JSON credential file. VSO projects this into the destination K8s Secret as a
key named `admin` with the same value. The resulting K8s Secret has one data
key: `admin` (matching `auth.secretKey` in the Helm values).

**Note on the `mount` and `path` fields:** The `mount` field names the Vault
KV v2 mount and the `path` field is the path *within* that mount (the part
after the mount point). The two combine to the full Vault path
(`apps/gossipgrid/admin`). The mount is **not** inferred from the Vault
connection or the VaultAuth — it must be stated explicitly in `mount`. If your
Vault uses a different application-secrets mount (e.g. `my-kv`), set `mount:
my-kv` and keep `path: gossipgrid/admin` (do not put `my-kv/...` in `path`).

### 1d. Rotation

To rotate the admin password:

```sh
# 1. Produce a new credential file (increment the generation; optionally a
#    new signing key with --new-signing-key).
gossipgrid credential rotate \
  --file /tmp/gossipgrid-credential.json \
  --out /tmp/gossipgrid-credential-rotated.json
# Prompts for the NEW password (hidden, with confirmation). The generation
# is incremented; the old-generation sessions will be rejected once each
# replica loads the new generation (no overlap window).

# 2. Verify the new file.
gossipgrid credential verify --file /tmp/gossipgrid-credential-rotated.json

# 3. Write to Vault (a new KV v2 version; same apps mount as section 1b).
CRED_JSON=$(cat /tmp/gossipgrid-credential-rotated.json)
vault kv put apps/gossipgrid/admin admin="$CRED_JSON"
```

**Rotation propagation (eventual, not instantaneous):**

- VSO detects the Vault secret change and atomically updates the Kubernetes
  Secret `gossipgrid-auth`.
- The kubelet atomically swaps in the updated file in the mounted Secret
  volume (no `subPath` — the full-volume swap is honored).
- Each GossipGrid replica's poll-based reloader (default 2s) detects the file
  change, validates it, and publishes a new immutable credential snapshot
  **without restarting the pod or process**.
- Propagation across the 5 replicas is **eventual** (each loads on its own
  poll cycle). There is **no overlap window**: once a replica has loaded the
  new generation, it rejects old-generation sessions immediately
  (generation binding). The `MAX_STALE` window (5m) bounds the "stuck in a
  bad state" → 503 signal, **not** a session-overlap window.
- **Measure the actual propagation window** during the staging acceptance
  test (the time from the Vault write to the last replica rejecting
  old-generation sessions). Do not assume it.

## 2. Workload CA trust bundle (K8s Secret)

The GossipGrid pods mount a workload CA trust bundle at
`/certs-ca/ca-bundle.pem` (a cluster-owned K8s Secret, **not** created by
the Helm chart). This is the dedicated workload CA trust hierarchy for peer
mTLS (a separate trust domain from the browser CA).

The Secret is mounted as a **full volume** (no subPath) at the directory
`/certs-ca` (the `peerTLS.trustBundleMountPath` Helm value). The bundle file
appears inside it at `/certs-ca/ca-bundle.pem` (the `peerTLS.trustBundleSecretKey`
Helm value). A full-volume mount is required so the kubelet can atomically
swap in an updated bundle on CA rotation **without a pod restart** — a subPath
mount would pin the pod to the initial file version.

Provisioning:

```sh
# The trust bundle is one or more workload CA certificates (PEM). It may hold
# multiple CAs during a CA transition. Create the K8s Secret in the
# gossipgrid namespace. The key name MUST be "ca-bundle.pem" (matching
# peerTLS.trustBundleSecretKey in the Helm values) so the items projection
# in the StatefulSet maps it correctly.
kubectl -n gossipgrid create secret generic gossipgrid-workload-ca-bundle \
  --from-file=ca-bundle.pem=/path/to/workload-ca-bundle.pem
```

The Secret must exist in the `gossipgrid` namespace before the pods start; a
missing Secret causes the pods to fail at startup (fail-closed). The
`peerTLS.trustBundleSecretName` in the Helm values must match the Secret name,
and the Secret's key name must match `peerTLS.trustBundleSecretKey`
(`ca-bundle.pem` by default).

**The trust bundle is a cluster-wide prerequisite** (owned by the cluster
operations track / trust-manager). The GossipGrid Helm chart references it but
does not create it. If the cluster uses trust-manager to distribute the CA
bundle, configure the trust-manager `TrustBundle` resource to project the
workload CA into the `gossipgrid-workload-ca-bundle` Secret (or adjust the
Helm value to match the trust-manager Secret name).

## 3. Peer mTLS Issuer (cert-manager, Vault-backed)

The per-pod peer mTLS certificates are issued **directly by the cert-manager
CSI driver** (driver: `csi.cert-manager.io`) using the Vault-backed Issuer
configured in the StatefulSet's CSI volume attributes
(`csi.cert-manager.io/issuer-name`, `csi.cert-manager.io/issuer-kind`).
No cert-manager Certificate resource is needed — the CSI driver requests
certificates from cert-manager based on the `volumeAttributes` in the Pod
spec, creating a unique certificate per pod without an intermediate Secret.
This is a cluster-wide prerequisite (owned by the cluster operations track);
the GossipGrid Helm chart does not create the Issuer.

The Issuer must be Vault-backed and configured to sign certificates with the
workload CA (the same CA in the trust bundle, section 2).

```yaml
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: vault-workload
  namespace: gossipgrid
spec:
  vault:
    # The Vault PKI secrets engine path (the workload CA signing role).
    # Adjust to match your Vault PKI mount and role.
    path: pki/root/sign
    # The Vault secret (token or Kubernetes auth) for the Issuer to use.
    # This is a cluster-wide credential (owned by the cluster operations
    # track); do not put it in this repo.
    secretRef:
      name: vault-workload-issuer-creds
      key: token
    # The CA certificate (the workload CA that signs the per-pod certs).
    # This must be the same CA as in the trust bundle (section 2).
    caSecretRef:
      name: workload-ca-cert
```

The `peerTLS.certManagerIssuerName` in the Helm values must match the Issuer
name (or set `certManagerClusterIssuerName` for a ClusterIssuer — the
`csi.cert-manager.io/issuer-kind` volume attribute in the StatefulSet is
set to `ClusterIssuer` in that case). The workload CA (the `caSecretRef`)
must match the trust bundle in section 2 (the same CA that the pods trust).

**Prerequisite:** the cert-manager CSI driver (`csi.cert-manager.io`) must be
installed and running. Verify with:

```sh
kubectl get csidriver csi.cert-manager.io
```

If it is missing, the per-pod peer mTLS mount will fail (blocker).

## 4. Browser TLS Issuer (cert-manager, Vault-backed)

The browser-facing TLS certificate (Gateway HTTPS termination) is issued by a
separate cert-manager Issuer (Vault-backed) for the **browser CA** (a
separate trust domain from the workload CA). This Issuer is a cluster-wide
prerequisite; the GossipGrid Helm chart does not create it.

```yaml
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: vault-browser
  namespace: gossipgrid
spec:
  vault:
    path: pki/browser/sign
    secretRef:
      name: vault-browser-issuer-creds
      key: token
    caSecretRef:
      name: browser-ca-cert
```

The browser CA (the `caSecretRef`) must be the CA whose chain is pre-installed
in the browser's trust store. The `gateway.browserIssuerName` in the Helm
values must match the Issuer name.

## 5. Verification checklist (before deploying the Helm chart)

Before running `helm install` (or applying the Argo CD Application), verify
each prerequisite:

```sh
# 1. The VSO-created K8s Secret exists and has the "admin" data key.
#    (The VaultStaticSecret resource in section 1c creates this Secret.)
kubectl -n gossipgrid get secret gossipgrid-auth -o jsonpath='{.data}' | \
  jq 'keys'
# Expected: ["admin"]

# 1b. The VaultStaticSecret resource exists and is in sync, and its mount
#     and path resolve to the provisioned secret (the "apps" mount).
kubectl -n gossipgrid get vaultstaticsecret gossipgrid-auth -o jsonpath='{.spec.mount},{.spec.path},{.status}'
# Expected: "apps","gossipgrid/admin", and a status showing the secret is
# synced (check your VSO version for the exact status format; older versions
# may not expose status).

# 2. The workload CA trust bundle Secret exists and has the "ca-bundle.pem"
#    data key.
kubectl -n gossipgrid get secret gossipgrid-workload-ca-bundle -o jsonpath='{.data}' | \
  jq 'keys'
# Expected: ["ca-bundle\.pem"]

# 3. The peer mTLS Issuer exists and is Ready.
kubectl -n gossipgrid get issuer vault-workload -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'
# Expected: True

# 4. The browser TLS Issuer exists and is Ready.
kubectl -n gossipgrid get issuer vault-browser -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'
# Expected: True

# 5. The cert-manager CSI driver is registered (required for per-pod peer mTLS).
kubectl get csidriver csi.cert-manager.io
# Expected: the driver is listed (no error).

# 6. The Cilium GatewayClass exists.
kubectl get gatewayclass cilium
# Expected: the GatewayClass is listed (no error).

# 7. The cluster DNS domain (confirm it matches clusterDomain in the Helm values).
kubectl get --raw '/api/v1/namespaces/kube-system/configmaps/coredns' -o jsonpath='{.data.'\''clusters'\''}'
```

## 6. Live Cilium datapath assumptions (verify against the cluster)

The Helm chart's Gateway and NetworkPolicy rest on Cilium datapath
assumptions that **cannot be verified from the chart or this repository** —
they must be tested against the actual cluster before the staging
deployment. The chart does not weaken the policy by default; the checks
below are what the operator must run to confirm the policy actually
enforces as intended.

**Assumption A: Cilium enforces the standard `NetworkPolicy` on the pod
datapath.** The chart renders a standard Kubernetes `NetworkPolicy`
(`networking.k8s.io/v1`). Cilium implements the standard API, but the
datapath enforcement (whether Cilium's native policy or the
KubeProxy-reconciliation path is active) depends on the Cilium version and
configuration. **Verify:** from a non-GossipGrid pod (e.g. a `busybox`
deployed in a different namespace), attempt to reach the GossipGrid web
port (`8080`) and peer port (`8443`). Both must be refused. If they are
reachable, the NetworkPolicy is not being enforced by the Cilium datapath
as expected — report the exact gap (Cilium version, policy mode,
reconciliation status) instead of assuming it will work.

**Assumption B: The Gateway data-plane pods are in the namespace
`networkPolicy.gatewayNamespace` (default `kube-system`).** The NetworkPolicy
allows ingress to the web port only from pods in that namespace. If the
Cilium Gateway data-plane pods run in a different namespace (e.g.
`cilium-gateway`), the policy will **block the Gateway's own traffic to the
web Service** — the browser will get connection refused. **Verify:** run
`kubectl get pods -A -o wide | grep -i gateway` (or the actual label the
Cilium Gateway data-plane pods carry) to find the namespace, then set
`networkPolicy.gatewayNamespace` in the Helm values (or
`values-staging.yaml`) to that namespace. Do not set it to `0.0.0.0/0` or
remove the policy to "make it work" — that defeats the purpose.

**Assumption C: The Gateway's LoadBalancer IP is reachable from the LAN
via the MetalLB / L2 announcement path.** The Gateway's
`infrastructure.parametersRef` (kind `Service`, name the Gateway name)
asks Cilium to create a LoadBalancer-type Service. Whether Cilium
allocates an IP and advertises it (via MetalLB or L2 announcements)
depends on the cluster's Cilium configuration. **Verify:** after
deployment, `kubectl -n gossipgrid get gateway gossipgrid-gateway -o wide`
must show an `EXTERNAL-IP` (not `<pending>`). If it is pending, the
MetalLB / L2 announcement path is not configured — report the exact gap
and consider `lbType: ClusterIP` (in-cluster access only) as a stopgap for
the 7A smoke test, or use `kubectl port-forward` to the web Service.

**Assumption D: The browser TLS certificate is loaded by the Gateway and
the public FQDN is resolvable.** The Gateway's HTTPS listener references
the `gossipgrid-browser-tls` Secret (created by the cert-manager
Certificate). **Verify:** `curl -k -s -v
https://<publicFQDN>/ 2>&1 | grep -A5 "SSL certificate\|subject\|issuer"`
must show a certificate whose subject/SAN matches the public FQDN and
whose issuer is the Vault browser CA. If the certificate is not loaded or
the FQDN does not resolve, the Gateway is not serving the expected TLS
— report the exact gap (Secret contents, DNS record, Gateway status).

**Assumption E: Peer mTLS traffic between GossipGrid pods flows through the
Cilium datapath (not via the Gateway or the web Service).** The peer port
(`8443`) is **not** exposed through the Gateway or the web Service — it is
reachable only via the headless Service (per-pod DNS) and the peer mTLS
listener. **Verify:** from a GossipGrid pod, `curl --cert /certs/tls.crt
--key /certs/tls.key --cacert /certs-ca/ca-bundle.pem
https://gossipgrid-1.gossipgrid-headless.gossipgrid.svc.cluster.local:8443/
internal/status` must return 200. From a non-GossipGrid pod, the same
request (with a valid GossipGrid cert) must be refused by the NetworkPolicy
(only GossipGrid pods can reach the peer port). If the peer port is
reachable from outside the GossipGrid pod set, the NetworkPolicy is not
enforcing the peer-port restriction — report the exact gap.

These five assumptions are the **cluster-specific** prerequisites that
cannot be verified from the chart alone. The operator must run the
corresponding `kubectl` / `curl` commands against the actual cluster
before the 7A staging deployment and record the results in the staging
report. The chart is deliberately conservative (no `0.0.0.0/0`, no
weakened policy by default); the verification above is to confirm the
conservative policy actually enforces as intended on the cluster's
Cilium datapath.

## 7. Exact gaps to report (do not invent installations)

If any of the above prerequisites are **missing** (e.g. the cert-manager CSI
driver is not registered, VSO is not installed, or the Cilium GatewayClass
does not exist), **report the exact gap** instead of inventing an
installation. The cluster operations track owns those resources; the GossipGrid
team should not install or configure them. Record the gap in the staging
acceptance test report (section 9 of the task description) as a **blocker
requiring operator action**.

Typical gaps and their impact:

| Gap | Impact on GossipGrid staging |
| --- | --- |
| cert-manager CSI driver (`csi.cert-manager.io`) missing | Per-pod peer mTLS mount fails; pods fail at startup (fail-closed). **Blocker.** |
| VSO not installed | The `gossipgrid-auth` Secret is not created; authenticated requests return 503 (fail-closed) until the Secret is provisioned manually. **Blocker** (or provision the Secret manually as a stopgap). |
| Cilium GatewayClass missing | The Gateway cannot be created; browser HTTPS is unavailable. **Blocker** (or use a different GatewayClass name). |
| MetalLB / L2 announcements not configured | The Gateway's LoadBalancer IP is not allocated; the public FQDN is not reachable from the LAN. **Blocker** (or use ClusterIP and port-forward). |
| Vault PKI secrets engine not enabled | The Issuers cannot issue certificates; the `Certificate` resources stay in `Pending`. **Blocker.** |
| Cluster DNS domain differs from `cluster.local` | Peer discovery and the identity suffix are wrong; peer mTLS identity checks fail. **Blocker** (update `clusterDomain` in the Helm values). |
