# GossipGrid Milestone 7A staging acceptance-test runbook

This runbook documents the **14 acceptance tests** for the Milestone 7A
staging deployment, the **deployment and smoke-test commands**, and the
**exact blockers** that require operator action. It is a guide for the
operator; it does **not** automatically run any test or deploy anything.

**Milestone 7A constraint (repeated for emphasis):** lifecycle enforcement is
**disabled** (`GOSSIPGRID_LIFECYCLE_ENABLED=false`, the default) because
Milestones 5B (Kubernetes metadata provider) and 5C (operator recovery
procedure) are not yet implemented. The staging phase therefore has
**reduced safety guarantees**: a pod replacement loses its local SQLite data
and is re-reconciled by anti-entropy (a best-effort test, **not** an
authoritative-snapshot guarantee). Do **not** claim authoritative initial
snapshot synchronization, safe operator-controlled one-time genesis,
lifecycle-based readiness authorization, or catastrophic recovery guarantees
— those capabilities belong to later milestones (5B/5C/7B).

## 0. Pre-deployment prerequisites (verify first)

Before deploying, verify all cluster prerequisites (see
`deploy/vault/PROVISIONING.md` for the exact `kubectl` commands). The
following must be confirmed against the **actual cluster** — do not assume
they are installed merely because they are documented:

- [ ] Vault (KV v2 + PKI secrets engines) is reachable and the
      `gossipgrid/admin` KV v2 secret is provisioned.
- [ ] Vault Secrets Operator (VSO) is installed and the
      `gossipgrid-auth` K8s Secret is created in the `gossipgrid` namespace.
- [ ] cert-manager is installed and the `vault-workload` and `vault-browser`
      Issuers are Ready.
- [ ] **cert-manager CSI driver is registered** (`kubectl get csidriver
      csi.cert-manager.io`). If missing, this is a **blocker** — report the
      exact gap instead of inventing an installation.
- [ ] Cilium Gateway API is enabled and the `cilium` GatewayClass exists.
- [ ] MetalLB (or L2 announcements) is configured for LoadBalancer IP
      allocation.
- [ ] The cluster DNS domain matches `clusterDomain` in the Helm values
      (default `cluster.local`).
- [ ] The `gossipgrid-workload-ca-bundle` K8s Secret exists in the
      `gossipgrid` namespace (the workload CA trust bundle).
- [ ] An immutable container image (commit SHA or digest) is published to the
      registry (see `make image` and the GitLab CI pipeline).

## 1. Deployment

**Do not deploy to the real cluster without an explicit review of the
generated manifests and configuration.** The deployment is owned by Argo CD
(not by a direct `helm upgrade`), to avoid a competing deployment controller.

### 1a. Apply the Argo CD Application (GitOps)

The Argo CD Application manifest is at `deploy/argocd/application.yaml`
(a template — adjust `source.repoURL` and `destination.namespace` to match
your cluster). The staging values are at
`deploy/helm/gossipgrid/values-staging.yaml` (a valid, self-contained single
source — it lives in the chart directory so `helm.valueFiles` references it by
a path relative to the chart root with no manual copy; set the immutable
image reference and FQDN there for the actual staging deployment).

```sh
# Apply the Argo CD Application (adjust the namespace to match your Argo CD
# installation). This creates the Application resource; Argo CD then
# reconciles the cluster to the recorded desired state.
kubectl apply -f deploy/argocd/application.yaml

# Watch Argo CD reconcile (the Application should reach "Synced" and "Healthy").
argocd app get gossipgrid -n argocd
# or:
kubectl -n argocd get application gossipgrid -o wide
```

### 1b. Alternative: direct `helm install` (NOT recommended for 7A)

For a one-off staging test (before the Argo CD integration is wired), a direct
`helm install` is possible but creates a competing deployment controller
(Argo CD and Helm would both own the resources). **Prefer Argo CD.** If you
must use `helm install` for a quick test, delete the resources before
switching to Argo CD.

```sh
# Build the immutable image (only when the manifests are ready and the build
# is actually needed — see the build-efficiency policy).
make image IMAGE=registry.gitlab.example.internal/gossipgrid

# Push the image to the registry (adjust the registry URL).
docker push registry.gitlab.example.internal/gossipgrid:<tag>

# Install the chart (adjust the namespace and values).
helm install gossipgrid deploy/helm/gossipgrid \
  -n gossipgrid --create-namespace \
  -f deploy/helm/gossipgrid/values-staging.yaml
```

### 1c. Verify the pods started

```sh
# All 5 pods should be Running and Ready (eventually — the startup probe
# accommodates CSI certificate issuance and credential load).
kubectl -n gossipgrid get pods -l app.kubernetes.io/name=gossipgrid -o wide

# Check the StatefulSet.
kubectl -n gossipgrid get statefulset gossipgrid

# Check the Services.
kubectl -n gossipgrid get service

# Check the Gateway (if enabled).
kubectl -n gossipgrid get gateway gossipgrid-gateway
kubectl -n gossipgrid get httproute gossipgrid-gateway-web
```

**Expected:** 5 pods `1/1 Running`, the StatefulSet `5/5 Ready`, the headless
Service `gossipgrid-headless` with 5 endpoints (including unready pods —
`publishNotReadyAddresses: true`), the web Service `gossipgrid-web` with only
ready endpoints, and the Gateway with an allocated LoadBalancer IP.

## 2. Acceptance tests (14 tests)

Run each test and record the result (PASS/FAIL) with evidence (command output,
log excerpts, or screenshots). **Do not invent successful tests or claim
resources were deployed without actually verifying them.**

### Test 1: Five GossipGrid pods run successfully

```sh
kubectl -n gossipgrid get pods -l app.kubernetes.io/name=gossipgrid
# Expected: gossipgrid-0, -1, -2, -3, -4 all 1/1 Running (no CrashLoopBackOff,
# no Pending). Check the pod events for any CSI or credential mount errors.
kubectl -n gossipgrid describe pod gossipgrid-0 | grep -A20 "Events:"
```

**PASS criteria:** all 5 pods `1/1 Running` for at least 5 minutes without
restarts (the restart count in `kubectl get pods -o wide` is 0).

### Test 2: Each pod has independent SQLite storage

```sh
# Create a message on gossipgrid-0 (via the public API, authenticated).
# First, get a session cookie (login) — see Test 8.
# Then, POST a message to gossipgrid-0's web Service (load-balanced).
curl -k -s -c /tmp/cookies.txt -X POST \
  -H "Content-Type: application/json" \
  -d '{"body":"test-msg-0"}' \
  https://<publicFQDN>/api/messages

# Now, check the message on each pod's local SQLite via the peer mTLS
# /internal/status (which reports the local store) or by port-forwarding to
# each pod's web port and GET /api/messages.
for i in 0 1 2 3 4; do
  kubectl -n gossipgrid port-forward pod/gossipgrid-$i 18080:8080 &
  PF_PID_$i=$!
  sleep 1
  echo "=== gossipgrid-$i ==="
  curl -s -b /tmp/cookies.txt http://localhost:18080/api/messages | jq '.[] | .id, .body, .origin'
  kill $PF_PID_$i
done
```

**PASS criteria:** the message created on one pod appears on all 5 pods
(eventually — via gossip fanout and anti-entropy). Each pod's local SQLite
is independent (the `origin` field shows which pod created the message; the
message is replicated, not shared).

**Note:** in 7A (lifecycle disabled), a pod replacement loses its local SQLite
and is re-reconciled by anti-entropy (best-effort). This test does NOT validate
authoritative initial snapshot synchronization or safe genesis.

### Test 3: Peer DNS discovery works

```sh
# From a GossipGrid pod, resolve the headless Service DNS names.
kubectl -n gossipgrid exec gossipgrid-0 -- nslookup gossipgrid-1.gossipgrid-headless.gossipgrid.svc.cluster.local
# or (if nslookup is not in the scratch image, use the /etc/hosts or the
# application's discovery log):
kubectl -n gossipgrid logs gossipgrid-0 | grep -i "discovery\|peer" | head -20
```

**PASS criteria:** each pod's DNS name resolves to its pod IP (5 A records for
`gossipgrid-headless.gossipgrid.svc.cluster.local`, one per pod). The
application's discovery log shows 5 peers discovered (including itself).

### Test 4: Peer mTLS connections succeed

```sh
# From a GossipGrid pod, make an mTLS request to another pod's peer endpoint.
# The per-pod certificate is at /certs/tls.crt, the key at /certs/tls.key,
# and the trust bundle at /certs-ca/ca-bundle.pem.
kubectl -n gossipgrid exec gossipgrid-0 -- \
  curl -s --cert /certs/tls.crt --key /certs/tls.key \
  --cacert /certs-ca/ca-bundle.pem \
  https://gossipgrid-1.gossipgrid-headless.gossipgrid.svc.cluster.local:8443/internal/status
```

**PASS criteria:** the request returns 200 with the peer's status JSON (node ID
`gossipgrid-1`, version, etc.). The mTLS handshake succeeds (TLS 1.3, mutual
auth, identity authorized). Check the application log for any mTLS errors.

**Negative test (verify identity authorization):** attempt an mTLS request with
a certificate for the wrong pod identity (e.g. a cert for `gossipgrid-99` or a
cert from a different StatefulSet). The handshake should be **rejected**
(403 or connection reset) because the identity check fails.

### Test 5: Messages propagate between replicas

```sh
# Create a message on one pod and verify it appears on the others (via
# gossip fanout — automatic epidemic propagation, Milestone 4B).
# Use the same method as Test 2, but verify the propagation is automatic
# (no manual sync trigger).
#
# The gossip engine fans out new messages to .Values.gossip.fanout (default 2)
# peers for .Values.gossip.rounds (default 3) rounds. With 5 replicas, a
# message should reach all replicas within a few seconds (eventual, not
# instantaneous).
sleep 15  # allow gossip fanout to propagate
# Re-run the per-pod GET /api/messages from Test 2 and confirm the message
# is present on all 5 pods.
```

**PASS criteria:** a message created on one pod appears on all 5 pods within
~30 seconds (the gossip fanout rounds + propagation delay). The `origin` field
shows the creating pod.

### Test 6: Anti-entropy reconciles missing messages

```sh
# Simulate a missing message: delete a message from one pod's local SQLite
# (via a direct SQLite query on the pod's /data/gossipgrid.db), then verify
# anti-entropy re-fetches it from a peer.
#
# The anti-entropy engine (Milestone 4C) periodically (default 30s) compares
# bounded message ID inventories with authenticated peers and fetches missing
# records. A message deleted from one pod's local store should be
# re-fetched from a peer that still has it.
#
# NOTE: deleting a message from the local SQLite is a manual intervention
# (not a normal operation). This test validates the anti-entropy
# reconciliation path.
kubectl -n gossipgrid exec gossipgrid-2 -- \
  /bin/sh -c 'sqlite3 /data/gossipgrid.db "DELETE FROM messages WHERE id=\"<message-id>\""' 2>/dev/null || \
  # The scratch image may not have sqlite3; use the application's API or a
  # debug container. This is a manual test step.
  echo "sqlite3 not available in scratch image; use a debug container"

sleep 45  # allow one anti-entropy cycle (default 30s interval)
# Re-run the per-pod GET /api/messages and confirm the deleted message is
# back on gossipgrid-2 (re-fetched from a peer).
```

**PASS criteria:** a message deleted from one pod's local store is re-fetched
by anti-entropy within ~60 seconds (one cycle + fetch delay). The message
appears on the pod again (idempotent insert — `ON CONFLICT DO NOTHING`).

**Note:** in 7A (lifecycle disabled), anti-entropy is best-effort reconciliation
within the provisional trust model. This test does NOT validate authoritative
initial snapshot synchronization or safe genesis.

### Test 7: Browser HTTPS works through Cilium Gateway

```sh
# Access the public FQDN over HTTPS (the Cilium Gateway terminates TLS).
# The browser CA must be installed in the trust store (use -k to skip
# verification for the test; in production, the CA is pre-installed).
curl -k -s https://<publicFQDN>/
# Expected: the GossipGrid web UI (HTML) or a 401 (authentication required).
#
# Verify the TLS certificate (the browser cert issued by the Vault browser CA).
curl -k -s -v https://<publicFQDN>/ 2>&1 | grep -A5 "SSL certificate\|subject\|issuer"
```

**PASS criteria:** the public FQDN is reachable over HTTPS (443) via the
Cilium Gateway; the TLS certificate is issued by the Vault browser CA (the
subject CN / SAN is the public FQDN); the response is the GossipGrid web UI
or a 401 (authentication required, see Test 8).

### Test 8: Web authentication is enforced

```sh
# Without a session cookie, the API endpoints should return 401 (authentication
# required) — not 200 (unauthenticated access).
curl -k -s -o /dev/null -w "%{http_code}\n" https://<publicFQDN>/api/messages
# Expected: 401 (or 503 if the credential file is not yet loaded — see the
# fail-closed note below).

# With a valid session cookie (login), the endpoints should return 200.
# First, log in (POST /login with the admin password — the password is NOT
# stored in Git or in the K8s Secret; it is only in the credential file's
# verifier, which is PBKDF2-hashed).
curl -k -s -c /tmp/cookies.txt -X POST \
  -H "Content-Type: application/json" \
  -d '{"password":"<admin-password>"}' \
  https://<publicFQDN>/login
# Expected: 200 with a session cookie set (the cookie is HttpOnly, Secure,
# SameSite=Strict; no password or verifier in the cookie).

# Now, use the cookie to GET /api/messages.
curl -k -s -b /tmp/cookies.txt https://<publicFQDN>/api/messages
# Expected: 200 with the message list.
```

**PASS criteria:** unauthenticated requests to `/api/messages` return 401 (or
503 if the credential file is not yet loaded — fail-closed); authenticated
requests (with a valid session cookie) return 200. The `/healthz` and `/readyz`
endpoints are always unauthenticated (200).

**Fail-closed note:** if the `gossipgrid-auth` K8s Secret is not yet
provisioned by VSO, authenticated requests return 503 (fail-closed — no
silent anonymous access). This is the correct behavior; provision the Secret
(see `deploy/vault/PROVISIONING.md`) and re-test.

### Test 9: One login works across all five replicas

```sh
# Log in once (on any replica, via the load-balanced public FQDN) and use the
# session cookie on all 5 replicas. The session is signed with the common
# session-signing key (in the credential file), so a single login is valid on
# every replica (no server-side session store).
#
# The public FQDN is load-balanced (via the Cilium Gateway -> web Service ->
# ready pods). A browser refresh can land on a different replica. The UI
# explicitly displays which replica responded.
for i in 0 1 2 3 4; do
  kubectl -n gossipgrid port-forward pod/gossipgrid-$i 18080:8080 &
  PF_PID=$!
  sleep 1
  echo "=== gossipgrid-$i ==="
  # Use the SAME session cookie (from Test 8) on each pod.
  curl -s -b /tmp/cookies.txt http://localhost:18080/api/messages -o /dev/null -w "HTTP %{http_code}\n"
  kill $PF_PID
done
```

**PASS criteria:** the same session cookie (from a single login) is accepted on
all 5 replicas (all return 200, not 401). The UI shows which replica responded
(demonstrating load balancing and that the session is valid cluster-wide).

### Test 10: Password rotation via Vault/VSO takes effect without restarting pods

```sh
# Rotate the password (see deploy/vault/PROVISIONING.md section 1d):
# 1. gossipgrid credential rotate --file <old> --out <new>
# 2. vault kv put apps/gossipgrid/admin admin="$(cat <new>)"   (the apps mount)
#
# Record the time of the Vault write (T0).
#
# Watch the Kubernetes Secret update (VSO atomically updates it).
kubectl -n gossipgrid get secret gossipgrid-auth -o wide
# (The resourceVersion changes when VSO updates it.)
#
# Watch the pod logs for the credential reload (the poll-based reloader
# detects the file change, validates it, and publishes a new immutable
# snapshot — no restart).
kubectl -n gossipgrid logs -f gossipgrid-0 | grep -i "credential\|reload\|generation"
#
# Verify the pods do NOT restart (the restart count stays the same).
kubectl -n gossipgrid get pods -l app.kubernetes.io/name=gossipgrid -o wide
# (The RESTARTS column should not increase during rotation.)
```

**PASS criteria:** after the Vault write, the `gossipgrid-auth` Secret's
resourceVersion changes (VSO updated it); the pod logs show a credential reload
(new generation loaded); the pod restart count does NOT increase (no restart).
The new password works (login with the new password returns 200).

### Test 11: Old credentials and sessions are rejected after each replica adopts the new generation

```sh
# After rotation (Test 10), the old-generation sessions should be rejected
# IMMEDIATELY once a replica loads the new generation (no overlap window —
# generation binding).
#
# 1. Get a session cookie with the OLD password (before rotation) — if you
#    still have it from Test 8/9.
# 2. After rotation, use the OLD session cookie on each replica.
for i in 0 1 2 3 4; do
  kubectl -n gossipgrid port-forward pod/gossipgrid-$i 18080:8080 &
  PF_PID=$!
  sleep 1
  echo "=== gossipgrid-$i (old session) ==="
  curl -s -b /tmp/old-cookies.txt http://localhost:18080/api/messages -o /dev/null -w "HTTP %{http_code}\n"
  kill $PF_PID
done
#
# 3. Get a new session cookie with the NEW password and verify it works on
#    all replicas.
```

**PASS criteria:** the old-generation session cookie returns 401 (rejected) on
each replica **once that replica has loaded the new generation** (eventual —
not instantaneous; the propagation window is measured in Test 12). The
new-generation session cookie returns 200 on all replicas. There is **no
overlap window**: a replica that has loaded the new generation rejects
old-generation sessions immediately (it does not accept both old and new
simultaneously).

### Test 12: Pod restart counts do not increase during rotation

```sh
# Record the restart counts before and after rotation (Test 10).
kubectl -n gossipgrid get pods -l app.kubernetes.io/name=gossipgrid -o wide
# (Compare the RESTARTS column before and after the rotation.)
```

**PASS criteria:** the restart count for each pod is the same before and after
rotation (no pod restarted during the rotation). The credential reload is
in-process (poll-based reloader), not a pod restart.

### Test 13: Health and readiness probes remain functional

```sh
# Check the probe status on each pod (the startup, liveness, and readiness
# probes should all be passing).
kubectl -n gossipgrid describe pod gossipgrid-0 | grep -A10 "Readiness gates\|Conditions"
#
# Check the /healthz and /readyz endpoints on each pod (via port-forward).
for i in 0 1 2 3 4; do
  kubectl -n gossipgrid port-forward pod/gossipgrid-$i 18080:8080 &
  PF_PID=$!
  sleep 1
  echo "=== gossipgrid-$i ==="
  curl -s -o /dev/null -w "healthz: %{http_code}\n" http://localhost:18080/healthz
  curl -s -o /dev/null -w "readyz:  %{http_code}\n" http://localhost:18080/readyz
  kill $PF_PID
done
```

**PASS criteria:** `/healthz` returns 200 on all 5 pods (the HTTP event loop is
alive); `/readyz` returns 200 on all 5 pods (in 7A with lifecycle disabled,
/readyz returns 200 once the listeners are up). The pod Conditions show
`Ready: True`. The pods are not being restarted by the liveness probe (a peer
outage does NOT trigger a restart storm — the liveness probe only checks
/healthz).

### Test 14: Internal peer endpoints are not publicly exposed

```sh
# The peer mTLS endpoints (/internal/status, /internal/messages,
# /internal/sync/*) are registered ONLY on the peer listener (port 8443, TLS
# 1.3, mTLS) and are NOT registered on the public router (port 8080). They
# are not exposed through the public Gateway.
#
# 1. Try to reach /internal/status on the public FQDN (port 443, via the
#    Gateway). It should NOT be reachable (the public router does not
#    register /internal/*).
curl -k -s -o /dev/null -w "%{http_code}\n" https://<publicFQDN>/internal/status
# Expected: 404 (not found on the public router) — NOT 200.
#
# 2. Try to reach the peer port (8443) from outside the cluster (via the
#    public FQDN). The peer port is NOT exposed through the Gateway (only
#    port 443 -> 8080 is routed). The peer Service is a headless ClusterIP
#    (no external access).
curl -k -s -o /dev/null -w "%{http_code}\n" https://<publicFQDN>:8443/internal/status
# Expected: connection refused (the peer port is not exposed externally).
#
# 3. Verify the NetworkPolicy restricts the peer port to GossipGrid pods only
#    (a non-GossipGrid pod cannot reach port 8443).
kubectl -n gossipgrid get networkpolicy gossipgrid-peer-and-web -o yaml
```

**PASS criteria:** `/internal/status` is NOT reachable on the public FQDN
(404 on port 443); the peer port (8443) is NOT exposed externally (connection
refused); the NetworkPolicy restricts ingress to port 8443 to GossipGrid pods
only. The peer mTLS identity check is the primary security boundary; the
NetworkPolicy is a second layer.

## 3. Measuring the password-rotation propagation window

During Test 10–12, **measure the actual propagation window**:

1. Record `T0` = the time of the Vault write (`vault kv put`).
2. Record `T_last` = the time when the **last** replica (of the 5) loads the
   new generation (check the pod logs for the last "credential reload" /
   "generation loaded" message).
3. **Propagation window** = `T_last - T0`.

The propagation is **eventual, not instantaneous** (each replica loads on its
own poll cycle, default 2s). There is **no overlap window** (a replica that has
loaded the new generation rejects old-generation sessions immediately). The
`MAX_STALE` window (5m) bounds the "stuck in a bad state" → 503 signal, **not**
a session-overlap window.

**Record the measured window** in the staging report. Do not assume it (the
actual value depends on VSO's sync delay, the kubelet's file-swap timing, and
each replica's poll cycle).

## 4. Distinguishing best-effort anti-entropy from authoritative snapshot guarantees

In Milestone 7A (lifecycle disabled), a pod replacement (e.g. `kubectl delete
pod gossipgrid-2` — the pod is recreated with a fresh emptyDir, losing its
local SQLite) is **re-reconciled by anti-entropy** (best-effort): the new pod
discovers its peers via DNS, and the anti-entropy engine (default 30s cycle)
fetches missing messages from peers that still have them.

This is **NOT** an authoritative initial snapshot synchronization, safe
operator-controlled one-time genesis, or lifecycle readiness gating guarantee.
Those capabilities belong to Milestones 5B (Kubernetes metadata provider,
initial snapshot transfer, snapshot-source eligibility) and 7B (lifecycle-aware
staging).

**Do not claim** that a replaced pod "safely re-syncs from an authoritative
snapshot" in 7A. The actual behavior is: the new pod starts with an empty
SQLite, discovers its peers, and anti-entropy re-fetches the messages that
peers still have (eventual convergence; a small window of missing data is
possible). This is a **best-effort test**, not a **guarantee**.

To test pod replacement (optional, for the staging report):

```sh
# Delete one pod (it is recreated with a fresh emptyDir; the local SQLite is
# lost).
kubectl -n gossipgrid delete pod gossipgrid-3
# Wait for the new pod to start and become Ready.
kubectl -n gossipgrid get pods -l app.kubernetes.io/name=gossipgrid -w
# Wait for an anti-entropy cycle (default 30s) and verify the messages are
# re-fetched.
sleep 60
# Re-run the per-pod GET /api/messages and confirm gossipgrid-3 has the
# messages (re-fetched from peers).
```

## 5. Blockers requiring operator action

Record any of the following as **blockers** (do not invent a workaround or
claim the test passed):

| Blocker | Evidence | Impact |
| --- | --- | --- |
| cert-manager CSI driver missing | `kubectl get csidriver csi.cert-manager.io` returns "No resources found" | Per-pod peer mTLS mount fails; pods fail at startup (fail-closed). |
| VSO not installed / Secret not created | `kubectl -n gossipgrid get secret gossipgrid-auth` returns "NotFound" | Authenticated requests return 503 (fail-closed) until the Secret is provisioned. |
| Cilium GatewayClass missing | `kubectl get gatewayclass` does not list `cilium` | The Gateway cannot be created; browser HTTPS is unavailable. |
| LoadBalancer IP not allocated | `kubectl -n gossipgrid get gateway gossipgrid-gateway -o wide` shows no EXTERNAL-IP | The public FQDN is not reachable from the LAN (MetalLB / L2 not configured). |
| Vault PKI secrets engine not enabled | The `Certificate` resources stay in `Pending` (check `kubectl -n gossipgrid get certificate`) | The Issuers cannot issue certificates; peer mTLS and browser TLS are unavailable. |
| Cluster DNS domain mismatch | The CoreDNS `clusters` key does not match `clusterDomain` in the Helm values | Peer discovery and the identity suffix are wrong; peer mTLS identity checks fail. |
| Workload CA trust bundle Secret missing | `kubectl -n gossipgrid get secret gossipgrid-workload-ca-bundle` returns "NotFound" | Pods fail at startup (fail-closed) — the trust bundle is required for peer mTLS. |

For each blocker, record the **exact** `kubectl` output (not a paraphrase) and
the **specific** gap (e.g. "the cert-manager CSI driver is not registered;
`kubectl get csidriver csi.cert-manager.io` returns No resources found").
The cluster operations track owns those resources; the GossipGrid team should
not install or configure them.

## 6. Cleanup

After the staging tests are complete (if requested):

```sh
# Delete the Argo CD Application (Argo CD prunes the resources).
kubectl delete -f deploy/argocd/application.yaml
# or:
argocd app delete gossipgrid -n argocd

# If you used `helm install` directly, delete the release.
helm uninstall gossipgrid -n gossipgrid

# Delete the namespace (optional).
kubectl delete namespace gossipgrid
```

**Do not delete the Vault secrets or the cert-manager Issuers** — those are
cluster-wide prerequisites owned by the cluster operations track.
