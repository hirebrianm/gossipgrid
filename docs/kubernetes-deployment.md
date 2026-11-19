# Kubernetes and delivery design

**Scope separation:** application source and Helm packaging are developed here; cluster-wide prerequisites (Vault
issuer, cert-manager, trust-manager, Cilium Gateway/LB configuration, DNS, GitLab Agent access) are handled in a
separate cluster operations track.

## Kubernetes objects planned

| Object | Function |
| --- | --- |
| StatefulSet | N replicas, stable names, `Parallel` startup, no PVC templates |
| `emptyDir` | Pod-lifetime writable SQLite directory |
| Headless Service | Direct peer addresses, named gossip port, publish not-ready pods |
| ClusterIP Service | User-facing HTTP routing to ready pods |
| Gateway / HTTPRoute | Browser HTTPS termination with Vault-issued cert |
| cert-manager Certificate | Managed browser TLS Secret for Gateway |
| cert-manager CSI volume | Unique short-lived mTLS certificate per pod |
| ConfigMap | Non-secret cluster metadata/epoch and incident history reference |
| ServiceAccount / Role / RoleBinding | Minimum metadata update permission only if app controls epochs |
| NetworkPolicy | Later restriction of peer traffic and ingress |

## Failure-safe probes

- **Liveness:** server/event loop is alive. Peer outage must not trigger restart storms.
- **Readiness:** local storage usable, required TLS material available, and initial synchronization complete for this
  epoch. DNS peer Service still exposes unready peers, but web Service does not.
- **Startup:** accommodates time for CSI issuance and the initial snapshot without premature liveness restarts.
- During a total-loss lockout no ready web endpoints may exist; diagnostics available via direct pod port-forward and
  logs/events.

## Browser load balancing

Target is **Cilium Gateway API** with a LAN-reachable LoadBalancer IP. Verify the local Cilium version, Gateway
support, LB IPAM, and L2 announcements or an alternative advertisement mode. Do not assume Cilium deployment
automatically enables these capabilities. The Gateway serves Vault-issued HTTPS; the upstream application listener is
initially HTTP on `8080`.

A browser refresh can land on another ready replica. UI explicitly displays which replica responded, helping illustrate
load balancing and replication lag. Session affinity is intentionally not required for v1.

## Peer communication

`gossipgrid-<ordinal>.gossipgrid-headless.<namespace>.svc.<cluster-domain>` identifies each StatefulSet pod.
Never assume `cluster.local` if the cluster DNS domain differs. Headless DNS serves discovery; actual source
validity is established by the private status API and mutual TLS, not by DNS results alone.

## Container and CI/CD

- Build using Go, CGO, and static musl/SQLite linkage in a builder image; final runtime `scratch`.
- Use a Makefile as the single command interface: `fmt`, `vet`, `test`, `test-race`, `build`, `image`, `qa`,
  `helm-lint`, `integration` as they become relevant.
- GitLab CI: QA -> integration -> image build/push -> Helm lint/template -> controlled Helm deploy (initially
  manual/protected).
- Tag with immutable commit SHA and record image digests; do not use `latest` as a deployment identity.
- Helm values: replica count, namespace, image, resources, ports, issuer references, CA bundles, sync timings, and
  intended reset policy. Keep secrets out of source.
- Implement manifests only after app behavior and local integration tests pass.

## Tests to demonstrate

1. Five pods gossip to convergence under normal operation.
2. A pod replacement loses local SQLite but repopulates from a synchronized peer.
3. A temporary partition causes missing/unreachable status, then heals via anti-entropy.
4. Reload pod mTLS credentials as cert-manager rotates them.
5. Gateway serves browser HTTPS with the trusted Vault CA certificate.
6. Scale 5 -> 7; newly added pods synchronize before becoming ready.
7. All pods lose data; reset fails closed until authorized; incident remains visible afterward.
8. GitLab deployment performs a controlled rollout while maintaining observable health.

## Cluster references

- [Kubernetes StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
- [Kubernetes headless Services and DNS](https://kubernetes.io/docs/concepts/services-networking/service/)
- [Kubernetes `emptyDir` volumes](https://kubernetes.io/docs/reference/storage/volumes/#emptydir)
- [cert-manager CSI driver](https://cert-manager.io/docs/usage/csi-driver/)
- [cert-manager Vault Issuer](https://cert-manager.io/docs/configuration/vault/)
- [Gateway API TLS guide](https://gateway-api.sigs.k8s.io/guides/user-guides/tls/)
- [Cilium L2 announcement documentation](https://docs.cilium.io/en/stable/network/l2-announcements/)
