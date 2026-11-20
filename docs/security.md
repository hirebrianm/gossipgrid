# Security architecture

This document describes the peer mTLS security architecture (implemented in Milestone 3B) and the broader target
design for the rest of the system. It is not an assertion of deployed security or formal certification.

## Threat model and trust boundaries

- Browser-to-Gateway HTTP requests are terminated using a server certificate chained to a CA trusted by the user's
  desktops.
- Internal peer-to-peer gossip/synchronization uses independent HTTPS **mutual TLS** with a dedicated workload CA trust
  hierarchy.
- Internal peer endpoints are not exposed through the public Gateway.
- Both certificate chain trust **and intended peer workload identity** are verified. Merely having a certificate issued
  by the workload CA is not sufficient.
- No `InsecureSkipVerify`, disabling hostname verification, or wildcard authorization shortcuts.
- Admin-only metadata reset uses tightly scoped Kubernetes RBAC, no write access to arbitrary cluster resources.
- User-submitted message bodies are untrusted and encoded in HTML; JSON/API payload sizes bounded; logging avoids
  secrets.

## Two PKI trust domains

**Browser HTTPS:** Vault PKI role/issuer signs the certificate for a deliberate internal DNS hostname; cert-manager
creates/rotates the TLS Secret referenced by Gateway API. Browsers trust the appropriate CA chain already installed in
their trusted store. Browser HTTPS ends at Gateway; initial Gateway-to-app traffic is internal HTTP. Backend TLS is a
separate exercise.

**Workload mTLS:** A separate Vault PKI issuer/CA signs per-pod certificates requested by cert-manager CSI.
Certificates and private keys are mounted for each pod, renewed automatically, and hot-reloaded by Go (implemented in
Milestone 3B — see the section below). A workload CA bundle is distributed/rotated via trust-manager or a narrowly
managed mount, allowing overlap during CA transitions.

**Certificate identity and file layout (Milestone 3B).** Each pod's certificate DNS identity is
`<pod>.<GOSSIPGRID_PEER_IDENTITY_SUFFIX>` (e.g. `gossipgrid-0.gossipgrid-headless.default.svc.cluster.local`). The
suffix is operator-configured (the cluster DNS domain is not hard-coded). The same certificate serves both the server
and client roles, so it carries both `serverAuth` and `clientAuth` Extended Key Usages. The expected file layout per
pod is:

| File | Env var | Content |
| --- | --- | --- |
| `/certs/tls.crt` | `GOSSIPGRID_PEER_CERT_FILE` | Leaf certificate followed by its issuer chain (PEM) |
| `/certs/tls.key` | `GOSSIPGRID_PEER_KEY_FILE` | Private key (PKCS8 PEM; EC, RSA PEM also accepted) |
| `/certs/ca-bundle.pem` | `GOSSIPGRID_PEER_TRUST_BUNDLE_FILE` | One or more workload CA certificates (PEM) |

**Security limitations.** The peer listener is not yet exposed through the Kubernetes Gateway (that is a later
milestone); peer gossip, synchronization, and readiness gating are not implemented yet, so `/internal/status` reports
identity and version only, not synchronization state. The hot-reload is poll-based (not filesystem-event based); a
brief window between a leaf-certificate update and its key update is handled by retrying the reload rather than
failing. No application-level FIPS 140-3 certification or production-readiness is claimed.

Vault itself and issuer resources are cluster prerequisites and must not be embedded in the application or its Docker
image. Select ECDSA P-256 / SHA-256 initially, subject to compatibility verification with the actual Go FIPS build and
Vault roles.

## Implemented peer mTLS (Milestone 3B)

The `internal/peertls` package implements the workload mTLS transport. Key properties, verified by 20 negative/positive
unit tests plus a two-node integration test (all run under the FIPS build and the race detector):

- **TLS 1.3 only.** Both the peer listener and the peer client set `MinVersion`/`MaxVersion` to `VersionTLS13`. A
  TLS 1.2 client is rejected at the handshake.
- **Dedicated workload CA trust.** The server uses `ClientCAs` (the workload trust bundle) to verify client
  certificates; the client uses `RootCAs` (the same bundle) to verify the server. The system root store is never used,
  so a certificate merely chaining to a public CA is not trusted.
- **Mutual TLS + identity authorization.** `ClientAuth: RequireAndVerifyClientCert` requires a client certificate; the
  additional `VerifyPeerCertificate` hooks enforce the GossipGrid identity policy (below) beyond the standard chain and
  hostname checks.
- **Identity authorization rules.** A peer certificate is authorized only when: it has **exactly one DNS SAN**; the
  SAN is `<statefulset-name>-<ordinal>.<identity-suffix>` with an exact match on the configured
  `GOSSIPGRID_PEER_IDENTITY_SUFFIX` and the configured StatefulSet name; the ordinal is a non-negative integer; and the
  required EKU is present (`clientAuth` for a client, `serverAuth` for a server). Wildcard SANs, IP/URI SANs,
  multiple-SAN certificates, a different StatefulSet name, and the Common Name are all rejected. This means a
  certificate from a different StatefulSet — even one signed by the same trusted workload CA — is not authorized.
- **Dial address vs expected identity.** The peer client takes the network dial address and the expected GossipGrid
  peer identity as distinct inputs, so a client can dial `127.0.0.1:8443` while verifying a certificate for an
  in-cluster FQDN. This underpins local multi-process testing and is what prevents identity checks from being bypassed
  by dialing an IP or by supplying an untrusted identity in an HTTP header.
- **Certificate hot-reload.** Credential files are polled at `GOSSIPGRID_PEER_TLS_RELOAD_INTERVAL` (default 2s). A
  successful reload publishes an immutable credential snapshot atomically. The server uses `GetConfigForClient` to
  return a fresh per-connection TLS config from the latest snapshot on each new handshake, so **new** handshakes use
  the latest credentials without a restart and without any in-place mutation of a captured config. A failed reload
  preserves the previous valid snapshot — partially loaded or malformed material never replaces valid credentials. The
  trust bundle may hold multiple CAs during a CA transition. Existing connections are not torn down during normal
  leaf-certificate rotation.
- **Diagnostics without secrets.** `Diagnostics` reports the leaf certificate SHA-256 fingerprint, pod ID, load time,
  and reload count — never the private key or the PEM content.
- **Complete credential validation at load time.** Before a credential snapshot is published, the loader verifies:
  (a) the private key matches the leaf certificate's public key; (b) the local certificate chain verifies against the
  configured workload CA trust bundle; (c) the certificate is within its validity period; (d) the identity is
  authorized (exactly one DNS SAN, correct suffix, StatefulSet name prefix, ordinal in range [0, ExpectedReplicas-1]);
  (e) both `serverAuth` and `clientAuth` EKUs are present; (f) the local certificate's pod identity matches the
  configured `GOSSIPGRID_NODE_ID`. A mismatched key/cert pair or a local cert signed by an untrusted CA fails startup.
- **Private endpoint isolation.** `GET /internal/status` is served only on the peer listener (TLS 1.3, mTLS) and
  requires a successful client-certificate handshake; it is not registered on the public HTTP router (verified by a
  negative test). The production `ListenAndServe` entry point serves HTTPS only — plaintext HTTP is rejected.
- **FIPS.** The mTLS handshake (ECDSA P-256, SHA-256, TLS 1.3) is exercised by tests run under the FIPS-enabled build
  (`GOFIPS140=v1.0.0`, the default for `make test`/`make test-race`).
- **Negative tests (27 scenarios):** the 20 scenarios above, plus: production listener serves HTTPS (not plaintext);
  manager lifecycle (failed Start, repeated Start, concurrent Shutdown); key/cert mismatch; local cert not signed by
  configured CA; certificate with IP SAN; out-of-range ordinal; NodeID mismatch.

## Go TLS handling (target design for the remainder)

- Peer client: verify server name, chain, approved identity; bound timeouts and connections (implemented; see above).
- Report cert lifetime and trust bundle fingerprint in diagnostics without disclosing private keys (implemented; see
  above).
- Gossip/synchronization epoch and state checks over mTLS are a later milestone (Milestones 4/5); a wrong-epoch
  response is a security/consistency incident, not a merge.

## FIPS 140-3 approach

- Keep a normal build and a **FIPS build profile**, both statically linked for `scratch`.
- Select a pinned **native Go FIPS 140-3 cryptographic module** with `GOFIPS140` and pin an appropriate Go toolchain;
  verify availability and the module's published security policy before choosing the exact version.
- Add automated tests/build evidence for module version, runtime FIPS mode, TLS behavior, binary static linkage, and
  container digest. Expose *runtime status* in the UI, not an unconditional badge.
- Distinguish a FIPS-enabled Go binary from a validated **whole application/deployment**. Vault, Gateway proxy, Cilium,
  cert-manager, underlying operating environments, and other cryptographic code have independent assurance
  requirements.
- Host Rocky Linux FIPS experiments are optional and do not replace application-level module verification.

## `scratch` image

A multi-stage Alpine/musl build can statically link the standard GORM SQLite CGO driver with static SQLite. Verify the
resulting executable with ELF tools during CI. Embed templates/assets via `go:embed`, mount a writable `/data`
ephemeral volume, supply certs through Kubernetes volumes, and set non-root UID/GID and volume permissions. Do not
include signing keys, a shell, or package managers in the final image.

Reference starting point supplied by project owner: [go-sdk-demo Dockerfile](https://github.com/hirebrianm/go-sdk-demo/blob/main/Dockerfile).
Adapt it after review; it is not copied into the initial documentation-only commit.

## Future security exercises

Cilium NetworkPolicy to restrict `8443` only to GossipGrid peers; Gateway exposure limits; TLS rotation under load;
failed trust rotation; dynamic revocation behavior; RBAC negative tests; audit logs; container vulnerability/SBOM
scan; non-root and read-only root filesystem; cryptographic module verification.
