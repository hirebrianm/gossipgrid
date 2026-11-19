# Security architecture

This is the **target design**, not an assertion of deployed security or formal certification.

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
Certificates and private keys are mounted for each pod, renewed automatically, and hot-reloaded by Go. A workload CA
bundle is distributed/rotated via trust-manager or a narrowly managed mount, allowing overlap during CA transitions.
Define DNS/URI SAN formats and EKU constraints during implementation, including exact service namespace and expected
pod name.

Vault itself and issuer resources are cluster prerequisites and must not be embedded in the application or its Docker
image. Select ECDSA P-256 / SHA-256 initially, subject to compatibility verification with the actual Go FIPS build and
Vault roles.

## Go TLS handling

- Peer listener `:8443`: TLS 1.3, verify-and-require client certificates, validate named peer identity and CA trust.
- Peer client: verify server name, chain, approved identity; bound timeouts and connections.
- Reload certificate/key and trust material on change atomically. Existing connections may persist until reconnected;
  tests explicitly verify that new connections pick up rotated credentials.
- Report cert lifetime, trust bundle version/fingerprint, and last successful reload in diagnostics without disclosing
  private keys.
- Negative tests: unknown CA, unexpected identity, expired certificate, malformed reload, invalid EKU, wrong epoch, and
  TLS failures.

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
