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
milestone). The hot-reload is poll-based (not filesystem-event based); a brief window between a leaf-certificate
update and its key update is handled by retrying the reload rather than failing. No application-level FIPS 140-3
certification or production-readiness is claimed.

Peer gossip (Milestone 4B), anti-entropy synchronization (Milestone 4C), and readiness gating (Milestone 5A) are now
implemented and opt-in. When enabled, `/internal/status` reports the replica's lifecycle state, epoch, binding, and
readiness reason in addition to identity and version.

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
- **Private endpoint isolation.** `GET /internal/status` and (when enabled) `POST /internal/messages` are served only
  on the peer listener (TLS 1.3, mTLS) and require a successful client-certificate handshake; they are not registered
  on the public HTTP router (verified by negative tests). The production `ListenAndServe` entry point serves HTTPS
  only — plaintext HTTP is rejected.
- **FIPS.** The mTLS handshake (ECDSA P-256, SHA-256, TLS 1.3) is exercised by tests run under the FIPS-enabled build
  (`GOFIPS140=v1.0.0`, the default for `make test`/`make test-race`).
- **Negative tests (27 scenarios):** the 20 scenarios above, plus: production listener serves HTTPS (not plaintext);
  manager lifecycle (failed Start, repeated Start, concurrent Shutdown); key/cert mismatch; local cert not signed by
  configured CA; certificate with IP SAN; out-of-range ordinal; NodeID mismatch.

## Authenticated peer message exchange (Milestone 4A)

When `GOSSIPGRID_PEER_MESSAGE_EXCHANGE_ENABLED=true` (and peer mTLS is enabled), the private peer listener registers
`POST /internal/messages` for authenticated peer message exchange. Security properties, verified by 25 unit/negative
tests plus a two-node integration test (all run under the FIPS build and the race detector):

- **mTLS-only.** The endpoint is reachable only through the authenticated peer listener (TLS 1.3, mutual TLS,
  dedicated workload CA trust, strict identity authorization). It is not registered on the public HTTP router.
- **Identity authorization.** The sender must present a client certificate that passes the full GossipGrid identity
  policy (exactly one DNS SAN, correct suffix, StatefulSet name prefix, ordinal in range, clientAuth EKU).
- **No header-based identity.** The receiver does not accept a supplied HTTP header as proof of peer identity; the
  authenticated TLS client identity is the sole basis for authorization.
- **Bounded input.** The request body is bounded to 8192 bytes; the message body is bounded to 4096 bytes (the
  store's limit). The JSON decoder enforces exactly one document, no unknown fields, and canonical UUIDv7.
- **Idempotent, immutable insert.** The receiver uses the existing atomic `ON CONFLICT DO NOTHING` insert. A new
  message is stored; an identical duplicate is a no-op; a conflicting duplicate returns 409 without overwriting the
  original row. The original ID, body, origin, and timestamp are never regenerated or overwritten.
- **Opt-in and mTLS-gated.** Default is `false`. When disabled, the endpoint is not registered. Enabling it requires
  `GOSSIPGRID_PEER_TLS_ENABLED=true`; configuration validation fails otherwise. This prevents accidental
  cross-epoch replication before the lifecycle milestone (5A) established the epoch-fencing protocol.
- **No epoch claim at 4A time.** At Milestone 4A, cluster genesis, epoch management, and recovery lockout were not
  established; it is a provisional direct-exchange capability, not a complete safe cluster bootstrap protocol.
  Milestone 5A (2026-11-21) subsequently implemented epoch fencing and the recovery lockout (opt-in via
  `GOSSIPGRID_LIFECYCLE_ENABLED`): the message exchange endpoint checks the peer's epoch context against the local
  binding and rejects mismatches with 409. Automatic genesis and operator recovery/reset are still deferred to
  5B and later.
- **4A scope.** At the time of Milestone 4A, automatic gossip fanout, bounded queues, retry scheduling, and
  anti-entropy were not yet implemented. `POST /api/messages` did not automatically replicate in 4A.
  Milestone 4B (2026-11-20) subsequently implemented automatic epidemic gossip fanout (opt-in); when
  `GOSSIPGRID_GOSSIP_ENABLED=true`, successful local writes **do** initiate automatic propagation.

## Anti-entropy sync endpoints (Milestone 4C)

When `GOSSIPGRID_ANTI_ENTROPY_ENABLED=true` (and peer mTLS and peer message exchange are enabled), the private peer
listener additionally registers `GET /internal/sync/ids` and `POST /internal/sync/messages`. Security properties:

- **mTLS-only.** Both endpoints are reachable only through the authenticated peer listener (TLS 1.3, mutual TLS,
  dedicated workload CA trust, strict identity authorization). They are not registered on the public HTTP router.
- **No header-based identity.** The authenticated TLS client identity is the sole basis for authorization.
- **Bounded input (inventory).** The page limit is clamped to 1–1024 server-side. The cursor must be a valid
  canonical UUIDv7. Responses are bounded to 256 KiB (detected explicitly; truncated JSON is rejected).
- **Bounded input (fetch).** The request body is bounded to 64 KiB. The ID array is bounded to 8 entries per
  batch. 8 × 32,768 (the conservative per-record wire-size allowance) = 262,144 = 256 KiB is a sizing
  allowance that keeps the worst-case fully-populated response at or below the response limit; it is
  not a proof that the complete JSON envelope always fits 256 KiB — the server's actual
  encoded-response validation enforces the limit explicitly (a response that exceeds it, e.g. from
  legacy records with bodies beyond the current 4096-byte store limit, returns an explicit 500 error
  and is never written). Each ID must be a valid canonical UUIDv7. Duplicate IDs are rejected.
  Multiple JSON documents are rejected. Unknown fields are rejected.
- **No silent partial results.** If any requested ID is missing on the server, the response is 404 with the list
  of missing IDs. Partial results are never presented as complete.
- **Idempotent, immutable insert.** Fetched records are inserted via the existing atomic `ON CONFLICT DO NOTHING`
  path. Conflicting content never overwrites the local row.
- **Response validation (client-side).** The client validates: strict ascending ID order, no duplicates, correct
  cursor semantics, exact count match (no extra/missing records), valid timestamps. Malformed responses are
  rejected; no record is inserted from a protocol-defect response.
- **Opt-in and mTLS-gated.** Default is `false`. Enabling requires peer mTLS, peer message exchange, and static or
  DNS discovery. Gossip need not be enabled.
- **No epoch claim at 4C time.** At Milestone 4C, a previously authorized replica carrying old-epoch data could
  reintroduce it; 4C is periodic reconciliation within the provisional trust model, not a complete safe cluster
  bootstrap protocol. Milestone 5A (2026-11-21) subsequently implemented epoch fencing (opt-in): when lifecycle
  protection is enabled, the anti-entropy engine checks admission before network operations and classifies epoch
  mismatch (409) as a permanent error (no retry), preventing an old-epoch replica from reintroducing its data.

## Lifecycle and epoch fencing (Milestone 5A — implemented 2026-11-21)

When `GOSSIPGRID_LIFECYCLE_ENABLED=true` (and peer mTLS is enabled), the private peer listener and public HTTP
router enforce lifecycle-based admission and epoch fencing. Security properties, verified by unit tests and real-mTLS
integration tests (all run under the FIPS build and the race detector):

- **Opt-in and mTLS-gated.** Default is `false`. Enabling requires `GOSSIPGRID_PEER_TLS_ENABLED=true`; configuration
  validation fails otherwise. When disabled, all existing behavior is preserved (readiness always 200, no admission
  checks, no epoch headers).
- **mTLS remains the sole identity basis.** Epoch headers (`X-GossipGrid-Cluster-ID`, `X-GossipGrid-Epoch-ID`) are
  fencing metadata, not credentials. A peer with a valid mTLS certificate but a different epoch is rejected with
  409 — its data is not merged. The authenticated TLS client identity is still required for all private endpoints.
- **No header-based identity.** The receiver does not accept a supplied HTTP header as proof of peer identity. Epoch
  headers only determine whether data from an already-authenticated peer is eligible for merging.
- **Fail-closed admission.** When the replica is not in a ready state (`READY` or `DEGRADED`), public writes return
  503 (no message inserted) and private replication is rejected. When the replica has no valid metadata observation,
  writes are also rejected. No path exists to bypass admission when lifecycle is enabled.
- **Epoch mismatch is permanent.** A 409 response (epoch mismatch) is classified as a permanent error by the gossip
  and anti-entropy engines. It is not retried. This prevents a stuck old-epoch replica from endlessly retrying
  against a new-epoch cluster.
- **Bounded header input.** Epoch headers are bounded to 64 bytes each. Duplicate headers, oversized headers, or
  missing headers (when required) are rejected. This prevents DoS via oversized header injection.
- **Durable binding is exclusive.** The `local_metadata` binding is a singleton row. A database bound to one cluster
  epoch cannot be re-bound to a different one without an operator-performed recovery. A nonempty database without
  a binding is rejected. This prevents accidental cross-epoch mixing.
- **No data deletion on mismatch.** When an epoch mismatch is detected, the replica transitions to
  `RECOVERY_REQUIRED`. Local data is preserved; no messages are deleted. The binding row is intact. This is a
  lockout, not a destructive event.
- **Status endpoint always available.** `GET /internal/status` returns 200 even when the replica is unready, so
  operators can diagnose the state (lifecycle state, epoch, binding, readiness reason) without the replica being
  ready. Unready replicas do not supply authoritative synchronization snapshots.
- **Integration tests use real mTLS.** Five integration test scenarios (matching epochs, wrong-epoch peer, unready
  sync source, epoch change, startup without metadata) use real TLS 1.3 mutual TLS with generated certificates,
  not mock-only HTTP. Security-negative results (409 rejection, data preservation, no merge) are verified under
  the race detector.
- **No production metadata provider.** The Kubernetes ConfigMap-backed metadata provider is not yet implemented.
  The production path uses a no-op source that always reports metadata unavailable, so replicas with lifecycle
  enabled fail closed. No security guarantee is claimed for cluster recovery until the 5B provider is deployed.
- **No automatic genesis or reset.** 5A does not implement automatic cluster initialization, automatic epoch reset,
  or operator reset endpoints. A fresh replica with no binding cannot automatically become ready. A
  `RECOVERY_REQUIRED` replica cannot automatically return to `READY`. These are deferred to 5B and later.
- **Commit authorization (lock-order guarantee).** The anti-entropy cursor commit
  (`Controller.CommitUnderAuthorization`) evaluates the complete authorization decision — ready state,
  not stopped, fresh valid metadata with `Initialized=true`, and a matching durable binding with no
  recovery lockout — from a single locked snapshot (`authorizationSnapshotLocked`) that reads all
  authorization fields under one `c.mu` RLock and one `m.mu` RLock (no second lock acquisition, no
  SQLite read while the locks are held). The RLocks are held until the in-memory cursor mutation
  completes, so a revocation (Stop or a state/metadata change) that linearizes before the commit is
  observed and the callback is not executed; a revocation that linearizes after the callback completes
  is a normal, permitted revocation. All lifecycle state transitions (`SetRecoveryRequired`, `SetReady`,
  `SetDegraded`, `SetDiscovering`, `SetSyncing`, and the metadata-driven `reevaluateState`) acquire
  `c.mu` RLock before `m.mu` Lock, matching the commit's `c.mu` RLock → `m.mu` RLock order. This
  ensures that a readiness-revoking transition cannot linearize while a cursor commit holds the RLocks:
  the transition is queued behind the commit and is observed as a revocation by the next authorization
  evaluation. Lock order is `Controller.mu` (RLock) → `Machine.mu` (RLock) → `Engine.mu` (Lock); the
  engine's locks are never held while the controller's or machine's locks are held, so no inverse order
  exists. A cached binding that is absent or does not match the accepted metadata fails closed (no
  database read inside the protected commit).

## Go TLS handling (target design for the remainder)

- Peer client: verify server name, chain, approved identity; bound timeouts and connections (implemented; see above).
- Report cert lifetime and trust bundle fingerprint in diagnostics without disclosing private keys (implemented; see
  above).
- Gossip/synchronization epoch and state checks over mTLS are implemented (Milestone 5A, opt-in via
  `GOSSIPGRID_LIFECYCLE_ENABLED`): the peer status and sync endpoints enforce epoch fencing (409 permanent / 503
  unready), a wrong-epoch response is a security/consistency incident (never merged), and a lifecycle-enabled client
  additionally requires the response envelope to confirm the peer runs the epoch-fencing protocol.

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
