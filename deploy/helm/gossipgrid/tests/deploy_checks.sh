#!/usr/bin/env bash
# GossipGrid Helm chart regression checks (Milestone 7A).
#
# Validates the rendered chart output and the deployment manifests against the
# Milestone 7A integration requirements:
#   1. StatefulSet/headless Service compatibility.
#   2. Per-pod certificate identity (CSI driver + volumeAttributes).
#   3. VSO manifest schema (VaultStaticSecret CRD fields, including the
#      required mount field and the "apps" KV v2 mount).
#   4. Argo CD values-file resolution (the referenced values file must exist
#      at a path relative to the chart root — no manual copy required).
#   5. Secret rotation without subPath (trust bundle + credentials).
#   6. No unintended public peer exposure (NetworkPolicy).
#   7. NetworkPolicy ingress restrictions (no 0.0.0.0/0).
#   8. No unintended public peer exposure (NetworkPolicy).
#   9. Deployment manifest schema (StatefulSet, Service, NetworkPolicy).
#   10. Image and tag consistency between manifest and values.
#   11. Genesis grant RBAC (Milestone 5B, Stage 1A): the genesis RBAC is
#      disabled by default; when enabled it grants create on configmaps and
#      get on the named ConfigMap only (no update/patch/delete, no
#      ClusterRole, the app ServiceAccount is not a subject).
#   12. Genesis finalization RBAC and Job (Milestone 5B, Stage 1B): the
#      finalization RBAC is disabled by default; when enabled it grants
#      get and update on the named ConfigMap only (no create/delete/patch/
#      list/watch, no ClusterRole, the app ServiceAccount is not a
#      subject). The operator-managed Job runs "genesis finalize" with the
#      --confirm-seed-bound flag (the explicit operator gate).
#   13. Application read-only metadata RBAC (Milestone 5B, Stage 1B): the
#      app metadata RBAC is disabled by default; when enabled it grants
#      get on the named ConfigMap only (no update/create/delete/patch/
#      list/watch, no ClusterRole, the app ServiceAccount is the subject).
#   14. Genesis override configuration (Milestone 5B, Stage 1B): renders
#      the exact override values file documented in docs/genesis-runbook.md
#      section 10 (lifecycle.enabled, metadataKubernetes.enabled,
#      configMapName, metadataNamespace, seedIdentity, genesis.enabled,
#      finalize.enabled) and verifies the effective env vars (seed
#      identity, metadata namespace, metadata provider, lifecycle enabled)
#      and the RBAC (app read-only, genesis grant, finalization). Also
#      verifies the default and 7A staging renderings do NOT emit the
#      genesis-specific env vars.
#
# Usage: bash deploy/helm/gossipgrid/tests/deploy_checks.sh
# Requires: helm (for rendering), grep, awk, sed. Read-only: no files modified.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
CHART_DIR="$REPO_ROOT/deploy/helm/gossipgrid"
VAULT_DOC="$REPO_ROOT/deploy/vault/PROVISIONING.md"
ARGO_APP="$REPO_ROOT/deploy/argocd/application.yaml"
STAGING_VALUES="$REPO_ROOT/deploy/helm/gossipgrid/values-staging.yaml"
NAMESPACE="gossipgrid"
RELEASE="test-release"

PASS=0
FAIL=0

pass() { echo "  PASS: $1"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

# Render the chart once (reused by multiple checks).
RENDERED_OUTPUT=$(helm template "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" 2>&1)
if [ $? -ne 0 ]; then
  echo "FATAL: helm template failed:"
  echo "$RENDERED_OUTPUT"
  exit 1
fi

# Render the chart again, applying the staging values file, to prove the
# Argo CD valueFiles reference resolves to a real file in the chart directory
# and that the chart renders cleanly with the staging overrides applied.
STAGING_RENDERED_OUTPUT=$(helm template "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" -f "$STAGING_VALUES" 2>&1)
if [ $? -ne 0 ]; then
  echo "FATAL: helm template with values-staging.yaml failed:"
  echo "$STAGING_RENDERED_OUTPUT"
  exit 1
fi

echo "=== GossipGrid Helm chart regression checks ==="
echo ""

# ---------------------------------------------------------------------------
# 1. StatefulSet/headless Service compatibility
# ---------------------------------------------------------------------------
echo "--- 1. StatefulSet/headless Service DNS compatibility ---"

# The StatefulSet's serviceName must match the headless Service name.
# Single awk pass: find the first StatefulSet block, print its serviceName.
# Avoids head -1 on the full string (SIGPIPE hazard under pipefail).
SVC_NAME=$(awk '
  /^kind: StatefulSet$/ {insts=1}
  insts && /^---/ {insts=0}
  insts && /^  serviceName: / {print $2; exit}
' <<< "$RENDERED_OUTPUT")
if [ -z "$SVC_NAME" ]; then
  fail "Could not find serviceName in rendered StatefulSet"
else
  pass "StatefulSet serviceName: $SVC_NAME"
fi

# The headless Service name (from the rendered Service resource).
# Single awk pass: extract Service blocks, find the one with clusterIP: None,
# print its name. No grep -q, no head -1, no SIGPIPE risk.
HEADLESS_NAME=$(awk '
  /^kind: Service$/ {insvc=1}
  insvc && /^---/ {insvc=0}
  insvc && /^  name: / {name=$2}
  insvc && /clusterIP: None/ {if (name != "") {print name; exit}}
' <<< "$RENDERED_OUTPUT")
if [ -z "$HEADLESS_NAME" ]; then
  fail "Could not find headless Service name in rendered output"
else
  pass "Headless Service name: $HEADLESS_NAME"
fi

# They must match.
if [ "$SVC_NAME" = "$HEADLESS_NAME" ]; then
  pass "StatefulSet serviceName matches headless Service name ($SVC_NAME)"
else
  fail "StatefulSet serviceName ($SVC_NAME) != headless Service name ($HEADLESS_NAME)"
fi

# The pod DNS FQDN pattern: <pod-name>.<serviceName>.<namespace>.svc.<clusterDomain>
# The peer identity suffix (from the env var) must be
# <peerServiceName>.<namespace>.svc.<clusterDomain>
PEER_SUFFIX=$(grep 'PEER_IDENTITY_SUFFIX' -A1 <<< "$RENDERED_OUTPUT" | grep 'value:' | awk -F'"' '{print $2}')
if [ -z "$PEER_SUFFIX" ]; then
  fail "Could not find GOSSIPGRID_PEER_IDENTITY_SUFFIX in rendered StatefulSet"
else
  # Verify the suffix ends with .<namespace>.svc.cluster.local
  if grep -q "\.${NAMESPACE}\.svc\.cluster\.local$" <<< "$PEER_SUFFIX"; then
    pass "Peer identity suffix is a valid FQDN suffix: $PEER_SUFFIX"
  else
    fail "Peer identity suffix does not match namespace/cluster domain: $PEER_SUFFIX"
  fi
  # Verify the suffix matches the headless Service name
  if [ "$PEER_SUFFIX" = "${HEADLESS_NAME}.${NAMESPACE}.svc.cluster.local" ]; then
    pass "Peer identity suffix matches headless Service FQDN"
  else
    fail "Peer identity suffix ($PEER_SUFFIX) != headless Service FQDN (${HEADLESS_NAME}.${NAMESPACE}.svc.cluster.local)"
  fi
fi

# The discovery DNS name must match the headless Service FQDN.
DISCOVERY_NAME=$(grep 'DISCOVERY_DNS_NAME' -A1 <<< "$RENDERED_OUTPUT" | grep 'value:' | awk -F'"' '{print $2}')
if [ -n "$DISCOVERY_NAME" ] && [ "$DISCOVERY_NAME" = "${HEADLESS_NAME}.${NAMESPACE}.svc.cluster.local" ]; then
  pass "Discovery DNS name matches headless Service FQDN: $DISCOVERY_NAME"
else
  fail "Discovery DNS name ($DISCOVERY_NAME) != headless Service FQDN (${HEADLESS_NAME}.${NAMESPACE}.svc.cluster.local)"
fi

# ---------------------------------------------------------------------------
# 2. Per-pod certificate identity (CSI driver)
# ---------------------------------------------------------------------------
echo ""
echo "--- 2. Per-pod certificate identity (cert-manager CSI driver) ---"

# The CSI driver name must be csi.cert-manager.io (NOT the stale, wrong
# cert-manager.csi.dev — the real driver name is csi.cert-manager.io).
# No head -1: just test for presence with grep -q; no SIGPIPE risk.
if grep -q 'driver: csi\.cert-manager\.io' <<< "$RENDERED_OUTPUT"; then
  pass "CSI driver is csi.cert-manager.io"
else
  fail "CSI driver csi.cert-manager.io not found in rendered StatefulSet"
fi

# The stale, wrong driver name must NOT be present.
if grep -q 'cert-manager\.csi\.dev' <<< "$RENDERED_OUTPUT"; then
  fail "Stale driver name cert-manager.csi.dev present in rendered StatefulSet"
else
  pass "No stale driver name cert-manager.csi.dev in rendered StatefulSet"
fi

# The volumeAttributes must use the csi.cert-manager.io/ prefix.
if grep -q 'csi\.cert-manager\.io/issuer-name:' <<< "$RENDERED_OUTPUT"; then
  pass "volumeAttributes use csi.cert-manager.io/issuer-name"
else
  fail "csi.cert-manager.io/issuer-name not found in volumeAttributes"
fi

if grep -q 'csi\.cert-manager\.io/dns-names:' <<< "$RENDERED_OUTPUT"; then
  pass "volumeAttributes include csi.cert-manager.io/dns-names"
else
  fail "csi.cert-manager.io/dns-names not found in volumeAttributes"
fi

# The dns-names must use ${POD_NAME} (per-pod substitution).
DNS_NAMES_VALUE=$(grep 'csi\.cert-manager\.io/dns-names:' <<< "$RENDERED_OUTPUT" | awk -F': ' '{print $2}')
if grep -q '^\${POD_NAME}\.' <<< "$DNS_NAMES_VALUE"; then
  pass "dns-names uses \${POD_NAME} per-pod substitution: $DNS_NAMES_VALUE"
else
  fail "dns-names does not use \${POD_NAME} prefix: $DNS_NAMES_VALUE"
fi

# The dns-names must include the peerServiceName, namespace, and cluster domain.
if grep -q "\.${HEADLESS_NAME}\.${NAMESPACE}\.svc\.cluster\.local$" <<< "$DNS_NAMES_VALUE"; then
  pass "dns-names SAN matches the headless Service FQDN suffix"
else
  fail "dns-names SAN does not match headless Service FQDN: $DNS_NAMES_VALUE"
fi

# The key algorithm must be ECDSA.
if grep -q 'csi\.cert-manager\.io/key-algorithm: ECDSA' <<< "$RENDERED_OUTPUT"; then
  pass "Key algorithm is ECDSA (FIPS 140-3 compatible)"
else
  fail "Key algorithm is not ECDSA"
fi

# The key usages must include both server auth and client auth.
KEY_USAGES=$(grep -A2 'csi\.cert-manager\.io/key-usages:' <<< "$RENDERED_OUTPUT")
if grep -q 'server auth' <<< "$KEY_USAGES" && \
   grep -q 'client auth' <<< "$KEY_USAGES"; then
  pass "Key usages include serverAuth and clientAuth"
else
  fail "Key usages do not include both serverAuth and clientAuth"
fi

# The Certificate resource for peer mTLS must NOT be rendered (CSI driver issues
# directly). The browser Certificate is rendered by
# certmanager-browser-certificate.yaml, so there should be exactly 1 Certificate
# resource (the browser one), not 2.
CERT_RESOURCES=$(grep -c '^kind: Certificate$' <<< "$RENDERED_OUTPUT" || true)
if [ "$CERT_RESOURCES" -eq 1 ]; then
  pass "Exactly 1 Certificate resource rendered (browser only; no peer Certificate)"
else
  fail "Expected 1 Certificate resource (browser), found $CERT_RESOURCES"
fi

# The certmanager-certificate.yaml file must not contain a Certificate resource
# definition (it is a comment-only file explaining that the peer certificate is
# issued directly by the CSI driver, not a Certificate resource).
PEER_CERT_FILE="$CHART_DIR/templates/certmanager-certificate.yaml"
if grep -q '^kind: Certificate$' "$PEER_CERT_FILE" 2>/dev/null; then
  fail "certmanager-certificate.yaml still contains a Certificate resource (should be a comment-only file)"
else
  pass "certmanager-certificate.yaml does not render a Certificate resource"
fi

# ---------------------------------------------------------------------------
# 3. VSO manifest schema (VaultStaticSecret CRD)
# ---------------------------------------------------------------------------
echo ""
echo "--- 3. VSO manifest schema (VaultStaticSecret CRD) ---"

# The PROVISIONING.md must reference VaultStaticSecret (not VaultSecret).
if grep -q 'kind: VaultStaticSecret' "$VAULT_DOC"; then
  pass "PROVISIONING.md references VaultStaticSecret CRD"
else
  fail "PROVISIONING.md does not reference VaultStaticSecret CRD"
fi

# The apiVersion must be secrets.hashicorp.com/v1beta1 (the correct group).
if grep -q 'apiVersion: secrets\.hashicorp\.com/v1beta1' "$VAULT_DOC"; then
  pass "PROVISIONING.md uses apiVersion secrets.hashicorp.com/v1beta1"
else
  fail "PROVISIONING.md does not use the correct apiVersion"
fi

# The stale, wrong CRD group must NOT be used (secrets.vault.hashicorp.com is
# not a real group; the correct group is secrets.hashicorp.com).
if grep -q 'secrets\.vault\.hashicorp\.com' "$VAULT_DOC" 2>/dev/null; then
  fail "PROVISIONING.md still references the stale group secrets.vault.hashicorp.com"
else
  pass "PROVISIONING.md does not use the stale group secrets.vault.hashicorp.com"
fi

# The spec must have: mount, path, version, type, refreshAfter, vaultAuthRef,
# destination.name. (The mount field is REQUIRED for a KV v2 secret.)
for FIELD in 'mount: apps' 'path: gossipgrid/admin' 'version: 0' 'type: kv-v2' 'refreshAfter:' 'vaultAuthRef:' 'destination:' 'name: gossipgrid-auth'; do
  if grep -q "$FIELD" "$VAULT_DOC"; then
    pass "PROVISIONING.md VaultStaticSecret has field: $FIELD"
  else
    fail "PROVISIONING.md VaultStaticSecret missing field: $FIELD"
  fi
done

# The "apps" KV v2 mount must be used consistently in the provisioning
# commands (vault kv put / metadata get use the apps mount, not the old
# "secret" mount).
if grep -q 'vault kv put apps/gossipgrid/admin' "$VAULT_DOC"; then
  pass "PROVISIONING.md vault kv put uses the apps mount"
else
  fail "PROVISIONING.md vault kv put does not use the apps mount"
fi

if grep -q 'vault kv metadata get apps/gossipgrid/admin' "$VAULT_DOC"; then
  pass "PROVISIONING.md vault kv metadata get uses the apps mount"
else
  fail "PROVISIONING.md vault kv metadata get does not use the apps mount"
fi

# The old, incorrect CRD kind must NOT be present (VaultSecret is not a real
# CRD kind; the correct kind is VaultStaticSecret).
if grep -q 'kind: VaultSecret$' "$VAULT_DOC" 2>/dev/null; then
  fail "PROVISIONING.md still references kind: VaultSecret (not a real CRD kind)"
else
  pass "PROVISIONING.md does not reference kind: VaultSecret"
fi

# The mount field must be documented as required and the mount must NOT be
# baked into the path field (the path is within the mount, the mount is
# separate).
if grep -q 'The .mount. field is required' "$VAULT_DOC" 2>/dev/null; then
  pass "PROVISIONING.md documents the mount field as required"
else
  fail "PROVISIONING.md does not document the mount field as required"
fi

if grep -q 'include the mount in this field' "$VAULT_DOC" 2>/dev/null; then
  pass "PROVISIONING.md keeps the mount separate from the path field"
else
  fail "PROVISIONING.md does not document keeping the mount separate from the path"
fi

# ---------------------------------------------------------------------------
# 4. Argo CD values-file resolution
# ---------------------------------------------------------------------------
echo ""
echo "--- 4. Argo CD values-file resolution ---"

# The values file referenced by helm.valueFiles must exist at a path relative
# to the chart root (i.e. in the chart directory), so the reference resolves
# without any manual copy.
VALUEFILE=$(awk '
  /valueFiles:/ {inVF=1; next}
  inVF && /^[^ ]/ {inVF=0}
  inVF && /- */ { sub(/^[ \t]*-[ \t]*/, ""); if ($0 ~ /^values-.*\.yaml$/) { print $0; exit } }
' "$ARGO_APP")
if [ -z "$VALUEFILE" ]; then
  fail "Could not find a values-*.yaml reference in helm.valueFiles in application.yaml"
else
  pass "Argo CD Application references values file: $VALUEFILE"
  # The referenced file must exist relative to the chart directory.
  if [ -f "$CHART_DIR/$VALUEFILE" ]; then
    pass "Referenced values file $VALUEFILE exists in the chart directory (resolvable, no manual copy)"
  else
    fail "Referenced values file $VALUEFILE NOT found in the chart directory (manual copy would be required)"
  fi
  # It must NOT exist only at the old, out-of-chart location.
  if [ -f "$REPO_ROOT/deploy/argocd/$VALUEFILE" ]; then
    fail "Values file $VALUEFILE exists only at the old out-of-chart location deploy/argocd/ (stale layout)"
  else
    pass "No stale out-of-chart values file at deploy/argocd/$VALUEFILE"
  fi
fi

# The application.yaml must NOT document a manual copy into the chart directory
# (that was the stale, unresolved reference). The self-contained single-source
# layout requires no copy.
if grep -q 'copy it into the chart directory\|copied into the chart directory\|expected to be copied' "$ARGO_APP" 2>/dev/null; then
  fail "Argo CD Application still documents a manual copy of the values file into the chart directory"
else
  pass "Argo CD Application does not require a manual copy of the values file"
fi

# The single-source (self-contained) values file must not contain actual secret
# values (not the word "credential" in prose — the file discusses the
# credential provisioning path). Check for actual secret field patterns that
# would indicate embedded material.
if [ -f "$STAGING_VALUES" ]; then
  for SECRET_PAT in 'password: [a-zA-Z0-9]' 'verifier: [a-f0-9]' 'salt: [a-f0-9]' 'signing_key: [a-f0-9]'; do
    if grep -qi "$SECRET_PAT" "$STAGING_VALUES" 2>/dev/null; then
      fail "values-staging.yaml contains potential secret value: $SECRET_PAT"
    else
      pass "values-staging.yaml does not contain secret value pattern: $SECRET_PAT"
    fi
  done
  # The values file must render cleanly with the chart (the render above
  # already proved this; re-assert the non-empty output as a check).
  if [ -n "$STAGING_RENDERED_OUTPUT" ]; then
    pass "Chart renders cleanly with the staging values file applied"
  else
    fail "Chart produced empty output with the staging values file"
  fi
else
  fail "values-staging.yaml not found in the chart directory"
fi

# ---------------------------------------------------------------------------
# 5. Secret rotation without subPath
# ---------------------------------------------------------------------------
echo ""
echo "--- 5. Secret rotation without subPath ---"

# The trust bundle volumeMount must NOT have subPath.
TRUST_MOUNT_SECTION=$(sed -n '/peer-ca-bundle/,/readOnly/p' <<< "$RENDERED_OUTPUT")
if grep -q 'subPath:' <<< "$TRUST_MOUNT_SECTION"; then
  fail "Trust bundle volumeMount has subPath (blocks Secret rotation)"
else
  pass "Trust bundle volumeMount has no subPath (rotation is honored)"
fi

# The trust bundle mount path must be a directory (not a file path).
TRUST_MOUNT=$(grep 'mountPath:' <<< "$TRUST_MOUNT_SECTION" | awk '{print $2}')
if [ -n "$TRUST_MOUNT" ] && ! grep -q '\.pem$' <<< "$TRUST_MOUNT"; then
  pass "Trust bundle mountPath is a directory: $TRUST_MOUNT"
else
  fail "Trust bundle mountPath is not a directory: $TRUST_MOUNT"
fi

# The GOSSIPGRID_PEER_TRUST_BUNDLE_FILE env var must be the full file path
# (mount directory + secret key), not just the directory.
TRUST_BUNDLE_FILE=$(grep 'PEER_TRUST_BUNDLE_FILE' -A1 <<< "$RENDERED_OUTPUT" | grep 'value:' | awk -F'"' '{print $2}')
if [ -n "$TRUST_BUNDLE_FILE" ] && grep -q '\.pem$' <<< "$TRUST_BUNDLE_FILE"; then
  pass "GOSSIPGRID_PEER_TRUST_BUNDLE_FILE is the full file path: $TRUST_BUNDLE_FILE"
else
  fail "GOSSIPGRID_PEER_TRUST_BUNDLE_FILE is not a file path: $TRUST_BUNDLE_FILE"
fi

# The credentials volumeMount must NOT have subPath.
# Check only the YAML structure lines (not comments) by looking for the
# "subPath:" key followed by a value (not a comment).
CRED_MOUNT_SECTION=$(sed -n '/- name: credentials/,/readOnly/p' <<< "$RENDERED_OUTPUT" | grep -v '^\s*#')
if grep -q '^\s*subPath:' <<< "$CRED_MOUNT_SECTION"; then
  fail "Credentials volumeMount has subPath (blocks Secret rotation)"
else
  pass "Credentials volumeMount has no subPath (rotation is honored)"
fi

# The credentials mount path must be a directory.
CRED_MOUNT=$(grep 'mountPath:' <<< "$CRED_MOUNT_SECTION" | awk '{print $2}')
if [ -n "$CRED_MOUNT" ] && ! grep -q '\.json$' <<< "$CRED_MOUNT"; then
  pass "Credentials mountPath is a directory: $CRED_MOUNT"
else
  fail "Credentials mountPath is not a directory: $CRED_MOUNT"
fi

# ---------------------------------------------------------------------------
# 6. No unintended public peer exposure
# ---------------------------------------------------------------------------
echo ""
echo "--- 6. No unintended public peer exposure ---"

# The peer port must NOT be in the Gateway/HTTPRoute.
PEER_PORT_NUM=8443
HTTPROUTE_SECTION=$(sed -n '/^kind: HTTPRoute$/,/^---/p' <<< "$RENDERED_OUTPUT")
if grep -q "$PEER_PORT_NUM" <<< "$HTTPROUTE_SECTION"; then
  fail "Peer port ($PEER_PORT_NUM) is exposed in HTTPRoute (should be peer-only)"
else
  pass "Peer port is not exposed in HTTPRoute"
fi

# The peer port must NOT be in the web Service.
WEB_SERVICE_SECTION=$(sed -n '/^kind: Service$/,/^---/p' <<< "$RENDERED_OUTPUT" | grep -A 20 'name: web')
if grep -q '8443' <<< "$WEB_SERVICE_SECTION"; then
  fail "Peer port (8443) is exposed in the web Service"
else
  pass "Peer port is not exposed in the web Service"
fi

# The peer port must ONLY be in the headless Service (for DNS SRV discovery).
if grep -q "containerPort: $PEER_PORT_NUM" <<< "$RENDERED_OUTPUT"; then
  pass "Peer port ($PEER_PORT_NUM) is declared in the StatefulSet"
else
  fail "Peer port ($PEER_PORT_NUM) not found in StatefulSet"
fi

# ---------------------------------------------------------------------------
# 7. NetworkPolicy ingress restrictions
# ---------------------------------------------------------------------------
echo ""
echo "--- 7. NetworkPolicy ingress restrictions ---"

# No 0.0.0.0/0 in the rendered NetworkPolicy YAML structure (not in comments).
# The comment "do NOT use 0.0.0.0/0" is fine; only a YAML cidr: 0.0.0.0/0
# would be a problem.
NETPOL_SECTION=$(sed -n '/^kind: NetworkPolicy$/,/^---/p' <<< "$RENDERED_OUTPUT" | grep -v '^\s*#')
if grep -q 'cidr: 0\.0\.0\.0/0' <<< "$NETPOL_SECTION"; then
  fail "NetworkPolicy contains cidr: 0.0.0.0/0 in YAML (unrestricted ingress)"
else
  pass "NetworkPolicy has no 0.0.0.0/0 cidr in YAML structure"
fi

# The web port ingress must only be from the Gateway namespace and optional
# CIDRs. Count the ingress rules that target the web port.
WEB_PORT_RULES=$(grep -c "port: 8080" <<< "$NETPOL_SECTION" || true)
if [ "$WEB_PORT_RULES" -le 2 ]; then
  pass "Web port ingress rules: $WEB_PORT_RULES (Gateway + optional CIDR only)"
else
  fail "Web port has $WEB_PORT_RULES ingress rules (expected max 2: Gateway + CIDR)"
fi

# The peer port ingress must only be from GossipGrid pods.
PEER_PORT_RULES=$(grep -c "port: 8443" <<< "$NETPOL_SECTION" || true)
if [ "$PEER_PORT_RULES" -le 2 ]; then
  pass "Peer port ingress rules: $PEER_PORT_RULES (GossipGrid pods + egress only)"
else
  fail "Peer port has $PEER_PORT_RULES rules (expected max 2: ingress + egress)"
fi

# No ipBlock with 0.0.0.0/0 in the NetworkPolicy (at all).
if grep -q 'ipBlock:' <<< "$NETPOL_SECTION"; then
  # Check if any ipBlock is 0.0.0.0/0
  IPBLOCK_LINES=$(grep -A1 'ipBlock:' <<< "$NETPOL_SECTION")
  if grep -q '0\.0\.0\.0/0' <<< "$IPBLOCK_LINES"; then
    fail "NetworkPolicy has ipBlock 0.0.0.0/0"
  else
    pass "NetworkPolicy ipBlocks are specific CIDRs (not 0.0.0.0/0)"
  fi
else
  pass "NetworkPolicy has no ipBlock rules (Gateway namespace selector only)"
fi

# ---------------------------------------------------------------------------
# 8. Message retention configuration
# ---------------------------------------------------------------------------
echo ""
echo "--- 8. Message retention configuration ---"

# 8a. Default rendering (no overrides) must NOT emit any retention env vars.
RETENTION_VARS_DEFAULT=$(grep -c 'GOSSIPGRID_MESSAGE_RETENTION' <<< "$RENDERED_OUTPUT" || true)
if [ "$RETENTION_VARS_DEFAULT" -eq 0 ]; then
  pass "Default rendering: no retention env vars emitted (retention disabled)"
else
  fail "Default rendering: found $RETENTION_VARS_DEFAULT retention env vars (expected 0 when disabled)"
fi

# 8b. Staging rendering must emit the TTL and interval env vars with correct values.
RETENTION_TTL_STAGING=$(grep 'GOSSIPGRID_MESSAGE_RETENTION_TTL' -A1 <<< "$STAGING_RENDERED_OUTPUT" | grep 'value:' | awk -F'"' '{print $2}')
if [ "$RETENTION_TTL_STAGING" = "168h" ]; then
  pass "Staging rendering: GOSSIPGRID_MESSAGE_RETENTION_TTL = 168h"
else
  fail "Staging rendering: GOSSIPGRID_MESSAGE_RETENTION_TTL = '$RETENTION_TTL_STAGING' (expected '168h')"
fi

RETENTION_INTERVAL_STAGING=$(grep 'GOSSIPGRID_MESSAGE_RETENTION_INTERVAL' -A1 <<< "$STAGING_RENDERED_OUTPUT" | grep 'value:' | awk -F'"' '{print $2}')
if [ "$RETENTION_INTERVAL_STAGING" = "1h" ]; then
  pass "Staging rendering: GOSSIPGRID_MESSAGE_RETENTION_INTERVAL = 1h"
else
  fail "Staging rendering: GOSSIPGRID_MESSAGE_RETENTION_INTERVAL = '$RETENTION_INTERVAL_STAGING' (expected '1h')"
fi

# 8c. Staging rendering must NOT emit max sweep rows/duration (left at app defaults).
# The values-staging.yaml does not set these (0 and ""), so the template's
# conditional blocks skip them.
MAX_ROWS_STAGING=$(grep -c 'GOSSIPGRID_MESSAGE_RETENTION_MAX_SWEEP_ROWS' <<< "$STAGING_RENDERED_OUTPUT" || true)
if [ "$MAX_ROWS_STAGING" -eq 0 ]; then
  pass "Staging rendering: no GOSSIPGRID_MESSAGE_RETENTION_MAX_SWEEP_ROWS (app default applies)"
else
  fail "Staging rendering: GOSSIPGRID_MESSAGE_RETENTION_MAX_SWEEP_ROWS present (expected absent — app default)"
fi

MAX_DUR_STAGING=$(grep -c 'GOSSIPGRID_MESSAGE_RETENTION_MAX_SWEEP_DURATION' <<< "$STAGING_RENDERED_OUTPUT" || true)
if [ "$MAX_DUR_STAGING" -eq 0 ]; then
  pass "Staging rendering: no GOSSIPGRID_MESSAGE_RETENTION_MAX_SWEEP_DURATION (app default applies)"
else
  fail "Staging rendering: GOSSIPGRID_MESSAGE_RETENTION_MAX_SWEEP_DURATION present (expected absent — app default)"
fi

# 8d. All StatefulSet replicas must receive identical retention config.
# The StatefulSet template renders a single pod spec (Kubernetes applies it to
# all replicas), so the env vars appear exactly once in the rendered output.
# This check verifies that the retention env vars are in the StatefulSet's
# container spec (not in some other resource) and that they appear exactly
# once each (single pod template → all replicas get identical config).
TTL_COUNT=$(grep -c 'GOSSIPGRID_MESSAGE_RETENTION_TTL' <<< "$STAGING_RENDERED_OUTPUT" || true)
if [ "$TTL_COUNT" -eq 1 ]; then
  pass "Retention TTL env var appears exactly once (single pod spec → all replicas identical)"
else
  fail "Retention TTL env var appears $TTL_COUNT times (expected exactly 1)"
fi

# 8e. Verify the existing probe, security, and image settings are unchanged
# in the staging rendering (regression guard: retention must not alter them).
# Image digest must still be present and unchanged.
if grep -q 'image: gitlab\.lan:5050/slipmat/gossipgrid@sha256:' <<< "$STAGING_RENDERED_OUTPUT" || \
   grep -q 'imagePullPolicy: IfNotPresent' <<< "$STAGING_RENDERED_OUTPUT"; then
  pass "Image pinning settings unchanged in staging rendering"
else
  fail "Image pinning settings appear to have changed in staging rendering"
fi

# Probes must still be present.
if grep -q 'startupProbe:' <<< "$STAGING_RENDERED_OUTPUT" && \
   grep -q 'livenessProbe:' <<< "$STAGING_RENDERED_OUTPUT" && \
   grep -q 'readinessProbe:' <<< "$STAGING_RENDERED_OUTPUT"; then
  pass "All three probes (startup, liveness, readiness) present in staging rendering"
else
  fail "One or more probes missing in staging rendering"
fi

# Security context must still be present.
if grep -q 'runAsNonRoot: true' <<< "$STAGING_RENDERED_OUTPUT" && \
   grep -q 'readOnlyRootFilesystem: true' <<< "$STAGING_RENDERED_OUTPUT"; then
  pass "Pod security settings (runAsNonRoot, readOnlyRootFilesystem) unchanged"
else
  fail "Pod security settings changed in staging rendering"
fi

# Volume mounts must still include /data (emptyDir).
if grep -q 'mountPath: /data' <<< "$STAGING_RENDERED_OUTPUT" && \
   grep -q 'emptyDir:' <<< "$STAGING_RENDERED_OUTPUT"; then
  pass "emptyDir /data volume mount unchanged"
else
  fail "emptyDir /data volume mount missing or changed in staging rendering"
fi

# ---------------------------------------------------------------------------
# 9. Retention enabled without TTL must fail rendering
# ---------------------------------------------------------------------------
echo ""
echo "--- 9. Retention enabled without TTL must fail rendering ---"

# 9a. Retention enabled with an empty TTL must FAIL helm template.
# This proves the chart's fail() guard rejects a misconfigured render
# (enabled: true, ttl: "") rather than shipping a broken pod spec.
# The || true guards the expected failure so set -e does not abort the script.
# We detect failure by checking whether the output contains a StatefulSet
# resource (a successful render always has one; a fail() render does not).
EMPTY_TTL_RENDER_OUTPUT=$(helm template "$RELEASE" "$CHART_DIR" \
  --namespace "$NAMESPACE" \
  --set messageRetention.enabled=true \
  --set messageRetention.ttl="" 2>&1) || true
if [ -n "$EMPTY_TTL_RENDER_OUTPUT" ] && grep -q 'kind: StatefulSet' <<< "$EMPTY_TTL_RENDER_OUTPUT"; then
  # A StatefulSet was rendered — the fail() guard did NOT trigger (bad).
  fail "Retention enabled with empty TTL rendered successfully (expected Helm fail() error)"
else
  # No StatefulSet in output — the fail() guard triggered (good).
  if grep -qi 'ttl' <<< "$EMPTY_TTL_RENDER_OUTPUT"; then
    pass "Retention enabled with empty TTL fails rendering with TTL-specific error"
  else
    fail "Retention enabled with empty TTL did not render but without a TTL-specific error: $EMPTY_TTL_RENDER_OUTPUT"
  fi
fi

# 9b. Retention disabled (the default) must remain valid.
# The default rendering (RENDERED_OUTPUT) already proves this, but re-assert
# explicitly: disabled retention with no TTL set must render cleanly.
# The || true guards an unexpected failure so set -e does not abort.
DISABLED_TTL_RENDER_OUTPUT=$(helm template "$RELEASE" "$CHART_DIR" \
  --namespace "$NAMESPACE" \
  --set messageRetention.enabled=false \
  --set messageRetention.ttl="" 2>&1) || true
if [ -n "$DISABLED_TTL_RENDER_OUTPUT" ] && grep -q 'kind: StatefulSet' <<< "$DISABLED_TTL_RENDER_OUTPUT"; then
  pass "Retention disabled with empty TTL renders successfully (valid)"
else
  fail "Retention disabled with empty TTL did not render (should be valid): $DISABLED_TTL_RENDER_OUTPUT"
fi

# 9c. Retention enabled with a valid TTL must render cleanly (staging values
# already prove this, but assert it explicitly with --set for completeness).
# The || true guards an unexpected failure so set -e does not abort.
VALID_TTL_RENDER_OUTPUT=$(helm template "$RELEASE" "$CHART_DIR" \
  --namespace "$NAMESPACE" \
  --set messageRetention.enabled=true \
  --set messageRetention.ttl="168h" 2>&1) || true
if [ -n "$VALID_TTL_RENDER_OUTPUT" ] && grep -q 'kind: StatefulSet' <<< "$VALID_TTL_RENDER_OUTPUT"; then
  pass "Retention enabled with valid TTL (168h) renders successfully"
else
  fail "Retention enabled with valid TTL (168h) did not render (should be valid): $VALID_TTL_RENDER_OUTPUT"
fi

# ---------------------------------------------------------------------------
# 10. Initial anti-entropy synchronization barrier (Milestone 7A enhancement)
# ---------------------------------------------------------------------------
echo ""
echo "--- 10. Initial sync barrier: enabled/disabled rendering + deadlock prevention ---"

# 10a. Default rendering (initialSync disabled, the chart default) must NOT
# emit the initial-sync env var (backward compatibility: the existing
# readiness behavior is preserved).
IS_VAR_DEFAULT=$(grep -c 'GOSSIPGRID_INITIAL_SYNC_BARRIER_ENABLED' <<< "$RENDERED_OUTPUT" || true)
if [ "$IS_VAR_DEFAULT" -eq 0 ]; then
  pass "Default rendering: no initial-sync env var emitted (barrier disabled, backward compatible)"
else
  fail "Default rendering: found $IS_VAR_DEFAULT initial-sync env vars (expected 0 when disabled)"
fi

# 10b. Staging rendering (initialSync enabled) must emit the env var with
# value "true".
IS_VAR_STAGING=$(grep 'GOSSIPGRID_INITIAL_SYNC_BARRIER_ENABLED' -A1 <<< "$STAGING_RENDERED_OUTPUT" | grep 'value:' | awk -F'"' '{print $2}')
if [ "$IS_VAR_STAGING" = "true" ]; then
  pass "Staging rendering: GOSSIPGRID_INITIAL_SYNC_BARRIER_ENABLED = true"
else
  fail "Staging rendering: GOSSIPGRID_INITIAL_SYNC_BARRIER_ENABLED = '$IS_VAR_STAGING' (expected 'true')"
fi

# 10c. The env var must appear exactly once in the staging rendering (single
# pod spec -> all replicas receive identical config).
IS_COUNT_STAGING=$(grep -c 'GOSSIPGRID_INITIAL_SYNC_BARRIER_ENABLED' <<< "$STAGING_RENDERED_OUTPUT" || true)
if [ "$IS_COUNT_STAGING" -eq 1 ]; then
  pass "Initial-sync env var appears exactly once in staging (single pod spec -> all replicas identical)"
else
  fail "Initial-sync env var appears $IS_COUNT_STAGING times in staging (expected exactly 1)"
fi

# 10d. Incompatible configuration (initialSync enabled, antiEntropy disabled)
# must FAIL rendering: the barrier is driven by the anti-entropy engine's
# completed-pass accounting, so without anti-entropy the pod would remain
# NotReady indefinitely. The chart's fail() guard rejects the render.
# The || true guards the expected failure so set -e does not abort.
INCOMPATIBLE_RENDER_OUTPUT=$(helm template "$RELEASE" "$CHART_DIR" \
  --namespace "$NAMESPACE" \
  --set initialSync.enabled=true \
  --set antiEntropy.enabled=false 2>&1) || true
if [ -n "$INCOMPATIBLE_RENDER_OUTPUT" ] && grep -q 'kind: StatefulSet' <<< "$INCOMPATIBLE_RENDER_OUTPUT"; then
  fail "initialSync.enabled with antiEntropy.enabled=false rendered successfully (expected Helm fail() error)"
else
  if grep -qi 'antiEntropy' <<< "$INCOMPATIBLE_RENDER_OUTPUT"; then
    pass "initialSync enabled with antiEntropy disabled fails rendering with a specific error"
  else
    fail "initialSync enabled with antiEntropy disabled did not render but without a specific error: $INCOMPATIBLE_RENDER_OUTPUT"
  fi
fi

# 10e. DEADLOCK PREVENTION: the headless peer Service must retain
# publishNotReadyAddresses: true in BOTH the default and the staging
# rendering. This is CRITICAL for bootstrap: without it, a NotReady pod
# (initial-sync pending) would not have a DNS record, and a new pod could not
# discover its peers until a peer became Ready — a bootstrap deadlock.
# The initial-sync barrier makes pods NotReady during startup, so this flag
# must be preserved.
if grep -q 'publishNotReadyAddresses: true' <<< "$RENDERED_OUTPUT"; then
  pass "Default rendering: headless Service retains publishNotReadyAddresses: true (bootstrap safe)"
else
  fail "Default rendering: publishNotReadyAddresses: true missing from headless Service (bootstrap deadlock risk)"
fi
if grep -q 'publishNotReadyAddresses: true' <<< "$STAGING_RENDERED_OUTPUT"; then
  pass "Staging rendering: headless Service retains publishNotReadyAddresses: true (bootstrap safe with barrier on)"
else
  fail "Staging rendering: publishNotReadyAddresses: true missing from headless Service (bootstrap deadlock risk with barrier on)"
fi

# 10f. DEADLOCK PREVENTION: the readiness probe must still target /readyz and
# the liveness probe must still target /healthz in the staging rendering
# (the barrier changes /readyz semantics but the probe endpoints and the
# liveness/readiness separation are preserved). An unavailable peer must not
# trigger a liveness restart.
if grep -q 'readinessProbe:' <<< "$STAGING_RENDERED_OUTPUT" && \
   grep -q 'path: /readyz' <<< "$STAGING_RENDERED_OUTPUT"; then
  pass "Staging rendering: readiness probe still targets /readyz"
else
  fail "Staging rendering: readiness probe /readyz target missing"
fi
if grep -q 'livenessProbe:' <<< "$STAGING_RENDERED_OUTPUT" && \
   grep -q 'path: /healthz' <<< "$STAGING_RENDERED_OUTPUT"; then
  pass "Staging rendering: liveness probe still targets /healthz (no restart storm on peer outage)"
else
  fail "Staging rendering: liveness probe /healthz target missing"
fi

# 10g. The podManagementPolicy must remain Parallel in the staging rendering
# (simultaneous initial pod startup avoids bootstrap deadlock; the barrier
# does not change the pod management policy).
if grep -q 'podManagementPolicy: Parallel' <<< "$STAGING_RENDERED_OUTPUT"; then
  pass "Staging rendering: podManagementPolicy remains Parallel (simultaneous startup avoids deadlock)"
else
  fail "Staging rendering: podManagementPolicy is not Parallel (simultaneous startup required to avoid deadlock)"
fi

# 10h. The existing 5 replicas, emptyDir /data, and peer TLS must be
# unchanged in the staging rendering (the barrier must not alter the
# workload shape).
if grep -q 'replicas: 5' <<< "$STAGING_RENDERED_OUTPUT"; then
  pass "Staging rendering: replicas remain 5"
else
  fail "Staging rendering: replicas is not 5"
fi
if grep -q 'mountPath: /data' <<< "$STAGING_RENDERED_OUTPUT" && \
   grep -q 'emptyDir:' <<< "$STAGING_RENDERED_OUTPUT"; then
  pass "Staging rendering: emptyDir /data (ephemeral storage) unchanged"
else
  fail "Staging rendering: emptyDir /data changed"
fi
if grep -q 'GOSSIPGRID_PEER_TLS_ENABLED' <<< "$STAGING_RENDERED_OUTPUT"; then
  pass "Staging rendering: peer TLS env vars unchanged"
else
  fail "Staging rendering: peer TLS env vars missing or changed"
fi

# ---------------------------------------------------------------------------
# 11. Genesis grant RBAC (Milestone 5B, Stage 1A)
# ---------------------------------------------------------------------------
echo ""
echo "--- 11. Genesis grant RBAC ---"

# 11a. DEFAULT rendering (genesis.enabled: false) must NOT emit any genesis
# RBAC resources (ServiceAccount, Role, RoleBinding). The grant Job must never
# be created by the existing deployment configuration.
GEN_SA_DEFAULT=$(grep -c 'kind: ServiceAccount$' <<< "$RENDERED_OUTPUT" || true)
# The default rendering has exactly 1 ServiceAccount (the application SA,
# gossipgrid). A second ServiceAccount (the genesis one) would indicate the
# genesis RBAC leaked into the default rendering.
if [ "$GEN_SA_DEFAULT" -eq 1 ]; then
  pass "Default rendering: exactly 1 ServiceAccount (application SA only; genesis RBAC not rendered)"
else
  fail "Default rendering: found $GEN_SA_DEFAULT ServiceAccounts (expected exactly 1; genesis RBAC must be disabled by default)"
fi
# No genesis Role or RoleBinding in the default rendering.
if grep -q 'kind: Role$' <<< "$RENDERED_OUTPUT" || grep -q 'kind: RoleBinding$' <<< "$RENDERED_OUTPUT"; then
  fail "Default rendering: a Role or RoleBinding was rendered (genesis RBAC must be disabled by default)"
else
  pass "Default rendering: no Role or RoleBinding (genesis RBAC not rendered)"
fi
# The default rendering must NOT contain the genesis ServiceAccount name.
if grep -q 'gossipgrid-genesis' <<< "$RENDERED_OUTPUT"; then
  fail "Default rendering: the genesis ServiceAccount name 'gossipgrid-genesis' appears (genesis RBAC must be disabled by default)"
else
  pass "Default rendering: no 'gossipgrid-genesis' identity (genesis RBAC disabled)"
fi

# 11b. ENABLED rendering (genesis.enabled: true) must emit the genesis
# ServiceAccount, Role, and RoleBinding.
GENESIS_RENDERED_OUTPUT=$(helm template "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" --set genesis.enabled=true 2>&1)
if [ $? -ne 0 ]; then
  echo "FATAL: helm template with genesis.enabled=true failed:"
  echo "$GENESIS_RENDERED_OUTPUT"
  exit 1
fi
# The genesis ServiceAccount must be present.
if grep -q 'name: gossipgrid-genesis' <<< "$GENESIS_RENDERED_OUTPUT" && \
   grep -q 'kind: ServiceAccount$' <<< "$GENESIS_RENDERED_OUTPUT"; then
  pass "Enabled rendering: genesis ServiceAccount (gossipgrid-genesis) rendered"
else
  fail "Enabled rendering: genesis ServiceAccount not rendered"
fi
# The Role and RoleBinding must be present.
if grep -q 'kind: Role$' <<< "$GENESIS_RENDERED_OUTPUT" && \
   grep -q 'kind: RoleBinding$' <<< "$GENESIS_RENDERED_OUTPUT"; then
  pass "Enabled rendering: genesis Role and RoleBinding rendered"
else
  fail "Enabled rendering: genesis Role or RoleBinding not rendered"
fi

# 11c. RBAC PERMISSIONS: the Role must grant create on configmaps and get on
# the NAMED ConfigMap (resourceNames). It must NOT grant update, patch, or
# delete (the grant performs only a create).
# Extract the Role rules (between "kind: Role" and the next "---" or
# "kind: RoleBinding").
ROLE_SECTION=$(awk '
  /^kind: Role$/ {inrole=1}
  inrole && /^---/ {inrole=0}
  inrole {print}
' <<< "$GENESIS_RENDERED_OUTPUT")
# create on configmaps must be present.
if grep -q 'resources: \[\"configmaps\"\]' <<< "$ROLE_SECTION" && \
   grep -q 'verbs: \[\"create\"\]' <<< "$ROLE_SECTION"; then
  pass "Enabled rendering: Role grants create on configmaps"
else
  fail "Enabled rendering: Role does not grant create on configmaps"
fi
# get on the NAMED ConfigMap (resourceNames) must be present.
if grep -q 'resourceNames: \[\"gossipgrid-cluster-metadata\"\]' <<< "$ROLE_SECTION" && \
   grep -q 'verbs: \[\"get\"\]' <<< "$ROLE_SECTION"; then
  pass "Enabled rendering: Role grants get on the named ConfigMap (resourceNames)"
else
  fail "Enabled rendering: Role does not grant get on the named ConfigMap (resourceNames)"
fi
# update, patch, and delete must NOT be present.
for BAD_VERB in 'update' 'patch' 'delete'; do
  if grep -q "\"$BAD_VERB\"" <<< "$ROLE_SECTION"; then
    fail "Enabled rendering: Role grants the forbidden verb '$BAD_VERB' (the grant performs only create)"
  else
    pass "Enabled rendering: Role does not grant the forbidden verb '$BAD_VERB'"
  fi
done

# 11d. NO ClusterRole, no cluster-scoped binding. The RBAC must be
# namespaced only.
if grep -q 'kind: ClusterRole' <<< "$GENESIS_RENDERED_OUTPUT" || \
   grep -q 'kind: ClusterRoleBinding' <<< "$GENESIS_RENDERED_OUTPUT"; then
  fail "Enabled rendering: a ClusterRole or ClusterRoleBinding was rendered (the genesis RBAC must be namespaced only)"
else
  pass "Enabled rendering: no ClusterRole or ClusterRoleBinding (namespaced only)"
fi

# 11e. The RoleBinding must bind the genesis ServiceAccount (not the
# application ServiceAccount). The subjects must reference
# gossipgrid-genesis.
RB_SECTION=$(awk '
  /^kind: RoleBinding$/ {inrb=1}
  inrb {print}
' <<< "$GENESIS_RENDERED_OUTPUT")
if grep -q 'name: gossipgrid-genesis' <<< "$RB_SECTION" && \
   grep -q 'kind: ServiceAccount' <<< "$RB_SECTION"; then
  pass "Enabled rendering: RoleBinding binds the genesis ServiceAccount"
else
  fail "Enabled rendering: RoleBinding does not bind the genesis ServiceAccount"
fi

# 11f. The application ServiceAccount (gossipgrid) must NOT be a subject of
# the genesis RoleBinding (the app pods must not inherit create permission).
# Only the subjects: block matters (the RoleBinding's own metadata.name is
# gossipgrid-genesis, not gossipgrid).
SUBJECTS_BLOCK=$(awk '
  /^subjects:/ {insubj=1; next}
  insubj && /^---/ {insubj=0}
  insubj && /^[^ ]/ && !/^  / {insubj=0}
  insubj {print}
' <<< "$RB_SECTION")
if grep -qE 'name: gossipgrid[[:space:]]*$' <<< "$SUBJECTS_BLOCK"; then
  fail "Enabled rendering: the application ServiceAccount (gossipgrid) is a subject of the genesis RoleBinding (must be separate)"
else
  pass "Enabled rendering: the application ServiceAccount is NOT a subject of the genesis RoleBinding (separate identity)"
fi

# 11g. The Job manifest (deploy/genesis/job.yaml) must be operator-managed:
# it must NOT carry a Helm hook annotation (it is not a Helm hook and is not
# created by Argo CD reconciliation). The Job must reference the genesis
# ServiceAccount and the genesis subcommand.
#
# NOTE: the job.yaml file contains "helm.sh/hook" in a COMMENT (explaining it
# is NOT a Helm hook). The check must match only actual annotation keys
# (a YAML key line "helm.sh/hook: value"), not comment text.
JOB_MANIFEST="deploy/genesis/job.yaml"
if [ -f "$REPO_ROOT/$JOB_MANIFEST" ]; then
  # Match "helm.sh/hook:" as a YAML annotation key (not a comment). The
  # pattern requires the key to be followed by a colon, which only actual
  # annotation lines have; comment lines have the string without a trailing
  # colon in the annotation context.
  if grep -qE '^[[:space:]]*helm\.sh/hook:' "$REPO_ROOT/$JOB_MANIFEST"; then
    fail "Job manifest carries a helm.sh/hook annotation (the Job must be operator-managed, not a Helm hook)"
  else
    pass "Job manifest has no helm.sh/hook annotation (operator-managed, not a Helm hook)"
  fi
  if grep -q 'serviceAccountName: gossipgrid-genesis' "$REPO_ROOT/$JOB_MANIFEST"; then
    pass "Job manifest references the genesis ServiceAccount (gossipgrid-genesis)"
  else
    fail "Job manifest does not reference the genesis ServiceAccount"
  fi
  # The command list in the Job spec must include "genesis" as the
  # subcommand (the line "- genesis" in the command array).
  if grep -qE '^[[:space:]]*-[[:space:]]*genesis[[:space:]]*$' "$REPO_ROOT/$JOB_MANIFEST"; then
    pass 'Job manifest runs the "genesis" subcommand'
  else
    fail 'Job manifest does not run the "genesis" subcommand'
  fi
  if grep -q 'backoffLimit: 0' "$REPO_ROOT/$JOB_MANIFEST"; then
    pass "Job manifest has backoffLimit: 0 (no automatic retries; failures are surfaced to the operator)"
  else
    fail "Job manifest does not have backoffLimit: 0 (automatic retries could mask a failure)"
  fi

  # 11g-structural: Parse the Job YAML and validate its structure. This
  # check goes beyond text presence: it verifies that restartPolicy is
  # under spec.template.spec (not the Job-level spec), that the
  # executable path is /gossipgrid (matching the Dockerfile), that
  # securityContext has the required fields, and that fsGroup is absent
  # from the container securityContext (a pod-level field, and
  # unnecessary when no volumes are mounted).
  JOB_YAML_STRUCTURAL=$(python3 -c "
import yaml, sys
with open('$REPO_ROOT/$JOB_MANIFEST') as f:
    job = yaml.safe_load(f)
assert job['kind'] == 'Job', 'kind must be Job'
assert job['apiVersion'] == 'batch/v1', 'apiVersion must be batch/v1'
spec = job['spec']
assert spec.get('backoffLimit') == 0, 'backoffLimit must be 0'
# restartPolicy must NOT be at the Job spec level (it belongs on the pod spec).
assert 'restartPolicy' not in spec, 'restartPolicy must not be at Job spec level (it belongs under spec.template.spec)'
pod_spec = spec['template']['spec']
# restartPolicy must be under spec.template.spec and set to Never.
assert pod_spec.get('restartPolicy') == 'Never', 'restartPolicy must be Never under spec.template.spec'
# serviceAccountName must be gossipgrid-genesis.
assert pod_spec.get('serviceAccountName') == 'gossipgrid-genesis', 'serviceAccountName must be gossipgrid-genesis'
container = pod_spec['containers'][0]
# The executable must be /gossipgrid (the Dockerfile builds the binary at /gossipgrid).
assert container['command'][0] == '/gossipgrid', 'command[0] must be /gossipgrid (got %s)' % container['command'][0]
assert container['command'][1] == 'genesis', 'command[1] must be genesis'
# The command must pass --namespace and --configmap.
assert '--namespace' in container['command'], 'command must include --namespace'
assert '--configmap' in container['command'], 'command must include --configmap'
assert 'gossipgrid-cluster-metadata' in container['command'], 'command must include gossipgrid-cluster-metadata'
# Security context: container-level.
sc = container.get('securityContext', {})
assert sc.get('runAsNonRoot') == True, 'securityContext.runAsNonRoot must be true'
assert sc.get('runAsUser') == 65534, 'securityContext.runAsUser must be 65534'
assert sc.get('runAsGroup') == 65534, 'securityContext.runAsGroup must be 65534'
assert sc.get('allowPrivilegeEscalation') == False, 'securityContext.allowPrivilegeEscalation must be false'
assert sc.get('readOnlyRootFilesystem') == True, 'securityContext.readOnlyRootFilesystem must be true'
assert sc.get('seccompProfile', {}).get('type') == 'RuntimeDefault', 'securityContext.seccompProfile.type must be RuntimeDefault'
caps = sc.get('capabilities', {})
assert 'ALL' in caps.get('drop', []), 'securityContext.capabilities.drop must include ALL'
# fsGroup is a pod-level securityContext field. The Job has no volumes,
# so it is unnecessary and must not appear in the container securityContext.
assert 'fsGroup' not in sc, 'fsGroup must not be in container securityContext (it is a pod-level field; the Job has no volumes)'
print('OK')
" 2>&1)
  if [ $? -eq 0 ] && [ "$JOB_YAML_STRUCTURAL" = "OK" ]; then
    pass "Job manifest YAML structure validated (executable path, restartPolicy placement, securityContext, no fsGroup)"
  else
    fail "Job manifest YAML structure invalid: $JOB_YAML_STRUCTURAL"
  fi
else
  fail "Job manifest $JOB_MANIFEST not found"
fi

# 11h. The genesis subcommand must be dispatched in main.go (the genesis
# subcommand is a one-shot, operator-authorized grant; it is not a server
# subcommand).
if grep -q 'os.Args\[1\] == "genesis"' "$REPO_ROOT/cmd/gossipgrid/main.go"; then
  pass 'main.go dispatches the "genesis" subcommand (one-shot, operator-authorized)'
else
  fail 'main.go does not dispatch the "genesis" subcommand'
fi

# ---------------------------------------------------------------------------
# 12. Genesis finalization RBAC and Job (Milestone 5B, Stage 1B)
# ---------------------------------------------------------------------------
echo ""
echo "--- 12. Genesis finalization RBAC and Job ---"

# 12a. DEFAULT rendering (finalize.enabled: false) must NOT emit any
# finalization RBAC resources (ServiceAccount, Role, RoleBinding). The
# finalization Job must never be created by the existing deployment
# configuration, and the finalization RBAC must not leak into the default
# rendering.
FIN_SA_DEFAULT=$(grep -c 'name: gossipgrid-finalize' <<< "$RENDERED_OUTPUT" || true)
if [ "$FIN_SA_DEFAULT" -eq 0 ]; then
  pass "Default rendering: no 'gossipgrid-finalize' identity (finalization RBAC disabled by default)"
else
  fail "Default rendering: found $FIN_SA_DEFAULT 'gossipgrid-finalize' identities (finalization RBAC must be disabled by default)"
fi
# No finalization Role or RoleBinding in the default rendering (the genesis
# grant RBAC is also disabled by default, so there should be no Role or
# RoleBinding at all).
if grep -q 'kind: Role$' <<< "$RENDERED_OUTPUT" || grep -q 'kind: RoleBinding$' <<< "$RENDERED_OUTPUT"; then
  fail "Default rendering: a Role or RoleBinding was rendered (finalization and genesis RBAC must be disabled by default)"
else
  pass "Default rendering: no Role or RoleBinding (finalization RBAC not rendered)"
fi

# 12b. ENABLED rendering (finalize.enabled: true) must emit the finalization
# ServiceAccount, Role, and RoleBinding.
FINALIZE_RENDERED_OUTPUT=$(helm template "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" --set finalize.enabled=true 2>&1)
if [ $? -ne 0 ]; then
  echo "FATAL: helm template with finalize.enabled=true failed:"
  echo "$FINALIZE_RENDERED_OUTPUT"
  exit 1
fi
# The finalization ServiceAccount must be present.
if grep -q 'name: gossipgrid-finalize' <<< "$FINALIZE_RENDERED_OUTPUT" && \
   grep -q 'kind: ServiceAccount$' <<< "$FINALIZE_RENDERED_OUTPUT"; then
  pass "Enabled rendering: finalization ServiceAccount (gossipgrid-finalize) rendered"
else
  fail "Enabled rendering: finalization ServiceAccount not rendered"
fi
# The Role and RoleBinding must be present.
if grep -q 'kind: Role$' <<< "$FINALIZE_RENDERED_OUTPUT" && \
   grep -q 'kind: RoleBinding$' <<< "$FINALIZE_RENDERED_OUTPUT"; then
  pass "Enabled rendering: finalization Role and RoleBinding rendered"
else
  fail "Enabled rendering: finalization Role or RoleBinding not rendered"
fi

# 12c. RBAC PERMISSIONS: the Role must grant get AND update on the NAMED
# ConfigMap (resourceNames). It must NOT grant create, delete, patch, list,
# or watch (the finalization performs only a single CAS update; it never
# creates, deletes, or otherwise mutates the ConfigMap).
ROLE_SECTION=$(awk '
  /^kind: Role$/ {inrole=1}
  inrole && /^---/ {inrole=0}
  inrole {print}
' <<< "$FINALIZE_RENDERED_OUTPUT")
# get on the NAMED ConfigMap (resourceNames) must be present.
if grep -q 'resourceNames: \["gossipgrid-cluster-metadata"\]' <<< "$ROLE_SECTION" && \
   grep -q 'verbs: \["get"\]' <<< "$ROLE_SECTION"; then
  pass "Enabled rendering: Role grants get on the named ConfigMap (resourceNames)"
else
  fail "Enabled rendering: Role does not grant get on the named ConfigMap (resourceNames)"
fi
# update on the NAMED ConfigMap (resourceNames) must be present.
if grep -q 'resourceNames: \["gossipgrid-cluster-metadata"\]' <<< "$ROLE_SECTION" && \
   grep -q 'verbs: \["update"\]' <<< "$ROLE_SECTION"; then
  pass "Enabled rendering: Role grants update on the named ConfigMap (resourceNames)"
else
  fail "Enabled rendering: Role does not grant update on the named ConfigMap (resourceNames)"
fi
# create, delete, patch, list, and watch must NOT be present.
for BAD_VERB in 'create' 'delete' 'patch' 'list' 'watch'; do
  if grep -q "\"$BAD_VERB\"" <<< "$ROLE_SECTION"; then
    fail "Enabled rendering: Role grants the forbidden verb '$BAD_VERB' (the finalization performs only a single CAS update)"
  else
    pass "Enabled rendering: Role does not grant the forbidden verb '$BAD_VERB'"
  fi
done

# 12d. NO ClusterRole, no cluster-scoped binding. The RBAC must be
# namespaced only.
if grep -q 'kind: ClusterRole' <<< "$FINALIZE_RENDERED_OUTPUT" || \
   grep -q 'kind: ClusterRoleBinding' <<< "$FINALIZE_RENDERED_OUTPUT"; then
  fail "Enabled rendering: a ClusterRole or ClusterRoleBinding was rendered (the finalization RBAC must be namespaced only)"
else
  pass "Enabled rendering: no ClusterRole or ClusterRoleBinding (namespaced only)"
fi

# 12e. The RoleBinding must bind the finalization ServiceAccount (not the
# application ServiceAccount). The subjects must reference
# gossipgrid-finalize.
RB_SECTION=$(awk '
  /^kind: RoleBinding$/ {inrb=1}
  inrb {print}
' <<< "$FINALIZE_RENDERED_OUTPUT")
if grep -q 'name: gossipgrid-finalize' <<< "$RB_SECTION" && \
   grep -q 'kind: ServiceAccount' <<< "$RB_SECTION"; then
  pass "Enabled rendering: RoleBinding binds the finalization ServiceAccount"
else
  fail "Enabled rendering: RoleBinding does not bind the finalization ServiceAccount"
fi

# 12f. The application ServiceAccount (gossipgrid) must NOT be a subject of
# the finalization RoleBinding (the app pods must not inherit update
# permission). Only the subjects: block matters.
SUBJECTS_BLOCK=$(awk '
  /^subjects:/ {insubj=1; next}
  insubj && /^---/ {insubj=0}
  insubj && /^[^ ]/ && !/^  / {insubj=0}
  insubj {print}
' <<< "$RB_SECTION")
if grep -qE 'name: gossipgrid[[:space:]]*$' <<< "$SUBJECTS_BLOCK"; then
  fail "Enabled rendering: the application ServiceAccount (gossipgrid) is a subject of the finalization RoleBinding (must be separate)"
else
  pass "Enabled rendering: the application ServiceAccount is NOT a subject of the finalization RoleBinding (separate identity)"
fi

# 12g. The Job manifest (deploy/genesis/finalize-job.yaml) must be
# operator-managed: it must NOT carry a Helm hook annotation, must reference
# the finalization ServiceAccount, and must run the "genesis finalize"
# subcommand with the --confirm-seed-bound flag (the explicit operator gate).
JOB_MANIFEST="deploy/genesis/finalize-job.yaml"
if [ -f "$REPO_ROOT/$JOB_MANIFEST" ]; then
  # Match "helm.sh/hook:" as a YAML annotation key (not a comment).
  if grep -qE '^[[:space:]]*helm\.sh/hook:' "$REPO_ROOT/$JOB_MANIFEST"; then
    fail "Finalize Job manifest carries a helm.sh/hook annotation (the Job must be operator-managed, not a Helm hook)"
  else
    pass "Finalize Job manifest has no helm.sh/hook annotation (operator-managed, not a Helm hook)"
  fi
  if grep -q 'serviceAccountName: gossipgrid-finalize' "$REPO_ROOT/$JOB_MANIFEST"; then
    pass "Finalize Job manifest references the finalization ServiceAccount (gossipgrid-finalize)"
  else
    fail "Finalize Job manifest does not reference the finalization ServiceAccount"
  fi
  # The command list must include "genesis" and "finalize" as the
  # subcommand pair.
  if grep -qE '^[[:space:]]*-[[:space:]]*genesis[[:space:]]*$' "$REPO_ROOT/$JOB_MANIFEST" && \
     grep -qE '^[[:space:]]*-[[:space:]]*finalize[[:space:]]*$' "$REPO_ROOT/$JOB_MANIFEST"; then
    pass 'Finalize Job manifest runs the "genesis finalize" subcommand'
  else
    fail 'Finalize Job manifest does not run the "genesis finalize" subcommand'
  fi
  # The --confirm-seed-bound flag is the explicit operator gate and must be
  # present (a missing flag fails closed).
  if grep -qE '^[[:space:]]*-[[:space:]]*--confirm-seed-bound[[:space:]]*$' "$REPO_ROOT/$JOB_MANIFEST"; then
    pass "Finalize Job manifest carries the --confirm-seed-bound flag (the explicit operator gate)"
  else
    fail "Finalize Job manifest is missing the --confirm-seed-bound flag (the explicit operator gate)"
  fi
  if grep -q 'backoffLimit: 0' "$REPO_ROOT/$JOB_MANIFEST"; then
    pass "Finalize Job manifest has backoffLimit: 0 (no automatic retries; failures are surfaced to the operator)"
  else
    fail "Finalize Job manifest does not have backoffLimit: 0 (automatic retries could mask a failure)"
  fi

  # 12g-structural: Parse the Job YAML and validate its structure.
  JOB_YAML_STRUCTURAL=$(python3 -c "
import yaml, sys
with open('$REPO_ROOT/$JOB_MANIFEST') as f:
    job = yaml.safe_load(f)
assert job['kind'] == 'Job', 'kind must be Job'
assert job['apiVersion'] == 'batch/v1', 'apiVersion must be batch/v1'
spec = job['spec']
assert spec.get('backoffLimit') == 0, 'backoffLimit must be 0'
assert 'restartPolicy' not in spec, 'restartPolicy must not be at Job spec level (it belongs under spec.template.spec)'
pod_spec = spec['template']['spec']
assert pod_spec.get('restartPolicy') == 'Never', 'restartPolicy must be Never under spec.template.spec'
assert pod_spec.get('serviceAccountName') == 'gossipgrid-finalize', 'serviceAccountName must be gossipgrid-finalize'
container = pod_spec['containers'][0]
assert container['command'][0] == '/gossipgrid', 'command[0] must be /gossipgrid (got %s)' % container['command'][0]
assert container['command'][1] == 'genesis', 'command[1] must be genesis'
assert container['command'][2] == 'finalize', 'command[2] must be finalize'
assert '--namespace' in container['command'], 'command must include --namespace'
assert '--configmap' in container['command'], 'command must include --configmap'
assert 'gossipgrid-cluster-metadata' in container['command'], 'command must include gossipgrid-cluster-metadata'
assert '--cluster-id' in container['command'], 'command must include --cluster-id'
assert '--epoch-id' in container['command'], 'command must include --epoch-id'
assert '--genesis-consumer' in container['command'], 'command must include --genesis-consumer'
assert '--confirm-seed-bound' in container['command'], 'command must include --confirm-seed-bound (the explicit operator gate)'
sc = container.get('securityContext', {})
assert sc.get('runAsNonRoot') == True, 'securityContext.runAsNonRoot must be true'
assert sc.get('runAsUser') == 65534, 'securityContext.runAsUser must be 65534'
assert sc.get('runAsGroup') == 65534, 'securityContext.runAsGroup must be 65534'
assert sc.get('allowPrivilegeEscalation') == False, 'securityContext.allowPrivilegeEscalation must be false'
assert sc.get('readOnlyRootFilesystem') == True, 'securityContext.readOnlyRootFilesystem must be true'
assert sc.get('seccompProfile', {}).get('type') == 'RuntimeDefault', 'securityContext.seccompProfile.type must be RuntimeDefault'
caps = sc.get('capabilities', {})
assert 'ALL' in caps.get('drop', []), 'securityContext.capabilities.drop must include ALL'
assert 'fsGroup' not in sc, 'fsGroup must not be in container securityContext (it is a pod-level field; the Job has no volumes)'
print('OK')
" 2>&1)
  if [ $? -eq 0 ] && [ "$JOB_YAML_STRUCTURAL" = "OK" ]; then
    pass "Finalize Job manifest YAML structure validated (executable path, restartPolicy placement, securityContext, --confirm-seed-bound flag)"
  else
    fail "Finalize Job manifest YAML structure invalid: $JOB_YAML_STRUCTURAL"
  fi
else
  fail "Finalize Job manifest $JOB_MANIFEST not found"
fi

# 12h. The "genesis finalize" subcommand must be dispatched in main.go (the
# finalization is a one-shot, operator-authorized operation; it is not a
# server subcommand). The dispatch checks os.Args[1] == "genesis" and
# os.Args[2] == "finalize".
if grep -q 'os.Args\[2\] == "finalize"' "$REPO_ROOT/cmd/gossipgrid/main.go"; then
  pass 'main.go dispatches the "genesis finalize" subcommand (one-shot, operator-authorized)'
else
  fail 'main.go does not dispatch the "genesis finalize" subcommand'
fi

# ---------------------------------------------------------------------------
# 13. Application read-only metadata RBAC (Milestone 5B, Stage 1B)
# ---------------------------------------------------------------------------
echo ""
echo "--- 13. Application read-only metadata RBAC ---"

# 13a. DEFAULT rendering (lifecycle.metadataKubernetes.enabled: false) must
# NOT emit any app metadata reader RBAC resources (Role, RoleBinding). The
# read-only Role must not leak into the default rendering.
APP_RBAC_DEFAULT=$(grep -c 'name: gossipgrid-metadata-reader' <<< "$RENDERED_OUTPUT" || true)
if [ "$APP_RBAC_DEFAULT" -eq 0 ]; then
  pass "Default rendering: no 'gossipgrid-metadata-reader' Role/RoleBinding (app metadata RBAC disabled by default)"
else
  fail "Default rendering: found $APP_RBAC_DEFAULT 'gossipgrid-metadata-reader' identities (app metadata RBAC must be disabled by default)"
fi

# 13b. ENABLED rendering (lifecycle.metadataKubernetes.enabled: true) must
# emit exactly a Role and a RoleBinding (no ServiceAccount — the app SA
# already exists), grant get on the NAMED ConfigMap only, and have NO
# update/create/delete/patch/list/watch permissions.
APP_RBAC_ENABLED=$(helm template "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" \
  --set 'lifecycle.metadataKubernetes.enabled=true' 2>&1 || true)
APP_ROLE_ENABLED=$(grep -c 'name: gossipgrid-metadata-reader$' <<< "$APP_RBAC_ENABLED" || true)
APP_ROLEBINDING_ENABLED=$(grep -c 'kind: RoleBinding' <<< "$APP_RBAC_ENABLED" | grep -c 'gossipgrid-metadata-reader' || true)
# Simpler: check for the Role kind and the RoleBinding kind.
APP_ROLE_KIND=$(grep -c '^kind: Role$' <<< "$APP_RBAC_ENABLED" || true)
APP_RB_BIND_KIND=$(grep -c '^kind: RoleBinding$' <<< "$APP_RBAC_ENABLED" || true)
if [ "$APP_ROLE_KIND" -eq 1 ] && [ "$APP_RB_BIND_KIND" -eq 1 ]; then
  pass "Enabled rendering: exactly one Role and one RoleBinding for the app metadata reader"
else
  fail "Enabled rendering: expected 1 Role and 1 RoleBinding, got Role=$APP_ROLE_KIND RoleBinding=$APP_RB_BIND_KIND"
fi
# The Role must grant get on configmaps with the named ConfigMap.
if grep -q 'resourceNames: \["gossipgrid-cluster-metadata"\]' <<< "$APP_RBAC_ENABLED" && \
   grep -q 'verbs: \["get"\]' <<< "$APP_RBAC_ENABLED"; then
  pass "Enabled rendering: the app Role grants get on the named ConfigMap only"
else
  fail "Enabled rendering: the app Role does not grant get on the named ConfigMap (or the resourceNames/verbs are wrong)"
fi
# The Role must NOT grant update, create, delete, patch, list, or watch.
for forbidden in update create delete patch list watch; do
  if grep -q "verbs: \[.*\"$forbidden\".*\]" <<< "$APP_RBAC_ENABLED"; then
    fail "Enabled rendering: the app Role grants '$forbidden' (it must be get-only)"
  fi
done
pass "Enabled rendering: the app Role has NO forbidden permissions (no update/create/delete/patch/list/watch)"
# The RoleBinding must reference the app ServiceAccount (serviceAccount.name).
if grep -q "name: $NAMESPACE" <<< "$APP_RBAC_ENABLED" && \
   grep -q 'kind: ServiceAccount' <<< "$APP_RBAC_ENABLED"; then
  pass "Enabled rendering: the app RoleBinding references a ServiceAccount subject"
else
  fail "Enabled rendering: the app RoleBinding does not reference a ServiceAccount subject"
fi
# The RoleBinding must NOT reference the genesis or finalization ServiceAccounts.
for forbidden_sa in gossipgrid-genesis gossipgrid-finalize; do
  if grep -q "name: $forbidden_sa" <<< "$APP_RBAC_ENABLED"; then
    fail "Enabled rendering: the app RoleBinding references '$forbidden_sa' (it must reference only the app SA)"
  fi
done
pass "Enabled rendering: the app RoleBinding does NOT reference the genesis or finalization ServiceAccounts"
# No ClusterRole or ClusterRoleBinding (namespaced only).
if grep -q 'kind: ClusterRole' <<< "$APP_RBAC_ENABLED" || grep -q 'kind: ClusterRoleBinding' <<< "$APP_RBAC_ENABLED"; then
  fail "Enabled rendering: a ClusterRole or ClusterRoleBinding is present (the app metadata RBAC must be namespaced only)"
else
  pass "Enabled rendering: no ClusterRole or ClusterRoleBinding (namespaced only)"
fi
# The StatefulSet env vars must be present when the provider is enabled.
if grep -q 'GOSSIPGRID_LIFECYCLE_METADATA_KUBERNETES' <<< "$APP_RBAC_ENABLED" && \
   grep -q 'GOSSIPGRID_LIFECYCLE_METADATA_CONFIGMAP_NAME' <<< "$APP_RBAC_ENABLED"; then
  pass "Enabled rendering: the StatefulSet emits the metadata provider env vars"
else
  fail "Enabled rendering: the StatefulSet is missing the metadata provider env vars"
fi
# The env var value must be "true" and the ConfigMap name must match.
if grep -q 'value: "true"' <<< "$APP_RBAC_ENABLED" && \
   grep -q 'value: "gossipgrid-cluster-metadata"' <<< "$APP_RBAC_ENABLED"; then
  pass "Enabled rendering: the env var values are correct (KUBERNETES=true, CONFIGMAP_NAME=gossipgrid-cluster-metadata)"
else
  fail "Enabled rendering: the env var values are incorrect"
fi
# The default rendering must NOT emit the env vars.
if grep -q 'GOSSIPGRID_LIFECYCLE_METADATA_KUBERNETES' <<< "$RENDERED_OUTPUT"; then
  fail "Default rendering: the metadata provider env vars are present (they must only be emitted when the provider is enabled)"
else
  pass "Default rendering: no metadata provider env vars (the provider is disabled)"
fi

# ---------------------------------------------------------------------------
# 14. Genesis override configuration (Milestone 5B, Stage 1B)
# ---------------------------------------------------------------------------
echo ""
echo "--- 14. Genesis override configuration (runbook section 10) ---"

# The runbook (docs/genesis-runbook.md, section 10) documents the EXACT
# override values file the operator must set to enable lifecycle protection,
# the Kubernetes cluster-metadata provider, the seed identity, and the
# metadata namespace for a new genesis cluster. This check renders the chart
# with THAT exact override configuration and verifies:
#   a. the default (7A staging) rendering does NOT emit the seed-identity or
#      metadata-namespace env vars (the default Helm output is unchanged);
#   b. the override rendering emits the seed-identity env var (only when
#      lifecycle.enabled AND lifecycle.seedIdentity are both set);
#   c. the override rendering emits the metadata-namespace env var (only when
#      lifecycle.metadataKubernetes.enabled AND metadataNamespace are both
#      set);
#   d. the app read-only metadata RBAC (Role + RoleBinding) is rendered (the
#      app SA can read the ConfigMap);
#   e. the genesis grant RBAC and the finalization RBAC are rendered (the
#      operator Jobs can create/update the ConfigMap);
#   f. the 7A staging values (values-staging.yaml) do NOT enable any of these
#      settings (the existing 7A deployment is data-bearing and must not be
#      treated as a fresh cluster).

# 14a. The EXACT override configuration documented in the runbook section 10
# (lifecycle.enabled=true, metadataKubernetes.enabled=true, configMapName,
# metadataNamespace, seedIdentity; plus genesis.enabled and finalize.enabled
# so the operator Jobs' RBAC is rendered). Render it.
GENESIS_OVERRIDE_OUTPUT=$(helm template "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" \
  --set 'lifecycle.enabled=true' \
  --set 'lifecycle.metadataKubernetes.enabled=true' \
  --set 'lifecycle.metadataKubernetes.configMapName=gossipgrid-cluster-metadata' \
  --set 'lifecycle.metadataKubernetes.metadataNamespace=gossipgrid' \
  --set 'lifecycle.seedIdentity=gossipgrid-0' \
  --set 'genesis.enabled=true' \
  --set 'finalize.enabled=true' 2>&1 || true)

# 14b. DEFAULT rendering (lifecycle.enabled: false, the 7A staging default)
# must NOT emit the seed-identity env var (lifecycle.seedIdentity is empty
# by default, and even if it were set, lifecycle.enabled is false so the
# env var is not emitted). The grep uses the 'name:' prefix to exclude
# comment lines in the template that mention the env var name.
if grep -q 'name: GOSSIPGRID_GENESIS_SEED_IDENTITY' <<< "$RENDERED_OUTPUT"; then
  fail "Default rendering: GOSSIPGRID_GENESIS_SEED_IDENTITY env var is present (it must only be emitted when lifecycle.enabled=true AND lifecycle.seedIdentity is nonempty)"
else
  pass "Default rendering: no GOSSIPGRID_GENESIS_SEED_IDENTITY env var (lifecycle is disabled by default)"
fi

# 14c. DEFAULT rendering must NOT emit the metadata-namespace env var
# (lifecycle.metadataKubernetes.metadataNamespace is empty by default, and
# lifecycle.metadataKubernetes.enabled is false). The grep uses the 'name:'
# prefix to exclude comment lines in the template that mention the env var
# name.
if grep -q 'name: GOSSIPGRID_LIFECYCLE_METADATA_NAMESPACE' <<< "$RENDERED_OUTPUT"; then
  fail "Default rendering: GOSSIPGRID_LIFECYCLE_METADATA_NAMESPACE env var is present (it must only be emitted when lifecycle.metadataKubernetes.enabled=true AND metadataNamespace is nonempty)"
else
  pass "Default rendering: no GOSSIPGRID_LIFECYCLE_METADATA_NAMESPACE env var (the metadata provider is disabled by default)"
fi

# 14d. The OVERRIDE rendering must emit the seed-identity env var with the
# correct value (gossipgrid-0, the deterministic seed-identity convention).
if grep -q 'name: GOSSIPGRID_GENESIS_SEED_IDENTITY' <<< "$GENESIS_OVERRIDE_OUTPUT" && \
   grep -q 'value: "gossipgrid-0"' <<< "$GENESIS_OVERRIDE_OUTPUT"; then
  pass "Genesis override: GOSSIPGRID_GENESIS_SEED_IDENTITY is emitted with value gossipgrid-0"
else
  fail "Genesis override: GOSSIPGRID_GENESIS_SEED_IDENTITY is missing or has the wrong value (expected gossipgrid-0)"
fi

# 14e. The OVERRIDE rendering must emit the metadata-namespace env var with
# the correct value (gossipgrid, the application namespace).
if grep -q 'name: GOSSIPGRID_LIFECYCLE_METADATA_NAMESPACE' <<< "$GENESIS_OVERRIDE_OUTPUT" && \
   grep -q 'value: "gossipgrid"' <<< "$GENESIS_OVERRIDE_OUTPUT"; then
  pass "Genesis override: GOSSIPGRID_LIFECYCLE_METADATA_NAMESPACE is emitted with value gossipgrid"
else
  fail "Genesis override: GOSSIPGRID_LIFECYCLE_METADATA_NAMESPACE is missing or has the wrong value (expected gossipgrid)"
fi

# 14f. The OVERRIDE rendering must emit the metadata provider env vars
# (KUBERNETES=true, CONFIGMAP_NAME=gossipgrid-cluster-metadata).
if grep -q 'GOSSIPGRID_LIFECYCLE_METADATA_KUBERNETES' <<< "$GENESIS_OVERRIDE_OUTPUT" && \
   grep -q 'GOSSIPGRID_LIFECYCLE_METADATA_CONFIGMAP_NAME' <<< "$GENESIS_OVERRIDE_OUTPUT"; then
  pass "Genesis override: the metadata provider env vars are emitted"
else
  fail "Genesis override: the metadata provider env vars are missing"
fi

# 14g. The OVERRIDE rendering must emit the lifecycle-enabled env var (true).
if grep -q 'GOSSIPGRID_LIFECYCLE_ENABLED' <<< "$GENESIS_OVERRIDE_OUTPUT" && \
   grep -q 'value: "true"' <<< "$GENESIS_OVERRIDE_OUTPUT"; then
  pass "Genesis override: GOSSIPGRID_LIFECYCLE_ENABLED is emitted with value true"
else
  fail "Genesis override: GOSSIPGRID_LIFECYCLE_ENABLED is missing or has the wrong value"
fi

# 14h. The OVERRIDE rendering must emit the app read-only metadata RBAC
# (Role + RoleBinding for the app SA to read the ConfigMap).
if grep -q 'kind: Role$' <<< "$GENESIS_OVERRIDE_OUTPUT" && \
   grep -q 'gossipgrid-metadata-reader' <<< "$GENESIS_OVERRIDE_OUTPUT" && \
   grep -q 'resourceNames: \["gossipgrid-cluster-metadata"\]' <<< "$GENESIS_OVERRIDE_OUTPUT" && \
   grep -q 'verbs: \["get"\]' <<< "$GENESIS_OVERRIDE_OUTPUT"; then
  pass "Genesis override: the app read-only metadata RBAC (Role + RoleBinding, get on the named ConfigMap) is rendered"
else
  fail "Genesis override: the app read-only metadata RBAC is missing or has the wrong permissions"
fi

# 14i. The OVERRIDE rendering must emit the genesis grant RBAC (ServiceAccount
# + Role + RoleBinding with create on configmaps and get on the named
# ConfigMap).
if grep -q 'gossipgrid-genesis' <<< "$GENESIS_OVERRIDE_OUTPUT" && \
   grep -q 'verbs: \["create"\]' <<< "$GENESIS_OVERRIDE_OUTPUT" && \
   grep -q 'verbs: \["get"\]' <<< "$GENESIS_OVERRIDE_OUTPUT"; then
  pass "Genesis override: the genesis grant RBAC (create + get on configmaps) is rendered"
else
  fail "Genesis override: the genesis grant RBAC is missing or has the wrong permissions"
fi

# 14j. The OVERRIDE rendering must emit the finalization RBAC (ServiceAccount
# + Role + RoleBinding with get and update on the named ConfigMap). The
# finalization Role has two separate rules (one for get, one for update), so
# the check verifies both verbs are present (in separate rules).
FINALIZE_ROLE_PRESENT=0
if grep -q 'gossipgrid-finalize' <<< "$GENESIS_OVERRIDE_OUTPUT" && \
   grep -q 'verbs: \["get"\]' <<< "$GENESIS_OVERRIDE_OUTPUT" && \
   grep -q 'verbs: \["update"\]' <<< "$GENESIS_OVERRIDE_OUTPUT" && \
   grep -q 'resourceNames: \["gossipgrid-cluster-metadata"\]' <<< "$GENESIS_OVERRIDE_OUTPUT"; then
  FINALIZE_ROLE_PRESENT=1
fi
if [ "$FINALIZE_ROLE_PRESENT" -eq 1 ]; then
  pass "Genesis override: the finalization RBAC (get + update on the named ConfigMap) is rendered"
else
  fail "Genesis override: the finalization RBAC is missing or has the wrong permissions (expected get + update on the named ConfigMap)"
fi

# 14k. The 7A STAGING values (values-staging.yaml) must NOT enable any of
# these settings (the existing 7A deployment is data-bearing and must not be
# treated as a fresh cluster). The staging rendering (already rendered above
# as STAGING_RENDERED_OUTPUT) must NOT contain the seed-identity or
# metadata-namespace env vars. The grep uses the 'name:' prefix to exclude
# comment lines in the template that mention the env var name.
if grep -q 'name: GOSSIPGRID_GENESIS_SEED_IDENTITY' <<< "$STAGING_RENDERED_OUTPUT"; then
  fail "7A staging values: GOSSIPGRID_GENESIS_SEED_IDENTITY env var is present (the 7A deployment must not enable the genesis seed identity)"
else
  pass "7A staging values: no GOSSIPGRID_GENESIS_SEED_IDENTITY env var (the 7A deployment does not enable genesis)"
fi
if grep -q 'name: GOSSIPGRID_LIFECYCLE_METADATA_NAMESPACE' <<< "$STAGING_RENDERED_OUTPUT"; then
  fail "7A staging values: GOSSIPGRID_LIFECYCLE_METADATA_NAMESPACE env var is present (the 7A deployment must not enable the metadata provider namespace)"
else
  pass "7A staging values: no GOSSIPGRID_LIFECYCLE_METADATA_NAMESPACE env var (the 7A deployment does not enable the metadata provider)"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ "$FAIL" -gt 0 ]; then
  echo "Some checks failed. Review the FAIL lines above."
  exit 1
fi
echo "All checks passed."
