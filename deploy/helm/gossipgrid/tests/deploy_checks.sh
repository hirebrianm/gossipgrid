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
SVC_NAME=$(echo "$RENDERED_OUTPUT" | grep '^  serviceName:' | head -1 | awk '{print $2}')
if [ -z "$SVC_NAME" ]; then
  fail "Could not find serviceName in rendered StatefulSet"
else
  pass "StatefulSet serviceName: $SVC_NAME"
fi

# The headless Service name (from the rendered Service resource).
HEADLESS_NAME=$(echo "$RENDERED_OUTPUT" | sed -n '/^kind: Service$/,/^---/p' | grep 'clusterIP: None' -B 20 | grep '^  name:' | head -1 | awk '{print $2}')
if [ -z "$HEADLESS_NAME" ]; then
  # Try a different approach: find the Service with clusterIP: None
  HEADLESS_NAME=$(echo "$RENDERED_OUTPUT" | awk '/^kind: Service$/,/^---/' | grep -B 30 'clusterIP: None' | grep '^  name:' | head -1 | awk '{print $2}')
fi
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
PEER_SUFFIX=$(echo "$RENDERED_OUTPUT" | grep 'PEER_IDENTITY_SUFFIX' -A1 | grep 'value:' | awk -F'"' '{print $2}')
if [ -z "$PEER_SUFFIX" ]; then
  fail "Could not find GOSSIPGRID_PEER_IDENTITY_SUFFIX in rendered StatefulSet"
else
  # Verify the suffix ends with .<namespace>.svc.cluster.local
  if echo "$PEER_SUFFIX" | grep -q "\.${NAMESPACE}\.svc\.cluster\.local$"; then
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
DISCOVERY_NAME=$(echo "$RENDERED_OUTPUT" | grep 'DISCOVERY_DNS_NAME' -A1 | grep 'value:' | awk -F'"' '{print $2}')
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
CSI_DRIVER=$(echo "$RENDERED_OUTPUT" | grep 'driver: csi\.cert-manager\.io' | head -1)
if [ -n "$CSI_DRIVER" ]; then
  pass "CSI driver is csi.cert-manager.io"
else
  fail "CSI driver csi.cert-manager.io not found in rendered StatefulSet"
fi

# The stale, wrong driver name must NOT be present.
if echo "$RENDERED_OUTPUT" | grep -q 'cert-manager\.csi\.dev'; then
  fail "Stale driver name cert-manager.csi.dev present in rendered StatefulSet"
else
  pass "No stale driver name cert-manager.csi.dev in rendered StatefulSet"
fi

# The volumeAttributes must use the csi.cert-manager.io/ prefix.
if echo "$RENDERED_OUTPUT" | grep -q 'csi\.cert-manager\.io/issuer-name:'; then
  pass "volumeAttributes use csi.cert-manager.io/issuer-name"
else
  fail "csi.cert-manager.io/issuer-name not found in volumeAttributes"
fi

if echo "$RENDERED_OUTPUT" | grep -q 'csi\.cert-manager\.io/dns-names:'; then
  pass "volumeAttributes include csi.cert-manager.io/dns-names"
else
  fail "csi.cert-manager.io/dns-names not found in volumeAttributes"
fi

# The dns-names must use ${POD_NAME} (per-pod substitution).
DNS_NAMES_VALUE=$(echo "$RENDERED_OUTPUT" | grep 'csi\.cert-manager\.io/dns-names:' | awk -F': ' '{print $2}')
if echo "$DNS_NAMES_VALUE" | grep -q '^\${POD_NAME}\.'; then
  pass "dns-names uses \${POD_NAME} per-pod substitution: $DNS_NAMES_VALUE"
else
  fail "dns-names does not use \${POD_NAME} prefix: $DNS_NAMES_VALUE"
fi

# The dns-names must include the peerServiceName, namespace, and cluster domain.
if echo "$DNS_NAMES_VALUE" | grep -q "\.${HEADLESS_NAME}\.${NAMESPACE}\.svc\.cluster\.local$"; then
  pass "dns-names SAN matches the headless Service FQDN suffix"
else
  fail "dns-names SAN does not match headless Service FQDN: $DNS_NAMES_VALUE"
fi

# The key algorithm must be ECDSA.
if echo "$RENDERED_OUTPUT" | grep -q 'csi\.cert-manager\.io/key-algorithm: ECDSA'; then
  pass "Key algorithm is ECDSA (FIPS 140-3 compatible)"
else
  fail "Key algorithm is not ECDSA"
fi

# The key usages must include both server auth and client auth.
if echo "$RENDERED_OUTPUT" | grep 'csi\.cert-manager\.io/key-usages:' | grep -q 'server auth' && \
   echo "$RENDERED_OUTPUT" | grep 'csi\.cert-manager\.io/key-usages:' | grep -q 'client auth'; then
  pass "Key usages include serverAuth and clientAuth"
else
  fail "Key usages do not include both serverAuth and clientAuth"
fi

# The Certificate resource for peer mTLS must NOT be rendered (CSI driver issues
# directly). The browser Certificate is rendered by
# certmanager-browser-certificate.yaml, so there should be exactly 1 Certificate
# resource (the browser one), not 2.
CERT_RESOURCES=$(echo "$RENDERED_OUTPUT" | grep -c '^kind: Certificate$' || true)
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
if echo "$RENDERED_OUTPUT" | sed -n '/peer-ca-bundle/,/readOnly/p' | grep -q 'subPath:'; then
  fail "Trust bundle volumeMount has subPath (blocks Secret rotation)"
else
  pass "Trust bundle volumeMount has no subPath (rotation is honored)"
fi

# The trust bundle mount path must be a directory (not a file path).
TRUST_MOUNT=$(echo "$RENDERED_OUTPUT" | sed -n '/peer-ca-bundle/,/readOnly/p' | grep 'mountPath:' | awk '{print $2}')
if [ -n "$TRUST_MOUNT" ] && ! echo "$TRUST_MOUNT" | grep -q '\.pem$'; then
  pass "Trust bundle mountPath is a directory: $TRUST_MOUNT"
else
  fail "Trust bundle mountPath is not a directory: $TRUST_MOUNT"
fi

# The GOSSIPGRID_PEER_TRUST_BUNDLE_FILE env var must be the full file path
# (mount directory + secret key), not just the directory.
TRUST_BUNDLE_FILE=$(echo "$RENDERED_OUTPUT" | grep 'PEER_TRUST_BUNDLE_FILE' -A1 | grep 'value:' | awk -F'"' '{print $2}')
if [ -n "$TRUST_BUNDLE_FILE" ] && echo "$TRUST_BUNDLE_FILE" | grep -q '\.pem$'; then
  pass "GOSSIPGRID_PEER_TRUST_BUNDLE_FILE is the full file path: $TRUST_BUNDLE_FILE"
else
  fail "GOSSIPGRID_PEER_TRUST_BUNDLE_FILE is not a file path: $TRUST_BUNDLE_FILE"
fi

# The credentials volumeMount must NOT have subPath.
# Check only the YAML structure lines (not comments) by looking for the
# "subPath:" key followed by a value (not a comment).
if echo "$RENDERED_OUTPUT" | sed -n '/- name: credentials/,/readOnly/p' | grep -v '^\s*#' | grep -q '^\s*subPath:'; then
  fail "Credentials volumeMount has subPath (blocks Secret rotation)"
else
  pass "Credentials volumeMount has no subPath (rotation is honored)"
fi

# The credentials mount path must be a directory.
CRED_MOUNT=$(echo "$RENDERED_OUTPUT" | sed -n '/name: credentials/,/readOnly/p' | grep 'mountPath:' | awk '{print $2}')
if [ -n "$CRED_MOUNT" ] && ! echo "$CRED_MOUNT" | grep -q '\.json$'; then
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
if echo "$RENDERED_OUTPUT" | sed -n '/^kind: HTTPRoute$/,/^---/p' | grep -q "$PEER_PORT_NUM"; then
  fail "Peer port ($PEER_PORT_NUM) is exposed in HTTPRoute (should be peer-only)"
else
  pass "Peer port is not exposed in HTTPRoute"
fi

# The peer port must NOT be in the web Service.
if echo "$RENDERED_OUTPUT" | sed -n '/^kind: Service$/,/^---/p' | grep 'name: web' -A 20 | grep -q '8443'; then
  fail "Peer port (8443) is exposed in the web Service"
else
  pass "Peer port is not exposed in the web Service"
fi

# The peer port must ONLY be in the headless Service (for DNS SRV discovery).
if echo "$RENDERED_OUTPUT" | grep -q "containerPort: $PEER_PORT_NUM"; then
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
if echo "$RENDERED_OUTPUT" | sed -n '/^kind: NetworkPolicy$/,/^---/p' | grep -v '^\s*#' | grep -q 'cidr: 0\.0\.0\.0/0'; then
  fail "NetworkPolicy contains cidr: 0.0.0.0/0 in YAML (unrestricted ingress)"
else
  pass "NetworkPolicy has no 0.0.0.0/0 cidr in YAML structure"
fi

# The web port ingress must only be from the Gateway namespace and optional
# CIDRs. Count the ingress rules that target the web port.
WEB_PORT_RULES=$(echo "$RENDERED_OUTPUT" | sed -n '/^kind: NetworkPolicy$/,/^---/p' | grep -c "port: 8080" || true)
if [ "$WEB_PORT_RULES" -le 2 ]; then
  pass "Web port ingress rules: $WEB_PORT_RULES (Gateway + optional CIDR only)"
else
  fail "Web port has $WEB_PORT_RULES ingress rules (expected max 2: Gateway + CIDR)"
fi

# The peer port ingress must only be from GossipGrid pods.
PEER_PORT_RULES=$(echo "$RENDERED_OUTPUT" | sed -n '/^kind: NetworkPolicy$/,/^---/p' | grep -c "port: 8443" || true)
if [ "$PEER_PORT_RULES" -le 2 ]; then
  pass "Peer port ingress rules: $PEER_PORT_RULES (GossipGrid pods + egress only)"
else
  fail "Peer port has $PEER_PORT_RULES rules (expected max 2: ingress + egress)"
fi

# No ipBlock with 0.0.0.0/0 in the NetworkPolicy (at all).
if echo "$RENDERED_OUTPUT" | sed -n '/^kind: NetworkPolicy$/,/^---/p' | grep -q 'ipBlock:' ; then
  # Check if any ipBlock is 0.0.0.0/0
  if echo "$RENDERED_OUTPUT" | sed -n '/^kind: NetworkPolicy$/,/^---/p' | grep -A1 'ipBlock:' | grep -q '0\.0\.0\.0/0'; then
    fail "NetworkPolicy has ipBlock 0.0.0.0/0"
  else
    pass "NetworkPolicy ipBlocks are specific CIDRs (not 0.0.0.0/0)"
  fi
else
  pass "NetworkPolicy has no ipBlock rules (Gateway namespace selector only)"
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
