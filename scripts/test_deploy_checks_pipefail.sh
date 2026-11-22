#!/usr/bin/env bash
# Regression test: deploy_checks.sh patterns are safe under bash pipefail.
#
# The original script used `echo "$RENDERED_OUTPUT" | grep -q pattern` which
# triggers SIGPIPE (exit 141) under `set -o pipefail` when grep -q exits early
# and the upstream echo (writing a multi-MB helm template output) receives a
# SIGPIPE. This test proves the corrected here-string / process-substitution
# patterns do not produce exit code 141, even with a large input string.
#
# Usage: bash scripts/test_deploy_checks_pipefail.sh
# Fails (exit 1) when any pattern produces SIGPIPE or an unexpected exit code.

set -euo pipefail

PASS=0
FAIL=0

pass() { echo "  PASS: $1"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

# --- Build a large test string (~5 MB, similar to a full helm template output) ---
# Simulates the rendered Kubernetes manifests: many YAML documents with realistic
# field names that the deploy_checks.sh patterns look for.
LARGE_OUTPUT=""
for i in $(seq 1 2000); do
  LARGE_OUTPUT+="apiVersion: v1
kind: Service
metadata:
  name: gossipgrid-${i}
  namespace: gossipgrid
spec:
  clusterIP: None
  ports:
    - port: 8443
      name: peer
  selector:
    app: gossipgrid
---
apiVersion: v1
kind: Service
metadata:
  name: web
  namespace: gossipgrid
spec:
  type: ClusterIP
  ports:
    - port: 8080
      name: web
  selector:
    app: gossipgrid
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: gossipgrid-${i}
  namespace: gossipgrid
spec:
  serviceName: gossipgrid
  replicas: ${i}
  template:
    spec:
      containers:
        - name: gossipgrid
          ports:
            - containerPort: 8443
          volumeMounts:
            - name: peer-ca-bundle
              mountPath: /etc/gossipgrid/ca
              readOnly: true
            - name: credentials
              mountPath: /etc/gossipgrid/credentials
              readOnly: true
          env:
            - name: GOSSIPGRID_PEER_IDENTITY_SUFFIX
              value: \"gossipgrid.gossipgrid.svc.cluster.local\"
            - name: GOSSIPGRID_DISCOVERY_DNS_NAME
              value: \"gossipgrid.gossipgrid.svc.cluster.local\"
            - name: GOSSIPGRID_PEER_TRUST_BUNDLE_FILE
              value: \"/etc/gossipgrid/ca/ca-bundle.pem\"
      volumes:
        - name: peer-ca-bundle
          secret:
            secretName: gossipgrid-ca-bundle
        - name: credentials
          csi:
            driver: csi.cert-manager.io
            volumeAttributes:
              csi.cert-manager.io/issuer-name: gossipgrid-peer-issuer
              csi.cert-manager.io/dns-names: \${POD_NAME}.gossipgrid.gossipgrid.svc.cluster.local
              csi.cert-manager.io/key-algorithm: ECDSA
              csi.cert-manager.io/key-usages:
                - server auth
                - client auth
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: gossipgrid-netpol-${i}
  namespace: gossipgrid
spec:
  podSelector:
    matchLabels:
      app: gossipgrid
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: cilium
      ports:
        - port: 8080
    - from:
        - podSelector:
            matchLabels:
              app: gossipgrid
      ports:
        - port: 8443
  egress:
    - to:
        - ipBlock:
            cidr: 10.0.0.0/8
      ports:
        - port: 8443
"
done

SIZE_KB=$(( $(printf '%s' "$LARGE_OUTPUT" | wc -c) / 1024 ))
echo "Test string size: ~${SIZE_KB} KB"
echo ""

# --- Test 1: here-string grep -q (the fix for the original SIGPIPE bug) ---------
echo "--- 1. grep -q with here-string on large output ---"
rc=0
grep -q 'csi\.cert-manager\.io/dns-names:' <<< "$LARGE_OUTPUT" || rc=$?
if [ $rc -eq 0 ]; then
  pass "grep -q here-string: found pattern, no SIGPIPE"
else
  fail "grep -q here-string: unexpected exit code $rc"
fi

rc=0
grep -q 'pattern-that-does-not-exist-xyz' <<< "$LARGE_OUTPUT" || rc=$?
if [ $rc -eq 1 ]; then
  pass "grep -q here-string: not-found returns 1 (not 141)"
else
  fail "grep -q here-string: not-found returned $rc (expected 1)"
fi

# --- Test 2: here-string grep -q with sed section extraction -------------------
echo ""
echo "--- 2. grep -q with here-string on sed-extracted section ---"
SECTION=$(sed -n '/^kind: NetworkPolicy$/,/^---/p' <<< "$LARGE_OUTPUT")
rc=0
grep -q 'cidr:' <<< "$SECTION" || rc=$?
if [ $rc -eq 0 ]; then
  pass "sed section + grep -q here-string: found pattern"
else
  fail "sed section + grep -q here-string: unexpected exit code $rc"
fi

# --- Test 3: grep -c with here-string (the fix for `grep -c || true`) -----------
echo ""
echo "--- 3. grep -c with here-string ---"
COUNT=$(grep -c 'port: 8443' <<< "$LARGE_OUTPUT" || true)
if [ -n "$COUNT" ] && [ "$COUNT" -gt 0 ]; then
  pass "grep -c here-string: counted $COUNT occurrences, no SIGPIPE"
else
  fail "grep -c here-string: got '$COUNT' (expected >0)"
fi

# --- Test 4: process substitution for chained grep ------------------------------
echo ""
echo "--- 4. grep -q with process substitution (key-usages check) ---"
KEY_USAGES=$(grep -A2 'csi\.cert-manager\.io/key-usages:' <<< "$LARGE_OUTPUT")
rc=0
grep -q 'server auth' <<< "$KEY_USAGES" || rc=$?
if [ $rc -eq 0 ]; then
  pass "grep -A2 + here-string grep -q: found 'server auth'"
else
  fail "grep -A2 + here-string grep -q: unexpected exit code $rc"
fi

# --- Test 5: demonstrate the OLD pattern would SIGPIPE under pipefail -----------
# (This is informational — it confirms the bug existed. We do NOT assert this
#  fails, because on some systems/versions the SIGPIPE may not trigger for
#  small-enough buffers. We print the result for the report.)
echo ""
echo "--- 5. Old pattern (echo | grep -q) under pipefail [informational] ---"
rc=0
echo "$LARGE_OUTPUT" | grep -q 'csi\.cert-manager\.io/dns-names:' || rc=$?
if [ $rc -eq 141 ]; then
  echo "  CONFIRMED: old pattern produced SIGPIPE (exit 141) — the bug is real"
  pass "Old pattern reproduced the SIGPIPE bug (exit 141)"
elif [ $rc -eq 0 ]; then
  echo "  NOTE: old pattern did not SIGPIPE on this system (buffer was small enough)"
  pass "Old pattern did not SIGPIPE on this system (no regression to detect)"
else
  echo "  NOTE: old pattern returned exit $rc"
  pass "Old pattern returned exit $rc (not 141)"
fi

# --- Test 6: single awk pass (StatefulSet serviceName extraction) ----------------
# The deploy_checks.sh replacement for the old grep | head -1 | awk pipeline:
# a single awk program that reads the full input and prints the first
# StatefulSet's serviceName. No head -1, no SIGPIPE risk.
echo ""
echo "--- 6. single awk pass (StatefulSet serviceName pattern) ---"
RC=0
SVC_NAME=$(awk '
  /^kind: StatefulSet$/ {insts=1}
  insts && /^---/ {insts=0}
  insts && /^  serviceName: / {print $2; exit}
' <<< "$LARGE_OUTPUT") || RC=$?
if [ $RC -eq 0 ]; then
  pass "single awk pass (StatefulSet): no SIGPIPE (rc=0), name=$SVC_NAME"
else
  fail "single awk pass (StatefulSet): unexpected exit code $RC"
fi

# --- Test 6b: demonstrate the OLD grep | head -1 | awk would SIGPIPE -----------
# The old pattern: grep ... <<< "$LARGE" | head -1 | awk ...
# head -1 reads one line and closes; grep receives SIGPIPE if still writing.
echo ""
echo "--- 6b. Old grep | head -1 | awk pattern [informational] ---"
RC=0
OLD_SVC=$(grep '^  serviceName:' <<< "$LARGE_OUTPUT" | head -1 | awk '{print $2}') || RC=$?
if [ $RC -eq 141 ]; then
  echo "  CONFIRMED: old grep | head -1 | awk produced SIGPIPE (exit 141)"
  pass "Old grep | head -1 | awk pattern reproduced SIGPIPE (exit 141)"
elif [ $RC -eq 0 ]; then
  echo "  NOTE: old pattern did not SIGPIPE on this system (buffer small enough)"
  pass "Old grep | head -1 | awk pattern did not SIGPIPE on this system"
else
  echo "  NOTE: old pattern returned exit $RC"
  pass "Old grep | head -1 | awk pattern returned exit $RC (not 141)"
fi

# --- Test 7: grep -A1 | grep | awk (env var value extraction pattern) ------------
# The deploy_checks.sh pattern: grep KEY -A1 <<< "$LARGE" | grep 'value:' | awk ...
echo ""
echo "--- 7. grep -A1 | grep | awk (env var value pattern) ---"
RC=0
VAL=$(grep 'PEER_IDENTITY_SUFFIX' -A1 <<< "$LARGE_OUTPUT" | grep 'value:' | awk -F'"' '{print $2}') || RC=$?
if [ $RC -eq 0 ]; then
  pass "grep -A1 | grep | awk: no SIGPIPE (rc=0)"
else
  fail "grep -A1 | grep | awk: unexpected exit code $RC"
fi

# --- Test 8: sed -n range | grep -v (section extraction with comment filtering) --
# The deploy_checks.sh pattern: sed -n '/start/,/end/p' <<< "$LARGE" | grep -v '^\s*#'
echo ""
echo "--- 8. sed -n range | grep -v (section + comment filter pattern) ---"
RC=0
SECTION=$(sed -n '/^kind: Service$/,/^---/p' <<< "$LARGE_OUTPUT" | grep -v '^\s*#') || RC=$?
if [ $RC -eq 0 ]; then
  pass "sed -n range | grep -v: no SIGPIPE (rc=0)"
else
  fail "sed -n range | grep -v: unexpected exit code $RC"
fi

# --- Test 9: sed -n range | grep -A (section extraction with context lines) ------
# The deploy_checks.sh pattern: sed -n '/^kind: Service$/,/^---/p' <<< "$LARGE" | grep -A 20 'name: web'
# sed reads all input (no early exit), grep -A reads all input too — no SIGPIPE.
# If the pattern is not found, grep -A exits 1, which is correct under pipefail.
echo ""
echo "--- 9. sed -n range | grep -A (section + context pattern) ---"
RC=0
SECTION=$(sed -n '/^kind: Service$/,/^---/p' <<< "$LARGE_OUTPUT" | grep -A 20 'name: web') || RC=$?
if [ $RC -eq 0 ] || [ $RC -eq 1 ]; then
  pass "sed -n range | grep -A: no SIGPIPE (rc=$RC)"
else
  fail "sed -n range | grep -A: unexpected exit code $RC"
fi

# --- Test 10: single awk pass (headless Service name extraction pattern) ----------
# The deploy_checks.sh replacement for the old multi-pipeline pattern:
# a single awk program that reads the full input and prints the Service name.
echo ""
echo "--- 10. single awk pass (headless Service name pattern) ---"
RC=0
NAME=$(awk '
  /^kind: Service$/ {insvc=1}
  insvc && /^---/ {insvc=0}
  insvc && /^  name: / {name=$2}
  insvc && /clusterIP: None/ {if (name != "") {print name; exit}}
' <<< "$LARGE_OUTPUT") || RC=$?
if [ $RC -eq 0 ]; then
  pass "single awk pass: no SIGPIPE (rc=0), name=$NAME"
else
  fail "single awk pass: unexpected exit code $RC"
fi

# --- Test 11: demonstrate the OLD echo | grep -A1 | grep | awk would SIGPIPE -----
# (Informational: the old pattern used echo "$VAR" | grep ... on the full
#  multi-MB string. We demonstrate the hazard with the old echo-based form.)
echo ""
echo "--- 11. Old echo | grep -A1 | grep | awk pattern [informational] ---"
RC=0
echo "$LARGE_OUTPUT" | grep 'PEER_IDENTITY_SUFFIX' -A1 | grep 'value:' | awk -F'"' '{print $2}' >/dev/null || RC=$?
if [ $RC -eq 141 ]; then
  echo "  CONFIRMED: old echo | grep -A1 pattern produced SIGPIPE (exit 141)"
  pass "Old echo | grep -A1 pattern reproduced SIGPIPE (exit 141)"
elif [ $RC -eq 0 ]; then
  echo "  NOTE: old pattern did not SIGPIPE on this system (buffer small enough)"
  pass "Old echo | grep -A1 pattern did not SIGPIPE on this system"
else
  echo "  NOTE: old pattern returned exit $RC"
  pass "Old echo | grep -A1 pattern returned exit $RC (not 141)"
fi

# --- Summary --------------------------------------------------------------------
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ "$FAIL" -gt 0 ]; then
  echo "Regression test FAILED: pipefail safety patterns are broken."
  exit 1
fi
echo "All pipefail safety regression tests passed."
