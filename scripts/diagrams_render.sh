#!/usr/bin/env bash
# diagrams_render.sh — LEVEL 2 (render validation) of the Mermaid validation
# pipeline. It renders every canonical .mmd source using the pinned Mermaid CLI
# (mmdc) to SVG and PNG in a TEMPORARY directory, and verifies:
#
#   1. mmdc exits 0 (no syntax or rendering error reported).
#   2. The generated SVG and PNG exist.
#   3. The outputs are non-empty (an empty SVG/PNG is an obviously invalid
#      output — a render error that mmdc did not report).
#
# The temporary directory is created with mktemp, is removed on every exit
# path (success, failure, or interrupt via a trap), and is never written into
# the version-controlled docs/diagrams/generated/ tree. The committed generated
# outputs are produced only by `make architecture` (see the Makefile).
#
# Usage:
#   scripts/diagrams_render.sh <base1> [base2 ...]
#
# The argument list is the set of diagram bases (matching the Makefile's
# DIAGRAM_BASES variable). The script is POSIX-compatible (uses only npx,
# mktemp, wc, printf, cat) so it runs under both bash and /bin/sh (macOS).
# It exits non-zero on the first failure and prints the offending line.

set -u

DIAGRAM_DIR="docs/diagrams"
MERMAID_CLI_VERSION="11.4.1"

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <base1> [base2 ...]" >&2
  exit 2
fi

# Render into a temporary directory (created with mktemp) so that this
# validation step is read-only with respect to the repository. The trap
# guarantees the directory is removed on every exit path, including an
# interrupted run, so no temporary files are left behind.
TMP_GEN=$(mktemp -d)
cleanup() { rm -rf "$TMP_GEN"; }
trap cleanup EXIT

status=0
for base in "$@"; do
  mmd="$DIAGRAM_DIR/$base.mmd"
  svg="$TMP_GEN/$base.svg"
  png="$TMP_GEN/$base.png"
  if [ ! -f "$mmd" ]; then
    echo "FAIL: $mmd not found"
    status=1
    continue
  fi
  echo "Rendering $mmd (mmdc v$MERMAID_CLI_VERSION)..."

  # Render SVG.
  cfg=$(mktemp)
  printf '{"args":["--no-sandbox","--disable-setuid-sandbox"]}' > "$cfg"
  npx --yes @mermaid-js/mermaid-cli@"$MERMAID_CLI_VERSION" \
    -i "$mmd" -o "$svg" \
    -c "$DIAGRAM_DIR/mermaid-config.json" -p "$cfg" >/dev/null 2>&1
  rc=$?
  rm -f "$cfg"
  if [ $rc -ne 0 ]; then
    echo "FAIL: SVG render failed for $base (rc=$rc)"
    status=1
    continue
  fi

  # Render PNG.
  cfg=$(mktemp)
  printf '{"args":["--no-sandbox","--disable-setuid-sandbox"]}' > "$cfg"
  npx --yes @mermaid-js/mermaid-cli@"$MERMAID_CLI_VERSION" \
    -i "$mmd" -o "$png" \
    -c "$DIAGRAM_DIR/mermaid-config.json" -b white -w 1600 -p "$cfg" >/dev/null 2>&1
  rc=$?
  rm -f "$cfg"
  if [ $rc -ne 0 ]; then
    echo "FAIL: PNG render failed for $base (rc=$rc)"
    status=1
    continue
  fi

  # Verify the outputs exist and are non-empty (an empty SVG/PNG is an
  # obviously invalid output — a render error that mmdc did not report).
  if [ ! -s "$svg" ]; then
    echo "FAIL: $svg is missing or empty"
    status=1
    continue
  fi
  if [ ! -s "$png" ]; then
    echo "FAIL: $png is missing or empty"
    status=1
    continue
  fi
  svg_bytes=$(wc -c < "$svg" | tr -d ' ')
  png_bytes=$(wc -c < "$png" | tr -d ' ')
  echo "  OK: $svg ($svg_bytes bytes), $png ($png_bytes bytes)"
done

if [ "$status" -eq 0 ]; then
  echo "All diagrams rendered successfully (level 2: render)."
else
  echo "diagrams-render (level 2): failures detected (see FAIL lines above)." >&2
fi
exit "$status"
