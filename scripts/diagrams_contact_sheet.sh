#!/usr/bin/env bash
# diagrams_contact_sheet.sh — LEVEL 3 (visual review) of the Mermaid validation
# pipeline. It generates a temporary contact sheet (an HTML file with a grid of
# all diagram PNGs) for manual visual inspection. The contact sheet is written
# to the system temp directory (OUTSIDE version-controlled documentation) and
# the operator opens it in a browser to look at each rendered diagram.
#
# The operator is expected to actually inspect the rendered diagrams for:
#   - clipped labels,
#   - overlapping nodes,
#   - incorrectly interpreted note directives,
#   - unreadable text,
#   - excessively small fonts,
#   - broken infrastructure boundaries,
#   - unintended layout changes.
#
# This step is a convenience, NOT a CI gate (the automated checks are levels
# 1 and 2). If a visual reviewer (human or vision model) is not available,
# this step does NOT claim the diagrams are visually correct — it only
# generates the sheet and prints the path.
#
# Usage:
#   scripts/diagrams_contact_sheet.sh <base1> [base2 ...]
#
# The argument list is the set of diagram bases (matching the Makefile's
# DIAGRAM_BASES variable). It first ensures all diagrams are rendered (by
# calling diagrams_render.sh), then writes the contact sheet. The script is
# POSIX-compatible (uses only npx, mktemp, printf, cat, ls) so it runs under
# both bash and /bin/sh (macOS).

set -u

DIAGRAM_DIR="docs/diagrams"
DIAGRAM_GEN="$DIAGRAM_DIR/generated"

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <base1> [base2 ...]" >&2
  exit 2
fi

# Ensure all diagrams are rendered (call the render step, level 2).
echo "Ensuring all diagrams are rendered (level 2)..."
bash "$(dirname "$0")/diagrams_render.sh" "$@" || {
  echo "WARNING: some diagrams failed to render; the contact sheet will show them as missing." >&2
}

# Generate the contact sheet as a temporary HTML file (a grid of all diagram
# PNGs). It is written to the system temp directory (not the repo) and the
# operator opens it in a browser for visual review.
sheet=$(mktemp -t gossipgrid-contact-sheet.XXXXXX.html)

cat > "$sheet" <<'HTML_HEAD'
<!doctype html><html><head><meta charset="utf-8"><title>GossipGrid diagram contact sheet</title>
<style>body{font-family:sans-serif;margin:20px}h1{font-size:18px}.note{font-size:13px;color:#475569;margin-bottom:16px}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(520px,1fr));gap:16px}.cell{border:1px solid #cbd5e1;border-radius:8px;padding:8px;background:#f8fafc}.cell h2{font-size:13px;margin:0 0 6px;color:#0f172a}.cell img{width:100%;height:auto;border-radius:4px;background:white}.missing{color:#dc2626;font-size:12px}</style>
</head><body><h1>GossipGrid diagram contact sheet — visual review</h1>
<p class="note">Inspect each diagram for clipped labels, overlapping nodes, incorrectly interpreted note directives, unreadable text, excessively small fonts, broken infrastructure boundaries, or unintended layout changes.</p>
<div class="grid">
HTML_HEAD

# Resolve an absolute path for the generated directory (the file:// URL in the
# contact sheet must be absolute for the browser to load the images).
abs_gen=$(cd "$DIAGRAM_GEN" && pwd)

missing=0
for base in "$@"; do
  png="$DIAGRAM_GEN/$base.png"
  if [ ! -f "$png" ]; then
    printf '<div class="cell"><h2>%s</h2><p class="missing">missing (render failed)</p></div>' "$base" >> "$sheet"
    missing=1
    continue
  fi
  abs_png="$abs_gen/$base.png"
  printf '<div class="cell"><h2>%s</h2><img src="file://%s" alt="%s"/></div>' "$base" "$abs_png" "$base" >> "$sheet"
done
printf '</div></body></html>' >> "$sheet"

if [ "$missing" -ne 0 ]; then
  echo "Some diagram PNGs are missing (see the contact sheet); visual review is incomplete." >&2
fi

echo "Contact sheet written to: $sheet"
echo "Open it in a browser for visual review. This is a convenience, not a CI gate."
echo "NOTE: If a visual reviewer (human or vision model) is not available, this step does NOT claim the diagrams are visually correct — it only generates the sheet. Inspect it yourself."
