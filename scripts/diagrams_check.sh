#!/usr/bin/env bash
# diagrams_check.sh — LEVEL 1 (source validation) of the Mermaid validation
# pipeline. It performs two checks:
#
#   1. SOURCE SYNC: the native Mermaid blocks embedded in the documentation
#      files match the authoritative .mmd sources byte-for-byte.
#   2. SOURCE LINT: the .mmd sources have no malformed or unmatched
#      multiline note/end note pairs, no known problematic inline-note
#      patterns, and (state diagrams only) no unescaped semicolons in
#      transition labels (Mermaid treats ';' as a statement separator).
#
# Usage:
#   scripts/diagrams_check.sh <base:doc1,doc2> [base:doc ...]
#
# The "base:doc1,doc2" argument format matches the Makefile's DIAGRAM_DOC_MAP
# variable, so the Make target passes $(DIAGRAM_DOC_MAP) through unchanged.
#
# The script is POSIX-compatible (uses only awk, tr, cat, diff, grep, mktemp)
# so it runs under both bash and /bin/sh (macOS). It exits non-zero on the
# first failure and prints a diff or the offending line.

set -u

DIAGRAM_DIR="docs/diagrams"

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <base:doc1,doc2> [base:doc ...]" >&2
  exit 2
fi

status=0

# --- Check 1: source sync (embedded blocks match .mmd sources) ---
for pair in "$@"; do
  base=${pair%%:*}
  docs=${pair##*:}
  mmd="$DIAGRAM_DIR/$base.mmd"
  if [ ! -f "$mmd" ]; then
    echo "skip: $mmd not found"
    continue
  fi
  for doc in $(printf '%s' "$docs" | tr ',' ' '); do
    if [ ! -f "$doc" ]; then
      echo "skip: $doc not found"
      continue
    fi
    extracted=$(awk -v base="$base" '
      index($0, "<!-- DIAGRAM:" base " -->") {flag=1; next}
      flag && /^```mermaid$/ {flag=2; next}
      flag==2 && /^```$/ {flag=0; next}
      flag==2 {print}
    ' "$doc")
    if [ -z "$extracted" ]; then
      echo "FAIL: no mermaid block found in $doc for $base (marker not found)"
      status=1
      continue
    fi
    if [ "$extracted" = "$(cat "$mmd")" ]; then
      echo "OK: $doc ($base)"
    else
      echo "FAIL: $doc ($base) — embedded block does not match $mmd"
      tmp=$(mktemp)
      printf '%s\n' "$extracted" > "$tmp"
      diff "$tmp" "$mmd" | head -20
      rm -f "$tmp"
      status=1
    fi
  done
done

# --- Check 2: source lint (note/end note pairing, inline-note patterns) ---
# Iterate over all .mmd files in the diagram directory (not just the ones in
# DIAGRAM_DOC_MAP, since a .mmd may exist without an embedded doc).
for mmd in "$DIAGRAM_DIR"/*.mmd; do
  if [ ! -f "$mmd" ]; then
    continue
  fi
  base=$(basename "$mmd" .mmd)

  # 2a. Unmatched note/end note: count "note ... of <STATE>" opening lines
  # (a multiline note starts with "note <dir> of <STATE>" on its own line,
  # WITHOUT a trailing ":") and "end note" closing lines. They must be equal.
  # A multiline note opening line matches: ^[[:space:]]*note[[:space:]]+[a-z]+[[:space:]]+of[[:space:]]+[A-Za-z_]+[[:space:]]*$
  # (no trailing colon, no text after the state name).
  note_open=$(grep -cE '^[[:space:]]*note[[:space:]]+(left|right|top|bottom)[[:space:]]+of[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*$' "$mmd" 2>/dev/null) || note_open=0
  note_close=$(grep -cE '^[[:space:]]*end[[:space:]]+note[[:space:]]*$' "$mmd" 2>/dev/null) || note_close=0
  if [ "$note_open" != "$note_close" ]; then
    echo "FAIL: $base — unmatched note/end note ($note_open note opens vs $note_close 'end note' closes)"
    status=1
  fi

  # 2b. Known problematic inline-note pattern: "note <dir> of <STATE> : text"
  # (a colon after the state name on the same line). This is the pattern that
  # renders as multiple tiny nodes in some Mermaid renderers. It is a single
  # line (not a multiline note) and must not appear in a state diagram.
  inline_note=$(grep -nE '^[[:space:]]*note[[:space:]]+(left|right|top|bottom)[[:space:]]+of[[:space:]]+[A-Za-z_][A-Za-z0-9_]*[[:space:]]*:' "$mmd" 2>/dev/null) || inline_note=""
  if [ -n "$inline_note" ]; then
    echo "FAIL: $base — inline note pattern (renders as tiny nodes in some renderers):"
    printf '%s\n' "$inline_note"
    echo "  Fix: use the multiline form:"
    echo "      note right of <STATE>"
    echo "          <text line 1>"
    echo "          <text line 2>"
    echo "      end note"
    status=1
  fi

  # 2c. stateDiagram-only check: a "note ... of" line with no "end note" pair
  # (a multiline note that is missing its terminator). This is covered by 2a
  # (open/close mismatch), but we add a specific message for clarity.
  if [ "$note_open" -gt 0 ] && [ "$note_close" -eq 0 ]; then
    echo "FAIL: $base — state diagram has a multiline note without an 'end note' terminator"
    status=1
  fi

  # 2d. stateDiagram-only check: a ";" (semicolon) on a transition label
  # (a state transition line "<A> --> <B>: <label>"). Mermaid uses ";" as a
  # statement separator, so an unescaped semicolon in the label is parsed as
  # the end of the transition — the text after it is dropped or mis-parsed.
  # This is state-diagram-specific: in other diagram types (flowchart,
  # sequence) semicolons inside quoted strings are legal, so we only flag
  # them on state transition lines here.
  is_state=$(grep -cE '^[[:space:]]*stateDiagram' "$mmd" 2>/dev/null) || is_state=0
  if [ "$is_state" -gt 0 ]; then
    semi_lines=$(grep -nE '^[[:space:]]*[A-Za-z_*][A-Za-z0-9_*]*[[:space:]]*--+[[:space:]]*>?[^:]*:.*;' "$mmd" 2>/dev/null) || semi_lines=""
    if [ -n "$semi_lines" ]; then
      echo "FAIL: $base — unescaped semicolon in a state transition label (Mermaid treats ';' as a statement separator and will drop or mis-parse the text after it):"
      printf '%s\n' "$semi_lines"
      echo "  Fix: remove the ';', or reword the label (e.g. 'a; b' -> 'a, b' or 'a - b')."
      status=1
    fi
  fi
done

# --- Check 3: discover all mermaid blocks in all .md files and verify each
# has a DIAGRAM marker, a registered canonical .mmd source, generated SVG+PNG
# artifacts, and SVG/PNG links near the diagram. ---
#
# This check is exhaustive: it walks every .md file in the repository
# (excluding docs/diagrams/ which is not documentation content) and inspects
# every ```mermaid block. A block is valid when:
#   1. The line immediately above the ```mermaid fence contains
#      "<!-- DIAGRAM:<base> -->"
#   2. docs/diagrams/<base>.mmd exists (canonical source)
#   3. docs/diagrams/generated/<base>.svg exists (generated artifact)
#   4. docs/diagrams/generated/<base>.png exists (generated artifact)
#   5. The .md file contains a link to <base>.svg and <base>.png
#      (the links may be anywhere in the file, not necessarily immediately
#      adjacent to the diagram block)
#
# Note: we do NOT verify that the base appears in DIAGRAM_DOC_MAP here,
# because the script receives the map as arguments and the map is already
# enforced by Check 1 (source sync). Check 3 verifies the structural
# integrity of every block regardless of registration.

# Build a list of all .md files (exclude docs/diagrams/ which is not doc
# content, and exclude .git/).
md_files=$(find . -name '*.md' -not -path './.git/*' -not -path './docs/diagrams/*' 2>/dev/null | sort)

for doc in $md_files; do
  # Find line numbers of ```mermaid fences in this file
  mermaid_lines=$(grep -n '^```mermaid$' "$doc" 2>/dev/null) || mermaid_lines=""
  [ -z "$mermaid_lines" ] && continue

  for ml in $mermaid_lines; do
    ml_num=${ml%%:*}
    # The DIAGRAM marker must be on the line immediately above the fence
    prev_line=$((ml_num - 1))
    if [ "$prev_line" -lt 1 ]; then
      echo "FAIL: $doc line $ml_num — mermaid block has no DIAGRAM marker (no line above)"
      status=1
      continue
    fi
    marker_line=$(sed -n "${prev_line}p" "$doc")
    case "$marker_line" in
      *"<!-- DIAGRAM:"*)
        base=$(printf '%s' "$marker_line" | sed 's/.*<!-- DIAGRAM:\([^ ]*\) -->.*/\1/')
        # 3a: canonical .mmd source exists
        mmd="$DIAGRAM_DIR/$base.mmd"
        if [ ! -f "$mmd" ]; then
          echo "FAIL: $doc line $ml_num — DIAGRAM:$base block has no canonical source ($mmd not found)"
          status=1
          continue
        fi
        # 3b: generated SVG artifact exists
        svg="$DIAGRAM_DIR/generated/$base.svg"
        if [ ! -f "$svg" ]; then
          echo "FAIL: $doc line $ml_num — DIAGRAM:$base block has no generated SVG ($svg not found)"
          status=1
          continue
        fi
        # 3c: generated PNG artifact exists
        png="$DIAGRAM_DIR/generated/$base.png"
        if [ ! -f "$png" ]; then
          echo "FAIL: $doc line $ml_num — DIAGRAM:$base block has no generated PNG ($png not found)"
          status=1
          continue
        fi
        # 3d: doc contains a link to the SVG and a link to the PNG
        if ! grep -q "$base\.svg" "$doc"; then
          echo "FAIL: $doc line $ml_num — DIAGRAM:$base block has no link to $base.svg in $doc"
          status=1
          continue
        fi
        if ! grep -q "$base\.png" "$doc"; then
          echo "FAIL: $doc line $ml_num — DIAGRAM:$base block has no link to $base.png in $doc"
          status=1
          continue
        fi
        echo "OK: $doc line $ml_num ($base — all artifacts and links present)"
        ;;
      *)
        echo "FAIL: $doc line $ml_num — mermaid block has no DIAGRAM marker (line $prev_line: $marker_line)"
        status=1
        ;;
    esac
  done
done

if [ "$status" -eq 0 ]; then
  echo "All embedded Mermaid blocks match their .mmd sources; no malformed note patterns; all blocks have markers, sources, artifacts, and links."
else
  echo "diagrams-check (level 1: source): failures detected (see FAIL lines above)." >&2
fi
exit "$status"
