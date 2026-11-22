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

if [ "$status" -eq 0 ]; then
  echo "All embedded Mermaid blocks match their .mmd sources; no malformed note patterns."
else
  echo "diagrams-check (level 1: source): failures detected (see FAIL lines above)." >&2
fi
exit "$status"
