#!/usr/bin/env bash
# diagrams_check.sh — LEVEL 1 (source validation) of the Mermaid validation
# pipeline. It performs four checks:
#
#   1. SOURCE SYNC: the native Mermaid blocks embedded in the documentation
#      files match the authoritative .mmd sources byte-for-byte.
#   2. SOURCE LINT: the .mmd sources have no malformed or unmatched
#      multiline note/end note pairs, no known problematic inline-note
#      patterns, and (state diagrams only) no unescaped semicolons in
#      transition labels (Mermaid treats ';' as a statement separator).
#   3. ADJACENT LINKS + ARTIFACTS: every ```mermaid block in every document
#      has a DIAGRAM marker, a registered canonical .mmd source, generated
#      SVG+PNG artifacts, and a standardized adjacent SVG/PNG link line
#      immediately above the marker whose relative paths resolve to files
#      relative to the containing document.
#   4. SELF-TEST (regression coverage for Check 3): a throwaway fixture
#      suite that asserts the adjacency validator accepts a correctly-linked
#      block and rejects each documented deficiency (missing SVG link, missing
#      PNG link, wrong diagram base, a broken relative path, missing generated
#      artifacts, and non-adjacent links). Run via --self-test, or as part of
#      the diagram QA (the Makefile diagrams-check target). The fixtures are
#      built in a temporary directory and removed on exit, so the check stays
#      read-only with respect to the repository.
#
# Usage:
#   scripts/diagrams_check.sh <base:doc1,doc2> [base:doc ...]
#   scripts/diagrams_check.sh --self-test
#
# The "base:doc1,doc2" argument format matches the Makefile's DIAGRAM_DOC_MAP
# variable, so the Make target passes $(DIAGRAM_DOC_MAP) through unchanged.
# The --self-test argument is dispatched before any document argument is
# interpreted as a diagram mapping, so it never produces a spurious
# "skip: docs/diagrams/--self-test.mmd not found" message.
#
# The script is POSIX-compatible (uses only awk, tr, cat, diff, grep, sed,
# mktemp) so it runs under both bash and /bin/sh (macOS). It exits non-zero
# on the first failure and prints a diff or the offending line.

set -u

DIAGRAM_DIR="docs/diagrams"

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <base:doc1,doc2> [base:doc ...] | --self-test" >&2
  exit 2
fi

# --- Check 4 (self-test): focused negative/positive coverage for the Check 3
# adjacency validation. Invoked explicitly via
# "scripts/diagrams_check.sh --self-test". It builds a throwaway repository
# layout in a temp directory, runs the same validate_doc function used by the
# main run against a set of fixture documents, and asserts which fixtures pass
# and which fail. The temp tree is removed on exit, so the check is read-only
# with respect to the repository and leaves no deliberately-invalid fixture
# files behind. When invoked, it runs in place of the repository checks.
#
# This definition precedes the dispatch below (and is in scope when the
# fixtures reuse link_rel_for / validate_doc), so --self-test is handled
# before any argument is interpreted as a "base:doc" mapping.

# Relative path prefix from a document to the "diagrams/" directory (the
# parent of "generated/"). The calculation is general for any document
# nesting: it strips a leading "./" (find . output), finds the document's
# directory, and walks up ("../") once per path segment between the document
# directory and the repository root. It is bounded: it stops when the directory
# stops shrinking (a no-op strip), which always happens at or before the root,
# so it terminates for any input (a bare "x.md" -> ""; "a/b/c/x.md" ->
# "../../docs/"; "docs/x.md" -> ""; "docs/concepts/x.md" -> "../").
# The caller appends "diagrams/generated/" to reach the artifact directory.
link_rel_for() {
  local doc="$1" dir up="" slashes
  case "$doc" in
    ./*) doc="${doc#./}" ;;
  esac
  # Strip the file name to get the document's directory.
  dir="${doc%/*}"
  [ "$dir" = "$doc" ] && dir="."

  if [ "$dir" = "." ]; then
    # Document at the repository root (README.md): prefix is "docs/".
    printf 'docs/'
    return 0
  fi

  # Count the number of "/" characters in the document's directory path.
  # This equals the number of "../" needed to reach the repository root
  # from the parent of the document's last segment.
  # "docs" -> 0 slashes -> 0 "../" (already at docs/)
  # "docs/concepts" -> 1 slash -> 1 "../" (one level below docs/)
  # "a/b/c" -> 2 slashes -> 3 "../" (3 levels deep, then descend into docs/)
  slashes=$(printf '%s' "$dir" | tr -cd '/' | wc -c | tr -d ' \n')
  # tr -cd '/' keeps only "/" characters; wc -c counts them (may pad with
  # spaces on some platforms); tr -d ' \n' strips whitespace and newlines.
  # The number of "../" needed = slashes + 1 if NOT under docs/,
  # or slashes if under docs/ (since we stop at "docs").
  # Build the "../" prefix: a simple bounded counter loop (avoids seq,
  # which behaves differently on macOS vs GNU for "seq 0").
  local n i=0
  case "$dir" in
    docs*)
      # Under docs/: walk up `slashes` times (stops at "docs").
      n="$slashes"
      ;;
    *)
      # NOT under docs/: walk up `slashes + 1` times (reaches root),
      # then descend into docs/.
      n="$((slashes + 1))"
      ;;
  esac
  while [ "$i" -lt "$n" ]; do
    up="${up}../"
    i=$((i + 1))
  done
  case "$dir" in
    docs*) : ;;
    *) up="${up}docs/" ;;
  esac
  printf '%s' "$up"
}

# validate_doc <doc> <diagram_dir>: validate every mermaid block in <doc>.
# Prints an OK or FAIL line per block and returns 0/1. This is the single
# implementation of the adjacency rule, shared by the main run (Check 3) and
# the negative/positive self-test (Check 4). It is defined before the
# self-test dispatch so the fixtures can reuse it.
validate_doc() {
  local doc="$1"
  local diagram_dir="$2"
  local docdir rel link_line link_expected rc=0
  case "$doc" in
    ./*) doc="${doc#./}" ;;
  esac
  docdir=$(printf '%s' "$doc" | sed 's|/[^/]*$||')
  [ "$docdir" = "$doc" ] && docdir="."
  # rel is the prefix to diagrams/; the artifacts live in diagrams/generated/.
  rel="$(link_rel_for "$doc")diagrams/generated/"

  local mermaid_lines ml ml_num prev_line marker_line base mmd svg png cand cand_text
  mermaid_lines=$(grep -n '^```mermaid$' "$doc" 2>/dev/null) || mermaid_lines=""
  [ -z "$mermaid_lines" ] && return 0

  for ml in $mermaid_lines; do
    ml_num=${ml%%:*}
    prev_line=$((ml_num - 1))
    if [ "$prev_line" -lt 1 ]; then
      echo "FAIL: $doc line $ml_num — mermaid block has no DIAGRAM marker (no line above)"
      rc=1
      continue
    fi
    marker_line=$(sed -n "${prev_line}p" "$doc")
    case "$marker_line" in
      *"<!-- DIAGRAM:"*)
        base=$(printf '%s' "$marker_line" | sed 's/.*<!-- DIAGRAM:\([^ ]*\) -->.*/\1/')
        mmd="$diagram_dir/$base.mmd"
        if [ ! -f "$mmd" ]; then
          echo "FAIL: $doc line $ml_num — DIAGRAM:$base block has no canonical source ($mmd not found)"
          rc=1
          continue
        fi
        svg="$diagram_dir/generated/$base.svg"
        if [ ! -f "$svg" ]; then
          echo "FAIL: $doc line $ml_num — DIAGRAM:$base block has no generated SVG ($svg not found)"
          rc=1
          continue
        fi
        png="$diagram_dir/generated/$base.png"
        if [ ! -f "$png" ]; then
          echo "FAIL: $doc line $ml_num — DIAGRAM:$base block has no generated PNG ($png not found)"
          rc=1
          continue
        fi
        # The standardized adjacent artifact-link line, immediately above the
        # marker, allowing one intervening blank line.
        link_line=""
        cand=$((prev_line - 1))
        if [ "$cand" -ge 1 ]; then
          cand_text=$(sed -n "${cand}p" "$doc")
          if [ -n "$cand_text" ]; then
            link_line="$cand_text"
          else
            # blank line; the link line is one above the marker
            link_line=$(sed -n "$((cand - 1))p" "$doc")
          fi
        fi
        link_expected="[View full-size SVG]($rel$base.svg) · [View PNG]($rel$base.png)"
        if [ "$link_line" = "$link_expected" ]; then
          # Both artifact files must resolve relative to the document.
          if [ ! -f "$docdir/$rel$base.svg" ] || [ ! -f "$docdir/$rel$base.png" ]; then
            echo "FAIL: $doc line $ml_num — DIAGRAM:$base adjacent link paths do not resolve to files"
            rc=1
            continue
          fi
          echo "OK: $doc line $ml_num ($base — adjacent artifact links resolve)"
        else
          echo "FAIL: $doc line $ml_num — DIAGRAM:$base has no standardized adjacent artifact-link line"
          echo "  Expected: $link_expected"
          echo "  Found:    $( [ -n "$link_line" ] && printf '%s' "$link_line" || echo '(no line above marker)' )"
          rc=1
        fi
        ;;
      *)
        echo "FAIL: $doc line $ml_num — mermaid block has no DIAGRAM marker (line $prev_line: $marker_line)"
        rc=1
        ;;
    esac
  done
  return "$rc"
}

run_self_test() {
  self_root=$(mktemp -d)
  trap 'rm -rf "$self_root"' EXIT
  self_diagram_dir="$self_root/docs/diagrams"
  self_gen="$self_diagram_dir/generated"
  mkdir -p "$self_gen" "$self_root/docs/concepts" "$self_root/a/b/c"
  # Canonical sources + generated artifacts for each fixture base. The
  # "other" and "deep" bases let the path-calculation fixtures (a document at
  # the repository root and a deeply nested document) resolve their artifact
  # links through the correct relative prefix ("docs/").
  for b in ok wrongbase brokenpath deep readme; do
    printf 'flowchart LR\n  A --> B\n' > "$self_diagram_dir/$b.mmd"
    : > "$self_gen/$b.svg"
    : > "$self_gen/$b.png"
  done
  # missing: a canonical source whose generated SVG/PNG are intentionally
  # absent (the "missing-generated-artifact" fixture tests artifact existence,
  # distinct from a wrong relative link path).
  printf 'flowchart LR\n  A --> B\n' > "$self_diagram_dir/missing.mmd"

  # Run from within the temp root so that document paths are relative
  # (e.g. "docs/concepts/<name>.md" or "a/b/c/<name>.md") and link_rel_for's
  # general relative-path calculation applies exactly as it does in the real
  # repository.
  (
    cd "$self_root"
    # make_fixture <name> <dir> <link-line> [marker-line]: write a fixture doc
    # in <dir> with a single mermaid block and validate it with the real
    # validate_doc. The marker defaults to "<!-- DIAGRAM:ok -->". Prints the
    # validator's OK/FAIL line; returns its status.
    make_fixture() {
      local name="$1" dir="$2" linkline="$3" markerline="${4:-}"
      [ -z "$markerline" ] && markerline="<!-- DIAGRAM:ok -->"
      {
        printf '# %s\n\n' "$name"
        printf '%s\n\n' "$linkline"
        printf '%s\n' "$markerline"
        printf '```mermaid\nflowchart LR\n  A --> B\n```\n'
      } > "$dir/$name.md"
      validate_doc "$dir/$name.md" "docs/diagrams"
    }

    local self_status=0 out
    # 1. correct links pass (document in docs/concepts/ -> prefix "../")
    if out=$(make_fixture ok docs/concepts '[View full-size SVG](../diagrams/generated/ok.svg) · [View PNG](../diagrams/generated/ok.png)'); then
      echo "OK: self-test fixture 'valid-links' accepted"
    else
      echo "FAIL (self-test): 'valid-links' should have been accepted: $out"
      self_status=1
    fi
    # 2. a missing SVG link fails
    if out=$(make_fixture missingsvg docs/concepts '[View PNG](../diagrams/generated/ok.png)'); then
      echo "FAIL (self-test): 'missing-svg-link' should have been rejected but was accepted"
      self_status=1
    else
      echo "OK: self-test fixture 'missing-svg-link' rejected"
    fi
    # 3. a missing PNG link fails
    if out=$(make_fixture missingpng docs/concepts '[View full-size SVG](../diagrams/generated/ok.svg)'); then
      echo "FAIL (self-test): 'missing-png-link' should have been rejected but was accepted"
      self_status=1
    else
      echo "OK: self-test fixture 'missing-png-link' rejected"
    fi
    # 4. a wrong diagram base fails (links point at base 'ok', marker is 'wrongbase')
    if out=$(make_fixture wrongbase docs/concepts '[View full-size SVG](../diagrams/generated/ok.svg) · [View PNG](../diagrams/generated/ok.png)' '<!-- DIAGRAM:wrongbase -->'); then
      echo "FAIL (self-test): 'wrong-diagram-base' should have been rejected but was accepted"
      self_status=1
    else
      echo "OK: self-test fixture 'wrong-diagram-base' rejected"
    fi
    # 5. a broken relative path fails: the canonical source AND the generated
    # artifacts for 'brokenpath' exist in the correct generated/ directory, so
    # the link is well-formed for the base — but the Markdown links use an
    # incorrect relative path (../../ instead of ../), which is the
    # "adjacent artifact links do not resolve" case. This tests a wrong link
    # path, not missing artifacts.
    if out=$(make_fixture brokenpath docs/concepts '[View full-size SVG](../../diagrams/generated/brokenpath.svg) · [View PNG](../../diagrams/generated/brokenpath.png)' '<!-- DIAGRAM:brokenpath -->'); then
      echo "FAIL (self-test): 'broken-relative-path' should have been rejected but was accepted"
      self_status=1
    else
      echo "OK: self-test fixture 'broken-relative-path' rejected"
    fi
    # 6. a missing generated artifact fails: the links are the correct
    # relative path for base 'missing', but the generated SVG/PNG do not
    # exist, so the artifact existence check fails. Distinct from fixture 5.
    if out=$(make_fixture missing docs/concepts '[View full-size SVG](../diagrams/generated/missing.svg) · [View PNG](../diagrams/generated/missing.png)' '<!-- DIAGRAM:missing -->'); then
      echo "FAIL (self-test): 'missing-generated-artifact' should have been rejected but was accepted"
      self_status=1
    else
      echo "OK: self-test fixture 'missing-generated-artifact' rejected"
    fi
    # 7. links elsewhere in the document do not satisfy the adjacency
    # requirement (a paragraph sits between the link line and the marker)
    {
      printf '# distant\n\n'
      printf '[View full-size SVG](../diagrams/generated/ok.svg) · [View PNG](../diagrams/generated/ok.png)\n\n'
      printf 'A paragraph that separates the links from the marker.\n\n'
      printf '<!-- DIAGRAM:ok -->\n'
      printf '```mermaid\nflowchart LR\n  A --> B\n```\n'
    } > docs/concepts/distant.md
    if out=$(validate_doc "docs/concepts/distant.md" "docs/diagrams"); then
      echo "FAIL (self-test): 'non-adjacent-links' should have been rejected but was accepted"
      self_status=1
    else
      echo "OK: self-test fixture 'non-adjacent-links' rejected"
    fi
    # 8. path calculation: a document at the repository root (README.md) uses
    # the "docs/" prefix (the document's directory is ".", so no "../" is
    # needed, but the diagrams live under docs/). The correct link is accepted.
    if out=$(make_fixture readme . '[View full-size SVG](docs/diagrams/generated/readme.svg) · [View PNG](docs/diagrams/generated/readme.png)' '<!-- DIAGRAM:readme -->'); then
      echo "OK: self-test fixture 'root-doc-valid' accepted (README.md path calc)"
    else
      echo "FAIL (self-test): 'root-doc-valid' should have been accepted: $out"
      self_status=1
    fi
    # 9. path calculation: a document in the docs/ root (docs/x.md) uses the
    # "" (empty) prefix. The correct link is accepted.
    if out=$(make_fixture docsroot docs '[View full-size SVG](diagrams/generated/ok.svg) · [View PNG](diagrams/generated/ok.png)'); then
      echo "OK: self-test fixture 'docs-root-valid' accepted (docs/ path calc)"
    else
      echo "FAIL (self-test): 'docs-root-valid' should have been accepted: $out"
      self_status=1
    fi
    # 10. path calculation: a document in docs/concepts/ uses the "../" prefix
    # (exercises the general calculation for a docs/concepts document). The
    # correct link is accepted.
    if out=$(make_fixture concepts docs/concepts '[View full-size SVG](../diagrams/generated/ok.svg) · [View PNG](../diagrams/generated/ok.png)'); then
      echo "OK: self-test fixture 'concepts-valid' accepted (docs/concepts/ path calc)"
    else
      echo "FAIL (self-test): 'concepts-valid' should have been accepted: $out"
      self_status=1
    fi
    # 11. path calculation: a deeply nested document (a/b/c/x.md) is NOT under
    # docs/, so the walk-up reaches the repository root (three "../") and then
    # descends into docs/: the prefix is "../../../docs/". The correct link is
    # accepted; this exercises the general calculation beyond the first level.
    if out=$(make_fixture deep a/b/c '[View full-size SVG](../../../docs/diagrams/generated/deep.svg) · [View PNG](../../../docs/diagrams/generated/deep.png)' '<!-- DIAGRAM:deep -->'); then
      echo "OK: self-test fixture 'deep-nested-valid' accepted (a/b/c/ path calc)"
    else
      echo "FAIL (self-test): 'deep-nested-valid' should have been accepted: $out"
      self_status=1
    fi

    if [ "$self_status" -eq 0 ]; then
      echo "OK: self-test — adjacent-link validation accepts the valid, root, docs-root, concepts, and deep-nested fixtures and rejects missing-SVG, missing-PNG, wrong-base, broken-path, missing-artifact, and non-adjacent fixtures."
      return 0
    fi
    echo "diagrams-check (self-test): fixture assertions failed (see FAIL (self-test) lines above)." >&2
    return 1
  )
}

# --- Self-test dispatch (Check 4) ---
# The --self-test argument runs the regression-coverage fixture suite and
# returns; it is NOT a diagram base, so it is dispatched here (before any
# argument is interpreted as a "base:doc" mapping in Check 1). Because
# run_self_test is defined above, this dispatch never produces the spurious
# "skip: docs/diagrams/--self-test.mmd not found" message.
if [ "$1" = "--self-test" ]; then
  run_self_test
  exit $?
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
# artifacts, and a standardized ADJACENT SVG/PNG link line immediately above
# the marker. ---
#
# This check is exhaustive: it walks every .md file in the repository
# (excluding docs/diagrams/ which is not documentation content) and inspects
# every ```mermaid block. A block is valid when:
#   1. The line immediately above the ```mermaid fence contains
#      "<!-- DIAGRAM:<base> -->"
#   2. <diagram_dir>/<base>.mmd exists (canonical source)
#   3. <diagram_dir>/generated/<base>.svg exists (generated artifact)
#   4. <diagram_dir>/generated/<base>.png exists (generated artifact)
#   5. The line immediately above the marker (allowing one intervening blank
#      line) is the standardized artifact-link line:
#          [View full-size SVG](<rel>/<base>.svg) · [View PNG](<rel>/<base>.png)
#      where <rel> is the relative prefix from the containing document to
#      <diagram_dir>/generated. Both artifact files must resolve relative to
#      the document. A link elsewhere in the document does NOT satisfy the
#      requirement — it must be on that adjacent line.
#
# Note: we do NOT verify that the base appears in DIAGRAM_DOC_MAP here,
# because the script receives the map as arguments and the map is already
# enforced by Check 1 (source sync). Check 3 verifies the structural
# integrity of every block regardless of registration.
#
# The implementation is in validate_doc (defined near the top of the script,
# before the self-test dispatch, so the fixtures can reuse it). link_rel_for
# computes the relative prefix for arbitrary document nesting.

# Build a list of all .md files (exclude docs/diagrams/ which is not doc
# content, and exclude .git/).
md_files=$(find . -name '*.md' -not -path './.git/*' -not -path './docs/diagrams/*' 2>/dev/null | sort)

for doc in $md_files; do
  validate_doc "$doc" "$DIAGRAM_DIR" || status=1
done

if [ "$status" -eq 0 ]; then
  echo "All embedded Mermaid blocks match their .mmd sources; no malformed note patterns; all blocks have markers, sources, artifacts, and standardized adjacent artifact links."
else
  echo "diagrams-check (level 1: source): failures detected (see FAIL lines above)." >&2
fi
exit "$status"
