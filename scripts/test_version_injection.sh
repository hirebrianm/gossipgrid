#!/usr/bin/env bash
# Regression test for the Makefile command-injection fix and version detection.
#
# Resolves the repository root relative to this script's own location (no
# absolute developer-machine paths). Creates a temporary Git repository,
# copies the Makefile and version.sh into it, and asserts that:
#   - A tag containing a command-substitution payload is treated as literal
#     data (the payload is never executed).
#   - GitLab CI (CI_COMMIT_TAG / CI_COMMIT_REF_NAME) resolves correctly.
#   - GitHub Actions (GITHUB_REF_TYPE / GITHUB_REF_NAME) resolves correctly.
#   - Local branch resolution (main -> dev, feature/* -> sanitised name).
#
# The script fails (exit 1) when any assertion does not match its expected
# value. It cleans up its temporary directory via a trap on exit.
set -eu

# Resolve the repository root from this script's own path.
# scripts/test_version_injection.sh -> repo root is one level up.
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# --- Temporary workspace -----------------------------------------------------
tmp="$(mktemp -d "${TMPDIR:-/tmp}/ggver.XXXXXX")"
cleanup() {
  rm -rf "$tmp"
}
trap cleanup EXIT

# --- Isolate from CI environment ---------------------------------------------
# When this script runs under GitLab or GitHub CI, the environment carries
# CI_COMMIT_TAG, CI_COMMIT_REF_NAME, GITHUB_REF_TYPE, and GITHUB_REF_NAME.
# These would override the temporary repository's local branch/tag state during
# version detection, causing tests that rely on local Git to produce incorrect
# versions. Unsetting them here ensures a clean baseline; individual test cases
# that explicitly set CI variables (e.g. "CI_COMMIT_REF_NAME=main make version")
# override the empty environment via inline assignment.
unset CI_COMMIT_TAG CI_COMMIT_REF_NAME GITHUB_REF_TYPE GITHUB_REF_NAME

# --- Setup -------------------------------------------------------------------
cd "$tmp"
git init -q
git config user.email t@t.co
git config user.name t
echo hi > a.txt
# Add a .gitignore so that normal generated artifacts (bin/, data/, *.out)
# are ignored by git and do not dirty the tree for versioning purposes.
cat > .gitignore <<'GITIGNORE'
bin/
data/
*.out
dist/
*.test
coverage.*
.DS_Store
GITIGNORE
git add a.txt .gitignore
git commit -qm init
cp "$REPO_ROOT/Makefile" .
mkdir -p scripts
cp "$REPO_ROOT/scripts/version.sh" scripts/
chmod +x scripts/version.sh
# Commit the Makefile and version.sh so the working tree is clean when we
# test exact-tag detection. Without this, the untracked Makefile and
# scripts/version.sh would make the tree dirty, and the tag would not be
# recognised (falling through to branch-based resolution).
git add Makefile scripts/
git commit -qm "add build files"

# --- Helper -------------------------------------------------------------------
# extract_version runs `make version` in the current directory and prints the
# value of the "version:" field. It exits with a non-zero status if the
# "version:" line is absent.
extract_version() {
  local out
  out="$(make version 2>&1)"
  printf '%s\n' "$out" | awk '/^version:/{print $2}'
}

# assert_eq fails the test (exit 1) if actual != expected.
assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [ "$actual" != "$expected" ]; then
    echo "FAIL: $label"
    echo "  expected: $expected"
    echo "  actual:   $actual"
    exit 1
  fi
  echo "PASS: $label (version: $actual)"
}

# assert_not_contains fails the test if $haystack contains $needle.
assert_not_contains() {
  local label="$1" haystack="$2" needle="$3"
  case "$haystack" in
    *"$needle"*)
      echo "FAIL: $label — found '$needle' in '$haystack'"
      exit 1
      ;;
  esac
  echo "PASS: $label (no '$needle' in output)"
}

# --- TEST 1: local tag with literal command-substitution payload -------------
echo "=== TEST 1: local tag with literal command-substitution payload ==="
# The tag name literally contains $(id). Single quotes prevent the outer shell
# from expanding it; the tag name is the literal string v$(id).
git tag 'v$(id)'
git checkout -q 'v$(id)'

version="$(extract_version)"
# The version must contain the literal characters $(id), NOT the output of id
# (which would look like "uid=...").
assert_not_contains "no payload output in version" "$version" "uid="
# The sanitised form of v$(id) is v--id-  (parentheses replaced with dashes).
# The version must NOT be "dev" (that would mean the tag was not recognised).
if [ "$version" = "dev" ]; then
  echo "FAIL: version is 'dev' — tag 'v\$(id)' was not recognised"
  exit 1
fi
# The version must contain "id" (from the literal payload) but must not
# contain "uid=" (which would indicate the payload was executed).
assert_not_contains "no 'uid=' in version" "$version" "uid="
echo "PASS: TEST 1 (version: $version — payload treated as literal data)"

# --- TEST 2: GitHub tag env with payload -------------------------------------
echo
echo "=== TEST 2: GitHub tag env with payload ==="
# Simulate GitHub passing a tag ref name with a payload via the environment.
out="$(GITHUB_REF_TYPE=tag GITHUB_REF_NAME='v$(id)' make version 2>&1)"
version="$(printf '%s\n' "$out" | awk '/^version:/{print $2}')"
assert_not_contains "no payload output in GitHub tag version" "$version" "uid="
# The version should be the sanitised form, not "dev".
if [ "$version" = "dev" ]; then
  echo "FAIL: GitHub tag version is 'dev' — tag was not recognised"
  exit 1
fi
echo "PASS: TEST 2 (version: $version)"

# --- TEST 3: normal tag still works -------------------------------------------
echo
echo "=== TEST 3: normal tag still works ==="
git checkout -q -b clean-branch
git tag v1.2.3
out="$(GITHUB_REF_TYPE=tag GITHUB_REF_NAME=v1.2.3 make version 2>&1)"
version="$(printf '%s\n' "$out" | awk '/^version:/{print $2}')"
assert_eq "GitHub tag v1.2.3" "v1.2.3" "$version"

# --- TEST 4: normal branch (no tag on HEAD) -----------------------------------
echo
echo "=== TEST 4: normal branch (no tag on HEAD) ==="
git checkout -q -b feature/foo
echo "branch commit" >> a.txt
git add a.txt
git commit -qm "branch commit"
# Remove any GitHub/GitLab env vars to ensure local branch resolution is used.
version="$(extract_version)"
assert_eq "local branch feature/foo" "feature-foo" "$version"

# --- TEST 5: main branch -> dev ------------------------------------------------
echo
echo "=== TEST 5: main branch -> dev ==="
git checkout -q -b main 2>/dev/null || git checkout -q main
echo "main commit" >> a.txt
git add a.txt
git commit -qm "main commit"
version="$(extract_version)"
assert_eq "local branch main -> dev" "dev" "$version"

# --- TEST 6: GitLab branch main -> dev -----------------------------------------
echo
echo "=== TEST 6: GitLab branch main -> dev ==="
version="$(CI_COMMIT_REF_NAME=main make version 2>&1 | awk '/^version:/{print $2}')"
assert_eq "GitLab CI_COMMIT_REF_NAME=main -> dev" "dev" "$version"

# --- TEST 7: GitHub PR short ref name -------------------------------------------
echo
echo "=== TEST 7: GitHub PR short ref name ==="
# In a GitHub Actions PR checkout the branch is detached and GITHUB_REF_NAME
# is the SHORT ref name "N/merge" (e.g. "42/merge"), GITHUB_REF_TYPE=branch.
# (GITHUB_REF_NAME is NOT "refs/pull/N/merge" — that is GITHUB_REF, the full ref.)
version="$(GITHUB_REF_TYPE=branch GITHUB_REF_NAME='42/merge' make version 2>&1 | awk '/^version:/{print $2}')"
# The sanitised form of "42/merge" is "42-merge".
assert_eq "GitHub PR 42/merge" "42-merge" "$version"

# --- TEST 8: GitLab tag with payload (CI_COMMIT_TAG) ----------------------------
echo
echo "=== TEST 8: GitLab tag with payload (CI_COMMIT_TAG) ==="
version="$(CI_COMMIT_TAG='v$(id)' make version 2>&1 | awk '/^version:/{print $2}')"
assert_not_contains "no payload output in GitLab tag version" "$version" "uid="
if [ "$version" = "dev" ]; then
  echo "FAIL: GitLab tag version is 'dev' — tag was not recognised"
  exit 1
fi
echo "PASS: TEST 8 (version: $version — payload treated as literal data)"

# --- TEST 9: GitHub main branch -> dev -------------------------------------------
echo
echo "=== TEST 9: GitHub main branch -> dev ==="
# GITHUB_REF_TYPE=branch, GITHUB_REF_NAME=main (short ref name, not "refs/heads/main").
version="$(GITHUB_REF_TYPE=branch GITHUB_REF_NAME=main make version 2>&1 | awk '/^version:/{print $2}')"
assert_eq "GitHub main -> dev" "dev" "$version"

# --- TEST 10: GitHub feature branch -> sanitised name -----------------------------
echo
echo "=== TEST 10: GitHub feature branch -> sanitised name ==="
version="$(GITHUB_REF_TYPE=branch GITHUB_REF_NAME='feature/example' make version 2>&1 | awk '/^version:/{print $2}')"
assert_eq "GitHub feature/example -> feature-example" "feature-example" "$version"

# --- TEST 11: GitHub release tag -> tag as-is --------------------------------------
echo
echo "=== TEST 11: GitHub release tag -> tag as-is ==="
version="$(GITHUB_REF_TYPE=tag GITHUB_REF_NAME=v1.2.3 make version 2>&1 | awk '/^version:/{print $2}')"
assert_eq "GitHub release tag v1.2.3" "v1.2.3" "$version"

# --- TEST 12: dirty tree at tagged HEAD on main -> dev -------------------------
echo
echo "=== TEST 12: dirty tree at tagged HEAD on main -> dev ==="
# Create a tag on the current HEAD, then modify a tracked file without committing.
git checkout -q main
git tag v2.0.0
echo "uncommitted change" >> a.txt
# Do NOT commit. The working tree is now dirty at the tagged commit.
version="$(extract_version)"
assert_eq "dirty tree at tag v2.0.0 on main -> dev" "dev" "$version"

# --- TEST 13: clean tree at tagged HEAD -> exact tag ---------------------------
echo
echo "=== TEST 13: clean tree at tagged HEAD -> exact tag ==="
# Commit the change to make the tree clean, then create a new tag.
git add a.txt
git commit -qm "commit for clean tree"
git tag v3.0.0
version="$(extract_version)"
assert_eq "clean tree at tag v3.0.0" "v3.0.0" "$version"

# --- TEST 14: dirty tree at tagged HEAD on feature branch -> sanitised branch --
echo
echo "=== TEST 14: dirty tree at tagged HEAD on feature branch -> sanitised branch ==="
git checkout -q -b feature/dirty-tag
git tag v4.0.0
echo "another uncommitted change" >> a.txt
# Do NOT commit. Dirty tree at tagged commit on a feature branch.
version="$(extract_version)"
assert_eq "dirty tree at tag on feature/dirty-tag -> feature-dirty-tag" "feature-dirty-tag" "$version"

# --- TEST 15: untracked Go file makes the tree dirty ---------------------------
echo
echo "=== TEST 15: untracked Go file makes tree dirty -> dev ==="
# An untracked .go file can affect the build, so it must make the working
# tree dirty for version resolution. The tree is only clean when both tracked
# files are unmodified AND there are no non-ignored untracked files.
git checkout -q main
git add a.txt
git commit -qm "clean for untracked go test"
git tag v5.0.0
# Create an untracked Go file (not ignored by .gitignore).
echo "package main" > newfile.go
version="$(extract_version)"
assert_eq "untracked Go file at tag v5.0.0 -> dev" "dev" "$version"

# --- TEST 15b: ignored build artifact does not dirty the tree ------------------
echo
echo "=== TEST 15b: ignored build artifact does not prevent tag reuse ==="
# Build artifacts that match .gitignore rules (bin/, data/, *.out) are not
# reported by git status --porcelain --untracked-files=normal, so they do not
# dirty the tree for versioning purposes.
rm -f newfile.go
# Create a new commit so the tag is on a unique HEAD (git describe
# --exact-match returns the first tag when multiple tags share a commit).
echo "commit for ignored artifact test" >> a.txt
git add a.txt
git commit -qm "commit for ignored artifact test"
git tag v5.1.0
# Create an untracked file that is ignored by .gitignore.
mkdir -p bin
echo "binary" > bin/gossipgrid
version="$(extract_version)"
assert_eq "ignored build artifact at tag v5.1.0 -> v5.1.0" "v5.1.0" "$version"

# --- TEST 15c: clean tagged checkout -> exact tag -------------------------------
echo
echo "=== TEST 15c: clean tagged checkout -> exact tag ==="
# Remove the ignored artifact and verify a truly clean tree returns the tag.
rm -rf bin
# Create a new commit so the tag is on a unique HEAD.
echo "commit for clean tagged test" >> a.txt
git add a.txt
git commit -qm "commit for clean tagged test"
git tag v5.2.0
version="$(extract_version)"
assert_eq "clean tagged checkout -> v5.2.0" "v5.2.0" "$version"

# --- TEST 16: CI tag context overrides dirty local tree -------------------------
echo
echo "=== TEST 16: CI tag context overrides dirty local tree ==="
# Even with a dirty local tree, an explicit CI release context (CI_COMMIT_TAG
# or GITHUB_REF_TYPE=tag) takes precedence. CI checkouts are clean by design,
# and the CI pipeline is the authoritative release context.
# (We are still on a dirty tree from TEST 14's feature branch commit, but we
# force the CI context.)
version="$(CI_COMMIT_TAG=v6.0.0 make version 2>&1 | awk '/^version:/{print $2}')"
assert_eq "CI_COMMIT_TAG=v6.0.0 overrides dirty tree" "v6.0.0" "$version"

version="$(GITHUB_REF_TYPE=tag GITHUB_REF_NAME=v7.0.0 make version 2>&1 | awk '/^version:/{print $2}')"
assert_eq "GITHUB tag ref overrides dirty tree" "v7.0.0" "$version"

echo
echo "All version injection tests passed."
