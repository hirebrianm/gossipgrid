#!/usr/bin/env bash
# Resolve the GossipGrid build version from CI environment variables and Git
# state, and print a value that is safe to pass through GNU Make's $(shell)
# macro expansion.
#
# Untrusted data (Git ref names, CI ref names) is read from the environment or
# from git, and is only ever compared, tested, or transformed via bash
# parameter expansion — never interpolated into a shell command. Crucially, the
# value printed to stdout is sanitised BEFORE it is read by the Makefile: all
# characters outside [a-zA-Z0-9_.-] are replaced with "-". This means the
# $(shell ...) output never contains a "..." pattern, so even if GNU Make
# performs macro expansion on the output, there is nothing for the shell to
# execute. This is the core defence against command injection via a ref name
# such as "$(id)".
#
# Precedence:
#   1. CI_COMMIT_TAG (GitLab) or a GitHub tag ref (GITHUB_REF_TYPE=tag).
#   2. An exact tag on HEAD (local tag) — only when the working tree is clean.
#      A dirty tree at a tagged commit falls through to branch-based resolution
#      to prevent local uncommitted changes from reusing a release tag.
#   3. CI_COMMIT_REF_NAME (GitLab) or a GitHub branch ref.
#   4. The local branch name.
#   5. Fallback: "dev" (untagged main, detached HEAD without ref metadata).
#
# Output: a single line containing the sanitised version (safe for Makefile
# macro expansion, linker flags, and Docker tags).
set -eu

resolve_raw() {
  # 1. Explicit CI tag (GitLab, or GitHub tag ref).
  if [ -n "${CI_COMMIT_TAG:-}" ]; then
    printf '%s\n' "$CI_COMMIT_TAG"
    return 0
  fi
  if [ "${GITHUB_REF_TYPE:-}" = "tag" ] && [ -n "${GITHUB_REF_NAME:-}" ]; then
    printf '%s\n' "$GITHUB_REF_NAME"
    return 0
  fi

  # 2. Exact tag on HEAD (local annotated/lightweight tag), but ONLY when the
  #    working tree is clean. A dirty tree at a tagged commit means local
  #    uncommitted changes exist — reusing the release tag would be misleading.
  #    Fall through to branch-based resolution instead.
  local tag
  tag="$(git describe --tags --exact-match HEAD 2>/dev/null || true)"
  if [ -n "$tag" ]; then
    # Check for uncommitted changes. Both tracked file modifications
    # (git diff) and untracked files (git status --porcelain --untracked-files=normal)
    # can affect the build. The --untracked-files=normal flag respects .gitignore:
    # ignored files (e.g. bin/, data/) are not reported, so normal generated
    # artifacts do not dirty the tree for versioning purposes.
    if git diff --quiet HEAD 2>/dev/null; then
      # Tracked files are clean; now check for non-ignored untracked files.
      local untracked
      untracked="$(git status --porcelain --untracked-files=normal 2>/dev/null || true)"
      if [ -z "$untracked" ]; then
        printf '%s\n' "$tag"
        return 0
      fi
    fi
    # Dirty tree (tracked modifications or non-ignored untracked files):
    # do not reuse the release tag. Fall through to branch-based resolution
    # (main -> dev, feature branch -> sanitised branch name).
  fi

  # 3. CI branch name (GitLab) or GitHub branch ref.
  # GITHUB_REF_NAME is a SHORT ref name (e.g. "main", "feature/example",
  # "42/merge" for PRs), NOT a full "refs/heads/..." path.
  if [ -n "${CI_COMMIT_REF_NAME:-}" ]; then
    if [ "$CI_COMMIT_REF_NAME" = "main" ]; then
      printf 'dev\n'
    else
      printf '%s\n' "$CI_COMMIT_REF_NAME"
    fi
    return 0
  fi
  if [ -n "${GITHUB_REF_TYPE:-}" ] && [ -n "${GITHUB_REF_NAME:-}" ]; then
    case "$GITHUB_REF_NAME" in
      main) printf 'dev\n' ;;
      */merge) printf '%s\n' "$GITHUB_REF_NAME" ;;
      *) printf '%s\n' "$GITHUB_REF_NAME" ;;
    esac
    return 0
  fi

  # 4. Local branch name (detached HEAD -> "HEAD" -> fall through).
  local br
  br="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
  if [ -n "$br" ] && [ "$br" != "HEAD" ]; then
    if [ "$br" = "main" ]; then
      printf 'dev\n'
    else
      printf '%s\n' "$br"
    fi
    return 0
  fi

  # 5. Fallback.
  printf 'dev\n'
}

sanitise() {
  local v="$1"
  # Replace any char outside the allowed set with "-". This also removes all
  # shell metacharacters ($, (, ), `, |, ;, &, spaces, etc.), making the value
  # safe for Makefile macro expansion and shell interpolation.
  v="${v//[^a-zA-Z0-9_.-]/-}"
  # Strip a leading "." or "-".
  case "$v" in
    .*) v="${v#.}" ;;
    -*) v="${v#-}" ;;
  esac
  # Strip a trailing "." or "-" (only the last char, if it matches).
  v="${v%%[.-]}"
  # If empty (ref was all invalid chars), use "dev".
  [ -n "$v" ] || v="dev"
  printf '%s\n' "$v"
}

# Always print the sanitised value. The sanitised value is safe for Makefile
# macro expansion (no shell metacharacters), linker flags, and Docker tags.
# Normal tag/branch names are preserved in shape: "v1.2.3" -> "v1.2.3",
# "feature/foo" -> "feature-foo", "main" -> "dev".
sanitise "$(resolve_raw)"
