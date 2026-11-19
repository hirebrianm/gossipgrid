# GossipGrid Makefile — the single command interface for local development and CI.
#
# The same commands work on macOS (local development) and Linux (CI).
#
# Version detection precedence:
#   1. Exact Git tag on HEAD (or CI_COMMIT_TAG in GitLab CI)
#   2. Sanitised branch name if not on main (or CI_COMMIT_REF_NAME in GitLab CI)
#   3. "dev"
#
# FIPS: builds are FIPS-enabled by default (FIPS_MODULE=v1.0.0).
#       Override with FIPS_MODULE=off for an ordinary development build.
#
# `qa` *verifies* formatting (fails on a diff) rather than rewriting sources;
# `fmt` is the target that actually modifies files.

BIN_DIR    := bin
BIN        := $(BIN_DIR)/gossipgrid

# Image name is configurable; defaults to "gossipgrid" (no registry prefix).
IMAGE      ?= gossipgrid

# FIPS module: default is v1.0.0 (FIPS-enabled build). Set to "off" for a
# non-FIPS development build. Passed to go build/test via GOFIPS140 and to
# the Docker builder via --build-arg FIPS_MODULE.
FIPS_MODULE ?= v1.0.0

# Resolve GOFIPS140 and the FIPSRequired linker flag:
#   FIPS_MODULE=off   -> GOFIPS140=off (explicit, overrides any inherited env
#                        var) and FIPSRequired=false.
#   FIPS_MODULE=<ver> -> GOFIPS140=<ver> and FIPSRequired=true.
#
# GOFIPS140=off is set explicitly (not just unset) so that an inherited
# GOFIPS140=v1.0.0 in the calling shell cannot override an explicit
# FIPS_MODULE=off.
ifeq ($(FIPS_MODULE),off)
  GOFIPS140_ENV    := GOFIPS140=off
  FIPS_LDFLAG      := -X main.FIPSRequired=false
else
  GOFIPS140_ENV    := GOFIPS140=$(FIPS_MODULE)
  FIPS_LDFLAG      := -X main.FIPSRequired=true
endif

# --- Version detection --------------------------------------------------------
#
# Git ref names (tags, branch names) are untrusted data: they may contain
# shell metacharacters. They are never interpolated into shell command text.
# Instead, all resolution and sanitisation happens inside the POSIX shell
# helper below, where values are passed as positional arguments ($1, $2, ...)
# and are only ever compared, tested, or piped through sed as data — never
# re-interpreted by the shell. This prevents a tag such as "$(id)" from
# executing during Makefile evaluation.
#
# Precedence:
#   1. An explicit CI tag (GitLab CI_COMMIT_TAG, GitHub GITHUB_REF_NAME when
#      GITHUB_REF_TYPE=tag) takes precedence over branch-name fallbacks.
#   2. An exact tag on HEAD (local annotated/lightweight tag).
#   3. A CI branch name (GitLab CI_COMMIT_REF_NAME, GitHub GITHUB_REF_NAME
#      when GITHUB_REF_TYPE=branch) — "main" resolves to "dev".
#   4. The local branch name (detached HEAD yields empty -> "dev").
#   5. Fallback: "dev" (e.g. untagged main, or a detached HEAD with no ref
#      metadata in GitHub Actions — handled deliberately, not an error).
#
# GitHub pull requests check out in detached HEAD with GITHUB_REF_NAME of the
# short form "N/merge" (e.g. "42/merge", GITHUB_REF_TYPE=branch). The helper
# passes that ref through as data and sanitises it, so a PR build gets a safe,
# unique version without depending on complete Git history being present.

# CI variables: empty when not in the corresponding CI, so the Git-based
# fallback is used. The ?= form lets the environment override them in tests.
# They are exported so the version helper (scripts/version.sh) can read them
# from the environment.
CI_COMMIT_TAG      ?=
CI_COMMIT_REF_NAME ?=
GITHUB_REF_TYPE    ?=
GITHUB_REF_NAME    ?=
export CI_COMMIT_TAG CI_COMMIT_REF_NAME GITHUB_REF_TYPE GITHUB_REF_NAME

# Resolve the build version. scripts/version.sh reads CI variables from the
# environment and queries git for tag/branch state, then prints a value that
# has already been sanitised (all characters outside [a-zA-Z0-9_.-] replaced
# with "-"). Because the $(shell ...) output contains no shell metacharacters,
# a ref name such as "$(id)" is treated as literal data and never executed,
# even if GNU Make performs macro expansion on the output. The sanitised value
# is used for both display and the Docker tag, preserving the shape of normal
# tag/branch names (v1.2.3 -> v1.2.3, feature/foo -> feature-foo, main -> dev).
VERSION := $(shell bash scripts/version.sh)
ifeq ($(VERSION),)
  VERSION := dev
endif

# The Docker tag uses the same sanitised value.
SANITISED_VERSION := $(VERSION)

# The image reference combines the configurable name with the resolved tag.
IMAGE_REF := $(IMAGE):$(SANITISED_VERSION)

# --- Tool availability checks -------------------------------------------------
#
# Each target that requires an external tool checks for its presence and prints
# a message if the binary is not on PATH. These checks do NOT auto-install.

define check_tool
	@command -v $(1) >/dev/null 2>&1 || { \
		echo "Error: $(1) is not installed or not on PATH."; \
		echo "Install: $(2)"; \
		exit 1; \
	}
endef

# --- Targets -----------------------------------------------------------------

.PHONY: help
help: ## Show this help (default target)
	@awk 'BEGIN{FS="##"} /^[a-zA-Z0-9_-]+:.*## /{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@printf '\n'
	@printf 'Default target: help. Run make <target> for a specific action.\n'
	@printf '\n'
	@printf 'FIPS builds are enabled by default (FIPS_MODULE=v1.0.0).\n'
	@printf 'Override with FIPS_MODULE=off for a non-FIPS development build.\n'

# Build tags: sqlite_omit_load_extension is required for the CGO SQLite driver
# to link statically (omits sqlite3_load_extension, which needs dynamic loading).
GO_TAGS := sqlite_omit_load_extension

.PHONY: build
build: ## Build the binary into ./bin
	@mkdir -p $(BIN_DIR)
	$(GOFIPS140_ENV) CGO_ENABLED=1 go build -trimpath -tags $(GO_TAGS) -ldflags="-X main.Version=$(SANITISED_VERSION) $(FIPS_LDFLAG)" -o $(BIN) ./cmd/gossipgrid

.PHONY: run
run: build ## Build and run the application locally
	$(BIN)

.PHONY: test
test: ## Run unit tests
	$(GOFIPS140_ENV) CGO_ENABLED=1 go test -tags $(GO_TAGS) ./...

.PHONY: test-race
test-race: ## Run unit tests with race detection
	$(GOFIPS140_ENV) CGO_ENABLED=1 go test -tags $(GO_TAGS) -race ./...

.PHONY: fmt
fmt: ## Format Go sources (modifies files)
	gofmt -s -w .

.PHONY: fmt-check
fmt-check: ## Verify formatting without modifying (fails on diff)
	@files=$$(gofmt -s -l .); \
	if [ -n "$$files" ]; then \
		echo "The following files are not gofmt-formatted:"; \
		printf '%s\n' "$$files"; \
		echo "Run 'make fmt' to fix."; \
		exit 1; \
	fi

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: lint
lint: ## Run golangci-lint (fails on findings)
	$(call check_tool,golangci-lint,go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest)
	golangci-lint run ./...

.PHONY: security
security: ## Run gosec security scan (fails on actionable findings)
	$(call check_tool,gosec,go install github.com/securego/gosec/v2/cmd/gosec@latest)
	gosec ./...

# Markdown linting uses the .markdownlint.yml configuration explicitly (no
# auto-discovery) and --no-cache (no generated cache file). docs-lint is the
# read-only check used by qa; docs-fix-preview only shows the fixes --fix would
# apply without modifying any file (goldmark-lint v0.6.3 `--fix` mangles literal
# angle-bracket placeholders, so fixes are never applied automatically).
MD_LINT_CONFIG := .markdownlint.yml

.PHONY: docs-lint
docs-lint: ## Lint repository Markdown documents (fails on violations; does not modify files)
	$(call check_tool,goldmark-lint,go install github.com/mrueg/goldmark-lint/cmd/goldmark-lint@v0.6.3)
	goldmark-lint --no-cache --config $(MD_LINT_CONFIG) '**/*.md'

.PHONY: docs-fix-preview
docs-fix-preview: ## Show Markdown fixes --fix would apply (does not modify files)
	$(call check_tool,goldmark-lint,go install github.com/mrueg/goldmark-lint/cmd/goldmark-lint@v0.6.3)
	goldmark-lint --no-cache --fix-dry-run --config $(MD_LINT_CONFIG) '**/*.md'

.PHONY: qa
qa: fmt-check vet lint security docs-lint test test-race test-version-injection ## Verify fmt, vet, lint, security, docs, tests, race, and version regression

.PHONY: test-version-injection
test-version-injection: ## Run the version-detection and command-injection regression tests
	bash scripts/test_version_injection.sh

.PHONY: image
image: ## Build the static Docker image
	$(call check_tool,docker,see https://docs.docker.com/get-docker/)
	docker build --build-arg BUILD_VERSION=$(SANITISED_VERSION) --build-arg FIPS_MODULE=$(FIPS_MODULE) --tag $(IMAGE_REF) .

.PHONY: version
version: ## Show the resolved build version and image reference
	@echo "version:      $(VERSION)"
	@echo "tag:          $(SANITISED_VERSION)"
	@echo "fips_module:  $(FIPS_MODULE)"
	@echo "image:        $(IMAGE_REF)"

.PHONY: clean
clean: ## Remove build outputs
	rm -rf $(BIN_DIR)
