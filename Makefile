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

# CI variables: empty when not in GitLab CI, so the Git-based fallback is used.
CI_COMMIT_TAG      ?=
CI_COMMIT_REF_NAME ?=

# Exact tag on HEAD (empty when HEAD is not tagged).
GIT_TAG := $(shell git describe --tags --exact-match HEAD 2>/dev/null)

# Current branch name (empty in detached HEAD).
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null)

# Resolve: CI tag > exact tag > (CI branch > git branch if not main) > dev.
ifneq ($(CI_COMMIT_TAG),)
  VERSION := $(CI_COMMIT_TAG)
else ifneq ($(GIT_TAG),)
  VERSION := $(GIT_TAG)
else ifneq ($(CI_COMMIT_REF_NAME),)
  ifeq ($(CI_COMMIT_REF_NAME),main)
    VERSION := dev
  else
    VERSION := $(CI_COMMIT_REF_NAME)
  endif
else ifneq ($(GIT_BRANCH),)
  ifeq ($(GIT_BRANCH),main)
    VERSION := dev
  else
    VERSION := $(GIT_BRANCH)
  endif
else
  VERSION := dev
endif

# Sanitise for use as a Docker tag: replace invalid chars with "-", trim dashes.
# The negated class [^a-zA-Z0-9_.-] matches anything NOT a valid tag character
# (including /, spaces, colons, etc.) and replaces it with "-".
# Uses | as the sed delimiter so / in branch names is treated literally.
SANITISED_VERSION := $(shell printf '%s' "$(VERSION)" | sed -e 's|[^a-zA-Z0-9_.-]|-|g' -e 's|^[.-]||' -e 's|[.-]$$||')
ifeq ($(SANITISED_VERSION),)
  SANITISED_VERSION := dev
endif

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

.PHONY: build
build: ## Build the binary into ./bin
	@mkdir -p $(BIN_DIR)
	$(GOFIPS140_ENV) go build -trimpath -ldflags="-X main.Version=$(SANITISED_VERSION) $(FIPS_LDFLAG)" -o $(BIN) ./cmd/gossipgrid

.PHONY: run
run: build ## Build and run the application locally
	$(BIN)

.PHONY: test
test: ## Run unit tests
	$(GOFIPS140_ENV) go test ./...

.PHONY: test-race
test-race: ## Run unit tests with race detection
	$(GOFIPS140_ENV) go test -race ./...

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

.PHONY: qa
qa: fmt-check vet lint security test test-race ## Verify fmt, vet, lint, security, tests, and race detection

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
