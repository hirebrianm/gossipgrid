# GossipGrid Makefile — the single command interface for local development and CI.
#
# The same commands work on macOS (local development) and Linux (CI).
#
# Version detection precedence:
#   1. Exact Git tag on HEAD (or CI_COMMIT_TAG in GitLab CI) — only when the
#      working tree is clean. A dirty tree falls through to branch resolution
#      to prevent local uncommitted changes from reusing a release tag.
#   2. Sanitised branch name if not on main (or CI_COMMIT_REF_NAME in GitLab CI)
#   3. "dev"
#
# FIPS: builds are FIPS-enabled by default (FIPS_MODULE=v1.0.0).
#       Override with FIPS_MODULE=off for an ordinary development build.
#
# `qa` *verifies* formatting (fails on a diff) rather than rewriting sources;
# `fmt` is the target that actually modifies files.
#
# Artifact generation is deliberately separated from validation:
#   - `make architecture`    generates the committed SVG/PNG diagram outputs.
#   - `make build`          compiles the persistent application binary into ./bin.
#   - `make qa`             validation only; it never regenerates diagram assets,
#                            never compiles the persistent binary, never builds a
#                            container image, and never rewrites tracked sources.
#   - `make all`            explicit aggregate: generate diagrams, build, then run
#                            qa, sequentially (not in parallel under -j).
#   - `make clean`          removes build outputs (the persistent binary only).

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
#   2. An exact tag on HEAD (local annotated/lightweight tag) — only when the
#      working tree is clean. A dirty tree falls through to branch resolution
#      to prevent local uncommitted changes from reusing a release tag.
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

# Architecture diagrams: docs/diagrams/*.mmd are the authoritative Mermaid
# sources, each rendered to a pair of outputs under docs/diagrams/generated/:
#   <name>.svg  (scalable, preferred)
#   <name>.png  (raster convenience)
# The Mermaid CLI version is pinned so regeneration is reproducible and the
# generated outputs are stable enough for reasonable version-control diffs. If
# the pinned version is unavailable, update MERMAID_CLI_VERSION and re-run.
#
# Each .mmd file must contain exactly one Mermaid diagram (no markdown
# front-matter, no multiple diagrams in a file) — mmdc renders a single diagram
# per input. Companion diagrams (e.g. the lifecycle sequence diagram) are
# separate .mmd files and are rendered independently.
MERMAID_CLI_VERSION := 11.4.1
DIAGRAM_DIR    := docs/diagrams
DIAGRAM_GEN    := $(DIAGRAM_DIR)/generated
# The set of rendered diagram bases. system-architecture is the main diagram
# embedded in the README; the rest are linked from the documentation pages.
DIAGRAM_BASES  := system-architecture gossip-replication lifecycle-recovery \
                  lifecycle-recovery-sequence lifecycle-5a security-credentials \
                  security-credential-rotation gitops-delivery \
                  concept-gossip-propagation concept-eventual-consistency concept-gossip-vs-antientropy \
                  concept-epoch-fencing concept-operator-genesis concept-bootstrap-admission \
                  concept-bootstrap-privilege-isolation concept-transactional-install \
                  concept-revocation-linearization concept-ephemeral-recovery concept-gitops-rollback

.PHONY: docs-lint
docs-lint: ## Lint repository Markdown documents (fails on violations; does not modify files)
	$(call check_tool,goldmark-lint,go install github.com/mrueg/goldmark-lint/cmd/goldmark-lint@v0.6.3)
	goldmark-lint --no-cache --config $(MD_LINT_CONFIG) '**/*.md'

.PHONY: docs-fix-preview
docs-fix-preview: ## Show Markdown fixes --fix would apply (does not modify files)
	$(call check_tool,goldmark-lint,go install github.com/mrueg/goldmark-lint/cmd/goldmark-lint@v0.6.3)
	goldmark-lint --no-cache --fix-dry-run --config $(MD_LINT_CONFIG) '**/*.md'

# Architecture diagram generation. Uses npx to run a pinned version of
# @mermaid-js/mermaid-cli (mmdc). The npx package is fetched on first use and
# cached in the npm cache; no global install is required. A puppeteer config
# is passed so mmdc can run in a headless environment.
#
# The -b white option sets the PNG canvas to opaque white, matching the
# light-background "base" theme used by mermaid-config.json (so the SVG and
# PNG share a coherent light canvas and text contrast). The -w option sets the
# PNG width for a readable raster image. The light canvas is the supported
# baseline; native inline Mermaid separately supports the host's adaptive theme.
#
# Regenerate all diagrams:   make architecture
# Regenerate one:            make architecture DIAGRAM=system-architecture
#
# Determinism: mmdc output for a given .mmd source and pinned version is stable
# across runs (same coordinates, same text), so committed SVG/PNG diffs reflect
# only genuine source changes. Re-run after editing a .mmd to refresh outputs.

.PHONY: architecture
architecture: ## Render all docs/diagrams/*.mmd to SVG+PNG in docs/diagrams/generated/ (use DIAGRAM=<base> for one)
	$(call check_tool,npx,install Node.js >= 18 and npm)
	@mkdir -p $(DIAGRAM_GEN)
	@targets="$(DIAGRAM_BASES)"; \
	if [ -n "$(DIAGRAM)" ]; then targets="$(DIAGRAM)"; fi; \
	config="$(DIAGRAM_DIR)/mermaid-config.json"; \
	for base in $$targets; do \
		mmd="$(DIAGRAM_DIR)/$$base.mmd"; \
		svg="$(DIAGRAM_GEN)/$$base.svg"; \
		png="$(DIAGRAM_GEN)/$$base.png"; \
		if [ ! -f "$$mmd" ]; then echo "skip: $$mmd not found"; continue; fi; \
		echo "Rendering $$mmd (mmdc v$(MERMAID_CLI_VERSION))..."; \
		cfg=$$(mktemp); \
		printf '{"args":["--no-sandbox","--disable-setuid-sandbox"]}' > "$$cfg"; \
		npx --yes @mermaid-js/mermaid-cli@$(MERMAID_CLI_VERSION) -i "$$mmd" -o "$$svg" -c "$$config" -p "$$cfg"; \
		rc=$$?; rm -f "$$cfg"; \
		if [ $$rc -ne 0 ]; then echo "SVG generation failed for $$base (rc=$$rc)"; exit $$rc; fi; \
		cfg=$$(mktemp); \
		printf '{"args":["--no-sandbox","--disable-setuid-sandbox"]}' > "$$cfg"; \
		npx --yes @mermaid-js/mermaid-cli@$(MERMAID_CLI_VERSION) -i "$$mmd" -o "$$png" -c "$$config" -b white -w 1600 -p "$$cfg"; \
		rc=$$?; rm -f "$$cfg"; \
		if [ $$rc -ne 0 ]; then echo "PNG generation failed for $$base (rc=$$rc)"; exit $$rc; fi; \
		echo "  -> $$svg"; \
		echo "  -> $$png"; \
	done; \
	echo "Done. Generated files in $(DIAGRAM_GEN)/:"; \
	ls -1 $(DIAGRAM_GEN)/

# diagrams-check verifies that the native Mermaid blocks embedded in the
# documentation files match the authoritative .mmd sources. For each diagram
# base, it extracts the mermaid block (between the marker comment and the
# closing fence) from the mapped Markdown file and diffs it against the .mmd
# source. A mismatch means the embedded block was edited independently of the
# .mmd source — the sync-check fails and the operator must re-embed (copy the
# .mmd content into the Markdown block) or re-edit the .mmd.
#
# The mapping (diagram base -> Markdown file) is documented in the Makefile
# variable DIAGRAM_DOC_MAP below. Add a new diagram base here and in
# DIAGRAM_BASES together.
# The mapping is "base:doc1,doc2,..." — a diagram base may be embedded in
# multiple documentation files (e.g. system-architecture appears in both the
# README and docs/architecture.md). Each doc must carry a marker comment
# "<!-- DIAGRAM:<base> -->" immediately above its ```mermaid block.
DIAGRAM_DOC_MAP := \
  system-architecture:README.md,docs/architecture.md \
  gossip-replication:docs/gossip-and-bootstrap.md \
  lifecycle-recovery:docs/recovery.md \
  lifecycle-recovery-sequence:docs/recovery.md \
  lifecycle-5a:docs/gossip-and-bootstrap.md \
  security-credentials:docs/security.md \
  security-credential-rotation:docs/security.md \
  gitops-delivery:docs/kubernetes-deployment.md \
  concept-gossip-propagation:docs/concepts/gossip-protocols.md \
  concept-eventual-consistency:docs/concepts/eventual-consistency.md \
  concept-gossip-vs-antientropy:docs/concepts/anti-entropy.md \
  concept-epoch-fencing:docs/concepts/epoch-fencing.md \
  concept-operator-genesis:docs/lifecycle-metadata-and-genesis.md \
  concept-bootstrap-admission:docs/gossip-and-bootstrap.md \
  concept-bootstrap-privilege-isolation:docs/concepts/distributed-authority.md \
  concept-transactional-install:docs/concepts/concurrency-and-linearization.md \
  concept-revocation-linearization:docs/concepts/concurrency-and-linearization.md \
  concept-ephemeral-recovery:docs/concepts/ephemeral-state-and-recovery.md \
  concept-gitops-rollback:docs/concepts/ephemeral-state-and-recovery.md

.PHONY: diagrams-check
diagrams-check: ## LEVEL 1: Verify embedded Mermaid blocks in docs match .mmd sources, no malformed note patterns, adjacent artifact links resolve, and self-test fixtures pass
	@bash scripts/diagrams_check.sh $(DIAGRAM_DOC_MAP)
	@bash scripts/diagrams_check.sh --self-test

# LEVEL 2 (render validation): render every canonical .mmd source using the
# pinned Mermaid CLI (mmdc) to SVG and PNG in a TEMPORARY directory, and verify
# the outputs exist and are non-empty. Fail if Mermaid reports a syntax or
# rendering error, if the generated SVG/PNG is missing, or if the output is
# empty. The temporary directory is removed on every exit path (a trap in the
# script), so this target is read-only with respect to the repository: it
# never regenerates or touches docs/diagrams/generated/. The committed
# generated outputs are produced only by `make architecture` (level 0, the
# generation target), which is deliberately NOT part of qa: qa must verify
# without modifying tracked files. This target is an explicit operator/CI
# validation step, distinct from the source-sync check (level 1, diagrams-check).
.PHONY: diagrams-render
diagrams-render: ## LEVEL 2: Render all .mmd to SVG+PNG in a temp dir and verify outputs are non-empty (read-only, never touches docs/diagrams/generated/)
	$(call check_tool,npx,install Node.js >= 18 and npm)
	@bash scripts/diagrams_render.sh $(DIAGRAM_BASES)

# LEVEL 3 (visual review): generate a temporary contact sheet (a grid of all
# diagram PNGs) for manual visual inspection. The contact sheet is written to
# a temporary path OUTSIDE version-controlled documentation (the operator
# inspects it and discards it). It is a convenience for the operator, not a
# CI gate (the automated checks are levels 1 and 2). The operator is expected
# to actually look at the rendered diagrams — not merely check file sizes or
# exit codes. If a visual reviewer is unavailable, this step reports that
# limitation rather than claiming success.
.PHONY: diagrams-contact-sheet
diagrams-contact-sheet: ## LEVEL 3: Generate a temporary contact sheet (grid of all diagram PNGs) for visual review (kept outside VCS)
	@bash scripts/diagrams_contact_sheet.sh $(DIAGRAM_BASES)

# helm-lint validates the GossipGrid Helm chart (deploy/helm/gossipgrid) with
# `helm lint`. It is a read-only check (no cluster access, no modifications).
# The chart is part of the Milestone 7A Kubernetes staging deliverables.
HELM_CHART := deploy/helm/gossipgrid
HELM_NAMESPACE := gossipgrid

.PHONY: helm-lint
helm-lint: ## Lint the GossipGrid Helm chart (fails on findings)
	$(call check_tool,helm,see https://helm.sh/docs/intro/install/)
	helm lint $(HELM_CHART) --namespace $(HELM_NAMESPACE)

# helm-template renders the GossipGrid Helm chart with `helm template` (no
# cluster access) and verifies the output is non-empty and parses as YAML.
# It is a read-only check used to verify the chart renders correctly before
# deployment (Milestone 7A validation).
.PHONY: helm-template
helm-template: ## Render the GossipGrid Helm chart and verify the output parses (read-only, no cluster)
	$(call check_tool,helm,see https://helm.sh/docs/intro/install/)
	@out=$$(helm template test-release $(HELM_CHART) --namespace $(HELM_NAMESPACE) 2>&1); \
	rc=$$?; \
	if [ $$rc -ne 0 ]; then echo "helm template failed:"; echo "$$out"; exit $$rc; fi; \
	if [ -z "$$out" ]; then echo "helm template produced empty output"; exit 1; fi; \
	lines=$$(printf '%s\n' "$$out" | wc -l | tr -d ' '); \
	echo "helm template: OK ($$lines lines rendered)"
	@command -v python3 >/dev/null 2>&1 || { \
		echo "ERROR: python3 is required for YAML validation (install with: apk add python3 py3-yaml)"; \
		exit 1; \
	}
	@python3 -c "import yaml" 2>/dev/null || { \
		echo "ERROR: PyYAML is required for YAML validation (install with: apk add py3-yaml)"; \
		exit 1; \
	}
	@helm template test-release $(HELM_CHART) --namespace $(HELM_NAMESPACE) 2>/dev/null | \
		python3 -c "import sys, yaml; list(yaml.safe_load_all(sys.stdin)); print('YAML parse: OK')"

# deploy-checks runs the GossipGrid Helm chart regression checks
# (deploy/helm/gossipgrid/tests/deploy_checks.sh). It validates the rendered
# chart output against the Milestone 7A integration requirements:
# StatefulSet/headless Service DNS compatibility, per-pod certificate identity
# (cert-manager CSI driver), VSO manifest schema, Argo CD values references,
# Secret rotation without subPath, no public peer exposure, and NetworkPolicy
# ingress restrictions. It is a read-only check (no cluster access).
DEPLOY_CHECKS := deploy/helm/gossipgrid/tests/deploy_checks.sh

.PHONY: deploy-checks
deploy-checks: ## Run the GossipGrid Helm chart regression checks (read-only, no cluster)
	$(call check_tool,helm,see https://helm.sh/docs/intro/install/)
	$(call check_tool,python3,install with: apk add python3 py3-yaml (Alpine) or apt-get install python3 python3-yaml (Debian))
	bash $(DEPLOY_CHECKS)

# all is the explicit aggregate workflow: generate the diagram assets, compile
# the application binary, then run the validation suite. The recipe is
# sequential (each line is a separate recursive Make invocation) so that `make
# -j all` does not run qa concurrently with artifact generation. It does NOT
# include image publication, deployment, contact sheets, fmt, or clean; those
# remain their own explicit targets. The FIPS and version-injection behavior of
# build is unchanged (build is invoked as-is; the qa suite carries the version
# regression tests).
.PHONY: all
all: ## Generate diagrams, build the application, and run QA
	$(MAKE) architecture
	$(MAKE) build
	$(MAKE) qa

# qa is verification only: it must not modify any tracked file. diagram
# generation (make architecture) writes the committed SVG/PNG outputs and is
# therefore not part of qa; render validation is available as the explicit,
# read-only target diagrams-render (renders into a temporary directory).
# helm-lint and deploy-checks are included in qa (the Helm chart is part of
# the deliverables; the regression checks validate the rendered output).
.PHONY: qa
qa: fmt-check vet lint security docs-lint diagrams-check helm-lint deploy-checks test test-race test-version-injection test-deploy-pipefail ## Verify fmt, vet, lint, security, docs, diagrams (source sync+lint), helm, deploy checks, tests, race, and version regression

.PHONY: test-version-injection
test-version-injection: ## Run the version-detection and command-injection regression tests
	bash scripts/test_version_injection.sh

.PHONY: test-deploy-pipefail
test-deploy-pipefail: ## Run the pipefail safety regression test for deploy_checks.sh
	bash scripts/test_deploy_checks_pipefail.sh

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
clean: ## Remove build outputs (bin/, plus any stray root-level legacy gossipgrid executable)
	rm -rf $(BIN_DIR)
	# Historical build invocations may have emitted the binary at the repository
	# root (a bare `go build ./cmd/gossipgrid` with no -o). Only remove a regular
	# file named exactly "gossipgrid" at the root — never a directory, never
	# recursively, and never a source path (cmd/gossipgrid is untouched). This
	# is a no-op when no such file exists.
	@if [ -f "./gossipgrid" ] && [ ! -d "./gossipgrid" ]; then rm -f ./gossipgrid; fi
