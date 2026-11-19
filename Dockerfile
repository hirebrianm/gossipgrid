# GossipGrid — multistage static build, final image `scratch`.
#
# Stage 1 (build): CGO static build of the Go binary with the GORM/SQLite
# driver (mattn/go-sqlite3). The `sqlite_omit_load_extension` build tag omits
# dynamic library loading, producing a fully static musl binary.
#
# Stage 2 (scratch): final minimal image. The /data directory is created in
# the build stage and copied into scratch with --chown to set the correct
# non-root ownership (65534:65534). A .keep file ensures the directory is
# non-empty and survives the COPY.
#
# FIPS: FIPS_MODULE=v1.0.0 (default) enables FIPS; "off" produces a non-FIPS build.

FROM golang:1.27-alpine AS build

ARG BUILD_VERSION=dev
# FIPS module: "v1.0.0" (default) enables FIPS; "off" produces a non-FIPS build.
# Passed from the Makefile via --build-arg FIPS_MODULE=...
ARG FIPS_MODULE=v1.0.0

# build-base provides gcc/musl for static linking.
# sqlite-static provides the static SQLite library for CGO linking.
# sqlite-dev provides the headers and .pc file for CGO compilation.
RUN apk add --no-cache build-base sqlite-static sqlite-dev

WORKDIR /src

# Copy module files first for better layer caching.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# Create /data with a .keep file. The directory must be non-empty for
# COPY into scratch to preserve it. Ownership and permissions are set
# via --chown and --chmod in the final stage's COPY instruction.
RUN mkdir -p /data && touch /data/.keep

# CGO_ENABLED=1 + linkmode external + -static produces a fully static musl
# binary suitable for `scratch`.
#
# GOFIPS140 and FIPS_REQUIRED are computed in a preceding shell statement and
# explicitly injected into the build environment and linker flags. FIPS_MODULE
# is untrusted build-arg data: it is only compared (== "off") and its value is
# passed through a variable — never interpolated into shell command text — so a
# value like $(id) cannot execute during the build.
#   FIPS_MODULE=off   -> GOFIPS140=off, FIPS_REQUIRED=false
#   FIPS_MODULE=<ver> -> GOFIPS140=<ver>, FIPS_REQUIRED=true
#
# FIPSRequired is injected via -ldflags -X so the binary records at compile
# time whether FIPS was required, independent of runtime state.
#
# The `sqlite_omit_load_extension` build tag tells mattn/go-sqlite3 to skip
# the sqlite3_load_extension call, which requires dynamic library loading
# and would break static linking.
RUN set -eu; \
    if [ "$FIPS_MODULE" = "off" ]; then GOFIPS140=off; FIPS_REQUIRED=false; else GOFIPS140="$FIPS_MODULE"; FIPS_REQUIRED=true; fi; \
    echo "GOFIPS140=$GOFIPS140 FIPS_REQUIRED=$FIPS_REQUIRED"; \
    CGO_ENABLED=1 CC=gcc \
    GOFIPS140="$GOFIPS140" \
    go build \
      -trimpath \
      -tags sqlite_omit_load_extension \
      -ldflags="-s -w -linkmode external -extldflags '-static' -X main.Version=${BUILD_VERSION} -X main.FIPSRequired=${FIPS_REQUIRED}" \
      -o /gossipgrid \
      ./cmd/gossipgrid

FROM scratch

# Non-root user required. scratch has no /etc/passwd, so `USER` must be a
# numeric UID:GID. 65534:65534 (nobody) is a conventional non-root pair.
#
# COPY --chown sets ownership on both the directory and its contents.
# This is the correct mechanism for scratch images: there is no shell to
# run chown at runtime.
#
# Note: Kubernetes emptyDir mounts will overlay /data at runtime and have
# their own filesystem permissions. The image-level ownership here ensures
# the container works without a volume mount (e.g. local testing).
COPY --from=build --chown=65534:65534 /gossipgrid /gossipgrid
COPY --from=build --chown=65534:65534 /data /data

USER 65534:65534

EXPOSE 8080

ENV GOSSIPGRID_DB_PATH=/data/gossipgrid.db

ENTRYPOINT ["/gossipgrid"]
