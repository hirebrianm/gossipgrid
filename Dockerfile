# GossipGrid — multistage static build, final image `scratch`.
#
# This milestone builds the HTTP server (no database yet). The build stage
# enables CGO and static linking with the same technique used by the project
# owner's reference implementation, so when the GORM/SQLite milestone lands the
# only change is adding `sqlite-static`/`sqlite-dev` and the SQLite build tags.
#
# Reference: https://github.com/hirebrianm/go-sdk-demo/blob/main/Dockerfile
#
# The FIPS build profile is introduced later as a separately validated build;
# it is not part of this base image.

FROM golang:1.27-alpine AS build

ARG BUILD_VERSION=dev
# FIPS module: "v1.0.0" (default) enables FIPS; "off" produces a non-FIPS build.
# Passed from the Makefile via --build-arg FIPS_MODULE=...
ARG FIPS_MODULE=v1.0.0

# build-base provides gcc/musl for static linking. sqlite-static/sqlite-dev and
# the "sqlite_omit_load_extension" build tag are added when SQLite is introduced.
RUN apk add --no-cache build-base

WORKDIR /src

# Copy module files first for better layer caching.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# CGO_ENABLED=1 + linkmode external + -static produces a fully static musl
# binary suitable for `scratch`.
#
# GOFIPS140 is set from the FIPS_MODULE build arg:
#   FIPS_MODULE=off   -> GOFIPS140=off (explicit, overrides any inherited env
#                        var) and FIPSRequired=false.
#   FIPS_MODULE=<ver> -> GOFIPS140=<ver> and FIPSRequired=true.
#
# FIPSRequired is injected via -ldflags -X so the binary records at compile
# time whether FIPS was required, independent of runtime state.
RUN CGO_ENABLED=1 CC=gcc \
  GOFIPS140=$(if [ "$FIPS_MODULE" = "off" ]; then echo "off"; else echo "$FIPS_MODULE"; fi) \
  FIPS_REQUIRED=$(if [ "$FIPS_MODULE" = "off" ]; then echo "false"; else echo "true"; fi) \
  go build \
    -trimpath \
    -ldflags="-s -w -linkmode external -extldflags '-static' -X main.Version=${BUILD_VERSION} -X main.FIPSRequired=${FIPS_REQUIRED}" \
    -o /gossipgrid \
    ./cmd/gossipgrid

FROM scratch

# Non-root user required. scratch has no /etc/passwd, so `USER` must be a
# numeric UID:GID. 65534:65534 (nobody) is a conventional non-root pair.
COPY --from=build /gossipgrid /gossipgrid

USER 65534:65534

EXPOSE 8080

ENTRYPOINT ["/gossipgrid"]
