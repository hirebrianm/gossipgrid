# ADR-0003: Static `scratch` image and optional FIPS build

**Status:** Accepted direction; exact toolchain and crypto module not yet pinned · **Date:** 2026-11-18

## Context

The project should demonstrate security-conscious Go engineering without unnecessary runtime Linux distribution
dependencies. GORM's conventional SQLite driver uses CGO; Go now has native FIPS mode for its cryptographic module.

## Decision

Use multi-stage Alpine/musl tooling to compile Go with CGO and statically linked SQLite; package a verified static
executable in `scratch`. Embed web assets and templates. Provide normal and FIPS build profiles using a pinned native
Go module selection (`GOFIPS140`) once the compatible toolchain is chosen. Validate runtime FIPS state and TLS behavior
in tests and diagnostics. Do not substitute a Rocky base image solely to obtain FIPS support.

## Consequences

The build stage requires a C toolchain and static SQLite libraries; CI must inspect ELF dependencies and test the
binary. `scratch` requires deliberate CA mounts and a writable data volume. FIPS operation of Go's cryptographic
module is not certification of the container image, external Gateway, Vault, cluster hosts, or the whole application.

References: [Go FIPS documentation](https://go.dev/doc/security/fips140),
[owner's prior static Dockerfile](https://github.com/hirebrianm/go-sdk-demo/blob/main/Dockerfile).
