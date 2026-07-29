# syntax=docker/dockerfile:1.6
# Multi-stage Dockerfile for Fluxum — zero-CVE edition, same pattern as the
# Nexus and Synap images.
#
# The runtime image is `FROM scratch` carrying ONLY a fully static
# (<arch>-unknown-linux-musl) fluxum-server binary + user database + CA
# bundle + example config. Zero OS packages → zero CVEs by construction.
# Trade-off: no shell in the image — `docker exec ... sh` does not work;
# debug via `docker logs` and the HTTP admin API. The container HEALTHCHECK
# uses the binary itself (`fluxum-server --healthcheck`).
#
# The image tag ALWAYS matches the workspace version (Cargo.toml
# [workspace.package].version) — server and SDKs ride one release train
# (sdks/rust/tests/version_sync.rs enforces the manifests; this comment and
# the LABEL below are the image's part of the contract).
#
# HOW TO BUILD (single arch, local):
#   docker build -t hivehub/fluxum:0.3.0 -t hivehub/fluxum:latest .
#
# HOW TO BUILD + PUBLISH MULTI-ARCH (Docker Hub — hivehub/fluxum):
#   docker login
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     --sbom=true --provenance=mode=max \
#     -t hivehub/fluxum:0.3.0 -t hivehub/fluxum:latest --push .
#
#   Each platform builds NATIVELY (arm64 under qemu/binfmt on an amd64
#   host — same pattern as the Nexus/Synap Dockerfiles). No cross-toolchain:
#   the builder base is multi-arch and `musl-tools` provides the host-arch
#   musl-gcc on both platforms; `TARGETARCH` picks the matching Rust target
#   triple. The arm64 leg is slow under qemu on first build; BuildKit cache
#   mounts make re-runs incremental.
#
# HOW TO RUN:
#   docker run -d \
#     --name fluxum \
#     -p 15800:15800 \
#     -p 15801:15801 \
#     -v fluxum-data:/var/lib/fluxum \
#     -e FLUXUM_AUTH_SECRET=$(openssl rand -hex 32) \
#     -e FLUXUM_SERVER_ALLOW_PLAINTEXT=true \
#     hivehub/fluxum:latest
#
#   FLUXUM_SERVER_ALLOW_PLAINTEXT is for links encrypted BELOW Fluxum
#   (compose networks, a mesh, a TLS-terminating LB). For direct exposure,
#   drop it and set server.tls.cert/key instead (docs/DEPLOYMENT.md §TLS).
#
#   The runtime honors container resource limits: the boot-time hardware
#   probe (SPEC-016, FR-05) reads cgroup CPU/memory limits, so `--cpus` /
#   `--memory` flow into the `auto` derivations (worker threads, shards,
#   memory budget) with no extra configuration.
#
# HOW TO VERIFY:
#   curl http://localhost:15800/health
#   docker logs fluxum
#
# For more details, see docs/DEPLOYMENT.md

# Build stage — static musl binary, built NATIVELY per platform.
#
# `rustlang/rust:nightly` is Debian-based and multi-arch; the workspace is
# edition-2024/nightly (rust-toolchain.toml), and the image's default
# toolchain IS a dated nightly. Deliberately, rust-toolchain.toml is NOT
# copied into the build context stage: it would make cargo resolve the
# undated `nightly` channel and re-download a fresh toolchain WITHOUT the
# musl target added below. `musl-tools` provides the HOST-arch musl-gcc that
# the `cc`-built C deps (lz4, zstd) compile against; Rust targets
# <arch>-unknown-linux-musl with crt-static by default, producing a fully
# static PIE with no interpreter — runnable in `scratch`.
FROM rustlang/rust:nightly AS builder

ARG TARGETARCH
RUN apt-get update && apt-get install -y \
    musl-tools \
    file \
    && rm -rf /var/lib/apt/lists/* \
 && case "${TARGETARCH:-amd64}" in \
      amd64) TARGET_TRIPLE=x86_64-unknown-linux-musl ;; \
      arm64) TARGET_TRIPLE=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported TARGETARCH '${TARGETARCH}'" >&2; exit 1 ;; \
    esac \
 && rustup target add "${TARGET_TRIPLE}"

WORKDIR /app

# Copy the workspace manifest/lockfile and source for every workspace member
# declared in the root Cargo.toml (`crates/*` + `sdks/rust`). `cargo build`
# fails with "failed to load manifest for workspace member" if any member
# directory is missing, even when building a single package.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY sdks/rust ./sdks/rust

# Build ONLY fluxum-server (the runtime ships a single binary) in release
# mode for the musl target. BuildKit cache mounts keep the registry and
# target dir warm across rebuilds. The binary is staged outside the cache
# mount (only paths outside the mount survive into later stages), and the
# build fails fast if the result is not statically linked — a dynamic
# binary would be unrunnable in `scratch`.
# Cache mounts are namespaced PER TARGET ARCH: with one shared id (the
# default — the target path), the amd64 and arm64 legs run two cargos
# unpacking into the same registry cache concurrently, and the loser dies
# with `.cargo-ok: File exists`. Distinct ids cost one cold build per arch
# and remove the race entirely.
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=cargo-registry-${TARGETARCH} \
    --mount=type=cache,target=/app/target,id=fluxum-target-${TARGETARCH} \
    case "${TARGETARCH:-amd64}" in \
      amd64) TARGET_TRIPLE=x86_64-unknown-linux-musl ;; \
      arm64) TARGET_TRIPLE=aarch64-unknown-linux-musl ;; \
      *) echo "unsupported TARGETARCH '${TARGETARCH}'" >&2; exit 1 ;; \
    esac \
 && cargo build --release --package fluxum-server \
      --target "${TARGET_TRIPLE}" \
 && mkdir -p /out/release \
 && cp "target/${TARGET_TRIPLE}/release/fluxum-server" /out/release/fluxum-server \
 && file /out/release/fluxum-server | grep -Eq 'static-pie linked|statically linked'
# (`ldd` is NOT a reliable static gate: glibc's ldd prints "statically
# linked" and exits 0 for static-PIE binaries, so only `file` is checked.)

# Rootfs-prep stage
#
# Assembles everything the scratch image needs with the right ownership:
# passwd/group entries for the non-root user, the CA bundle for outbound
# TLS, and the /var/lib/fluxum data tree (the config's relative `./data`
# default resolves there via WORKDIR). Done here because scratch has no
# shell to run RUN steps. Pinned to $BUILDPLATFORM: its output is
# arch-neutral text files and empty directories, so there is no reason to
# run it under qemu.
FROM --platform=${BUILDPLATFORM:-linux/amd64} alpine:3.22 AS rootfs
RUN apk add --no-cache ca-certificates \
 && echo 'fluxum:x:1000:' > /rootfs-group \
 && echo 'fluxum:x:1000:1000::/var/lib/fluxum:/sbin/nologin' > /rootfs-passwd \
 && mkdir -p /rootfs-data /rootfs-tmp \
 && chown -R 1000:1000 /rootfs-data \
 && chmod 1777 /rootfs-tmp

# Runtime stage — scratch: zero OS packages, zero CVEs.
FROM scratch

# OCI image metadata. `org.opencontainers.image.version` is the canonical
# place container registries read the version from and must match the
# pushed tag.
LABEL org.opencontainers.image.title="Fluxum" \
      org.opencontainers.image.description="General-purpose realtime database: in-memory MVCC engine with a tiered on-disk store, server-side modules, live query subscriptions, and a read-only Postgres wire endpoint" \
      org.opencontainers.image.version="0.3.0" \
      org.opencontainers.image.vendor="HiveLLM" \
      org.opencontainers.image.source="https://github.com/hivellm/fluxum" \
      org.opencontainers.image.documentation="https://github.com/hivellm/fluxum/blob/main/docs/DEPLOYMENT.md" \
      org.opencontainers.image.licenses="Apache-2.0"

# User database (so `USER fluxum` resolves) + directory skeleton with
# ownership. scratch has no mkdir/chown — everything arrives via COPY.
COPY --from=rootfs /rootfs-passwd /etc/passwd
COPY --from=rootfs /rootfs-group /etc/group
COPY --from=rootfs --chown=1000:1000 /rootfs-data /var/lib/fluxum
COPY --from=rootfs --chown=1000:1000 /rootfs-tmp /tmp

# CA bundle for any TLS peer the operator points Fluxum at (backups to
# object storage, server peers behind TLS, …).
COPY --from=rootfs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# The static binary — the only executable in the image — and the annotated
# example config for reference (`docker cp`), since the container runs on
# built-in defaults + FLUXUM_* environment overrides.
COPY --from=builder --chmod=0755 /out/release/fluxum-server /usr/local/bin/fluxum-server
COPY config/config.example.yml /etc/fluxum/config.example.yml

# All durable state lives under the volume; the config's relative ./data
# default resolves here via WORKDIR.
VOLUME /var/lib/fluxum
WORKDIR /var/lib/fluxum
USER fluxum

# A container must bind beyond loopback to be reachable. TLS posture is the
# operator's call (SEC-059): either terminate TLS here via server.tls.*, or
# run plaintext ONLY on an encrypted/trusted network with
# FLUXUM_SERVER_ALLOW_PLAINTEXT=true (see docs/DEPLOYMENT.md §TLS).
ENV FLUXUM_SERVER_TCP_HOST=0.0.0.0
ENV TZ=UTC

# Expose default ports.
#   15800 — HTTP: admin API + console + /rpc (FluxRPC over Streamable HTTP).
#   15801 — FluxRPC binary TCP.
EXPOSE 15800 15801

# Health check via the binary itself (`--healthcheck` probes GET /health on
# the loopback HTTP listener and exits 0/1). No shell exists in this image,
# so exec-form with the absolute path is mandatory.
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/fluxum-server", "--healthcheck"]

ENTRYPOINT ["/usr/local/bin/fluxum-server"]
