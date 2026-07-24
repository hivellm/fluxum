# Fluxum container image (deployment guide: docs/DEPLOYMENT.md).
#
# Build from the REPOSITORY ROOT (the build context must see the workspace):
#   docker build -f deploy/Dockerfile -t fluxum:latest .
#
# The runtime honors container resource limits: the boot-time hardware probe
# (SPEC-016, FR-05) reads cgroup CPU/memory limits, so `--cpus` / `--memory`
# flow into the `auto` derivations (worker threads, shards, memory budget)
# with no extra configuration.

# --- build stage -----------------------------------------------------------
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p fluxum-server

# --- runtime stage -----------------------------------------------------------
FROM debian:bookworm-slim
# curl serves the HEALTHCHECK; ca-certificates for any TLS peer the operator
# points Fluxum at.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /var/lib/fluxum --shell /usr/sbin/nologin fluxum \
    && install -d -o fluxum -g fluxum /var/lib/fluxum

COPY --from=build /src/target/release/fluxum-server /usr/local/bin/fluxum-server
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

EXPOSE 15800 15801
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s \
    CMD curl -fsS http://127.0.0.1:15800/health || exit 1
ENTRYPOINT ["fluxum-server"]
