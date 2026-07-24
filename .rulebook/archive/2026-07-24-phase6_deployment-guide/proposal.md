# Proposal: phase6_deployment-guide

## Why
ROADMAP M7's definition-of-done requires a deployment guide (systemd, Docker,
config reference, droplet profile) before 0.1.0. Fluxum is a single static
binary with a directly exposed port posture; operators need the canonical
install/run/upgrade path, and the FR-05 container-awareness claim needs to be
true of the shipped binary, not just of the core library.

## What Changes
docs/DEPLOYMENT.md (install, systemd, docker, config layering, TLS/exposure,
data layout, upgrades with migrate --plan, 1 vCPU/512 MB droplet profile);
deploy/fluxum.service (hardened DynamicUser unit, SIGTERM drain, SIGHUP
reload); deploy/Dockerfile + docker-compose.yml + .dockerignore;
config/config.example.yml rewritten as the complete config reference, pinned
by test; and the FR-05 hardware probe wired into the real boot path
(boot::assemble installs the derived effective config for /health HWA-013;
main.rs sizes the Tokio runtime from the derivation).

## Impact
- Governing docs: ROADMAP M7, SPEC-016 (probe), SPEC-012 OBS-080 (layering),
  SPEC-025 OPS-030/040 (drain/reload), PRD NFR-12
- Affected code: crates/fluxum-server/src/{boot.rs,main.rs} (probe wiring),
  config/config.example.yml, deploy/*, docs/DEPLOYMENT*.md
- Tests: fluxum-core/tests/config_example.rs (reference completeness),
  fluxum-server/tests/boot_probe.rs (boot installs the derivation)
- Breaking change: NO
- User benefit: a verbatim-followable path from binary to a healthy node on
  systemd or Docker, with container limits honored and every config key
  documented and drift-proof.
