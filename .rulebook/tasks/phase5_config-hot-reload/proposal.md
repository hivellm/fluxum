# Proposal: phase5_config-hot-reload

## Why
Config is loaded once at boot: the layered YAML + `FLUXUM_*` loader (crates/fluxum-core/src/config/mod.rs) resolves every key with a recorded ValueSource and hands the frozen struct to the server. Changing an operational knob — log level, slow-reducer threshold, reducer rate limits (crates/fluxum-core/src/reducer/ratelimit.rs token buckets), send-buffer sizes (crates/fluxum-core/src/subscription/sendbuffer.rs) — today means a full process restart, which (until phase5_graceful-drain lands) is a downtime window just to raise log verbosity or tighten a rate limit. The admin surface (crates/fluxum-server/src/admin.rs) already serves `/health`, a natural place to expose effective values. SPEC-025 OPS-040/041 define a bounded hot-reload subset plus strict rejection of non-reloadable keys.

## What Changes
Make a defined subset of config reloadable at runtime — log level/format, slow-reducer threshold, reducer rate limits, and send-buffer sizes — via SIGHUP or an admin reload endpoint. On reload, the loader re-reads the file/env, validates, and atomically publishes the new values to the live components (tracing filter, rate limiter, send buffers); `/health` re-exposes the effective values and their source. Non-reloadable keys (ports, storage paths, shard count) are rejected on reload with a clear error and the reload is all-or-nothing — never partially applied, so a bad reload leaves the running config untouched.

## Impact
- Governing spec: SPEC-025 §5 Config hot-reload (OPS-040, OPS-041) — docs/specs/SPEC-025-operations-multitenancy.md
- Related specs: SPEC-012 (observability / effective-config in /health, OBS-080/081), SPEC-004 (reducer rate limits), SPEC-006 (admin surface)
- New PRD requirements: FR-142 (config hot-reload)
- Requirements covered: OPS-040, OPS-041
- Affected code: crates/fluxum-core/src/config/mod.rs (reload + reloadable-key classification), crates/fluxum-server/src/admin.rs (reload endpoint + effective values in /health), crates/fluxum-server/src/main.rs (SIGHUP), live consumers: tracing filter, crates/fluxum-core/src/reducer/ratelimit.rs, crates/fluxum-core/src/subscription/sendbuffer.rs
- Depends on: phase0 config loader (archived), phase5 server
- Breaking change: NO (additive; boot behavior unchanged)
- User benefit: tune log verbosity, rate limits, and buffers in production with no restart; unsafe key changes fail loudly instead of silently
