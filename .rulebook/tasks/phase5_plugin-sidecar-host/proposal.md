# Proposal: phase5_plugin-sidecar-host

## Why
Heavy or optional plugins — a model-based full-text re-ranker, a Vectorizer client — must not live in the Fluxum binary/image (single-binary, no runtime deps, 1 vCPU / 512 MB, idle RSS < 100 MB). The answer is out-of-process plugins: a sidecar process Fluxum calls over RPC, so the model runtime and its weights stay entirely outside the core image. This is process isolation over a wire protocol, NOT WASM/FFI/dlopen, so it stays within the PRD contract. This task implements the sidecar host that the framework core (phase3) declares.

## What Changes
Implement the out-of-process host: a Plugin RPC over the FluxRPC framing family (u32 LE + MessagePack, reusing SPEC-006 transport landed in T5.1), and a generic proxy that implements a capability trait by calling the sidecar. Add per-call timeouts and graceful degradation (ReadPath failure falls back to the base result — e.g. pure BM25 — never an error to the client), a circuit breaker after repeated failures, sidecar authentication (server-peer identity only when explicitly granted; no RLS bypass by default), and fluxum_plugin_sidecar_* metrics. Enforce PLG-021: a sidecar may not bind a WritePath capability (the KeyProvider-to-KMS exception requires runtime key caching so the commit path makes no per-tx network call).

## Impact
- Governing spec: SPEC-020 (§4.2 sidecar host, §3 placement/PLG-021, §7 security) — docs/specs/SPEC-020-plugin-system.md
- Related specs: SPEC-006 (FluxRPC framing reused for Plugin RPC), SPEC-009 (sidecar auth / server-peer), SPEC-012 (plugin metrics), SPEC-011 (constraints — image stays lean)
- New PRD requirements: FR-98 (out-of-process sidecar plugins)
- Affected code: crates/fluxum-core plugin host (sidecar proxy, circuit breaker, timeouts), crates/fluxum-protocol (Plugin RPC messages), crates/fluxum-server (sidecar connection/auth), metrics
- Depends on: phase3_plugin-framework-core (capability registry, placement rules); T5.1 (FluxRPC TCP transport) — archived
- Breaking change: NO (additive host; sidecars are opt-in via config)
- User benefit: model/Vectorizer plugins run as separate processes — the core binary and image stay lean, and a slow/failed sidecar degrades gracefully instead of breaking queries
