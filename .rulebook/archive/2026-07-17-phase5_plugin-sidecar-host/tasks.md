## 1. Implementation
- [x] 1.1 Plugin RPC message set over FluxRPC framing (u32 LE + MessagePack); capability call/response envelopes (PLG-031; SPEC-006) — landed in 8f298be (crates/fluxum-protocol/src/plugin_rpc.rs)
- [x] 1.2 Generic sidecar proxy implementing a capability trait by calling the sidecar endpoint (PLG-031) — SidecarProxy impls ScoreReranker/Retriever/Fusion; one serialized blocking connection per binding, redials on any transport failure
- [x] 1.3 Per-call timeout + graceful degradation: ReadPath failure/timeout falls back to base result, never a client error (PLG-031) — deadline re-armed to remaining budget per read; proxy returns PluginError, the existing subscription call sites keep the base BM25 list; Fusion degrades to built-in RRF in place
- [x] 1.4 Circuit breaker after repeated failures; hot open/close; fluxum_plugin_sidecar_errors_total{plugin, reason} (PLG-031) — 5 consecutive failures open for a (configurable) cooldown, half-open trial closes/re-opens; errors metered by reason, exposed on /metrics + GET /plugins
- [x] 1.5 Sidecar authentication; server-peer identity only when explicitly granted; no RLS bypass by default (PLG-031/061) — shared token in Hello handshake (never logged/reported); proxy granted no identity, opaque pk bytes + call-site #[visibility]/filters keep RLS intact
- [x] 1.6 Enforce PLG-021: sidecar may not bind a WritePath capability; KeyProvider-to-KMS exception requires runtime key caching (no per-tx network call) (PLG-021) — enforced at PluginRegistry::build via Capability::sidecar_allowed (landed with the framework core; key_provider/stream_sink bind but carry no ReadPath wire)
- [x] 1.7 Verification: with a reranker/retriever sidecar stopped or timing out, queries return the base BM25 result within budget, breaker opens, metrics increment, no client error — plugin_sidecar.rs end-to-end over the real MATCH path against a real TCP fake sidecar

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation — module docs in sidecar.rs, SPEC-020 §4.2 YAML example gains the token field, BoundHost/BoundPlugin doc comments updated
- [x] 2.2 Write tests covering the new behavior — crates/fluxum-core/tests/plugin_sidecar.rs (22 tests) + admin_api /plugins+/metrics sidecar assertions
- [x] 2.3 Run tests and confirm they pass — full fluxum-core/server/protocol suites green (72 suites), clippy clean, sidecar.rs line coverage 93%
