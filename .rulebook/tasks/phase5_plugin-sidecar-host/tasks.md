## 1. Implementation
- [ ] 1.1 Plugin RPC message set over FluxRPC framing (u32 LE + MessagePack); capability call/response envelopes (PLG-031; SPEC-006)
- [ ] 1.2 Generic sidecar proxy implementing a capability trait by calling the sidecar endpoint (PLG-031)
- [ ] 1.3 Per-call timeout + graceful degradation: ReadPath failure/timeout falls back to base result, never a client error (PLG-031)
- [ ] 1.4 Circuit breaker after repeated failures; hot open/close; fluxum_plugin_sidecar_errors_total{plugin, reason} (PLG-031)
- [ ] 1.5 Sidecar authentication; server-peer identity only when explicitly granted; no RLS bypass by default (PLG-031/061)
- [ ] 1.6 Enforce PLG-021: sidecar may not bind a WritePath capability; KeyProvider-to-KMS exception requires runtime key caching (no per-tx network call) (PLG-021)
- [ ] 1.7 Verification: with a reranker/retriever sidecar stopped or timing out, queries return the base BM25 result within budget, breaker opens, metrics increment, no client error

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
