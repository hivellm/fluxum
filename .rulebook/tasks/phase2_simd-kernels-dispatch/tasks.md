## 1. Implementation
- [ ] 1.1 Land scalar reference implementations first: CRC32C, hashing, FluxBIN batch encode/decode, batched predicate evaluation (they are the behavioral oracle)
- [ ] 1.2 Implement the runtime dispatch layer selecting AVX-512 / AVX2 / SSE4.2 / NEON / scalar per kernel at boot (FR-111, HWA-030..)
- [ ] 1.3 SIMD kernel variants behind the dispatch layer, replacing scalars without changing behavior (FR-112)
- [ ] 1.4 `FLUXUM_SIMD` override: forced-scalar mode passes the whole workspace suite; forcing an unsupported tier aborts boot with a clear error (HWA-032/HWA-034)
- [ ] 1.5 Per-kernel selection reported in the effective-config boot event and /health (HWA-033)
- [ ] 1.6 ISA-matrix CI workflow (x86-64 + aarch64 runners) running scalar-parity property tests for every selectable variant; no release selects an unexercised variant (HWA-052/HWA-053, NFR-14; family precedent: Vectorizer simd-matrix.yml)
- [ ] 1.7 Verification (DAG exit test): SIMD kernels bit-identical to scalar reference on every supported ISA (FR-112)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
