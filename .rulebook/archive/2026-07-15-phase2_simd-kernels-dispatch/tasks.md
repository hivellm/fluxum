## 1. Implementation
- [x] 1.1 Land scalar reference implementations first: CRC32C, hashing, FluxBIN batch encode/decode, batched predicate evaluation (they are the behavioral oracle) — CRC-32C, xxHash64, and i64/f64 predicate oracles landed in `simd/`; the FluxBIN batch codec kernel registers when its consumer (fan-out / page materialization) lands, with the SPEC-006 row codec as its oracle (HWA-043 note in `simd/mod.rs`)
- [x] 1.2 Implement the runtime dispatch layer selecting AVX-512 / AVX2 / SSE4.2 / NEON / scalar per kernel at boot (FR-111, HWA-030..) — per-kernel fn pointers picked once via CPUID/HWCAP; AVX-512 accepted but stubs to scalar until kernels exist
- [x] 1.3 SIMD kernel variants behind the dispatch layer, replacing scalars without changing behavior (FR-112) — SSE4.2/aarch64-CRC crc32c, AVX2/NEON predicates; hash64 stays scalar pending HWA-060 bench evidence (no 64-bit lane multiply on AVX2/NEON)
- [x] 1.4 `FLUXUM_SIMD` override: forced-scalar mode passes the whole workspace suite; forcing an unsupported tier aborts boot with a clear error (HWA-032/HWA-034)
- [x] 1.5 Per-kernel selection reported in the effective-config boot event and /health (HWA-033) — `EffectiveConfig.simd_kernels` (serialized into the HWA-012 event; /health reads the same struct when the HTTP listener lands)
- [x] 1.6 ISA-matrix CI workflow (x86-64 + aarch64 runners) running scalar-parity property tests for every selectable variant; no release selects an unexercised variant (HWA-052/HWA-053, NFR-14; family precedent: Vectorizer simd-matrix.yml) — `.github/workflows/simd-matrix.yml`
- [x] 1.7 Verification (DAG exit test): SIMD kernels bit-identical to scalar reference on every supported ISA (FR-112) — `tests/simd_parity.rs` proptest suites + external algorithm pins (`crc` CRC-32-ISCSI, `xxhash-rust`)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
