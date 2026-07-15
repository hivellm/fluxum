# Proposal: phase2_simd-kernels-dispatch

## Why
SIMD on CRC, hashing, codec, and predicate scans is the hardware-adaptivity pillar; runtime dispatch lets one binary exploit AVX-512 or NEON while staying bit-identical to scalar.

## What Changes
Land scalar reference kernels, the runtime dispatch layer with FLUXUM_SIMD override, SIMD variants behind it, and the ISA-matrix scalar-parity CI.

## Impact
- DAG task: T2.10
- Affected specs: SPEC-016 (HWA-030..053), SPEC-013 (TST-100/101)
- PRD requirements: FR-111, FR-112, NFR-14
- Affected code: crates/fluxum-core (simd), .github/workflows (ISA matrix)
- Depends on: T2.1
- Breaking change: NO
- User benefit: hot-path speedups with provable correctness on every ISA
