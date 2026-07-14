# Proposal: phase2_simd-kernels-dispatch

## Why
CRC, hashing, row codec, and predicate scans dominate CPU on hot paths; SIMD kernels behind runtime dispatch exploit whatever ISA the host has without changing behavior (FR-112: scalar parity).

## What Changes
Implement SIMD kernels with runtime dispatch (AVX-512/AVX2/SSE4.2/NEON/scalar) for CRC32, hashing, FluxBIN batch codec, and batched predicate evaluation; scalar reference implementations land first and remain the oracle.

## Impact
- DAG task: T2.10
- Affected specs: SPEC-016 (hardware adaptivity and SIMD)
- PRD requirements: FR-111, FR-112, NFR-14
- Affected code: crates/fluxum-core (simd/dispatch module), consumed by fluxum-server
- Depends on: T2.1 (phase2_memstore-mvcc); parallel track behind the dispatch layer
- Breaking change: NO
- User benefit: hot paths automatically use the fastest instruction set available, identical results everywhere
