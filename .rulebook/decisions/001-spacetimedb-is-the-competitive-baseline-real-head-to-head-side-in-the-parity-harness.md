# 1. SpacetimeDB is the competitive baseline — real head-to-head side in the parity harness

**Status**: proposed
**Date**: 2026-07-21
**Related Tasks**: phase0_parity-spacetimedb-baseline

## Context

The T6.3 parity harness compares Fluxum against PostgreSQL/SQLite only (NFR-11); "vs SpacetimeDB" impressions came from framing, not data (analysis F-008, docs/analysis/parity-benchmark-corrections/). SpacetimeDB's own published benchmarks are in-process vs SQLite — no over-the-network harness exists anywhere.

## Decision

User decision (2026-07-21): add a real SpacetimeDB side to fluxum-bench and treat SpacetimeDB as the baseline Fluxum must reach. Demo app implemented 1:1 as a SpacetimeDB WASM module, driven via the published spacetimedb-sdk through the side-agnostic workload::Side trait; report gains a competitive-baseline ratio block (fluxum/spacetimedb, target ≥ 1.0× per workload class) kept separate from the NFR-11 PG verdicts. Materialized as phase0_parity-spacetimedb-baseline; phase0_parity-report-honesty consumes it.

## Alternatives Considered

- Reframe the narrative as PostgreSQL parity only and let the sqlite side mirror SpacetimeDB's published methodology (rejected — user wants a measurable head-to-head, not a mirror)

## Consequences

Both products run the same demo app over real sockets through their published SDKs on the same machine — an honest comparison SpacetimeDB itself does not publish. SPEC-013 §10 gains a new TST id (TST-097 proposed); regression guard tracks the ratios informationally and floors each class once it first reaches ≥ 1×. Classes below 1× become recorded findings with measured deltas.
