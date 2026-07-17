# Proposal: phase3_crdt-text-column

## Why
The PRD lists collaborative documents as a target use case, yet multi-primary / cross-shard convergence is an explicit non-goal (SPEC-023 §8). Those two facts are only reconcilable if merge happens WITHIN a single authoritative shard: because Fluxum already serializes writes through one writer per shard (the reducer engine at crates/fluxum-core/src/reducer/engine.rs), a CRDT merged inside that single-writer boundary gives concurrent editing without any multi-primary machinery. There is no text-merge column type today (`FluxType` at crates/fluxum-core/src/schema/mod.rs is scalars + Option/Vec). This adds a `CrdtText` column that converges concurrent edits within the shard. It is research-y and marked P2.

## What Changes
Add a `CrdtText` column type that accepts character-level edit ops and merges them deterministically within the single-writer shard, exposing one converged value to all subscribers. Edit ops are expressed as reducer calls (so they ride the existing single-writer serialization) and fan out to subscribers as compact op diffs, not full-document rewrites. Scope is deliberately single-shard: no multi-primary or cross-shard CRDT convergence. This is P2 / research — the op model, deterministic merge, and diff encoding are the exploratory core.

## Impact
- Governing spec: SPEC-023 §7 (CRDT text column, DMX-060..061) — docs/specs/SPEC-023-data-model-extensions.md
- Related specs: SPEC-001 (FluxType/ColumnSchema — new CrdtText type), SPEC-006 (op-diff wire encoding), and the phase-3 reducer-engine spec the op application derives from
- New PRD requirements: FR-134 (CRDT text column)
- Requirements covered: DMX-060, DMX-061
- Affected code: crates/fluxum-core/src/schema/mod.rs (CrdtText FluxType), crates/fluxum-core/src/reducer (op application within the single-writer boundary), crates/fluxum-protocol (compact op-diff encoding), crates/fluxum-core/src/subscription (op-diff fan-out)
- Depends on: phase-3 reducer engine — archived
- Breaking change: NO (new opt-in column type; existing tables unaffected)
- Priority: P2 / research (single-shard CRDT only; multi-primary convergence remains a non-goal)
- User benefit: collaborative text editing with deterministic convergence inside one shard, fanned out as compact op diffs instead of whole-document rewrites
