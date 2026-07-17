# Proposal: phase4_typed-edges-graph-traversal

## Why
Inventories, guilds, and dependency trees are relations, but Fluxum's non-goals explicitly exclude a general JOIN/graph query engine (SPEC-023 §8). Modeling "the items a player owns" or "the members of a guild" today means a join table plus manual lookups; there is no typed relation or traversal primitive. The composite-PK B-tree index already exists (crates/fluxum-core/src/index/btree.rs) and gives exactly the ordered `(from, ...)` prefix scan a point traversal needs. This adds typed directed edges with O(log n + k) neighbor traversal — deliberately scoped to point traversals, not a general JOIN, to respect the no-joins non-goal. Marked P2.

## What Changes
Add `#[fluxum::edge]` to declare a typed directed relation `(from, to, props)` backed by composite-PK-indexed rows, so neighbors of a node are a B-tree prefix scan. `traverse` helpers walk edges in O(log n + k) without any general JOIN engine — point traversals only (neighbors of X), not Cypher/SurrealQL. Edge sets are subscribable like tables, so a client can subscribe to a node's neighbors and receive live diffs as edges are added or removed. This is P2: it borders the no-joins non-goal and stays strictly within point traversals.

## Impact
- Governing spec: SPEC-023 §6 (Typed edges & traversal, DMX-050..051) — docs/specs/SPEC-023-data-model-extensions.md
- Related specs: SPEC-001 (macro surface, composite primary keys), SPEC-008 (B-tree/composite index), and the phase-4 subscription spec the neighbor subscriptions derive from
- New PRD requirements: FR-133 (typed edges & traversal)
- Requirements covered: DMX-050, DMX-051
- Affected code: crates/fluxum-macros (edge macro expansion), crates/fluxum-core/src/index/btree.rs (composite-PK prefix scan for neighbors), crates/fluxum-core/src/schema (edge relation descriptor), crates/fluxum-core/src/subscription (neighbor-set subscriptions)
- Depends on: phase-1 macros and phase-4 subscriptions — both archived
- Breaking change: NO (new opt-in edge declaration; existing tables unaffected)
- Priority: P2 (borders the general-JOIN non-goal; scoped to point traversals only)
- User benefit: model inventories, guilds, and dependency trees as typed relations with O(log n + k) neighbor traversal and live neighbor subscriptions, without a JOIN engine
