# Proposal: phase4_index-aware-query-planner

## Why
Fluxum has a complete composite B-tree index (memcomparable keys, equality/range/prefix scans, MVCC-consistent, public Snapshot::index_scan at crates/fluxum-core/src/store/committed.rs:325) but the query path never uses it: InitialData and one-off queries iterate the entire primary row map and apply a per-row predicate closure (crates/fluxum-core/src/subscription/mod.rs:482), and CompiledPlan carries no index reference (crates/fluxum-core/src/sql/mod.rs:94). A filtered read like a marketplace listing (category = X AND price BETWEEN a AND b ORDER BY price LIMIT 50) costs O(rows in table) instead of O(log n + k) even with #[index(btree(category, price))] declared. This is the single highest-leverage query optimization: the index primitive is already built and tested, only the planner wiring is missing. Internal change — does not touch the wire or the frozen module API.

## What Changes
Add a rule-based access-path planner that runs at query compile time and annotates CompiledPlan with FullScan | IndexScan { index_id, bounds }, a residual filter for non-pushed-down conditions, an ordered_by_index flag, and cursor bounds. Split WHERE into index bound conditions (equality prefix + single next-column range) and residual conditions; push bounds into Snapshot::index_scan; when the index order matches ORDER BY, skip the in-RAM sort and turn LIMIT into an index-ordered top-N that stops after n authorized rows. Apply RLS/masking within the scan before counting toward LIMIT. Add a /query/explain admin surface. Transparency is guaranteed: IndexScan and FullScan always return the same row set (residual filter). Scope is the snapshot read path (InitialData + one-off query); TxUpdate delta evaluation and value-level fan-out pruning are unchanged.

## Impact
- Governing spec: SPEC-018 (§2 selection, §3 pushdown, §4 index-ordered ORDER BY/LIMIT, §7 CompiledPlan) — docs/specs/SPEC-018-query-planner.md
- Related specs: SPEC-005 (SUB-010/013/020/021 subscription evaluation), SPEC-001 (DM-030/031 index guarantees), SPEC-002 (index_scan), SPEC-017 (CT-041 masking within scan)
- New PRD requirements: FR-93 (index-aware query planning)
- Affected code: crates/fluxum-core/src/sql/mod.rs (CompiledPlan, planner), crates/fluxum-core/src/subscription/mod.rs (snapshot evaluator branch on access path), crates/fluxum-core/src/store/committed.rs (index_scan consumer), crates/fluxum-server (/query/explain)
- Depends on: T4.1 (SQL compiler) + T4.2 (subscription manager) — archived; index primitive already built
- Breaking change: NO (additive CompiledPlan fields; selection is an optimization, correctness unchanged)
- User benefit: complex filtered/sorted reads become index range scans + index-ordered top-N instead of full table scans — the difference between a scalable and an unscalable marketplace listing
