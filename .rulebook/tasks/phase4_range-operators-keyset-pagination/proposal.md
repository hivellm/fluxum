# Proposal: phase4_range-operators-keyset-pagination

## Why
The subscription/query SQL subset today has only = / IN / BETWEEN / AND (crates/fluxum-core/src/sql/parse.rs:95), so a common filter like listed_at > t is not even expressible, and there is no server-side pagination — only LIMIT head-truncation, no OFFSET/cursor. Real listing queries (marketplace, feeds, leaderboards) need open-ended range filters and stable, cheap paging. This extends the query surface with comparison operators and keyset (seek) pagination, both of which lean on the index-aware planner to stay O(log n + page). The operator/pagination additions touch the module API / query surface that FREEZES at T6.1, so they are time-boxed: land before the freeze or defer to a post-freeze additive revision.

## What Changes
Extend the SQL subset with <, >, <=, >= as top-level AND conditions, folding a same-column pair (price >= a AND price <= b) into a single interval for index pushdown. Add keyset pagination via an AFTER (value, pk) cursor clause usable only with ORDER BY on an indexed column, translated by the planner into an index lower bound (value > c OR (value = c AND pk > k)); the primary key is appended as the final sort term for a total, unambiguous order. OFFSET is deliberately NOT added (linear + unstable). Type-checking reuses the existing schema-typed coercion and rejects comparison operators on Bool/Option/List. SDK codegen and /schema query docs reflect the extended operator set. Pagination applies to the snapshot (InitialData/one-off) only, consistent with SUB-013.

## Impact
- Governing spec: SPEC-018 (§5 range operators, §6 keyset pagination) — docs/specs/SPEC-018-query-planner.md
- Related specs: SPEC-005 (SUB-010 subset, SUB-013 diff semantics), SPEC-006 (OneOffQuery), SPEC-011 (SDK codegen / query surface — T6.1 freeze), SPEC-001 (index order)
- New PRD requirements: FR-94 (keyset pagination)
- Affected code: crates/fluxum-core/src/sql/lexer.rs + parse.rs (operators, AFTER clause), crates/fluxum-core/src/sql/mod.rs (interval folding, cursor → bounds), SDK codegen + /schema docs
- Depends on: phase4_index-aware-query-planner (cursor/interval push down through the access path)
- Sequencing: query-surface change — SHOULD land before the T6.1 module API freeze
- Breaking change: NO (additive operators + optional AFTER clause; existing queries unaffected)
- User benefit: open-ended range filters (price/date) and stable, index-fast pagination for listing UIs — no OFFSET scans, no client-side windowing of huge sets
