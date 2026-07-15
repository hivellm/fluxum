# Proposal: phase4_fulltext-match-query-bm25

## Why
With the positional inverted index in place (phase2_fulltext-inverted-index), the query path needs a MATCH operator that routes through it and ranks by BM25 — otherwise the index is unusable, the same way the B-tree index sat unused before the query planner. This delivers the actually-useful full-text: boolean AND-of-terms, phrase match, term-prefix (typeahead), and BM25 relevance ordering for the initial snapshot, with boolean matching for live diffs. It mirrors the spatial predicate integration (SUB-011): index-routed, no full-scan fallback, snapshot-ranked / boolean-live. The query-surface additions touch the module API that freezes at T6.1, so they are time-boxed.

## What Changes
Extend the query SQL subset with a MATCH predicate over #[fulltext] columns, combinable with ordinary AND filters (SPEC-018): AND-of-terms by default, quoted phrase match via stored positions, trailing-* term prefix via BTreeMap range scan. Evaluate by intersecting posting lists then applying residual filters. Add BM25 scoring (configurable k1/b) with ORDER BY SCORE DESC LIMIT n top-N for InitialData/one-off queries (ranking is snapshot-only per SUB-013), and an opt-in _score projection. For TxUpdate fan-out, match delta rows by re-analyzing and testing the boolean predicate, with a term->plans pruning index keeping fan-out O(P_matched + S_matched). Reflect the MATCH/phrase/prefix surface and _score in /schema and SDK codegen.

## Impact
- Governing spec: SPEC-019 (§5 query surface, §6 BM25 ranking, §7 introspection/SDK) — docs/specs/SPEC-019-fulltext-search.md
- Related specs: SPEC-005 (SUB-010 subset, SUB-011 index-routed predicate, SUB-013 snapshot-only ranking, SUB-022/023 pruning), SPEC-018 (residual filters via access path), SPEC-011 (SDK/query surface — T6.1 freeze)
- New PRD requirements: FR-95 (full-text query), FR-96 (BM25 ranking)
- Affected code: crates/fluxum-core/src/sql/ (lexer/parse MATCH + phrase + prefix), crates/fluxum-core/src/subscription/mod.rs (index-routed eval, term-pruning, boolean live match), BM25 scorer, /schema + SDK codegen
- Depends on: phase2_fulltext-inverted-index (index + stats + analyzer); phase4_index-aware-query-planner (residual filter access path)
- Sequencing: query-surface change — SHOULD land before the T6.1 module API freeze
- Breaking change: NO (additive MATCH operator + optional SCORE)
- User benefit: rankable keyword search (boolean + phrase + typeahead, BM25) native in the DB — the minimally-acceptable full-text that conventional DBs never delivered, without an external engine
