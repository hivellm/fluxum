# Proposal: phase2_fulltext-inverted-index

## Why
Conventional DBs handle keyword search poorly (LIKE '%..%' is an unrankable full scan), forcing an external engine. Fluxum's PRD delegated full-text to "Nexus and Vectorizer", but that premise is wrong: Nexus is a graph database and Vectorizer is vector/semantic search — neither provides lexical full-text. So there is a real gap. This task builds the storage-layer foundation for a minimal native lexical FTS: a positional inverted index maintained in the same commit-merge pipeline as the B-tree/spatial indexes, plus the corpus statistics BM25 needs. It mirrors the spatial-index precedent (native index, not a bolt-on) and stays bounded — lexical keyword only, semantic search still delegated to Vectorizer.

## What Changes
Add a #[fulltext(col, language, stop_words, stemming)] table attribute and a FullTextIndex type: a deterministic analyzer pipeline (Unicode tokenization with positions -> case-fold -> stop-words -> stemming per language), positional posting lists (term -> [(pk, tf, positions)]), and BM25 stats (per-doc length, per-term df, doc count). Maintenance rides the commit merge exactly like btree/spatial (copy-on-write TableState swap, rollback discards TxState, bit-identical-to-rebuild invariant). Postings keyed in a BTreeMap to enable term-prefix scans. Follow the tiered-storage rule: postings pageable/evictable, rebuildable from rows on recovery behind the 503 gate. The MATCH query operator, ranking, and subscription integration are the sibling phase4 task.

## Impact
- Governing spec: SPEC-019 (§2 declaration, §3 analyzer, §4 index structure & maintenance) — docs/specs/SPEC-019-fulltext-search.md
- Related specs: SPEC-001 (DM-020 attribute surface, TableSchema), SPEC-002 (index maintenance pipeline, STG-007 rebuild invariant), SPEC-015 (TIER-050 paged/evictable), SPEC-017 (normalize-then-tokenize; no #[encrypted] fulltext), SPEC-010 (analyzer id in __schema_meta__)
- New PRD requirements: FR-95 (native lexical full-text) — and PRD §8 non-goal corrected (Nexus/Vectorizer premise)
- Affected code: crates/fluxum-macros/src/table.rs (#[fulltext] parsing), crates/fluxum-core/src/index/ (new FullTextIndex + analyzer), crates/fluxum-core/src/store/ (commit-merge maintenance, TableState), crates/fluxum-core/src/schema (ColumnSchema/TableSchema)
- Depends on: T1.1 (data-model macros), T2.1 (MemStore/commit merge) — archived; mirrors T2.5/T2.6 spatial indexes
- Breaking change: NO (additive attribute + additive index type)
- User benefit: native, rankable keyword search index — no external Meilisearch/Elastic, no full scans
