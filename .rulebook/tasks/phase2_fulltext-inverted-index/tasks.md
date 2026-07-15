## 1. Implementation
- [ ] 1.1 Parse #[fulltext(col, language, stop_words, stemming)] in the proc-macro; reject on non-String/Vec<String> columns and on #[encrypted] columns at compile time (FTS-001/002; crates/fluxum-macros/src/table.rs)
- [ ] 1.2 Deterministic analyzer pipeline: Unicode tokenization with positions -> case-fold -> per-language stop-words -> per-language stemming; "simple" analyzer = tokenize+lowercase; versioned AnalyzerId (FTS-010)
- [ ] 1.3 FullTextIndex: positional posting lists term -> [(pk, tf, positions)] in a BTreeMap (prefix-scan ready); doc_len map, per-term df, total_docs/total_len for BM25 (FTS-020)
- [ ] 1.4 Commit-merge maintenance like btree/spatial: insert/delete/update on a private pre-swap copy, atomic TableState swap, rollback discards TxState (FTS-021; crates/fluxum-core/src/index/mod.rs)
- [ ] 1.5 verify_index_integrity analogue: postings/positions/df/doc-len bit-identical to a fresh rebuild over committed rows (FTS-021; STG-007 rule-2)
- [ ] 1.6 Tiered-storage compliance: postings pageable/evictable under memory budget; rebuild-from-rows on recovery behind the 503 "rebuilding" gate (FTS-022; SPEC-015 TIER-050)
- [ ] 1.7 Expose the fulltext index in ColumnSchema/TableSchema (column, analyzer id, BM25 params); analyzer id into __schema_meta__ (FTS-050/051)
- [ ] 1.8 Verification: random insert/update/delete sequence keeps the index rebuild-identical; analyzer determinism across supported languages; a corpus larger than the buffer-pool budget is served with paged postings

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
