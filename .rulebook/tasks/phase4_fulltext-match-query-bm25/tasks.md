## 1. Implementation
- [ ] 1.1 Lexer/parser: MATCH predicate over a #[fulltext] column; reject MATCH on unindexed columns (compile-time) and fuzzy ~ / OR-NOT-inside-MATCH / field-boost with 400 (FTS-030/033; sql/lexer.rs, sql/parse.rs)
- [ ] 1.2 AND-of-terms matching: intersect posting lists of analyzed query terms; apply ordinary AND filters as residual conditions via the SPEC-018 access path (FTS-030)
- [ ] 1.3 Term-prefix (typeahead): trailing-* resolves to a BTreeMap range scan over postings, unioning lists (FTS-031)
- [ ] 1.4 Phrase match: quoted group matches adjacent in-order tokens using stored positions; mixed phrase+term queries (FTS-032)
- [ ] 1.5 BM25 scorer with configurable k1 (1.2) / b (0.75) using df/doc-len/avgdl; phrase scored as one synthetic term (FTS-040)
- [ ] 1.6 ORDER BY SCORE DESC + LIMIT n = BM25 top-N for InitialData/one-off only (snapshot per SUB-013/QP-021); opt-in _score projection (FTS-041)
- [ ] 1.7 Live boolean fan-out: re-analyze delta row and test boolean predicate; register query terms in a term->plans pruning index so only relevant plans evaluate, O(P_matched + S_matched) (FTS-042; SUB-022/023)
- [ ] 1.8 Reflect MATCH/phrase/prefix surface + _score in /schema and SDK codegen (FTS-050/052; SPEC-011) — before T6.1 freeze
- [ ] 1.9 Verification: boolean/prefix/phrase return exactly the reference sets; BM25 order matches a reference impl for a known corpus; MATCH rows-scanned bounded by posting lists not table size; SUB correctness property test green with MATCH plans

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
