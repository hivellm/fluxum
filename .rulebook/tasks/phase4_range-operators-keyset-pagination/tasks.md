## 1. Implementation
- [ ] 1.1 Lexer + parser: add <, >, <=, >= as top-level AND conditions; keep OR/!=/LIKE/NULL rejected with the existing 400 diagnostic (QP-030; sql/lexer.rs, sql/parse.rs:95)
- [ ] 1.2 Fold a same-column range pair (col >= a AND col <= b) into one closed/half-open interval equivalent to BETWEEN for pushdown (QP-030)
- [ ] 1.3 Type-check comparison operators via existing schema-typed coercion (int->float widening); reject on Bool/Option/List at compile time (QP-032; sql/mod.rs:273/338)
- [ ] 1.4 Parse the AFTER (value, pk) cursor clause; allow only with ORDER BY on an indexed column (QP-040)
- [ ] 1.5 Append primary key as the final ORDER BY term (implicitly if omitted) for a total order and unambiguous cursor (QP-041)
- [ ] 1.6 Translate AFTER cursor into an index lower bound (value > c OR (value = c AND pk > k)) through the planner access path; no OFFSET (QP-040)
- [ ] 1.7 Pagination applies to snapshot (InitialData/one-off) only; subsequent pages via new SubscribeSingle/OneOffQuery with advanced cursor (QP-042)
- [ ] 1.8 Reflect extended operator set in SDK codegen + /schema query documentation (QP-031; SPEC-011) — before T6.1 freeze
- [ ] 1.9 Verification: paging a large table returns each row exactly once, no gaps/overlaps under a stable snapshot; page N+1 rows-scanned approx page size independent of N; range operators push down and return correct sets

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
