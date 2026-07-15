## 1. Implementation
- [x] 1.1 Implement the SQL subset tokenizer/parser: `SELECT * FROM T [WHERE pred] [IN REGION (x,y,w,h)] [WITHIN RADIUS r OF (x,y)] [ORDER BY ...] [LIMIT n]` (FR-30, FR-35, SUB-010) — `crate::sql::{lexer, parse}`: closed tokenizer (rejects `;`, comments, quoted idents, out-of-subset operators) + recursive-descent parser over the exact grammar
- [x] 1.2 Compile to `CompiledPlan`: table filter, equality/range predicates, spatial constraint, visibility rule slot (SUB-011) — `sql::compile` resolves the table/columns, coerces every literal to its `FluxType` (range-checked), builds the predicate closure, validates spatial clauses against the `#[spatial]` declaration (SPX-022), and leaves the `rls` slot for T4.3
- [x] 1.3 Reject every unsupported construct (JOIN, GROUP BY/HAVING/aggregates, DML, subqueries, CTEs) with error 400 (SUB-012) — named `REJECTED_KEYWORDS` diagnostics at ident/keyword/trailing positions; all map to `codes::MALFORMED`
- [x] 1.4 Query-text normalization + query-hash so identical queries share one plan (feeds T4.2 dedup, SUB-020) — `normalize()` (uppercase keywords, canonical spacing/literals, case-sensitive identifiers) + `QueryHash` via the platform-stable xxHash64 kernel (HWA-042); `equalities` exposed structurally for the T4.2 SUB-023 pruning index
- [x] 1.5 Injection-attempt corpus: malformed and hostile query strings never crash the parser or produce a plan (DAG T4.1 exit; also feeds the T6.6 security audit) — `tests/sql_injection_corpus.rs`: ~75 hostile inputs (SQLi payloads, encoding tricks, every SUB-012 form) all 400; long/deep inputs bounded (8 KiB cap, flat grammar); control chars inside string literals correctly treated as data
- [x] 1.6 Verification (DAG exit test): parser + plan unit tests; injection corpus green — `tests/sql_compiler.rs` (11 tests) + `tests/sql_injection_corpus.rs` (4 tests) green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation (module docs on `crate::sql` incl. the injection posture; grammar in `sql::parse`; per-item rustdoc on `CompiledPlan`)
- [x] 2.2 Write tests covering the new behavior (compiler suite + injection corpus)
- [x] 2.3 Run tests and confirm they pass (full workspace suite green locally; fmt + clippy clean) — CI deferred per the no-Actions directive (quota)
