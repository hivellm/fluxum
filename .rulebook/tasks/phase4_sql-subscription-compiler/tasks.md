## 1. Implementation
- [ ] 1.1 Implement the SQL subset tokenizer/parser: `SELECT * FROM T [WHERE pred] [IN REGION (x,y,w,h)] [WITHIN RADIUS r OF (x,y)] [ORDER BY ...] [LIMIT n]` (FR-30, FR-35, SUB-010)
- [ ] 1.2 Compile to `CompiledPlan`: table filter, equality/range predicates, spatial constraint, visibility rule slot (SUB-011)
- [ ] 1.3 Reject every unsupported construct (JOIN, GROUP BY/HAVING/aggregates, DML, subqueries, CTEs) with error 400 (SUB-012)
- [ ] 1.4 Query-text normalization + query-hash so identical queries share one plan (feeds T4.2 dedup, SUB-020)
- [ ] 1.5 Injection-attempt corpus: malformed and hostile query strings never crash the parser or produce a plan (DAG T4.1 exit; also feeds the T6.6 security audit)
- [ ] 1.6 Verification (DAG exit test): parser + plan unit tests; injection corpus green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
