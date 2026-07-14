## 1. Implementation
- [ ] 1.1 Implement `SubscriptionManager`: register/unsubscribe CompiledPlans with server-assigned query_ids; `Subscribe` (batch), `SubscribeSingle`, `Unsubscribe` (FR-30, FR-31, SUB-001..SUB-006)
- [ ] 1.2 Post-commit fan-out loop producing incremental TxUpdate diffs; InitialData identical to a direct CommittedState query
- [ ] 1.3 Query-hash dedup: N identical (normalized) queries share exactly one CompiledPlan - one evaluation + one FluxBIN encoding for all subscribers; per-subscriber work limited to a refcounted-buffer enqueue (SUB-020/SUB-023)
- [ ] 1.4 Value-level plan pruning via `search_args` (plans indexed by equality-filter values) + `table_watchers` fast-path skip for commits touching no watched tables - cost O(matching plans), never O(clients) (SUB-021/SUB-024/SUB-040)
- [ ] 1.5 ORDER BY / LIMIT applied to InitialData only; diffs unordered and unlimited (FR-34, SUB-013)
- [ ] 1.6 Admission control: max_subscriptions_per_connection and max_compiled_plans rejections with typed 429, leaving existing subscriptions intact (SUB-044)
- [ ] 1.7 Verification (DAG exit test): fan-out correctness + dedup/pruning perf tests (1000 clients x 1000 distinct values = 1 plan evaluation, 1 encode per commit; compile-once profiling shows zero SQL parsing after registration)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
