## 1. Implementation
- [ ] 1.1 Resolve `commit_log_dir` / `checkpoint_dir` / `page_dir` under `data_dir` when they are not explicitly configured (post-merge, so `FLUXUM_STORAGE_DATA_DIR` alone re-roots all three); an explicit value still wins verbatim
- [ ] 1.2 Report the resolved paths (and their provenance: explicit vs derived) in the `/health` config block, like every other derived value
- [ ] 1.3 Audit the remaining CWD-relative defaults (blob store, CDC sink, audit sinks) and re-root them the same way
- [ ] 1.4 Changelog + upgrade note: a deployment that set `data_dir` and relied on the CWD-relative sub-paths must move its directories or pin them explicitly

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation (DEPLOYMENT.md, config examples)
- [ ] 2.2 Write tests covering the new behavior (data_dir-only config places log/checkpoints/pages under it; explicit sub-paths unchanged)
- [ ] 2.3 Run tests and confirm they pass
