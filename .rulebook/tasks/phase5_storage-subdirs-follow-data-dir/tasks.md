## 1. Implementation
- [x] 1.1 Resolve `commit_log_dir` / `checkpoint_dir` / `page_dir` (and `replication.archive.dir`) under `data_dir` when they are not explicitly configured — `Config::resolve_storage_dirs`, called at the end of `load_with` so `FLUXUM_STORAGE_DATA_DIR` alone re-roots all four; an explicit value wins verbatim (loader provenance for loaded configs, "still equals its built-in default" for hand-built ones), and the pass is idempotent
- [x] 1.2 Report the resolved paths and their provenance: new `ValueSource::Derived`, and a `storage` block on `GET /health` (`ShardContext::storage_paths`) listing `data_dir` + the four resolved paths with `env`/`file`/`derived` sources
- [x] 1.3 Audit the remaining CWD-relative defaults — the four literals above were the only ones; the blob store and CDC sink already derive from `data_dir` at assembly, and no other config key carries a `./` path
- [x] 1.4 Changelog + upgrade note: the behavioral break is called out in `CHANGELOG.md` with the two migration options (move the directories, or pin the old locations explicitly) and how to confirm via `/health`

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation (`docs/DEPLOYMENT.md` §7 data-directory layout — `data_dir` as the single knob, `/health` provenance, pages-are-a-cache backup guidance; `config/config.example.yml` no longer restates the derivable sub-paths)
- [x] 2.2 Write tests covering the new behavior (`data_dir` alone roots every sub-directory; an explicit sub-directory outranks it; hand-built configs resolve and re-resolve idempotently; untouched defaults stay put; end-to-end boot placement + `/health` provenance in `boot_probe.rs`)
- [x] 2.3 Run tests and confirm they pass
