# Proposal: phase5_storage-subdirs-follow-data-dir

## Why

`storage.data_dir` is documented as "Root data directory", but the three
sub-directories do **not** derive from it — each carries its own literal,
CWD-relative default (`crates/fluxum-core/src/config/mod.rs`):

```rust
data_dir:        "./data",
commit_log_dir:  "./data/log",
checkpoint_dir:  "./data/checkpoints",
page_dir:        "./data/pages",
```

So an operator (or a test) that sets only `storage.data_dir` —
`FLUXUM_STORAGE_DATA_DIR=/var/lib/fluxum`, the documented knob — still writes
the commit log, checkpoints, and cold-tier pages into `./data/*` relative to
the **process working directory**. The data of record silently lands
somewhere other than where the operator pointed the database, survives
restarts in the wrong place, and is invisible to backup/volume mounts
configured against `data_dir`.

Found the hard way: `crates/fluxum-cli/tests/dev_smoke.rs` sets `DATA_DIR` +
`COMMIT_LOG_DIR` to a fresh temp dir; its pages nonetheless accumulated in
`crates/fluxum-cli/data/pages/` (checked into nobody's expectations, left
behind across runs), which only surfaced when a page-format bump made those
stale files unreadable. The same shape bit the Python SDK conformance runner
(fixed there by pinning CWD + `PAGE_DIR` per scenario).

## What Changes

Make each storage sub-directory default to a path **under** `data_dir`,
while an explicitly configured sub-directory still wins:

- `commit_log_dir` → `<data_dir>/log`
- `checkpoint_dir` → `<data_dir>/checkpoints`
- `page_dir` → `<data_dir>/pages`

Resolution happens after the env/file/profile layers merge (so
`FLUXUM_STORAGE_DATA_DIR` alone re-roots all three), and the effective paths
are reported in `/health`'s config block with their provenance, like every
other derived value. Also audit the other CWD-relative defaults (`blob`,
`cdc`, audit sinks) for the same pattern.

## Impact

- Affected specs: SPEC-012 (config precedence/provenance), SPEC-002 /
  SPEC-015 (on-disk layout), SPEC-025 (deployment: volume mounts)
- Affected code: `crates/fluxum-core/src/config/mod.rs` (defaults +
  post-merge resolution), `docs/DEPLOYMENT.md`, `config/*.yml` examples
- Breaking change: YES (behavioral) — a deployment that sets `data_dir` and
  relies on the *current* CWD-relative sub-paths will find its data under
  `data_dir` after upgrading; must be called out in the changelog with the
  migration (move the directories, or set the sub-paths explicitly)
- Risk: MEDIUM — path resolution touches every durable artifact; the
  mitigation is that explicit configuration keeps its exact meaning
- User benefit: `data_dir` finally means what it says — one knob places the
  whole database, so volume mounts, backups, and disk sizing stop silently
  missing the commit log and cold tier

## Notes

Discovered while implementing `phase2_paged-index-overflow-keys`; that task
made boot resilient to unreadable page files (the cold tier is a cache), but
the misplacement itself is a config bug and is tracked here rather than
smuggled into a storage-format change.
