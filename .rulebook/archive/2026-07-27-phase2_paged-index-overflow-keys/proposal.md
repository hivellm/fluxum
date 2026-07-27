# Proposal: phase2_paged-index-overflow-keys

## Why

The paged B-tree (`store/pager/tree.rs`) stores keys inline in node pages, so
key length is capped at `node_budget/2 - 10` (~2 KB at 4 KiB pages) to
guarantee interior fan-out ≥ 2 — without that bound, uniformly huge keys
degrade fan-out to 1 and `bulk_load`'s level building stops shrinking
(livelock), which is exactly what the first 1.3 attempt hit on
`recovery_bench` (2 KiB indexed payloads).

The cap is Postgres-parity behavior (PG rejects index rows over ~page/3;
ours is ~page/2, more generous), and the resident `imbl` indexes' unlimited
keys were accidental generosity. Still, since the phase2 cutover the cap now
applies to **primary keys, secondary index keys, and `#[unique]` values** on
the live path, and a workload that legitimately indexes multi-KB
`Str`/`Bytes` values gets a schema error where the resident store accepted
it.

## What Changes

Key-overflow support in the paged tree: a node entry stores a bounded,
order-preserving key *prefix* inline (routing/comparison fast path) plus the
full key in an overflow chain (TIER-026 machinery already exists for
values), with full-key comparison faulted in only on prefix ties. This keeps
fan-out healthy for any key length while preserving exact ordering — range
scans and `#[unique]` exact matches stay correct. Node payload encoding
gains a new entry tag; the page format version bumps (the format froze at
G5, so this is a versioned extension, decode side keeps compatibility with
tag-0/1 pages).

## Impact

- Affected specs: SPEC-015 (TIER-021 node payload encoding, TIER-026,
  TIER-050)
- Affected code: `crates/fluxum-core/src/store/pager/tree.rs` (encode /
  decode / route / search / scan / split / bulk_load / supersede),
  `format.rs` (version bump)
- Depends on: phase2_tiered-live-store-integration
- Breaking change: NO wire impact; on-disk page format extension (pages are
  in-process scratch rebuilt from WAL+checkpoint, but the format version
  still bumps honestly)
- Risk: MEDIUM-HIGH — node-format change under the most correctness-critical
  structure; ordering property tests are the guardrail
- User benefit: indexing / `#[unique]` over multi-KB values works instead of
  erroring; removes the one behavioral regression vs the resident store

## Notes

Until this lands, oversized keys fail with the documented ~2 KB cap error
(`tree.rs::max_key` docs). `recovery_bench` payload sits at 1900 B
deliberately to stay under the cap — restore it to 2048 when this task
lands, as its own witness.
