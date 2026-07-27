## 1. Implementation
- [x] 1.1 Node-format extension: overflow-key entry tag (inline order-preserving prefix + full key in a TIER-026 overflow chain), page-format version bump with decode compatibility for existing tags — leaf tags `0..=3` are the (inline|overflow key) × (inline|overflow value) matrix; interior separators mark overflow with bit 15 of `key_len` (never set by v1 writers); `FORMAT_VERSION = 2`
- [x] 1.2 Routing/search/scan/split/bulk_load over prefix-tied keys: full-key comparison faults overflow pages only on prefix collision (`cmp_probe`; the allocation-free hot path returns `NeedsFullKeys` and the caller re-searches on the parsed path); splits and bulk loads keep fan-out ≥ 2 regardless of key length (in-node key bytes bounded at `node_budget/8`); supersede walks key chains like value chains, and a same-key value update reuses the entry's existing key chain
- [x] 1.3 Lift the `max_key` cap on the live path (primary, secondary, `#[unique]`) and restore `recovery_bench` PAYLOAD_BYTES to 2048 as the regression witness — only empty keys and `u32`-overflow lengths are rejected now
- [x] 1.4 Property tests: ordering equivalence vs a resident `BTreeMap` oracle over arbitrary-length keys (incl. shared multi-KB prefixes), CoW snapshot correctness across overflow-key writes
- [x] 1.5 Boot resilience for the format bump: `Pager::open` discards page files this build cannot read (the cold tier is a cache — recovery rebuilds it from checkpoint + commit log), so a version bump never strands a durable database behind a disposable tier; verifying files are left untouched

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation (`tree.rs` module docs "Long keys" + node encoding; SPEC-015 TIER-027 added and the TIER-021 version bits updated; `format.rs` version history)
- [x] 2.2 Write tests covering the new behavior (shared-prefix exactness, CoW snapshots over overflow keys, `bulk_load` with uniformly long keys, the `BTreeMap` oracle property test, and the page-tier discard on boot)
- [x] 2.3 Run tests and confirm they pass (fluxum-core 869/0; workspace green; codespell clean)
