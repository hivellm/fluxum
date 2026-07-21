# Proposal: phase0_parity-spacetimedb-baseline

## Why

User decision (2026-07-21): **SpacetimeDB is the competitive baseline Fluxum must reach**, and
that requires a real, measured head-to-head — not a methodological mirror. Today the parity
harness has no SpacetimeDB side at all (`crates/fluxum-bench/src/main.rs` knows only
`fluxum | postgres | sqlite`) — analysis finding **F-008** in
`docs/analysis/parity-benchmark-corrections/`. Any "vs SpacetimeDB" impression currently comes
from framing, not data; this task makes the claim measurable.

Notably, SpacetimeDB itself has no over-the-network comparison harness — its published
benchmarks are in-process vs SQLite (`docs/analysis/spacetimedb-code/09-ops-testing-bench.md`
§4.5: "Provides comparisons between the underlying spacetime datastore, spacetime modules,
and sqlite"; §"Answer for NFR-11": no Postgres performance harness). Our harness will be the
more honest one: **both** products run the same demo app over real sockets through their
published SDKs, same machine, same workload driver.

The harness is already built for this: every side implements the side-agnostic
`workload::Side` / `BenchClient` traits (`crates/fluxum-bench/src/workload.rs:17-56` —
add_task, send_chat, subscribe_chat, prepare_reads, hot_read, load_my_data), so a new side
inherits the identical client behavior, measurement (TST-091), and report pipeline.

## What Changes

- **SpacetimeDB module**: the demo app (tasks + chat) implemented as a SpacetimeDB Rust
  module (compiled to WASM, `spacetime publish`), mirroring the Fluxum demo schema and
  reducers 1:1 — `Task`/`ChatMessage` tables, `add_task`/`send_chat` reducers, channel-filtered
  subscription (`SELECT * FROM ChatMessage WHERE channel = X`).
- **New side driver**: `spacetimedb_side.rs` implementing `Side`/`BenchClient` via the
  published SpacetimeDB Rust client SDK (`spacetimedb-sdk`, bindings via `spacetime generate`):
  reducer calls awaited to ack; subscription push callbacks; `hot_read` = client-cache lookup
  (symmetric to Fluxum's live-view lookup — both SDKs materialize a local cache); `load_my_data`
  = fresh subscription initial sync.
- **Server**: pinned SpacetimeDB version (Docker like the `postgres:17` side, or native
  binary — decided and recorded in 1.1), honest durability settings documented on both sides
  per TST-090.
- **Report**: a new **competitive-baseline ratio block** (fluxum/spacetimedb per workload
  class, target ≥ 1.0× = parity to reach), kept separate from the NFR-11 PG-parity verdicts so
  neither gate pollutes the other; regression guard tracks the ratios and floors each class
  once it first reaches ≥ 1×.
- **Spec/PRD**: SPEC-013 §10 amended with the SpacetimeDB baseline (new TST id), PRD gains
  the explicit competitive-baseline requirement.

## Impact

- Affected specs: SPEC-013 §10 (new TST-097 SpacetimeDB competitive baseline), PRD (baseline
  requirement note)
- PRD requirements: makes the "SpacetimeDB-class" claim measurable; feeds Gate G6 evidence
- Affected code: crates/fluxum-bench/src/{spacetimedb_side.rs (new), main.rs, report.rs},
  new SpacetimeDB module crate/dir for the demo app
- Depends on: nothing hard to build (can proceed in parallel with P0-A/P0-B);
  `phase0_parity-report-honesty` consumes its output for the committed report; final numbers
  meaningful after `phase6_memstore-structural-sharing` lands
- Breaking change: NO
- User benefit: the product's positioning claim becomes a number on a page — "reaches
  SpacetimeDB" is measured on every release, with the gap per workload class tracked until
  closed
