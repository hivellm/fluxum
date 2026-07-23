# Test-coverage policy & justified residuals

**Policy (2026-07-16):** the target is **100% line coverage on production code**; >90% is the
hard floor, never the goal. Measured with `cargo llvm-cov --workspace` locally (no CI). Gaps are
closed with behavior tests — asserting a specific diagnostic, error, or state transition — never
with padding. What cannot be covered is listed here with a reason; nothing is silently ignored.

**Current standing:** **90.23% lines** — 2026-07-23, gate command below (PG + SpacetimeDB
drivers live), after phase6_reducer-test-simulation-kit (`fluxum-testkit` lands ~96%-covered
by its own author-facing suite). **The floor is recovered** from the ~89.8% breach the T6.6
leva recorded and has grown three levas straight: 90.02% (optimistic mutations) → 90.09%
(offline persistence) → 90.23% (testkit). The **standing debt items below remain open**
(they are why the margin is not wider, category 9 aside): (a) the `fluxum dev`
watch/restart loop body + `logs` network glue (T6 inner-loop); (b) the `fluxum-bench
load`/`fanout` command handlers in `main.rs` + `load.rs` sustained paths the short-window
smokes don't reach (the `/metrics`-scrape and counter parsing ARE covered). **The next task
touching fluxum-cli or fluxum-bench should still factor those into pure functions and cover
them.** Prior standings: 90.09%, 90.02%, ~89.8% (T6.6), 89.93% (T6 inner-loop), 90.12%
(P0 parity campaign). The P0-B growth briefly
dipped the floor to 89.96%; recovered by covering the pipelining trait defaults +
`ratio_interval` arms and by the **PG-gated baseline smoke** (`baseline_postgres_runs_all_workloads`,
`FLUXUM_BENCH_PG_URL` — the `Db::Pg` half and the real LISTEN/NOTIFY hop, formerly a named
residual). History: 96.3% at the 2026-07-16 campaign (pre-T6.3, ~22.8k lines); the T6.3
parity-harness growth dropped the floor to 88.96% (2026-07-21), recovered on 2026-07-22 by
(a) an in-process behavior test for the baseline app (`baseline/server.rs` `serve_on` seam:
router + handlers + WebSocket fan-out + the SQLite `db.rs` half over real sockets) and
(b) categories 10/11 below — generated bindings and sync-gated vendored copies are counted at
their source of truth, not double-billed. Largest honest residuals: `fluxum-bench/src/main.rs`
CLI (category 9), `boot.rs`/`main.rs` entry points.

## How proc-macro coverage works here

Proc-macro code executes at *dependent-crate compile time*, so trybuild UI tests exercise it but
attribute no coverage. Every expansion function therefore also has `#[cfg(test)]` unit tests
calling `expand*(TokenStream)` directly (`crates/fluxum-macros/src/*`), asserting emitted tokens
or `compile_error!` messages. trybuild remains the diagnostics-format golden layer.

## Justified residual categories

1. **Defensive invariant guards** — `unreachable!`, `debug_assert!`, and error arms that the
   public API makes unreachable by construction (e.g. pager split with <2 entries, subscription
   candidate indexes out of sync with `queries`, `field.ident == None` after syn parsing).
   They exist to fail loudly on engine bugs, which is exactly why tests cannot reach them.
2. **Infeasible allocations** — paths requiring values > `u32::MAX` bytes/items (FluxBIN length
   prefixes, page `raw_len`): >4 GiB test allocations are not reasonable.
3. **Machine/platform-dependent arms** — SIMD dispatch for ISAs this machine lacks (NEON,
   no-HW-CRC fallback), HWA-055 kernel self-check failure paths (kernels cannot fail on correct
   hardware), OS-specific branches (`seek_write` returning 0, drive-root paths, pre-epoch clock),
   hardware-probe fallbacks. SIMD *correctness* is guarded by scalar-parity property tests
   (FR-112) rather than per-ISA line coverage.
4. **`fluxum-dst/src/sim.rs`** — the deterministic-simulation harness's uncovered lines are all
   divergence `panic!` arms: they fire only when the storage engine is actually buggy. A passing
   DST run *not* executing them is the success criterion.
5. **Const-fn test fixtures** — `const fn` table constructors evaluated at compile time carry no
   runtime instrumentation.
6. **`tracing` field expressions** — field closures never evaluate without an active subscriber.
7. **Real-time timing tests** — `tick_drift.rs` self-skips under `LLVM_PROFILE_FILE`
   (instrumentation distorts real-time semantics); its RED-020 stall/reset arms have
   coverage-safe equivalents in `schedule_deferred.rs`.
8. **Race-window arms** — branches requiring a precise interleaving that cannot be forced
   deterministically without production seams (e.g. the sweeper's phase-2 re-verify racing a
   rewrite, writer-task death mid-route, `wait_durable` post-`changed()` actor exit).
9. **Binary entry points** — `fluxum-server/src/main.rs`, `fluxum-cli` stubs,
   `fluxum-bench/src/main.rs` (the harness CLI: exercised by the release parity runs, whose
   numbers a debug/instrumented build must never produce).
10. **Generated third-party bindings** — `fluxum-bench/src/spacetimedb_bindings/` is
    `spacetime generate` output (TST-097): a full client API surface of which the harness uses
    exactly the six `BenchClient` operations. Exclude with
    `--ignore-filename-regex spacetimedb_bindings`; the used paths are covered through the
    env-gated `spacetimedb_smoke` test against the live pinned server.
11. **Vendored protocol copies in the published SDK** — `sdks/rust/src/protocol/*` are
    byte-identical copies of `crates/fluxum-protocol/src/*` (the published crate cannot depend
    on the unpublished one); `sdks/rust/tests/protocol_sync.rs` fails the gate on any byte
    difference. The behavior is covered once, at the source of truth (~95–100 % per file);
    counting the copies again is double-billing the same lines. Exclude with
    `--ignore-filename-regex "sdks[/\\\\]rust[/\\\\]src[/\\\\]protocol"`.

Gate command of record:
`cargo llvm-cov --workspace --ignore-filename-regex "spacetimedb_bindings|sdks[/\\\\]rust[/\\\\]src[/\\\\]protocol"`
(with `FLUXUM_BENCH_STDB_URL` set when the pinned SpacetimeDB container is up, so the
TST-097 side driver is exercised live, and `FLUXUM_BENCH_PG_URL` set when the docker PG is
up, so the baseline's PostgreSQL half runs).

Per-line detail lives in the per-area reports of the coverage campaign (2026-07-16); when one of
these categories gains a test seam (e.g. injectable fs faults), the corresponding lines move out
of this list.
