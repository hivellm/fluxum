# Test-coverage policy & justified residuals

**Policy (2026-07-16):** the target is **100% line coverage on production code**; >90% is the
hard floor, never the goal. Measured with `cargo llvm-cov --workspace` locally (no CI). Gaps are
closed with behavior tests — asserting a specific diagnostic, error, or state transition — never
with padding. What cannot be covered is listed here with a reason; nothing is silently ignored.

**Current standing:** **90.01% lines** — 2026-07-28, gate command below (PG + SpacetimeDB
drivers live), after phase8_integrated-admin-dashboard + the chat-latency and MMO samples.
The dashboard surface (console views, `POST /rows`, `POST /backup`, richer `/sessions`,
the demo module's `Player`/`move_player`) initially pulled the standing to 89.49%; the
floor was recovered with behavior suites, each pinning a named diagnostic: the `/rows`
JSON→RowValue converter (41%→93.5%: every scalar arm, narrowing errors, Option/List
recursion, decimal/hex parsers), the admin dispatch arms (`tests/admin_arms.rs`: audit pk
coercion across every supported key type on one composite key, checkpoint/backup/sessions/
bans refusals, reducer argument policing, `/health` lifecycle statuses, `/schema` FTS
rendering), the demo reducers (`tests/demo_reducers.rs`: move_player spawn/move/clamp,
chat/task validations), the full metrics exposition (`fluxum-core/tests/metrics_render.rs`:
every reason label + the replication-peer and CDC-sink conditional blocks, 112→3 missed),
the config `validate()` refusals (63→5), and the CLI dispatch arms
(`fluxum-cli/tests/cli_dispatch_arms.rs`, 95→~45). Measured as one accumulated profdata:
the full-workspace gate run plus the new suites merged via `cargo llvm-cov --no-report`
(no production code changed between runs — only tests were added). Prior: 90.13% —
2026-07-26, after phase7_csharp-sdk (T7.6, completing the FIVE-SDK set): the Rust
surface grew only by `generate/csharp.rs` (the `fluxum generate --lang csharp` codegen),
covered by its emit/determinism/unmodelled-type unit tests — the SDK itself is C# (out of
the Rust llvm-cov scope; validated by its own 13-test xUnit suite, 11 conformance
scenarios + 2 codec tests). Prior: 90.11% after phase7_go-sdk (T7.5): the Rust surface
grew only by `generate/go.rs`, covered by its unit tests — the SDK itself is Go (its own
`go test` suite). Prior: 90.08% after phase7_python-sdk (T7.4): the Rust
surface grew only by `generate/python.rs`, covered by its unit tests — the SDK itself is
Python (out of Rust llvm-cov scope; its own 20-test pytest suite). Prior:
90.05% after phase7_replica-sets-failover COMPLETE (T7.2 — the custom Raft-style
election, demote-on-fence, ReplicaStale admission, the /health replication object, the
SDK replica-set failover, and the replication DST; ~2,300 more lines). The floor holds:
election.rs is covered by its pure-function unit suites (decide_vote/majority/quorum/
staleness/jitter) plus the failover/demote e2e; the election TASK's async reconnect arms
(follow/candidacy timing, peer rotation) are the honest residual — timing-driven loops the
deterministic suites cannot pin without production seams (category 8), exercised live by
the drill. Prior: 90.09% after phase7_replication-streaming COMPLETE (phase C — the REP-021
semi-sync visibility barrier, REP-022 quorum-loss block/degrade, REP-031 fencing on any
channel + stale-batch rejection, divergence refusal). Prior: 90.00% after T7.1 phases A+B
(~1,600 more lines: the replica apply path and stream helpers pinned by oracle-exact
suites). Prior: 90.01% after phase7_backup-object-storage-archive (~1,000 more lines:
seekable-zstd framing at 90%+, the S3 SigV4 store at 92%, remote push/restore/PITR at
84%+ via the in-process S3 wire fixture; the margin is thin — the standing debt now
includes `cli/backup.rs` remote/error plumbing (62%) and remote.rs error arms, which the
next backup-adjacent task should factor into testable seams). Prior: 90.14% after
phase7_backup-pitr (~1,000 new lines: `fluxum_core::backup`
create/verify/restore/PITR at 82%+, the CLI dispatch + wrappers recovered by the
`backup_cli` dispatch suite after an initial dip to 89.92%; residuals are error-arm
plumbing in `cli/backup.rs` and the `main.rs` drain-checkpoint glue, category 9).
Prior: 90.16% after phase6_deployment-guide (the FR-05 probe wired into the real
boot path and covered by `boot_probe.rs`; `config_example.rs` pins the config reference;
the new `main.rs` runtime-sizing lines are category-9 binary glue); 90.09% after
phase6_admin-web-console (the console module, the DEV-031 gate, and
the `/console/watch` stream are covered by their unit + integration suites; the embedded
`console.html` is an asset, not instrumented lines); 90.08% after
phase6_seed-fixtures-migrate-plan (the plan/verdict matrix and the seed path are covered
by their suites; that dip from 90.23% was the new CLI glue — `migrate.rs`'s cargo-spawn
wrapper and the `run()` dispatch arms — the same category-9 shape as the standing debt).
**The floor holds**, recovered from the ~89.8% T6.6 breach:
90.02% → 90.09% → 90.23% → 90.08% → 90.09% → 90.16% → 90.14% → 90.01% → 90.00% → 90.09% → 90.05% → 90.08% → 90.11% → 90.13%. The **standing debt items below remain open**: (a) the
`fluxum dev` watch/restart loop body + `logs` network glue (T6 inner-loop); (b) the
`fluxum-bench load`/`fanout` command handlers in `main.rs` + `load.rs` sustained paths the
short-window smokes don't reach (the `/metrics`-scrape and counter parsing ARE covered);
(c) now also the `fluxum migrate --plan` cargo-spawn wrapper. **The next task touching
fluxum-cli or fluxum-bench should factor those into pure functions and cover them.** Prior
standings: 90.23% (testkit), 90.09%, 90.02%, ~89.8% (T6.6), 89.93% (T6 inner-loop), 90.12%
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
