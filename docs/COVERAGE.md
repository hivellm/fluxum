# Test-coverage policy & justified residuals

**Policy (2026-07-16):** the target is **100% line coverage on production code**; >90% is the
hard floor, never the goal. Measured with `cargo llvm-cov --workspace` locally (no CI). Gaps are
closed with behavior tests — asserting a specific diagnostic, error, or state transition — never
with padding. What cannot be covered is listed here with a reason; nothing is silently ignored.

**Current standing:** 96.3% lines workspace-wide (845 uncovered of ~22.8k); every remaining
uncovered line falls into one of the justified categories below.

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
9. **Binary entry points** — `fluxum-server/src/main.rs`, `fluxum-cli` stubs.

Per-line detail lives in the per-area reports of the coverage campaign (2026-07-16); when one of
these categories gains a test seam (e.g. injectable fs faults), the corresponding lines move out
of this list.
