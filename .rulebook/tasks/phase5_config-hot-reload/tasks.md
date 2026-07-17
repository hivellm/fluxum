## 1. Implementation
- [x] 1.1 Classify each config key as reloadable vs non-reloadable; reloadable set = log level/format, slow-reducer threshold, reducer rate limits, send-buffer sizes (OPS-040/OPS-041; crates/fluxum-core/src/config/mod.rs)
- [x] 1.2 Add a reload path that re-reads file+env through the existing layered loader, validates, and produces the new effective config with ValueSource (OPS-040; crates/fluxum-core/src/config/mod.rs)
- [x] 1.3 Reject any changed non-reloadable key (ports, storage paths, shard count) with a clear error; reload is all-or-nothing, never partially applied (OPS-041; crates/fluxum-core/src/config/mod.rs)
- [x] 1.4 Atomically publish reloaded values to live consumers: tracing level/format filter, slow-reducer threshold, rate-limiter options, send-buffer sizes (OPS-040; crates/fluxum-core/src/reducer/ratelimit.rs, crates/fluxum-core/src/subscription/sendbuffer.rs)
- [~] 1.5 Trigger reload from SIGHUP and from an admin reload endpoint (OPS-040; crates/fluxum-server/src/main.rs, crates/fluxum-server/src/admin.rs)
  - `POST /config/reload` shipped. SIGHUP is **split to phase6_fluxum-dev-inner-loop-cli 1.9**: the trigger needs `crates/fluxum-server/src/main.rs`, still a T0.1 stub that only `println!`s, and a `#[cfg(unix)]` handler cannot be compiled — let alone tested — on this host (no linux C toolchain for a cross-check). `ShardContext::reload_config()` is the whole operation and is signal-ready; SIGHUP becomes a three-line call site once a real `main.rs` exists.
- [x] 1.6 Re-expose effective reloadable values (and source) in `/health` after reload (OPS-040; crates/fluxum-server/src/admin.rs)
- [x] 1.7 Verification: reload level info→debug takes effect with no restart and /health reflects it; a changed port is rejected with a clear error and the running config is unchanged (no partial apply)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass

## Notes

**1.1 required inventing a key.** OPS-040 names "rate limits" as reloadable, but
`reducer.shard_max_reducers_per_sec` did not exist in `Config` — `RateLimiterOptions` was
built from `::default()` and never read config at all. Listing the key without adding it
would have made the allowlist entry silently never match. Added the key, sourced the
limiter's default from one shared constant, and made the RED-052 guard live-tunable.

**Two bugs the "make it real" work surfaced:**
- `RateLimiter.global` was `Option<Mutex<TokenBucket>>` — the `Option` outside the lock,
  so a guard that booted disabled (`0`) could never be enabled by a reload. Moved the
  `Option` inside: `Mutex<Option<TokenBucket>>`.
- A naive retune (rebuild the bucket at the new rate) refills it to full, making
  `POST /config/reload` a rate-limit bypass anyone could spam. `TokenBucket::retune`
  credits tokens earned under the *old* rate, then clamps to the new capacity.
  `retuning_the_guard_does_not_hand_out_a_free_burst` pins it.

**Boot and reload share one publish path** (`ShardContext::publish_reloadable`), on
purpose: a key that applied on reload but was only read at assembly time would silently
revert on the next restart.

**`subscriptions.send_buffer_bytes` has no live consumer yet.** `SubscriberBuffer` is
constructed by tests only — the transport does not build per-connection buffers yet. The
reload publishes the value to `ShardContext::send_buffer_bytes()`, which is the natural
read point when the transport is built (a per-connection buffer reads it at admission, so
a reload sizes every subsequent connection with no live-swap needed). Nothing is inert:
the value is published and asserted; there is simply no reader in-tree today.

**Coverage 93.07%** (`cargo llvm-cov --workspace --summary-only`), workspace suite green
(102 binaries, 0 failures), clippy clean.
