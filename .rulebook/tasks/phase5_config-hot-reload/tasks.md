## 1. Implementation
- [ ] 1.1 Classify each config key as reloadable vs non-reloadable; reloadable set = log level/format, slow-reducer threshold, reducer rate limits, send-buffer sizes (OPS-040/OPS-041; crates/fluxum-core/src/config/mod.rs)
- [ ] 1.2 Add a reload path that re-reads file+env through the existing layered loader, validates, and produces the new effective config with ValueSource (OPS-040; crates/fluxum-core/src/config/mod.rs)
- [ ] 1.3 Reject any changed non-reloadable key (ports, storage paths, shard count) with a clear error; reload is all-or-nothing, never partially applied (OPS-041; crates/fluxum-core/src/config/mod.rs)
- [ ] 1.4 Atomically publish reloaded values to live consumers: tracing level/format filter, slow-reducer threshold, rate-limiter options, send-buffer sizes (OPS-040; crates/fluxum-core/src/reducer/ratelimit.rs, crates/fluxum-core/src/subscription/sendbuffer.rs)
- [ ] 1.5 Trigger reload from SIGHUP and from an admin reload endpoint (OPS-040; crates/fluxum-server/src/main.rs, crates/fluxum-server/src/admin.rs)
- [ ] 1.6 Re-expose effective reloadable values (and source) in `/health` after reload (OPS-040; crates/fluxum-server/src/admin.rs)
- [ ] 1.7 Verification: reload level info→debug takes effect with no restart and /health reflects it; a changed port is rejected with a clear error and the running config is unchanged (no partial apply)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
