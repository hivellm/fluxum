## 1. Implementation

- [ ] 1.1 Establish the `fluxum` CLI command tree (clap subcommands, shared config/connection resolution) replacing the stub (DEV-010, DEV-011, DEV-012; crates/fluxum-cli)
- [ ] 1.2 Implement `fluxum init [--template]` scaffolding a runnable module — schema + reducers + config + a client — that boots with one command (DEV-011; crates/fluxum-cli)
- [ ] 1.3 Implement `fluxum dev` module-crate file watcher that debounces changes and triggers a rebuild (DEV-010; crates/fluxum-cli)
- [ ] 1.4 Wire the rebuild → server restart path via snapshot + log replay around ShardContext assembly (DEV-010; crates/fluxum-cli, crates/fluxum-server/src/lib.rs)
- [ ] 1.5 Regenerate SDK bindings on each successful rebuild so the next client call hits the new code (DEV-010; crates/fluxum-cli, SDK codegen)
- [ ] 1.6 Implement merged module + system log streaming for `fluxum dev` over the admin transport (DEV-010, DEV-012; crates/fluxum-cli, crates/fluxum-server/src/admin.rs)
- [ ] 1.7 Implement `fluxum logs -f` with level/format filters against the structured tracing stream (DEV-012; crates/fluxum-cli)
- [ ] 1.8 Handle rebuild/restart failure surfacing (keep loop alive, print actionable errors, preserve prior running server) (DEV-010; crates/fluxum-cli)
- [ ] 1.9 Wire the boot config into the assembly and trigger reload from SIGHUP: call `logging::init` → `ShardContext::install_config(path, config, log_handle)` at startup, and spawn a `#[cfg(unix)]` `SIGHUP` watcher that calls `ShardContext::reload_config()` (split from phase5_config-hot-reload 1.5, which shipped the reload itself + `POST /config/reload`; the signal half needs the real `main.rs`, which is still a T0.1 stub) (SPEC-025 OPS-040; crates/fluxum-server/src/main.rs)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
