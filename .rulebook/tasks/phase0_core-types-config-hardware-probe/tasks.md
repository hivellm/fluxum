## 1. Implementation
- [x] 1.1 Define `FluxumError` in fluxum-core using thiserror, with variants for config, storage, protocol, and auth failures
- [x] 1.2 Add `Identity`, `ConnectionId`, `EntityId`, `Timestamp` newtypes with serde + Display impls
- [x] 1.3 Implement the YAML config loader with `FLUXUM_` env-var overrides (precedence: env > file > built-in default)
- [x] 1.4 Implement the boot-time hardware probe (logical cores, total RAM, cgroup cpu/memory limits) and wire it into adaptive config defaults (SPEC-016; FR-05, FR-113): worker counts, buffer-pool size, queue depths derived from the probe, overridable in config
- [x] 1.5 Implement the `development` profile (`FLUXUM_PROFILE=development`: single shard, auth `none`, pretty logs - FR-04, SPEC-012 acceptance 9) and ship an example `config.yml` documenting every key and its default
- [x] 1.6 Emit the single `effective configuration` boot event: probe inputs + every derived value with its source (`auto`/`config`/`env`) (SPEC-016 HWA-012/HWA-033)
- [x] 1.7 Verification (DAG exit test): unit tests covering config precedence (default vs file vs env override) + probe fallback when limits are absent or a probe fails (HWA-003/HWA-004: no panic, WARN logged)
- [ ] 1.8 Gate G0 input: `cargo test` green on Linux, macOS, and Windows CI

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
