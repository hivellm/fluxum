## 1. Implementation
- [ ] 1.1 Complete the ROADMAP M0 workspace layout: add the `crates/fluxum-bench` crate (parity/benchmark harness home, T6.3) and the `sdks/rust` placeholder crate, and register both as workspace members (crates/fluxum-core/-macros/-protocol/-server/-cli already exist)
- [ ] 1.2 Align CI toolchain with the PRD section 9 constraint: workflows honor `rust-toolchain.toml` (nightly, edition 2024) instead of pinning stable; fmt + clippy `-D warnings` on all targets/features
- [ ] 1.3 Confirm workspace lints (`unwrap_used`/`expect_used`/`undocumented_unsafe_blocks` = deny) apply to every member incl. the new crates
- [ ] 1.4 Repo hygiene verified: codespell workflow + `.codespellrc` (already landed), CHANGELOG in Keep-a-Changelog format (already landed) - keep green
- [ ] 1.5 Verification (DAG T0.1 exit test): green pipeline (fmt + clippy + nextest) on the skeleton crates on Linux/macOS/Windows (NFR-09)
- [ ] 1.6 Gate G0 input: `cargo test` green on 3 OS

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
