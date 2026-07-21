## 1. Implementation
- [x] 1.1 Decide and pin the SpacetimeDB deployment for the bench box (Win10): Docker image vs native binary, exact version pinned; document its durability/fsync configuration and match honesty with the Fluxum side per TST-090; record server version, SDK crate version, and module toolchain in the report metadata
- [x] 1.2 Implement the demo app as a SpacetimeDB Rust module mirroring the Fluxum demo 1:1: `Task`/`ChatMessage` tables (same fields/indexes), `add_task`/`send_chat` reducers, channel-filtered chat subscription; publish to the local standalone and generate Rust client bindings (`spacetime generate`)
- [x] 1.3 Implement `spacetimedb_side` (`workload::Side` + `BenchClient`) over the published SpacetimeDB Rust SDK: add_task/send_chat awaited to reducer ack; subscribe_chat via subscription callback; hot_read as client-cache lookup (symmetric to Fluxum's live view); load_my_data as fresh-subscription initial sync returning row count; wire into `main.rs` side selection and smoke-test all six BenchClient ops against a live server
- [x] 1.4 Report + spec: add the competitive-baseline ratio block (fluxum/spacetimedb per class: write, e2e p99, hot, cold, mixed; target ≥ 1.0×) separate from the NFR-11 PG verdicts; regression guard (TST-095) tracks the ratios informationally and floors each class once it first reaches ≥ 1×; amend SPEC-013 §10 (new TST-097) and the PRD with the baseline requirement
- [x] 1.5 Verification (exit test): full `fluxum-bench report` run with the spacetimedb side on the same machine/protocol as the other sides (runs/pinning per P0-C rigor); committed report includes the fluxum-vs-spacetimedb table with SDK/protocol asymmetries documented honestly; every class where Fluxum is below 1× gets a recorded finding with the measured delta so closing the gap is trackable work, not vibes

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
