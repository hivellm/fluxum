## 1. Implementation
- [x] 1.1 Add a `fanout_started` AtomicBool to `ShardContext` and make `spawn_fanout` a no-op on the second call (crates/fluxum-server/src/lib.rs), so a combined HTTP+TCP server spawns exactly one fan-out loop over its single commit broadcast — not one per transport. Fixed in commit `1ef44df`.

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation — the guard's rationale is in the `spawn_fanout` doc comment (why duplicate delivery corrupts the SDK-044 refcount).
- [x] 2.2 Write tests covering the new behavior — regression test `both_transports_deliver_each_commit_exactly_once` in crates/fluxum-server/tests/fanout.rs: serve both transports over one `ShardContext`, publish a commit, assert it arrives exactly once.
- [x] 2.3 Run tests and confirm they pass — `cargo test -p fluxum-server --test fanout` green (7 passed); full fluxum-server suite green.
