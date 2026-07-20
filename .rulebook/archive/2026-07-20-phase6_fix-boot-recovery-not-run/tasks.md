## 1. Implementation
- [x] 1.1 In `boot::assemble`, `create_dir_all` the commit-log directory, open a `CheckpointRepo`, and run `fluxum_core::checkpoint::recover(&store, &repo, &commit_log_dir, shard)` before constructing the pipeline; log the recovery outcome (last_tx_id, checkpoint, replayed records) when non-empty (crates/fluxum-server/src/boot.rs). Fixed in commit `f88f913`.

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation — the recovery call carries a comment explaining the failure it prevents (empty store over a non-empty log → STG-015 rejects every commit) and why the directory is created before replay.
- [x] 2.2 Write tests covering the new behavior — covered by the conformance corpus `reconnect-resync` scenario: a real `restart_server` on the same data dir, then assert committed rows survived and post-restart commits still reach a reconnected client.
- [x] 2.3 Run tests and confirm they pass — scenario green; full fluxum-server and fluxum-core suites green.

## Note
The recovery machinery itself (checkpoint adoption, log replay, next-tx-id, auto-inc high-water,
STG-021/031 fallback) was already implemented and unit-tested in fluxum-core under the storage
tasks; this task is purely the missing wire-up in the served binary's boot path.
