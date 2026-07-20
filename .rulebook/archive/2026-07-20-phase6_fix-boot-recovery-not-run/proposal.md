# Proposal: phase6_fix-boot-recovery-not-run

## Why
`boot::assemble` built a fresh `MemStore` and opened the commit log but NEVER RAN RECOVERY, so a
restarted server came up empty over a non-empty commit log — and the log's STG-015 monotonicity
check then rejected every new commit (`tx_id does not strictly increase`). The result was the
worst of both: no durability across restart AND no new writes accepted. The STG-030 machinery
(newest verified checkpoint + log replay + next-tx-id + auto-inc high-water) existed and was
fully unit-tested in fluxum-core; it was simply never wired into the served binary. Found by the
SDK conformance corpus (`reconnect-resync`, TST-052), which restarts a real server on the same
ports and data directory.

## What Changes
Run `fluxum_core::checkpoint::recover` in `boot::assemble` BEFORE the pipeline opens: create the
log directory if absent (recovery replays it first, and an absent dir is an I/O error, not an
empty log), adopt the newest verified checkpoint, replay the log past it, and install the
recovered state so tx-id assignment resumes at `last_tx_id + 1`.

## Impact
- Affected specs: SPEC-002 (STG-015 monotonic tx-id, STG-030 recovery, STG-040 auto-inc)
- Affected code: crates/fluxum-server/src/boot.rs
- Breaking change: NO
- User benefit: a restarted server recovers its committed data and keeps accepting writes — the
  core durability promise, now actually reachable from the shipped binary
