# Proposal: phase5_reducer-idempotency-keys

## Why

Once the SDK supports optimistic mutations backed by an offline replay queue, a
reducer call retried after a lost ack would double-apply — resend
`transfer_funds` after a dropped connection and the funds move twice. Today
`ReducerCall` (`crates/fluxum-protocol/src/messages.rs:130`) carries no way to
mark a call as a retry of an earlier one, and the reducer engine
(`crates/fluxum-core/src/reducer/engine.rs`, `crates/fluxum-core/src/reducer/mod.rs`)
executes every submission unconditionally. An optional client-assigned
`idempotency_key` with a bounded, durable dedup window makes submission
exactly-once: a replayed key returns the original `ReducerResult` without
re-running the reducer body. The pieces to hang this on already exist — the
schedule worker (`crates/fluxum-core/src/scheduler/mod.rs`, `schedule_worker`)
can prune the window, and transaction durability lives in
`crates/fluxum-core/src/txn/mod.rs`.

## What Changes

Add an optional `idempotency_key` to `ReducerCall`. Before executing a reducer,
the shard checks a bounded durable dedup window scoped per `(Identity, reducer)`;
if the key is already present it returns the original `ReducerResult` and skips
execution. The dedup record is written as part of the reducer commit so it
survives crash/restart, is bounded by count and age (configurable), and is
pruned by the existing schedule worker. This pairs with the SDK offline queue,
which attaches a stable key to every queued call so reconnect replay is safe.

## Impact

- Governing spec: SPEC-021 (Client Sync & Resilience) — docs/specs/SPEC-021-client-sync-resilience.md §4
- Related specs: SPEC-006 (FluxRPC wire), SPEC-003/reducer engine specs
- New PRD requirements: FR-122 (reducer idempotency)
- Requirements covered: CS-030, CS-031, CS-032
- Affected code: crates/fluxum-protocol/src/messages.rs (optional `idempotency_key` on `ReducerCall`), crates/fluxum-core/src/reducer/{mod.rs,engine.rs} (pre-execute dedup check + return cached `ReducerResult`), crates/fluxum-core/src/scheduler/mod.rs (prune the dedup window), crates/fluxum-core/src/txn/mod.rs (durability of the dedup record across commit)
- Depends on: phase3 reducer engine (archived)
- Breaking change: NO (`idempotency_key` is optional/additive; must land before G5 wire freeze). Pairs with phase6_sdk-optimistic-mutations-offline-queue
- User benefit: a call retried after a lost ack applies exactly once — no double transfers, double inserts, or duplicated side effects on reconnect.
