# Proposal: phase6_fix-duplicate-commit-fanout

## Why
The combined HTTP+TCP server ran TWO commit fan-out loops over one broadcast, so every
subscriber received every `TxUpdate` twice. A duplicated delivery corrupts the SDK-044 refcount
in every client cache: the duplicated insert pins the refcount at 2, an update's delete only
brings it back to 1, and the application is left seeing both the pre-update and post-update row
forever. Found by the SDK conformance corpus (`txupdate-diff`, TST-052) on its first run against
the real server.

## What Changes
Guard `spawn_fanout` with a `fanout_started` AtomicBool on `ShardContext`, the same idempotency
pattern the ephemeral/TTL sweepers already use: both transports request the fan-out on serve, and
only the first call spawns it.

## Impact
- Affected specs: SPEC-005 (SUB-021 fan-out), SPEC-011 (SDK-044 cache refcount)
- Affected code: crates/fluxum-server/src/lib.rs (`spawn_fanout`, `ShardContext`), crates/fluxum-server/tests/fanout.rs
- Breaking change: NO
- User benefit: correct once-only delivery — client caches no longer accumulate phantom rows
