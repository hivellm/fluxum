# Proposal: phase6_fix-fanout-query-id-stamp

## Why
The commit fan-out never stamped the per-connection `query_id` on delivered `TxUpdate`s:
`QueryDelta.subscribers` was `Vec<u128>` (connection ids only) and the fan-out hardcoded
`query_id: 0`. A delivered row therefore carried no handle back to the subscription that produced
it — exactly what an SDK needs for SDK-044 refcount attribution and for `Unsubscribe` to know
which rows to drop. Query ids are assigned PER CONNECTION (SUB-001), so the stamp must be the id
THAT connection holds, not a global one. Surfaced while building the conformance corpus's
`unsubscribe` scenario (TST-052).

## What Changes
`QueryDelta.subscribers` becomes `Vec<(u128, u32)>` — each target with the `query_id` that
connection assigned. `on_commit` looks up each subscriber's id (`query_id_of`); the server fan-out
groups targets by id and stamps each delivered `TableUpdate.query_id` (rows still encoded once per
delta, SUB-024). A `QueryDelta::connections()` helper keeps existing call sites readable.

## Impact
- Affected specs: SPEC-005 (SUB-001 per-connection query_id, SUB-024 shared encoding), SPEC-011 (SDK-044)
- Affected code: crates/fluxum-core/src/subscription/mod.rs (`QueryDelta`, `on_commit`, `query_id_of`), crates/fluxum-server/src/lib.rs (fan-out grouping + stamping)
- Breaking change: NO (internal type; wire `TableUpdate.query_id` was already 0-filled)
- User benefit: SDKs can attribute rows to subscriptions — the prerequisite for correct
  overlapping-subscription refcounts and for unsubscribe to release the right rows
