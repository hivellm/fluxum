## 1. Implementation
- [x] 1.1 Parse `ephemeral` as a `#[fluxum::table]` argument (mutually exclusive with public/private/global — so an ephemeral table is never global, DMX-012); modeled as `TableAccess::Ephemeral` (memory-only + client-visible) — no TableSchema field ripple (DMX-010, DMX-012; crates/fluxum-macros/src/table.rs, crates/fluxum-core/src/schema/mod.rs)
- [ ] 1.2 Parse the `expire_after = "..."` table argument and the owner `ConnectionId` binding for ephemeral tables; validate the owner column is a `ConnectionId` (DMX-011; crates/fluxum-macros/src/table.rs) — DEFERRED to the DMX-011 increment (needs per-table ephemeral metadata; see proposal note)
- [x] 1.3 WAL-skipping commit path: the tx pipeline filters ephemeral-table mutations out of the logged diff (`MemStore::is_ephemeral`); the full diff still drives fan-out (DMX-010; crates/fluxum-core/src/txn/mod.rs, crates/fluxum-core/src/store/memstore.rs)
- [x] 1.4 Ephemeral tables excluded from checkpoints (`repo.write`) and thus from recovery, and — being absent from the log — never replicated; they start empty after restart (DMX-010, DMX-012; crates/fluxum-core/src/checkpoint/repo.rs)
- [x] 1.5 Ephemeral insert/delete diffs fan out to subscribers exactly like durable rows (the full `TxDiff` reaches `SubscriptionManager`); ephemeral tables are client-visible via `TableAccess::is_client_visible` (DMX-010; crates/fluxum-core/src/subscription/mod.rs)
- [ ] 1.6 On owner disconnect, delete the connection's ephemeral rows and emit delete diffs via the reducer disconnect lifecycle hook (DMX-011; crates/fluxum-core/src/reducer)
- [ ] 1.7 Drive `expire_after` expiry from the scheduler, dropping expired ephemeral rows with delete diffs (DMX-011; crates/fluxum-core/src/scheduler/mod.rs)
- [x] 1.8 Verification (DMX-010/012 portion): `#[table(ephemeral)]` compiles (trybuild pass); an ephemeral write fans out but the WAL logs only durable tables; an ephemeral-only tx writes no row data yet keeps tx_id gap-free; after a checkpoint+restart the ephemeral table is empty (crates/fluxum-core/tests/ephemeral_tables.rs, crates/fluxum-macros/tests/ui/*). Disconnect/expiry verification lands with 1.2/1.6/1.7.

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
