## 1. Implementation
- [ ] 1.1 Parse `ephemeral` as a `#[fluxum::table]` argument and reject `ephemeral` + `global` together; carry an ephemeral flag on TableSchema/TableAccess (DMX-010, DMX-012; crates/fluxum-macros/src/table.rs)
- [ ] 1.2 Parse the `expire_after = "..."` table argument and the owner `ConnectionId` binding for ephemeral tables; validate the owner column is a `ConnectionId` (DMX-011; crates/fluxum-macros/src/table.rs)
- [ ] 1.3 Add a WAL-skipping commit path in MemStore so ephemeral-table writes never append to the commit log or checkpoint set (DMX-010; crates/fluxum-core/src/store/memstore.rs)
- [ ] 1.4 Bypass ephemeral tables in commit-log replay/recovery so they start empty after restart and are never replicated (DMX-010, DMX-012; crates/fluxum-core/src/commitlog)
- [ ] 1.5 Fan out ephemeral insert/update/delete diffs to subscribers on commit exactly like durable rows (DMX-010; crates/fluxum-core/src/subscription/mod.rs)
- [ ] 1.6 On owner disconnect, delete the connection's ephemeral rows and emit delete diffs via the reducer disconnect lifecycle hook (DMX-011; crates/fluxum-core/src/reducer)
- [ ] 1.7 Drive `expire_after` expiry from the scheduler, dropping expired ephemeral rows with delete diffs (DMX-011; crates/fluxum-core/src/scheduler/mod.rs)
- [ ] 1.8 Verification: an `ephemeral` `Cursor` table bound to `ConnectionId` with `expire_after` compiles; high-rate updates write nothing to the commit log; rows vanish on disconnect/expiry and after restart the table is empty

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
