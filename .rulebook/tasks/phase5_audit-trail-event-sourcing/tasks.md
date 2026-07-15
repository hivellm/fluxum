## 1. Implementation
- [ ] 1.1 Define the audit query request/response: filter by table + optional row key + time or tx_id range; each result row carries caller, reducer_name, tx_id, timestamp in commit order (OPS-020; crates/fluxum-server/src/admin.rs)
- [ ] 1.2 Add an indexed commit-log read: map (table, row key) and time/tx_id to the segments+offsets that touched it so audit does not full-scan the log (OPS-020; crates/fluxum-core/src/commitlog/segment.rs, record.rs)
- [ ] 1.3 Extend the read to span archived segments (local and, when configured, object-store) so history predating the live log is included — no separate audit store (OPS-020; crates/fluxum-core/src/commitlog/replay.rs)
- [ ] 1.4 Wire the `audit` endpoint into the admin HTTP surface using the RPC-052 envelope, returning ordered results as JSON (OPS-020; crates/fluxum-server/src/admin.rs)
- [ ] 1.5 Access control: reject audit reads from any identity other than admin / registered server-peer (AUTH-062) with a typed error (OPS-021; crates/fluxum-server/src/admin.rs)
- [ ] 1.6 Apply column masking / field-crypto to audit output so masked or encrypted columns never surface plaintext in an audit result (OPS-021; crates/fluxum-core/src/commitlog + SPEC-017 masking path)
- [ ] 1.7 Verification: a row changed by three reducer calls returns exactly those three entries with correct caller/tx_id/timestamp in commit order, across a live+archived segment boundary, with a masked column redacted and a non-admin caller refused

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
