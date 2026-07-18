# Proposal: phase5_audit-trail-event-sourcing

## Why
The commit log already IS a durable, ordered record of every committing reducer call: records in crates/fluxum-core/src/commitlog/record.rs carry the mutation set per transaction, sealed into segments (crates/fluxum-core/src/commitlog/segment.rs) and replayable in commit order (crates/fluxum-core/src/commitlog/replay.rs). Compliance and security reviews routinely ask "who changed this row, and when?" — today that requires an external log pipeline even though the answer is sitting in the commit log. Exposing it as an admin query over the existing log (and archived segments) is nearly free and needs no separate audit store. The admin HTTP surface already exists (crates/fluxum-server/src/admin.rs, RPC-052 envelope, runs under the RLS-bypass server admin identity), so the query has a natural home. SPEC-025 OPS-020/021 specify this surface.

## What Changes
Add an admin `audit` query that, for a given table / row key / time (or tx_id) range, returns the ordered sequence of committing reducer calls that touched it — each with `caller` (Identity), `reducer_name`, `tx_id`, and `timestamp` — by reading the commit log and archived segments, with no separate audit store. To make this efficient rather than a full-log scan, add a lightweight index from (table, row key) and from time/tx_id to the segments/offsets that touched them. Audit reads honor access control: only the admin / server-peer identity may issue them (AUTH-062), and results never expose masked or field-encrypted column plaintext (SPEC-017 crypto/masking is applied to audit output exactly as to normal reads).

## Impact
- Governing spec: SPEC-025 §3 Audit trail / event-sourcing surface (OPS-020, OPS-021) — docs/specs/SPEC-025-operations-multitenancy.md
- Related specs: SPEC-002 (commit log), SPEC-006 (admin RPC surface, AUTH-062 server admin identity), SPEC-017 (field crypto / column masking), SPEC-011 (schema for table/column resolution)
- New PRD requirements: FR-140 (audit trail)
- Requirements covered: OPS-020, OPS-021
- Affected code: crates/fluxum-core/src/commitlog/ (indexed read over segments — record.rs, segment.rs, replay.rs), crates/fluxum-server/src/admin.rs (audit endpoint + envelope), access-control gate reusing the server admin identity
- Depends on: phase2_commitlog (commit log), phase5 admin HTTP API (archived)
- Breaking change: NO (additive read-only query; adds an optional log index)
- User benefit: who-changed-what over any row/time range for compliance and incident forensics, straight from the commit log with no external audit pipeline
