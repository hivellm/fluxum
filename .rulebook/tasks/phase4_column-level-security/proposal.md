# Proposal: phase4_column-level-security

## Why
"Return raw data only to users with permission" is the read-side half of field-level security, and it is exactly what PostgreSQL does with column privileges (GRANT SELECT (col)) plus dynamic masking (PostgreSQL Anonymizer). Fluxum's RLS is whole-row only — visible() returns bool and never rewrites a column (crates/fluxum-core/src/subscription/mod.rs:773). This task adds the orthogonal per-column authorization + masking decision and applies it uniformly across every read surface, so unauthorized callers get a masked value while authorized ones (owner/role/server-peer) get the raw value.

## What Changes
Implement SPEC-017 §6: resolve a per-column authorized decision from #[column_grant(select=public|owner|"role"|server_peer)] against ctx.identity/roles/server-peer, and substitute a masked value (#[masked(null|redact|ciphertext|hash)]) on the read path when unauthorized. Apply masking uniformly to InitialData, TxUpdate diffs, one-off queries, and HTTP reads; compose with row-level #[visibility] so a masked-column change still fans out to authorized subscribers and leaks nothing (presence/ordering) to unauthorized ones. Feed the authorized flag into the phase3 crypto on_read hook so decryption happens only when granted. Extend /schema JSON with transform/grant/mask metadata (key names only) and the schema hash; handle migration interaction via __schema_meta__.

## Impact
- Governing spec: SPEC-017 §6 (field-level security) + §7 (introspection) + §8 (migration) — docs/specs/SPEC-017-column-transforms.md
- Related specs: SPEC-005 (subscription fan-out, RLS composition), SPEC-009 (AUTH-062 server-peer, AUTH-070 roles), SPEC-010 (schema migration, __schema_meta__), SPEC-011 (schema JSON / SDK codegen), SPEC-013 (PostgreSQL parity harness)
- New PRD requirements: FR-91 (field-level security — masking/authorization half)
- Affected code: crates/fluxum-core/src/subscription/mod.rs (read-path masking + diff safety), one-off query + HTTP /query path, crates/fluxum-server (/schema JSON), migration/__schema_meta__
- Depends on: phase1_column-transforms-type-surface (grant/mask schema), phase3_field-level-crypto (on_read hook + authorized flag + engine verify counters), T4.x subscription manager + RLS
- Also absorbs the read-projection follow-ups split out of phase3_field-level-crypto: the `<field>_verified` sibling for `#[signed]` columns (CT-034), Prometheus export of the phase-3 transform counters (CT-014/034), and `#[signed(by = <Identity column>)]` per-identity keys (CT-037 [P2]). The phase-3 task delivered the crypto executors (ECIES + Ed25519 by=server), the write/read hooks, and server-peer-default authorization.
- Breaking change: NO (additive; columns without a grant default to public)
- User benefit: Postgres-equivalent column privileges + dynamic masking — raw values only to permitted callers, across every read path
