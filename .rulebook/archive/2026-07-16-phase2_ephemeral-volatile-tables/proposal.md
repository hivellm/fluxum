# Proposal: phase2_ephemeral-volatile-tables

## Why
Presence, live cursors, and typing indicators are a ubiquitous realtime need: they must fan out to subscribers immediately but MUST NOT be durable. Today the only way to model them is a normal `#[fluxum::table]`, so a row like `OnlineUser`/`Cursor` pays the full commit-log append, checkpoint, and replication cost of durable state it never needs — every 30-updates/sec cursor move hits the WAL path in crates/fluxum-core/src/store/memstore.rs and crates/fluxum-core/src/commitlog. The macro's table-argument parser (crates/fluxum-macros/src/table.rs:166-216) accepts only `public`/`private`/`global`/`primary_key`/`partition_by`, with no way to declare a memory-only table. This adds a first-class `ephemeral` table kind that skips durability but keeps live fan-out.

## What Changes
Admit `#[fluxum::table(ephemeral)]`: rows bypass the commit log, checkpoints, and replication and live only in memory, but fan out on commit exactly like normal rows. Ephemeral rows MAY declare `expire_after` and are dropped (with delete diffs) on expiry or on owner disconnect via `ConnectionId` binding. Ephemeral tables MUST NOT be `global`/replicated and are excluded from recovery, so they start empty after a restart. The macro gains an `ephemeral` argument (mutually exclusive with `global`) and an `expire_after`/owner-binding surface; MemStore learns a WAL-skipping commit path; the subscription engine fans out ephemeral diffs like durable ones; the reducer disconnect hook and the scheduler drive expiry cleanup.

## Impact
- Governing spec: SPEC-023 §2 (Ephemeral / volatile tables, DMX-010..012) — docs/specs/SPEC-023-data-model-extensions.md
- Related specs: SPEC-001 (table macro surface, TableAccess), and the phase-2 storage / phase-4 subscription specs the fan-out and store paths derive from
- New PRD requirements: FR-129 (ephemeral/volatile tables)
- Requirements covered: DMX-010, DMX-011, DMX-012
- Affected code: crates/fluxum-macros/src/table.rs (ephemeral arg + expire_after/owner binding), crates/fluxum-core/src/store/memstore.rs (WAL-skip commit path), crates/fluxum-core/src/commitlog (bypass on ephemeral tables), crates/fluxum-core/src/subscription/mod.rs (fan out ephemeral diffs), crates/fluxum-core/src/reducer (on_disconnect cleanup), crates/fluxum-core/src/scheduler/mod.rs (expire_after sweep)
- Depends on: phase-2 storage (MemStore/commit log) and phase-1 macros — both archived
- Breaking change: NO (new opt-in table kind; existing tables unaffected)
- User benefit: presence, cursors, and typing indicators with live fan-out and zero durability cost — no commit-log, checkpoint, or replication overhead
