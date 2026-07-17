# SPEC-023 — Data Model Extensions (ephemeral, TTL, rich types, blobs, edges, CRDT)

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 1 (rich types) · Phase 2 (ephemeral, blob) · Phase 3 (TTL, CRDT) · Phase 4 (typed edges) ([DAG](../DAG.md)) |
| **PRD requirements** | FR-15, FR-18, FR-32 (extends); new: FR-129 (ephemeral tables), FR-130 (row TTL), FR-131 (rich column types), FR-132 (blob store), FR-133 (typed edges), FR-134 (CRDT text column) |
| **Requirement prefix** | `DMX-` |
| **Source** | New (Fluxum-native). Adds data-model primitives the realtime workload needs but the closed type universe ([schema/mod.rs](../../crates/fluxum-core/src/schema/mod.rs), macro at [table.rs](../../crates/fluxum-macros/src/table.rs)) cannot express today. |

Keywords are RFC 2119. Requirement IDs `DMX-xxx` are stable. Priority tags: `[P0]`/`[P1]`/`[P2]`.

## 1. Scope

Six model extensions: **ephemeral/volatile tables** (presence, cursors — no durability, auto-expiry),
**row TTL**, **rich column types** (enums/tagged-unions, nested structs), a first-class **blob store**
for large objects, **typed edges** for graph traversal without joins, and a single-shard **CRDT text
column** for collaborative editing. Each is opt-in per table/column and must not perturb the hot path
for tables that do not use it.

## 2. Ephemeral / volatile tables (`DMX-01x`)

### Requirement: Non-durable, auto-expiring state
- **DMX-010** [P1] `#[fluxum::table(ephemeral)]` rows MUST bypass the commit log, checkpoints, and
  replication; they live only in memory and fan out on commit like normal rows.
- **DMX-011** [P1] Ephemeral rows MAY declare `expire_after` and MUST be dropped (with delete diffs) on
  expiry or on owner disconnect (`ConnectionId` binding).
- **DMX-012** [P1] Ephemeral tables MUST NOT be `global`/replicated and MUST be excluded from recovery;
  after restart they start empty.

#### Scenario: Presence without durability
Given `Cursor` is `ephemeral` bound to `ConnectionId` with `expire_after=10s`
When a client updates its cursor 30×/s then disconnects
Then subscribers see live cursor diffs, nothing is written to the commit log, and the cursor row
vanishes on disconnect.

## 3. Row TTL (`DMX-02x`)

### Requirement: Time-to-live expiration for durable tables
- **DMX-020** [P1] `#[ttl(field)]` or `#[ttl(after = "30m")]` SHALL cause the schedule worker to delete
  expired rows in normal transactions, emitting delete diffs; deletion is at-least-once and idempotent.
- **DMX-021** [P1] TTL sweeps MUST be batched and bounded so they never stall the writer.

#### Scenario: Session expiry
Given `Session` has `#[ttl(after="30m")]`
When a session row's age exceeds 30 minutes
Then it is deleted by a background transaction and its subscribers receive the delete.

#### Implementation status (phase 3 — complete)
- **Declaration** ([macros/src/table.rs](../../crates/fluxum-macros/src/table.rs)): `#[ttl(col)]` (the
  column must be a `Timestamp`, checked at compile time) and `#[ttl(after = "30m")]`; at most one
  `#[ttl]` per table. Registered as a link-time `TtlDef` side registry
  ([schema/mod.rs](../../crates/fluxum-core/src/schema/mod.rs)) with a schema-assembly backstop
  (Timestamp type, positive duration, not on an ephemeral table).
- **Two expiry modes**: `#[ttl(col)]` is an **absolute** expiry stored in the row — restart-safe and
  exact (the sweep deletes rows whose `Timestamp` column is `<= now`). `#[ttl(after)]` is a **sliding**
  window since the last write, tracked by the same in-memory identity witness as the DMX-011 ephemeral
  sweeper — best-effort, and the age resets on restart (industry-standard approximate TTL). A row
  rewritten with changed data refreshes its window.
- **Sweeper** ([scheduler/mod.rs `TtlSweeper`](../../crates/fluxum-core/src/scheduler/mod.rs)): scans a
  wait-free snapshot, then deletes in one ordinary transaction, re-verifying each row so a write that
  raced the snapshot wins — at-least-once and idempotent (a re-sweep of an already-deleted or refreshed
  row is a no-op). Each pass is bounded to `TTL_SWEEP_BATCH` (1024) rows; a larger backlog drains across
  successive passes so a mass expiry never stalls the single writer (DMX-021). The delete diffs fan out
  to subscribers like any commit. Started per shard on serve
  (`ShardContext::start_ttl_sweeper`), mirroring the ephemeral sweeper.

## 4. Rich column types (`DMX-03x`)

### Requirement: Enums, tagged unions, nested structs
- **DMX-030** [P1] The column type universe SHALL admit `#[derive(FluxType)]` enums (tagged unions with
  payloads) and nested structs, encoded in FluxBIN as `u8 tag + payload` / sequential fields; additive,
  landing before the G5 wire freeze.
- **DMX-031** [P1] Enum/struct columns MAY be part of equality filters; ordering/index support is limited
  to their derivable memcomparable encoding.

#### Scenario: Tagged status column
Given `Task.status` is an enum `Todo | Doing | Done(by: Identity)`
When a client subscribes `WHERE status = Done`
Then matching rows are delivered and the payload decodes losslessly in every SDK.

## 5. Blob / large-object store (`DMX-04x`)

### Requirement: First-class large objects
- **DMX-040** [P1] A `Blob` column type SHALL store large values (> frame limit) in a content-addressed,
  refcounted blob store, keeping only a reference inline in the row (building on the existing commit-log
  blob overflow path).
- **DMX-041** [P1] Blob upload/download SHALL stream over a dedicated admin/transport path, not the
  16 MB frame; blobs are GC'd when their refcount reaches zero.

#### Scenario: Avatar attachment
Given `User.avatar: Blob`
When a client uploads a 4 MB image and sets it on a row
Then the row carries a content hash reference, the bytes stream out of band, and deleting all
referencing rows reclaims the blob.

## 6. Typed edges & traversal (`DMX-05x`)

### Requirement: Graph relations without JOINs
- **DMX-050** [P2] `#[fluxum::edge]` SHALL declare a typed directed relation `(from, to, props)` backed by
  composite-PK-indexed rows; `traverse` helpers walk edges in O(log n + k) without a general JOIN engine.
- **DMX-051** [P2] Edge sets MAY be subscribed like tables (neighbors of X).

#### Scenario: Inventory ownership
Given `Owns` edges from `Player` to `Item`
When a client subscribes to a player's `Owns` neighbors
Then it receives that player's items and live diffs as edges are added/removed.

## 7. CRDT text column (`DMX-06x`)

### Requirement: Collaborative text within a shard
- **DMX-060** [P2] A `CrdtText` column SHALL accept concurrent character-level edit ops and merge them
  deterministically within the single-writer shard, exposing a converged value to all subscribers.
- **DMX-061** [P2] Edit ops MUST be expressible as reducer calls and fan out as compact op diffs, not
  full-document rewrites.

#### Scenario: Two editors, one paragraph
Given a `Doc.body: CrdtText` with two concurrent editors
When both insert text at the same position in overlapping transactions
Then the shard merges the ops deterministically and both clients converge to the same text.

## 8. Non-goals

- Multi-primary / cross-shard CRDT convergence (single authoritative shard only).
- A general JOIN/graph query language (typed edges are point traversals, not Cypher/SurrealQL).
- Vector/semantic columns (delegated to Vectorizer).
