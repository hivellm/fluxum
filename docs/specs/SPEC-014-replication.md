# SPEC-014 — Replication & Backup

| | |
|---|---|
| **Status** | Draft; **§8 Backup + §9 PITR shipped** (T7.3, FR-103/FR-104): `fluxum backup create/verify/restore` over [`fluxum_core::backup`](../../crates/fluxum-core/src/backup/mod.rs) — hot create (REP-060/061), REP-062 archival wired into the boot-spawned checkpoint worker (`replication.archive.*` config, retention sweep, `fluxum_archive_segments_pending`), bit-flip-precise verify (REP-064), exact-head restore (REP-063), PITR to `--to-tx-id`/`--to-timestamp` with boundary report, chain-gap refusal, roll-forward guard, and the REP-072 `pitr.lineage` fencing-epoch marker the next boot adopts (`POST /checkpoint` serves `--fresh-checkpoint`). §2–§7 replication remain open (T7.1/T7.2). |
| **Phase / tasks** | Phase 7 · T7.1–T7.3 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-100, FR-101, FR-102, FR-103, FR-104, FR-105; NFR-08 |
| **Requirement prefix** | `REP-` |
| **Source** | new — availability track for the 0.2.0 competitive launch |

Requirement IDs `REP-xxx`. Integers little-endian unless stated otherwise. This spec is on the
competitive-launch track: every requirement is **[P1]** unless explicitly tagged otherwise.
It builds on the storage engine (SPEC-002: commit log, checkpoints), sharding (SPEC-007:
`ShardCoord`, global tables), the wire protocol (SPEC-006), authentication (SPEC-009), and
observability (SPEC-012). Throughout this spec, **checkpoint** refers to the SPEC-002 snapshot
(the `SnapshotRepo` point-in-time dump) — the PRD and ARCHITECTURE use the two terms
interchangeably.

## 1. Overview

Per shard, Fluxum runs a **replica set**: one primary + N replicas. The primary executes all
reducers (single writer, FR-12); replicas apply the primary's commit log and serve reads and
subscription fan-out.

```
            writes                    commit-log stream
Clients ──────────────▶ PRIMARY ═══════════════════════▶ REPLICA 1
            reads /                   (async | semi_sync)      │
            subscriptions ─────────────────────────────▶ REPLICA 2
                                                          serve reads +
                                                          subscription fan-out
```

The design has one central decision: **the commit log is the replication protocol.** There is no
second wire format to define, version, or test — full sync is a checkpoint transfer, partial sync
is a log tail from an offset, and backups and PITR replay the same segments.

## 2. Replica set model

- **REP-001** [P1] **Replica set per shard.** Each shard (SPEC-007) **SHALL** be servable by a
  replica set of one **primary** and N ≥ 0 **replicas**. Each member is a full `ShardHost`
  instance with its own `MemStore`, `CommitLog`, and checkpoint directory. A replica set with
  N = 0 degenerates to the single-node deployment of SPEC-002 and requires none of the machinery
  in this spec.

- **REP-002** [P1] **Roles.** At any instant, at most one member of a replica set **SHALL** hold
  the `primary` role for a given epoch (REP-004). Only the primary executes reducers and appends
  new entries to the authoritative log. All other members hold the `replica` role: they apply the
  replicated log and **SHALL NOT** originate transactions. Role changes occur only through
  election and promotion (§5) — never by operator edit of a running process.

- **REP-003** [P1] **Configuration.** Replica-set membership and behavior **SHALL** be configured
  in the `replication:` section of `config.yml` (env-overridable via `FLUXUM_*`, FR-04). The
  full key set is normative in §11. Minimal form:

  ```yaml
  replication:
    role: primary            # primary | replica — initial role; auto after first election
    mode: async              # async | semi_sync (REP-020/REP-021)
    peers: []                # replica-set members, "host:port" (FluxRPC TCP)
  ```

  `role` is a bootstrap hint only: after the first election, the consensus state machine owns
  role assignment, and the effective role is reported via `/health` (REP-080).

- **REP-004** [P1] **Epoch numbers.** Each replica set **SHALL** maintain a monotonically
  increasing `u64` **epoch**, starting at 1 and incremented by exactly one on every successful
  primary election. The current epoch **SHALL** be durably persisted by every member before it
  acts under that epoch. Every replication session, batch envelope, and heartbeat carries the
  sender's epoch (REP-011, REP-016); epochs are the fencing mechanism that makes split-brain
  writes impossible (REP-031). The `TxRecord` body itself carries no epoch — the entry format is
  frozen at G5 (REP-010) — so epochs live exclusively in the replication envelope and consensus
  state.

- **REP-005** [P1] **Member identity and authentication.** Replica-set members **SHALL**
  authenticate to each other as privileged server peers (SPEC-009, FR-72):
  `Identity = SHA-256("SERVER:" + member_name)`. Replication and election traffic **SHALL** be
  rejected from any connection not authenticated as a configured peer.

## 3. Replication protocol — log streaming

- **REP-010** [P1] **The commit log is the replication stream.** Replication **SHALL** transport
  the shard's commit-log entries byte-identically in the format of SPEC-002 STG-011
  (`u32 LE length + MessagePack TxRecord + CRC32`), which is frozen at gate G5 together with the
  wire format. Replication **SHALL NOT** introduce any alternative row or mutation encoding.
  A replica's local commit log, after applying the stream, is byte-equivalent to the primary's
  log over the same `tx_id` range.

- **REP-011** [P1] **Transport and session handshake.** Replication connections **SHALL** run
  over FluxRPC TCP (SPEC-006, port 15801) using ordinary FluxRPC envelopes and server-peer
  authentication (REP-005). A session begins with a handshake:

  ```rust
  pub struct ReplicaHello {
      pub shard_id: u32,
      pub member_name: String,
      pub epoch: u64,              // highest epoch the replica has seen
      pub last_applied_tx_id: u64, // 0 if empty
  }

  pub struct PrimaryHello {
      pub shard_id: u32,
      pub epoch: u64,              // current primary epoch
      pub first_available_tx_id: u64, // oldest tx still on local segments
      pub latest_tx_id: u64,
      pub sync: SyncDecision,      // Full | Partial { from_tx_id: u64 }
  }
  ```

  The primary **SHALL** answer with `sync: Partial` when the replica's offset is still covered by
  its retained log segments and the replica's history is an ancestor of the primary's (REP-013);
  otherwise `sync: Full` (REP-012).

- **REP-012** [P1] **Full sync = checkpoint transfer + tail.** When a replica is empty or too far
  behind (its `last_applied_tx_id < first_available_tx_id`), the primary **SHALL**:

  ```
  1. Select its latest checkpoint (SPEC-002 STG-020); if none exists, write one
     (non-blocking, STG-022).
  2. Stream the checkpoint to the replica, zstd-compressed (FR-19).
  3. Stream log entries from checkpoint.last_tx_id + 1 to the current head.
  4. Transition the session to continuous streaming.
  ```

  The replica loads the checkpoint into `CommittedState`, replays the tail, and is then in
  continuous sync. Full sync **SHALL NOT** stall the primary's writer: checkpoint selection and
  streaming run off the write path.

- **REP-013** [P1] **Partial sync = stream from offset.** When the replica's
  `last_applied_tx_id` is covered by retained segments, the primary **SHALL** stream entries from
  `last_applied_tx_id + 1` onward. Before resuming, the replica's history **MUST** be validated:
  if the replica holds entries beyond the point at which a newer epoch began (a diverged suffix
  written under a deposed primary), the replica **SHALL** truncate its local log back to the last
  entry common with the primary's history and rebuild `CommittedState` to that point (from its
  own checkpoint + replay) before applying the stream. If no common point can be established, the
  session falls back to full sync.

  **Scenario: divergence truncation**
  ```
  Given  replica R holds tx 100..110 written under epoch 3
  And    the current primary (epoch 4) diverged at tx 105
  When   R connects and offers last_applied_tx_id = 110, epoch = 3
  Then   R truncates its log back to tx 105, restores state to tx 105,
         and resumes partial sync from tx 106 under epoch 4
  ```

- **REP-014** [P1] **Replica apply rules.** For every received entry, a replica **SHALL**:

  1. Verify the CRC32 (STG-011); a mismatch aborts the session and triggers resync.
  2. Verify strict `tx_id` monotonicity (`previous + 1`; STG-015); a gap or repeat aborts the
     session and triggers resync.
  3. Append the entry to its local commit log (durability on the replica).
  4. Apply the mutations to its `CommittedState` atomically (same merge semantics as STG-005).
  5. Evaluate its local subscriptions and fan out `TxUpdate` to clients attached to this replica
     (REP-043).
  6. Acknowledge the applied offset per flow control (REP-017) and, in `semi_sync` mode, per
     REP-021.

- **REP-015** [P1] **Fan-out metadata sidecar.** The enriched `TxUpdate` (FR-43) carries fields
  that are not in the frozen `TxRecord` (`reducer_name`, `caller`, `duration_us`). The
  replication batch envelope **SHALL** carry this metadata alongside each entry:

  ```rust
  pub struct ReplBatch {
      pub epoch: u64,
      pub entries: Vec<Bytes>,          // raw STG-011 log entries, byte-identical
      pub meta: Vec<TxMeta>,            // parallel to entries
  }

  pub struct TxMeta {
      pub reducer_name: String,
      pub caller: Identity,
      pub duration_us: u32,
  }
  ```

  This lets replicas emit `TxUpdate` messages identical to the primary's without widening the
  frozen log format. `TxMeta` is transient — it is not written to the replica's log.

- **REP-016** [P1] **Heartbeats.** The primary **SHALL** send a heartbeat to every replica each
  `heartbeat_interval_ms` (default 500 ms) carrying `{ epoch, latest_tx_id }`, including when
  idle. Replicas use heartbeats to (a) measure lag when no entries flow, and (b) detect primary
  failure: a member that receives neither entries nor heartbeats for `election_timeout_ms`
  (default 3,000 ms) **SHALL** consider the primary unreachable and initiate an election (§5).

- **REP-017** [P1] **Flow control.** Streaming **SHALL** be windowed: the primary keeps at most
  `window_bytes` (default 8 MiB) of unacknowledged entries in flight per replica and buffers
  beyond that in its retained segments — never in unbounded memory. Replicas acknowledge applied
  offsets at least every `ack_interval_ms` (default 100 ms) or every window quarter, whichever
  comes first. A slow replica **SHALL NOT** stall the primary's writer in `async` mode: when a
  replica falls behind the retained-segment horizon, the primary **SHALL** drop the session and
  mark the replica `LAGGING`; the replica later re-enters via partial or full sync (REP-011).

## 4. Acknowledgment modes

- **REP-020** [P1] **`async` (default).** In `async` mode the primary **SHALL** send the
  `ReducerResult` and fan out `TxUpdate` as soon as the transaction commits locally
  (SPEC-002 pipeline) — replication proceeds in the background. Durability on the primary is
  bounded by the async log window (NFR-08, < ~50 ms); replication lag is additionally bounded
  only by network and replica apply speed, and is observable (REP-080/REP-081). On failover, the
  unreplicated tail of an `async` primary MAY be lost (REP-034).

- **REP-021** [P1] **`semi_sync` — quorum acknowledgment.** In `semi_sync` mode, for every
  committed transaction the primary **SHALL** withhold both the `ReducerResult` to the caller and
  the `TxUpdate` fan-out to subscribers until a **quorum** of replica-set members (counting
  itself) has durably appended the entry to their local commit logs. Quorum is
  `⌈(members + 1) / 2⌉` by default (`semi_sync.quorum: majority`), overridable to an explicit
  count. Because nothing a client can observe precedes quorum, a transaction observed by any
  client survives any single-member failure: **zero committed-transaction loss on failover**.
  This extends the single-node durability window of NFR-08 to the replica set.

  **Scenario: semi-sync visibility barrier**
  ```
  Given  mode = semi_sync with members = 3 (quorum 2)
  When   a reducer commits tx 500 on the primary
  Then   neither the ReducerResult nor any TxUpdate for tx 500 is sent
         until at least one replica has durably appended tx 500
  And    after quorum, the caller receives ReducerResult and subscribers
         receive TxUpdate for tx 500
  ```

- **REP-022** [P1] **Quorum loss behavior.** If quorum is not reached within
  `semi_sync.ack_timeout_ms` (default 1,000 ms), behavior **SHALL** follow
  `semi_sync.on_quorum_loss`:

  - `block` (default): the primary stops acknowledging new writes (callers receive a retryable
    `Unavailable` error after the timeout) until quorum returns. Safest; preserves the
    zero-loss guarantee unconditionally.
  - `degrade`: the primary falls back to `async` acknowledgment, emits a `WARN` log event and
    sets `fluxum_replication_degraded{shard} = 1` until quorum returns. The zero-loss guarantee
    is suspended while degraded — this **MUST** be stated in the operator documentation.

- **REP-023** [P1] **Mode is per replica set.** The acknowledgment mode is configured per
  deployment (`replication.mode`) and applies uniformly to all transactions of a shard. Per-call
  mode overrides are out of scope for 0.2.0. [P2] A future `#[fluxum::reducer(durability =
  "quorum")]` per-reducer override MAY be added without changing the wire protocol.

## 5. Failover

- **REP-030** [P1] **Consensus-based election.** Primary election **SHALL** be decided by a
  consensus algorithm requiring a majority of configured members — no self-appointed primaries.
  The library choice is open (PRD OQ-8): `openraft` (Vectorizer precedent) or a custom Raft
  implementation (Nexus precedent); both satisfy this spec. Requirements on the integration,
  regardless of library:

  - Election is triggered by heartbeat loss (REP-016) or by explicit operator step-down.
  - A candidate **SHALL NOT** win unless its applied log is at least as up-to-date (highest
    `(epoch, tx_id)`) as that of a majority — in `semi_sync` mode this guarantees the winner
    holds every quorum-acknowledged transaction.
  - The consensus transport **SHALL** be restricted to authenticated replica-set peers
    (REP-005).
  - Operators **SHOULD** deploy odd member counts (≥ 3). A two-member set cannot elect a new
    primary automatically and **SHALL** document manual promotion as the recovery path.

- **REP-031** [P1] **Epoch fencing.** On winning an election, the new primary increments the
  epoch (REP-004). Every member **SHALL** reject replication batches, heartbeats, and semi-sync
  ack requests whose envelope epoch is lower than the highest epoch it has persisted, and
  **SHALL** answer with the higher epoch. A deposed primary that receives such a rejection, or
  observes a higher epoch by any channel, **SHALL** immediately stop acknowledging writes, demote
  itself to replica, and resync (truncating its diverged suffix per REP-013). Fenced write
  attempts increment `fluxum_replication_fenced_total` (REP-081).

  **Scenario: stale primary fenced**
  ```
  Given  a network partition isolates primary P (epoch 4) from replicas R1, R2
  And    R1 wins election for epoch 5
  When   P heals and streams a batch with envelope epoch 4 to R2
  Then   R2 rejects the batch and replies with epoch 5
  And    P demotes itself to replica, truncates its diverged suffix,
         and partial-syncs from R1
  ```

- **REP-032** [P1] **Promotion sequence.** A replica that wins the election for epoch E+1
  **SHALL** promote in this order:

  ```
  1. Finish applying every log entry it has already received and durably appended.
  2. Persist epoch E+1 (consensus state).
  3. Assume the primary role: open the shard writer, resume tx_id from
     last_applied_tx_id + 1 (STG-015).
  4. Notify ShardCoord (routing-table update, REP-050) and all reachable peers.
  5. Serve replication sessions: surviving replicas reconnect, truncate any
     diverged suffixes (REP-013), and resume streaming under epoch E+1.
  6. Accept reducer calls.
  ```

  Steps 1–3 **MUST** complete before any client-visible write is accepted.

- **REP-033** [P1] **SDK behavior on failover.** Client SDKs (SPEC-011) **SHALL** treat failover
  as a reconnect (SPEC-006 RPC-062): on connection loss, reconnect with exponential backoff to
  the current primary (endpoint discovery via `ShardCoord` or the configured endpoint list),
  **re-authenticate** (same token ⇒ same `Identity`, SPEC-009), and **resubscribe** every active
  query. The fresh `InitialData` response **SHALL** be reconciled against the existing local
  cache: the SDK computes the row-level difference and emits only the resulting insert/delete
  callbacks to application code — the application never observes a cache wipe, only the net
  changes across the failover.

- **REP-034** [P1] **Data-loss contract per mode.** After a failover:

  - `semi_sync`: zero committed-transaction loss — every transaction whose `ReducerResult` or
    `TxUpdate` was sent to any client survives (REP-021, REP-030).
  - `async`: transactions appended on the old primary but not yet replicated to the election
    winner are rolled back when the old primary rejoins (divergence truncation, REP-013). The
    loss window equals the replication lag at failure time and is observable
    (`fluxum_replication_lag_ms`). This trade-off **MUST** be documented; deployments that
    cannot tolerate it use `semi_sync`.

- **REP-035** [P1] **Operator-initiated switchover.** `fluxum replica step-down` (CLI/admin API)
  **SHALL** trigger a clean handover: the primary stops accepting writes, waits for one replica
  to reach its head offset (bounded by a timeout), then triggers an election it does not contest.
  A clean switchover loses zero transactions in either mode.

## 6. Read replicas

- **REP-040** [P1] **Read offload.** Replicas **SHALL** serve read-only traffic: `OneOffQuery`
  (SPEC-006), HTTP `POST /query` and `GET /view/:name`, and **subscription fan-out**
  (clients connect their subscriptions to a replica; `Subscribe`/`InitialData`/`TxUpdate` run
  entirely on the replica). Row-level security (`#[visibility(...)]`, SPEC-001/SPEC-005)
  **SHALL** be evaluated on the replica with the subscriber's identity, producing exactly the
  rows the primary would produce for that identity.

- **REP-041** [P1] **Bounded, observable staleness.** A replica's staleness is its replication
  lag (`primary latest_tx_id − last_applied_tx_id`, and the wall-clock equivalent measured via
  heartbeats). Lag **SHALL** be continuously exposed (REP-080/REP-081). When lag exceeds
  `max_staleness_ms` (default 5,000 ms), the replica **SHALL** stop admitting new queries and
  subscriptions, answering with a retryable `ReplicaStale` error that names the primary endpoint;
  already-attached subscribers keep their streams (their updates are delayed, not wrong — diffs
  are applied in `tx_id` order). It resumes admission once lag falls below the bound.

  **Scenario: staleness bound**
  ```
  Given  max_staleness_ms = 5000 and a replica lagging 12 s behind the primary
  When   a client sends OneOffQuery to that replica
  Then   the query is rejected with ReplicaStale { primary: "10.0.0.5:15801" }
  And    the SDK transparently retries against the primary
  ```

- **REP-042** [P1] **Writes are rejected and redirected.** A `ReducerCall` received by a replica
  **SHALL** be rejected without executing, with a `NotPrimary` error carrying the current primary
  endpoint and epoch. SDKs **SHALL** follow the redirect automatically and cache the primary
  endpoint until the next `NotPrimary` or reconnect. Replicas **SHALL NOT** proxy write traffic
  to the primary (no hidden extra hop on the write path).

- **REP-043** [P1] **Fan-out equivalence.** `TxUpdate` messages emitted by a replica **SHALL** be
  content-identical to those the primary would emit for the same subscription and identity: same
  `tx_id`, `timestamp`, `reducer_name`, `caller`, `duration_us` (from `TxMeta`, REP-015), same
  rows after RLS filtering, delivered in strictly increasing `tx_id` order. Offloading fan-out to
  replicas removes broadcast work from the primary's write path (PRD risk mitigation: fan-out
  bottleneck) without changing what any client observes, apart from bounded added latency.

- **REP-044** [P2] **Read-your-writes sessions.** SDKs MAY offer an opt-in session guarantee:
  after a reducer call returns with `tx_id = N`, subsequent reads routed to a replica wait until
  the replica has applied ≥ N (or transparently fall through to the primary). Not required for
  0.2.0.

## 7. Global tables and ShardCoord interaction

- **REP-050** [P1] **Routing follows the election.** `ShardCoord` (SPEC-007) **SHALL** track, per
  shard, the current primary endpoint and epoch, and the set of readable replicas with their
  lag. Reducer calls and handoff traffic route to the primary; `OneOffQuery` and new
  subscriptions MAY be placed on eligible replicas (REP-041) per a configurable placement policy
  (`reads: primary | prefer_replica`). On election, the new primary's notification (REP-032 step
  4) updates the routing table; calls in flight to the old primary fail with `NotPrimary` and are
  retried by the SDK against the new one.

- **REP-051** [P1] **Global tables.** `#[fluxum::table(global)]` tables (SPEC-007 SHD-030) are
  written on their authoritative shard, and those writes are ordinary commit-log entries — they
  replicate to that shard's replicas through the normal stream with no special casing.
  `ShardCoord`'s read-only propagation of global-table mutations to *other* shards remains
  SHD-030 behavior and targets each shard's primary; each receiving primary's replicas then see
  the propagated write via their own replica-set stream. After a failover of the authoritative
  shard, the new primary **SHALL** resume global-table propagation from its committed state.

- **REP-052** [P1] **Entity handoff across failover.** Entity handoff (SPEC-007 SHD-040) steps
  are ordinary committed transactions on the source and destination shards, so they replicate in
  order like any other write. If either shard's primary fails mid-handoff, `ShardCoord`
  **SHALL** abort the handoff per the SHD abort rule (clear the `__handoff__` marker, keep the
  entity on the source shard) and retry against the newly elected primary. A handoff **SHALL**
  never complete partially as a result of an election.

## 8. Backup

- **REP-060** [P1] **Hot backup.** `fluxum backup create --out <path>` **SHALL** produce a
  consistent backup of a running deployment with **no writer stall**: per shard, it packages
  (a) the latest checkpoint (triggering a fresh non-blocking one first when
  `--fresh-checkpoint` is passed; STG-022) and (b) all log segments covering
  `checkpoint.last_tx_id + 1` through the log head at backup start. All payloads are
  zstd-compressed (FR-19). Reducer throughput during backup **SHALL NOT** drop other than by
  shared disk/CPU bandwidth — no locks are taken on the write path.

- **REP-061** [P1] **Backup manifest.** Every backup **SHALL** contain a manifest
  (`manifest.mpack`, MessagePack) that makes it self-describing and verifiable:

  ```rust
  pub struct BackupManifest {
      pub backup_id: String,          // UUID
      pub created_at: i64,            // µs since Unix epoch
      pub format_version: u32,
      pub schema_version: u32,        // SPEC-010 __schema_meta__ version
      pub shards: Vec<ShardBackup>,
  }

  pub struct ShardBackup {
      pub shard_id: u32,
      pub checkpoint_file: String,
      pub checkpoint_last_tx_id: u64,
      pub segments: Vec<SegmentEntry>, // { file, first_tx_id, last_tx_id, crc32 }
      pub checkpoint_crc32: u32,
  }
  ```

  A multi-shard deployment is captured as one backup with one `ShardBackup` per shard.

- **REP-062** [P1] **Log segment archival policy.** When `replication.archive.enabled: true`
  (default `true` whenever backups or PITR are in use), a commit-log segment **SHALL** be copied,
  zstd-compressed, to `archive.dir` **before** checkpoint-driven truncation is allowed to delete
  it (SPEC-002 STG-013 gains this precondition). Archived segments are retained for
  `archive.retention` (default `7d`); the PITR window (§9) equals the archive retention window.
  Archival is asynchronous and **SHALL NOT** block the writer; a failed archival blocks segment
  deletion, not writes, and raises a `WARN` + metric.

- **REP-063** [P1] **Restore.** `fluxum backup restore --from <path> --data-dir <dir>` **SHALL**
  refuse to run against a non-empty data directory unless `--force` is given. It verifies every
  file CRC against the manifest, decompresses the checkpoint into the checkpoint directory and
  the segments into the commit-log directory, and exits; the normal SPEC-002 recovery sequence
  (STG-030) then reconstructs `CommittedState` on next startup. A restored full backup reproduces
  exactly the committed state as of the last segment's `last_tx_id`.

- **REP-064** [P1] **Verify.** `fluxum backup verify --from <path>` **SHALL** validate a backup
  without restoring it: (a) manifest is decodable and complete (every listed file present, no
  extras required); (b) CRC32 of every checkpoint and segment file matches the manifest;
  (c) structural checks — every `TxRecord` in every segment decodes, per-entry CRCs hold, and
  `tx_id` is strictly contiguous from `checkpoint_last_tx_id + 1` across the segment chain per
  shard. Exit code 0 on success; non-zero with a per-file error report otherwise.

- **REP-065** [P2] **Remote targets and scheduling.** Backup/archive destinations other than the
  local filesystem (e.g. S3-compatible object storage) and built-in backup scheduling MAY be
  added post-launch; for 0.2.0, operators schedule `fluxum backup create` externally (cron) and
  ship the output directory themselves.

## 9. Point-in-time recovery (PITR)

- **REP-070** [P1] **Replay to a target.** `fluxum backup restore` **SHALL** accept
  `--to-timestamp <rfc3339|µs>` or `--to-tx-id <n>` (mutually exclusive). After restoring the
  base backup (REP-063), it **SHALL** locate the archived segments (REP-062) that continue the
  backup's `tx_id` chain and replay them, stopping at the target: all transactions with
  `timestamp ≤ target` (or `tx_id ≤ n`) are applied, inclusive. The target MAY fall beyond the
  backup's own segments — that is the normal case, and it is why archival (REP-062) is a
  precondition for PITR.

- **REP-071** [P1] **Boundary semantics.** PITR **SHALL** never apply a partial transaction: the
  unit of replay is the whole `TxRecord`. On completion the CLI **SHALL** report the last applied
  `tx_id` and its `timestamp`. If the archive chain has a gap before the target (missing
  segment), the restore **SHALL** fail with the covered range reported — it **SHALL NOT**
  silently stop early.

  **Scenario: PITR to a timestamp**
  ```
  Given  a backup taken at 02:00 and archived segments through 14:00
  And    an operator error (bulk delete) committed at 13:37:10 as tx 91457
  When   fluxum backup restore --from ./bk --to-timestamp "13:37:09" runs
  Then   the restored state contains every transaction up to and including
         the last tx with timestamp ≤ 13:37:09, and nothing after
  And    the CLI reports the last applied tx_id and timestamp
  ```

- **REP-072** [P1] **Post-PITR lineage.** A node started from a PITR restore has forked history.
  It **SHALL** begin with an epoch strictly greater than any epoch recorded in the restored log,
  and it **SHALL NOT** be joined to a replica set whose members have advanced past the restore
  point — the handshake history check (REP-013) detects the divergence and refuses partial sync.
  The supported pattern: the restored node becomes the seed of a (new or wiped) replica set, and
  peers full-sync from it (REP-012).

## 10. Observability

- **REP-080** [P1] **Health endpoint.** `/health` (SPEC-012 OBS-060) **SHALL** be extended
  with a per-shard `replication` object; the lock-free constraint (OBS-061, FR-91) still applies
  — values come from pre-computed state published by the replication subsystem:

  ```json
  {
    "status": "ok",
    "shards": [
      {
        "id": "0", "state": "ready", "tx_id": 84201, "queue_depth": 0,
        "replication": {
          "role": "primary",
          "epoch": 5,
          "mode": "semi_sync",
          "replicas": [
            { "name": "node-b", "state": "streaming", "acked_tx_id": 84201, "lag_ms": 3 },
            { "name": "node-c", "state": "lagging",   "acked_tx_id": 79004, "lag_ms": 4180 }
          ]
        }
      }
    ]
  }
  ```

  On a replica, the object reports `role: "replica"`, the primary's endpoint, its own
  `last_applied_tx_id`, and its current lag. A shard in `semi_sync` that has lost quorum reports
  `status: "degraded"` overall.

- **REP-081** [P1] **Metrics.** The following **SHALL** be exported at `/metrics`, extending
  the SPEC-012 registry (FR-105):

  ```
  fluxum_replication_role{shard}                    Gauge      // 0 = replica, 1 = primary
  fluxum_replication_epoch{shard}                   Gauge
  fluxum_replication_connected_replicas{shard}      Gauge      // primary side
  fluxum_replication_offset{shard, peer}            Gauge      // last acked tx_id per peer
  fluxum_replication_lag_tx{shard, peer}            Gauge      // head tx_id − acked tx_id
  fluxum_replication_lag_ms{shard, peer}            Gauge      // wall-clock lag via heartbeats
  fluxum_replication_semi_sync_wait_us{shard}       Histogram  // commit → quorum-ack latency
  fluxum_replication_degraded{shard}                Gauge      // 1 while on_quorum_loss=degrade active
  fluxum_replication_fenced_total{shard}            Counter    // writes rejected by epoch fencing
  fluxum_replication_elections_total{shard}         Counter
  fluxum_replication_full_syncs_total{shard}        Counter
  fluxum_backup_last_success_timestamp              Gauge      // seconds since epoch
  fluxum_backup_duration_ms                         Histogram
  fluxum_archive_segments_pending{shard}            Gauge      // awaiting archival before deletion
  ```

- **REP-082** [P1] **Log events.** The following **SHALL** be emitted as structured `tracing`
  events (SPEC-012 OBS-070): election started/won/lost (`INFO`), promotion and demotion
  (`INFO`), epoch-fencing rejection (`WARN`), full/partial sync start and completion with
  transferred bytes and duration (`INFO`), replica marked `LAGGING` or `ReplicaStale` admission
  stop (`WARN`), semi-sync quorum loss and recovery (`WARN`), and backup
  create/restore/verify outcomes (`INFO` on success, `ERROR` on failure).

## 11. Configuration

Replication and backup keys in `config.yml` (env-overridable via `FLUXUM_*`); defaults are
normative per the requirements above:

```yaml
replication:
  role: primary                 # primary | replica — bootstrap hint (REP-003)
  mode: async                   # async | semi_sync (REP-020/REP-021)
  peers: []                     # replica-set members, "name@host:port" (REP-005)
  heartbeat_interval_ms: 500    # REP-016
  election_timeout_ms: 3000     # REP-016
  window_bytes: 8388608         # 8 MiB flow-control window (REP-017)
  ack_interval_ms: 100          # REP-017
  max_staleness_ms: 5000        # read-replica admission bound (REP-041)
  reads: prefer_replica         # primary | prefer_replica — ShardCoord placement (REP-050)
  semi_sync:
    quorum: majority            # majority | <count> (REP-021)
    ack_timeout_ms: 1000        # REP-022
    on_quorum_loss: block       # block | degrade (REP-022)
  archive:
    enabled: true               # REP-062 — precondition for PITR
    dir: ./data/archive
    retention: 7d               # PITR window (REP-062, §9)
```

## Acceptance criteria

1. **Failover drill, zero committed-tx loss** (T7.2, PRD §12.2): a 3-member replica set in
   `semi_sync` under sustained writes; the primary is killed (`kill -9`). A replica wins the
   election, promotes (REP-032), and every transaction that any client observed
   (`ReducerResult` or `TxUpdate`) is present on the new primary (REP-021, REP-034). SDK
   clients reconnect, re-authenticate, resubscribe, and their reconciled caches match the new
   primary's state exactly (REP-033).
2. **Semi-sync visibility barrier** (T7.1): with acks artificially delayed on all replicas, no
   `ReducerResult` and no `TxUpdate` is delivered before quorum append is confirmed; commit →
   quorum-ack latency appears in `fluxum_replication_semi_sync_wait_us`.
3. **Cold-sync and offset-sync convergence** (T7.1): (a) an empty replica joins a loaded shard
   and converges via checkpoint transfer + tail (REP-012); (b) a replica stopped mid-stream
   rejoins and converges via partial sync from its offset (REP-013). In both cases the replica's
   `CommittedState` equals the primary's and its log is byte-identical over the shared `tx_id`
   range (REP-010, REP-014).
4. **Epoch fencing** (T7.1/T7.2): a partitioned stale primary's batches are rejected by peers,
   it demotes and truncates its diverged suffix, `fluxum_replication_fenced_total` increments,
   and no write from the stale epoch survives in the set (REP-031, REP-013).
5. **Fan-out offload verified** (T7.2): subscribers attached to a replica receive `TxUpdate`
   streams content-identical (including RLS filtering and `TxMeta` fields) to subscribers on the
   primary (REP-043); moving 1,000 subscribers from primary to replica reduces the primary's
   `fluxum_fanout_messages_total` rate accordingly while write throughput is unchanged; a
   replica pushed past `max_staleness_ms` rejects new admissions with `ReplicaStale` (REP-041).
6. **Backup + PITR round-trip in CI** (T7.3, PRD §12.2): under sustained writes,
   `fluxum backup create` completes with no writer stall (throughput within noise of baseline);
   `verify` passes; `verify` fails with a precise report after a single injected bit-flip in a
   segment (REP-064); full restore reproduces the exact state at the backup head (REP-063); PITR
   to a mid-history `--to-timestamp` and `--to-tx-id` reproduces exactly the transactions up to
   the inclusive target and reports the boundary (REP-070/REP-071); the restored node refuses to
   partial-sync into the old set and seeds a new one (REP-072).
7. **Observability contract** (T7.2): `/health` reports role/epoch/replicas/lag per REP-080
   within the OBS-061 latency bound; all REP-081 metrics are present and move correctly through
   a scripted election + resync + backup sequence; all REP-082 events are emitted as structured
   JSON.
