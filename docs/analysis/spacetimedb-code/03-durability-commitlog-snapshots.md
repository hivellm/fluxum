# SpacetimeDB Deep Dive 03 — Durability: Commitlog, Snapshots, Recovery

| | |
|---|---|
| **Source** | SpacetimeDB v2.7.0, commit `1a8df2a` (real source tree) |
| **Crates analyzed** | `crates/commitlog` (~9k LOC), `crates/durability` (~800 LOC), `crates/snapshot` (~3k LOC), `crates/fs-utils`, plus recovery call sites in `crates/engine` / `crates/datastore` / `crates/core` |
| **Key files** | `crates/commitlog/src/{segment.rs, commit.rs, commitlog.rs, repo/{mod,fs}.rs, index/indexfile.rs, payload/txdata.rs, stream/{reader,writer}.rs}`, `crates/durability/src/{lib.rs, imp/local.rs}`, `crates/snapshot/src/{lib.rs, remote.rs}`, `crates/fs-utils/src/{lockfile.rs, compression.rs, dir_trie.rs}`, `crates/engine/src/{relational_db.rs, snapshot.rs, durability.rs}`, `crates/datastore/src/locking_tx_datastore/replay.rs` |
| **Fluxum specs compared** | SPEC-002 (storage engine: commit log, checkpoints, recovery), SPEC-014 (replication & backup) |
| **Date** | 2026-07-14 |

## 1. Layering

SpacetimeDB splits durability into cleanly separated crates:

```
crates/commitlog    — generic, payload-agnostic segmented log: Commitlog<T, R: Repo>
crates/durability   — Durability trait (async append + durable-offset watch) + Local
                      actor wrapping Commitlog<Txdata<T>>; History trait for replay
crates/snapshot     — SnapshotRepository: content-addressed page/blob snapshots
crates/fs-utils     — lockfiles (flock + create_new flavors), seekable zstd, DirTrie
crates/engine       — RelationalDB::open recovery sequencing, SnapshotWorker
crates/datastore    — Replay decoder/visitor (schema-aware log replay)
```

The commitlog crate knows nothing about rows or schemas — records are any `T: Encode`, and reading requires a caller-supplied `Decoder` (`crates/commitlog/src/payload.rs`). This is what lets the same log bytes serve local replay, debugging tools (`fold_transactions` with a zero-sized decoder), and byte-level replication mirroring.

## 2. Segment format

File naming (`crates/paths/src/server.rs`): segments are `{min_tx_offset:020}.stdb.log`, offset indexes `{min_tx_offset:020}.stdb.ofs`, in one commitlog dir per replica. The file name *is* the metadata — recovery lists the directory, parses the offsets, and sorts (`Fs::existing_offsets`, `crates/commitlog/src/repo/fs.rs`).

**Segment header** — 10 bytes (`segment::Header`, `crates/commitlog/src/segment.rs`):

```
MAGIC "(ds)^2" (6 bytes) | log_format_version u8 (=1) | checksum_algorithm u8 (=0 CRC32c) | 2 reserved
```

**Commit framing** — the unit of storage is a `Commit` (batch of `n` transactions), not a transaction (`commit::Header`, `crates/commitlog/src/commit.rs`):

```
min_tx_offset u64 LE | epoch u64 LE | n u16 LE | len u32 LE | records[len] | crc32c u32 LE
```

Notable properties:

- **CRC32C covers header + payload** (via `Crc32cWriter`), so a corrupted offset/count/length field is detected, not just a corrupted body.
- **Epoch in every commit**: `Commit::epoch` is documented as "the monotonically increasing term number of the leader … in a distributed deployment". `Commitlog::set_epoch` flushes pending data before bumping; a lower epoch is rejected (`io::ErrorKind::InvalidInput`). The log format itself carries fencing lineage — this is a v1 addition (v0 headers lacked epoch; `Header::decode_v0` handles them via the segment header's `log_format_version`).
- **All-zeros header = logical EOF** — this is deliberate, so segments can be preallocated with `fallocate(FALLOC_FL_KEEP_SIZE)` (feature `fallocate`, Linux-only) without zero-fill being mistaken for corruption.
- A commit holds at most `u16::MAX` transactions; transaction offsets inside a commit must be contiguous (`Writer::commit` rejects gaps *before* writing anything).

## 3. Writer path

`Commitlog<T,R>` (`crates/commitlog/src/lib.rs`) is an `RwLock<Generic<R,T>>`; `Generic` holds `head: Writer<SegmentWriter>` plus `tail: Vec<u64>` (segment min-offsets, `crates/commitlog/src/commitlog.rs`).

- **Buffering**: each segment writer is a `BufWriter` with `Options::write_buffer_size` (default **128 KiB**). `commit()` serializes into an in-memory `Commit.records` buffer, then writes the framed commit into the BufWriter. `flush()` pushes to the OS; `sync()` calls `File::sync_data`.
- **Write/fsync failure = panic, deliberately.** If writing the commit fails, they "don't know how much of the commit has been written" so `Writer::commit` panics rather than continuing with a corrupt tail; `Generic::sync` likewise panics on fsync failure ("an fsync failure leaves a file in a more or less undefined state … forcing the user to re-read the state from disk") — the post-fsyncgate stance: never retry fsync. A `panicked` flag suppresses the `Drop` impl's `flush_and_sync` after any failed I/O.
- **Rotation**: when `head.len() >= max_segment_size` (default **1 GiB**), the head is flushed, fsynced, and a new segment created (`Generic::commit` → `start_new_segment`). Segment creation is atomic and durable: header written to a tempfile, `sync_all()`, then rename into place; a sibling `.lock` flock file guards the create-if-not-exists race (`Fs::create_segment`). *Observation:* the parent directory is not explicitly fsynced after the rename — snapshot code does fsync directories (`FileOrDirPath::sync_all`, `crates/snapshot/src/lib.rs`), the commitlog repo does not; on strict-POSIX semantics the new dirent could be lost, though the durable offset only advances after data fsync.
- **fsync policy / group commit** lives one layer up, in `crates/durability/src/imp/local.rs`. `Local::append_tx` pushes a `PreparedTx` into a bounded `async_channel` (capacity `4 × batch_capacity`, `batch_capacity` default **4096**) and returns immediately. A background **actor** drains the whole queue in one gulp (`recv_many`), writes each tx as its **own commit** on a blocking thread, then performs **one `flush_and_sync` per batch** — classic group commit whose batch size adapts to load. Only after the fsync succeeds does it publish the new durable offset into a `tokio::sync::watch` channel (`DurableOffset`). Notably, a code comment explains why one tx per commit despite the format supporting batching: "a commit is an atomic unit of storage (a torn write will corrupt all transactions contained in it), and it is very unclear when it is both correct and beneficial to bundle more than a single transaction."
- **Ack semantics**: the reducer result is sent as soon as the tx commits in memory — durability is asynchronous (same stance as Fluxum STG-012) — but because the durable offset is a first-class watch channel, clients can opt into *confirmed reads* that block on `DurableOffset::wait_for(offset)` (`crates/core/src/client/client_connection.rs`), and replication/quorum layers can gate on it.
- **Single-writer lock**: `Local::open` takes an OS advisory lock on `<replica_dir>/db.lock` (`flock(2)` via `fs2`; `LockedFile` in `crates/fs-utils/src/lockfile.rs`) writing `pid=…;timestamp_utc=…` metadata. flock means kill -9 cannot leave a stale lock.

### Offset index

A sparse per-segment index (`crates/commitlog/src/index/indexfile.rs`): a **memory-mapped, fixed-capacity file** of 16-byte `(tx_offset u64, byte_offset u64)` LE entries, capacity `max_segment_size / offset_index_interval_bytes` (default: one entry per **4096 bytes** written). Lookup is binary search for greatest-key ≤ target. Two crash-safety mechanisms:

1. `Options::offset_index_require_segment_fsync` (default **true**): an index entry is only appended after the segment has been fsynced, so the index never references non-durable bytes. Flushing the mmap is `flush_async` (msync) — best-effort.
2. Readers **never trust the index blindly**: `segment::seek_to_offset` decodes and checksum-verifies the commit at the indexed byte offset and checks its `min_tx_offset` matches the index key before seeking (`validate_commit_at_byte_offset`). Any index failure degrades to sequential scan with a warning — the index is an optimization, never load-bearing.

## 4. Reader / replay traversal

Free functions (`commits_from`, `transactions_from`, `fold_transactions_from` in `crates/commitlog/src/lib.rs`) traverse read-only without the open-for-write consistency pass. The `Commits` iterator (`crates/commitlog/src/commitlog.rs`) implements unusually careful sequencing logic (`CommitInfo`):

- **Contiguity enforced**: a commit whose `min_tx_offset` isn't the expected next offset yields `Traversal::OutOfOrder { expected, actual, prev_error }` — and it preserves the *previous* segment-tail error so a torn write followed by a gap reports the root cause, not just the symptom.
- **Duplicates skipped**: same offset *and* same CRC as the last seen commit ⇒ silently skipped (happens when a write was retried into a new segment after a crash — the corrupt tail and its rewrite coexist).
- **Forks detected**: same offset, *different* CRC ⇒ `Traversal::Forked { offset }`. The checksum doubles as a history-divergence detector — a replication primitive living in the storage layer.
- **Torn tail tolerated**: an error decoding the *last* commit of the log is not an application error — "a subsequent append will bring the log into a consistent state". `fold_transactions` swallows it automatically (it peeks whether the iterator is exhausted); plain iterator consumers are documented to do the same check.
- Checksum mismatches surface as typed `Traversal::Checksum { offset }` (`error::ChecksumMismatch` downcast), distinct from I/O errors. Property tests flip every bit position (`crates/commitlog/src/tests/bitflip.rs`) and truncate at every byte (`crates/commitlog/src/tests/partial.rs`).

`Decoder` receives `(log_format_version, tx_offset, reader)` per record — version-aware decoding per segment. Crucially there is **no per-record length prefix for mutations**, so even *skipping* a record requires schema knowledge (`Decoder::skip_record` → `Visitor::skip_row` decodes each row) — see §7.

## 5. Open/resume: repair without destroying evidence

`Generic::open` (`crates/commitlog/src/commitlog.rs`) + `resume_segment_writer` (`crates/commitlog/src/repo/mod.rs`) implement the recovery-of-the-writer path:

1. List segments; take the newest. If it contains ≤ header bytes (**empty segment**, e.g. crash right after rotation), delete it and try the previous one — loops.
2. `segment::Metadata::extract` traverses the segment to find the true end: it first walks the **offset index backwards** (`find_valid_indexed_commit`) validating candidate commits until one checksums clean, then scans forward commit-by-commit, verifying checksums and offset contiguity, accumulating `tx_range`, `size_in_bytes`, `max_epoch`.
3. Outcomes (`ResumedSegment`): `Resumed(writer)` — clean; `Corrupted(meta)` — a valid prefix exists but the tail is torn: the segment is **left as-is** (corrupt bytes and all) and a **new segment** is started at `meta.tx_range.end`; `Sealed(meta)` — segment is zstd-compressed (immutable), also start a new one; `Empty` — delete, recurse.
4. On clean resume, trailing garbage *shorter than a commit header* (invisible to `read_exact`-based traversal) is removed: `assert!(actual_len < meta.size_in_bytes + commit::Header::LEN)` then `ftruncate` to the validated size, plus `fallocate` and seek-to-end. **fsync after ftruncate** is done in the explicit truncation path ("Some filesystems require fsync after ftruncate", `reset_to_internal`).
5. Key design choice: **corruption on open does not truncate**. The torn tail is superseded by rewriting the same offsets into the new segment; readers reconcile via the duplicate/fork logic of §4. Truncation is reserved for the *explicit* `reset_to(offset)` API (delete whole newer segments, then binary-scan and `ftruncate` segment + index) — used by the replication layer when a diverged suffix must actually be removed.
6. `committed_meta()` (`CommittedMeta::{Complete, Prefix}`) lets callers learn the durable offset — including "valid prefix + trailing garbage" — without opening for write and without creating a segment in an empty repo.

Edge cases with dedicated handling: first commit of newest segment corrupt (hard error — can't establish durable offset), more than one trailing empty segment (reported as `Prefix` anomaly), sealed-vs-writable detection via zstd magic sniffing (`ReadOnlySegment::sealed`), log-format-version mismatch between options and existing segment (refuse to resume; new segment instead).

## 6. Compression & retention

- Sealed segments are compressed **in place** into *seekable* zstd archives with 4 KiB max frames (`repo::segment_compressor`, `crates/fs-utils/src/compression.rs`) — random access into a compressed segment decompresses ~4 KiB on average, so compressed segments remain usable for replay *and* for serving replication from arbitrary offsets. Compression = sealing: the active head segment is never compressed (`Commitlog::compress_segments` refuses).
- `CompressReader` sniffs the zstd magic so all read paths are format-transparent.
- The OSS crate has **no automatic retention/deletion**: `reset` / `reset_to` exist, sizes are observable (`SizeOnDisk`), and the `History::tx_range_hint` min-offset explicitly supports a log whose prefix was archived elsewhere — the trim/archive policy lives in the (closed) control plane. The engine enforces the safety invariant instead: a snapshot must be "connected" to the log (`min_commitlog_offset ≤ snapshot_offset + 1`) or recovery refuses (`RestoreSnapshotError::NoConnectedSnapshot`).

## 7. Tx payload encoding (`Txdata`)

`crates/commitlog/src/payload/txdata.rs` — the canonical record type `Txdata<T>`:

```
flags u8 (HAVE_INPUTS | HAVE_OUTPUTS | HAVE_MUTATIONS)
[Inputs]    u32 len-prefix | u8 name-len | reducer_name (≤255) | reducer_args bytes
[Outputs]   u8 len | reducer error string
[Mutations] varint n × ( table_id u32 | varint m × BSATN row )   -- inserts
            varint n × ( table_id u32 | varint m × BSATN row )   -- deletes
            varint n × table_id u32                              -- truncates
```

- **Full rows, not diffs** — deletes carry the full row value (matches Fluxum's `TableMutation` inserts; Fluxum deletes carry only PKs).
- The log stores *what the tx did* (mutations) **and** *what caused it* (reducer name + args + error output) — inputs/outputs enable auditing and potential deterministic re-execution, at the cost of log volume.
- Rows are **BSATN, not self-describing**: decoding (and even skipping) requires the row type. Hence the `Visitor` trait (`visit_insert/visit_delete/skip_row/visit_truncate/visit_tx_start/visit_tx_end`) — the decoder is *stateful* because "the requirement to store schema information in the log itself" means schema is resolved dynamically during traversal.

### Schema at replay time

`crates/datastore/src/locking_tx_datastore/replay.rs` is the sobering part: ~1,000 lines of `ReplayVisitor`/`ReplayCommittedState` to replay a log **across schema changes**, because DDL is just system-table row mutations in the same log:

- Row types are looked up live from the in-flight `st_table`/`st_column` state as replay progresses.
- Within one transaction, inserts are replayed before deletes, so a column-type migration shows *two* `st_column` rows for the same column mid-tx → `replay_columns_to_ignore` bookkeeping; an `st_table` update looks like insert-then-delete → `replay_table_updated`; a dropped table's row deletes arrive after its `st_table` delete → `replay_table_dropped`.
- **Constraints are deliberately NOT checked during replay** (long in-code essay: constraints evolve over the log; they were already checked at original commit; op order within a committed tx is irrelevant). Indexes are built **after** replay (`rebuild_state_after_replay` → `reschema_tables`, `build_missing_tables`, `build_indexes`, `build_sequence_state`), and sequences/auto-inc state is *reconstructed at the end*, not tracked during replay.
- Historical-bug fixups run post-replay (`fixup_delete_duplicate_system_sequence_rows`, `fixup_delete_orphaned_st_event_table_rows`) — old logs written by buggy versions must still replay; the replay path accumulates version archaeology forever.
- `ErrorBehavior::FailFast` in production ("we'd rather get a hard error than showing customers incorrect data"); `Warn` mode exists for forensic replay of broken logs.
- `visit_tx_start` verifies the offset equals the expected `next_tx_offset` — the snapshot/log seam check.

## 8. Snapshots

`crates/snapshot/src/lib.rs`. A snapshot is a **directory**, not a file: `{tx_offset:020}.snapshot_dir/` containing `objects/` (a `DirTrie` fan-out of **content-addressed** files keyed by blake3 hash) plus `{tx_offset:020}.snapshot_bsatn`.

- **Objects** = serialized memory **pages** (BSATN of `spacetimedb_table::page::Page`) and large **blobs**. The snapshot file is `blake3(snapshot_bsatn) || bsatn(Snapshot)` where `Snapshot { magic: "txyz", version: 0, database_identity, replica_id, module_abi_version: [7,0], tx_offset, blobs: Vec<BlobEntry{hash,uses}>, tables: Vec<TableEntry{table_id, page hashes}> }`.
- **Incremental by hardlink**: unmodified pages are hardlinked from the previous snapshot's object repo (`DirTrie::hardlink_or_write`); pages cache their `unmodified_hash`. Frequent snapshots cost only the changed pages.
- **Two-phase durable creation** (`UnflushedSnapshot`): phase 1 (datastore locked) writes all objects *without any fsync* and holds a `Lockfile` (create_new-style `.lock`); phase 2 (`sync_all`, off the lock) fsyncs every object file + parent dirs + object-repo root, then writes and fsyncs the snapshot file, then removes the lockfile. **The snapshot file's existence is the commit record**; `all_snapshots()` skips dirs whose lockfile exists or whose snapshot file is missing — a crash mid-snapshot self-heals into "snapshot doesn't exist".
- **When taken**: on **commitlog segment rotation** — `Fs::create_segment` fires the `on_new_segment` callback wired by `Local::open`, which calls `SnapshotWorker::request_snapshot_ignore_closed` (`crates/engine/src/snapshot.rs`). Cadence is therefore *bytes of log* (1 GiB), not tx count or wall clock, which directly bounds replay work. The worker is an async actor that takes a brief read lock on committed state; a panicking snapshot worker never blocks the durability actor.
- **Invalidation, not deletion**: bad or superseded snapshots are renamed to `.invalid_snapshot` (`invalidate_snapshot` / `invalidate_newer_snapshots`).
- **Restore** (`read_snapshot`): verifies blake3 of the snapshot file, then of *every* page and blob against the recorded hashes; deserializes pages via the page pool. Returns `ReconstructedSnapshot { tables: BTreeMap<TableId, Vec<Box<Page>>>, blob_store, tx_offset, module_abi_version, … }` — schema is *not* stored separately; it is recovered from the system-table pages inside the snapshot itself.
- Snapshot **compression** mirrors the commitlog: zstd-in-place, hardlinking already-compressed objects from the parent snapshot, snapshot file compressed **last** as the "whole snapshot compressed" marker.
- `crates/snapshot/src/remote.rs`: async streaming snapshot transfer by content hash (`BlobProvider`, `synchronize_snapshot`, `verify_snapshot`) — objects fetched individually, hardlinked when already present locally. This is the full-sync seeding path for replicas.

## 9. Recovery sequence (engine)

`RelationalDB::open` (`crates/engine/src/relational_db.rs:279`):

```
1. durable_tx_offset ← durability layer (from commitlog committed_meta / watch)
2. (min_commitlog_offset, max) ← history.tx_range_hint()
3. restore_from_snapshot_or_bootstrap:
   a. invalidate_newer_snapshots(durable_tx_offset)      // snapshot/log divergence
   b. loop: latest_snapshot_older_than(upper_bound)
        - reject if not "connected": min_commitlog_offset > snapshot_offset + 1
        - identity mismatch  → hard error
        - permanent error (HashMismatch/BadMagic/BadVersion/Deserialize)
                             → invalidate_snapshot, try next older
        - transient error (I/O, Incomplete-lockfile, …)
                             → try next older WITHOUT invalidating
   c. no snapshot: bootstrap fresh iff min_commitlog_offset == 0,
      else RestoreSnapshotError::NoConnectedSnapshot
4. apply_history: fold_transactions_from(replay.next_tx_offset)   // snapshot.tx_offset + 1
   with per-tx offset contiguity check (ReplayError::InvalidOffset)
5. rebuild_state_after_replay (indexes, sequences, fixups)
6. migrate_system_tables  — snapshots/logs may predate newer system tables
7. identity/owner sanity check against st_module; report still-connected clients
```

Two subtleties worth stealing:

- Step 3a exists because **snapshots don't record the epoch**: after a leader failover resets the log to offset 9, a pre-existing snapshot at offset 10 is silently wrong — `create_snapshot` *also* calls `invalidate_newer_snapshots(tx_offset − 1)` on every snapshot to avoid using a diverged snapshot as hardlink parent (comment at `crates/snapshot/src/lib.rs:848`).
- The transient/permanent error split means one corrupt snapshot degrades to an older one plus longer replay — recovery never fails just because the newest snapshot is bad, and permanently-bad snapshots are quarantined so future snapshots don't hardlink against them.

## 10. Replication hooks in the OSS code

There is no leader election / quorum machinery in OSS (that's the hosted control plane), but the storage layer ships every primitive a replication protocol needs:

- **Epoch in every commit header** + `Commitlog::set_epoch` / `Generic::epoch` (fencing lineage recorded durably in the log itself; `Metadata::max_epoch` recovered on open).
- **Fork detection** at read time (`Traversal::Forked`, same offset ≠ same checksum).
- **`stream` feature** (`crates/commitlog/src/stream/`): the log *is* the wire format.
  - `stream::reader::commits(repo, range)` yields the **raw bytes** of segment headers and commits covering a tx-offset range, using the offset index to seek — "mirroring commitlogs over the network".
  - `stream::writer::StreamWriter` consumes such a stream into a local repo: creates a new local segment whenever a segment MAGIC appears in-stream, verifies each commit's CRC and offset contiguity **without decoding payloads**, maintains a local offset index, and fsyncs on segment close. On (re)create it validates the local tail with `OnTrailingData::{Error, Trim}` — `Trim` ftruncates a torn tail (segment + index) before resuming append. `Progress::range_written` callbacks report applied ranges (ack offsets).
- **`durability::History`** decouples "where the log comes from" from replay; `RelationalDB::apply(history)` notes the exclusivity restriction "may be lifted in the future to allow for 'live' followers".
- **Remote snapshot sync** (`crates/snapshot/src/remote.rs`) = full-sync seeding.
- `crates/dst/src/sim/commitlog.rs`: a deterministic-simulation commitlog for DST — relevant to Fluxum SPEC-013.

So: **yes — their log format doubles as the replication stream, byte-identically, exactly as Fluxum plans** (SPEC-002 STG-016 / SPEC-014 REP-010), and it works because commit framing carries offset + epoch + CRC in a fixed header that a receiver can validate without payload knowledge.

## What Fluxum will face

Comparison against SPEC-002 (STG-*) and SPEC-014 (REP-*), in rough priority order.

**1. Recovery edge cases SPEC-002 misses.** STG-030/031 describe "truncate at first corrupt entry" and stop. SpacetimeDB handles a much longer tail of real-world cases that our acceptance criteria (crash suite, bit-flip drills) will surface:

- *Trailing garbage shorter than one header* — invisible to `read_exact` traversal; they explicitly `ftruncate` it on resume with an assert bounding it (`resume_segment_writer`). Our replay loop needs the same distinction between "clean EOF", "partial header", and "partial body".
- *All-zero bytes = preallocation sentinel, not corruption* — required if we ever fallocate segments.
- *Empty segment at the tail* (crash between rotation and first commit) — delete and recurse to the previous segment; also the "more than one empty segment" anomaly.
- *Duplicate entries after crash-retry* (same tx_id, same CRC → skip) vs *fork* (same tx_id, different CRC → error). STG-015 only says "decrease or repeat = corruption" — but a repeat is *normal* after a torn write followed by rewrite. We need the same-CRC exemption, and the different-CRC case is precisely SPEC-014's divergence detection (REP-013) falling out of the storage layer for free.
- *Corrupt first entry of the newest segment* (can't establish a durable offset → hard error, not silent empty log).
- *fsync after ftruncate* on the truncation path; *fsync-failure = panic, never retry*; *segment header made durable before the segment is used*. Also decide our stance on parent-directory fsync for segment creation (SpacetimeDB fsyncs dirs for snapshots but not for new segments — we should do both).
- Consider their non-destructive philosophy: on open, keep the corrupt tail and start a new segment rather than truncating (STG-031 mandates truncation). For single-node Fluxum truncation is acceptable; once SPEC-014 lands, truncating at open destroys the evidence needed to distinguish "torn local write" from "diverged suffix under an old epoch". Recommend: keep STG-031's *logical* behavior (replay stops at first corrupt entry) but make physical truncation an explicit `reset_to`-style operation invoked by the replication layer, as they do.

**2. Snapshot/log divergence — SPEC-002 has a gap.** STG-030 trusts "the latest valid checkpoint" unconditionally. SpacetimeDB (a) computes the durable log offset *first* and only considers snapshots ≤ it, (b) invalidates (renames, never deletes) newer snapshots, (c) requires the snapshot to be *connected* to the retained log (`min_offset ≤ snapshot_offset + 1` — critical once STG-013 compaction deletes old segments), and (d) falls back to older snapshots on corruption, distinguishing transient from permanent errors. Fluxum's recovery sequence should adopt all four. Related gap: **SPEC-002's `Snapshot` struct has no integrity checksum at all** (CRC only appears in the SPEC-014 backup manifest, REP-061); SpacetimeDB blake3-verifies the manifest *and every object* on restore, and our checkpoint-equivalence acceptance criterion can't detect silent checkpoint corruption without it. Add a whole-file hash (and per-table or per-chunk hashes if the checkpoint gets chunked) to STG-021.

**3. Checksum coverage and framing.** STG-011 CRCs only the MessagePack body; the `u32` length prefix is outside the checksum, so a corrupted length mis-frames the rest of the segment and the failure is misreported. SpacetimeDB's CRC covers the full header (offset, epoch, count, length). Also, `tx_id` living *inside* the MessagePack body means monotonicity/contiguity checks require payload decode, whereas their fixed header lets `StreamWriter` validate a replication stream without deserializing rows — cheap replica-side validation (REP-014 steps 1–2) argues for pulling `tx_id` (and length) into a checksummed fixed header before the G5 freeze.

**4. Epoch placement.** REP-004 deliberately keeps epochs out of the frozen `TxRecord`, envelope-only. SpacetimeDB made the opposite call — epoch in every stored commit — and their snapshot-invalidated-on-failover workaround (§9) shows why it matters: without epochs in durable artifacts, PITR lineage (REP-072), divergence truncation (REP-013), and snapshot validity after failover all need a *separate durably-persisted epoch→offset map*, and checkpoints need epoch stamps too. If the G5 freeze is still open, an epoch (or `(epoch, tx_id)` pair) in the entry header is the single cheapest insurance for the whole SPEC-014 program; if not, SPEC-014 must specify the epoch-map persistence and checkpoint-invalidation-on-promotion rules explicitly (currently unstated).

**5. Real fsync/latency numbers to copy.** Their production trade-off is *not* "no fsync" (our STG-012 phrasing) — it is **continuous group commit**: bounded queue (4×4096), drain-all batching, one fsync per batch, durable offset published via a watch channel, ack decoupled from durability. Under load this amortizes fsync to near-zero per-tx cost; when idle it degrades to fsync-per-tx *without* adding ack latency, so the durability gap is typically far below our 50 ms bound. Recommend replacing STG-012's "OS write-behind only" with this actor + group-fsync + published-durable-offset design: it costs little, makes the crash-suite window deterministic (queue depth × fsync latency instead of OS flush timing), and the `DurableOffset` watch is exactly the primitive REP-021 semi-sync needs (quorum-ack = wait on N members' durable offsets) plus enables opt-in confirmed reads. Other defaults worth adopting: 128 KiB write buffer; sparse mmap offset index (1 entry / 4 KiB) with entries appended only post-fsync and always validated on read; 1 GiB segments (our 128 MB is fine, but note their snapshot cadence is tied to rotation).

**6. Checkpoint format will be our scaling cliff.** STG-020/021: full MessagePack dump every 10,000 tx. SpacetimeDB snapshots are content-addressed pages with hardlink dedup — an incremental snapshot costs only changed pages, which is what makes snapshot-per-1-GiB-of-log affordable and keeps replay bounded by log bytes, not database size. A full-dump checkpoint every 10k tx on a 10 GB database re-writes 10 GB each time; either raise the interval (hurting the STG-032 30 s recovery target, since replay grows) or move to dedup/incremental checkpoints. At minimum: switch the trigger from tx-count to log-bytes (rotation-driven, like theirs) so checkpoint cost tracks write volume, and adopt their two-phase creation (write-unsynced-under-lock, fsync-off-lock, manifest-file-last-as-commit-record, lockfile marks in-progress) to satisfy STG-022's non-blocking requirement with crash-safe semantics — a half-written Fluxum checkpoint must be self-identifying garbage, which STG-021's single-file format only achieves if the file is written via tempfile+rename or hash-verified.

**7. Schema at replay time — SPEC-002 understates it.** STG-050's "stable CRC32 table IDs enable replay without live schema lookup" only solves table *identity*. Once SPEC-010 migrations exist, the log spans schema versions and replay must apply DDL mid-stream; SpacetimeDB's 1,000-line `ReplayVisitor` (mid-transaction ordering hazards, ignore-sets, post-replay reschema) is the price of schema-in-the-log with non-self-describing rows. Fluxum's MessagePack rows are self-describing at the *decode* level (we can always skip a record — they can't), which removes the worst constraint, but semantic mapping across migrations remains. Concrete rules to adopt now: (a) **do not check constraints during replay**; (b) build secondary/spatial indexes *after* replay, not during; (c) rebuild auto-inc counters (STG-040) in a post-replay pass; (d) keep a `FailFast` vs `Warn` replay mode for forensics; (e) accept that replay code accumulates compatibility fixups forever — version the log format from day one (our format has *no version field per segment*; add the segment-header version byte before G5).

**8. Replication stream mechanics (SPEC-014 validation).** Their `stream` module confirms our central REP-010 bet and supplies the missing details: include *segment headers* in the stream so replica logs are byte-identical including boundaries; validate CRC + contiguity receiver-side without payload decode; on session (re)create, trim the replica's torn tail (`OnTrailingData::Trim`) before appending — REP-014 should state this pre-append tail check explicitly; use the offset index for partial-sync seeks (REP-013); compress sealed segments with *seekable* zstd (small frames) so archived/compressed segments can still serve partial sync and PITR without full decompression — REP-062's archive format should specify seekable framing. For REP-012 full sync, their content-addressed remote snapshot sync (hash-addressed objects, hardlink dedup, resumable per-object) is far more failure-tolerant than our single-blob checkpoint transfer.

**9. Locking.** SPEC-002 never specifies a data-dir lock, but STG-003's single-writer guarantee depends on one. Use their split: **flock-style advisory lock** (stale-proof after kill -9, pid+timestamp metadata for diagnostics) on the shard data dir; **create_new lockfile-as-state-marker** only where a leftover lock correctly means "artifact incomplete" (checkpoint dirs), with recovery treating locked artifacts as nonexistent.

**10. Minor but load-bearing details.** Zero-padded fixed-width offset filenames (`{:020}`) make directory sort order = offset order (our `shard-<id>-<first_tx_id>.log`, STG-014, sorts wrong past 10-digit tx_ids without padding); "empty log" must be distinguishable from "offset 0 durable" (their `Option<u64>` durable offset — our `recovered_tx_id = 0` conflates them, off-by-one risk at STG-015 resume); require strict tx_id *contiguity*, not mere monotonicity — every consistency check they have (dup/fork/seek/stream validation) leans on exact `prev + n` arithmetic.
