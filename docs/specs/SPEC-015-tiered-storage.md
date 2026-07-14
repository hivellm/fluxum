# SPEC-015 — Tiered Storage & Compression

| | |
|---|---|
| **Status** | Draft — page format freezes at gate G5 |
| **Phase / tasks** | Phase 2 · T2.8–T2.9 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-10, FR-18, FR-19, FR-110; NFR-02, NFR-07, NFR-12, NFR-13 |
| **Requirement prefix** | `TIER-` |
| **Source** | New — replaces the all-in-RAM assumption inherited from SpacetimeDB |

Requirement IDs `TIER-xxx`. Integers little-endian unless stated otherwise. This spec owns the
**buffer pool**, the **paged cold tier** (its own on-disk page format), and **compression**.
SPEC-002 owns `MemStore`/MVCC semantics, the `CommitLog`, and the recovery sequence — this spec
refines the physical representation of `CommittedState` without changing any semantics defined
there. The on-disk page format is part of the **format freeze at gate G5**: replication and
point-in-time recovery replay it, so it carries an explicit version and MUST evolve only by
version bump.

## 1. Overview

SpacetimeDB requires the whole dataset in RAM. Fluxum does not — committed state is **tiered**,
in the mold of PostgreSQL's buffer-pool model but with a realtime-first hot path:

```
                      memory.budget (auto = f(RAM, cgroup limits))
        ┌─────────────────────────────────────────────┐
        │                BUFFER POOL                  │   hot: < 1 µs reads,
        │   uncompressed pages, clock-LRU eviction    │   zero disk I/O
        └───────────────▲───────────────┬─────────────┘
                fault in │              │ evict (spill, compress)
        ┌───────────────┴───────────────▼─────────────┐
        │              PAGED COLD TIER                │   cold: one page-in
        │   fixed-size pages, FluxBIN rows, CRC32C,   │
        │   LZ4 per page (zstd for checkpoints)       │
        └─────────────────────────────────────────────┘
```

- The **hot working set** lives uncompressed in the buffer pool: microsecond reads, zero disk
  I/O (FR-10, NFR-02, NFR-07).
- **Cold data** lives in a paged, compressed on-disk store in Fluxum's own format; pages fault
  into the pool on demand and are evicted under the memory budget (FR-18).
- **Durability is never the cold tier's job.** Writes land in the hot tier and the `CommitLog`
  (SPEC-002); recovery is always checkpoint pages + log replay. The cold tier exists so that
  datasets are bounded by disk, not RAM (NFR-12, NFR-13).

Pages are the unit of I/O, eviction, compression, and checksums. Indexes are paged too. Tiering
is **invisible above the storage API**: the transaction (SPEC-003), reducer (SPEC-004), and
subscription (SPEC-005) layers see one logical `CommittedState`. These types live in
`fluxum-core` (`store/` and `pager/` modules) and have no network dependencies.

## 2. Memory budget

- **TIER-001** [P0] **One knob.** The system SHALL expose a single memory ceiling in
  `config.yml`: `memory.budget: auto | <bytes>` (default `auto`), overridable via
  `FLUXUM_MEMORY_BUDGET`. The budget is **per process** and is shared by all shards hosted in
  that process. An explicit `<bytes>` value SHALL be honored as given; values below the 128 MiB
  floor SHALL be rejected at boot with a configuration error.

- **TIER-002** [P0] **`auto` derivation.** When `memory.budget: auto`, the effective budget
  SHALL be derived from the hardware/container detection layer (SPEC-016; cgroup-detection
  crate choice tracked by PRD OQ-10) as:

  ```
  effective_limit = min(detected_physical_ram, cgroup_memory_limit)   # cgroup term absent → ignored
  budget          = max(128 MiB, auto_fraction × effective_limit)     # auto_fraction default 0.5
  ```

  Both the fraction (`memory.auto_fraction`, default `0.5`) and the floor
  (`memory.auto_floor_bytes`, default 128 MiB) are **tunable defaults**, not frozen constants.
  Detection SHALL honor cgroup v1/v2 limits when present (container deployments); if detection
  fails entirely, the system SHALL fall back to the floor and emit a structured warning
  (SPEC-012). On the reference 1 vCPU / 512 MB droplet profile this yields a 256 MiB budget
  (NFR-12).

- **TIER-003** [P0] **Enforcement via the buffer pool.** The budget SHALL be enforced primarily
  through the buffer pool: pool capacity = `memory.bufferpool_fraction × budget` (default
  `0.8`; the remainder is headroom for `TxState`, subscription buffers, connection state, and
  allocator slack). The pool SHALL NOT allocate frames beyond its capacity. A fault-in that
  finds no evictable frame (all frames pinned) SHALL fail with
  `FluxumError::BufferPoolExhausted` — causing the calling transaction to roll back per
  SPEC-002 STG-006 — rather than allocate past the ceiling.

- **TIER-004** [P0] **Bounded RSS.** Steady-state process RSS SHALL NOT exceed
  `budget + tolerance`, where `tolerance = max(64 MiB, 0.10 × budget)` (tunable via
  `memory.budget_tolerance_bytes`). Process RSS MUST NOT grow unbounded with dataset size: it
  is a function of the budget and configuration, never of the number of rows on disk (FR-110).
  This bound is asserted by the droplet-profile CI job (NFR-12).

  ```
  Given  a shard whose on-disk dataset is 10× the memory budget
  When   a uniform-random read workload runs for one hour
  Then   process RSS stays ≤ budget + tolerance for the entire run
         and every read returns the correct committed row
  ```

- **TIER-005** [P1] **Budget transparency.** The effective budget, its derivation inputs
  (detected RAM, cgroup limit, fraction, floor), and the resulting pool capacity SHALL be
  logged at boot (`tracing`) and exposed in `/health` (SPEC-016 adaptive-tuning report,
  FR-113).

## 3. Buffer pool

- **TIER-010** [P0] **Page-granular pool.** The buffer pool SHALL manage fixed-size frames of
  `storage.page_size` bytes holding **uncompressed** page images. The frame table SHALL map
  `PageKey { shard_id: u32, table_id: u32, page_id: u64 }` to a frame in O(1) expected time.
  One pool SHALL serve all shards in the process (single budget domain); frames are tagged
  with their owning shard and table.

  ```rust
  pub struct BufferPool {
      frames: Box<[Frame]>,                       // capacity fixed at boot (TIER-003)
      map: DashMap<PageKey, FrameId>,             // page → frame lookup
      clock_hand: AtomicUsize,                    // eviction cursor (TIER-011)
  }

  pub struct Frame {
      key: PageKey,
      data: Box<[u8]>,        // page_size bytes, uncompressed
      pins: AtomicU32,        // TIER-012
      referenced: AtomicBool, // second-chance bit
      dirty: AtomicBool,      // TIER-013
  }
  ```

- **TIER-011** [P0] **Clock-LRU (second-chance) eviction.** Eviction SHALL use the clock
  algorithm: a hit sets the frame's `referenced` bit (no list manipulation, no lock on the hot
  path); the evictor sweeps a circular hand, clearing set bits and evicting the first frame
  that is unreferenced and unpinned. Clean frames SHALL be preferred over dirty frames when
  both are eligible under the hand (a clean victim is dropped instantly; a dirty victim
  requires a spill write, TIER-013).

- **TIER-012** [P0] **Pin semantics.** Frames SHALL carry a pin count. A frame SHALL be pinned
  while any of the following holds: an active transaction is reading rows from it (scan
  cursors, `query_pk`), the commit merge (SPEC-002 STG-005) is applying mutations to it, or a
  checkpoint is flushing it. Pinned frames SHALL NOT be evicted. All pins held by a
  transaction SHALL be released no later than its commit or rollback. Pool exhaustion by pins
  fails the faulting operation per TIER-003 — it never deadlocks and never over-allocates.

- **TIER-013** [P0] **Dirty-page handling.** The commit merge SHALL mark every modified frame
  dirty. A dirty frame SHALL NOT be discarded: before eviction it MUST be spilled to the cold
  tier as a copy-on-write page write (TIER-025), and only then dropped from the pool. Spills
  are an availability mechanism (so evicted-then-reread data can fault back in), **not** a
  durability mechanism: correctness after a crash never depends on spilled pages, because
  recovery is checkpoint root + `CommitLog` replay (TIER-061). Frames flushed by a completed
  checkpoint SHALL be marked clean without leaving the pool.

- **TIER-014** [P0] **Hot-path invariant.** A buffer-pool **hit** SHALL perform **zero disk
  I/O** — no syscalls, no page reads, no decompression, no frame allocation (NFR-07, FR-10).
  A committed-state point lookup that hits the pool SHALL complete in < 1 µs (NFR-02). The
  read path on a hit is: frame-table lookup → set `referenced` bit → read row bytes. This
  invariant SHALL be guarded by an instrumented assertion in debug builds and by the
  acceptance test in §12.

  ```
  Given  a page resident in the buffer pool
  When   a transaction reads a row on that page via query_pk::<T>
  Then   fluxum_page_reads_total does not increment,
         no I/O syscall is issued,
         and the lookup completes in < 1 µs
  ```

- **TIER-015** [P1] **Scan resistance.** Pages faulted in by full-table scans SHOULD be
  inserted with the `referenced` bit clear so that a single large scan cannot flush the
  resident working set (one clock sweep reclaims scan pages first).

## 4. Paged cold tier — on-disk format

- **TIER-020** [P0] **Own format, zero storage dependencies.** The cold tier SHALL be
  implemented in Fluxum's own on-disk page format using standard file I/O. It MUST NOT depend
  on RocksDB, LMDB, SQLite, or any other embedded storage engine (PRD §8: no external runtime
  dependencies). Rationale: full control over the hot path, the freeze surface, and the
  single-binary profile.

- **TIER-021** [P0] **Page layout.** Every on-disk page SHALL consist of a 32-byte header
  followed by the payload. Rows in leaf-page payloads are FluxBIN-encoded (SPEC-006). All
  integers little-endian:

  | Offset | Size | Field | Notes |
  |---|---|---|---|
  | 0 | 4 | `magic: u32` | bytes `"FLXP"` (`0x46 0x4C 0x58 0x50`), read as `0x50584C46` LE |
  | 4 | 8 | `page_id: u64` | unique per (shard, table) |
  | 12 | 4 | `table_id: u32` | SPEC-002 STG-050 stable ID |
  | 16 | 4 | `row_count: u32` | rows (leaf) or entries (interior) in this page |
  | 20 | 2 | `flags: u16` | see below |
  | 22 | 2 | `reserved: u16` | zero; MUST be ignored on read |
  | 24 | 4 | `payload_len: u32` | stored payload bytes (post-compression) |
  | 28 | 4 | `crc32c: u32` | over bytes 0..32 (this field zeroed) + payload — CRC32C (Castagnoli), hardware-accelerated |
  | 32 | … | payload | FluxBIN rows / index entries, possibly compressed |

  The `crc32c` field is the **per-page integrity hash**: a fast, hardware-accelerated hash
  (CRC32C via SSE4.2/ARMv8 CRC instructions; a BLAKE3-class hash MAY additionally be
  maintained per page for content addressing, TIER-063) that MUST be verified on every
  fault-in (TIER-032) before the page's contents are served. *(adopted from SpacetimeDB
  analysis, files 02/03)*

  `flags` bit assignments: bits 0–1 = compression codec (`0` none, `1` LZ4, `2` zstd,
  `3` reserved); bit 2 = index page (interior/leaf B-tree node rather than data leaf); bit 3 =
  overflow page (TIER-026); bits 8–11 = page-format version (initially `1`). Unknown flag bits
  SHALL cause the page to be rejected as unreadable (forward-compatibility guard). This layout
  freezes at gate G5; any change bumps the version bits.

- **TIER-022** [P0] **Page size.** The logical page size (uncompressed payload capacity +
  header) SHALL default to **8 KB** (`storage.page_size: 8192`), with `4096` and `16384`
  accepted. The page size is fixed at database creation, recorded in each page file's
  superblock, and immutable thereafter (changing it requires an offline rebuild). **OQ-7**
  tracks the 4/8/16 KB benchmark decision and the mmap-vs-pread read path; until it is
  resolved the default read path SHALL be `pread` into pool-owned frames — `pread` keeps
  budget accounting exact, whereas mmap hands residency control to the OS page cache. Any
  future mmap mode MUST preserve TIER-004 accounting and the TIER-014 invariant.

  *Evidence for OQ-7 (recorded, decision still open):* SpacetimeDB uses **64 KiB** in-RAM
  pages (`u16` intra-page offsets, chosen for RAM locality and amortized per-page overhead —
  it has no disk tier); disk spill favors smaller pages for random fault-in, since a fault
  reads the whole page and a smaller page reads fewer bytes per served row and pollutes the
  pool less. 16-bit intra-page offsets cap a page at 64 KiB, so the same offset scheme works
  at 4/8/16 KB with headroom. This evidence informs but does not resolve the 4/8/16 KB
  benchmark decision tracked by OQ-7. *(adopted from SpacetimeDB analysis, file 02)*

- **TIER-023** [P0] **Page files per table per shard.** Cold pages SHALL be stored under
  `storage.page_dir` (default `./data/pages`) as one page file per table per shard:
  `shard-<shard_id>/table-<table_id>.pages`. Each page file SHALL begin with a superblock
  (magic `"FLXS"`, format version, `page_size`, `shard_id`, `table_id`, CRC32C). Per-shard
  directories keep shard datasets independently movable (entity handoff and per-shard
  recovery, SPEC-007).

- **TIER-024** [P0] **Allocation and free-list.** Stored pages are variable-length physical
  records (header + compressed payload), allocated within page files at 256-byte granularity
  from a per-file **free-extent list**. Allocation SHALL be first-fit from the free list,
  extending the file when no extent fits. Extents occupied by superseded page versions SHALL
  return to the free list only after no retained checkpoint references them (SPEC-002 STG-023
  retention keeps older roots valid). Rationale for variable-length records: fixed slots would
  forfeit the disk-footprint benefit of compression (FR-19).

- **TIER-025** [P0] **Copy-on-write; torn-page protection.** A live (checkpoint-referenced)
  page SHALL NEVER be overwritten in place. Every page write — evictor spill or checkpoint
  flush — SHALL go to a freshly allocated extent, and the page directory (TIER-060) is
  repointed afterwards. Torn-page protection follows from CoW + CRC: a crash mid-write can
  only tear an extent that no durable root references yet; the torn copy fails its CRC, is
  treated as unreferenced garbage, and is reclaimed — the previous checkpoint's page version
  remains intact. No double-write buffer and no full-page images in the log are required.

  ```
  Given  a checkpoint is flushing page P to a new extent
  When   the process is killed mid-write (torn page on disk)
  Then   recovery opens the previous checkpoint root,
         the torn extent is unreferenced and later reclaimed,
         and P's prior version + log replay reproduce the committed state
  ```

- **TIER-026** [P1] **Overflow pages.** A row whose FluxBIN encoding exceeds the leaf payload
  capacity SHALL be stored as a chain of overflow pages (flag bit 3), with the leaf holding
  the key and the head overflow `page_id`. Chains SHALL be read with sequential page-ins on
  fault; each overflow page carries its own header and CRC.

## 5. Write path, eviction, and fault-in

- **TIER-030** [P0] **Writes always land hot.** All mutations SHALL be applied to buffer-pool
  frames (marking them dirty) and appended to the `CommitLog` per SPEC-002 STG-005/STG-010.
  The cold tier is NEVER on the commit path: commit latency SHALL be independent of cold-tier
  state and page-file I/O (NFR-03 is preserved unchanged). If a mutation targets a
  non-resident page, that page is faulted in (TIER-032) before the merge applies to it.

- **TIER-031** [P0] **Asynchronous eviction/spill.** Eviction and dirty-page spill SHALL run
  in a dedicated background evictor task, driven by watermarks: the evictor wakes when pool
  occupancy crosses the high watermark (default 95% of capacity) and reclaims down to the low
  watermark (default 90%), both tunable. Eviction and spill I/O MUST NOT block the shard
  writer thread: the writer never performs spill writes, compression, or free-list I/O
  inline. If the pool saturates faster than the evictor drains it, faulting operations wait
  on the evictor or fail per TIER-003 — the writer is never wedged by cold-tier I/O.

- **TIER-032** [P0] **Fault-in path.** A read miss SHALL be served by: (1) locate the page in
  the live page directory, (2) issue a **single** `pread` of the physical record, (3) verify
  the per-page CRC32C integrity hash (TIER-021 — verification on fault-in is mandatory, never
  skipped), (4) decompress the payload if the codec flag says so, (5) insert the frame into
  the pool (evicting per TIER-011 if at capacity), (6) pin and serve. Exactly one physical
  read per faulted page. Concurrent misses on the same page SHALL be coalesced single-flight
  (one I/O; all waiters share the resulting frame). A CRC mismatch is handled per TIER-062.

  ```
  Given  a page resident only in the cold tier
  When   two transactions concurrently read rows on that page
  Then   exactly one page-in I/O occurs,
         the payload is CRC-verified and decompressed once,
         and both reads are served from the same pool frame
  ```

- **TIER-033** [P1] **Cold-read latency.** A single-page fault-in on NVMe/SSD SHOULD complete
  within 2× of an equivalent PostgreSQL cold page read (NFR-11 parity target, measured by the
  `fluxum-bench` harness). Sequential scans MAY use bounded readahead, which MUST respect
  pool capacity and the memory budget.

- **TIER-034** [P1] **Warm-up.** After recovery, the pool starts empty and fills on demand.
  The system MAY prefetch hot pages recorded in a prior-run heat hint file; it MUST function
  correctly (only slower) without one.

## 6. Compression

- **TIER-040** [P1] **LZ4 per cold page by default.** Page payloads SHALL be compressed with
  LZ4 when written to the cold tier, subject to a min-size threshold: payloads smaller than
  `storage.compression_min_bytes` (default 1024) are stored raw, and a compressed result that
  saves less than 12.5% of the payload is discarded in favor of the raw bytes. The codec
  actually used is recorded per page in the header flags (TIER-021), so every page is
  self-describing.

- **TIER-041** [P1] **Codec configuration.** `storage.page_compression: lz4 | zstd | none`
  (default `lz4`) SHALL select the codec for newly written pages. `zstd` trades fault-in
  latency for ratio (suited to read-mostly archival tables); `none` disables page
  compression. Because pages are self-describing, changing the setting online is valid —
  files with mixed codecs SHALL read correctly.

- **TIER-042** [P1] **zstd for checkpoints and backups.** Checkpoint manifests, portable
  snapshot exports (SPEC-002 STG-021 format), backup archives, and replication seed images
  (SPEC-014) SHALL be compressed with zstd (default level 3, tunable via
  `storage.checkpoint_compression_level`). These artifacts are written and read off the hot
  path, where zstd's higher ratio beats LZ4's speed advantage.

- **TIER-043** [P1] **Compression ratio target.** Page compression SHALL achieve ≥ 3×
  (compressed bytes ≤ ⅓ of raw FluxBIN bytes) on the reference corpus — the SPEC-013
  conformance corpus over the canonical demo schema (`User`, `ChatMessage`, `Task`, `Sensor`)
  populated with realistic text and telemetry distributions. The T2.9 benchmark measures and
  publishes this ratio; per-table ratios are exposed as metrics (TIER-080).

- **TIER-044** [P0] **Decompress on fault-in only.** Decompression SHALL occur exactly once,
  on the fault-in path (TIER-032). The buffer pool holds only uncompressed frames; a pool hit
  never touches a codec (TIER-014). Compression/decompression SHALL round-trip bit-identically
  (property-tested, T2.9), and the kernels are eligible for SIMD dispatch with mandatory
  scalar parity (SPEC-016, FR-111/FR-112).

## 7. Paged indexes

- **TIER-050** [P0] **B-trees as pages.** The primary row map and all secondary B-tree indexes
  (SPEC-002 STG-002) SHALL be physically stored as paged B-trees: interior nodes hold keys and
  child `page_id`s, leaf nodes hold FluxBIN rows (primary) or key → primary-key entries
  (secondary), each node exactly one page with the index flag set (TIER-021). The logical
  semantics of SPEC-002 are unchanged — O(log n) point lookup, ordered range scans — and the
  `BTreeMap` types in SPEC-002 describe this logical contract, which the paged representation
  implements. Index pages participate in the pool, eviction, compression, and checkpoints
  exactly like data pages.

  **Indexes MUST be paged and evictable under the same memory budget (TIER-001–TIER-004) as
  data pages** — index memory counts against the pool, index pages fault in on demand and are
  evicted under pressure, and index residency SHALL never be assumed by any code path. This
  is explicitly called out as **the novel work in this spec**: SpacetimeDB's rows are
  page-based, but its indexes (and pointer map, blob store) are conventional heap-allocated
  `std` structures that are RAM-bound and cannot spill — there is no precedent to port, and
  no fault-in seam exists in its engine to copy. Fluxum's paged-index design (including
  eviction-safe row addressing: index entries reference logical primary keys or pinned
  physical coordinates, never bare heap pointers, so an evicted-and-refaulted page reappears
  at the same logical coordinates) must therefore be designed and validated from scratch.
  *(adopted from SpacetimeDB analysis, file 02)*

- **TIER-051** [P1] **Spatial indexes.** QuadTree and R-tree nodes (SPEC-008) SHALL likewise
  be stored as pages and faulted/evicted through the same pool. SPEC-008's linear-quadtree
  (BTreeMap-backed, no pointer chasing) representation maps directly onto the paged B-tree of
  TIER-050. Spatial queries fault node pages on demand and compete for the same budget;
  frequently queried regions stay hot naturally under clock-LRU.

## 8. Checkpoints and recovery

From T2.8 onward, a checkpoint is a **page-level copy-on-write flush**, refining SPEC-002's
`SnapshotRepo`: cadence (STG-020, `snapshot_interval_tx`), non-blocking execution (STG-022),
and retention (STG-023) carry over unchanged. The monolithic MessagePack snapshot (STG-021)
remains the portable **export** format for backups and replica seeding; it is no longer the
recovery fast path.

- **TIER-060** [P0] **Checkpoint procedure.** A checkpoint SHALL: (1) write all dirty pages
  copy-on-write to fresh extents (TIER-025), (2) write updated page-directory nodes — the
  directory is itself a CoW B-tree of pages keyed by `page_id`, so only dirtied directory
  nodes are rewritten, (3) write a manifest recording the directory root location,
  `last_tx_id`, and free-list state, (4) `fsync` in that order (data → directory → manifest),
  (5) atomically swap the root by renaming a `CURRENT` pointer file to the new manifest, and
  (6) only then mark commit-log segments with `tx_id ≤ last_tx_id` eligible for truncation
  (STG-013). Checkpointing runs in the background and SHALL NOT block reducer execution
  (STG-022); frames dirtied during the flush are handled by CoW and belong to the next
  checkpoint.

- **TIER-061** [P0] **Recovery = checkpoint pages + log replay.** Shard recovery (refining
  STG-030) SHALL: read `CURRENT` → open the manifest → adopt the page-directory root, then
  replay `CommitLog` entries with `tx_id > manifest.last_tx_id` into the hot tier. No full
  cold-tier scan is performed; page CRCs are verified lazily on fault-in. The 30 s / 10 GB
  recovery target (STG-032, NFR-06) applies to the paged engine.

- **TIER-062** [P1] **Page corruption handling.** A CRC32C mismatch on fault-in SHALL fail the
  read with `FluxumError::PageCorrupt { shard_id, table_id, page_id }`, roll back the
  affected transaction, and emit a structured operator notification (SPEC-012). Because
  retained checkpoints are immutable (TIER-024/025), the page SHALL be recoverable from an
  older retained root plus log replay. The crash suite (T2.7) SHALL include CRC bit-flip
  drills on page files, not just on the commit log.

- **TIER-063** [P1] **Content-addressed pages — shared mechanism with SPEC-002 checkpoints.**
  Each page SHALL maintain a lazily computed **content hash** (BLAKE3-class) over its
  uncompressed image, invalidated on any mutation and recomputed on demand. This hash is the
  object key under SPEC-002 STG-021's content-addressed checkpoint scheme: an unchanged page
  (hash still valid, unmodified since the previous checkpoint) is recognized by hash and
  **shared** with the previous checkpoint's object set rather than rewritten, making
  checkpoint cost proportional to changed pages. The same mechanism SHALL serve replication
  seeding and remote checkpoint sync (SPEC-014): objects are fetched/verified by content
  hash and deduplicated against those already present locally. Page content hashing and
  checkpoint object addressing SHALL be one shared implementation, not two parallel schemes.
  *(adopted from SpacetimeDB analysis, files 02/03)*

## 9. Capacity

- **TIER-070** [P0] **Bounded by disk, not RAM.** A single shard SHALL correctly serve
  datasets of at least 10× the memory budget, limited only by disk capacity (FR-18). The
  reference validation profile is 1 vCPU / 512 MB with a dataset ≥ 10× RAM, fully functional
  (NFR-12) — reads correct, writes durable, subscriptions live.

- **TIER-071** [P1] **One billion rows.** Combined with sharding (SPEC-007 — each shard owns
  independent page files, pool budget share, and `CommitLog`), a deployment SHALL sustain
  ≥ 1 billion rows, validated by the 0.2.0-gate soak test (NFR-13, PRD goal G6).

## 10. Observability

- **TIER-080** [P0] **Metrics.** The tier SHALL export the following Prometheus metrics,
  registered in the SPEC-012 catalog (labels: `shard`, plus `table` where marked):

  | Metric | Type | Meaning |
  |---|---|---|
  | `fluxum_bufferpool_bytes` | gauge | bytes currently held in pool frames |
  | `fluxum_bufferpool_capacity_bytes` | gauge | configured pool capacity (TIER-003) |
  | `fluxum_bufferpool_hits_total` / `fluxum_bufferpool_misses_total` | counter | hit ratio = hits / (hits + misses) |
  | `fluxum_bufferpool_evictions_total` | counter | evictions (rate = evictions/s); label `kind`: `clean`\|`spill` |
  | `fluxum_page_reads_total` / `fluxum_page_writes_total` | counter | physical page I/O (`table` label) |
  | `fluxum_page_compression_ratio` | gauge | raw ÷ stored bytes, per `table` (TIER-043) |
  | `fluxum_coldtier_bytes` | gauge | on-disk page-file footprint per `table` |

  `fluxum_page_reads_total` is the observable witness of the TIER-014 invariant: it MUST NOT
  increase while a workload is served entirely from the pool.

- **TIER-081** [P1] **Structured events.** The system SHALL log (`tracing`, SPEC-012): budget
  derivation at boot (TIER-005), evictor-pressure warnings when occupancy stays above the
  high watermark for a sustained period, `BufferPoolExhausted` failures, and every
  `PageCorrupt` detection with its page coordinates.

## 11. Configuration

Keys in `config.yml` (env-var overrides with the `FLUXUM_` prefix); defaults are normative per
the requirements above. Checkpoint cadence and retention keys live in SPEC-002 §8.

```yaml
memory:
  budget: auto                       # TIER-001 — auto | <bytes>
  auto_fraction: 0.5                 # TIER-002 (tunable)
  auto_floor_bytes: 134217728        # 128 MiB — TIER-002
  bufferpool_fraction: 0.8           # TIER-003
  budget_tolerance_bytes: 67108864   # 64 MiB — TIER-004 (effective: max(this, 10% of budget))

storage:
  page_dir: ./data/pages             # TIER-023
  page_size: 8192                    # TIER-022 — 4096 | 8192 | 16384 (OQ-7)
  page_compression: lz4              # TIER-041 — lz4 | zstd | none
  compression_min_bytes: 1024        # TIER-040
  checkpoint_compression_level: 3    # TIER-042 — zstd level
  evictor_high_watermark: 0.95       # TIER-031
  evictor_low_watermark: 0.90        # TIER-031
```

## Acceptance criteria

1. **10× dataset on the droplet profile** (T2.8, gate G2): on the 1 vCPU / 512 MB CI profile
   (256 MiB auto budget), a dataset ≥ 10× the budget passes the full correctness suite —
   point reads, range scans, secondary-index and spatial queries, writes, and subscription
   diffs all correct while pages fault and evict continuously (TIER-070, NFR-12). The run
   SHALL include an index-dominated workload whose index pages alone exceed the budget:
   index pages demonstrably fault and evict (witnessed by `fluxum_page_reads_total` with the
   index flag) while all index-backed queries stay correct (TIER-050). *(adopted from
   SpacetimeDB analysis, file 02)*
2. **Budget never exceeded**: throughout criterion 1's run and a one-hour random-read soak,
   process RSS never exceeds `budget + max(64 MiB, 10%)` (TIER-004), and
   `fluxum_bufferpool_bytes` never exceeds `fluxum_bufferpool_capacity_bytes` (TIER-003).
   Pin-exhaustion tests receive `BufferPoolExhausted` + rollback, never OOM.
3. **Compression ratio** (T2.9): ≥ 3× on the SPEC-013 reference corpus over the canonical demo
   schema, published by the compression benchmark; LZ4 and zstd page round-trips are
   bit-identical under property testing (TIER-043, TIER-044, FR-19).
4. **Crash suite green on paged paths** (T2.7, SPEC-013): kill -9 at every checkpoint boundary
   (mid page write, after data/before manifest, after manifest/before `CURRENT` swap, after
   swap/before log truncation) loses zero acknowledged transactions beyond the SPEC-002
   async-log window; CRC bit-flip drills on page files trigger `PageCorrupt` handling and
   recover from a retained root + replay (TIER-025, TIER-061, TIER-062).
5. **Hot-path zero-disk-I/O assertion** (NFR-07, NFR-02): with the working set resident, an
   instrumented run asserts `fluxum_page_reads_total` stays constant and no I/O syscalls are
   issued on the read path (strace/ETW harness), while the committed-state point-lookup
   benchmark stays < 1 µs on a pool hit (TIER-014).
6. **Integrity and content addressing** (T2.8/T2.3): every fault-in verifies the per-page
   CRC32C before serving (a page whose stored hash is tampered is never served — always
   `PageCorrupt`; TIER-021, TIER-032, TIER-062); page content hashes round-trip through
   evict/fault cycles unchanged, and two consecutive checkpoints over a mostly unchanged
   dataset share unchanged page objects by content hash instead of rewriting them (TIER-063,
   SPEC-002 STG-021). *(adopted from SpacetimeDB analysis, files 02/03)*
