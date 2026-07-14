# SPEC-008 — Geospatial Indexes

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 2 · T2.5–T2.6 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-35, FR-60, FR-61, FR-62 |
| **Requirement prefix** | `SPX-` |
| **Source** | UzDB spec 09, ported TML → Rust and generalized (game → general-purpose) |

Requirement IDs `SPX-xxx`. RFC 2119 keywords are normative. Priority tags: `[P0]` MVP blocker ·
`[P1]` required for launch · `[P2]` post-launch.

## 1. Overview

SpacetimeDB has no native geospatial indexing: a location-based live query runs as an O(n)
filter scan — every row in the table is checked for every update, for every subscribed client.
Past roughly 10,000 rows this becomes the dominant bottleneck for location-aware workloads.

Fluxum provides **QuadTree** and **R-tree** indexes as first-class storage primitives in
`fluxum-core` (`fluxum_core::index`). They give O(log n + k) spatial queries, where n is the
number of indexed rows and k is the number of results — both for one-off queries and for
subscription evaluation (SPEC-005).

**Scope.** Spatial indexes apply to **persistent geospatial** tables — rows that represent
durable geographic state with a bounded update cadence:

- Sensor installations and IoT device placements
- Vehicle / asset current locations (fleet tracking, updated at bounded rates)
- Zones, geofences, and service/coverage areas
- Points of interest (POIs) and static infrastructure

Unbounded high-frequency position **streams** (per-frame or sub-second telemetry firehoses) are
an application concern: downsample or aggregate them before persisting. See §6.

## 2. QuadTree index

- **SPX-001** [P0] — **QuadTree declaration.** A table annotated with
  `#[spatial(quadtree(x, y))]` SHALL maintain a QuadTree index over the two named coordinate
  columns. The attribute names the columns explicitly (first argument = X column, second = Y
  column), so custom column names are supported directly, e.g. `#[spatial(quadtree(cx, cy))]`.
  Both coordinate columns MUST be of type `f32` or `f64`; any other type SHALL be rejected at
  schema validation.

  ```rust
  #[fluxum::table(public, primary_key(grid_x, grid_y))]
  #[spatial(quadtree(x, y))]
  pub struct Sensor {
      pub grid_x: i32,          // composite PK component
      pub grid_y: i32,          // composite PK component
      pub x: f32,               // indexed coordinate X
      pub y: f32,               // indexed coordinate Y
      pub reading: f64,
      pub updated_at: Timestamp,
  }
  ```

- **SPX-002** [P0] — **QuadTree implementation.** The QuadTree SHALL be implemented in
  `fluxum_core::index::QuadTree`, backed by a `BTreeMap` as the underlying sorted structure:
  nodes are stored flat in the map, keyed by their quadrant path from the root, so that the
  children of a node are contiguous and a subtree visit is a `BTreeMap` range scan. There is
  **no pointer-chased node graph** — no `Box`/`Rc` child links, no unsafe code, and
  cache-friendly traversal.

  ```rust
  // crates/fluxum-core/src/index/quadtree.rs (illustrative)
  pub struct QuadTree {
      bounds: Rect,                    // root bounds (SPX-004)
      bucket_size: usize,              // max entries per leaf, default 8 (SPX-003)
      nodes: BTreeMap<NodeKey, Node>,  // flat sorted node storage — no pointer chasing
      len: usize,
  }

  pub struct Rect { pub x: f64, pub y: f64, pub w: f64, pub h: f64 }

  /// Quadrant path from the root packed into a sortable key (2 bits per level).
  /// Children of `path` sort as `path * 4 + quadrant` at `depth + 1`, so a subtree
  /// is one contiguous key range.
  struct NodeKey { depth: u8, path: u64 }

  enum Node {
      Leaf(Vec<Entry>),   // at most bucket_size entries before splitting
      Internal,
  }

  struct Entry { x: f64, y: f64, pk: PkValue }  // coordinates widened to f64 internally

  impl QuadTree {
      pub fn insert(&mut self, x: f64, y: f64, pk: PkValue);                  // O(log n)
      pub fn remove(&mut self, x: f64, y: f64, pk: &PkValue) -> bool;         // O(log n)
      pub fn query_point(&self, x: f64, y: f64) -> Vec<PkValue>;              // O(log n)
      pub fn query_region(&self, region: Rect) -> Vec<PkValue>;               // O(log n + k)
      pub fn query_radius(&self, x: f64, y: f64, r: f64) -> Vec<PkValue>;     // O(log n + k)
  }
  ```

  The QuadTree SHALL support, with n = total indexed rows and k = rows matching the spatial
  predicate:

  | Operation | Complexity |
  |---|---|
  | Insert | O(log n) |
  | Delete | O(log n) |
  | Point query | O(log n) |
  | Region query (bounding box) | O(log n + k) |
  | Radius query | O(log n + k) |

- **SPX-003** [P0] — **QuadTree bucket capacity.** The QuadTree bucket size (maximum entries
  per leaf before the leaf splits into four quadrants) SHALL be configurable, defaulting
  to **8**. This balances tree height against linear scan within a bucket.

- **SPX-004** [P0] — **QuadTree bounds.** When the table is partitioned with the
  geospatial-region strategy (SPEC-007), each shard's QuadTree SHALL be initialised with that
  shard's assigned region bounds (the `ShardHost` region), not the global coordinate bounds —
  this keeps each shard's tree sized appropriately for the data it owns. For tables not
  partitioned by geospatial region (single shard, hash- or range-partitioned), the root bounds
  SHALL come from the table's configured coordinate range (`config.yml`), and rows outside the
  configured bounds SHALL still be indexed correctly (root expansion or overflow bucket —
  implementation-defined, correctness required).

## 3. R-tree index

- **SPX-010** [P1] — **R-tree declaration.** A table annotated with
  `#[spatial(rtree(min_x, min_y, max_x, max_y))]` SHALL maintain an R-tree index in
  `fluxum_core::index::RTree` over the axis-aligned bounding box formed by the four named
  columns. R-trees index **extents**, not just points, and are preferred for **range query**
  workloads (zone/geofence overlap, rectangular region loading) and for non-uniform data
  distributions where QuadTree insertion imbalance would degrade performance. All four columns
  MUST be of type `f32` or `f64`, and `min_x <= max_x`, `min_y <= max_y` MUST hold per row
  (violations SHALL fail the insert with a constraint error). Point rows MAY be stored as
  degenerate boxes (`min == max`).

  ```rust
  #[fluxum::table(public)]
  #[spatial(rtree(min_x, min_y, max_x, max_y))]
  pub struct Zone {
      #[primary_key]
      #[auto_inc]
      pub id: u64,
      pub name: String,
      pub min_x: f32,
      pub min_y: f32,
      pub max_x: f32,
      pub max_y: f32,
      pub restricted: bool,
  }
  ```

- **SPX-011** [P1] — **Index selection guidance.** The following heuristic SHALL be documented
  (not enforced):

  | Workload | Recommended index |
  |---|---|
  | Uniform point data, frequent insert/delete (sensors, vehicles) | QuadTree |
  | Non-uniform / skewed point data, bounding-box range queries | R-tree |
  | Extended geometry with area (zones, geofences, coverage areas) | R-tree |
  | Static rows loaded once (POIs, infrastructure) | Either |

## 4. Geospatial SQL extensions

- **SPX-020** [P0] — **`IN REGION` predicate.** Subscription queries (SPEC-005) and
  `OneOffQuery` messages (SPEC-006) SHALL support the `IN REGION` predicate for tables with a
  spatial index:

  ```sql
  SELECT * FROM Sensor IN REGION (0.0, 0.0, 4000.0, 4000.0)
  ```

  Syntax: `IN REGION (x, y, w, h)` where `(x, y)` is the bottom-left corner and `w`, `h` are
  the width and height of the axis-aligned bounding box (the box covers
  `[x, x + w] × [y, y + h]`, bounds inclusive). `w` and `h` MUST be non-negative; a negative
  width or height SHALL be rejected at query compile time with a code 400 error.

  Semantics per index kind:
  - QuadTree table: returns rows whose `(x, y)` point falls within the box.
  - R-tree table: returns rows whose stored bounding box **intersects** the query box.

  The query SHALL be resolved via the spatial index. Full table scans SHALL NOT be performed.

- **SPX-021** [P0] — **`WITHIN RADIUS` predicate.**

  ```sql
  SELECT * FROM Vehicle WITHIN RADIUS 500.0 OF (1200.0, 800.0)
  SELECT * FROM Sensor  WITHIN RADIUS 200.0 OF (0.0, 0.0)
  ```

  `WITHIN RADIUS r OF (x, y)` returns rows where
  `sqrt((row.x - x)^2 + (row.y - y)^2) <= r` (Euclidean distance; implementations SHOULD
  compare squared distances to avoid the square root). `r` MUST be non-negative.

  Execution SHALL use a bounding-box approximation first — the box `(x - r, y - r, 2r, 2r)`
  against the index, O(log n + k′) — then apply the exact circle filter to the candidates
  (O(k′), where k′ ≥ k is the candidate count). Rows at distance exactly `r` are included.
  For R-tree tables, a row matches when the minimum distance from its bounding box to
  `(x, y)` is ≤ r.

  There is no implicit query origin (no `OF SELF`): a client whose center of interest moves
  SHALL re-issue the subscription with updated coordinates (see SPEC-005 subscription
  replacement semantics).

- **SPX-022** [P0] — **Spatial predicate on a non-spatial table.** Applying `IN REGION` or
  `WITHIN RADIUS` to a table without a `#[spatial(...)]` annotation SHALL be rejected at query
  compile time with:

  ```text
  Error { code: 400, message: "table 'X' has no spatial index" }
  ```

- **SPX-023** [P0] — **Complexity guarantee.** The runtime SHALL guarantee that spatial
  predicates are resolved through the spatial index. A full-table-scan fallback for spatial
  predicates SHALL NOT be implemented. If the index is unavailable (being rebuilt after
  recovery, SPX-031), the query SHALL return:

  ```text
  Error { code: 503, message: "spatial index not ready" }
  ```

## 5. Spatial index maintenance

- **SPX-030** [P0] — **Index sync on insert/delete.** Every insert of a row into a table with
  a spatial index SHALL also insert the coordinate values (point for QuadTree, bounding box for
  R-tree) together with the row's primary key into the spatial index. Every delete SHALL remove
  the corresponding entry. This happens as part of the `CommittedState` merge step of the
  commit pipeline (SPEC-002); uncommitted `TxState` changes are not visible to the index, and a
  rolled-back transaction leaves the index untouched.

- **SPX-031** [P0] — **Index rebuild on recovery.** Spatial indexes are not persisted. After
  crash recovery, each spatial index SHALL be rebuilt from the recovered `CommittedState` rows
  (SPEC-002) before the shard is marked READY. Rebuilding SHALL complete before the shard
  accepts new `ReducerCall` messages; spatial queries arriving during the rebuild receive the
  503 error of SPX-023.

- **SPX-032** [P0] — **Index update on row update.** When a row's indexed coordinate columns
  change in an update operation (upsert) — `x`/`y` for QuadTree, any of
  `min_x`/`min_y`/`max_x`/`max_y` for R-tree — the old entry SHALL be removed from the index
  and the new coordinates SHALL be inserted, atomically with respect to the commit. Stale
  coordinate entries SHALL NOT remain.

  ```rust
  #[fluxum::reducer]
  fn update_location(ctx: &ReducerContext, vehicle_id: u64, x: f64, y: f64) -> Result<(), String> {
      let mut v = ctx.tx.query_pk::<Vehicle>(&vehicle_id)?.ok_or("unknown vehicle")?;
      v.x = x;
      v.y = y;
      v.updated_at = ctx.timestamp;
      ctx.tx.upsert::<Vehicle>(v)?; // on commit: old (x, y) removed, new (x, y) inserted
      Ok(())
  }
  ```

## 6. Scope boundaries (what spatial indexes are not for)

- **SPX-040** [P0] — **Persistent geo data, not high-frequency streams.** Spatial indexes serve
  **persistent geospatial state**: rows with identity that are updated at a bounded cadence
  (a vehicle's current location, a sensor's installation point, a zone boundary). They SHALL
  NOT be positioned as a transport for unbounded high-frequency position streams: every
  spatially-indexed write pays O(log n) index maintenance inside the commit path plus commit-log
  durability, so a per-frame or sub-second firehose of appended position events belongs in the
  application layer (downsample/aggregate before persisting, or keep the raw stream in a
  telemetry pipeline outside the database).

  The following anti-pattern SHALL be documented:

  ```rust
  // ANTI-PATTERN: do not append a raw position event stream through a spatial index.
  #[fluxum::table(public)]
  #[spatial(quadtree(x, y))] // WRONG: unbounded ephemeral stream, not persistent geo state
  pub struct GpsTickLog {
      #[primary_key]
      #[auto_inc]
      pub id: u64,
      pub device: Identity,
      pub x: f32,
      pub y: f32,
  }
  ```

  A non-fatal schema-validation **advisory** SHALL be emitted when a `#[spatial]` table's name
  matches common event-stream patterns (`*Log`, `*Stream`, `*Tick`, `*Trace`, `*History`),
  stating that spatial indexes are for persistent geospatial state and that high-frequency
  streams should be downsampled first. Unlike the UzDB source, this is an advisory rather than
  a rejection: bounded-rate location tracking (e.g. fleet vehicles updated via
  `update_location`) is a fully supported Fluxum workload.

  The correct pattern is one row per entity (`Vehicle`, `Sensor`), updated in place at a
  bounded rate (SPX-032), with the spatial index answering `IN REGION` / `WITHIN RADIUS`
  queries over the current state.

## Acceptance criteria

1. **Correctness property tests** (proptest): for randomized insert/delete/update workloads,
   `query_point`, `query_region`, and `query_radius` on both `QuadTree` and `RTree` return
   exactly the same row sets as a brute-force O(n) reference implementation, including boundary
   rows (points on box edges; rows at distance exactly `r`).
2. **1M-point benchmark** (criterion): with 1,000,000 indexed rows, an `IN REGION` query
   selecting on the order of 1,000 rows executes **at least 10× faster** than an O(n) full scan
   evaluating the same predicate, and measured latency scales consistently with O(log n + k)
   as n grows.
3. **Radius semantics**: `WITHIN RADIUS` results equal the exact Euclidean-distance filter;
   the bounding-box prefilter produces candidates k′ ≥ k and every false positive is removed
   by the exact circle filter.
4. **Compile-time rejection**: a spatial predicate on a table without `#[spatial]` returns
   `Error { code: 400, message: "table 'X' has no spatial index" }`; negative `w`/`h`/`r`
   values are rejected with code 400; a `#[spatial]` attribute naming a non-float column fails
   schema validation.
5. **Rebuild gate**: in a crash-recovery test (SPEC-002 suite), the shard does not accept
   `ReducerCall` messages until the spatial index rebuild completes; spatial queries during the
   rebuild return `Error { code: 503, message: "spatial index not ready" }`; after the rebuild,
   spatial query results are identical to the pre-crash committed state.
6. **Update coherence**: after an upsert that moves a row's coordinates, a region query on the
   old location no longer returns the row and a region query on the new location does — no
   stale index entries (SPX-032).
7. **Configuration**: QuadTree bucket size is configurable and defaults to 8 (SPX-003); a
   non-default bucket size produces identical query results.
8. **Advisory lint**: `#[spatial]` on a table named `GpsTickLog` emits the non-fatal
   event-stream advisory; `Vehicle` and `Sensor` do not (SPX-040).
