# SPEC-010 — Schema Migration

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 3 · T3.6 ([DAG](../DAG.md)) |
| **PRD requirements** | FR-27, FR-80 |
| **Requirement prefix** | `MIG-` |
| **Source** | UzDB spec 11, ported TML → Rust and generalized (game → general-purpose) |

Requirement IDs `MIG-xxx`. RFC 2119 keywords normative. Priority tags: `[P0]` MVP blocker ·
`[P1]` required for launch · `[P2]` post-launch. The migration runner lives in `fluxum-core`;
the `#[fluxum::migration]` attribute lives in `fluxum-macros`.

## 1. Overview

SpacetimeDB's schema migration story is a known pain point: adding columns with defaults is
supported, but removing columns, renaming columns, and type changes all require manual migration
reducers with no automated tooling.

Fluxum improves on this with:

- A `#[fluxum::migration(version = N)]` attribute for migration functions.
- Automatic schema diffing to detect what changed between versions.
- Migrations applied at startup, before the shard is marked READY and accepts connections.

**Rust-specific angle.** The compiled-in schema is not published to a running server — it comes
from the link-time schema registry (SPEC-001): tables, reducers, and migration functions are
collected at link time and compiled into the server binary via `fluxum::ServerBuilder`. A schema
change therefore always arrives as a new binary, and the schema diff runs **at boot** — after
recovery of `CommittedState` (SPEC-002) and before the shard serves any traffic. There is no
window in which clients can observe a half-migrated schema.

## 2. Schema versioning

- **MIG-001** [P0] **Schema version declaration.** An application module SHALL declare its schema
  version as a compile-time constant registered into the link-time registry:

  ```rust
  fluxum::schema_version!(3); // registers SCHEMA_VERSION: u32 = 3
  ```

  If the module does not declare a version, `SCHEMA_VERSION` defaults to `1`. The runtime SHALL
  read this constant at startup and compare it against the `schema_version` stored in the
  `__schema_meta__` system table.

- **MIG-002** [P0] **Schema metadata table.** The runtime SHALL maintain a `__schema_meta__`
  system table (stored name unchanged; defined by the runtime, not by application code):

  ```rust
  #[fluxum::table(private, global)] // stored table name: "__schema_meta__"
  struct SchemaMeta {
      #[primary_key]
      key: String,   // e.g. "schema_version", "last_migration", "schema_catalog"
      value: Vec<u8>, // MessagePack-encoded value (rmp-serde)
  }
  ```

  On first startup (empty `CommittedState`), the runtime writes
  `schema_version = SCHEMA_VERSION`. On subsequent startups, the runtime reads `schema_version`
  and runs pending migrations if the current code's `SCHEMA_VERSION` is higher.

- **MIG-003** [P0] **Downgrade rejection.** If the code's `SCHEMA_VERSION` is **lower** than the
  stored `schema_version`, the server SHALL refuse to start with:

  ```
  FATAL: schema downgrade detected (code=2 < stored=3). Aborting.
  ```

## 3. Migration functions

- **MIG-010** [P0] **`#[fluxum::migration]` attribute.** A function annotated with
  `#[fluxum::migration(version = N)]` SHALL be executed when the server starts and the stored
  schema version is less than `N`. Migration functions are collected through the link-time
  registry (SPEC-001), like reducers. Two migration functions declaring the same `version` SHALL
  be rejected at startup with a descriptive error.

  ```rust
  fluxum::schema_version!(3);

  #[fluxum::migration(version = 2)]
  fn migrate_v2(ctx: &mut MigrationContext) -> fluxum::Result<()> {
      // v1 -> v2: Task gains a "priority" column (no #[default]) — backfill existing rows.
      ctx.add_column("task", "priority", FluxValue::U8(0))?;
      Ok(())
  }

  #[fluxum::migration(version = 3)]
  fn migrate_v3(ctx: &mut MigrationContext) -> fluxum::Result<()> {
      // v2 -> v3: Sensor renames "reading" to "value" — rewritten in place.
      ctx.rename_column("sensor", "reading", "value")?;
      Ok(())
  }
  ```

- **MIG-011** [P0] **MigrationContext.** Migration functions SHALL receive a `MigrationContext`
  instead of a `ReducerContext`:

  ```rust
  pub struct MigrationContext {
      pub from_version: u32, // schema_version stored at startup
      pub to_version: u32,   // version this migration step targets
      pub tx: TxHandle,      // full read/write access to CommittedState
  }

  impl MigrationContext {
      // Table-level DDL. create_table takes the compiled-in type; drop_table takes the
      // stored name, since the removed table's type no longer exists in code.
      pub fn create_table<T: fluxum::Table>(&mut self) -> fluxum::Result<()>;
      pub fn drop_table(&mut self, table: &str) -> fluxum::Result<()>;

      // Column-level DDL. Table/column names are the stored names (SPEC-001 naming).
      pub fn add_column(&mut self, table: &str, column: &str, default: FluxValue)
          -> fluxum::Result<()>;
      pub fn rename_column(&mut self, table: &str, from: &str, to: &str)
          -> fluxum::Result<()>;
      pub fn drop_column(&mut self, table: &str, column: &str) -> fluxum::Result<()>;
  }
  ```

  Migration functions have full read/write access via `ctx.tx` (`scan_all`, `upsert`, `insert`,
  `delete`, …) and may read old table layouts alongside new ones (see MIG-013). Row-level
  visibility rules (`#[visibility]`) and reducer rate limits do not apply during migration.

- **MIG-012** [P0] **Execution order.** Migrations SHALL be executed in ascending `version`
  order. If the stored version is 1 and the code is at version 3, the migrations for version 2
  and then version 3 SHALL be executed in sequence before the shard is marked READY. Each
  migration step SHALL update `__schema_meta__.schema_version` to its own `version` **within the
  same transaction** as the step itself (see MIG-040), so a crash between steps resumes at the
  correct point on restart.

- **MIG-013** [P1] **Old-layout access during migration.** To support data transformations
  (rename column, change type), migrations SHALL be able to declare versioned alias types that
  map to a stored table's **old** column layout. Alias types are readable during migration only
  and are not part of the compiled schema catalog:

  ```rust
  // Old layout — Sensor.reading was f32 before v4:
  #[fluxum::migration_table(name = "sensor", primary_key(grid_x, grid_y))]
  struct SensorOld {
      grid_x: i32,
      grid_y: i32,
      x: f32,
      y: f32,
      reading: f32, // widened to f64 in v4
      updated_at: Timestamp,
  }

  #[fluxum::migration(version = 4)]
  fn migrate_v4(ctx: &mut MigrationContext) -> fluxum::Result<()> {
      // Type change f32 -> f64: transform every row.
      for old in ctx.tx.scan_all::<SensorOld>()? {
          ctx.tx.upsert::<Sensor>(Sensor {
              grid_x: old.grid_x,
              grid_y: old.grid_y,
              x: old.x,
              y: old.y,
              reading: old.reading as f64,
              updated_at: old.updated_at,
          })?;
      }
      Ok(())
  }
  ```

  The runtime SHALL preserve `CommittedState` rows under the old column layout until the
  migration function transforms them. After the migration commits, all rows in the table SHALL
  conform to the new schema.

## 4. Automatic schema diff

- **MIG-020** [P1] **Schema diff at boot.** At every startup, after recovery and before serving
  traffic, the runtime SHALL compute a diff between the stored schema catalog (persisted in
  `__schema_meta__` under key `schema_catalog`, MessagePack-encoded) and the compiled schema from
  the link-time registry (SPEC-001). After a successful startup (including any migrations), the
  runtime SHALL persist the current compiled catalog back to `schema_catalog`. The diff SHALL
  detect:

  | Change type | Detection | Required action |
  |---|---|---|
  | New table | Table in compiled schema, not in stored | Create table (safe; no data migration needed) |
  | Removed table | Table in stored schema, not in compiled | Drop all rows — requires an explicit `#[fluxum::migration]` calling `ctx.drop_table` |
  | New column with default | Column added with `#[default(value)]` field attribute | Auto-fill missing column for existing rows |
  | New column without default | Column added without `#[default]` | Requires a `#[fluxum::migration]` (e.g. `ctx.add_column`) |
  | Removed column | Column in stored layout, not in compiled | Requires a `#[fluxum::migration]` calling `ctx.drop_column` |
  | Renamed column | Explicit `#[rename(from = "old")]` field attribute | Runtime renames in place (equivalently: `ctx.rename_column` in a migration) |
  | Column type change | Column type differs | Requires a `#[fluxum::migration]` (row transform via MIG-013 alias types) |

- **MIG-021** [P1] **Safe auto-apply.** The runtime SHALL auto-apply schema changes that are safe
  and non-destructive, without requiring a migration function:
  - Adding a table.
  - Adding a column with `#[default(value)]`.
  - Renaming a column carrying `#[rename(from = "old")]`.

  Auto-applied changes SHALL execute in a single startup transaction, be logged at `info` level,
  and still require the `SCHEMA_VERSION` bump of MIG-023.

- **MIG-022** [P0] **Abort on incompatible change.** The runtime SHALL require an explicit
  `#[fluxum::migration]` function for destructive or ambiguous changes: column type changes,
  column removals, table removals, additions without a default, and renames without a
  `#[rename]` annotation. If such a change is detected in the diff and no migration function
  covers the corresponding version step, the server SHALL refuse to start with a descriptive
  error listing every offending diff entry (table, column, change type, required action). The
  stored data SHALL remain untouched.

- **MIG-023** [P1] **Version-bump enforcement.** If the schema diff is non-empty but the code's
  `SCHEMA_VERSION` equals the stored `schema_version`, the server SHALL refuse to start with an
  error instructing the developer to bump `fluxum::schema_version!`. This prevents accidental
  deployment of schema changes that would bypass the migration path.

## 5. Versioned reducers

- **MIG-030** [P1] **Reducer versioning.** A reducer MAY specify a version:
  `#[fluxum::reducer(version = 2)]` (default `version = 1` when omitted). The runtime SHALL store
  multiple versions of the same reducer name. Because Rust forbids duplicate `fn` names in one
  module, the wire name is given via the `name` attribute argument and defaults to the `fn` name.
  Clients on older SDKs MAY continue calling `version: 1` while new clients use `version: 2`:

  ```rust
  // Old signature (v1) — kept for backwards compatibility; delegates to v2.
  #[fluxum::reducer(name = "update_reading", version = 1)]
  fn update_reading_v1(
      ctx: &ReducerContext,
      grid_x: i32,
      grid_y: i32,
      reading: f64,
  ) -> Result<(), String> {
      update_reading_v2(ctx, grid_x, grid_y, reading, ctx.timestamp)
  }

  // New signature (v2) — clients report the capture timestamp themselves.
  #[fluxum::reducer(name = "update_reading", version = 2)]
  fn update_reading_v2(
      ctx: &ReducerContext,
      grid_x: i32,
      grid_y: i32,
      reading: f64,
      captured_at: Timestamp,
  ) -> Result<(), String> {
      ctx.tx.upsert::<Sensor>(Sensor {
          grid_x,
          grid_y,
          x: grid_x as f32,
          y: grid_y as f32,
          reading,
          updated_at: captured_at,
      })?;
      Ok(())
  }
  ```

- **MIG-031** [P0] **Default reducer version.** When a `ReducerCall` (SPEC-006) does not specify
  a version, the runtime SHALL invoke the **highest registered version** of that reducer.

- **MIG-032** [P2] **Deprecated reducer removal.** A reducer version MAY be annotated
  `#[fluxum::reducer(name = "...", version = 1, deprecated_since = 3)]`. The runtime SHALL:
  1. Continue serving calls to the deprecated version.
  2. Log a warning for each call to a deprecated reducer (and count it in the
     `fluxum_reducer_deprecated_calls_total` metric, SPEC-012).
  3. Optionally reject calls after a configurable grace period (`config.yml` key
     `reducers.deprecation_grace`, `FLUXUM_`-prefixed env override).

## 6. Migration atomicity

- **MIG-040** [P0] **Each migration is one transaction.** Each `#[fluxum::migration]` function
  SHALL execute inside a single database transaction (SPEC-003). If the migration function
  returns `Err` or panics (caught via `catch_unwind`, per the reducer panic-isolation policy),
  the transaction SHALL be rolled back and the server SHALL refuse to start, logging the error.
  This ensures the database is never left in a partially migrated state.

- **MIG-041** [P1] **Idempotent migrations (retry safe).** Migration functions SHOULD be written
  to be idempotent: if a migration is interrupted (server crash) and re-run, it SHALL produce the
  same result as if it had run once. The runtime does not enforce this — it is the developer's
  responsibility. Guidance SHALL be provided in the module-authoring documentation.

## Acceptance criteria

1. **Add-column migration passes.** A store created at `SCHEMA_VERSION = 1` booted with a v2
   binary whose migration calls `ctx.add_column("task", "priority", FluxValue::U8(0))`: startup
   completes, every pre-existing `task` row carries `priority = 0`, and
   `__schema_meta__.schema_version` reads 2.
2. **Rename-column migration passes.** A v3 binary renaming `sensor.reading` to `sensor.value`
   (via `ctx.rename_column` or `#[rename(from = "reading")]`): all row data is preserved under
   the new column name; queries against the old name fail.
3. **Incompatible change aborts startup.** A binary changing `sensor.reading` from `f32` to
   `f64` (or removing a column) with **no** covering migration function: the server refuses to
   start, the error names the table, column, and change type, and the stored data is unmodified.
4. **Downgrade aborts.** A binary with `SCHEMA_VERSION` lower than the stored version fails fast
   with the MIG-003 `FATAL` message.
5. **Ordering and resume.** Stored version 1, code version 3: migrations v2 then v3 run in
   ascending order, each in its own transaction, before the shard is marked READY. Killing the
   process after v2 commits and restarting resumes at v3 only.
6. **Failure rollback.** A migration that returns `Err` or panics midway leaves
   `CommittedState` and `schema_version` unchanged; the server exits non-zero; restarting with a
   fixed binary re-runs the migration from the stored version.
7. **Versioned reducers.** With v1 and v2 of `update_reading` registered: a `ReducerCall`
   without a version invokes v2; `version: 1` invokes v1; a `deprecated_since` version logs a
   warning per call.
8. **Safe auto-apply.** A version-bumped binary that only adds a new table and a new column with
   `#[default]` starts without any migration function, applies both changes in one startup
   transaction, and logs them; the same binary without the version bump aborts per MIG-023.
