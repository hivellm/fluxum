//! Fluxum procedural macros (SPEC-001, SPEC-004, SPEC-010).
//!
//! Currently implemented: [`macro@table`], [`macro@migration`],
//! [`macro@reducer`], the lifecycle hooks ([`macro@on_init`],
//! [`macro@on_shard_start`], [`macro@on_connect`], [`macro@on_disconnect`]),
//! and [`macro@view`]. The remaining function-item macros
//! (`#[fluxum::procedure]`, `#[tick]`, `#[schedule]`) land with T3.4+ per
//! `docs/DAG.md`.

use proc_macro::TokenStream;

mod migration;
mod reducer;
mod table;

/// Declares a Rust struct as a Fluxum database table (SPEC-001 DM-001).
///
/// Every named field becomes a column, in declaration order. The macro
/// generates a `'static` `fluxum_core::schema::TableSchema`, an
/// implementation of `fluxum_core::schema::Table`, and a link-time registry
/// entry collected at startup by `Schema::assemble()` (DM-040) — no manual
/// registration.
///
/// # Table arguments
///
/// | Argument | Meaning |
/// |---|---|
/// | `public` | Visible to client subscriptions (DM-005) |
/// | `private` | Server-internal only — the default (DM-005) |
/// | `global` | Replicated read-only to all shards (DM-007) |
/// | `primary_key(a, b, ...)` | Composite primary key (DM-003) |
/// | `partition_by(col)` | Partition key for sharding; not with `global` (DM-008) |
///
/// # Field attributes
///
/// | Attribute | Meaning |
/// |---|---|
/// | `#[primary_key]` | Single-column primary key (DM-002) |
/// | `#[auto_inc]` | Server-assigned monotonic id; `u64` `#[primary_key]` only (DM-004) |
/// | `#[default(value)]` | Backfill value: the schema diff auto-adds the column to existing rows (SPEC-010 MIG-021) |
/// | `#[rename(from = "old")]` | Column was renamed from `old`: the schema diff renames it in place (SPEC-010 MIG-021) |
///
/// # Additional struct attributes (written **below** `#[fluxum::table]`)
///
/// | Attribute | Meaning |
/// |---|---|
/// | `#[unique(a, b, ...)]` | Multi-column unique constraint (DM-006) |
/// | `#[index(btree(a, ...))]` | B-tree secondary index, single or composite (DM-030/031) |
/// | `#[spatial(quadtree(x, y))]` | QuadTree spatial index over two `f32`/`f64` columns (DM-032) |
/// | `#[spatial(rtree(a, b, c, d))]` | R-tree spatial index over four `f32`/`f64` columns (DM-032) |
/// | `#[visibility(rule)]` | `owner_only(col)` \| `public_all` \| `shard_local` \| `custom(f)` (DM-060/061) |
///
/// Column types are the closed universe of SPEC-001 §3: `bool`, the sized
/// ints, `f32`/`f64`, `String`, `Vec<u8>`, `Identity`, `ConnectionId`,
/// `EntityId`, `Timestamp`, plus `Option<T>` and `Vec<T>` over those. Any
/// other type — including maps and nested table structs — is a compile
/// error (DM-012).
///
/// # Example
///
/// ```ignore
/// use fluxum::Identity;
///
/// #[fluxum::table(public, partition_by(owner))]
/// #[visibility(owner_only(owner))]
/// pub struct Task {
///     #[primary_key]
///     #[auto_inc]
///     pub id: u64,
///     pub owner: Identity,
///     pub title: String,
///     pub done: bool,
/// }
/// ```
#[proc_macro_attribute]
pub fn table(args: TokenStream, input: TokenStream) -> TokenStream {
    table::expand(args.into(), input.into()).into()
}

/// Declares a schema migration step (SPEC-010 MIG-010).
///
/// The function runs at startup — after recovery, before the shard serves
/// traffic — when the stored `schema_version` is lower than `version`,
/// inside one transaction (MIG-040): returning `Err` (or panicking) rolls
/// the whole step back and the server refuses to start. Steps execute in
/// ascending `version` order and each stamps its own version into
/// `__schema_meta__`, so an interrupted sequence resumes at the correct
/// point (MIG-012).
///
/// Registered through the same link-time registry as tables (DM-040); the
/// module's target version is declared with `fluxum::schema_version!(N)`
/// (default 1, MIG-001).
///
/// # Example
///
/// ```ignore
/// fluxum::schema_version!(2);
///
/// #[fluxum::migration(version = 2)]
/// fn migrate_v2(ctx: &mut MigrationContext) -> fluxum::Result<()> {
///     // v1 -> v2: Task gains a "priority" column — backfill existing rows.
///     ctx.add_column("Task", "priority", RowValue::U8(0))
/// }
/// ```
#[proc_macro_attribute]
pub fn migration(args: TokenStream, input: TokenStream) -> TokenStream {
    migration::expand(args.into(), input.into()).into()
}

/// Declares a reducer — the primary mutation API (SPEC-004 RED-001).
///
/// The function keeps its exact signature (`&ReducerContext` first, then
/// typed parameters, returning `Result<(), String>`); the macro submits it
/// to the link-time reducer registry (RED-006) together with generated
/// dispatch glue that decodes the `ReducerCall`'s `FluxValue` argument list
/// into the declared parameter types, and an argument pre-check the engine
/// runs **before** any transaction is started — an argument count or type
/// mismatch never allocates a `TxState` (RED-001).
///
/// Returning `Err(message)` rolls the transaction back fully and sends the
/// message to the caller (RED-060); a panic is caught by the engine,
/// rolls back identically, and never takes the shard down (RED-061).
///
/// `#[fluxum::reducer(max_rate = "N/s")]` declares a per-`(Identity,
/// reducer)` token-bucket rate limit (RED-050): calls beyond N per second
/// are rejected with a wire-ready 429 before any `TxState` exists.
/// Server-to-server identities are exempt (AUTH-062).
///
/// # Example
///
/// ```ignore
/// #[fluxum::reducer]
/// fn update_reading(ctx: &ReducerContext, grid_x: i32, grid_y: i32, value: f64)
///     -> Result<(), String>
/// {
///     let sensor = ctx.tx.query_pk::<Sensor>((grid_x, grid_y))
///         .map_err(|e| e.to_string())?
///         .ok_or_else(|| "unknown sensor".to_string())?;
///     ctx.tx.upsert::<Sensor>(Sensor { reading: value, ..sensor })
///         .map_err(|e| e.to_string())?;
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn reducer(args: TokenStream, input: TokenStream) -> TokenStream {
    reducer::expand_reducer(args.into(), input.into()).into()
}

/// Declares the fresh-shard initializer (SPEC-004 RED-010): runs exactly
/// once, the first time a shard starts with an empty `CommittedState` (no
/// checkpoint, no commit log) — never on recovery restarts. Signature:
/// `fn(ctx: &ReducerContext) -> Result<(), String>`.
#[proc_macro_attribute]
pub fn on_init(args: TokenStream, input: TokenStream) -> TokenStream {
    reducer::expand_lifecycle(reducer::Hook::Init, args.into(), input.into()).into()
}

/// Declares a shard-start hook (SPEC-004 RED-013): runs on every startup —
/// including recovery restarts — after `CommittedState` is recovered and
/// before the shard accepts `ReducerCall`s.
#[proc_macro_attribute]
pub fn on_shard_start(args: TokenStream, input: TokenStream) -> TokenStream {
    reducer::expand_lifecycle(reducer::Hook::ShardStart, args.into(), input.into()).into()
}

/// Declares a client-connect hook (SPEC-004 RED-011): runs in its own
/// transaction when an authenticated client connects; the client's
/// `Identity` and `ConnectionId` arrive through the context.
#[proc_macro_attribute]
pub fn on_connect(args: TokenStream, input: TokenStream) -> TokenStream {
    reducer::expand_lifecycle(reducer::Hook::Connect, args.into(), input.into()).into()
}

/// Declares a client-disconnect hook (SPEC-004 RED-012): runs in its own
/// transaction when a client's connection drops (clean close or timeout).
#[proc_macro_attribute]
pub fn on_disconnect(args: TokenStream, input: TokenStream) -> TokenStream {
    reducer::expand_lifecycle(reducer::Hook::Disconnect, args.into(), input.into()).into()
}

/// Declares a fixed-timestep periodic reducer (SPEC-004 RED-020, FR-21):
/// `#[fluxum::tick(rate = N)]` runs the function N times per second on an
/// absolute-target clock — a 1–3-period stall re-fires immediately, a
/// longer stall logs one warning and resets with no catch-up burst, and
/// the function never runs concurrently with itself on a shard. Signature:
/// `fn(ctx: &ReducerContext) -> Result<(), String>`; each firing is a full
/// reducer transaction under the server identity (RED-025). Ticks are
/// schedule-only — clients get 403 — unless `client_callable = true`.
#[proc_macro_attribute]
pub fn tick(args: TokenStream, input: TokenStream) -> TokenStream {
    reducer::expand_tick(args.into(), input.into()).into()
}

/// Declares a deferred reducer persisted in `__schedule__` (SPEC-004
/// RED-021..RED-024, FR-22): `#[fluxum::schedule(delay_ms = N)]` enqueues a
/// one-shot firing N ms after shard start; adding `every_ms = M` makes it
/// recurring with drift-free intended-time rescheduling. Rows survive crash
/// recovery (at-least-once; removal or reschedule commits atomically with
/// the execution). Schedule-only for clients (403) unless
/// `client_callable = true`; dynamic one-shots go through
/// `ctx.schedule_after(delay, reducer, args)`.
#[proc_macro_attribute]
pub fn schedule(args: TokenStream, input: TokenStream) -> TokenStream {
    reducer::expand_schedule(args.into(), input.into()).into()
}

/// Declares a read-only view for the HTTP admin API (`GET /view/:name`,
/// SPEC-004 RED-030). The function receives a `&ViewContext` — whose
/// `ReadOnlyTxHandle` has **no write methods**, so a view that tries to
/// write does not compile (RED-031) — plus typed parameters, and returns
/// any `serde::Serialize` value, delivered to the caller as JSON.
#[proc_macro_attribute]
pub fn view(args: TokenStream, input: TokenStream) -> TokenStream {
    reducer::expand_view(args.into(), input.into()).into()
}
