//! Fluxum procedural macros (SPEC-001, SPEC-010).
//!
//! Currently implemented: [`macro@table`] and [`macro@migration`]. The
//! remaining function-item macros (`#[fluxum::reducer]`, `#[view]`,
//! `#[procedure]`, `#[tick]`, `#[schedule]`, lifecycle hooks) land in
//! phase 3+ per `docs/DAG.md`.

use proc_macro::TokenStream;

mod migration;
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
