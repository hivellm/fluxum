//! Fluxum procedural macros (SPEC-001).
//!
//! Currently implemented: [`macro@table`]. The function-item macros
//! (`#[fluxum::reducer]`, `#[view]`, `#[procedure]`, `#[tick]`,
//! `#[schedule]`, lifecycle hooks, `#[migration]`) land in phase 3+ per
//! `docs/DAG.md`.

use proc_macro::TokenStream;

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
