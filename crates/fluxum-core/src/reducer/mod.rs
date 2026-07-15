//! Reducer API (SPEC-004 §2, T3.2): [`ReducerContext`] — the first parameter
//! of every reducer — and the typed [`TxHandle`] read/write surface bound to
//! the call's transaction, plus the [`ReducerRegistry`] that dispatches
//! reducers by name and lets a reducer call another reducer inside the same
//! transaction (RED-005).
//!
//! # How the pieces fit (the T3.3 engine seam)
//!
//! The transaction pipeline ([`crate::txn::TxPipeline`], T3.1) executes
//! `FnOnce(&mut Tx) -> Result<()>` jobs. This module turns a raw [`Tx`] into
//! the module-author API: the engine wraps a `ReducerCall` as
//!
//! ```ignore
//! pipeline.call(Box::new(move |tx| {
//!     registry.dispatch(caller, &name, &args, tx)
//! })).await
//! ```
//!
//! and every table access inside the reducer goes through `ctx.tx`. Closures
//! (tests, lifecycle hooks) enter the same way via [`with_context`].
//!
//! # Design decisions (T3.2)
//!
//! - **`&ReducerContext`, not `&mut`** (RED-001): reducers take a shared
//!   reference — the ergonomic shape every SPEC-004 example uses — so
//!   [`TxHandle`] reaches the underlying single-writer [`Tx`] through a
//!   [`RefCell`]. Reducer execution is single-threaded by construction
//!   (TXN-010: one writer per shard) and every method scopes its borrow
//!   internally, so the cell can never be contended; `try_borrow` failures
//!   are surfaced as errors rather than panics to keep the reducer path
//!   unwind-free (RED-061).
//! - **Read isolation is explicit** (TXN-050/051, FR-17): [`TxHandle::scan`],
//!   [`TxHandle::query_pk`], and [`TxHandle::scan_where`] read the committed
//!   pre-transaction snapshot only — never this call's pending writes.
//!   Read-your-own-writes goes through [`TxHandle::scan_pending`] /
//!   [`TxHandle::count_pending`] / [`TxHandle::scan_all`] /
//!   [`TxHandle::scan_all_where`], which method a reducer uses being an
//!   explicit, reviewable statement of intent.
//! - **Nested calls share one transaction, no savepoints** (RED-005,
//!   TXN-051): [`TxHandle::call`] runs the callee against the same `TxState`.
//!   An `Err` from the callee propagates to the caller, which may handle it
//!   or let it roll back the whole transaction. There is no partial rollback:
//!   writes the callee buffered *before* failing remain in the transaction if
//!   the caller handles the error and commits — SPEC-003 has no savepoints.
//! - **Recursion is capped** ([`MAX_CALL_DEPTH`]): unbounded
//!   reducer-calls-reducer recursion would overflow the stack, and a stack
//!   overflow aborts the process instead of unwinding — the one failure
//!   RED-061's `catch_unwind` cannot isolate. The cap turns it into an
//!   ordinary rollback error.
//! - **Typed rows ride the [`Table`] conversions** (DM-043): `#[fluxum::table]`
//!   generates `into_values`/`from_values`/`pk_values`, so this module is
//!   plain plumbing — no per-table codegen, no reflection. Decoded rows are
//!   cloned out of the store's `Arc`-shared [`Row`]s; payload copies happen
//!   only for owned column types (`String`, `Vec<_>`).
//! - **Index and spatial typed accessors are deliberately absent**: their
//!   ergonomic surface (`query_index`, `spatial_radius` — RED-003) needs the
//!   query/argument model of the SDK and view tasks (T3.4+, SPEC-008/011);
//!   until then reducers reach them through the engine-level seams on
//!   [`Tx`] itself ([`Tx::index_eq`], [`Tx::spatial_radius`]).

use std::cell::{Cell, Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::fmt;

use fluxum_protocol::{FluxValue, codes};

use crate::error::{FluxumError, Result};
use crate::schema::Table;
use crate::store::{Row, TableId, Tx};
use crate::types::{ConnectionId, Identity, Timestamp};

/// Maximum reducer-calls-reducer nesting depth (RED-005 guard).
///
/// Deep enough for any sane delegation chain; shallow enough that runaway
/// recursion becomes a rollback error long before the stack — whose overflow
/// would abort the process rather than unwind (RED-061) — is at risk.
pub const MAX_CALL_DEPTH: u32 = 64;

/// Who is calling, resolved by the transport/auth layer before the
/// transaction starts (RED-002 inputs; identity derivation is SPEC-009).
///
/// Scheduled executions (SPEC-004 §4, T3.5) construct this with the server
/// identity and the reserved nil `ConnectionId(0)` per RED-025.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReducerCaller {
    /// Stable 256-bit caller identity (SPEC-009).
    pub identity: Identity,
    /// Ephemeral per-connection identifier.
    pub connection_id: ConnectionId,
    /// Call admission timestamp (µs since Unix epoch).
    pub timestamp: Timestamp,
    /// The shard this reducer runs on.
    pub shard_id: u32,
}

/// A registered reducer body: receives the context and the raw `FluxValue`
/// argument list of the `ReducerCall` (RED-001; the `#[fluxum::reducer]`
/// macro of T3.4 generates the decode glue that turns `args` into typed
/// parameters).
pub type ReducerHandler =
    Box<dyn Fn(&ReducerContext<'_, '_, '_>, &[FluxValue]) -> Result<()> + Send + Sync>;

/// Wrap a closure or fn as a [`ReducerHandler`] (helps closure inference
/// across the higher-ranked context lifetimes).
pub fn handler<F>(f: F) -> ReducerHandler
where
    F: Fn(&ReducerContext<'_, '_, '_>, &[FluxValue]) -> Result<()> + Send + Sync + 'static,
{
    Box::new(f)
}

/// Name → reducer map (RED-006): populated at startup (`ServerBuilder`,
/// link-time collection lands with T3.4's `#[fluxum::reducer]`), then read
/// by every dispatch — including nested [`TxHandle::call`]s.
#[derive(Default)]
pub struct ReducerRegistry {
    handlers: HashMap<String, ReducerHandler>,
}

impl fmt::Debug for ReducerRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut names: Vec<&str> = self.names().collect();
        names.sort_unstable();
        f.debug_struct("ReducerRegistry")
            .field("reducers", &names)
            .finish()
    }
}

impl ReducerRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a reducer under `name`. A duplicate name is a startup error
    /// (RED-006) — [`FluxumError::Schema`], which must abort boot.
    pub fn register(&mut self, name: impl Into<String>, handler: ReducerHandler) -> Result<()> {
        let name = name.into();
        if self.handlers.contains_key(&name) {
            return Err(FluxumError::Schema(format!(
                "duplicate reducer name `{name}`: reducer names must be unique (RED-006)"
            )));
        }
        self.handlers.insert(name, handler);
        Ok(())
    }

    /// Whether `name` is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.handlers.contains_key(name)
    }

    /// Registered reducer names (unordered).
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.handlers.keys().map(String::as_str)
    }

    /// Execute reducer `name` against `tx` — the root dispatch the T3.3
    /// engine submits to the pipeline. An unknown name is rejected with a
    /// wire-ready 404 before any table access (RED-006; when dispatch is
    /// used as the job body, no `TxState` effect exists yet either).
    ///
    /// `Err` from the reducer propagates to the pipeline, which rolls the
    /// transaction back (TXN-022, RED-004).
    pub fn dispatch(
        &self,
        caller: ReducerCaller,
        name: &str,
        args: &[FluxValue],
        tx: &mut Tx<'_>,
    ) -> Result<()> {
        let handler = self
            .handlers
            .get(name)
            .ok_or_else(|| unknown_reducer(name))?;
        with_context(self, caller, tx, |ctx| handler(ctx, args))
    }
}

/// Run `body` with a [`ReducerContext`] over `tx` — the closure-shaped
/// entry ([`ReducerRegistry::dispatch`] is the by-name shape). Used by
/// lifecycle hooks, tests, and anywhere the reducer body is already in hand.
pub fn with_context<'t, 's, R>(
    registry: &'t ReducerRegistry,
    caller: ReducerCaller,
    tx: &'t mut Tx<'s>,
    body: impl FnOnce(&ReducerContext<'_, 't, 's>) -> Result<R>,
) -> Result<R> {
    let env = TxEnv {
        tx: RefCell::new(tx),
        registry,
        caller,
        depth: Cell::new(0),
    };
    let ctx = env.context();
    body(&ctx)
}

/// Shared per-transaction state behind every [`TxHandle`] of one reducer
/// call tree: the single-writer [`Tx`], the registry for nested calls, the
/// caller metadata, and the RED-005 recursion depth.
struct TxEnv<'t, 's> {
    tx: RefCell<&'t mut Tx<'s>>,
    registry: &'t ReducerRegistry,
    caller: ReducerCaller,
    depth: Cell<u32>,
}

impl<'t, 's> TxEnv<'t, 's> {
    /// A context over this environment — same transaction, same caller
    /// (nested calls run under the outer caller's identity, RED-005).
    fn context<'e>(&'e self) -> ReducerContext<'e, 't, 's> {
        ReducerContext {
            identity: self.caller.identity,
            connection_id: self.caller.connection_id,
            timestamp: self.caller.timestamp,
            shard_id: self.caller.shard_id,
            tx: TxHandle { env: self },
        }
    }
}

/// What every reducer receives at call time (RED-002): the caller's
/// identity, connection, and timestamp, the shard, and the transaction-bound
/// table access handle.
///
/// The UzDB `entity_id` field does not exist in Fluxum — rows are addressed
/// exclusively through table primary keys (RED-002 note).
pub struct ReducerContext<'e, 't, 's> {
    /// 256-bit caller identity, stable across sessions (SPEC-009).
    pub identity: Identity,
    /// Ephemeral per-connection identifier.
    pub connection_id: ConnectionId,
    /// Call timestamp (µs since Unix epoch).
    pub timestamp: Timestamp,
    /// Shard this reducer runs on.
    pub shard_id: u32,
    /// Read/write handle bound to this call's transaction (RED-003).
    pub tx: TxHandle<'e, 't, 's>,
}

/// Typed read/write surface of one reducer transaction (RED-003, FR-20).
///
/// All writes are buffered in the transaction's `TxState` and become visible
/// to others only on commit (RED-004); constraint violations (PK,
/// `#[unique]`) surface immediately at the write call (TXN-040/041).
/// Reads are split by visibility — committed-only by default (TXN-050),
/// pending/combined only through the explicitly named methods (TXN-051,
/// FR-17). Cheap to copy; `ctx.tx` is the canonical way to reach it.
#[derive(Clone, Copy)]
pub struct TxHandle<'e, 't, 's> {
    env: &'e TxEnv<'t, 's>,
}

impl<'e, 't, 's> TxHandle<'e, 't, 's> {
    // --- Writes (RED-003) --------------------------------------------------

    /// Insert a row; errors on a primary-key conflict (TXN-040) or a
    /// `#[unique]` violation (TXN-041). Returns the row **as stored** — for
    /// `#[auto_inc]` tables a `0` placeholder id comes back with the
    /// assigned value (TXN-042), so callers never re-read for it.
    pub fn insert<T: Table>(&self, row: T) -> Result<T> {
        let stored = self
            .exclusive()?
            .insert(table_of::<T>(), row.into_values())?;
        T::from_values(stored.values())
    }

    /// Insert or replace by primary key (the TXN-040 exception); `#[unique]`
    /// constraints against *other* rows still apply. Returns the row as
    /// stored, auto-inc placeholder resolved exactly as in
    /// [`TxHandle::insert`].
    pub fn upsert<T: Table>(&self, row: T) -> Result<T> {
        let stored = self
            .exclusive()?
            .upsert(table_of::<T>(), row.into_values())?;
        T::from_values(stored.values())
    }

    /// Delete the row with primary key `pk`. Returns whether a (committed
    /// or pending) row was deleted; deleting a row this same transaction
    /// inserted cancels the insert entirely (STG-007).
    pub fn delete<T: Table>(&self, pk: T::Pk) -> Result<bool> {
        self.exclusive()?
            .delete(table_of::<T>(), &T::pk_values(&pk))
    }

    /// Delete every committed row matching `pred`; returns how many rows
    /// were deleted. The predicate is evaluated over the committed
    /// pre-transaction snapshot — the same view [`TxHandle::scan`] reads
    /// (TXN-050); rows inserted by this transaction are not candidates.
    pub fn delete_where<T: Table>(&self, pred: impl Fn(&T) -> bool) -> Result<u64> {
        let rows = self.committed_rows::<T>()?;
        let mut deleted = 0u64;
        for row in rows {
            let typed = T::from_values(row.values())?;
            if pred(&typed)
                && self
                    .exclusive()?
                    .delete(table_of::<T>(), &T::pk_values(&typed.primary_key()))?
            {
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    // --- Committed-state reads (TXN-050: pre-transaction snapshot only) ----

    /// Point lookup by primary key against the committed snapshot captured
    /// at transaction begin. Never sees this transaction's pending writes.
    pub fn query_pk<T: Table>(&self, pk: T::Pk) -> Result<Option<T>> {
        let row = self
            .shared()?
            .query_pk(table_of::<T>(), &T::pk_values(&pk))?;
        row.map(|r| T::from_values(r.values())).transpose()
    }

    /// Full scan of the committed snapshot, in encoded-PK byte order.
    /// Never sees this transaction's pending writes (TXN-050).
    pub fn scan<T: Table>(&self) -> Result<Vec<T>> {
        decode_rows(&self.committed_rows::<T>()?)
    }

    /// Filtered scan of the committed snapshot (TXN-050 view).
    pub fn scan_where<T: Table>(&self, pred: impl Fn(&T) -> bool) -> Result<Vec<T>> {
        let mut rows = self.scan::<T>()?;
        rows.retain(|row| pred(row));
        Ok(rows)
    }

    // --- Intra-transaction reads (TXN-051, FR-17) ---------------------------

    /// Rows written by THIS transaction — pending inserts and the new
    /// content of upsert replacements — in encoded-PK byte order.
    pub fn scan_pending<T: Table>(&self) -> Result<Vec<T>> {
        let rows: Vec<Row> = {
            let tx = self.shared()?;
            tx.scan_pending(table_of::<T>())?.cloned().collect()
        };
        decode_rows(&rows)
    }

    /// How many of this transaction's pending rows match `pred`.
    pub fn count_pending<T: Table>(&self, pred: impl Fn(&T) -> bool) -> Result<u64> {
        let matching = self
            .scan_pending::<T>()?
            .into_iter()
            .filter(|row| pred(row))
            .count();
        Ok(matching as u64)
    }

    /// Combined view: committed rows plus this transaction's pending
    /// writes, deduplicated by primary key — a pending insert or upsert
    /// wins over the committed row with the same key, a pending delete
    /// removes it (TXN-051). Order: committed keys in encoded-PK order
    /// (replacements in place), then this transaction's new inserts in
    /// encoded-PK order.
    pub fn scan_all<T: Table>(&self) -> Result<Vec<T>> {
        let rows: Vec<Row> = {
            let tx = self.shared()?;
            tx.scan_all(table_of::<T>())?.cloned().collect()
        };
        decode_rows(&rows)
    }

    /// Filtered combined view (see [`TxHandle::scan_all`]).
    pub fn scan_all_where<T: Table>(&self, pred: impl Fn(&T) -> bool) -> Result<Vec<T>> {
        let mut rows = self.scan_all::<T>()?;
        rows.retain(|row| pred(row));
        Ok(rows)
    }

    // --- Reducer delegation (RED-005) ---------------------------------------

    /// Call another registered reducer **within the same transaction**: the
    /// callee shares this call's `TxState` and runs under the same caller
    /// identity; no new transaction is started. The callee's `Err`
    /// propagates — handle it to keep the transaction alive, or let it
    /// bubble up for a full rollback of both write sets. Note there are no
    /// savepoints: writes the callee made before failing stay in the
    /// transaction if the error is handled and the transaction commits.
    pub fn call(&self, reducer: &str, args: &[FluxValue]) -> Result<()> {
        let handler = self
            .env
            .registry
            .handlers
            .get(reducer)
            .ok_or_else(|| unknown_reducer(reducer))?;
        let depth = self.env.depth.get();
        if depth >= MAX_CALL_DEPTH {
            return Err(FluxumError::query(
                codes::INTERNAL,
                format!(
                    "reducer call depth exceeded {MAX_CALL_DEPTH} calling `{reducer}`: \
                     unbounded recursion via ctx.tx.call (RED-005)"
                ),
            ));
        }
        self.env.depth.set(depth + 1);
        let ctx = self.env.context();
        let result = handler(&ctx, args);
        self.env.depth.set(depth);
        result
    }

    // --- Internals -----------------------------------------------------------

    /// Committed rows of `T`'s table, cloned out (`Arc` bumps) so the
    /// transaction borrow is released before predicates or decoding run.
    fn committed_rows<T: Table>(&self) -> Result<Vec<Row>> {
        let tx = self.shared()?;
        Ok(tx.scan(table_of::<T>())?.cloned().collect())
    }

    /// Shared access to the transaction. Cannot be contended in correct
    /// usage (every method scopes its borrow); surfaced as an error, never
    /// a panic (RED-061).
    fn shared(&self) -> Result<Ref<'e, &'t mut Tx<'s>>> {
        self.env.tx.try_borrow().map_err(|_| reentrant_handle())
    }

    /// Exclusive access to the transaction (write path). Same contention
    /// story as [`TxHandle::shared`].
    fn exclusive(&self) -> Result<RefMut<'e, &'t mut Tx<'s>>> {
        self.env.tx.try_borrow_mut().map_err(|_| reentrant_handle())
    }
}

/// The stable table id of `T` (STG-050).
fn table_of<T: Table>() -> TableId {
    TableId::of(T::SCHEMA.name)
}

/// Decode `Arc`-shared stored rows into typed rows.
fn decode_rows<T: Table>(rows: &[Row]) -> Result<Vec<T>> {
    rows.iter()
        .map(|row| T::from_values(row.values()))
        .collect()
}

fn unknown_reducer(name: &str) -> FluxumError {
    FluxumError::query(
        codes::NOT_FOUND,
        format!("unknown reducer `{name}` (RED-006)"),
    )
}

fn reentrant_handle() -> FluxumError {
    FluxumError::Storage(
        "TxHandle used reentrantly from inside another TxHandle operation \
         (e.g. a scan predicate issuing writes); restructure the reducer to \
         finish the read before writing"
            .into(),
    )
}
