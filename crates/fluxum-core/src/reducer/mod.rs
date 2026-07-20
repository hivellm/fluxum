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

pub mod args;
pub mod engine;
pub mod idempotency;
pub mod ratelimit;
pub mod stdlib;
pub mod view;

pub use stdlib::Rng;

use std::cell::{Cell, Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::fmt;

/// Re-exported for the `#[fluxum::reducer]` macro expansion (one stable
/// root path) and for module authors passing raw argument lists.
pub use fluxum_protocol::FluxValue;
use fluxum_protocol::codes;

use crate::error::{FluxumError, Result};
use crate::schema::Table;
use crate::store::{Row, TableId, TriggerKind, Tx};
use crate::types::{ConnectionId, Identity, Timestamp};

pub use engine::{
    CallOutcome, LifecycleDef, LifecycleHooks, LifecycleKind, MAX_IDEMPOTENCY_KEY_BYTES,
    ReducerBounds, ReducerEngine, StartupReport,
};
pub use idempotency::{IDEMPOTENCY_TABLE, IDEMPOTENCY_TABLE_NAME, IdempotencyOptions};
pub use ratelimit::{QueryLimiter, QueryRejected, QuerySource, RateLimiter, RateLimiterOptions};
pub use view::{
    MaterializedViewDef, MvAggregate, MvTopN, ReadOnlyTxHandle, ViewContext, ViewDef, ViewRegistry,
    registered_materialized_views,
};

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
/// macro generates the decode glue that turns `args` into typed parameters).
pub type ReducerHandler =
    Box<dyn Fn(&ReducerContext<'_, '_, '_>, &[FluxValue]) -> Result<()> + Send + Sync>;

/// The static handler shape `#[fluxum::reducer]` emits (fn pointer, so a
/// [`ReducerDef`] can live in the link-time registry).
pub type ReducerFnPtr = fn(&ReducerContext<'_, '_, '_>, &[FluxValue]) -> Result<()>;

/// Argument pre-validation (arity + per-parameter decode) run by the engine
/// **before** any transaction is started (RED-001).
pub type ArgCheckFn = fn(&[FluxValue]) -> Result<()>;

/// Wrap a closure or fn as a [`ReducerHandler`] (helps closure inference
/// across the higher-ranked context lifetimes).
pub fn handler<F>(f: F) -> ReducerHandler
where
    F: Fn(&ReducerContext<'_, '_, '_>, &[FluxValue]) -> Result<()> + Send + Sync + 'static,
{
    Box::new(f)
}

/// One `#[fluxum::reducer]` in the link-time registry (RED-006): collected
/// by [`ReducerRegistry::from_registered`] at startup, exactly like tables
/// (DM-040) and migrations (MIG-010).
pub struct ReducerDef {
    /// Reducer function name — the `ReducerCall` dispatch key.
    pub name: &'static str,
    /// The macro-generated dispatch glue (decode args, call the function).
    pub handler: ReducerFnPtr,
    /// The macro-generated pre-transaction argument check (RED-001).
    pub check_args: ArgCheckFn,
    /// Whether clients may invoke this reducer via `ReducerCall` (RED-025).
    /// `#[fluxum::reducer]` emits `true`; `#[fluxum::tick]` /
    /// `#[fluxum::schedule]` emit `false` unless the declaration opts in
    /// with `client_callable = true` — schedule-only reducers answer
    /// clients with a wire-ready 403 before any transaction.
    pub client_callable: bool,
    /// `#[fluxum::reducer(max_rate = "N/s")]` (RED-050): the per-
    /// `(Identity, reducer)` admission rate; `0` = unlimited.
    pub max_rate_per_sec: u32,
}

inventory::collect!(ReducerDef);

/// Iterate every `#[fluxum::reducer]` registered in this binary, in linker
/// order (the registry map is name-keyed; order is irrelevant).
pub fn registered_reducers() -> impl Iterator<Item = &'static ReducerDef> {
    inventory::iter::<ReducerDef>()
}

/// One declared reducer parameter, as the module's source spells it
/// (SPEC-011 SDK-001): the name a generated client uses for the argument and
/// the Rust type string a generator maps to its own type system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParamDef {
    /// The parameter's declared name.
    pub name: &'static str,
    /// Its declared Rust type, verbatim (`u64`, `String`, …).
    pub ty: &'static str,
}

/// A reducer's call signature, for `/schema` and every SDK generator that
/// consumes it (SPEC-011, FR-81).
///
/// Deliberately a *separate* link-time registry rather than fields on
/// [`ReducerDef`]: the signature is metadata only the `#[fluxum::reducer]`
/// macro can know, while `ReducerDef` is also written by hand (tests,
/// embedders) where a signature would be noise. Keeping them apart means a
/// hand-written def stays valid and simply reports no parameters, and adding
/// signature metadata never breaks an existing definition — which is exactly
/// the additive-only contract the module API freeze commits to.
#[derive(Debug, Clone, Copy)]
pub struct ReducerSignature {
    /// The reducer this describes (joins to [`ReducerDef::name`]).
    pub reducer: &'static str,
    /// Declared parameters after the context, in call order.
    pub params: &'static [ParamDef],
    /// The declared return type, verbatim.
    pub returns: &'static str,
}

inventory::collect!(ReducerSignature);

/// Iterate every reducer signature registered in this binary.
pub fn registered_signatures() -> impl Iterator<Item = &'static ReducerSignature> {
    inventory::iter::<ReducerSignature>()
}

/// The signature of `reducer`, if the macro registered one (a hand-written
/// [`ReducerDef`] has none).
pub fn signature_of(reducer: &str) -> Option<&'static ReducerSignature> {
    registered_signatures().find(|sig| sig.reducer == reducer)
}

/// The static handler shape the `#[fluxum::on_insert/on_update/on_delete]`
/// macros emit (SPEC-022 RV-031): `(ctx, old row, new row)` — `old` is set
/// for Update/Delete, `new` for Insert/Update; rows arrive decrypted.
pub type TriggerFnPtr = fn(&ReducerContext<'_, '_, '_>, Option<&Row>, Option<&Row>) -> Result<()>;

/// One `#[fluxum::on_insert(Table)]` / `on_update` / `on_delete` hook in the
/// link-time registry (SPEC-022 RV-031): dispatched by [`TxHandle`] inside
/// the same transaction as the mutation that fired it, under the caller's
/// identity — an `Err` rolls the whole transaction back.
pub struct TriggerDef {
    /// The `#[fluxum::table]` struct name the hook watches.
    pub table: &'static str,
    /// Which mutation fires the hook.
    pub kind: TriggerKind,
    /// The hook function's name (diagnostics).
    pub name: &'static str,
    /// The macro-generated dispatch glue (decode rows, call the function).
    pub handler: TriggerFnPtr,
}

inventory::collect!(TriggerDef);

/// Iterate every registered trigger hook in this binary (linker order).
pub fn registered_triggers() -> impl Iterator<Item = &'static TriggerDef> {
    inventory::iter::<TriggerDef>()
}

/// The link-time trigger map, keyed by table id (built once, lazily).
fn trigger_map() -> &'static HashMap<TableId, Vec<&'static TriggerDef>> {
    static MAP: std::sync::OnceLock<HashMap<TableId, Vec<&'static TriggerDef>>> =
        std::sync::OnceLock::new();
    MAP.get_or_init(|| {
        let mut map: HashMap<TableId, Vec<&'static TriggerDef>> = HashMap::new();
        for def in registered_triggers() {
            map.entry(TableId::of(def.table)).or_default().push(def);
        }
        map
    })
}

/// Whether `table` has any registered `#[fluxum::on_*]` hook (RV-031) — the
/// store's write/delete paths record trigger events only when this holds.
pub fn has_triggers(table: TableId) -> bool {
    trigger_map().contains_key(&table)
}

/// The registered hooks of `(table, kind)`, in linker order.
fn triggers_for(table: TableId, kind: TriggerKind) -> Vec<&'static TriggerDef> {
    trigger_map()
        .get(&table)
        .map(|defs| defs.iter().copied().filter(|d| d.kind == kind).collect())
        .unwrap_or_default()
}

/// One registered reducer: the dispatch body plus the optional
/// pre-transaction argument check (absent for closure-registered reducers,
/// which have no declared signature to check against) and the RED-025
/// client-callability flag.
struct Registered {
    handler: ReducerHandler,
    check_args: Option<ArgCheckFn>,
    client_callable: bool,
    max_rate_per_sec: u32,
}

/// Name → reducer map (RED-006): populated at startup — from the link-time
/// registry via [`ReducerRegistry::from_registered`], or programmatically
/// via [`ReducerRegistry::register`] — then read by every dispatch,
/// including nested [`TxHandle::call`]s.
#[derive(Default)]
pub struct ReducerRegistry {
    handlers: HashMap<String, Registered>,
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

    /// Collect every `#[fluxum::reducer]` of this binary (RED-006). A
    /// duplicate name aborts startup with a [`FluxumError::Schema`].
    pub fn from_registered() -> Result<Self> {
        Self::from_defs(registered_reducers())
    }

    /// [`ReducerRegistry::from_registered`] with explicit defs — the seam
    /// tests and embedders use instead of the link-time registry.
    pub fn from_defs(defs: impl IntoIterator<Item = &'static ReducerDef>) -> Result<Self> {
        let mut registry = Self::new();
        for def in defs {
            registry.register_def(def)?;
        }
        Ok(registry)
    }

    /// Register one link-time [`ReducerDef`] (duplicate name = startup
    /// error, RED-006).
    pub fn register_def(&mut self, def: &'static ReducerDef) -> Result<()> {
        self.insert(
            def.name.to_owned(),
            Box::new(def.handler),
            Some(def.check_args),
            def.client_callable,
            def.max_rate_per_sec,
        )
    }

    /// Register a reducer under `name`. A duplicate name is a startup error
    /// (RED-006) — [`FluxumError::Schema`], which must abort boot. Closure
    /// registrations carry no argument check (there is no declared
    /// signature), are client-callable, and declare no rate limit;
    /// [`ReducerRegistry::check_call`] then validates the name only.
    pub fn register(&mut self, name: impl Into<String>, handler: ReducerHandler) -> Result<()> {
        self.insert(name.into(), handler, None, true, 0)
    }

    fn insert(
        &mut self,
        name: String,
        handler: ReducerHandler,
        check_args: Option<ArgCheckFn>,
        client_callable: bool,
        max_rate_per_sec: u32,
    ) -> Result<()> {
        if self.handlers.contains_key(&name) {
            return Err(FluxumError::Schema(format!(
                "duplicate reducer name `{name}`: reducer names must be unique (RED-006)"
            )));
        }
        self.handlers.insert(
            name,
            Registered {
                handler,
                check_args,
                client_callable,
                max_rate_per_sec,
            },
        );
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

    /// The declared `(client_callable, max_rate_per_sec)` of `name` — the
    /// admission facts `/schema` publishes for SDK codegen (SDK-001).
    pub fn declaration(&self, name: &str) -> Option<(bool, u32)> {
        self.handlers
            .get(name)
            .map(|r| (r.client_callable, r.max_rate_per_sec))
    }

    /// Client-path name admission (RED-006/RED-025): unknown names are a
    /// wire-ready 404 and a schedule-only reducer answers clients 403 — no
    /// transaction or `TxState` ever exists. Returns the reducer's declared
    /// `max_rate` (RED-050; `0` = unlimited) for the engine's rate check.
    ///
    /// Scheduled executions ([`crate::scheduler`]) dispatch directly and
    /// are exempt from admission entirely.
    pub fn admission(&self, name: &str) -> Result<u32> {
        let registered = self
            .handlers
            .get(name)
            .ok_or_else(|| unknown_reducer(name))?;
        if !registered.client_callable {
            return Err(FluxumError::query(
                codes::REDUCER_SCHEDULE_ONLY,
                format!("schedule-only reducer `{name}` (RED-025)"),
            ));
        }
        Ok(registered.max_rate_per_sec)
    }

    /// Argument admission (RED-001): a `#[fluxum::reducer]`-declared
    /// signature validates arity and argument types before any transaction.
    pub fn check_args(&self, name: &str, args: &[FluxValue]) -> Result<()> {
        let registered = self
            .handlers
            .get(name)
            .ok_or_else(|| unknown_reducer(name))?;
        if let Some(check) = registered.check_args {
            check(args)?;
        }
        Ok(())
    }

    /// Full client admission minus rate limiting: name + callability +
    /// arguments ([`ReducerRegistry::admission`] then
    /// [`ReducerRegistry::check_args`]).
    pub fn check_call(&self, name: &str, args: &[FluxValue]) -> Result<()> {
        self.admission(name)?;
        self.check_args(name, args)
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
        self.dispatch_bounded(caller, name, args, tx, ExecBounds::default())
    }

    /// [`ReducerRegistry::dispatch`] under SEC-046 [`ExecBounds`] — the
    /// engine's client path (deadline + write ceiling); scheduled and
    /// lifecycle executions keep the unbounded form.
    pub fn dispatch_bounded(
        &self,
        caller: ReducerCaller,
        name: &str,
        args: &[FluxValue],
        tx: &mut Tx<'_>,
        bounds: ExecBounds,
    ) -> Result<()> {
        let registered = self
            .handlers
            .get(name)
            .ok_or_else(|| unknown_reducer(name))?;
        with_context_bounded(self, caller, tx, bounds, |ctx| {
            (registered.handler)(ctx, args)
        })
    }
}

/// SEC-046 execution bounds for one reducer transaction: the cooperative
/// wall-clock deadline (polled at every host-call boundary — the `ctx.tx`
/// surface every table access goes through) and the per-transaction write
/// ceiling. [`ExecBounds::default`] is fully unbounded (lifecycle hooks,
/// scheduled executions, embedders); the engine derives client-call bounds
/// from its configured [`engine::ReducerBounds`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ExecBounds {
    /// Abort (roll back) host calls past this instant. `None` = no deadline.
    pub deadline: Option<std::time::Instant>,
    /// Ceiling on the estimated bytes this transaction may buffer through
    /// `insert`/`upsert`. `0` = unbounded.
    pub max_tx_bytes: u64,
}

/// Run `body` with a [`ReducerContext`] over `tx` — the closure-shaped
/// entry ([`ReducerRegistry::dispatch`] is the by-name shape). Used by
/// lifecycle hooks, tests, and anywhere the reducer body is already in hand.
/// Unbounded ([`ExecBounds::default`]); the engine's client path enters via
/// [`with_context_bounded`].
pub fn with_context<'t, 's, R>(
    registry: &'t ReducerRegistry,
    caller: ReducerCaller,
    tx: &'t mut Tx<'s>,
    body: impl FnOnce(&ReducerContext<'_, 't, 's>) -> Result<R>,
) -> Result<R> {
    with_context_bounded(registry, caller, tx, ExecBounds::default(), body)
}

/// [`with_context`] under SEC-046 [`ExecBounds`]. A breached bound is
/// **latched**: even a reducer that catches the typed error and returns
/// `Ok` still rolls back — a caught deadline must not smuggle a partial
/// write set into a commit.
pub fn with_context_bounded<'t, 's, R>(
    registry: &'t ReducerRegistry,
    caller: ReducerCaller,
    tx: &'t mut Tx<'s>,
    bounds: ExecBounds,
    body: impl FnOnce(&ReducerContext<'_, 't, 's>) -> Result<R>,
) -> Result<R> {
    // SEC-020: seed the transaction's RNG from (tx_id, shard_id) before the
    // Tx is moved behind the RefCell.
    let seed = stdlib::seed_from(tx.tx_id(), caller.shard_id);
    let env = TxEnv {
        tx: RefCell::new(tx),
        registry,
        caller,
        depth: Cell::new(0),
        rng: stdlib::Rng::new(seed),
        bounds,
        written_bytes: Cell::new(0),
        breached: Cell::new(None),
    };
    let ctx = env.context();
    let result = body(&ctx);
    match env.breached.get() {
        // SEC-046: the latch wins over a caught error — see above.
        Some(code) if result.is_ok() => Err(bound_breach_error(code)),
        _ => result,
    }
}

/// The typed wire error of a breached SEC-046 bound.
fn bound_breach_error(code: u16) -> FluxumError {
    if code == codes::REDUCER_TX_BUDGET_EXCEEDED {
        FluxumError::query(
            codes::REDUCER_TX_BUDGET_EXCEEDED,
            "reducer aborted: per-transaction write ceiling exceeded; rolled back (SEC-046)",
        )
    } else {
        FluxumError::query(
            codes::REDUCER_DEADLINE_EXCEEDED,
            "reducer aborted: execution deadline exceeded; rolled back (SEC-046)",
        )
    }
}

/// Shared per-transaction state behind every [`TxHandle`] of one reducer
/// call tree: the single-writer [`Tx`], the registry for nested calls, the
/// caller metadata, and the RED-005 recursion depth.
struct TxEnv<'t, 's> {
    tx: RefCell<&'t mut Tx<'s>>,
    registry: &'t ReducerRegistry,
    caller: ReducerCaller,
    depth: Cell<u32>,
    /// The deterministic per-transaction RNG (SEC-020), seeded from
    /// `(tx_id, shard_id)` and shared by the whole call tree so nested
    /// reducers draw from one reproducible stream.
    rng: stdlib::Rng,
    /// SEC-046 execution bounds for this call tree.
    bounds: ExecBounds,
    /// Estimated bytes buffered by this transaction's inserts/upserts,
    /// checked against `bounds.max_tx_bytes`.
    written_bytes: Cell<u64>,
    /// The first breached bound's wire code — latched so a caught error
    /// still rolls the transaction back (see [`with_context_bounded`]).
    breached: Cell<Option<u16>>,
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

impl ReducerContext<'_, '_, '_> {
    /// Enqueue `reducer` for execution after `delay` (RED-021, FR-22).
    ///
    /// The enqueue is a `__schedule__` insert **inside this call's
    /// transaction**: if the transaction rolls back, the scheduled call is
    /// discarded with it — the [`crate::scheduler::ScheduleWorker`] re-reads
    /// the committed row at fire time, so a rolled-back enqueue can never
    /// fire. Re-arming from inside the fired reducer creates a recurring
    /// pattern (RED-022). The target must be a registered reducer; the
    /// shard's schema must include [`crate::scheduler::SCHEDULE_TABLE`].
    pub fn schedule_after(
        &self,
        delay: std::time::Duration,
        reducer: &str,
        args: &[FluxValue],
    ) -> Result<()> {
        if !self.tx.env.registry.contains(reducer) {
            return Err(unknown_reducer(reducer));
        }
        let delay_us = i64::try_from(delay.as_micros()).map_err(|_| {
            FluxumError::query(
                codes::REDUCER_BAD_ARGS,
                format!("schedule_after delay {delay:?} overflows the µs clock"),
            )
        })?;
        self.tx.insert(crate::scheduler::ScheduleEntry {
            id: 0,
            reducer_name: reducer.to_owned(),
            args: crate::scheduler::encode_args(args)?,
            execute_at_us: self.timestamp.as_micros().saturating_add(delay_us),
            period_us: 0,
            shard_id: self.shard_id,
        })?;
        Ok(())
    }

    /// The transaction's deterministic RNG (SPEC-026 SEC-020), seeded from
    /// `(tx_id, shard_id)` and shared across the whole reducer call tree.
    ///
    /// Use this instead of `rand::random`/`OsRng`: reducer output must be
    /// reproducible under commit-log replay and deterministic simulation
    /// (SEC-021), which OS entropy breaks. The same transaction always yields
    /// the same sequence.
    ///
    /// ```ignore
    /// let roll = ctx.rng().below(6) + 1;      // fair d6, deterministic
    /// let jitter = ctx.rng().range(-50, 50);  // deterministic jitter
    /// ```
    pub fn rng(&self) -> &Rng {
        &self.tx.env.rng
    }

    /// Floor `ctx.timestamp` to a multiple of `interval` — deterministic
    /// time-bucketing (SEC-020) that never reads the wall clock. Returns the
    /// bucket's start in µs since the Unix epoch. A zero interval returns the
    /// raw timestamp. Correct for pre-epoch (negative) timestamps.
    pub fn time_bucket(&self, interval: std::time::Duration) -> i64 {
        let micros = self.timestamp.as_micros();
        let step = i64::try_from(interval.as_micros()).unwrap_or(i64::MAX);
        if step <= 0 {
            return micros;
        }
        micros - micros.rem_euclid(step)
    }

    /// The index of the `interval`-sized bucket that `ctx.timestamp` falls in
    /// (SEC-020): `floor(timestamp / interval)`. A zero interval yields `0`.
    /// Correct for pre-epoch (negative) timestamps.
    pub fn bucket_index(&self, interval: std::time::Duration) -> i64 {
        let micros = self.timestamp.as_micros();
        let step = i64::try_from(interval.as_micros()).unwrap_or(i64::MAX);
        if step <= 0 {
            return 0;
        }
        micros.div_euclid(step)
    }
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
        self.charge_write(&stored)?;
        self.dispatch_triggers()?;
        T::from_values(self.decrypt_row::<T>(stored)?.values())
    }

    /// Insert or replace by primary key (the TXN-040 exception); `#[unique]`
    /// constraints against *other* rows still apply. Returns the row as
    /// stored, auto-inc placeholder resolved exactly as in
    /// [`TxHandle::insert`].
    pub fn upsert<T: Table>(&self, row: T) -> Result<T> {
        let stored = self
            .exclusive()?
            .upsert(table_of::<T>(), row.into_values())?;
        self.charge_write(&stored)?;
        self.dispatch_triggers()?;
        T::from_values(self.decrypt_row::<T>(stored)?.values())
    }

    /// Delete the row with primary key `pk`. Returns whether a (committed
    /// or pending) row was deleted; deleting a row this same transaction
    /// inserted cancels the insert entirely (STG-007).
    pub fn delete<T: Table>(&self, pk: T::Pk) -> Result<bool> {
        let deleted = self
            .exclusive()?
            .delete(table_of::<T>(), &T::pk_values(&pk))?;
        self.dispatch_triggers()?;
        Ok(deleted)
    }

    /// Delete every committed row matching `pred`; returns how many rows
    /// were deleted. The predicate is evaluated over the committed
    /// pre-transaction snapshot — the same view [`TxHandle::scan`] reads
    /// (TXN-050); rows inserted by this transaction are not candidates.
    pub fn delete_where<T: Table>(&self, pred: impl Fn(&T) -> bool) -> Result<u64> {
        let rows = self.committed_rows::<T>()?;
        let mut deleted = 0u64;
        for row in rows {
            let typed = T::from_values(self.decrypt_row::<T>(row)?.values())?;
            if pred(&typed)
                && self
                    .exclusive()?
                    .delete(table_of::<T>(), &T::pk_values(&typed.primary_key()))?
            {
                deleted += 1;
            }
        }
        self.dispatch_triggers()?;
        Ok(deleted)
    }

    // --- Committed-state reads (TXN-050: pre-transaction snapshot only) ----

    /// Point lookup by primary key against the committed snapshot captured
    /// at transaction begin. Never sees this transaction's pending writes.
    pub fn query_pk<T: Table>(&self, pk: T::Pk) -> Result<Option<T>> {
        let row = self
            .shared()?
            .query_pk(table_of::<T>(), &T::pk_values(&pk))?;
        row.map(|r| T::from_values(self.decrypt_row::<T>(r)?.values()))
            .transpose()
    }

    /// Full scan of the committed snapshot, in encoded-PK byte order.
    /// Never sees this transaction's pending writes (TXN-050).
    pub fn scan<T: Table>(&self) -> Result<Vec<T>> {
        self.decode_rows_plain::<T>(self.committed_rows::<T>()?)
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
        self.decode_rows_plain::<T>(rows)
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
        self.decode_rows_plain::<T>(rows)
    }

    /// Filtered combined view (see [`TxHandle::scan_all`]).
    pub fn scan_all_where<T: Table>(&self, pred: impl Fn(&T) -> bool) -> Result<Vec<T>> {
        let mut rows = self.scan_all::<T>()?;
        rows.retain(|row| pred(row));
        Ok(rows)
    }

    // --- Edge traversal (SPEC-023 DMX-050) -----------------------------------

    /// The outgoing edges of one node: every committed `E` row whose `from`
    /// column equals `from` — one B-tree prefix scan over the edge's
    /// `btree(from)` neighbor index, O(log n + k). A point traversal, never
    /// a JOIN (SPEC-023 §8); committed view (TXN-050).
    pub fn traverse<E: Table>(&self, from: impl Into<crate::store::RowValue>) -> Result<Vec<E>> {
        let table = table_of::<E>();
        let schema = E::SCHEMA;
        let Some(from_column) = schema.columns.first().filter(|c| c.name == "from") else {
            return Err(FluxumError::Storage(format!(
                "`{}` is not a #[fluxum::edge] table: traversal needs the `from` \
                 neighbor index (DMX-050)",
                schema.name
            )));
        };
        let index = crate::index::IndexId::of(schema.name, &[from_column.name]);
        let from = from.into();
        let rows: Vec<Row> = {
            let tx = self.shared()?;
            tx.index_eq(table, index, std::slice::from_ref(&from))?
                .cloned()
                .collect()
        };
        self.decode_rows_plain::<E>(rows)
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
        let registered = self
            .env
            .registry
            .handlers
            .get(reducer)
            .ok_or_else(|| unknown_reducer(reducer))?;
        let depth = self.env.depth.get();
        if depth >= MAX_CALL_DEPTH {
            return Err(FluxumError::query(
                codes::SYS_INTERNAL,
                format!(
                    "reducer call depth exceeded {MAX_CALL_DEPTH} calling `{reducer}`: \
                     unbounded recursion via ctx.tx.call (RED-005)"
                ),
            ));
        }
        self.env.depth.set(depth + 1);
        let ctx = self.env.context();
        let result = (registered.handler)(&ctx, args);
        self.env.depth.set(depth);
        result
    }

    // --- Internals -----------------------------------------------------------

    /// SPEC-022 RV-031: drain the transaction's recorded mutation events and
    /// run every registered `#[fluxum::on_*]` hook — inside this same
    /// transaction, under the caller's identity. A hook's own mutations
    /// dispatch recursively through its `ctx.tx` calls; the RED-005 depth
    /// cap bounds runaway trigger→mutation→trigger recursion. A hook `Err`
    /// propagates and rolls the whole transaction back.
    fn dispatch_triggers(&self) -> Result<()> {
        loop {
            let events = self.exclusive()?.take_trigger_events();
            if events.is_empty() {
                return Ok(());
            }
            for event in events {
                let defs = triggers_for(event.table, event.kind);
                if defs.is_empty() {
                    continue;
                }
                // Hooks see plaintext rows (SPEC-017 CT-031: server peers).
                let decrypt = |row: &Option<Row>| -> Result<Option<Row>> {
                    row.as_ref()
                        .map(|r| self.shared()?.decrypt_stored(event.table, r))
                        .transpose()
                };
                let old = decrypt(&event.old)?;
                let new = decrypt(&event.new)?;
                let depth = self.env.depth.get();
                if depth >= MAX_CALL_DEPTH {
                    return Err(FluxumError::query(
                        codes::SYS_INTERNAL,
                        format!(
                            "trigger dispatch depth exceeded {MAX_CALL_DEPTH}: unbounded \
                             mutation→trigger recursion (RV-031/RED-005)"
                        ),
                    ));
                }
                self.env.depth.set(depth + 1);
                let ctx = self.env.context();
                let result = defs
                    .iter()
                    .try_for_each(|def| (def.handler)(&ctx, old.as_ref(), new.as_ref()));
                self.env.depth.set(depth);
                result?;
            }
        }
    }

    /// Committed rows of `T`'s table, cloned out (`Arc` bumps) so the
    /// transaction borrow is released before predicates or decoding run.
    fn committed_rows<T: Table>(&self) -> Result<Vec<Row>> {
        let tx = self.shared()?;
        Ok(tx.scan(table_of::<T>())?.cloned().collect())
    }

    /// Decrypt a stored row's `#[encrypted]` columns for the reducer
    /// (SPEC-017 CT-031). Reducers run as server peers (AUTH-062), so they are
    /// always authorized to see plaintext. A no-op when no transform engine is
    /// attached or `T`'s table has no encrypted column.
    fn decrypt_row<T: Table>(&self, row: Row) -> Result<Row> {
        let engine = self.env.tx.borrow().transform_engine();
        let Some(engine) = engine else {
            return Ok(row);
        };
        let table = table_of::<T>();
        if !engine.touches(table) {
            return Ok(row);
        }
        let mut values = row.values().to_vec();
        let pk = crate::store::row::encode_pk_of_row(T::SCHEMA, &values)?;
        engine.on_read_row(table, &mut values, pk.as_bytes(), true)?;
        Ok(Row::new(values))
    }

    /// [`Self::decrypt_row`] over many rows, then typed decode.
    fn decode_rows_plain<T: Table>(&self, rows: Vec<Row>) -> Result<Vec<T>> {
        rows.into_iter()
            .map(|row| T::from_values(self.decrypt_row::<T>(row)?.values()))
            .collect()
    }

    /// SEC-046: poll the cooperative execution deadline. Sits inside
    /// [`TxHandle::shared`]/[`TxHandle::exclusive`], so **every** host call
    /// — reads, writes, nested dispatch, triggers — is a check point; a
    /// runaway loop that never calls the host is bounded by the SEC-021
    /// determinism rule (no I/O, no ambient time) making such a loop useless.
    fn check_deadline(&self) -> Result<()> {
        if let Some(deadline) = self.env.bounds.deadline
            && std::time::Instant::now() > deadline
        {
            self.env
                .breached
                .set(Some(codes::REDUCER_DEADLINE_EXCEEDED));
            return Err(bound_breach_error(codes::REDUCER_DEADLINE_EXCEEDED));
        }
        Ok(())
    }

    /// SEC-046: account one stored row against the per-transaction write
    /// ceiling; breach → latch + typed error (rollback).
    fn charge_write(&self, stored: &Row) -> Result<()> {
        let max = self.env.bounds.max_tx_bytes;
        if max == 0 {
            return Ok(());
        }
        let written = self
            .env
            .written_bytes
            .get()
            .saturating_add(estimate_values_bytes(stored.values()));
        self.env.written_bytes.set(written);
        if written > max {
            self.env
                .breached
                .set(Some(codes::REDUCER_TX_BUDGET_EXCEEDED));
            return Err(bound_breach_error(codes::REDUCER_TX_BUDGET_EXCEEDED));
        }
        Ok(())
    }

    /// Shared access to the transaction. Cannot be contended in correct
    /// usage (every method scopes its borrow); surfaced as an error, never
    /// a panic (RED-061).
    fn shared(&self) -> Result<Ref<'e, &'t mut Tx<'s>>> {
        self.check_deadline()?;
        self.env.tx.try_borrow().map_err(|_| reentrant_handle())
    }

    /// Exclusive access to the transaction (write path). Same contention
    /// story as [`TxHandle::shared`].
    fn exclusive(&self) -> Result<RefMut<'e, &'t mut Tx<'s>>> {
        self.check_deadline()?;
        self.env.tx.try_borrow_mut().map_err(|_| reentrant_handle())
    }
}

/// Estimate a stored row's in-memory footprint (SEC-046 write accounting):
/// a fixed per-value overhead plus every owned payload byte, recursively.
/// An estimate, deliberately — the ceiling is an availability bound, not an
/// accounting ledger, so being within a small constant factor is enough.
fn estimate_values_bytes(values: &[crate::store::RowValue]) -> u64 {
    use crate::store::RowValue as V;
    fn one(value: &crate::store::RowValue) -> u64 {
        16 + match value {
            V::Str(s) => s.len() as u64,
            V::Bytes(b) => b.len() as u64,
            V::Optional(Some(inner)) => one(inner),
            V::List(items) | V::Struct(items) => items.iter().map(one).sum(),
            V::Enum { payload, .. } => payload.iter().map(one).sum(),
            _ => 0,
        }
    }
    values.iter().map(one).sum()
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
        codes::REDUCER_UNKNOWN,
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
