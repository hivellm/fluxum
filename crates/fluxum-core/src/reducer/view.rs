//! Read-only views (SPEC-004 §5, T3.3): [`ViewContext`] and the
//! [`ReadOnlyTxHandle`] a `#[fluxum::view]` function receives, plus the
//! [`ViewRegistry`] the HTTP admin API (`GET /view/:name`, phase 5) will
//! dispatch through.
//!
//! Write-safety is a **type-level** guarantee (RED-031): `ReadOnlyTxHandle`
//! simply has no write methods, so a view that attempts `ctx.tx.insert(...)`
//! fails to compile (the fluxum-macros UI suite pins that). Views read the
//! committed state directly through a lock-free [`Snapshot`] (RED-030) —
//! they never enter the transaction pipeline, never queue behind writers,
//! and cannot observe in-flight transactions.

use std::collections::HashMap;

use fluxum_protocol::codes;
/// Re-exported for the `#[fluxum::view]` macro expansion (result
/// serialization to the admin API's JSON).
pub use serde_json;

use crate::error::{FluxumError, Result};
use crate::schema::Table;
use crate::store::Snapshot;
use crate::types::Timestamp;

use super::{FluxValue, decode_rows, table_of};

/// What every view receives at call time (RED-031): call metadata plus the
/// read-only committed-state handle.
pub struct ViewContext<'a> {
    /// Call timestamp (µs since Unix epoch).
    pub timestamp: Timestamp,
    /// Shard this view reads.
    pub shard_id: u32,
    /// Read-only committed-state access (RED-030).
    pub tx: ReadOnlyTxHandle<'a>,
}

/// The typed read-only surface of a view (RED-031): the read operations of
/// RED-003 over a committed [`Snapshot`], and nothing else — write methods
/// do not exist on this type, by design.
#[derive(Clone, Copy)]
pub struct ReadOnlyTxHandle<'a> {
    snapshot: &'a Snapshot,
}

impl<'a> ReadOnlyTxHandle<'a> {
    /// A handle over a committed snapshot.
    pub fn new(snapshot: &'a Snapshot) -> Self {
        Self { snapshot }
    }

    /// Point lookup by primary key against the committed state.
    pub fn query_pk<T: Table>(&self, pk: T::Pk) -> Result<Option<T>> {
        let row = self
            .snapshot
            .query_pk(table_of::<T>(), &T::pk_values(&pk))?;
        row.map(|r| T::from_values(r.values())).transpose()
    }

    /// Full scan of the committed state, in encoded-PK byte order.
    pub fn scan<T: Table>(&self) -> Result<Vec<T>> {
        let rows: Vec<crate::store::Row> = self.snapshot.scan(table_of::<T>())?.cloned().collect();
        decode_rows(&rows)
    }

    /// Filtered scan of the committed state.
    pub fn scan_where<T: Table>(&self, pred: impl Fn(&T) -> bool) -> Result<Vec<T>> {
        let mut rows = self.scan::<T>()?;
        rows.retain(|row| pred(row));
        Ok(rows)
    }
}

/// The static handler shape `#[fluxum::view]` emits: decode the arguments,
/// run the view, serialize the result for the JSON admin API.
pub type ViewFnPtr = fn(&ViewContext<'_>, &[FluxValue]) -> Result<serde_json::Value>;

/// One `#[fluxum::view]` in the link-time registry (RED-030), collected by
/// [`ViewRegistry::from_registered`] at startup.
pub struct ViewDef {
    /// View function name — the `GET /view/:name` dispatch key.
    pub name: &'static str,
    /// The macro-generated dispatch glue.
    pub handler: ViewFnPtr,
}

inventory::collect!(ViewDef);

/// Iterate every `#[fluxum::view]` registered in this binary.
pub fn registered_views() -> impl Iterator<Item = &'static ViewDef> {
    inventory::iter::<ViewDef>()
}

// ---------------------------------------------------------------------------
// Materialized views (SPEC-022 RV-010..013)
// ---------------------------------------------------------------------------

/// The aggregate of a `#[fluxum::view(materialized)]` declaration (RV-010).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MvAggregate {
    /// `count` — rows per group.
    Count,
    /// `sum(col)` — numeric column sum.
    Sum(&'static str),
    /// `avg(col)` — numeric column mean.
    Avg(&'static str),
    /// `min(col)`.
    Min(&'static str),
    /// `max(col)`.
    Max(&'static str),
}

impl MvAggregate {
    /// The aggregated column, if the function takes one.
    pub fn column(&self) -> Option<&'static str> {
        match self {
            Self::Count => None,
            Self::Sum(c) | Self::Avg(c) | Self::Min(c) | Self::Max(c) => Some(c),
        }
    }
}

/// The sorted-window shape of a top-N materialized view (RV-012).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MvTopN {
    /// The sort column.
    pub column: &'static str,
    /// `true` = highest first (the leaderboard shape).
    pub descending: bool,
    /// Window size.
    pub limit: u32,
}

/// One `#[fluxum::view(materialized, …)]` in the link-time registry
/// (SPEC-022 RV-010): a view over one base table with either an aggregate
/// (+ optional `GROUP BY`) or a sorted top-N window — maintained
/// incrementally from commit delta rows by the subscription manager's view
/// engine, never by re-scanning.
pub struct MaterializedViewDef {
    /// The view's name — the subscription key and the pushed `table_name`.
    pub name: &'static str,
    /// The base `#[fluxum::table]` struct name.
    pub table: &'static str,
    /// The aggregate (`None` for a top-N window view).
    pub aggregate: Option<MvAggregate>,
    /// `GROUP BY` column (aggregate views only; `None` = one global group).
    pub group_by: Option<&'static str>,
    /// The sorted window (top-N views only).
    pub top_n: Option<MvTopN>,
}

inventory::collect!(MaterializedViewDef);

/// Iterate every registered materialized view in this binary.
pub fn registered_materialized_views() -> impl Iterator<Item = &'static MaterializedViewDef> {
    inventory::iter::<MaterializedViewDef>()
}

/// Name → view map (RED-030): populated at startup, dispatched by the HTTP
/// admin API (phase 5).
#[derive(Default)]
pub struct ViewRegistry {
    views: HashMap<String, ViewFnPtr>,
}

impl std::fmt::Debug for ViewRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut names: Vec<&str> = self.names().collect();
        names.sort_unstable();
        f.debug_struct("ViewRegistry")
            .field("views", &names)
            .finish()
    }
}

impl ViewRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Collect every `#[fluxum::view]` of this binary. A duplicate name
    /// aborts startup.
    pub fn from_registered() -> Result<Self> {
        Self::from_defs(registered_views())
    }

    /// [`ViewRegistry::from_registered`] with explicit defs (test seam).
    pub fn from_defs(defs: impl IntoIterator<Item = &'static ViewDef>) -> Result<Self> {
        let mut registry = Self::new();
        for def in defs {
            registry.register(def.name, def.handler)?;
        }
        Ok(registry)
    }

    /// Register a view under `name`; a duplicate name is a startup error.
    pub fn register(&mut self, name: impl Into<String>, handler: ViewFnPtr) -> Result<()> {
        let name = name.into();
        if self.views.contains_key(&name) {
            return Err(FluxumError::Schema(format!(
                "duplicate view name `{name}`: view names must be unique (RED-030)"
            )));
        }
        self.views.insert(name, handler);
        Ok(())
    }

    /// Whether `name` is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.views.contains_key(name)
    }

    /// Registered view names (unordered).
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.views.keys().map(String::as_str)
    }

    /// Execute view `name` over `snapshot` (RED-030): lock-free, no
    /// transaction, no pipeline. Unknown names are a wire-ready 404.
    pub fn dispatch(
        &self,
        name: &str,
        snapshot: &Snapshot,
        shard_id: u32,
        args: &[FluxValue],
    ) -> Result<serde_json::Value> {
        let handler = self.views.get(name).ok_or_else(|| {
            FluxumError::query(
                codes::REDUCER_UNKNOWN_VIEW,
                format!("unknown view `{name}` (RED-030)"),
            )
        })?;
        let ctx = ViewContext {
            timestamp: Timestamp::now(),
            shard_id,
            tx: ReadOnlyTxHandle::new(snapshot),
        };
        handler(&ctx, args)
    }
}
