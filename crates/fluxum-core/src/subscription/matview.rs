//! Materialized-view engine (SPEC-022 RV-010..013): resolves the link-time
//! `#[fluxum::view(materialized)]` declarations against the schema,
//! maintains per-group aggregate accumulators and sorted top-N windows
//! **incrementally from commit delta rows** (never a re-scan), and renders
//! the changed view rows as `TableUpdate`s the subscription fan-out pushes.
//!
//! # View row shapes
//!
//! - Aggregate view (`COUNT`/`SUM`/`MIN`/`MAX`/`AVG`, optional `GROUP BY`):
//!   `[group, value]` — `group` is the grouping column's value (`'*'` for a
//!   global aggregate) and doubles as the view row's primary key; a change
//!   is `delete [group]` + `insert [group, value]`.
//! - Top-N view (`ORDER BY col LIMIT n`): `[rank, value, pk]` — `rank` is
//!   the 1-based window position (the view row's primary key), `value` the
//!   sort value, `pk` the base row's encoded primary key as bytes. Enter/
//!   leave/reorder deltas touch only the ranks that changed (RV-012):
//!   bounded by the window, never by the table.
//!
//! # Crash consistency (RV-013)
//!
//! State is memory-only and rebuilt from the base table on startup/recovery
//! ([`MatViewEngine::init`]); [`MatViewEngine::validate_against`] asserts
//! the incremental state equals a bit-identical fresh recompute (the test
//! harness contract).

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

use fluxum_protocol::{RowList, RowListBuilder, TableUpdate};

use crate::error::{FluxumError, Result};
use crate::index::btree;
use crate::reducer::{MaterializedViewDef, MvAggregate, registered_materialized_views};
use crate::schema::{FluxType, Schema, TableSchema};
use crate::store::committed::Snapshot;
use crate::store::row::{encode_pk_of_row, encode_row};
use crate::store::{Row, RowValue, TableId, TxDiff};

/// One resolved view: the def bound to schema ordinals.
struct ResolvedView {
    name: String,
    table: TableId,
    table_schema: &'static TableSchema,
    kind: ViewKind,
}

enum ViewKind {
    Aggregate {
        agg: MvAggregate,
        /// Ordinal of the aggregated column (`None` for COUNT).
        value: Option<u16>,
        /// Ordinal of the GROUP BY column (`None` = one global group).
        group: Option<u16>,
    },
    TopN {
        ordinal: u16,
        descending: bool,
        limit: usize,
    },
}

/// One group's running accumulators. All aggregate kinds share the struct —
/// the per-kind rendering picks what it needs; the constant overhead keeps
/// the maintenance code single-pathed.
#[derive(Debug, Default, Clone, PartialEq)]
struct GroupAcc {
    /// The group column's value (`Str("*")` for the global group).
    group_value: Option<RowValue>,
    count: i64,
    sum_int: i128,
    sum_float: f64,
    /// Multiset of aggregated values, memcomparable-keyed — what makes
    /// MIN/MAX deletable without a re-scan.
    values: BTreeMap<Vec<u8>, (RowValue, i64)>,
}

#[derive(Default)]
struct ViewState {
    /// Aggregate views: group key (memcomparable) → accumulators.
    groups: BTreeMap<Vec<u8>, GroupAcc>,
    /// Top-N views: the FULL ordered multiset `(sort key, pk)` — the window
    /// is its first `limit` entries; keeping the tail is what makes a
    /// leave/enter at the boundary O(log n) instead of a re-scan.
    ordered: BTreeMap<Vec<u8>, (RowValue, Vec<u8>)>,
}

impl ViewState {
    fn eq_state(&self, other: &Self) -> bool {
        self.groups == other.groups && self.ordered == other.ordered
    }
}

/// The engine: resolved views + their states, maintained on every commit.
/// Interior-mutable so the (widely shared) `&SubscriptionManager::on_commit`
/// signature stays untouched.
#[derive(Default)]
pub(crate) struct MatViewEngine {
    views: Vec<ResolvedView>,
    states: Mutex<Vec<ViewState>>,
}

impl MatViewEngine {
    /// Resolve every registered materialized view whose base table is in
    /// `schema`, validate its columns, and rebuild state from `snapshot`
    /// (RV-013: recovery = rebuild from the base table). Errors abort
    /// assembly with a descriptive message.
    pub(crate) fn init(schema: &Schema, snapshot: &Snapshot) -> Result<Self> {
        let mut views = Vec::new();
        for def in registered_materialized_views() {
            let Some(table_schema) = schema.table(def.table) else {
                continue; // registry is process-global; schemas may be subsets
            };
            views.push(resolve(def, table_schema)?);
        }
        let mut states: Vec<ViewState> = views.iter().map(|_| ViewState::default()).collect();
        for (view, state) in views.iter().zip(&mut states) {
            for row in snapshot.scan(view.table)? {
                apply_row(view, state, row, 1)?;
            }
        }
        Ok(Self {
            views,
            states: Mutex::new(states),
        })
    }

    /// The current view rows, encoded — the `InitialData` of a view
    /// subscription (RV-011).
    pub(crate) fn snapshot_rows(&self, name: &str) -> Result<Option<TableUpdate>> {
        let states = self
            .states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some((index, view)) = self.views.iter().enumerate().find(|(_, v)| v.name == name)
        else {
            return Ok(None);
        };
        let state = &states[index];
        let mut inserts = RowListBuilder::new();
        for values in render_all(view, state) {
            inserts.push_row(&encode_row(&values)?);
        }
        Ok(Some(TableUpdate {
            table_id: TableId::of(&view.name).as_u32(),
            table_name: view.name.clone(),
            query_id: 0,
            inserts: inserts.finish(),
            deletes: RowList::empty(),
        }))
    }

    /// Apply one commit's delta rows (RV-010: only the delta, only affected
    /// groups/ranks) and render the changed view rows per view (RV-011).
    pub(crate) fn on_commit(&self, diff: &TxDiff) -> Result<Vec<(String, TableUpdate)>> {
        let mut states = self
            .states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut out = Vec::new();
        for (view, state) in self.views.iter().zip(states.iter_mut()) {
            let Some(table_diff) = diff.tables.iter().find(|t| t.table_id == view.table) else {
                continue;
            };
            // Render-before: only what this commit can touch.
            let before = render_all(view, state);
            for (_, old) in &table_diff.deletes {
                apply_row(view, state, old, -1)?;
            }
            for row in &table_diff.inserts {
                apply_row(view, state, row, 1)?;
            }
            let after = render_all(view, state);
            if before == after {
                continue;
            }
            // Delta = rows keyed by their view PK (group / rank): a changed
            // key is delete+insert, a vanished key delete, a new key insert.
            let key_of = |values: &[RowValue]| -> Result<Vec<u8>> {
                encode_row(std::slice::from_ref(&values[0]))
            };
            let before_map: HashMap<Vec<u8>, &Vec<RowValue>> = before
                .iter()
                .map(|v| Ok((key_of(v)?, v)))
                .collect::<Result<_>>()?;
            let after_map: HashMap<Vec<u8>, &Vec<RowValue>> = after
                .iter()
                .map(|v| Ok((key_of(v)?, v)))
                .collect::<Result<_>>()?;
            let mut inserts = RowListBuilder::new();
            let mut deletes = RowListBuilder::new();
            let mut changed = false;
            for (key, values) in &after_map {
                if before_map.get(key).is_none_or(|old| *old != *values) {
                    inserts.push_row(&encode_row(values)?);
                    changed = true;
                    if before_map.contains_key(key) {
                        deletes.push_row(key);
                    }
                }
            }
            for key in before_map.keys() {
                if !after_map.contains_key(key) {
                    deletes.push_row(key);
                    changed = true;
                }
            }
            if changed {
                out.push((
                    view.name.clone(),
                    TableUpdate {
                        table_id: TableId::of(&view.name).as_u32(),
                        table_name: view.name.clone(),
                        query_id: 0,
                        inserts: inserts.finish(),
                        deletes: deletes.finish(),
                    },
                ));
            }
        }
        Ok(out)
    }

    /// RV-013: assert the incremental state equals a bit-identical fresh
    /// rebuild from `snapshot`. Test-harness contract; also the recovery
    /// validation seam.
    pub(crate) fn validate_against(&self, snapshot: &Snapshot) -> Result<()> {
        let fresh = Self::init_from_views(&self.views, snapshot)?;
        let states = self
            .states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for ((view, state), fresh_state) in self.views.iter().zip(states.iter()).zip(&fresh) {
            if !state.eq_state(fresh_state) {
                return Err(FluxumError::Storage(format!(
                    "materialized view `{}`: incremental state diverged from a fresh \
                     rebuild (RV-013)",
                    view.name
                )));
            }
        }
        Ok(())
    }

    fn init_from_views(views: &[ResolvedView], snapshot: &Snapshot) -> Result<Vec<ViewState>> {
        let mut states: Vec<ViewState> = views.iter().map(|_| ViewState::default()).collect();
        for (view, state) in views.iter().zip(&mut states) {
            for row in snapshot.scan(view.table)? {
                apply_row(view, state, row, 1)?;
            }
        }
        Ok(states)
    }
}

/// Resolve one def against its table schema.
fn resolve(def: &'static MaterializedViewDef, table: &'static TableSchema) -> Result<ResolvedView> {
    let fail = |detail: String| {
        FluxumError::Schema(format!(
            "materialized view `{}` over `{}`: {detail} (RV-010)",
            def.name, def.table
        ))
    };
    let ordinal_of = |name: &str| -> Result<u16> {
        table
            .columns
            .iter()
            .position(|c| c.name == name)
            .map(|i| u16::try_from(i).unwrap_or(u16::MAX))
            .ok_or_else(|| fail(format!("unknown column `{name}`")))
    };
    let kind = match (&def.aggregate, &def.top_n) {
        (Some(agg), None) => {
            let value = agg.column().map(&ordinal_of).transpose()?;
            if let (Some(ordinal), MvAggregate::Sum(_) | MvAggregate::Avg(_)) = (value, agg) {
                let ty = &table.columns[usize::from(ordinal)].ty;
                if !matches!(
                    ty,
                    FluxType::I8
                        | FluxType::I16
                        | FluxType::I32
                        | FluxType::I64
                        | FluxType::U8
                        | FluxType::U16
                        | FluxType::U32
                        | FluxType::U64
                        | FluxType::F32
                        | FluxType::F64
                ) {
                    return Err(fail(format!("SUM/AVG needs a numeric column, got {ty:?}")));
                }
            }
            let group = def.group_by.map(&ordinal_of).transpose()?;
            ViewKind::Aggregate {
                agg: *agg,
                value,
                group,
            }
        }
        (None, Some(top)) => {
            if top.limit == 0 {
                return Err(fail("top-N limit must be >= 1".into()));
            }
            ViewKind::TopN {
                ordinal: ordinal_of(top.column)?,
                descending: top.descending,
                limit: top.limit as usize,
            }
        }
        (Some(_), Some(_)) => {
            return Err(fail(
                "declare an aggregate OR a top-N window, not both".into(),
            ));
        }
        (None, None) => return Err(fail("declare an aggregate or a top-N window".into())),
    };
    Ok(ResolvedView {
        name: def.name.to_owned(),
        table: TableId::of(def.table),
        table_schema: table,
        kind,
    })
}

/// Apply one base row to a view's state with `sign` +1 (insert) / −1
/// (delete) — the whole incremental-maintenance kernel.
fn apply_row(view: &ResolvedView, state: &mut ViewState, row: &Row, sign: i64) -> Result<()> {
    match &view.kind {
        ViewKind::Aggregate { value, group, .. } => {
            let group_value = match group {
                Some(ordinal) => row
                    .value(*ordinal)
                    .cloned()
                    .unwrap_or(RowValue::Str("*".into())),
                None => RowValue::Str("*".into()),
            };
            let mut key = Vec::new();
            btree::encode_value(&group_value, &mut key);
            let acc = state.groups.entry(key.clone()).or_default();
            acc.group_value.get_or_insert(group_value);
            acc.count += sign;
            if let Some(ordinal) = value
                && let Some(v) = row.value(*ordinal)
            {
                match numeric_of(v) {
                    Some(Numeric::Int(n)) => acc.sum_int += i128::from(sign) * n,
                    Some(Numeric::Float(x)) => acc.sum_float += sign as f64 * x,
                    None => {}
                }
                let mut vkey = Vec::new();
                btree::encode_value(v, &mut vkey);
                let entry = acc.values.entry(vkey.clone()).or_insert((v.clone(), 0));
                entry.1 += sign;
                if entry.1 <= 0 {
                    acc.values.remove(&vkey);
                }
            }
            if acc.count <= 0 {
                state.groups.remove(&key);
            }
        }
        ViewKind::TopN {
            ordinal,
            descending,
            ..
        } => {
            let Some(v) = row.value(*ordinal) else {
                return Ok(());
            };
            let pk = encode_pk_of_row(view.table_schema, row.values())?;
            let mut key = Vec::new();
            btree::encode_value(v, &mut key);
            if *descending {
                // Invert so the BTreeMap's ascending order is best-first.
                for byte in &mut key {
                    *byte = !*byte;
                }
            }
            key.extend_from_slice(pk.as_bytes());
            if sign > 0 {
                state
                    .ordered
                    .insert(key, (v.clone(), pk.as_bytes().to_vec()));
            } else {
                state.ordered.remove(&key);
            }
        }
    }
    Ok(())
}

/// Render every current view row (aggregate: per group; top-N: the window).
fn render_all(view: &ResolvedView, state: &ViewState) -> Vec<Vec<RowValue>> {
    match &view.kind {
        ViewKind::Aggregate { agg, .. } => state
            .groups
            .values()
            .map(|acc| {
                let group = acc.group_value.clone().unwrap_or(RowValue::Str("*".into()));
                vec![group, aggregate_value(*agg, acc)]
            })
            .collect(),
        ViewKind::TopN { limit, .. } => state
            .ordered
            .values()
            .take(*limit)
            .enumerate()
            .map(|(rank, (value, pk))| {
                vec![
                    RowValue::U32(u32::try_from(rank + 1).unwrap_or(u32::MAX)),
                    value.clone(),
                    RowValue::Bytes(pk.clone()),
                ]
            })
            .collect(),
    }
}

/// The aggregate's current value for one group.
fn aggregate_value(agg: MvAggregate, acc: &GroupAcc) -> RowValue {
    #[allow(clippy::cast_precision_loss)] // AVG is a float by definition
    match agg {
        MvAggregate::Count => RowValue::I64(acc.count),
        MvAggregate::Sum(_) => {
            if acc.sum_float != 0.0 {
                RowValue::F64(acc.sum_float + acc.sum_int as f64)
            } else {
                RowValue::I64(i64::try_from(acc.sum_int).unwrap_or(i64::MAX))
            }
        }
        MvAggregate::Avg(_) => {
            let total = acc.sum_float + acc.sum_int as f64;
            RowValue::F64(if acc.count == 0 {
                0.0
            } else {
                total / acc.count as f64
            })
        }
        MvAggregate::Min(_) => acc
            .values
            .values()
            .next()
            .map_or(RowValue::I64(0), |(v, _)| v.clone()),
        MvAggregate::Max(_) => acc
            .values
            .values()
            .next_back()
            .map_or(RowValue::I64(0), |(v, _)| v.clone()),
    }
}

enum Numeric {
    Int(i128),
    Float(f64),
}

fn numeric_of(value: &RowValue) -> Option<Numeric> {
    Some(match value {
        RowValue::I8(n) => Numeric::Int(i128::from(*n)),
        RowValue::I16(n) => Numeric::Int(i128::from(*n)),
        RowValue::I32(n) => Numeric::Int(i128::from(*n)),
        RowValue::I64(n) => Numeric::Int(i128::from(*n)),
        RowValue::U8(n) => Numeric::Int(i128::from(*n)),
        RowValue::U16(n) => Numeric::Int(i128::from(*n)),
        RowValue::U32(n) => Numeric::Int(i128::from(*n)),
        RowValue::U64(n) => Numeric::Int(i128::from(*n)),
        RowValue::F32(x) => Numeric::Float(f64::from(*x)),
        RowValue::F64(x) => Numeric::Float(*x),
        _ => return None,
    })
}
