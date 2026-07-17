//! SQL subscription compiler (SPEC-005 §3–4, T4.1; FR-30, FR-35): compiles
//! the SUB-010/SUB-011 SQL subset into a [`CompiledPlan`] exactly once at
//! subscription time — commits evaluate the plan, never the SQL (SUB-020).
//!
//! # What a plan carries
//!
//! - a **table filter** (the single `FROM` table's [`TableId`]),
//! - a compiled **predicate closure** over stored [`Row`]s, with the raw
//!   equality conditions also exposed structurally
//!   ([`CompiledPlan::equalities`]) so the T4.2 `SubscriptionManager` can
//!   build the SUB-023 value-level pruning index without re-parsing,
//! - the **spatial constraint** (SUB-011), validated against the table's
//!   `#[spatial(...)]` declaration (SPX-022: no index, no spatial query),
//! - a **visibility slot** ([`CompiledPlan::rls`], filled by T4.3),
//! - `ORDER BY` / `LIMIT` for `InitialData` only (SUB-013),
//! - the [`QueryHash`] over the normalized text that drives cross-client
//!   plan deduplication (SUB-020).
//!
//! # Injection posture (T4.1 exit; T6.6 input)
//!
//! The grammar is closed and literal-typed: the lexer rejects every
//! character outside the subset (`;`, comment introducers, quoted
//! identifiers, extra operators), the parser rejects every SUB-012
//! construct by name, and literals are coerced against the schema's column
//! types — there is no string concatenation anywhere between the query text
//! and evaluation, so a query can only ever *fail to compile*, never change
//! meaning. The injection corpus in `tests/sql_injection_corpus.rs` pins
//! this for hundreds of hostile inputs.

mod lexer;
mod parse;

use std::cmp::Ordering;
use std::fmt;
use std::ops::Bound;

use fluxum_protocol::codes;

use crate::error::{FluxumError, Result};
use crate::index::{IndexId, Rect};
use crate::schema::{FluxType, IndexSchema, Schema, TableSchema, VisibilityRule};
use crate::store::{Row, RowValue, TableId};
use crate::types::Identity;

use lexer::unsupported;
use parse::{CmpOp, CondAst, Lit, QueryAst, SpatialAst};

/// Compiled predicate over a stored row (SUB-020).
pub type FilterFn = Box<dyn Fn(&Row) -> bool + Send + Sync>;

/// Row-level visibility filter (SUB-030; compiled in by T4.3).
pub type RlsFn = Box<dyn Fn(&Row, &Identity) -> bool + Send + Sync>;

/// Stable hash of the normalized query text (SUB-020): equal hash ⇒ the
/// same subscription query ⇒ one shared plan across clients. Derived via
/// the platform-stable xxHash64 kernel (HWA-042).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct QueryHash(pub u64);

impl fmt::Display for QueryHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// `ORDER BY` for `InitialData` only (SUB-013).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderSpec {
    /// Ordinal of the sort column.
    pub column: u16,
    /// `DESC` when true.
    pub descending: bool,
}

/// A spatial predicate (SUB-011), evaluated through the table's spatial
/// index by the subscription manager (SUB-022).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SpatialConstraint {
    /// `IN REGION (x, y, w, h)` — bounds inclusive (SPX-020).
    Region(Rect),
    /// `WITHIN RADIUS r OF (x, y)` — Euclidean, distance exactly `r`
    /// included (SPX-021).
    Radius {
        /// Center X.
        x: f64,
        /// Center Y.
        y: f64,
        /// Radius (finite, non-negative).
        r: f64,
    },
}

/// One compiled subscription query (SUB-020): commits evaluate this, the
/// SQL text is never touched again.
pub struct CompiledPlan {
    /// Per-connection handle assigned at `Subscribe` time by the
    /// subscription manager (T4.2); `0` until registered.
    pub query_id: u32,
    /// The tables this plan reads (the SUB-010 subset: exactly one).
    pub table_ids: Vec<TableId>,
    /// Compiled predicate; `None` = every row of the table matches.
    pub filter: Option<FilterFn>,
    /// Row-level visibility filter slot (SUB-030, compiled in by T4.3).
    pub rls: Option<RlsFn>,
    /// `InitialData`-only ordering (SUB-013).
    pub order_by: Option<OrderSpec>,
    /// `InitialData`-only row cap (SUB-013).
    pub limit: Option<u32>,
    /// Top-level equality conditions `(column ordinal, value)` — the
    /// structural seam the SUB-023 value-level pruning index is built from.
    pub equalities: Vec<(u16, RowValue)>,
    /// Spatial constraint (SUB-011), if any.
    pub spatial: Option<SpatialConstraint>,
    /// Hash of [`CompiledPlan::normalized`] (SUB-020 dedup key).
    pub query_hash: QueryHash,
    /// Whitespace- and keyword-case-normalized query text.
    pub normalized: String,
    /// SPEC-018 QP-001/050: how the snapshot evaluator reads candidate rows.
    /// Rule-based, deterministic for a given schema + normalized query, and
    /// transparent (QP-002): `IndexScan` and `FullScan` always yield the
    /// same row set — `residual` re-checks what the bounds don't guarantee.
    pub access: AccessPath,
    /// The conditions NOT covered by the index bounds (QP-010) — applied
    /// per row an `IndexScan` yields. `filter` stays the complete predicate
    /// (used by `FullScan` and by `TxUpdate` delta evaluation, QP-003).
    pub residual: Option<FilterFn>,
    /// Human-readable residual conditions (QP-051 explain surface).
    pub residual_desc: Vec<String>,
    /// QP-020: the index scan already yields rows in the query's `ORDER BY`
    /// order (DESC served by a reverse walk) — skip the in-RAM sort and
    /// allow the QP-021 index-ordered top-N early stop.
    pub ordered_by_index: bool,
    /// QP-040 `AFTER` keyset cursor slot — parsed and planned by the
    /// keyset-pagination task; carried here so the extension stays additive
    /// (QP-050). Always `None` until that task lands.
    pub cursor: Option<Keyset>,
}

/// How the snapshot evaluator reads a plan's candidate rows (QP-001).
#[derive(Debug, Clone, PartialEq)]
pub enum AccessPath {
    /// Iterate the primary row map and filter per row (today's behavior).
    FullScan,
    /// One or more bounded B-tree index scans (QP-010/011).
    IndexScan(IndexScanPlan),
}

/// The bounded-scan shape an `IndexScan` executes (QP-010/011/012): an
/// equality prefix (several probes under `IN` expansion), plus at most one
/// range on the next index column — the single-range shape
/// `Snapshot::index_scan` natively supports.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexScanPlan {
    /// The chosen index (STG-051 stable id).
    pub index_id: IndexId,
    /// The index's key column ordinals, declaration order.
    pub columns: Vec<u16>,
    /// Equality probes: each is one `index_scan` prefix. `IN` on a prefix
    /// column multiplies probes (QP-011), pre-sorted in memcomparable key
    /// order so the merged result streams in index order.
    pub probes: Vec<Vec<RowValue>>,
    /// Lower bound on the column after the equality prefix.
    pub lower: Bound<RowValue>,
    /// Upper bound on the column after the equality prefix.
    pub upper: Bound<RowValue>,
}

/// A keyset cursor (QP-040/041): the `(ORDER BY value, primary key)` of the
/// previous page's last row. Parsing/planning lands with the phase-4
/// keyset-pagination task.
#[derive(Debug, Clone, PartialEq)]
pub struct Keyset {
    /// The last row's `ORDER BY` column value.
    pub order_value: RowValue,
    /// The last row's primary-key value (the QP-041 tiebreak).
    pub pk_value: RowValue,
}

impl fmt::Debug for CompiledPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledPlan")
            .field("query_id", &self.query_id)
            .field("table_ids", &self.table_ids)
            .field("has_filter", &self.filter.is_some())
            .field("has_rls", &self.rls.is_some())
            .field("order_by", &self.order_by)
            .field("limit", &self.limit)
            .field("equalities", &self.equalities)
            .field("spatial", &self.spatial)
            .field("query_hash", &self.query_hash)
            .field("normalized", &self.normalized)
            .field("access", &self.access)
            .field("has_residual", &self.residual.is_some())
            .field("ordered_by_index", &self.ordered_by_index)
            .finish()
    }
}

impl CompiledPlan {
    /// Evaluate the compiled predicate against a stored row (`true` when
    /// the plan has no WHERE clause). The spatial constraint is NOT
    /// evaluated here — the subscription manager resolves it through the
    /// spatial index (SUB-022) so only candidate rows are touched.
    pub fn matches(&self, row: &Row) -> bool {
        self.filter.as_ref().is_none_or(|filter| filter(row))
    }
}

/// Compile `sql` against the assembled schema (SUB-020): tokenize, parse
/// the closed SUB-010/SUB-011 grammar, resolve the table and columns,
/// coerce every literal to its column's type, and build the predicate
/// closure. Every failure is a wire-ready 400.
pub fn compile(schema: &Schema, sql: &str) -> Result<CompiledPlan> {
    let tokens = lexer::tokenize(sql)?;
    let ast = parse::Parser::new(&tokens).parse_query()?;

    let table = schema.table(&ast.table).ok_or_else(|| {
        FluxumError::query(
            codes::SQL_UNKNOWN_TABLE,
            format!("unknown table `{}`", ast.table),
        )
    })?;
    let table_id = TableId::of(table.name);

    let mut conditions = Vec::with_capacity(ast.conditions.len());
    let mut equalities = Vec::new();
    for cond in &ast.conditions {
        let compiled = compile_condition(table, cond)?;
        if let CompiledCond::Eq(ordinal, value) = &compiled {
            equalities.push((*ordinal, value.clone()));
        }
        conditions.push(compiled);
    }

    let spatial = ast
        .spatial
        .map(|clause| compile_spatial(table, clause))
        .transpose()?;

    let order_by = ast
        .order_by
        .as_ref()
        .map(|(column, descending)| {
            let (ordinal, _) = resolve_column(table, column)?;
            Ok::<_, FluxumError>(OrderSpec {
                column: ordinal,
                descending: *descending,
            })
        })
        .transpose()?;

    let normalized = normalize(&ast);
    let query_hash = QueryHash(crate::simd::global().hash64(normalized.as_bytes(), 0));

    // SPEC-018 QP-001: rule-based access-path selection at compile time.
    // Spatial queries keep their dedicated index path (SUB-022) untouched.
    let (access, consumed, ordered_by_index) = if spatial.is_some() {
        (AccessPath::FullScan, Vec::new(), false)
    } else {
        plan_access(table, &conditions, order_by.as_ref())
    };
    // QP-010: the residual filter re-checks everything the index bounds do
    // not guarantee — the transparency invariant (QP-002) rests on it.
    let residual_conds: Vec<CompiledCond> = conditions
        .iter()
        .enumerate()
        .filter(|(i, _)| !consumed.contains(i))
        .map(|(_, cond)| cond.clone())
        .collect();
    let residual_desc: Vec<String> = residual_conds
        .iter()
        .map(|cond| cond.describe(table))
        .collect();
    let residual: Option<FilterFn> = if residual_conds.is_empty() {
        None
    } else {
        Some(Box::new(move |row: &Row| {
            residual_conds.iter().all(|cond| cond.matches(row))
        }))
    };

    let filter: Option<FilterFn> = if conditions.is_empty() {
        None
    } else {
        Some(Box::new(move |row: &Row| {
            conditions.iter().all(|cond| cond.matches(row))
        }))
    };

    // Row-level visibility (SUB-030): compile the table's `#[visibility]`
    // rule into the caller-parameterized `rls` slot. The closure takes the
    // viewer identity as a parameter, so one plan is shared across every
    // per-identity subscription bucket (the subscription manager folds the
    // identity into the dedup key and carries the viewer per bucket).
    let rls = compile_visibility(table);

    // QP-041: an explicit ORDER BY tiebreak must be the primary key, same
    // direction — it is implicit otherwise, so the total order is fixed.
    if let Some((tiebreak_col, tiebreak_desc)) = &ast.order_tiebreak {
        let &[pk_ordinal] = table.primary_key else {
            return Err(unsupported(
                "an ORDER BY tiebreak requires a single-column primary key (QP-041)",
            ));
        };
        let (ordinal, _) = resolve_column(table, tiebreak_col)?;
        if ordinal != pk_ordinal {
            return Err(unsupported(format!(
                "the ORDER BY tiebreak must be the primary key `{}` (QP-041)",
                table.columns[usize::from(pk_ordinal)].name
            )));
        }
        if order_by.map(|o| o.descending) != Some(*tiebreak_desc) {
            return Err(unsupported(
                "the ORDER BY tiebreak direction must match the first term (QP-041)",
            ));
        }
    }

    // QP-040: compile the AFTER cursor — typed values, index-served order
    // required, and the seek folded into the scan's bound on the order
    // column (the `value = c AND pk <= k` residue is skipped in-scan).
    let mut access = access;
    let mut cursor = None;
    if let Some((order_lit, pk_lit)) = &ast.after {
        let Some(order) = order_by else {
            return Err(unsupported("AFTER requires ORDER BY (QP-040)"));
        };
        if !ordered_by_index {
            return Err(unsupported(
                "AFTER requires ORDER BY on an indexed column (QP-040)",
            ));
        }
        let &[pk_ordinal] = table.primary_key else {
            return Err(unsupported(
                "AFTER requires a single-column primary key (QP-041)",
            ));
        };
        let order_column = &table.columns[usize::from(order.column)];
        let order_value = coerce(table, order_column.name, order_lit, &order_column.ty)?;
        let pk_column = &table.columns[usize::from(pk_ordinal)];
        let pk_value = coerce(table, pk_column.name, pk_lit, &pk_column.ty)?;
        if let AccessPath::IndexScan(scan) = &mut access {
            let prefix_len = scan.probes.first().map_or(0, Vec::len);
            if scan.columns.get(prefix_len).copied() == Some(order.column) {
                if order.descending {
                    scan.upper =
                        tighter_upper(scan.upper.clone(), Bound::Included(order_value.clone()));
                } else {
                    scan.lower =
                        tighter_lower(scan.lower.clone(), Bound::Included(order_value.clone()));
                }
            }
        }
        cursor = Some(Keyset {
            order_value,
            pk_value,
        });
    }

    Ok(CompiledPlan {
        query_id: 0,
        table_ids: vec![table_id],
        filter,
        rls,
        order_by,
        limit: ast.limit,
        equalities,
        spatial,
        query_hash,
        normalized,
        access,
        residual,
        residual_desc,
        ordered_by_index,
        cursor,
    })
}

/// The `IN` probe-expansion cap (QP-011): `col IN (v1..vk)` on prefix
/// columns expands to at most this many bounded scans; beyond it, the `IN`
/// stays a residual filter over a broader scan.
pub const INDEX_IN_EXPANSION_MAX: usize = 128;

/// SPEC-018 QP-001: pick the access path for `conditions` + `order_by`.
/// For each declared B-tree index: longest equality prefix (`=`, with `IN`
/// multiplying probes up to [`INDEX_IN_EXPANSION_MAX`]), then at most one
/// range (`BETWEEN`) on the next key column (QP-012). Score by
/// `(prefix length, range-binds-next, order-served)`; ties break by index
/// declaration order, so the plan is deterministic for a schema + query
/// (SUB-020 dedup stability). Returns `(path, consumed condition indexes,
/// ordered_by_index)`.
fn plan_access(
    table: &TableSchema,
    conditions: &[CompiledCond],
    order_by: Option<&OrderSpec>,
) -> (AccessPath, Vec<usize>, bool) {
    /// One scored candidate: `(score, plan, consumed conds, order served)`.
    type Candidate = ((usize, bool, bool), IndexScanPlan, Vec<usize>, bool);
    let mut best: Option<Candidate> = None;
    for index in table.indexes {
        let IndexSchema::BTree { columns } = index else {
            continue;
        };
        let mut consumed: Vec<usize> = Vec::new();
        let mut probes: Vec<Vec<RowValue>> = vec![Vec::new()];
        let mut prefix_len = 0usize;
        for &col in *columns {
            let eq = conditions.iter().enumerate().find(|(i, cond)| {
                !consumed.contains(i) && matches!(cond, CompiledCond::Eq(ord, _) if *ord == col)
            });
            if let Some((i, CompiledCond::Eq(_, value))) = eq {
                for probe in &mut probes {
                    probe.push(value.clone());
                }
                consumed.push(i);
                prefix_len += 1;
                continue;
            }
            let in_cond = conditions.iter().enumerate().find(|(i, cond)| {
                !consumed.contains(i) && matches!(cond, CompiledCond::In(ord, _) if *ord == col)
            });
            if let Some((i, CompiledCond::In(_, values))) = in_cond {
                // QP-011: expand IN into per-value probes, capped; past the
                // cap the IN (and everything after it) stays residual.
                if probes.len().saturating_mul(values.len().max(1)) > INDEX_IN_EXPANSION_MAX
                    || values.is_empty()
                {
                    break;
                }
                probes = probes
                    .into_iter()
                    .flat_map(|probe| {
                        values.iter().map(move |value| {
                            let mut next = probe.clone();
                            next.push(value.clone());
                            next
                        })
                    })
                    .collect();
                consumed.push(i);
                prefix_len += 1;
                continue;
            }
            break;
        }
        // QP-010/012/030: one bounded interval on the column right after
        // the equality prefix — every BETWEEN and comparison condition on
        // that column folds (intersects) into it, so `price >= a AND
        // price <= b` pushes down exactly like `price BETWEEN a AND b`.
        let mut lower: Bound<RowValue> = Bound::Unbounded;
        let mut upper: Bound<RowValue> = Bound::Unbounded;
        let mut has_range = false;
        if let Some(&range_col) = columns.get(prefix_len) {
            for (i, cond) in conditions.iter().enumerate() {
                if consumed.contains(&i) {
                    continue;
                }
                match cond {
                    CompiledCond::Between(ord, low, high) if *ord == range_col => {
                        lower = tighter_lower(lower, Bound::Included(low.clone()));
                        upper = tighter_upper(upper, Bound::Included(high.clone()));
                    }
                    CompiledCond::Cmp(ord, op, value) if *ord == range_col => {
                        match op {
                            CmpOp::Gt => {
                                lower = tighter_lower(lower, Bound::Excluded(value.clone()));
                            }
                            CmpOp::Ge => {
                                lower = tighter_lower(lower, Bound::Included(value.clone()));
                            }
                            CmpOp::Lt => {
                                upper = tighter_upper(upper, Bound::Excluded(value.clone()));
                            }
                            CmpOp::Le => {
                                upper = tighter_upper(upper, Bound::Included(value.clone()));
                            }
                        }
                    }
                    _ => continue,
                }
                consumed.push(i);
                has_range = true;
            }
        }
        // QP-020: order served when the single probe's next index column IS
        // the ORDER BY column (DESC via reverse walk), or the ORDER BY
        // column sits inside the equality prefix (all values equal — any
        // order serves it).
        let ordered = order_by.is_some_and(|order| {
            probes.len() == 1
                && (columns.get(prefix_len).copied() == Some(order.column)
                    || columns[..prefix_len].contains(&order.column))
        });
        if prefix_len == 0 && !has_range && !ordered {
            continue; // the index contributes nothing for this query
        }
        let score = (prefix_len, has_range, ordered);
        if best.as_ref().is_none_or(|(s, ..)| score > *s) {
            let names: Vec<&str> = columns
                .iter()
                .map(|&ord| table.columns[usize::from(ord)].name)
                .collect();
            let mut plan = IndexScanPlan {
                index_id: IndexId::of(table.name, &names),
                columns: columns.to_vec(),
                probes,
                lower,
                upper,
            };
            // QP-011: probes stream in memcomparable key order.
            plan.probes.sort_by_cached_key(|probe| {
                let mut key = Vec::new();
                for value in probe {
                    crate::index::btree::encode_value(value, &mut key);
                }
                key
            });
            best = Some((score, plan, consumed, ordered));
        }
    }
    match best {
        Some((_, plan, consumed, ordered)) => (AccessPath::IndexScan(plan), consumed, ordered),
        None => (AccessPath::FullScan, Vec::new(), false),
    }
}

/// The tighter of two lower bounds (the greater value; on equal values
/// `Excluded` is tighter). Mixed-variant values keep `a` (cannot happen for
/// same-column bounds).
fn tighter_lower(a: Bound<RowValue>, b: Bound<RowValue>) -> Bound<RowValue> {
    match (&a, &b) {
        (Bound::Unbounded, _) => b,
        (_, Bound::Unbounded) => a,
        (
            Bound::Included(x) | Bound::Excluded(x),
            Bound::Included(y) | Bound::Excluded(y),
        ) => match cmp_values(x, y) {
            Some(Ordering::Less) => b,
            Some(Ordering::Greater) | None => a,
            Some(Ordering::Equal) => {
                if matches!(a, Bound::Excluded(_)) { a } else { b }
            }
        },
    }
}

/// The tighter of two upper bounds (the smaller value; on equal values
/// `Excluded` is tighter).
fn tighter_upper(a: Bound<RowValue>, b: Bound<RowValue>) -> Bound<RowValue> {
    match (&a, &b) {
        (Bound::Unbounded, _) => b,
        (_, Bound::Unbounded) => a,
        (
            Bound::Included(x) | Bound::Excluded(x),
            Bound::Included(y) | Bound::Excluded(y),
        ) => match cmp_values(x, y) {
            Some(Ordering::Greater) => b,
            Some(Ordering::Less) | None => a,
            Some(Ordering::Equal) => {
                if matches!(a, Bound::Excluded(_)) { a } else { b }
            }
        },
    }
}

/// QP-051: the `EXPLAIN` surface — compile `sql` and describe the chosen
/// access path (index, probes, bounds, residual, order servicing) without
/// executing it. Served by the HTTP admin `POST /query/explain`.
pub fn explain(schema: &Schema, sql: &str) -> Result<serde_json::Value> {
    use serde_json::json;
    let plan = compile(schema, sql)?;
    let table = schema
        .tables()
        .find(|t| TableId::of(t.name) == plan.table_ids[0])
        .ok_or_else(|| FluxumError::Storage("explain: table vanished".into()))?;
    let bound = |b: &Bound<RowValue>, name: &str| match b {
        Bound::Unbounded => json!(null),
        Bound::Included(v) => json!(format!("{name} {v:?} (inclusive)")),
        Bound::Excluded(v) => json!(format!("{name} {v:?} (exclusive)")),
    };
    let access = match &plan.access {
        AccessPath::FullScan => json!({ "kind": "full_scan" }),
        AccessPath::IndexScan(scan) => {
            let columns: Vec<&str> = scan
                .columns
                .iter()
                .map(|&ord| table.columns[usize::from(ord)].name)
                .collect();
            json!({
                "kind": "index_scan",
                "index": columns,
                "probes": scan.probes.len(),
                "equality_prefix_len": scan.probes.first().map_or(0, Vec::len),
                "lower": bound(&scan.lower, ">="),
                "upper": bound(&scan.upper, "<="),
            })
        }
    };
    Ok(json!({
        "table": table.name,
        "normalized": plan.normalized,
        "access": access,
        "residual": plan.residual_desc,
        "ordered_by_index": plan.ordered_by_index,
        "order_by": plan.order_by.map(|o| json!({
            "column": table.columns[usize::from(o.column)].name,
            "descending": o.descending,
        })),
        "limit": plan.limit,
        "cursor": plan.cursor.as_ref().map(|c| json!({
            "order_value": format!("{:?}", c.order_value),
            "pk_value": format!("{:?}", c.pk_value),
        })),
    }))
}

/// Compile a table's [`VisibilityRule`] into the [`RlsFn`] applied per
/// subscriber (SUB-030). `None` means "no row-level filter" — the plan is
/// not caller-parameterized. Only `owner_only` is enforced in T4.3;
/// `shard_local` (needs the shard context of phase 5) and `custom` (SUB-032,
/// P2) are documented gaps that currently impose no filter.
fn compile_visibility(table: &TableSchema) -> Option<RlsFn> {
    match table.visibility {
        VisibilityRule::OwnerOnly { owner } => {
            Some(Box::new(move |row: &Row, viewer: &Identity| {
                row.value(owner) == Some(&RowValue::Identity(*viewer))
            }))
        }
        VisibilityRule::PublicAll | VisibilityRule::ShardLocal | VisibilityRule::Custom(_) => None,
    }
}

/// One compiled WHERE condition with literals already coerced to the
/// column's type — evaluation is a typed value comparison, never a cast.
#[derive(Clone)]
enum CompiledCond {
    Eq(u16, RowValue),
    In(u16, Vec<RowValue>),
    Between(u16, RowValue, RowValue),
    /// A QP-030 comparison (`<`, `<=`, `>`, `>=`).
    Cmp(u16, CmpOp, RowValue),
}

impl CompiledCond {
    /// Human-readable rendering for the QP-051 explain surface.
    fn describe(&self, table: &TableSchema) -> String {
        let name = |ord: u16| table.columns[usize::from(ord)].name;
        match self {
            Self::Eq(ord, value) => format!("{} = {value:?}", name(*ord)),
            Self::In(ord, values) => format!("{} IN ({} values)", name(*ord), values.len()),
            Self::Between(ord, low, high) => {
                format!("{} BETWEEN {low:?} AND {high:?}", name(*ord))
            }
            Self::Cmp(ord, op, value) => format!("{} {op} {value:?}", name(*ord)),
        }
    }

    fn matches(&self, row: &Row) -> bool {
        match self {
            Self::Eq(ordinal, value) => row.value(*ordinal) == Some(value),
            Self::In(ordinal, values) => row.value(*ordinal).is_some_and(|v| values.contains(v)),
            Self::Between(ordinal, low, high) => row.value(*ordinal).is_some_and(|v| {
                matches!(
                    cmp_values(v, low),
                    Some(Ordering::Greater | Ordering::Equal)
                ) && matches!(cmp_values(v, high), Some(Ordering::Less | Ordering::Equal))
            }),
            Self::Cmp(ordinal, op, bound) => row.value(*ordinal).is_some_and(|v| {
                let Some(ord) = cmp_values(v, bound) else {
                    return false;
                };
                match op {
                    CmpOp::Lt => ord == Ordering::Less,
                    CmpOp::Le => ord != Ordering::Greater,
                    CmpOp::Gt => ord == Ordering::Greater,
                    CmpOp::Ge => ord != Ordering::Less,
                }
            }),
        }
    }
}

fn compile_condition(table: &TableSchema, cond: &CondAst) -> Result<CompiledCond> {
    match cond {
        CondAst::Eq(column, lit) => {
            let (ordinal, ty) = resolve_column(table, column)?;
            Ok(CompiledCond::Eq(ordinal, coerce(table, column, lit, ty)?))
        }
        CondAst::In(column, lits) => {
            let (ordinal, ty) = resolve_column(table, column)?;
            let values = lits
                .iter()
                .map(|lit| coerce(table, column, lit, ty))
                .collect::<Result<Vec<_>>>()?;
            Ok(CompiledCond::In(ordinal, values))
        }
        CondAst::Between(column, low, high) => {
            let (ordinal, ty) = resolve_column(table, column)?;
            if matches!(ty, FluxType::Option(_) | FluxType::List(_) | FluxType::Bool) {
                return Err(unsupported(format!(
                    "BETWEEN is not defined over column `{column}` of type {ty:?}"
                )));
            }
            Ok(CompiledCond::Between(
                ordinal,
                coerce(table, column, low, ty)?,
                coerce(table, column, high, ty)?,
            ))
        }
        // SPEC-018 QP-030/032: comparison operators, same type discipline
        // as BETWEEN (schema-typed coercion; no order over Bool/Option/List).
        CondAst::Cmp(column, op, lit) => {
            let (ordinal, ty) = resolve_column(table, column)?;
            if matches!(ty, FluxType::Option(_) | FluxType::List(_) | FluxType::Bool) {
                return Err(unsupported(format!(
                    "`{op}` is not defined over column `{column}` of type {ty:?} (QP-032)"
                )));
            }
            Ok(CompiledCond::Cmp(
                ordinal,
                *op,
                coerce(table, column, lit, ty)?,
            ))
        }
    }
}

fn compile_spatial(table: &TableSchema, clause: SpatialAst) -> Result<SpatialConstraint> {
    let has_spatial = table
        .indexes
        .iter()
        .any(|index| matches!(index, IndexSchema::Spatial { .. }));
    if !has_spatial {
        return Err(FluxumError::query(
            codes::SQL_NO_SPATIAL_INDEX,
            format!("table '{}' has no spatial index (SPX-022)", table.name),
        ));
    }
    match clause {
        SpatialAst::Region { x, y, w, h } => {
            for value in [x, y, w, h] {
                if !value.is_finite() {
                    return Err(unsupported("non-finite REGION coordinates"));
                }
            }
            Ok(SpatialConstraint::Region(Rect::new(x, y, w, h)))
        }
        SpatialAst::Radius { r, x, y } => {
            if !(r.is_finite() && x.is_finite() && y.is_finite()) {
                return Err(unsupported("non-finite RADIUS parameters"));
            }
            if r < 0.0 {
                return Err(unsupported("negative radius"));
            }
            Ok(SpatialConstraint::Radius { x, y, r })
        }
    }
}

fn resolve_column<'s>(table: &'s TableSchema, column: &str) -> Result<(u16, &'s FluxType)> {
    table
        .columns
        .iter()
        .position(|c| c.name == column)
        .map(|position| {
            #[allow(clippy::cast_possible_truncation)] // DM-001 caps columns at u16
            (position as u16, &table.columns[position].ty)
        })
        .ok_or_else(|| {
            FluxumError::query(
                codes::SQL_UNKNOWN_COLUMN,
                format!("unknown column `{column}` on table `{}`", table.name),
            )
        })
}

/// Coerce a parsed literal to a column's [`FluxType`] (range-checked; no
/// cross-kind coercion beyond int→float widening).
fn coerce(table: &TableSchema, column: &str, lit: &Lit, ty: &FluxType) -> Result<RowValue> {
    let mismatch = || {
        FluxumError::query(
            codes::SQL_TYPE_MISMATCH,
            format!(
                "table `{}`, column `{column}`: literal {lit} does not inhabit the column \
                 type {ty:?}",
                table.name
            ),
        )
    };
    let int = |n: i64| -> Result<RowValue> {
        let value = match ty {
            FluxType::I8 => i8::try_from(n).ok().map(RowValue::I8),
            FluxType::I16 => i16::try_from(n).ok().map(RowValue::I16),
            FluxType::I32 => i32::try_from(n).ok().map(RowValue::I32),
            FluxType::I64 => Some(RowValue::I64(n)),
            FluxType::U8 => u8::try_from(n).ok().map(RowValue::U8),
            FluxType::U16 => u16::try_from(n).ok().map(RowValue::U16),
            FluxType::U32 => u32::try_from(n).ok().map(RowValue::U32),
            FluxType::U64 => u64::try_from(n).ok().map(RowValue::U64),
            #[allow(clippy::cast_precision_loss)] // SQL int→float widening
            FluxType::F32 => Some(RowValue::F32(n as f32)),
            #[allow(clippy::cast_precision_loss)]
            FluxType::F64 => Some(RowValue::F64(n as f64)),
            FluxType::EntityId => u64::try_from(n)
                .ok()
                .map(|v| RowValue::EntityId(crate::types::EntityId::new(v))),
            FluxType::Timestamp => {
                Some(RowValue::Timestamp(crate::types::Timestamp::from_micros(n)))
            }
            _ => None,
        };
        value.ok_or_else(mismatch)
    };
    match (lit, ty) {
        (lit, FluxType::Option(inner)) => {
            let inner_value = coerce(table, column, lit, inner)?;
            Ok(RowValue::Optional(Some(Box::new(inner_value))))
        }
        (Lit::Int(n), _) => int(*n),
        #[allow(clippy::cast_possible_truncation)] // f64→f32 rounds, by contract
        (Lit::Float(x), FluxType::F32) => Ok(RowValue::F32(*x as f32)),
        (Lit::Float(x), FluxType::F64) => Ok(RowValue::F64(*x)),
        (Lit::Str(s), FluxType::Str) => Ok(RowValue::Str(s.clone())),
        (Lit::Bool(b), FluxType::Bool) => Ok(RowValue::Bool(*b)),
        _ => Err(mismatch()),
    }
}

/// Total order between two same-variant [`RowValue`]s; `None` across
/// variants or for NaN floats. Shared with the subscription manager's
/// SUB-013 `ORDER BY` (which is why it is `pub(crate)`).
pub(crate) fn cmp_row_values(a: &RowValue, b: &RowValue) -> Option<Ordering> {
    cmp_values(a, b)
}

/// Total order between two same-variant values (BETWEEN evaluation);
/// `None` across variants or for NaN floats.
fn cmp_values(a: &RowValue, b: &RowValue) -> Option<Ordering> {
    match (a, b) {
        (RowValue::I8(x), RowValue::I8(y)) => Some(x.cmp(y)),
        (RowValue::I16(x), RowValue::I16(y)) => Some(x.cmp(y)),
        (RowValue::I32(x), RowValue::I32(y)) => Some(x.cmp(y)),
        (RowValue::I64(x), RowValue::I64(y)) => Some(x.cmp(y)),
        (RowValue::U8(x), RowValue::U8(y)) => Some(x.cmp(y)),
        (RowValue::U16(x), RowValue::U16(y)) => Some(x.cmp(y)),
        (RowValue::U32(x), RowValue::U32(y)) => Some(x.cmp(y)),
        (RowValue::U64(x), RowValue::U64(y)) => Some(x.cmp(y)),
        (RowValue::F32(x), RowValue::F32(y)) => x.partial_cmp(y),
        (RowValue::F64(x), RowValue::F64(y)) => x.partial_cmp(y),
        (RowValue::Str(x), RowValue::Str(y)) => Some(x.cmp(y)),
        (RowValue::EntityId(x), RowValue::EntityId(y)) => Some(x.as_u64().cmp(&y.as_u64())),
        (RowValue::Timestamp(x), RowValue::Timestamp(y)) => Some(x.as_micros().cmp(&y.as_micros())),
        _ => None,
    }
}

/// Canonical text of a parsed query (SUB-020): keywords uppercase, exactly
/// one space between tokens, literals re-rendered canonically. Identifier
/// case is preserved (table/column names are case-sensitive declarations).
fn normalize(ast: &QueryAst) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(64);
    let _ = write!(out, "SELECT * FROM {}", ast.table);
    for (index, cond) in ast.conditions.iter().enumerate() {
        out.push_str(if index == 0 { " WHERE " } else { " AND " });
        match cond {
            CondAst::Eq(column, lit) => {
                let _ = write!(out, "{column} = {lit}");
            }
            CondAst::In(column, lits) => {
                let _ = write!(out, "{column} IN (");
                for (i, lit) in lits.iter().enumerate() {
                    if i > 0 {
                        out.push_str(", ");
                    }
                    let _ = write!(out, "{lit}");
                }
                out.push(')');
            }
            CondAst::Between(column, low, high) => {
                let _ = write!(out, "{column} BETWEEN {low} AND {high}");
            }
            CondAst::Cmp(column, op, lit) => {
                let _ = write!(out, "{column} {op} {lit}");
            }
        }
    }
    match ast.spatial {
        Some(SpatialAst::Region { x, y, w, h }) => {
            let _ = write!(out, " IN REGION ({x}, {y}, {w}, {h})");
        }
        Some(SpatialAst::Radius { r, x, y }) => {
            let _ = write!(out, " WITHIN RADIUS {r} OF ({x}, {y})");
        }
        None => {}
    }
    if let Some((column, descending)) = &ast.order_by {
        let _ = write!(
            out,
            " ORDER BY {column} {}",
            if *descending { "DESC" } else { "ASC" }
        );
        // QP-041: the PK tiebreak is implicit; an explicit one renders so
        // equal queries normalize identically only when truly equal.
        if let Some((tiebreak, tb_desc)) = &ast.order_tiebreak {
            let _ = write!(
                out,
                ", {tiebreak} {}",
                if *tb_desc { "DESC" } else { "ASC" }
            );
        }
    }
    if let Some(limit) = ast.limit {
        let _ = write!(out, " LIMIT {limit}");
    }
    if let Some((order_value, pk_value)) = &ast.after {
        let _ = write!(out, " AFTER ({order_value}, {pk_value})");
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::schema::{ColumnSchema, SpatialKind, TableAccess};

    static RICH_COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "a_i8",
            ty: FluxType::I8,
        },
        ColumnSchema {
            name: "b_i16",
            ty: FluxType::I16,
        },
        ColumnSchema {
            name: "c_u8",
            ty: FluxType::U8,
        },
        ColumnSchema {
            name: "d_u16",
            ty: FluxType::U16,
        },
        ColumnSchema {
            name: "e_f32",
            ty: FluxType::F32,
        },
        ColumnSchema {
            name: "f_f64",
            ty: FluxType::F64,
        },
        ColumnSchema {
            name: "g_entity",
            ty: FluxType::EntityId,
        },
        ColumnSchema {
            name: "h_bool",
            ty: FluxType::Bool,
        },
    ];

    static RICH: TableSchema = TableSchema {
        name: "Rich",
        columns: RICH_COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes: &[IndexSchema::Spatial {
            kind: SpatialKind::QuadTree,
            columns: &[5, 5],
        }],
        visibility: VisibilityRule::PublicAll,
    };

    fn coerce_ok(column: &str, lit: &Lit) -> RowValue {
        let (_, ty) = resolve_column(&RICH, column).unwrap();
        coerce(&RICH, column, lit, ty).unwrap()
    }

    fn coerce_err(column: &str, lit: &Lit) -> String {
        let (_, ty) = resolve_column(&RICH, column).unwrap();
        coerce(&RICH, column, lit, ty).unwrap_err().to_string()
    }

    /// SUB-010 literal coercion: int literals inhabit every numeric column
    /// kind (range-checked), widen to floats, and reject non-numeric columns.
    #[test]
    fn int_literals_coerce_to_every_numeric_column_kind() {
        assert_eq!(coerce_ok("a_i8", &Lit::Int(-5)), RowValue::I8(-5));
        assert_eq!(coerce_ok("b_i16", &Lit::Int(-300)), RowValue::I16(-300));
        assert_eq!(coerce_ok("c_u8", &Lit::Int(200)), RowValue::U8(200));
        assert_eq!(coerce_ok("d_u16", &Lit::Int(60_000)), RowValue::U16(60_000));
        assert_eq!(coerce_ok("e_f32", &Lit::Int(2)), RowValue::F32(2.0));
        assert_eq!(coerce_ok("f_f64", &Lit::Int(3)), RowValue::F64(3.0));
        assert_eq!(
            coerce_ok("g_entity", &Lit::Int(9)),
            RowValue::EntityId(crate::types::EntityId::new(9))
        );
        // Range breach and kind mismatch are both wire-ready 400s.
        assert!(coerce_err("a_i8", &Lit::Int(200)).contains("does not inhabit"));
        assert!(coerce_err("g_entity", &Lit::Int(-1)).contains("does not inhabit"));
        assert!(coerce_err("h_bool", &Lit::Int(1)).contains("does not inhabit"));
    }

    #[test]
    fn float_literals_coerce_only_to_float_columns() {
        assert_eq!(coerce_ok("e_f32", &Lit::Float(0.25)), RowValue::F32(0.25));
        assert_eq!(coerce_ok("f_f64", &Lit::Float(1.5)), RowValue::F64(1.5));
        assert!(coerce_err("a_i8", &Lit::Float(1.5)).contains("does not inhabit"));
    }

    /// cmp_row_values: total within a variant, `None` across variants —
    /// the BETWEEN / ORDER BY comparison contract.
    #[test]
    fn cmp_row_values_orders_within_a_variant_only() {
        use std::cmp::Ordering::Less;
        let cases: &[(RowValue, RowValue)] = &[
            (RowValue::I8(-1), RowValue::I8(1)),
            (RowValue::I16(-1), RowValue::I16(1)),
            (RowValue::I32(-1), RowValue::I32(1)),
            (RowValue::I64(-1), RowValue::I64(1)),
            (RowValue::U8(1), RowValue::U8(2)),
            (RowValue::U16(1), RowValue::U16(2)),
            (RowValue::U32(1), RowValue::U32(2)),
            (RowValue::U64(1), RowValue::U64(2)),
            (RowValue::F32(0.5), RowValue::F32(1.5)),
            (RowValue::F64(0.5), RowValue::F64(1.5)),
            (RowValue::Str("a".into()), RowValue::Str("b".into())),
            (
                RowValue::EntityId(crate::types::EntityId::new(1)),
                RowValue::EntityId(crate::types::EntityId::new(2)),
            ),
            (
                RowValue::Timestamp(crate::types::Timestamp::from_micros(1)),
                RowValue::Timestamp(crate::types::Timestamp::from_micros(2)),
            ),
        ];
        for (lo, hi) in cases {
            assert_eq!(cmp_row_values(lo, hi), Some(Less), "{lo:?} < {hi:?}");
        }
        // Cross-variant and non-orderable values have no order.
        assert_eq!(cmp_row_values(&RowValue::I8(1), &RowValue::I16(1)), None);
        assert_eq!(
            cmp_row_values(&RowValue::Bool(true), &RowValue::Bool(false)),
            None
        );
        // NaN floats have no order either.
        assert_eq!(
            cmp_row_values(&RowValue::F64(f64::NAN), &RowValue::F64(1.0)),
            None
        );
    }

    /// SPX-020/021: non-finite spatial parameters are rejected (the lexer
    /// already refuses non-finite literals; this is the compiler's own
    /// backstop over the parsed AST).
    #[test]
    fn compile_spatial_rejects_non_finite_parameters() {
        let err = compile_spatial(
            &RICH,
            SpatialAst::Region {
                x: f64::NAN,
                y: 0.0,
                w: 1.0,
                h: 1.0,
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("non-finite REGION"), "{err}");

        let err = compile_spatial(
            &RICH,
            SpatialAst::Radius {
                r: f64::INFINITY,
                x: 0.0,
                y: 0.0,
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("non-finite RADIUS"), "{err}");
    }

    #[test]
    fn query_hash_and_plan_debug_render() {
        assert_eq!(QueryHash(0xAB).to_string(), "00000000000000ab");
        let plan = CompiledPlan {
            query_id: 1,
            table_ids: vec![TableId::of("Rich")],
            filter: None,
            rls: None,
            order_by: None,
            limit: Some(5),
            equalities: vec![],
            spatial: None,
            query_hash: QueryHash(7),
            normalized: "SELECT * FROM Rich LIMIT 5".to_owned(),
            access: AccessPath::FullScan,
            residual: None,
            residual_desc: vec![],
            ordered_by_index: false,
            cursor: None,
        };
        let debug = format!("{plan:?}");
        assert!(debug.contains("has_filter"), "{debug}");
        assert!(debug.contains("SELECT * FROM Rich LIMIT 5"), "{debug}");
    }
}
