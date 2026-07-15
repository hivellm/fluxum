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

use fluxum_protocol::codes;

use crate::error::{FluxumError, Result};
use crate::index::Rect;
use crate::schema::{FluxType, IndexSchema, Schema, TableSchema};
use crate::store::{Row, RowValue, TableId};
use crate::types::Identity;

use lexer::unsupported;
use parse::{CondAst, Lit, QueryAst, SpatialAst};

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
        FluxumError::query(codes::MALFORMED, format!("unknown table `{}`", ast.table))
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

    let filter: Option<FilterFn> = if conditions.is_empty() {
        None
    } else {
        Some(Box::new(move |row: &Row| {
            conditions.iter().all(|cond| cond.matches(row))
        }))
    };

    Ok(CompiledPlan {
        query_id: 0,
        table_ids: vec![table_id],
        filter,
        rls: None,
        order_by,
        limit: ast.limit,
        equalities,
        spatial,
        query_hash,
        normalized,
    })
}

/// One compiled WHERE condition with literals already coerced to the
/// column's type — evaluation is a typed value comparison, never a cast.
enum CompiledCond {
    Eq(u16, RowValue),
    In(u16, Vec<RowValue>),
    Between(u16, RowValue, RowValue),
}

impl CompiledCond {
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
    }
}

fn compile_spatial(table: &TableSchema, clause: SpatialAst) -> Result<SpatialConstraint> {
    let has_spatial = table
        .indexes
        .iter()
        .any(|index| matches!(index, IndexSchema::Spatial { .. }));
    if !has_spatial {
        return Err(FluxumError::query(
            codes::MALFORMED,
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
                codes::MALFORMED,
                format!("unknown column `{column}` on table `{}`", table.name),
            )
        })
}

/// Coerce a parsed literal to a column's [`FluxType`] (range-checked; no
/// cross-kind coercion beyond int→float widening).
fn coerce(table: &TableSchema, column: &str, lit: &Lit, ty: &FluxType) -> Result<RowValue> {
    let mismatch = || {
        FluxumError::query(
            codes::MALFORMED,
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
    }
    if let Some(limit) = ast.limit {
        let _ = write!(out, " LIMIT {limit}");
    }
    out
}
