//! Shard routing (SPEC-007 §2/§3, T5.4): the pure partition-resolution
//! math `ShardCoord` routes with — deterministic, platform-stable, and
//! configuration-driven (SHD-001..004, SHD-012/013).
//!
//! # Determinism (SHD-003)
//!
//! For a fixed configuration the same partition key value always resolves
//! to the same shard, across restarts, platforms, and versions: the hash
//! strategy runs the platform-stable xxHash64 kernel (HWA-042) over the
//! FluxBIN encoding of the key — never a per-process seeded hasher — and
//! the range/region strategies are pure arithmetic over ordered
//! boundaries / the `floor(coord / region_size)` grid.
//!
//! # Decided open questions
//!
//! - **OQ-2** (tasks vs processes): the reference deployment runs every
//!   `ShardHost` as a tokio task inside one process (the spec's normative
//!   shape); process-per-shard remains a deployment alternative.
//! - **OQ-6** (default placement): tables without `partition_by` live on
//!   shard 0, per the normative SHD-004 text.

use std::collections::HashMap;

use crate::error::{FluxumError, Result};
use crate::schema::{Schema, TableAccess};
use crate::store::{RowValue, TableId};

/// A shard's stable identifier (SHD-010).
pub type ShardId = u32;

/// The `__handoff__` marker table (SHD-041 step 4): one row per entity with
/// a handoff in flight on this shard. Include it in the assembled schema of
/// every sharded deployment (like `__schedule__`/`__schema_meta__`).
pub static HANDOFF_TABLE: crate::schema::TableSchema = crate::schema::TableSchema {
    name: "__handoff__",
    columns: &[
        crate::schema::ColumnSchema {
            name: "entity_key",
            ty: crate::schema::FluxType::Str,
        },
        crate::schema::ColumnSchema {
            name: "state",
            ty: crate::schema::FluxType::Str,
        },
    ],
    primary_key: &[0],
    auto_inc: None,
    access: crate::schema::TableAccess::Private,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: crate::schema::VisibilityRule::PublicAll,
};

/// Encode an entity's partition-key values into the stable byte identity
/// used for handoff bookkeeping and queue keying (FluxBIN — the same
/// encoding the hash strategy routes over).
pub fn encode_entity_key(values: &[RowValue]) -> Result<Vec<u8>> {
    crate::store::row::encode_row(values)
}

/// One table's partition strategy (SHD-002/012).
#[derive(Debug, Clone, PartialEq)]
pub enum PartitionStrategy {
    /// `shard_id = stable_hash64(fluxbin(key)) % shard_count`.
    Hash {
        /// The configured shard count.
        shard_count: u32,
    },
    /// Ordered key boundaries; the greatest boundary ≤ key wins. Keys below
    /// the first boundary route to the first boundary's shard.
    Range {
        /// `(inclusive lower boundary, owning shard)`, ascending.
        boundaries: Vec<(i64, ShardId)>,
    },
    /// Rectangular grid over two numeric columns:
    /// `grid = (floor(x / region_size), floor(y / region_size))`, row-major
    /// over the grid, wrapped modulo the shard count.
    Region {
        /// Cell edge length.
        region_size: f64,
        /// Grid columns (cells per row) for row-major cell numbering.
        grid_columns: u32,
        /// The configured shard count.
        shard_count: u32,
    },
}

impl PartitionStrategy {
    /// Resolve the owning shard of one partition key (SHD-012). `values`
    /// carries the key columns: one value for hash/range, `(x, y)` for
    /// region.
    pub fn shard_for(&self, values: &[RowValue]) -> Result<ShardId> {
        match self {
            Self::Hash { shard_count } => {
                let [key] = values else {
                    return Err(FluxumError::Storage(
                        "hash partitioning takes exactly one key value (SHD-002)".into(),
                    ));
                };
                let encoded = crate::store::row::encode_row(std::slice::from_ref(key))?;
                let hash = crate::simd::global().hash64(&encoded, 0);
                #[allow(clippy::cast_possible_truncation)] // modulo shard_count
                Ok((hash % u64::from((*shard_count).max(1))) as ShardId)
            }
            Self::Range { boundaries } => {
                let key = match values {
                    [value] => orderable_i64(value)?,
                    _ => {
                        return Err(FluxumError::Storage(
                            "range partitioning takes exactly one key value (SHD-002)".into(),
                        ));
                    }
                };
                let mut owner = boundaries.first().map_or(0, |(_, shard)| *shard);
                for (boundary, shard) in boundaries {
                    if *boundary <= key {
                        owner = *shard;
                    } else {
                        break;
                    }
                }
                Ok(owner)
            }
            Self::Region {
                region_size,
                grid_columns,
                shard_count,
            } => {
                let (x, y) = match values {
                    [x, y] => (numeric_f64(x)?, numeric_f64(y)?),
                    _ => {
                        return Err(FluxumError::Storage(
                            "region partitioning takes exactly (x, y) (SHD-002)".into(),
                        ));
                    }
                };
                if *region_size <= 0.0 {
                    return Err(FluxumError::Storage(
                        "region_size must be > 0 (SHD-013)".into(),
                    ));
                }
                #[allow(clippy::cast_possible_truncation)] // grid cells are small
                let grid_x = (x / region_size).floor() as i64;
                #[allow(clippy::cast_possible_truncation)]
                let grid_y = (y / region_size).floor() as i64;
                // Row-major cell number, wrapped modulo the shard count —
                // deterministic and total over the whole plane (cells
                // outside any configured bounds still resolve).
                let columns = i64::from((*grid_columns).max(1));
                let cell = grid_y
                    .rem_euclid(columns)
                    .saturating_add(grid_x.rem_euclid(columns).saturating_mul(columns));
                #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
                Ok((cell.rem_euclid(i64::from((*shard_count).max(1)))) as ShardId)
            }
        }
    }
}

/// The resolved routing table of one deployment (SHD-010): per-table
/// strategies plus the SHD-004 default shard.
#[derive(Debug, Default)]
pub struct ShardRouter {
    partitioning: HashMap<TableId, (PartitionStrategy, Vec<u16>)>,
    default_shard: ShardId,
    authoritative_global: ShardId,
}

impl ShardRouter {
    /// Build the router from the assembled schema: every table declaring
    /// `partition_by` gets `strategy` (region for two-column keys); tables
    /// without one live on shard 0 (SHD-004, OQ-6 normative default).
    /// `#[fluxum::table(global)]` tables route writes to the authoritative
    /// shard (shard 0, SHD-030).
    pub fn from_schema(schema: &Schema, shard_count: u32) -> Self {
        let mut partitioning = HashMap::new();
        for table in schema.tables() {
            let Some(ordinal) = table.partition_by else {
                continue;
            };
            // The macro surface carries one partition ordinal; two-column
            // region keys ride spatial declarations (SHD-002's region form
            // is configured per table via ShardingConfig, SHD-013).
            partitioning.insert(
                TableId::of(table.name),
                (PartitionStrategy::Hash { shard_count }, vec![ordinal]),
            );
        }
        Self {
            partitioning,
            default_shard: 0,
            authoritative_global: 0,
        }
    }

    /// Override one table's strategy (the SHD-013 per-table config).
    pub fn set_strategy(&mut self, table: TableId, strategy: PartitionStrategy, key: Vec<u16>) {
        self.partitioning.insert(table, (strategy, key));
    }

    /// Every partitioned table and its key ordinals — the partition domain
    /// an entity's row set spans (SHD-043).
    pub fn partitioned_tables(&self) -> Vec<(TableId, Vec<u16>)> {
        self.partitioning
            .iter()
            .map(|(table, (_, key))| (*table, key.clone()))
            .collect()
    }

    /// Resolve an entity key directly (tables of one partition domain share
    /// the strategy, SHD-043) — the SHD-011/044 routing entry for
    /// entity-keyed calls.
    pub fn shard_of_key(&self, key: &[RowValue]) -> Result<ShardId> {
        let Some((strategy, _)) = self.partitioning.values().next() else {
            return Ok(self.default_shard);
        };
        strategy.shard_for(key)
    }

    /// The shard owning `row` of `table` (SHD-012): the table's strategy
    /// over its key columns, or the default shard for unpartitioned tables
    /// (SHD-004). Global tables answer the authoritative shard (SHD-030).
    pub fn shard_of_row(
        &self,
        schema: &Schema,
        table: TableId,
        values: &[RowValue],
    ) -> Result<ShardId> {
        if let Some(table_schema) = schema.tables().find(|t| TableId::of(t.name) == table)
            && table_schema.access == TableAccess::Global
        {
            return Ok(self.authoritative_global);
        }
        let Some((strategy, key_ordinals)) = self.partitioning.get(&table) else {
            return Ok(self.default_shard);
        };
        let key: Vec<RowValue> = key_ordinals
            .iter()
            .map(|&ordinal| {
                values.get(usize::from(ordinal)).cloned().ok_or_else(|| {
                    FluxumError::Storage(format!(
                        "partition key ordinal {ordinal} missing from the row (SHD-001)"
                    ))
                })
            })
            .collect::<Result<_>>()?;
        strategy.shard_for(&key)
    }

    /// The shard a caller identity acquires affinity to (SHD-011): the
    /// stable hash of the identity over `shard_count` for hash-partitioned
    /// domains; the default shard when nothing is partitioned.
    pub fn affinity_of(&self, identity: &crate::types::Identity, shard_count: u32) -> ShardId {
        if self.partitioning.is_empty() || shard_count <= 1 {
            return self.default_shard;
        }
        let hash = crate::simd::global().hash64(identity.as_bytes(), 0);
        #[allow(clippy::cast_possible_truncation)]
        {
            (hash % u64::from(shard_count.max(1))) as ShardId
        }
    }

    /// The authoritative shard for `#[fluxum::table(global)]` writes
    /// (SHD-030; shard 0 by default).
    pub fn authoritative_global(&self) -> ShardId {
        self.authoritative_global
    }
}

fn orderable_i64(value: &RowValue) -> Result<i64> {
    Ok(match value {
        RowValue::I8(n) => i64::from(*n),
        RowValue::I16(n) => i64::from(*n),
        RowValue::I32(n) => i64::from(*n),
        RowValue::I64(n) => *n,
        RowValue::U8(n) => i64::from(*n),
        RowValue::U16(n) => i64::from(*n),
        RowValue::U32(n) => i64::from(*n),
        RowValue::U64(n) => i64::try_from(*n).unwrap_or(i64::MAX),
        RowValue::Timestamp(ts) => ts.as_micros(),
        RowValue::EntityId(id) => i64::try_from(id.as_u64()).unwrap_or(i64::MAX),
        other => {
            return Err(FluxumError::Storage(format!(
                "range partitioning needs an orderable integer key, got {other:?} (SHD-002)"
            )));
        }
    })
}

fn numeric_f64(value: &RowValue) -> Result<f64> {
    Ok(match value {
        RowValue::F32(x) => f64::from(*x),
        RowValue::F64(x) => *x,
        RowValue::I32(n) => f64::from(*n),
        RowValue::I64(n) => {
            #[allow(clippy::cast_precision_loss)]
            {
                *n as f64
            }
        }
        other => {
            return Err(FluxumError::Storage(format!(
                "region partitioning needs numeric (x, y), got {other:?} (SHD-002)"
            )));
        }
    })
}
