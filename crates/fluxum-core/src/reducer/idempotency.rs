//! Exactly-once reducer submission (SPEC-021 §4, CS-030..032).
//!
//! A client that loses the ack for a `ReducerCall` cannot know whether the
//! call applied. Resending it blindly double-applies — the classic double
//! transfer. An optional client-assigned `idempotency_key` closes that: the
//! shard records applied keys in a bounded, durable window and answers a
//! replayed key with the original result instead of re-running the body.
//!
//! # Shape
//!
//! The window is the `__idempotency__` system table, one row per applied
//! key, written **inside the reducer's own transaction** — so it commits
//! atomically with the effects it guards and rides the commit log to
//! survive restart (CS-031). Its primary key is `(identity, reducer, key)`,
//! which *is* the CS-031 scoping rule: two callers, or one caller across
//! two reducers, can reuse a key without colliding.
//!
//! # What is and isn't deduplicated
//!
//! Only **committed** calls are recorded. A reducer that returns `Err` or
//! panics rolls its transaction back, and the dedup row — written in that
//! same transaction — rolls back with it. That is the honest outcome rather
//! than a gap: a failed call applied nothing, so re-running it on retry is
//! safe. The guarantee is therefore "an applied call applies once", not
//! "a failed call is remembered".
//!
//! The window is bounded by count and age ([`IdempotencyOptions`]) and
//! pruned by the schedule worker. A key pruned before its retry arrives is
//! executed again — a key is a safety net for prompt retries, not an
//! indefinite promise.

use std::time::Duration;

use crate::error::Result;
use crate::schema::{ColumnSchema, FluxType, TableAccess, TableSchema, VisibilityRule};
use crate::store::{RowValue, TableId, Tx};
use crate::types::{Identity, Timestamp};

/// The dedup window's table name.
pub const IDEMPOTENCY_TABLE_NAME: &str = "__idempotency__";

static IDEMPOTENCY_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "identity",
        ty: FluxType::Identity,
    },
    ColumnSchema {
        name: "reducer",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "key",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "created_us",
        ty: FluxType::I64,
    },
];

/// The `__idempotency__` dedup window (CS-030/031). Include it in the
/// assembled schema of any deployment that accepts `idempotency_key`, like
/// `__schedule__`.
///
/// The composite primary key `(identity, reducer, key)` enforces the CS-031
/// scope: a key is only ever matched for the same caller and reducer.
pub static IDEMPOTENCY_TABLE: TableSchema = TableSchema {
    name: IDEMPOTENCY_TABLE_NAME,
    columns: IDEMPOTENCY_COLS,
    primary_key: &[0, 1, 2],
    auto_inc: None,
    access: TableAccess::Private,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

/// Bounds on the dedup window (CS-031, configurable).
#[derive(Debug, Clone, Copy)]
pub struct IdempotencyOptions {
    /// Keep at most this many records; the oldest beyond it are pruned.
    pub max_records: usize,
    /// Prune records older than this.
    pub max_age: Duration,
}

impl Default for IdempotencyOptions {
    fn default() -> Self {
        Self {
            // Roughly a busy client fleet's in-flight retries, and long
            // enough to cover a reconnect: a retry that takes more than an
            // hour is re-executed rather than remembered forever.
            max_records: 100_000,
            max_age: Duration::from_secs(3600),
        }
    }
}

/// The primary-key values addressing one key's record (CS-031 scope).
pub fn record_pk(identity: &Identity, reducer: &str, key: &str) -> Vec<RowValue> {
    vec![
        RowValue::Identity(*identity),
        RowValue::Str(reducer.to_owned()),
        RowValue::Str(key.to_owned()),
    ]
}

/// Whether `key` has already been applied for `(identity, reducer)`.
///
/// Reads the transaction's committed snapshot, so calling it *inside* the
/// reducer job makes the check-then-act atomic against the shard's single
/// writer: two concurrent calls carrying the same key cannot both miss.
pub fn already_applied(
    tx: &Tx<'_>,
    table: TableId,
    identity: &Identity,
    reducer: &str,
    key: &str,
) -> Result<bool> {
    Ok(tx
        .query_pk(table, &record_pk(identity, reducer, key))?
        .is_some())
}

/// Record `key` as applied, in the caller's own transaction (CS-031: the
/// record commits with the effects it guards, or not at all).
pub fn record(
    tx: &mut Tx<'_>,
    table: TableId,
    identity: &Identity,
    reducer: &str,
    key: &str,
) -> Result<()> {
    let mut values = record_pk(identity, reducer, key);
    values.push(RowValue::I64(Timestamp::now().as_micros()));
    tx.upsert(table, values)?;
    Ok(())
}

/// The primary keys of records to prune at `now_us` under `options`
/// (CS-031): everything past `max_age`, plus the oldest beyond
/// `max_records`. Pure — the caller deletes them in a transaction.
///
/// `records` is `(pk values, created_us)` for every row in the window.
pub fn prunable(
    mut records: Vec<(Vec<RowValue>, i64)>,
    now_us: i64,
    options: &IdempotencyOptions,
) -> Vec<Vec<RowValue>> {
    let max_age_us = i64::try_from(options.max_age.as_micros()).unwrap_or(i64::MAX);
    let mut doomed: Vec<Vec<RowValue>> = Vec::new();

    // Age bound: anything older than the window.
    records.retain(|(pk, created_us)| {
        if now_us.saturating_sub(*created_us) > max_age_us {
            doomed.push(pk.clone());
            false
        } else {
            true
        }
    });

    // Count bound: drop the oldest surplus. `sort_by_key` is stable and the
    // caller scans in encoded-PK order, so records sharing a timestamp are
    // pruned in a deterministic order rather than an arbitrary one.
    if records.len() > options.max_records {
        records.sort_by_key(|(_, created_us)| *created_us);
        let surplus = records.len() - options.max_records;
        doomed.extend(records.into_iter().take(surplus).map(|(pk, _)| pk));
    }
    doomed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(id: u8, created_us: i64) -> (Vec<RowValue>, i64) {
        (
            record_pk(&Identity::from_bytes([id; 32]), "transfer", "k"),
            created_us,
        )
    }

    #[test]
    fn the_scope_is_the_primary_key() {
        let alice = Identity::from_bytes([1; 32]);
        let bob = Identity::from_bytes([2; 32]);
        // CS-031: the same key never collides across callers or reducers.
        assert_ne!(
            record_pk(&alice, "transfer", "k"),
            record_pk(&bob, "transfer", "k"),
            "distinct callers"
        );
        assert_ne!(
            record_pk(&alice, "transfer", "k"),
            record_pk(&alice, "refund", "k"),
            "distinct reducers"
        );
        assert_eq!(record_pk(&alice, "transfer", "k").len(), 3);
    }

    #[test]
    fn age_bound_prunes_only_what_is_stale() {
        let options = IdempotencyOptions {
            max_records: 100,
            max_age: Duration::from_secs(60),
        };
        let now = 100_000_000i64; // µs
        let fresh = rec(1, now - 1_000_000); // 1s old
        let stale = rec(2, now - 120_000_000); // 120s old
        let doomed = prunable(vec![fresh.clone(), stale.clone()], now, &options);
        assert_eq!(doomed, vec![stale.0], "only the stale record");
    }

    #[test]
    fn count_bound_prunes_the_oldest_surplus() {
        let options = IdempotencyOptions {
            max_records: 2,
            max_age: Duration::from_secs(3600),
        };
        let now = 100_000_000i64;
        let oldest = rec(1, now - 3_000_000);
        let middle = rec(2, now - 2_000_000);
        let newest = rec(3, now - 1_000_000);
        let doomed = prunable(
            vec![newest.clone(), oldest.clone(), middle.clone()],
            now,
            &options,
        );
        assert_eq!(doomed, vec![oldest.0], "the oldest beyond the cap");
    }

    #[test]
    fn a_window_inside_both_bounds_prunes_nothing() {
        let options = IdempotencyOptions::default();
        let now = 100_000_000i64;
        assert!(prunable(vec![rec(1, now), rec(2, now)], now, &options).is_empty());
    }

    #[test]
    fn both_bounds_apply_together() {
        let options = IdempotencyOptions {
            max_records: 1,
            max_age: Duration::from_secs(60),
        };
        let now = 1_000_000_000i64;
        let stale = rec(1, now - 120_000_000);
        let older_fresh = rec(2, now - 2_000_000);
        let newest = rec(3, now - 1_000_000);
        let doomed = prunable(
            vec![stale.clone(), older_fresh.clone(), newest.clone()],
            now,
            &options,
        );
        // Stale goes on age; of the two fresh ones, the older exceeds the
        // count cap; the newest survives.
        assert_eq!(doomed.len(), 2);
        assert!(doomed.contains(&stale.0));
        assert!(doomed.contains(&older_fresh.0));
        assert!(!doomed.contains(&newest.0));
    }
}
