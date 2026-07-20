//! T4.5 subscription-correctness property suite (SPEC-013 TST-030..034;
//! SPEC-005 acceptance 8; NFR-10; DAG exit / Gate G4): the product promise
//! that a subscribed client's cache — seeded from `InitialData` and
//! maintained **solely** by applying `TxUpdate` diffs — is byte-for-byte
//! identical to the server-side result of its query, after every one of
//! 10,000 random mutations across public and `owner_only` tables.
//!
//! In-crate (not `tests/`) so the model can decode the wire `RowList`s with
//! the same `crate::store::row` codec the manager encodes with — exactly
//! what an SDK does, and the fixture the phase-6 SDK conformance corpus is
//! seeded from.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;

use fluxum_protocol::RowList;

use crate::schema::{ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule};
use crate::store::row::{decode_row, encode_pk_of_row};
use crate::store::{MemStore, RowValue, TableId};
use crate::types::Identity;

use super::{Subscriber, SubscriptionLimits, SubscriptionManager};

// --- Tables: one public, one owner_only ----------------------------------------

static SENSOR_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "channel",
        ty: FluxType::U32,
    },
    ColumnSchema {
        name: "reading",
        ty: FluxType::I64,
    },
];
static SENSOR: TableSchema = TableSchema {
    name: "Sensor",
    columns: SENSOR_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

static TASK_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "owner",
        ty: FluxType::Identity,
    },
    ColumnSchema {
        name: "value",
        ty: FluxType::I64,
    },
];
static TASK: TableSchema = TableSchema {
    name: "Task",
    columns: TASK_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::OwnerOnly { owner: 1 },
};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::from_tables([&SENSOR, &TASK]).unwrap())
}

// --- Deterministic PRNG (splitmix64 — no rand dep, as in the DST/pager tests) --

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// --- The model client: a cache keyed by encoded PK bytes -----------------------
//
// Seeded from InitialData; maintained ONLY by applying TxUpdate diffs. The
// key is the FluxBIN-encoded PK: insert rows decode to full rows (re-encode
// their PK for the key), delete rows ARE the PK bytes already (RPC-042).

struct ModelClient {
    subscriber: Subscriber,
    sql: String,
    table: &'static TableSchema,
    cache: HashMap<Vec<u8>, Vec<RowValue>>,
}

impl ModelClient {
    fn seed(&mut self, list: &RowList) {
        for row in decode_full_rows(self.table, list) {
            let key = pk_key(self.table, &row);
            self.cache.insert(key, row);
        }
    }

    /// Apply one query delta: deletes first, then inserts — so an in-place
    /// update (delete(old) + insert(new) on the same PK) lands as the new
    /// value, not an accidental removal.
    fn apply(&mut self, deletes: &RowList, inserts: &RowList) {
        for pk_bytes in list_rows(deletes) {
            self.cache.remove(pk_bytes);
        }
        for row in decode_full_rows(self.table, inserts) {
            let key = pk_key(self.table, &row);
            self.cache.insert(key, row);
        }
    }

    /// The cache as a sorted (key, row) vector for comparison.
    fn sorted(&self) -> Vec<(Vec<u8>, Vec<RowValue>)> {
        let mut entries: Vec<_> = self
            .cache
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }
}

fn list_rows(list: &RowList) -> impl Iterator<Item = &[u8]> {
    list.iter()
}

fn decode_full_rows(table: &TableSchema, list: &RowList) -> Vec<Vec<RowValue>> {
    list.iter()
        .map(|bytes| decode_row(table, bytes).unwrap().values().to_vec())
        .collect()
}

fn pk_key(table: &TableSchema, values: &[RowValue]) -> Vec<u8> {
    encode_pk_of_row(table, values).unwrap().as_bytes().to_vec()
}

// --- The property harness ------------------------------------------------------

#[test]
fn ten_thousand_mutations_keep_every_client_cache_equal_to_server_state() {
    const MUTATIONS: usize = 10_000;
    const CLIENTS: usize = 24;
    const CHANNELS: u64 = 4;
    const OWNERS: u64 = 3;
    const VALUES: u64 = 5;

    let schema = schema();
    let store = MemStore::new(&schema).unwrap();
    let sensor_id = store.table_id("Sensor").unwrap();
    let task_id = store.table_id("Task").unwrap();
    let mut mgr = SubscriptionManager::new(Arc::clone(&schema), SubscriptionLimits::default());
    let mut rng = Rng(0xF1F0_2026_0715_0004);

    let owner_ids: Vec<Identity> = (0..OWNERS)
        .map(|i| Identity::from_bytes([0x10 + i as u8; 32]))
        .collect();

    // Build a population of clients with random subscriptions. Some are
    // server peers (RLS bypass); the rest are per-identity viewers.
    let mut clients: Vec<ModelClient> = Vec::new();
    for conn in 0..CLIENTS as u128 {
        let (sql, table): (String, &'static TableSchema) = match rng.below(5) {
            0 => ("SELECT * FROM Sensor".into(), &SENSOR),
            1 => (
                format!(
                    "SELECT * FROM Sensor WHERE channel = {}",
                    rng.below(CHANNELS)
                ),
                &SENSOR,
            ),
            2 => ("SELECT * FROM Task".into(), &TASK),
            3 => (
                format!("SELECT * FROM Task WHERE value = {}", rng.below(VALUES)),
                &TASK,
            ),
            _ => (
                format!("SELECT * FROM Task WHERE value = {}", rng.below(VALUES)),
                &TASK,
            ),
        };
        // Half the Task subscribers are server peers; Sensor is public so
        // the viewer never matters there.
        let subscriber = if table.name == "Task" && rng.below(2) == 0 {
            Subscriber::server_peer(crate::auth::server_identity(&format!("svc-{conn}")))
        } else {
            Subscriber::client(owner_ids[(conn as usize) % owner_ids.len()])
        };
        let sub = mgr
            .subscribe(conn, subscriber.clone(), &sql, &store.snapshot())
            .unwrap();
        let mut client = ModelClient {
            subscriber,
            sql,
            table,
            cache: HashMap::new(),
        };
        client.seed(&sub.initial.tables[0].inserts);
        clients.push(client);
    }

    // A running record of live primary keys per table (to pick existing
    // rows for updates/deletes).
    let mut sensor_pks: Vec<u64> = Vec::new();
    let mut task_pks: Vec<u64> = Vec::new();
    let mut next_id: u64 = 1;

    for _ in 0..MUTATIONS {
        let on_task = rng.below(2) == 0;
        let mut tx = store.begin();
        if on_task {
            mutate(
                &mut tx,
                task_id,
                &mut task_pks,
                &mut next_id,
                &mut rng,
                |rng, id| {
                    vec![
                        RowValue::U64(id),
                        RowValue::Identity(owner_ids[(rng.below(OWNERS)) as usize]),
                        RowValue::I64(rng.below(VALUES) as i64),
                    ]
                },
            );
        } else {
            mutate(
                &mut tx,
                sensor_id,
                &mut sensor_pks,
                &mut next_id,
                &mut rng,
                |rng, id| {
                    vec![
                        RowValue::U64(id),
                        RowValue::U32(rng.below(CHANNELS) as u32),
                        RowValue::I64(rng.below(1_000) as i64),
                    ]
                },
            );
        }
        let diff = tx.commit().unwrap();

        // Apply the fan-out diffs to each affected client's cache.
        let deltas = mgr.on_commit(&diff).unwrap();
        for delta in &deltas {
            for &(conn, _query_id) in &delta.subscribers {
                clients[conn as usize].apply(&delta.update.deletes, &delta.update.inserts);
            }
        }

        // Every client's diff-maintained cache equals the server's current
        // answer for its subscription (SPEC-005 acceptance 8, 100% accuracy).
        let snapshot = store.snapshot();
        for client in &clients {
            let server = mgr
                .snapshot_result(client.subscriber.clone(), &client.sql, &snapshot)
                .unwrap();
            let mut expected: Vec<(Vec<u8>, Vec<RowValue>)> =
                decode_full_rows(client.table, &server.tables[0].inserts)
                    .into_iter()
                    .map(|row| (pk_key(client.table, &row), row))
                    .collect();
            expected.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(
                client.sorted(),
                expected,
                "client cache diverged from server state for `{}`",
                client.sql
            );
        }
    }
}

/// Perform one random insert / update / delete on `table` inside `tx`,
/// keeping `pks` in sync with the live primary keys.
fn mutate(
    tx: &mut crate::store::Tx<'_>,
    table: TableId,
    pks: &mut Vec<u64>,
    next_id: &mut u64,
    rng: &mut Rng,
    row: impl Fn(&mut Rng, u64) -> Vec<RowValue>,
) {
    // Bias toward inserts early (empty table) then mix in updates/deletes.
    let op = if pks.is_empty() { 0 } else { rng.below(3) };
    match op {
        0 => {
            // Insert a brand-new row.
            let id = *next_id;
            *next_id += 1;
            tx.insert(table, row(rng, id)).unwrap();
            pks.push(id);
        }
        1 => {
            // Update (upsert) an existing row.
            let id = pks[(rng.below(pks.len() as u64)) as usize];
            tx.upsert(table, row(rng, id)).unwrap();
        }
        _ => {
            // Delete an existing row.
            let index = (rng.below(pks.len() as u64)) as usize;
            let id = pks.swap_remove(index);
            assert!(tx.delete(table, &[RowValue::U64(id)]).unwrap());
        }
    }
}
