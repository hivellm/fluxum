//! The SPEC-013 reference corpus over the canonical demo schema (SPEC-015
//! TIER-043): `User`, `ChatMessage`, `Task`, and `Sensor` populated with
//! realistic text and telemetry distributions — chat text and task prose
//! drawn from a natural-language word pool, telemetry with per-device
//! identifiers, quantized readings (sensors report fixed precision), and
//! interval timestamps.
//!
//! Shared by the T2.9 compression-ratio benchmark
//! (`benches/compression.rs`) and the acceptance suite
//! (`tests/page_compression.rs`) via `#[path]` inclusion, so the published
//! ratio and the DAG exit test measure the same corpus.

use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, TableId};

/// Deterministic splitmix64 PRNG — no `rand` dependency, stable corpus.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn pick<'a>(&mut self, pool: &[&'a str]) -> &'a str {
        pool[(self.next() % pool.len() as u64) as usize]
    }

    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }
}

/// Natural-language phrase pool for chat text and task prose. Human text is
/// redundant at the *phrase* level, not the character level — greetings,
/// stock expressions, and domain vocabulary recur constantly — and that
/// phrase-level repetition is what makes real message logs compressible.
static PHRASES: &[&str] = &[
    "can you take a look at this",
    "sounds good to me",
    "the latency spike is back on the primary",
    "deployed to production a few minutes ago",
    "the dashboard shows the same numbers",
    "let me check the metrics first",
    "the checkpoint finished without errors",
    "we should bump the memory budget",
    "the replica caught up after the restart",
    "tests are green on all three platforms",
    "please review the storage engine change",
    "the customer reported the issue this morning",
    "I will pick this up tomorrow",
    "the query planner chose the secondary index",
    "throughput is back to normal",
    "the commit log rotated as expected",
    "thanks for the quick turnaround",
    "the shard handoff completed cleanly",
    "we need a follow-up ticket for that",
    "the backup restore drill passed",
    "looks good, merging now",
    "the sensor batch arrived late again",
    "buffer pool occupancy stays under the watermark",
    "the release notes are ready for review",
    "same error as last week's incident",
    "the on-call rotation changes on monday",
    "page compression cut the disk usage",
    "the migration ran in under a minute",
    "let's move this to the infra channel",
    "the fix is in the release branch",
    "readings from the north cluster look stable",
    "the invoice export job finished",
];

/// A message/prose string of roughly `min_len..max_len` bytes assembled
/// from recurring phrases.
fn prose(rng: &mut Rng, min_len: u64, max_len: u64) -> String {
    let target = rng.range(min_len, max_len) as usize;
    let mut out = String::new();
    while out.len() < target {
        if !out.is_empty() {
            out.push_str(", ");
        }
        out.push_str(rng.pick(PHRASES));
    }
    out
}

/// Microseconds since the epoch, corpus base time.
const T0: i64 = 1_700_000_000_000_000;

// --- Canonical demo schema (hand-built statics, macro-output stand-ins) ----

static USER_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "name",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "email",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "bio",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "created_at",
        ty: FluxType::I64,
    },
    ColumnSchema {
        name: "active",
        ty: FluxType::Bool,
    },
];

pub static USER: TableSchema = TableSchema {
    name: "User",
    columns: USER_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

static CHAT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "sender",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "channel",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "text",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "sent_at",
        ty: FluxType::I64,
    },
];

pub static CHAT_MESSAGE: TableSchema = TableSchema {
    name: "ChatMessage",
    columns: CHAT_COLS,
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
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "title",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "description",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "state",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "priority",
        ty: FluxType::U8,
    },
    ColumnSchema {
        name: "updated_at",
        ty: FluxType::I64,
    },
];

pub static TASK: TableSchema = TableSchema {
    name: "Task",
    columns: TASK_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

static SENSOR_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "device",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "grid_x",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "grid_y",
        ty: FluxType::I32,
    },
    ColumnSchema {
        name: "x",
        ty: FluxType::F32,
    },
    ColumnSchema {
        name: "y",
        ty: FluxType::F32,
    },
    ColumnSchema {
        name: "reading",
        ty: FluxType::F64,
    },
    ColumnSchema {
        name: "battery",
        ty: FluxType::U8,
    },
    ColumnSchema {
        name: "updated_at",
        ty: FluxType::I64,
    },
];

pub static SENSOR: TableSchema = TableSchema {
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

static FIRST_NAMES: &[&str] = &[
    "alice", "bruno", "carla", "diego", "elena", "felipe", "gina", "hugo", "iris", "joao", "karin",
    "lucas", "marta", "nadia", "otto", "paula",
];

static LAST_NAMES: &[&str] = &[
    "almeida",
    "barbosa",
    "costa",
    "duarte",
    "esteves",
    "ferreira",
    "gomes",
    "henrique",
    "iglesias",
    "junqueira",
    "klein",
    "lima",
    "moraes",
    "nogueira",
    "oliveira",
    "pereira",
];

static CHANNELS: &[&str] = &[
    "general",
    "engineering",
    "support",
    "alerts",
    "random",
    "release",
    "oncall",
    "product",
    "design",
    "billing",
    "infra",
    "data",
];

static STATES: &[&str] = &["backlog", "in_progress", "review", "done", "blocked"];

/// Stock short replies — a large share of real chat traffic is verbatim
/// repeats of a small set of acknowledgements.
static STOCK_REPLIES: &[&str] = &[
    "thanks!",
    "+1",
    "lgtm",
    "sounds good to me",
    "on it",
    "done, please verify",
    "will do",
    "same here",
    "taking a look now",
    "fixed in the latest build",
    "good catch",
    "see the thread above",
];

/// One realistic row of `table` with primary key `id`.
pub fn row_for(table: &TableSchema, id: u64, rng: &mut Rng) -> Vec<RowValue> {
    if std::ptr::eq(table, &USER) {
        let first = rng.pick(FIRST_NAMES);
        let last = rng.pick(LAST_NAMES);
        vec![
            RowValue::U64(id),
            RowValue::Str(format!("{first} {last}")),
            RowValue::Str(format!("{first}.{last}{}@example.com", id % 1000)),
            RowValue::Str(prose(rng, 100, 260)),
            RowValue::I64(T0 + (id as i64) * 86_400_000_000 / 100),
            RowValue::Bool(!rng.next().is_multiple_of(10)),
        ]
    } else if std::ptr::eq(table, &CHAT_MESSAGE) {
        // Senders are Zipf-like (a small active core writes most traffic),
        // and roughly a third of messages are verbatim stock replies.
        let sender = if rng.next() % 5 < 4 {
            1 + rng.next() % 40
        } else {
            1 + rng.next() % 5_000
        };
        let text = if rng.next().is_multiple_of(4) {
            rng.pick(STOCK_REPLIES).to_owned()
        } else {
            prose(rng, 70, 260)
        };
        vec![
            RowValue::U64(id),
            RowValue::U64(sender),
            RowValue::Str(rng.pick(CHANNELS).to_owned()),
            RowValue::Str(text),
            // Millisecond-precision arrival stamps (chat systems rarely
            // store sub-millisecond time).
            RowValue::I64(T0 + (id as i64) * 2_000_000 + (rng.next() % 1_000) as i64 * 1_000),
        ]
    } else if std::ptr::eq(table, &TASK) {
        vec![
            RowValue::U64(id),
            RowValue::U64(rng.range(1, 500)),
            RowValue::Str(prose(rng, 20, 50)),
            RowValue::Str(prose(rng, 150, 340)),
            RowValue::Str(rng.pick(STATES).to_owned()),
            RowValue::U8((rng.next() % 5) as u8),
            RowValue::I64(T0 + (id as i64) * 60_000_000),
        ]
    } else if std::ptr::eq(table, &SENSOR) {
        // Telemetry with realistic structure: 256 fixed devices on a grid
        // (position is a per-device constant), fixed-precision readings
        // that drift slowly around a per-device baseline (sensors report
        // quantized values that repeat for long stretches), a battery
        // level that decays over hours, and a 1 Hz interval timestamp.
        //
        // Devices report round-robin (`id % 256`), the shape of an
        // interleaved ingestion feed. Note on storage order: the primary
        // tree sorts by FluxBIN **key bytes** (little-endian), so leaf
        // pages group rows by `id`'s low byte — i.e. per device — which is
        // exactly the on-page locality an insertion-ordered store would
        // give the same feed.
        let device_no = id % 256;
        let baseline = 18.0 + (device_no % 40) as f64 * 0.5;
        vec![
            RowValue::U64(id),
            RowValue::Str(format!("sensor-{device_no:04}")),
            RowValue::I32((device_no % 16) as i32),
            RowValue::I32((device_no / 16) as i32),
            RowValue::F32(((device_no % 16) * 100) as f32),
            RowValue::F32(((device_no / 16) * 100) as f32),
            RowValue::F64(baseline + (rng.next() % 4) as f64 * 0.05),
            RowValue::U8((100 - (id / 20_000).min(70)) as u8),
            RowValue::I64(T0 + (id as i64) * 1_000_000),
        ]
    } else {
        panic!("unknown corpus table `{}`", table.name);
    }
}

/// A committed [`MemStore`] holding `rows` corpus rows of `table`.
pub fn populated(table: &'static TableSchema, rows: u64) -> (MemStore, TableId) {
    let schema = Schema::from_tables([table]).expect("corpus schema assembles");
    let store = MemStore::new(&schema).expect("corpus store builds");
    let table_id = store.table_id(table.name).expect("corpus table registered");
    let mut rng = Rng(0xF1A5_0000 ^ table.name.len() as u64);
    let mut inserted = 0u64;
    while inserted < rows {
        let mut tx = store.begin();
        for _ in 0..1_000.min(rows - inserted) {
            tx.insert(table_id, row_for(table, inserted, &mut rng))
                .expect("corpus insert");
            inserted += 1;
        }
        tx.commit().expect("corpus commit");
    }
    (store, table_id)
}
