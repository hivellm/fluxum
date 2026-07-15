//! T3.2 macro-integration suite (SPEC-004 RED-002/003, DM-043): tables
//! declared with `#[fluxum::table]` drive the typed `TxHandle` end to end —
//! the generated `into_values` / `from_values` / `pk_values` conversions
//! round-trip every column-type shape (nested `Option`/`Vec`, `Vec<u8>`,
//! identity/timestamp newtypes, composite PKs) through a real `MemStore`
//! transaction, including the FR-17 intra-transaction visibility split.
#![allow(dead_code)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::reducer::{ReducerCaller, ReducerRegistry, with_context};
use fluxum_core::schema::{Schema, Table};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_macros as fluxum;

/// Every conversion shape in one table: auto-inc PK, newtypes, `String`,
/// `Vec<u8>` (Bytes), `Vec<String>` (List), `Option<i32>`, and a nested
/// `Vec<Option<u16>>`.
#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub sender: Identity,
    pub channel: u32,
    pub content: String,
    pub tags: Vec<String>,
    pub priority: Option<i32>,
    pub payload: Vec<u8>,
    pub readings: Vec<Option<u16>>,
    pub sent_at: Timestamp,
}

/// Composite PK: `Table::Pk` is a tuple and `pk_values` follows the
/// table-level `primary_key(...)` declaration order.
#[fluxum::table(public, primary_key(grid_x, grid_y))]
#[derive(Debug, Clone, PartialEq)]
pub struct Sensor {
    pub grid_x: i32,
    pub grid_y: i32,
    pub reading: f64,
    pub label: Option<String>,
}

fn message() -> Message {
    Message {
        id: 0, // auto_inc placeholder
        sender: Identity::from_token("alice"),
        channel: 42,
        content: "hello".into(),
        tags: vec!["urgent".into(), "ops".into()],
        priority: Some(-7),
        payload: vec![0xAB, 0x01],
        readings: vec![Some(3), None],
        sent_at: Timestamp::from_micros(1_720_000_000_000_000),
    }
}

fn caller() -> ReducerCaller {
    ReducerCaller {
        identity: Identity::from_token("tester"),
        connection_id: ConnectionId::new(9),
        timestamp: Timestamp::from_micros(1_720_000_000_000_000),
        shard_id: 1,
    }
}

// --- Generated conversions (DM-043) -----------------------------------------

#[test]
fn generated_into_values_produces_declaration_order_row_values() {
    let values = message().into_values();
    assert_eq!(values.len(), 9);
    assert_eq!(values[0], RowValue::U64(0));
    assert_eq!(values[1], RowValue::Identity(Identity::from_token("alice")));
    assert_eq!(values[2], RowValue::U32(42));
    assert_eq!(values[3], RowValue::Str("hello".into()));
    assert_eq!(
        values[4],
        RowValue::List(vec![
            RowValue::Str("urgent".into()),
            RowValue::Str("ops".into())
        ])
    );
    assert_eq!(
        values[5],
        RowValue::Optional(Some(Box::new(RowValue::I32(-7))))
    );
    assert_eq!(values[6], RowValue::Bytes(vec![0xAB, 0x01]));
    assert_eq!(
        values[7],
        RowValue::List(vec![
            RowValue::Optional(Some(Box::new(RowValue::U16(3)))),
            RowValue::Optional(None),
        ])
    );
    assert_eq!(
        values[8],
        RowValue::Timestamp(Timestamp::from_micros(1_720_000_000_000_000))
    );
}

#[test]
fn generated_from_values_round_trips_and_none_variants_hold() {
    let original = message();
    let restored = Message::from_values(&original.clone().into_values()).unwrap();
    assert_eq!(restored, original);

    // None / empty-collection shapes survive the round trip too.
    let sparse = Message {
        priority: None,
        tags: vec![],
        payload: vec![],
        readings: vec![],
        ..message()
    };
    let restored = Message::from_values(&sparse.clone().into_values()).unwrap();
    assert_eq!(restored, sparse);

    let sensor = Sensor {
        grid_x: -2,
        grid_y: 9,
        reading: 1.25,
        label: None,
    };
    let restored = Sensor::from_values(&sensor.clone().into_values()).unwrap();
    assert_eq!(restored, sensor);
}

#[test]
fn generated_pk_values_follow_primary_key_declaration_order() {
    assert_eq!(Message::pk_values(&7), vec![RowValue::U64(7)]);
    assert_eq!(
        Sensor::pk_values(&(-2, 9)),
        vec![RowValue::I32(-2), RowValue::I32(9)]
    );
    // primary_key() still agrees with pk_values().
    let sensor = Sensor {
        grid_x: -2,
        grid_y: 9,
        reading: 0.0,
        label: None,
    };
    assert_eq!(sensor.primary_key(), (-2, 9));
}

#[test]
fn generated_from_values_rejects_arity_and_type_mismatches() {
    let err = Message::from_values(&[RowValue::U64(1)]).unwrap_err();
    assert!(err.to_string().contains("declares 9 columns"), "{err}");

    let mut wrong = message().into_values();
    wrong[3] = RowValue::Bool(true); // content: String
    let err = Message::from_values(&wrong).unwrap_err();
    assert!(
        err.to_string()
            .contains("column `content` does not inhabit its declared column type"),
        "{err}"
    );

    // A mistyped element nested inside a List is caught too.
    let mut nested = message().into_values();
    nested[4] = RowValue::List(vec![RowValue::U8(1)]); // tags: Vec<String>
    let err = Message::from_values(&nested).unwrap_err();
    assert!(err.to_string().contains("column `tags`"), "{err}");
}

// --- End to end: macro tables through the typed TxHandle (RED-003) ---------

#[test]
fn macro_tables_drive_the_typed_txhandle_end_to_end() {
    let schema = Schema::from_tables([Message::SCHEMA, Sensor::SCHEMA]).unwrap();
    let store = MemStore::new(&schema).unwrap();
    let registry = ReducerRegistry::new();

    // Transaction 1: typed writes; intra-tx visibility split (FR-17).
    let mut tx = store.begin();
    let stored = with_context(&registry, caller(), &mut tx, |ctx| {
        let stored = ctx.tx.insert(message())?;
        assert_eq!(stored.id, 1, "auto-inc id came back on the typed row");
        ctx.tx.insert(Sensor {
            grid_x: -2,
            grid_y: 9,
            reading: 1.25,
            label: Some("north".into()),
        })?;

        // Committed-only default reads (TXN-050)…
        assert!(ctx.tx.scan::<Message>()?.is_empty());
        assert_eq!(ctx.tx.query_pk::<Sensor>((-2, 9))?, None);
        // …and the explicit pending/combined views (TXN-051).
        assert_eq!(ctx.tx.scan_pending::<Message>()?, vec![stored.clone()]);
        assert_eq!(ctx.tx.count_pending::<Message>(|m| m.channel == 42)?, 1);
        assert_eq!(ctx.tx.scan_all::<Message>()?.len(), 1);
        Ok(stored)
    })
    .unwrap();
    tx.commit().unwrap();

    // Transaction 2: typed reads over committed state, upsert, delete.
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        let found = ctx.tx.query_pk::<Message>(1)?.unwrap();
        assert_eq!(found, stored);

        let by_channel = ctx.tx.scan_where::<Message>(|m| m.channel == 42)?;
        assert_eq!(by_channel.len(), 1);
        assert_eq!(by_channel[0].tags, vec!["urgent", "ops"]);

        let sensor = ctx.tx.query_pk::<Sensor>((-2, 9))?.unwrap();
        ctx.tx.upsert(Sensor {
            reading: 2.5,
            label: None,
            ..sensor
        })?;

        // Combined view deduplicates by PK: the upsert content wins.
        let all = ctx.tx.scan_all::<Sensor>()?;
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].reading, 2.5);
        assert_eq!(all[0].label, None);

        assert!(ctx.tx.delete::<Message>(1)?);
        assert!(!ctx.tx.delete::<Message>(1)?);
        Ok(())
    })
    .unwrap();
    tx.commit().unwrap();

    // Committed outcome.
    let mut tx = store.begin();
    with_context(&registry, caller(), &mut tx, |ctx| {
        assert!(ctx.tx.scan::<Message>()?.is_empty());
        assert_eq!(ctx.tx.query_pk::<Sensor>((-2, 9))?.unwrap().reading, 2.5);
        Ok(())
    })
    .unwrap();
    tx.rollback();
}
