//! End-to-end `#[derive(FluxType)]` rich column types (SPEC-023 DMX-030/031,
//! task phase1_rich-column-types-enums-structs): tagged-union enums and nested
//! structs as `#[fluxum::table]` columns — schema descriptor, typed⇄dynamic
//! row conversion, and value equality.
#![allow(dead_code)]
#![allow(clippy::expect_used)]

use fluxum_core::schema::{FluxType, FluxTypeDef, Table};
use fluxum_core::store::RowValue;
use fluxum_core::types::{Identity, Timestamp};
use fluxum_macros as fluxum;

#[derive(fluxum::FluxType)]
pub enum Status {
    Todo,
    Doing,
    Done { by: Identity },
    Snoozed(Timestamp),
}

#[derive(fluxum::FluxType)]
pub struct Point {
    pub x: i32,
    pub y: i32,
}

#[fluxum::table(public)]
pub struct Task {
    #[primary_key]
    pub id: u64,
    pub status: Status,
    pub origin: Point,
}

fn identity(seed: u8) -> Identity {
    Identity::from_bytes([seed; 32])
}

#[test]
fn schema_descriptor_carries_enum_and_struct_shapes() {
    let schema = <Task as Table>::SCHEMA;
    // status: enum
    match schema.columns[1].ty {
        FluxType::Enum(e) => {
            assert_eq!(e.name, "Status");
            assert_eq!(e.variants.len(), 4);
            assert_eq!(e.variants[0].name, "Todo");
            assert!(e.variants[0].payload.is_empty());
            assert_eq!(e.variants[2].name, "Done");
            assert!(matches!(e.variants[2].payload, [FluxType::Identity]));
            assert!(matches!(e.variants[3].payload, [FluxType::Timestamp]));
        }
        other => panic!("expected enum column, got {other:?}"),
    }
    // origin: nested struct
    match schema.columns[2].ty {
        FluxType::Struct(s) => {
            assert_eq!(s.name, "Point");
            assert_eq!(s.fields.len(), 2);
            assert_eq!(s.fields[0].name, "x");
            assert!(matches!(s.fields[0].ty, FluxType::I32));
        }
        other => panic!("expected struct column, got {other:?}"),
    }
}

#[test]
fn typed_row_round_trips_through_dynamic_row_values() {
    let task = Task {
        id: 1,
        status: Status::Done { by: identity(7) },
        origin: Point { x: -3, y: 4 },
    };
    let values = task.into_values();
    // Done is the third variant (tag 2), carrying one Identity.
    match &values[1] {
        RowValue::Enum { tag, payload } => {
            assert_eq!(*tag, 2);
            assert!(matches!(payload.as_slice(), [RowValue::Identity(_)]));
        }
        other => panic!("expected enum value, got {other:?}"),
    }
    assert!(matches!(values[2], RowValue::Struct(_)));

    // Rebuild the typed row and re-serialize: the dynamic values must match
    // exactly (covers to_row_value + from_row_value codegen).
    let rebuilt = Task::from_values(&values).expect("from_values");
    assert_eq!(rebuilt.into_values(), values);
}

#[test]
fn unit_variant_and_snoozed_round_trip() {
    for status in [
        Status::Todo,
        Status::Doing,
        Status::Snoozed(Timestamp::from_micros(42)),
    ] {
        let values = Task {
            id: 9,
            status,
            origin: Point { x: 0, y: 0 },
        }
        .into_values();
        let back = Task::from_values(&values).expect("from_values");
        assert_eq!(back.into_values(), values);
    }
}

#[test]
fn rich_values_support_equality() {
    let done1 = Status::Done { by: identity(1) }.to_row_value();
    let done1_again = Status::Done { by: identity(1) }.to_row_value();
    let done2 = Status::Done { by: identity(2) }.to_row_value();
    let todo = Status::Todo.to_row_value();
    assert_eq!(done1, done1_again);
    assert_ne!(done1, done2); // same variant, different payload
    assert_ne!(done1, todo); // different variant
    assert_eq!(
        Point { x: 5, y: -1 }.to_row_value(),
        Point { x: 5, y: -1 }.to_row_value()
    );
    assert_ne!(
        Point { x: 5, y: -1 }.to_row_value(),
        Point { x: 5, y: 0 }.to_row_value()
    );
}
