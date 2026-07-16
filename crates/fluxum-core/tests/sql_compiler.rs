//! T4.1 SQL-compiler suite (SPEC-005 SUB-010..SUB-013, SUB-020; FR-30,
//! FR-35; DAG exit test): every supported form compiles to a working
//! `CompiledPlan`, predicates evaluate correctly over stored rows, spatial
//! clauses validate against the `#[spatial]` declaration, every SUB-012
//! construct is rejected with a named 400, and normalization + `QueryHash`
//! deduplicate equivalent query texts.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::schema::{
    ColumnSchema, FluxType, IndexSchema, Schema, SpatialKind, TableAccess, TableSchema,
    VisibilityRule,
};
use fluxum_core::sql::{CompiledPlan, SpatialConstraint, compile};
use fluxum_core::store::{MemStore, Row, RowValue, TableId};

// --- Schema fixtures -----------------------------------------------------------

static MESSAGE_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "channel",
        ty: FluxType::U32,
    },
    ColumnSchema {
        name: "sender",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "sent_at",
        ty: FluxType::Timestamp,
    },
    ColumnSchema {
        name: "priority",
        ty: FluxType::Option(&FluxType::I32),
    },
    ColumnSchema {
        name: "urgent",
        ty: FluxType::Bool,
    },
];

static MESSAGE: TableSchema = TableSchema {
    name: "ChatMessage",
    columns: MESSAGE_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

static VEHICLE_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "x",
        ty: FluxType::F64,
    },
    ColumnSchema {
        name: "y",
        ty: FluxType::F64,
    },
];

static VEHICLE: TableSchema = TableSchema {
    name: "Vehicle",
    columns: VEHICLE_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[IndexSchema::Spatial {
        kind: SpatialKind::QuadTree,
        columns: &[1, 2],
    }],
    visibility: VisibilityRule::PublicAll,
};

fn schema() -> Schema {
    Schema::from_tables([&MESSAGE, &VEHICLE]).unwrap()
}

/// A committed ChatMessage row to evaluate plans against.
fn message_row(channel: u32, sender: &str, sent_at: i64, priority: Option<i32>) -> Row {
    let schema = schema();
    let store = MemStore::new(&schema).unwrap();
    let table = store.table_id("ChatMessage").unwrap();
    let mut tx = store.begin();
    tx.insert(
        table,
        vec![
            RowValue::U64(0),
            RowValue::U32(channel),
            RowValue::Str(sender.into()),
            RowValue::Timestamp(fluxum_core::types::Timestamp::from_micros(sent_at)),
            RowValue::Optional(priority.map(|p| Box::new(RowValue::I32(p)))),
            RowValue::Bool(false),
        ],
    )
    .unwrap();
    tx.commit().unwrap();
    let snapshot = store.snapshot();
    let rows: Vec<Row> = snapshot.scan(table).unwrap().cloned().collect();
    rows.into_iter().next().unwrap()
}

fn plan_of(sql: &str) -> CompiledPlan {
    compile(&schema(), sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn reject(sql: &str) -> String {
    match compile(&schema(), sql) {
        Err(e) => {
            assert!(
                matches!(e.query_code(), Some(c) if (3000..=3999).contains(&c)),
                "{sql} must be an SQL-range error: {e}"
            );
            e.to_string()
        }
        Ok(plan) => panic!("{sql} must be rejected, compiled {plan:?}"),
    }
}

// --- SUB-010: every supported form ----------------------------------------------

#[test]
fn full_table_scan_compiles_with_no_filter() {
    let plan = plan_of("SELECT * FROM ChatMessage");
    assert_eq!(plan.table_ids, vec![TableId::of("ChatMessage")]);
    assert!(plan.filter.is_none());
    assert!(plan.equalities.is_empty());
    assert!(plan.matches(&message_row(1, "ana", 10, None)));
}

#[test]
fn equality_predicate_evaluates_and_exposes_the_pruning_seam() {
    let plan = plan_of("SELECT * FROM ChatMessage WHERE channel = 7");
    assert!(plan.matches(&message_row(7, "ana", 10, None)));
    assert!(!plan.matches(&message_row(8, "ana", 10, None)));
    // SUB-023 seam: the equality is exposed structurally, coerced to the
    // column type (channel is U32).
    assert_eq!(plan.equalities, vec![(1, RowValue::U32(7))]);
}

#[test]
fn in_list_between_and_conjunction_evaluate() {
    let plan = plan_of("SELECT * FROM ChatMessage WHERE channel IN (1, 3, 5)");
    assert!(plan.matches(&message_row(3, "ana", 10, None)));
    assert!(!plan.matches(&message_row(2, "ana", 10, None)));

    let plan = plan_of("SELECT * FROM ChatMessage WHERE sent_at BETWEEN 100 AND 200");
    assert!(plan.matches(&message_row(1, "ana", 100, None)));
    assert!(plan.matches(&message_row(1, "ana", 200, None)));
    assert!(!plan.matches(&message_row(1, "ana", 99, None)));

    let plan =
        plan_of("SELECT * FROM ChatMessage WHERE channel = 2 AND sender = 'bo' AND urgent = FALSE");
    assert!(plan.matches(&message_row(2, "bo", 10, None)));
    assert!(!plan.matches(&message_row(2, "ana", 10, None)));
    assert!(!plan.matches(&message_row(3, "bo", 10, None)));
    assert_eq!(plan.equalities.len(), 3, "every top-level equality exposed");
}

#[test]
fn string_escapes_and_option_columns_evaluate() {
    let plan = plan_of("SELECT * FROM ChatMessage WHERE sender = 'o''brien'");
    assert!(plan.matches(&message_row(1, "o'brien", 10, None)));

    // A literal against Option<i32> matches the Some-wrapped value.
    let plan = plan_of("SELECT * FROM ChatMessage WHERE priority = -2");
    assert!(plan.matches(&message_row(1, "ana", 10, Some(-2))));
    assert!(!plan.matches(&message_row(1, "ana", 10, None)));
}

#[test]
fn order_by_and_limit_land_in_the_initialdata_slots() {
    let plan =
        plan_of("SELECT * FROM ChatMessage WHERE channel = 1 ORDER BY sent_at DESC LIMIT 50");
    let order = plan.order_by.unwrap();
    assert_eq!(order.column, 3, "sent_at ordinal");
    assert!(order.descending);
    assert_eq!(plan.limit, Some(50));
    // SUB-013: the commit-path predicate is unaffected by ORDER BY/LIMIT.
    assert!(plan.matches(&message_row(1, "ana", 10, None)));

    let plan = plan_of("SELECT * FROM ChatMessage ORDER BY sent_at");
    assert!(!plan.order_by.unwrap().descending, "ASC is the default");
    assert_eq!(plan.limit, None);
}

// --- SUB-011: spatial clauses ----------------------------------------------------

#[test]
fn spatial_clauses_compile_against_the_spatial_declaration() {
    let plan = plan_of("SELECT * FROM Vehicle IN REGION (0, 0, 100, 50)");
    match plan.spatial.unwrap() {
        SpatialConstraint::Region(rect) => {
            assert_eq!((rect.x, rect.y, rect.w, rect.h), (0.0, 0.0, 100.0, 50.0));
        }
        other => panic!("expected a region, got {other:?}"),
    }

    let plan = plan_of("SELECT * FROM Vehicle WITHIN RADIUS 25.5 OF (10, -4.25)");
    match plan.spatial.unwrap() {
        SpatialConstraint::Radius { x, y, r } => {
            assert_eq!((x, y, r), (10.0, -4.25, 25.5));
        }
        other => panic!("expected a radius, got {other:?}"),
    }

    // WHERE composes with a spatial clause.
    let plan = plan_of("SELECT * FROM Vehicle WHERE id = 9 IN REGION (0, 0, 1, 1)");
    assert!(plan.spatial.is_some());
    assert_eq!(plan.equalities, vec![(0, RowValue::U64(9))]);
}

#[test]
fn spatial_clauses_require_a_spatial_index_and_finite_parameters() {
    let err = reject("SELECT * FROM ChatMessage IN REGION (0, 0, 1, 1)");
    assert!(err.contains("no spatial index"), "{err}");
    let err = reject("SELECT * FROM Vehicle WITHIN RADIUS -5 OF (0, 0)");
    assert!(err.contains("negative radius"), "{err}");
}

// --- SUB-012: named rejections ----------------------------------------------------

#[test]
fn every_sub012_construct_is_rejected_with_a_named_400() {
    let cases = [
        ("SELECT * FROM ChatMessage JOIN Vehicle", "JOIN"),
        ("SELECT * FROM ChatMessage GROUP BY channel", "GROUP BY"),
        ("SELECT * FROM ChatMessage HAVING channel = 1", "HAVING"),
        ("INSERT INTO ChatMessage", "read-only"),
        ("UPDATE ChatMessage", "read-only"),
        ("DELETE FROM ChatMessage", "read-only"),
        ("WITH cte AS x SELECT * FROM cte", "CTE"),
        (
            "SELECT * FROM ChatMessage WHERE channel = 1 OR channel = 2",
            "AND only",
        ),
        ("SELECT * FROM ChatMessage WHERE NOT channel = 1", "NOT"),
        ("SELECT * FROM ChatMessage WHERE priority = NULL", "NULL"),
    ];
    for (sql, marker) in cases {
        let err = reject(sql);
        assert!(err.contains("unsupported query syntax"), "{sql}: {err}");
        assert!(err.contains(marker), "{sql}: expected `{marker}` in: {err}");
    }
}

#[test]
fn schema_and_type_errors_are_wire_ready_400s() {
    let err = reject("SELECT * FROM Ghost");
    assert!(err.contains("unknown table `Ghost`"), "{err}");
    let err = reject("SELECT * FROM ChatMessage WHERE ghost = 1");
    assert!(err.contains("unknown column `ghost`"), "{err}");
    let err = reject("SELECT * FROM ChatMessage WHERE channel = 'seven'");
    assert!(err.contains("does not inhabit"), "{err}");
    let err = reject("SELECT * FROM ChatMessage WHERE channel = -1");
    assert!(err.contains("does not inhabit"), "{err}");
    let err = reject("SELECT * FROM ChatMessage WHERE urgent BETWEEN 0 AND 1");
    assert!(err.contains("BETWEEN"), "{err}");
    let err = reject("SELECT * FROM ChatMessage ORDER BY ghost");
    assert!(err.contains("unknown column `ghost`"), "{err}");
}

// --- Parser diagnostics for malformed clause shapes --------------------------------

#[test]
fn malformed_clause_shapes_get_named_diagnostics() {
    let err = reject("SELECT * FROM ChatMessage LIMIT 5000000000");
    assert!(err.contains("exceeds the u32 range"), "{err}");

    // A non-integer LIMIT names the offending token (Float / Str display).
    let err = reject("SELECT * FROM ChatMessage LIMIT 1.5");
    assert!(err.contains("`1.5`"), "{err}");
    let err = reject("SELECT * FROM ChatMessage LIMIT 'ten'");
    assert!(err.contains("'ten'"), "{err}");

    // IN-list separator errors name what was found instead.
    let err = reject("SELECT * FROM ChatMessage WHERE channel IN (1 2)");
    assert!(err.contains("expected `,` or `)` in the IN list"), "{err}");

    // A comma where a literal belongs (Comma token display).
    let err = reject("SELECT * FROM ChatMessage WHERE channel = ,");
    assert!(err.contains("expected a literal value, got `,`"), "{err}");

    // Spatial coordinate-list shapes.
    let err = reject("SELECT * FROM Vehicle IN REGION 0, 0, 1, 1");
    assert!(err.contains("expected `(`"), "{err}");
    let err = reject("SELECT * FROM Vehicle IN REGION (0 0, 1, 1)");
    assert!(err.contains("expected `,` between coordinates"), "{err}");
    let err = reject("SELECT * FROM Vehicle IN REGION (0, 0, 1, 1 LIMIT 5");
    assert!(err.contains("expected `)`"), "{err}");
    let err = reject("SELECT * FROM Vehicle WITHIN RADIUS wide OF (0, 0)");
    assert!(err.contains("the radius"), "{err}");
}

#[test]
fn float_literals_compile_and_normalize_canonically() {
    // Vehicle.x is F64: a float equality compiles, evaluates, and its
    // canonical text re-renders the float literal.
    let plan = plan_of("SELECT * FROM Vehicle WHERE x = 1.5");
    assert_eq!(plan.equalities, vec![(1, RowValue::F64(1.5))]);
    assert_eq!(plan.normalized, "SELECT * FROM Vehicle WHERE x = 1.5");
}

// --- SUB-020: normalization + QueryHash -------------------------------------------

#[test]
fn equivalent_texts_normalize_to_one_hash() {
    let a = plan_of("SELECT * FROM ChatMessage WHERE channel = 7 AND sender = 'bo'");
    let b = plan_of("select   *   from ChatMessage\n\twhere channel=7 and sender='bo'");
    assert_eq!(a.normalized, b.normalized);
    assert_eq!(a.query_hash, b.query_hash);
    assert_eq!(
        a.normalized,
        "SELECT * FROM ChatMessage WHERE channel = 7 AND sender = 'bo'"
    );

    // Different queries stay distinct.
    let c = plan_of("SELECT * FROM ChatMessage WHERE channel = 8 AND sender = 'bo'");
    assert_ne!(a.query_hash, c.query_hash);
    // Identifier case is NOT folded — declarations are case-sensitive.
    assert!(compile(&schema(), "SELECT * FROM chatmessage").is_err());
}

#[test]
fn owner_only_tables_compile_a_caller_parameterized_rls_slot() {
    // A public table has no RLS slot; an owner_only table does (T4.3).
    static OWNED_COLS: &[ColumnSchema] = &[
        ColumnSchema {
            name: "id",
            ty: FluxType::U64,
        },
        ColumnSchema {
            name: "owner",
            ty: FluxType::Identity,
        },
    ];
    static OWNED: TableSchema = TableSchema {
        name: "Owned",
        columns: OWNED_COLS,
        primary_key: &[0],
        auto_inc: None,
        access: TableAccess::Public,
        partition_by: None,
        unique: &[],
        indexes: &[],
        visibility: VisibilityRule::OwnerOnly { owner: 1 },
    };
    let owned_schema = Schema::from_tables([&OWNED]).unwrap();

    let public_plan = compile(&schema(), "SELECT * FROM ChatMessage").unwrap();
    assert!(public_plan.rls.is_none(), "PublicAll table: no RLS slot");

    let owned_plan = compile(&owned_schema, "SELECT * FROM Owned").unwrap();
    let rls = owned_plan
        .rls
        .as_ref()
        .expect("owner_only compiles an RLS slot");

    // The compiled closure passes a row only for its owner identity.
    let owner = fluxum_core::types::Identity::from_bytes([9u8; 32]);
    let other = fluxum_core::types::Identity::from_bytes([1u8; 32]);
    let store = MemStore::new(&owned_schema).unwrap();
    let owned_id = store.table_id("Owned").unwrap();
    let mut tx = store.begin();
    tx.insert(owned_id, vec![RowValue::U64(1), RowValue::Identity(owner)])
        .unwrap();
    tx.commit().unwrap();
    let snapshot = store.snapshot();
    let row = snapshot.scan(owned_id).unwrap().next().unwrap();
    assert!(rls(row, &owner), "owner sees their row");
    assert!(!rls(row, &other), "another identity does not");
}

#[test]
fn normalization_covers_spatial_order_and_limit() {
    let a = plan_of("SELECT * FROM Vehicle  WITHIN  RADIUS  25.5  OF ( 10 , -4.25 )");
    let b = plan_of("select * from Vehicle within radius 25.5 of (10, -4.25)");
    assert_eq!(a.query_hash, b.query_hash);

    let a = plan_of("SELECT * FROM ChatMessage ORDER BY sent_at ASC LIMIT 10");
    let b = plan_of("select * from ChatMessage order by sent_at limit 10");
    assert_eq!(
        a.query_hash, b.query_hash,
        "explicit ASC and the default normalize identically"
    );
}
