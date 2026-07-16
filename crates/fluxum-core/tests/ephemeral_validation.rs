//! SPEC-023 DMX-011 — startup validation of registered `EphemeralDef`s:
//! cleanup metadata on a non-ephemeral table, a non-`ConnectionId` `#[owner]`
//! binding, and a non-positive `expire_after` each abort schema assembly.
//!
//! Own test binary: the defs below are process-global inventory submissions,
//! so they must never share a binary with tests that assemble unrelated
//! schemas containing these table names.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::schema::{
    ColumnSchema, EphemeralDef, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};

const fn table(
    name: &'static str,
    columns: &'static [ColumnSchema],
    access: TableAccess,
) -> TableSchema {
    TableSchema {
        name,
        columns,
        primary_key: &[0],
        auto_inc: None,
        access,
        partition_by: None,
        unique: &[],
        indexes: &[],
        visibility: VisibilityRule::PublicAll,
    }
}

static PLAIN_COLS: &[ColumnSchema] = &[ColumnSchema {
    name: "id",
    ty: FluxType::U64,
}];

static OWNED_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "who",
        ty: FluxType::Str, // NOT ConnectionId
    },
];

// Case 1: cleanup metadata on a durable (non-ephemeral) table.
static NOT_EPHEMERAL: TableSchema = table("EvNotEphemeral", PLAIN_COLS, TableAccess::Public);
fluxum_core::schema::inventory::submit! {
    EphemeralDef {
        table: "EvNotEphemeral",
        owner: None,
        expire_after_us: Some(1_000_000),
    }
}

// Case 2: #[owner] bound to a non-ConnectionId column.
static BAD_OWNER: TableSchema = table("EvBadOwner", OWNED_COLS, TableAccess::Ephemeral);
fluxum_core::schema::inventory::submit! {
    EphemeralDef {
        table: "EvBadOwner",
        owner: Some(1),
        expire_after_us: None,
    }
}

// Case 3: non-positive expire_after.
static BAD_TTL: TableSchema = table("EvBadTtl", PLAIN_COLS, TableAccess::Ephemeral);
fluxum_core::schema::inventory::submit! {
    EphemeralDef {
        table: "EvBadTtl",
        owner: None,
        expire_after_us: Some(0),
    }
}

fn reject(t: &'static TableSchema, needle: &str) {
    let err = match Schema::from_tables([t]) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("schema with `{}` must fail DMX-011 validation", t.name),
    };
    assert!(err.contains(needle), "{err}");
    assert!(err.contains(t.name), "{err}");
}

#[test]
fn cleanup_metadata_on_a_non_ephemeral_table_is_rejected() {
    reject(&NOT_EPHEMERAL, "non-ephemeral table");
}

#[test]
fn owner_binding_must_be_a_connection_id_column() {
    reject(&BAD_OWNER, "ConnectionId");
}

#[test]
fn expire_after_must_be_positive() {
    reject(&BAD_TTL, "expire_after must be positive");
}
