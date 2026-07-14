//! End-to-end registry tests (T1.1 items 1.4/1.7/1.8): tables declared with
//! `#[fluxum::table]` in this test crate *and* in a second workspace crate
//! (`fluxum-testmod`) are collected at link time by the registry that lives
//! in `fluxum-core` (SPEC-001 acceptance 2). Mechanism per OQ-1: `inventory`
//! — see the T1.1 task's `oq1-linktime-registry.md`.
#![allow(dead_code)]
#![allow(clippy::expect_used)]

use fluxum_core::schema::{
    FluxType, IndexSchema, Schema, SpatialKind, Table, TableAccess, VisibilityRule,
};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_macros as fluxum;
// A crate that is linked but never referenced is dropped by the linker along
// with its registrations (OQ-1); this reference keeps fluxum-testmod's
// cross-crate table declarations alive.
use fluxum_testmod as _;

#[fluxum::table(public)]
pub struct User {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub identity: Identity,
    pub name: String,
}

#[fluxum::table(public)]
pub struct OnlineUser {
    #[primary_key]
    pub identity: Identity,
    pub connection_id: ConnectionId,
    pub connected_at: Timestamp,
}

#[fluxum::table(public)]
#[index(btree(channel, sent_at))]
pub struct ChatMessage {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub sender: Identity,
    pub channel: u32,
    pub content: String,
    pub sent_at: Timestamp,
}

#[fluxum::table(public, partition_by(owner))]
#[index(btree(owner))]
#[visibility(owner_only(owner))]
pub struct Task {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub owner: Identity,
    pub title: String,
    pub done: bool,
}

#[fluxum::table(public, primary_key(grid_x, grid_y))]
#[spatial(quadtree(x, y))]
pub struct Sensor {
    pub grid_x: i32,
    pub grid_y: i32,
    pub x: f32,
    pub y: f32,
    pub reading: f64,
    pub updated_at: Timestamp,
}

#[fluxum::table]
pub struct SessionSecret {
    #[primary_key]
    pub identity: Identity,
    pub secret: Vec<u8>,
}

fn assemble() -> Schema {
    Schema::assemble().unwrap_or_else(|e| panic!("schema must assemble: {e}"))
}

#[test]
fn all_declared_tables_are_collected_at_link_time() {
    let schema = assemble();
    let names: Vec<&str> = schema.tables().map(|t| t.name).collect();
    assert_eq!(
        names,
        [
            "AuditEvent", // declared in fluxum-testmod, not in this crate
            "ChatMessage",
            "OnlineUser",
            "Sensor",
            "SessionSecret",
            "Task",
            "User"
        ]
    );
}

#[test]
fn tables_from_a_second_workspace_crate_are_collected() {
    // SPEC-001 acceptance 2: declarations spanning two workspace crates all
    // appear in the assembled schema.
    let schema = assemble();
    let audit = schema
        .table("AuditEvent")
        .expect("AuditEvent is declared in fluxum-testmod");
    assert_eq!(audit.access, TableAccess::Public);
    assert_eq!(audit.primary_key, &[0u16]);
    assert_eq!(audit.auto_inc, Some(0));
    assert_eq!(audit.indexes, &[IndexSchema::BTree { columns: &[1] }]);
    assert_eq!(audit.columns[1].ty, FluxType::Identity);
}

#[test]
fn user_schema_introspects_per_dm042() {
    let schema = assemble();
    let user = schema.table("User").expect("User registered");
    assert_eq!(user.name, "User");
    assert_eq!(user.primary_key, &[0u16]);
    assert_eq!(user.auto_inc, Some(0));
    assert_eq!(user.access, TableAccess::Public);
    assert_eq!(user.partition_by, None);
    assert_eq!(user.visibility, VisibilityRule::PublicAll);
    let cols: Vec<(&str, FluxType)> = user.columns.iter().map(|c| (c.name, c.ty)).collect();
    assert_eq!(
        cols,
        [
            ("id", FluxType::U64),
            ("identity", FluxType::Identity),
            ("name", FluxType::Str),
        ]
    );
}

#[test]
fn chat_message_has_composite_btree_index() {
    let schema = assemble();
    let chat = schema.table("ChatMessage").expect("registered");
    assert_eq!(chat.indexes, &[IndexSchema::BTree { columns: &[2, 4] }]);
    assert_eq!(chat.columns[2].name, "channel");
    assert_eq!(chat.columns[4].name, "sent_at");
}

#[test]
fn task_partitioning_and_visibility() {
    let schema = assemble();
    let task = schema.table("Task").expect("registered");
    assert_eq!(task.partition_by, Some(1));
    assert_eq!(task.visibility, VisibilityRule::OwnerOnly { owner: 1 });
    assert_eq!(task.indexes, &[IndexSchema::BTree { columns: &[1] }]);
}

#[test]
fn sensor_composite_pk_and_quadtree() {
    let schema = assemble();
    let sensor = schema.table("Sensor").expect("registered");
    assert_eq!(sensor.primary_key, &[0u16, 1]);
    assert_eq!(sensor.auto_inc, None);
    assert_eq!(
        sensor.indexes,
        &[IndexSchema::Spatial {
            kind: SpatialKind::QuadTree,
            columns: &[2, 3],
        }]
    );
}

#[test]
fn session_secret_is_private_by_default() {
    let schema = assemble();
    let secret = schema.table("SessionSecret").expect("registered");
    assert_eq!(secret.access, TableAccess::Private);
    assert_eq!(secret.columns[1].ty, FluxType::Bytes);
}

#[test]
fn table_trait_exposes_schema_and_typed_pk() {
    assert_eq!(User::SCHEMA.name, "User");

    let user = User {
        id: 7,
        identity: Identity::from_token("alice"),
        name: "Alice".into(),
    };
    assert_eq!(user.primary_key(), 7);

    let online = OnlineUser {
        identity: Identity::from_token("alice"),
        connection_id: ConnectionId::new(1),
        connected_at: Timestamp::from_micros(42),
    };
    assert_eq!(online.primary_key(), Identity::from_token("alice"));

    let sensor = Sensor {
        grid_x: -2,
        grid_y: 9,
        x: 0.5,
        y: 1.5,
        reading: 101.25,
        updated_at: Timestamp::from_micros(0),
    };
    // Composite PK is a tuple in `primary_key(...)` argument order (DM-003).
    assert_eq!(sensor.primary_key(), (-2, 9));
}
