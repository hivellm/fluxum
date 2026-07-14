//! The canonical SPEC-001 example schema (task T1.1 item 1.5):
//! User / OnlineUser / ChatMessage / Task / Sensor, plus a private table.

use fluxum_core::schema::{Table, TableAccess};
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
use fluxum_macros as fluxum;

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
    pub id: u64, // caller passes 0; runtime assigns
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

#[fluxum::table] // private by default (DM-005)
pub struct SessionSecret {
    #[primary_key]
    pub identity: Identity,
    pub secret: Vec<u8>,
}

fn main() {
    assert_eq!(User::SCHEMA.name, "User");
    assert_eq!(User::SCHEMA.auto_inc, Some(0));
    assert_eq!(Sensor::SCHEMA.primary_key, &[0u16, 1]);
    assert_eq!(SessionSecret::SCHEMA.access, TableAccess::Private);

    let sensor = Sensor {
        grid_x: 3,
        grid_y: -4,
        x: 1.5,
        y: 2.5,
        reading: 20.25,
        updated_at: Timestamp::from_micros(0),
    };
    assert_eq!(sensor.primary_key(), (3, -4));
}
