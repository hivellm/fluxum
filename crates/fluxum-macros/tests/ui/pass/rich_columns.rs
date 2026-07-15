//! SPEC-023 DMX-030: `#[derive(FluxType)]` enums and nested structs are valid
//! `#[fluxum::table]` columns.

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
    pub tags: Vec<Status>,
    pub maybe: Option<Point>,
}

fn main() {}
