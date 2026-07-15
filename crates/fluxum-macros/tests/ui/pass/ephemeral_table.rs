//! SPEC-023 DMX-010: `#[fluxum::table(ephemeral)]` declares a memory-only,
//! client-visible table.

use fluxum_core::types::ConnectionId;
use fluxum_macros as fluxum;

#[fluxum::table(ephemeral)]
pub struct Cursor {
    #[primary_key]
    pub conn: ConnectionId,
    pub x: i32,
    pub y: i32,
}

fn main() {}
