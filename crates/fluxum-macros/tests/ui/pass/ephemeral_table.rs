//! SPEC-023 DMX-010/011: `#[fluxum::table(ephemeral)]` declares a memory-only,
//! client-visible table; `expire_after` gives rows a TTL and `#[owner]` binds
//! them to a `ConnectionId` for disconnect cleanup.

use fluxum_core::types::ConnectionId;
use fluxum_macros as fluxum;

#[fluxum::table(ephemeral, expire_after = "10s")]
pub struct Cursor {
    #[primary_key]
    #[owner]
    pub conn: ConnectionId,
    pub x: i32,
    pub y: i32,
}

#[fluxum::table(ephemeral)]
pub struct Typing {
    #[primary_key]
    pub conn: ConnectionId,
    pub channel: u32,
}

fn main() {}
