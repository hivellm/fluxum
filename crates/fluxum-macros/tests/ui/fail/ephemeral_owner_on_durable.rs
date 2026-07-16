//! DMX-011: `#[owner]` only applies to ephemeral tables.

use fluxum_core::types::ConnectionId;
use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Session {
    #[primary_key]
    #[owner]
    pub conn: ConnectionId,
}

fn main() {}
