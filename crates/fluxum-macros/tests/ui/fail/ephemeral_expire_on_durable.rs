//! DMX-011: `expire_after` only applies to ephemeral tables.

use fluxum_macros as fluxum;

#[fluxum::table(public, expire_after = "10s")]
pub struct Session {
    #[primary_key]
    pub id: u64,
}

fn main() {}
