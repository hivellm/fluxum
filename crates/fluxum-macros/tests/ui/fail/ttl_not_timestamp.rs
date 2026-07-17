//! SPEC-023 DMX-020: `#[ttl(col)]` requires the column to be a `Timestamp` —
//! a non-timestamp column is a compile error.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[ttl(expires_at)]
pub struct Session {
    #[primary_key]
    pub id: u64,
    pub expires_at: u64,
}

fn main() {}
