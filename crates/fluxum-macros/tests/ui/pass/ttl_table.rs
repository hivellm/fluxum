//! SPEC-023 DMX-020: `#[ttl(...)]` declarations compile — an absolute
//! `Timestamp` column and the sliding `after` duration form.

use fluxum_core::types::Timestamp;
use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[ttl(expires_at)]
pub struct Session {
    #[primary_key]
    pub id: u64,
    pub expires_at: Timestamp,
}

#[fluxum::table(public)]
#[ttl(after = "30m")]
pub struct RateBucket {
    #[primary_key]
    pub key: u64,
    pub count: u32,
}

fn main() {}
