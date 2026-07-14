//! DM-002: two `#[primary_key]` fields — composite keys use the table-level
//! argument instead.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Pair {
    #[primary_key]
    pub a: u64,
    #[primary_key]
    pub b: u64,
}

fn main() {}
