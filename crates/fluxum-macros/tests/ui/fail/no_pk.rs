//! DM-002: every table has exactly one primary key.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Orphan {
    pub value: u64,
}

fn main() {}
