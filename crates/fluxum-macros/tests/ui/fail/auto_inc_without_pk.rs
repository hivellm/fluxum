//! DM-004: `#[auto_inc]` is only valid on the `#[primary_key]` field.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Counter {
    #[primary_key]
    pub id: u64,
    #[auto_inc]
    pub serial: u64,
}

fn main() {}
