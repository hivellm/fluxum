//! DM-004: `#[auto_inc]` requires a `u64` primary-key column.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Narrow {
    #[primary_key]
    #[auto_inc]
    pub id: u32,
    pub value: String,
}

fn main() {}
