//! DM-005/DM-007: `public`, `private`, `global` are mutually exclusive.

use fluxum_macros as fluxum;

#[fluxum::table(public, global)]
pub struct Confused {
    #[primary_key]
    pub id: u64,
}

fn main() {}
