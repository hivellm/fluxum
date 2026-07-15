//! MIG-020: #[default] carries the backfill value.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Task {
    #[primary_key]
    pub id: u64,
    #[default]
    pub priority: u8,
}

fn main() {}
