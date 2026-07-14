//! DM-033: a column set cannot be indexed twice with the same index type.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[index(btree(owner))]
#[index(btree(owner))]
pub struct Task {
    #[primary_key]
    pub id: u64,
    pub owner: u64,
}

fn main() {}
