//! Index/partition/visibility references must name existing columns (DM-040).

use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[index(btree(owner_id))]
pub struct Task {
    #[primary_key]
    pub id: u64,
    pub owner: u64,
}

fn main() {}
