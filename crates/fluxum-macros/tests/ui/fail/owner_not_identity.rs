//! DM-060: the `owner_only` column must be of type `Identity`.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[visibility(owner_only(owner))]
pub struct Task {
    #[primary_key]
    pub id: u64,
    pub owner: u64,
}

fn main() {}
