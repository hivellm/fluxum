//! CT-013: `#[encrypted]` never applies to a primary-key column.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Vault {
    #[primary_key]
    #[encrypted(ecies, key = "a")]
    pub id: u64,
}

fn main() {}
