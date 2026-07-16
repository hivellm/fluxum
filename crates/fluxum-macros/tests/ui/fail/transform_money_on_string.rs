//! CT-021: `#[normalize(money)]` requires a `Decimal` column.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Order {
    #[primary_key]
    pub id: u64,
    #[normalize(money, scale = 2)]
    pub amount: String,
}

fn main() {}
