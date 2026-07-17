//! SPEC-022 RV-050: `#[computed(expr)]` compiles — arithmetic over siblings
//! and a derivation over an earlier computed column.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[index(btree(total))]
pub struct Line {
    #[primary_key]
    pub id: u64,
    pub qty: u64,
    pub price: u64,
    #[computed(qty * price)]
    pub total: u64,
    #[computed(total + 1)]
    pub total_plus: u64,
}

fn main() {}
