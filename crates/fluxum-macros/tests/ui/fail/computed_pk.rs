//! SPEC-022 RV-050: a `#[computed]` column cannot be a primary key.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Bad {
    #[primary_key]
    #[computed(a + 1)]
    pub id: u64,
    pub a: u64,
}

fn main() {}
