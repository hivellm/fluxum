//! SPEC-022 RV-030: `#[not_null]` is only meaningful on an Option column.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Bad {
    #[primary_key]
    pub id: u64,
    #[not_null]
    pub name: String,
}

fn main() {}
