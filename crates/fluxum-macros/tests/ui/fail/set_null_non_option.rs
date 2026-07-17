//! SPEC-022 RV-032: `on_delete = set_null` needs an Option-typed column.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Parent {
    #[primary_key]
    pub id: u64,
}

#[fluxum::table(public)]
pub struct Bad {
    #[primary_key]
    pub id: u64,
    #[references(Parent(id), on_delete = set_null)]
    pub parent_id: u64,
}

fn main() {}
