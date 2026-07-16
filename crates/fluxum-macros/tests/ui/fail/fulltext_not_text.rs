//! SPEC-019 FTS-002: a `#[fulltext]` column must be text — a numeric column
//! is a compile error.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[fulltext(score)]
pub struct Doc {
    #[primary_key]
    pub id: u64,
    pub score: u32,
}

fn main() {}
