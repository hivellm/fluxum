//! SPEC-019 FTS-002: an `#[encrypted]` column cannot also be `#[fulltext]` —
//! ciphertext is not analyzable (the column is a protected index column,
//! CT-013).

use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[fulltext(body)]
pub struct Doc {
    #[primary_key]
    pub id: u64,
    #[encrypted(ecies, key = "docs")]
    pub body: String,
}

fn main() {}
