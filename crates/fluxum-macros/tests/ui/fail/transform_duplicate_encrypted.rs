//! CT-002: at most one `#[encrypted]` per column.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Vault {
    #[primary_key]
    pub id: u64,
    #[encrypted(ecies, key = "a")]
    #[encrypted(ecies, key = "b")]
    pub secret: Vec<u8>,
}

fn main() {}
