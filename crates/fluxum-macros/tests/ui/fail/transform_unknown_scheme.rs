//! CT-030: unknown encryption schemes are a compile error.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Vault {
    #[primary_key]
    pub id: u64,
    #[encrypted(rsa, key = "a")]
    pub secret: Vec<u8>,
}

fn main() {}
