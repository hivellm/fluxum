//! CT-021..023: unknown normalize kinds are a compile error.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Order {
    #[primary_key]
    pub id: u64,
    #[normalize(hex)]
    pub blob: Vec<u8>,
}

fn main() {}
