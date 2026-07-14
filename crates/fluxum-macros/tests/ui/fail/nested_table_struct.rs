//! DM-012: nested table-struct columns are rejected — the column type
//! universe is closed; relationships go through EntityId/u64 columns.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Address {
    #[primary_key]
    pub id: u64,
    pub city: String,
}

#[fluxum::table(public)]
pub struct Customer {
    #[primary_key]
    pub id: u64,
    pub address: Address,
}

fn main() {}
