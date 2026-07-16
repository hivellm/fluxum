//! SPEC-017 CT-001: every column-transform attribute compiles, including a
//! declared (not-yet-executed) `#[encrypted]` column.

use fluxum_core::types::{Decimal, Identity, Timestamp};
use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Payment {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub owner: Identity,
    #[normalize(money, scale = 2, currency = "USD")]
    pub amount: Decimal,
    #[normalize(datetime)]
    pub at: Timestamp,
    #[normalize(string, form = nfc, case = lower, trim = true)]
    pub memo: String,
    #[encrypted(ecies, key = "payment_key")]
    #[masked(null)]
    #[column_grant(select = "auditor")]
    pub card: Vec<u8>,
    #[signed(ed25519, by = server)]
    pub total: i64,
}

fn main() {}
