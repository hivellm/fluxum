//! SPEC-023 DMX-040: a `BlobRef` column compiles — the row carries the
//! 32-byte content-hash reference, never the payload bytes.

use fluxum_core::types::BlobRef;
use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct User {
    #[primary_key]
    pub id: u64,
    pub avatar: BlobRef,
    pub cover: Option<BlobRef>,
}

fn main() {}
