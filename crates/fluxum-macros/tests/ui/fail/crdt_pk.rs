//! SPEC-023 DMX-060: a `CrdtText` document has no ordering and cannot be a
//! primary key (or any other key).

use fluxum_core::crdt::CrdtText;
use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Bad {
    #[primary_key]
    pub body: CrdtText,
    pub id: u64,
}

fn main() {}
