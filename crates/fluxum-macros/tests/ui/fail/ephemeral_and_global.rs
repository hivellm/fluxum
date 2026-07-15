//! SPEC-023 DMX-012: an ephemeral table is never global/replicated — the two
//! access kinds are mutually exclusive.

use fluxum_macros as fluxum;

#[fluxum::table(ephemeral, global)]
pub struct Cursor {
    #[primary_key]
    pub conn: u32,
}

fn main() {}
