//! DMX-011: the `#[owner]` binding must be a `ConnectionId` column.

use fluxum_macros as fluxum;

#[fluxum::table(ephemeral)]
pub struct Cursor {
    #[primary_key]
    pub id: u64,
    #[owner]
    pub who: String,
}

fn main() {}
