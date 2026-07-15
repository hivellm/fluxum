//! RED-031: `ReadOnlyTxHandle` has no write methods — a `#[fluxum::view]`
//! that attempts to write MUST fail to compile.

use fluxum_core::reducer::ViewContext;
use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[derive(Debug, Clone)]
pub struct Task {
    #[primary_key]
    pub id: u64,
    pub done: bool,
}

#[fluxum::view]
fn complete_everything(ctx: &ViewContext) -> u64 {
    // Views are read-only: there is no `insert` (nor `upsert`/`delete`) on
    // ReadOnlyTxHandle, by design.
    ctx.tx
        .insert(Task {
            id: 1,
            done: true,
        })
        .unwrap();
    1
}

fn main() {}
