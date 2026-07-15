//! SPEC-023 DMX-031: a `#[derive(FluxType)]` enum/struct supports equality
//! only — it cannot be a primary key, partition key, unique, or index key.

use fluxum_macros as fluxum;

#[derive(fluxum::FluxType)]
pub enum Kind {
    A,
    B,
}

#[fluxum::table(public)]
pub struct Thing {
    #[primary_key]
    pub kind: Kind,
}

fn main() {}
