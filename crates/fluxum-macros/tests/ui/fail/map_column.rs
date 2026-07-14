//! DM-012: `HashMap`/`BTreeMap` are not valid column types.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Bag {
    #[primary_key]
    pub id: u64,
    pub items: std::collections::HashMap<String, u32>,
}

fn main() {}
