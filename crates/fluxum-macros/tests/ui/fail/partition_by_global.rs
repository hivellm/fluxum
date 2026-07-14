//! DM-008: `partition_by` must not be combined with `global`.

use fluxum_macros as fluxum;

#[fluxum::table(global, partition_by(region))]
pub struct Config {
    #[primary_key]
    pub id: u64,
    pub region: u32,
}

fn main() {}
