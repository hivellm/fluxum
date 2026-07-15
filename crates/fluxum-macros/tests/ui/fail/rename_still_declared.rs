//! MIG-020: a #[rename(from)] source must be the old, removed column name.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Sensor {
    #[primary_key]
    pub id: u64,
    pub reading: f64,
    #[rename(from = "reading")]
    pub value: f64,
}

fn main() {}
