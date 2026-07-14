//! DM-003: `#[primary_key]` field + table-level `primary_key(...)` is invalid.

use fluxum_macros as fluxum;

#[fluxum::table(public, primary_key(grid_x, grid_y))]
pub struct Sensor {
    #[primary_key]
    pub grid_x: i32,
    pub grid_y: i32,
    pub reading: f64,
}

fn main() {}
