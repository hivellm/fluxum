//! DM-004: composite primary keys do not support `#[auto_inc]`.

use fluxum_macros as fluxum;

#[fluxum::table(public, primary_key(grid_x, grid_y))]
pub struct Sensor {
    pub grid_x: i32,
    pub grid_y: i32,
    #[auto_inc]
    pub counter: u64,
}

fn main() {}
