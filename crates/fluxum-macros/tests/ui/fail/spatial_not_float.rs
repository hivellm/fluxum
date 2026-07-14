//! DM-032: spatial index columns must be `f32` or `f64`.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[spatial(quadtree(grid_x, grid_y))]
pub struct Sensor {
    #[primary_key]
    pub id: u64,
    pub grid_x: i32,
    pub grid_y: i32,
}

fn main() {}
