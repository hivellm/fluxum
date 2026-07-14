//! DM-033: a table cannot declare both spatial index families.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
#[spatial(quadtree(x, y))]
#[spatial(rtree(min_x, min_y, max_x, max_y))]
pub struct Geo {
    #[primary_key]
    pub id: u64,
    pub x: f32,
    pub y: f32,
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

fn main() {}
