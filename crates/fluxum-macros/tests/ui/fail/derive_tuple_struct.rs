//! SPEC-023 DMX-030: `#[derive(FluxType)]` on a struct requires named fields.

use fluxum_macros as fluxum;

#[derive(fluxum::FluxType)]
pub struct Point(i32, i32);

fn main() {}
