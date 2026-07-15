//! SPEC-023 DMX-030: a `#[derive(FluxType)]` enum needs at least one variant.

use fluxum_macros as fluxum;

#[derive(fluxum::FluxType)]
pub enum Empty {}

fn main() {}
