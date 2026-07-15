//! SPEC-023 DMX-030: `#[derive(FluxType)]` does not support generic types.

use fluxum_macros as fluxum;

#[derive(fluxum::FluxType)]
pub struct Wrapper<T> {
    pub inner: T,
}

fn main() {}
