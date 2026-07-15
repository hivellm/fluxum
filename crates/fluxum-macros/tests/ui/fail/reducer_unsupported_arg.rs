//! RED-001: a reducer parameter type outside the argument universe
//! (`ReducerArg`) is a compile error at the reducer declaration.

use std::collections::HashMap;

use fluxum_core::reducer::ReducerContext;
use fluxum_macros as fluxum;

#[fluxum::reducer]
fn configure(ctx: &ReducerContext, settings: HashMap<String, String>) -> Result<(), String> {
    let _ = (ctx, settings);
    Ok(())
}

fn main() {}
