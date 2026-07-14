//! DM-001: tables must be structs with named fields.

use fluxum_macros as fluxum;

#[fluxum::table(public)]
pub struct Point(pub f32, pub f32);

fn main() {}
