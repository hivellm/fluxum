//! MIG-001/MIG-010: version 1 is the initial schema — migrations start at 2.

use fluxum_macros as fluxum;

#[fluxum::migration(version = 1)]
fn migrate(ctx: &mut fluxum_core::migration::MigrationContext) -> fluxum_core::Result<()> {
    let _ = ctx;
    Ok(())
}

fn main() {}
