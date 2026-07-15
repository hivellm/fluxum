//! MIG-010: a migration must declare its target version.

use fluxum_macros as fluxum;

#[fluxum::migration]
fn migrate(ctx: &mut fluxum_core::migration::MigrationContext) -> fluxum_core::Result<()> {
    let _ = ctx;
    Ok(())
}

fn main() {}
