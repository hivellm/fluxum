//! `fluxum migrate --plan` — preview a schema migration without touching
//! anything (SPEC-024 DEV-041, FR-138).
//!
//! The schema lives in the module BINARY (link-time registry), so the plan
//! must be computed by the module's own executable: this command builds the
//! module crate (`cargo build`, same machinery as `fluxum dev`) and runs
//! the produced binary with `FLUXUM_MIGRATE_PLAN=1` — the seam
//! `fluxum_server::boot::serve` honors before opening any transport. The
//! child prints the diff with each entry's auto-apply classification
//! (safe/additive `[auto]` vs `[BLOCKS]` requires-migration) and exits
//! without serving; nothing on disk is mutated (the plan path only READS
//! the data directory — the commit log is never opened for writing and
//! `__schema_meta__` is never written).
//!
//! Exit codes, propagated to the caller: `0` — the next real boot proceeds
//! (up to date, first boot, auto-applies, or pending migration steps);
//! `3` — the next boot REFUSES (MIG-022 incompatible change or MIG-023
//! missing version bump); `1` — the plan itself failed (build error,
//! unreadable data directory).

use std::path::Path;
use std::process::Command;

use crate::{CliError, dev};

/// Build the module at `dir` and run its binary in plan mode. Returns the
/// child's exit code (see the module docs for the code contract).
pub fn migrate_plan(dir: &Path) -> Result<i32, CliError> {
    let exe = dev::cargo_build(dir).map_err(CliError::Response)?;
    let status = Command::new(&exe)
        .current_dir(dir)
        .env("FLUXUM_MIGRATE_PLAN", "1")
        .status()
        .map_err(|e| CliError::Connect(format!("cannot run {}: {e}", exe.display())))?;
    Ok(status.code().unwrap_or(1))
}
