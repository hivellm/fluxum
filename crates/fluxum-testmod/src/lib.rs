//! Test-only Fluxum module crate (SPEC-001 acceptance 2, task T1.1 item 1.7).
//!
//! Declares a table in a *separate workspace crate* so the `fluxum-macros`
//! integration tests can verify that the link-time registry (DM-040) collects
//! `#[fluxum::table]` declarations across crate boundaries. Never published;
//! not part of the shipped server.
//!
//! Per OQ-1 (see the T1.1 task's `oq1-linktime-registry.md`): a crate that is
//! linked but never referenced is dropped by the linker together with its
//! registrations, so consumers must reference this crate
//! (`use fluxum_testmod as _;`).

use fluxum_core::types::{Identity, Timestamp};
use fluxum_macros as fluxum;

/// Cross-crate registry fixture: an audit-trail table.
#[fluxum::table(public)]
#[index(btree(actor))]
pub struct AuditEvent {
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    pub actor: Identity,
    pub action: String,
    pub at: Timestamp,
}
