//! fluxum-dst — deterministic simulation testing for the storage/commitlog
//! engine (SPEC-013 §14, T2.7; gate **G2** requirement per TST-134).
//!
//! The harness drives the **real** engine — `MemStore`, `CommitLog`,
//! `CheckpointRepo`, real files, real torn-tail quarantine, real
//! checkpoint+replay recovery — under a seeded, fully deterministic driver:
//!
//! - **Seeded runtime** ([`rng::SimRng`], TST-130/TST-131): every decision —
//!   op mix, commit vs abort, ack points, checkpoint cadence and corruption,
//!   compaction, crash points, and the fault applied at each crash — derives
//!   from the run seed alone, so any failure reproduces from its seed.
//! - **Fault injection** ([`sim`], TST-131): lost fsync (un-acked suffix
//!   drop at an entry boundary), torn writes (mid-frame cuts), bit flips in
//!   log entries, and checkpoint-manifest corruption — the physically
//!   possible `kill -9` disk states under the STG-012 fsync model.
//! - **Model oracle** ([`model::Model`], TST-132): a trivially correct
//!   in-memory shadow checked for acceptance/rejection parity on every
//!   operation, full state equality at every commit and rollback, and
//!   row-set equality after every crash-replay (TST-133, the simulated twin
//!   of the OS-level kill harness in `fluxum-core/tests/crash_kill9.rs`).
//! - **Determinism log** ([`sim::SimReport::trace`], TST-130): each run
//!   emits a chained hash trace; [`sim::run_seed_checked`] executes every
//!   seed twice and fails loudly — "non-determinism detected for seed N at
//!   checkpoint M" — if the traces diverge.
//!
//! Scope: this crate currently determinizes the storage/commitlog layers,
//! whose production code is synchronous file I/O driven from the commit
//! path — the single-threaded seeded driver covers their entire behavior
//! space of interest without an executor seam. The task-scheduling /
//! network-fault simulation surface (message loss, duplication, reordering,
//! partitions) extends this crate when replication lands (SPEC-014, T7.x),
//! as scheduled by TST-134.
//!
//! Run locally (TST-007): `cargo test -p fluxum-dst` for the bounded per-PR
//! run; `FLUXUM_DST_SEEDS=64 FLUXUM_DST_OPS=1500 cargo test -p fluxum-dst
//! --release` for the nightly long run; `FLUXUM_DST_SEED=<n>` to reproduce
//! one failing seed.

pub mod model;
pub mod rng;
pub mod sim;

pub use model::{Model, ModelState};
pub use rng::SimRng;
pub use sim::{SimReport, run_seed, run_seed_checked};
