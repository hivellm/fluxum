//! `boot::migration_plan` (SPEC-024 DEV-041): the read-only preview a
//! `fluxum migrate --plan` run reaches through `FLUXUM_MIGRATE_PLAN=1`.
//!
//! The core diff/verdict matrix is pinned in
//! `fluxum-core/tests/schema_migration.rs`; what belongs to the SERVER is
//! the assembly seam — link-time registry + `__schema_meta__` + the
//! configured data directories — and above all the side-effect contract: a
//! plan against a directory that does not exist reports first boot and
//! CREATES NOTHING.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};

use fluxum_core::config::Config;
use fluxum_core::migration::PlanVerdict;

#[test]
fn a_fresh_data_dir_plans_as_first_boot_and_creates_nothing() {
    fluxum_demo::link();
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        storage: fluxum_core::config::StorageConfig {
            commit_log_dir: dir.path().join("log"),
            checkpoint_dir: dir.path().join("snapshots"),
            ..Config::default().storage
        },
        ..Config::default()
    };

    let plan = fluxum_server::boot::migration_plan(&config).unwrap();
    assert_eq!(plan.verdict, PlanVerdict::FirstBoot);
    assert!(!plan.refuses());
    let rendered = plan.render();
    assert!(rendered.contains("first boot"), "{rendered}");

    // The whole point of a PLAN: nothing was created, nothing was written.
    assert!(
        !dir.path().join("log").exists(),
        "plan must not create the commit-log dir"
    );
    assert!(
        !dir.path().join("snapshots").exists(),
        "plan must not create the checkpoint dir"
    );
}

fn server_binary() -> PathBuf {
    let name = if cfg!(windows) {
        "fluxum-server.exe"
    } else {
        "fluxum-server"
    };
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/debug")
        .join(name)
}

#[test]
fn the_binary_flag_prints_the_plan_and_exits_clean() {
    // What `fluxum migrate --plan` ultimately runs. Skips without a built
    // binary, loudly, like the other spawn-based suites.
    if !server_binary().exists() {
        eprintln!("skipping: no server binary — run: cargo build -p fluxum-server");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(server_binary())
        .arg("--migrate-plan")
        .env("FLUXUM_PROFILE", "development")
        .env("FLUXUM_STORAGE_DATA_DIR", dir.path())
        .env("FLUXUM_STORAGE_COMMIT_LOG_DIR", dir.path().join("log"))
        .env(
            "FLUXUM_STORAGE_CHECKPOINT_DIR",
            dir.path().join("snapshots"),
        )
        .output()
        .expect("run fluxum-server --migrate-plan");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "exit 0 for a boot that proceeds: {stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(stdout.contains("schema migration plan"), "{stdout}");
    assert!(stdout.contains("first boot"), "{stdout}");
    assert!(
        !dir.path().join("log").exists(),
        "the flag run must not create the data dir either"
    );
}
