//! FR-05 / HWA-012/013 — the REAL boot path (`boot::assemble`, what the
//! reference binary and every embedder run) probes the hardware and installs
//! the derived effective config, so `GET /health` on a production server
//! reports the probe inputs and every `auto` value with its provenance.
//! Container-awareness (cgroup limits winning over host totals) lives in the
//! probe itself (fluxum-core hw::cgroup); this pins that boot actually runs
//! it — the gap that made /health report no `config` block from the shipped
//! binary.
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[tokio::test(flavor = "multi_thread")]
async fn assemble_installs_the_probe_derived_effective_config() {
    // The demo module provides the link-time schema, as in the binary.
    fluxum_demo::link();
    let dir = tempfile::tempdir().unwrap();
    let mut config = fluxum_core::config::Config::default();
    config.storage.data_dir = dir.path().into();
    config.storage.commit_log_dir = dir.path().join("log");
    config.storage.checkpoint_dir = dir.path().join("checkpoints");
    config.storage.page_dir = dir.path().join("pages");
    config.auth.provider = fluxum_core::config::AuthProvider::None;

    let ctx = fluxum_server::boot::assemble(&config).unwrap();

    let effective = ctx
        .effective_config()
        .expect("HWA-013: boot installs the effective config");
    assert!(effective["worker_threads"]["value"].as_u64().unwrap() >= 1);
    assert!(effective["shards"]["value"].as_u64().unwrap() >= 1);
    // TIER-001/002: the derived budget never lands below the 128 MiB floor.
    assert!(effective["memory_budget_bytes"]["value"].as_u64().unwrap() >= 128 << 20);
    // The probe inputs ride along, so an operator can see what the
    // derivation saw (HWA-013).
    assert!(effective["hardware"]["logical_cores"].as_u64().unwrap() >= 1);
}

/// SPEC-015 TIER-021: the cold-tier page directory is a *cache* — recovery
/// rebuilds every tree from the newest checkpoint plus the commit log. Page
/// files written by an older page format (a version bump) or a half-written
/// run must therefore never strand a durable database behind a disposable
/// tier: boot discards the directory and rebuilds it.
#[tokio::test(flavor = "multi_thread")]
async fn boot_discards_a_page_directory_it_cannot_read() {
    fluxum_demo::link();
    let dir = tempfile::tempdir().unwrap();
    let pages = dir.path().join("pages");
    let mut config = fluxum_core::config::Config::default();
    config.storage.data_dir = dir.path().into();
    config.storage.commit_log_dir = dir.path().join("log");
    config.storage.checkpoint_dir = dir.path().join("checkpoints");
    config.storage.page_dir = pages.clone();
    config.auth.provider = fluxum_core::config::AuthProvider::None;

    // A page file this build cannot read: a superblock from another format
    // (the shape a version bump leaves behind), in the on-disk layout
    // `page_dir/shard-<id>/table-<id>.pages`.
    let shard = pages.join("shard-0");
    std::fs::create_dir_all(&shard).unwrap();
    let mut superblock = vec![0u8; 32];
    superblock[..4].copy_from_slice(&0x584D_5546u32.to_le_bytes()); // magic
    superblock[4..8].copy_from_slice(&0xFFFF_FFFFu32.to_le_bytes()); // version
    std::fs::write(shard.join("table-1634948758.pages"), &superblock).unwrap();

    // Boot succeeds and the unreadable tier is gone, recreated empty.
    let ctx = fluxum_server::boot::assemble(&config).unwrap();
    assert!(
        ctx.effective_config().is_some(),
        "boot completed over a discarded page tier"
    );
    assert!(
        pages.is_dir(),
        "the page tier is recreated, not left missing"
    );
}

/// Setting `storage.data_dir` alone must place the whole database there:
/// the dev-loop shape (a temp data dir, everything else defaulted) used to
/// scatter pages and checkpoints into the process working directory, where
/// no volume mount or backup would ever find them.
#[tokio::test(flavor = "multi_thread")]
async fn data_dir_alone_places_the_whole_database() {
    fluxum_demo::link();
    let dir = tempfile::tempdir().unwrap();
    let mut config = fluxum_core::config::Config::default();
    config.storage.data_dir = dir.path().into();
    config.auth.provider = fluxum_core::config::AuthProvider::None;
    config.resolve_storage_dirs();

    assert_eq!(config.storage.page_dir, dir.path().join("pages"));
    assert_eq!(config.storage.commit_log_dir, dir.path().join("log"));
    assert_eq!(
        config.storage.checkpoint_dir,
        dir.path().join("checkpoints")
    );

    let ctx = fluxum_server::boot::assemble(&config).unwrap();
    // The binary installs the running config right after assembly; do the
    // same so the `/health` rendering below sees it.
    ctx.install_config(None, config.clone(), None);

    // The tier is created where the operator pointed the database…
    assert!(dir.path().join("pages").is_dir());
    // …and `/health` reports the resolved locations with their provenance,
    // so "where is my commit log" has an answer at runtime.
    let storage = ctx.storage_paths().expect("the installed config renders");
    assert_eq!(
        storage["storage.page_dir"]["value"].as_str().unwrap(),
        dir.path().join("pages").display().to_string()
    );
    assert_eq!(
        storage["storage.page_dir"]["source"].as_str().unwrap(),
        "derived"
    );
}
