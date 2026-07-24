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
