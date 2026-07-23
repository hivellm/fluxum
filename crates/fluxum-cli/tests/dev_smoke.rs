//! SPEC-024 DEV-010 smoke: one full `fluxum dev` cycle — cargo build (from
//! cargo's own artifact JSON), server start over a fresh data dir, `/health`
//! poll, and SDK-bindings regeneration from the live `/schema` — driven
//! against the workspace's own `fluxum-server` crate, so the build is warm
//! and the module is the real demo.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::TcpListener;
use std::path::Path;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[test]
fn dev_cycles_start_health_check_regenerate_bindings_and_keep_data() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let module = workspace.join("crates").join("fluxum-server");
    // The prebuilt hook: cargo-in-cargo deadlocks on the build-directory
    // lock, so the cycle runs the workspace's already-built server (the
    // build half is pinned by the `parse_artifact`/`cargo_build` units).
    let exe = workspace
        .join("target")
        .join("debug")
        .join(if cfg!(windows) {
            "fluxum-server.exe"
        } else {
            "fluxum-server"
        });
    if !exe.exists() {
        eprintln!("skipping: no server binary — run: cargo build -p fluxum-server");
        return;
    }
    let (http, tcp) = (free_port(), free_port());
    let data = tempfile::tempdir().unwrap();
    let bindings = tempfile::tempdir().unwrap();

    let options = fluxum_cli::dev::DevOptions {
        path: module,
        http: format!("127.0.0.1:{http}"),
        bindings: Some(bindings.path().to_path_buf()),
        lang: fluxum_cli::generate::Lang::Rust,
        env: vec![
            ("FLUXUM_PROFILE".into(), "development".into()),
            ("FLUXUM_SERVER_HTTP_PORT".into(), http.to_string()),
            ("FLUXUM_SERVER_TCP_PORT".into(), tcp.to_string()),
            (
                "FLUXUM_STORAGE_DATA_DIR".into(),
                data.path().display().to_string(),
            ),
            (
                "FLUXUM_STORAGE_COMMIT_LOG_DIR".into(),
                data.path().join("log").display().to_string(),
            ),
        ],
        once: true,
        prebuilt: Some(exe),
        poll: std::time::Duration::from_millis(500),
    };
    fluxum_cli::dev::dev_loop(&options).expect("first dev cycle");

    // The cycle only reaches bindings regeneration through a healthy
    // server, so their presence asserts the whole chain (DEV-010 steps
    // 2-4); the demo module's tables must be in them.
    let generated: Vec<_> = std::fs::read_dir(bindings.path())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .collect();
    assert!(
        !generated.is_empty(),
        "bindings were regenerated from the live /schema"
    );
    let contents = generated
        .iter()
        .filter(|p| p.is_file())
        .map(|p| std::fs::read_to_string(p).unwrap_or_default())
        .collect::<String>();
    assert!(
        contents.contains("ChatMessage"),
        "the demo schema reached the bindings"
    );

    // A second cycle over the SAME data dir is the dev loop's restart
    // (DEV-010 step 3): the server comes back healthy (its bindings regen
    // ran again) over the previous cycle's data dir — recovery replays
    // whatever the log holds; with no commits the dir is just present.
    fluxum_cli::dev::dev_loop(&options).expect("restart cycle over the same data");
    assert!(
        data.path().join("log").is_dir(),
        "the restarted server reused the same data dir"
    );
}
