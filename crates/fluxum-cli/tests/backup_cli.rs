//! `fluxum backup` CLI dispatch (SPEC-014 REP-060..070): flag validation
//! exits 2 with usage, operational failures exit 1 with the engine's own
//! message, and the layout resolution honors `--config` and `--data-dir`.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::Write as _;
use std::net::TcpListener;

use fluxum_cli::backup::DataLayout;
use fluxum_cli::run;

fn args(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| (*s).to_owned()).collect()
}

#[test]
fn flag_validation_exits_2_before_touching_anything() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().to_str().unwrap().to_owned();
    let with_data = |list: &[&str]| {
        let mut v = args(list);
        v.extend(args(&["--data-dir", &data]));
        v
    };
    // create: --out is required.
    assert_eq!(run(with_data(&["backup", "create"])), 2);
    // create: --fresh-checkpoint needs --server.
    assert_eq!(
        run(with_data(&[
            "backup",
            "create",
            "--out",
            "x",
            "--fresh-checkpoint"
        ])),
        2
    );
    // verify/restore: --from is required. (verify needs NO layout at all —
    // it must work on a machine with no server config.)
    assert_eq!(run(args(&["backup", "verify"])), 2);
    assert_eq!(run(with_data(&["backup", "restore"])), 2);
    // restore: the two PITR targets are mutually exclusive (REP-070).
    assert_eq!(
        run(with_data(&[
            "backup",
            "restore",
            "--from",
            "x",
            "--to-tx-id",
            "5",
            "--to-timestamp",
            "5"
        ])),
        2
    );
    // restore: malformed targets.
    assert_eq!(
        run(with_data(&[
            "backup", "restore", "--from", "x", "--to-tx-id", "soon"
        ])),
        2
    );
    assert_eq!(
        run(with_data(&[
            "backup",
            "restore",
            "--from",
            "x",
            "--to-timestamp",
            "yesterday"
        ])),
        2
    );
    // An unknown subcommand is not a backup command at all.
    assert_eq!(run(args(&["backup", "prune"])), 2);
    // A --config that does not load resolves no layout.
    assert_eq!(
        run(args(&[
            "backup",
            "create",
            "--out",
            "x",
            "--config",
            "no-such-config.yml"
        ])),
        2
    );
}

#[test]
fn operational_failures_exit_1_with_the_engine_message() {
    let dir = tempfile::tempdir().unwrap();
    let empty = dir.path().join("empty-data");
    std::fs::create_dir_all(&empty).unwrap();

    // create over an empty layout: "nothing to back up".
    let out = dir.path().join("out");
    assert_eq!(
        run(args(&[
            "backup",
            "create",
            "--out",
            out.to_str().unwrap(),
            "--data-dir",
            empty.to_str().unwrap(),
        ])),
        1
    );
    // verify of a directory with no manifest.
    assert_eq!(
        run(args(&[
            "backup",
            "verify",
            "--from",
            dir.path().to_str().unwrap()
        ])),
        1
    );
    // restore from a missing backup.
    assert_eq!(
        run(args(&[
            "backup",
            "restore",
            "--from",
            dir.path().join("nope").to_str().unwrap(),
            "--data-dir",
            empty.to_str().unwrap(),
        ])),
        1
    );
}

#[test]
fn data_layout_resolves_config_data_dir_and_defaults() {
    // --data-dir derives the server's default layout beneath it.
    let dir = tempfile::tempdir().unwrap();
    let layout = DataLayout::resolve(None, Some(dir.path())).unwrap();
    assert_eq!(layout.log_dir, dir.path().join("log"));
    assert_eq!(layout.checkpoint_dir, dir.path().join("checkpoints"));
    assert_eq!(layout.archive_dir, dir.path().join("archive"));

    // --config loads the real config (development profile needs no secret).
    let config = dir.path().join("config.yml");
    std::fs::write(
        &config,
        "profile: development\nstorage:\n  commit_log_dir: /x/log\n  checkpoint_dir: /x/ckpt\n\
         replication:\n  archive:\n    dir: /x/arch\n",
    )
    .unwrap();
    let layout = DataLayout::resolve(Some(&config), None).unwrap();
    assert_eq!(layout.log_dir, std::path::PathBuf::from("/x/log"));
    assert_eq!(layout.checkpoint_dir, std::path::PathBuf::from("/x/ckpt"));
    assert_eq!(layout.archive_dir, std::path::PathBuf::from("/x/arch"));

    // A config that does not parse is an error, not a default.
    std::fs::write(&config, "storage: [not, a, map]\n").unwrap();
    assert!(DataLayout::resolve(Some(&config), None).is_err());
}

/// `--fresh-checkpoint` posts to the server before creating; the reported
/// coverage is printed and create proceeds (here into an empty layout, whose
/// engine error exits 1 — the request path itself is what this pins).
#[test]
fn fresh_checkpoint_posts_to_the_server_first() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0u8; 2048];
        let _ = std::io::Read::read(&mut stream, &mut buf);
        let request = String::from_utf8_lossy(&buf).into_owned();
        let body = r#"{"success":true,"payload":{"fresh":true,"last_tx_id":41}}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).unwrap();
        request
    });

    let dir = tempfile::tempdir().unwrap();
    let code = run(args(&[
        "backup",
        "create",
        "--out",
        dir.path().join("out").to_str().unwrap(),
        "--data-dir",
        dir.path().join("empty").to_str().unwrap(),
        "--fresh-checkpoint",
        "--server",
        &addr,
    ]));
    // The checkpoint request reached the server as POST /checkpoint...
    let request = handle.join().unwrap();
    assert!(request.starts_with("POST /checkpoint"), "{request}");
    // ...and the create itself then failed on the empty layout (exit 1).
    assert_eq!(code, 1);
}
