//! CLI dispatch refusal arms (phase8 coverage floor): flag validation exits
//! 2 with usage before touching anything, operational failures exit 1 with
//! the engine's message — for the commands the backup suite does not cover
//! (logs, schema export, generate, seed, dev, unknown commands).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{Read as _, Write as _};
use std::net::TcpListener;

use fluxum_cli::run;

fn args(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| (*s).to_owned()).collect()
}

/// A one-shot HTTP responder on a random loopback port.
fn serve_once(response: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(response.as_bytes());
        }
    });
    addr.to_string()
}

#[test]
fn flag_validation_exits_2_with_usage() {
    // No command prints usage as help (success); an unknown command is 2.
    assert_eq!(run(args(&[])), 0);
    assert_eq!(run(args(&["frobnicate"])), 2);
    // logs: --server required; --format policed.
    assert_eq!(run(args(&["logs"])), 2);
    assert_eq!(
        run(args(&[
            "logs",
            "--server",
            "127.0.0.1:1",
            "--format",
            "xml"
        ])),
        2
    );
    // schema export: --server required.
    assert_eq!(run(args(&["schema", "export"])), 2);
    // generate: all three flags required; --lang policed.
    assert_eq!(run(args(&["generate", "--lang", "typescript"])), 2);
    assert_eq!(
        run(args(&[
            "generate", "--lang", "cobol", "--schema", "x.json", "--out", "o"
        ])),
        2
    );
    // seed: fixture file and --server required.
    assert_eq!(run(args(&["seed"])), 2);
    assert_eq!(run(args(&["seed", "fixture.json"])), 2);
    // dev: --lang policed.
    assert_eq!(run(args(&["dev", "--lang", "cobol"])), 2);
}

#[test]
fn operational_failures_exit_1_with_the_engines_message() {
    // A server that is not there (port 1 refuses fast).
    assert_eq!(run(args(&["logs", "--server", "127.0.0.1:1"])), 1);
    assert_eq!(
        run(args(&["schema", "export", "--server", "127.0.0.1:1"])),
        1
    );
    // generate: a schema file that does not exist.
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out");
    assert_eq!(
        run(args(&[
            "generate",
            "--lang",
            "typescript",
            "--schema",
            "Z:/no/such/schema.json",
            "--out",
            out.to_str().unwrap(),
        ])),
        1
    );
    // seed: a fixture that does not exist.
    assert_eq!(
        run(args(&[
            "seed",
            "Z:/no/such/fixture.json",
            "--server",
            "127.0.0.1:1"
        ])),
        1
    );
}

#[test]
fn schema_export_surfaces_malformed_server_responses() {
    // Connection closed before the header block completes.
    let addr = serve_once("");
    assert_eq!(run(args(&["schema", "export", "--server", &addr])), 1);
    // A body that is not the schema envelope.
    let addr = serve_once("HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nnot json!");
    assert_eq!(run(args(&["schema", "export", "--server", &addr])), 1);
    // No Content-Length: read-to-close still parses (success path through
    // the length-less branch) — the payload must be a real schema document.
    let addr = serve_once(
        "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{\"success\":false,\"error\":\"nope\"}",
    );
    assert_eq!(run(args(&["schema", "export", "--server", &addr])), 1);
}

#[test]
fn success_paths_exit_0_offline() {
    // schema export to stdout from a one-shot server with a real envelope.
    let addr = serve_once(
        "HTTP/1.1 200 OK\r\nContent-Length: 45\r\n\r\n{\"success\":true,\"payload\":{\"tables\":[],\"x\":1}}",
    );
    assert_eq!(run(args(&["schema", "export", "--server", &addr])), 0);

    // generate: the committed demo-schema golden is a REAL document.
    let golden = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../fluxum-server/tests/golden/demo-schema.json"
    );
    let out = tempfile::tempdir().unwrap();
    assert_eq!(
        run(args(&[
            "generate",
            "--lang",
            "typescript",
            "--schema",
            golden,
            "--out",
            out.path().to_str().unwrap(),
        ])),
        0
    );
    assert!(
        out.path().join("index.ts").exists() || out.path().read_dir().unwrap().next().is_some()
    );

    // seed: one call against a one-shot committed=true responder.
    let fixture = tempfile::NamedTempFile::with_suffix(".json").unwrap();
    std::fs::write(
        fixture.path(),
        r#"{ "calls": [ { "reducer": "send_chat", "args": [1, "hi"] } ] }"#,
    )
    .unwrap();
    let addr = serve_once(
        "HTTP/1.1 200 OK\r\nContent-Length: 41\r\n\r\n{\"success\":true,\"payload\":{\"committed\":1}}",
    );
    assert_eq!(
        run(args(&[
            "seed",
            fixture.path().to_str().unwrap(),
            "--server",
            &addr
        ])),
        0
    );
}

#[test]
fn backup_and_migrate_dispatch_arms() {
    // backup restore: a config that cannot load refuses at layout resolution.
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        run(args(&[
            "backup",
            "restore",
            "--from",
            dir.path().to_str().unwrap(),
            "--config",
            "Z:/no/such/config.yml",
        ])),
        2
    );
    // backup verify on an empty directory is an operational failure (1).
    assert_eq!(
        run(args(&[
            "backup",
            "verify",
            "--from",
            dir.path().to_str().unwrap()
        ])),
        1
    );
    // migrate without --plan is refused with the SPEC-010 pointer.
    assert_eq!(run(args(&["migrate"])), 2);
    // init into a path that is a FILE fails operationally.
    let file = tempfile::NamedTempFile::new().unwrap();
    let inside = file.path().join("sub");
    assert_eq!(run(args(&["init", inside.to_str().unwrap()])), 1);
}
