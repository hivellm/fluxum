//! Phase8 1.8 — the dashboard e2e smoke: spawn the REAL `fluxum-server`
//! binary (the SDK-conformance-runner pattern) and sweep the console
//! contract over plain HTTP — the shell + CSP, the boot document, and every
//! JSON surface each console view drives. This is the "does the deployed
//! binary actually serve the dashboard" gate; browser-level behavior is
//! covered interactively (Playwright) and by the per-view suites.
//!
//! Skips loudly when the binary is absent, like the other spawn suites.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// A port pair away from the 15800 defaults, so a dev server can coexist.
const HTTP_PORT: u16 = 15850;
const TCP_PORT: u16 = 15851;

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

/// Kills the spawned server even when an assertion panics.
struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Resp {
    status: u16,
    headers: String,
    body: String,
}

/// One blocking HTTP/1.1 request. Reads exactly the header block plus its
/// `Content-Length` body — the server keeps connections alive, so reading
/// to EOF would burn the whole read timeout on every call.
fn request(method: &str, path: &str, body: Option<&str>) -> Resp {
    let mut stream = TcpStream::connect(("127.0.0.1", HTTP_PORT)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let body = body.unwrap_or("");
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).unwrap();
    let mut raw = Vec::new();
    let headers_end = loop {
        if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        let mut chunk = [0u8; 4096];
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break raw.len(),
            Ok(n) => raw.extend_from_slice(&chunk[..n]),
        }
    };
    let head = String::from_utf8_lossy(&raw[..headers_end]).into_owned();
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let content_length: usize = head
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().to_owned())
        })
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut body = raw.split_off(headers_end.min(raw.len()));
    while body.len() < content_length {
        let mut chunk = [0u8; 4096];
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => break,
            Ok(n) => body.extend_from_slice(&chunk[..n]),
        }
    }
    Resp {
        status,
        headers: head,
        body: String::from_utf8_lossy(&body).into_owned(),
    }
}

fn json(resp: &Resp) -> serde_json::Value {
    serde_json::from_str(&resp.body).unwrap_or(serde_json::Value::Null)
}

#[test]
fn the_deployed_binary_serves_the_whole_console_contract() {
    if !server_binary().exists() {
        eprintln!("skipping: no server binary — run: cargo build -p fluxum-server");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let child = std::process::Command::new(server_binary())
        .env("FLUXUM_PROFILE", "development")
        .env("FLUXUM_SERVER_HTTP_PORT", HTTP_PORT.to_string())
        .env("FLUXUM_SERVER_TCP_PORT", TCP_PORT.to_string())
        .env("FLUXUM_STORAGE_DATA_DIR", dir.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn fluxum-server");
    let _guard = Child(child);

    // Boot: poll /health until the shard is ready.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if TcpStream::connect(("127.0.0.1", HTTP_PORT)).is_ok()
            && request("GET", "/health", None).status == 200
        {
            break;
        }
        assert!(Instant::now() < deadline, "server never became healthy");
        std::thread::sleep(Duration::from_millis(200));
    }

    // The shell: served with the self-contained CSP, one complete document.
    let shell = request("GET", "/console", None);
    assert_eq!(shell.status, 200);
    assert!(
        shell.headers.contains("Content-Security-Policy"),
        "CSP header"
    );
    assert!(
        shell.headers.contains("default-src 'none'"),
        "no external origins: {}",
        shell.headers
    );
    assert!(shell.body.contains("Fluxum"), "the console page");
    assert!(shell.body.ends_with("</html>\n"), "complete document");

    // The boot document: development profile → console open.
    let state = request("GET", "/console/state", None);
    assert_eq!(
        json(&state)["payload"]["console_open"],
        true,
        "{}",
        state.body
    );

    // Schema (Data/Schema/Reducers views boot from this).
    let schema = request("GET", "/schema", None);
    let doc = json(&schema);
    assert_eq!(doc["success"], true);
    assert!(
        doc["payload"]["tables"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["name"] == "Task"),
        "the demo module's Task table is present"
    );
    assert!(!doc["payload"]["reducers"].as_array().unwrap().is_empty());

    // Row edit + query round-trip (the Data view's write path).
    let owner = "0".repeat(64);
    let row = format!(
        r#"{{"table":"Task","op":"upsert","row":{{"id":0,"owner":"{owner}","title":"smoke","done":false}}}}"#
    );
    let edit = request("POST", "/rows", Some(&row));
    assert_eq!(edit.status, 200, "{}", edit.body);
    let queried = request(
        "POST",
        "/query",
        Some(r#"{"sql":"SELECT * FROM Task LIMIT 10"}"#),
    );
    assert!(queried.body.contains("smoke"), "{}", queried.body);

    // Explain (Query view).
    let explain = request(
        "POST",
        "/query/explain",
        Some(r#"{"sql":"SELECT * FROM Task LIMIT 1"}"#),
    );
    assert_eq!(
        json(&explain)["payload"]["table"],
        "Task",
        "{}",
        explain.body
    );

    // Reducer invoke (Reducers view).
    let invoked = request("POST", "/reducer/send_chat", Some(r#"[1, "smoke"]"#));
    assert_eq!(
        json(&invoked)["payload"]["committed"],
        true,
        "{}",
        invoked.body
    );

    // Metrics + sessions + bans (Overview/Metrics/Sessions views).
    let metrics = request("GET", "/metrics", None);
    assert!(metrics.body.contains("fluxum_table_rows"), "metrics text");
    let sessions = request("GET", "/sessions", None);
    assert_eq!(json(&sessions)["success"], true, "{}", sessions.body);
    let banned = request(
        "POST",
        "/bans",
        Some(r#"{"entry":"203.0.113.9","ttl_secs":60}"#),
    );
    assert_eq!(banned.status, 200, "{}", banned.body);
    let listed = request("GET", "/bans", None);
    assert!(listed.body.contains("203.0.113.9"), "{}", listed.body);
    let lifted = request("DELETE", "/bans/203.0.113.9", None);
    assert_eq!(lifted.status, 200, "{}", lifted.body);

    // Ops: checkpoint, then a backup round-trip against the live layout.
    let checkpoint = request("POST", "/checkpoint", Some("{}"));
    assert_eq!(checkpoint.status, 200, "{}", checkpoint.body);
    let out = dir.path().join("smoke-backup");
    let body = format!(
        r#"{{"out": {}}}"#,
        serde_json::json!(out.display().to_string())
    );
    let backup = request("POST", "/backup", Some(&body));
    assert_eq!(backup.status, 200, "{}", backup.body);
    let body = format!(
        r#"{{"dir": {}}}"#,
        serde_json::json!(out.display().to_string())
    );
    let verify = request("POST", "/backup/verify", Some(&body));
    assert_eq!(json(&verify)["payload"]["ok"], true, "{}", verify.body);

    // Drain last: new /rpc work refused 503, and /health still ANSWERS —
    // with 503 (OBS-060: shutting_down → error), which is exactly how a
    // load balancer sees the shard leave rotation.
    let drain = request("POST", "/drain", Some("{}"));
    assert_eq!(json(&drain)["payload"]["draining"], true, "{}", drain.body);
    let refused = request("POST", "/rpc", Some("x"));
    assert_eq!(refused.status, 503, "draining refuses new rpc work");
    let health = request("GET", "/health", None);
    assert_eq!(health.status, 503, "health answers 503 while draining");
    assert!(
        health.body.contains("\"status\""),
        "the health document still renders: {}",
        health.body
    );
}
