//! Fluxum CLI library: the subcommand implementations backing the `fluxum`
//! binary.
//!
//! # `fluxum schema export`
//!
//! Fetches `GET /schema` from a running server and writes it out (SPEC-011,
//! FR-81) — the machine-readable module contract every SDK generator
//! consumes, and the artifact the T6.1 **module API freeze** is pinned
//! against.
//!
//! The exported document is byte-identical to the `/schema` payload: the
//! admin envelope is unwrapped and the payload re-serialized with
//! `serde_json`, whose maps are sorted, so two exports of the same schema are
//! the same bytes. That is what makes a committed golden file a usable freeze
//! gate.
//!
//! HTTP here is hand-rolled over `std::net::TcpStream` rather than pulled
//! from a client crate: the request is one unconditional `GET` against an
//! operator-supplied address, and Fluxum ships as a single binary with no
//! runtime dependencies — a full HTTP stack would be a large dependency for
//! one request.

use std::io::{Read, Write};
use std::net::TcpStream;

/// What a CLI subcommand can fail with.
#[derive(Debug)]
pub enum CliError {
    /// The server address could not be parsed or reached.
    Connect(String),
    /// The server answered, but not with a usable `/schema` document.
    Response(String),
    /// Writing the output file failed.
    Io(std::io::Error),
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(m) => write!(f, "cannot reach the server: {m}"),
            Self::Response(m) => write!(f, "unusable /schema response: {m}"),
            Self::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for CliError {}

impl From<std::io::Error> for CliError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// Strip an optional scheme and trailing slash, leaving `host:port`.
fn host_port(server: &str) -> &str {
    server
        .trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
}

/// Fetch the schema document from `server` (`host:port`, optionally
/// `http://`-prefixed) and return it as canonical pretty JSON with a
/// trailing newline — exactly what [`export_schema`] writes.
pub fn fetch_schema(server: &str) -> Result<String, CliError> {
    let addr = host_port(server);
    let mut stream =
        TcpStream::connect(addr).map_err(|e| CliError::Connect(format!("{addr}: {e}")))?;
    let request = format!(
        "GET /schema HTTP/1.1\r\nHost: {addr}\r\nAccept: application/json\r\n\
         Connection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes())?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw)?;

    let text = String::from_utf8_lossy(&raw);
    let (head, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| CliError::Response("no header/body separator".into()))?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| CliError::Response("no status line".into()))?;
    if status != "200" {
        return Err(CliError::Response(format!("server answered {status}")));
    }
    canonical_schema(body)
}

/// Unwrap the RPC-052 admin envelope and re-serialize the payload
/// canonically. A bare (un-enveloped) document is accepted too, so the
/// exporter is not coupled to the envelope surviving unchanged.
pub fn canonical_schema(body: &str) -> Result<String, CliError> {
    let value: serde_json::Value =
        serde_json::from_str(body.trim()).map_err(|e| CliError::Response(e.to_string()))?;
    if value.get("success") == Some(&serde_json::Value::Bool(false)) {
        let message = value
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown error");
        return Err(CliError::Response(message.to_owned()));
    }
    let payload = value.get("payload").unwrap_or(&value);
    if !payload.is_object() {
        return Err(CliError::Response("payload is not an object".into()));
    }
    let mut out =
        serde_json::to_string_pretty(payload).map_err(|e| CliError::Response(e.to_string()))?;
    out.push('\n');
    Ok(out)
}

/// `fluxum schema export --server <url> [--out <file>]`: fetch the schema and
/// write it to `out` (or return it for stdout when `out` is `None`).
pub fn export_schema(server: &str, out: Option<&std::path::Path>) -> Result<String, CliError> {
    let document = fetch_schema(server)?;
    if let Some(path) = out {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, document.as_bytes())?;
    }
    Ok(document)
}

/// The `fluxum` binary's usage text.
pub const USAGE: &str = "\
fluxum — Fluxum command-line tool

USAGE:
    fluxum schema export --server <host:port> [--out <file>]

COMMANDS:
    schema export    Fetch GET /schema from a running server and write the
                     module contract (SPEC-011). Prints to stdout without
                     --out. The output is canonical, so committing it gives
                     a byte-for-byte API-freeze gate.
";

/// Run the CLI over `args` (without the program name). Returns the process
/// exit code; anything printed has already been printed.
pub fn run<I, S>(args: I) -> i32
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args: Vec<String> = args.into_iter().map(|a| a.as_ref().to_owned()).collect();
    match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["schema", "export", rest @ ..] => {
            let Some(server) = flag(rest, "--server") else {
                eprintln!("schema export: --server <host:port> is required\n\n{USAGE}");
                return 2;
            };
            let out = flag(rest, "--out").map(std::path::PathBuf::from);
            match export_schema(&server, out.as_deref()) {
                Ok(document) => {
                    if out.is_none() {
                        print!("{document}");
                    }
                    0
                }
                Err(e) => {
                    eprintln!("schema export failed: {e}");
                    1
                }
            }
        }
        ["--help" | "-h"] | [] => {
            print!("{USAGE}");
            0
        }
        other => {
            eprintln!("unknown command: {}\n\n{USAGE}", other.join(" "));
            2
        }
    }
}

/// The value of `--name value` in `args`.
fn flag(args: &[&str], name: &str) -> Option<String> {
    args.iter()
        .position(|a| *a == name)
        .and_then(|i| args.get(i + 1))
        .map(|v| (*v).to_owned())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::io::Write as _;
    use std::net::TcpListener;

    use super::*;

    /// A one-shot HTTP server answering `GET /schema` with `body`.
    fn serve_once(status: &'static str, body: &'static str) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            // Read the request line (enough to let the client finish writing).
            let mut buf = [0u8; 1024];
            let _ = std::io::Read::read(&mut stream, &mut buf);
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes());
        });
        addr
    }

    #[test]
    fn export_unwraps_the_envelope_and_canonicalizes() {
        // Keys deliberately out of order in the response.
        let addr = serve_once(
            "200 OK",
            r#"{"success":true,"payload":{"tables":[],"schema_version":1}}"#,
        );
        let document = fetch_schema(&addr).unwrap();
        // Canonical: the payload only, sorted keys, pretty, trailing newline.
        assert_eq!(
            document, "{\n  \"schema_version\": 1,\n  \"tables\": []\n}\n",
            "the export is the payload, canonically serialized"
        );
    }

    #[test]
    fn export_accepts_an_http_prefixed_server() {
        let addr = serve_once("200 OK", r#"{"success":true,"payload":{"a":1}}"#);
        let document = fetch_schema(&format!("http://{addr}/")).unwrap();
        assert!(document.contains("\"a\": 1"), "{document}");
    }

    #[test]
    fn a_non_200_is_reported_not_written() {
        let addr = serve_once("503 Service Unavailable", "{}");
        let err = fetch_schema(&addr).unwrap_err();
        assert!(err.to_string().contains("503"), "{err}");
    }

    #[test]
    fn an_error_envelope_surfaces_its_message() {
        let err = canonical_schema(r#"{"success":false,"error":"shard draining"}"#).unwrap_err();
        assert!(err.to_string().contains("shard draining"), "{err}");
    }

    #[test]
    fn a_bare_document_is_accepted_too() {
        let out = canonical_schema(r#"{"schema_version":1}"#).unwrap();
        assert_eq!(out, "{\n  \"schema_version\": 1\n}\n");
    }

    #[test]
    fn export_writes_the_file_and_creates_its_directory() {
        let addr = serve_once(
            "200 OK",
            r#"{"success":true,"payload":{"schema_version":1}}"#,
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("schema.json");
        let document = export_schema(&addr, Some(&path)).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), document);
    }

    #[test]
    fn an_unreachable_server_is_a_connect_error() {
        // Bind then drop: nothing listens on that port.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let err = fetch_schema(&addr).unwrap_err();
        assert!(matches!(err, CliError::Connect(_)), "{err}");
    }

    #[test]
    fn usage_is_shown_for_help_and_unknown_commands() {
        assert_eq!(run(["--help"]), 0);
        assert_eq!(run(Vec::<String>::new()), 0);
        assert_eq!(run(["wat"]), 2);
        // `schema export` without --server is a usage error, not a panic.
        assert_eq!(run(["schema", "export"]), 2);
    }
}
