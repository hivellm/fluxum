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

pub mod dev;
pub mod generate;
pub mod init;
pub mod logs;

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

/// One `GET` against a running server, returning the raw response body.
/// The one-request-per-connection shape every subcommand shares — see the
/// module docs for why HTTP is hand-rolled here.
///
/// The body is read to `Content-Length` when the server states one (a real
/// server keeps the connection alive whatever `Connection: close` asked
/// for), falling back to read-until-EOF for servers that do close.
pub fn fetch_path(server: &str, path: &str) -> Result<String, CliError> {
    let addr = host_port(server);
    let mut stream =
        TcpStream::connect(addr).map_err(|e| CliError::Connect(format!("{addr}: {e}")))?;
    // A stuck server must fail the command, not hang it.
    let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(10)));
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nAccept: application/json\r\n\
         Connection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes())?;

    let mut raw = Vec::new();
    let mut chunk = [0u8; 4096];
    let (head_len, body_start) = loop {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Err(CliError::Response("connection closed before headers".into()));
        }
        raw.extend_from_slice(&chunk[..n]);
        if let Some(split) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break (split, split + 4);
        }
    };
    let head = String::from_utf8_lossy(&raw[..head_len]).into_owned();
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or_else(|| CliError::Response("no status line".into()))?
        .to_owned();
    let content_length: Option<usize> = head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("content-length")
            .then(|| value.trim().parse().ok())
            .flatten()
    });
    match content_length {
        Some(length) => {
            while raw.len() < body_start + length {
                let n = stream.read(&mut chunk)?;
                if n == 0 {
                    break; // truncated — surface what arrived
                }
                raw.extend_from_slice(&chunk[..n]);
            }
        }
        None => {
            // No stated length: the server closes when done.
            loop {
                match stream.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => raw.extend_from_slice(&chunk[..n]),
                }
            }
        }
    }
    if status != "200" {
        return Err(CliError::Response(format!("server answered {status}")));
    }
    Ok(String::from_utf8_lossy(&raw[body_start..]).into_owned())
}

/// Fetch the schema document from `server` (`host:port`, optionally
/// `http://`-prefixed) and return it as canonical pretty JSON with a
/// trailing newline — exactly what [`export_schema`] writes.
pub fn fetch_schema(server: &str) -> Result<String, CliError> {
    canonical_schema(&fetch_path(server, "/schema")?)
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
    fluxum init <dir> [--name <crate>] [--template notes] [--fluxum-path <checkout>]
    fluxum dev [--path <dir>] [--http <host:port>] [--bindings <dir>] [--lang <lang>]
    fluxum logs --server <host:port> [-f] [--level <level>] [--format json|pretty]
    fluxum schema export --server <host:port> [--out <file>]
    fluxum generate --lang <lang> --schema <url_or_file> --out <dir>

COMMANDS:
    init             Scaffold a runnable Fluxum application (schema +
                     reducers + config + client instructions) that boots
                     with `cargo run` (SPEC-024 DEV-011). --fluxum-path
                     points the dependencies at a Fluxum checkout.

    dev              The edit-save-see loop (DEV-010): watch the module
                     crate, rebuild on save, restart the server with data
                     intact (snapshot + commit-log replay), regenerate
                     bindings into --bindings, and stream the merged logs.
                     A failed build keeps the previous server running.

    logs             Stream the server's structured logs from GET /logs
                     (DEV-012). -f follows; --level warn narrows;
                     --format pretty renders one-liners (default: json).

    schema export    Fetch GET /schema from a running server and write the
                     module contract (SPEC-011). Prints to stdout without
                     --out. The output is canonical, so committing it gives
                     a byte-for-byte API-freeze gate.

    generate         Emit typed client bindings from a schema. --schema takes
                     a running server (host:port) or a saved schema.json;
                     both produce identical bytes, so bindings can be
                     committed and diffed in review.

                     --lang    typescript | ts | rust | rs
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
        ["init", rest @ ..] => {
            let Some(dir) = rest.first().filter(|a| !a.starts_with("--")) else {
                eprintln!("init: a target directory is required\n\n{USAGE}");
                return 2;
            };
            let options = init::InitOptions {
                name: flag(rest, "--name"),
                fluxum_path: flag(rest, "--fluxum-path"),
                template: flag(rest, "--template").unwrap_or_else(|| "notes".to_owned()),
            };
            let dir = std::path::PathBuf::from(dir);
            match init::scaffold(&dir, &options) {
                Ok(written) => {
                    for rel in written {
                        println!("{}", dir.join(rel).display());
                    }
                    println!("\nscaffolded — next: cd {} && cargo run", dir.display());
                    0
                }
                Err(e) => {
                    eprintln!("init failed: {e}");
                    1
                }
            }
        }
        ["dev", rest @ ..] => {
            let lang = match flag(rest, "--lang") {
                None => generate::Lang::Rust,
                Some(text) => match generate::Lang::parse(&text) {
                    Some(lang) => lang,
                    None => {
                        eprintln!("dev: unknown --lang `{text}` (supported: typescript, rust)");
                        return 2;
                    }
                },
            };
            let options = dev::DevOptions {
                path: flag(rest, "--path").map_or_else(|| ".".into(), Into::into),
                http: flag(rest, "--http").unwrap_or_else(|| "127.0.0.1:15800".to_owned()),
                bindings: flag(rest, "--bindings").map(Into::into),
                lang,
                ..dev::DevOptions::default()
            };
            match dev::dev_loop(&options) {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("dev failed: {e}");
                    1
                }
            }
        }
        ["logs", rest @ ..] => {
            let Some(server) = flag(rest, "--server") else {
                eprintln!("logs: --server <host:port> is required\n\n{USAGE}");
                return 2;
            };
            let level = match flag(rest, "--level") {
                None => None,
                Some(text) => match logs::Level::parse(&text) {
                    Some(level) => Some(level),
                    None => {
                        eprintln!(
                            "logs: unknown --level `{text}` (trace|debug|info|warn|error)"
                        );
                        return 2;
                    }
                },
            };
            let format = match flag(rest, "--format") {
                None => logs::Format::Json,
                Some(text) => match logs::Format::parse(&text) {
                    Some(format) => format,
                    None => {
                        eprintln!("logs: unknown --format `{text}` (json|pretty)");
                        return 2;
                    }
                },
            };
            let options = logs::LogsOptions {
                server,
                follow: rest.contains(&"-f") || rest.contains(&"--follow"),
                level,
                format,
            };
            match logs::stream(&options, &mut std::io::stdout()) {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("logs failed: {e}");
                    1
                }
            }
        }
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
        ["generate", rest @ ..] => {
            let (Some(lang), Some(schema), Some(out)) = (
                flag(rest, "--lang"),
                flag(rest, "--schema"),
                flag(rest, "--out"),
            ) else {
                eprintln!("generate: --lang, --schema and --out are all required\n\n{USAGE}");
                return 2;
            };
            let Some(lang) = generate::Lang::parse(&lang) else {
                eprintln!("generate: unknown --lang `{lang}` (supported: typescript, rust)");
                return 2;
            };
            match generate::load_schema(&schema)
                .and_then(|doc| generate::generate(lang, &doc))
                .and_then(|files| generate::write_files(std::path::Path::new(&out), &files))
            {
                Ok(written) => {
                    for path in written {
                        println!("{}", path.display());
                    }
                    0
                }
                Err(e) => {
                    eprintln!("generate failed: {e}");
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

    #[test]
    fn inner_loop_commands_validate_their_flags() {
        // `init` needs a directory; `logs` needs --server; every enum flag
        // rejects garbage with a usage error (2), never a panic.
        assert_eq!(run(["init"]), 2);
        assert_eq!(run(["init", "--name", "x"]), 2, "a flag is not a directory");
        assert_eq!(run(["logs"]), 2);
        assert_eq!(run(["logs", "--server", "h:1", "--level", "loud"]), 2);
        assert_eq!(run(["logs", "--server", "h:1", "--format", "xml"]), 2);
        assert_eq!(run(["dev", "--lang", "cobol"]), 2);
        // `dev` on a directory without a crate is a real (1) failure that
        // names `fluxum init`.
        let empty = tempfile::tempdir().unwrap();
        let path = empty.path().display().to_string();
        assert_eq!(run(["dev", "--path", &path]), 1);
    }

    #[test]
    fn init_through_the_cli_scaffolds_and_reports_the_files() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("demo-app").display().to_string();
        assert_eq!(
            run(["init", &target, "--fluxum-path", "/checkout/fluxum"]),
            0
        );
        assert!(std::path::Path::new(&target).join("src/main.rs").exists());
        // Re-running refuses to clobber the crate it just made.
        assert_eq!(run(["init", &target]), 1);
    }
}
