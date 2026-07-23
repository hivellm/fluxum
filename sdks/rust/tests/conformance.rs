//! The Rust runner for the shared SDK conformance corpus
//! (SPEC-013 TST-052; `tests/conformance/` at the repo root).
//!
//! Like the TypeScript runner, this is an INTERPRETER over the SAME corpus —
//! it reads the same `corpus.json` and `scenarios/*.json` and must observe the
//! same results, which is the whole point of a language-agnostic corpus. All
//! scenarios run (SDK-050 T6.4 exit test), including `reconnect-resync`: the
//! blocking `Connection` auto-reconnects, resubscribes and reconciles
//! (SDK-047), so a `restart_server` step is survivable.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use fluxum_sdk::protocol::FluxBinReader;
use fluxum_sdk::{Connection, TableSchema};
use serde_json::Value;

// --- Locating the corpus and the server binary ------------------------------

fn repo_root() -> PathBuf {
    // CARGO_MANIFEST_DIR is sdks/rust; the repo root is two up.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

fn corpus_dir() -> PathBuf {
    repo_root().join("tests/conformance")
}

fn server_binary() -> PathBuf {
    let name = if cfg!(windows) {
        "fluxum-server.exe"
    } else {
        "fluxum-server"
    };
    repo_root().join("target/debug").join(name)
}

// --- A spawned server, one per scenario -------------------------------------

struct Server {
    child: Child,
    tcp_url: String,
    http_url: String,
    http_port: u16,
    tcp_port: u16,
    data_dir: PathBuf,
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn wait_for_port(port: u16, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "server did not bind {port}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

impl Server {
    fn start(label: &str) -> Self {
        let http_port = free_port();
        let tcp_port = free_port();
        let data_dir =
            std::env::temp_dir().join(format!("fluxum-conf-{label}-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&data_dir);

        let child = Self::launch(http_port, tcp_port, &data_dir);
        Server {
            child,
            tcp_url: format!("fluxum://127.0.0.1:{tcp_port}"),
            http_url: format!("http://127.0.0.1:{http_port}"),
            http_port,
            tcp_port,
            data_dir,
        }
    }

    fn launch(http_port: u16, tcp_port: u16, data_dir: &Path) -> Child {
        let child = Command::new(server_binary())
            .env("FLUXUM_PROFILE", "development")
            .env("FLUXUM_SERVER_HTTP_PORT", http_port.to_string())
            .env("FLUXUM_SERVER_TCP_PORT", tcp_port.to_string())
            .env("FLUXUM_STORAGE_DATA_DIR", data_dir)
            .env("FLUXUM_STORAGE_COMMIT_LOG_DIR", data_dir.join("log"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn fluxum-server");
        wait_for_port(tcp_port, Duration::from_secs(20));
        wait_for_port(http_port, Duration::from_secs(20));
        child
    }

    /// Crash-and-recover: kill the process and boot a fresh one on the SAME
    /// ports over the SAME data dir, so recovery (STG-030) replays the commit
    /// log and reconnecting clients find the server where they left it.
    fn restart(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        self.child = Self::launch(self.http_port, self.tcp_port, &self.data_dir);
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// --- The corpus schema and canonical value decoding -------------------------

struct Corpus {
    /// table → (column name, FluxBIN type) in declaration order.
    tables: HashMap<String, Vec<(String, String)>>,
    /// table → primary-key column name.
    pk: HashMap<String, String>,
    scenarios: Vec<String>,
}

fn load_corpus() -> Corpus {
    let doc: Value =
        serde_json::from_str(&std::fs::read_to_string(corpus_dir().join("corpus.json")).unwrap())
            .unwrap();
    let mut tables = HashMap::new();
    let mut pk = HashMap::new();
    for (name, spec) in doc["tables"].as_object().unwrap() {
        let cols = spec["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                let pair = c.as_array().unwrap();
                (
                    pair[0].as_str().unwrap().to_owned(),
                    pair[1].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        tables.insert(name.clone(), cols);
        pk.insert(
            name.clone(),
            spec["primary_key"].as_str().unwrap().to_owned(),
        );
    }
    let scenarios = doc["scenarios"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s.as_str().unwrap().to_owned())
        .collect();
    Corpus {
        tables,
        pk,
        scenarios,
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Decode a FluxBIN row into canonical comparison values (corpus README):
/// 64-bit → decimal string, Identity/ConnectionId → hex, others native.
fn canonical_row(cols: &[(String, String)], bytes: &[u8]) -> HashMap<String, Value> {
    let mut reader = FluxBinReader::new(bytes);
    let mut row = HashMap::new();
    for (name, ty) in cols {
        let value = match ty.as_str() {
            "Bool" => Value::Bool(reader.read_bool().unwrap()),
            "U8" => Value::from(reader.read_u8().unwrap()),
            "U16" => Value::from(reader.read_u16().unwrap()),
            "U32" => Value::from(reader.read_u32().unwrap()),
            "I8" => Value::from(reader.read_i8().unwrap()),
            "I16" => Value::from(reader.read_i16().unwrap()),
            "I32" => Value::from(reader.read_i32().unwrap()),
            "U64" => Value::from(reader.read_u64().unwrap().to_string()),
            "I64" => Value::from(reader.read_i64().unwrap().to_string()),
            "EntityId" => Value::from(reader.read_entity_id().unwrap().to_string()),
            "Timestamp" => Value::from(reader.read_timestamp().unwrap().to_string()),
            "F32" => Value::from(reader.read_f32().unwrap()),
            "F64" => Value::from(reader.read_f64().unwrap()),
            "Str" => Value::from(reader.read_str().unwrap().to_owned()),
            "Bytes" => Value::from(hex(reader.read_bytes().unwrap())),
            "Identity" => Value::from(hex(&reader.read_identity().unwrap())),
            "ConnectionId" => Value::from(hex(&reader.read_connection_id().unwrap().to_le_bytes())),
            other => panic!("corpus type {other} not handled by the Rust runner"),
        };
        row.insert(name.clone(), value);
    }
    row
}

/// A table's primary-key projections, derived from the manifest.
fn table_schema(name: &str, cols: &[(String, String)], pk_col: &str) -> TableSchema {
    let pk_index = cols.iter().position(|(c, _)| c == pk_col).unwrap();
    let types: Vec<String> = cols.iter().map(|(_, t)| t.clone()).collect();
    let pk_type = types[pk_index].clone();
    let types_for_row = types.clone();

    TableSchema {
        name: name.to_owned(),
        pk_of_row: Box::new(move |row| {
            // The PK bytes are a stable, collision-free slice: read up to and
            // including the pk column and hash on those raw bytes.
            let mut reader = FluxBinReader::new(row);
            for ty in types_for_row.iter().take(pk_index + 1) {
                skip_value(&mut reader, ty);
            }
            let consumed = row.len() - reader.remaining();
            row[..consumed].to_vec()
        }),
        pk_of_delete: Box::new(move |entry| {
            let mut reader = FluxBinReader::new(entry);
            skip_value(&mut reader, &pk_type);
            let consumed = entry.len() - reader.remaining();
            entry[..consumed].to_vec()
        }),
    }
}

fn skip_value(reader: &mut FluxBinReader<'_>, ty: &str) {
    match ty {
        "Bool" => {
            reader.read_bool().unwrap();
        }
        "U8" => {
            reader.read_u8().unwrap();
        }
        "U16" => {
            reader.read_u16().unwrap();
        }
        "U32" => {
            reader.read_u32().unwrap();
        }
        "I8" => {
            reader.read_i8().unwrap();
        }
        "I16" => {
            reader.read_i16().unwrap();
        }
        "I32" => {
            reader.read_i32().unwrap();
        }
        "U64" => {
            reader.read_u64().unwrap();
        }
        "I64" => {
            reader.read_i64().unwrap();
        }
        "EntityId" => {
            reader.read_entity_id().unwrap();
        }
        "Timestamp" => {
            reader.read_timestamp().unwrap();
        }
        "F32" => {
            reader.read_f32().unwrap();
        }
        "F64" => {
            reader.read_f64().unwrap();
        }
        "Str" => {
            reader.read_str().unwrap();
        }
        "Bytes" => {
            reader.read_bytes().unwrap();
        }
        "Identity" => {
            reader.read_identity().unwrap();
        }
        "ConnectionId" => {
            reader.read_connection_id().unwrap();
        }
        other => panic!("cannot skip corpus type {other}"),
    }
}

// --- The interpreter --------------------------------------------------------

struct Session<'a> {
    corpus: &'a Corpus,
    server: Server,
    /// Which transport this run connects through: the TCP or the HTTP URL.
    transport: Transport,
    clients: HashMap<String, Connection>,
    handles: HashMap<String, Vec<u32>>,
}

/// The corpus runs once per transport (SPEC-006 acceptance 6: the SAME
/// scenarios must observe the SAME results over TCP and Streamable HTTP).
#[derive(Clone, Copy)]
enum Transport {
    Tcp,
    Http,
}

impl Transport {
    fn name(self) -> &'static str {
        match self {
            Transport::Tcp => "tcp",
            Transport::Http => "http",
        }
    }

    fn url(self, server: &Server) -> String {
        match self {
            Transport::Tcp => server.tcp_url.clone(),
            Transport::Http => server.http_url.clone(),
        }
    }
}

impl<'a> Session<'a> {
    fn schemas(&self) -> Vec<TableSchema> {
        self.corpus
            .tables
            .iter()
            .map(|(name, cols)| table_schema(name, cols, &self.corpus.pk[name]))
            .collect()
    }

    fn client(&self, name: &Value) -> &Connection {
        let name = name.as_str().unwrap();
        self.clients
            .get(name)
            .unwrap_or_else(|| panic!("step names client {name:?} before its connect step"))
    }

    /// Resolve "$identity:NAME" / "*"; everything else is literal.
    fn resolve(&self, expected: &Value) -> Value {
        if let Some(name) = expected.as_str().and_then(|s| s.strip_prefix("$identity:")) {
            return Value::from(hex(&self.client(&Value::from(name)).identity()));
        }
        expected.clone()
    }

    fn matches(&self, expected: &Value, actual: &Value) -> bool {
        if expected.as_str() == Some("*") {
            return true;
        }
        &self.resolve(expected) == actual
    }

    fn row_matches(
        &self,
        expected: &serde_json::Map<String, Value>,
        actual: &HashMap<String, Value>,
    ) -> bool {
        expected
            .iter()
            .all(|(col, want)| actual.get(col).is_some_and(|got| self.matches(want, got)))
    }

    fn canonical_rows(&self, client: &Connection, table: &str) -> Vec<HashMap<String, Value>> {
        let cols = &self.corpus.tables[table];
        client
            .rows(table)
            .iter()
            .map(|bytes| canonical_row(cols, bytes))
            .collect()
    }
}

const AWAIT: Duration = Duration::from_secs(5);

fn flux_args(args: &[Value]) -> Vec<fluxum_sdk::protocol::FluxValue> {
    use fluxum_sdk::protocol::FluxValue;
    args.iter()
        .map(|v| match v {
            Value::Bool(b) => FluxValue::Bool(*b),
            Value::Number(n) if n.is_i64() => FluxValue::I64(n.as_i64().unwrap()),
            Value::Number(n) if n.is_u64() => FluxValue::I64(n.as_u64().unwrap() as i64),
            Value::Number(n) => FluxValue::F64(n.as_f64().unwrap()),
            Value::String(s) => FluxValue::Str(s.clone()),
            Value::Null => FluxValue::Null,
            other => panic!("reducer arg {other:?} not representable as FluxValue"),
        })
        .collect()
}

fn run_step(session: &mut Session<'_>, step: &Value) {
    let (kind, body) = step.as_object().unwrap().iter().next().unwrap();
    match kind.as_str() {
        "connect" => {
            let name = body["client"].as_str().unwrap().to_owned();
            let token = body
                .get("token")
                .and_then(Value::as_str)
                .map(|t| t.as_bytes().to_vec())
                .unwrap_or_default();
            let schemas = session.schemas();
            let url = session.transport.url(&session.server);
            let conn = Connection::connect(&url, &token, schemas).expect("connect");
            session.clients.insert(name, conn);
        }
        "close" => {
            let name = body["client"].as_str().unwrap();
            session.clients.remove(name);
        }
        "restart_server" => {
            session.server.restart();
        }
        "subscribe" => {
            let queries: Vec<&str> = body["queries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|q| q.as_str().unwrap())
                .collect();
            let ids = session
                .client(&body["client"])
                .subscribe(&queries)
                .expect("subscribe");
            if let Some(label) = body.get("as").and_then(Value::as_str) {
                session.handles.insert(label.to_owned(), ids);
            }
        }
        "subscribe_error" => {
            let queries: Vec<&str> = body["queries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|q| q.as_str().unwrap())
                .collect();
            let err = session
                .client(&body["client"])
                .subscribe(&queries)
                .unwrap_err();
            assert_error(&err, &body["expect_error"]);
        }
        "unsubscribe" => {
            let label = body["handles"].as_str().unwrap();
            let ids = session
                .handles
                .get(label)
                .unwrap_or_else(|| panic!("unknown handle {label}"))
                .clone();
            session
                .client(&body["client"])
                .unsubscribe(&ids)
                .expect("unsubscribe");
        }
        "call" => {
            let reducer = body["reducer"].as_str().unwrap();
            let args = flux_args(body["args"].as_array().unwrap());
            let result = session.client(&body["client"]).call_reducer(reducer, args);
            match body.get("expect_error") {
                None => result.expect("reducer call succeeds"),
                Some(expect) => assert_error(&result.unwrap_err(), expect),
            }
        }
        "call_until_error" => {
            let reducer = body["reducer"].as_str().unwrap();
            let attempts = body["attempts"].as_u64().unwrap();
            let mut failed = false;
            for _ in 0..attempts {
                let args = flux_args(body["args"].as_array().unwrap());
                if let Err(err) = session.client(&body["client"]).call_reducer(reducer, args) {
                    assert_error(&err, &body["expect_error"]);
                    failed = true;
                    break;
                }
            }
            assert!(failed, "expected a rejection within {attempts} calls");
        }
        "await_row" | "await_gone" | "await_count" => {
            let client_name = body["client"].clone();
            let table = body["table"].as_str().unwrap().to_owned();
            let empty = serde_json::Map::new();
            let where_ = body
                .get("where")
                .and_then(Value::as_object)
                .unwrap_or(&empty)
                .clone();
            let want: usize = match kind.as_str() {
                "await_row" => 1,
                "await_gone" => 0,
                _ => body["count"].as_u64().unwrap() as usize,
            };
            let at_least = kind == "await_row";
            let deadline = Instant::now() + AWAIT;
            loop {
                let matching = {
                    let client = session.client(&client_name);
                    session
                        .canonical_rows(client, &table)
                        .iter()
                        .filter(|row| session.row_matches(&where_, row))
                        .count()
                };
                if (at_least && matching >= want) || (!at_least && matching == want) {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "{kind} {table} {where_:?}: {matching} matching, wanted {want} after {AWAIT:?}"
                );
                std::thread::sleep(Duration::from_millis(25));
            }
        }
        "expect_cache" => {
            let client = session.client(&body["client"]);
            let table = body["table"].as_str().unwrap();
            let expected = body["rows"].as_array().unwrap();
            let mut actual = session.canonical_rows(client, table);
            for want in expected {
                let want = want.as_object().unwrap();
                let pos = actual.iter().position(|row| session.row_matches(want, row));
                let pos = pos.unwrap_or_else(|| {
                    panic!("{table}: no cached row matches {want:?}; cache {actual:?}")
                });
                actual.remove(pos);
            }
            assert!(
                actual.is_empty(),
                "{table}: unexpected extra rows {actual:?}"
            );
        }
        "expect_distinct_identities" => {
            let names = body["clients"].as_array().unwrap();
            let ids: Vec<String> = names
                .iter()
                .map(|n| hex(&session.client(n).identity()))
                .collect();
            let unique: std::collections::HashSet<_> = ids.iter().collect();
            assert_eq!(unique.len(), names.len(), "identities collide: {ids:?}");
        }
        other => panic!("unknown step {other:?} — runner and corpus_version disagree"),
    }
}

/// Assert a client error matches an `expect_error` object (`code`/`catalog`/
/// `contains`). Rust surfaces both server `Error` frames and reducer
/// rejections as `ClientError`; catalog is only carried by the former.
fn assert_error(err: &fluxum_sdk::ClientError, expect: &Value) {
    use fluxum_sdk::ClientError;
    let expect = expect.as_object().unwrap();
    let (code, catalog, message): (Option<u16>, Option<&str>, String) = match err {
        ClientError::Server {
            code,
            name,
            message,
        } => (Some(*code), Some(name), message.clone()),
        ClientError::Reducer { code, message, .. } => (Some(*code), None, message.clone()),
        other => (None, None, other.to_string()),
    };
    if let Some(c) = expect.get("code").and_then(Value::as_u64) {
        assert_eq!(code, Some(c as u16), "error code mismatch: {message}");
    }
    if let Some(cat) = expect.get("catalog").and_then(Value::as_str) {
        assert_eq!(catalog, Some(cat), "error catalog mismatch: {message}");
    }
    if let Some(sub) = expect.get("contains").and_then(Value::as_str) {
        assert!(message.contains(sub), "error {message:?} lacks {sub:?}");
    }
}

#[test]
fn conformance_corpus_is_green() {
    let binary = server_binary();
    if !binary.exists() {
        eprintln!(
            "skipping: no server binary at {} — run: cargo build -p fluxum-server",
            binary.display()
        );
        return;
    }
    let corpus = load_corpus();
    let mut ran = 0;
    for transport in [Transport::Tcp, Transport::Http] {
        for name in &corpus.scenarios {
            let scenario: Value = serde_json::from_str(
                &std::fs::read_to_string(
                    corpus_dir().join("scenarios").join(format!("{name}.json")),
                )
                .unwrap(),
            )
            .unwrap();

            // The label keys the data dir; the transport must be part of it,
            // or the second transport's server RECOVERS the first run's
            // commit log (STG-030) and starts with its rows.
            let server = Server::start(&format!("{}-{name}", transport.name()));
            let mut session = Session {
                corpus: &corpus,
                server,
                transport,
                clients: HashMap::new(),
                handles: HashMap::new(),
            };
            for step in scenario["steps"].as_array().unwrap() {
                run_step(&mut session, step);
            }
            ran += 1;
            eprintln!("conformance[{}]: {name} ok", transport.name());
        }
    }
    assert!(ran > 0, "no scenarios ran");
}
