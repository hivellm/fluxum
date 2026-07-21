//! The SpacetimeDB side of the competitive baseline (TST-097): the same
//! demo app, published as a SpacetimeDB WASM module
//! (`spacetimedb-module/`), driven through the published `spacetimedb-sdk`
//! client — exactly as [`crate::fluxum_side`] drives Fluxum through
//! `fluxum-sdk`. Both SDKs materialize a local cache from subscriptions, so
//! every [`BenchClient`] operation maps to the same behavior:
//!
//! - `add_task` / `send_chat`: reducer call awaited to its ack
//!   (`*_then` callback carrying the reducer status);
//! - `subscribe_chat`: `chat_message` insert callbacks under a
//!   channel-filtered subscription;
//! - `hot_read`: client-cache lookup through the SDK's unique index on
//!   `task.id` — the in-process live-view read, like Fluxum's;
//! - `load_my_data`: a fresh subscription's initial sync, counted once
//!   applied.
//!
//! The server is the pinned `clockworklabs/spacetime:v2.6.1` container (see
//! `docs/parity/spacetimedb-baseline.md`); row visibility of `task` is
//! enforced server-side by the module's RLS filter, so `SELECT * FROM task`
//! delivers only the caller's rows — symmetric with Fluxum's `owner_only`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, mpsc};
use std::time::{Duration, Instant};

use spacetimedb_sdk::{DbContext, Table};

use crate::spacetimedb_bindings::{
    ChatMessageTableAccess, DbConnection, TaskTableAccess, add_task, send_chat,
};
use crate::workload::{BenchClient, Side};

/// Reducer-ack and subscription-applied wait limit. Generous: a stall this
/// long is a failure worth reporting, not a latency to be absorbed.
const WAIT: Duration = Duration::from_secs(30);

/// `seed` → the server-issued token for that seed's identity, so the same
/// seed names the same user across sessions and (server-restart) runs —
/// the SpacetimeDB analogue of the Fluxum side's deterministic dev tokens.
fn token_cache() -> &'static Mutex<HashMap<u64, String>> {
    static CACHE: OnceLock<Mutex<HashMap<u64, String>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The SpacetimeDB [`Side`]: a running standalone's HTTP URL and the
/// published database name.
pub struct SpacetimeDbSide {
    uri: String,
    db_name: String,
}

impl SpacetimeDbSide {
    /// A side talking to the database `db_name` on the server at `uri`
    /// (e.g. `http://127.0.0.1:15300`).
    #[must_use]
    pub fn new(uri: impl Into<String>, db_name: impl Into<String>) -> Self {
        SpacetimeDbSide {
            uri: uri.into(),
            db_name: db_name.into(),
        }
    }
}

impl Side for SpacetimeDbSide {
    fn name(&self) -> &'static str {
        "spacetimedb"
    }

    fn client(&self, seed: u64) -> Result<Box<dyn BenchClient>, String> {
        let token = token_cache()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&seed)
            .cloned();
        let (connected_tx, connected_rx) = mpsc::channel::<Result<String, String>>();
        let error_tx = connected_tx.clone();
        let conn = DbConnection::builder()
            .with_uri(self.uri.clone())
            .with_database_name(self.db_name.clone())
            .with_token(token)
            .on_connect(move |_conn, _identity, token| {
                let _ = connected_tx.send(Ok(token.to_owned()));
            })
            .on_connect_error(move |_ctx, error| {
                let _ = error_tx.send(Err(error.to_string()));
            })
            .build()
            .map_err(|e| format!("spacetimedb connect: {e}"))?;
        conn.run_threaded();
        // Block until the server has answered with our identity: session
        // setup is never measured, and every later operation needs the
        // connection live (mirrors `fluxum_sdk::Connection::connect`).
        let token = connected_rx
            .recv_timeout(WAIT)
            .map_err(|_| "spacetimedb connect: no on_connect within 30 s".to_owned())??;
        token_cache()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(seed, token);
        Ok(Box::new(SpacetimeClient {
            conn,
            read_keys: Vec::new(),
            read_cursor: 0,
        }))
    }
}

struct SpacetimeClient {
    conn: DbConnection,
    /// Round-robin cursor + key snapshot for the read loop, over the SDK's
    /// client cache (the live view `hot_read` reads from).
    read_keys: Vec<u64>,
    read_cursor: usize,
}

impl Drop for SpacetimeClient {
    fn drop(&mut self) {
        // Close the socket eagerly; the `run_threaded` loop exits with it.
        let _ = self.conn.disconnect();
    }
}

/// Collapse a reducer callback's status into the harness error shape:
/// `Ok(Ok(()))` committed, `Ok(Err(msg))` reducer failed, `Err` SDK-internal.
fn ack(
    status: Result<Result<(), String>, spacetimedb_sdk::error::InternalError>,
) -> Result<(), String> {
    match status {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(e.to_string()),
    }
}

impl SpacetimeClient {
    /// Subscribe and block until the initial sync is applied to the client
    /// cache — the same contract as `fluxum_sdk::Connection::subscribe`.
    fn subscribe_applied(&self, query: String) -> Result<(), String> {
        let (tx, rx) = mpsc::channel::<Result<(), String>>();
        let error_tx = tx.clone();
        let what = query.clone();
        self.conn
            .subscription_builder()
            .on_applied(move |_ctx| {
                let _ = tx.send(Ok(()));
            })
            .on_error(move |_ctx, error| {
                let _ = error_tx.send(Err(format!("{what}: {error}")));
            })
            .subscribe(query);
        rx.recv_timeout(WAIT)
            .map_err(|_| "subscription not applied within 30 s".to_owned())?
    }
}

impl BenchClient for SpacetimeClient {
    fn add_task(&mut self, title: &str) -> Result<(), String> {
        let (tx, rx) = mpsc::channel();
        self.conn
            .reducers
            .add_task_then(title.to_owned(), move |_ctx, status| {
                let _ = tx.send(ack(status));
            })
            .map_err(|e| format!("add_task: {e}"))?;
        rx.recv_timeout(WAIT)
            .map_err(|_| "add_task: no reducer ack within 30 s".to_owned())?
    }

    fn send_chat(&mut self, channel: u32, content: &str) -> Result<(), String> {
        let (tx, rx) = mpsc::channel();
        self.conn
            .reducers
            .send_chat_then(channel, content.to_owned(), move |_ctx, status| {
                let _ = tx.send(ack(status));
            })
            .map_err(|e| format!("send_chat: {e}"))?;
        rx.recv_timeout(WAIT)
            .map_err(|_| "send_chat: no reducer ack within 30 s".to_owned())?
    }

    fn subscribe_chat(
        &mut self,
        channel: u32,
        on_message: Box<dyn Fn(&str) + Send + Sync>,
    ) -> Result<(), String> {
        // Listener BEFORE the subscription: initial rows and the first
        // transaction updates must not race past it (same order as the
        // Fluxum side).
        self.conn.db.chat_message().on_insert(move |_ctx, row| {
            on_message(&row.content);
        });
        self.subscribe_applied(format!(
            "SELECT * FROM chat_message WHERE channel = {channel}"
        ))
    }

    fn prepare_reads(&mut self, rows: u32) -> Result<(), String> {
        // The module's RLS filter delivers only this identity's rows —
        // server-side, like Fluxum's `owner_only` (DM-060).
        self.subscribe_applied("SELECT * FROM task".to_owned())?;
        for i in 0..rows {
            self.add_task(&format!("seed {i}"))?;
        }
        // The acked inserts' transaction updates may still be in flight;
        // wait for the SDK cache (the live view) to catch up.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let seen = self.conn.db.task().count();
            if seen >= u64::from(rows) {
                break;
            }
            if Instant::now() > deadline {
                return Err(format!("client cache has {seen}/{rows} rows after 10 s"));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        self.read_keys = self.conn.db.task().iter().map(|task| task.id).collect();
        self.read_keys.sort_unstable();
        Ok(())
    }

    fn hot_read(&mut self) -> Result<String, String> {
        let Some(&key) = self
            .read_keys
            .get(self.read_cursor % self.read_keys.len().max(1))
        else {
            return Err("hot_read before prepare_reads".to_owned());
        };
        self.read_cursor = self.read_cursor.wrapping_add(1);
        self.conn
            .db
            .task()
            .id()
            .find(&key)
            .map(|task| task.title)
            .ok_or_else(|| format!("task {key} vanished from the client cache"))
    }

    fn load_my_data(&mut self) -> Result<u32, String> {
        // A fresh subscription's initial sync — "open the app after a cold
        // start", timed by the caller; the count proves the rows arrived.
        self.subscribe_applied("SELECT * FROM task".to_owned())?;
        Ok(u32::try_from(self.conn.db.task().count()).unwrap_or(u32::MAX))
    }
}

/// Distinct seed namespaces for harness-internal sessions (reset probes),
/// far above the workloads' `run * 10_000 + …` seeds.
static ADMIN_SEED: AtomicU64 = AtomicU64::new(u64::MAX / 2);

/// Ping the side: open a session and read the `task` cache. Used by the
/// report path to fail fast (with the docker one-liner in the error) before
/// half a matrix has run against a dead server.
pub fn probe(uri: &str, db_name: &str) -> Result<(), String> {
    let side = SpacetimeDbSide::new(uri, db_name);
    let mut client = side.client(ADMIN_SEED.fetch_add(1, Ordering::Relaxed))?;
    client.load_my_data().map(drop)
}
