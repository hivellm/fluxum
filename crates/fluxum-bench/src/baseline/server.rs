//! The baseline app-server: axum over [`Db`], with the WebSocket fan-out hub.
//!
//! Endpoints (see [`super::protocol`]):
//! - `POST /tasks`      — the acked small write (task insert)
//! - `POST /chat`       — chat insert; fans out to channel subscribers
//! - `GET  /subscribe`  — WebSocket; `?channel=N`; one JSON text frame per
//!   message posted to that channel after the socket opened
//!
//! Fan-out: a process-wide `tokio::sync::broadcast` feeds every socket. On
//! PostgreSQL it is fed by a dedicated `LISTEN chat` connection — the
//! change signal crosses the database like production LISTEN/NOTIFY setups
//! do. On SQLite the `POST /chat` handler feeds it after commit.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use tokio::sync::broadcast;

use super::db::Db;
use super::protocol::{AddTask, ChatPush, SendChat};

/// Shared app state.
struct AppState {
    db: Db,
    fanout: broadcast::Sender<ChatPush>,
}

/// Build the router over an already-connected database.
fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/tasks", post(add_task).get(list_tasks))
        .route("/task", get(task_title))
        .route("/chat", post(send_chat))
        .route("/subscribe", get(subscribe))
        .with_state(state)
}

#[derive(serde::Deserialize)]
struct TaskParams {
    user: String,
}

/// The "load my data" read (TST-092 d): every task for one user, JSON out.
async fn list_tasks(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TaskParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let rows = state
        .db
        .tasks_for(&params.user)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let body: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(id, title, done)| serde_json::json!({ "id": id, "title": title, "done": done }))
        .collect();
    Ok(Json(serde_json::Value::Array(body)))
}

/// The hot single-row read (TST-092 c): indexed point SELECT, JSON out.
async fn task_title(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TaskParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let title = state
        .db
        .task_title(&params.user)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "no tasks for that user".to_owned()))?;
    Ok(Json(serde_json::json!({ "title": title })))
}

async fn add_task(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AddTask>,
) -> Result<StatusCode, (StatusCode, String)> {
    state
        .db
        .add_task(&body.user, &body.title)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(StatusCode::CREATED)
}

async fn send_chat(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SendChat>,
) -> Result<StatusCode, (StatusCode, String)> {
    state
        .db
        .send_chat(&body.user, body.channel, &body.content)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e))?;
    // SQLite has no NOTIFY: the app server is the change signal, post-commit.
    // (On PostgreSQL the LISTEN task feeds the fan-out instead — sending
    // here too would double-deliver.)
    if matches!(state.db, Db::Sqlite(_)) {
        let _ = state.fanout.send(ChatPush {
            channel: body.channel,
            content: body.content,
        });
    }
    Ok(StatusCode::CREATED)
}

#[derive(serde::Deserialize)]
struct SubscribeParams {
    channel: u32,
}

async fn subscribe(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SubscribeParams>,
    upgrade: WebSocketUpgrade,
) -> axum::response::Response {
    let receiver = state.fanout.subscribe();
    upgrade.on_upgrade(move |socket| pump(socket, receiver, params.channel))
}

/// Push every fan-out message for `channel` down one socket until it closes.
async fn pump(mut socket: WebSocket, mut receiver: broadcast::Receiver<ChatPush>, channel: u32) {
    loop {
        match receiver.recv().await {
            Ok(push) if push.channel == channel => {
                let Ok(frame) = serde_json::to_string(&push) else {
                    continue;
                };
                if socket.send(Message::Text(frame.into())).await.is_err() {
                    return; // client went away
                }
            }
            Ok(_) => {}                                        // other channel
            Err(broadcast::error::RecvError::Lagged(_)) => {} // slow socket: skip, like any push hub
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

/// A TCP listener whose accepted sockets have Nagle disabled — small push
/// frames must leave immediately, same as the Fluxum server's sockets.
fn listener_with_nodelay(listener: tokio::net::TcpListener) -> NoDelayListener {
    NoDelayListener(listener)
}

struct NoDelayListener(tokio::net::TcpListener);

impl axum::serve::Listener for NoDelayListener {
    type Io = tokio::net::TcpStream;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            if let Ok((stream, addr)) = self.0.accept().await {
                let _ = stream.set_nodelay(true);
                return (stream, addr);
            }
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        self.0.local_addr()
    }
}

/// On PostgreSQL: hold the `LISTEN chat` connection and feed the fan-out.
async fn listen_task(pool: sqlx::PgPool, fanout: broadcast::Sender<ChatPush>) {
    let mut listener = match sqlx::postgres::PgListener::connect_with(&pool).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("baseline-server: LISTEN connect failed: {e}");
            return;
        }
    };
    if let Err(e) = listener.listen("chat").await {
        eprintln!("baseline-server: LISTEN chat failed: {e}");
        return;
    }
    loop {
        match listener.recv().await {
            Ok(notification) => {
                match serde_json::from_str::<ChatPush>(notification.payload()) {
                    Ok(push) => drop(fanout.send(push)),
                    Err(e) => eprintln!("baseline-server: bad NOTIFY payload: {e}"),
                }
            }
            Err(e) => {
                // The listener reconnects internally; a persistent error
                // here means the database is gone.
                eprintln!("baseline-server: LISTEN recv: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

/// Connect the database and serve the app on `listener` (the async body of
/// [`serve_blocking`], split out so tests can run it in-process on an
/// ephemeral port — the spawned-child path attributes no coverage).
async fn serve_on(
    database_url: &str,
    max_connections: u32,
    listener: tokio::net::TcpListener,
) -> Result<(), String> {
    let db = Db::connect(database_url, max_connections).await?;
    let (fanout, _) = broadcast::channel(16_384);
    if let Db::Postgres(pool) = &db {
        tokio::spawn(listen_task(pool.clone(), fanout.clone()));
    }
    let state = Arc::new(AppState { db, fanout });
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    // The parent process watches for this line to know the server is up.
    println!("baseline-server listening on {addr}");
    // Same Nagle treatment as the Fluxum server (TST-091: the incumbent
    // is tuned like a competent deployment, not handicapped).
    axum::serve(listener_with_nodelay(listener), router(state))
        .await
        .map_err(|e| e.to_string())
}

/// Serve the baseline app on `127.0.0.1:port` (blocking; builds its own
/// runtime). `database_url` is `postgres://…` or `sqlite://…`;
/// `max_connections` sizes the pool (TST-091: a competent incumbent pools).
pub fn serve_blocking(database_url: &str, port: u16, max_connections: u32) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .map_err(|e| format!("bind 127.0.0.1:{port}: {e}"))?;
        serve_on(database_url, max_connections, listener).await
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Boot the app in-process over a throwaway SQLite file and return its
    /// base URL (the serve task runs on the runtime until the test ends).
    async fn boot() -> String {
        let db_path = std::env::temp_dir().join(format!(
            "fluxum-baseline-test-{}-{}.sqlite",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let url = format!("sqlite://{}", db_path.display());
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            serve_on(&url, 4, listener).await.unwrap();
        });
        format!("http://127.0.0.1:{port}")
    }

    /// The whole demo-app loop against the real router over real sockets:
    /// acked writes, owner-scoped reads, hot single-row read, and the
    /// WebSocket fan-out with server-side channel filtering — the behaviors
    /// `BaselineSide` relies on for every TST-092 workload.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn baseline_app_serves_the_demo_loop() {
        let base = boot().await;
        let agent = ureq::AgentBuilder::new().build();

        // Acked small write (TST-092 a): 201 and the row is really there.
        let created = agent
            .post(&format!("{base}/tasks"))
            .send_json(serde_json::json!({ "user": "u1", "title": "first task" }))
            .unwrap();
        assert_eq!(created.status(), 201);
        agent
            .post(&format!("{base}/tasks"))
            .send_json(serde_json::json!({ "user": "u1", "title": "second task" }))
            .unwrap();

        // "Load my data" (TST-092 d): only this user's rows come back.
        let rows: serde_json::Value = agent
            .get(&format!("{base}/tasks"))
            .query("user", "u1")
            .call()
            .unwrap()
            .into_json()
            .unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 2);
        let none: serde_json::Value = agent
            .get(&format!("{base}/tasks"))
            .query("user", "nobody")
            .call()
            .unwrap()
            .into_json()
            .unwrap();
        assert_eq!(none.as_array().unwrap().len(), 0);

        // Hot single-row read (TST-092 c): the indexed point SELECT — the
        // user's newest task (ORDER BY id DESC LIMIT 1); 404 names the miss
        // for a user with no rows.
        let hot: serde_json::Value = agent
            .get(&format!("{base}/task"))
            .query("user", "u1")
            .call()
            .unwrap()
            .into_json()
            .unwrap();
        assert_eq!(hot["title"], "second task");
        let miss = agent
            .get(&format!("{base}/task"))
            .query("user", "nobody")
            .call();
        assert_eq!(miss.unwrap_err().into_response().unwrap().status(), 404);

        // Fan-out (TST-092 b): subscribe channel 7, then post to channel 9
        // (must NOT arrive — server-side filter in `pump`) and channel 7.
        let ws_url = format!("{}/subscribe?channel=7", base.replace("http://", "ws://"));
        let (mut socket, _) = tungstenite::connect(&ws_url).unwrap();
        for (channel, content) in [(9, "other channel"), (7, "delivered")] {
            let sent = agent
                .post(&format!("{base}/chat"))
                .send_json(serde_json::json!({
                    "user": "u1", "channel": channel, "content": content
                }))
                .unwrap();
            assert_eq!(sent.status(), 201);
        }
        let frame = socket.read().unwrap();
        let push: serde_json::Value =
            serde_json::from_str(frame.to_text().unwrap()).unwrap();
        assert_eq!(push["channel"], 7);
        assert_eq!(push["content"], "delivered");
    }

    /// The same loop over **PostgreSQL** — the `Db::Postgres` arms and the
    /// REAL LISTEN/NOTIFY hop (`listen_task`): `pg_notify` fires inside the
    /// INSERT statement, crosses the database on the dedicated LISTEN
    /// connection, and lands on the WebSocket. Gated on the operator's
    /// docker PG (like the SpacetimeDB smoke) and run in-process because the
    /// spawned-child path attributes no coverage. The shared database
    /// persists across runs, so a unique user + channel isolate this one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn baseline_app_serves_the_demo_loop_over_postgres() {
        let Ok(url) = std::env::var("FLUXUM_BENCH_PG_URL") else {
            eprintln!(
                "skipping: set FLUXUM_BENCH_PG_URL=postgres://fluxum:fluxum@127.0.0.1:15432/parity \
                 (docker fluxum-parity-pg) to run the PG in-process loop"
            );
            return;
        };
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            serve_on(&url, 4, listener).await.unwrap();
        });
        let base = format!("http://127.0.0.1:{port}");
        let agent = ureq::AgentBuilder::new().build();

        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let user = format!("cov-{nonce}");
        #[allow(clippy::cast_possible_truncation)]
        let channel = 100_000 + (nonce % 50_000) as u32;

        // Acked small writes against the real pool.
        for title in ["first task", "second task"] {
            let created = agent
                .post(&format!("{base}/tasks"))
                .send_json(serde_json::json!({ "user": user, "title": title }))
                .unwrap();
            assert_eq!(created.status(), 201);
        }

        // Owner-scoped load: exactly this run's rows.
        let rows: serde_json::Value = agent
            .get(&format!("{base}/tasks"))
            .query("user", &user)
            .call()
            .unwrap()
            .into_json()
            .unwrap();
        assert_eq!(rows.as_array().unwrap().len(), 2);

        // Hot indexed point read: the user's newest row.
        let hot: serde_json::Value = agent
            .get(&format!("{base}/task"))
            .query("user", &user)
            .call()
            .unwrap()
            .into_json()
            .unwrap();
        assert_eq!(hot["title"], "second task");

        // The LISTEN/NOTIFY fan-out: channel-filtered, cross-database.
        let ws_url =
            format!("{}/subscribe?channel={channel}", base.replace("http://", "ws://"));
        let (mut socket, _) = tungstenite::connect(&ws_url).unwrap();
        for (target, content) in [(channel + 1, "other channel"), (channel, "delivered")] {
            let sent = agent
                .post(&format!("{base}/chat"))
                .send_json(serde_json::json!({
                    "user": user, "channel": target, "content": content
                }))
                .unwrap();
            assert_eq!(sent.status(), 201);
        }
        let frame = socket.read().unwrap();
        let push: serde_json::Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
        assert_eq!(push["channel"], channel);
        assert_eq!(push["content"], "delivered");
    }
}
