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
        .route("/tasks", post(add_task))
        .route("/chat", post(send_chat))
        .route("/subscribe", get(subscribe))
        .with_state(state)
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

/// Serve the baseline app on `127.0.0.1:port` (blocking; builds its own
/// runtime). `database_url` is `postgres://…` or `sqlite://…`;
/// `max_connections` sizes the pool (TST-091: a competent incumbent pools).
pub fn serve_blocking(database_url: &str, port: u16, max_connections: u32) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    runtime.block_on(async move {
        let db = Db::connect(database_url, max_connections).await?;
        let (fanout, _) = broadcast::channel(16_384);
        if let Db::Postgres(pool) = &db {
            tokio::spawn(listen_task(pool.clone(), fanout.clone()));
        }
        let state = Arc::new(AppState { db, fanout });
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .map_err(|e| format!("bind 127.0.0.1:{port}: {e}"))?;
        let addr = listener.local_addr().map_err(|e| e.to_string())?;
        // The parent process watches for this line to know the server is up.
        println!("baseline-server listening on {addr}");
        axum::serve(listener, router(state))
            .await
            .map_err(|e| e.to_string())
    })
}
