//! The incumbent-stack side of the parity comparison (TST-090b): the same
//! demo application (chat + tasks + live subscriptions) as an **app-server +
//! SQL database**, the architecture Fluxum replaces.
//!
//! Stack (OQ-9): axum + sqlx — the representative Rust incumbent, kept
//! in-repo so the whole comparison builds and runs with one toolchain
//! (TST-096). A Node/Express variant remains open under OQ-9; nothing here
//! precludes adding it, since the workload driver only sees the HTTP/WS
//! protocol below.
//!
//! Architecture (the honest shape of the incumbent):
//! - **writes** are `POST` endpoints: parse → SQL `INSERT` (prepared
//!   statement, pooled connection) → HTTP 2xx after commit;
//! - **live queries** are a WebSocket: the app server is the fan-out hub.
//!   On PostgreSQL the path is `INSERT` → `NOTIFY` → `PgListener` →
//!   broadcast → WS push, i.e. the database's own change-signal mechanism,
//!   not an app-side shortcut around it. On SQLite (no LISTEN/NOTIFY) the
//!   app server broadcasts after commit — the standard architecture for an
//!   embedded database.
//!
//! The server runs as its **own process** (`fluxum-bench baseline-server`),
//! like the incumbent it models — an in-process baseline would share the
//! driver's CPU and undercount the stack's real cost.

pub mod db;
pub mod server;

/// The wire protocol between the workload driver and the baseline server —
/// one place, so the client (`baseline_side`) and server cannot drift.
pub mod protocol {
    /// `POST /tasks` body.
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct AddTask {
        /// The acting user (the incumbent's session token stand-in).
        pub user: String,
        /// Task title.
        pub title: String,
    }

    /// `POST /chat` body.
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    pub struct SendChat {
        /// The acting user.
        pub user: String,
        /// Channel number.
        pub channel: u32,
        /// Message body.
        pub content: String,
    }

    /// One chat message pushed over the subscription WebSocket
    /// (`GET /subscribe?channel=N`), as a JSON text frame.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    pub struct ChatPush {
        /// Channel the message was posted to.
        pub channel: u32,
        /// Message body.
        pub content: String,
    }
}
