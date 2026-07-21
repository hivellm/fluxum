//! The parity demo app (chat + presence + per-user tasks) as a SpacetimeDB
//! module — the 1:1 mirror of `crates/fluxum-demo` for the competitive
//! baseline (TST-097). Same tables, same fields, same reducer validation;
//! each platform pays its own idiomatic cost for the same product rules.
//!
//! Where the platforms differ, the mirror is by *behavior*, documented here:
//!
//! - **Row visibility** (`Task`): Fluxum declares `owner_only(owner)`
//!   (DM-060); SpacetimeDB expresses the same server-side rule as a
//!   row-level-security filter (`client_visibility_filter`, `:sender`).
//! - **Presence** (`OnlineUser`): Fluxum's `ephemeral` + `#[owner]` drops a
//!   connection's rows on disconnect inside the engine (DMX-011);
//!   SpacetimeDB's idiom is the `client_connected` / `client_disconnected`
//!   lifecycle reducer pair doing the insert/delete.
//! - **Chat rate limit** (RED-050, `max_rate = "20/s"`): Fluxum enforces it
//!   before a transaction opens, in memory; SpacetimeDB has no reducer rate
//!   limiting, so the same product rule is a budget table updated inside
//!   the transaction — what a real SpacetimeDB app would ship. The harness
//!   keeps its offered chat load under the limit on BOTH sides, so the
//!   limiter never rejects during measurement (TST-090).

use spacetimedb::{ConnectionId, Identity, ReducerContext, Table, Timestamp};

// --- Tables -----------------------------------------------------------------

/// A chat message. Public: every subscriber sees every message.
///
/// `channel` is btree-indexed so the channel-filtered subscription
/// (`SELECT * FROM chat_message WHERE channel = N`) evaluates off an index —
/// the baseline gets its best-practice setup, symmetric with the covering
/// indexes the PostgreSQL side receives.
#[spacetimedb::table(accessor = chat_message, public)]
pub struct ChatMessage {
    /// Server-assigned message id.
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    /// Who sent it.
    pub sender: Identity,
    /// Channel number.
    #[index(btree)]
    pub channel: u32,
    /// Message body.
    pub content: String,
    /// Server timestamp at commit.
    pub sent_at: Timestamp,
}

/// A task, visible only to its owner (see [`TASK_OWNER_ONLY`]).
#[spacetimedb::table(accessor = task, public)]
pub struct Task {
    /// Server-assigned task id.
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    /// The only identity that can see this row.
    #[index(btree)]
    pub owner: Identity,
    /// What to do.
    pub title: String,
    /// Whether it is finished.
    pub done: bool,
}

/// DM-060 mirror: the server delivers a `task` subscription only the
/// caller's own rows — two clients subscribing to `SELECT * FROM task` get
/// different rows, enforced server-side, exactly like Fluxum's
/// `owner_only(owner)`.
#[spacetimedb::client_visibility_filter]
const TASK_OWNER_ONLY: spacetimedb::Filter =
    spacetimedb::Filter::Sql("SELECT * FROM task WHERE owner = :sender");

/// Who is currently connected — one row per **connection**, not per
/// identity (same key choice as the Fluxum demo, for the same reason: two
/// tabs must not share a presence row).
#[spacetimedb::table(accessor = online_user, public)]
pub struct OnlineUser {
    /// The connection this presence belongs to; deleted when it closes.
    #[primary_key]
    pub connection: ConnectionId,
    /// Who is on the other end. Not unique: one identity may hold several.
    pub identity: Identity,
    /// When the connection was established.
    pub connected_at: Timestamp,
}

/// Per-identity chat budget (RED-050 mirror). Private — clients never see
/// their own accounting.
#[spacetimedb::table(accessor = send_chat_budget)]
pub struct SendChatBudget {
    /// The rate-limited sender.
    #[primary_key]
    pub sender: Identity,
    /// Start of the current one-second window, µs since the Unix epoch.
    pub window_start_micros: i64,
    /// Messages sent inside the current window.
    pub sent: u32,
}

/// The Fluxum demo's `max_rate = "20/s"` on `send_chat`.
const CHAT_RATE_PER_SEC: u32 = 20;

// --- Reducers ---------------------------------------------------------------

/// Post a message to a channel. Validation identical to the Fluxum demo;
/// rate-limited per identity via [`SendChatBudget`].
#[spacetimedb::reducer]
pub fn send_chat(ctx: &ReducerContext, channel: u32, content: String) -> Result<(), String> {
    if content.is_empty() {
        return Err("message is empty".to_string());
    }
    if content.len() > 4096 {
        return Err("message is too long (max 4096 bytes)".to_string());
    }
    spend_chat_budget(ctx)?;
    ctx.db.chat_message().insert(ChatMessage {
        id: 0,
        sender: ctx.sender(),
        channel,
        content,
        sent_at: ctx.timestamp,
    });
    Ok(())
}

/// One-second fixed windows: at most [`CHAT_RATE_PER_SEC`] sends per window
/// per identity, mirroring RED-050's verdict (a 429-equivalent error).
fn spend_chat_budget(ctx: &ReducerContext) -> Result<(), String> {
    let now = ctx.timestamp.to_micros_since_unix_epoch();
    let budgets = ctx.db.send_chat_budget();
    match budgets.sender().find(ctx.sender()) {
        Some(budget) if now - budget.window_start_micros < 1_000_000 => {
            if budget.sent >= CHAT_RATE_PER_SEC {
                return Err("rate limit exceeded: 20/s".to_string());
            }
            budgets.sender().update(SendChatBudget {
                sent: budget.sent + 1,
                ..budget
            });
        }
        _ => {
            // First message ever, or a new window.
            budgets.sender().delete(ctx.sender());
            budgets.insert(SendChatBudget {
                sender: ctx.sender(),
                window_start_micros: now,
                sent: 1,
            });
        }
    }
    Ok(())
}

/// Create a task owned by the caller.
#[spacetimedb::reducer]
pub fn add_task(ctx: &ReducerContext, title: String) -> Result<(), String> {
    if title.is_empty() {
        return Err("task title is empty".to_string());
    }
    ctx.db.task().insert(Task {
        id: 0,
        owner: ctx.sender(),
        title,
        done: false,
    });
    Ok(())
}

/// Mark one of the caller's own tasks done. The ownership check is explicit
/// for the same reason as in the Fluxum demo: visibility filters govern
/// subscriptions, not reducers.
#[spacetimedb::reducer]
pub fn complete_task(ctx: &ReducerContext, id: u64) -> Result<(), String> {
    let task = ctx
        .db
        .task()
        .id()
        .find(id)
        .ok_or_else(|| format!("no task {id}"))?;

    if task.owner != ctx.sender() {
        return Err("not your task".to_string());
    }
    if task.done {
        return Ok(()); // idempotent: completing twice is not an error
    }

    ctx.db.task().id().update(Task { done: true, ..task });
    Ok(())
}

// --- Lifecycle --------------------------------------------------------------

/// Record presence when a client connects (the Fluxum demo's `presence_up`).
#[spacetimedb::reducer(client_connected)]
pub fn presence_up(ctx: &ReducerContext) -> Result<(), String> {
    let connection = ctx.connection_id().ok_or("client_connected without a connection id")?;
    ctx.db.online_user().insert(OnlineUser {
        connection,
        identity: ctx.sender(),
        connected_at: ctx.timestamp,
    });
    Ok(())
}

/// Drop presence on disconnect — SpacetimeDB's idiom for what Fluxum's
/// `ephemeral` + `#[owner]` does inside the engine (DMX-011).
#[spacetimedb::reducer(client_disconnected)]
pub fn presence_down(ctx: &ReducerContext) {
    if let Some(connection) = ctx.connection_id() {
        ctx.db.online_user().connection().delete(connection);
    }
}
