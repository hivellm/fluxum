//! The Fluxum demo module: chat, presence, and per-user tasks.
//!
//! This is what a Fluxum application *is* — a crate. Nothing here is
//! registered by configuration: `#[fluxum::table]` and `#[fluxum::reducer]`
//! submit to the link-time registry (DM-040, RED-006), and the server collects
//! whatever is linked into it.
//!
//! # Linking caveat (OQ-1)
//!
//! A dependency that is never *referenced* is dropped by the linker, and its
//! registrations go with it — the server would then start with an empty
//! schema and refuse to boot. Hence [`link`], which the binary calls for the
//! side effect of touching this crate.
//!
//! It exercises the three things the demo scenario needs (SPEC-011 acceptance
//! 8): a reducer that produces a `TxUpdate`, per-row visibility so two users
//! see different rows, and connect/disconnect presence.

use fluxum_core::reducer::ReducerContext;
use fluxum_core::types::{Identity, Timestamp};
use fluxum_macros as fluxum;

/// Force the linker to keep this crate, and with it every registration above.
///
/// Calling it is the whole point; it does nothing else.
pub fn link() {}

// --- Tables -----------------------------------------------------------------

/// A chat message. Public: every subscriber sees every message.
#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct ChatMessage {
    /// Server-assigned message id.
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    /// Who sent it.
    pub sender: Identity,
    /// Channel number.
    pub channel: u32,
    /// Message body.
    pub content: String,
    /// Server timestamp at commit.
    pub sent_at: Timestamp,
}

/// A task, visible only to its owner.
///
/// `owner_only` is enforced by the server on the subscription path (DM-060),
/// not by the client filtering what it received — two clients subscribing to
/// the same `SELECT * FROM Task` get different rows.
#[fluxum::table(public)]
#[visibility(owner_only(owner))]
#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    /// Server-assigned task id.
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    /// The only identity that can see this row.
    pub owner: Identity,
    /// What to do.
    pub title: String,
    /// Whether it is finished.
    pub done: bool,
}

/// Who is currently connected, maintained by the lifecycle hooks.
///
/// # Known limitation: keyed by identity, not by connection
///
/// One identity with two live connections shares one row, so the first
/// `on_disconnect` deletes presence for both — reload the demo page and the
/// expiring old session erases the new one's row. Observed in practice, not
/// theoretical.
///
/// Correct presence is keyed by `ConnectionId`, or refcounted per identity.
/// Left as-is because the shape is what makes the bug legible: the table reads
/// as obviously right until two connections share a primary key.
#[fluxum::table(public)]
#[derive(Debug, Clone, PartialEq)]
pub struct OnlineUser {
    /// The connected identity.
    #[primary_key]
    pub identity: Identity,
    /// When the connection was established.
    pub connected_at: Timestamp,
}

// --- Reducers ---------------------------------------------------------------

/// Post a message to a channel.
///
/// Rate-limited per `(identity, reducer)`: a client that loops on this is
/// rejected with a 429 before a transaction is ever opened (RED-050), which
/// is what keeps one misbehaving tab from filling the commit log.
#[fluxum::reducer(max_rate = "20/s")]
fn send_chat(ctx: &ReducerContext, channel: u32, content: String) -> Result<(), String> {
    if content.is_empty() {
        return Err("message is empty".to_string());
    }
    if content.len() > 4096 {
        return Err("message is too long (max 4096 bytes)".to_string());
    }
    ctx.tx
        .insert(ChatMessage {
            id: 0,
            sender: ctx.identity,
            channel,
            content,
            sent_at: ctx.timestamp,
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Create a task owned by the caller.
#[fluxum::reducer]
fn add_task(ctx: &ReducerContext, title: String) -> Result<(), String> {
    if title.is_empty() {
        return Err("task title is empty".to_string());
    }
    ctx.tx
        .insert(Task {
            id: 0,
            owner: ctx.identity,
            title,
            done: false,
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Mark one of the caller's own tasks done.
///
/// The ownership check is explicit rather than left to `owner_only`: row
/// visibility governs what a subscription *delivers*, and a reducer runs
/// server-side against the whole table. Without this, anyone who guessed an id
/// could complete someone else's task.
#[fluxum::reducer]
fn complete_task(ctx: &ReducerContext, id: u64) -> Result<(), String> {
    let task = ctx
        .tx
        .query_pk::<Task>(id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no task {id}"))?;

    if task.owner != ctx.identity {
        return Err("not your task".to_string());
    }
    if task.done {
        return Ok(()); // idempotent: completing twice is not an error
    }

    ctx.tx
        .upsert(Task { done: true, ..task })
        .map_err(|e| e.to_string())?;
    Ok(())
}

// --- Lifecycle --------------------------------------------------------------

/// Record presence when a client connects (RED-012).
#[fluxum::on_connect]
fn presence_up(ctx: &ReducerContext) -> Result<(), String> {
    ctx.tx
        .upsert(OnlineUser {
            identity: ctx.identity,
            connected_at: ctx.timestamp,
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Drop presence when a client disconnects (RED-013).
#[fluxum::on_disconnect]
fn presence_down(ctx: &ReducerContext) -> Result<(), String> {
    ctx.tx
        .delete::<OnlineUser>(ctx.identity)
        .map_err(|e| e.to_string())?;
    Ok(())
}
