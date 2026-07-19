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
use fluxum_core::types::{ConnectionId, Identity, Timestamp};
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

/// Who is currently connected — one row per **connection**, not per identity.
///
/// Keyed by `ConnectionId` because presence is a property of a connection:
/// keying it by identity means two tabs share one row, and the first one to
/// disconnect deletes presence for both. That is not hypothetical — the demo
/// page did exactly this, and reloading it made the expiring old session erase
/// the new session's row.
///
/// `ephemeral` + `#[owner]` (SPEC-023 DMX-011) is the built-in answer: the
/// engine drops this connection's rows on disconnect, in the same transaction
/// as any `on_disconnect` hook, so presence and its cleanup fan out
/// atomically. It also makes the table memory-only, which is right — presence
/// from a previous run of the server is never true.
#[fluxum::table(ephemeral)]
#[derive(Debug, Clone, PartialEq)]
pub struct OnlineUser {
    /// The connection this presence belongs to; dropped when it closes.
    #[primary_key]
    #[owner]
    pub connection: ConnectionId,
    /// Who is on the other end. Not unique: one identity may hold several.
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
///
/// There is deliberately no matching `on_disconnect`: the `#[owner]` binding
/// on [`OnlineUser`] makes the engine drop this connection's row itself
/// (DMX-011). A hand-written hook would be a second implementation of the same
/// rule, free to drift from it — and the earlier one did, deleting by identity
/// and taking every other connection's presence with it.
#[fluxum::on_connect]
fn presence_up(ctx: &ReducerContext) -> Result<(), String> {
    ctx.tx
        .upsert(OnlineUser {
            connection: ctx.connection_id,
            identity: ctx.identity,
            connected_at: ctx.timestamp,
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}
