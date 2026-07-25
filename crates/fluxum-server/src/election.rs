//! Replica-set primary election (SPEC-014 §5; T7.2; FR-101).
//!
//! **OQ-8 resolution: a minimal custom Raft-style election, votes only.**
//! Fluxum does not need a consensus LOG — the commit log already IS the
//! replicated log (REP-010), with its own sync (REP-012/013), flow control
//! (REP-017) and quorum acknowledgment (REP-021). What §5 needs from
//! consensus is exactly leader election with the Raft safety rules:
//! majority votes, persisted `(epoch, voted_for)` before acting (REP-004),
//! and the up-to-dateness comparison on `(last_log_epoch, last_tx_id)`
//! (REP-030 — in `semi_sync` this guarantees the winner holds every
//! quorum-acknowledged transaction). `openraft` would bring a second log
//! replication machine to stub out; the family precedent (hand-rolled
//! SigV4 in T7.3, Nexus's custom Raft) and the DST determinism requirement
//! (TST-134) favor the ~small, fully testable election below.
//!
//! Shape: every member serves votes over the ordinary server-peer TCP
//! transport (`VoteRequest` → `VoteResponse`, routed like `ReplicaHello`).
//! A follower runs the replica client against its current primary and
//! times out on contact loss (`election_timeout_ms` + per-member jitter);
//! on timeout it becomes a candidate: persists its ballot, asks every peer
//! for a vote, and promotes on majority (REP-032). Losers return to
//! follower and retry with a higher epoch. A follower whose target is dead
//! ROTATES through its peer list, which is also how it finds the new
//! primary after an election (the winner answers `ReplicaHello` with a
//! `PrimaryHello` carrying the new epoch) — no extra announce message.
//!
//! Two-member sets cannot elect automatically (majority = 2): the follower
//! keeps retrying and the operator promotes manually, per REP-030.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::AsyncWriteExt;

use fluxum_core::FluxumError;
use fluxum_protocol::{ClientMessage, FrameCodec, ServerMessage, VoteRequest, VoteResponse};

use crate::ShardContext;
use crate::replication::{load_epoch, persist_epoch};

/// The persisted ballot marker, next to the commit log (REP-004: a vote
/// must be durable before it is answered).
pub const BALLOT_FILE: &str = "election.vote";

/// A durable vote: `epoch` was granted to `voted_for`. One ballot per
/// epoch, ever — the Raft safety rule. Persisted as a MessagePack
/// `(u64, String)` tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ballot {
    /// The election epoch this vote belongs to.
    pub epoch: u64,
    /// The member the vote went to (possibly ourselves).
    pub voted_for: String,
}

/// Load the persisted ballot, if any.
///
/// # Errors
/// An unreadable or undecodable marker.
pub fn load_ballot(log_dir: &Path) -> Result<Option<Ballot>, FluxumError> {
    let path = log_dir.join(BALLOT_FILE);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&path)?;
    let (epoch, voted_for): (u64, String) = rmp_serde::from_slice(&bytes)
        .map_err(|e| FluxumError::Storage(format!("election.vote decode failed: {e}")))?;
    Ok(Some(Ballot { epoch, voted_for }))
}

/// Durably persist a ballot (fsynced before any `VoteResponse` leaves).
///
/// # Errors
/// I/O failures.
pub fn persist_ballot(log_dir: &Path, ballot: &Ballot) -> Result<(), FluxumError> {
    std::fs::create_dir_all(log_dir)?;
    let path = log_dir.join(BALLOT_FILE);
    let bytes = rmp_serde::to_vec(&(ballot.epoch, ballot.voted_for.as_str()))
        .map_err(|e| FluxumError::Storage(format!("election.vote encode failed: {e}")))?;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&path)?;
    std::io::Write::write_all(&mut file, &bytes)?;
    file.sync_all()?;
    Ok(())
}

/// The majority of a replica set (REP-030): strictly more than half.
pub fn majority(members: usize) -> usize {
    members / 2 + 1
}

/// The Raft vote-grant rule (REP-030), pure so the DST can drive it:
/// deny while we still hear a live primary (leader stickiness, Raft §9.6 —
/// a slow follower's spurious timeout must not unseat a healthy primary),
/// deny a stale candidacy (`epoch <= acting epoch`), deny a second ballot
/// in the same epoch to a different candidate, deny a candidate whose log
/// head `(last_log_epoch, last_tx_id)` is behind ours; otherwise grant.
/// Returns the ballot to persist BEFORE answering, when granting.
pub fn decide_vote(
    acting_epoch: u64,
    ballot: Option<&Ballot>,
    our_head: (u64, u64),
    hears_a_primary: bool,
    req: &VoteRequest,
) -> Option<Ballot> {
    if hears_a_primary {
        return None;
    }
    if req.epoch <= acting_epoch {
        return None;
    }
    if let Some(b) = ballot
        && b.epoch >= req.epoch
        && !(b.epoch == req.epoch && b.voted_for == req.member_name)
    {
        return None;
    }
    if (req.last_log_epoch, req.last_applied_tx_id) < our_head {
        return None;
    }
    Some(Ballot {
        epoch: req.epoch,
        voted_for: req.member_name.clone(),
    })
}

/// The published role of this member — lock-free, read by write admission
/// (REP-042), `/health` (REP-080) and the metrics exporter (REP-081).
#[derive(Debug)]
pub struct RoleState {
    primary: AtomicBool,
    epoch: AtomicU64,
    /// The best-known primary endpoint (the `NotPrimary` redirect hint AND
    /// the follower's preferred dial target). Shared with the replica
    /// client, which stamps it on every `PrimaryHello` so a follower dials
    /// the live primary first instead of restarting from a dead peer.
    primary_hint: Arc<std::sync::Mutex<Option<String>>>,
}

impl RoleState {
    pub fn new(primary: bool, epoch: u64) -> Self {
        Self {
            primary: AtomicBool::new(primary),
            epoch: AtomicU64::new(epoch),
            primary_hint: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Whether this member currently accepts writes (REP-042).
    pub fn is_primary(&self) -> bool {
        self.primary.load(Ordering::SeqCst)
    }

    /// The epoch this member is acting under.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// The best-known primary endpoint, for redirects.
    pub fn primary_hint(&self) -> Option<String> {
        self.primary_hint
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn set(&self, primary: bool, epoch: u64) {
        self.primary.store(primary, Ordering::SeqCst);
        self.epoch.fetch_max(epoch, Ordering::SeqCst);
    }

    fn set_hint(&self, hint: Option<String>) {
        *self
            .primary_hint
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = hint;
    }

    /// The shared hint cell, handed to the replica client so it stamps the
    /// live primary endpoint on every `PrimaryHello`.
    fn hint_cell(&self) -> Arc<std::sync::Mutex<Option<String>>> {
        Arc::clone(&self.primary_hint)
    }
}

/// Election state shared between the TCP router (vote serving) and the
/// election task (candidacy) — REP-030.
pub struct ElectionState {
    shard_id: u32,
    member_name: String,
    log_dir: PathBuf,
    role: RoleState,
    /// Cached persisted ballot (the file stays authoritative).
    ballot: std::sync::Mutex<Option<Ballot>>,
    /// Touched on every message from the primary and on every granted
    /// vote — the follower's election timer AND the leader-stickiness
    /// vote rule watch it (REP-016/REP-030). Shared into
    /// [`crate::replication::ReplicaOptions::contact`].
    contact: Arc<crate::replication::ContactClock>,
    /// `replication.election_timeout_ms` — the stickiness window.
    election_timeout: Duration,
    /// `replication.max_staleness_ms` — REP-041 read-admission bound.
    max_staleness: Duration,
}

impl std::fmt::Debug for ElectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElectionState")
            .field("shard_id", &self.shard_id)
            .field("member", &self.member_name)
            .field("primary", &self.role.is_primary())
            .field("epoch", &self.role.epoch())
            .finish_non_exhaustive()
    }
}

impl ElectionState {
    /// Build the shared state; `primary`/`epoch` are the boot role hint
    /// (REP-003) and the persisted epoch.
    ///
    /// # Errors
    /// An unreadable ballot marker.
    pub fn new(
        shard_id: u32,
        member_name: String,
        log_dir: PathBuf,
        primary: bool,
        epoch: u64,
        election_timeout: Duration,
        max_staleness: Duration,
    ) -> Result<Arc<Self>, FluxumError> {
        let ballot = load_ballot(&log_dir)?;
        Ok(Arc::new(Self {
            shard_id,
            member_name,
            log_dir,
            role: RoleState::new(primary, epoch),
            ballot: std::sync::Mutex::new(ballot),
            contact: Arc::new(crate::replication::ContactClock::default()),
            election_timeout,
            max_staleness,
        }))
    }

    /// REP-041: whether a replica should refuse new reads because its data
    /// may be too stale — it has not heard from its primary within
    /// `max_staleness_ms`. The primary is never stale (it holds the head).
    pub fn read_is_stale(&self) -> bool {
        !self.role.is_primary() && self.contact.elapsed() > self.max_staleness
    }

    /// The member name (REP-005), for `/health`.
    pub fn member_name(&self) -> &str {
        &self.member_name
    }

    /// The shared best-known-primary cell, for the replica client to stamp
    /// on every `PrimaryHello` (REP-050).
    pub fn primary_hint_cell(&self) -> Arc<std::sync::Mutex<Option<String>>> {
        self.role.hint_cell()
    }

    /// The published role.
    pub fn role(&self) -> &RoleState {
        &self.role
    }

    /// Note contact from the primary (resets the election timer and arms
    /// the stickiness window).
    pub fn note_contact(&self) {
        self.contact.touch();
    }

    /// The shared contact clock, touched by the replica client on every
    /// message from the primary.
    pub fn contact_clock(&self) -> Arc<crate::replication::ContactClock> {
        Arc::clone(&self.contact)
    }

    /// Answer a peer's `VoteRequest` (REP-030): apply [`decide_vote`]
    /// against the persisted state, persist the ballot BEFORE answering
    /// (REP-004), and report our highest known epoch either way.
    pub fn answer(&self, ctx: &ShardContext, req: &VoteRequest) -> VoteResponse {
        let deny = |epoch: u64| VoteResponse {
            epoch,
            granted: false,
        };
        let acting = load_epoch(&self.log_dir).unwrap_or(self.role.epoch());
        if req.shard_id != self.shard_id {
            return deny(acting);
        }
        let log = ctx.engine.pipeline().log();
        let our_head = (log.epoch(), log.durable_tx_id().ok().flatten().unwrap_or(0));
        // Leader stickiness (Raft §9.6): an unfenced primary never endorses
        // a rival, and a follower that still hears its primary does not
        // either — a spurious timeout elsewhere must not unseat a healthy
        // primary and split the set's history.
        let hears_a_primary = if self.role.is_primary() {
            !ctx.replication_primary()
                .is_some_and(|primary| primary.fenced())
        } else {
            self.contact.elapsed() < self.election_timeout
        };
        let mut ballot = self
            .ballot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match decide_vote(acting, ballot.as_ref(), our_head, hears_a_primary, req) {
            Some(grant) => {
                if let Err(e) = persist_ballot(&self.log_dir, &grant) {
                    tracing::error!(target: "fluxum::election", error = %e,
                        "ballot persist failed; vote NOT granted (REP-004)");
                    return deny(acting);
                }
                tracing::info!(target: "fluxum::election",
                    candidate = %grant.voted_for, epoch = grant.epoch,
                    "vote granted (REP-030)");
                *ballot = Some(grant);
                // A live election is under way — don't start a rival one.
                self.note_contact();
                VoteResponse {
                    epoch: acting,
                    granted: true,
                }
            }
            None => {
                tracing::info!(target: "fluxum::election",
                    candidate = %req.member_name, epoch = req.epoch, acting,
                    "vote denied (REP-030)");
                deny(acting.max(ballot.as_ref().map_or(0, |b| b.epoch)))
            }
        }
    }

    /// Vote for ourselves in `epoch` (persisted first). Returns false if
    /// the ballot cannot be written — no candidacy without durability.
    fn self_ballot(&self, epoch: u64) -> bool {
        let grant = Ballot {
            epoch,
            voted_for: self.member_name.clone(),
        };
        if let Err(e) = persist_ballot(&self.log_dir, &grant) {
            tracing::error!(target: "fluxum::election", error = %e,
                "self-ballot persist failed; candidacy aborted (REP-004)");
            return false;
        }
        *self
            .ballot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(grant);
        true
    }

    /// The epoch a fresh candidacy would propose.
    fn next_epoch(&self) -> u64 {
        let acting = load_epoch(&self.log_dir).unwrap_or(self.role.epoch());
        let balloted = self
            .ballot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map_or(0, |b| b.epoch);
        acting.max(balloted) + 1
    }
}

/// Options for the election task of one member.
#[derive(Debug, Clone)]
pub struct ElectionOptions {
    /// The other replica-set members, `host:port` (REP-005).
    pub peers: Vec<String>,
    /// The server-peer token to authenticate with.
    pub token: Vec<u8>,
    /// `replication.election_timeout_ms` (REP-030: heartbeat-loss bound).
    pub election_timeout: Duration,
    /// The replica client options template (endpoint filled per target).
    pub replica: crate::replication::ReplicaOptions,
}

/// Deterministic per-member jitter so two followers rarely time out
/// together: `0..timeout/2`, keyed on `(member, attempt)`.
fn jitter(member: &str, attempt: u64, timeout: Duration) -> Duration {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    (member, attempt).hash(&mut hasher);
    let half = u64::try_from(timeout.as_millis())
        .unwrap_or(u64::MAX)
        .max(2)
        / 2;
    Duration::from_millis(hasher.finish() % half)
}

/// Run the member's election loop: follow (replica client + timer) →
/// candidacy on contact loss → promote on majority (REP-030/REP-032).
/// A member booted as primary PARKS, watching for a fence (REP-031): a
/// higher epoch on any channel demotes it to replica, and it rejoins the
/// follow loop under the new leader (REP-032 step 5 seen from the loser).
pub fn spawn_election(ctx: Arc<ShardContext>, state: Arc<ElectionState>, options: ElectionOptions) {
    tokio::spawn(async move {
        loop {
            if state.role.is_primary() {
                park_until_fenced(&ctx).await;
                demote(&ctx, &state);
            }
            tracing::debug!(target: "fluxum::election", member = %state.member_name,
                "election service: following");
            let mut attempt: u64 = 0;
            loop {
                follow_until_timeout(&ctx, &state, &options, attempt).await;
                tracing::debug!(target: "fluxum::election", member = %state.member_name,
                    attempt, "contact lost; standing for election (REP-030)");
                attempt += 1;
                if run_candidacy(&ctx, &state, &options).await {
                    tracing::info!(target: "fluxum::election",
                        epoch = state.role.epoch(),
                        "promoted to primary (REP-032)");
                    break; // back to the outer loop: park as primary
                }
                // Lost: grant the (possibly new) primary one full window to
                // make itself heard before standing again.
                state.note_contact();
            }
        }
    });
}

/// Park while primary until a fence lands (REP-031): the barrier already
/// refuses writes the moment `fenced` is set; here we wait to react.
async fn park_until_fenced(ctx: &Arc<ShardContext>) {
    loop {
        if ctx
            .replication_primary()
            .is_some_and(|primary| primary.fenced())
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Demote a fenced primary to replica (REP-031): adopt the higher epoch,
/// flip the published role, and note it so the follow loop resyncs. The
/// diverged-suffix truncation (REP-013) is a full resync — an `async`
/// primary's unreplicated tail is lost (REP-034), and in `semi_sync` those
/// txs were never client-visible (REP-021). Because a live in-process
/// store cannot be rebuilt in place (recovery needs a fresh store), the
/// clean rebuild happens on the next restart; until then the demoted
/// member follows and refuses writes (the barrier) and stale reads.
fn demote(ctx: &Arc<ShardContext>, state: &Arc<ElectionState>) {
    let epoch = ctx
        .replication_primary()
        .map_or_else(|| state.role.epoch(), |primary| primary.epoch());
    // Persist the epoch we were fenced to before acting under it (REP-004).
    let _ = persist_epoch(&state.log_dir, epoch);
    state.role.set(false, epoch);
    state.note_contact(); // give the new primary a window before standing
    ctx.metrics().set_replication_role(false);
    ctx.metrics().set_replication_epoch(epoch);
    tracing::warn!(target: "fluxum::election", epoch,
        "demoted to replica after a fence; rejoining the set (REP-031/REP-082)");
}

/// Follow the current primary until the election timer fires: run the
/// replica client against the best-known endpoint (rotating through peers
/// on failure) while watching the contact counter.
async fn follow_until_timeout(
    ctx: &Arc<ShardContext>,
    state: &Arc<ElectionState>,
    options: &ElectionOptions,
    attempt: u64,
) {
    let timeout =
        options.election_timeout + jitter(&state.member_name, attempt, options.election_timeout);
    let deadline_task = {
        let state = Arc::clone(state);
        async move {
            loop {
                let elapsed = state.contact.elapsed();
                if elapsed >= timeout {
                    return; // no contact for a full timeout — candidacy
                }
                tokio::time::sleep(timeout - elapsed).await;
            }
        }
    };
    let client_task = {
        let ctx = Arc::clone(ctx);
        let state = Arc::clone(state);
        let options = options.clone();
        async move {
            let mut target_ix = 0usize;
            loop {
                // Prefer the last known-good primary (REP-050): a live
                // session stamps it, so after a failover the follower dials
                // the winner directly instead of restarting from a dead
                // peer. If the hinted target is what just failed, drop it
                // and rotate through the configured peers to rediscover.
                let hinted = state.role.primary_hint();
                let target = hinted
                    .clone()
                    .unwrap_or_else(|| options.peers[target_ix % options.peers.len()].clone());
                let mut replica = options.replica.clone();
                replica.primary = target.clone();
                if let Err(e) = crate::replication::replica_session(&ctx, &replica).await {
                    tracing::debug!(target: "fluxum::election", error = %e, target = %target,
                        "replica session ended; rotating");
                }
                if hinted.is_some() {
                    state.role.set_hint(None); // that endpoint is no longer serving
                } else {
                    target_ix += 1;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    };
    tokio::select! {
        () = deadline_task => {}
        () = client_task => {}
    }
}

/// One candidacy round (REP-030): self-ballot for `acting + 1`, request
/// votes from every peer, promote on majority. Returns true when promoted.
async fn run_candidacy(
    ctx: &Arc<ShardContext>,
    state: &Arc<ElectionState>,
    options: &ElectionOptions,
) -> bool {
    let epoch = state.next_epoch();
    if !state.self_ballot(epoch) {
        return false;
    }
    let log = ctx.engine.pipeline().log();
    let request = VoteRequest {
        shard_id: state.shard_id,
        member_name: state.member_name.clone(),
        epoch,
        last_log_epoch: log.epoch(),
        last_applied_tx_id: log.durable_tx_id().ok().flatten().unwrap_or(0),
    };
    tracing::info!(target: "fluxum::election", epoch,
        head = request.last_applied_tx_id, "election started (REP-082)");
    ctx.metrics().note_election();

    let vote_timeout = options.election_timeout.min(Duration::from_secs(2));
    let mut tally = 1usize; // our own persisted ballot
    let mut asks = Vec::new();
    for peer in &options.peers {
        let peer = peer.clone();
        let token = options.token.clone();
        let request = request.clone();
        asks.push(tokio::spawn(async move {
            tokio::time::timeout(vote_timeout, request_vote(&peer, &token, &request))
                .await
                .unwrap_or_else(|_| {
                    Err(FluxumError::Storage(format!("vote from {peer}: timed out")))
                })
        }));
    }
    let mut highest_seen = epoch;
    for ask in asks {
        if let Ok(Ok(response)) = ask.await {
            if response.granted {
                tally += 1;
            }
            highest_seen = highest_seen.max(response.epoch);
        }
    }
    if highest_seen > epoch {
        // A newer epoch exists — adopt the knowledge and stand down.
        let _ = persist_epoch(&state.log_dir, highest_seen);
        tracing::info!(target: "fluxum::election", ours = epoch, theirs = highest_seen,
            "election lost to a higher epoch (REP-082)");
        return false;
    }
    let members = options.peers.len() + 1;
    if tally < majority(members) {
        tracing::info!(target: "fluxum::election", epoch, tally, members,
            "election lost — no majority (REP-082)");
        return false;
    }

    // REP-032 promotion: persist the epoch, raise the writer's envelope
    // epoch, publish the role — all before any client-visible write.
    if let Err(e) = persist_epoch(&state.log_dir, epoch) {
        tracing::error!(target: "fluxum::election", error = %e,
            "epoch persist failed; promotion aborted (REP-004)");
        return false;
    }
    if let Err(e) = log.set_epoch(epoch).await {
        tracing::error!(target: "fluxum::election", error = %e,
            "writer epoch raise failed; promotion aborted");
        return false;
    }
    if let Some(primary) = ctx.replication_primary() {
        primary.adopt_epoch(epoch);
    }
    state.role.set(true, epoch);
    state.role.set_hint(None);
    ctx.metrics().set_replication_role(true);
    ctx.metrics().set_replication_epoch(epoch);
    tracing::info!(target: "fluxum::election", epoch, tally, members,
        "election won (REP-030/REP-082)");
    true
}

/// Dial one peer and ask for its vote: authenticate as a server peer,
/// send the `VoteRequest`, await the `VoteResponse`.
async fn request_vote(
    peer: &str,
    token: &[u8],
    request: &VoteRequest,
) -> Result<VoteResponse, FluxumError> {
    let io_err = |e: std::io::Error| FluxumError::Storage(format!("vote io: {e}"));
    let mut stream = tokio::net::TcpStream::connect(peer).await.map_err(io_err)?;
    let codec = FrameCodec::default();
    let encode = |m: &ClientMessage| {
        m.encode()
            .map_err(|e| FluxumError::Storage(format!("vote encode: {e}")))
            .and_then(|body| {
                codec
                    .encode(&body)
                    .map_err(|e| FluxumError::Storage(format!("vote frame: {e}")))
            })
    };
    let auth = ClientMessage::Authenticate(fluxum_protocol::Authenticate {
        id: 1,
        token: token.to_vec(),
        compression: None,
        tx_updates: None,
        namespace: None,
    });
    stream.write_all(&encode(&auth)?).await.map_err(io_err)?;
    stream
        .write_all(&encode(&ClientMessage::VoteRequest(request.clone()))?)
        .await
        .map_err(io_err)?;
    let mut reader = crate::replication::FrameReader::new(codec);
    loop {
        match reader.next_message(&mut stream).await? {
            ServerMessage::VoteResponse(response) => return Ok(response),
            ServerMessage::AuthResult(_) => {}
            ServerMessage::Error(e) => {
                return Err(FluxumError::Storage(format!(
                    "vote refused by {peer}: {}",
                    e.message
                )));
            }
            other => {
                tracing::debug!(target: "fluxum::election", ?other,
                    "ignoring non-vote frame");
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn req(member: &str, epoch: u64, log_epoch: u64, tx: u64) -> VoteRequest {
        VoteRequest {
            shard_id: 0,
            member_name: member.into(),
            epoch,
            last_log_epoch: log_epoch,
            last_applied_tx_id: tx,
        }
    }

    #[test]
    fn the_ballot_round_trips_and_rejects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(load_ballot(dir.path()).unwrap(), None);
        let ballot = Ballot {
            epoch: 4,
            voted_for: "node-b".into(),
        };
        persist_ballot(dir.path(), &ballot).unwrap();
        assert_eq!(load_ballot(dir.path()).unwrap(), Some(ballot));
        std::fs::write(dir.path().join(BALLOT_FILE), b"\xc1junk").unwrap();
        assert!(load_ballot(dir.path()).is_err());
    }

    #[test]
    fn majority_is_strict() {
        assert_eq!(majority(1), 1);
        assert_eq!(majority(2), 2);
        assert_eq!(majority(3), 2);
        assert_eq!(majority(4), 3);
        assert_eq!(majority(5), 3);
    }

    #[test]
    fn votes_follow_the_raft_safety_rules() {
        let head = (3, 100);

        // A stale candidacy (epoch not beyond the acting one) is denied.
        assert_eq!(
            decide_vote(5, None, head, false, &req("a", 5, 3, 100)),
            None
        );
        assert_eq!(
            decide_vote(5, None, head, false, &req("a", 4, 3, 100)),
            None
        );

        // A fresh epoch with an up-to-date log wins the ballot.
        let grant = decide_vote(5, None, head, false, &req("a", 6, 3, 100)).unwrap();
        assert_eq!((grant.epoch, grant.voted_for.as_str()), (6, "a"));

        // Leader stickiness (Raft §9.6): while we still hear a primary,
        // even a perfect candidacy is denied — a slow follower's timeout
        // must not unseat a healthy primary.
        assert_eq!(decide_vote(5, None, head, true, &req("a", 6, 3, 100)), None);

        // One ballot per epoch: a different candidate is denied, the SAME
        // candidate re-asking is re-granted (idempotent retry).
        let ballot = Ballot {
            epoch: 6,
            voted_for: "a".into(),
        };
        assert_eq!(
            decide_vote(5, Some(&ballot), head, false, &req("b", 6, 3, 100)),
            None
        );
        assert!(decide_vote(5, Some(&ballot), head, false, &req("a", 6, 3, 100)).is_some());
        // And a ballot in a NEWER epoch blocks older candidacies entirely.
        assert_eq!(
            decide_vote(5, Some(&ballot), head, false, &req("b", 6, 9, 999)),
            None
        );

        // REP-030 up-to-dateness: a behind log head is denied on either
        // component; equal is enough; ahead on epoch beats tx count.
        assert_eq!(decide_vote(5, None, head, false, &req("a", 6, 3, 99)), None);
        assert_eq!(
            decide_vote(5, None, head, false, &req("a", 6, 2, 500)),
            None
        );
        assert!(decide_vote(5, None, head, false, &req("a", 6, 4, 0)).is_some());
    }

    #[test]
    fn role_state_publishes_and_hints() {
        let role = RoleState::new(false, 3);
        assert!(!role.is_primary());
        assert_eq!(role.epoch(), 3);
        assert_eq!(role.primary_hint(), None);
        role.set_hint(Some("10.0.0.5:15801".into()));
        assert_eq!(role.primary_hint().as_deref(), Some("10.0.0.5:15801"));
        role.set(true, 4);
        assert!(role.is_primary());
        assert_eq!(role.epoch(), 4);
        // Epochs never regress through the role surface.
        role.set(false, 2);
        assert_eq!(role.epoch(), 4);
    }

    #[test]
    fn jitter_is_deterministic_and_bounded() {
        let t = Duration::from_millis(3000);
        let a = jitter("node-a", 0, t);
        assert_eq!(a, jitter("node-a", 0, t), "same inputs, same jitter");
        assert!(a < t / 2 + Duration::from_millis(1));
        // Different members almost surely disagree (fixed vectors here).
        assert_ne!(jitter("node-a", 0, t), jitter("node-b", 0, t));
    }

    #[tokio::test]
    async fn a_replica_reads_go_stale_after_the_staleness_bound() {
        let dir = tempfile::tempdir().unwrap();
        // A replica with a 40 ms staleness bound; contact is fresh at birth.
        let replica = ElectionState::new(
            0,
            "node-b".into(),
            dir.path().to_path_buf(),
            false,
            1,
            Duration::from_secs(3),
            Duration::from_millis(40),
        )
        .unwrap();
        assert!(!replica.read_is_stale(), "fresh contact is not stale");
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            replica.read_is_stale(),
            "no contact past the bound is stale"
        );
        // Contact resets the clock.
        replica.note_contact();
        assert!(!replica.read_is_stale(), "contact clears staleness");

        // A PRIMARY is never stale — it holds the head, whatever the clock.
        let primary = ElectionState::new(
            0,
            "node-a".into(),
            dir.path().to_path_buf(),
            true,
            1,
            Duration::from_secs(3),
            Duration::from_millis(1),
        )
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!primary.read_is_stale(), "a primary is never read-stale");
    }
}
