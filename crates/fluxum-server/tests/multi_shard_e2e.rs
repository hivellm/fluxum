//! SPEC-007 SHD-010/011 — the multi-shard **server assembly**: the boot
//! path provisions `sharding.shards` fully-independent hosts behind a
//! `ShardCoord`, sessions acquire shard affinity at authentication and
//! route every call to their shard, and the admin surface reports every
//! host.
//!
//! T5.4 delivered `ShardCoord`/`ShardHost` as a library with its own
//! suites (`shard_coord.rs`, `entity_handoff.rs`) over hand-built hosts;
//! what was never covered — because it did not exist — is the path from
//! `sharding.shards: 2` in a config to a serving two-shard deployment.
//! These tests run that path end to end.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use fluxum_core::reducer::ReducerContext;
use fluxum_core::types::Identity;
use fluxum_macros as fluxum;
use fluxum_protocol::{ClientMessage, ServerMessage};
use fluxum_server::session::Session;

// The test binary's own partitioned module: `Note.owner` is the partition
// key, so identity affinity (SHD-011) spreads distinct callers across
// shards. The demo module's tables are all unpartitioned, which is exactly
// why they cannot exercise routing.
#[fluxum::table(public, partition_by(owner))]
#[derive(Debug, Clone, PartialEq)]
pub struct Note {
    /// Server-assigned id.
    #[primary_key]
    #[auto_inc]
    pub id: u64,
    /// The partition key: the caller who wrote it.
    pub owner: Identity,
    /// Payload.
    pub body: String,
}

/// Insert a note owned by the caller.
#[fluxum::reducer]
fn add_note(ctx: &ReducerContext, body: String) -> Result<(), String> {
    ctx.tx
        .insert(Note {
            id: 0,
            owner: ctx.identity,
            body,
        })
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// A two-shard config over `dir`.
fn config(dir: &std::path::Path) -> fluxum_core::config::Config {
    let mut config = fluxum_core::config::Config::default();
    config.storage.data_dir = dir.into();
    config.storage.commit_log_dir = dir.join("log");
    config.storage.checkpoint_dir = dir.join("checkpoints");
    config.storage.page_dir = dir.join("pages");
    config.auth.provider = fluxum_core::config::AuthProvider::None;
    config.sharding.shards = fluxum_core::config::AutoOr::Value(2);
    config
}

/// A token whose `none`-provider identity (`SHA-256(token)`) has affinity
/// to `shard` under `coord` — found by scanning, since the hash is
/// deterministic but not invertible.
fn token_for_shard(coord: &Arc<fluxum_server::shard::ShardCoord>, shard: u32) -> Vec<u8> {
    for n in 0..10_000u32 {
        let token = format!("client-{n}").into_bytes();
        if coord.affinity_of(&Identity::from_token(&token)) == shard {
            return token;
        }
    }
    panic!("no token hashed to shard {shard} in 10k tries — affinity is broken");
}

/// An authenticated session over the default shard's context; the SHD-011
/// rebind happens inside `Authenticate`.
async fn authed(ctx: &Arc<fluxum_server::ShardContext>, token: &[u8]) -> Session {
    let mut session = Session::new(Arc::clone(ctx));
    let routed = session
        .handle(ClientMessage::Authenticate(fluxum_protocol::Authenticate {
            id: 1,
            token: token.to_vec(),
            compression: None,
            tx_updates: None,
            namespace: None,
        }))
        .await;
    assert!(
        routed
            .responses
            .iter()
            .any(|m| matches!(m, ServerMessage::AuthResult(_))),
        "authentication failed: {:?}",
        routed.responses
    );
    session
}

async fn call(session: &mut Session, id: u32, reducer: &str, body: &str) {
    let routed = session
        .handle(ClientMessage::ReducerCall(fluxum_protocol::ReducerCall {
            id,
            reducer: reducer.into(),
            version: None,
            args: vec![fluxum_core::reducer::FluxValue::Str(body.into())],
            idempotency_key: None,
        }))
        .await;
    assert!(
        routed
            .responses
            .iter()
            .any(|m| matches!(m, ServerMessage::ReducerResult(r) if r.outcome.is_ok())),
        "reducer call failed: {:?}",
        routed.responses
    );
}

/// Committed `Note` rows on one shard's store.
fn notes_on(ctx: &Arc<fluxum_server::ShardContext>) -> usize {
    let table = ctx.store().table_id("Note").unwrap();
    ctx.store().snapshot().scan(table).unwrap().len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_two_shard_config_provisions_two_hosts_and_routes_by_affinity() {
    fluxum_demo::link();
    let dir = tempfile::tempdir().unwrap();
    let assembled = fluxum_server::boot::assemble(&config(dir.path())).unwrap();

    // The configured count is honoured: a coordinator over exactly the
    // shards asked for, each with its own on-disk world (SHD-020).
    let coord = assembled
        .coord
        .as_ref()
        .expect("shards=2 must assemble a coordinator — no silent downgrade");
    assert_eq!(coord.shard_ids().collect::<Vec<_>>(), vec![0, 1]);
    for shard in 0..2u32 {
        assert!(
            dir.path()
                .join("log")
                .join(format!("shard-{shard}"))
                .is_dir(),
            "shard {shard} owns its own commit-log directory"
        );
        assert!(
            dir.path()
                .join("pages")
                .join(format!("shard-{shard}"))
                .is_dir(),
            "shard {shard} owns its own page directory"
        );
    }
    let host0 = coord.host(0).cloned().unwrap();
    let host1 = coord.host(1).cloned().unwrap();
    assert!(
        !Arc::ptr_eq(host0.store(), host1.store()),
        "shards share nothing (SHD-020)"
    );

    // SHD-011: a session authenticates on the default shard's listener and
    // rebinds to its affinity shard; its writes land there and only there.
    let token1 = token_for_shard(coord, 1);
    let mut session1 = authed(&assembled.ctx, &token1).await;
    assert_eq!(
        session1.ctx().shard_id,
        1,
        "the session rebound to its affinity shard"
    );
    call(&mut session1, 2, "add_note", "on shard one").await;
    assert_eq!(
        notes_on(&host1),
        1,
        "the write landed on the affinity shard"
    );
    assert_eq!(notes_on(&host0), 0, "and nowhere else");

    let token0 = token_for_shard(coord, 0);
    let mut session0 = authed(&assembled.ctx, &token0).await;
    assert_eq!(session0.ctx().shard_id, 0);
    call(&mut session0, 2, "add_note", "on shard zero").await;
    assert_eq!(notes_on(&host0), 1);
    assert_eq!(notes_on(&host1), 1, "shard 1 undisturbed");

    // The affinity binding survives the per-request HTTP rebuild: a session
    // rebuilt from persisted state re-resolves the same shard (SHD-011).
    let state = session1.into_state();
    let rebuilt = Session::with_state(Arc::clone(&assembled.ctx), state);
    assert_eq!(
        rebuilt.ctx().shard_id,
        1,
        "with_state re-resolves the persisted affinity shard"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_single_shard_config_assembles_no_coordinator() {
    fluxum_demo::link();
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.sharding.shards = fluxum_core::config::AutoOr::Value(1);
    let assembled = fluxum_server::boot::assemble(&config).unwrap();
    assert!(
        assembled.coord.is_none(),
        "one shard is the assembly every deployment has always run"
    );
    // And the storage layout is the classic one — no shard-0 subdirectory,
    // so existing data directories keep recovering unchanged.
    assert!(dir.path().join("log").is_dir());
    assert!(!dir.path().join("log").join("shard-0").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unhostable_shard_count_is_refused_not_downgraded() {
    fluxum_demo::link();
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    // Pin the budget rather than trusting the host's `auto` derivation: on
    // a large workstation the derived budget is big enough that even 4096
    // pools clear the per-shard floor — and the boot then really assembles
    // 4096 shards. 128 shards over 256 MiB leaves ~1.6 MiB per pool, below
    // the floor on every machine; the boot must refuse with the actual
    // arithmetic, not start one shard and pretend.
    config.memory.budget =
        fluxum_core::config::AutoOr::Value(fluxum_core::config::ByteSize(256 << 20));
    config.sharding.shards = fluxum_core::config::AutoOr::Value(128);
    let err = match fluxum_server::boot::assemble(&config) {
        Ok(_) => panic!("an unhostable shard count booted anyway"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("sharding.shards"), "{err}");
    assert!(err.contains("per shard"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metrics_report_every_shard_distinctly() {
    use std::fmt::Write as _;

    fluxum_demo::link();
    let dir = tempfile::tempdir().unwrap();
    let assembled = fluxum_server::boot::assemble(&config(dir.path())).unwrap();
    let coord = assembled.coord.as_ref().unwrap();

    // One write on each shard, so the per-shard row gauges differ from a
    // copy of one shard's block.
    for shard in [0u32, 1] {
        let token = token_for_shard(coord, shard);
        let mut session = authed(&assembled.ctx, &token).await;
        call(&mut session, 2, "add_note", "row").await;
    }

    // The exposition the soak's TST-112 assertion reads: every shard's own
    // capacity gauge, and every shard's own data.
    let http = fluxum_server::http::serve(
        Arc::clone(&assembled.ctx),
        "127.0.0.1:0",
        fluxum_server::http::HttpOptions::default(),
    )
    .await
    .unwrap();
    let addr = http.local_addr;
    let body = admin_get(&format!("http://{addr}/metrics")).await;
    http.shutdown();

    let mut missing = String::new();
    for series in [
        "fluxum_bufferpool_capacity_bytes{shard=\"0\"}",
        "fluxum_bufferpool_capacity_bytes{shard=\"1\"}",
        "fluxum_table_rows{shard=\"0\",table=\"Note\"} 1",
        "fluxum_table_rows{shard=\"1\",table=\"Note\"} 1",
        "fluxum_reclaim_pending_pages{shard=\"0\"}",
        "fluxum_reclaim_pending_pages{shard=\"1\"}",
    ] {
        if !body.contains(series) {
            let _ = writeln!(missing, "  missing: {series}");
        }
    }
    assert!(missing.is_empty(), "{missing}\nexposition:\n{body}");
}

/// A minimal HTTP GET. The admin transport keeps the connection alive, so
/// the body is read to its `Content-Length`, never to EOF — a `read_to_end`
/// here blocks forever on the kept-open socket (the same pitfall
/// `fluxum-bench`'s `scrape_metrics` documents).
async fn admin_get(url: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = url
        .strip_prefix("http://")
        .and_then(|u| u.split_once('/'))
        .map(|(host, path)| (host.to_owned(), format!("/{path}")))
        .unwrap();
    let mut stream = tokio::net::TcpStream::connect(&addr.0).await.unwrap();
    stream
        .write_all(
            format!(
                "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
                addr.1, addr.0
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut raw = Vec::new();
    let mut chunk = [0u8; 8192];
    let body_start = loop {
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(n > 0, "connection closed before headers");
        raw.extend_from_slice(&chunk[..n]);
        if let Some(split) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break split + 4;
        }
    };
    let head = String::from_utf8_lossy(&raw[..body_start]).into_owned();
    let content_length: usize = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())
                .flatten()
        })
        .expect("admin responses carry Content-Length");
    let mut body = raw[body_start..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut chunk).await.unwrap();
        assert!(n > 0, "connection closed mid-body");
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);
    String::from_utf8_lossy(&body).into_owned()
}
