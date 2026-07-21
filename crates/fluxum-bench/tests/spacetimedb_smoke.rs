//! Live-server smoke for the SpacetimeDB competitive-baseline side
//! (TST-097): all six `BenchClient` operations against a real pinned
//! standalone with the demo module published
//! (`docs/parity/spacetimedb-baseline.md`).
//!
//! Gated on `FLUXUM_BENCH_STDB_URL` (e.g. `http://127.0.0.1:15300`) so the
//! suite stays green on machines without the docker server; the parity gate
//! for this side is the release harness run, this asserts the plumbing.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fluxum_bench::spacetimedb_side::SpacetimeDbSide;
use fluxum_bench::workload::Side;

#[test]
fn all_six_bench_ops_work_against_a_live_server() {
    let Ok(url) = std::env::var("FLUXUM_BENCH_STDB_URL") else {
        eprintln!("skipped: FLUXUM_BENCH_STDB_URL unset (needs a live SpacetimeDB)");
        return;
    };
    let db =
        std::env::var("FLUXUM_BENCH_STDB_DB").unwrap_or_else(|_| "fluxum-parity-demo".to_owned());
    let side = SpacetimeDbSide::new(url, db);

    // Fresh identities per invocation: rows persist server-side across test
    // runs, and `prepare_reads` asserts on this identity's row count.
    let base = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    // add_task: acked write.
    let mut writer = side.client(base).expect("connect writer");
    writer.add_task("smoke task").expect("add_task acked");

    // subscribe_chat + send_chat: a published message reaches a subscriber
    // on another session, channel-filtered.
    let channel = (base % 1_000_000) as u32;
    let (tx, rx) = mpsc::channel::<String>();
    let mut subscriber = side.client(base + 1).expect("connect subscriber");
    subscriber
        .subscribe_chat(
            channel,
            Box::new(move |content| {
                let _ = tx.send(content.to_owned());
            }),
        )
        .expect("subscribe_chat applied");
    writer
        .send_chat(channel, "smoke message")
        .expect("send_chat acked");
    let delivered = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("delivery within 10 s");
    assert_eq!(delivered, "smoke message");

    // prepare_reads + hot_read: the client cache serves this user's rows.
    let mut reader = side.client(base + 2).expect("connect reader");
    reader.prepare_reads(5).expect("prepare_reads");
    let title = reader.hot_read().expect("hot_read");
    assert!(title.starts_with("seed "), "unexpected title {title:?}");

    // load_my_data: a FRESH session's initial sync sees the seeded rows —
    // and only this identity's rows (the module's RLS owner filter).
    let mut fresh = side.client(base + 2).expect("reconnect reader");
    let rows = fresh.load_my_data().expect("load_my_data");
    assert_eq!(rows, 5, "owner-filtered initial sync row count");
    let mut stranger = side.client(base + 3).expect("connect stranger");
    let stranger_rows = stranger.load_my_data().expect("stranger load_my_data");
    assert_eq!(stranger_rows, 0, "RLS must hide other users' tasks");
}
