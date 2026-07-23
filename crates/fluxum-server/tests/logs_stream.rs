//! SPEC-024 DEV-012 — `GET /logs`: the structured-log stream behind
//! `fluxum logs`. Own test binary: the tap rides the process-global tracing
//! subscriber, which this process must win.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::config::{LogFormat, LoggingConfig};
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_server::ShardContext;
use fluxum_server::http::{self, HttpOptions};

const SHARD: u32 = 9;

static NOTE_COLS: &[ColumnSchema] = &[ColumnSchema {
    name: "id",
    ty: FluxType::U64,
}];
static NOTE: TableSchema = TableSchema {
    name: "Note",
    columns: NOTE_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fn build_ctx() -> Arc<ShardContext> {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&NOTE]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.path().join("log"),
            SHARD,
            1,
            CommitLogOptions::default(),
        )
        .unwrap(),
    );
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let engine = ReducerEngine::new(
        pipeline,
        Arc::new(ReducerRegistry::from_defs([]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("logs-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    ShardContext::new(engine, subs, auth, SHARD, 256)
}

/// Read from `socket` until `needle` appears or the deadline passes; returns
/// everything read.
async fn read_until(socket: &mut TcpStream, needle: &str, deadline: Duration) -> String {
    let mut collected = Vec::new();
    let mut chunk = [0u8; 4096];
    let end = tokio::time::Instant::now() + deadline;
    loop {
        let text = String::from_utf8_lossy(&collected).into_owned();
        if text.contains(needle) {
            return text;
        }
        let Ok(read) = tokio::time::timeout_at(end, socket.read(&mut chunk)).await else {
            return text; // deadline — return what arrived for the assert message
        };
        match read {
            Ok(0) | Err(_) => return String::from_utf8_lossy(&collected).into_owned(),
            Ok(n) => collected.extend_from_slice(&chunk[..n]),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logs_endpoint_serves_the_ring_and_follows_live_lines() {
    // The tap installs with the global subscriber (DEV-012). JSON console
    // format keeps the test output machine-shaped too.
    let _handle = fluxum_server::logging::init(&LoggingConfig {
        level: "info".into(),
        format: LogFormat::Json,
    })
    .expect("this test binary owns the global subscriber");

    tracing::info!(target: "logs_test", "ring-line-before-connect");

    let ctx = build_ctx();
    let server = http::serve(Arc::clone(&ctx), "127.0.0.1:0", HttpOptions::default())
        .await
        .unwrap();

    // Catch-up only (no follow): the pre-connect line arrives, then the
    // stream terminates with the last chunk.
    let mut socket = TcpStream::connect(server.local_addr).await.unwrap();
    socket
        .write_all(
            format!(
                "GET /logs HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
                server.local_addr
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let response = read_until(&mut socket, "0\r\n\r\n", Duration::from_secs(5)).await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response.contains("application/x-ndjson"),
        "content type: {response}"
    );
    assert!(
        response.contains("ring-line-before-connect"),
        "the ring served the pre-connect line: {response}"
    );

    // Follow: a line emitted AFTER the stream attaches arrives live.
    let mut socket = TcpStream::connect(server.local_addr).await.unwrap();
    socket
        .write_all(
            format!(
                "GET /logs?follow=1 HTTP/1.1\r\nHost: {}\r\n\r\n",
                server.local_addr
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    // Wait until the catch-up is through before emitting the live line.
    let head = read_until(
        &mut socket,
        "ring-line-before-connect",
        Duration::from_secs(5),
    )
    .await;
    assert!(head.contains("ring-line-before-connect"), "{head}");
    tracing::warn!(target: "logs_test", "live-line-after-attach");
    let live = read_until(
        &mut socket,
        "live-line-after-attach",
        Duration::from_secs(5),
    )
    .await;
    assert!(
        live.contains("live-line-after-attach"),
        "the follow stream delivers new lines: {live}"
    );
    // The line is the tap's JSON, carrying its level for the CLI's filter.
    assert!(live.contains("\"level\":\"WARN\""), "{live}");

    server.shutdown();
}
