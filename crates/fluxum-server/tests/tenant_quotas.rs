//! SPEC-025 §7 (OPS-060/061) — per-tenant resource quotas: a tenant that
//! saturates its reducer-rate quota gets 429s while its neighbour is
//! untouched, subscription/memory ceilings refuse only the offender, and
//! `fluxum_tenant_*` reports usage against the ceilings.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::Result;
use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{
    FluxValue, LifecycleHooks, ReducerContext, ReducerDef, ReducerEngine, ReducerRegistry,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, Table, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_protocol::{
    Authenticate, ClientMessage, FrameCodec, ReducerCall, ServerMessage, Subscribe, codes,
};
use fluxum_server::ShardContext;
use fluxum_server::namespace::Namespace;
use fluxum_server::quota::TenantQuotas;
use fluxum_server::tcp::{self, TcpOptions};

const SHARD: u32 = 11;

static NOTE_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "text",
        ty: FluxType::Str,
    },
];
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

#[derive(Debug, Clone, PartialEq)]
struct Note {
    id: u64,
    text: String,
}

impl Table for Note {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &NOTE;
    fn primary_key(&self) -> u64 {
        self.id
    }
    fn into_values(self) -> Vec<RowValue> {
        vec![RowValue::U64(self.id), RowValue::Str(self.text)]
    }
    fn from_values(values: &[RowValue]) -> Result<Self> {
        match values {
            [RowValue::U64(id), RowValue::Str(text)] => Ok(Self {
                id: *id,
                text: text.clone(),
            }),
            other => Err(fluxum_core::FluxumError::Storage(format!(
                "Note: {other:?}"
            ))),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn write_note(ctx: &ReducerContext<'_, '_, '_>, args: &[FluxValue]) -> Result<()> {
    let text = match args.first() {
        Some(FluxValue::Str(s)) => s.clone(),
        _ => return Err(fluxum_core::FluxumError::Reducer("write_note(text)".into())),
    };
    ctx.tx.insert(Note { id: 0, text })?;
    Ok(())
}

fn check_args(args: &[FluxValue]) -> Result<()> {
    fluxum_core::reducer::args::check_arity("write_note", args, 1)
}

static WRITE_NOTE: ReducerDef = ReducerDef {
    name: "write_note",
    handler: write_note,
    check_args,
    client_callable: true,
    max_rate_per_sec: 0,
};

fn build_db(name: &str) -> (ReducerEngine, SubscriptionManager) {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&NOTE]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(
            &dir.path().join(format!("log-{name}")),
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
        Arc::new(ReducerRegistry::from_defs([&WRITE_NOTE]).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity(name),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    (engine, subs)
}

struct Harness {
    server: tcp::TcpServer,
    ctx: Arc<ShardContext>,
}

/// `acme` carries `quotas`; `globex` is unquotaed (the neighbour).
async fn start(quotas: TenantQuotas) -> Harness {
    let (engine, subs) = build_db("default");
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 256);

    let (engine, subs) = build_db("acme");
    ctx.register_namespace(Namespace::with_quotas("acme", engine, subs, 256, quotas))
        .unwrap();
    let (engine, subs) = build_db("globex");
    ctx.register_namespace(Namespace::new("globex", engine, subs, 256))
        .unwrap();

    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    Harness { server, ctx }
}

struct Client {
    stream: TcpStream,
    codec: FrameCodec,
    buf: Vec<u8>,
}

impl Client {
    async fn connect(addr: std::net::SocketAddr, namespace: &str) -> Self {
        let mut c = Self {
            stream: TcpStream::connect(addr).await.unwrap(),
            codec: FrameCodec::default(),
            buf: Vec::new(),
        };
        c.send(ClientMessage::Authenticate(Authenticate {
            id: 1,
            token: b"user".to_vec(),
            compression: None,
            tx_updates: None,
            namespace: Some(namespace.to_owned()),
        }))
        .await;
        assert!(matches!(
            c.recv().await.unwrap(),
            ServerMessage::AuthResult(_)
        ));
        c
    }

    async fn send(&mut self, message: ClientMessage) {
        let body = message.encode().unwrap();
        let framed = self.codec.encode(&body).unwrap();
        self.stream.write_all(&framed).await.unwrap();
    }

    async fn recv(&mut self) -> Option<ServerMessage> {
        loop {
            if let Ok(Some((frame, consumed))) = self.codec.decode(&self.buf) {
                let msg = match frame {
                    fluxum_protocol::Frame::Body(body) => {
                        Some(ServerMessage::decode(body).unwrap())
                    }
                    fluxum_protocol::Frame::KeepAlive => None,
                };
                self.buf.drain(..consumed);
                if let Some(msg) = msg {
                    return Some(msg);
                }
                continue;
            }
            let mut chunk = [0u8; 4096];
            match self.stream.read(&mut chunk).await {
                Ok(0) => return None,
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(_) => return None,
            }
        }
    }

    /// Call the reducer; `Ok(())` committed, `Err(code)` refused.
    async fn write_note(&mut self, text: &str) -> std::result::Result<(), u16> {
        self.send(ClientMessage::ReducerCall(ReducerCall {
            id: 2,
            reducer: "write_note".into(),
            version: None,
            args: vec![FluxValue::Str(text.into())],
            idempotency_key: None,
        }))
        .await;
        match self.recv().await.unwrap() {
            ServerMessage::ReducerResult(r) => match r.outcome {
                Ok(()) => Ok(()),
                Err(e) => Err(e.code),
            },
            ServerMessage::Error(e) => Err(e.code),
            other => panic!("unexpected {other:?}"),
        }
    }

    async fn subscribe(&mut self, sql: &str) -> std::result::Result<(), u16> {
        self.send(ClientMessage::Subscribe(Subscribe {
            id: 3,
            queries: vec![sql.to_owned()],
        }))
        .await;
        match self.recv().await.unwrap() {
            ServerMessage::InitialData(_) => Ok(()),
            ServerMessage::Error(e) => Err(e.code),
            other => panic!("unexpected {other:?}"),
        }
    }
}

// --- OPS-060 acceptance: noisy neighbour contained -------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_over_its_reducer_quota_is_throttled_while_its_neighbour_is_not() {
    let h = start(TenantQuotas {
        max_reducer_calls_per_sec: Some(3.0),
        ..TenantQuotas::default()
    })
    .await;

    let mut acme = Client::connect(h.server.local_addr, "acme").await;
    // The burst is admitted, then acme is throttled.
    for i in 0..3 {
        assert_eq!(acme.write_note(&format!("a{i}")).await, Ok(()), "burst {i}");
    }
    assert_eq!(
        acme.write_note("over").await,
        Err(codes::REDUCER_RATE_LIMITED),
        "a tenant past its quota gets a retryable 429"
    );

    // globex is a different tenant with no quota: entirely unaffected while
    // acme keeps hammering.
    let mut globex = Client::connect(h.server.local_addr, "globex").await;
    for i in 0..20 {
        assert_eq!(
            globex.write_note(&format!("g{i}")).await,
            Ok(()),
            "the neighbour is never charged for acme's excess"
        );
        let _ = acme.write_note("still hammering").await;
    }

    // The refusal is counted against acme only.
    let acme_ns = h
        .ctx
        .namespaces()
        .into_iter()
        .find(|n| n.name() == "acme")
        .unwrap();
    assert!(
        acme_ns
            .quotas()
            .exceeded(fluxum_server::quota::Quota::ReducerRate)
            > 0
    );
    let globex_ns = h
        .ctx
        .namespaces()
        .into_iter()
        .find(|n| n.name() == "globex")
        .unwrap();
    assert_eq!(
        globex_ns
            .quotas()
            .exceeded(fluxum_server::quota::Quota::ReducerRate),
        0
    );

    h.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_rate_quota_refills_so_a_throttled_tenant_recovers() {
    let h = start(TenantQuotas {
        max_reducer_calls_per_sec: Some(4.0),
        ..TenantQuotas::default()
    })
    .await;
    let mut acme = Client::connect(h.server.local_addr, "acme").await;
    for _ in 0..4 {
        acme.write_note("burst").await.unwrap();
    }
    assert_eq!(
        acme.write_note("over").await,
        Err(codes::REDUCER_RATE_LIMITED)
    );
    // A quota is a rate, not a lifetime budget: after a refill window the
    // tenant is served again.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(acme.write_note("later").await, Ok(()));
    h.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_at_its_subscription_quota_is_refused_but_its_neighbour_is_not() {
    let h = start(TenantQuotas {
        max_subscriptions: Some(1),
        ..TenantQuotas::default()
    })
    .await;

    let mut acme = Client::connect(h.server.local_addr, "acme").await;
    assert_eq!(acme.subscribe("SELECT * FROM Note").await, Ok(()));
    // A second, distinct subscription is over the ceiling.
    let refused = acme.subscribe("SELECT * FROM Note WHERE id = 1").await;
    assert!(refused.is_err(), "a tenant at its ceiling is refused");

    // The neighbour subscribes freely.
    let mut globex = Client::connect(h.server.local_addr, "globex").await;
    for i in 0..5 {
        assert_eq!(
            globex
                .subscribe(&format!("SELECT * FROM Note WHERE id = {i}"))
                .await,
            Ok(()),
            "an unquotaed tenant is unaffected"
        );
    }
    h.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_over_its_memory_quota_cannot_write() {
    // A ceiling of 1 byte: any committed row puts acme over it, so the write
    // *after* the first is refused with a typed exhaustion error.
    let h = start(TenantQuotas {
        max_memory_bytes: Some(1),
        ..TenantQuotas::default()
    })
    .await;
    let mut acme = Client::connect(h.server.local_addr, "acme").await;
    assert_eq!(
        acme.write_note("first").await,
        Ok(()),
        "empty tenant writes"
    );
    let err = acme.write_note("second").await.unwrap_err();
    assert_eq!(err, codes::CLUSTER_SHARD_UNAVAILABLE);

    // globex, unquotaed, keeps writing.
    let mut globex = Client::connect(h.server.local_addr, "globex").await;
    for _ in 0..5 {
        assert_eq!(globex.write_note("fine").await, Ok(()));
    }
    h.server.shutdown();
}

// --- OPS-061 attribution ----------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tenant_metrics_report_usage_against_the_quota() {
    let h = start(TenantQuotas {
        max_reducer_calls_per_sec: Some(2.0),
        max_memory_bytes: Some(4_096),
        ..TenantQuotas::default()
    })
    .await;
    let mut acme = Client::connect(h.server.local_addr, "acme").await;
    acme.write_note("one").await.unwrap();
    acme.write_note("two").await.unwrap();
    let _ = acme.write_note("over").await; // trips the rate quota

    let resp = fluxum_server::admin::dispatch(&h.ctx, "GET", "/metrics", &[]).await;
    let text = match &resp.body {
        serde_json::Value::String(text) => text.clone(),
        other => panic!("expected metrics text, got {other:?}"),
    };
    assert!(
        text.contains("fluxum_tenant_memory_bytes{namespace=\"acme\"}"),
        "usage is exposed per tenant"
    );
    assert!(
        text.contains("fluxum_tenant_storage_bytes{namespace=\"acme\"}"),
        "durable footprint is exposed"
    );
    assert!(
        text.contains("fluxum_tenant_quota_bytes{namespace=\"acme\", quota=\"memory\"} 4096"),
        "the configured ceiling is exposed next to usage:\n{text}"
    );
    assert!(
        text.contains(
            "fluxum_tenant_quota_exceeded_total{namespace=\"acme\", quota=\"reducer_rate\"} 1"
        ),
        "the breach is counted:\n{text}"
    );
    assert!(
        text.contains(
            "fluxum_tenant_quota_exceeded_total{namespace=\"globex\", quota=\"reducer_rate\"} 0"
        ),
        "an untouched tenant reports zero, not a missing series"
    );
    h.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unquotaed_namespace_behaves_exactly_as_before() {
    let h = start(TenantQuotas::default()).await;
    let mut acme = Client::connect(h.server.local_addr, "acme").await;
    for i in 0..50 {
        assert_eq!(acme.write_note(&format!("n{i}")).await, Ok(()));
    }
    for i in 0..5 {
        assert_eq!(
            acme.subscribe(&format!("SELECT * FROM Note WHERE id = {i}"))
                .await,
            Ok(())
        );
    }
    h.server.shutdown();
}
