//! NFR-05 benchmark (T5.1 item 1.6, TST-062): FluxRPC round-trip over
//! loopback TCP — one `ReducerCall` → `ReducerResult` — must hold p99 <
//! 0.5 ms in a release build. Run with `cargo bench -p fluxum-server`.
//!
//! The integration suite (`tests/tcp_loopback.rs`) is the DAG exit test;
//! this bench is the standing latency guard.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Runtime;

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
use fluxum_protocol::{Authenticate, ClientMessage, Frame, FrameCodec, ReducerCall, ServerMessage};
use fluxum_server::ShardContext;
use fluxum_server::tcp::{self, TcpOptions};

static CHAT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "text",
        ty: FluxType::Str,
    },
];
static CHAT: TableSchema = TableSchema {
    name: "Chat",
    columns: CHAT_COLS,
    primary_key: &[0],
    auto_inc: Some(0),
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

#[derive(Debug, Clone, PartialEq)]
struct ChatRow {
    id: u64,
    text: String,
}
impl Table for ChatRow {
    type Pk = u64;
    const SCHEMA: &'static TableSchema = &CHAT;
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
            _ => Err(fluxum_core::FluxumError::Storage("bad row".into())),
        }
    }
    fn pk_values(pk: &u64) -> Vec<RowValue> {
        vec![RowValue::U64(*pk)]
    }
}

fn noop(ctx: &ReducerContext<'_, '_, '_>, _args: &[FluxValue]) -> Result<()> {
    ctx.tx.insert(ChatRow {
        id: 0,
        text: "x".into(),
    })?;
    Ok(())
}
fn check(_args: &[FluxValue]) -> Result<()> {
    Ok(())
}
static NOOP: ReducerDef = ReducerDef {
    name: "noop",
    handler: noop,
    check_args: check,
    client_callable: true,
    max_rate_per_sec: 0,
};

struct Bench {
    stream: TcpStream,
    codec: FrameCodec,
    buf: Vec<u8>,
}

impl Bench {
    async fn round_trip(&mut self, id: u32) {
        let call = ClientMessage::ReducerCall(ReducerCall {
            id,
            reducer: "noop".into(),
            version: None,
            args: vec![],
            idempotency_key: None,
        });
        let framed = self.codec.encode(&call.encode().unwrap()).unwrap();
        self.stream.write_all(&framed).await.unwrap();
        loop {
            if let Ok(Some((frame, consumed))) = self.codec.decode(&self.buf) {
                let done = matches!(frame, Frame::Body(_));
                self.buf.drain(..consumed);
                if done {
                    return;
                }
                continue;
            }
            let mut chunk = [0u8; 1024];
            let n = self.stream.read(&mut chunk).await.unwrap();
            self.buf.extend_from_slice(&chunk[..n]);
        }
    }
}

async fn setup() -> (Bench, std::net::SocketAddr) {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&CHAT]).unwrap();
    let store = Arc::new(MemStore::new(&schema).unwrap());
    let log = Arc::new(
        CommitLog::open(&dir.path().join("log"), 1, 1, CommitLogOptions::default()).unwrap(),
    );
    let (pipeline, worker) =
        TxPipeline::new(Arc::clone(&store), log, TxPipelineOptions::default()).unwrap();
    tokio::spawn(worker.run());
    let engine = ReducerEngine::new(
        pipeline,
        Arc::new(ReducerRegistry::from_defs([&NOOP]).unwrap()),
        LifecycleHooks::none(),
        1,
        fluxum_core::auth::server_identity("bench"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, 1, 256);
    let server = tcp::serve(ctx, "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    let mut client = Bench {
        stream: TcpStream::connect(server.local_addr).await.unwrap(),
        codec: FrameCodec::default(),
        buf: Vec::new(),
    };
    // Authenticate once.
    let auth = ClientMessage::Authenticate(Authenticate {
        id: 0,
        token: b"bench".to_vec(),
        compression: None,
        tx_updates: None,
    });
    let framed = client.codec.encode(&auth.encode().unwrap()).unwrap();
    client.stream.write_all(&framed).await.unwrap();
    // Drain the AuthResult.
    loop {
        if let Ok(Some((frame, consumed))) = client.codec.decode(&client.buf) {
            let done = matches!(frame, Frame::Body(_));
            client.buf.drain(..consumed);
            if done {
                break;
            }
            continue;
        }
        let mut chunk = [0u8; 1024];
        let n = client.stream.read(&mut chunk).await.unwrap();
        client.buf.extend_from_slice(&chunk[..n]);
    }
    let _ = ServerMessage::decode(&[]).is_err(); // keep the import used
    (client, server.local_addr)
}

fn loopback_rtt(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (mut client, _addr) = rt.block_on(setup());
    let mut id = 1u32;
    c.bench_function("fluxrpc_tcp_reducer_rtt", |b| {
        b.iter(|| {
            rt.block_on(client.round_trip(id));
            id = id.wrapping_add(1);
        });
    });
}

criterion_group!(benches, loopback_rtt);
criterion_main!(benches);
