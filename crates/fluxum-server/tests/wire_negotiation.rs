//! RPC-008 / RPC-035 wire-option negotiation, end to end against the real
//! TCP transport (SPEC-006 acceptance 16/17): the light/full split of one
//! delivery group, the AuthResult echo, the connection-lifetime rule, the
//! kill-switch, and the gzip stream with its context-carryover witness.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use fluxum_core::auth::{Authenticator, NoneProvider, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::{MemStore, RowValue, TxDiff};
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_protocol::{
    Authenticate, ClientMessage, FrameCodec, Resume, ServerMessage, StreamDecompressor,
    SubscribeSingle, TAG_GZIP_STREAM, TAG_UNCOMPRESSED, codes,
};
use fluxum_server::tcp::{self, TcpOptions};
use fluxum_server::{ShardContext, WirePolicy};

const SHARD: u32 = 1;

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
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

struct Harness {
    server: tcp::TcpServer,
    store: Arc<MemStore>,
    ctx: Arc<ShardContext>,
}

async fn start() -> Harness {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let schema = Schema::from_tables([&CHAT]).unwrap();
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
        fluxum_core::auth::server_identity("wire-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(NoneProvider), ServerPeerRegistry::empty());
    let ctx = ShardContext::new(engine, subs, auth, SHARD, 64);
    let server = tcp::serve(Arc::clone(&ctx), "127.0.0.1:0", TcpOptions::default())
        .await
        .unwrap();
    Harness { server, store, ctx }
}

fn commit_row(store: &MemStore, id: u64, text: &str) -> TxDiff {
    let tid = store.table_id("Chat").unwrap();
    let mut tx = store.begin();
    tx.insert(tid, vec![RowValue::U64(id), RowValue::Str(text.into())])
        .unwrap();
    tx.commit().unwrap()
}

/// One received frame, as it crossed the wire.
struct WireFrame {
    /// The RPC-008 tag when the stream is armed; `None` before/without.
    tag: Option<u8>,
    /// Bytes of the frame body as framed (tag + payload when armed).
    wire_len: usize,
    message: ServerMessage,
}

/// A raw framed client that understands the RPC-008 tagged stream.
struct WireClient {
    stream: TcpStream,
    codec: FrameCodec,
    buf: Vec<u8>,
    /// Armed after the AuthResult that echoed gzip.
    inflate: Option<StreamDecompressor>,
    armed: bool,
}

impl WireClient {
    async fn connect(addr: std::net::SocketAddr) -> Self {
        Self {
            stream: TcpStream::connect(addr).await.unwrap(),
            codec: FrameCodec::default(),
            buf: Vec::new(),
            inflate: None,
            armed: false,
        }
    }

    async fn send(&mut self, message: ClientMessage) {
        let body = message.encode().unwrap();
        let framed = self.codec.encode(&body).unwrap();
        self.stream.write_all(&framed).await.unwrap();
    }

    async fn authenticate(
        &mut self,
        compression: Option<&str>,
        tx_updates: Option<&str>,
    ) -> ServerMessage {
        self.send(ClientMessage::Authenticate(Authenticate {
            id: 1,
            token: b"wire".to_vec(),
            compression: compression.map(str::to_string),
            tx_updates: tx_updates.map(str::to_string),
            namespace: None,
        }))
        .await;
        let reply = self.recv().await.unwrap();
        // RPC-008: arm the decompressor off the echo alone.
        if let ServerMessage::AuthResult(auth) = &reply.message
            && auth.compression.as_deref() == Some("gzip")
        {
            self.armed = true;
            self.inflate = Some(StreamDecompressor::new());
        }
        reply.message
    }

    async fn subscribe(&mut self, query: &str) -> ServerMessage {
        self.send(ClientMessage::SubscribeSingle(SubscribeSingle {
            id: 2,
            query: query.into(),
        }))
        .await;
        self.recv().await.unwrap().message
    }

    async fn recv(&mut self) -> Option<WireFrame> {
        loop {
            if let Ok(Some((frame, consumed))) = self.codec.decode(&self.buf) {
                let out = match frame {
                    fluxum_protocol::Frame::Body(body) => {
                        let wire_len = body.len();
                        if self.armed {
                            let (tag, payload) = (body[0], &body[1..]);
                            let decoded = match tag {
                                TAG_GZIP_STREAM => {
                                    let inflated = self
                                        .inflate
                                        .as_mut()
                                        .unwrap()
                                        .inflate_chunk(payload, 16 << 20)
                                        .expect("valid stream chunk");
                                    ServerMessage::decode(&inflated).unwrap()
                                }
                                TAG_UNCOMPRESSED => ServerMessage::decode(payload).unwrap(),
                                other => panic!("unexpected compression tag {other:#x}"),
                            };
                            Some(WireFrame {
                                tag: Some(tag),
                                wire_len,
                                message: decoded,
                            })
                        } else {
                            Some(WireFrame {
                                tag: None,
                                wire_len,
                                message: ServerMessage::decode(body).unwrap(),
                            })
                        }
                    }
                    fluxum_protocol::Frame::KeepAlive => None,
                };
                self.buf.drain(..consumed);
                if let Some(out) = out {
                    return Some(out);
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

    async fn recv_within(&mut self, timeout: Duration) -> Option<WireFrame> {
        tokio::time::timeout(timeout, self.recv()).await.ok()?
    }
}

// --- SPEC-006 acceptance 17: the light/full split -----------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn light_and_full_sessions_split_one_delivery_group() {
    let h = start().await;

    let mut full = WireClient::connect(h.server.local_addr).await;
    let reply = full.authenticate(None, Some("full")).await;
    let ServerMessage::AuthResult(auth) = reply else {
        panic!("expected AuthResult, got {reply:?}");
    };
    assert_eq!(auth.tx_updates.as_deref(), Some("full"));
    assert_eq!(auth.compression.as_deref(), Some("none"));
    full.subscribe("SELECT * FROM Chat").await;

    let mut light = WireClient::connect(h.server.local_addr).await;
    let ServerMessage::AuthResult(auth) = light.authenticate(None, Some("light")).await else {
        panic!("expected AuthResult");
    };
    assert_eq!(auth.tx_updates.as_deref(), Some("light"));
    light.subscribe("SELECT * FROM Chat").await;

    let diff = commit_row(&h.store, 1, "split");
    h.ctx.publish_commit(diff);

    let full_msg = full.recv_within(Duration::from_secs(3)).await.unwrap();
    let ServerMessage::TxUpdate(update) = full_msg.message else {
        panic!("full session must receive the enriched TxUpdate");
    };
    let light_msg = light.recv_within(Duration::from_secs(3)).await.unwrap();
    let ServerMessage::TxUpdateLight(light_update) = light_msg.message else {
        panic!("light session must receive TxUpdateLight");
    };

    // Same commit, same rows, same cursor — only provenance is stripped.
    assert_eq!(light_update.tx_id, update.tx_id);
    assert_eq!(light_update.tx_offset, update.tx_offset);
    assert_eq!(light_update.shard_id, update.shard_id);
    assert_eq!(light_update.tables.len(), update.tables.len());
    assert_eq!(
        light_update.tables[0].inserts.rows_data,
        update.tables[0].inserts.rows_data
    );
    // And the light frame is materially smaller on the wire.
    assert!(
        light_msg.wire_len < full_msg.wire_len,
        "light {} vs full {}",
        light_msg.wire_len,
        full_msg.wire_len
    );
    h.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_light_session_resumes_with_light_replay() {
    let h = start().await;
    let mut light = WireClient::connect(h.server.local_addr).await;
    light.authenticate(None, Some("light")).await;
    let ServerMessage::InitialData(initial) = light.subscribe("SELECT * FROM Chat").await else {
        panic!("expected InitialData");
    };
    let query_id = initial.tables.first().map_or(1, |t| t.query_id);

    // Two commits, drain both live deltas.
    h.ctx.publish_commit(commit_row(&h.store, 10, "first"));
    h.ctx.publish_commit(commit_row(&h.store, 11, "second"));
    let first = light.recv_within(Duration::from_secs(3)).await.unwrap();
    let ServerMessage::TxUpdateLight(first) = first.message else {
        panic!("expected TxUpdateLight");
    };
    let second = light.recv_within(Duration::from_secs(3)).await.unwrap();
    assert!(matches!(second.message, ServerMessage::TxUpdateLight(_)));

    // Resume from before the second commit: the replay must be light too.
    light
        .send(ClientMessage::Resume(Resume {
            id: 9,
            query_id,
            from_offset: first.tx_offset,
        }))
        .await;
    let replayed = light.recv_within(Duration::from_secs(3)).await.unwrap();
    let ServerMessage::TxUpdateLight(replayed) = replayed.message else {
        panic!("a light session's resume replay must be TxUpdateLight");
    };
    assert!(replayed.tx_offset > first.tx_offset);
    h.server.shutdown();
}

// --- RPC-020: rejection matrix -------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn unknown_and_unsupported_tokens_are_rejected_with_400() {
    let h = start().await;
    for (compression, tx_updates) in [
        (Some("zstd"), None),
        (Some("brotli"), None), // reserved, not in this build (RPC-008)
        (None, Some("lite")),
        (None, Some("delta")), // specified (RPC-036), not implemented
    ] {
        let mut client = WireClient::connect(h.server.local_addr).await;
        let reply = client.authenticate(compression, tx_updates).await;
        let ServerMessage::Error(err) = reply else {
            panic!("{compression:?}/{tx_updates:?} must be refused, got {reply:?}");
        };
        assert_eq!(err.code, codes::PROTO_MALFORMED);
        // The connection survives: a corrected retry authenticates.
        let ServerMessage::AuthResult(_) = client.authenticate(None, None).await else {
            panic!("the connection must stay open after a 400");
        };
    }
    h.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reauthenticate_cannot_change_pinned_options() {
    let h = start().await;
    let mut client = WireClient::connect(h.server.local_addr).await;
    client.authenticate(None, Some("light")).await;

    // Naming a different value is refused …
    let reply = client.authenticate(None, Some("full")).await;
    let ServerMessage::Error(err) = reply else {
        panic!("changing tx_updates mid-connection must be refused");
    };
    assert_eq!(err.code, codes::PROTO_MALFORMED);

    // … while a re-auth that names nothing keeps the pin.
    let ServerMessage::AuthResult(auth) = client.authenticate(None, None).await else {
        panic!("silent re-auth must succeed");
    };
    assert_eq!(auth.tx_updates.as_deref(), Some("light"));
    h.server.shutdown();
}

// --- SPEC-006 acceptance 16: the gzip stream ----------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn a_gzip_session_gets_a_tagged_stream_with_carryover() {
    let h = start().await;
    let mut client = WireClient::connect(h.server.local_addr).await;
    let ServerMessage::AuthResult(auth) = client.authenticate(Some("gzip"), None).await else {
        panic!("expected AuthResult");
    };
    assert_eq!(auth.compression.as_deref(), Some("gzip"));

    // Every frame after the (untagged) AuthResult carries a tag.
    let sub = {
        client
            .send(ClientMessage::SubscribeSingle(SubscribeSingle {
                id: 2,
                query: "SELECT * FROM Chat".into(),
            }))
            .await;
        client.recv_within(Duration::from_secs(3)).await.unwrap()
    };
    assert!(matches!(sub.message, ServerMessage::InitialData(_)));
    assert!(sub.tag.is_some(), "post-negotiation frames must be tagged");

    // Identical row bodies over one stream: the shared window turns later
    // frames into back-references — the carryover witness. The text is
    // incompressible on its own (random-ish hex), so a small later frame
    // can only come from the window.
    let text = "9f8a7b6c5d4e3f2a1b0c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1d0e9f8a";
    let mut sizes = Vec::new();
    for i in 0..6u64 {
        h.ctx.publish_commit(commit_row(&h.store, 100 + i, text));
        let frame = client.recv_within(Duration::from_secs(3)).await.unwrap();
        assert!(matches!(frame.message, ServerMessage::TxUpdate(_)));
        assert_eq!(frame.tag, Some(TAG_GZIP_STREAM), "body is over threshold");
        sizes.push(frame.wire_len);
    }
    let first = sizes[0];
    let last = *sizes.last().unwrap();
    assert!(
        last * 2 < first,
        "context carryover missing: first frame {first} bytes, last {last} bytes"
    );
    h.server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_kill_switch_degrades_to_none_with_an_honest_echo() {
    let h = start().await;
    h.ctx.set_wire_policy(WirePolicy {
        compression_enabled: false,
        compression_threshold_bytes: 64,
    });
    let mut client = WireClient::connect(h.server.local_addr).await;
    let ServerMessage::AuthResult(auth) = client.authenticate(Some("gzip"), None).await else {
        panic!("expected AuthResult");
    };
    // The echo says none; the client never arms; frames stay untagged.
    assert_eq!(auth.compression.as_deref(), Some("none"));
    let msg = client.subscribe("SELECT * FROM Chat").await;
    assert!(matches!(msg, ServerMessage::InitialData(_)));
    h.server.shutdown();
}
