//! SPEC-026 SEC-058/059 — transport TLS termination and secret hygiene.
//! A directly-accepted FluxRPC/TCP connection completes a real TLS handshake
//! (self-signed fixture cert) and then speaks the identical binary protocol
//! over the encrypted stream; the plaintext-on-public-bind guard refuses an
//! unsafe config at load; and `Secret<T>` config fields never serialize their
//! bytes.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use fluxum_core::auth::{AuthClaims, AuthProvider, Authenticator, ServerPeerRegistry};
use fluxum_core::commitlog::{CommitLog, CommitLogOptions};
use fluxum_core::reducer::{LifecycleHooks, ReducerEngine, ReducerRegistry};
use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::store::MemStore;
use fluxum_core::subscription::{SubscriptionLimits, SubscriptionManager};
use fluxum_core::txn::{TxPipeline, TxPipelineOptions};
use fluxum_protocol::{Authenticate, ClientMessage, FrameCodec, ServerMessage};
use fluxum_server::ShardContext;
use fluxum_server::tcp::{self, TcpOptions};
use fluxum_server::tls;

const SHARD: u32 = 1;
const CERT_PEM: &str = include_str!("fixtures/tls/cert.pem");
const KEY_PEM: &str = include_str!("fixtures/tls/key.pem");

static CHAT_COLS: &[ColumnSchema] = &[ColumnSchema {
    name: "id",
    ty: FluxType::U64,
}];
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

#[derive(Debug)]
struct AnyProvider;
impl AuthProvider for AnyProvider {
    fn authenticate(&self, token: &[u8]) -> std::result::Result<AuthClaims, String> {
        Ok(AuthClaims {
            canonical_token: token.to_vec(),
            display_name: None,
            roles: Vec::new(),
            expires_at: None,
        })
    }
    fn refresh(&self, token: &[u8]) -> std::result::Result<Vec<u8>, String> {
        Ok(token.to_vec())
    }
}

fn make_ctx() -> Arc<ShardContext> {
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
        Arc::new(ReducerRegistry::from_defs(std::iter::empty()).unwrap()),
        LifecycleHooks::none(),
        SHARD,
        fluxum_core::auth::server_identity("tls-test"),
    );
    let subs = SubscriptionManager::new(Arc::new(schema), SubscriptionLimits::default());
    let auth = Authenticator::with_provider(Arc::new(AnyProvider), ServerPeerRegistry::empty());
    ShardContext::new(engine, subs, auth, SHARD, 256)
}

/// Write the fixture cert/key to disk and build a server TLS acceptor.
fn acceptor() -> (tokio_rustls::TlsAcceptor, Box<tempfile::TempDir>) {
    let dir = Box::new(tempfile::tempdir().unwrap());
    let cert = dir.path().join("cert.pem");
    let key = dir.path().join("key.pem");
    std::fs::write(&cert, CERT_PEM).unwrap();
    std::fs::write(&key, KEY_PEM).unwrap();
    (tls::load_acceptor(&cert, &key).unwrap(), dir)
}

/// A rustls client that trusts the fixture cert.
fn connector() -> TlsConnector {
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(CERT_PEM.as_bytes()) {
        roots.add(cert.unwrap()).unwrap();
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(config))
}

fn auth_bytes() -> Vec<u8> {
    let codec = FrameCodec::default();
    let body = ClientMessage::Authenticate(Authenticate {
        id: 1,
        token: b"alice".to_vec(),
        compression: None,
        tx_updates: None,
        namespace: None,
    })
    .encode()
    .unwrap();
    codec.encode(&body).unwrap()
}

/// Read one server message from a stream, or `None`.
async fn recv<S: tokio::io::AsyncRead + Unpin>(stream: &mut S) -> Option<ServerMessage> {
    let codec = FrameCodec::default();
    let mut buf = Vec::new();
    loop {
        if let Ok(Some((fluxum_protocol::Frame::Body(body), _))) = codec.decode(&buf) {
            return Some(ServerMessage::decode(body).unwrap());
        }
        let mut chunk = [0u8; 4096];
        match tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut chunk)).await
        {
            Ok(Ok(0)) | Err(_) => return None,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            Ok(Err(_)) => return None,
        }
    }
}

// --- SEC-059: FluxRPC/TCP authenticates over a real TLS handshake -----------------

#[tokio::test(flavor = "multi_thread")]
async fn tcp_authenticates_over_tls() {
    let ctx = make_ctx();
    let (acceptor, _dir) = acceptor();
    let server = tcp::serve_tls(
        Arc::clone(&ctx),
        "127.0.0.1:0",
        TcpOptions::default(),
        Some(acceptor),
    )
    .await
    .unwrap();
    let addr = server.local_addr;

    // A TLS client completes the handshake and speaks FluxRPC over it.
    let tcp = TcpStream::connect(addr).await.unwrap();
    let name = ServerName::try_from("localhost").unwrap();
    let mut tls = connector().connect(name, tcp).await.expect("TLS handshake");
    tls.write_all(&auth_bytes()).await.unwrap();
    assert!(
        matches!(recv(&mut tls).await, Some(ServerMessage::AuthResult(_))),
        "authenticated over the encrypted stream"
    );

    server.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_plaintext_client_cannot_talk_to_the_tls_listener() {
    let ctx = make_ctx();
    let (acceptor, _dir) = acceptor();
    let server = tcp::serve_tls(
        Arc::clone(&ctx),
        "127.0.0.1:0",
        TcpOptions::default(),
        Some(acceptor),
    )
    .await
    .unwrap();

    // A plaintext FluxRPC frame is not a TLS ClientHello: the server's TLS
    // handshake fails and the connection is dropped — no AuthResult.
    let mut plain = TcpStream::connect(server.local_addr).await.unwrap();
    plain.write_all(&auth_bytes()).await.unwrap();
    assert!(
        recv(&mut plain).await.is_none(),
        "a plaintext client gets nothing from the TLS listener"
    );

    server.shutdown();
}

// --- SEC-059: the plaintext-on-public-bind guard --------------------------------

#[test]
fn a_public_authenticating_bind_without_tls_is_refused_at_load() {
    use fluxum_core::config::{AuthProvider as Provider, Config};

    // token auth on 0.0.0.0 without TLS, no opt-out → refused.
    let mut cfg = Config::default();
    cfg.server.tcp_host = "0.0.0.0".into();
    cfg.auth.provider = Provider::Token;
    cfg.auth.secret = Some("s3cret".into());
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("cleartext"), "guard fires: {err}");

    // allow_plaintext opts out (trusted network / proxy-terminated TLS).
    let mut ok = cfg.clone();
    ok.server.allow_plaintext = true;
    ok.validate().unwrap();

    // Loopback is always fine.
    let mut lo = cfg.clone();
    lo.server.tcp_host = "127.0.0.1".into();
    lo.validate().unwrap();
}

// --- SEC-058: secret config fields never serialize their bytes ------------------

#[test]
fn secrets_are_redacted_when_the_config_is_serialized() {
    use fluxum_core::config::{AuthProvider as Provider, Config, ServerPeer};

    let mut cfg = Config::default();
    cfg.auth.provider = Provider::Token;
    cfg.auth.secret = Some("TOPSECRETVALUE".into());
    cfg.auth.server_peers.push(ServerPeer {
        name: "ops".into(),
        token: "PEERTOKENVALUE".into(),
    });

    let json = serde_json::to_string(&cfg).unwrap();
    assert!(
        !json.contains("TOPSECRETVALUE"),
        "auth.secret leaked: {json}"
    );
    assert!(
        !json.contains("PEERTOKENVALUE"),
        "peer token leaked: {json}"
    );
    assert!(json.contains("[redacted]"), "redaction marker present");

    // Debug redacts too.
    let dbg = format!("{:?}", cfg.auth);
    assert!(!dbg.contains("TOPSECRETVALUE") && !dbg.contains("PEERTOKENVALUE"));
}
