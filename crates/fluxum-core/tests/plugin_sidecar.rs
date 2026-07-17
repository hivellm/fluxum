//! SPEC-020 §4.2 (PLG-031) — the out-of-process sidecar host: the proxy
//! speaks Plugin RPC to a real socket, every failure mode degrades to the
//! base BM25 result instead of erroring the client, the circuit breaker
//! opens after repeated failures and stops paying the timeout, and the
//! errors are metered by reason.
//!
//! The fake sidecar below is a real TCP server on a real ephemeral port,
//! not a mock: the whole point of PLG-031 is process isolation over a wire,
//! and a proxy tested against an in-memory double would prove nothing about
//! timeouts, half-open connections, or reconnects.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use fluxum_core::config::{Config, PluginDecl, PluginHost, PluginScope};
use fluxum_core::plugin::{
    BreakerState, Capability, FtQuery, PluginCtx, PluginRegistry, ScoreReranker, Scored,
    SidecarConfig, SidecarErrorReason, SidecarProxy,
};
use fluxum_core::schema::{
    ColumnSchema, FluxType, FullTextLanguage, IndexSchema, Schema, TableAccess, TableSchema,
    VisibilityRule,
};
use fluxum_core::store::{MemStore, PkBytes, RowValue};
use fluxum_core::subscription::{Subscriber, SubscriptionLimits, SubscriptionManager};
use fluxum_core::types::Identity;
use fluxum_protocol::frame::{Frame, FrameCodec};
use fluxum_protocol::plugin_rpc::{
    Candidate, Candidates, PLUGIN_RPC_VERSION, PluginRequest, PluginResponse, PluginRpcError, Ready,
};

// --- The fake sidecar -------------------------------------------------------------

/// How the fake sidecar misbehaves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Behavior {
    /// Handshake, then reverse the candidate order — a "model" whose output
    /// is trivially distinguishable from BM25's.
    Reverse,
    /// Handshake, then never answer a call (a wedged model runtime).
    Hang,
    /// Handshake, then answer every call with `Error` (up, healthy, says no).
    Refuse,
    /// Answer the handshake with a version this build does not speak.
    WrongVersion,
    /// Refuse every other call — an intermittently flaky sidecar, which the
    /// breaker must tolerate rather than trip on.
    Flaky,
    /// Accept only the configured token.
    RequireToken(&'static str),
    /// Accept the connection, then close it without a word.
    Hangup,
    /// Answer a call, but with the wrong `call_id` — a desynchronized
    /// sidecar the proxy must reject rather than trust.
    WrongCallId,
    /// Answer a call with a mid-stream `Ready` (a protocol violation).
    ReadyMidStream,
    /// Answer the handshake with `Candidates` instead of `Ready`.
    CandidatesAtHandshake,
    /// Send a keep-alive frame before the real response (RPC-001) — the
    /// proxy must skip it and keep waiting within the same budget.
    KeepAlivePrefix,
}

struct FakeSidecar {
    addr: SocketAddr,
    calls: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
}

impl FakeSidecar {
    fn start(behavior: Behavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let (thread_calls, thread_stop) = (Arc::clone(&calls), Arc::clone(&stop));
        thread::spawn(move || {
            for stream in listener.incoming() {
                if thread_stop.load(Ordering::Relaxed) {
                    return;
                }
                let Ok(stream) = stream else { return };
                let (calls, stop) = (Arc::clone(&thread_calls), Arc::clone(&thread_stop));
                thread::spawn(move || serve(stream, behavior, &calls, &stop));
            }
        });
        Self { addr, calls, stop }
    }

    fn endpoint(&self) -> String {
        self.addr.to_string()
    }

    /// Calls the sidecar actually received — the difference between "the
    /// host degraded" and "the host degraded *without dialing*", which is
    /// the whole claim of the breaker.
    fn calls(&self) -> u64 {
        self.calls.load(Ordering::Relaxed)
    }
}

impl Drop for FakeSidecar {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// An address nothing listens on: bind, record, drop. A "stopped sidecar"
/// must be a port that actively refuses, not a black hole, so the test does
/// not depend on connect-timeout behavior it did not intend to exercise.
fn dead_endpoint() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr.to_string()
}

fn serve(mut stream: TcpStream, behavior: Behavior, calls: &AtomicU64, stop: &AtomicBool) {
    if behavior == Behavior::Hangup {
        return;
    }
    let codec = FrameCodec::default();
    let mut buf = Vec::new();
    loop {
        let request: PluginRequest = match read_frame(&mut stream, &mut buf, &codec) {
            Some(bytes) => match rmp_serde::from_slice(&bytes) {
                Ok(request) => request,
                Err(_) => return,
            },
            None => return,
        };
        // A call carries a `call_id` and a candidate list, whichever of the
        // three ReadPath calls it is. `Fuse` has no single list, so it
        // reverses the lexical one — enough to be observably the sidecar's.
        let call = match &request {
            PluginRequest::Hello(hello) => {
                let response = match behavior {
                    Behavior::WrongVersion => PluginResponse::Ready(Ready {
                        version: PLUGIN_RPC_VERSION + 1,
                        name: "fake".into(),
                    }),
                    Behavior::CandidatesAtHandshake => PluginResponse::Candidates(Candidates {
                        call_id: 0,
                        candidates: vec![],
                    }),
                    Behavior::RequireToken(expected)
                        if hello.token.as_deref() != Some(expected) =>
                    {
                        PluginResponse::Error(PluginRpcError {
                            call_id: 0,
                            message: "bad token".into(),
                        })
                    }
                    _ => PluginResponse::Ready(Ready {
                        version: PLUGIN_RPC_VERSION,
                        name: "fake".into(),
                    }),
                };
                write_frame(&mut stream, &codec, &response);
                continue;
            }
            PluginRequest::Rerank(r) => (r.call_id, r.candidates.clone()),
            PluginRequest::Retrieve(r) => (
                r.call_id,
                (1..=r.k).map(|i| candidate(i as u8, i as f64)).collect(),
            ),
            PluginRequest::Fuse(r) => {
                let mut merged = r.lexical.clone();
                merged.extend(r.dense.clone());
                (r.call_id, merged)
            }
        };
        let (call_id, mut candidates) = call;
        let nth = calls.fetch_add(1, Ordering::Relaxed);
        let behavior = match behavior {
            Behavior::Flaky if nth.is_multiple_of(2) => Behavior::Refuse,
            Behavior::Flaky => Behavior::Reverse,
            other => other,
        };
        let response = match behavior {
            Behavior::Hang => {
                // Outlive the call's budget without answering. Poll `stop`
                // so the thread does not outlive the test.
                for _ in 0..200 {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                return;
            }
            Behavior::Refuse => PluginResponse::Error(PluginRpcError {
                call_id,
                message: "model not loaded".into(),
            }),
            Behavior::WrongCallId => PluginResponse::Candidates(Candidates {
                call_id: call_id.wrapping_add(999),
                candidates,
            }),
            Behavior::ReadyMidStream => PluginResponse::Ready(Ready {
                version: PLUGIN_RPC_VERSION,
                name: "fake".into(),
            }),
            _ => {
                candidates.reverse();
                PluginResponse::Candidates(Candidates {
                    call_id,
                    candidates,
                })
            }
        };
        if behavior == Behavior::KeepAlivePrefix
            && stream.write_all(&FrameCodec::keepalive()).is_err()
        {
            return;
        }
        write_frame(&mut stream, &codec, &response);
    }
}

fn candidate(pk: u8, score: f64) -> Candidate {
    Candidate {
        pk: vec![pk],
        score,
    }
}

fn write_frame(stream: &mut TcpStream, codec: &FrameCodec, response: &PluginResponse) {
    let body = rmp_serde::to_vec(response).unwrap();
    let _ = stream.write_all(&codec.encode(&body).unwrap());
}

fn read_frame(stream: &mut TcpStream, buf: &mut Vec<u8>, codec: &FrameCodec) -> Option<Vec<u8>> {
    loop {
        if let Ok(Some((Frame::Body(body), consumed))) = codec.decode(buf) {
            let out = body.to_vec();
            buf.drain(..consumed);
            return Some(out);
        }
        let mut chunk = [0u8; 1024];
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    }
}

// --- Proxy harness ----------------------------------------------------------------

const TIMEOUT: Duration = Duration::from_millis(150);
/// Short enough that the half-open recovery tests need not sleep the
/// production five seconds out.
const COOLDOWN: Duration = Duration::from_millis(200);

fn proxy(endpoint: String, token: Option<&str>) -> Arc<SidecarProxy> {
    proxy_with(Capability::ScoreReranker, endpoint, token)
}

fn proxy_with(
    capability: Capability,
    endpoint: String,
    token: Option<&str>,
) -> Arc<SidecarProxy> {
    Arc::new(SidecarProxy::new(SidecarConfig {
        name: "reranker".into(),
        capability,
        endpoint,
        timeout: TIMEOUT,
        token: token.map(ToOwned::to_owned),
        breaker_cooldown: COOLDOWN,
    }))
}

fn ctx() -> PluginCtx {
    PluginCtx {
        identity: Identity::from_bytes([1; 32]),
        is_server_peer: false,
        shard_id: 0,
    }
}

fn query() -> FtQuery {
    FtQuery {
        table: "Item".into(),
        column: "description".into(),
        query: "sword".into(),
        limit: 3,
    }
}

fn scored(pks: &[u8]) -> Vec<Scored> {
    pks.iter()
        .enumerate()
        .map(|(i, pk)| Scored {
            pk: PkBytes::from_bytes(vec![*pk]),
            score: (pks.len() - i) as f64,
        })
        .collect()
}

fn pks(scored: &[Scored]) -> Vec<u8> {
    scored.iter().map(|s| s.pk.as_bytes()[0]).collect()
}

// --- 1.2: the proxy implements the trait by calling the sidecar --------------------

#[test]
fn a_healthy_sidecar_serves_the_capability_over_the_wire() {
    let sidecar = FakeSidecar::start(Behavior::Reverse);
    let proxy = proxy(sidecar.endpoint(), None);

    let out = proxy.rerank(&query(), scored(&[1, 2, 3]), &ctx()).unwrap();
    assert_eq!(pks(&out), vec![3, 2, 1], "the sidecar's order came back");
    assert_eq!(sidecar.calls(), 1);
    assert_eq!(proxy.stats().breaker_state(), BreakerState::Closed);

    // The connection is reused: a second call must not re-handshake, which
    // the fake would answer with `Ready` mid-stream and desynchronize.
    let out = proxy.rerank(&query(), scored(&[4, 5]), &ctx()).unwrap();
    assert_eq!(pks(&out), vec![5, 4]);
    assert_eq!(sidecar.calls(), 2);
    assert_eq!(proxy.stats().errors(SidecarErrorReason::Timeout), 0);
}

// --- 1.3: every failure mode degrades, none of them errors the client -------------

#[test]
fn a_stopped_sidecar_fails_the_call_rather_than_hanging() {
    let proxy = proxy(dead_endpoint(), None);
    let err = proxy
        .rerank(&query(), scored(&[1, 2]), &ctx())
        .expect_err("a call to a dead endpoint fails");
    assert!(err.to_string().contains("connect"), "{err}");
    assert_eq!(proxy.stats().errors(SidecarErrorReason::Connect), 1);
}

#[test]
fn a_hanging_sidecar_is_cut_off_at_the_timeout_not_waited_out() {
    let sidecar = FakeSidecar::start(Behavior::Hang);
    let proxy = proxy(sidecar.endpoint(), None);

    let started = Instant::now();
    let err = proxy
        .rerank(&query(), scored(&[1, 2]), &ctx())
        .expect_err("a hung sidecar must not answer");
    let elapsed = started.elapsed();

    assert_eq!(proxy.stats().errors(SidecarErrorReason::Timeout), 1);
    assert!(err.to_string().contains("timeout"), "{err}");
    // The budget is the query's, so the bound is what matters, not the
    // wire. Generous upper slack: a loaded CI box schedules threads late,
    // and a flaky timing test is worse than a loose one.
    assert!(
        elapsed >= TIMEOUT,
        "must actually wait for the budget, not fail early: {elapsed:?}"
    );
    assert!(
        elapsed < TIMEOUT * 5,
        "must not wait past the budget: {elapsed:?}"
    );
}

#[test]
fn a_refusing_sidecar_is_metered_apart_from_a_broken_one() {
    let sidecar = FakeSidecar::start(Behavior::Refuse);
    let proxy = proxy(sidecar.endpoint(), None);
    let err = proxy.rerank(&query(), scored(&[1]), &ctx()).unwrap_err();

    assert!(err.to_string().contains("model not loaded"), "{err}");
    assert_eq!(proxy.stats().errors(SidecarErrorReason::Refused), 1);
    assert_eq!(
        proxy.stats().errors(SidecarErrorReason::Transport),
        0,
        "a healthy sidecar that declines is not a transport failure — an \
         operator restarts the process for one and not the other"
    );
}

#[test]
fn a_version_mismatch_fails_loudly_instead_of_negotiating_down() {
    let sidecar = FakeSidecar::start(Behavior::WrongVersion);
    let proxy = proxy(sidecar.endpoint(), None);
    let err = proxy.rerank(&query(), scored(&[1]), &ctx()).unwrap_err();

    assert!(err.to_string().contains("Plugin RPC v"), "{err}");
    assert_eq!(proxy.stats().errors(SidecarErrorReason::Protocol), 1);
}

#[test]
fn a_sidecar_restart_costs_one_call_not_the_binding() {
    let dead = FakeSidecar::start(Behavior::Hangup);
    let proxy_dead = proxy(dead.endpoint(), None);
    // The sidecar accepts and closes: the handshake dies on EOF.
    let err = proxy_dead.rerank(&query(), scored(&[1]), &ctx()).unwrap_err();
    assert!(err.to_string().contains("transport"), "{err}");
    drop(dead);

    // A healthy sidecar comes back up. The proxy must redial rather than
    // hold the dead connection forever.
    let up = FakeSidecar::start(Behavior::Reverse);
    let proxy_up = proxy(up.endpoint(), None);
    let out = proxy_up.rerank(&query(), scored(&[1, 2]), &ctx()).unwrap();
    assert_eq!(pks(&out), vec![2, 1], "reconnected after the failure");
}

#[test]
fn the_retriever_and_fusion_capabilities_call_the_sidecar_too() {
    use fluxum_core::plugin::{Fusion, Retriever};

    // Retriever: the sidecar synthesizes k candidates; the proxy carries
    // them back as `Scored` with the opaque pks intact.
    let sidecar = FakeSidecar::start(Behavior::Reverse);
    let retr = proxy_with(Capability::Retriever, sidecar.endpoint(), None);
    let dense = retr.retrieve(&query(), 3, &ctx()).unwrap();
    assert_eq!(pks(&dense), vec![3, 2, 1], "the sidecar's dense list");

    // Fusion: the sidecar returns a merged list; the proxy returns it.
    let sidecar = FakeSidecar::start(Behavior::Reverse);
    let fuse = proxy_with(Capability::Fusion, sidecar.endpoint(), None);
    let fused = fuse.fuse(&scored(&[1, 2]), &scored(&[7, 8]), &ctx());
    assert_eq!(pks(&fused), vec![8, 7, 2, 1], "lexical+dense, reversed");
}

#[test]
fn a_failing_fusion_sidecar_degrades_to_default_rrf_in_place() {
    use fluxum_core::plugin::{Fusion, ReciprocalRankFusion};

    // `Fusion::fuse` is infallible by signature, so a dead sidecar cannot
    // surface an error to the caller — it must fall back to the built-in RRF
    // right here (PLG-041).
    let fuse = proxy_with(Capability::Fusion, dead_endpoint(), None);
    let lexical = scored(&[1, 2, 3]);
    let dense = scored(&[3, 4]);
    let out = fuse.fuse(&lexical, &dense, &ctx());
    let reference = ReciprocalRankFusion::default().fuse(&lexical, &dense, &ctx());
    assert_eq!(
        pks(&out),
        pks(&reference),
        "a broken fusion sidecar yields the exact default-RRF order"
    );
}

#[test]
fn a_desynchronized_sidecar_response_is_rejected() {
    // A `call_id` that does not match the in-flight call means the stream is
    // desynchronized; trusting it would attribute one query's scores to
    // another. It is a protocol failure, and the connection is dropped.
    let sidecar = FakeSidecar::start(Behavior::WrongCallId);
    let proxy = proxy(sidecar.endpoint(), None);
    let err = proxy.rerank(&query(), scored(&[1]), &ctx()).unwrap_err();
    assert!(err.to_string().contains("in flight"), "{err}");
    assert_eq!(proxy.stats().errors(SidecarErrorReason::Protocol), 1);
}

#[test]
fn a_mid_stream_ready_is_a_protocol_error() {
    let sidecar = FakeSidecar::start(Behavior::ReadyMidStream);
    let proxy = proxy(sidecar.endpoint(), None);
    let err = proxy.rerank(&query(), scored(&[1]), &ctx()).unwrap_err();
    assert!(err.to_string().contains("Ready"), "{err}");
    assert_eq!(proxy.stats().errors(SidecarErrorReason::Protocol), 1);
}

#[test]
fn a_handshake_answered_with_candidates_is_a_protocol_error() {
    let sidecar = FakeSidecar::start(Behavior::CandidatesAtHandshake);
    let proxy = proxy(sidecar.endpoint(), None);
    let err = proxy.rerank(&query(), scored(&[1]), &ctx()).unwrap_err();
    assert!(err.to_string().contains("candidates"), "{err}");
    assert_eq!(proxy.stats().errors(SidecarErrorReason::Protocol), 1);
}

#[test]
fn a_keep_alive_before_the_response_is_skipped() {
    // RPC-001: a keep-alive frame is not a response. The proxy must drain it
    // and keep waiting for the real one within the same budget.
    let sidecar = FakeSidecar::start(Behavior::KeepAlivePrefix);
    let proxy = proxy(sidecar.endpoint(), None);
    let out = proxy.rerank(&query(), scored(&[1, 2]), &ctx()).unwrap();
    assert_eq!(pks(&out), vec![2, 1], "the response after the keep-alive");
}

// --- 1.4: the breaker recovers ----------------------------------------------------

#[test]
fn the_breaker_recovers_when_the_sidecar_comes_back() {
    // A sidecar that hangs the first FAILURE_THRESHOLD calls, then serves —
    // exercising the full open → half-open → closed recovery on one proxy
    // and one endpoint, which is the real operational story.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let served = Arc::new(AtomicU64::new(0));
    let (thread_stop, thread_served) = (Arc::clone(&stop), Arc::clone(&served));
    thread::spawn(move || {
        let codec = FrameCodec::default();
        for stream in listener.incoming() {
            if thread_stop.load(Ordering::Relaxed) {
                return;
            }
            let Ok(mut stream) = stream else { return };
            let mut buf = Vec::new();
            // Handshake.
            let Some(_hello) = read_frame(&mut stream, &mut buf, &codec) else {
                continue;
            };
            write_frame(
                &mut stream,
                &codec,
                &PluginResponse::Ready(Ready {
                    version: PLUGIN_RPC_VERSION,
                    name: "recovering".into(),
                }),
            );
            let Some(bytes) = read_frame(&mut stream, &mut buf, &codec) else {
                continue;
            };
            let PluginRequest::Rerank(rerank) =
                rmp_serde::from_slice::<PluginRequest>(&bytes).unwrap()
            else {
                continue;
            };
            // The first few connections hang (breaker opens); later ones
            // serve (breaker closes).
            if thread_served.fetch_add(1, Ordering::Relaxed) < 5 {
                thread::sleep(Duration::from_millis(400));
                continue;
            }
            let mut candidates = rerank.candidates;
            candidates.reverse();
            write_frame(
                &mut stream,
                &codec,
                &PluginResponse::Candidates(Candidates {
                    call_id: rerank.call_id,
                    candidates,
                }),
            );
        }
    });

    let proxy = proxy(addr.to_string(), None);
    for _ in 0..5 {
        let _ = proxy.rerank(&query(), scored(&[1]), &ctx());
    }
    assert_eq!(proxy.stats().breaker_state(), BreakerState::Open);

    // After the cooldown, the trial call reaches a now-healthy sidecar and
    // closes the breaker.
    let mut closed = false;
    for _ in 0..20 {
        thread::sleep(COOLDOWN + Duration::from_millis(20));
        if proxy.rerank(&query(), scored(&[1, 2]), &ctx()).is_ok() {
            closed = true;
            break;
        }
    }
    assert!(closed, "the breaker must let the recovered sidecar back in");
    assert_eq!(proxy.stats().breaker_state(), BreakerState::Closed);
    stop.store(true, Ordering::Relaxed);
    let _ = TcpStream::connect(addr); // unblock accept()
}

#[test]
fn a_proxy_builds_the_instance_its_capability_can_be_called_through() {
    use fluxum_core::plugin::PluginInstance;
    // A ReadPath capability yields a live instance of that variant; a
    // capability with no Plugin RPC wire yet yields none (the binding is
    // still legal and introspectable).
    for (cap, is_some) in [
        (Capability::ScoreReranker, true),
        (Capability::Retriever, true),
        (Capability::Fusion, true),
        (Capability::StreamSink, false),
        (Capability::KeyProvider, false),
    ] {
        let proxy = proxy_with(cap, "127.0.0.1:1".into(), None);
        let instance = proxy.instance();
        assert_eq!(instance.is_some(), is_some, "{cap:?}");
        if let Some(instance) = instance {
            assert_eq!(instance.capability(), cap);
            assert!(matches!(
                (cap, &instance),
                (Capability::ScoreReranker, PluginInstance::ScoreReranker(_))
                    | (Capability::Retriever, PluginInstance::Retriever(_))
                    | (Capability::Fusion, PluginInstance::Fusion(_))
            ));
        }
    }
}

// --- 1.5: authentication ----------------------------------------------------------

#[test]
fn the_sidecar_authenticates_the_host_by_shared_token() {
    let sidecar = FakeSidecar::start(Behavior::RequireToken("s3cret"));

    let wrong = proxy(sidecar.endpoint(), Some("guess"));
    let err = wrong.rerank(&query(), scored(&[1]), &ctx()).unwrap_err();
    assert!(err.to_string().contains("handshake rejected"), "{err}");

    let missing = proxy(sidecar.endpoint(), None);
    assert!(missing.rerank(&query(), scored(&[1]), &ctx()).is_err());

    let right = proxy(sidecar.endpoint(), Some("s3cret"));
    let out = right.rerank(&query(), scored(&[1, 2]), &ctx()).unwrap();
    assert_eq!(pks(&out), vec![2, 1], "the right token is served");
}

#[test]
fn the_token_never_reaches_the_plugins_report() {
    let schema = Arc::new(Schema::from_tables([&ITEM]).unwrap());
    let registry = PluginRegistry::build(
        &schema,
        &config_with_sidecar("127.0.0.1:15899", Some("s3cret-do-not-leak")),
    )
    .unwrap();
    let report = serde_json::to_string(&registry.report()).unwrap();
    assert!(
        !report.contains("s3cret-do-not-leak"),
        "PLG-060: `GET /plugins` reports state, never secrets: {report}"
    );
}

// --- 1.4: the circuit breaker -----------------------------------------------------

#[test]
fn the_breaker_opens_after_repeated_failures_and_stops_paying_the_timeout() {
    let sidecar = FakeSidecar::start(Behavior::Hang);
    let proxy = proxy(sidecar.endpoint(), None);
    let stats = proxy.stats();

    // Five consecutive timeouts (FAILURE_THRESHOLD) to open it.
    for _ in 0..5 {
        assert!(proxy.rerank(&query(), scored(&[1]), &ctx()).is_err());
    }
    assert_eq!(stats.breaker_state(), BreakerState::Open);
    assert_eq!(stats.breaker_opened_total(), 1);
    assert_eq!(stats.errors(SidecarErrorReason::Timeout), 5);
    let dialed = sidecar.calls();

    // The next call must fail *fast* — that is the breaker's entire job: a
    // dead sidecar costs one timeout per cooldown, not one per query.
    let started = Instant::now();
    let err = proxy.rerank(&query(), scored(&[1]), &ctx()).unwrap_err();
    assert!(err.to_string().contains("circuit breaker open"), "{err}");
    assert!(
        started.elapsed() < TIMEOUT / 2,
        "an open breaker must not pay the timeout: {:?}",
        started.elapsed()
    );
    assert_eq!(
        sidecar.calls(),
        dialed,
        "an open breaker must not reach the sidecar at all"
    );
    assert_eq!(stats.errors(SidecarErrorReason::BreakerOpen), 1);
}

#[test]
fn a_successful_call_resets_the_failure_run() {
    // One sidecar that refuses every other call: the breaker counts
    // *consecutive* failures, so an intermittently-flaky sidecar (well under
    // the threshold in a row) must never trip it.
    let sidecar = FakeSidecar::start(Behavior::Flaky);
    let proxy = proxy(sidecar.endpoint(), None);

    let (mut ok, mut err) = (0, 0);
    for _ in 0..20 {
        if proxy.rerank(&query(), scored(&[1]), &ctx()).is_ok() {
            ok += 1;
        } else {
            err += 1;
        }
    }
    assert!(ok > 0 && err > 0, "the sidecar really did flap: {ok} ok, {err} err");
    assert_eq!(
        proxy.stats().breaker_state(),
        BreakerState::Closed,
        "an intermittent failure never reaches the consecutive threshold"
    );
    assert_eq!(proxy.stats().breaker_opened_total(), 0);
}

// --- 1.7: end-to-end over the real MATCH path -------------------------------------

static ITEM_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "description",
        ty: FluxType::Str,
    },
];
static ITEM: TableSchema = TableSchema {
    name: "Item",
    columns: ITEM_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[IndexSchema::FullText {
        column: 1,
        language: FullTextLanguage::Simple,
        stop_words: false,
        stemming: false,
    }],
    visibility: VisibilityRule::PublicAll,
};

/// Tuned so pure BM25 gives the deterministic order 3 > 2 > 1 for 'sword'
/// (id 4 has no match) — the same corpus the in-process hook test uses, so
/// a sidecar reranker is checked against the identical BM25 baseline.
const CORPUS: &[(u64, &str)] = &[
    (1, "sword"),
    (2, "sword sword shield"),
    (3, "sword sword sword arena"),
    (4, "a dragon with no blade at all"),
];

const RANKED: &str =
    "SELECT * FROM Item WHERE description MATCH 'sword' ORDER BY SCORE DESC LIMIT 3";

fn config_with_sidecar(endpoint: &str, token: Option<&str>) -> Config {
    Config {
        plugins: vec![PluginDecl {
            name: "reranker".into(),
            capability: "score_reranker".into(),
            host: PluginHost::Sidecar {
                endpoint: endpoint.into(),
                timeout_ms: TIMEOUT.as_millis() as u64,
                token: token.map(ToOwned::to_owned),
            },
            applies_to: PluginScope {
                tables: vec!["Item".into()],
                columns: vec!["description".into()],
            },
        }],
        ..Config::default()
    }
}

fn seeded(schema: &Schema) -> MemStore {
    let store = MemStore::new(schema).unwrap();
    let item = store.table_id("Item").unwrap();
    let mut tx = store.begin();
    for (id, description) in CORPUS {
        tx.insert(
            item,
            vec![RowValue::U64(*id), RowValue::Str((*description).into())],
        )
        .unwrap();
    }
    tx.commit().unwrap();
    store
}

/// A manager wired to a sidecar-hosted reranker at `endpoint`.
fn manager_with(schema: &Arc<Schema>, endpoint: &str) -> (SubscriptionManager, Arc<PluginRegistry>) {
    let registry =
        Arc::new(PluginRegistry::build(schema, &config_with_sidecar(endpoint, None)).unwrap());
    let mut manager = SubscriptionManager::new(Arc::clone(schema), SubscriptionLimits::default());
    manager.set_plugins(Arc::clone(&registry));
    (manager, registry)
}

fn ids(manager: &SubscriptionManager, store: &MemStore) -> Vec<u64> {
    let result = manager
        .query_json(
            Subscriber::server_peer(Identity::from_bytes([9; 32])),
            RANKED,
            &store.snapshot(),
        )
        .expect("a MATCH query must never fail because of a sidecar (PLG-031)");
    result["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_u64().unwrap())
        .collect()
}

#[test]
fn a_sidecar_reranker_reorders_a_real_match_query() {
    let sidecar = FakeSidecar::start(Behavior::Reverse);
    let schema = Arc::new(Schema::from_tables([&ITEM]).unwrap());
    let store = seeded(&schema);
    let (manager, _registry) = manager_with(&schema, &sidecar.endpoint());

    assert_eq!(
        ids(&manager, &store),
        vec![1, 2, 3],
        "the sidecar's order is authoritative for the top-K, exactly as an \
         in-process reranker's is (PLG-040) — the query path cannot tell the \
         two hosts apart"
    );
}

#[test]
fn a_stopped_sidecar_leaves_match_queries_serving_bm25() {
    let schema = Arc::new(Schema::from_tables([&ITEM]).unwrap());
    let store = seeded(&schema);
    let (manager, registry) = manager_with(&schema, &dead_endpoint());

    // The PLG-031 acceptance criterion: with the sidecar stopped, MATCH
    // still returns the pure-BM25 result and no client error.
    assert_eq!(
        ids(&manager, &store),
        vec![3, 2, 1],
        "a stopped sidecar degrades to BM25, it does not fail the query"
    );

    let stats = registry.get("reranker").unwrap().sidecar.clone().unwrap();
    assert_eq!(stats.errors(SidecarErrorReason::Connect), 1);
    assert_eq!(
        registry.get("reranker").unwrap().state.errors(),
        1,
        "a sidecar failure also meters as an ordinary plugin error"
    );
}

#[test]
fn a_timing_out_sidecar_degrades_within_budget_and_opens_the_breaker() {
    let sidecar = FakeSidecar::start(Behavior::Hang);
    let schema = Arc::new(Schema::from_tables([&ITEM]).unwrap());
    let store = seeded(&schema);
    let (manager, registry) = manager_with(&schema, &sidecar.endpoint());
    let stats = registry.get("reranker").unwrap().sidecar.clone().unwrap();

    // Five queries against a wedged sidecar: each degrades to BM25, and the
    // fifth trips the breaker.
    for _ in 0..5 {
        assert_eq!(
            ids(&manager, &store),
            vec![3, 2, 1],
            "a hung sidecar degrades to BM25 (PLG-031)"
        );
    }
    assert_eq!(stats.breaker_state(), BreakerState::Open);
    assert_eq!(stats.errors(SidecarErrorReason::Timeout), 5);

    // From here the sidecar costs the query nothing at all: this is why the
    // breaker exists, and it is the difference between a degraded database
    // and an unusably slow one.
    let started = Instant::now();
    assert_eq!(ids(&manager, &store), vec![3, 2, 1]);
    assert!(
        started.elapsed() < TIMEOUT,
        "with the breaker open the query pays no sidecar cost: {:?}",
        started.elapsed()
    );

    let report = registry.report();
    let row = report.iter().find(|p| p.name == "reranker").unwrap();
    assert_eq!(row.breaker, Some("open"), "PLG-060 reports the breaker");
    assert!(
        row.sidecar_errors.contains(&("timeout", 5)),
        "fluxum_plugin_sidecar_errors_total{{reason=\"timeout\"}}: {:?}",
        row.sidecar_errors
    );
    assert_eq!(row.health, "active", "degrading is not disabling");
}

// --- PLG-061: a sidecar is granted nothing --------------------------------------

#[test]
fn a_sidecar_cannot_disable_itself_out_of_the_operators_hands() {
    // PLG-061: hot disable works on a sidecar binding like any other, with
    // no core restart — the breaker is automatic, this is the manual lever.
    let sidecar = FakeSidecar::start(Behavior::Reverse);
    let schema = Arc::new(Schema::from_tables([&ITEM]).unwrap());
    let store = seeded(&schema);
    let (manager, registry) = manager_with(&schema, &sidecar.endpoint());

    assert_eq!(ids(&manager, &store), vec![1, 2, 3], "sidecar in effect");
    assert!(registry.set_disabled("reranker", true));
    assert_eq!(
        ids(&manager, &store),
        vec![3, 2, 1],
        "a disabled sidecar falls back to BM25 with no restart (PLG-061)"
    );
    assert!(registry.set_disabled("reranker", false));
    assert_eq!(ids(&manager, &store), vec![1, 2, 3], "and back on");
}
