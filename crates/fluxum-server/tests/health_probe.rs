//! `fluxum-server --healthcheck` (SPEC-025 OPS-020): the probe the scratch
//! container image's `HEALTHCHECK` execs. Exercises both transport paths —
//! plaintext and TLS (SEC-059, self-signed fixture cert) — against a
//! minimal one-shot loopback HTTP responder, plus the failure modes: a
//! non-200 status and an unreachable listener.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

use fluxum_core::config::Config;
use fluxum_server::health;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

const CERT_PEM: &str = include_str!("fixtures/tls/cert.pem");
const KEY_PEM: &str = include_str!("fixtures/tls/key.pem");
const OK_RESPONSE: &str = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok";

/// A `Config` whose loopback HTTP listener is `port`.
fn config_for(port: u16) -> Config {
    let mut config = Config::default();
    config.server.http_port = port;
    config
}

/// Read until the end of the request headers (`\r\n\r\n`) or EOF.
fn read_request(stream: &mut impl Read) {
    let mut buf = [0_u8; 1024];
    let mut seen = Vec::new();
    loop {
        let n = stream.read(&mut buf).unwrap();
        seen.extend_from_slice(&buf[..n]);
        if n == 0 || seen.windows(4).any(|w| w == b"\r\n\r\n") {
            return;
        }
    }
}

/// Serve exactly one plaintext HTTP exchange on an ephemeral loopback port.
fn one_shot_http(response: &'static str) -> (u16, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_request(&mut stream);
        stream.write_all(response.as_bytes()).unwrap();
    });
    (port, handle)
}

#[test]
fn probe_reports_healthy_on_200() {
    let (port, server) = one_shot_http(OK_RESPONSE);
    health::probe(&config_for(port)).unwrap();
    server.join().unwrap();
}

#[test]
fn probe_rejects_non_200() {
    let (port, server) =
        one_shot_http("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n");
    let err = health::probe(&config_for(port)).unwrap_err();
    assert!(err.contains("503"), "{err}");
    server.join().unwrap();
}

#[test]
fn probe_rejects_unreachable_listener() {
    // Bind-then-drop reserves a port that is closed by the time the probe
    // dials it.
    let port = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let err = health::probe(&config_for(port)).unwrap_err();
    assert!(err.contains("connect"), "{err}");
}

#[test]
fn probe_speaks_tls_when_configured() {
    // One-shot TLS responder using the fixture self-signed pair — the same
    // PEM files `server.tls.{cert,key}` would point at in production.
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(CERT_PEM.as_bytes())
        .collect::<Result<_, _>>()
        .unwrap();
    let key = PrivateKeyDer::from_pem_slice(KEY_PEM.as_bytes()).unwrap();
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        let mut conn = rustls::ServerConnection::new(Arc::new(server_config)).unwrap();
        let (mut sock, _) = listener.accept().unwrap();
        let mut tls = rustls::Stream::new(&mut conn, &mut sock);
        read_request(&mut tls);
        tls.write_all(OK_RESPONSE.as_bytes()).unwrap();
        tls.flush().unwrap();
    });

    // The probe only checks `tls.is_enabled()` (cert + key both set); the
    // paths are read by the *server* at bind time, not by the probe.
    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("cert.pem");
    let key_path = dir.path().join("key.pem");
    std::fs::write(&cert_path, CERT_PEM).unwrap();
    std::fs::write(&key_path, KEY_PEM).unwrap();
    let mut config = config_for(port);
    config.server.tls.cert = Some(cert_path);
    config.server.tls.key = Some(key_path);

    health::probe(&config).unwrap();
    server.join().unwrap();
}
