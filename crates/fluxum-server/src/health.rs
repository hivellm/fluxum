//! Liveness probe for `fluxum-server --healthcheck` (SPEC-025 OPS-020).
//!
//! The published container image runs `FROM scratch` — no shell, no curl —
//! so the Docker `HEALTHCHECK` execs the server binary itself, which sends
//! `GET /health` to the loopback HTTP listener and exits 0/1. Loopback
//! always passes the SEC-054 admin guard, so the probe holds under any
//! `server.admin` posture.
//!
//! When `server.tls` is configured, both listeners terminate TLS (SEC-059),
//! so the probe completes a rustls handshake with certificate verification
//! disabled: the question is "is this process serving?", not "is the peer
//! who it claims?" — the certificate on the other side is this very
//! server's own, typically for a public name that `127.0.0.1` would never
//! verify against anyway.

use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use fluxum_core::config::Config;
use tokio_rustls::rustls;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::crypto::WebPkiSupportedAlgorithms;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::{DigitallySignedStruct, SignatureScheme};

/// Per-step socket budget. The whole probe stays well inside the 3 s the
/// Dockerfile `HEALTHCHECK --timeout` allows.
const STEP_TIMEOUT: Duration = Duration::from_secs(2);

/// Probe `GET /health` on `127.0.0.1:{server.http_port}`.
///
/// # Errors
/// The listener is unreachable, the (optional) TLS handshake fails, or the
/// response status is not `200`.
pub fn probe(config: &Config) -> Result<(), String> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), config.server.http_port);
    let stream = TcpStream::connect_timeout(&addr, STEP_TIMEOUT)
        .map_err(|e| format!("connect {addr}: {e}"))?;
    stream
        .set_read_timeout(Some(STEP_TIMEOUT))
        .and_then(|()| stream.set_write_timeout(Some(STEP_TIMEOUT)))
        .map_err(|e| format!("socket timeouts: {e}"))?;

    let request = b"GET /health HTTP/1.1\r\nhost: 127.0.0.1\r\nconnection: close\r\n\r\n";
    let status_line = if config.server.tls.is_enabled() {
        let mut conn = tls_connection()?;
        let mut sock = stream;
        let mut tls = rustls::Stream::new(&mut conn, &mut sock);
        exchange(&mut tls, request)?
    } else {
        let mut sock = stream;
        exchange(&mut sock, request)?
    };

    match status_line.split_whitespace().nth(1) {
        Some("200") => Ok(()),
        _ => Err(format!("unhealthy response: {status_line}")),
    }
}

/// Write `request`, then read up to the end of the status line.
fn exchange(stream: &mut (impl Read + Write), request: &[u8]) -> Result<String, String> {
    stream
        .write_all(request)
        .and_then(|()| stream.flush())
        .map_err(|e| format!("send request: {e}"))?;

    // The status line is tiny; responses carry `Content-Length`, so there is
    // no need to drain the body — read until the first LF or the buffer is
    // full, then let dropping the socket end the conversation.
    let mut buf = [0_u8; 512];
    let mut len = 0;
    while len < buf.len() {
        match stream.read(&mut buf[len..]) {
            Ok(0) => break,
            Ok(n) => {
                len += n;
                if buf[..len].contains(&b'\n') {
                    break;
                }
            }
            Err(e) => return Err(format!("read response: {e}")),
        }
    }
    let line = buf[..len].split(|&b| b == b'\n').next().unwrap_or_default();
    Ok(String::from_utf8_lossy(line).trim_end().to_owned())
}

/// A TLS client connection to loopback that accepts whatever certificate the
/// server presents (see the module comment for why that is sound here).
fn tls_connection() -> Result<rustls::ClientConnection, String> {
    let provider = rustls::crypto::ring::default_provider();
    let algorithms = provider.signature_verification_algorithms;
    let tls_config = rustls::ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("tls protocol versions: {e}"))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(TrustAnyCert(algorithms)))
        .with_no_client_auth();
    rustls::ClientConnection::new(
        Arc::new(tls_config),
        ServerName::IpAddress(Ipv4Addr::LOCALHOST.into()),
    )
    .map_err(|e| format!("tls client: {e}"))
}

/// Accepts any server certificate; still verifies handshake signatures, so
/// the peer must at least hold the presented key.
#[derive(Debug)]
struct TrustAnyCert(WebPkiSupportedAlgorithms);

impl ServerCertVerifier for TrustAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}
