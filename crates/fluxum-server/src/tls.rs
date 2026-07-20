//! Optional built-in transport TLS (SPEC-026 SEC-059): `rustls` termination
//! shared by both listeners.
//!
//! When `server.tls.{cert,key}` are configured, [`load_acceptor`] builds a
//! [`tokio_rustls::TlsAcceptor`] from the PEM files. Each accepted socket is
//! wrapped by [`MaybeTls`] — a single concrete stream type that is either the
//! plaintext socket or the TLS-terminated one — so the transport read/route/
//! write loops are written once against `MaybeTls` and neither branches on
//! TLS nor pays for generics/monomorphization.

use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Build a TLS acceptor from PEM cert-chain and private-key files.
///
/// # Errors
/// The files cannot be read, contain no certificate / no private key, or
/// rustls rejects the pair.
pub fn load_acceptor(cert_path: &Path, key_path: &Path) -> io::Result<TlsAcceptor> {
    let cert_pem = std::fs::read(cert_path)?;
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&cert_pem)
        .collect::<Result<_, _>>()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no certificate in {}", cert_path.display()),
        ));
    }

    let key_pem = std::fs::read(key_path)?;
    let key = PrivateKeyDer::from_pem_slice(&key_pem).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no private key in {}: {e}", key_path.display()),
        )
    })?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// A connection socket that may or may not be TLS-terminated. Both variants
/// are `Unpin`, so the pin projections below are safe `get_mut` accesses.
pub enum MaybeTls {
    /// Plaintext TCP.
    Plain(TcpStream),
    /// TLS over TCP (boxed — the rustls stream is large).
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

impl MaybeTls {
    /// Complete the TLS handshake on `stream` when `acceptor` is set (SEC-059);
    /// otherwise wrap it plaintext. A handshake failure is an `io::Error`,
    /// closing the connection.
    pub async fn accept(stream: TcpStream, acceptor: Option<&TlsAcceptor>) -> io::Result<Self> {
        match acceptor {
            Some(acceptor) => Ok(Self::Tls(Box::new(acceptor.accept(stream).await?))),
            None => Ok(Self::Plain(stream)),
        }
    }
}

impl AsyncRead for MaybeTls {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTls {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}
