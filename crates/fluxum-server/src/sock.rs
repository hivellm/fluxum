//! Listener/socket hardening knobs (SPEC-026 SEC-042): the accept backlog,
//! TCP keepalive, and `TCP_DEFER_ACCEPT` both transports share. Every knob
//! defaults to today's behavior — a config nobody touched binds a listener
//! byte-identical to a plain `TcpListener::bind`.

use std::io;
use std::time::Duration;

use tokio::net::{TcpListener, TcpSocket, TcpStream};

/// Listener-level socket options, resolved from `server.*` config.
#[derive(Debug, Clone, Copy, Default)]
pub struct SocketOptions {
    /// Listen backlog (`None` = the tokio default, 1024).
    pub accept_backlog: Option<u32>,
    /// Keepalive probe time for accepted sockets (`None` = off).
    pub tcp_keepalive: Option<Duration>,
    /// `TCP_DEFER_ACCEPT` window (`None` = off; Linux only, logged and
    /// ignored elsewhere).
    pub defer_accept: Option<Duration>,
}

impl SocketOptions {
    /// Resolve from config: a `0` leaves the corresponding knob at today's
    /// behavior.
    pub fn from_config(server: &fluxum_core::config::ServerConfig) -> Self {
        Self {
            accept_backlog: (server.accept_backlog != 0).then_some(server.accept_backlog),
            tcp_keepalive: (server.tcp_keepalive_secs != 0)
                .then(|| Duration::from_secs(server.tcp_keepalive_secs)),
            defer_accept: (server.tcp_defer_accept_secs != 0)
                .then(|| Duration::from_secs(server.tcp_defer_accept_secs)),
        }
    }
}

/// Bind `addr` with the hardening knobs applied. Mirrors what
/// `TcpListener::bind` does (first resolvable address wins, `SO_REUSEADDR`
/// on non-Windows) plus the configured backlog and `TCP_DEFER_ACCEPT`.
pub async fn bind(
    addr: impl tokio::net::ToSocketAddrs,
    options: SocketOptions,
) -> io::Result<TcpListener> {
    let mut last_err = None;
    for addr in tokio::net::lookup_host(addr).await? {
        let socket = if addr.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        // Parity with TcpListener::bind: tokio sets SO_REUSEADDR on every
        // non-Windows platform (on Windows it would allow port hijacking).
        #[cfg(not(windows))]
        socket.set_reuseaddr(true)?;
        apply_defer_accept(&socket, options.defer_accept);
        match socket
            .bind(addr)
            .and_then(|()| socket.listen(options.accept_backlog.unwrap_or(1024)))
        {
            Ok(listener) => return Ok(listener),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "address resolved to nothing")
    }))
}

/// Apply the keepalive knob to an accepted socket. Best-effort: a failure
/// is logged, never fatal — a reaped-late dead peer beats a dropped live one.
pub fn apply_keepalive(stream: &TcpStream, keepalive: Option<Duration>) {
    let Some(time) = keepalive else { return };
    let sock = socket2::SockRef::from(stream);
    let params = socket2::TcpKeepalive::new().with_time(time);
    if let Err(e) = sock.set_tcp_keepalive(&params) {
        tracing::debug!(target: "fluxum::server", error = %e, "tcp keepalive not applied");
    }
}

/// `TCP_DEFER_ACCEPT` (SEC-042): the kernel holds a completed handshake
/// until the client sends data, so connect-and-idle floods never wake the
/// accept loop. Linux-only; elsewhere the knob is logged and ignored.
#[cfg(target_os = "linux")]
fn apply_defer_accept(socket: &TcpSocket, defer: Option<Duration>) {
    use std::os::fd::AsRawFd;
    let Some(window) = defer else { return };
    let secs: libc::c_int = libc::c_int::try_from(window.as_secs()).unwrap_or(libc::c_int::MAX);
    // SAFETY: a plain setsockopt on a socket we own, with a c_int-sized
    // value buffer, exactly as the man page prescribes.
    let rc = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::IPPROTO_TCP,
            libc::TCP_DEFER_ACCEPT,
            std::ptr::from_ref(&secs).cast(),
            libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>()).unwrap_or(4),
        )
    };
    if rc != 0 {
        let e = io::Error::last_os_error();
        tracing::debug!(target: "fluxum::server", error = %e, "TCP_DEFER_ACCEPT not applied");
    }
}

#[cfg(not(target_os = "linux"))]
fn apply_defer_accept(_socket: &TcpSocket, defer: Option<Duration>) {
    if defer.is_some() {
        tracing::debug!(
            target: "fluxum::server",
            "tcp_defer_accept_secs is Linux-only; ignored on this platform"
        );
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[tokio::test]
    async fn default_options_bind_and_serve() {
        let listener = bind("127.0.0.1:0", SocketOptions::default()).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server_side, _) = listener.accept().await.unwrap();
        // Keepalive off is a no-op; on applies without error.
        apply_keepalive(&server_side, None);
        apply_keepalive(&server_side, Some(Duration::from_secs(30)));
        drop(client);
    }

    #[tokio::test]
    async fn explicit_backlog_and_defer_accept_bind() {
        let options = SocketOptions {
            accept_backlog: Some(64),
            tcp_keepalive: Some(Duration::from_secs(30)),
            defer_accept: Some(Duration::from_secs(5)),
        };
        // On Linux this applies TCP_DEFER_ACCEPT; elsewhere it logs and
        // ignores — either way the listener binds and accepts.
        let listener = bind("127.0.0.1:0", options).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        // With defer-accept the kernel may hold the connection until data
        // flows; send a byte so accept completes on every platform.
        use tokio::io::AsyncWriteExt;
        client.write_all(b"x").await.unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        apply_keepalive(&stream, options.tcp_keepalive);
    }

    #[test]
    fn zeroed_config_resolves_to_defaults() {
        let server = fluxum_core::config::ServerConfig::default();
        let options = SocketOptions::from_config(&server);
        assert!(options.accept_backlog.is_none());
        assert!(options.tcp_keepalive.is_none());
        assert!(options.defer_accept.is_none());

        let hardened = fluxum_core::config::ServerConfig {
            accept_backlog: 4096,
            tcp_keepalive_secs: 60,
            tcp_defer_accept_secs: 5,
            ..fluxum_core::config::ServerConfig::default()
        };
        let options = SocketOptions::from_config(&hardened);
        assert_eq!(options.accept_backlog, Some(4096));
        assert_eq!(options.tcp_keepalive, Some(Duration::from_secs(60)));
        assert_eq!(options.defer_accept, Some(Duration::from_secs(5)));
    }
}
