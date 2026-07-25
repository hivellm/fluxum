//! SEC-059 — `load_acceptor`'s failure modes are operator-actionable
//! errors, never panics: missing files, PEM without a certificate, PEM
//! without a private key, and a mismatched pair are each named.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_server::tls::load_acceptor;

#[test]
fn tls_material_failures_are_named_io_errors() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("absent.pem");

    // Missing files: plain io errors.
    assert!(load_acceptor(&missing, &missing).is_err());

    // A readable file with no certificate in it.
    let empty = dir.path().join("empty.pem");
    std::fs::write(&empty, "-- not pem --\n").unwrap();
    let err = match load_acceptor(&empty, &empty) {
        Err(err) => err,
        Ok(_) => panic!("junk PEM must not load"),
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err}");

    // A certificate present but the key file holds none.
    let cert = dir.path().join("cert.pem");
    std::fs::write(
        &cert,
        "-----BEGIN CERTIFICATE-----\nMIIBszCCAVmgAwIBAgIUGjRuG0AeIQ+RhBK09lHkT6JiIvcwCgYIKoZIzj0EAwIw\nGjEYMBYGA1UEAwwPZmx1eHVtLXRlc3QtY2VydDAeFw0yNDAxMDEwMDAwMDBaFw0z\nNDAxMDEwMDAwMDBaMBoxGDAWBgNVBAMMD2ZsdXh1bS10ZXN0LWNlcnQwWTATBgcq\nhkjOPQIBBggqhkjOPQMBBwNCAAQ0mFTHhCJz1RDPFPSSjzMYFyMFwzKUFGVvGDRP\nnpe4/dGZ38792ryjXfBhPtWv1SPvhb4Cs2vNQ6ZWkNPfSs2ZoyEwHzAdBgNVHQ4E\nFgQUS7v0Q1qEsUFEjT7BXd0S3B2eFmMwCgYIKoZIzj0EAwIDSAAwRQIhAKN9opsz\nOOMkc2jTsIRDwjBTIHtsBXqrq2XPuMdo3TWjAiAcXfCoR1kFmkjcXcHHLZuVSB0i\nbK9nWzn2N70/PSPYlQ==\n-----END CERTIFICATE-----\n",
    )
    .unwrap();
    let err = match load_acceptor(&cert, &empty) {
        Err(err) => err,
        Ok(_) => panic!("a keyless pair must not load"),
    };
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("no private key"), "{err}");
}

/// SEC-042: a `0` leaves each socket knob at today's behavior; non-zero
/// values resolve to the corresponding option.
#[test]
fn socket_options_resolve_zero_as_off() {
    use fluxum_server::sock::SocketOptions;
    use std::time::Duration;

    let mut server = fluxum_core::config::ServerConfig::default();
    let off = SocketOptions::from_config(&server);
    assert!(off.accept_backlog.is_none());
    assert!(off.tcp_keepalive.is_none());
    assert!(off.defer_accept.is_none());

    server.accept_backlog = 4096;
    server.tcp_keepalive_secs = 60;
    server.tcp_defer_accept_secs = 5;
    let on = SocketOptions::from_config(&server);
    assert_eq!(on.accept_backlog, Some(4096));
    assert_eq!(on.tcp_keepalive, Some(Duration::from_secs(60)));
    assert_eq!(on.defer_accept, Some(Duration::from_secs(5)));
}
