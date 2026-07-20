//! Structured security-event trail (SPEC-012 OBS-090..092; OWASP A09 Logging
//! & Alerting Failures, F-022/F-023).
//!
//! The security-relevant allow/deny moments — authentication outcomes,
//! pre-auth connection-guard rejections, session-token rejections, and admin
//! access-control decisions — are emitted on a dedicated `tracing` target so
//! they survive the default `info` filter (denials at `WARN`, allows at
//! `INFO`). Without this the same events logged at `debug` and were invisible
//! unless an operator turned on global debug, so a live attack left no
//! observable footprint at the default level.
//!
//! Every event shares a uniform field schema — `event`, `outcome`, and the
//! subset of `identity` / `operator` / `source_ip` / `reason` / `resource`
//! that applies — emitted through the helpers here so field names never drift
//! across call sites. **No helper ever logs token bytes or secret material:**
//! identities are the public `SHA-256`-derived value, never the token.

use std::net::IpAddr;

/// The dedicated tracing target. A subscriber can route or retain this target
/// independently (e.g. to a durable security log) — see
/// [`fluxum_core::config`] and the server's logging init.
pub const TARGET: &str = "security";

/// A successful authentication (`INFO`): the connection authenticated as
/// `identity_hex` from `source_ip`.
pub fn auth_success(source_ip: IpAddr, identity_hex: &str) {
    tracing::info!(
        target: TARGET,
        event = "auth_success",
        outcome = "allow",
        %source_ip,
        identity = identity_hex,
    );
}

/// A failed authentication (`WARN`): `source_ip` presented a bad credential.
/// `reason` is a category, never the token.
pub fn auth_failure(source_ip: IpAddr, reason: &str) {
    tracing::warn!(
        target: TARGET,
        event = "auth_failure",
        outcome = "deny",
        %source_ip,
        reason,
    );
}

/// A pre-auth connection refused by the guard (`WARN`, SPEC-026 SEC-03x):
/// caps, backoff, blocklist, global ceiling, overload shed, or a spoofed
/// proxy preamble. `reason` is the `fluxum_conn_rejected_total` label.
pub fn conn_rejected(source_ip: IpAddr, reason: &str) {
    tracing::warn!(
        target: TARGET,
        event = "conn_rejected",
        outcome = "deny",
        %source_ip,
        reason,
    );
}

/// An HTTP session request refused (`WARN`, SPEC-026 SEC-05x): an unknown
/// token, an IP-binding mismatch (suspected hijack), an expired session, or a
/// revoked one. `reason` is the `fluxum_session_rejected_total` label.
pub fn session_rejected(source_ip: IpAddr, reason: &str) {
    tracing::warn!(
        target: TARGET,
        event = "session_rejected",
        outcome = "deny",
        %source_ip,
        reason,
    );
}

/// An admin-API request refused by the access guard (`WARN`, SPEC-026
/// SEC-054): an untrusted source IP or a missing/invalid operator credential.
pub fn admin_denied(source_ip: IpAddr, route: &str, reason: &str) {
    tracing::warn!(
        target: TARGET,
        event = "admin_denied",
        outcome = "deny",
        %source_ip,
        route,
        reason,
    );
}

/// A state-changing admin operation that was allowed (`INFO`, SPEC-026
/// SEC-054): the operator (a display name or `"loopback"`, never a token) and
/// the route. The post-incident "who changed what over the ops surface" trail.
pub fn admin_mutation(source_ip: IpAddr, operator: &str, route: &str) {
    tracing::info!(
        target: TARGET,
        event = "admin_mutation",
        outcome = "allow",
        %source_ip,
        operator,
        route,
    );
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    /// A `MakeWriter` that appends every log line into a shared buffer.
    #[derive(Clone, Default)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);
    impl Write for Buffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> MakeWriter<'a> for Buffer {
        type Writer = Buffer;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Run `f` with a subscriber capturing this thread's events at the given
    /// filter, and return everything it logged.
    fn capture(filter: &str, f: impl FnOnce()) -> String {
        let buf = Buffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_env_filter(EnvFilter::new(filter))
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        let bytes = buf.0.lock().unwrap().clone();
        String::from_utf8(bytes).unwrap()
    }

    fn ip() -> IpAddr {
        "203.0.113.9".parse().unwrap()
    }

    #[test]
    fn denials_are_visible_at_the_default_info_level() {
        // The default filter operators run at.
        let out = capture("info", || {
            auth_failure(ip(), "bad_credential");
            conn_rejected(ip(), "blocked");
            session_rejected(ip(), "ip_mismatch");
            admin_denied(ip(), "/reducer/x", "untrusted_ip");
        });
        assert!(out.contains("auth_failure"), "auth_failure visible: {out}");
        assert!(out.contains("outcome=\"deny\""), "carries outcome: {out}");
        assert!(out.contains("203.0.113.9"), "carries source_ip");
        assert!(out.contains("reason=\"bad_credential\""));
        assert!(out.contains("conn_rejected"));
        assert!(out.contains("session_rejected") && out.contains("ip_mismatch"));
        assert!(out.contains("admin_denied") && out.contains("/reducer/x"));
        // The denials are WARN-level so they survive even a `warn` global floor.
        let quiet = capture("warn", || auth_failure(ip(), "bad_credential"));
        assert!(
            quiet.contains("auth_failure"),
            "WARN denial survives warn floor"
        );
    }

    #[test]
    fn allow_events_carry_their_attribution() {
        let out = capture("info", || {
            auth_success(ip(), "abcd1234");
            admin_mutation(ip(), "operator-name", "/config/reload");
        });
        assert!(out.contains("auth_success") && out.contains("outcome=\"allow\""));
        assert!(
            out.contains("identity=\"abcd1234\""),
            "identity attributed: {out}"
        );
        assert!(out.contains("admin_mutation") && out.contains("operator=\"operator-name\""));
        assert!(out.contains("/config/reload"));
    }

    #[test]
    fn the_target_is_the_dedicated_security_target() {
        // Filtered to ONLY the security target: the events still come through,
        // proving they carry `target: "security"` and can be routed on it.
        let out = capture("security=info", || auth_failure(ip(), "bad_credential"));
        assert!(
            out.contains("auth_failure"),
            "routed on the security target: {out}"
        );
        // A non-security event at the same call is filtered out.
        let none = capture("security=info", || {
            tracing::info!(target: "fluxum::other", "unrelated");
        });
        assert!(
            !none.contains("unrelated"),
            "only the security target passes"
        );
    }
}
