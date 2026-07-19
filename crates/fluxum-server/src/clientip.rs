//! Trusted-proxy client-IP resolution (SPEC-026 SEC-035/036): the pure
//! logic both transports use to decide *which* IP the per-IP defenses key
//! on when Fluxum runs behind a reverse proxy / load balancer.
//!
//! The trust rule is identical on both transports and deliberately blunt:
//! forwarding metadata is honored **only** when the socket peer is listed in
//! `server.trusted_proxies`; from anyone else it is at best ignored (HTTP
//! `X-Forwarded-For`) and at worst a protocol error (a TCP PROXY v2
//! preamble, which no ordinary client has any business sending). With the
//! list empty — the default — this module is inert and the socket peer *is*
//! the client, byte-identical to the pre-proxy-awareness behavior.
//!
//! - **HTTP** ([`resolve_forwarded_for`]): `X-Forwarded-For` under the
//!   rightmost-untrusted rule — walk the chain right to left, skip hops that
//!   are themselves trusted proxies, and the first untrusted address is the
//!   client. Entries to its left are client-forgeable noise and never
//!   consulted.
//! - **TCP** ([`parse_v2_preamble`]): the PROXY protocol v2 binary preamble
//!   (v1 text form is deliberately unsupported: v2 is length-prefixed and
//!   unambiguous, and every proxy that speaks v1 also speaks v2).
//!
//! Parsing here is pure (bytes in, verdict out) so it unit-tests without
//! sockets; the transports own the IO and the metrics.

use std::net::IpAddr;

use fluxum_core::net::IpSet;

/// The 12-byte PROXY protocol v2 signature.
pub const V2_SIG: [u8; 12] = [
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A,
];

/// Largest possible v2 preamble: 16-byte header + a full u16 of payload.
pub const V2_MAX_LEN: usize = 16 + u16::MAX as usize;

/// Resolve the effective client IP for an HTTP request (SEC-035).
///
/// `peer` is the socket peer; `xff` is the raw `X-Forwarded-For` value, if
/// the request carried one. Returns the IP every per-IP defense should key
/// on, or an error when a *trusted* proxy sent a header we cannot parse —
/// the caller rejects that request and counts it, because garbage from the
/// one peer we rely on for attribution is a misconfiguration worth surfacing,
/// never something to guess around.
///
/// # Errors
/// A malformed entry in a header from a trusted peer.
pub fn resolve_forwarded_for(
    peer: IpAddr,
    xff: Option<&str>,
    trusted: &IpSet,
) -> Result<IpAddr, String> {
    // Untrusted peer (or feature off): the header is client-forgeable noise.
    if trusted.is_empty() || !trusted.contains(peer) {
        return Ok(peer);
    }
    let Some(raw) = xff else {
        // A trusted proxy that sent no header is a direct request from the
        // proxy host itself (health checks, probes).
        return Ok(peer);
    };
    // Rightmost-untrusted: every hop a trusted proxy appended is skipped;
    // the first address *not* ours is what the outermost trusted proxy saw.
    let mut leftmost = None;
    for entry in raw.rsplit(',') {
        let ip = parse_forwarded_entry(entry)
            .ok_or_else(|| format!("malformed X-Forwarded-For entry '{}'", entry.trim()))?;
        if !trusted.contains(ip) {
            return Ok(ip);
        }
        leftmost = Some(ip);
    }
    // The whole chain is trusted proxies talking to each other; the leftmost
    // is the closest thing to a client the chain names. An empty header
    // parsed no entries and is malformed.
    leftmost.ok_or_else(|| "empty X-Forwarded-For".to_owned())
}

/// One `X-Forwarded-For` entry: a bare IP, a v4 `ip:port`, or a bracketed
/// v6 `[ip]:port` / `[ip]`.
fn parse_forwarded_entry(entry: &str) -> Option<IpAddr> {
    let entry = entry.trim();
    if let Ok(ip) = entry.parse::<IpAddr>() {
        return Some(ip.to_canonical());
    }
    // `[v6]` or `[v6]:port`.
    if let Some(rest) = entry.strip_prefix('[') {
        let (inside, _after) = rest.split_once(']')?;
        return inside.parse::<IpAddr>().ok().map(|ip| ip.to_canonical());
    }
    // `v4:port` (a lone colon; more than one means it was unbracketed v6,
    // already handled by the bare parse above).
    if let Some((host, port)) = entry.split_once(':')
        && port.parse::<u16>().is_ok()
    {
        return host
            .parse::<IpAddr>()
            .ok()
            .filter(|ip| ip.is_ipv4())
            .map(|ip| ip.to_canonical());
    }
    None
}

/// The verdict on a buffered PROXY v2 preamble (SEC-036).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum V2Preamble {
    /// Not enough bytes yet to finish the header/payload; read more. The
    /// bytes so far *are* a valid prefix of a preamble.
    Incomplete,
    /// A complete, valid preamble: the client source address it carries
    /// (`None` for `LOCAL` commands and `UNSPEC` families — the proxy is
    /// talking for itself, use the socket peer), and how many bytes the
    /// preamble consumed.
    Complete {
        /// Source address the proxy asserts, if the preamble names one.
        source: Option<IpAddr>,
        /// Total preamble length to strip from the stream.
        consumed: usize,
    },
}

/// Whether `buf` is (a prefix of) the v2 signature. On a short buffer this
/// answers for the bytes present, so a reader can keep the check cheap while
/// bytes trickle in.
pub fn is_v2_signature_prefix(buf: &[u8]) -> bool {
    let n = buf.len().min(V2_SIG.len());
    buf[..n] == V2_SIG[..n]
}

/// Parse a PROXY protocol v2 preamble at the start of `buf`.
///
/// # Errors
/// The bytes are not a well-formed preamble (bad signature once 12 bytes
/// are present, wrong version, unknown command, or a payload too short for
/// its declared address family).
pub fn parse_v2_preamble(buf: &[u8]) -> Result<V2Preamble, String> {
    if !is_v2_signature_prefix(buf) {
        return Err("not a PROXY v2 signature".to_owned());
    }
    if buf.len() < 16 {
        return Ok(V2Preamble::Incomplete);
    }
    let ver_cmd = buf[12];
    if ver_cmd >> 4 != 0x2 {
        return Err(format!("unsupported PROXY version {:#x}", ver_cmd >> 4));
    }
    let cmd = ver_cmd & 0x0F;
    if cmd > 0x1 {
        return Err(format!("unknown PROXY v2 command {cmd:#x}"));
    }
    let fam = buf[13] >> 4;
    let len = usize::from(u16::from_be_bytes([buf[14], buf[15]]));
    let total = 16 + len;
    if buf.len() < total {
        return Ok(V2Preamble::Incomplete);
    }
    // LOCAL (health check) or UNSPEC: the proxy speaks for itself.
    if cmd == 0x0 || fam == 0x0 {
        return Ok(V2Preamble::Complete {
            source: None,
            consumed: total,
        });
    }
    let payload = &buf[16..total];
    let source = match fam {
        // INET: src4 dst4 srcport dstport.
        0x1 => {
            if payload.len() < 12 {
                return Err("PROXY v2 INET payload too short".to_owned());
            }
            let octets: [u8; 4] = payload[0..4].try_into().unwrap_or_default();
            Some(IpAddr::from(octets))
        }
        // INET6: src16 dst16 srcport dstport.
        0x2 => {
            if payload.len() < 36 {
                return Err("PROXY v2 INET6 payload too short".to_owned());
            }
            let octets: [u8; 16] = payload[0..16].try_into().unwrap_or_default();
            Some(IpAddr::from(octets).to_canonical())
        }
        // AF_UNIX or reserved: no IP to key on.
        _ => None,
    };
    Ok(V2Preamble::Complete {
        source,
        consumed: total,
    })
}

/// Build a v2 `PROXY` preamble for an IPv4/IPv6 source (tests and the
/// conformance corpus; a real deployment's proxy writes these).
#[must_use]
pub fn encode_v2_preamble(source: IpAddr, source_port: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(52);
    out.extend_from_slice(&V2_SIG);
    out.push(0x21); // version 2, command PROXY
    match source {
        IpAddr::V4(src) => {
            out.push(0x11); // INET / STREAM
            out.extend_from_slice(&12u16.to_be_bytes());
            out.extend_from_slice(&src.octets());
            out.extend_from_slice(&[0, 0, 0, 0]); // dst addr (unused here)
            out.extend_from_slice(&source_port.to_be_bytes());
            out.extend_from_slice(&[0, 0]); // dst port
        }
        IpAddr::V6(src) => {
            out.push(0x21); // INET6 / STREAM
            out.extend_from_slice(&36u16.to_be_bytes());
            out.extend_from_slice(&src.octets());
            out.extend_from_slice(&[0u8; 16]);
            out.extend_from_slice(&source_port.to_be_bytes());
            out.extend_from_slice(&[0, 0]);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn trusted(entries: &[&str]) -> IpSet {
        IpSet::parse(&entries.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>()).unwrap()
    }

    #[test]
    fn an_untrusted_peer_never_gets_its_header_honored() {
        let t = trusted(&["10.0.0.1"]);
        // Spoofed XFF from a random client: ignored, peer wins.
        assert_eq!(
            resolve_forwarded_for(ip("203.0.113.9"), Some("1.2.3.4"), &t).unwrap(),
            ip("203.0.113.9")
        );
        // Feature off: even the trusted-looking peer is just a peer.
        assert_eq!(
            resolve_forwarded_for(ip("10.0.0.1"), Some("1.2.3.4"), &IpSet::default()).unwrap(),
            ip("10.0.0.1")
        );
    }

    #[test]
    fn rightmost_untrusted_entry_wins() {
        let t = trusted(&["10.0.0.0/8"]);
        // client → evil header value → proxy chain. The rightmost entry not
        // ours (198.51.100.7) is the client; the forged 1.1.1.1 to its left
        // is never consulted.
        assert_eq!(
            resolve_forwarded_for(ip("10.0.0.1"), Some("1.1.1.1, 198.51.100.7, 10.0.0.2"), &t)
                .unwrap(),
            ip("198.51.100.7")
        );
        // No inner hops: the single entry is the client.
        assert_eq!(
            resolve_forwarded_for(ip("10.0.0.1"), Some("198.51.100.7"), &t).unwrap(),
            ip("198.51.100.7")
        );
    }

    #[test]
    fn an_all_trusted_chain_falls_back_to_the_leftmost() {
        let t = trusted(&["10.0.0.0/8"]);
        assert_eq!(
            resolve_forwarded_for(ip("10.0.0.1"), Some("10.9.9.9, 10.0.0.2"), &t).unwrap(),
            ip("10.9.9.9")
        );
    }

    #[test]
    fn a_trusted_peer_without_the_header_is_itself_the_client() {
        let t = trusted(&["10.0.0.1"]);
        assert_eq!(
            resolve_forwarded_for(ip("10.0.0.1"), None, &t).unwrap(),
            ip("10.0.0.1")
        );
    }

    #[test]
    fn malformed_entries_from_a_trusted_proxy_are_an_error() {
        let t = trusted(&["10.0.0.1"]);
        for bad in ["not-an-ip", "", "198.51.100.7, garbage"] {
            resolve_forwarded_for(ip("10.0.0.1"), Some(bad), &t).unwrap_err();
        }
    }

    #[test]
    fn entries_with_ports_and_brackets_parse() {
        let t = trusted(&["10.0.0.1"]);
        assert_eq!(
            resolve_forwarded_for(ip("10.0.0.1"), Some("198.51.100.7:4711"), &t).unwrap(),
            ip("198.51.100.7")
        );
        assert_eq!(
            resolve_forwarded_for(ip("10.0.0.1"), Some("[2001:db8::7]:443"), &t).unwrap(),
            ip("2001:db8::7")
        );
        assert_eq!(
            resolve_forwarded_for(ip("10.0.0.1"), Some(" 2001:db8::7 "), &t).unwrap(),
            ip("2001:db8::7")
        );
    }

    #[test]
    fn v2_preamble_roundtrips_v4_and_v6() {
        for (src, port) in [(ip("203.0.113.9"), 4711u16), (ip("2001:db8::9"), 443)] {
            let bytes = encode_v2_preamble(src, port);
            match parse_v2_preamble(&bytes).unwrap() {
                V2Preamble::Complete { source, consumed } => {
                    assert_eq!(source, Some(src));
                    assert_eq!(consumed, bytes.len());
                }
                V2Preamble::Incomplete => panic!("complete preamble parsed as incomplete"),
            }
            // Trailing frame bytes after the preamble are not consumed.
            let mut with_tail = bytes.clone();
            with_tail.extend_from_slice(b"tail");
            match parse_v2_preamble(&with_tail).unwrap() {
                V2Preamble::Complete { consumed, .. } => assert_eq!(consumed, bytes.len()),
                V2Preamble::Incomplete => panic!("complete preamble parsed as incomplete"),
            }
        }
    }

    #[test]
    fn a_partial_preamble_asks_for_more_bytes() {
        let bytes = encode_v2_preamble(ip("203.0.113.9"), 4711);
        for cut in [0, 1, 11, 12, 15, 16, bytes.len() - 1] {
            assert_eq!(
                parse_v2_preamble(&bytes[..cut]).unwrap(),
                V2Preamble::Incomplete,
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn local_command_and_unspec_family_carry_no_source() {
        let mut local = encode_v2_preamble(ip("203.0.113.9"), 1);
        local[12] = 0x20; // version 2, command LOCAL
        match parse_v2_preamble(&local).unwrap() {
            V2Preamble::Complete { source, .. } => assert_eq!(source, None),
            V2Preamble::Incomplete => panic!("unexpected incomplete"),
        }

        let mut unspec = encode_v2_preamble(ip("203.0.113.9"), 1);
        unspec[13] = 0x00; // UNSPEC
        match parse_v2_preamble(&unspec).unwrap() {
            V2Preamble::Complete { source, .. } => assert_eq!(source, None),
            V2Preamble::Incomplete => panic!("unexpected incomplete"),
        }
    }

    #[test]
    fn garbage_and_wrong_versions_are_errors() {
        // An ordinary FluxRPC frame does not look like a preamble.
        assert!(parse_v2_preamble(b"\x00\x00\x00\x04ABCD").is_err());
        // Signature but a v1-style or wrong version nibble.
        let mut bad_ver = encode_v2_preamble(ip("203.0.113.9"), 1);
        bad_ver[12] = 0x11;
        assert!(parse_v2_preamble(&bad_ver).is_err());
        // Unknown command.
        let mut bad_cmd = encode_v2_preamble(ip("203.0.113.9"), 1);
        bad_cmd[12] = 0x2F;
        assert!(parse_v2_preamble(&bad_cmd).is_err());
        // Declared INET but payload too short for it.
        let mut short = encode_v2_preamble(ip("203.0.113.9"), 1);
        short[15] = 4; // truncate the declared length
        let short = &short[..20];
        assert!(parse_v2_preamble(short).is_err());
    }

    #[test]
    fn signature_prefix_detection_handles_partial_buffers() {
        assert!(is_v2_signature_prefix(&[]));
        assert!(is_v2_signature_prefix(&V2_SIG[..5]));
        assert!(is_v2_signature_prefix(&V2_SIG));
        assert!(!is_v2_signature_prefix(b"GET /rpc"));
        assert!(!is_v2_signature_prefix(&[0x0D, 0x0A, 0x0D, 0x0B]));
    }
}
