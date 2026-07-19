## 1. Implementation
- [x] 1.1 Config: `server.trusted_proxies` (IP + CIDR, v4/v6, default empty = disabled); validate at load, hot-reloadable
- [x] 1.2 Shared resolver: given socket peer + transport metadata, produce the effective client IP; peer not in trusted_proxies → peer IP, ignore any forwarding metadata
- [x] 1.3 HTTP: parse `X-Forwarded-For` with the rightmost-untrusted rule when the peer is trusted; malformed header from a trusted proxy → reject the request, count it
- [x] 1.4 TCP: PROXY protocol v2 preamble accepted only from trusted peers (counted against the handshake byte/time budget); preamble from an untrusted peer or malformed preamble → close, count it
- [x] 1.5 Wire the resolved IP through ConnGuard try_accept/note_auth_* on both transports and into connection logs/metrics labels
- [x] 1.6 Spec: extend SPEC-026 §4 (new SEC-03x IDs, config block, trust rules) and add the SPEC-006 transport preamble note
- [x] 1.7 Verification: integration tests — forwarded IP honored only from a trusted proxy, spoofed XFF/preamble from untrusted peer ignored/rejected, per-IP caps bite the forwarded IP not the proxy IP, disabled feature is byte-identical to today

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
