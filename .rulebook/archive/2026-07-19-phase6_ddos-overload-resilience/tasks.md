## 1. Implementation
- [x] 1.1 Bounded ConnGuard: `max_tracked_ips` config (default 100000, 0 = unbounded); pressure eviction skips entries with live conns or armed auth-failure streaks (SEC-031 preserved); `fluxum_connguard_tracked_ips` gauge + eviction counter; at full saturation newcomers are admitted untracked rather than growing the map
- [x] 1.2 Cheap-reject audit: every pre-auth rejection (blocklist, global cap, accept-rate, backoff, handshake budget, overload shed, spoofed preamble) on TCP closes with zero response bytes (the pre-auth oversized-frame 413 was removed — post-auth keeps it); HTTP exceptions documented in SEC-043 (400 for malformed XFF from a *trusted* proxy, 413 for pre-auth oversized POST — one status head, no amplification)
- [x] 1.3 Admission control: overload signal from conn count vs global cap and guard pressure (memory headroom is independently bounded by the SPEC-015 pager budget — no live RSS probe exists to consult, noted in SEC-041); states normal → shed-preauth → shed-all-new; TCP sheds at accept, HTTP sheds `/rpc` only (admin surface deliberately never gated, mirroring the drain design); `fluxum_overload_state` gauge + transition logs
- [x] 1.4 Socket knobs: `accept_backlog`, `tcp_keepalive_secs`, `tcp_defer_accept_secs` (Linux-gated via libc; logged+ignored elsewhere) in `sock.rs`, wired into both listeners, all config-defaulted to current behavior; pre/post-auth idle+read timeouts already existed as SEC-031 handshake budget + RPC-060 idle timeout
- [x] 1.5 Flood tests: 60-distinct-IP flood keeps the guard map under an 8-entry cap with evictions counted; under shed-preauth an established TCP session keeps getting responses and an HTTP session-bearing POST is served while pre-auth POSTs drop; flood stop → immediate recovery (instantaneous signal, no cool-down)
- [x] 1.6 Deployment hardening guide (docs/DEPLOYMENT-HARDENING.md): SYN cookies, somaxconn/backlog, conntrack sizing, fd ulimits, nftables ban/rate sets mirroring in-process bans (fed from GET /bans), honest volumetric scope statement; linked from README
- [x] 1.7 Spec: SPEC-026 §5 non-goal rewritten for the direct-exposure posture; SEC-040..043 added; SPEC-012 OBS-042 gains the new metrics

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
