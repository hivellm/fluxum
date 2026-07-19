## 1. Implementation
- [ ] 1.1 Bounded ConnGuard: `max_tracked_ips` config (default generous, 0 = unbounded); pressure eviction skips entries with live conns or armed auth-failure streaks (SEC-031 preserved); `fluxum_connguard_tracked_ips` gauge + eviction counter
- [ ] 1.2 Cheap-reject audit: verify/refactor every pre-auth rejection (blocklist, global cap, accept-rate, backoff, handshake budget) on TCP and HTTP to close without allocation or response bytes where the protocol allows; document the one-line exceptions (HTTP status codes)
- [ ] 1.3 Admission control: overload signal from conn count vs global cap, guard pressure, and memory-budget headroom; states normal → shed-preauth → shed-all-new; accept loop consults it before ConnGuard; `fluxum_overload_state` gauge + transition logs
- [ ] 1.4 Socket knobs: accept backlog, TCP keepalive, pre/post-auth idle and read timeouts, TCP_DEFER_ACCEPT (platform-gated); wired into both listeners, all config-defaulted to current behavior
- [ ] 1.5 Flood tests: many-distinct-IP simulated flood keeps guard memory under the cap and evicts sanely; under shed-preauth an established session's reducer calls and TxUpdates keep flowing; flood stop → immediate recovery to normal
- [ ] 1.6 Deployment hardening guide (docs/): SYN cookies, somaxconn/backlog, conntrack sizing, fd ulimits, example nftables/ipset rules mirroring in-process bans, provider/upstream guidance for volumetric attacks; linked from README + deployment guide task
- [ ] 1.7 Spec: SPEC-026 §5 non-goal rewritten for the direct-exposure posture; new SEC-04x requirements for 1.1–1.4; SPEC-012 gains the new metrics

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
