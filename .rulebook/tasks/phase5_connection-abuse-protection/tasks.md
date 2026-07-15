## 1. Implementation
- [ ] 1.1 Add a shared per-IP connection tracker (concurrent count + accept-rate token bucket) usable by both transports (SEC-030; crates/fluxum-server/src/session.rs)
- [ ] 1.2 Enforce the per-IP concurrent-connection cap and accept-rate limit on the TCP accept path, rejecting excess before session setup (SEC-030; crates/fluxum-server/src/tcp.rs)
- [ ] 1.3 Enforce the same caps on the HTTP transport accept/upgrade path (SEC-030; crates/fluxum-server/src/http.rs)
- [ ] 1.4 Add a per-address failed-`Authenticate` throttle with exponential backoff that delays/refuses further attempts after a threshold (SEC-031; crates/fluxum-core/src/auth)
- [ ] 1.5 Impose a bounded time and size budget on the handshake / `Authenticate` exchange and drop slow or oversized handshakes (SEC-031; crates/fluxum-server/src/session.rs)
- [ ] 1.6 Emit `fluxum_conn_rejected_total{reason}` for each rejection class (conn-cap, accept-rate, failed-auth, handshake-budget) (SEC-032; crates/fluxum-server/src/session.rs)
- [ ] 1.7 Expose configuration for the caps/thresholds with permissive, documented defaults (SEC-030, SEC-031; crates/fluxum-server/src/session.rs)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
