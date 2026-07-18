## 1. Implementation
- [x] 1.1 Add a shared per-IP connection tracker (concurrent count + accept-rate token bucket) usable by both transports (SEC-030) — landed as crates/fluxum-server/src/connguard.rs (ConnGuard + ConnPermit RAII), held on ShardContext so TCP and HTTP share one per-IP view
- [x] 1.2 Enforce the per-IP concurrent-connection cap and accept-rate limit on the TCP accept path, rejecting excess before session setup (SEC-030; crates/fluxum-server/src/tcp.rs) — gated at accept; refused sockets dropped + metered
- [x] 1.3 Enforce the same caps on the HTTP transport accept/upgrade path (SEC-030; crates/fluxum-server/src/http.rs) — same shared guard gated at accept; permit held for the connection life
- [x] 1.4 Add a per-address failed-`Authenticate` throttle with exponential backoff that delays/refuses further attempts after a threshold (SEC-031) — in the guard (note_auth_failure/success), refusing the IP's next connection at accept; streak not reclaimed mid-count so a disconnect can't reset it
- [x] 1.5 Impose a bounded time and size budget on the handshake / `Authenticate` exchange and drop slow or oversized handshakes (SEC-031) — TCP: deadline-aware pre-auth read (time) + tighter pre-auth frame cap (size); HTTP: pre-auth POST body cap
- [x] 1.6 Emit `fluxum_conn_rejected_total{reason}` for each rejection class (conn-cap, accept-rate, failed-auth, handshake-budget) (SEC-032) — added to fluxum-core Metrics with every reason label emitted even at zero
- [x] 1.7 Expose configuration for the caps/thresholds with permissive, documented defaults (SEC-030, SEC-031) — server.connection_limits (ConnectionLimitsConfig) in fluxum-core config; 0 disables each limit; config.example.yml documents the defaults

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation — module + config doc comments, SPEC-026 config/implementation subsection + YAML example, config.example.yml block
- [x] 2.2 Write tests covering the new behavior — connguard unit tests (cap/rate/backoff/zeroed/GC) + tests/connection_abuse.rs real-socket integration suite (both transports, handshake budget, brute-force backoff, metric exposition)
- [x] 2.3 Run tests and confirm they pass — full fluxum-core (454 lib) + fluxum-server suites green; clippy + fmt clean
