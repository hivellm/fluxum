## 1. Implementation
- [x] 1.1 Config: `server.connection_limits.blocklist` / `allowlist` (IP + CIDR, v4/v6) and `max_total_conns` (0 = uncapped); validate entries at load, register the new keys in the hot-reload allowlist
- [x] 1.2 ConnGuard: check order blocklist → allowlist (when non-empty, allowlist is exclusive) → global ceiling → existing backoff/rate/cap checks; runtime ban table with optional TTL merged with the static lists
- [x] 1.3 Admin API: `POST /bans` (entry, optional ttl_secs), `DELETE /bans/{entry}`, `GET /bans` listing static + runtime entries with remaining TTL (mounted at the admin root, matching `/health`, `/drain` — the surface has no `/admin` prefix)
- [x] 1.4 Metrics: `fluxum_conn_rejected_total{reason}` gains `blocked` and `global_cap`; both transports record them
- [x] 1.5 Spec: extend SPEC-026 §4 with the new SEC-03x requirements and config block; note the allowlist-exclusive semantics
- [x] 1.6 Verification: integration test — banned IP refused on TCP and HTTP before auth, unban readmits, TTL expiry readmits, global ceiling refuses connection N+1 across distinct IPs, hot-reload of the static lists applies without restart

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
