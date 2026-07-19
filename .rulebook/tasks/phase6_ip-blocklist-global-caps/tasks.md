## 1. Implementation
- [ ] 1.1 Config: `server.connection_limits.blocklist` / `allowlist` (IP + CIDR, v4/v6) and `max_total_conns` (0 = uncapped); validate entries at load, register the new keys in the hot-reload allowlist
- [ ] 1.2 ConnGuard: check order blocklist → allowlist (when non-empty, allowlist is exclusive) → global ceiling → existing backoff/rate/cap checks; runtime ban table with optional TTL merged with the static lists
- [ ] 1.3 Admin API: `POST /admin/bans` (ip_or_cidr, optional ttl), `DELETE /admin/bans/{entry}`, `GET /admin/bans` listing static + runtime entries with remaining TTL
- [ ] 1.4 Metrics: `fluxum_conn_rejected_total{reason}` gains `blocked` and `global_cap`; both transports record them
- [ ] 1.5 Spec: extend SPEC-026 §4 with the new SEC-03x requirements and config block; note the allowlist-exclusive semantics
- [ ] 1.6 Verification: integration test — banned IP refused on TCP and HTTP before auth, unban readmits, TTL expiry readmits, global ceiling refuses connection N+1 across distinct IPs, hot-reload of the static lists applies without restart

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
