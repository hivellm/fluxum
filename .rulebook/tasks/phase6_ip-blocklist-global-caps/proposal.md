# Proposal: phase6_ip-blocklist-global-caps

## Why
The pre-auth guard (SPEC-026 SEC-030/031, `fluxum_server::connguard`) is purely reactive and per-IP: it throttles churn and brute-force but gives an operator no way to *ban* a known-abusive address outright, no allowlist for locked-down deployments, and no global connection ceiling — a distributed flood from many IPs sails past every per-IP cap and exhausts sessions/memory.

## What Changes
Static + runtime IP blocklist/allowlist and a global connection backstop, layered in front of the existing `ConnGuard` checks:
- `server.connection_limits.blocklist` / `allowlist` config keys (IP and CIDR entries, IPv4/IPv6), hot-reloadable via the existing config-reload allowlist.
- Admin API endpoints to ban/unban an IP or CIDR at runtime with an optional TTL (runtime state only; static config is the durable path).
- `max_total_conns` global ceiling (0 = uncapped, permissive default) checked before per-IP state is touched.
- Rejections surface as `fluxum_conn_rejected_total{reason}` with new reasons `blocked` and `global_cap` (SEC-032 extension); SPEC-026 §4 updated with new SEC-03x requirement IDs.

## Impact
- DAG task: new (phase 6 hardening; additive, no DAG renumbering)
- Affected specs: SPEC-026 (security hardening §4), SPEC-025 (admin API surface)
- PRD requirements: FR-147 (extends)
- Affected code: crates/fluxum-core/src/config, crates/fluxum-server/src/connguard.rs, crates/fluxum-server/src/http.rs (admin API), crates/fluxum-core/src/metrics.rs
- Depends on: none (connguard and admin API already landed in phase 5)
- Breaking change: NO (all knobs default permissive/off)
- User benefit: operators can ban abusive sources immediately and bound total resource use under distributed floods
