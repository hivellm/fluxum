# Proposal: phase6_proxy-aware-client-ip

## Why
Every per-IP defense (SEC-030/031 caps, backoff, and the upcoming blocklist) keys on the socket peer address. The documented production posture is "run behind a reverse proxy" (ROADMAP: TLS deferred to a proxy) — but behind one, *every* connection carries the proxy's IP: the per-IP caps then throttle the proxy itself (self-inflicted outage) while real attackers become indistinguishable. Proxy awareness is what makes the whole abuse-protection layer function in the recommended deployment.

## What Changes
Trusted-proxy client-IP resolution shared by both transports:
- `server.trusted_proxies` config (IP/CIDR list, default empty = feature off, socket peer IP used as today).
- HTTP: honor `X-Forwarded-For` (rightmost-untrusted algorithm) only when the socket peer is a trusted proxy; never trust the header otherwise.
- TCP: accept PROXY protocol v2 preamble only from trusted proxy peers; a preamble from anyone else is a protocol error.
- The resolved client IP feeds ConnGuard, the blocklist, session identity logging, and `fluxum_conn_rejected_total` attribution.

## Impact
- DAG task: new (phase 6 hardening; additive)
- Affected specs: SPEC-026 (§4), SPEC-006 (transport preamble note)
- PRD requirements: FR-147 (extends)
- Affected code: crates/fluxum-core/src/config, crates/fluxum-server/src/tcp.rs, crates/fluxum-server/src/http.rs, crates/fluxum-server/src/connguard.rs call sites
- Depends on: none (pairs with phase6_ip-blocklist-global-caps but neither blocks the other)
- Breaking change: NO (off by default; behavior unchanged with an empty trusted list)
- User benefit: per-IP rate limits, backoff, and bans keep working when Fluxum runs behind the recommended reverse proxy / load balancer
