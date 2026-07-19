# Proposal: phase6_ddos-overload-resilience

## Why
Fluxum's deployment posture is a **directly exposed port** (no mandatory proxy/CDN in front), so "delegate DDoS to the proxy" (SPEC-026 §5 non-goal) is not a safe assumption. The current defenses reject abusive peers correctly but were not designed to *survive* a large distributed flood: `ConnGuard` allocates one `IpState` per distinct source IP (a many-IP flood grows the map without bound → the defense itself becomes the OOM vector), there is no admission control under CPU/memory pressure (pre-auth work competes with established clients), and there is no documented OS/socket hardening baseline (SYN cookies, backlog, conntrack, ulimits). The invariant to establish: under flood, established authenticated clients keep working, memory stays bounded, and recovery is immediate when the flood stops. True volumetric (link-saturation) attacks remain upstream/provider territory — stated honestly in docs — but nothing below the link should destabilize the process.

## What Changes
Overload resilience for the direct-exposure threat model, layered under the existing guard:
- **Bounded guard memory**: cap on tracked `IpState` entries with pressure eviction that never reclaims an entry holding live connections or an armed failed-auth streak (preserves SEC-031); eviction is itself counted.
- **Cheap-reject audit**: every pre-auth rejection path (TCP + HTTP) closes with zero allocation and zero response bytes where protocol allows — no amplification, no per-reject session state.
- **Admission control / brownout**: a load signal (conn count, guard pressure, memory budget headroom) gates the accept loop — under pressure, new pre-auth connections are shed first while established sessions are untouched; state transitions logged + `fluxum_overload_state` gauge.
- **Socket/listener hardening knobs**: configurable accept backlog, TCP keepalive, pre-auth and post-auth idle/read timeouts, `TCP_DEFER_ACCEPT` where the platform supports it.
- **Deployment hardening guide**: OS baseline for exposed ports (SYN cookies, `somaxconn`, conntrack sizing, fd ulimits, nftables/ipset rate rules that mirror the in-process bans) + honest scope statement on volumetric attacks.
- SPEC-026 §5 non-goal rewritten to match the direct-exposure posture; new SEC-04x requirement block.

## Impact
- DAG task: new (phase 6 hardening; additive)
- Affected specs: SPEC-026 (§4/§5, new SEC-04x), SPEC-012 (overload metrics)
- PRD requirements: FR-147 (extends); NFR resilience criteria
- Affected code: crates/fluxum-server/src/connguard.rs, crates/fluxum-server/src/tcp.rs, crates/fluxum-server/src/http.rs, crates/fluxum-core/src/config, crates/fluxum-core/src/metrics.rs; docs/ (deployment hardening guide)
- Depends on: none (composes with phase6_ip-blocklist-global-caps and phase6_proxy-aware-client-ip; none blocks another)
- Breaking change: NO (new knobs default to current behavior)
- User benefit: a directly exposed Fluxum port degrades gracefully under flood — bounded memory, established clients prioritized, instant recovery — instead of being destabilized
