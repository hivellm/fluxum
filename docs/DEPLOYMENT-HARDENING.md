# Deployment hardening for a directly exposed Fluxum port

Fluxum's production posture is a **directly exposed port** — no mandatory
reverse proxy or CDN in front. The in-process defenses (SPEC-026 §4:
per-IP caps, failed-auth backoff, blocklist/allowlist, global connection
ceiling, bounded guard memory, admission control) keep the *process* stable
under abuse, but the operating system in front of that process has its own
knobs, and an exposed port deserves all of them.

**Honest scope.** A true volumetric attack — enough packets to saturate the
network link itself — is won or lost upstream, at your hosting provider or
transit layer, before a single byte reaches this machine. Nothing on this
page (or in any userspace process) absorbs a saturated NIC. What this page
*does* ensure: everything below link saturation — SYN floods, connection
floods, distinct-IP swarms, slow-drip handshakes — degrades the service
gracefully instead of destabilizing it, and recovery is immediate when the
flood stops. For volumetric protection, use a provider that filters at the
edge (most clouds do for common patterns) or front Fluxum with an anycast
scrubbing service for the exposed ports.

## 1. Fluxum's own knobs (recap)

All under `server:` in `config.yml` — see SPEC-026 §4 for semantics:

```yaml
server:
  connection_limits:
    max_conns_per_ip: 1024        # per-IP concurrency (SEC-030)
    accept_rate_per_sec: 512      # per-IP accept rate (SEC-030)
    handshake_timeout_secs: 10    # slowloris budget (SEC-031)
    handshake_max_bytes: 65536    # pre-auth frame cap (SEC-031)
    failed_auth_threshold: 10     # brute-force backoff (SEC-031)
    blocklist: []                 # bans, IP/CIDR (SEC-033)
    allowlist: []                 # non-empty = only these (SEC-033)
    max_total_conns: 20000        # global ceiling (SEC-034) — SET THIS
    max_tracked_ips: 100000       # guard memory bound (SEC-040)
    overload_shed_fraction: 0.90  # admission control (SEC-041)
    overload_shed_all_fraction: 0.98
  accept_backlog: 4096            # listen(2) backlog (SEC-042)
  tcp_keepalive_secs: 60          # reap dead peers (SEC-042)
  tcp_defer_accept_secs: 5        # Linux: data-before-wakeup (SEC-042)
```

`max_total_conns` defaults to uncapped — on an exposed port, set it to what
your file-descriptor budget and memory actually support; it is also the
denominator the SEC-041 admission control sheds against. Runtime bans need
no restart: `POST /bans {"entry": "203.0.113.0/24", "ttl_secs": 3600}`.

## 2. Kernel baseline (Linux)

`/etc/sysctl.d/99-fluxum.conf`:

```conf
# SYN-flood survival: SYN cookies kick in when the SYN backlog fills, so
# half-open floods cost the kernel nothing to shrug off.
net.ipv4.tcp_syncookies = 1
net.ipv4.tcp_max_syn_backlog = 8192

# The accept-queue ceiling; server.accept_backlog is clamped to this.
net.core.somaxconn = 8192

# Recycle TIME_WAIT ports faster under connect/disconnect churn.
net.ipv4.tcp_fin_timeout = 15

# Keepalive floor for sockets Fluxum did not configure itself.
net.ipv4.tcp_keepalive_time = 300
```

Apply with `sysctl --system`.

**Connection tracking.** If the host runs a stateful firewall (nftables/
iptables with conntrack, the default on most distros), size the table for
your flood ceiling or the *firewall* becomes the bottleneck:

```conf
net.netfilter.nf_conntrack_max = 262144
```

and consider `NOTRACK` for the Fluxum ports if you rely on Fluxum's own
limits instead of stateful rules (advanced; see nftables docs).

**File descriptors.** Every connection is an fd. In the systemd unit:

```ini
[Service]
LimitNOFILE=131072
```

Keep `LimitNOFILE` comfortably above `max_total_conns` (each connection
costs one fd, plus files, plus headroom).

## 3. Firewall rate rules that mirror the in-process bans

The in-process guard refuses abusive peers cheaply — but a kernel-level
drop is cheaper still (no accept, no wakeup). These nftables rules mirror
SEC-030/033 one layer down. `/etc/nftables.conf` sketch:

```nft
table inet fluxum {
    # Mirrors the SEC-033 blocklist: fed by hand or by tooling that reads
    # GET /bans and writes the set. TTL entries expire on their own.
    set banned4 { type ipv4_addr; flags interval, timeout; }
    set banned6 { type ipv6_addr; flags interval, timeout; }

    # Per-source connection-rate mirror of SEC-030 accept_rate_per_sec.
    set connlimit4 { type ipv4_addr; flags dynamic; timeout 1m; }

    chain inbound {
        type filter hook input priority 0; policy accept;

        ip  saddr @banned4 drop
        ip6 saddr @banned6 drop

        # New connections to the Fluxum ports, rate-limited per source:
        # anything past 60 new conns/min per address is dropped in-kernel.
        tcp dport { 15800, 15801 } ct state new \
            add @connlimit4 { ip saddr limit rate over 60/minute } drop

        # Optional hard per-source concurrency mirror of max_conns_per_ip.
        tcp dport { 15800, 15801 } ct count over 1024 drop
    }
}
```

To push a runtime Fluxum ban into the kernel set:

```sh
nft add element inet fluxum banned4 '{ 203.0.113.9 timeout 1h }'
```

(A small cron/timer that diffs `GET /bans` into the nft set keeps the two
layers in sync; the in-process ban works on its own regardless.)

## 4. What to watch

- `fluxum_conn_rejected_total{reason}` — which defense is firing
  (`blocked`, `global_cap`, `overload`, `accept_rate`, `failed_auth`, …).
- `fluxum_overload_state` — 0 normal, 1 shedding pre-auth, 2 shedding all
  new; alert on any sustained non-zero.
- `fluxum_connguard_tracked_ips` / `fluxum_connguard_evictions_total` — a
  distinct-IP flood shows up here first.
- `/health` stays lock-free and served even mid-shed — a probe that stops
  answering means the problem is below Fluxum (link, kernel, fd budget).

The admin surface (`/health`, `/metrics`, `/bans`) is deliberately never
gated by admission control: it is the toolset an operator fights a flood
with.

## 5. Checklist

- [ ] `max_total_conns` set to a deliberate number; `LimitNOFILE` above it
- [ ] SYN cookies + backlog sysctls applied; `somaxconn` ≥ `accept_backlog`
- [ ] conntrack sized (or bypassed) for the flood ceiling
- [ ] nftables ban/rate sets in place, mirroring the in-process rules
- [ ] alerts on `fluxum_overload_state != 0` and `conn_rejected` rates
- [ ] provider/upstream story for true volumetric attacks understood
