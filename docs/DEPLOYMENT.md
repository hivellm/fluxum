# Deployment guide

How to install, run, upgrade, and size a single-node Fluxum (ROADMAP M7).
Security posture for a directly exposed port lives in
[DEPLOYMENT-HARDENING.md](DEPLOYMENT-HARDENING.md); this page is the
operational path. Everything below uses the reference binary
`fluxum-server`; an application that links `fluxum-server` as a library gets
the same behavior (config loading, signals, drain) through `boot::serve`.

## 1. The shape of a deployment

Fluxum is **one static binary**. The application's schema and reducers are
compiled into it (`#[fluxum::table]` / `#[fluxum::reducer]` register at link
time), so "deploying an app" and "deploying the database" are the same act:
ship a binary, point it at a config file and a data directory.

| Port | Listener | Serves |
|------|----------|--------|
| **15800** | HTTP | `/rpc` (FluxRPC over Streamable HTTP), admin API (`/health`, `/metrics`, `/schema`, `/query`, …), the [admin console](specs/SPEC-024-developer-experience-tooling.md) at `/console`, `/logs` |
| **15801** | TCP | FluxRPC binary protocol |

Both bind `server.tcp_host` (default `127.0.0.1` — loopback only until you
deliberately expose it; see [§6 TLS](#6-tls-and-exposure)).

## 2. Install

Build a release binary (any machine with the Rust toolchain; the SIMD tier
is selected at **runtime**, so one x86-64 binary serves AVX-512, AVX2, and
scalar hosts alike — SPEC-016):

```sh
cargo build --release -p fluxum-server
# → target/release/fluxum-server
```

Then either follow [§3 systemd](#3-systemd) or [§4 Docker](#4-docker).
Verify any install with:

```sh
curl -fsS http://127.0.0.1:15800/health
```

`/health` answers `200` with shard state, last tx id, connection count, and
the effective (probe-derived) configuration — the first stop for "what is
this node actually running with".

## 3. systemd

The unit file ships at [`deploy/fluxum.service`](../deploy/fluxum.service),
hardened (dynamic user, read-only system, no capabilities, syscall filter)
and wired for the server's signal contract: **SIGTERM** drains gracefully
(SPEC-025 OPS-030 — stop admitting, finish in-flight, final checkpoint),
**SIGHUP** hot-reloads the reloadable config keys (OPS-040).

```sh
install -m 755 target/release/fluxum-server /usr/local/bin/fluxum-server
install -m 644 deploy/fluxum.service /etc/systemd/system/fluxum.service
install -d /etc/fluxum
# 644, not 640: the unit runs under DynamicUser (an ephemeral UID that is
# in no group), and the config carries no secrets — those live in the env
# file below, mode 600, read by systemd itself.
install -m 644 config/config.example.yml /etc/fluxum/config.yml   # then edit
printf 'FLUXUM_AUTH_SECRET=%s\n' "$(openssl rand -hex 32)" > /etc/fluxum/fluxum.env
chmod 600 /etc/fluxum/fluxum.env
systemctl daemon-reload
systemctl enable --now fluxum
curl -fsS http://127.0.0.1:15800/health
```

Day-2 operations:

```sh
systemctl reload fluxum        # SIGHUP: apply reloadable keys, no restart
journalctl -u fluxum -f        # JSON log lines (logging.format: json)
systemctl restart fluxum       # SIGTERM drain, then a fresh boot + replay
```

State lives in `/var/lib/fluxum` (systemd's `StateDirectory`); the unit's
`WorkingDirectory` points there so the config's relative `./data` default
resolves to `/var/lib/fluxum/data` — see [§7 data directory](#7-data-directory-layout).

## 4. Docker

The image builds the release binary in a build stage and runs it as a
non-root user ([`deploy/Dockerfile`](../deploy/Dockerfile)):

```sh
docker build -f deploy/Dockerfile -t fluxum:latest .
docker run -d --name fluxum \
  -p 15800:15800 -p 15801:15801 \
  -e FLUXUM_AUTH_SECRET="$(openssl rand -hex 32)" \
  -e FLUXUM_SERVER_ALLOW_PLAINTEXT=true \
  -v fluxum-data:/var/lib/fluxum \
  --cpus 1 --memory 512m \
  fluxum:latest
curl -fsS http://127.0.0.1:15800/health
```

Or with compose: [`deploy/docker-compose.yml`](../deploy/docker-compose.yml)
(`docker compose -f deploy/docker-compose.yml up -d`).

**Container limits are honored** (FR-05): the boot-time hardware probe reads
cgroup v1/v2 CPU quota and memory limits, so `--cpus 1 --memory 512m` is
what the `auto` keys derive from — not the host's totals. `/health` shows
the derivation (`config.probe` and each derived value with its source). The
image's `HEALTHCHECK` polls `/health`, so `docker ps` reports readiness.

`FLUXUM_SERVER_ALLOW_PLAINTEXT=true` is required because a container binds
`0.0.0.0` and Fluxum refuses a non-loopback bind with real auth and no TLS
by default — read [§6](#6-tls-and-exposure) before exposing the ports beyond
a trusted network.

## 5. Configuration reference

The reference of record is
[`config/config.example.yml`](../config/config.example.yml): **every key**
with its built-in default and a one-line meaning. It cannot drift — a test
(`crates/fluxum-core/tests/config_example.rs`) fails the build if a `Config`
key is missing from the file.

The layering rule (SPEC-012 OBS-080), highest priority first:

1. **`FLUXUM_*` environment variable** — the key's path upper-cased and
   joined with `_`: `server.tcp_port` → `FLUXUM_SERVER_TCP_PORT`,
   `memory.budget` → `FLUXUM_MEMORY_BUDGET`.
2. **The config file** (`--config /etc/fluxum/config.yml`). Values of the
   exact form `${VAR}` are expanded from the environment (how
   `auth.secret: ${FLUXUM_AUTH_SECRET}` works).
3. **Profile defaults** (`profile: development` flips single-shard, auth
   `none`, pretty logs, open console).
4. **Built-in defaults** — what the example file documents.

Keys accepting `auto` (worker threads, shards, memory budget, fan-out
concurrency, commit-log buffer) derive from the hardware probe; an explicit
value always wins. `/health` reports every derived value **with its
source**, and after a reload, `/health`'s `reloadable` block shows what is
actually in force — how you confirm a change landed (OPS-040).

Hot-reloadable keys (SIGHUP or `POST /config/reload`; the full list is
`fluxum_core::config::RELOADABLE_KEYS`): logging level/format, slow-reducer
threshold, reducer admission/deadline/tx-cap, query bounds and rates,
subscription send buffer, connection-limit lists and ceilings, trusted
proxies, and the admin access policy. Listener addresses, ports, storage
paths, shard count, and auth provider are **frozen** — a reload naming them
is rejected with the offending keys listed, and the running config is
untouched (OPS-041).

## 6. TLS and exposure

Fluxum's production posture is a directly exposed port — no mandatory proxy
(SPEC-026). Three legal postures:

- **Loopback / same host** (default): `tcp_host: 127.0.0.1`, nothing to do.
- **Built-in TLS**: set `server.tls.cert` + `server.tls.key` (PEM); both
  listeners terminate TLS before the first byte (SEC-059). `/health` reports
  `"tls": true`.
- **Plaintext on a trusted link**: `server.allow_plaintext: true` — only
  where the link is encrypted below Fluxum (service mesh, VPN, compose
  network behind a TLS-terminating LB). Without it, a non-loopback bind with
  real auth and no TLS **refuses to boot** rather than leak bearer tokens.

Exposed ports also want the OS-level hardening pass:
[DEPLOYMENT-HARDENING.md](DEPLOYMENT-HARDENING.md) (sysctls, nftables
mirrors of the in-process limits, fd budgets) and a deliberate
`server.connection_limits.max_total_conns`.

## 7. Data directory layout

All durable state lives under `storage.data_dir` (default `./data`,
`/var/lib/fluxum/data` under the shipped unit/image):

```
data/
├── log/            # commit-log segments (storage.commit_log_dir) — the source of truth
├── checkpoints/    # periodic full checkpoints (storage.checkpoint_dir)
└── pages/          # cold-tier page files (storage.page_dir)
```

- **Boot** = load the latest checkpoint, replay the commit log after it
  (SPEC-002). Recovery time is bounded by `storage.checkpoint_interval_tx`.
- The directories may live on different volumes (each key is independent);
  the commit log is the fsync-latency-critical one.
- `archive/` (`replication.archive.dir`) holds byte-identical copies of
  truncated log segments (SPEC-014 REP-062) — the PITR source. Its
  `retention` window (default `7d`) IS the PITR window.

### Backup and point-in-time recovery

Hot backups take no lock and never stall writers (SPEC-014 REP-060):

```sh
fluxum backup create --out /backups/nightly --config /etc/fluxum/config.yml \
    --fresh-checkpoint --server 127.0.0.1:15800   # optional: checkpoint first
fluxum backup verify --from /backups/nightly       # CRCs + record decode + tx chain
```

Restore and PITR (`--to-timestamp`/`--to-tx-id`, inclusive; the boundary tx
and timestamp are reported):

```sh
fluxum backup restore --from /backups/nightly --data-dir /var/lib/fluxum/data
fluxum backup restore --from /backups/nightly --data-dir /var/lib/fluxum/data \
    --to-timestamp "2026-07-24T13:37:09Z" --archive-dir /var/lib/fluxum/data/archive
```

The next boot's normal recovery reconstructs the state. A PITR restore forks
history: the node boots with a raised fencing epoch (the `pitr.lineage`
marker) and must seed a new replica set rather than rejoin the old one
(REP-072). Schedule backups externally (cron / a systemd timer) per REP-065.

## 8. Upgrades

Deployment is a **fast binary restart** (there is no hot code swap —
ARCHITECTURE):

1. Build/pull the new binary or image.
2. **Preview the migration**: `fluxum-server --migrate-plan --config …`
   (or `FLUXUM_MIGRATE_PLAN=1`) prints the schema diff and the verdict
   without mutating anything (SPEC-024 DEV-041). Exit 0 = the new binary
   boots (additive changes auto-apply); exit 3 = it would refuse (a
   breaking change needs an explicit migration + version bump) — resolve
   before proceeding.
3. Restart onto the new binary: `systemctl restart fluxum` (SIGTERM drains:
   in-flight transactions finish, subscribers are cut cleanly, a final
   checkpoint lands) or roll the container.
4. Verify: `/health` `200`, and `GET /schema` reports the expected
   `schema_version`; clients reconnect and `Resume` their subscriptions
   (SPEC-021).

Rolling restarts across a replica set ride the same drain primitive
(OPS-030/031); orchestrator pre-stop hooks can call `POST /drain` and poll
`/health` for `shutting_down`.

## 9. Droplet profile (1 vCPU / 512 MB)

Fluxum's floor target is a 512 MB droplet (NFR-12). The `auto` derivations
land correctly there with **zero tuning**:

| Key | `auto` on 1 vCPU / 512 MB |
|-----|---------------------------|
| `runtime.worker_threads` | 1 |
| `sharding.shards` | 1 |
| `memory.budget` | 256 MiB (`max(128 MiB floor, 0.5 × 512 MiB)`) |
| `subscriptions.fanout_concurrency` | 2 |
| `storage.commit_log_write_buffer_bytes` | 512 KiB |

Guidance for that class of machine:

- Leave `memory.budget: auto`. The budget is enforced by eviction to the
  cold tier (SPEC-015), so a dataset larger than RAM degrades to page
  fetches instead of OOM. If the kernel OOM-killer ever fires, *lower*
  `memory.auto_fraction` (0.4) — something else on the box needs the rest.
- Set `server.connection_limits.max_total_conns` to a deliberate number
  (e.g. `1024`) — the default is uncapped, and 512 MB of RAM is the real
  ceiling; expectation ≈ a few thousand mostly idle subscribers, not tens of
  thousands.
- Keep `page_compression: lz4` (default): cheapest CPU per byte, and one
  vCPU is the scarce resource.
- Swap: a small swapfile (512 MB) absorbs allocator spikes but keep
  `vm.swappiness=10` — the hot tier must stay resident.
- Expect single-shard throughput in the tens of thousands of reducer
  calls/s on such a machine (the ≥100k/s M7 target is a full-size host);
  `fluxum_reducer_calls_total` and `/metrics` tell you where you actually
  are.

## 10. Verify an install (checklist)

```sh
curl -fsS http://127.0.0.1:15800/health          # 200, "status":"ok"
curl -fsS http://127.0.0.1:15800/metrics | head  # Prometheus text
curl -fsS http://127.0.0.1:15800/schema | head   # the module's tables
```

- `/health.config` shows the probe + every derived value and its source.
- The admin console is at `http://127.0.0.1:15800/console` (production
  profile: requires a `auth.server_peers` operator token — DEV-031).
- Logs are JSON on stdout/journal; `GET /logs?follow=1` streams them over
  the admin transport.
