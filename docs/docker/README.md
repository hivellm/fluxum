# Fluxum

**General-purpose realtime database: live query subscriptions over a tiered MVCC engine, in one 12 MB image.**

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://github.com/hivellm/fluxum/blob/main/LICENSE)
[![GitHub](https://img.shields.io/badge/GitHub-hivellm%2Ffluxum-blue?logo=github)](https://github.com/hivellm/fluxum)

Fluxum is a realtime database written in Rust: application logic lives
in the server as compiled modules (tables + reducers), clients hold
**live SQL subscriptions** that push row diffs on every commit, and an
in-memory MVCC engine spills to a budgeted on-disk cold tier so the
dataset outgrows RAM without the process outgrowing its container.
Single binary; ships a built-in admin console, replication, hot
backup/PITR, sharding, and a read-only Postgres wire endpoint.

## Quick start

```bash
# Pull + run (plaintext opt-in is for links encrypted BELOW Fluxum —
# compose networks, a mesh, a TLS-terminating LB; see TLS notes below)
docker run -d \
  --name fluxum \
  -p 15800:15800 \
  -p 15801:15801 \
  -v fluxum-data:/var/lib/fluxum \
  -e FLUXUM_AUTH_SECRET="$(openssl rand -hex 32)" \
  -e FLUXUM_SERVER_ALLOW_PLAINTEXT=true \
  hivehub/fluxum:latest

# Smoke-test
curl http://localhost:15800/health
```

Then open the **admin console** at `http://localhost:15800/console` —
data browser with row editing, SQL console with EXPLAIN and a live
mode, reducer invocation, sessions, metrics dashboards and ops actions,
all served from the binary itself (no CDN, works air-gapped).

```bash
# One-off SQL over the admin API
curl -X POST http://localhost:15800/query \
  -d '{"sql": "SELECT * FROM ChatMessage LIMIT 10"}'

# Call a reducer
curl -X POST http://localhost:15800/reducer/send_chat -d '[1, "hello"]'
```

## Supported tags

| Tag | Contents |
|---|---|
| `latest`, `0.1.0-alpha` | First public cut: the full engine (MVCC single-writer commits, tiered storage under a memory budget, live subscriptions with resume), five first-party SDKs (Rust / TypeScript / Python / Go / C#), semi-sync replication with elections + fencing, hot backup / PITR with S3 archival, multi-shard hosting, the integrated admin console, full-text search (BM25), and the read-only pgwire endpoint. |

Pin a specific tag for production. `latest` floats forward on every release.

## Image layout

- **Base**: `FROM scratch` — zero OS packages, **zero CVEs by
  construction**. There is no shell: `docker exec ... sh` does not
  work; debug via `docker logs` and the HTTP admin API.
- **Binary**: `/usr/local/bin/fluxum-server` (fully static musl build;
  the only executable in the image).
- **User**: `fluxum` (uid 1000), non-root.
- **Data directory**: `/var/lib/fluxum` — the declared volume; all
  durable state (commit log, checkpoints, cold-tier pages, archive)
  lives under it.
- **Reference config**: `/etc/fluxum/config.example.yml` (annotated;
  extract with `docker cp` — the container runs on built-in defaults
  plus `FLUXUM_*` environment overrides).
- **Image size**: ~12 MB. Multi-arch: `linux/amd64` + `linux/arm64`,
  published with SBOM + provenance attestations.

## Ports

| Port | Purpose | Notes |
|---|---|---|
| `15800` | HTTP — admin JSON API, `/rpc` (FluxRPC over Streamable HTTP, what the browser SDK uses), the `/console` admin UI, `/metrics`, `/health` | Primary entry point. |
| `15801` | FluxRPC binary TCP (`fluxum://host:15801`) | Native SDK transport. Leave unpublished for HTTP-only deployments. |
| `15802` | Replication listener | Only when `replication.peers` is configured; not exposed by default. |

## Environment variables

Every `config.yml` key maps to an environment override as
`FLUXUM_<SECTION>_<KEY>` (env outranks file). The ones most deployments
touch:

| Variable | Default | Effect |
|---|---|---|
| `FLUXUM_AUTH_SECRET` | _(unset)_ | The token-auth secret. **Required** outside the `development` profile. |
| `FLUXUM_SERVER_ALLOW_PLAINTEXT` | `false` | A non-loopback bind with real auth and no TLS is refused by default; set `true` only when the link below Fluxum is already encrypted (compose network, mesh, TLS-terminating LB). |
| `FLUXUM_PROFILE` | _(production posture)_ | `development` opens the console without an operator token and relaxes auth — never in production. |
| `FLUXUM_STORAGE_DATA_DIR` | `/var/lib/fluxum/data` | Root for commit log / checkpoints / pages / archive (each also settable individually). |
| `FLUXUM_MEMORY_BUDGET` | `auto` | The tiered-storage RAM ceiling; `auto` derives from the **cgroup** limit, so `--memory 512m` is what the derivation sees. |
| `FLUXUM_SHARDING_SHARDS` | `auto` | Explicit `N` assembles N fully-independent shards behind the coordinator. |
| `FLUXUM_SERVER_HTTP_PORT` / `FLUXUM_SERVER_TCP_PORT` | `15800` / `15801` | Listener ports. |
| `FLUXUM_LOGGING_LEVEL` | `info` | `trace`..`error`; hot-reloadable via `POST /config/reload`. |
| `TZ` | `UTC` | Container timezone. |

**Container limits are honored** (FR-05): the boot-time hardware probe
reads cgroup v1/v2 CPU and memory limits, so `--cpus` / `--memory` are
what the `auto` derivations (worker threads, shards, memory budget) see
— not the host's totals. `GET /health` shows every derived value with
its provenance (`auto` / `config` / `env`).

## Production deployment with TLS

For direct exposure, terminate TLS in Fluxum itself (drop
`FLUXUM_SERVER_ALLOW_PLAINTEXT`) and keep the secret out of the
environment via the config file's `${VAR}` expansion:

```bash
docker run -d \
  --name fluxum \
  -p 15800:15800 \
  -p 15801:15801 \
  -v fluxum-data:/var/lib/fluxum \
  -v $(pwd)/config.yml:/etc/fluxum/config.yml:ro \
  -v $(pwd)/certs:/etc/fluxum/certs:ro \
  -e FLUXUM_AUTH_SECRET="$(cat secrets/auth_secret.txt)" \
  hivehub/fluxum:latest \
  -c /etc/fluxum/config.yml
```

```yaml
# config.yml
auth:
  secret: ${FLUXUM_AUTH_SECRET}
server:
  tls:
    cert: /etc/fluxum/certs/fullchain.pem
    key: /etc/fluxum/certs/privkey.pem
```

Fluxum is designed for direct port exposure: per-IP admission control,
runtime IP/CIDR bans, overload shedding and session hardening are
in-process — no mandatory proxy in front.

## docker-compose

```yaml
services:
  fluxum:
    image: hivehub/fluxum:latest
    ports:
      - "15800:15800"   # HTTP: admin API + console + /rpc
      - "15801:15801"   # FluxRPC binary TCP
    environment:
      FLUXUM_AUTH_SECRET: change-me-generate-a-real-secret
      # Only when the compose network itself is the encrypted boundary:
      FLUXUM_SERVER_ALLOW_PLAINTEXT: "true"
    volumes:
      - fluxum-data:/var/lib/fluxum
    # The FR-05 probe reads these: 1 CPU / 512 MB derives 1 worker
    # thread, 1 shard and a ~256 MB memory budget automatically.
    cpus: 1.0
    mem_limit: 512m
    restart: unless-stopped

volumes:
  fluxum-data:
```

## Health check

The image ships a `HEALTHCHECK` that execs the binary itself (there is
no curl in a scratch image): `fluxum-server --healthcheck` probes
`GET /health` on the loopback listener every 15 s.

```bash
docker inspect --format='{{.State.Health.Status}}' fluxum
# healthy
```

`GET /health` is lock-free by contract (< 50 ms even under sustained
write load) and answers **503 while draining or degraded**, so a load
balancer pulls the instance exactly when it should.

## Features

- **Live query subscriptions**: clients subscribe with SQL and receive
  row diffs (`TxUpdate`) pushed on every commit — resumable after a
  disconnect from a per-query delta window, no snapshot re-download.
- **Server-side modules**: tables and reducers are compiled into the
  server (`#[fluxum::table]` / `#[fluxum::reducer]`); all writes go
  through reducers with per-identity admission rates.
- **Tiered storage under a budget**: in-memory MVCC with a copy-on-write
  paged store that evicts to a compressed on-disk cold tier; the RAM
  ceiling is enforced, observable (`/metrics`), and derived from the
  container's cgroup limits.
- **Five first-party SDKs**: Rust, TypeScript (browser + Node),
  Python, Go, C# — one conformance corpus keeps them honest.
- **Admin console built in**: data browser with row editing, SQL
  console (EXPLAIN, keyset pagination, live mode), reducer invocation,
  sessions & bans, metrics dashboards, config reload / checkpoint /
  drain / hot backup — one self-contained page at `/console`.
- **Replication & backups**: semi-sync replication with elections and
  fencing; hot backup with no writer stall, offline verify, PITR from
  archived segments, S3 archival.
- **Sharding**: explicit `sharding.shards: N` assembles N independent
  shards (own store, pool split, commit log, checkpoints) behind a
  coordinator; sessions route by identity affinity, rows by partition
  key.
- **Full-text search**: `#[fulltext]` columns with BM25 `MATCH`
  queries, live under subscriptions.
- **Postgres wire (read-only)**: point `psql` or a BI tool at the
  pgwire port for read-only SQL over the same data.
- **Hardware adaptivity**: boot-time probe (cores, RAM, cgroup limits)
  drives every `auto` knob; per-kernel SIMD selection (AVX-512 → AVX2 →
  NEON → scalar) with the chosen tier reported in `/health`.

## Links

- **Source**: https://github.com/hivellm/fluxum
- **Documentation**: https://github.com/hivellm/fluxum/tree/main/docs
- **Deployment guide**: https://github.com/hivellm/fluxum/blob/main/docs/DEPLOYMENT.md
- **Console guide**: https://github.com/hivellm/fluxum/blob/main/docs/CONSOLE.md
- **SDKs**: https://github.com/hivellm/fluxum/tree/main/sdks
- **Issues**: https://github.com/hivellm/fluxum/issues

## License

Apache 2.0. © HiveLLM Contributors.
