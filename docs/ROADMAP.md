# Fluxum — Roadmap

> Current phase: **Design** — architecture and specs complete, Rust implementation starting.
> Milestones map 1:1 to the [DAG](DAG.md) phases; each gate (`G<n>`) is the milestone's exit
> criterion. Two releases are planned: **0.1.0 (MVP)** and **0.2.0 (competitive launch)** —
> the launch bar is set by the [PRD](PRD.md): faster than app-server + PostgreSQL (NFR-11),
> PostgreSQL-like memory envelope (NFR-12), five SDKs, replica sets, billions of rows (NFR-13).

Status legend: ✅ Completed · 🚧 In Progress · 📋 Planned · 🔮 Future (post-launch)

---

## Timeline

```
2026 Q3                        2026 Q4                        2027 Q1              2027 Q2
│                              │                              │                    │
├─ M0 Bootstrap (G0)           │                              │                    │
│  ├─ M1 Foundation (G1) ──────┤                              │                    │
│  │    └─ M2 Storage + Tiering (G2) ─────────┤               │                    │
│  │         └─ M3 Runtime (G3)               │               │                    │
│  │              └─ M4 Subscriptions (G4)    │               │                    │
│  │                   └─ M5 Transports + Sharding (G5) ──────┤                    │
│  │                        └─ M6 SDK Codegen + Parity ───────┤                    │
│  │                             └─ M7 Hardening → 0.1.0 (G6) ├─ M8 Replication    │
│  │                                                          ├─ M9 SDK breadth +  │
│  │                                                          │    billion-row soak│
│  │                                                          └──── → 0.2.0 (G7) ──┤
```

---

## Milestones

### M0 — Bootstrap (DAG Phase 0) 📋
**Gate:** G0 | **Blocks:** everything

Cargo workspace (`crates/fluxum-core`, `-macros`, `-protocol`, `-server`, `-cli`, `-bench`;
`sdks/rust`), nightly toolchain, workspace lints (`unwrap_used`/`expect_used`/
`undocumented_unsafe_blocks` denied), CI matrix (fmt + clippy `-D warnings` + nextest on
Linux/macOS/Windows), core types (`FluxumError`, `Identity`, `ConnectionId`, `EntityId`,
`Timestamp`), YAML config loader with `FLUXUM_*` env overrides **and hardware probe**
(cores, RAM, cgroup limits → adaptive defaults, PRD FR-05).

**Definition of done:** `cargo test` green on 3 OSes with the skeleton crates.

---

### M1 — Foundation (DAG Phase 1) 📋
**Gate:** G1 | **Depends on:** M0

| Component | Description |
|-----------|-------------|
| `#[fluxum::table]` | Proc-macro surface: `#[primary_key]`, `#[auto_inc]`, `#[index]`, composite PKs, `#[spatial]`, `#[visibility]`, `partition_by`; link-time schema registry |
| `FluxValue` + FluxBIN | Value enum + schema-driven binary row codec (roundtrip property-tested) |
| FluxRPC types | Message enums + `u32 LE + MessagePack` frame codec |
| `AuthProvider` | Trait + `token`/`jwt`/`none` built-ins; `Identity = SHA-256(token)` |

**Definition of done:** example schema (User/OnlineUser/ChatMessage/Task/Sensor) compiles;
codec roundtrip and auth suites green.

---

### M2 — Storage Engine + Tiering (DAG Phase 2) 📋
**Gate:** G2 | **Depends on:** M1

| Component | Description |
|-----------|-------------|
| `MemStore` | Committed state (hot tier in buffer pool) + `TxState` (in-flight writes), MVCC |
| **Paged cold tier** | Own page format (FluxBIN rows + CRC32), clock-LRU eviction, `memory.budget: auto` enforcement — datasets bounded by disk, not RAM |
| **Compression** | LZ4 per cold page, zstd for checkpoints/backups; ≥ 3× target ratio |
| `CommitLog` | Append-only `u32 LE + MessagePack + CRC32`; async writer, rotation, replay; doubles as replication stream later |
| Checkpoints | Periodic dumps; recovery = checkpoint + log replay; log truncation (compaction) |
| B-tree indexes | Single + composite secondary indexes (paged) |
| QuadTree / R-tree | Geospatial indexes + `IN REGION` / `WITHIN RADIUS` predicates |
| **SIMD kernels** | Runtime dispatch (AVX-512/AVX2/SSE4.2/NEON/scalar): CRC32, hashing, FluxBIN batch codec, predicate eval — scalar-parity enforced in CI |
| Crash suite | kill -9 harness, CRC corruption drills (log **and** pages), 10 GB recovery benchmark |

**Definition of done:** crash + replay suite loses zero committed transactions; recovery < 30 s
for a 10 GB log; a dataset 10× the memory budget is served correctly on the small-droplet profile.

---

### M3 — Reducer Runtime (DAG Phase 3) 📋
**Gate:** G3 | **Depends on:** M2

| Component | Description |
|-----------|-------------|
| Transactions | Commit pipeline, rollback, monotonic `tx_id`, constraint checks |
| `ReducerContext` + `TxHandle` | `insert`/`delete`/`upsert`/`query_pk`/`scan`/`scan_where` + intra-tx `scan_pending`/`scan_all` |
| Reducer engine | Dispatch, lifecycle hooks, `catch_unwind` panic isolation |
| `#[fluxum::tick(rate = N)]` | Fixed-timestep clock; drift detection at 3× period |
| `#[fluxum::schedule]` | One-shot and recurring deferred reducers (`__schedule__`) |
| Rate limiting | Token bucket per `(Identity, reducer)`, pre-transaction rejection |
| Schema migration | `#[fluxum::migration(version)]`, auto-diff, `__schema_meta__` |

**Definition of done:** rollback, tick-drift, rate-limit, and migration suites all pass.

---

### M4 — Subscriptions (DAG Phase 4) 📋
**Gate:** G4 | **Depends on:** M3

| Component | Description |
|-----------|-------------|
| SQL compiler | `SELECT * FROM T [WHERE …] [IN REGION …]` → `CompiledPlan` |
| `SubscriptionManager` | Register/unsubscribe plans; post-commit fan-out of `TxUpdate` diffs |
| `#[visibility]` RLS | `owner_only` filter per subscriber identity; server-peer bypass |
| Backpressure | 3-tier send buffer (Normal / Pressured / Full); slow client disconnected |

**Definition of done:** property suite — 10,000 random mutations; all subscriber caches match
server state exactly; slow-consumer stress test green.

---

### M5 — Transports + Sharding (DAG Phase 5) 📋
**Gate:** G5 (**wire + on-disk formats freeze here**) | **Depends on:** M4

| Component | Description |
|-----------|-------------|
| FluxRPC TCP | Port 15801 — backend services, native SDKs |
| Streamable HTTP | Port 15800 `/rpc` — web/mobile clients: binary `POST` frames + `GET` push stream (fetch `ReadableStream`), `Fluxum-Session` binding |
| HTTP admin | Port 15800 — unversioned paths: `/health`, `/metrics`, `/schema`, `/reducer`, `/query` |
| `ShardCoord` + `ShardHost` | Partition-key routing (hash/range/region); independent storage per shard; global-table replication |
| Entity handoff | 11-step atomic row-set migration between shards |
| Observability | Prometheus `fluxum_*` metrics, structured JSON logs, slow-reducer warnings |

**Definition of done:** end-to-end demo — client authenticates → subscribes → calls reducer →
receives `TxUpdate`; 2-shard handoff test with zero data loss; kill -9 + restart recovers all
committed state.

---

### M6 — SDK Codegen + Parity Harness (DAG Phase 6, first half) 📋
**Depends on:** M5

```bash
fluxum generate --lang typescript --out ./sdk
fluxum schema export --server http://localhost:15800 --out ./schema.json
```

| Deliverable | Description |
|--------|--------|
| JavaScript/TypeScript SDK | **Browser-native**: binary FluxRPC over Streamable HTTP (fetch `ReadableStream`, `ArrayBuffer` decode, no JSON hot path), plain-JS consumable (ESM/CJS + typings, zero deps), Node TCP support; typed tables, reducer calls, subscription callbacks, local cache — powers the demo app |
| Rust SDK | `fluxum-sdk` client crate (shares `fluxum-protocol`) |
| **`fluxum-bench` parity harness** | The same app implemented on app-server + PostgreSQL (and SQLite variant); equal hardware, honest durability configs; comparative report generator — a release artifact from 0.1.0 onward (NFR-11) |

**Definition of done:** demo app (chat + presence + per-user tasks) runs entirely on the
generated TypeScript SDK; parity report v1 produced.

---

### M7 — Production Hardening → 0.1.0 (DAG Phase 6, second half) 📋
**Gate:** G6 | **Depends on:** M5, M6

- Load test: ≥ 100,000 reducer calls/s sustained on a single shard
- **Parity targets met**: write throughput ≥ 10×, end-to-end change→client p99 ≥ 10× lower,
  cold reads within 2× of PostgreSQL
- Security audit: auth bypass paths, RLS bypass paths, SQL injection in the subscription compiler
- All P0 Prometheus metrics visible in a Grafana dashboard
- Deployment guide: systemd, Docker, config reference, droplet profile

**Definition of done:** PRD §12.1 acceptance criteria all green — tag **0.1.0**.

---

### M8 — Replication & Backup (DAG Phase 7, availability track) 📋
**Depends on:** M7

| Component | Description |
|-----------|-------------|
| Replication streaming | Commit log **is** the replication protocol: full sync via checkpoint transfer, partial sync from log offset; async + semi-sync quorum modes; epoch fencing |
| Replica sets | Per-shard primary + N replicas; consensus-based election (OQ-8), automatic failover; SDKs reconnect/resubscribe transparently |
| Read offload | Replicas serve one-off reads and subscription fan-out; bounded, observable staleness (`fluxum_replication_lag`) |
| Backup + PITR | `fluxum backup create/restore/verify` (hot, zstd, no writer stall); point-in-time recovery from archived log segments |

**Definition of done:** failover drill with zero committed-tx loss (semi-sync);
backup + restore + PITR round-trip in CI.

---

### M9 — SDK Breadth + Scale Validation → 0.2.0 (DAG Phase 7, launch track) 📋
**Gate:** G7 | **Depends on:** M7 (parallel with M8)

| Deliverable | Description |
|--------|--------|
| Python SDK | asyncio-first; shared conformance corpus |
| Go SDK | context-aware; shared conformance corpus |
| C# SDK | async/await, NuGet; shared conformance corpus |
| Billion-row soak | ≥ 1B rows sharded + tiered, sustained writes + subscriptions, memory within budget (NFR-13) |
| Droplet validation | Full test profile on 1 vCPU / 512 MB with dataset ≥ 10× RAM (NFR-12) |
| Parity report v2 | Published with the release |

**Definition of done:** PRD §12.2 acceptance criteria all green — tag **0.2.0**.
With five SDKs (TypeScript, Python, Go, Rust, C#), replica sets, and the published parity report,
Fluxum is minimally competitive.

---

## Parallel tracks

After M3, these tracks are independent (see [DAG §2](DAG.md#2-critical-path)):

```
Track A (critical path)       Track B                  Track C
─────────────────────────     ─────────────────────    ─────────────────────
M4 Subscriptions              Geospatial indexes       SIMD kernels (T2.10)
M5 Transports + Sharding      (T2.5–T2.6, needed by    trail scalar refs;
M6 SDK Codegen + Parity        T4.1 only)               parity baseline app
M7 Hardening → 0.1.0                                    (T6.3) buildable
M8 ∥ M9 → 0.2.0                                         any time after G1
```

---

## Post-launch backlog 🔮

| Feature | Notes |
|---------|-------|
| Native TLS (TCP) + HTTPS | FR-46; use a reverse proxy meanwhile |
| RBAC — `ctx.roles` from `AuthClaims` | FR-73; manual guards work for now |
| C++ codegen target | FR-87 |
| `POST /procedure/:name` admin endpoint | FR-26 (P2 half) |
| Shard split / merge tooling | Ops tooling for repartitioning |
| Multi-primary (active-active) replication | Explicit non-goal for launch (PRD §8) |
| Multiple simultaneous auth providers | Pluggable multi-provider |
| HiveHub.Cloud integration | Multi-tenant SaaS mode via `hivehub-internal-sdk` (family pattern) |
| UMICP endpoint | Family inter-component protocol |
