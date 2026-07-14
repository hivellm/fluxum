# Fluxum — Product Requirements Document (PRD)

| | |
|---|---|
| **Product** | Fluxum — general-purpose realtime database (database-as-a-server) in Rust |
| **Status** | Approved for implementation (successor of the UzDB design, ported TML → Rust and generalized) |
| **Version** | 1.1 of this document, targeting product releases 0.1.0 (MVP) and 0.2.0 (competitive launch) |
| **Date** | 2026-07-14 |
| **Owner** | Andre Ferreira |
| **Reference** | [SpacetimeDB analysis](analysis/spacetimedb/00-README.md) |
| **Related** | [DAG.md](DAG.md) (implementation plan) · [SPEC index](specs/README.md) (normative specs) · [ARCHITECTURE.md](ARCHITECTURE.md) |

---

## 1. Executive summary

Fluxum is a general-purpose realtime database written in Rust that eliminates the intermediate
application server. Application logic runs as atomic **reducer** functions inside the database
process — compiled natively into the server binary, with no WASM sandbox and no FFI. Clients
subscribe directly to live data and receive incremental diffs after every committed transaction —
no polling, no cache invalidation, no separate push service.

**The product's reason to exist is speed against the incumbent stack.** The permanent baseline is
a functionally identical application built on an intermediate server + PostgreSQL (and SQLite
where applicable): every performance claim Fluxum makes is measured against that parity app, on
the same hardware, by a benchmark harness that ships with the repository. If Fluxum is not
decisively faster for realtime workloads, it has no reason to exist.

The entire backend ships as a single binary. It runs on a 512 MB droplet and on a 64-core server,
adapting to the hardware it finds; datasets are not bounded by RAM (tiered storage with
compression), and horizontal scale comes from sharding plus replica sets, targeting billions of
rows.

Fluxum is the Rust implementation of the UzDB design (originally targeting TML). All architectural
decisions, the SpacetimeDB reference analysis, and the improvement catalogue carry over,
generalized to arbitrary realtime workloads; the implementation language, tooling, and conventions
follow the HiveLLM family (Nexus, Vectorizer, Synap).

## 2. Problem statement

### 2.1 The classic realtime backend stack is broken — and it is the baseline

Every application that shows shared mutable state live — chat, presence, collaborative documents,
dashboards, order books, fleet tracking — is built on the same taxed stack:

```
Client → App Server (Node.js / Go / …) → ORM → PostgreSQL
                └── Redis cache  └── WebSocket fan-out service
```

| Problem | Impact |
|---------|--------|
| Two process boundaries per write | Latency × 2; each hop adds 1–5 ms |
| Cache inconsistency | Redis and PostgreSQL drift; stale reads are common |
| Manual push notifications | WebSocket fan-out code duplicated in every project; `LISTEN/NOTIFY` doesn't scale |
| No row-level security at the DB layer | Application must re-implement ownership checks |
| WASM module overhead (SpacetimeDB) | FFI marshaling on every reducer call |
| No native geospatial indexing | Location-based live queries are O(n) filter scans |
| Schema migration fragility | Manual reducer-based migrations with no diff support |

**Baseline principle.** This stack is not just the problem statement — it is the yardstick. The
repository maintains a parity application (same schema, same operations, same client behavior)
implemented on app-server + PostgreSQL, and the `fluxum-bench` harness runs both sides on equal
hardware. Comparative results are published with every release (NFR-11, [SPEC-013](specs/SPEC-013-testing-conformance.md)).

### 2.2 SpacetimeDB proved the concept — but has ceilings

SpacetimeDB (Rust core, WASM modules) demonstrated the database-as-a-server architecture at
scale — sustaining 150,000 tx/s in production from a single binary with ACID semantics and push
subscriptions, roughly 100× the throughput of a Node.js + PostgreSQL baseline. But it has hard
limits:

| Limitation | Consequence |
|------------|-------------|
| WASM sandbox | FFI overhead on every reducer call; JIT compile at startup |
| All data must fit in RAM | Memory-bounded datasets; expensive servers for large data |
| Single-node | No horizontal scale, no replica failover |
| O(n) location filter | Geospatial live queries bottleneck past ~10,000 rows |
| Manual `schedule_reducer` recursion for periodic logic | Fragile high-frequency loops |
| Imperative Rust filter for row visibility | Hard to audit, easy to miss a path |
| No composite primary keys | Cannot represent `(grid_x, grid_y)` or `(tenant, key)` as a PK |
| Energy-based rate limiting | Complex; developers just want `"10/s"` |

### 2.3 The opportunity

A realtime database designed from scratch — native module compilation, tiered storage that does
not hold RAM hostage, native geospatial indexes, declarative primitives (RLS, rate limits,
periodic reducers), SIMD-accelerated hot paths, replica sets, and horizontal partitioning — can
beat the app-server + PostgreSQL stack on every realtime metric while operating within
PostgreSQL-like resource envelopes. The full improvement catalogue is in the
[gaps analysis](analysis/README.md).

## 3. Product vision

> **Fluxum is the realtime persistence layer that disappears into the stack.**
> Developers write reducers in Rust; Fluxum handles ACID, push sync, geospatial queries, rate
> limiting, row-level security, schema migration, replication, and multi-shard scale — invisibly,
> on whatever hardware it is given.

### 3.1 Design philosophy

1. **Native over sandbox.** Application modules are plain Rust crates compiled into the server
   binary. No WASM, no FFI, no JIT at startup, no cross-boundary marshaling.
2. **Declarative over imperative.** `#[fluxum::tick(rate = N)]` beats `schedule_reducer`
   recursion. `#[visibility(owner_only(owner))]` beats a filter closure the auditor will miss.
3. **Single binary.** No external dependencies — no Redis, no Postgres, no ZooKeeper.
4. **Correctness over features.** ACID is not negotiable. Partial writes never reach clients.
5. **Scale horizontally.** Partitioned shards with independent storage; replica sets for
   availability and read scale.
6. **Measure against PostgreSQL, always.** Every optimization is justified by the parity
   benchmark, not by microbenchmarks in isolation.
7. **Adapt to the hardware.** Memory budgets, worker counts, and SIMD kernels are derived from
   what the machine offers — a small droplet and a big server both get the optimal configuration
   without hand-tuning.

### 3.2 Goals

| ID | Goal |
|---|---|
| G1 | Eliminate the application-server tier: reducers, subscriptions, and auth live in one process |
| G2 | Exceed SpacetimeDB: native modules, tiered storage (data ≫ RAM), composite PKs, O(log n) geospatial queries, declarative RLS/rate-limit/tick, sharding + replica sets |
| G3 | Decisively beat the app-server + PostgreSQL baseline on realtime metrics (NFR-11), within a PostgreSQL-like memory envelope (NFR-12) |
| G4 | Type-safe generated client SDKs — TypeScript, Python, Go, Rust, and C# — from a single schema source of truth |
| G5 | HiveLLM-family operational profile: single YAML config, Prometheus metrics, structured logs, one binary |
| G6 | Billions of rows per deployment via sharding + tiered storage, validated by soak tests |

## 4. Target users

| Persona | Context | What they need |
|---|---|---|
| **P1 — Realtime app backend engineer** | Builds chat/presence/collaboration/dashboards/marketplaces with live state shared across many clients | Real-time state sync without building a WebSocket fan-out service; ACID without the ORM tax; ops cost no worse than Postgres |
| **P2 — Backend service engineer (trusted peer)** | Runs services (pricing engines, ingestion pipelines, schedulers) that mutate shared state at high rates | Fire-and-forget `ReducerCall` over loopback TCP with sub-millisecond RTT; privileged identity |
| **P3 — Web/mobile client developer** | Connects from the browser via Streamable HTTP; renders live state | Typed incremental diffs via a generated SDK in their language; direct reducer calls |
| **P4 — Operator / SRE** | Deploys on anything from a $6 droplet to a large dedicated server | Predictable memory ceiling, backups + PITR, replica failover, metrics, one binary |

## 5. Use cases

| ID | Use case | Flow |
|---|---|---|
| UC-1 | Session & presence | `Authenticate` → `on_connect` reducer inserts into `OnlineUser` → clients subscribed to `OnlineUser` see the user appear instantly → `on_disconnect` cleanup |
| UC-2 | Per-user data isolation | `#[visibility(owner_only(owner))]` on `Task`; client subscribes `SELECT * FROM Task`; server filters rows per identity — zero application filter code |
| UC-3 | Geospatial live queries | `Sensor` rows with `#[spatial(quadtree(x, y))]` + composite PK `(grid_x, grid_y)`; client subscribes `SELECT * FROM Sensor IN REGION (0, 0, 4000, 4000)` — O(log n + k) |
| UC-4 | Trusted service integration | An ingestion service calls `ReducerCall("update_reading", [sensor, value])` at high rate over loopback; every dashboard subscribed to that region gets the diff instantly |
| UC-5 | Scheduled jobs | A `#[fluxum::schedule]` reducer aggregates hourly stats, then `ctx.schedule_after(Duration::from_hours(1), "rollup_stats", args)` |
| UC-6 | Rate-limited user actions | `#[fluxum::reducer(max_rate = "5/s")]` on `send_chat` — token bucket per `(Identity, reducer)` enforced before the transaction starts |
| UC-7 | Large dataset on small hardware | A 200 GB event-history deployment runs on a 4 GB VM: hot working set in the buffer pool, cold pages compressed on disk, subscriptions unaffected |
| UC-8 | High availability | A replica set per shard: primary fails, a replica is promoted, clients reconnect and resubscribe; nightly `fluxum backup` + PITR covers operator error |

## 6. Functional requirements

Requirement IDs are stable and referenced by the [specs](specs/README.md) and the [DAG](DAG.md).
Priority: **P0** = MVP (0.1.0) blocker, **P1** = required for competitive launch (0.2.0),
**P2** = post-launch candidate.

### 6.1 Core model (FR-0x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-01 | DB-as-server: reducers run inside the database process; no intermediate application server. | P0 | SPEC-004 |
| FR-02 | Single binary; no external runtime dependencies (no Postgres, Redis, Kafka, ZooKeeper). | P0 | — |
| FR-03 | Native Rust modules: application logic is a Rust crate using `fluxum` proc-macros, statically compiled into the server binary. No WASM, no FFI, no dynamic loading. | P0 | SPEC-001, SPEC-004 |
| FR-04 | Single YAML config file with `FLUXUM_*` environment-variable overrides; `development` profile (no auth, single shard). | P0 | SPEC-012 |
| FR-05 | Hardware adaptivity: at boot, detect cores, total/available RAM, and container (cgroup) limits; derive worker counts, buffer-pool size, and queue depths. Sane behavior from 1 vCPU / 512 MB to large multi-core servers with no hand-tuning. | P0 | SPEC-016 |

### 6.2 Storage & transactions (FR-1x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-10 | Hot working set served from memory (buffer pool); durability via append-only `CommitLog`. Hot-path reads perform zero disk I/O. | P0 | SPEC-002, SPEC-015 |
| FR-11 | ACID transactions: every reducer call is one atomic transaction; full rollback on error or panic. | P0 | SPEC-003 |
| FR-12 | MVCC: lock-free reads from committed state; single writer per shard. | P0 | SPEC-002, SPEC-003 |
| FR-13 | Crash recovery: checkpoint + log replay; CRC32 per log entry; recovery truncates at the first corrupt entry. | P0 | SPEC-002 |
| FR-14 | Periodic checkpoints every N committed transactions (configurable); checkpoints allow truncating (compacting) old commit-log segments. | P0 | SPEC-002 |
| FR-15 | Composite primary keys: `#[fluxum::table(primary_key(a, b))]`. | P0 | SPEC-001 |
| FR-16 | B-tree secondary indexes, single-column (P0) and multi-column composite (P1). | P0/P1 | SPEC-001 |
| FR-17 | Intra-transaction reads: `scan_pending`, `scan_all`, `count_pending` see in-flight writes. | P0 | SPEC-004 |
| FR-18 | Tiered storage: cold data lives in a paged on-disk store (own format — no RocksDB/LSM dependency); pages are faulted into the buffer pool on demand and evicted under the memory budget. Datasets are bounded by disk, not RAM. | P0 | SPEC-015 |
| FR-19 | Compression: cold pages compressed with LZ4 by default (zstd optional); checkpoints and backups compressed with zstd. Target ≥ 3× compression on typical row data. | P1 | SPEC-015 |

### 6.3 Reducers (FR-2x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-20 | `#[fluxum::reducer]` — atomic mutation functions callable by clients over FluxRPC. | P0 | SPEC-004 |
| FR-21 | `#[fluxum::tick(rate = N)]` — fixed-timestep periodic reducers with absolute-clock targets, missed-tick logging, and 3×-period drift reset. | P0 | SPEC-004 |
| FR-22 | `#[fluxum::schedule]` — one-shot and recurring deferred reducers persisted in `__schedule__`. | P0 | SPEC-004 |
| FR-23 | Lifecycle hooks: `#[fluxum::on_init]`, `#[fluxum::on_connect]`, `#[fluxum::on_disconnect]`. | P0 | SPEC-004 |
| FR-24 | Declarative rate limiting `max_rate = "N/s"`: token bucket per `(Identity, reducer)`, rejected before the transaction starts (error 429). | P0 | SPEC-004 |
| FR-25 | Panic isolation: a panicking reducer rolls back and returns an error; the shard never crashes. | P0 | SPEC-004 |
| FR-26 | `#[fluxum::view]` (read-only) and `#[fluxum::procedure]` (admin) HTTP-exposed functions. | P1 | SPEC-004 |
| FR-27 | Versioned reducers `#[fluxum::reducer(version = N)]` for API evolution. | P2 | SPEC-010 |

### 6.4 Subscriptions (FR-3x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-30 | SQL push subscriptions: `SELECT * FROM T [WHERE …]` compiled once; incremental `TxUpdate` diffs delivered after every commit. Clients never poll. | P0 | SPEC-005 |
| FR-31 | `SubscribeSingle` + `Unsubscribe` by server-assigned query ID (granular management). | P1 | SPEC-005 |
| FR-32 | `#[visibility(owner_only(field))]` declarative row-level security applied to initial data and diffs. | P0 | SPEC-005 |
| FR-33 | 3-tier per-client fan-out backpressure (Normal / Pressured / Full); one slow client never stalls others. | P1 | SPEC-005 |
| FR-34 | `ORDER BY` / `LIMIT` apply to `InitialData` only; diffs are unordered and unlimited (documented semantics). | P1 | SPEC-005 |
| FR-35 | Geospatial subscription predicates: `IN REGION (x, y, w, h)` and `WITHIN RADIUS r OF (x, y)`. | P0 | SPEC-005, SPEC-008 |

### 6.5 Protocol (FR-4x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-40 | FluxRPC binary protocol: `u32 LE length + MessagePack` envelope, multiplexed by per-message `id` (out-of-order responses supported). | P0 | SPEC-006 |
| FR-41 | FluxBIN row encoding for `TableUpdate` rows: schema-driven, no field names/tags, ~40% smaller than MessagePack. | P0 | SPEC-006 |
| FR-42 | Two transports carrying identical messages: FluxRPC TCP (:15801) and **FluxRPC over Streamable HTTP** (`POST /rpc` + `GET /rpc` binary push stream on :15800) — the browser transport; fully binary, consumed via fetch `ReadableStream`, no WebSocket. | P0 | SPEC-006 |
| FR-43 | Enriched `TxUpdate` (`tx_id`, `timestamp`, `reducer_name`, `caller`, `duration_us`, `tables`) as the default, with a per-connection `tx_updates: full \| light` opt-out that omits metadata for bandwidth-critical clients (SpacetimeDB stripped metadata entirely in its v2 protocol for fan-out cost; Fluxum keeps it opt-out). | P0 | SPEC-006 |
| FR-44 | HTTP/JSON admin API (:15800), unversioned paths: `/health`, `/metrics`, `/schema`, `POST /reducer/:name`, `POST /query`, `/view/:name`. | P0 | SPEC-006 |
| FR-45 | Idle connection timeout and max frame size enforcement (default 16 MB, configurable). | P1 | SPEC-006 |
| FR-46 | TLS on the TCP transport; HTTPS for the Streamable HTTP and admin surfaces. | P2 | SPEC-006 |

### 6.6 Sharding (FR-5x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-50 | Horizontal partitioning: tables declare a partition key (`partition_by`); each shard owns independent storage and subscriptions. Hash, range, and geospatial-region strategies. | P0 | SPEC-007 |
| FR-51 | `ShardCoord` routes connections and reducer calls to the owning shard. | P0 | SPEC-007 |
| FR-52 | Entity handoff: atomic row-set migration when a partition key changes shard. | P0 | SPEC-007 |
| FR-53 | `#[fluxum::table(global)]` tables replicated read-only to all shards. | P0 | SPEC-007 |
| FR-54 | Cross-shard subscription aggregation by `ShardCoord`. | P1 | SPEC-007 |

### 6.7 Geospatial indexes (FR-6x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-60 | `#[spatial(quadtree(x, y))]` — O(log n + k) point and radius queries. | P0 | SPEC-008 |
| FR-61 | `#[spatial(rtree(…))]` — bounding-box range queries. | P1 | SPEC-008 |
| FR-62 | Geospatial SQL predicates (`IN REGION`, `WITHIN RADIUS`) resolved via the spatial index, never a full scan. | P0 | SPEC-008 |

### 6.8 Authentication & security (FR-7x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-70 | Stable 256-bit `Identity`, stable across reconnects **and token rotation**: `jwt` provider derives it from stable claims (hash of issuer‖subject — token refresh never changes identity); opaque `token` provider uses SHA-256(token). Ephemeral `ConnectionId` (u128) per connection. | P0 | SPEC-009 |
| FR-71 | Pluggable `AuthProvider` trait with `token`, `jwt`, and `none` (dev-only, loopback) built-ins. | P0 | SPEC-009 |
| FR-72 | Server-to-server identity `SHA-256("SERVER:" + name)`: privileged service peers bypass row-level security. | P1 | SPEC-009 |
| FR-73 | RBAC: `ctx.roles` from `AuthClaims`. | P2 | SPEC-009 |

### 6.9 Migration, codegen & SDKs (FR-8x)

Five SDKs are the minimum competitive surface: **TypeScript, Python, Go, Rust, C#**.

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-80 | `#[fluxum::migration(version = N)]` with `__schema_meta__` tracking, automatic schema diff, and safe auto-apply for additive changes; incompatible change without a migration aborts startup. | P1 | SPEC-010 |
| FR-81 | Machine-readable schema introspection at `GET /schema` (tables, reducers, types) + `fluxum schema export`. | P0 | SPEC-011 |
| FR-82 | JavaScript/TypeScript SDK, **browser-native**: `fluxum generate --lang typescript` — typed tables, reducer calls, subscription callbacks, local client cache. Runs in browsers over binary FluxRPC via **Streamable HTTP** (`/rpc` on :15800, FluxBIN decoded via `ArrayBuffer`/`DataView` from a fetch `ReadableStream` — never JSON on the hot path) and in Node.js (TCP or Streamable HTTP). Ships as plain ESM/CJS JavaScript + `.d.ts` typings — consumable from vanilla JS with no TypeScript toolchain and zero runtime dependencies. | P0 | SPEC-011 |
| FR-83 | Python SDK (typed client, asyncio-first, same conformance corpus). | P1 | SPEC-011 |
| FR-84 | Rust client SDK (`fluxum-sdk`) sharing `fluxum-protocol` types. | P1 | SPEC-011 |
| FR-85 | Go SDK (idiomatic, context-aware, same conformance corpus). | P1 | SPEC-011 |
| FR-86 | C# SDK (async/await, NuGet, same conformance corpus). | P1 | SPEC-011 |
| FR-87 | C++ codegen target (typed structs + reducer helpers). | P2 | SPEC-011 |
| FR-88 | WebTransport (HTTP/3 / QUIC) browser transport as a lower-latency evolution of Streamable HTTP, same message layer. | P2 | SPEC-011 |

### 6.10 Observability (FR-9x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-90 | Prometheus-compatible metrics at `/metrics` (`fluxum_reducer_calls_total`, `fluxum_reducer_duration_us`, `fluxum_fanout_messages_total`, `fluxum_memstore_bytes`, `fluxum_subscriber_drops_total`, buffer-pool and replication-lag gauges, …). | P0 | SPEC-012 |
| FR-91 | `/health` responds in < 50 ms without taking storage locks. | P0 | SPEC-012 |
| FR-92 | Structured JSON logging (`tracing`), configurable level and format (json/pretty). | P1 | SPEC-012 |
| FR-93 | Slow-reducer warning above a configurable threshold (default 5 ms). | P1 | SPEC-012 |

### 6.11 Replication, backup & resilience (FR-10x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-100 | Replica sets: per shard, one primary + N replicas. Replication streams the commit log (the log is the replication protocol); full sync via checkpoint transfer + partial sync via log offset. Async by default; optional semi-synchronous quorum acknowledgment. | P1 | SPEC-014 |
| FR-101 | Automatic failover: replica-set members elect a new primary on failure (consensus-based); clients reconnect and resubscribe transparently via the SDKs. | P1 | SPEC-014 |
| FR-102 | Read replicas: replicas serve read-only queries and **subscription fan-out** (offloading broadcast work from the primary); staleness is bounded and observable. | P1 | SPEC-014 |
| FR-103 | `fluxum backup create / restore / verify`: hot backup (checkpoint + archived log segments) without blocking writers; zstd-compressed; integrity-verified. | P1 | SPEC-014 |
| FR-104 | Point-in-time recovery: restore a backup and replay archived commit-log segments up to a target timestamp / tx_id. | P1 | SPEC-014 |
| FR-105 | Replication observability: role, connected replicas, replication offset/lag exposed via `/health` and `fluxum_replication_*` metrics. | P1 | SPEC-012, SPEC-014 |

### 6.12 Resource management & acceleration (FR-11x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-110 | Memory budget: a single configurable ceiling (`memory.budget: auto \| <bytes>`) enforced primarily through the buffer pool; `auto` derives from detected RAM/cgroup limits. The process never grows unbounded with dataset size. | P0 | SPEC-015, SPEC-016 |
| FR-111 | SIMD acceleration with runtime dispatch (AVX-512 / AVX2 / SSE4.2 / NEON, scalar fallback) on hot kernels: CRC32, hashing, FluxBIN batch encode/decode, predicate evaluation over row batches, page compression. | P1 | SPEC-016 |
| FR-112 | SIMD correctness: every SIMD kernel is bit-identical to its scalar reference implementation; enforced by property tests on an ISA matrix in CI. | P0 | SPEC-016, SPEC-013 |
| FR-113 | Adaptive tuning: worker-thread counts, fan-out concurrency, WAL buffer sizes, and checkpoint cadence derived from detected hardware; overridable in config; effective values logged at boot and exposed in `/health`. | P1 | SPEC-016 |

## 7. Non-functional requirements

| ID | Category | Requirement | Target |
|---|----------|-------------|--------|
| NFR-01 | Throughput | Reducer throughput per shard | ≥ 100,000 tx/s |
| NFR-02 | Read latency | Committed-state lookup, hot (in buffer pool) | < 1 µs |
| NFR-03 | Write latency | Reducer commit p99 (async log, no fsync) | < 1 ms |
| NFR-04 | Fan-out latency | `TxUpdate` delivery p99 (1,000 subscribers) | < 5 ms |
| NFR-05 | Round-trip | FluxRPC over loopback TCP p99 | < 0.5 ms |
| NFR-06 | Recovery | Crash recovery for a 10 GB commit log | < 30 s |
| NFR-07 | Memory | Disk I/O on the hot read path (buffer-pool hit) | 0 disk ops |
| NFR-08 | Durability | Data-loss window on process crash (single node) | < ~50 ms (log buffer) |
| NFR-09 | Quality | `cargo fmt --check`, `clippy -D warnings` (incl. `unwrap_used`/`expect_used` deny), nextest green on Linux/macOS/Windows | every PR |
| NFR-10 | Correctness | Subscription diff accuracy under property testing | 100% |
| NFR-11 | **Comparative baseline** | Measured by the `fluxum-bench` parity harness — the same application on app-server + PostgreSQL, equal hardware, honest durability settings on both sides — published every release: write throughput ≥ 10×; end-to-end change→subscriber p99 latency ≥ 10× lower (vs `LISTEN/NOTIFY` fan-out); hot reads ≥ 50× lower latency (in-process vs SQL round trip); cold (page-in) reads within 2× of PostgreSQL | every release |
| NFR-12 | Memory envelope | Fully functional on 1 vCPU / 512 MB with a dataset ≥ 10× RAM (tiered storage); idle baseline RSS < 100 MB | validated in CI profile |
| NFR-13 | Capacity | ≥ 1 billion rows per deployment (sharded + tiered), sustained by a soak test | 0.2.0 gate |
| NFR-14 | SIMD parity | SIMD kernels bit-identical to scalar reference on every supported ISA | CI matrix |

Reference baseline: SpacetimeDB in production — 150,000 tx/s, single binary.

## 8. Non-goals (contract)

| Out of scope | Rationale |
|--------------|-----------|
| Full SQL (JOINs, CTEs, aggregates) | Not needed for live subscriptions; offline analytics use other tools |
| WASM / multi-language module support | Native Rust modules eliminate FFI overhead — the whole point |
| Distributed cross-shard transactions | Shard boundary = transaction boundary; cross-shard writes are by design impossible |
| Embedding a third-party storage engine (RocksDB, LMDB, SQLite) | The paged store, commit log, and buffer pool are owned code — the performance envelope is the product |
| OIDC / OAuth2 as built-in providers | The pluggable `AuthProvider` trait handles any token scheme |
| Full-text / vector search | Out of scope — the family has Nexus and Vectorizer for that |
| GraphQL API | FluxRPC + HTTP/JSON admin covers all client needs |
| Multi-primary (active-active) replication | Replica sets are single-primary per shard; conflict-free multi-primary is a research project, not a launch feature |

## 9. Constraints

| Constraint | Value |
|------------|-------|
| Implementation language | Rust (edition 2024, nightly toolchain — HiveLLM family standard) |
| Target runtime | Single process, single binary; tokio async runtime |
| External dependencies | None at runtime (no Postgres, Redis, Kafka, ZooKeeper) |
| Protocol | FluxRPC — same framing family as SynapRPC / VectorizerRPC / Nexus RPC (`u32 LE + MessagePack`), proven across HiveLLM |
| Row encoding | FluxBIN (BSATN-equivalent); ~40% smaller than MessagePack for typed rows |
| Ports | 15800 HTTP (admin API + Streamable HTTP `/rpc`) · 15801 FluxRPC TCP (HiveLLM 15xxx range) |
| Memory | Bounded by the configured budget (FR-110); dataset size bounded by disk, not RAM |
| Minimum hardware | 1 vCPU / 512 MB (small droplet profile, NFR-12) |
| Minimum shard count | 1 (single-shard dev mode); replica sets optional |
| Maximum message size | 16 MB (configurable) |
| Dev-mode auth | `none` provider, loopback only |

## 10. Success metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| **Parity vs PostgreSQL** | NFR-11 ratios hold | `fluxum-bench` comparative report, every release (T6.3) |
| Reducer throughput | ≥ 100,000 tx/s per shard | `fluxum_reducer_calls_total` under load test (T6.6) |
| Commit latency p99 | < 1 ms | `fluxum_reducer_duration_us` histogram |
| FluxRPC round-trip p99 | < 0.5 ms | Client-side timing on loopback |
| Recovery time (10 GB log) | < 30 s | Timed restart test (T2.7) |
| Fan-out latency p99 | < 5 ms (1,000 subscribers) | `TxUpdate` delivery timestamp diff |
| Zero data loss on crash | 100% | Crash + replay correctness suite (T2.7) |
| Dataset ≫ RAM | 10× RAM dataset on 512 MB profile | Tiered-storage soak (SPEC-015 acceptance) |
| Billion-row capacity | ≥ 1B rows sharded deployment | Soak test (T7.7) |
| Failover | Replica promoted, clients resubscribed, zero committed-tx loss (semi-sync) | Failover drill (T7.2) |
| SDK coverage | 5 SDKs passing the same conformance corpus | Conformance CI (T4.5 corpus, T7.4–T7.6) |
| Subscription correctness | 100% diff accuracy | Property suite (T4.5) |

## 11. Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Tiered storage complexity (page format, eviction, torn pages) | High | High | Own the format early (Phase 2); crash suite covers paged paths; page CRC + shadow-write checkpoint protocol |
| Proc-macro complexity (schema registry, attribute surface) | Medium | Medium | T1.1 lands the full macro surface early; golden-file expansion tests (`trybuild`) |
| `catch_unwind` limits on panic isolation | Medium | High | TxState discarded on panic; committed state never mutated mid-transaction; abort-on-double-panic documented |
| Parity benchmark contested ("unfair to Postgres") | Medium | Medium | Publish harness + configs; tuned Postgres (indexes, prepared statements, `synchronous_commit` documented both ways); SQLite variant included |
| Failover correctness (split-brain) | Medium | Critical | Consensus-based election (SPEC-014); fencing via epoch numbers in the replication stream; semi-sync mode for zero-loss guarantee |
| SIMD kernel divergence across ISAs | Medium | High | FR-112 scalar-parity property tests on ISA matrix in CI (family precedent: Vectorizer `simd-matrix.yml`) |
| Fan-out bottleneck at scale | Low | High | 3-tier backpressure; read replicas offload fan-out (FR-102) |
| Memory exhaustion on large datasets | Low (by design) | High | FR-110 budget + buffer-pool eviction; `fluxum_bufferpool_*` metrics alert |
| SDK maintenance burden (5 languages) | Medium | Medium | Single conformance corpus (SPEC-013); thin SDKs over generated code; family precedent (Vectorizer/Synap ship 5–7 SDKs) |

## 12. Acceptance criteria

### 12.1 MVP (0.1.0)

- [ ] Single-shard Fluxum process boots, accepts FluxRPC connections, executes reducers, and delivers `TxUpdate`
- [ ] A complete demo app (chat + presence + per-user tasks) runs on Fluxum via the generated TypeScript SDK
- [ ] Throughput benchmark: ≥ 100,000 small-write reducer calls/s on one shard
- [ ] **Parity report v1**: `fluxum-bench` runs the demo workload against app-server + PostgreSQL on equal hardware; NFR-11 write-throughput and end-to-end-latency ratios met
- [ ] Tiered storage: dataset 10× the memory budget served correctly on the small-droplet profile
- [ ] Crash recovery: zero committed-transaction loss after kill -9 + restart (commit-log replay)
- [ ] Subscription correctness: property suite (10,000 random mutations) — every client cache matches server state
- [ ] 2-shard integration test: entity handoff across a partition boundary with zero data loss
- [ ] Prometheus metrics endpoint functional; Grafana dashboard shows all P0 metrics

### 12.2 Competitive launch (0.2.0)

- [ ] Replica set: automatic failover drill passes; zero committed-tx loss in semi-sync mode
- [ ] `fluxum backup` + PITR restore verified in CI
- [ ] Five SDKs (TypeScript, Python, Go, Rust, C#) pass the shared conformance corpus
- [ ] Billion-row soak: ≥ 1B rows across shards, sustained writes + subscriptions, memory within budget
- [ ] Parity report v2 published with the release

## 13. Open questions

| # | Question | Owner | Due |
|---|----------|-------|-----|
| OQ-1 | Reducer registration: `inventory` vs `linkme` vs explicit `ServerBuilder::register` — link-time collection behavior on all 3 OSes? | @Andre | Before T1.1 |
| OQ-2 | One process with N `ShardHost` tokio tasks vs one process per shard for multi-shard deployments? | @Andre | Before T5.4 |
| OQ-3 | FluxBIN: hand-rolled codec vs `#[derive(FluxBin)]` proc macro from day one? | @Andre | Before T1.2 |
| OQ-4 | Does `#[fluxum::tick]` run on the shard writer thread or a dedicated scheduler task feeding the reducer queue? | @Andre | Before T3.4 |
| OQ-5 | Commit-log segment size and rotation policy defaults? | @Andre | Before T2.2 |
| OQ-6 | Default partitioning strategy when `partition_by` is absent — everything on shard 0, or hash of PK? | @Andre | Before T5.4 |
| OQ-7 | Page size (4 KB vs 8 KB vs 16 KB) and read path: mmap vs pread into pool — per-OS trade-offs? | @Andre | Before T2.8 |
| OQ-8 | Consensus for failover: `openraft` (Vectorizer precedent) vs custom Raft (Nexus precedent)? | @Andre | Before T7.2 |
| OQ-9 | Parity baseline app stack: axum+sqlx vs Node/Express+pg — which is the *fairest* representative? (Possibly both.) | @Andre | Before T6.3 |
| OQ-10 | cgroup/container limit detection: which crate, and how to behave when limits are absent? | @Andre | Before T0.2 |

## 14. References

- [ARCHITECTURE.md](ARCHITECTURE.md) — full system design, data flows, key decisions
- [DAG.md](DAG.md) — dependency graph, 8-phase build order, critical path
- [ROADMAP.md](ROADMAP.md) — milestones and parallel tracks
- [Spec index](specs/README.md) — 16 normative implementation specs
- [Analysis](analysis/README.md) — SpacetimeDB/Convex/SurrealDB reference studies + gaps analysis
