# Fluxum — Product Requirements Document (PRD)

| | |
|---|---|
| **Product** | Fluxum — realtime game database (database-as-a-server) in Rust |
| **Status** | Approved for implementation (successor of the UzDB design, ported TML → Rust) |
| **Version** | 1.0 of this document, targeting product release 0.1.0 (MVP) |
| **Date** | 2026-07-14 |
| **Owner** | Andre Ferreira |
| **Reference** | [SpacetimeDB analysis](analysis/spacetimedb/00-README.md) |
| **Related** | [DAG.md](DAG.md) (implementation plan) · [SPEC index](specs/README.md) (normative specs) · [ARCHITECTURE.md](ARCHITECTURE.md) |

---

## 1. Executive summary

Fluxum is a realtime game database written in Rust that eliminates the intermediate application
server from MMORPG backends. Game logic runs as atomic **reducer** functions inside the database
process — compiled natively into the server binary, with no WASM sandbox and no FFI. Clients
subscribe directly to live data and receive incremental diffs after every committed transaction —
no polling, no cache invalidation, no separate state-sync service.

The entire backend ships as a single binary.

Fluxum is the Rust implementation of the UzDB design (originally targeting TML). All architectural
decisions, the SpacetimeDB reference analysis, and the improvement catalogue carry over; the
implementation language, tooling, and conventions follow the HiveLLM family (Nexus, Vectorizer,
Synap).

## 2. Problem statement

### 2.1 The classic MMORPG backend stack is broken

Every MMORPG built on a traditional stack carries the same structural tax:

```
Client → Game Server (Node.js / C++) → ORM → PostgreSQL / Redis
```

| Problem | Impact |
|---------|--------|
| Two process boundaries per write | Latency × 2; each hop adds 1–5 ms |
| Cache inconsistency | Redis and PostgreSQL drift; stale reads are common |
| Manual push notifications | WebSocket fan-out code duplicated in every project |
| No row-level security at the DB layer | Application must re-implement ownership checks |
| WASM module overhead (SpacetimeDB) | FFI marshaling on every reducer call |
| No native spatial indexing | Area-of-interest queries are O(n) filter scans |
| Schema migration fragility | Manual reducer-based migrations with no diff support |

### 2.2 SpacetimeDB proved the concept — but has ceilings

SpacetimeDB (Rust core, WASM modules) demonstrated at scale — BitCraft Online sustained
150,000 tx/s from a single binary with ACID semantics and push subscriptions, roughly 100× the
throughput of a Node.js + PostgreSQL baseline. But it has hard limits:

| Limitation | Consequence |
|------------|-------------|
| WASM sandbox | FFI overhead on every reducer call; JIT compile at startup |
| Single-node, RAM bounded | No horizontal scale for large worlds |
| O(n) area-of-interest filter | Spatial queries bottleneck past ~10,000 entities |
| Manual `schedule_reducer` recursion for ticks | Fragile game-loop implementation |
| Imperative Rust filter for row visibility | Hard to audit, easy to miss a path |
| No composite primary keys | Cannot represent `(chunk_x, chunk_y)` as a PK |
| Energy-based rate limiting | Complex; game devs just want `"10/s"` |

### 2.3 The opportunity

A database designed from scratch for game backends — native module compilation, native spatial
indexes, declarative game primitives, multi-shard worlds — can exceed SpacetimeDB on every metric
while being simpler to operate and extend. The full improvement catalogue is in the
[gaps analysis](analysis/README.md).

## 3. Product vision

> **Fluxum is the persistence layer for MMORPG backends that disappears into the stack.**
> Developers write reducers in Rust; Fluxum handles ACID, push sync, spatial queries, rate
> limiting, row-level security, schema migration, and multi-shard scale — invisibly.

### 3.1 Design philosophy

1. **Native over sandbox.** Game modules are plain Rust crates compiled into the server binary.
   No WASM, no FFI, no JIT at startup, no cross-boundary marshaling.
2. **Declarative over imperative.** `#[fluxum::tick(rate = 60)]` beats `schedule_reducer`
   recursion. `#[visibility(owner_only(owner))]` beats a filter closure the auditor will miss.
3. **Single binary.** No external dependencies — no Redis, no Postgres, no ZooKeeper.
4. **Correctness over features.** ACID is not negotiable. Partial writes never reach clients.
5. **Scale horizontally.** Multi-shard world regions with independent storage per shard.

### 3.2 Goals

| ID | Goal |
|---|---|
| G1 | Eliminate the application-server tier: reducers, subscriptions, and auth live in one process |
| G2 | Exceed SpacetimeDB: native modules, composite PKs, O(log n) spatial queries, declarative RLS/rate-limit/tick, multi-shard worlds |
| G3 | Sub-millisecond writes and microsecond reads on the hot path (see §7) |
| G4 | Type-safe generated client SDKs from a single schema source of truth |
| G5 | HiveLLM-family operational profile: single YAML config, Prometheus metrics, structured logs, one binary |

## 4. Target users

| Persona | Context | What they need |
|---|---|---|
| **P1 — MMORPG backend engineer** | Builds persistent online games (50–10,000 CCU); owns persistence, sessions, economy, chat, inventory | Real-time state sync without building a WebSocket fan-out service; ACID without the ORM tax |
| **P2 — Game engine engineer (C++ game server)** | Writes loot tables, combat resolution, AI — not persistence plumbing | Fire-and-forget `ReducerCall` over loopback TCP with sub-millisecond RTT |
| **P3 — Browser/mobile client developer** | Connects via WebSocket; renders live state | Typed incremental diffs via a generated TypeScript SDK; direct reducer calls |

## 5. Use cases

| ID | Use case | Flow |
|---|---|---|
| UC-1 | Player session management | `Authenticate` → `on_connect` reducer → `Subscribe` (inventory, stats, quests) → `InitialData` → reducer calls → `TxUpdate` push → `on_disconnect` cleanup |
| UC-2 | Inventory with ownership isolation | `#[visibility(owner_only(owner))]` on `Inventory`; client subscribes `SELECT * FROM Inventory`; server filters rows per identity — zero application filter code |
| UC-3 | World zones with spatial queries | `TerrainChunk` with `#[spatial(quadtree(x, y))]` + composite PK `(cx, cy)`; client subscribes `SELECT * FROM TerrainChunk IN REGION (0, 0, 4000, 4000)` — O(log n + k) |
| UC-4 | C++ game server combat integration | C++ resolves damage → `ReducerCall("apply_damage", [mob_id, 42])` → atomic HP decrement + `DamageEvent` insert → `TxUpdate` to all zone subscribers |
| UC-5 | Scheduled game events | `#[fluxum::schedule]` reducer spawns a world boss, then `ctx.schedule_after(Duration::from_hours(6), "spawn_boss", args)` |
| UC-6 | Rate-limited chat | `#[fluxum::reducer(max_rate = "5/s")]` — token bucket per `(Identity, reducer)` enforced by the runtime before the transaction starts |

## 6. Functional requirements

Requirement IDs are stable and referenced by the [specs](specs/README.md) and the [DAG](DAG.md).
Priority: **P0** = MVP blocker, **P1** = required for launch, **P2** = post-launch candidate.

### 6.1 Core model (FR-0x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-01 | DB-as-server: reducers run inside the database process; no intermediate application server. | P0 | SPEC-004 |
| FR-02 | Single binary; no external runtime dependencies (no Postgres, Redis, Kafka, ZooKeeper). | P0 | — |
| FR-03 | Native Rust modules: game logic is a Rust crate using `fluxum` proc-macros, statically compiled into the server binary. No WASM, no FFI, no dynamic loading. | P0 | SPEC-001, SPEC-004 |
| FR-04 | Single YAML config file with `FLUXUM_*` environment-variable overrides; `development` profile (no auth, single shard). | P0 | SPEC-012 |

### 6.2 Storage & transactions (FR-1x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-10 | In-memory storage (`CommittedState`) with append-only `CommitLog` for durability; no disk I/O on the read path. | P0 | SPEC-002 |
| FR-11 | ACID transactions: every reducer call is one atomic transaction; full rollback on error or panic. | P0 | SPEC-003 |
| FR-12 | MVCC: lock-free reads from `CommittedState`; single writer per shard. | P0 | SPEC-002, SPEC-003 |
| FR-13 | Crash recovery: snapshot + log replay; CRC32 per log entry; recovery truncates at the first corrupt entry. | P0 | SPEC-002 |
| FR-14 | Periodic snapshots every N committed transactions (configurable). | P0 | SPEC-002 |
| FR-15 | Composite primary keys: `#[fluxum::table(primary_key(a, b))]`. | P0 | SPEC-001 |
| FR-16 | B-tree secondary indexes, single-column (P0) and multi-column composite (P1). | P0/P1 | SPEC-001 |
| FR-17 | Intra-transaction reads: `scan_pending`, `scan_all`, `count_pending` see in-flight writes. | P0 | SPEC-004 |

### 6.3 Reducers (FR-2x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-20 | `#[fluxum::reducer]` — atomic mutation functions callable by clients over FluxRPC. | P0 | SPEC-004 |
| FR-21 | `#[fluxum::tick(rate = N)]` — fixed-timestep game loop with absolute-clock targets, missed-tick logging, and 3×-period drift reset. | P0 | SPEC-004 |
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
| FR-35 | Spatial subscription predicates: `IN REGION (x, y, w, h)` and `WITHIN RADIUS r OF (x, y)`. | P0 | SPEC-005, SPEC-008 |

### 6.5 Protocol (FR-4x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-40 | FluxRPC binary protocol: `u32 LE length + MessagePack` envelope, multiplexed by per-message `id` (out-of-order responses supported). | P0 | SPEC-006 |
| FR-41 | FluxBIN row encoding for `TableUpdate` rows: schema-driven, no field names/tags, ~40% smaller than MessagePack. | P0 | SPEC-006 |
| FR-42 | TCP (:15801) and WebSocket (:15802, subprotocol `v1.bin.fluxum`) transports carrying identical messages. | P0 | SPEC-006 |
| FR-43 | Enriched `TxUpdate`: `tx_id`, `timestamp`, `reducer_name`, `caller`, `duration_us`, `tables`. | P0 | SPEC-006 |
| FR-44 | HTTP/JSON admin API (:15800): `/v1/health`, `/v1/metrics`, `/v1/schema`, `POST /v1/reducer/:name`, `POST /v1/query`, `/v1/view/:name`. | P0 | SPEC-006 |
| FR-45 | Idle connection timeout and max frame size enforcement (default 16 MB, configurable). | P1 | SPEC-006 |
| FR-46 | TLS on TCP and WebSocket transports. | P2 | SPEC-006 |

### 6.6 Sharding (FR-5x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-50 | Multi-shard world: rectangular regions; each shard owns independent `MemStore`, `CommitLog`, and `SubscriptionManager`. | P0 | SPEC-007 |
| FR-51 | `ShardCoord` routes connections and reducer calls to the owning shard. | P0 | SPEC-007 |
| FR-52 | Player handoff: atomic entity-state migration when crossing shard boundaries. | P0 | SPEC-007 |
| FR-53 | `#[fluxum::table(global)]` tables replicated read-only to all shards. | P0 | SPEC-007 |
| FR-54 | Cross-shard subscription aggregation by `ShardCoord`. | P1 | SPEC-007 |

### 6.7 Spatial indexes (FR-6x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-60 | `#[spatial(quadtree(x, y))]` — O(log n + k) point and radius queries on persistent world geometry. | P0 | SPEC-008 |
| FR-61 | `#[spatial(rtree(…))]` — bounding-box range queries. | P1 | SPEC-008 |
| FR-62 | Spatial SQL predicates (`IN REGION`, `WITHIN RADIUS`) resolved via the spatial index, never a full scan. | P0 | SPEC-008 |

### 6.8 Authentication & security (FR-7x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-70 | Stable 256-bit `Identity = SHA-256(token)`; same token ⇒ same identity across reconnects. Ephemeral `ConnectionId` (u128) per connection. | P0 | SPEC-009 |
| FR-71 | Pluggable `AuthProvider` trait with `token`, `jwt`, and `none` (dev-only, loopback) built-ins. | P0 | SPEC-009 |
| FR-72 | Server-to-server identity `SHA-256("SERVER:" + name)`: privileged peers bypass row-level security. | P1 | SPEC-009 |
| FR-73 | RBAC: `ctx.roles` from `AuthClaims`. | P2 | SPEC-009 |

### 6.9 Migration, codegen & developer experience (FR-8x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-80 | `#[fluxum::migration(version = N)]` with `__schema_meta__` tracking, automatic schema diff, and safe auto-apply for additive changes; incompatible change without a migration aborts startup. | P1 | SPEC-010 |
| FR-81 | Machine-readable schema introspection at `GET /v1/schema` (tables, reducers, types) + `fluxum schema export`. | P0 | SPEC-011 |
| FR-82 | `fluxum generate --lang typescript`: typed tables, reducer calls, subscription callbacks, local client cache. | P1 | SPEC-011 |
| FR-83 | `fluxum generate --lang cpp`: typed structs + reducer call helpers. | P1 | SPEC-011 |
| FR-84 | Rust client SDK (`fluxum-sdk`) sharing `fluxum-protocol` types. | P1 | SPEC-011 |
| FR-85 | `fluxum generate --lang csharp`. | P2 | SPEC-011 |

### 6.10 Observability (FR-9x)

| ID | Requirement | Priority | Spec |
|---|---|---|---|
| FR-90 | Prometheus-compatible metrics at `/v1/metrics` (`fluxum_reducer_calls_total`, `fluxum_reducer_duration_us`, `fluxum_fanout_messages_total`, `fluxum_memstore_bytes`, `fluxum_subscriber_drops_total`, …). | P0 | SPEC-012 |
| FR-91 | `/v1/health` responds in < 50 ms without taking storage locks. | P0 | SPEC-012 |
| FR-92 | Structured JSON logging (`tracing`), configurable level and format (json/pretty). | P1 | SPEC-012 |
| FR-93 | Slow-reducer warning above a configurable threshold (default 5 ms). | P1 | SPEC-012 |

## 7. Non-functional requirements

| ID | Category | Requirement | Target |
|---|----------|-------------|--------|
| NFR-01 | Throughput | Reducer throughput per shard | ≥ 100,000 tx/s |
| NFR-02 | Read latency | `CommittedState` lookup (in-process) | < 1 µs |
| NFR-03 | Write latency | Reducer commit p99 (async log, no fsync) | < 1 ms |
| NFR-04 | Fan-out latency | `TxUpdate` delivery p99 (1,000 subscribers) | < 5 ms |
| NFR-05 | Round-trip | FluxRPC over loopback TCP p99 | < 0.5 ms |
| NFR-06 | Recovery | Crash recovery for a 10 GB commit log | < 30 s |
| NFR-07 | Memory | Disk I/O on the hot read path | 0 disk ops |
| NFR-08 | Durability | Data-loss window on process crash | < ~50 ms (log buffer) |
| NFR-09 | Quality | `cargo fmt --check`, `clippy -D warnings` (incl. `unwrap_used`/`expect_used` deny), nextest green on Linux/macOS/Windows | every PR |
| NFR-10 | Correctness | Subscription diff accuracy under property testing | 100% |

Reference baseline: BitCraft Online on SpacetimeDB — 150,000 tx/s, single binary.

## 8. Non-goals (contract)

| Out of scope | Rationale |
|--------------|-----------|
| Real-time position tracking | Belongs to the UDP layer of the C++ game server (~20–60 Hz, loss-tolerant) |
| Full SQL (JOINs, CTEs, aggregates) | Not needed for game subscriptions; offline analytics use other tools |
| WASM / multi-language module support | Native Rust modules eliminate FFI overhead — the whole point |
| Distributed cross-shard transactions | Shard boundary = transaction boundary; cross-shard writes are by design impossible |
| On-disk B-tree / LSM / SSTable storage | All data in RAM; durability is the commit log's job |
| OIDC / OAuth2 as built-in providers | The pluggable `AuthProvider` trait handles any token scheme |
| Full-text search | Out of scope for game persistence (the family has Vectorizer/Nexus for search) |
| GraphQL API | FluxRPC + HTTP/JSON admin covers all client needs |

## 9. Constraints

| Constraint | Value |
|------------|-------|
| Implementation language | Rust (edition 2024, nightly toolchain — HiveLLM family standard) |
| Target runtime | Single process, single binary; tokio async runtime |
| External dependencies | None at runtime (no Postgres, Redis, Kafka, ZooKeeper) |
| Protocol | FluxRPC — same framing family as SynapRPC / VectorizerRPC / Nexus RPC (`u32 LE + MessagePack`), proven across HiveLLM |
| Row encoding | FluxBIN (BSATN-equivalent); ~40% smaller than MessagePack for typed rows |
| Ports | 15800 HTTP admin · 15801 FluxRPC TCP · 15802 WebSocket (HiveLLM 15xxx range) |
| Minimum shard count | 1 (single-shard dev mode) |
| Maximum message size | 16 MB (configurable) |
| Dev-mode auth | `none` provider, loopback only |

## 10. Success metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Reducer throughput | ≥ 100,000 tx/s per shard | `fluxum_reducer_calls_total` under load test (T6.6) |
| Commit latency p99 | < 1 ms | `fluxum_reducer_duration_us` histogram |
| FluxRPC round-trip p99 | < 0.5 ms | Client-side timing on loopback |
| Recovery time (10 GB log) | < 30 s | Timed restart test (T2.7) |
| Fan-out latency p99 | < 5 ms (1,000 subscribers) | `TxUpdate` delivery timestamp diff |
| Zero data loss on crash | 100% | Crash + replay correctness suite (T2.7) |
| SDK type safety | Zero runtime type errors | Generated SDK used in the test game (T6.5) |
| Subscription correctness | 100% diff accuracy | Property suite (T4.5) |

## 11. Risks

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Proc-macro complexity (schema registry, attribute surface) | Medium | Medium | T1.1 lands the full macro surface early; golden-file expansion tests (`trybuild`) |
| `catch_unwind` limits on panic isolation (non-unwind-safe state) | Medium | High | TxState is discarded on panic; `CommittedState` is never mutated mid-transaction; abort-on-double-panic documented |
| MVCC write contention on hot shards | Medium | Medium | Single-writer design eliminates read/write contention; queue-depth metric alerts |
| Fan-out bottleneck at scale | Low | High | 3-tier backpressure; drop metrics alert before clients stall |
| Commit-log corruption on crash | Low | Critical | CRC32 per entry; recovery truncates at first corrupt entry (T2.7 drills) |
| Memory exhaustion on large worlds | Medium | High | `fluxum_memstore_bytes` alert; shard subdivision reduces per-shard footprint |
| SDK codegen drift from schema | Low | Medium | Schema version in `InitialData`; SDK detects mismatch and reconnects |

## 12. Acceptance criteria (MVP / 0.1.0)

The MVP is shippable when:

- [ ] Single-shard Fluxum process boots, accepts FluxRPC connections, executes reducers, and delivers `TxUpdate`
- [ ] A complete test game (inventory + chat + sessions) runs on Fluxum via the generated TypeScript SDK
- [ ] Throughput benchmark: ≥ 100,000 `move_player` calls/s on one shard
- [ ] Crash recovery: zero committed-transaction loss after kill -9 + restart (commit-log replay)
- [ ] Subscription correctness: property suite (10,000 random mutations) — every client cache matches server state
- [ ] 2-shard integration test: player crosses a shard boundary with zero data loss
- [ ] Prometheus metrics endpoint functional; Grafana dashboard shows all P0 metrics
- [ ] All P0 requirements in §6 have corresponding passing tests in the specs' acceptance suites

## 13. Open questions

| # | Question | Owner | Due |
|---|----------|-------|-----|
| OQ-1 | Reducer registration: `inventory` vs `linkme` vs explicit `ServerBuilder::register` — link-time collection behavior on all 3 OSes? | @Andre | Before T1.1 |
| OQ-2 | One process with N `ShardHost` tokio tasks vs one process per shard for multi-shard deployments? | @Andre | Before T5.4 |
| OQ-3 | FluxBIN: hand-rolled codec vs `#[derive(FluxBin)]` proc macro from day one? | @Andre | Before T1.2 |
| OQ-4 | Does `#[fluxum::tick]` run on the shard writer thread or a dedicated scheduler task feeding the reducer queue? | @Andre | Before T3.4 |
| OQ-5 | Commit-log segment size and rotation policy defaults? | @Andre | Before T2.2 |

## 14. References

- [ARCHITECTURE.md](ARCHITECTURE.md) — full system design, data flows, key decisions
- [DAG.md](DAG.md) — dependency graph, 7-phase build order, critical path
- [ROADMAP.md](ROADMAP.md) — milestones and parallel tracks
- [Spec index](specs/README.md) — 13 normative implementation specs
- [Analysis](analysis/README.md) — SpacetimeDB/Convex/SurrealDB reference studies + gaps analysis
