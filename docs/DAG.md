# Fluxum — Implementation DAG

Dependency graph of all implementation work items from empty repo to the 0.1.0 MVP release.
Derived from the [ROADMAP](ROADMAP.md) milestones and the [PRD](PRD.md) requirements; each task
links the spec(s) that govern it.

Conventions:

- **Task IDs** are `T<phase>.<n>` and are stable — commits, issues, and PRs should reference them.
- An edge `A → B` means **B cannot start until A is done** (hard dependency). Tasks with no edge
  between them may run in parallel.
- **Gates** (`G<phase>`) are the phase exit criteria — quality checkpoints that block everything
  downstream of them.
- The FluxRPC wire format and the FluxBIN row encoding freeze at **G5** (first external clients);
  the public module API (`#[fluxum::table]` / `#[fluxum::reducer]` surface) freezes at **T6.1**
  (SDK codegen is the forcing function).

## 1. Graph

```mermaid
graph TD
    subgraph P0["Phase 0 — Bootstrap"]
        T0.1["T0.1 Cargo workspace + CI skeleton<br/>(crates/*, nightly, fmt+clippy -D warnings, nextest 3 OS)"]
        T0.2["T0.2 Core types + config loader<br/>(FluxumError, Identity, EntityId, Timestamp, YAML + FLUXUM_ env)"]
        G0{{"G0: cargo test green on 3 OS"}}
        T0.1 --> T0.2
        T0.1 --> G0
        T0.2 --> G0
    end

    subgraph P1["Phase 1 — Foundation"]
        T1.1["T1.1 Data-model macros + schema registry<br/>(#[table], #[primary_key], #[auto_inc], #[index], composite PK)"]
        T1.2["T1.2 FluxValue + FluxBIN codec<br/>+ FluxRPC message types"]
        T1.3["T1.3 AuthProvider trait<br/>+ token/jwt/none providers"]
        G1{{"G1: example schema compiles;<br/>codec roundtrip property tests green"}}
        G0 --> T1.1
        G0 --> T1.2
        G0 --> T1.3
        T1.1 --> G1
        T1.2 --> G1
        T1.3 --> G1
    end

    subgraph P2["Phase 2 — Storage core"]
        T2.1["T2.1 MemStore<br/>(CommittedState + TxState, MVCC)"]
        T2.2["T2.2 CommitLog<br/>(append-only, CRC32, async writer, rotation, replay)"]
        T2.3["T2.3 SnapshotRepo<br/>(periodic dumps, snapshot + replay recovery)"]
        T2.4["T2.4 B-tree secondary indexes<br/>(single + composite)"]
        T2.5["T2.5 QuadTree spatial index"]
        T2.6["T2.6 R-tree + spatial predicates"]
        T2.7["T2.7 Crash suite<br/>(kill -9 harness, CRC corruption drills, recovery bench)"]
        G2{{"G2: crash suite loses zero committed tx;<br/>recovery truncates at first corrupt entry"}}
        G1 --> T2.1
        T2.1 --> T2.2
        T2.2 --> T2.3
        T2.1 --> T2.4
        T2.4 --> T2.5
        T2.5 --> T2.6
        T2.2 --> T2.7
        T2.3 --> T2.7
        T2.4 --> G2
        T2.7 --> G2
    end

    subgraph P3["Phase 3 — Execution core"]
        T3.1["T3.1 Transactions<br/>(commit pipeline, rollback, tx_id, constraints)"]
        T3.2["T3.2 ReducerContext + TxHandle<br/>(insert/delete/upsert/query_pk/scan/scan_pending/scan_all)"]
        T3.3["T3.3 Reducer engine<br/>(#[reducer] dispatch, lifecycle hooks, panic isolation)"]
        T3.4["T3.4 #[tick] fixed-timestep scheduler<br/>+ #[schedule] deferred reducers"]
        T3.5["T3.5 Rate limiting<br/>(token bucket per (Identity, reducer))"]
        T3.6["T3.6 Schema migration<br/>(#[migration(version)], auto-diff, __schema_meta__)"]
        G3{{"G3: rollback, tick-drift, rate-limit,<br/>and migration suites green"}}
        G2 --> T3.1
        T3.1 --> T3.2
        T3.2 --> T3.3
        T3.3 --> T3.4
        T3.3 --> T3.5
        T3.1 --> T3.6
        T3.4 --> G3
        T3.5 --> G3
        T3.6 --> G3
    end

    subgraph P4["Phase 4 — Subscriptions"]
        T4.1["T4.1 SQL subset compiler → CompiledPlan<br/>(WHERE, IN REGION, WITHIN RADIUS)"]
        T4.2["T4.2 SubscriptionManager<br/>(register/unsubscribe, fan-out loop, TxUpdate diffs)"]
        T4.3["T4.3 #[visibility] row-level security<br/>(owner_only + server bypass)"]
        T4.4["T4.4 Backpressure<br/>(3-tier per-client send buffer)"]
        T4.5["T4.5 Subscription correctness property suite<br/>(10k random mutations ⇒ cache ≡ server)"]
        G4{{"G4: property suite green;<br/>slow-consumer stress test green"}}
        G3 --> T4.1
        T2.6 --> T4.1
        T4.1 --> T4.2
        T4.2 --> T4.3
        T4.2 --> T4.4
        T4.3 --> T4.5
        T4.4 --> T4.5
        T4.5 --> G4
    end

    subgraph P5["Phase 5 — Transport & scale"]
        T5.1["T5.1 FluxRPC TCP transport<br/>(framing, sessions, message routing)"]
        T5.2["T5.2 WebSocket transport<br/>(subprotocol v1.bin.fluxum)"]
        T5.3["T5.3 HTTP/JSON admin API<br/>(/v1/health /v1/metrics /v1/schema /v1/reducer /v1/query)"]
        T5.4["T5.4 ShardCoord + ShardHost<br/>(routing, registry, global-table replication)"]
        T5.5["T5.5 Player handoff<br/>+ cross-shard subscription aggregation"]
        T5.6["T5.6 Observability<br/>(Prometheus metrics, JSON logs, slow-reducer warnings)"]
        G5{{"G5: e2e demo (auth→subscribe→reducer→TxUpdate);<br/>2-shard handoff test; WIRE FORMAT FROZEN"}}
        G4 --> T5.1
        T5.1 --> T5.2
        T5.1 --> T5.3
        G4 --> T5.4
        T5.4 --> T5.5
        T5.3 --> T5.6
        T5.4 --> T5.6
        T5.2 --> G5
        T5.3 --> G5
        T5.5 --> G5
        T5.6 --> G5
    end

    subgraph P6["Phase 6 — Developer experience & hardening"]
        T6.1["T6.1 /v1/schema JSON finalized<br/>+ fluxum schema export (MODULE API FREEZE)"]
        T6.2["T6.2 fluxum generate --lang typescript<br/>+ TS SDK runtime (local cache)"]
        T6.3["T6.3 fluxum generate --lang cpp"]
        T6.4["T6.4 Rust client SDK (fluxum-sdk)"]
        T6.5["T6.5 Test game on generated SDK<br/>(inventory + chat + sessions)"]
        T6.6["T6.6 Load test ≥100k tx/s/shard<br/>+ security audit + Grafana dashboard"]
        G6{{"G6: PRD §12 MVP acceptance criteria all green — 0.1.0"}}
        G5 --> T6.1
        T6.1 --> T6.2
        T6.1 --> T6.3
        G5 --> T6.4
        T6.2 --> T6.5
        G5 --> T6.6
        T6.5 --> G6
        T6.6 --> G6
        T6.3 --> G6
        T6.4 --> G6
    end
```

## 2. Critical path

The longest hard-dependency chain (everything else parallelizes around it):

```
T0.1 → T0.2 → G0 → T1.1 → G1
     → T2.1 → T2.2 → T2.3 → T2.7 → G2
     → T3.1 → T3.2 → T3.3 → T3.4 → G3
     → T4.1 → T4.2 → T4.4 → T4.5 → G4
     → T5.1 → T5.2/T5.3 → G5
     → T6.1 → T6.2 → T6.5 → G6
```

Implications:

- **Storage (Phase 2) is the schedule anchor** — the crash suite (T2.7) gates every later phase,
  and MVCC + CRC recovery are the subtlest code in the system. Do not rush them.
- **Spatial indexes (T2.5/T2.6) are off the critical path** until T4.1 needs the spatial SQL
  predicates. They can trail the rest of Phase 2 without blocking Phase 3.
- **Transports intentionally start late** (G4): everything in Phases 0–4 may change internal
  APIs freely; after G5 the wire format only changes with a protocol version bump.
- **Sharding (T5.4/T5.5) parallelizes with transports** — both only need G4.
- Parallelization opportunities: T1.1/T1.2/T1.3 are three independent workstreams after G0;
  T2.4→T2.5→T2.6 runs beside the T2.2→T2.3 chain; T3.6 runs beside T3.2–T3.5; all of Phase 6
  fans out after G5.

## 3. Task table

| ID | Task | Depends on | Spec(s) | PRD reqs | Deliverable / exit test |
|---|---|---|---|---|---|
| **T0.1** | Cargo workspace (`crates/fluxum-core`, `-macros`, `-protocol`, `-server`, `-cli`; `sdks/rust`), nightly toolchain, workspace lints (`unwrap_used = "deny"`), CI: fmt + clippy `-D warnings` + nextest on Linux/macOS/Windows | — | SPEC-013 | NFR-09 | Green pipeline on empty-ish crates |
| **T0.2** | `FluxumError` (thiserror), `Identity`/`ConnectionId`/`EntityId`/`Timestamp` newtypes, YAML config loader with `FLUXUM_` env overrides | T0.1 | SPEC-001, SPEC-009 | FR-04 | Unit tests on config precedence |
| **G0** | Phase 0 gate | T0.1, T0.2 | — | — | `cargo test` green on 3 OS |
| **T1.1** | `#[fluxum::table]` proc macro: `#[primary_key]`, `#[auto_inc]`, `#[index(btree(...))]`, composite PKs, `#[spatial]`/`#[visibility]` attribute parsing; link-time schema registry (inventory); `TableSchema` introspection | G0 | SPEC-001 | FR-15, FR-16, FR-81 | Example schema (Player/Position/TerrainChunk/Inventory) compiles; registry unit tests |
| **T1.2** | `FluxValue` enum, FluxBIN row codec (all primitive + product/sum types), FluxRPC message types + `u32 LE + MessagePack` frame codec | G0 | SPEC-006 | FR-40, FR-41 | Roundtrip property tests (proptest) for every type |
| **T1.3** | `AuthProvider` trait (object-safe) + `token`/`jwt`/`none` built-ins; `Identity = SHA-256(token)`; server identity `SHA-256("SERVER:" + name)` | G0 | SPEC-009 | FR-70, FR-71, FR-72 | Unit tests: stable identity across reconnects; provider matrix |
| **G1** | Phase 1 gate | T1.1, T1.2, T1.3 | — | — | Schema + codec + auth suites green |
| **T2.1** | `MemStore`: `CommittedState` (BTreeMap per table) + `TxState` (in-flight inserts/deletes); MVCC merge on commit, discard on rollback; lock-free committed reads | G1 | SPEC-002 | FR-10, FR-12 | ACID unit tests: insert/delete/query_pk/scan |
| **T2.2** | `CommitLog`: append-only `u32 LE + MessagePack + CRC32` entries, async writer (no fsync per tx), segment rotation, replay | T2.1 | SPEC-002 | FR-10, FR-13 | Write/replay tests incl. corruption truncation |
| **T2.3** | `SnapshotRepo`: dump every N committed tx; recovery = latest snapshot + log replay | T2.2 | SPEC-002 | FR-13, FR-14 | Snapshot + restore equivalence tests |
| **T2.4** | Secondary B-tree indexes (single + composite), maintained on commit | T2.1 | SPEC-001, SPEC-002 | FR-16 | Index consistency property tests |
| **T2.5** | QuadTree spatial index (BTreeMap-backed, no pointer chasing): insert/point/radius/delete | T2.4 | SPEC-008 | FR-60 | Spatial correctness tests vs brute force |
| **T2.6** | R-tree bounding-box index + `IN REGION` / `WITHIN RADIUS` predicate evaluation | T2.5 | SPEC-008 | FR-61, FR-62 | 1M-point query ≥10× faster than O(n) scan |
| **T2.7** | Crash suite: kill -9 harness at every commit boundary, CRC bit-flip drills, 10 GB recovery benchmark | T2.2, T2.3 | SPEC-013 | FR-13, NFR-06 | Zero committed-tx loss over full matrix |
| **G2** | Phase 2 gate | T2.4, T2.7 | — | — | Crash suite green; recovery < 30 s / 10 GB |
| **T3.1** | Transaction pipeline: validate → merge `CommittedState` → append `CommitLog` → respond; rollback discards `TxState`; monotonic `tx_id` per shard; PK-uniqueness + auto-inc constraints | G2 | SPEC-003 | FR-11, FR-15 | Concurrent-read/sequential-write harness |
| **T3.2** | `ReducerContext` + `TxHandle`: `insert`/`delete`/`upsert`/`query_pk`/`scan`/`scan_where` + intra-tx `scan_pending`/`scan_all`/`count_pending` | T3.1 | SPEC-004 | FR-17, FR-20 | TxHandle used by all reducer tests |
| **T3.3** | Reducer engine: `#[fluxum::reducer]` dispatch, `on_init`/`on_connect`/`on_disconnect` hooks, `catch_unwind` panic isolation (panic ⇒ rollback, shard never dies) | T3.2 | SPEC-004 | FR-20, FR-23, FR-25 | Panic-injection tests |
| **T3.4** | `#[fluxum::tick(rate)]` fixed-timestep clock (absolute targets, missed-tick log, 3×-period drift reset) + `#[fluxum::schedule]` one-shot/recurring via `__schedule__` | T3.3 | SPEC-004 | FR-21, FR-22 | Tick-drift timing tests |
| **T3.5** | `max_rate = "N/s"` token bucket per `(Identity, reducer)`; rejection before `TxState` creation (429) | T3.3 | SPEC-004 | FR-24 | Rate-limit conformance tests |
| **T3.6** | `#[fluxum::migration(version)]` runner: `__schema_meta__`, auto-diff, safe auto-apply for additive changes, abort on incompatible schema | T3.1 | SPEC-010 | FR-80 | Add/rename-column migrations pass; incompatible change aborts startup |
| **G3** | Phase 3 gate | T3.4, T3.5, T3.6 | — | — | Rollback + tick + rate-limit + migration suites green |
| **T4.1** | SQL subset compiler: `SELECT * FROM T [WHERE pred] [IN REGION …] [WITHIN RADIUS …]` → `CompiledPlan` (table filter, spatial constraint, visibility rule) | G3, T2.6 | SPEC-005 | FR-30, FR-35 | Parser + plan unit tests; injection-attempt corpus |
| **T4.2** | `SubscriptionManager`: register/unsubscribe plans per client, post-commit fan-out loop producing `TxUpdate { inserts, deletes }`; ORDER BY/LIMIT on `InitialData` only | T4.1 | SPEC-005 | FR-30, FR-31, FR-34 | Fan-out correctness tests |
| **T4.3** | `#[visibility(owner_only(field))]` RLS applied per subscriber identity; server-peer bypass | T4.2 | SPEC-005 | FR-32, FR-72 | RLS matrix tests (player/server/other) |
| **T4.4** | 3-tier per-client send buffer (Normal / Pressured / Full): non-blocking checks, drop policy + `fluxum_subscriber_drops_total` | T4.2 | SPEC-005 | FR-33 | Slow-consumer stress test |
| **T4.5** | Property suite: 10 000 random mutations across random subscriptions ⇒ every client cache ≡ server state | T4.3, T4.4 | SPEC-013 | NFR-10 | Suite green in CI |
| **G4** | Phase 4 gate | T4.5 | — | — | Subscription correctness + backpressure green |
| **T5.1** | FluxRPC TCP transport (:15801): frame parser, session state machine, routing for `Authenticate`/`ReducerCall`/`Subscribe`/`SubscribeSingle`/`Unsubscribe`/`OneOffQuery`; idle timeout + max frame size | G4 | SPEC-006 | FR-40, FR-42, FR-45 | Loopback integration tests |
| **T5.2** | WebSocket transport (:15802), subprotocol `v1.bin.fluxum`, same message layer | T5.1 | SPEC-006 | FR-42 | Browser-client integration test |
| **T5.3** | HTTP/JSON admin (:15800, axum): `/v1/health`, `/v1/metrics`, `/v1/schema`, `POST /v1/reducer/:name`, `POST /v1/query`, `/v1/view/:name` | T5.1 | SPEC-006 | FR-44, FR-91 | curl tests for all endpoints |
| **T5.4** | `ShardCoord` (request routing, shard registry, `#[table(global)]` replication) + `ShardHost` per-region loop (MemStore + CommitLog + SubscriptionManager) | G4 | SPEC-007 | FR-50, FR-51, FR-53 | Single- and multi-shard boot tests |
| **T5.5** | Player handoff (11-step atomic migration) + cross-shard subscription aggregation | T5.4 | SPEC-007 | FR-52, FR-54 | 2-shard boundary-crossing test, zero data loss |
| **T5.6** | Observability: all P0 `fluxum_*` Prometheus metrics, structured JSON logs, slow-reducer warnings | T5.3, T5.4 | SPEC-012 | FR-90, FR-92, FR-93 | Metrics endpoint + log format tests |
| **G5** | Phase 5 gate — **wire format frozen** | T5.2, T5.3, T5.5, T5.6 | SPEC-006 | — | e2e demo + 2-shard handoff green |
| **T6.1** | `/v1/schema` JSON finalized + `fluxum schema export` — **module API freeze** | G5 | SPEC-011 | FR-81 | Schema golden-file test |
| **T6.2** | `fluxum generate --lang typescript` + TS SDK runtime (typed tables, reducer calls, subscription callbacks, local cache) | T6.1 | SPEC-011 | FR-82 | Generated SDK compiles with zero manual stubs |
| **T6.3** | `fluxum generate --lang cpp` (typed structs + reducer helpers) | T6.1 | SPEC-011 | FR-83 | Generated header compiles in test harness |
| **T6.4** | Rust client SDK (`fluxum-sdk`, shares `fluxum-protocol`) | G5 | SPEC-011 | FR-84 | Conformance subset green |
| **T6.5** | Test game (inventory + chat + sessions) running end-to-end on the generated TS SDK | T6.2 | SPEC-013 | FR-82 | Demo scenario scripted in CI |
| **T6.6** | Load test ≥ 100 000 reducer calls/s on one shard; security audit (auth bypass, RLS bypass, SQL injection); Grafana dashboard | G5 | SPEC-012, SPEC-013 | NFR-01, FR-90 | Load report + audit with no P0 findings |
| **G6** | **Release 0.1.0 (MVP)** | T6.3, T6.4, T6.5, T6.6 | — | PRD §12 | Acceptance checklist all green |

## 4. Workstream view (suggested parallel staffing)

| Workstream | Tasks | Can run concurrently with |
|---|---|---|
| **Storage** | T2.1–T2.4, T2.7, T3.1 | Spatial, Macros/DX |
| **Spatial** | T2.5, T2.6 | Storage (after T2.4), Execution |
| **Execution/runtime** | T3.2–T3.6 | Subscriptions prep, Spatial |
| **Subscriptions** | T4.1–T4.5 | Transport prep (message types are done in T1.2) |
| **Transport** | T5.1–T5.3 | Sharding |
| **Sharding** | T5.4, T5.5 | Transport |
| **Macros/DX** | T1.1, T6.1–T6.4 | Everything (macro surface stabilizes early, codegen late) |
| **Quality/CI** | T0.1, T2.7, T4.5, T5.6, T6.5, T6.6 | Everything (continuous) |

## 5. Change control

- Adding/removing a task or edge requires updating this file **and** the affected spec in the same PR.
- A gate may not be weakened without a PRD change (the gates are PRD §12 acceptance criteria
  decomposed by phase).
- Post-MVP candidates (TLS transports, RBAC, C#/Rust codegen targets, shard split/merge tooling,
  log compaction, multi-provider auth) are **not** in this DAG by design; see
  [ROADMAP §post-MVP](ROADMAP.md#post-mvp-backlog).
