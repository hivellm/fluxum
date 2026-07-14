# Fluxum — Specifications

This directory is the **implementation contract** for Fluxum. The specs are ported from the UzDB
design set (TML → Rust, generalized to arbitrary realtime workloads) and say *what to build*,
normatively. The reference studies that explain *why* live in [`docs/analysis/`](../analysis/README.md).

## How to navigate

| Question | Read |
|---|---|
| What are we building and why? Requirements and release criteria? | [**PRD.md**](../PRD.md) |
| What order does the work happen in? What blocks what? | [**DAG.md**](../DAG.md) |
| How does the whole system fit together? | [**ARCHITECTURE.md**](../ARCHITECTURE.md) |
| How exactly does component X behave? | The SPEC for that component (below) |
| Why was it designed this way? | [`docs/analysis/`](../analysis/README.md) |

Traceability chain: **PRD** requirement IDs (`FR-xx`, `NFR-xx`) → **DAG** tasks (`T<phase>.<n>`)
→ **SPEC** requirement IDs (`DM-xxx`, `STG-xxx`, …) → tests (SPEC-013).

## Specifications

| Spec | Scope | Prefix | Freeze event |
|---|---|---|---|
| [SPEC-001](SPEC-001-data-model.md) — Data Model | Tables, types, `#[fluxum::table]` attribute surface, composite PKs, indexes, schema registry | `DM-` | Module API freeze (T6.1) |
| [SPEC-002](SPEC-002-storage-engine.md) — Storage Engine | MemStore (CommittedState/TxState), CommitLog, SnapshotRepo, recovery | `STG-` | Log format freezes with wire (G5) |
| [SPEC-003](SPEC-003-transactions.md) — Transactions | ACID guarantees, MVCC, single-writer, commit pipeline, rollback, constraints | `TXN-` | — |
| [SPEC-004](SPEC-004-reducers.md) — Reducers | Reducer execution, ReducerContext/TxHandle, lifecycle hooks, `#[tick]` drift, `#[schedule]`, rate limiting, panic isolation | `RED-` | Module API freeze (T6.1) |
| [SPEC-005](SPEC-005-subscriptions.md) — Subscriptions | SQL subset, CompiledPlan, fan-out, SubscribeSingle, ORDER BY semantics, RLS, backpressure | `SUB-` | — |
| [SPEC-006](SPEC-006-protocol-fluxrpc.md) — FluxRPC Protocol | Wire framing, FluxValue, FluxBIN rows, messages, transports, HTTP admin | `RPC-` | **Wire freeze (G5)** |
| [SPEC-007](SPEC-007-sharding.md) — Sharding | Partitioning strategies, ShardCoord/ShardHost, entity handoff, global tables | `SHD-` | — |
| [SPEC-008](SPEC-008-spatial-indexes.md) — Geospatial Indexes | QuadTree, R-tree, `IN REGION` / `WITHIN RADIUS` SQL extensions | `SPX-` | — |
| [SPEC-009](SPEC-009-authentication.md) — Authentication | Identity derivation, AuthProvider trait, server-to-server identity, RBAC | `AUTH-` | — |
| [SPEC-010](SPEC-010-schema-migration.md) — Schema Migration | `#[migration(version)]`, auto-diff, `__schema_meta__`, safe auto-apply | `MIG-` | — |
| [SPEC-011](SPEC-011-sdk-codegen.md) — SDK Codegen | `/schema` JSON, `fluxum generate`, SDK matrix (browser-native JS/TS, Python, Go, Rust, C#; C++/WebTransport P2), conformance corpus, client cache | `SDK-` | Schema JSON freeze (T6.1) |
| [SPEC-012](SPEC-012-observability.md) — Observability | Prometheus metrics catalogue, health endpoint, structured logs | `OBS-` | — |
| [SPEC-013](SPEC-013-testing-conformance.md) — Testing & Conformance | Crash suites, subscription property tests, **PostgreSQL parity harness (NFR-11)**, load/soak tests, SIMD parity, quality gates | `TST-` | — |
| [SPEC-014](SPEC-014-replication.md) — Replication & Backup | Replica sets (primary election, failover, epoch fencing), commit-log streaming, read/fan-out offload, `fluxum backup`, PITR | `REP-` | Stream format = log format (frozen at G5) |
| [SPEC-015](SPEC-015-tiered-storage.md) — Tiered Storage & Compression | Buffer pool, `memory.budget`, paged cold tier (page format, eviction), LZ4/zstd compression, datasets ≫ RAM | `TIER-` | **Page format freeze (G5)** |
| [SPEC-016](SPEC-016-hardware-adaptivity.md) — Hardware Adaptivity & SIMD | Boot-time hardware probe (cores/RAM/cgroups), adaptive tuning, SIMD runtime dispatch, scalar-parity rule | `HWA-` | — |

## Conventions

- RFC 2119 keywords (**MUST**, **SHALL**, **MUST NOT**, **SHOULD**, **MAY**) are normative.
- Requirement IDs are stable and referenced from commits, PRs, and tests. Removing or changing
  the meaning of an ID requires the same review bar as the behavior change itself.
- Each requirement carries a priority tag: `[P0]` MVP (0.1.0) blocker · `[P1]` required for the
  competitive launch (0.2.0) · `[P2]` post-launch.
- Two hard freezes exist: the **persisted/wire formats** (FluxRPC framing, FluxBIN encoding,
  commit-log entry format, on-disk page format — replication and PITR replay them) freeze at
  gate **G5**; the **module API** (`#[fluxum::*]` attribute surface and the `/schema` JSON)
  freezes at task **T6.1**. After a freeze, changes require a version bump of the frozen artifact.
- Integers are little-endian unless stated otherwise.
- Provenance: these specs originate from the UzDB design set
  (`E:\UzmiGames\UzDB\docs\specs\`, 2026-04), ported to Rust and generalized. The improvement
  catalogue applied there (row-encoding split, enriched TxUpdate, composite PKs, backpressure
  tiers, tick drift semantics, intra-tx reads, rate limiting, server identity, SubscribeSingle)
  is already folded into these documents.

## Change control

1. A change to product behavior lands as: PRD update (if requirements change) + SPEC update +
   DAG update (if work items change) — in the same PR.
2. Post-freeze changes to SPEC-006 (wire) require a protocol-version bump proposal; post-freeze
   changes to SPEC-001/004/011 (module API + schema JSON) must be additive.
3. Divergence between these specs and the UzDB planning docs: **these specs win**.
