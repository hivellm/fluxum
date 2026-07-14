# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **Adopted performance/stability findings from the SpacetimeDB source dossier into the specs**:
  scalable fan-out via query-hash dedup + value-level plan pruning (SPEC-005, T4.2 — never
  O(clients)); flat row lists, compression negotiation (none/gzip/brotli), `tx_updates: full|light`
  opt-out, bounded per-connection queues (SPEC-006); CRC32C + epoch framing, group-commit flush
  actor, non-destructive torn-tail repair, incremental content-addressed checkpoints,
  rollback/undelete + blob-GC correctness rules (SPEC-002); paged evictable indexes + per-page
  checksums (SPEC-015); refcounted client cache with FluxBIN-byte row identity and
  mutate-then-callback ordering, bounded SDK channels, auto-reconnect reaffirmed (SPEC-011);
  deterministic simulation testing (DST) + process-level restart harness (SPEC-013, T2.7);
  rollback-safe at-least-once scheduling with restart rescan, schedule-only reducers reject
  client calls by default (SPEC-004); JWT identity derived from stable claims — token rotation
  never changes Identity (SPEC-009, FR-70); enriched TxUpdate kept as default with light opt-out
  (FR-43).

### Added
- SpacetimeDB **source-code dossier** (`docs/analysis/spacetimedb-code/`, 11 files): deep
  implementation analysis of the real v2.7.0 codebase (~237k LOC Rust, 45 crates,
  commit `1a8df2a`), subsystem-by-subsystem, each mapped to Fluxum's specs and DAG tasks;
  synthesis of ranked hard problems, adopt/avoid list, and roadmap impact in
  `10-hard-problems.md`. Headline findings: replication absent from their OSS (SPEC-014 is a
  differentiator), no published PostgreSQL parity benchmarks (NFR-11 uncontested), subscription
  fan-out must use query-hash dedup + value-level pruning (SPEC-005 impact), paged indexes are
  the novel part of tiered storage (SPEC-015).

## [0.1.0-alpha] - 2026-07-14

> Design phase — complete documentation set; implementation starts at DAG Phase 0.

### Added
- Product Requirements Document (`docs/PRD.md` v1.1): ~70 functional requirements (FR-01…FR-113
  by area), 14 non-functional requirements — including the **permanent comparative baseline vs
  app-server + PostgreSQL** (NFR-11), the PostgreSQL-like memory envelope (NFR-12), billion-row
  capacity (NFR-13), and SIMD scalar-parity (NFR-14) — personas, use cases, risks, acceptance
  criteria for 0.1.0 (MVP) and 0.2.0 (competitive launch).
- Implementation DAG (`docs/DAG.md`): 8 phases (T0.x–T7.x), gates G0–G7, critical path,
  task table with per-task exit tests, workstream view. Phase 2 covers tiered storage,
  compression, and SIMD; Phase 7 covers replica sets, backup/PITR, SDK breadth, and the
  billion-row soak.
- System architecture (`docs/ARCHITECTURE.md`): database-as-a-server model, native static Rust
  module design (no WASM/FFI), workspace layout (`fluxum-core`/`-macros`/`-protocol`/`-server`/
  `-cli`/`-bench` + `sdks/`), FluxRPC protocol (`u32 LE + MessagePack` envelope, FluxBIN row
  encoding), **tiered storage under a single `memory.budget`** (buffer pool + paged LZ4 cold
  tier), CommitLog doubling as replication stream, replica sets with consensus failover,
  SIMD runtime dispatch + hardware adaptivity, ShardCoord/ShardHost runtime, key decisions.
- Roadmap (`docs/ROADMAP.md`): milestones M0–M9 mapped to DAG gates; 0.1.0 = MVP with parity
  report v1; 0.2.0 = competitive launch (replica sets, backup/PITR, 5 SDKs, 1B-row soak).
- 16 normative implementation specs (`docs/specs/SPEC-001…SPEC-016`) with stable requirement
  IDs (`DM-`/`STG-`/`TXN-`/`RED-`/`SUB-`/`RPC-`/`SHD-`/`SPX-`/`AUTH-`/`MIG-`/`SDK-`/`OBS-`/`TST-`/
  `REP-`/`TIER-`/`HWA-`) and the traceability chain PRD → DAG → SPEC → tests.
- SDK plan: five SDKs as the minimum competitive surface — JavaScript/TypeScript, Python, Go,
  Rust, C# (shared conformance corpus; C++ post-launch). The JS/TS SDK is **browser-native**:
  the browser speaks binary FluxRPC directly to the database over **Streamable HTTP** (`/rpc`:
  POST frames + GET push stream via fetch `ReadableStream`, FluxBIN on `ArrayBuffer`, no
  WebSocket, no JSON hot path, no gateway), ships as plain JS (ESM/CJS + `.d.ts`, zero
  dependencies, ≤ 50 KB min+gzip) and also runs on Node over TCP; WebTransport is a P2
  follow-up (FR-88).
- API surface decisions: HTTP paths are **unversioned** (`/health`, `/metrics`, `/schema`,
  `/reducer/:name`, `/query` — no `/v1` prefix; compatibility via format freezes + additive
  evolution) and the port set is 15800 (HTTP: admin + `/rpc`) + 15801 (FluxRPC TCP) — no
  WebSocket port.
- Reference analysis (`docs/analysis/`): SpacetimeDB (10 files), Convex, SurrealDB studies and
  the gaps/improvement catalogue, inherited from the UzDB design set.
- Family standard files: README, LICENSE (Apache-2.0), CONTRIBUTING, SECURITY, this changelog.

### Provenance
- Fluxum is the Rust implementation of the UzDB design (2026-04, originally targeting the TML
  language), generalized from its original domain focus to a general-purpose realtime database.
  Naming migration: UzDB → Fluxum · UzRPC → FluxRPC · UzBIN → FluxBIN · ports 789x → 1580x.
