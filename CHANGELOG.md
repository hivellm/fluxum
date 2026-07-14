# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
