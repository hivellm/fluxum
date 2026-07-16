# SPEC-028 — Stable Machine-Readable Error Catalog

| | |
|---|---|
| **Status** | Draft (implemented; freezes with the wire at G5) |
| **Phase / tasks** | Phase 1 (`phase1_error-catalog`) — pre-G5 wire change ([DAG](../DAG.md)) |
| **PRD requirements** | Amends FR-40/FR-43 error semantics; consumed by SPEC-011 SDK codegen |
| **Requirement prefix** | `ERR-` |
| **Source** | Fluxum-native. Clients connect directly to the database — the wire `Error` frame IS the public failure API; HTTP-style codes and free-form strings were too weak for that role. |

Keywords are RFC 2119. Requirement IDs `ERR-xxx` are stable. Priority: all `[P0]` (the wire
format freezes at G5; this must land before it).

## 1. Scope

One registry of stable `u16` error codes in per-subsystem ranges, a structured wire `Error`
payload (name, severity, retry semantics, SQLSTATE, typed details), structured reducer outcomes,
a total mapping from every internal `FluxumError`, HTTP status derivation for Streamable HTTP,
and a generated error reference — all deriving from a single table in `fluxum-protocol`
(`src/codes.rs`). The task's normative requirement set (ranges, payload, retryability, SQLSTATE,
single-source registry, mapping totality, HTTP derivation, RED-060 amendment) lives in the
rulebook spec delta and is archived with the task; this document pins the released surface.

## 2. Code ranges & stability (ERR-001..003) `[P0]`

- **ERR-001** Ranges: 1xxx `PROTO_`, 2xxx `AUTH_`, 3xxx `SQL_`/`TXN_`, 4xxx `SCHEMA_`,
  5xxx `REDUCER_`, 6xxx `SUB_`, 7xxx `STORAGE_`, 8xxx `CLUSTER_`, 9xxx `SYS_`. Every code has a
  unique canonical `SCREAMING_SNAKE` name.
- **ERR-002** Codes and names are never reused, renumbered, or renamed once released; retiring an
  error retires its number permanently. Registry tests enforce uniqueness, range membership, and
  the spec-pinned assignments.
- **ERR-003** The released catalog is `fluxum_protocol::codes::CATALOG`; `docs/errors.md` is
  generated from it (one section per entry) and kept in sync by a golden test
  (`FLUXUM_REGEN_DOCS=1` regenerates).

## 3. Wire payload (ERR-010..012) `[P0]`

- **ERR-010** `ErrorMessage` carries `id: Option<u32>`, `code: u16`, `name: String`,
  `message: String`, `severity: error|fatal`, `retryable: bool`, `retry_after_ms: Option<u32>`,
  `sqlstate: Option<String>`, `details: [(String, FluxValue)]`. `severity = fatal` means the
  server closes the connection after the frame.
- **ERR-011** `ErrorMessage::from_catalog` is the only sanctioned constructor: name, severity,
  retryability, and SQLSTATE come from the registry; an uncataloged code degrades to
  `SYS_INTERNAL` (debug builds assert). `details` keys are exactly those the entry documents.
- **ERR-012** `ReducerResult.outcome` errors are structured: `[code, app_code, message]` —
  5001 `REDUCER_USER_ERROR` wraps a body's own `Err(message)` verbatim (optional
  application-defined `app_code`, its own namespace); 5002 `REDUCER_PANIC` for panics; other
  system-caused failures (unknown reducer, bad args, rate limit, schedule-only) remain `Error`
  frames with their own codes.

## 4. Retry semantics (ERR-020) `[P0]`

Every entry declares `retryable`; transient conditions (rate limit, shard unavailable,
buffer-pool exhaustion, spatial rebuild, overload, handoff) are `retryable: true`, attaching
`retry_after_ms` when estimable (the RED-050 token bucket advertises its refill period). A client
retrying only when `retryable` and honoring `retry_after_ms` never worsens the condition.

## 5. SQLSTATE & HTTP derivation (ERR-030..031) `[P0]`

- **ERR-030** SQL-range entries carry a PostgreSQL-compatible SQLSTATE
  (3001→`42P01`, 3100→`23505`, 3200→`40001`, …); all others send `sqlstate: None`.
- **ERR-031** Streamable HTTP derives its status from the entry's `http_status`
  (AUTH→401, `PROTO_FRAME_TOO_LARGE`→413, `PROTO_SESSION_EXPIRED`→404, rate limits→429,
  unavailability→503, `SYS_INTERNAL`→500), preserving pre-catalog observable semantics.

## 6. Internal mapping totality (ERR-040) `[P0]`

`FluxumError::to_wire()` maps every variant onto a released entry — the exhaustive match makes an
unmapped new variant a compile error, and the adherence test pins each variant's code and checks
emitted `details` keys against the registry. `FluxumError::Query` carries catalog codes (the
HTTP-era codes are retired; `entry(400) == None`).

## 7. Amendments to earlier specs

- **SPEC-006 RPC-034**: the HTTP-compatible code table is replaced by this catalog; the `Error`
  payload is the ERR-010 shape. (Frame codes emitted by `fluxum-protocol` itself: RPC-001
  malformed → 1000, RPC-061 too-large → 1003, RPC-060 idle → 1004, RPC-007 stale session → 1005.)
- **SPEC-004 RED-060**: reducer `Err(String)` reaches the caller as the ERR-012 structured
  outcome (5001, message verbatim); RED-061 panics are 5002, never 5001.
