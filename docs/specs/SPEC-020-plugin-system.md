# SPEC-020 — Plugin System (capability registry + in-process & sidecar hosts)

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | Phase 3 (framework core) · Phase 5 (sidecar host) · Phase 4 (query hooks) · Phase 7 (CDC sink) ([DAG](../DAG.md)) |
| **PRD requirements** | FR-70/FR-32 (extends existing seams); new: FR-97 (plugin capability framework), FR-98 (out-of-process sidecar plugins) |
| **Requirement prefix** | `PLG-` |
| **Source** | New (Fluxum-native). Generalizes the existing ad-hoc extension seams (`AuthProvider` [SPEC-009](SPEC-009-authentication.md); `ColumnTransform`/codec [SPEC-017](SPEC-017-column-transforms.md); `KeyProvider` CT-037; `visibility(custom)` SUB-032) into one framework, and adds new hooks for full-text re-ranking, external-retriever fusion, and CDC. |

Keywords **MUST**, **MUST NOT**, **SHALL**, **SHOULD**, **MAY** are RFC 2119. Requirement IDs
`PLG-xxx` are stable. Priority tags: `[P0]` MVP · `[P1]` competitive launch · `[P2]` post-launch.

## 1. Scope & non-distortion contract

Fluxum keeps its core lean and its purpose narrow, but several capabilities are inherently
pluggable — auth schemes, column codecs, and (the motivating case) model-based improvements to
full-text scoring and integration with the family's **Vectorizer**. This spec defines a single
**plugin capability framework** with **two hosting modes**, so heavy or optional functionality
(models, external retrievers, CDC consumers) never bloats the core binary or image.

**This does NOT reopen the WASM/FFI non-goal** ([PRD §8](../PRD.md)). A plugin is either:

- **In-process** — a compiled Rust crate implementing a capability trait, registered at link time
  (the existing `inventory`/`linkme` registry, DM-040) and **feature-gated** by a Cargo feature, so
  it is present in the binary only when explicitly enabled. Native Rust, no dynamic loading, no FFI.
- **Out-of-process (sidecar)** — a separate process Fluxum calls over **Plugin RPC** (the FluxRPC
  framing family, `u32 LE + MessagePack`, [SPEC-006](SPEC-006-protocol-fluxrpc.md)). Heavy
  dependencies (a model runtime, a Vectorizer client) live in the sidecar; the Fluxum image is
  unaffected. Not FFI, not `dlopen` — process isolation over a wire protocol.

**The non-distortion rule (PLG-020) is the heart of this spec:** the deterministic single-writer
commit path admits only deterministic, bounded, in-process plugins; non-deterministic or sidecar
plugins are confined to the **read/query path** (ranking — snapshot-only, cannot corrupt stored
state or diff correctness) and to the **off-commit CDC path** (external side effects only). This is
what lets model-based scoring exist without breaking Fluxum's determinism, latency, or
deterministic-simulation testing ([SPEC-013](SPEC-013-testing-conformance.md)).

## 2. Capabilities

- **PLG-001** [P0] A **capability** is a typed extension point: a Rust trait plus a fixed invocation
  site plus a **placement class** (PLG-020). The v1 capability set:

  | Capability | Trait (object-safe) | Placement | Host | Status |
  |---|---|---|---|---|
  | Auth | `AuthProvider` (SPEC-009 AUTH-030) | WritePath | in-proc | existing → adopted |
  | Column codec/transform | `ColumnTransform` (SPEC-017 CT-010) | WritePath | in-proc | existing → adopted |
  | Key provider | `KeyProvider` (SPEC-017 CT-037) | WritePath | in-proc (sidecar for KMS, PLG-021) | existing → adopted |
  | Row visibility | `VisibilityFn` (SUB-032) | WritePath¹ | in-proc | existing → adopted |
  | **Full-text re-rank** | `ScoreReranker` | ReadPath | in-proc **or sidecar** | new |
  | **Retriever / fusion** | `Retriever` (+ `Fusion`) | ReadPath | in-proc **or sidecar** | new |
  | **CDC stream sink** | `StreamSink` | OffPath | in-proc **or sidecar** | new |

  ¹ visibility runs on the read/fan-out path but is a pure deterministic predicate; it is classed
  WritePath-strict (deterministic, no sidecar) because subscription correctness depends on it.

- **PLG-002** [P0] Existing seams SHALL be **adopted** as capabilities without breaking their current
  trait APIs: the framework registers `AuthProvider`/`ColumnTransform`/`KeyProvider`/`VisibilityFn`
  instances through the unified `PluginRegistry` and exposes them in introspection (PLG-050), but
  their call sites and semantics are unchanged. No destructive rewrite of the archived auth code.

- **PLG-003** [P0] The capability set is **closed**: a plugin binds one or more of the defined
  capabilities. There is no arbitrary "run any code anywhere" hook — every extension point is a
  reviewed trait with a defined placement, cost expectation, and failure policy. Adding a capability
  is a spec change.

### 2.1 New capability traits

```rust
/// ReadPath: re-score/re-order the top-K candidates of a MATCH query (SPEC-019). Snapshot-only.
pub trait ScoreReranker: Send + Sync {
    fn rerank(&self, query: &FtQuery, candidates: Vec<Scored>, ctx: &PluginCtx)
        -> Result<Vec<Scored>, PluginError>;   // returns reordered candidates
}

/// ReadPath: contribute external candidates + scores for hybrid fusion (e.g. Vectorizer).
pub trait Retriever: Send + Sync {
    fn retrieve(&self, query: &FtQuery, k: usize, ctx: &PluginCtx)
        -> Result<Vec<Scored>, PluginError>;   // (primary_key, score) from the external retriever
}
// Fusion of lexical (BM25) and retriever result lists. Default impl: Reciprocal Rank Fusion.
pub trait Fusion: Send + Sync {
    fn fuse(&self, lexical: &[Scored], dense: &[Scored], ctx: &PluginCtx) -> Vec<Scored>;
}

/// OffPath: receive committed deltas from the commit log, off the commit path (CDC).
pub trait StreamSink: Send + Sync {
    fn on_commit(&self, batch: &CommitBatch) -> Result<(), PluginError>;  // at-least-once (PLG-041)
    fn checkpoint(&self) -> Offset;                                       // resume point
}
```

## 3. Placement & the commit-path guarantee

- **PLG-020** [P0] Each capability has a **placement class** governing where it runs and what it may
  do. `ServerBuilder::build()` SHALL reject a plugin binding that violates its class:

  | Class | Runs on | Determinism | Sidecar allowed? | Failure policy |
  |---|---|---|---|---|
  | **WritePath** | inside the reducer/commit transaction | MUST be deterministic + bounded | **No** (in-proc only) | error → transaction rollback (PLG-030) |
  | **ReadPath** | query/`InitialData`/one-off, after base evaluation | MAY be non-deterministic | Yes | timeout/error → **fallback to base result** (PLG-031) |
  | **OffPath** | asynchronously, fed by the commit log | side effects only, never feeds back into state | Yes | lag/error → buffer, then drop + metric; never stalls commit (PLG-041) |

- **PLG-021** [P0] A **sidecar** plugin MUST NOT bind a WritePath capability — a network round-trip on
  the single-writer commit path would break latency (NFR-11) and determinism. (`KeyProvider` to an
  external KMS is the one WritePath capability permitted a sidecar, and only if the runtime caches
  keys so the commit path makes no per-transaction network call.)

- **PLG-022** [P0] Non-deterministic plugins (models) are structurally confined so they cannot
  corrupt correctness: `ScoreReranker`/`Retriever` affect only the **order** of a snapshot result
  (SUB-013 already forbids ranking on live diffs, so a model never touches `TxUpdate` correctness);
  `StreamSink` produces only external side effects. Stored rows, indexes, and diff evaluation are
  never a function of plugin output. The deterministic-simulation suite (SPEC-013) runs with sidecar
  plugins disabled or replaced by deterministic stubs.

## 4. Hosting

### 4.1 In-process (compiled, feature-gated)

- **PLG-030** [P0] An in-process plugin SHALL be a Rust crate implementing a capability trait,
  registered via the link-time registry and enabled by a Cargo feature; it is absent from the binary
  unless its feature is on. It runs under `catch_unwind` isolation (like reducers, RED-004): a panic
  disables the plugin and increments `fluxum_plugin_panics_total`, and — for a WritePath plugin —
  rolls back the enclosing transaction rather than crashing the shard.

### 4.2 Out-of-process (sidecar over Plugin RPC)

- **PLG-031** [P0] A sidecar plugin SHALL be a separate process reachable at a configured endpoint.
  The runtime hosts a generic **proxy** that implements the capability trait by issuing a Plugin RPC
  call (FluxRPC framing, SPEC-006) to the sidecar. Each ReadPath/OffPath call SHALL have a
  configurable **timeout**; on timeout, error, or unavailability the runtime SHALL **degrade
  gracefully** — a `ScoreReranker`/`Retriever` failure yields the base BM25 result (SPEC-019
  FTS-040), never an error to the client — and open a **circuit breaker** after repeated failures,
  emitting `fluxum_plugin_sidecar_errors_total{plugin, reason}`. A sidecar SHALL authenticate to
  Fluxum and MAY be granted a server-peer identity (AUTH-062) if it needs RLS bypass; by default it
  does not bypass RLS.

### 4.3 Manifest & validation

- **PLG-032** [P0] Plugins SHALL be declared in `config.yml`; `ServerBuilder::build()` SHALL validate
  every binding (capability exists, placement legal for the host, referenced tables/columns/queries
  exist, feature compiled in for in-proc) and abort startup on any violation:

  ```yaml
  plugins:
    - name: ft_reranker
      capability: score_reranker
      host: { kind: sidecar, endpoint: "127.0.0.1:15810", timeout_ms: 40 }
      applies_to: { tables: [Item], columns: [description] }
    - name: vectorizer_hybrid
      capability: retriever            # + fusion (default RRF)
      host: { kind: sidecar, endpoint: "127.0.0.1:15811", timeout_ms: 60 }
      applies_to: { tables: [Item] }
    - name: vectorizer_ingest
      capability: stream_sink          # CDC → Vectorizer embedding pipeline
      host: { kind: sidecar, endpoint: "127.0.0.1:15811" }
      applies_to: { tables: [Item], columns: [name, description] }
    - name: audit_codec
      capability: column_transform     # in-proc, feature-gated
      host: { kind: in_process, feature: "plugin-audit" }
  ```

## 5. Query-path hooks (full-text re-rank & hybrid fusion)

- **PLG-040** [P1] When a `ScoreReranker` is bound to a `MATCH` query's table/column
  ([SPEC-019](SPEC-019-fulltext-search.md) FTS-040), the runtime SHALL evaluate BM25 to a candidate
  top-K (`K ≥ LIMIT`, configurable `rerank_candidate_k`, default 100), pass the candidates to the
  reranker, and return its order — truncated to `LIMIT`. On reranker failure/timeout the BM25 order
  stands (PLG-031). Re-ranking applies to `InitialData`/one-off only (SUB-013).

- **PLG-041** [P1] When a `Retriever` is bound, the runtime SHALL request its top-K, combine it with
  the BM25 list via the `Fusion` capability (default **Reciprocal Rank Fusion**, no score-scale
  normalization needed), and return the fused top-N. This is the sanctioned **hybrid retrieval**
  contract with Vectorizer; the dense/model half lives entirely in the sidecar. On retriever
  failure the lexical BM25 result stands.

## 6. CDC stream sink

- **PLG-050** [P1] A `StreamSink` SHALL be fed committed deltas from the **commit log**, off the
  commit path, reusing the replication stream substrate ([SPEC-014](SPEC-014-replication.md)):
  delivery is **at-least-once** with a persisted per-sink **offset checkpoint** so a restarted sink
  resumes without loss. A slow or failed sink SHALL be bounded by a buffer with a drop policy
  (`fluxum_plugin_sink_lag`, drop past threshold) and MUST NOT stall or back-pressure commits
  (the non-blocking guarantee of SUB-041 applies). This is the substrate for Vectorizer ingestion
  (embed changed rows) and generic external integrations.

## 7. Introspection & security

- **PLG-060** [P0] `GET /plugins` (HTTP admin) SHALL list active plugins: name, capability, host
  (in-proc feature | sidecar endpoint), placement, health/circuit state, and applies-to scope —
  never secrets. Plugin identities and sidecar endpoints SHALL NOT leak key material or tokens.

- **PLG-061** [P0] Plugins SHALL NOT widen the security model implicitly: a plugin runs with no more
  privilege than configured; RLS bypass requires an explicit server-peer grant (AUTH-062); a sidecar
  connection is authenticated like any peer (SPEC-009). A misbehaving plugin SHALL be isolatable
  (disable via config + hot circuit-break) without restarting the core.

## 8. Acceptance criteria

1. **Capability registry & adoption (PLG-001/002):** `AuthProvider`, `ColumnTransform`,
   `KeyProvider`, and `visibility(custom)` all appear in `GET /plugins` as capabilities with
   unchanged behavior; a new capability cannot be bound without a matching trait.
2. **Placement enforcement (PLG-020/021):** binding a sidecar host to a WritePath capability aborts
   `build()` with a descriptive error; binding an in-proc feature that is not compiled aborts; a
   legal set starts cleanly.
3. **In-proc isolation (PLG-030):** a panicking in-proc plugin is disabled and metered; a WritePath
   plugin error rolls back its transaction with no partial writes (joint with RED-004).
4. **Sidecar graceful degradation (PLG-031/040/041):** with a `ScoreReranker`/`Retriever` sidecar
   stopped or timing out, `MATCH` queries still return the pure-BM25 result within the query budget,
   the circuit breaker opens, and `fluxum_plugin_sidecar_errors_total` increments — no client error.
5. **Hybrid fusion (PLG-041):** for a known corpus, RRF of the BM25 list and a stub retriever list
   yields the exact fused order a reference RRF implementation produces; disabling the retriever
   falls back to BM25 order.
6. **Determinism containment (PLG-022):** the deterministic-simulation suite (SPEC-013) passes with a
   non-deterministic reranker stub bound — stored state, indexes, and `TxUpdate` diffs are
   bit-identical to a run with no plugin (only snapshot ordering differs).
7. **CDC at-least-once (PLG-050):** a `StreamSink` fed a stream of commits receives every committed
   delta at least once; killing and restarting the sink resumes from the checkpoint with no missed
   commit; a stalled sink never blocks commit throughput and is dropped past the lag threshold.
8. **Introspection & security (PLG-060/061):** `/plugins` reports state without secrets; a plugin
   without a server-peer grant cannot read rows hidden by `#[visibility]`; a plugin is disabled via
   config + circuit break without a core restart.
