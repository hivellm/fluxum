# SPEC-013 — Testing & Conformance

| | |
|---|---|
| **Status** | Draft |
| **Phase / tasks** | cross-cutting · T0.1, T2.7, T4.5, T6.3, T6.5, T6.6, T7.1–T7.7 ([DAG](../DAG.md)) |
| **PRD requirements** | NFR-01–NFR-14 · FR-112 |
| **Requirement prefix** | `TST-` |
| **Source** | new — consolidates DAG gate criteria and UzDB acceptance criteria |

This spec is the acceptance machinery for every other spec: it defines the test taxonomy, the
per-PR quality gates, and the named suites that the [DAG](../DAG.md) gates (G0–G7) reference.
A gate **MUST NOT** be declared green unless every suite mapped to it in
[§16](#16-acceptance-criteria) passes.

Repository layout: unit tests inline per crate; integration tests in `crates/<crate>/tests/`;
criterion benches in `crates/<crate>/benches/`; cross-SDK conformance and injection corpora as
data files under `tests/conformance/`; crash-harness, load-test, profile, soak, and
replication-drill drivers under `tests/`; the deterministic-simulation suites and their
runtime abstraction as their own crate, `crates/fluxum-dst` (§14); the process-level
smoketest harness under `tests/smoketests/` with its guard utility in `crates/fluxum-testing`
(§15); the PostgreSQL parity harness as its own crate, `crates/fluxum-bench`.

## 1. Test taxonomy & tooling

| Layer | Tooling | Location | Blocking gate |
|---|---|---|---|
| Unit tests | `#[cfg(test)]` modules, cargo-nextest | inline per crate | every PR (G0+) |
| Integration tests | cargo-nextest | `crates/*/tests/` | every PR (G0+) |
| Property tests | proptest | inline + `crates/*/tests/` | G1, G2, G4 |
| Proc-macro golden tests | trybuild | `crates/fluxum-macros/tests/` | G1 |
| Crash & durability suite | custom kill-harness | `tests/crash/` | G2, then nightly |
| Subscription property suite | proptest + simulated clients | `crates/fluxum-core/tests/` | G4 |
| Protocol fuzzing & conformance | proptest + corpus runners | `tests/conformance/` | G5, G6 |
| Benchmarks | criterion (`harness = false`) | `crates/*/benches/` | G6 (report); per-PR smoke |
| End-to-end demo | scripted CI scenario | `tests/demo/` | G6 |
| Security audit | written checklist + suites | `tests/security/` | G6 |
| PostgreSQL parity harness | workload drivers + report generator | `crates/fluxum-bench` | G6, G7 (release report); CI ratio guard |
| SIMD scalar-parity suite | proptest across ISA matrix | inline + `crates/*/tests/` | per-PR (kernel changes); nightly full matrix |
| Tiered-storage & droplet profile | cgroup-limited CI profile | `tests/profile/` | G2, then nightly |
| Billion-row soak | soak driver + report | `tests/soak/` | G7 |
| Replication & backup drills | multi-process drill harness | `tests/replication/` | G7; backup/PITR round-trip in CI |
| Deterministic simulation (DST) | seeded sim runtime + model oracle + fault injection | `crates/fluxum-dst` | G2 (storage/commitlog), G7 (replication); nightly long runs |
| Process-level smoketests | guard harness spawning the real server binary | `tests/smoketests/` + `crates/fluxum-testing` | G6; restart/persistence drills at G2 |

- **TST-001** [P0] Unit tests **MUST** live inline in `#[cfg(test)]` modules next to the code
  they exercise; integration tests **MUST** live in `crates/<crate>/tests/`. Test-only code
  **MUST NOT** appear in production modules outside `#[cfg(test)]`.
- **TST-002** [P0] **cargo-nextest** is the canonical test runner: CI **MUST** execute
  `cargo nextest run --workspace` (plus `cargo test --doc` for doctests). All tests **MUST**
  pass under nextest's default parallel execution — no dependence on test ordering or shared
  mutable global state.
- **TST-003** [P0] Property-based tests **MUST** use **proptest**. Every property suite **MUST**
  persist failing seeds (`proptest-regressions/` committed to the repository) so counterexamples
  become permanent regression tests.
- **TST-004** [P1] Benchmarks **MUST** use **criterion** with `harness = false` declared in the
  crate manifest, under `crates/<crate>/benches/`. No `#[bench]`/nightly-harness benchmarks.
- **TST-005** [P1] `fluxum-macros` **MUST** carry **trybuild** golden tests: compile-pass cases
  covering every attribute of the canonical example schema (`#[fluxum::table]` with
  `#[primary_key]`, `#[auto_inc]`, composite `primary_key(a, b)`, `#[index(btree(...))]`,
  `#[spatial]`, `#[visibility]`, `partition_by`, `#[fluxum::reducer]`, `#[fluxum::tick]`,
  `#[fluxum::schedule]`, lifecycle hooks, `#[fluxum::migration]`), and compile-fail (UI) cases
  with pinned diagnostics for misuse (missing primary key, duplicate `#[auto_inc]`, visibility
  rule naming a non-existent field, invalid `max_rate` string, …).
- **TST-006** [P0] CI **MUST** run the full fmt + clippy + nextest pipeline on a matrix of
  **Linux, macOS, and Windows** for every PR; a PR **MUST NOT** merge unless all three platforms
  are green (NFR-09).
- **TST-007** [P1] Every named suite in this spec **MUST** be runnable locally with a single
  documented command. CI-only tests are not accepted.

## 2. Quality gates (NFR-09)

- **TST-010** [P0] `cargo fmt --check` **MUST** pass on every PR.
- **TST-011** [P0] `cargo clippy --workspace --all-targets -- -D warnings` **MUST** pass on every
  PR. Workspace lints **MUST** deny `clippy::unwrap_used`, `clippy::expect_used`, and
  `clippy::undocumented_unsafe_blocks`. Production code **MUST NOT** carry `#[allow]` overrides
  for these lints; test and bench code **MAY** relax `unwrap_used`/`expect_used` only via a
  scoped `#[allow]` inside `#[cfg(test)]` or bench modules.
- **TST-012** [P0] **Zero `#[ignore]` growth**: CI **MUST** enforce a committed baseline count of
  ignored tests; the count **MUST NOT** increase, and every `#[ignore]` **MUST** reference a
  tracking issue in its attribute comment. Shrinking the baseline is always allowed.
- **TST-013** [P0] Deleting or weakening a test that a gate in [§16](#16-acceptance-criteria)
  depends on **MUST** update this spec and the [DAG](../DAG.md) in the same PR; a gate **MUST
  NOT** be weakened without a PRD change (DAG §5 change control).

## 3. Crash & durability suite (T2.7)

The crash suite validates FR-13 and NFR-06/NFR-08 against `CommitLog` + `SnapshotRepo`
([SPEC-002](SPEC-002-storage-engine.md)).

- **TST-020** [P0] **kill -9 harness**: a driver process runs a randomized workload of the
  canonical reducers (small writes, deletes, upserts) while a supervisor kills it with `SIGKILL`
  (or the Windows equivalent of immediate process termination) at **every instrumented commit
  boundary**: (a) before the commit-log append, (b) mid-entry (partial write), (c) after the
  append but before the client acknowledgment, (d) after the acknowledgment, (e) during a
  snapshot write, (f) during commit-log segment rotation. After each kill the harness **MUST**
  restart the process, run recovery, and verify the invariants below. The full boundary matrix
  **MUST** be exercised on every suite run.
- **TST-021** [P0] **Zero committed-transaction loss invariant**: every transaction whose
  commit-log append completed **MUST** be present after recovery, and recovery **MUST NOT**
  surface any partial transaction. Transactions still in the async writer buffer at kill time
  **MAY** be lost, but only atomically (all-or-nothing per transaction) and only within the
  NFR-08 window (< ~50 ms of writes).
- **TST-022** [P0] **CRC bit-flip drills**: the harness **MUST** flip random bits in the length
  prefix, payload, and CRC32 field of randomly chosen log entries (head, middle, and tail
  positions). Recovery **MUST** detect the corruption via CRC32, truncate the log at the first
  corrupt entry (FR-13), log the truncation offset, and **MUST NOT** panic or replay any data
  from at or beyond the corrupt entry.
- **TST-023** [P0] **Torn-tail truncation sweep**: the harness **MUST** truncate the commit log
  at every byte offset within its final entry (including inside the length prefix and inside the
  CRC). Recovery **MUST** treat every such truncation as a clean end-of-log and open successfully
  with all prior entries intact.
- **TST-024** [P0] **10 GB recovery benchmark**: cold restart (latest snapshot + log replay) of a
  10 GB commit log **MUST** complete to serving state in **< 30 s** (NFR-06) on the reference
  runner. The measurement **MUST** record CPU model, RAM, disk class, and OS. This benchmark runs
  nightly and at G2/G6, not per PR.
- **TST-025** [P0] **Post-recovery equivalence**: after every harness iteration, the recovered
  `CommittedState` **MUST** be logically equal to the pre-crash committed state for all tables
  (row-set equality per table), and all secondary B-tree and spatial indexes **MUST** be
  consistent with the recovered rows (verified against a brute-force rebuild).
- **TST-026** [P1] The crash suite **MUST** run on Linux and Windows CI (file-system and
  process-termination semantics differ); macOS **MAY** run a reduced iteration count.

## 4. Subscription correctness property suite (T4.5)

The property suite validates NFR-10 against the `SubscriptionManager` and row-level security
([SPEC-005](SPEC-005-subscriptions.md)).

- **TST-030** [P0] Each run **MUST** apply at least **10,000 random mutations** (inserts,
  updates, deletes across all canonical tables, executed through reducers) against a randomized
  population of simulated clients (at least 8), each holding 1–8 randomized subscriptions drawn
  from: plain `WHERE` predicates, `IN REGION` / `WITHIN RADIUS` spatial predicates, and tables
  with `#[visibility(owner_only(...))]`. After every commit's fan-out, **every simulated client
  cache** (`InitialData` plus applied `TxUpdate` diffs) **MUST** be exactly equal to the server's
  visible state for that client's identity — 100% diff accuracy, no tolerance.
- **TST-031** [P0] The mutation/subscription generator **MUST** cover: `Subscribe`,
  `SubscribeSingle`, and `Unsubscribe` issued mid-run; client disconnect and reconnect with a
  fresh `InitialData` resync that **MUST** converge to the same cache; overlapping subscriptions
  on the same table; rows moving into and out of spatial regions; and ownership changes on
  `owner_only` tables (the row **MUST** appear/disappear in the affected clients' caches).
- **TST-032** [P0] The suite **MUST** use a seeded RNG with deterministic replay: any failure
  **MUST** shrink to a minimal counterexample and its seed **MUST** be committed as a regression
  case (TST-003).
- **TST-033** [P1] **Slow-consumer stress test**: with a sustained mutation workload and ≥ 100
  subscribed clients, one client stops reading its socket. The test **MUST** assert: the client's
  send buffer transitions Normal → Pressured → Full and the drop policy of
  [SPEC-005](SPEC-005-subscriptions.md) is applied; `fluxum_subscriber_drops_total` increments;
  all other clients' fan-out latency stays within the NFR-04 bound; and server memory remains
  bounded (no unbounded queue growth).
- **TST-034** [P0] The property suite **MUST** run in CI on every PR (a scaled-down iteration
  count **MAY** be used per PR, with the full 10,000-mutation run nightly and at G4). G4 **MUST
  NOT** pass on the scaled-down run.

## 5. Reducer runtime suites

These suites validate FR-11, FR-21, FR-24, FR-25, and FR-80
([SPEC-004](SPEC-004-reducers.md), [SPEC-010](SPEC-010-schema-migration.md)).

- **TST-040** [P0] **Rollback-on-error**: a reducer that performs writes through `TxHandle` and
  then returns `Err` **MUST** leave no observable state change: no rows visible to subsequent
  reads, no commit-log entry appended, no `TxUpdate` emitted, and the reducer's error message
  propagated to the caller.
- **TST-041** [P0] **Rollback-on-panic injection**: panics **MUST** be injected at randomized
  points inside reducers (before any write, between writes, after the last write) and inside
  lifecycle hooks. Each panic **MUST** result in: `TxState` discarded (full rollback), an error
  returned to the caller, and the shard continuing to serve subsequent reducer calls — the shard
  process **MUST NOT** terminate. A soak variant **MUST** verify that repeated panics do not leak
  memory or degrade throughput.
- **TST-042** [P0] **`#[tick]` drift timing tests**: a `#[fluxum::tick(rate = N)]` reducer
  **MUST** be validated against absolute clock targets: over a measured run the executed tick
  count **MUST** match wall-clock expectation within tolerance (no cumulative drift); an injected
  delay **MUST** produce a missed-tick log entry; a delay exceeding 3× the tick period **MUST**
  trigger a drift reset (re-anchoring to the current time with no burst catch-up), per FR-21.
- **TST-043** [P0] **Rate-limit conformance**: for `max_rate = "N/s"`, N calls within one second
  from one identity **MUST** all succeed and call N+1 **MUST** be rejected with error 429
  **before** any `TxState` is created (asserted via storage instrumentation). Buckets **MUST** be
  independent per `(Identity, reducer)`: a second identity and a second reducer are unaffected.
  Token refill **MUST** restore capacity within one period.
- **TST-044** [P1] **Migration before/after tests**: starting from a persisted store at schema
  v1, booting a module at schema v2 **MUST** be tested for: an additive change (new column with
  default) auto-applying with existing rows readable; a `#[fluxum::migration(version = N)]`
  transform running exactly once with data preserved and `__schema_meta__` advanced; an
  incompatible change without a migration aborting startup with a diagnostic naming the table and
  column; and a second boot of the migrated store being a no-op (idempotence).

## 6. Protocol conformance

These suites validate FR-40, FR-41, and FR-45 ([SPEC-006](SPEC-006-protocol-fluxrpc.md)) and the
generated SDKs ([SPEC-011](SPEC-011-sdk-codegen.md)).

- **TST-050** [P0] **FluxBIN roundtrip property tests** for every supported type: all integer
  widths, `f32`/`f64` (including NaN, ±infinity, and −0.0 per the encoding rules of SPEC-006),
  `bool`, `String` (arbitrary Unicode), `Vec<u8>`, `Option<T>`, `Vec<T>`, nested product and sum
  types, and the `Identity` / `ConnectionId` / `EntityId` / `Timestamp` newtypes.
  `decode(encode(v)) == v` **MUST** hold for all generated values, and decoding **arbitrary byte
  strings MUST NOT panic** — it returns either a valid value or a decode error.
- **TST-051** [P0] **Frame fuzzing**: the FluxRPC frame parser (`u32 LE length + MessagePack`)
  **MUST** be fuzzed with: length prefix 0; lengths smaller than the minimal envelope; exactly
  the max frame size; max + 1 (rejected per FR-45); `u32::MAX`; truncated payloads; multiple
  pipelined frames in one read; and garbage MessagePack envelopes. The parser **MUST** never
  panic and **MUST NOT** allocate the declared frame length before validating it against the
  configured maximum.
- **TST-052** [P1] **Cross-SDK conformance corpus**: a versioned, declarative scenario corpus in
  `tests/conformance/` (connect, authenticate, subscribe, call reducers, expected `InitialData`
  and `TxUpdate` contents) **MUST** be executed by runners for the **Rust** (`fluxum-sdk`),
  **TypeScript**, and **C++** clients against the same server build, with identical observable
  results required from all three. Each SDK is **release-blocked** until the corpus passes. New
  cases may be added freely; changing an expected value requires the same review bar as a wire
  format change.
- **TST-053** [P1] **Schema golden file**: the `GET /schema` JSON for the canonical example
  schema **MUST** be committed as a golden file; any diff fails CI and requires explicit review
  (this enforces the module API freeze at T6.1). `fluxum schema export` output **MUST** be
  byte-identical to the endpoint response.
- **TST-054** [P0] **Wire-freeze enforcement**: after G5, any change to FluxRPC framing, message
  types, or FluxBIN encoding **MUST** bump the protocol version and add conformance cases
  exercising both the old and new versions; the corpus is pinned per protocol version.

## 7. Performance & load suite (T6.6)

Targets are the PRD §7 NFRs and §10 success metrics. All measured results **MUST** record
hardware (CPU model, RAM, OS) in the committed report.

- **TST-060** [P0] **Load test**: ≥ **100,000 reducer calls/s sustained on one shard** for at
  least 60 s (NFR-01), using the high-frequency small-write reducer (`update_reading`) driven
  over loopback FluxRPC by concurrent trusted-peer connections. Throughput **MUST** be measured
  via the `fluxum_reducer_calls_total` delta, with zero errored calls. The load report is a G6
  deliverable.
- **TST-061** [P0] **Fan-out latency**: with 1,000 concurrent subscribers holding overlapping
  subscriptions, `TxUpdate` delivery p99 (commit time to client receipt) **MUST** be **< 5 ms**
  (NFR-04).
- **TST-062** [P0] **Loopback round-trip**: `ReducerCall` p99 over loopback TCP, measured
  client-side, **MUST** be **< 0.5 ms** (NFR-05).
- **TST-063** [P1] **Criterion micro-benchmarks** with committed baselines **MUST** track:
  `CommittedState` primary-key lookup < 1 µs (NFR-02); reducer commit p99 < 1 ms with the async
  log (NFR-03); and the 1M-point spatial query ≥ 10× faster than the equivalent full scan
  (T2.6 exit test).
- **TST-064** [P1] **Regression guard**: every PR **MUST** run a scaled-down smoke bench of the
  hot paths with a **±20% fence** against the committed baselines; a breach fails the PR.
  Full-size benches run nightly on the reference runner. Baseline updates **MUST** be explicit,
  reviewed commits — never automatic.

## 8. End-to-end demo application (T6.5)

The demo app is the DX acceptance vehicle for FR-82 and the PRD §10 SDK type-safety metric.

- **TST-070** [P0] A demo application implementing **chat + presence + per-user tasks** on the
  canonical example schema **MUST** run end-to-end on the **generated TypeScript SDK**. The CI
  pipeline **MUST**: build the server with the demo module, boot it, run
  `fluxum generate --lang typescript`, compile the generated SDK and demo with **zero manual
  stubs**, and execute the scripted scenario.
- **TST-071** [P0] The scripted scenario **MUST** cover, asserting on **client caches** (not
  server introspection): two or more clients authenticate; presence via `on_connect` /
  `on_disconnect` (each client observes the other appear in and disappear from `OnlineUser`);
  `send_chat` messages fan out to all subscribed clients, and exceeding its `max_rate` returns
  error 429 to the offending client only; `Task` rows are visible **only to their owner** (client
  A never observes client B's tasks in any message); `complete_task` updates propagate to the
  owner's cache.
- **TST-072** [P1] The scenario **MUST** complete with **zero runtime type errors** in the
  generated SDK: TypeScript compiled in strict mode, no `any` in the generated code, and no
  decode failures at runtime (PRD §10 SDK type-safety metric).

## 9. Security audit checklist (T6.6)

- **TST-080** [P0] **Auth bypass**: automated tests **MUST** assert that `ReducerCall`,
  `Subscribe`, and `OneOffQuery` sent before a successful `Authenticate` are rejected on both TCP
  and Streamable HTTP transports; invalid and expired credentials are rejected per provider (`token`,
  `jwt`); the `none` provider refuses non-loopback connections; and `Identity` is derived
  server-side (`SHA-256(token)`) — any client-supplied identity field is ignored.
- **TST-081** [P0] **RLS bypass matrix**: for `owner_only` tables, the matrix {owner, non-owner,
  server-peer} × {`InitialData`, `TxUpdate` diffs, `OneOffQuery`, HTTP `POST /query`} **MUST**
  be tested. A non-owner **MUST NOT** receive the row's data **nor any delete/update notification
  that leaks its existence**. The RLS bypass **MUST** apply only to server-peer identities
  (`SHA-256("SERVER:" + name)`).
- **TST-082** [P0] **Subscription-compiler injection corpus**: a committed corpus of malicious
  subscription strings (quote escapes, comment sequences, stacked statements, `UNION`-style
  constructs, oversized and deeply nested predicates) **MUST** be rejected by the SQL-subset
  compiler with parse errors — a malformed query **MUST** never compile to a plan broader than
  its syntactically valid interpretation. The corpus lives in `tests/conformance/injection/` and
  **MUST** grow with every new finding.
- **TST-083** [P0] **Frame-size DoS**: the server **MUST** reject an oversized declared frame
  length before allocating for it (TST-051); partial-frame slowloris connections **MUST** be
  closed by the idle timeout; rapid connect/disconnect churn and subscription bombs (thousands of
  subscriptions from one client) **MUST** leave memory bounded and other clients serviced.
- **TST-084** [P1] The security audit (auth bypass, RLS bypass, SQL injection, frame-size DoS)
  **MUST** be executed as a written checklist at T6.6 with a committed report; **zero open P0
  findings** is required for the 0.1.0 release (G6).

## 10. PostgreSQL parity harness (T6.3)

The parity harness is the permanent comparative baseline of [PRD §2.1](../PRD.md): every
performance claim Fluxum makes is measured against a functionally identical application on the
incumbent app-server + PostgreSQL stack (NFR-11). It lives in `crates/fluxum-bench`, is permanent
infrastructure (not a one-off), and its comparative report is a release artifact from G6
(report v1) onward.

- **TST-090** [P0] **Parity application**: `fluxum-bench` **MUST** implement the *same*
  application twice — (a) as a Fluxum module (reducers + subscriptions) and (b) on a
  representative app-server + PostgreSQL stack, with the same schema, the same operations, and
  the same client behavior. The baseline stack selection is tracked by OQ-9 (axum + sqlx vs
  Node/Express + pg — possibly both); a **SQLite variant MUST be included where applicable**.
  Both implementations and all of their configuration live in the repository.
- **TST-091** [P0] **Honesty rules** — a comparative run is valid only if: both sides run on
  **equal hardware** (same machine or identical instances, recorded in the report); the
  PostgreSQL side is **competently tuned** — indexes covering every benchmark query, prepared
  statements, and a connection pool; **durability settings are documented on both sides** —
  Fluxum's async commit log (and its NFR-08 loss window) stated against the PostgreSQL
  `synchronous_commit` setting used; every workload performs a documented **warmup** before
  measurement; and every result is the product of **multiple runs with variance reported**.
- **TST-092** [P0] **Workload matrix** — the harness **MUST** measure at minimum:
  (a) **write throughput**: the high-frequency small-write reducer vs the equivalent
  HTTP → app-server → SQL update path; (b) **end-to-end change→subscriber p99 latency**:
  commit-to-client-receipt for Fluxum `TxUpdate` vs PostgreSQL `LISTEN/NOTIFY` + app-server
  push fan-out; (c) **hot read latency**: buffer-pool-resident lookup vs a SQL round trip
  on a cached page; (d) **cold read latency**: cold-tier page fault-in vs PostgreSQL reading an
  uncached page; (e) a **mixed workload** combining writes, reads, and live subscribers.
- **TST-093** [P0] **Targets (NFR-11)**: write throughput ≥ **10×** the baseline; end-to-end
  change→subscriber p99 ≥ **10×** lower; hot reads ≥ **50×** lower latency; cold reads within
  **2×** of PostgreSQL.
- **TST-094** [P0] **Versioned report artifact**: the comparative report **MUST** be published
  with **every release** (report v1 at G6 / 0.1.0; report v2 at G7 / 0.2.0). The report records
  hardware (CPU model, RAM, disk class, OS), the versions and full configurations of both
  stacks, raw measurements, the NFR-11 ratios, and per-workload variance.
- **TST-095** [P0] **Ratio regression guard**: CI **MUST** compare current parity ratios against
  the previous release's published report; a regression of any NFR-11 ratio beyond the
  documented tolerance **fails CI**. The comparison baseline advances only when a new release
  publishes its report — never automatically (same review bar as TST-064 baselines).
- **TST-096** [P1] **Reproducibility**: the harness, both parity applications, and all tuning
  configurations **MUST** be runnable by a third party with one documented command per side
  (mitigates the "unfair to Postgres" contestation risk of PRD §11).

## 11. SIMD scalar-parity suite (FR-112, NFR-14)

Validates the scalar-parity rule of [SPEC-016](SPEC-016-hardware-adaptivity.md): SIMD kernels
are an optimization, never a behavior change.

- **TST-100** [P0] Every SIMD kernel (CRC32, hashing, FluxBIN batch encode/decode, batched
  predicate evaluation, page compression — the kernel inventory of
  [SPEC-016](SPEC-016-hardware-adaptivity.md)) **MUST** have a scalar reference implementation
  and a property test asserting **bit-identical** output between the SIMD kernel and its scalar
  reference on randomized inputs, including boundary shapes: empty input, single element,
  lengths that are not a multiple of the SIMD lane width, and maximum batch sizes. A kernel
  without a passing parity test **MUST NOT** be reachable from the runtime dispatcher.
- **TST-101** [P0] **ISA matrix in CI (NFR-14)**: the parity suite **MUST** run green on
  **x86-64 with AVX-512, AVX2, and SSE4.2** dispatch levels, on **aarch64 with NEON**, and with
  dispatch **forced to scalar** — each leg independently. An ISA level not natively available on
  a hosted runner **MUST** still be covered (emulation such as Intel SDE, or a dedicated
  runner); silently skipping a matrix leg is not permitted.
- **TST-102** [P1] **Dispatch forcing**: forcing each dispatch level via configuration **MUST**
  demonstrably select the intended kernel (asserted via instrumentation or the logged effective
  values of FR-113) and produce results identical to every other level.
- **TST-103** [P1] Parity failures **MUST** shrink and persist their seeds per TST-003. The
  parity suite **MUST** run on every PR that touches a kernel or the dispatch layer; the full
  ISA matrix runs nightly.

## 12. Tiered-storage & droplet profile suites (NFR-12, NFR-13)

Validates the memory-envelope and capacity pillars against
[SPEC-015](SPEC-015-tiered-storage.md).

- **TST-110** [P0] **10×-budget dataset correctness (small-droplet profile)**: a CI profile
  constrained to **1 vCPU / 512 MB** (cgroup-enforced, per NFR-12) **MUST** load a dataset at
  least **10× the memory budget** and serve the full workload correctly — reducer writes,
  primary-key and secondary-index reads spanning hot and cold pages, and live subscriptions —
  with results row-set-equal to an unconstrained reference run. G2 **MUST NOT** pass without
  this suite green.
- **TST-111** [P0] **Budget-enforcement assertions**: throughout TST-110 (and the soak of
  TST-112) buffer-pool gauges and process RSS **MUST** be sampled continuously, and no sample
  may exceed the configured `memory.budget` plus the documented allowance of
  [SPEC-015](SPEC-015-tiered-storage.md); eviction **MUST** be observed engaging under
  pressure; idle baseline RSS **MUST** be < 100 MB (NFR-12).
- **TST-112** [P1] **Billion-row soak (T7.7, NFR-13)**: a sharded + tiered deployment **MUST**
  sustain **≥ 1 billion rows** with continuous writes and live subscriptions for the documented
  soak duration; memory **MUST** stay within budget on every shard throughout; the run **MUST**
  produce a committed report artifact (row count, duration, sustained throughput, memory
  high-water marks, hardware). This soak is a G7 exit criterion.

## 13. Replication & backup drills (T7.1–T7.3)

Validates FR-100–FR-105 against [SPEC-014](SPEC-014-replication.md).

- **TST-120** [P1] **Failover drill (T7.2)**: with a replica set (one primary, ≥ 2 replicas) in
  **semi-synchronous** mode under sustained reducer load, the harness kills the primary with
  immediate process termination. The drill **MUST** assert: a replica is elected primary
  (consensus per [SPEC-014](SPEC-014-replication.md)); **zero committed-transaction loss** —
  every transaction acknowledged under semi-sync is present on the new primary; and SDK clients
  **reconnect and resubscribe** automatically, their caches reconverging to the server's
  visible state (reusing the TST-030 equivalence check).
- **TST-121** [P1] **Replica convergence (T7.1)**: a brand-new replica **MUST** converge via
  **full sync** (checkpoint transfer) and a lagging replica via **partial sync** (commit-log
  offset), each ending row-set-equal to the primary across all tables; replication offset and
  lag **MUST** be observable via `/health` and the `fluxum_replication_*` metrics during the
  test (FR-105).
- **TST-122** [P1] **Backup / restore / verify + PITR round-trip (T7.3)**: in CI,
  `fluxum backup create` **MUST** run against a server under live writes without stalling
  writers; `fluxum backup verify` **MUST** pass on the produced artifact; `fluxum backup
  restore` **MUST** yield a state row-set-equal to the backup point; and a **PITR** run **MUST**
  replay archived commit-log segments to a target timestamp / `tx_id` and match the known
  historical state at that point exactly.

## 14. Deterministic simulation testing (DST)

The DST suites validate the storage engine, commitlog, and replication layers
([SPEC-002](SPEC-002-storage-engine.md), [SPEC-014](SPEC-014-replication.md)) under a seeded,
fully deterministic runtime, complementing the OS-level crash harness of §3 (which remains the
non-simulated backstop). *(section adopted from SpacetimeDB analysis, file 09 — their `dst`
crate exists but is engine-only and not in CI; Fluxum goes further by covering replication and
gating CI on it.)*

- **TST-130** [P0] **Deterministic runtime seam**: the workspace **MUST** provide a runtime
  abstraction (`crates/fluxum-dst` plus a `simulation` feature on the runtime crate) over
  time, task scheduling, randomness, and file/network IO, such that storage, commitlog, and
  replication code runs **unmodified** on either the production executor (tokio) or a seeded
  deterministic simulated executor with simulated time. The simulator **MUST** maintain a
  determinism log of checkpoints and **MUST** fail loudly ("non-determinism detected for seed
  N at checkpoint M") if two runs of the same seed diverge; the same-seed ⇒ identical-trace
  property is itself a CI-checked property. *(adopted from SpacetimeDB analysis, file 09)*
- **TST-131** [P0] **Fault injection ("buggify")**: the simulated runtime **MUST** expose
  probabilistic fault-injection points (FoundationDB-style buggify) at, minimum: fsync and
  write failures, partial writes, IO latency spikes and reordering, task-scheduling
  perturbation, clock jumps, and (for replication) message loss, duplication, reordering, and
  partitions. Fault probabilities are seed-derived so every failure reproduces from its seed
  alone. *(adopted from SpacetimeDB analysis, file 09)*
- **TST-132** [P0] **Model oracle**: each DST suite **MUST** drive the real engine and a
  simple in-memory model with the same generated interaction stream (begin/insert/delete/
  commit/abort, unique-constraint acceptance/rejection, snapshot, replay) and check every
  observation against the model — acceptance/rejection parity per operation and full state
  equality at commit and after replay. Divergence in either direction is a failure.
  *(adopted from SpacetimeDB analysis, file 09)*
- **TST-133** [P0] **Crash-replay property**: using an in-memory simulated durability/
  commitlog implementation, the flagship property is crash-replay equivalence — at
  seed-chosen points the suite drops the database and reopens it from the simulated commitlog
  (with buggify faults active during both write and replay), and the recovered state **MUST**
  be row-set-equal to the model's committed state. This is the simulated twin of the §3
  kill-harness (TST-020/TST-025), which remains required. *(adopted from SpacetimeDB
  analysis, file 09)*
- **TST-134** [P0] **DST in CI**: the storage/commitlog DST suites **MUST** run in CI on every
  PR touching the storage, commitlog, or replication crates (bounded interaction count), with
  full-length multi-seed runs nightly; failing seeds are committed as regression cases
  (TST-003). Storage/commitlog DST green is a **G2** exit requirement; the replication DST
  suite **MUST** land with SPEC-014's implementation (not after) and is a **G7** exit
  requirement. *(adopted from SpacetimeDB analysis, file 09)*

## 15. Process-level smoketest harness

An end-to-end harness that exercises the real shipped binary — modeled on SpacetimeDB's guard
utility + smoketests architecture (one isolated server per test, driven through the real
product surface). *(section adopted from SpacetimeDB analysis, file 09)*

- **TST-140** [P0] **Guard utility**: `crates/fluxum-testing` **MUST** provide a guard that
  spawns the **real server binary** (not an in-process harness) on a random free port with an
  isolated temporary data directory and isolated CLI/client configuration per test, probes
  readiness (health endpoint polling with timeout) before the test body runs, and guarantees
  teardown (process kill + temp-dir removal) on success. On failure it **MUST** capture and
  attach the server's stdout/stderr and log files to the test report. Tests acquire servers
  only through the guard — no shared long-lived test server. *(adopted from SpacetimeDB
  analysis, file 09)*
- **TST-141** [P0] **Kill/restart with preserved data dir**: the guard **MUST** support
  killing the server (both graceful shutdown and immediate termination) and restarting it on
  the **same preserved data directory**, enabling restart/persistence drills as ordinary
  smoketests: state written before the kill is asserted present and correct after restart,
  and connected clients observe the documented disconnect/reconnect behavior (SDK-047).
  *(adopted from SpacetimeDB analysis, file 09)*
- **TST-142** [P1] **Harness ergonomics**: smoketests **MUST** default to precompiled fixture
  modules (native modules compile once as normal Rust — no per-test module compilation tax);
  the runner **MUST** hash-check the server binary under test against the current build to
  prevent stale-binary runs; and the suite **MUST** support a `--server <url>` remote mode so
  the same corpus runs unchanged against any deployment. *(adopted from SpacetimeDB analysis,
  file 09)*

## 16. Acceptance criteria

Gate-to-suite mapping — a gate passes only when its full test set is green (on the CI matrix of
TST-006 unless a suite states otherwise):

| Gate | DAG exit criterion | Required test set |
|---|---|---|
| **G0** | `cargo test` green on 3 OS | TST-001, TST-002, TST-006; quality gates TST-010–TST-012 active in CI |
| **G1** | Example schema compiles; codec roundtrip property tests green | TST-003, TST-005 (trybuild schema cases), TST-050; SPEC-001/SPEC-009 unit suites |
| **G2** | Crash suite loses zero committed tx; recovery truncates at first corrupt entry; < 30 s / 10 GB; dataset 10× memory budget served on the droplet profile; storage/commitlog DST green *(adopted from SpacetimeDB analysis, file 09)* | TST-020–TST-026, TST-110, TST-111; TST-130–TST-134 (storage/commitlog DST); TST-140–TST-141 (restart/persistence drills) |
| **G3** | Rollback, tick-drift, rate-limit, migration suites green | TST-040–TST-044 |
| **G4** | Subscription property suite + slow-consumer stress green | TST-030–TST-034 (full 10,000-mutation run per TST-034) |
| **G5** | e2e flow (auth → subscribe → reducer → `TxUpdate`); 2-shard handoff; wire format frozen | TST-051, TST-054; SPEC-006 transport integration suites; SPEC-007 2-shard handoff test |
| **G6** | PRD §12.1 MVP acceptance criteria all green, incl. parity report v1 — 0.1.0 | TST-052, TST-053, TST-060–TST-064, TST-070–TST-072, TST-080–TST-084, TST-090–TST-096; TST-140–TST-142 (full smoketest suite) |
| **G7** | PRD §12.2 all green — 0.2.0: failover + PITR + 5 SDK conformance + 1B-row soak + parity report v2 | TST-120–TST-122; TST-112; TST-052 corpus green for the Python, Go, and C# runners (T7.4–T7.6) in addition to the G6 set; TST-094 + TST-095 (parity report v2); replication DST green per TST-134 |

Completeness checks for this spec itself:

1. Every NFR in [PRD §7](../PRD.md) maps to at least one suite above (NFR-01 → TST-060;
   NFR-02/03 → TST-063; NFR-04 → TST-061; NFR-05 → TST-062; NFR-06 → TST-024; NFR-07 → TST-063
   read-path bench; NFR-08 → TST-021; NFR-09 → TST-006, TST-010–TST-012; NFR-10 → TST-030;
   NFR-11 → TST-090–TST-096; NFR-12 → TST-110, TST-111; NFR-13 → TST-112;
   NFR-14 → TST-100, TST-101).
2. Gate definitions in the [DAG](../DAG.md) reference only suites defined here or in the
   acceptance criteria of the spec that owns the feature (e.g., the 2-shard handoff test in
   [SPEC-007](SPEC-007-sharding.md)).
3. Every suite is runnable locally with one documented command (TST-007).
