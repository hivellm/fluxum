# 10 — Hard Problems & Roadmap Impact (synthesis)

| | |
|---|---|
| **Source** | SpacetimeDB v2.7.0, commit `1a8df2a` (2026-07-13, analyzed 2026-07-14) |
| **Synthesizes** | Files [01](01-architecture-crates.md)–[09](09-ops-testing-bench.md) of this analysis |
| **Audience** | Fluxum implementers — read before starting any DAG phase |
| **Compared against** | [PRD](../../PRD.md) §6/§7/§12, [DAG](../../DAG.md) tasks T0–T7, SPEC-001…016 |

This is the decision document. Files 01–09 are the evidence; this file ranks what will actually
hurt, checks Fluxum's contrarian bets against what SpacetimeDB shipped (and regretted), and maps
every finding onto a DAG task.

---

## §1 Scale reality check

SpacetimeDB is **~237k Rust LOC across 45 crates** (plus ~91k LOC of C++/C#/TS bindings and 4
client SDKs at 5–20k LOC each), after years of production iteration. Fluxum's 6-crate plan is a
starting topology, not a scope estimate. Where the mass actually sits (file 01):

| Subsystem | LOC | Fluxum verdict |
|---|---|---|
| `table` (pages, BFLATN, blob store, indexes) | 21,100 | **Must rebuild** — and add the tiered layer they don't have. Largest single investment. |
| `cli` | 18,744 | **Must rebuild most of it.** DX is a product surface (§2h); `spacetime dev` alone is 2,100 LOC. |
| `schema` (ModuleDef, validation, auto-migrate) | 15,653 | **~⅓ needed** (~4–6k LOC). Half exists only because ModuleDef crosses a serialization boundary in two wire versions (v9/v10) from untrusted guests. Native modules build validated defs in-process. |
| `datastore` (tx machine, system tables, replay) | 15,634 | **Must rebuild** — the overlay/undelete/DDL-rollback machinery is correctness-critical (§2b). |
| `smoketests` + `testing` + `guard` + `dst` + `sqltest` + `bench` + `index-scan-gate` | ~24,000 | **Must rebuild the categories** — `fluxum-bench` covers one of five test-infrastructure categories (file 01/09). |
| `sats` (type system + BSATN + JSON codec) | 14,150 | **Partially avoided**: FluxBIN ≈ BSATN, but no JSON codec, no u256 arithmetic, no serde bridge for a dual protocol. Budget ~half. |
| `codegen` (5 language backends) | 13,582 | **Must rebuild** for 5 SDK targets — as a library, not CLI-internal code. Note their Unreal backend alone is 7k LOC; Fluxum has no engine-native target. |
| `commitlog` + `durability` + `snapshot` | ~13,200 | **Must rebuild**, and file 03 shows the subtlety is in recovery edges, not the happy path. |
| `core` (module hosts, clients, subscription actor) | 41,483 | **Split verdict**: ~10k of WASM/V8 hosts + energy deleted; subscription manager+actor+delta (~9k) must be rebuilt; client-connection infra partially transfers. |
| bindings (Rust/C#/C++/TS guest side + `bindings-sys`) | ~70,000 (mixed langs) | **Deleted entirely** by the native-module decision — the single biggest scope win. |

**What Fluxum genuinely avoids** (≈90–100k LOC-equivalent): both sandbox hosts (wasmtime + V8,
~10k LOC in `core`), the entire guest ABI and per-language module bindings (~70k), energy/fuel
metering, the `update` version-multiplexer binary (1.3k), the rolldown bundler + wasm toolchain
in the CLI, ~⅔ of `schema` (versioned RawModuleDef deserialization), the JSON protocol half of
`sats` and the `FormatSwitch` generic infection across ten crates, and hot-publish choreography
(watch-channel module swap, throwaway validation instances).

**What Fluxum must build that SpacetimeDB never did**: tiered storage with paged indexes and a
fault-in seam (SPEC-015), open-source replication + failover (SPEC-014 — OSS SpacetimeDB has
vocabulary only, `num_replicas = 1` hard-coded), sharding + entity handoff (SPEC-007), spatial
indexes (SPEC-008), per-caller rate limiting (they have none), the Streamable HTTP session
registry, five SDKs where they stopped at four, and the Postgres parity harness.

Net: the deletions and additions roughly cancel. **Expect Fluxum-at-0.2.0 to be a
150–200k-LOC-class system.** Plan `fluxum-core`'s internal module boundaries (types / commitlog /
row-store / tx / query / subscription) with enforced one-way imports from day one so later crate
extraction is `git mv`, not surgery (file 01 §6).

---

## §2 Hard problems, ranked (hardest first)

### (a) Incremental subscription evaluation at scale — SPEC-005 / T4.2

**What it is.** Turning "N clients × M queries × every commit" into something sublinear.
SPEC-005's sketch (`plans: HashMap<ConnectionId, Vec<CompiledPlan>>` + per-table watchers) is
O(clients) per commit — the exact cliff SpacetimeDB engineered away.

**Evidence (file 05).** Their manager is ~9k lines of subtle Rust built around three structures:
(1) **query-hash dedup** — `QueryHash = blake3(sql)` (+ caller identity when parameterized);
one entry per unique query text, compiled once, evaluated once per commit, encoded once, bytes
`Bytes`-refcount-cloned per subscriber; (2) **value-level pruning** — `SearchArguments`
(`(table, col, value) → queries`) so 1,000 clients with distinct `WHERE id = ?` values cost 1
evaluation per matching row, plus `JoinEdges` for the two-table case; (3) an ordered
**SendWorker** queue detaching network fan-out from the commit thread, with durability gating by
tx_offset. Their delta algebra ("same plan, delta-flagged scans" — no separate diff engine) is
the most valuable single idea in the codebase. Hard-won negative results in source comments:
rayon on the per-commit path was a net loss (single-threaded now); compressing under the tx lock
was a net loss (per-client, off-lock, >1 KiB threshold now).

**Recommendation.** T4.2's deliverable must include the dedup and pruning structures as core
scope, not optimizations: `HashMap<QueryHash, QueryState>` with per-query subscriber sets;
`SearchArguments`-style equality pruning; the **spatial analogue** for FR-35 (a region index over
subscribed regions, served by the SPEC-008 quadtree — no counterpart exists in SpacetimeDB, this
is novel); a single-threaded eval loop on the commit thread; an ordered send-worker. Adopt the
delta-flagged-scan IVM design — for Fluxum's single-table scope it collapses to exactly 2
fragments (insert/delete) and makes deletes "run the same filter+RLS over old values", which
kills SUB-021's per-row subscription lookup. Add subscribe-time admission control (cardinality
estimate vs `row_limit`) and compile-time plan classification (`scan_type`, `unindexed_columns`)
exported as metric labels — SPEC-005 and SPEC-012 currently have neither.

### (b) Storage correctness machinery + the tiered-storage delta — SPEC-002 / T2.x, SPEC-015 / T2.8

**What it is.** The row store is the biggest and riskiest component, and SPEC-002 as written
describes a different, heavier engine (`BTreeMap<PK, Row>` with materialized rows) that misses
provably-necessary machinery.

**Evidence (file 02).** `crates/table` is 21k LOC of unsafe-dense pages (64 KiB, BFLATN layout,
intra-page var-len granules, blob-threshold overflow, freelists), `RowPointer` physical identity
with documented ABA hazards, a monomorphized index matrix, `StaticLayout` memcpy fast paths, and
BLAKE3 per-page content hashes. `datastore` adds the overlay: tx insert-tables mirroring
committed index structure, per-page delete bitsets, delete-then-reinsert **undelete**
cancellation, cross-state unique-constraint checks (committed probes filtered by the tx delete
set), a refcounted content-addressed **blob store with a tx-local overlay** (rollback = drop),
sequence batch allocation as in-tx system-table writes, and `PendingSchemaChange` reverse-replay
for transactional DDL rollback (~130 lines of careful inverse ops). SPEC-002 mentions none of
(i) constraint-check placement relative to TxState, (ii) undelete semantics, (iii) DDL undo log,
(iv) blob GC, (v) auto-inc batching, (vi) an st_*-style durable catalog.

**The tiered delta (file 02 §4.1).** Their *page payload* is nearly spill-ready: self-relative
16-bit offsets, no uninit bytes, BSATN-serializable, content-hashed, deterministic physical
layout across replicas — pages already round-trip to disk for snapshots. But **everything above
the pages assumes residency**: `Vec<Box<Page>>` O(1) access with no fault-in seam, and all
indexes + PointerMap + blob store are conventional RAM heap structures. Their own TODO ("once
indexes are managed by the page cache") confirms **paged indexes are future work for them too**.
SPEC-015's index spill is genuinely novel — no precedent in this codebase.

**Recommendation.** Amend SPEC-002 with the six omissions (they are correctness requirements).
For T2.1, take the pragmatic middle path: keep SPEC-002's logical `BTreeMap` semantics but store
rows as boxed FluxBIN bytes, and design `RowId`/page coordinates + the constraint/undelete/undo
machinery from day one so T2.8's pager lands under the same logical API. Adopt from their pages:
16-bit intra-page offsets, granule-style var-len with a blob threshold, per-page content hash
(buys dirty tracking + incremental checkpoint dedup), pooled page frames. For T2.8, decide the
index-addressing question early: logical keys in indexes (lookup per hit) vs physical addresses
+ pin discipline (their experience: physical wins big but creates the ABA/stability
obligations). Budget T2.1+T2.8 as multiple person-months with proptest/fuzz investment
comparable to the code itself.

### (c) Commitlog / recovery subtleties — SPEC-002 / T2.2–T2.7, SPEC-014

**What it is.** The append path is easy; the recovery and replication edges are where the 9k LOC
went.

**Evidence (file 03).** Commit framing carries `min_tx_offset | epoch | n | len | CRC32C` — the
CRC covers **header + payload** (a corrupted length can't mis-frame the segment), the **epoch is
in every durable commit** (fencing lineage; snapshots-invalidated-on-failover exists because
snapshots *don't* carry it). Recovery handles: trailing garbage shorter than a header
(ftruncate + fsync), all-zeros = fallocate sentinel not corruption, empty tail segments,
duplicate commits after crash-retry (same offset+CRC → skip) vs **forks** (same offset,
different CRC → replication divergence detected in the storage layer), corrupt-first-entry hard
error, corruption-on-open does **not** truncate (torn tail superseded, evidence preserved;
truncation is an explicit `reset_to` invoked by replication). Group commit is an actor: bounded
queue (4×4096), drain-all, one fsync per batch, durable offset published on a watch channel —
which is exactly the primitive semi-sync quorum ack and confirmed reads need. fsync failure =
panic, never retry. Snapshots are content-addressed page objects with hardlink dedup, two-phase
creation (manifest-file-last = commit record), blake3-verified on restore, cadence tied to
segment rotation (bytes of log, not tx count). Replay across schema changes cost them ~1,000
lines of `ReplayVisitor` with permanent version archaeology.

**Recommendation.** Before the G5 format freeze: pull `tx_id` + length into a **checksummed
fixed entry header** (replica-side validation without payload decode), add an **epoch** field
(cheapest insurance for all of SPEC-014 — PITR lineage, divergence truncation, checkpoint
validity after promotion), and a per-segment format-version byte. Replace STG-012's "OS
write-behind" with the group-commit actor + published `DurableOffset`. **Fix the checkpoint
scaling cliff now**: STG-020's full dump every 10k tx rewrites the whole database each time on a
10 GB dataset; switch the trigger to log-bytes (rotation-driven) and adopt content-addressed
incremental checkpoints (the per-page hash from (b) makes this nearly free), two-phase durable
creation, and whole-file + per-object integrity hashes (SPEC-002's `Snapshot` currently has no
checksum at all). T2.7's crash suite must cover the full edge-case list from file 03 §"What
Fluxum will face" pt. 1, plus bit-flip and truncate-at-every-byte property tests. Replay rules:
no constraint checks, indexes built after, auto-inc rebuilt after, FailFast/Warn modes.

### (d) Client SDK cache semantics — SPEC-011 / T6.2

**What it is.** The client cache is where SpacetimeDB spent its SDK complexity budget in *every*
language — and where its four SDKs silently diverged.

**Evidence (file 07).** The reference (Rust) cache: rows keyed by **raw BSATN bytes** (solves
float hashing and PK-less tables in one move), **per-row refcounts** for overlapping
subscriptions (semantic delete only at refcount 0), **inserts applied before deletes** within an
update (refcount must never transiently hit zero; the server legitimately sends
`[delete r0, insert r0]` under join semantics — pairs must cancel), **update = PK-matched
delete+insert post-pass** (tables without PK get no update events),
**mutate-all-then-callback-all** atomic visibility (C#'s PreApply/Apply/PostApply is the
cleanest form), insert-only event tables bypassing the cache, parse/decompress off the hot
thread with in-order handoff. Known unsolved wart: per-query row eviction is impossible with
refcounts alone — the TS SDK makes subscription errors *fatal* for this reason. Ordering
guarantee worth copying: InitialData enqueued while still holding the read tx, so a client can
never see a TxUpdate ordered before its InitialData. No SDK auto-reconnects at the core layer.

**Recommendation.** Write cache semantics as normative, fixture-testable rules in SPEC-011/013
**before the second SDK exists**. Adopt: FluxBIN-bytes identity (PK projection when available),
refcounts *or* — better — per-query row bookkeeping decided up front (it makes unsubscribe
eviction and partial-error recovery trivial and avoids their fatal-error wart),
inserts-before-deletes, PK coalescing, atomic callback visibility. Fluxum's SDK-082
auto-reconnect + resubscribe is a genuine differentiator with **no prior art here** — it must
specify cache reconciliation on re-`InitialData` (clear-and-replay vs diff-against-stale);
budget design time, not just code time.

### (e) The five-language SDK factory problem — SPEC-011/013 / T6.2, T6.4, T7.4–T7.6

**What it is.** Each SDK is a full runtime (cache + codec + protocol), 5–20k LOC in their world,
kept consistent only by discipline.

**Evidence (file 07).** SpacetimeDB's conformance story is **hand-mirrored test modules** in four
languages ("must be kept in sync" READMEs) with no shared wire-fixture corpus. The divergences
this failed to catch shipped: Unreal's brotli decompression is stubbed, TS lacks one-off queries,
three different cache keying strategies across four SDKs. They stopped at four languages because
each new one costs a team.

**Recommendation.** Fluxum's bets — thin runtimes, straight-line generated decoders, one shared
conformance corpus — are the correct mitigations, but the corpus **must cover cache-application
scenarios** (insert-before-delete ordering, same-row delete/insert cancellation, overlap
refcounts, update coalescing, reconnect reconciliation), not just codec round-trips — that is
exactly where their SDKs diverged. Copy BSATN's negative-test discipline into FluxBIN (reject
bad bools, unknown tags, truncation, with typed errors). Enforce the TS bundle-size gate from
the first commit (their 30 KB-brotli budget is CI-enforced) and run headless-Chromium
conformance early (their `DecompressionStream('brotli')` portability trap is only catchable in
a real browser). Adopt the flat `BsatnRowList` row encoding (§4) before G5 — it is the wire
structure every SDK decodes.

### (f) RLS: compiled query fragments vs `owner_only` — SPEC-005 / T4.3

**What it is.** SpacetimeDB RLS rules are SQL fragments compiled at publish time, expanded as
views (UNION of fragments, recursive with cycle detection) into the *same* incremental plans as
user queries. Fluxum's `#[visibility(owner_only(field))]` is an O(1) per-row column check.

**Evidence (files 05, 08).** Their expressiveness is real: "visible if a row in another table
says so" (group membership, ownership-via-join) is one SQL line for them and **impossible** for
Fluxum's owner_only. It is also *why* their incremental engine had to support joins at all —
every RLS-protected table turns every subscription against it into a join, paying 8 delta
fragments + per-commit delta index builds + bag-semantics refcounting. RLS applies only to
client reads, never to reducer code; validated at publish, not per-query.

**Recommendation.** Keep owner_only for 0.1 — it covers the 80% case with trivially correct
deltas and avoids the join wall entirely — but **document the gap explicitly** in SPEC-005:
relational visibility is done via denormalized owner columns or client-side, and the P2
`custom(fn)` escape hatch defeats index pruning (per-row evaluation on every fan-out). Adopt
their two rules verbatim: validate visibility declarations at startup (fail fast), and state
that RLS never applies to reducer code. If join-based RLS is ever added, it drags the entire
two-table delta algebra of §2a with it — price that consciously.

### (g) Scheduler correctness — SPEC-004 / T3.4

**What it is.** Rollback-safe, crash-safe deferred execution.

**Evidence (file 08).** Their invariant set validates SPEC-004's design almost exactly: schedule
state is transactional rows (insert rolled back ⇒ never fires — the actor **re-reads the
committed row at fire time**, so no unhook is needed); restart rescans all scheduled tables and
re-enqueues; past-due entries fire once ASAP, no backfill; interval rescheduling rebased on
intended tick time (anti-drift). Deltas: they are at-least-once (row deleted *after* the run);
Fluxum's delete-in-same-tx is stronger (effectively exactly-once) — keep it and document it.
Their pitfall: scheduled reducers remain client-callable unless the module checks the sender.
They have no `#[tick]` — 60 Hz loops pay table overhead per tick; Fluxum's dedicated tick
scheduler is the better realtime fit.

**Recommendation.** T3.4: make `#[tick]`/`#[schedule]` functions non-callable via `ReducerCall`
by default; define a reserved identity + nil ConnectionId for scheduled contexts (SPEC-004
currently leaves `ctx.identity` unspecified); implement the fire-time committed-row re-read and
restart rescan as acceptance tests. Also adopt from file 08: `on_connect` returning `Err`
rejects the connection atomically with session bookkeeping, and the `__session__` table is
scanned on restart to guarantee `on_disconnect` eventually fires for crash-time clients.

### (h) The DX surface we underestimated — SPEC-011 / T6.x + new work

**What it is.** SpacetimeDB's CLI is 18.7k LOC; onboarding is a CI'd product surface (25
templates embedded in the binary with committed bindings, 38 test modules, a flagship demo).

**Evidence (file 07).** The single highest-leverage command is `spacetime dev` (2,100 LOC:
watch → regenerate → rebuild → republish → stream logs → run client). Other cheap-and-delightful
patterns: stale-generated-file GC via a magic header prefix; language auto-detection; `call`
validating args against the fetched schema with edit-distance suggestions; UNSTABLE warnings on
early commands; `spacetime logs -f` streaming merged module+system logs over HTTP (file 09).
Fluxum's generate-from-`/schema` (no toolchain, no server binary needed on client machines) is
strictly better than their generate-from-artifact — they retrofitted a hidden flag toward it.

**Recommendation.** Spec and schedule a `fluxum dev` inner-loop command (see §5); embed
templates in the binary; add log-streaming with `?follow` to SPEC-012. Fluxum's restart-based
native-module deploy makes the loop *simpler* than theirs (no publish step, just
rebuild+restart) — that's a demo-able advantage if the command exists.

---

## §3 What Fluxum deliberately does differently — validated or challenged

| Decision | Verdict | Evidence |
|---|---|---|
| **Native Rust modules (no WASM/V8)** | ✓ **Validated** | The boundary tax is real and *measured by them*: per-`AbiCall` timing (`abi_duration`), every scanned row is host-encode → linear-memory copy → guest-decode, handle slabs, errno maps, fuel bookkeeping (file 04). Deletes ~90k LOC of guest machinery. Accepted trade: a wild module takes the process down — keep the `ModuleHost`-style trait seam, `catch_unwind` + host-owned TxSlot discipline, and time-based budgets (fuel is a wasm-only luxury; even their V8 lane ships wall-clock metering *stubbed*). |
| **Binary-only protocol** | ✓ **Validated by their own retreat** | v1 shipped BSATN+JSON; `FormatSwitch` generics infected ten crates and forced double encodes at fan-out; **v2 deleted JSON entirely** (file 06). Keep JSON confined to the HTTP admin envelope, never in `fluxum-protocol`. |
| **Streamable HTTP (no WebSocket)** | ✓ Sound, with a bill | ~900 lines of their `subscribe.rs` are pure WS ceremony Fluxum skips; their v3 protocol exists only to get the frame-coalescing Fluxum's framing has natively (file 06). In exchange Fluxum owns what WS gave free: session↔queue binding across `GET /rpc` reconnects, expiry sweeping, single-push-stream rule. Their per-client infra transfers unchanged: kick-don't-block at ~16k queued messages, encode task split from socket writer, pooled buffers, per-session compression (brotli-1, >1 KiB, tag byte). No prior art for HTTP-streaming realtime SDKs — T5.2/T6.2 are pioneering, test in real browsers early. |
| **Tiered storage (data ≫ RAM)** | ✓ **Our edge — and genuinely hard** | SpacetimeDB is fully RAM-bound (the PRD's critique holds at v2.7.0). Their page *format* transfers nearly intact; the fault-in seam and **paged indexes have no precedent** — their own TODOs defer both (file 02). This is Fluxum's most differentiating and most novel storage work. |
| **Open replication (SPEC-014)** | ✓ **Our edge — uncontested in OSS** | Confirmed absent: `num_replicas = 1` hard-coded, zero consensus code (file 09). Better: their storage layer independently validates "the commit log *is* the replication protocol" (epoch in every commit, fork detection, byte-stream mirroring with receiver-side CRC validation, content-addressed remote snapshot sync = full-sync seeding). Their trap to avoid: features unobservable from one node ("we can't test that there is any difference in behavior") — multi-node test harness must land *with* T7.1, not after. |
| **Postgres parity harness (NFR-11)** | ✓ **Uncontested** | They benchmark vs SQLite (flattering) and themselves; Postgres appears only as a *correctness* oracle (`sqltest`, not in CI) (file 09). Keep both parity dimensions separate: add a Postgres/SQLite correctness oracle for the query surface alongside the performance harness. |
| **`max_rate` declarative rate limiting** | ✓ **Genuine differentiator** | SpacetimeDB has **no per-caller rate limiting anywhere** — energy meters execution cost, not call frequency; a hostile client can spam reducers limited only by transport backpressure (file 08). Keep max_rate; add the half they *do* have and we don't: a wall-clock watchdog for runaway native reducers. |
| **Enriched `TxUpdate` (FR-43)** | ⚠ **Challenged — reconsider before G5** | Fluxum's FR-43 broadcast (`reducer_name`, `caller`, `duration_us` to every subscriber per commit) is v1 `TransactionUpdate`, which SpacetimeDB explicitly regretted (bandwidth pressure toward one-letter reducer names; a "lying" energy field) and **stripped in v2** to rows-only, moving metadata to the caller-only response (file 06). Either justify the enrichment for Fluxum's ops/debugging story, or adopt: per-session `light` mode opt-out, interned reducer IDs (id↔name map at subscribe), metadata full only to the caller. Decide before the wire freeze. |
| **`Identity = SHA-256(token)` (FR-70)** | ⚠ **Flag for SPEC-009** | SpacetimeDB moved *beyond* token-hash: identity = f(issuer, subject) survives token rotation by construction, with a self-validating `0xc200` prefix + checksum (file 08). Fluxum's scheme is workable iff every provider canonicalizes to something claims-like — which converges on their design with extra steps. Recommendation: keep SHA-256 derivation but make the built-in `jwt` provider canonicalize to `"{iss}|{sub}"`; steal the version-prefix + checksum format; keep the embedded-identity cross-check pattern as the future migration path. |
| **Restart-based deploys (no hot-publish)** | ✓ Validated, with a keeper | Deletes the watch-channel swap, dual-module handoff, throwaway validation instances. What survives: `ponder_migrate`-style schema diffing at startup (stored catalog vs compiled-in def), exhaustive human-readable rejection reasons, and a `fluxum migrate --plan` dry-run (file 04). That is the core of SPEC-010/T3.6. |
| **No composite-PK / geospatial gaps (PRD §2.2)** | ✓ Still true | v2.7.0 has no spatial indexing and PKs remain single unique indexes. FR-15/FR-60 remain differentiators. Note their `#[table]` API maturity (typed index handles, `try_insert` typed errors, PK-gated `update`) is the bar for T1.1/T3.2 ergonomics — diverge deliberately. |

---

## §4 Adopt / avoid list

**Adopt (concrete, with landing site):**

1. **DST runtime seam** (file 09) — a `runtime::Handle` (tokio vs seeded deterministic sim,
   virtual time, buggify fault injection, determinism-log) from the **first commit** of
   `fluxum-core`; their retrofit pain is documented in their own coverage tracker. → T0.2/T2.1.
2. **Smoketest architecture** (file 09) — one isolated server+data-dir per test, drive the real
   CLI as a subprocess, `--server URL` remote mode, checked-in old-version data-dir fixtures,
   quickstart-docs-as-tests. Skip their tax: precompiled fixture modules by default. → SPEC-013.
3. **Flat row lists** (`BsatnRowList`: `FixedSize(u16) | RowOffsets` + one `Bytes` buffer) —
   replaces RPC-032's `Vec<Vec<u8>>`; zero per-row overhead, zero-copy fan-out, browser-friendly.
   Wire-format change: **must land before G5**. → T1.2/SPEC-006.
4. **Group-commit actor** — bounded queue, drain-all, one fsync per batch, `DurableOffset`
   watch channel (feeds semi-sync ack and confirmed reads). → T2.2/SPEC-002/014.
5. **Content-addressed incremental snapshots** — per-page content hash, hardlink dedup,
   two-phase creation, manifest-last-as-commit-record, blake3-verified restore, cadence by
   log-bytes. → T2.3/SPEC-002.
6. **Delta-query algebra** — same compiled plan, delta-flagged scans, `DeltaStore` trait;
   plus query-hash dedup + `SearchArguments` pruning + ordered SendWorker. → T4.1/T4.2.
7. **Refcounted client cache semantics** (or per-query row sets — decide first): byte-identity
   keys, inserts-before-deletes, PK coalescing, atomic callback visibility. → T6.2/SPEC-011.
8. Smaller keepers: typed `metrics_group!` macro, replay/snapshot timing + subscription
   forensic metrics, `paths`-style typed dir layout crate, `memory-usage` leaf trait before the
   buffer pool, `error_stream` accumulate-all-errors validation, flock data-dir lock +
   `metadata.toml` version gating, zero-padded offset filenames, workspace hygiene
   (`panic = "unwind"`, `disallowed-macros` banning `println!`, `profiling` profile),
   max-SQL-length + recursion caps, timestamp-seeded reducer RNG, in-band token echo,
   short-lived connect tokens, `spacetime logs -f` streaming, edit-distance CLI suggestions.

**Avoid:**

1. **The energy system** — fuel metering exists to bill untrusted sandboxed code; even they
   ship it stubbed for V8 and `NullEnergyMonitor` in OSS. Fluxum needs wall-clock budgets +
   watchdog + `max_rate`, nothing more.
2. **Hand-mirrored test modules per language** — the O(SDKs × features) sync tax whose failures
   shipped (stubbed brotli, missing features, divergent cache keying). One shared fixture corpus
   covering cache-application scenarios instead.
3. **Unbounded SDK channels** — their Rust SDK uses an unbounded mpsc for pending mutations and
   the server's SendWorker queue is unbounded. Bound everything; Fluxum's 3-tier backpressure
   (FR-33) is already more specified than what they ship — keep it.
4. **No auto-reconnect** — their core SDKs are dead on disconnect with stale caches. Fluxum's
   SDK-082 is a differentiator; don't copy their omission.
5. **Dual wire formats** (JSON+binary), **three parallel protocol generations** in one manager,
   **per-query-update compression under the tx lock**, **generate-from-artifact** requiring
   module toolchains on client machines, **full-dump checkpoints on a tx-count trigger** — all
   explicitly walked back or regretted in their source.

---

## §5 DAG impact table

| Task | Evidence | Suggested adjustment |
|---|---|---|
| **T1.2** (FluxValue/FluxBIN + FluxRPC types) | File 06: `BsatnRowList`, strict-decode negative tests, `to_len()` pre-sizing, static-size memcpy path | Add flat row-list structure + negative-test corpus to the deliverable now — wire format freezes at G5 and every SDK depends on it. Effort roughly +30%. |
| **T2.1** (MemStore) | File 02: SPEC-002's structures miss constraint placement, undelete, DDL undo, blob GC, auto-inc batching, durable catalog | Amend SPEC-002 first; design the logical API to survive the T2.8 pager underneath. This is the schedule anchor — do not let "BTreeMap + rows" pass its exit test and be declared done. |
| **T2.2** (CommitLog) | File 03: checksummed fixed header, epoch field, group-commit actor, torn-tail taxonomy, fork/dup semantics | Estimate is optimistic if scoped as "append + CRC + replay". The recovery edge cases and the header design (epoch, tx_id, version byte) are the real work; several are irreversible after G5. |
| **T2.3** (SnapshotRepo) | File 03 §6: full-dump-per-10k-tx is a scaling cliff on 10 GB datasets | Re-scope to content-addressed incremental checkpoints triggered by log-bytes; add integrity hashes (currently absent from SPEC-002). Bigger than "periodic dumps". |
| **T2.7** (Crash suite) | File 03: bit-flip at every position, truncate at every byte, empty-segment, dup-vs-fork, snapshot/log divergence matrix | Expand the drill matrix beyond "CRC corruption"; add snapshot-fallback-chain tests. Also the natural home for the DST determinism property (same seed ⇒ identical trace). |
| **T2.8** (Paged tier + buffer pool) | File 02 §4.1: page format transfers; fault-in seam and **paged indexes are novel** — SpacetimeDB defers both | Split the estimate: page format + eviction (precedented) vs index spill + eviction-safe row addressing (novel research-grade work, feeds OQ-7). Highest-uncertainty task in Phase 2. |
| **T3.4** (tick + schedule) | File 08: fire-time committed-row re-read, restart rescan, non-callable-by-clients, reserved scheduled identity | Mostly validated; add the four correctness items to the exit tests. Low risk if specced now. |
| **T3.6** (migrations) | File 04: `auto_migrate.rs` is 2.6k LOC of diff planning + exhaustive rejection reasons | "Auto-diff + safe auto-apply" is a subsystem, not a task-week; add `fluxum migrate --plan` dry-run to the deliverable. |
| **T4.1** (SQL compiler) | Files 01/05: their pipeline is 5 crates / ~10k LOC *for a deliberately tiny subset*; `IN`/`BETWEEN` (SUB-010) exceed their dialect | Budget as a subsystem. Layer it (parse → typed → plan → exec) even inside one crate. Add hostile-corpus limits (max length, recursion caps) to exit tests. |
| **T4.2** (SubscriptionManager) | File 05: their manager+actor+delta ≈ 9k lines; O(clients) fan-out is the cliff; pruning + dedup + SendWorker are core | **Most underestimated task in the DAG.** Re-scope deliverable to include query-hash dedup, value-level pruning (incl. the spatial pruning index for FR-35 — novel), admission control, and the ordered send-worker. Likely 2–3× the current implied estimate. |
| **T4.3** (RLS) | Files 05/08: owner_only avoids the join wall; startup validation; reads-only rule | Cheap as specced — *because* of the expressiveness cut. Document the gap (§2f) in SPEC-005 so it's a decision, not a surprise. |
| **T5.2** (Streamable HTTP) | File 06 §7–8: session registry, queue survival across reconnects, expiry, per-client encode/compress infra | No prior art in this codebase or elsewhere; the browser test (headless Chromium) should run against real proxies/HTTP semantics early, not at T6.2. |
| **T6.1→T6.2 ordering** | File 07: generate-from-schema validated; version-floor enforcement in generated code | Fine as ordered; add a generated-code min-runtime-version guard (their `ensureMinimumVersionOrThrow`) and the SDK-043 schema-version handshake they lack. |
| **T6.2** (TS SDK) | File 07: "the cache is the SDK" — refcounts/ordering/coalescing/reconnect is where every SpacetimeDB SDK spent its budget; their browser bundle is 30 KB brotli with CI budgets | Deliverable is a **client cache runtime + reconnect protocol**, not "codegen + transport". Auto-reconnect cache reconciliation has no prior art. Enforce the size budget from commit one. Likely 2× the implied estimate. |
| **T6.5** (demo app) | File 01/07: 25 templates + flagship demo are CI'd product surface | Treat the demo as the first template; plan the template mechanism (embedded in binary) alongside it. |
| **T7.1/T7.2** (replication) | Files 03/09: log-as-stream validated (include segment headers; receiver CRC+contiguity validation; trim-torn-tail-before-append; seekable-zstd archives); no OSS consensus prior art; single-node-unobservability trap | Add REP-014 pre-append tail check + seekable archive framing to SPEC-014; require the multi-node harness (and ideally DST multi-node sim) to land *with* T7.1. The epoch-in-log decision (T2.2) is a hard prerequisite — check it at G5. |
| **T7.4–T7.6** (Python/Go/C# SDKs) | File 07: 5–20k LOC per SDK in their world; corpus must cover cache scenarios | Estimates hold **only if** T6.2's corpus includes cache-application fixtures and the thin-runtime rule is enforced. Otherwise each SDK re-derives §2d by hand. |
| **New: `fluxum dev`** | File 07 §6.3: 2.1k-LOC watch loop is their single highest-leverage DX asset; Fluxum has no equivalent specced | Add a Phase-6 task (after T6.2): watch → rebuild → restart → tail logs → run client. Native modules make it simpler than theirs — visible competitive win for the T6.5 demo. |
| **New: log streaming** | File 09: `GET /logs?follow` + merged module/system stream | Small SPEC-012 addition, outsized DX value; slot into T5.6/T5.3. |
| **New (post-0.2 candidate): pgwire front-end** | Files 01/06: their `pg` crate is 714 LOC on `pgwire` over the public SQL entry point; unlocks psql/BI ecosystem | Keep Fluxum's SQL execution behind one `sql_direct`-style function so a future `fluxum-pg` crate needs no internal APIs. Roadmap, not DAG. |

**Cross-cutting schedule note.** Three decisions are *irreversible at G5* and appear above
repeatedly: the commitlog entry header (epoch, tx_id, version, CRC coverage), the flat row-list
wire shape, and the enriched-vs-light TxUpdate question. Resolve all three in SPEC-002/SPEC-006
amendments before Phase 2 completes — every later phase replays these formats.
