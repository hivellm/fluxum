# 07 — A06:2025 Insecure Design & A10:2025 Mishandling of Exceptional Conditions

Two 2025 categories that converge on one Fluxum property: **unbounded work on the
single-writer / subscription path is an availability design gap.** Panics are
handled well; *cost* is not.

---

## F-014 — No query `LIMIT` ceiling and no query execution timeout (HIGH)

**Evidence.** `LIMIT` is optional and applies only to `InitialData`/one-off
snapshots (SUB-013); there is **no maximum and no mandatory `LIMIT`**. A
`SELECT * FROM big_table` with no `WHERE` compiles to a `FullScan`
(`crates/fluxum-core/src/sql/mod.rs:641-644`), and `query_json`
(`crates/fluxum-core/src/subscription/mod.rs` ~`:865`) iterates all matching rows
with no cap when the client omits `LIMIT`. Nothing bounds the wall-clock time of
a full scan or an expensive `MATCH`/FTS predicate; the work runs to completion
under the subscription lock.

**Impact.** A single client (or the unauthenticated admin `/query`, F-001) can
force an unbounded scan that holds the subscription lock and stalls fan-out for
every other subscriber on the shard — an availability DoS and an insecure-design
gap. Under direct exposure this is reachable pre-any-quota.

**Confidence: High.**

**Fix direction.** Enforce a configurable default + maximum `LIMIT`, a
per-query row-scan budget, and a wall-clock deadline that aborts the plan.

---

## F-015 — No reducer execution-time or memory bound; hostile/buggy reducer stalls its shard (HIGH)

**Evidence.** Reducers run on the shard's single writer. A slow-reducer WARN
threshold exists (`crates/fluxum-core/src/reducer/engine.rs:592-602`) but it only
*observes* — it does not interrupt. Nothing caps execution time or allocation.
The plugin docs claim reducers are "deterministic, bounded"
(`plugin/mod.rs:10-11`) but boundedness is unenforced.

**Impact.** A reducer that loops or allocates without bound produces head-of-line
blocking for *all* clients on that shard until it OOMs or returns — the classic
A10 "mishandling of exceptional conditions" turning into a shard-wide outage.

**Confidence: High.**

**Fix direction.** A cooperative execution deadline (checked at reducer stdlib
boundaries) and a per-transaction allocation ceiling, with the transaction rolled
back and counted on breach — mirroring the existing panic→rollback path.

---

## F-016 — Subscriptions and one-off queries are not rate-limited per identity; limiter is Identity-keyed and bypassable (MEDIUM)

**Evidence.** The reducer limiter (`crates/fluxum-core/src/reducer/ratelimit.rs`)
governs **only** reducer admission — per-`(Identity, reducer)` token buckets plus
a global shard admission guard (RED-052, default 200 000/s, `ratelimit.rs:85`).
Subscription registration and `POST /query` have **no** token bucket; the only
subscription bound is the optional *count* ceiling in tenant quotas
(`crates/fluxum-server/src/quota.rs:227-241`), which is per-namespace, not
per-identity. Because buckets key on `Identity` (derived from the token), under
`auth.provider: none` (dev) each distinct token string is a distinct identity, so
the per-identity limiter is trivially reset by rotating the token — the global
shard guard is then the *sole* backstop. Buckets are in-memory and reset on
restart, so a crash-loop clears all throttles.

**Impact.** A client can open expensive full-scan subscriptions at socket speed
(compounding F-014), and per-identity throttles are evadable in permissive-auth
deployments.

**Confidence: High.**

**Fix direction.** Add a per-identity (and per-connection) token bucket in front
of subscription registration and one-off queries; make the global shard guard
mandatory-on; consider keying a secondary bucket on connection/IP so token
rotation cannot mint fresh budget.

---

## F-017 — Client-supplied `idempotency_key` has no length cap (MEDIUM)

**Evidence.** `crates/fluxum-core/src/reducer/idempotency.rs:128-139` records a
client-supplied `String` `idempotency_key` into a composite PK
`(identity, reducer, key)`. The window is bounded by *count* (100 000) and *age*
(1 h) (`idempotency.rs:88-98`) but not by *bytes* per key.

**Impact.** Large keys inflate the idempotency window table's memory footprint —
a low-grade memory-amplification vector within an authenticated session.

**Confidence: Medium.**

**Fix direction.** Cap `idempotency_key` length (e.g. 256 bytes) at decode time
and reject longer keys with a typed error.

---

## Positives (A06/A10) — exceptional conditions are otherwise well-contained

- **Reducer panics are isolated**: `catch_unwind` → rollback → wire 500, shard
  survives (`reducer/engine.rs:12-14`, `:408-417`); panics logged at ERROR with
  backtrace, business `Err` at DEBUG.
- **Idempotency is race-safe**: check-and-record happens inside the reducer's own
  transaction on the single writer, so two concurrent same-key calls cannot both
  miss (`idempotency.rs:109-139`).
- **Typed argument decoding** rejects arity/type mismatches before any
  transaction begins (`reducer/args.rs:16-18`, `:172-209`), with range-checked
  integer decodes.
- **Frame/handshake budgets** already blunt slowloris and oversized pre-auth
  frames (`tcp.rs`/`http.rs` handshake timeout + `handshake_max_bytes`).
