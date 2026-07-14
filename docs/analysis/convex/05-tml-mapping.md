# 05 — Mapping to UzDB / TML

## Mapping strategy

- **ADOPT** — take the concept as-is, implement the same design in TML
- **ADAPT** — take the concept but modify it to fit TML or improve on Convex's gaps
- **DISCARD** — Convex's approach does not apply or is superseded by TML/UzDB capabilities
- **IMPROVE** — Convex has the right idea but UzDB can do it better

---

## Architecture mapping

| Convex concept | UzDB / TML mapping | Category |
|---------------|--------------------|----------|
| No intermediate server (DB is the backend) | Same — UzDB runtime IS the server | ADOPT |
| TypeScript functions as API surface | TML `@reducer` functions as API surface | ADAPT |
| V8 runtime for function execution | TML native compilation — no VM overhead | IMPROVE |
| Cloud-only deployment | Single binary, self-hostable | IMPROVE |
| Document-oriented storage | Relational typed tables (`@table`) | DISCARD |
| WAL-based key-value store | In-memory store + append-only commit log | ADAPT |

---

## Function model mapping

| Convex concept | UzDB / TML mapping | Category |
|---------------|--------------------|----------|
| `query` (read-only, subscribable) | `@view` function (read-only, subscribable) | ADOPT |
| `mutation` (ACID write, atomic) | `@reducer` (atomic, single-writer) | ADOPT |
| `action` (side effects, non-transactional) | `@procedure` (HTTP-callable, non-transactional) | ADOPT |
| `scheduler` / cron functions | `@tick(rate: 60hz)` + `@schedule(cron: "*/5 * * * *")` | IMPROVE |
| TypeScript type inference from schema | TML type system — native, not inferred | IMPROVE |
| `ctx.db.query("table").filter(...)` | SQL + TML queries with full SQL support | IMPROVE |
| Cannot call mutation from mutation | `@reducer` can call other `@reducer` in same tx | IMPROVE |

**Key TML advantage on tick loop:**
Convex's minimum practical schedule interval is ~1 second (cron-like).
UzDB's `@tick(rate: 60hz)` runs inside the DB process at true 60Hz —
no network round-trip, no cold start, deterministic execution.

---

## Real-time / subscription mapping

| Convex concept | UzDB / TML mapping | Category |
|---------------|--------------------|----------|
| Reactive query subscriptions | SQL subscriptions + client local cache | ADOPT |
| Client `useQuery()` hook | UzDB SDK hooks (same pattern) | ADOPT |
| Full document re-send on change | Incremental delta diffs (insert/delete/update rows) | IMPROVE |
| JSON wire encoding | UzBIN binary encoding | IMPROVE |
| Dependency tracking (doc-level) | Dependency tracking (row-level) | ADOPT |
| No delta/diff computation | Delta rows computed from TxState diff | IMPROVE |
| No spatial subscription | `WITHIN RADIUS N OF SELF` spatial subscriptions | IMPROVE |
| Global subscription (O(all rows) fan-out) | AoI-bounded subscriptions (O(nearby rows)) | IMPROVE |

---

## Transaction model mapping

| Convex concept | UzDB / TML mapping | Category |
|---------------|--------------------|----------|
| MVCC snapshot isolation | MVCC snapshot isolation per shard | ADOPT |
| Auto-retry on conflict | **No auto-retry** — explicit conflict handling | DISCARD |
| Non-deterministic execution (retries) | Deterministic single-writer per shard | IMPROVE |
| No distributed transactions | No cross-shard transactions (by design) | ADOPT |
| Atomic mutation (all-or-nothing) | Atomic reducer (all-or-nothing) | ADOPT |

**Why DISCARD auto-retry:**
Game simulations must be deterministic. The commit log must be a faithful record of what happened,
in what order. Auto-retry means the "real" execution order is hidden — two conflicting mutations
may execute in different order than the client triggered them. SpacetimeDB's single-writer model
is correct. UzDB follows the same.

---

## Developer experience mapping

| Convex concept | UzDB / TML mapping | Category |
|---------------|--------------------|----------|
| TypeScript schema (`defineSchema`) | TML `@table` type declarations | ADOPT |
| Auto-generated client SDK from schema | `uzdb generate --lang <lang>` | ADOPT |
| Optimistic update helpers (SDK) | UzDB SDK optimistic update pattern | ADOPT |
| Hot reload of functions in dev | TML recompile + hot-swap in dev mode | ADAPT |
| Convex Dashboard (web UI) | UzDB admin UI (future scope) | ADAPT |
| Zero config for dev | Single binary `uzdb dev` | ADOPT |

---

## Summary: what UzDB takes from Convex

### ADOPT (implement the same)
1. **No intermediate server** — the central insight, shared with SpacetimeDB
2. **Typed functions as API surface** — functions ARE the API, no REST design needed
3. **Push subscriptions** — declare intent, receive automatic updates
4. **Auto-generated client SDKs** — type-safe, no manual protocol code
5. **Optimistic update pattern** — essential for responsive game UI
6. **Zero-config development** — single binary, no external services

### IMPROVE (Convex has the right idea, UzDB does it better)
1. **Execution model:** TML native > V8 (5–10× faster, 0ms overhead)
2. **Wire encoding:** UzBIN binary delta > JSON full document (1000× less bandwidth)
3. **Subscription model:** AoI spatial > global (scales to MMORPG player counts)
4. **Transaction model:** single-writer deterministic > MVCC auto-retry (game correctness)
5. **Game loop:** `@tick(60hz)` in-process > cron-like scheduled functions
6. **Deployment:** self-hosted binary > cloud-only (studio requirements)

### DISCARD
1. **Document model** — relational tables with spatial indexes are required for games
2. **V8 JavaScript runtime** — TML native compilation is the point; no VM
3. **MVCC auto-retry** — non-deterministic; games require deterministic execution
4. **JSON protocol** — binary delta is non-negotiable at MMORPG scale
5. **Cloud-only** — games need on-premise; Convex's lock-in is unacceptable

---

## Final verdict for UzDB

Convex confirms that the "DB-as-server with reactive subscriptions" architecture is sound for
real-time applications. Its DX (developer experience) is excellent and worth emulating.

However, Convex was designed for **web applications** (React apps, SaaS tools), not games.
Every place Convex made a tradeoff, it chose web-friendly over game-friendly:
JSON over binary, cloud-managed over self-hosted, MVCC auto-retry over determinism,
document model over relational, no tick loop over server-side physics.

UzDB must make the opposite tradeoffs at every one of these points.
