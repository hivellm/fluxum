# SurrealDB — Reference Analysis

**Project:** UzDB (MMORPG database in TML)
**Reference:** [SurrealDB](https://surrealdb.com/) (SurrealDB Ltd.)
**Date:** 2026-04-14

---

## Purpose

SurrealDB is a multi-model database (document + graph + relational) written in Rust with a strong
real-time story (LIVE SELECT), built-in WASM functions, and geospatial query support. It is the
most technically mature alternative to SpacetimeDB for real-time game backends. This analysis
extracts architectural patterns, real-time design decisions, and limitations relevant to UzDB,
with focus on **multiplayer games and MMORPGs**.

---

## Files

| # | File | Content |
|---|------|---------|
| 01 | [01-overview.md](01-overview.md) | What SurrealDB is, design philosophy, comparison with SpacetimeDB |
| 02 | [02-architecture.md](02-architecture.md) | System layers, storage backends, WASM runtime, protocol |
| 03 | [03-realtime.md](03-realtime.md) | LIVE SELECT pipeline, notification model, limitations |
| 04 | [04-game-patterns.md](04-game-patterns.md) | Graph model, AoI patterns, game-specific limitations |
| 05 | [05-tml-mapping.md](05-tml-mapping.md) | Mapping to UzDB/TML — ADOPT / ADAPT / DISCARD |

---

## Key Findings (executive summary)

1. **Closest architecture to UzDB among real databases:** SurrealDB is a single Rust binary,
   self-hosted, with WASM modules for server-side logic and real-time LIVE queries. The technical
   stack is almost identical in language and goals to what UzDB must build in TML.

2. **LIVE SELECT is the right primitive:** Push-based subscriptions where the server automatically
   sends deltas on data change is the correct pattern. SurrealDB implements this via `LIVE SELECT`
   over WebSocket. However, the implementation sends **full documents** and has ~39ms notification
   latency — both unacceptable for games.

3. **Graph model is a genuine advantage for game data:** Player → inventory → items,
   player → guild → members, quest → prerequisite → quests. SurrealDB's RELATE + fetch syntax
   maps naturally to MMORPG data structures. UzDB should consider a graph-aware query extension.

4. **Geospatial built-ins are a strong signal:** `geo::distance()`, `geo::within()` confirm that
   real-time games need spatial queries as first-class features. UzDB's QuadTree/R-tree indexes
   are the right answer; SurrealDB's approach (function-level, not index-level) is insufficient.

5. **39ms notification latency is the critical flaw for games:** A 60Hz game tick is 16.7ms.
   SurrealDB's LIVE query notification latency (~39ms measured) means clients are always ≥2 ticks
   behind. SpacetimeDB achieves 1–5ms. UzDB targets <1ms (in-process push).

6. **No server-side game loop:** SurrealDB is a database, not a game engine. Game physics, AI,
   and tick-based simulation must live in a separate application server. This is exactly the
   architecture SpacetimeDB (and UzDB) eliminates.

7. **Full document on change, no delta encoding:** Every LIVE notification sends the full document.
   For position updates, this is wasteful by 10–100×. UzDB must implement row-level delta diffs.

8. **Pluggable storage backends are a good pattern:** RocksDB (single-node), TiKV (distributed),
   in-memory. UzDB should design the same: `MemStore` + `CommitLog` for game hot path, pluggable
   cold storage for historical data.

---

## Gaps in SurrealDB (opportunities for UzDB)

| Gap | UzDB opportunity |
|-----|-----------------|
| ~39ms notification latency | In-process push <1ms (no network hop) |
| Full document per notification | Row-level delta diffs (UzBIN) |
| No server-side game loop | `@tick(rate: 60hz)` declarative game loop |
| No AoI management | Spatial subscriptions `WITHIN RADIUS N OF SELF` |
| WASM function overhead | TML native compilation — zero FFI overhead |
| No subscription ordering guarantees | Commit-log-ordered push (linearizable) |
| No built-in spatial index | QuadTree / R-tree first-class primitives |
| Notification filtering done client-side | Server-side AoI + per-client WHERE filters |
| No single-writer determinism | Single-writer per shard (deterministic, replayable) |
