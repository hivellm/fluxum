# Convex — Reference Analysis

**Project:** UzDB (MMORPG database in TML)
**Reference:** [Convex](https://www.convex.dev/) (Convex Inc.)
**Date:** 2026-04-14

---

## Purpose

Convex is the closest existing architecture to UzDB's "DB-as-server" vision — it runs TypeScript
functions directly inside the database, eliminating the traditional backend server layer.
This analysis extracts architectural patterns, real-time design decisions, and limitations
relevant to UzDB's design in TML, with focus on **real-time multiplayer games**.

---

## Files

| # | File | Content |
|---|------|---------|
| 01 | [01-overview.md](01-overview.md) | What Convex is, design philosophy, comparison with SpacetimeDB |
| 02 | [02-architecture.md](02-architecture.md) | System layers, function types, transaction model, V8 runtime |
| 03 | [03-realtime.md](03-realtime.md) | Reactive query model, subscription pipeline, WebSocket protocol |
| 04 | [04-game-patterns.md](04-game-patterns.md) | Game suitability, known limits, patterns for multiplayer |
| 05 | [05-tml-mapping.md](05-tml-mapping.md) | Mapping to UzDB/TML — ADOPT / ADAPT / DISCARD |

---

## Key Findings (executive summary)

1. **Same "no intermediate server" goal, different execution model:** Convex eliminates the app
   server by running TypeScript (V8) inside the DB. SpacetimeDB runs WASM. TML compiles natively —
   no sandbox overhead at all. The goal is identical; the implementation cost differs significantly.

2. **Reactive query model is correct:** Convex's `useQuery` / subscription pattern (declare intent,
   receive automatic diffs) is the right UX for real-time clients. UzDB must implement the same.

3. **V8 is 5–10× slower than WASM for game logic:** Convex's V8 runtime adds 1–5ms per function
   call. SpacetimeDB WASM adds <0.1ms. TML native compilation adds ~0ms. For high-frequency game
   state, this difference is decisive.

4. **JSON is 10× less efficient than binary for game workloads:** Convex sends full JSON documents
   on every update, no delta encoding. At 20 updates/s for 100 players, this is ~500KB/s vs
   SpacetimeDB's ~50KB/s with BSATN. UzDB must use binary delta protocol.

5. **Stateless functions cannot implement a game loop:** Convex has no server-side tick loop.
   Game physics, AI, area-of-interest computation — all require persistent server-side state.
   SpacetimeDB (and UzDB) have this via scheduled reducers.

6. **Convex's conflict model is problematic for games:** MVCC with auto-retry on conflict works
   for web apps. For deterministic game simulations, conflicts must be handled explicitly — not
   silently retried. SpacetimeDB's single-writer model is correct for games.

7. **No spatial queries:** Convex has no spatial index. "Find entities within 100 units" requires
   a full table scan. UzDB's QuadTree/R-tree indexes are a critical differentiator.

8. **Cloud-only, vendor lock-in:** Convex cannot be self-hosted. Game studios need on-premise
   deployment for latency, compliance, and cost reasons. UzDB must be self-hostable.

---

## Gaps in Convex (opportunities for UzDB)

| Gap | UzDB opportunity |
|-----|-----------------|
| V8 runtime (5–10ms per call) | TML native compilation (~0ms overhead) |
| JSON encoding (no delta) | UzBIN binary delta protocol |
| Stateless functions (no tick loop) | `@tick(rate: 60hz)` declarative game loop |
| No spatial indexes | QuadTree / R-tree as first-class primitives |
| MVCC auto-retry (non-deterministic) | Single-writer per shard (deterministic, replayable) |
| Cloud-only | Self-hostable single binary |
| No AoI subscription | Spatial subscriptions `WITHIN RADIUS N OF SELF` |
| Full document updates | Incremental delta diffs |
