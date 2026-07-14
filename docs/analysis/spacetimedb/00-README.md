# SpacetimeDB — Reference Analysis

**Project:** UzDB (MMORPG database in TML)
**Reference:** [SpacetimeDB](https://github.com/ClockworkLabs/SpacetimeDB) (Clockwork Labs)
**Date:** 2026-04-14

---

## Purpose

SpacetimeDB is the closest existing project to UzDB's vision: a database that is also a server,
designed from the ground up for massive real-time games. This analysis extracts patterns,
architectural decisions, and anti-patterns from SpacetimeDB to inform UzDB's design in TML.

---

## Files

| # | File | Content |
|---|------|---------|
| 01 | [01-overview.md](01-overview.md) | What SpacetimeDB is, design philosophy, comparison with alternatives |
| 02 | [02-architecture.md](02-architecture.md) | System layers, core components, data flow |
| 03 | [03-data-model.md](03-data-model.md) | Table model, types, indexes, in-memory storage |
| 04 | [04-modules-reducers.md](04-modules-reducers.md) | WASM module system, reducers, ABI, lifecycle |
| 05 | [05-transactions.md](05-transactions.md) | ACID transactions, MVCC, commit log, durability |
| 06 | [06-subscriptions.md](06-subscriptions.md) | Subscription pipeline, RLS, client sync |
| 07 | [07-protocol.md](07-protocol.md) | HTTP + WebSocket protocol, BSATN encoding, client SDKs |
| 08 | [08-mmorpg-patterns.md](08-mmorpg-patterns.md) | BitCraft Online patterns, scalability, real-time design |
| 09 | [09-tml-mapping.md](09-tml-mapping.md) | Mapping to UzDB/TML — what to adopt, adapt, or discard |
| 10 | [10-tml-stdlib.md](10-tml-stdlib.md) | TML stdlib inventory — what exists vs what UzDB must build |

---

## Key Findings (executive summary)

1. **DB-as-server:** eliminating the intermediate server layer is the central design decision.
   UzDB must follow the same principle — game logic lives inside the database.

2. **Reducers = game API:** all state mutations go through atomic reducers.
   In TML, this maps to functions annotated with `@reducer` compiled into the UzDB runtime.

3. **Incremental subscriptions:** clients do not poll — they receive automatic diffs.
   This is the correct pattern for MMORPGs with thousands of concurrent players.

4. **In-memory + commit log:** everything in RAM, durability via append-only log.
   TML must implement the same: `MemStore` + `CommitLog`.

5. **WASM as logic sandbox:** SpacetimeDB uses WASM to isolate user modules.
   For UzDB in TML, the TML runtime replaces the WASM sandbox with native compilation.

6. **BSATN (Binary SpacetimeDB Algebraic Type Notation):** efficient binary protocol.
   UzDB should define its own equivalent binary protocol or use BSATN as a base.

7. **BitCraft proves the model:** 150,000 tx/s vs 1,500 tx/s with Node.js+PostgreSQL.
   The model works in production for a real MMORPG.

---

## Gaps in SpacetimeDB (opportunities for UzDB)

- **Limited horizontal scale:** SpacetimeDB is primarily single-node. UzDB can design native sharding from the start.
- **WASM FFI overhead:** JIT compilation and FFI boundary latency. TML compiles natively, eliminating this layer.
- **No native spatial queries:** MMORPGs need region/proximity queries. UzDB can include spatial indexes (quadtree/R-tree) as first-class primitives.
- **Manual schema migrations:** SpacetimeDB does not automate migrations. UzDB can improve this significantly.
