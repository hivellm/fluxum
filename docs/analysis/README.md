# Fluxum — Reference Analysis

Reference studies that ground Fluxum's design. These documents were produced for the UzDB design
phase (the direct predecessor of Fluxum) and are preserved here as the evidence base for the
architectural decisions in the [PRD](../PRD.md) and [ARCHITECTURE.md](../ARCHITECTURE.md).

> **Provenance note:** these studies were written when the project targeted a domain-specific
> use case, so some sections analyze the reference systems through that lens (file names such as
> `04-game-patterns.md` / `08-mmorpg-patterns.md` reflect the studied systems' own ecosystems and
> the original scope). Fluxum itself is a **general-purpose realtime database** — the technical
> conclusions (storage model, transaction semantics, subscription protocol, encoding trade-offs)
> are what carried over; the domain framing did not. Files analyzing the mapping to the TML
> language (`*-tml-mapping.md`, `10-tml-stdlib.md`) are historical: Fluxum is implemented in Rust.

## Contents

| Study | Files | What it informed |
|---|---|---|
| [SpacetimeDB](spacetimedb/00-README.md) | 10 files: overview, architecture, data model, modules/reducers, transactions, subscriptions, protocol, usage patterns, mappings | The core database-as-a-server concept: reducers, push subscriptions, in-memory + commit log, BSATN encoding, Identity model. Fluxum adopts its strengths and fixes its ceilings |
| [Convex](convex/00-README.md) | 6 files | Reactive query/subscription ergonomics, developer experience of transactional functions |
| [SurrealDB](surrealdb/00-README.md) | 6 files | Live queries, multi-model trade-offs, what to deliberately *not* build |
| [Gaps analysis](gaps-analysis.md) | 1 file | The improvement catalogue applied to the specs: FluxBIN row-encoding split (C1), enriched `TxUpdate` (C2), composite PKs (C3), fan-out backpressure (C4), tick drift semantics (H1), intra-transaction reads (H2), declarative rate limiting (H3), server-to-server identity (H4), `SubscribeSingle` (H5) |

> The gaps analysis is preserved verbatim from the UzDB design set (it uses the original UzDB /
> UzRPC / UzBIN names and the original game examples). Name mapping: UzDB → Fluxum ·
> UzRPC → FluxRPC · UzBIN → FluxBIN · `@decorator` (TML) → `#[fluxum::…]` (Rust). Every C/H/M
> item listed there is already folded into the current [specs](../specs/README.md).

## How to read this alongside the specs

- Want to know **why** a spec says what it says → find the topic here.
- Want to know **what to build** → the [specs](../specs/README.md) are normative; where this
  analysis and a spec disagree, the spec wins.
