# SpacetimeDB v2.7.0 — Source-Code Analysis (implementation dossier)

| | |
|---|---|
| **Source** | https://github.com/clockworklabs/spacetimedb |
| **Version / commit** | v2.7.0 · `1a8df2a` (2026-07-13) |
| **Scale** | ~237,000 LOC Rust across 45 crates + C#/TS/C++/Unreal SDKs |
| **Purpose** | Know exactly what Fluxum will face to reach (and exceed) SpacetimeDB's level |
| **Method** | Shallow clone analyzed subsystem-by-subsystem against Fluxum's specs |

Unlike the earlier [design-phase study](../spacetimedb/00-README.md) (written from documentation,
2026-04), this dossier reads the **real implementation**. Every file ends with a
**"What Fluxum will face"** section mapped to our [specs](../../specs/README.md) and
[DAG](../../DAG.md) tasks.

## Contents

| File | Subsystem | Compares against |
|---|---|---|
| [01](01-architecture-crates.md) | Crate map, layering, dual WASM+V8 module hosts, workspace | Our 6-crate plan |
| [02](02-storage-table-engine.md) | 64 KiB BFLATN page engine, indexes, datastore MVCC/TxState | SPEC-002, SPEC-015 |
| [03](03-durability-commitlog-snapshots.md) | Commitlog (epoch+CRC32C, group commit), content-addressed snapshots, recovery | SPEC-002, SPEC-014 |
| [04](04-module-host-abi.md) | Module host, wasmtime fuel, macro→ModuleDef pipeline, hot-publish | SPEC-001, SPEC-004 |
| [05](05-query-subscriptions.md) | SQL pipeline, incremental view maintenance, fan-out dedup/pruning | SPEC-005 |
| [06](06-protocol-client-api.md) | SATS/BSATN, ws protocol v1→v3 evolution, compression, pgwire | SPEC-006 |
| [07](07-sdks-codegen-cli.md) | Codegen (5 targets), SDK client caches, CLI (18.7k LOC), templates | SPEC-011 |
| [08](08-auth-rls-scheduler.md) | OIDC identity, RLS as compiled SQL views, scheduled tables, energy | SPEC-004/005/009 |
| [09](09-ops-testing-bench.md) | Standalone vs cloud, DST harness, smoketests, benchmarks | SPEC-012/013/014 |
| [**10**](10-hard-problems.md) | **Synthesis: hard problems ranked, adopt/avoid, DAG impact** | PRD + DAG |

## Headline findings

- **Replication is absent from their OSS** (`num_replicas = 1` hard-coded) — Fluxum's replica
  sets (SPEC-014) are a real differentiator, not parity work.
- **Nobody publishes a PostgreSQL parity benchmark** (they compare only vs SQLite) — our NFR-11
  harness is an uncontested edge.
- **Subscriptions are the hardest subsystem**: their fan-out scales via query-hash dedup +
  value-level pruning, not per-client loops — SPEC-005's naive fan-out sketch must adopt this.
- **Native modules validated**: the WASM/V8 hosts, ABI marshaling, and versioned ModuleDef
  serialization account for a large share of their complexity that Fluxum skips by design.
- **Their in-RAM page format is nearly spill-ready but their indexes are not** — the genuinely
  novel work in our tiered storage (SPEC-015) is paged indexes, not paged rows.
