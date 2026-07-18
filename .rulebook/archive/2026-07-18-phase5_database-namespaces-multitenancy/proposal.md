# Proposal: phase5_database-namespaces-multitenancy

## Why
Today the server binary hosts exactly one logical database: a shard owns a single MemStore + TxPipeline (crates/fluxum-core/src/store, crates/fluxum-core/src/txn), one schema, and one subscription/identity scope, and a session authenticates straight into that single store (crates/fluxum-server/src/session.rs allocates a ConnectionId with no database dimension). Running N tenants therefore means N processes — N ports, N config files, N buffer pools — which is wasteful for the many-small-tenant SaaS shape that HiveHub.Cloud targets. SPEC-025 OPS-050/051 let one binary host multiple named databases with strict isolation, which is the substrate per-tenant quotas build on. Marked P2.

## What Changes
Let the server host multiple named databases, each with its own MemStore/TxPipeline instance, schema, subscription set, and identity scope, addressable by name at connect time. A connection selects its namespace on connect and is bound to it for its lifetime; no transaction or subscription may cross a namespace boundary (strict isolation, a hard non-goal to relax). Routing (reducer calls, queries, subscriptions) is keyed by namespace, and metrics/backups/quotas are attributed per namespace (namespace label on `fluxum_*` series, per-namespace backup manifests). A single default namespace preserves today's single-DB behavior for existing deployments.

## Impact
- Governing spec: SPEC-025 §6 Database namespaces (OPS-050, OPS-051) — docs/specs/SPEC-025-operations-multitenancy.md
- Related specs: SPEC-002 (per-store MemStore/TxPipeline), SPEC-006 (session/connect), SPEC-012 (per-namespace metrics), SPEC-013/014 (per-namespace checkpoint/backup)
- New PRD requirements: FR-143 (database namespaces)
- Requirements covered: OPS-050, OPS-051
- Affected code: crates/fluxum-core/src/store + crates/fluxum-core/src/txn (per-namespace store/pipeline instances), crates/fluxum-server/src/session.rs (namespace selection on connect + lifetime binding), server routing/registry, crates/fluxum-server metrics (namespace label)
- Depends on: phase5 runtime (shard/session/transports)
- Breaking change: NO (single default namespace = current behavior)
- User benefit: multi-tenant SaaS in one process — independent DBs per tenant with strict isolation, no process-per-tenant overhead
- Priority: P2
