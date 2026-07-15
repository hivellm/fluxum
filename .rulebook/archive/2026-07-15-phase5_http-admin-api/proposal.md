# Proposal: phase5_http-admin-api

## Why
Operators and tooling need plain HTTP/JSON access for health, metrics, schema introspection, and ad-hoc reducer/query calls without an SDK.

## What Changes
Implement the HTTP/JSON admin API (:15800, axum) with unversioned paths: /health, /metrics, /schema, POST /reducer/:name, POST /query, /view/:name.

## Impact
- DAG task: T5.3
- Affected specs: SPEC-006 (FluxRPC protocol, HTTP admin section)
- PRD requirements: FR-44, FR-91
- Affected code: crates/fluxum-server (transport/admin)
- Depends on: T5.1 (phase5_fluxrpc-tcp-transport)
- Breaking change: NO
- User benefit: curl-friendly operations, monitoring, and debugging surface
