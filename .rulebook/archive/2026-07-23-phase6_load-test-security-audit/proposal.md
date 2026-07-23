# Proposal: phase6_load-test-security-audit

## Why
0.1.0 cannot ship without proof of the 100k tx/s/shard target and without an adversarial pass over auth, RLS, and SQL injection surfaces.

## What Changes
Run the load test (at least 100,000 reducer calls/s on one shard), the security audit (auth bypass, RLS bypass, SQL injection), and build the Grafana dashboard over the fluxum_* metrics.

## Impact
- DAG task: T6.6
- Affected specs: SPEC-012 (observability), SPEC-013 (testing and conformance)
- PRD requirements: NFR-01, FR-90
- Affected code: fluxum-bench (load driver), ops/grafana dashboard, audit fixes as needed
- Depends on: G5
- Breaking change: NO
- User benefit: verified headline throughput and a hardened security surface at MVP
