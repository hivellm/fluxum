# Proposal: phase2_rtree-spatial-predicates

## Why
Bounding-box queries need an R-tree, and the IN REGION / WITHIN RADIUS SQL predicates must resolve through spatial indexes - never a scan - before the subscription compiler (T4.1) can use them.

## What Changes
Implement the R-tree index and the spatial predicate evaluation (bbox prefilter + exact circle filter), with the 400/503 error paths and the 1M-point benchmark.

## Impact
- DAG task: T2.6
- Affected specs: SPEC-008
- PRD requirements: FR-61, FR-62
- Affected code: crates/fluxum-core (spatial)
- Depends on: T2.5
- Breaking change: NO
- User benefit: region subscriptions at least 10x faster than scans at 1M rows
