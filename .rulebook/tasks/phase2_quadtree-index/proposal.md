# Proposal: phase2_quadtree-index

## Why
Native geospatial indexing is a headline differentiator vs SpacetimeDB's O(n) location filters; the QuadTree is the P0 point/radius workhorse.

## What Changes
Implement the BTreeMap-backed QuadTree (insert/point/radius/delete), configurable bucket size, update coherence on coordinate moves, and the event-stream advisory lint.

## Impact
- DAG task: T2.5
- Affected specs: SPEC-008 (SPX-001..040)
- PRD requirements: FR-60
- Affected code: crates/fluxum-core (spatial)
- Depends on: T2.4
- Breaking change: NO
- User benefit: O(log n + k) geospatial live queries
