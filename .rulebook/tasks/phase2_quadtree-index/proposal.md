# Proposal: phase2_quadtree-index

## Why
Point/radius queries over positional data are a first-class workload; a BTreeMap-backed quadtree gives cache-friendly spatial lookups without pointer chasing.

## What Changes
Implement the QuadTree spatial index (BTreeMap-backed, no pointer chasing) with insert, point query, radius query, and delete.

## Impact
- DAG task: T2.5
- Affected specs: SPEC-008 (geospatial indexes)
- PRD requirements: FR-60
- Affected code: crates/fluxum-server (storage/spatial)
- Depends on: T2.4 (phase2_btree-indexes)
- Breaking change: NO
- User benefit: efficient point and radius queries over #[spatial] columns
