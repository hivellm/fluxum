# Proposal: phase2_rtree-spatial-predicates

## Why
Region (bounding-box) queries need an R-tree, and the IN REGION / WITHIN RADIUS SQL predicates that subscriptions rely on need real predicate evaluation over the spatial indexes.

## What Changes
Implement the R-tree bounding-box index plus IN REGION / WITHIN RADIUS predicate evaluation over the spatial indexes.

## Impact
- DAG task: T2.6
- Affected specs: SPEC-008 (geospatial indexes)
- PRD requirements: FR-61, FR-62
- Affected code: crates/fluxum-server (storage/spatial)
- Depends on: T2.5 (phase2_quadtree-index); feeds T4.1 (SQL compiler)
- Breaking change: NO
- User benefit: region and radius subscription filters that scale far beyond linear scans
