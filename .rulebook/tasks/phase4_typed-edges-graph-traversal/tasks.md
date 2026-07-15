## 1. Implementation
- [ ] 1.1 Add a `#[fluxum::edge]` macro that declares a typed directed relation `(from, to, props)` and expands to a composite-PK-indexed edge table (DMX-050; crates/fluxum-macros)
- [ ] 1.2 Represent the edge relation as a schema descriptor keyed by a `(from, to)` composite primary key (DMX-050; crates/fluxum-core/src/schema)
- [ ] 1.3 Back neighbor lookup with a composite-PK B-tree `(from, ...)` prefix scan so traversal is O(log n + k) (DMX-050; crates/fluxum-core/src/index/btree.rs)
- [ ] 1.4 Provide `traverse` helpers that walk a node's outgoing edges without invoking any general JOIN engine (point traversals only) (DMX-050; crates/fluxum-core/src/schema)
- [ ] 1.5 Make edge sets subscribable like tables so a client can subscribe to the neighbors of a node (DMX-051; crates/fluxum-core/src/subscription)
- [ ] 1.6 Deliver live edge diffs to neighbor subscribers as edges are added and removed (DMX-051; crates/fluxum-core/src/subscription)
- [ ] 1.7 Verification: `Owns` edges from `Player` to `Item` compile; subscribing to a player's `Owns` neighbors returns that player's items and live diffs; traversal uses the composite-PK prefix scan (no full scan)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
