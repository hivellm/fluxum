## 1. Implementation
- [ ] 1.1 Add a `CrdtText` FluxType and its stored representation (converged value plus CRDT metadata) (DMX-060; crates/fluxum-core/src/schema/mod.rs)
- [ ] 1.2 Define the character-level edit-op model (insert/delete with position identifiers) for the column (DMX-060; crates/fluxum-core/src/schema/mod.rs)
- [ ] 1.3 Apply and merge edit ops deterministically within the single-writer shard boundary so all replicas of the value converge (DMX-060; crates/fluxum-core/src/reducer)
- [ ] 1.4 Express edit ops as reducer calls that ride the existing single-writer serialization (DMX-061; crates/fluxum-core/src/reducer)
- [ ] 1.5 Encode compact op diffs on the wire instead of full-document rewrites (DMX-061; crates/fluxum-protocol)
- [ ] 1.6 Fan out op diffs to subscribers of a `CrdtText` column, exposing the converged value (DMX-060, DMX-061; crates/fluxum-core/src/subscription)
- [ ] 1.7 Verification: a `Doc.body: CrdtText` compiles; two concurrent editors inserting at the same position in overlapping transactions converge to the same text; subscribers receive compact op diffs, not full rewrites

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
