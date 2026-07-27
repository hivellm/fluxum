## 1. Implementation
- [ ] 1.1 Node-format extension: overflow-key entry tag (inline order-preserving prefix + full key in a TIER-026 overflow chain), page-format version bump with decode compatibility for existing tags
- [ ] 1.2 Routing/search/scan/split/bulk_load over prefix-tied keys: full-key comparison faults overflow pages only on prefix collision; splits and bulk loads keep fan-out ≥ 2 regardless of key length; supersede walks key chains like value chains
- [ ] 1.3 Lift the `max_key` cap on the live path (primary, secondary, `#[unique]`) and restore `recovery_bench` PAYLOAD_BYTES to 2048 as the regression witness
- [ ] 1.4 Property tests: ordering equivalence vs a resident oracle over arbitrary-length keys (incl. shared multi-KB prefixes), CoW snapshot correctness across overflow-key writes

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation (tree.rs module docs, SPEC-015 TIER-021 notes)
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
