## 1. Implementation
- [ ] 1.1 Evaluate `#[visibility(owner_only(field))]` per subscriber identity on InitialData AND every TxUpdate diff (FR-32, SUB-030)
- [ ] 1.2 Server-peer bypass: server identities read rows hidden from other identities (FR-72, SUB-031, AUTH-061)
- [ ] 1.3 Private tables (visibility private) never appear in any client message (SPEC-001 acceptance 9)
- [ ] 1.4 Verification (DAG exit test): RLS matrix tests - {owner, other user, server peer} x {InitialData, TxUpdate}: owner sees own rows only, others see nothing, server peer sees all (SUB acceptance 5); joint two-client Task test with SPEC-001

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
