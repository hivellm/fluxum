## 1. Implementation
- [ ] 1.1 Build the soak harness: sharded + tiered deployment loaded to >= 1 billion rows with sustained writes AND live subscriptions throughout (NFR-13, TST-112)
- [ ] 1.2 Memory-within-budget assertion for the whole soak (RSS ceiling per FR-110/TIER-004); soak report published as a release artifact
- [ ] 1.3 Small-droplet profile validation: 1 vCPU / 512 MB with dataset >= 10x RAM passes the full functional profile; idle baseline RSS < 100 MB (NFR-12, TST-110/TST-111, HWA-021)
- [ ] 1.4 Verification (DAG exit test): soak report - memory within budget throughout; droplet suite green with one documented command
- [ ] 1.5 Gate G7 input (PRD 12.2 billion-row criterion; parity report v2 owned by phase6_postgres-parity-harness)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
