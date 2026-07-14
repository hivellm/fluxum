## 1. Implementation
- [ ] 1.1 Build the kill -9 harness terminating the process at every commit boundary (before append, mid-append, after append/before ack, after ack) - zero acknowledged tx lost beyond the bounded async-write window (SPEC-002 acceptance 1, NFR-08)
- [ ] 1.2 CRC corruption drills: bit-flip and truncation injected at every byte offset of the final log entry AND on cold-tier page files (replay stops at first corrupt entry, torn tail quarantined; PageCorrupt recovery from retained root + replay) (STG-031, TIER-061/TIER-062)
- [ ] 1.3 10 GB commit-log recovery benchmark: timed restart with a recent checkpoint completes in < 30 s (NFR-06, STG-032)
- [ ] 1.4 Deterministic simulation (DST) suite for storage/commitlog: seeded runtime, fault injection, model oracle (TST-130..TST-134)
- [ ] 1.5 Process-level restart/persistence drills (TST-140/TST-141)
- [ ] 1.6 Tiered recovery equivalence: with cold tier populated, crash + recovery produces logical state identical to the all-hot case (rows, indexes, tx_id, auto-inc) regardless of pre-crash residency (STG-030, SPEC-002 acceptance 6); crash during eviction loses nothing
- [ ] 1.7 Kill -9 at every checkpoint boundary (mid page write, after data/before manifest, after manifest/before CURRENT swap, after swap/before truncation) (SPEC-015 acceptance 4)
- [ ] 1.8 Verification (DAG exit test): zero committed-tx loss over the full matrix; DST green in CI
- [ ] 1.9 Gate G2 input: crash suite + recovery bench + 10x-dataset droplet run green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
