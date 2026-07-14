## 1. Implementation
- [ ] 1.1 Build the soak driver: load 1B rows across shards with tiered storage, then sustain mixed writes + live subscriptions
- [ ] 1.2 Run the billion-row soak with continuous invariant checks (correctness sampling, memory.budget compliance, latency percentiles)
- [ ] 1.3 Run the small-droplet validation: 1 vCPU / 512 MB profile with dataset at least 10x RAM under realistic load
- [ ] 1.4 Capture metrics throughout and generate the soak report artifact
- [ ] 1.5 Fix any budget violations or degradations surfaced and re-run to a clean pass
- [ ] 1.6 Verification (DAG exit test): soak report complete; memory stays within budget throughout both runs
- [ ] 1.7 Gate G7 input: PRD section 12.2 all green - failover + PITR + 5 SDKs + 1B-row soak + parity report v2 (release 0.2.0)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
