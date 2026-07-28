# Proposal: phase7_billion-row-soak-droplet-validation

## Why
The scale (1B rows, sharded + tiered) and frugality (1 vCPU / 512 MB) claims are launch-defining; only sustained soak runs on both extremes prove them.

## What Changes
Run the billion-row soak (sharded + tiered storage under sustained writes and subscriptions) and the small-droplet profile validation (1 vCPU / 512 MB, dataset at least 10x RAM), producing the soak report.

## Impact
- DAG task: T7.7
- Affected specs: SPEC-013 (testing and conformance), SPEC-015 (tiered storage)
- PRD requirements: NFR-12, NFR-13
- Affected code: fluxum-bench (soak driver), ops profiles, soak report artifact
- Depends on: G6
- Breaking change: NO
- User benefit: proven operation at billion-row scale and on tiny cloud instances
