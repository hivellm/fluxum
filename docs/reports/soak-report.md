# Fluxum billion-row soak report

- harness `0.1.0` · 2026-07-28 · AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 127 GiB RAM)
- **verdict: FAIL ❌**

## Dataset & duration

- rows loaded: 10000000
- sustain duration: 300s
- shards: 2 reported (2 requested)

## Memory (TIER-004 / NFR-12)

- budget: 384 MiB
- tolerance: 38 MiB
- idle RSS: 12.1 MiB
- peak RSS: 1067.2 MiB
- within budget: NO
- idle RSS < 100 MB (NFR-12): yes *(recorded, not enforced for this profile)*
- eviction engaged (TST-111): yes *(recorded, not required for this profile)*

### Buffer pool per shard (TIER-080 / TST-112)

| shard | peak pool | capacity | within |
|---|---|---|---|
| 0 | 145.8 MiB | 153.6 MiB | yes |
| 1 | 145.2 MiB | 153.6 MiB | yes |

## Sustained load

- write throughput: 4052 ops/s (p99 61.89 ms)
- subscription deliveries: 83850
