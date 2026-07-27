# Fluxum billion-row soak report

- harness `0.1.0` · 2026-07-27 · AMD Ryzen 9 7950X3D 16-Core Processor (32 cores, 127 GiB RAM)
- **verdict: PASS ✅**

## Dataset & duration

- rows loaded: 1000000
- sustain duration: 60s

## Memory (TIER-004 / NFR-12)

- budget: 512 MiB
- tolerance: 51 MiB
- idle RSS: 13.1 MiB
- peak RSS: 396.9 MiB
- within budget: yes

## Sustained load

- write throughput: 1336 ops/s (p99 8.50 ms)
- subscription deliveries: 6450
