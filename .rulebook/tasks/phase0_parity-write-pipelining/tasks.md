## 1. Implementation
- [ ] 1.1 SDK: support multiple in-flight reducer calls per connection — pipelined request IDs with futures/callbacks matched to acks; preserve per-connection ordering semantics and error attribution (an ack/error resolves exactly its own call); document the concurrency contract (max in-flight, backpressure behavior when the window is full)
- [ ] 1.2 Bench: add a `--pipeline N` mode to the write workload (`crates/fluxum-bench`) keeping the acked-serial path as default, so the report shows both acked-serial latency and pipelined throughput as separate, honestly-labeled rows — never conflated into one number
- [ ] 1.3 Run the pipelined write workload on the same demo reducer/machine as the parity report; sweep N (e.g. 1/8/32/128) and record the throughput curve and where it flattens
- [ ] 1.4 Verification (exit test): either demonstrate ≥ 100 000 tx/s on one shard (NFR-01, SPEC-013 TST-060) under pipelining, or record precisely what still caps it (network, single-writer commit, ack path) with per-stage measurements so `phase6_load-test-security-audit` (T6.6) starts from a known ceiling — "unknown ceiling" is not an acceptable exit state

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
