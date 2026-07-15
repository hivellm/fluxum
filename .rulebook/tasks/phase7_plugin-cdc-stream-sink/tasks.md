## 1. Implementation
- [ ] 1.1 Bind StreamSink to the commit-log/replication stream; feed committed deltas off the commit path (PLG-050; SPEC-014)
- [ ] 1.2 Persisted per-sink offset checkpoint; restarted sink resumes from checkpoint with no missed commit (at-least-once) (PLG-050)
- [ ] 1.3 Bounded buffer + drop policy + fluxum_plugin_sink_lag; a stalled sink never back-pressures commits and is dropped past threshold (PLG-050; SUB-041)
- [ ] 1.4 Support in-process (feature-gated) and sidecar sinks (via the phase5 host) (PLG-050)
- [ ] 1.5 Verification: a sink receives every committed delta at least once; kill+restart resumes from checkpoint with no gap; a stalled sink leaves commit throughput unaffected and is dropped past the lag threshold

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
