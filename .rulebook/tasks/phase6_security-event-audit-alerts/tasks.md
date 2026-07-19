## 1. Implementation
- [ ] 1.1 Structured `security`-target events (own tracing target so they survive the default `info` filter) for: auth success/failure (+ source IP, reason), connection-guard rejections, RLS/column-grant denials, and admin mutations (operator identity + route). Never log token bytes or secret material
- [ ] 1.2 Consistent event schema: `event`, `outcome`, `identity`/`operator` (where known), `source_ip`, `reason`, `resource`; emitted via a single helper so fields stay uniform across call sites
- [ ] 1.3 Raise the security-relevant denials from `debug` to the `security` target (auth failure, RLS deny, column-grant deny) so they are visible at default level without turning on global debug
- [ ] 1.4 Reference Prometheus alert rules (shipped in `docs/`): auth-failure rate, rejection spike, slow-reducer WARN rate, queue/backpressure saturation; each with a short runbook note
- [ ] 1.5 Spec: SPEC-012 gains the security-event trail + alerting requirement; logging spec documents the `security` target and the "never log secrets" rule
- [ ] 1.6 Verification: an auth failure and an RLS denial are visible at default `info` on the `security` target with source IP and reason and no token bytes; admin mutations emit an operator-attributed event; the alert rules load and evaluate against the exported metrics

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
