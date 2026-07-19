# Proposal: phase6_security-event-audit-alerts

## Why
OWASP Top 10:2025 findings **F-022 (Medium, A09 Logging & Alerting Failures)** and
**F-023 (Low)**: there is no durable security-event trail, and auth failures and
RLS/column-grant denials log at `debug` — invisible at the default `info` level, so
a live attack leaves no observable footprint. The abuse metrics that do exist ship
no alerting rules, so operators have nothing to fire on.

## What Changes
Structured `security`-target events that survive the default `info` filter for the
security-relevant moments (auth success/failure, connguard rejections, RLS/column
denials, admin mutations), never logging token bytes; plus a set of reference
Prometheus alert rules for the abuse metrics.

## Impact
- Affected specs: SPEC-012 (metrics/observability), SPEC logging.
- Affected code: auth path, connection guard, RLS/column-grant evaluation, admin
  dispatch, `metrics.rs`.
- Breaking change: NO (additive logging/metrics).
- User benefit: attacks are observable at the default log level and alertable;
  post-incident forensics have a durable trail.

## Notes
Should land after `phase6_admin-surface-authz` so admin-mutation and denial events
have a defined authz outcome to emit. Emits the `fluxum_admin_rejected_total` /
`fluxum_session_rejected_total` counters that the authz/session tasks introduce.
