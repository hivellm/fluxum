# Security Policy

## Reporting a vulnerability

Please report security vulnerabilities privately to **team@hivellm.org** with the subject
`[SECURITY] fluxum: <short description>`. Do **not** open a public GitHub issue for
security-sensitive reports.

Include: affected version/commit, reproduction steps, impact assessment, and any suggested fix.
You should receive an acknowledgement within 72 hours.

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.1.x (pre-release) | Best effort — design/implementation phase |

## Security model

Fluxum's trust boundaries, as designed in the [specs](docs/specs/README.md):

- **Authentication** ([SPEC-009](docs/specs/SPEC-009-authentication.md)): clients present opaque
  tokens; `Identity = SHA-256(token)`. The `none` provider is restricted to loopback and intended
  for development only. Server peers use the reserved `SHA-256("SERVER:" + name)` namespace and
  are configured explicitly in `config.yml`.
- **Authorization**: row-level security is declarative (`#[visibility(owner_only(field))]`,
  [SPEC-005](docs/specs/SPEC-005-subscriptions.md)) and applied by the subscription engine to
  initial data and diffs; only configured server peers bypass it.
- **Isolation**: application logic runs in-process but can only mutate state through the
  transaction layer (`TxHandle`); panics are caught and rolled back
  ([SPEC-004](docs/specs/SPEC-004-reducers.md)).
- **Abuse resistance**: declarative per-identity rate limiting (`max_rate`), 3-tier fan-out
  backpressure against slow-consumer attacks, idle timeouts, and max frame size enforcement
  ([SPEC-006](docs/specs/SPEC-006-protocol-fluxrpc.md)).
- **Injection surface**: the subscription SQL compiler accepts a bounded grammar (no DDL/DML) and
  is a mandatory target of the security audit (T6.6, [SPEC-013](docs/specs/SPEC-013-testing-conformance.md)).
- **Transport security**: TLS is a post-MVP feature (FR-46); until then, deploy behind a TLS
  reverse proxy for untrusted networks. Binary transports should not be exposed publicly without
  authentication enabled.

## Hardening checklist for operators

- Never run the `none` auth provider outside loopback development.
- Set `FLUXUM_AUTH_SECRET` (and server-peer tokens) via environment, not committed config.
- Expose port 15800 (admin HTTP) only to trusted networks; it serves schema and metrics.
- Monitor `fluxum_subscriber_drops_total` and rate-limit rejection metrics for abuse signals.
