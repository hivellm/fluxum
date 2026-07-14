# Proposal: phase1_auth-providers

## Why
Every connection needs a stable `Identity` before transports, RLS, and rate limiting can be built; the provider trait keeps auth pluggable without touching the core.

## What Changes
Implement the object-safe `AuthProvider` trait with `token`/`jwt`/`none` built-in providers, `Identity = SHA-256(token)` derivation, and the server-to-server identity `SHA-256("SERVER:" + name)`.

## Impact
- DAG task: T1.3
- Affected specs: SPEC-009 (authentication)
- PRD requirements: FR-70, FR-71, FR-72
- Affected code: crates/fluxum-core (auth module)
- Depends on: G0
- Breaking change: NO
- User benefit: stable identities across reconnects and pluggable authentication out of the box
