## 1. Implementation
- [ ] 1.1 Fail-closed RLS visibility (F-003): make `shard_local` / `custom` / `member_of` either enforce a real filter or be rejected at schema-load with a clear error, so a declared rule never silently means "no filter" (`sql/mod.rs:743-765`); add a regression test per mode
- [ ] 1.2 Asymmetric JWT provider variant (F-019): a verify-only provider that holds a public key, so the DB never stores token-minting material; symmetric HS256 stays available but is documented as lower-assurance
- [ ] 1.3 Sidecar transport hardening (F-021): document and enforce mTLS-or-loopback as a hard requirement for the sidecar channel; treat a response decode failure as a breaker trip rather than a trusted value
- [ ] 1.4 Bound permissive-auth identity minting (F-020): cap/observe distinct identities minted under permissive auth so it cannot be used to multiply identities without limit
- [ ] 1.5 Metrics: `fluxum_rls_denied_total{mode}`, sidecar decode-failure / breaker-trip counter; wired for the new fail-closed paths
- [ ] 1.6 Spec: SPEC-009 auth (asymmetric provider + permissive bound), SPEC RLS/visibility (fail-closed modes), SPEC sidecar (mandatory mTLS/loopback)
- [ ] 1.7 Verification: a schema declaring an unimplemented visibility mode is rejected (or filters correctly) — never silently open; an asymmetric-provider token verifies with the public key and the DB holds no secret; a corrupt/unauthenticated sidecar response trips the breaker instead of being trusted; permissive-auth identity minting is bounded

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
