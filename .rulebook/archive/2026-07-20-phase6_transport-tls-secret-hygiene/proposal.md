# Proposal: phase6_transport-tls-secret-hygiene

## Why
OWASP Top 10:2025 findings **F-011 (A04, no transport TLS)** and **F-006 (A02,
config secrets plaintext + `Serialize`-derived)**. Nothing in the codebase
terminates TLS, so on the direct-exposure posture bearer tokens and all row data
travel in cleartext over the public bind; a passive on-path observer harvests
credentials. Separately, `auth.secret`, `server_peers[].token`,
`encryption.keys[].key_hex`, `transforms.keys[].secret`, and the sidecar `token`
are plain fields on `Serialize`-derived config structs — one stray debug/serialize
of the config leaks minting material.

## What Changes
Optional built-in TLS (`rustls`) on both listeners with a guard that refuses a
non-loopback authenticating listener over plaintext; a `Secret<T>` newtype that
redacts `Debug`/`Serialize` and zeroizes on drop, wrapping every secret config
field.

## Impact
- Affected specs: SPEC transport/config; new SEC requirement — no cleartext
  credentials on a non-loopback bind.
- Affected code: both listeners (`fluxum-server`), config `mod.rs`, sidecar
  transport config.
- Breaking change: NO by default (TLS opt-in); the plaintext-on-public-bind
  refusal can be overridden by explicit opt-out for trusted-network deploys.
- User benefit: tokens and data are encrypted in transit; secrets can no longer
  leak through a serialized/`Debug`-printed config.

## Notes
Complements `phase6_admin-surface-authz` (F-001); together they make a
directly-exposed node defensible by default. Both are P0 and should precede any
public-exposure milestone.
