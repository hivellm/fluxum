# Proposal: phase6_supply-chain-cargo-deny

## Why
OWASP Top 10:2025 findings **F-009 (High, A03 Software Supply Chain Failures)** and
**F-010 (Low)**: there is no `cargo-audit` / `cargo-deny` / SBOM step and no
advisory gate anywhere, so a dependency with a known RUSTSEC advisory, an
incompatible license, or an unexpected source can land silently. First-party
registry deps (e.g. `thunder-rpc`) have no pinned-source allow-list. Under the
no-GitHub-Actions constraint the gate must run in the **local** suite.

## What Changes
A `deny.toml` (advisories + bans + licenses + `[sources]` allow-list) wired into
the local gate via `cargo deny check`, plus a CycloneDX SBOM generated on release.

## Impact
- Affected specs: SPEC build/release, contribution/gate docs.
- Affected code: repo-root `deny.toml`, local gate script/`Makefile`/`justfile`,
  release process.
- Breaking change: NO (adds a gate; may flag existing advisories to triage).
- User benefit: known-vulnerable, wrongly-licensed, or unexpected-source deps are
  caught before merge; a shippable SBOM for downstream consumers.

## Notes
Independent of the other OWASP phases — can run anytime. Aligns with
[memory: no-github-actions-for-now]: gate is the local suite, not CI.
