## 1. Implementation
- [ ] 1.1 Add repo-root `deny.toml`: `[advisories]` (deny RUSTSEC, allow-list with justification for any accepted), `[bans]` (deny duplicate/yanked, wildcard versions), `[licenses]` (allow-list matching the workspace's license policy), `[sources]` allow-list covering the first-party registry source for `thunder-rpc` and any other internal deps (F-010)
- [ ] 1.2 Wire `cargo deny check` into the local gate (script / `Makefile` / `justfile`) alongside the existing suite + clippy + coverage, so it runs pre-merge without GitHub Actions
- [ ] 1.3 Triage the initial `cargo deny check` output: pin/upgrade or record an explicit, dated exception for each current advisory/license finding
- [ ] 1.4 Generate a CycloneDX SBOM on release (`cargo cyclonedx`); document where the artifact lands
- [ ] 1.5 Spec/docs: build/release + contribution gate updated to require `cargo deny check` green; SBOM step documented
- [ ] 1.6 Verification: `cargo deny check` runs clean (or with only documented exceptions) in a clean worktree; an injected test advisory/denied-license/unlisted-source fails the gate; SBOM generates and validates

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
