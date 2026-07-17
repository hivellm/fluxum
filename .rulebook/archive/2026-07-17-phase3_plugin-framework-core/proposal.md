# Proposal: phase3_plugin-framework-core

## Why
Fluxum already has several ad-hoc extension seams (AuthProvider, ColumnTransform/codec, KeyProvider, visibility(custom)) and needs new ones (full-text re-rank, external-retriever fusion, CDC) so that optional/heavy functionality — model-based scoring, Vectorizer integration — lives outside the lean core. Without a single framework these are inconsistent and there is no disciplined place to host a plugin without bloating the binary. This task builds the capability registry + hosting framework and the placement/determinism rules that keep plugins from distorting the DB. Crucially it does NOT reopen the WASM/FFI non-goal: in-process plugins are compiled, feature-gated Rust; the sidecar host (separate task) is process-isolated RPC.

## What Changes
Define a PluginRegistry and a closed capability set (each = trait + invocation site + placement class: WritePath deterministic-in-proc-only | ReadPath fallible | OffPath async). Adopt the existing seams (Auth/Transform/Key/Visibility) as capabilities via thin adapters without breaking their APIs or call sites. Implement the in-process host: link-time registration + Cargo feature gating (absent from the binary unless enabled) + catch_unwind isolation (panic disables plugin / rolls back WritePath tx). Add config.yml plugins manifest parsing and ServerBuilder::build() validation (capability exists, placement legal for host, feature compiled, applies_to targets exist). Add GET /plugins introspection and the security rules (no implicit privilege; RLS bypass only via explicit server-peer grant). The sidecar host, the query-path hooks, and the CDC sink are sibling tasks binding into this framework.

## Impact
- Governing spec: SPEC-020 (§2 capabilities, §3 placement, §4.1 in-proc host, §4.3 manifest, §7 introspection/security) — docs/specs/SPEC-020-plugin-system.md
- Related specs: SPEC-009 (AuthProvider adopted), SPEC-017 (ColumnTransform/KeyProvider adopted), SPEC-005 (visibility(custom) adopted), SPEC-001 (link-time registry DM-040), SPEC-013 (deterministic-simulation containment)
- New PRD requirements: FR-97 (plugin capability framework)
- Affected code: crates/fluxum-core (new plugin module: registry, capability traits, in-proc host), crates/fluxum-core/src/config (plugins manifest), ServerBuilder validation, crates/fluxum-server (GET /plugins)
- Depends on: T1.3 (AuthProvider), T3.x (reducer/catch_unwind isolation) — adopts existing seams
- Breaking change: NO (adapters preserve existing seam APIs; capabilities are additive)
- User benefit: one disciplined, reviewed extension framework; optional features become opt-in plugins that never bloat the core binary
