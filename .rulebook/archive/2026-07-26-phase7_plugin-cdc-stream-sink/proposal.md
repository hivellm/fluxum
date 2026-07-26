# Proposal: phase7_plugin-cdc-stream-sink

## Why
Integrating with external systems — above all feeding Vectorizer's embedding pipeline with changed rows — needs a change-data-capture path: a plugin that receives committed deltas and pushes them out. This must run off the commit path so it never stalls the single-writer, and it must be at-least-once so an external index cannot silently miss updates. Fluxum's commit log already IS the replication stream, so the CDC sink reuses that substrate rather than inventing a new one. This is the OffPath capability of the plugin system and the substrate for Vectorizer ingestion and generic integrations.

## What Changes
Bind the StreamSink capability (defined by phase3) to the commit-log/replication stream (SPEC-014): feed each sink committed deltas off the commit path, with a persisted per-sink offset checkpoint for at-least-once resume after restart. Add a bounded buffer with a drop policy and fluxum_plugin_sink_lag metric so a slow/failed sink is dropped past threshold and never back-pressures commits (the SUB-041 non-blocking guarantee applies). Sinks may be in-process (feature-gated) or sidecar (phase5 host) — e.g. a Vectorizer ingest sidecar that embeds changed name/description fields.

## Impact
- Governing spec: SPEC-020 (§6 CDC stream sink PLG-050) — docs/specs/SPEC-020-plugin-system.md
- Related specs: SPEC-014 (commit-log/replication stream substrate), SPEC-005 (SUB-041 non-blocking guarantee), SPEC-002 (commit log), SPEC-012 (sink lag metric)
- New PRD requirements: FR-97 (plugin framework — CDC capability)
- Affected code: crates/fluxum-core (StreamSink dispatch off the commit path, offset checkpoint, bounded buffer), commit-log/replication tail, metrics
- Depends on: phase3_plugin-framework-core (capability + placement); T7.1 (replication streaming) — commit-log stream substrate; optionally phase5_plugin-sidecar-host for sidecar sinks
- Breaking change: NO (additive; sinks are opt-in via config)
- User benefit: reliable at-least-once CDC out of Fluxum — Vectorizer ingestion and external integrations without touching the commit path or risking missed updates
