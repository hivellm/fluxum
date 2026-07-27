# Parity benchmark runs — analysis snapshots

Dated, ad-hoc runs of the parity harness (`fluxum-bench report`) captured for analysis.
These are **not** release artifacts: the versioned reports of record live in
[docs/parity/](../../parity/) and only move with a release; methodology and setup are
documented in [docs/parity/spacetimedb-baseline.md](../../parity/spacetimedb-baseline.md)
and [crates/fluxum-bench/README.md](../../../crates/fluxum-bench/README.md).

| Date | Report | Branch state | Headline |
|---|---|---|---|
| 2026-07-26 | [Fluxum × SpacetimeDB head-to-head](2026-07-26-fluxum-vs-spacetimedb.md) ([raw md](raw-report-2026-07-26.md) · [raw json](raw-report-2026-07-26.json)) | `feat/tiered-live-store-integration` (phase-2 PagedTree CoW cutover in tree) | Fluxum ≥ 10× on every socket class (write 14.3×, e2e p99 12.5×); branch-local write-path regression ~3.5× vs published report, quantified and attributed |
