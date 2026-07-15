# Proposal: phase1_rich-column-types-enums-structs

## Why
The column type universe is closed: `FluxTy`/`FluxType` admit only scalars, `Option<T>`, and `Vec<T>`, and the macro rejects everything else — a map or a nested table struct is a hard compile error (crates/fluxum-macros/src/table.rs:922-991, `FluxType` enum at crates/fluxum-core/src/schema/mod.rs:72). So a status like `Todo | Doing | Done(by: Identity)` or a small nested struct must be flattened into parallel columns, which is lossy and awkward. FluxBIN already encodes enums as a `u8` tag plus payload (crates/fluxum-protocol/src/value.rs tagged forms), so the wire format is ready — this admits `#[derive(FluxType)]` enums and nested structs as first-class column types. It is wire-affecting and MUST land before the G5 wire freeze.

## What Changes
Admit `#[derive(FluxType)]` on enums (tagged unions carrying payloads) and on nested structs, and allow those types as columns. Enums encode in FluxBIN as `u8 tag + payload`; nested structs encode as their fields in sequence — additive to the existing wire format, which already tags enums. Enum/struct columns are usable in equality filters; ordering and index support are limited to the derivable memcomparable encoding (no arbitrary comparators). The macro's `parse_flux_type` gains recognition of derived types, `FluxType` gains `Enum`/`Struct` (or a nested-schema) variant, and the protocol codec threads the tag/field encoding through row and filter paths.

## Impact
- Governing spec: SPEC-023 §4 (Rich column types, DMX-030..031) — docs/specs/SPEC-023-data-model-extensions.md
- Related specs: SPEC-001 (FluxType/ColumnSchema, closed type universe, macro surface), SPEC-006 (FluxBIN wire — enum tag + payload / struct field encoding), SPEC-011 (schema JSON / SDK codegen for the new shapes)
- New PRD requirements: FR-131 (rich column types)
- Requirements covered: DMX-030, DMX-031
- Affected code: crates/fluxum-macros (derive FluxType for enums/structs; parse_flux_type in src/table.rs), crates/fluxum-core/src/schema/mod.rs (FluxType Enum/Struct variants + ColumnSchema), crates/fluxum-protocol/src/fluxbin.rs and crates/fluxum-protocol/src/value.rs (tag + payload / sequential-field encoding)
- Depends on: phase-1 macros and phase-1 FluxBIN — both archived
- Breaking change: NO (additive FluxType variants and additive wire encoding), but wire-affecting — SHOULD land before the G5 wire freeze
- User benefit: model tagged unions and nested structs directly as columns instead of flattening them, with lossless decode in every SDK
