# Proposal: phase1_column-transforms-type-surface

## Why
Real applications wrap the DB in a DTO layer to normalize values (money as exact fixed-point, timestamps to canonical UTC, strings to canonical Unicode/case) and to protect sensitive fields. Fluxum has no per-column transform seam: FluxType is a closed universe, ColumnSchema carries only {name, ty}, and the macro parses only #[primary_key]/#[auto_inc]/#[default]/#[rename] (crates/fluxum-macros/src/table.rs:99-111). This task lays the type-system and attribute foundation that the crypto (phase3) and column-security (phase4) tasks build on. The wire-affecting parts (FluxType::Decimal, transform-aware stored types) MUST land before the G5 wire freeze.

## What Changes
Add the per-column transform attribute surface (#[normalize]/#[encrypted]/#[signed]/#[masked]/#[column_grant]) parsed by the proc-macro; a first-class FluxType::Decimal { scale } newtype with its FluxBIN encoding; the ColumnTransform trait + TransformCtx + TransformDescriptor; the money/datetime/string normalizers; and the additive ColumnSchema extension (stored_ty, transforms, grant, mask) with ServerBuilder::build() validation. Crypto execution and read-path masking are out of scope here (phase3/phase4) — this task defines the shapes, parsing, validation, and the deterministic normalizers.

## Impact
- Governing spec: SPEC-017 (Column Transforms, §2 attribute surface, §3 trait, §4 normalization, §7 schema/registry) — docs/specs/SPEC-017-column-transforms.md
- Related specs: SPEC-001 (data model: FluxType, ColumnSchema, TableSchema, macro surface), SPEC-006 (FluxBIN wire — new Decimal tag), SPEC-011 (schema JSON / SDK codegen)
- New PRD requirements: FR-90 (column transforms), FR-92 (native decimal/normalized types)
- Affected code: crates/fluxum-macros/src/table.rs (attribute parsing), crates/fluxum-core/src/schema (ColumnSchema/FluxType/TableSchema), crates/fluxum-protocol (FluxBIN Decimal encoding), crates/fluxum-core (ColumnTransform trait + normalizers)
- Depends on: T1.1 (data-model macros), T1.2 (FluxValue + FluxBIN) — both archived
- Sequencing: wire-affecting (Decimal + stored_ty) — SHOULD land before the G5 freeze
- Breaking change: NO (additive attributes, additive FluxType variant, additive ColumnSchema fields)
- User benefit: exact money/decimal, canonical datetime/string, and the declarative transform surface — no hand-rolled DTOs
