## 1. Implementation
- [ ] 1.1 Extend the proc-macro per-field Column model with an ordered transforms pipeline and parse #[normalize(kind, ...)], #[encrypted(scheme, key)], #[signed(scheme, by)], #[masked(strategy)], #[column_grant(select=...)] (CT-001, CT-003; crates/fluxum-macros/src/table.rs)
- [ ] 1.2 trybuild golden diagnostics for invalid combinations: two #[encrypted] on one column, #[encrypted] on #[primary_key]/#[index]/partition_by/#[spatial], #[normalize(money)] on a non-Decimal column, unknown scheme/kind (CT-002, CT-013)
- [ ] 1.3 Add FluxType::Decimal { scale: u8 } + fluxum::Decimal { unscaled: i128, scale: u8 } newtype; comparison/ordering on the exact rational value (CT-020)
- [ ] 1.4 FluxBIN encoding for Decimal (i128 LE unscaled + u8 scale) — assign a new wire tag; roundtrip property tests byte-exact (CT-020; SPEC-006) — MUST precede G5 freeze
- [ ] 1.5 Define ColumnTransform trait + TransformCtx + TransformDescriptor (CT-010)
- [ ] 1.6 Implement deterministic normalizers: money (scale + optional currency metadata, reject precision loss), datetime (canonical UTC micros, optional assume_tz), string (nfc/nfkc, case fold/lower, trim) (CT-021/022/023)
- [ ] 1.7 Extend ColumnSchema additively: stored_ty, transforms, grant, mask; thread through TableSchema generation (CT-050)
- [ ] 1.8 ServerBuilder::build() validation for transform/type mismatches and normalizer target-type rules (CT-051)
- [ ] 1.9 Verification: example schema with money/datetime/string + a declared (not-yet-executed) #[encrypted] column compiles; schema registry reflects transforms; Decimal roundtrip + normalizer property tests green

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
