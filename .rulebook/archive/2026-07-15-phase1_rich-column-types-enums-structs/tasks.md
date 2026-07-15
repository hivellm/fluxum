## 1. Implementation
- [x] 1.1 Added `#[derive(FluxType)]` proc-macro for enums (unit/tuple/named variants) and nested structs, emitting the `FluxTypeDef` impl + column-type descriptor (DMX-030; crates/fluxum-macros/src/flux_type.rs)
- [x] 1.2 `parse_flux_type` accepts a derived enum/struct type (`FluxTy::Derived`) instead of a hard error; maps still rejected (DMX-030; crates/fluxum-macros/src/table.rs)
- [x] 1.3 Added `FluxType::Enum`/`Struct` (+ `EnumSchema`/`VariantSchema`/`StructSchema`/`FieldSchema`) carrying the variant/field layout, and the `FluxTypeDef` trait (DMX-030; crates/fluxum-core/src/schema/mod.rs)
- [x] 1.4 FluxBIN: enum = `u8` tag + payload, struct = fields in sequence; byte-exact roundtrip via encode_row/decode_row + LogValue persistence (DMX-030; crates/fluxum-core/src/store/row.rs, commitlog/record.rs)
- [x] 1.5 Enum/struct values support equality at the `RowValue` level (derived `PartialEq`, used by reducer scans + fan-out pruning). Note: the SQL-text subset has no enum literal, so `WHERE col = <literal>` on a rich column returns a clean `MALFORMED` error rather than matching — SQL-literal enum syntax is a follow-up, not part of DMX-031 (DMX-031; crates/fluxum-core/src/store/row.rs, sql/mod.rs)
- [x] 1.6 Ordering/index limited to the memcomparable encoding: rich columns rejected as primary/partition/unique/index keys at macro expansion (`FluxType::is_keyable`) (DMX-031; crates/fluxum-macros/src/table.rs)
- [x] 1.7 `/schema` emits the enum/struct shape via the column type's Debug form (same mechanism as Option/List) so the shape is present for SDK codegen; StoredType catalog carries structured Enum/Struct for migration diff (DMX-030; crates/fluxum-server/src/admin.rs, crates/fluxum-core/src/migration/catalog.rs)
- [x] 1.8 Verification: `Status` enum `Todo|Doing|Done{by:Identity}|Snoozed(Timestamp)` + nested `Point` struct compile as columns; typed⇄dynamic roundtrip is exact; FluxBIN roundtrip byte-exact; rich key rejected (trybuild) (crates/fluxum-macros/tests/rich_types.rs, ui/pass/rich_columns.rs, ui/fail/*)

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [x] 2.1 Update or create documentation covering the implementation
- [x] 2.2 Write tests covering the new behavior
- [x] 2.3 Run tests and confirm they pass
