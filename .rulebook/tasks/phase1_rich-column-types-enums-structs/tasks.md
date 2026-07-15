## 1. Implementation
- [ ] 1.1 Add a `#[derive(FluxType)]` proc-macro for enums (tagged unions with payloads) and nested structs, emitting their column-type descriptor (DMX-030; crates/fluxum-macros)
- [ ] 1.2 Extend `parse_flux_type` / the closed universe so a field of a derived enum or struct type is accepted as a column instead of a compile error (DMX-030; crates/fluxum-macros/src/table.rs)
- [ ] 1.3 Add `Enum`/`Struct` (nested-schema) variants to `FluxType` and carry the variant/field layout on ColumnSchema (DMX-030; crates/fluxum-core/src/schema/mod.rs)
- [ ] 1.4 Encode enums in FluxBIN as `u8 tag + payload` and nested structs as their fields in sequence, with byte-exact roundtrip (DMX-030; crates/fluxum-protocol/src/fluxbin.rs, crates/fluxum-protocol/src/value.rs)
- [ ] 1.5 Support enum/struct columns in equality filters (tag + payload equality) (DMX-031; crates/fluxum-core/src/schema, filter path)
- [ ] 1.6 Limit ordering/index support to the derivable memcomparable encoding and reject index/order requests the encoding cannot satisfy (DMX-031; crates/fluxum-core/src/schema)
- [ ] 1.7 Emit the enum/struct shapes into the schema JSON so SDK codegen can decode them losslessly (DMX-030; crates/fluxum-core/src/schema)
- [ ] 1.8 Verification: a `Task.status` enum `Todo | Doing | Done(by: Identity)` and a nested-struct column compile; `WHERE status = Done` matches; FluxBIN roundtrip is byte-exact and decodes in the schema JSON

## 2. Tail (docs + tests — check or waive with tailWaiver)
- [ ] 2.1 Update or create documentation covering the implementation
- [ ] 2.2 Write tests covering the new behavior
- [ ] 2.3 Run tests and confirm they pass
