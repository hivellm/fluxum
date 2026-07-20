//! Compile gate for `fluxum generate --lang rust` (SPEC-011 SDK-050).
//!
//! The generated bindings under `tests/generated/` are produced from the T6.1
//! frozen golden schema (`crates/fluxum-server/tests/golden/schema.json`) and
//! committed. Including them here means the ordinary workspace build IS the
//! gate: if the generator emits code that does not compile against the SDK it
//! targets, `cargo test -p fluxum-sdk` goes red — the Rust analog of the
//! TypeScript generator's `tsc --noEmit` gate.
//!
//! It does more than compile them: it round-trips a real row through the
//! generated `decode`, so a generator that emits code that compiles but
//! mis-reads a column is caught too.
//!
//! Regenerate after a schema change:
//! `fluxum generate --lang rust --schema crates/fluxum-server/tests/golden/schema.json --out sdks/rust/tests/generated`
#![allow(clippy::unwrap_used, clippy::expect_used)]

#[path = "generated/mod.rs"]
mod generated;

use fluxum_sdk::protocol::FluxBinWriter;

#[test]
fn the_generated_bindings_expose_typed_rows_and_schema() {
    // A struct field, the schema-version constant, and the cache hook are all
    // present and typed — this is the "compiles against the SDK" assertion.
    // The constant is used (not asserted — it is a compile-time literal) to
    // prove it exists and is a `u32`.
    let _version: u32 = generated::SCHEMA_VERSION;
    let schema = generated::ChatMessage::table_schema();
    assert_eq!(schema.name, "ChatMessage");
}

#[test]
fn a_generated_decode_round_trips_a_real_row() {
    // Encode a ChatMessage exactly as the server would (columns in declaration
    // order), then decode it through the GENERATED decode and check equality —
    // proving the emitted reader calls match the schema's column layout.
    let mut w = FluxBinWriter::new();
    w.write_u64(7); // id
    w.write_u32(3); // channel
    w.write_str("hello").unwrap(); // body
    w.write_timestamp(1_700_000_000_000_000); // sent_at
    let bytes = w.into_bytes();

    let row = generated::ChatMessage::decode(&bytes).expect("decode");
    assert_eq!(row.id, 7);
    assert_eq!(row.channel, 3);
    assert_eq!(row.body, "hello");
    assert_eq!(row.sent_at, 1_700_000_000_000_000);

    // The primary-key projection reads the leading u64 as the key.
    let pk = (generated::ChatMessage::table_schema().pk_of_row)(&bytes);
    assert_eq!(pk, 7u64.to_le_bytes().to_vec());
}
