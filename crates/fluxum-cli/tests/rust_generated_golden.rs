//! SPEC-011 SDK-050 — the committed Rust bindings stay in sync with the
//! generator.
//!
//! The compile gate itself lives in `sdks/rust/tests/generated_compiles.rs`:
//! the committed bindings under `sdks/rust/tests/generated/` are compiled
//! against the SDK by the ordinary workspace build, and a real row is
//! round-tripped through the generated `decode`. That is the Rust analog of
//! the TypeScript `tsc` gate — no separate toolchain to install.
//!
//! This test closes the other half: regenerating from the T6.1 golden schema
//! must reproduce the committed file byte for byte, so a schema change that
//! was not regenerated fails here. Set `FLUXUM_REGEN=1` to rewrite it.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;

use fluxum_cli::generate::{Lang, generate, load_schema};

const GOLDEN: &str = include_str!("../../fluxum-server/tests/golden/schema.json");

fn committed_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../sdks/rust/tests/generated/mod.rs")
}

#[test]
fn committed_rust_bindings_match_a_fresh_generation() {
    // Load the golden schema through the same canonicalization the CLI uses.
    let dir = tempfile::tempdir().unwrap();
    let schema_path = dir.path().join("schema.json");
    std::fs::write(&schema_path, GOLDEN).unwrap();
    let doc = load_schema(schema_path.to_str().unwrap()).unwrap();

    let files = generate(Lang::Rust, &doc).unwrap();
    let fresh = files
        .get("mod.rs")
        .expect("the Rust generator emits mod.rs");

    let path = committed_path();
    if std::env::var_os("FLUXUM_REGEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, fresh).unwrap();
        return;
    }

    let committed = std::fs::read_to_string(&path)
        .unwrap_or_default()
        .replace("\r\n", "\n");
    assert_eq!(
        committed.trim_end(),
        fresh.trim_end(),
        "sdks/rust/tests/generated/mod.rs is out of sync with the generator — regenerate with \
         FLUXUM_REGEN=1 cargo test -p fluxum-cli --test rust_generated_golden"
    );
}
