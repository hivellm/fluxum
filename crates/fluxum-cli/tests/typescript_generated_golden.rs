//! SPEC-011 SDK-021 / FR-82 — the committed TypeScript bindings stay in sync
//! with the generator.
//!
//! The committed files live under `sdks/typescript/tests/generated/` and are
//! generated from the DEMO module's schema golden
//! (`crates/fluxum-server/tests/golden/demo-schema.json`) — the module the
//! served binary actually links — so the `generated.e2e` test over there can
//! drive a real server through them (T6.5 1.6: the demo end-to-end on the
//! generated SDK). The Rust twin is `rust_generated_golden.rs`.
//!
//! Regenerating from the golden must reproduce every committed file byte for
//! byte, so a schema change that was not regenerated fails here. Set
//! `FLUXUM_REGEN=1` to rewrite them.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;

use fluxum_cli::generate::{Lang, generate, load_schema};

const GOLDEN: &str = include_str!("../../fluxum-server/tests/golden/demo-schema.json");

fn committed_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../sdks/typescript/tests/generated")
}

#[test]
fn committed_typescript_bindings_match_a_fresh_generation() {
    // Load the golden schema through the same canonicalization the CLI uses.
    let dir = tempfile::tempdir().unwrap();
    let schema_path = dir.path().join("schema.json");
    std::fs::write(&schema_path, GOLDEN).unwrap();
    let doc = load_schema(schema_path.to_str().unwrap()).unwrap();

    let files = generate(Lang::TypeScript, &doc).unwrap();
    assert!(!files.is_empty());

    let committed = committed_dir();
    if std::env::var_os("FLUXUM_REGEN").is_some() {
        std::fs::create_dir_all(&committed).unwrap();
        for (name, contents) in &files {
            std::fs::write(committed.join(name), contents).unwrap();
        }
        return;
    }

    for (name, fresh) in &files {
        let on_disk = std::fs::read_to_string(committed.join(name))
            .unwrap_or_default()
            .replace("\r\n", "\n");
        assert_eq!(
            on_disk.trim_end(),
            fresh.trim_end(),
            "sdks/typescript/tests/generated/{name} is out of sync with the generator — \
             regenerate with FLUXUM_REGEN=1 cargo test -p fluxum-cli --test typescript_generated_golden"
        );
    }
}
