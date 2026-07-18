//! The gate for the vendored wire layer.
//!
//! `src/protocol/` is a verbatim copy of `crates/fluxum-protocol/src/`, so the
//! SDK can publish without an internal crate going to crates.io with it.
//! Duplicating a wire format between a server and its own client is normally
//! how the two drift apart, and an encoding disagreement does not fail one
//! message — it desynchronizes the connection. This test is what makes the
//! duplication safe: the copies cannot differ by a byte and still be green.
//!
//! The server-side crate is the source of truth. After editing it, re-sync
//! with:
//!
//! ```text
//! SYNC_PROTOCOL=1 cargo test -p fluxum-sdk --test protocol_sync
//! ```
//!
//! which rewrites the copies and then asserts as usual. The regenerate path
//! lives here rather than in a binary target so it does not ship inside the
//! published package — it is workspace maintenance, not SDK functionality.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;

/// The mirrored file set. `no_vendored_file_is_left_behind_or_missing` pins
/// this against what is actually on disk, so it cannot quietly go stale.
const VENDORED_MODULES: [&str; 7] = [
    "codes", "fluxbin", "frame", "messages", "rowlist", "tagged", "value",
];

fn sdk_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// `None` when the server-side crate is absent — the published-package case,
/// where `cargo test` runs against the vendored copy alone and has nothing to
/// compare against. Skipping there is correct; skipping in the workspace would
/// silently disarm the gate, which `source_crate_is_present_in_the_workspace`
/// prevents.
fn source_dir() -> Option<PathBuf> {
    let dir = sdk_root().join("../../crates/fluxum-protocol/src");
    dir.is_dir().then_some(dir)
}

#[test]
fn source_crate_is_present_in_the_workspace() {
    // Guards the skip in every other test here: inside the workspace the
    // source must exist, so a wrong path can never turn the sync check into a
    // no-op that passes.
    let in_workspace = sdk_root().join("../../Cargo.toml").is_file();
    if !in_workspace {
        return;
    }
    assert!(
        source_dir().is_some(),
        "in the workspace but crates/fluxum-protocol/src is missing — the \
         sync check would silently pass against nothing"
    );
}

#[test]
fn vendored_modules_match_the_source_byte_for_byte() {
    let Some(source) = source_dir() else {
        return;
    };
    let dest = sdk_root().join("src/protocol");
    // Opt-in regenerate. Only ever writes into the SDK: the server-side crate
    // is the source of truth and is never touched here.
    let bless = std::env::var_os("SYNC_PROTOCOL").is_some();

    for module in VENDORED_MODULES {
        let from = source.join(format!("{module}.rs"));
        let to = dest.join(format!("{module}.rs"));

        let expected = std::fs::read(&from)
            .unwrap_or_else(|e| panic!("cannot read source {}: {e}", from.display()));

        if bless {
            // Skip untouched files so blessing does not churn mtimes and
            // force a rebuild of everything.
            if std::fs::read(&to).is_ok_and(|existing| existing == expected) {
                continue;
            }
            std::fs::write(&to, &expected)
                .unwrap_or_else(|e| panic!("cannot write {}: {e}", to.display()));
            println!("synced {module}.rs");
            continue;
        }

        let actual = std::fs::read(&to)
            .unwrap_or_else(|e| panic!("cannot read vendored {}: {e}", to.display()));

        assert!(
            expected == actual,
            "{module}.rs has drifted from crates/fluxum-protocol/src/{module}.rs.\n\
             The server and this SDK would speak different protocols.\n\
             Fix: SYNC_PROTOCOL=1 cargo test -p fluxum-sdk --test protocol_sync"
        );
    }
}

#[test]
fn no_vendored_file_is_left_behind_or_missing() {
    // A module deleted from the source, or added without updating the list,
    // would otherwise pass unnoticed: the byte comparison only walks the list.
    let dest = sdk_root().join("src/protocol");
    let mut found: Vec<String> = std::fs::read_dir(&dest)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".rs") && name != "mod.rs")
        .collect();
    found.sort();

    let mut expected: Vec<String> = VENDORED_MODULES
        .iter()
        .map(|m| format!("{m}.rs"))
        .collect();
    expected.sort();

    assert_eq!(
        found, expected,
        "src/protocol/ does not hold exactly the vendored module list"
    );
}

#[test]
fn plugin_rpc_is_not_vendored() {
    // Server-only (the SPEC-016 sidecar transport). If it ever appears here,
    // the SDK has grown a dependency on something no client speaks.
    assert!(
        !sdk_root().join("src/protocol/plugin_rpc.rs").exists(),
        "plugin_rpc is the sidecar transport, not part of the client wire"
    );
}
