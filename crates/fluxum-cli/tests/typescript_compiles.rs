//! SPEC-011 SDK-021 / FR-82 — the generated TypeScript bindings compile
//! under `tsc --strict` with **zero manual stubs**.
//!
//! This shells out to a real `tsc` because that is the only claim worth
//! making: a hand-rolled check of the emitted text would only assert what the
//! generator already believes. The compiler is the independent judge.
//!
//! It is skipped (not failed) when no TypeScript compiler is reachable, so a
//! Rust-only checkout still builds; CI installs one, and the assertion below
//! makes the skip visible rather than silent.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::{Path, PathBuf};
use std::process::Command;

/// The golden `/schema` document — the same one the T6.1 freeze gate pins.
const GOLDEN: &str = include_str!("../../fluxum-server/tests/golden/schema.json");

/// A `tsc` we can run: a local `node_modules/.bin/tsc` under `dir`, if the
/// install below succeeded.
fn local_tsc(dir: &Path) -> Option<PathBuf> {
    for name in ["tsc.cmd", "tsc"] {
        let path = dir.join("node_modules").join(".bin").join(name);
        if path.exists() {
            return Some(path);
        }
    }
    None
}

/// Install TypeScript into `dir`. Returns false when npm is unavailable or
/// offline — the test then skips rather than failing for the wrong reason.
fn install_typescript(dir: &Path) -> bool {
    for npm in ["npm.cmd", "npm"] {
        let ok = Command::new(npm)
            .args(["install", "--no-save", "--silent", "typescript@5"])
            .current_dir(dir)
            .status();
        if matches!(ok, Ok(status) if status.success()) {
            return true;
        }
    }
    false
}

#[test]
fn generated_typescript_compiles_under_strict() {
    let schema: serde_json::Value = serde_json::from_str(GOLDEN).unwrap();
    let files = fluxum_cli::generate::generate(fluxum_cli::generate::Lang::TypeScript, &schema)
        .expect("the golden schema generates");

    let dir = tempfile::tempdir().unwrap();
    fluxum_cli::generate::write_files(dir.path(), &files).unwrap();
    // The strictest reasonable configuration: if the emitted code needs a
    // manual stub or leans on `any`, this is where it shows.
    std::fs::write(
        dir.path().join("tsconfig.json"),
        r#"{
  "compilerOptions": {
    "strict": true,
    "target": "ES2020",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "noEmit": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "exactOptionalPropertyTypes": true,
    "noUncheckedIndexedAccess": true
  },
  "include": ["*.ts"]
}
"#,
    )
    .unwrap();

    if !install_typescript(dir.path()) {
        eprintln!("SKIP: no npm/TypeScript available; the strict-compile claim is unverified here");
        return;
    }
    let tsc = local_tsc(dir.path()).expect("npm install reported success, so tsc must exist");
    let out = Command::new(&tsc)
        .args(["--noEmit", "-p", "tsconfig.json"])
        .current_dir(dir.path())
        .output()
        .expect("tsc runs");

    assert!(
        out.status.success(),
        "generated TypeScript must compile under --strict with no manual stubs:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}
