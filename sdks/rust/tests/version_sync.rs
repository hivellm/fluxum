//! The gate for the release train (owner rule, 2026-07-29): the server and
//! every SDK carry ONE version — a Docker Hub tag, a crates.io release, an
//! npm/PyPI/NuGet package and the Go module tag must all read the same
//! number, or "which SDK works with which server" becomes archaeology.
//!
//! This test pins every manifest in the repo to the workspace version:
//!
//! - `Cargo.toml` `[workspace.package].version` (the server / image version);
//! - this crate's own version (`CARGO_PKG_VERSION`);
//! - `sdks/typescript/package.json` + its `package-lock.json`;
//! - `sdks/python/pyproject.toml`;
//! - `sdks/csharp/Fluxum.Sdk/Fluxum.Sdk.csproj`.
//!
//! The Go SDK has no in-repo version field — Go modules version by git tag
//! on `hivellm/fluxum-go` — so its part of the contract is procedural: tag
//! `v<workspace version>` when releasing. The Dockerfile's
//! `org.opencontainers.image.version` label is asserted here too, since the
//! pushed tag must match it.
//!
//! Like `protocol_sync`, every check skips outside the workspace (the
//! published package has no siblings to compare against), and
//! `source_is_present_in_the_workspace` keeps that skip honest.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;

fn repo_root() -> Option<PathBuf> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    root.join("Cargo.toml").is_file().then_some(root)
}

/// Extract `version = "x"` / `"version": "x"` style values with a plain
/// scan — no TOML/JSON/XML parser needed for a literal the test controls.
fn capture(text: &str, prefix: &str, suffix: &str) -> Option<String> {
    let start = text.find(prefix)? + prefix.len();
    let rest = &text[start..];
    let end = rest.find(suffix)?;
    Some(rest[..end].to_string())
}

#[test]
fn source_is_present_in_the_workspace() {
    // Guards the skips below, exactly like protocol_sync: if this crate
    // sits in the workspace, the root manifest must be findable — a wrong
    // relative path can never quietly disarm the gate.
    let in_workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates")
        .is_dir();
    if !in_workspace {
        return;
    }
    assert!(
        repo_root().is_some(),
        "in the workspace but the root Cargo.toml is missing — the version \
         gate would silently pass against nothing"
    );
}

#[test]
fn every_manifest_carries_the_workspace_version() {
    let Some(root) = repo_root() else {
        return;
    };
    let read = |rel: &str| std::fs::read_to_string(root.join(rel)).expect(rel);

    let workspace = capture(
        &read("Cargo.toml"),
        "[workspace.package]\nversion = \"",
        "\"",
    )
    .expect("workspace version");

    let mut versions: Vec<(&str, String)> = vec![
        ("workspace Cargo.toml", workspace.clone()),
        (
            "fluxum-sdk (this crate)",
            env!("CARGO_PKG_VERSION").to_string(),
        ),
        (
            "sdks/typescript/package.json",
            capture(
                &read("sdks/typescript/package.json"),
                "\"version\": \"",
                "\"",
            )
            .unwrap(),
        ),
        (
            "sdks/typescript/package-lock.json",
            capture(
                &read("sdks/typescript/package-lock.json"),
                "\"version\": \"",
                "\"",
            )
            .unwrap(),
        ),
        (
            "sdks/python/pyproject.toml",
            capture(&read("sdks/python/pyproject.toml"), "version = \"", "\"").unwrap(),
        ),
        (
            "sdks/python/fluxum/__init__.py (__version__)",
            capture(
                &read("sdks/python/fluxum/__init__.py"),
                "__version__ = \"",
                "\"",
            )
            .unwrap(),
        ),
        (
            "sdks/csharp/Fluxum.Sdk/Fluxum.Sdk.csproj",
            capture(
                &read("sdks/csharp/Fluxum.Sdk/Fluxum.Sdk.csproj"),
                "<Version>",
                "</Version>",
            )
            .unwrap(),
        ),
        (
            "Dockerfile (OCI version label)",
            capture(
                &read("Dockerfile"),
                "org.opencontainers.image.version=\"",
                "\"",
            )
            .unwrap(),
        ),
    ];

    versions.retain(|(_, v)| *v != workspace);
    assert!(
        versions.is_empty(),
        "the release train moves together: workspace is {workspace}, but these \
         disagree (fix them, and remember the Go tag v{workspace}): {versions:?}"
    );
}
