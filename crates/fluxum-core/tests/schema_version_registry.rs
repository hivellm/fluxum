//! MIG-001: a `fluxum::schema_version!(0)` declaration is a startup error.
//! (Own test binary: the link-time registry is per-binary, so this must not
//! share a binary with tests that rely on the default version.)
#![allow(clippy::unwrap_used, clippy::expect_used)]

fluxum_core::schema_version!(0);

#[test]
fn declared_schema_version_zero_is_rejected() {
    let err = fluxum_core::migration::declared_schema_version()
        .unwrap_err()
        .to_string();
    assert!(err.contains("schema_version!(0) is invalid"), "{err}");
}
