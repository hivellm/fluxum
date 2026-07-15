//! MIG-001: more than one `fluxum::schema_version!` declaration in a binary
//! is a startup error. (Own test binary — see schema_version_registry.rs.)
#![allow(clippy::unwrap_used, clippy::expect_used)]

fluxum_core::schema_version!(2);
fluxum_core::schema_version!(3);

#[test]
fn multiple_schema_version_declarations_are_rejected() {
    let err = fluxum_core::migration::declared_schema_version()
        .unwrap_err()
        .to_string();
    assert!(err.contains("multiple fluxum::schema_version!"), "{err}");
    assert!(err.contains("exactly once"), "{err}");
}
