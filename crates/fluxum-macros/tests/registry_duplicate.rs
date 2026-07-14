//! DM-040 / SPEC-001 acceptance 2: a duplicate table name (here declared in
//! two different modules, as it would be from two different crates) makes
//! schema assembly fail with a descriptive error. Lives in its own test
//! binary so the poisoned registry cannot leak into other tests.
#![allow(dead_code)]

use fluxum_core::error::FluxumError;
use fluxum_core::schema::Schema;

mod first {
    use fluxum_macros as fluxum;

    #[fluxum::table(public)]
    pub struct Duplicate {
        #[primary_key]
        pub id: u64,
    }
}

mod second {
    use fluxum_macros as fluxum;

    #[fluxum::table]
    pub struct Duplicate {
        #[primary_key]
        pub id: u64,
        pub extra: bool,
    }
}

#[test]
fn duplicate_table_name_aborts_assembly_with_descriptive_error() {
    match Schema::assemble() {
        Err(FluxumError::Schema(msg)) => {
            assert!(msg.contains("duplicate table name `Duplicate`"), "{msg}");
            assert!(msg.contains("DM-040"), "{msg}");
        }
        Ok(_) => panic!("assembly must fail on duplicate table names"),
        Err(other) => panic!("expected FluxumError::Schema, got {other}"),
    }
}
