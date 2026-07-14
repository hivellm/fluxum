//! Golden-file macro tests (SPEC-001 acceptance 1, task T1.1 item 1.6):
//! every DM-020 table attribute compiles; every invalid combination fails
//! with the specified diagnostic.

#[test]
fn ui() {
    let t = trybuild::TestCases::new();
    t.pass("tests/ui/pass/*.rs");
    t.compile_fail("tests/ui/fail/*.rs");
}
