//! T4.1 injection corpus (SPEC-005 SUB-012; DAG T4.1 exit test; feeds the
//! T6.6 security audit): a broad corpus of malformed and hostile query
//! strings — classic SQLi payloads, encoding tricks, resource-exhaustion
//! shapes, and every SUB-012 construct — never crashes the compiler and
//! never yields a `CompiledPlan`. Only the closed SUB-010/SUB-011 grammar
//! compiles; everything else is a wire-ready 400.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::schema::{
    ColumnSchema, FluxType, Schema, TableAccess, TableSchema, VisibilityRule,
};
use fluxum_core::sql::compile;

static ACCOUNT_COLS: &[ColumnSchema] = &[
    ColumnSchema {
        name: "id",
        ty: FluxType::U64,
    },
    ColumnSchema {
        name: "owner",
        ty: FluxType::Str,
    },
    ColumnSchema {
        name: "balance",
        ty: FluxType::I64,
    },
];

static ACCOUNT: TableSchema = TableSchema {
    name: "Account",
    columns: ACCOUNT_COLS,
    primary_key: &[0],
    auto_inc: None,
    access: TableAccess::Public,
    partition_by: None,
    unique: &[],
    indexes: &[],
    visibility: VisibilityRule::PublicAll,
};

fn schema() -> Schema {
    Schema::from_tables([&ACCOUNT]).unwrap()
}

/// Every hostile / malformed input MUST be rejected with a 400 (never a
/// panic, never a compiled plan).
const HOSTILE: &[&str] = &[
    // --- classic SQL injection payloads ---
    "SELECT * FROM Account WHERE owner = 'x' OR '1'='1'",
    "SELECT * FROM Account WHERE owner = 'x'; DROP TABLE Account",
    "SELECT * FROM Account WHERE owner = 'x'--",
    "SELECT * FROM Account WHERE owner = 'x' -- comment",
    "SELECT * FROM Account WHERE owner = 'x' /* block */",
    "SELECT * FROM Account WHERE owner = 'x' # hash",
    "SELECT * FROM Account WHERE id = 1; DELETE FROM Account WHERE 1=1",
    "SELECT * FROM Account WHERE owner = '' OR 1=1 --",
    "SELECT * FROM Account UNION SELECT * FROM Account",
    "SELECT * FROM Account WHERE id = (SELECT id FROM Account)",
    "SELECT * FROM Account WHERE owner LIKE '%admin%'",
    "SELECT * FROM Account WHERE owner = 0x41414141",
    "SELECT * FROM Account WHERE id = 1 OR SLEEP(10)",
    "SELECT * FROM Account WHERE id = 1; EXEC xp_cmdshell('dir')",
    "'; DROP TABLE Account; --",
    "SELECT * FROM Account WHERE owner = \"admin\"",
    "SELECT * FROM Account WHERE owner = `admin`",
    // --- unsupported constructs (SUB-012) ---
    "SELECT * FROM Account JOIN Account",
    "SELECT * FROM Account GROUP BY owner",
    "SELECT * FROM Account HAVING balance > 0",
    "SELECT COUNT(*) FROM Account",
    "SELECT MAX(balance) FROM Account",
    "INSERT INTO Account VALUES (1)",
    "UPDATE Account SET balance = 0",
    "DELETE FROM Account",
    "DROP TABLE Account",
    "ALTER TABLE Account",
    "CREATE TABLE X",
    "WITH cte AS (SELECT 1) SELECT * FROM cte",
    "SELECT * FROM Account WHERE NOT id = 1",
    "SELECT * FROM Account WHERE balance IS NULL",
    "SELECT * FROM Account WHERE balance = NULL",
    "SELECT * FROM Account WHERE id > 5",
    "SELECT * FROM Account WHERE id < 5",
    "SELECT * FROM Account WHERE id != 5",
    "SELECT * FROM Account WHERE id <> 5",
    "SELECT * FROM Account WHERE id >= 5",
    // --- malformed structure ---
    "",
    "   ",
    "\n\t\r",
    "SELECT",
    "SELECT *",
    "SELECT * FROM",
    "FROM Account",
    "SELECT id FROM Account",
    "SELECT * FROM Account WHERE",
    "SELECT * FROM Account WHERE owner",
    "SELECT * FROM Account WHERE owner =",
    "SELECT * FROM Account WHERE = 5",
    "SELECT * FROM Account WHERE owner = = 5",
    "SELECT * FROM Account WHERE owner IN ()",
    "SELECT * FROM Account WHERE owner IN (1,)",
    "SELECT * FROM Account WHERE owner IN 1, 2",
    "SELECT * FROM Account WHERE id BETWEEN 1",
    "SELECT * FROM Account WHERE id BETWEEN 1 AND",
    "SELECT * FROM Account ORDER BY",
    "SELECT * FROM Account LIMIT",
    "SELECT * FROM Account LIMIT -1",
    "SELECT * FROM Account LIMIT abc",
    "SELECT * FROM Account (",
    "SELECT * FROM Account ))",
    "SELECT ** FROM Account",
    "SELECT * * FROM Account",
    "SELECT * FROM Account Account",
    "SELECT * FROM Account WHERE id = 1 2 3",
    // --- encoding / unicode tricks ---
    "SELECT\0* FROM Account",
    "SELECT * FROM Account\0",
    "SΕLECT * FROM Account",       // Greek capital epsilon
    "ＳＥＬＥＣＴ * FROM Account", // fullwidth
    // (a control char INSIDE a string literal is legitimate data, not an
    // injection — it only ever compares against stored bytes; covered by
    // `control_chars_in_string_literals_are_data` below.)
    // --- resource / range ---
    "SELECT * FROM Account WHERE id = 99999999999999999999999999",
    "SELECT * FROM Account WHERE balance = 1e400",
    "SELECT * FROM Account LIMIT 99999999999999999999",
    // --- unknown schema references ---
    "SELECT * FROM NoSuchTable",
    "SELECT * FROM Account WHERE nope = 1",
    "SELECT * FROM Account ORDER BY nope",
];

#[test]
fn hostile_inputs_are_always_rejected_never_panic() {
    let schema = schema();
    for sql in HOSTILE {
        match compile(&schema, sql) {
            Ok(plan) => panic!("hostile input compiled to a plan: {sql:?} -> {plan:?}"),
            Err(e) => assert_eq!(
                e.query_code(),
                Some(400),
                "hostile input must be a wire-ready 400: {sql:?} -> {e}"
            ),
        }
    }
}

#[test]
fn deeply_nested_and_long_inputs_are_bounded_not_crashing() {
    let schema = schema();
    // A very long IN list and a very long string do not crash — they are
    // either compiled (well-formed) or rejected, but always terminate.
    let long_in = format!(
        "SELECT * FROM Account WHERE id IN ({})",
        (0..5_000)
            .map(|n| n.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    let _ = compile(&schema, &long_in); // must return, either arm is fine

    let long_string = format!(
        "SELECT * FROM Account WHERE owner = '{}'",
        "a".repeat(4_000)
    );
    assert!(compile(&schema, &long_string).is_ok());

    // Past the byte cap: rejected, not OOM.
    let too_long = format!(
        "SELECT * FROM Account WHERE owner = '{}'",
        "a".repeat(16_000)
    );
    let err = compile(&schema, &too_long).unwrap_err();
    assert_eq!(err.query_code(), Some(400), "{err}");

    // Many parentheses never recurse unboundedly (the grammar is flat).
    let parens = format!("SELECT * FROM Account WHERE id IN {}", "(".repeat(1_000));
    let err = compile(&schema, &parens).unwrap_err();
    assert_eq!(err.query_code(), Some(400), "{err}");
}

#[test]
fn control_chars_in_string_literals_are_data_not_injection() {
    let schema = schema();
    // A string literal is a value compared against stored bytes — any
    // Unicode content, including control characters, is legitimate data
    // (there is no downstream re-interpretation, so no injection surface).
    for payload in ["\u{202e}", "\t\n", "'", "-- not a comment here", ";"] {
        let escaped = payload.replace('\'', "''");
        let sql = format!("SELECT * FROM Account WHERE owner = '{escaped}'");
        compile(&schema, &sql).unwrap_or_else(|e| panic!("{sql:?} is valid data: {e}"));
    }
}

#[test]
fn only_the_supported_forms_compile() {
    let schema = schema();
    let supported = [
        "SELECT * FROM Account",
        "SELECT * FROM Account WHERE id = 1",
        "SELECT * FROM Account WHERE owner = 'ana'",
        "SELECT * FROM Account WHERE id IN (1, 2, 3)",
        "SELECT * FROM Account WHERE balance BETWEEN 0 AND 100",
        "SELECT * FROM Account WHERE id = 1 AND owner = 'ana'",
        "SELECT * FROM Account ORDER BY balance DESC LIMIT 10",
    ];
    for sql in supported {
        compile(&schema, sql).unwrap_or_else(|e| panic!("{sql} must compile: {e}"));
    }
}
