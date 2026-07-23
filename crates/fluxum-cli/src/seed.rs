//! `fluxum seed <file>` — load a fixture into a running instance
//! (SPEC-024 DEV-040, FR-138).
//!
//! # Fixture format
//!
//! A JSON document listing reducer calls, applied **in order**:
//!
//! ```json
//! {
//!   "calls": [
//!     { "reducer": "add_task", "args": ["write tests"] },
//!     { "reducer": "send_chat", "args": [1, "hello"], "repeat": 3 }
//!   ]
//! }
//! ```
//!
//! Every mutation goes through a reducer — the SPEC-024 §6 rule (no direct
//! row edits outside reducers) applies to seeding exactly as it does to the
//! console, so "fixture rows" are expressed as calls to the module's own
//! reducers, whose args carry the row data. `repeat` expands one entry into
//! N identical calls (bulk data), defaulting to 1.
//!
//! # Application
//!
//! Each call is one `POST /reducer/<name>` against the admin surface
//! (RPC-051), which runs the full production admission path — argument
//! validation, rate limits, the reducer body, the commit — so a fixture
//! that seeds is a fixture the application could have produced. Application
//! stops at the first failure: fixtures are ordered (later calls may depend
//! on earlier rows), so skip-and-continue would seed a half-state silently.

use std::path::Path;

use crate::{CliError, post_path};

/// One reducer call in a fixture file.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
pub struct FixtureCall {
    /// The reducer to invoke.
    pub reducer: String,
    /// Positional JSON arguments (default: none).
    #[serde(default)]
    pub args: Vec<serde_json::Value>,
    /// How many times to apply this call (default 1).
    #[serde(default = "one")]
    pub repeat: u32,
}

fn one() -> u32 {
    1
}

/// A parsed fixture document.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
pub struct Fixture {
    /// The calls, applied in order.
    pub calls: Vec<FixtureCall>,
}

/// Parse a fixture document, rejecting the shapes that would otherwise fail
/// call-by-call at seed time with worse messages.
pub fn parse_fixture(text: &str) -> Result<Fixture, CliError> {
    let fixture: Fixture = serde_json::from_str(text)
        .map_err(|e| CliError::Response(format!("invalid fixture: {e}")))?;
    if fixture.calls.is_empty() {
        return Err(CliError::Response(
            "fixture has no calls — nothing to seed".into(),
        ));
    }
    for (i, call) in fixture.calls.iter().enumerate() {
        if call.reducer.trim().is_empty() {
            return Err(CliError::Response(format!(
                "fixture call #{}: `reducer` is empty",
                i + 1
            )));
        }
        if call.repeat == 0 {
            return Err(CliError::Response(format!(
                "fixture call #{} (`{}`): repeat 0 means \"never\" — remove the entry instead",
                i + 1,
                call.reducer
            )));
        }
    }
    Ok(fixture)
}

/// What one seeding run applied.
#[derive(Debug, PartialEq, Eq)]
pub struct SeedReport {
    /// Reducer calls applied, in order (repeat entries expanded).
    pub applied: usize,
}

/// Load `file` and apply it to the server at `server` (DEV-040). Prints one
/// line per fixture entry; stops at the first failing call with the
/// server's own error.
pub fn run_seed(server: &str, file: &Path) -> Result<SeedReport, CliError> {
    let text = std::fs::read_to_string(file)?;
    let fixture = parse_fixture(&text)?;

    let mut applied = 0usize;
    for call in &fixture.calls {
        let body = serde_json::to_string(&call.args)
            .map_err(|e| CliError::Response(format!("cannot encode args: {e}")))?;
        for _ in 0..call.repeat {
            let response = post_path(server, &format!("/reducer/{}", call.reducer), &body)
                .map_err(|e| match e {
                    CliError::Response(m) => CliError::Response(format!(
                        "`{}` failed after {applied} applied call(s): {m}",
                        call.reducer
                    )),
                    other => other,
                })?;
            // The admin envelope's failure shape still arrives as HTTP 4xx/5xx
            // (handled above); a 200 body is checked for `success` anyway so
            // a policy change server-side cannot make failures look green.
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&response)
                && value.get("success") == Some(&serde_json::Value::Bool(false))
            {
                let message = value
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown error");
                return Err(CliError::Response(format!(
                    "`{}` failed after {applied} applied call(s): {message}",
                    call.reducer
                )));
            }
            applied += 1;
        }
        if call.repeat == 1 {
            println!(
                "seeded {} {}",
                call.reducer,
                serde_json::Value::from(call.args.clone())
            );
        } else {
            println!(
                "seeded {} ×{} {}",
                call.reducer,
                call.repeat,
                serde_json::Value::from(call.args.clone())
            );
        }
    }
    Ok(SeedReport { applied })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn a_fixture_parses_with_defaults_applied() {
        let fixture = parse_fixture(
            r#"{ "calls": [
                { "reducer": "add_task", "args": ["write tests"] },
                { "reducer": "tick" },
                { "reducer": "send_chat", "args": [1, "hi"], "repeat": 3 }
            ] }"#,
        )
        .unwrap();
        assert_eq!(fixture.calls.len(), 3);
        assert_eq!(fixture.calls[0].repeat, 1, "repeat defaults to 1");
        assert!(fixture.calls[1].args.is_empty(), "args default to none");
        assert_eq!(fixture.calls[2].repeat, 3);
    }

    #[test]
    fn bad_fixtures_are_rejected_with_positions() {
        assert!(matches!(
            parse_fixture("not json"),
            Err(CliError::Response(m)) if m.contains("invalid fixture")
        ));
        assert!(matches!(
            parse_fixture(r#"{ "calls": [] }"#),
            Err(CliError::Response(m)) if m.contains("no calls")
        ));
        assert!(matches!(
            parse_fixture(r#"{ "calls": [ { "reducer": "  " } ] }"#),
            Err(CliError::Response(m)) if m.contains("#1")
        ));
        assert!(matches!(
            parse_fixture(r#"{ "calls": [ { "reducer": "x", "repeat": 0 } ] }"#),
            Err(CliError::Response(m)) if m.contains("repeat 0")
        ));
    }
}
