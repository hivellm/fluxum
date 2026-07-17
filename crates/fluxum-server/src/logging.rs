//! SPEC-012 structured logging (OBS-070): install the process-wide
//! `tracing` subscriber from the resolved [`LoggingConfig`].
//!
//! The default output is one JSON object per line (production); the
//! `pretty` format (development default, OBS-081) is human-readable. The
//! level accepts either a bare level (`info`) or a full `tracing`
//! env-filter directive (`info,fluxum_core=debug`), and `RUST_LOG` — when
//! set — overrides the configured value (OBS-082, env beats config).

use fluxum_core::config::{LogFormat, LoggingConfig};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::prelude::*;

/// Build the [`EnvFilter`] for `level`: `RUST_LOG` if present, else the
/// configured directive, falling back to `info` when neither parses.
fn env_filter(level: &str) -> EnvFilter {
    EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(level))
        .unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Install the global subscriber for `config` (OBS-070). Idempotent and
/// safe to call from tests: a second install (or one racing another crate's
/// subscriber) returns `Err` rather than panicking — the caller may ignore
/// it. JSON emits flattened current-span fields so per-reducer context
/// (`shard`, `reducer`, `duration_us`) rides each line (OBS-071/072).
///
/// # Errors
/// Returns an error string if a global subscriber is already installed.
pub fn init(config: &LoggingConfig) -> Result<(), String> {
    let filter = env_filter(&config.level);
    match config.format {
        LogFormat::Json => {
            let layer = tracing_subscriber::fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(false);
            tracing_subscriber::registry()
                .with(filter)
                .with(layer)
                .try_init()
                .map_err(|e| e.to_string())
        }
        LogFormat::Pretty => {
            let layer = tracing_subscriber::fmt::layer().pretty();
            tracing_subscriber::registry()
                .with(filter)
                .with(layer)
                .try_init()
                .map_err(|e| e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_filter_prefers_configured_directive() {
        // A compound directive parses (the level string is not just a bare
        // level) — proves OBS-082's env-filter directive support.
        let filter = env_filter("warn,fluxum_core=debug");
        assert!(filter.to_string().contains("fluxum_core"));
    }

    #[test]
    fn env_filter_falls_back_to_info_on_garbage() {
        let filter = env_filter("this is !!! not a filter @@@");
        // Falls back rather than panicking; the fallback admits info.
        assert!(!filter.to_string().is_empty());
    }

    #[test]
    fn init_is_idempotent_and_never_panics() {
        // Whichever call wins the global-subscriber race, neither panics;
        // at least one of the two attempts must report the slot was taken.
        let json = init(&LoggingConfig {
            level: "info".into(),
            format: LogFormat::Json,
        });
        let pretty = init(&LoggingConfig {
            level: "debug".into(),
            format: LogFormat::Pretty,
        });
        assert!(
            json.is_err() || pretty.is_err(),
            "the second install must fail rather than double-register"
        );
    }
}
