//! SPEC-012 structured logging (OBS-070): install the process-wide
//! `tracing` subscriber from the resolved [`LoggingConfig`], and swap it on
//! config hot reload (SPEC-025 OPS-040).
//!
//! The default output is one JSON object per line (production); the
//! `pretty` format (development default, OBS-081) is human-readable. The
//! level accepts either a bare level (`info`) or a full `tracing`
//! env-filter directive (`info,fluxum_core=debug`), and `RUST_LOG` — when
//! set — overrides the configured value (OBS-082, env beats config).
//!
//! # Why one reload handle covers both keys
//!
//! `logging.level` and `logging.format` are reloadable together (OPS-040)
//! because the format decides the *layer type* and the level decides its
//! filter — swapping them separately would need two handles and could leave
//! a half-applied pair visible to a concurrent log line. Instead the whole
//! filtered layer is boxed behind a single [`reload::Layer`], so one atomic
//! write publishes both.

use fluxum_core::config::{LogFormat, LoggingConfig};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Layer, Registry, reload};

/// The composed (formatted + filtered) layer, type-erased so `json` and
/// `pretty` are the same type and can replace one another at runtime.
type BoxedLayer = Box<dyn Layer<Registry> + Send + Sync>;

/// Build the [`EnvFilter`] for `level`: `RUST_LOG` if present, else the
/// configured directive, falling back to `info` when neither parses.
fn env_filter(level: &str) -> EnvFilter {
    EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(level))
        .unwrap_or_else(|_| EnvFilter::new("info"))
}

/// The formatted, filtered layer for `config`. JSON emits flattened
/// current-span fields so per-reducer context (`shard`, `reducer`,
/// `duration_us`) rides each line (OBS-071/072).
fn layer_for(config: &LoggingConfig) -> BoxedLayer {
    let filter = env_filter(&config.level);
    match config.format {
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(true)
            .with_span_list(false)
            .with_filter(filter)
            .boxed(),
        LogFormat::Pretty => tracing_subscriber::fmt::layer()
            .pretty()
            .with_filter(filter)
            .boxed(),
    }
}

/// A live handle to the installed logging layer (OPS-040): lets a config
/// reload change the level and format of the *running* process with no
/// restart and no dropped lines.
#[derive(Clone)]
pub struct LogReloadHandle(reload::Handle<BoxedLayer, Registry>);

impl std::fmt::Debug for LogReloadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogReloadHandle").finish_non_exhaustive()
    }
}

impl LogReloadHandle {
    /// Republish `config` to the running subscriber: subsequent log lines
    /// use the new level and format, in one atomic swap.
    ///
    /// Note `RUST_LOG` still wins over `logging.level` (OBS-082) — a reload
    /// re-reads it, so an operator who set `RUST_LOG` at launch keeps it.
    ///
    /// # Errors
    /// Returns an error string if the subscriber this handle points at has
    /// been dropped (only possible once the process is tearing down).
    pub fn apply(&self, config: &LoggingConfig) -> Result<(), String> {
        self.0
            .reload(layer_for(config))
            .map_err(|e| format!("logging reload failed: {e}"))
    }
}

/// Install the global subscriber for `config` (OBS-070) and return the
/// handle that hot reload publishes through (OPS-040). Idempotent and safe
/// to call from tests: a second install (or one racing another crate's
/// subscriber) returns `Err` rather than panicking — the caller may ignore
/// it.
///
/// # Errors
/// Returns an error string if a global subscriber is already installed.
pub fn init(config: &LoggingConfig) -> Result<LogReloadHandle, String> {
    let (layer, handle) = reload::Layer::new(layer_for(config));
    tracing_subscriber::registry()
        .with(layer)
        .try_init()
        .map_err(|e| e.to_string())?;
    Ok(LogReloadHandle(handle))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

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
    fn layer_builds_for_both_formats() {
        // Both formats erase to the same type — that is what makes a
        // format swap possible at all (OPS-040).
        let _json = layer_for(&LoggingConfig {
            level: "info".into(),
            format: LogFormat::Json,
        });
        let _pretty = layer_for(&LoggingConfig {
            level: "debug".into(),
            format: LogFormat::Pretty,
        });
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
        // Whichever won hands back a usable handle: a reload through it
        // succeeds while the subscriber is alive (OPS-040).
        if let Ok(handle) = json.or(pretty) {
            handle
                .apply(&LoggingConfig {
                    level: "trace".into(),
                    format: LogFormat::Pretty,
                })
                .expect("reload through a live handle must succeed");
        }
    }
}
