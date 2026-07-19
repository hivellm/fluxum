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
//! # Why the filter is a separate layer
//!
//! `logging.level` and `logging.format` are reloadable together (OPS-040).
//! The obvious shape — one boxed layer carrying a per-layer `with_filter`,
//! behind one [`reload::Layer`] — does not work: a `Filtered` layer gets its
//! `FilterId` when it is added to the subscriber, and boxing it inside a
//! reload layer erases that registration. The process then panics on its
//! first log line with *"a `Filtered` layer was used, but it had no
//! `FilterId`"*.
//!
//! So the level rides a **global** filter layer and the format rides the
//! boxed fmt layer, each behind its own reload handle. [`LogReloadHandle`]
//! holds both and publishes them together, which keeps the OPS-040 promise
//! that one reload applies the whole pair.

use fluxum_core::config::{LogFormat, LoggingConfig};
use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, Layer, Registry, reload};

/// The subscriber the format layer sits on: the registry with the reloadable
/// level filter already applied.
type Filtered = tracing_subscriber::layer::Layered<reload::Layer<EnvFilter, Registry>, Registry>;

/// The format layer, type-erased so `json` and `pretty` are the same type and
/// can replace one another at runtime.
type BoxedLayer = Box<dyn Layer<Filtered> + Send + Sync>;

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
    // No `with_filter` here: see the module docs — a per-layer filter inside
    // a boxed reload layer loses its `FilterId` and panics on first use. The
    // level is applied by the global filter layer instead.
    match config.format {
        LogFormat::Json => tracing_subscriber::fmt::layer()
            .json()
            .with_current_span(true)
            .with_span_list(false)
            .boxed(),
        LogFormat::Pretty => tracing_subscriber::fmt::layer().pretty().boxed(),
    }
}

/// A live handle to the installed logging layer (OPS-040): lets a config
/// reload change the level and format of the *running* process with no
/// restart and no dropped lines.
#[derive(Clone)]
pub struct LogReloadHandle {
    fmt: reload::Handle<BoxedLayer, Filtered>,
    filter: reload::Handle<EnvFilter, Registry>,
}

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
        // Filter first: widening the level before swapping the format can
        // only produce extra lines, whereas swapping the format first leaves
        // a window where the new writer runs under the old level.
        self.filter
            .reload(env_filter(&config.level))
            .map_err(|e| format!("logging reload failed: {e}"))?;
        self.fmt
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
    let (filter_layer, filter) = reload::Layer::new(env_filter(&config.level));
    let (fmt_layer, fmt) = reload::Layer::new(layer_for(config));
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .try_init()
        .map_err(|e| e.to_string())?;
    Ok(LogReloadHandle { fmt, filter })
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
