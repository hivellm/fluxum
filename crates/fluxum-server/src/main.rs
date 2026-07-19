//! The Fluxum reference server binary.
//!
//! Usage:
//!
//! ```text
//! fluxum-server [--config path/to/config.yml]
//! ```
//!
//! With no `--config`, the built-in defaults apply and `FLUXUM_`-prefixed
//! environment variables still override them (SPEC-006 RPC-003).
//!
//! The application itself is not configured here — it is *linked* here. Any
//! crate in this binary's dependency graph that declares `#[fluxum::table]` and
//! `#[fluxum::reducer]` registers through `inventory`, and startup collects it.

use std::process::ExitCode;
use std::sync::Arc;

use fluxum_core::config::Config;
use fluxum_server::{ShardContext, boot, logging};

fn main() -> ExitCode {
    // Touch the module crate so the linker keeps it — and with it the
    // `inventory` registrations that ARE the schema (OQ-1). An unreferenced
    // dependency is dropped wholesale, and the server would boot with no
    // tables.
    fluxum_demo::link();

    let args: Vec<String> = std::env::args().skip(1).collect();
    let config_path = match parse_args(&args) {
        Ok(Some(Args::Help)) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Ok(Some(Args::Version)) => {
            println!("fluxum-server {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        Ok(Some(Args::Config(path))) => Some(path),
        Ok(None) => None,
        Err(message) => {
            eprintln!("{message}\n\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };

    let config = match Config::load(config_path.as_deref()) {
        Ok(config) => config,
        Err(err) => {
            // Before logging is initialised, so stderr is the only channel.
            eprintln!("configuration error: {err}");
            return ExitCode::FAILURE;
        }
    };

    let log_handle = match logging::init(&config.logging) {
        Ok(handle) => Some(handle),
        Err(err) => {
            eprintln!("cannot initialise logging: {err}");
            return ExitCode::FAILURE;
        }
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("cannot start the async runtime: {err}");
            return ExitCode::FAILURE;
        }
    };

    runtime.block_on(run(config, config_path, log_handle))
}

const USAGE: &str = "\
fluxum-server — the Fluxum reference server

USAGE:
    fluxum-server [--config <path>]

OPTIONS:
    -c, --config <path>    Configuration file (YAML). Defaults apply if omitted.
    -h, --help             Print this message.
    -V, --version          Print the version.

Configuration keys can also be set with FLUXUM_-prefixed environment
variables, which override the file.";

enum Args {
    Help,
    Version,
    Config(std::path::PathBuf),
}

/// Parse the command line, which is deliberately one option wide.
///
/// Everything else is a config key, and config keys belong in the file or in
/// `FLUXUM_*` — duplicating them as flags would create a third precedence
/// level to reason about.
fn parse_args(args: &[String]) -> Result<Option<Args>, String> {
    let Some(first) = args.first() else {
        return Ok(None);
    };
    match first.as_str() {
        "-h" | "--help" => Ok(Some(Args::Help)),
        "-V" | "--version" => Ok(Some(Args::Version)),
        "-c" | "--config" => match args.get(1) {
            Some(path) => Ok(Some(Args::Config(path.into()))),
            None => Err(format!("{first} needs a path")),
        },
        other => Err(format!("unrecognised argument: {other}")),
    }
}

async fn run(
    config: Config,
    config_path: Option<std::path::PathBuf>,
    log_handle: Option<logging::LogReloadHandle>,
) -> ExitCode {
    let server = match boot::serve(config.clone()).await {
        Ok(server) => server,
        Err(err) => {
            tracing::error!(error = %err, "startup failed");
            eprintln!("startup failed: {err}");
            return ExitCode::FAILURE;
        }
    };

    // Registers the source so `POST /config/reload` and SIGHUP can re-read it
    // (SPEC-025 OPS-040). Done after binding so a config that cannot serve
    // never becomes the reloadable baseline.
    server
        .ctx
        .install_config(config_path, config, log_handle);

    tracing::info!(
        http = %server.http.local_addr,
        tcp = %server.tcp.local_addr,
        "fluxum-server is up"
    );
    println!(
        "fluxum-server {} listening — HTTP {} (admin + /rpc), TCP {}",
        env!("CARGO_PKG_VERSION"),
        server.http.local_addr,
        server.tcp.local_addr,
    );

    spawn_reload_watcher(Arc::clone(&server.ctx));
    wait_for_shutdown().await;

    // Drain before closing listeners: in-flight transactions get to finish,
    // which is the difference between a rolling restart and dropped work
    // (SPEC-025 OPS-030).
    tracing::info!("shutting down; draining");
    server.ctx.begin_drain();
    server.shutdown();
    ExitCode::SUCCESS
}

/// Re-read the config file on `SIGHUP` (SPEC-025 OPS-040).
///
/// Unix only: there is no SIGHUP on Windows, where `POST /config/reload`
/// remains the way in.
#[cfg(unix)]
fn spawn_reload_watcher(ctx: Arc<ShardContext>) {
    use tokio::signal::unix::{SignalKind, signal};

    tokio::spawn(async move {
        let mut hangup = match signal(SignalKind::hangup()) {
            Ok(stream) => stream,
            Err(err) => {
                tracing::warn!(error = %err, "cannot watch SIGHUP; config reload is HTTP-only");
                return;
            }
        };
        while hangup.recv().await.is_some() {
            match ctx.reload_config() {
                Ok(changed) if changed.is_empty() => {
                    tracing::info!("SIGHUP: config re-read, nothing changed");
                }
                Ok(changed) => tracing::info!(keys = ?changed, "SIGHUP: config reloaded"),
                // A bad config on reload leaves the running one in place —
                // the server keeps serving rather than dying on a typo.
                Err(err) => tracing::error!(error = %err, "SIGHUP: reload rejected, keeping the running config"),
            }
        }
    });
}

#[cfg(not(unix))]
fn spawn_reload_watcher(_ctx: Arc<ShardContext>) {}

/// Resolve on Ctrl-C, or on `SIGTERM` where it exists — the signal a
/// container runtime sends first, and the one an orchestrator waits on before
/// escalating to SIGKILL.
async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
