//! CDC stream-sink pumps (SPEC-020 §6, PLG-050): the server-side async loop
//! that drives each configured `stream_sink` binding's
//! [`CdcSink`](fluxum_core::cdc::CdcSink) off the durable-log watch.
//!
//! The delivery state machine — at-least-once, per-sink checkpoint, bounded
//! batch, drop policy — lives in `fluxum_core::cdc` and is synchronous. This
//! module is only the wake loop: it subscribes to the same durable-log watch
//! the replication streamer follows (SPEC-014), and on every durable advance
//! runs the sink's `drain_to` on the blocking pool (a log read plus, for a
//! sidecar sink, a network round-trip — neither belongs on an async worker).
//! Reading the durable log never touches the write path, so a stalled sink
//! can never back-pressure commits (SUB-041).

use std::path::PathBuf;
use std::sync::Arc;

use fluxum_core::cdc::{CdcOptions, CdcSink};
use fluxum_core::plugin::{Capability, PluginInstance, PluginRegistry};
use fluxum_core::store::TableId;
use tokio::sync::Notify;

use crate::ShardContext;

/// A running CDC pump. Dropping the handle does **not** stop the task (it is
/// detached); call [`stop`](CdcPump::stop) for a graceful end — the in-flight
/// batch finishes, then the loop exits at its next await.
pub struct CdcPump {
    name: String,
    stop: Arc<Notify>,
}

impl CdcPump {
    /// The sink's manifest name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Ask the pump to stop after the current batch. Uses `notify_one` so a
    /// stop sent while the pump is mid-drain (no waiter registered) is stored
    /// as a permit and consumed at the next `select!` — never missed.
    pub fn stop(&self) {
        self.stop.notify_one();
    }
}

/// Spawn one pump per `stream_sink` binding in `registry` (PLG-050).
/// `log_dir` is the shard's commit-log directory (the stream source);
/// `checkpoint_dir` is where the per-sink offset files live. A binding whose
/// instance is absent (only `key_provider` shaped) is skipped; a checkpoint
/// that cannot be created logs and skips that sink rather than aborting boot.
pub fn spawn_sinks(
    ctx: &Arc<ShardContext>,
    registry: &Arc<PluginRegistry>,
    log_dir: PathBuf,
    checkpoint_dir: PathBuf,
) -> Vec<CdcPump> {
    let mut pumps = Vec::new();
    for plugin in registry.plugins() {
        if plugin.capability != Capability::StreamSink {
            continue;
        }
        let Some(PluginInstance::StreamSink(sink)) = plugin.instance.clone() else {
            continue;
        };
        let tables: Vec<u32> = plugin
            .tables
            .iter()
            .map(|t| TableId::of(t).as_u32())
            .collect();
        let cdc = match CdcSink::new(
            plugin.name.clone(),
            sink,
            Arc::clone(&plugin.state),
            &checkpoint_dir,
            log_dir.clone(),
            ctx.shard_id,
            tables,
            Arc::clone(ctx.metrics()),
            CdcOptions::default(),
        ) {
            Ok(cdc) => cdc,
            Err(e) => {
                tracing::error!(
                    target: "fluxum::cdc",
                    sink = %plugin.name, error = %e,
                    "could not initialize CDC sink checkpoint; sink not started (PLG-050)"
                );
                continue;
            }
        };
        pumps.push(spawn_one(ctx, cdc));
    }
    pumps
}

/// Spawn the wake loop for one sink.
fn spawn_one(ctx: &Arc<ShardContext>, cdc: CdcSink) -> CdcPump {
    let stop = Arc::new(Notify::new());
    let name = cdc.name().to_owned();
    let ctx = Arc::clone(ctx);
    let stop_task = Arc::clone(&stop);
    let mut cdc = cdc;
    tokio::spawn(async move {
        let log = ctx.engine.pipeline().log();
        let mut watch = log.subscribe_durable();
        // Baseline the seen version so `changed()` waits for the *next*
        // durable advance rather than firing once spuriously.
        let _ = watch.borrow_and_update();
        loop {
            let tip = match log.durable_tx_id() {
                Ok(Some(tip)) => tip,
                Ok(None) => 0,
                // The log failed durably — nothing more will ever commit;
                // end the pump rather than spin on a dead watermark.
                Err(_) => return,
            };
            // `drain_to` blocks (log read + possibly a sidecar round-trip), so
            // run it on the blocking pool and take ownership back afterwards.
            let (returned, outcome) = match tokio::task::spawn_blocking(move || {
                let outcome = cdc.drain_to(tip);
                (cdc, outcome)
            })
            .await
            {
                Ok(pair) => pair,
                // The blocking task panicked despite `PluginState::guard`
                // (or the runtime is shutting down) — do not respawn.
                Err(_) => return,
            };
            cdc = returned;
            if let Err(e) = outcome {
                tracing::warn!(
                    target: "fluxum::cdc",
                    sink = %cdc.name(), error = %e,
                    "CDC drain hit an operational fault; retrying on the next commit"
                );
            }
            tokio::select! {
                () = stop_task.notified() => return,
                changed = watch.changed() => {
                    if changed.is_err() {
                        return; // the log closed (shutdown)
                    }
                }
            }
        }
    });
    CdcPump { name, stop }
}
