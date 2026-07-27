//! Graceful drain (SPEC-025 §4, OPS-030/031) — split from `lib.rs` to
//! honour the file-size convention.

#[allow(clippy::wildcard_imports)]
use super::*;

// --- Graceful drain (SPEC-025 §4, OPS-030/031) ----------------------------------

/// Tuning for [`drain`].
#[derive(Debug, Clone, Copy)]
pub struct DrainOptions {
    /// The whole drain must finish inside this budget (OPS-030 "exit
    /// cleanly within a bounded deadline"). A straggler past it is
    /// force-closed and logged rather than hanging the deploy.
    pub deadline: std::time::Duration,
}

impl Default for DrainOptions {
    fn default() -> Self {
        Self {
            deadline: std::time::Duration::from_secs(30),
        }
    }
}

/// What a drain did — the deploy's evidence that nothing was dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainReport {
    /// Whether in-flight transactions quiesced inside the deadline. `false`
    /// means the barrier timed out and stragglers were force-closed.
    pub quiesced: bool,
    /// The last **durable** tx id at the end of drain — what is actually on
    /// disk, which is what a restart would replay from. Includes the drain's
    /// own quiesce barrier, which commits like any other transaction.
    pub last_tx_id: u64,
    /// The tx id the final checkpoint captured, if one was taken. Restart
    /// replays only what committed after it — with a checkpoint at
    /// `last_tx_id`, that is nothing.
    pub checkpoint_tx_id: Option<u64>,
}

/// Drain `ctx` for a rolling restart (OPS-030): refuse new work, let
/// in-flight transactions commit, checkpoint, and report — all inside
/// `options.deadline`.
///
/// The steps, in order:
/// 1. **Refuse new work.** [`ShardContext::begin_drain`] flips the accept
///    loops and the session router; new connections, subscriptions and
///    reducer calls get a *retryable* signal, so an SDK reconnects to the
///    restarted process and retries rather than surfacing an error
///    (OPS-031). Already-admitted calls are untouched.
/// 2. **Quiesce.** A barrier job on the shard's FIFO single-writer queue
///    completes only after every previously-submitted reducer has committed
///    or rolled back — the queue's own ordering *is* the proof, so no
///    separate in-flight counter can drift from reality.
/// 3. **Checkpoint.** With the writer idle, `checkpoint` captures a
///    snapshot at the final commit, so restart replays little or nothing.
///    `None` skips it (the caller has no snapshot repo).
///
/// Callers own the actual exit: `drain` deliberately does not stop the
/// transports, so a caller can drain, inspect the report, and only then
/// `shutdown()` — the existing force path stays available and unchanged.
///
/// # Errors
/// Returns the checkpoint's error if the final checkpoint fails. Failing to
/// quiesce is not an error — it is reported as `quiesced: false`, since a
/// straggler must not block the deploy.
pub async fn drain(
    ctx: &Arc<ShardContext>,
    checkpoint: Option<&fluxum_core::checkpoint::SnapshotWorker>,
    options: DrainOptions,
) -> Result<DrainReport, fluxum_core::FluxumError> {
    let started = std::time::Instant::now();
    // 1. Stop admitting new work.
    ctx.begin_drain();

    // 2. Wait for in-flight transactions, bounded by the deadline.
    //
    // The barrier is submitted to the same FIFO single-writer queue, so it
    // runs only after every call already admitted — the queue's ordering
    // *is* the proof, which no separate in-flight counter could match. It is
    // also the shard's final commit, so its receipt names the tx id the
    // whole drain must make durable.
    let barrier = ctx.engine.pipeline().call(Box::new(|_tx| Ok(())));
    let mut barrier_tx: Option<u64> = None;
    let quiesced = match tokio::time::timeout(options.deadline, barrier).await {
        Ok(Ok(receipt)) => {
            barrier_tx = Some(receipt.tx_id);
            true
        }
        // The barrier itself failing (a rolled-back no-op) still means the
        // writer reached it, so everything before it is done.
        Ok(Err(_)) => true,
        Err(_) => {
            tracing::warn!(
                target: "fluxum::server",
                shard = ctx.shard_id,
                deadline_ms = u64::try_from(options.deadline.as_millis()).unwrap_or(u64::MAX),
                "drain deadline elapsed with transactions still in flight; \
                 force-closing stragglers"
            );
            false
        }
    };

    // 3. Final checkpoint, so restart replays little or nothing.
    //
    // The stamp comes from the **commit log**, not `health().last_tx_id`:
    // health tracks what the assembly *published* to the fan-out, whereas
    // the log is what is actually on disk. The distinction matters in both
    // directions — a checkpoint stamped past the durable tail would make
    // replay skip real commits, and one that trusted an assembly which
    // forgot to publish would silently under-cover.
    let log = ctx.engine.pipeline().log();
    // The log appends asynchronously, so a commit that has *returned* is not
    // yet on disk. A drain exists to lose nothing, so wait for the tail to
    // fsync before checkpointing and exiting — otherwise the process could
    // exit having acked writes that never landed. Bounded by the same
    // deadline: an fsync that will not complete must not hang the deploy.
    if let Some(tx_id) = barrier_tx
        && tokio::time::timeout(options.deadline, log.wait_durable(tx_id))
            .await
            .is_err()
    {
        tracing::warn!(
            target: "fluxum::server",
            shard = ctx.shard_id,
            tx_id,
            "drain deadline elapsed waiting for the commit log to become durable"
        );
    }
    let durable = log.durable_tx_id()?.unwrap_or(0);
    let checkpoint_tx_id = match checkpoint {
        // The worker only snapshots commits it has been *told* about (its
        // feed is an accelerator, decoupled from the commit path — see
        // `observe_commit`), so name the durable tail explicitly rather than
        // hoping that feed kept up: a drain that checkpointed short would
        // leave exactly the replay it exists to prevent.
        Some(worker) if durable > 0 => {
            worker.observe_commit(durable);
            let stats = worker.checkpoint_now()?;
            Some(stats.last_tx_id)
        }
        // Nothing durable has ever committed: there is no state to snapshot,
        // and asking would error rather than no-op.
        Some(_) | None => None,
    };

    let report = DrainReport {
        quiesced,
        last_tx_id: durable,
        checkpoint_tx_id,
    };
    tracing::info!(
        target: "fluxum::server",
        shard = ctx.shard_id,
        quiesced = report.quiesced,
        last_tx_id = report.last_tx_id,
        checkpoint_tx_id = report.checkpoint_tx_id,
        elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "drain complete"
    );
    Ok(report)
}

/// Resolve when the process is asked to terminate — SIGTERM on Unix (what
/// an orchestrator sends before SIGKILL), Ctrl-C everywhere.
///
/// The trigger half of OPS-030: an assembly awaits this, calls [`drain`],
/// then stops its transports. It is a separate function from `drain` so the
/// signal source is swappable — tests and a `fluxum drain` command drive the
/// same drain path without a signal.
///
/// # Errors
/// Returns an error if the signal handler cannot be registered.
pub async fn terminate_requested() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = sigterm.recv() => {}
            result = tokio::signal::ctrl_c() => result?,
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        // Windows has no SIGTERM; Ctrl-C (and CTRL_CLOSE_EVENT, which tokio
        // maps onto it) is the equivalent stop request.
        tokio::signal::ctrl_c().await
    }
}
