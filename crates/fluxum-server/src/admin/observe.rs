//! `GET /health` (RPC-053) and `GET /metrics` (SPEC-012 + TIER-080) —
//! split from the parent module to honour the file-size convention.

#[allow(clippy::wildcard_imports)]
use super::*;

// --- GET /health (RPC-053: lock-free, < 50 ms) ---------------------------------

pub(super) fn health(ctx: &Arc<ShardContext>) -> AdminResponse {
    use fluxum_core::metrics::ShardState;
    let health = ctx.health(); // atomics + channel gauge — no storage lock
    // OBS-060: status + HTTP code derive from the shard's lifecycle state.
    let (status, code) = match health.state {
        ShardState::Ready => ("ok", 200),
        ShardState::Recovering => ("degraded", 503),
        ShardState::Starting | ShardState::ShuttingDown => ("error", 503),
    };
    let mut shard = json!({
        "id": health.shard_id.to_string(),
        "state": health.state.as_str(),
        "tx_id": health.last_tx_id,
        "queue_depth": health.queue_depth,
    });
    // SPEC-014 REP-080: the per-shard replication object, from pre-computed
    // published state (no storage lock — OBS-061). A shard in `semi_sync`
    // that has lost quorum is `degraded` overall.
    let mut status = status;
    if let Some(election) = ctx.election() {
        let role = election.role();
        let metrics = ctx.metrics();
        let mut repl = json!({
            "role": if role.is_primary() { "primary" } else { "replica" },
            "epoch": role.epoch(),
        });
        if role.is_primary() {
            repl["connected_replicas"] =
                json!(ctx.replication_primary().map_or(0, |p| p.connected()));
            if metrics.replication_degraded() {
                // Still serving (reads/writes proceed), but the zero-loss
                // guarantee is suspended — flag it without failing /health.
                status = "degraded";
            }
            repl["degraded"] = json!(metrics.replication_degraded());
        } else {
            if let Some(hint) = role.primary_hint() {
                repl["primary"] = json!(hint);
            }
            if let Some((offset, lag_tx)) = metrics.replication_peer("primary") {
                repl["acked_tx_id"] = json!(offset);
                repl["lag_tx"] = json!(lag_tx);
            }
            repl["stale"] = json!(election.read_is_stale());
        }
        shard["replication"] = repl;
    }
    let mut body = json!({
        "status": status,
        "shards": [shard],
        "connections": ctx.metrics().connections_active(),
        "uptime_s": ctx.uptime_s(),
        // SEC-059: transport-encryption posture — a boolean, never key material.
        "tls": ctx.tls_enabled(),
    });
    // HWA-013: the effective configuration — probe inputs, derived values
    // with their sources, and the per-kernel SIMD selection. Pre-rendered at
    // install, so this stays a clone on the < 50 ms path (OBS-061).
    if let Some(effective) = ctx.effective_config()
        && let Some(map) = body.as_object_mut()
    {
        map.insert("config".into(), effective.clone());
    }
    // OPS-040: the reloadable values actually in force, with each one's
    // source — this is how an operator confirms a reload landed (and, when
    // a value looks unchanged, sees that `env` outranked the file).
    // Re-rendered on publish, so this is a clone here too.
    if let Some(reloadable) = ctx.reloadable_config()
        && let Some(map) = body.as_object_mut()
    {
        map.insert("reloadable".into(), reloadable);
    }
    // The resolved on-disk locations: sub-directories follow
    // `storage.data_dir` unless configured themselves, so this is the only
    // place an operator can read back where the data really lives.
    if let Some(storage) = ctx.storage_paths()
        && let Some(map) = body.as_object_mut()
    {
        map.insert("storage".into(), storage);
    }
    AdminResponse { status: code, body }
}

// --- GET /metrics (Prometheus text; T5.6 expands the metric set) ----------------

/// HELP/TYPE headers for the SPEC-015 TIER-080 pager series, in the order
/// [`fluxum_core::store::pager::MetricsSnapshot::samples`] emits them. Kept
/// beside the sample loop so a new counter there is a visible omission here.
pub(super) const TIER_080_HEADERS: &str = "\
# HELP fluxum_bufferpool_bytes Bytes currently resident in the buffer pool (TIER-004).
# TYPE fluxum_bufferpool_bytes gauge
# HELP fluxum_bufferpool_capacity_bytes The pool's configured ceiling (TIER-003).
# TYPE fluxum_bufferpool_capacity_bytes gauge
# HELP fluxum_bufferpool_hits_total Page requests served from the pool.
# TYPE fluxum_bufferpool_hits_total counter
# HELP fluxum_bufferpool_misses_total Page requests that faulted from the cold tier.
# TYPE fluxum_bufferpool_misses_total counter
# HELP fluxum_bufferpool_evictions_total Pages evicted, by kind (clean drop vs spill).
# TYPE fluxum_bufferpool_evictions_total counter
# HELP fluxum_page_reads_total Pages read from the cold tier, by index/data.
# TYPE fluxum_page_reads_total counter
# HELP fluxum_page_writes_total Pages written to the cold tier.
# TYPE fluxum_page_writes_total counter
# HELP fluxum_page_compression_raw_bytes_total Pre-compression page bytes (TIER-024).
# TYPE fluxum_page_compression_raw_bytes_total counter
# HELP fluxum_page_compression_stored_bytes_total Post-compression page bytes (TIER-024).
# TYPE fluxum_page_compression_stored_bytes_total counter
";

pub(super) async fn metrics(ctx: &Arc<ShardContext>) -> AdminResponse {
    let health = ctx.health();
    // OBS-012: publish the live queue depth before rendering the gauge.
    ctx.metrics().set_queue_depth(health.queue_depth);
    // OBS-020: refresh the active-subscription gauge from the manager.
    {
        let active = ctx.subscriptions.lock().await.plan_count();
        ctx.metrics()
            .set_subscriptions_active(i64::try_from(active).unwrap_or(i64::MAX));
    }
    // SEC-040/041: refresh guard pressure + overload state at scrape time.
    {
        let guard = ctx.conn_guard();
        ctx.metrics()
            .set_connguard_pressure(guard.tracked_ips() as u64, guard.evictions_total());
        let _ = ctx.overload_state(); // publishes the gauge + logs transitions
    }
    // OBS-010..OBS-050: the shard's own counter block (the default database).
    let mut text = ctx.metrics().prometheus(health.last_tx_id);
    // SPEC-025 OPS-051: the same series per named namespace, each carrying a
    // `namespace` label so a tenant's load is attributable. Only the series
    // lines are appended — the HELP/TYPE headers were already emitted above,
    // and repeating them for the same metric name is invalid exposition.
    let tenants = ctx.namespaces();
    for ns in &tenants {
        let block = ns
            .metrics()
            .prometheus_in_namespace(ns.name(), ns.last_tx_id());
        for line in block
            .lines()
            .filter(|l| !l.starts_with('#') && !l.is_empty())
        {
            text.push_str(line);
            text.push('\n');
        }
    }
    // SPEC-025 OPS-061: per-tenant quota usage and ceilings, so an operator
    // sees headroom and can alert *before* a tenant starts being refused. An
    // unset ceiling reports 0 ("no limit") next to usage, which is always
    // meaningful.
    if !tenants.is_empty() {
        text.push_str(
            "# HELP fluxum_tenant_memory_bytes Estimated in-memory footprint per tenant.\n\
             # TYPE fluxum_tenant_memory_bytes gauge\n",
        );
        for ns in &tenants {
            let _ = writeln!(
                text,
                "fluxum_tenant_memory_bytes{{namespace=\"{}\"}} {}",
                ns.name(),
                ns.memory_bytes()
            );
        }
        text.push_str(
            "# HELP fluxum_tenant_storage_bytes Durable commit-log bytes per tenant.\n\
             # TYPE fluxum_tenant_storage_bytes gauge\n",
        );
        for ns in &tenants {
            let _ = writeln!(
                text,
                "fluxum_tenant_storage_bytes{{namespace=\"{}\"}} {}",
                ns.name(),
                ns.storage_bytes()
            );
        }
        text.push_str(
            "# HELP fluxum_tenant_subscriptions_active Live subscription plans per tenant.\n\
             # TYPE fluxum_tenant_subscriptions_active gauge\n",
        );
        for ns in &tenants {
            let live = ns.subscriptions().lock().await.plan_count();
            let _ = writeln!(
                text,
                "fluxum_tenant_subscriptions_active{{namespace=\"{}\"}} {live}",
                ns.name(),
            );
        }
        text.push_str(
            "# HELP fluxum_tenant_quota_bytes Configured ceiling per tenant (0 = unlimited).\n\
             # TYPE fluxum_tenant_quota_bytes gauge\n",
        );
        for ns in &tenants {
            let q = *ns.quotas().quotas();
            let _ = writeln!(
                text,
                "fluxum_tenant_quota_bytes{{namespace=\"{}\", quota=\"memory\"}} {}",
                ns.name(),
                q.max_memory_bytes.unwrap_or(0),
            );
            let _ = writeln!(
                text,
                "fluxum_tenant_quota_bytes{{namespace=\"{}\", quota=\"storage\"}} {}",
                ns.name(),
                q.max_storage_bytes.unwrap_or(0),
            );
        }
        text.push_str(
            "# HELP fluxum_tenant_quota_exceeded_total Times a tenant hit a quota (OPS-060).\n\
             # TYPE fluxum_tenant_quota_exceeded_total counter\n",
        );
        for ns in &tenants {
            for quota in crate::quota::Quota::ALL {
                let _ = writeln!(
                    text,
                    "fluxum_tenant_quota_exceeded_total{{namespace=\"{}\", quota=\"{}\"}} {}",
                    ns.name(),
                    quota.as_str(),
                    ns.quotas().exceeded(quota),
                );
            }
        }
    }
    // OBS-030/031: per-table row counts + an estimated MemStore footprint.
    // Lock-free snapshot; the byte figure is a schema-width estimate (the
    // spec's `memstore_bytes` is explicitly an estimate, not exact bytes).
    {
        // On a multi-shard deployment every host reports its own block —
        // TST-112's "within budget on every shard" reads exactly these
        // shard-labelled series, and emitting one shard N times would make
        // that assertion pass vacuously. HELP/TYPE headers are emitted once
        // (repeating them per shard is invalid exposition).
        let hosts: Vec<Arc<ShardContext>> = match ctx.coord() {
            Some(coord) => coord
                .shard_ids()
                .collect::<Vec<_>>()
                .into_iter()
                .filter_map(|s| coord.host(s).cloned())
                .collect(),
            None => vec![Arc::clone(ctx)],
        };
        text.push_str(
            "# HELP fluxum_table_rows Committed rows per table.
             # TYPE fluxum_table_rows gauge
",
        );
        let mut memstore_block = String::from(
            "# HELP fluxum_memstore_bytes Estimated in-memory CommittedState size.
             # TYPE fluxum_memstore_bytes gauge
",
        );
        for host in &hosts {
            let shard = host.shard_id;
            let snapshot = host.store().snapshot();
            let mut estimated_bytes: u64 = 0;
            for table in host.store().table_schemas() {
                let table_id = fluxum_core::store::TableId::of(table.name);
                let rows = snapshot.row_count(table_id).unwrap_or(0);
                let rows_u64 = u64::try_from(rows).unwrap_or(u64::MAX);
                let _ = writeln!(
                    text,
                    "fluxum_table_rows{{shard=\"{shard}\",table=\"{}\"}} {rows_u64}",
                    table.name,
                );
                // ~24 bytes per column (tag + inline scalar / small heap) — a
                // coarse gauge for RAM-pressure alerting (OBS-031).
                let width = u64::try_from(table.columns.len()).unwrap_or(0) * 24;
                estimated_bytes = estimated_bytes.saturating_add(rows_u64.saturating_mul(width));
            }
            let _ = writeln!(
                memstore_block,
                "fluxum_memstore_bytes{{shard=\"{shard}\"}} {estimated_bytes}",
            );
        }
        text.push_str(&memstore_block);
        // SPEC-015 TIER-080: the buffer-pool and page-I/O series. Unlike
        // `fluxum_memstore_bytes` above these are *exact* — the pool's own
        // accounting, the enforced side of the TIER-004 budget — which is
        // what makes them the in-process witness SPEC-013 TST-111 samples
        // alongside process RSS, and what proves eviction engaged under
        // pressure. SPEC-015 TIER-061 / SPEC-022 RV-020 ride along: pool
        // occupancy alone cannot tell a pool full of live pages from one
        // full of version garbage waiting on a pinned snapshot.
        text.push_str(
            "# HELP fluxum_reclaim_pending_pages Superseded pages awaiting reclamation (TIER-061).
             # TYPE fluxum_reclaim_pending_pages gauge
             # HELP fluxum_reclaim_live_versions Pinned versions holding those pages live.
             # TYPE fluxum_reclaim_live_versions gauge
             # HELP fluxum_temporal_window_snapshots Snapshots AS OF can currently reach (RV-020).
             # TYPE fluxum_temporal_window_snapshots gauge
             # HELP fluxum_temporal_window_budget_evictions_total Snapshots dropped to honour the RV-020 byte ceiling.
             # TYPE fluxum_temporal_window_budget_evictions_total counter
",
        );
        for host in &hosts {
            let shard = host.shard_id;
            let store = host.store();
            let pending = store.reclaim_pending();
            let _ = writeln!(
                text,
                "fluxum_reclaim_pending_pages{{shard=\"{shard}\"}} {}
                 fluxum_reclaim_live_versions{{shard=\"{shard}\"}} {}
                 fluxum_temporal_window_snapshots{{shard=\"{shard}\"}} {}
                 fluxum_temporal_window_budget_evictions_total{{shard=\"{shard}\"}} {}",
                pending.pages,
                pending.live_versions,
                store.temporal_window_len(),
                store.temporal_window_budget_evictions(),
            );
        }
        text.push_str(TIER_080_HEADERS);
        for host in &hosts {
            let shard = host.shard_id;
            let pager = host.store().pager().metrics().snapshot();
            for (series, value) in pager.samples() {
                // A series may already carry labels (`name{kind="clean"}`);
                // the shard label is spliced into the existing set.
                let line = match series.split_once('{') {
                    Some((name, rest)) => format!("{name}{{shard=\"{shard}\",{rest} {value}"),
                    None => format!("{series}{{shard=\"{shard}\"}} {value}"),
                };
                text.push_str(&line);
                text.push('\n');
            }
        }
    }
    // SPEC-017 CT-014/034: transform read-error and signature-verify meters.
    if let Some(engine) = ctx.store().transform_engine() {
        text.push_str(&format!(
            "# HELP fluxum_transform_read_errors_total Read-path transform errors (CT-014).\n\
             # TYPE fluxum_transform_read_errors_total counter\n\
             fluxum_transform_read_errors_total {}\n\
             # HELP fluxum_signature_verify_failures_total Signature verifications that failed (CT-034).\n\
             # TYPE fluxum_signature_verify_failures_total counter\n\
             fluxum_signature_verify_failures_total {}\n",
            engine.read_errors(),
            engine.verify_failures(),
        ));
    }
    // SPEC-020 PLG-030: per-plugin panic/error meters.
    if let Some(registry) = ctx.plugins() {
        let bound = registry.plugins();
        if !bound.is_empty() {
            text.push_str(
                "# HELP fluxum_plugin_panics_total Panics caught per plugin (PLG-030).\n\
                 # TYPE fluxum_plugin_panics_total counter\n",
            );
            for plugin in bound {
                text.push_str(&format!(
                    "fluxum_plugin_panics_total{{plugin=\"{}\"}} {}\n",
                    plugin.name,
                    plugin.state.panics()
                ));
            }
            text.push_str(
                "# HELP fluxum_plugin_errors_total Non-panic plugin errors (PLG-031).\n\
                 # TYPE fluxum_plugin_errors_total counter\n",
            );
            for plugin in bound {
                text.push_str(&format!(
                    "fluxum_plugin_errors_total{{plugin=\"{}\"}} {}\n",
                    plugin.name,
                    plugin.state.errors()
                ));
            }
            // PLG-031: the sidecar breakdown. Emitted only when a sidecar is
            // bound, but then for every reason label — an alert on
            // `rate(...{reason="timeout"})` must not go stale-for-lack-of-series
            // on the run where the sidecar is healthy.
            let sidecars: Vec<_> = bound.iter().filter(|p| p.sidecar.is_some()).collect();
            if !sidecars.is_empty() {
                text.push_str(
                    "# HELP fluxum_plugin_sidecar_errors_total Sidecar Plugin RPC failures \
                     by reason (PLG-031).\n\
                     # TYPE fluxum_plugin_sidecar_errors_total counter\n",
                );
                for plugin in &sidecars {
                    let Some(stats) = &plugin.sidecar else {
                        continue;
                    };
                    for (reason, count) in stats.by_reason() {
                        text.push_str(&format!(
                            "fluxum_plugin_sidecar_errors_total{{plugin=\"{}\", reason=\"{reason}\"}} {count}\n",
                            plugin.name,
                        ));
                    }
                }
                text.push_str(
                    "# HELP fluxum_plugin_sidecar_calls_total Sidecar Plugin RPC calls \
                     attempted (PLG-031).\n\
                     # TYPE fluxum_plugin_sidecar_calls_total counter\n",
                );
                for plugin in &sidecars {
                    let Some(stats) = &plugin.sidecar else {
                        continue;
                    };
                    text.push_str(&format!(
                        "fluxum_plugin_sidecar_calls_total{{plugin=\"{}\"}} {}\n",
                        plugin.name,
                        stats.calls()
                    ));
                }
                text.push_str(
                    "# HELP fluxum_plugin_sidecar_breaker_open Whether the sidecar circuit \
                     breaker is currently open (PLG-031).\n\
                     # TYPE fluxum_plugin_sidecar_breaker_open gauge\n",
                );
                for plugin in &sidecars {
                    let Some(stats) = &plugin.sidecar else {
                        continue;
                    };
                    let open =
                        u8::from(stats.breaker_state() == fluxum_core::plugin::BreakerState::Open);
                    text.push_str(&format!(
                        "fluxum_plugin_sidecar_breaker_open{{plugin=\"{}\"}} {open}\n",
                        plugin.name,
                    ));
                }
            }
        }
    }
    AdminResponse {
        status: 200,
        body: Value::String(text), // the caller serves it as text/plain
    }
}
