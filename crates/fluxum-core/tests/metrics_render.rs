//! The full Prometheus exposition (SPEC-012), rendered with every meter
//! populated — replication peers, CDC sinks, every rejection/abort/drop
//! reason, every lifecycle and overload state — so each label arm and each
//! conditional render block is pinned by the text it emits.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::metrics::{
    AdminRejectReason, ConnRejectReason, DropReason, FanoutStage, Metrics, OverloadState,
    QueryAbortReason, QueryRateBucket, ReducerAbortReason, ReducerOutcome, SessionRejectReason,
    ShardState,
};

#[test]
fn every_reason_label_is_stable() {
    // The label strings ARE the metric contract — alerts key on them.
    for (label, want) in [
        (DropReason::BufferFull.label(), "buffer_full"),
        (DropReason::IdleTimeout.label(), "idle_timeout"),
        (DropReason::FrameTooLarge.label(), "frame_too_large"),
        (ReducerOutcome::Ok.label(), "ok"),
        (ReducerOutcome::Err.label(), "err"),
        (ReducerOutcome::RateLimited.label(), "rate_limited"),
        (ReducerOutcome::QueueFull.label(), "queue_full"),
    ] {
        assert_eq!(label, want);
    }
    for state in [
        ShardState::Starting,
        ShardState::Recovering,
        ShardState::Ready,
        ShardState::ShuttingDown,
    ] {
        assert!(!state.as_str().is_empty());
    }
    for state in [
        OverloadState::Normal,
        OverloadState::ShedPreauth,
        OverloadState::ShedAllNew,
    ] {
        assert!(!state.as_str().is_empty());
    }
    for reason in [
        SessionRejectReason::UnknownToken,
        SessionRejectReason::IpMismatch,
        SessionRejectReason::Expired,
        SessionRejectReason::Revoked,
    ] {
        assert!(!reason.as_str().is_empty());
    }
    for reason in [
        AdminRejectReason::UntrustedIp,
        AdminRejectReason::Unauthenticated,
    ] {
        assert!(!reason.as_str().is_empty());
    }
    for reason in [
        QueryAbortReason::Limit,
        QueryAbortReason::ScanBudget,
        QueryAbortReason::Deadline,
    ] {
        assert!(!reason.as_str().is_empty());
    }
    for reason in [ReducerAbortReason::Deadline, ReducerAbortReason::Alloc] {
        assert!(!reason.as_str().is_empty());
    }
    for bucket in [QueryRateBucket::Identity, QueryRateBucket::Source] {
        assert!(!bucket.as_str().is_empty());
    }
    for reason in [
        ConnRejectReason::ConnCap,
        ConnRejectReason::AcceptRate,
        ConnRejectReason::FailedAuth,
        ConnRejectReason::HandshakeBudget,
        ConnRejectReason::ProxyPreamble,
        ConnRejectReason::ProxyHeader,
        ConnRejectReason::Blocked,
        ConnRejectReason::GlobalCap,
        ConnRejectReason::Overload,
    ] {
        assert!(!reason.as_str().is_empty());
    }
}

#[test]
fn the_exposition_renders_every_populated_section() {
    let metrics = Metrics::new(9);
    assert_eq!(metrics.shard_id(), 9);

    // Reducers, commits, queue, subscriptions, fan-out.
    metrics.record_reducer("send_chat", ReducerOutcome::Ok, 1_500);
    metrics.record_reducer("send_chat", ReducerOutcome::Err, 12_000);
    metrics.record_reducer("send_chat", ReducerOutcome::RateLimited, 10);
    metrics.record_reducer("send_chat", ReducerOutcome::QueueFull, 10);
    metrics.note_commit();
    metrics.note_rollback();
    metrics.set_queue_depth(3);
    metrics.set_subscriptions_active(4);
    metrics.note_fanout(17);
    for stage in [
        FanoutStage::RecvLag,
        FanoutStage::Eval,
        FanoutStage::Enqueue,
        FanoutStage::Flush,
        FanoutStage::ServerTotal,
        FanoutStage::QueueWait,
    ] {
        metrics.note_fanout_stage(stage, 42);
    }
    for reason in [
        DropReason::BufferFull,
        DropReason::IdleTimeout,
        DropReason::FrameTooLarge,
    ] {
        metrics.note_drop(reason);
    }

    // Connections, auth, rejections of every flavour.
    metrics.note_connect();
    metrics.note_connect();
    metrics.note_disconnect();
    assert_eq!(metrics.connections_active(), 1);
    metrics.note_auth(true);
    metrics.note_auth(false);
    metrics.note_conn_rejected(ConnRejectReason::Blocked);
    assert_eq!(metrics.conn_rejected(ConnRejectReason::Blocked), 1);
    metrics.note_session_rejected(SessionRejectReason::Revoked);
    assert_eq!(metrics.session_rejected(SessionRejectReason::Revoked), 1);
    metrics.note_admin_rejected(AdminRejectReason::UntrustedIp);
    assert_eq!(metrics.admin_rejected(AdminRejectReason::UntrustedIp), 1);
    metrics.note_query_aborted(QueryAbortReason::ScanBudget);
    assert_eq!(metrics.query_aborted(QueryAbortReason::ScanBudget), 1);
    metrics.note_reducer_aborted(ReducerAbortReason::Deadline);
    assert_eq!(metrics.reducer_aborted(ReducerAbortReason::Deadline), 1);
    metrics.note_query_rate_limited(QueryRateBucket::Identity);
    assert_eq!(metrics.query_rate_limited(QueryRateBucket::Identity), 1);
    metrics.set_connguard_pressure(5, 2);

    // Lifecycle, overload, slow-reducer knob.
    metrics.set_shard_state(ShardState::Ready);
    assert_eq!(metrics.shard_state(), ShardState::Ready);
    let previous = metrics.swap_overload_state(OverloadState::ShedPreauth);
    assert_eq!(previous, OverloadState::Normal);
    assert_eq!(metrics.overload_state(), OverloadState::ShedPreauth);
    metrics.set_slow_reducer_threshold_us(1_000);
    assert!(metrics.is_slow(2_000));
    assert!(!metrics.is_slow(500));
    metrics.set_recovered_tx_id(41);
    metrics.set_archive_segments_pending(6);
    assert_eq!(metrics.archive_segments_pending(), 6);

    // Replication: role, epoch, peers (offset + lag), degradation, fencing.
    metrics.set_replication_role(true);
    assert!(metrics.replication_role_primary());
    metrics.set_replication_epoch(3);
    metrics.set_replication_connected(2);
    metrics.set_replication_peer("replica-a", 40, 2);
    metrics.set_replication_peer("replica-b", 39, 3);
    assert_eq!(metrics.replication_peer("replica-a"), Some((40, 2)));
    metrics.note_semi_sync_wait(120);
    assert_eq!(metrics.semi_sync_waits(), (120, 1), "(sum µs, count)");
    metrics.note_election();
    assert_eq!(metrics.replication_elections_total(), 1);
    metrics.note_full_sync();
    assert_eq!(metrics.replication_full_syncs_total(), 1);
    metrics.set_replication_degraded(true);
    assert!(metrics.replication_degraded());
    metrics.note_replication_fenced();
    assert_eq!(metrics.replication_fenced_total(), 1);

    // CDC sinks (PLG-050).
    metrics.set_sink_lag("warehouse", 7);
    metrics.note_sink_delivered("warehouse", 20);
    metrics.note_sink_dropped("warehouse");
    metrics.note_sink_error("warehouse");
    assert_eq!(metrics.sink_lag("warehouse"), 7);
    assert_eq!(metrics.sink_delivered("warehouse"), 20);
    assert_eq!(metrics.sink_dropped("warehouse"), 1);
    assert_eq!(metrics.sink_errors("warehouse"), 1);

    let text = metrics.prometheus(41);
    for series in [
        "fluxum_reducer_calls_total",
        "fluxum_subscriber_drops_total",
        "fluxum_fanout_messages_total",
        "fluxum_fanout_stage_us",
        "fluxum_connections_active",
        "fluxum_auth_failure_total",
        "fluxum_conn_rejected_total",
        "fluxum_session_rejected_total",
        "fluxum_admin_rejected_total",
        "fluxum_query_aborted_total",
        "fluxum_reducer_aborted_total",
        "fluxum_query_rate_limited_total",
        "fluxum_connguard_tracked_ips",
        "fluxum_overload_state",
        "fluxum_shard_state",
        "fluxum_shard_recovered_tx_id",
        "fluxum_archive_segments_pending",
        "fluxum_replication_role",
        "fluxum_replication_epoch",
        "fluxum_replication_connected_replicas",
        "fluxum_replication_offset{shard=\"9\",peer=\"replica-a\"} 40",
        "fluxum_replication_lag_tx{shard=\"9\",peer=\"replica-b\"} 3",
        "fluxum_replication_semi_sync_wait_us",
        "fluxum_replication_elections_total",
        "fluxum_replication_full_syncs_total",
        "fluxum_replication_degraded",
        "fluxum_replication_fenced_total",
        "fluxum_plugin_sink_lag{shard=\"9\",sink=\"warehouse\"} 7",
        "fluxum_plugin_sink_delivered_total",
    ] {
        assert!(text.contains(series), "missing `{series}` in:\n{text}");
    }

    // The per-namespace rendering carries the tenant label on every series.
    let tenant = metrics.prometheus_in_namespace("acme", 41);
    assert!(tenant.contains("namespace=\"acme\""), "{tenant}");

    // Removal empties the conditional blocks again.
    metrics.remove_replication_peer("replica-a");
    metrics.remove_replication_peer("replica-b");
    metrics.remove_sink("warehouse");
    assert_eq!(metrics.replication_peer("replica-a"), None);
    let text = metrics.prometheus(41);
    assert!(!text.contains("fluxum_replication_offset"), "peers removed");
    assert!(!text.contains("fluxum_plugin_sink_lag"), "sinks removed");
}
