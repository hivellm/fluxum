use super::*;

#[test]
fn histogram_places_observations_and_renders_pinned_buckets() {
    let m = Metrics::new(0);
    // 40µs → le=50 bucket; 300µs → le=500; 60000µs → +Inf only.
    m.record_reducer("send", ReducerOutcome::Ok, 40);
    m.record_reducer("send", ReducerOutcome::Ok, 300);
    m.record_reducer("send", ReducerOutcome::Err, 60_000);
    let text = m.prometheus(7);
    // Every pinned bucket boundary appears exactly once per reducer.
    for bound in REDUCER_DURATION_BUCKETS_US {
        assert!(
            text.contains(&format!("reducer=\"send\",le=\"{bound}\"")),
            "missing le={bound}"
        );
    }
    // Cumulative: le=50 has 1, le=500 has 2, +Inf has all 3.
    assert!(text.contains("reducer=\"send\",le=\"50\"} 1"));
    assert!(text.contains("reducer=\"send\",le=\"500\"} 2"));
    assert!(text.contains("reducer=\"send\",le=\"+Inf\"} 3"));
    assert!(text.contains("fluxum_reducer_duration_us_count{shard=\"0\",reducer=\"send\"} 3"));
    assert!(text.contains("fluxum_reducer_duration_us_sum{shard=\"0\",reducer=\"send\"} 60340"));
}

#[test]
fn outcome_counters_and_tx_counters_render() {
    let m = Metrics::new(2);
    m.record_reducer("a", ReducerOutcome::Ok, 10);
    m.record_reducer("a", ReducerOutcome::RateLimited, 1);
    m.record_reducer("a", ReducerOutcome::QueueFull, 1);
    m.note_commit();
    m.note_rollback();
    m.note_commit();
    let text = m.prometheus(0);
    assert!(text.contains("reducer=\"a\",outcome=\"ok\"} 1"));
    assert!(text.contains("reducer=\"a\",outcome=\"rate_limited\"} 1"));
    assert!(text.contains("reducer=\"a\",outcome=\"queue_full\"} 1"));
    assert!(text.contains("fluxum_tx_commits_total{shard=\"2\"} 2"));
    assert!(text.contains("fluxum_tx_rollbacks_total{shard=\"2\"} 1"));
}

#[test]
fn shard_state_and_slow_threshold_track() {
    let m = Metrics::new(0);
    assert_eq!(m.shard_state(), ShardState::Ready);
    assert!(!m.is_slow(4999));
    m.set_slow_reducer_threshold_us(1);
    assert!(m.is_slow(2));
    m.set_shard_state(ShardState::Recovering);
    assert_eq!(m.shard_state(), ShardState::Recovering);
    assert_eq!(m.shard_state().as_str(), "recovering");
    assert!(
        m.prometheus(0)
            .contains("fluxum_shard_state{shard=\"0\"} 1")
    );
}

/// SEC-045/046/047: the bounds counters accumulate per reason and every
/// label renders a series even at zero.
#[test]
fn execution_bounds_counters_accumulate_and_render() {
    let m = Metrics::new(3);
    m.note_query_aborted(QueryAbortReason::ScanBudget);
    m.note_query_aborted(QueryAbortReason::ScanBudget);
    m.note_query_aborted(QueryAbortReason::Deadline);
    m.note_reducer_aborted(ReducerAbortReason::Alloc);
    m.note_query_rate_limited(QueryRateBucket::Source);
    assert_eq!(m.query_aborted(QueryAbortReason::ScanBudget), 2);
    assert_eq!(m.query_aborted(QueryAbortReason::Limit), 0);
    assert_eq!(m.reducer_aborted(ReducerAbortReason::Alloc), 1);
    assert_eq!(m.query_rate_limited(QueryRateBucket::Source), 1);
    let text = m.prometheus(0);
    assert!(text.contains("fluxum_query_aborted_total{shard=\"3\", reason=\"scan_budget\"} 2"));
    assert!(text.contains("fluxum_query_aborted_total{shard=\"3\", reason=\"limit\"} 0"));
    assert!(text.contains("fluxum_query_aborted_total{shard=\"3\", reason=\"deadline\"} 1"));
    assert!(text.contains("fluxum_reducer_aborted_total{shard=\"3\", reason=\"alloc\"} 1"));
    assert!(text.contains("fluxum_reducer_aborted_total{shard=\"3\", reason=\"deadline\"} 0"));
    assert!(text.contains("fluxum_query_rate_limited_total{shard=\"3\", bucket=\"source\"} 1"));
    assert!(text.contains("fluxum_query_rate_limited_total{shard=\"3\", bucket=\"identity\"} 0"));
}

#[test]
fn fanout_connection_and_drop_counters_accumulate() {
    let m = Metrics::new(0);
    m.note_fanout(3);
    m.note_fanout(2);
    m.note_drop(DropReason::BufferFull);
    m.note_connect();
    m.note_connect();
    m.note_disconnect();
    m.note_auth(true);
    m.note_auth(false);
    m.set_subscriptions_active(3);
    let text = m.prometheus(0);
    assert!(text.contains("fluxum_fanout_messages_total{shard=\"0\"} 2"));
    assert!(text.contains("fluxum_fanout_rows_total{shard=\"0\"} 5"));
    assert!(text.contains("reason=\"buffer_full\"} 1"));
    assert!(text.contains("fluxum_connections_active{shard=\"0\"} 1"));
    assert!(text.contains("fluxum_connections_total{shard=\"0\"} 2"));
    assert!(text.contains("fluxum_auth_success_total{shard=\"0\"} 1"));
    assert!(text.contains("fluxum_auth_failure_total{shard=\"0\"} 1"));
    assert!(text.contains("fluxum_subscriptions_active{shard=\"0\"} 3"));
}
