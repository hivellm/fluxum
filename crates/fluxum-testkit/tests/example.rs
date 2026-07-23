//! The author-facing suite (SPEC-024 DEV-020/DEV-021, task 1.7): these tests
//! read exactly as a module author's own would — link the module crate,
//! boot a seeded [`TestShard`], drive reducers, assert on rows, diffs,
//! replay, and recovery.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use fluxum_core::store::RowValue;
// Link the demo module so its #[fluxum::table]/#[fluxum::reducer]
// registrations survive the linker (OQ-1) — the one line every author's
// test file needs for their own crate.
use fluxum_demo as _;
use fluxum_testkit::{FluxValue, TestShard};

fn title_of(row: &[RowValue]) -> String {
    // Task columns, declaration order: id, owner, title, done.
    match &row[2] {
        RowValue::Str(title) => title.clone(),
        other => panic!("expected a title string, got {other:?}"),
    }
}

#[test]
fn a_non_owner_call_errors_and_leaves_the_row_unchanged() {
    // The DEV-020 acceptance scenario, verbatim: `complete_task` from a
    // non-owner identity returns Err and the task row is untouched.
    let mut shard = TestShard::new(7).expect("boot");
    let alice = shard.identity("alice");
    let mallory = shard.identity("mallory");

    shard
        .call(
            alice,
            "add_task",
            vec![FluxValue::Str("ship the testkit".into())],
        )
        .expect("alice adds her task");
    let before = shard.rows("Task");
    assert_eq!(before.len(), 1);

    let err = shard
        .call(mallory, "complete_task", vec![FluxValue::I64(1)])
        .expect_err("mallory must be refused");
    assert!(err.to_string().contains("not your task"), "{err}");
    assert_eq!(
        shard.rows("Task"),
        before,
        "rolled back: the row is unchanged"
    );
}

#[test]
fn receipts_expose_the_emitted_diff() {
    let mut shard = TestShard::new(11).expect("boot");
    let alice = shard.identity("alice");

    let receipt = shard
        .call(
            alice,
            "add_task",
            vec![FluxValue::Str("read the diff".into())],
        )
        .expect("add_task");
    assert_eq!(receipt.touched(), vec!["Task"]);
    let inserted = receipt.inserted("Task");
    assert_eq!(inserted.len(), 1);
    assert_eq!(title_of(&inserted[0]), "read the diff");
    assert!(receipt.deleted("Task").is_empty());

    // Completing the task is an upsert: the diff carries the old row out
    // and the new row in, atomically.
    let receipt = shard
        .call(alice, "complete_task", vec![FluxValue::I64(1)])
        .expect("complete_task");
    let inserted = receipt.inserted("Task");
    let deleted = receipt.deleted("Task");
    assert_eq!(inserted.len(), 1);
    assert_eq!(deleted.len(), 1);
    assert_eq!(inserted[0][3], RowValue::Bool(true), "now done");
    assert_eq!(deleted[0][3], RowValue::Bool(false), "was not done");
}

#[test]
fn the_same_seed_produces_a_bit_identical_run() {
    // DEV-020's determinism contract: seeded clock + seeded RNG → the same
    // test body always lands on the same state.
    let run = |seed: u64| {
        let mut shard = TestShard::new(seed).expect("boot");
        let user = shard.identity("user");
        for _ in 0..5 {
            let n = shard.rng().below(1000);
            shard
                .call(user, "add_task", vec![FluxValue::Str(format!("task {n}"))])
                .expect("add_task");
        }
        (shard.fingerprint(), shard.rows("Task"))
    };
    let (fp_a, rows_a) = run(42);
    let (fp_b, rows_b) = run(42);
    assert_eq!(fp_a, fp_b, "same seed, same fingerprint");
    assert_eq!(rows_a, rows_b, "same seed, same rows");

    let (fp_c, _) = run(43);
    assert_ne!(fp_a, fp_c, "a different seed drives different inputs");
}

#[test]
fn a_recorded_sequence_replays_deterministically() {
    // DEV-020: record a mixed run — commits, a reducer rejection, a
    // non-owner rejection — and replay the tape on a fresh shard: every
    // outcome matches and the final state is bit-identical.
    let mut shard = TestShard::new(23).expect("boot");
    let alice = shard.identity("alice");
    let mallory = shard.identity("mallory");

    shard
        .call(alice, "add_task", vec![FluxValue::Str("one".into())])
        .expect("commit");
    shard
        .call(alice, "add_task", vec![FluxValue::Str(String::new())])
        .expect_err("empty title is rejected");
    shard
        .call(mallory, "complete_task", vec![FluxValue::I64(1)])
        .expect_err("non-owner is rejected");
    shard
        .call(alice, "complete_task", vec![FluxValue::I64(1)])
        .expect("owner completes");

    let replayed = TestShard::replay(23, shard.recording()).expect("replay");
    assert_eq!(
        replayed.fingerprint(),
        shard.fingerprint(),
        "replay converges on the identical state"
    );
    assert_eq!(replayed.recording().len(), shard.recording().len());
}

#[test]
fn a_clean_crash_recovers_every_commit() {
    // DEV-021 baseline: kill -9 with no disk fault — recovery replays the
    // log and the shard continues, auto-inc watermark included.
    let mut shard = TestShard::new(5).expect("boot");
    let alice = shard.identity("alice");
    shard
        .call(alice, "add_task", vec![FluxValue::Str("first".into())])
        .expect("first");
    shard
        .call(alice, "add_task", vec![FluxValue::Str("second".into())])
        .expect("second");
    let before = shard.fingerprint();

    let mut shard = shard.crash().recover().expect("recover");
    assert_eq!(shard.fingerprint(), before, "every commit survived");

    // The recovered shard keeps working, and no auto-inc id is ever reused
    // — STG-040 allocates in batches, so a gap after recovery is expected
    // and correct (the watermark rides the commit log).
    let receipt = shard
        .call(alice, "add_task", vec![FluxValue::Str("third".into())])
        .expect("post-recovery call");
    let inserted = receipt.inserted("Task");
    match inserted[0][0] {
        RowValue::U64(id) => assert!(id > 2, "id {id} must not reuse 1 or 2"),
        ref other => panic!("expected a U64 id, got {other:?}"),
    }
}

#[test]
fn a_lost_fsync_drops_exactly_the_last_commit() {
    // DEV-021 mid-commit crash: the last entry vanishes at its boundary —
    // recovery keeps the prefix, loses only the tail commit.
    let mut shard = TestShard::new(6).expect("boot");
    let alice = shard.identity("alice");
    shard
        .call(alice, "add_task", vec![FluxValue::Str("kept".into())])
        .expect("kept");
    shard
        .call(alice, "add_task", vec![FluxValue::Str("lost".into())])
        .expect("lost");

    let mut crashed = shard.crash();
    assert!(crashed.lose_last_commit().expect("cut the tail"));
    let shard = crashed.recover().expect("recover");

    let titles: Vec<String> = shard.rows("Task").iter().map(|r| title_of(r)).collect();
    assert_eq!(titles, vec!["kept"], "only the un-fsynced tail is gone");
}

#[test]
fn a_torn_tail_is_quarantined_and_the_prefix_survives() {
    // DEV-021 torn write: the last entry is cut mid-frame; recovery must
    // quarantine it and keep everything before it, then keep serving.
    let mut shard = TestShard::new(9).expect("boot");
    let alice = shard.identity("alice");
    shard
        .call(alice, "add_task", vec![FluxValue::Str("intact".into())])
        .expect("intact");
    shard
        .call(alice, "add_task", vec![FluxValue::Str("torn".into())])
        .expect("torn");

    let mut crashed = shard.crash();
    assert!(crashed.tear_last_commit().expect("tear the tail"));
    let mut shard = crashed.recover().expect("recover");

    let titles: Vec<String> = shard.rows("Task").iter().map(|r| title_of(r)).collect();
    assert_eq!(titles, vec!["intact"], "the torn entry was quarantined");

    shard
        .call(alice, "add_task", vec![FluxValue::Str("after".into())])
        .expect("the recovered shard keeps serving");
    assert_eq!(shard.rows("Task").len(), 2);
}

#[test]
fn faults_on_an_empty_log_report_nothing_to_corrupt() {
    let shard = TestShard::new(1).expect("boot");
    let mut crashed = shard.crash();
    assert!(
        !crashed.lose_last_commit().expect("no entries"),
        "nothing to lose"
    );
    assert!(
        !crashed.tear_last_commit().expect("no entries"),
        "nothing to tear"
    );
    let shard = crashed.recover().expect("recover an empty shard");
    assert!(shard.rows("Task").is_empty());
}

#[test]
fn the_simulated_clock_is_visible_and_advanceable() {
    let mut shard = TestShard::new(3).expect("boot");
    let t0 = shard.now();
    shard.advance(60_000_000); // one simulated minute
    assert_eq!(shard.now().as_micros() - t0.as_micros(), 60_000_000);

    // The clock stamps ctx.timestamp: send_chat writes sent_at from it.
    let user = shard.identity("user");
    shard
        .call(
            user,
            "send_chat",
            vec![FluxValue::I64(1), FluxValue::Str("hi".into())],
        )
        .expect("send_chat");
    let rows = shard.rows("ChatMessage");
    assert_eq!(rows.len(), 1);
    // ChatMessage columns: id, sender, channel, content, sent_at.
    assert_eq!(
        rows[0][4],
        RowValue::Timestamp(shard.now()),
        "ctx.timestamp came from the simulated clock"
    );
}
