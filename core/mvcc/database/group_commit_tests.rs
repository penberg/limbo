use super::{get_rows, FixedYieldInjector, MvccTestDbNoConn};
use crate::mvcc::database::{CommitCoordinator, CommitYieldPoint, GroupWork, LogRecord};
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use crate::{Connection, Database, LimboError, StepResult, Value};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

fn pragma_int(conn: &Arc<Connection>, query: &str) -> i64 {
    let rows = get_rows(conn, query);
    assert_eq!(rows.len(), 1, "{query} returned {rows:?}");
    match rows[0][0] {
        Value::Numeric(crate::Numeric::Integer(n)) => n,
        ref other => panic!("{query} returned {other:?}"),
    }
}

fn exec_retry(conn: &Arc<Connection>, sql: &str) -> Result<(), LimboError> {
    for _ in 0..100_000 {
        match conn.execute(sql) {
            Ok(()) => return Ok(()),
            Err(LimboError::Busy) => std::thread::yield_now(),
            Err(err) => return Err(err),
        }
    }
    Err(LimboError::Busy)
}

#[test]
fn group_commit_pragma_defaults_on_and_round_trips() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();

    assert_eq!(pragma_int(&conn, "PRAGMA mvcc_group_commit"), 1);

    for off in ["no", "off", "false", "0"] {
        conn.execute(format!("PRAGMA mvcc_group_commit = {off}"))
            .unwrap();
        assert_eq!(
            pragma_int(&conn, "PRAGMA mvcc_group_commit"),
            0,
            "`= {off}` should disable group commit"
        );
        conn.execute("PRAGMA mvcc_group_commit = on").unwrap();
        assert_eq!(pragma_int(&conn, "PRAGMA mvcc_group_commit"), 1);
    }
}

#[test]
fn group_commit_pragma_is_store_wide() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setter = db.connect();
    let observer = db.connect();

    setter.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    assert_eq!(pragma_int(&observer, "PRAGMA mvcc_group_commit"), 1);
}

#[test]
fn group_commit_pragma_needs_mvcc() {
    let io = Arc::new(crate::io::MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(crate::SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();

    let err = conn
        .execute("PRAGMA mvcc_group_commit = yes")
        .expect_err("group commit needs an MVCC store");
    assert!(
        err.to_string().contains("MVCC not enabled"),
        "unexpected error: {err}"
    );
    let err = conn
        .prepare("PRAGMA mvcc_group_commit")
        .expect_err("querying group commit needs an MVCC store");
    assert!(
        err.to_string().contains("MVCC not enabled"),
        "unexpected error: {err}"
    );
}

fn two_writers_both_commit(db: MvccTestDbNoConn, group_commit: bool) {
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    if group_commit {
        setup.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    }
    setup.close().unwrap();

    let conn_a = db.connect();
    let conn_b = db.connect();
    let rounds = 40i64;
    for round in 0..rounds {
        conn_a.execute("BEGIN CONCURRENT").unwrap();
        conn_b.execute("BEGIN CONCURRENT").unwrap();
        exec_retry(&conn_a, &format!("INSERT INTO t VALUES ({}, 1)", round * 2)).unwrap();
        exec_retry(
            &conn_b,
            &format!("INSERT INTO t VALUES ({}, 1)", round * 2 + 1),
        )
        .unwrap();
        exec_retry(&conn_a, "COMMIT").unwrap();
        exec_retry(&conn_b, "COMMIT").unwrap();
    }

    let reader = db.connect();
    assert_eq!(
        get_rows(&reader, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::from_i64(rounds * 2)]]
    );
}

#[test]
fn two_writers_both_commit_with_group_commit_truncate() {
    two_writers_both_commit(MvccTestDbNoConn::new_with_random_db(), true);
}

#[test]
fn two_writers_both_commit_with_group_commit_passive() {
    two_writers_both_commit(MvccTestDbNoConn::new_with_random_db_passive(), true);
}

#[test]
fn two_writers_both_commit_with_group_commit_off_truncate() {
    two_writers_both_commit(MvccTestDbNoConn::new_with_random_db(), false);
}

#[test]
fn two_writers_both_commit_with_group_commit_off_passive() {
    two_writers_both_commit(MvccTestDbNoConn::new_with_random_db_passive(), false);
}

#[test]
fn commits_batch_into_one_group() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    setup.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    setup.close().unwrap();

    const WRITERS: usize = 8;
    const MAX_ROUNDS: usize = 2000;
    let store = db.get_mvcc_store();
    let db = Arc::new(db);
    let barrier = Arc::new(Barrier::new(WRITERS));
    let committed = Arc::new(AtomicUsize::new(0));
    let largest_group = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + Duration::from_secs(120);

    let writers = (0..WRITERS)
        .map(|writer| {
            let db = db.clone();
            let barrier = barrier.clone();
            let committed = committed.clone();
            let largest_group = largest_group.clone();
            let stop = stop.clone();
            let store = store.clone();
            std::thread::spawn(move || {
                let conn = db.connect();
                for round in 0..MAX_ROUNDS {
                    let pk = (round * WRITERS + writer) as i64;
                    conn.execute("BEGIN CONCURRENT").unwrap();
                    exec_retry(&conn, &format!("INSERT INTO t VALUES ({pk}, 1)")).unwrap();
                    barrier.wait();
                    exec_retry(&conn, "COMMIT").unwrap();
                    committed.fetch_add(1, Ordering::Relaxed);
                    largest_group.fetch_max(store.last_group_commit_size(), Ordering::Relaxed);
                    barrier.wait();
                    if writer == 0 {
                        let grouped = largest_group.load(Ordering::Relaxed) >= 2;
                        stop.store(grouped || Instant::now() >= deadline, Ordering::Release);
                    }
                    barrier.wait();
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                }
                conn.close().unwrap();
            })
        })
        .collect::<Vec<_>>();
    for writer in writers {
        writer.join().unwrap();
    }

    let largest = largest_group.load(Ordering::Relaxed);
    assert!(
        largest >= 2,
        "no commit ever appended another connection's record: largest batch was {largest}"
    );

    let reader = db.connect();
    assert_eq!(
        get_rows(&reader, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::from_i64(
            committed.load(Ordering::Relaxed) as i64
        )]]
    );
}

#[test]
fn batched_records_survive_a_restart() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    const WRITERS: usize = 4;
    let rounds = 25usize;
    {
        let setup = db.connect();
        setup
            .execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        setup
            .execute("PRAGMA mvcc_checkpoint_threshold = -1")
            .unwrap();
        setup.execute("PRAGMA mvcc_group_commit = yes").unwrap();
        setup.close().unwrap();

        let shared = Arc::new(db.get_db());
        let barrier = Arc::new(Barrier::new(WRITERS));
        let writers = (0..WRITERS)
            .map(|writer| {
                let shared = shared.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let conn = shared.connect().unwrap();
                    for round in 0..rounds {
                        let pk = (round * WRITERS + writer) as i64;
                        conn.execute("BEGIN CONCURRENT").unwrap();
                        exec_retry(&conn, &format!("INSERT INTO t VALUES ({pk}, 1)")).unwrap();
                        barrier.wait();
                        exec_retry(&conn, "COMMIT").unwrap();
                    }
                    conn.close().unwrap();
                })
            })
            .collect::<Vec<_>>();
        for writer in writers {
            writer.join().unwrap();
        }
    }

    db.restart();
    let reader = db.connect();
    assert_eq!(
        get_rows(&reader, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::from_i64((WRITERS * rounds) as i64)]]
    );
}

fn empty_record(end_ts: u64) -> LogRecord {
    LogRecord::empty(end_ts, crate::alloc::DynAllocator::default())
}

#[test]
fn requeued_records_go_back_in_ticket_order() {
    let coordinator = CommitCoordinator::new();
    let first = coordinator.enqueue(1, empty_record(10));
    let second = coordinator.enqueue(2, empty_record(20));

    let batch = coordinator.take_pending();
    assert_eq!(
        batch.iter().map(|queued| queued.ticket).collect::<Vec<_>>(),
        vec![first, second]
    );

    let latecomer = coordinator.enqueue(3, empty_record(30));
    coordinator.requeue(batch.into_iter());
    assert_eq!(
        coordinator
            .take_pending()
            .iter()
            .map(|queued| queued.ticket)
            .collect::<Vec<_>>(),
        vec![first, second, latecomer]
    );
}

#[test]
fn drop_pending_only_removes_queued_records() {
    let coordinator = CommitCoordinator::new();

    let queued = coordinator.enqueue(1, empty_record(10));
    assert!(coordinator.drop_pending(queued));

    let claimed = coordinator.enqueue(2, empty_record(20));
    let _batch = coordinator.take_pending();
    assert!(
        !coordinator.drop_pending(claimed),
        "a record the leader already took is not in the queue"
    );
}

#[test]
fn durability_watermark_only_moves_forward() {
    let coordinator = CommitCoordinator::new();
    coordinator.note_written(7);
    coordinator.mark_durable(7);
    coordinator.mark_durable(3);
    assert_eq!(coordinator.durable_through(), 7);
}

#[test]
fn failed_leader_does_not_publish_unsynced_prefix() {
    let coordinator = CommitCoordinator::new();
    let first = coordinator.enqueue(1, empty_record(10));
    let second = coordinator.enqueue(2, empty_record(20));
    let mut batch = coordinator.take_pending();
    let writing = batch.pop_front().unwrap();
    assert_eq!(writing.ticket, first);
    coordinator.note_written(first);
    coordinator.requeue(batch.into_iter());

    assert_eq!(
        coordinator.durable_through(),
        0,
        "owning log bytes is not the same as fsyncing them"
    );
    assert_eq!(coordinator.written_through(), first);

    coordinator.mark_durable(second);
    assert_eq!(
        coordinator.durable_through(),
        first,
        "durable cannot pass a ticket that was not owned"
    );
}

#[test]
fn failed_mid_batch_leader_does_not_cover_retry_hole() {
    let coordinator = CommitCoordinator::new();
    let t2 = coordinator.enqueue(2, empty_record(20));
    let t3 = coordinator.enqueue(3, empty_record(30));
    let t4 = coordinator.enqueue(4, empty_record(40));
    assert_eq!((t2, t3, t4), (1, 2, 3));

    let mut batch = coordinator.take_pending();
    let _owned = batch.pop_front().unwrap();
    let _retry = batch.pop_front().unwrap();
    let later = batch.pop_front().unwrap();
    coordinator.note_written(t2);
    coordinator.request_retry(t3);
    coordinator.requeue(std::iter::once(later));
    coordinator.note_written(t4);
    coordinator.mark_durable(coordinator.written_through());

    assert!(
        coordinator.durable_through() < t3,
        "T3 is a retry hole; durable must not cover it (durable={}, t3={t3})",
        coordinator.durable_through()
    );
    assert!(
        !matches!(
            coordinator.take_work(),
            GroupWork::Lead { writing, .. } if writing.ticket >= t3
        ),
        "later tickets cannot be taken while an earlier ticket is a retry hole"
    );
    assert!(
        coordinator.take_retry(t3),
        "T3 must still be retrying, not acked via the watermark"
    );
}

fn step_until_yield_or_done(stmt: &mut crate::Statement) -> StepResult {
    for _ in 0..10_000 {
        match stmt.step().unwrap() {
            StepResult::IO => continue,
            other => return other,
        }
    }
    panic!("statement kept returning IO")
}

#[test]
fn dropped_commit_after_log_record_is_owned_still_commits() {
    dropped_after_own_still_commits(true);
}

#[test]
fn dropped_commit_after_log_record_is_owned_still_commits_without_group() {
    dropped_after_own_still_commits(false);
}

fn dropped_after_own_still_commits(group_commit: bool) {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    if group_commit {
        conn.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    }

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogicalLogOwned.point(),
    ])));

    {
        let mut commit = conn.prepare("COMMIT").unwrap();
        assert!(
            matches!(step_until_yield_or_done(&mut commit), StepResult::Yield),
            "COMMIT should yield after owning the logical-log record"
        );
    }

    assert!(
        conn.get_mv_tx_id().is_none(),
        "the dropped commit should be finished, not rolled back"
    );

    let rows = get_rows(&conn, "SELECT pk FROM t");
    assert_eq!(rows, vec![vec![Value::from_i64(1)]]);
}

#[test]
fn begin_immediate_still_commits_with_group_commit_on() {
    let db = MvccTestDbNoConn::new_with_random_db();
    let conn = db.connect();
    conn.execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    conn.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    conn.execute("BEGIN IMMEDIATE").unwrap();
    conn.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        get_rows(&conn, "SELECT pk FROM t"),
        vec![vec![Value::from_i64(1)]]
    );
}

#[test]
fn dropped_waiter_after_log_tx_still_commits() {
    let mut db = MvccTestDbNoConn::new_with_random_db();
    let setup = db.connect();
    setup
        .execute("CREATE TABLE t (pk INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    setup.execute("PRAGMA mvcc_group_commit = yes").unwrap();
    setup.close().unwrap();

    let conn_a = db.connect();
    let conn_b = db.connect();
    conn_a.execute("BEGIN CONCURRENT").unwrap();
    conn_b.execute("BEGIN CONCURRENT").unwrap();
    conn_a.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    conn_b.execute("INSERT INTO t VALUES (2, 1)").unwrap();

    conn_a.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
        CommitYieldPoint::LogicalLogWriteIssued.point(),
    ])));
    conn_b.set_yield_injector(Some(FixedYieldInjector::new([
        CommitYieldPoint::LogRecordPrepared.point(),
        CommitYieldPoint::LogicalLogWriteIssued.point(),
    ])));

    let store = db.get_mvcc_store();
    let lock = &store.commit_coordinator.pager_commit_lock;
    assert!(lock.write(), "hold the commit lock so both enqueue");

    let mut commit_a = conn_a.prepare("COMMIT").unwrap();
    let mut commit_b = conn_b.prepare("COMMIT").unwrap();
    assert!(matches!(
        step_until_yield_or_done(&mut commit_a),
        StepResult::Yield
    ));
    assert!(matches!(
        step_until_yield_or_done(&mut commit_b),
        StepResult::Yield
    ));
    assert!(matches!(
        step_until_yield_or_done(&mut commit_a),
        StepResult::Yield
    ));
    assert!(matches!(
        step_until_yield_or_done(&mut commit_b),
        StepResult::Yield
    ));

    lock.unlock();

    assert!(
        matches!(step_until_yield_or_done(&mut commit_a), StepResult::Yield),
        "leader should yield after issuing log_tx for the waiter"
    );
    assert!(
        store.last_group_commit_size() >= 2,
        "both commits must be in one batch"
    );

    drop(commit_b);
    conn_a.set_yield_injector(None);
    loop {
        match commit_a.step().unwrap() {
            StepResult::Done => break,
            StepResult::IO | StepResult::Yield => continue,
            other => panic!("leader COMMIT ended with {other:?}"),
        }
    }

    let reader = db.connect();
    assert_eq!(
        get_rows(&reader, "SELECT pk FROM t ORDER BY pk"),
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]],
        "waiter dropped after log_tx must still commit"
    );

    db.restart();
    let reader = db.connect();
    assert_eq!(
        get_rows(&reader, "SELECT pk FROM t ORDER BY pk"),
        vec![vec![Value::from_i64(1)], vec![Value::from_i64(2)]],
        "recovery must not replay an aborted waiter"
    );
}
