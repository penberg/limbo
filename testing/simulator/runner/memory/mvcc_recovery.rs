use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use turso_core::SqliteDialect;

use anyhow::Result;
use turso_core::{Connection, Database, DatabaseOpts, IO, OpenFlags, StepResult};

use crate::runner::SimIO;
use crate::runner::memory::io::MemorySimIO;

fn make_io(seed: u64, latency_probability: u8) -> Arc<MemorySimIO> {
    Arc::new(MemorySimIO::new(seed, 4096, latency_probability, 1, 5))
}

fn open_conn(io: Arc<MemorySimIO>, path: &str) -> Result<Arc<Connection>> {
    let db = Database::open_file_with_flags(
        io as Arc<dyn IO>,
        path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect()?;
    Ok(conn)
}

fn open_two_conns(io: Arc<MemorySimIO>, path: &str) -> Result<(Arc<Connection>, Arc<Connection>)> {
    let db = Database::open_file_with_flags(
        io as Arc<dyn IO>,
        path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )?;
    let conn1 = db.connect()?;
    let conn2 = db.connect()?;
    Ok((conn1, conn2))
}

fn enable_mvcc(conn: &Arc<Connection>) -> Result<()> {
    conn.execute("PRAGMA journal_mode = 'mvcc'")?;
    Ok(())
}

fn query_count(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<i64> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM t")?;
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist for count query");
                let count = row.get::<i64>(0).expect("count column should exist");
                return Ok(count);
            }
            StepResult::Done => panic!("count query ended without a row"),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

fn query_rows(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare("SELECT id, v FROM t ORDER BY id")?;
    let mut rows = Vec::new();
    loop {
        match stmt.step()? {
            StepResult::IO => io.step()?,
            StepResult::Row => {
                let row = stmt.row().expect("row should exist");
                rows.push((
                    row.get::<i64>(0).expect("id column should exist"),
                    row.get::<String>(1).expect("v column should exist"),
                ));
            }
            StepResult::Done => return Ok(rows),
            other => panic!("unexpected step result: {other:?}"),
        }
    }
}

fn set_fault(io: &MemorySimIO, wal: bool, log: bool) {
    io.inject_fault_selective(&[("-wal", wal), (".db-log", log)]);
}

fn clear_faults(io: &MemorySimIO) {
    set_fault(io, false, false);
}

fn find_file_path_by_suffix(io: &MemorySimIO, suffix: &str) -> String {
    io.files
        .borrow()
        .keys()
        .find(|path| path.ends_with(suffix))
        .cloned()
        .unwrap_or_else(|| panic!("expected file with suffix {suffix}"))
}

fn mutate_file_by_suffix(io: &MemorySimIO, suffix: &str, mutator: impl FnOnce(&mut Vec<u8>)) {
    let path = find_file_path_by_suffix(io, suffix);
    let files = io.files.borrow();
    let file = files
        .get(&path)
        .unwrap_or_else(|| panic!("missing file for path {path}"));
    let mut state = file.state.borrow_mut();
    let mut bytes = vec![0; state.buffer.len];
    state.buffer.read(0, &mut bytes);
    mutator(&mut bytes);
    state.resize(bytes.len());
    state.write(0, &bytes);
}

fn remove_file_by_suffix(io: &MemorySimIO, suffix: &str) -> Result<()> {
    let path = find_file_path_by_suffix(io, suffix);
    io.remove_file(&path)?;
    Ok(())
}

fn assert_open_fails_closed(io: Arc<MemorySimIO>, path: &str) {
    let err = match open_conn(io, path) {
        Ok(_) => panic!("open must fail closed"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("Corrupt") || msg.contains("corrupt"),
        "expected Corrupt error, got: {msg}"
    );
}

fn setup_interrupted_checkpoint(conn: &Arc<Connection>, io: &MemorySimIO) -> Result<()> {
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;
    set_fault(io, false, true);
    let checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)");
    clear_faults(io);
    assert!(
        checkpoint.is_err(),
        "expected checkpoint to fail while logical-log faults are injected"
    );
    Ok(())
}

/// What this test checks: A faulted MVCC checkpoint reports an error instead of panicking.
/// Why this matters: A database must fail in a controlled way; panic paths make recovery and debugging much harder.
#[test]
fn sim_mvcc_faulted_explicit_checkpoint_returns_error_without_panic() -> Result<()> {
    let seed = 201;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_no_panic_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    enable_mvcc(&conn)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;
    conn.execute("INSERT INTO t VALUES (2, 'b')")?;

    set_fault(io.as_ref(), true, false);
    let result = catch_unwind(AssertUnwindSafe(|| {
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    }));
    clear_faults(io.as_ref());

    assert!(
        result.is_ok(),
        "checkpoint path must return an error, not panic"
    );
    assert!(
        result.expect("catch_unwind checked above").is_err(),
        "checkpoint should fail under WAL faults"
    );
    Ok(())
}

/// What this test checks: Data committed before a faulted checkpoint is still visible after restart.
/// Why this matters: Checkpoint is cleanup work, not a second commit gate; committed rows must stay committed.
#[test]
fn sim_mvcc_restart_recovers_commit_after_checkpoint_fault() -> Result<()> {
    let seed = 101;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_checkpoint_fault_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    enable_mvcc(&conn)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;
    conn.execute("INSERT INTO t VALUES (2, 'b')")?;

    set_fault(io.as_ref(), true, false);
    let checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)");
    clear_faults(io.as_ref());
    assert!(
        checkpoint.is_err(),
        "expected checkpoint to fail while WAL faults are injected"
    );

    drop(conn);
    let conn = open_conn(io.clone(), &path)?;
    let rows = query_rows(&conn, io.as_ref())?;
    assert_eq!(rows, vec![(1, "a".to_string()), (2, "b".to_string())]);
    Ok(())
}

/// What this test checks: A write that fails during logical-log append does not appear after restart.
/// Why this matters: A failed commit must leave no ghost rows behind.
#[test]
fn sim_mvcc_restart_drops_failed_log_append() -> Result<()> {
    let seed = 102;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_log_fault_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    enable_mvcc(&conn)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;

    set_fault(io.as_ref(), false, true);
    let failed_insert = conn.execute("INSERT INTO t VALUES (2, 'b')");
    clear_faults(io.as_ref());
    assert!(
        failed_insert.is_err(),
        "expected commit to fail while logical-log faults are injected"
    );

    drop(conn);
    let conn = open_conn(io.clone(), &path)?;
    let rows = query_rows(&conn, io.as_ref())?;
    assert_eq!(rows, vec![(1, "a".to_string())]);
    Ok(())
}

/// What this test checks: Running recovery multiple times yields the same stable result.
/// Why this matters: Restart behavior must be repeatable; the first recovery run should not leave hidden one-time state.
#[test]
fn sim_mvcc_restart_recovery_is_idempotent() -> Result<()> {
    let seed = 202;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_restart_idempotent_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    enable_mvcc(&conn)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;
    conn.execute("INSERT INTO t VALUES (2, 'b')")?;

    set_fault(io.as_ref(), false, true);
    let checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)");
    clear_faults(io.as_ref());
    assert!(checkpoint.is_err(), "faulted checkpoint should fail");

    drop(conn);
    let conn = open_conn(io.clone(), &path)?;
    assert_eq!(
        query_rows(&conn, io.as_ref())?,
        vec![(1, "a".to_string()), (2, "b".to_string())]
    );

    drop(conn);
    let conn = open_conn(io.clone(), &path)?;
    assert_eq!(
        query_rows(&conn, io.as_ref())?,
        vec![(1, "a".to_string()), (2, "b".to_string())]
    );
    Ok(())
}

/// What this test checks: Startup fails closed when WAL has committed state but the MVCC log file is missing.
/// Why this matters: In uncertain durability states, refusing to open is safer than guessing and risking data loss.
#[test]
fn sim_mvcc_fail_closed_case_wal_without_log() -> Result<()> {
    let seed = 203;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_fail_closed_missing_log_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    enable_mvcc(&conn)?;
    setup_interrupted_checkpoint(&conn, io.as_ref())?;
    drop(conn);

    remove_file_by_suffix(io.as_ref(), ".db-log")?;
    assert_open_fails_closed(io, &path);
    Ok(())
}

/// What this test checks: Startup fails closed when WAL has committed state and the MVCC log header is corrupted.
/// Why this matters: Corrupt metadata near the recovery boundary must be treated as a hard stop, not best-effort replay.
#[test]
fn sim_mvcc_fail_closed_case_wal_with_corrupt_log_header() -> Result<()> {
    let seed = 204;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_fail_closed_corrupt_log_with_wal_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    enable_mvcc(&conn)?;
    setup_interrupted_checkpoint(&conn, io.as_ref())?;
    drop(conn);

    mutate_file_by_suffix(io.as_ref(), ".db-log", |buf| {
        if buf.is_empty() {
            buf.extend_from_slice(&[0xAA; 8]);
        } else {
            buf[0] ^= 0xFF;
        }
    });
    assert_open_fails_closed(io, &path);
    Ok(())
}

/// What this test checks: Startup fails closed when there is no WAL safety net and the MVCC log header is corrupted.
/// Why this matters: Without WAL to reconcile from, accepting a broken log would risk inventing state.
#[test]
fn sim_mvcc_fail_closed_case_no_wal_with_corrupt_log_header() -> Result<()> {
    let seed = 205;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_fail_closed_corrupt_log_no_wal_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    enable_mvcc(&conn)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    drop(conn);

    mutate_file_by_suffix(io.as_ref(), ".db-log", |buf| {
        buf.clear();
        buf.extend(std::iter::repeat_n(0xAB, 56));
    });
    assert_open_fails_closed(io, &path);
    Ok(())
}

/// What this test checks: Recovery applies a valid logical-log prefix and ignores a corrupted tail frame.
/// Why this matters: A torn tail should not erase good earlier work, and should not partially apply bad data.
#[test]
fn sim_mvcc_recovery_keeps_valid_prefix_drops_corrupt_tail_frame() -> Result<()> {
    let seed = 206;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_prefix_tail_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    enable_mvcc(&conn)?;
    conn.execute("PRAGMA mvcc_checkpoint_threshold = -1")?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;
    conn.execute("INSERT INTO t VALUES (2, 'b')")?;
    conn.execute("INSERT INTO t VALUES (3, 'c')")?;
    drop(conn);

    mutate_file_by_suffix(io.as_ref(), ".db-log", |buf| {
        let last = buf.last_mut().expect("expected non-empty logical log");
        *last ^= 0xFF;
    });

    let conn = open_conn(io.clone(), &path)?;
    assert_eq!(
        query_rows(&conn, io.as_ref())?,
        vec![(1, "a".to_string()), (2, "b".to_string())]
    );
    Ok(())
}

/// What this test checks: A checkpoint can fail under WAL fault injection and then succeed on retry.
/// Why this matters: One transient I/O failure should not leave the database permanently wedged as "busy".
#[test]
fn sim_mvcc_checkpoint_retry_after_wal_fault_succeeds() -> Result<()> {
    let seed = 207;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_retry_wal_fault_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    enable_mvcc(&conn)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;
    conn.execute("INSERT INTO t VALUES (2, 'b')")?;

    set_fault(io.as_ref(), true, false);
    let checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)");
    clear_faults(io.as_ref());
    assert!(
        checkpoint.is_err(),
        "checkpoint should fail under WAL fault"
    );

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    drop(conn);

    let conn = open_conn(io.clone(), &path)?;
    assert_eq!(query_count(&conn, io.as_ref())?, 2);
    Ok(())
}

/// What this test checks: A checkpoint can fail during logical-log cleanup and still succeed on a later retry.
/// Why this matters: Mid-checkpoint failure recovery must leave the system able to make forward progress.
#[test]
fn sim_mvcc_checkpoint_retry_after_log_truncate_fault_succeeds() -> Result<()> {
    let seed = 208;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_retry_log_fault_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    enable_mvcc(&conn)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;
    conn.execute("INSERT INTO t VALUES (2, 'b')")?;

    set_fault(io.as_ref(), false, true);
    let checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)");
    clear_faults(io.as_ref());
    assert!(
        checkpoint.is_err(),
        "checkpoint should fail under logical-log fault"
    );

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    drop(conn);

    let conn = open_conn(io.clone(), &path)?;
    assert_eq!(query_count(&conn, io.as_ref())?, 2);
    Ok(())
}

/// What this test checks: A failed checkpoint on one connection does not block writes on another connection.
/// Why this matters: Error cleanup must release locks promptly, or unrelated work stalls.
#[test]
fn sim_mvcc_faulted_checkpoint_does_not_block_other_connection() -> Result<()> {
    let seed = 209;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_two_conn_lock_cleanup_{seed}.db");

    let (conn1, conn2) = open_two_conns(io.clone(), &path)?;
    enable_mvcc(&conn1)?;
    conn1.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn1.execute("INSERT INTO t VALUES (1, 'a')")?;

    set_fault(io.as_ref(), false, true);
    let checkpoint = conn1.execute("PRAGMA wal_checkpoint(TRUNCATE)");
    clear_faults(io.as_ref());
    assert!(
        checkpoint.is_err(),
        "faulted checkpoint should fail on first connection"
    );

    conn2.execute("INSERT INTO t VALUES (2, 'from_conn2')")?;
    assert_eq!(
        query_rows(&conn2, io.as_ref())?,
        vec![(1, "a".to_string()), (2, "from_conn2".to_string())]
    );
    Ok(())
}

/// Regression protected:
///
/// `DELETE FROM t WHERE id = 1` commits its MVCC transaction and then starts an
/// auto-checkpoint because `mvcc_checkpoint_threshold` is 0. If that checkpoint
/// yields for WAL I/O and the I/O fails, `Program::normal_step` sees the error
/// before `CommitStateMachine::Checkpoint` gets another step. Without explicit
/// cleanup from `Program::abort()` or `ProgramState::reset()`, the checkpoint
/// write lock stays held and a second connection can get `Database is busy` when
/// it tries to insert row 2.
#[test]
fn sim_mvcc_faulted_auto_checkpoint_does_not_block_other_connection() -> Result<()> {
    let seed = 211;
    let io = make_io(seed, 100);
    let temp_dir = tempfile::TempDir::new()?;
    let path = temp_dir
        .path()
        .join(format!("sim_mvcc_auto_checkpoint_lock_cleanup_{seed}.db"));
    let path = path.to_str().expect("temp dir path should be valid UTF-8");

    let (conn1, conn2) = open_two_conns(io.clone(), path)?;
    enable_mvcc(&conn1)?;
    conn1.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn1.execute("INSERT INTO t VALUES (1, 'a')")?;
    conn1.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    // Force the DELETE commit to enter CommitStateMachine::Checkpoint.
    conn1.execute("PRAGMA mvcc_checkpoint_threshold = 0")?;

    // The DELETE commits the MVCC transaction before the auto-checkpoint reports
    // this WAL fault through ProgramState::io_completions.
    set_fault(io.as_ref(), true, false);
    let delete = conn1.execute("DELETE FROM t WHERE id = 1");
    clear_faults(io.as_ref());
    assert!(
        delete.is_err(),
        "auto-checkpoint should fail while WAL faults are injected"
    );

    // This insert takes the checkpoint read lock before starting its MVCC
    // transaction. A leaked checkpoint write lock makes it return Busy.
    conn2.execute("INSERT INTO t VALUES (2, 'from_conn2')")?;
    assert_eq!(
        query_rows(&conn2, io.as_ref())?,
        vec![(2, "from_conn2".to_string())]
    );
    Ok(())
}

/// What this test checks: A faulted `ALTER TABLE ... RENAME` behaves atomically across restart.
/// Why this matters: Schema changes should look all-or-nothing to users; half-renamed tables are corruption by another name.
#[test]
fn sim_mvcc_failed_rename_is_atomic_across_restart() -> Result<()> {
    let seed = 210;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_failed_rename_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    enable_mvcc(&conn)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("CREATE INDEX idx_t_v ON t(v)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;

    set_fault(io.as_ref(), false, true);
    let rename = conn.execute("ALTER TABLE t RENAME TO t2");
    clear_faults(io.as_ref());
    assert!(
        rename.is_err(),
        "rename should fail under logical-log fault"
    );
    drop(conn);

    let conn = open_conn(io.clone(), &path)?;
    let rows = query_rows(&conn, io.as_ref())?;
    assert_eq!(rows, vec![(1, "a".to_string())]);
    assert!(
        conn.prepare("SELECT * FROM t2").is_err(),
        "new table name must not exist after failed rename"
    );
    Ok(())
}

/// What this test checks: A faulted `DROP TABLE` does not partially apply across restart.
/// Why this matters: Destructive DDL must be atomic; either the table is gone, or it is intact.
#[test]
fn sim_mvcc_failed_drop_table_is_atomic_across_restart() -> Result<()> {
    let seed = 211;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_failed_drop_{seed}.db");

    let conn = open_conn(io.clone(), &path)?;
    enable_mvcc(&conn)?;
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn.execute("INSERT INTO t VALUES (1, 'a')")?;

    set_fault(io.as_ref(), false, true);
    let drop_table = conn.execute("DROP TABLE t");
    clear_faults(io.as_ref());
    assert!(
        drop_table.is_err(),
        "drop table should fail under logical-log fault"
    );
    drop(conn);

    let conn = open_conn(io.clone(), &path)?;
    assert_eq!(query_rows(&conn, io.as_ref())?, vec![(1, "a".to_string())]);
    Ok(())
}

/// What this test checks: A `BEGIN CONCURRENT` reader keeps its snapshot stable under latency while another writer commits.
/// Why this matters: Snapshot isolation is a user-facing contract; reads inside one transaction must not drift.
#[test]
fn sim_mvcc_snapshot_isolation_holds_under_latency_and_restart() -> Result<()> {
    let seed = 212;
    let io = make_io(seed, 100);
    let path = format!("sim_mvcc_snapshot_latency_{seed}.db");

    let (conn1, conn2) = open_two_conns(io.clone(), &path)?;
    enable_mvcc(&conn1)?;
    conn1.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;
    conn1.execute("INSERT INTO t VALUES (1, 'a')")?;

    conn1.execute("BEGIN CONCURRENT")?;
    assert_eq!(query_count(&conn1, io.as_ref())?, 1);
    conn2.execute("INSERT INTO t VALUES (2, 'b')")?;
    assert_eq!(
        query_count(&conn1, io.as_ref())?,
        1,
        "reader transaction must preserve its snapshot"
    );
    conn1.execute("COMMIT")?;
    assert_eq!(query_count(&conn1, io.as_ref())?, 2);

    drop(conn1);
    drop(conn2);
    let conn = open_conn(io.clone(), &path)?;
    assert_eq!(query_count(&conn, io.as_ref())?, 2);
    Ok(())
}

/// What this test checks: A seeded mixed workload with random fault toggles runs without panics and remains recoverable.
/// Why this matters: Real bugs hide in weird interleavings; a deterministic stress loop catches edge cases early.
#[test]
fn sim_mvcc_randomized_fault_campaign_no_panics_and_recoverable() -> Result<()> {
    for seed in 300_u64..=305_u64 {
        let io = make_io(seed, 80);
        let path = format!("sim_mvcc_random_campaign_{seed}.db");
        let conn = open_conn(io.clone(), &path)?;
        enable_mvcc(&conn)?;
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")?;

        for step in 0_u64..20_u64 {
            match (seed + step) % 3 {
                0 => set_fault(io.as_ref(), true, false),
                1 => set_fault(io.as_ref(), false, true),
                _ => clear_faults(io.as_ref()),
            }

            let sql = match step % 5 {
                0 => format!("INSERT INTO t VALUES ({}, 'v{}')", step + 1, step + 1),
                1 => format!(
                    "INSERT OR REPLACE INTO t VALUES ({}, 'u{}')",
                    (step % 7) + 1,
                    step + 1
                ),
                2 => "UPDATE t SET v = v WHERE id % 2 = 0".to_string(),
                3 => "PRAGMA wal_checkpoint(TRUNCATE)".to_string(),
                _ => "SELECT COUNT(*) FROM t".to_string(),
            };

            let run = catch_unwind(AssertUnwindSafe(|| conn.execute(&sql)));
            assert!(
                run.is_ok(),
                "seed={seed} step={step} panicked while running: {sql}"
            );
            let _ = run.expect("checked above");
            clear_faults(io.as_ref());
        }

        drop(conn);
        let conn = open_conn(io.clone(), &path)?;
        let count = query_count(&conn, io.as_ref())?;
        assert!(count >= 0, "row count must be non-negative");
    }
    Ok(())
}
