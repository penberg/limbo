use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::sync::Arc;
use turso_core::{Database, DatabaseOpts, IO, OpenFlags, SqliteDialect, Statement};
use turso_whopper::{IOFaultConfig, SimulatorIO};

fn run_to_done(stmt: &mut Statement, io: &SimulatorIO) {
    loop {
        match stmt.step().expect("step") {
            turso_core::StepResult::Done => return,
            turso_core::StepResult::IO => io.step().expect("io step"),
            _ => {}
        }
    }
}

/// Regression test for MVCC concurrent commit yield-spin deadlock.
///
/// Under round-robin cooperative scheduling, when two BEGIN CONCURRENT
/// transactions commit simultaneously, the VDBE must yield (return
/// StepResult::IO) when pager_commit_lock is held by the other connection.
///
/// Before the fix in core/vdbe/mod.rs, Completion::new_yield() had
/// finished()==true, so the VDBE inner loop retried without ever returning
/// and both commits could starve.
#[test]
fn test_concurrent_commit_no_yield_spin() {
    let io_rng = ChaCha8Rng::seed_from_u64(42);
    let fault_config = IOFaultConfig {
        cosmic_ray_probability: 0.0,
    };
    let io = Arc::new(SimulatorIO::new(false, io_rng, fault_config));

    let db_path = format!("test-yield-spin-{}.db", std::process::id());
    let db = Database::open_file_with_flags(
        io.clone(),
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .expect("open db");

    let setup = db.connect().expect("setup conn");
    setup
        .execute("PRAGMA journal_mode = 'mvcc'")
        .expect("enable mvcc");
    setup
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
        .expect("create table");
    setup.close().expect("close setup");

    let conn1 = db.connect().expect("conn1");
    let conn2 = db.connect().expect("conn2");

    let mut stmt = conn1.prepare("BEGIN CONCURRENT").expect("prepare");
    run_to_done(&mut stmt, &io);
    let mut stmt = conn2.prepare("BEGIN CONCURRENT").expect("prepare");
    run_to_done(&mut stmt, &io);

    let mut stmt = conn1
        .prepare("INSERT INTO t VALUES (1, 'a')")
        .expect("prepare");
    run_to_done(&mut stmt, &io);
    let mut stmt = conn2
        .prepare("INSERT INTO t VALUES (2, 'b')")
        .expect("prepare");
    run_to_done(&mut stmt, &io);

    let mut commit1 = conn1.prepare("COMMIT").expect("prepare commit1");
    let mut commit2 = conn2.prepare("COMMIT").expect("prepare commit2");

    let mut done1 = false;
    let mut done2 = false;
    let max_steps = 10_000;

    for step in 0..max_steps {
        if done1 && done2 {
            break;
        }

        if !done1 {
            match commit1.step().expect("commit1 step") {
                turso_core::StepResult::Done => done1 = true,
                turso_core::StepResult::IO => {}
                _ => {}
            }
        }
        io.step().expect("io step");

        if !done2 {
            match commit2.step().expect("commit2 step") {
                turso_core::StepResult::Done => done2 = true,
                turso_core::StepResult::IO => {}
                _ => {}
            }
        }
        io.step().expect("io step");

        assert!(
            step < max_steps - 1,
            "concurrent commits did not complete within {max_steps} steps: done1={done1}, done2={done2}"
        );
    }

    assert!(done1, "commit1 should have completed");
    assert!(done2, "commit2 should have completed");

    let verify = db.connect().expect("verify conn");
    let mut stmt = verify.prepare("SELECT COUNT(*) FROM t").expect("prepare");
    let mut count = 0i64;
    loop {
        match stmt.step().expect("step") {
            turso_core::StepResult::Row => {
                if let Some(row) = stmt.row() {
                    count = row
                        .get_values()
                        .next()
                        .expect("count value")
                        .as_int()
                        .expect("count int");
                }
            }
            turso_core::StepResult::Done => break,
            turso_core::StepResult::IO => io.step().expect("io"),
            _ => {}
        }
    }
    assert_eq!(count, 2, "both inserts should be visible");
}

/// End-to-end coverage for checkpoints racing suspended statements: with the
/// probe probability at 1.0, every simulation step that leaves a statement
/// suspended mid-execution fires a same-connection checkpoint attempt
/// (PRAGMA wal_checkpoint or the Connection::checkpoint API) and the run
/// fails unless the engine rejects it with StatementsInProgress/TableLocked.
/// On unguarded builds the checkpoint runs and the suspended statement
/// panics on resume, loses its write, or silently returns wrong rows.
#[test]
fn test_checkpoint_probe_rejects_checkpoints_while_statements_are_suspended() {
    use turso_whopper::properties::{IntegrityCheckProperty, Property};
    use turso_whopper::workloads::{
        BeginWorkload, CommitWorkload, InsertWorkload, IntegrityCheckWorkload, RollbackWorkload,
        SelectWorkload, UpdateWorkload, WalCheckpointWorkload, Workload,
    };
    use turso_whopper::{Whopper, WhopperOpts};

    let workloads: Vec<(u32, Box<dyn Workload>)> = vec![
        (30, Box::new(InsertWorkload)),
        (20, Box::new(UpdateWorkload)),
        (15, Box::new(SelectWorkload)),
        (10, Box::new(BeginWorkload)),
        (8, Box::new(CommitWorkload)),
        (4, Box::new(RollbackWorkload)),
        (5, Box::new(IntegrityCheckWorkload)),
        (
            5,
            Box::new(WalCheckpointWorkload {
                allow_passive: true,
            }),
        ),
    ];
    let properties: Vec<Box<dyn Property>> = vec![Box::new(IntegrityCheckProperty)];

    let opts = WhopperOpts {
        seed: Some(0xC0FFEE),
        max_connections: 3,
        max_steps: 5_000,
        workloads,
        properties,
        checkpoint_probe_probability: 1.0,
        ..WhopperOpts::default()
    };
    let mut whopper = Whopper::new(opts).expect("create whopper");
    whopper
        .run()
        .expect("simulation must complete without contract violations");
    assert!(
        whopper.stats.checkpoint_probes > 0,
        "the run never left a statement suspended, so the probe tested nothing"
    );
}

/// While a COMMIT was suspended inside its post-commit auto-checkpoint, a
/// failed statement on the same connection (a rejected checkpoint probe) was
/// dropped. Its teardown decided it was the last active statement — a failed
/// statement leaves the active count on its step error, so the count held
/// only the suspended sibling — and "rolled back" the connection: it reset
/// the pager's in-flight commit and checkpoint state and closed the shared
/// attached write transaction. The resumed COMMIT then started over and
/// panicked releasing a WAL write lock it no longer held.
///
/// The seed replays a CI failure of the btree-rebalance whopper profile; the
/// panic fired around step 14700.
#[test]
fn test_dropped_failed_statement_keeps_suspended_sibling_commit_intact() {
    use rand::Rng;
    use turso_whopper::chaotic_btree::BtreeRebalanceProfile;
    use turso_whopper::chaotic_elle::ChaoticWorkloadProfile;
    use turso_whopper::properties::{IntegrityCheckProperty, Property};
    use turso_whopper::workloads::{IntegrityCheckWorkload, WalCheckpointWorkload, Workload};
    use turso_whopper::{Whopper, WhopperOpts};

    let workloads: Vec<(u32, Box<dyn Workload>)> = vec![
        (20, Box::new(IntegrityCheckWorkload)),
        (
            5,
            Box::new(WalCheckpointWorkload {
                allow_passive: false,
            }),
        ),
    ];
    let properties: Vec<Box<dyn Property>> = vec![Box::new(IntegrityCheckProperty)];
    let chaotic: Vec<(f64, &'static str, Box<dyn ChaoticWorkloadProfile>)> = vec![(
        1.0,
        "btree-rebalance",
        Box::new(BtreeRebalanceProfile::default()),
    )];

    let opts = WhopperOpts::btree_rebalance()
        .with_seed(16427142037514425436)
        .with_max_steps(20_000)
        .with_max_connections(4)
        .with_workloads(workloads)
        .with_properties(properties)
        .with_chaotic_profiles(chaotic)
        .with_allocation_fault_probability(0.0);
    let mut whopper = Whopper::new(opts).expect("create whopper");
    // Mirror main.rs's run_inprocess loop instead of Whopper::run: the CLI
    // burns one rng draw per step on its reopen check, and this seed replays
    // a CLI failure, so the draw must stay in the stream for the schedule to
    // match.
    while !whopper.is_done() {
        let _ = whopper.rng.random_bool(0.0);
        match whopper.step() {
            Ok(_) => {}
            Err(e) => panic!("statement teardown must not clobber a suspended commit: {e}"),
        }
    }
}

/// The FTS workloads must exercise the index, not `fts_match`'s scalar
/// fallback, and a seeded run must replay: segment ids and index
/// incarnations are drawn from the seeded IO, so two runs of one seed end
/// with byte-identical database files. Before the fix the MVCC log of two
/// same-seed runs differed in every `fts2/chunk/<uuid>` path.
#[test]
fn test_fts_workloads_use_the_index_and_replay_with_the_seed() {
    use turso_whopper::operations::Operation;
    use turso_whopper::properties::{
        FtsSelfDifferentialProperty, IntegrityCheckProperty, Property,
    };
    use turso_whopper::workloads::{
        BeginWorkload, CommitWorkload, FtsDeleteWorkload, FtsInsertWorkload, FtsMatchWorkload,
        FtsOptimizeWorkload, FtsUpdateWorkload, RollbackWorkload, Workload, fts_sim_schema,
    };
    use turso_whopper::{Stats, Whopper, WhopperOpts};

    fn run(seed: u64) -> (Stats, Vec<(String, Vec<u8>)>) {
        let workloads: Vec<(u32, Box<dyn Workload>)> = vec![
            (20, Box::new(FtsInsertWorkload)),
            (8, Box::new(FtsUpdateWorkload)),
            (6, Box::new(FtsDeleteWorkload)),
            (12, Box::new(FtsMatchWorkload)),
            (2, Box::new(FtsOptimizeWorkload)),
            (10, Box::new(BeginWorkload)),
            (8, Box::new(CommitWorkload)),
            (3, Box::new(RollbackWorkload)),
        ];
        let properties: Vec<Box<dyn Property>> = vec![
            Box::new(IntegrityCheckProperty),
            Box::new(FtsSelfDifferentialProperty),
        ];
        let opts = WhopperOpts {
            seed: Some(seed),
            max_connections: 3,
            max_steps: 4_000,
            enable_mvcc: true,
            elle_tables: fts_sim_schema(),
            workloads,
            properties,
            ..WhopperOpts::default()
        };
        let mut whopper = Whopper::new(opts).expect("create whopper");
        whopper
            .run()
            .expect("FTS workloads must not violate a property");
        (whopper.stats.clone(), whopper.db_file_bytes())
    }

    let (first_stats, first_files) = run(0xF75);
    assert!(
        first_stats.fts_checks > 0,
        "no FTS differential completed, so the workloads tested nothing"
    );

    let (second_stats, second_files) = run(0xF75);
    assert_eq!(first_stats.fts_checks, second_stats.fts_checks);
    assert!(!first_files.is_empty());
    assert_eq!(first_files.len(), second_files.len());
    for ((name, first), (other_name, second)) in first_files.iter().zip(&second_files) {
        assert_eq!(name, other_name);
        assert!(
            first == second,
            "{name} differs between two runs of one seed: something (FTS segment ids, index \
             incarnations) is not drawn from the seeded IO, so seeds do not replay"
        );
    }

    // The differential's `fts_match` side must be planned through the
    // index method; the scalar fallback would make the comparison vacuous.
    let io = Arc::new(SimulatorIO::new(
        false,
        ChaCha8Rng::seed_from_u64(7),
        IOFaultConfig {
            cosmic_ray_probability: 0.0,
        },
    ));
    let db_path = format!("test-fts-plan-{}.db", std::process::id());
    let db = Database::open_file_with_flags(
        io.clone(),
        &db_path,
        OpenFlags::default(),
        DatabaseOpts::new().with_index_method(true),
        None,
        Arc::new(SqliteDialect),
    )
    .expect("open db");
    let conn = db.connect().expect("connect");
    for (_, sql) in fts_sim_schema() {
        conn.execute(&sql).expect("bootstrap FTS schema");
    }
    let differential = Operation::FtsMatchDifferential {
        token: "alpha".to_string(),
    }
    .sql();
    let mut stmt = conn
        .prepare(format!("EXPLAIN {differential}"))
        .expect("prepare explain");
    let mut opcodes = Vec::new();
    loop {
        match stmt.step().expect("step explain") {
            turso_core::StepResult::Row => {
                let row = stmt.row().expect("explain row");
                opcodes.push(row.get::<&str>(1).expect("opcode column").to_string());
            }
            turso_core::StepResult::IO => io.step().expect("io step"),
            turso_core::StepResult::Done => break,
            other => panic!("unexpected step result {other:?}"),
        }
    }
    assert!(
        opcodes.iter().any(|opcode| opcode == "IndexMethodQuery"),
        "the FTS differential is not planned through the index method: {opcodes:?}"
    );
}

/// Seed 7705886949262960907 with allocation faults off reads k6 as [] after
/// append 19 already committed. Elle reports G-single-item and incompatible-order.
#[test]
fn test_elle_passive_seed_keeps_committed_text_pk_on_unique_lookup() {
    use rand::Rng;
    use std::sync::atomic::AtomicI64;
    use turso_whopper::chaotic_elle::{ChaoticElleProfile, ChaoticWorkloadProfile, ElleModelKind};
    use turso_whopper::properties::ElleHistoryRecorder;
    use turso_whopper::workloads::{
        BeginWorkload, CommitWorkload, ElleAppendWorkload, ElleReadWorkload, RollbackWorkload,
        Workload,
    };
    use turso_whopper::{StepResult, Whopper, WhopperOpts};

    let history_path = std::env::temp_dir().join(format!(
        "elle-passive-unique-miss-{}.edn",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&history_path);
    let elle_counter = Arc::new(AtomicI64::new(1));
    let workloads: Vec<(u32, Box<dyn Workload>)> = vec![
        (
            40,
            Box::new(ElleAppendWorkload::with_counter(elle_counter.clone())),
        ),
        (30, Box::new(ElleReadWorkload)),
        (30, Box::new(BeginWorkload)),
        (15, Box::new(CommitWorkload)),
        (5, Box::new(RollbackWorkload)),
    ];
    let chaotic: Vec<(f64, &'static str, Box<dyn ChaoticWorkloadProfile>)> = vec![(
        0.3,
        "chaotic-elle",
        Box::new(ChaoticElleProfile::new(
            "elle_lists".to_string(),
            ElleModelKind::ListAppend,
            elle_counter,
            true,
        )),
    )];
    let opts = WhopperOpts::fast()
        .with_seed(7705886949262960907)
        .with_max_steps(100_000)
        .with_max_connections(4)
        .with_enable_mvcc(true)
        .with_experimental_mvcc_passive_checkpoint(true)
        .with_mvcc_checkpoint_threshold(Some(1024))
        .with_allocation_fault_probability(0.0)
        .with_elle_tables(vec![(
            "elle_lists".to_string(),
            "CREATE TABLE IF NOT EXISTS elle_lists (key TEXT PRIMARY KEY, vals TEXT DEFAULT '')"
                .to_string(),
        )])
        .with_workloads(workloads)
        .with_properties(vec![Box::new(ElleHistoryRecorder::new(
            history_path.clone(),
        ))])
        .with_chaotic_profiles(chaotic);
    let mut whopper = Whopper::new(opts).expect("create whopper");
    while !whopper.is_done() {
        let _ = whopper.rng.random_bool(0.0);
        match whopper.step() {
            Ok(StepResult::Ok) => {}
            Ok(StepResult::WalSizeLimitExceeded) => break,
            Err(e) => panic!("elle passive seed must keep running: {e}"),
        }
    }
    whopper.finalize_properties().expect("export elle history");
    let history = std::fs::read_to_string(&history_path).expect("read elle history");
    let _ = std::fs::remove_file(&history_path);
    if let Some((key, index, created_at)) = first_empty_read_of_committed_key(&history) {
        panic!("unique lookup missed committed {key} at event {index} (created at {created_at})");
    }
}

fn first_empty_read_of_committed_key(history: &str) -> Option<(String, u64, u64)> {
    let mut created: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    let mut invoke_time: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
    for line in history.lines() {
        let process = edn_field_u64(line, ":process")?;
        let time = edn_field_u64(line, ":time")?;
        let index = edn_field_u64(line, ":index")?;
        if line.contains(":type :invoke") {
            invoke_time.insert(process, time);
            continue;
        }
        if !line.contains(":type :ok") {
            if line.contains(":type :fail") || line.contains(":type :info") {
                invoke_time.remove(&process);
            }
            continue;
        }
        let snapshot = invoke_time.remove(&process).unwrap_or(time);
        let mut rest = line;
        while let Some(at) = rest.find("[:") {
            let slice = &rest[at..];
            if let Some((key, value)) = parse_edn_append(slice) {
                created.entry(key).or_insert(time);
                rest = value;
                continue;
            }
            if let Some((key, empty, next)) = parse_edn_read(slice) {
                if empty {
                    if let Some(&created_at) = created.get(&key) {
                        if snapshot > created_at {
                            return Some((key, index, created_at));
                        }
                    }
                }
                rest = next;
                continue;
            }
            rest = &slice[2..];
        }
    }
    None
}

fn edn_field_u64(line: &str, field: &str) -> Option<u64> {
    let after = line.split_once(field)?.1;
    let digits = after
        .trim_start_matches(|c: char| !c.is_ascii_digit())
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect::<String>();
    digits.parse().ok()
}

fn parse_edn_append(slice: &str) -> Option<(String, &str)> {
    let rest = slice.strip_prefix("[:append \"")?;
    let (key, rest) = rest.split_once('"')?;
    Some((key.to_string(), rest))
}

fn parse_edn_read(slice: &str) -> Option<(String, bool, &str)> {
    let rest = slice.strip_prefix("[:r \"")?;
    let (key, rest) = rest.split_once('"')?;
    let rest = rest.trim_start();
    if rest.starts_with("[]") {
        return Some((key.to_string(), true, rest));
    }
    Some((key.to_string(), false, rest))
}
