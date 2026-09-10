use crate::SqliteDialect;
use std::collections::{HashMap, HashSet};

use crate::io::{MemoryIO, PlatformIO, IO};
use crate::mvcc::cursor::CursorYieldPoint;
use crate::mvcc::yield_hooks::YieldPointMarker;
use crate::mvcc::yield_points::{YieldInjector, YieldPoint};
use crate::sync::{Arc, Mutex};
#[cfg(any(feature = "fts", feature = "io_memory_yield"))]
use crate::StepResult;
use crate::{Connection, Database, DatabaseOpts, LimboError, OpenFlags, Result, Value};

#[derive(Debug)]
struct FailingPrepareIndexMethod;

#[derive(Debug)]
struct FailingPrepareAttachment {
    table_name: String,
    index_name: String,
}

#[derive(Debug, Default)]
struct FailingPrepareCursor {
    dirty: bool,
}

impl crate::index_method::IndexMethod for FailingPrepareIndexMethod {
    fn attach(
        &self,
        configuration: &crate::index_method::IndexMethodConfiguration,
    ) -> Result<Arc<dyn crate::index_method::IndexMethodAttachment>> {
        Ok(Arc::new(FailingPrepareAttachment {
            table_name: configuration.table_name.clone(),
            index_name: configuration.index_name.clone(),
        }))
    }
}

impl crate::index_method::IndexMethodAttachment for FailingPrepareAttachment {
    fn definition<'a>(&'a self) -> crate::index_method::IndexMethodDefinition<'a> {
        crate::index_method::IndexMethodDefinition {
            method_name: "failing_prepare",
            table_name: &self.table_name,
            index_name: &self.index_name,
            patterns: &[],
            backing_btree: false,
            results_materialized: true,
            mvcc_support: crate::index_method::IndexMethodMvccSupport::ExternalTransactional,
        }
    }

    fn init(&self) -> Result<Box<dyn crate::index_method::IndexMethodCursor>> {
        Ok(Box::new(FailingPrepareCursor::default()))
    }
}

impl crate::index_method::IndexMethodCursor for FailingPrepareCursor {
    fn create(
        &mut self,
        _context: &crate::index_method::IndexMethodContext,
    ) -> crate::types::IOResultOr<()> {
        Ok(crate::IOResult::Done(()))
    }

    fn destroy(
        &mut self,
        _context: &crate::index_method::IndexMethodContext,
    ) -> crate::types::IOResultOr<()> {
        Ok(crate::IOResult::Done(()))
    }

    fn open_read(
        &mut self,
        _context: &crate::index_method::IndexMethodContext,
    ) -> crate::types::IOResultOr<()> {
        Ok(crate::IOResult::Done(()))
    }

    fn open_write(
        &mut self,
        _context: &crate::index_method::IndexMethodContext,
    ) -> crate::types::IOResultOr<()> {
        Ok(crate::IOResult::Done(()))
    }

    fn insert(&mut self, _values: &[crate::vdbe::Register]) -> crate::types::IOResultOr<()> {
        self.dirty = true;
        Ok(crate::IOResult::Done(()))
    }

    fn delete(&mut self, _values: &[crate::vdbe::Register]) -> crate::types::IOResultOr<()> {
        self.dirty = true;
        Ok(crate::IOResult::Done(()))
    }

    fn query_start(&mut self, _values: &[crate::vdbe::Register]) -> crate::types::IOResultOr<bool> {
        Ok(crate::IOResult::Done(false))
    }

    fn query_next(&mut self) -> crate::types::IOResultOr<bool> {
        Ok(crate::IOResult::Done(false))
    }

    fn query_column(&mut self, _idx: usize) -> crate::types::IOResultOr<Value> {
        Err(LimboError::InternalError("failing_prepare has no query rows".to_string()).into())
    }

    fn query_rowid(&mut self) -> crate::types::IOResultOr<Option<i64>> {
        Ok(crate::IOResult::Done(None))
    }

    fn stage_statement_commit(
        &mut self,
        _context: &crate::index_method::IndexMethodContext,
    ) -> crate::types::IOResultOr<()> {
        if self.dirty {
            return Err(LimboError::InternalError(
                "forced index-method preparation failure".to_string(),
            )
            .into());
        }
        Ok(crate::IOResult::Done(()))
    }
}

#[derive(Debug)]
struct FixedYieldInjector {
    remaining: Mutex<HashSet<YieldPoint>>,
}

impl FixedYieldInjector {
    fn new(points: impl IntoIterator<Item = YieldPoint>) -> Arc<Self> {
        Arc::new(Self {
            remaining: Mutex::new(points.into_iter().collect()),
        })
    }
}

impl YieldInjector for FixedYieldInjector {
    fn should_yield(&self, _instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        self.remaining.lock().remove(&point)
    }
}

fn drive_attach(conn: &Arc<Connection>, path: &str, alias: &str) {
    let mut state = crate::connection::AttachDatabaseState::default();
    loop {
        match conn.attach_database(path, alias, &mut state).unwrap() {
            crate::IOResult::Done(()) => return,
            crate::IOResult::IO(io) => io.wait(conn.db.io.as_ref()).unwrap(),
        }
    }
}

fn get_rows(conn: &Arc<Connection>, query: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(query).unwrap();
    let mut rows = Vec::new();
    stmt.run_with_row_callback(|row| {
        let values = row.get_values().cloned().collect::<Vec<_>>();
        rows.push(values);
        Ok(())
    })
    .unwrap();
    rows
}

#[test]
fn fail_rolls_back_base_rows_when_index_method_preparation_fails() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:index-method-failing-prepare",
        OpenFlags::default(),
        DatabaseOpts::new().with_index_method(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    db.builtin_syms.write().index_methods.insert(
        "failing_prepare".to_string(),
        Arc::new(FailingPrepareIndexMethod),
    );
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fail ON docs USING failing_prepare(body)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER fail_second BEFORE INSERT ON docs WHEN NEW.id = 2 BEGIN \
         SELECT RAISE(FAIL, 'stop'); END",
    )
    .unwrap();

    let error = conn
        .execute("INSERT INTO docs VALUES (1, 'first'), (2, 'second')")
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("forced index-method preparation failure"),
        "unexpected error: {error}"
    );
    assert!(get_rows(&conn, "SELECT id FROM docs").is_empty());
}

/// Delegates to `MemoryIO` and counts every `step` / `wait_for_completion`
/// made while the test is inside `Statement::step`: that is the engine
/// pumping I/O synchronously instead of yielding it to the caller.
#[cfg(any(feature = "fts", feature = "io_memory_yield"))]
struct NoPumpInsideStepIo {
    inner: Arc<dyn crate::IO>,
    inside_step: std::sync::atomic::AtomicBool,
    pumps_inside_step: std::sync::atomic::AtomicUsize,
}

#[cfg(any(feature = "fts", feature = "io_memory_yield"))]
impl std::fmt::Debug for NoPumpInsideStepIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoPumpInsideStepIo")
            .field("pumps_inside_step", &self.pumps_inside_step())
            .finish()
    }
}

#[cfg(any(feature = "fts", feature = "io_memory_yield"))]
impl NoPumpInsideStepIo {
    fn new(inner: Arc<dyn crate::IO>) -> Self {
        Self {
            inner,
            inside_step: std::sync::atomic::AtomicBool::new(false),
            pumps_inside_step: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn note_pump(&self) {
        if self.inside_step.load(std::sync::atomic::Ordering::SeqCst) {
            self.pumps_inside_step
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    fn pumps_inside_step(&self) -> usize {
        self.pumps_inside_step
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Run `sql` to completion the way an async host does: every I/O
    /// yield comes back to us and we pump it ourselves. Returns how many
    /// times the statement handed us I/O.
    fn drive(&self, conn: &Arc<Connection>, sql: &str) -> usize {
        let mut stmt = conn.prepare(sql).unwrap();
        let mut io_yields = 0;
        loop {
            self.inside_step
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let result = stmt.step();
            self.inside_step
                .store(false, std::sync::atomic::Ordering::SeqCst);
            match result.unwrap() {
                StepResult::IO => {
                    io_yields += 1;
                    self.inner.step().unwrap();
                }
                StepResult::Yield => {}
                StepResult::Done => return io_yields,
                other => panic!("unexpected result driving {sql}: {other:?}"),
            }
        }
    }
}

#[cfg(feature = "fts")]
impl crate::Clock for NoPumpInsideStepIo {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

#[cfg(any(feature = "fts", feature = "io_memory_yield"))]
impl crate::IO for NoPumpInsideStepIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        direct: bool,
    ) -> crate::Result<Arc<dyn crate::File>> {
        self.inner.open_file(path, flags, direct)
    }

    fn remove_file(&self, path: &str) -> crate::Result<()> {
        self.inner.remove_file(path)
    }

    fn step(&self) -> crate::Result<()> {
        self.note_pump();
        self.inner.step()
    }

    fn wait_for_completion(&self, c: crate::Completion) -> crate::Result<()> {
        self.note_pump();
        self.inner.wait_for_completion(c)
    }

    fn cancel(&self, c: &[crate::Completion]) -> crate::Result<()> {
        self.inner.cancel(c)
    }

    fn drain_completions(&self, completions: &[crate::Completion]) -> crate::Result<()> {
        self.inner.drain_completions(completions)
    }

    fn get_memory_io(&self) -> Arc<MemoryIO> {
        self.inner.get_memory_io()
    }
}

/// The FTS backing store is created and dropped with nested DDL
/// statements. They must be stepped cooperatively: an I/O yield inside
/// them has to reach the caller of `Statement::step`, not be pumped with
/// `io.step()` inside the opcode (which blocks, and is impossible on hosts
/// with no synchronous I/O pump).
#[cfg(feature = "fts")]
#[test]
fn fts_backing_store_ddl_yields_its_io_instead_of_pumping_it() {
    let io = Arc::new(NoPumpInsideStepIo::new(Arc::new(MemoryIO::new())));
    let db = Database::open_file_with_flags(
        io.clone(),
        ":memory:fts-backing-store-ddl",
        OpenFlags::default(),
        DatabaseOpts::new().with_index_method(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();

    // The first cursor `next()` of the whole CREATE INDEX happens inside
    // the nested CREATE TABLE (its schema scan), so this yield fires there.
    let injector = FixedYieldInjector::new([CursorYieldPoint::NextStart.point()]);
    conn.set_yield_injector(Some(injector.clone()));
    io.drive(&conn, "CREATE INDEX docs_fts ON docs USING fts(body)");
    conn.set_yield_injector(None);
    assert!(
        injector.remaining.lock().is_empty(),
        "the injected yield never fired, so nothing was tested"
    );
    assert_eq!(
        io.pumps_inside_step(),
        0,
        "CREATE INDEX pumped I/O synchronously inside Statement::step for its \
         backing-store DDL instead of yielding it"
    );
    conn.execute("INSERT INTO docs VALUES (1, 'cooperative ddl')")
        .unwrap();
    assert_eq!(
        get_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'cooperative')"
        )
        .len(),
        1
    );

    let injector = FixedYieldInjector::new([CursorYieldPoint::NextStart.point()]);
    conn.set_yield_injector(Some(injector.clone()));
    io.drive(&conn, "DROP INDEX docs_fts");
    conn.set_yield_injector(None);
    assert!(injector.remaining.lock().is_empty());
    assert_eq!(
        io.pumps_inside_step(),
        0,
        "DROP INDEX pumped I/O synchronously inside Statement::step for its \
         backing-store DDL instead of yielding it"
    );
    assert!(get_rows(
        &conn,
        "SELECT name FROM sqlite_schema WHERE name LIKE '%fts_dir%'"
    )
    .is_empty());
}

/// Yields exactly once at every visit of every cursor boundary: the first
/// time a `(cursor, point)` pair asks it yields, the re-ask on re-entry
/// proceeds, the next visit yields again. Exercises re-entry at every
/// resumable boundary a statement crosses.
#[cfg(feature = "fts")]
#[derive(Debug, Default)]
struct YieldAtEveryVisit {
    armed: Mutex<HashMap<(u64, YieldPoint), bool>>,
    fired: std::sync::atomic::AtomicUsize,
}

#[cfg(feature = "fts")]
impl YieldInjector for YieldAtEveryVisit {
    fn should_yield(&self, instance_id: u64, _selection_key: u64, point: YieldPoint) -> bool {
        let mut armed = self.armed.lock();
        let slot = armed.entry((instance_id, point)).or_insert(true);
        let fire = *slot;
        *slot = !fire;
        if fire {
            self.fired.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        fire
    }
}

/// The backing-store DDL and everything around it must survive being
/// suspended and resumed at every cursor boundary, with no synchronous
/// I/O pump anywhere inside `Statement::step`.
#[cfg(feature = "fts")]
#[test]
fn fts_backing_store_ddl_survives_a_yield_at_every_cursor_boundary() {
    let io = Arc::new(NoPumpInsideStepIo::new(Arc::new(MemoryIO::new())));
    let db = Database::open_file_with_flags(
        io.clone(),
        ":memory:fts-backing-store-ddl-every-yield",
        OpenFlags::default(),
        DatabaseOpts::new().with_index_method(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    for id in 1..=20 {
        conn.execute(format!(
            "INSERT INTO docs VALUES ({id}, 'row {id} {}')",
            if id % 2 == 0 { "even" } else { "odd" }
        ))
        .unwrap();
    }

    let injector = Arc::new(YieldAtEveryVisit::default());
    conn.set_yield_injector(Some(injector.clone()));
    // Creates the backing store with nested DDL, then populates the index
    // from the 20 rows through the insert hook.
    io.drive(&conn, "CREATE INDEX docs_fts ON docs USING fts(body)");
    let after_create = injector.fired.load(std::sync::atomic::Ordering::SeqCst);
    assert!(
        after_create > 0,
        "no cursor boundary yielded during CREATE INDEX"
    );
    assert_eq!(
        io.pumps_inside_step(),
        0,
        "CREATE INDEX pumped I/O inside Statement::step"
    );

    io.drive(&conn, "INSERT INTO docs VALUES (21, 'row 21 odd')");
    io.drive(&conn, "DROP INDEX docs_fts");
    assert!(
        injector.fired.load(std::sync::atomic::Ordering::SeqCst) > after_create,
        "no cursor boundary yielded after CREATE INDEX"
    );
    assert_eq!(
        io.pumps_inside_step(),
        0,
        "DROP INDEX pumped I/O inside Statement::step"
    );
    io.drive(&conn, "CREATE INDEX docs_fts ON docs USING fts(body)");
    assert_eq!(io.pumps_inside_step(), 0);
    conn.set_yield_injector(None);

    assert_eq!(
        get_rows(
            &conn,
            "SELECT count(*) FROM docs WHERE fts_match(body, 'odd')"
        )[0][0],
        Value::from_i64(11)
    );
    assert_eq!(
        get_rows(
            &conn,
            "SELECT count(*) FROM docs WHERE fts_match(body, 'even')"
        )[0][0],
        Value::from_i64(10)
    );
    assert_eq!(
        get_rows(
            &conn,
            "SELECT count(*) FROM sqlite_schema WHERE name LIKE '%fts_dir_docs_fts%'"
        )[0][0],
        Value::from_i64(2),
        "one backing table and one backing index"
    );
}

/// Same contract on a backend with no synchronous completions:
/// `MemoryYieldIO` finishes every read/write/sync only at `io.step()`, the
/// way a WASM-style host behaves. Any I/O the backing-store DDL performs
/// must surface as `StepResult::IO` from the outer statement — a
/// synchronous pump inside the opcode is the bug. Runs in WAL and MVCC
/// journal modes.
#[cfg(all(feature = "fts", feature = "io_memory_yield"))]
#[test]
fn fts_backing_store_ddl_yields_real_io_on_a_deferred_backend() {
    for mvcc in [false, true] {
        let io = Arc::new(NoPumpInsideStepIo::new(Arc::new(
            crate::MemoryYieldIO::new(),
        )));
        let db = Database::open_file_with_flags(
            io.clone(),
            &format!("fts-ddl-deferred-io-{mvcc}.db"),
            OpenFlags::default(),
            DatabaseOpts::new().with_index_method(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        for id in 1..=20 {
            conn.execute(format!("INSERT INTO docs VALUES ({id}, 'seeded row {id}')"))
                .unwrap();
        }

        let io_yields = io.drive(&conn, "CREATE INDEX docs_fts ON docs USING fts(body)");
        assert!(
            io_yields > 0,
            "the deferred backend surfaced no I/O during CREATE INDEX (mvcc={mvcc})"
        );
        assert_eq!(
            io.pumps_inside_step(),
            0,
            "CREATE INDEX pumped deferred I/O inside Statement::step (mvcc={mvcc})"
        );

        conn.execute("INSERT INTO docs VALUES (21, 'fresh row 21')")
            .unwrap();
        assert_eq!(
            get_rows(
                &conn,
                "SELECT count(*) FROM docs WHERE fts_match(body, 'seeded')"
            )[0][0],
            Value::from_i64(20),
            "mvcc={mvcc}"
        );

        io.drive(&conn, "DROP INDEX docs_fts");
        assert_eq!(
            io.pumps_inside_step(),
            0,
            "DROP INDEX pumped deferred I/O inside Statement::step (mvcc={mvcc})"
        );
        io.drive(&conn, "CREATE INDEX docs_fts ON docs USING fts(body)");
        assert_eq!(io.pumps_inside_step(), 0, "mvcc={mvcc}");
        assert_eq!(
            get_rows(
                &conn,
                "SELECT count(*) FROM docs WHERE fts_match(body, 'fresh')"
            )[0][0],
            Value::from_i64(1),
            "mvcc={mvcc}"
        );
    }
}

/// The toy vector index creates and drops its backing stores through the
/// handle that core owns. As a result, its DDL must give its I/O to the
/// caller, like all other index-method work. It must not run the I/O inside
/// the opcode.
#[cfg(feature = "io_memory_yield")]
#[test]
fn toy_index_backing_store_ddl_yields_real_io_on_a_deferred_backend() {
    for mvcc in [false, true] {
        let io = Arc::new(NoPumpInsideStepIo::new(Arc::new(
            crate::MemoryYieldIO::new(),
        )));
        let db = Database::open_file_with_flags(
            io.clone(),
            &format!("toy-ddl-deferred-io-{mvcc}.db"),
            OpenFlags::default(),
            DatabaseOpts::new().with_index_method(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap();
        let conn = db.connect().unwrap();
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        }
        conn.execute("CREATE TABLE vectors(id INTEGER PRIMARY KEY, embedding)")
            .unwrap();
        for id in 1..=5 {
            conn.execute(format!(
                "INSERT INTO vectors VALUES ({id}, vector32_sparse('[{id}, 0, 1]'))"
            ))
            .unwrap();
        }

        let io_yields = io.drive(
            &conn,
            "CREATE INDEX vectors_idx ON vectors USING toy_vector_sparse_ivf (embedding)",
        );
        assert!(
            io_yields > 0,
            "the deferred backend surfaced no I/O during CREATE INDEX (mvcc={mvcc})"
        );
        assert_eq!(
            io.pumps_inside_step(),
            0,
            "CREATE INDEX pumped deferred I/O inside Statement::step (mvcc={mvcc})"
        );
        assert_eq!(
            get_rows(
                &conn,
                "SELECT id FROM vectors \
                 ORDER BY vector_distance_jaccard(embedding, vector32_sparse('[3, 0, 1]')) \
                 LIMIT 1"
            )[0][0],
            Value::from_i64(3),
            "mvcc={mvcc}"
        );

        io.drive(&conn, "DROP INDEX vectors_idx");
        assert_eq!(
            io.pumps_inside_step(),
            0,
            "DROP INDEX pumped deferred I/O inside Statement::step (mvcc={mvcc})"
        );
        assert_eq!(
            get_rows(
                &conn,
                "SELECT count(*) FROM sqlite_master \
                 WHERE name IN ('vectors_idx_inverted_index', 'vectors_idx_stats')"
            )[0][0],
            Value::from_i64(0),
            "mvcc={mvcc}"
        );
    }
}

#[cfg(feature = "fts")]
#[test]
fn abandoning_after_index_method_prepare_rolls_back_without_drop_io() {
    use crate::index_method::IndexMethodYieldPoint;

    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        ":memory:index-method-finalize-abandon",
        OpenFlags::default(),
        DatabaseOpts::new().with_index_method(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        IndexMethodYieldPoint::AfterPrepareStatement.point(),
    ])));
    let mut insert = conn
        .prepare("INSERT INTO docs VALUES (1, 'abandoned after prepare')")
        .unwrap();
    loop {
        match insert.step().unwrap() {
            StepResult::IO => io.step().unwrap(),
            StepResult::Yield => break,
            StepResult::Done => panic!("INSERT completed before the injected finalization yield"),
            other => panic!("unexpected INSERT result: {other:?}"),
        }
    }
    drop(insert);
    conn.set_yield_injector(None);

    assert!(get_rows(&conn, "SELECT id FROM docs").is_empty());
    assert!(get_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(body, 'abandoned')"
    )
    .is_empty());

    conn.execute("INSERT INTO docs VALUES (2, 'surviving retry')")
        .unwrap();
    assert_eq!(
        get_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'surviving')"
        ),
        vec![vec![Value::from_i64(2)]]
    );
}

fn open_mvcc_database_with_opts(path: &str, opts: DatabaseOpts) -> Arc<Database> {
    let io: Arc<dyn IO> = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file_with_flags(
        io,
        path,
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.close().unwrap();
    db
}

struct SameConnectionMvcc {
    conn: Arc<Connection>,
    observer: Arc<Connection>,
}

struct SameConnectionWal {
    conn: Arc<Connection>,
    observer: Arc<Connection>,
}

impl SameConnectionWal {
    fn new(path: &str) -> Self {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, path, Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        let observer = db.connect().unwrap();
        Self { conn, observer }
    }

    fn setup_rows_table(&self) {
        self.conn
            .execute("CREATE TABLE rows(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
    }
}

impl SameConnectionMvcc {
    fn new(path: &str) -> Self {
        let io = Arc::new(MemoryIO::new());
        let db = Database::open_file(io, path, Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
        let observer = db.connect().unwrap();
        Self { conn, observer }
    }

    fn setup_rows_table(&self) {
        self.conn
            .execute("CREATE TABLE rows(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
    }

    fn setup_rows_and_source_tables(&self) {
        self.setup_rows_table();
        self.conn
            .execute("CREATE TABLE src(id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        self.conn
            .execute("INSERT INTO src VALUES (2, 'src-two')")
            .unwrap();
        self.conn
            .execute("PRAGMA wal_checkpoint(TRUNCATE)")
            .unwrap();
    }

    fn observer_ids(&self) -> Vec<i64> {
        ids_from_query(&self.observer, "SELECT id FROM rows ORDER BY id")
    }

    fn observer_count(&self) -> i64 {
        scalar_i64(&self.observer, "SELECT COUNT(*) FROM rows")
    }

    fn observer_value_for_id(&self, id: i64) -> String {
        scalar_text(
            &self.observer,
            &format!("SELECT v FROM rows WHERE id = {id}"),
        )
    }
}

fn scalar_i64(conn: &Arc<Connection>, sql: &str) -> i64 {
    let rows = get_rows(conn, sql);
    assert_eq!(rows.len(), 1, "expected one row for {sql}, got {rows:?}");
    rows[0][0]
        .as_int()
        .unwrap_or_else(|| panic!("expected integer scalar for {sql}, got {:?}", rows[0][0]))
}

fn scalar_text(conn: &Arc<Connection>, sql: &str) -> String {
    let rows = get_rows(conn, sql);
    assert_eq!(rows.len(), 1, "expected one row for {sql}, got {rows:?}");
    rows[0][0].to_string()
}

fn ids_from_query(conn: &Arc<Connection>, sql: &str) -> Vec<i64> {
    get_rows(conn, sql)
        .into_iter()
        .map(|row| {
            row[0]
                .as_int()
                .unwrap_or_else(|| panic!("expected integer id for {sql}, got {:?}", row[0]))
        })
        .collect()
}

fn prepare_insert_returning(conn: &Arc<Connection>, id: i64, value: &str) -> crate::Statement {
    conn.prepare(format!(
        "INSERT INTO rows VALUES ({id}, '{value}') RETURNING id"
    ))
    .unwrap()
}

fn step_returning_id(stmt: &mut crate::Statement) -> i64 {
    match stmt.step().unwrap() {
        crate::StepResult::Row => stmt.row().unwrap().get::<i64>(0).unwrap(),
        other => panic!("expected RETURNING row, got {other:?}"),
    }
}

/// Same-connection conflicts surface as `Err(StatementsInProgress)` — a
/// BUSY-class error that aborts the statement instead of the retryable
/// `StepResult::Busy`, mirroring SQLite's "SQL statements in progress"
/// rejections (error-class SQLITE_BUSY that never invokes the busy handler).
fn expect_step_busy(stmt: &mut crate::Statement, context: &str) {
    match stmt.step() {
        Err(LimboError::StatementsInProgress(_)) => {}
        Ok(result) => panic!("expected StatementsInProgress for {context}, got {result:?}"),
        Err(err) => panic!("expected StatementsInProgress for {context}, got {err:?}"),
    }
}

fn expect_busy(result: Result<()>, context: &str) {
    let err = result.expect_err(context);
    assert!(
        matches!(err, LimboError::StatementsInProgress(_)),
        "expected StatementsInProgress for {context}, got {err:?}"
    );
}

fn expect_unfinished_write_commit_error(result: Result<()>) {
    let err = result.expect_err("COMMIT should reject an abandoned unfinished write");
    assert!(
        matches!(&err, LimboError::TxError(message) if message.contains("unfinished write statement was abandoned")),
        "expected unfinished-write TxError, got {err:?}"
    );
}

/// Step until the injected yield suspends the statement mid-execution,
/// driving any genuine I/O encountered before the injection point. Injected
/// yields surface as `StepResult::Yield` (explicit yields are not stored as
/// pending I/O), so the statement stays parked at the same PC afterwards.
fn expect_injected_yield(stmt: &mut crate::Statement, context: &str) {
    loop {
        match stmt.step().unwrap() {
            crate::StepResult::Yield => return,
            crate::StepResult::IO => stmt.get_pager().io.step().unwrap(),
            other => panic!("expected injected yield during {context}, got {other:?}"),
        }
    }
}

fn finish_without_rows(stmt: &mut crate::Statement) {
    loop {
        match stmt.step().unwrap() {
            crate::StepResult::Done => return,
            crate::StepResult::IO => stmt.get_pager().io.step().unwrap(),
            crate::StepResult::Row => panic!("expected statement to finish without more rows"),
            other => panic!("expected statement to finish, got {other:?}"),
        }
    }
}

fn drain_returning_ids(stmt: &mut crate::Statement) -> Vec<i64> {
    let mut ids = Vec::new();
    loop {
        match stmt.step().unwrap() {
            crate::StepResult::Done => return ids,
            crate::StepResult::IO => stmt.get_pager().io.step().unwrap(),
            crate::StepResult::Row => ids.push(stmt.row().unwrap().get::<i64>(0).unwrap()),
            other => panic!("expected RETURNING rows or Done, got {other:?}"),
        }
    }
}

fn prepare_yielding_insert_select(env: &SameConnectionMvcc) -> crate::Statement {
    prepare_yielding_insert_select_sql(env, "INSERT INTO rows SELECT id, v FROM src")
}

fn prepare_yielding_insert_select_sql(env: &SameConnectionMvcc, sql: &str) -> crate::Statement {
    prepare_yielding_statement(&env.conn, sql, CursorYieldPoint::NextStart, "INSERT SELECT")
}

/// Prepare a statement, force it to suspend at one cursor yield point, then
/// disable injection so later steps exercise normal resume/cleanup behavior.
fn prepare_yielding_statement(
    conn: &Arc<Connection>,
    sql: impl AsRef<str>,
    yield_point: CursorYieldPoint,
    context: &str,
) -> crate::Statement {
    conn.set_yield_injector(Some(FixedYieldInjector::new([yield_point.point()])));
    let mut stmt = conn.prepare(sql).unwrap();
    expect_injected_yield(&mut stmt, context);
    conn.set_yield_injector(None);
    stmt
}

fn prepare_yielding_update_all_rows(conn: &Arc<Connection>, value: &str) -> crate::Statement {
    prepare_yielding_statement(
        conn,
        format!("UPDATE rows SET v = '{value}'"),
        CursorYieldPoint::NextStart,
        "UPDATE all rows",
    )
}

fn prepare_wal_update_yielding_on_table_read(
    env: &SameConnectionWal,
    value: &str,
) -> crate::Statement {
    let root_page = scalar_i64(
        &env.conn,
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'rows'",
    );
    env.conn.get_pager().arm_spill_yield_on_read(root_page, 0);
    let mut stmt = env
        .conn
        .prepare(format!("UPDATE rows SET v = '{value}'"))
        .unwrap();
    expect_injected_yield(&mut stmt, "WAL UPDATE table read");
    stmt
}

#[test]
fn test_returning_owner_drop_does_not_commit_interrupted_drop_table() {
    let io = Arc::new(MemoryIO::new());
    let path = ":memory:returning-owner-interrupted-drop-table";
    let db = Database::open_file(io.clone(), path, Arc::new(SqliteDialect)).unwrap();
    let conn = db.connect().unwrap();

    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    conn.execute(
        "CREATE TABLE core(\
            id INTEGER PRIMARY KEY, \
            row_number INTEGER, \
            deletion_timestamp INTEGER\
        )",
    )
    .unwrap();
    conn.execute("CREATE TABLE other(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.execute(
        "CREATE UNIQUE INDEX core_active_row_number_index \
         ON core (row_number) WHERE deletion_timestamp IS NULL",
    )
    .unwrap();

    let before = get_rows(
        &conn,
        "SELECT type, name FROM sqlite_schema \
         WHERE tbl_name = 'core' ORDER BY rowid",
    );
    assert_eq!(before.len(), 2);
    assert_eq!(before[0][0].to_string(), "table");
    assert_eq!(before[0][1].to_string(), "core");
    assert_eq!(before[1][0].to_string(), "index");
    assert_eq!(before[1][1].to_string(), "core_active_row_number_index");

    let mut returning_owner = conn
        .prepare("INSERT INTO other VALUES (1) RETURNING id")
        .unwrap();
    match returning_owner.step().unwrap() {
        crate::StepResult::Row => {
            let row = returning_owner.row().unwrap();
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
        }
        other => panic!("expected INSERT RETURNING to yield its row; got {other:?}"),
    }

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        CursorYieldPoint::NextStart.point()
    ])));
    let mut drop_stmt = conn.prepare("DROP TABLE core").unwrap();
    expect_step_busy(&mut drop_stmt, "DROP TABLE while RETURNING is active");
    conn.set_yield_injector(None);

    drop(returning_owner);
    drop(drop_stmt);
    drop(conn);
    drop(db);

    let db = Database::open_file(io, path, Arc::new(SqliteDialect)).expect(
        "reopen should not fail; dropping a RETURNING owner must not commit another statement's interrupted DROP",
    );
    let conn = db.connect().unwrap();
    let after = get_rows(
        &conn,
        "SELECT type, name FROM sqlite_schema \
         WHERE tbl_name = 'core' ORDER BY rowid",
    );
    assert_eq!(after.len(), 2, "schema must not be half-dropped: {after:?}");
    assert_eq!(after[0][0].to_string(), "table");
    assert_eq!(after[0][1].to_string(), "core");
    assert_eq!(after[1][0].to_string(), "index");
    assert_eq!(after[1][1].to_string(), "core_active_row_number_index");
}

#[test]
fn test_drop_table_while_returning_active_is_table_locked() {
    let env = SameConnectionMvcc::new(":memory:drop-table-returning-active-locked");
    env.conn
        .execute("CREATE TABLE core(id INTEGER PRIMARY KEY)")
        .unwrap();
    env.conn
        .execute("CREATE TABLE other(id INTEGER PRIMARY KEY)")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut returning = env
        .conn
        .prepare("INSERT INTO other VALUES (1), (2) RETURNING id")
        .unwrap();
    assert_eq!(step_returning_id(&mut returning), 1);

    let mut dropper = env.conn.prepare("DROP TABLE core").unwrap();
    expect_step_busy(&mut dropper, "DROP TABLE while RETURNING is active");
    drop(dropper);

    assert_eq!(
        scalar_i64(
            &env.observer,
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'core'"
        ),
        1,
        "failed DROP must leave the table schema intact"
    );
    assert_eq!(
        scalar_i64(&env.observer, "SELECT COUNT(*) FROM other"),
        0,
        "failed DROP must not commit the active RETURNING writer"
    );

    assert_eq!(step_returning_id(&mut returning), 2);
    finish_without_rows(&mut returning);
    assert_eq!(scalar_i64(&env.observer, "SELECT COUNT(*) FROM other"), 2);
}

#[test]
fn test_second_returning_writer_is_busy_and_first_can_commit_after_second_drop() {
    let env = SameConnectionMvcc::new(":memory:multi-returning-first-then-second");
    env.setup_rows_table();

    let mut first = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut first), 1);
    let mut second = prepare_insert_returning(&env.conn, 2, "two");
    expect_step_busy(&mut second, "second RETURNING writer");

    assert_eq!(env.observer_ids(), Vec::<i64>::new());
    drop(second);
    drop(first);
    assert_eq!(env.observer_ids(), vec![1]);
}

#[test]
fn test_second_returning_writer_is_busy_and_first_can_commit_after_first_drop() {
    let env = SameConnectionMvcc::new(":memory:multi-returning-second-then-first");
    env.setup_rows_table();

    let mut first = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut first), 1);
    let mut second = prepare_insert_returning(&env.conn, 2, "two");
    expect_step_busy(&mut second, "second RETURNING writer");

    drop(second);
    assert_eq!(env.observer_ids(), Vec::<i64>::new());
    drop(first);
    assert_eq!(env.observer_ids(), vec![1]);
}

#[test]
fn test_busy_second_returning_writer_does_not_block_first_reset_commit() {
    let env = SameConnectionMvcc::new(":memory:returning-reset-defers");
    env.setup_rows_table();

    let mut first = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut first), 1);
    let mut second = prepare_insert_returning(&env.conn, 2, "two");
    expect_step_busy(&mut second, "second RETURNING writer");

    drop(second);
    first.reset().unwrap();
    assert_eq!(env.observer_ids(), vec![1]);
}

#[test]
fn test_plain_insert_done_does_not_commit_while_returning_writer_active() {
    let env = SameConnectionMvcc::new(":memory:plain-insert-waits-for-returning");
    env.setup_rows_table();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    expect_busy(
        env.conn.execute("INSERT INTO rows VALUES (2, 'plain-two')"),
        "plain INSERT while RETURNING is active",
    );

    assert_eq!(
        env.observer_ids(),
        Vec::<i64>::new(),
        "completed plain INSERT must not commit the shared tx while RETURNING is active"
    );
    drop(returning);
    assert_eq!(env.observer_ids(), vec![1]);
}

#[test]
fn test_same_connection_select_does_not_commit_active_returning_writer() {
    let env = SameConnectionMvcc::new(":memory:select-does-not-commit-returning");
    env.setup_rows_table();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);

    assert_eq!(
        scalar_i64(&env.conn, "SELECT COUNT(*) FROM rows"),
        1,
        "the owning connection sees its uncommitted write"
    );
    assert_eq!(
        env.observer_count(),
        0,
        "a read statement must not finalize the active implicit write tx"
    );
    drop(returning);
    assert_eq!(env.observer_ids(), vec![1]);
}

#[test]
fn test_suspended_read_does_not_finish_joined_returning_writer() {
    let env = SameConnectionMvcc::new(":memory:suspended-read-joined-writer");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'existing')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut reader = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 10);
    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);

    drop(reader);
    assert_eq!(
        env.observer_ids(),
        vec![10],
        "dropping the read that opened the transaction must not finish the joined writer"
    );
    drop(returning);
    assert_eq!(env.observer_ids(), vec![1, 10]);
}

#[test]
fn test_completed_writer_waits_for_sibling_mvcc_reader_to_commit() {
    let env = SameConnectionMvcc::new(":memory:completed-writer-waits-for-reader");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'existing')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut reader = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 10);

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    finish_without_rows(&mut returning);
    drop(returning);

    assert_eq!(
        env.observer_ids(),
        vec![10],
        "completed writer must not commit while the reader still holds the shared MVCC transaction"
    );

    finish_without_rows(&mut reader);
    assert_eq!(
        env.observer_ids(),
        vec![1, 10],
        "the last sibling statement should commit the completed writer's changes"
    );
}

#[test]
fn test_suspended_read_does_not_finish_sibling_read_transaction() {
    let env = SameConnectionMvcc::new(":memory:suspended-read-sibling-read");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten'), (20, 'twenty')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut first = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut first), 10);
    let mut second = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut second), 10);

    drop(first);
    assert_eq!(
        step_returning_id(&mut second),
        20,
        "dropping one read must not close the transaction under a sibling read"
    );
    finish_without_rows(&mut second);
    assert_eq!(env.conn.get_mv_tx(), None);
    assert_eq!(
        env.conn.get_tx_state(),
        crate::connection::TransactionState::None
    );
}

#[test]
fn test_wal_returning_writer_can_commit_while_sibling_reader_is_active() {
    let env = SameConnectionWal::new(":memory:wal-reader-active-writer-commit");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut reader = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 10);

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    finish_without_rows(&mut returning);

    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![1, 10],
        "WAL mode follows SQLite's only-active-writer rule: the writer can commit while a reader is still active"
    );
    finish_without_rows(&mut reader);
}

#[test]
fn test_wal_begin_immediate_does_not_count_as_active_writer() {
    let env = SameConnectionWal::new(":memory:wal-begin-immediate-not-active-writer");
    env.setup_rows_table();

    let mut begin = env.conn.prepare("BEGIN IMMEDIATE").unwrap();
    finish_without_rows(&mut begin);

    let mut insert = env
        .conn
        .prepare("INSERT INTO rows VALUES (1, 'one')")
        .unwrap();
    finish_without_rows(&mut insert);
    assert_eq!(
        scalar_i64(&env.observer, "SELECT COUNT(*) FROM rows"),
        0,
        "BEGIN IMMEDIATE opens the transaction, but the INSERT is still uncommitted"
    );

    env.conn.execute("COMMIT").unwrap();
    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![1]
    );
}

#[test]
fn test_wal_dropping_reader_does_not_rollback_active_returning_writer() {
    let env = SameConnectionWal::new(":memory:wal-drop-reader-active-writer");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut reader = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 10);
    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);

    drop(reader);
    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![10],
        "dropping the reader must not commit the active writer"
    );

    finish_without_rows(&mut returning);
    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![1, 10]
    );
}

#[test]
fn test_wal_dropping_one_reader_does_not_close_sibling_reader_transaction() {
    let env = SameConnectionWal::new(":memory:wal-sibling-readers");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten'), (20, 'twenty')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut first = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut first), 10);
    let mut second = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut second), 10);

    drop(first);
    assert_eq!(
        step_returning_id(&mut second),
        20,
        "dropping one reader must not close the transaction under a sibling reader"
    );
    finish_without_rows(&mut second);
}

#[test]
fn test_wal_dropping_one_attached_reader_keeps_sibling_reader_locked() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:wal-attached-sibling-readers",
        OpenFlags::default(),
        DatabaseOpts::new().with_attach(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    drive_attach(&conn, ":memory:wal-attached-sibling-readers-aux", "aux");
    conn.execute("CREATE TABLE aux.rows(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO aux.rows VALUES (10), (20)")
        .unwrap();

    let mut first = conn.prepare("SELECT id FROM aux.rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut first), 10);
    let mut second = conn.prepare("SELECT id FROM aux.rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut second), 10);

    drop(first);
    let detach_err = conn
        .execute("DETACH aux")
        .expect_err("the sibling reader must keep the attached database locked");
    assert!(
        matches!(detach_err, LimboError::InvalidArgument(ref message) if message == "database aux is locked"),
        "expected attached database lock error, got {detach_err:?}"
    );
    assert_eq!(
        step_returning_id(&mut second),
        20,
        "dropping one attached reader must not close the transaction under its sibling"
    );
    finish_without_rows(&mut second);
    conn.execute("DETACH aux").unwrap();
}

#[test]
fn review_attached_writer_success_is_not_lost_when_sibling_dropped() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:review-attached-writer-main",
        OpenFlags::default(),
        DatabaseOpts::new().with_attach(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    let aux = ":memory:review-attached-writer-aux";
    drive_attach(&conn, aux, "aux");
    conn.execute("CREATE TABLE rows(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO rows VALUES (1), (2)").unwrap();
    conn.execute("CREATE TABLE aux.rows(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO aux.rows VALUES (10)").unwrap();

    let mut reader = conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 1);
    conn.execute("UPDATE aux.rows SET id = 11").unwrap();
    drop(reader);

    assert_eq!(ids_from_query(&conn, "SELECT id FROM aux.rows"), vec![11]);
}

#[test]
fn review_transactionless_last_sibling_closes_attached_txn() {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        ":memory:review-attached-transactionless-main",
        OpenFlags::default(),
        DatabaseOpts::new().with_attach(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    let aux = ":memory:review-attached-transactionless-aux";
    drive_attach(&conn, aux, "aux");
    conn.execute("CREATE TABLE aux.rows(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("INSERT INTO aux.rows VALUES (10), (20)")
        .unwrap();

    let mut literal = conn.prepare("SELECT 1").unwrap();
    assert_eq!(step_returning_id(&mut literal), 1);
    let mut attached = conn.prepare("SELECT id FROM aux.rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut attached), 10);
    assert_eq!(drain_returning_ids(&mut attached), vec![20]);
    finish_without_rows(&mut literal);

    conn.execute("DETACH aux").unwrap();
}

#[test]
fn test_insert_select_is_busy_while_returning_writer_is_active() {
    let env = SameConnectionMvcc::new(":memory:suspended-insert-select-resume");
    env.setup_rows_and_source_tables();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    let mut insert_select = env
        .conn
        .prepare("INSERT INTO rows SELECT id, v FROM src")
        .unwrap();
    expect_step_busy(
        &mut insert_select,
        "INSERT SELECT while RETURNING is active",
    );

    drop(insert_select);
    drop(returning);
    assert_eq!(env.observer_ids(), vec![1]);
}

#[test]
fn test_rejected_insert_select_does_not_roll_back_active_returning_writer() {
    let env = SameConnectionMvcc::new(":memory:suspended-insert-select-abandon");
    env.setup_rows_and_source_tables();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    let mut insert_select = env
        .conn
        .prepare("INSERT INTO rows SELECT id, v FROM src")
        .unwrap();
    expect_step_busy(
        &mut insert_select,
        "INSERT SELECT while RETURNING is active",
    );

    drop(insert_select);
    drop(returning);
    assert_eq!(
        env.observer_ids(),
        vec![1],
        "rejected sibling writer must not roll back the active RETURNING writer"
    );
}

#[test]
fn test_first_writer_without_statement_savepoint_abandon_rolls_back_joined_writer() {
    let env = SameConnectionMvcc::new(":memory:first-writer-without-savepoint-abandon");
    env.setup_rows_and_source_tables();

    let insert_select = prepare_yielding_insert_select(&env);
    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    expect_step_busy(
        &mut returning,
        "RETURNING writer while INSERT SELECT is active",
    );

    drop(insert_select);
    drop(returning);

    assert_eq!(
        env.observer_ids(),
        Vec::<i64>::new(),
        "abandoning a non-final writer without a local rollback boundary must roll back the shared tx"
    );
}

#[test]
fn test_first_writer_without_statement_savepoint_resume_last_commits_joined_writer() {
    let env = SameConnectionMvcc::new(":memory:first-writer-without-savepoint-resume");
    env.setup_rows_and_source_tables();

    let mut insert_select = prepare_yielding_insert_select(&env);
    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    expect_step_busy(
        &mut returning,
        "RETURNING writer while INSERT SELECT is active",
    );

    drop(returning);
    finish_without_rows(&mut insert_select);

    assert_eq!(env.observer_ids(), vec![2]);
}

#[test]
fn test_first_writer_without_statement_savepoint_error_rolls_back_joined_writer() {
    let env = SameConnectionMvcc::new(":memory:first-writer-without-savepoint-error");
    env.setup_rows_table();
    env.conn
        .execute("CREATE TABLE src(id INTEGER, v TEXT)")
        .unwrap();
    env.conn
        .execute("INSERT INTO rows VALUES (1, 'existing')")
        .unwrap();
    env.conn
        .execute("INSERT INTO src VALUES (2, 'src-two'), (1, 'duplicate')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut insert_select =
        prepare_yielding_insert_select_sql(&env, "INSERT INTO rows SELECT id, v FROM src");
    let mut returning = prepare_insert_returning(&env.conn, 3, "joined");
    expect_step_busy(
        &mut returning,
        "RETURNING writer while INSERT SELECT is active",
    );

    let err = insert_select
        .step()
        .expect_err("resumed INSERT SELECT should hit duplicate primary key");
    assert!(
        matches!(err, LimboError::Constraint(_)),
        "expected duplicate-key constraint error, got {err:?}"
    );
    drop(insert_select);
    drop(returning);

    assert_eq!(
        env.observer_ids(),
        vec![1],
        "an error from a writer without a local rollback boundary must roll back the whole shared tx"
    );
}

#[test]
fn test_attached_only_returning_writer_defers_shared_auto_txn_commit() {
    let main_dir = tempfile::TempDir::new().unwrap();
    let main_path = main_dir.path().join("main.db");
    let db = open_mvcc_database_with_opts(
        main_path.to_str().unwrap(),
        DatabaseOpts::new().with_attach(true),
    );
    let aux_dir = tempfile::TempDir::new().unwrap();
    let aux_path = aux_dir.path().join("aux.db");
    let aux_path = aux_path.to_str().unwrap();

    let conn = db.connect().unwrap();
    drive_attach(&conn, aux_path, "aux");
    conn.execute("CREATE TABLE aux.rows(id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();

    let observer = db.connect().unwrap();
    drive_attach(&observer, aux_path, "aux");

    let mut returning = conn
        .prepare("INSERT INTO aux.rows VALUES (1, 'one'), (2, 'two') RETURNING id")
        .unwrap();
    assert_eq!(step_returning_id(&mut returning), 1);

    expect_busy(
        conn.execute("INSERT INTO aux.rows VALUES (3, 'plain')"),
        "attached INSERT while attached RETURNING is active",
    );
    assert_eq!(
        ids_from_query(&observer, "SELECT id FROM aux.rows ORDER BY id"),
        Vec::<i64>::new(),
        "rejected attached writer must not commit the active RETURNING writer"
    );

    assert_eq!(drain_returning_ids(&mut returning), vec![2]);
    assert_eq!(
        ids_from_query(&observer, "SELECT id FROM aux.rows ORDER BY id"),
        vec![1, 2]
    );
}

#[test]
fn test_mvcc_explicit_tx_unfinished_writer_poisons_transaction() {
    let env = SameConnectionMvcc::new(":memory:explicit-unfinished-writer-poison");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten'), (20, 'twenty')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    env.conn.execute("BEGIN").unwrap();

    let older = prepare_yielding_update_all_rows(&env.conn, "older");
    let mut newer = env
        .conn
        .prepare("UPDATE rows SET v = 'newer' RETURNING id")
        .unwrap();
    expect_step_busy(&mut newer, "second UPDATE writer in explicit transaction");

    drop(newer);
    drop(older);
    expect_unfinished_write_commit_error(env.conn.execute("COMMIT"));

    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![10, 20]
    );
    assert_eq!(env.observer_value_for_id(10), "ten");
    assert_eq!(env.observer_value_for_id(20), "twenty");

    env.conn
        .execute("INSERT INTO rows VALUES (30, 'thirty')")
        .unwrap();
    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![10, 20, 30]
    );
}

#[test]
fn test_wal_explicit_tx_unfinished_writer_poisons_transaction() {
    let env = SameConnectionWal::new(":memory:wal-explicit-unfinished-writer-poison");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten'), (20, 'twenty')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    env.conn.execute("BEGIN").unwrap();

    let writer = prepare_wal_update_yielding_on_table_read(&env, "partial");
    drop(writer);
    expect_unfinished_write_commit_error(env.conn.execute("COMMIT"));

    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![10, 20]
    );
    assert_eq!(
        scalar_text(&env.observer, "SELECT v FROM rows WHERE id = 10"),
        "ten"
    );
    assert_eq!(
        scalar_text(&env.observer, "SELECT v FROM rows WHERE id = 20"),
        "twenty"
    );
}

#[test]
fn test_explicit_tx_rollback_clears_unfinished_writer_poison() {
    let env = SameConnectionMvcc::new(":memory:explicit-unfinished-writer-rollback-clear");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten'), (20, 'twenty')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    env.conn.execute("BEGIN").unwrap();

    let writer = prepare_yielding_update_all_rows(&env.conn, "partial");
    drop(writer);
    env.conn.execute("ROLLBACK").unwrap();

    env.conn
        .execute("INSERT INTO rows VALUES (30, 'thirty')")
        .unwrap();
    assert_eq!(
        ids_from_query(&env.observer, "SELECT id FROM rows ORDER BY id"),
        vec![10, 20, 30]
    );
}

#[test]
fn test_unfinished_drop_abandon_first_rolls_back_only_drop() {
    let env = SameConnectionMvcc::new(":memory:unfinished-drop-first");
    env.conn
        .execute("CREATE TABLE core(id INTEGER PRIMARY KEY)")
        .unwrap();
    env.setup_rows_table();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    let mut dropper = env.conn.prepare("DROP TABLE core").unwrap();
    expect_step_busy(&mut dropper, "DROP TABLE while RETURNING is active");

    drop(dropper);
    drop(returning);

    assert_eq!(
        env.observer_ids(),
        vec![1],
        "rejected DROP must not roll back the active RETURNING writer"
    );
    assert_eq!(
        scalar_i64(
            &env.observer,
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'core'"
        ),
        1
    );
}

#[test]
fn test_update_is_busy_while_returning_writer_is_active() {
    let env = SameConnectionMvcc::new(":memory:unfinished-update-abandon");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'original')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    let mut updater = env.conn.prepare("UPDATE rows SET v = 'updated'").unwrap();
    expect_step_busy(&mut updater, "UPDATE while RETURNING is active");

    // The rejection aborts the UPDATE at step time, releasing its root
    // statement slot, so dropping the RETURNING writer commits its own
    // insert instead of deferring to a rejected sibling that will never run.
    drop(returning);
    drop(updater);

    assert_eq!(env.observer_ids(), vec![1, 10]);
    assert_eq!(env.observer_value_for_id(10), "original");
}

#[test]
fn test_delete_is_busy_while_returning_writer_is_active() {
    let env = SameConnectionMvcc::new(":memory:unfinished-delete-abandon");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'original')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    let mut deleter = env.conn.prepare("DELETE FROM rows WHERE id = 10").unwrap();
    expect_step_busy(&mut deleter, "DELETE while RETURNING is active");

    // The rejection aborts the DELETE at step time, releasing its root
    // statement slot, so dropping the RETURNING writer commits its own
    // insert instead of deferring to a rejected sibling that will never run.
    drop(returning);
    drop(deleter);

    assert_eq!(env.observer_ids(), vec![1, 10]);
    assert_eq!(env.observer_value_for_id(10), "original");
}

#[test]
fn test_duplicate_insert_is_busy_while_returning_writer_is_active() {
    let env = SameConnectionMvcc::new(":memory:constraint-error-rolls-back-failing");
    env.setup_rows_table();

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);

    expect_busy(
        env.conn.execute("INSERT INTO rows VALUES (1, 'duplicate')"),
        "duplicate INSERT while RETURNING is active",
    );
    drop(returning);

    assert_eq!(
        env.observer_ids(),
        vec![1],
        "rejected duplicate writer must not roll back the active RETURNING writer"
    );
}

#[test]
fn test_rejected_second_returning_and_drop_do_not_rollback_first_returning() {
    let env = SameConnectionMvcc::new(":memory:two-returning-then-unfinished-drop");
    env.conn
        .execute("CREATE TABLE core(id INTEGER PRIMARY KEY)")
        .unwrap();
    env.setup_rows_table();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut first = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut first), 1);
    let mut second = prepare_insert_returning(&env.conn, 2, "two");
    expect_step_busy(&mut second, "second RETURNING writer");
    let mut dropper = env.conn.prepare("DROP TABLE core").unwrap();
    expect_step_busy(&mut dropper, "DROP TABLE while RETURNING is active");

    drop(second);
    drop(dropper);
    drop(first);

    assert_eq!(env.observer_ids(), vec![1]);
    assert_eq!(
        scalar_i64(
            &env.observer,
            "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'core'"
        ),
        1
    );
}

#[test]
fn test_insert_is_busy_while_update_returning_writer_is_active() {
    let env = SameConnectionMvcc::new(":memory:update-returning-plus-insert-returning");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'original')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut update = env
        .conn
        .prepare("UPDATE rows SET v = 'updated' WHERE id = 10 RETURNING id")
        .unwrap();
    assert_eq!(step_returning_id(&mut update), 10);
    let mut insert = prepare_insert_returning(&env.conn, 1, "one");
    expect_step_busy(&mut insert, "INSERT while UPDATE RETURNING is active");

    drop(insert);
    drop(update);
    assert_eq!(env.observer_ids(), vec![10]);
    assert_eq!(env.observer_value_for_id(10), "updated");
}

#[test]
fn test_insert_is_busy_while_delete_returning_writer_is_active() {
    let env = SameConnectionMvcc::new(":memory:delete-returning-plus-insert-returning");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'original')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut delete = env
        .conn
        .prepare("DELETE FROM rows WHERE id = 10 RETURNING id")
        .unwrap();
    assert_eq!(step_returning_id(&mut delete), 10);
    let mut insert = prepare_insert_returning(&env.conn, 1, "one");
    expect_step_busy(&mut insert, "INSERT while DELETE RETURNING is active");

    drop(insert);
    drop(delete);
    assert_eq!(env.observer_ids(), Vec::<i64>::new());
}

/// SQLite rejects SAVEPOINT and RELEASE with SQLITE_BUSY while write
/// statements are in progress (vdbe.c, OP_Savepoint), and aborts in-progress
/// statements on ROLLBACK TO. Turso cannot abort a suspended statement, so all
/// three savepoint operations are rejected while a writer is suspended; the
/// writer can then resume and the savepoint stack stays usable.
#[test]
fn test_wal_savepoint_ops_are_busy_while_writer_suspended() {
    let env = SameConnectionWal::new(":memory:wal-savepoint-busy-mid-writer");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten'), (20, 'twenty')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    env.conn.execute("BEGIN").unwrap();
    env.conn.execute("SAVEPOINT s").unwrap();

    let mut writer = prepare_wal_update_yielding_on_table_read(&env, "updated");
    expect_busy(
        env.conn.execute("SAVEPOINT t"),
        "SAVEPOINT while a writer is suspended",
    );
    expect_busy(
        env.conn.execute("ROLLBACK TO s"),
        "ROLLBACK TO while a writer is suspended",
    );
    expect_busy(
        env.conn.execute("RELEASE s"),
        "RELEASE while a writer is suspended",
    );

    finish_without_rows(&mut writer);
    env.conn.execute("RELEASE s").unwrap();
    env.conn.execute("COMMIT").unwrap();

    assert_eq!(
        scalar_text(&env.observer, "SELECT v FROM rows WHERE id = 10"),
        "updated"
    );
    assert_eq!(
        scalar_text(&env.observer, "SELECT v FROM rows WHERE id = 20"),
        "updated"
    );
}

#[test]
fn test_wal_savepoint_is_busy_while_autocommit_writer_suspended() {
    let env = SameConnectionWal::new(":memory:wal-savepoint-busy-autocommit-writer");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut writer = prepare_wal_update_yielding_on_table_read(&env, "updated");
    expect_busy(
        env.conn.execute("SAVEPOINT s"),
        "SAVEPOINT while an autocommit writer is suspended",
    );

    finish_without_rows(&mut writer);
    assert_eq!(
        scalar_text(&env.observer, "SELECT v FROM rows WHERE id = 10"),
        "updated"
    );
    env.conn.execute("SAVEPOINT s").unwrap();
    env.conn.execute("RELEASE s").unwrap();
}

#[test]
fn test_mvcc_savepoint_is_busy_while_writer_suspended() {
    let env = SameConnectionMvcc::new(":memory:mvcc-savepoint-busy-mid-writer");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'ten')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    env.conn.execute("BEGIN").unwrap();

    let mut writer = prepare_yielding_update_all_rows(&env.conn, "updated");
    expect_busy(
        env.conn.execute("SAVEPOINT s"),
        "SAVEPOINT while an MVCC writer is suspended",
    );

    finish_without_rows(&mut writer);
    env.conn.execute("COMMIT").unwrap();
    assert_eq!(env.observer_value_for_id(10), "updated");
}

/// A same-connection rejection must bypass the busy handler: only the
/// application finishing or resetting its own statement can resolve it, so
/// retrying would burn the whole busy_timeout before failing anyway. The
/// rejection is an error (`StatementsInProgress`), not the retryable
/// `StepResult::Busy`, so the armed 600s busy_timeout never engages — if the
/// rejection ever regressed into the retryable path, the step below would
/// surface as StepResult::Sleep and expect_step_busy would catch it.
#[test]
fn test_second_writer_busy_does_not_invoke_busy_handler() {
    let env = SameConnectionMvcc::new(":memory:second-writer-skips-busy-handler");
    env.setup_rows_table();
    env.conn
        .set_busy_timeout(std::time::Duration::from_secs(600));

    let mut first = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut first), 1);
    let mut second = prepare_insert_returning(&env.conn, 2, "two");
    expect_step_busy(&mut second, "second writer with busy_timeout set");

    drop(second);
    drop(first);
    assert_eq!(env.observer_ids(), vec![1]);
}

// The two tests below pin known data-loss gaps in the MVCC deferred-commit
// model; they document current behavior, not desired behavior. A writer that
// finishes while a sibling statement holds the shared implicit MVCC
// transaction open defers its commit to the last sibling, so any path that
// ends that transaction in a rollback silently discards changes whose caller
// already observed success. The durable fix is committing at the writer's own
// halt; see the FIXME in `halt` (core/vdbe/execute.rs).

#[test]
fn test_mvcc_completed_writer_changes_lost_when_last_reader_abandoned() {
    let env = SameConnectionMvcc::new(":memory:completed-writer-lost-reader-abandoned");
    env.setup_rows_table();
    env.conn
        .execute("INSERT INTO rows VALUES (10, 'existing')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut reader = env.conn.prepare("SELECT id FROM rows ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 10);

    let mut returning = prepare_insert_returning(&env.conn, 1, "one");
    assert_eq!(step_returning_id(&mut returning), 1);
    finish_without_rows(&mut returning);
    drop(returning);

    // The reader is abandoned mid-scan instead of finishing, so the shared
    // transaction ends through the rollback path and the completed writer's
    // row is lost. Compare test_completed_writer_waits_for_sibling_mvcc_reader_to_commit,
    // where the reader finishes normally and the writer's row commits.
    drop(reader);

    assert_eq!(
        env.observer_ids(),
        vec![10],
        "pins the deferred-commit gap: an abandoned last reader discards the completed writer's row"
    );
}

#[test]
fn test_mvcc_completed_writer_changes_lost_when_joining_writer_errors() {
    let env = SameConnectionMvcc::new(":memory:completed-writer-lost-joining-writer-error");
    env.setup_rows_table();
    env.conn
        .execute("CREATE TABLE src(id INTEGER, v TEXT)")
        .unwrap();
    env.conn
        .execute("INSERT INTO src VALUES (2, 'two'), (1, 'duplicate')")
        .unwrap();
    env.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut reader = env.conn.prepare("SELECT id FROM src ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 1);

    env.conn
        .execute("INSERT INTO rows VALUES (1, 'one')")
        .unwrap();

    // The joining writer changes a row (id 2) before hitting the duplicate,
    // and has no local rollback boundary, so the whole shared transaction is
    // rolled back — including the earlier INSERT that already reported success.
    let err = env
        .conn
        .execute("INSERT INTO rows SELECT id, v FROM src")
        .expect_err("second insert should hit duplicate primary key");
    assert!(
        matches!(err, LimboError::Constraint(_)),
        "expected duplicate-key constraint error, got {err:?}"
    );

    drop(reader);
    assert_eq!(
        env.observer_ids(),
        Vec::<i64>::new(),
        "pins the deferred-commit gap: a failing joined writer discards the completed writer's row"
    );
}

#[cfg(feature = "fts")]
fn open_fts_mvcc_db(path: &str) -> (Arc<Database>, Arc<Connection>, Arc<Connection>) {
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io,
        path,
        OpenFlags::default(),
        DatabaseOpts::new().with_index_method(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
    let observer = db.connect().unwrap();
    (db, conn, observer)
}

/// A writer that finishes while a sibling statement holds the shared implicit
/// MVCC transaction open defers its commit to the last sibling (see
/// test_completed_writer_waits_for_sibling_mvcc_reader_to_commit). Its FTS
/// writes must be deferred with it: once the base row commits, the document
/// must be searchable. The deferred halt path must stage the writer's
/// index-method work or hand its cursor to the connection — otherwise the
/// pending documents die with the statement and the base row commits without
/// its index entries.
#[cfg(feature = "fts")]
#[test]
fn fts_writes_survive_deferred_shared_autocommit() {
    let (_db, conn, observer) = open_fts_mvcc_db(":memory:fts-deferred-shared-autocommit");
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (10, 'existing seed')")
        .unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();

    let mut reader = conn.prepare("SELECT id FROM docs ORDER BY id").unwrap();
    assert_eq!(step_returning_id(&mut reader), 10);

    let mut writer = conn
        .prepare("INSERT INTO docs VALUES (1, 'deferred needle') RETURNING id")
        .unwrap();
    assert_eq!(step_returning_id(&mut writer), 1);
    finish_without_rows(&mut writer);
    drop(writer);

    finish_without_rows(&mut reader);
    drop(reader);

    assert_eq!(
        ids_from_query(&observer, "SELECT id FROM docs ORDER BY id"),
        vec![1, 10],
        "the last sibling statement commits the completed writer's base row"
    );
    assert_eq!(
        ids_from_query(
            &observer,
            "SELECT id FROM docs WHERE fts_match(body, 'needle')"
        ),
        vec![1],
        "the FTS document must commit together with its base row"
    );
}

/// Dropping a connection mid-transaction is how the engine recovers when an
/// application abandons its handle (e.g. after a panic): Connection::drop
/// rolls the transaction back and releases its locks and leases. A
/// transaction-owned index-method cursor registered on the connection holds a
/// context whose Arc points back at that same connection, and the cycle must
/// not keep the connection alive — otherwise the drop recovery never runs,
/// the MVCC transaction stays active, and its FTS write lease blocks every
/// other writer forever.
#[cfg(feature = "fts")]
#[test]
fn dropping_connection_mid_transaction_releases_its_fts_write_lease() {
    let (_db, conn, observer) = open_fts_mvcc_db(":memory:fts-conn-drop-mid-tx");
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'abandoned mid transaction')")
        .unwrap();

    let weak = Arc::downgrade(&conn);
    drop(conn);

    observer
        .execute("INSERT INTO docs VALUES (2, 'after the drop')")
        .expect("dropping the writing connection must release its FTS write lease");
    assert!(
        weak.upgrade().is_none(),
        "a dropped connection must be freed; a registered index-method cursor must not keep it alive"
    );
}

/// INSERT OR FAIL keeps the rows changed before the failing one. The
/// constraint error is parked while the kept rows' index-method writes are
/// staged; an interrupt request arriving in that window must not replace the
/// statement's outcome and roll back the rows FAIL promised to keep.
#[cfg(feature = "fts")]
#[test]
fn interrupt_during_fail_staging_keeps_fail_outcome() {
    use crate::index_method::IndexMethodYieldPoint;

    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file_with_flags(
        io.clone(),
        ":memory:fts-interrupt-during-fail",
        OpenFlags::default(),
        DatabaseOpts::new().with_index_method(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    conn.set_yield_injector(Some(FixedYieldInjector::new([
        IndexMethodYieldPoint::AfterPrepareStatement.point(),
    ])));
    let mut insert = conn
        .prepare("INSERT OR FAIL INTO docs VALUES (1, 'kept row'), (1, 'duplicate')")
        .unwrap();
    expect_injected_yield(&mut insert, "OR FAIL index-method staging");
    conn.set_yield_injector(None);

    // The statement is suspended between staging the kept row's FTS writes
    // and surfacing the parked constraint error.
    conn.interrupt();

    let err = loop {
        match insert.step() {
            Err(err) => break err,
            Ok(StepResult::IO) => io.step().unwrap(),
            Ok(other) => panic!("OR FAIL must surface its constraint error, got {other:?}"),
        }
    };
    assert!(
        matches!(err, LimboError::Constraint(_)),
        "expected the parked constraint error, got {err:?}"
    );
    drop(insert);

    assert_eq!(
        ids_from_query(&conn, "SELECT id FROM docs ORDER BY id"),
        vec![1],
        "OR FAIL keeps rows changed before the failure"
    );
    assert_eq!(
        ids_from_query(&conn, "SELECT id FROM docs WHERE fts_match(body, 'kept')"),
        vec![1],
        "the kept row's FTS document must survive the interrupt request"
    );
}
