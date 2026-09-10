use std::sync::Arc;
use turso::IoBackend;
use turso_core::SqliteDialect;
use turso_core::{Database, OpenFlags};
use turso_sdk_kit::rsapi::{TursoDatabase, TursoDatabaseConfig, TursoStatusCode};

/// Regression test: DATABASE_MANAGER registry returns stale Database after
/// the underlying file is renamed.
///
/// Steps:
///   1. Open database "A.db", create a table and insert data
///   2. Close all connections and drop the Database
///   3. Rename A.db -> B.db on disk
///   4. Open a *new* database at path "A.db"
///   5. The new A.db should be a fresh, empty database
#[test]
fn test_database_rename_registry_stale() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path_a = tmp_dir.path().join("A.db");
    let path_b = tmp_dir.path().join("B.db");

    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());

    // 1. Open database A and populate it.
    let db_a = Database::open_file_with_flags(
        io.clone(),
        path_a.to_str().unwrap(),
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();

    let conn_a = db_a.connect().unwrap();
    conn_a.execute("CREATE TABLE t(x INTEGER)").unwrap();
    conn_a.execute("INSERT INTO t VALUES (42)").unwrap();

    // 2. Close all connections and drop the Database.
    drop(conn_a);
    drop(db_a);

    // 3. Rename A.db -> B.db on disk.
    std::fs::rename(&path_a, &path_b).unwrap();
    let wal_a = tmp_dir.path().join("A.db-wal");
    let wal_b = tmp_dir.path().join("B.db-wal");
    if wal_a.exists() {
        std::fs::rename(&wal_a, &wal_b).unwrap();
    }
    let shm_a = tmp_dir.path().join("A.db-shm");
    let shm_b = tmp_dir.path().join("B.db-shm");
    if shm_a.exists() {
        std::fs::rename(&shm_a, &shm_b).unwrap();
    }

    // 4. Open a new database at the original path A.db.
    let db_a2 = Database::open_file_with_flags(
        io.clone(),
        path_a.to_str().unwrap(),
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();

    // 5. The new A.db should be empty — querying table 't' should fail.
    let conn_a2 = db_a2.connect().unwrap();
    let result = conn_a2.execute("SELECT x FROM t");
    assert!(
        result.is_err(),
        "New database at A.db should not have table 't' — \
         DATABASE_MANAGER returned stale Database after rename"
    );
}

fn open_plain_file(path: &std::path::Path) -> turso_core::Result<Arc<Database>> {
    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());
    Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
}

/// SQLite must refuse the file as "file is not a database" (SQLITE_NOTADB).
/// Opening is lazy in SQLite, so run a query to make it read the header.
fn assert_sqlite_says_not_a_database(path: &std::path::Path) {
    let conn = rusqlite::Connection::open(path).unwrap();
    let err = conn
        .query_row("SELECT count(*) FROM sqlite_schema", [], |_| Ok(()))
        .unwrap_err();
    match err {
        rusqlite::Error::SqliteFailure(e, _) => assert_eq!(
            e.code,
            rusqlite::ErrorCode::NotADatabase,
            "sqlite reported {err:?} for {}",
            path.display()
        ),
        other => panic!("expected SQLITE_NOTADB from sqlite, got {other:?}"),
    }
}

fn assert_turso_says_not_a_database(path: &std::path::Path) {
    match open_plain_file(path) {
        Err(turso_core::LimboError::NotADB) => {}
        Err(other) => panic!("expected NotADB for {}, got {other:?}", path.display()),
        Ok(_) => panic!(
            "expected NotADB for {}, got a successful open",
            path.display()
        ),
    }
}

/// A file whose header is garbage must report NotADB ("file is not a
/// database"), like SQLite, and not a corruption error derived from a
/// garbage header field such as the page size. The corrupt8.test cascade
/// started with a garbage header being reported as "invalid page size",
/// which the sqlite3 compat layer then surfaced as "out of memory".
#[test]
fn test_open_garbage_header_reports_not_a_database() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("garbage.db");
    // 1050 zero bytes: no SQLite magic, page size field reads as 0.
    std::fs::write(&path, vec![0u8; 1050]).unwrap();

    assert_sqlite_says_not_a_database(&path);
    assert_turso_says_not_a_database(&path);
}

/// A non-empty file shorter than the 512-byte header can never be a valid
/// database. SQLite zero-fills the missing bytes and then rejects the header,
/// so it reports "file is not a database" whether or not the file starts
/// with the SQLite magic.
#[test]
fn test_open_short_file_reports_not_a_database() {
    let tmp_dir = tempfile::TempDir::new().unwrap();

    let garbage = tmp_dir.path().join("short-garbage.db");
    std::fs::write(&garbage, vec![0x42u8; 100]).unwrap();
    assert_sqlite_says_not_a_database(&garbage);
    assert_turso_says_not_a_database(&garbage);

    let truncated = tmp_dir.path().join("short-truncated.db");
    let mut bytes = b"SQLite format 3\0".to_vec();
    bytes.resize(100, 0);
    std::fs::write(&truncated, bytes).unwrap();
    assert_sqlite_says_not_a_database(&truncated);
    assert_turso_says_not_a_database(&truncated);
}

/// Regression test: TursoConnection.close() must finalize outstanding statements
/// so that the Statement → Arc<Connection> → Arc<Database> chain is broken.
///
/// Without the fix, un-finalized statements keep the Database alive in
/// DATABASE_MANAGER. After a file rename, reopening the same path returns
/// the stale Database.
///
/// This exercises the SDK-level fix: TursoConnection tracks all prepared
/// statements and close() drops them before closing the connection.
#[test]
fn test_sdk_close_finalizes_leaked_statements() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path_a = tmp_dir.path().join("A.db");
    let path_b = tmp_dir.path().join("B.db");

    // 1. Open database A via SDK and populate it.
    let db_a = TursoDatabase::new(TursoDatabaseConfig {
        path: path_a.to_str().unwrap().to_string(),
        experimental_features: None,
        async_io: false,
        encryption: None,
        vfs: IoBackend::Default,
        io: None,
        db_file: None,
        page_codec: None,
        open_flags: OpenFlags::default(),
    });
    let _ = db_a.open().unwrap();
    let conn_a = db_a.connect().unwrap();

    let mut create_stmt = conn_a.prepare_single("CREATE TABLE t(x INTEGER)").unwrap();
    assert_eq!(
        create_stmt.execute(None).unwrap().status,
        TursoStatusCode::Done
    );

    let mut insert_stmt = conn_a.prepare_single("INSERT INTO t VALUES (42)").unwrap();
    assert_eq!(
        insert_stmt.execute(None).unwrap().status,
        TursoStatusCode::Done
    );

    // 2. Prepare a statement but do NOT finalize or drop it — simulates an
    //    un-GC'd binding-level Statement object.
    let mut leaked_stmt = conn_a.prepare_single("SELECT x FROM t").unwrap();
    assert_eq!(leaked_stmt.step(None).unwrap(), TursoStatusCode::Row);

    // 3. close() finalizes all outstanding statements (the fix).
    //    This drops the inner turso_core::Statement, releasing the
    //    Arc<Connection> → Arc<Database> chain.
    conn_a.close().unwrap();

    // The leaked statement should now be finalized.
    assert!(leaked_stmt.step(None).is_err());

    // 4. Drop remaining handles.
    drop(leaked_stmt);
    drop(create_stmt);
    drop(insert_stmt);
    drop(conn_a);
    drop(db_a);

    // 5. Rename A.db → B.db on disk.
    std::fs::rename(&path_a, &path_b).unwrap();
    for ext in &["-wal", "-shm"] {
        let from = tmp_dir.path().join(format!("A.db{ext}"));
        let to = tmp_dir.path().join(format!("B.db{ext}"));
        if from.exists() {
            std::fs::rename(&from, &to).unwrap();
        }
    }

    // 6. Open a new database at the original path A.db.
    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let db_a2 = Database::open_file_with_flags(
        io,
        path_a.to_str().unwrap(),
        OpenFlags::Create,
        turso_core::DatabaseOpts::new(),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();

    // 7. The new A.db should be empty — table 't' should NOT exist.
    let conn_a2 = db_a2.connect().unwrap();
    let result = conn_a2.execute("SELECT x FROM t");
    assert!(
        result.is_err(),
        "New database at A.db should not have table 't' — \
         close() should have finalized statements and released the stale Database"
    );
}

/// Database::open with OpenOptions: works with pre-opened storage, and — when
/// no storage is supplied — resolves the default file storage at `path`.
#[test]
fn test_database_open_with_options() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("opts.db");
    let path = path.to_str().unwrap();

    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());

    let file = io.open_file(path, OpenFlags::Create, false).unwrap();
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(file));
    let db = Database::open(
        io.clone(),
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .storage(db_file)
            .flags(OpenFlags::Create),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE t(x INTEGER)").unwrap();
    conn.execute("INSERT INTO t VALUES (1)").unwrap();

    // With no storage supplied, open resolves the default file storage at
    // `path` and returns the same registered instance.
    let db_default = Database::open(
        io,
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect)).flags(OpenFlags::Create),
    )
    .unwrap();
    assert!(
        Arc::ptr_eq(&db, &db_default),
        "open without storage must resolve default storage and hit the registered instance"
    );
}

/// Database::open_async still requires storage: default-storage resolution is
/// synchronous (registry lookup, file probes, file open) and only runs in the
/// synchronous Database::open entry point.
#[test]
fn test_open_async_requires_storage() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("async-needs-storage.db");
    let path = path.to_str().unwrap();

    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let mut state = turso_core::OpenDbAsyncState::new();
    let err = Database::open_async(
        &mut state,
        io,
        path,
        &turso_core::OpenOptions::new(Arc::new(SqliteDialect)),
    )
    .expect_err("open_async without storage must fail");
    assert!(
        matches!(*err, turso_core::LimboError::InvalidArgument(ref m) if m.contains("storage")),
        "expected InvalidArgument about missing storage, got {err:?}"
    );
}

/// The registry-aware Database::open rejects a custom WAL path: the process
/// registry keys on the default WAL, so a nonstandard WAL must go through the
/// raw do_open path instead.
#[test]
fn test_open_rejects_wal_path() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("reject-wal.db");
    let path = path.to_str().unwrap();

    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let file = io.open_file(path, OpenFlags::Create, false).unwrap();
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(file));

    let err = Database::open(
        io.clone(),
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .storage(db_file)
            .flags(OpenFlags::Create)
            .wal_path(format!("{path}-wal-override")),
    )
    .expect_err("Database::open must reject a custom wal_path");
    assert!(
        matches!(err, turso_core::LimboError::InvalidArgument(ref m) if m.contains("wal_path")),
        "expected InvalidArgument about wal_path, got {err:?}"
    );

    // Register a canonical instance, then repeat with no pre-opened storage:
    // the rejection must fire before default-storage resolution, so a registry
    // hit cannot silently return the cached default-WAL instance.
    let _registered = Database::open(
        io.clone(),
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect)).flags(OpenFlags::Create),
    )
    .unwrap();
    let err = Database::open(
        io,
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .flags(OpenFlags::Create)
            .wal_path(format!("{path}-wal-override")),
    )
    .expect_err("Database::open must reject a custom wal_path even with a registry hit");
    assert!(
        matches!(err, turso_core::LimboError::InvalidArgument(ref m) if m.contains("wal_path")),
        "expected InvalidArgument about wal_path on the registry-hit path, got {err:?}"
    );
}

/// do_open skips the process-wide registry: opening the same path first
/// through the registry-aware open and then through do_open yields two
/// distinct Database instances, and do_open does not register itself (so a
/// later open() does not observe the do_open instance).
#[test]
fn test_do_open_skips_registry() {
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("do-open-distinct.db");
    let path = path.to_str().unwrap();

    let io: Arc<dyn turso_core::IO + Send> = Arc::new(turso_core::PlatformIO::new().unwrap());
    let file = io.open_file(path, OpenFlags::Create, false).unwrap();
    let db_file = Arc::new(turso_core::storage::database::DatabaseFile::new(file));

    // Register a canonical instance via the registry-aware open.
    let registered = Database::open(
        io.clone(),
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .storage(db_file.clone())
            .flags(OpenFlags::Create),
    )
    .unwrap();

    // do_open must NOT return the registered instance.
    let detached = Database::do_open(
        io.clone(),
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .storage(db_file.clone())
            .flags(OpenFlags::Create),
    )
    .unwrap();
    assert!(
        !Arc::ptr_eq(&registered, &detached),
        "do_open must open a fresh Database instance, not the registered one"
    );

    // A registry-aware open still returns the originally registered instance,
    // proving do_open did not register itself over it.
    let registered_again = Database::open(
        io,
        path,
        turso_core::OpenOptions::new(Arc::new(SqliteDialect))
            .storage(db_file)
            .flags(OpenFlags::Create),
    )
    .unwrap();
    assert!(
        Arc::ptr_eq(&registered, &registered_again),
        "open() must still return the registered instance; do_open must not replace it"
    );
}
