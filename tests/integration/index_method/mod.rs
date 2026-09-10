use std::collections::HashMap;
use std::sync::Arc;

use core_tester::common::rng_from_time_or_env;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha8Rng;
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
use turso_core::index_method::fts::FtsIndexMethod;
use turso_core::{
    index_method::{
        toy_vector_sparse_ivf::VectorSparseInvertedIndexMethod, IndexMethod, IndexMethodAttachment,
        IndexMethodConfiguration, IndexMethodContext,
    },
    schema::IndexColumn,
    types::IOResult,
    vector::{self, vector_types::VectorType},
    Numeric, Register, Result, Value, MAIN_DB_ID,
};

use crate::common::{limbo_exec_rows, limbo_exec_rows_fallible, ExecRows, TempDatabase};

fn run<T>(db: &TempDatabase, mut f: impl FnMut() -> turso_core::types::IOResultOr<T>) -> Result<T> {
    loop {
        match f()? {
            IOResult::Done(value) => return Ok(value),
            IOResult::IO(iocompletions) => {
                while !iocompletions.finished() {
                    db.io.step().unwrap();
                }
            }
        }
    }
}

fn index_method_context(
    connection: &Arc<turso_core::Connection>,
    attachment: &dyn IndexMethodAttachment,
) -> IndexMethodContext {
    IndexMethodContext::for_test(connection, MAIN_DB_ID, attachment).unwrap()
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
fn fts_test_stats(
    db: &TempDatabase,
    conn: &Arc<turso_core::Connection>,
    table_name: &str,
    index_name: &str,
    columns: &[(&str, usize)],
) -> turso_core::index_method::IndexMethodTestStats {
    let attachment = FtsIndexMethod
        .attach(&IndexMethodConfiguration {
            table_name: table_name.to_string(),
            index_name: index_name.to_string(),
            columns: columns
                .iter()
                .map(|&(name, index)| IndexColumn::new(name, index))
                .collect(),
            parameters: HashMap::default(),
        })
        .unwrap();
    let mut cursor = attachment.init().unwrap();
    run(db, || {
        cursor.open_read(&index_method_context(conn, attachment.as_ref()))
    })
    .unwrap();
    cursor.test_stats().unwrap().unwrap()
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
fn fts_attachment_test_stats(
    db: &TempDatabase,
    conn: &Arc<turso_core::Connection>,
    table_name: &str,
    index_name: &str,
) -> turso_core::index_method::IndexMethodTestStats {
    let attachment = conn
        .with_schema_mut(|schema| {
            schema
                .get_index(table_name, index_name)
                .and_then(|index| index.index_method.clone())
        })
        .unwrap()
        .expect("FTS attachment must exist in the connection schema");
    let mut cursor = attachment.init().unwrap();
    run(db, || {
        cursor.open_read(&index_method_context(conn, attachment.as_ref()))
    })
    .unwrap();
    cursor.test_stats().unwrap().unwrap()
}

/// Under MVCC the stats probe needs a live transaction; the touching SELECT
/// starts one. Works identically in WAL mode.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
fn fts_stats_in_txn(
    db: &TempDatabase,
    conn: &Arc<turso_core::Connection>,
    table_name: &str,
    index_name: &str,
) -> turso_core::index_method::IndexMethodTestStats {
    conn.execute("BEGIN").unwrap();
    limbo_exec_rows(
        conn,
        &format!("SELECT count(*) FROM {table_name} WHERE fts_match(body, 'probe')"),
    );
    let stats = fts_test_stats(db, conn, table_name, index_name, &[("body", 1)]);
    conn.execute("COMMIT").unwrap();
    stats
}

fn sparse_vector(v: &str) -> Value {
    let vector = vector::operations::text::vector_from_text(VectorType::Float32Sparse, v).unwrap();
    vector::operations::serialize::vector_serialize(vector).expect(turso_core::alloc::ALLOC_ERR_MSG)
}

// This raw-cursor test manually opens pager write transactions.
#[turso_macros::test(init_sql = "CREATE TABLE t(name, embedding)")]
fn test_vector_sparse_ivf_create_destroy(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let schema_rows = || {
        limbo_exec_rows(&conn, "SELECT * FROM sqlite_master")
            .into_iter()
            .map(|x| match &x[1] {
                rusqlite::types::Value::Text(t) => t.clone(),
                _ => unreachable!(),
            })
            .collect::<Vec<String>>()
    };

    assert_eq!(schema_rows(), vec!["t"]);

    let index = VectorSparseInvertedIndexMethod;
    let attached = index
        .attach(&IndexMethodConfiguration {
            table_name: "t".to_string(),
            index_name: "t_idx".to_string(),
            columns: vec![IndexColumn::new("embedding", 1)],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.create(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();
    assert_eq!(
        schema_rows(),
        vec!["t", "t_idx_inverted_index", "t_idx_stats"]
    );

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.destroy(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();
    assert_eq!(schema_rows(), vec!["t"]);
}

// This raw-cursor test manually opens pager write transactions.
#[turso_macros::test(init_sql = "CREATE TABLE t(name, embedding)")]
fn test_vector_sparse_ivf_insert_query(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let index = VectorSparseInvertedIndexMethod;
    let attached = index
        .attach(&IndexMethodConfiguration {
            table_name: "t".to_string(),
            index_name: "t_idx".to_string(),
            columns: vec![IndexColumn::new("embedding", 1)],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.create(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();

    for (i, vector_str) in [
        "[0, 0, 0, 1]",
        "[0, 0, 1, 0]",
        "[0, 1, 0, 0]",
        "[1, 0, 0, 0]",
    ]
    .iter()
    .enumerate()
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.open_write(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();

        let values = [
            Register::Value(sparse_vector(vector_str)),
            Register::Value(Value::from_i64((i + 1) as i64)),
        ];
        run(&tmp_db, || cursor.insert(&values)).unwrap();
        conn.execute(format!(
            "INSERT INTO t VALUES ('{i}', vector32_sparse('{vector_str}'))"
        ))
        .unwrap();
    }
    for (vector, results) in [
        ("[0, 0, 0, 1]", &[(1, 0.0)][..]),
        ("[0, 0, 1, 0]", &[(2, 0.0)][..]),
        ("[0, 1, 0, 0]", &[(3, 0.0)][..]),
        ("[1, 0, 0, 0]", &[(4, 0.0)][..]),
        ("[1, 0, 0, 1]", &[(1, 0.5), (4, 0.5)][..]),
        (
            "[1, 1, 1, 1]",
            &[(1, 0.75), (2, 0.75), (3, 0.75), (4, 0.75)][..],
        ),
    ] {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.open_read(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();

        let values = [
            Register::Value(Value::from_i64(0)),
            Register::Value(sparse_vector(vector)),
            Register::Value(Value::from_i64(5)),
        ];
        assert!(run(&tmp_db, || cursor.query_start(&values)).unwrap());

        for (i, (rowid, dist)) in results.iter().enumerate() {
            assert_eq!(
                *rowid,
                run(&tmp_db, || cursor.query_rowid()).unwrap().unwrap()
            );
            assert_eq!(
                *dist,
                run(&tmp_db, || cursor.query_column(0)).unwrap().as_float()
            );
            assert_eq!(
                i + 1 < results.len(),
                run(&tmp_db, || cursor.query_next()).unwrap()
            );
        }
    }
}

// This raw-cursor test manually opens pager write transactions.
#[turso_macros::test(init_sql = "CREATE TABLE t(name, embedding)")]
fn test_vector_sparse_ivf_update(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let index = VectorSparseInvertedIndexMethod;
    let attached = index
        .attach(&IndexMethodConfiguration {
            table_name: "t".to_string(),
            index_name: "t_idx".to_string(),
            columns: vec![IndexColumn::new("embedding", 1)],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.create(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();

    let mut writer = attached.init().unwrap();
    run(&tmp_db, || {
        writer.open_write(&index_method_context(&conn, attached.as_ref()))
    })
    .unwrap();

    let v0_str = "[0, 1, 0, 0]";
    let v1_str = "[1, 0, 0, 1]";
    let q = sparse_vector("[1, 0, 0, 1]");
    let v0 = sparse_vector(v0_str);
    let v1 = sparse_vector(v1_str);
    let insert0_values = [
        Register::Value(v0.clone()),
        Register::Value(Value::from_i64(1)),
    ];
    let insert1_values = [
        Register::Value(v1.clone()),
        Register::Value(Value::from_i64(1)),
    ];
    let query_values = [
        Register::Value(Value::from_i64(0)),
        Register::Value(q.clone()),
        Register::Value(Value::from_i64(1)),
    ];
    run(&tmp_db, || writer.insert(&insert0_values)).unwrap();
    conn.execute(format!(
        "INSERT INTO t VALUES ('test', vector32_sparse('{v0_str}'))"
    ))
    .unwrap();

    let mut reader = attached.init().unwrap();
    run(&tmp_db, || {
        reader.open_read(&index_method_context(&conn, attached.as_ref()))
    })
    .unwrap();
    assert!(!run(&tmp_db, || reader.query_start(&query_values)).unwrap());

    conn.execute(format!(
        "UPDATE t SET embedding = vector32_sparse('{v1_str}') WHERE rowid = 1"
    ))
    .unwrap();
    run(&tmp_db, || writer.delete(&insert0_values)).unwrap();
    run(&tmp_db, || writer.insert(&insert1_values)).unwrap();

    let mut reader = attached.init().unwrap();
    run(&tmp_db, || {
        reader.open_read(&index_method_context(&conn, attached.as_ref()))
    })
    .unwrap();
    assert!(run(&tmp_db, || reader.query_start(&query_values)).unwrap());
    assert_eq!(1, run(&tmp_db, || reader.query_rowid()).unwrap().unwrap());
    assert_eq!(
        0.0,
        run(&tmp_db, || reader.query_column(0)).unwrap().as_float()
    );
    assert!(!run(&tmp_db, || reader.query_next()).unwrap());
}

#[turso_macros::test(mvcc)]
fn test_vector_sparse_ivf_mvcc_sql(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE vectors(id INTEGER PRIMARY KEY, embedding)")
        .unwrap();
    conn.execute("CREATE INDEX vectors_idx ON vectors USING toy_vector_sparse_ivf (embedding)")
        .unwrap();
    conn.execute(
        "INSERT INTO vectors VALUES \
         (1, vector32_sparse('[1, 0, 0]')), \
         (2, vector32_sparse('[0, 1, 0]'))",
    )
    .unwrap();

    let nearest = |vector: &str| {
        limbo_exec_rows(
            &conn,
            &format!(
                "SELECT id FROM vectors \
                 ORDER BY vector_distance_jaccard(embedding, vector32_sparse('{vector}')) \
                 LIMIT 1"
            ),
        )
    };
    assert_eq!(
        nearest("[1, 0, 0]"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE vectors SET embedding = vector32_sparse('[0, 0, 1]') WHERE id = 1")
        .unwrap();
    assert_eq!(
        nearest("[0, 0, 1]"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    conn.execute("ROLLBACK").unwrap();
    assert_eq!(
        nearest("[1, 0, 0]"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_eq!(
        nearest("[0, 1, 0]"),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
}

// This differential harness disables automatic WAL actions on both databases.
#[turso_macros::test]
fn test_vector_sparse_ivf_fuzz(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();

    let opts = tmp_db.db_opts;
    let flags = tmp_db.db_flags;

    const DIMS: usize = 40;
    const MOD: u32 = 5;

    let (mut rng, _) = rng_from_time_or_env();
    let mut operation = 0;
    for delta in [0.0, 0.01, 0.05, 0.1, 0.5] {
        let seed = rng.next_u64();
        tracing::info!("======== seed: {} ========", seed);

        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let builder = TempDatabase::builder()
            .with_opts(opts)
            .with_flags(flags)
            .with_init_sql("CREATE TABLE t(key TEXT PRIMARY KEY, embedding)");
        let simple_db = builder.clone().build();
        let index_db = builder.build();
        tracing::info!(
            "simple_db: {:?}, index_db: {:?}",
            simple_db.path,
            index_db.path,
        );
        let simple_conn = simple_db.connect_limbo();
        let index_conn = index_db.connect_limbo();
        simple_conn.wal_auto_actions_disable();
        index_conn.wal_auto_actions_disable();
        index_conn
            .execute(format!("CREATE INDEX t_idx ON t USING toy_vector_sparse_ivf (embedding) WITH (delta = {delta})"))
            .unwrap();

        let vector = |rng: &mut ChaCha8Rng| {
            let mut values = Vec::with_capacity(DIMS);
            for _ in 0..DIMS {
                if rng.next_u32() % MOD == 0 {
                    values.push((rng.next_u32() as f32 / (u32::MAX as f32)).to_string());
                } else {
                    values.push("0".to_string())
                }
            }
            format!("[{}]", values.join(", "))
        };

        let mut keys = Vec::new();
        for _ in 0..200 {
            let choice = rng.next_u32() % 4;
            operation += 1;
            if choice == 0 {
                let key = rng.next_u64().to_string();
                let v = vector(&mut rng);
                let sql = format!("INSERT INTO t VALUES ('{key}', vector32_sparse('{v}'))");
                tracing::info!("({}) {}", operation, sql);
                simple_conn.execute(&sql).unwrap();
                index_conn.execute(sql).unwrap();
                keys.push(key);
            } else if choice == 1 && !keys.is_empty() {
                let idx = rng.next_u32() as usize % keys.len();
                let key = &keys[idx];
                let v = vector(&mut rng);
                let sql =
                    format!("UPDATE t SET embedding = vector32_sparse('{v}') WHERE key = '{key}'",);
                tracing::info!("({}) {}", operation, sql);
                simple_conn.execute(&sql).unwrap();
                index_conn.execute(&sql).unwrap();
            } else if choice == 2 && !keys.is_empty() {
                let idx = rng.next_u32() as usize % keys.len();
                let key = &keys[idx];
                let sql = format!("DELETE FROM t WHERE key = '{key}'");
                tracing::info!("({}) {}", operation, sql);
                simple_conn.execute(&sql).unwrap();
                index_conn.execute(&sql).unwrap();
                keys.remove(idx);
            } else {
                let v = vector(&mut rng);
                let k = rng.next_u32() % 20 + 1;
                let sql = format!(
                    "SELECT key, vector_distance_jaccard(embedding, vector32_sparse('{v}')) as d FROM t ORDER BY d LIMIT {k}"
                );
                tracing::info!("({}) {}", operation, sql);
                let simple_rows = limbo_exec_rows(&simple_conn, &sql);
                let index_rows = limbo_exec_rows(&index_conn, &sql);
                tracing::info!("simple: {:?}, index_rows: {:?}", simple_rows, index_rows);
                assert!(index_rows.len() <= simple_rows.len());
                for (a, b) in index_rows.iter().zip(simple_rows.iter()) {
                    if delta == 0.0 {
                        assert_eq!(a, b);
                    } else {
                        match (&a[1], &b[1]) {
                            (rusqlite::types::Value::Real(a), rusqlite::types::Value::Real(b)) => {
                                assert!(
                                    *a >= *b || (*a - *b).abs() < 1e-5,
                                    "a={}, b={}, delta={}",
                                    *a,
                                    *b,
                                    delta
                                );
                                assert!(
                                    *a - delta <= *b || (*a - delta - *b).abs() < 1e-5,
                                    "a={}, b={}, delta={}",
                                    *a,
                                    *b,
                                    delta
                                );
                            }
                            _ => panic!("unexpected column values"),
                        }
                    }
                }
                for row in simple_rows.iter().skip(index_rows.len()) {
                    match row[1] {
                        rusqlite::types::Value::Real(r) => assert!((1.0 - r) < 1e-5),
                        _ => panic!("unexpected simple row value"),
                    }
                }
            }
        }
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(init_sql = "CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")]
fn test_fts_create_destroy(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let schema_rows = || {
        limbo_exec_rows(
            &conn,
            "SELECT name FROM sqlite_master WHERE type='table' OR type='index'",
        )
        .into_iter()
        .map(|x| match &x[0] {
            rusqlite::types::Value::Text(t) => t.clone(),
            _ => unreachable!(),
        })
        .collect::<Vec<String>>()
    };

    // Initially just the docs table
    assert_eq!(schema_rows(), vec!["docs"]);

    let index = FtsIndexMethod;
    let attached = index
        .attach(&IndexMethodConfiguration {
            table_name: "docs".to_string(),
            index_name: "fts_docs".to_string(),
            columns: vec![IndexColumn::new("title", 1), IndexColumn::new("body", 2)],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.create(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();

    // After create, should have docs table plus FTS internal tables
    let tables = schema_rows();
    assert!(tables.contains(&"docs".to_string()));
    // FTS creates internal directory table for Tantivy storage
    assert!(tables.iter().any(|t| t.contains("fts_dir")));

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.destroy(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();

    // After destroy, internal FTS directory tables should be removed
    let tables_after = schema_rows();
    assert!(tables_after.contains(&"docs".to_string()));
    assert!(!tables_after.iter().any(|t| t.contains("fts_dir")));
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(init_sql = "CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")]
fn test_fts_insert_query(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    let index = FtsIndexMethod;
    let attached = index
        .attach(&IndexMethodConfiguration {
            table_name: "docs".to_string(),
            index_name: "fts_docs".to_string(),
            columns: vec![IndexColumn::new("title", 1), IndexColumn::new("body", 2)],
            parameters: HashMap::default(),
        })
        .unwrap();

    conn.wal_insert_begin().unwrap();
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.create(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
    }
    conn.wal_insert_end(true).unwrap();

    // Insert test documents
    let docs = [
        (
            1,
            "Introduction to Rust",
            "Rust is a systems programming language",
        ),
        (2, "Python Basics", "Python is great for beginners"),
        (
            3,
            "Advanced Rust",
            "Rust has powerful features like ownership",
        ),
        (
            4,
            "Database Systems",
            "Databases store and retrieve data efficiently",
        ),
    ];

    for (id, title, body) in docs {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.open_write(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();

        let values = [
            Register::Value(Value::Text(turso_core::types::Text::from(title))),
            Register::Value(Value::Text(turso_core::types::Text::from(body))),
            Register::Value(Value::from_i64(id)),
        ];
        run(&tmp_db, || cursor.insert(&values)).unwrap();
        // Flush FTS data before executing SQL (which auto-commits the transaction)
        // This mimics the VDBE's explicit statement-finalization phase.
        run(&tmp_db, || {
            cursor.stage_statement_commit(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();
        conn.execute(format!(
            "INSERT INTO docs VALUES ({id}, '{title}', '{body}')"
        ))
        .unwrap();
    }

    // Query for "Rust" - should match docs 1 and 3
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.open_read(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();

        // Pattern 0 = fts_score pattern with ORDER BY DESC LIMIT
        let values = [
            Register::Value(Value::from_i64(0)), // pattern index
            Register::Value(Value::Text(turso_core::types::Text::from("Rust"))),
            Register::Value(Value::from_i64(10)), // limit
        ];
        assert!(run(&tmp_db, || cursor.query_start(&values)).unwrap());

        // Collect results
        let mut results = Vec::new();
        loop {
            let rowid = run(&tmp_db, || cursor.query_rowid()).unwrap().unwrap();
            let score = run(&tmp_db, || cursor.query_column(0)).unwrap();
            if let Value::Numeric(Numeric::Float(s)) = score {
                results.push((rowid, f64::from(s)));
            }
            if !run(&tmp_db, || cursor.query_next()).unwrap() {
                break;
            }
        }

        // Should have 2 results for "Rust" (docs 1 and 3)
        assert_eq!(results.len(), 2);
        // Both rowids should be 1 or 3
        assert!(results.iter().all(|(r, _)| *r == 1 || *r == 3));
        // Scores should be positive
        assert!(results.iter().all(|(_, s)| *s > 0.0));
    }

    // Query for "Python" - should match doc 2
    {
        let mut cursor = attached.init().unwrap();
        run(&tmp_db, || {
            cursor.open_read(&index_method_context(&conn, attached.as_ref()))
        })
        .unwrap();

        let values = [
            Register::Value(Value::from_i64(0)),
            Register::Value(Value::Text(turso_core::types::Text::from("Python"))),
            Register::Value(Value::from_i64(10)),
        ];
        assert!(run(&tmp_db, || cursor.query_start(&values)).unwrap());

        let rowid = run(&tmp_db, || cursor.query_rowid()).unwrap().unwrap();
        assert_eq!(rowid, 2);
        assert!(!run(&tmp_db, || cursor.query_next()).unwrap());
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_sql_queries(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create table and FTS index via SQL
    conn.execute("CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert test data
    conn.execute("INSERT INTO articles VALUES (1, 'Database Performance', 'Optimizing database queries is important for performance')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (2, 'Web Development', 'Modern web applications use JavaScript and APIs')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (3, 'Database Design', 'Good database design leads to better performance')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (4, 'API Development', 'RESTful APIs are common in web services')")
        .unwrap();

    // Test fts_score with fts_match query (FTS index requires fts_match in WHERE to be used)
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'database') as score, id, title FROM articles WHERE fts_match(title, body, 'database') ORDER BY score DESC LIMIT 10",
    );
    assert_eq!(rows.len(), 2); // Should match docs 1 and 3
                               // Verify results contain expected IDs
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[1] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));

    // Test fts_match in WHERE clause with fts_score (combined pattern)
    // 'web' appears in doc 2 ("Web Development") and doc 4 ("web services")
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'web') as score, id, title FROM articles WHERE fts_match(title, body, 'web')",
    );
    assert_eq!(rows.len(), 2); // Should match docs 2 and 4
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[1] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&2));
    assert!(ids.contains(&4));
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_order_by_and_limit(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create table and FTS index
    conn.execute("CREATE TABLE notes(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_notes ON notes USING fts (title, body)")
        .unwrap();

    // Insert multiple documents with the search term appearing different number of times
    conn.execute("INSERT INTO notes VALUES (1, 'test', 'This is a test document')")
        .unwrap();
    conn.execute("INSERT INTO notes VALUES (2, 'test test', 'test test test')")
        .unwrap();
    conn.execute("INSERT INTO notes VALUES (3, 'another', 'Another document without the keyword')")
        .unwrap();
    conn.execute("INSERT INTO notes VALUES (4, 'test again', 'The test word appears in test')")
        .unwrap();

    // Test ORDER BY score DESC LIMIT
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'test') as score, id FROM notes WHERE fts_match(title, body, 'test') ORDER BY score DESC LIMIT 2",
    );
    assert_eq!(rows.len(), 2);
    // First result should have higher score than second
    let score1 = match &rows[0][0] {
        rusqlite::types::Value::Real(r) => *r,
        _ => panic!("Expected Real"),
    };
    let score2 = match &rows[1][0] {
        rusqlite::types::Value::Real(r) => *r,
        _ => panic!("Expected Real"),
    };
    assert!(score1 >= score2, "Results should be ordered by score DESC");

    // Test without LIMIT - should return all matches
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'test') as score, id FROM notes WHERE fts_match(title, body, 'test') ORDER BY score DESC",
    );
    assert_eq!(rows.len(), 3); // Posts 1, 2, and 4 contain "test"

    // Verify all scores are in descending order
    let scores: Vec<f64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Real(r) => Some(*r),
            _ => None,
        })
        .collect();
    for i in 1..scores.len() {
        assert!(
            scores[i - 1] >= scores[i],
            "Scores should be in descending order"
        );
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_limit_zero_and_negative(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    conn.execute("INSERT INTO articles VALUES (1, 'hello world', 'this is a test')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (2, 'another', 'hello again')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (3, 'no match', 'something else')")
        .unwrap();

    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'hello') as score FROM articles ORDER BY score DESC LIMIT 0",
    );
    assert!(rows.is_empty());

    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'hello') as score FROM articles ORDER BY score DESC LIMIT -1",
    );
    assert_eq!(rows.len(), 2);

    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'hello') as score FROM articles WHERE fts_match(title, body, 'hello') ORDER BY score DESC",
    );
    assert_eq!(rows.len(), 2);
}

/// Test FTS function recognition mode - queries that don't match predefined patterns
/// but are optimized via fts_match/fts_score function detection.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_function_recognition(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create table with extra columns to ensure queries don't match simple patterns
    conn.execute(
        "CREATE TABLE articles(id INTEGER PRIMARY KEY, author TEXT, category TEXT, title TEXT, body TEXT, views INTEGER)",
    )
    .unwrap();
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert test data
    conn.execute(
        "INSERT INTO articles VALUES (1, 'Alice', 'tech', 'Rust Programming Guide', 'Learn Rust from scratch', 100)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (2, 'Bob', 'tech', 'Python Basics', 'Introduction to Python', 200)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (3, 'Alice', 'science', 'Rust in Nature', 'Oxidation and rust formation', 50)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (4, 'Charlie', 'tech', 'Advanced Rust Patterns', 'Rust ownership and lifetimes', 300)",
    )
    .unwrap();

    // Test 1: Query with many extra SELECT columns (doesn't match patterns)
    // This exercises function recognition: pattern expects only fts_score() as score
    // but we SELECT multiple additional columns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, author, title, category, views, fts_score(title, body, 'Rust') as score FROM articles WHERE fts_match(title, body, 'Rust')",
    );
    assert_eq!(rows.len(), 3); // Posts 1, 3, 4 contain "Rust"
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
    assert!(ids.contains(&4));

    // Test 2: Query with extra WHERE and multiple columns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, title, views FROM articles WHERE fts_match(title, body, 'Rust') AND author = 'Alice'",
    );
    assert_eq!(rows.len(), 2); // Posts 1 and 3 by Alice containing Rust
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));

    // Test 3: Complex query with score, extra columns, WHERE, and ORDER BY
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'Rust') as score, id, title, author FROM articles WHERE fts_match(title, body, 'Rust') AND category = 'tech' ORDER BY score DESC",
    );
    assert_eq!(rows.len(), 2); // Posts 1 and 4 are tech posts about Rust
                               // Verify scores are in descending order
    let scores: Vec<f64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Real(r) => Some(*r),
            _ => None,
        })
        .collect();
    assert!(scores.len() == 2);
    assert!(scores[0] >= scores[1]);

    // Test 4: Query with only fts_match (no fts_score) and extra columns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, author, views FROM articles WHERE fts_match(title, body, 'Python')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Integer(i) => assert_eq!(*i, 2),
        _ => panic!("Expected integer id"),
    }
}

/// Test query patterns that wouldn't work with pattern-based matching
/// but should work with function recognition.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_flexible_query_patterns(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute(
        "CREATE TABLE docs(id INTEGER PRIMARY KEY, author TEXT, category TEXT, title TEXT, body TEXT, created_at INTEGER)",
    )
    .unwrap();
    conn.execute("CREATE INDEX fts_docs ON docs USING fts (title, body)")
        .unwrap();

    // Insert test data
    conn.execute("INSERT INTO docs VALUES (1, 'Alice', 'tech', 'Rust Guide', 'Learn Rust programming', 1000)")
        .unwrap();
    conn.execute(
        "INSERT INTO docs VALUES (2, 'Bob', 'tech', 'Python Guide', 'Learn Python basics', 2000)",
    )
    .unwrap();
    conn.execute("INSERT INTO docs VALUES (3, 'Alice', 'science', 'Rust Chemistry', 'Rust and oxidation', 3000)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (4, 'Charlie', 'tech', 'Advanced Rust', 'Rust patterns and idioms', 4000)")
        .unwrap();
    conn.execute(
        "INSERT INTO docs VALUES (5, 'Alice', 'tech', 'More Rust', 'Even more Rust content', 5000)",
    )
    .unwrap();

    // Test 1: SELECT specific columns (not * or just score) - wouldn't match patterns
    // Patterns expect SELECT * or SELECT fts_score(...) as score
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, title FROM docs WHERE fts_match(title, body, 'Rust')",
    );
    assert_eq!(rows.len(), 4); // Posts 1, 3, 4, 5

    // Test 2: ORDER BY non-score column ASC - patterns only support ORDER BY score DESC
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, title FROM docs WHERE fts_match(title, body, 'Rust') ORDER BY id ASC",
    );
    assert_eq!(rows.len(), 4);
    // Verify order by id
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![1, 3, 4, 5]);

    // Test 3: ORDER BY non-score column DESC - wouldn't match patterns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, created_at FROM docs WHERE fts_match(title, body, 'Rust') ORDER BY created_at DESC",
    );
    assert_eq!(rows.len(), 4);
    // Verify order by created_at DESC
    let created_ats: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[1] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert_eq!(created_ats, vec![5000, 4000, 3000, 1000]);

    // Test 4: Multiple WHERE conditions with different operators
    // Patterns don't have additional WHERE conditions
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Rust') AND created_at >= 3000 AND author = 'Alice'",
    );
    assert_eq!(rows.len(), 2); // Posts 3 and 5 (Alice, Rust, created_at >= 3000)
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&3));
    assert!(ids.contains(&5));

    // Test 5: LIMIT with non-pattern SELECT columns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, author FROM docs WHERE fts_match(title, body, 'Rust') LIMIT 2",
    );
    assert_eq!(rows.len(), 2); // Should return exactly 2 rows

    // Test 6: Computed expressions in SELECT - patterns don't handle expressions
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, author || ' wrote ' || title as description FROM docs WHERE fts_match(title, body, 'Python')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][1] {
        rusqlite::types::Value::Text(t) => assert_eq!(t, "Bob wrote Python Guide"),
        _ => panic!("Expected text"),
    }

    // Test 7: fts_score with extra columns and WHERE - wouldn't match combined patterns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'Rust') as score, id, author, category FROM docs WHERE fts_match(title, body, 'Rust') AND category = 'tech'",
    );
    // Should return tech posts about Rust: 1, 4, 5
    assert_eq!(rows.len(), 3);
    let ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[1] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&4));
    assert!(ids.contains(&5));
    // Verify scores are returned
    for row in &rows {
        match &row[0] {
            rusqlite::types::Value::Real(score) => assert!(*score > 0.0),
            _ => panic!("Expected real score"),
        }
    }

    // Test 8: Multiple SELECT expressions with score
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id * 10 as id_times_ten, fts_score(title, body, 'Rust') as score FROM docs WHERE fts_match(title, body, 'Rust')",
    );
    assert_eq!(rows.len(), 4);
    // Verify id * 10 calculation works
    let id_times_tens: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    // Should contain 10, 30, 40, 50 (ids 1,3,4,5 * 10)
    assert!(id_times_tens.contains(&10));
    assert!(id_times_tens.contains(&30));
    assert!(id_times_tens.contains(&40));
    assert!(id_times_tens.contains(&50));
}

/// Test FTS with different tokenizer configurations via WITH clause
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_tokenizer_configuration(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Test 1: Default tokenizer (should work without WITH clause)
    conn.execute("CREATE TABLE docs_default(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_default ON docs_default USING fts (content)")
        .unwrap();

    conn.execute("INSERT INTO docs_default VALUES (1, 'Hello World')")
        .unwrap();
    conn.execute("INSERT INTO docs_default VALUES (2, 'hello there')")
        .unwrap();

    // Default tokenizer lowercases, so "hello" should match both
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs_default WHERE fts_match(content, 'hello')",
    );
    assert_eq!(rows.len(), 2);

    // Test 2: Raw tokenizer (exact match only, no tokenization)
    conn.execute("CREATE TABLE docs_raw(id INTEGER PRIMARY KEY, tag TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_raw ON docs_raw USING fts (tag) WITH (tokenizer = 'raw')")
        .unwrap();

    conn.execute("INSERT INTO docs_raw VALUES (1, 'user-123')")
        .unwrap();
    conn.execute("INSERT INTO docs_raw VALUES (2, 'user-456')")
        .unwrap();
    conn.execute("INSERT INTO docs_raw VALUES (3, 'admin-123')")
        .unwrap();

    // Raw tokenizer should only match exact string
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs_raw WHERE fts_match(tag, 'user-123')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Integer(i) => assert_eq!(*i, 1),
        _ => panic!("Expected integer"),
    }

    // Partial match should NOT work with raw tokenizer
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs_raw WHERE fts_match(tag, 'user')",
    );
    assert_eq!(rows.len(), 0);

    // Test 3: Simple tokenizer (whitespace/punctuation split)
    conn.execute("CREATE TABLE docs_simple(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();
    conn.execute(
        "CREATE INDEX fts_simple ON docs_simple USING fts (content) WITH (tokenizer = 'simple')",
    )
    .unwrap();

    conn.execute("INSERT INTO docs_simple VALUES (1, 'Hello World')")
        .unwrap();
    conn.execute("INSERT INTO docs_simple VALUES (2, 'HELLO there')")
        .unwrap();

    // Simple tokenizer does basic split but preserves case
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs_simple WHERE fts_match(content, 'Hello')",
    );
    // Simple tokenizer in Tantivy lowercases by default too
    assert!(!rows.is_empty());
}

/// Test that invalid tokenizer names are rejected
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_invalid_tokenizer_rejected(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();

    // This should fail because 'invalid_tokenizer' is not a supported tokenizer
    let result = conn.execute(
        "CREATE INDEX fts_docs ON docs USING fts (content) WITH (tokenizer = 'invalid_tokenizer')",
    );
    assert!(result.is_err());
}

/// Test FTS with ngram tokenizer for substring matching
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_ngram_tokenizer(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE products(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute(
        "CREATE INDEX fts_products ON products USING fts (name) WITH (tokenizer = 'ngram')",
    )
    .unwrap();

    conn.execute("INSERT INTO products VALUES (1, 'iPhone 15 Pro')")
        .unwrap();
    conn.execute("INSERT INTO products VALUES (2, 'Samsung Galaxy')")
        .unwrap();
    conn.execute("INSERT INTO products VALUES (3, 'Google Pixel')")
        .unwrap();

    // Ngram tokenizer should allow partial matches
    // Search for "Pho" should match "iPhone"
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM products WHERE fts_match(name, 'Pho')",
    );
    // With ngram(2,3), "Pho" generates ngrams that should match ngrams in "iPhone"
    assert!(!rows.is_empty());

    // Ngram should also follow the default/simple tokenizer case-insensitive behavior
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM products WHERE fts_match(name, 'pho')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Integer(i) => assert_eq!(*i, 1),
        _ => panic!("Expected integer"),
    }

    // Search for "Gal" should match "Galaxy"
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM products WHERE fts_match(name, 'Gal')",
    );
    assert!(!rows.is_empty());
}

/// A one-character typo in a WITH clause key must be an error, not a
/// silently different index (issue #8169).
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn fts_with_clause_rejects_typo_key(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    let result =
        conn.execute("CREATE INDEX fts_docs ON docs USING fts (body) WITH (tokenzier = 'ngram')");
    assert!(
        result.is_err(),
        "typo key 'tokenzier' was accepted; the index silently uses the default tokenizer"
    );
}

/// An unrecognised extra key next to a valid one must also be an error.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn fts_with_clause_rejects_unknown_key(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    let result = conn.execute(
        "CREATE INDEX fts_docs ON docs USING fts (body) WITH (tokenizer = 'ngram', completely_bogus_key = 42)",
    );
    assert!(
        result.is_err(),
        "unknown key 'completely_bogus_key' was accepted and ignored"
    );
}

/// The same key spelled in two casings must be rejected as a duplicate,
/// not have one spelling silently win.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn fts_with_clause_rejects_duplicate_key_across_casings(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    let result = conn.execute(
        "CREATE INDEX fts_docs ON docs USING fts (body) WITH (tokenizer = 'ngram', TOKENIZER = 'raw')",
    );
    assert!(
        result.is_err(),
        "'tokenizer' and 'TOKENIZER' name the same key and must be a duplicate error"
    );
}

/// Keys are case-insensitive like other SQL keywords in DDL: a mis-cased
/// key must configure the index, not be silently ignored (issue #8169).
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn fts_with_clause_treats_keys_case_insensitively(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_docs ON docs USING fts (body) WITH (TOKENIZER = 'ngram')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'alpha')")
        .unwrap();

    // Only ngram can match the 2-character prefix 'al'; the default
    // tokenizer indexes whole words and returns nothing.
    let rows = limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'al')");
    assert_eq!(
        rows.len(),
        1,
        "TOKENIZER = 'ngram' was accepted but ignored: the index got the default tokenizer"
    );
}

/// min_gram/max_gram in the WITH clause must actually change the ngram
/// window instead of being ignored (issue #8169). With the default window
/// of (2, 3) a 1-character query term can never match; min_gram = 1 makes
/// it match.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn fts_with_clause_configures_ngram_window(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE narrow(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute(
        "CREATE INDEX fts_narrow ON narrow USING fts (body) WITH (tokenizer = 'ngram', min_gram = 1, max_gram = 3)",
    )
    .unwrap();
    conn.execute("INSERT INTO narrow VALUES (1, 'alpha')")
        .unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT id FROM narrow WHERE fts_match(body, 'a')");
    assert_eq!(
        rows.len(),
        1,
        "min_gram = 1 was ignored: a 1-character term did not match"
    );

    // The default window of (2, 3) cannot index 1-character grams, so the
    // same query on a default ngram index finds nothing.
    conn.execute("CREATE TABLE wide(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_wide ON wide USING fts (body) WITH (tokenizer = 'ngram')")
        .unwrap();
    conn.execute("INSERT INTO wide VALUES (1, 'alpha')")
        .unwrap();
    let rows = limbo_exec_rows(&conn, "SELECT id FROM wide WHERE fts_match(body, 'a')");
    assert_eq!(
        rows.len(),
        0,
        "default ngram window unexpectedly matched a 1-character term"
    );
}

/// Bad ngram window values must fail at CREATE INDEX time with a clear
/// error rather than falling back to the default window.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn fts_with_clause_rejects_bad_ngram_window(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();

    // min_gram must be a positive integer
    for clause in [
        "WITH (tokenizer = 'ngram', min_gram = 0)",
        "WITH (tokenizer = 'ngram', min_gram = 'one')",
        // min_gram above max_gram (explicit or the default of 3)
        "WITH (tokenizer = 'ngram', min_gram = 4)",
        "WITH (tokenizer = 'ngram', min_gram = 3, max_gram = 2)",
        // the window only makes sense for the ngram tokenizer
        "WITH (tokenizer = 'raw', min_gram = 1)",
        "WITH (max_gram = 4)",
    ] {
        let result = conn.execute(format!(
            "CREATE INDEX fts_docs ON docs USING fts (body) {clause}"
        ));
        assert!(result.is_err(), "bad ngram window `{clause}` was accepted");
    }
}

/// Test fts_highlight function for text highlighting
/// Signature: fts_highlight(text1, text2, ..., before_tag, after_tag, query)
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_highlight_basic(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Test basic highlighting (single text column)
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('The quick brown fox', '<b>', '</b>', 'quick')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "The <b>quick</b> brown fox");
        }
        _ => panic!("Expected text result"),
    }

    // Test multiple matches
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('hello world hello', '[', ']', 'hello')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "[hello] world [hello]");
        }
        _ => panic!("Expected text result"),
    }

    // Test case-insensitive matching (tokenizer lowercases)
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('Hello World', '<em>', '</em>', 'hello')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "<em>Hello</em> World");
        }
        _ => panic!("Expected text result"),
    }

    // Test no matches - should return original text
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('The quick brown fox', '<b>', '</b>', 'zebra')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "The quick brown fox");
        }
        _ => panic!("Expected text result"),
    }

    // Test empty query - should return original text
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('Some text here', '<b>', '</b>', '')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "Some text here");
        }
        _ => panic!("Expected text result"),
    }

    // Test multiple text columns
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight('Hello world', 'Goodbye moon', '<b>', '</b>', 'world')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "Hello <b>world</b> Goodbye moon");
        }
        _ => panic!("Expected text result"),
    }
}

/// Test fts_highlight with FTS index queries
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_highlight_with_fts_query(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create table and FTS index
    conn.execute("CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert test data
    conn.execute("INSERT INTO articles VALUES (1, 'Database Design', 'Learn about database optimization and query performance')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (2, 'Web Development', 'Building modern web applications with databases')")
        .unwrap();

    // Query with fts_match and fts_highlight together
    // New signature: fts_highlight(text..., before_tag, after_tag, query)
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, fts_highlight(body, '<mark>', '</mark>', 'database') as highlighted FROM articles WHERE fts_match(title, body, 'database')",
    );

    // Should match article 1 (has "database" in both title and body)
    assert!(!rows.is_empty());

    // Check that the highlighted body contains the mark tags
    let mut found_highlight = false;
    for row in &rows {
        if let rusqlite::types::Value::Text(s) = &row[1] {
            if s.contains("<mark>") && s.contains("</mark>") {
                found_highlight = true;
                break;
            }
        }
    }
    assert!(
        found_highlight,
        "Expected highlighted text with <mark> tags"
    );
}

/// Test fts_highlight with NULL values
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_highlight_null_handling(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // NULL text should skip that column (not return NULL)
    // New behavior: NULL text columns are skipped when concatenating
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_highlight(NULL, 'some text', '<b>', '</b>', 'text')",
    );
    assert_eq!(rows.len(), 1);
    match &rows[0][0] {
        rusqlite::types::Value::Text(s) => {
            assert_eq!(s, "some <b>text</b>");
        }
        _ => panic!("Expected text result"),
    }

    // NULL query should return NULL
    let rows = limbo_exec_rows(&conn, "SELECT fts_highlight('text', '<b>', '</b>', NULL)");
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], rusqlite::types::Value::Null));

    // NULL before_tag should return NULL
    let rows = limbo_exec_rows(&conn, "SELECT fts_highlight('text', NULL, '</b>', 'query')");
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], rusqlite::types::Value::Null));

    // NULL after_tag should return NULL
    let rows = limbo_exec_rows(&conn, "SELECT fts_highlight('text', '<b>', NULL, 'query')");
    assert_eq!(rows.len(), 1);
    assert!(matches!(rows[0][0], rusqlite::types::Value::Null));
}

/// Test field weights configuration for FTS indexes
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_field_weights(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create table with title and body columns
    conn.execute("CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();

    // Create FTS index with title weighted 2x higher than body
    conn.execute(
        "CREATE INDEX fts_weighted ON articles USING fts (title, body) WITH (weights='title=2.0,body=1.0')",
    )
    .unwrap();

    // Insert test data - same word in different columns
    conn.execute("INSERT INTO articles VALUES (1, 'rust programming', 'learn python programming')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (2, 'python basics', 'rust is fast')")
        .unwrap();

    // Search for "rust" - article 1 has it in title (2x boost), article 2 has it in body (1x boost)
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id, fts_score(title, body, 'rust') as score FROM articles WHERE fts_match(title, body, 'rust') ORDER BY score DESC",
    );
    assert_eq!(rows.len(), 2);

    // Article 1 should have higher score (rust in title with 2x boost)
    match &rows[0][0] {
        rusqlite::types::Value::Integer(id) => assert_eq!(*id, 1),
        _ => panic!("Expected integer id"),
    }

    // Article 2 should have lower score (rust in body with 1x boost)
    match &rows[1][0] {
        rusqlite::types::Value::Integer(id) => assert_eq!(*id, 2),
        _ => panic!("Expected integer id"),
    }

    // Verify scores - title match should have higher score than body match
    let score1 = match &rows[0][1] {
        rusqlite::types::Value::Real(s) => *s,
        _ => panic!("Expected real score"),
    };
    let score2 = match &rows[1][1] {
        rusqlite::types::Value::Real(s) => *s,
        _ => panic!("Expected real score"),
    };
    assert!(
        score1 > score2,
        "Title match (boosted 2x) should score higher than body match"
    );
}

/// Test that invalid weight configurations are rejected
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_invalid_weights_rejected(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();

    // Unknown column name should fail
    let result = conn.execute(
        "CREATE INDEX fts_bad ON docs USING fts (title, body) WITH (weights='unknown=2.0')",
    );
    assert!(result.is_err());

    // Invalid weight value should fail
    let result =
        conn.execute("CREATE INDEX fts_bad2 ON docs USING fts (title) WITH (weights='title=abc')");
    assert!(result.is_err());

    // Negative weight should fail
    let result =
        conn.execute("CREATE INDEX fts_bad3 ON docs USING fts (title) WITH (weights='title=-1.0')");
    assert!(result.is_err());

    // Missing equals sign should fail
    let result =
        conn.execute("CREATE INDEX fts_bad4 ON docs USING fts (title) WITH (weights='title2.0')");
    assert!(result.is_err());
}

/// Regression test: Query -> Insert -> Query should not panic with "dirty pages must be empty"
/// This tests that FTS cursor caching doesn't share pending_writes between cursors,
/// which would cause writes from one cursor to affect the Drop behavior of another.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(
    init_sql = "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)"
)]
fn test_fts_query_insert_query_no_panic(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create FTS index
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert some initial data
    conn.execute(
        "INSERT INTO articles VALUES (1, 'Rust Programming', 'Rust is a systems language')",
    )
    .unwrap();
    conn.execute("INSERT INTO articles VALUES (2, 'Python Guide', 'Python is easy to learn')")
        .unwrap();

    // Query a few times (this caches the directory)
    let rows = limbo_exec_rows(
        &conn,
        "SELECT * FROM articles WHERE fts_match(title, body, 'Rust')",
    );
    assert_eq!(rows.len(), 1);

    let rows = limbo_exec_rows(
        &conn,
        "SELECT * FROM articles WHERE fts_match(title, body, 'Python')",
    );
    assert_eq!(rows.len(), 1);

    let rows = limbo_exec_rows(
        &conn,
        "SELECT * FROM articles WHERE fts_match(title, body, 'programming')",
    );
    assert_eq!(rows.len(), 1);

    // Insert more data (this should not cause dirty pages to leak to next read)
    conn.execute("INSERT INTO articles VALUES (3, 'Go Tutorial', 'Go is great for concurrency')")
        .unwrap();

    // Query again, should NOT panic with "dirty pages must be empty for read txn"
    let rows = limbo_exec_rows(
        &conn,
        "SELECT * FROM articles WHERE fts_match(title, body, 'Go')",
    );
    assert_eq!(rows.len(), 1);
    let rows = limbo_exec_rows(
        &conn,
        "SELECT * FROM articles WHERE fts_match(title, body, 'Rust')",
    );
    assert_eq!(rows.len(), 1);
}

/// Comprehensive FTS lifecycle test:
/// 1. Create index on table with many rows
/// 2. Query with FTS methods
/// 3. Insert into table
/// 4. Query again
/// 5. Delete from table
/// 6. Query again
/// 7. Large update
/// 8. Query again
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(
    init_sql = "CREATE TABLE docs(id INTEGER PRIMARY KEY, category TEXT, title TEXT, body TEXT)"
)]
fn test_fts_comprehensive_lifecycle(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // 1. Create FTS index
    conn.execute("CREATE INDEX fts_docs ON docs USING fts (title, body)")
        .unwrap();

    // Insert a moderate number of rows (100 documents across 4 categories)
    let categories = ["tech", "science", "business", "entertainment"];
    let tech_terms = [
        "Rust",
        "Python",
        "JavaScript",
        "programming",
        "software",
        "database",
    ];
    let science_terms = [
        "physics",
        "chemistry",
        "biology",
        "research",
        "experiment",
        "discovery",
    ];
    let business_terms = [
        "market",
        "investment",
        "startup",
        "revenue",
        "growth",
        "strategy",
    ];
    let entertainment_terms = [
        "movie",
        "music",
        "concert",
        "festival",
        "celebrity",
        "streaming",
    ];

    for i in 1..=100 {
        let category = categories[(i - 1) % 4];
        let terms = match category {
            "tech" => &tech_terms,
            "science" => &science_terms,
            "business" => &business_terms,
            _ => &entertainment_terms,
        };
        let term1 = terms[(i - 1) % terms.len()];
        let term2 = terms[i % terms.len()];
        let title = format!("{term1} Article {i}");
        let body = format!("This is article {i} about {term1} and {term2}. More content here.",);
        conn.execute(format!(
            "INSERT INTO docs VALUES ({i}, '{category}', '{title}', '{body}')",
        ))
        .unwrap();
    }

    // 2. Query with FTS methods - verify initial state
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Rust')",
    );
    assert!(!rows.is_empty(), "Should find Rust documents");
    let rust_count_initial = rows.len();

    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Python')",
    );
    assert!(!rows.is_empty(), "Should find Python documents");

    // Query with score ordering
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'programming') as score, id FROM docs WHERE fts_match(title, body, 'programming') ORDER BY score DESC LIMIT 10",
    );
    assert!(!rows.is_empty(), "Should find programming documents");

    // 3. Insert new documents
    conn.execute("INSERT INTO docs VALUES (101, 'tech', 'Advanced Rust Techniques', 'Deep dive into Rust programming patterns and idioms')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (102, 'tech', 'Rust Memory Safety', 'Exploring Rust ownership and borrowing mechanisms')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (103, 'science', 'Rust Prevention', 'Studying corrosion and metal oxidation')")
        .unwrap();

    // 4. Query again - verify inserts are indexed
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Rust')",
    );
    // Should have more Rust documents now (original + new inserts)
    assert!(
        rows.len() >= rust_count_initial + 2,
        "Should find more Rust documents after insert. Got {}, expected at least {}",
        rows.len(),
        rust_count_initial + 2
    );

    // Verify specific new document is findable
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'ownership borrowing')",
    );
    assert_eq!(rows.len(), 1, "Should find the memory safety document");
    match &rows[0][0] {
        rusqlite::types::Value::Integer(id) => assert_eq!(*id, 102),
        _ => panic!("Expected integer id"),
    }

    // 5. Delete from table
    conn.execute("DELETE FROM docs WHERE id = 101").unwrap();

    // 6. Query again - verify delete is reflected
    // Note: FTS delete support depends on implementation
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Advanced Techniques')",
    );
    // After delete, should not find document 101's content
    let has_deleted_doc = rows
        .iter()
        .any(|r| matches!(&r[0], rusqlite::types::Value::Integer(101)));
    assert!(!has_deleted_doc && rows.is_empty());

    // Other documents should still be queryable
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'ownership')",
    );
    assert_eq!(
        rows.len(),
        1,
        "Document 102 should still be findable after deleting 101"
    );

    // 7. Large update - update many rows
    conn.execute("UPDATE docs SET title = 'Updated ' || title WHERE category = 'tech'")
        .unwrap();

    // 8. Query again after update
    // Note: FTS update support may vary - just verify no panics and basic queries work
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Python')",
    );
    assert!(
        !rows.is_empty(),
        "Should still find Python documents after update"
    );

    let _science_rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'science')",
    );
    // Science docs weren't updated, should still work
    // Note: "science" might be in body text or not

    // Verify fts_score still works
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'database') as score, id FROM docs WHERE fts_match(title, body, 'database') ORDER BY score DESC",
    );
    // Just verify it doesn't panic and returns valid results
    for row in &rows {
        match &row[0] {
            rusqlite::types::Value::Real(score) => assert!(*score >= 0.0),
            rusqlite::types::Value::Integer(_) => {} // Some implementations may return int
            _ => panic!("Expected numeric score"),
        }
    }

    // Final verification - complex query with multiple conditions
    let rows = limbo_exec_rows(
        &conn,
        "SELECT fts_score(title, body, 'Rust') as score, id, category FROM docs WHERE fts_match(title, body, 'Rust') AND category = 'tech' ORDER BY score DESC LIMIT 5",
    );
    // Should find tech documents about Rust
    assert!(
        !rows.is_empty(),
        "Should find tech documents about Rust with complex query"
    );

    // Verify all results have category='tech'
    for row in &rows {
        match &row[2] {
            rusqlite::types::Value::Text(cat) => assert_eq!(cat, "tech"),
            _ => panic!("Expected text category"),
        }
    }
}

/// Test FTS behavior with explicit transactions
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(
    init_sql = "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, content TEXT)"
)]
fn test_fts_with_explicit_transactions(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create FTS index
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, content)")
        .unwrap();

    // Insert initial data
    conn.execute(
        "INSERT INTO articles VALUES (1, 'Rust Basics', 'Introduction to Rust programming')",
    )
    .unwrap();

    // Verify initial data is indexed
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE fts_match(title, content, 'Rust')",
    );
    assert_eq!(rows.len(), 1);

    // Start explicit transaction
    conn.execute("BEGIN").unwrap();

    // Insert within transaction
    conn.execute(
        "INSERT INTO articles VALUES (2, 'Advanced Rust', 'Rust ownership and lifetimes')",
    )
    .unwrap();
    conn.execute("INSERT INTO articles VALUES (3, 'Python Guide', 'Python for beginners')")
        .unwrap();

    // Commit transaction
    conn.execute("COMMIT").unwrap();

    // Verify all data is now indexed
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE fts_match(title, content, 'Rust')",
    );
    assert_eq!(rows.len(), 2, "Should find 2 Rust articles after commit");

    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE fts_match(title, content, 'Python')",
    );
    assert_eq!(rows.len(), 1, "Should find 1 Python article after commit");

    // Test rollback scenario
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO articles VALUES (4, 'Go Guide', 'Go concurrency patterns')")
        .unwrap();
    conn.execute("ROLLBACK").unwrap();

    // Verify rollback worked - Go article should not exist
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE fts_match(title, content, 'Go')",
    );
    assert_eq!(rows.len(), 0, "Should not find Go article after rollback");

    // Verify other data still intact
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE fts_match(title, content, 'Rust')",
    );
    assert_eq!(
        rows.len(),
        2,
        "Rust articles should still be indexed after rollback"
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn test_fts_mvcc_lifecycle(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'committed alpha'), (2, 'committed beta')")
        .unwrap();

    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'committed') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE docs SET body = 'ephemeral update' WHERE id = 1")
        .unwrap();
    conn.execute("DELETE FROM docs WHERE id = 2").unwrap();
    conn.execute("INSERT INTO docs VALUES (3, 'ephemeral insert')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'ephemeral') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );
    conn.execute("ROLLBACK").unwrap();

    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'committed') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
    assert!(limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(body, 'ephemeral')"
    )
    .is_empty());

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE docs SET body = 'durable update' WHERE id = 1")
        .unwrap();
    conn.execute("DELETE FROM docs WHERE id = 2").unwrap();
    conn.execute("INSERT INTO docs VALUES (3, 'durable insert')")
        .unwrap();
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'durable') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );

    conn.execute("OPTIMIZE INDEX docs_fts").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'durable') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );

    conn.execute("DROP INDEX docs_fts").unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'durable') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );
}

/// A connection handle dropped mid-transaction with a parked FTS cursor used
/// to keep itself alive forever: the cursor's context held a strong
/// `Arc<Connection>`, so the connection referenced itself, its `Drop` never
/// ran, and its WAL write lock was never released — every later writer got
/// Busy until the process died.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn fts_dropped_connection_mid_transaction_releases_locks(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'hello')")
        .unwrap();

    // Drop the handle with no COMMIT / ROLLBACK / close().
    let weak = std::sync::Arc::downgrade(&conn);
    drop(conn);
    assert_eq!(
        weak.strong_count(),
        0,
        "dropping the handle must actually drop the connection"
    );

    // A fresh connection must be able to write; the dropped transaction's
    // row must be gone.
    let fresh = tmp_db.connect_limbo();
    fresh
        .execute("INSERT INTO docs VALUES (2, 'world')")
        .unwrap();
    assert!(
        limbo_exec_rows(&fresh, "SELECT id FROM docs WHERE fts_match(body, 'hello')").is_empty()
    );
    assert_eq!(
        limbo_exec_rows(&fresh, "SELECT id FROM docs WHERE fts_match(body, 'world')"),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
}

/// One statement driving two FTS write cursors over the same index — here a
/// trigger inserting into the table it fired on — used to let both cursors
/// flush divergent Tantivy directories over one backing store, killing the
/// index on disk permanently (every later read returned Corrupt). The second
/// writer must be refused, the statement must roll back atomically, and the
/// index must stay fully usable.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn fts_second_write_cursor_in_one_statement_fails_cleanly(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(a TEXT)").unwrap();
    conn.execute("CREATE INDEX ft ON t USING fts(a)").unwrap();
    conn.execute(
        "CREATE TRIGGER tr AFTER INSERT ON t WHEN NEW.a <> 'stop' BEGIN \
         INSERT INTO t VALUES('stop'); END",
    )
    .unwrap();

    // The trigger's INSERT opens a second write cursor on the same FTS index
    // while the firing statement's writer is still open. It must fail...
    assert!(conn.execute("INSERT INTO t VALUES ('go')").is_err());
    // ...and the whole statement must roll back, keeping table and index in
    // sync (previously the base row committed and the index died on disk).
    assert!(limbo_exec_rows(&conn, "SELECT rowid FROM t").is_empty());

    // The index must stay healthy and writable afterwards.
    conn.execute("DROP TRIGGER tr").unwrap();
    conn.execute("INSERT INTO t VALUES ('after')").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT a FROM t WHERE fts_match(a, 'after')"),
        vec![vec![rusqlite::types::Value::Text("after".to_string())]]
    );
}

/// Regression test: a write cursor that hits the shared read cache must not
/// adopt the cached Tantivy `Index`, whose directory belongs to the cache
/// entry — its writes would land in the cache entry's pending map and never
/// reach the backing B-tree. On a TEMP (or ATTACHed) database the
/// `is_in_write_tx()` guard is false, so a read followed by a write used to
/// lose every later FTS write silently.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn fts_temp_db_write_after_cached_read_reaches_index(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TEMP TABLE t(a TEXT)").unwrap();
    conn.execute("CREATE INDEX temp.ft ON t USING fts(a)")
        .unwrap();
    // Warm the read cache for the TEMP database before any write.
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT rowid FROM t WHERE fts_match(a, 'alpha')"),
        Vec::<Vec<rusqlite::types::Value>>::new()
    );

    conn.execute("INSERT INTO t VALUES ('alpha')").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT rowid FROM t WHERE fts_match(a, 'alpha')"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );

    conn.execute("DELETE FROM t WHERE rowid = 1").unwrap();
    // The deleted row must not come back as a phantom from a stale index.
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT rowid FROM t WHERE fts_match(a, 'alpha')"),
        Vec::<Vec<rusqlite::types::Value>>::new()
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn fts_trigger_writes_survive_repeated_subprogram_runs(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("CREATE TABLE source(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER copy_docs AFTER INSERT ON source BEGIN \
         INSERT INTO docs VALUES(NEW.id, NEW.body); END",
    )
    .unwrap();

    conn.execute("INSERT INTO source VALUES (1, 'first trigger'), (2, 'second trigger')")
        .unwrap();

    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'trigger') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
}

/// RAISE(IGNORE) is not an error: everything the trigger wrote before the
/// RAISE is kept. The trigger's FTS writes used to be discarded while its base
/// rows survived, leaving the table and index permanently out of sync in both
/// directions (missing entry after an INSERT, phantom entry after a DELETE).
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn fts_raise_ignore_keeps_trigger_writes_in_index(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("CREATE TABLE src(v INTEGER)").unwrap();
    conn.execute(
        "CREATE TRIGGER tr BEFORE INSERT ON src BEGIN \
         INSERT INTO docs VALUES(NEW.v, 'ignoredrow'); \
         SELECT RAISE(IGNORE); END",
    )
    .unwrap();

    conn.execute("INSERT INTO src VALUES (1)").unwrap();

    // The trigger's row is kept, and so must its index entry be.
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs ORDER BY id"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'ignoredrow')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
}

/// The DELETE direction of the RAISE(IGNORE) divergence: a row deleted by the
/// trigger must not come back as a phantom from a stale index entry.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn fts_raise_ignore_keeps_trigger_deletes_in_index(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'orphanword')")
        .unwrap();
    conn.execute("CREATE TABLE src(v INTEGER)").unwrap();
    conn.execute(
        "CREATE TRIGGER tr BEFORE INSERT ON src BEGIN \
         DELETE FROM docs WHERE id = 1; \
         SELECT RAISE(IGNORE); END",
    )
    .unwrap();

    conn.execute("INSERT INTO src VALUES (1)").unwrap();

    assert!(limbo_exec_rows(&conn, "SELECT id FROM docs").is_empty());
    assert!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'orphanword')"
        )
        .is_empty(),
        "the deleted row must not survive as a phantom index entry"
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn fts_raise_fail_keeps_base_rows_and_index_in_sync(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER fail_second BEFORE INSERT ON docs WHEN NEW.id = 2 BEGIN \
         SELECT RAISE(FAIL, 'stop'); END",
    )
    .unwrap();

    assert!(conn
        .execute("INSERT INTO docs VALUES (1, 'first kept row'), (2, 'second rejected row')")
        .is_err());

    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs ORDER BY id"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'kept') ORDER BY id"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );

    conn.execute("BEGIN").unwrap();
    assert!(conn
        .execute("INSERT INTO docs VALUES (3, 'transaction kept row'), (2, 'still rejected')")
        .is_err());
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'kept') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'kept') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );

    conn.execute("CREATE TABLE source(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER copy_then_fail AFTER INSERT ON source BEGIN \
         INSERT INTO docs VALUES(NEW.id, NEW.body); \
         SELECT RAISE(FAIL, 'after copy'); END",
    )
    .unwrap();
    assert!(conn
        .execute("INSERT INTO source VALUES (4, 'trigger kept row')")
        .is_err());
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'kept') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
            vec![rusqlite::types::Value::Integer(4)],
        ]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn test_fts_mvcc_connection_isolation(tmp_db: TempDatabase) {
    let writer = tmp_db.connect_limbo();
    let observer = tmp_db.connect_limbo();

    writer
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    writer
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (1, 'committed token')")
        .unwrap();

    writer.execute("BEGIN").unwrap();
    writer
        .execute("UPDATE docs SET body = 'uncommitted token' WHERE id = 1")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (2, 'uncommitted token')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &writer,
            "SELECT id FROM docs WHERE fts_match(body, 'uncommitted') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
    assert!(limbo_exec_rows(
        &observer,
        "SELECT id FROM docs WHERE fts_match(body, 'uncommitted')"
    )
    .is_empty());
    assert_eq!(
        limbo_exec_rows(
            &observer,
            "SELECT id FROM docs WHERE fts_match(body, 'committed')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );

    writer.execute("COMMIT").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &observer,
            "SELECT id FROM docs WHERE fts_match(body, 'uncommitted') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn test_fts_mvcc_same_index_concurrent_writers_both_commit() {
    let tmp_db = TempDatabase::builder()
        .with_db_name("fts-same-index-concurrent-writers.db")
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();

    first
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    // Two BEGIN CONCURRENT transactions insert different rows into the same
    // FTS index. Each writer builds its own immutable segment and publishes
    // it under a fresh segment id, so their registry rows are disjoint keys:
    // both commit.
    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO docs VALUES (1, 'writer one')")
        .unwrap();
    second
        .execute("INSERT INTO docs VALUES (2, 'writer two')")
        .unwrap();

    // Neither transaction sees the other's uncommitted document.
    assert_eq!(
        limbo_exec_rows(
            &first,
            "SELECT id FROM docs WHERE fts_match(body, 'writer') ORDER BY id"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    assert_eq!(
        limbo_exec_rows(
            &second,
            "SELECT id FROM docs WHERE fts_match(body, 'writer') ORDER BY id"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );

    first.execute("COMMIT").unwrap();
    second.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(&first, "SELECT id FROM docs ORDER BY id"),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
    assert_eq!(
        limbo_exec_rows(
            &first,
            "SELECT id FROM docs WHERE fts_match(body, 'writer') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
    // A third connection sees both publications at a fresh snapshot.
    let third = tmp_db.connect_limbo();
    assert_eq!(
        limbo_exec_rows(
            &third,
            "SELECT id FROM docs WHERE fts_match(body, 'writer') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
}

/// Two overlapping BEGIN CONCURRENT writers on one FTS index, with every
/// commit checkpointed immediately (threshold 0). Under the v1 whole-manifest
/// format this scenario corrupted the index on disk (the second writer
/// rewrote shared metadata from a superseded base) and had to be refused.
/// With segment-registry storage each writer appends rows under its own
/// segment id, so both publications coexist — across the checkpoint too.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn test_fts_mvcc_overlapping_writers_publish_across_checkpoint() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();

    first
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    first
        .execute("CREATE TABLE t(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX ft ON t USING fts (body)")
        .unwrap();

    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first.execute("INSERT INTO t VALUES (1, 'alpha')").unwrap();
    first.execute("COMMIT").unwrap();

    // `second` began before `first` published. Its segment rows are disjoint
    // from the winner's, so it commits even though its snapshot is older.
    second.execute("INSERT INTO t VALUES (2, 'beta')").unwrap();
    second.execute("COMMIT").unwrap();

    // The index must be readable and the base table writable from a fresh
    // connection, with both publications visible.
    let third = tmp_db.connect_limbo();
    assert_eq!(
        limbo_exec_rows(&third, "SELECT id FROM t WHERE fts_match(body, 'alpha')"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    assert_eq!(
        limbo_exec_rows(&third, "SELECT id FROM t WHERE fts_match(body, 'beta')"),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
    third.execute("INSERT INTO t VALUES (3, 'gamma')").unwrap();
    assert_eq!(
        limbo_exec_rows(&third, "SELECT id FROM t WHERE fts_match(body, 'gamma')"),
        vec![vec![rusqlite::types::Value::Integer(3)]]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn test_fts_mvcc_different_index_writers_do_not_conflict() {
    let tmp_db = TempDatabase::builder()
        .with_db_name("fts-different-index-writers.db")
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();

    first
        .execute("CREATE TABLE first_docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX first_fts ON first_docs USING fts(body)")
        .unwrap();
    first
        .execute("CREATE TABLE second_docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX second_fts ON second_docs USING fts(body)")
        .unwrap();

    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO first_docs VALUES (1, 'first writer')")
        .unwrap();
    second
        .execute("INSERT INTO second_docs VALUES (2, 'second writer')")
        .unwrap();
    first.execute("COMMIT").unwrap();
    second.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(
            &first,
            "SELECT id FROM first_docs WHERE fts_match(body, 'writer')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    assert_eq!(
        limbo_exec_rows(
            &first,
            "SELECT id FROM second_docs WHERE fts_match(body, 'writer')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn test_fts_mvcc_opposite_index_order_writers_both_commit() {
    let tmp_db = TempDatabase::builder()
        .with_db_name("fts-opposite-index-order.db")
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();

    first
        .execute("CREATE TABLE a(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX a_fts ON a USING fts(body)")
        .unwrap();
    first
        .execute("CREATE TABLE b(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX b_fts ON b USING fts(body)")
        .unwrap();

    // Two transactions write both FTS indexes in opposite order. Writers
    // take no per-index lease anymore — segment appends commute — so
    // neither order can conflict or deadlock.
    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO a VALUES (1, 'survives')")
        .unwrap();
    second
        .execute("INSERT INTO b VALUES (2, 'survives')")
        .unwrap();
    first
        .execute("INSERT INTO b VALUES (1, 'survives')")
        .unwrap();
    second
        .execute("INSERT INTO a VALUES (2, 'survives')")
        .unwrap();
    first.execute("COMMIT").unwrap();
    second.execute("COMMIT").unwrap();

    for table in ["a", "b"] {
        assert_eq!(
            limbo_exec_rows(
                &second,
                &format!("SELECT id FROM {table} WHERE fts_match(body, 'survives') ORDER BY id")
            ),
            vec![
                vec![rusqlite::types::Value::Integer(1)],
                vec![rusqlite::types::Value::Integer(2)],
            ],
            "both writers' documents must be searchable in {table}"
        );
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn test_fts_mvcc_savepoint_rollback_does_not_block_concurrent_writer() {
    let tmp_db = TempDatabase::builder()
        .with_db_name("fts-savepoint-concurrent-writer.db")
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();

    first
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    first.execute("BEGIN CONCURRENT").unwrap();
    first.execute("SAVEPOINT pending_write").unwrap();
    first
        .execute("INSERT INTO docs VALUES (1, 'rolled back savepoint')")
        .unwrap();
    first.execute("ROLLBACK TO pending_write").unwrap();

    // Writers take no per-index lease, so the open transaction with a
    // rolled-back savepoint cannot block a concurrent writer.
    second.execute("BEGIN CONCURRENT").unwrap();
    second
        .execute("INSERT INTO docs VALUES (2, 'concurrent writer')")
        .unwrap();
    second.execute("COMMIT").unwrap();

    // The first transaction continues past its savepoint and commits; the
    // rolled-back document must not survive anywhere.
    first
        .execute("INSERT INTO docs VALUES (3, 'kept after savepoint')")
        .unwrap();
    first.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(&second, "SELECT id FROM docs ORDER BY id"),
        vec![
            vec![rusqlite::types::Value::Integer(2)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );
    assert!(limbo_exec_rows(
        &second,
        "SELECT id FROM docs WHERE fts_match(body, 'savepoint') AND id = 1"
    )
    .is_empty());
    assert_eq!(
        limbo_exec_rows(
            &second,
            "SELECT id FROM docs WHERE fts_match(body, 'concurrent')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
    assert_eq!(
        limbo_exec_rows(&second, "SELECT id FROM docs WHERE fts_match(body, 'kept')"),
        vec![vec![rusqlite::types::Value::Integer(3)]]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_connection_close_releases_merge_lease() {
    let tmp_db = TempDatabase::builder()
        .with_db_name("fts-close-merge-lease.db")
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();

    first
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for id in 0..3 {
        first
            .execute(format!("INSERT INTO docs VALUES ({id}, 'merge fodder')"))
            .unwrap();
    }

    // Take the per-index merge lease mid-transaction, then close without
    // committing: the abandoned lease must not starve later merges.
    first.execute("BEGIN CONCURRENT").unwrap();
    first.execute("OPTIMIZE INDEX docs_fts").unwrap();
    first.close().unwrap();

    second.execute("OPTIMIZE INDEX docs_fts").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &second,
            "SELECT count(*) FROM docs WHERE fts_match(body, 'merge')"
        ),
        vec![vec![rusqlite::types::Value::Integer(3)]]
    );
    assert_eq!(
        fts_stats_in_txn(&tmp_db, &second, "docs", "docs_fts").segment_count,
        Some(1),
        "the second connection's merge must run after the holder closed"
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn test_fts_mvcc_recovery(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'logical recovery'), (2, 'logical recovery')")
        .unwrap();

    let path = tmp_db.path.clone();
    let io = tmp_db.io.clone();
    let opts = tmp_db.db_opts;
    let flags = tmp_db.db_flags;
    conn.close().unwrap();
    drop(conn);
    drop(tmp_db);

    let db = turso_core::Database::open_file_with_flags(
        io.clone(),
        path.to_str().unwrap(),
        flags,
        opts,
        None,
        std::sync::Arc::new(turso_core::SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'recovery') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    conn.close().unwrap();
    drop(conn);
    drop(db);

    let db = turso_core::Database::open_file_with_flags(
        io,
        path.to_str().unwrap(),
        flags,
        opts,
        None,
        std::sync::Arc::new(turso_core::SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'recovery') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_reopen_after_crash_shaped_drop_recovers_all_generations() {
    // Crash-shaped: the database handle is dropped without close(), so no
    // final checkpoint runs. Recovery must serve both the checkpointed
    // generation (in the main file) and the WAL-only generation.
    for mvcc in [false, true] {
        let mut builder = TempDatabase::builder()
            .with_opts(turso_core::DatabaseOpts::new().with_index_method(true));
        if mvcc {
            builder = builder.with_mvcc(true);
        }
        let tmp_db = builder.build();
        let conn = tmp_db.connect_limbo();
        conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
            .unwrap();
        conn.execute("INSERT INTO docs VALUES (1, 'checkpointed generation')")
            .unwrap();
        conn.execute("PRAGMA wal_checkpoint(FULL)").unwrap();
        conn.execute("INSERT INTO docs VALUES (2, 'walonly generation')")
            .unwrap();

        let path = tmp_db.path.clone();
        let opts = tmp_db.db_opts;
        let flags = tmp_db.db_flags;
        drop(conn);
        drop(tmp_db);

        let reopened = TempDatabase::builder()
            .with_db_path(&path)
            .with_opts(opts)
            .with_flags(flags)
            .with_mvcc(mvcc)
            .build();
        let conn = reopened.connect_limbo();
        for (token, id) in [("checkpointed", 1i64), ("walonly", 2)] {
            assert_eq!(
                limbo_exec_rows(
                    &conn,
                    &format!("SELECT id FROM docs WHERE fts_match(body, '{token}')")
                ),
                vec![vec![rusqlite::types::Value::Integer(id)]],
                "mvcc={mvcc}: generation '{token}' lost across a crash-shaped reopen"
            );
        }
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_checkpoint_modes_preserve_index_content() {
    for mvcc in [false, true] {
        let opts = turso_core::DatabaseOpts::new()
            .with_index_method(true)
            .with_experimental_mvcc_passive_checkpoint(true);
        let mut builder = TempDatabase::builder().with_opts(opts);
        if mvcc {
            builder = builder.with_mvcc(true);
        }
        let tmp_db = builder.build();
        let conn = tmp_db.connect_limbo();
        conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
            .unwrap();

        let cases = [
            ("PASSIVE", "checkpointalpha"),
            ("FULL", "checkpointbravo"),
            ("RESTART", "checkpointcharlie"),
            ("TRUNCATE", "checkpointdelta"),
        ];
        for (id, (mode, token)) in cases.into_iter().enumerate() {
            let id = id as i64 + 1;
            conn.execute(format!("INSERT INTO docs VALUES ({id}, '{token}')"))
                .unwrap();
            conn.execute(format!("PRAGMA wal_checkpoint({mode})"))
                .unwrap();

            for (visible_id, (_, visible_token)) in cases.iter().take(id as usize).enumerate() {
                let visible_id = visible_id as i64 + 1;
                assert_eq!(
                    limbo_exec_rows(
                        &conn,
                        &format!("SELECT id FROM docs WHERE fts_match(body, '{visible_token}')")
                    ),
                    vec![vec![rusqlite::types::Value::Integer(visible_id)]],
                    "{mode} checkpoint lost FTS generation {visible_id}"
                );
            }
        }
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn test_fts_passive_checkpoint_preserves_pinned_reader() {
    for mvcc in [false, true] {
        let opts = turso_core::DatabaseOpts::new()
            .with_index_method(true)
            .with_experimental_mvcc_passive_checkpoint(true);
        let mut builder = TempDatabase::builder().with_opts(opts);
        if mvcc {
            builder = builder.with_mvcc(true);
        }
        let tmp_db = builder.build();
        let writer = tmp_db.connect_limbo();
        let reader = tmp_db.connect_limbo();
        writer
            .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        writer
            .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
            .unwrap();
        writer
            .execute("INSERT INTO docs VALUES (1, 'pinned generation')")
            .unwrap();

        reader.execute("BEGIN").unwrap();
        assert_eq!(
            limbo_exec_rows(
                &reader,
                "SELECT id FROM docs WHERE fts_match(body, 'pinned')"
            ),
            vec![vec![rusqlite::types::Value::Integer(1)]]
        );

        writer
            .execute("INSERT INTO docs VALUES (2, 'new generation')")
            .unwrap();
        if let Err(error) = writer.execute("PRAGMA wal_checkpoint(PASSIVE)") {
            assert!(
                matches!(error, turso_core::LimboError::Busy),
                "passive checkpoint returned unexpected error in {} mode: {error}",
                if mvcc { "MVCC" } else { "WAL" }
            );
        }
        assert!(
            limbo_exec_rows(&reader, "SELECT id FROM docs WHERE fts_match(body, 'new')").is_empty(),
            "a checkpoint must not move a pinned reader to the new FTS manifest"
        );
        reader.execute("COMMIT").unwrap();

        assert_eq!(
            limbo_exec_rows(&reader, "SELECT id FROM docs WHERE fts_match(body, 'new')"),
            vec![vec![rusqlite::types::Value::Integer(2)]],
            "the next transaction must observe the post-checkpoint manifest"
        );
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_switch_to_mvcc(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'before switch')")
        .unwrap();

    conn.pragma_update("journal_mode", "'mvcc'").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'switch')"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );

    conn.execute("UPDATE docs SET body = 'updated after transition' WHERE id = 1")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (2, 'inserted after transition')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'transition') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
    assert!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'switch')").is_empty()
    );

    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'transition') ORDER BY id"
        )
        .len(),
        2
    );

    conn.pragma_update("journal_mode", "'wal'").unwrap();
    conn.execute("UPDATE docs SET body = 'returned to wal' WHERE id = 1")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (3, 'created in wal')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'wal') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ],
        "FTS state must survive the MVCC-to-WAL transition"
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(init_sql = "CREATE TABLE docs(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")]
fn test_fts_optimize_index(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create FTS index
    conn.execute("CREATE INDEX fts_docs ON docs USING fts (title, body)")
        .unwrap();

    // Insert multiple batches of documents to create multiple segments
    for i in 0..10 {
        conn.execute(format!(
            "INSERT INTO docs VALUES ({i}, 'Document {i}', 'Content about topic {i} with keywords')",
        ))
        .unwrap();
    }

    // Verify documents are searchable
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(title, body, 'Document')",
    );
    assert_eq!(rows.len(), 10, "Should find all 10 documents");

    // Open an independent cursor so the catalog is reconstructed from the
    // physical backing B-tree rather than inherited from an attachment cache.
    #[cfg(feature = "test_helper")]
    let stats_before_optimize = fts_test_stats(
        &tmp_db,
        &conn,
        "docs",
        "fts_docs",
        &[("title", 1), ("body", 2)],
    );
    #[cfg(feature = "test_helper")]
    assert_eq!(
        stats_before_optimize.segment_count,
        Some(10),
        "each single-row commit publishes one immutable segment; merges run only in OPTIMIZE"
    );

    // Run OPTIMIZE INDEX on specific index
    conn.execute("OPTIMIZE INDEX fts_docs").unwrap();
    #[cfg(feature = "test_helper")]
    let stats_after_optimize = fts_test_stats(
        &tmp_db,
        &conn,
        "docs",
        "fts_docs",
        &[("title", 1), ("body", 2)],
    );
    #[cfg(feature = "test_helper")]
    assert_eq!(stats_after_optimize.segment_count, Some(1));
    #[cfg(feature = "test_helper")]
    assert!(
        stats_after_optimize.storage_file_count < stats_before_optimize.storage_file_count,
        "optimize must physically remove obsolete segment files: \
         before={}, after={}",
        stats_before_optimize.storage_file_count,
        stats_after_optimize.storage_file_count
    );

    // Verify documents are still searchable after optimize
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE (title, body) MATCH 'Document'",
    );
    assert_eq!(
        rows.len(),
        10,
        "Should still find all 10 documents after optimize"
    );

    // Verify content is correct
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE (title, body) MATCH 'topic'",
    );
    assert_eq!(rows.len(), 10, "Should find all documents with 'topic'");
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test(init_sql = "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT)")]
fn test_fts_optimize_all_indexes(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create second table manually
    conn.execute("CREATE TABLE posts(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();

    // Create FTS indexes on multiple tables
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title)")
        .unwrap();
    conn.execute("CREATE INDEX fts_posts ON posts USING fts (content)")
        .unwrap();

    // Insert data
    conn.execute("INSERT INTO articles VALUES (1, 'Rust Programming')")
        .unwrap();
    conn.execute("INSERT INTO articles VALUES (2, 'Python Guide')")
        .unwrap();
    conn.execute("INSERT INTO posts VALUES (1, 'Learning Rust is fun')")
        .unwrap();
    conn.execute("INSERT INTO posts VALUES (2, 'Advanced Rust patterns')")
        .unwrap();

    // Run OPTIMIZE INDEX without specifying index name (optimizes all)
    conn.execute("OPTIMIZE INDEX").unwrap();

    // Verify all indexes still work
    let rows = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE fts_match(title, 'Rust')",
    );
    assert_eq!(rows.len(), 1, "Should find Rust article");

    let rows = limbo_exec_rows(&conn, "SELECT id FROM posts WHERE content MATCH 'Rust'");
    assert_eq!(rows.len(), 2, "Should find both Rust posts");
}

/// Test that FTS functions work with column arguments in any order.
/// The index is created with columns (title, body), but queries should work
/// with fts_match(body, title, ...) as well as fts_match(title, body, ...).
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_column_order_agnostic(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create table and FTS index with columns in order (title, body)
    conn.execute("CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert test data - use 'database' in both articles 1 and 3
    conn.execute(
        "INSERT INTO articles VALUES (1, 'Database Design', 'Learn about database systems')",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (2, 'Web Development', 'Building modern web applications')",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (3, 'SQL Basics', 'Introduction to database and SQL')",
    )
    .unwrap();

    // Test standard column order: (title, body)
    let rows_standard = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE (title, body) MATCH 'database'",
    );
    assert_eq!(
        rows_standard.len(),
        2,
        "Standard order should find 2 matches (articles 1 and 3)"
    );
    let ids_standard: Vec<i64> = rows_standard
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids_standard.contains(&1));
    assert!(ids_standard.contains(&3));

    // Test reversed column order: (body, title)
    // This should work with column-order-agnostic matching
    let rows_reversed = limbo_exec_rows(
        &conn,
        "SELECT id FROM articles WHERE (body, title) MATCH 'database'",
    );
    assert_eq!(
        rows_reversed.len(),
        2,
        "Reversed column order should find same 2 matches"
    );
    let ids_reversed: Vec<i64> = rows_reversed
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(ids_reversed.contains(&1));
    assert!(ids_reversed.contains(&3));

    // Test fts_score with reversed column order
    let rows_score_reversed = limbo_exec_rows(
        &conn,
        "SELECT id, fts_score(body, title, 'database') as score FROM articles WHERE (body, title) MATCH 'database' ORDER BY score DESC",
    );
    assert_eq!(
        rows_score_reversed.len(),
        2,
        "fts_score with reversed columns should work"
    );

    // Verify both orderings return the same results
    assert_eq!(
        ids_standard.len(),
        ids_reversed.len(),
        "Both column orderings should return same number of results"
    );
    for id in &ids_standard {
        assert!(
            ids_reversed.contains(id),
            "Both orderings should return same IDs"
        );
    }
}

/// Test that FTS works with JOINS
/// This tests the removal of the single-table restriction for custom index methods.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_with_join(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create tables
    conn.execute(
        "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT, author_id INTEGER)",
    )
    .unwrap();
    conn.execute("CREATE TABLE authors(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    // Create FTS index on articles
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert authors
    conn.execute("INSERT INTO authors VALUES (1, 'Alice')")
        .unwrap();
    conn.execute("INSERT INTO authors VALUES (2, 'Bob')")
        .unwrap();
    conn.execute("INSERT INTO authors VALUES (3, 'Charlie')")
        .unwrap();

    // Insert articles with author references - use 'database' consistently
    conn.execute(
        "INSERT INTO articles VALUES (1, 'Database Design', 'Learn about database systems', 1)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (2, 'Web Development', 'Building modern web applications', 2)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (3, 'SQL Basics', 'Introduction to database and SQL', 1)",
    )
    .unwrap();
    conn.execute("INSERT INTO articles VALUES (4, 'API Design', 'RESTful API best practices', 3)")
        .unwrap();

    // Test FTS with JOIN - find articles about 'database' with author names
    let rows = limbo_exec_rows(
        &conn,
        "SELECT a.id, a.title, u.name FROM articles a JOIN authors u ON a.author_id = u.id WHERE (a.title, a.body) MATCH 'database'",
    );
    assert_eq!(
        rows.len(),
        2,
        "Should find 2 articles about database (articles 1 and 3)"
    );

    // Verify the results contain expected data
    let result_ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(result_ids.contains(&1), "Should include article 1");
    assert!(result_ids.contains(&3), "Should include article 3");

    // Verify author names are correctly joined
    let author_names: Vec<String> = rows
        .iter()
        .filter_map(|r| match &r[2] {
            rusqlite::types::Value::Text(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    // Both articles 1 and 3 are by Alice
    assert_eq!(
        author_names.iter().filter(|&n| n == "Alice").count(),
        2,
        "Both matching articles should be by Alice"
    );

    // Test FTS with JOIN and additional WHERE conditions
    let rows = limbo_exec_rows(
        &conn,
        "SELECT a.id, a.title, u.name FROM articles a JOIN authors u ON a.author_id = u.id WHERE (a.title, a.body) MATCH 'web' AND u.name = 'Bob'",
    );
    assert_eq!(rows.len(), 1, "Should find 1 article about web by Bob");
    let id = match &rows[0][0] {
        rusqlite::types::Value::Integer(i) => *i,
        _ => panic!("Expected integer id"),
    };
    assert_eq!(id, 2, "Should be article 2 (Web Development by Bob)");
}

/// Test FTS with LEFT JOIN to ensure outer joins work correctly with FTS.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_with_left_join(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create tables
    conn.execute(
        "CREATE TABLE posts(id INTEGER PRIMARY KEY, title TEXT, content TEXT, category_id INTEGER)",
    )
    .unwrap();
    conn.execute("CREATE TABLE categories(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    // Create FTS index
    conn.execute("CREATE INDEX fts_posts ON posts USING fts (title, content)")
        .unwrap();

    // Insert categories
    conn.execute("INSERT INTO categories VALUES (1, 'Technology')")
        .unwrap();
    conn.execute("INSERT INTO categories VALUES (2, 'Science')")
        .unwrap();

    // Insert posts - some with category, some without (NULL category_id)
    conn.execute(
        "INSERT INTO posts VALUES (1, 'Rust Programming', 'Systems programming with Rust', 1)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO posts VALUES (2, 'Python Basics', 'Introduction to Python programming', 1)",
    )
    .unwrap();
    conn.execute("INSERT INTO posts VALUES (3, 'Rust in Nature', 'How rust affects metal', 2)")
        .unwrap();
    conn.execute(
        "INSERT INTO posts VALUES (4, 'Uncategorized Rust', 'A post about Rust without category', NULL)",
    )
    .unwrap();

    // Test FTS with LEFT JOIN - should include post without category
    let rows = limbo_exec_rows(
        &conn,
        "SELECT p.id, p.title, c.name FROM posts p LEFT JOIN categories c ON p.category_id = c.id WHERE fts_match(p.title, p.content, 'Rust')",
    );
    assert_eq!(rows.len(), 3, "Should find 3 posts about Rust");

    // Verify we got the right posts
    let result_ids: Vec<i64> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Integer(i) => Some(*i),
            _ => None,
        })
        .collect();
    assert!(result_ids.contains(&1), "Should include post 1");
    assert!(result_ids.contains(&3), "Should include post 3");
    assert!(
        result_ids.contains(&4),
        "Should include post 4 (uncategorized)"
    );

    // Verify NULL category is preserved in LEFT JOIN
    let null_category_count = rows
        .iter()
        .filter(|r| matches!(&r[2], rusqlite::types::Value::Null))
        .count();
    assert_eq!(null_category_count, 1, "One post should have NULL category");
}

/// Test that FTS participates in join order optimization.
/// Uses EXPLAIN QUERY PLAN to verify the actual join order and that FTS is used.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_join_order_optimization(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create a small authors table and a larger articles table
    conn.execute("CREATE TABLE authors(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT, author_id INTEGER)",
    )
    .unwrap();

    // Create FTS index on articles
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert a few authors (small table)
    for i in 1..=5 {
        conn.execute(format!("INSERT INTO authors VALUES ({i}, 'Author{i}')"))
            .unwrap();
    }
    // so we use real statistics
    conn.execute("ANALYZE").unwrap();

    // Insert many articles (larger table) - more than authors to show cardinality difference
    for i in 1..=50 {
        let author_id = (i % 5) + 1;
        let (title, body) = if i % 10 == 0 {
            // Every 10th article is about database
            (
                format!("Database Article {i}"),
                "Content about database systems and SQL".to_string(),
            )
        } else {
            (
                format!("General Article {i}"),
                "General content about various topics".to_string(),
            )
        };
        conn.execute(format!(
            "INSERT INTO articles VALUES ({i}, '{title}', '{body}', {author_id})"
        ))
        .unwrap();
    }

    // Check the query plan using EXPLAIN QUERY PLAN
    let query = "SELECT a.id, a.title, u.name FROM articles a JOIN authors u ON a.author_id = u.id WHERE fts_match(a.title, a.body, 'database')";
    let eqp_rows = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query}"));

    // Extract table access order and check for FTS usage
    let mut table_order = Vec::new();
    let mut has_fts_search = false;
    for row in &eqp_rows {
        if let rusqlite::types::Value::Text(detail) = &row[3] {
            // Check for FTS index method query (format: "QUERY INDEX METHOD fts")
            if detail.contains("INDEX METHOD") || detail.contains("fts_articles") {
                has_fts_search = true;
            }
            // Extract table name from SCAN or SEARCH lines
            if let Some(rest) = detail.strip_prefix("SCAN ") {
                let table = rest.split_whitespace().next().unwrap();
                table_order.push(table.to_string());
            } else if let Some(rest) = detail.strip_prefix("SEARCH ") {
                let table = rest.split_whitespace().next().unwrap();
                table_order.push(table.to_string());
            } else if detail.starts_with("QUERY INDEX METHOD") {
                // FTS queries show up as "QUERY INDEX METHOD fts"
                table_order.push("articles".to_string());
            }
        }
    }

    // Verify that the optimizer is using the FTS index
    assert!(
        has_fts_search,
        "Expected FTS index to be used in query plan. Plan details: {:?}",
        eqp_rows
            .iter()
            .filter_map(|r| r.get(3).and_then(|v| match v {
                rusqlite::types::Value::Text(t) => Some(t.as_str()),
                _ => None,
            }))
            .collect::<Vec<_>>()
    );

    // Verify the join order: FTS should be first, authors second
    assert_eq!(
        table_order.len(),
        2,
        "Expected 2 tables in join order, got: {table_order:?}"
    );
    assert_eq!(
        table_order[0], "articles",
        "Expected articles (FTS) to be first in join order, got: {table_order:?}"
    );
    assert!(
        table_order[1] == "u" || table_order[1] == "authors",
        "Expected authors to be second in join order, got: {table_order:?}"
    );

    // Execute the query and verify results
    let rows = limbo_exec_rows(&conn, query);

    // Should find 5 articles about database
    assert_eq!(rows.len(), 5, "Should find 5 articles about database");

    // Verify all results have valid author names
    for row in &rows {
        let author_name = match &row[2] {
            rusqlite::types::Value::Text(t) => t.clone(),
            _ => panic!("Expected text for author name"),
        };
        assert!(
            author_name.starts_with("Author"),
            "Author name should start with 'Author'"
        );
    }

    // Test with reversed table order in SQL, optimizer should still use FTS
    let query2 = "SELECT a.id, a.title, u.name FROM authors u JOIN articles a ON u.id = a.author_id WHERE fts_match(a.title, a.body, 'database')";
    let eqp_rows2 = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query2}"));

    let mut has_fts_search2 = false;
    for row in &eqp_rows2 {
        if let rusqlite::types::Value::Text(detail) = &row[3] {
            if detail.contains("INDEX METHOD") || detail.contains("fts_articles") {
                has_fts_search2 = true;
            }
        }
    }
    assert!(
        has_fts_search2,
        "Expected FTS index to be used with reversed table order. Plan details: {:?}",
        eqp_rows2
            .iter()
            .filter_map(|r| r.get(3).and_then(|v| match v {
                rusqlite::types::Value::Text(t) => Some(t.as_str()),
                _ => None,
            }))
            .collect::<Vec<_>>()
    );

    let rows2 = limbo_exec_rows(&conn, query2);
    assert_eq!(
        rows2.len(),
        5,
        "Should find same 5 articles with reversed table order"
    );
}

/// Test FTS with multiple joins to verify cost-based optimization works
/// with more complex join patterns.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_multi_table_join(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    // Create three tables: categories, authors, articles
    conn.execute("CREATE TABLE categories(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE authors(id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute(
        "CREATE TABLE articles(id INTEGER PRIMARY KEY, title TEXT, body TEXT, author_id INTEGER, category_id INTEGER)",
    )
    .unwrap();

    // Create FTS index on articles
    conn.execute("CREATE INDEX fts_articles ON articles USING fts (title, body)")
        .unwrap();

    // Insert categories
    conn.execute("INSERT INTO categories VALUES (1, 'Technology')")
        .unwrap();
    conn.execute("INSERT INTO categories VALUES (2, 'Science')")
        .unwrap();
    conn.execute("INSERT INTO categories VALUES (3, 'Arts')")
        .unwrap();

    // Insert authors
    conn.execute("INSERT INTO authors VALUES (1, 'Alice')")
        .unwrap();
    conn.execute("INSERT INTO authors VALUES (2, 'Bob')")
        .unwrap();

    // Insert articles
    conn.execute(
        "INSERT INTO articles VALUES (1, 'Database Systems', 'Introduction to database management', 1, 1)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (2, 'Machine Learning', 'AI and neural networks', 2, 2)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (3, 'SQL Performance', 'Optimizing database queries', 1, 1)",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO articles VALUES (4, 'Modern Art', 'Contemporary art movements', 2, 3)",
    )
    .unwrap();

    // Test three-way join with FTS
    let rows = limbo_exec_rows(
        &conn,
        "SELECT a.title, u.name, c.name FROM articles a \
         JOIN authors u ON a.author_id = u.id \
         JOIN categories c ON a.category_id = c.id \
         WHERE (a.title, a.body) MATCH 'database'",
    );

    // Should find 2 articles about database (articles 1 and 3)
    assert_eq!(rows.len(), 2, "Should find 2 articles about database");

    // Verify we got the right combination
    let titles: Vec<String> = rows
        .iter()
        .filter_map(|r| match &r[0] {
            rusqlite::types::Value::Text(t) => Some(t.clone()),
            _ => None,
        })
        .collect();
    assert!(titles.contains(&"Database Systems".to_string()));
    assert!(titles.contains(&"SQL Performance".to_string()));
}

/// Regression test for issue 7522: a rolled-back transaction containing FTS
/// writes and OPTIMIZE INDEX must not leave the shared directory cache
/// pointing at segment files whose BTree rows were rolled back. Before the
/// fix, the next write against the index failed with
/// `FileDoesNotExist("<uuid>.term")`.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_rolled_back_optimize_does_not_leak_segment_state() {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, x TEXT, f TEXT, b BLOB)")
        .unwrap();
    conn.execute("CREATE INDEX idx ON t USING fts(f)").unwrap();
    conn.execute(
        "INSERT INTO t(id,x,f,b) VALUES (270323, 'x', 'optimize', X'01'), (-596572, NULL, 'foo', X'02')",
    )
    .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE t SET b=X'D9', f='rust token full search text search rollback'")
        .unwrap();
    conn.execute("OPTIMIZE INDEX idx").unwrap();
    conn.execute("ROLLBACK").unwrap();

    // Writes after the rollback must see the pre-transaction index state.
    conn.execute("INSERT INTO t(id) VALUES (32378), (NULL), (524997)")
        .unwrap();
    conn.execute("DELETE FROM t WHERE x").unwrap();

    // The rolled-back UPDATE must not be searchable; the surviving row is.
    let hits = limbo_exec_rows(&conn, "SELECT id FROM t WHERE f MATCH 'foo'");
    assert_eq!(
        hits.len(),
        1,
        "pre-transaction document must remain searchable"
    );
    let rolled_back = limbo_exec_rows(&conn, "SELECT id FROM t WHERE f MATCH 'rollback'");
    assert!(
        rolled_back.is_empty(),
        "rolled-back document must not be searchable"
    );
}

/// OPTIMIZE runs inside the caller's transaction. A rollback must restore
/// both the pre-merge registry rows and every retired segment, and a later
/// OPTIMIZE must be able to merge those restored segments.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_rolled_back_optimize_restores_segments() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for id in 0..7 {
        conn.execute(format!(
            "INSERT INTO docs VALUES ({id}, 'committed common document {id}')"
        ))
        .unwrap();
    }
    assert_eq!(
        fts_test_stats(&tmp_db, &conn, "docs", "docs_fts", &[("body", 1)]).segment_count,
        Some(7)
    );

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO docs VALUES (7, 'ephemeralrollbacktoken common document')")
        .unwrap();
    conn.execute("OPTIMIZE INDEX docs_fts").unwrap();
    assert_eq!(
        fts_test_stats(&tmp_db, &conn, "docs", "docs_fts", &[("body", 1)]).segment_count,
        Some(1),
        "the in-transaction OPTIMIZE should merge everything into one segment"
    );
    conn.execute("ROLLBACK").unwrap();

    assert_eq!(
        fts_test_stats(&tmp_db, &conn, "docs", "docs_fts", &[("body", 1)]).segment_count,
        Some(7),
        "rollback must restore the seven pre-merge segments"
    );
    assert!(limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(body, 'ephemeralrollbacktoken')"
    )
    .is_empty());

    conn.execute("INSERT INTO docs VALUES (8, 'surviving common document')")
        .unwrap();
    conn.execute("OPTIMIZE INDEX docs_fts").unwrap();
    assert_eq!(
        fts_test_stats(&tmp_db, &conn, "docs", "docs_fts", &[("body", 1)]).segment_count,
        Some(1),
        "a later OPTIMIZE should merge the restored segments"
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'common')").len(),
        8
    );
}

/// Sequential INSERT statements append segments without ever reading the
/// existing index: the write fast path stops at format detection.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_insert_statements_never_load_the_index() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    conn.execute("INSERT INTO docs VALUES (1, 'first appended document')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (2, 'second appended document')")
        .unwrap();
    // The insert statements never read the index, and the segments they
    // published are already resident in the shared byte cache — so nothing
    // has been loaded from backing storage at all.
    let stats = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(
        stats.full_snapshot_loads,
        Some(0),
        "insert statements must append segments without reading the index"
    );
    assert_eq!(stats.segment_count, Some(2));
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'appended')"
        )
        .len(),
        2
    );
    let before_drop = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");

    // Destroying an index must clear its shared caches so an index with the
    // same name can be created immediately with a fresh incarnation.
    conn.execute("DROP INDEX docs_fts").unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'appended')"
        )
        .len(),
        2
    );
    let after_recreate = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_ne!(
        after_recreate.index_incarnation, before_drop.index_incarnation,
        "drop/recreate must allocate a distinct persistent index incarnation"
    );
}

/// Later statements inside one explicit transaction must see the documents
/// earlier statements published (own-write visibility of segment rows), and
/// the whole set becomes visible to others only at COMMIT.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_explicit_transaction_sees_own_writes_before_commit() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'explicit transaction writer')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'transaction')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]],
        "the second statement must see the first statement's document"
    );
    conn.execute("INSERT INTO docs VALUES (2, 'newest transaction cursor')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'transaction') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)]
        ]
    );
    conn.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'transaction') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)]
        ]
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_statements_in_one_transaction_publish_independent_segments() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    conn.execute("BEGIN CONCURRENT").unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'first retained writer')")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (2, 'second retained writer')")
        .unwrap();
    // Own segments are visible to the transaction before commit.
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'retained') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)]
        ]
    );
    conn.execute("COMMIT").unwrap();

    // The stats probe needs a live MVCC transaction; the SELECT starts it.
    conn.execute("BEGIN").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'retained') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)]
        ]
    );
    let stats = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    conn.execute("COMMIT").unwrap();
    assert_eq!(
        stats.segment_count,
        Some(2),
        "each statement's flush publishes one immutable segment"
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_repeated_reads_reuse_the_cached_searcher() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'first transaction')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'first')"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let after_first = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    conn.execute("COMMIT").unwrap();

    // A new read transaction over the unchanged registry sees the same
    // segment set, so it shares the cached searcher instead of rebuilding.
    conn.execute("BEGIN").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'first')"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let after_second = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    conn.execute("COMMIT").unwrap();

    assert_eq!(
        after_second.full_snapshot_loads, after_first.full_snapshot_loads,
        "an unchanged segment set must not be reloaded from storage"
    );
    // The SELECT under test and the stats probe both hit; require two so the
    // assertion fails if the SELECT is deleted.
    assert!(
        after_second.read_cache_hits.unwrap() >= after_first.read_cache_hits.unwrap() + 2,
        "repeated reads over one segment set must share the cached searcher"
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_savepoint_rollback_discards_statement_documents() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    conn.execute("SAVEPOINT before_fts").unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'rolled back writer state')")
        .unwrap();
    conn.execute("ROLLBACK TO before_fts").unwrap();
    conn.execute("COMMIT").unwrap();

    assert!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'rolled')").is_empty(),
        "ROLLBACK TO must remove the savepoint's segment rows"
    );

    conn.execute("INSERT INTO docs VALUES (2, 'surviving writer state')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'surviving')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_rolled_back_concurrent_writer_leaves_no_trace() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let keeper = tmp_db.connect_limbo();
    let quitter = tmp_db.connect_limbo();

    keeper
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    keeper
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    keeper.execute("BEGIN CONCURRENT").unwrap();
    quitter.execute("BEGIN CONCURRENT").unwrap();
    keeper
        .execute("INSERT INTO docs VALUES (1, 'keeper writer')")
        .unwrap();
    quitter
        .execute("INSERT INTO docs VALUES (2, 'quitter writer')")
        .unwrap();
    // One concurrent writer rolls back; its segment rows vanish with its
    // transaction and must not disturb the other writer.
    quitter.execute("ROLLBACK").unwrap();
    keeper
        .execute("INSERT INTO docs VALUES (3, 'keeper second statement')")
        .unwrap();
    keeper.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(
            &keeper,
            "SELECT id FROM docs WHERE fts_match(body, 'writer') ORDER BY id"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]],
        "the rolled-back document must not be searchable"
    );

    // The quitter retries in a fresh transaction and succeeds.
    quitter.execute("BEGIN CONCURRENT").unwrap();
    quitter
        .execute("INSERT INTO docs VALUES (2, 'quitter retry writer')")
        .unwrap();
    quitter.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(
            &keeper,
            "SELECT id FROM docs WHERE fts_match(body, 'writer') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
}

/// Alternating writers on two WAL connections: every commit must be
/// visible to reads on both connections (each read scans the registry at
/// its own WAL snapshot; stale cached state can never resurface).
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_wal_alternating_connection_writes_stay_searchable() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn_a = tmp_db.connect_limbo();
    let conn_b = tmp_db.connect_limbo();

    conn_a
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn_a
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    conn_a
        .execute("INSERT INTO docs VALUES (1, 'alpha document')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn_b,
            "SELECT id FROM docs WHERE fts_match(body, 'alpha')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    conn_b
        .execute("INSERT INTO docs VALUES (2, 'beta document')")
        .unwrap();
    conn_a
        .execute("INSERT INTO docs VALUES (3, 'gamma document')")
        .unwrap();

    for conn in [&conn_a, &conn_b] {
        assert_eq!(
            limbo_exec_rows(
                conn,
                "SELECT id FROM docs WHERE fts_match(body, 'document') ORDER BY id"
            ),
            vec![
                vec![rusqlite::types::Value::Integer(1)],
                vec![rusqlite::types::Value::Integer(2)],
                vec![rusqlite::types::Value::Integer(3)],
            ]
        );
    }
}

/// A retained-cache budget too small to hold anything only affects
/// performance: every committed document stays searchable, reloaded from
/// the backing rows on each read.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_tiny_retained_cache_budget_only_affects_performance() {
    use turso_core::index_method::fts::set_fts_retained_cache_bytes_for_test;

    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn_a = tmp_db.connect_limbo();
    let conn_b = tmp_db.connect_limbo();

    conn_a
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn_a
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn_a
        .execute("INSERT INTO docs VALUES (1, 'alpha stays visible')")
        .unwrap();

    set_fts_retained_cache_bytes_for_test(Some(1));
    conn_b
        .execute("INSERT INTO docs VALUES (2, 'bravo must survive')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn_a,
            "SELECT id FROM docs WHERE fts_match(body, 'bravo')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
    conn_a
        .execute("INSERT INTO docs VALUES (3, 'charlie added later')")
        .unwrap();
    set_fts_retained_cache_bytes_for_test(None);

    assert_eq!(
        limbo_exec_rows(
            &conn_a,
            "SELECT id FROM docs WHERE fts_match(body, 'bravo') OR fts_match(body, 'charlie') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(2)],
            vec![rusqlite::types::Value::Integer(3)],
        ],
        "every committed document must stay searchable under cache churn"
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_create_persists_real_index_incarnation() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    // CREATE INDEX stages the v2 control row, which mints a real
    // incarnation so drop/recreate lifetimes are distinguishable.
    let stats = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(stats.storage_format_version, Some(2));
    assert!(
        stats
            .index_incarnation
            .is_some_and(|incarnation| incarnation != 0),
        "the persisted control record must carry a real index incarnation"
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_segment_registry_is_transactional() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    let created = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(created.storage_format_version, Some(2));
    assert_eq!(created.segment_count, Some(0));

    conn.execute("INSERT INTO docs VALUES (1, 'committed segment')")
        .unwrap();
    let committed = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(
        committed.segment_count,
        Some(1),
        "a committed write must publish its registry row"
    );
    assert_eq!(committed.index_incarnation, created.index_incarnation);

    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO docs VALUES (2, 'rolled back segment')")
        .unwrap();
    conn.execute("ROLLBACK").unwrap();
    let rolled_back = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(
        rolled_back.segment_count, committed.segment_count,
        "rollback must remove the transaction's registry rows"
    );
    assert_eq!(rolled_back.index_incarnation, created.index_incarnation);
    assert!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'rolled')").is_empty()
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_reuses_snapshot_within_one_read_transaction() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'same snapshot cache')")
        .unwrap();

    conn.execute("BEGIN").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'snapshot')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let after_first = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");

    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'snapshot')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let after_second = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    conn.execute("COMMIT").unwrap();

    assert_eq!(
        after_second.full_snapshot_loads, after_first.full_snapshot_loads,
        "the second read in one MVCC transaction must not rescan the directory"
    );
    // The stats probe itself opens a read cursor and scores one cache hit, so
    // require two: the probe's and the SELECT under test's. A plain `>` would
    // pass even with the SELECT deleted.
    assert!(
        after_second.read_cache_hits.unwrap() >= after_first.read_cache_hits.unwrap() + 2,
        "the second read must use the transaction-bound snapshot cache"
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_wal_reuses_segments_after_unrelated_commit() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE unrelated(value TEXT)").unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'stable manifest')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'stable')"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let before = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");

    conn.execute("INSERT INTO unrelated VALUES ('changes the WAL position')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'stable')"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let after = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");

    assert_eq!(
        after.full_snapshot_loads, before.full_snapshot_loads,
        "an unrelated commit must not reload the FTS segment files"
    );
    assert!(
        after.read_cache_hits > before.read_cache_hits,
        "the unchanged segment set must share the cached searcher across the commit"
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_reuses_segments_across_read_transactions() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'autocommit manifest')")
        .unwrap();
    conn.execute("BEGIN").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'autocommit')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let before = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    conn.execute("COMMIT").unwrap();

    conn.execute("BEGIN").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'autocommit')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let after = fts_attachment_test_stats(&tmp_db, &conn, "docs", "docs_fts");
    conn.execute("COMMIT").unwrap();

    assert_eq!(
        after.full_snapshot_loads, before.full_snapshot_loads,
        "a new MVCC read transaction must not reload an unchanged segment set"
    );
    assert!(
        after.read_cache_hits > before.read_cache_hits,
        "the new MVCC snapshot must share the cached searcher for the same segment set"
    );
}

#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_new_segment_set_invalidates_stale_searcher_once() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let writer = tmp_db.connect_limbo();
    let reader = tmp_db.connect_limbo();

    writer
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    writer
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (1, 'first generation')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &reader,
            "SELECT id FROM docs WHERE fts_match(body, 'first')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
    let before_write = fts_attachment_test_stats(&tmp_db, &reader, "docs", "docs_fts");

    writer
        .execute("INSERT INTO docs VALUES (2, 'second generation')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(
            &reader,
            "SELECT id FROM docs WHERE fts_match(body, 'second')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]],
        "the observer must reject its stale snapshot after the writer commits"
    );
    let after_write = fts_attachment_test_stats(&tmp_db, &reader, "docs", "docs_fts");
    assert!(
        after_write.read_cache_misses > before_write.read_cache_misses,
        "a changed segment set must miss the searcher cache"
    );
    // Pins that the SELECT — not the stats probe — performed the reload: the
    // probe after a successful reload scores a cache hit, while a probe that
    // had to do the reload itself would not. Without this, deleting the
    // SELECT above still satisfies the two counter assertions.
    assert!(
        after_write.read_cache_hits > before_write.read_cache_hits,
        "the stats probe after the reload must hit the refreshed cache"
    );

    assert_eq!(
        limbo_exec_rows(
            &reader,
            "SELECT id FROM docs WHERE fts_match(body, 'second')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
    let after_reuse = fts_attachment_test_stats(&tmp_db, &reader, "docs", "docs_fts");
    assert_eq!(
        after_reuse.full_snapshot_loads, after_write.full_snapshot_loads,
        "the newly loaded segment set must be reusable without another full scan"
    );
}

/// FTS read state belongs to the connection snapshot that populated it.
/// Sharing that state with another connection must not expose uncommitted index
/// maintenance from an active writer transaction.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_uncommitted_changes_are_connection_isolated() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let writer = tmp_db.connect_limbo();
    let observer = tmp_db.connect_limbo();

    writer
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();
    writer
        .execute("CREATE INDEX docs_fts ON docs USING fts(content)")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (10, 'charlie'), (13, 'charlie'), (20, 'unrelated')")
        .unwrap();

    let query = "SELECT id FROM docs WHERE fts_match(content, 'charlie') ORDER BY id";
    assert_eq!(
        limbo_exec_rows(&observer, query),
        vec![
            vec![rusqlite::types::Value::Integer(10)],
            vec![rusqlite::types::Value::Integer(13)],
        ]
    );
    assert_eq!(
        limbo_exec_rows(&writer, query),
        vec![
            vec![rusqlite::types::Value::Integer(10)],
            vec![rusqlite::types::Value::Integer(13)],
        ],
        "writer should warm its own cached read state before starting the transaction"
    );

    writer.execute("BEGIN").unwrap();
    writer
        .execute("UPDATE docs SET content = NULL WHERE id = 10")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (14, 'charlie')")
        .unwrap();

    assert_eq!(
        limbo_exec_rows(&writer, query),
        vec![
            vec![rusqlite::types::Value::Integer(13)],
            vec![rusqlite::types::Value::Integer(14)],
        ]
    );
    assert_eq!(
        limbo_exec_rows(&observer, query),
        vec![
            vec![rusqlite::types::Value::Integer(10)],
            vec![rusqlite::types::Value::Integer(13)],
        ],
        "observer must retain its committed FTS snapshot"
    );

    writer.execute("ROLLBACK").unwrap();
    assert_eq!(
        limbo_exec_rows(&writer, query),
        vec![
            vec![rusqlite::types::Value::Integer(10)],
            vec![rusqlite::types::Value::Integer(13)],
        ]
    );

    writer
        .execute("INSERT INTO docs VALUES (14, 'charlie')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&observer, query),
        vec![
            vec![rusqlite::types::Value::Integer(10)],
            vec![rusqlite::types::Value::Integer(13)],
            vec![rusqlite::types::Value::Integer(14)],
        ],
        "observer must discard its cached FTS state when the WAL snapshot advances"
    );
}

/// The searcher cache is keyed by the visible segment set, so any number of
/// connections reading the same committed index share one entry — the
/// per-connection cache ceiling of the v1 design is gone — and the byte
/// cache stays within its aggregate budget.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_read_cache_is_shared_across_connections_and_bounded() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let setup = tmp_db.connect_limbo();
    setup
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();
    setup
        .execute("CREATE INDEX docs_fts ON docs USING fts(content)")
        .unwrap();
    setup
        .execute("INSERT INTO docs VALUES (1, 'database document')")
        .unwrap();

    let attachment = FtsIndexMethod
        .attach(&IndexMethodConfiguration {
            table_name: "docs".to_string(),
            index_name: "docs_fts".to_string(),
            columns: vec![IndexColumn::new("content", 1)],
            parameters: HashMap::default(),
        })
        .unwrap();
    let readers = (0..5).map(|_| tmp_db.connect_limbo()).collect::<Vec<_>>();

    let mut hits_before = 0;
    for (round, reader) in readers.iter().enumerate() {
        let mut cursor = attachment.init().unwrap();
        run(&tmp_db, || {
            cursor.open_read(&index_method_context(reader, attachment.as_ref()))
        })
        .unwrap();
        let stats = cursor.test_stats().unwrap().unwrap();
        assert_eq!(
            stats.cached_connection_count,
            Some(1),
            "every reader of one segment set must share one cached searcher (round {round})"
        );
        // The probe attachment is fresh (its own byte cache), so the first
        // reader loads the one segment; every later reader is served from
        // the shared cache.
        assert_eq!(
            stats.full_snapshot_loads,
            Some(1),
            "only the first reader may load segment bytes (round {round})"
        );
        if round > 0 {
            assert!(
                stats.read_cache_hits.unwrap() > hits_before,
                "later readers must hit the shared searcher cache (round {round})"
            );
        }
        hits_before = stats.read_cache_hits.unwrap();
        assert!(
            stats.cached_bytes.unwrap() <= 192 * 1024 * 1024,
            "retained segment bytes exceeded the aggregate cache budget"
        );
    }
}

/// Unordered MATCH cursors stream from Tantivy. UPDATE and DELETE must first
/// collect their rowids so index maintenance cannot perturb the active scorer.
/// Regression test: under MVCC, an autocommit write that runs while a sibling
/// root statement is still open cannot commit at its own halt — it joins the
/// shared implicit transaction. That halt exit used to release the statement
/// savepoint without staging the FTS documents or handing the cursor to the
/// connection, so the base row committed while its index entry was silently
/// dropped (and the cursor's Drop tripped a debug assert).
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_deferred_autocommit_write_keeps_index_entries() {
    let tmp_db = TempDatabase::builder()
        .with_mvcc(true)
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("INSERT INTO docs VALUES (1, 'alpha')")
        .unwrap();

    // Statement A: step to the first row and hold it open.
    let mut reader = conn.prepare("SELECT id FROM docs").unwrap();
    loop {
        match reader.step().unwrap() {
            turso_core::StepResult::Row => break,
            turso_core::StepResult::IO => reader.get_pager().io.step().unwrap(),
            other => panic!("expected a row from the held-open reader, got {other:?}"),
        }
    }

    // Statement B on the same connection: its halt defers the commit to the
    // shared implicit transaction that statement A still holds open.
    conn.execute("INSERT INTO docs VALUES (2, 'bravo')")
        .unwrap();

    // Finish statement A so the shared transaction commits.
    loop {
        match reader.step().unwrap() {
            turso_core::StepResult::Row => {}
            turso_core::StepResult::IO => reader.get_pager().io.step().unwrap(),
            turso_core::StepResult::Done => break,
            other => panic!("expected the reader to finish, got {other:?}"),
        }
    }
    drop(reader);

    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs ORDER BY id"),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(2)],
        ]
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'bravo')"),
        vec![vec![rusqlite::types::Value::Integer(2)]],
        "the deferred autocommit write must keep its FTS index entry"
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_streaming_dml_collects_stable_rowids() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, content TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(content)")
        .unwrap();

    let values = (0..32)
        .map(|id| format!("({id}, 'common document {id}')"))
        .collect::<Vec<_>>()
        .join(",");
    conn.execute(format!("INSERT INTO docs VALUES {values}"))
        .unwrap();

    conn.execute(
        "UPDATE docs SET content = 'updated document' \
         WHERE fts_match(content, 'common')",
    )
    .unwrap();
    assert!(limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(content, 'common')"
    )
    .is_empty());
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(content, 'updated')"
        )
        .len(),
        32
    );

    conn.execute("DELETE FROM docs WHERE fts_match(content, 'updated')")
        .unwrap();
    assert!(limbo_exec_rows(&conn, "SELECT id FROM docs").is_empty());
    assert!(limbo_exec_rows(
        &conn,
        "SELECT id FROM docs WHERE fts_match(content, 'updated')"
    )
    .is_empty());
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_fk_cascade_delete_flush(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();
    conn.execute("PRAGMA foreign_keys=ON").unwrap();
    conn.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute(
        "CREATE TABLE docs(\
             id INTEGER PRIMARY KEY, \
             parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE, \
             title TEXT, \
             body TEXT)",
    )
    .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(title, body)")
        .unwrap();
    conn.execute("INSERT INTO parent VALUES (1), (2)").unwrap();
    conn.execute("INSERT INTO docs VALUES (10, 1, 'cascade', 'x'), (20, 2, 'keep', 'y')")
        .unwrap();
    conn.execute("DELETE FROM parent WHERE id = 1").unwrap();

    let remaining: Vec<(i64,)> = conn.exec_rows("SELECT id FROM docs ORDER BY id");
    assert_eq!(remaining, vec![(20,)]);

    let stale: Vec<(i64,)> =
        conn.exec_rows("SELECT id FROM docs WHERE fts_match(title, body, 'cascade') ORDER BY id");
    assert!(
        stale.is_empty(),
        "cascade-deleted row must be removed from the FTS index, got {stale:?}"
    );
    let alive: Vec<(i64,)> =
        conn.exec_rows("SELECT id FROM docs WHERE fts_match(title, body, 'keep') ORDER BY id");
    assert_eq!(alive, vec![(20,)]);
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_trigger_subprogram_flush(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE source(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE audit(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX audit_fts ON audit USING fts (body)")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER src_trigger AFTER INSERT ON source BEGIN \
             INSERT INTO audit(id, body) VALUES (NEW.id, NEW.body); \
         END",
    )
    .unwrap();

    // trigger fires once per row through the same cached subprogram
    // 'charlie' is the last fire, and is flushed at its own halt
    conn.execute("INSERT INTO source(id, body) VALUES (1, 'alpha'), (2, 'bravo'), (3, 'charlie')")
        .unwrap();

    for (term, expected_id) in [("alpha", 1), ("bravo", 2), ("charlie", 3)] {
        let ids: Vec<(i64,)> = conn.exec_rows(&format!(
            "SELECT id FROM audit WHERE fts_match(body, '{term}')"
        ));
        assert_eq!(
            ids,
            vec![(expected_id,)],
            "FTS index updated by the trigger should return id {expected_id} for '{term}'"
        );
    }
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_trigger_update_subprogram_flush(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE message(id INTEGER PRIMARY KEY, tag TEXT)")
        .unwrap();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts (body)")
        .unwrap();
    conn.execute("INSERT INTO docs(id, body) VALUES (1, 'original text')")
        .unwrap();
    conn.execute(
        "CREATE TRIGGER msg_update_trg AFTER UPDATE ON message BEGIN \
             UPDATE docs SET body = 'updated text' WHERE id = NEW.id; \
         END",
    )
    .unwrap();

    conn.execute("INSERT INTO message(id, tag) VALUES (1, 'aba')")
        .unwrap();

    // UPDATE fires trigger, which re-indexes doc 1 inside the subprogram (FTS delete + insert)
    conn.execute("UPDATE message SET tag = 'bab' WHERE id = 1")
        .unwrap();

    let updated: Vec<(i64,)> =
        conn.exec_rows("SELECT id FROM docs WHERE fts_match(body, 'updated')");
    assert_eq!(
        updated,
        vec![(1,)],
        "re-indexed doc must be found by its NEW term"
    );

    let stale: Vec<(i64,)> =
        conn.exec_rows("SELECT id FROM docs WHERE fts_match(body, 'original')");
    assert!(
        stale.is_empty(),
        "the OLD term must be gone from the FTS index after the in-subprogram UPDATE, got {stale:?}"
    );
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[turso_macros::test]
fn test_fts_trigger_abort_not_flushed(tmp_db: TempDatabase) {
    let _ = env_logger::try_init();
    let conn = tmp_db.connect_limbo();

    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    conn.execute("CREATE TABLE log(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX log_fts ON log USING fts (body)")
        .unwrap();
    conn.execute("CREATE TABLE uniq_t(x INTEGER UNIQUE)")
        .unwrap();
    conn.execute("INSERT INTO uniq_t(x) VALUES (1)").unwrap();

    // trigger buffers FTS doc, then violates UNIQUE -> subprogram aborts
    conn.execute(
        "CREATE TRIGGER t_trg AFTER INSERT ON t BEGIN \
             INSERT INTO log(id, body) VALUES (NEW.id, 'ghost'); \
             INSERT INTO uniq_t(x) VALUES (1); \
         END",
    )
    .unwrap();

    // insert must fail: trigger's UNIQUE violation aborts the whole statement
    let res = conn.execute("INSERT INTO t(id) VALUES (1)");
    assert!(
        res.is_err(),
        "expected trigger's UNIQUE violation to abort the insert"
    );

    // whole statement rolled back
    let t_rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM t");
    assert!(t_rows.is_empty(), "aborted top-level insert must roll back");

    let log_rows: Vec<(i64,)> = conn.exec_rows("SELECT id FROM log");
    assert!(log_rows.is_empty(), "aborted trigger insert must roll back");

    let ghost: Vec<(i64,)> = conn.exec_rows("SELECT id FROM log WHERE fts_match(body, 'ghost')");
    assert!(
        ghost.is_empty(),
        "aborted subprogram's FTS doc must not be indexed, got {ghost:?}"
    );
}

/// A database whose FTS index was written by the pre-registry
/// implementation (fixture created by tursodb v0.8.0-pre.7: a whole
/// Tantivy directory stored as `(path, chunk_no, bytes)` rows, including a
/// `.del` file from a DELETE and an UPDATE) is refused with a rebuild hint
/// on reads and writes. The base table stays readable, and
/// `DROP INDEX` + `CREATE INDEX` rebuilds the index from it.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
fn check_pre_registry_store_is_refused_until_rebuilt(mvcc: bool) {
    use rusqlite::types::Value::{Integer, Text};

    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("integration/index_method/fixtures/fts_pre_registry_v0.8.0-pre.7.db");
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let db_path = tmp_dir.path().join("pre_registry.db");
    std::fs::copy(&fixture, &db_path).unwrap();
    let tmp_db = TempDatabase::builder()
        .with_db_path(&db_path)
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();
    if mvcc {
        conn.pragma_update("journal_mode", "'mvcc'").unwrap();
    }

    // The fixture holds rows 1, 3, 4, 5, 6 (row 2 deleted, row 3 updated).
    // Opening the database, loading the catalog, and using anything other
    // than the old index must all work: the check lives in the cursor open
    // path, not in schema loading, so the database is never bricked.
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT name FROM sqlite_master WHERE name = 'docs_fts'"
        ),
        vec![vec![Text("docs_fts".to_string())]],
        "the catalog must load with the old index in it"
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT count(*) FROM docs"),
        vec![vec![Integer(5)]],
        "the base table must not depend on the index's storage format"
    );
    conn.execute("CREATE TABLE unrelated(x)").unwrap();
    conn.execute("INSERT INTO unrelated VALUES (1)").unwrap();
    let expect_refused = |err: turso_core::LimboError, what: &str| {
        let text = err.to_string();
        assert!(
            text.contains("older version of Turso") && text.contains("DROP INDEX docs_fts"),
            "{what} on a pre-registry store must ask for a rebuild, got: {text}"
        );
    };
    expect_refused(
        limbo_exec_rows_fallible(
            &tmp_db,
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'alpha')",
        )
        .unwrap_err(),
        "a query",
    );
    expect_refused(
        conn.execute("INSERT INTO docs VALUES (7, 'oscar papa')")
            .unwrap_err(),
        "a write",
    );
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT count(*) FROM docs"),
        vec![vec![Integer(5)]],
        "a refused write must not leave a base-table row behind"
    );

    // DROP INDEX never opens the store, so the rebuild always works, and
    // the table is writable again as soon as the old index is gone.
    conn.execute("DROP INDEX docs_fts").unwrap();
    conn.execute("INSERT INTO docs VALUES (7, 'oscar papa')")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();

    let ids = |term: &str| {
        limbo_exec_rows(
            &conn,
            &format!("SELECT id FROM docs WHERE fts_match(body, '{term}') ORDER BY id"),
        )
    };
    assert_eq!(ids("alpha"), vec![vec![Integer(1)]]);
    assert_eq!(
        ids("kilo"),
        vec![vec![Integer(3)]],
        "the rebuilt index reflects the fixture's UPDATE"
    );
    assert!(
        ids("charlie").is_empty(),
        "the rebuilt index reflects the fixture's DELETE"
    );
    assert!(
        ids("echo").is_empty(),
        "the pre-UPDATE posting must not come back"
    );
    assert_eq!(
        ids("oscar"),
        vec![vec![Integer(7)]],
        "the rebuild must index rows written while no index existed"
    );
    conn.execute("INSERT INTO docs VALUES (8, 'papa quebec')")
        .unwrap();
    assert_eq!(ids("quebec"), vec![vec![Integer(8)]]);
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_pre_registry_store_is_refused_until_rebuilt() {
    check_pre_registry_store_is_refused_until_rebuilt(false);
}

#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_pre_registry_store_is_refused_until_rebuilt_under_mvcc() {
    check_pre_registry_store_is_refused_until_rebuilt(true);
}

/// A long-running MVCC reader must see a frozen segment set while
/// concurrent transactions publish new segments and a merge retires the
/// ones it is reading — the retired registry rows stay visible to the old
/// snapshot through their version chains.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_long_reader_sees_frozen_segments_across_commits_and_merge() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let writer = tmp_db.connect_limbo();
    let reader = tmp_db.connect_limbo();

    writer
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    writer
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (1, 'stable alpha')")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (2, 'stable bravo')")
        .unwrap();

    let query = "SELECT id FROM docs WHERE fts_match(body, 'stable') ORDER BY id";
    let frozen = vec![
        vec![rusqlite::types::Value::Integer(1)],
        vec![rusqlite::types::Value::Integer(2)],
    ];

    reader.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(limbo_exec_rows(&reader, query), frozen);

    // Concurrent commits add a segment, tombstone a document, and a merge
    // retires every segment the reader is pinned to.
    writer
        .execute("INSERT INTO docs VALUES (3, 'stable charlie')")
        .unwrap();
    writer.execute("DELETE FROM docs WHERE id = 1").unwrap();
    writer.execute("OPTIMIZE INDEX docs_fts").unwrap();

    assert_eq!(
        limbo_exec_rows(&reader, query),
        frozen,
        "the pinned reader must keep seeing its snapshot's segment set"
    );
    reader.execute("COMMIT").unwrap();

    assert_eq!(
        limbo_exec_rows(&reader, query),
        vec![
            vec![rusqlite::types::Value::Integer(2)],
            vec![rusqlite::types::Value::Integer(3)],
        ],
        "a fresh snapshot sees the merged post-delete state"
    );
}

/// Repro of the CLI smoke-test failure: SELECT between UPDATE and DELETE,
/// then the same query after DELETE resurrects the updated row's old
/// posting.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_update_then_delete_with_interleaved_reads_keeps_tombstones() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute(
        "INSERT INTO docs VALUES (1, 'the quick brown fox'), (2, 'jumped over the lazy dog'), (3, 'quick reflexes')",
    )
    .unwrap();
    let quick = "SELECT id FROM docs WHERE fts_match(body, 'quick') ORDER BY id";
    assert_eq!(
        limbo_exec_rows(&conn, quick),
        vec![
            vec![rusqlite::types::Value::Integer(1)],
            vec![rusqlite::types::Value::Integer(3)],
        ]
    );
    conn.execute("UPDATE docs SET body = 'slow green turtle' WHERE id = 1")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, quick),
        vec![vec![rusqlite::types::Value::Integer(3)]]
    );
    conn.execute("DELETE FROM docs WHERE id = 3").unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, quick),
        Vec::<Vec<rusqlite::types::Value>>::new(),
        "both quick postings are dead: one tombstoned by the UPDATE, one by the DELETE"
    );
}

/// Two concurrent OPTIMIZE transactions on one index. Registry-row deletes
/// get no engine-level commit validation (non-unique index keys skip
/// `check_index_for_conflicts`), so the merge mutex is the ONLY thing
/// standing between two merges that each retire the same descriptor rows.
/// If both ran, each would publish its own merged segment over the same
/// inputs and every document would match twice.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_concurrent_optimize_refused_by_merge_mutex() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();

    first
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for id in 0..8 {
        first
            .execute(format!("INSERT INTO docs VALUES ({id}, 'common doc {id}')"))
            .unwrap();
    }
    assert_eq!(
        fts_stats_in_txn(&tmp_db, &first, "docs", "docs_fts").segment_count,
        Some(8)
    );

    first.execute("BEGIN CONCURRENT").unwrap();
    first.execute("OPTIMIZE INDEX docs_fts").unwrap();

    // The merge mutex must refuse the overlapping merge outright.
    second.execute("BEGIN CONCURRENT").unwrap();
    let refused = second.execute("OPTIMIZE INDEX docs_fts");
    assert!(
        matches!(
            refused,
            Err(turso_core::LimboError::Busy | turso_core::LimboError::WriteWriteConflict)
        ),
        "second concurrent OPTIMIZE must be refused by the merge mutex, got: {refused:?}"
    );
    second.execute("ROLLBACK").unwrap();
    first.execute("COMMIT").unwrap();

    // Exactly one merged segment; every document matches exactly once.
    let third = tmp_db.connect_limbo();
    assert_eq!(
        fts_stats_in_txn(&tmp_db, &third, "docs", "docs_fts").segment_count,
        Some(1),
        "exactly one merge may retire the eight input segments"
    );
    assert_eq!(
        limbo_exec_rows(
            &third,
            "SELECT id FROM docs WHERE fts_match(body, 'common') ORDER BY id"
        )
        .len(),
        8,
        "each document must match exactly once after the merge"
    );

    // Once the winner has committed, a fresh transaction can merge again.
    second.execute("OPTIMIZE INDEX docs_fts").unwrap();
}

/// The §11 claim: the merge mutex survives checkpoints. Checkpoint GC can
/// erase the version-chain evidence the eager delete-conflict check needs,
/// so if the lease's staleness refusal depended on version chains, a
/// checkpoint between the two merges would let the second one through.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_optimize_vs_optimize_across_checkpoint() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let first = tmp_db.connect_limbo();
    let second = tmp_db.connect_limbo();

    first
        .execute("PRAGMA mvcc_checkpoint_threshold = 0")
        .unwrap();
    first
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    first
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for id in 0..6 {
        first
            .execute(format!("INSERT INTO docs VALUES ({id}, 'common doc {id}')"))
            .unwrap();
    }

    // `second` pins its snapshot before the merge publishes.
    second.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &second,
            "SELECT id FROM docs WHERE fts_match(body, 'common')"
        )
        .len(),
        6
    );

    // The merge commits and is checkpointed immediately (threshold 0).
    first.execute("OPTIMIZE INDEX docs_fts").unwrap();

    // A merge from the still-open older snapshot must be refused: its
    // snapshot predates the last publish, and merging a superseded segment
    // set would resurrect the retired descriptors.
    let refused = second.execute("OPTIMIZE INDEX docs_fts");
    assert!(
        matches!(
            refused,
            Err(turso_core::LimboError::Busy | turso_core::LimboError::WriteWriteConflict)
        ),
        "stale-snapshot OPTIMIZE must be refused even after a checkpoint, got: {refused:?}"
    );
    // The WriteWriteConflict refusal aborts the transaction outright, while
    // a Busy refusal leaves it open — accept either termination state.
    let _ = second.execute("ROLLBACK");

    let third = tmp_db.connect_limbo();
    assert_eq!(
        fts_stats_in_txn(&tmp_db, &third, "docs", "docs_fts").segment_count,
        Some(1)
    );
    assert_eq!(
        limbo_exec_rows(
            &third,
            "SELECT id FROM docs WHERE fts_match(body, 'common') ORDER BY id"
        )
        .len(),
        6,
        "each document must match exactly once after the checkpointed merge"
    );
    third
        .execute("INSERT INTO docs VALUES (100, 'still writable')")
        .unwrap();
}

/// A merge and a plain writer overlap; both must commit in either commit
/// order. The merge deletes the old descriptor rows while the writer appends
/// a disjoint new one — a false conflict here would reintroduce the old
/// single-writer ceiling, and a lost segment would silently drop documents.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_writer_and_merge_commit_concurrently() {
    for merge_commits_first in [true, false] {
        let tmp_db = TempDatabase::builder()
            .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
            .with_mvcc(true)
            .build();
        let writer = tmp_db.connect_limbo();
        let merger = tmp_db.connect_limbo();

        writer
            .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        writer
            .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
            .unwrap();
        for id in 0..5 {
            writer
                .execute(format!("INSERT INTO docs VALUES ({id}, 'common doc {id}')"))
                .unwrap();
        }

        writer.execute("BEGIN CONCURRENT").unwrap();
        writer
            .execute("INSERT INTO docs VALUES (100, 'common fresh writer doc')")
            .unwrap();

        merger.execute("BEGIN CONCURRENT").unwrap();
        merger.execute("OPTIMIZE INDEX docs_fts").unwrap();

        if merge_commits_first {
            merger.execute("COMMIT").unwrap();
            writer.execute("COMMIT").unwrap_or_else(|e| {
                panic!("writer appending a disjoint segment must not conflict with the merge: {e}")
            });
        } else {
            writer.execute("COMMIT").unwrap();
            merger.execute("COMMIT").unwrap_or_else(|e| {
                panic!("merge of pre-existing segments must not conflict with the writer: {e}")
            });
        }

        let third = tmp_db.connect_limbo();
        assert_eq!(
            limbo_exec_rows(
                &third,
                "SELECT id FROM docs WHERE fts_match(body, 'common') ORDER BY id"
            )
            .len(),
            6,
            "all six documents must match exactly once (merge_commits_first={merge_commits_first})"
        );
        assert_eq!(
            limbo_exec_rows(
                &third,
                "SELECT id FROM docs WHERE fts_match(body, 'fresh') ORDER BY id"
            ),
            vec![vec![rusqlite::types::Value::Integer(100)]],
            "the concurrent writer's segment must survive the merge (merge_commits_first={merge_commits_first})"
        );
        assert_eq!(
            fts_stats_in_txn(&tmp_db, &third, "docs", "docs_fts").segment_count,
            Some(2),
            "merged segment plus the writer's segment (merge_commits_first={merge_commits_first})"
        );
    }
}

/// §8 row 2: two concurrent transactions touching the same row resolve by
/// first-committer-wins on the base row, and the loser's tombstones and
/// segments leave no trace — in memory or on disk after a checkpoint.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_same_row_writers_conflict_and_leave_index_clean() {
    for (loser_sql, loser_token) in [
        ("UPDATE docs SET body = 'beta loser' WHERE id = 1", "loser"),
        ("DELETE FROM docs WHERE id = 1", ""),
    ] {
        let tmp_db = TempDatabase::builder()
            .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
            .with_mvcc(true)
            .build();
        let first = tmp_db.connect_limbo();
        let second = tmp_db.connect_limbo();

        first
            .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        first
            .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
            .unwrap();
        first
            .execute("INSERT INTO docs VALUES (1, 'original token')")
            .unwrap();

        first.execute("BEGIN CONCURRENT").unwrap();
        second.execute("BEGIN CONCURRENT").unwrap();
        first
            .execute("UPDATE docs SET body = 'alpha winner' WHERE id = 1")
            .unwrap();

        // The loser may be refused eagerly at the statement or at COMMIT;
        // silent success of both is the corruption case.
        let mut lost = second.execute(loser_sql).is_err();
        first.execute("COMMIT").unwrap();
        if !lost {
            lost = second.execute("COMMIT").is_err();
        }
        assert!(
            lost,
            "second writer of the same row must lose first-committer-wins ({loser_sql})"
        );
        if second.execute("ROLLBACK").is_err() {
            // The failed COMMIT may already have rolled the transaction back.
        }

        let third = tmp_db.connect_limbo();
        let assert_clean = |conn: &std::sync::Arc<turso_core::Connection>| {
            assert_eq!(
                limbo_exec_rows(conn, "SELECT id FROM docs WHERE fts_match(body, 'winner')"),
                vec![vec![rusqlite::types::Value::Integer(1)]],
                "the winner's posting must be searchable"
            );
            assert!(
                limbo_exec_rows(
                    conn,
                    "SELECT id FROM docs WHERE fts_match(body, 'original')"
                )
                .is_empty(),
                "the winner's tombstone must kill the original posting"
            );
            if !loser_token.is_empty() {
                assert!(
                    limbo_exec_rows(
                        conn,
                        &format!("SELECT id FROM docs WHERE fts_match(body, '{loser_token}')")
                    )
                    .is_empty(),
                    "the loser's segment must never become visible"
                );
            }
        };
        assert_clean(&third);

        // Nothing half-published from the loser may survive to disk.
        third.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let reopened = tmp_db.connect_limbo();
        assert_clean(&reopened);
    }
}

/// Delete every document, then OPTIMIZE. The Tantivy merger silently drops
/// input segments whose meta claims zero docs, and a merge whose inputs are
/// fully tombstoned produces an empty output — neither may publish a
/// zero-doc descriptor that poisons later merges, and the index must stay
/// fully usable afterwards.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[turso_macros::test(mvcc)]
fn fts_optimize_after_deleting_everything(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for id in 0..6 {
        conn.execute(format!("INSERT INTO docs VALUES ({id}, 'common doc {id}')"))
            .unwrap();
    }
    conn.execute("DELETE FROM docs").unwrap();
    assert!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'common')").is_empty()
    );
    assert!(limbo_exec_rows(
        &conn,
        "SELECT fts_score(body, 'common') FROM docs WHERE fts_match(body, 'common')"
    )
    .is_empty());

    conn.execute("OPTIMIZE INDEX docs_fts").unwrap();
    assert!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'common')").is_empty(),
        "tombstoned postings must not resurrect through the merge"
    );

    // Same-transaction insert-then-delete: the writer kills postings in its
    // own private, not-yet-published segment.
    conn.execute("BEGIN").unwrap();
    conn.execute("INSERT INTO docs VALUES (7, 'common ephemeral')")
        .unwrap();
    conn.execute("DELETE FROM docs WHERE id = 7").unwrap();
    conn.execute("COMMIT").unwrap();
    assert!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'common')").is_empty()
    );
    conn.execute("OPTIMIZE INDEX docs_fts").unwrap();

    // The index must remain fully usable.
    conn.execute("INSERT INTO docs VALUES (8, 'common survivor')")
        .unwrap();
    assert_eq!(
        limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'common')"),
        vec![vec![rusqlite::types::Value::Integer(8)]]
    );
    conn.execute("OPTIMIZE INDEX docs_fts").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT id FROM docs WHERE fts_match(body, 'survivor')"
        ),
        vec![vec![rusqlite::types::Value::Integer(8)]]
    );
    let stats = fts_stats_in_txn(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(
        stats.segment_count,
        Some(1),
        "empty segments must be compacted away, leaving only the survivor's"
    );
}

/// BM25 statistics are per-Searcher over the snapshot's segment set: a
/// pinned reader's scores must be bit-stable across concurrent commits and
/// merges, and the score (TopDocs) path must honor tombstones end-to-end.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_scores_stable_within_snapshot_and_adapt_across() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let reader = tmp_db.connect_limbo();
    let writer = tmp_db.connect_limbo();

    writer
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    writer
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for (id, extra) in [(1, "apple"), (2, "banana"), (3, "cherry"), (4, "date")] {
        writer
            .execute(format!("INSERT INTO docs VALUES ({id}, 'shared {extra}')"))
            .unwrap();
    }

    let score_query = "SELECT id, fts_score(body, 'shared') AS s FROM docs \
                       WHERE fts_match(body, 'shared') ORDER BY s DESC, id ASC";
    reader.execute("BEGIN CONCURRENT").unwrap();
    let before = limbo_exec_rows(&reader, score_query);
    assert_eq!(before.len(), 4);

    // Concurrent churn: more `shared` docs, a delete, and a merge.
    for id in 10..30 {
        writer
            .execute(format!(
                "INSERT INTO docs VALUES ({id}, 'shared filler {id}')"
            ))
            .unwrap();
    }
    writer.execute("DELETE FROM docs WHERE id = 2").unwrap();
    writer.execute("OPTIMIZE INDEX docs_fts").unwrap();

    let during = limbo_exec_rows(&reader, score_query);
    assert_eq!(
        during, before,
        "a pinned snapshot's scores and order must be frozen across concurrent commits and merges"
    );
    reader.execute("COMMIT").unwrap();

    let after = limbo_exec_rows(&reader, score_query);
    assert_eq!(
        after.len(),
        23,
        "fresh snapshot: 4 original - 1 deleted + 20 filler"
    );
    assert!(
        !after
            .iter()
            .any(|row| row[0] == rusqlite::types::Value::Integer(2)),
        "the tombstoned document must not appear on the score path"
    );
    assert_ne!(
        after[..4],
        before[..],
        "scores must adapt to the new snapshot's statistics"
    );
}

/// DROP INDEX racing an open concurrent FTS writer: whichever side is
/// refused, the database must end in a consistent state — never Corrupt,
/// and a rebuilt index must reflect exactly the committed rows.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_drop_index_vs_concurrent_writer_stays_consistent() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let writer = tmp_db.connect_limbo();
    let dropper = tmp_db.connect_limbo();

    writer
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    writer
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    writer
        .execute("INSERT INTO docs VALUES (1, 'committed base doc')")
        .unwrap();

    writer.execute("BEGIN CONCURRENT").unwrap();
    writer
        .execute("INSERT INTO docs VALUES (2, 'racing drop doc')")
        .unwrap();

    let drop_result = dropper.execute("DROP INDEX docs_fts");
    let commit_result = writer.execute("COMMIT");
    // Exactly one side must win: both succeeding means the writer published
    // segment rows into an index that no longer exists. Which side loses is
    // implementation-defined (today: the DROP wins and the writer's COMMIT
    // is refused with SchemaConflict).
    assert!(
        drop_result.is_err() || commit_result.is_err(),
        "DROP INDEX and a concurrent FTS writer must not both succeed: \
         drop={drop_result:?} commit={commit_result:?}"
    );
    let _ = writer.execute("ROLLBACK");

    // Whatever happened, the database must stay consistent and rebuildable.
    let fresh = tmp_db.connect_limbo();
    let _ = fresh.execute("DROP INDEX docs_fts");
    fresh
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    let expected_rows = limbo_exec_rows(&fresh, "SELECT id FROM docs ORDER BY id");
    let matched = limbo_exec_rows(
        &fresh,
        "SELECT id FROM docs WHERE fts_match(body, 'doc') ORDER BY id",
    );
    assert_eq!(
        matched, expected_rows,
        "a rebuilt index must reflect exactly the committed base rows"
    );
}

/// A tombstone writer whose snapshot predates a committed merge must be
/// refused. Its visible segment set is the pre-merge one, so its tombstones
/// would target retired segments — and the "deleted" postings would
/// resurrect through the merged segment after both commit.
///
/// Reduced from `fts_mvcc_concurrent_writers_model_fuzz` seed 8919.
#[cfg(all(feature = "fts", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_stale_snapshot_deleter_refused_after_merge() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let writer = tmp_db.connect_limbo();
    let merger = tmp_db.connect_limbo();

    writer
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    writer
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for id in 0..4 {
        writer
            .execute(format!("INSERT INTO docs VALUES ({id}, 'stale doc {id}')"))
            .unwrap();
    }

    // Pin the writer's snapshot before the merge, then merge and commit.
    writer.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &writer,
            "SELECT id FROM docs WHERE fts_match(body, 'stale')"
        )
        .len(),
        4
    );
    merger.execute("OPTIMIZE INDEX docs_fts").unwrap();

    // The writer's UPDATE tombstones postings in segments the merge just
    // retired; it must be refused rather than allowed to publish tombstones
    // no future reader will apply.
    let refused = writer.execute("UPDATE docs SET body = 'fresh doc' WHERE id = 1");
    let lost = refused.is_err() || writer.execute("COMMIT").is_err();
    assert!(
        lost,
        "a stale-snapshot tombstone writer must lose against a committed merge, got: {refused:?}"
    );
    let _ = writer.execute("ROLLBACK");

    // Retried at a fresh snapshot, the update succeeds — and the old posting
    // must be gone everywhere, not resurrected by the merged segment.
    writer
        .execute("UPDATE docs SET body = 'fresh doc' WHERE id = 1")
        .unwrap();
    let fresh = tmp_db.connect_limbo();
    assert_eq!(
        limbo_exec_rows(
            &fresh,
            "SELECT id FROM docs WHERE fts_match(body, 'stale') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(0)],
            vec![rusqlite::types::Value::Integer(2)],
            vec![rusqlite::types::Value::Integer(3)],
        ],
        "id 1's old posting must not survive the update"
    );
    assert_eq!(
        limbo_exec_rows(&fresh, "SELECT id FROM docs WHERE fts_match(body, 'fresh')"),
        vec![vec![rusqlite::types::Value::Integer(1)]]
    );
}

// ============ MVCC merge lease vs. tombstone writers ============

/// An uncommitted DELETE registers its transaction as a tombstone writer.
/// A merge that starts while that deleter is still active must be refused
/// (Busy): committing it would retire the segment the deleter's tombstone
/// targets and resurrect the deleted posting in the merged segment.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_active_deleter_blocks_merge() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let deleter = tmp_db.connect_limbo();
    let merger = tmp_db.connect_limbo();

    deleter
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    deleter
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for id in 0..4 {
        deleter
            .execute(format!("INSERT INTO docs VALUES ({id}, 'common doc {id}')"))
            .unwrap();
    }

    deleter.execute("BEGIN CONCURRENT").unwrap();
    deleter.execute("DELETE FROM docs WHERE id = 1").unwrap();

    merger.execute("BEGIN CONCURRENT").unwrap();
    let refused = merger.execute("OPTIMIZE INDEX docs_fts");
    assert!(
        matches!(refused, Err(turso_core::LimboError::Busy)),
        "merge overlapping an active deleter must be Busy, got: {refused:?}"
    );
    merger.execute("ROLLBACK").unwrap();

    deleter.execute("COMMIT").unwrap();

    // With the deleter committed, a fresh-snapshot merge succeeds and the
    // deleted posting must not come back.
    merger.execute("OPTIMIZE INDEX docs_fts").unwrap();
    let reader = tmp_db.connect_limbo();
    assert_eq!(
        limbo_exec_rows(
            &reader,
            "SELECT id FROM docs WHERE fts_match(body, 'common') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(0)],
            vec![rusqlite::types::Value::Integer(2)],
            vec![rusqlite::types::Value::Integer(3)],
        ],
    );
    assert_eq!(
        fts_stats_in_txn(&tmp_db, &reader, "docs", "docs_fts").segment_count,
        Some(1)
    );
}

/// The merger pins its snapshot (a read), then a deleter commits. The merge
/// at the stale snapshot cannot see the tombstone; letting it commit would
/// drop the tombstone with the retired segment. Must be refused.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_merge_at_stale_snapshot_refused_after_delete_commit() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let deleter = tmp_db.connect_limbo();
    let merger = tmp_db.connect_limbo();

    deleter
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    deleter
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for id in 0..4 {
        deleter
            .execute(format!("INSERT INTO docs VALUES ({id}, 'common doc {id}')"))
            .unwrap();
    }

    merger.execute("BEGIN CONCURRENT").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &merger,
            "SELECT id FROM docs WHERE fts_match(body, 'common')"
        )
        .len(),
        4
    );

    deleter.execute("DELETE FROM docs WHERE id = 1").unwrap();

    let refused = merger.execute("OPTIMIZE INDEX docs_fts");
    let lost = refused.is_err() || merger.execute("COMMIT").is_err();
    assert!(
        lost,
        "a merge whose snapshot predates a committed delete must be refused, got: {refused:?}"
    );
    let _ = merger.execute("ROLLBACK");

    let reader = tmp_db.connect_limbo();
    assert_eq!(
        limbo_exec_rows(
            &reader,
            "SELECT id FROM docs WHERE fts_match(body, 'common') ORDER BY id"
        ),
        vec![
            vec![rusqlite::types::Value::Integer(0)],
            vec![rusqlite::types::Value::Integer(2)],
            vec![rusqlite::types::Value::Integer(3)],
        ],
    );
    // A fresh merge sees the tombstone and compacts it away.
    merger.execute("OPTIMIZE INDEX docs_fts").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &reader,
            "SELECT id FROM docs WHERE fts_match(body, 'common') ORDER BY id"
        )
        .len(),
        3
    );
}

/// A merge in flight holds the lease; a deleter arriving while it is held
/// must be refused (Busy), and succeed once retried at a fresh snapshot.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_in_flight_merge_blocks_deleter() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let deleter = tmp_db.connect_limbo();
    let merger = tmp_db.connect_limbo();

    deleter
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    deleter
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for id in 0..4 {
        deleter
            .execute(format!("INSERT INTO docs VALUES ({id}, 'common doc {id}')"))
            .unwrap();
    }

    merger.execute("BEGIN CONCURRENT").unwrap();
    merger.execute("OPTIMIZE INDEX docs_fts").unwrap();

    deleter.execute("BEGIN CONCURRENT").unwrap();
    let refused = deleter.execute("DELETE FROM docs WHERE id = 1");
    assert!(
        matches!(refused, Err(turso_core::LimboError::Busy)),
        "delete overlapping an in-flight merge must be Busy, got: {refused:?}"
    );
    deleter.execute("ROLLBACK").unwrap();
    merger.execute("COMMIT").unwrap();

    deleter.execute("DELETE FROM docs WHERE id = 1").unwrap();
    let reader = tmp_db.connect_limbo();
    assert_eq!(
        limbo_exec_rows(
            &reader,
            "SELECT id FROM docs WHERE fts_match(body, 'common') ORDER BY id"
        )
        .len(),
        3
    );
}

/// Two transactions each delete a different row from the same segment and
/// commit concurrently: neither blocks the other and both tombstones land.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_mvcc_concurrent_deleters_do_not_serialize() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let a = tmp_db.connect_limbo();
    let b = tmp_db.connect_limbo();

    a.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    a.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    a.execute("INSERT INTO docs VALUES (1, 'common one'), (2, 'common two'), (3, 'common three')")
        .unwrap();

    a.execute("BEGIN CONCURRENT").unwrap();
    b.execute("BEGIN CONCURRENT").unwrap();
    a.execute("DELETE FROM docs WHERE id = 1").unwrap();
    b.execute("DELETE FROM docs WHERE id = 2").unwrap();
    a.execute("COMMIT").unwrap();
    b.execute("COMMIT").unwrap();

    let reader = tmp_db.connect_limbo();
    assert_eq!(
        limbo_exec_rows(
            &reader,
            "SELECT id FROM docs WHERE fts_match(body, 'common')"
        ),
        vec![vec![rusqlite::types::Value::Integer(3)]],
    );
    reader.execute("OPTIMIZE INDEX docs_fts").unwrap();
    assert_eq!(
        limbo_exec_rows(
            &reader,
            "SELECT id FROM docs WHERE fts_match(body, 'common')"
        ),
        vec![vec![rusqlite::types::Value::Integer(3)]],
    );
}

/// A backing store that exists but holds no control row (the state left by
/// recreating the internal table behind the index's back) reads as an empty
/// index but must refuse writes: segments appended without a control row
/// would make every later open fail with a corruption error.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_store_without_control_row_refuses_writes_and_reads_empty() {
    for mvcc in [false, true] {
        let tmp_db = TempDatabase::builder()
            .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
            .with_mvcc(mvcc)
            .build();
        let conn = tmp_db.connect_limbo();
        conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
            .unwrap();
        conn.execute("INSERT INTO docs VALUES (1, 'alpha')")
            .unwrap();

        conn.execute("CREATE TABLE touch(x)").unwrap();

        // Under MVCC, BEGIN starts the transaction lazily; the wiper needs a
        // live write transaction, so touch an unrelated table first.
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO touch VALUES (1)").unwrap();
        let mut wiper =
            turso_core::index_method::fts::FtsBackingRowWiper::new(&conn, MAIN_DB_ID, "docs_fts")
                .unwrap();
        run(&tmp_db, || wiper.step()).unwrap();
        drop(wiper);
        conn.execute("COMMIT").unwrap();

        assert!(
            limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'alpha')").is_empty(),
            "a control-less store reads as an empty index (mvcc={mvcc})"
        );
        let refused = conn.execute("INSERT INTO docs VALUES (2, 'beta')");
        assert!(
            matches!(refused, Err(turso_core::LimboError::Corrupt(_))),
            "a write to a control-less store must be refused, got: {refused:?} (mvcc={mvcc})"
        );
        // Nothing was appended, so the index still opens and reads as empty.
        assert!(
            limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'beta')").is_empty(),
            "the refused write must leave the store readable (mvcc={mvcc})"
        );

        // The rebuild the error message asks for works.
        conn.execute("DROP INDEX docs_fts").unwrap();
        conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
            .unwrap();
        conn.execute("INSERT INTO docs VALUES (2, 'beta')").unwrap();
        assert_eq!(
            limbo_exec_rows(&conn, "SELECT id FROM docs WHERE fts_match(body, 'beta')"),
            vec![vec![rusqlite::types::Value::Integer(2)]],
            "the rebuilt index is writable and searchable (mvcc={mvcc})"
        );
    }
}

/// Worktree-local debug tool: dump the raw FTS backing rows of the whopper
/// database named by FTS_DUMP_DB, list physical duplicate keys, and re-run
/// the fts_match/base-scan differential per token on a fresh (disk-truth)
/// open. Does nothing without the env var.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_dump_backing_rows_tool() {
    let Some(path) = std::env::var_os("FTS_DUMP_DB") else {
        return;
    };
    let tmp_db = TempDatabase::builder()
        .with_db_path(std::path::Path::new(&path))
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();

    conn.execute("BEGIN").unwrap();
    let _ = limbo_exec_rows(&conn, "SELECT count(*) FROM fts_docs");
    let mut dumper =
        turso_core::index_method::fts::FtsBackingRowDumper::new(&conn, MAIN_DB_ID, "fts_docs_fts")
            .unwrap();
    run(&tmp_db, || dumper.step()).unwrap();
    let rows = std::mem::take(&mut dumper.rows);
    drop(dumper);
    conn.execute("ROLLBACK").unwrap();

    println!("total backing rows: {}", rows.len());
    let mut counts: std::collections::BTreeMap<&str, usize> = Default::default();
    for (path, _, _, _) in &rows {
        let kind = if path.starts_with("fts2/seg/") {
            "seg"
        } else if path.starts_with("fts2/chunk/") {
            "chunk"
        } else if path.starts_with("fts2/tomb/") {
            "tomb"
        } else {
            "other"
        };
        *counts.entry(kind).or_default() += 1;
    }
    println!("row kinds: {counts:?}");
    for window in rows.windows(2) {
        let (ap, ac, al, ah) = &window[0];
        let (bp, bc, bl, bh) = &window[1];
        if ap == bp && ac == bc {
            println!(
                "DUPLICATE KEY: path={ap} chunk_no={ac} len_a={al} hash_a={ah:016x} \
                 len_b={bl} hash_b={bh:016x} identical={}",
                al == bl && ah == bh
            );
        }
    }
    for (path, chunk_no, len, _) in &rows {
        if path.starts_with("fts2/seg/") || path.starts_with("fts2/control") {
            println!("row: {path} chunk_no={chunk_no} len={len}");
        }
    }
    let tomb_counts: std::collections::BTreeMap<String, usize> = rows
        .iter()
        .filter(|(p, _, _, _)| p.starts_with("fts2/tomb/"))
        .fold(Default::default(), |mut m, (p, _, _, _)| {
            *m.entry(p.clone()).or_default() += 1;
            m
        });
    println!("tombstone rows per segment: {tomb_counts:?}");

    for token in [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
    ] {
        let sql = format!(
            "SELECT \
               (SELECT group_concat(id) FROM (\
                  SELECT id FROM fts_docs WHERE fts_match(body, '{token}') \
                  EXCEPT \
                  SELECT id FROM fts_docs WHERE (' '||body||' ') LIKE '% {token} %')), \
               (SELECT group_concat(id) FROM (\
                  SELECT id FROM fts_docs WHERE (' '||body||' ') LIKE '% {token} %' \
                  EXCEPT \
                  SELECT id FROM fts_docs WHERE fts_match(body, '{token}')))"
        );
        let rows = limbo_exec_rows(&conn, &sql);
        println!("differential {token}: {rows:?}");
    }
}

/// Deterministic reproduction of the multiprocess torn-read anomaly the
/// whopper FTS differential caught (worktree-local; asserts CONSISTENCY, so
/// it is RED while the bug exists).
///
/// Mechanism under test: `Pager::read_page`'s cache-hit fast path returns
/// shared-cache pages with no per-snapshot validation. The cache is shared
/// by every connection of one Database (one "process"), while commits from
/// another Database over the same file (another "process") do not write
/// through it. A connection holding an old read snapshot legally re-loads
/// old-version pages into the shared cache after a sibling's
/// changed-detection cleared it; a fresh-snapshot sibling then cache-hits a
/// MIXTURE of old and new pages inside one statement.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn multiprocess_shared_cache_serves_snapshot_consistent_pages() {
    use turso_core::{Database, DatabaseOpts, OpenFlags, SqliteDialect};
    // Multiprocess WAL needs an IO backend with shared-memory coordination.
    // The default platform IO has it on unix but not on Windows (only the
    // IOCP backend does there), and the open below refuses without it.
    {
        let probe: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        if !probe.supports_shared_wal_coordination() {
            println!("skipping: platform IO does not support multiprocess WAL");
            return;
        }
    }
    let tmp_dir = tempfile::TempDir::new().unwrap();
    let path = tmp_dir.path().join("torn.db");
    let path = path.to_str().unwrap();
    let open = || {
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new().unwrap());
        Database::open_file_with_flags(
            io,
            path,
            OpenFlags::default(),
            DatabaseOpts::new()
                .with_multiprocess_wal(true)
                .with_index_method(true),
            None,
            Arc::new(SqliteDialect),
        )
        .unwrap()
    };
    let db1 = open();
    let db2 = open();

    let setup = db1.connect().unwrap();
    setup
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    setup
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for id in 0..200 {
        setup
            .execute(format!(
                "INSERT INTO docs VALUES ({id}, 'alpha filler{id}')"
            ))
            .unwrap();
    }

    let run_rows = |conn: &Arc<turso_core::Connection>, sql: &str| -> Vec<Vec<turso_core::Value>> {
        let mut stmt = conn.prepare(sql).unwrap();
        let mut rows = Vec::new();
        loop {
            match stmt.step().unwrap() {
                turso_core::StepResult::Row => {
                    rows.push(stmt.row().unwrap().get_values().cloned().collect());
                }
                turso_core::StepResult::IO => {
                    stmt._io().step().unwrap();
                }
                turso_core::StepResult::Done => break,
                r => panic!("unexpected step result {r:?}"),
            }
        }
        rows
    };

    // Step 1: "process" 1, connection X pins an old snapshot and warms the
    // shared cache with FTS backing pages at that snapshot.
    let conn_x = db1.connect().unwrap();
    conn_x.execute("BEGIN").unwrap();
    let _ = run_rows(
        &conn_x,
        "SELECT count(*) FROM docs WHERE fts_match(body, 'alpha')",
    );

    // Step 2: "process" 2 commits FTS churn — deletes make old postings
    // dead (tombstones) and add new ones. Process 1's cache sees none of it.
    let writer = db2.connect().unwrap();
    for id in 0..50 {
        writer
            .execute(format!(
                "UPDATE docs SET body = 'bravo fresh{id}' WHERE id = {id}"
            ))
            .unwrap();
    }

    // Step 3: process 1, connection Z begins a fresh snapshot: its
    // changed-detection clears the shared cache and loads some NEW pages.
    let conn_z = db1.connect().unwrap();
    let _ = run_rows(&conn_z, "SELECT count(*) FROM docs WHERE id < 10");

    // Step 4: connection X (still pinned at the OLD snapshot) runs another
    // FTS statement: its loads are bounded by its own snapshot and re-fill
    // the shared cache with OLD-version FTS pages.
    let _ = run_rows(
        &conn_x,
        "SELECT count(*) FROM docs WHERE fts_match(body, 'alpha')",
    );
    conn_x.execute("COMMIT").unwrap();

    // Step 5: a fresh-snapshot statement on process 1 must see a
    // self-consistent view: fts_match and the padded-LIKE base scan agree.
    // `conn_z`'s last-seen snapshot is already current, so its begin skips
    // the cache clear — it reads whatever mixture the shared cache holds.
    let conn_y = conn_z;
    for token in ["alpha", "bravo"] {
        let rows = run_rows(
            &conn_y,
            &format!(
                "SELECT \
                   (SELECT count(*) FROM (\
                      SELECT id FROM docs WHERE fts_match(body, '{token}') \
                      EXCEPT \
                      SELECT id FROM docs WHERE (' '||body||' ') LIKE '% {token} %')) \
                 + (SELECT count(*) FROM (\
                      SELECT id FROM docs WHERE (' '||body||' ') LIKE '% {token} %' \
                      EXCEPT \
                      SELECT id FROM docs WHERE fts_match(body, '{token}')))"
            ),
        );
        assert_eq!(
            rows,
            vec![vec![turso_core::Value::from_i64(0)]],
            "token {token}: fts_match and the base scan disagree — torn snapshot \
             served from the shared page cache"
        );
    }
}

/// B1 write-path auto-merge: single-row autocommit inserts must not let the
/// visible segment set grow past `PRAGMA fts_merge_threshold` by more than
/// the one segment the triggering statement itself appends. Runs in both
/// WAL and MVCC modes (the maintenance lease is a no-op in WAL).
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_auto_merge_bounds_segment_count() {
    for mvcc in [false, true] {
        let tmp_db = TempDatabase::builder()
            .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
            .with_mvcc(mvcc)
            .build();
        let conn = tmp_db.connect_limbo();
        conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
            .unwrap();
        conn.execute("PRAGMA fts_merge_threshold = 4").unwrap();
        assert_eq!(
            limbo_exec_rows(&conn, "PRAGMA fts_merge_threshold"),
            vec![vec![rusqlite::types::Value::Integer(4)]]
        );

        const ROWS: i64 = 30;
        for id in 0..ROWS {
            conn.execute(format!("INSERT INTO docs VALUES ({id}, 'common doc {id}')"))
                .unwrap();
        }

        let stats = fts_stats_in_txn(&tmp_db, &conn, "docs", "docs_fts");
        let segment_count = stats.segment_count.expect("snapshot must be loaded");
        assert!(
            segment_count <= 5,
            "auto-merge must keep the visible set at threshold + 1 at most, \
             got {segment_count} segments (mvcc={mvcc})"
        );

        // Every document still matches exactly once through the merged view.
        assert_eq!(
            limbo_exec_rows(
                &conn,
                "SELECT count(*) FROM docs WHERE fts_match(body, 'common')"
            ),
            vec![vec![rusqlite::types::Value::Integer(ROWS)]],
            "mvcc={mvcc}"
        );
    }
}

/// B1: a merge-threshold of 0 disables the write path merge entirely — each
/// autocommit insert keeps appending its own segment.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_auto_merge_disabled_by_zero_threshold() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    conn.execute("PRAGMA fts_merge_threshold = 0").unwrap();
    for id in 0..12 {
        conn.execute(format!("INSERT INTO docs VALUES ({id}, 'plain doc {id}')"))
            .unwrap();
    }
    assert_eq!(
        fts_stats_in_txn(&tmp_db, &conn, "docs", "docs_fts").segment_count,
        Some(12),
        "threshold 0 must leave one segment per single-row insert"
    );
    assert!(
        conn.execute("PRAGMA fts_merge_threshold = -1").is_err(),
        "negative thresholds must be rejected"
    );
}

/// B1: a concurrent transaction holding the maintenance lease makes the
/// write-path merge skip silently — the writer's inserts must succeed, and
/// the deferred merge happens on a later, uncontended insert.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_auto_merge_skips_when_lease_contended() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let writer = tmp_db.connect_limbo();
    let merger = tmp_db.connect_limbo();

    writer
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    writer
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    writer.execute("PRAGMA fts_merge_threshold = 2").unwrap();
    for id in 0..3 {
        writer
            .execute(format!("INSERT INTO docs VALUES ({id}, 'early doc {id}')"))
            .unwrap();
    }

    // The merger's open transaction holds the maintenance lease.
    merger.execute("BEGIN CONCURRENT").unwrap();
    merger.execute("OPTIMIZE INDEX docs_fts").unwrap();

    // Every insert is past the threshold, so each one attempts the merge;
    // the held lease must make it skip, never fail the writer.
    for id in 10..16 {
        writer
            .execute(format!("INSERT INTO docs VALUES ({id}, 'later doc {id}')"))
            .unwrap_or_else(|e| {
                panic!("a contended auto-merge must skip, not fail the writer: {e}")
            });
    }
    merger.execute("COMMIT").unwrap();

    // The lease is free again: the next insert merges everything down.
    writer
        .execute("INSERT INTO docs VALUES (100, 'final doc')")
        .unwrap();
    let fresh = tmp_db.connect_limbo();
    let stats = fts_stats_in_txn(&tmp_db, &fresh, "docs", "docs_fts");
    let segment_count = stats.segment_count.expect("snapshot must be loaded");
    assert!(
        segment_count <= 3,
        "the deferred merge must collapse the backlog, got {segment_count} segments"
    );
    assert_eq!(
        limbo_exec_rows(
            &fresh,
            "SELECT count(*) FROM docs WHERE fts_match(body, 'doc')"
        ),
        vec![vec![rusqlite::types::Value::Integer(10)]],
        "every document must match exactly once after skipped and deferred merges"
    );
}

/// B2 helper: build one large (>100KB) merged segment from `rows` documents
/// with wide vocabulary, then return the connection. The follow-up OPTIMIZE
/// compacts the batch flushes into a single segment.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
fn fts_build_large_segment(conn: &Arc<turso_core::Connection>, rows: usize) {
    conn.execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    let mut sql = String::from("INSERT INTO docs VALUES ");
    for id in 0..rows {
        if id > 0 {
            sql.push(',');
        }
        sql.push_str(&format!(
            "({id}, 'bulk document number{id} vocab{} vocab{} filler{} extra{} common corpus text')",
            id * 7 % 3000,
            id * 13 % 3000,
            id * 17 % 3000,
            id * 19 % 3000,
        ));
    }
    conn.execute(sql).unwrap();
    conn.execute("OPTIMIZE INDEX docs_fts").unwrap();
}

/// B2 tiered candidacy: the write-path merge must only rewrite the small
/// tier — one large segment plus many single-row segments merge down to
/// exactly two segments (the untouched large one and the merged smalls),
/// not one.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_auto_merge_spares_large_segments() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();
    fts_build_large_segment(&conn, 3000);
    let stats = fts_stats_in_txn(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(stats.segment_count, Some(1));
    assert!(
        stats.cached_bytes.unwrap() > 100 * 1024,
        "premise: the merged segment must exceed the smallest merge layer \
         (100KB), got {} bytes",
        stats.cached_bytes.unwrap()
    );

    conn.execute("PRAGMA fts_merge_threshold = 2").unwrap();
    for id in 10_000..10_006 {
        conn.execute(format!("INSERT INTO docs VALUES ({id}, 'tiny doc {id}')"))
            .unwrap();
    }

    let stats = fts_stats_in_txn(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(
        stats.segment_count,
        Some(2),
        "the large segment must not be rewritten by the small-tier merge"
    );
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT count(*) FROM docs WHERE fts_match(body, 'tiny')"
        ),
        vec![vec![rusqlite::types::Value::Integer(6)]]
    );
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT count(*) FROM docs WHERE fts_match(body, 'corpus')"
        ),
        vec![vec![rusqlite::types::Value::Integer(3000)]]
    );
}

/// B2 dead-space awareness: a segment that is at least half tombstoned is
/// picked by the write-path merge even when its size tier would spare it.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_auto_merge_rewrites_tombstone_heavy_segments() {
    let tmp_db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let conn = tmp_db.connect_limbo();
    fts_build_large_segment(&conn, 3000);
    let stats = fts_stats_in_txn(&tmp_db, &conn, "docs", "docs_fts");
    assert!(
        stats.cached_bytes.unwrap() > 100 * 1024,
        "premise: large segment"
    );

    // Tombstone 60% of the large segment, then trigger the write-path merge
    // exactly once (the second small insert pushes the count past the
    // threshold).
    conn.execute("DELETE FROM docs WHERE id < 1800").unwrap();
    conn.execute("PRAGMA fts_merge_threshold = 2").unwrap();
    for id in 10_000..10_002 {
        conn.execute(format!("INSERT INTO docs VALUES ({id}, 'tiny doc {id}')"))
            .unwrap();
    }

    let stats = fts_stats_in_txn(&tmp_db, &conn, "docs", "docs_fts");
    assert_eq!(
        stats.segment_count,
        Some(1),
        "a >=50% tombstoned segment must be rewritten regardless of its size tier"
    );
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT count(*) FROM docs WHERE fts_match(body, 'corpus')"
        ),
        vec![vec![rusqlite::types::Value::Integer(1200)]],
        "only the live documents survive the compaction"
    );
    assert_eq!(
        limbo_exec_rows(
            &conn,
            "SELECT count(*) FROM docs WHERE fts_match(body, 'tiny')"
        ),
        vec![vec![rusqlite::types::Value::Integer(2)]]
    );
}

/// B3 starvation probe: under a loop of concurrent single-row UPDATEs
/// (each one a short-lived deleter transaction), an OPTIMIZE issued
/// repeatedly from another connection must succeed within a bounded number
/// of attempts — contention refusals are fine, permanent starvation is not.
#[cfg(all(feature = "fts", feature = "test_helper", not(target_family = "wasm")))]
#[test]
fn fts_optimize_succeeds_under_concurrent_update_churn() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let tmp_db = Arc::new(
        TempDatabase::builder()
            .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
            .with_mvcc(true)
            .build(),
    );
    let setup = tmp_db.connect_limbo();
    setup
        .execute("CREATE TABLE docs(id INTEGER PRIMARY KEY, body TEXT)")
        .unwrap();
    setup
        .execute("CREATE INDEX docs_fts ON docs USING fts(body)")
        .unwrap();
    for id in 0..40 {
        setup
            .execute(format!("INSERT INTO docs VALUES ({id}, 'seed doc {id}')"))
            .unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let mut updaters = Vec::new();
    // Two updaters on disjoint id ranges: they contend with OPTIMIZE via
    // deleter registration, never with each other on base rows.
    for (lo, hi) in [(0i64, 20i64), (20, 40)] {
        let tmp_db = Arc::clone(&tmp_db);
        let stop = Arc::clone(&stop);
        updaters.push(std::thread::spawn(move || {
            let conn = tmp_db.connect_limbo();
            // Keep the churn manual-OPTIMIZE-shaped: no write-path merges.
            conn.execute("PRAGMA fts_merge_threshold = 0").unwrap();
            let mut round = 0i64;
            while !stop.load(Ordering::Acquire) {
                for id in lo..hi {
                    if stop.load(Ordering::Acquire) {
                        break;
                    }
                    match conn.execute(format!(
                        "UPDATE docs SET body = 'round {round} doc {id}' WHERE id = {id}"
                    )) {
                        Ok(_) => {}
                        Err(
                            turso_core::LimboError::Busy
                            | turso_core::LimboError::WriteWriteConflict
                            | turso_core::LimboError::SchemaUpdated,
                        ) => {}
                        Err(e) => panic!("updater failed abnormally: {e}"),
                    }
                }
                round += 1;
            }
        }));
    }

    // Give the churn a moment to be genuinely concurrent.
    std::thread::sleep(std::time::Duration::from_millis(50));

    let optimizer = tmp_db.connect_limbo();
    const MAX_ATTEMPTS: usize = 500;
    let mut attempts = 0;
    let succeeded = loop {
        attempts += 1;
        match optimizer.execute("OPTIMIZE INDEX docs_fts") {
            Ok(_) => break true,
            Err(
                turso_core::LimboError::Busy
                | turso_core::LimboError::WriteWriteConflict
                | turso_core::LimboError::SchemaUpdated,
            ) => {
                if attempts >= MAX_ATTEMPTS {
                    break false;
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            Err(e) => panic!("OPTIMIZE failed abnormally: {e}"),
        }
    };

    stop.store(true, Ordering::Release);
    for updater in updaters {
        updater.join().unwrap();
    }
    assert!(
        succeeded,
        "OPTIMIZE was starved for {MAX_ATTEMPTS} attempts under concurrent UPDATE churn"
    );
    println!("OPTIMIZE succeeded after {attempts} attempt(s)");

    // The index is still coherent after the churn + merge.
    let check = tmp_db.connect_limbo();
    assert_eq!(
        limbo_exec_rows(
            &check,
            "SELECT count(*) FROM docs WHERE fts_match(body, 'doc')"
        ),
        vec![vec![rusqlite::types::Value::Integer(40)]]
    );
}
