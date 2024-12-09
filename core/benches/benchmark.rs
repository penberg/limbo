use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use fallible_iterator::FallibleIterator;
use limbo_core::parser::parse_sql_statement;
use limbo_core::{Database, PlatformIO, IO};
use pprof::criterion::{Output, PProfProfiler};
use sqlite3_parser::lexer::sql::Parser;
use std::sync::Arc;

fn bench(c: &mut Criterion) {
    if std::env::var("DISABLE_PARSER_BENCHMARK").is_err() {
        parser_bench(c);
    }
    if std::env::var("DISABLE_LIMBO_BENCHMARK").is_err() {
        limbo_bench(c);
    }
    // https://github.com/penberg/limbo/issues/174
    // The rusqlite benchmark crashes on Mac M1 when using the flamegraph features
    if std::env::var("DISABLE_RUSQLITE_BENCHMARK").is_err() {
        rusqlite_bench(c);
    }
}

fn parser_bench(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("parser");
    group.throughput(Throughput::Elements(1));

    let sql_statements = vec![
        ("SELECT_STAR_LIMIT_1".to_string(), "SELECT * FROM users LIMIT 1".to_string()),
        ("MULTIPLE_JOINS".to_string(), "SELECT foo,bar,baz,bad FROM users LEFT OUTER JOIN products INNER JOIN gizmos LIMIT 100".to_string()),
        ("MULTIPLE_JOINS_WITH_WHERE".to_string(), "SELECT foo,bar,baz,bad FROM users LEFT OUTER JOIN products INNER JOIN gizmos WHERE foo = 1 AND bar = 2 OR baz < 3 AND bad = 4 LIMIT 100".to_string()),
        ("MULTIPLE_JOINS_WITH_WHERE_GROUPBY_AND_ORDERBY".to_string(), "SELECT foo,bar,baz,bad FROM users LEFT OUTER JOIN products INNER JOIN gizmos WHERE foo = 1 AND bar = 2 OR baz < 3 AND bad = 4 GROUP BY foo,bar ORDER BY baz DESC, bad ASC LIMIT 100".to_string()),
    ];

    for (i, (test_name, sql)) in sql_statements.into_iter().enumerate() {
        group.bench_function(format!("Parse SQL, sqlite3_parser: '{}'", test_name), |b| {
            b.iter(|| {
                let mut p = Parser::new(sql.as_bytes());
                p.next().unwrap();
            });
        });
        let mut sql2 = sql.clone();
        group.bench_function(format!("Parse SQL, limbo: '{}'", test_name), |b| {
            b.iter(|| {
                parse_sql_statement(&mut sql2).unwrap();
            });
        });
    }

    group.finish();
}

fn limbo_bench(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("limbo");
    group.throughput(Throughput::Elements(1));
    let io = Arc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/testing.db").unwrap();
    let conn = db.connect();

    group.bench_function("Prepare statement: 'SELECT 1'", |b| {
        b.iter(|| {
            conn.prepare("SELECT 1").unwrap();
        });
    });

    group.bench_function("Prepare statement: 'SELECT * FROM users LIMIT 1'", |b| {
        b.iter(|| {
            conn.prepare("SELECT * FROM users LIMIT 1").unwrap();
        });
    });

    let mut stmt = conn.prepare("SELECT 1").unwrap();
    group.bench_function("Execute prepared statement: 'SELECT 1'", |b| {
        let io = io.clone();
        b.iter(|| {
            let mut rows = stmt.query().unwrap();
            match rows.next_row().unwrap() {
                limbo_core::RowResult::Row(row) => {
                    assert_eq!(row.get::<i64>(0).unwrap(), 1);
                }
                limbo_core::RowResult::IO => {
                    io.run_once().unwrap();
                }
                limbo_core::RowResult::Done => {
                    unreachable!();
                }
            }
            stmt.reset();
        });
    });

    let mut stmt = conn.prepare("SELECT * FROM users LIMIT 1").unwrap();
    group.bench_function(
        "Execute prepared statement: 'SELECT * FROM users LIMIT 1'",
        |b| {
            let io = io.clone();
            b.iter(|| {
                let mut rows = stmt.query().unwrap();
                match rows.next_row().unwrap() {
                    limbo_core::RowResult::Row(row) => {
                        assert_eq!(row.get::<i64>(0).unwrap(), 1);
                    }
                    limbo_core::RowResult::IO => {
                        io.run_once().unwrap();
                    }
                    limbo_core::RowResult::Done => {
                        unreachable!();
                    }
                }
                stmt.reset();
            });
        },
    );

    let mut stmt = conn.prepare("SELECT * FROM users LIMIT 100").unwrap();
    group.bench_function(
        "Execute prepared statement: 'SELECT * FROM users LIMIT 100'",
        |b| {
            let io = io.clone();
            b.iter(|| {
                let mut rows = stmt.query().unwrap();
                match rows.next_row().unwrap() {
                    limbo_core::RowResult::Row(row) => {
                        assert_eq!(row.get::<i64>(0).unwrap(), 1);
                    }
                    limbo_core::RowResult::IO => {
                        io.run_once().unwrap();
                    }
                    limbo_core::RowResult::Done => {
                        unreachable!();
                    }
                }
                stmt.reset();
            });
        },
    );
}

fn rusqlite_bench(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("rusqlite");
    group.throughput(Throughput::Elements(1));

    let conn = rusqlite::Connection::open("../testing/testing.db").unwrap();

    conn.pragma_update(None, "locking_mode", "EXCLUSIVE")
        .unwrap();
    group.bench_function("Prepare statement: 'SELECT 1'", |b| {
        b.iter(|| {
            conn.prepare("SELECT 1").unwrap();
        });
    });

    group.bench_function("Prepare statement: 'SELECT * FROM users LIMIT 1'", |b| {
        b.iter(|| {
            conn.prepare("SELECT * FROM users LIMIT 1").unwrap();
        });
    });

    let mut stmt = conn.prepare("SELECT 1").unwrap();
    group.bench_function("Execute prepared statement: 'SELECT 1'", |b| {
        b.iter(|| {
            let mut rows = stmt.query(()).unwrap();
            let row = rows.next().unwrap().unwrap();
            let val: i64 = row.get(0).unwrap();
            assert_eq!(val, 1);
        });
    });

    let mut stmt = conn.prepare("SELECT * FROM users LIMIT 1").unwrap();
    group.bench_function(
        "Execute prepared statement: 'SELECT * FROM users LIMIT 1'",
        |b| {
            b.iter(|| {
                let mut rows = stmt.query(()).unwrap();
                let row = rows.next().unwrap().unwrap();
                let id: i64 = row.get(0).unwrap();
                assert_eq!(id, 1);
            });
        },
    );

    let mut stmt = conn.prepare("SELECT * FROM users LIMIT 100").unwrap();
    group.bench_function(
        "Execute prepared statement: 'SELECT * FROM users LIMIT 100'",
        |b| {
            b.iter(|| {
                let mut rows = stmt.query(()).unwrap();
                let row = rows.next().unwrap().unwrap();
                let id: i64 = row.get(0).unwrap();
                assert_eq!(id, 1);
            });
        },
    );
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench
}
criterion_main!(benches);
