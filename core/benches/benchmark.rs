use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use limbo_core::{Database, PlatformIO, IO};
use pprof::criterion::{Output, PProfProfiler};
use std::rc::Rc;

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("limbo");
    group.throughput(Throughput::Elements(1));

    let io = Rc::new(PlatformIO::new().unwrap());
    let db = Database::open_file(io.clone(), "../testing/hello.db").unwrap();
    let conn = db.connect();

    let mut stmt = conn.prepare("SELECT 1").unwrap();
    group.bench_function("Execute prepared statement: 'SELECT 1'", |b| {
        let io = io.clone();
        b.iter(|| {
            let mut rows = stmt.query().unwrap();
            match rows.next().unwrap() {
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
                match rows.next().unwrap() {
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

    drop(group);

    let mut group = c.benchmark_group("rusqlite");
    group.throughput(Throughput::Elements(1));

    let conn = rusqlite::Connection::open("../testing/hello.db").unwrap();

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
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench
}
criterion_main!(benches);
