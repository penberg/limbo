use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use limbo_core::{Database, IO};
use pprof::criterion::{Output, PProfProfiler};

fn bench_db() -> Database {
    let io = IO::default();
    Database::open(io, "../testing/hello.db").unwrap()
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("limbo");
    group.throughput(Throughput::Elements(1));

    let db = bench_db();
    let conn = db.connect();

    let stmt = conn.prepare("SELECT 1").unwrap();
    group.bench_function("Execute prepared statement: 'SELECT 1'", |b| {
        b.iter(|| {
            let mut rows = stmt.query().unwrap();
            let row = rows.next().unwrap().unwrap();
            assert_eq!(row.get::<i64>(0).unwrap(), 1);
            stmt.reset();
        });
    });

    let stmt = conn.prepare("SELECT * FROM users LIMIT 1").unwrap();
    group.bench_function(
        "Execute prepared statement: 'SELECT * FROM users LIMIT 1'",
        |b| {
            b.iter(|| {
                let mut rows = stmt.query().unwrap();
                let row = rows.next().unwrap().unwrap();
                assert_eq!(row.get::<i64>(0).unwrap(), 1);
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
