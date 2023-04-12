use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mvcc_rs::database::{Database, LocalClock, Row};
use pprof::criterion::{Output, PProfProfiler};

fn bench(c: &mut Criterion) {
    let clock = LocalClock::default();
    let db = Database::new(clock);
    c.bench_function("begin_tx", |b| {
        b.iter(|| {
            db.begin_tx();
        })
    });

    let clock = LocalClock::default();
    let db = Database::new(clock);
    c.bench_function("begin_tx + rollback_tx", |b| {
        b.iter(|| {
            let tx_id = db.begin_tx();
            db.rollback_tx(tx_id)
        })
    });

    let clock = LocalClock::default();
    let db = Database::new(clock);
    c.bench_function("begin_tx + commit_tx", |b| {
        b.iter(|| {
            let tx_id = db.begin_tx();
            db.commit_tx(tx_id)
        })
    });

    let clock = LocalClock::default();
    let db = Database::new(clock);
    let tx = db.begin_tx();
    db.insert(
        tx,
        Row {
            id: 1,
            data: "Hello".to_string(),
        },
    )
    .unwrap();
    c.bench_function("read", |b| {
        b.iter(|| {
            db.read(tx, 1).unwrap();
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench
}
criterion_main!(benches);
