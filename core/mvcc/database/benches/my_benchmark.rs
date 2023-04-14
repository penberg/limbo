use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use mvcc_rs::clock::LocalClock;
use mvcc_rs::database::{Database, Row};
use pprof::criterion::{Output, PProfProfiler};

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc-ops-throughput");
    group.throughput(Throughput::Elements(1));

    let clock = LocalClock::default();
    let db = Database::new(clock);
    group.bench_function("begin_tx", |b| {
        b.iter(|| {
            db.begin_tx();
        })
    });

    let clock = LocalClock::default();
    let db = Database::new(clock);
    group.bench_function("begin_tx + rollback_tx", |b| {
        b.iter(|| {
            let tx_id = db.begin_tx();
            db.rollback_tx(tx_id)
        })
    });

    let clock = LocalClock::default();
    let db = Database::new(clock);
    group.bench_function("begin_tx + commit_tx", |b| {
        b.iter(|| {
            let tx_id = db.begin_tx();
            db.commit_tx(tx_id)
        })
    });

    let clock = LocalClock::default();
    let db = Database::new(clock);
    group.bench_function("begin_tx-read-commit_tx", |b| {
        b.iter(|| {
            let tx_id = db.begin_tx();
            db.read(tx_id, 1).unwrap();
            db.commit_tx(tx_id)
        })
    });

    let clock = LocalClock::default();
    let db = Database::new(clock);
    group.bench_function("begin_tx-update-commit_tx", |b| {
        b.iter(|| {
            let tx_id = db.begin_tx();
            db.update(
                tx_id,
                Row {
                    id: 1,
                    data: "World".to_string(),
                },
            )
            .unwrap();
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
    group.bench_function("read", |b| {
        b.iter(|| {
            db.read(tx, 1).unwrap();
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
    group.bench_function("update", |b| {
        b.iter(|| {
            db.update(
                tx,
                Row {
                    id: 1,
                    data: "World".to_string(),
                },
            )
            .unwrap();
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = bench
}
criterion_main!(benches);
