use criterion::async_executor::FuturesExecutor;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use mvcc_rs::clock::LocalClock;
use mvcc_rs::database::{Database, Row, RowID};
use pprof::criterion::{Output, PProfProfiler};

fn bench_db() -> Database<LocalClock> {
    let clock = LocalClock::default();
    let storage = mvcc_rs::persistent_storage::Storage::new_noop();
    Database::new(clock, storage)
}

fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc-ops-throughput");
    group.throughput(Throughput::Elements(1));

    let db = bench_db();
    group.bench_function("begin_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            db.begin_tx().await;
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx + rollback_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let tx_id = db.begin_tx().await;
            db.rollback_tx(tx_id).await
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx + commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let tx_id = db.begin_tx().await;
            db.commit_tx(tx_id).await
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx-read-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let tx_id = db.begin_tx().await;
            db.read(
                tx_id,
                RowID {
                    table_id: 1,
                    row_id: 1,
                },
            )
            .await
            .unwrap();
            db.commit_tx(tx_id).await
        })
    });

    let db = bench_db();
    group.bench_function("begin_tx-update-commit_tx", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            let tx_id = db.begin_tx().await;
            db.update(
                tx_id,
                Row {
                    id: RowID {
                        table_id: 1,
                        row_id: 1,
                    },
                    data: "World".to_string(),
                },
            )
            .await
            .unwrap();
            db.commit_tx(tx_id).await
        })
    });

    let db = bench_db();
    let tx = futures::executor::block_on(db.begin_tx());
    futures::executor::block_on(db.insert(
        tx,
        Row {
            id: RowID {
                table_id: 1,
                row_id: 1,
            },
            data: "Hello".to_string(),
        },
    ))
    .unwrap();
    group.bench_function("read", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            db.read(
                tx,
                RowID {
                    table_id: 1,
                    row_id: 1,
                },
            )
            .await
            .unwrap();
        })
    });

    let db = bench_db();
    let tx = futures::executor::block_on(db.begin_tx());
    futures::executor::block_on(db.insert(
        tx,
        Row {
            id: RowID {
                table_id: 1,
                row_id: 1,
            },
            data: "Hello".to_string(),
        },
    ))
    .unwrap();
    group.bench_function("update", |b| {
        b.to_async(FuturesExecutor).iter(|| async {
            db.update(
                tx,
                Row {
                    id: RowID {
                        table_id: 1,
                        row_id: 1,
                    },
                    data: "World".to_string(),
                },
            )
            .await
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
