use mvcc_rs::clock::LocalClock;
use mvcc_rs::database::{Database, Row, RowID};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Once};

static IDS: AtomicU64 = AtomicU64::new(1);

static START: Once = Once::new();

#[test]
fn test_non_overlapping_concurrent_inserts() {
    START.call_once(|| {
        tracing_subscriber::fmt::init();
    });
    // Two threads insert to the database concurrently using non-overlapping
    // row IDs.
    let clock = LocalClock::default();
    let storage = mvcc_rs::persistent_storage::Storage::new_noop();
    let db = Arc::new(Database::new(clock, storage));
    let iterations = 100000;

    let th1 = {
        let db = db.clone();
        std::thread::spawn(move || {
            for _ in 0..iterations {
                let tx = db.begin_tx();
                let id = IDS.fetch_add(1, Ordering::SeqCst);
                let id = RowID {
                    table_id: 1,
                    row_id: id,
                };
                let row = Row {
                    id,
                    data: "Hello".to_string(),
                };
                db.insert(tx, row.clone()).unwrap();
                db.commit_tx(tx).unwrap();
                let tx = db.begin_tx();
                let committed_row = db.read(tx, id).unwrap();
                db.commit_tx(tx).unwrap();
                assert_eq!(committed_row, Some(row));
            }
        })
    };
    let th2 = {
        std::thread::spawn(move || {
            for _ in 0..iterations {
                let tx = db.begin_tx();
                let id = IDS.fetch_add(1, Ordering::SeqCst);
                let id = RowID {
                    table_id: 1,
                    row_id: id,
                };
                let row = Row {
                    id,
                    data: "World".to_string(),
                };
                db.insert(tx, row.clone()).unwrap();
                db.commit_tx(tx).unwrap();
                let tx = db.begin_tx();
                let committed_row = db.read(tx, id).unwrap();
                db.commit_tx(tx).unwrap();
                assert_eq!(committed_row, Some(row));
            }
        })
    };
    th1.join().unwrap();
    th2.join().unwrap();
}

#[test]
fn test_overlapping_concurrent_inserts_read_your_writes() {
    START.call_once(|| {
        tracing_subscriber::fmt::init();
    }); // Two threads insert to the database concurrently using overlapping row IDs.
    let clock = LocalClock::default();
    let storage = mvcc_rs::persistent_storage::Storage::new_noop();
    let db = Arc::new(Database::new(clock, storage));
    let iterations = 100000;

    let work = |prefix: &'static str| {
        let db = db.clone();
        std::thread::spawn(move || {
            for i in 0..iterations {
                if i % 1000 == 0 {
                    tracing::debug!("{prefix}: {i}");
                }
                if i % 10000 == 0 {
                    let dropped = db.drop_unused_row_versions();
                    tracing::debug!("garbage collected {dropped} versions");
                }
                let tx = db.begin_tx();
                let id = i % 16;
                let id = RowID {
                    table_id: 1,
                    row_id: id,
                };
                let row = Row {
                    id,
                    data: format!("{prefix} @{tx}"),
                };
                db.upsert(tx, row.clone()).unwrap();
                let committed_row = db.read(tx, id).unwrap();
                db.commit_tx(tx).unwrap();
                assert_eq!(committed_row, Some(row));
            }
        })
    };

    let threads = vec![work("A"), work("B"), work("C"), work("D")];
    for th in threads {
        th.join().unwrap();
    }
}
