use mvcc_rs::clock::LocalClock;
use mvcc_rs::database::{Database, Row, RowID};
use shuttle::sync::atomic::AtomicU64;
use shuttle::sync::Arc;
use shuttle::thread;
use std::sync::atomic::Ordering;

#[test]
fn test_non_overlapping_concurrent_inserts() {
    // Two threads insert to the database concurrently using non-overlapping
    // row IDs.
    let clock = LocalClock::default();
    let storage = mvcc_rs::persistent_storage::Storage::new_noop();
    let db = Arc::new(Database::new(clock, storage));
    let ids = Arc::new(AtomicU64::new(0));
    shuttle::check_random(
        move || {
            {
                let db = db.clone();
                let ids = ids.clone();
                thread::spawn(move || {
                    let tx = db.begin_tx();
                    let id = ids.fetch_add(1, Ordering::SeqCst);
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
                });
            }
            {
                let db = db.clone();
                let ids = ids.clone();
                thread::spawn(move || {
                    let tx = db.begin_tx();
                    let id = ids.fetch_add(1, Ordering::SeqCst);
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
                });
            }
        },
        100,
    );
}
