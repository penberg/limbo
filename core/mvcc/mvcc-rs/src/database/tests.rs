use super::*;
use crate::clock::LocalClock;
use tracing_test::traced_test;

fn test_db() -> Database<LocalClock> {
    let clock = LocalClock::new();
    let storage = crate::persistent_storage::Storage::new_noop();
    Database::new(clock, storage)
}

#[traced_test]
#[test]
fn test_insert_read() {
    let db = test_db();

    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    db.commit_tx(tx1).unwrap();

    let tx2 = db.begin_tx();
    let row = db
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

#[traced_test]
#[test]
fn test_read_nonexistent() {
    let db = test_db();
    let tx = db.begin_tx();
    let row = db.read(
        tx,
        RowID {
            table_id: 1,
            row_id: 1,
        },
    );
    assert!(row.unwrap().is_none());
}

#[traced_test]
#[test]
fn test_delete() {
    let db = test_db();

    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    db.delete(
        tx1,
        RowID {
            table_id: 1,
            row_id: 1,
        },
    )
    .unwrap();
    let row = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap();
    assert!(row.is_none());
    db.commit_tx(tx1).unwrap();

    let tx2 = db.begin_tx();
    let row = db
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap();
    assert!(row.is_none());
}

#[traced_test]
#[test]
fn test_delete_nonexistent() {
    let db = test_db();
    let tx = db.begin_tx();
    assert!(!db
        .delete(
            tx,
            RowID {
                table_id: 1,
                row_id: 1
            }
        )
        .unwrap());
}

#[traced_test]
#[test]
fn test_commit() {
    let db = test_db();
    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    let tx1_updated_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "World".to_string(),
    };
    db.update(tx1, tx1_updated_row.clone()).unwrap();
    let row = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_updated_row, row);
    db.commit_tx(tx1).unwrap();

    let tx2 = db.begin_tx();
    let row = db
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    db.commit_tx(tx2).unwrap();
    assert_eq!(tx1_updated_row, row);
    db.drop_unused_row_versions();
}

#[traced_test]
#[test]
fn test_rollback() {
    let db = test_db();
    let tx1 = db.begin_tx();
    let row1 = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string(),
    };
    db.insert(tx1, row1.clone()).unwrap();
    let row2 = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row1, row2);
    let row3 = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "World".to_string(),
    };
    db.update(tx1, row3.clone()).unwrap();
    let row4 = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row3, row4);
    db.rollback_tx(tx1);
    let tx2 = db.begin_tx();
    let row5 = db
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap();
    assert_eq!(row5, None);
}

#[traced_test]
#[test]
fn test_dirty_write() {
    let db = test_db();

    // T1 inserts a row with ID 1, but does not commit.
    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    // T2 attempts to delete row with ID 1, but fails because T1 has not committed.
    let tx2 = db.begin_tx();
    let tx2_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "World".to_string(),
    };
    assert!(!db.update(tx2, tx2_row).unwrap());

    let row = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

#[traced_test]
#[test]
fn test_dirty_read() {
    let db = test_db();

    // T1 inserts a row with ID 1, but does not commit.
    let tx1 = db.begin_tx();
    let row1 = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string(),
    };
    db.insert(tx1, row1).unwrap();

    // T2 attempts to read row with ID 1, but doesn't see one because T1 has not committed.
    let tx2 = db.begin_tx();
    let row2 = db
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap();
    assert_eq!(row2, None);
}

#[ignore]
#[traced_test]
#[test]
fn test_dirty_read_deleted() {
    let db = test_db();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    db.commit_tx(tx1).unwrap();

    // T2 deletes row with ID 1, but does not commit.
    let tx2 = db.begin_tx();
    assert!(db
        .delete(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1
            }
        )
        .unwrap());

    // T3 reads row with ID 1, but doesn't see the delete because T2 hasn't committed.
    let tx3 = db.begin_tx();
    let row = db
        .read(
            tx3,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

#[traced_test]
#[test]
fn test_fuzzy_read() {
    let db = test_db();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    db.commit_tx(tx1).unwrap();

    // T2 reads the row with ID 1 within an active transaction.
    let tx2 = db.begin_tx();
    let row = db
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);

    // T3 updates the row and commits.
    let tx3 = db.begin_tx();
    let tx3_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "World".to_string(),
    };
    db.update(tx3, tx3_row).unwrap();
    db.commit_tx(tx3).unwrap();

    // T2 still reads the same version of the row as before.
    let row = db
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

#[traced_test]
#[test]
fn test_lost_update() {
    let db = test_db();

    // T1 inserts a row with ID 1 and commits.
    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello".to_string(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    let row = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
    db.commit_tx(tx1).unwrap();

    // T2 attempts to update row ID 1 within an active transaction.
    let tx2 = db.begin_tx();
    let tx2_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "World".to_string(),
    };
    assert!(db.update(tx2, tx2_row.clone()).unwrap());

    // T3 also attempts to update row ID 1 within an active transaction.
    let tx3 = db.begin_tx();
    let tx3_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "Hello, world!".to_string(),
    };
    assert_eq!(
        Err(DatabaseError::WriteWriteConflict),
        db.update(tx3, tx3_row)
    );

    db.commit_tx(tx2).unwrap();
    assert_eq!(Err(DatabaseError::TxTerminated), db.commit_tx(tx3));

    let tx4 = db.begin_tx();
    let row = db
        .read(
            tx4,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx2_row, row);
}

// Test for the visibility to check if a new transaction can see old committed values.
// This test checks for the typo present in the paper, explained in https://github.com/penberg/mvcc-rs/issues/15
#[traced_test]
#[test]
fn test_committed_visibility() {
    let db = test_db();

    // let's add $10 to my account since I like money
    let tx1 = db.begin_tx();
    let tx1_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "10".to_string(),
    };
    db.insert(tx1, tx1_row.clone()).unwrap();
    db.commit_tx(tx1).unwrap();

    // but I like more money, so let me try adding $10 more
    let tx2 = db.begin_tx();
    let tx2_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "20".to_string(),
    };
    assert!(db.update(tx2, tx2_row.clone()).unwrap());
    let row = db
        .read(
            tx2,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(row, tx2_row);

    // can I check how much money I have?
    let tx3 = db.begin_tx();
    let row = db
        .read(
            tx3,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(tx1_row, row);
}

// Test to check if a older transaction can see (un)committed future rows
#[traced_test]
#[test]
fn test_future_row() {
    let db = test_db();

    let tx1 = db.begin_tx();

    let tx2 = db.begin_tx();
    let tx2_row = Row {
        id: RowID {
            table_id: 1,
            row_id: 1,
        },
        data: "10".to_string(),
    };
    db.insert(tx2, tx2_row).unwrap();

    // transaction in progress, so tx1 shouldn't be able to see the value
    let row = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap();
    assert_eq!(row, None);

    // lets commit the transaction and check if tx1 can see it
    db.commit_tx(tx2).unwrap();
    let row = db
        .read(
            tx1,
            RowID {
                table_id: 1,
                row_id: 1,
            },
        )
        .unwrap();
    assert_eq!(row, None);
}

#[traced_test]
#[test]
fn test_storage1() {
    let clock = LocalClock::new();
    let mut path = std::env::temp_dir();
    path.push(format!(
        "mvcc-rs-storage-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let storage = crate::persistent_storage::Storage::new_json_on_disk(path.clone());
    let db = Database::new(clock, storage);

    let tx1 = db.begin_tx();
    let tx2 = db.begin_tx();
    let tx3 = db.begin_tx();

    db.insert(
        tx3,
        Row {
            id: RowID {
                table_id: 1,
                row_id: 1,
            },
            data: "testme".to_string(),
        },
    )
    .unwrap();

    db.commit_tx(tx1).unwrap();
    db.rollback_tx(tx2);
    db.commit_tx(tx3).unwrap();

    let tx4 = db.begin_tx();
    db.insert(
        tx4,
        Row {
            id: RowID {
                table_id: 1,
                row_id: 2,
            },
            data: "testme2".to_string(),
        },
    )
    .unwrap();
    db.insert(
        tx4,
        Row {
            id: RowID {
                table_id: 1,
                row_id: 3,
            },
            data: "testme3".to_string(),
        },
    )
    .unwrap();

    assert_eq!(
        db.read(
            tx4,
            RowID {
                table_id: 1,
                row_id: 1
            }
        )
        .unwrap()
        .unwrap()
        .data,
        "testme"
    );
    assert_eq!(
        db.read(
            tx4,
            RowID {
                table_id: 1,
                row_id: 2
            }
        )
        .unwrap()
        .unwrap()
        .data,
        "testme2"
    );
    assert_eq!(
        db.read(
            tx4,
            RowID {
                table_id: 1,
                row_id: 3
            }
        )
        .unwrap()
        .unwrap()
        .data,
        "testme3"
    );
    db.commit_tx(tx4).unwrap();

    let clock = LocalClock::new();
    let storage = crate::persistent_storage::Storage::new_json_on_disk(path);
    let db = Database::new(clock, storage);
    db.recover().unwrap();
    println!("{:#?}", db);

    let tx5 = db.begin_tx();
    println!(
        "{:#?}",
        db.read(
            tx5,
            RowID {
                table_id: 1,
                row_id: 1
            }
        )
    );
    assert_eq!(
        db.read(
            tx5,
            RowID {
                table_id: 1,
                row_id: 1
            }
        )
        .unwrap()
        .unwrap()
        .data,
        "testme"
    );
    assert_eq!(
        db.read(
            tx5,
            RowID {
                table_id: 1,
                row_id: 2
            }
        )
        .unwrap()
        .unwrap()
        .data,
        "testme2"
    );
    assert_eq!(
        db.read(
            tx5,
            RowID {
                table_id: 1,
                row_id: 3
            }
        )
        .unwrap()
        .unwrap()
        .data,
        "testme3"
    );
}
