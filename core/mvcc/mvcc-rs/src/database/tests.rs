use super::*;
use crate::clock::LocalClock;
use tracing_test::traced_test;

fn test_db() -> Database<LocalClock, String> {
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
    let db: Database<LocalClock, String> = Database::new(clock, storage);
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

/* States described in the Hekaton paper *for serializability*:

Table 1: Case analysis of action to take when version V’s
Begin field contains the ID of transaction TB
------------------------------------------------------------------------------------------------------
TB’s state   | TB’s end timestamp | Action to take when transaction T checks visibility of version V.
------------------------------------------------------------------------------------------------------
Active       | Not set            | V is visible only if TB=T and V’s end timestamp equals infinity.
------------------------------------------------------------------------------------------------------
Preparing    | TS                 | V’s begin timestamp will be TS ut V is not yet committed. Use TS
                                  | as V’s begin time when testing visibility. If the test is true,
                                  | allow T to speculatively read V. Committed TS V’s begin timestamp
                                  | will be TS and V is committed. Use TS as V’s begin time to test
                                  | visibility.
------------------------------------------------------------------------------------------------------
Committed    | TS                 | V’s begin timestamp will be TS and V is committed. Use TS as V’s
                                  | begin time to test visibility.
------------------------------------------------------------------------------------------------------
Aborted      | Irrelevant         | Ignore V; it’s a garbage version.
------------------------------------------------------------------------------------------------------
Terminated   | Irrelevant         | Reread V’s Begin field. TB has terminated so it must have finalized
or not found |                    | the timestamp.
------------------------------------------------------------------------------------------------------

Table 2: Case analysis of action to take when V's End field
contains a transaction ID TE.
------------------------------------------------------------------------------------------------------
TE’s state   | TE’s end timestamp | Action to take when transaction T checks visibility of a version V
             |                    | as of read time RT.
------------------------------------------------------------------------------------------------------
Active       | Not set            | V is visible only if TE is not T.
------------------------------------------------------------------------------------------------------
Preparing    | TS                 | V’s end timestamp will be TS provided that TE commits. If TS > RT,
                                  | V is visible to T. If TS < RT, T speculatively ignores V.
------------------------------------------------------------------------------------------------------
Committed    | TS                 | V’s end timestamp will be TS and V is committed. Use TS as V’s end
                                  | timestamp when testing visibility.
------------------------------------------------------------------------------------------------------
Aborted      | Irrelevant         | V is visible.
------------------------------------------------------------------------------------------------------
Terminated   | Irrelevant         | Reread V’s End field. TE has terminated so it must have finalized
or not found |                    | the timestamp.
*/

fn new_tx(tx_id: TxID, begin_ts: u64, state: TransactionState) -> RwLock<Transaction> {
    let state = state.into();
    RwLock::new(Transaction {
        state,
        tx_id,
        begin_ts,
        write_set: SkipSet::new(),
        read_set: SkipSet::new(),
    })
}

#[traced_test]
#[test]
fn test_snapshot_isolation_tx_visible1() {
    let txs: SkipMap<TxID, RwLock<Transaction>> = SkipMap::from_iter([
        (1, new_tx(1, 1, TransactionState::Committed(2))),
        (2, new_tx(2, 2, TransactionState::Committed(5))),
        (3, new_tx(3, 3, TransactionState::Aborted)),
        (5, new_tx(5, 5, TransactionState::Preparing)),
        (6, new_tx(6, 6, TransactionState::Committed(10))),
        (7, new_tx(7, 7, TransactionState::Active)),
    ]);

    let current_tx = new_tx(4, 4, TransactionState::Preparing);
    let current_tx = current_tx.read().unwrap();

    let rv_visible = |begin: TxTimestampOrID, end: Option<TxTimestampOrID>| {
        let row_version = RowVersion {
            begin,
            end,
            row: Row {
                id: RowID {
                    table_id: 1,
                    row_id: 1,
                },
                data: "testme".to_string(),
            },
        };
        tracing::debug!("Testing visibility of {row_version:?}");
        is_version_visible(&txs, &current_tx, &row_version)
    };

    // begin visible:   transaction committed with ts < current_tx.begin_ts
    // end visible:     inf
    assert!(rv_visible(TxTimestampOrID::TxID(1), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(TxTimestampOrID::TxID(2), None));

    // begin invisible: transaction aborted
    assert!(!rv_visible(TxTimestampOrID::TxID(3), None));

    // begin visible:   timestamp < current_tx.begin_ts
    // end invisible:   transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(
        TxTimestampOrID::Timestamp(0),
        Some(TxTimestampOrID::TxID(1))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     transaction committed with ts < current_tx.begin_ts
    assert!(rv_visible(
        TxTimestampOrID::Timestamp(0),
        Some(TxTimestampOrID::TxID(2))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end invisible:   transaction aborted
    assert!(!rv_visible(
        TxTimestampOrID::Timestamp(0),
        Some(TxTimestampOrID::TxID(3))
    ));

    // begin invisible: transaction preparing
    assert!(!rv_visible(TxTimestampOrID::TxID(5), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(TxTimestampOrID::TxID(6), None));

    // begin invisible: transaction active
    assert!(!rv_visible(TxTimestampOrID::TxID(7), None));

    // begin invisible: transaction committed with ts > current_tx.begin_ts
    assert!(!rv_visible(TxTimestampOrID::TxID(6), None));

    // begin invisible:   transaction active
    assert!(!rv_visible(TxTimestampOrID::TxID(7), None));

    // begin visible:   timestamp < current_tx.begin_ts
    // end invisible:     transaction preparing
    assert!(!rv_visible(
        TxTimestampOrID::Timestamp(0),
        Some(TxTimestampOrID::TxID(5))
    ));

    // begin invisible: timestamp > current_tx.begin_ts
    assert!(!rv_visible(
        TxTimestampOrID::Timestamp(6),
        Some(TxTimestampOrID::TxID(6))
    ));

    // begin visible:   timestamp < current_tx.begin_ts
    // end visible:     some active transaction will eventually overwrite this version,
    //                  but that hasn't happened
    //                  (this is the https://avi.im/blag/2023/hekaton-paper-typo/ case, I believe!)
    assert!(rv_visible(
        TxTimestampOrID::Timestamp(0),
        Some(TxTimestampOrID::TxID(7))
    ));
}
