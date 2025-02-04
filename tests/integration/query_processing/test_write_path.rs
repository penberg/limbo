use crate::common;
use crate::common::{compare_string, do_flush, TempDatabase};
use limbo_core::{Connection, StepResult, Value};
use log::debug;
use std::rc::Rc;

#[test]
fn test_simple_overflow_page() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db =
        TempDatabase::new_with_rusqlite("CREATE TABLE test (x INTEGER PRIMARY KEY, t TEXT);");
    let conn = tmp_db.connect_limbo();

    let mut huge_text = String::new();
    for i in 0..8192 {
        huge_text.push((b'A' + (i % 24) as u8) as char);
    }

    let list_query = "SELECT * FROM test LIMIT 1";
    let insert_query = format!("INSERT INTO test VALUES (1, '{}')", huge_text.as_str());

    match conn.query(insert_query) {
        Ok(Some(ref mut rows)) => loop {
            match rows.step()? {
                StepResult::IO => {
                    tmp_db.io.run_once()?;
                }
                StepResult::Done => break,
                _ => unreachable!(),
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{}", err);
        }
    };

    // this flush helped to review hex of test.db
    do_flush(&conn, &tmp_db)?;

    match conn.query(list_query) {
        Ok(Some(ref mut rows)) => loop {
            match rows.step()? {
                StepResult::Row(row) => {
                    let first_value = &row.values[0];
                    let text = &row.values[1];
                    let id = match first_value {
                        Value::Integer(i) => *i as i32,
                        Value::Float(f) => *f as i32,
                        _ => unreachable!(),
                    };
                    let text = match text {
                        Value::Text(t) => *t,
                        _ => unreachable!(),
                    };
                    assert_eq!(1, id);
                    compare_string(&huge_text, text);
                }
                StepResult::IO => {
                    tmp_db.io.run_once()?;
                }
                StepResult::Interrupt => break,
                StepResult::Done => break,
                StepResult::Busy => unreachable!(),
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{}", err);
        }
    }
    do_flush(&conn, &tmp_db)?;
    Ok(())
}

#[test]
fn test_sequential_overflow_page() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db =
        TempDatabase::new_with_rusqlite("CREATE TABLE test (x INTEGER PRIMARY KEY, t TEXT);");
    let conn = tmp_db.connect_limbo();
    let iterations = 10_usize;

    let mut huge_texts = Vec::new();
    for i in 0..iterations {
        let mut huge_text = String::new();
        for _j in 0..8192 {
            huge_text.push((b'A' + i as u8) as char);
        }
        huge_texts.push(huge_text);
    }

    for i in 0..iterations {
        let huge_text = &huge_texts[i];
        let insert_query = format!("INSERT INTO test VALUES ({}, '{}')", i, huge_text.as_str());
        match conn.query(insert_query) {
            Ok(Some(ref mut rows)) => loop {
                match rows.step()? {
                    StepResult::IO => {
                        tmp_db.io.run_once()?;
                    }
                    StepResult::Done => break,
                    _ => unreachable!(),
                }
            },
            Ok(None) => {}
            Err(err) => {
                eprintln!("{}", err);
            }
        };
    }

    let list_query = "SELECT * FROM test LIMIT 1";
    let mut current_index = 0;
    match conn.query(list_query) {
        Ok(Some(ref mut rows)) => loop {
            match rows.step()? {
                StepResult::Row(row) => {
                    let first_value = &row.values[0];
                    let text = &row.values[1];
                    let id = match first_value {
                        Value::Integer(i) => *i as i32,
                        Value::Float(f) => *f as i32,
                        _ => unreachable!(),
                    };
                    let text = match text {
                        Value::Text(t) => *t,
                        _ => unreachable!(),
                    };
                    let huge_text = &huge_texts[current_index];
                    assert_eq!(current_index, id as usize);
                    compare_string(huge_text, text);
                    current_index += 1;
                }
                StepResult::IO => {
                    tmp_db.io.run_once()?;
                }
                StepResult::Interrupt => break,
                StepResult::Done => break,
                StepResult::Busy => unreachable!(),
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{}", err);
        }
    }
    do_flush(&conn, &tmp_db)?;
    Ok(())
}

#[ignore]
#[test]
fn test_sequential_write() -> anyhow::Result<()> {
    let _ = env_logger::try_init();

    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (x INTEGER PRIMARY KEY);");
    let conn = tmp_db.connect_limbo();

    let list_query = "SELECT * FROM test";
    let max_iterations = 10000;
    for i in 0..max_iterations {
        debug!("inserting {} ", i);
        if (i % 100) == 0 {
            let progress = (i as f64 / max_iterations as f64) * 100.0;
            println!("progress {:.1}%", progress);
        }
        let insert_query = format!("INSERT INTO test VALUES ({})", i);
        match conn.query(insert_query) {
            Ok(Some(ref mut rows)) => loop {
                match rows.step()? {
                    StepResult::IO => {
                        tmp_db.io.run_once()?;
                    }
                    StepResult::Done => break,
                    _ => unreachable!(),
                }
            },
            Ok(None) => {}
            Err(err) => {
                eprintln!("{}", err);
            }
        };

        let mut current_read_index = 0;
        match conn.query(list_query) {
            Ok(Some(ref mut rows)) => loop {
                match rows.step()? {
                    StepResult::Row(row) => {
                        let first_value = row.values.first().expect("missing id");
                        let id = match first_value {
                            Value::Integer(i) => *i as i32,
                            Value::Float(f) => *f as i32,
                            _ => unreachable!(),
                        };
                        assert_eq!(current_read_index, id);
                        current_read_index += 1;
                    }
                    StepResult::IO => {
                        tmp_db.io.run_once()?;
                    }
                    StepResult::Interrupt => break,
                    StepResult::Done => break,
                    StepResult::Busy => {
                        panic!("Database is busy");
                    }
                }
            },
            Ok(None) => {}
            Err(err) => {
                eprintln!("{}", err);
            }
        }
        common::do_flush(&conn, &tmp_db)?;
    }
    Ok(())
}

#[test]
/// There was a regression with inserting multiple rows with a column containing an unary operator :)
/// https://github.com/tursodatabase/limbo/pull/679
fn test_regression_multi_row_insert() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (x REAL);");
    let conn = tmp_db.connect_limbo();

    let insert_query = "INSERT INTO test VALUES (-2), (-3), (-1)";
    let list_query = "SELECT * FROM test";

    match conn.query(insert_query) {
        Ok(Some(ref mut rows)) => loop {
            match rows.step()? {
                StepResult::IO => {
                    tmp_db.io.run_once()?;
                }
                StepResult::Done => break,
                _ => unreachable!(),
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{}", err);
        }
    };

    common::do_flush(&conn, &tmp_db)?;

    let mut current_read_index = 1;
    let expected_ids = vec![-3, -2, -1];
    let mut actual_ids = Vec::new();
    match conn.query(list_query) {
        Ok(Some(ref mut rows)) => loop {
            match rows.step()? {
                StepResult::Row(row) => {
                    let first_value = row.values.first().expect("missing id");
                    let id = match first_value {
                        Value::Float(f) => *f as i32,
                        _ => panic!("expected float"),
                    };
                    actual_ids.push(id);
                    current_read_index += 1;
                }
                StepResult::IO => {
                    tmp_db.io.run_once()?;
                }
                StepResult::Interrupt => break,
                StepResult::Done => break,
                StepResult::Busy => {
                    panic!("Database is busy");
                }
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{}", err);
        }
    }

    assert_eq!(current_read_index, 4); // Verify we read all rows
                                       // sort ids
    actual_ids.sort();
    assert_eq!(actual_ids, expected_ids);
    Ok(())
}

#[test]
fn test_statement_reset() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("create table test (i integer);");
    let conn = tmp_db.connect_limbo();

    conn.execute("insert into test values (1)")?;
    conn.execute("insert into test values (2)")?;

    let mut stmt = conn.prepare("select * from test")?;

    loop {
        match stmt.step()? {
            StepResult::Row(row) => {
                assert_eq!(row.values[0], Value::Integer(1));
                break;
            }
            StepResult::IO => tmp_db.io.run_once()?,
            _ => break,
        }
    }

    stmt.reset();

    loop {
        match stmt.step()? {
            StepResult::Row(row) => {
                assert_eq!(row.values[0], Value::Integer(1));
                break;
            }
            StepResult::IO => tmp_db.io.run_once()?,
            _ => break,
        }
    }

    Ok(())
}

#[test]
#[ignore]
fn test_wal_checkpoint() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (x INTEGER PRIMARY KEY);");
    // threshold is 1000 by default
    let iterations = 1001_usize;
    let conn = tmp_db.connect_limbo();

    for i in 0..iterations {
        let insert_query = format!("INSERT INTO test VALUES ({})", i);
        do_flush(&conn, &tmp_db)?;
        conn.checkpoint()?;
        match conn.query(insert_query) {
            Ok(Some(ref mut rows)) => loop {
                match rows.step()? {
                    StepResult::IO => {
                        tmp_db.io.run_once()?;
                    }
                    StepResult::Done => break,
                    _ => unreachable!(),
                }
            },
            Ok(None) => {}
            Err(err) => {
                eprintln!("{}", err);
            }
        };
    }

    do_flush(&conn, &tmp_db)?;
    conn.clear_page_cache()?;
    let list_query = "SELECT * FROM test LIMIT 1";
    let mut current_index = 0;
    match conn.query(list_query) {
        Ok(Some(ref mut rows)) => loop {
            match rows.step()? {
                StepResult::Row(row) => {
                    let first_value = &row.values[0];
                    let id = match first_value {
                        Value::Integer(i) => *i as i32,
                        Value::Float(f) => *f as i32,
                        _ => unreachable!(),
                    };
                    assert_eq!(current_index, id as usize);
                    current_index += 1;
                }
                StepResult::IO => {
                    tmp_db.io.run_once()?;
                }
                StepResult::Interrupt => break,
                StepResult::Done => break,
                StepResult::Busy => unreachable!(),
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{}", err);
        }
    }
    do_flush(&conn, &tmp_db)?;
    Ok(())
}

#[test]
fn test_wal_restart() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let tmp_db = TempDatabase::new_with_rusqlite("CREATE TABLE test (x INTEGER PRIMARY KEY);");
    // threshold is 1000 by default

    fn insert(i: usize, conn: &Rc<Connection>, tmp_db: &TempDatabase) -> anyhow::Result<()> {
        debug!("inserting {}", i);
        let insert_query = format!("INSERT INTO test VALUES ({})", i);
        match conn.query(insert_query) {
            Ok(Some(ref mut rows)) => loop {
                match rows.step()? {
                    StepResult::IO => {
                        tmp_db.io.run_once()?;
                    }
                    StepResult::Done => break,
                    _ => unreachable!(),
                }
            },
            Ok(None) => {}
            Err(err) => {
                eprintln!("{}", err);
            }
        };
        debug!("inserted {}", i);
        tmp_db.io.run_once()?;
        Ok(())
    }

    fn count(conn: &Rc<Connection>, tmp_db: &TempDatabase) -> anyhow::Result<usize> {
        debug!("counting");
        let list_query = "SELECT count(x) FROM test";
        loop {
            if let Some(ref mut rows) = conn.query(list_query)? {
                loop {
                    match rows.step()? {
                        StepResult::Row(row) => {
                            let first_value = &row.values[0];
                            let count = match first_value {
                                Value::Integer(i) => *i as i32,
                                _ => unreachable!(),
                            };
                            debug!("counted {}", count);
                            return Ok(count as usize);
                        }
                        StepResult::IO => {
                            tmp_db.io.run_once()?;
                        }
                        StepResult::Interrupt => break,
                        StepResult::Done => break,
                        StepResult::Busy => panic!("Database is busy"),
                    }
                }
            }
        }
    }

    {
        let conn = tmp_db.connect_limbo();
        insert(1, &conn, &tmp_db)?;
        assert_eq!(count(&conn, &tmp_db)?, 1);
        conn.close()?;
    }
    {
        let conn = tmp_db.connect_limbo();
        assert_eq!(
            count(&conn, &tmp_db)?,
            1,
            "failed to read from wal from another connection"
        );
        conn.close()?;
    }
    Ok(())
}
