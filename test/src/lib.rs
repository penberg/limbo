#[cfg(test)]
mod tests {
    use limbo_core::{Database, RowResult, Value};
    use rusqlite::Connection;
    use std::env::current_dir;
    use std::sync::Arc;
    #[test]
    fn test_sequential_write() -> anyhow::Result<()> {
        env_logger::init();
        let path = "test.db";

        let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::PlatformIO::new()?);
        dbg!(path);
        let mut path = current_dir()?;
        path.push("test.db");
        {
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            let connection = Connection::open(&path)?;
            connection.execute("CREATE TABLE test (x INTEGER PRIMARY KEY);", ())?;
        }

        let db = Database::open_file(io.clone(), path.to_str().unwrap())?;
        let conn = db.connect();

        let list_query = "SELECT * FROM test";
        let max_iterations = 10000;
        for i in 0..max_iterations {
            if (i % 100) == 0 {
                let progress = (i as f64 / max_iterations as f64) * 100.0;
                println!("progress {:.1}%", progress);
            }
            let insert_query = format!("INSERT INTO test VALUES ({})", i);
            match conn.query(insert_query) {
                Ok(Some(ref mut rows)) => loop {
                    match rows.next_row()? {
                        RowResult::IO => {
                            io.run_once()?;
                        }
                        RowResult::Done => break,
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
                    match rows.next_row()? {
                        RowResult::Row(row) => {
                            let first_value = row.values.first().expect("missing id");
                            let id = match first_value {
                                Value::Integer(i) => *i as i32,
                                Value::Float(f) => *f as i32,
                                _ => unreachable!(),
                            };
                            assert_eq!(current_read_index, id);
                            current_read_index += 1;
                        }
                        RowResult::IO => {
                            io.run_once()?;
                        }
                        RowResult::Done => break,
                    }
                },
                Ok(None) => {}
                Err(err) => {
                    eprintln!("{}", err);
                }
            }
            conn.cacheflush()?;
        }
        Ok(())
    }
}
