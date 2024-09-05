use clap::{Parser, ValueEnum};
use limbo_core::{Database, RowResult, Value};
use rustyline::{error::ReadlineError, DefaultEditor};
use std::borrow::Borrow;
use std::fmt::format;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
enum OutputMode {
    Raw,
    Pretty,
}

impl std::fmt::Display for OutputMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_possible_value()
            .expect("no values are skipped")
            .get_name()
            .fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_sequential_write() -> anyhow::Result<()> {
        env_logger::init();
        let path = "test.db";

        let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::PlatformIO::new()?);
        dbg!(path);

        // run reset command
        let result = Command::new("./reset.sh")
            .output()
            .expect("failed to execute process");
        println!("finished creating db {:?}", result.stdout);
        println!("finished creating db {:?}", result.stderr);
        let db = Database::open_file(io.clone(), path)?;
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

    #[test]
    fn simple_test() {
        assert_eq!(2 + 2, 4);
    }
}

fn main() -> anyhow::Result<()> {
    Ok(())
}
