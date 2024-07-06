use clap::{Parser, ValueEnum};
use cli_table::{Cell, Table};
use limbo_core::{Database, RowResult, Value};
use rustyline::{error::ReadlineError, DefaultEditor};
use std::{path::PathBuf, rc::Rc};

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

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Opts {
    database: PathBuf,
    sql: Option<String>,
    #[clap(short, long, default_value_t = OutputMode::Raw)]
    output_mode: OutputMode,
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let opts = Opts::parse();
    let path = opts.database.to_str().unwrap();
    let io = Rc::new(limbo_core::PlatformIO::new()?);
    let db = Database::open_file(io.clone(), path)?;
    let conn = db.connect();
    if let Some(sql) = opts.sql {
        query(io.clone(), &conn, &sql, &opts.output_mode)?;
        return Ok(());
    }
    let mut rl = DefaultEditor::new()?;
    let home = dirs::home_dir().unwrap();
    let history_file = home.join(".limbo_history");
    if history_file.exists() {
        rl.load_history(history_file.as_path())?;
    }
    println!("Welcome to Limbo SQL shell!");
    loop {
        let readline = rl.readline("\x1b[90m>\x1b[0m ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.to_owned())?;
                if line.trim().starts_with('.') {
                    handle_dot_command(io.clone(), &conn, &line)?;
                } else {
                    query(io.clone(), &conn, &line, &opts.output_mode)?;
                }
            }
            Err(ReadlineError::Interrupted) => {
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                anyhow::bail!(err)
            }
        }
    }
    rl.save_history(history_file.as_path())?;
    Ok(())
}

fn handle_dot_command(
    io: Rc<dyn limbo_core::IO>,
    conn: &limbo_core::Connection,
    line: &str,
) -> anyhow::Result<()> {
    let args: Vec<&str> = line.split_whitespace().collect();

    if args.is_empty() {
        return Ok(());
    }

    match args[0] {
        ".schema" => match args.get(1) {
            Some(table_name) => match display_schema(io, conn, Some(table_name)) {
                Ok(found) => {
                    if !found {
                        println!("Error: Table '{}' not found.", table_name);
                    }
                }
                Err(e) => println!("Error: {}", e),
            },
            None => {
                println!("Error: .schema command requires a table name.");
                println!("Usage: .schema <table_name>");
            }
        },
        _ => {
            println!("Unknown command: {}", args[0]);
            println!("Available commands:");
            println!("  .schema <table_name> - Display the schema for a specific table");
        }
    }

    Ok(())
}

fn display_schema(
    io: Rc<dyn limbo_core::IO>,
    conn: &limbo_core::Connection,
    table: Option<&str>,
) -> anyhow::Result<bool> {
    if let Some(table_name) = table {
        let sql = format!(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND name = '{}' AND name NOT LIKE 'sqlite_%'",
            table_name
        );

        let mut found = false;
        match conn.query(&sql) {
            Ok(Some(ref mut rows)) => loop {
                match rows.next()? {
                    RowResult::Row(row) => {
                        if let Some(Value::Text(schema)) = row.values.get(0) {
                            println!("{};", schema);
                            found = true;
                        }
                    }
                    RowResult::IO => {
                        io.run_once()?;
                    }
                    RowResult::Done => break,
                }
            },
            Ok(None) => {
                println!("Error: Table '{}' not found.", table_name);
            }
            Err(err) => {
                if err.to_string().contains("no such table: sqlite_schema") {
                    return Err(anyhow::anyhow!("Unable to access database schema. The database may be using an older SQLite version or may not be properly initialized."));
                } else {
                    return Err(anyhow::anyhow!("Error querying schema: {}", err));
                }
            }
        }

        Ok(found)
    } else {
        Ok(false) // This case should not occur due to the check in handle_dot_command
    }
}
fn query(
    io: Rc<dyn limbo_core::IO>,
    conn: &limbo_core::Connection,
    sql: &str,
    output_mode: &OutputMode,
) -> anyhow::Result<()> {
    match conn.query(sql) {
        Ok(Some(ref mut rows)) => match output_mode {
            OutputMode::Raw => loop {
                match rows.next()? {
                    RowResult::Row(row) => {
                        for (i, value) in row.values.iter().enumerate() {
                            if i > 0 {
                                print!("|");
                            }
                            match value {
                                Value::Null => print!("NULL"),
                                Value::Integer(i) => print!("{}", i),
                                Value::Float(f) => print!("{}", f),
                                Value::Text(s) => print!("{}", s),
                                Value::Blob(b) => print!("{:?}", b),
                            }
                        }
                        println!();
                    }
                    RowResult::IO => {
                        io.run_once()?;
                    }
                    RowResult::Done => break,
                }
            },
            OutputMode::Pretty => {
                let mut table_rows: Vec<Vec<_>> = vec![];
                loop {
                    match rows.next()? {
                        RowResult::Row(row) => {
                            table_rows.push(
                                row.values
                                    .iter()
                                    .map(|value| match value {
                                        Value::Null => "NULL".cell(),
                                        Value::Integer(i) => i.to_string().cell(),
                                        Value::Float(f) => f.to_string().cell(),
                                        Value::Text(s) => s.cell(),
                                        Value::Blob(b) => format!("{:?}", b).cell(),
                                    })
                                    .collect(),
                            );
                        }
                        RowResult::IO => {
                            io.run_once()?;
                        }
                        RowResult::Done => break,
                    }
                }
                let table = table_rows.table();
                cli_table::print_stdout(table).unwrap();
            }
        },
        Ok(None) => {}
        Err(err) => {
            eprintln!("{}", err);
        }
    }
    Ok(())
}
