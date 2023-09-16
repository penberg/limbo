use clap::{Parser, ValueEnum};
use cli_table::{Cell, Table};
use lig_core::{Database, Value};
use rustyline::{error::ReadlineError, DefaultEditor};
use std::path::PathBuf;

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
    #[clap(short, long, default_value_t = OutputMode::Raw)]
    output_mode: OutputMode,
}

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    let path = opts.database.to_str().unwrap();
    let io = lig_core::IO::new()?;
    let db = Database::open_file(io, path)?;
    let conn = db.connect();
    let mut rl = DefaultEditor::new()?;
    let home = dirs::home_dir().unwrap();
    let history_file = home.join(".lig_history");
    if history_file.exists() {
        rl.load_history(history_file.as_path())?;
    }
    println!("Welcome to Lig SQL shell!");
    loop {
        let readline = rl.readline("\x1b[90m>\x1b[0m ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.to_owned())?;
                match conn.query(line) {
                    Ok(Some(ref mut rows)) => match opts.output_mode {
                        OutputMode::Raw => {
                            while let Some(row) = rows.next()? {
                                print!("|");
                                for val in row.values.iter() {
                                    match val {
                                        Value::Null => print!("NULL|"),
                                        Value::Integer(i) => print!("{}|", i),
                                        Value::Float(f) => print!("{}|", f),
                                        Value::Text(s) => print!("{}|", s),
                                        Value::Blob(b) => print!("{:?}|", b),
                                    }
                                }
                                println!();
                            }
                        }
                        OutputMode::Pretty => {
                            let mut table_rows: Vec<Vec<_>> = vec![];
                            while let Some(row) = rows.next()? {
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
                            let table = table_rows.table();
                            cli_table::print_stdout(table).unwrap();
                        }
                    },
                    Ok(None) => {}
                    Err(err) => {
                        eprintln!("{}", err);
                    }
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
