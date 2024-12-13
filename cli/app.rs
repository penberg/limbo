use crate::opcodes_dictionary::OPCODE_DESCRIPTIONS;
use cli_table::{Cell, Table};
use limbo_core::{Database, RowResult, Value, IO};

use clap::{Parser, ValueEnum};
use std::{
    io::{self, LineWriter, Write},
    path::PathBuf,
    rc::Rc,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Opts {
    pub database: Option<PathBuf>,
    pub sql: Option<String>,
    #[clap(short, long, default_value_t = OutputMode::Raw)]
    pub output_mode: OutputMode,
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
pub enum OutputMode {
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

#[derive(Debug, Clone)]
pub enum Command {
    /// Quit the shell
    Quit,
    /// Open a database file
    Open,
    /// Display help message
    Help,
    /// Display schema for a table
    Schema,
    /// Set output file (stdout or file)
    SetOutput,
    /// Set output display mode
    OutputMode,
    /// Show vdbe opcodes
    Opcodes,
    /// Change the current working directory
    Cwd,
    /// Display information about settings
    ShowInfo,
}

impl FromStr for Command {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            ".quit" => Ok(Self::Quit),
            ".open" => Ok(Self::Open),
            ".help" => Ok(Self::Help),
            ".schema" => Ok(Self::Schema),
            ".opcodes" => Ok(Self::Opcodes),
            ".mode" => Ok(Self::OutputMode),
            ".output" => Ok(Self::SetOutput),
            ".cd" => Ok(Self::Cwd),
            ".show" => Ok(Self::ShowInfo),
            _ => Err("Unknown command".to_string()),
        }
    }
}

pub struct Limbo {
    io: Arc<limbo_core::PlatformIO>,
    writer: Box<dyn Write>,
    conn: Option<Rc<limbo_core::Connection>>,
    filename: Option<String>,
    db_file: Option<String>,
    pub interrupt_count: Arc<AtomicUsize>,
    pub output_mode: OutputMode,
    pub is_stdout: bool,
}

impl Limbo {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new(opts: &Opts) -> anyhow::Result<Self> {
        println!("Limbo v{}", env!("CARGO_PKG_VERSION"));
        println!("Enter \".help\" for usage hints.");
        let io = Arc::new(limbo_core::PlatformIO::new()?);
        let mut db_file = None;
        let conn = if let Some(path) = &opts.database {
            let path = path.to_str().unwrap();
            db_file = Some(path.to_string());
            let db = Database::open_file(io.clone(), path)?;
            Some(db.connect())
        } else {
            println!("No database file specified: Use .open <file> to open a database.");
            None
        };
        Ok(Self {
            io: Arc::new(limbo_core::PlatformIO::new()?),
            writer: Box::new(LineWriter::new(std::io::stdout())),
            filename: None,
            conn,
            db_file,
            interrupt_count: AtomicUsize::new(0).into(),
            output_mode: opts.output_mode,
            is_stdout: true,
        })
    }

    fn show_info(&mut self) {
        self.writeln("------------------------------\nCurrent settings:");
        self.writeln(format!("Output mode: {}", self.output_mode));
        let output = self
            .filename
            .as_ref()
            .unwrap_or(&"STDOUT".to_string())
            .clone();
        self.writeln(format!("Output mode: {output}"));
        self.writeln(format!(
            "DB filename: {}",
            self.db_file.clone().unwrap_or(":none:".to_string())
        ));
        self.writeln(format!(
            "CWD: {}",
            std::env::current_dir().unwrap().display()
        ));
        let _ = self.writer.flush();
    }

    pub fn close(&mut self) {
        self.conn.as_mut().map(|c| c.close());
    }

    pub fn open_db(&mut self, path: &str) -> anyhow::Result<()> {
        let db = Database::open_file(self.io.clone(), path)?;
        self.conn = Some(db.connect());
        self.db_file = Some(path.to_string());
        Ok(())
    }

    pub fn set_output_file(&mut self, path: &str) -> io::Result<()> {
        if let Ok(file) = std::fs::File::create(path) {
            self.writer = Box::new(file);
            self.is_stdout = false;
            return Ok(());
        }
        Err(io::Error::new(io::ErrorKind::NotFound, "File not found"))
    }

    fn set_output_stdout(&mut self) {
        self.writer = Box::new(io::stdout());
        self.is_stdout = true;
    }

    pub fn set_mode(&mut self, mode: OutputMode) {
        self.output_mode = mode;
    }

    pub fn write<D: AsRef<[u8]>>(&mut self, data: D) -> io::Result<()> {
        self.writer.write_all(data.as_ref())
    }

    pub fn writeln<D: AsRef<[u8]>>(&mut self, data: D) {
        self.writer.write_all(data.as_ref()).unwrap();
        self.writer.write_all(b"\n").unwrap();
        let _ = self.writer.flush();
    }

    pub fn reset_interrupt_count(&self) {
        self.interrupt_count
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn display_help_message(&mut self) {
        let _ = self.writer.write_all(HELP_MSG.as_ref());
    }

    pub fn incr_inturrupt_count(&mut self) -> usize {
        self.interrupt_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub fn handle_dot_command(&mut self, line: &str) {
        let args: Vec<&str> = line.split_whitespace().collect();
        if args.is_empty() {
            return;
        }

        match Command::from_str(args[0]) {
            Ok(Command::Quit) => {
                println!("Exiting Limbo SQL Shell.");
                self.close();
                std::process::exit(0)
            }
            Ok(Command::Open) => {
                if args.len() < 2 {
                    println!("Error: No database file specified.");
                } else if self.open_db(args[1]).is_err() {
                    println!("Error: Unable to open database file.");
                }
            }
            Ok(Command::Schema) => {
                if self.conn.is_none() {
                    println!("Error: no database currently open");
                    return;
                }
                let table_name = args.get(1).copied();
                let _ = self.display_schema(table_name);
            }
            Ok(Command::Opcodes) => {
                if args.len() > 1 {
                    for op in &OPCODE_DESCRIPTIONS {
                        if op.name.eq_ignore_ascii_case(args.get(1).unwrap()) {
                            self.writeln(format!("{}", op));
                        }
                    }
                } else {
                    for op in &OPCODE_DESCRIPTIONS {
                        println!("{}\n", op);
                    }
                }
            }
            Ok(Command::OutputMode) => {
                if args.len() < 2 {
                    println!("Error: No output mode specified.");
                    return;
                }
                match OutputMode::from_str(args[1], true) {
                    Ok(mode) => {
                        self.set_mode(mode);
                    }
                    Err(e) => {
                        println!("{e}");
                    }
                }
            }
            Ok(Command::SetOutput) => {
                if args.len() == 2 {
                    if let Err(e) = self.set_output_file(args[1]) {
                        println!("Error: {}", e);
                    }
                } else {
                    self.set_output_stdout();
                }
            }
            Ok(Command::Cwd) => {
                if args.len() < 2 {
                    println!("USAGE: .cd <directory>");
                    return;
                }
                let _ = std::env::set_current_dir(args[1]);
            }
            Ok(Command::ShowInfo) => {
                self.show_info();
            }
            Ok(Command::Help) => {
                self.display_help_message();
            }
            _ => {
                println!("Unknown command: {}", args[0]);
                println!("enter: .help for all available commands");
            }
        }
    }

    pub fn query(&mut self, sql: &str) -> anyhow::Result<()> {
        if self.conn.is_none() {
            println!("Error: No database file specified.");
            return Ok(());
        }
        let conn = self.conn.as_ref().unwrap().clone();
        match conn.query(sql) {
            Ok(Some(ref mut rows)) => match self.output_mode {
                OutputMode::Raw => loop {
                    if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                        println!("Query interrupted.");
                        return Ok(());
                    }

                    match rows.next_row() {
                        Ok(RowResult::Row(row)) => {
                            for (i, value) in row.values.iter().enumerate() {
                                if i > 0 {
                                    let _ = self.write(b"|");
                                }
                                self.write(
                                    match value {
                                        Value::Null => "".to_string(),
                                        Value::Integer(i) => format!("{}", i),
                                        Value::Float(f) => format!("{:?}", f),
                                        Value::Text(s) => s.to_string(),
                                        Value::Blob(b) => {
                                            format!("{}", String::from_utf8_lossy(b))
                                        }
                                    }
                                    .as_bytes(),
                                )?;
                            }
                            self.writeln("");
                        }
                        Ok(RowResult::IO) => {
                            self.io.run_once()?;
                        }
                        Ok(RowResult::Done) => {
                            break;
                        }
                        Err(err) => {
                            eprintln!("{}", err);
                            break;
                        }
                    }
                },
                OutputMode::Pretty => {
                    if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                        println!("Query interrupted.");
                        return Ok(());
                    }
                    let mut table_rows: Vec<Vec<_>> = vec![];
                    loop {
                        match rows.next_row() {
                            Ok(RowResult::Row(row)) => {
                                table_rows.push(
                                    row.values
                                        .iter()
                                        .map(|value| match value {
                                            Value::Null => "".cell(),
                                            Value::Integer(i) => i.to_string().cell(),
                                            Value::Float(f) => f.to_string().cell(),
                                            Value::Text(s) => s.cell(),
                                            Value::Blob(b) => {
                                                format!("{}", String::from_utf8_lossy(b)).cell()
                                            }
                                        })
                                        .collect(),
                                );
                            }
                            Ok(RowResult::IO) => {
                                self.io.run_once()?;
                            }
                            Ok(RowResult::Done) => break,
                            Err(err) => {
                                eprintln!("{}", err);
                                break;
                            }
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
        // for now let's cache flush always
        conn.cacheflush()?;
        Ok(())
    }

    fn display_schema(&mut self, table: Option<&str>) -> anyhow::Result<()> {
        let sql = match table {
        Some(table_name) => format!(
            "SELECT sql FROM sqlite_schema WHERE type IN ('table', 'index') AND tbl_name = '{}' AND name NOT LIKE 'sqlite_%'",
            table_name
        ),
        None => String::from(
            "SELECT sql FROM sqlite_schema WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%'"
        ),
    };

        match self.conn.as_ref().unwrap().query(&sql) {
            Ok(Some(ref mut rows)) => {
                let mut found = false;
                loop {
                    match rows.next_row()? {
                        RowResult::Row(row) => {
                            if let Some(Value::Text(schema)) = row.values.first() {
                                self.writeln(format!("{};", schema));
                                found = true;
                            }
                        }
                        RowResult::IO => {
                            self.io.run_once()?;
                        }
                        RowResult::Done => break,
                    }
                }
                if !found {
                    if let Some(table_name) = table {
                        self.writeln(format!("Error: Table '{}' not found.", table_name));
                    } else {
                        self.writeln("No tables or indexes found in the database.");
                    }
                }
            }
            Ok(None) => {
                println!("No results returned from the query.");
            }
            Err(err) => {
                if err.to_string().contains("no such table: sqlite_schema") {
                    return Err(anyhow::anyhow!("Unable to access database schema. The database may be using an older SQLite version or may not be properly initialized."));
                } else {
                    return Err(anyhow::anyhow!("Error querying schema: {}", err));
                }
            }
        }

        Ok(())
    }
}

const HELP_MSG: &str = r#"
Limbo SQL Shell Help
==============

Welcome to the Limbo SQL Shell! You can execute any standard SQL command here.
In addition to standard SQL commands, the following special commands are available:

Special Commands:
-----------------
.quit                      Stop interpreting input stream and exit.
.open <database_file>      Open and connect to a database file.
.output <mode>             Change the output mode. Available modes are 'raw' and 'pretty'.
.schema <table_name>       Show the schema of the specified table.
.opcodes                   Display all the opcodes defined by the virtual machine
.cd <directory>            Change the current working directory.
.help                      Display this help message.

Usage Examples:
---------------
1. To quit the Limbo SQL Shell:
   .quit

2. To open a database file at path './employees.db':
   .open employees.db

3. To view the schema of a table named 'employees':
   .schema employees

4. To list all available SQL opcodes:
   .opcodes

5. To change the current output mode to 'pretty':
   .mode pretty

6. Send output to STDOUT if no file is specified:
   .output

7. To change the current working directory to '/tmp':
   .cd /tmp

8. Show the current values of settings:
   .show

Note:
-----
- All SQL commands must end with a semicolon (;).
- Special commands do not require a semicolon.

"#;
