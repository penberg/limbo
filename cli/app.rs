use crate::opcodes_dictionary::OPCODE_DESCRIPTIONS;
use cli_table::{Cell, Table};
use limbo_core::{Database, LimboError, RowResult, Value};

use clap::{Parser, ValueEnum};
use std::{
    io::{self, Write},
    path::PathBuf,
    rc::Rc,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Parser)]
#[command(name = "limbo")]
#[command(author, version, about, long_about = None)]
pub struct Opts {
    #[clap(index = 1)]
    pub database: Option<PathBuf>,
    #[clap(index = 2)]
    pub sql: Option<String>,
    #[clap(short = 'm', long, default_value_t = OutputMode::Raw)]
    pub output_mode: OutputMode,
    #[clap(short, long, default_value = "")]
    pub output: String,
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
    /// Set output file (or stdout if empty)
    SetOutput,
    /// Set output display mode
    OutputMode,
    /// Show vdbe opcodes
    Opcodes,
    /// Change the current working directory
    Cwd,
    /// Display information about settings
    ShowInfo,
    /// Set the value of NULL to be printedin 'raw' mode
    NullValue,
}

impl Command {
    fn min_args(&self) -> usize {
        (match self {
            Self::Quit
            | Self::Schema
            | Self::Help
            | Self::Opcodes
            | Self::ShowInfo
            | Self::SetOutput => 0,
            Self::Open | Self::OutputMode | Self::Cwd | Self::NullValue => 1,
        } + 1) // argv0
    }

    fn useage(&self) -> &str {
        match self {
            Self::Quit => ".quit",
            Self::Open => ".open <file>",
            Self::Help => ".help",
            Self::Schema => ".schema ?<table>?",
            Self::Opcodes => ".opcodes",
            Self::OutputMode => ".mode <mode>",
            Self::SetOutput => ".output ?file?",
            Self::Cwd => ".cd <directory>",
            Self::ShowInfo => ".show",
            Self::NullValue => ".nullvalue <string>",
        }
    }
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
            ".nullvalue" => Ok(Self::NullValue),
            _ => Err("Unknown command".to_string()),
        }
    }
}

const PROMPT: &str = "limbo> ";

pub struct Limbo {
    pub prompt: String,
    io: Arc<dyn limbo_core::IO>,
    writer: Box<dyn Write>,
    conn: Rc<limbo_core::Connection>,
    output_filename: String,
    db_file: String,
    output_mode: OutputMode,
    is_stdout: bool,
    input_buff: String,
    null_value: String,
}

impl Limbo {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new(opts: &Opts) -> anyhow::Result<Self> {
        let io: Arc<dyn limbo_core::IO> = match opts.database {
            Some(ref path) if path.exists() => Arc::new(limbo_core::PlatformIO::new()?),
            _ => Arc::new(limbo_core::MemoryIO::new()?),
        };
        let db_file = opts
            .database
            .as_ref()
            .map_or(":memory:".to_string(), |p| p.to_string_lossy().to_string());
        let db = Database::open_file(io.clone(), &db_file)?;
        let conn = db.connect();
        let writer: Box<dyn Write> = if !opts.output.is_empty() {
            Box::new(std::fs::File::create(&opts.output)?)
        } else {
            Box::new(io::stdout())
        };
        let output = if opts.output.is_empty() {
            "STDOUT"
        } else {
            &opts.output
        };
        Ok(Self {
            prompt: PROMPT.to_string(),
            io,
            writer,
            output_filename: output.to_string(),
            conn,
            db_file,
            output_mode: opts.output_mode,
            is_stdout: true,
            input_buff: String::new(),
            null_value: String::new(),
        })
    }

    fn set_multiline_prompt(&mut self) {
        self.prompt = match self.input_buff.chars().fold(0, |acc, c| match c {
            '(' => acc + 1,
            ')' => acc - 1,
            _ => acc,
        }) {
            n if n < 0 => String::from(")x!...>"),
            0 => String::from("   ...> "),
            n if n < 10 => format!("(x{}...> ", n),
            _ => String::from("(.....> "),
        };
    }

    fn show_info(&mut self) -> std::io::Result<()> {
        self.writeln("------------------------------\nCurrent settings:")?;
        self.writeln(format!("Output mode: {}", self.output_mode))?;
        self.writeln(format!("DB filename: {}", self.db_file))?;
        self.writeln(format!("Output: {}", self.output_filename))?;
        self.writeln(format!(
            "CWD: {}",
            std::env::current_dir().unwrap().display()
        ))?;
        let null_value = if self.null_value.is_empty() {
            "\'\'".to_string()
        } else {
            self.null_value.clone()
        };
        self.writeln(format!("Null value: {}", null_value))?;
        self.writer.flush()
    }

    pub fn reset_input(&mut self) {
        self.prompt = PROMPT.to_string();
        self.input_buff.clear();
    }

    pub fn close_conn(&mut self) -> Result<(), LimboError> {
        self.conn.close()
    }

    fn open_db(&mut self, path: &str) -> anyhow::Result<()> {
        self.conn.close()?;
        match path {
            ":memory:" => {
                let io = Arc::new(limbo_core::MemoryIO::new()?);
                let db = Database::open_file(io.clone(), path)?;
                self.conn = db.connect();
                self.db_file = ":memory:".to_string();
                return Ok(());
            }
            path => {
                let db = Database::open_file(self.io.clone(), path)?;
                self.conn = db.connect();
                self.db_file = path.to_string();
                return Ok(());
            }
        }
    }

    fn set_output_file(&mut self, path: &str) -> Result<(), String> {
        match std::fs::File::create(path) {
            Ok(file) => {
                self.writer = Box::new(file);
                self.is_stdout = false;
                return Ok(());
            }
            Err(e) => {
                return Err(e.to_string());
            }
        }
    }

    fn set_output_stdout(&mut self) {
        let _ = self.writer.flush();
        self.writer = Box::new(io::stdout());
        self.is_stdout = true;
    }

    fn set_mode(&mut self, mode: OutputMode) {
        self.output_mode = mode;
    }

    fn writeln<D: AsRef<[u8]>>(&mut self, data: D) -> io::Result<()> {
        self.writer.write_all(data.as_ref())?;
        self.writer.write_all(b"\n")
    }

    fn buffer_input(&mut self, line: &str) {
        self.input_buff.push_str(line);
        self.input_buff.push(' ');
    }

    pub fn handle_input_line(
        &mut self,
        line: &str,
        interrupt_count: &Arc<AtomicUsize>,
        rl: &mut rustyline::DefaultEditor,
    ) -> anyhow::Result<()> {
        if self.input_buff.is_empty() {
            if line.is_empty() {
                return Ok(());
            }
            if line.starts_with('.') {
                self.handle_dot_command(line);
                rl.add_history_entry(line.to_owned())?;
                interrupt_count.store(0, Ordering::SeqCst);
                return Ok(());
            }
        }
        if line.ends_with(';') {
            self.buffer_input(line);
            let buff = self.input_buff.clone();
            buff.split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .for_each(|stmt| {
                    if let Err(e) = self.query(stmt, interrupt_count) {
                        let _ = self.writeln(e.to_string());
                    }
                });
            self.reset_input();
        } else {
            self.buffer_input(line);
            self.set_multiline_prompt();
        }
        rl.add_history_entry(line.to_owned())?;
        interrupt_count.store(0, Ordering::SeqCst);
        Ok(())
    }

    pub fn handle_dot_command(&mut self, line: &str) {
        let args: Vec<&str> = line.split_whitespace().collect();
        if args.is_empty() {
            return;
        }
        if let Ok(ref cmd) = Command::from_str(args[0]) {
            if args.len() < cmd.min_args() {
                let _ = self.writeln(format!("Insufficient arguments: USAGE: {}", cmd.useage()));
                return;
            }
            match cmd {
                Command::Quit => {
                    let _ = self.writeln("Exiting Limbo SQL Shell.");
                    let _ = self.close_conn();
                    std::process::exit(0)
                }
                Command::Open => {
                    if self.open_db(args[1]).is_err() {
                        let _ = self.writeln("Error: Unable to open database file.");
                    }
                }
                Command::Schema => {
                    let table_name = args.get(1).copied();
                    if let Err(e) = self.display_schema(table_name) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Opcodes => {
                    if args.len() > 1 {
                        for op in &OPCODE_DESCRIPTIONS {
                            if op.name.eq_ignore_ascii_case(args.get(1).unwrap().trim()) {
                                let _ = self.writeln(format!("{}", op));
                            }
                        }
                    } else {
                        for op in &OPCODE_DESCRIPTIONS {
                            let _ = self.writeln(format!("{}\n", op));
                        }
                    }
                }
                Command::NullValue => {
                    self.null_value = args[1].to_string();
                }
                Command::OutputMode => match OutputMode::from_str(args[1], true) {
                    Ok(mode) => {
                        self.set_mode(mode);
                    }
                    Err(e) => {
                        let _ = self.writeln(e);
                    }
                },
                Command::SetOutput => {
                    if args.len() == 2 {
                        if let Err(e) = self.set_output_file(args[1]) {
                            let _ = self.writeln(format!("Error: {}", e));
                        }
                    } else {
                        self.set_output_stdout();
                    }
                }
                Command::Cwd => {
                    let _ = std::env::set_current_dir(args[1]);
                }
                Command::ShowInfo => {
                    let _ = self.show_info();
                }
                Command::Help => {
                    let _ = self.writeln(HELP_MSG);
                }
            }
        } else {
            let _ = self.writeln(format!(
                "Unknown command: {}\nenter: .help for all available commands",
                args[0]
            ));
        }
    }

    pub fn query(&mut self, sql: &str, interrupt_count: &Arc<AtomicUsize>) -> anyhow::Result<()> {
        match self.conn.query(sql) {
            Ok(Some(ref mut rows)) => match self.output_mode {
                OutputMode::Raw => loop {
                    if interrupt_count.load(Ordering::SeqCst) > 0 {
                        println!("Query interrupted.");
                        return Ok(());
                    }

                    match rows.next_row() {
                        Ok(RowResult::Row(row)) => {
                            for (i, value) in row.values.iter().enumerate() {
                                if i > 0 {
                                    let _ = self.writer.write(b"|");
                                }
                                let _ = self.writer.write(
                                    match value {
                                        Value::Null => self.null_value.clone(),
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
                            let _ = self.writeln("");
                        }
                        Ok(RowResult::IO) => {
                            self.io.run_once()?;
                        }
                        Ok(RowResult::Done) => {
                            break;
                        }
                        Err(err) => {
                            let _ = self.writeln(err.to_string());
                            break;
                        }
                    }
                },
                OutputMode::Pretty => {
                    if interrupt_count.load(Ordering::SeqCst) > 0 {
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
                                            Value::Null => self.null_value.clone().cell(),
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
                                let _ = self.writeln(format!("{}", err));
                                break;
                            }
                        }
                    }
                    if let Ok(table) = table_rows.table().display() {
                        let _ = self.writeln(format!("{}", table));
                    } else {
                        let _ = self.writeln("Error displaying table.");
                    }
                }
            },
            Ok(None) => {}
            Err(err) => {
                let _ = self.writeln(format!("{}", err));
            }
        }
        // for now let's cache flush always
        self.conn.cacheflush()?;
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

        match self.conn.query(&sql) {
            Ok(Some(ref mut rows)) => {
                let mut found = false;
                loop {
                    match rows.next_row()? {
                        RowResult::Row(row) => {
                            if let Some(Value::Text(schema)) = row.values.first() {
                                let _ = self.writeln(format!("{};", schema));
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
                        let _ = self.writeln(format!("Error: Table '{}' not found.", table_name));
                    } else {
                        let _ = self.writeln("No tables or indexes found in the database.");
                    }
                }
            }
            Ok(None) => {
                let _ = self.writeln("No results returned from the query.");
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
.show                      Display current settings.
.open <database_file>      Open and connect to a database file.
.output <mode>             Change the output mode. Available modes are 'raw' and 'pretty'.
.schema <table_name>       Show the schema of the specified table.
.opcodes                   Display all the opcodes defined by the virtual machine
.cd <directory>            Change the current working directory.
.nullvalue <string>        Set the value to be displayed for null values.
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

9. Set the value 'NULL' to be displayed for null values instead of empty string:
   .nullvalue "NULL"

Note:
-----
- All SQL commands must end with a semicolon (;).
- Special commands do not require a semicolon.

"#;
