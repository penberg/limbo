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
    #[clap(index = 1, help = "SQLite database file", default_value = ":memory:")]
    pub database: Option<PathBuf>,
    #[clap(index = 2, help = "Optional SQL command to execute")]
    pub sql: Option<String>,
    #[clap(short = 'm', long, default_value_t = OutputMode::Raw)]
    pub output_mode: OutputMode,
    #[clap(short, long, default_value = "")]
    pub output: String,
    #[clap(
        short,
        long,
        help = "don't display program information on start",
        default_value_t = false
    )]
    pub quiet: bool,
    #[clap(short, long, help = "Print commands before execution")]
    pub echo: bool,
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
    /// Toggle 'echo' mode to repeat commands before execution
    Echo,
    /// Display tables
    Tables,
}

impl Command {
    fn min_args(&self) -> usize {
        (match self {
            Self::Quit
            | Self::Schema
            | Self::Help
            | Self::Opcodes
            | Self::ShowInfo
            | Self::Tables
            | Self::SetOutput => 0,
            Self::Open | Self::OutputMode | Self::Cwd | Self::Echo | Self::NullValue => 1,
        } + 1) // argv0
    }

    fn usage(&self) -> &str {
        match self {
            Self::Quit => ".quit",
            Self::Open => ".open <file>",
            Self::Help => ".help",
            Self::Schema => ".schema ?<table>?",
            Self::Opcodes => ".opcodes",
            Self::OutputMode => ".mode raw|pretty",
            Self::SetOutput => ".output ?file?",
            Self::Cwd => ".cd <directory>",
            Self::ShowInfo => ".show",
            Self::NullValue => ".nullvalue <string>",
            Self::Echo => ".echo on|off",
            Self::Tables => ".tables",
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
            ".tables" => Ok(Self::Tables),
            ".opcodes" => Ok(Self::Opcodes),
            ".mode" => Ok(Self::OutputMode),
            ".output" => Ok(Self::SetOutput),
            ".cd" => Ok(Self::Cwd),
            ".show" => Ok(Self::ShowInfo),
            ".nullvalue" => Ok(Self::NullValue),
            ".echo" => Ok(Self::Echo),
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
    pub interrupt_count: Arc<AtomicUsize>,
    input_buff: String,
    opts: Settings,
}

pub struct Settings {
    output_filename: String,
    db_file: String,
    null_value: String,
    output_mode: OutputMode,
    echo: bool,
    is_stdout: bool,
}

impl From<&Opts> for Settings {
    fn from(opts: &Opts) -> Self {
        Self {
            null_value: String::new(),
            output_mode: opts.output_mode,
            echo: false,
            is_stdout: opts.output == "",
            output_filename: opts.output.clone(),
            db_file: opts
                .database
                .as_ref()
                .map_or(":memory:".to_string(), |p| p.to_string_lossy().to_string()),
        }
    }
}

impl std::fmt::Display for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Settings:\nOutput mode: {}\nDB: {}\nOutput: {}\nNull value: {}\nCWD: {}\nEcho: {}",
            self.output_mode,
            self.db_file,
            match self.is_stdout {
                true => "STDOUT",
                false => &self.output_filename,
            },
            self.null_value,
            std::env::current_dir().unwrap().display(),
            match self.echo {
                true => "on",
                false => "off",
            }
        )
    }
}

impl Limbo {
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn new() -> anyhow::Result<Self> {
        let opts = Opts::parse();
        let db_file = opts
            .database
            .as_ref()
            .map_or(":memory:".to_string(), |p| p.to_string_lossy().to_string());

        let io = get_io(&db_file)?;
        let db = Database::open_file(io.clone(), &db_file)?;
        let conn = db.connect();
        let interrupt_count = Arc::new(AtomicUsize::new(0));
        {
            let interrupt_count: Arc<AtomicUsize> = Arc::clone(&interrupt_count);
            ctrlc::set_handler(move || {
                // Increment the interrupt count on Ctrl-C
                interrupt_count.fetch_add(1, Ordering::SeqCst);
            })
            .expect("Error setting Ctrl-C handler");
        }
        let mut app = Self {
            prompt: PROMPT.to_string(),
            io,
            writer: get_writer(&opts.output),
            conn,
            interrupt_count,
            input_buff: String::new(),
            opts: Settings::from(&opts),
        };
        if opts.sql.is_some() {
            app.handle_first_input(opts.sql.as_ref().unwrap());
        }
        if !opts.quiet {
            app.write_fmt(format_args!("Limbo v{}", env!("CARGO_PKG_VERSION")))?;
            app.writeln("Enter \".help\" for usage hints.")?;
            app.display_in_memory()?;
        }
        return Ok(app);
    }

    fn handle_first_input(&mut self, cmd: &str) {
        if cmd.trim().starts_with('.') {
            self.handle_dot_command(&cmd);
        } else if let Err(e) = self.query(&cmd) {
            eprintln!("{}", e);
        }
        std::process::exit(0);
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

    fn display_in_memory(&mut self) -> std::io::Result<()> {
        if self.opts.db_file == ":memory:" {
            self.writeln("Connected to a transient in-memory database.")?;
            self.writeln("Use \".open FILENAME\" to reopen on a persistent database")?;
        }
        Ok(())
    }

    fn show_info(&mut self) -> std::io::Result<()> {
        let opts = format!("{}", self.opts);
        self.writeln(opts)
    }

    pub fn reset_input(&mut self) {
        self.prompt = PROMPT.to_string();
        self.input_buff.clear();
    }

    pub fn close_conn(&mut self) -> Result<(), LimboError> {
        self.conn.close()
    }

    fn toggle_echo(&mut self, arg: &str) {
        match arg.trim().to_lowercase().as_str() {
            "on" => self.opts.echo = true,
            "off" => self.opts.echo = false,
            _ => {}
        }
    }

    fn open_db(&mut self, path: &str) -> anyhow::Result<()> {
        self.conn.close()?;
        match path {
            ":memory:" => {
                let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::MemoryIO::new()?);
                self.io = Arc::clone(&io);
                let db = Database::open_file(self.io.clone(), path)?;
                self.conn = db.connect();
                self.opts.db_file = ":memory:".to_string();
                return Ok(());
            }
            path => {
                let io: Arc<dyn limbo_core::IO> = Arc::new(limbo_core::PlatformIO::new()?);
                self.io = Arc::clone(&io);
                let db = Database::open_file(self.io.clone(), path)?;
                self.conn = db.connect();
                self.opts.db_file = path.to_string();
                return Ok(());
            }
        }
    }

    fn set_output_file(&mut self, path: &str) -> Result<(), String> {
        if path.is_empty() || path.trim().eq_ignore_ascii_case("stdout") {
            self.set_output_stdout();
            return Ok(());
        }
        match std::fs::File::create(path) {
            Ok(file) => {
                self.writer = Box::new(file);
                self.opts.is_stdout = false;
                self.opts.output_mode = OutputMode::Raw;
                self.opts.output_filename = path.to_string();
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
        self.opts.is_stdout = true;
    }

    fn set_mode(&mut self, mode: OutputMode) -> Result<(), String> {
        if mode == OutputMode::Pretty && !self.opts.is_stdout {
            return Err("pretty output can only be written to a tty".to_string());
        } else {
            self.opts.output_mode = mode;
            Ok(())
        }
    }

    fn write_fmt(&mut self, fmt: std::fmt::Arguments) -> io::Result<()> {
        let _ = self.writer.write_fmt(fmt);
        self.writer.write_all(b"\n")
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
        rl: &mut rustyline::DefaultEditor,
    ) -> anyhow::Result<()> {
        if self.input_buff.is_empty() {
            if line.is_empty() {
                return Ok(());
            }
            if line.starts_with('.') {
                self.handle_dot_command(line);
                rl.add_history_entry(line.to_owned())?;
                self.interrupt_count.store(0, Ordering::SeqCst);
                return Ok(());
            }
        }
        if line.ends_with(';') {
            self.buffer_input(line);
            let buff = self.input_buff.clone();
            let echo = self.opts.echo;
            buff.split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .for_each(|stmt| {
                    if echo {
                        let _ = self.writeln(stmt);
                    }
                    if let Err(e) = self.query(stmt) {
                        let _ = self.writeln(e.to_string());
                    }
                });
            self.reset_input();
        } else {
            self.buffer_input(line);
            self.set_multiline_prompt();
        }
        rl.add_history_entry(line.to_owned())?;
        self.interrupt_count.store(0, Ordering::SeqCst);
        Ok(())
    }

    pub fn handle_dot_command(&mut self, line: &str) {
        let args: Vec<&str> = line.split_whitespace().collect();
        if args.is_empty() {
            return;
        }
        if let Ok(ref cmd) = Command::from_str(args[0]) {
            if args.len() < cmd.min_args() {
                let _ = self.write_fmt(format_args!(
                    "Insufficient arguments: USAGE: {}",
                    cmd.usage()
                ));
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
                Command::Tables => {
                    let pattern = args.get(1).copied();
                    if let Err(e) = self.display_tables(pattern) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Opcodes => {
                    if args.len() > 1 {
                        for op in &OPCODE_DESCRIPTIONS {
                            if op.name.eq_ignore_ascii_case(args.get(1).unwrap().trim()) {
                                let _ = self.write_fmt(format_args!("{}", op));
                            }
                        }
                    } else {
                        for op in &OPCODE_DESCRIPTIONS {
                            let _ = self.write_fmt(format_args!("{}\n", op));
                        }
                    }
                }
                Command::NullValue => {
                    self.opts.null_value = args[1].to_string();
                }
                Command::OutputMode => match OutputMode::from_str(args[1], true) {
                    Ok(mode) => {
                        if let Err(e) = self.set_mode(mode) {
                            let _ = self.write_fmt(format_args!("Error: {}", e));
                        }
                    }
                    Err(e) => {
                        let _ = self.writeln(e);
                    }
                },
                Command::SetOutput => {
                    if args.len() == 2 {
                        if let Err(e) = self.set_output_file(args[1]) {
                            let _ = self.write_fmt(format_args!("Error: {}", e));
                        }
                    } else {
                        self.set_output_stdout();
                    }
                }
                Command::Echo => {
                    self.toggle_echo(args[1]);
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
            let _ = self.write_fmt(format_args!(
                "Unknown command: {}\nenter: .help for all available commands",
                args[0]
            ));
        }
    }

    pub fn query(&mut self, sql: &str) -> anyhow::Result<()> {
        match self.conn.query(sql) {
            Ok(Some(ref mut rows)) => match self.opts.output_mode {
                OutputMode::Raw => loop {
                    if self.interrupt_count.load(Ordering::SeqCst) > 0 {
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
                                        Value::Null => self.opts.null_value.clone(),
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
                        Ok(RowResult::Interrupt) => break,
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
                                            Value::Null => self.opts.null_value.clone().cell(),
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
                            Ok(RowResult::Interrupt) => break,
                            Ok(RowResult::Done) => break,
                            Err(err) => {
                                let _ = self.write_fmt(format_args!("{}", err));
                                break;
                            }
                        }
                    }
                    if let Ok(table) = table_rows.table().display() {
                        let _ = self.write_fmt(format_args!("{}", table));
                    } else {
                        let _ = self.writeln("Error displaying table.");
                    }
                }
            },
            Ok(None) => {}
            Err(err) => {
                let _ = self.write_fmt(format_args!("{}", err));
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
                                let _ = self.write_fmt(format_args!("{};", schema));
                                found = true;
                            }
                        }
                        RowResult::IO => {
                            self.io.run_once()?;
                        }
                        RowResult::Interrupt => break,
                        RowResult::Done => break,
                    }
                }
                if !found {
                    if let Some(table_name) = table {
                        let _ = self
                            .write_fmt(format_args!("Error: Table '{}' not found.", table_name));
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

    fn display_tables(&mut self, pattern: Option<&str>) -> anyhow::Result<()> {
        let sql = match pattern {
            Some(pattern) => format!(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name LIKE '{}' ORDER BY 1",
                pattern
            ),
            None => String::from(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY 1"
            ),
        };

        match self.conn.query(&sql) {
            Ok(Some(ref mut rows)) => {
                let mut tables = String::new();
                loop {
                    match rows.next_row()? {
                        RowResult::Row(row) => {
                            if let Some(Value::Text(table)) = row.values.first() {
                                tables.push_str(table);
                                tables.push(' ');
                            }
                        }
                        RowResult::IO => {
                            self.io.run_once()?;
                        }
                        RowResult::Interrupt => break,
                        RowResult::Done => break,
                    }
                }

                if tables.len() > 0 {
                    let _ = self.writeln(tables.trim_end());
                } else {
                    if let Some(pattern) = pattern {
                        let _ = self.write_fmt(format_args!(
                            "Error: Tables with pattern '{}' not found.",
                            pattern
                        ));
                    } else {
                        let _ = self.writeln("No tables found in the database.");
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

fn get_writer(output: &str) -> Box<dyn Write> {
    match output {
        "" => Box::new(io::stdout()),
        _ => match std::fs::File::create(output) {
            Ok(file) => Box::new(file),
            Err(e) => {
                eprintln!("Error: {}", e);
                Box::new(io::stdout())
            }
        },
    }
}

fn get_io(db: &str) -> anyhow::Result<Arc<dyn limbo_core::IO>> {
    Ok(match db {
        ":memory:" => Arc::new(limbo_core::MemoryIO::new()?),
        _ => Arc::new(limbo_core::PlatformIO::new()?),
    })
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
.tables <pattern>          List names of tables matching LIKE pattern TABLE
.opcodes                   Display all the opcodes defined by the virtual machine
.cd <directory>            Change the current working directory.
.nullvalue <string>        Set the value to be displayed for null values.
.echo on|off               Toggle echo mode to repeat commands before execution.
.help                      Display this help message.

Usage Examples:
---------------
1. To quit the Limbo SQL Shell:
   .quit

2. To open a database file at path './employees.db':
   .open employees.db

3. To view the schema of a table named 'employees':
   .schema employees

4. To list all tables:
   .tables

5. To list all available SQL opcodes:
   .opcodes

6. To change the current output mode to 'pretty':
   .mode pretty

7. Send output to STDOUT if no file is specified:
   .output

8. To change the current working directory to '/tmp':
   .cd /tmp

9. Show the current values of settings:
   .show

Note:
- All SQL commands must end with a semicolon (;).
- Special commands do not require a semicolon."#;
