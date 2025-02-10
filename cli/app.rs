use crate::{
    import::{ImportFile, IMPORT_HELP},
    opcodes_dictionary::OPCODE_DESCRIPTIONS,
};
use comfy_table::{Attribute, Cell, CellAlignment, ContentArrangement, Row, Table};
use limbo_core::{Database, LimboError, Statement, StepResult, Value};

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
    #[clap(
        default_value_t,
        value_enum,
        short,
        long,
        help = "Select I/O backend. The only other choice to 'syscall' is\n\
        \t'io-uring' when built for Linux with feature 'io_uring'\n"
    )]
    pub io: Io,
}

#[derive(Copy, Clone)]
pub enum DbLocation {
    Memory,
    Path,
}

#[derive(Copy, Clone, ValueEnum)]
pub enum Io {
    Syscall,
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    IoUring,
}

impl Default for Io {
    /// Custom Default impl with cfg! macro, to provide compile-time default to Clap based on platform
    /// The cfg! could be elided, but Clippy complains
    /// The default value can still be overridden with the Clap argument
    fn default() -> Self {
        match cfg!(all(target_os = "linux", feature = "io_uring")) {
            true => {
                #[cfg(all(target_os = "linux", feature = "io_uring"))]
                {
                    Io::IoUring
                }
                #[cfg(any(
                    not(target_os = "linux"),
                    all(target_os = "linux", not(feature = "io_uring"))
                ))]
                {
                    Io::Syscall
                }
            }
            false => Io::Syscall,
        }
    }
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
    /// Import data from FILE into TABLE
    Import,
    /// Loads an extension library
    LoadExtension,
}

impl Command {
    fn min_args(&self) -> usize {
        1 + match self {
            Self::Quit
            | Self::Schema
            | Self::Help
            | Self::Opcodes
            | Self::ShowInfo
            | Self::Tables
            | Self::SetOutput => 0,
            Self::Open
            | Self::OutputMode
            | Self::Cwd
            | Self::Echo
            | Self::NullValue
            | Self::LoadExtension => 1,
            Self::Import => 2,
        } // argv0
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
            Self::LoadExtension => ".load",
            Self::Import => &IMPORT_HELP,
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
            ".import" => Ok(Self::Import),
            ".load" => Ok(Self::LoadExtension),
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
    io: Io,
}

impl From<&Opts> for Settings {
    fn from(opts: &Opts) -> Self {
        Self {
            null_value: String::new(),
            output_mode: opts.output_mode,
            echo: false,
            is_stdout: opts.output.is_empty(),
            output_filename: opts.output.clone(),
            db_file: opts
                .database
                .as_ref()
                .map_or(":memory:".to_string(), |p| p.to_string_lossy().to_string()),
            io: opts.io,
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
    pub fn new() -> anyhow::Result<Self> {
        let opts = Opts::parse();
        let db_file = opts
            .database
            .as_ref()
            .map_or(":memory:".to_string(), |p| p.to_string_lossy().to_string());

        let io = {
            match db_file.as_str() {
                ":memory:" => get_io(DbLocation::Memory, opts.io)?,
                _path => get_io(DbLocation::Path, opts.io)?,
            }
        };
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
        Ok(app)
    }

    fn handle_first_input(&mut self, cmd: &str) {
        if cmd.trim().starts_with('.') {
            self.handle_dot_command(cmd);
        } else {
            let conn = self.conn.clone();
            let runner = conn.query_runner(cmd.as_bytes());
            for output in runner {
                if let Err(e) = self.print_query_result(cmd, output) {
                    let _ = self.writeln(e.to_string());
                }
            }
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

    #[cfg(not(target_family = "wasm"))]
    fn handle_load_extension(&mut self, path: &str) -> Result<(), String> {
        let ext_path = limbo_core::resolve_ext_path(path).map_err(|e| e.to_string())?;
        self.conn
            .load_extension(ext_path)
            .map_err(|e| e.to_string())
    }

    fn display_in_memory(&mut self) -> io::Result<()> {
        if self.opts.db_file == ":memory:" {
            self.writeln("Connected to a transient in-memory database.")?;
            self.writeln("Use \".open FILENAME\" to reopen on a persistent database")?;
        }
        Ok(())
    }

    fn show_info(&mut self) -> io::Result<()> {
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
        let io = {
            match path {
                ":memory:" => get_io(DbLocation::Memory, self.opts.io)?,
                _path => get_io(DbLocation::Path, self.opts.io)?,
            }
        };
        self.io = Arc::clone(&io);
        let db = Database::open_file(self.io.clone(), path)?;
        self.conn = db.connect();
        self.opts.db_file = path.to_string();
        Ok(())
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
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        }
    }

    fn set_output_stdout(&mut self) {
        let _ = self.writer.flush();
        self.writer = Box::new(io::stdout());
        self.opts.is_stdout = true;
    }

    fn set_mode(&mut self, mode: OutputMode) -> Result<(), String> {
        if mode == OutputMode::Pretty && !self.opts.is_stdout {
            Err("pretty output can only be written to a tty".to_string())
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
        if line.trim_start().starts_with("--") {
            if let Some(remaining) = line.split_once('\n') {
                let after_comment = remaining.1.trim();
                if !after_comment.is_empty() {
                    rl.add_history_entry(after_comment.to_owned())?;
                    self.buffer_input(after_comment);

                    if after_comment.ends_with(';') {
                        if self.opts.echo {
                            let _ = self.writeln(after_comment);
                        }
                        let conn = self.conn.clone();
                        let runner = conn.query_runner(after_comment.as_bytes());
                        for output in runner {
                            if let Err(e) = self.print_query_result(after_comment, output) {
                                let _ = self.writeln(e.to_string());
                            }
                        }
                        self.reset_input();
                    } else {
                        self.set_multiline_prompt();
                    }
                    self.interrupt_count.store(0, Ordering::SeqCst);
                    return Ok(());
                }
            }
            return Ok(());
        }

        if let Some(comment_pos) = line.find("--") {
            let before_comment = line[..comment_pos].trim();
            if !before_comment.is_empty() {
                return self.handle_input_line(before_comment, rl);
            }
        }

        if line.ends_with(';') {
            self.buffer_input(line);
            let buff = self.input_buff.clone();
            let echo = self.opts.echo;
            if echo {
                let _ = self.writeln(&buff);
            }
            let conn = self.conn.clone();
            let runner = conn.query_runner(buff.as_bytes());
            for output in runner {
                if let Err(e) = self.print_query_result(&buff, output) {
                    let _ = self.writeln(e.to_string());
                }
            }
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
                Command::Import => {
                    let mut import_file =
                        ImportFile::new(self.conn.clone(), self.io.clone(), &mut self.writer);
                    if let Err(e) = import_file.import(&args) {
                        let _ = self.writeln(e.to_string());
                    };
                }
                Command::LoadExtension =>
                {
                    #[cfg(not(target_family = "wasm"))]
                    if let Err(e) = self.handle_load_extension(args[1]) {
                        let _ = self.writeln(&e);
                    }
                }
            }
        } else {
            let _ = self.write_fmt(format_args!(
                "Unknown command: {}\nenter: .help for all available commands",
                args[0]
            ));
        }
    }

    fn print_query_result(
        &mut self,
        sql: &str,
        mut output: Result<Option<Statement>, LimboError>,
    ) -> anyhow::Result<()> {
        match output {
            Ok(Some(ref mut rows)) => match self.opts.output_mode {
                OutputMode::Raw => loop {
                    if self.interrupt_count.load(Ordering::SeqCst) > 0 {
                        println!("Query interrupted.");
                        return Ok(());
                    }

                    match rows.step() {
                        Ok(StepResult::Row) => {
                            let row = rows.row().unwrap();
                            for (i, value) in row.get_values().iter().enumerate() {
                                let value = value.to_value();
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
                        Ok(StepResult::IO) => {
                            self.io.run_once()?;
                        }
                        Ok(StepResult::Interrupt) => break,
                        Ok(StepResult::Done) => {
                            break;
                        }
                        Ok(StepResult::Busy) => {
                            let _ = self.writeln("database is busy");
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
                    let mut table = Table::new();
                    table
                        .set_content_arrangement(ContentArrangement::Dynamic)
                        .set_truncation_indicator("…")
                        .apply_modifier("││──├─┼┤│─┼├┤┬┴┌┐└┘");
                    if rows.num_columns() > 0 {
                        let header = (0..rows.num_columns())
                            .map(|i| {
                                let name = rows.get_column_name(i).cloned().unwrap_or_default();
                                Cell::new(name).add_attribute(Attribute::Bold)
                            })
                            .collect::<Vec<_>>();
                        table.set_header(header);
                    }
                    loop {
                        match rows.step() {
                            Ok(StepResult::Row) => {
                                let record = rows.row().unwrap();
                                let mut row = Row::new();
                                row.max_height(1);
                                for value in record.get_values() {
                                    let (content, alignment) = match value.to_value() {
                                        Value::Null => {
                                            (self.opts.null_value.clone(), CellAlignment::Left)
                                        }
                                        Value::Integer(i) => (i.to_string(), CellAlignment::Right),
                                        Value::Float(f) => (f.to_string(), CellAlignment::Right),
                                        Value::Text(s) => (s.to_string(), CellAlignment::Left),
                                        Value::Blob(b) => (
                                            String::from_utf8_lossy(b).to_string(),
                                            CellAlignment::Left,
                                        ),
                                    };
                                    row.add_cell(Cell::new(content).set_alignment(alignment));
                                }
                                table.add_row(row);
                            }
                            Ok(StepResult::IO) => {
                                self.io.run_once()?;
                            }
                            Ok(StepResult::Interrupt) => break,
                            Ok(StepResult::Done) => break,
                            Ok(StepResult::Busy) => {
                                let _ = self.writeln("database is busy");
                                break;
                            }
                            Err(err) => {
                                let _ = self.write_fmt(format_args!(
                                    "{:?}",
                                    miette::Error::from(err).with_source_code(sql.to_owned())
                                ));
                                break;
                            }
                        }
                    }

                    if table.header().is_some() {
                        let _ = self.write_fmt(format_args!("{}", table));
                    }
                }
            },
            Ok(None) => {}
            Err(err) => {
                let _ = self.write_fmt(format_args!(
                    "{:?}",
                    miette::Error::from(err).with_source_code(sql.to_owned())
                ));
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
                    match rows.step()? {
                        StepResult::Row => {
                            let row = rows.row().unwrap();
                            if let Some(Value::Text(schema)) =
                                row.get_values().first().map(|v| v.to_value())
                            {
                                let _ = self.write_fmt(format_args!("{};", schema));
                                found = true;
                            }
                        }
                        StepResult::IO => {
                            self.io.run_once()?;
                        }
                        StepResult::Interrupt => break,
                        StepResult::Done => break,
                        StepResult::Busy => {
                            let _ = self.writeln("database is busy");
                            break;
                        }
                    }
                }
                if !found {
                    if let Some(table_name) = table {
                        let _ = self
                            .write_fmt(format_args!("-- Error: Table '{}' not found.", table_name));
                    } else {
                        let _ = self.writeln("-- No tables or indexes found in the database.");
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
                    match rows.step()? {
                        StepResult::Row => {
                            let row = rows.row().unwrap();
                            if let Some(Value::Text(table)) =
                                row.get_values().first().map(|v| v.to_value())
                            {
                                tables.push_str(table);
                                tables.push(' ');
                            }
                        }
                        StepResult::IO => {
                            self.io.run_once()?;
                        }
                        StepResult::Interrupt => break,
                        StepResult::Done => break,
                        StepResult::Busy => {
                            let _ = self.writeln("database is busy");
                            break;
                        }
                    }
                }

                if !tables.is_empty() {
                    let _ = self.writeln(tables.trim_end());
                } else if let Some(pattern) = pattern {
                    let _ = self.write_fmt(format_args!(
                        "Error: Tables with pattern '{}' not found.",
                        pattern
                    ));
                } else {
                    let _ = self.writeln("No tables found in the database.");
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

fn get_io(db_location: DbLocation, io_choice: Io) -> anyhow::Result<Arc<dyn limbo_core::IO>> {
    Ok(match db_location {
        DbLocation::Memory => Arc::new(limbo_core::MemoryIO::new()?),
        DbLocation::Path => {
            match io_choice {
                Io::Syscall => {
                    // We are building for Linux/macOS and syscall backend has been selected
                    #[cfg(target_family = "unix")]
                    {
                        Arc::new(limbo_core::UnixIO::new()?)
                    }
                    // We are not building for Linux/macOS and syscall backend has been selected
                    #[cfg(not(target_family = "unix"))]
                    {
                        Arc::new(limbo_core::PlatformIO::new()?)
                    }
                }
                // We are building for Linux and io_uring backend has been selected
                #[cfg(all(target_os = "linux", feature = "io_uring"))]
                Io::IoUring => Arc::new(limbo_core::UringIO::new()?),
            }
        }
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
.import --csv FILE TABLE   Import csv data from FILE into TABLE
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

10. To import csv file 'sample.csv' into 'csv_table' table:
   .import --csv sample.csv csv_table

Note:
- All SQL commands must end with a semicolon (;).
- Special commands do not require a semicolon."#;
