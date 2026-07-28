use crate::{
    commands::{
        args::{DbConfigMode, EchoMode, HeadersMode, ParameterArgs, ParameterCommand, TimerMode},
        import::ImportFile,
        Command, CommandParser,
    },
    config::Config,
    dot_command::tokenize_dot_command,
    helper::LimboHelper,
    input::{
        get_io, get_writer, open_flags, ApplyWriter, DbLocation, NoopProgress, OutputMode,
        ProgressSink, Settings, StderrProgress,
    },
    manual,
    opcodes_dictionary::OPCODE_DESCRIPTIONS,
    read_state_machine::ReadState,
    HISTORY_FILE,
};
use anyhow::{anyhow, Context};
use clap::Parser;
use comfy_table::{Attribute, Cell, CellAlignment, ContentArrangement, Row, Table};
use rustyline::{error::ReadlineError, history::DefaultHistory, Editor};
use std::num::NonZeroUsize;
use std::{
    fs::File,
    io::{self, BufRead, BufReader, IsTerminal, Write},
    mem::{forget, ManuallyDrop},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use turso_core::{
    io_error, Connection, Database, EqpFormat, LimboError, Numeric, OpenFlags, QueryMode,
    SqliteDialect, Statement, Value,
};

#[derive(Parser, Debug)]
#[command(name = "Turso")]
#[command(author, version, about, long_about = None)]
pub struct Opts {
    #[clap(index = 1, help = "SQLite database file", default_value = ":memory:")]
    pub database: Option<PathBuf>,
    #[clap(index = 2, help = "Optional SQL command to execute")]
    pub sql: Option<String>,
    #[clap(short = 'm', long, default_value_t = OutputMode::Pretty)]
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
        short = 'v',
        long,
        help = "Select VFS. options are io_uring (if feature enabled), experimental_win_iocp (if feature enabled on windows), memory, and syscall"
    )]
    pub vfs: Option<String>,
    #[clap(long, help = "Open the database in read-only mode")]
    pub readonly: bool,
    #[clap(long, help = "Enable experimental views feature")]
    pub experimental_views: bool,
    #[clap(
        long,
        help = "Enable experimental custom types (CREATE TYPE / DROP TYPE)"
    )]
    pub experimental_custom_types: bool,
    #[clap(short = 't', long, help = "specify output file for log traces")]
    pub tracing_output: Option<String>,
    #[clap(long, help = "Start MCP server instead of interactive shell")]
    pub mcp: bool,
    #[clap(
        long,
        help = "Start sync server instead of interactive shell and listen at given address (e.g. 0.0.0.0:8080)"
    )]
    pub sync_server: Option<String>,
    #[clap(
        long,
        requires = "sync_server",
        conflicts_with = "database",
        help = "Serve every database under PATH over the sync server, addressed as /db/{name}"
    )]
    pub sync_dir: Option<PathBuf>,
    #[clap(
        long,
        requires = "sync_dir",
        default_value_t = 256,
        help = "Maximum databases kept open at once under --sync-dir"
    )]
    pub sync_max_databases: usize,
    #[clap(long, help = "Enable experimental encryption feature")]
    pub experimental_encryption: bool,
    #[clap(long, help = "Enable experimental index method feature")]
    pub experimental_index_method: bool,
    #[clap(long, help = "Enable experimental autovacuum feature")]
    pub experimental_autovacuum: bool,
    #[clap(long, help = "Enable experimental vacuum feature")]
    pub experimental_vacuum: bool,
    #[clap(long, help = "Enable experimental attach feature")]
    pub experimental_attach: bool,
    #[clap(long, help = "Enable experimental generated columns feature")]
    pub experimental_generated_columns: bool,
    #[clap(long, help = "Enable experimental WITHOUT ROWID tables feature")]
    pub experimental_without_rowid: bool,
    #[clap(
        long,
        help = "Enable experimental multiprocess WAL coordination (on Windows, use --vfs experimental_win_iocp)"
    )]
    pub experimental_multiprocess_wal: bool,
    #[clap(
        long,
        help = "Enable experimental passive MVCC checkpointing (requires journal_mode=mvcc)"
    )]
    pub experimental_mvcc_passive_checkpoint: bool,
    #[cfg(feature = "mvcc_repl")]
    #[clap(long, help = "Start MVCC concurrent transaction harness")]
    pub mvcc: bool,
    #[clap(
        long,
        help = "Enable unsafe testing features (e.g. sqlite_dbpage writes)"
    )]
    pub unsafe_testing: bool,
}

const PROMPT: &str = "turso> ";

pub struct Limbo {
    pub prompt: String,
    io: Arc<dyn turso_core::IO>,
    writer: Option<Box<dyn Write>>,
    conn: Arc<turso_core::Connection>,
    pub interrupt_count: Arc<AtomicUsize>,
    input_buff: ManuallyDrop<String>,
    pub(crate) opts: Settings,
    pub(crate) db_opts: turso_core::DatabaseOpts,
    read_state: ReadState,
    pub rl: Option<Editor<LimboHelper, DefaultHistory>>,
    config: Option<Config>,
    had_query_error: bool,
    parameter_bindings: Vec<ParameterBinding>,
}

#[derive(Clone)]
struct ParameterBinding {
    name: Box<str>,
    index: Option<NonZeroUsize>,
    value: Value,
}

struct QueryStatistics {
    io_time_elapsed_samples: Vec<Duration>,
    execute_time_elapsed_samples: Vec<Duration>,
}

/// A lending iterator over query result rows with optional statistics tracking.
struct RowStepper<'a> {
    rows: &'a mut Statement,
    stats: Option<std::cell::RefCell<&'a mut QueryStatistics>>,
}

impl<'a> RowStepper<'a> {
    fn new(rows: &'a mut Statement, stats: Option<&'a mut QueryStatistics>) -> Self {
        Self {
            rows,
            stats: stats.map(std::cell::RefCell::new),
        }
    }

    /// Advances to the next row, returning it if available.
    /// Returns Ok(Some(row)) for each row, Ok(None) when done, or Err on failure.
    fn next_row(&mut self) -> Result<Option<&turso_core::Row>, LimboError> {
        let execution_time = std::cell::Cell::new(Instant::now());
        let io_time = std::cell::Cell::new(Instant::now());

        let result = self.rows.run_one_step_blocking(
            || {
                // Push execution sample to not count IO time in execution
                if let Some(stats) = self.stats.as_ref() {
                    stats
                        .borrow_mut()
                        .execute_time_elapsed_samples
                        .push(execution_time.get().elapsed());
                }
                // Start io timer
                io_time.set(Instant::now());
                Ok(())
            },
            || {
                // Push sample when we end IO
                if let Some(stats) = self.stats.as_ref() {
                    stats
                        .borrow_mut()
                        .io_time_elapsed_samples
                        .push(io_time.get().elapsed());
                }
                // Restart Execution timer
                execution_time.set(Instant::now());
                Ok(())
            },
        );

        match result {
            Ok(row_opt) => {
                if let Some(stats) = self.stats.as_ref() {
                    stats
                        .borrow_mut()
                        .execute_time_elapsed_samples
                        .push(execution_time.get().elapsed());
                }
                Ok(row_opt)
            }
            Err(e) => {
                if let Some(stats) = self.stats.as_ref() {
                    stats
                        .borrow_mut()
                        .execute_time_elapsed_samples
                        .push(execution_time.get().elapsed());
                }
                Err(e)
            }
        }
    }
}

/// metadata from db, fetched from pragmas
struct DbMetadata {
    page_size: i64,
    page_count: i64,
    filename: String,
}

struct DbPage<'a> {
    pgno: i64,
    data: &'a [u8],
}

impl Limbo {
    pub fn new() -> anyhow::Result<(Self, WorkerGuard)> {
        let mut opts = Opts::parse();
        let guard = Self::init_tracing(&opts)?;

        let db_file = opts
            .database
            .as_ref()
            .map_or(":memory:".to_string(), |p| p.to_string_lossy().to_string());

        let db_opts = turso_core::DatabaseOpts::new()
            .with_views(opts.experimental_views)
            .with_custom_types(opts.experimental_custom_types)
            .with_encryption(opts.experimental_encryption)
            .with_index_method(opts.experimental_index_method)
            .with_autovacuum(opts.experimental_autovacuum)
            .with_vacuum(opts.experimental_vacuum)
            .with_attach(opts.experimental_attach)
            .with_generated_columns(opts.experimental_generated_columns)
            .with_without_rowid(opts.experimental_without_rowid)
            .with_multiprocess_wal(opts.experimental_multiprocess_wal)
            .with_experimental_mvcc_passive_checkpoint(opts.experimental_mvcc_passive_checkpoint)
            .with_unsafe_testing(opts.unsafe_testing);

        let db_file = normalize_db_path(db_file);

        let (io, conn) = if db_file.starts_with("file:") {
            Connection::from_uri(&db_file, db_opts, Arc::new(SqliteDialect))?
        } else {
            let flags = open_flags(opts.readonly);
            let (io, db) = Database::open_new(
                &db_file,
                opts.vfs.as_ref(),
                flags,
                db_opts.turso_cli(),
                None,
                Arc::new(SqliteDialect),
            )?;
            let conn = db.connect()?;
            (io, conn)
        };
        unsafe {
            let mut ext_api = conn._build_turso_ext();
            if !limbo_completion::register_extension_static(&mut ext_api).is_ok() {
                return Err(anyhow!(
                    "Failed to register completion extension".to_string()
                ));
            }
            conn._free_extension_ctx(ext_api);
        }
        let interrupt_count = Arc::new(AtomicUsize::new(0));
        {
            let interrupt_count: Arc<AtomicUsize> = Arc::clone(&interrupt_count);
            ctrlc::set_handler(move || {
                // Increment the interrupt count on Ctrl-C
                interrupt_count.fetch_add(1, Ordering::Release);
            })
            .expect("Error setting Ctrl-C handler");
        }
        let sql = opts.sql.take();
        let has_sql = sql.is_some();
        let quiet = opts.quiet || !IsTerminal::is_terminal(&std::io::stdin());
        let config = Config::for_output_mode(opts.output_mode);
        let mut app = Self {
            prompt: PROMPT.to_string(),
            io,
            writer: Some(get_writer(&opts.output)),
            conn,
            interrupt_count,
            input_buff: ManuallyDrop::new(sql.unwrap_or_default()),
            read_state: ReadState::default(),
            opts: Settings::from(opts),
            db_opts,
            rl: None,
            config: Some(config),
            had_query_error: false,
            parameter_bindings: Vec::new(),
        };
        app.first_run(has_sql, quiet)?;
        Ok((app, guard))
    }

    pub fn with_config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    pub fn with_readline(mut self, mut rl: Editor<LimboHelper, DefaultHistory>) -> Self {
        let h = LimboHelper::new(
            self.conn.clone(),
            self.config.as_ref().map(|c| c.highlight.clone()),
        );
        rl.set_helper(Some(h));
        self.rl = Some(rl);
        self
    }

    fn first_run(&mut self, has_sql: bool, quiet: bool) -> Result<(), LimboError> {
        // Skip startup messages and SQL execution in MCP/SyncServer mode
        if self.is_mcp_mode() || self.is_sync_server_mode() {
            return Ok(());
        }

        if has_sql {
            self.handle_first_input()?;
        }
        if !quiet {
            self.writeln_fmt(format_args!("Turso v{}", env!("CARGO_PKG_VERSION")))
                .map_err(|e| io_error(e, "write"))?;
            self.writeln("Enter \".help\" for usage hints.")
                .map_err(|e| io_error(e, "write"))?;

            // Add random feature hint
            if let Some(hint) = manual::get_random_feature_hint() {
                self.writeln(&hint).map_err(|e| io_error(e, "write"))?;
            }

            self.display_in_memory().map_err(|e| io_error(e, "write"))?;
        }
        Ok(())
    }

    fn handle_first_input(&mut self) -> Result<(), LimboError> {
        self.consume(true);
        self.close_conn()?;
        std::process::exit(i32::from(self.had_query_error));
    }

    fn set_multiline_prompt(&mut self) {
        self.prompt = match self.input_buff.chars().fold(0, |acc, c| match c {
            '(' => acc + 1,
            ')' => acc - 1,
            _ => acc,
        }) {
            n if n < 0 => String::from(")x!...>"),
            0 => String::from("   ...> "),
            n if n < 10 => format!("(x{n}...> "),
            _ => String::from("(.....> "),
        };
    }

    #[cfg(not(target_family = "wasm"))]
    fn handle_load_extension(&mut self, path: &str) -> Result<(), String> {
        let ext_path = turso_core::resolve_ext_path(path).map_err(|e| e.to_string())?;
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

    fn display_stats(&mut self, args: crate::commands::args::StatsArgs) -> io::Result<()> {
        use crate::commands::args::StatsToggle;

        // Handle on/off toggle
        if let Some(toggle) = args.toggle {
            match toggle {
                StatsToggle::On => {
                    self.opts.stats = true;
                    self.writeln("Stats display enabled.")?;
                }
                StatsToggle::Off => {
                    self.opts.stats = false;
                    self.writeln("Stats display disabled.")?;
                }
            }
            return Ok(());
        }

        // Display all metrics
        let output = {
            let metrics = self.conn.metrics.read();
            format!("{metrics}")
        };

        self.writeln(output)?;

        if args.reset {
            self.conn.metrics.write().reset();
            self.writeln("Statistics reset.")?;
        }

        Ok(())
    }

    pub fn reset_input(&mut self) {
        self.prompt = PROMPT.to_string();
        self.input_buff.clear();
        self.read_state = ReadState::default();
    }

    pub fn close_conn(&mut self) -> Result<(), LimboError> {
        self.conn.close()
    }

    pub fn get_connection(&self) -> Arc<turso_core::Connection> {
        self.conn.clone()
    }

    pub fn is_mcp_mode(&self) -> bool {
        self.opts.mcp
    }

    pub fn is_sync_server_mode(&self) -> bool {
        self.opts.sync_server_address.is_some()
    }

    pub fn get_interrupt_count(&self) -> Arc<AtomicUsize> {
        self.interrupt_count.clone()
    }

    pub fn has_query_error(&self) -> bool {
        self.had_query_error
    }

    fn toggle_echo(&mut self, arg: EchoMode) {
        match arg {
            EchoMode::On => self.opts.echo = true,
            EchoMode::Off => self.opts.echo = false,
        }
    }

    fn open_db(&mut self, path: &str, vfs_name: Option<&str>) -> anyhow::Result<()> {
        self.conn.close()?;
        let (io, db) = if let Some(vfs_name) = vfs_name {
            self.conn
                .open_new(path, vfs_name, Arc::new(SqliteDialect))?
        } else {
            let io = {
                match path {
                    ":memory:" => get_io(DbLocation::Memory, &self.opts.io.to_string())?,
                    _path => get_io(DbLocation::Path, &self.opts.io.to_string())?,
                }
            };
            (
                io.clone(),
                Database::open_file_with_flags(
                    io.clone(),
                    path,
                    OpenFlags::default(),
                    self.db_opts,
                    None,
                    Arc::new(SqliteDialect),
                )?,
            )
        };
        self.io = io;
        self.conn = db.connect()?;
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
                self.writer = Some(Box::new(file));
                self.opts.is_stdout = false;
                self.opts.output_mode = OutputMode::List;
                self.opts.output_filename = path.to_string();
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        }
    }

    fn set_output_stdout(&mut self) {
        let _ = self.writer.as_mut().unwrap().flush();
        self.writer = Some(Box::new(io::stdout()));
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
        self.writer.as_mut().unwrap().write_fmt(fmt)
    }

    fn writeln_fmt(&mut self, fmt: std::fmt::Arguments) -> io::Result<()> {
        self.writer.as_mut().unwrap().write_fmt(fmt)?;
        self.writer.as_mut().unwrap().write_all(b"\n")
    }

    fn write<D: AsRef<[u8]>>(&mut self, data: D) -> io::Result<()> {
        self.writer.as_mut().unwrap().write_all(data.as_ref())
    }

    fn writeln<D: AsRef<[u8]>>(&mut self, data: D) -> io::Result<()> {
        self.writer.as_mut().unwrap().write_all(data.as_ref())?;
        self.writer.as_mut().unwrap().write_all(b"\n")
    }

    fn run_query(&mut self, input: &str) {
        let echo = self.opts.echo;
        if echo {
            let _ = self.writeln(input);
        }

        let start = Instant::now();
        let mut stats = if self.opts.timer {
            Some(QueryStatistics {
                io_time_elapsed_samples: vec![],
                execute_time_elapsed_samples: vec![],
            })
        } else {
            None
        };

        let conn = self.conn.clone();
        let runner = conn.query_runner(input.as_bytes());
        let had_error_before = self.had_query_error;
        let capture_stats = self.opts.stats;
        let mut last_stmt_metrics = None;
        for mut output in runner {
            if let Ok(Some(ref mut stmt)) = output {
                if let Err(err) = self.apply_parameter_bindings(stmt) {
                    output = Err(err);
                }
            }
            if self
                .print_query_result(input, &mut output, stats.as_mut())
                .is_err()
                || self.had_query_error != had_error_before
            {
                self.had_query_error = true;
                break;
            }
            // Capture metrics after stepping, before the Statement is dropped
            if capture_stats {
                if let Ok(Some(ref stmt)) = output {
                    last_stmt_metrics = Some(stmt.metrics());
                }
            }
        }

        self.print_query_performance_stats(start, stats.as_ref());

        // Display stats if enabled
        if let Some(ref last) = last_stmt_metrics {
            let _ = self.writeln(format!("\n{last}"));
        }
    }

    fn apply_parameter_bindings(&self, stmt: &mut Statement) -> Result<(), LimboError> {
        for binding in &self.parameter_bindings {
            if let Some(index) = binding.index {
                if stmt.parameters().has_slot(index) {
                    stmt.bind_at(index, binding.value.clone())?;
                }
                continue;
            }

            if let Some(index) = stmt.parameter_index(&binding.name) {
                stmt.bind_at(index, binding.value.clone())?;
            }
        }
        Ok(())
    }

    fn handle_parameter_command(&mut self, args: ParameterArgs) -> Result<(), String> {
        match args.command {
            ParameterCommand::Set(args) => {
                validate_parameter_name(&args.name)?;
                let index = parameter_name_to_index(&args.name);
                let value = parse_parameter_value(&args.value)?;

                if let Some(existing) = self
                    .parameter_bindings
                    .iter_mut()
                    .find(|binding| binding.name.as_ref() == args.name)
                {
                    existing.index = index;
                    existing.value = value;
                } else {
                    self.parameter_bindings.push(ParameterBinding {
                        name: args.name.into_boxed_str(),
                        index,
                        value,
                    });
                }
                Ok(())
            }
            ParameterCommand::List => self.list_parameter_bindings(),
            ParameterCommand::Clear(args) => {
                if let Some(name) = args.name {
                    validate_parameter_name(&name)?;
                    self.parameter_bindings
                        .retain(|binding| binding.name.as_ref() != name);
                } else {
                    self.parameter_bindings.clear();
                }
                Ok(())
            }
        }
    }

    fn list_parameter_bindings(&mut self) -> Result<(), String> {
        if self.parameter_bindings.is_empty() {
            return Ok(());
        }

        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| "writer is not initialized".to_string())?;

        for binding in &self.parameter_bindings {
            writer
                .write_fmt(format_args!("{} = {}\n", binding.name, binding.value))
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn print_query_performance_stats(&mut self, start: Instant, stats: Option<&QueryStatistics>) {
        let elapsed_as_str = |duration: Duration| {
            if duration.as_secs() >= 1 {
                format!("{} s", duration.as_secs_f64())
            } else if duration.as_millis() >= 1 {
                format!("{} ms", duration.as_millis() as f64)
            } else if duration.as_micros() >= 1 {
                format!("{} us", duration.as_micros() as f64)
            } else {
                format!("{} ns", duration.as_nanos())
            }
        };
        let sample_stats_as_str = |name: &str, samples: &Vec<Duration>| {
            if samples.is_empty() {
                return format!("{name}: No samples available");
            }
            let avg_time_spent = samples.iter().sum::<Duration>() / samples.len() as u32;
            let total_time = samples.iter().fold(Duration::ZERO, |acc, x| acc + *x);
            format!(
                "{}: avg={}, total={}",
                name,
                elapsed_as_str(avg_time_spent),
                elapsed_as_str(total_time),
            )
        };
        if self.opts.timer {
            if let Some(stats) = stats {
                let _ = self.writeln("Command stats:\n----------------------------");
                let _ = self.writeln(format!(
                    "total: {} (this includes parsing/coloring of cli app)\n",
                    elapsed_as_str(start.elapsed())
                ));

                let _ = self.writeln("query execution stats:\n----------------------------");
                let _ = self.writeln(sample_stats_as_str(
                    "Execution",
                    &stats.execute_time_elapsed_samples,
                ));
                let _ = self.writeln(sample_stats_as_str("I/O", &stats.io_time_elapsed_samples));
            }
        }
    }

    fn reset_line(&mut self) {
        // Entry is auto added to history
        // self.rl.add_history_entry(line.to_owned())?;
        self.interrupt_count.store(0, Ordering::Release);
    }

    // consume will consume `input_buff`
    pub fn consume(&mut self, flush: bool) {
        if self.input_buff.trim().is_empty() {
            return;
        }

        self.reset_line();

        // we are taking ownership of input_buff here
        // its always safe because we split the string in two parts
        fn take_usable_part(app: &mut Limbo) -> (String, usize) {
            let ptr = app.input_buff.as_mut_ptr();
            let (len, cap) = (app.input_buff.len(), app.input_buff.capacity());
            app.input_buff =
                ManuallyDrop::new(unsafe { String::from_raw_parts(ptr.add(len), 0, cap - len) });
            (unsafe { String::from_raw_parts(ptr, len, len) }, unsafe {
                ptr.add(len).addr()
            })
        }

        fn concat_usable_part(app: &mut Limbo, mut part: String, old_address: usize) {
            let ptr = app.input_buff.as_mut_ptr();
            let (len, cap) = (app.input_buff.len(), app.input_buff.capacity());

            // if the address is not the same, meaning the string has been reallocated
            // so we just drop the part we took earlier
            if ptr.addr() != old_address || !app.input_buff.is_empty() {
                return;
            }

            let head_ptr = part.as_mut_ptr();
            let (head_len, head_cap) = (part.len(), part.capacity());
            forget(part); // move this part into `input_buff`
            app.input_buff = ManuallyDrop::new(unsafe {
                String::from_raw_parts(head_ptr, head_len + len, head_cap + cap)
            });
        }

        let value = self.input_buff.trim();
        let is_dot_command = value.starts_with('.');
        let is_complete = self.read_state.is_complete();

        match (is_dot_command, is_complete) {
            (true, _) => {
                let (owned_value, old_address) = take_usable_part(self);
                self.handle_dot_command(owned_value.trim().strip_prefix('.').unwrap());
                concat_usable_part(self, owned_value, old_address);
                self.reset_input();
            }
            (false, true) => {
                let (owned_value, old_address) = take_usable_part(self);
                self.run_query(owned_value.trim());
                concat_usable_part(self, owned_value, old_address);
                self.reset_input();
            }
            (false, false) if flush => {
                let (owned_value, old_address) = take_usable_part(self);
                self.run_query(owned_value.trim());
                concat_usable_part(self, owned_value, old_address);
                self.reset_input();
            }
            (false, false) => {
                self.set_multiline_prompt();
            }
        }
    }

    pub fn handle_dot_command(&mut self, line: &str) {
        let (args, unterminated_quote) = tokenize_dot_command(line);
        if let Some(quote) = unterminated_quote {
            let _ = self.writeln_fmt(format_args!("unterminated {quote} quote"));
            return;
        }
        if args.is_empty() {
            return;
        }
        let parse = CommandParser::try_parse_from(args);
        match parse {
            Err(err) => {
                // Let clap print with Styled Colors instead
                let _ = err.print();
            }
            Ok(cmd) => match cmd.command {
                Command::Exit(args) => {
                    self.save_history();
                    std::process::exit(args.code);
                }
                Command::Quit => {
                    let _ = self.writeln("Exiting Turso SQL Shell.");
                    let _ = self.close_conn();
                    self.save_history();
                    std::process::exit(0)
                }
                Command::Open(args) => {
                    if let Err(e) = self.open_db(&args.path, args.vfs_name.as_deref()) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Schema(args) => {
                    if let Err(e) = self.display_schema(args.table_name.as_deref()) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Tables(args) => {
                    if let Err(e) = self.display_tables(args.pattern.as_deref()) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Databases => {
                    if let Err(e) = self.display_databases() {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Opcodes(args) => {
                    if let Some(opcode) = args.opcode {
                        for op in &OPCODE_DESCRIPTIONS {
                            if op.name.eq_ignore_ascii_case(opcode.trim()) {
                                let _ = self.writeln_fmt(format_args!("{op}"));
                            }
                        }
                    } else {
                        for op in &OPCODE_DESCRIPTIONS {
                            let _ = self.writeln_fmt(format_args!("{op}\n"));
                        }
                    }
                }
                Command::NullValue(args) => {
                    self.opts.null_value = args.value;
                }
                Command::OutputMode(args) => {
                    if let Err(e) = self.set_mode(args.mode) {
                        let _ = self.writeln_fmt(format_args!("Error: {e}"));
                    }
                }
                Command::SetOutput(args) => {
                    if let Some(path) = args.path {
                        if let Err(e) = self.set_output_file(&path) {
                            let _ = self.writeln_fmt(format_args!("Error: {e}"));
                        }
                    } else {
                        self.set_output_stdout();
                    }
                }
                Command::Echo(args) => {
                    self.toggle_echo(args.mode);
                }
                Command::Cwd(args) => {
                    let _ = std::env::set_current_dir(args.directory);
                }
                Command::ShowInfo => {
                    let _ = self.show_info();
                }
                Command::Stats(args) => {
                    if let Err(e) = self.display_stats(args) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Import(args) => {
                    let w = self.writer.as_mut().unwrap();
                    let mut import_file = ImportFile::new(self.conn.clone(), w);
                    import_file.import(args)
                }
                Command::LoadExtension(args) => {
                    #[cfg(not(target_family = "wasm"))]
                    if let Err(e) = self.handle_load_extension(&args.path) {
                        let _ = self.writeln(&e);
                    }
                }
                Command::Dump => {
                    if let Err(e) = self.dump_database() {
                        let _ = self.writeln_fmt(format_args!("/****** ERROR: {e} ******/"));
                    }
                }
                Command::DbConfig(args) => match (args.config.as_deref(), args.mode) {
                    (Some("dqs_dml"), Some(DbConfigMode::On)) => {
                        self.conn.set_dqs_dml(true);
                    }
                    (Some("dqs_dml"), Some(DbConfigMode::Off)) => {
                        self.conn.set_dqs_dml(false);
                    }
                    (Some("dqs_dml"), None) => {
                        let val = if self.conn.get_dqs_dml() { "on" } else { "off" };
                        let _ = self.writeln(format!("dqs_dml {val}"));
                    }
                    (Some(name), _) => {
                        let _ = self.writeln(format!("unknown dbconfig: {name}"));
                    }
                    (None, _) => {
                        let dqs = if self.conn.get_dqs_dml() { "on" } else { "off" };
                        let _ = self.writeln(format!("dqs_dml {dqs}"));
                    }
                },
                Command::ListVfs => {
                    let _ = self.writeln("Available VFS modules:");
                    self.conn.list_vfs().iter().for_each(|v| {
                        let _ = self.writeln(v);
                    });
                }
                Command::ListIndexes(args) => {
                    if let Err(e) = self.display_indexes(args.tbl_name) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Timer(timer_mode) => {
                    self.opts.timer = match timer_mode.mode {
                        TimerMode::On => true,
                        TimerMode::Off => false,
                    };
                }
                Command::Headers(headers_mode) => {
                    self.opts.headers = match headers_mode.mode {
                        HeadersMode::On => true,
                        HeadersMode::Off => false,
                    };
                }
                Command::Clone(args) => {
                    if let Err(e) = self.clone_database(&args.output_file) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Manual(args) => {
                    let w = self.writer.as_mut().unwrap();
                    if let Err(e) = manual::display_manual(args.page.as_deref(), w) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Read(args) => {
                    if let Err(e) = self.read_sql_file(&args.path) {
                        let _ = self.writeln(e.to_string());
                    }
                }
                Command::Parameter(args) => {
                    if let Err(e) = self.handle_parameter_command(args) {
                        let _ = self.writeln_fmt(format_args!("Error: {e}"));
                    }
                }
                Command::Dbtotxt(args) => {
                    if let Err(e) = self.dump_database_as_text(args.page_no) {
                        let _ = self.writeln_fmt(format_args!("ERROR:{e}"));
                    }
                }
            },
        }
    }

    fn print_query_result(
        &mut self,
        sql: &str,
        output: &mut Result<Option<Statement>, LimboError>,
        statistics: Option<&mut QueryStatistics>,
    ) -> anyhow::Result<()> {
        match output {
            Ok(Some(ref mut rows)) => {
                let query_mode = rows.get_query_mode();
                let output_mode = self.opts.output_mode;

                match (output_mode, query_mode) {
                    (OutputMode::List, _) => {
                        self.print_list_mode(rows, statistics)?;
                    }
                    (
                        _,
                        QueryMode::ExplainQueryPlan {
                            format: EqpFormat::Text,
                        },
                    ) => {
                        self.print_explain_query_plan(rows, statistics)?;
                    }
                    (
                        _,
                        QueryMode::ExplainQueryPlan {
                            format: EqpFormat::Json,
                        },
                    ) => {
                        self.print_list_mode(rows, statistics)?;
                    }
                    (_, QueryMode::Explain) => {
                        self.print_explain(rows, statistics)?;
                    }
                    (OutputMode::Pretty, _) => {
                        self.print_pretty_mode(rows, statistics)?;
                    }
                    (OutputMode::Line, _) => {
                        self.print_line_mode(rows, statistics)?;
                    }
                }
            }
            Ok(None) => {}

            Err(ref err) => {
                match err {
                    LimboError::Busy => {}
                    LimboError::Interrupt => {}
                    _ => {
                        let report =
                            miette::Error::from(err.clone()).with_source_code(sql.to_owned());
                        let _ = self.writeln_fmt(format_args!("{report:?}"));
                    }
                }
                anyhow::bail!("We have to throw here, even if we printed error");
            }
        }
        Ok(())
    }

    fn print_explain_query_plan(
        &mut self,
        rows: &mut Statement,
        statistics: Option<&mut QueryStatistics>,
    ) -> turso_core::Result<()> {
        struct Entry {
            id: usize,
            detail: String,
            child_prefix: String,
            children: Vec<Entry>,
        }

        fn add_children(id: usize, parent_id: usize, detail: String, current: &mut Entry) -> bool {
            if current.id == parent_id {
                current.children.push(Entry {
                    id,
                    detail,
                    child_prefix: current.child_prefix.clone() + "   ",
                    children: vec![],
                });
                if current.children.len() > 1 {
                    let idx = current.children.len() - 2;
                    current.children[idx].child_prefix = current.child_prefix.clone() + "|  ";
                }
                return false;
            }
            for child in &mut current.children {
                if !add_children(id, parent_id, detail.clone(), child) {
                    return false;
                }
            }
            true
        }

        fn print_entry(app: &mut Limbo, entry: &Entry, prefix: &str) {
            writeln!(app, "{}{}", prefix, entry.detail).unwrap();
            for (i, child) in entry.children.iter().enumerate() {
                let is_last = i == entry.children.len() - 1;
                let child_prefix = format!(
                    "{}{}",
                    entry.child_prefix,
                    if is_last { "`--" } else { "|--" }
                );
                print_entry(app, child, child_prefix.as_str());
            }
        }

        let mut root = Entry {
            id: 0,
            detail: "QUERY PLAN".to_owned(),
            child_prefix: "".to_owned(),
            children: vec![],
        };

        let mut stepper = RowStepper::new(rows, statistics);
        loop {
            match stepper.next_row() {
                Ok(Some(row)) => {
                    let id = row.get_value(0).as_uint() as usize;
                    let parent_id = row.get_value(1).as_uint() as usize;
                    let detail = row.get_value(3).to_string();
                    add_children(id, parent_id, detail, &mut root);
                }
                Ok(None) => break,
                Err(e) => {
                    self.handle_step_error(e);
                    break;
                }
            }
        }

        print_entry(self, &root, "");
        Ok(())
    }

    fn print_explain(
        &mut self,
        rows: &mut Statement,
        statistics: Option<&mut QueryStatistics>,
    ) -> turso_core::Result<()> {
        fn get_explain_indent(
            indent_count: usize,
            curr_insn: &str,
            prev_insn: &str,
            p1: &str,
            unclosed_begin_subrtns: &mut Vec<String>,
        ) -> usize {
            let indent_count = match prev_insn {
                "Rewind" | "Last" | "SorterSort" | "SeekGE" | "SeekGT" | "SeekLE" | "SeekLT"
                | "BeginSubrtn" | "IndexMethodQuery" => indent_count + 1,
                _ => indent_count,
            };

            if curr_insn == "BeginSubrtn" {
                unclosed_begin_subrtns.push(p1.to_string());
            }

            match curr_insn {
                "Next" | "SorterNext" | "Prev" => indent_count.saturating_sub(1),
                "Return" => {
                    let matching = unclosed_begin_subrtns.iter().position(|b| b == p1);
                    if let Some(idx) = matching {
                        unclosed_begin_subrtns.remove(idx);
                        indent_count.saturating_sub(1)
                    } else {
                        indent_count
                    }
                }
                _ => indent_count,
            }
        }

        let _ =
            self.writeln("addr  opcode             p1    p2    p3    p4             p5  comment");
        let _ =
            self.writeln("----  -----------------  ----  ----  ----  -------------  --  -------");

        let mut prev_insn = String::new();
        let mut indent_count = 0;
        let indent = "  ";
        let mut unclosed_begin_subrtns = vec![];

        let mut stepper = RowStepper::new(rows, statistics);
        loop {
            match stepper.next_row() {
                Ok(Some(row)) => {
                    let insn = row.get_value(1).to_string();
                    let p1 = row.get_value(2).to_string();
                    indent_count = get_explain_indent(
                        indent_count,
                        &insn,
                        &prev_insn,
                        &p1,
                        &mut unclosed_begin_subrtns,
                    );
                    let _ = self.writeln(format!(
                        "{:<4}  {:<17}  {:<4}  {:<4}  {:<4}  {:<13}  {:<2}  {}",
                        row.get_value(0).to_string(),
                        &(indent.repeat(indent_count) + &insn),
                        p1,
                        row.get_value(3).to_string(),
                        row.get_value(4).to_string(),
                        row.get_value(5).to_string(),
                        row.get_value(6).to_string(),
                        row.get_value(7),
                    ));
                    prev_insn = insn;
                }
                Ok(None) => break,
                Err(e) => {
                    self.handle_step_error(e);
                    break;
                }
            }
        }
        Ok(())
    }

    fn print_list_mode(
        &mut self,
        rows: &mut Statement,
        statistics: Option<&mut QueryStatistics>,
    ) -> turso_core::Result<()> {
        let num_columns = rows.num_columns();
        let column_names: Vec<String> = (0..num_columns)
            .map(|i| rows.get_column_name(i).to_string())
            .collect();
        let print_headers = self.opts.headers;
        let null_value = self.opts.null_value.clone();

        let mut headers_printed = false;
        let mut stepper = RowStepper::new(rows, statistics);
        loop {
            match stepper.next_row() {
                Ok(Some(row)) => {
                    if print_headers && !headers_printed {
                        for (i, name) in column_names.iter().enumerate() {
                            if i > 0 {
                                let _ = self.write(b"|");
                            }
                            let _ = self.write(name.as_bytes());
                        }
                        let _ = self.writeln("");
                        headers_printed = true;
                    }

                    for (i, value) in row.get_values().enumerate() {
                        if i > 0 {
                            let _ = self.write(b"|");
                        }
                        match value {
                            Value::Null => {
                                let _ = self.write(null_value.as_bytes());
                            }
                            // Write blob bytes raw, like sqlite3 does in list
                            // mode. Going through Display would replace bytes
                            // that are not valid UTF-8 with U+FFFD.
                            Value::Blob(bytes) => {
                                self.write(bytes).map_err(|e| io_error(e, "write"))?;
                            }
                            _ => {
                                write!(self, "{value}").map_err(|e| io_error(e, "write"))?;
                            }
                        }
                    }
                    let _ = self.writeln("");
                }
                Ok(None) => break,
                Err(e) => {
                    self.handle_step_error(e);
                    break;
                }
            }
        }
        Ok(())
    }

    fn print_pretty_mode(
        &mut self,
        rows: &mut Statement,
        statistics: Option<&mut QueryStatistics>,
    ) -> turso_core::Result<()> {
        let config = self.config.as_ref().unwrap();
        let null_value = self.opts.null_value.clone();
        let num_columns = rows.num_columns();
        let column_names: Vec<String> = (0..num_columns)
            .map(|i| rows.get_column_name(i).to_string())
            .collect();
        let header_color = config.table.header_color.as_comfy_table_color();
        let column_colors: Vec<_> = config
            .table
            .column_colors
            .iter()
            .map(|c| c.as_comfy_table_color())
            .collect();

        let mut table = Table::new();
        table
            .set_content_arrangement(ContentArrangement::Dynamic)
            .set_truncation_indicator("…")
            .apply_modifier("││──├─┼┤│─┼├┤┬┴┌┐└┘");

        if num_columns > 0 {
            let header = column_names
                .iter()
                .map(|name| {
                    Cell::new(name)
                        .add_attribute(Attribute::Bold)
                        .fg(header_color)
                })
                .collect::<Vec<_>>();
            table.set_header(header);
        }

        let mut stepper = RowStepper::new(rows, statistics);
        loop {
            match stepper.next_row() {
                Ok(Some(row)) => {
                    let mut table_row = Row::new();
                    for (idx, value) in row.get_values().enumerate() {
                        let (content, alignment) = match value {
                            Value::Null => (null_value.clone(), CellAlignment::Left),
                            Value::Numeric(_) => (format!("{value}"), CellAlignment::Right),
                            Value::Text(_) => (format!("{value}"), CellAlignment::Left),
                            Value::Blob(_) => (format!("{value}"), CellAlignment::Left),
                        };
                        table_row.add_cell(
                            Cell::new(content)
                                .set_alignment(alignment)
                                .fg(column_colors[idx % column_colors.len()]),
                        );
                    }
                    table.add_row(table_row);
                }
                Ok(None) => break,
                Err(e) => {
                    self.handle_step_error(e);
                    break;
                }
            }
        }

        if !table.is_empty() {
            writeln!(self, "{table}").map_err(|e| io_error(e, "write"))?;
        }
        Ok(())
    }

    fn print_line_mode(
        &mut self,
        rows: &mut Statement,
        statistics: Option<&mut QueryStatistics>,
    ) -> turso_core::Result<()> {
        let num_columns = rows.num_columns();
        let column_names: Vec<String> = (0..num_columns)
            .map(|i| rows.get_column_name(i).to_string())
            .collect();
        let max_width = column_names.iter().map(|n| n.len()).max().unwrap_or(0);
        let formatted_columns: Vec<String> = column_names
            .iter()
            .map(|n| format!("{n:>max_width$}"))
            .collect();
        let null_value = self.opts.null_value.clone();

        let mut first_row_printed = false;
        let mut stepper = RowStepper::new(rows, statistics);
        loop {
            match stepper.next_row() {
                Ok(Some(row)) => {
                    if first_row_printed {
                        self.writeln("").map_err(|e| io_error(e, "write"))?;
                    } else {
                        first_row_printed = true;
                    }

                    for (i, value) in row.get_values().enumerate() {
                        self.write(&formatted_columns[i])
                            .map_err(|e| io_error(e, "write"))?;
                        self.write(b" = ").map_err(|e| io_error(e, "write"))?;
                        if matches!(value, Value::Null) {
                            self.write(null_value.as_bytes())
                                .map_err(|e| io_error(e, "write"))?;
                        } else {
                            write!(self, "{value}").map_err(|e| io_error(e, "write"))?;
                        }
                        self.writeln("").map_err(|e| io_error(e, "write"))?;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    self.handle_step_error(e);
                    break;
                }
            }
        }
        Ok(())
    }

    fn handle_step_error(&mut self, err: LimboError) {
        self.had_query_error = true;
        match err {
            LimboError::Interrupt => {
                let _ = self.writeln(LimboError::Interrupt.to_string());
            }
            LimboError::Busy => {
                let _ = self.writeln("database is busy");
            }
            _ => {
                // Mirror the sqlite3 shell: the bare sqlite3_errmsg text plus
                // the result code, e.g.
                // "Runtime error: UNIQUE constraint failed: t.a (19)".
                // The shell omits the code for plain SQLITE_ERROR (1).
                let code = err.sqlite_result_code();
                if code == 1 {
                    let _ = self.writeln_fmt(format_args!("Runtime error: {err}"));
                } else {
                    let _ = self.writeln_fmt(format_args!("Runtime error: {err} ({code})"));
                }
            }
        }
    }

    pub fn init_tracing(opts: &Opts) -> Result<WorkerGuard, std::io::Error> {
        let ((non_blocking, guard), should_emit_ansi) = if let Some(file) = &opts.tracing_output {
            (
                tracing_appender::non_blocking(
                    std::fs::File::options()
                        .append(true)
                        .create(true)
                        .open(file)?,
                ),
                false,
            )
        } else {
            (
                tracing_appender::non_blocking(std::io::stderr()),
                IsTerminal::is_terminal(&std::io::stderr()),
            )
        };
        let default_env_filter = EnvFilter::builder()
            .with_default_directive(tracing::level_filters::LevelFilter::WARN.into())
            .from_env_lossy();

        // Disable rustyline traces
        if let Err(e) = tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(non_blocking)
                    .with_line_number(true)
                    .with_thread_ids(true)
                    .with_ansi(should_emit_ansi),
            )
            .with(default_env_filter.add_directive("rustyline=off".parse().unwrap()))
            .try_init()
        {
            println!("Unable to setup tracing appender: {e:?}");
        }
        Ok(guard)
    }

    fn print_schema_entry(&mut self, db_display_name: &str, row: &turso_core::Row) -> bool {
        if let (Ok(Value::Text(schema)), Ok(Value::Text(obj_type)), Ok(Value::Text(obj_name))) = (
            row.get::<&Value>(0),
            row.get::<&Value>(1),
            row.get::<&Value>(2),
        ) {
            let modified_schema = if db_display_name == "main" {
                schema.as_str().to_string()
            } else {
                // We need to modify the SQL to include the database prefix in table names
                // This is a simple approach - for CREATE TABLE statements, insert db name after "TABLE "
                // For CREATE INDEX statements, insert db name after "ON "
                let schema_str = schema.as_str();
                if schema_str.to_uppercase().contains("CREATE TABLE ") {
                    // Find "CREATE TABLE " and insert database name after it
                    if let Some(pos) = schema_str.to_uppercase().find("CREATE TABLE ") {
                        let before = &schema_str[..pos + "CREATE TABLE ".len()];
                        let after = &schema_str[pos + "CREATE TABLE ".len()..];
                        format!("{before}{db_display_name}.{after}")
                    } else {
                        schema_str.to_string()
                    }
                } else if schema_str.to_uppercase().contains(" ON ") {
                    // For indexes, find " ON " and insert database name after it
                    if let Some(pos) = schema_str.to_uppercase().find(" ON ") {
                        let before = &schema_str[..pos + " ON ".len()];
                        let after = &schema_str[pos + " ON ".len()..];
                        format!("{before}{db_display_name}.{after}")
                    } else {
                        schema_str.to_string()
                    }
                } else {
                    schema_str.to_string()
                }
            };
            let _ = self.writeln_fmt(format_args!("{modified_schema};"));
            // For views, add the column comment like SQLite does
            if obj_type.as_str() == "view" {
                let columns = self
                    .get_view_columns(obj_name.as_str())
                    .unwrap_or_else(|_| "x".to_string());
                let _ = self.writeln_fmt(format_args!("/* {}({}) */", obj_name.as_str(), columns));
            }
            true
        } else {
            false
        }
    }

    /// Get column names for a view to generate the SQLite-compatible comment
    fn get_view_columns(&mut self, view_name: &str) -> anyhow::Result<String> {
        // Get column information using PRAGMA table_info
        let pragma_sql = format!("PRAGMA table_info({view_name})");

        let mut columns = Vec::new();
        let handler = |row: &turso_core::Row| {
            // Column name is in the second column (index 1) of PRAGMA table_info
            if let Ok(Value::Text(col_name)) = row.get::<&Value>(1) {
                columns.push(col_name.as_str().to_string());
            }
            Ok(())
        };
        if let Err(err) = self.handle_row(&pragma_sql, handler) {
            return Err(anyhow::anyhow!(
                "Error retrieving columns for view '{}': {}",
                view_name,
                err
            ));
        }
        if columns.is_empty() {
            anyhow::bail!("PRAGMA table_info returned no columns for view '{}'. The view may be corrupted or the database schema is invalid.", view_name);
        }
        Ok(columns.join(","))
    }

    fn query_one_table_schema(
        &mut self,
        db_prefix: &str,
        db_display_name: &str,
        table_name: &str,
    ) -> anyhow::Result<bool> {
        // Yeah, sqlite also has this hardcoded: https://github.com/sqlite/sqlite/blob/31efe5a0f2f80a263457a1fc6524783c0c45769b/src/shell.c.in#L10765
        match table_name {
            "sqlite_master" | "sqlite_schema" | "sqlite_temp_master" | "sqlite_temp_schema" => {
                let schema = format!(
                                    "CREATE TABLE {table_name} (\n type text,\n name text,\n tbl_name text,\n rootpage integer,\n sql text\n);",
                                );
                let _ = self.writeln(&schema);
                return Ok(true);
            }
            _ => {}
        }
        let sql = format!(
            "SELECT sql, type, name FROM {db_prefix}.sqlite_schema WHERE type IN ('table', 'index', 'view', 'trigger') AND (tbl_name = '{table_name}' OR name = '{table_name}') AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%' ORDER BY CASE type WHEN 'table' THEN 1 WHEN 'view' THEN 2 WHEN 'index' THEN 3 WHEN 'trigger' THEN 4 END, rowid"
        );

        let mut found = false;
        match self.conn.query(&sql) {
            Ok(Some(ref mut rows)) => {
                let res = rows.run_with_row_callback(|row| {
                    found |= self.print_schema_entry(db_display_name, row);
                    Ok(())
                });
                match res {
                    Ok(_) => {}
                    Err(LimboError::Interrupt) => {
                        let _ = self.writeln(LimboError::Interrupt.to_string());
                    }
                    Err(LimboError::Busy) => {
                        let _ = self.writeln("database is busy");
                    }
                    Err(err) => return Err(anyhow::anyhow!(err)),
                }
            }
            Ok(None) => {}
            Err(_) => {} // Table not found in this database
        }
        Ok(found)
    }

    fn query_all_tables_schema(
        &mut self,
        db_prefix: &str,
        db_display_name: &str,
    ) -> anyhow::Result<()> {
        let sql = format!("SELECT sql, type, name FROM {db_prefix}.sqlite_schema WHERE type IN ('table', 'index', 'view', 'trigger') AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__turso_internal_%' ORDER BY CASE type WHEN 'table' THEN 1 WHEN 'view' THEN 2 WHEN 'index' THEN 3 WHEN 'trigger' THEN 4 END, rowid");

        match self.conn.query(&sql) {
            Ok(Some(ref mut rows)) => {
                let res = rows.run_with_row_callback(|row| {
                    self.print_schema_entry(db_display_name, row);
                    Ok(())
                });
                match res {
                    Ok(_) => {}
                    Err(LimboError::Busy) => {
                        let _ = self.writeln("database is busy");
                    }
                    Err(LimboError::Interrupt) => {
                        let _ = self.writeln(LimboError::Interrupt.to_string());
                    }
                    Err(err) => return Err(anyhow!(err)),
                }
            }
            Ok(None) => {}
            Err(err) => {
                // If we can't access this database's schema, just skip it
                if !err.to_string().contains("no such table") {
                    eprintln!(
                        "Warning: Could not query schema for database '{db_display_name}': {err}"
                    );
                }
            }
        }
        Ok(())
    }

    fn display_schema(&mut self, table: Option<&str>) -> anyhow::Result<()> {
        match table {
            Some(table_spec) => {
                // Parse table name to handle database prefixes (e.g., "db.table")
                let clean_table_spec = table_spec.trim_end_matches(';');

                let (target_db, table_name) =
                    if let Some((db, tbl)) = clean_table_spec.split_once('.') {
                        (db, tbl)
                    } else {
                        ("main", clean_table_spec)
                    };

                // Query only the specific table in the specific database
                if target_db == "main" {
                    self.query_one_table_schema("main", "main", table_name)?;
                } else {
                    // Check if the database is attached
                    let attached_databases = self.conn.list_attached_databases();
                    if attached_databases.contains(&target_db.to_string()) {
                        self.query_one_table_schema(target_db, target_db, table_name)?;
                    }
                }
            }
            None => {
                // Show schema for all tables in all databases
                let attached_databases = self.conn.list_attached_databases();

                // Query main database first
                self.query_all_tables_schema("main", "main")?;

                // Query all attached databases
                for db_name in attached_databases {
                    self.query_all_tables_schema(&db_name, &db_name)?;
                }
            }
        }

        Ok(())
    }

    fn display_indexes(&mut self, maybe_table: Option<String>) -> anyhow::Result<()> {
        let mut indexes = String::new();

        for name in self.database_names()? {
            let prefix = (name != "main").then_some(&name);
            let sql = match maybe_table {
                Some(ref tbl_name) => format!(
                    "SELECT name FROM {name}.sqlite_schema WHERE type='index' AND tbl_name = '{tbl_name}' ORDER BY 1"
                ),
                None => format!("SELECT name FROM {name}.sqlite_schema WHERE type='index' ORDER BY 1"),
            };
            let handler = |row: &turso_core::Row| {
                if let Ok(Value::Text(idx)) = row.get::<&Value>(0) {
                    if let Some(prefix) = prefix {
                        indexes.push_str(prefix);
                        indexes.push('.');
                    }
                    indexes.push_str(idx.as_str());
                    indexes.push(' ');
                }
                Ok(())
            };
            if let Err(err) = self.handle_row(&sql, handler) {
                if err.to_string().contains("no such table: sqlite_schema") {
                    return Err(anyhow::anyhow!("Unable to access database schema. The database may be using an older SQLite version or may not be properly initialized."));
                } else {
                    return Err(anyhow::anyhow!("Error querying schema: {}", err));
                }
            }
        }
        if !indexes.is_empty() {
            let _ = self.writeln(indexes.trim_end().as_bytes());
        }
        Ok(())
    }

    fn display_tables(&mut self, pattern: Option<&str>) -> anyhow::Result<()> {
        let mut tables = String::new();

        for name in self.database_names()? {
            let prefix = (name != "main").then_some(&name);
            let sql = match pattern {
                Some(pattern) => format!(
                    "SELECT name FROM {name}.sqlite_schema WHERE type in ('table', 'view') AND name NOT LIKE 'sqlite_%' AND name LIKE '{pattern}' ORDER BY 1"
                ),
                None => format!(
                    "SELECT name FROM {name}.sqlite_schema WHERE type in ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY 1"
                ),
            };
            let handler = |row: &turso_core::Row| {
                if let Ok(Value::Text(table)) = row.get::<&Value>(0) {
                    if let Some(prefix) = prefix {
                        tables.push_str(prefix);
                        tables.push('.');
                    }
                    tables.push_str(table.as_str());
                    tables.push(' ');
                }
                Ok(())
            };
            if let Err(e) = self.handle_row(&sql, handler) {
                if e.to_string().contains("no such table: sqlite_schema") {
                    return Err(anyhow::anyhow!("Unable to access database schema. The database may be using an older SQLite version or may not be properly initialized."));
                } else {
                    return Err(anyhow::anyhow!("Error querying schema: {}", e));
                }
            }
        }
        if !tables.is_empty() {
            let _ = self.writeln(tables.trim_end().as_bytes());
        } else if let Some(pattern) = pattern {
            let _ = self.writeln_fmt(format_args!(
                "Error: Tables with pattern '{pattern}' not found."
            ));
        } else {
            let _ = self.writeln(b"No tables found in the database.");
        }
        Ok(())
    }

    fn database_names(&mut self) -> anyhow::Result<Vec<String>> {
        let sql = "PRAGMA database_list";
        let mut db_names: Vec<String> = Vec::new();
        let handler = |row: &turso_core::Row| {
            if let Ok(Value::Text(name)) = row.get::<&Value>(1) {
                db_names.push(name.to_string());
            }
            Ok(())
        };
        match self.handle_row(sql, handler) {
            Ok(_) => Ok(db_names),
            Err(e) => Err(anyhow::anyhow!("Error in database list: {}", e)),
        }
    }

    fn handle_row<F>(&mut self, sql: &str, handler: F) -> anyhow::Result<()>
    where
        F: FnMut(&turso_core::Row) -> turso_core::Result<()>,
    {
        match self.conn.query(sql) {
            Ok(Some(ref mut rows)) => {
                let res = rows.run_with_row_callback(handler);
                match res {
                    Ok(_) => {}
                    Err(LimboError::Busy) => {
                        let _ = self.writeln("database is busy");
                    }
                    Err(LimboError::Interrupt) => {
                        let _ = self.writeln(LimboError::Interrupt.to_string());
                    }
                    Err(err) => return Err(anyhow!(err)),
                }
            }
            Ok(None) => {
                let _ = self.writeln("No results returned from the query.");
            }
            Err(err) => {
                return Err(anyhow::anyhow!("Error querying database: {}", err));
            }
        }
        Ok(())
    }

    fn display_databases(&mut self) -> anyhow::Result<()> {
        let sql = "PRAGMA database_list";
        let conn = self.conn.clone();
        let mut databases = Vec::new();
        self.handle_row(sql, |row| {
            if let (
                Ok(Value::Numeric(Numeric::Integer(seq))),
                Ok(Value::Text(name)),
                Ok(file_value),
            ) = (
                row.get::<&Value>(0),
                row.get::<&Value>(1),
                row.get::<&Value>(2),
            ) {
                let file = match file_value {
                    Value::Text(path) => path.as_str(),
                    Value::Null => "",
                    _ => "",
                };

                // Format like SQLite: "main: /path/to/file r/w"
                let file_display = if file.is_empty() {
                    "\"\"".to_string()
                } else {
                    file.to_string()
                };

                // Detect readonly mode from connection
                let mode = if conn.is_readonly(*seq as usize) {
                    "r/o"
                } else {
                    "r/w"
                };

                databases.push(format!("{}: {} {}", name.as_str(), file_display, mode));
            }
            Ok(())
        })?;

        for db in databases {
            let _ = self.writeln(db);
        }

        Ok(())
    }

    // readline will read inputs from rustyline or stdin
    // and write it to input_buff.
    pub fn readline(&mut self) -> Result<(), ReadlineError> {
        use std::fmt::Write;

        if let Some(rl) = &mut self.rl {
            let result = rl.readline(&self.prompt)?;
            self.read_state.process(&result);
            let _ = self.input_buff.write_str(result.as_str());
        } else {
            let mut reader = std::io::stdin().lock();
            let prev_len = self.input_buff.len();
            if reader.read_line(&mut self.input_buff)? == 0 {
                return Err(ReadlineError::Eof);
            }
            self.read_state.process(&self.input_buff[prev_len..]);
        }

        if !self.input_buff.ends_with(char::is_whitespace) {
            let _ = self.input_buff.write_char('\n');
        }
        Ok(())
    }

    pub fn dump_database_from_conn<W: Write, P: ProgressSink>(
        fk: bool,
        conn: Arc<Connection>,
        out: &mut W,
        mut progress: P,
    ) -> anyhow::Result<()> {
        // Snapshot for consistency
        Self::exec_all_conn(&conn, "BEGIN")?;
        // FIXME: we don't yet support PRAGMA foreign_keys=OFF internally,
        // so for now this hacky boolean that decides not to emit it when cloning
        if fk {
            writeln!(out, "PRAGMA foreign_keys=OFF;")?;
        }
        writeln!(out, "BEGIN TRANSACTION;")?;
        // FIXME: At this point, SQLite executes the following:
        // sqlite3_exec(p->db, "SAVEPOINT dump; PRAGMA writable_schema=ON", 0, 0, 0);
        // we don't have those yet, so don't.
        // Emit CREATE TYPE statements from __turso_internal_types before table DDL,
        // so that tables referencing custom types can be restored correctly.
        Self::dump_custom_types(&conn, out)?;
        let q_tables = r#"
        SELECT name, sql
        FROM sqlite_schema
        WHERE type='table' AND sql NOT NULL
        ORDER BY tbl_name = 'sqlite_sequence', rowid
    "#;
        if let Some(mut rows) = conn.query(q_tables)? {
            rows.run_with_row_callback(|row| {
                let name: &str = row.get::<&str>(0)?;
                // Skip sqlite_sequence and every internal object. Index-method
                // backing tables (e.g. FTS's __turso_internal_fts_dir_*) are
                // rejected on replay because their names are reserved, and the
                // trailing CREATE INDEX ... USING ... rebuilds them anyway.
                if name == "sqlite_sequence"
                    || name.starts_with(turso_core::schema::TURSO_INTERNAL_PREFIX)
                {
                    return Ok(());
                }
                let ddl: &str = row.get::<&str>(1)?;
                writeln!(out, "{ddl};").map_err(|e| io_error(e, "write"))?;
                Self::dump_table_from_conn(&conn, out, name, &mut progress)?;
                progress.on(name);
                Ok(())
            })?;
        }
        Self::dump_sqlite_sequence(&conn, out)?;
        Self::dump_schema_objects(&conn, out, &mut progress)?;
        Self::exec_all_conn(&conn, "COMMIT")?;
        writeln!(out, "COMMIT;")?;
        Ok(())
    }

    fn exec_all_conn(conn: &Arc<Connection>, sql: &str) -> turso_core::Result<()> {
        if let Some(mut rows) = conn.query(sql)? {
            rows.run_with_row_callback(|_| Ok(()))?;
        }
        Ok(())
    }

    fn dump_table_from_conn<W: Write, P: ProgressSink>(
        conn: &Arc<Connection>,
        out: &mut W,
        table_name: &str,
        progress: &mut P,
    ) -> turso_core::Result<()> {
        let pragma = format!("PRAGMA table_info({})", quote_ident(table_name));
        let (mut cols, mut types) = (Vec::new(), Vec::new());

        if let Some(mut rows) = conn.query(pragma)? {
            rows.run_with_row_callback(|row| {
                let ty = row.get::<&str>(2)?.to_string();
                let name = row.get::<&str>(1)?.to_string();
                match ty.as_str() {
                    "index" => progress.on(&name),
                    "view" => progress.on(&name),
                    "trigger" => progress.on(&name),
                    _ => {}
                }
                cols.push(name);
                types.push(ty);
                Ok(())
            })?;
        }
        // FIXME: sqlite has logic to check rowid and optionally preserve it, but it requires
        // pragma index_list, and it seems to be relevant only for indexes.
        let cols_str = cols
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let select = format!("SELECT {cols_str} FROM {}", quote_ident(table_name));
        if let Some(mut rows) = conn.query(select)? {
            rows.run_with_row_callback(|row| {
                write!(out, "INSERT INTO {} VALUES(", quote_ident(table_name))
                    .map_err(|e| io_error(e, "write"))?;
                for i in 0..cols.len() {
                    if i > 0 {
                        out.write_all(b",").map_err(|e| io_error(e, "write"))?;
                    }
                    let v = row.get::<&Value>(i)?;
                    Self::write_sql_value_from_value(out, v).map_err(|e| io_error(e, "write"))?;
                }
                out.write_all(b");\n").map_err(|e| io_error(e, "write"))?;
                Ok(())
            })?;
        }
        Ok(())
    }

    fn dump_custom_types<W: Write>(conn: &Arc<Connection>, out: &mut W) -> anyhow::Result<()> {
        // Check if the internal types table exists before querying it.
        let check = format!(
            "SELECT 1 FROM sqlite_schema WHERE name='{}' AND type='table'",
            turso_core::schema::TURSO_TYPES_TABLE_NAME
        );
        let mut has_types = false;
        if let Some(mut rows) = conn.query(&check)? {
            rows.run_with_row_callback(|_| {
                has_types = true;
                Ok(())
            })?;
        }
        if !has_types {
            return Ok(());
        }
        let q = format!(
            "SELECT sql FROM {} ORDER BY rowid",
            turso_core::schema::TURSO_TYPES_TABLE_NAME
        );
        if let Some(mut rows) = conn.query(&q)? {
            rows.run_with_row_callback(|row| {
                let sql: &str = row.get::<&str>(0)?;
                writeln!(out, "{sql};").map_err(|e| io_error(e, "write"))?;
                Ok(())
            })?;
        }
        Ok(())
    }

    fn dump_sqlite_sequence<W: Write>(conn: &Arc<Connection>, out: &mut W) -> anyhow::Result<()> {
        let mut has_seq = false;
        if let Some(mut rows) =
            conn.query("SELECT 1 FROM sqlite_schema WHERE name='sqlite_sequence' AND type='table'")?
        {
            rows.run_with_row_callback(|_| {
                has_seq = true;
                Ok(())
            })?;
        }
        if !has_seq {
            return Ok(());
        }
        writeln!(out, "DELETE FROM sqlite_sequence;")?;
        if let Some(mut rows) = conn.query("SELECT name, seq FROM sqlite_sequence")? {
            rows.run_with_row_callback(|r| {
                let name = r.get::<&str>(0)?;
                let seq = r.get::<i64>(1)?;
                writeln!(
                    out,
                    "INSERT INTO sqlite_sequence(name,seq) VALUES({},{});",
                    sql_quote_string(name),
                    seq
                )
                .map_err(|e| io_error(e, "write"))?;
                Ok(())
            })?;
        }
        Ok(())
    }

    fn dump_schema_objects<W: Write, P: ProgressSink>(
        conn: &Arc<Connection>,
        out: &mut W,
        progress: &mut P,
    ) -> anyhow::Result<()> {
        // SQLite’s shell usually emits views after tables.
        // Emit only user objects: sql NOT NULL and name NOT LIKE 'sqlite_%'
        let sql = r#"
            SELECT name, sql FROM sqlite_schema
            WHERE sql NOT NULL
              AND name NOT LIKE 'sqlite_%'
              AND name NOT LIKE '\_\_turso\_internal\_%' ESCAPE '\'
              AND type IN ('index','trigger','view')
            ORDER BY CASE type WHEN 'view' THEN 1 WHEN 'index' THEN 2 WHEN 'trigger' THEN 3 END, rowid
        "#;
        if let Some(mut rows) = conn.query(sql)? {
            rows.run_with_row_callback(|row| {
                let ddl: &str = row.get::<&str>(1)?;
                let name: &str = row.get::<&str>(0)?;
                progress.on(name);
                writeln!(out, "{ddl};").map_err(|e| io_error(e, "write"))?;
                Ok(())
            })?;
        }
        Ok(())
    }

    fn write_sql_value_from_value<W: Write>(out: &mut W, v: &Value) -> io::Result<()> {
        match v {
            Value::Null => out.write_all(b"NULL"),
            Value::Numeric(Numeric::Integer(i)) => out.write_all(format!("{i}").as_bytes()),
            Value::Numeric(Numeric::Float(f)) => write!(out, "{}", f64::from(*f)).map(|_| ()),
            Value::Text(s) => {
                out.write_all(b"'")?;
                let bytes = s.value.as_bytes();
                let mut i = 0;
                while i < bytes.len() {
                    let b = bytes[i];
                    if b == b'\'' {
                        out.write_all(b"''")?;
                    } else {
                        out.write_all(&[b])?;
                    }
                    i += 1;
                }
                out.write_all(b"'")
            }
            Value::Blob(b) => {
                out.write_all(b"X'")?;
                const HEX: &[u8; 16] = b"0123456789abcdef";
                for &byte in b {
                    out.write_all(&[HEX[(byte >> 4) as usize], HEX[(byte & 0x0F) as usize]])?;
                }
                out.write_all(b"'")
            }
        }
    }

    fn dump_database(&mut self) -> anyhow::Result<()> {
        // Move writer out so we don’t hold a field-borrow of self during the call.
        let mut out = std::mem::take(&mut self.writer).unwrap();
        let conn = self.conn.clone();
        // dont print progress because it would interfere with piping output of .dump
        let res = Self::dump_database_from_conn(true, conn, &mut out, NoopProgress);
        // Put writer back
        self.writer = Some(out);
        res
    }

    fn clone_database(&mut self, output_file: &str) -> anyhow::Result<()> {
        use std::path::Path;
        if Path::new(output_file).exists() {
            anyhow::bail!("Refusing to overwrite existing file: {output_file}");
        }
        let io: Arc<dyn turso_core::IO> = Arc::new(turso_core::PlatformIO::new()?);
        let db = Database::open_file(io.clone(), output_file, Arc::new(SqliteDialect))?;
        let target = db.connect()?;

        let mut applier = ApplyWriter::new(&target);
        Self::dump_database_from_conn(false, self.conn.clone(), &mut applier, StderrProgress)?;
        applier.finish()?;
        Ok(())
    }

    fn read_sql_file(&mut self, path: &str) -> anyhow::Result<()> {
        let file =
            File::open(path).map_err(|e| anyhow!("Error: cannot open \"{}\" – {}", path, e))?;
        let reader = BufReader::new(file);

        let mut query_buffer = String::new();
        let mut state = ReadState::default();

        for line in reader.lines() {
            let line = line
                .map_err(|e| anyhow!("Error: file \"{}\" is not valid UTF-8 text – {}", path, e))?;

            if !query_buffer.is_empty() {
                query_buffer.push('\n');
            }
            query_buffer.push_str(&line);

            state.process(&line);

            if state.is_complete() {
                self.run_query(&query_buffer);
                query_buffer.clear();
                state = ReadState::default();
            }
        }

        let remaining = query_buffer.trim();
        if !remaining.is_empty() {
            self.run_query(remaining);
        }
        query_buffer.clear();
        Ok(())
    }

    fn save_history(&mut self) {
        if let Some(rl) = &mut self.rl {
            let _ = rl.save_history(HISTORY_FILE.as_path());
        }
    }

    fn fetch_db_metadata(&mut self) -> anyhow::Result<DbMetadata> {
        let page_size: i64 = if let Some(mut rows) = self.conn.query("PRAGMA page_size")? {
            fetch_single_i64(&mut rows).context("Failed to execute PRAGMA page_size")?
        } else {
            anyhow::bail!("Failed to prepare PRAGMA page_size");
        };

        let page_count: i64 = if let Some(mut rows) = self.conn.query("PRAGMA page_count")? {
            fetch_single_i64(&mut rows).context("Failed to execute PRAGMA page_count")?
        } else {
            anyhow::bail!("Failed to prepare PRAGMA page_count");
        };

        let filename = PathBuf::from(self.opts.db_file.clone())
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        Ok(DbMetadata {
            page_size,
            page_count,
            filename,
        })
    }

    fn write_page_hexdump(&mut self, page: &DbPage, page_size: i64) -> anyhow::Result<()> {
        let mut seen_page_label = false;

        for (i, chunk) in page.data.chunks(16).enumerate() {
            if chunk.iter().all(|&b| b == 0) {
                continue;
            }

            if !seen_page_label {
                writeln!(
                    self,
                    "| page {} offset {}",
                    page.pgno,
                    (page.pgno - 1) * page_size
                )?;
                seen_page_label = true;
            }

            // Line offset
            write!(self, "|  {:5}:", i * 16)?;

            // Hex bytes
            for byte in chunk {
                write!(self, " {byte:02x}")?;
            }
            for _ in 0..(16 - chunk.len()) {
                write!(self, "   ")?; // Pad partial lines
            }

            write!(self, "   ")?;

            // ASCII
            for &byte in chunk {
                let ch = match byte {
                    b' '..=b'~' if ![b'{', b'}', b'"', b'\\'].contains(&byte) => byte as char,
                    _ => '.',
                };
                write!(self, "{ch}")?;
            }
            writeln!(self)?;
        }
        Ok(())
    }

    fn dump_database_as_text(&mut self, page_no: Option<i64>) -> anyhow::Result<()> {
        let metadata = self.fetch_db_metadata()?;
        tracing::debug!(
            page_size = metadata.page_size,
            page_count = metadata.page_count,
            "Fetched metadata"
        );

        if let Some(pgno) = page_no {
            if pgno <= 0 {
                anyhow::bail!("Page number must be a positive integer.");
            }
            if pgno > metadata.page_count {
                anyhow::bail!(
                    "Page number {pgno} is out of bounds. The database only has {} pages.",
                    metadata.page_count
                );
            }
        }

        writeln!(
            self,
            "| size {} pagesize {} filename {}",
            metadata.page_count * metadata.page_size,
            metadata.page_size,
            &metadata.filename
        )?;

        let dump_sql = if let Some(pgno) = page_no {
            format!("SELECT pgno, data FROM sqlite_dbpage WHERE pgno = {pgno}")
        } else {
            "SELECT pgno, data FROM sqlite_dbpage ORDER BY pgno".to_string()
        };

        let mut pages: Vec<(i64, Vec<u8>)> = Vec::new();
        if let Some(mut rows) = self.conn.query(&dump_sql)? {
            rows.run_with_row_callback(|row| {
                let pgno: i64 = row.get(0)?;
                let value: &Value = row.get(1)?;
                let data: Vec<u8> = match value {
                    Value::Blob(bytes) => bytes.clone(),
                    _ => vec![],
                };
                pages.push((pgno, data));
                Ok(())
            })?;
        }

        for (pgno, data) in &pages {
            let page = DbPage { pgno: *pgno, data };
            self.write_page_hexdump(&page, metadata.page_size)?;
        }

        writeln!(self, "| end {}", &metadata.filename)?;

        Ok(())
    }
}

fn quote_ident(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

fn sql_quote_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push('\'');
        }
        out.push(ch);
    }
    out.push('\'');
    out
}

fn validate_parameter_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("parameter name cannot be empty".to_string());
    }

    match name.as_bytes()[0] {
        b':' | b'@' | b'$' | b'#' => Ok(()),
        b'?' => {
            let Some(rest) = name.strip_prefix('?') else {
                return Err("invalid parameter name".to_string());
            };
            if rest.is_empty() {
                return Err("parameter name '?N' must include digits".to_string());
            }
            if rest.parse::<usize>().ok().filter(|idx| *idx > 0).is_none() {
                return Err("parameter name '?N' must use an index >= 1".to_string());
            }
            Ok(())
        }
        _ => Err("parameter name must start with one of ':', '@', '$', '#', '?'".to_string()),
    }
}

fn parameter_name_to_index(name: &str) -> Option<NonZeroUsize> {
    let value = name.strip_prefix('?')?.parse::<usize>().ok()?;
    value.try_into().ok()
}

fn parse_parameter_value(value: &str) -> Result<Value, String> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("null") {
        return Ok(Value::Null);
    }

    if let Ok(integer) = value.parse::<i64>() {
        return Ok(Value::from_i64(integer));
    }

    if value.contains(['.', 'e', 'E']) {
        if let Ok(float) = value.parse::<f64>() {
            return Ok(Value::from_f64(float));
        }
    }

    if let Some(hex) = value
        .strip_prefix("x'")
        .or_else(|| value.strip_prefix("X'"))
        .and_then(|stripped| stripped.strip_suffix('\''))
    {
        return parse_hex_blob(hex).map(Value::from_blob);
    }

    if let Some(inner) = value
        .strip_prefix('\'')
        .and_then(|stripped| stripped.strip_suffix('\''))
    {
        return Ok(Value::build_text(unescape_single_quoted(inner)));
    }

    Ok(Value::build_text(value.to_owned()))
}

fn parse_hex_blob(hex: &str) -> Result<Vec<u8>, String> {
    if !hex.len().is_multiple_of(2) {
        return Err("hex blob literal must contain an even number of digits".to_string());
    }

    let mut out = Vec::with_capacity(hex.len() / 2);
    let mut bytes = hex.as_bytes().iter().copied();
    while let (Some(hi), Some(lo)) = (bytes.next(), bytes.next()) {
        let h = decode_hex_nibble(hi)?;
        let l = decode_hex_nibble(lo)?;
        out.push((h << 4) | l);
    }
    Ok(out)
}

fn decode_hex_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(10 + (byte - b'a')),
        b'A'..=b'F' => Ok(10 + (byte - b'A')),
        _ => Err("hex blob literal contains non-hex characters".to_string()),
    }
}

fn unescape_single_quoted(s: &str) -> String {
    if !s.contains("''") {
        return s.to_owned();
    }

    s.replace("''", "'")
}

impl Drop for Limbo {
    fn drop(&mut self) {
        self.save_history();
        unsafe {
            ManuallyDrop::drop(&mut self.input_buff);
        }
    }
}

fn fetch_single_i64(rows: &mut turso_core::Statement) -> anyhow::Result<i64> {
    let mut result: Option<i64> = None;
    rows.run_with_row_callback(|row| {
        result = Some(row.get(0)?);
        Ok(())
    })?;
    result.ok_or_else(|| anyhow!("query did not return a row"))
}

/// Normalize `path?key=val` to `file:path?key=val` so query parameters
/// are parsed as URI options (e.g. `?locking=shared_reads`) instead of
/// being treated as part of the filename.
///
/// Only the *last* `?` that introduces a valid `key=value` query string is
/// treated as the query separator. Earlier `?` characters are
/// percent-encoded (`%3F`) so they remain part of the filename.
/// A trailing `?` with no `key=value` pair is left alone (it is just part
/// of the filename).
fn normalize_db_path(db_file: String) -> String {
    if db_file.starts_with("file:") {
        return db_file;
    }

    // Walk from the right to find the last '?' whose suffix looks like
    // query parameters (contains at least one '=').
    if let Some(pos) = db_file.rfind('?') {
        let query = &db_file[pos + 1..];
        if query.contains('=') {
            let path = &db_file[..pos];
            // Percent-encode any '?' inside the path portion so the URI
            // parser does not mistake them for the query separator.
            let encoded_path = path.replace('?', "%3F");
            return format!("file:{encoded_path}?{query}");
        }
    }

    db_file
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_db_path_adds_file_prefix_for_query_params() {
        assert_eq!(
            normalize_db_path("test.db?locking=shared_reads".into()),
            "file:test.db?locking=shared_reads"
        );
    }

    #[test]
    fn test_normalize_db_path_preserves_existing_file_prefix() {
        assert_eq!(
            normalize_db_path("file:test.db?mode=ro".into()),
            "file:test.db?mode=ro"
        );
    }

    #[test]
    fn test_normalize_db_path_preserves_file_triple_slash() {
        assert_eq!(
            normalize_db_path("file:///tmp/test.db?mode=ro".into()),
            "file:///tmp/test.db?mode=ro"
        );
    }

    #[test]
    fn test_normalize_db_path_plain_path_unchanged() {
        assert_eq!(normalize_db_path("test.db".into()), "test.db");
    }

    #[test]
    fn test_normalize_db_path_memory_unchanged() {
        assert_eq!(normalize_db_path(":memory:".into()), ":memory:");
    }

    #[test]
    fn test_normalize_db_path_multiple_query_params() {
        assert_eq!(
            normalize_db_path("test.db?locking=shared_reads&cache=shared".into()),
            "file:test.db?locking=shared_reads&cache=shared"
        );
    }

    #[test]
    fn test_normalize_db_path_absolute_path_with_query() {
        assert_eq!(
            normalize_db_path("/tmp/my.db?mode=ro".into()),
            "file:/tmp/my.db?mode=ro"
        );
    }

    #[test]
    fn test_normalize_db_path_question_mark_in_filename_no_query() {
        // '?' is legitimately part of the filename, no key=value follows
        assert_eq!(normalize_db_path("what?.db".into()), "what?.db");
    }

    #[test]
    fn test_normalize_db_path_filename_contains_question_mark_with_query() {
        // File is literally "foo.bar?mode=ro", opened with ?mode=ro query.
        // The '?' in the filename must be percent-encoded so the URI parser
        // treats only the last ?mode=ro as the query string.
        assert_eq!(
            normalize_db_path("foo.bar?mode=ro?mode=ro".into()),
            "file:foo.bar%3Fmode=ro?mode=ro"
        );
    }
}
