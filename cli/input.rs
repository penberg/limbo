use crate::app::Opts;
use clap::ValueEnum;
use std::{
    fmt::{Display, Formatter},
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
};
use turso_core::{LimboError, OpenFlags};

#[derive(Copy, Clone)]
pub enum DbLocation {
    Memory,
    Path,
}

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Debug)]
pub enum Io {
    Syscall,
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    IoUring,
    #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
    WindowsIOCP,
    External(String),
    Memory,
}

impl Display for Io {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Io::Memory => write!(f, "memory"),
            Io::Syscall => write!(f, "syscall"),
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            Io::IoUring => write!(f, "io_uring"),
            #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
            Io::WindowsIOCP => write!(f, "experimental_win_iocp"),
            Io::External(str) => write!(f, "{str}"),
        }
    }
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
                    Io::Syscall // FIXME: make io_uring faster so it can be the default
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
    List,
    Pretty,
    Line,
}

impl std::fmt::Display for OutputMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.to_possible_value()
            .expect("no values are skipped")
            .get_name()
            .fmt(f)
    }
}

pub struct Settings {
    pub output_filename: String,
    pub db_file: String,
    pub null_value: String,
    pub output_mode: OutputMode,
    pub echo: bool,
    pub is_stdout: bool,
    pub io: Io,
    pub timer: bool,
    pub headers: bool,
    pub mcp: bool,
    pub sync_server_address: Option<String>,
    pub sync_dir: Option<PathBuf>,
    pub sync_max_databases: usize,
    pub vfs: Option<String>,
    pub flags: OpenFlags,
    pub stats: bool,
}

pub fn open_flags(readonly: bool) -> OpenFlags {
    let mut flags = OpenFlags::default();
    flags.set(OpenFlags::ReadOnly, readonly);
    flags
}

impl From<Opts> for Settings {
    fn from(opts: Opts) -> Self {
        Self {
            null_value: String::new(),
            output_mode: opts.output_mode,
            echo: false,
            is_stdout: opts.output.is_empty(),
            output_filename: opts.output,
            db_file: opts
                .database
                .as_ref()
                .map_or(":memory:".to_string(), |p| p.to_string_lossy().to_string()),
            io: match opts.vfs.as_ref().unwrap_or(&String::new()).as_str() {
                "memory" | ":memory:" => Io::Memory,
                "syscall" => Io::Syscall,
                #[cfg(all(target_os = "linux", feature = "io_uring"))]
                "io_uring" => Io::IoUring,
                #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
                "experimental_win_iocp" => Io::WindowsIOCP,
                "" => Io::default(),
                vfs => Io::External(vfs.to_string()),
            },
            timer: false,
            headers: false,
            mcp: opts.mcp,
            sync_server_address: opts.sync_server,
            sync_dir: opts.sync_dir,
            sync_max_databases: opts.sync_max_databases,
            flags: open_flags(opts.readonly),
            vfs: opts.vfs,
            stats: false,
        }
    }
}

impl std::fmt::Display for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Settings:\nOutput mode: {}\nDB: {}\nOutput: {}\nNull value: {}\nCWD: {}\nEcho: {}\nHeaders: {}",
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
            },
            match self.headers {
                true => "on",
                false => "off",
            }
        )
    }
}

pub fn get_writer(output: &str) -> Box<dyn Write> {
    match output {
        "" => Box::new(io::stdout()),
        _ => match std::fs::File::create(output) {
            Ok(file) => Box::new(file),
            Err(e) => {
                eprintln!("Error: {e}");
                Box::new(io::stdout())
            }
        },
    }
}

pub fn get_io(db_location: DbLocation, io_choice: &str) -> anyhow::Result<Arc<dyn turso_core::IO>> {
    Ok(match db_location {
        DbLocation::Memory => Arc::new(turso_core::MemoryIO::new()),
        DbLocation::Path => {
            match io_choice {
                "memory" => Arc::new(turso_core::MemoryIO::new()),
                "syscall" => {
                    // We are building for Linux/macOS and syscall backend has been selected
                    #[cfg(target_family = "unix")]
                    {
                        Arc::new(turso_core::UnixIO::new()?)
                    }
                    // We are not building for Linux/macOS and syscall backend has been selected
                    #[cfg(not(target_family = "unix"))]
                    {
                        Arc::new(turso_core::PlatformIO::new()?)
                    }
                }
                // We are building for Linux and io_uring backend has been selected
                #[cfg(all(target_os = "linux", feature = "io_uring"))]
                "io_uring" => Arc::new(turso_core::UringIO::new()?),
                #[cfg(all(target_os = "windows", feature = "experimental_win_iocp"))]
                "experimental_win_iocp" => Arc::new(turso_core::WindowsIOCP::new()?),
                _ => Arc::new(turso_core::PlatformIO::new()?),
            }
        }
    })
}

#[cfg(all(test, target_os = "windows", feature = "experimental_win_iocp"))]
mod tests {
    use super::{get_io, DbLocation};

    #[test]
    fn experimental_win_iocp_backend_is_available_for_path_databases() {
        let io = get_io(DbLocation::Path, "experimental_win_iocp")
            .expect("windows cli should construct the experimental_win_iocp backend");
        drop(io);
    }
}

pub struct ApplyWriter<'a> {
    target: &'a Arc<turso_core::Connection>,
    // accumulate raw bytes to support non-utf8 BLOB types
    buf: Vec<u8>,
}

impl<'a> ApplyWriter<'a> {
    pub fn new(target: &'a Arc<turso_core::Connection>) -> Self {
        Self {
            target,
            buf: Vec::new(),
        }
    }

    // Find the next statement terminator ;\n or ;\r\n in a byte buffer.
    // Returns (end_idx_inclusive, drain_len), where drain_len includes the newline(s).
    fn find_stmt_end(buf: &[u8]) -> Option<(usize, usize)> {
        let mut i = 0;
        while i < buf.len() {
            // Look for ';'
            if buf[i] == b';' {
                // Accept ;\n
                if i + 1 < buf.len() && buf[i + 1] == b'\n' {
                    return Some((i, 2));
                }
                // Accept ;\r\n
                if i + 2 < buf.len() && buf[i + 1] == b'\r' && buf[i + 2] == b'\n' {
                    return Some((i, 3));
                }
            }
            i += 1;
        }
        None
    }

    pub fn flush_complete_statements(&mut self) -> io::Result<()> {
        while let Some((end_inclusive, drain_len)) = Self::find_stmt_end(&self.buf) {
            // Copy stmt bytes [0..=end_inclusive]
            let stmt_bytes = self.buf[..=end_inclusive].to_vec();
            // Drain including the trailing newline(s)
            self.buf.drain(..end_inclusive + drain_len);
            self.exec_stmt_bytes(&stmt_bytes)?;
        }
        Ok(())
    }

    // Handle final trailing statement that ends with ';' followed only by ASCII whitespace.
    pub fn finish(mut self) -> io::Result<()> {
        // Skip if buffer empty or no ';'
        if let Some(semicolon_pos) = self.buf.iter().rposition(|&b| b == b';') {
            // Are all bytes after ';' ASCII whitespace?
            if self.buf[semicolon_pos + 1..]
                .iter()
                .all(|&b| matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
            {
                let stmt_bytes = self.buf[..=semicolon_pos].to_vec();
                self.buf.clear();
                self.exec_stmt_bytes(&stmt_bytes)?;
            }
        }
        Ok(())
    }

    fn exec_stmt_bytes(&self, stmt_bytes: &[u8]) -> io::Result<()> {
        // SQL must be UTF-8. If not, surface a clear error.
        let sql = std::str::from_utf8(stmt_bytes).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("non-UTF8 SQL: {e}"))
        })?;
        self.exec_stmt(sql)
            .map_err(|e| io::Error::other(e.to_string()))
    }

    fn exec_stmt(&self, sql: &str) -> Result<(), LimboError> {
        match self.target.query(sql) {
            Ok(Some(mut rows)) => {
                rows.run_with_row_callback(|_| Ok(()))?;
            }
            Ok(None) => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }
}

impl<'a> Write for ApplyWriter<'a> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        self.flush_complete_statements()?;
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.flush_complete_statements()
    }
}

pub trait ProgressSink {
    fn on<S: Display>(&mut self, _p: S) {}
}

pub struct NoopProgress;
impl ProgressSink for NoopProgress {}

pub struct StderrProgress;

impl ProgressSink for StderrProgress {
    fn on<S: Display>(&mut self, s: S) {
        eprintln!("{s}... done");
    }
}

pub const BEFORE_HELP_MSG: &str = r#"

Turso SQL Shell Help
==============
Welcome to the Turso SQL Shell! You can execute any standard SQL command here.
In addition to standard SQL commands, the following special commands are available:"#;
pub const AFTER_HELP_MSG: &str = r#"Usage Examples:
---------------
1. To quit the Turso SQL Shell:
   .quit

2. To open a database file at path './employees.db':
   .open employees.db

3. To view the schema of a table named 'employees':
   .schema employees

4. To list all tables:
   .tables

5. To list all databases:
   .databases

6. To list all available SQL opcodes:
   .opcodes

7. To change the current output mode to 'pretty':
   .mode pretty

8. Send output to STDOUT if no file is specified:
   .output

9. To change the current working directory to '/tmp':
   .cd /tmp

10. Show the current values of settings:
   .show

11. To import csv file 'sample.csv' into 'csv_table' table:
   .import --csv sample.csv csv_table

12. To display the database contents as SQL:
   .dump

13. To load an extension library:
   .load /target/debug/liblimbo_regexp

14. To list all available VFS:
   .listvfs

15. To show names of indexes:
   .indexes ?TABLE?

16. To turn on column headers in list mode:
   .headers on

17. To turn off column headers in list mode:
   .headers off

18. To clone the open database to another file:
   .clone output_file.db

19. To view manual pages for features:
   .manual mcp    # View MCP server documentation
   .man           # List all available manuals

20. To bind parameters for subsequent SQL statements:
   .parameter set :name alice
   .parameter list
   .parameter clear :name

Note:
- All SQL commands must end with a semicolon (;).
- Special commands start with a dot (.) and are not required to end with a semicolon."#;
