use anyhow::Result;
use clap::Parser;
use lig_core::{Database, DatabaseRef};
use rustyline::{error::ReadlineError, DefaultEditor};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Opts {
    database: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    let io = IO::new();
    let db = Database::open(Arc::new(io), opts.database.to_str().unwrap())?;
    let conn = db.connect();
    let mut rl = DefaultEditor::new()?;
    let home = dirs::home_dir().unwrap();
    let history_file = home.join(".lig_history");
    if history_file.exists() {
        rl.load_history(history_file.as_path())?;
    }
    loop {
        let readline = rl.readline("⚡️ ");
        match readline {
            Ok(line) => {
                if let Err(err) = conn.execute(&line) {
                    eprintln!("{}", err);
                } else {
                    rl.add_history_entry(line)?;
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

struct IO {
    inner: RefCell<IOInner>,
}

struct IOInner {
    db_refs: usize,
    db_files: HashMap<DatabaseRef, File>,
}

impl lig_core::IO for IO {
    fn open(&self, path: &str) -> Result<DatabaseRef> {
        println!("Opening database file {}", path);
        let file = std::fs::File::open(path)?;
        let mut inner = self.inner.borrow_mut();
        let db_ref = inner.db_refs;
        inner.db_refs += 1;
        inner.db_files.insert(db_ref, file);
        Ok(db_ref)
    }

    fn get(&self, database_ref: DatabaseRef, page_idx: usize, buf: &mut [u8]) -> Result<()> {
        let page_size = buf.len();
        assert!(page_idx > 0);
        assert!(page_size >= 512);
        assert!(page_size <= 65536);
        assert!((page_size & (page_size - 1)) == 0);
        let mut inner = self.inner.borrow_mut();
        let file = inner.db_files.get_mut(&database_ref).unwrap();
        let pos = (page_idx - 1) * page_size;
        file.seek(std::io::SeekFrom::Start(pos as u64))?;
        file.read_exact(buf)?;
        Ok(())
    }
}

impl IO {
    fn new() -> Self {
        Self {
            inner: RefCell::new(IOInner {
                db_refs: 0,
                db_files: HashMap::new(),
            }),
        }
    }
}
