use limbo_core::{Connection, Database, File, OpenFlags, PlatformIO, Result, RowResult, IO};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::TempDir;

use anarchist_readable_name_generator_lib::readable_name_custom;

struct SimulatorEnv {
    opts: SimulatorOpts,
    tables: Vec<Table>,
    connections: Vec<SimConnection>,
    io: Arc<SimulatorIO>,
    db: Rc<Database>,
    rng: ChaCha8Rng,
}

#[derive(Clone)]
enum SimConnection {
    Connected(Rc<Connection>),
    Disconnected,
}

#[derive(Debug)]
struct SimulatorOpts {
    ticks: usize,
    max_connections: usize,
    max_tables: usize,
    // this next options are the distribution of workload where read_percent + write_percent +
    // delete_percent == 100%
    read_percent: usize,
    write_percent: usize,
    delete_percent: usize,
    page_size: usize,
}

struct Table {
    rows: Vec<Vec<Value>>,
    name: String,
    columns: Vec<Column>,
}

#[derive(Clone)]
struct Column {
    name: String,
    column_type: ColumnType,
    primary: bool,
    unique: bool,
}

#[derive(Clone)]
enum ColumnType {
    Integer,
    Float,
    Text,
    Blob,
}

#[derive(Debug, PartialEq)]
enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[allow(clippy::arc_with_non_send_sync)]
fn main() {
    let _ = env_logger::try_init();
    let seed = match std::env::var("SEED") {
        Ok(seed) => seed.parse::<u64>().unwrap(),
        Err(_) => rand::thread_rng().next_u64(),
    };
    println!("Seed: {}", seed);
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    let (read_percent, write_percent, delete_percent) = {
        let mut remaining = 100;
        let read_percent = rng.gen_range(0..=remaining);
        remaining -= read_percent;
        let write_percent = rng.gen_range(0..=remaining);
        remaining -= write_percent;
        let delete_percent = remaining;
        (read_percent, write_percent, delete_percent)
    };

    let opts = SimulatorOpts {
        ticks: rng.gen_range(0..4096),
        max_connections: 1, // TODO: for now let's use one connection as we didn't implement
        // correct transactions procesing
        max_tables: rng.gen_range(0..128),
        read_percent,
        write_percent,
        delete_percent,
        page_size: 4096, // TODO: randomize this too
    };
    let io = Arc::new(SimulatorIO::new(seed, opts.page_size).unwrap());

    let mut path = TempDir::new().unwrap().into_path();
    path.push("simulator.db");
    println!("path to db '{:?}'", path);
    let db = match Database::open_file(io.clone(), path.as_path().to_str().unwrap()) {
        Ok(db) => db,
        Err(e) => {
            panic!("error opening simulator test file {:?}: {:?}", path, e);
        }
    };

    let connections = vec![SimConnection::Disconnected; opts.max_connections];
    let mut env = SimulatorEnv {
        opts,
        tables: vec![],
        connections,
        rng,
        io,
        db,
    };

    println!("Initial opts {:?}", env.opts);

    for _ in 0..env.opts.ticks {
        let connection_index = env.rng.gen_range(0..env.opts.max_connections);
        let mut connection = env.connections[connection_index].clone();

        match &mut connection {
            SimConnection::Connected(conn) => {
                let disconnect = env.rng.gen_ratio(1, 100);
                if disconnect {
                    log::info!("disconnecting {}", connection_index);
                    let _ = conn.close();
                    env.connections[connection_index] = SimConnection::Disconnected;
                } else {
                    match process_connection(&mut env, conn) {
                        Ok(_) => {}
                        Err(err) => {
                            log::error!("error {}", err);
                            break;
                        }
                    }
                }
            }
            SimConnection::Disconnected => {
                log::info!("disconnecting {}", connection_index);
                env.connections[connection_index] = SimConnection::Connected(env.db.connect());
            }
        }
    }

    env.io.print_stats();
}

fn process_connection(env: &mut SimulatorEnv, conn: &mut Rc<Connection>) -> Result<()> {
    let management = env.rng.gen_ratio(1, 100);
    if management {
        // for now create table only
        maybe_add_table(env, conn)?;
    } else if env.tables.is_empty() {
        maybe_add_table(env, conn)?;
    } else {
        let roll = env.rng.gen_range(0..100);
        if roll < env.opts.read_percent {
            // read
            do_select(env, conn)?;
        } else if roll < env.opts.read_percent + env.opts.write_percent {
            // write
            do_write(env, conn)?;
        } else {
            // delete
            // TODO
        }
    }
    Ok(())
}

fn do_select(env: &mut SimulatorEnv, conn: &mut Rc<Connection>) -> Result<()> {
    let table = env.rng.gen_range(0..env.tables.len());
    let table_name = {
        let table = &env.tables[table];
        table.name.clone()
    };
    let rows = get_all_rows(env, conn, format!("SELECT * FROM {}", table_name).as_str())?;

    let table = &env.tables[table];
    compare_equal_rows(&table.rows, &rows);
    Ok(())
}

fn do_write(env: &mut SimulatorEnv, conn: &mut Rc<Connection>) -> Result<()> {
    let mut query = String::new();
    let table = env.rng.gen_range(0..env.tables.len());
    {
        let table = &env.tables[table];
        query.push_str(format!("INSERT INTO {} VALUES (", table.name).as_str());
    }

    let columns = env.tables[table].columns.clone();
    let mut row = vec![];

    // gen insert query
    for column in &columns {
        let value = match column.column_type {
            ColumnType::Integer => Value::Integer(env.rng.gen_range(i64::MIN..i64::MAX)),
            ColumnType::Float => Value::Float(env.rng.gen_range(-1e10..1e10)),
            ColumnType::Text => Value::Text(gen_random_text(env)),
            ColumnType::Blob => Value::Blob(gen_random_text(env).as_bytes().to_vec()),
        };

        query.push_str(value.to_string().as_str());
        query.push(',');
        row.push(value);
    }

    let table = &mut env.tables[table];
    table.rows.push(row);

    query.pop();
    query.push_str(");");

    let _ = get_all_rows(env, conn, query.as_str())?;

    Ok(())
}

fn compare_equal_rows(a: &[Vec<Value>], b: &[Vec<Value>]) {
    assert_eq!(a.len(), b.len(), "lengths are different");
    for (r1, r2) in a.iter().zip(b) {
        for (v1, v2) in r1.iter().zip(r2) {
            assert_eq!(v1, v2, "values are different");
        }
    }
}

fn maybe_add_table(env: &mut SimulatorEnv, conn: &mut Rc<Connection>) -> Result<()> {
    if env.tables.len() < env.opts.max_tables {
        let table = Table {
            rows: vec![],
            name: gen_random_name(env),
            columns: gen_columns(env),
        };
        let rows = get_all_rows(env, conn, table.to_create_str().as_str())?;
        log::debug!("{:?}", rows);
        let rows = get_all_rows(
            env,
            conn,
            format!(
                "SELECT sql FROM sqlite_schema WHERE type IN ('table', 'index') AND name = '{}';",
                table.name
            )
            .as_str(),
        )?;
        log::debug!("{:?}", rows);
        assert!(rows.len() == 1);
        let as_text = match &rows[0][0] {
            Value::Text(t) => t,
            _ => unreachable!(),
        };
        assert!(
            *as_text != table.to_create_str(),
            "table was not inserted correctly"
        );
        env.tables.push(table);
    }
    Ok(())
}

fn gen_random_name(env: &mut SimulatorEnv) -> String {
    let name = readable_name_custom("_", &mut env.rng);
    name.replace("-", "_")
}

fn gen_random_text(env: &mut SimulatorEnv) -> String {
    let big_text = env.rng.gen_ratio(1, 1000);
    if big_text {
        let max_size: u64 = 2 * 1024 * 1024 * 1024;
        let size = env.rng.gen_range(1024..max_size);
        let mut name = String::new();
        for i in 0..size {
            name.push(((i % 26) as u8 + b'A') as char);
        }
        name
    } else {
        let name = readable_name_custom("_", &mut env.rng);
        name.replace("-", "_")
    }
}

fn gen_columns(env: &mut SimulatorEnv) -> Vec<Column> {
    let mut column_range = env.rng.gen_range(1..128);
    let mut columns = vec![];
    while column_range > 0 {
        let column_type = match env.rng.gen_range(0..4) {
            0 => ColumnType::Integer,
            1 => ColumnType::Float,
            2 => ColumnType::Text,
            3 => ColumnType::Blob,
            _ => unreachable!(),
        };
        let column = Column {
            name: gen_random_name(env),
            column_type,
            primary: false,
            unique: false,
        };
        columns.push(column);
        column_range -= 1;
    }
    columns
}

fn get_all_rows(
    env: &mut SimulatorEnv,
    conn: &mut Rc<Connection>,
    query: &str,
) -> Result<Vec<Vec<Value>>> {
    log::info!("running query '{}'", &query[0..query.len().min(4096)]);
    let mut out = vec![];
    let rows = conn.query(query);
    if rows.is_err() {
        let err = rows.err();
        log::error!(
            "Error running query '{}': {:?}",
            &query[0..query.len().min(4096)],
            err
        );
        return Err(err.unwrap());
    }
    let rows = rows.unwrap();
    assert!(rows.is_some());
    let mut rows = rows.unwrap();
    'rows_loop: loop {
        env.io.inject_fault(env.rng.gen_ratio(1, 10000));
        match rows.next_row()? {
            RowResult::Row(row) => {
                let mut r = vec![];
                for el in &row.values {
                    let v = match el {
                        limbo_core::Value::Null => Value::Null,
                        limbo_core::Value::Integer(i) => Value::Integer(*i),
                        limbo_core::Value::Float(f) => Value::Float(*f),
                        limbo_core::Value::Text(t) => Value::Text(t.to_string()),
                        limbo_core::Value::Blob(b) => Value::Blob(b.to_vec()),
                    };
                    r.push(v);
                }

                out.push(r);
            }
            RowResult::IO => {
                env.io.inject_fault(env.rng.gen_ratio(1, 10000));
                if env.io.run_once().is_err() {
                    log::info!("query inject fault");
                    break 'rows_loop;
                }
            }
            RowResult::Done => {
                break;
            }
        }
    }
    Ok(out)
}

struct SimulatorIO {
    inner: Box<dyn IO>,
    fault: RefCell<bool>,
    files: RefCell<Vec<Rc<SimulatorFile>>>,
    rng: RefCell<ChaCha8Rng>,
    nr_run_once_faults: RefCell<usize>,
    page_size: usize,
}

impl SimulatorIO {
    fn new(seed: u64, page_size: usize) -> Result<Self> {
        let inner = Box::new(PlatformIO::new()?);
        let fault = RefCell::new(false);
        let files = RefCell::new(vec![]);
        let rng = RefCell::new(ChaCha8Rng::seed_from_u64(seed));
        let nr_run_once_faults = RefCell::new(0);
        Ok(Self {
            inner,
            fault,
            files,
            rng,
            nr_run_once_faults,
            page_size,
        })
    }

    fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
        for file in self.files.borrow().iter() {
            file.inject_fault(fault);
        }
    }

    fn print_stats(&self) {
        println!("run_once faults: {}", self.nr_run_once_faults.borrow());
        for file in self.files.borrow().iter() {
            file.print_stats();
        }
    }
}

impl IO for SimulatorIO {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        _direct: bool,
    ) -> Result<Rc<dyn limbo_core::File>> {
        let inner = self.inner.open_file(path, flags, false)?;
        let file = Rc::new(SimulatorFile {
            inner,
            fault: RefCell::new(false),
            nr_pread_faults: RefCell::new(0),
            nr_pwrite_faults: RefCell::new(0),
            reads: RefCell::new(0),
            writes: RefCell::new(0),
            syncs: RefCell::new(0),
            page_size: self.page_size,
        });
        self.files.borrow_mut().push(file.clone());
        Ok(file)
    }

    fn run_once(&self) -> Result<()> {
        if *self.fault.borrow() {
            *self.nr_run_once_faults.borrow_mut() += 1;
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        self.inner.run_once().unwrap();
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        self.rng.borrow_mut().next_u64() as i64
    }

    fn get_current_time(&self) -> String {
        "2024-01-01 00:00:00".to_string()
    }
}

struct SimulatorFile {
    inner: Rc<dyn File>,
    fault: RefCell<bool>,
    nr_pread_faults: RefCell<usize>,
    nr_pwrite_faults: RefCell<usize>,
    writes: RefCell<usize>,
    reads: RefCell<usize>,
    syncs: RefCell<usize>,
    page_size: usize,
}

impl SimulatorFile {
    fn inject_fault(&self, fault: bool) {
        self.fault.replace(fault);
    }

    fn print_stats(&self) {
        println!(
            "pread faults: {}, pwrite faults: {}, reads: {}, writes: {}, syncs: {}",
            *self.nr_pread_faults.borrow(),
            *self.nr_pwrite_faults.borrow(),
            *self.reads.borrow(),
            *self.writes.borrow(),
            *self.syncs.borrow(),
        );
    }
}

impl limbo_core::File for SimulatorFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        if *self.fault.borrow() {
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        self.inner.lock_file(exclusive)
    }

    fn unlock_file(&self) -> Result<()> {
        if *self.fault.borrow() {
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        self.inner.unlock_file()
    }

    fn pread(&self, pos: usize, c: Rc<limbo_core::Completion>) -> Result<()> {
        if *self.fault.borrow() {
            *self.nr_pread_faults.borrow_mut() += 1;
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        *self.reads.borrow_mut() += 1;
        self.inner.pread(pos, c)
    }

    fn pwrite(
        &self,
        pos: usize,
        buffer: Rc<std::cell::RefCell<limbo_core::Buffer>>,
        c: Rc<limbo_core::Completion>,
    ) -> Result<()> {
        if *self.fault.borrow() {
            *self.nr_pwrite_faults.borrow_mut() += 1;
            return Err(limbo_core::LimboError::InternalError(
                "Injected fault".into(),
            ));
        }
        *self.writes.borrow_mut() += 1;
        self.inner.pwrite(pos, buffer, c)
    }

    fn sync(&self, c: Rc<limbo_core::Completion>) -> Result<()> {
        *self.syncs.borrow_mut() += 1;
        self.inner.sync(c)
    }

    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
}

impl Drop for SimulatorFile {
    fn drop(&mut self) {
        self.inner.unlock_file().expect("Failed to unlock file");
    }
}

impl ColumnType {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Integer => "INTEGER",
            Self::Float => "FLOAT",
            Self::Text => "TEXT",
            Self::Blob => "BLOB",
        }
    }
}

impl Table {
    pub fn to_create_str(&self) -> String {
        let mut out = String::new();

        out.push_str(format!("CREATE TABLE {} (", self.name).as_str());

        assert!(!self.columns.is_empty());
        for column in &self.columns {
            out.push_str(format!("{} {},", column.name, column.column_type.as_str()).as_str());
        }
        // remove last comma
        out.pop();

        out.push_str(");");
        out
    }
}

impl Value {
    pub fn to_string(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Integer(i) => i.to_string(),
            Self::Float(f) => f.to_string(),
            Self::Text(t) => format!("'{}'", t.clone()),
            Self::Blob(vec) => to_sqlite_blob(vec),
        }
    }
}

fn to_sqlite_blob(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{:02X}", b)).collect();
    format!("X'{}'", hex)
}
