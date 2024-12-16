use generation::{Arbitrary, ArbitraryFrom};
use limbo_core::{Connection, Database, File, OpenFlags, PlatformIO, Result, RowResult, IO};
use model::table::{Column, Name, Table, Value};
use model::query::{Insert, Predicate, Query, Select};
use properties::{property_insert_select, property_select_all};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::TempDir;

mod generation;
mod properties;
mod model;

struct SimulatorEnv {
    opts: SimulatorOpts,
    tables: Vec<Table>,
    connections: Vec<SimConnection>,
    io: Arc<SimulatorIO>,
    db: Arc<Database>,
    rng: ChaCha8Rng,
}

#[derive(Clone)]
enum SimConnection {
    Connected(Rc<Connection>),
    Disconnected,
}

#[derive(Debug, Copy, Clone)]
enum SimulatorMode {
    Random,
    Workload,
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
    mode: SimulatorMode,
    page_size: usize,
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
        mode: SimulatorMode::Workload,
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
        tables: Vec::new(),
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
    if env.tables.is_empty() {
        maybe_add_table(env, conn)?;
    }

    match env.opts.mode {
        SimulatorMode::Random => {
            match env.rng.gen_range(0..2) {
                // Randomly insert a value and check that the select result contains it.
                0 => property_insert_select(env, conn),
                // Check that the current state of the in-memory table is the same as the one in the
                // database.
                1 => property_select_all(env, conn),
                // Perform a random query, update the in-memory table with the result.
                2 => {
                    let table_index = env.rng.gen_range(0..env.tables.len());
                    let query = Query::arbitrary_from(&mut env.rng, &env.tables[table_index]);
                    let rows = get_all_rows(env, conn, query.to_string().as_str())?;
                    env.tables[table_index].rows = rows;
                }
                _ => unreachable!(),
            }
        }
        SimulatorMode::Workload => {
            let picked = env.rng.gen_range(0..100);

            if env.rng.gen_ratio(1, 100) {
                maybe_add_table(env, conn)?;
            }

            if picked < env.opts.read_percent {
                let query = Select::arbitrary_from(&mut env.rng, &env.tables);
                let _ = get_all_rows(env, conn, Query::Select(query).to_string().as_str())?;
            } else if picked < env.opts.read_percent + env.opts.write_percent {
                let table_index = env.rng.gen_range(0..env.tables.len());
                let column_index = env.rng.gen_range(0..env.tables[table_index].columns.len());
                let column = &env.tables[table_index].columns[column_index].clone();
                let mut rng = env.rng.clone();
                let value = Value::arbitrary_from(&mut rng, &column.column_type);
                let mut row = Vec::new();
                for (i, column) in env.tables[table_index].columns.iter().enumerate() {
                    if i == column_index {
                        row.push(value.clone());
                    } else {
                        let value = Value::arbitrary_from(&mut rng, &column.column_type);
                        row.push(value);
                    }
                }
                let query = Query::Insert(Insert {
                    table: env.tables[table_index].name.clone(),
                    values: row.clone(),
                });
                let _ = get_all_rows(env, conn, query.to_string().as_str())?;
                env.tables[table_index].rows.push(row.clone());
            } else {
                let table_index = env.rng.gen_range(0..env.tables.len());
                let query = Query::Select(Select {
                    table: env.tables[table_index].name.clone(),
                    predicate: Predicate::And(Vec::new()),
                });
                let _ = get_all_rows(env, conn, query.to_string().as_str())?;
            }
        }
    }

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
            rows: Vec::new(),
            name: Name::arbitrary(&mut env.rng).0,
            columns: (1..env.rng.gen_range(1..128))
                .map(|_| Column::arbitrary(&mut env.rng))
                .collect(),
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



fn get_all_rows(
    env: &mut SimulatorEnv,
    conn: &mut Rc<Connection>,
    query: &str,
) -> Result<Vec<Vec<Value>>> {
    log::info!("running query '{}'", &query[0..query.len().min(4096)]);
    let mut out = Vec::new();
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
                let mut r = Vec::new();
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
        let files = RefCell::new(Vec::new());
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

