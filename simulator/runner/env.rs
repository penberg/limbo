use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;

use limbo_core::{Connection, Database};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use crate::model::table::Table;

use crate::runner::io::SimulatorIO;

use super::cli::SimulatorCLI;

pub trait SimulatorEnvTrait {
    fn tables(&self) -> &Vec<Table>;
    fn tables_mut(&mut self) -> &mut Vec<Table>;
}

#[derive(Clone)]
pub(crate) struct SimulatorEnv {
    pub(crate) opts: SimulatorOpts,
    pub(crate) tables: Vec<Table>,
    pub(crate) connections: Vec<SimConnection>,
    pub(crate) io: Arc<SimulatorIO>,
    pub(crate) db: Arc<Database>,
    pub(crate) rng: ChaCha8Rng,
}

impl SimulatorEnvTrait for SimulatorEnv {
    fn tables(&self) -> &Vec<Table> {
        &self.tables
    }

    fn tables_mut(&mut self) -> &mut Vec<Table> {
        &mut self.tables
    }
}

impl SimulatorEnv {
    pub(crate) fn new(seed: u64, cli_opts: &SimulatorCLI, db_path: &Path) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);

        let (create_percent, read_percent, write_percent, delete_percent, drop_percent) = {
            let total = 100.0;

            let read_percent = rng.gen_range(0.0..=total);
            let write_percent = total - read_percent;

            // Create percent should be 5-15% of the write percent
            let create_percent = rng.gen_range(0.05..=0.15) * write_percent;
            // Drop percent should be 2-5% of the write percent
            let drop_percent = rng.gen_range(0.02..=0.05) * write_percent;
            // Delete percent should be 10-20% of the write percent
            let delete_percent = rng.gen_range(0.1..=0.2) * write_percent;

            let write_percent = write_percent - create_percent - delete_percent - drop_percent;

            (
                create_percent,
                read_percent,
                write_percent,
                delete_percent,
                drop_percent,
            )
        };

        let opts = SimulatorOpts {
            ticks: rng.gen_range(cli_opts.minimum_size..=cli_opts.maximum_size),
            max_connections: 1, // TODO: for now let's use one connection as we didn't implement
            // correct transactions processing
            max_tables: rng.gen_range(0..128),
            create_percent,
            read_percent,
            write_percent,
            delete_percent,
            drop_percent,
            page_size: 4096, // TODO: randomize this too
            max_interactions: rng.gen_range(cli_opts.minimum_size..=cli_opts.maximum_size),
            max_time_simulation: cli_opts.maximum_time,
        };

        let io = Arc::new(SimulatorIO::new(seed, opts.page_size).unwrap());

        // Remove existing database file if it exists
        if db_path.exists() {
            std::fs::remove_file(db_path).unwrap();
        }

        let db = match Database::open_file(io.clone(), db_path.to_str().unwrap()) {
            Ok(db) => db,
            Err(e) => {
                panic!("error opening simulator test file {:?}: {:?}", db_path, e);
            }
        };

        let connections = vec![SimConnection::Disconnected; opts.max_connections];

        SimulatorEnv {
            opts,
            tables: Vec::new(),
            connections,
            rng,
            io,
            db,
        }
    }
}

pub trait ConnectionTrait {
    fn is_connected(&self) -> bool;
    fn disconnect(&mut self);
}

#[derive(Clone)]
pub(crate) enum SimConnection {
    Connected(Rc<Connection>),
    Disconnected,
}

impl ConnectionTrait for SimConnection {
    fn is_connected(&self) -> bool {
        match self {
            SimConnection::Connected(_) => true,
            SimConnection::Disconnected => false,
        }
    }

    fn disconnect(&mut self) {
        *self = SimConnection::Disconnected;
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SimulatorOpts {
    pub(crate) ticks: usize,
    pub(crate) max_connections: usize,
    pub(crate) max_tables: usize,
    // this next options are the distribution of workload where read_percent + write_percent +
    // delete_percent == 100%
    pub(crate) create_percent: f64,
    pub(crate) read_percent: f64,
    pub(crate) write_percent: f64,
    pub(crate) delete_percent: f64,
    pub(crate) drop_percent: f64,
    pub(crate) max_interactions: usize,
    pub(crate) page_size: usize,
    pub(crate) max_time_simulation: usize,
}
