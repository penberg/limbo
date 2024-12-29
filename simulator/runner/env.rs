use std::rc::Rc;
use std::sync::Arc;

use limbo_core::{Connection, Database};
use rand_chacha::ChaCha8Rng;

use crate::model::table::Table;

use crate::runner::io::SimulatorIO;

pub(crate) struct SimulatorEnv {
    pub(crate) opts: SimulatorOpts,
    pub(crate) tables: Vec<Table>,
    pub(crate) connections: Vec<SimConnection>,
    pub(crate) io: Arc<SimulatorIO>,
    pub(crate) db: Arc<Database>,
    pub(crate) rng: ChaCha8Rng,
}

#[derive(Clone)]
pub(crate) enum SimConnection {
    Connected(Rc<Connection>),
    Disconnected,
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
    pub(crate) max_interactions: usize,
    pub(crate) page_size: usize,
}
