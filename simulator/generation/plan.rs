use std::{fmt::Display, rc::Rc};

use limbo_core::{Connection, Result, RowResult};
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;

use crate::{
    model::{
        query::{Create, Insert, Predicate, Query, Select},
        table::Value,
    },
    SimConnection, SimulatorEnv, SimulatorOpts,
};

use crate::generation::{frequency, Arbitrary, ArbitraryFrom};

use super::{pick, pick_index};

pub(crate) type ResultSet = Vec<Vec<Value>>;

pub(crate) struct InteractionPlan {
    pub(crate) plan: Vec<Interaction>,
    pub(crate) stack: Vec<ResultSet>,
    pub(crate) interaction_pointer: usize,
}

impl Display for InteractionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for interaction in &self.plan {
            match interaction {
                Interaction::Query(query) => write!(f, "{};\n", query)?,
                Interaction::Assertion(assertion) => {
                    write!(f, "-- ASSERT: {};\n", assertion.message)?
                }
                Interaction::Fault(fault) => write!(f, "-- FAULT: {};\n", fault)?,
            }
        }

        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct InteractionStats {
    pub(crate) read_count: usize,
    pub(crate) write_count: usize,
    pub(crate) delete_count: usize,
}

impl Display for InteractionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Read: {}, Write: {}, Delete: {}",
            self.read_count, self.write_count, self.delete_count
        )
    }
}

pub(crate) enum Interaction {
    Query(Query),
    Assertion(Assertion),
    Fault(Fault),
}

impl Display for Interaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Interaction::Query(query) => write!(f, "{}", query),
            Interaction::Assertion(assertion) => write!(f, "ASSERT: {}", assertion.message),
            Interaction::Fault(fault) => write!(f, "FAULT: {}", fault),
        }
    }
}

pub(crate) struct Assertion {
    pub(crate) func: Box<dyn Fn(&Vec<ResultSet>) -> bool>,
    pub(crate) message: String,
}

pub(crate) enum Fault {
    Disconnect,
}

impl Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Fault::Disconnect => write!(f, "DISCONNECT"),
        }
    }
}

pub(crate) struct Interactions(Vec<Interaction>);

impl Interactions {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) {
        for interaction in &self.0 {
            match interaction {
                Interaction::Query(query) => match query {
                    Query::Create(create) => {
                        env.tables.push(create.table.clone());
                    }
                    Query::Insert(insert) => {
                        let table = env
                            .tables
                            .iter_mut()
                            .find(|t| t.name == insert.table)
                            .unwrap();
                        table.rows.extend(insert.values.clone());
                    }
                    Query::Delete(_) => todo!(),
                    Query::Select(_) => {}
                },
                Interaction::Assertion(_) => {}
                Interaction::Fault(_) => {}
            }
        }
    }
}

impl InteractionPlan {
    pub(crate) fn new() -> Self {
        InteractionPlan {
            plan: Vec::new(),
            stack: Vec::new(),
            interaction_pointer: 0,
        }
    }

    pub(crate) fn push(&mut self, interaction: Interaction) {
        self.plan.push(interaction);
    }

    pub(crate) fn stats(&self) -> InteractionStats {
        let mut read = 0;
        let mut write = 0;
        let mut delete = 0;

        for interaction in &self.plan {
            match interaction {
                Interaction::Query(query) => match query {
                    Query::Select(_) => read += 1,
                    Query::Insert(_) => write += 1,
                    Query::Delete(_) => delete += 1,
                    Query::Create(_) => {}
                },
                Interaction::Assertion(_) => {}
                Interaction::Fault(_) => {}
            }
        }

        InteractionStats {
            read_count: read,
            write_count: write,
            delete_count: delete,
        }
    }
}

impl ArbitraryFrom<SimulatorEnv> for InteractionPlan {
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        let mut plan = InteractionPlan::new();

        let mut env = SimulatorEnv {
            opts: env.opts.clone(),
            tables: vec![],
            connections: vec![],
            io: env.io.clone(),
            db: env.db.clone(),
            rng: ChaCha8Rng::seed_from_u64(rng.next_u64()),
        };

        let num_interactions = rng.gen_range(0..env.opts.max_interactions);

        // First create at least one table
        let create_query = Create::arbitrary(rng);
        env.tables.push(create_query.table.clone());
        plan.push(Interaction::Query(Query::Create(create_query)));

        while plan.plan.len() < num_interactions {
            log::debug!(
                "Generating interaction {}/{}",
                plan.plan.len(),
                num_interactions
            );
            let interactions = Interactions::arbitrary_from(rng, &(&env, plan.stats()));
            interactions.shadow(&mut env);

            plan.plan.extend(interactions.0.into_iter());
        }

        log::info!("Generated plan with {} interactions", plan.plan.len());
        plan
    }
}

impl Interaction {
    pub(crate) fn execute_query(&self, conn: &mut Rc<Connection>) -> Result<ResultSet> {
        match self {
            Interaction::Query(query) => {
                let query_str = query.to_string();
                let rows = conn.query(&query_str);
                if rows.is_err() {
                    let err = rows.err();
                    log::error!(
                        "Error running query '{}': {:?}",
                        &query_str[0..query_str.len().min(4096)],
                        err
                    );
                    return Err(err.unwrap());
                }
                let rows = rows.unwrap();
                assert!(rows.is_some());
                let mut rows = rows.unwrap();
                let mut out = Vec::new();
                while let Ok(row) = rows.next_row() {
                    match row {
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
                        RowResult::IO => {}
                        RowResult::Interrupt => {}
                        RowResult::Done => {
                            break;
                        }
                    }
                }

                Ok(out)
            }
            Interaction::Assertion(_) => {
                unreachable!("unexpected: this function should only be called on queries")
            }
            Interaction::Fault(fault) => {
                unreachable!("unexpected: this function should only be called on queries")
            }
        }
    }

    pub(crate) fn execute_assertion(&self, stack: &Vec<ResultSet>) -> Result<()> {
        match self {
            Interaction::Query(_) => {
                unreachable!("unexpected: this function should only be called on assertions")
            }
            Interaction::Assertion(assertion) => {
                if !assertion.func.as_ref()(stack) {
                    return Err(limbo_core::LimboError::InternalError(
                        assertion.message.clone(),
                    ));
                }
                Ok(())
            }
            Interaction::Fault(_) => {
                unreachable!("unexpected: this function should only be called on assertions")
            }
        }
    }

    pub(crate) fn execute_fault(&self, env: &mut SimulatorEnv, conn_index: usize) -> Result<()> {
        match self {
            Interaction::Query(_) => {
                unreachable!("unexpected: this function should only be called on faults")
            }
            Interaction::Assertion(_) => {
                unreachable!("unexpected: this function should only be called on faults")
            }
            Interaction::Fault(fault) => {
                match fault {
                    Fault::Disconnect => {
                        match env.connections[conn_index] {
                            SimConnection::Connected(ref mut conn) => {
                                conn.close()?;
                            }
                            SimConnection::Disconnected => {
                                return Err(limbo_core::LimboError::InternalError(
                                    "Tried to disconnect a disconnected connection".to_string(),
                                ));
                            }
                        }
                        env.connections[conn_index] = SimConnection::Disconnected;
                    }
                }
                Ok(())
            }
        }
    }
}

fn property_insert_select<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    // Get a random table
    let table = pick(&env.tables, rng);
    // Pick a random column
    let column_index = pick_index(table.columns.len(), rng);
    let column = &table.columns[column_index].clone();
    // Generate a random value of the column type
    let value = Value::arbitrary_from(rng, &column.column_type);
    // Create a whole new row
    let mut row = Vec::new();
    for (i, column) in table.columns.iter().enumerate() {
        if i == column_index {
            row.push(value.clone());
        } else {
            let value = Value::arbitrary_from(rng, &column.column_type);
            row.push(value);
        }
    }
    // Insert the row
    let insert_query = Interaction::Query(Query::Insert(Insert {
        table: table.name.clone(),
        values: vec![row.clone()],
    }));

    // Select the row
    let select_query = Interaction::Query(Query::Select(Select {
        table: table.name.clone(),
        predicate: Predicate::Eq(column.name.clone(), value.clone()),
    }));

    // Check that the row is there
    let assertion = Interaction::Assertion(Assertion {
        message: format!(
            "row [{:?}] not found in table {} after inserting ({} = {})",
            row.iter().map(|v| v.to_string()).collect::<Vec<String>>(),
            table.name,
            column.name,
            value,
        ),
        func: Box::new(move |stack: &Vec<ResultSet>| {
            let rows = stack.last().unwrap();
            rows.iter().any(|r| r == &row)
        }),
    });

    Interactions(vec![insert_query, select_query, assertion])
}

fn create_table<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    let create_query = Interaction::Query(Query::Create(Create::arbitrary(rng)));
    Interactions(vec![create_query])
}

fn random_read<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    let select_query = Interaction::Query(Query::Select(Select::arbitrary_from(rng, &env.tables)));
    Interactions(vec![select_query])
}

fn random_write<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    let table = pick(&env.tables, rng);
    let insert_query = Interaction::Query(Query::Insert(Insert::arbitrary_from(rng, table)));
    Interactions(vec![insert_query])
}

fn random_fault<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    let fault = Interaction::Fault(Fault::Disconnect);
    Interactions(vec![fault])
}

impl ArbitraryFrom<(&SimulatorEnv, InteractionStats)> for Interactions {
    fn arbitrary_from<R: rand::Rng>(
        rng: &mut R,
        (env, stats): &(&SimulatorEnv, InteractionStats),
    ) -> Self {
        let remaining_read =
            ((((env.opts.max_interactions * env.opts.read_percent) as f64) / 100.0) as usize)
                .saturating_sub(stats.read_count);
        let remaining_write = ((((env.opts.max_interactions * env.opts.write_percent) as f64)
            / 100.0) as usize)
            .saturating_sub(stats.write_count);

        frequency(
            vec![
                (
                    usize::min(remaining_read, remaining_write),
                    Box::new(|rng: &mut R| property_insert_select(rng, env)),
                ),
                (
                    remaining_read,
                    Box::new(|rng: &mut R| random_read(rng, env)),
                ),
                (
                    remaining_write,
                    Box::new(|rng: &mut R| random_write(rng, env)),
                ),
                (
                    remaining_write / 10,
                    Box::new(|rng: &mut R| create_table(rng, env)),
                ),
                (1, Box::new(|rng: &mut R| random_fault(rng, env))),
            ],
            rng,
        )
    }
}
