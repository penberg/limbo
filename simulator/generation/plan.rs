use std::{fmt::Display, rc::Rc};

use limbo_core::{Connection, Result, StepResult};

use crate::{
    model::{
        query::{Create, Insert, Predicate, Query, Select},
        table::Value,
    },
    SimConnection, SimulatorEnv,
};

use crate::generation::{frequency, Arbitrary, ArbitraryFrom};

use super::{pick, pick_index};

pub(crate) type ResultSet = Result<Vec<Vec<Value>>>;

pub(crate) struct InteractionPlan {
    pub(crate) plan: Vec<Property>,
    pub(crate) stack: Vec<ResultSet>,
    pub(crate) interaction_pointer: usize,
    pub(crate) secondary_pointer: usize,
}

pub(crate) struct Property {
    pub(crate) name: Option<String>,
    pub(crate) interactions: Vec<Interaction>,
}

impl Property {
    pub(crate) fn new(name: Option<String>, interactions: Vec<Interaction>) -> Self {
        Self { name, interactions }
    }

    pub(crate) fn anonymous(interactions: Vec<Interaction>) -> Self {
        Self {
            name: None,
            interactions,
        }
    }
}

impl Display for InteractionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for property in &self.plan {
            if let Some(name) = &property.name {
                writeln!(f, "-- begin testing '{}'", name)?;
            }

            for interaction in &property.interactions {
                if property.name.is_some() {
                    write!(f, "\t")?;
                }

                match interaction {
                    Interaction::Query(query) => writeln!(f, "{};", query)?,
                    Interaction::Assumption(assumption) => {
                        writeln!(f, "-- ASSUME: {};", assumption.message)?
                    }
                    Interaction::Assertion(assertion) => {
                        writeln!(f, "-- ASSERT: {};", assertion.message)?
                    }
                    Interaction::Fault(fault) => writeln!(f, "-- FAULT: {};", fault)?,
                }
            }

            if let Some(name) = &property.name {
                writeln!(f, "-- end testing '{}'", name)?;
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
    pub(crate) create_count: usize,
}

impl Display for InteractionStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Read: {}, Write: {}, Delete: {}, Create: {}",
            self.read_count, self.write_count, self.delete_count, self.create_count
        )
    }
}

pub(crate) enum Interaction {
    Query(Query),
    Assumption(Assertion),
    Assertion(Assertion),
    Fault(Fault),
}

impl Display for Interaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(query) => write!(f, "{}", query),
            Self::Assumption(assumption) => write!(f, "ASSUME: {}", assumption.message),
            Self::Assertion(assertion) => write!(f, "ASSERT: {}", assertion.message),
            Self::Fault(fault) => write!(f, "FAULT: {}", fault),
        }
    }
}

type AssertionFunc = dyn Fn(&Vec<ResultSet>, &SimulatorEnv) -> bool;

pub(crate) struct Assertion {
    pub(crate) func: Box<AssertionFunc>,
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

impl Property {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) {
        for interaction in &self.interactions {
            match interaction {
                Interaction::Query(query) => match query {
                    Query::Create(create) => {
                        if !env.tables.iter().any(|t| t.name == create.table.name) {
                            env.tables.push(create.table.clone());
                        }
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
                Interaction::Assumption(_) => {}
                Interaction::Fault(_) => {}
            }
        }
    }
}

impl InteractionPlan {
    pub(crate) fn new() -> Self {
        Self {
            plan: Vec::new(),
            stack: Vec::new(),
            interaction_pointer: 0,
            secondary_pointer: 0,
        }
    }

    pub(crate) fn stats(&self) -> InteractionStats {
        let mut read = 0;
        let mut write = 0;
        let mut delete = 0;
        let mut create = 0;

        for property in &self.plan {
            for interaction in &property.interactions {
                match interaction {
                    Interaction::Query(query) => match query {
                        Query::Select(_) => read += 1,
                        Query::Insert(_) => write += 1,
                        Query::Delete(_) => delete += 1,
                        Query::Create(_) => create += 1,
                    },
                    Interaction::Assertion(_) => {}
                    Interaction::Assumption(_) => {}
                    Interaction::Fault(_) => {}
                }
            }
        }

        InteractionStats {
            read_count: read,
            write_count: write,
            delete_count: delete,
            create_count: create,
        }
    }
}

impl InteractionPlan {
    // todo: This is a hack to get around the fact that `ArbitraryFrom<T>` can't take a mutable
    // reference of T, so instead write a bespoke function without using the trait system.
    pub(crate) fn arbitrary_from<R: rand::Rng>(rng: &mut R, env: &mut SimulatorEnv) -> Self {
        let mut plan = InteractionPlan::new();

        let num_interactions = env.opts.max_interactions;

        // First create at least one table
        let create_query = Create::arbitrary(rng);
        env.tables.push(create_query.table.clone());

        plan.plan.push(Property {
            name: Some("initial table creation".to_string()),
            interactions: vec![Interaction::Query(Query::Create(create_query))],
        });

        while plan.plan.len() < num_interactions {
            log::debug!(
                "Generating interaction {}/{}",
                plan.plan.len(),
                num_interactions
            );
            let property = Property::arbitrary_from(rng, &(env, plan.stats()));
            property.shadow(env);

            plan.plan.push(property);
        }

        log::info!("Generated plan with {} interactions", plan.plan.len());
        plan
    }
}

impl Interaction {
    pub(crate) fn execute_query(&self, conn: &mut Rc<Connection>) -> ResultSet {
        match self {
            Self::Query(query) => {
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
                        StepResult::Row(row) => {
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
                        StepResult::IO => {}
                        StepResult::Interrupt => {}
                        StepResult::Done => {
                            break;
                        }
                        StepResult::Busy => {}
                    }
                }

                Ok(out)
            }
            Self::Assertion(_) => {
                unreachable!("unexpected: this function should only be called on queries")
            }
            Self::Assumption(_) => {
                unreachable!("unexpected: this function should only be called on queries")
            }
            Self::Fault(_) => {
                unreachable!("unexpected: this function should only be called on queries")
            }
        }
    }

    pub(crate) fn execute_assertion(
        &self,
        stack: &Vec<ResultSet>,
        env: &SimulatorEnv,
    ) -> Result<()> {
        match self {
            Self::Query(_) => {
                unreachable!("unexpected: this function should only be called on assertions")
            }
            Self::Assertion(assertion) => {
                if !assertion.func.as_ref()(stack, env) {
                    return Err(limbo_core::LimboError::InternalError(
                        assertion.message.clone(),
                    ));
                }
                Ok(())
            }
            Self::Assumption(_) => {
                unreachable!("unexpected: this function should only be called on assertions")
            }
            Self::Fault(_) => {
                unreachable!("unexpected: this function should only be called on assertions")
            }
        }
    }

    pub(crate) fn execute_assumption(
        &self,
        stack: &Vec<ResultSet>,
        env: &SimulatorEnv,
    ) -> Result<()> {
        match self {
            Self::Query(_) => {
                unreachable!("unexpected: this function should only be called on assumptions")
            }
            Self::Assertion(_) => {
                unreachable!("unexpected: this function should only be called on assumptions")
            }
            Self::Assumption(assumption) => {
                if !assumption.func.as_ref()(stack, env) {
                    return Err(limbo_core::LimboError::InternalError(
                        assumption.message.clone(),
                    ));
                }
                Ok(())
            }
            Self::Fault(_) => {
                unreachable!("unexpected: this function should only be called on assumptions")
            }
        }
    }

    pub(crate) fn execute_fault(&self, env: &mut SimulatorEnv, conn_index: usize) -> Result<()> {
        match self {
            Self::Query(_) => {
                unreachable!("unexpected: this function should only be called on faults")
            }
            Self::Assertion(_) => {
                unreachable!("unexpected: this function should only be called on faults")
            }
            Self::Assumption(_) => {
                unreachable!("unexpected: this function should only be called on faults")
            }
            Self::Fault(fault) => {
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

fn property_insert_select<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Property {
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

    // Check that the table exists
    let assumption = Interaction::Assumption(Assertion {
        message: format!("table {} exists", table.name),
        func: Box::new({
            let table_name = table.name.clone();
            move |_: &Vec<ResultSet>, env: &SimulatorEnv| {
                env.tables.iter().any(|t| t.name == table_name)
            }
        }),
    });

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
        func: Box::new(move |stack: &Vec<ResultSet>, _: &SimulatorEnv| {
            let rows = stack.last().unwrap();
            match rows {
                Ok(rows) => rows.iter().any(|r| r == &row),
                Err(_) => false,
            }
        }),
    });

    Property::new(
        Some("select contains inserted value".to_string()),
        vec![assumption, insert_query, select_query, assertion],
    )
}

fn property_double_create_failure<R: rand::Rng>(rng: &mut R, _env: &SimulatorEnv) -> Property {
    let create_query = Create::arbitrary(rng);
    let table_name = create_query.table.name.clone();
    let cq1 = Interaction::Query(Query::Create(create_query.clone()));
    let cq2 = Interaction::Query(Query::Create(create_query.clone()));

    let assertion = Interaction::Assertion(Assertion {
        message:
            "creating two tables with the name should result in a failure for the second query"
                .to_string(),
        func: Box::new(move |stack: &Vec<ResultSet>, _: &SimulatorEnv| {
            let last = stack.last().unwrap();
            match last {
                Ok(_) => false,
                Err(e) => e
                    .to_string()
                    .contains(&format!("Table {table_name} already exists")),
            }
        }),
    });

    Property::new(
        Some("creating the same table twice fails".to_string()),
        vec![cq1, cq2, assertion],
    )
}

fn create_table<R: rand::Rng>(rng: &mut R, _env: &SimulatorEnv) -> Property {
    let create_query = Interaction::Query(Query::Create(Create::arbitrary(rng)));
    Property::anonymous(vec![create_query])
}

fn random_read<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Property {
    let select_query = Interaction::Query(Query::Select(Select::arbitrary_from(rng, &env.tables)));
    Property::anonymous(vec![select_query])
}

fn random_write<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Property {
    let table = pick(&env.tables, rng);
    let insert_query = Interaction::Query(Query::Insert(Insert::arbitrary_from(rng, table)));
    Property::anonymous(vec![insert_query])
}

fn random_fault<R: rand::Rng>(_rng: &mut R, _env: &SimulatorEnv) -> Property {
    let fault = Interaction::Fault(Fault::Disconnect);
    Property::anonymous(vec![fault])
}

impl ArbitraryFrom<(&SimulatorEnv, InteractionStats)> for Property {
    fn arbitrary_from<R: rand::Rng>(
        rng: &mut R,
        (env, stats): &(&SimulatorEnv, InteractionStats),
    ) -> Self {
        let remaining_read = ((env.opts.max_interactions as f64 * env.opts.read_percent / 100.0)
            - (stats.read_count as f64))
            .max(0.0);
        let remaining_write = ((env.opts.max_interactions as f64 * env.opts.write_percent / 100.0)
            - (stats.write_count as f64))
            .max(0.0);
        let remaining_create = ((env.opts.max_interactions as f64 * env.opts.create_percent
            / 100.0)
            - (stats.create_count as f64))
            .max(0.0);

        frequency(
            vec![
                (
                    f64::min(remaining_read, remaining_write),
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
                    remaining_create,
                    Box::new(|rng: &mut R| create_table(rng, env)),
                ),
                (1.0, Box::new(|rng: &mut R| random_fault(rng, env))),
                (
                    remaining_create / 2.0,
                    Box::new(|rng: &mut R| property_double_create_failure(rng, env)),
                ),
            ],
            rng,
        )
    }
}
