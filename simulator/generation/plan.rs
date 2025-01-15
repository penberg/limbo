use std::{fmt::Display, rc::Rc, vec};

use limbo_core::{Connection, Result, StepResult};

use crate::{
    model::{
        query::{Create, Insert, Query, Select},
        table::Value,
    },
    SimConnection, SimulatorEnv,
};

use crate::generation::{frequency, Arbitrary, ArbitraryFrom};

use super::{pick, property::Property};

pub(crate) type ResultSet = Result<Vec<Vec<Value>>>;

#[derive(Clone)]
pub(crate) struct InteractionPlan {
    pub(crate) plan: Vec<Interactions>,
}

pub(crate) struct InteractionPlanState {
    pub(crate) stack: Vec<ResultSet>,
    pub(crate) interaction_pointer: usize,
    pub(crate) secondary_pointer: usize,
}

#[derive(Clone)]
pub(crate) enum Interactions {
    Property(Property),
    Query(Query),
    Fault(Fault),
}

impl Interactions {
    pub(crate) fn name(&self) -> Option<String> {
        match self {
            Interactions::Property(property) => Some(property.name()),
            Interactions::Query(_) => None,
            Interactions::Fault(_) => None,
        }
    }

    pub(crate) fn interactions(&self) -> Vec<Interaction> {
        match self {
            Interactions::Property(property) => property.interactions(),
            Interactions::Query(query) => vec![Interaction::Query(query.clone())],
            Interactions::Fault(fault) => vec![Interaction::Fault(fault.clone())],
        }
    }
}

impl Interactions {
    pub(crate) fn dependencies(&self) -> Vec<String> {
        match self {
            Interactions::Property(property) => {
                property
                    .interactions()
                    .iter()
                    .fold(vec![], |mut acc, i| match i {
                        Interaction::Query(q) => {
                            acc.extend(q.dependencies());
                            acc
                        }
                        _ => acc,
                    })
            }
            Interactions::Query(query) => query.dependencies(),
            Interactions::Fault(_) => vec![],
        }
    }

    pub(crate) fn uses(&self) -> Vec<String> {
        match self {
            Interactions::Property(property) => {
                property
                    .interactions()
                    .iter()
                    .fold(vec![], |mut acc, i| match i {
                        Interaction::Query(q) => {
                            acc.extend(q.uses());
                            acc
                        }
                        _ => acc,
                    })
            }
            Interactions::Query(query) => query.uses(),
            Interactions::Fault(_) => vec![],
        }
    }
}

impl Display for InteractionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for interactions in &self.plan {
            match interactions {
                Interactions::Property(property) => {
                    let name = property.name();
                    writeln!(f, "-- begin testing '{}'", name)?;
                    for interaction in property.interactions() {
                        write!(f, "\t")?;

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
                    writeln!(f, "-- end testing '{}'", name)?;
                }
                Interactions::Fault(fault) => {
                    writeln!(f, "-- FAULT '{}'", fault)?;
                }
                Interactions::Query(query) => {
                    writeln!(f, "{};", query)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
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

enum AssertionAST {
    Pick(),
}

pub(crate) struct Assertion {
    pub(crate) func: Box<AssertionFunc>,
    pub(crate) message: String,
}

#[derive(Debug, Clone)]
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

impl Interactions {
    pub(crate) fn shadow(&self, env: &mut SimulatorEnv) {
        match self {
            Interactions::Property(property) => {
                for interaction in property.interactions() {
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
            Interactions::Query(query) => match query {
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
            Interactions::Fault(_) => {}
        }
    }
}

impl InteractionPlan {
    pub(crate) fn new() -> Self {
        Self { plan: Vec::new() }
    }

    pub(crate) fn stats(&self) -> InteractionStats {
        let mut read = 0;
        let mut write = 0;
        let mut delete = 0;
        let mut create = 0;

        for interactions in &self.plan {
            match interactions {
                Interactions::Property(property) => {
                    for interaction in &property.interactions() {
                        if let Interaction::Query(query) = interaction {
                            match query {
                                Query::Select(_) => read += 1,
                                Query::Insert(_) => write += 1,
                                Query::Delete(_) => delete += 1,
                                Query::Create(_) => create += 1,
                            }
                        }
                    }
                }
                Interactions::Query(query) => match query {
                    Query::Select(_) => read += 1,
                    Query::Insert(_) => write += 1,
                    Query::Delete(_) => delete += 1,
                    Query::Create(_) => create += 1,
                },
                Interactions::Fault(_) => {}
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

impl ArbitraryFrom<&mut SimulatorEnv> for InteractionPlan {
    fn arbitrary_from<R: rand::Rng>(rng: &mut R, env: &mut SimulatorEnv) -> Self {
        let mut plan = InteractionPlan::new();

        let num_interactions = env.opts.max_interactions;

        // First create at least one table
        let create_query = Create::arbitrary(rng);
        env.tables.push(create_query.table.clone());

        plan.plan
            .push(Interactions::Query(Query::Create(create_query)));

        while plan.plan.len() < num_interactions {
            log::debug!(
                "Generating interaction {}/{}",
                plan.plan.len(),
                num_interactions
            );
            let interactions = Interactions::arbitrary_from(rng, (env, plan.stats()));
            interactions.shadow(env);

            plan.plan.push(interactions);
        }

        log::info!("Generated plan with {} interactions", plan.plan.len());
        plan
    }
}

impl Interaction {
    pub(crate) fn execute_query(&self, conn: &mut Rc<Connection>) -> ResultSet {
        if let Self::Query(query) = self {
            let query_str = query.to_string();
            let rows = conn.query(&query_str);
            if rows.is_err() {
                let err = rows.err();
                log::debug!(
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
        } else {
            unreachable!("unexpected: this function should only be called on queries")
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

fn create_table<R: rand::Rng>(rng: &mut R, _env: &SimulatorEnv) -> Interactions {
    Interactions::Query(Query::Create(Create::arbitrary(rng)))
}

fn random_read<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    Interactions::Query(Query::Select(Select::arbitrary_from(rng, &env.tables)))
}

fn random_write<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Interactions {
    let table = pick(&env.tables, rng);
    let insert_query = Query::Insert(Insert::arbitrary_from(rng, table));
    Interactions::Query(insert_query)
}

fn random_fault<R: rand::Rng>(_rng: &mut R, _env: &SimulatorEnv) -> Interactions {
    Interactions::Fault(Fault::Disconnect)
}

impl ArbitraryFrom<(&SimulatorEnv, InteractionStats)> for Interactions {
    fn arbitrary_from<R: rand::Rng>(
        rng: &mut R,
        (env, stats): (&SimulatorEnv, InteractionStats),
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
                    f64::min(remaining_read, remaining_write) + remaining_create,
                    Box::new(|rng: &mut R| {
                        Interactions::Property(Property::arbitrary_from(rng, (env, &stats)))
                    }),
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
            ],
            rng,
        )
    }
}
