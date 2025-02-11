use std::sync::{Arc, Mutex};

use crate::{
    generation::{
        pick_index,
        plan::{Interaction, InteractionPlanState, ResultSet},
    },
    model::{
        query::Query,
        table::{Table, Value},
    },
    runner::execution::ExecutionContinuation,
    InteractionPlan,
};

use super::{
    env::{ConnectionTrait, SimConnection, SimulatorEnv, SimulatorEnvTrait},
    execution::{execute_interaction, Execution, ExecutionHistory, ExecutionResult},
};

pub(crate) struct SimulatorEnvRusqlite {
    pub(crate) tables: Vec<Table>,
    pub(crate) connections: Vec<RusqliteConnection>,
}

pub(crate) enum RusqliteConnection {
    Connected(rusqlite::Connection),
    Disconnected,
}

impl ConnectionTrait for RusqliteConnection {
    fn is_connected(&self) -> bool {
        match self {
            RusqliteConnection::Connected(_) => true,
            RusqliteConnection::Disconnected => false,
        }
    }

    fn disconnect(&mut self) {
        *self = RusqliteConnection::Disconnected;
    }
}

impl SimulatorEnvTrait for SimulatorEnvRusqlite {
    fn tables(&self) -> &Vec<Table> {
        &self.tables
    }

    fn tables_mut(&mut self) -> &mut Vec<Table> {
        &mut self.tables
    }
}

pub(crate) fn run_simulation(
    env: Arc<Mutex<SimulatorEnv>>,
    plans: &mut [InteractionPlan],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    log::info!("Executing database interaction plan...");

    let mut states = plans
        .iter()
        .map(|_| InteractionPlanState {
            stack: vec![],
            interaction_pointer: 0,
            secondary_pointer: 0,
        })
        .collect::<Vec<_>>();
    let env = env.lock().unwrap();

    let rusqlite_env = SimulatorEnvRusqlite {
        tables: env.tables.clone(),
        connections: (0..env.connections.len())
            .map(|_| RusqliteConnection::Connected(rusqlite::Connection::open_in_memory().unwrap()))
            .collect::<Vec<_>>(),
    };
    let mut rusqlite_states = plans
        .iter()
        .map(|_| InteractionPlanState {
            stack: vec![],
            interaction_pointer: 0,
            secondary_pointer: 0,
        })
        .collect::<Vec<_>>();

    let result = execute_plans(
        Arc::new(Mutex::new(env.clone())),
        rusqlite_env,
        plans,
        &mut states,
        &mut rusqlite_states,
        last_execution,
    );

    env.io.print_stats();

    log::info!("Simulation completed");

    result
}

fn execute_query_rusqlite(
    connection: &rusqlite::Connection,
    query: &Query,
) -> rusqlite::Result<Vec<Vec<Value>>> {
    match query {
        Query::Create(create) => {
            connection.execute(create.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Select(select) => {
            let mut stmt = connection.prepare(select.to_string().as_str())?;
            let columns = stmt.column_count();
            let rows = stmt.query_map([], |row| {
                let mut values = vec![];
                for i in 0..columns {
                    let value = row.get_unwrap(i);
                    match value {
                        rusqlite::types::Value::Null => values.push(Value::Null),
                        rusqlite::types::Value::Integer(i) => values.push(Value::Integer(i)),
                        rusqlite::types::Value::Real(f) => values.push(Value::Float(f)),
                        rusqlite::types::Value::Text(s) => values.push(Value::Text(s)),
                        rusqlite::types::Value::Blob(b) => values.push(Value::Blob(b)),
                    }
                }
                Ok(values)
            })?;
            let mut result = vec![];
            for row in rows {
                result.push(row?);
            }
            Ok(result)
        }
        Query::Insert(insert) => {
            connection.execute(insert.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Delete(delete) => {
            connection.execute(delete.to_string().as_str(), ())?;
            Ok(vec![])
        }
        Query::Drop(drop) => {
            connection.execute(drop.to_string().as_str(), ())?;
            Ok(vec![])
        }
    }
}

pub(crate) fn execute_plans(
    env: Arc<Mutex<SimulatorEnv>>,
    mut rusqlite_env: SimulatorEnvRusqlite,
    plans: &mut [InteractionPlan],
    states: &mut [InteractionPlanState],
    rusqlite_states: &mut [InteractionPlanState],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut history = ExecutionHistory::new();
    let now = std::time::Instant::now();

    let mut env = env.lock().unwrap();
    for _tick in 0..env.opts.ticks {
        // Pick the connection to interact with
        let connection_index = pick_index(env.connections.len(), &mut env.rng);
        let state = &mut states[connection_index];

        history.history.push(Execution::new(
            connection_index,
            state.interaction_pointer,
            state.secondary_pointer,
        ));
        let mut last_execution = last_execution.lock().unwrap();
        last_execution.connection_index = connection_index;
        last_execution.interaction_index = state.interaction_pointer;
        last_execution.secondary_index = state.secondary_pointer;
        // Execute the interaction for the selected connection
        match execute_plan(
            &mut env,
            &mut rusqlite_env,
            connection_index,
            plans,
            states,
            rusqlite_states,
        ) {
            Ok(_) => {}
            Err(err) => {
                return ExecutionResult::new(history, Some(err));
            }
        }
        // Check if the maximum time for the simulation has been reached
        if now.elapsed().as_secs() >= env.opts.max_time_simulation as u64 {
            return ExecutionResult::new(
                history,
                Some(limbo_core::LimboError::InternalError(
                    "maximum time for simulation reached".into(),
                )),
            );
        }
    }

    ExecutionResult::new(history, None)
}

fn execute_plan(
    env: &mut SimulatorEnv,
    rusqlite_env: &mut SimulatorEnvRusqlite,
    connection_index: usize,
    plans: &mut [InteractionPlan],
    states: &mut [InteractionPlanState],
    rusqlite_states: &mut [InteractionPlanState],
) -> limbo_core::Result<()> {
    let connection = &env.connections[connection_index];
    let plan = &mut plans[connection_index];
    let state = &mut states[connection_index];
    let rusqlite_state = &mut rusqlite_states[connection_index];
    if state.interaction_pointer >= plan.plan.len() {
        return Ok(());
    }

    let interaction = &plan.plan[state.interaction_pointer].interactions()[state.secondary_pointer];

    if let SimConnection::Disconnected = connection {
        log::info!("connecting {}", connection_index);
        env.connections[connection_index] = SimConnection::Connected(env.db.connect());
    } else {
        let limbo_result =
            execute_interaction(env, connection_index, interaction, &mut state.stack);
        let ruqlite_result = execute_interaction_rusqlite(
            rusqlite_env,
            connection_index,
            interaction,
            &mut rusqlite_state.stack,
        );

        match (limbo_result, ruqlite_result) {
            (Ok(next_execution), Ok(next_execution_rusqlite)) => {
                if next_execution != next_execution_rusqlite {
                    log::error!("limbo and rusqlite results do not match");
                    return Err(limbo_core::LimboError::InternalError(
                        "limbo and rusqlite results do not match".into(),
                    ));
                }
                log::debug!("connection {} processed", connection_index);
                // Move to the next interaction or property
                match next_execution {
                    ExecutionContinuation::NextInteraction => {
                        if state.secondary_pointer + 1
                            >= plan.plan[state.interaction_pointer].interactions().len()
                        {
                            // If we have reached the end of the interactions for this property, move to the next property
                            state.interaction_pointer += 1;
                            state.secondary_pointer = 0;
                        } else {
                            // Otherwise, move to the next interaction
                            state.secondary_pointer += 1;
                        }
                    }
                    ExecutionContinuation::NextProperty => {
                        // Skip to the next property
                        state.interaction_pointer += 1;
                        state.secondary_pointer = 0;
                    }
                }
            }
            (Err(err), Ok(_)) => {
                log::error!("limbo and rusqlite results do not match");
                log::error!("limbo error {}", err);
                return Err(err);
            }
            (Ok(_), Err(err)) => {
                log::error!("limbo and rusqlite results do not match");
                log::error!("rusqlite error {}", err);
                return Err(err);
            }
            (Err(err), Err(err_rusqlite)) => {
                log::error!("limbo and rusqlite both fail, requires manual check");
                log::error!("limbo error {}", err);
                log::error!("rusqlite error {}", err_rusqlite);
                return Err(err);
            }
        }
    }

    Ok(())
}

fn execute_interaction_rusqlite(
    env: &mut SimulatorEnvRusqlite,
    connection_index: usize,
    interaction: &Interaction,
    stack: &mut Vec<ResultSet>,
) -> limbo_core::Result<ExecutionContinuation> {
    log::info!("executing in rusqlite: {}", interaction);
    match interaction {
        Interaction::Query(query) => {
            let conn = match &mut env.connections[connection_index] {
                RusqliteConnection::Connected(conn) => conn,
                RusqliteConnection::Disconnected => unreachable!(),
            };

            log::debug!("{}", interaction);
            let results = execute_query_rusqlite(conn, query).map_err(|e| {
                limbo_core::LimboError::InternalError(format!("error executing query: {}", e))
            });
            log::debug!("{:?}", results);
            stack.push(results);
        }
        Interaction::Assertion(_) => {
            interaction.execute_assertion(stack, env)?;
            stack.clear();
        }
        Interaction::Assumption(_) => {
            let assumption_result = interaction.execute_assumption(stack, env);
            stack.clear();

            if assumption_result.is_err() {
                log::warn!("assumption failed: {:?}", assumption_result);
                return Ok(ExecutionContinuation::NextProperty);
            }
        }
        Interaction::Fault(_) => {
            log::debug!("faults are not supported in differential testing mode");
        }
    }

    Ok(ExecutionContinuation::NextInteraction)
}
