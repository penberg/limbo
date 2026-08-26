use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::{Arc, Mutex},
};

use itertools::Itertools;
use similar_asserts::SimpleDiff;
use sql_generation::model::table::SimValue;

use crate::{
    model::interactions::{ConnectionState, InteractionPlanIterator, InteractionPlanState},
    runner::execution::ExecutionContinuation,
};

use super::{
    env::SimulatorEnv,
    execution::{Execution, ExecutionHistory, ExecutionResult},
};

pub fn run_simulation(
    env: Arc<Mutex<SimulatorEnv>>,
    rusqlite_env: Arc<Mutex<SimulatorEnv>>,
    plan: impl InteractionPlanIterator,
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    tracing::info!("Executing database interaction plan...");

    let num_conns = {
        let env = env.lock().unwrap();
        env.connections.len()
    };

    let mut conn_states = (0..num_conns)
        .map(|_| ConnectionState::default())
        .collect::<Vec<_>>();

    let mut rusqlite_states = conn_states.clone();

    let mut state = InteractionPlanState {
        interaction_pointer: 0,
    };

    let result = execute_interactions(
        env,
        rusqlite_env,
        plan,
        &mut state,
        &mut conn_states,
        &mut rusqlite_states,
        last_execution,
    );

    tracing::info!("Simulation completed");

    result
}

pub(crate) fn execute_interactions(
    env: Arc<Mutex<SimulatorEnv>>,
    rusqlite_env: Arc<Mutex<SimulatorEnv>>,
    mut plan: impl InteractionPlanIterator,
    state: &mut InteractionPlanState,
    conn_states: &mut [ConnectionState],
    rusqlite_states: &mut [ConnectionState],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut history = ExecutionHistory::new();

    let mut env = env.lock().unwrap();
    let mut rusqlite_env = rusqlite_env.lock().unwrap();

    env.clear_tables();
    rusqlite_env.clear_tables();

    // Generate from the comparison environment so mode-gated interactions,
    // such as simulated power loss, cannot leak in through the Default clone.
    let mut interaction = plan
        .next(&mut rusqlite_env)
        .expect("we should always have at least 1 interaction to start");

    let now = std::time::Instant::now();

    for _tick in 0..env.opts.ticks {
        let connection_index = interaction.connection_index;
        let turso_conn_state = &mut conn_states[connection_index];
        let rusqlite_conn_state = &mut rusqlite_states[connection_index];

        history
            .history
            .push(Execution::new(connection_index, state.interaction_pointer));
        let mut last_execution = last_execution.lock().unwrap();
        last_execution.connection_index = connection_index;
        last_execution.interaction_index = state.interaction_pointer;

        // first execute turso
        let turso_res = super::execution::execute_plan(&mut env, &interaction, turso_conn_state);

        // second execute rusqlite
        let rusqlite_res =
            super::execution::execute_plan(&mut rusqlite_env, &interaction, rusqlite_conn_state);

        // Compare results
        let next = match compare_results(
            turso_res,
            turso_conn_state,
            rusqlite_res,
            rusqlite_conn_state,
        ) {
            Ok(next) => next,
            Err(err) => return ExecutionResult::new(history, Some(err)),
        };

        match next {
            ExecutionContinuation::Stay => {}
            ExecutionContinuation::NextInteractionOutsideThisProperty => {
                // Skip remaining interactions in this property by advancing until we
                // find an interaction with a different id (i.e., a different property)
                let current_property_id = interaction.id();
                loop {
                    state.interaction_pointer += 1;
                    let Some(new_interaction) = plan.next(&mut rusqlite_env) else {
                        // No more interactions, we're done
                        return ExecutionResult::new(history, None);
                    };
                    if new_interaction.id() != current_property_id {
                        interaction = new_interaction;
                        break;
                    }
                }
            }
            ExecutionContinuation::NextInteraction => {
                state.interaction_pointer += 1;
                let Some(new_interaction) = plan.next(&mut rusqlite_env) else {
                    break;
                };
                interaction = new_interaction;
            }
        }

        // Check if the maximum time for the simulation has been reached
        if now.elapsed().as_secs() >= env.opts.max_time_simulation as u64 {
            return ExecutionResult::new(
                history,
                Some(turso_core::LimboError::InternalError(
                    "maximum time for simulation reached".into(),
                )),
            );
        }
    }

    ExecutionResult::new(history, None)
}

fn compare_results(
    turso_res: turso_core::Result<ExecutionContinuation>,
    turso_conn_state: &mut ConnectionState,
    rusqlite_res: turso_core::Result<ExecutionContinuation>,
    rusqlite_conn_state: &mut ConnectionState,
) -> turso_core::Result<ExecutionContinuation> {
    let next = match (turso_res, rusqlite_res) {
        (Ok(v1), Ok(v2)) => {
            assert_eq!(v1, v2);
            let turso_values = turso_conn_state.stack.last();
            let rusqlite_values = rusqlite_conn_state.stack.last();
            match (turso_values, rusqlite_values) {
                (Some(turso_values), Some(rusqlite_values)) => {
                    match (turso_values, rusqlite_values) {
                        (Ok(turso_values), Ok(rusqlite_values)) => {
                            if !compare_order_insensitive(turso_values, rusqlite_values) {
                                tracing::error!(
                                    "returned values from limbo and rusqlite results do not match"
                                );

                                fn val_to_string(sim_val: &SimValue) -> String {
                                    match &sim_val.0 {
                                        turso_core::Value::Blob(blob) => {
                                            let convert_blob = || -> anyhow::Result<String> {
                                                let val = String::from_utf8(blob.clone())?;
                                                Ok(val)
                                            };

                                            convert_blob().unwrap_or_else(|_| sim_val.to_string())
                                        }
                                        _ => sim_val.to_string(),
                                    }
                                }

                                let turso_string_values: Vec<Vec<_>> = turso_values
                                    .iter()
                                    .map(|rows| rows.iter().map(val_to_string).collect())
                                    .sorted()
                                    .collect();

                                let rusqlite_string_values: Vec<Vec<_>> = rusqlite_values
                                    .iter()
                                    .map(|rows| rows.iter().map(val_to_string).collect())
                                    .sorted()
                                    .collect();

                                let turso_string = format!("{turso_string_values:#?}");
                                let rusqlite_string = format!("{rusqlite_string_values:#?}");
                                let diff = SimpleDiff::from_str(
                                    &turso_string,
                                    &rusqlite_string,
                                    "turso",
                                    "rusqlite",
                                );
                                tracing::error!(%diff);

                                return Err(turso_core::LimboError::InternalError(
                                    "returned values from limbo and rusqlite results do not match"
                                        .into(),
                                ));
                            }
                        }
                        (Err(turso_err), Err(rusqlite_err)) => {
                            tracing::warn!("limbo and rusqlite both fail, requires manual check");
                            tracing::warn!("limbo error {}", turso_err);
                            tracing::warn!("rusqlite error {}", rusqlite_err);
                        }
                        (Ok(turso_err), Err(rusqlite_err)) => {
                            tracing::error!(
                                "limbo and rusqlite results do not match, limbo returned values but rusqlite failed"
                            );
                            tracing::error!("limbo values {:?}", turso_err);
                            tracing::error!("rusqlite error {}", rusqlite_err);
                            return Err(turso_core::LimboError::InternalError(
                                "limbo and rusqlite results do not match".into(),
                            ));
                        }
                        (Err(turso_err), Ok(_)) => {
                            tracing::error!(
                                "limbo and rusqlite results do not match, limbo failed but rusqlite returned values"
                            );
                            tracing::error!("limbo error {}", turso_err);
                            return Err(turso_core::LimboError::InternalError(
                                "limbo and rusqlite results do not match".into(),
                            ));
                        }
                    }
                    v1
                }
                (None, None) => v1,
                _ => {
                    tracing::error!("limbo and rusqlite results do not match");
                    return Err(turso_core::LimboError::InternalError(
                        "limbo and rusqlite results do not match".into(),
                    ));
                }
            }
        }
        (Err(err), Ok(_)) => {
            tracing::error!("limbo and rusqlite results do not match");
            tracing::error!("limbo error {}", err);
            return Err(err);
        }
        (Ok(val), Err(err)) => {
            tracing::error!("limbo and rusqlite results do not match");
            tracing::error!("limbo {:?}", val);
            tracing::error!("rusqlite error {}", err);
            return Err(err);
        }
        (Err(err), Err(err_rusqlite)) => {
            tracing::error!("limbo and rusqlite both fail, requires manual check");
            tracing::error!("limbo error {}", err);
            tracing::error!("rusqlite error {}", err_rusqlite);
            // todo: Previously, we returned an error here, but now we just log it.
            //       The problem is that the errors might be different, and we cannot
            //       just assume both of them being errors has the same semantics.
            // return Err(err);
            ExecutionContinuation::NextInteraction
        }
    };
    Ok(next)
}

fn count_rows(values: &[Vec<SimValue>]) -> BTreeMap<&Vec<SimValue>, i32> {
    let mut counter = BTreeMap::new();
    for row in values.iter() {
        match counter.entry(row) {
            Entry::Vacant(entry) => {
                entry.insert(1);
            }
            Entry::Occupied(mut entry) => {
                let counter = entry.get_mut();

                *counter += 1;
            }
        }
    }
    counter
}

fn compare_order_insensitive(
    turso_values: &[Vec<SimValue>],
    rusqlite_values: &[Vec<SimValue>],
) -> bool {
    if turso_values.len() != rusqlite_values.len() {
        return false;
    }

    let turso_counter = count_rows(turso_values);
    let rusqlite_counter = count_rows(rusqlite_values);

    turso_counter == rusqlite_counter
}
