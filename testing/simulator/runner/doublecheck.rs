use std::{
    fs,
    sync::{Arc, Mutex},
};

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
    doublecheck_env: Arc<Mutex<SimulatorEnv>>,
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

    let mut doublecheck_states = conn_states.clone();

    let mut state = InteractionPlanState {
        interaction_pointer: 0,
    };

    let mut result = execute_plans(
        env.clone(),
        doublecheck_env.clone(),
        plan,
        &mut state,
        &mut conn_states,
        &mut doublecheck_states,
        last_execution,
    );

    {
        env.clear_poison();
        let env = env.lock().unwrap();

        doublecheck_env.clear_poison();
        let doublecheck_env = doublecheck_env.lock().unwrap();

        // Check if the database files are the same
        let db = fs::read(env.get_db_path()).expect("should be able to read default database file");
        let doublecheck_db = fs::read(doublecheck_env.get_db_path())
            .expect("should be able to read doublecheck database file");

        if db != doublecheck_db {
            tracing::error!("Database files are different, check binary diffs for more details.");
            tracing::debug!("Default database path: {}", env.get_db_path().display());
            tracing::debug!(
                "Doublecheck database path: {}",
                doublecheck_env.get_db_path().display()
            );
            result.error = result.error.or_else(|| {
                Some(turso_core::LimboError::InternalError(
                    "database files are different, check binary diffs for more details.".into(),
                ))
            });
        }
    }

    tracing::info!("Simulation completed");

    result
}

pub(crate) fn execute_plans(
    env: Arc<Mutex<SimulatorEnv>>,
    doublecheck_env: Arc<Mutex<SimulatorEnv>>,
    mut plan: impl InteractionPlanIterator,
    state: &mut InteractionPlanState,
    conn_states: &mut [ConnectionState],
    doublecheck_states: &mut [ConnectionState],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut history = ExecutionHistory::new();

    let mut env = env.lock().unwrap();
    let mut doublecheck_env = doublecheck_env.lock().unwrap();

    env.clear_tables();
    doublecheck_env.clear_tables();

    // Generate from the comparison environment so mode-gated interactions,
    // such as simulated power loss, cannot leak in through the Default clone.
    let mut interaction = plan
        .next(&mut doublecheck_env)
        .expect("we should always have at least 1 interaction to start");

    let now = std::time::Instant::now();

    for _tick in 0..env.opts.ticks {
        let connection_index = interaction.connection_index;
        let turso_conn_state = &mut conn_states[connection_index];
        let doublecheck_conn_state = &mut doublecheck_states[connection_index];

        history
            .history
            .push(Execution::new(connection_index, state.interaction_pointer));
        let mut last_execution = last_execution.lock().unwrap();
        last_execution.connection_index = connection_index;
        last_execution.interaction_index = state.interaction_pointer;

        // first execute turso
        let turso_res = super::execution::execute_plan(&mut env, &interaction, turso_conn_state);

        // second execute doublecheck
        let doublecheck_res = super::execution::execute_plan(
            &mut doublecheck_env,
            &interaction,
            doublecheck_conn_state,
        );

        // Compare results
        let next = match compare_results(
            turso_res,
            turso_conn_state,
            doublecheck_res,
            doublecheck_conn_state,
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
                    let Some(new_interaction) = plan.next(&mut doublecheck_env) else {
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
                let Some(new_interaction) = plan.next(&mut doublecheck_env) else {
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
    turso_state: &mut ConnectionState,
    doublecheck_res: turso_core::Result<ExecutionContinuation>,
    doublecheck_state: &mut ConnectionState,
) -> turso_core::Result<ExecutionContinuation> {
    let next = match (turso_res, doublecheck_res) {
        (Ok(v1), Ok(v2)) => {
            assert_eq!(v1, v2);
            let turso_values = turso_state.stack.last();
            let doublecheck_values = doublecheck_state.stack.last();
            match (turso_values, doublecheck_values) {
                (Some(limbo_values), Some(doublecheck_values)) => {
                    match (limbo_values, doublecheck_values) {
                        (Ok(limbo_values), Ok(doublecheck_values)) => {
                            if limbo_values != doublecheck_values {
                                tracing::error!(
                                    "returned values from limbo and doublecheck results do not match"
                                );
                                tracing::debug!("limbo values {:?}", limbo_values);
                                tracing::debug!("doublecheck values {:?}", doublecheck_values);
                                return Err(turso_core::LimboError::InternalError(
                                            "returned values from limbo and doublecheck results do not match".into(),
                                        ));
                            }
                            v1
                        }
                        (Err(limbo_err), Err(doublecheck_err)) => {
                            if limbo_err.to_string() != doublecheck_err.to_string() {
                                tracing::error!("limbo and doublecheck errors do not match");
                                tracing::error!("limbo error {}", limbo_err);
                                tracing::error!("doublecheck error {}", doublecheck_err);
                                return Err(turso_core::LimboError::InternalError(
                                    "limbo and doublecheck errors do not match".into(),
                                ));
                            }
                            v1
                        }
                        (Ok(limbo_result), Err(doublecheck_err)) => {
                            tracing::error!(
                                "limbo and doublecheck results do not match, limbo returned values but doublecheck failed"
                            );
                            tracing::error!("limbo values {:?}", limbo_result);
                            tracing::error!("doublecheck error {}", doublecheck_err);
                            return Err(turso_core::LimboError::InternalError(
                                "limbo and doublecheck results do not match".into(),
                            ));
                        }
                        (Err(limbo_err), Ok(_)) => {
                            tracing::error!(
                                "limbo and doublecheck results do not match, limbo failed but doublecheck returned values"
                            );
                            tracing::error!("limbo error {}", limbo_err);
                            return Err(turso_core::LimboError::InternalError(
                                "limbo and doublecheck results do not match".into(),
                            ));
                        }
                    }
                }
                (None, None) => v1,
                _ => {
                    tracing::error!("limbo and doublecheck results do not match");
                    return Err(turso_core::LimboError::InternalError(
                        "limbo and doublecheck results do not match".into(),
                    ));
                }
            }
        }
        (Err(err), Ok(_)) => {
            tracing::error!("limbo and doublecheck results do not match");
            tracing::error!("limbo error {}", err);
            return Err(err);
        }
        (Ok(val), Err(err)) => {
            tracing::error!("limbo and doublecheck results do not match");
            tracing::error!("limbo {:?}", val);
            tracing::error!("doublecheck error {}", err);
            return Err(err);
        }
        (Err(err), Err(err_doublecheck)) => {
            if err.to_string() != err_doublecheck.to_string() {
                tracing::error!("limbo and doublecheck errors do not match");
                tracing::error!("limbo error {}", err);
                tracing::error!("doublecheck error {}", err_doublecheck);
                return Err(turso_core::LimboError::InternalError(
                    "limbo and doublecheck errors do not match".into(),
                ));
            }
            ExecutionContinuation::NextInteraction
        }
    };
    Ok(next)
}
