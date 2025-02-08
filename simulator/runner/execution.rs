use std::sync::{Arc, Mutex};

use limbo_core::{LimboError, Result};

use crate::generation::{
    pick_index,
    plan::{Interaction, InteractionPlan, InteractionPlanState, ResultSet},
};

use super::env::{SimConnection, SimulatorEnv};

#[derive(Debug, Clone, Copy)]
pub(crate) struct Execution {
    pub(crate) connection_index: usize,
    pub(crate) interaction_index: usize,
    pub(crate) secondary_index: usize,
}

impl Execution {
    pub(crate) fn new(
        connection_index: usize,
        interaction_index: usize,
        secondary_index: usize,
    ) -> Self {
        Self {
            connection_index,
            interaction_index,
            secondary_index,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ExecutionHistory {
    pub(crate) history: Vec<Execution>,
}

impl ExecutionHistory {
    pub(crate) fn new() -> Self {
        Self {
            history: Vec::new(),
        }
    }
}

pub(crate) struct ExecutionResult {
    pub(crate) history: ExecutionHistory,
    pub(crate) error: Option<LimboError>,
}

impl ExecutionResult {
    pub(crate) fn new(history: ExecutionHistory, error: Option<LimboError>) -> Self {
        Self { history, error }
    }
}

pub(crate) fn execute_plans(
    env: Arc<Mutex<SimulatorEnv>>,
    plans: &mut [InteractionPlan],
    states: &mut [InteractionPlanState],
    last_execution: Arc<Mutex<Execution>>,
) -> ExecutionResult {
    let mut history = ExecutionHistory::new();
    let now = std::time::Instant::now();
    env.clear_poison();
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
        match execute_plan(&mut env, connection_index, plans, states) {
            Ok(_) => {}
            Err(err) => {
                return ExecutionResult::new(history, Some(err));
            }
        }
        // Check if the maximum time for the simulation has been reached
        if now.elapsed().as_secs() >= env.opts.max_time_simulation as u64 {
            return ExecutionResult::new(
                history,
                Some(LimboError::InternalError(
                    "maximum time for simulation reached".into(),
                )),
            );
        }
    }

    ExecutionResult::new(history, None)
}

fn execute_plan(
    env: &mut SimulatorEnv,
    connection_index: usize,
    plans: &mut [InteractionPlan],
    states: &mut [InteractionPlanState],
) -> Result<()> {
    let connection = &env.connections[connection_index];
    let plan = &mut plans[connection_index];
    let state = &mut states[connection_index];

    if state.interaction_pointer >= plan.plan.len() {
        return Ok(());
    }

    let interaction = &plan.plan[state.interaction_pointer].interactions()[state.secondary_pointer];

    if let SimConnection::Disconnected = connection {
        log::info!("connecting {}", connection_index);
        env.connections[connection_index] = SimConnection::Connected(env.db.connect());
    } else {
        match execute_interaction(env, connection_index, interaction, &mut state.stack) {
            Ok(next_execution) => {
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
            Err(err) => {
                log::error!("error {}", err);
                return Err(err);
            }
        }
    }

    Ok(())
}

/// The next point of control flow after executing an interaction.
/// `execute_interaction` uses this type in conjunction with a result, where
/// the `Err` case indicates a full-stop due to a bug, and the `Ok` case
/// indicates the next step in the plan.
pub(crate) enum ExecutionContinuation {
    /// Default continuation, execute the next interaction.
    NextInteraction,
    /// Typically used in the case of preconditions failures, skip to the next property.
    NextProperty,
}

pub(crate) fn execute_interaction(
    env: &mut SimulatorEnv,
    connection_index: usize,
    interaction: &Interaction,
    stack: &mut Vec<ResultSet>,
) -> Result<ExecutionContinuation> {
    log::info!("executing: {}", interaction);
    match interaction {
        Interaction::Query(_) => {
            let conn = match &mut env.connections[connection_index] {
                SimConnection::Connected(conn) => conn,
                SimConnection::Disconnected => unreachable!(),
            };

            log::debug!("{}", interaction);
            let results = interaction.execute_query(conn);
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
            interaction.execute_fault(env, connection_index)?;
        }
    }

    Ok(ExecutionContinuation::NextInteraction)
}
