use clap::Parser;
use generation::plan::{Interaction, InteractionPlan, ResultSet};
use generation::{pick_index, ArbitraryFrom};
use limbo_core::{Connection, Database, Result, StepResult, IO};
use model::table::Value;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use runner::cli::SimulatorCLI;
use runner::env::{SimConnection, SimulatorEnv, SimulatorOpts};
use runner::io::SimulatorIO;
use std::backtrace::Backtrace;
use std::io::Write;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::TempDir;

mod generation;
mod model;
mod runner;

#[allow(clippy::arc_with_non_send_sync)]
fn main() {
    let _ = env_logger::try_init();

    let cli_opts = SimulatorCLI::parse();

    let seed = match cli_opts.seed {
        Some(seed) => seed,
        None => rand::thread_rng().next_u64(),
    };

    let output_dir = match &cli_opts.output_dir {
        Some(dir) => Path::new(dir).to_path_buf(),
        None => TempDir::new().unwrap().into_path(),
    };

    let db_path = output_dir.join("simulator.db");
    let plan_path = output_dir.join("simulator.plan");

    // Print the seed, the locations of the database and the plan file
    log::info!("database path: {:?}", db_path);
    log::info!("simulator plan path: {:?}", plan_path);
    log::info!("seed: {}", seed);

    std::panic::set_hook(Box::new(move |info| {
        log::error!("panic occurred");

        let payload = info.payload();
        if let Some(s) = payload.downcast_ref::<&str>() {
            log::error!("{}", s);
        } else if let Some(s) = payload.downcast_ref::<String>() {
            log::error!("{}", s);
        } else {
            log::error!("unknown panic payload");
        }

        let bt = Backtrace::force_capture();
        log::error!("captured backtrace:\n{}", bt);
    }));

    let result = std::panic::catch_unwind(|| run_simulation(seed, &cli_opts, &db_path, &plan_path));

    if cli_opts.doublecheck {
        // Move the old database and plan file to a new location
        let old_db_path = db_path.with_extension("_old.db");
        let old_plan_path = plan_path.with_extension("_old.plan");

        std::fs::rename(&db_path, &old_db_path).unwrap();
        std::fs::rename(&plan_path, &old_plan_path).unwrap();

        // Run the simulation again
        let result2 =
            std::panic::catch_unwind(|| run_simulation(seed, &cli_opts, &db_path, &plan_path));

        match (result, result2) {
            (Ok(Ok(_)), Err(_)) => {
                log::error!("doublecheck failed! first run succeeded, but second run panicked.");
            }
            (Ok(Err(_)), Err(_)) => {
                log::error!(
                    "doublecheck failed! first run failed assertion, but second run panicked."
                );
            }
            (Err(_), Ok(Ok(_))) => {
                log::error!("doublecheck failed! first run panicked, but second run succeeded.");
            }
            (Err(_), Ok(Err(_))) => {
                log::error!(
                    "doublecheck failed! first run panicked, but second run failed assertion."
                );
            }
            (Ok(Ok(_)), Ok(Err(_))) => {
                log::error!(
                    "doublecheck failed! first run succeeded, but second run failed assertion."
                );
            }
            (Ok(Err(_)), Ok(Ok(_))) => {
                log::error!(
                    "doublecheck failed! first run failed assertion, but second run succeeded."
                );
            }
            (Err(_), Err(_)) | (Ok(_), Ok(_)) => {
                // Compare the two database files byte by byte
                let old_db = std::fs::read(&old_db_path).unwrap();
                let new_db = std::fs::read(&db_path).unwrap();
                if old_db != new_db {
                    log::error!("doublecheck failed! database files are different.");
                } else {
                    log::info!("doublecheck succeeded! database files are the same.");
                }
            }
        }

        // Move the new database and plan file to a new location
        let new_db_path = db_path.with_extension("_double.db");
        let new_plan_path = plan_path.with_extension("_double.plan");

        std::fs::rename(&db_path, &new_db_path).unwrap();
        std::fs::rename(&plan_path, &new_plan_path).unwrap();

        // Move the old database and plan file back
        std::fs::rename(&old_db_path, &db_path).unwrap();
        std::fs::rename(&old_plan_path, &plan_path).unwrap();
    }
    // Print the seed, the locations of the database and the plan file at the end again for easily accessing them.
    println!("database path: {:?}", db_path);
    println!("simulator plan path: {:?}", plan_path);
    println!("seed: {}", seed);
}

fn run_simulation(
    seed: u64,
    cli_opts: &SimulatorCLI,
    db_path: &Path,
    plan_path: &Path,
) -> Result<()> {
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

    if cli_opts.maximum_size < 1 {
        panic!("maximum size must be at least 1");
    }

    let opts = SimulatorOpts {
        ticks: rng.gen_range(1..=cli_opts.maximum_size),
        max_connections: 1, // TODO: for now let's use one connection as we didn't implement
        // correct transactions procesing
        max_tables: rng.gen_range(0..128),
        read_percent,
        write_percent,
        delete_percent,
        page_size: 4096, // TODO: randomize this too
        max_interactions: rng.gen_range(1..=cli_opts.maximum_size),
    };
    let io = Arc::new(SimulatorIO::new(seed, opts.page_size).unwrap());

    let db = match Database::open_file(io.clone(), db_path.to_str().unwrap()) {
        Ok(db) => db,
        Err(e) => {
            panic!("error opening simulator test file {:?}: {:?}", db_path, e);
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

    log::info!("Generating database interaction plan...");
    let mut plans = (1..=env.opts.max_connections)
        .map(|_| InteractionPlan::arbitrary_from(&mut env.rng.clone(), &env))
        .collect::<Vec<_>>();

    let mut f = std::fs::File::create(plan_path).unwrap();
    // todo: create a detailed plan file with all the plans. for now, we only use 1 connection, so it's safe to use the first plan.
    f.write(plans[0].to_string().as_bytes()).unwrap();

    log::info!("{}", plans[0].stats());

    log::info!("Executing database interaction plan...");

    let result = execute_plans(&mut env, &mut plans);
    if result.is_err() {
        log::error!("error executing plans: {:?}", result.as_ref().err());
    }

    env.io.print_stats();

    log::info!("Simulation completed");

    result
}

fn execute_plans(env: &mut SimulatorEnv, plans: &mut Vec<InteractionPlan>) -> Result<()> {
    // todo: add history here by recording which interaction was executed at which tick
    for _tick in 0..env.opts.ticks {
        // Pick the connection to interact with
        let connection_index = pick_index(env.connections.len(), &mut env.rng);
        // Execute the interaction for the selected connection
        execute_plan(env, connection_index, plans)?;
    }

    Ok(())
}

fn execute_plan(
    env: &mut SimulatorEnv,
    connection_index: usize,
    plans: &mut Vec<InteractionPlan>,
) -> Result<()> {
    let connection = &env.connections[connection_index];
    let plan = &mut plans[connection_index];

    if plan.interaction_pointer >= plan.plan.len() {
        return Ok(());
    }

    let interaction = &plan.plan[plan.interaction_pointer];

    if let SimConnection::Disconnected = connection {
        log::info!("connecting {}", connection_index);
        env.connections[connection_index] = SimConnection::Connected(env.db.connect());
    } else {
        match execute_interaction(env, connection_index, interaction, &mut plan.stack) {
            Ok(_) => {
                log::debug!("connection {} processed", connection_index);
                plan.interaction_pointer += 1;
            }
            Err(err) => {
                log::error!("error {}", err);
                return Err(err);
            }
        }
    }

    Ok(())
}

fn execute_interaction(
    env: &mut SimulatorEnv,
    connection_index: usize,
    interaction: &Interaction,
    stack: &mut Vec<ResultSet>,
) -> Result<()> {
    log::info!("executing: {}", interaction);
    match interaction {
        generation::plan::Interaction::Query(_) => {
            let conn = match &mut env.connections[connection_index] {
                SimConnection::Connected(conn) => conn,
                SimConnection::Disconnected => unreachable!(),
            };

            log::debug!("{}", interaction);
            let results = interaction.execute_query(conn)?;
            log::debug!("{:?}", results);
            stack.push(results);
        }
        generation::plan::Interaction::Assertion(_) => {
            interaction.execute_assertion(stack)?;
            stack.clear();
        }
        Interaction::Fault(_) => {
            interaction.execute_fault(env, connection_index)?;
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
