#![allow(clippy::arc_with_non_send_sync, dead_code)]
use clap::Parser;
use core::panic;
use generation::plan::{InteractionPlan, InteractionPlanState};
use generation::ArbitraryFrom;
use limbo_core::Database;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use runner::cli::SimulatorCLI;
use runner::env::{SimConnection, SimulatorEnv, SimulatorOpts};
use runner::execution::{execute_plans, Execution, ExecutionHistory, ExecutionResult};
use runner::io::SimulatorIO;
use std::any::Any;
use std::backtrace::Backtrace;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

mod generation;
mod model;
mod runner;
mod shrink;

struct Paths {
    db: PathBuf,
    plan: PathBuf,
    shrunk_plan: PathBuf,
    history: PathBuf,
    doublecheck_db: PathBuf,
    shrunk_db: PathBuf,
}

impl Paths {
    fn new(output_dir: &Path, shrink: bool, doublecheck: bool) -> Self {
        let paths = Paths {
            db: PathBuf::from(output_dir).join("simulator.db"),
            plan: PathBuf::from(output_dir).join("simulator.plan"),
            shrunk_plan: PathBuf::from(output_dir).join("simulator_shrunk.plan"),
            history: PathBuf::from(output_dir).join("simulator.history"),
            doublecheck_db: PathBuf::from(output_dir).join("simulator_double.db"),
            shrunk_db: PathBuf::from(output_dir).join("simulator_shrunk.db"),
        };

        // Print the seed, the locations of the database and the plan file
        log::info!("database path: {:?}", paths.db);
        if doublecheck {
            log::info!("doublecheck database path: {:?}", paths.doublecheck_db);
        } else if shrink {
            log::info!("shrunk database path: {:?}", paths.shrunk_db);
        }
        log::info!("simulator plan path: {:?}", paths.plan);
        if shrink {
            log::info!("shrunk plan path: {:?}", paths.shrunk_plan);
        }
        log::info!("simulator history path: {:?}", paths.history);

        paths
    }
}

fn main() -> Result<(), String> {
    let _ = env_logger::try_init();

    let cli_opts = SimulatorCLI::parse();
    cli_opts.validate()?;

    let seed = match cli_opts.seed {
        Some(seed) => seed,
        None => rand::thread_rng().next_u64(),
    };

    let output_dir = match &cli_opts.output_dir {
        Some(dir) => Path::new(dir).to_path_buf(),
        None => TempDir::new().map_err(|e| format!("{:?}", e))?.into_path(),
    };

    let paths = Paths::new(&output_dir, cli_opts.shrink, cli_opts.doublecheck);
    log::info!("seed: {}", seed);

    let last_execution = Arc::new(Mutex::new(Execution::new(0, 0, 0)));

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

    let result = SandboxedResult::from(
        std::panic::catch_unwind(|| {
            run_simulation(
                seed,
                &cli_opts,
                &paths.db,
                &paths.plan,
                last_execution.clone(),
                None,
            )
        }),
        last_execution.clone(),
    );

    if cli_opts.doublecheck {
        // Run the simulation again
        let result2 = SandboxedResult::from(
            std::panic::catch_unwind(|| {
                run_simulation(
                    seed,
                    &cli_opts,
                    &paths.doublecheck_db,
                    &paths.plan,
                    last_execution.clone(),
                    None,
                )
            }),
            last_execution.clone(),
        );

        match (result, result2) {
            (SandboxedResult::Correct, SandboxedResult::Panicked { .. }) => {
                log::error!("doublecheck failed! first run succeeded, but second run panicked.");
            }
            (SandboxedResult::FoundBug { .. }, SandboxedResult::Panicked { .. }) => {
                log::error!(
                    "doublecheck failed! first run failed an assertion, but second run panicked."
                );
            }
            (SandboxedResult::Panicked { .. }, SandboxedResult::Correct) => {
                log::error!("doublecheck failed! first run panicked, but second run succeeded.");
            }
            (SandboxedResult::Panicked { .. }, SandboxedResult::FoundBug { .. }) => {
                log::error!(
                    "doublecheck failed! first run panicked, but second run failed an assertion."
                );
            }
            (SandboxedResult::Correct, SandboxedResult::FoundBug { .. }) => {
                log::error!(
                    "doublecheck failed! first run succeeded, but second run failed an assertion."
                );
            }
            (SandboxedResult::FoundBug { .. }, SandboxedResult::Correct) => {
                log::error!(
                    "doublecheck failed! first run failed an assertion, but second run succeeded."
                );
            }
            (SandboxedResult::Correct, SandboxedResult::Correct)
            | (SandboxedResult::FoundBug { .. }, SandboxedResult::FoundBug { .. })
            | (SandboxedResult::Panicked { .. }, SandboxedResult::Panicked { .. }) => {
                // Compare the two database files byte by byte
                let db_bytes = std::fs::read(&paths.db).unwrap();
                let doublecheck_db_bytes = std::fs::read(&paths.doublecheck_db).unwrap();
                if db_bytes != doublecheck_db_bytes {
                    log::error!("doublecheck failed! database files are different.");
                } else {
                    log::info!("doublecheck succeeded! database files are the same.");
                }
            }
        }
    } else {
        // No doublecheck, run shrinking if panicking or found a bug.
        match &result {
            SandboxedResult::Correct => {
                log::info!("simulation succeeded");
            }
            SandboxedResult::Panicked {
                error,
                last_execution,
            }
            | SandboxedResult::FoundBug {
                error,
                last_execution,
                ..
            } => {
                if let SandboxedResult::FoundBug { history, .. } = &result {
                    // No panic occurred, so write the history to a file
                    let f = std::fs::File::create(&paths.history).unwrap();
                    let mut f = std::io::BufWriter::new(f);
                    for execution in history.history.iter() {
                        writeln!(
                            f,
                            "{} {} {}",
                            execution.connection_index,
                            execution.interaction_index,
                            execution.secondary_index
                        )
                        .unwrap();
                    }
                }

                log::error!("simulation failed: '{}'", error);

                if cli_opts.shrink {
                    log::info!("Starting to shrink");
                    let shrink = Some(last_execution);
                    let last_execution = Arc::new(Mutex::new(*last_execution));

                    let shrunk = SandboxedResult::from(
                        std::panic::catch_unwind(|| {
                            run_simulation(
                                seed,
                                &cli_opts,
                                &paths.shrunk_db,
                                &paths.shrunk_plan,
                                last_execution.clone(),
                                shrink,
                            )
                        }),
                        last_execution,
                    );

                    match (shrunk, &result) {
                        (
                            SandboxedResult::Panicked { error: e1, .. },
                            SandboxedResult::Panicked { error: e2, .. },
                        )
                        | (
                            SandboxedResult::FoundBug { error: e1, .. },
                            SandboxedResult::FoundBug { error: e2, .. },
                        ) => {
                            if &e1 != e2 {
                                log::error!(
                                    "shrinking failed, the error was not properly reproduced"
                                );
                            } else {
                                log::info!("shrinking succeeded");
                            }
                        }
                        (_, SandboxedResult::Correct) => {
                            unreachable!("shrinking should never be called on a correct simulation")
                        }
                        _ => {
                            log::error!("shrinking failed, the error was not properly reproduced");
                        }
                    }

                    // Write the shrunk plan to a file
                    let shrunk_plan = std::fs::read(&paths.shrunk_plan).unwrap();
                    let mut f = std::fs::File::create(&paths.shrunk_plan).unwrap();
                    f.write_all(&shrunk_plan).unwrap();
                }
            }
        }
    }

    // Print the seed, the locations of the database and the plan file at the end again for easily accessing them.
    println!("database path: {:?}", paths.db);
    if cli_opts.doublecheck {
        println!("doublecheck database path: {:?}", paths.doublecheck_db);
    } else if cli_opts.shrink {
        println!("shrunk database path: {:?}", paths.shrunk_db);
    }
    println!("simulator plan path: {:?}", paths.plan);
    if cli_opts.shrink {
        println!("shrunk plan path: {:?}", paths.shrunk_plan);
    }
    println!("simulator history path: {:?}", paths.history);
    println!("seed: {}", seed);

    Ok(())
}

fn move_db_and_plan_files(output_dir: &Path) {
    let old_db_path = output_dir.join("simulator.db");
    let old_plan_path = output_dir.join("simulator.plan");

    let new_db_path = output_dir.join("simulator_double.db");
    let new_plan_path = output_dir.join("simulator_double.plan");

    std::fs::rename(&old_db_path, &new_db_path).unwrap();
    std::fs::rename(&old_plan_path, &new_plan_path).unwrap();
}

fn revert_db_and_plan_files(output_dir: &Path) {
    let old_db_path = output_dir.join("simulator.db");
    let old_plan_path = output_dir.join("simulator.plan");

    let new_db_path = output_dir.join("simulator_double.db");
    let new_plan_path = output_dir.join("simulator_double.plan");

    std::fs::rename(&new_db_path, &old_db_path).unwrap();
    std::fs::rename(&new_plan_path, &old_plan_path).unwrap();
}

enum SandboxedResult {
    Panicked {
        error: String,
        last_execution: Execution,
    },
    FoundBug {
        error: String,
        history: ExecutionHistory,
        last_execution: Execution,
    },
    Correct,
}

impl SandboxedResult {
    fn from(
        result: Result<ExecutionResult, Box<dyn Any + Send>>,
        last_execution: Arc<Mutex<Execution>>,
    ) -> Self {
        match result {
            Ok(ExecutionResult { error: None, .. }) => SandboxedResult::Correct,
            Ok(ExecutionResult { error: Some(e), .. }) => {
                let error = format!("{:?}", e);
                let last_execution = last_execution.lock().unwrap();
                SandboxedResult::Panicked {
                    error,
                    last_execution: *last_execution,
                }
            }
            Err(payload) => {
                log::error!("panic occurred");
                let err = if let Some(s) = payload.downcast_ref::<&str>() {
                    log::error!("{}", s);
                    s.to_string()
                } else if let Some(s) = payload.downcast_ref::<String>() {
                    log::error!("{}", s);
                    s.to_string()
                } else {
                    log::error!("unknown panic payload");
                    "unknown panic payload".to_string()
                };

                last_execution.clear_poison();

                SandboxedResult::Panicked {
                    error: err,
                    last_execution: *last_execution.lock().unwrap(),
                }
            }
        }
    }
}

fn run_simulation(
    seed: u64,
    cli_opts: &SimulatorCLI,
    db_path: &Path,
    plan_path: &Path,
    last_execution: Arc<Mutex<Execution>>,
    shrink: Option<&Execution>,
) -> ExecutionResult {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);

    let (create_percent, read_percent, write_percent, delete_percent) = {
        let mut remaining = 100.0;
        let read_percent = rng.gen_range(0.0..=remaining);
        remaining -= read_percent;
        let write_percent = rng.gen_range(0.0..=remaining);
        remaining -= write_percent;
        let delete_percent = remaining;

        let create_percent = write_percent / 10.0;
        let write_percent = write_percent - create_percent;

        (create_percent, read_percent, write_percent, delete_percent)
    };

    let opts = SimulatorOpts {
        ticks: rng.gen_range(cli_opts.minimum_size..=cli_opts.maximum_size),
        max_connections: 1, // TODO: for now let's use one connection as we didn't implement
        // correct transactions procesing
        max_tables: rng.gen_range(0..128),
        create_percent,
        read_percent,
        write_percent,
        delete_percent,
        page_size: 4096, // TODO: randomize this too
        max_interactions: rng.gen_range(cli_opts.minimum_size..=cli_opts.maximum_size),
        max_time_simulation: cli_opts.maximum_time,
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
        .map(|_| InteractionPlan::arbitrary_from(&mut env.rng.clone(), &mut env))
        .collect::<Vec<_>>();
    let mut states = plans
        .iter()
        .map(|_| InteractionPlanState {
            stack: vec![],
            interaction_pointer: 0,
            secondary_pointer: 0,
        })
        .collect::<Vec<_>>();

    let plan = if let Some(failing_execution) = shrink {
        // todo: for now, we only use 1 connection, so it's safe to use the first plan.
        println!("Interactions Before: {}", plans[0].plan.len());
        let shrunk = plans[0].shrink_interaction_plan(failing_execution);
        println!("Interactions After: {}", shrunk.plan.len());
        shrunk
    } else {
        plans[0].clone()
    };

    let mut f = std::fs::File::create(plan_path).unwrap();
    // todo: create a detailed plan file with all the plans. for now, we only use 1 connection, so it's safe to use the first plan.
    f.write_all(plan.to_string().as_bytes()).unwrap();

    log::info!("{}", plan.stats());

    log::info!("Executing database interaction plan...");

    let result = execute_plans(&mut env, &mut plans, &mut states, last_execution);

    env.io.print_stats();

    log::info!("Simulation completed");

    result
}
