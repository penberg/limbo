#![allow(clippy::arc_with_non_send_sync, dead_code)]
use clap::Parser;
use generation::plan::{Interaction, InteractionPlan, InteractionPlanState};
use generation::ArbitraryFrom;
use limbo_core::Database;
use notify::event::{DataChange, ModifyKind};
use notify::{EventKind, RecursiveMode, Watcher};
use rand::prelude::*;
use runner::cli::SimulatorCLI;
use runner::env::SimulatorEnv;
use runner::execution::{execute_plans, Execution, ExecutionHistory, ExecutionResult};
use runner::{differential, watch};
use std::any::Any;
use std::backtrace::Backtrace;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
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
        log::info!(
            "simulator plan serialized path: {:?}",
            paths.plan.with_extension("plan.json")
        );
        if shrink {
            log::info!("shrunk plan path: {:?}", paths.shrunk_plan);
        }
        log::info!("simulator history path: {:?}", paths.history);

        paths
    }
}

fn main() -> Result<(), String> {
    init_logger();

    let cli_opts = SimulatorCLI::parse();
    cli_opts.validate()?;

    let seed = cli_opts.seed.unwrap_or_else(|| thread_rng().next_u64());

    let output_dir = match &cli_opts.output_dir {
        Some(dir) => Path::new(dir).to_path_buf(),
        None => TempDir::new().map_err(|e| format!("{:?}", e))?.into_path(),
    };

    banner();
    let paths = Paths::new(&output_dir, cli_opts.shrink, cli_opts.doublecheck);

    log::info!("seed: {}", seed);

    let last_execution = Arc::new(Mutex::new(Execution::new(0, 0, 0)));
    let (env, plans) = setup_simulation(seed, &cli_opts, &paths.db, &paths.plan);

    if cli_opts.watch {
        watch_mode(seed, &cli_opts, &paths, last_execution.clone()).unwrap();
    } else if cli_opts.differential {
        differential_testing(env, plans, last_execution.clone())
    } else {
        run_simulator(&cli_opts, &paths, env, plans, last_execution.clone());
    }

    // Print the seed, the locations of the database and the plan file at the end again for easily accessing them.
    println!("database path: {:?}", paths.db);
    if cli_opts.doublecheck {
        println!("doublecheck database path: {:?}", paths.doublecheck_db);
    } else if cli_opts.shrink {
        println!("shrunk database path: {:?}", paths.shrunk_db);
    }
    println!("simulator plan path: {:?}", paths.plan);
    println!(
        "simulator plan serialized path: {:?}",
        paths.plan.with_extension("plan.json")
    );
    if cli_opts.shrink {
        println!("shrunk plan path: {:?}", paths.shrunk_plan);
    }
    println!("simulator history path: {:?}", paths.history);
    println!("seed: {}", seed);

    Ok(())
}

fn watch_mode(
    seed: u64,
    cli_opts: &SimulatorCLI,
    paths: &Paths,
    last_execution: Arc<Mutex<Execution>>,
) -> notify::Result<()> {
    let (tx, rx) = mpsc::channel::<notify::Result<notify::Event>>();
    println!("watching {:?}", paths.plan);
    // Use recommended_watcher() to automatically select the best implementation
    // for your platform. The `EventHandler` passed to this constructor can be a
    // closure, a `std::sync::mpsc::Sender`, a `crossbeam_channel::Sender`, or
    // another type the trait is implemented for.
    let mut watcher = notify::recommended_watcher(tx)?;

    // Add a path to be watched. All files and directories at that path and
    // below will be monitored for changes.
    watcher.watch(&paths.plan, RecursiveMode::NonRecursive)?;
    // Block forever, printing out events as they come in
    for res in rx {
        match res {
            Ok(event) => {
                if let EventKind::Modify(ModifyKind::Data(DataChange::Content)) = event.kind {
                    log::info!("plan file modified, rerunning simulation");

                    let result = SandboxedResult::from(
                        std::panic::catch_unwind(|| {
                            let plan: Vec<Vec<Interaction>> =
                                InteractionPlan::compute_via_diff(&paths.plan);

                            let mut env = SimulatorEnv::new(seed, cli_opts, &paths.db);
                            plan.iter().for_each(|is| {
                                is.iter().for_each(|i| {
                                    i.shadow(&mut env);
                                });
                            });
                            let env = Arc::new(Mutex::new(env.clone()));
                            watch::run_simulation(env, &mut [plan], last_execution.clone())
                        }),
                        last_execution.clone(),
                    );
                    match result {
                        SandboxedResult::Correct => {
                            log::info!("simulation succeeded");
                            println!("simulation succeeded");
                        }
                        SandboxedResult::Panicked { error, .. }
                        | SandboxedResult::FoundBug { error, .. } => {
                            log::error!("simulation failed: '{}'", error);
                            println!("simulation failed: '{}'", error);
                        }
                    }
                }
            }
            Err(e) => println!("watch error: {:?}", e),
        }
    }

    Ok(())
}

fn run_simulator(
    cli_opts: &SimulatorCLI,
    paths: &Paths,
    env: SimulatorEnv,
    plans: Vec<InteractionPlan>,
    last_execution: Arc<Mutex<Execution>>,
) {
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

    let env = Arc::new(Mutex::new(env));
    let result = SandboxedResult::from(
        std::panic::catch_unwind(|| {
            run_simulation(env.clone(), &mut plans.clone(), last_execution.clone())
        }),
        last_execution.clone(),
    );

    if cli_opts.doublecheck {
        doublecheck(env.clone(), paths, &plans, last_execution.clone(), result);
    } else {
        // No doublecheck, run shrinking if panicking or found a bug.
        match &result {
            SandboxedResult::Correct => {
                log::info!("simulation succeeded");
                println!("simulation succeeded");
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
                println!("simulation failed: '{}'", error);

                if cli_opts.shrink {
                    log::info!("Starting to shrink");

                    let shrunk_plans = plans
                        .iter()
                        .map(|plan| {
                            let shrunk = plan.shrink_interaction_plan(last_execution);
                            log::info!("{}", shrunk.stats());
                            shrunk
                        })
                        .collect::<Vec<_>>();

                    // Write the shrunk plan to a file
                    let mut f = std::fs::File::create(&paths.shrunk_plan).unwrap();
                    f.write_all(shrunk_plans[0].to_string().as_bytes()).unwrap();

                    let last_execution = Arc::new(Mutex::new(*last_execution));

                    let shrunk = SandboxedResult::from(
                        std::panic::catch_unwind(|| {
                            run_simulation(
                                env.clone(),
                                &mut shrunk_plans.clone(),
                                last_execution.clone(),
                            )
                        }),
                        last_execution,
                    );

                    match (&shrunk, &result) {
                        (
                            SandboxedResult::Panicked { error: e1, .. },
                            SandboxedResult::Panicked { error: e2, .. },
                        )
                        | (
                            SandboxedResult::FoundBug { error: e1, .. },
                            SandboxedResult::FoundBug { error: e2, .. },
                        ) => {
                            if e1 != e2 {
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
                }
            }
        }
    }
}

fn doublecheck(
    env: Arc<Mutex<SimulatorEnv>>,
    paths: &Paths,
    plans: &[InteractionPlan],
    last_execution: Arc<Mutex<Execution>>,
    result: SandboxedResult,
) {
    {
        let mut env_ = env.lock().unwrap();
        env_.db =
            Database::open_file(env_.io.clone(), paths.doublecheck_db.to_str().unwrap()).unwrap();
    }

    // Run the simulation again
    let result2 = SandboxedResult::from(
        std::panic::catch_unwind(|| {
            run_simulation(env.clone(), &mut plans.to_owned(), last_execution.clone())
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
}

fn differential_testing(
    env: SimulatorEnv,
    plans: Vec<InteractionPlan>,
    last_execution: Arc<Mutex<Execution>>,
) {
    let env = Arc::new(Mutex::new(env));
    let result = SandboxedResult::from(
        std::panic::catch_unwind(|| {
            let plan = plans[0].clone();
            differential::run_simulation(env, &mut [plan], last_execution.clone())
        }),
        last_execution.clone(),
    );

    if let SandboxedResult::Correct = result {
        log::info!("simulation succeeded");
        println!("simulation succeeded");
    } else {
        log::error!("simulation failed");
        println!("simulation failed");
    }
}

#[derive(Debug)]
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

fn setup_simulation(
    mut seed: u64,
    cli_opts: &SimulatorCLI,
    db_path: &Path,
    plan_path: &Path,
) -> (SimulatorEnv, Vec<InteractionPlan>) {
    if let Some(load) = &cli_opts.load {
        let seed_path = PathBuf::from(load).with_extension("seed");
        let seed_str = std::fs::read_to_string(&seed_path).unwrap();
        seed = seed_str.parse().unwrap();
    }

    let mut env = SimulatorEnv::new(seed, cli_opts, db_path);

    // todo: the loading works correctly because of a hacky decision
    // Right now, the plan generation is the only point we use the rng, so the environment doesn't
    // even need it. In the future, especially with multi-connections and multi-threading, we might
    // use the RNG for more things such as scheduling, so this assumption will fail.  When that happens,
    // we'll need to reachitect this logic by saving and loading RNG state.
    let plans = if let Some(load) = &cli_opts.load {
        log::info!("Loading database interaction plan...");
        let plan = std::fs::read_to_string(load).unwrap();
        let plan: InteractionPlan = serde_json::from_str(&plan).unwrap();
        vec![plan]
    } else {
        log::info!("Generating database interaction plan...");
        (1..=env.opts.max_connections)
            .map(|_| InteractionPlan::arbitrary_from(&mut env.rng.clone(), &mut env))
            .collect::<Vec<_>>()
    };

    // todo: for now, we only use 1 connection, so it's safe to use the first plan.
    let plan = plans[0].clone();

    let mut f = std::fs::File::create(plan_path).unwrap();
    // todo: create a detailed plan file with all the plans. for now, we only use 1 connection, so it's safe to use the first plan.
    f.write_all(plan.to_string().as_bytes()).unwrap();

    let serialized_plan_path = plan_path.with_extension("plan.json");
    let mut f = std::fs::File::create(&serialized_plan_path).unwrap();
    f.write_all(serde_json::to_string(&plan).unwrap().as_bytes())
        .unwrap();

    let seed_path = plan_path.with_extension("seed");
    let mut f = std::fs::File::create(&seed_path).unwrap();
    f.write_all(seed.to_string().as_bytes()).unwrap();

    log::info!("{}", plan.stats());
    (env, plans)
}

fn run_simulation(
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
    let result = execute_plans(env.clone(), plans, &mut states, last_execution);

    let env = env.lock().unwrap();
    env.io.print_stats();

    log::info!("Simulation completed");

    result
}

fn init_logger() {
    env_logger::Builder::from_env(env_logger::Env::default().filter_or("RUST_LOG", "info"))
        .format_timestamp(None)
        .format_module_path(false)
        .format_target(false)
        .init();
}

fn banner() {
    println!("{}", BANNER);
}

const BANNER: &str = r#"
  ,_______________________________.
  | ,___________________________. |
  | |                           | |
  | | >HELLO                    | |
  | |                           | |
  | | >A STRANGE GAME.          | |
  | | >THE ONLY WINNING MOVE IS | |
  | | >NOT TO PLAY.             | |
  | |___________________________| |
  |                               |
  |                               |
  `-------------------------------`
          |              |
          |______________|
      ,______________________.
     / /====================\ \
    / /======================\ \
   /____________________________\
   \____________________________/

"#;
