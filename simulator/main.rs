use clap::Parser;
use generation::plan::{Interaction, InteractionPlan, ResultSet};
use generation::{pick_index, Arbitrary, ArbitraryFrom};
use limbo_core::{Connection, Database, Result, RowResult, IO};
use model::query::{Create, Query};
use model::table::{Column, Name, Table, Value};
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use simulator::cli::SimulatorCLI;
use simulator::env::{SimConnection, SimulatorEnv, SimulatorOpts};
use simulator::io::SimulatorIO;
use std::io::Write;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use tempfile::TempDir;

mod generation;
mod model;
mod properties;
mod simulator;

#[allow(clippy::arc_with_non_send_sync)]
fn main() {
    let _ = env_logger::try_init();

    let opts = SimulatorCLI::parse();

    let seed = match opts.seed {
        Some(seed) => seed,
        None => rand::thread_rng().next_u64(),
    };

    let output_dir = match opts.output_dir {
        Some(dir) => Path::new(&dir).to_path_buf(),
        None => TempDir::new().unwrap().into_path(),
    };

    let db_path = output_dir.join("simulator.db");
    let plan_path = output_dir.join("simulator.plan");

    // Print the seed, the locations of the database and the plan file
    log::info!("database path: {:?}", db_path);
    log::info!("simulator plan path: {:?}", plan_path);
    log::info!("seed: {}", seed);

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

    let opts = SimulatorOpts {
        ticks: rng.gen_range(0..opts.maximum_size),
        max_connections: 1, // TODO: for now let's use one connection as we didn't implement
        // correct transactions procesing
        max_tables: rng.gen_range(0..128),
        read_percent,
        write_percent,
        delete_percent,
        page_size: 4096, // TODO: randomize this too
        max_interactions: rng.gen_range(0..opts.maximum_size),
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

    let mut f = std::fs::File::create(plan_path.clone()).unwrap();
    // todo: create a detailed plan file with all the plans. for now, we only use 1 connection, so it's safe to use the first plan.
    f.write(plans[0].to_string().as_bytes()).unwrap();

    log::info!("{}", plans[0].stats());

    log::info!("Executing database interaction plan...");

    let result = execute_plans(&mut env, &mut plans);
    if result.is_err() {
        log::error!("error executing plans: {:?}", result.err());
    }

    env.io.print_stats();

    // Print the seed, the locations of the database and the plan file at the end again for easily accessing them.
    log::info!("database path: {:?}", db_path);
    log::info!("simulator plan path: {:?}", plan_path);
    log::info!("seed: {}", seed);
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

fn maybe_add_table(env: &mut SimulatorEnv, conn: &mut Rc<Connection>) -> Result<()> {
    if env.tables.len() < env.opts.max_tables {
        let table = Table {
            rows: Vec::new(),
            name: Name::arbitrary(&mut env.rng).0,
            columns: (1..env.rng.gen_range(1..128))
                .map(|_| Column::arbitrary(&mut env.rng))
                .collect(),
        };
        let query = Query::Create(Create {
            table: table.clone(),
        });
        let rows = get_all_rows(env, conn, query.to_string().as_str())?;
        log::debug!("{:?}", rows);
        let rows = get_all_rows(
            env,
            conn,
            format!(
                "SELECT sql FROM sqlite_schema WHERE type IN ('table', 'index') AND name = '{}';",
                table.name
            )
            .as_str(),
        )?;
        log::debug!("{:?}", rows);
        assert!(rows.len() == 1);
        let as_text = match &rows[0][0] {
            Value::Text(t) => t,
            _ => unreachable!(),
        };
        assert!(
            *as_text != query.to_string(),
            "table was not inserted correctly"
        );
        env.tables.push(table);
    }
    Ok(())
}

fn get_all_rows(
    env: &mut SimulatorEnv,
    conn: &mut Rc<Connection>,
    query: &str,
) -> Result<Vec<Vec<Value>>> {
    log::info!("running query '{}'", &query[0..query.len().min(4096)]);
    let mut out = Vec::new();
    let rows = conn.query(query);
    if rows.is_err() {
        let err = rows.err();
        log::error!(
            "Error running query '{}': {:?}",
            &query[0..query.len().min(4096)],
            err
        );
        return Err(err.unwrap());
    }
    let rows = rows.unwrap();
    assert!(rows.is_some());
    let mut rows = rows.unwrap();
    'rows_loop: loop {
        env.io.inject_fault(env.rng.gen_ratio(1, 10000));
        match rows.next_row()? {
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
            RowResult::IO => {
                env.io.inject_fault(env.rng.gen_ratio(1, 10000));
                if env.io.run_once().is_err() {
                    log::info!("query inject fault");
                    break 'rows_loop;
                }
            }
            RowResult::Interrupt => {
                break;
            }
            RowResult::Done => {
                break;
            }
        }
    }
    Ok(out)
}
