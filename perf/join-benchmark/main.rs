use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde_json::{json, Value as JsonValue};
use turso_core::{Database, OpenFlags, OpenOptions, PlatformIO, SqliteDialect, StepResult, Value};

#[derive(Parser)]
#[command(name = "turso-join-benchmark")]
#[command(about = "Measure join query execution without printing result rows")]
struct Args {
    /// Database file that contains the benchmark data.
    #[arg(long)]
    database: PathBuf,

    /// Directory that contains one SQL query per file.
    #[arg(long)]
    query_dir: PathBuf,

    /// Run only queries whose file stem contains this text.
    #[arg(long)]
    filter: Option<String>,

    /// Run a query with this exact file stem. This option can occur more than once.
    #[arg(long = "query")]
    query_names: Vec<String>,

    /// Number of unmeasured executions before each measured query.
    #[arg(long, default_value_t = 1)]
    warmups: usize,

    /// Number of measured executions for each query.
    #[arg(long, default_value_t = 5)]
    repetitions: usize,

    /// Stop an execution after this many seconds. Zero disables the timeout.
    #[arg(long, default_value_t = 30)]
    timeout_seconds: u64,

    /// Print each query plan as one JSON record. Do not execute the query.
    #[arg(long)]
    plans: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let queries = load_queries(&args.query_dir, args.filter.as_deref(), &args.query_names)?;
    if queries.is_empty() {
        bail!("no SQL files matched the query selection");
    }

    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new()?);
    let database = Database::open(
        io,
        &args.database.to_string_lossy(),
        OpenOptions::new(Arc::new(SqliteDialect)).flags(OpenFlags::ReadOnly),
    )?;
    let connection = database.connect()?;

    for query in queries {
        if args.plans {
            print_plan(&connection, &query)?;
            continue;
        }

        let mut statement = connection
            .prepare(&query.sql)
            .with_context(|| format!("prepare query {}", query.name))?;
        set_timeout(&mut statement, args.timeout_seconds);

        for _ in 0..args.warmups {
            execute(&database, &mut statement)?;
            statement.reset()?;
        }

        for repetition in 0..args.repetitions {
            statement.reset_metrics();
            let start = Instant::now();
            let result_rows = execute(&database, &mut statement)?;
            let elapsed = start.elapsed();
            let metrics = statement.metrics();
            println!(
                "{}",
                json!({
                    "query": query.name,
                    "repetition": repetition,
                    "elapsed_ns": elapsed.as_nanos().min(u64::MAX as u128) as u64,
                    "result_rows": result_rows,
                    "rows_read": metrics.rows_read,
                    "vm_steps": metrics.vm_steps,
                    "instructions": metrics.insn_executed,
                    "fullscan_steps": metrics.fullscan_steps,
                    "index_steps": metrics.index_steps,
                    "btree_seeks": metrics.btree_seeks,
                    "btree_table_seeks": metrics.btree_table_seeks,
                    "btree_index_seeks": metrics.btree_index_seeks,
                    "btree_deferred_seeks": metrics.btree_deferred_seeks,
                    "btree_next": metrics.btree_next,
                    "btree_prev": metrics.btree_prev,
                    "sort_operations": metrics.sort_operations,
                    "hash_spill_bytes": metrics.hash_join.spill_bytes_written,
                    "hash_load_bytes": metrics.hash_join.load_bytes_read,
                    "hash_probe_calls": metrics.hash_join.probe_calls,
                })
            );
            statement.reset()?;
        }
    }

    Ok(())
}

struct Query {
    name: String,
    sql: String,
}

fn load_queries(
    query_dir: &Path,
    filter: Option<&str>,
    query_names: &[String],
) -> Result<Vec<Query>> {
    let mut paths = fs::read_dir(query_dir)
        .with_context(|| format!("read query directory {}", query_dir.display()))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();

    let mut queries = Vec::new();
    for path in paths {
        if path.extension().and_then(|value| value.to_str()) != Some("sql") {
            continue;
        }
        let name = path
            .file_stem()
            .and_then(|value| value.to_str())
            .context("query file name is not UTF-8")?
            .to_string();
        if filter.is_some_and(|filter| !name.contains(filter)) {
            continue;
        }
        if !query_names.is_empty() && !query_names.contains(&name) {
            continue;
        }
        queries.push(Query {
            name,
            sql: fs::read_to_string(&path)
                .with_context(|| format!("read query file {}", path.display()))?,
        });
    }
    Ok(queries)
}

fn set_timeout(statement: &mut turso_core::Statement, timeout_seconds: u64) {
    let timeout = (timeout_seconds != 0).then(|| Duration::from_secs(timeout_seconds));
    statement.set_query_timeout_override(Some(timeout));
}

fn execute(database: &Database, statement: &mut turso_core::Statement) -> Result<u64> {
    let mut result_rows = 0_u64;
    loop {
        match statement.step()? {
            StepResult::Row => result_rows = result_rows.saturating_add(1),
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => database.io.step()?,
            StepResult::Done => return Ok(result_rows),
            StepResult::Interrupt => bail!("query was interrupted"),
            StepResult::Busy => bail!("database was busy"),
        }
    }
}

fn print_plan(connection: &Arc<turso_core::Connection>, query: &Query) -> Result<()> {
    let sql = format!("EXPLAIN QUERY PLAN FORMAT=JSON {}", query.sql);
    let rows = connection.prepare(sql)?.run_collect_rows()?;
    let Some(Value::Text(plan)) = rows.first().and_then(|row| row.first()) else {
        bail!("query {} did not return a JSON plan", query.name);
    };
    let plan: JsonValue = serde_json::from_str(plan.as_str())?;
    println!("{}", json!({"query": query.name, "plan": plan}));
    Ok(())
}
