use anyhow::Result;
use clap::Parser;
use memory_benchmark::fts::{
    CorpusConfig, Execution, FtsConfig, FtsObserver, FtsPhase, IndexStats, QueryCase, QueryState,
    RunResult, run_fts,
};
use memory_benchmark::workload::JournalMode;
use serde::Serialize;
use std::path::PathBuf;
use std::time::Instant;

#[cfg(not(clippy))]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[derive(Parser)]
#[command(about = "Profile FTS query allocations, excluding corpus setup and warm-up")]
struct Args {
    #[arg(long, default_value = "common")]
    query: QueryCase,
    #[arg(long, default_value = "warm")]
    state: QueryState,
    #[arg(long, default_value = "10000")]
    documents: usize,
    #[arg(long, default_value = "0")]
    extra_tokens: usize,
    #[arg(long)]
    cache_pages: Option<usize>,
    #[arg(long, default_value = "0")]
    min_index_bytes: usize,
    #[arg(long, conflicts_with = "transactions_per_connection")]
    queries: Option<usize>,
    #[arg(long, default_value = "wal")]
    mode: JournalMode,
    #[arg(long, default_value = "1")]
    connections: usize,
    #[arg(long)]
    transactions_per_connection: Option<usize>,
    #[arg(long, requires = "transactions_per_connection")]
    queries_per_transaction: Option<usize>,
    #[arg(long, default_value = "dhat-heap.json")]
    dhat_file: PathBuf,
}

#[derive(Serialize)]
struct QueryHeap {
    completed_queries: usize,
    completed_transactions: usize,
    live_query_bytes: usize,
}

#[derive(Serialize)]
struct PhaseEvent {
    event: &'static str,
    phase: FtsPhase,
    elapsed_ms: u128,
}

struct DhatObserver {
    #[cfg(not(clippy))]
    path: PathBuf,
    #[cfg(not(clippy))]
    profiler: Option<dhat::Profiler>,
    stats: Option<dhat::HeapStats>,
    query_heap: Vec<QueryHeap>,
    phases: Vec<PhaseEvent>,
    start: Instant,
    rss_before: Option<usize>,
    rss_after: Option<usize>,
    index: Option<IndexStats>,
}

#[derive(Serialize)]
struct Report {
    corpus: CorpusConfig,
    index: IndexStats,
    query: QueryCase,
    state: QueryState,
    mode: JournalMode,
    connections: usize,
    transactions: usize,
    queries_per_transaction: Option<usize>,
    max_active_transactions: usize,
    documents: usize,
    queries: usize,
    rows_per_query: usize,
    total_allocated_bytes: u64,
    total_allocations: u64,
    allocated_bytes_per_query: f64,
    allocations_per_query: f64,
    peak_live_query_bytes: usize,
    retained_query_bytes: usize,
    rss_before_query_bytes: Option<usize>,
    rss_after_profiler_bytes: Option<usize>,
    query_heap: Vec<QueryHeap>,
    phases: Vec<PhaseEvent>,
    dhat_file: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let execution = match args.transactions_per_connection {
        Some(per_connection) => Execution::Transactions {
            per_connection,
            queries_per_transaction: args.queries_per_transaction.unwrap_or(1),
        },
        None => Execution::Queries(args.queries.unwrap_or(match args.state {
            QueryState::First => 1,
            QueryState::Warm => 100,
        })),
    };
    let config = FtsConfig {
        query: args.query,
        state: args.state,
        documents: args.documents,
        corpus: CorpusConfig {
            extra_tokens: args.extra_tokens,
            cache_pages: args.cache_pages,
            min_index_bytes: args.min_index_bytes,
        },
        mode: args.mode,
        connections: args.connections,
        execution,
    };
    config.validate()?;
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.connections)
        .enable_all()
        .build()?;
    let mut observer = DhatObserver {
        #[cfg(not(clippy))]
        path: args.dhat_file.clone(),
        #[cfg(not(clippy))]
        profiler: None,
        stats: None,
        query_heap: Vec::with_capacity(execution.batches()),
        phases: Vec::with_capacity(6),
        start: Instant::now(),
        rss_before: None,
        rss_after: None,
        index: None,
    };
    let result = rt.block_on(run_fts(config, &mut observer))?;
    let stats = observer
        .stats
        .expect("run phase must finish before reporting");
    let report = Report {
        corpus: config.corpus,
        index: observer.index.expect("index measured during setup"),
        query: args.query,
        state: args.state,
        mode: args.mode,
        connections: args.connections,
        transactions: result.transactions,
        queries_per_transaction: match execution {
            Execution::Queries(_) => None,
            Execution::Transactions {
                queries_per_transaction,
                ..
            } => Some(queries_per_transaction),
        },
        max_active_transactions: result.max_active_transactions,
        documents: args.documents,
        queries: result.queries,
        rows_per_query: result.rows / result.queries,
        total_allocated_bytes: stats.total_bytes,
        total_allocations: stats.total_blocks,
        allocated_bytes_per_query: stats.total_bytes as f64 / result.queries as f64,
        allocations_per_query: stats.total_blocks as f64 / result.queries as f64,
        peak_live_query_bytes: stats.max_bytes,
        retained_query_bytes: stats.curr_bytes,
        rss_before_query_bytes: observer.rss_before,
        rss_after_profiler_bytes: observer.rss_after,
        query_heap: observer.query_heap,
        phases: observer.phases,
        dhat_file: args.dhat_file,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

impl FtsObserver for DhatObserver {
    fn on_index(&mut self, stats: &IndexStats) {
        eprintln!("{}", serde_json::json!({"event": "index", "index": stats}));
        self.index = Some(stats.clone());
    }

    fn on_phase(&mut self, phase: FtsPhase) {
        if phase == FtsPhase::Cleanup {
            self.stats = Some(dhat::HeapStats::get());
            #[cfg(not(clippy))]
            drop(
                self.profiler
                    .take()
                    .expect("run phase must start profiling"),
            );
            self.rss_after = rss();
        }
        let event = PhaseEvent {
            event: "phase",
            phase,
            elapsed_ms: self.start.elapsed().as_millis(),
        };
        eprintln!(
            "{}",
            serde_json::to_string(&event).expect("phase event must serialize")
        );
        self.phases.push(event);
        if phase == FtsPhase::Run {
            self.rss_before = rss();
            #[cfg(not(clippy))]
            {
                self.profiler = Some(dhat::Profiler::builder().file_name(&self.path).build());
            }
        }
    }

    fn after_batch(&mut self, progress: &RunResult) {
        self.query_heap.push(QueryHeap {
            completed_queries: progress.queries,
            completed_transactions: progress.transactions,
            live_query_bytes: dhat::HeapStats::get().curr_bytes,
        });
    }
}

fn rss() -> Option<usize> {
    memory_stats::memory_stats().map(|stats| stats.physical_mem)
}
