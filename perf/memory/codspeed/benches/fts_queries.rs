use divan::{Bencher, black_box};
use memory_benchmark::fts::{
    CorpusConfig, Execution, FtsConfig, FtsWorkload, QueryCase, QueryState,
};
use memory_benchmark::workload::JournalMode;

#[cfg(not(feature = "codspeed"))]
#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

const CONFIGURATIONS: &[(QueryState, usize, usize)] = &[
    (QueryState::First, 1_000, 1),
    (QueryState::First, 10_000, 1),
    (QueryState::Warm, 1_000, 1),
    (QueryState::Warm, 10_000, 1),
    (QueryState::Warm, 10_000, 10),
    (QueryState::Warm, 10_000, 100),
];

fn main() {
    divan::main();
}

macro_rules! query_bench {
    ($name:ident, $case:ident) => {
        #[turso_macros::divan_bench(args = CONFIGURATIONS, sample_size = 1)]
        fn $name(bencher: Bencher, configuration: &(QueryState, usize, usize)) {
            let &(state, documents, queries) = configuration;
            benchmark(bencher, QueryCase::$case, state, documents, queries);
        }
    };
}

query_bench!(rare, Rare);
query_bench!(common, Common);
query_bench!(and, And);
query_bench!(or, Or);
query_bench!(phrase, Phrase);
query_bench!(ranked, Ranked);

mod transactions {
    use super::*;

    const CONFIGURATIONS: &[(JournalMode, usize, usize, usize)] = &[
        (JournalMode::Wal, 1, 10, 2),
        (JournalMode::Mvcc, 1, 10, 2),
        (JournalMode::Mvcc, 2, 10, 2),
        (JournalMode::Mvcc, 4, 10, 2),
    ];

    macro_rules! transaction_bench {
        ($name:ident, $case:ident) => {
            #[turso_macros::divan_bench(args = CONFIGURATIONS, sample_size = 1)]
            fn $name(bencher: Bencher, configuration: &(JournalMode, usize, usize, usize)) {
                let &(mode, connections, per_connection, queries_per_transaction) = configuration;
                benchmark_workload(
                    bencher,
                    FtsConfig {
                        query: QueryCase::$case,
                        state: QueryState::Warm,
                        documents: 1_000,
                        corpus: CorpusConfig::default(),
                        mode,
                        connections,
                        execution: Execution::Transactions {
                            per_connection,
                            queries_per_transaction,
                        },
                    },
                );
            }
        };
    }

    transaction_bench!(rare, Rare);
    transaction_bench!(common, Common);
    transaction_bench!(and, And);
    transaction_bench!(or, Or);
    transaction_bench!(phrase, Phrase);
    transaction_bench!(ranked, Ranked);
}

#[cfg(feature = "fts-stress")]
#[turso_macros::divan_bench(args = [1, 2], sample_size = 1, sample_count = 1)]
fn oversized_index(bencher: Bencher, connections: usize) {
    benchmark_workload(
        bencher,
        FtsConfig {
            query: QueryCase::Common,
            state: QueryState::Warm,
            documents: 20_000,
            corpus: CorpusConfig {
                extra_tokens: 1024,
                cache_pages: Some(200),
                min_index_bytes: 256 * 1024 * 1024,
            },
            mode: JournalMode::Mvcc,
            connections,
            execution: Execution::Transactions {
                per_connection: 3,
                queries_per_transaction: 2,
            },
        },
    );
}

fn benchmark(
    bencher: Bencher,
    case: QueryCase,
    state: QueryState,
    documents: usize,
    queries: usize,
) {
    benchmark_workload(
        bencher,
        FtsConfig {
            query: case,
            state,
            documents,
            corpus: CorpusConfig::default(),
            mode: JournalMode::Wal,
            connections: 1,
            execution: Execution::Queries(queries),
        },
    );
}

fn benchmark_workload(bencher: Bencher, config: FtsConfig) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.connections)
        .enable_all()
        .build()
        .unwrap();
    bencher
        .with_inputs(|| rt.block_on(FtsWorkload::prepare(config, &mut ())).unwrap())
        .bench_local_refs(|workload| {
            black_box(rt.block_on(workload.run(&mut ())).unwrap());
        });
}
