//! FTS Query Performance Benchmarks
//!
//! Measures full-text search query performance including:
//! - Cold query (first query after index creation, no cached directory)
//! - Warm query (repeated queries with cached directory)
//! - Alternating warm queries across a connection pool
//! - Insert + query lifecycle (write, commit, query)
//! - Querying after many small commits (segment maintenance)
//! - Sustained single-row commit throughput, including foreground merges
//!
//! Run with: cargo bench --bench fts_benchmark --features fts

#[cfg(not(feature = "codspeed"))]
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
#[cfg(not(feature = "codspeed"))]
use pprof::criterion::{Output, PProfProfiler};
use turso_core::SqliteDialect;

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{criterion_group, criterion_main, BenchmarkId, Criterion};

use std::sync::Arc;
use tempfile::TempDir;
use turso_core::{Database, DatabaseOpts, OpenFlags, PlatformIO, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(not(feature = "codspeed"))]
macro_rules! iter_custom_or_iter {
    ($b:expr, |$iters:ident| $body:block) => {
        $b.iter_custom(|$iters| $body)
    };
}

#[cfg(feature = "codspeed")]
macro_rules! iter_custom_or_iter {
    ($b:expr, |$iters:ident| $body:block) => {
        $b.iter(|| {
            let $iters = 1;
            $body
        })
    };
}

/// Helper to execute a statement to completion, stepping through IO.
fn run_to_completion(
    stmt: &mut turso_core::Statement,
    db: &Arc<Database>,
) -> turso_core::Result<()> {
    loop {
        match stmt.step()? {
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                db.io.step()?;
            }
            StepResult::Done => break,
            StepResult::Row => {}
            StepResult::Interrupt | StepResult::Busy => {
                panic!("Unexpected step result");
            }
        }
    }
    Ok(())
}

/// Helper to step a statement and count result rows.
fn run_and_count_rows(
    stmt: &mut turso_core::Statement,
    db: &Arc<Database>,
) -> turso_core::Result<usize> {
    let mut count = 0;
    loop {
        match stmt.step()? {
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                db.io.step()?;
            }
            StepResult::Done => break,
            StepResult::Row => {
                count += 1;
            }
            StepResult::Interrupt | StepResult::Busy => {
                panic!("Unexpected step result");
            }
        }
    }
    Ok(count)
}

/// Setup a database with an FTS-indexed table populated with `row_count` rows.
fn setup_fts_db(temp_dir: &TempDir, row_count: usize) -> Arc<Database> {
    let db_path = temp_dir.path().join("fts_bench.db");
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new().unwrap());
    let opts = DatabaseOpts::new().with_index_method(true);
    let db = Database::open_file_with_flags(
        io,
        db_path.to_str().unwrap(),
        OpenFlags::default(),
        opts,
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect().unwrap();

    // Create table and FTS index
    conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
        .unwrap();
    conn.execute("CREATE INDEX docs_fts ON docs USING fts (title, body)")
        .unwrap();

    // Insert rows in batches of 500
    let batch_size = 500;
    for batch_start in (0..row_count).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(row_count);
        let mut sql = String::from("INSERT INTO docs (id, title, body) VALUES ");
        for i in batch_start..batch_end {
            if i > batch_start {
                sql.push(',');
            }
            // Vary content so term dictionaries have realistic distribution
            let word_a = match i % 7 {
                0 => "database",
                1 => "performance",
                2 => "optimization",
                3 => "benchmark",
                4 => "storage",
                5 => "indexing",
                _ => "computing",
            };
            let word_b = match i % 5 {
                0 => "systems",
                1 => "analysis",
                2 => "engineering",
                3 => "architecture",
                _ => "design",
            };
            sql.push_str(&format!(
                "({i}, '{word_a} document {i}', 'This is the body of document {i} about {word_a} and {word_b} with additional text for realistic content size')"
            ));
        }
        conn.execute(&sql).unwrap();
    }

    db
}

/// Setup an index with one FTS commit per row to exercise segment churn.
fn setup_fts_churn_db(temp_dir: &TempDir, commit_count: usize) -> Arc<Database> {
    let db = setup_fts_db(temp_dir, 0);
    let conn = db.connect().unwrap();

    for id in 0..commit_count {
        let marker = if id == 0 { "needle" } else { "haystack" };
        conn.execute(format!(
            "INSERT INTO docs (id, title, body) VALUES \
             ({id}, 'segment {id}', \
             'common {marker} term in independently committed document {id}')"
        ))
        .unwrap();
    }

    db
}

/// Benchmark: Cold FTS query (no cached directory — measures full loading pipeline)
///
/// This measures the worst-case: open_read must scan the BTree catalog,
/// load hot files, create the Tantivy Index, build a Reader+Searcher,
/// parse the query, and execute the search. Each iteration uses a fresh
/// connection to avoid directory cache hits.
#[turso_macros::codspeed_criterion_benchmark]
fn bench_fts_cold_query(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("FTS Cold Query");
    group.sample_size(20); // Cold queries are slow; reduce samples

    for row_count in [1000, 5000, 10000] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_fts_db(&temp_dir, row_count);

        group.bench_function(
            BenchmarkId::new("cold_query", format!("{row_count}_rows")),
            |b| {
                iter_custom_or_iter!(b, |iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        // Fresh connection = no cached directory
                        let conn = db.connect().unwrap();
                        let start = std::time::Instant::now();
                        let mut stmt = conn
                            .query(
                                "SELECT id, title FROM docs WHERE (title, body) MATCH 'database'",
                            )
                            .unwrap()
                            .unwrap();
                        let _rows = run_and_count_rows(&mut stmt, &db).unwrap();
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Warm FTS query (cached directory — measures query-only path)
///
/// After the first query loads and caches the directory, subsequent queries
/// skip the catalog scan and PreloadingEssentials entirely. This measures
/// the pure query execution path using the cached Index, Reader, Searcher, and
/// QueryParser.
#[turso_macros::codspeed_criterion_benchmark]
fn bench_fts_warm_query(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("FTS Warm Query");

    for row_count in [1000, 5000, 10000] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_fts_db(&temp_dir, row_count);
        let conn = db.connect().unwrap();

        // Warm up: run one query to populate the directory cache
        let mut stmt = conn
            .query("SELECT id FROM docs WHERE (title, body) MATCH 'database'")
            .unwrap()
            .unwrap();
        run_to_completion(&mut stmt, &db).unwrap();

        group.bench_function(
            BenchmarkId::new("warm_query", format!("{row_count}_rows")),
            |b| {
                iter_custom_or_iter!(b, |iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        let mut stmt = conn
                            .query(
                                "SELECT id, title FROM docs WHERE (title, body) MATCH 'database'",
                            )
                            .unwrap()
                            .unwrap();
                        let _rows = run_and_count_rows(&mut stmt, &db).unwrap();
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: warm queries alternating across active connections.
///
/// Each connection owns a snapshot cache. Alternating between readers should
/// remain close to the single-connection warm path instead of repeatedly
/// rescanning the FTS directory.
#[turso_macros::codspeed_criterion_benchmark]
fn bench_fts_connection_pool_query(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("FTS Connection Pool Query");
    let temp_dir = tempfile::tempdir().unwrap();
    let db = setup_fts_db(&temp_dir, 5_000);

    for connection_count in [1, 2, 4] {
        let connections = (0..connection_count)
            .map(|_| db.connect().unwrap())
            .collect::<Vec<_>>();
        for conn in &connections {
            let mut stmt = conn
                .query("SELECT id FROM docs WHERE (title, body) MATCH 'database'")
                .unwrap()
                .unwrap();
            run_to_completion(&mut stmt, &db).unwrap();
        }

        group.bench_function(
            BenchmarkId::new("alternating_warm_query", connection_count),
            |b| {
                iter_custom_or_iter!(b, |iters| {
                    let mut total = std::time::Duration::ZERO;
                    for iteration in 0..iters {
                        let conn = &connections[iteration as usize % connections.len()];
                        let start = std::time::Instant::now();
                        let mut stmt = conn
                            .query(
                                "SELECT id, title FROM docs \
                                 WHERE (title, body) MATCH 'database'",
                            )
                            .unwrap()
                            .unwrap();
                        let _rows = run_and_count_rows(&mut stmt, &db).unwrap();
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: FTS query with different search selectivity
///
/// Measures how the number of matching documents affects query time.
/// "database" matches ~1/7 of docs, "performance" matches ~1/7,
/// "database performance" (AND) matches fewer.
#[turso_macros::codspeed_criterion_benchmark]
fn bench_fts_query_selectivity(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("FTS Query Selectivity");

    let row_count = 10000;
    let temp_dir = tempfile::tempdir().unwrap();
    let db = setup_fts_db(&temp_dir, row_count);
    let conn = db.connect().unwrap();

    // Warm up
    let mut stmt = conn
        .query("SELECT id FROM docs WHERE (title, body) MATCH 'database'")
        .unwrap()
        .unwrap();
    run_to_completion(&mut stmt, &db).unwrap();

    let queries = [
        ("single_common_term", "database"),
        ("single_uncommon_term", "optimization"),
        ("two_term_and", "database engineering"),
        ("phrase_query", "\"database document\""),
    ];

    for (name, query_term) in queries {
        let sql = format!("SELECT id, title FROM docs WHERE (title, body) MATCH '{query_term}'");

        group.bench_function(BenchmarkId::new("selectivity", name), |b| {
            iter_custom_or_iter!(b, |iters| {
                let mut total = std::time::Duration::ZERO;
                for _ in 0..iters {
                    let start = std::time::Instant::now();
                    let mut stmt = conn.query(&sql).unwrap().unwrap();
                    let _rows = run_and_count_rows(&mut stmt, &db).unwrap();
                    total += start.elapsed();
                }
                total
            });
        });
    }

    group.finish();
}

/// Benchmark: Insert + query lifecycle
///
/// Measures the cost of inserting new rows, committing, and then querying.
/// This exercises the full write path (IndexWriter, segment creation, BTree flush)
/// followed by directory cache invalidation and a cold re-query.
#[turso_macros::codspeed_criterion_benchmark]
fn bench_fts_insert_then_query(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("FTS Insert+Query Lifecycle");
    group.sample_size(20);

    for row_count in [1000, 5000] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_fts_db(&temp_dir, row_count);
        let conn = db.connect().unwrap();

        // Use a shared counter that persists across warmup + sampling invocations
        let counter = std::cell::Cell::new(row_count + 1_000_000);

        group.bench_function(
            BenchmarkId::new("insert_query", format!("{row_count}_rows")),
            |b| {
                iter_custom_or_iter!(b, |iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();

                        // Insert 10 new rows (use rowid=NULL to auto-assign)
                        let c = counter.get();
                        let mut sql = String::from("INSERT INTO docs (id, title, body) VALUES ");
                        for j in 0..10 {
                            if j > 0 {
                                sql.push(',');
                            }
                            let id = c + j;
                            sql.push_str(&format!(
                                "({id}, 'new document {id}', 'freshly inserted content about database systems')"
                            ));
                        }
                        counter.set(c + 10);
                        conn.execute(&sql).unwrap();

                        // Query (exercises cache invalidation + re-query)
                        let mut stmt = conn
                            .query(
                                "SELECT id, title FROM docs WHERE (title, body) MATCH 'database'",
                            )
                            .unwrap()
                            .unwrap();
                        let _rows = run_and_count_rows(&mut stmt, &db).unwrap();

                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: top-k query after many single-row commits.
///
/// This isolates the read amplification caused by accumulating many small
/// Tantivy segments and guards the effectiveness of automatic maintenance.
#[turso_macros::codspeed_criterion_benchmark]
fn bench_fts_segment_churn_query(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("FTS Segment Churn");
    group.sample_size(20);

    for commit_count in [64, 256, 1024] {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = setup_fts_churn_db(&temp_dir, commit_count);
        let conn = db.connect().unwrap();
        let dense_sql = "SELECT fts_score(body, 'common') AS score, id \
                         FROM docs \
                         WHERE fts_match(body, 'common') \
                         ORDER BY score DESC LIMIT 10";
        let sparse_sql = "SELECT fts_score(body, 'needle') AS score, id \
                          FROM docs \
                          WHERE fts_match(body, 'needle') \
                          ORDER BY score DESC LIMIT 10";

        let mut stmt = conn.query(dense_sql).unwrap().unwrap();
        assert_eq!(run_and_count_rows(&mut stmt, &db).unwrap(), 10);
        let mut stmt = conn.query(sparse_sql).unwrap().unwrap();
        assert_eq!(run_and_count_rows(&mut stmt, &db).unwrap(), 1);

        group.bench_function(
            BenchmarkId::new("top_10_query", format!("{commit_count}_commits")),
            |b| {
                iter_custom_or_iter!(b, |iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        let mut stmt = conn.query(dense_sql).unwrap().unwrap();
                        assert_eq!(run_and_count_rows(&mut stmt, &db).unwrap(), 10);
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
        group.bench_function(
            BenchmarkId::new("sparse_top_10_query", format!("{commit_count}_commits")),
            |b| {
                iter_custom_or_iter!(b, |iters| {
                    let mut total = std::time::Duration::ZERO;
                    for _ in 0..iters {
                        let start = std::time::Instant::now();
                        let mut stmt = conn.query(sparse_sql).unwrap().unwrap();
                        assert_eq!(run_and_count_rows(&mut stmt, &db).unwrap(), 1);
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: sustained single-row commits.
///
/// Each measured iteration starts from an empty indexed table and executes a
/// fixed number of autocommit inserts. This includes every merge boundary in
/// that prefix and prevents warmup from growing the measured database.
#[turso_macros::codspeed_criterion_benchmark]
fn bench_fts_single_row_commit_churn(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("FTS Single Row Commit Churn");
    group.sample_size(10);

    // merge_threshold 0 disables the write-path auto-merge; the default (32)
    // pays for merges past the threshold. The pair isolates what B1's
    // auto-merge costs a single-row-commit workload.
    for (commit_count, merge_threshold) in [(64, 0), (64, 32), (256, 0), (256, 32)] {
        group.bench_function(
            BenchmarkId::new(
                "committed_rows",
                format!("{commit_count}_merge_threshold_{merge_threshold}"),
            ),
            |b| {
                iter_custom_or_iter!(b, |iters| {
                    let mut total = std::time::Duration::ZERO;
                    for repetition in 0..iters {
                        let temp_dir = tempfile::tempdir().unwrap();
                        let db = setup_fts_db(&temp_dir, 0);
                        let conn = db.connect().unwrap();
                        conn.execute(format!("PRAGMA fts_merge_threshold = {merge_threshold}"))
                            .unwrap();
                        let start = std::time::Instant::now();
                        for id in 0..commit_count {
                            let id = id as u64 + repetition * commit_count as u64;
                            conn.execute(format!(
                                "INSERT INTO docs (id, title, body) VALUES \
                                 ({id}, 'commit {id}', \
                                 'independently committed database document {id}')"
                            ))
                            .unwrap();
                        }
                        total += start.elapsed();
                    }
                    total
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: the first large tiered-merge boundary.
///
/// Seven 1,000-row commits leave seven segments. The eighth commit triggers
/// an 8,000-document merge, so the delta isolates foreground maintenance cost.
#[turso_macros::codspeed_criterion_benchmark]
fn bench_fts_large_merge_boundary(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("FTS Large Merge Boundary");
    group.sample_size(10);
    let rows_per_commit = 1_000;

    for commit_count in [7, 8] {
        group.bench_function(BenchmarkId::new("1000_row_commits", commit_count), |b| {
            iter_custom_or_iter!(b, |iters| {
                let mut total = std::time::Duration::ZERO;
                for repetition in 0..iters {
                    let temp_dir = tempfile::tempdir().unwrap();
                    let db = setup_fts_db(&temp_dir, 0);
                    let conn = db.connect().unwrap();
                    let statements = (0..commit_count)
                        .map(|commit| {
                            let first_id =
                                (repetition as usize * commit_count + commit) * rows_per_commit;
                            let mut sql =
                                String::from("INSERT INTO docs (id, title, body) VALUES ");
                            for offset in 0..rows_per_commit {
                                if offset > 0 {
                                    sql.push(',');
                                }
                                let id = first_id + offset;
                                sql.push_str(&format!(
                                    "({id}, 'document {id}', \
                                         'database content for merged document {id}')"
                                ));
                            }
                            sql
                        })
                        .collect::<Vec<_>>();

                    let start = std::time::Instant::now();
                    for sql in statements {
                        conn.execute(sql).unwrap();
                    }
                    total += start.elapsed();
                }
                total
            });
        });
    }

    group.finish();
}

#[cfg(not(feature = "codspeed"))]
criterion_group! {
    name = fts_benches;
    config = Criterion::default()
        .with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)))
        .sample_size(50);
    targets = bench_fts_cold_query, bench_fts_warm_query, bench_fts_connection_pool_query, bench_fts_query_selectivity, bench_fts_insert_then_query, bench_fts_segment_churn_query, bench_fts_single_row_commit_churn, bench_fts_large_merge_boundary
}

#[cfg(feature = "codspeed")]
criterion_group! {
    name = fts_benches;
    config = Criterion::default().sample_size(50);
    targets = bench_fts_cold_query, bench_fts_warm_query, bench_fts_connection_pool_query, bench_fts_query_selectivity, bench_fts_insert_then_query, bench_fts_segment_churn_query, bench_fts_single_row_commit_churn, bench_fts_large_merge_boundary
}

criterion_main!(fts_benches);
