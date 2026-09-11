//! Microbenchmark isolating the per-row COUNT aggregate path (op_agg_step).
//!
//! Drives turso_core in-process over a seeded table and times COUNT in the three shapes
//! that go row-by-row through op_agg_step (and therefore exercise the per-row argument
//! handling that the COUNT fast path optimizes):
//!   - count_filtered : SELECT COUNT(*) FROM t WHERE g <= 'group-07'  (covering-index range)
//!   - count_text_col : SELECT COUNT(g) FROM t                        (covering index, COUNT over TEXT)
//!   - count_groupby  : SELECT g, COUNT(*) FROM t GROUP BY g          (covering-index streaming count)
//!
//! The unfiltered whole-table `SELECT COUNT(*) FROM t` is deliberately NOT benchmarked: it
//! takes the OP_Count btree shortcut and never enters op_agg_step.
//!
//! Run:  cargo bench -p turso_core --bench count_benchmark

#[cfg(feature = "codspeed")]
use codspeed_criterion_compat::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion,
};
#[cfg(not(feature = "codspeed"))]
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use turso_core::{Database, PlatformIO, SqliteDialect, StepResult};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const N: usize = 1_000_000;

// The first three drive the count through the covering index on `g` (small, resident working set)
// so the measurement isolates the per-row aggregate path and stays stable under memory
// pressure -- a full table scan thrashes the OS memory compressor and is too noisy to A/B.
const QUERIES: &[(&str, &str)] = &[
    // count with a clause: index range over ~half the rows, count(*).
    (
        "count_filtered",
        "SELECT COUNT(*) FROM t WHERE g <= 'group-07'",
    ),
    // count of a text column: covering-index scan, COUNT(text) -- the per-row arg is a TEXT
    // value, which the pre-fix code cloned (heap alloc) every row.
    ("count_text_col", "SELECT COUNT(g) FROM t"),
    // count with group by: covering-index streaming aggregate.
    ("count_groupby", "SELECT g, COUNT(*) FROM t GROUP BY g"),
    // count of a NOT NULL column: no NULLs to check for, so it's as fast as count(*).
    ("count_notnull_col", "SELECT COUNT(v) FROM t"),
];

/// Seed a self-contained db file via rusqlite. `g` has low cardinality (16 groups) and is
/// indexed so every benchmarked query runs through the covering index on `g`; `payload` and
/// `v` are unused by the queries (they just give the rows realistic width), except
/// count_notnull_col that counts v and needs it NOT NULL.
fn seed_db(n: usize) -> TempDir {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("count.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch(
        "PRAGMA journal_mode=DELETE;
         CREATE TABLE t(id INTEGER PRIMARY KEY, g TEXT, payload TEXT, v INTEGER NOT NULL);",
    )
    .unwrap();
    let tx = conn.unchecked_transaction().unwrap();
    {
        let mut ins = tx
            .prepare("INSERT INTO t(id, g, payload, v) VALUES (?1, ?2, ?3, ?4)")
            .unwrap();
        for i in 1..=n as i64 {
            let g = format!("group-{:02}", i % 16);
            let payload = format!("row-payload-{i}");
            ins.execute((i, g, payload, i)).unwrap();
        }
    }
    tx.commit().unwrap();
    conn.execute_batch("CREATE INDEX idx_t_g ON t(g);").unwrap();
    dir
}

fn drain_turso(db: &Database, stmt: &mut turso_core::Statement) {
    loop {
        match stmt.step().unwrap() {
            StepResult::Row => {
                black_box(stmt.row());
            }
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                db.io.step().unwrap();
            }
            StepResult::Done => break,
            StepResult::Interrupt | StepResult::Busy => unreachable!(),
        }
    }
    stmt.reset().unwrap();
}

#[turso_macros::codspeed_criterion_benchmark]
fn bench_count(criterion: &mut Criterion) {
    let dir = seed_db(N);
    let path = dir.path().join("count.db");

    let mut group = criterion.benchmark_group("count");
    // Stability: each query runs ~1M aggregate steps (tens of ms); give criterion a long
    // measurement window and a healthy sample count so run-to-run variance stays low.
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(12));
    group.warm_up_time(Duration::from_secs(3));

    for (label, sql) in QUERIES {
        #[allow(clippy::arc_with_non_send_sync)]
        let io = Arc::new(PlatformIO::new().unwrap());
        let db = Database::open_file(io, path.to_str().unwrap(), Arc::new(SqliteDialect)).unwrap();
        let conn = db.connect().unwrap();
        // Cache sized to hold the ~15MB covering index (the queries' whole working set) so the
        // measurement is CPU-bound, while staying small enough to avoid paging under memory
        // pressure -- both are needed for a stable result.
        {
            let mut p = conn.prepare("PRAGMA cache_size=-32768").unwrap();
            while !matches!(p.step().unwrap(), StepResult::Done) {
                db.io.step().unwrap();
            }
        }
        group.bench_with_input(BenchmarkId::new(*label, N), &N, |b, _| {
            let mut stmt = conn.prepare(sql).unwrap();
            b.iter(|| drain_turso(&db, &mut stmt));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_count);
criterion_main!(benches);
