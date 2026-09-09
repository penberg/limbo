use std::time::Duration;

use anyhow::Result;
use clap::ValueEnum;
use turso::Connection;
use turso::params::Params;

use super::profile::{
    Phase, Profile, WorkItem, checkpoint::Checkpoint, insert::InsertHeavy, mixed::Mixed,
    read::ReadHeavy, recursive_cte::RecursiveCte, scan::ScanHeavy, series_blob::SeriesBlob,
    update_churn::UpdateChurn,
};

#[derive(Debug, Clone, Copy, ValueEnum, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum JournalMode {
    Wal,
    Mvcc,
}

impl std::fmt::Display for JournalMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JournalMode::Wal => write!(f, "wal"),
            JournalMode::Mvcc => write!(f, "mvcc"),
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum WorkloadProfile {
    InsertHeavy,
    ReadHeavy,
    Mixed,
    ScanHeavy,
    RecursiveCte,
    SeriesBlob,
    UpdateChurn,
}

impl std::fmt::Display for WorkloadProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkloadProfile::InsertHeavy => write!(f, "insert-heavy"),
            WorkloadProfile::ReadHeavy => write!(f, "read-heavy"),
            WorkloadProfile::Mixed => write!(f, "mixed"),
            WorkloadProfile::ScanHeavy => write!(f, "scan-heavy"),
            WorkloadProfile::RecursiveCte => write!(f, "recursive-cte"),
            WorkloadProfile::SeriesBlob => write!(f, "series-blob"),
            WorkloadProfile::UpdateChurn => write!(f, "update-churn"),
        }
    }
}

/// Parameters for a single workload run.
pub struct WorkloadConfig {
    pub mode: JournalMode,
    pub workload: WorkloadProfile,
    pub iterations: usize,
    pub batch_size: usize,
    pub connections: usize,
    pub timeout: Duration,
    pub cache_size: Option<i64>,
    pub checkpoint: bool,
    /// MVCC logical-log auto-checkpoint threshold in bytes (`PRAGMA
    /// mvcc_checkpoint_threshold`). Lowering it below the amount of log the
    /// workload writes guarantees automatic checkpoints fire during the run.
    /// Only meaningful in MVCC mode; WAL's 1000-frame threshold is not
    /// configurable.
    pub mvcc_checkpoint_threshold: Option<i64>,
    /// MVCC inline-GC trigger threshold in live-version growth (`PRAGMA
    /// mvcc_gc_threshold`; -1 disables inline GC). Only meaningful in MVCC mode.
    pub mvcc_gc_threshold: Option<i64>,
}

/// Hooks invoked at workload milestones so callers can take measurements.
/// `on_phase` fires once per phase transition; `after_batch` fires after every
/// batch of work completes.
pub trait WorkloadObserver {
    fn on_phase(&mut self, _phase: Phase) {}
    fn after_batch(&mut self) {}
}

/// No-op observer for callers that only need the workload executed.
impl WorkloadObserver for () {}

pub fn create_profile(
    workload: WorkloadProfile,
    iterations: usize,
    batch_size: usize,
    checkpoint: bool,
) -> Box<dyn Profile> {
    let profile: Box<dyn Profile> = match workload {
        WorkloadProfile::InsertHeavy => Box::new(InsertHeavy::new(iterations, batch_size)),
        WorkloadProfile::ReadHeavy => Box::new(ReadHeavy::new(iterations, batch_size)),
        WorkloadProfile::Mixed => Box::new(Mixed::new(iterations, batch_size)),
        WorkloadProfile::ScanHeavy => Box::new(ScanHeavy::new(iterations, batch_size)),
        WorkloadProfile::RecursiveCte => Box::new(RecursiveCte::new(iterations, batch_size)),
        WorkloadProfile::SeriesBlob => Box::new(SeriesBlob::new(iterations, batch_size)),
        WorkloadProfile::UpdateChurn => Box::new(UpdateChurn::new(iterations, batch_size)),
    };

    if checkpoint {
        Box::new(Checkpoint::new(profile))
    } else {
        profile
    }
}

/// Execute the configured workload against a fresh database at `db_path`,
/// driving the profile through its Setup/Run/Checkpoint phases.
pub async fn run_workload(
    db_path: &str,
    cfg: &WorkloadConfig,
    observer: &mut dyn WorkloadObserver,
) -> Result<String> {
    let mut profile = create_profile(cfg.workload, cfg.iterations, cfg.batch_size, cfg.checkpoint);
    let workload_name = profile.name().to_string();

    let db = turso::Builder::new_local(db_path).build().await?;

    // Setup connection for schema/seeding and journal mode
    let setup_conn = db.connect()?;
    setup_conn.busy_timeout(cfg.timeout)?;

    let mode_str = match cfg.mode {
        JournalMode::Wal => "'wal'",
        JournalMode::Mvcc => "'mvcc'",
    };
    setup_conn.pragma_update("journal_mode", mode_str).await?;

    if let Some(cache_size) = cfg.cache_size {
        setup_conn
            .pragma_update("cache_size", &cache_size.to_string())
            .await?;
    }

    if let Some(threshold) = cfg.mvcc_checkpoint_threshold {
        setup_conn
            .pragma_update("mvcc_checkpoint_threshold", &threshold.to_string())
            .await?;
    }

    if let Some(threshold) = cfg.mvcc_gc_threshold {
        setup_conn
            .pragma_update("mvcc_gc_threshold", &threshold.to_string())
            .await?;
    }

    let begin_stmt = match cfg.mode {
        JournalMode::Wal => "BEGIN",
        JournalMode::Mvcc => "BEGIN CONCURRENT",
    };

    let mut last_phase = None;

    loop {
        let (phase, batches) = profile.next_batch(cfg.connections);

        if phase == Phase::Done {
            break;
        }

        if last_phase != Some(phase) {
            observer.on_phase(phase);
            last_phase = Some(phase);
        }

        if batches.is_empty() {
            continue;
        }

        match phase {
            Phase::Setup => {
                // Setup runs sequentially on a single connection.
                let items = batches.into_iter().next().unwrap_or_default();
                if !items.is_empty() {
                    setup_conn.execute("BEGIN", ()).await?;
                    execute_items(&setup_conn, items).await?;
                    setup_conn.execute("COMMIT", ()).await?;
                }
            }
            Phase::Run => {
                // Run phase: dispatch batches concurrently across connections.
                let mut handles = Vec::with_capacity(batches.len());
                for items in batches {
                    if items.is_empty() {
                        continue;
                    }
                    let conn = db.connect()?;
                    conn.busy_timeout(cfg.timeout)?;
                    let begin = begin_stmt.to_string();
                    handles.push(tokio::spawn(async move {
                        conn.execute(&begin, ()).await?;
                        execute_items(&conn, items).await?;
                        conn.execute("COMMIT", ()).await?;
                        Ok::<_, turso::Error>(())
                    }));
                }
                for handle in handles {
                    handle.await??;
                }
            }
            Phase::Checkpoint => {
                let items = batches.into_iter().next().unwrap_or_default();
                if !items.is_empty() {
                    execute_checkpoint_items(&setup_conn, items).await?;
                }
            }
            Phase::Done => unreachable!(),
        }

        observer.after_batch();
    }

    Ok(workload_name)
}

async fn execute_items(conn: &Connection, items: Vec<WorkItem>) -> Result<(), turso::Error> {
    for item in items {
        let is_query = item
            .sql
            .trim_start()
            .get(..6)
            .is_some_and(|s| s.eq_ignore_ascii_case("SELECT"));
        if is_query {
            let mut rows = conn
                .query(&item.sql, Params::Positional(item.params))
                .await?;
            while rows.next().await?.is_some() {}
        } else if item.params.is_empty() {
            conn.execute(&item.sql, ()).await?;
        } else {
            let mut stmt = conn.prepare(&item.sql).await?;
            stmt.execute(Params::Positional(item.params)).await?;
        }
    }
    Ok(())
}

async fn execute_checkpoint_items(
    conn: &Connection,
    items: Vec<WorkItem>,
) -> Result<(), turso::Error> {
    for item in items {
        let mut rows = conn
            .query(&item.sql, Params::Positional(item.params))
            .await?;
        while rows.next().await?.is_some() {}
    }
    Ok(())
}

pub fn clean_db_files(db_path: &str) {
    for suffix in ["", "-wal", "-shm", "-journal", "-log"] {
        let path = if suffix.is_empty() {
            db_path.to_string()
        } else {
            format!("{db_path}{suffix}")
        };
        if std::path::Path::new(&path).exists() {
            let _ = std::fs::remove_file(&path);
        }
    }
}
