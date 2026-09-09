use anyhow::{Result, ensure};
use clap::ValueEnum;
use rand::{RngCore, SeedableRng, rngs::StdRng};
use serde::Serialize;
use std::fmt::Write;
use std::sync::Arc;
use tempfile::TempDir;
use turso::{Connection, Database};
use turso_core::IO;

use crate::workload::JournalMode;

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueryCase {
    Rare,
    Common,
    And,
    Or,
    Phrase,
    Ranked,
}

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum QueryState {
    First,
    Warm,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum FtsPhase {
    Setup,
    Open,
    Warmup,
    Run,
    Cleanup,
    Done,
}

pub trait FtsObserver {
    fn on_phase(&mut self, _phase: FtsPhase) {}
    fn on_index(&mut self, _stats: &IndexStats) {}
    fn after_batch(&mut self, _progress: &RunResult) {}
}

impl FtsObserver for () {}

#[derive(Clone, Copy, Debug)]
pub enum Execution {
    Queries(usize),
    Transactions {
        per_connection: usize,
        queries_per_transaction: usize,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct FtsConfig {
    pub query: QueryCase,
    pub state: QueryState,
    pub documents: usize,
    pub corpus: CorpusConfig,
    pub mode: JournalMode,
    pub connections: usize,
    pub execution: Execution,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct CorpusConfig {
    pub extra_tokens: usize,
    pub cache_pages: Option<usize>,
    pub min_index_bytes: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct IndexStats {
    pub segment_bytes: usize,
    pub segments: usize,
    pub largest_segment_bytes: usize,
    pub page_size: usize,
    pub configured_cache_pages: Option<usize>,
}

pub struct FtsWorkload {
    sessions: Vec<QuerySession>,
    _fixture: FtsFixture,
    config: FtsConfig,
}

#[derive(Default, Debug, Serialize)]
pub struct RunResult {
    pub queries: usize,
    pub transactions: usize,
    pub rows: usize,
    pub id_sum: i64,
    pub max_active_transactions: usize,
}

pub struct FtsFixture {
    directory: TempDir,
}

#[derive(Clone)]
pub struct QuerySession {
    conn: Connection,
    _db: Database,
}

#[derive(Debug, PartialEq, Eq)]
pub struct QueryResult {
    pub rows: usize,
    pub id_sum: i64,
}

pub async fn run_fts(config: FtsConfig, observer: &mut dyn FtsObserver) -> Result<RunResult> {
    let workload = FtsWorkload::prepare(config, observer).await?;
    let result = workload.run(observer).await;
    workload.finish(observer);
    result
}

impl FtsWorkload {
    pub async fn prepare(config: FtsConfig, observer: &mut dyn FtsObserver) -> Result<Self> {
        config.validate()?;
        observer.on_phase(FtsPhase::Setup);
        let fixture =
            FtsFixture::create_in_mode(config.documents, config.mode, config.corpus).await?;
        let stats = fixture.index_stats(config.corpus.cache_pages)?;
        observer.on_index(&stats);
        ensure!(
            stats.segment_bytes >= config.corpus.min_index_bytes,
            "index contains {} segment bytes, below requested minimum {}; increase documents or extra tokens",
            stats.segment_bytes,
            config.corpus.min_index_bytes
        );
        observer.on_phase(FtsPhase::Open);
        let first = fixture.open().await?;
        let mut sessions = Vec::with_capacity(config.connections);
        for _ in 1..config.connections {
            sessions.push(QuerySession {
                conn: first._db.connect()?,
                _db: first._db.clone(),
            });
        }
        sessions.push(first);
        if let Some(pages) = config.corpus.cache_pages {
            for session in &sessions {
                session
                    .conn
                    .pragma_update("cache_size", &pages.to_string())
                    .await?;
            }
        }
        let workload = Self {
            sessions,
            _fixture: fixture,
            config,
        };
        observer.on_phase(FtsPhase::Warmup);
        if matches!(config.state, QueryState::Warm) {
            workload.batch(1).await?;
        }
        Ok(workload)
    }

    pub async fn run(&self, observer: &mut dyn FtsObserver) -> Result<RunResult> {
        observer.on_phase(FtsPhase::Run);
        let mut progress = RunResult::default();
        for _ in 0..self.config.execution.batches() {
            let batch = self
                .batch(self.config.execution.queries_per_batch())
                .await?;
            progress.queries += batch.queries;
            progress.transactions += batch.transactions;
            progress.rows += batch.rows;
            progress.id_sum += batch.id_sum;
            progress.max_active_transactions = progress
                .max_active_transactions
                .max(batch.max_active_transactions);
            observer.after_batch(&progress);
        }
        Ok(progress)
    }

    pub fn finish(self, observer: &mut dyn FtsObserver) {
        observer.on_phase(FtsPhase::Cleanup);
        drop(self);
        observer.on_phase(FtsPhase::Done);
    }

    async fn batch(&self, queries: usize) -> Result<RunResult> {
        let transactions = matches!(self.config.execution, Execution::Transactions { .. });
        let outcome = async {
            let mut active = 0;
            if transactions {
                let begin = match self.config.mode {
                    JournalMode::Wal => "BEGIN",
                    JournalMode::Mvcc => "BEGIN CONCURRENT",
                };
                for session in &self.sessions {
                    session.begin(begin).await?;
                }
                for session in &self.sessions {
                    active += usize::from(!session.conn.is_autocommit()?);
                }
                ensure!(
                    active == self.sessions.len(),
                    "transactions must overlap before queries start"
                );
            }
            let mut result = self.query_all(queries).await?;
            if transactions {
                for session in &self.sessions {
                    session.commit().await?;
                    ensure!(
                        session.conn.is_autocommit()?,
                        "COMMIT must end the transaction"
                    );
                }
                result.transactions = self.sessions.len();
                result.max_active_transactions = active;
            }
            Ok(result)
        }
        .await;
        if outcome.is_err() {
            for session in &self.sessions {
                if !session.conn.is_autocommit()? {
                    session.conn.execute("ROLLBACK", ()).await?;
                }
            }
        }
        outcome
    }

    async fn query_all(&self, queries: usize) -> Result<RunResult> {
        if self.sessions.len() == 1 {
            return self.sessions[0]
                .query_batch(self.config.query, queries)
                .await;
        }
        let mut workers = tokio::task::JoinSet::new();
        for session in &self.sessions {
            let session = session.clone();
            let case = self.config.query;
            workers.spawn(async move { session.query_batch(case, queries).await });
        }
        let result = async {
            let mut result = RunResult::default();
            while let Some(worker) = workers.join_next().await {
                let batch = worker??;
                result.queries += batch.queries;
                result.rows += batch.rows;
                result.id_sum += batch.id_sum;
            }
            Ok(result)
        }
        .await;
        if result.is_err() {
            workers.shutdown().await;
        }
        result
    }
}

impl FtsConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.documents > 0, "documents must be positive");
        ensure!(
            self.corpus
                .cache_pages
                .is_none_or(|pages| (200..=i32::MAX as usize).contains(&pages)),
            "cache pages must be between 200 and i32::MAX"
        );
        ensure!(self.connections > 0, "connections must be positive");
        ensure!(
            self.execution.batches() > 0,
            "queries or transactions must be positive"
        );
        ensure!(
            self.execution.queries_per_batch() > 0,
            "queries per transaction must be positive"
        );
        ensure!(
            !matches!(self.state, QueryState::First)
                || (self.execution.batches() == 1 && self.execution.queries_per_batch() == 1),
            "first-query runs require one query per connection; use warm for repeated transactions"
        );
        self.execution
            .batches()
            .checked_mul(self.execution.queries_per_batch())
            .and_then(|n| n.checked_mul(self.connections))
            .ok_or_else(|| anyhow::anyhow!("total query count overflows usize"))?;
        Ok(())
    }
}

impl Execution {
    pub fn batches(self) -> usize {
        match self {
            Self::Queries(queries) => queries,
            Self::Transactions { per_connection, .. } => per_connection,
        }
    }

    fn queries_per_batch(self) -> usize {
        match self {
            Self::Queries(_) => 1,
            Self::Transactions {
                queries_per_transaction,
                ..
            } => queries_per_transaction,
        }
    }
}

impl FtsFixture {
    pub async fn create(documents: usize) -> Result<Self> {
        Self::create_in_mode(documents, JournalMode::Wal, CorpusConfig::default()).await
    }

    async fn create_in_mode(
        documents: usize,
        mode: JournalMode,
        corpus: CorpusConfig,
    ) -> Result<Self> {
        ensure!(documents > 0, "documents must be positive");
        let fixture = Self {
            directory: tempfile::tempdir()?,
        };
        let session = fixture.open().await?;
        let mode = match mode {
            JournalMode::Wal => "'wal'",
            JournalMode::Mvcc => "'mvcc'",
        };
        session.conn.pragma_update("journal_mode", mode).await?;
        session
            .conn
            .execute(
                "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, body TEXT)",
                (),
            )
            .await?;
        let mut rng = StdRng::seed_from_u64(0x4654_534d);
        for start in (0..documents).step_by(500) {
            session.conn.execute("BEGIN", ()).await?;
            let mut insert = session
                .conn
                .prepare("INSERT INTO docs VALUES (?1, ?2, ?3)")
                .await?;
            for id in start..documents.min(start + 500) {
                let rare = if id % 100 == 0 { "rare" } else { "plain" };
                let alpha = if id % 2 == 0 { "alpha" } else { "gamma" };
                let beta = if id % 3 == 0 { "beta" } else { "delta" };
                let prefix = if id % 200 == 100 {
                    "rare common".to_string()
                } else {
                    format!("common {rare}")
                };
                let mut body = format!("{prefix} {alpha} {beta} search document storage text");
                for _ in 0..corpus.extra_tokens {
                    write!(body, " x{:016x}", rng.next_u64())?;
                }
                insert
                    .execute(turso::params![id as i64, format!("document {id:08}"), body])
                    .await?;
            }
            session.conn.execute("COMMIT", ()).await?;
        }
        session
            .conn
            .execute("CREATE INDEX docs_fts ON docs USING fts (title, body)", ())
            .await?;
        for case in QueryCase::value_variants() {
            session.check_index(*case).await?;
        }
        Ok(fixture)
    }

    fn index_stats(&self, cache_pages: Option<usize>) -> Result<IndexStats> {
        let io = Arc::new(turso_core::PlatformIO::new()?);
        let db = turso_core::Database::open(
            io.clone(),
            self.directory.path().join("fts.db").to_str().unwrap(),
            turso_core::OpenOptions::new(Arc::new(turso_core::SqliteDialect))
                .db_opts(turso_core::DatabaseOpts::new().with_index_method(true)),
        )?;
        let conn = db.connect()?;
        conn.execute("BEGIN")?;
        conn.execute("SELECT count(*) FROM docs")?;
        let mut dumper = turso_core::index_method::fts::FtsBackingRowDumper::new(
            &conn,
            turso_core::MAIN_DB_ID,
            "docs_fts",
        )?;
        loop {
            match dumper.step()? {
                turso_core::IOResult::Done(()) => break,
                turso_core::IOResult::IO(completions) => {
                    while !completions.finished() {
                        io.step()?;
                    }
                }
            }
        }
        let mut sizes = std::collections::BTreeMap::<&str, usize>::new();
        let mut segments = 0;
        for (path, _, bytes, _) in &dumper.rows {
            if let Some(suffix) = path.strip_prefix("fts2/chunk/") {
                let (segment, _) = suffix.split_once('/').expect("segment chunk path");
                *sizes.entry(segment).or_default() += bytes;
            } else if path.starts_with("fts2/seg/") {
                segments += 1;
            }
        }
        ensure!(
            segments > 0 && sizes.len() == segments,
            "missing FTS segment data"
        );
        let mut page_size = 0;
        conn.query("PRAGMA page_size")?
            .expect("page size query")
            .run_with_row_callback(|row| {
                page_size = row.get::<i64>(0)? as usize;
                Ok(())
            })?;
        let stats = IndexStats {
            segment_bytes: sizes.values().sum(),
            segments,
            largest_segment_bytes: *sizes.values().max().unwrap(),
            page_size,
            configured_cache_pages: cache_pages,
        };
        drop(dumper);
        conn.execute("ROLLBACK")?;
        conn.close()?;
        Ok(stats)
    }

    pub async fn session(&self, case: QueryCase, state: QueryState) -> Result<QuerySession> {
        let session = self.open().await?;
        if matches!(state, QueryState::Warm) {
            session.query(case).await?;
        }
        Ok(session)
    }

    async fn open(&self) -> Result<QuerySession> {
        let path = self.directory.path().join("fts.db");
        let db = turso::Builder::new_local(path.to_str().unwrap())
            .experimental_index_method(true)
            .build()
            .await?;
        let conn = db.connect()?;
        Ok(QuerySession { conn, _db: db })
    }
}

impl QuerySession {
    async fn begin(&self, sql: &str) -> Result<()> {
        self.conn.execute(sql, ()).await?;
        Ok(())
    }

    async fn commit(&self) -> Result<()> {
        self.conn.execute("COMMIT", ()).await?;
        Ok(())
    }

    async fn query_batch(&self, case: QueryCase, queries: usize) -> Result<RunResult> {
        let mut result = RunResult::default();
        for _ in 0..queries {
            let query = std::hint::black_box(self.query(case).await?);
            result.queries += 1;
            result.rows += query.rows;
            result.id_sum += query.id_sum;
        }
        Ok(result)
    }

    pub async fn query(&self, case: QueryCase) -> Result<QueryResult> {
        let mut rows = self.conn.query(case.sql(), ()).await?;
        let mut result = QueryResult { rows: 0, id_sum: 0 };
        while let Some(row) = rows.next().await? {
            result.rows += 1;
            result.id_sum += row.get::<i64>(0)?;
        }
        Ok(result)
    }

    async fn check_index(&self, case: QueryCase) -> Result<()> {
        let mut rows = self
            .conn
            .query(&format!("EXPLAIN QUERY PLAN {}", case.sql()), ())
            .await?;
        let mut indexed = false;
        let mut details = Vec::new();
        while let Some(row) = rows.next().await? {
            let detail = row.get::<String>(3)?;
            indexed |= detail == "QUERY INDEX METHOD fts";
            details.push(detail);
        }
        ensure!(indexed, "FTS index not used for {case:?}: {details:?}");
        Ok(())
    }
}

impl QueryCase {
    pub fn sql(self) -> &'static str {
        match self {
            Self::Rare => "SELECT id FROM docs WHERE fts_match(title, body, 'rare')",
            Self::Common => "SELECT id FROM docs WHERE fts_match(title, body, 'common')",
            Self::And => "SELECT id FROM docs WHERE fts_match(title, body, 'alpha AND beta')",
            Self::Or => "SELECT id FROM docs WHERE fts_match(title, body, 'alpha OR beta')",
            Self::Phrase => "SELECT id FROM docs WHERE fts_match(title, body, '\"common rare\"')",
            Self::Ranked => {
                "SELECT id, fts_score(title, body, 'alpha OR beta') AS score FROM docs WHERE fts_match(title, body, 'alpha OR beta') ORDER BY score DESC LIMIT 10"
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn index_size_includes_segments_on_both_sides_of_document_flush() -> Result<()> {
        for (documents, segments) in [(1000, 1), (1001, 2)] {
            let fixture = FtsFixture::create(documents).await?;
            let stats = fixture.index_stats(None)?;
            assert_eq!(stats.segments, segments);
            assert_eq!(stats.page_size, 4096);
            assert!(stats.largest_segment_bytes > 0);
            if segments == 1 {
                assert_eq!(stats.segment_bytes, stats.largest_segment_bytes);
            } else {
                assert!(stats.segment_bytes > stats.largest_segment_bytes);
            }
            let session = fixture
                .session(QueryCase::Common, QueryState::First)
                .await?;
            assert_eq!(session.query(QueryCase::Common).await?.rows, documents);
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mvcc_transactions_overlap_repeat_and_finish_before_cleanup() -> Result<()> {
        let mut observer = RecordingObserver::default();
        let config = FtsConfig {
            query: QueryCase::And,
            state: QueryState::Warm,
            documents: 103,
            corpus: CorpusConfig {
                extra_tokens: 1024,
                cache_pages: Some(200),
                min_index_bytes: 200 * 4096 + 1,
            },
            mode: JournalMode::Mvcc,
            connections: 2,
            execution: Execution::Transactions {
                per_connection: 3,
                queries_per_transaction: 2,
            },
        };
        let workload = FtsWorkload::prepare(config, &mut observer).await?;
        let mut mode = workload.sessions[0]
            .conn
            .query("PRAGMA journal_mode", ())
            .await?;
        assert_eq!(mode.next().await?.unwrap().get::<String>(0)?, "mvcc");
        drop(mode);
        let result = workload.run(&mut observer).await?;
        assert_eq!(result.queries, 12);
        assert_eq!(result.transactions, 6);
        assert_eq!(result.max_active_transactions, 2);
        assert_eq!(result.rows, 216);
        assert_eq!(result.id_sum, 11_016);
        assert_eq!(observer.batches, [(4, 2), (8, 4), (12, 6)]);
        for session in &workload.sessions {
            assert!(session.conn.is_autocommit()?);
            let mut cache = session.conn.query("PRAGMA cache_size", ()).await?;
            assert_eq!(cache.next().await?.unwrap().get::<i64>(0)?, 200);
        }
        workload.finish(&mut observer);
        assert_eq!(
            observer.phases,
            [
                FtsPhase::Setup,
                FtsPhase::Open,
                FtsPhase::Warmup,
                FtsPhase::Run,
                FtsPhase::Cleanup,
                FtsPhase::Done
            ]
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failed_query_workers_finish_before_transactions_are_rolled_back() -> Result<()> {
        let config = FtsConfig {
            query: QueryCase::Common,
            state: QueryState::First,
            documents: 5,
            corpus: CorpusConfig::default(),
            mode: JournalMode::Mvcc,
            connections: 2,
            execution: Execution::Transactions {
                per_connection: 1,
                queries_per_transaction: 1,
            },
        };
        let workload = FtsWorkload::prepare(config, &mut ()).await?;
        workload.sessions[0]
            .conn
            .execute("DROP TABLE docs", ())
            .await?;
        let mut observer = RecordingObserver::default();
        assert!(workload.run(&mut observer).await.is_err());
        assert!(observer.batches.is_empty());
        for session in &workload.sessions {
            assert!(session.conn.is_autocommit()?);
            session.begin("BEGIN CONCURRENT").await?;
            session.commit().await?;
        }
        workload.finish(&mut observer);
        assert_eq!(
            observer.phases,
            [FtsPhase::Run, FtsPhase::Cleanup, FtsPhase::Done]
        );
        Ok(())
    }

    #[tokio::test]
    async fn query_cases_return_expected_rows_after_reopening_and_warming() -> Result<()> {
        let fixture = FtsFixture::create(103).await?;
        for state in [QueryState::First, QueryState::Warm] {
            for (case, count, sum) in [
                (QueryCase::Rare, 2, 100),
                (QueryCase::Common, 103, 5253),
                (QueryCase::And, 18, 918),
                (QueryCase::Or, 69, 3519),
                (QueryCase::Phrase, 1, 0),
            ] {
                let session = fixture.session(case, state).await?;
                for _ in 0..2 {
                    assert_eq!(
                        session.query(case).await?,
                        QueryResult {
                            rows: count,
                            id_sum: sum
                        }
                    );
                }
            }
            let session = fixture.session(QueryCase::Ranked, state).await?;
            let mut rows = session.conn.query(QueryCase::Ranked.sql(), ()).await?;
            let mut previous = f64::INFINITY;
            let mut ids = Vec::new();
            while let Some(row) = rows.next().await? {
                let id = row.get::<i64>(0)?;
                let score = row.get::<f64>(1)?;
                assert!((0..103).contains(&id));
                assert_eq!(id % 6, 0);
                assert!(!ids.contains(&id));
                assert!(score > 0.0 && score <= previous);
                previous = score;
                ids.push(id);
            }
            assert_eq!(ids.len(), 10);
        }
        Ok(())
    }

    #[tokio::test]
    async fn ranked_limit_handles_fewer_than_ten_matches() -> Result<()> {
        let fixture = FtsFixture::create(5).await?;
        let session = fixture
            .session(QueryCase::Ranked, QueryState::First)
            .await?;
        assert_eq!(
            session.query(QueryCase::Ranked).await?,
            QueryResult { rows: 4, id_sum: 9 }
        );
        Ok(())
    }

    #[derive(Default)]
    struct RecordingObserver {
        phases: Vec<FtsPhase>,
        batches: Vec<(usize, usize)>,
    }

    impl FtsObserver for RecordingObserver {
        fn on_phase(&mut self, phase: FtsPhase) {
            self.phases.push(phase);
        }

        fn after_batch(&mut self, progress: &RunResult) {
            assert_eq!(self.phases.last(), Some(&FtsPhase::Run));
            self.batches.push((progress.queries, progress.transactions));
        }
    }
}
