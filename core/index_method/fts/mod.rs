//! Full-text search index method: Tantivy over segment-registry storage.
//!
//! Storage model (format v2, see [`format`]): writers only ever add
//! immutable segments; the only shared mutable state is the segment
//! registry, and registry entries are ordinary MVCC-versioned rows in the
//! backing B-tree. Each transaction's view of the index is its snapshot's
//! view of the registry, appends by different transactions commute, and
//! rollback is automatic. Deletes are MVCC-versioned tombstone rows per
//! `(segment, doc)`; merges are the only operation that retires other
//! transactions' rows and are serialized by the per-index lease.
//!
//! Under MVCC this allows multiple `BEGIN CONCURRENT` transactions to write
//! the same FTS index concurrently. In WAL mode the same format runs with
//! degenerate concurrency: the pager write lock serializes writers.

use crate::sync::{Arc, Weak};
use crate::types::IOResultOr;
use crate::{
    index_method::{
        open_index_cursor, parse_patterns, IndexMethod, IndexMethodAttachment,
        IndexMethodConfiguration, IndexMethodContext, IndexMethodCursor, IndexMethodDefinition,
    },
    return_if_io,
    schema::IndexColumn,
    storage::btree::CursorTrait,
    translate::collate::CollationSeq,
    turso_assert,
    types::{IOResult, KeyInfo, SeekKey, SeekOp, SeekResult},
    util::quote_identifier,
    vdbe::Register,
    Connection, LimboError, Result, Value,
};
use parking_lot::Mutex;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use std::collections::{BTreeSet, VecDeque};
use std::path::PathBuf;
use std::{
    cell::RefCell,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};
use tantivy::{
    directory::RamDirectory,
    fastfield::Column,
    index::SegmentId,
    indexer::{AddOperation, SegmentWriter},
    query::{EnableScoring, Query, Scorer},
    schema::{Field, IndexRecordOption, Schema},
    tokenizer::{
        NgramTokenizer, RawTokenizer, SimpleTokenizer, TextAnalyzer, TokenStream,
        WhitespaceTokenizer,
    },
    DocAddress, DocSet, Index, IndexReader, IndexSettings, Searcher, TantivyDocument, Term,
    TERMINATED,
};
use turso_parser::ast::{Select, SortOrder};
use uncased::UncasedStr;

mod directory;
mod format;
mod rows;

use directory::{BuildDirectory, SnapshotDirectory};
use format::{
    alive_bitset_bytes, parse_segment_id, segment_chunk_path, segment_chunk_prefix,
    segment_registry_path, segment_tombstone_path, synthesize_meta_json, tombstone_del_file_name,
    with_tantivy_footer, FtsControlV2, LoadedSegment, SegmentData, SegmentDescriptor,
    SegmentFileEntry, FTS2_CONTROL_PATH, FTS2_PATH_PREFIX, FTS2_SEGMENT_PREFIX, FTS2_TOMB_PREFIX,
};
use rows::{
    chunk_rows, row_fields, seek_key_for_path, PathTarget, PendingRow, RowDeleter, RowInserter,
};

/// Name identifier for the FTS index method, used in `CREATE INDEX ... USING fts`.
pub const FTS_INDEX_METHOD_NAME: &str = "fts";

/// Memory budget for Tantivy's per-segment writer arena.
pub const DEFAULT_MEMORY_BUDGET_BYTES: usize = 64 * 1024 * 1024;

/// Chunk size (512 KiB) for splitting segment files into backing rows.
pub const DEFAULT_CHUNK_SIZE: usize = 512 * 1024;

/// Documents and tombstones buffered before an intra-statement flush builds
/// an immutable segment. Statement end always flushes whatever is buffered.
pub const BATCH_COMMIT_SIZE: usize = 1000;

/// Longest accepted `fts_match` / MATCH query string, in bytes.
const FTS_MAX_QUERY_BYTES: usize = 16 * 1024;

/// Deepest accepted parenthesis nesting in a query string. Guards Tantivy's
/// recursive parser against stack overflow.
const FTS_MAX_QUERY_NESTING: usize = 64;

/// Maximum assembled searchers retained per FTS attachment. Entries are
/// keyed by the visible segment set (plus per-segment tombstone state), so
/// any snapshot seeing the same set shares one entry.
const FTS_MAX_CACHED_SEARCHERS: usize = 4;

/// Aggregate resident byte budget for the shared per-segment cache.
///
/// This bounds only what is *retained for reuse* after a statement
/// finishes. It is not a bound on live memory: a cursor keeps every visible
/// segment resident while it runs, however large the index is, because
/// Tantivy reads through synchronous callbacks that cannot fall back to
/// storage I/O.
const FTS_MAX_RETAINED_CACHE_BYTES: usize = 192 * 1024 * 1024;

#[cfg(feature = "test_helper")]
crate::thread::thread_local! {
    static FTS_RETAINED_CACHE_BYTES_OVERRIDE: core::cell::Cell<Option<usize>> =
        const { core::cell::Cell::new(None) };
}

/// Override the retained-cache budget for tests on the current thread, so
/// budget eviction is reachable without multi-hundred-MiB indexes.
/// Pass `None` to restore the default.
#[cfg(feature = "test_helper")]
pub fn set_fts_retained_cache_bytes_for_test(bytes: Option<usize>) {
    FTS_RETAINED_CACHE_BYTES_OVERRIDE.with(|cell| cell.set(bytes));
}

fn fts_max_retained_cache_bytes() -> usize {
    #[cfg(feature = "test_helper")]
    if let Some(bytes) = FTS_RETAINED_CACHE_BYTES_OVERRIDE.with(|cell| cell.get()) {
        return bytes;
    }
    FTS_MAX_RETAINED_CACHE_BYTES
}

/// Mint distinct on-disk index incarnations within one process.
static NEXT_FTS_INDEX_INCARNATION: AtomicU64 = AtomicU64::new(1);
/// Distinguishes cursor instances within a process so a cursor can recognize
/// its own claim on the per-index writer slot across re-entrant calls.
static NEXT_FTS_CURSOR_INSTANCE: AtomicU64 = AtomicU64::new(1);

const ROWID_FIELD: &str = "rowid";

// Thread-local tokenizer cache to avoid creating a new tokenizer for each call.
// TextAnalyzer is not Send/Sync, so we use thread_local storage.
crate::thread::thread_local! {
    static FTS_TOKENIZER: RefCell<TextAnalyzer> = RefCell::new(
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(tantivy::tokenizer::LowerCaser)
            .build()
    );
}

/// Highlight matching terms in text by wrapping them with tags.
///
/// Standalone function that can be used without an FTS index.
/// It tokenizes both the query and text using Tantivy's default tokenizer,
/// finds matching terms, and wraps them with the specified tags.
pub fn fts_highlight(text: &str, query: &str, before_tag: &str, after_tag: &str) -> String {
    if text.is_empty() || query.is_empty() {
        return text.to_string();
    }

    FTS_TOKENIZER.with(|tokenizer| {
        let mut tokenizer = tokenizer.borrow_mut();

        // Extract query terms (lowercased)
        let query_terms: HashSet<String> = {
            let mut terms = HashSet::default();
            let mut query_stream = tokenizer.token_stream(query);
            while let Some(token) = query_stream.next() {
                terms.insert(token.text.to_string());
            }
            terms
        };
        if query_terms.is_empty() {
            return text.to_string();
        }

        // Tokenize the text and track positions of matching tokens
        let match_ranges: Vec<(usize, usize)> = {
            let mut ranges = Vec::new();
            let mut text_stream = tokenizer.token_stream(text);
            while let Some(token) = text_stream.next() {
                if query_terms.contains(&token.text) {
                    ranges.push((token.offset_from, token.offset_to));
                }
            }
            ranges
        };

        if match_ranges.is_empty() {
            return text.to_string();
        }

        // Optimized string building: pre-calculate size and build forward
        let extra_len = match_ranges.len() * (before_tag.len() + after_tag.len());
        let mut result = String::with_capacity(text.len() + extra_len);
        let mut last_end = 0;

        for (start, end) in &match_ranges {
            // Validate UTF-8 boundaries
            if *start > text.len()
                || *end > text.len()
                || !text.is_char_boundary(*start)
                || !text.is_char_boundary(*end)
            {
                continue;
            }

            // Append text before this match
            if *start > last_end {
                result.push_str(&text[last_end..*start]);
            }

            // Append highlighted match
            result.push_str(before_tag);
            result.push_str(&text[*start..*end]);
            result.push_str(after_tag);

            last_end = *end;
        }

        // Append remaining text after last match
        if last_end < text.len() {
            result.push_str(&text[last_end..]);
        }

        result
    })
}

/// Check if text matches a query by testing for any common terms.
///
/// Standalone function that can be used without an FTS index.
/// It tokenizes both the query and text using Tantivy's default tokenizer,
/// and returns true if any query terms appear in the text.
pub fn fts_match(text: &str, query: &str) -> bool {
    if text.is_empty() || query.is_empty() {
        return false;
    }

    FTS_TOKENIZER.with(|tokenizer| {
        let mut tokenizer = tokenizer.borrow_mut();

        // Extract query terms (lowercased)
        let query_terms: HashSet<String> = {
            let mut terms = HashSet::default();
            let mut query_stream = tokenizer.token_stream(query);
            while let Some(token) = query_stream.next() {
                terms.insert(token.text.to_string());
            }
            terms
        };
        if query_terms.is_empty() {
            return false;
        }

        // Tokenize the text and check if any query terms appear
        let mut text_stream = tokenizer.token_stream(text);
        while let Some(token) = text_stream.next() {
            if query_terms.contains(&token.text) {
                return true;
            }
        }
        false
    })
}

/// Creates default `KeyInfo` for BTree index columns.
fn key_info() -> KeyInfo {
    KeyInfo {
        sort_order: SortOrder::Asc,
        collation: CollationSeq::Binary,
        nulls_order: None,
    }
}

/// Parse field weights from a string like "body=2.0,title=1.0"
/// Returns a HashMap mapping column names to tantivy 'boost factors'
fn parse_field_weights(weights_str: &str, columns: &[IndexColumn]) -> Result<HashMap<String, f32>> {
    let mut weights = HashMap::default();

    if weights_str.is_empty() {
        return Ok(weights);
    }

    // Get valid column names for validation
    let valid_columns: HashSet<&str> = columns.iter().map(|c| c.name.as_str()).collect();

    // Parse format: "col1=1.5,col2=2.0"
    for part in weights_str.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let (col_name, weight_str) = part.split_once('=').ok_or_else(|| {
            LimboError::ParseError(format!(
                "invalid weight format '{part}'. Expected 'column=weight' (e.g., 'title=2.0')",
            ))
        })?;

        let col_name = col_name.trim();
        let weight_str = weight_str.trim();

        // Validate column exists in index
        if !valid_columns.contains(col_name) {
            return Err(LimboError::ParseError(format!(
                "unknown column '{}' in weights. Valid columns: {}",
                col_name,
                columns
                    .iter()
                    .map(|c| c.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }

        let weight: f32 = weight_str.parse().map_err(|_| {
            LimboError::ParseError(format!(
                "invalid weight value '{weight_str}' for column '{col_name}'. Expected a number (e.g., 2.0)",
            ))
        })?;
        if weight <= 0.0 {
            return Err(LimboError::ParseError(format!(
                "weight for column '{col_name}' must be positive, got {weight}",
            )));
        }

        weights.insert(col_name.to_string(), weight);
    }

    Ok(weights)
}

/// Factory for creating FTS index attachments.
#[derive(Debug)]
pub struct FtsIndexMethod;

impl IndexMethod for FtsIndexMethod {
    fn attach(&self, cfg: &IndexMethodConfiguration) -> Result<Arc<dyn IndexMethodAttachment>> {
        let attachment = FtsIndexAttachment::new(cfg.clone())?;
        Ok(Arc::new(attachment))
    }
}

/// The single cursor allowed to flush this FTS index for a given connection.
///
/// One statement can open two write cursors over the same index — a trigger
/// whose body writes the FTS-indexed table it fired on. Their buffered
/// deletes would tombstone the same postings twice and their buffers would
/// not see each other's documents, so the second writer is refused with a
/// `Raise(Abort)` error and the statement rolls back cleanly.
///
/// The slot guards intra-statement reentrancy only. Cross-transaction
/// writers need no exclusion: they append disjoint segment rows (WAL mode
/// additionally serializes them through the pager write lock).
#[derive(Debug)]
struct FtsWriterSlot {
    connection: Weak<Connection>,
    cursor_instance: u64,
}

#[derive(Debug, Default)]
struct FtsRuntimeStats {
    /// Immutable segments built by this attachment (one per flush boundary).
    segment_builds: AtomicUsize,
    /// Searcher-cache lookups / hits / misses.
    read_cache_lookups: AtomicUsize,
    read_cache_hits: AtomicUsize,
    read_cache_misses: AtomicUsize,
    /// Segments whose chunks were loaded from backing storage (byte-cache
    /// misses).
    segment_loads: AtomicUsize,
    /// Merge-mutex (lease) acquisitions and rejections; maintenance only.
    write_lease_acquisitions: AtomicUsize,
    write_lease_rejections: AtomicUsize,
}

/// Shared per-segment byte cache: segment id → resident file bytes.
///
/// A segment's bytes never change, so entries need no snapshot identity and
/// are shared by every connection. Uncommitted segments may be inserted by
/// their writing transaction: their ids are unguessable and only
/// discoverable through visible registry rows, so other snapshots can never
/// look them up; rollback purges them.
#[derive(Debug, Default)]
struct SegmentByteCache {
    /// Least recently used first.
    entries: Vec<(SegmentId, Arc<SegmentData>)>,
}

impl SegmentByteCache {
    fn total_bytes(&self) -> usize {
        self.entries.iter().fold(0usize, |total, (_, data)| {
            total.saturating_add(data.total_bytes)
        })
    }

    fn get(&mut self, id: &SegmentId) -> Option<Arc<SegmentData>> {
        let position = self.entries.iter().position(|(entry, _)| entry == id)?;
        let entry = self.entries.remove(position);
        let data = Arc::clone(&entry.1);
        self.entries.push(entry);
        Some(data)
    }

    fn put(&mut self, id: SegmentId, data: Arc<SegmentData>, budget: usize) {
        self.entries.retain(|(entry, _)| *entry != id);
        self.entries.push((id, data));
        // Always keep the newest entry; evict older ones to fit the budget.
        while self.entries.len() > 1 && self.total_bytes() > budget {
            self.entries.remove(0);
        }
    }

    fn remove(&mut self, id: &SegmentId) {
        self.entries.retain(|(entry, _)| entry != id);
    }
}

/// Cache identity of one assembled searcher: the visible segment set with
/// each segment's tombstone state. Exact comparison — a wrong reuse would
/// silently produce wrong query results.
type SearcherKey = Vec<(SegmentId, u32, BTreeSet<u32>)>;

fn searcher_key(segments: &[LoadedSegment]) -> SearcherKey {
    let mut key: SearcherKey = segments
        .iter()
        .map(|segment| {
            (
                segment.id(),
                segment.descriptor.max_doc,
                segment.deleted.clone(),
            )
        })
        .collect();
    key.sort_by_key(|(id, _, _)| id.uuid_string());
    key
}

struct SearcherCacheEntry {
    key: SearcherKey,
    index: Index,
    reader: IndexReader,
    parser: Arc<tantivy::query::QueryParser>,
}

#[derive(Default)]
struct SearcherCache {
    /// Least recently used first.
    entries: Vec<SearcherCacheEntry>,
}

impl SearcherCache {
    fn get(
        &mut self,
        key: &SearcherKey,
    ) -> Option<(Index, IndexReader, Arc<tantivy::query::QueryParser>)> {
        let position = self.entries.iter().position(|entry| &entry.key == key)?;
        let entry = self.entries.remove(position);
        let checkout = (
            entry.index.clone(),
            entry.reader.clone(),
            Arc::clone(&entry.parser),
        );
        self.entries.push(entry);
        Some(checkout)
    }

    fn put(&mut self, entry: SearcherCacheEntry) {
        self.entries.retain(|existing| existing.key != entry.key);
        self.entries.push(entry);
        while self.entries.len() > FTS_MAX_CACHED_SEARCHERS {
            self.entries.remove(0);
        }
    }

    fn purge_segments(&mut self, ids: &[SegmentId]) {
        self.entries
            .retain(|entry| !entry.key.iter().any(|(id, _, _)| ids.contains(id)));
    }
}

/// Attachment-level shared state.
#[derive(Default)]
struct FtsShared {
    segment_bytes: Mutex<SegmentByteCache>,
    searchers: Mutex<SearcherCache>,
    /// The one cursor currently allowed to flush this index per connection.
    writer_slot: Mutex<Option<FtsWriterSlot>>,
    /// Throwaway index used only to mint `SegmentMeta` values for
    /// synthesized `meta.json` content.
    scratch: Mutex<Option<Index>>,
    /// Heuristic count of visible segments: bumped by segment-appending
    /// publishes, reset by merges, reconciled by every full registry scan.
    /// Only used to decide whether the write-path auto-merge should pay for
    /// the real scan — never for correctness (the merge recomputes the true
    /// visible set from its own snapshot). May drift on rollbacks or across
    /// processes; the next scan or merge corrects it.
    visible_segment_estimate: AtomicUsize,
    stats: FtsRuntimeStats,
}

impl std::fmt::Debug for FtsShared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtsShared").finish_non_exhaustive()
    }
}

impl FtsShared {
    /// The scratch index exists because tantivy only hands out `SegmentMeta`
    /// values through an `Index`'s segment inventory
    /// (`Index::new_segment_meta`), and the read path needs one per visible
    /// segment to synthesize the `meta.json` a `SnapshotDirectory` serves.
    /// It is an empty in-RAM index with this attachment's schema: nothing is
    /// ever written to or read from it, and one instance is shared by every
    /// cursor of the attachment.
    fn scratch_index(&self, schema: &Schema) -> Result<Index> {
        let mut scratch = self.scratch.lock();
        if let Some(index) = scratch.as_ref() {
            return Ok(index.clone());
        }
        let index = Index::create(
            RamDirectory::create(),
            schema.clone(),
            IndexSettings::default(),
        )
        .map_err(|e| LimboError::InternalError(format!("FTS scratch index: {e}")))?;
        *scratch = Some(index.clone());
        Ok(index)
    }
}

/// FTS index attachment that holds configuration and creates cursors.
#[derive(Debug)]
pub struct FtsIndexAttachment {
    /// Internal configuration
    cfg: IndexMethodConfiguration,
    /// Tantivy schema for the FTS index
    schema: Schema,
    /// Tantivy field for the rowid column
    rowid_field: Field,
    /// Schema fields for each indexed text column
    text_fields: Vec<(IndexColumn, Field)>,
    /// Parsed query patterns for FTS queries
    patterns: Vec<Select>,
    /// Weights for each field in FTS scoring, from the WITH clause.
    field_weights: HashMap<String, f32>,
    /// (min_gram, max_gram) for the ngram tokenizer, from the WITH clause
    /// `min_gram`/`max_gram` keys. [DEFAULT_NGRAM_WINDOW] unless configured.
    ngram_window: (usize, usize),
    shared: Arc<FtsShared>,
}

/// Supported tokenizer names for FTS indexes
pub const SUPPORTED_TOKENIZERS: &[&str] = &[
    "default",    // Tantivy default: lowercase + punctuation split + 40 char limit
    "raw",        // No tokenization - exact match only
    "simple",     // Basic whitespace/punctuation split
    "whitespace", // Split on whitespace only
    "ngram",      // N-gram tokenizer (2-3 chars by default)
];

/// Supported keys in the WITH clause of an FTS index
pub const SUPPORTED_WITH_KEYS: &[&str] = &["tokenizer", "weights", "min_gram", "max_gram"];

/// Ngram window used when `min_gram`/`max_gram` are not given.
pub const DEFAULT_NGRAM_WINDOW: (usize, usize) = (2, 3);

impl FtsIndexAttachment {
    #[aristo::intent(
        "Every WITH clause key this constructor accepts is also consumed by it: \
         a key outside the supported list is an error, and lookups go through \
         one case-folded map so a mis-cased key cannot slip past. Adding a key \
         to the supported list without reading it from that map recreates the \
         accepted-but-ignored failure this guards against — the DDL succeeds \
         but the index silently gets different semantics than it asked for.",
        verify = "neural",
        id = "fts_with_keys_all_validated_and_consumed"
    )]
    pub fn new(cfg: IndexMethodConfiguration) -> Result<Self> {
        // Validate WITH clause keys the same way bad values are validated:
        // a typo like `tokenzier` must be an error, not a silently different
        // index. Keys are matched case-insensitively.
        let mut parameters: HashMap<&UncasedStr, &Value> = HashMap::default();
        for (key, value) in &cfg.parameters {
            let key_uncased = UncasedStr::new(key);
            if !SUPPORTED_WITH_KEYS
                .iter()
                .any(|supported| UncasedStr::new(supported) == key_uncased)
            {
                return Err(LimboError::ParseError(format!(
                    "unsupported FTS WITH parameter '{}'. Supported parameters: {}",
                    key,
                    SUPPORTED_WITH_KEYS.join(", ")
                )));
            }
            if parameters.insert(key_uncased, value).is_some() {
                return Err(LimboError::ParseError(format!(
                    "duplicate FTS WITH parameter '{key}'"
                )));
            }
        }

        // Parse tokenizer from WITH clause parameters, default to "default"
        // The parser may include surrounding quotes in the value, so we strip them
        let tokenizer_name = parameters
            .get(UncasedStr::new("tokenizer"))
            .and_then(|v| match v {
                Value::Text(t) => {
                    let s = t.to_string();
                    // Strip surrounding single or double quotes if present
                    let trimmed = s.trim_matches(|c| c == '\'' || c == '"');
                    Some(trimmed.to_string())
                }
                _ => None,
            })
            .unwrap_or_else(|| "default".to_string());

        // Validate tokenizer name
        if !SUPPORTED_TOKENIZERS.contains(&tokenizer_name.as_str()) {
            return Err(LimboError::ParseError(format!(
                "unsupported FTS tokenizer '{}'. Supported tokenizers: {}",
                tokenizer_name,
                SUPPORTED_TOKENIZERS.join(", ")
            )));
        }

        // Parse the ngram window: WITH (tokenizer = 'ngram', min_gram = 1, max_gram = 3)
        let parse_gram = |key: &str| -> Result<Option<usize>> {
            let Some(value) = parameters.get(UncasedStr::new(key)) else {
                return Ok(None);
            };
            match value {
                Value::Numeric(crate::numeric::Numeric::Integer(v)) if *v >= 1 => {
                    Ok(Some(usize::try_from(*v).expect("checked to be positive")))
                }
                _ => Err(LimboError::ParseError(format!(
                    "FTS WITH parameter '{key}' must be a positive integer"
                ))),
            }
        };
        let min_gram = parse_gram("min_gram")?;
        let max_gram = parse_gram("max_gram")?;
        if (min_gram.is_some() || max_gram.is_some()) && tokenizer_name != "ngram" {
            return Err(LimboError::ParseError(format!(
                "FTS WITH parameters 'min_gram' and 'max_gram' require tokenizer = 'ngram', got tokenizer = '{tokenizer_name}'"
            )));
        }
        let ngram_window = (
            min_gram.unwrap_or(DEFAULT_NGRAM_WINDOW.0),
            max_gram.unwrap_or(DEFAULT_NGRAM_WINDOW.1),
        );
        if ngram_window.0 > ngram_window.1 {
            return Err(LimboError::ParseError(format!(
                "FTS ngram window is invalid: min_gram ({}) is greater than max_gram ({})",
                ngram_window.0, ngram_window.1
            )));
        }

        // Parse field weights from WITH clause: weights='body=2.0,title=1.0'
        let field_weights = if let Some(weights_value) = parameters.get(UncasedStr::new("weights"))
        {
            let weights_str = match weights_value {
                Value::Text(t) => {
                    let s = t.to_string();
                    s.trim_matches(|c| c == '\'' || c == '"').to_string()
                }
                _ => String::new(),
            };
            parse_field_weights(&weights_str, &cfg.columns)?
        } else {
            HashMap::default()
        };

        // Build Tantivy schema (no Directory or Index creation yet)
        let mut schema_builder = Schema::builder();

        // Use FAST field for rowid to enable efficient columnar access during query result retrieval.
        // This avoids loading full documents from the .store file just to get the rowid.
        let rowid_field = schema_builder.add_i64_field(
            ROWID_FIELD,
            tantivy::schema::INDEXED | tantivy::schema::FAST,
        );

        let mut text_fields = Vec::with_capacity(cfg.columns.len());
        for col in &cfg.columns {
            let opts = tantivy::schema::TextOptions::default().set_indexing_options(
                tantivy::schema::TextFieldIndexing::default()
                    .set_tokenizer(&tokenizer_name)
                    .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions),
            );
            let field = schema_builder.add_text_field(&col.name, opts);
            text_fields.push((col.clone(), field));
        }

        let schema = schema_builder.build();

        // Build query patterns for FTS
        // Order matters: more specific patterns should come first
        let cols = cfg
            .columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        // Pattern 0: score with ORDER BY DESC LIMIT
        let score_pattern = format!(
            "SELECT fts_score({}, ?) as score FROM {} ORDER BY score DESC LIMIT ?",
            cols, cfg.table_name
        );
        // Pattern 1: combined + ORDER BY DESC + LIMIT (most specific)
        let combined_ordered_limit = format!(
            "SELECT fts_score({}, ?1) as score FROM {} WHERE fts_match({}, ?1) ORDER BY score DESC LIMIT ?",
            cols, cfg.table_name, cols
        );
        // Pattern 2: combined + ORDER BY DESC (no LIMIT)
        let combined_ordered = format!(
            "SELECT fts_score({}, ?1) as score FROM {} WHERE fts_match({}, ?1) ORDER BY score DESC",
            cols, cfg.table_name, cols
        );
        // Pattern 3: combined + LIMIT (no ORDER BY)
        let combined_limit = format!(
            "SELECT fts_score({}, ?1) as score FROM {} WHERE fts_match({}, ?1) LIMIT ?",
            cols, cfg.table_name, cols
        );
        // Pattern 4: combined (no ORDER BY, no LIMIT)
        let combined = format!(
            "SELECT fts_score({}, ?1) as score FROM {} WHERE fts_match({}, ?1)",
            cols, cfg.table_name, cols
        );
        // Pattern 5: match + LIMIT
        let match_limit = format!(
            "SELECT * FROM {} WHERE fts_match({}, ?) LIMIT ?",
            cfg.table_name, cols
        );
        // Pattern 6: match (no LIMIT)
        let match_pattern = format!(
            "SELECT * FROM {} WHERE fts_match({}, ?)",
            cfg.table_name, cols
        );
        let patterns = parse_patterns(&[
            &score_pattern,          // 0
            &combined_ordered_limit, // 1
            &combined_ordered,       // 2
            &combined_limit,         // 3
            &combined,               // 4
            &match_limit,            // 5
            &match_pattern,          // 6
        ])?;
        Ok(Self {
            cfg,
            schema,
            rowid_field,
            text_fields,
            patterns,
            field_weights,
            ngram_window,
            shared: Arc::new(FtsShared::default()),
        })
    }
}

impl IndexMethodAttachment for FtsIndexAttachment {
    fn definition<'a>(&'a self) -> IndexMethodDefinition<'a> {
        IndexMethodDefinition {
            method_name: FTS_INDEX_METHOD_NAME,
            table_name: &self.cfg.table_name,
            index_name: &self.cfg.index_name,
            patterns: &self.patterns,
            backing_btree: false,
            // Unordered match queries stream directly from Tantivy scorers.
            // UPDATE/DELETE must therefore collect stable rowids before writes.
            results_materialized: false,
            mvcc_support: super::IndexMethodMvccSupport::TransactionalBackingStore,
        }
    }

    fn init(&self) -> Result<Box<dyn IndexMethodCursor>> {
        Ok(Box::new(FtsCursor::new(self)))
    }
}

/// Nested DDL statements a cursor is driving for `create` or `destroy`,
/// stepped cooperatively so their I/O reaches the caller instead of being
/// pumped inside the opcode. The connection is nested only while one of
/// them is being stepped or dropped: the flag tells the pager that the
/// statement's `Halt` and reset must not finalize the parent's
/// transaction, and it must not leak to other statements stepped on this
/// connection while the parent is suspended at a yield.
struct NestedDdl {
    connection: Weak<Connection>,
    /// SQL still to run, in order. Each statement is prepared only when
    /// its turn comes: a later one may depend on schema an earlier one
    /// creates (the backing index on the backing table).
    pending: VecDeque<String>,
    current: Option<crate::Statement>,
}

impl NestedDdl {
    fn new(conn: &Arc<Connection>, statements: impl IntoIterator<Item = String>) -> Self {
        Self {
            connection: Arc::downgrade(conn),
            pending: statements.into_iter().collect(),
            current: None,
        }
    }

    /// Prepare `sql` nested: `__turso_internal_` names are refused to
    /// top-level statements. No statement subtransaction: the parent
    /// statement's transaction covers it (a subtransaction here would
    /// fail with DatabaseBusy).
    fn prepare(conn: &Arc<Connection>, sql: String) -> Result<crate::Statement> {
        conn.start_nested();
        let stmt = conn.prepare(sql);
        conn.end_nested();
        let stmt = stmt?;
        stmt.program
            .prepared
            .needs_stmt_subtransactions
            .store(false, Ordering::Relaxed);
        Ok(stmt)
    }

    /// Drive the statements to completion in order, handing their I/O to
    /// the caller; re-enter after each yield until `Done`.
    fn step(&mut self) -> IOResultOr<()> {
        let conn = self.connection.upgrade().ok_or_else(|| {
            LimboError::InternalError("FTS nested DDL outlived its connection".into())
        })?;
        loop {
            if self.current.is_none() {
                let Some(sql) = self.pending.pop_front() else {
                    return Ok(IOResult::Done(()));
                };
                self.current = Some(Self::prepare(&conn, sql)?);
            }
            let stmt = self.current.as_mut().expect("prepared above");
            conn.start_nested();
            let result = stmt.run_ignore_rows_nonblock();
            if !matches!(result, Ok(IOResult::IO(_))) {
                // Drop a finished (or failed) statement while still
                // nested: its reset consults `is_nested_stmt()`.
                self.current = None;
            }
            conn.end_nested();
            return_if_io!(result);
        }
    }
}

impl Drop for NestedDdl {
    fn drop(&mut self) {
        // A statement abandoned mid-flight (the parent statement was reset)
        // is dropped nested for the same reason a finished one is.
        if self.current.is_none() {
            return;
        }
        let Some(conn) = self.connection.upgrade() else {
            self.current = None;
            return;
        };
        conn.start_nested();
        self.current = None;
        conn.end_nested();
    }
}

/// Pattern indices for FTS queries
const FTS_PATTERN_SCORE: i64 = 0;
const FTS_PATTERN_COMBINED_ORDERED_LIMIT: i64 = 1;
const FTS_PATTERN_COMBINED_ORDERED: i64 = 2;
const FTS_PATTERN_COMBINED_LIMIT: i64 = 3;
const FTS_PATTERN_COMBINED: i64 = 4;
const FTS_PATTERN_MATCH_LIMIT: i64 = 5;
const FTS_PATTERN_MATCH: i64 = 6;

fn bounded_query_limit(limit: Option<i64>, live_docs: u64) -> usize {
    let live_docs = usize::try_from(live_docs).unwrap_or(usize::MAX);
    match limit {
        Some(0) => 0,
        Some(limit) if limit > 0 => usize::try_from(limit).unwrap_or(usize::MAX).min(live_docs),
        // A negative LIMIT means unlimited in SQLite.
        Some(_) | None => live_docs,
    }
}

// ============================ cursor ============================

/// One document buffered for the next segment build.
struct BufferedDoc {
    rowid: i64,
    doc: TantivyDocument,
}

/// In-memory effects to apply once a publish's rows are durably staged.
enum PublishApply {
    /// Nothing beyond the rows themselves (the control row).
    Nothing,
    /// A statement flush: append the new segment (if any) to the visible
    /// set. Tombstones were already applied to `segments` at delete() time.
    AppendSegment(Option<LoadedSegment>),
    /// Merge: the visible set becomes exactly these segments.
    ReplaceSegments(Vec<LoadedSegment>),
}

/// A resumable row publication: delete rows, then insert rows, then apply
/// the in-memory effects exactly once.
struct PendingPublish {
    inserter: Option<RowInserter>,
    deleter: Option<RowDeleter>,
    apply: PublishApply,
}

/// Driver states for the open/scan machine. Everything that must survive
/// an I/O yield inside one state lives in the variant; scan results shared
/// across states live in scratch fields on the cursor
/// (`scan_descriptors`, `scan_tombs`, `scan_data`).
#[derive(Debug)]
enum FtsState {
    /// Initial state.
    Init,
    /// Seeking the v2 control row, which marks a v2 store.
    SeekControl,
    /// Advancing after the control seek returned `TryAdvance`.
    AdvanceToControl,
    /// Positioned at (or after) where the control row would be.
    ReadControl,
    /// No control row: rewinding to tell an empty store from one written
    /// by the pre-registry FTS implementation (which is refused).
    ProbeFormat { rewound: bool },
    /// Range-scanning the visible `(SEGMENT, *)` registry rows.
    ScanSegments { seeked: bool, advance_pending: bool },
    /// Range-scanning the visible tombstone rows.
    ScanTombs { seeked: bool, advance_pending: bool },
    /// Loading chunk rows for visible segments absent from the byte cache.
    /// `queue` holds indices into `scan_descriptors`.
    LoadChunks {
        queue: Vec<usize>,
        pos: usize,
        /// file_ord -> (chunk_no -> bytes) for the segment at `queue[pos]`.
        chunks: HashMap<u32, HashMap<i64, Vec<u8>>>,
        seeked: bool,
        advance_pending: bool,
    },
    /// Assembling the Tantivy view over the loaded segment set.
    BuildIndex,
    /// Snapshot (or format probe) complete.
    Ready,
}

/// Streaming query support: one segment's scorer plus its rowid column.
struct FtsStreamingSegment {
    scorer: Box<dyn Scorer>,
    rowids: Column<i64>,
    alive: Option<tantivy::fastfield::AliveBitSet>,
}

struct FtsHitStream {
    segments: Vec<FtsStreamingSegment>,
    segment_pos: usize,
    remaining: usize,
    scores_enabled: bool,
    current: Option<(f32, i64)>,
}

impl FtsHitStream {
    fn advance(&mut self) -> Result<bool> {
        self.current = None;
        if self.remaining == 0 {
            return Ok(false);
        }

        while let Some(segment) = self.segments.get_mut(self.segment_pos) {
            let doc_id = segment.scorer.doc();
            if doc_id == TERMINATED {
                self.segment_pos += 1;
                continue;
            }
            let score = self.scores_enabled.then(|| segment.scorer.score());
            segment.scorer.advance();

            if segment
                .alive
                .as_ref()
                .is_some_and(|alive| alive.is_deleted(doc_id))
            {
                continue;
            }
            let rowid = segment.rowids.first(doc_id).ok_or_else(|| {
                LimboError::InternalError("FTS: rowid fast field missing value".into())
            })?;
            self.current = Some((score.unwrap_or(0.0), rowid));
            self.remaining -= 1;
            return Ok(true);
        }

        Ok(false)
    }
}

/// Cursor for executing FTS operations (queries, inserts, deletes).
///
/// A write cursor never opens a Tantivy `IndexWriter`: it buffers documents
/// and builds one immutable segment per flush boundary through
/// `SegmentWriter`, then publishes the segment by inserting registry and
/// chunk rows through the ordinary (MVCC-aware) backing cursor. Insert-only
/// statements never load the existing index at all.
pub struct FtsCursor {
    schema: Schema,
    rowid_field: Field,
    /// (min_gram, max_gram) window for the ngram tokenizer
    ngram_window: (usize, usize),
    text_fields: Vec<(IndexColumn, Field)>,
    /// The user-visible index name, for error messages.
    index_name: String,
    dir_table_name: String,
    /// Pre-computed default fields for QueryParser (avoids rebuilding Vec per query)
    default_fields: Vec<Field>,
    /// Pre-computed (Field, boost) pairs for QueryParser (avoids re-iterating per query)
    field_boosts: Vec<(Field, f32)>,
    shared: Arc<FtsShared>,
    /// This cursor's identity in [`FtsWriterSlot`] claims.
    cursor_instance_id: u64,
    /// Weak so a cursor parked on its connection does not keep the
    /// connection alive (see `IndexMethodContext::connection`).
    connection: Option<Weak<Connection>>,
    database_id: Option<usize>,
    fts_dir_cursor: Option<Box<dyn CursorTrait>>,
    /// Backing-store DDL in flight for `create` / `destroy`; `Some` only
    /// while it is suspended at an I/O yield.
    pending_ddl: Option<NestedDdl>,
    btree_root_page: Option<i64>,

    control: Option<FtsControlV2>,
    /// The snapshot's visible segment set (descriptors + resident bytes +
    /// tombstone state), including this transaction's own published
    /// segments. Valid once `snapshot_loaded`.
    segments: Vec<LoadedSegment>,
    snapshot_loaded: bool,

    // Scratch for the open/scan machine.
    scan_descriptors: Vec<SegmentDescriptor>,
    scan_tombs: HashMap<SegmentId, BTreeSet<u32>>,
    scan_data: HashMap<SegmentId, Arc<SegmentData>>,
    /// When true, `open` stops after format detection instead of loading
    /// the snapshot (the insert fast path).
    probe_only: bool,

    // Assembled Tantivy view.
    index: Option<Index>,
    reader: Option<IndexReader>,
    searcher: Option<Searcher>,
    cached_parser: Option<Arc<tantivy::query::QueryParser>>,

    // Write buffers.
    doc_buffer: Vec<BufferedDoc>,
    /// Tombstone rows queued for the next flush. The same tombstones are
    /// already applied to `segments[..].deleted`, which is the source of
    /// truth for this transaction's own reads.
    pending_tombstone_rows: Vec<(SegmentId, u32)>,
    /// Row publication in flight (statement flush, control row, or merge).
    publish: Option<PendingPublish>,
    /// Set when a statement flush published a new segment; tells
    /// `stage_statement_commit` to consider a write-path merge once the
    /// flush publication completes. Survives IO yields so the auto-merge
    /// check resumes exactly once per flushed statement.
    auto_merge_pending: bool,
    /// Segment ids this transaction published into the shared byte cache;
    /// purged on rollback.
    own_published: Vec<SegmentId>,

    state: FtsState,
    /// True while `open_write` is driving the shared open state machine.
    opening_for_write: bool,
    /// True once this cursor holds the per-connection writer slot.
    holds_writer_slot: bool,
    /// True once this transaction registered as a tombstone writer with the
    /// MVCC merge mutex.
    registered_deleter: bool,

    // Query iteration.
    current_hits: Vec<(f32, DocAddress, i64)>,
    streaming_hits: Option<FtsHitStream>,
    hit_pos: usize,
    current_pattern: i64,
}

impl FtsCursor {
    /// Creates a new FTS cursor with the given configuration.
    fn new(attachment: &FtsIndexAttachment) -> Self {
        let dir_table_name = format!(
            "{}fts_dir_{}",
            crate::schema::TURSO_INTERNAL_PREFIX,
            attachment.cfg.index_name
        );
        let text_fields = attachment.text_fields.clone();
        let default_fields: Vec<Field> = text_fields.iter().map(|(_, f)| *f).collect();
        let field_boosts: Vec<(Field, f32)> = text_fields
            .iter()
            .filter_map(|(col, field)| {
                attachment
                    .field_weights
                    .get(&col.name)
                    .map(|&boost| (*field, boost))
            })
            .collect();
        Self {
            schema: attachment.schema.clone(),
            rowid_field: attachment.rowid_field,
            ngram_window: attachment.ngram_window,
            text_fields,
            index_name: attachment.cfg.index_name.clone(),
            dir_table_name,
            default_fields,
            field_boosts,
            shared: Arc::clone(&attachment.shared),
            cursor_instance_id: NEXT_FTS_CURSOR_INSTANCE.fetch_add(1, Ordering::Relaxed),
            connection: None,
            database_id: None,
            fts_dir_cursor: None,
            pending_ddl: None,
            btree_root_page: None,
            control: None,
            segments: Vec::new(),
            snapshot_loaded: false,
            scan_descriptors: Vec::new(),
            scan_tombs: HashMap::default(),
            scan_data: HashMap::default(),
            probe_only: false,
            index: None,
            reader: None,
            searcher: None,
            cached_parser: None,
            doc_buffer: Vec::new(),
            pending_tombstone_rows: Vec::new(),
            publish: None,
            auto_merge_pending: false,
            own_published: Vec::new(),
            state: FtsState::Init,
            opening_for_write: false,
            holds_writer_slot: false,
            registered_deleter: false,
            current_hits: Vec::new(),
            streaming_hits: None,
            hit_pos: 0,
            current_pattern: FTS_PATTERN_SCORE,
        }
    }

    fn pending_op_count(&self) -> usize {
        self.doc_buffer.len() + self.pending_tombstone_rows.len()
    }

    fn is_publishing(&self) -> bool {
        self.publish.is_some()
    }

    /// Claim the per-index writer slot for this cursor, or refuse if another
    /// cursor on the same connection already holds it. See [`FtsWriterSlot`].
    fn claim_writer_slot(&mut self) -> Result<()> {
        if self.holds_writer_slot {
            return Ok(());
        }
        let conn = self
            .connection
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                LimboError::InternalError("FTS cursor claimed writer slot before open".to_string())
            })?;
        {
            let mut slot = self.shared.writer_slot.lock();
            if let Some(claim) = slot.as_ref() {
                let same_live_connection = claim
                    .connection
                    .upgrade()
                    .is_some_and(|owner| Arc::ptr_eq(&owner, &conn));
                if same_live_connection && claim.cursor_instance != self.cursor_instance_id {
                    // Raise(Abort) so the whole statement rolls back: the
                    // refused write may sit mid-statement (a trigger body),
                    // and its base rows must not commit without their index
                    // entries.
                    return Err(LimboError::Raise(
                        turso_parser::ast::ResolveType::Abort,
                        "statement already has an open writer on this FTS index; \
                         a trigger cannot write the FTS-indexed table its firing \
                         statement is writing"
                            .to_string(),
                    ));
                }
                // A claim from another (or dead) connection: cross-connection
                // writers append disjoint segments and need no exclusion, so
                // this claim is stale for us. Replace it.
            }
            *slot = Some(FtsWriterSlot {
                connection: Arc::downgrade(&conn),
                cursor_instance: self.cursor_instance_id,
            });
        }
        self.holds_writer_slot = true;
        Ok(())
    }

    /// Release the writer slot if this cursor holds it. Idempotent.
    fn release_writer_slot(&mut self) {
        self.holds_writer_slot = false;
        let mut slot = self.shared.writer_slot.lock();
        if slot
            .as_ref()
            .is_some_and(|claim| claim.cursor_instance == self.cursor_instance_id)
        {
            *slot = None;
        }
    }

    /// Resolve this index's stable MVCC table id, if MVCC is active.
    fn mvcc_index_id(
        &self,
        conn: &Arc<Connection>,
        database_id: usize,
    ) -> Result<Option<(Arc<crate::MvStore>, u64, crate::mvcc::database::MVTableId)>> {
        let Some(mv_store) = conn.mv_store_for_db(database_id) else {
            return Ok(None);
        };
        let tx_id = conn.get_mv_tx_id_for_db(database_id).ok_or_else(|| {
            LimboError::InternalError(
                "FTS write opened without an active MVCC transaction".to_string(),
            )
        })?;
        let root_page = self.btree_root_page.ok_or_else(|| {
            LimboError::InternalError("FTS backing root is not initialized".to_string())
        })?;
        let snapshot_ts = mv_store.read_snapshot_ts(tx_id);
        // A PASSIVE checkpoint can retire this root page under a stale
        // compiled plan; that is a stale-schema read, not corruption.
        let index_id = if conn.experimental_mvcc_passive_checkpoint_enabled() {
            mv_store
                .try_get_table_id_from_root_page_at(root_page, snapshot_ts)
                .ok_or(LimboError::SchemaUpdated)?
        } else {
            mv_store.get_table_id_from_root_page_at(root_page, snapshot_ts)
        };
        Ok(Some((mv_store, tx_id, index_id)))
    }

    /// Under MVCC, take the per-index maintenance lease (the merge mutex)
    /// for this cursor's transaction. Reentrant for the owning transaction;
    /// a no-op in WAL mode, where the pager write lock already serializes.
    /// Only merge/OPTIMIZE and index teardown take this — plain writers
    /// append disjoint rows and run concurrently.
    fn acquire_mvcc_maintenance_lease(&self) -> Result<()> {
        let conn = self
            .connection
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| LimboError::InternalError("FTS cursor has no connection".to_string()))?;
        let database_id = self.database_id.ok_or_else(|| {
            LimboError::InternalError("FTS database id is not initialized".to_string())
        })?;
        let Some((mv_store, tx_id, index_id)) = self.mvcc_index_id(&conn, database_id)? else {
            return Ok(());
        };
        match mv_store.acquire_index_method_write_lease(tx_id, index_id) {
            Ok(()) => {
                self.shared
                    .stats
                    .write_lease_acquisitions
                    .fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(err @ (LimboError::WriteWriteConflict | LimboError::Busy)) => {
                self.shared
                    .stats
                    .write_lease_rejections
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
            Err(err) => Err(err),
        }
    }

    /// Under MVCC, announce this transaction as a tombstone writer so a
    /// concurrent merge cannot retire the segments it is deleting from.
    /// Idempotent per transaction; a no-op in WAL mode.
    fn register_mvcc_deleter(&mut self) -> Result<()> {
        if self.registered_deleter {
            return Ok(());
        }
        let conn = self
            .connection
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| LimboError::InternalError("FTS cursor has no connection".to_string()))?;
        let database_id = self.database_id.ok_or_else(|| {
            LimboError::InternalError("FTS database id is not initialized".to_string())
        })?;
        if let Some((mv_store, tx_id, index_id)) = self.mvcc_index_id(&conn, database_id)? {
            mv_store.register_index_method_deleter(tx_id, index_id)?;
        }
        self.registered_deleter = true;
        Ok(())
    }

    /// Open the backing B-tree cursor for the FTS row store.
    fn open_cursor(&mut self, conn: &Arc<Connection>, database_id: usize) -> Result<()> {
        if self.fts_dir_cursor.is_some() {
            return Ok(());
        }
        // The index stores all 3 columns: (path, chunk_no, bytes) as the key.
        let index_name = format!("{}_key", self.dir_table_name);
        let scratch = conn
            .with_schema(database_id, |schema| {
                schema.get_index(&self.dir_table_name, &index_name).cloned()
            })
            .ok_or_else(|| {
                LimboError::InternalError(format!(
                    "index {} for table {} not found",
                    index_name, self.dir_table_name
                ))
            })?;
        self.btree_root_page = Some(scratch.root_page);
        self.fts_dir_cursor = Some(open_index_cursor(
            conn,
            database_id,
            &self.dir_table_name,
            &index_name,
            [key_info(), key_info(), key_info()],
        )?);
        Ok(())
    }

    /// Register custom tokenizers with a Tantivy index.
    fn register_tokenizers(&self, index: &Index) {
        let tokenizers = index.tokenizers();
        tokenizers.register("raw", RawTokenizer::default());
        tokenizers.register("simple", SimpleTokenizer::default());
        tokenizers.register("whitespace", WhitespaceTokenizer::default());
        // Full n-grams for substring matching (not prefix-only). The window
        // comes from the WITH clause `min_gram`/`max_gram` keys and was
        // validated at CREATE INDEX time, so construction cannot fail here.
        let (min_gram, max_gram) = self.ngram_window;
        if let Ok(ngram) = NgramTokenizer::new(min_gram, max_gram, false) {
            // Lowercase the n-grams so matching is case-insensitive, like the
            // other tokenizers.
            let analyzer = TextAnalyzer::builder(ngram)
                .filter(tantivy::tokenizer::LowerCaser)
                .build();
            tokenizers.register("ngram", analyzer);
        }
    }

    fn build_query_parser(&self, index: &Index) -> Arc<tantivy::query::QueryParser> {
        let mut parser = tantivy::query::QueryParser::for_index(index, self.default_fields.clone());
        for &(field, boost) in &self.field_boosts {
            parser.set_field_boost(field, boost);
        }
        Arc::new(parser)
    }

    /// Assemble the Tantivy view (index, reader, searcher, parser) over the
    /// cursor's in-memory segment set, consulting the shared searcher cache
    /// first. Pure CPU work — every byte is already resident.
    fn build_snapshot_view(&mut self, publish_to_cache: bool) -> Result<()> {
        let key = searcher_key(&self.segments);
        self.shared
            .stats
            .read_cache_lookups
            .fetch_add(1, Ordering::Relaxed);
        if let Some((index, reader, parser)) = self.shared.searchers.lock().get(&key) {
            self.shared
                .stats
                .read_cache_hits
                .fetch_add(1, Ordering::Relaxed);
            self.searcher = Some(reader.searcher());
            self.index = Some(index);
            self.reader = Some(reader);
            self.cached_parser = Some(parser);
            return Ok(());
        }
        self.shared
            .stats
            .read_cache_misses
            .fetch_add(1, Ordering::Relaxed);

        let mut files: HashMap<PathBuf, Arc<[u8]>> = HashMap::default();
        for segment in &self.segments {
            for (name, data) in &segment.data.files {
                files.insert(PathBuf::from(name), Arc::clone(data));
            }
            if !segment.deleted.is_empty() {
                // Serve the tombstone set as the segment's `.del` file so
                // the alive filter is enforced at the SegmentReader level
                // and every query path honors it.
                files.insert(
                    PathBuf::from(tombstone_del_file_name(&segment.id())),
                    Arc::from(with_tantivy_footer(alive_bitset_bytes(
                        segment.descriptor.max_doc,
                        &segment.deleted,
                    ))?),
                );
            }
        }
        let scratch = self.shared.scratch_index(&self.schema)?;
        let meta_json = synthesize_meta_json(&scratch, &self.schema, &self.segments)?;
        let directory = SnapshotDirectory::new(files, meta_json);
        let index = Index::open(directory)
            .map_err(|e| LimboError::InternalError(format!("FTS snapshot open: {e}")))?;
        self.register_tokenizers(&index);
        let reader = index
            .reader_builder()
            // Manual reload policy: this directory's `watch` cannot deliver
            // callbacks, so the default on-commit policy would never fire.
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()
            .map_err(|e: tantivy::TantivyError| LimboError::InternalError(e.to_string()))?;
        let parser = self.build_query_parser(&index);
        if publish_to_cache {
            self.shared.searchers.lock().put(SearcherCacheEntry {
                key,
                index: index.clone(),
                reader: IndexReader::clone(&reader),
                parser: Arc::clone(&parser),
            });
        }
        self.searcher = Some(reader.searcher());
        self.index = Some(index);
        self.reader = Some(reader);
        self.cached_parser = Some(parser);
        Ok(())
    }

    /// Whether this cursor's assembled view may enter the shared searcher
    /// cache. A view containing this transaction's own uncommitted segments
    /// is keyed by ids other snapshots cannot construct, so sharing it is
    /// harmless — but it would waste a cache slot; keep only clean views.
    fn view_is_cacheable(&self) -> bool {
        self.own_published.is_empty()
            && self.doc_buffer.is_empty()
            && self.pending_tombstone_rows.is_empty()
    }

    /// Invalidate the assembled view after the segment set changed.
    fn invalidate_snapshot_view(&mut self) {
        self.index = None;
        self.reader = None;
        self.searcher = None;
        self.cached_parser = None;
    }

    /// Make sure `self.searcher` reflects the current in-memory segment set.
    fn ensure_searcher(&mut self) -> Result<()> {
        if self.searcher.is_some() {
            return Ok(());
        }
        let cacheable = self.view_is_cacheable();
        self.build_snapshot_view(cacheable)
    }

    /// Drive the open/scan state machine until the snapshot (or, with
    /// `probe_only`, just the format detection) is complete.
    ///
    /// The machine runs three range scans over the backing rows — registry
    /// descriptors, tombstones, then chunks of segments the byte cache does
    /// not hold — and assembles the Tantivy view from the result. A store
    /// with no control row is either empty or was written by the
    /// pre-registry FTS implementation; the latter is refused with a
    /// rebuild hint, since that layout is not readable by this code.
    fn drive_open(&mut self) -> IOResultOr<()> {
        let conn = self
            .connection
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| {
                LimboError::InternalError("FTS cursor has no live connection".to_string())
            })?;
        let database_id = self.database_id.ok_or_else(|| {
            LimboError::InternalError("FTS database id is not initialized".to_string())
        })?;
        loop {
            match &mut self.state {
                FtsState::Init => {
                    self.open_cursor(&conn, database_id)?;
                    self.scan_descriptors.clear();
                    self.scan_tombs.clear();
                    self.scan_data.clear();
                    self.state = FtsState::SeekControl;
                }
                FtsState::SeekControl => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;
                    let seek_key = seek_key_for_path(FTS2_CONTROL_PATH)?;
                    let seek_result = return_if_io!(cursor.seek(
                        SeekKey::IndexKey(seek_key.as_record_ref()),
                        SeekOp::GE { eq_only: false },
                    ));
                    self.state = match seek_result {
                        SeekResult::NotFound => FtsState::ProbeFormat { rewound: false },
                        SeekResult::TryAdvance => FtsState::AdvanceToControl,
                        SeekResult::Found => FtsState::ReadControl,
                    };
                }
                FtsState::AdvanceToControl => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;
                    return_if_io!(cursor.next());
                    self.state = if cursor.has_record() {
                        FtsState::ReadControl
                    } else {
                        FtsState::ProbeFormat { rewound: false }
                    };
                }
                FtsState::ReadControl => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;
                    if !cursor.has_record() {
                        self.state = FtsState::ProbeFormat { rewound: false };
                        continue;
                    }
                    let record = return_if_io!(cursor.record()).ok_or_else(|| {
                        LimboError::Corrupt("FTS cursor has no record payload".into())
                    })?;
                    let (path, _, bytes) = row_fields(record)?;
                    if path != FTS2_CONTROL_PATH {
                        self.state = FtsState::ProbeFormat { rewound: false };
                        continue;
                    }
                    self.control = Some(FtsControlV2::decode(&bytes)?);
                    if self.probe_only {
                        // Insert fast path: the store is v2; nothing else
                        // needs loading to append segments.
                        self.state = FtsState::Ready;
                        return Ok(IOResult::Done(()));
                    }
                    self.state = FtsState::ScanSegments {
                        seeked: false,
                        advance_pending: false,
                    };
                }
                FtsState::ProbeFormat { rewound } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;
                    if !*rewound {
                        return_if_io!(cursor.rewind());
                        *rewound = true;
                    }
                    if !cursor.has_record() {
                        // Empty store with no control row. `create()` always
                        // stages the control row in the same transaction as
                        // the table, so this only happens when the backing
                        // table was recreated behind the index's back. A read
                        // sees an empty index; a write must not append
                        // segments the reader will later refuse for lacking
                        // a control row.
                        if self.opening_for_write {
                            return Err(LimboError::Corrupt(format!(
                                "FTS index {name} has a backing store with no control row \
                                 and cannot be written; rebuild it with `DROP INDEX {name}` \
                                 followed by `CREATE INDEX ... USING fts`",
                                name = self.index_name
                            ))
                            .into());
                        }
                        self.segments.clear();
                        self.snapshot_loaded = true;
                        self.state = FtsState::BuildIndex;
                        continue;
                    }
                    let record = return_if_io!(cursor.record()).ok_or_else(|| {
                        LimboError::Corrupt("FTS cursor has no record payload".into())
                    })?;
                    let (path, _, _) = row_fields(record)?;
                    if path.starts_with(FTS2_PATH_PREFIX) {
                        // v2 rows must be accompanied by the control row,
                        // which sorts inside the same scan range and was not
                        // found.
                        return Err(LimboError::Corrupt(
                            "FTS v2 store has rows but no control record".into(),
                        )
                        .into());
                    }
                    // Rows without the `fts2/` prefix were written by the
                    // pre-registry FTS implementation (a whole Tantivy
                    // directory keyed by file name). That layout is not
                    // supported; the index has to be rebuilt from the base
                    // table. `DROP INDEX` does not open the store, so the
                    // rebuild always works.
                    return Err(LimboError::InvalidArgument(format!(
                        "FTS index {name} was created by an older version of Turso \
                         and its storage format is no longer supported; rebuild it \
                         with `DROP INDEX {name}` followed by `CREATE INDEX ... USING fts`",
                        name = self.index_name
                    ))
                    .into());
                }
                FtsState::ScanSegments {
                    seeked,
                    advance_pending,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;
                    if !*seeked {
                        let seek_key = seek_key_for_path(FTS2_SEGMENT_PREFIX)?;
                        let seek_result = return_if_io!(cursor.seek(
                            SeekKey::IndexKey(seek_key.as_record_ref()),
                            SeekOp::GE { eq_only: false },
                        ));
                        *seeked = true;
                        match seek_result {
                            SeekResult::NotFound => {
                                self.state = FtsState::ScanTombs {
                                    seeked: false,
                                    advance_pending: false,
                                };
                                continue;
                            }
                            SeekResult::TryAdvance => {
                                *advance_pending = true;
                            }
                            SeekResult::Found => {}
                        }
                    }
                    if *advance_pending {
                        return_if_io!(cursor.next());
                        *advance_pending = false;
                    }
                    if !cursor.has_record() {
                        self.state = FtsState::ScanTombs {
                            seeked: false,
                            advance_pending: false,
                        };
                        continue;
                    }
                    let record = return_if_io!(cursor.record()).ok_or_else(|| {
                        LimboError::Corrupt("FTS cursor has no record payload".into())
                    })?;
                    let (path, _, bytes) = row_fields(record)?;
                    let Some(uuid) = path.strip_prefix(FTS2_SEGMENT_PREFIX) else {
                        // Rows sit in key order and nothing sorts between
                        // the segment and tombstone ranges, so the only
                        // legitimate range end is a tombstone row (or no
                        // row at all, handled above). Anything else is a
                        // corrupted registry row; stopping silently here
                        // would drop every segment after it.
                        if !path.starts_with(FTS2_TOMB_PREFIX) {
                            return Err(LimboError::Corrupt(format!(
                                "FTS registry scan hit an unrecognized row: {path}"
                            ))
                            .into());
                        }
                        self.state = FtsState::ScanTombs {
                            seeked: false,
                            advance_pending: false,
                        };
                        continue;
                    };
                    let segment_id = parse_segment_id(uuid)?;
                    let descriptor = SegmentDescriptor::decode(segment_id, &bytes)?;
                    // Duplicate segment ids in one searcher trip a
                    // SearcherGeneration assert inside Tantivy; dedupe the
                    // registry scan defensively.
                    if self
                        .scan_descriptors
                        .iter()
                        .all(|existing| existing.segment_id != segment_id)
                    {
                        self.scan_descriptors.push(descriptor);
                    } else {
                        let existing = self
                            .scan_descriptors
                            .iter()
                            .find(|existing| existing.segment_id == segment_id);
                        tracing::error!(
                            segment = %segment_id.uuid_string(),
                            identical = existing == Some(&descriptor),
                            existing = ?existing,
                            duplicate = ?descriptor,
                            "duplicate FTS registry row; keeping the first"
                        );
                    }
                    *advance_pending = true;
                }
                FtsState::ScanTombs {
                    seeked,
                    advance_pending,
                } => {
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;
                    if !*seeked {
                        let seek_key = seek_key_for_path(FTS2_TOMB_PREFIX)?;
                        let seek_result = return_if_io!(cursor.seek(
                            SeekKey::IndexKey(seek_key.as_record_ref()),
                            SeekOp::GE { eq_only: false },
                        ));
                        *seeked = true;
                        match seek_result {
                            SeekResult::NotFound => {
                                self.state = self.chunk_load_state();
                                continue;
                            }
                            SeekResult::TryAdvance => {
                                *advance_pending = true;
                            }
                            SeekResult::Found => {}
                        }
                    }
                    if *advance_pending {
                        return_if_io!(cursor.next());
                        *advance_pending = false;
                    }
                    if !cursor.has_record() {
                        self.state = self.chunk_load_state();
                        continue;
                    }
                    let record = return_if_io!(cursor.record()).ok_or_else(|| {
                        LimboError::Corrupt("FTS cursor has no record payload".into())
                    })?;
                    let (path, doc_id, _) = row_fields(record)?;
                    let Some(uuid) = path.strip_prefix(FTS2_TOMB_PREFIX) else {
                        // Tombstones are the last v2 range; no row
                        // legitimately follows them. A mismatch mid-scan is
                        // a corrupted tombstone row, and stopping silently
                        // would resurrect every deleted doc after it.
                        return Err(LimboError::Corrupt(format!(
                            "FTS tombstone scan hit an unrecognized row: {path}"
                        ))
                        .into());
                    };
                    let segment_id = parse_segment_id(uuid)?;
                    let doc_id = u32::try_from(doc_id).map_err(|_| {
                        LimboError::Corrupt("FTS tombstone doc id out of range".into())
                    })?;
                    self.scan_tombs
                        .entry(segment_id)
                        .or_default()
                        .insert(doc_id);
                    *advance_pending = true;
                }
                FtsState::LoadChunks {
                    queue,
                    pos,
                    chunks,
                    seeked,
                    advance_pending,
                } => {
                    let Some(descriptor_idx) = queue.get(*pos).copied() else {
                        self.state = FtsState::BuildIndex;
                        continue;
                    };
                    let segment_id = self.scan_descriptors[descriptor_idx].segment_id;
                    let prefix = segment_chunk_prefix(&segment_id);
                    let cursor = self.fts_dir_cursor.as_mut().ok_or_else(|| {
                        LimboError::InternalError("cursor not initialized".into())
                    })?;
                    if !*seeked {
                        let seek_key = seek_key_for_path(&prefix)?;
                        let seek_result = return_if_io!(cursor.seek(
                            SeekKey::IndexKey(seek_key.as_record_ref()),
                            SeekOp::GE { eq_only: false },
                        ));
                        *seeked = true;
                        if matches!(seek_result, SeekResult::TryAdvance) {
                            *advance_pending = true;
                        }
                    }
                    if *advance_pending {
                        return_if_io!(cursor.next());
                        *advance_pending = false;
                    }
                    let mut segment_done = !cursor.has_record();
                    if !segment_done {
                        let record = return_if_io!(cursor.record()).ok_or_else(|| {
                            LimboError::Corrupt("FTS cursor has no record payload".into())
                        })?;
                        let (path, chunk_no, bytes) = row_fields(record)?;
                        match path.strip_prefix(prefix.as_str()) {
                            Some(file_ord) => {
                                let file_ord: u32 = file_ord.parse().map_err(|_| {
                                    LimboError::Corrupt(format!(
                                        "FTS chunk row has malformed file ordinal: {path}"
                                    ))
                                })?;
                                if chunks
                                    .entry(file_ord)
                                    .or_default()
                                    .insert(chunk_no, bytes)
                                    .is_some()
                                {
                                    return Err(LimboError::Corrupt(format!(
                                        "duplicate FTS chunk {path}:{chunk_no}"
                                    ))
                                    .into());
                                }
                                *advance_pending = true;
                            }
                            None => segment_done = true,
                        }
                    }
                    if segment_done {
                        let descriptor = &self.scan_descriptors[descriptor_idx];
                        let data = assemble_segment_data(descriptor, std::mem::take(chunks))?;
                        let data = Arc::new(data);
                        self.shared
                            .stats
                            .segment_loads
                            .fetch_add(1, Ordering::Relaxed);
                        self.shared.segment_bytes.lock().put(
                            descriptor.segment_id,
                            Arc::clone(&data),
                            fts_max_retained_cache_bytes(),
                        );
                        self.scan_data.insert(descriptor.segment_id, data);
                        *pos += 1;
                        *seeked = false;
                        *advance_pending = false;
                    }
                }
                FtsState::BuildIndex => {
                    if !self.snapshot_loaded {
                        // Adopt the scan results as the visible set.
                        let descriptors = std::mem::take(&mut self.scan_descriptors);
                        let mut tombs = std::mem::take(&mut self.scan_tombs);
                        let mut data_by_id = std::mem::take(&mut self.scan_data);
                        self.segments = descriptors
                            .into_iter()
                            .map(|descriptor| {
                                let id = descriptor.segment_id;
                                let data = data_by_id.remove(&id).ok_or_else(|| {
                                    LimboError::Corrupt(format!(
                                        "FTS segment {} has a registry row but no loaded data",
                                        id.uuid_string()
                                    ))
                                })?;
                                let deleted = tombs.remove(&id).unwrap_or_default();
                                // Tantivy asserts (panics) on delete counts
                                // and doc ids beyond `max_doc`; a corrupt
                                // tombstone row must error instead.
                                if deleted.last().is_some_and(|doc| *doc >= descriptor.max_doc) {
                                    return Err(LimboError::Corrupt(format!(
                                        "FTS segment {} has a tombstone past max_doc {}",
                                        id.uuid_string(),
                                        descriptor.max_doc
                                    )));
                                }
                                Ok(LoadedSegment::new(descriptor, data, deleted))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        if !tombs.is_empty() {
                            // Tombstones whose segment has no registry row.
                            // Under the merge lease a retiring merge deletes
                            // the segment's tombstone rows with it, so these
                            // only come from a bug or a damaged store. They
                            // are harmless to skip (nothing references the
                            // segment) but must not vanish silently.
                            tracing::warn!(
                                segments = tombs.len(),
                                rows = tombs.values().map(BTreeSet::len).sum::<usize>(),
                                "FTS store has tombstone rows for segments with no registry row"
                            );
                        }
                        self.snapshot_loaded = true;
                        // A full scan is ground truth for the auto-merge
                        // trigger heuristic; reconcile any drift.
                        self.shared
                            .visible_segment_estimate
                            .store(self.segments.len(), Ordering::Relaxed);
                    }
                    self.ensure_searcher()?;
                    self.state = FtsState::Ready;
                    return Ok(IOResult::Done(()));
                }
                FtsState::Ready => {
                    return Ok(IOResult::Done(()));
                }
            }
        }
    }

    /// The state that loads chunk rows for scanned descriptors the byte
    /// cache does not already hold. Cache hits are collected here.
    fn chunk_load_state(&mut self) -> FtsState {
        let mut queue = Vec::new();
        let mut cache = self.shared.segment_bytes.lock();
        for (idx, descriptor) in self.scan_descriptors.iter().enumerate() {
            match cache.get(&descriptor.segment_id) {
                Some(data) => {
                    self.scan_data.insert(descriptor.segment_id, data);
                }
                None => queue.push(idx),
            }
        }
        FtsState::LoadChunks {
            queue,
            pos: 0,
            chunks: HashMap::default(),
            seeked: false,
            advance_pending: false,
        }
    }

    /// Load the snapshot view if this cursor skipped it (the insert fast
    /// path). Needed before the first delete or same-transaction query.
    fn ensure_snapshot_loaded(&mut self) -> IOResultOr<()> {
        if self.snapshot_loaded {
            return Ok(IOResult::Done(()));
        }
        if matches!(self.state, FtsState::Ready) {
            // The fast path stopped after format detection; resume with the
            // registry scan (the control row is already decoded).
            self.state = FtsState::ScanSegments {
                seeked: false,
                advance_pending: false,
            };
        }
        self.probe_only = false;
        self.drive_open()
    }

    /// Make sure the backing table and its `backing_btree` index exist,
    /// creating them on the first `create`. Every later open takes the
    /// fast path and never prepares a statement.
    fn ensure_backing_store(
        &mut self,
        conn: &Arc<Connection>,
        database_id: usize,
    ) -> IOResultOr<()> {
        if let Some(ddl) = self.pending_ddl.as_mut() {
            return_if_io!(ddl.step());
            self.pending_ddl = None;
            return Ok(IOResult::Done(()));
        }
        let table_name = self.dir_table_name.clone();
        let index_name = format!("{table_name}_key");
        let already_exists = conn.with_schema(database_id, |schema| {
            schema.get_btree_table(&table_name).is_some()
                && schema.get_index(&table_name, &index_name).is_some()
        });
        if already_exists {
            return Ok(IOResult::Done(()));
        }
        let db_prefix = conn
            .get_database_name_by_index(database_id)
            .filter(|name| name != "main")
            .map(|name| format!("{}.", quote_identifier(&name)))
            .unwrap_or_default();
        let table_ident = quote_identifier(&table_name);
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {db_prefix}{table_ident} \
             (path TEXT NOT NULL, chunk_no INTEGER NOT NULL, bytes BLOB NOT NULL)"
        );
        // backing_btree stores all columns in the index B-tree, without
        // rowid indirection, so cursors work on the exact key structure.
        let create_index_sql = format!(
            "CREATE INDEX IF NOT EXISTS {db_prefix}{index_ident} ON {table_ident} \
             USING {method} (path, chunk_no, bytes)",
            index_ident = quote_identifier(&index_name),
            method = super::BACKING_BTREE_INDEX_METHOD_NAME,
        );
        // The store is a table plus its index, so two DDL statements run
        // here, once per CREATE INDEX, nested inside the parent statement
        // and stepped cooperatively (see [`NestedDdl`]). A helper that
        // creates a backing B-tree without going through SQL would replace
        // both.
        self.drive_nested_ddl(NestedDdl::new(conn, [create_table_sql, create_index_sql]))
    }

    /// Step freshly prepared nested DDL; park it on the cursor if it
    /// yields so the next entry resumes it.
    fn drive_nested_ddl(&mut self, mut ddl: NestedDdl) -> IOResultOr<()> {
        let result = ddl.step();
        if matches!(result, Ok(IOResult::IO(_))) {
            self.pending_ddl = Some(ddl);
        }
        result
    }

    /// Mint a fresh on-disk index incarnation. Drawn from the IO's random
    /// source when the cursor is attached to a database: two processes
    /// creating the same index in different files get different values,
    /// and the simulator's seeded IO hands out the same value on the same
    /// seed, so a seeded run replays byte for byte. Without an IO
    /// (connection-less unit tests) a process-local counter keeps values
    /// distinct.
    fn mint_index_incarnation(&self) -> u64 {
        let root = self.btree_root_page.unwrap_or_default() as u64;
        let entropy = self
            .io_random_u64()
            .unwrap_or_else(|| NEXT_FTS_INDEX_INCARNATION.fetch_add(1, Ordering::Relaxed));
        (root.rotate_left(32) ^ entropy).max(1)
    }

    /// One 64-bit draw from the pager's IO random source, if the cursor is
    /// attached to a database.
    fn io_random_u64(&self) -> Option<u64> {
        self.connection
            .as_ref()
            .and_then(Weak::upgrade)
            .zip(self.database_id)
            .and_then(|(conn, database_id)| conn.get_pager_from_database_index(&database_id).ok())
            .map(|pager| pager.io.generate_random_number() as u64)
    }

    /// Mint the id of a segment this cursor is about to build. The id is
    /// the key prefix of every backing row, so it decides B-tree layout;
    /// drawing it from the IO random source keeps seeded simulator runs
    /// replaying byte for byte (tantivy's own ids come from OS entropy).
    /// Without an IO, tantivy's random id is used.
    fn mint_segment_id(&self) -> SegmentId {
        let (Some(hi), Some(lo)) = (self.io_random_u64(), self.io_random_u64()) else {
            return SegmentId::generate_random();
        };
        let value = (u128::from(hi) << 64) | u128::from(lo);
        SegmentId::from_uuid_string(&format!("{value:032x}"))
            .expect("32 hex digits are a valid simple uuid")
    }

    /// Build one immutable segment from the buffered documents (if any) and
    /// stage its rows, plus any pending tombstone rows, for publication.
    fn stage_flush(&mut self) -> Result<()> {
        turso_assert!(
            self.publish.is_none(),
            "FTS staged a flush while a publication is in flight"
        );
        let mut inserts = Vec::new();
        let mut new_segment = None;
        if !self.doc_buffer.is_empty() {
            let (segment, rows) = self.build_segment()?;
            if let Some(segment) = segment {
                inserts.extend(rows);
                self.own_published.push(segment.id());
                self.shared.segment_bytes.lock().put(
                    segment.id(),
                    Arc::clone(&segment.data),
                    fts_max_retained_cache_bytes(),
                );
                new_segment = Some(segment);
            }
        }
        for (segment_id, doc_id) in self.pending_tombstone_rows.drain(..) {
            inserts.push(PendingRow {
                path: segment_tombstone_path(&segment_id),
                chunk_no: i64::from(doc_id),
                bytes: Vec::new(),
            });
        }
        self.doc_buffer.clear();
        if inserts.is_empty() && new_segment.is_none() {
            return Ok(());
        }
        self.publish = Some(PendingPublish {
            inserter: Some(RowInserter::new(inserts)),
            deleter: None,
            apply: PublishApply::AppendSegment(new_segment),
        });
        Ok(())
    }

    /// Serialize the buffered documents into one immutable segment through
    /// a private `BuildDirectory`, and return it with its backing rows.
    ///
    /// This is what `IndexWriter`'s worker thread does internally, minus the
    /// `SegmentUpdater` handoff: no `meta.json` write reaches storage, no
    /// `.managed.json`, no lock file, no merge. Returns `None` when the
    /// buffer produced no documents.
    fn build_segment(&mut self) -> Result<(Option<LoadedSegment>, Vec<PendingRow>)> {
        let build_dir = BuildDirectory::default();
        // `Index::create` writes the initial meta.json into the build
        // directory's in-memory slot; it never reaches the B-tree.
        let index = Index::create(
            build_dir.clone(),
            self.schema.clone(),
            IndexSettings::default(),
        )
        .map_err(|e| LimboError::InternalError(format!("FTS build index: {e}")))?;
        // The segment writer pulls tokenizers off `segment.index()`, so the
        // build index needs the same registrations as the read side.
        self.register_tokenizers(&index);
        let segment_id = self.mint_segment_id();
        let segment = index.segment(index.new_segment_meta(segment_id, 0));
        let mut writer = SegmentWriter::for_segment(DEFAULT_MEMORY_BUDGET_BYTES, segment)
            .map_err(|e| LimboError::InternalError(format!("FTS segment writer: {e}")))?;
        for buffered in self.doc_buffer.drain(..) {
            writer
                .add_document(AddOperation {
                    // Opstamps are never persisted in segment data; they only
                    // order deletes inside IndexWriter, which does not exist
                    // here.
                    opstamp: 0,
                    document: buffered.doc,
                })
                .map_err(|e| LimboError::InternalError(format!("FTS add_document: {e}")))?;
        }
        let max_doc = writer.max_doc();
        if max_doc == 0 {
            return Ok((None, Vec::new()));
        }
        writer
            .finalize()
            .map_err(|e| LimboError::InternalError(format!("FTS segment finalize: {e}")))?;
        self.shared
            .stats
            .segment_builds
            .fetch_add(1, Ordering::Relaxed);

        let captured = build_dir.captured_files();
        segment_rows_from_files(segment_id, max_doc, captured)
    }

    /// Drive the in-flight publication (row deletions, then row inserts,
    /// then in-memory effects exactly once).
    fn drive_publish(&mut self) -> IOResultOr<()> {
        let Some(publish) = self.publish.as_mut() else {
            return Ok(IOResult::Done(()));
        };
        let cursor = self
            .fts_dir_cursor
            .as_mut()
            .ok_or_else(|| LimboError::InternalError("cursor not initialized".into()))?;
        if let Some(deleter) = publish.deleter.as_mut() {
            return_if_io!(deleter.step(cursor.as_mut()));
            publish.deleter = None;
        }
        if let Some(inserter) = publish.inserter.as_mut() {
            return_if_io!(inserter.step(cursor.as_mut()));
            publish.inserter = None;
        }
        let publish = self.publish.take().expect("publication checked above");
        match publish.apply {
            PublishApply::Nothing => {}
            PublishApply::AppendSegment(new_segment) => {
                if let Some(segment) = new_segment {
                    self.segments.push(segment);
                    self.shared
                        .visible_segment_estimate
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.invalidate_snapshot_view();
            }
            PublishApply::ReplaceSegments(segments) => {
                self.segments = segments;
                self.shared
                    .visible_segment_estimate
                    .store(self.segments.len(), Ordering::Relaxed);
                self.invalidate_snapshot_view();
            }
        }
        Ok(IOResult::Done(()))
    }

    /// Merge the given subset of the loaded snapshot's visible segments
    /// into one (compacting tombstones away) and stage the result for
    /// publication: retire every candidate segment's rows, insert the
    /// merged segment's rows, keep the rest untouched. The caller must hold
    /// the writer slot and the maintenance lease, and drive the staged
    /// publication afterwards. OPTIMIZE passes every visible segment; the
    /// write-path auto-merge passes its tiered candidates.
    fn stage_merge_of_segments(&mut self, candidate_ids: &HashSet<SegmentId>) -> Result<()> {
        self.ensure_searcher()?;
        let index = self
            .index
            .as_ref()
            .ok_or_else(|| LimboError::InternalError("FTS index not initialized".into()))?;
        // Merge inputs come from the snapshot view, whose synthesized
        // `.del` files carry this snapshot's tombstones — the merge compacts
        // deletes away for free. The merger silently drops inputs whose
        // meta claims zero live docs, which is exactly what retiring a
        // fully-tombstoned segment needs.
        let input_metas = index
            .searchable_segment_metas()
            .map_err(|e| LimboError::InternalError(format!("FTS optimize: {e}")))?;
        let input_segments: Vec<tantivy::Segment> = input_metas
            .iter()
            .filter(|meta| candidate_ids.contains(&meta.id()))
            .map(|meta| index.segment(meta.clone()))
            .collect();
        let build_dir = BuildDirectory::default();
        let live_total: u64 = self
            .segments
            .iter()
            .filter(|segment| candidate_ids.contains(&segment.id()))
            .map(LoadedSegment::live_docs)
            .sum();
        let merged = if live_total > 0 {
            let merged_index = tantivy::indexer::merge_filtered_segments(
                &input_segments,
                IndexSettings::default(),
                vec![None; input_segments.len()],
                build_dir.clone(),
            )
            .map_err(|e| LimboError::InternalError(format!("FTS merge failed: {e}")))?;
            let merged_metas = merged_index
                .searchable_segment_metas()
                .map_err(|e| LimboError::InternalError(format!("FTS merged metas: {e}")))?;
            let merged_meta = merged_metas
                .first()
                .ok_or_else(|| LimboError::InternalError("FTS merge produced no segment".into()))?;
            // `merge_filtered_segments` names its output after an OS-random
            // id; re-key the files to a minted one (see `mint_segment_id`).
            // Segment file bytes never embed the id, only the names do.
            let segment_id = self.mint_segment_id();
            let captured =
                rename_segment_files(build_dir.captured_files(), &merged_meta.id(), &segment_id)?;
            let (segment, rows) =
                segment_rows_from_files(segment_id, merged_meta.max_doc(), captured)?;
            self.shared
                .stats
                .segment_builds
                .fetch_add(1, Ordering::Relaxed);
            segment.map(|segment| (segment, rows))
        } else {
            // Every visible document is tombstoned: retire everything and
            // publish nothing.
            None
        };

        // Retire the inputs: descriptor, chunk, and tombstone rows of every
        // merged segment. Old snapshots keep seeing them through their MVCC
        // version chains until GC's low-water mark passes them. Segments
        // outside the candidate set survive untouched.
        let mut deletes = Vec::new();
        let mut new_segments = Vec::new();
        for segment in &self.segments {
            let id = segment.id();
            if candidate_ids.contains(&id) {
                deletes.push(PathTarget::Exact(segment_registry_path(&id)));
                deletes.push(PathTarget::Prefix(segment_chunk_prefix(&id)));
                deletes.push(PathTarget::Exact(segment_tombstone_path(&id)));
                self.shared.segment_bytes.lock().remove(&id);
            } else {
                new_segments.push(segment.clone());
            }
        }
        let mut inserts = Vec::new();
        if let Some((segment, rows)) = merged {
            tracing::debug!(
                inputs = candidate_ids.len(),
                survivors = new_segments.len(),
                merged_docs = segment.descriptor.max_doc,
                "FTS merge: merged candidate segments"
            );
            inserts = rows;
            self.own_published.push(segment.id());
            self.shared.segment_bytes.lock().put(
                segment.id(),
                Arc::clone(&segment.data),
                fts_max_retained_cache_bytes(),
            );
            new_segments.push(segment);
        }
        self.publish = Some(PendingPublish {
            inserter: Some(RowInserter::new(inserts)),
            deleter: Some(RowDeleter::new(deletes)),
            apply: PublishApply::ReplaceSegments(new_segments),
        });
        Ok(())
    }

    /// Which visible segments the write-path merge should rewrite (B2):
    /// - a segment at least half tombstoned is always a candidate —
    ///   rewriting reclaims its dead space no matter how big it is;
    /// - otherwise candidates are every clean segment at or below the
    ///   smallest size layer holding at least two of them, so one huge
    ///   segment is not rewritten on every trigger.
    ///
    /// Returns an empty set when nothing is worth rewriting (no pair of
    /// mergeable clean segments and no tombstone-heavy segment).
    fn auto_merge_candidates(&self) -> HashSet<SegmentId> {
        /// ParadeDB-style size layers. Segments at or past the last
        /// boundary never merge on size grounds.
        const FTS_MERGE_LAYER_BYTES: [u64; 4] = [100 << 10, 1 << 20, 100 << 20, 1 << 30];
        fn segment_bytes(segment: &LoadedSegment) -> u64 {
            segment.descriptor.files.iter().map(|file| file.size).sum()
        }

        let mut candidates: HashSet<SegmentId> = self
            .segments
            .iter()
            .filter(|segment| {
                segment.descriptor.max_doc > 0
                    && segment.deleted.len() as u64 * 2 >= u64::from(segment.descriptor.max_doc)
            })
            .map(LoadedSegment::id)
            .collect();
        for ceiling in FTS_MERGE_LAYER_BYTES {
            let group: Vec<SegmentId> = self
                .segments
                .iter()
                .filter(|segment| {
                    !candidates.contains(&segment.id()) && segment_bytes(segment) <= ceiling
                })
                .map(LoadedSegment::id)
                .collect();
            if group.len() >= 2 {
                candidates.extend(group);
                return candidates;
            }
        }
        // No clean tier is mergeable; rewriting only pays off if a
        // tombstone-heavy segment reclaims space.
        candidates
    }

    /// After a statement flush published a new segment, merge the visible
    /// set down if it exceeds the connection's `fts_merge_threshold`. Runs
    /// inside the same transaction, under the same lease and admissibility
    /// rules as OPTIMIZE — so a refused lease (`Busy` /
    /// `WriteWriteConflict`) skips the merge silently: a writer must never
    /// fail because maintenance was contended.
    fn try_auto_merge(&mut self) -> Result<IOResult<()>> {
        // Re-entry after an IO yield inside the merge publication: the
        // pending flag was already cleared when the merge was staged, so
        // this only handles yields from the snapshot scan below.
        let Some(conn) = self.connection.as_ref().and_then(Weak::upgrade) else {
            self.auto_merge_pending = false;
            return Ok(IOResult::Done(()));
        };
        let threshold = conn.get_fts_merge_threshold();
        if threshold <= 0 {
            self.auto_merge_pending = false;
            return Ok(IOResult::Done(()));
        }
        // Cheap pre-check: below the threshold, a flushed statement must pay
        // nothing beyond this load — the estimate keeps the insert fast path
        // scan-free. Over-estimates cost one wasted scan; under-estimates
        // delay the merge until the next reconciling scan.
        if self
            .shared
            .visible_segment_estimate
            .load(Ordering::Relaxed)
            .max(self.segments.len())
            <= threshold as usize
        {
            self.auto_merge_pending = false;
            return Ok(IOResult::Done(()));
        }
        // The insert fast path stops after format detection; counting the
        // visible set needs the full registry scan (resumable on IO).
        return_if_io!(self.ensure_snapshot_loaded());
        if self.segments.len() <= threshold as usize {
            self.auto_merge_pending = false;
            return Ok(IOResult::Done(()));
        }
        match self.acquire_mvcc_maintenance_lease() {
            Ok(()) => {}
            Err(LimboError::Busy | LimboError::WriteWriteConflict) => {
                tracing::debug!("FTS auto-merge: lease contended, skipping");
                self.auto_merge_pending = false;
                return Ok(IOResult::Done(()));
            }
            Err(err) => return Err(err),
        }
        if let Some((mv_store, tx_id, index_id)) = self.mvcc_index_id(
            &conn,
            self.database_id
                .ok_or_else(|| LimboError::InternalError("FTS database id not set".into()))?,
        )? {
            match mv_store.check_index_method_merge_admissible(tx_id, index_id) {
                Ok(()) => {}
                Err(LimboError::Busy | LimboError::WriteWriteConflict) => {
                    tracing::debug!("FTS auto-merge: deleter overlap, skipping");
                    self.auto_merge_pending = false;
                    return Ok(IOResult::Done(()));
                }
                Err(err) => return Err(err),
            }
        }
        // Tiered candidacy: rewrite the small tier and tombstone-heavy
        // segments, never a big clean segment on every trigger.
        let candidates = self.auto_merge_candidates();
        if candidates.is_empty() {
            tracing::debug!("FTS auto-merge: no tier is worth rewriting, skipping");
            self.auto_merge_pending = false;
            return Ok(IOResult::Done(()));
        }
        self.stage_merge_of_segments(&candidates)?;
        // Clear before driving: a yield inside the publication resumes
        // through `stage_statement_commit`'s is_publishing branch, which
        // must not evaluate the trigger again.
        self.auto_merge_pending = false;
        return_if_io!(self.drive_publish());
        Ok(IOResult::Done(()))
    }

    /// Complete any in-flight or due batch publication before a mutation.
    /// The VDBE retries the current instruction when an operation returns
    /// `IOResult::IO`, so this must finish before Tantivy state changes.
    fn flush_gate(&mut self) -> IOResultOr<()> {
        if self.is_publishing() {
            return_if_io!(self.drive_publish());
        }
        // `>=`, not `==`: a delete can push several tombstones past the gate
        // in one step (one per live posting for the rowid), so the count can
        // legitimately overshoot the batch size between gates.
        if self.pending_op_count() >= BATCH_COMMIT_SIZE {
            self.stage_flush()?;
            return_if_io!(self.drive_publish());
        }
        Ok(IOResult::Done(()))
    }

    /// Locate every live posting for `rowid` across the in-memory segment
    /// set. One rowid-term lookup per segment, checked against the
    /// transaction's own tombstone state (the searcher's alive bitsets may
    /// lag behind tombstones applied since the view was built).
    fn live_postings_for_rowid(&mut self, rowid: i64) -> Result<Vec<(SegmentId, u32)>> {
        if self.segments.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_searcher()?;
        let searcher = self
            .searcher
            .as_ref()
            .expect("searcher built by ensure_searcher");
        let term = Term::from_field_i64(self.rowid_field, rowid);
        let mut hits = Vec::new();
        for segment_reader in searcher.segment_readers() {
            let segment_id = segment_reader.segment_id();
            let Some(segment) = self
                .segments
                .iter()
                .find(|segment| segment.id() == segment_id)
            else {
                continue;
            };
            let inverted = segment_reader
                .inverted_index(self.rowid_field)
                .map_err(|e| LimboError::InternalError(format!("FTS rowid lookup: {e}")))?;
            let Some(mut postings) = inverted
                .read_postings(&term, IndexRecordOption::Basic)
                .map_err(|e| LimboError::InternalError(format!("FTS rowid postings: {e}")))?
            else {
                continue;
            };
            loop {
                let doc_id = postings.doc();
                if doc_id == TERMINATED {
                    break;
                }
                // Raw postings are not alive-filtered; consult the
                // transaction's own tombstone state.
                if !segment.deleted.contains(&doc_id) {
                    hits.push((segment_id, doc_id));
                }
                postings.advance();
            }
        }
        Ok(hits)
    }

    fn constant_integer_expression(expr: &turso_parser::ast::Expr) -> Option<i64> {
        match expr {
            turso_parser::ast::Expr::Parenthesized(expressions) if expressions.len() == 1 => {
                Self::constant_integer_expression(&expressions[0])
            }
            _ => match crate::util::parse_signed_number(expr).ok()? {
                Value::Numeric(crate::numeric::Numeric::Integer(value)) => Some(value),
                Value::Numeric(crate::numeric::Numeric::Float(value)) => {
                    crate::util::cast_real_to_integer(f64::from(value)).ok()
                }
                Value::Null | Value::Text(_) | Value::Blob(_) => None,
            },
        }
    }

    /// Purge this transaction's published-but-uncommitted segments from the
    /// shared caches (rollback paths).
    fn purge_own_published(&mut self) {
        if self.own_published.is_empty() {
            return;
        }
        let mut bytes = self.shared.segment_bytes.lock();
        for id in &self.own_published {
            bytes.remove(id);
        }
        drop(bytes);
        self.shared
            .searchers
            .lock()
            .purge_segments(&self.own_published);
        self.own_published.clear();
    }

    /// Reset every piece of per-transaction state. Used by abort, rollback,
    /// savepoint rollback (coarse: the next access rescans from the
    /// correctly reverted backing rows), and close.
    fn reset_to_init(&mut self) {
        self.release_writer_slot();
        self.doc_buffer.clear();
        self.pending_tombstone_rows.clear();
        self.publish = None;
        self.auto_merge_pending = false;
        self.segments.clear();
        self.snapshot_loaded = false;
        self.scan_descriptors.clear();
        self.scan_tombs.clear();
        self.scan_data.clear();
        self.control = None;
        self.invalidate_snapshot_view();
        self.fts_dir_cursor = None;
        self.current_hits.clear();
        self.streaming_hits = None;
        self.hit_pos = 0;
        self.registered_deleter = false;
        self.probe_only = false;
        self.opening_for_write = false;
        self.state = FtsState::Init;
    }
}

/// Assemble one segment's files from its scanned chunk rows, validating
/// them against the descriptor.
fn assemble_segment_data(
    descriptor: &SegmentDescriptor,
    mut chunks: HashMap<u32, HashMap<i64, Vec<u8>>>,
) -> Result<SegmentData> {
    let mut files: HashMap<String, Arc<[u8]>> = HashMap::default();
    for (file_ord, entry) in descriptor.files.iter().enumerate() {
        let file_ord = file_ord as u32;
        let chunk_map = chunks.remove(&file_ord).ok_or_else(|| {
            LimboError::Corrupt(format!(
                "FTS segment {} is missing chunks for file {}",
                descriptor.segment_id.uuid_string(),
                entry.name
            ))
        })?;
        if chunk_map.len() != entry.num_chunks as usize {
            return Err(LimboError::Corrupt(format!(
                "FTS segment file {} has {} chunks but the descriptor records {}",
                entry.name,
                chunk_map.len(),
                entry.num_chunks
            )));
        }
        let assembled = assemble_chunks(std::path::Path::new(&entry.name), chunk_map)?;
        if assembled.len() as u64 != entry.size {
            return Err(LimboError::Corrupt(format!(
                "FTS segment file {} has {} bytes but the descriptor records {}",
                entry.name,
                assembled.len(),
                entry.size
            )));
        }
        files.insert(entry.name.clone(), assembled);
    }
    if !chunks.is_empty() {
        return Err(LimboError::Corrupt(format!(
            "FTS segment {} stores chunks for files absent from its descriptor",
            descriptor.segment_id.uuid_string()
        )));
    }
    Ok(SegmentData::new(files))
}

/// Concatenate one file's chunk rows (`chunk_no` → bytes) into whole bytes.
///
/// The chunks are written straight into the shared allocation: building a
/// `Vec` first and converting it with `Arc::from` would copy every byte a
/// second time and hold both copies at once. Each chunk is dropped as soon
/// as it has been copied, so peak memory is the file plus one chunk.
fn assemble_chunks(path: &std::path::Path, mut chunks: HashMap<i64, Vec<u8>>) -> Result<Arc<[u8]>> {
    let max_chunk =
        chunks.keys().max().copied().ok_or_else(|| {
            LimboError::Corrupt(format!("FTS file {} has no chunks", path.display()))
        })?;
    if max_chunk < 0 {
        return Err(LimboError::Corrupt(format!(
            "FTS file {} has a negative chunk number",
            path.display()
        )));
    }
    let total: usize = chunks.values().map(Vec::len).sum();
    let mut assembled = Arc::<[u8]>::new_uninit_slice(total);
    let buffer = Arc::get_mut(&mut assembled).expect("a freshly allocated Arc is unique");
    let mut offset = 0;
    for chunk_no in 0..=max_chunk {
        let data = chunks.remove(&chunk_no).ok_or_else(|| {
            LimboError::Corrupt(format!(
                "FTS file {} is missing chunk {}",
                path.display(),
                chunk_no
            ))
        })?;
        for (slot, byte) in buffer[offset..offset + data.len()].iter_mut().zip(&data) {
            slot.write(*byte);
        }
        offset += data.len();
    }
    if !chunks.is_empty() {
        // Keys outside `0..=max_chunk` (a negative chunk number next to
        // valid ones) were counted into `total` but never written.
        return Err(LimboError::Corrupt(format!(
            "FTS file {} has chunk numbers outside 0..={}",
            path.display(),
            max_chunk
        )));
    }
    turso_assert!(
        offset == total,
        "FTS chunk assembly must write exactly the bytes it counted"
    );
    // SAFETY: `total` is the sum of every chunk's length and every chunk was
    // consumed by the loop above exactly once, writing `total` bytes
    // contiguously from offset 0 (asserted), so every byte of the slice is
    // initialized.
    Ok(unsafe { assembled.assume_init() })
}

/// Turn a built segment's captured files into a `LoadedSegment` plus its
/// descriptor and chunk rows.
/// Re-key captured segment files from segment id `from` to `to`. Tantivy
/// names every component `<segment uuid>.<ext>`; the bytes never carry the
/// id, so a rename is all a merged segment needs to take a minted id.
fn rename_segment_files(
    files: HashMap<PathBuf, Arc<[u8]>>,
    from: &SegmentId,
    to: &SegmentId,
) -> Result<HashMap<PathBuf, Arc<[u8]>>> {
    let from = from.uuid_string();
    let to = to.uuid_string();
    files
        .into_iter()
        .map(|(path, bytes)| {
            let rest = path
                .to_str()
                .and_then(|name| name.strip_prefix(from.as_str()))
                .ok_or_else(|| {
                    LimboError::InternalError(format!(
                        "FTS merge wrote a file that is not named after its segment: {}",
                        path.display()
                    ))
                })?;
            Ok((PathBuf::from(format!("{to}{rest}")), bytes))
        })
        .collect()
}

fn segment_rows_from_files(
    segment_id: SegmentId,
    max_doc: u32,
    captured: HashMap<PathBuf, Arc<[u8]>>,
) -> Result<(Option<LoadedSegment>, Vec<PendingRow>)> {
    let mut file_names: Vec<String> = captured
        .keys()
        .filter_map(|path| path.to_str().map(str::to_string))
        .collect();
    // Deterministic file order keeps descriptors stable.
    file_names.sort();
    let mut inserts = Vec::new();
    let mut entries = Vec::new();
    let mut data_files: HashMap<String, Arc<[u8]>> = HashMap::default();
    for (file_ord, name) in file_names.into_iter().enumerate() {
        let bytes = captured
            .get(std::path::Path::new(&name))
            .expect("name enumerated from this map");
        let file_ord = u32::try_from(file_ord)
            .map_err(|_| LimboError::InternalError("FTS segment has too many files".into()))?;
        let chunk_path = segment_chunk_path(&segment_id, file_ord);
        let rows = chunk_rows(&chunk_path, bytes, DEFAULT_CHUNK_SIZE);
        entries.push(SegmentFileEntry {
            name: name.clone(),
            size: bytes.len() as u64,
            num_chunks: rows.len() as u32,
        });
        inserts.extend(rows);
        data_files.insert(name, Arc::clone(bytes));
    }
    let descriptor = SegmentDescriptor {
        segment_id,
        max_doc,
        files: entries,
    };
    inserts.push(PendingRow {
        path: segment_registry_path(&segment_id),
        chunk_no: 0,
        bytes: descriptor.encode()?,
    });
    let segment = LoadedSegment::new(
        descriptor,
        Arc::new(SegmentData::new(data_files)),
        BTreeSet::new(),
    );
    Ok((Some(segment), inserts))
}

/// Test helper: delete every row of an FTS index's backing store, leaving a
/// store that exists but has no control row. Drive `step` to completion
/// inside a write transaction. Only reachable state this simulates: the
/// backing table recreated behind the index's back.
#[cfg(feature = "test_helper")]
pub struct FtsBackingRowWiper {
    cursor: Box<dyn CursorTrait>,
    deleter: RowDeleter,
}

#[cfg(feature = "test_helper")]
impl FtsBackingRowWiper {
    pub fn new(conn: &Arc<Connection>, database_id: usize, index_name: &str) -> Result<Self> {
        let dir_table_name = format!(
            "{}fts_dir_{}",
            crate::schema::TURSO_INTERNAL_PREFIX,
            index_name
        );
        let key_index_name = format!("{dir_table_name}_key");
        let cursor = open_index_cursor(
            conn,
            database_id,
            &dir_table_name,
            &key_index_name,
            [key_info(), key_info(), key_info()],
        )?;
        Ok(Self {
            cursor,
            // Every path is a prefix match for the empty string.
            deleter: RowDeleter::new(vec![PathTarget::Prefix(String::new())]),
        })
    }

    pub fn step(&mut self) -> IOResultOr<()> {
        self.deleter.step(self.cursor.as_mut())
    }
}

/// Test helper: enumerate every raw row of an FTS index's backing store as
/// `(path, chunk_no, byte_len, fnv64-of-bytes)`. Drive `step` to completion
/// inside a read transaction; rows land in `rows` in key order, including
/// physical duplicates the reader normally dedupes.
#[cfg(feature = "test_helper")]
pub struct FtsBackingRowDumper {
    cursor: Box<dyn CursorTrait>,
    started: bool,
    advance_pending: bool,
    pub rows: Vec<(String, i64, usize, u64)>,
}

#[cfg(feature = "test_helper")]
impl FtsBackingRowDumper {
    pub fn new(conn: &Arc<Connection>, database_id: usize, index_name: &str) -> Result<Self> {
        let dir_table_name = format!(
            "{}fts_dir_{}",
            crate::schema::TURSO_INTERNAL_PREFIX,
            index_name
        );
        let key_index_name = format!("{dir_table_name}_key");
        let cursor = open_index_cursor(
            conn,
            database_id,
            &dir_table_name,
            &key_index_name,
            [key_info(), key_info(), key_info()],
        )?;
        Ok(Self {
            cursor,
            started: false,
            advance_pending: false,
            rows: Vec::new(),
        })
    }

    pub fn step(&mut self) -> IOResultOr<()> {
        loop {
            if !self.started {
                return_if_io!(self.cursor.rewind());
                self.started = true;
            }
            if self.advance_pending {
                return_if_io!(self.cursor.next());
                self.advance_pending = false;
            }
            if !self.cursor.has_record() {
                return Ok(IOResult::Done(()));
            }
            let record = return_if_io!(self.cursor.record()).ok_or_else(|| {
                LimboError::Corrupt("FTS dump cursor has no record payload".into())
            })?;
            let (path, chunk_no, bytes) = row_fields(record)?;
            let hash = bytes.iter().fold(0xcbf2_9ce4_8422_2325u64, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
            });
            self.rows.push((path, chunk_no, bytes.len(), hash));
            self.advance_pending = true;
        }
    }
}

// ============================ trait impl ============================

impl Drop for FtsCursor {
    fn drop(&mut self) {
        self.release_writer_slot();
        let is_publishing = self.is_publishing();
        if self.pending_op_count() != 0 || is_publishing {
            tracing::error!(
                pending_ops = self.pending_op_count(),
                is_publishing,
                "FTS cursor dropped before explicit statement finalization"
            );
            debug_assert!(
                crate::thread::panicking(),
                "FTS cursor dropped with pending persistence work"
            );
        }
    }
}

impl IndexMethodCursor for FtsCursor {
    /// Creates the FTS index storage (internal BTree table for segment
    /// rows) and stages the v2 control row.
    fn create(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        let conn = context.connection()?;
        let database_id = context.database().id;
        self.database_id = Some(database_id);
        self.connection = Some(Arc::downgrade(&conn));
        if self.is_publishing() {
            return_if_io!(self.drive_publish());
            return Ok(IOResult::Done(()));
        }
        return_if_io!(self.ensure_backing_store(&conn, database_id));
        self.open_cursor(&conn, database_id)?;
        self.claim_writer_slot()?;
        let control = FtsControlV2::new(self.mint_index_incarnation());
        self.publish = Some(PendingPublish {
            inserter: Some(RowInserter::new(vec![PendingRow {
                path: FTS2_CONTROL_PATH.to_string(),
                chunk_no: 0,
                bytes: control.encode(),
            }])),
            deleter: None,
            apply: PublishApply::Nothing,
        });
        self.control = Some(control);
        // A fresh index has an empty visible set; the CREATE INDEX
        // statement's population inserts can proceed without a scan.
        self.segments.clear();
        self.snapshot_loaded = true;
        self.state = FtsState::Ready;
        return_if_io!(self.drive_publish());
        Ok(IOResult::Done(()))
    }

    /// Destroys the FTS index, dropping all storage and clearing caches.
    fn destroy(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        let conn = context.connection()?;
        let database_id = context.database().id;
        self.database_id = Some(database_id);
        self.connection = Some(Arc::downgrade(&conn));
        if let Some(ddl) = self.pending_ddl.as_mut() {
            // Resuming the DROP TABLE below after an I/O yield.
            return_if_io!(ddl.step());
            self.pending_ddl = None;
            self.state = FtsState::Init;
            return Ok(IOResult::Done(()));
        }
        tracing::debug!(
            "FTS destroy: dropping internal storage {}",
            self.dir_table_name
        );

        // Teardown retires every row, like a merge: serialize with
        // maintenance through the same slot and merge mutex.
        self.open_cursor(&conn, database_id)?;
        self.claim_writer_slot()?;
        self.acquire_mvcc_maintenance_lease()?;

        // Drop in-memory state and shared caches. The drop is not committed
        // yet, but a recreated index mints fresh segment ids, so no stale
        // cache entry can ever validate against it; clearing is safe and
        // frees the bytes immediately.
        self.segments.clear();
        self.snapshot_loaded = false;
        self.invalidate_snapshot_view();
        self.fts_dir_cursor = None;
        self.control = None;
        *self.shared.segment_bytes.lock() = SegmentByteCache::default();
        *self.shared.searchers.lock() = SearcherCache::default();

        // Drop the internal storage table; the backing_btree index is
        // dropped automatically with it. Nested inside the parent DROP
        // INDEX statement and stepped cooperatively (see [`NestedDdl`]).
        let db_prefix = conn
            .get_database_name_by_index(database_id)
            .filter(|name| name != "main")
            .map(|name| format!("{}.", quote_identifier(&name)))
            .unwrap_or_default();
        let drop_table_sql = format!(
            "DROP TABLE IF EXISTS {db_prefix}{}",
            quote_identifier(&self.dir_table_name)
        );
        return_if_io!(self.drive_nested_ddl(NestedDdl::new(&conn, [drop_table_sql])));

        self.state = FtsState::Init;
        Ok(IOResult::Done(()))
    }

    /// Opens the index for reading: scan the visible registry rows at this
    /// snapshot and assemble a searcher over exactly those segments.
    fn open_read(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        let conn = context.connection()?;
        let database_id = context.database().id;
        self.database_id = Some(database_id);
        self.connection = Some(Arc::downgrade(&conn));
        if matches!(self.state, FtsState::Ready) {
            return self.ensure_snapshot_loaded();
        }
        self.probe_only = false;
        self.drive_open()
    }

    /// Opens the index for writing. Pure inserts only need to know the
    /// store's format — they append segments without reading the index —
    /// so this stops after format detection.
    fn open_write(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        let conn = context.connection()?;
        let database_id = context.database().id;
        self.database_id = Some(database_id);
        self.connection = Some(Arc::downgrade(&conn));
        self.opening_for_write = true;
        if matches!(self.state, FtsState::Ready) {
            return Ok(IOResult::Done(()));
        }
        return_if_io!(self.ensure_backing_store(&conn, database_id));
        if !self.snapshot_loaded {
            self.probe_only = true;
        }
        let result = self.drive_open();
        if !matches!(result, Ok(IOResult::IO(_))) {
            self.probe_only = false;
            self.opening_for_write = false;
        }
        result
    }

    /// Buffers a document for the next segment build. Values are text
    /// columns followed by rowid.
    fn insert(&mut self, values: &[Register]) -> IOResultOr<()> {
        self.claim_writer_slot()?;
        return_if_io!(self.flush_gate());

        // Last register is rowid
        let rowid_reg = values.last().ok_or_else(|| {
            LimboError::InternalError("FTS insert requires at least rowid".into())
        })?;
        let rowid = match rowid_reg {
            Register::Value(Value::Numeric(crate::numeric::Numeric::Integer(i))) => *i,
            _ => {
                return Err(LimboError::InternalError("FTS rowid must be integer".into()).into());
            }
        };

        let mut doc = TantivyDocument::default();
        doc.add_i64(self.rowid_field, rowid);

        for ((_col, field), reg) in self.text_fields.iter().zip(&values[..values.len() - 1]) {
            match reg {
                Register::Value(Value::Text(t)) => {
                    doc.add_text(*field, t.as_str());
                }
                Register::Value(Value::Null) => continue,
                // Coerce every non-NULL value to text before tokenizing, the
                // way FTS5's sqlite3_value_text() does. Skipping them would
                // make the index silently miss rows a plain scan matches.
                Register::Value(value) => {
                    if let Some(text) = value.cast_text() {
                        doc.add_text(*field, &text);
                    }
                }
                _ => continue,
            }
        }

        self.doc_buffer.push(BufferedDoc { rowid, doc });
        Ok(IOResult::Done(()))
    }

    /// Deletes a document by rowid: drop it from the buffer if it has not
    /// been serialized yet, and tombstone every live posting it has in the
    /// visible segment set.
    fn delete(&mut self, values: &[Register]) -> IOResultOr<()> {
        self.claim_writer_slot()?;
        // Announce this transaction as a tombstone writer before any
        // tombstone exists, so a concurrent merge cannot retire the
        // segments out from under it.
        self.register_mvcc_deleter()?;
        return_if_io!(self.flush_gate());
        // A delete must see the visible segment set; the insert fast path
        // skips loading it.
        return_if_io!(self.ensure_snapshot_loaded());

        // Last register is rowid
        let rowid_reg = values.last().ok_or_else(|| {
            LimboError::InternalError("FTS delete requires at least rowid".into())
        })?;
        let rowid = match rowid_reg {
            Register::Value(Value::Numeric(crate::numeric::Numeric::Integer(i))) => *i,
            _ => {
                return Err(LimboError::InternalError("FTS rowid must be integer".into()).into());
            }
        };

        // The transaction's own unflushed documents are simply un-buffered.
        self.doc_buffer.retain(|buffered| buffered.rowid != rowid);

        // Postings in visible segments get tombstone rows, applied to the
        // in-memory set immediately (own-write visibility) and queued as
        // rows for the next flush.
        let postings = self.live_postings_for_rowid(rowid)?;
        for (segment_id, doc_id) in postings {
            if let Some(segment) = self
                .segments
                .iter_mut()
                .find(|segment| segment.id() == segment_id)
            {
                if segment.deleted.insert(doc_id) {
                    self.pending_tombstone_rows.push((segment_id, doc_id));
                }
            }
        }

        Ok(IOResult::Done(()))
    }

    /// Starts an FTS query. Parses the query string and executes the search.
    /// Returns true if there are results, false otherwise.
    fn query_start(&mut self, values: &[Register]) -> IOResultOr<bool> {
        self.ensure_searcher()?;
        let searcher = self
            .searcher
            .as_ref()
            .expect("FTS searcher initialized immediately above");
        if values.len() < 2 {
            return Err(LimboError::InternalError(
                "FTS query_start: expected pattern id and query string".into(),
            )
            .into());
        }

        // values[0] = pattern index
        let pattern_idx = match &values[0] {
            Register::Value(Value::Numeric(crate::numeric::Numeric::Integer(i))) => *i,
            _ => FTS_PATTERN_SCORE,
        };
        self.current_pattern = pattern_idx;

        // values[1] = query string
        let query_str = match &values[1] {
            Register::Value(Value::Text(t)) => t.as_str().to_string(),
            _ => return Err(LimboError::InternalError("FTS query must be text".into()).into()),
        };

        // Determine the optional SQL LIMIT captured by the selected pattern.
        let limit_raw = match pattern_idx {
            // Patterns without LIMIT fetch every live document that matches.
            FTS_PATTERN_MATCH | FTS_PATTERN_COMBINED | FTS_PATTERN_COMBINED_ORDERED => None,
            // Patterns with LIMIT use the captured value.
            FTS_PATTERN_SCORE
            | FTS_PATTERN_MATCH_LIMIT
            | FTS_PATTERN_COMBINED_LIMIT
            | FTS_PATTERN_COMBINED_ORDERED_LIMIT => Some(if values.len() > 2 {
                // Coerce with the same rules as a plain LIMIT (MustBeInt):
                // numeric text and integral reals become integers; anything
                // else is a datatype mismatch, never a silent default.
                let coerced = match &values[2] {
                    Register::Value(Value::Numeric(crate::numeric::Numeric::Integer(i))) => {
                        Some(*i)
                    }
                    Register::Value(Value::Numeric(crate::numeric::Numeric::Float(f))) => {
                        crate::util::cast_real_to_integer(f64::from(*f)).ok()
                    }
                    Register::Value(Value::Text(text)) => {
                        match crate::util::checked_cast_text_to_numeric(text.as_str(), true) {
                            Ok(Value::Numeric(crate::numeric::Numeric::Integer(i))) => Some(i),
                            Ok(Value::Numeric(crate::numeric::Numeric::Float(f))) => {
                                crate::util::cast_real_to_integer(f64::from(f)).ok()
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                };
                coerced
                    .ok_or_else(|| LimboError::Constraint("datatype mismatch (19)".to_string()))?
            } else {
                return Err(LimboError::InternalError(
                    "FTS query_start: LIMIT pattern selected but no limit value captured"
                        .to_string(),
                )
                .into());
            }),
            _ => {
                return Err(LimboError::InternalError(format!(
                    "FTS query_start: unknown pattern {pattern_idx}"
                ))
                .into());
            }
        };

        let parser = self
            .cached_parser
            .as_deref()
            .expect("parser built with the searcher");

        // Bound the query string before it reaches Tantivy's recursive
        // parser: a few KiB of nested parentheses would otherwise burn
        // minutes of CPU or overflow the stack (an abort no catch_unwind
        // contains). `parse_query_lenient` uses the non-backtracking parse
        // path, so nesting inside these bounds stays linear.
        if query_str.len() > FTS_MAX_QUERY_BYTES {
            return Err(LimboError::InternalError(format!(
                "FTS query is too long ({} bytes; the limit is {FTS_MAX_QUERY_BYTES})",
                query_str.len()
            ))
            .into());
        }
        let mut depth = 0usize;
        for byte in query_str.bytes() {
            match byte {
                b'(' => {
                    depth += 1;
                    if depth > FTS_MAX_QUERY_NESTING {
                        return Err(LimboError::InternalError(format!(
                            "FTS query nests deeper than {FTS_MAX_QUERY_NESTING} parentheses"
                        ))
                        .into());
                    }
                }
                b')' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        let (query, parse_errors) = parser.parse_query_lenient(&query_str);
        if let Some(error) = parse_errors.first() {
            return Err(LimboError::InternalError(format!("FTS parse error: {error:?}")).into());
        }

        // TopDocs keeps a heap proportional to its limit. Cap that heap at the
        // number of live documents: this preserves unlimited-query semantics
        // and prevents a huge SQL LIMIT from allocating beyond the largest
        // possible result set.
        let limit = bounded_query_limit(limit_raw, searcher.num_docs());
        self.current_hits.clear();
        self.streaming_hits = None;
        self.hit_pos = 0;
        if limit == 0 {
            return Ok(IOResult::Done(false));
        }

        // Unordered patterns can walk Tantivy's per-segment scorers directly.
        // This keeps memory constant for the common MATCH path. Global score
        // ordering still uses TopDocs because it inherently needs a top-k heap.
        let streaming_scores = match pattern_idx {
            FTS_PATTERN_MATCH | FTS_PATTERN_MATCH_LIMIT => Some(false),
            FTS_PATTERN_COMBINED | FTS_PATTERN_COMBINED_LIMIT => Some(true),
            _ => None,
        };
        if let Some(scores_enabled) = streaming_scores {
            let scoring = if scores_enabled {
                EnableScoring::enabled_from_searcher(searcher)
            } else {
                EnableScoring::disabled_from_searcher(searcher)
            };
            let weight = query
                .weight(scoring)
                .map_err(|e| LimboError::InternalError(format!("FTS query weight error: {e}")))?;
            let mut segments = Vec::with_capacity(searcher.segment_readers().len());
            for segment_reader in searcher.segment_readers() {
                let scorer = weight
                    .scorer(segment_reader, 1.0)
                    .map_err(|e| LimboError::InternalError(format!("FTS scorer error: {e}")))?;
                let rowids = segment_reader
                    .fast_fields()
                    .i64(ROWID_FIELD)
                    .map_err(|e| LimboError::InternalError(format!("FTS fast field error: {e}")))?;
                segments.push(FtsStreamingSegment {
                    scorer,
                    rowids,
                    alive: segment_reader.alive_bitset().cloned(),
                });
            }
            let mut stream = FtsHitStream {
                segments,
                segment_pos: 0,
                remaining: limit,
                scores_enabled,
                current: None,
            };
            let has_result = stream.advance()?;
            self.streaming_hits = Some(stream);
            return Ok(IOResult::Done(has_result));
        }

        // A global score ordering with no effective LIMIT: TopDocs would
        // eagerly allocate per-segment heaps sized to the whole corpus before
        // scoring a single document. Walk the scorers and sort what actually
        // matched instead, so memory is proportional to the matches.
        if limit >= searcher.num_docs() as usize {
            let weight = query
                .weight(EnableScoring::enabled_from_searcher(searcher))
                .map_err(|e| LimboError::InternalError(format!("FTS query weight error: {e}")))?;
            for (segment_ord, segment_reader) in searcher.segment_readers().iter().enumerate() {
                let mut scorer = weight
                    .scorer(segment_reader, 1.0)
                    .map_err(|e| LimboError::InternalError(format!("FTS scorer error: {e}")))?;
                let rowids = segment_reader
                    .fast_fields()
                    .i64(ROWID_FIELD)
                    .map_err(|e| LimboError::InternalError(format!("FTS fast field error: {e}")))?;
                let alive = segment_reader.alive_bitset();
                loop {
                    let doc_id = scorer.doc();
                    if doc_id == TERMINATED {
                        break;
                    }
                    let score = scorer.score();
                    scorer.advance();
                    if alive.is_some_and(|alive| alive.is_deleted(doc_id)) {
                        continue;
                    }
                    let rowid = rowids.first(doc_id).ok_or_else(|| {
                        LimboError::InternalError("FTS: rowid fast field missing value".into())
                    })?;
                    self.current_hits.push((
                        score,
                        DocAddress::new(segment_ord as u32, doc_id),
                        rowid,
                    ));
                }
            }
            self.current_hits
                .sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
            return Ok(IOResult::Done(!self.current_hits.is_empty()));
        }

        let top_docs = searcher
            .search(
                &query,
                &tantivy::collector::TopDocs::with_limit(limit).order_by_score(),
            )
            .map_err(|e| LimboError::InternalError(format!("FTS search error: {e}")))?;

        // Group results by segment for efficient fast field access.
        // This avoids creating a new fast field reader for each document.
        let mut by_segment: HashMap<u32, Vec<(f32, tantivy::DocAddress)>> = HashMap::default();
        for (score, doc_addr) in top_docs {
            by_segment
                .entry(doc_addr.segment_ord)
                .or_default()
                .push((score, doc_addr));
        }

        // Process each segment's results with a single fast field reader.
        // Fast fields provide columnar O(1) access to rowids without loading full documents.
        for (segment_ord, hits) in by_segment {
            let segment_reader = searcher.segment_reader(segment_ord);
            let rowid_reader = segment_reader
                .fast_fields()
                .i64(ROWID_FIELD)
                .map_err(|e| LimboError::InternalError(format!("FTS fast field error: {e}")))?;

            for (score, doc_addr) in hits {
                let rowid = rowid_reader.first(doc_addr.doc_id).ok_or_else(|| {
                    LimboError::InternalError("FTS: rowid fast field missing value".into())
                })?;
                self.current_hits.push((score, doc_addr, rowid));
            }
        }

        // Re-sort by score since we grouped by segment (preserves original ranking order)
        self.current_hits
            .sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        Ok(IOResult::Done(!self.current_hits.is_empty()))
    }

    /// Advances to the next query result. Returns true if more results exist.
    fn query_next(&mut self) -> IOResultOr<bool> {
        if let Some(stream) = &mut self.streaming_hits {
            return Ok(IOResult::Done(stream.advance()?));
        }
        if self.hit_pos >= self.current_hits.len() {
            return Ok(IOResult::Done(false));
        }
        self.hit_pos += 1;
        Ok(IOResult::Done(self.hit_pos < self.current_hits.len()))
    }

    /// Returns the column value for the current result (score or match indicator).
    fn query_column(&mut self, idx: usize) -> IOResultOr<Value> {
        // Column 0 = score for fts_score, or 1 (true) for fts_match
        if idx != 0 {
            return Err(LimboError::InternalError("FTS: only column 0 supported".into()).into());
        }

        match self.current_pattern {
            FTS_PATTERN_MATCH | FTS_PATTERN_MATCH_LIMIT => {
                if self
                    .streaming_hits
                    .as_ref()
                    .is_some_and(|stream| stream.current.is_none())
                    || (self.streaming_hits.is_none() && self.hit_pos >= self.current_hits.len())
                {
                    return Err(LimboError::InternalError(
                        "FTS: query_column out of bounds".into(),
                    )
                    .into());
                }
                // For fts_match patterns, return 1 (true) - indicates this row matches
                Ok(IOResult::Done(Value::from_i64(1)))
            }
            FTS_PATTERN_SCORE
            | FTS_PATTERN_COMBINED
            | FTS_PATTERN_COMBINED_LIMIT
            | FTS_PATTERN_COMBINED_ORDERED
            | FTS_PATTERN_COMBINED_ORDERED_LIMIT => {
                // For fts_score and combined patterns, return the actual score
                let score = if let Some(stream) = &self.streaming_hits {
                    stream.current.map(|(score, _)| score).ok_or_else(|| {
                        LimboError::InternalError("FTS: query_column out of bounds".into())
                    })?
                } else {
                    self.current_hits
                        .get(self.hit_pos)
                        .map(|(score, _, _)| *score)
                        .ok_or_else(|| {
                            LimboError::InternalError("FTS: query_column out of bounds".into())
                        })?
                };
                Ok(IOResult::Done(Value::from_f64(score as f64)))
            }
            _ => {
                // Unknown pattern - return score as default
                let (score, _, _) = self.current_hits.get(self.hit_pos).ok_or_else(|| {
                    LimboError::InternalError("FTS: query_column out of bounds".into())
                })?;
                Ok(IOResult::Done(Value::from_f64(*score as f64)))
            }
        }
    }

    /// Returns the rowid for the current query result.
    fn query_rowid(&mut self) -> IOResultOr<Option<i64>> {
        if let Some(stream) = &self.streaming_hits {
            return Ok(IOResult::Done(stream.current.map(|(_, rowid)| rowid)));
        }
        if self.hit_pos >= self.current_hits.len() {
            return Ok(IOResult::Done(None));
        }
        let (_, _, rowid) = self.current_hits[self.hit_pos];
        Ok(IOResult::Done(Some(rowid)))
    }

    /// Builds and publishes the statement's segment and tombstone rows
    /// before the statement savepoint is released. If the flush published a
    /// new segment and the visible set exceeds `PRAGMA fts_merge_threshold`,
    /// merges it down in the same transaction (skipped silently on
    /// maintenance contention).
    fn stage_statement_commit(&mut self, _context: &IndexMethodContext) -> IOResultOr<()> {
        if self.is_publishing() {
            return_if_io!(self.drive_publish());
        } else if self.pending_op_count() > 0 {
            tracing::debug!(
                "FTS stage_statement_commit: flushing {} pending operations",
                self.pending_op_count()
            );
            self.stage_flush()?;
            self.auto_merge_pending = matches!(
                self.publish.as_ref().map(|publish| &publish.apply),
                Some(PublishApply::AppendSegment(Some(_)))
            );
            return_if_io!(self.drive_publish());
        }
        if self.auto_merge_pending {
            return_if_io!(self.try_auto_merge());
        }
        // This cursor's statement-scope writes are staged; it never flushes
        // again, so a later statement's cursor may write this index.
        self.release_writer_slot();
        Ok(IOResult::Done(()))
    }

    fn abort_statement(&mut self, _context: &IndexMethodContext) {
        // The statement's backing rows (if any were staged) are undone by
        // the engine's savepoint rollback; drop the in-memory mirror and
        // rescan on the next access.
        self.purge_own_published();
        self.reset_to_init();
    }

    fn on_transaction_committed(&mut self, _context: &IndexMethodContext) {
        // Own segments are durable now; the shared byte cache entries stay.
        self.own_published.clear();
    }

    fn on_transaction_rolled_back(&mut self, _context: &IndexMethodContext) {
        self.purge_own_published();
        self.reset_to_init();
    }

    fn on_savepoint_rolled_back(&mut self, _context: &IndexMethodContext) {
        // Correct but coarse: the hook carries no savepoint identity, so we
        // cannot tell whether the rollback reverted rows this cursor's view
        // depends on. Discard everything; the next FTS access reloads from
        // the (correctly reverted) backing B-tree.
        self.purge_own_published();
        self.reset_to_init();
    }

    fn close(&mut self, _context: &IndexMethodContext) {
        if self.pending_op_count() != 0 || self.is_publishing() {
            // close() is the explicit "discard whatever is left" hook, so it
            // owns this decision: log it loudly, then normalize the cursor so
            // Drop has nothing left to enforce.
            tracing::error!(
                pending_ops = self.pending_op_count(),
                is_publishing = self.is_publishing(),
                "closing FTS cursor with unprepared writes; discarding them"
            );
        }
        self.reset_to_init();
    }

    /// Merge the visible segments into one, compacting tombstones away.
    /// Call via `OPTIMIZE INDEX idx_name`. The only operation that touches
    /// other transactions' rows; serialized by the per-index merge mutex.
    fn optimize(&mut self, context: &IndexMethodContext) -> IOResultOr<()> {
        let conn = context.connection()?;
        let database_id = context.database().id;
        self.database_id = Some(database_id);
        self.connection = Some(Arc::downgrade(&conn));

        // Resume a publication this opcode started before its last yield.
        // Only a merge publication ends the opcode: the pre-merge flush of
        // buffered work below also publishes, and after it completes the
        // merge itself is still to do.
        if self.is_publishing() {
            let is_merge = matches!(
                self.publish.as_ref().map(|publish| &publish.apply),
                Some(PublishApply::ReplaceSegments(_))
            );
            return_if_io!(self.drive_publish());
            if is_merge {
                return Ok(IOResult::Done(()));
            }
        }

        if !matches!(self.state, FtsState::Ready) {
            return_if_io!(self.ensure_backing_store(&conn, database_id));
            self.opening_for_write = true;
            let result = self.drive_open();
            if !matches!(result, Ok(IOResult::IO(_))) {
                self.opening_for_write = false;
            }
            return_if_io!(result);
        }
        self.claim_writer_slot()?;
        // The merge mutex: concurrent merges are refused, and lease
        // acquisition also refuses if a tombstone writer is active or
        // committed past our snapshot (its deletes would be lost).
        self.acquire_mvcc_maintenance_lease()?;
        return_if_io!(self.ensure_snapshot_loaded());

        // Publish any pending buffered work first, as its own segment.
        if self.pending_op_count() > 0 {
            self.stage_flush()?;
            return_if_io!(self.drive_publish());
        }

        let total_tombstones: usize = self.segments.iter().map(|s| s.deleted.len()).sum();
        if self.segments.len() <= 1 && total_tombstones == 0 {
            tracing::debug!(
                "FTS optimize: nothing to merge ({} segments)",
                self.segments.len()
            );
            return Ok(IOResult::Done(()));
        }

        // Belt and braces: re-verify no deleter overlapped between lease
        // acquisition and here (the lease blocks new deleters, so this can
        // only fail if the acquire raced an in-flight registration).
        if let Some((mv_store, tx_id, index_id)) = self.mvcc_index_id(&conn, database_id)? {
            mv_store.check_index_method_merge_admissible(tx_id, index_id)?;
        }

        // OPTIMIZE is the explicit "compact now" command: it merges every
        // visible segment, with no tier exemptions.
        let all_visible: HashSet<SegmentId> = self.segments.iter().map(LoadedSegment::id).collect();
        self.stage_merge_of_segments(&all_visible)?;
        return_if_io!(self.drive_publish());
        Ok(IOResult::Done(()))
    }

    /// Estimates the cost of executing a query with the given pattern.
    ///
    /// FTS queries are typically very selective (returning a small fraction of rows).
    fn estimate_cost(
        &self,
        context: &crate::index_method::IndexMethodCostContext<'_>,
    ) -> Option<crate::index_method::IndexMethodCostEstimate> {
        // FTS is typically very selective - assume ~1% of rows match
        // This is a conservative estimate; real selectivity depends on query terms
        let selectivity = 0.01;
        let estimated_matches = (context.base_table_rows * selectivity).max(1.0).ceil() as u64;
        let literal_limit = if matches!(
            context.pattern_idx as i64,
            FTS_PATTERN_SCORE
                | FTS_PATTERN_COMBINED_ORDERED_LIMIT
                | FTS_PATTERN_COMBINED_LIMIT
                | FTS_PATTERN_MATCH_LIMIT
        ) {
            context
                .arguments
                .get(1)
                .and_then(Self::constant_integer_expression)
        } else {
            None
        };
        let estimated_rows = match literal_limit {
            Some(0) => 0,
            Some(limit) if limit > 0 => estimated_matches.min(limit as u64),
            // A negative LIMIT means "unlimited" in SQLite. Runtime
            // expressions are unknown at planning time and stay conservative.
            Some(_) | None => estimated_matches,
        };

        let globally_ranked = matches!(
            context.pattern_idx as i64,
            FTS_PATTERN_SCORE | FTS_PATTERN_COMBINED_ORDERED_LIMIT | FTS_PATTERN_COMBINED_ORDERED
        );
        let streaming_limit = matches!(
            context.pattern_idx as i64,
            FTS_PATTERN_COMBINED_LIMIT | FTS_PATTERN_MATCH_LIMIT
        );
        let scores_matches = !matches!(
            context.pattern_idx as i64,
            FTS_PATTERN_MATCH | FTS_PATTERN_MATCH_LIMIT
        );
        let visited_matches = if streaming_limit {
            estimated_rows
        } else {
            estimated_matches
        };
        let scored_matches = if scores_matches {
            if globally_ranked {
                estimated_matches
            } else {
                visited_matches
            }
        } else {
            0
        };

        // Cost model:
        // - Load cost: the dominant real cost. A cold query materializes the
        //   visible segments in memory, linear in index bytes. Segments
        //   resident in the shared byte cache make the load warm.
        // - Base cost: logarithmic in vocabulary size (approximated by table size)
        // - Posting traversal: stops at LIMIT for unordered streaming patterns
        // - Scoring: omitted for MATCH-only patterns
        // - Top-k materialization: required only for global score ordering
        let retained_bytes = self.shared.segment_bytes.lock().total_bytes();
        const ESTIMATED_INDEX_BYTES_PER_ROW: f64 = 64.0;
        const PAGE_BYTES: f64 = 4096.0;
        let load_cost = if retained_bytes > 0 {
            // Resident segments make reuse likely; charge a token amount.
            retained_bytes as f64 / PAGE_BYTES * 0.01
        } else {
            (context.base_table_rows * ESTIMATED_INDEX_BYTES_PER_ROW) / PAGE_BYTES
        };
        let base_cost = context.base_table_rows.max(1.0).ln() * 10.0;
        let traversal_cost = visited_matches as f64 * 0.05;
        let scoring_cost = scored_matches as f64 * 0.05;
        let materialization_cost = if globally_ranked {
            estimated_rows as f64 * 0.05
        } else {
            0.0
        };

        Some(crate::index_method::IndexMethodCostEstimate {
            estimated_cost: load_cost
                + base_cost
                + traversal_cost
                + scoring_cost
                + materialization_cost,
            estimated_rows,
        })
    }

    #[cfg(feature = "test_helper")]
    fn test_stats(&self) -> Result<Option<crate::index_method::IndexMethodTestStats>> {
        let stats = &self.shared.stats;
        let format_version = self.control.as_ref().map(|_| format::FTS_STORAGE_FORMAT_V2);
        let file_count: usize = self
            .segments
            .iter()
            .map(|segment| segment.descriptor.files.len())
            .sum();
        Ok(Some(crate::index_method::IndexMethodTestStats {
            storage_format_version: format_version,
            index_incarnation: self.control.as_ref().map(|c| c.index_incarnation),
            // The transactional manifest is gone in format v2: the visible
            // registry rows are the manifest.
            manifest_generation: None,
            manifest_file_count: self.snapshot_loaded.then_some(file_count),
            storage_file_count: file_count,
            segment_count: self.snapshot_loaded.then_some(self.segments.len()),
            cached_connection_count: Some(self.shared.searchers.lock().entries.len()),
            cached_bytes: Some(self.shared.segment_bytes.lock().total_bytes()),
            cache_admission_rejections: None,
            // Writers are transaction-private in format v2; there is no
            // shared retained writer.
            cached_writer: Some(false),
            tantivy_writer_constructions: Some(stats.segment_builds.load(Ordering::Relaxed)),
            writer_cache_lookups: None,
            writer_cache_hits: None,
            writer_cache_validation_failures: None,
            writer_cache_rollback_discards: None,
            writer_cache_misses: None,
            read_cache_lookups: Some(stats.read_cache_lookups.load(Ordering::Relaxed)),
            read_cache_hits: Some(stats.read_cache_hits.load(Ordering::Relaxed)),
            read_cache_misses: Some(stats.read_cache_misses.load(Ordering::Relaxed)),
            // In format v2 the registry scan is cheap and runs per
            // snapshot; the expensive part is loading segment bytes, which
            // the shared byte cache avoids. Report byte loads here.
            full_snapshot_loads: Some(stats.segment_loads.load(Ordering::Relaxed)),
            manifest_validation_hits: None,
            manifest_validation_misses: None,
            write_lease_acquisitions: Some(stats.write_lease_acquisitions.load(Ordering::Relaxed)),
            write_lease_rejections: Some(stats.write_lease_rejections.load(Ordering::Relaxed)),
        }))
    }
}

#[cfg(test)]
mod tests;
