use std::fmt;

/// Hash join spill/probe metrics.
#[derive(Debug, Default, Clone)]
pub struct HashJoinMetrics {
    // Spill metrics
    pub spill_bytes_written: u64,
    pub spill_chunks: u64,
    pub spill_max_chunks_per_partition: u64,
    pub spill_max_partition_bytes: u64,

    // Load metrics
    pub load_bytes_read: u64,

    // Probe metrics
    pub probe_calls: u64,

    // Grace hash join metrics
    pub probe_spill_bytes_written: u64,
    pub probe_spill_chunks: u64,
    pub grace_partitions_processed: u64,
    pub grace_probe_rows_streamed: u64,
    pub grace_probe_rows_buffered: u64,
    pub grace_matches: u64,
}

impl HashJoinMetrics {
    pub fn merge(&mut self, other: &HashJoinMetrics) {
        self.spill_bytes_written = self
            .spill_bytes_written
            .wrapping_add(other.spill_bytes_written);
        self.spill_chunks = self.spill_chunks.wrapping_add(other.spill_chunks);
        self.spill_max_chunks_per_partition = self
            .spill_max_chunks_per_partition
            .max(other.spill_max_chunks_per_partition);
        self.spill_max_partition_bytes = self
            .spill_max_partition_bytes
            .max(other.spill_max_partition_bytes);
        self.load_bytes_read = self.load_bytes_read.wrapping_add(other.load_bytes_read);
        self.probe_calls = self.probe_calls.wrapping_add(other.probe_calls);
        self.probe_spill_bytes_written = self
            .probe_spill_bytes_written
            .wrapping_add(other.probe_spill_bytes_written);
        self.probe_spill_chunks = self
            .probe_spill_chunks
            .wrapping_add(other.probe_spill_chunks);
        self.grace_partitions_processed = self
            .grace_partitions_processed
            .wrapping_add(other.grace_partitions_processed);
        self.grace_probe_rows_streamed = self
            .grace_probe_rows_streamed
            .wrapping_add(other.grace_probe_rows_streamed);
        self.grace_probe_rows_buffered = self
            .grace_probe_rows_buffered
            .wrapping_add(other.grace_probe_rows_buffered);
        self.grace_matches = self.grace_matches.wrapping_add(other.grace_matches);
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Statement-level execution metrics
///
/// These metrics are collected unconditionally during statement execution
/// with minimal overhead (simple counter increments). The cost of incrementing
/// these counters is negligible compared to the actual work being measured.
#[derive(Debug, Default, Clone)]
pub struct StatementMetrics {
    // Row operations
    pub rows_read: u64,
    pub rows_written: u64,

    // Execution statistics
    pub vm_steps: u64,
    pub insn_executed: u64,
    pub reprepares: u64,

    // Table scan metrics
    pub fullscan_steps: u64,
    pub index_steps: u64,

    // Sort and filter operations
    pub sort_operations: u64,
    pub filter_operations: u64,

    // B-tree operations
    pub btree_seeks: u64,
    pub btree_next: u64,
    pub btree_prev: u64,

    /// Signed counter tracking seeks and cursor advances, decremented on sort
    /// rewind to avoid double-counting. Exposed as `sqlite3_search_count`.
    pub search_count: i64,

    // Hash join spill/probe metrics
    pub hash_join: HashJoinMetrics,
}

impl StatementMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get total row operations
    pub fn total_row_ops(&self) -> u64 {
        self.rows_read + self.rows_written
    }

    /// Merge another metrics instance into this one (for aggregation)
    pub fn merge(&mut self, other: &StatementMetrics) {
        self.rows_read = self.rows_read.wrapping_add(other.rows_read);
        self.rows_written = self.rows_written.wrapping_add(other.rows_written);
        self.vm_steps = self.vm_steps.wrapping_add(other.vm_steps);
        self.insn_executed = self.insn_executed.wrapping_add(other.insn_executed);
        self.reprepares = self.reprepares.wrapping_add(other.reprepares);
        self.fullscan_steps = self.fullscan_steps.wrapping_add(other.fullscan_steps);
        self.index_steps = self.index_steps.wrapping_add(other.index_steps);
        self.sort_operations = self.sort_operations.wrapping_add(other.sort_operations);
        self.filter_operations = self.filter_operations.wrapping_add(other.filter_operations);
        self.btree_seeks = self.btree_seeks.wrapping_add(other.btree_seeks);
        self.btree_next = self.btree_next.wrapping_add(other.btree_next);
        self.btree_prev = self.btree_prev.wrapping_add(other.btree_prev);
        self.search_count = self.search_count.wrapping_add(other.search_count);
        self.hash_join.merge(&other.hash_join);
    }

    /// Reset all counters to zero
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl fmt::Display for StatementMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Statement Metrics:")?;
        writeln!(f, "  Row Operations:")?;
        writeln!(f, "    Rows read:        {}", self.rows_read)?;
        writeln!(f, "    Rows written:     {}", self.rows_written)?;
        writeln!(f, "  Execution:")?;
        writeln!(f, "    VM steps:         {}", self.vm_steps)?;
        writeln!(f, "    Instructions:     {}", self.insn_executed)?;
        writeln!(f, "    Reprepares:       {}", self.reprepares)?;
        writeln!(f, "  Table Access:")?;
        writeln!(f, "    Full scan steps:  {}", self.fullscan_steps)?;
        writeln!(f, "    Index steps:      {}", self.index_steps)?;
        writeln!(f, "  Operations:")?;
        writeln!(f, "    Sort operations:  {}", self.sort_operations)?;
        writeln!(f, "    Filter operations:{}", self.filter_operations)?;
        writeln!(f, "  B-tree Operations:")?;
        writeln!(f, "    Seeks:            {}", self.btree_seeks)?;
        writeln!(f, "    Next:             {}", self.btree_next)?;
        writeln!(f, "    Prev:             {}", self.btree_prev)?;
        writeln!(f, "  Hash Join:")?;
        writeln!(
            f,
            "    Spill bytes:      {}",
            self.hash_join.spill_bytes_written
        )?;
        writeln!(f, "    Spill chunks:     {}", self.hash_join.spill_chunks)?;
        writeln!(
            f,
            "    Max chunks/part:  {}",
            self.hash_join.spill_max_chunks_per_partition
        )?;
        writeln!(
            f,
            "    Max part bytes:   {}",
            self.hash_join.spill_max_partition_bytes
        )?;
        writeln!(
            f,
            "    Load bytes:       {}",
            self.hash_join.load_bytes_read
        )?;
        writeln!(f, "    Probes:           {}", self.hash_join.probe_calls)?;
        writeln!(
            f,
            "    Probe spill bytes: {}",
            self.hash_join.probe_spill_bytes_written
        )?;
        writeln!(
            f,
            "    Probe spill chunks: {}",
            self.hash_join.probe_spill_chunks
        )?;
        writeln!(
            f,
            "    Grace partitions:  {}",
            self.hash_join.grace_partitions_processed
        )?;
        writeln!(
            f,
            "    Grace streamed:    {}",
            self.hash_join.grace_probe_rows_streamed
        )?;
        writeln!(
            f,
            "    Grace buffered:    {}",
            self.hash_join.grace_probe_rows_buffered
        )?;
        writeln!(f, "    Grace matches:     {}", self.hash_join.grace_matches)?;
        Ok(())
    }
}

/// Connection-level metrics aggregation
#[derive(Debug, Default, Clone)]
pub struct ConnectionMetrics {
    /// Total number of statements executed
    pub total_statements: u64,

    /// Aggregate metrics from all statements
    pub aggregate: StatementMetrics,

    /// High-water marks for monitoring
    pub max_vm_steps_per_statement: u64,
    pub max_rows_read_per_statement: u64,
}

impl ConnectionMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a completed statement's metrics (borrows, no clone).
    pub fn record_statement(&mut self, metrics: &StatementMetrics) {
        self.total_statements = self.total_statements.wrapping_add(1);

        // Update high-water marks
        self.max_vm_steps_per_statement = self.max_vm_steps_per_statement.max(metrics.vm_steps);
        self.max_rows_read_per_statement = self.max_rows_read_per_statement.max(metrics.rows_read);

        // Aggregate into total
        self.aggregate.merge(metrics);
    }

    /// Reset connection metrics
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl fmt::Display for ConnectionMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Connection Metrics:")?;
        writeln!(f, "  Total statements:     {}", self.total_statements)?;
        writeln!(f, "  High-water marks:")?;
        writeln!(
            f,
            "    Max VM steps:       {}",
            self.max_vm_steps_per_statement
        )?;
        writeln!(
            f,
            "    Max rows read:      {}",
            self.max_rows_read_per_statement
        )?;
        writeln!(f)?;
        writeln!(f, "Aggregate Statistics:")?;
        write!(f, "{}", self.aggregate)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_merge() {
        let mut m1 = StatementMetrics::new();
        m1.rows_read = 100;
        m1.vm_steps = 50;
        m1.hash_join.spill_bytes_written = 42;

        let mut m2 = StatementMetrics::new();
        m2.rows_read = 200;
        m2.vm_steps = 75;
        m2.hash_join.spill_bytes_written = 8;
        m2.hash_join.spill_max_partition_bytes = 1024;

        m1.merge(&m2);
        assert_eq!(m1.rows_read, 300);
        assert_eq!(m1.vm_steps, 125);
        assert_eq!(m1.hash_join.spill_bytes_written, 50);
        assert_eq!(m1.hash_join.spill_max_partition_bytes, 1024);
    }

    #[test]
    fn test_connection_metrics_high_water() {
        let mut conn_metrics = ConnectionMetrics::new();

        let mut stmt1 = StatementMetrics::new();
        stmt1.vm_steps = 100;
        stmt1.rows_read = 50;
        conn_metrics.record_statement(&stmt1);

        let mut stmt2 = StatementMetrics::new();
        stmt2.vm_steps = 75;
        stmt2.rows_read = 100;
        conn_metrics.record_statement(&stmt2);

        assert_eq!(conn_metrics.max_vm_steps_per_statement, 100);
        assert_eq!(conn_metrics.max_rows_read_per_statement, 100);
        assert_eq!(conn_metrics.total_statements, 2);
        assert_eq!(conn_metrics.aggregate.vm_steps, 175);
        assert_eq!(conn_metrics.aggregate.rows_read, 150);
    }
}
