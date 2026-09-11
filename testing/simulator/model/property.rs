use serde::{Deserialize, Serialize};
use sql_generation::model::query::{Create, Insert, Select, predicate::Predicate, update::Update};

use crate::model::{CreateSequence, DropSequence, Query, QueryDiscriminants};

/// Properties are representations of executable specifications
/// about the database behavior.
#[derive(Debug, Clone, Serialize, Deserialize, strum::EnumDiscriminants, strum::IntoStaticStr)]
#[strum_discriminants(derive(strum::EnumIter, strum::IntoStaticStr))]
#[strum(serialize_all = "Train-Case")]
pub enum Property {
    /// Insert-Select is a property in which the inserted row
    /// must be in the resulting rows of a select query that has a
    /// where clause that matches the inserted row.
    /// The execution of the property is as follows
    ///     INSERT INTO <t> VALUES (...)
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     SELECT * FROM <t> WHERE <predicate>
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - The inserted row will not be deleted.
    /// - The inserted row will not be updated.
    /// - The table `t` will not be renamed, dropped, or altered.
    InsertValuesSelect {
        /// The insert query
        insert: Insert,
        /// Selected row index
        row_index: usize,
        /// Additional interactions in the middle of the property
        queries: Vec<Query>,
        /// The select query
        select: Select,
        /// Interactive query information if any
        interactive: Option<InteractiveQueryInfo>,
    },
    /// ReadYourUpdatesBack verifies UPDATE behavior for both success and failure cases.
    ///
    /// Execution:
    ///     SELECT <cols> FROM <t> WHERE <predicate>  -- snapshot before
    ///     UPDATE <t> SET <cols> WHERE <predicate>
    ///     SELECT <cols> FROM <t> WHERE <predicate>  -- snapshot after
    ///
    /// Assertion:
    /// - If UPDATE succeeded: after rows have updated values
    /// - If UPDATE failed (e.g., constraint error): before == after (rollback verified)
    ReadYourUpdatesBack {
        update: Update,
        select_before: Select,
        select_after: Select,
    },
    /// TableHasExpectedContent is a property in which the table
    /// must have the expected content, i.e. all the insertions and
    /// updates and deletions should have been persisted in the way
    /// we think they were.
    /// The execution of the property is as follows
    ///     SELECT * FROM <t>
    ///     ASSERT <expected_content>
    TableHasExpectedContent {
        table: String,
    },
    /// AllTablesHaveExpectedContent is a property in which the table
    /// must have the expected content, i.e. all the insertions and
    /// updates and deletions should have been persisted in the way
    /// we think they were.
    /// The execution of the property is as follows
    ///     SELECT * FROM <t>
    ///     ASSERT <expected_content>
    /// for each table in the simulator model
    AllTableHaveExpectedContent {
        tables: Vec<String>,
    },
    /// Double Create Failure is a property in which creating
    /// the same table twice leads to an error.
    /// The execution of the property is as follows
    ///     CREATE TABLE <t> (...)
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     CREATE TABLE <t> (...) -> Error
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - Table `t` will not be renamed or dropped.
    DoubleCreateFailure {
        /// The create query
        create: Create,
        /// Additional interactions in the middle of the property
        queries: Vec<Query>,
    },
    /// Select Limit is a property in which the select query
    /// has a limit clause that is respected by the query.
    /// The execution of the property is as follows
    ///     SELECT * FROM <t> WHERE <predicate> LIMIT <n>
    /// This property is a single-interaction property.
    /// The interaction has the following constraints;
    /// - The select query will respect the limit clause.
    SelectLimit {
        /// The select query
        select: Select,
    },
    /// Delete-Select is a property in which the deleted row
    /// must not be in the resulting rows of a select query that has a
    /// where clause that matches the deleted row. In practice, `p1` of
    /// the delete query will be used as the predicate for the select query,
    /// hence the select should return NO ROWS.
    /// The execution of the property is as follows
    ///     DELETE FROM <t> WHERE <predicate>
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     SELECT * FROM <t> WHERE <predicate>
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - A row that holds for the predicate will not be inserted.
    /// - The table `t` will not be renamed, dropped, or altered.
    DeleteSelect {
        table: String,
        predicate: Predicate,
        queries: Vec<Query>,
    },
    /// Drop-Select is a property in which selecting from a dropped table
    /// should result in an error.
    /// The execution of the property is as follows
    ///     DROP TABLE <t>
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     SELECT * FROM <t> WHERE <predicate> -> Error
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - The table `t` will not be created, no table will be renamed to `t`.
    DropSelect {
        table: String,
        queries: Vec<Query>,
        select: Select,
    },
    /// Select-Select-Optimizer is a property in which we test the optimizer by
    /// running two equivalent select queries, one with `SELECT <predicate> from <t>`
    /// and the other with `SELECT * from <t> WHERE <predicate>`. As highlighted by
    /// Rigger et al. in Non-Optimizing Reference Engine Construction(NoREC), SQLite
    /// tends to optimize `where` statements while keeping the result column expressions
    /// unoptimized. This property is used to test the optimizer. The property is successful
    /// if the two queries return the same number of rows.
    SelectSelectOptimizer {
        table: String,
        predicate: Predicate,
    },
    /// Where-True-False-Null is a property that tests the boolean logic implementation
    /// in the database. It relies on the fact that `P == true || P == false || P == null` should return true,
    /// as SQLite uses a ternary logic system. This property is invented in "Finding Bugs in Database Systems via Query Partitioning"
    /// by Rigger et al. and it is canonically called Ternary Logic Partitioning (TLP).
    WhereTrueFalseNull {
        select: Select,
        predicate: Predicate,
    },
    /// UNION-ALL-Preserves-Cardinality is a property that tests the UNION ALL operator
    /// implementation in the database. It relies on the fact that `SELECT * FROM <t
    /// > WHERE <predicate> UNION ALL SELECT * FROM <t> WHERE <predicate>`
    /// should return the same number of rows as `SELECT <predicate> FROM <t> WHERE <predicate>`.
    /// > The property is succesfull when the UNION ALL of 2 select queries returns the same number of rows
    /// > as the sum of the two select queries.
    UnionAllPreservesCardinality {
        select: Select,
        where_clause: Predicate,
    },
    /// FsyncNoWait tests recovery when a query is interrupted at fsync.
    ///
    /// # Interactions
    /// - Execute `query` until the memory backend has a pending fsync
    /// - Simulate power loss and reopen the database
    /// - Retry `query`; a query that reaches no fsync is not retried
    /// - Compare the recovered database with the shadow model
    ///
    FsyncNoWait {
        query: Query,
    },
    FaultyQuery {
        query: Query,
    },
    /// SavepointRollback wraps random write interactions in a named savepoint,
    /// rolls them back, then checks that the database still matches the shadow
    /// model. This targets pager/WAL/cache-spill bugs where rolled-back page
    /// contents remain visible.
    SavepointRollback {
        queries: Vec<Query>,
        tables: Vec<String>,
        write_kinds: Vec<QueryDiscriminants>,
    },
    /// SequenceMonotonicity verifies that nextval() returns monotonically increasing
    /// values matching the expected arithmetic sequence.
    ///
    /// Execution:
    ///     CREATE SEQUENCE seq START WITH s INCREMENT BY i ...
    ///     SELECT nextval('seq')  -- expect s
    ///     SELECT nextval('seq')  -- expect s+i
    ///     ...
    ///     DROP SEQUENCE seq
    SequenceMonotonicity {
        create: CreateSequence,
        num_calls: usize,
        drop: DropSequence,
    },
    /// Property used to subsititute a property with its queries only
    Queries {
        queries: Vec<Query>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InteractiveQueryInfo {
    pub start_with_immediate: bool,
    pub end_with_commit: bool,
}

impl Property {
    /// Property Does some sort of fault injection
    pub fn check_tables(&self) -> bool {
        matches!(
            self,
            Property::FsyncNoWait { .. } | Property::FaultyQuery { .. }
        )
    }

    pub fn has_extensional_queries(&self) -> bool {
        matches!(
            self,
            Property::InsertValuesSelect { .. }
                | Property::DoubleCreateFailure { .. }
                | Property::DeleteSelect { .. }
                | Property::DropSelect { .. }
                | Property::SavepointRollback { .. }
                | Property::Queries { .. }
        )
    }

    pub fn get_extensional_queries(&mut self) -> Option<&mut Vec<Query>> {
        match self {
            Property::InsertValuesSelect { queries, .. }
            | Property::DoubleCreateFailure { queries, .. }
            | Property::DeleteSelect { queries, .. }
            | Property::DropSelect { queries, .. }
            | Property::SavepointRollback { queries, .. }
            | Property::Queries { queries } => Some(queries),
            Property::FsyncNoWait { .. } | Property::FaultyQuery { .. } => None,
            Property::SequenceMonotonicity { .. } => None,
            Property::SelectLimit { .. }
            | Property::SelectSelectOptimizer { .. }
            | Property::WhereTrueFalseNull { .. }
            | Property::UnionAllPreservesCardinality { .. }
            | Property::ReadYourUpdatesBack { .. }
            | Property::TableHasExpectedContent { .. }
            | Property::AllTableHaveExpectedContent { .. } => None,
        }
    }
}

impl PropertyDiscriminants {
    pub fn name(&self) -> &'static str {
        self.into()
    }

    pub fn check_tables(&self) -> bool {
        matches!(
            self,
            Self::AllTableHaveExpectedContent | Self::TableHasExpectedContent
        )
    }
}
