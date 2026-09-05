use std::num::{NonZero, NonZeroUsize};

/// Convert a usize to u32 for instruction fields (registers, counts).
/// Panics if the value exceeds u32::MAX, which would require an impossible
/// four-billion-register program; u16 was too small for real queries (e.g. a
/// handful of CTEs at the 2000-column limit already exceed 65535 registers).
#[inline]
pub fn to_u32(v: usize) -> u32 {
    v.try_into().expect("value exceeds u32::MAX")
}

use super::{execute, BranchOffset, CursorID, FuncCtx, InsnFunction, PageIdx};
use crate::function::AccumulatorFunc;
use crate::{
    schema::{BTreeTable, CheckConstraint, Column, ForeignKey, Index},
    storage::{pager::CreateBTreeFlags, wal::CheckpointMode},
    sync::{Arc, OnceLock, Weak},
    translate::{collate::CollationSeq, emitter::TransactionMode, plan::BitSet},
    types::KeyInfo,
    vdbe::affinity::Affinity,
    PreparedProgram, Value,
};
use strum::EnumCount;
use strum_macros::{EnumDiscriminants, FromRepr, VariantArray};
use turso_macros::Description;
use turso_parser::ast::{ResolveType, SortOrder};

/// The program run by an `Insn::Program` instruction.
///
/// Most callers already have a finished trigger or foreign-key action program.
/// Recursive foreign-key actions are different: while compiling one action
/// program, the generated SQL can need to emit a call back to that same action
/// program before it has finished compiling.
#[derive(Debug, Clone)]
pub enum Subprogram {
    /// A finished trigger or foreign-key action program.
    PreparedProgram(Arc<PreparedProgram>),
    /// A recursive foreign-key action program that is still being compiled.
    ///
    /// Example: `t(id PRIMARY KEY, parent REFERENCES t(id) ON DELETE CASCADE)`.
    /// The action that deletes child rows from `t` can itself delete more rows
    /// from `t`, so it must call the same action program that is being built.
    /// The slot is filled after compilation finishes. The stored reference is
    /// weak so the finished program does not own itself.
    Pending(Arc<OnceLock<Weak<PreparedProgram>>>),
}

impl Subprogram {
    /// Return the finished program that `Insn::Program` should run.
    ///
    /// `Pending` must have been filled during compilation before execution
    /// reaches the instruction. If it has not been filled, compilation emitted
    /// a recursive foreign-key action call without connecting it to the
    /// finished action program.
    pub(super) fn prepared_program(&self) -> crate::Result<Arc<PreparedProgram>> {
        match self {
            Self::PreparedProgram(program) => Ok(program.clone()),
            Self::Pending(program) => program.get().and_then(Weak::upgrade).ok_or_else(|| {
                crate::LimboError::InternalError(
                    "recursive foreign-key action subprogram was not resolved".into(),
                )
            }),
        }
    }
}

/// Known custom type comparator functions for sorting and MIN/MAX aggregates.
/// These replace heap-allocated String names with a compact enum.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SortComparatorType {
    NumericLt,
    StringReverse,
    TestUintLt,
    ArrayLt,
}

/// Flags provided to comparison instructions (e.g. Eq, Ne) which determine behavior related to NULL values.
#[derive(Clone, Copy, Debug, Default)]
pub struct CmpInsFlags(usize);

impl CmpInsFlags {
    const NULL_EQ: usize = 0x80;
    const JUMP_IF_NULL: usize = 0x10;
    const AFFINITY_MASK: usize = 0x47;
    const ARRAY_CMP: usize = 0x100;

    fn has(&self, flag: usize) -> bool {
        (self.0 & flag) != 0
    }

    pub fn null_eq(mut self) -> Self {
        self.0 |= CmpInsFlags::NULL_EQ;
        self
    }

    pub fn jump_if_null(mut self) -> Self {
        self.0 |= CmpInsFlags::JUMP_IF_NULL;
        self
    }

    pub fn has_jump_if_null(&self) -> bool {
        self.has(CmpInsFlags::JUMP_IF_NULL)
    }

    pub fn has_nulleq(&self) -> bool {
        self.has(CmpInsFlags::NULL_EQ)
    }

    pub fn with_affinity(mut self, affinity: Affinity) -> Self {
        let aff_code = affinity.as_char_code() as usize;
        self.0 = (self.0 & !Self::AFFINITY_MASK) | aff_code;
        self
    }

    pub fn get_affinity(&self) -> Affinity {
        let aff_code = (self.0 & Self::AFFINITY_MASK) as u8;
        Affinity::from_char_code(aff_code)
    }

    pub fn array_cmp(mut self) -> Self {
        self.0 |= Self::ARRAY_CMP;
        self
    }

    pub fn has_array_cmp(&self) -> bool {
        self.has(Self::ARRAY_CMP)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct IdxInsertFlags(pub u8);
impl IdxInsertFlags {
    pub const APPEND: u8 = 0x01; // Hint: insert likely at the end
    pub const NCHANGE: u8 = 0x02; // Increment the change counter
    pub const USE_SEEK: u8 = 0x04; // Skip seek if last one was same key
    pub const NO_OP_DUPLICATE: u8 = 0x08; // Do not error on duplicate key
    pub fn new() -> Self {
        IdxInsertFlags(0)
    }
    pub fn has(&self, flag: u8) -> bool {
        (self.0 & flag) != 0
    }
    pub fn append(mut self, append: bool) -> Self {
        if append {
            self.0 |= IdxInsertFlags::APPEND;
        } else {
            self.0 &= !IdxInsertFlags::APPEND;
        }
        self
    }
    pub fn use_seek(mut self, seek: bool) -> Self {
        if seek {
            self.0 |= IdxInsertFlags::USE_SEEK;
        } else {
            self.0 &= !IdxInsertFlags::USE_SEEK;
        }
        self
    }
    pub fn nchange(mut self, change: bool) -> Self {
        if change {
            self.0 |= IdxInsertFlags::NCHANGE;
        } else {
            self.0 &= !IdxInsertFlags::NCHANGE;
        }
        self
    }
    /// If this is set, we will not error on duplicate key.
    /// This is a bit of a hack we use to make ephemeral indexes for UNION work --
    /// instead we should allow overwriting index interior cells, which we currently don't;
    /// this should (and will) be fixed in a future PR.
    pub fn no_op_duplicate(mut self) -> Self {
        self.0 |= IdxInsertFlags::NO_OP_DUPLICATE;
        self
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct InsertFlags(pub u8);

impl InsertFlags {
    pub const UPDATE_ROWID_CHANGE: u8 = 0x01; // Flag indicating this is part of an UPDATE statement where the row's rowid is changed
    pub const REQUIRE_SEEK: u8 = 0x02; // Flag indicating that a seek is required to insert the row
    pub const EPHEMERAL_TABLE_INSERT: u8 = 0x04; // Flag indicating that this is an insert into an ephemeral table
    pub const SKIP_LAST_ROWID: u8 = 0x08; // Flag indicating that last_insert_rowid() must not be updated
    pub const SKIP_STATEMENT_CHANGE_COUNT: u8 = 0x10; // Flag indicating that changes() must not count this insert
    pub const SKIP_ALL_CHANGE_COUNTS: u8 = 0x20; // Flag indicating that neither changes() nor total_changes() must count this insert

    pub fn new() -> Self {
        InsertFlags(0)
    }

    pub fn has(&self, flag: u8) -> bool {
        (self.0 & flag) != 0
    }

    pub fn require_seek(mut self) -> Self {
        self.0 |= InsertFlags::REQUIRE_SEEK;
        self
    }

    pub fn update_rowid_change(mut self) -> Self {
        self.0 |= InsertFlags::UPDATE_ROWID_CHANGE;
        self
    }

    pub fn is_ephemeral_table_insert(mut self) -> Self {
        self.0 |= InsertFlags::EPHEMERAL_TABLE_INSERT;
        self
    }

    pub fn skip_last_rowid(mut self) -> Self {
        self.0 |= InsertFlags::SKIP_LAST_ROWID;
        self
    }

    pub fn skip_statement_change_count(mut self) -> Self {
        self.0 |= InsertFlags::SKIP_STATEMENT_CHANGE_COUNT;
        self
    }

    pub fn skip_all_change_counts(mut self) -> Self {
        self.0 |= InsertFlags::SKIP_ALL_CHANGE_COUNTS;
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub enum RegisterOrLiteral<T: Copy + std::fmt::Display> {
    Register(usize),
    Literal(T),
}

#[derive(Debug, Clone, Copy)]
pub enum SavepointOp {
    Begin,
    Release,
    RollbackTo,
}

impl From<PageIdx> for RegisterOrLiteral<PageIdx> {
    fn from(value: PageIdx) -> Self {
        RegisterOrLiteral::Literal(value)
    }
}

impl<T: Copy + std::fmt::Display> std::fmt::Display for RegisterOrLiteral<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Literal(lit) => lit.fmt(f),
            Self::Register(reg) => reg.fmt(f),
        }
    }
}

/// Data for HashBuild instruction (boxed to keep Insn small).
#[derive(Debug, Clone)]
pub struct HashBuildData {
    pub cursor_id: CursorID,
    pub key_start_reg: usize,
    pub num_keys: usize,
    pub hash_table_id: usize,
    pub mem_budget: usize,
    pub collations: Vec<CollationSeq>,
    /// Starting register for payload columns to store in the hash entry.
    /// When Some: payload_start_reg..payload_start_reg+num_payload-1 contain values to cache.
    pub payload_start_reg: Option<usize>,
    /// Number of payload columns to read
    pub num_payload: usize,
    /// Whether to track which entries are matched (for FULL OUTER JOIN).
    pub track_matched: bool,
}

/// Key columns of a seek that use `IS` instead of `=`, so NULL may match in
/// them. Most seeks have no such columns, so the empty mask is a null pointer
/// and only IS-seeks allocate (boxed to keep Insn small).
///
/// Build it from a [`BitSet`] via `From`; that keeps the invariant that the
/// inner option is `None` exactly when the mask is empty.
#[derive(Debug, Clone, Default)]
pub struct NullMatchingMask(Option<Box<BitSet>>);

impl NullMatchingMask {
    /// Returns true if key column `idx` uses `IS`, so NULL may match in it.
    pub fn get(&self, idx: usize) -> bool {
        self.0.as_ref().is_some_and(|mask| mask.get(idx))
    }

    /// Returns true if no key column uses `IS`.
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }
}

impl From<BitSet> for NullMatchingMask {
    fn from(mask: BitSet) -> Self {
        Self((!mask.is_empty()).then(|| Box::new(mask)))
    }
}

/// Data for AddColumn instruction (boxed to keep Insn small).
#[derive(Debug, Clone)]
pub struct AddColumnData {
    pub db: usize,
    pub table: String,
    pub column: Column,
    pub check_constraints: Vec<CheckConstraint>,
    pub foreign_keys: Vec<Arc<ForeignKey>>,
}

/// Data for IntegrityCk instruction (boxed to keep Insn small).
#[derive(Debug, Clone)]
pub struct IntegrityCkData {
    pub db: usize,
    pub max_errors: usize,
    pub roots: Vec<i64>,
    pub dropped_roots: Vec<i64>,
    pub message_register: usize,
}

/// Data for AddSequence instruction (boxed to keep Insn small).
#[derive(Debug, Clone)]
pub struct AddSequenceData {
    pub db: usize,
    pub name: String,
    pub start: i64,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cycle: bool,
}

/// Data for SorterOpen instruction (boxed to keep Insn small).
#[derive(Debug, Clone)]
pub struct SorterOpenData {
    pub cursor_id: CursorID, // P1
    pub columns: usize,      // P2
    /// Combined order, collation, and nulls ordering per column.
    pub order_collations_nulls: crate::alloc::Vec<(
        SortOrder,
        Option<CollationSeq>,
        Option<turso_parser::ast::NullsOrder>,
    )>,
    /// Per-column custom type comparators for ORDER BY sorting.
    /// When present, the comparator is used instead of standard value comparison.
    pub comparators: crate::alloc::Vec<Option<SortComparatorType>>,
}

/// Data for AggStep instruction (boxed to keep Insn small).
#[derive(Debug, Clone)]
pub struct AggStepData {
    pub acc_reg: usize,
    pub col: usize,
    pub delimiter: usize,
    pub func: AccumulatorFunc,
    /// Optional custom type comparator for MIN/MAX aggregates.
    pub comparator: Option<SortComparatorType>,
    /// Collation for comparison-based aggregates (MIN/MAX), resolved at
    /// translation time from the argument expression.
    pub collation: Option<CollationSeq>,
}

/// Data for ArrayEncode instruction (boxed to keep Insn small).
#[derive(Debug, Clone)]
pub struct ArrayEncodeData {
    pub reg: usize,
    pub element_affinity: Affinity,
    pub element_type: Arc<str>,
    pub table_name: Arc<str>,
    pub col_name: Arc<str>,
}

/// Data for HashDistinct instruction (boxed to keep Insn small).
#[derive(Debug, Clone)]
pub struct HashDistinctData {
    pub hash_table_id: usize,
    pub key_start_reg: usize,
    pub num_keys: usize,
    pub collations: Vec<CollationSeq>,
    pub target_pc: BranchOffset,
}

// There are currently 190 opcodes in sqlite
#[repr(u8)]
#[derive(Description, Debug, Clone, EnumDiscriminants)]
#[strum_discriminants(vis(pub(crate)))]
#[strum_discriminants(derive(VariantArray, EnumCount, FromRepr))]
#[strum_discriminants(name(InsnVariants))]
pub enum Insn {
    /// Initialize the program state and jump to the given PC.
    Init {
        target_pc: BranchOffset,
    },
    /// Write a NULL into register dest. If dest_end is Some, then also write NULL into register dest_end and every register in between dest and dest_end. If dest_end is not set, then only register dest is set to NULL.
    Null {
        dest: usize,
        dest_end: Option<usize>,
    },
    /// Mark the beginning of a subroutine tha can be entered in-line. This opcode is identical to Null
    /// it has a different name only to make the byte code easier to read and verify
    BeginSubrtn {
        dest: usize,
        dest_end: Option<usize>,
    },
    /// Move the cursor P1 to a null row. Any Column operations that occur while the cursor is on the null row will always write a NULL.
    NullRow {
        cursor_id: CursorID,
    },
    /// Add two registers and store the result in a third register.
    Add {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Subtract rhs from lhs and store in dest
    Subtract {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Multiply two registers and store the result in a third register.
    Multiply {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Updates the value of register dest_reg to the maximum of its current
    /// value and the value in src_reg.
    ///
    ///    - dest_reg = max(int(dest_reg), int(src_reg))
    ///
    /// Both registers are converted to integers before the comparison.
    MemMax {
        dest_reg: usize, // P1
        src_reg: usize,  // P2
    },
    /// Divide lhs by rhs and store the result in a third register.
    Divide {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Compare two vectors of registers in reg(P1)..reg(P1+P3-1) (call this vector "A") and in reg(P2)..reg(P2+P3-1) ("B"). Save the result of the comparison for use by the next Jump instruct.
    Compare {
        start_reg_a: usize,
        start_reg_b: usize,
        count: usize,
        key_info: Vec<KeyInfo>,
    },
    /// Place the result of rhs bitwise AND lhs in third register.
    BitAnd {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Place the result of rhs bitwise OR lhs in third register.
    BitOr {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Place the result of bitwise NOT register P1 in dest register.
    BitNot {
        reg: usize,
        dest: usize,
    },
    /// Checkpoint the database (applying wal file content to database file).
    Checkpoint {
        database: usize,                 // checkpoint database P1
        checkpoint_mode: CheckpointMode, // P2 checkpoint mode
        dest: usize,                     // P3 checkpoint result
    },
    /// Divide lhs by rhs and place the remainder in dest register.
    Remainder {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Jump to the instruction at address P1, P2, or P3 depending on whether in the most recent Compare instruction the P1 vector was less than, equal to, or greater than the P2 vector, respectively.
    Jump {
        target_pc_lt: BranchOffset,
        target_pc_eq: BranchOffset,
        target_pc_gt: BranchOffset,
    },
    /// Move the P3 values in register P1..P1+P3-1 over into registers P2..P2+P3-1. Registers P1..P1+P3-1 are left holding a NULL. It is an error for register ranges P1..P1+P3-1 and P2..P2+P3-1 to overlap. It is an error for P3 to be less than 1.
    Move {
        source_reg: usize,
        dest_reg: usize,
        count: usize,
    },
    /// If the given register is a positive integer, decrement it by decrement_by and jump to the given PC.
    IfPos {
        reg: usize,
        target_pc: BranchOffset,
        decrement_by: usize,
    },
    /// If the given register is not NULL, jump to the given PC.
    NotNull {
        reg: usize,
        target_pc: BranchOffset,
    },
    /// Compare two registers and jump to the given PC if they are equal.
    Eq {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// CmpInsFlags are nulleq (null = null) or jump_if_null.
        ///
        /// jump_if_null jumps if either of the operands is null. Used for "jump when false" logic.
        /// Eg. "SELECT * FROM users WHERE id = NULL" becomes:
        /// <JUMP TO NEXT ROW IF id != NULL>
        /// Without the jump_if_null flag it would not jump because the logical comparison "id != NULL" is never true.
        /// This flag indicates that if either is null we should still jump.
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },
    /// Compute a hash on num_keys registers starting with r[key_reg]. Check to see if that hash
    /// is found in the bloom filter associated with the cursor/hash_table. If it is not present
    /// then jump to target_pc. Otherwise fall through.
    /// False negatives are harmless. It is always safe to fall through, even if the value is
    /// in the bloom filter. A false negative causes more CPU cycles to be used, but it should
    /// still yield the correct answer. However, an incorrect answer may well arise from a
    /// false positive - if the jump is taken when it should fall through.
    Filter {
        cursor_id: CursorID,
        /// Jump target if bloom filter says "definitely not present"
        target_pc: BranchOffset,
        /// Start register containing the key(s) to check
        key_reg: usize,
        /// Number of key registers to hash together
        num_keys: usize,
    },
    /// Compute a hash on num_keys registers starting with r[key_reg] and add that hash to
    /// the bloom filter associated with the cursor/hash_table.
    FilterAdd {
        cursor_id: CursorID,
        key_reg: usize,
        num_keys: usize,
    },
    /// Compare two registers and jump to the given PC if they are not equal.
    Ne {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// CmpInsFlags are nulleq (null = null) or jump_if_null.
        ///
        /// jump_if_null jumps if either of the operands is null. Used for "jump when false" logic.
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },
    /// Compare two registers and jump to the given PC if the left-hand side is less than the right-hand side.
    Lt {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// jump_if_null: Jump if either of the operands is null. Used for "jump when false" logic.
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },
    // Compare two registers and jump to the given PC if the left-hand side is less than or equal to the right-hand side.
    Le {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// jump_if_null: Jump if either of the operands is null. Used for "jump when false" logic.
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },
    /// Compare two registers and jump to the given PC if the left-hand side is greater than the right-hand side.
    Gt {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// jump_if_null: Jump if either of the operands is null. Used for "jump when false" logic.
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },
    /// Compare two registers and jump to the given PC if the left-hand side is greater than or equal to the right-hand side.
    Ge {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
        /// jump_if_null: Jump if either of the operands is null. Used for "jump when false" logic.
        flags: CmpInsFlags,
        collation: Option<CollationSeq>,
    },
    /// Jump to target_pc if r\[reg\] != 0 or (r\[reg\] == NULL && r\[jump_if_null\] != 0)
    If {
        reg: usize,              // P1
        target_pc: BranchOffset, // P2
        /// P3. If r\[reg\] is null, jump iff r\[jump_if_null\] != 0
        jump_if_null: bool,
    },
    /// Jump to target_pc if r\[reg\] != 0 or (r\[reg\] == NULL && r\[jump_if_null\] != 0)
    IfNot {
        reg: usize,              // P1
        target_pc: BranchOffset, // P2
        /// P3. If r\[reg\] is null, jump iff r\[jump_if_null\] != 0
        jump_if_null: bool,
    },
    /// Open a cursor for reading.
    OpenRead {
        cursor_id: CursorID,
        root_page: PageIdx,
        db: usize,
    },

    /// Open a cursor for a virtual table.
    VOpen {
        cursor_id: CursorID,
    },

    /// Create a new virtual table.
    VCreate {
        module_name: usize, // P1: Name of the module that contains the virtual table implementation
        table_name: usize,  // P2: Name of the virtual table
        args_reg: Option<usize>,
    },

    /// Initialize the position of the virtual table cursor.
    VFilter {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
        arg_count: usize,
        args_reg: usize,
        idx_str: Option<usize>,
        idx_num: usize,
    },

    /// Read a column from the current row of the virtual table cursor.
    VColumn {
        cursor_id: CursorID,
        column: usize,
        dest: usize,
    },

    /// `VUpdate`: Virtual Table Insert/Update/Delete Instruction
    VUpdate {
        cursor_id: usize,     // P1: Virtual table cursor number
        arg_count: usize,     // P2: Number of arguments in argv[]
        start_reg: usize,     // P3: Start register for argv[]
        conflict_action: u16, // P4: Conflict resolution flags
    },

    /// Advance the virtual table cursor to the next row.
    /// TODO: async
    VNext {
        cursor_id: CursorID,
        pc_if_next: BranchOffset,
    },

    /// P4 is the name of a virtual table in database P1. Call the xDestroy method of that table.
    VDestroy {
        /// Name of a virtual table being destroyed
        table_name: String,
        ///  The database within which this virtual table needs to be destroyed (P1).
        db: usize,
    },
    VBegin {
        /// The database within which this virtual table transaction needs to begin (P1).
        cursor_id: CursorID,
    },
    VRename {
        /// The database within which this virtual table needs to be renamed (P1).
        cursor_id: CursorID,
        /// New name of the virtual table (P2).
        new_name_reg: usize,
    },

    /// Open a cursor for a pseudo-table that contains a single row.
    OpenPseudo {
        cursor_id: CursorID,
        content_reg: usize,
        num_fields: usize,
    },

    /// Rewind the cursor to the beginning of the B-Tree.
    Rewind {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
    },

    Last {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
    },

    /// Read a column from the current row of the cursor.
    Column {
        cursor_id: CursorID,
        column: usize,
        dest: usize,
        default: Option<Value>,
    },

    /// Read `defaults.len()` consecutive columns starting at `start_column` from the current row
    /// of the cursor into consecutive registers starting at `dest`.
    ColumnRange {
        cursor_id: CursorID,
        start_column: usize,
        dest: usize,
        // this can't be a SmallVec because it would make the enum too large.
        defaults: Vec<Option<Value>>,
    },

    /// Jump to `target_pc` if the cursor's current record contains a field at
    /// the given column index.  Falls through when the record has fewer fields
    /// (a "short record" from before ALTER TABLE ADD COLUMN).
    ///
    /// # Why this instruction exists
    ///
    /// SQLite's `Column` instruction has a P4 operand that holds an optional
    /// constant default value.  When a record is shorter than expected (because
    /// the column was added after the row was written), Column writes P4 into
    /// the destination register.  Because P4 must be a compile-time constant,
    /// SQLite enforces this in `sqlite3AlterFinishAddColumn`:
    ///
    /// ```c
    /// rc = sqlite3ValueFromExpr(db, pDflt, SQLITE_UTF8, SQLITE_AFF_BLOB, &pVal);
    /// if( !pVal ){
    ///     sqlite3ErrorIfNotEmpty(pParse, zDb, zTab,
    ///         "Cannot add a column with non-constant default");
    /// }
    /// ```
    ///
    /// PostgreSQL takes a different approach: it stores the evaluated default
    /// in `pg_attribute.attmissingval` (for non-volatile expressions) or
    /// rewrites the entire table (for volatile ones).  This lets Postgres
    /// accept `ALTER TABLE ADD COLUMN x DEFAULT now()`.
    ///
    /// We want the same flexibility as Postgres.  Custom types with ENCODE
    /// make this need appear sooner: even when the DEFAULT is a constant
    /// (which SQLite requires), the stored value must be ENCODE(DEFAULT) —
    /// an arbitrary SQL expression applied to that constant.  For example,
    /// `DEFAULT ('Hello')` with `ENCODE (lower(value))` needs to store
    /// `'hello'`.  Since the ENCODE expression can be complex (CASE, function
    /// calls, etc.), it cannot always be constant-folded into a P4 value.
    ///
    /// `ColumnHasField` lets us detect short records at runtime and branch to
    /// bytecode that computes the encoded default dynamically, without
    /// restricting what expressions ENCODE (or future non-constant defaults)
    /// may contain.
    ColumnHasField {
        cursor_id: CursorID,
        column: usize,
        target_pc: BranchOffset,
    },

    TypeCheck {
        start_reg: usize, // P1
        count: usize,     // P2
        /// GENERATED ALWAYS AS ... STORED columns are only checked if P3 is zero.
        /// When P3 is non-zero, no type checking occurs for stored generated columns.
        check_generated: bool, // P3
        table_reference: Arc<BTreeTable>, // P4
    },

    /// Parse a JSON text array into a native record-format BLOB, validating
    /// and coercing each element against the declared type using STRICT
    /// type-checking logic (apply_affinity_char + value_type check).
    /// Input: reg = JSON text like '[1,2,3]'. Output: reg = record-format BLOB.
    /// Raises SQLITE_CONSTRAINT on type mismatch.
    ArrayEncode {
        data: Box<ArrayEncodeData>,
    },

    /// Convert a native record-format BLOB back to PostgreSQL-style array text for display.
    /// Input: reg = record-format BLOB. Output: reg = PG array text like '{1,2,3}'.
    ArrayDecode {
        reg: usize,
    },

    /// Access element at index from a record-format array BLOB.
    /// If array is NULL or index out of bounds, dest = NULL.
    ArrayElement {
        array_reg: usize,
        index_reg: usize,
        dest: usize,
    },

    /// Get the number of elements in a record-format array BLOB.
    /// If input is NULL, dest = 0.
    ArrayLength {
        reg: usize,
        dest: usize,
    },

    /// Create an array from contiguous registers (static count).
    /// Reads `count` values from start_reg..start_reg+count,
    /// serializes via ImmutableRecord, stores Value::Blob in dest.
    MakeArray {
        start_reg: usize,
        count: usize,
        dest: usize,
    },

    /// Create an array from contiguous registers (dynamic count).
    /// Like MakeArray but count is read from count_reg at runtime.
    MakeArrayDynamic {
        start_reg: usize,
        count_reg: usize,
        dest: usize,
    },

    /// Extract a field from a struct blob by field index.
    /// If src_reg is NULL, dest = NULL.
    StructField {
        src_reg: usize,
        field_index: usize,
        dest: usize,
    },

    /// Pack a tag index and a value into a union blob.
    /// Format: [tag_index: 1 byte][record-format value].
    UnionPack {
        tag_index: u8,
        value_reg: usize,
        dest: usize,
    },

    /// Extract the tag name from a union blob as text.
    /// Reads the tag index byte, looks up the name via tag_names.
    /// If src_reg is NULL, dest = NULL. Otherwise dest = tag name as Text.
    UnionTag {
        src_reg: usize,
        dest: usize,
        tag_names: Arc<[String]>,
    },

    /// Extract the value from a union blob if the tag index matches.
    /// If tag matches, dest = extracted value. If mismatch or NULL, dest = NULL.
    UnionExtract {
        src_reg: usize,
        expected_tag: u8,
        dest: usize,
    },

    /// Copy a register value to a dynamically-computed destination.
    /// dest = registers[base + registers[offset_reg]]
    /// registers[base + registers[offset_reg]] = registers[src]
    RegCopyOffset {
        src: usize,
        base: usize,
        offset_reg: usize,
    },
    /// Read `registers[amount]` bytes at `registers[offset]` within column `column`
    /// of the cursor's current table row into `dest` (as a blob), following overflow
    /// pages. The VDBE backing for sqlite3_blob_read: byte-level payload access
    /// without materializing the whole value. Stores NULL into `dest` when the
    /// handle expired (the row was written after the cursor pinned it): expiry is a
    /// per-operation failure in SQLite (SQLITE_ABORT), not a program abort, so the
    /// paused blob program and its transaction must stay alive.
    BlobRead {
        cursor: CursorID,
        column: usize,
        offset: usize,
        amount: usize,
        dest: usize,
    },
    /// Write the blob in `src` at `registers[offset]` within column `column` of the
    /// cursor's current table row, in place across the local page and overflow chain.
    /// The VDBE backing for sqlite3_blob_write; cannot change the value's size.
    /// Stores 1 into `dest` on success, or NULL when the handle expired (see
    /// BlobRead) — expiry must not abort the program, because writes made before it
    /// belong to the paused program's transaction and must survive to commit.
    BlobWrite {
        cursor: CursorID,
        column: usize,
        offset: usize,
        src: usize,
        dest: usize,
    },
    /// Store the byte length of column `column` of the cursor's current table row
    /// into `dest`, erroring unless the value is byte-addressable (TEXT or BLOB).
    /// The VDBE backing for sqlite3_blob_open's length and type validation, run by
    /// the same program that holds the row's cursor so the answer cannot go stale.
    BlobLen {
        cursor: CursorID,
        column: usize,
        dest: usize,
    },
    /// Concatenate/append/prepend arrays. PostgreSQL-compatible semantics:
    /// - blob || blob → array_cat
    /// - blob || scalar → array_append
    /// - scalar || blob → array_prepend
    ///
    /// Falls back to string Concat for non-array operands.
    ArrayConcat {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },

    /// Set element at index in a record-format array BLOB.
    /// Extracts all elements, replaces element at index, rebuilds blob.
    ArraySetElement {
        array_reg: usize,
        index_reg: usize,
        value_reg: usize,
        dest: usize,
    },

    /// Extract a subslice of elements from a record-format array BLOB.
    /// Creates a new array blob from elements[start..end].
    ArraySlice {
        array_reg: usize,
        start_reg: usize,
        end_reg: usize,
        dest: usize,
    },

    // Make a record and write it to destination register.
    MakeRecord {
        start_reg: u32, // P1
        count: u32,     // P2
        dest_reg: u32,  // P3
        index_name: Option<String>,
        affinity_str: Option<String>,
    },

    /// Emit a row of results.
    ResultRow {
        start_reg: usize, // P1
        count: usize,     // P2
    },

    /// Advance the cursor to the next row.
    Next {
        cursor_id: CursorID,
        pc_if_next: BranchOffset,
        /// True when this step is part of a full table scan (a loop over the
        /// whole table with no index or rowid constraint). Only these steps
        /// count toward SQLITE_STMTSTATUS_FULLSCAN_STEP, matching SQLite,
        /// which tags the opcode with P5 at codegen time.
        fullscan: bool,
    },

    Prev {
        cursor_id: CursorID,
        pc_if_prev: BranchOffset,
        /// See [Insn::Next::fullscan].
        fullscan: bool,
    },

    /// Halt the program.
    Halt {
        err_code: usize,
        description: String,
        /// Override the program's resolve_type for error handling (used by RAISE).
        on_error: Option<ResolveType>,
        /// If set, read the error description from this register instead of
        /// the static `description` field (used by RAISE with expression messages).
        description_reg: Option<usize>,
    },

    /// Halt the program if P3 is null.
    HaltIfNull {
        target_reg: usize,   // P3
        description: String, // p4
        err_code: usize,     // p1
    },

    /// Start a transaction.
    Transaction {
        db: usize,                // p1
        tx_mode: TransactionMode, // p2
        schema_cookie: u32,       // p3
    },

    /// Set database auto-commit mode and potentially rollback.
    AutoCommit {
        auto_commit: bool,
        rollback: bool,
    },

    /// Execute a named savepoint operation.
    Savepoint {
        op: SavepointOp,
        name: String,
    },

    /// Branch to the given PC.
    Goto {
        target_pc: BranchOffset,
    },

    /// Stores the current program counter into register 'return_reg' then jumps to address target_pc.
    Gosub {
        target_pc: BranchOffset,
        return_reg: usize,
    },

    /// Returns to the program counter stored in register 'return_reg'.
    /// If can_fallthrough is true, fall through to the next instruction
    /// if return_reg does not contain an integer value. Otherwise raise an error.
    Return {
        return_reg: usize,
        can_fallthrough: bool,
    },

    /// Invoke a trigger or foreign-key action subprogram.
    ///
    /// According to SQLite documentation (https://sqlite.org/opcode.html):
    /// "The Program opcode invokes the trigger subprogram. The Program instruction
    /// allocates and initializes a fresh register set for each invocation of the
    /// subprogram, so subprograms can be reentrant and recursive. The Param opcode
    /// is used by subprograms to access content in registers of the calling bytecode program."
    Program {
        /// Parent register indices for each parameter the subprogram reads.
        /// At runtime, values are copied from these parent registers into
        /// the child statement's parameters via bind_at.
        param_registers: Vec<usize>,
        program: Subprogram,
        /// Jump target when RAISE(IGNORE) fires in the subprogram.
        /// Points to the "skip this row" address in the parent program.
        ignore_jump_target: BranchOffset,
    },

    /// Copy the current VM change counter to the connection-level counters, then reset it.
    /// SQLite emits this between trigger-body DML statements so `changes()` and
    /// `total_changes()` observe the just-completed statement without leaking into the next one.
    ResetCount,

    /// Write the number of rows the current statement has changed so far into a register.
    /// Emitted at the end of INSERT/UPDATE/DELETE programs when PRAGMA count_changes is on,
    /// followed by a ResultRow that returns the count to the caller.
    ChangeCount {
        dest: usize,
    },

    /// Write an integer value into a register.
    Integer {
        value: i64,
        dest: usize,
    },

    /// Write a float value into a register
    Real {
        value: f64,
        dest: usize,
    },

    /// If register holds an integer, transform it to a float
    RealAffinity {
        register: usize,
    },

    // Write a string value into a register.
    String8 {
        value: String,
        dest: usize,
    },

    /// Write a blob value into a register.
    Blob {
        value: crate::ValueBlob,
        dest: usize,
    },

    /// Read a complete row of data from the current cursor and write it to the destination register.
    RowData {
        cursor_id: CursorID,
        dest: usize,
    },

    /// Read the rowid of the current row.
    RowId {
        cursor_id: CursorID,
        dest: usize,
    },
    /// Read the rowid of the current row from an index cursor.
    IdxRowId {
        cursor_id: CursorID,
        dest: usize,
    },

    /// Seek to a rowid in the cursor. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekRowid {
        cursor_id: CursorID,
        src_reg: usize,
        target_pc: BranchOffset,
    },
    SeekEnd {
        cursor_id: CursorID,
    },

    /// P1 is an open index cursor and P3 is a cursor on the corresponding table. This opcode does a deferred seek of the P3 table cursor to the row that corresponds to the current row of P1.
    /// This is a deferred seek. Nothing actually happens until the cursor is used to read a record. That way, if no reads occur, no unnecessary I/O happens.
    DeferredSeek {
        index_cursor_id: CursorID,
        table_cursor_id: CursorID,
    },

    /// If cursor_id refers to an SQL table (B-Tree that uses integer keys), use the value in start_reg as the key.
    /// If cursor_id refers to an SQL index, then start_reg is the first in an array of num_regs registers that are used as an unpacked index key.
    /// Seek to the first index entry that is greater than or equal to the given key. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekGE {
        is_index: bool,
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
        eq_only: bool,
        /// Key columns that use `IS` instead of `=`. NULL may match in these
        /// columns, so the seek must not stop when their key value is NULL.
        null_matching_mask: NullMatchingMask,
    },

    /// If cursor_id refers to an SQL table (B-Tree that uses integer keys), use the value in start_reg as the key.
    /// If cursor_id refers to an SQL index, then start_reg is the first in an array of num_regs registers that are used as an unpacked index key.
    /// Seek to the first index entry that is greater than the given key. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekGT {
        is_index: bool,
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    /// cursor_id is a cursor pointing to a B-Tree index that uses integer keys, this op writes the value obtained from MakeRecord into the index.
    /// P3 + P4 are for the original column values that make up that key in unpacked (pre-serialized) form.
    /// If P5 has the OPFLAG_APPEND bit set, that is a hint to the b-tree layer that this insert is likely to be an append.
    /// OPFLAG_NCHANGE bit set, then the change counter is incremented by this instruction. If the OPFLAG_NCHANGE bit is clear, then the change counter is unchanged
    IdxInsert {
        cursor_id: CursorID,
        record_reg: usize, // P2 the register containing the record to insert
        unpacked_start: Option<usize>, // P3 the index of the first register for the unpacked key
        unpacked_count: Option<u32>, // P4 # of unpacked values in the key in P2
        flags: IdxInsertFlags, // TODO: optimization
    },

    /// The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
    /// If the P1 index entry is greater or equal than the key value then jump to P2. Otherwise fall through to the next instruction.
    // If cursor_id refers to an SQL table (B-Tree that uses integer keys), use the value in start_reg as the key.
    // If cursor_id refers to an SQL index, then start_reg is the first in an array of num_regs registers that are used as an unpacked index key.
    // Seek to the first index entry that is less than or equal to the given key. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekLE {
        is_index: bool,
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
        eq_only: bool,
        /// Key columns that use `IS` instead of `=`. NULL may match in these
        /// columns, so the seek must not stop when their key value is NULL.
        null_matching_mask: NullMatchingMask,
    },

    // If cursor_id refers to an SQL table (B-Tree that uses integer keys), use the value in start_reg as the key.
    // If cursor_id refers to an SQL index, then start_reg is the first in an array of num_regs registers that are used as an unpacked index key.
    // Seek to the first index entry that is less than the given key. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekLT {
        is_index: bool,
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    // The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
    // If the P1 index entry is greater or equal than the key value then jump to P2. Otherwise fall through to the next instruction.
    IdxGE {
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    /// The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
    /// If the P1 index entry is greater than the key value then jump to P2. Otherwise fall through to the next instruction.
    IdxGT {
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    /// The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
    /// If the P1 index entry is lesser or equal than the key value then jump to P2. Otherwise fall through to the next instruction.
    IdxLE {
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    /// The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
    /// If the P1 index entry is lesser than the key value then jump to P2. Otherwise fall through to the next instruction.
    IdxLT {
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    /// Decrement the given register and jump to the given PC if the result is zero.
    DecrJumpZero {
        reg: usize,
        target_pc: BranchOffset,
    },

    AggStep {
        data: Box<AggStepData>,
    },

    /// Mirror-image of AggStep: fires when a row crosses the frame-start
    /// cursor on its way out of the frame. The runtime arm undoes the
    /// matching xStep — sum subtracts, count decrements, position
    /// counters advance.
    AggInverse {
        acc_reg: usize,
        col: usize,
        delimiter: usize,
        func: AccumulatorFunc,
        comparator: Option<SortComparatorType>,
    },

    AggFinal {
        register: usize,
        func: AccumulatorFunc,
    },

    /// Similar to AggFinal, but instead of writing the result back into the
    /// accumulator register, it stores the result in a separate destination
    /// register.
    AggValue {
        acc_reg: usize,
        dest_reg: usize,
        func: AccumulatorFunc,
    },

    /// Open a sorter.
    SorterOpen {
        data: Box<SorterOpenData>,
    },

    /// Insert a row into the sorter.
    SorterInsert {
        cursor_id: CursorID,
        record_reg: usize,
    },

    /// `cursor_id` is a sorter cursor. This instruction compares a prefix of the record blob in register `sorted_record_reg`
    /// against a prefix of the entry that the sorter cursor currently points to.
    /// Only the first `num_regs` fields of `sorted_record_reg` and the sorter record are compared.
    /// Fall through to next instruction if the two records compare equal to each other.
    /// Jump to `pc_when_nonequal` if they are different.
    SorterCompare {
        cursor_id: CursorID,
        pc_when_nonequal: BranchOffset,
        sorted_record_reg: usize,
        num_regs: usize,
    },

    /// Sort the rows in the sorter.
    SorterSort {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
    },

    /// Retrieve the next row from the sorter.
    SorterData {
        cursor_id: CursorID,  // P1
        dest_reg: usize,      // P2
        pseudo_cursor: usize, // P3
    },

    /// Advance to the next row in the sorter.
    SorterNext {
        cursor_id: CursorID,
        pc_if_next: BranchOffset,
    },

    /// Insert the integer value held by register P2 into a RowSet object held in register P1.
    /// An assertion fails if P2 is not an integer.
    RowSetAdd {
        rowset_reg: usize, // P1 - register holding RowSet
        value_reg: usize,  // P2 - register holding integer value to add
    },

    /// Extract the smallest value from the RowSet object in P1 and put that value into register P3.
    /// Or, if RowSet object P1 is initially empty, leave P3 unchanged and jump to instruction P2.
    RowSetRead {
        rowset_reg: usize,         // P1 - register holding RowSet
        pc_if_empty: BranchOffset, // P2 - jump target if empty
        dest_reg: usize,           // P3 - register to store smallest value
    },

    /// Register P3 is assumed to hold a 64-bit integer value. If register P1 contains a RowSet object
    /// and that RowSet object contains the value held in P3, jump to register P2. Otherwise, insert
    /// the integer in P3 into the RowSet and continue on to the next opcode.
    /// P4 is the batch identifier (0 for first set, -1 for final set, >0 for other sets).
    RowSetTest {
        rowset_reg: usize,         // P1 - register holding RowSet
        pc_if_found: BranchOffset, // P2 - jump target if value found
        value_reg: usize,          // P3 - register holding integer value to test/insert
        batch: i32,                // P4 - batch identifier
    },

    /// Function
    Function {
        constant_mask: i32, // P1
        start_reg: usize,   // P2, start of argument registers
        dest: usize,        // P3
        func: FuncCtx,      // P4
    },

    /// Cast register P1 to affinity P2 and store in register P1
    Cast {
        reg: usize,
        affinity: Affinity,
    },

    InitCoroutine {
        yield_reg: usize,
        jump_on_definition: BranchOffset,
        start_offset: BranchOffset,
    },

    EndCoroutine {
        yield_reg: usize,
    },

    Yield {
        yield_reg: usize,
        end_offset: BranchOffset,
        /// For coroutine body yields (end_offset == 0): the start register of the
        /// output columns and how many there are.  op_yield uses these to strip
        /// the JSON subtype so that it does not survive the subquery boundary,
        /// mirroring SQLite's OP_Copy P5=0x0002 behaviour.
        /// Set to 0/0 for parent-side (non-body) yields.
        subtype_clear_start_reg: usize,
        subtype_clear_count: usize,
    },

    Insert {
        cursor: CursorID,
        key_reg: usize,    // Must be int.
        record_reg: usize, // Blob of record data.
        flag: InsertFlags, // Flags used by insert, for now not used.
        table_name: String,
    },

    Int64 {
        _p1: usize,     //  unused
        out_reg: usize, // the output register
        _p3: usize,     // unused
        value: i64,     //  the value being written into the output register
    },

    Delete {
        cursor_id: CursorID,
        table_name: String,
        /// Whether the DELETE is part of an UPDATE statement. If so, it doesn't count towards the change counter.
        is_part_of_update: bool,
    },

    /// If P5 is not zero, then raise an SQLITE_CORRUPT_INDEX error if no matching index entry
    /// is found. This happens when running an UPDATE or DELETE statement and the index entry to
    /// be updated or deleted is not found. For some uses of IdxDelete (example: the EXCEPT operator)
    /// it does not matter that no matching entry is found. For those cases, P5 is zero.
    IdxDelete {
        start_reg: usize,
        num_regs: usize,
        cursor_id: CursorID,
        raise_error_if_no_matching_entry: bool, // P5
    },

    NewRowid {
        cursor: CursorID,        // P1
        rowid_reg: usize,        // P2  Destination register to store the new rowid
        prev_largest_reg: usize, // P3 Previous largest rowid in the table (Not used for now)
    },

    MustBeInt {
        reg: usize,
        target_pc: Option<BranchOffset>,
    },

    SoftNull {
        reg: usize,
    },

    /// If P4==0 then register P3 holds a blob constructed by [MakeRecord](https://sqlite.org/opcode.html#MakeRecord).
    /// If P4>0 then register P3 is the first of P4 registers that form an unpacked record.
    ///
    /// Cursor P1 is on an index btree. If the record identified by P3 and P4 contains any NULL value, jump immediately
    /// to P2. If all terms of the record are not-NULL then a check is done to determine if any row in the P1 index
    /// btree has a matching key prefix. If there are no matches, jump immediately to P2. If there is a match, fall
    /// through and leave the P1 cursor pointing to the matching row.\
    ///
    /// This opcode is similar to [NotFound](https://sqlite.org/opcode.html#NotFound) with the exceptions that the
    /// branch is always taken if any part of the search key input is NULL.
    NoConflict {
        cursor_id: CursorID,     // P1 index cursor
        target_pc: BranchOffset, // P2 jump target
        record_reg: usize,
        num_regs: usize,
    },

    NotExists {
        cursor: CursorID,
        rowid_reg: usize,
        target_pc: BranchOffset,
    },

    OffsetLimit {
        limit_reg: usize,
        combined_reg: usize,
        offset_reg: usize,
    },

    OpenWrite {
        cursor_id: CursorID,
        root_page: RegisterOrLiteral<PageIdx>,
        db: usize,
    },

    /// Make a copy of register src..src+extra_amount into dst..dst+extra_amount.
    Copy {
        src_reg: usize,
        dst_reg: usize,
        /// 0 extra_amount means we include src_reg, dst_reg..=dst_reg+amount = src_reg..=src_reg+amount
        extra_amount: usize,
    },

    /// Allocate a new b-tree.
    CreateBtree {
        /// Allocate b-tree in main database if zero or in temp database if non-zero (P1).
        db: usize,
        /// The root page of the new b-tree (P2).
        root: usize,
        /// Flags (P3).
        flags: CreateBTreeFlags,
    },

    /// Create custom index method (calls [crate::index_method::IndexMethodCursor::create] under the hood)
    IndexMethodCreate {
        db: usize,
        cursor_id: CursorID,
    },
    /// Destroy custom index method (calls [crate::index_method::IndexMethodCursor::destroy] under the hood)
    IndexMethodDestroy {
        db: usize,
        cursor_id: CursorID,
    },
    /// Optimize custom index method (calls [crate::index_method::IndexMethodCursor::optimize] under the hood)
    IndexMethodOptimize {
        db: usize,
        cursor_id: CursorID,
    },
    /// Query custom index method (call [crate::index_method::IndexMethodCursor::query_start] under the hood)
    IndexMethodQuery {
        db: usize,
        cursor_id: CursorID,
        start_reg: usize,
        count_reg: usize,
        pc_if_empty: BranchOffset,
    },

    /// Delete all contents from a persistent table or index b-tree while keeping its root page.
    ClearBtree {
        db: usize,
        root: i64,
    },

    /// Deletes an entire database table or index whose root page in the database file is given by P1.
    Destroy {
        /// The database index (0 = main, 1 = temp, 2+ = attached)
        db: usize,
        /// The root page of the table/index to destroy
        root: i64,
        /// Register to store the former value of any moved root page (for AUTOVACUUM)
        former_root_reg: usize,
        /// Whether this is a temporary table (1) or main database table (0)
        is_temp: usize,
    },

    /// Deletes all contents from the ephemeral table that the cursor points to.
    ///
    /// In Turso, we do not currently distinguish strictly between ephemeral
    /// and standard tables at the type level. Therefore, it is the caller’s
    /// responsibility to ensure that `ResetSorter` is applied only to ephemeral
    /// tables.
    ///
    /// SQLite also supports sorter cursors, but this is not yet implemented in Turso.
    ResetSorter {
        cursor_id: CursorID,
    },

    ///  Drop a table
    DropTable {
        ///  The database within which this b-tree needs to be dropped (P1).
        db: usize,
        ///  unused register p2
        _p2: usize,
        ///  unused register p3
        _p3: usize,
        //  The name of the table being dropped
        table_name: String,
    },
    DropView {
        /// The database within which this view needs to be dropped
        db: usize,
        /// The name of the view being dropped
        view_name: String,
    },
    DropIndex {
        ///  The database within which this index needs to be dropped (P1).
        db: usize,
        //  The name of the index being dropped
        index: Arc<Index>,
    },
    /// Drop a trigger
    DropTrigger {
        /// The database within which this trigger needs to be dropped (P1).
        db: usize,
        /// The name of the trigger being dropped
        trigger_name: String,
    },
    /// Drop a custom type from the in-memory schema
    DropType {
        /// The database within which this type needs to be dropped
        db: usize,
        /// The name of the type being dropped
        type_name: String,
    },
    /// Add a fully-configured sequence to the in-memory schema.
    /// Emitted by CREATE SEQUENCE after ParseSchema has added the backing table.
    AddSequence {
        data: Box<AddSequenceData>,
    },
    /// Drop a sequence from the in-memory schema
    DropSequence {
        /// The database within which this sequence needs to be dropped
        db: usize,
        /// The name of the sequence being dropped
        seq_name: String,
    },
    /// Begin the autonomous inner transaction that wraps a sequence
    /// read-modify-write. The translator emits this immediately before
    /// the cursor-based RMW bytecode and pairs it with a matching
    /// `SequenceCommitInnerTx`.
    ///
    /// Path selection happens here at runtime:
    ///
    /// * Skipped — when MVCC is not in use for `db`, OR the connection's
    ///   current outer tx is exclusive (autocommit Write, BEGIN, BEGIN
    ///   IMMEDIATE, BEGIN DEFERRED). The cursor RMW that follows runs
    ///   inline in the outer tx. The matching `SequenceCommitInnerTx`
    ///   is a no-op. Under exclusive no other writer can race, so
    ///   in-tx rollback unburning the value is acceptable per Turso's
    ///   relaxed-burnt-value contract.
    /// * Wrapped — outer is Concurrent or autocommit Read or none.
    ///   Begins a fresh Concurrent inner tx via `mv_store.begin_tx`,
    ///   saves the outer's `(tx_id, mode)`, and swaps the connection's
    ///   mv_tx for `db` to the inner. The cursor RMW runs against the
    ///   inner. `SequenceCommitInnerTx` then commits the inner
    ///   independently of the outer's eventual fate.
    ///
    /// Outputs:
    /// * `path_kind_reg` — Integer(0)=Skipped, Integer(1)=Wrapped.
    /// * `saved_outer_reg` — opaque snapshot of the prior mv_tx for
    ///   `db`. Format is private to the begin/commit pair; consumers
    ///   must not interpret it. `Null` when Skipped.
    SequenceBeginInnerTx {
        db: usize,
        path_kind_reg: usize,
        saved_outer_reg: usize,
    },
    /// Commit the autonomous inner transaction started by a matching
    /// `SequenceBeginInnerTx`. Multi-step: drives the inner's
    /// `CommitStateMachine` one step per opcode call, yielding
    /// `InsnFunctionStepResult::IO` to the VDBE driver when the
    /// state machine needs IO (mirrors `op_auto_commit`'s drive of
    /// the outer commit).
    ///
    /// On terminal outcomes the opcode advances the pc by 1 and writes
    /// `status_reg`:
    /// * `0 (Ok)` — commit completed (or path was Skipped).
    /// * `1 (ConflictRetry)` — commit hit
    ///   `WriteWriteConflict`/`BusySnapshot`/`Conflict`. Inner tx
    ///   rolled back. The translator emits a conditional jump back to
    ///   the retry-top label after this opcode so the RMW restarts
    ///   under a fresh inner tx.
    ///
    /// Other errors propagate up unchanged.
    SequenceCommitInnerTx {
        db: usize,
        path_kind_reg: usize,
        saved_outer_reg: usize,
        status_reg: usize,
    },
    /// Compute the next value of a sequence from the just-read watermark row.
    /// Pure synchronous arithmetic — no I/O. The caller (translator) emits
    /// cursor seek + Column reads to load the watermark into `in_value_reg`
    /// and `in_is_called_reg`; this opcode applies the start/inc/min/max/cycle
    /// logic to produce the next value into `out_value_reg`. If
    /// `was_empty_reg` (set by caller via IsNull-style branching) indicates
    /// the backing table is empty, the next value is `start_value`. Returns
    /// `LimboError::DatabaseFull` on overflow when `cycle` is false.
    SequenceComputeNext {
        db: usize,
        seq_name_reg: usize,
        in_value_reg: usize,
        in_is_called_reg: usize,
        was_empty_reg: usize,
        out_value_reg: usize,
    },
    /// Record this connection's currval for a sequence after a successful
    /// nextval/setval. Pure synchronous register operation. currval is
    /// per-connection global (not schema-qualified), so no database id
    /// is carried — schema-qualified currvals would be a separate feature.
    SetSequenceCurrval {
        seq_name_reg: usize,
        value_reg: usize,
    },
    /// Publish sequence allocation metadata for sync watermarks after a
    /// successful sequence RMW commit.
    SequenceTrackAllocation {
        db: usize,
        seq_name_reg: usize,
        value_reg: usize,
    },
    /// Register an in-flight sequence allocation against the *outer*
    /// transaction *before* the inner-tx RMW commit publishes the new
    /// boundary. Emitted ahead of `SequenceCommitInnerTx` so the active
    /// allocation is visible to `sequence_watermark_experimental()` the
    /// instant another connection can observe (and advance past) this
    /// value. See `op_sequence_register_allocation` for the race this
    /// closes.
    SequenceRegisterAllocation {
        db: usize,
        seq_name_reg: usize,
        value_reg: usize,
        /// Register holding the encoded saved outer mv_tx (the blob written
        /// by `SequenceBeginInnerTx`). Empty blob means "no outer tx".
        saved_outer_reg: usize,
    },
    /// Add a custom type to the in-memory schema by parsing its CREATE TYPE SQL
    AddType {
        /// The database within which this type needs to be added
        db: usize,
        /// The full CREATE TYPE SQL string
        sql: String,
    },

    /// Close a cursor.
    Close {
        cursor_id: CursorID,
    },

    /// Check if the register is null.
    IsNull {
        /// Source register (P1).
        reg: usize,

        /// Jump to this PC if the register is null (P2).
        target_pc: BranchOffset,
    },

    ParseSchema {
        db: usize,
        where_clause: Option<String>,
        /// The database containing the table used by an unqualified TEMP trigger.
        ///
        /// SQLite can look through every attached database while rebuilding a
        /// trigger. Turso's schema reader handles one schema at a time, so it
        /// cannot repeat the lookup already done by CREATE TRIGGER. The SQL in
        /// temp.sqlite_schema only says `ON table`, not which database supplied
        /// `table`. CREATE TRIGGER passes the answer here so ParseSchema can keep
        /// it on the in-memory trigger. Every other ParseSchema use leaves this
        /// as `None`.
        trigger_target_database_id: Option<usize>,
    },

    /// Populate all materialized views after schema parsing
    /// The cursors parameter contains a mapping of view names to cursor IDs that have been
    /// opened to the view's btree for writing the materialized data
    PopulateMaterializedViews {
        /// Mapping of view name to cursor_id for writing to the view's btree
        cursors: Vec<(String, usize)>,
    },

    /// Place the result of lhs >> rhs in dest register.
    ShiftRight {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },

    /// Place the result of lhs << rhs in dest register.
    ShiftLeft {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },

    /// Add immediate value to register and force integer conversion.
    /// Add the constant P2 to the value in register P1. The result is always an integer.
    /// To force any register to be an integer, just add 0.
    AddImm {
        register: usize, // P1: target register
        value: i64,      // P2: immediate value to add
    },

    /// Get parameter variable.
    Variable {
        index: NonZero<usize>,
        dest: usize,
    },
    /// If either register is null put null else put 0
    ZeroOrNull {
        /// Source register (P1).
        rg1: usize,
        rg2: usize,
        dest: usize,
    },
    /// Interpret the value in reg as boolean and store its compliment in destination
    Not {
        reg: usize,
        dest: usize,
    },
    /// Interpret the value in register `reg` as a boolean and store in `dest`.
    /// Used to implement IS TRUE, IS FALSE, IS NOT TRUE, IS NOT FALSE.
    ///
    /// A value is considered "true" if it is a non-zero number.
    /// Strings, blobs, and zero are "false". NULL is handled specially.
    ///
    /// - If reg is NULL, store `null_value` in dest
    /// - Otherwise, store 1 if the value is a non-zero number, 0 otherwise
    /// - If `invert` is true, invert the result (0↔1)
    IsTrue {
        reg: usize,
        dest: usize,
        /// Value to store if input is NULL (0 or 1)
        null_value: bool,
        /// Whether to invert the result
        invert: bool,
    },
    /// Concatenates the `rhs` and `lhs` values and stores the result in the third register.
    Concat {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Take the logical AND of the values in registers P1 and P2 and write the result into register P3.
    And {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Take the logical OR of the values in register P1 and P2 and store the answer in register P3.
    Or {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    /// Do nothing. Continue downward to the next opcode.
    Noop,
    /// Write the current number of pages in database P1 to memory cell P2.
    PageCount {
        db: usize,
        dest: usize,
    },
    /// Read cookie number P3 from database P1 and write it into register P2
    ReadCookie {
        db: usize,
        dest: usize,
        cookie: Cookie,
    },
    /// Write the value in register P3 into cookie number P2 of database P1.
    /// If P2 is the SCHEMA_VERSION cookie (cookie number 1) then the internal schema version is set to P3-P5
    SetCookie {
        db: usize,
        cookie: Cookie,
        value: i32,
        p5: u16,
    },
    /// Open a new cursor P1 to a transient table.
    OpenEphemeral {
        cursor_id: usize,
        is_table: bool,
    },
    /// Works the same as OpenEphemeral, name just distinguishes its use; used for transient indexes in joins.
    OpenAutoindex {
        cursor_id: usize,
    },
    /// Opens a new cursor that points to the same table as the original.
    /// In SQLite, this is restricted to cursors opened by `OpenEphemeral`
    /// (i.e., ephemeral tables), and only ephemeral cursors may be duplicated.
    /// In Turso, we currently do not strictly distinguish between ephemeral
    /// and standard tables at the type level. Therefore, it is the caller’s
    /// responsibility to ensure that `OpenDup` is applied only to ephemeral
    /// cursors.
    OpenDup {
        new_cursor_id: CursorID,
        original_cursor_id: CursorID,
    },
    /// Fall through to the next instruction on the first invocation, otherwise jump to target_pc
    Once {
        target_pc_when_reentered: BranchOffset,
    },
    /// Forget that any [Insn::Once] between this instruction and `region_end`
    /// has already run. Trigger bodies are inlined and re-executed once per
    /// affected row, so their run-once blocks (uncorrelated subqueries,
    /// hash/ephemeral builds) must re-run each firing instead of reusing a
    /// value cached during an earlier firing. Emitted at the start of each
    /// trigger firing, this restores the fresh once-state that SQLite gets from
    /// a per-invocation trigger sub-program.
    ResetOnce {
        region_end: BranchOffset,
    },
    /// Search for a record in the index cursor.
    /// If any entry for which the key is a prefix exists, jump to target_pc.
    /// Otherwise, continue to the next instruction.
    Found {
        cursor_id: CursorID,
        target_pc: BranchOffset,
        record_reg: usize,
        num_regs: usize,
    },
    /// Search for record in the index cusor, if any entry for which the key is a prefix exists
    /// is a no-op, otherwise go to target_pc
    /// Example =>
    /// For a index key (1,2,3):
    /// NotFound((1,2,3)) => No-op
    /// NotFound((1,2)) => No-op
    /// NotFound((2,2, 1)) => Jump
    NotFound {
        cursor_id: CursorID,
        target_pc: BranchOffset,
        record_reg: usize,
        num_regs: usize,
    },
    /// Apply affinities to a range of registers. Affinities must have the same size of count
    Affinity {
        start_reg: usize,
        count: NonZeroUsize,
        affinities: String,
    },

    /// Store the number of entries (an integer value) in the table or index opened by cursor P1 in register P2.
    ///
    /// If P3==0, then an exact count is obtained, which involves visiting every btree page of the table.
    /// But if P3 is non-zero, an estimate is returned based on the current cursor position.
    Count {
        cursor_id: CursorID,
        target_reg: usize,
        exact: bool,
    },

    /// Perform low-level btree/freelist structural integrity checks.
    /// Writes NULL to `message_register` when no structural problem is found,
    /// otherwise writes a textual error summary.
    /// Higher-level semantic checks (row/index consistency, constraints, etc.)
    /// are emitted as normal VDBE bytecode in translation.
    ///
    /// In passive MVCC mode, `dropped_roots` lists checkpointed objects dropped before the next
    /// checkpoint; execute walks them after live roots and skips pages already accounted for.
    IntegrityCk {
        data: Box<IntegrityCkData>,
    },
    RenameTable {
        db: usize,
        from: String,
        to: String,
    },
    DropColumn {
        db: usize,
        table: String,
        column_index: usize,
    },
    AddColumn {
        data: Box<AddColumnData>,
    },
    AlterColumn {
        db: usize,
        table: String,
        column_index: usize,
        definition: Box<turso_parser::ast::ColumnDefinition>,
        rename: bool,
    },
    /// Try to set the maximum page count for database P1 to the value in P3.
    /// Do not let the maximum page count fall below the current page count and
    /// do not change the maximum page count value if P3==0.
    /// Store the maximum page count after the change in register P2.
    MaxPgcnt {
        db: usize,      // P1: database index
        dest: usize,    // P2: output register
        new_max: usize, // P3: new maximum page count (0 = just return current)
    },
    /// Get or set the journal mode for database P1.
    /// If P3 is not null, it contains the new journal mode string.
    /// Store the resulting journal mode in register P2.
    JournalMode {
        db: usize,                // P1: database index
        dest: usize,              // P2: output register for result
        new_mode: Option<String>, // P3: new journal mode (if setting)
    },
    IfNeg {
        reg: usize,
        target_pc: BranchOffset,
    },

    /// Find the next available sequence number for cursor P1. Write the sequence number into register P2.
    /// The sequence number on the cursor is incremented after this instruction.
    Sequence {
        cursor_id: CursorID,
        target_reg: usize,
    },

    /// P1 is a sorter cursor. If the sequence counter is currently zero, jump to P2. Regardless of whether or not the jump is taken, increment the the sequence value.
    SequenceTest {
        cursor_id: CursorID,
        target_pc: BranchOffset,
        value_reg: usize,
    },

    // OP_Explain
    Explain {
        p1: usize,                                     // P1: address of instruction
        p2: Option<usize>,                             // P2: address of parent explain instruction
        detail: Box<crate::translate::eqp::EqpDetail>, // P4: structured plan that `Display`s to the detail text
    },
    // Increment a "constraint counter" by P2 (P2 may be negative or positive).
    // If P1 is non-zero, the database constraint counter is incremented (deferred foreign key constraints).
    // Otherwise, if P1 is zero, the statement counter is incremented (immediate foreign key constraints).
    FkCounter {
        increment_value: isize,
        deferred: bool,
    },
    // This opcode tests if a foreign key constraint-counter is currently zero. If so, jump to instruction P2. Otherwise, fall through to the next instruction.
    // If P1 is non-zero, then the jump is taken if the database constraint-counter is zero (the one that counts deferred constraint violations).
    // If P1 is zero, the jump is taken if the statement constraint-counter is zero (immediate foreign key constraint violations).
    FkIfZero {
        deferred: bool,
        target_pc: BranchOffset,
    },
    // Check if there are any unresolved foreign key constraint violations.
    // If P1 is zero, check the statement constraint-counter (immediate FK violations).
    // If P1 is non-zero, check the database constraint-counter (deferred FK violations).
    // If violations exist, throw SQLITE_CONSTRAINT_FOREIGNKEY.
    FkCheck {
        deferred: bool,
    },

    /// Build a hash table from a cursor for hash join.
    HashBuild {
        data: Box<HashBuildData>,
    },

    /// Deduplicate using a hash table. Jumps to target_pc if duplicate found.
    HashDistinct {
        data: Box<HashDistinctData>,
    },

    /// Finalize the hash table build phase. Transitions the hash table from Building to Probing state.
    /// Should be called after the HashBuild loop completes.
    HashBuildFinalize {
        hash_table_id: usize,
    },

    /// Probe a hash table for matches.
    /// Extract probe keys from registers key_start_reg..key_start_reg+num_keys-1,
    /// hash them, and look up matches in the hash table stored in hash_table_reg.
    /// For each match, load the build-side rowid into dest_reg and continue.
    /// If payload columns were stored during build, they are written to
    /// payload_dest_reg..payload_dest_reg+num_payload-1.
    /// If no matches, jump to target_pc.
    HashProbe {
        hash_table_id: u32,
        key_start_reg: u32,
        num_keys: u32,
        dest_reg: u32,
        target_pc: BranchOffset,
        /// Starting register to write payload columns from hash entry.
        payload_dest_reg: Option<u32>,
        /// Number of payload columns expected
        num_payload: u32,
        /// Register containing probe-side rowid for grace hash join buffering.
        /// When Some and target partition is on disk, buffer the probe row
        /// instead of loading the partition on demand.
        /// When None, this instruction is running inside grace processing and
        /// the build partition must already be loaded.
        probe_rowid_reg: Option<u32>,
    },

    /// Advance to next matching row in hash table bucket.
    /// Used for handling hash collisions and duplicate keys.
    /// If another match is found, store rowid in dest_reg (and payload in payload_dest_reg if set).
    /// If no more matches, jump to target_pc.
    HashNext {
        hash_table_id: usize,
        dest_reg: usize,
        target_pc: BranchOffset,
        /// Starting register to write payload columns from hash entry, if we are caching payload.
        payload_dest_reg: Option<usize>,
        /// Number of payload columns expected
        num_payload: usize,
    },

    /// Free hash table resources.
    /// Closes the hash table referenced by hash_table_id and releases memory.
    HashClose {
        hash_table_id: usize,
    },

    /// Clear hash table entries without releasing the table itself.
    HashClear {
        hash_table_id: usize,
    },

    /// Mark the current hash table match entry as "matched" (for FULL OUTER JOIN).
    HashMarkMatched {
        hash_table_id: usize,
    },

    /// Reset all matched_bits in a hash table to false.
    /// Emitted at the start of each outer-loop iteration so that marks from
    /// a previous probe pass don't suppress NULL-fill rows in the current one.
    HashResetMatched {
        hash_table_id: usize,
    },

    /// Begin scanning unmatched entries in the hash table (for FULL OUTER JOIN).
    /// Writes the first unmatched entry's rowid to dest_reg and payload to payload_dest_reg.
    /// If no unmatched entries exist, jumps to target_pc.
    HashScanUnmatched {
        hash_table_id: usize,
        dest_reg: usize,
        target_pc: BranchOffset,
        payload_dest_reg: Option<usize>,
        num_payload: usize,
    },

    /// Advance to the next unmatched entry in the hash table (for FULL OUTER JOIN).
    /// If another unmatched entry is found, writes rowid to dest_reg and payload to payload_dest_reg.
    /// If no more unmatched entries, jumps to target_pc.
    HashNextUnmatched {
        hash_table_id: usize,
        dest_reg: usize,
        target_pc: BranchOffset,
        payload_dest_reg: Option<usize>,
        num_payload: usize,
    },

    /// Initialize grace hash join processing after the probe cursor is exhausted.
    /// Finalizes probe-side spills and calls grace_begin.
    /// Jumps to target_pc if no spilling occurred or no partitions to process.
    HashGraceInit {
        hash_table_id: u32,
        target_pc: BranchOffset,
    },

    /// Load the current grace partition's build side from disk.
    /// Also loads the first probe chunk. Jumps to target_pc when all partitions done.
    HashGraceLoadPartition {
        hash_table_id: u32,
        target_pc: BranchOffset,
    },

    /// Advance to next probe entry in the current grace partition.
    /// Writes probe keys to key_start_reg..key_start_reg+num_keys-1 and probe rowid to probe_rowid_dest.
    /// Jumps to target_pc when probe entries exhausted.
    HashGraceNextProbe {
        hash_table_id: u32,
        key_start_reg: u32,
        num_keys: u32,
        probe_rowid_dest: u32,
        target_pc: BranchOffset,
    },

    /// Evict current grace partition and advance to the next one.
    /// Jumps to target_pc when all partitions are processed.
    HashGraceAdvancePartition {
        hash_table_id: u32,
        target_pc: BranchOffset,
    },

    /// VACUUM INTO - create a compacted copy of the database at the specified path.
    /// This copies all schema and data from the current database to a new file.
    VacuumInto {
        /// Database name to vacuum
        schema_name: String,
        /// Destination file path for the vacuumed database
        dest_path: String,
    },

    /// In-place VACUUM - compact the database (by writing to a temporary location and then copying
    /// back)
    Vacuum {
        /// Database index to vacuum (0 = main)
        db: usize,
    },

    /// Ensure turso_cdc_version table exists and insert/replace a version row,
    /// then enable CDC on the connection. Runs nested SQL at VDBE execution time
    /// (same pattern as ParseSchema). CDC is enabled after version table operations
    /// so those operations are not captured.
    ///
    /// A dedicated opcode is needed because the PRAGMA SET handler may create the
    /// CDC table (via translate_create_table) and then needs to insert data into
    /// turso_cdc_version — which requires a schema change followed by DML against
    /// the new table. This is hard to express in a single translation plan since
    /// plans are compiled against a fixed schema, so the version table operations
    /// are deferred to execution time via this opcode.
    InitCdcVersion {
        cdc_table_name: String,
        version: crate::CdcVersion,
        cdc_mode: String,
    },
}

const fn get_insn_virtual_table() -> [InsnFunction; InsnVariants::COUNT] {
    let mut result: [InsnFunction; InsnVariants::COUNT] = [execute::op_init; InsnVariants::COUNT];

    let mut insn = 0;
    while insn < InsnVariants::COUNT {
        result[insn] = InsnVariants::from_repr(insn as u8)
            .expect("insn index should be valid within COUNT")
            .to_function();
        insn += 1;
    }

    result
}

const INSN_VTABLE: [InsnFunction; InsnVariants::COUNT] = get_insn_virtual_table();

/// Dispatches one instruction.
///
/// The opcodes that are most likely to run in a loop are matched directly so LLVM can inline them
/// into the dispatch loop.
#[inline(always)]
pub(crate) fn dispatch_insn(
    program: &super::Program,
    state: &mut super::ProgramState,
    insn: &Insn,
    pager: &std::sync::Arc<crate::Pager>,
) -> execute::InsnResult {
    match insn {
        Insn::Next { .. } => execute::op_next(program, state, insn, pager),
        Insn::ResultRow { .. } => execute::op_result_row(program, state, insn, pager),
        Insn::Column { .. } => execute::op_column(program, state, insn, pager),
        Insn::ColumnRange { .. } => execute::op_column_range(program, state, insn, pager),
        Insn::RowId { .. } => execute::op_row_id(program, state, insn, pager),
        Insn::Prev { .. } => execute::op_prev(program, state, insn, pager),
        _ => insn.to_function()(program, state, insn, pager),
    }
}

impl InsnVariants {
    // This function is used for testing
    #[allow(dead_code)]
    #[inline(always)]
    pub(crate) const fn to_function_fast(self) -> InsnFunction {
        INSN_VTABLE[self as usize]
    }

    // This function is used for generating `INSN_VTABLE`.
    // We need to keep this function to make sure we implement all opcodes
    pub(crate) const fn to_function(self) -> InsnFunction {
        match self {
            InsnVariants::Init => execute::op_init,
            InsnVariants::Null => execute::op_null,
            InsnVariants::BeginSubrtn => execute::op_null,
            InsnVariants::NullRow => execute::op_null_row,
            InsnVariants::Add => execute::op_add,
            InsnVariants::Subtract => execute::op_subtract,
            InsnVariants::Multiply => execute::op_multiply,
            InsnVariants::Divide => execute::op_divide,
            InsnVariants::DropIndex => execute::op_drop_index,
            InsnVariants::Compare => execute::op_compare,
            InsnVariants::BitAnd => execute::op_bit_and,
            InsnVariants::BitOr => execute::op_bit_or,
            InsnVariants::BitNot => execute::op_bit_not,
            InsnVariants::Checkpoint => execute::op_checkpoint,
            InsnVariants::Remainder => execute::op_remainder,
            InsnVariants::Jump => execute::op_jump,
            InsnVariants::Move => execute::op_move,
            InsnVariants::IfPos => execute::op_if_pos,
            InsnVariants::NotNull => execute::op_not_null,
            InsnVariants::Eq => execute::op_eq,
            InsnVariants::Ne => execute::op_ne,
            InsnVariants::Lt => execute::op_lt,
            InsnVariants::Le => execute::op_le,
            InsnVariants::Gt => execute::op_gt,
            InsnVariants::Ge => execute::op_ge,
            InsnVariants::If => execute::op_if,
            InsnVariants::IfNot => execute::op_if_not,
            InsnVariants::OpenRead => execute::op_open_read,
            InsnVariants::VOpen => execute::op_vopen,
            InsnVariants::VCreate => execute::op_vcreate,
            InsnVariants::VFilter => execute::op_vfilter,
            InsnVariants::VColumn => execute::op_vcolumn,
            InsnVariants::VUpdate => execute::op_vupdate,
            InsnVariants::VNext => execute::op_vnext,
            InsnVariants::VDestroy => execute::op_vdestroy,
            InsnVariants::OpenPseudo => execute::op_open_pseudo,
            InsnVariants::Rewind => execute::op_rewind,
            InsnVariants::Last => execute::op_last,
            InsnVariants::Column => execute::op_column,
            InsnVariants::ColumnRange => execute::op_column_range,
            InsnVariants::ColumnHasField => execute::op_column_has_field,
            InsnVariants::TypeCheck => execute::op_type_check,
            InsnVariants::ArrayEncode => execute::op_array_encode,
            InsnVariants::ArrayDecode => execute::op_array_decode,
            InsnVariants::ArrayElement => execute::op_array_element,
            InsnVariants::ArrayLength => execute::op_array_length,
            InsnVariants::MakeArray => execute::op_make_array,
            InsnVariants::MakeArrayDynamic => execute::op_make_array_dynamic,
            InsnVariants::StructField => execute::op_struct_field,
            InsnVariants::UnionPack => execute::op_union_pack,
            InsnVariants::UnionTag => execute::op_union_tag,
            InsnVariants::UnionExtract => execute::op_union_extract,
            InsnVariants::RegCopyOffset => execute::op_reg_copy_offset,
            InsnVariants::BlobRead => execute::op_blob_read,
            InsnVariants::BlobWrite => execute::op_blob_write,
            InsnVariants::BlobLen => execute::op_blob_len,
            InsnVariants::ArrayConcat => execute::op_array_concat,
            InsnVariants::ArraySetElement => execute::op_array_set_element,
            InsnVariants::ArraySlice => execute::op_array_slice,
            InsnVariants::MakeRecord => execute::op_make_record,
            InsnVariants::ResultRow => execute::op_result_row,
            InsnVariants::Next => execute::op_next,
            InsnVariants::Prev => execute::op_prev,
            InsnVariants::Halt => execute::op_halt,
            InsnVariants::HaltIfNull => execute::op_halt_if_null,
            InsnVariants::Transaction => execute::op_transaction,
            InsnVariants::AutoCommit => execute::op_auto_commit,
            InsnVariants::Savepoint => execute::op_savepoint,
            InsnVariants::Goto => execute::op_goto,
            InsnVariants::Gosub => execute::op_gosub,
            InsnVariants::Return => execute::op_return,
            InsnVariants::Integer => execute::op_integer,
            InsnVariants::Program => execute::op_program,
            InsnVariants::ResetCount => execute::op_reset_count,
            InsnVariants::ChangeCount => execute::op_change_count,
            InsnVariants::Real => execute::op_real,
            InsnVariants::RealAffinity => execute::op_real_affinity,
            InsnVariants::String8 => execute::op_string8,
            InsnVariants::Blob => execute::op_blob,
            InsnVariants::RowData => execute::op_row_data,
            InsnVariants::RowId => execute::op_row_id,
            InsnVariants::IdxRowId => execute::op_idx_row_id,
            InsnVariants::SeekRowid => execute::op_seek_rowid,
            InsnVariants::DeferredSeek => execute::op_deferred_seek,
            InsnVariants::SeekGE
            | InsnVariants::SeekGT
            | InsnVariants::SeekLE
            | InsnVariants::SeekLT => execute::op_seek,
            InsnVariants::SeekEnd => execute::op_seek_end,
            InsnVariants::IdxGE => execute::op_idx_ge,
            InsnVariants::IdxGT => execute::op_idx_gt,
            InsnVariants::IdxLE => execute::op_idx_le,
            InsnVariants::IdxLT => execute::op_idx_lt,
            InsnVariants::DecrJumpZero => execute::op_decr_jump_zero,
            InsnVariants::AggStep => execute::op_agg_step,
            InsnVariants::AggInverse => execute::op_agg_inverse,
            InsnVariants::AggFinal | InsnVariants::AggValue => execute::op_agg_final,
            InsnVariants::SorterOpen => execute::op_sorter_open,
            InsnVariants::SorterInsert => execute::op_sorter_insert,
            InsnVariants::SorterSort => execute::op_sorter_sort,
            InsnVariants::SorterData => execute::op_sorter_data,
            InsnVariants::SorterNext => execute::op_sorter_next,
            InsnVariants::SorterCompare => execute::op_sorter_compare,
            InsnVariants::RowSetAdd => execute::op_rowset_add,
            InsnVariants::RowSetRead => execute::op_rowset_read,
            InsnVariants::RowSetTest => execute::op_rowset_test,
            InsnVariants::Function => execute::op_function,
            InsnVariants::Cast => execute::op_cast,
            InsnVariants::InitCoroutine => execute::op_init_coroutine,
            InsnVariants::EndCoroutine => execute::op_end_coroutine,
            InsnVariants::Yield => execute::op_yield,
            InsnVariants::Insert => execute::op_insert,
            InsnVariants::Int64 => execute::op_int_64,
            InsnVariants::IdxInsert => execute::op_idx_insert,
            InsnVariants::Delete => execute::op_delete,
            InsnVariants::NewRowid => execute::op_new_rowid,
            InsnVariants::MustBeInt => execute::op_must_be_int,
            InsnVariants::SoftNull => execute::op_soft_null,
            InsnVariants::NoConflict => execute::op_no_conflict,
            InsnVariants::NotExists => execute::op_not_exists,
            InsnVariants::OffsetLimit => execute::op_offset_limit,
            InsnVariants::OpenWrite => execute::op_open_write,
            InsnVariants::Copy => execute::op_copy,
            InsnVariants::CreateBtree => execute::op_create_btree,
            InsnVariants::IndexMethodCreate => execute::op_index_method_create,
            InsnVariants::IndexMethodDestroy => execute::op_index_method_destroy,
            InsnVariants::IndexMethodOptimize => execute::op_index_method_optimize,
            InsnVariants::IndexMethodQuery => execute::op_index_method_query,
            InsnVariants::ClearBtree => execute::op_clear_btree,
            InsnVariants::Destroy => execute::op_destroy,
            InsnVariants::ResetSorter => execute::op_reset_sorter,
            InsnVariants::DropTable => execute::op_drop_table,
            InsnVariants::DropTrigger => execute::op_drop_trigger,
            InsnVariants::DropType => execute::op_drop_type,
            InsnVariants::AddSequence => execute::op_add_sequence,
            InsnVariants::DropSequence => execute::op_drop_sequence,
            InsnVariants::SequenceComputeNext => execute::op_sequence_compute_next,
            InsnVariants::SetSequenceCurrval => execute::op_set_sequence_currval,
            InsnVariants::SequenceTrackAllocation => execute::op_sequence_track_allocation,
            InsnVariants::SequenceRegisterAllocation => execute::op_sequence_register_allocation,
            InsnVariants::SequenceBeginInnerTx => execute::op_sequence_begin_inner_tx,
            InsnVariants::SequenceCommitInnerTx => execute::op_sequence_commit_inner_tx,
            InsnVariants::AddType => execute::op_add_type,
            InsnVariants::DropView => execute::op_drop_view,
            InsnVariants::Close => execute::op_close,
            InsnVariants::IsNull => execute::op_is_null,
            InsnVariants::ParseSchema => execute::op_parse_schema,
            InsnVariants::PopulateMaterializedViews => execute::op_populate_materialized_views,
            InsnVariants::ShiftRight => execute::op_shift_right,
            InsnVariants::ShiftLeft => execute::op_shift_left,
            InsnVariants::AddImm => execute::op_add_imm,
            InsnVariants::Variable => execute::op_variable,
            InsnVariants::ZeroOrNull => execute::op_zero_or_null,
            InsnVariants::Not => execute::op_not,
            InsnVariants::IsTrue => execute::op_is_true,
            InsnVariants::Concat => execute::op_concat,
            InsnVariants::And => execute::op_and,
            InsnVariants::Or => execute::op_or,
            InsnVariants::Noop => execute::op_noop,
            InsnVariants::PageCount => execute::op_page_count,
            InsnVariants::ReadCookie => execute::op_read_cookie,
            InsnVariants::SetCookie => execute::op_set_cookie,
            InsnVariants::OpenEphemeral | InsnVariants::OpenAutoindex => execute::op_open_ephemeral,
            InsnVariants::Once => execute::op_once,
            InsnVariants::ResetOnce => execute::op_reset_once,
            InsnVariants::Found | InsnVariants::NotFound => execute::op_found,
            InsnVariants::Affinity => execute::op_affinity,
            InsnVariants::IdxDelete => execute::op_idx_delete,
            InsnVariants::Count => execute::op_count,
            InsnVariants::IntegrityCk => execute::op_integrity_check,
            InsnVariants::RenameTable => execute::op_rename_table,
            InsnVariants::DropColumn => execute::op_drop_column,
            InsnVariants::AddColumn => execute::op_add_column,
            InsnVariants::AlterColumn => execute::op_alter_column,
            InsnVariants::MaxPgcnt => execute::op_max_pgcnt,
            InsnVariants::JournalMode => execute::op_journal_mode,
            InsnVariants::IfNeg => execute::op_if_neg,
            InsnVariants::Explain => execute::op_noop,
            InsnVariants::OpenDup => execute::op_open_dup,
            InsnVariants::MemMax => execute::op_mem_max,
            InsnVariants::Sequence => execute::op_sequence,
            InsnVariants::SequenceTest => execute::op_sequence_test,
            InsnVariants::FkCounter => execute::op_fk_counter,
            InsnVariants::FkIfZero => execute::op_fk_if_zero,
            InsnVariants::FkCheck => execute::op_fk_check,
            InsnVariants::VBegin => execute::op_vbegin,
            InsnVariants::VRename => execute::op_vrename,
            InsnVariants::FilterAdd => execute::op_filter_add,
            InsnVariants::Filter => execute::op_filter,
            InsnVariants::HashBuild => execute::op_hash_build,
            InsnVariants::HashDistinct => execute::op_hash_distinct,
            InsnVariants::HashBuildFinalize => execute::op_hash_build_finalize,
            InsnVariants::HashProbe => execute::op_hash_probe,
            InsnVariants::HashNext => execute::op_hash_next,
            InsnVariants::HashClose => execute::op_hash_close,
            InsnVariants::HashClear => execute::op_hash_clear,
            InsnVariants::HashMarkMatched => execute::op_hash_mark_matched,
            InsnVariants::HashResetMatched => execute::op_hash_reset_matched,
            InsnVariants::HashScanUnmatched => execute::op_hash_scan_unmatched,
            InsnVariants::HashNextUnmatched => execute::op_hash_next_unmatched,
            InsnVariants::HashGraceInit => execute::op_hash_grace_init,
            InsnVariants::HashGraceLoadPartition => execute::op_hash_grace_load_partition,
            InsnVariants::HashGraceNextProbe => execute::op_hash_grace_next_probe,
            InsnVariants::HashGraceAdvancePartition => execute::op_hash_grace_advance_partition,
            InsnVariants::VacuumInto => execute::op_vacuum_into,
            InsnVariants::Vacuum => execute::op_vacuum,
            InsnVariants::InitCdcVersion => execute::op_init_cdc_version,
        }
    }
}

impl Insn {
    // SAFETY: If the enumeration specifies a primitive representation,
    // then the discriminant may be reliably accessed via unsafe pointer casting
    #[inline(always)]
    pub(crate) const fn discriminant(&self) -> u8 {
        unsafe { *(self as *const Self as *const u8) }
    }

    #[inline(always)]
    pub const fn to_function(&self) -> InsnFunction {
        // dont use this because its still using match
        // InsnVariants::from(self).to_function_fast()
        INSN_VTABLE[self.discriminant() as usize]
    }

    /// Returns true if this opcode cannot directly modify persistent database
    /// contents. This is used to compute PreparedProgram::readonly, mirroring
    /// SQLite's sqlite3_stmt_readonly() classification over compiled bytecode.
    pub fn is_readonly(&self) -> bool {
        match self {
            Self::Checkpoint { .. }
            | Self::VCreate { .. }
            | Self::VUpdate { .. }
            | Self::VDestroy { .. }
            | Self::VRename { .. }
            | Self::Transaction {
                tx_mode: TransactionMode::Write | TransactionMode::Concurrent,
                ..
            }
            | Self::Insert { .. }
            | Self::Delete { .. }
            | Self::IdxDelete { .. }
            | Self::OpenWrite { .. }
            | Self::CreateBtree { .. }
            | Self::IndexMethodCreate { .. }
            | Self::IndexMethodDestroy { .. }
            | Self::IndexMethodOptimize { .. }
            | Self::ClearBtree { .. }
            | Self::Destroy { .. }
            | Self::DropTable { .. }
            | Self::DropView { .. }
            | Self::DropIndex { .. }
            | Self::DropTrigger { .. }
            | Self::DropType { .. }
            | Self::AddSequence { .. }
            | Self::DropSequence { .. }
            | Self::SequenceComputeNext { .. }
            | Self::SetSequenceCurrval { .. }
            | Self::SequenceTrackAllocation { .. }
            | Self::SequenceRegisterAllocation { .. }
            | Self::SequenceBeginInnerTx { .. }
            | Self::SequenceCommitInnerTx { .. }
            | Self::AddType { .. }
            | Self::ParseSchema { .. }
            | Self::PopulateMaterializedViews { .. }
            | Self::SetCookie { .. }
            | Self::RenameTable { .. }
            | Self::DropColumn { .. }
            | Self::AddColumn { .. }
            | Self::AlterColumn { .. }
            | Self::JournalMode { .. }
            | Self::Vacuum { .. } => false,
            Self::MaxPgcnt { new_max, .. } => *new_max == 0,
            // A recursive foreign-key action is treated as writable while it's still being
            // compiled; only fully-prepared subprograms can declare themselves read-only.
            Self::Program { program, .. } => match program {
                Subprogram::PreparedProgram(p) => p.is_readonly(),
                Subprogram::Pending(_) => false,
            },
            _ => true,
        }
    }
}

// TODO: Add remaining cookies.
#[derive(Description, Debug, Clone, Copy)]
pub enum Cookie {
    /// The schema cookie.
    SchemaVersion = 1,
    /// The schema format number. Supported schema formats are 1, 2, 3, and 4.
    DatabaseFormat = 2,
    /// Default page cache size.
    DefaultPageCacheSize = 3,
    /// The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
    LargestRootPageNumber = 4,
    /// The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
    DatabaseTextEncoding = 5,
    /// The "user version" as read and set by the user_version pragma.
    UserVersion = 6,
    /// The auto-vacuum mode setting.
    IncrementalVacuum = 7,
    /// The application ID as set by the application_id pragma.
    ApplicationId = 8,
}

#[cfg(test)]
mod tests {
    use strum::VariantArray;

    #[test]
    fn test_make_sure_correct_insn_table() {
        for variant in super::InsnVariants::VARIANTS {
            let func1 = variant.to_function();
            let func2 = variant.to_function_fast();
            assert_eq!(
                func1 as usize, func2 as usize,
                "Variant {:?} does not match in fast table at index {}",
                variant, *variant as usize
            );
        }
    }

    #[test]
    fn test_insn_size_does_not_grow() {
        // Interpreter dispatch is sensitive to instruction size. Widening a
        // variant past the current largest one silently degrades every query;
        // grow this bound only deliberately.
        assert!(
            std::mem::size_of::<super::Insn>() <= 96,
            "Insn grew to {} bytes",
            std::mem::size_of::<super::Insn>()
        );
    }
}

#[cfg(test)]
mod error_size_tests {
    /// `LimboError` rides in the `Result` of every opcode call and every
    /// cursor operation, so its size is copied around once per executed
    /// instruction. A fat new variant (see the boxed `LexerError`) silently
    /// taxes the whole hot path.
    #[test]
    fn limbo_error_stays_small() {
        assert!(std::mem::size_of::<crate::LimboError>() <= 40);
        // The niche-packed boxed-error result returns in registers; anything
        // past 16 bytes goes back through memory on every executed insn.
        assert!(std::mem::size_of::<super::execute::InsnResult>() <= 16);
    }
}
