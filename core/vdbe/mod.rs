//! The virtual database engine (VDBE).
//!
//! The VDBE is a register-based virtual machine that execute bytecode
//! instructions that represent SQL statements. When an application prepares
//! an SQL statement, the statement is compiled into a sequence of bytecode
//! instructions that perform the needed operations, such as reading or
//! writing to a b-tree, sorting, or aggregating data.
//!
//! The instruction set of the VDBE is similar to SQLite's instruction set,
//! but with the exception that bytecodes that perform I/O operations are
//! return execution back to the caller instead of blocking. This is because
//! Limbo is designed for applications that need high concurrency such as
//! serverless runtimes. In addition, asynchronous I/O makes storage
//! disaggregation easier.
//!
//! You can find a full list of SQLite opcodes at:
//!
//! https://www.sqlite.org/opcode.html

pub mod builder;
pub mod explain;
pub mod sorter;

mod datetime;

use crate::error::{LimboError, SQLITE_CONSTRAINT_PRIMARYKEY};
use crate::function::{AggFunc, FuncCtx, ScalarFunc};
use crate::pseudo::PseudoCursor;
use crate::schema::Table;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::{btree::BTreeCursor, pager::Pager};
use crate::types::{
    AggContext, Cursor, CursorResult, OwnedRecord, OwnedValue, Record, SeekKey, SeekOp,
};
use crate::util::parse_schema_rows;
#[cfg(feature = "json")]
use crate::{function::JsonFunc, json::get_json};
use crate::{Connection, Result, TransactionState};
use crate::{Rows, DATABASE_VERSION};

use datetime::{exec_date, exec_time, exec_unixepoch};

use rand::distributions::{Distribution, Uniform};
use rand::{thread_rng, Rng};
use regex::Regex;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::rc::{Rc, Weak};

pub type BranchOffset = i64;

pub type CursorID = usize;

pub type PageIdx = usize;

#[allow(dead_code)]
#[derive(Debug)]
pub enum Func {
    Scalar(ScalarFunc),
    #[cfg(feature = "json")]
    Json(JsonFunc),
}

impl Display for Func {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Func::Scalar(scalar_func) => scalar_func.to_string(),
            #[cfg(feature = "json")]
            Func::Json(json_func) => json_func.to_string(),
        };
        write!(f, "{}", str)
    }
}

#[derive(Debug)]
pub enum Insn {
    // Initialize the program state and jump to the given PC.
    Init {
        target_pc: BranchOffset,
    },
    // Write a NULL into register dest. If dest_end is Some, then also write NULL into register dest_end and every register in between dest and dest_end. If dest_end is not set, then only register dest is set to NULL.
    Null {
        dest: usize,
        dest_end: Option<usize>,
    },
    // Move the cursor P1 to a null row. Any Column operations that occur while the cursor is on the null row will always write a NULL.
    NullRow {
        cursor_id: CursorID,
    },
    // Add two registers and store the result in a third register.
    Add {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    // Subtract rhs from lhs and store in dest
    Subtract {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    // Multiply two registers and store the result in a third register.
    Multiply {
        lhs: usize,
        rhs: usize,
        dest: usize,
    },
    // Compare two vectors of registers in reg(P1)..reg(P1+P3-1) (call this vector "A") and in reg(P2)..reg(P2+P3-1) ("B"). Save the result of the comparison for use by the next Jump instruct.
    Compare {
        start_reg_a: usize,
        start_reg_b: usize,
        count: usize,
    },
    // Jump to the instruction at address P1, P2, or P3 depending on whether in the most recent Compare instruction the P1 vector was less than, equal to, or greater than the P2 vector, respectively.
    Jump {
        target_pc_lt: BranchOffset,
        target_pc_eq: BranchOffset,
        target_pc_gt: BranchOffset,
    },
    // Move the P3 values in register P1..P1+P3-1 over into registers P2..P2+P3-1. Registers P1..P1+P3-1 are left holding a NULL. It is an error for register ranges P1..P1+P3-1 and P2..P2+P3-1 to overlap. It is an error for P3 to be less than 1.
    Move {
        source_reg: usize,
        dest_reg: usize,
        count: usize,
    },
    // If the given register is a positive integer, decrement it by decrement_by and jump to the given PC.
    IfPos {
        reg: usize,
        target_pc: BranchOffset,
        decrement_by: usize,
    },
    // If the given register is not NULL, jump to the given PC.
    NotNull {
        reg: usize,
        target_pc: BranchOffset,
    },
    // Compare two registers and jump to the given PC if they are equal.
    Eq {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
    },
    // Compare two registers and jump to the given PC if they are not equal.
    Ne {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
    },
    // Compare two registers and jump to the given PC if the left-hand side is less than the right-hand side.
    Lt {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
    },
    // Compare two registers and jump to the given PC if the left-hand side is less than or equal to the right-hand side.
    Le {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
    },
    // Compare two registers and jump to the given PC if the left-hand side is greater than the right-hand side.
    Gt {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
    },
    // Compare two registers and jump to the given PC if the left-hand side is greater than or equal to the right-hand side.
    Ge {
        lhs: usize,
        rhs: usize,
        target_pc: BranchOffset,
    },
    /// Jump to target_pc if r\[reg\] != 0 or (r\[reg\] == NULL && r\[null_reg\] != 0)
    If {
        reg: usize,              // P1
        target_pc: BranchOffset, // P2
        /// P3. If r\[reg\] is null, jump iff r\[null_reg\] != 0
        null_reg: usize,
    },
    /// Jump to target_pc if r\[reg\] != 0 or (r\[reg\] == NULL && r\[null_reg\] != 0)
    IfNot {
        reg: usize,              // P1
        target_pc: BranchOffset, // P2
        /// P3. If r\[reg\] is null, jump iff r\[null_reg\] != 0
        null_reg: usize,
    },
    // Open a cursor for reading.
    OpenReadAsync {
        cursor_id: CursorID,
        root_page: PageIdx,
    },

    // Await for the completion of open cursor.
    OpenReadAwait,

    // Open a cursor for a pseudo-table that contains a single row.
    OpenPseudo {
        cursor_id: CursorID,
        content_reg: usize,
        num_fields: usize,
    },

    // Rewind the cursor to the beginning of the B-Tree.
    RewindAsync {
        cursor_id: CursorID,
    },

    // Await for the completion of cursor rewind.
    RewindAwait {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
    },

    // Read a column from the current row of the cursor.
    Column {
        cursor_id: CursorID,
        column: usize,
        dest: usize,
    },

    // Make a record and write it to destination register.
    MakeRecord {
        start_reg: usize, // P1
        count: usize,     // P2
        dest_reg: usize,  // P3
    },

    // Emit a row of results.
    ResultRow {
        start_reg: usize, // P1
        count: usize,     // P2
    },

    // Advance the cursor to the next row.
    NextAsync {
        cursor_id: CursorID,
    },

    // Await for the completion of cursor advance.
    NextAwait {
        cursor_id: CursorID,
        pc_if_next: BranchOffset,
    },

    // Halt the program.
    Halt {
        err_code: usize,
        description: String,
    },

    // Start a transaction.
    Transaction {
        write: bool,
    },

    // Branch to the given PC.
    Goto {
        target_pc: BranchOffset,
    },

    // Stores the current program counter into register 'return_reg' then jumps to address target_pc.
    Gosub {
        target_pc: BranchOffset,
        return_reg: usize,
    },

    // Returns to the program counter stored in register 'return_reg'.
    Return {
        return_reg: usize,
    },

    // Write an integer value into a register.
    Integer {
        value: i64,
        dest: usize,
    },

    // Write a float value into a register
    Real {
        value: f64,
        dest: usize,
    },

    // If register holds an integer, transform it to a float
    RealAffinity {
        register: usize,
    },

    // Write a string value into a register.
    String8 {
        value: String,
        dest: usize,
    },

    // Write a blob value into a register.
    Blob {
        value: Vec<u8>,
        dest: usize,
    },

    // Read the rowid of the current row.
    RowId {
        cursor_id: CursorID,
        dest: usize,
    },

    // Seek to a rowid in the cursor. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekRowid {
        cursor_id: CursorID,
        src_reg: usize,
        target_pc: BranchOffset,
    },

    // P1 is an open index cursor and P3 is a cursor on the corresponding table. This opcode does a deferred seek of the P3 table cursor to the row that corresponds to the current row of P1.
    // This is a deferred seek. Nothing actually happens until the cursor is used to read a record. That way, if no reads occur, no unnecessary I/O happens.
    DeferredSeek {
        index_cursor_id: CursorID,
        table_cursor_id: CursorID,
    },

    // If cursor_id refers to an SQL table (B-Tree that uses integer keys), use the value in start_reg as the key.
    // If cursor_id refers to an SQL index, then start_reg is the first in an array of num_regs registers that are used as an unpacked index key.
    // Seek to the first index entry that is greater than or equal to the given key. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekGE {
        is_index: bool,
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    // If cursor_id refers to an SQL table (B-Tree that uses integer keys), use the value in start_reg as the key.
    // If cursor_id refers to an SQL index, then start_reg is the first in an array of num_regs registers that are used as an unpacked index key.
    // Seek to the first index entry that is greater than the given key. If not found, jump to the given PC. Otherwise, continue to the next instruction.
    SeekGT {
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

    // The P4 register values beginning with P3 form an unpacked index key that omits the PRIMARY KEY. Compare this key value against the index that P1 is currently pointing to, ignoring the PRIMARY KEY or ROWID fields at the end.
    // If the P1 index entry is greater than the key value then jump to P2. Otherwise fall through to the next instruction.
    IdxGT {
        cursor_id: CursorID,
        start_reg: usize,
        num_regs: usize,
        target_pc: BranchOffset,
    },

    // Decrement the given register and jump to the given PC if the result is zero.
    DecrJumpZero {
        reg: usize,
        target_pc: BranchOffset,
    },

    AggStep {
        acc_reg: usize,
        col: usize,
        delimiter: usize,
        func: AggFunc,
    },

    AggFinal {
        register: usize,
        func: AggFunc,
    },

    // Open a sorter.
    SorterOpen {
        cursor_id: CursorID, // P1
        columns: usize,      // P2
        order: OwnedRecord,  // P4. 0 if ASC and 1 if DESC
    },

    // Insert a row into the sorter.
    SorterInsert {
        cursor_id: CursorID,
        record_reg: usize,
    },

    // Sort the rows in the sorter.
    SorterSort {
        cursor_id: CursorID,
        pc_if_empty: BranchOffset,
    },

    // Retrieve the next row from the sorter.
    SorterData {
        cursor_id: CursorID,  // P1
        dest_reg: usize,      // P2
        pseudo_cursor: usize, // P3
    },

    // Advance to the next row in the sorter.
    SorterNext {
        cursor_id: CursorID,
        pc_if_next: BranchOffset,
    },

    // Function
    Function {
        constant_mask: i32, // P1
        start_reg: usize,   // P2, start of argument registers
        dest: usize,        // P3
        func: FuncCtx,      // P4
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
    },

    InsertAsync {
        cursor: CursorID,
        key_reg: usize,    // Must be int.
        record_reg: usize, // Blob of record data.
        flag: usize,       // Flags used by insert, for now not used.
    },

    InsertAwait {
        cursor_id: usize,
    },

    NewRowid {
        cursor: CursorID,        // P1
        rowid_reg: usize,        // P2  Destination register to store the new rowid
        prev_largest_reg: usize, // P3 Previous largest rowid in the table (Not used for now)
    },

    MustBeInt {
        reg: usize,
    },

    SoftNull {
        reg: usize,
    },

    NotExists {
        cursor: CursorID,
        rowid_reg: usize,
        target_pc: BranchOffset,
    },

    OpenWriteAsync {
        cursor_id: CursorID,
        root_page: PageIdx,
    },

    OpenWriteAwait {},

    Copy {
        src_reg: usize,
        dst_reg: usize,
        amount: usize, // 0 amount means we include src_reg, dst_reg..=dst_reg+amount = src_reg..=src_reg+amount
    },

    /// Allocate a new b-tree.
    CreateBtree {
        /// Allocate b-tree in main database if zero or in temp database if non-zero (P1).
        db: usize,
        /// The root page of the new b-tree (P2).
        root: usize,
        /// Flags (P3).
        flags: usize,
    },

    /// Close a cursor.
    Close {
        cursor_id: CursorID,
    },

    /// Check if the register is null.
    IsNull {
        /// Source register (P1).
        src: usize,

        /// Jump to this PC if the register is null (P2).
        target_pc: BranchOffset,
    },
    ParseSchema {
        db: usize,
        where_clause: String,
    },
}

// Index of insn in list of insns
type InsnReference = usize;

pub enum StepResult<'a> {
    Done,
    IO,
    Row(Record<'a>),
}

struct RegexCache {
    like: HashMap<String, Regex>,
    glob: HashMap<String, Regex>,
}
impl RegexCache {
    fn new() -> Self {
        RegexCache {
            like: HashMap::new(),
            glob: HashMap::new(),
        }
    }
}

/// The program state describes the environment in which the program executes.
pub struct ProgramState {
    pub pc: BranchOffset,
    cursors: RefCell<BTreeMap<CursorID, Box<dyn Cursor>>>,
    registers: Vec<OwnedValue>,
    last_compare: Option<std::cmp::Ordering>,
    deferred_seek: Option<(CursorID, CursorID)>,
    ended_coroutine: bool, // flag to notify yield coroutine finished
    regex_cache: RegexCache,
}

impl ProgramState {
    pub fn new(max_registers: usize) -> Self {
        let cursors = RefCell::new(BTreeMap::new());
        let mut registers = Vec::with_capacity(max_registers);
        registers.resize(max_registers, OwnedValue::Null);
        Self {
            pc: 0,
            cursors,
            registers,
            last_compare: None,
            deferred_seek: None,
            ended_coroutine: false,
            regex_cache: RegexCache::new(),
        }
    }

    pub fn column_count(&self) -> usize {
        self.registers.len()
    }

    pub fn column(&self, i: usize) -> Option<String> {
        Some(format!("{:?}", self.registers[i]))
    }
}

pub struct Program {
    pub max_registers: usize,
    pub insns: Vec<Insn>,
    pub cursor_ref: Vec<(Option<String>, Option<Table>)>,
    pub database_header: Rc<RefCell<DatabaseHeader>>,
    pub comments: HashMap<BranchOffset, &'static str>,
    pub connection: Weak<Connection>,
    pub auto_commit: bool,
}

impl Program {
    pub fn explain(&self) {
        println!("addr  opcode             p1    p2    p3    p4             p5  comment");
        println!("----  -----------------  ----  ----  ----  -------------  --  -------");
        let mut indent_count: usize = 0;
        let indent = "  ";
        let mut prev_insn: Option<&Insn> = None;
        for (addr, insn) in self.insns.iter().enumerate() {
            indent_count = get_indent_count(indent_count, insn, prev_insn);
            print_insn(
                self,
                addr as InsnReference,
                insn,
                indent.repeat(indent_count),
            );
            prev_insn = Some(insn);
        }
    }

    pub fn step<'a>(
        &self,
        state: &'a mut ProgramState,
        pager: Rc<Pager>,
    ) -> Result<StepResult<'a>> {
        loop {
            let insn = &self.insns[state.pc as usize];
            trace_insn(self, state.pc as InsnReference, insn);
            let mut cursors = state.cursors.borrow_mut();
            match insn {
                Insn::Init { target_pc } => {
                    assert!(*target_pc >= 0);
                    state.pc = *target_pc;
                }
                Insn::Add { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Integer(lhs + rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs + rhs);
                        }
                        (OwnedValue::Float(f), OwnedValue::Integer(i))
                        | (OwnedValue::Integer(i), OwnedValue::Float(f)) => {
                            state.registers[dest] = OwnedValue::Float(*f + *i as f64);
                        }
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Agg(aggctx), other) | (other, OwnedValue::Agg(aggctx)) => {
                            match other {
                                OwnedValue::Null => {
                                    state.registers[dest] = OwnedValue::Null;
                                }
                                OwnedValue::Integer(i) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc + *i as f64);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Integer(acc + i);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                OwnedValue::Float(f) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc + f);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Float(*acc as f64 + f);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                OwnedValue::Agg(aggctx2) => {
                                    let acc = aggctx.final_value();
                                    let acc2 = aggctx2.final_value();
                                    match (acc, acc2) {
                                        (OwnedValue::Integer(acc), OwnedValue::Integer(acc2)) => {
                                            state.registers[dest] = OwnedValue::Integer(acc + acc2);
                                        }
                                        (OwnedValue::Float(acc), OwnedValue::Float(acc2)) => {
                                            state.registers[dest] = OwnedValue::Float(acc + acc2);
                                        }
                                        (OwnedValue::Integer(acc), OwnedValue::Float(acc2)) => {
                                            state.registers[dest] =
                                                OwnedValue::Float(*acc as f64 + acc2);
                                        }
                                        (OwnedValue::Float(acc), OwnedValue::Integer(acc2)) => {
                                            state.registers[dest] =
                                                OwnedValue::Float(acc + *acc2 as f64);
                                        }
                                        _ => {
                                            todo!("{:?} {:?}", acc, acc2);
                                        }
                                    }
                                }
                                rest => unimplemented!("{:?}", rest),
                            }
                        }
                        _ => {
                            todo!();
                        }
                    }
                    state.pc += 1;
                }
                Insn::Subtract { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Integer(lhs - rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs - rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Integer(rhs))
                        | (OwnedValue::Integer(rhs), OwnedValue::Float(lhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs - *rhs as f64);
                        }
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Agg(aggctx), other) | (other, OwnedValue::Agg(aggctx)) => {
                            match other {
                                OwnedValue::Null => {
                                    state.registers[dest] = OwnedValue::Null;
                                }
                                OwnedValue::Integer(i) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc - *i as f64);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Integer(acc - i);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                OwnedValue::Float(f) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc - f);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Float(*acc as f64 - f);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                rest => unimplemented!("{:?}", rest),
                            }
                        }
                        others => {
                            todo!("{:?}", others);
                        }
                    }
                    state.pc += 1;
                }
                Insn::Multiply { lhs, rhs, dest } => {
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let dest = *dest;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (OwnedValue::Integer(lhs), OwnedValue::Integer(rhs)) => {
                            state.registers[dest] = OwnedValue::Integer(lhs * rhs);
                        }
                        (OwnedValue::Float(lhs), OwnedValue::Float(rhs)) => {
                            state.registers[dest] = OwnedValue::Float(lhs * rhs);
                        }
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        (OwnedValue::Agg(aggctx), other) | (other, OwnedValue::Agg(aggctx)) => {
                            match other {
                                OwnedValue::Null => {
                                    state.registers[dest] = OwnedValue::Null;
                                }
                                OwnedValue::Integer(i) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc * *i as f64);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Integer(acc * i);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                OwnedValue::Float(f) => match aggctx.final_value() {
                                    OwnedValue::Float(acc) => {
                                        state.registers[dest] = OwnedValue::Float(acc * f);
                                    }
                                    OwnedValue::Integer(acc) => {
                                        state.registers[dest] = OwnedValue::Float(*acc as f64 * f);
                                    }
                                    _ => {
                                        todo!("{:?}", aggctx);
                                    }
                                },
                                rest => unimplemented!("{:?}", rest),
                            }
                        }
                        others => {
                            todo!("{:?}", others);
                        }
                    }
                    state.pc += 1;
                }
                Insn::Null { dest, dest_end } => {
                    if let Some(dest_end) = dest_end {
                        for i in *dest..=*dest_end {
                            state.registers[i] = OwnedValue::Null;
                        }
                    } else {
                        state.registers[*dest] = OwnedValue::Null;
                    }
                    state.pc += 1;
                }
                Insn::NullRow { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.set_null_flag(true);
                    state.pc += 1;
                }
                Insn::Compare {
                    start_reg_a,
                    start_reg_b,
                    count,
                } => {
                    let start_reg_a = *start_reg_a;
                    let start_reg_b = *start_reg_b;
                    let count = *count;

                    if start_reg_a + count > start_reg_b {
                        return Err(LimboError::InternalError(
                            "Compare registers overlap".to_string(),
                        ));
                    }

                    let mut cmp = None;
                    for i in 0..count {
                        let a = &state.registers[start_reg_a + i];
                        let b = &state.registers[start_reg_b + i];
                        cmp = Some(a.cmp(b));
                        if cmp != Some(std::cmp::Ordering::Equal) {
                            break;
                        }
                    }
                    state.last_compare = cmp;
                    state.pc += 1;
                }
                Insn::Jump {
                    target_pc_lt,
                    target_pc_eq,
                    target_pc_gt,
                } => {
                    let cmp = state.last_compare.take();
                    if cmp.is_none() {
                        return Err(LimboError::InternalError(
                            "Jump without compare".to_string(),
                        ));
                    }
                    let target_pc = match cmp.unwrap() {
                        std::cmp::Ordering::Less => *target_pc_lt,
                        std::cmp::Ordering::Equal => *target_pc_eq,
                        std::cmp::Ordering::Greater => *target_pc_gt,
                    };
                    assert!(target_pc >= 0);
                    state.pc = target_pc;
                }
                Insn::Move {
                    source_reg,
                    dest_reg,
                    count,
                } => {
                    let source_reg = *source_reg;
                    let dest_reg = *dest_reg;
                    let count = *count;
                    for i in 0..count {
                        state.registers[dest_reg + i] = std::mem::replace(
                            &mut state.registers[source_reg + i],
                            OwnedValue::Null,
                        );
                    }
                    state.pc += 1;
                }
                Insn::IfPos {
                    reg,
                    target_pc,
                    decrement_by,
                } => {
                    assert!(*target_pc >= 0);
                    let reg = *reg;
                    let target_pc = *target_pc;
                    match &state.registers[reg] {
                        OwnedValue::Integer(n) if *n > 0 => {
                            state.pc = target_pc;
                            state.registers[reg] = OwnedValue::Integer(*n - *decrement_by as i64);
                        }
                        OwnedValue::Integer(_) => {
                            state.pc += 1;
                        }
                        _ => {
                            return Err(LimboError::InternalError(
                                "IfPos: the value in the register is not an integer".into(),
                            ));
                        }
                    }
                }
                Insn::NotNull { reg, target_pc } => {
                    assert!(*target_pc >= 0);
                    let reg = *reg;
                    let target_pc = *target_pc;
                    match &state.registers[reg] {
                        OwnedValue::Null => {
                            state.pc += 1;
                        }
                        _ => {
                            state.pc = target_pc;
                        }
                    }
                }

                Insn::Eq {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] == state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Ne {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] != state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Lt {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] < state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Le {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] <= state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Gt {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] > state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::Ge {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc;
                        }
                        _ => {
                            if state.registers[lhs] >= state.registers[rhs] {
                                state.pc = target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                    }
                }
                Insn::If {
                    reg,
                    target_pc,
                    null_reg,
                } => {
                    assert!(*target_pc >= 0);
                    if exec_if(&state.registers[*reg], &state.registers[*null_reg], false) {
                        state.pc = *target_pc;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::IfNot {
                    reg,
                    target_pc,
                    null_reg,
                } => {
                    assert!(*target_pc >= 0);
                    if exec_if(&state.registers[*reg], &state.registers[*null_reg], true) {
                        state.pc = *target_pc;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::OpenReadAsync {
                    cursor_id,
                    root_page,
                } => {
                    let cursor = Box::new(BTreeCursor::new(
                        pager.clone(),
                        *root_page,
                        self.database_header.clone(),
                    ));
                    cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::OpenReadAwait => {
                    state.pc += 1;
                }
                Insn::OpenPseudo {
                    cursor_id,
                    content_reg: _,
                    num_fields: _,
                } => {
                    let cursor = Box::new(PseudoCursor::new());
                    cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::RewindAsync { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    match cursor.rewind()? {
                        CursorResult::Ok(()) => {}
                        CursorResult::IO => {
                            // If there is I/O, the instruction is restarted.
                            return Ok(StepResult::IO);
                        }
                    }
                    state.pc += 1;
                }
                Insn::RewindAwait {
                    cursor_id,
                    pc_if_empty,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    if cursor.is_empty() {
                        state.pc = *pc_if_empty;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::Column {
                    cursor_id,
                    column,
                    dest,
                } => {
                    if let Some((index_cursor_id, table_cursor_id)) = state.deferred_seek.take() {
                        let index_cursor = cursors.get_mut(&index_cursor_id).unwrap();
                        let rowid = index_cursor.rowid()?;
                        let table_cursor = cursors.get_mut(&table_cursor_id).unwrap();
                        match table_cursor.seek(SeekKey::TableRowId(rowid.unwrap()), SeekOp::EQ)? {
                            CursorResult::Ok(_) => {}
                            CursorResult::IO => {
                                state.deferred_seek = Some((index_cursor_id, table_cursor_id));
                                return Ok(StepResult::IO);
                            }
                        }
                    }

                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    if let Some(ref record) = *cursor.record()? {
                        let null_flag = cursor.get_null_flag();
                        state.registers[*dest] = if null_flag {
                            OwnedValue::Null
                        } else {
                            record.values[*column].clone()
                        };
                    } else {
                        state.registers[*dest] = OwnedValue::Null;
                    }
                    state.pc += 1;
                }
                Insn::MakeRecord {
                    start_reg,
                    count,
                    dest_reg,
                } => {
                    let record = make_owned_record(&state.registers, start_reg, count);
                    state.registers[*dest_reg] = OwnedValue::Record(record);
                    state.pc += 1;
                }
                Insn::ResultRow { start_reg, count } => {
                    let record = make_record(&state.registers, start_reg, count);
                    state.pc += 1;
                    return Ok(StepResult::Row(record));
                }
                Insn::NextAsync { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.set_null_flag(false);
                    match cursor.next()? {
                        CursorResult::Ok(_) => {}
                        CursorResult::IO => {
                            // If there is I/O, the instruction is restarted.
                            return Ok(StepResult::IO);
                        }
                    }
                    state.pc += 1;
                }
                Insn::NextAwait {
                    cursor_id,
                    pc_if_next,
                } => {
                    assert!(*pc_if_next >= 0);
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    if !cursor.is_empty() {
                        state.pc = *pc_if_next;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::Halt {
                    err_code,
                    description,
                } => {
                    match *err_code {
                        0 => {}
                        SQLITE_CONSTRAINT_PRIMARYKEY => {
                            return Err(LimboError::Constraint(format!(
                                "UNIQUE constraint failed: {} (19)",
                                description
                            )));
                        }
                        _ => {
                            return Err(LimboError::Constraint(format!(
                                "undocumented halt error code {}",
                                description
                            )));
                        }
                    }
                    log::trace!("Halt auto_commit {}", self.auto_commit);
                    if self.auto_commit {
                        return match pager.end_tx() {
                            Ok(crate::storage::wal::CheckpointStatus::IO) => Ok(StepResult::IO),
                            Ok(crate::storage::wal::CheckpointStatus::Done) => Ok(StepResult::Done),
                            Err(e) => Err(e),
                        };
                    } else {
                        return Ok(StepResult::Done);
                    }
                }
                Insn::Transaction { write } => {
                    let connection = self.connection.upgrade().unwrap();
                    if let Some(db) = connection.db.upgrade() {
                        // TODO(pere): are backpointers good ?? this looks ugly af
                        // upgrade transaction if needed
                        let new_transaction_state =
                            match (db.transaction_state.borrow().clone(), write) {
                                (crate::TransactionState::Write, true) => TransactionState::Write,
                                (crate::TransactionState::Write, false) => TransactionState::Write,
                                (crate::TransactionState::Read, true) => TransactionState::Write,
                                (crate::TransactionState::Read, false) => TransactionState::Read,
                                (crate::TransactionState::None, true) => TransactionState::Read,
                                (crate::TransactionState::None, false) => TransactionState::Read,
                            };
                        // TODO(Pere):
                        //  1. lock wal
                        //  2. lock shared
                        //  3. lock write db if write
                        db.transaction_state.replace(new_transaction_state.clone());
                        if matches!(new_transaction_state, TransactionState::Write) {
                            pager.begin_read_tx()?;
                        } else {
                            pager.begin_write_tx()?;
                        }
                    }
                    state.pc += 1;
                }
                Insn::Goto { target_pc } => {
                    assert!(*target_pc >= 0);
                    state.pc = *target_pc;
                }
                Insn::Gosub {
                    target_pc,
                    return_reg,
                } => {
                    assert!(*target_pc >= 0);
                    state.registers[*return_reg] = OwnedValue::Integer(state.pc + 1);
                    state.pc = *target_pc;
                }
                Insn::Return { return_reg } => {
                    if let OwnedValue::Integer(pc) = state.registers[*return_reg] {
                        if pc < 0 {
                            return Err(LimboError::InternalError(
                                "Return register is negative".to_string(),
                            ));
                        }
                        state.pc = pc;
                    } else {
                        return Err(LimboError::InternalError(
                            "Return register is not an integer".to_string(),
                        ));
                    }
                }
                Insn::Integer { value, dest } => {
                    state.registers[*dest] = OwnedValue::Integer(*value);
                    state.pc += 1;
                }
                Insn::Real { value, dest } => {
                    state.registers[*dest] = OwnedValue::Float(*value);
                    state.pc += 1;
                }
                Insn::RealAffinity { register } => {
                    if let OwnedValue::Integer(i) = &state.registers[*register] {
                        state.registers[*register] = OwnedValue::Float(*i as f64);
                    };
                    state.pc += 1;
                }
                Insn::String8 { value, dest } => {
                    state.registers[*dest] = OwnedValue::Text(Rc::new(value.into()));
                    state.pc += 1;
                }
                Insn::Blob { value, dest } => {
                    state.registers[*dest] = OwnedValue::Blob(Rc::new(value.clone()));
                    state.pc += 1;
                }
                Insn::RowId { cursor_id, dest } => {
                    if let Some((index_cursor_id, table_cursor_id)) = state.deferred_seek.take() {
                        let index_cursor = cursors.get_mut(&index_cursor_id).unwrap();
                        let rowid = index_cursor.rowid()?;
                        let table_cursor = cursors.get_mut(&table_cursor_id).unwrap();
                        match table_cursor.seek(SeekKey::TableRowId(rowid.unwrap()), SeekOp::EQ)? {
                            CursorResult::Ok(_) => {}
                            CursorResult::IO => {
                                state.deferred_seek = Some((index_cursor_id, table_cursor_id));
                                return Ok(StepResult::IO);
                            }
                        }
                    }

                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    if let Some(ref rowid) = cursor.rowid()? {
                        state.registers[*dest] = OwnedValue::Integer(*rowid as i64);
                    } else {
                        state.registers[*dest] = OwnedValue::Null;
                    }
                    state.pc += 1;
                }
                Insn::SeekRowid {
                    cursor_id,
                    src_reg,
                    target_pc,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let rowid = match &state.registers[*src_reg] {
                        OwnedValue::Integer(rowid) => *rowid as u64,
                        OwnedValue::Null => {
                            state.pc = *target_pc;
                            continue;
                        }
                        other => {
                            return Err(LimboError::InternalError(
                                format!("SeekRowid: the value in the register is not an integer or NULL: {}", other)
                            ));
                        }
                    };
                    match cursor.seek(SeekKey::TableRowId(rowid), SeekOp::EQ)? {
                        CursorResult::Ok(found) => {
                            if !found {
                                state.pc = *target_pc;
                            } else {
                                state.pc += 1;
                            }
                        }
                        CursorResult::IO => {
                            // If there is I/O, the instruction is restarted.
                            return Ok(StepResult::IO);
                        }
                    }
                }
                Insn::DeferredSeek {
                    index_cursor_id,
                    table_cursor_id,
                } => {
                    state.deferred_seek = Some((*index_cursor_id, *table_cursor_id));
                    state.pc += 1;
                }
                Insn::SeekGE {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                    is_index,
                } => {
                    if *is_index {
                        let cursor = cursors.get_mut(cursor_id).unwrap();
                        let record_from_regs: OwnedRecord =
                            make_owned_record(&state.registers, start_reg, num_regs);
                        match cursor.seek(SeekKey::IndexKey(&record_from_regs), SeekOp::GE)? {
                            CursorResult::Ok(found) => {
                                if !found {
                                    state.pc = *target_pc;
                                } else {
                                    state.pc += 1;
                                }
                            }
                            CursorResult::IO => {
                                // If there is I/O, the instruction is restarted.
                                return Ok(StepResult::IO);
                            }
                        }
                    } else {
                        let cursor = cursors.get_mut(cursor_id).unwrap();
                        let rowid = match &state.registers[*start_reg] {
                            OwnedValue::Null => {
                                // All integer values are greater than null so we just rewind the cursor
                                match cursor.rewind()? {
                                    CursorResult::Ok(()) => {}
                                    CursorResult::IO => {
                                        // If there is I/O, the instruction is restarted.
                                        return Ok(StepResult::IO);
                                    }
                                }
                                state.pc += 1;
                                continue;
                            }
                            OwnedValue::Integer(rowid) => *rowid as u64,
                            _ => {
                                return Err(LimboError::InternalError(
                                    "SeekGE: the value in the register is not an integer".into(),
                                ));
                            }
                        };
                        match cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GE)? {
                            CursorResult::Ok(found) => {
                                if !found {
                                    state.pc = *target_pc;
                                } else {
                                    state.pc += 1;
                                }
                            }
                            CursorResult::IO => {
                                // If there is I/O, the instruction is restarted.
                                return Ok(StepResult::IO);
                            }
                        }
                    }
                }
                Insn::SeekGT {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                    is_index,
                } => {
                    if *is_index {
                        let cursor = cursors.get_mut(cursor_id).unwrap();
                        let record_from_regs: OwnedRecord =
                            make_owned_record(&state.registers, start_reg, num_regs);
                        match cursor.seek(SeekKey::IndexKey(&record_from_regs), SeekOp::GT)? {
                            CursorResult::Ok(found) => {
                                if !found {
                                    state.pc = *target_pc;
                                } else {
                                    state.pc += 1;
                                }
                            }
                            CursorResult::IO => {
                                // If there is I/O, the instruction is restarted.
                                return Ok(StepResult::IO);
                            }
                        }
                    } else {
                        let cursor = cursors.get_mut(cursor_id).unwrap();
                        let rowid = match &state.registers[*start_reg] {
                            OwnedValue::Null => {
                                // All integer values are greater than null so we just rewind the cursor
                                match cursor.rewind()? {
                                    CursorResult::Ok(()) => {}
                                    CursorResult::IO => {
                                        // If there is I/O, the instruction is restarted.
                                        return Ok(StepResult::IO);
                                    }
                                }
                                state.pc += 1;
                                continue;
                            }
                            OwnedValue::Integer(rowid) => *rowid as u64,
                            _ => {
                                return Err(LimboError::InternalError(
                                    "SeekGT: the value in the register is not an integer".into(),
                                ));
                            }
                        };
                        match cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GT)? {
                            CursorResult::Ok(found) => {
                                if !found {
                                    state.pc = *target_pc;
                                } else {
                                    state.pc += 1;
                                }
                            }
                            CursorResult::IO => {
                                // If there is I/O, the instruction is restarted.
                                return Ok(StepResult::IO);
                            }
                        }
                    }
                }
                Insn::IdxGE {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                } => {
                    assert!(*target_pc >= 0);
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let record_from_regs: OwnedRecord =
                        make_owned_record(&state.registers, start_reg, num_regs);
                    if let Some(ref idx_record) = *cursor.record()? {
                        // omit the rowid from the idx_record, which is the last value
                        if idx_record.values[..idx_record.values.len() - 1]
                            >= *record_from_regs.values
                        {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        state.pc = *target_pc;
                    }
                }
                Insn::IdxGT {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let record_from_regs: OwnedRecord =
                        make_owned_record(&state.registers, start_reg, num_regs);
                    if let Some(ref idx_record) = *cursor.record()? {
                        // omit the rowid from the idx_record, which is the last value
                        if idx_record.values[..idx_record.values.len() - 1]
                            > *record_from_regs.values
                        {
                            state.pc = *target_pc;
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        state.pc = *target_pc;
                    }
                }
                Insn::DecrJumpZero { reg, target_pc } => {
                    assert!(*target_pc >= 0);
                    match state.registers[*reg] {
                        OwnedValue::Integer(n) => {
                            let n = n - 1;
                            if n == 0 {
                                state.pc = *target_pc;
                            } else {
                                state.registers[*reg] = OwnedValue::Integer(n);
                                state.pc += 1;
                            }
                        }
                        _ => unreachable!("DecrJumpZero on non-integer register"),
                    }
                }
                Insn::AggStep {
                    acc_reg,
                    col,
                    delimiter,
                    func,
                } => {
                    if let OwnedValue::Null = &state.registers[*acc_reg] {
                        state.registers[*acc_reg] = match func {
                            AggFunc::Avg => OwnedValue::Agg(Box::new(AggContext::Avg(
                                OwnedValue::Float(0.0),
                                OwnedValue::Integer(0),
                            ))),
                            AggFunc::Sum => {
                                OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Null)))
                            }
                            AggFunc::Total => {
                                // The result of total() is always a floating point value.
                                // No overflow error is ever raised if any prior input was a floating point value.
                                // Total() never throws an integer overflow.
                                OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Float(0.0))))
                            }
                            AggFunc::Count => {
                                OwnedValue::Agg(Box::new(AggContext::Count(OwnedValue::Integer(0))))
                            }
                            AggFunc::Max => {
                                let col = state.registers[*col].clone();
                                match col {
                                    OwnedValue::Integer(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Max(None)))
                                    }
                                    OwnedValue::Float(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Max(None)))
                                    }
                                    OwnedValue::Text(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Max(None)))
                                    }
                                    _ => {
                                        unreachable!();
                                    }
                                }
                            }
                            AggFunc::Min => {
                                let col = state.registers[*col].clone();
                                match col {
                                    OwnedValue::Integer(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Min(None)))
                                    }
                                    OwnedValue::Float(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Min(None)))
                                    }
                                    OwnedValue::Text(_) => {
                                        OwnedValue::Agg(Box::new(AggContext::Min(None)))
                                    }
                                    _ => {
                                        unreachable!();
                                    }
                                }
                            }
                            AggFunc::GroupConcat | AggFunc::StringAgg => OwnedValue::Agg(Box::new(
                                AggContext::GroupConcat(OwnedValue::Text(Rc::new("".to_string()))),
                            )),
                        };
                    }
                    match func {
                        AggFunc::Avg => {
                            let col = state.registers[*col].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Avg(acc, count) = agg.borrow_mut() else {
                                unreachable!();
                            };
                            *acc += col;
                            *count += 1;
                        }
                        AggFunc::Sum | AggFunc::Total => {
                            let col = state.registers[*col].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Sum(acc) = agg.borrow_mut() else {
                                unreachable!();
                            };
                            *acc += col;
                        }
                        AggFunc::Count => {
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Count(count) = agg.borrow_mut() else {
                                unreachable!();
                            };
                            *count += 1;
                        }
                        AggFunc::Max => {
                            let col = state.registers[*col].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Max(acc) = agg.borrow_mut() else {
                                unreachable!();
                            };

                            match (acc.as_mut(), col) {
                                (None, value) => {
                                    *acc = Some(value);
                                }
                                (
                                    Some(OwnedValue::Integer(ref mut current_max)),
                                    OwnedValue::Integer(value),
                                ) => {
                                    if value > *current_max {
                                        *current_max = value;
                                    }
                                }
                                (
                                    Some(OwnedValue::Float(ref mut current_max)),
                                    OwnedValue::Float(value),
                                ) => {
                                    if value > *current_max {
                                        *current_max = value;
                                    }
                                }
                                (
                                    Some(OwnedValue::Text(ref mut current_max)),
                                    OwnedValue::Text(value),
                                ) => {
                                    if value > *current_max {
                                        *current_max = value;
                                    }
                                }
                                _ => {
                                    eprintln!("Unexpected types in max aggregation");
                                }
                            }
                        }
                        AggFunc::Min => {
                            let col = state.registers[*col].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Min(acc) = agg.borrow_mut() else {
                                unreachable!();
                            };

                            match (acc.as_mut(), col) {
                                (None, value) => {
                                    *acc.borrow_mut() = Some(value);
                                }
                                (
                                    Some(OwnedValue::Integer(ref mut current_min)),
                                    OwnedValue::Integer(value),
                                ) => {
                                    if value < *current_min {
                                        *current_min = value;
                                    }
                                }
                                (
                                    Some(OwnedValue::Float(ref mut current_min)),
                                    OwnedValue::Float(value),
                                ) => {
                                    if value < *current_min {
                                        *current_min = value;
                                    }
                                }
                                (
                                    Some(OwnedValue::Text(ref mut current_min)),
                                    OwnedValue::Text(value),
                                ) => {
                                    if value < *current_min {
                                        *current_min = value;
                                    }
                                }
                                _ => {
                                    eprintln!("Unexpected types in min aggregation");
                                }
                            }
                        }
                        AggFunc::GroupConcat | AggFunc::StringAgg => {
                            let col = state.registers[*col].clone();
                            let delimiter = state.registers[*delimiter].clone();
                            let OwnedValue::Agg(agg) = state.registers[*acc_reg].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::GroupConcat(acc) = agg.borrow_mut() else {
                                unreachable!();
                            };
                            if acc.to_string().is_empty() {
                                *acc = col;
                            } else {
                                *acc += delimiter;
                                *acc += col;
                            }
                        }
                    };
                    state.pc += 1;
                }
                Insn::AggFinal { register, func } => {
                    match state.registers[*register].borrow_mut() {
                        OwnedValue::Agg(agg) => {
                            match func {
                                AggFunc::Avg => {
                                    let AggContext::Avg(acc, count) = agg.borrow_mut() else {
                                        unreachable!();
                                    };
                                    *acc /= count.clone();
                                }
                                AggFunc::Sum | AggFunc::Total => {}
                                AggFunc::Count => {}
                                AggFunc::Max => {}
                                AggFunc::Min => {}
                                AggFunc::GroupConcat | AggFunc::StringAgg => {}
                            };
                        }
                        OwnedValue::Null => {
                            // when the set is empty
                            match func {
                                AggFunc::Total => {
                                    state.registers[*register] = OwnedValue::Float(0.0);
                                }
                                AggFunc::Count => {
                                    state.registers[*register] = OwnedValue::Integer(0);
                                }
                                _ => {}
                            }
                        }
                        _ => {
                            unreachable!();
                        }
                    };
                    state.pc += 1;
                }
                Insn::SorterOpen {
                    cursor_id,
                    columns: _,
                    order,
                } => {
                    let order = order
                        .values
                        .iter()
                        .map(|v| match v {
                            OwnedValue::Integer(i) => *i == 0,
                            _ => unreachable!(),
                        })
                        .collect();
                    let cursor = Box::new(sorter::Sorter::new(order));
                    cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::SorterData {
                    cursor_id,
                    dest_reg,
                    pseudo_cursor: sorter_cursor,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let record = match *cursor.record()? {
                        Some(ref record) => record.clone(),
                        None => {
                            state.pc += 1;
                            continue;
                        }
                    };
                    state.registers[*dest_reg] = OwnedValue::Record(record.clone());
                    let sorter_cursor = cursors.get_mut(sorter_cursor).unwrap();
                    sorter_cursor.insert(&OwnedValue::Integer(0), &record, false)?; // fix key later
                    state.pc += 1;
                }
                Insn::SorterInsert {
                    cursor_id,
                    record_reg,
                } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    let record = match &state.registers[*record_reg] {
                        OwnedValue::Record(record) => record,
                        _ => unreachable!("SorterInsert on non-record register"),
                    };
                    // TODO: set correct key
                    cursor.insert(&OwnedValue::Integer(0), record, false)?;
                    state.pc += 1;
                }
                Insn::SorterSort {
                    cursor_id,
                    pc_if_empty,
                } => {
                    if let Some(cursor) = cursors.get_mut(cursor_id) {
                        cursor.rewind()?;
                        state.pc += 1;
                    } else {
                        state.pc = *pc_if_empty;
                    }
                }
                Insn::SorterNext {
                    cursor_id,
                    pc_if_next,
                } => {
                    assert!(*pc_if_next >= 0);
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    match cursor.next()? {
                        CursorResult::Ok(_) => {}
                        CursorResult::IO => {
                            // If there is I/O, the instruction is restarted.
                            return Ok(StepResult::IO);
                        }
                    }
                    if !cursor.is_empty() {
                        state.pc = *pc_if_next;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::Function {
                    constant_mask,
                    func,
                    start_reg,
                    dest,
                } => {
                    let arg_count = func.arg_count;
                    match &func.func {
                        #[cfg(feature = "json")]
                        crate::function::Func::Json(JsonFunc::Json) => {
                            let json_value = &state.registers[*start_reg];
                            let json_str = get_json(json_value);
                            match json_str {
                                Ok(json) => state.registers[*dest] = json,
                                Err(e) => return Err(e),
                            }
                        }
                        crate::function::Func::Scalar(scalar_func) => match scalar_func {
                            ScalarFunc::Cast => {
                                assert!(arg_count == 2);
                                assert!(*start_reg + 1 < state.registers.len());
                                let reg_value_argument = state.registers[*start_reg].clone();
                                let OwnedValue::Text(reg_value_type) =
                                    state.registers[*start_reg + 1].clone()
                                else {
                                    unreachable!("Cast with non-text type");
                                };
                                let result = exec_cast(&reg_value_argument, &reg_value_type);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Char => {
                                let reg_values =
                                    state.registers[*start_reg..*start_reg + arg_count].to_vec();
                                state.registers[*dest] = exec_char(reg_values);
                            }
                            ScalarFunc::Coalesce => {}
                            ScalarFunc::Concat => {
                                let result = exec_concat(
                                    &state.registers[*start_reg..*start_reg + arg_count],
                                );
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::ConcatWs => {
                                let result = exec_concat_ws(
                                    &state.registers[*start_reg..*start_reg + arg_count],
                                );
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Glob => {
                                let pattern = &state.registers[*start_reg];
                                let text = &state.registers[*start_reg + 1];
                                let result = match (pattern, text) {
                                    (OwnedValue::Text(pattern), OwnedValue::Text(text)) => {
                                        let cache = if *constant_mask > 0 {
                                            Some(&mut state.regex_cache.glob)
                                        } else {
                                            None
                                        };
                                        OwnedValue::Integer(exec_glob(cache, pattern, text) as i64)
                                    }
                                    _ => {
                                        unreachable!("Like on non-text registers");
                                    }
                                };
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::IfNull => {}
                            ScalarFunc::Instr => {
                                let reg_value = &state.registers[*start_reg];
                                let pattern_value = &state.registers[*start_reg + 1];
                                let result = exec_instr(reg_value, pattern_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Like => {
                                let pattern = &state.registers[*start_reg];
                                let text = &state.registers[*start_reg + 1];
                                let result = match (pattern, text) {
                                    (OwnedValue::Text(pattern), OwnedValue::Text(text)) => {
                                        let cache = if *constant_mask > 0 {
                                            Some(&mut state.regex_cache.like)
                                        } else {
                                            None
                                        };
                                        OwnedValue::Integer(exec_like(cache, pattern, text) as i64)
                                    }
                                    _ => {
                                        unreachable!("Like on non-text registers");
                                    }
                                };
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Abs
                            | ScalarFunc::Lower
                            | ScalarFunc::Upper
                            | ScalarFunc::Length
                            | ScalarFunc::Typeof
                            | ScalarFunc::Unicode
                            | ScalarFunc::Quote
                            | ScalarFunc::RandomBlob
                            | ScalarFunc::Sign
                            | ScalarFunc::ZeroBlob => {
                                let reg_value = state.registers[*start_reg].borrow_mut();
                                let result = match scalar_func {
                                    ScalarFunc::Sign => exec_sign(reg_value),
                                    ScalarFunc::Abs => exec_abs(reg_value),
                                    ScalarFunc::Lower => exec_lower(reg_value),
                                    ScalarFunc::Upper => exec_upper(reg_value),
                                    ScalarFunc::Length => Some(exec_length(reg_value)),
                                    ScalarFunc::Typeof => Some(exec_typeof(reg_value)),
                                    ScalarFunc::Unicode => Some(exec_unicode(reg_value)),
                                    ScalarFunc::Quote => Some(exec_quote(reg_value)),
                                    ScalarFunc::RandomBlob => Some(exec_randomblob(reg_value)),
                                    ScalarFunc::ZeroBlob => Some(exec_zeroblob(reg_value)),
                                    _ => unreachable!(),
                                };
                                state.registers[*dest] = result.unwrap_or(OwnedValue::Null);
                            }
                            ScalarFunc::Hex => {
                                let reg_value = state.registers[*start_reg].borrow_mut();
                                let result = exec_hex(reg_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Unhex => {
                                let reg_value = state.registers[*start_reg].clone();
                                let ignored_chars = state.registers.get(*start_reg + 1);
                                let result = exec_unhex(&reg_value, ignored_chars);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Random => {
                                state.registers[*dest] = exec_random();
                            }
                            ScalarFunc::Trim => {
                                let reg_value = state.registers[*start_reg].clone();
                                let pattern_value = state.registers.get(*start_reg + 1).cloned();
                                let result = exec_trim(&reg_value, pattern_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::LTrim => {
                                let reg_value = state.registers[*start_reg].clone();
                                let pattern_value = state.registers.get(*start_reg + 1).cloned();
                                let result = exec_ltrim(&reg_value, pattern_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::RTrim => {
                                let reg_value = state.registers[*start_reg].clone();
                                let pattern_value = state.registers.get(*start_reg + 1).cloned();
                                let result = exec_rtrim(&reg_value, pattern_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Round => {
                                let reg_value = state.registers[*start_reg].clone();
                                let precision_value = state.registers.get(*start_reg + 1).cloned();
                                let result = exec_round(&reg_value, precision_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Min => {
                                let reg_values = state.registers
                                    [*start_reg..*start_reg + arg_count]
                                    .iter()
                                    .collect();
                                state.registers[*dest] = exec_min(reg_values);
                            }
                            ScalarFunc::Max => {
                                let reg_values = state.registers
                                    [*start_reg..*start_reg + arg_count]
                                    .iter()
                                    .collect();
                                state.registers[*dest] = exec_max(reg_values);
                            }
                            ScalarFunc::Nullif => {
                                let first_value = &state.registers[*start_reg];
                                let second_value = &state.registers[*start_reg + 1];
                                state.registers[*dest] = exec_nullif(first_value, second_value);
                            }
                            ScalarFunc::Substr | ScalarFunc::Substring => {
                                let str_value = &state.registers[*start_reg];
                                let start_value = &state.registers[*start_reg + 1];
                                let length_value = &state.registers[*start_reg + 2];
                                let result = exec_substring(str_value, start_value, length_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Date => {
                                let result =
                                    exec_date(&state.registers[*start_reg..*start_reg + arg_count]);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Time => {
                                let result =
                                    exec_time(&state.registers[*start_reg..*start_reg + arg_count]);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::UnixEpoch => {
                                if *start_reg == 0 {
                                    let unixepoch: String = exec_unixepoch(&OwnedValue::Text(
                                        Rc::new("now".to_string()),
                                    ))?;
                                    state.registers[*dest] = OwnedValue::Text(Rc::new(unixepoch));
                                } else {
                                    let datetime_value = &state.registers[*start_reg];
                                    let unixepoch = exec_unixepoch(datetime_value);
                                    match unixepoch {
                                        Ok(time) => {
                                            state.registers[*dest] = OwnedValue::Text(Rc::new(time))
                                        }
                                        Err(e) => {
                                            return Err(LimboError::ParseError(format!(
                                                "Error encountered while parsing datetime value: {}",
                                                e
                                            )));
                                        }
                                    }
                                }
                            }
                            ScalarFunc::SqliteVersion => {
                                let version_integer: i64 =
                                    DATABASE_VERSION.get().unwrap().parse()?;
                                let version = execute_sqlite_version(version_integer);
                                state.registers[*dest] = OwnedValue::Text(Rc::new(version));
                            }
                        },
                        crate::function::Func::Agg(_) => {
                            unreachable!("Aggregate functions should not be handled here")
                        }
                    }
                    state.pc += 1;
                }
                Insn::InitCoroutine {
                    yield_reg,
                    jump_on_definition,
                    start_offset,
                } => {
                    state.registers[*yield_reg] = OwnedValue::Integer(*start_offset);
                    state.pc = *jump_on_definition;
                }
                Insn::EndCoroutine { yield_reg } => {
                    if let OwnedValue::Integer(pc) = state.registers[*yield_reg] {
                        state.ended_coroutine = true;
                        state.pc = pc - 1; // yield jump is always next to yield. Here we substract 1 to go back to yield instruction
                    } else {
                        unreachable!();
                    }
                }
                Insn::Yield {
                    yield_reg,
                    end_offset,
                } => {
                    if let OwnedValue::Integer(pc) = state.registers[*yield_reg] {
                        if state.ended_coroutine {
                            state.pc = *end_offset;
                        } else {
                            // swap
                            (state.pc, state.registers[*yield_reg]) =
                                (pc, OwnedValue::Integer(state.pc + 1));
                        }
                    } else {
                        unreachable!();
                    }
                }
                Insn::InsertAsync {
                    cursor,
                    key_reg,
                    record_reg,
                    flag: _,
                } => {
                    let cursor = cursors.get_mut(cursor).unwrap();
                    let record = match &state.registers[*record_reg] {
                        OwnedValue::Record(r) => r,
                        _ => unreachable!("Not a record! Cannot insert a non record value."),
                    };
                    let key = &state.registers[*key_reg];
                    match cursor.insert(key, record, true)? {
                        CursorResult::Ok(_) => {
                            state.pc += 1;
                        }
                        CursorResult::IO => {
                            // If there is I/O, the instruction is restarted.
                            return Ok(StepResult::IO);
                        }
                    }
                }
                Insn::InsertAwait { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    state.pc += 1;
                }
                Insn::NewRowid {
                    cursor, rowid_reg, ..
                } => {
                    let cursor = cursors.get_mut(cursor).unwrap();
                    // TODO: make io handle rng
                    let rowid = get_new_rowid(cursor, thread_rng())?;
                    match rowid {
                        CursorResult::Ok(rowid) => {
                            state.registers[*rowid_reg] = OwnedValue::Integer(rowid);
                        }
                        CursorResult::IO => return Ok(StepResult::IO),
                    }
                    state.pc += 1;
                }
                Insn::MustBeInt { reg } => {
                    match state.registers[*reg] {
                        OwnedValue::Integer(_) => {}
                        _ => {
                            crate::bail_parse_error!(
                                "MustBeInt: the value in the register is not an integer"
                            );
                        }
                    };
                    state.pc += 1;
                }
                Insn::SoftNull { reg } => {
                    state.registers[*reg] = OwnedValue::Null;
                    state.pc += 1;
                }
                Insn::NotExists {
                    cursor,
                    rowid_reg,
                    target_pc,
                } => {
                    let cursor = cursors.get_mut(cursor).unwrap();
                    match cursor.exists(&state.registers[*rowid_reg])? {
                        CursorResult::Ok(true) => state.pc += 1,
                        CursorResult::Ok(false) => state.pc = *target_pc,
                        CursorResult::IO => return Ok(StepResult::IO),
                    };
                }
                // this cursor may be reused for next insert
                // Update: tablemoveto is used to travers on not exists, on insert depending on flags if nonseek it traverses again.
                // If not there might be some optimizations obviously.
                Insn::OpenWriteAsync {
                    cursor_id,
                    root_page,
                } => {
                    let cursor = Box::new(BTreeCursor::new(
                        pager.clone(),
                        *root_page,
                        self.database_header.clone(),
                    ));
                    cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::OpenWriteAwait {} => {
                    state.pc += 1;
                }
                Insn::Copy {
                    src_reg,
                    dst_reg,
                    amount,
                } => {
                    for i in 0..=*amount {
                        state.registers[*dst_reg + i] = state.registers[*src_reg + i].clone();
                    }
                    state.pc += 1;
                }
                Insn::CreateBtree { db, root, flags: _ } => {
                    if *db > 0 {
                        // TODO: implement temp datbases
                        todo!("temp databases not implemented yet");
                    }
                    let mut cursor = Box::new(BTreeCursor::new(
                        pager.clone(),
                        0,
                        self.database_header.clone(),
                    ));

                    let root_page = cursor.btree_create(1);
                    state.registers[*root] = OwnedValue::Integer(root_page as i64);
                    state.pc += 1;
                }
                Insn::Close { cursor_id } => {
                    cursors.remove(cursor_id);
                    state.pc += 1;
                }
                Insn::IsNull { src, target_pc } => {
                    if matches!(state.registers[*src], OwnedValue::Null) {
                        state.pc = *target_pc;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::ParseSchema {
                    db: _,
                    where_clause,
                } => {
                    let conn = self.connection.upgrade();
                    let conn = conn.as_ref().unwrap();
                    let stmt = conn.prepare(format!(
                        "SELECT * FROM  sqlite_schema WHERE {}",
                        where_clause
                    ))?;
                    let rows = Rows { stmt };
                    let mut schema = RefCell::borrow_mut(&conn.schema);
                    // TODO: This function below is synchronous, make it not async
                    parse_schema_rows(Some(rows), &mut *schema, conn.pager.io.clone())?;
                    state.pc += 1;
                }
            }
        }
    }
}

fn get_new_rowid<R: Rng>(cursor: &mut Box<dyn Cursor>, mut rng: R) -> Result<CursorResult<i64>> {
    cursor.seek_to_last()?;
    let mut rowid = cursor.rowid()?.unwrap_or(0) + 1;
    if rowid > i64::MAX.try_into().unwrap() {
        let distribution = Uniform::from(1..=i64::MAX);
        let max_attempts = 100;
        for count in 0..max_attempts {
            rowid = distribution.sample(&mut rng).try_into().unwrap();
            match cursor.seek(SeekKey::TableRowId(rowid), SeekOp::EQ)? {
                CursorResult::Ok(false) => break, // Found a non-existing rowid
                CursorResult::Ok(true) => {
                    if count == max_attempts - 1 {
                        return Err(LimboError::InternalError(
                            "Failed to generate a new rowid".to_string(),
                        ));
                    } else {
                        continue; // Try next random rowid
                    }
                }
                CursorResult::IO => return Ok(CursorResult::IO),
            }
        }
    }
    Ok(CursorResult::Ok(rowid.try_into().unwrap()))
}

fn make_record<'a>(registers: &'a [OwnedValue], start_reg: &usize, count: &usize) -> Record<'a> {
    let mut values = Vec::with_capacity(*count);
    for r in registers.iter().skip(*start_reg).take(*count) {
        values.push(crate::types::to_value(r))
    }
    Record::new(values)
}

fn make_owned_record(registers: &[OwnedValue], start_reg: &usize, count: &usize) -> OwnedRecord {
    let mut values = Vec::with_capacity(*count);
    for r in registers.iter().skip(*start_reg).take(*count) {
        values.push(r.clone())
    }
    OwnedRecord::new(values)
}

fn trace_insn(program: &Program, addr: InsnReference, insn: &Insn) {
    if !log::log_enabled!(log::Level::Trace) {
        return;
    }
    log::trace!(
        "{}",
        explain::insn_to_str(
            program,
            addr,
            insn,
            String::new(),
            program.comments.get(&(addr as BranchOffset)).copied()
        )
    );
}

fn print_insn(program: &Program, addr: InsnReference, insn: &Insn, indent: String) {
    let s = explain::insn_to_str(
        program,
        addr,
        insn,
        indent,
        program.comments.get(&(addr as BranchOffset)).copied(),
    );
    println!("{}", s);
}

fn get_indent_count(indent_count: usize, curr_insn: &Insn, prev_insn: Option<&Insn>) -> usize {
    let indent_count = if let Some(insn) = prev_insn {
        match insn {
            Insn::RewindAwait { .. }
            | Insn::SorterSort { .. }
            | Insn::SeekGE { .. }
            | Insn::SeekGT { .. } => indent_count + 1,
            _ => indent_count,
        }
    } else {
        indent_count
    };

    match curr_insn {
        Insn::NextAsync { .. } | Insn::SorterNext { .. } => indent_count - 1,
        _ => indent_count,
    }
}

fn exec_lower(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Text(t) => Some(OwnedValue::Text(Rc::new(t.to_lowercase()))),
        t => Some(t.to_owned()),
    }
}

fn exec_length(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
            OwnedValue::Integer(reg.to_string().len() as i64)
        }
        OwnedValue::Blob(blob) => OwnedValue::Integer(blob.len() as i64),
        OwnedValue::Agg(aggctx) => exec_length(aggctx.final_value()),
        _ => reg.to_owned(),
    }
}

fn exec_upper(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Text(t) => Some(OwnedValue::Text(Rc::new(t.to_uppercase()))),
        t => Some(t.to_owned()),
    }
}

fn exec_concat(registers: &[OwnedValue]) -> OwnedValue {
    let mut result = String::new();
    for reg in registers {
        match reg {
            OwnedValue::Text(text) => result.push_str(text),
            OwnedValue::Integer(i) => result.push_str(&i.to_string()),
            OwnedValue::Float(f) => result.push_str(&f.to_string()),
            _ => continue,
        }
    }
    OwnedValue::Text(Rc::new(result))
}

fn exec_concat_ws(registers: &[OwnedValue]) -> OwnedValue {
    if registers.is_empty() {
        return OwnedValue::Null;
    }

    let separator = match &registers[0] {
        OwnedValue::Text(text) => text.clone(),
        OwnedValue::Integer(i) => Rc::new(i.to_string()),
        OwnedValue::Float(f) => Rc::new(f.to_string()),
        _ => return OwnedValue::Null,
    };

    let mut result = String::new();
    for (i, reg) in registers.iter().enumerate().skip(1) {
        if i > 1 {
            result.push_str(&separator);
        }
        match reg {
            OwnedValue::Text(text) => result.push_str(text),
            OwnedValue::Integer(i) => result.push_str(&i.to_string()),
            OwnedValue::Float(f) => result.push_str(&f.to_string()),
            _ => continue,
        }
    }

    OwnedValue::Text(Rc::new(result))
}

fn exec_sign(reg: &OwnedValue) -> Option<OwnedValue> {
    let num = match reg {
        OwnedValue::Integer(i) => *i as f64,
        OwnedValue::Float(f) => *f,
        OwnedValue::Text(s) => {
            if let Ok(i) = s.parse::<i64>() {
                i as f64
            } else if let Ok(f) = s.parse::<f64>() {
                f
            } else {
                return Some(OwnedValue::Null);
            }
        }
        OwnedValue::Blob(b) => match std::str::from_utf8(b) {
            Ok(s) => {
                if let Ok(i) = s.parse::<i64>() {
                    i as f64
                } else if let Ok(f) = s.parse::<f64>() {
                    f
                } else {
                    return Some(OwnedValue::Null);
                }
            }
            Err(_) => return Some(OwnedValue::Null),
        },
        _ => return Some(OwnedValue::Null),
    };

    let sign = if num > 0.0 {
        1
    } else if num < 0.0 {
        -1
    } else {
        0
    };

    Some(OwnedValue::Integer(sign))
}

fn exec_abs(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Integer(x) => {
            if x < &0 {
                Some(OwnedValue::Integer(-x))
            } else {
                Some(OwnedValue::Integer(*x))
            }
        }
        OwnedValue::Float(x) => {
            if x < &0.0 {
                Some(OwnedValue::Float(-x))
            } else {
                Some(OwnedValue::Float(*x))
            }
        }
        OwnedValue::Null => Some(OwnedValue::Null),
        _ => Some(OwnedValue::Float(0.0)),
    }
}

fn exec_random() -> OwnedValue {
    let mut buf = [0u8; 8];
    getrandom::getrandom(&mut buf).unwrap();
    let random_number = i64::from_ne_bytes(buf);
    OwnedValue::Integer(random_number)
}

fn exec_randomblob(reg: &OwnedValue) -> OwnedValue {
    let length = match reg {
        OwnedValue::Integer(i) => *i,
        OwnedValue::Float(f) => *f as i64,
        OwnedValue::Text(t) => t.parse().unwrap_or(1),
        _ => 1,
    }
    .max(1) as usize;

    let mut blob: Vec<u8> = vec![0; length];
    getrandom::getrandom(&mut blob).expect("Failed to generate random blob");
    OwnedValue::Blob(Rc::new(blob))
}

fn exec_quote(value: &OwnedValue) -> OwnedValue {
    match value {
        OwnedValue::Null => OwnedValue::Text(OwnedValue::Null.to_string().into()),
        OwnedValue::Integer(_) | OwnedValue::Float(_) => value.to_owned(),
        OwnedValue::Blob(_) => todo!(),
        OwnedValue::Text(s) => {
            let mut quoted = String::with_capacity(s.len() + 2);
            quoted.push('\'');
            for c in s.chars() {
                if c == '\0' {
                    break;
                } else {
                    quoted.push(c);
                }
            }
            quoted.push('\'');
            OwnedValue::Text(Rc::new(quoted))
        }
        _ => OwnedValue::Null, // For unsupported types, return NULL
    }
}

fn exec_char(values: Vec<OwnedValue>) -> OwnedValue {
    let result: String = values
        .iter()
        .filter_map(|x| {
            if let OwnedValue::Integer(i) = x {
                Some(*i as u8 as char)
            } else {
                None
            }
        })
        .collect();
    OwnedValue::Text(Rc::new(result))
}

fn construct_like_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::from("(?i)^");
    regex_pattern.push_str(&pattern.replace('%', ".*").replace('_', "."));
    regex_pattern.push('$');
    Regex::new(&regex_pattern).unwrap()
}

// Implements LIKE pattern matching. Caches the constructed regex if a cache is provided
fn exec_like(regex_cache: Option<&mut HashMap<String, Regex>>, pattern: &str, text: &str) -> bool {
    if let Some(cache) = regex_cache {
        match cache.get(pattern) {
            Some(re) => re.is_match(text),
            None => {
                let re = construct_like_regex(pattern);
                let res = re.is_match(text);
                cache.insert(pattern.to_string(), re);
                res
            }
        }
    } else {
        let re = construct_like_regex(pattern);
        re.is_match(text)
    }
}

fn construct_glob_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::from("^");
    regex_pattern.push_str(&pattern.replace('*', ".*").replace("?", "."));
    regex_pattern.push('$');
    Regex::new(&regex_pattern).unwrap()
}

// Implements GLOB pattern matching. Caches the constructed regex if a cache is provided
fn exec_glob(regex_cache: Option<&mut HashMap<String, Regex>>, pattern: &str, text: &str) -> bool {
    if let Some(cache) = regex_cache {
        match cache.get(pattern) {
            Some(re) => re.is_match(text),
            None => {
                let re = construct_glob_regex(pattern);
                let res = re.is_match(text);
                cache.insert(pattern.to_string(), re);
                res
            }
        }
    } else {
        let re = construct_glob_regex(pattern);
        re.is_match(text)
    }
}

fn exec_min(regs: Vec<&OwnedValue>) -> OwnedValue {
    regs.iter()
        .min()
        .map(|&v| v.to_owned())
        .unwrap_or(OwnedValue::Null)
}

fn exec_max(regs: Vec<&OwnedValue>) -> OwnedValue {
    regs.iter()
        .max()
        .map(|&v| v.to_owned())
        .unwrap_or(OwnedValue::Null)
}

fn exec_nullif(first_value: &OwnedValue, second_value: &OwnedValue) -> OwnedValue {
    if first_value != second_value {
        first_value.clone()
    } else {
        OwnedValue::Null
    }
}

fn exec_substring(
    str_value: &OwnedValue,
    start_value: &OwnedValue,
    length_value: &OwnedValue,
) -> OwnedValue {
    if let (OwnedValue::Text(str), OwnedValue::Integer(start), OwnedValue::Integer(length)) =
        (str_value, start_value, length_value)
    {
        let start = *start as usize;
        if start > str.len() {
            return OwnedValue::Text(Rc::new("".to_string()));
        }

        let start_idx = start - 1;
        let str_len = str.len();
        let end = if *length != -1 {
            start_idx + *length as usize
        } else {
            str_len
        };
        let substring = &str[start_idx..end.min(str_len)];

        OwnedValue::Text(Rc::new(substring.to_string()))
    } else if let (OwnedValue::Text(str), OwnedValue::Integer(start)) = (str_value, start_value) {
        let start = *start as usize;
        if start > str.len() {
            return OwnedValue::Text(Rc::new("".to_string()));
        }

        let start_idx = start - 1;
        let str_len = str.len();
        let substring = &str[start_idx..str_len];

        OwnedValue::Text(Rc::new(substring.to_string()))
    } else {
        OwnedValue::Null
    }
}

fn exec_instr(reg: &OwnedValue, pattern: &OwnedValue) -> OwnedValue {
    if reg == &OwnedValue::Null || pattern == &OwnedValue::Null {
        return OwnedValue::Null;
    }

    if let (OwnedValue::Blob(reg), OwnedValue::Blob(pattern)) = (reg, pattern) {
        let result = reg
            .windows(pattern.len())
            .position(|window| window == **pattern)
            .map_or(0, |i| i + 1);
        return OwnedValue::Integer(result as i64);
    }

    let reg_str;
    let reg = match reg {
        OwnedValue::Text(s) => s.as_str(),
        _ => {
            reg_str = reg.to_string();
            reg_str.as_str()
        }
    };

    let pattern_str;
    let pattern = match pattern {
        OwnedValue::Text(s) => s.as_str(),
        _ => {
            pattern_str = pattern.to_string();
            pattern_str.as_str()
        }
    };

    match reg.find(pattern) {
        Some(position) => OwnedValue::Integer(position as i64 + 1),
        None => OwnedValue::Integer(0),
    }
}

fn exec_typeof(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Null => OwnedValue::Text(Rc::new("null".to_string())),
        OwnedValue::Integer(_) => OwnedValue::Text(Rc::new("integer".to_string())),
        OwnedValue::Float(_) => OwnedValue::Text(Rc::new("real".to_string())),
        OwnedValue::Text(_) => OwnedValue::Text(Rc::new("text".to_string())),
        OwnedValue::Blob(_) => OwnedValue::Text(Rc::new("blob".to_string())),
        OwnedValue::Agg(ctx) => exec_typeof(ctx.final_value()),
        OwnedValue::Record(_) => unimplemented!(),
    }
}

fn exec_hex(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_)
        | OwnedValue::Integer(_)
        | OwnedValue::Float(_)
        | OwnedValue::Blob(_) => {
            let text = reg.to_string();
            OwnedValue::Text(Rc::new(hex::encode_upper(text)))
        }
        _ => OwnedValue::Null,
    }
}

fn exec_unhex(reg: &OwnedValue, ignored_chars: Option<&OwnedValue>) -> OwnedValue {
    if ignored_chars.is_some() {
        unimplemented!("unhex(X,Y) is not implemented");
    }

    match reg {
        OwnedValue::Null => OwnedValue::Null,
        _ => match hex::decode(reg.to_string()) {
            Ok(bytes) => OwnedValue::Blob(Rc::new(bytes)),
            Err(_) => OwnedValue::Null,
        },
    }
}

fn exec_unicode(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_)
        | OwnedValue::Integer(_)
        | OwnedValue::Float(_)
        | OwnedValue::Blob(_) => {
            let text = reg.to_string();
            if let Some(first_char) = text.chars().next() {
                OwnedValue::Integer(first_char as u32 as i64)
            } else {
                OwnedValue::Null
            }
        }
        _ => OwnedValue::Null,
    }
}

fn exec_round(reg: &OwnedValue, precision: Option<OwnedValue>) -> OwnedValue {
    let precision = match precision {
        Some(OwnedValue::Text(x)) => x.parse().unwrap_or(0.0),
        Some(OwnedValue::Integer(x)) => x as f64,
        Some(OwnedValue::Float(x)) => x,
        None => 0.0,
        _ => return OwnedValue::Null,
    };

    let reg = match reg {
        OwnedValue::Text(x) => x.parse().unwrap_or(0.0),
        OwnedValue::Integer(x) => *x as f64,
        OwnedValue::Float(x) => *x,
        _ => return reg.to_owned(),
    };

    let precision = if precision < 1.0 { 0.0 } else { precision };
    let multiplier = 10f64.powi(precision as i32);
    OwnedValue::Float(((reg * multiplier).round()) / multiplier)
}

// Implements TRIM pattern matching.
fn exec_trim(reg: &OwnedValue, pattern: Option<OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::Text(Rc::new(
                    reg.to_string().trim_matches(&pattern_chars[..]).to_string(),
                ))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => OwnedValue::Text(Rc::new(t.trim().to_string())),
        (reg, _) => reg.to_owned(),
    }
}

// Implements LTRIM pattern matching.
fn exec_ltrim(reg: &OwnedValue, pattern: Option<OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::Text(Rc::new(
                    reg.to_string()
                        .trim_start_matches(&pattern_chars[..])
                        .to_string(),
                ))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => OwnedValue::Text(Rc::new(t.trim_start().to_string())),
        (reg, _) => reg.to_owned(),
    }
}

// Implements RTRIM pattern matching.
fn exec_rtrim(reg: &OwnedValue, pattern: Option<OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::Text(Rc::new(
                    reg.to_string()
                        .trim_end_matches(&pattern_chars[..])
                        .to_string(),
                ))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => OwnedValue::Text(Rc::new(t.trim_end().to_string())),
        (reg, _) => reg.to_owned(),
    }
}

fn exec_zeroblob(req: &OwnedValue) -> OwnedValue {
    let length: i64 = match req {
        OwnedValue::Integer(i) => *i,
        OwnedValue::Float(f) => *f as i64,
        OwnedValue::Text(s) => s.parse().unwrap_or(0),
        _ => 0,
    };
    OwnedValue::Blob(Rc::new(vec![0; length.max(0) as usize]))
}

// exec_if returns whether you should jump
fn exec_if(reg: &OwnedValue, null_reg: &OwnedValue, not: bool) -> bool {
    match reg {
        OwnedValue::Integer(0) | OwnedValue::Float(0.0) => not,
        OwnedValue::Integer(_) | OwnedValue::Float(_) => !not,
        OwnedValue::Null => match null_reg {
            OwnedValue::Integer(0) | OwnedValue::Float(0.0) => false,
            OwnedValue::Integer(_) | OwnedValue::Float(_) => true,
            _ => false,
        },
        _ => false,
    }
}

fn exec_cast(value: &OwnedValue, datatype: &str) -> OwnedValue {
    if matches!(value, OwnedValue::Null) {
        return OwnedValue::Null;
    }
    match affinity(datatype) {
        // NONE	Casting a value to a type-name with no affinity causes the value to be converted into a BLOB. Casting to a BLOB consists of first casting the value to TEXT in the encoding of the database connection, then interpreting the resulting byte sequence as a BLOB instead of as TEXT.
        // Historically called NONE, but it's the same as BLOB
        Affinity::BLOB => {
            // Convert to TEXT first, then interpret as BLOB
            // TODO: handle encoding
            let text = value.to_string();
            OwnedValue::Blob(Rc::new(text.into_bytes()))
        }
        // TEXT To cast a BLOB value to TEXT, the sequence of bytes that make up the BLOB is interpreted as text encoded using the database encoding.
        // Casting an INTEGER or REAL value into TEXT renders the value as if via sqlite3_snprintf() except that the resulting TEXT uses the encoding of the database connection.
        Affinity::TEXT => {
            // Convert everything to text representation
            // TODO: handle encoding and whatever sqlite3_snprintf does
            OwnedValue::Text(Rc::new(value.to_string()))
        }
        Affinity::REAL => match value {
            OwnedValue::Blob(b) => {
                // Convert BLOB to TEXT first
                let text = String::from_utf8_lossy(b);
                cast_text_to_real(&text)
            }
            OwnedValue::Text(t) => cast_text_to_real(t),
            OwnedValue::Integer(i) => OwnedValue::Float(*i as f64),
            OwnedValue::Float(f) => OwnedValue::Float(*f),
            _ => OwnedValue::Float(0.0),
        },
        Affinity::INTEGER => match value {
            OwnedValue::Blob(b) => {
                // Convert BLOB to TEXT first
                let text = String::from_utf8_lossy(b);
                cast_text_to_integer(&text)
            }
            OwnedValue::Text(t) => cast_text_to_integer(t),
            OwnedValue::Integer(i) => OwnedValue::Integer(*i),
            // A cast of a REAL value into an INTEGER results in the integer between the REAL value and zero
            // that is closest to the REAL value. If a REAL is greater than the greatest possible signed integer (+9223372036854775807)
            // then the result is the greatest possible signed integer and if the REAL is less than the least possible signed integer (-9223372036854775808)
            // then the result is the least possible signed integer.
            OwnedValue::Float(f) => {
                let i = f.floor() as i128;
                if i > i64::MAX as i128 {
                    OwnedValue::Integer(i64::MAX)
                } else if i < i64::MIN as i128 {
                    OwnedValue::Integer(i64::MIN)
                } else {
                    OwnedValue::Integer(i as i64)
                }
            }
            _ => OwnedValue::Integer(0),
        },
        Affinity::NUMERIC => match value {
            OwnedValue::Blob(b) => {
                let text = String::from_utf8_lossy(b);
                cast_text_to_numeric(&text)
            }
            OwnedValue::Text(t) => cast_text_to_numeric(t),
            OwnedValue::Integer(i) => OwnedValue::Integer(*i),
            OwnedValue::Float(f) => OwnedValue::Float(*f),
            _ => value.clone(), // TODO probably wrong
        },
    }
}

enum Affinity {
    INTEGER,
    TEXT,
    BLOB,
    REAL,
    NUMERIC,
}

/// For tables not declared as STRICT, the affinity of a column is determined by the declared type of the column, according to the following rules in the order shown:
/// If the declared type contains the string "INT" then it is assigned INTEGER affinity.
/// If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
/// If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
/// If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL affinity.
/// Otherwise, the affinity is NUMERIC.
/// Note that the order of the rules for determining column affinity is important. A column whose declared type is "CHARINT" will match both rules 1 and 2 but the first rule takes precedence and so the column affinity will be INTEGER.
fn affinity(datatype: &str) -> Affinity {
    // Note: callers of this function must ensure that the datatype is uppercase.
    // Rule 1: INT -> INTEGER affinity
    if datatype.contains("INT") {
        return Affinity::INTEGER;
    }

    // Rule 2: CHAR/CLOB/TEXT -> TEXT affinity
    if datatype.contains("CHAR") || datatype.contains("CLOB") || datatype.contains("TEXT") {
        return Affinity::TEXT;
    }

    // Rule 3: BLOB or empty -> BLOB affinity (historically called NONE)
    if datatype.contains("BLOB") || datatype.is_empty() {
        return Affinity::BLOB;
    }

    // Rule 4: REAL/FLOA/DOUB -> REAL affinity
    if datatype.contains("REAL") || datatype.contains("FLOA") || datatype.contains("DOUB") {
        return Affinity::REAL;
    }

    // Rule 5: Otherwise -> NUMERIC affinity
    Affinity::NUMERIC
}

/// When casting a TEXT value to INTEGER, the longest possible prefix of the value that can be interpreted as an integer number
/// is extracted from the TEXT value and the remainder ignored. Any leading spaces in the TEXT value when converting from TEXT to INTEGER are ignored.
/// If there is no prefix that can be interpreted as an integer number, the result of the conversion is 0.
/// If the prefix integer is greater than +9223372036854775807 then the result of the cast is exactly +9223372036854775807.
/// Similarly, if the prefix integer is less than -9223372036854775808 then the result of the cast is exactly -9223372036854775808.
/// When casting to INTEGER, if the text looks like a floating point value with an exponent, the exponent will be ignored
/// because it is no part of the integer prefix. For example, "CAST('123e+5' AS INTEGER)" results in 123, not in 12300000.
/// The CAST operator understands decimal integers only — conversion of hexadecimal integers stops at the "x" in the "0x" prefix of the hexadecimal integer string and thus result of the CAST is always zero.
fn cast_text_to_integer(text: &str) -> OwnedValue {
    let text = text.trim();
    if let Ok(i) = text.parse::<i64>() {
        return OwnedValue::Integer(i);
    }
    // Try to find longest valid prefix that parses as an integer
    // TODO: inefficient
    let mut end_index = text.len() - 1;
    while end_index > 0 {
        if let Ok(i) = text[..=end_index].parse::<i64>() {
            return OwnedValue::Integer(i);
        }
        end_index -= 1;
    }
    OwnedValue::Integer(0)
}

/// When casting a TEXT value to REAL, the longest possible prefix of the value that can be interpreted
/// as a real number is extracted from the TEXT value and the remainder ignored. Any leading spaces in
/// the TEXT value are ignored when converging from TEXT to REAL.
/// If there is no prefix that can be interpreted as a real number, the result of the conversion is 0.0.
fn cast_text_to_real(text: &str) -> OwnedValue {
    let trimmed = text.trim_start();
    if let Ok(num) = trimmed.parse::<f64>() {
        return OwnedValue::Float(num);
    }
    // Try to find longest valid prefix that parses as a float
    // TODO: inefficient
    let mut end_index = trimmed.len() - 1;
    while end_index > 0 {
        if let Ok(num) = trimmed[..=end_index].parse::<f64>() {
            return OwnedValue::Float(num);
        }
        end_index -= 1;
    }
    OwnedValue::Float(0.0)
}

/// NUMERIC	Casting a TEXT or BLOB value into NUMERIC yields either an INTEGER or a REAL result.
/// If the input text looks like an integer (there is no decimal point nor exponent) and the value
/// is small enough to fit in a 64-bit signed integer, then the result will be INTEGER.
/// Input text that looks like floating point (there is a decimal point and/or an exponent)
/// and the text describes a value that can be losslessly converted back and forth between IEEE 754
/// 64-bit float and a 51-bit signed integer, then the result is INTEGER. (In the previous sentence,
/// a 51-bit integer is specified since that is one bit less than the length of the mantissa of an
/// IEEE 754 64-bit float and thus provides a 1-bit of margin for the text-to-float conversion operation.)
/// Any text input that describes a value outside the range of a 64-bit signed integer yields a REAL result.
/// Casting a REAL or INTEGER value to NUMERIC is a no-op, even if a real value could be losslessly converted to an integer.
fn cast_text_to_numeric(text: &str) -> OwnedValue {
    if !text.contains('.') && !text.contains('e') && !text.contains('E') {
        // Looks like an integer
        if let Ok(i) = text.parse::<i64>() {
            return OwnedValue::Integer(i);
        }
    }
    // Try as float
    if let Ok(f) = text.parse::<f64>() {
        // Check if can be losslessly converted to 51-bit integer
        let i = f as i64;
        if f == i as f64 && i.abs() < (1i64 << 51) {
            return OwnedValue::Integer(i);
        }
        return OwnedValue::Float(f);
    }
    OwnedValue::Integer(0)
}

fn execute_sqlite_version(version_integer: i64) -> String {
    let major = version_integer / 1_000_000;
    let minor = (version_integer % 1_000_000) / 1_000;
    let release = version_integer % 1_000;

    format!("{}.{}.{}", major, minor, release)
}

#[cfg(test)]
mod tests {

    use crate::types::{SeekKey, SeekOp};

    use super::{
        exec_abs, exec_char, exec_hex, exec_if, exec_instr, exec_length, exec_like, exec_lower,
        exec_ltrim, exec_max, exec_min, exec_nullif, exec_quote, exec_random, exec_randomblob,
        exec_round, exec_rtrim, exec_sign, exec_substring, exec_trim, exec_typeof, exec_unhex,
        exec_unicode, exec_upper, exec_zeroblob, execute_sqlite_version, get_new_rowid, AggContext,
        Cursor, CursorResult, LimboError, OwnedRecord, OwnedValue, Result,
    };
    use mockall::{mock, predicate};
    use rand::{rngs::mock::StepRng, thread_rng};
    use std::{cell::Ref, collections::HashMap, rc::Rc};

    mock! {
        Cursor {
            fn seek_to_last(&mut self) -> Result<CursorResult<()>>;
            fn seek<'a>(&mut self, key: SeekKey<'a>, op: SeekOp) -> Result<CursorResult<bool>>;
            fn rowid(&self) -> Result<Option<u64>>;
            fn seek_rowid(&mut self, rowid: u64) -> Result<CursorResult<bool>>;
        }
    }

    impl Cursor for MockCursor {
        fn seek_to_last(&mut self) -> Result<CursorResult<()>> {
            self.seek_to_last()
        }

        fn rowid(&self) -> Result<Option<u64>> {
            self.rowid()
        }

        fn seek(&mut self, key: SeekKey<'_>, op: SeekOp) -> Result<CursorResult<bool>> {
            self.seek(key, op)
        }

        fn rewind(&mut self) -> Result<CursorResult<()>> {
            unimplemented!()
        }

        fn next(&mut self) -> Result<CursorResult<()>> {
            unimplemented!()
        }

        fn record(&self) -> Result<Ref<Option<OwnedRecord>>> {
            unimplemented!()
        }

        fn is_empty(&self) -> bool {
            unimplemented!()
        }

        fn set_null_flag(&mut self, _flag: bool) {
            unimplemented!()
        }

        fn get_null_flag(&self) -> bool {
            unimplemented!()
        }

        fn insert(
            &mut self,
            _key: &OwnedValue,
            _record: &OwnedRecord,
            _is_leaf: bool,
        ) -> Result<CursorResult<()>> {
            unimplemented!()
        }

        fn wait_for_completion(&mut self) -> Result<()> {
            unimplemented!()
        }

        fn exists(&mut self, _key: &OwnedValue) -> Result<CursorResult<bool>> {
            unimplemented!()
        }

        fn btree_create(&mut self, _flags: usize) -> u32 {
            unimplemented!()
        }
    }

    #[test]
    fn test_get_new_rowid() -> Result<()> {
        // Test case 0: Empty table
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid().return_once(|| Ok(None));

        let result = get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), thread_rng())?;
        assert_eq!(
            result,
            CursorResult::Ok(1),
            "For an empty table, rowid should be 1"
        );

        // Test case 1: Normal case, rowid within i64::MAX
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid().return_once(|| Ok(Some(100)));

        let result = get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), thread_rng())?;
        assert_eq!(result, CursorResult::Ok(101));

        // Test case 2: Rowid exceeds i64::MAX, need to generate random rowid
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid()
            .return_once(|| Ok(Some(std::i64::MAX as u64)));
        mock.expect_seek()
            .with(predicate::always(), predicate::always())
            .returning(|rowid, _| {
                if rowid == SeekKey::TableRowId(50) {
                    Ok(CursorResult::Ok(false))
                } else {
                    Ok(CursorResult::Ok(true))
                }
            });

        // Mock the random number generation
        let new_rowid =
            get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), StepRng::new(1, 1))?;
        assert_eq!(new_rowid, CursorResult::Ok(50));

        // Test case 3: IO error
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid()
            .return_once(|| Ok(Some(std::i64::MAX as u64)));
        mock.expect_seek()
            .with(predicate::always(), predicate::always())
            .return_once(|_, _| Ok(CursorResult::IO));

        let result = get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), thread_rng());
        assert!(matches!(result, Ok(CursorResult::IO)));

        // Test case 4: Failure to generate new rowid
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid()
            .return_once(|| Ok(Some(std::i64::MAX as u64)));
        mock.expect_seek()
            .with(predicate::always(), predicate::always())
            .returning(|_, _| Ok(CursorResult::Ok(true)));

        // Mock the random number generation
        let result = get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), StepRng::new(1, 1));
        assert!(matches!(result, Err(LimboError::InternalError(_))));

        Ok(())
    }

    #[test]
    fn test_length() {
        let input_str = OwnedValue::Text(Rc::new(String::from("bob")));
        let expected_len = OwnedValue::Integer(3);
        assert_eq!(exec_length(&input_str), expected_len);

        let input_integer = OwnedValue::Integer(123);
        let expected_len = OwnedValue::Integer(3);
        assert_eq!(exec_length(&input_integer), expected_len);

        let input_float = OwnedValue::Float(123.456);
        let expected_len = OwnedValue::Integer(7);
        assert_eq!(exec_length(&input_float), expected_len);

        let expected_blob = OwnedValue::Blob(Rc::new("example".as_bytes().to_vec()));
        let expected_len = OwnedValue::Integer(7);
        assert_eq!(exec_length(&expected_blob), expected_len);
    }

    #[test]
    fn test_quote() {
        let input = OwnedValue::Text(Rc::new(String::from("abc\0edf")));
        let expected = OwnedValue::Text(Rc::new(String::from("'abc'")));
        assert_eq!(exec_quote(&input), expected);

        let input = OwnedValue::Integer(123);
        let expected = OwnedValue::Integer(123);
        assert_eq!(exec_quote(&input), expected);

        let input = OwnedValue::Text(Rc::new(String::from("hello''world")));
        let expected = OwnedValue::Text(Rc::new(String::from("'hello''world'")));
        assert_eq!(exec_quote(&input), expected);
    }

    #[test]
    fn test_typeof() {
        let input = OwnedValue::Null;
        let expected: OwnedValue = OwnedValue::Text(Rc::new("null".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Integer(123);
        let expected: OwnedValue = OwnedValue::Text(Rc::new("integer".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Float(123.456);
        let expected: OwnedValue = OwnedValue::Text(Rc::new("real".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Text(Rc::new("hello".to_string()));
        let expected: OwnedValue = OwnedValue::Text(Rc::new("text".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Blob(Rc::new("limbo".as_bytes().to_vec()));
        let expected: OwnedValue = OwnedValue::Text(Rc::new("blob".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Integer(123))));
        let expected = OwnedValue::Text(Rc::new("integer".to_string()));
        assert_eq!(exec_typeof(&input), expected);
    }

    #[test]
    fn test_unicode() {
        assert_eq!(
            exec_unicode(&OwnedValue::Text(Rc::new("a".to_string()))),
            OwnedValue::Integer(97)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Text(Rc::new("😊".to_string()))),
            OwnedValue::Integer(128522)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Text(Rc::new("".to_string()))),
            OwnedValue::Null
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Integer(23)),
            OwnedValue::Integer(50)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Integer(0)),
            OwnedValue::Integer(48)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Float(0.0)),
            OwnedValue::Integer(48)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::Float(23.45)),
            OwnedValue::Integer(50)
        );
        assert_eq!(exec_unicode(&OwnedValue::Null), OwnedValue::Null);
        assert_eq!(
            exec_unicode(&OwnedValue::Blob(Rc::new("example".as_bytes().to_vec()))),
            OwnedValue::Integer(101)
        );
    }

    #[test]
    fn test_min_max() {
        let input_int_vec = vec![&OwnedValue::Integer(-1), &OwnedValue::Integer(10)];
        assert_eq!(exec_min(input_int_vec.clone()), OwnedValue::Integer(-1));
        assert_eq!(exec_max(input_int_vec.clone()), OwnedValue::Integer(10));

        let str1 = OwnedValue::Text(Rc::new(String::from("A")));
        let str2 = OwnedValue::Text(Rc::new(String::from("z")));
        let input_str_vec = vec![&str2, &str1];
        assert_eq!(
            exec_min(input_str_vec.clone()),
            OwnedValue::Text(Rc::new(String::from("A")))
        );
        assert_eq!(
            exec_max(input_str_vec.clone()),
            OwnedValue::Text(Rc::new(String::from("z")))
        );

        let input_null_vec = vec![&OwnedValue::Null, &OwnedValue::Null];
        assert_eq!(exec_min(input_null_vec.clone()), OwnedValue::Null);
        assert_eq!(exec_max(input_null_vec.clone()), OwnedValue::Null);

        let input_mixed_vec = vec![&OwnedValue::Integer(10), &str1];
        assert_eq!(exec_min(input_mixed_vec.clone()), OwnedValue::Integer(10));
        assert_eq!(
            exec_max(input_mixed_vec.clone()),
            OwnedValue::Text(Rc::new(String::from("A")))
        );
    }

    #[test]
    fn test_trim() {
        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("Bob and Alice")));
        assert_eq!(exec_trim(&input_str, None), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::Text(Rc::new(String::from("Bob and")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("Alice")));
        assert_eq!(exec_trim(&input_str, Some(pattern_str)), expected_str);
    }

    #[test]
    fn test_ltrim() {
        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("Bob and Alice     ")));
        assert_eq!(exec_ltrim(&input_str, None), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::Text(Rc::new(String::from("Bob and")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("Alice     ")));
        assert_eq!(exec_ltrim(&input_str, Some(pattern_str)), expected_str);
    }

    #[test]
    fn test_rtrim() {
        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice")));
        assert_eq!(exec_rtrim(&input_str, None), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::Text(Rc::new(String::from("Bob and")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice")));
        assert_eq!(exec_rtrim(&input_str, Some(pattern_str)), expected_str);

        let input_str = OwnedValue::Text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::Text(Rc::new(String::from("and Alice")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("     Bob")));
        assert_eq!(exec_rtrim(&input_str, Some(pattern_str)), expected_str);
    }

    #[test]
    fn test_upper_case() {
        let input_str = OwnedValue::Text(Rc::new(String::from("Limbo")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("LIMBO")));
        assert_eq!(exec_upper(&input_str).unwrap(), expected_str);

        let input_int = OwnedValue::Integer(10);
        assert_eq!(exec_upper(&input_int).unwrap(), input_int);
        assert_eq!(exec_upper(&OwnedValue::Null).unwrap(), OwnedValue::Null)
    }

    #[test]
    fn test_lower_case() {
        let input_str = OwnedValue::Text(Rc::new(String::from("Limbo")));
        let expected_str = OwnedValue::Text(Rc::new(String::from("limbo")));
        assert_eq!(exec_lower(&input_str).unwrap(), expected_str);

        let input_int = OwnedValue::Integer(10);
        assert_eq!(exec_lower(&input_int).unwrap(), input_int);
        assert_eq!(exec_lower(&OwnedValue::Null).unwrap(), OwnedValue::Null)
    }

    #[test]
    fn test_hex() {
        let input_str = OwnedValue::Text(Rc::new("limbo".to_string()));
        let expected_val = OwnedValue::Text(Rc::new(String::from("6C696D626F")));
        assert_eq!(exec_hex(&input_str), expected_val);

        let input_int = OwnedValue::Integer(100);
        let expected_val = OwnedValue::Text(Rc::new(String::from("313030")));
        assert_eq!(exec_hex(&input_int), expected_val);

        let input_float = OwnedValue::Float(12.34);
        let expected_val = OwnedValue::Text(Rc::new(String::from("31322E3334")));
        assert_eq!(exec_hex(&input_float), expected_val);
    }

    #[test]
    fn test_unhex() {
        let input = OwnedValue::Text(Rc::new(String::from("6F")));
        let expected = OwnedValue::Blob(Rc::new(vec![0x6f]));
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::Text(Rc::new(String::from("6f")));
        let expected = OwnedValue::Blob(Rc::new(vec![0x6f]));
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::Text(Rc::new(String::from("611")));
        let expected = OwnedValue::Null;
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::Text(Rc::new(String::from("")));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::Text(Rc::new(String::from("61x")));
        let expected = OwnedValue::Null;
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_unhex(&input, None), expected);
    }

    #[test]
    fn test_abs() {
        let int_positive_reg = OwnedValue::Integer(10);
        let int_negative_reg = OwnedValue::Integer(-10);
        assert_eq!(exec_abs(&int_positive_reg).unwrap(), int_positive_reg);
        assert_eq!(exec_abs(&int_negative_reg).unwrap(), int_positive_reg);

        let float_positive_reg = OwnedValue::Integer(10);
        let float_negative_reg = OwnedValue::Integer(-10);
        assert_eq!(exec_abs(&float_positive_reg).unwrap(), float_positive_reg);
        assert_eq!(exec_abs(&float_negative_reg).unwrap(), float_positive_reg);

        assert_eq!(
            exec_abs(&OwnedValue::Text(Rc::new(String::from("a")))).unwrap(),
            OwnedValue::Float(0.0)
        );
        assert_eq!(exec_abs(&OwnedValue::Null).unwrap(), OwnedValue::Null);
    }

    #[test]
    fn test_char() {
        assert_eq!(
            exec_char(vec![OwnedValue::Integer(108), OwnedValue::Integer(105)]),
            OwnedValue::Text(Rc::new("li".to_string()))
        );
        assert_eq!(exec_char(vec![]), OwnedValue::Text(Rc::new("".to_string())));
        assert_eq!(
            exec_char(vec![OwnedValue::Null]),
            OwnedValue::Text(Rc::new("".to_string()))
        );
        assert_eq!(
            exec_char(vec![OwnedValue::Text(Rc::new("a".to_string()))]),
            OwnedValue::Text(Rc::new("".to_string()))
        );
    }

    #[test]
    fn test_like_no_cache() {
        assert!(exec_like(None, "a%", "aaaa"));
        assert!(exec_like(None, "%a%a", "aaaa"));
        assert!(exec_like(None, "%a.a", "aaaa"));
        assert!(exec_like(None, "a.a%", "aaaa"));
        assert!(!exec_like(None, "%a.ab", "aaaa"));
    }

    #[test]
    fn test_like_with_cache() {
        let mut cache = HashMap::new();
        assert!(exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(exec_like(Some(&mut cache), "a.a%", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "%a.ab", "aaaa"));

        // again after values have been cached
        assert!(exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(exec_like(Some(&mut cache), "a.a%", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "%a.ab", "aaaa"));
    }

    #[test]
    fn test_random() {
        match exec_random() {
            OwnedValue::Integer(value) => {
                // Check that the value is within the range of i64
                assert!(
                    (i64::MIN..=i64::MAX).contains(&value),
                    "Random number out of range"
                );
            }
            _ => panic!("exec_random did not return an Integer variant"),
        }
    }

    #[test]
    fn test_exec_randomblob() {
        struct TestCase {
            input: OwnedValue,
            expected_len: usize,
        }

        let test_cases = vec![
            TestCase {
                input: OwnedValue::Integer(5),
                expected_len: 5,
            },
            TestCase {
                input: OwnedValue::Integer(0),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Integer(-1),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Text(Rc::new(String::from(""))),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Text(Rc::new(String::from("5"))),
                expected_len: 5,
            },
            TestCase {
                input: OwnedValue::Text(Rc::new(String::from("0"))),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Text(Rc::new(String::from("-1"))),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Float(2.9),
                expected_len: 2,
            },
            TestCase {
                input: OwnedValue::Float(-3.14),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Null,
                expected_len: 1,
            },
        ];

        for test_case in &test_cases {
            let result = exec_randomblob(&test_case.input);
            match result {
                OwnedValue::Blob(blob) => {
                    assert_eq!(blob.len(), test_case.expected_len);
                }
                _ => panic!("exec_randomblob did not return a Blob variant"),
            }
        }
    }

    #[test]
    fn test_exec_round() {
        let input_val = OwnedValue::Float(123.456);
        let expected_val = OwnedValue::Float(123.0);
        assert_eq!(exec_round(&input_val, None), expected_val);

        let input_val = OwnedValue::Float(123.456);
        let precision_val = OwnedValue::Integer(2);
        let expected_val = OwnedValue::Float(123.46);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::Float(123.456);
        let precision_val = OwnedValue::Text(Rc::new(String::from("1")));
        let expected_val = OwnedValue::Float(123.5);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::Text(Rc::new(String::from("123.456")));
        let precision_val = OwnedValue::Integer(2);
        let expected_val = OwnedValue::Float(123.46);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::Integer(123);
        let precision_val = OwnedValue::Integer(1);
        let expected_val = OwnedValue::Float(123.0);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);
    }

    #[test]
    fn test_exec_if() {
        let reg = OwnedValue::Integer(0);
        let null_reg = OwnedValue::Integer(0);
        assert!(!exec_if(&reg, &null_reg, false));
        assert!(exec_if(&reg, &null_reg, true));

        let reg = OwnedValue::Integer(1);
        let null_reg = OwnedValue::Integer(0);
        assert!(exec_if(&reg, &null_reg, false));
        assert!(!exec_if(&reg, &null_reg, true));

        let reg = OwnedValue::Null;
        let null_reg = OwnedValue::Integer(0);
        assert!(!exec_if(&reg, &null_reg, false));
        assert!(!exec_if(&reg, &null_reg, true));

        let reg = OwnedValue::Null;
        let null_reg = OwnedValue::Integer(1);
        assert!(exec_if(&reg, &null_reg, false));
        assert!(exec_if(&reg, &null_reg, true));

        let reg = OwnedValue::Null;
        let null_reg = OwnedValue::Null;
        assert!(!exec_if(&reg, &null_reg, false));
        assert!(!exec_if(&reg, &null_reg, true));
    }

    #[test]
    fn test_nullif() {
        assert_eq!(
            exec_nullif(&OwnedValue::Integer(1), &OwnedValue::Integer(1)),
            OwnedValue::Null
        );
        assert_eq!(
            exec_nullif(&OwnedValue::Float(1.1), &OwnedValue::Float(1.1)),
            OwnedValue::Null
        );
        assert_eq!(
            exec_nullif(
                &OwnedValue::Text(Rc::new("limbo".to_string())),
                &OwnedValue::Text(Rc::new("limbo".to_string()))
            ),
            OwnedValue::Null
        );

        assert_eq!(
            exec_nullif(&OwnedValue::Integer(1), &OwnedValue::Integer(2)),
            OwnedValue::Integer(1)
        );
        assert_eq!(
            exec_nullif(&OwnedValue::Float(1.1), &OwnedValue::Float(1.2)),
            OwnedValue::Float(1.1)
        );
        assert_eq!(
            exec_nullif(
                &OwnedValue::Text(Rc::new("limbo".to_string())),
                &OwnedValue::Text(Rc::new("limb".to_string()))
            ),
            OwnedValue::Text(Rc::new("limbo".to_string()))
        );
    }

    #[test]
    fn test_substring() {
        let str_value = OwnedValue::Text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(1);
        let length_value = OwnedValue::Integer(3);
        let expected_val = OwnedValue::Text(Rc::new(String::from("lim")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::Text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(1);
        let length_value = OwnedValue::Integer(10);
        let expected_val = OwnedValue::Text(Rc::new(String::from("limbo")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::Text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(10);
        let length_value = OwnedValue::Integer(3);
        let expected_val = OwnedValue::Text(Rc::new(String::from("")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::Text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(3);
        let length_value = OwnedValue::Null;
        let expected_val = OwnedValue::Text(Rc::new(String::from("mbo")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::Text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(10);
        let length_value = OwnedValue::Null;
        let expected_val = OwnedValue::Text(Rc::new(String::from("")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );
    }

    #[test]
    fn test_exec_instr() {
        let input = OwnedValue::Text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Text(Rc::new(String::from("im")));
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Text(Rc::new(String::from("limbo")));
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Text(Rc::new(String::from("o")));
        let expected = OwnedValue::Integer(5);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("liiiiimbo")));
        let pattern = OwnedValue::Text(Rc::new(String::from("ii")));
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Text(Rc::new(String::from("limboX")));
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Text(Rc::new(String::from("")));
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("")));
        let pattern = OwnedValue::Text(Rc::new(String::from("limbo")));
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("")));
        let pattern = OwnedValue::Text(Rc::new(String::from("")));
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Null;
        let pattern = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Null;
        let pattern = OwnedValue::Text(Rc::new(String::from("limbo")));
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Integer(123);
        let pattern = OwnedValue::Integer(2);
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Integer(123);
        let pattern = OwnedValue::Integer(5);
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Float(12.34);
        let pattern = OwnedValue::Float(2.3);
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Float(12.34);
        let pattern = OwnedValue::Float(5.6);
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Float(12.34);
        let pattern = OwnedValue::Text(Rc::new(String::from(".")));
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Blob(Rc::new(vec![1, 2, 3, 4, 5]));
        let pattern = OwnedValue::Blob(Rc::new(vec![3, 4]));
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Blob(Rc::new(vec![1, 2, 3, 4, 5]));
        let pattern = OwnedValue::Blob(Rc::new(vec![3, 2]));
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Blob(Rc::new(vec![0x61, 0x62, 0x63, 0x64, 0x65]));
        let pattern = OwnedValue::Text(Rc::new(String::from("cd")));
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Text(Rc::new(String::from("abcde")));
        let pattern = OwnedValue::Blob(Rc::new(vec![0x63, 0x64]));
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);
    }

    #[test]
    fn test_exec_sign() {
        let input = OwnedValue::Integer(42);
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Integer(-42);
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Integer(0);
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(0.0);
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(0.1);
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(42.0);
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Float(-42.0);
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Text(Rc::new("abc".to_string()));
        let expected = Some(OwnedValue::Null);
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Text(Rc::new("42".to_string()));
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Text(Rc::new("-42".to_string()));
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Text(Rc::new("0".to_string()));
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(Rc::new(b"abc".to_vec()));
        let expected = Some(OwnedValue::Null);
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(Rc::new(b"42".to_vec()));
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(Rc::new(b"-42".to_vec()));
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Blob(Rc::new(b"0".to_vec()));
        let expected = Some(OwnedValue::Integer(0));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::Null;
        let expected = Some(OwnedValue::Null);
        assert_eq!(exec_sign(&input), expected);
    }

    #[test]
    fn test_exec_zeroblob() {
        let input = OwnedValue::Integer(0);
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Null;
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Integer(4);
        let expected = OwnedValue::Blob(Rc::new(vec![0; 4]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Integer(-1);
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Text(Rc::new("5".to_string()));
        let expected = OwnedValue::Blob(Rc::new(vec![0; 5]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Text(Rc::new("-5".to_string()));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Text(Rc::new("text".to_string()));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Float(2.6);
        let expected = OwnedValue::Blob(Rc::new(vec![0; 2]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::Blob(Rc::new(vec![1]));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);
    }

    #[test]
    fn test_execute_sqlite_version() {
        let version_integer = 3046001;
        let expected = "3.46.1";
        assert_eq!(execute_sqlite_version(version_integer), expected);
    }
}
