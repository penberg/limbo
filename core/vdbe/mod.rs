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

use crate::error::LimboError;
use crate::function::{AggFunc, JsonFunc, ScalarFunc};
use crate::json::get_json;
use crate::pseudo::PseudoCursor;
use crate::schema::Table;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::{btree::BTreeCursor, pager::Pager};
use crate::types::{AggContext, Cursor, CursorResult, OwnedRecord, OwnedValue, Record};
use crate::Result;

use datetime::{exec_date, exec_time};

use rand::distributions::{Distribution, Uniform};
use rand::{thread_rng, Rng};
use regex::Regex;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

pub type BranchOffset = i64;

pub type CursorID = usize;

pub type PageIdx = usize;

#[derive(Debug)]
pub enum Func {
    Scalar(ScalarFunc),
    Json(JsonFunc),
}

impl ToString for Func {
    fn to_string(&self) -> String {
        match self {
            Func::Scalar(scalar_func) => scalar_func.to_string(),
            Func::Json(json_func) => json_func.to_string(),
        }
    }
}

#[derive(Debug)]
pub enum Insn {
    // Initialize the program state and jump to the given PC.
    Init {
        target_pc: BranchOffset,
    },
    // Set NULL in the given register.
    Null {
        dest: usize,
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

    // Await for the competion of open cursor.
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
    Halt,

    // Start a transaction.
    Transaction,

    // Branch to the given PC.
    Goto {
        target_pc: BranchOffset,
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
        // constant_mask: i32, // P1, not used for now
        start_reg: usize, // P2, start of argument registers
        dest: usize,      // P3
        func: Func,       // P4
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
}

// Index of insn in list of insns
type InsnReference = usize;

pub enum StepResult<'a> {
    Done,
    IO,
    Row(Record<'a>),
}

/// The program state describes the environment in which the program executes.
pub struct ProgramState {
    pub pc: BranchOffset,
    cursors: RefCell<BTreeMap<CursorID, Box<dyn Cursor>>>,
    registers: Vec<OwnedValue>,
    ended_coroutine: bool, // flag to notify yield coroutine finished
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
            ended_coroutine: false,
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
                        (OwnedValue::Null, _) | (_, OwnedValue::Null) => {
                            state.registers[dest] = OwnedValue::Null;
                        }
                        _ => {
                            todo!();
                        }
                    }
                    state.pc += 1;
                }
                Insn::Null { dest } => {
                    state.registers[*dest] = OwnedValue::Null;
                    state.pc += 1;
                }
                Insn::NullRow { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.set_null_flag(true);
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
                            if &state.registers[lhs] == &state.registers[rhs] {
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
                            if &state.registers[lhs] != &state.registers[rhs] {
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
                            if &state.registers[lhs] < &state.registers[rhs] {
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
                            if &state.registers[lhs] <= &state.registers[rhs] {
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
                            if &state.registers[lhs] > &state.registers[rhs] {
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
                            if &state.registers[lhs] >= &state.registers[rhs] {
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
                Insn::Halt => {
                    pager.end_read_tx()?;
                    return Ok(StepResult::Done);
                }
                Insn::Transaction => {
                    pager.begin_read_tx()?;
                    state.pc += 1;
                }
                Insn::Goto { target_pc } => {
                    assert!(*target_pc >= 0);
                    state.pc = *target_pc;
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
                Insn::RowId { cursor_id, dest } => {
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
                        _ => {
                            return Err(LimboError::InternalError(
                                "SeekRowid: the value in the register is not an integer".into(),
                            ));
                        }
                    };
                    match cursor.seek_rowid(rowid)? {
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
                            todo!();
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
                    func,
                    start_reg,
                    dest,
                } => match func {
                    Func::Json(JsonFunc::JSON) => {
                        let json_value = &state.registers[*start_reg];
                        let json_str = get_json(json_value);
                        match json_str {
                            Ok(json) => state.registers[*dest] = json,
                            Err(e) => return Err(e),
                        }
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Coalesce) => {}
                    Func::Scalar(ScalarFunc::Like) => {
                        let start_reg = *start_reg;
                        assert!(
                            start_reg + 2 <= state.registers.len(),
                            "not enough registers {} < {}",
                            start_reg,
                            state.registers.len()
                        );
                        let pattern = state.registers[start_reg].clone();
                        let text = state.registers[start_reg + 1].clone();
                        let result = match (pattern, text) {
                            (OwnedValue::Text(pattern), OwnedValue::Text(text)) => {
                                OwnedValue::Integer(exec_like(&pattern, &text) as i64)
                            }
                            _ => {
                                unreachable!("Like on non-text registers");
                            }
                        };
                        state.registers[*dest] = result;
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Abs) => {
                        let reg_value = state.registers[*start_reg].borrow_mut();
                        if let Some(value) = exec_abs(reg_value) {
                            state.registers[*dest] = value;
                        } else {
                            state.registers[*dest] = OwnedValue::Null;
                        }
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Upper) => {
                        let reg_value = state.registers[*start_reg].borrow_mut();
                        if let Some(value) = exec_upper(reg_value) {
                            state.registers[*dest] = value;
                        } else {
                            state.registers[*dest] = OwnedValue::Null;
                        }
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Lower) => {
                        let reg_value = state.registers[*start_reg].borrow_mut();
                        if let Some(value) = exec_lower(reg_value) {
                            state.registers[*dest] = value;
                        } else {
                            state.registers[*dest] = OwnedValue::Null;
                        }
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Length) => {
                        let reg_value = state.registers[*start_reg].borrow_mut();
                        state.registers[*dest] = exec_length(reg_value);
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Random) => {
                        state.registers[*dest] = exec_random();
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Trim) => {
                        let start_reg = *start_reg;
                        let reg_value = state.registers[start_reg].clone();
                        let pattern_value = state.registers.get(start_reg + 1).cloned();

                        let result = exec_trim(&reg_value, pattern_value);

                        state.registers[*dest] = result;
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::LTrim) => {
                        let start_reg = *start_reg;
                        let reg_value = state.registers[start_reg].clone();
                        let pattern_value = state.registers.get(start_reg + 1).cloned();

                        let result = exec_ltrim(&reg_value, pattern_value);

                        state.registers[*dest] = result;
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::RTrim) => {
                        let start_reg = *start_reg;
                        let reg_value = state.registers[start_reg].clone();
                        let pattern_value = state.registers.get(start_reg + 1).cloned();

                        let result = exec_rtrim(&reg_value, pattern_value);

                        state.registers[*dest] = result;
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Round) => {
                        let start_reg = *start_reg;
                        let reg_value = state.registers[start_reg].clone();
                        let precision_value = state.registers.get(start_reg + 1).cloned();
                        let result = exec_round(&reg_value, precision_value);
                        state.registers[*dest] = result;
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Min) => {
                        let start_reg = *start_reg;
                        let reg_values = state.registers[start_reg..state.registers.len()]
                            .iter()
                            .collect();
                        let min_fn = |a, b| if a < b { a } else { b };
                        if let Some(value) = exec_minmax(reg_values, min_fn) {
                            state.registers[*dest] = value;
                        } else {
                            state.registers[*dest] = OwnedValue::Null;
                        }
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Max) => {
                        let start_reg = *start_reg;
                        let reg_values = state.registers[start_reg..state.registers.len()]
                            .iter()
                            .collect();
                        let max_fn = |a, b| if a > b { a } else { b };
                        if let Some(value) = exec_minmax(reg_values, max_fn) {
                            state.registers[*dest] = value;
                        } else {
                            state.registers[*dest] = OwnedValue::Null;
                        }
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Substring) => {
                        let start_reg = *start_reg;
                        let str_value = &state.registers[start_reg];
                        let start_value = &state.registers[start_reg + 1];
                        let length_value = &state.registers[start_reg + 2];

                        let result = exec_substring(str_value, start_value, length_value);
                        state.registers[*dest] = result;
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Date) => {
                        if *start_reg == 0 {
                            let date_str =
                                exec_date(&OwnedValue::Text(Rc::new("now".to_string())))?;
                            state.registers[*dest] = OwnedValue::Text(Rc::new(date_str));
                        } else {
                            let time_value = &state.registers[*start_reg];
                            let date_str = exec_date(time_value);
                            match date_str {
                                Ok(date) => {
                                    state.registers[*dest] = OwnedValue::Text(Rc::new(date))
                                }
                                Err(e) => {
                                    return Err(LimboError::ParseError(format!(
                                        "Error encountered while parsing time value: {}",
                                        e
                                    )));
                                }
                            }
                        }
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Time) => {
                        if *start_reg == 0 {
                            let time_str =
                                exec_time(&OwnedValue::Text(Rc::new("now".to_string())))?;
                            state.registers[*dest] = OwnedValue::Text(Rc::new(time_str));
                        } else {
                            let datetime_value = &state.registers[*start_reg];
                            let time_str = exec_time(datetime_value);
                            match time_str {
                                Ok(time) => {
                                    state.registers[*dest] = OwnedValue::Text(Rc::new(time))
                                }
                                Err(e) => {
                                    return Err(LimboError::ParseError(format!(
                                        "Error encountered while parsing time value: {}",
                                        e
                                    )));
                                }
                            }
                        }
                        state.pc += 1;
                    }
                    Func::Scalar(ScalarFunc::Unicode) => {
                        let reg_value = state.registers[*start_reg].borrow_mut();
                        state.registers[*dest] = exec_unicode(reg_value);
                        state.pc += 1;
                    }
                },
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
            }
        }
    }
}

fn get_new_rowid<R: Rng>(cursor: &mut Box<dyn Cursor>, mut rng: R) -> Result<CursorResult<i64>> {
    cursor.seek_to_last()?;
    let mut rowid = cursor.rowid()?.unwrap_or(0) + 1;
    if rowid > std::i64::MAX.try_into().unwrap() {
        let distribution = Uniform::from(1..=std::i64::MAX);
        let max_attempts = 100;
        for count in 0..max_attempts {
            rowid = distribution.sample(&mut rng).try_into().unwrap();
            match cursor.seek_rowid(rowid)? {
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
            Insn::RewindAwait { .. } | Insn::SorterSort { .. } => indent_count + 1,
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
        _ => reg.to_owned(),
    }
}

fn exec_upper(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Text(t) => Some(OwnedValue::Text(Rc::new(t.to_uppercase()))),
        t => Some(t.to_owned()),
    }
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

// Implements LIKE pattern matching.
fn exec_like(pattern: &str, text: &str) -> bool {
    let re = Regex::new(&pattern.replace('%', ".*").replace('_', ".").to_string()).unwrap();
    re.is_match(text)
}

fn exec_minmax<'a>(
    regs: Vec<&'a OwnedValue>,
    op: fn(&'a OwnedValue, &'a OwnedValue) -> &'a OwnedValue,
) -> Option<OwnedValue> {
    regs.into_iter().reduce(|a, b| op(a, b)).cloned()
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

#[cfg(test)]
mod tests {
    use super::{
        exec_abs, exec_if, exec_length, exec_like, exec_lower, exec_ltrim, exec_minmax,
        exec_random, exec_round, exec_rtrim, exec_substring, exec_trim, exec_unicode, exec_upper,
        get_new_rowid, Cursor, CursorResult, LimboError, OwnedRecord, OwnedValue, Result,
    };
    use mockall::{mock, predicate, predicate::*};
    use rand::{rngs::mock::StepRng, thread_rng};
    use std::{cell::Ref, rc::Rc};

    mock! {
        Cursor {
            fn seek_to_last(&mut self) -> Result<CursorResult<()>>;
            fn rowid(&self) -> Result<Option<u64>>;
            fn seek_rowid(&mut self, rowid: u64) -> Result<CursorResult<bool>>;
        }
    }

    impl Cursor for MockCursor {
        fn seek_to_last(&mut self) -> Result<CursorResult<()>> {
            return self.seek_to_last();
        }

        fn rowid(&self) -> Result<Option<u64>> {
            return self.rowid();
        }

        fn seek_rowid(&mut self, rowid: u64) -> Result<CursorResult<bool>> {
            return self.seek_rowid(rowid);
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
        mock.expect_seek_rowid()
            .with(predicate::always())
            .returning(|rowid| {
                if rowid == 50 {
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
        mock.expect_seek_rowid()
            .with(predicate::always())
            .return_once(|_| Ok(CursorResult::IO));

        let result = get_new_rowid(&mut (Box::new(mock) as Box<dyn Cursor>), thread_rng());
        assert!(matches!(result, Ok(CursorResult::IO)));

        // Test case 4: Failure to generate new rowid
        let mut mock = MockCursor::new();
        mock.expect_seek_to_last()
            .return_once(|| Ok(CursorResult::Ok(())));
        mock.expect_rowid()
            .return_once(|| Ok(Some(std::i64::MAX as u64)));
        mock.expect_seek_rowid()
            .with(predicate::always())
            .returning(|_| Ok(CursorResult::Ok(true)));

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
    fn test_minmax() {
        let min_fn = |a, b| if a < b { a } else { b };
        let max_fn = |a, b| if a > b { a } else { b };
        let input_int_vec = vec![&OwnedValue::Integer(-1), &OwnedValue::Integer(10)];
        assert_eq!(
            exec_minmax(input_int_vec.clone(), min_fn),
            Some(OwnedValue::Integer(-1))
        );
        assert_eq!(
            exec_minmax(input_int_vec.clone(), max_fn),
            Some(OwnedValue::Integer(10))
        );

        let str1 = OwnedValue::Text(Rc::new(String::from("A")));
        let str2 = OwnedValue::Text(Rc::new(String::from("z")));
        let input_str_vec = vec![&str2, &str1];
        assert_eq!(
            exec_minmax(input_str_vec.clone(), min_fn),
            Some(OwnedValue::Text(Rc::new(String::from("A"))))
        );
        assert_eq!(
            exec_minmax(input_str_vec.clone(), max_fn),
            Some(OwnedValue::Text(Rc::new(String::from("z"))))
        );

        let input_null_vec = vec![&OwnedValue::Null, &OwnedValue::Null];
        assert_eq!(
            exec_minmax(input_null_vec.clone(), min_fn),
            Some(OwnedValue::Null)
        );
        assert_eq!(
            exec_minmax(input_null_vec.clone(), max_fn),
            Some(OwnedValue::Null)
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
    fn test_like() {
        assert!(exec_like("a%", "aaaa"));
        assert!(exec_like("%a%a", "aaaa"));
        assert!(exec_like("%a.a", "aaaa"));
        assert!(exec_like("a.a%", "aaaa"));
        assert!(!exec_like("%a.ab", "aaaa"));
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
    }
}
