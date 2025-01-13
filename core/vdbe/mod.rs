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
mod datetime;
pub mod explain;
pub mod insn;
pub mod likeop;
pub mod sorter;

use crate::error::{LimboError, SQLITE_CONSTRAINT_PRIMARYKEY};
#[cfg(feature = "uuid")]
use crate::ext::{exec_ts_from_uuid7, exec_uuid, exec_uuidblob, exec_uuidstr, ExtFunc, UuidFunc};
use crate::function::{AggFunc, FuncCtx, MathFunc, MathFuncArity, ScalarFunc};
use crate::pseudo::PseudoCursor;
use crate::result::LimboResult;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::storage::{btree::BTreeCursor, pager::Pager};
use crate::types::{AggContext, CursorResult, OwnedRecord, OwnedValue, Record, SeekKey, SeekOp};
use crate::util::parse_schema_rows;
use crate::vdbe::builder::CursorType;
use crate::vdbe::insn::Insn;
#[cfg(feature = "json")]
use crate::{
    function::JsonFunc, json::get_json, json::json_array, json::json_array_length,
    json::json_arrow_extract, json::json_arrow_shift_extract, json::json_error_position,
    json::json_extract, json::json_type,
};
use crate::{Connection, Result, Rows, TransactionState, DATABASE_VERSION};
use datetime::{exec_date, exec_datetime_full, exec_julianday, exec_time, exec_unixepoch};
use insn::{
    exec_add, exec_bit_and, exec_bit_not, exec_bit_or, exec_divide, exec_multiply, exec_remainder,
    exec_subtract,
};
use likeop::{construct_like_escape_arg, exec_glob, exec_like_with_escape};
use rand::distributions::{Distribution, Uniform};
use rand::{thread_rng, Rng};
use regex::{Regex, RegexBuilder};
use sorter::Sorter;
use std::borrow::{Borrow, BorrowMut};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::{Rc, Weak};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Represents a target for a jump instruction.
/// Stores 32-bit ints to keep the enum word-sized.
pub enum BranchOffset {
    /// A label is a named location in the program.
    /// If there are references to it, it must always be resolved to an Offset
    /// via program.resolve_label().
    Label(i32),
    /// An offset is a direct index into the instruction list.
    Offset(InsnReference),
    /// A placeholder is a temporary value to satisfy the compiler.
    /// It must be set later.
    Placeholder,
}

impl BranchOffset {
    /// Returns true if the branch offset is a label.
    pub fn is_label(&self) -> bool {
        matches!(self, BranchOffset::Label(_))
    }

    /// Returns true if the branch offset is an offset.
    pub fn is_offset(&self) -> bool {
        matches!(self, BranchOffset::Offset(_))
    }

    /// Returns the offset value. Panics if the branch offset is a label or placeholder.
    pub fn to_offset_int(&self) -> InsnReference {
        match self {
            BranchOffset::Label(v) => unreachable!("Unresolved label: {}", v),
            BranchOffset::Offset(v) => *v,
            BranchOffset::Placeholder => unreachable!("Unresolved placeholder"),
        }
    }

    /// Returns the label value. Panics if the branch offset is an offset or placeholder.
    pub fn to_label_value(&self) -> i32 {
        match self {
            BranchOffset::Label(v) => *v,
            BranchOffset::Offset(_) => unreachable!("Offset cannot be converted to label value"),
            BranchOffset::Placeholder => unreachable!("Unresolved placeholder"),
        }
    }

    /// Returns the branch offset as a signed integer.
    /// Used in explain output, where we don't want to panic in case we have an unresolved
    /// label or placeholder.
    pub fn to_debug_int(&self) -> i32 {
        match self {
            BranchOffset::Label(v) => *v,
            BranchOffset::Offset(v) => *v as i32,
            BranchOffset::Placeholder => i32::MAX,
        }
    }

    /// Adds an integer value to the branch offset.
    /// Returns a new branch offset.
    /// Panics if the branch offset is a label or placeholder.
    pub fn add<N: Into<u32>>(self, n: N) -> BranchOffset {
        BranchOffset::Offset(self.to_offset_int() + n.into())
    }
}

pub type CursorID = usize;

pub type PageIdx = usize;

// Index of insn in list of insns
type InsnReference = u32;

pub enum StepResult<'a> {
    Done,
    IO,
    Row(Record<'a>),
    Interrupt,
    Busy,
}

/// If there is I/O, the instruction is restarted.
/// Evaluate a Result<CursorResult<T>>, if IO return Ok(StepResult::IO).
macro_rules! return_if_io {
    ($expr:expr) => {
        match $expr? {
            CursorResult::Ok(v) => v,
            CursorResult::IO => return Ok(StepResult::IO),
        }
    };
}

struct RegexCache {
    like: HashMap<String, Regex>,
    glob: HashMap<String, Regex>,
}

impl RegexCache {
    fn new() -> Self {
        Self {
            like: HashMap::new(),
            glob: HashMap::new(),
        }
    }
}

/// The program state describes the environment in which the program executes.
pub struct ProgramState {
    pub pc: InsnReference,
    btree_table_cursors: RefCell<BTreeMap<CursorID, BTreeCursor>>,
    btree_index_cursors: RefCell<BTreeMap<CursorID, BTreeCursor>>,
    pseudo_cursors: RefCell<BTreeMap<CursorID, PseudoCursor>>,
    sorter_cursors: RefCell<BTreeMap<CursorID, Sorter>>,
    registers: Vec<OwnedValue>,
    last_compare: Option<std::cmp::Ordering>,
    deferred_seek: Option<(CursorID, CursorID)>,
    ended_coroutine: HashMap<usize, bool>, // flag to indicate that a coroutine has ended (key is the yield register)
    regex_cache: RegexCache,
    interrupted: bool,
}

impl ProgramState {
    pub fn new(max_registers: usize) -> Self {
        let btree_table_cursors = RefCell::new(BTreeMap::new());
        let btree_index_cursors = RefCell::new(BTreeMap::new());
        let pseudo_cursors = RefCell::new(BTreeMap::new());
        let sorter_cursors = RefCell::new(BTreeMap::new());
        let mut registers = Vec::with_capacity(max_registers);
        registers.resize(max_registers, OwnedValue::Null);
        Self {
            pc: 0,
            btree_table_cursors,
            btree_index_cursors,
            pseudo_cursors,
            sorter_cursors,
            registers,
            last_compare: None,
            deferred_seek: None,
            ended_coroutine: HashMap::new(),
            regex_cache: RegexCache::new(),
            interrupted: false,
        }
    }

    pub fn column_count(&self) -> usize {
        self.registers.len()
    }

    pub fn column(&self, i: usize) -> Option<String> {
        Some(format!("{:?}", self.registers[i]))
    }

    pub fn interrupt(&mut self) {
        self.interrupted = true;
    }

    pub fn is_interrupted(&self) -> bool {
        self.interrupted
    }
}

macro_rules! must_be_btree_cursor {
    ($cursor_id:expr, $cursor_ref:expr, $btree_table_cursors:expr, $btree_index_cursors:expr, $insn_name:expr) => {{
        let (_, cursor_type) = $cursor_ref.get($cursor_id).unwrap();
        let cursor = match cursor_type {
            CursorType::BTreeTable(_) => $btree_table_cursors.get_mut(&$cursor_id).unwrap(),
            CursorType::BTreeIndex(_) => $btree_index_cursors.get_mut(&$cursor_id).unwrap(),
            CursorType::Pseudo(_) => panic!("{} on pseudo cursor", $insn_name),
            CursorType::Sorter => panic!("{} on sorter cursor", $insn_name),
        };
        cursor
    }};
}

#[derive(Debug)]
pub struct Program {
    pub max_registers: usize,
    pub insns: Vec<Insn>,
    pub cursor_ref: Vec<(Option<String>, CursorType)>,
    pub database_header: Rc<RefCell<DatabaseHeader>>,
    pub comments: HashMap<InsnReference, &'static str>,
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
            if state.is_interrupted() {
                return Ok(StepResult::Interrupt);
            }
            let insn = &self.insns[state.pc as usize];
            trace_insn(self, state.pc as InsnReference, insn);
            let mut btree_table_cursors = state.btree_table_cursors.borrow_mut();
            let mut btree_index_cursors = state.btree_index_cursors.borrow_mut();
            let mut pseudo_cursors = state.pseudo_cursors.borrow_mut();
            let mut sorter_cursors = state.sorter_cursors.borrow_mut();
            match insn {
                Insn::Init { target_pc } => {
                    assert!(target_pc.is_offset());
                    state.pc = target_pc.to_offset_int();
                }
                Insn::Add { lhs, rhs, dest } => {
                    state.registers[*dest] =
                        exec_add(&state.registers[*lhs], &state.registers[*rhs]);
                    state.pc += 1;
                }
                Insn::Subtract { lhs, rhs, dest } => {
                    state.registers[*dest] =
                        exec_subtract(&state.registers[*lhs], &state.registers[*rhs]);
                    state.pc += 1;
                }
                Insn::Multiply { lhs, rhs, dest } => {
                    state.registers[*dest] =
                        exec_multiply(&state.registers[*lhs], &state.registers[*rhs]);
                    state.pc += 1;
                }
                Insn::Divide { lhs, rhs, dest } => {
                    state.registers[*dest] =
                        exec_divide(&state.registers[*lhs], &state.registers[*rhs]);
                    state.pc += 1;
                }
                Insn::Remainder { lhs, rhs, dest } => {
                    state.registers[*dest] =
                        exec_remainder(&state.registers[*lhs], &state.registers[*rhs]);
                    state.pc += 1;
                }
                Insn::BitAnd { lhs, rhs, dest } => {
                    state.registers[*dest] =
                        exec_bit_and(&state.registers[*lhs], &state.registers[*rhs]);
                    state.pc += 1;
                }
                Insn::BitOr { lhs, rhs, dest } => {
                    state.registers[*dest] =
                        exec_bit_or(&state.registers[*lhs], &state.registers[*rhs]);
                    state.pc += 1;
                }
                Insn::BitNot { reg, dest } => {
                    state.registers[*dest] = exec_bit_not(&state.registers[*reg]);
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
                    let cursor = must_be_btree_cursor!(
                        *cursor_id,
                        self.cursor_ref,
                        btree_table_cursors,
                        btree_index_cursors,
                        "NullRow"
                    );
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
                    assert!(target_pc_lt.is_offset());
                    assert!(target_pc_eq.is_offset());
                    assert!(target_pc_gt.is_offset());
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
                    state.pc = target_pc.to_offset_int();
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
                    assert!(target_pc.is_offset());
                    let reg = *reg;
                    let target_pc = *target_pc;
                    match &state.registers[reg] {
                        OwnedValue::Integer(n) if *n > 0 => {
                            state.pc = target_pc.to_offset_int();
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
                    assert!(target_pc.is_offset());
                    let reg = *reg;
                    let target_pc = *target_pc;
                    match &state.registers[reg] {
                        OwnedValue::Null => {
                            state.pc += 1;
                        }
                        _ => {
                            state.pc = target_pc.to_offset_int();
                        }
                    }
                }

                Insn::Eq {
                    lhs,
                    rhs,
                    target_pc,
                } => {
                    assert!(target_pc.is_offset());
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc.to_offset_int();
                        }
                        _ => {
                            if state.registers[lhs] == state.registers[rhs] {
                                state.pc = target_pc.to_offset_int();
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
                    assert!(target_pc.is_offset());
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc.to_offset_int();
                        }
                        _ => {
                            if state.registers[lhs] != state.registers[rhs] {
                                state.pc = target_pc.to_offset_int();
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
                    assert!(target_pc.is_offset());
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc.to_offset_int();
                        }
                        _ => {
                            if state.registers[lhs] < state.registers[rhs] {
                                state.pc = target_pc.to_offset_int();
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
                    assert!(target_pc.is_offset());
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc.to_offset_int();
                        }
                        _ => {
                            if state.registers[lhs] <= state.registers[rhs] {
                                state.pc = target_pc.to_offset_int();
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
                    assert!(target_pc.is_offset());
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc.to_offset_int();
                        }
                        _ => {
                            if state.registers[lhs] > state.registers[rhs] {
                                state.pc = target_pc.to_offset_int();
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
                    assert!(target_pc.is_offset());
                    let lhs = *lhs;
                    let rhs = *rhs;
                    let target_pc = *target_pc;
                    match (&state.registers[lhs], &state.registers[rhs]) {
                        (_, OwnedValue::Null) | (OwnedValue::Null, _) => {
                            state.pc = target_pc.to_offset_int();
                        }
                        _ => {
                            if state.registers[lhs] >= state.registers[rhs] {
                                state.pc = target_pc.to_offset_int();
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
                    assert!(target_pc.is_offset());
                    if exec_if(&state.registers[*reg], &state.registers[*null_reg], false) {
                        state.pc = target_pc.to_offset_int();
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::IfNot {
                    reg,
                    target_pc,
                    null_reg,
                } => {
                    assert!(target_pc.is_offset());
                    if exec_if(&state.registers[*reg], &state.registers[*null_reg], true) {
                        state.pc = target_pc.to_offset_int();
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::OpenReadAsync {
                    cursor_id,
                    root_page,
                } => {
                    let (_, cursor_type) = self.cursor_ref.get(*cursor_id).unwrap();
                    let cursor =
                        BTreeCursor::new(pager.clone(), *root_page, self.database_header.clone());
                    match cursor_type {
                        CursorType::BTreeTable(_) => {
                            btree_table_cursors.insert(*cursor_id, cursor);
                        }
                        CursorType::BTreeIndex(_) => {
                            btree_index_cursors.insert(*cursor_id, cursor);
                        }
                        CursorType::Pseudo(_) => {
                            panic!("OpenReadAsync on pseudo cursor");
                        }
                        CursorType::Sorter => {
                            panic!("OpenReadAsync on sorter cursor");
                        }
                    }
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
                    let cursor = PseudoCursor::new();
                    pseudo_cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::RewindAsync { cursor_id } => {
                    let cursor = must_be_btree_cursor!(
                        *cursor_id,
                        self.cursor_ref,
                        btree_table_cursors,
                        btree_index_cursors,
                        "RewindAsync"
                    );
                    return_if_io!(cursor.rewind());
                    state.pc += 1;
                }
                Insn::LastAsync { cursor_id } => {
                    let cursor = must_be_btree_cursor!(
                        *cursor_id,
                        self.cursor_ref,
                        btree_table_cursors,
                        btree_index_cursors,
                        "LastAsync"
                    );
                    return_if_io!(cursor.last());
                    state.pc += 1;
                }
                Insn::LastAwait {
                    cursor_id,
                    pc_if_empty,
                } => {
                    assert!(pc_if_empty.is_offset());
                    let cursor = must_be_btree_cursor!(
                        *cursor_id,
                        self.cursor_ref,
                        btree_table_cursors,
                        btree_index_cursors,
                        "LastAwait"
                    );
                    cursor.wait_for_completion()?;
                    if cursor.is_empty() {
                        state.pc = pc_if_empty.to_offset_int();
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::RewindAwait {
                    cursor_id,
                    pc_if_empty,
                } => {
                    assert!(pc_if_empty.is_offset());
                    let cursor = must_be_btree_cursor!(
                        *cursor_id,
                        self.cursor_ref,
                        btree_table_cursors,
                        btree_index_cursors,
                        "RewindAwait"
                    );
                    cursor.wait_for_completion()?;
                    if cursor.is_empty() {
                        state.pc = pc_if_empty.to_offset_int();
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
                        let index_cursor = btree_index_cursors.get_mut(&index_cursor_id).unwrap();
                        let rowid = index_cursor.rowid()?;
                        let table_cursor = btree_table_cursors.get_mut(&table_cursor_id).unwrap();
                        match table_cursor.seek(SeekKey::TableRowId(rowid.unwrap()), SeekOp::EQ)? {
                            CursorResult::Ok(_) => {}
                            CursorResult::IO => {
                                state.deferred_seek = Some((index_cursor_id, table_cursor_id));
                                return Ok(StepResult::IO);
                            }
                        }
                    }
                    let (_, cursor_type) = self.cursor_ref.get(*cursor_id).unwrap();
                    match cursor_type {
                        CursorType::BTreeTable(_) | CursorType::BTreeIndex(_) => {
                            let cursor = must_be_btree_cursor!(
                                *cursor_id,
                                self.cursor_ref,
                                btree_table_cursors,
                                btree_index_cursors,
                                "Column"
                            );
                            let record = cursor.record()?;
                            if let Some(record) = record.as_ref() {
                                state.registers[*dest] = if cursor.get_null_flag() {
                                    OwnedValue::Null
                                } else {
                                    record.values[*column].clone()
                                };
                            } else {
                                state.registers[*dest] = OwnedValue::Null;
                            }
                        }
                        CursorType::Sorter => {
                            let cursor = sorter_cursors.get_mut(cursor_id).unwrap();
                            if let Some(record) = cursor.record() {
                                state.registers[*dest] = record.values[*column].clone();
                            } else {
                                state.registers[*dest] = OwnedValue::Null;
                            }
                        }
                        CursorType::Pseudo(_) => {
                            let cursor = pseudo_cursors.get_mut(cursor_id).unwrap();
                            if let Some(record) = cursor.record() {
                                state.registers[*dest] = record.values[*column].clone();
                            } else {
                                state.registers[*dest] = OwnedValue::Null;
                            }
                        }
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
                    let cursor = must_be_btree_cursor!(
                        *cursor_id,
                        self.cursor_ref,
                        btree_table_cursors,
                        btree_index_cursors,
                        "NextAsync"
                    );
                    cursor.set_null_flag(false);
                    return_if_io!(cursor.next());
                    state.pc += 1;
                }
                Insn::PrevAsync { cursor_id } => {
                    let cursor = must_be_btree_cursor!(
                        *cursor_id,
                        self.cursor_ref,
                        btree_table_cursors,
                        btree_index_cursors,
                        "PrevAsync"
                    );
                    cursor.set_null_flag(false);
                    return_if_io!(cursor.prev());
                    state.pc += 1;
                }
                Insn::PrevAwait {
                    cursor_id,
                    pc_if_next,
                } => {
                    assert!(pc_if_next.is_offset());
                    let cursor = must_be_btree_cursor!(
                        *cursor_id,
                        self.cursor_ref,
                        btree_table_cursors,
                        btree_index_cursors,
                        "PrevAwait"
                    );
                    cursor.wait_for_completion()?;
                    if !cursor.is_empty() {
                        state.pc = pc_if_next.to_offset_int();
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::NextAwait {
                    cursor_id,
                    pc_if_next,
                } => {
                    assert!(pc_if_next.is_offset());
                    let cursor = must_be_btree_cursor!(
                        *cursor_id,
                        self.cursor_ref,
                        btree_table_cursors,
                        btree_index_cursors,
                        "NextAwait"
                    );
                    cursor.wait_for_completion()?;
                    if !cursor.is_empty() {
                        state.pc = pc_if_next.to_offset_int();
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
                    let current_state = connection.transaction_state.borrow().clone();
                    let (new_transaction_state, updated) = match (&current_state, write) {
                        (crate::TransactionState::Write, true) => (TransactionState::Write, false),
                        (crate::TransactionState::Write, false) => (TransactionState::Write, false),
                        (crate::TransactionState::Read, true) => (TransactionState::Write, true),
                        (crate::TransactionState::Read, false) => (TransactionState::Read, false),
                        (crate::TransactionState::None, true) => (TransactionState::Write, true),
                        (crate::TransactionState::None, false) => (TransactionState::Read, true),
                    };

                    if updated && matches!(current_state, TransactionState::None) {
                        if let LimboResult::Busy = pager.begin_read_tx()? {
                            log::trace!("begin_read_tx busy");
                            return Ok(StepResult::Busy);
                        }
                    }

                    if updated && matches!(new_transaction_state, TransactionState::Write) {
                        if let LimboResult::Busy = pager.begin_write_tx()? {
                            log::trace!("begin_write_tx busy");
                            return Ok(StepResult::Busy);
                        }
                    }
                    if updated {
                        connection
                            .transaction_state
                            .replace(new_transaction_state.clone());
                    }
                    state.pc += 1;
                }
                Insn::Goto { target_pc } => {
                    assert!(target_pc.is_offset());
                    state.pc = target_pc.to_offset_int();
                }
                Insn::Gosub {
                    target_pc,
                    return_reg,
                } => {
                    assert!(target_pc.is_offset());
                    state.registers[*return_reg] = OwnedValue::Integer((state.pc + 1) as i64);
                    state.pc = target_pc.to_offset_int();
                }
                Insn::Return { return_reg } => {
                    if let OwnedValue::Integer(pc) = state.registers[*return_reg] {
                        let pc: u32 = pc
                            .try_into()
                            .unwrap_or_else(|_| panic!("Return register is negative: {}", pc));
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
                    state.registers[*dest] = OwnedValue::build_text(Rc::new(value.into()));
                    state.pc += 1;
                }
                Insn::Blob { value, dest } => {
                    state.registers[*dest] = OwnedValue::Blob(Rc::new(value.clone()));
                    state.pc += 1;
                }
                Insn::RowId { cursor_id, dest } => {
                    if let Some((index_cursor_id, table_cursor_id)) = state.deferred_seek.take() {
                        let index_cursor = btree_index_cursors.get_mut(&index_cursor_id).unwrap();
                        let rowid = index_cursor.rowid()?;
                        let table_cursor = btree_table_cursors.get_mut(&table_cursor_id).unwrap();
                        match table_cursor.seek(SeekKey::TableRowId(rowid.unwrap()), SeekOp::EQ)? {
                            CursorResult::Ok(_) => {}
                            CursorResult::IO => {
                                state.deferred_seek = Some((index_cursor_id, table_cursor_id));
                                return Ok(StepResult::IO);
                            }
                        }
                    }

                    let cursor = btree_table_cursors.get_mut(cursor_id).unwrap();
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
                    assert!(target_pc.is_offset());
                    let cursor = btree_table_cursors.get_mut(cursor_id).unwrap();
                    let rowid = match &state.registers[*src_reg] {
                        OwnedValue::Integer(rowid) => *rowid as u64,
                        OwnedValue::Null => {
                            state.pc = target_pc.to_offset_int();
                            continue;
                        }
                        other => {
                            return Err(LimboError::InternalError(
                                format!("SeekRowid: the value in the register is not an integer or NULL: {}", other)
                            ));
                        }
                    };
                    let found = return_if_io!(cursor.seek(SeekKey::TableRowId(rowid), SeekOp::EQ));
                    if !found {
                        state.pc = target_pc.to_offset_int();
                    } else {
                        state.pc += 1;
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
                    assert!(target_pc.is_offset());
                    if *is_index {
                        let cursor = btree_index_cursors.get_mut(cursor_id).unwrap();
                        let record_from_regs: OwnedRecord =
                            make_owned_record(&state.registers, start_reg, num_regs);
                        let found = return_if_io!(
                            cursor.seek(SeekKey::IndexKey(&record_from_regs), SeekOp::GE)
                        );
                        if !found {
                            state.pc = target_pc.to_offset_int();
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        let cursor = btree_table_cursors.get_mut(cursor_id).unwrap();
                        let rowid = match &state.registers[*start_reg] {
                            OwnedValue::Null => {
                                // All integer values are greater than null so we just rewind the cursor
                                return_if_io!(cursor.rewind());
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
                        let found =
                            return_if_io!(cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GE));
                        if !found {
                            state.pc = target_pc.to_offset_int();
                        } else {
                            state.pc += 1;
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
                    assert!(target_pc.is_offset());
                    if *is_index {
                        let cursor = btree_index_cursors.get_mut(cursor_id).unwrap();
                        let record_from_regs: OwnedRecord =
                            make_owned_record(&state.registers, start_reg, num_regs);
                        let found = return_if_io!(
                            cursor.seek(SeekKey::IndexKey(&record_from_regs), SeekOp::GT)
                        );
                        if !found {
                            state.pc = target_pc.to_offset_int();
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        let cursor = btree_table_cursors.get_mut(cursor_id).unwrap();
                        let rowid = match &state.registers[*start_reg] {
                            OwnedValue::Null => {
                                // All integer values are greater than null so we just rewind the cursor
                                return_if_io!(cursor.rewind());
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
                        let found =
                            return_if_io!(cursor.seek(SeekKey::TableRowId(rowid), SeekOp::GT));
                        if !found {
                            state.pc = target_pc.to_offset_int();
                        } else {
                            state.pc += 1;
                        }
                    }
                }
                Insn::IdxGE {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                } => {
                    assert!(target_pc.is_offset());
                    let cursor = btree_index_cursors.get_mut(cursor_id).unwrap();
                    let record_from_regs: OwnedRecord =
                        make_owned_record(&state.registers, start_reg, num_regs);
                    if let Some(ref idx_record) = *cursor.record()? {
                        // omit the rowid from the idx_record, which is the last value
                        if idx_record.values[..idx_record.values.len() - 1]
                            >= *record_from_regs.values
                        {
                            state.pc = target_pc.to_offset_int();
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        state.pc = target_pc.to_offset_int();
                    }
                }
                Insn::IdxGT {
                    cursor_id,
                    start_reg,
                    num_regs,
                    target_pc,
                } => {
                    assert!(target_pc.is_offset());
                    let cursor = btree_index_cursors.get_mut(cursor_id).unwrap();
                    let record_from_regs: OwnedRecord =
                        make_owned_record(&state.registers, start_reg, num_regs);
                    if let Some(ref idx_record) = *cursor.record()? {
                        // omit the rowid from the idx_record, which is the last value
                        if idx_record.values[..idx_record.values.len() - 1]
                            > *record_from_regs.values
                        {
                            state.pc = target_pc.to_offset_int();
                        } else {
                            state.pc += 1;
                        }
                    } else {
                        state.pc = target_pc.to_offset_int();
                    }
                }
                Insn::DecrJumpZero { reg, target_pc } => {
                    assert!(target_pc.is_offset());
                    match state.registers[*reg] {
                        OwnedValue::Integer(n) => {
                            let n = n - 1;
                            if n == 0 {
                                state.pc = target_pc.to_offset_int();
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
                            AggFunc::GroupConcat | AggFunc::StringAgg => {
                                OwnedValue::Agg(Box::new(AggContext::GroupConcat(
                                    OwnedValue::build_text(Rc::new("".to_string())),
                                )))
                            }
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
                                    if value.value > current_max.value {
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
                                    OwnedValue::Text(text),
                                ) => {
                                    if text.value < current_min.value {
                                        *current_min = text;
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
                    let cursor = sorter::Sorter::new(order);
                    sorter_cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::SorterData {
                    cursor_id,
                    dest_reg,
                    pseudo_cursor,
                } => {
                    let sorter_cursor = sorter_cursors.get_mut(cursor_id).unwrap();
                    let record = match sorter_cursor.record() {
                        Some(record) => record.clone(),
                        None => {
                            state.pc += 1;
                            continue;
                        }
                    };
                    state.registers[*dest_reg] = OwnedValue::Record(record.clone());
                    let pseudo_cursor = pseudo_cursors.get_mut(pseudo_cursor).unwrap();
                    pseudo_cursor.insert(record);
                    state.pc += 1;
                }
                Insn::SorterInsert {
                    cursor_id,
                    record_reg,
                } => {
                    let cursor = sorter_cursors.get_mut(cursor_id).unwrap();
                    let record = match &state.registers[*record_reg] {
                        OwnedValue::Record(record) => record,
                        _ => unreachable!("SorterInsert on non-record register"),
                    };
                    cursor.insert(record);
                    state.pc += 1;
                }
                Insn::SorterSort {
                    cursor_id,
                    pc_if_empty,
                } => {
                    if let Some(cursor) = sorter_cursors.get_mut(cursor_id) {
                        cursor.sort();
                        state.pc += 1;
                    } else {
                        state.pc = pc_if_empty.to_offset_int();
                    }
                }
                Insn::SorterNext {
                    cursor_id,
                    pc_if_next,
                } => {
                    assert!(pc_if_next.is_offset());
                    let cursor = sorter_cursors.get_mut(cursor_id).unwrap();
                    cursor.next();
                    if !cursor.is_empty() {
                        state.pc = pc_if_next.to_offset_int();
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
                        crate::function::Func::Json(json_func) => match json_func {
                            JsonFunc::Json => {
                                let json_value = &state.registers[*start_reg];
                                let json_str = get_json(json_value);
                                match json_str {
                                    Ok(json) => state.registers[*dest] = json,
                                    Err(e) => return Err(e),
                                }
                            }
                            JsonFunc::JsonArray => {
                                let reg_values =
                                    &state.registers[*start_reg..*start_reg + arg_count];

                                let json_array = json_array(reg_values);

                                match json_array {
                                    Ok(json) => state.registers[*dest] = json,
                                    Err(e) => return Err(e),
                                }
                            }
                            JsonFunc::JsonExtract => {
                                let result = match arg_count {
                                    0 => json_extract(&OwnedValue::Null, &[]),
                                    _ => {
                                        let val = &state.registers[*start_reg];
                                        let reg_values = &state.registers
                                            [*start_reg + 1..*start_reg + arg_count];

                                        json_extract(val, reg_values)
                                    }
                                };

                                match result {
                                    Ok(json) => state.registers[*dest] = json,
                                    Err(e) => return Err(e),
                                }
                            }
                            JsonFunc::JsonArrowExtract | JsonFunc::JsonArrowShiftExtract => {
                                assert_eq!(arg_count, 2);
                                let json = &state.registers[*start_reg];
                                let path = &state.registers[*start_reg + 1];
                                let json_func = match json_func {
                                    JsonFunc::JsonArrowExtract => json_arrow_extract,
                                    JsonFunc::JsonArrowShiftExtract => json_arrow_shift_extract,
                                    _ => unreachable!(),
                                };
                                let json_str = json_func(json, path);
                                match json_str {
                                    Ok(json) => state.registers[*dest] = json,
                                    Err(e) => return Err(e),
                                }
                            }
                            JsonFunc::JsonArrayLength | JsonFunc::JsonType => {
                                let json_value = &state.registers[*start_reg];
                                let path_value = if arg_count > 1 {
                                    Some(&state.registers[*start_reg + 1])
                                } else {
                                    None
                                };
                                let func_result = match json_func {
                                    JsonFunc::JsonArrayLength => {
                                        json_array_length(json_value, path_value)
                                    }
                                    JsonFunc::JsonType => json_type(json_value, path_value),
                                    _ => unreachable!(),
                                };

                                match func_result {
                                    Ok(result) => state.registers[*dest] = result,
                                    Err(e) => return Err(e),
                                }
                            }
                            JsonFunc::JsonErrorPosition => {
                                let json_value = &state.registers[*start_reg];
                                match json_error_position(json_value) {
                                    Ok(pos) => state.registers[*dest] = pos,
                                    Err(e) => return Err(e),
                                }
                            }
                        },
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
                                let result = exec_cast(&reg_value_argument, &reg_value_type.value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::Changes => {
                                let res = &self.connection.upgrade().unwrap().last_change;
                                let changes = res.get();
                                state.registers[*dest] = OwnedValue::Integer(changes);
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
                                        OwnedValue::Integer(exec_glob(
                                            cache,
                                            &pattern.value,
                                            &text.value,
                                        )
                                            as i64)
                                    }
                                    _ => {
                                        unreachable!("Like on non-text registers");
                                    }
                                };
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::IfNull => {}
                            ScalarFunc::Iif => {}
                            ScalarFunc::Instr => {
                                let reg_value = &state.registers[*start_reg];
                                let pattern_value = &state.registers[*start_reg + 1];
                                let result = exec_instr(reg_value, pattern_value);
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::LastInsertRowid => {
                                if let Some(conn) = self.connection.upgrade() {
                                    state.registers[*dest] =
                                        OwnedValue::Integer(conn.last_insert_rowid() as i64);
                                } else {
                                    state.registers[*dest] = OwnedValue::Null;
                                }
                            }
                            ScalarFunc::Like => {
                                let pattern = &state.registers[*start_reg];
                                let text = &state.registers[*start_reg + 1];

                                let result = match (pattern, text) {
                                    (OwnedValue::Text(pattern), OwnedValue::Text(text))
                                        if arg_count == 3 =>
                                    {
                                        let escape = match construct_like_escape_arg(
                                            &state.registers[*start_reg + 2],
                                        ) {
                                            Ok(x) => x,
                                            Err(e) => return Result::Err(e),
                                        };

                                        OwnedValue::Integer(exec_like_with_escape(
                                            &pattern.value,
                                            &text.value,
                                            escape,
                                        )
                                            as i64)
                                    }
                                    (OwnedValue::Text(pattern), OwnedValue::Text(text)) => {
                                        let cache = if *constant_mask > 0 {
                                            Some(&mut state.regex_cache.like)
                                        } else {
                                            None
                                        };
                                        OwnedValue::Integer(exec_like(
                                            cache,
                                            &pattern.value,
                                            &text.value,
                                        )
                                            as i64)
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
                            | ScalarFunc::OctetLength
                            | ScalarFunc::Typeof
                            | ScalarFunc::Unicode
                            | ScalarFunc::Quote
                            | ScalarFunc::RandomBlob
                            | ScalarFunc::Sign
                            | ScalarFunc::Soundex
                            | ScalarFunc::ZeroBlob => {
                                let reg_value = state.registers[*start_reg].borrow_mut();
                                let result = match scalar_func {
                                    ScalarFunc::Sign => exec_sign(reg_value),
                                    ScalarFunc::Abs => exec_abs(reg_value),
                                    ScalarFunc::Lower => exec_lower(reg_value),
                                    ScalarFunc::Upper => exec_upper(reg_value),
                                    ScalarFunc::Length => Some(exec_length(reg_value)),
                                    ScalarFunc::OctetLength => Some(exec_octet_length(reg_value)),
                                    ScalarFunc::Typeof => Some(exec_typeof(reg_value)),
                                    ScalarFunc::Unicode => Some(exec_unicode(reg_value)),
                                    ScalarFunc::Quote => Some(exec_quote(reg_value)),
                                    ScalarFunc::RandomBlob => Some(exec_randomblob(reg_value)),
                                    ScalarFunc::ZeroBlob => Some(exec_zeroblob(reg_value)),
                                    ScalarFunc::Soundex => Some(exec_soundex(reg_value)),
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
                                assert!(arg_count == 1 || arg_count == 2);
                                let precision_value = if arg_count > 1 {
                                    Some(state.registers[*start_reg + 1].clone())
                                } else {
                                    None
                                };
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
                            ScalarFunc::TotalChanges => {
                                let res = &self.connection.upgrade().unwrap().total_changes;
                                let total_changes = res.get();
                                state.registers[*dest] = OwnedValue::Integer(total_changes);
                            }
                            ScalarFunc::DateTime => {
                                let result = exec_datetime_full(
                                    &state.registers[*start_reg..*start_reg + arg_count],
                                );
                                state.registers[*dest] = result;
                            }
                            ScalarFunc::JulianDay => {
                                if *start_reg == 0 {
                                    let julianday: String = exec_julianday(
                                        &OwnedValue::build_text(Rc::new("now".to_string())),
                                    )?;
                                    state.registers[*dest] =
                                        OwnedValue::build_text(Rc::new(julianday));
                                } else {
                                    let datetime_value = &state.registers[*start_reg];
                                    let julianday = exec_julianday(datetime_value);
                                    match julianday {
                                        Ok(time) => {
                                            state.registers[*dest] =
                                                OwnedValue::build_text(Rc::new(time))
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
                            ScalarFunc::UnixEpoch => {
                                if *start_reg == 0 {
                                    let unixepoch: String = exec_unixepoch(
                                        &OwnedValue::build_text(Rc::new("now".to_string())),
                                    )?;
                                    state.registers[*dest] =
                                        OwnedValue::build_text(Rc::new(unixepoch));
                                } else {
                                    let datetime_value = &state.registers[*start_reg];
                                    let unixepoch = exec_unixepoch(datetime_value);
                                    match unixepoch {
                                        Ok(time) => {
                                            state.registers[*dest] =
                                                OwnedValue::build_text(Rc::new(time))
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
                                state.registers[*dest] = OwnedValue::build_text(Rc::new(version));
                            }
                            ScalarFunc::Replace => {
                                assert!(arg_count == 3);
                                let source = &state.registers[*start_reg];
                                let pattern = &state.registers[*start_reg + 1];
                                let replacement = &state.registers[*start_reg + 2];
                                state.registers[*dest] = exec_replace(source, pattern, replacement);
                            }
                        },
                        #[allow(unreachable_patterns)]
                        crate::function::Func::Extension(extfn) => match extfn {
                            #[cfg(feature = "uuid")]
                            ExtFunc::Uuid(uuidfn) => match uuidfn {
                                UuidFunc::Uuid4Str => {
                                    state.registers[*dest] = exec_uuid(uuidfn, None)?
                                }
                                UuidFunc::Uuid7 => match arg_count {
                                    0 => {
                                        state.registers[*dest] =
                                            exec_uuid(uuidfn, None).unwrap_or(OwnedValue::Null);
                                    }
                                    1 => {
                                        let reg_value = state.registers[*start_reg].borrow();
                                        state.registers[*dest] = exec_uuid(uuidfn, Some(reg_value))
                                            .unwrap_or(OwnedValue::Null);
                                    }
                                    _ => unreachable!(),
                                },
                                _ => {
                                    // remaining accept 1 arg
                                    let reg_value = state.registers[*start_reg].borrow();
                                    state.registers[*dest] = match uuidfn {
                                        UuidFunc::Uuid7TS => Some(exec_ts_from_uuid7(reg_value)),
                                        UuidFunc::UuidStr => exec_uuidstr(reg_value).ok(),
                                        UuidFunc::UuidBlob => exec_uuidblob(reg_value).ok(),
                                        _ => unreachable!(),
                                    }
                                    .unwrap_or(OwnedValue::Null);
                                }
                            },
                            _ => unreachable!(), // when more extension types are added
                        },
                        crate::function::Func::External(f) => {
                            let result = (f.func)(&[])?;
                            state.registers[*dest] = result;
                        }
                        crate::function::Func::Math(math_func) => match math_func.arity() {
                            MathFuncArity::Nullary => match math_func {
                                MathFunc::Pi => {
                                    state.registers[*dest] =
                                        OwnedValue::Float(std::f64::consts::PI);
                                }
                                _ => {
                                    unreachable!(
                                        "Unexpected mathematical Nullary function {:?}",
                                        math_func
                                    );
                                }
                            },

                            MathFuncArity::Unary => {
                                let reg_value = &state.registers[*start_reg];
                                let result = exec_math_unary(reg_value, math_func);
                                state.registers[*dest] = result;
                            }

                            MathFuncArity::Binary => {
                                let lhs = &state.registers[*start_reg];
                                let rhs = &state.registers[*start_reg + 1];
                                let result = exec_math_binary(lhs, rhs, math_func);
                                state.registers[*dest] = result;
                            }

                            MathFuncArity::UnaryOrBinary => match math_func {
                                MathFunc::Log => {
                                    let result = match arg_count {
                                        1 => {
                                            let arg = &state.registers[*start_reg];
                                            exec_math_log(arg, None)
                                        }
                                        2 => {
                                            let base = &state.registers[*start_reg];
                                            let arg = &state.registers[*start_reg + 1];
                                            exec_math_log(arg, Some(base))
                                        }
                                        _ => unreachable!(
                                            "{:?} function with unexpected number of arguments",
                                            math_func
                                        ),
                                    };
                                    state.registers[*dest] = result;
                                }
                                _ => unreachable!(
                                    "Unexpected mathematical UnaryOrBinary function {:?}",
                                    math_func
                                ),
                            },
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
                    assert!(jump_on_definition.is_offset());
                    let start_offset = start_offset.to_offset_int();
                    state.registers[*yield_reg] = OwnedValue::Integer(start_offset as i64);
                    state.ended_coroutine.insert(*yield_reg, false);
                    let jump_on_definition = jump_on_definition.to_offset_int();
                    state.pc = if jump_on_definition == 0 {
                        state.pc + 1
                    } else {
                        jump_on_definition
                    };
                }
                Insn::EndCoroutine { yield_reg } => {
                    if let OwnedValue::Integer(pc) = state.registers[*yield_reg] {
                        state.ended_coroutine.insert(*yield_reg, true);
                        let pc: u32 = pc
                            .try_into()
                            .unwrap_or_else(|_| panic!("EndCoroutine: pc overflow: {}", pc));
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
                        if *state
                            .ended_coroutine
                            .get(yield_reg)
                            .expect("coroutine not initialized")
                        {
                            state.pc = end_offset.to_offset_int();
                        } else {
                            let pc: u32 = pc
                                .try_into()
                                .unwrap_or_else(|_| panic!("Yield: pc overflow: {}", pc));
                            // swap the program counter with the value in the yield register
                            // this is the mechanism that allows jumping back and forth between the coroutine and the caller
                            (state.pc, state.registers[*yield_reg]) =
                                (pc, OwnedValue::Integer((state.pc + 1) as i64));
                        }
                    } else {
                        unreachable!(
                            "yield_reg {} contains non-integer value: {:?}",
                            *yield_reg, state.registers[*yield_reg]
                        );
                    }
                }
                Insn::InsertAsync {
                    cursor,
                    key_reg,
                    record_reg,
                    flag: _,
                } => {
                    let cursor = btree_table_cursors.get_mut(cursor).unwrap();
                    let record = match &state.registers[*record_reg] {
                        OwnedValue::Record(r) => r,
                        _ => unreachable!("Not a record! Cannot insert a non record value."),
                    };
                    let key = &state.registers[*key_reg];
                    return_if_io!(cursor.insert(key, record, true));
                    state.pc += 1;
                }
                Insn::InsertAwait { cursor_id } => {
                    let cursor = btree_table_cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    // Only update last_insert_rowid for regular table inserts, not schema modifications
                    if cursor.root_page() != 1 {
                        if let Some(rowid) = cursor.rowid()? {
                            if let Some(conn) = self.connection.upgrade() {
                                conn.update_last_rowid(rowid);
                                let prev_total_changes = conn.total_changes.get();
                                conn.last_change.set(1);
                                conn.total_changes.set(prev_total_changes + 1);
                            }
                        }
                    }
                    state.pc += 1;
                }
                Insn::DeleteAsync { cursor_id } => {
                    let cursor = btree_table_cursors.get_mut(cursor_id).unwrap();
                    return_if_io!(cursor.delete());
                    state.pc += 1;
                }
                Insn::DeleteAwait { cursor_id } => {
                    let cursor = btree_table_cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    state.pc += 1;
                }
                Insn::NewRowid {
                    cursor, rowid_reg, ..
                } => {
                    let cursor = btree_table_cursors.get_mut(cursor).unwrap();
                    // TODO: make io handle rng
                    let rowid = return_if_io!(get_new_rowid(cursor, thread_rng()));
                    state.registers[*rowid_reg] = OwnedValue::Integer(rowid);
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
                    let cursor = must_be_btree_cursor!(
                        *cursor,
                        self.cursor_ref,
                        btree_table_cursors,
                        btree_index_cursors,
                        "NotExists"
                    );
                    let exists = return_if_io!(cursor.exists(&state.registers[*rowid_reg]));
                    if exists {
                        state.pc += 1;
                    } else {
                        state.pc = target_pc.to_offset_int();
                    }
                }
                // this cursor may be reused for next insert
                // Update: tablemoveto is used to travers on not exists, on insert depending on flags if nonseek it traverses again.
                // If not there might be some optimizations obviously.
                Insn::OpenWriteAsync {
                    cursor_id,
                    root_page,
                } => {
                    let (_, cursor_type) = self.cursor_ref.get(*cursor_id).unwrap();
                    let is_index = cursor_type.is_index();
                    let cursor =
                        BTreeCursor::new(pager.clone(), *root_page, self.database_header.clone());
                    if is_index {
                        btree_index_cursors.insert(*cursor_id, cursor);
                    } else {
                        btree_table_cursors.insert(*cursor_id, cursor);
                    }
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
                Insn::CreateBtree { db, root, flags } => {
                    if *db > 0 {
                        // TODO: implement temp datbases
                        todo!("temp databases not implemented yet");
                    }
                    let mut cursor = Box::new(BTreeCursor::new(
                        pager.clone(),
                        0,
                        self.database_header.clone(),
                    ));

                    let root_page = cursor.btree_create(*flags);
                    state.registers[*root] = OwnedValue::Integer(root_page as i64);
                    state.pc += 1;
                }
                Insn::Close { cursor_id } => {
                    let (_, cursor_type) = self.cursor_ref.get(*cursor_id).unwrap();
                    match cursor_type {
                        CursorType::BTreeTable(_) => {
                            let _ = btree_table_cursors.remove(cursor_id);
                        }
                        CursorType::BTreeIndex(_) => {
                            let _ = btree_index_cursors.remove(cursor_id);
                        }
                        CursorType::Pseudo(_) => {
                            let _ = pseudo_cursors.remove(cursor_id);
                        }
                        CursorType::Sorter => {
                            let _ = sorter_cursors.remove(cursor_id);
                        }
                    }
                    state.pc += 1;
                }
                Insn::IsNull { src, target_pc } => {
                    if matches!(state.registers[*src], OwnedValue::Null) {
                        state.pc = target_pc.to_offset_int();
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
                    parse_schema_rows(Some(rows), &mut schema, conn.pager.io.clone())?;
                    state.pc += 1;
                }
            }
        }
    }
}

fn get_new_rowid<R: Rng>(cursor: &mut BTreeCursor, mut rng: R) -> Result<CursorResult<i64>> {
    match cursor.seek_to_last()? {
        CursorResult::Ok(()) => {}
        CursorResult::IO => return Ok(CursorResult::IO),
    }
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
            program.comments.get(&(addr as u32)).copied()
        )
    );
}

fn print_insn(program: &Program, addr: InsnReference, insn: &Insn, indent: String) {
    let s = explain::insn_to_str(
        program,
        addr,
        insn,
        indent,
        program.comments.get(&(addr as u32)).copied(),
    );
    println!("{}", s);
}

fn get_indent_count(indent_count: usize, curr_insn: &Insn, prev_insn: Option<&Insn>) -> usize {
    let indent_count = if let Some(insn) = prev_insn {
        match insn {
            Insn::RewindAwait { .. }
            | Insn::LastAwait { .. }
            | Insn::SorterSort { .. }
            | Insn::SeekGE { .. }
            | Insn::SeekGT { .. } => indent_count + 1,
            _ => indent_count,
        }
    } else {
        indent_count
    };

    match curr_insn {
        Insn::NextAsync { .. } | Insn::SorterNext { .. } | Insn::PrevAsync { .. } => {
            indent_count - 1
        }
        _ => indent_count,
    }
}

fn exec_lower(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Text(t) => Some(OwnedValue::build_text(Rc::new(t.value.to_lowercase()))),
        t => Some(t.to_owned()),
    }
}

fn exec_length(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
            OwnedValue::Integer(reg.to_string().chars().count() as i64)
        }
        OwnedValue::Blob(blob) => OwnedValue::Integer(blob.len() as i64),
        OwnedValue::Agg(aggctx) => exec_length(aggctx.final_value()),
        _ => reg.to_owned(),
    }
}

fn exec_octet_length(reg: &OwnedValue) -> OwnedValue {
    match reg {
        OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
            OwnedValue::Integer(reg.to_string().into_bytes().len() as i64)
        }
        OwnedValue::Blob(blob) => OwnedValue::Integer(blob.len() as i64),
        OwnedValue::Agg(aggctx) => exec_octet_length(aggctx.final_value()),
        _ => reg.to_owned(),
    }
}

fn exec_upper(reg: &OwnedValue) -> Option<OwnedValue> {
    match reg {
        OwnedValue::Text(t) => Some(OwnedValue::build_text(Rc::new(t.value.to_uppercase()))),
        t => Some(t.to_owned()),
    }
}

fn exec_concat(registers: &[OwnedValue]) -> OwnedValue {
    let mut result = String::new();
    for reg in registers {
        match reg {
            OwnedValue::Text(text) => result.push_str(&text.value),
            OwnedValue::Integer(i) => result.push_str(&i.to_string()),
            OwnedValue::Float(f) => result.push_str(&f.to_string()),
            OwnedValue::Agg(aggctx) => result.push_str(&aggctx.final_value().to_string()),
            OwnedValue::Null => continue,
            OwnedValue::Blob(_) => todo!("TODO concat blob"),
            OwnedValue::Record(_) => unreachable!(),
        }
    }
    OwnedValue::build_text(Rc::new(result))
}

fn exec_concat_ws(registers: &[OwnedValue]) -> OwnedValue {
    if registers.is_empty() {
        return OwnedValue::Null;
    }

    let separator = match &registers[0] {
        OwnedValue::Text(text) => text.value.clone(),
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
            OwnedValue::Text(text) => result.push_str(&text.value),
            OwnedValue::Integer(i) => result.push_str(&i.to_string()),
            OwnedValue::Float(f) => result.push_str(&f.to_string()),
            _ => continue,
        }
    }

    OwnedValue::build_text(Rc::new(result))
}

fn exec_sign(reg: &OwnedValue) -> Option<OwnedValue> {
    let num = match reg {
        OwnedValue::Integer(i) => *i as f64,
        OwnedValue::Float(f) => *f,
        OwnedValue::Text(s) => {
            if let Ok(i) = s.value.parse::<i64>() {
                i as f64
            } else if let Ok(f) = s.value.parse::<f64>() {
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

/// Generates the Soundex code for a given word
pub fn exec_soundex(reg: &OwnedValue) -> OwnedValue {
    let s = match reg {
        OwnedValue::Null => return OwnedValue::build_text(Rc::new("?000".to_string())),
        OwnedValue::Text(s) => {
            // return ?000 if non ASCII alphabet character is found
            if !s.value.chars().all(|c| c.is_ascii_alphabetic()) {
                return OwnedValue::build_text(Rc::new("?000".to_string()));
            }
            s.clone()
        }
        _ => return OwnedValue::build_text(Rc::new("?000".to_string())), // For unsupported types, return NULL
    };

    // Remove numbers and spaces
    let word: String = s
        .value
        .chars()
        .filter(|c| !c.is_ascii_digit())
        .collect::<String>()
        .replace(" ", "");
    if word.is_empty() {
        return OwnedValue::build_text(Rc::new("0000".to_string()));
    }

    let soundex_code = |c| match c {
        'b' | 'f' | 'p' | 'v' => Some('1'),
        'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => Some('2'),
        'd' | 't' => Some('3'),
        'l' => Some('4'),
        'm' | 'n' => Some('5'),
        'r' => Some('6'),
        _ => None,
    };

    // Convert the word to lowercase for consistent lookups
    let word = word.to_lowercase();
    let first_letter = word.chars().next().unwrap();

    // Remove all occurrences of 'h' and 'w' except the first letter
    let code: String = word
        .chars()
        .skip(1)
        .filter(|&ch| ch != 'h' && ch != 'w')
        .fold(first_letter.to_string(), |mut acc, ch| {
            acc.push(ch);
            acc
        });

    // Replace consonants with digits based on Soundex mapping
    let tmp: String = code
        .chars()
        .map(|ch| match soundex_code(ch) {
            Some(code) => code.to_string(),
            None => ch.to_string(),
        })
        .collect();

    // Remove adjacent same digits
    let tmp = tmp.chars().fold(String::new(), |mut acc, ch| {
        if !acc.ends_with(ch) {
            acc.push(ch);
        }
        acc
    });

    // Remove all occurrences of a, e, i, o, u, y except the first letter
    let mut result = tmp
        .chars()
        .enumerate()
        .filter(|(i, ch)| *i == 0 || !matches!(ch, 'a' | 'e' | 'i' | 'o' | 'u' | 'y'))
        .map(|(_, ch)| ch)
        .collect::<String>();

    // If the first symbol is a digit, replace it with the saved first letter
    if let Some(first_digit) = result.chars().next() {
        if first_digit.is_ascii_digit() {
            result.replace_range(0..1, &first_letter.to_string());
        }
    }

    // Append zeros if the result contains less than 4 characters
    while result.len() < 4 {
        result.push('0');
    }

    // Retain the first 4 characters and convert to uppercase
    result.truncate(4);
    OwnedValue::build_text(Rc::new(result.to_uppercase()))
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
        OwnedValue::Text(t) => t.value.parse().unwrap_or(1),
        _ => 1,
    }
    .max(1) as usize;

    let mut blob: Vec<u8> = vec![0; length];
    getrandom::getrandom(&mut blob).expect("Failed to generate random blob");
    OwnedValue::Blob(Rc::new(blob))
}

fn exec_quote(value: &OwnedValue) -> OwnedValue {
    match value {
        OwnedValue::Null => OwnedValue::build_text(OwnedValue::Null.to_string().into()),
        OwnedValue::Integer(_) | OwnedValue::Float(_) => value.to_owned(),
        OwnedValue::Blob(_) => todo!(),
        OwnedValue::Text(s) => {
            let mut quoted = String::with_capacity(s.value.len() + 2);
            quoted.push('\'');
            for c in s.value.chars() {
                if c == '\0' {
                    break;
                } else {
                    quoted.push(c);
                }
            }
            quoted.push('\'');
            OwnedValue::build_text(Rc::new(quoted))
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
    OwnedValue::build_text(Rc::new(result))
}

fn construct_like_regex(pattern: &str) -> Regex {
    let mut regex_pattern = String::with_capacity(pattern.len() * 2);

    regex_pattern.push('^');

    for c in pattern.chars() {
        match c {
            '\\' => regex_pattern.push_str("\\\\"),
            '%' => regex_pattern.push_str(".*"),
            '_' => regex_pattern.push('.'),
            ch => {
                if regex_syntax::is_meta_character(c) {
                    regex_pattern.push('\\');
                }
                regex_pattern.push(ch);
            }
        }
    }

    regex_pattern.push('$');

    RegexBuilder::new(&regex_pattern)
        .case_insensitive(true)
        .dot_matches_new_line(true)
        .build()
        .unwrap()
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
        let str_len = str.value.len();

        if start > str_len {
            return OwnedValue::build_text(Rc::new("".to_string()));
        }

        let start_idx = start - 1;
        let end = if *length != -1 {
            start_idx + *length as usize
        } else {
            str_len
        };
        let substring = &str.value[start_idx..end.min(str_len)];

        OwnedValue::build_text(Rc::new(substring.to_string()))
    } else if let (OwnedValue::Text(str), OwnedValue::Integer(start)) = (str_value, start_value) {
        let start = *start as usize;
        let str_len = str.value.len();

        if start > str_len {
            return OwnedValue::build_text(Rc::new("".to_string()));
        }

        let start_idx = start - 1;
        let substring = &str.value[start_idx..str_len];

        OwnedValue::build_text(Rc::new(substring.to_string()))
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
        OwnedValue::Text(s) => s.value.as_str(),
        _ => {
            reg_str = reg.to_string();
            reg_str.as_str()
        }
    };

    let pattern_str;
    let pattern = match pattern {
        OwnedValue::Text(s) => s.value.as_str(),
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
        OwnedValue::Null => OwnedValue::build_text(Rc::new("null".to_string())),
        OwnedValue::Integer(_) => OwnedValue::build_text(Rc::new("integer".to_string())),
        OwnedValue::Float(_) => OwnedValue::build_text(Rc::new("real".to_string())),
        OwnedValue::Text(_) => OwnedValue::build_text(Rc::new("text".to_string())),
        OwnedValue::Blob(_) => OwnedValue::build_text(Rc::new("blob".to_string())),
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
            OwnedValue::build_text(Rc::new(hex::encode_upper(text)))
        }
        _ => OwnedValue::Null,
    }
}

fn exec_unhex(reg: &OwnedValue, ignored_chars: Option<&OwnedValue>) -> OwnedValue {
    match reg {
        OwnedValue::Null => OwnedValue::Null,
        _ => match ignored_chars {
            None => match hex::decode(reg.to_string()) {
                Ok(bytes) => OwnedValue::Blob(Rc::new(bytes)),
                Err(_) => OwnedValue::Null,
            },
            Some(ignore) => match ignore {
                OwnedValue::Text(_) => {
                    let pat = ignore.to_string();
                    let trimmed = reg
                        .to_string()
                        .trim_start_matches(|x| pat.contains(x))
                        .trim_end_matches(|x| pat.contains(x))
                        .to_string();
                    match hex::decode(trimmed) {
                        Ok(bytes) => OwnedValue::Blob(Rc::new(bytes)),
                        Err(_) => OwnedValue::Null,
                    }
                }
                _ => OwnedValue::Null,
            },
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

fn _to_float(reg: &OwnedValue) -> f64 {
    match reg {
        OwnedValue::Text(x) => x.value.parse().unwrap_or(0.0),
        OwnedValue::Integer(x) => *x as f64,
        OwnedValue::Float(x) => *x,
        _ => 0.0,
    }
}

fn exec_round(reg: &OwnedValue, precision: Option<OwnedValue>) -> OwnedValue {
    let precision = match precision {
        Some(OwnedValue::Text(x)) => x.value.parse().unwrap_or(0.0),
        Some(OwnedValue::Integer(x)) => x as f64,
        Some(OwnedValue::Float(x)) => x,
        Some(OwnedValue::Null) => return OwnedValue::Null,
        _ => 0.0,
    };

    let reg = match reg {
        OwnedValue::Agg(ctx) => _to_float(ctx.final_value()),
        _ => _to_float(reg),
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
                OwnedValue::build_text(Rc::new(
                    reg.to_string().trim_matches(&pattern_chars[..]).to_string(),
                ))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => OwnedValue::build_text(Rc::new(t.value.trim().to_string())),
        (reg, _) => reg.to_owned(),
    }
}

// Implements LTRIM pattern matching.
fn exec_ltrim(reg: &OwnedValue, pattern: Option<OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::build_text(Rc::new(
                    reg.to_string()
                        .trim_start_matches(&pattern_chars[..])
                        .to_string(),
                ))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => {
            OwnedValue::build_text(Rc::new(t.value.trim_start().to_string()))
        }
        (reg, _) => reg.to_owned(),
    }
}

// Implements RTRIM pattern matching.
fn exec_rtrim(reg: &OwnedValue, pattern: Option<OwnedValue>) -> OwnedValue {
    match (reg, pattern) {
        (reg, Some(pattern)) => match reg {
            OwnedValue::Text(_) | OwnedValue::Integer(_) | OwnedValue::Float(_) => {
                let pattern_chars: Vec<char> = pattern.to_string().chars().collect();
                OwnedValue::build_text(Rc::new(
                    reg.to_string()
                        .trim_end_matches(&pattern_chars[..])
                        .to_string(),
                ))
            }
            _ => reg.to_owned(),
        },
        (OwnedValue::Text(t), None) => {
            OwnedValue::build_text(Rc::new(t.value.trim_end().to_string()))
        }
        (reg, _) => reg.to_owned(),
    }
}

fn exec_zeroblob(req: &OwnedValue) -> OwnedValue {
    let length: i64 = match req {
        OwnedValue::Integer(i) => *i,
        OwnedValue::Float(f) => *f as i64,
        OwnedValue::Text(s) => s.value.parse().unwrap_or(0),
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
        Affinity::Blob => {
            // Convert to TEXT first, then interpret as BLOB
            // TODO: handle encoding
            let text = value.to_string();
            OwnedValue::Blob(Rc::new(text.into_bytes()))
        }
        // TEXT To cast a BLOB value to TEXT, the sequence of bytes that make up the BLOB is interpreted as text encoded using the database encoding.
        // Casting an INTEGER or REAL value into TEXT renders the value as if via sqlite3_snprintf() except that the resulting TEXT uses the encoding of the database connection.
        Affinity::Text => {
            // Convert everything to text representation
            // TODO: handle encoding and whatever sqlite3_snprintf does
            OwnedValue::build_text(Rc::new(value.to_string()))
        }
        Affinity::Real => match value {
            OwnedValue::Blob(b) => {
                // Convert BLOB to TEXT first
                let text = String::from_utf8_lossy(b);
                cast_text_to_real(&text)
            }
            OwnedValue::Text(t) => cast_text_to_real(&t.value),
            OwnedValue::Integer(i) => OwnedValue::Float(*i as f64),
            OwnedValue::Float(f) => OwnedValue::Float(*f),
            _ => OwnedValue::Float(0.0),
        },
        Affinity::Integer => match value {
            OwnedValue::Blob(b) => {
                // Convert BLOB to TEXT first
                let text = String::from_utf8_lossy(b);
                cast_text_to_integer(&text)
            }
            OwnedValue::Text(t) => cast_text_to_integer(&t.value),
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
        Affinity::Numeric => match value {
            OwnedValue::Blob(b) => {
                let text = String::from_utf8_lossy(b);
                cast_text_to_numeric(&text)
            }
            OwnedValue::Text(t) => cast_text_to_numeric(&t.value),
            OwnedValue::Integer(i) => OwnedValue::Integer(*i),
            OwnedValue::Float(f) => OwnedValue::Float(*f),
            _ => value.clone(), // TODO probably wrong
        },
    }
}

fn exec_replace(source: &OwnedValue, pattern: &OwnedValue, replacement: &OwnedValue) -> OwnedValue {
    // The replace(X,Y,Z) function returns a string formed by substituting string Z for every occurrence of
    // string Y in string X. The BINARY collating sequence is used for comparisons. If Y is an empty string
    // then return X unchanged. If Z is not initially a string, it is cast to a UTF-8 string prior to processing.

    // If any of the arguments is NULL, the result is NULL.
    if matches!(source, OwnedValue::Null)
        || matches!(pattern, OwnedValue::Null)
        || matches!(replacement, OwnedValue::Null)
    {
        return OwnedValue::Null;
    }

    let source = exec_cast(source, "TEXT");
    let pattern = exec_cast(pattern, "TEXT");
    let replacement = exec_cast(replacement, "TEXT");

    // If any of the casts failed, panic as text casting is not expected to fail.
    match (&source, &pattern, &replacement) {
        (OwnedValue::Text(source), OwnedValue::Text(pattern), OwnedValue::Text(replacement)) => {
            if pattern.value.is_empty() {
                return OwnedValue::build_text(source.value.clone());
            }

            let result = source
                .value
                .replace(pattern.value.as_str(), &replacement.value);
            OwnedValue::build_text(Rc::new(result))
        }
        _ => unreachable!("text cast should never fail"),
    }
}

enum Affinity {
    Integer,
    Text,
    Blob,
    Real,
    Numeric,
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
        return Affinity::Integer;
    }

    // Rule 2: CHAR/CLOB/TEXT -> TEXT affinity
    if datatype.contains("CHAR") || datatype.contains("CLOB") || datatype.contains("TEXT") {
        return Affinity::Text;
    }

    // Rule 3: BLOB or empty -> BLOB affinity (historically called NONE)
    if datatype.contains("BLOB") || datatype.is_empty() {
        return Affinity::Blob;
    }

    // Rule 4: REAL/FLOA/DOUB -> REAL affinity
    if datatype.contains("REAL") || datatype.contains("FLOA") || datatype.contains("DOUB") {
        return Affinity::Real;
    }

    // Rule 5: Otherwise -> NUMERIC affinity
    Affinity::Numeric
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

/// NUMERIC Casting a TEXT or BLOB value into NUMERIC yields either an INTEGER or a REAL result.
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

fn to_f64(reg: &OwnedValue) -> Option<f64> {
    match reg {
        OwnedValue::Integer(i) => Some(*i as f64),
        OwnedValue::Float(f) => Some(*f),
        OwnedValue::Text(t) => t.value.parse::<f64>().ok(),
        OwnedValue::Agg(ctx) => to_f64(ctx.final_value()),
        _ => None,
    }
}

fn exec_math_unary(reg: &OwnedValue, function: &MathFunc) -> OwnedValue {
    // In case of some functions and integer input, return the input as is
    if let OwnedValue::Integer(_) = reg {
        if matches! { function, MathFunc::Ceil | MathFunc::Ceiling | MathFunc::Floor | MathFunc::Trunc }
        {
            return reg.clone();
        }
    }

    let f = match to_f64(reg) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let result = match function {
        MathFunc::Acos => f.acos(),
        MathFunc::Acosh => f.acosh(),
        MathFunc::Asin => f.asin(),
        MathFunc::Asinh => f.asinh(),
        MathFunc::Atan => f.atan(),
        MathFunc::Atanh => f.atanh(),
        MathFunc::Ceil | MathFunc::Ceiling => f.ceil(),
        MathFunc::Cos => f.cos(),
        MathFunc::Cosh => f.cosh(),
        MathFunc::Degrees => f.to_degrees(),
        MathFunc::Exp => f.exp(),
        MathFunc::Floor => f.floor(),
        MathFunc::Ln => f.ln(),
        MathFunc::Log10 => f.log10(),
        MathFunc::Log2 => f.log2(),
        MathFunc::Radians => f.to_radians(),
        MathFunc::Sin => f.sin(),
        MathFunc::Sinh => f.sinh(),
        MathFunc::Sqrt => f.sqrt(),
        MathFunc::Tan => f.tan(),
        MathFunc::Tanh => f.tanh(),
        MathFunc::Trunc => f.trunc(),
        _ => unreachable!("Unexpected mathematical unary function {:?}", function),
    };

    if result.is_nan() {
        OwnedValue::Null
    } else {
        OwnedValue::Float(result)
    }
}

fn exec_math_binary(lhs: &OwnedValue, rhs: &OwnedValue, function: &MathFunc) -> OwnedValue {
    let lhs = match to_f64(lhs) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let rhs = match to_f64(rhs) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let result = match function {
        MathFunc::Atan2 => lhs.atan2(rhs),
        MathFunc::Mod => lhs % rhs,
        MathFunc::Pow | MathFunc::Power => lhs.powf(rhs),
        _ => unreachable!("Unexpected mathematical binary function {:?}", function),
    };

    if result.is_nan() {
        OwnedValue::Null
    } else {
        OwnedValue::Float(result)
    }
}

fn exec_math_log(arg: &OwnedValue, base: Option<&OwnedValue>) -> OwnedValue {
    let f = match to_f64(arg) {
        Some(f) => f,
        None => return OwnedValue::Null,
    };

    let base = match base {
        Some(base) => match to_f64(base) {
            Some(f) => f,
            None => return OwnedValue::Null,
        },
        None => 10.0,
    };

    if f <= 0.0 || base <= 0.0 || base == 1.0 {
        return OwnedValue::Null;
    }

    OwnedValue::Float(f.log(base))
}

#[cfg(test)]
mod tests {
    use crate::vdbe::exec_replace;

    use super::{
        exec_abs, exec_char, exec_hex, exec_if, exec_instr, exec_length, exec_like, exec_lower,
        exec_ltrim, exec_max, exec_min, exec_nullif, exec_quote, exec_random, exec_randomblob,
        exec_round, exec_rtrim, exec_sign, exec_soundex, exec_substring, exec_trim, exec_typeof,
        exec_unhex, exec_unicode, exec_upper, exec_zeroblob, execute_sqlite_version, AggContext,
        OwnedValue,
    };
    use std::{collections::HashMap, rc::Rc};

    #[test]
    fn test_length() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
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
        let input = OwnedValue::build_text(Rc::new(String::from("abc\0edf")));
        let expected = OwnedValue::build_text(Rc::new(String::from("'abc'")));
        assert_eq!(exec_quote(&input), expected);

        let input = OwnedValue::Integer(123);
        let expected = OwnedValue::Integer(123);
        assert_eq!(exec_quote(&input), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("hello''world")));
        let expected = OwnedValue::build_text(Rc::new(String::from("'hello''world'")));
        assert_eq!(exec_quote(&input), expected);
    }

    #[test]
    fn test_typeof() {
        let input = OwnedValue::Null;
        let expected: OwnedValue = OwnedValue::build_text(Rc::new("null".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Integer(123);
        let expected: OwnedValue = OwnedValue::build_text(Rc::new("integer".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Float(123.456);
        let expected: OwnedValue = OwnedValue::build_text(Rc::new("real".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::build_text(Rc::new("hello".to_string()));
        let expected: OwnedValue = OwnedValue::build_text(Rc::new("text".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Blob(Rc::new("limbo".as_bytes().to_vec()));
        let expected: OwnedValue = OwnedValue::build_text(Rc::new("blob".to_string()));
        assert_eq!(exec_typeof(&input), expected);

        let input = OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Integer(123))));
        let expected = OwnedValue::build_text(Rc::new("integer".to_string()));
        assert_eq!(exec_typeof(&input), expected);
    }

    #[test]
    fn test_unicode() {
        assert_eq!(
            exec_unicode(&OwnedValue::build_text(Rc::new("a".to_string()))),
            OwnedValue::Integer(97)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::build_text(Rc::new("😊".to_string()))),
            OwnedValue::Integer(128522)
        );
        assert_eq!(
            exec_unicode(&OwnedValue::build_text(Rc::new("".to_string()))),
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

        let str1 = OwnedValue::build_text(Rc::new(String::from("A")));
        let str2 = OwnedValue::build_text(Rc::new(String::from("z")));
        let input_str_vec = vec![&str2, &str1];
        assert_eq!(
            exec_min(input_str_vec.clone()),
            OwnedValue::build_text(Rc::new(String::from("A")))
        );
        assert_eq!(
            exec_max(input_str_vec.clone()),
            OwnedValue::build_text(Rc::new(String::from("z")))
        );

        let input_null_vec = vec![&OwnedValue::Null, &OwnedValue::Null];
        assert_eq!(exec_min(input_null_vec.clone()), OwnedValue::Null);
        assert_eq!(exec_max(input_null_vec.clone()), OwnedValue::Null);

        let input_mixed_vec = vec![&OwnedValue::Integer(10), &str1];
        assert_eq!(exec_min(input_mixed_vec.clone()), OwnedValue::Integer(10));
        assert_eq!(
            exec_max(input_mixed_vec.clone()),
            OwnedValue::build_text(Rc::new(String::from("A")))
        );
    }

    #[test]
    fn test_trim() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("Bob and Alice")));
        assert_eq!(exec_trim(&input_str, None), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("Bob and")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("Alice")));
        assert_eq!(exec_trim(&input_str, Some(pattern_str)), expected_str);
    }

    #[test]
    fn test_ltrim() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("Bob and Alice     ")));
        assert_eq!(exec_ltrim(&input_str, None), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("Bob and")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("Alice     ")));
        assert_eq!(exec_ltrim(&input_str, Some(pattern_str)), expected_str);
    }

    #[test]
    fn test_rtrim() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice")));
        assert_eq!(exec_rtrim(&input_str, None), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("Bob and")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice")));
        assert_eq!(exec_rtrim(&input_str, Some(pattern_str)), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("     Bob and Alice     ")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("and Alice")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("     Bob")));
        assert_eq!(exec_rtrim(&input_str, Some(pattern_str)), expected_str);
    }

    #[test]
    fn test_soundex() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("Pfister")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("P236")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("husobee")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("H210")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Tymczak")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("T522")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Ashcraft")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("A261")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Robert")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("R163")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Rupert")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("R163")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Rubin")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("R150")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Kant")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("K530")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("Knuth")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("K530")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("x")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("X000")));
        assert_eq!(exec_soundex(&input_str), expected_str);

        let input_str = OwnedValue::build_text(Rc::new(String::from("闪电五连鞭")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("?000")));
        assert_eq!(exec_soundex(&input_str), expected_str);
    }

    #[test]
    fn test_upper_case() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("Limbo")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("LIMBO")));
        assert_eq!(exec_upper(&input_str).unwrap(), expected_str);

        let input_int = OwnedValue::Integer(10);
        assert_eq!(exec_upper(&input_int).unwrap(), input_int);
        assert_eq!(exec_upper(&OwnedValue::Null).unwrap(), OwnedValue::Null)
    }

    #[test]
    fn test_lower_case() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("Limbo")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("limbo")));
        assert_eq!(exec_lower(&input_str).unwrap(), expected_str);

        let input_int = OwnedValue::Integer(10);
        assert_eq!(exec_lower(&input_int).unwrap(), input_int);
        assert_eq!(exec_lower(&OwnedValue::Null).unwrap(), OwnedValue::Null)
    }

    #[test]
    fn test_hex() {
        let input_str = OwnedValue::build_text(Rc::new("limbo".to_string()));
        let expected_val = OwnedValue::build_text(Rc::new(String::from("6C696D626F")));
        assert_eq!(exec_hex(&input_str), expected_val);

        let input_int = OwnedValue::Integer(100);
        let expected_val = OwnedValue::build_text(Rc::new(String::from("313030")));
        assert_eq!(exec_hex(&input_int), expected_val);

        let input_float = OwnedValue::Float(12.34);
        let expected_val = OwnedValue::build_text(Rc::new(String::from("31322E3334")));
        assert_eq!(exec_hex(&input_float), expected_val);
    }

    #[test]
    fn test_unhex() {
        let input = OwnedValue::build_text(Rc::new(String::from("6F")));
        let expected = OwnedValue::Blob(Rc::new(vec![0x6f]));
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("6f")));
        let expected = OwnedValue::Blob(Rc::new(vec![0x6f]));
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("611")));
        let expected = OwnedValue::Null;
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("")));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_unhex(&input, None), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("61x")));
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
            exec_abs(&OwnedValue::build_text(Rc::new(String::from("a")))).unwrap(),
            OwnedValue::Float(0.0)
        );
        assert_eq!(exec_abs(&OwnedValue::Null).unwrap(), OwnedValue::Null);
    }

    #[test]
    fn test_char() {
        assert_eq!(
            exec_char(vec![OwnedValue::Integer(108), OwnedValue::Integer(105)]),
            OwnedValue::build_text(Rc::new("li".to_string()))
        );
        assert_eq!(
            exec_char(vec![]),
            OwnedValue::build_text(Rc::new("".to_string()))
        );
        assert_eq!(
            exec_char(vec![OwnedValue::Null]),
            OwnedValue::build_text(Rc::new("".to_string()))
        );
        assert_eq!(
            exec_char(vec![OwnedValue::build_text(Rc::new("a".to_string()))]),
            OwnedValue::build_text(Rc::new("".to_string()))
        );
    }

    #[test]
    fn test_like_with_escape_or_regexmeta_chars() {
        assert!(exec_like(None, r#"\%A"#, r#"\A"#));
        assert!(exec_like(None, "%a%a", "aaaa"));
    }

    #[test]
    fn test_like_no_cache() {
        assert!(exec_like(None, "a%", "aaaa"));
        assert!(exec_like(None, "%a%a", "aaaa"));
        assert!(!exec_like(None, "%a.a", "aaaa"));
        assert!(!exec_like(None, "a.a%", "aaaa"));
        assert!(!exec_like(None, "%a.ab", "aaaa"));
    }

    #[test]
    fn test_like_with_cache() {
        let mut cache = HashMap::new();
        assert!(exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "a.a%", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "%a.ab", "aaaa"));

        // again after values have been cached
        assert!(exec_like(Some(&mut cache), "a%", "aaaa"));
        assert!(exec_like(Some(&mut cache), "%a%a", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "%a.a", "aaaa"));
        assert!(!exec_like(Some(&mut cache), "a.a%", "aaaa"));
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
                input: OwnedValue::build_text(Rc::new(String::from(""))),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::build_text(Rc::new(String::from("5"))),
                expected_len: 5,
            },
            TestCase {
                input: OwnedValue::build_text(Rc::new(String::from("0"))),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::build_text(Rc::new(String::from("-1"))),
                expected_len: 1,
            },
            TestCase {
                input: OwnedValue::Float(2.9),
                expected_len: 2,
            },
            TestCase {
                input: OwnedValue::Float(-3.15),
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
        let precision_val = OwnedValue::build_text(Rc::new(String::from("1")));
        let expected_val = OwnedValue::Float(123.5);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::build_text(Rc::new(String::from("123.456")));
        let precision_val = OwnedValue::Integer(2);
        let expected_val = OwnedValue::Float(123.46);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::Integer(123);
        let precision_val = OwnedValue::Integer(1);
        let expected_val = OwnedValue::Float(123.0);
        assert_eq!(exec_round(&input_val, Some(precision_val)), expected_val);

        let input_val = OwnedValue::Float(100.123);
        let expected_val = OwnedValue::Float(100.0);
        assert_eq!(exec_round(&input_val, None), expected_val);

        let input_val = OwnedValue::Float(100.123);
        let expected_val = OwnedValue::Null;
        assert_eq!(exec_round(&input_val, Some(OwnedValue::Null)), expected_val);
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
                &OwnedValue::build_text(Rc::new("limbo".to_string())),
                &OwnedValue::build_text(Rc::new("limbo".to_string()))
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
                &OwnedValue::build_text(Rc::new("limbo".to_string())),
                &OwnedValue::build_text(Rc::new("limb".to_string()))
            ),
            OwnedValue::build_text(Rc::new("limbo".to_string()))
        );
    }

    #[test]
    fn test_substring() {
        let str_value = OwnedValue::build_text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(1);
        let length_value = OwnedValue::Integer(3);
        let expected_val = OwnedValue::build_text(Rc::new(String::from("lim")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::build_text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(1);
        let length_value = OwnedValue::Integer(10);
        let expected_val = OwnedValue::build_text(Rc::new(String::from("limbo")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::build_text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(10);
        let length_value = OwnedValue::Integer(3);
        let expected_val = OwnedValue::build_text(Rc::new(String::from("")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::build_text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(3);
        let length_value = OwnedValue::Null;
        let expected_val = OwnedValue::build_text(Rc::new(String::from("mbo")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );

        let str_value = OwnedValue::build_text(Rc::new("limbo".to_string()));
        let start_value = OwnedValue::Integer(10);
        let length_value = OwnedValue::Null;
        let expected_val = OwnedValue::build_text(Rc::new(String::from("")));
        assert_eq!(
            exec_substring(&str_value, &start_value, &length_value),
            expected_val
        );
    }

    #[test]
    fn test_exec_instr() {
        let input = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("im")));
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("o")));
        let expected = OwnedValue::Integer(5);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("liiiiimbo")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("ii")));
        let expected = OwnedValue::Integer(2);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("limboX")));
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("")));
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let expected = OwnedValue::Integer(0);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("")));
        let pattern = OwnedValue::build_text(Rc::new(String::from("")));
        let expected = OwnedValue::Integer(1);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Null;
        let pattern = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("limbo")));
        let pattern = OwnedValue::Null;
        let expected = OwnedValue::Null;
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::Null;
        let pattern = OwnedValue::build_text(Rc::new(String::from("limbo")));
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
        let pattern = OwnedValue::build_text(Rc::new(String::from(".")));
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
        let pattern = OwnedValue::build_text(Rc::new(String::from("cd")));
        let expected = OwnedValue::Integer(3);
        assert_eq!(exec_instr(&input, &pattern), expected);

        let input = OwnedValue::build_text(Rc::new(String::from("abcde")));
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

        let input = OwnedValue::build_text(Rc::new("abc".to_string()));
        let expected = Some(OwnedValue::Null);
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::build_text(Rc::new("42".to_string()));
        let expected = Some(OwnedValue::Integer(1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::build_text(Rc::new("-42".to_string()));
        let expected = Some(OwnedValue::Integer(-1));
        assert_eq!(exec_sign(&input), expected);

        let input = OwnedValue::build_text(Rc::new("0".to_string()));
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

        let input = OwnedValue::build_text(Rc::new("5".to_string()));
        let expected = OwnedValue::Blob(Rc::new(vec![0; 5]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::build_text(Rc::new("-5".to_string()));
        let expected = OwnedValue::Blob(Rc::new(vec![]));
        assert_eq!(exec_zeroblob(&input), expected);

        let input = OwnedValue::build_text(Rc::new("text".to_string()));
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

    #[test]
    fn test_replace() {
        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("b")));
        let replace_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("aoa")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("b")));
        let replace_str = OwnedValue::build_text(Rc::new(String::from("")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("o")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("b")));
        let replace_str = OwnedValue::build_text(Rc::new(String::from("abc")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("abcoabc")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let replace_str = OwnedValue::build_text(Rc::new(String::from("b")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::build_text(Rc::new(String::from("")));
        let replace_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bob")));
        let pattern_str = OwnedValue::Null;
        let replace_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::Null;
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bo5")));
        let pattern_str = OwnedValue::Integer(5);
        let replace_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("boa")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bo5.0")));
        let pattern_str = OwnedValue::Float(5.0);
        let replace_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("boa")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bo5")));
        let pattern_str = OwnedValue::Float(5.0);
        let replace_str = OwnedValue::build_text(Rc::new(String::from("a")));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("bo5")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        let input_str = OwnedValue::build_text(Rc::new(String::from("bo5.0")));
        let pattern_str = OwnedValue::Float(5.0);
        let replace_str = OwnedValue::Float(6.0);
        let expected_str = OwnedValue::build_text(Rc::new(String::from("bo6.0")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );

        // todo: change this test to use (0.1 + 0.2) instead of 0.3 when decimals are implemented.
        let input_str = OwnedValue::build_text(Rc::new(String::from("tes3")));
        let pattern_str = OwnedValue::Integer(3);
        let replace_str = OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Float(0.3))));
        let expected_str = OwnedValue::build_text(Rc::new(String::from("tes0.3")));
        assert_eq!(
            exec_replace(&input_str, &pattern_str, &replace_str),
            expected_str
        );
    }
}
