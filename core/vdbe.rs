use crate::btree::BTreeCursor;
use crate::pager::Pager;
use crate::types::{AggContext, Cursor, CursorResult, OwnedValue, Record};

use anyhow::Result;
use core::fmt;
use std::borrow::BorrowMut;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

pub type BranchOffset = usize;

pub type CursorID = usize;

pub type PageIdx = usize;

pub enum Insn {
    // Initialize the program state and jump to the given PC.
    Init {
        target_pc: BranchOffset,
    },

    // Open a cursor for reading.
    OpenReadAsync {
        cursor_id: CursorID,
        root_page: PageIdx,
    },

    // Await for the competion of open cursor.
    OpenReadAwait,

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

    // Emit a row of results.
    ResultRow {
        register_start: usize,
        register_end: usize,
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

    // Decrement the given register and jump to the given PC if the result is zero.
    DecrJumpZero {
        reg: usize,
        target_pc: BranchOffset,
    },

    AggStep {
        acc_reg: usize,
        col: usize,
        func: AggFunc,
    },

    AggFinal {
        register: usize,
        func: AggFunc,
    },
}

pub enum AggFunc {
    Avg,
    Sum,
}

impl AggFunc {
    fn to_string(&self) -> &str {
        match self {
            AggFunc::Avg => "avg",
            AggFunc::Sum => "sum",
            _ => "unknown",
        }
    }
}

pub struct ProgramBuilder {
    next_free_register: usize,
    next_free_cursor_id: usize,
    insns: Vec<Insn>,
}

impl ProgramBuilder {
    pub fn new() -> Self {
        Self {
            next_free_register: 0,
            next_free_cursor_id: 0,
            insns: Vec::new(),
        }
    }

    pub fn alloc_register(&mut self) -> usize {
        let reg = self.next_free_register;
        self.next_free_register += 1;
        reg
    }

    pub fn alloc_registers(&mut self, amount: usize) -> usize {
        let reg = self.next_free_register;
        self.next_free_register += amount;
        reg
    }

    pub fn next_free_register(&self) -> usize {
        self.next_free_register
    }

    pub fn alloc_cursor_id(&mut self) -> usize {
        let cursor = self.next_free_cursor_id;
        self.next_free_cursor_id += 1;
        cursor
    }

    pub fn emit_placeholder(&mut self) -> usize {
        let offset = self.insns.len();
        self.insns.push(Insn::Halt);
        offset
    }

    pub fn emit_insn(&mut self, insn: Insn) {
        self.insns.push(insn);
    }

    pub fn fixup_insn(&mut self, offset: usize, insn: Insn) {
        self.insns[offset] = insn;
    }

    pub fn offset(&self) -> usize {
        self.insns.len()
    }

    pub fn build(self) -> Program {
        Program {
            max_registers: self.next_free_register,
            insns: self.insns,
        }
    }
}

pub enum StepResult<'a> {
    Done,
    IO,
    Row(Record<'a>),
}

/// The program state describes the environment in which the program executes.
pub struct ProgramState {
    pub pc: usize,
    cursors: RefCell<BTreeMap<usize, Box<dyn Cursor>>>,
    registers: Vec<OwnedValue>,
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
}

impl Program {
    pub fn explain(&self) {
        println!("addr  opcode         p1    p2    p3    p4             p5  comment");
        println!("----  -------------  ----  ----  ----  -------------  --  -------");
        for (addr, insn) in self.insns.iter().enumerate() {
            print_insn(addr, insn);
        }
    }

    pub fn step<'a>(
        &self,
        state: &'a mut ProgramState,
        pager: Rc<Pager>,
    ) -> Result<StepResult<'a>> {
        loop {
            let insn = &self.insns[state.pc];
            trace_insn(state.pc, insn);
            let mut cursors = state.cursors.borrow_mut();
            match insn {
                Insn::Init { target_pc } => {
                    state.pc = *target_pc;
                }
                Insn::OpenReadAsync {
                    cursor_id,
                    root_page,
                } => {
                    let cursor = Box::new(BTreeCursor::new(pager.clone(), *root_page));
                    cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::OpenReadAwait => {
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
                        state.registers[*dest] = record.values[*column].clone();
                    } else {
                        todo!();
                    }
                    state.pc += 1;
                }
                Insn::ResultRow {
                    register_start,
                    register_end,
                } => {
                    let record = make_record(&state.registers, register_end, register_start);
                    state.pc += 1;
                    return Ok(StepResult::Row(record));
                }
                Insn::NextAsync { cursor_id } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
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
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    cursor.wait_for_completion()?;
                    if !cursor.is_empty() {
                        state.pc = *pc_if_next;
                    } else {
                        state.pc += 1;
                    }
                }
                Insn::Halt => {
                    return Ok(StepResult::Done);
                }
                Insn::Transaction => {
                    state.pc += 1;
                }
                Insn::Goto { target_pc } => {
                    state.pc = *target_pc;
                }
                Insn::Integer { value, dest } => {
                    state.registers[*dest] = OwnedValue::Integer(*value);
                    state.pc += 1;
                }
                Insn::String8 { value, dest } => {
                    state.registers[*dest] = OwnedValue::Text(Rc::new(value.into()));
                    state.pc += 1;
                }
                Insn::RowId { cursor_id, dest } => {
                    let cursor = cursors.get_mut(cursor_id).unwrap();
                    if let Some(ref rowid) = *cursor.rowid()? {
                        state.registers[*dest] = OwnedValue::Integer(*rowid as i64);
                    } else {
                        todo!();
                    }
                    state.pc += 1;
                }
                Insn::DecrJumpZero { reg, target_pc } => match state.registers[*reg] {
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
                },
                Insn::AggStep { acc_reg, col, func } => {
                    if let OwnedValue::Null = &state.registers[*acc_reg] {
                        state.registers[*acc_reg] = match func {
                            AggFunc::Avg => OwnedValue::Agg(Box::new(AggContext::Avg(
                                OwnedValue::Float(0.0),
                                OwnedValue::Integer(0),
                            ))),
                            AggFunc::Sum => {
                                OwnedValue::Agg(Box::new(AggContext::Sum(OwnedValue::Float(0.0))))
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
                        AggFunc::Sum => {
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
                    };
                    state.pc += 1;
                }
                Insn::AggFinal { register, func } => {
                    match func {
                        AggFunc::Avg => {
                            let OwnedValue::Agg(agg) = state.registers[*register].borrow_mut()
                            else {
                                unreachable!();
                            };
                            let AggContext::Avg(acc, count) = agg.borrow_mut() else {
                                unreachable!();
                            };
                            *acc /= count.clone();
                        }
                        AggFunc::Sum => {}
                    };
                    state.pc += 1;
                }
            }
        }
    }
}

fn make_record<'a>(
    registers: &'a [OwnedValue],
    register_end: &usize,
    register_start: &usize,
) -> Record<'a> {
    let mut values = Vec::with_capacity(*register_end - *register_start);
    for i in *register_start..*register_end {
        values.push(crate::types::to_value(&registers[i]));
    }
    Record::new(values)
}

fn trace_insn(addr: usize, insn: &Insn) {
    if !log::log_enabled!(log::Level::Trace) {
        return;
    }
    log::trace!("{}", insn_to_str(addr, insn));
}

fn print_insn(addr: usize, insn: &Insn) {
    let s = insn_to_str(addr, insn);
    println!("{}", s);
}

enum IntValue {
    Int(i64),
    Usize(usize),
}

impl fmt::Display for IntValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IntValue::Int(i) => f.pad(i.to_string().as_str()),
            IntValue::Usize(i) => f.pad(i.to_string().as_str()),
        }
    }
}

fn insn_to_str(addr: usize, insn: &Insn) -> String {
    let (opcode, p1, p2, p3, p4, p5, comment): (
        &str,
        IntValue,
        IntValue,
        IntValue,
        &str,
        IntValue,
        String,
    ) = match insn {
        Insn::Init { target_pc } => (
            "Init",
            IntValue::Usize(0),
            IntValue::Usize(*target_pc),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            format!("Start at {}", target_pc),
        ),
        Insn::OpenReadAsync {
            cursor_id,
            root_page,
        } => (
            "OpenReadAsync",
            IntValue::Usize(*cursor_id),
            IntValue::Usize(*root_page),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            format!("root={}", root_page),
        ),
        Insn::OpenReadAwait => (
            "OpenReadAwait",
            IntValue::Usize(0),
            IntValue::Usize(0),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            "".to_string(),
        ),
        Insn::RewindAsync { cursor_id } => (
            "RewindAsync",
            IntValue::Usize(*cursor_id),
            IntValue::Usize(0),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            "".to_string(),
        ),
        Insn::RewindAwait {
            cursor_id,
            pc_if_empty,
        } => (
            "RewindAwait",
            IntValue::Usize(*cursor_id),
            IntValue::Usize(*pc_if_empty),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            "".to_string(),
        ),
        Insn::Column {
            cursor_id,
            column,
            dest,
        } => (
            "Column",
            IntValue::Usize(*cursor_id),
            IntValue::Usize(*column),
            IntValue::Usize(*dest),
            "",
            IntValue::Usize(0),
            format!("r[{}]= cursor {} column {}", dest, cursor_id, column),
        ),
        Insn::ResultRow {
            register_start,
            register_end,
        } => (
            "ResultRow",
            IntValue::Usize(*register_start),
            IntValue::Usize(*register_end),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            format!("output=r[{}..{}]", register_start, register_end),
        ),
        Insn::NextAsync { cursor_id } => (
            "NextAsync",
            IntValue::Usize(*cursor_id),
            IntValue::Usize(0),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            "".to_string(),
        ),
        Insn::NextAwait {
            cursor_id,
            pc_if_next,
        } => (
            "NextAwait",
            IntValue::Usize(*cursor_id),
            IntValue::Usize(*pc_if_next),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            "".to_string(),
        ),
        Insn::Halt => (
            "Halt",
            IntValue::Usize(0),
            IntValue::Usize(0),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            "".to_string(),
        ),
        Insn::Transaction => (
            "Transaction",
            IntValue::Usize(0),
            IntValue::Usize(0),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            "".to_string(),
        ),
        Insn::Goto { target_pc } => (
            "Goto",
            IntValue::Usize(0),
            IntValue::Usize(*target_pc),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            "".to_string(),
        ),
        Insn::Integer { value, dest } => (
            "Integer",
            IntValue::Usize(*dest),
            IntValue::Int(*value),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            "".to_string(),
        ),
        Insn::String8 { value, dest } => (
            "String8",
            IntValue::Usize(*dest),
            IntValue::Usize(0),
            IntValue::Usize(0),
            value.as_str(),
            IntValue::Usize(0),
            format!("r[{}]= '{}'", dest, value),
        ),
        Insn::RowId { cursor_id, dest } => (
            "RowId",
            IntValue::Usize(*cursor_id),
            IntValue::Usize(*dest),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            "".to_string(),
        ),
        Insn::DecrJumpZero { reg, target_pc } => (
            "DecrJumpZero",
            IntValue::Usize(*reg),
            IntValue::Usize(*target_pc),
            IntValue::Usize(0),
            "",
            IntValue::Usize(0),
            "".to_string(),
        ),
        Insn::AggStep { func, acc_reg, col } => (
            "AggStep",
            IntValue::Usize(0),
            IntValue::Usize(*col),
            IntValue::Usize(*acc_reg),
            func.to_string(),
            IntValue::Usize(0),
            format!("accum=r[{}] step({})", *acc_reg, *col),
        ),
        Insn::AggFinal { register, func } => (
            "AggFinal",
            IntValue::Usize(0),
            IntValue::Usize(*register),
            IntValue::Usize(0),
            func.to_string(),
            IntValue::Usize(0),
            format!("accum=r[{}]", *register),
        ),
    };
    format!(
        "{:<4}  {:<13}  {:<4}  {:<4}  {:<4}  {:<13}  {:<2}  {}",
        addr, opcode, p1, p2, p3, p4, p5, comment
    )
}
