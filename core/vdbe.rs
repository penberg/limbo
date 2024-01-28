use crate::btree::{Cursor, CursorResult};
use crate::pager::Pager;
use crate::types::{OwnedValue, Record};

use anyhow::Result;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::Arc;

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

    // Read the rowid of the current row.
    RowId {
        cursor_id: CursorID,
        dest: usize,
    },

    // Decrement the given register and jump to the given PC if the result is zero.
    //
    // Unlike in SQLite, if register is already zero, we don't decrement, but take the jump.
    DecrJumpZero {
        reg: usize,
        target_pc: BranchOffset,
    },
}

pub struct ProgramBuilder {
    next_free_register: usize,
    insns: Vec<Insn>,
}

impl ProgramBuilder {
    pub fn new() -> Self {
        Self {
            next_free_register: 0,
            insns: Vec::new(),
        }
    }

    pub fn alloc_register(&mut self) -> usize {
        let reg = self.next_free_register;
        self.next_free_register += 1;
        reg
    }

    pub fn next_free_register(&self) -> usize {
        self.next_free_register
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
    cursors: RefCell<BTreeMap<usize, Cursor>>,
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
        pager: Arc<Pager>,
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
                    let cursor = Cursor::new(pager.clone(), *root_page);
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
                    let mut values = Vec::with_capacity(*register_end - *register_start);
                    for i in *register_start..*register_end {
                        values.push(crate::types::to_value(&state.registers[i]));
                    }
                    state.pc += 1;
                    return Ok(StepResult::Row(Record::new(values)));
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
                    if cursor.has_record() {
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
                        if n > 0 {
                            state.registers[*reg] = OwnedValue::Integer(n - 1);
                            state.pc += 1;
                        } else {
                            state.pc = *target_pc;
                        }
                    }
                    _ => unreachable!("DecrJumpZero on non-integer register"),
                },
            }
        }
    }
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

fn insn_to_str(addr: usize, insn: &Insn) -> String {
    let (opcode, p1, p2, p3, p4, p5, comment) = match insn {
        Insn::Init { target_pc } => (
            "Init",
            0,
            *target_pc,
            0,
            "",
            0,
            format!("Starts at {}", target_pc),
        ),
        Insn::OpenReadAsync {
            cursor_id,
            root_page,
        } => (
            "OpenReadAsync",
            *cursor_id,
            *root_page,
            0,
            "",
            0,
            "".to_string(),
        ),
        Insn::OpenReadAwait => ("OpenReadAwait", 0, 0, 0, "", 0, "".to_string()),
        Insn::RewindAsync { cursor_id } => ("RewindAsync", *cursor_id, 0, 0, "", 0, "".to_string()),
        Insn::RewindAwait {
            cursor_id,
            pc_if_empty,
        } => (
            "RewindAwait",
            *cursor_id,
            *pc_if_empty,
            0,
            "",
            0,
            "".to_string(),
        ),
        Insn::Column {
            cursor_id,
            column,
            dest,
        } => ("Column", *cursor_id, *column, *dest, "", 0, "".to_string()),
        Insn::ResultRow {
            register_start,
            register_end,
        } => (
            "ResultRow",
            *register_start,
            *register_end,
            0,
            "",
            0,
            "".to_string(),
        ),
        Insn::NextAsync { cursor_id } => ("NextAsync", *cursor_id, 0, 0, "", 0, "".to_string()),
        Insn::NextAwait {
            cursor_id,
            pc_if_next,
        } => (
            "NextAwait",
            *cursor_id,
            *pc_if_next,
            0,
            "",
            0,
            "".to_string(),
        ),
        Insn::Halt => ("Halt", 0, 0, 0, "", 0, "".to_string()),
        Insn::Transaction => ("Transaction", 0, 0, 0, "", 0, "".to_string()),
        Insn::Goto { target_pc } => ("Goto", 0, *target_pc, 0, "", 0, "".to_string()),
        Insn::Integer { value, dest } => {
            ("Integer", *dest, *value as usize, 0, "", 0, "".to_string())
        }
        Insn::RowId { cursor_id, dest } => ("RowId", *cursor_id, *dest, 0, "", 0, "".to_string()),
        Insn::DecrJumpZero { reg, target_pc } => {
            ("DecrJumpZero", *reg, *target_pc, 0, "", 0, "".to_string())
        }
    };
    format!(
        "{:<4}  {:<13}  {:<4}  {:<4}  {:<4}  {:<13}  {:<2}  {}",
        addr, opcode, p1, p2, p3, p4, p5, comment
    )
}
