use crate::btree::Cursor;
use crate::pager::Pager;
use crate::schema::Schema;
use crate::sqlite3_ondisk::Value;

use anyhow::Result;
use sqlite3_parser::ast::{OneSelect, Select, Stmt};
use std::collections::HashMap;
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
        // FIXME: This is incorrect, it should be reading from registers.
        cursor_id: CursorID,
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

pub enum StepResult {
    Done,
    IO,
    Row,
}

/// The program state describes the environment in which the program executes.
pub struct ProgramState {
    pub pc: usize,
    cursors: HashMap<usize, Cursor>,
    registers: Vec<Option<Value>>,
    pager: Arc<Pager>,
}

impl ProgramState {
    pub fn new(pager: Arc<Pager>, max_registers: usize) -> Self {
        let cursors = HashMap::new();
        let mut registers = Vec::new();
        registers.resize(max_registers, None);
        Self {
            pc: 0,
            cursors,
            registers,
            pager,
        }
    }

    pub fn alloc_register(&mut self) -> usize {
        let reg = self.registers.len();
        self.registers.push(None);
        reg
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

    pub fn step(&self, state: &mut ProgramState) -> Result<StepResult> {
        loop {
            let insn = &self.insns[state.pc];
            trace_insn(state.pc, insn);
            match insn {
                Insn::Init { target_pc } => {
                    state.pc = *target_pc;
                }
                Insn::OpenReadAsync {
                    cursor_id,
                    root_page,
                } => {
                    let cursor = Cursor::new(state.pager.clone(), *root_page);
                    state.cursors.insert(*cursor_id, cursor);
                    state.pc += 1;
                }
                Insn::OpenReadAwait => {
                    state.pc += 1;
                }
                Insn::RewindAsync { cursor_id } => {
                    let cursor = state.cursors.get_mut(cursor_id).unwrap();
                    cursor.rewind()?;
                    state.pc += 1;
                }
                Insn::RewindAwait {
                    cursor_id,
                    pc_if_empty,
                } => {
                    let cursor = state.cursors.get_mut(cursor_id).unwrap();
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
                    let cursor = state.cursors.get_mut(cursor_id).unwrap();
                    if let Some(ref record) = *cursor.record()? {
                        state.registers[*dest] = Some(record.values[*column].clone());
                    } else {
                        todo!();
                    }
                    state.pc += 1;
                }
                Insn::ResultRow { cursor_id } => {
                    let cursor = state.cursors.get_mut(cursor_id).unwrap();
                    let _ = cursor.record()?;
                    state.pc += 1;
                    return Ok(StepResult::Row);
                }
                Insn::NextAsync { cursor_id } => {
                    let cursor = state.cursors.get_mut(cursor_id).unwrap();
                    cursor.next()?;
                    state.pc += 1;
                }
                Insn::NextAwait {
                    cursor_id,
                    pc_if_next,
                } => {
                    let cursor = state.cursors.get_mut(cursor_id).unwrap();
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
                    state.registers[*dest] = Some(Value::Integer(*value));
                    state.pc += 1;
                }
            }
        }
    }
}

pub fn translate(schema: &Schema, stmt: Stmt) -> Result<Program> {
    match stmt {
        Stmt::Select(select) => translate_select(schema, select),
        _ => todo!(),
    }
}

fn translate_select(schema: &Schema, select: Select) -> Result<Program> {
    match select.body.select {
        OneSelect::Select {
            columns,
            from: Some(from),
            ..
        } => {
            let cursor_id = 0;
            let table_name = match from.select {
                Some(select_table) => match *select_table {
                    sqlite3_parser::ast::SelectTable::Table(name, ..) => name.name,
                    _ => todo!(),
                },
                None => todo!(),
            };
            let table_name = table_name.0;
            let table = match schema.get_table(&table_name) {
                Some(table) => table,
                None => anyhow::bail!("Parse error: no such table: {}", table_name),
            };
            let root_page = table.root_page;
            let mut program = ProgramBuilder::new();
            let init_offset = program.emit_placeholder();
            let open_read_offset = program.offset();
            program.emit_insn(Insn::OpenReadAsync {
                cursor_id: 0,
                root_page,
            });
            program.emit_insn(Insn::OpenReadAwait);
            program.emit_insn(Insn::RewindAsync { cursor_id });
            let rewind_await_offset = program.emit_placeholder();
            translate_columns(&mut program, Some(cursor_id), Some(table), columns);
            program.emit_insn(Insn::ResultRow { cursor_id });
            program.emit_insn(Insn::NextAsync { cursor_id });
            program.emit_insn(Insn::NextAwait {
                cursor_id,
                pc_if_next: rewind_await_offset,
            });
            program.fixup_insn(
                rewind_await_offset,
                Insn::RewindAwait {
                    cursor_id,
                    pc_if_empty: program.offset(),
                },
            );
            program.emit_insn(Insn::Halt);
            program.fixup_insn(
                init_offset,
                Insn::Init {
                    target_pc: program.offset(),
                },
            );
            program.emit_insn(Insn::Transaction);
            program.emit_insn(Insn::Goto {
                target_pc: open_read_offset,
            });
            Ok(program.build())
        }
        OneSelect::Select {
            columns,
            from: None,
            ..
        } => {
            let mut program = ProgramBuilder::new();
            let init_offset = program.emit_placeholder();
            let after_init_offset = program.offset();
            translate_columns(&mut program, None, None, columns);
            program.emit_insn(Insn::ResultRow { cursor_id: 0 });
            program.emit_insn(Insn::Halt);
            program.fixup_insn(
                init_offset,
                Insn::Init {
                    target_pc: program.offset(),
                },
            );
            program.emit_insn(Insn::Goto {
                target_pc: after_init_offset,
            });
            Ok(program.build())
        }
        _ => todo!(),
    }
}

fn translate_columns(
    program: &mut ProgramBuilder,
    cursor_id: Option<usize>,
    table: Option<&crate::schema::Table>,
    columns: Vec<sqlite3_parser::ast::ResultColumn>,
) {
    for col in columns {
        match col {
            sqlite3_parser::ast::ResultColumn::Expr(expr, _) => match expr {
                sqlite3_parser::ast::Expr::Between {
                    lhs,
                    not,
                    start,
                    end,
                } => todo!(),
                sqlite3_parser::ast::Expr::Binary(_, _, _) => todo!(),
                sqlite3_parser::ast::Expr::Case {
                    base,
                    when_then_pairs,
                    else_expr,
                } => todo!(),
                sqlite3_parser::ast::Expr::Cast { expr, type_name } => todo!(),
                sqlite3_parser::ast::Expr::Collate(_, _) => todo!(),
                sqlite3_parser::ast::Expr::DoublyQualified(_, _, _) => todo!(),
                sqlite3_parser::ast::Expr::Exists(_) => todo!(),
                sqlite3_parser::ast::Expr::FunctionCall {
                    name,
                    distinctness,
                    args,
                    filter_over,
                } => todo!(),
                sqlite3_parser::ast::Expr::FunctionCallStar { name, filter_over } => todo!(),
                sqlite3_parser::ast::Expr::Id(_) => todo!(),
                sqlite3_parser::ast::Expr::InList { lhs, not, rhs } => todo!(),
                sqlite3_parser::ast::Expr::InSelect { lhs, not, rhs } => todo!(),
                sqlite3_parser::ast::Expr::InTable {
                    lhs,
                    not,
                    rhs,
                    args,
                } => todo!(),
                sqlite3_parser::ast::Expr::IsNull(_) => todo!(),
                sqlite3_parser::ast::Expr::Like {
                    lhs,
                    not,
                    op,
                    rhs,
                    escape,
                } => todo!(),
                sqlite3_parser::ast::Expr::Literal(lit) => match lit {
                    sqlite3_parser::ast::Literal::Numeric(val) => {
                        let dest = program.alloc_register();
                        program.emit_insn(Insn::Integer {
                            value: val.parse().unwrap(),
                            dest,
                        });
                    }
                    sqlite3_parser::ast::Literal::String(_) => todo!(),
                    sqlite3_parser::ast::Literal::Blob(_) => todo!(),
                    sqlite3_parser::ast::Literal::Keyword(_) => todo!(),
                    sqlite3_parser::ast::Literal::Null => todo!(),
                    sqlite3_parser::ast::Literal::CurrentDate => todo!(),
                    sqlite3_parser::ast::Literal::CurrentTime => todo!(),
                    sqlite3_parser::ast::Literal::CurrentTimestamp => todo!(),
                },
                sqlite3_parser::ast::Expr::Name(_) => todo!(),
                sqlite3_parser::ast::Expr::NotNull(_) => todo!(),
                sqlite3_parser::ast::Expr::Parenthesized(_) => todo!(),
                sqlite3_parser::ast::Expr::Qualified(_, _) => todo!(),
                sqlite3_parser::ast::Expr::Raise(_, _) => todo!(),
                sqlite3_parser::ast::Expr::Subquery(_) => todo!(),
                sqlite3_parser::ast::Expr::Unary(_, _) => todo!(),
                sqlite3_parser::ast::Expr::Variable(_) => todo!(),
            },
            sqlite3_parser::ast::ResultColumn::Star => {
                for i in 0..table.unwrap().columns.len() {
                    let dest = program.alloc_register();
                    program.emit_insn(Insn::Column {
                        column: i,
                        dest,
                        cursor_id: cursor_id.unwrap(),
                    });
                }
            }
            sqlite3_parser::ast::ResultColumn::TableStar(_) => todo!(),
        }
    }
}

fn trace_insn(addr: usize, insn: &Insn) {
    let s = insn_to_str(addr, insn);
    log::trace!("{}", s);
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
        Insn::ResultRow { cursor_id } => ("ResultRow", *cursor_id, 0, 0, "", 0, "".to_string()),
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
    };
    format!(
        "{:<4}  {:<13}  {:<4}  {:<4}  {:<4}  {:<13}  {:<2}  {}",
        addr, opcode, p1, p2, p3, p4, p5, comment
    )
}
