use crate::pager::Pager;
use crate::schema::Schema;

use anyhow::Result;
use sqlite3_parser::ast::{OneSelect, Select, SelectBody, Stmt};
use std::sync::Arc;

pub enum Insn {
    Init(InitInsn),
    OpenReadAsync(OpenReadAsyncInsn),
    OpenReadAwait,
    RewindAsync,
    RewindAwait(RewindAwaitInsn),
    Column,
    ResultRow,
    NextAsync,
    NextAwait,
    Halt,
    Transaction,
    Goto(GotoInsn),
}

pub struct InitInsn {
    pub target_pc: usize,
}

pub struct OpenReadAsyncInsn {
    pub root_page: usize,
}
pub struct RewindAwaitInsn {
    pub pc_if_empty: usize,
}

pub struct GotoInsn {
    pub target_pc: usize,
}
pub struct Program {
    pub insns: Vec<Insn>,
    pub pc: usize,
}

pub struct ProgramBuilder {
    pub insns: Vec<Insn>,
}

impl ProgramBuilder {
    pub fn new() -> Self {
        Self { insns: Vec::new() }
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
            insns: self.insns,
            pc: 0,
        }
    }
}

impl Program {
    pub fn explain(&self) {
        println!("addr  opcode         p1    p2    p3    p4             p5  comment");
        println!("----  -------------  ----  ----  ----  -------------  --  -------");
        for (addr, insn) in self.insns.iter().enumerate() {
            print_insn(addr, insn);
        }
    }

    pub fn step(&mut self, _pager: Arc<Pager>) -> Result<()> {
        loop {
            let insn = &self.insns[self.pc];
            print_insn(self.pc, insn);
            match insn {
                Insn::Init(init) => {
                    self.pc = init.target_pc;
                }
                Insn::OpenReadAsync(_) => {
                    self.pc += 1;
                }
                Insn::OpenReadAwait => {
                    self.pc += 1;
                }
                Insn::RewindAsync => {
                    self.pc += 1;
                }
                Insn::RewindAwait(rewind_await) => {
                    // TODO: Check if empty
                    self.pc = rewind_await.pc_if_empty;
                }
                Insn::Column => {
                    self.pc += 1;
                }
                Insn::ResultRow => {
                    self.pc += 1;
                }
                Insn::NextAsync => {
                    self.pc += 1;
                }
                Insn::NextAwait => {
                    self.pc += 1;
                }
                Insn::Halt => {
                    return Ok(());
                }
                Insn::Transaction => {
                    self.pc += 1;
                }
                Insn::Goto(goto) => {
                    self.pc = goto.target_pc;
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
            from: Some(from), ..
        } => {
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
            program.emit_insn(Insn::OpenReadAsync(OpenReadAsyncInsn { root_page }));
            program.emit_insn(Insn::OpenReadAwait);
            program.emit_insn(Insn::RewindAsync);
            let rewind_await_offset = program.emit_placeholder();
            program.emit_insn(Insn::Column);
            program.emit_insn(Insn::Column);
            program.emit_insn(Insn::ResultRow);
            program.emit_insn(Insn::NextAsync);
            program.emit_insn(Insn::NextAwait);
            program.fixup_insn(
                rewind_await_offset,
                Insn::RewindAwait(RewindAwaitInsn {
                    pc_if_empty: program.offset(),
                }),
            );
            program.emit_insn(Insn::Halt);
            program.fixup_insn(
                init_offset,
                Insn::Init(InitInsn {
                    target_pc: program.offset(),
                }),
            );
            program.emit_insn(Insn::Transaction);
            program.emit_insn(Insn::Goto(GotoInsn {
                target_pc: open_read_offset,
            }));
            Ok(program.build())
        }
        _ => todo!(),
    }
}

fn print_insn(addr: usize, insn: &Insn) {
    let (opcode, p1, p2, p3, p4, p5, comment) = match insn {
        Insn::Init(init) => (
            "Init",
            0,
            init.target_pc,
            0,
            "",
            0,
            format!("Starts at {}", init.target_pc),
        ),
        Insn::OpenReadAsync(open_read_async) => (
            "OpenReadAsync",
            0,
            open_read_async.root_page,
            0,
            "",
            0,
            "".to_string(),
        ),
        Insn::OpenReadAwait => ("OpenReadAwait", 0, 0, 0, "", 0, "".to_string()),
        Insn::RewindAsync => ("RewindAsync", 0, 0, 0, "", 0, "".to_string()),
        Insn::RewindAwait(rewind_await) => (
            "RewindAwait",
            0,
            rewind_await.pc_if_empty,
            0,
            "",
            0,
            "".to_string(),
        ),
        Insn::Column => ("Column", 0, 0, 0, "", 0, "".to_string()),
        Insn::ResultRow => ("ResultRow", 0, 0, 0, "", 0, "".to_string()),
        Insn::NextAsync => ("NextAsync", 0, 0, 0, "", 0, "".to_string()),
        Insn::NextAwait => ("NextAwait", 0, 0, 0, "", 0, "".to_string()),
        Insn::Halt => ("Halt", 0, 0, 0, "", 0, "".to_string()),
        Insn::Transaction => ("Transaction", 0, 0, 0, "", 0, "".to_string()),
        Insn::Goto(goto) => ("Goto", 0, goto.target_pc, 0, "", 0, "".to_string()),
    };
    println!(
        "{:<4}  {:<13}  {:<4}  {:<4}  {:<4}  {:<13}  {:<2}  {}",
        addr, opcode, p1, p2, p3, p4, p5, comment
    );
}
