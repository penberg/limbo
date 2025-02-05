use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    rc::{Rc, Weak},
};

use crate::{
    parameters::Parameters,
    schema::{BTreeTable, Index, PseudoTable},
    storage::sqlite3_ondisk::DatabaseHeader,
    translate::plan::{ResultSetColumn, TableReference},
    Connection,
};

use super::{BranchOffset, CursorID, Insn, InsnReference, Program};
#[allow(dead_code)]
pub struct ProgramBuilder {
    next_free_register: usize,
    next_free_cursor_id: usize,
    insns: Vec<Insn>,
    // for temporarily storing instructions that will be put after Transaction opcode
    constant_insns: Vec<Insn>,
    next_insn_label: Option<BranchOffset>,
    // Cursors that are referenced by the program. Indexed by CursorID.
    pub cursor_ref: Vec<(Option<String>, CursorType)>,
    /// A vector where index=label number, value=resolved offset. Resolved in build().
    label_to_resolved_offset: Vec<Option<InsnReference>>,
    // Bitmask of cursors that have emitted a SeekRowid instruction.
    seekrowid_emitted_bitmask: u64,
    // map of instruction index to manual comment (used in EXPLAIN only)
    comments: Option<HashMap<InsnReference, &'static str>>,
    pub parameters: Parameters,
    pub result_columns: Vec<ResultSetColumn>,
    pub table_references: Vec<TableReference>,
}

#[derive(Debug, Clone)]
pub enum CursorType {
    BTreeTable(Rc<BTreeTable>),
    BTreeIndex(Rc<Index>),
    Pseudo(Rc<PseudoTable>),
    Sorter,
}

impl CursorType {
    pub fn is_index(&self) -> bool {
        matches!(self, CursorType::BTreeIndex(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryMode {
    Normal,
    Explain,
}

pub struct ProgramBuilderOpts {
    pub query_mode: QueryMode,
    pub num_cursors: usize,
    pub approx_num_insns: usize,
    pub approx_num_labels: usize,
}

impl ProgramBuilder {
    pub fn new(opts: ProgramBuilderOpts) -> Self {
        Self {
            next_free_register: 1,
            next_free_cursor_id: 0,
            insns: Vec::with_capacity(opts.approx_num_insns),
            next_insn_label: None,
            cursor_ref: Vec::with_capacity(opts.num_cursors),
            constant_insns: Vec::new(),
            label_to_resolved_offset: Vec::with_capacity(opts.approx_num_labels),
            seekrowid_emitted_bitmask: 0,
            comments: if opts.query_mode == QueryMode::Explain {
                Some(HashMap::new())
            } else {
                None
            },
            parameters: Parameters::new(),
            result_columns: Vec::new(),
            table_references: Vec::new(),
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

    pub fn alloc_cursor_id(
        &mut self,
        table_identifier: Option<String>,
        cursor_type: CursorType,
    ) -> usize {
        let cursor = self.next_free_cursor_id;
        self.next_free_cursor_id += 1;
        self.cursor_ref.push((table_identifier, cursor_type));
        assert_eq!(self.cursor_ref.len(), self.next_free_cursor_id);
        cursor
    }

    pub fn emit_insn(&mut self, insn: Insn) {
        if let Some(label) = self.next_insn_label {
            self.label_to_resolved_offset.insert(
                label.to_label_value() as usize,
                Some(self.insns.len() as InsnReference),
            );
            self.next_insn_label = None;
        }
        self.insns.push(insn);
    }

    pub fn emit_string8(&mut self, value: String, dest: usize) {
        self.emit_insn(Insn::String8 { value, dest });
    }

    pub fn emit_string8_new_reg(&mut self, value: String) -> usize {
        let dest = self.alloc_register();
        self.emit_insn(Insn::String8 { value, dest });
        dest
    }

    pub fn emit_int(&mut self, value: i64, dest: usize) {
        self.emit_insn(Insn::Integer { value, dest });
    }

    pub fn emit_bool(&mut self, value: bool, dest: usize) {
        self.emit_insn(Insn::Integer {
            value: if value { 1 } else { 0 },
            dest,
        });
    }

    pub fn emit_null(&mut self, dest: usize) {
        self.emit_insn(Insn::Null {
            dest,
            dest_end: None,
        });
    }

    pub fn emit_result_row(&mut self, start_reg: usize, count: usize) {
        self.emit_insn(Insn::ResultRow { start_reg, count });
    }

    pub fn emit_halt(&mut self) {
        self.emit_insn(Insn::Halt {
            err_code: 0,
            description: String::new(),
        });
    }

    // no users yet, but I want to avoid someone else in the future
    // just adding parameters to emit_halt! If you use this, remove the
    // clippy warning please.
    #[allow(dead_code)]
    pub fn emit_halt_err(&mut self, err_code: usize, description: String) {
        self.emit_insn(Insn::Halt {
            err_code,
            description,
        });
    }

    pub fn emit_init(&mut self) -> BranchOffset {
        let target_pc = self.allocate_label();
        self.emit_insn(Insn::Init { target_pc });
        target_pc
    }

    pub fn emit_transaction(&mut self, write: bool) {
        self.emit_insn(Insn::Transaction { write });
    }

    pub fn emit_goto(&mut self, target_pc: BranchOffset) {
        self.emit_insn(Insn::Goto { target_pc });
    }

    pub fn add_comment(&mut self, insn_index: BranchOffset, comment: &'static str) {
        if let Some(comments) = &mut self.comments {
            comments.insert(insn_index.to_offset_int(), comment);
        }
    }

    // Emit an instruction that will be put at the end of the program (after Transaction statement).
    // This is useful for instructions that otherwise will be unnecessarily repeated in a loop.
    // Example: In `SELECT * from users where name='John'`, it is unnecessary to set r[1]='John' as we SCAN users table.
    // We could simply set it once before the SCAN started.
    pub fn mark_last_insn_constant(&mut self) {
        self.constant_insns.push(self.insns.pop().unwrap());
    }

    pub fn emit_constant_insns(&mut self) {
        self.insns.append(&mut self.constant_insns);
    }

    pub fn offset(&self) -> BranchOffset {
        BranchOffset::Offset(self.insns.len() as InsnReference)
    }

    pub fn allocate_label(&mut self) -> BranchOffset {
        let label_n = self.label_to_resolved_offset.len();
        self.label_to_resolved_offset.push(None);
        BranchOffset::Label(label_n as u32)
    }

    // Effectively a GOTO <next insn> without the need to emit an explicit GOTO instruction.
    // Useful when you know you need to jump to "the next part", but the exact offset is unknowable
    // at the time of emitting the instruction.
    pub fn preassign_label_to_next_insn(&mut self, label: BranchOffset) {
        self.next_insn_label = Some(label);
    }

    pub fn resolve_label(&mut self, label: BranchOffset, to_offset: BranchOffset) {
        assert!(matches!(label, BranchOffset::Label(_)));
        assert!(matches!(to_offset, BranchOffset::Offset(_)));
        self.label_to_resolved_offset[label.to_label_value() as usize] =
            Some(to_offset.to_offset_int());
    }

    /// Resolve unresolved labels to a specific offset in the instruction list.
    ///
    /// This function scans all instructions and resolves any labels to their corresponding offsets.
    /// It ensures that all labels are resolved correctly and updates the target program counter (PC)
    /// of each instruction that references a label.
    pub fn resolve_labels(&mut self) {
        let resolve = |pc: &mut BranchOffset, insn_name: &str| {
            if let BranchOffset::Label(label) = pc {
                let to_offset = self
                    .label_to_resolved_offset
                    .get(*label as usize)
                    .unwrap_or_else(|| {
                        panic!("Reference to undefined label in {}: {}", insn_name, label)
                    });
                *pc = BranchOffset::Offset(
                    to_offset
                        .unwrap_or_else(|| panic!("Unresolved label in {}: {}", insn_name, label)),
                );
            }
        };
        for insn in self.insns.iter_mut() {
            match insn {
                Insn::Init { target_pc } => {
                    resolve(target_pc, "Init");
                }
                Insn::Eq {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Eq");
                }
                Insn::Ne {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Ne");
                }
                Insn::Lt {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Lt");
                }
                Insn::Le {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Le");
                }
                Insn::Gt {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Gt");
                }
                Insn::Ge {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                    ..
                } => {
                    resolve(target_pc, "Ge");
                }
                Insn::If {
                    reg: _reg,
                    target_pc,
                    jump_if_null: _,
                } => {
                    resolve(target_pc, "If");
                }
                Insn::IfNot {
                    reg: _reg,
                    target_pc,
                    jump_if_null: _,
                } => {
                    resolve(target_pc, "IfNot");
                }
                Insn::RewindAwait {
                    cursor_id: _cursor_id,
                    pc_if_empty,
                } => {
                    resolve(pc_if_empty, "RewindAwait");
                }
                Insn::LastAwait {
                    cursor_id: _cursor_id,
                    pc_if_empty,
                } => {
                    resolve(pc_if_empty, "LastAwait");
                }
                Insn::Goto { target_pc } => {
                    resolve(target_pc, "Goto");
                }
                Insn::DecrJumpZero {
                    reg: _reg,
                    target_pc,
                } => {
                    resolve(target_pc, "DecrJumpZero");
                }
                Insn::SorterNext {
                    cursor_id: _cursor_id,
                    pc_if_next,
                } => {
                    resolve(pc_if_next, "SorterNext");
                }
                Insn::SorterSort { pc_if_empty, .. } => {
                    resolve(pc_if_empty, "SorterSort");
                }
                Insn::NotNull {
                    reg: _reg,
                    target_pc,
                } => {
                    resolve(target_pc, "NotNull");
                }
                Insn::IfPos { target_pc, .. } => {
                    resolve(target_pc, "IfPos");
                }
                Insn::NextAwait { pc_if_next, .. } => {
                    resolve(pc_if_next, "NextAwait");
                }
                Insn::PrevAwait { pc_if_next, .. } => {
                    resolve(pc_if_next, "PrevAwait");
                }
                Insn::InitCoroutine {
                    yield_reg: _,
                    jump_on_definition,
                    start_offset: _,
                } => {
                    resolve(jump_on_definition, "InitCoroutine");
                }
                Insn::NotExists {
                    cursor: _,
                    rowid_reg: _,
                    target_pc,
                } => {
                    resolve(target_pc, "NotExists");
                }
                Insn::Yield {
                    yield_reg: _,
                    end_offset,
                } => {
                    resolve(end_offset, "Yield");
                }
                Insn::SeekRowid { target_pc, .. } => {
                    resolve(target_pc, "SeekRowid");
                }
                Insn::Gosub { target_pc, .. } => {
                    resolve(target_pc, "Gosub");
                }
                Insn::Jump {
                    target_pc_eq,
                    target_pc_lt,
                    target_pc_gt,
                } => {
                    resolve(target_pc_eq, "Jump");
                    resolve(target_pc_lt, "Jump");
                    resolve(target_pc_gt, "Jump");
                }
                Insn::SeekGE { target_pc, .. } => {
                    resolve(target_pc, "SeekGE");
                }
                Insn::SeekGT { target_pc, .. } => {
                    resolve(target_pc, "SeekGT");
                }
                Insn::IdxGE { target_pc, .. } => {
                    resolve(target_pc, "IdxGE");
                }
                Insn::IdxGT { target_pc, .. } => {
                    resolve(target_pc, "IdxGT");
                }
                Insn::IsNull { reg: _, target_pc } => {
                    resolve(target_pc, "IsNull");
                }
                _ => continue,
            }
        }
        self.label_to_resolved_offset.clear();
    }

    // translate table to cursor id
    pub fn resolve_cursor_id(&self, table_identifier: &str) -> CursorID {
        self.cursor_ref
            .iter()
            .position(|(t_ident, _)| {
                t_ident
                    .as_ref()
                    .is_some_and(|ident| ident == table_identifier)
            })
            .unwrap()
    }

    pub fn build(
        mut self,
        database_header: Rc<RefCell<DatabaseHeader>>,
        connection: Weak<Connection>,
        change_cnt_on: bool,
    ) -> Program {
        self.resolve_labels();
        assert!(
            self.constant_insns.is_empty(),
            "constant_insns is not empty when build() is called, did you forget to call emit_constant_insns()?"
        );
        self.parameters.list.dedup();
        Program {
            max_registers: self.next_free_register,
            insns: self.insns,
            cursor_ref: self.cursor_ref,
            database_header,
            comments: self.comments,
            connection,
            auto_commit: true,
            parameters: self.parameters,
            n_change: Cell::new(0),
            change_cnt_on,
            result_columns: self.result_columns,
            table_references: self.table_references,
        }
    }
}
