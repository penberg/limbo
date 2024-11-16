use std::{
    cell::RefCell,
    collections::HashMap,
    rc::{Rc, Weak},
};

use crate::{storage::sqlite3_ondisk::DatabaseHeader, Connection};

use super::{BranchOffset, CursorID, Insn, InsnReference, Program, Table};

#[allow(dead_code)]
pub struct ProgramBuilder {
    next_free_register: usize,
    next_free_label: BranchOffset,
    next_free_cursor_id: usize,
    insns: Vec<Insn>,
    // for temporarily storing instructions that will be put after Transaction opcode
    constant_insns: Vec<Insn>,
    // Each label has a list of InsnReferences that must
    // be resolved. Lists are indexed by: label.abs() - 1
    unresolved_labels: Vec<Vec<InsnReference>>,
    next_insn_label: Option<BranchOffset>,
    // Cursors that are referenced by the program. Indexed by CursorID.
    pub cursor_ref: Vec<(Option<String>, Option<Table>)>,
    // List of deferred label resolutions. Each entry is a pair of (label, insn_reference).
    deferred_label_resolutions: Vec<(BranchOffset, InsnReference)>,
    // Bitmask of cursors that have emitted a SeekRowid instruction.
    seekrowid_emitted_bitmask: u64,
    // map of instruction index to manual comment (used in EXPLAIN)
    comments: HashMap<BranchOffset, &'static str>,
}

impl ProgramBuilder {
    pub fn new() -> Self {
        Self {
            next_free_register: 1,
            next_free_label: 0,
            next_free_cursor_id: 0,
            insns: Vec::new(),
            unresolved_labels: Vec::new(),
            next_insn_label: None,
            cursor_ref: Vec::new(),
            constant_insns: Vec::new(),
            deferred_label_resolutions: Vec::new(),
            seekrowid_emitted_bitmask: 0,
            comments: HashMap::new(),
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

    pub fn alloc_cursor_id(
        &mut self,
        table_identifier: Option<String>,
        table: Option<Table>,
    ) -> usize {
        let cursor = self.next_free_cursor_id;
        self.next_free_cursor_id += 1;
        self.cursor_ref.push((table_identifier, table));
        assert!(self.cursor_ref.len() == self.next_free_cursor_id);
        cursor
    }

    fn _emit_insn(&mut self, insn: Insn) {
        self.insns.push(insn);
    }

    pub fn emit_insn(&mut self, insn: Insn) {
        self._emit_insn(insn);
        if let Some(label) = self.next_insn_label {
            self.next_insn_label = None;
            self.resolve_label(label, (self.insns.len() - 1) as BranchOffset);
        }
    }

    pub fn add_comment(&mut self, insn_index: BranchOffset, comment: &'static str) {
        self.comments.insert(insn_index, comment);
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

    pub fn emit_insn_with_label_dependency(&mut self, insn: Insn, label: BranchOffset) {
        self._emit_insn(insn);
        self.add_label_dependency(label, (self.insns.len() - 1) as BranchOffset);
    }

    pub fn offset(&self) -> BranchOffset {
        self.insns.len() as BranchOffset
    }

    pub fn allocate_label(&mut self) -> BranchOffset {
        self.next_free_label -= 1;
        self.unresolved_labels.push(Vec::new());
        self.next_free_label
    }

    // Effectively a GOTO <next insn> without the need to emit an explicit GOTO instruction.
    // Useful when you know you need to jump to "the next part", but the exact offset is unknowable
    // at the time of emitting the instruction.
    pub fn preassign_label_to_next_insn(&mut self, label: BranchOffset) {
        self.next_insn_label = Some(label);
    }

    fn label_to_index(&self, label: BranchOffset) -> usize {
        (label.abs() - 1) as usize
    }

    pub fn add_label_dependency(&mut self, label: BranchOffset, insn_reference: BranchOffset) {
        assert!(insn_reference >= 0);
        assert!(label < 0);
        let label_index = self.label_to_index(label);
        assert!(label_index < self.unresolved_labels.len());
        let insn_reference = insn_reference as InsnReference;
        let label_references = &mut self.unresolved_labels[label_index];
        label_references.push(insn_reference);
    }

    pub fn defer_label_resolution(&mut self, label: BranchOffset, insn_reference: InsnReference) {
        self.deferred_label_resolutions
            .push((label, insn_reference));
    }

    pub fn resolve_label(&mut self, label: BranchOffset, to_offset: BranchOffset) {
        assert!(label < 0);
        assert!(to_offset >= 0);
        let label_index = self.label_to_index(label);
        assert!(
            label_index < self.unresolved_labels.len(),
            "Forbidden resolve of an unexistent label!"
        );

        let label_references = &mut self.unresolved_labels[label_index];
        for insn_reference in label_references.iter() {
            let insn = &mut self.insns[*insn_reference];
            match insn {
                Insn::Init { target_pc } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::Eq {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::Ne {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::Lt {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::Le {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::Gt {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::Ge {
                    lhs: _lhs,
                    rhs: _rhs,
                    target_pc,
                } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::If {
                    reg: _reg,
                    target_pc,
                    null_reg: _,
                } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::IfNot {
                    reg: _reg,
                    target_pc,
                    null_reg: _,
                } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::RewindAwait {
                    cursor_id: _cursor_id,
                    pc_if_empty,
                } => {
                    assert!(*pc_if_empty < 0);
                    *pc_if_empty = to_offset;
                }
                Insn::Goto { target_pc } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::DecrJumpZero {
                    reg: _reg,
                    target_pc,
                } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::SorterNext {
                    cursor_id: _cursor_id,
                    pc_if_next,
                } => {
                    assert!(*pc_if_next < 0);
                    *pc_if_next = to_offset;
                }
                Insn::SorterSort { pc_if_empty, .. } => {
                    assert!(*pc_if_empty < 0);
                    *pc_if_empty = to_offset;
                }
                Insn::NotNull {
                    reg: _reg,
                    target_pc,
                } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::IfPos { target_pc, .. } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::NextAwait { pc_if_next, .. } => {
                    assert!(*pc_if_next < 0);
                    *pc_if_next = to_offset;
                }
                Insn::InitCoroutine {
                    yield_reg: _,
                    jump_on_definition,
                    start_offset: _,
                } => {
                    *jump_on_definition = to_offset;
                }
                Insn::NotExists {
                    cursor: _,
                    rowid_reg: _,
                    target_pc,
                } => {
                    *target_pc = to_offset;
                }
                Insn::Yield {
                    yield_reg: _,
                    end_offset,
                } => {
                    *end_offset = to_offset;
                }
                Insn::SeekRowid { target_pc, .. } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::Gosub { target_pc, .. } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::Jump { target_pc_eq, .. } => {
                    // FIXME: this current implementation doesnt scale for insns that
                    // have potentially multiple label dependencies.
                    assert!(*target_pc_eq < 0);
                    *target_pc_eq = to_offset;
                }
                Insn::SeekGE { target_pc, .. } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::SeekGT { target_pc, .. } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::IdxGE { target_pc, .. } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::IdxGT { target_pc, .. } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                Insn::IsNull { src: _, target_pc } => {
                    assert!(*target_pc < 0);
                    *target_pc = to_offset;
                }
                _ => {
                    todo!("missing resolve_label for {:?}", insn);
                }
            }
        }
        label_references.clear();
    }

    // translate table to cursor id
    pub fn resolve_cursor_id(
        &self,
        table_identifier: &str,
        cursor_hint: Option<CursorID>,
    ) -> CursorID {
        if let Some(cursor_hint) = cursor_hint {
            return cursor_hint;
        }
        self.cursor_ref
            .iter()
            .position(|(t_ident, _)| {
                t_ident
                    .as_ref()
                    .is_some_and(|ident| ident == table_identifier)
            })
            .unwrap()
    }

    pub fn resolve_cursor_to_table(&self, cursor_id: CursorID) -> Option<Table> {
        self.cursor_ref[cursor_id].1.clone()
    }

    pub fn resolve_deferred_labels(&mut self) {
        for i in 0..self.deferred_label_resolutions.len() {
            let (label, insn_reference) = self.deferred_label_resolutions[i];
            self.resolve_label(label, insn_reference as BranchOffset);
        }
        self.deferred_label_resolutions.clear();
    }

    pub fn build(
        self,
        database_header: Rc<RefCell<DatabaseHeader>>,
        connection: Weak<Connection>,
    ) -> Program {
        assert!(
            self.deferred_label_resolutions.is_empty(),
            "deferred_label_resolutions is not empty when build() is called, did you forget to call resolve_deferred_labels()?"
        );
        assert!(
            self.constant_insns.is_empty(),
            "constant_insns is not empty when build() is called, did you forget to call emit_constant_insns()?"
        );
        Program {
            max_registers: self.next_free_register,
            insns: self.insns,
            cursor_ref: self.cursor_ref,
            database_header,
            comments: self.comments,
            connection,
            auto_commit: true,
        }
    }
}
