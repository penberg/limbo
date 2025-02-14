use crate::translate::{ProgramBuilder, ProgramBuilderOpts};
use crate::vdbe::insn::Insn;
use crate::{bail_parse_error, QueryMode, Result};
use limbo_sqlite3_parser::ast::{Name, TransactionType};

pub fn translate_tx_begin(
    tx_type: Option<TransactionType>,
    _tx_name: Option<Name>,
) -> Result<ProgramBuilder> {
    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode: QueryMode::Normal,
        num_cursors: 0,
        approx_num_insns: 0,
        approx_num_labels: 0,
    });
    let init_label = program.emit_init();
    let start_offset = program.offset();
    let tx_type = tx_type.unwrap_or(TransactionType::Deferred);
    match tx_type {
        TransactionType::Deferred => {
            bail_parse_error!("BEGIN DEFERRED not supported yet");
        }
        TransactionType::Immediate | TransactionType::Exclusive => {
            program.emit_insn(Insn::Transaction { write: true });
            // TODO: Emit transaction instruction on temporary tables when we support them.
            program.emit_insn(Insn::AutoCommit {
                auto_commit: false,
                rollback: false,
            });
        }
    }
    program.emit_halt();
    program.resolve_label(init_label, program.offset());
    program.emit_goto(start_offset);
    Ok(program)
}

pub fn translate_tx_commit(_tx_name: Option<Name>) -> Result<ProgramBuilder> {
    let mut program = ProgramBuilder::new(ProgramBuilderOpts {
        query_mode: QueryMode::Normal,
        num_cursors: 0,
        approx_num_insns: 0,
        approx_num_labels: 0,
    });
    let init_label = program.emit_init();
    let start_offset = program.offset();
    program.emit_insn(Insn::AutoCommit {
        auto_commit: true,
        rollback: false,
    });
    program.emit_halt();
    program.resolve_label(init_label, program.offset());
    program.emit_goto(start_offset);
    Ok(program)
}
