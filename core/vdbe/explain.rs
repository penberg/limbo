use super::{Insn, InsnReference, OwnedValue, Program};
use std::rc::Rc;

pub fn insn_to_str(
    program: &Program,
    addr: InsnReference,
    insn: &Insn,
    indent: String,
    manual_comment: Option<&'static str>,
) -> String {
    let (opcode, p1, p2, p3, p4, p5, comment): (&str, i32, i32, i32, OwnedValue, u16, String) =
        match insn {
            Insn::Init { target_pc } => (
                "Init",
                0,
                *target_pc as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("Start at {}", target_pc),
            ),
            Insn::Add { lhs, rhs, dest } => (
                "Add",
                *lhs as i32,
                *rhs as i32,
                *dest as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("r[{}]=r[{}]+r[{}]", dest, lhs, rhs),
            ),
            Insn::Subtract { lhs, rhs, dest } => (
                "Subtract",
                *lhs as i32,
                *rhs as i32,
                *dest as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("r[{}]=r[{}]-r[{}]", dest, lhs, rhs),
            ),
            Insn::Multiply { lhs, rhs, dest } => (
                "Multiply",
                *lhs as i32,
                *rhs as i32,
                *dest as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("r[{}]=r[{}]*r[{}]", dest, lhs, rhs),
            ),
            Insn::Null { dest, dest_end } => (
                "Null",
                0,
                *dest as i32,
                dest_end.map_or(0, |end| end as i32),
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                dest_end.map_or(format!("r[{}]=NULL", dest), |end| {
                    format!("r[{}..{}]=NULL", dest, end)
                }),
            ),
            Insn::NullRow { cursor_id } => (
                "NullRow",
                *cursor_id as i32,
                0,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("Set cursor {} to a (pseudo) NULL row", cursor_id),
            ),
            Insn::NotNull { reg, target_pc } => (
                "NotNull",
                *reg as i32,
                *target_pc as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("r[{}]!=NULL -> goto {}", reg, target_pc),
            ),
            Insn::Compare {
                start_reg_a,
                start_reg_b,
                count,
            } => (
                "Compare",
                *start_reg_a as i32,
                *start_reg_b as i32,
                *count as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!(
                    "r[{}..{}]==r[{}..{}]",
                    start_reg_a,
                    start_reg_a + (count - 1),
                    start_reg_b,
                    start_reg_b + (count - 1)
                ),
            ),
            Insn::Jump {
                target_pc_lt,
                target_pc_eq,
                target_pc_gt,
            } => (
                "Jump",
                *target_pc_lt as i32,
                *target_pc_eq as i32,
                *target_pc_gt as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::Move {
                source_reg,
                dest_reg,
                count,
            } => (
                "Move",
                *source_reg as i32,
                *dest_reg as i32,
                *count as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!(
                    "r[{}..{}]=r[{}..{}]",
                    dest_reg,
                    dest_reg + (count - 1),
                    source_reg,
                    source_reg + (count - 1)
                ),
            ),
            Insn::IfPos {
                reg,
                target_pc,
                decrement_by,
            } => (
                "IfPos",
                *reg as i32,
                *target_pc as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!(
                    "r[{}]>0 -> r[{}]-={}, goto {}",
                    reg, reg, decrement_by, target_pc
                ),
            ),
            Insn::Eq {
                lhs,
                rhs,
                target_pc,
            } => (
                "Eq",
                *lhs as i32,
                *rhs as i32,
                *target_pc as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("if r[{}]==r[{}] goto {}", lhs, rhs, target_pc),
            ),
            Insn::Ne {
                lhs,
                rhs,
                target_pc,
            } => (
                "Ne",
                *lhs as i32,
                *rhs as i32,
                *target_pc as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("if r[{}]!=r[{}] goto {}", lhs, rhs, target_pc),
            ),
            Insn::Lt {
                lhs,
                rhs,
                target_pc,
            } => (
                "Lt",
                *lhs as i32,
                *rhs as i32,
                *target_pc as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("if r[{}]<r[{}] goto {}", lhs, rhs, target_pc),
            ),
            Insn::Le {
                lhs,
                rhs,
                target_pc,
            } => (
                "Le",
                *lhs as i32,
                *rhs as i32,
                *target_pc as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("if r[{}]<=r[{}] goto {}", lhs, rhs, target_pc),
            ),
            Insn::Gt {
                lhs,
                rhs,
                target_pc,
            } => (
                "Gt",
                *lhs as i32,
                *rhs as i32,
                *target_pc as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("if r[{}]>r[{}] goto {}", lhs, rhs, target_pc),
            ),
            Insn::Ge {
                lhs,
                rhs,
                target_pc,
            } => (
                "Ge",
                *lhs as i32,
                *rhs as i32,
                *target_pc as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("if r[{}]>=r[{}] goto {}", lhs, rhs, target_pc),
            ),
            Insn::If {
                reg,
                target_pc,
                null_reg,
            } => (
                "If",
                *reg as i32,
                *target_pc as i32,
                *null_reg as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("if r[{}] goto {}", reg, target_pc),
            ),
            Insn::IfNot {
                reg,
                target_pc,
                null_reg,
            } => (
                "IfNot",
                *reg as i32,
                *target_pc as i32,
                *null_reg as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("if !r[{}] goto {}", reg, target_pc),
            ),
            Insn::OpenReadAsync {
                cursor_id,
                root_page,
            } => (
                "OpenReadAsync",
                *cursor_id as i32,
                *root_page as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!(
                    "table={}, root={}",
                    program.cursor_ref[*cursor_id]
                        .0
                        .as_ref()
                        .unwrap_or(&format!("cursor {}", cursor_id)),
                    root_page
                ),
            ),
            Insn::OpenReadAwait => (
                "OpenReadAwait",
                0,
                0,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::OpenPseudo {
                cursor_id,
                content_reg,
                num_fields,
            } => (
                "OpenPseudo",
                *cursor_id as i32,
                *content_reg as i32,
                *num_fields as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("{} columns in r[{}]", num_fields, content_reg),
            ),
            Insn::RewindAsync { cursor_id } => (
                "RewindAsync",
                *cursor_id as i32,
                0,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::RewindAwait {
                cursor_id,
                pc_if_empty,
            } => (
                "RewindAwait",
                *cursor_id as i32,
                *pc_if_empty as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!(
                    "Rewind table {}",
                    program.cursor_ref[*cursor_id]
                        .0
                        .as_ref()
                        .unwrap_or(&format!("cursor {}", cursor_id))
                ),
            ),
            Insn::Column {
                cursor_id,
                column,
                dest,
            } => {
                let (table_identifier, table) = &program.cursor_ref[*cursor_id];
                (
                    "Column",
                    *cursor_id as i32,
                    *column as i32,
                    *dest as i32,
                    OwnedValue::Text(Rc::new("".to_string())),
                    0,
                    format!(
                        "r[{}]={}.{}",
                        dest,
                        table_identifier
                            .as_ref()
                            .unwrap_or(&format!("cursor {}", cursor_id)),
                        table
                            .as_ref()
                            .and_then(|x| x.column_index_to_name(*column))
                            .unwrap_or(format!("column {}", *column).as_str())
                    ),
                )
            }
            Insn::MakeRecord {
                start_reg,
                count,
                dest_reg,
            } => (
                "MakeRecord",
                *start_reg as i32,
                *count as i32,
                *dest_reg as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!(
                    "r[{}]=mkrec(r[{}..{}])",
                    dest_reg,
                    start_reg,
                    start_reg + count - 1,
                ),
            ),
            Insn::ResultRow { start_reg, count } => (
                "ResultRow",
                *start_reg as i32,
                *count as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                if *count == 1 {
                    format!("output=r[{}]", start_reg)
                } else {
                    format!("output=r[{}..{}]", start_reg, start_reg + count - 1)
                },
            ),
            Insn::NextAsync { cursor_id } => (
                "NextAsync",
                *cursor_id as i32,
                0,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::NextAwait {
                cursor_id,
                pc_if_next,
            } => (
                "NextAwait",
                *cursor_id as i32,
                *pc_if_next as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::Halt {
                err_code,
                description: _,
            } => (
                "Halt",
                *err_code as i32,
                0,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::Transaction { write } => (
                "Transaction",
                0,
                *write as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::Goto { target_pc } => (
                "Goto",
                0,
                *target_pc as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::Gosub {
                target_pc,
                return_reg,
            } => (
                "Gosub",
                *return_reg as i32,
                *target_pc as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::Return { return_reg } => (
                "Return",
                *return_reg as i32,
                0,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::Integer { value, dest } => (
                "Integer",
                *value as i32,
                *dest as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("r[{}]={}", dest, value),
            ),
            Insn::Real { value, dest } => (
                "Real",
                0,
                *dest as i32,
                0,
                OwnedValue::Float(*value),
                0,
                format!("r[{}]={}", dest, value),
            ),
            Insn::RealAffinity { register } => (
                "RealAffinity",
                *register as i32,
                0,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::String8 { value, dest } => (
                "String8",
                0,
                *dest as i32,
                0,
                OwnedValue::Text(Rc::new(value.clone())),
                0,
                format!("r[{}]='{}'", dest, value),
            ),
            Insn::Blob { value, dest } => (
                "Blob",
                0,
                *dest as i32,
                0,
                OwnedValue::Blob(Rc::new(value.clone())),
                0,
                format!(
                    "r[{}]={} (len={})",
                    dest,
                    String::from_utf8_lossy(value),
                    value.len()
                ),
            ),
            Insn::RowId { cursor_id, dest } => (
                "RowId",
                *cursor_id as i32,
                *dest as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!(
                    "r[{}]={}.rowid",
                    dest,
                    &program.cursor_ref[*cursor_id]
                        .0
                        .as_ref()
                        .unwrap_or(&format!("cursor {}", cursor_id))
                ),
            ),
            Insn::SeekRowid {
                cursor_id,
                src_reg,
                target_pc,
            } => (
                "SeekRowid",
                *cursor_id as i32,
                *src_reg as i32,
                *target_pc as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!(
                    "if (r[{}]!={}.rowid) goto {}",
                    src_reg,
                    &program.cursor_ref[*cursor_id]
                        .0
                        .as_ref()
                        .unwrap_or(&format!("cursor {}", cursor_id)),
                    target_pc
                ),
            ),
            Insn::DeferredSeek {
                index_cursor_id,
                table_cursor_id,
            } => (
                "DeferredSeek",
                *index_cursor_id as i32,
                *table_cursor_id as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::SeekGT {
                is_index: _,
                cursor_id,
                start_reg,
                num_regs: _,
                target_pc,
            } => (
                "SeekGT",
                *cursor_id as i32,
                *target_pc as i32,
                *start_reg as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::SeekGE {
                is_index: _,
                cursor_id,
                start_reg,
                num_regs: _,
                target_pc,
            } => (
                "SeekGE",
                *cursor_id as i32,
                *target_pc as i32,
                *start_reg as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::IdxGT {
                cursor_id,
                start_reg,
                num_regs: _,
                target_pc,
            } => (
                "IdxGT",
                *cursor_id as i32,
                *target_pc as i32,
                *start_reg as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::IdxGE {
                cursor_id,
                start_reg,
                num_regs: _,
                target_pc,
            } => (
                "IdxGE",
                *cursor_id as i32,
                *target_pc as i32,
                *start_reg as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::DecrJumpZero { reg, target_pc } => (
                "DecrJumpZero",
                *reg as i32,
                *target_pc as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("if (--r[{}]==0) goto {}", reg, target_pc),
            ),
            Insn::AggStep {
                func,
                acc_reg,
                delimiter: _,
                col,
            } => (
                "AggStep",
                0,
                *col as i32,
                *acc_reg as i32,
                OwnedValue::Text(Rc::new(func.to_string().into())),
                0,
                format!("accum=r[{}] step(r[{}])", *acc_reg, *col),
            ),
            Insn::AggFinal { register, func } => (
                "AggFinal",
                0,
                *register as i32,
                0,
                OwnedValue::Text(Rc::new(func.to_string().into())),
                0,
                format!("accum=r[{}]", *register),
            ),
            Insn::SorterOpen {
                cursor_id,
                columns,
                order,
            } => {
                let _p4 = String::new();
                let to_print: Vec<String> = order
                    .values
                    .iter()
                    .map(|v| match v {
                        OwnedValue::Integer(i) => {
                            if *i == 0 {
                                "B".to_string()
                            } else {
                                "-B".to_string()
                            }
                        }
                        _ => unreachable!(),
                    })
                    .collect();
                (
                    "SorterOpen",
                    *cursor_id as i32,
                    *columns as i32,
                    0,
                    OwnedValue::Text(Rc::new(format!(
                        "k({},{})",
                        order.values.len(),
                        to_print.join(",")
                    ))),
                    0,
                    format!("cursor={}", cursor_id),
                )
            }
            Insn::SorterData {
                cursor_id,
                dest_reg,
                pseudo_cursor,
            } => (
                "SorterData",
                *cursor_id as i32,
                *dest_reg as i32,
                *pseudo_cursor as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("r[{}]=data", dest_reg),
            ),
            Insn::SorterInsert {
                cursor_id,
                record_reg,
            } => (
                "SorterInsert",
                *cursor_id as i32,
                *record_reg as i32,
                0,
                OwnedValue::Integer(0),
                0,
                format!("key=r[{}]", record_reg),
            ),
            Insn::SorterSort {
                cursor_id,
                pc_if_empty,
            } => (
                "SorterSort",
                *cursor_id as i32,
                *pc_if_empty as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::SorterNext {
                cursor_id,
                pc_if_next,
            } => (
                "SorterNext",
                *cursor_id as i32,
                *pc_if_next as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::Function {
                constant_mask,
                start_reg,
                dest,
                func,
            } => (
                "Function",
                *constant_mask,
                *start_reg as i32,
                *dest as i32,
                OwnedValue::Text(Rc::new(func.func.to_string())),
                0,
                if func.arg_count == 0 {
                    format!("r[{}]=func()", dest)
                } else if *start_reg == *start_reg + func.arg_count - 1 {
                    format!("r[{}]=func(r[{}])", dest, start_reg)
                } else {
                    format!(
                        "r[{}]=func(r[{}..{}])",
                        dest,
                        start_reg,
                        start_reg + func.arg_count - 1
                    )
                },
            ),
            Insn::InitCoroutine {
                yield_reg,
                jump_on_definition,
                start_offset,
            } => (
                "InitCoroutine",
                *yield_reg as i32,
                *jump_on_definition as i32,
                *start_offset as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::EndCoroutine { yield_reg } => (
                "EndCoroutine",
                *yield_reg as i32,
                0,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::Yield {
                yield_reg,
                end_offset,
            } => (
                "Yield",
                *yield_reg as i32,
                *end_offset as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::InsertAsync {
                cursor,
                key_reg,
                record_reg,
                flag,
            } => (
                "InsertAsync",
                *cursor as i32,
                *record_reg as i32,
                *key_reg as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                *flag as u16,
                "".to_string(),
            ),
            Insn::InsertAwait { cursor_id } => (
                "InsertAwait",
                *cursor_id as i32,
                0,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::NewRowid {
                cursor,
                rowid_reg,
                prev_largest_reg,
            } => (
                "NewRowId",
                *cursor as i32,
                *rowid_reg as i32,
                *prev_largest_reg as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::MustBeInt { reg } => (
                "MustBeInt",
                *reg as i32,
                0,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::SoftNull { reg } => (
                "SoftNull",
                *reg as i32,
                0,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::NotExists {
                cursor,
                rowid_reg,
                target_pc,
            } => (
                "NotExists",
                *cursor as i32,
                *target_pc as i32,
                *rowid_reg as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::OpenWriteAsync {
                cursor_id,
                root_page,
            } => (
                "OpenWriteAsync",
                *cursor_id as i32,
                *root_page as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::OpenWriteAwait {} => (
                "OpenWriteAwait",
                0,
                0,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::Copy {
                src_reg,
                dst_reg,
                amount,
            } => (
                "Copy",
                *src_reg as i32,
                *dst_reg as i32,
                *amount as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("r[{}]=r[{}]", dst_reg, src_reg),
            ),
            Insn::CreateBtree { db, root, flags } => (
                "CreateBtree",
                *db as i32,
                *root as i32,
                *flags as i32,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("r[{}]=root iDb={} flags={}", root, db, flags),
            ),
            Insn::Close { cursor_id } => (
                "Close",
                *cursor_id as i32,
                0,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                "".to_string(),
            ),
            Insn::IsNull { src, target_pc } => (
                "IsNull",
                *src as i32,
                *target_pc as i32,
                0,
                OwnedValue::Text(Rc::new("".to_string())),
                0,
                format!("if (r[{}]==NULL) goto {}", src, target_pc),
            ),
            Insn::ParseSchema { db, where_clause } => (
                "ParseSchema",
                *db as i32,
                0,
                0,
                OwnedValue::Text(Rc::new(where_clause.clone())),
                0,
                where_clause.clone(),
            ),
        };
    format!(
        "{:<4}  {:<17}  {:<4}  {:<4}  {:<4}  {:<13}  {:<2}  {}",
        addr,
        &(indent + opcode),
        p1,
        p2,
        p3,
        p4.to_string(),
        p5,
        manual_comment.map_or(comment.to_string(), |mc| format!("{}; {}", comment, mc))
    )
}
