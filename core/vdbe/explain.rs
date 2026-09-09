use crate::vdbe::{
    builder::CursorType,
    insn::{IntegrityCkData, RegisterOrLiteral, SorterOpenData},
};
use crate::HashSet;
use turso_parser::ast::{ResolveType, SortOrder};

use super::{Insn, InsnReference, PreparedProgram, Value};
use crate::function::{Func, ScalarFunc};
use crate::translate::eqp::EqpCteMaterialization;

pub const EXPLAIN_COLUMNS: [&str; 8] = ["addr", "opcode", "p1", "p2", "p3", "p4", "p5", "comment"];
pub const EXPLAIN_COLUMNS_TYPE: [&str; 8] = [
    "INTEGER", "TEXT", "INTEGER", "INTEGER", "INTEGER", "TEXT", "INTEGER", "TEXT",
];

pub const EXPLAIN_QUERY_PLAN_COLUMNS: [&str; 4] = ["id", "parent", "notused", "detail"];
pub const EXPLAIN_QUERY_PLAN_COLUMNS_TYPE: [&str; 4] = ["INTEGER", "INTEGER", "INTEGER", "TEXT"];

pub const EXPLAIN_QUERY_PLAN_JSON_COLUMNS: [&str; 1] = ["plan_json"];
pub const EXPLAIN_QUERY_PLAN_JSON_COLUMNS_TYPE: [&str; 1] = ["TEXT"];

#[derive(Debug, Clone, Default)]
pub struct ExplainInfo {
    /// map of instruction index to manual comment
    pub comments: Vec<(InsnReference, &'static str)>,
    /// Shared CTEs materialized before the main query, for `EXPLAIN QUERY PLAN`
    /// consumers. Empty outside `EXPLAIN QUERY PLAN` mode.
    pub cte_materializations: Vec<EqpCteMaterialization>,
}

impl ExplainInfo {
    pub fn comment_at(&self, insn_index: InsnReference) -> Option<&'static str> {
        self.comments
            .iter()
            .find(|(offset, _)| *offset == insn_index)
            .map(|(_, comment)| *comment)
    }

    pub(crate) fn remap_insn_indices(&mut self, old_to_new: &[usize]) {
        for (offset, _) in self.comments.iter_mut() {
            *offset = old_to_new[*offset as usize] as InsnReference;
        }
        for cte in self.cte_materializations.iter_mut() {
            for node_id in cte.node_ids.iter_mut() {
                *node_id = old_to_new[*node_id];
            }
        }
    }
}

pub fn insn_to_row(
    program: &PreparedProgram,
    insn: &Insn,
) -> (&'static str, i64, i64, i64, Value, i64, String) {
    let mut ephemeral_cursors = HashSet::default();
    for (insn, _) in &program.insns {
        match insn {
            Insn::OpenEphemeral { cursor_id, .. } => {
                ephemeral_cursors.insert(*cursor_id);
            }
            Insn::OpenAutoindex { cursor_id } => {
                ephemeral_cursors.insert(*cursor_id);
            }
            Insn::OpenDup { new_cursor_id, .. } => {
                // Note: relies on invariant that OpenDup is only for ephemeral cursors
                ephemeral_cursors.insert(*new_cursor_id);
            }
            _ => {}
        }
    }

    let get_table_or_index_name = |cursor_id: usize| -> String {
        let cursor_type = &program.cursor_ref[cursor_id].1;
        let name = match cursor_type {
            CursorType::BTreeTable(table) => table.name.as_str(),
            CursorType::BTreeIndex(index) => index.name.as_str(),
            CursorType::IndexMethod(descriptor) => descriptor.definition().index_name,
            CursorType::Pseudo(_) => "pseudo",
            CursorType::VirtualTable(virtual_table) => virtual_table.name.as_str(),
            CursorType::MaterializedView(table, _) => table.name.as_str(),
            CursorType::Sorter => "sorter",
        };
        if ephemeral_cursors.contains(&cursor_id) {
            format!("ephemeral({name})")
        } else {
            name.to_string()
        }
    };
    match insn {
            Insn::Init { target_pc } => (
                "Init",
                0,
                target_pc.as_debug_int() as i64,
                0,
                Value::build_text(""),
                0,
                format!("Start at {}", target_pc.as_debug_int()),
            ),
            Insn::Add { lhs, rhs, dest } => (
                "Add",
                *lhs as i64,
                *rhs as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]+r[{rhs}]"),
            ),
            Insn::Subtract { lhs, rhs, dest } => (
                "Subtract",
                *lhs as i64,
                *rhs as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]-r[{rhs}]"),
            ),
            Insn::Multiply { lhs, rhs, dest } => (
                "Multiply",
                *lhs as i64,
                *rhs as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]*r[{rhs}]"),
            ),
            Insn::Divide { lhs, rhs, dest } => (
                "Divide",
                *lhs as i64,
                *rhs as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]/r[{rhs}]"),
            ),
            Insn::BitAnd { lhs, rhs, dest } => (
                "BitAnd",
                *lhs as i64,
                *rhs as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]&r[{rhs}]"),
            ),
            Insn::BitOr { lhs, rhs, dest } => (
                "BitOr",
                *lhs as i64,
                *rhs as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]|r[{rhs}]"),
            ),
            Insn::BitNot { reg, dest } => (
                "BitNot",
                *reg as i64,
                *dest as i64,
                0,
                Value::build_text(""),
                0,
                format!("r[{dest}]=~r[{reg}]"),
            ),
            Insn::Checkpoint {
                database,
                checkpoint_mode: _,
                dest,
            } => (
                "Checkpoint",
                *database as i64,
                *dest as i64,
                0,
                Value::build_text(""),
                0,
                format!("r[{dest}]=~r[{database}]"),
            ),
            Insn::Remainder { lhs, rhs, dest } => (
                "Remainder",
                *lhs as i64,
                *rhs as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}]%r[{rhs}]"),
            ),
            Insn::Null { dest, dest_end } => (
                "Null",
                0,
                *dest as i64,
                dest_end.map_or(0, |end| end as i64),
                Value::build_text(""),
                0,
                dest_end.map_or(format!("r[{dest}]=NULL"), |end| {
                    format!("r[{dest}..{end}]=NULL")
                }),
            ),
            Insn::NullRow { cursor_id } => (
                "NullRow",
                *cursor_id as i64,
                0,
                0,
                Value::build_text(""),
                0,
                format!("Set cursor {cursor_id} to a (pseudo) NULL row"),
            ),
            Insn::NotNull { reg, target_pc } => (
                "NotNull",
                *reg as i64,
                target_pc.as_debug_int() as i64,
                0,
                Value::build_text(""),
                0,
                format!("r[{}]!=NULL -> goto {}", reg, target_pc.as_debug_int()),
            ),
            Insn::Compare {
                start_reg_a,
                start_reg_b,
                count,
                key_info,
            } => (
                "Compare",
                *start_reg_a as i64,
                *start_reg_b as i64,
                *count as i64,
                Value::build_text(format!("k({count}, {})", key_info.iter().map(|k| k.collation.to_string()).collect::<Vec<_>>().join(", "))),
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
                target_pc_lt.as_debug_int() as i64,
                target_pc_eq.as_debug_int() as i64,
                target_pc_gt.as_debug_int() as i64,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Move {
                source_reg,
                dest_reg,
                count,
            } => (
                "Move",
                *source_reg as i64,
                *dest_reg as i64,
                *count as i64,
                Value::build_text(""),
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
                *reg as i64,
                target_pc.as_debug_int() as i64,
                *decrement_by as i64,
                Value::build_text(""),
                0,
                format!(
                    "r[{}]>0 -> r[{}]-={}, goto {}",
                    reg,
                    reg,
                    decrement_by,
                    target_pc.as_debug_int()
                ),
            ),
            Insn::Eq {
                lhs,
                rhs,
                target_pc,
                collation,
                ..
            } => (
                "Eq",
                *lhs as i64,
                *rhs as i64,
                target_pc.as_debug_int() as i64,
                Value::build_text(collation.map_or("".to_string(), |c| c.to_string())),
                0,
                format!(
                    "if r[{}]==r[{}] goto {}",
                    lhs,
                    rhs,
                    target_pc.as_debug_int()
                ),
            ),
            Insn::Ne {
                lhs,
                rhs,
                target_pc,
                collation,
                ..
            } => (
                "Ne",
                *lhs as i64,
                *rhs as i64,
                target_pc.as_debug_int() as i64,
                Value::build_text(collation.map_or("".to_string(), |c| c.to_string())),
                0,
                format!(
                    "if r[{}]!=r[{}] goto {}",
                    lhs,
                    rhs,
                    target_pc.as_debug_int()
                ),
            ),
            Insn::Lt {
                lhs,
                rhs,
                target_pc,
                collation,
                ..
            } => (
                "Lt",
                *lhs as i64,
                *rhs as i64,
                target_pc.as_debug_int() as i64,
                Value::build_text(collation.map_or("".to_string(), |c| c.to_string())),
                0,
                format!("if r[{}]<r[{}] goto {}", lhs, rhs, target_pc.as_debug_int()),
            ),
            Insn::Le {
                lhs,
                rhs,
                target_pc,
                collation,
                ..
            } => (
                "Le",
                *lhs as i64,
                *rhs as i64,
                target_pc.as_debug_int() as i64,
                Value::build_text(collation.map_or("".to_string(), |c| c.to_string())),
                0,
                format!(
                    "if r[{}]<=r[{}] goto {}",
                    lhs,
                    rhs,
                    target_pc.as_debug_int()
                ),
            ),
            Insn::Gt {
                lhs,
                rhs,
                target_pc,
                collation,
                ..
            } => (
                "Gt",
                *lhs as i64,
                *rhs as i64,
                target_pc.as_debug_int() as i64,
                Value::build_text(collation.map_or("".to_string(), |c| c.to_string())),
                0,
                format!("if r[{}]>r[{}] goto {}", lhs, rhs, target_pc.as_debug_int()),
            ),
            Insn::Ge {
                lhs,
                rhs,
                target_pc,
                collation,
                ..
            } => (
                "Ge",
                *lhs as i64,
                *rhs as i64,
                target_pc.as_debug_int() as i64,
                Value::build_text(collation.map_or("".to_string(), |c| c.to_string())),
                0,
                format!(
                    "if r[{}]>=r[{}] goto {}",
                    lhs,
                    rhs,
                    target_pc.as_debug_int()
                ),
            ),
            Insn::If {
                reg,
                target_pc,
                jump_if_null,
            } => (
                "If",
                *reg as i64,
                target_pc.as_debug_int() as i64,
                *jump_if_null as i64,
                Value::build_text(""),
                0,
                format!("if r[{}] goto {}", reg, target_pc.as_debug_int()),
            ),
            Insn::IfNot {
                reg,
                target_pc,
                jump_if_null,
            } => (
                "IfNot",
                *reg as i64,
                target_pc.as_debug_int() as i64,
                *jump_if_null as i64,
                Value::build_text(""),
                0,
                format!("if !r[{}] goto {}", reg, target_pc.as_debug_int()),
            ),
            Insn::OpenRead {
                cursor_id,
                root_page,
                db,
            } => (
                "OpenRead",
                *cursor_id as i64,
                *root_page,
                *db as i64,
                Value::build_text(program.cursor_ref[*cursor_id]
                            .1.get_explain_description()),
                0,
                {
                    let cursor_type =
                        program.cursor_ref[*cursor_id]
                            .0
                            .as_ref()
                            .map_or("", |cursor_key| {
                                if cursor_key.index.is_some() {
                                    "index"
                                } else {
                                    "table"
                                }
                            });
                    format!(
                        "{}={}, root={}, iDb={}",
                        cursor_type,
                        get_table_or_index_name(*cursor_id),
                        root_page,
                        db
                    )
                },
            ),
            Insn::VOpen { cursor_id } => (
                "VOpen",
                *cursor_id as i64,
                0,
                0,
                Value::build_text(""),
                0,
                {
                    let cursor_type =
                        program.cursor_ref[*cursor_id]
                            .0
                            .as_ref()
                            .map_or("", |cursor_key| {
                                if cursor_key.index.is_some() {
                                    "index"
                                } else {
                                    "table"
                                }
                            });
                    format!("{} {}", cursor_type, get_table_or_index_name(*cursor_id),)
                },
            ),
            Insn::VCreate {
                table_name,
                module_name,
                args_reg,
            } => (
                "VCreate",
                *table_name as i64,
                *module_name as i64,
                args_reg.unwrap_or(0) as i64,
                Value::build_text(""),
                0,
                format!("table={table_name}, module={module_name}"),
            ),
            Insn::VFilter {
                cursor_id,
                pc_if_empty,
                arg_count,
                ..
            } => (
                "VFilter",
                *cursor_id as i64,
                pc_if_empty.as_debug_int() as i64,
                *arg_count as i64,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::VColumn {
                cursor_id,
                column,
                dest,
            } => (
                "VColumn",
                *cursor_id as i64,
                *column as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::VUpdate {
                cursor_id,
                arg_count,       // P2: Number of arguments in argv[]
                start_reg,       // P3: Start register for argv[]
                conflict_action, // P4: Conflict resolution flags
            } => (
                "VUpdate",
                *cursor_id as i64,
                *arg_count as i64,
                *start_reg as i64,
                Value::build_text(""),
                *conflict_action as i64,
                format!("args=r[{}..{}]", start_reg, start_reg + arg_count - 1),
            ),
            Insn::VNext {
                cursor_id,
                pc_if_next,
            } => (
                "VNext",
                *cursor_id as i64,
                pc_if_next.as_debug_int() as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::VDestroy { db, table_name } => (
                "VDestroy",
                *db as i64,
                0,
                0,
                Value::build_text(table_name.clone()),
                0,
                "".to_string(),
            ),
            Insn::VBegin{cursor_id} => (
                "VBegin",
                *cursor_id as i64,
                0,
                0,
                Value::build_text(""),
                0,
                "".into()
              ),
            Insn::VRename{cursor_id, new_name_reg} => (
               "VRename",
                *cursor_id as i64,
                 *new_name_reg as i64,
                 0,
                Value::build_text(""),
                 0,
                 "".into(),
            ),
            Insn::OpenPseudo {
                cursor_id,
                content_reg,
                num_fields,
            } => (
                "OpenPseudo",
                *cursor_id as i64,
                *content_reg as i64,
                *num_fields as i64,
                Value::build_text(""),
                0,
                format!("{num_fields} columns in r[{content_reg}]"),
            ),
            Insn::Rewind {
                cursor_id,
                pc_if_empty,
            } => (
                "Rewind",
                *cursor_id as i64,
                pc_if_empty.as_debug_int() as i64,
                0,
                Value::build_text(""),
                0,
                {
                    let cursor_type =
                        program.cursor_ref[*cursor_id]
                            .0
                            .as_ref()
                            .map_or("", |cursor_key| {
                                if cursor_key.index.is_some() {
                                    "index"
                                } else {
                                    "table"
                                }
                            });
                    format!(
                        "Rewind {} {}",
                        cursor_type,
                        get_table_or_index_name(*cursor_id),
                    )
                },
            ),
            Insn::Column {
                cursor_id,
                column,
                dest,
                default,
            } => {
                let cursor_type = &program.cursor_ref[*cursor_id].1;
                let column_name: Option<&String> = match cursor_type {
                    CursorType::BTreeTable(table) => {
                        let name = table.columns().get(*column).and_then(|v| v.name.as_ref());
                        name
                    }
                    CursorType::BTreeIndex(index) => {
                        let name = index.columns.get(*column).map(|c| &c.name);
                        name
                    }
                    CursorType::MaterializedView(table, _) => {
                        let name = table.columns().get(*column).and_then(|v| v.name.as_ref());
                        name
                    }
                    CursorType::Pseudo(_) => None,
                    CursorType::Sorter => None,
                    CursorType::IndexMethod(..) => None,
                    CursorType::VirtualTable(v) => {
                        let name = v.columns.get(*column).and_then(|c| c.name.as_ref());
                        name
                    }
                };
                (
                    "Column",
                    *cursor_id as i64,
                    *column as i64,
                    *dest as i64,
                    default.clone().unwrap_or_else(|| Value::build_text("")),
                    0,
                    format!(
                        "r[{}]={}.{}",
                        dest,
                        get_table_or_index_name(*cursor_id),
                        &column_name.map_or_else(|| format!("column {}", *column), |name| name.to_string())
                    ),
                )
            }
            Insn::ColumnRange {
                cursor_id,
                start_column,
                dest,
                defaults,
            } => {
                let count = defaults.len();
                let cursor_type = &program.cursor_ref[*cursor_id].1;
                let column_name = |column: usize| -> String {
                    let name: Option<&String> = match cursor_type {
                        CursorType::BTreeTable(table) => {
                            table.columns().get(column).and_then(|v| v.name.as_ref())
                        }
                        CursorType::BTreeIndex(index) => index.columns.get(column).map(|c| &c.name),
                        _ => {
                            None
                        }
                    };
                    name.map_or_else(|| format!("column {column}"), |name| name.to_string())
                };
                (
                    "ColumnRange",
                    *cursor_id as i64,
                    *start_column as i64,
                    *dest as i64,
                    Value::from_i64(count as i64),
                    0,
                    format!(
                        "r[{}..{}]={}.{}..{}",
                        dest,
                        dest + count - 1,
                        get_table_or_index_name(*cursor_id),
                        column_name(*start_column),
                        column_name(*start_column + count - 1),
                    ),
                )
            }
            Insn::ColumnHasField {
                cursor_id,
                column,
                target_pc,
            } => (
                "ColumnHasField",
                *cursor_id as i64,
                *column as i64,
                target_pc.as_debug_int().into(),
                Value::build_text(""),
                0,
                format!(
                    "if cursor {} record has field {} goto {}",
                    cursor_id, column, target_pc.as_debug_int()
                ),
            ),
            Insn::TypeCheck {
                start_reg,
                count,
                check_generated,
                ..
            } => (
                "TypeCheck",
                *start_reg as i64,
                *count as i64,
                *check_generated as i64,
                Value::build_text(""),
                0,
                String::from(""),
            ),
            Insn::ArrayEncode { data } => (
                "ArrayEncode",
                data.reg as i64,
                0,
                0,
                Value::build_text(""),
                0,
                format!(
                    "{}.{} ({})",
                    data.table_name, data.col_name, data.element_type
                ),
            ),
            Insn::ArrayDecode { reg } => (
                "ArrayDecode",
                *reg as i64,
                0,
                0,
                Value::build_text(""),
                0,
                String::new(),
            ),
            Insn::ArrayElement {
                array_reg,
                index_reg,
                dest,
            } => (
                "ArrayElement",
                *array_reg as i64,
                *index_reg as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                String::new(),
            ),
            Insn::ArrayLength { reg, dest } => (
                "ArrayLength",
                *reg as i64,
                *dest as i64,
                0,
                Value::build_text(""),
                0,
                String::new(),
            ),
            Insn::MakeArray {
                start_reg,
                count,
                dest,
            } => (
                "MakeArray",
                *start_reg as i64,
                *count as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                String::new(),
            ),
            Insn::MakeArrayDynamic {
                start_reg,
                count_reg,
                dest,
            } => (
                "MakeArrayDynamic",
                *start_reg as i64,
                *count_reg as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                String::new(),
            ),
            Insn::StructField {
                src_reg,
                field_index,
                dest,
            } => (
                "StructField",
                *src_reg as i64,
                *field_index as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                String::new(),
            ),
            Insn::UnionPack {
                tag_index,
                value_reg,
                dest,
            } => (
                "UnionPack",
                *value_reg as i64,
                *dest as i64,
                *tag_index as i64,
                Value::build_text(""),
                0,
                String::new(),
            ),
            Insn::UnionTag {
                src_reg,
                dest,
                tag_names: _,
            } => (
                "UnionTag",
                *src_reg as i64,
                *dest as i64,
                0,
                Value::build_text(""),
                0,
                String::new(),
            ),
            Insn::UnionExtract {
                src_reg,
                expected_tag,
                dest,
            } => (
                "UnionExtract",
                *src_reg as i64,
                *dest as i64,
                *expected_tag as i64,
                Value::build_text(""),
                0,
                String::new(),
            ),
            Insn::RegCopyOffset {
                src,
                base,
                offset_reg,
            } => (
                "RegCopyOffset",
                *src as i64,
                *base as i64,
                *offset_reg as i64,
                Value::build_text(""),
                0,
                String::new(),
            ),
            Insn::BlobRead {
                cursor,
                column,
                offset,
                amount,
                dest,
            } => (
                "BlobRead",
                *cursor as i64,
                *column as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=blob(cur={cursor} col={column}) @r[{offset}] len r[{amount}]"),
            ),
            Insn::BlobWrite {
                cursor,
                column,
                offset,
                src,
                dest,
            } => (
                "BlobWrite",
                *cursor as i64,
                *column as i64,
                *src as i64,
                Value::build_text(""),
                0,
                format!("blob(cur={cursor} col={column}) @r[{offset}]=r[{src}]; r[{dest}]=ok"),
            ),
            Insn::BlobLen {
                cursor,
                column,
                dest,
            } => (
                "BlobLen",
                *cursor as i64,
                *column as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=byte length of blob(cur={cursor} col={column})"),
            ),
            Insn::ArrayConcat { lhs, rhs, dest } => (
                "ArrayConcat",
                *lhs as i64,
                *rhs as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                String::new(),
            ),
            Insn::ArraySetElement {
                array_reg,
                index_reg,
                value_reg,
                dest,
            } => (
                "ArraySetElement",
                *array_reg as i64,
                *index_reg as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{value_reg}]"),
            ),
            Insn::ArraySlice {
                array_reg,
                start_reg,
                end_reg,
                dest,
            } => (
                "ArraySlice",
                *array_reg as i64,
                *start_reg as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("end_reg=r[{end_reg}]"),
            ),
            Insn::MakeRecord {
                start_reg,
                count,
                dest_reg,
                index_name,
                affinity_str: _,
            } => {
                let for_index = index_name.as_ref().map(|name| format!("; for {name}"));
                (
                    "MakeRecord",
                    *start_reg as i64,
                    *count as i64,
                    *dest_reg as i64,
                    Value::build_text(""),
                    0,
                    format!(
                        "r[{}]=mkrec(r[{}..{}]){}",
                        dest_reg,
                        start_reg,
                        start_reg + count - 1,
                        for_index.unwrap_or_else(|| "".to_string())
                    ),
                )
            }
            Insn::ResultRow { start_reg, count } => (
                "ResultRow",
                *start_reg as i64,
                *count as i64,
                0,
                Value::build_text(""),
                0,
                if *count == 1 {
                    format!("output=r[{start_reg}]")
                } else {
                    format!("output=r[{}..{}]", start_reg, start_reg + count - 1)
                },
            ),
            Insn::Next {
                cursor_id,
                pc_if_next,
                fullscan,
                ..
            } => (
                "Next",
                *cursor_id as i64,
                pc_if_next.as_debug_int() as i64,
                0,
                Value::build_text(""),
                // SQLite sets P5 to SQLITE_STMTSTATUS_FULLSCAN_STEP (1) on
                // full-table-scan steps
                *fullscan as i64,
                "".to_string(),
            ),
            Insn::Halt {
                err_code,
                description,
                on_error,
                description_reg,
            } => {
                let p2 = match on_error {
                    Some(ResolveType::Rollback) => 1,
                    Some(ResolveType::Abort) => 2,
                    Some(ResolveType::Fail) => 3,
                    Some(ResolveType::Ignore) => 4,
                    Some(ResolveType::Replace) => 5,
                    None => 0,
                };
                let p3 = description_reg.unwrap_or(0) as i64;
                (
                    "Halt",
                    *err_code as i64,
                    p2,
                    p3,
                    Value::build_text(description.clone()),
                    0,
                    "".to_string(),
                )
            }
            Insn::HaltIfNull {
                err_code,
                target_reg,
                description,
            } => (
                "HaltIfNull",
                *err_code as i64,
                0,
                *target_reg as i64,
                Value::build_text(description.clone()),
                0,
                "".to_string(),
            ),
            Insn::Transaction { db, tx_mode, schema_cookie} => (
                "Transaction",
                *db as i64,
                *tx_mode as i64,
                *schema_cookie as i64,
                Value::build_text(""),
                0,
                format!("iDb={db} tx_mode={tx_mode:?}"),
            ),
            Insn::Goto { target_pc } => (
                "Goto",
                0,
                target_pc.as_debug_int() as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Gosub {
                target_pc,
                return_reg,
            } => (
                "Gosub",
                *return_reg as i64,
                target_pc.as_debug_int() as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Return {
                return_reg,
                can_fallthrough,
            } => (
                "Return",
                *return_reg as i64,
                0,
                *can_fallthrough as i64,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Integer { value, dest } => (
                "Integer",
                *value,
                *dest as i64,
                0,
                Value::build_text(""),
                0,
                format!("r[{dest}]={value}"),
            ),
            Insn::Program {
                param_registers,
                ignore_jump_target,
                program: subprogram,
            } => {
                let sql = subprogram
                    .prepared_program()
                    .expect("Program subprogram should be resolved before EXPLAIN")
                    .sql
                    .clone();
                let comment = format!("subprogram={sql}");
                (
                    "Program",
                    // P1: first parent register that contains a param
                    param_registers.first().copied().unwrap_or(0) as i64,
                    // P2: ignore jump target (for RAISE(IGNORE))
                    ignore_jump_target.as_debug_int() as i64,
                    // P3: number of registers that contain params
                    param_registers.len() as i64,
                    Value::build_text(sql),
                    0,
                    comment,
                )
            }
            Insn::ResetCount => (
                "ResetCount",
                0,
                0,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::ChangeCount { dest } => (
                "ChangeCount",
                0,
                *dest as i64,
                0,
                Value::build_text(""),
                0,
                format!("r[{dest}]=changes"),
            ),
            Insn::Real { value, dest } => (
                "Real",
                0,
                *dest as i64,
                0,
                Value::from_f64(*value),
                0,
                format!("r[{dest}]={value}"),
            ),
            Insn::RealAffinity { register } => (
                "RealAffinity",
                *register as i64,
                0,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::String8 { value, dest } => (
                "String8",
                0,
                *dest as i64,
                0,
                Value::build_text(value.clone()),
                0,
                format!("r[{dest}]='{value}'"),
            ),
            Insn::Blob { value, dest } => (
                "Blob",
                0,
                *dest as i64,
                0,
                Value::Blob(value.clone()),
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
                *cursor_id as i64,
                *dest as i64,
                0,
                Value::build_text(""),
                0,
                format!("r[{}]={}.rowid", dest, get_table_or_index_name(*cursor_id)),
            ),
            Insn::IdxRowId { cursor_id, dest } => (
                "IdxRowId",
                *cursor_id as i64,
                *dest as i64,
                0,
                Value::build_text(""),
                0,
                format!(
                    "r[{}]={}.rowid",
                    dest,
                    program.cursor_ref[*cursor_id]
                        .0
                        .as_ref()
                        .map(|k| format!(
                            "cursor {} for {} {}",
                            cursor_id,
                            if k.index.is_some() { "index" } else { "table" },
                            get_table_or_index_name(*cursor_id),
                        ))
                        .unwrap_or_else(|| format!("cursor {cursor_id}"))
                ),
            ),
            Insn::SeekRowid {
                cursor_id,
                src_reg,
                target_pc,
            } => (
                "SeekRowid",
                *cursor_id as i64,
                *src_reg as i64,
                target_pc.as_debug_int() as i64,
                Value::build_text(""),
                0,
                format!(
                    "if (r[{}]!={}.rowid) goto {}",
                    src_reg,
                    &program.cursor_ref[*cursor_id]
                        .0
                        .as_ref()
                        .map(|k| format!(
                            "cursor {} for {} {}",
                            cursor_id,
                            if k.index.is_some() { "index" } else { "table" },
                            get_table_or_index_name(*cursor_id),
                        ))
                        .unwrap_or_else(|| format!("cursor {cursor_id}")),
                    target_pc.as_debug_int()
                ),
            ),
            Insn::DeferredSeek {
                index_cursor_id,
                table_cursor_id,
            } => (
                "DeferredSeek",
                *index_cursor_id as i64,
                *table_cursor_id as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::SeekGT {
                is_index: _,
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
            }
            | Insn::SeekGE {
                is_index: _,
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
                ..
            }
            | Insn::SeekLE {
                is_index: _,
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
                ..
            }
            | Insn::SeekLT {
                is_index: _,
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
            } => (
                match insn {
                    Insn::SeekGT { .. } => "SeekGT",
                    Insn::SeekGE { .. } => "SeekGE",
                    Insn::SeekLE { .. } => "SeekLE",
                    Insn::SeekLT { .. } => "SeekLT",
                    _ => unreachable!(),
                },
                *cursor_id as i64,
                target_pc.as_debug_int() as i64,
                *start_reg as i64,
                Value::build_text(""),
                0,
                format!("key=[{}..{}]", start_reg, start_reg + num_regs - 1),
            ),
            Insn::SeekEnd { cursor_id } => (
                "SeekEnd",
                *cursor_id as i64,
                0,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::IdxInsert {
                cursor_id,
                record_reg,
                unpacked_start,
                flags,
                ..
            } => (
                "IdxInsert",
                *cursor_id as i64,
                *record_reg as i64,
                unpacked_start.unwrap_or(0) as i64,
                Value::build_text(""),
                flags.0 as i64,
                format!("key=r[{record_reg}]"),
            ),
            Insn::IdxGT {
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
            }
            | Insn::IdxGE {
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
            }
            | Insn::IdxLE {
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
            }
            | Insn::IdxLT {
                cursor_id,
                start_reg,
                num_regs,
                target_pc,
            } => (
                match insn {
                    Insn::IdxGT { .. } => "IdxGT",
                    Insn::IdxGE { .. } => "IdxGE",
                    Insn::IdxLE { .. } => "IdxLE",
                    Insn::IdxLT { .. } => "IdxLT",
                    _ => unreachable!(),
                },
                *cursor_id as i64,
                target_pc.as_debug_int() as i64,
                *start_reg as i64,
                Value::build_text(""),
                0,
                format!("key=[{}..{}]", start_reg, start_reg + num_regs - 1),
            ),
            Insn::DecrJumpZero { reg, target_pc } => (
                "DecrJumpZero",
                *reg as i64,
                target_pc.as_debug_int() as i64,
                0,
                Value::build_text(""),
                0,
                format!("if (--r[{}]==0) goto {}", reg, target_pc.as_debug_int()),
            ),
            Insn::AggStep { data } => (
                "AggStep",
                0,
                data.col as i64,
                data.acc_reg as i64,
                Value::build_text(match &data.collation {
                    Some(collation) => format!("{}({collation})", data.func.as_str()),
                    None => data.func.as_str().to_string(),
                }),
                0,
                format!("accum=r[{}] step(r[{}])", data.acc_reg, data.col),
            ),
            Insn::AggInverse {
                func,
                acc_reg,
                delimiter: _,
                col,
                comparator: _,
            } => (
                "AggInverse",
                0,
                *col as i64,
                *acc_reg as i64,
                Value::build_text(func.as_str()),
                0,
                format!("accum=r[{}] inverse(r[{}])", *acc_reg, *col),
            ),
            Insn::AggFinal { register, func } => (
                "AggFinal",
                0,
                *register as i64,
                0,
                Value::build_text(func.as_str()),
                0,
                format!("accum=r[{}]", *register),
            ),
            Insn::AggValue {
                acc_reg,
                dest_reg,
                func,
            } => (
                "AggValue",
                0,
                *acc_reg as i64,
                *dest_reg as i64,
                Value::build_text(func.as_str()),
                0,
                format!("accum=r[{}] dest=r[{}]", *acc_reg, *dest_reg),
            ),
            Insn::SorterOpen { data } => {
                let SorterOpenData {
                    cursor_id,
                    columns,
                    order_collations_nulls,
                    ..
                } = data.as_ref();
                let to_print: Vec<String> = order_collations_nulls
                    .iter()
                    .map(|(order, collation, nulls)| {
                        let sign = match order {
                            SortOrder::Asc => "",
                            SortOrder::Desc => "-",
                        };
                        let coll_str = if let Some(coll) = collation {
                            format!("{sign}{coll}")
                        } else {
                            format!("{sign}B")
                        };
                        match nulls {
                            Some(turso_parser::ast::NullsOrder::First) => format!("{coll_str} NF"),
                            Some(turso_parser::ast::NullsOrder::Last) => format!("{coll_str} NL"),
                            None => coll_str,
                        }
                    })
                    .collect();
                (
                    "SorterOpen",
                    *cursor_id as i64,
                    *columns as i64,
                    0,
                    Value::build_text(format!("k({},{})", order_collations_nulls.len(), to_print.join(","))),
                    0,
                    format!("cursor={cursor_id}"),
                )
            }
            Insn::SorterData {
                cursor_id,
                dest_reg,
                pseudo_cursor,
            } => (
                "SorterData",
                *cursor_id as i64,
                *dest_reg as i64,
                *pseudo_cursor as i64,
                Value::build_text(""),
                0,
                format!("r[{dest_reg}]=data"),
            ),
            Insn::SorterInsert {
                cursor_id,
                record_reg,
            } => (
                "SorterInsert",
                *cursor_id as i64,
                *record_reg as i64,
                0,
                Value::from_i64(0),
                0,
                format!("key=r[{record_reg}]"),
            ),
            Insn::SorterSort {
                cursor_id,
                pc_if_empty,
            } => (
                "SorterSort",
                *cursor_id as i64,
                pc_if_empty.as_debug_int() as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::SorterNext {
                cursor_id,
                pc_if_next,
            } => (
                "SorterNext",
                *cursor_id as i64,
                pc_if_next.as_debug_int() as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::SorterCompare {
                cursor_id,
                pc_when_nonequal,
                sorted_record_reg,
                num_regs,
            } => (
                "SorterCompare",
                *cursor_id as i64,
                pc_when_nonequal.as_debug_int() as i64,
                *sorted_record_reg as i64,
                Value::build_text(num_regs.to_string()),
                0,
                "".to_string(),
            ),
            Insn::RowSetAdd {
                rowset_reg,
                value_reg,
            } => (
                "RowSetAdd",
                *rowset_reg as i64,
                *value_reg as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::RowSetRead {
                rowset_reg,
                pc_if_empty,
                dest_reg,
            } => (
                "RowSetRead",
                *rowset_reg as i64,
                pc_if_empty.as_debug_int() as i64,
                *dest_reg as i64,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::RowSetTest {
                rowset_reg,
                pc_if_found,
                value_reg,
                batch,
            } => (
                "RowSetTest",
                *rowset_reg as i64,
                pc_if_found.as_debug_int() as i64,
                *value_reg as i64,
                Value::build_text(batch.to_string()),
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
                *constant_mask as i64,
                *start_reg as i64,
                *dest as i64,
                {
                    let s = if matches!(&func.func, Func::Scalar(ScalarFunc::Like)) {
                        format!("like({})", func.arg_count)
                    } else {
                        func.func.to_string()
                    };
                    Value::build_text(s)
                },
                0,
                if func.arg_count == 0 {
                    format!("r[{dest}]=func()")
                } else if *start_reg == *start_reg + func.arg_count - 1 {
                    format!("r[{dest}]=func(r[{start_reg}])")
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
                *yield_reg as i64,
                jump_on_definition.as_debug_int() as i64,
                start_offset.as_debug_int() as i64,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::EndCoroutine { yield_reg } => (
                "EndCoroutine",
                *yield_reg as i64,
                0,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Yield {
                yield_reg,
                end_offset,
                ..
            } => (
                "Yield",
                *yield_reg as i64,
                end_offset.as_debug_int() as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Insert {
                cursor,
                key_reg,
                record_reg,
                flag,
                table_name,
            } => (
                "Insert",
                *cursor as i64,
                *record_reg as i64,
                *key_reg as i64,
                Value::build_text(table_name.clone()),
                flag.0 as i64,
                format!("intkey=r[{key_reg}] data=r[{record_reg}]"),
            ),
            Insn::Delete { cursor_id, table_name, .. } => (
                "Delete",
                *cursor_id as i64,
                0,
                0,
                Value::build_text(table_name.clone()),
                0,
                "".to_string(),
            ),
            Insn::IdxDelete {
                cursor_id,
                start_reg,
                num_regs,
                raise_error_if_no_matching_entry,
            } => (
                "IdxDelete",
                *cursor_id as i64,
                *start_reg as i64,
                *num_regs as i64,
                Value::build_text(""),
                *raise_error_if_no_matching_entry as i64,
                "".to_string(),
            ),
            Insn::NewRowid {
                cursor,
                rowid_reg,
                prev_largest_reg,
            } => (
                "NewRowid",
                *cursor as i64,
                *rowid_reg as i64,
                *prev_largest_reg as i64,
                Value::build_text(""),
                0,
                format!("r[{rowid_reg}]=rowid"),
            ),
            Insn::MustBeInt { reg, target_pc } => (
                "MustBeInt",
                *reg as i64,
                target_pc
                    .as_ref()
                    .map(|target_pc| target_pc.as_offset_int())
                    .unwrap_or_default() as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::SoftNull { reg } => (
                "SoftNull",
                *reg as i64,
                0,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::NoConflict {
                cursor_id,
                target_pc,
                record_reg,
                num_regs,
            } => {
                let key = if *num_regs > 0 {
                    format!("key=r[{}..{}]", record_reg, record_reg + num_regs - 1)
                } else {
                    format!("key=r[{record_reg}]")
                };
                (
                    "NoConflict",
                    *cursor_id as i64,
                    target_pc.as_debug_int() as i64,
                    *record_reg as i64,
                    Value::build_text(format!("{num_regs}")),
                    0,
                    key,
                )
            }
            Insn::NotExists {
                cursor,
                rowid_reg,
                target_pc,
            } => (
                "NotExists",
                *cursor as i64,
                target_pc.as_debug_int() as i64,
                *rowid_reg as i64,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::OffsetLimit {
                limit_reg,
                combined_reg,
                offset_reg,
            } => (
                "OffsetLimit",
                *limit_reg as i64,
                *combined_reg as i64,
                *offset_reg as i64,
                Value::build_text(""),
                0,
                format!(
                    "if r[{limit_reg}]>0 then r[{combined_reg}]=r[{limit_reg}]+max(0,r[{offset_reg}]) else r[{combined_reg}]=(-1)"
                ),
            ),
            Insn::OpenWrite {
                cursor_id,
                root_page,
                db,
                ..
            } => (
                "OpenWrite",
                *cursor_id as i64,
                match root_page {
                    RegisterOrLiteral::Literal(i) => *i as _,
                    RegisterOrLiteral::Register(i) => *i as _,
                },
                *db as i64,
                Value::build_text(""),
                0,
                format!("root={root_page}; iDb={db}"),
            ),
            Insn::Copy {
                src_reg,
                dst_reg,
                extra_amount,
            } => (
                "Copy",
                *src_reg as i64,
                *dst_reg as i64,
                *extra_amount as i64,
                Value::build_text(""),
                0,
                format!("r[{dst_reg}]=r[{src_reg}]"),
            ),
            Insn::CreateBtree { db, root, flags } => (
                "CreateBtree",
                *db as i64,
                *root as i64,
                flags.get_flags() as i64,
                Value::build_text(""),
                0,
                format!("r[{}]=root iDb={} flags={}", root, db, flags.get_flags()),
            ),
            Insn::IndexMethodCreate { db, cursor_id } => (
                "IndexMethodCreate",
                *db as i64,
                *cursor_id as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string()
            ),
            Insn::IndexMethodDestroy { db, cursor_id } => (
                "IndexMethodDestroy",
                *db as i64,
                *cursor_id as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string()
            ),
            Insn::IndexMethodOptimize { db, cursor_id } => (
                "IndexMethodOptimize",
                *db as i64,
                *cursor_id as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string()
            ),
            Insn::IndexMethodQuery { db, cursor_id, start_reg, .. } => (
                "IndexMethodQuery",
                *db as i64,
                *cursor_id as i64,
                *start_reg as i64,
                Value::build_text(""),
                0,
                "".to_string()
            ),
            Insn::ClearBtree { db, root } => (
                "ClearBtree",
                *root,
                *db as i64,
                0,
                Value::build_text(""),
                0,
                format!("root={root} iDb={db}"),
            ),
            Insn::Destroy {
                db,
                root,
                former_root_reg,
                is_temp,
            } => (
                "Destroy",
                *root,
                *former_root_reg as i64,
                *is_temp as i64,
                Value::build_text(""),
                0,
                format!(
                    "root iDb={db} former_root={former_root_reg} is_temp={is_temp}"
                ),
            ),
            Insn::ResetSorter { cursor_id } => (
                "ResetSorter",
                *cursor_id as i64,
                0,
                0,
                Value::build_text(""),
                0,
                format!("cursor={cursor_id}"),
            ),
            Insn::DropTable {
                db,
                _p2,
                _p3,
                table_name,
            } => (
                "DropTable",
                *db as i64,
                0,
                0,
                Value::build_text(table_name.clone()),
                0,
                format!("DROP TABLE {table_name}"),
            ),
            Insn::DropTrigger { db, trigger_name } => (
                "DropTrigger",
                *db as i64,
                0,
                0,
                Value::build_text(trigger_name.clone()),
                0,
                format!("DROP TRIGGER {trigger_name}"),
            ),
            Insn::DropType { db, type_name } => (
                "DropType",
                *db as i64,
                0,
                0,
                Value::build_text(type_name.clone()),
                0,
                format!("DROP TYPE {type_name}"),
            ),
            Insn::AddSequence { data } => (
                "AddSequence",
                data.db as i64,
                0,
                0,
                Value::build_text(data.name.clone()),
                0,
                format!("ADD SEQUENCE {}", data.name),
            ),
            Insn::DropSequence { db, seq_name } => (
                "DropSequence",
                *db as i64,
                0,
                0,
                Value::build_text(seq_name.clone()),
                0,
                format!("DROP SEQUENCE {seq_name}"),
            ),
            Insn::SequenceComputeNext {
                db,
                seq_name_reg,
                in_value_reg,
                in_is_called_reg,
                was_empty_reg,
                out_value_reg,
            } => (
                "SequenceComputeNext",
                *db as i64,
                *seq_name_reg as i64,
                *out_value_reg as i64,
                Value::Null,
                0,
                format!(
                    "r[{out_value_reg}] = next(r[{in_value_reg}], is_called=r[{in_is_called_reg}], empty=r[{was_empty_reg}])"
                ),
            ),
            Insn::SetSequenceCurrval {
                seq_name_reg,
                value_reg,
            } => (
                "SetSequenceCurrval",
                0,
                *seq_name_reg as i64,
                *value_reg as i64,
                Value::Null,
                0,
                format!("currval(r[{seq_name_reg}]) = r[{value_reg}]"),
            ),
            Insn::SequenceTrackAllocation {
                db,
                seq_name_reg,
                value_reg,
            } => (
                "SequenceTrackAllocation",
                *db as i64,
                *seq_name_reg as i64,
                *value_reg as i64,
                Value::Null,
                0,
                format!("track sequence allocation r[{seq_name_reg}] = r[{value_reg}]"),
            ),
            Insn::SequenceRegisterAllocation {
                db,
                seq_name_reg,
                value_reg,
                saved_outer_reg,
            } => (
                "SequenceRegisterAllocation",
                *db as i64,
                *seq_name_reg as i64,
                *value_reg as i64,
                Value::Null,
                0,
                format!(
                    "register allocation r[{seq_name_reg}] = r[{value_reg}] \
                     against outer mv_tx r[{saved_outer_reg}]"
                ),
            ),
            Insn::SequenceBeginInnerTx {
                db,
                path_kind_reg,
                saved_outer_reg,
            } => (
                "SequenceBeginInnerTx",
                *db as i64,
                *path_kind_reg as i64,
                *saved_outer_reg as i64,
                Value::Null,
                0,
                format!(
                    "r[{path_kind_reg}] = path; r[{saved_outer_reg}] = saved outer mv_tx"
                ),
            ),
            Insn::SequenceCommitInnerTx {
                db,
                path_kind_reg,
                saved_outer_reg,
                status_reg,
            } => (
                "SequenceCommitInnerTx",
                *db as i64,
                *path_kind_reg as i64,
                *saved_outer_reg as i64,
                Value::from_i64(*status_reg as i64),
                0,
                format!(
                    "commit inner tx; r[{status_reg}] = 0=Ok | 1=ConflictRetry"
                ),
            ),
            Insn::AddType { db, sql } => (
                "AddType",
                *db as i64,
                0,
                0,
                Value::build_text(sql.clone()),
                0,
                "ADD TYPE".to_string(),
            ),
            Insn::DropView { db, view_name } => (
                "DropView",
                *db as i64,
                0,
                0,
                Value::build_text(view_name.clone()),
                0,
                format!("DROP VIEW {view_name}"),
            ),
            Insn::DropIndex { db: _, index } => (
                "DropIndex",
                0,
                0,
                0,
                Value::build_text(index.name.clone()),
                0,
                format!("DROP INDEX {}", index.name),
            ),
            Insn::Close { cursor_id } => (
                "Close",
                *cursor_id as i64,
                0,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Last {
                cursor_id,
                pc_if_empty,
            } => (
                "Last",
                *cursor_id as i64,
                pc_if_empty.as_debug_int() as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::IsNull { reg, target_pc } => (
                "IsNull",
                *reg as i64,
                target_pc.as_debug_int() as i64,
                0,
                Value::build_text(""),
                0,
                format!("if (r[{}]==NULL) goto {}", reg, target_pc.as_debug_int()),
            ),
            Insn::ParseSchema {
                db, where_clause, ..
            } => (
                "ParseSchema",
                *db as i64,
                0,
                0,
                Value::build_text(where_clause.clone().unwrap_or_else(|| "NULL".to_string())),
                0,
                where_clause.clone().unwrap_or_else(|| "NULL".to_string()),
            ),
            Insn::PopulateMaterializedViews { cursors } => (
                "PopulateMaterializedViews",
                0,
                0,
                0,
                Value::Null,
                cursors.len() as i64,
                "".to_string(),
            ),
            Insn::Prev {
                cursor_id,
                pc_if_prev,
                fullscan,
                ..
            } => (
                "Prev",
                *cursor_id as i64,
                pc_if_prev.as_debug_int() as i64,
                0,
                Value::build_text(""),
                // SQLite sets P5 to SQLITE_STMTSTATUS_FULLSCAN_STEP (1) on
                // full-table-scan steps
                *fullscan as i64,
                "".to_string(),
            ),
            Insn::ShiftRight { lhs, rhs, dest } => (
                "ShiftRight",
                *rhs as i64,
                *lhs as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}] >> r[{rhs}]"),
            ),
            Insn::ShiftLeft { lhs, rhs, dest } => (
                "ShiftLeft",
                *rhs as i64,
                *lhs as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}] << r[{rhs}]"),
            ),
            Insn::AddImm { register, value } => (
                "AddImm",
                *register as i64,
                *value,
                0,
                Value::build_text(""),
                0,
                format!("r[{register}]=r[{register}]+{value}"),
            ),
            Insn::Variable { index, dest } => (
                "Variable",
                usize::from(*index) as i64,
                *dest as i64,
                0,
                Value::build_text(""),
                0,
                format!("r[{}]=parameter({})", *dest, *index),
            ),
            Insn::ZeroOrNull { rg1, rg2, dest } => (
                "ZeroOrNull",
                *rg1 as i64,
                *dest as i64,
                *rg2 as i64,
                Value::build_text(""),
                0,
                format!(
                    "((r[{rg1}]=NULL)|(r[{rg2}]=NULL)) ? r[{dest}]=NULL : r[{dest}]=0"
                ),
            ),
            Insn::Not { reg, dest } => (
                "Not",
                *reg as i64,
                *dest as i64,
                0,
                Value::build_text(""),
                0,
                format!("r[{dest}]=!r[{reg}]"),
            ),
            Insn::IsTrue { reg, dest, null_value, invert } => (
                "IsTrue",
                *reg as i64,
                *dest as i64,
                if *null_value { 1 } else { 0 },
                Value::build_text(""),
                if *invert { 1 } else { 0 },
                format!("r[{dest}] = IsTrue(r[{reg}], null={}, invert={})", *null_value as i64, *invert as i64),
            ),
            Insn::Concat { lhs, rhs, dest } => (
                "Concat",
                *rhs as i64,
                *lhs as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=r[{lhs}] + r[{rhs}]"),
            ),
            Insn::And { lhs, rhs, dest } => (
                "And",
                *rhs as i64,
                *lhs as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=(r[{lhs}] && r[{rhs}])"),
            ),
            Insn::Or { lhs, rhs, dest } => (
                "Or",
                *rhs as i64,
                *lhs as i64,
                *dest as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=(r[{lhs}] || r[{rhs}])"),
            ),
            Insn::Noop => ("Noop", 0, 0, 0, Value::build_text(""), 0, String::new()),
            Insn::PageCount { db, dest } => (
                "Pagecount",
                *db as i64,
                *dest as i64,
                0,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::ReadCookie { db, dest, cookie } => (
                "ReadCookie",
                *db as i64,
                *dest as i64,
                *cookie as i64,
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Filter{cursor_id, target_pc, key_reg, num_keys} => (
                "Filter",
                *cursor_id as i64,
                target_pc.as_debug_int() as i64,
                *key_reg as i64,
                Value::build_text(""),
                *num_keys as i64,
                format!("if !bloom_filter(r[{}..{}]) goto {}", key_reg, key_reg + num_keys, target_pc.as_debug_int()),
            ),
            Insn::FilterAdd{cursor_id, key_reg, num_keys} => (
                "FilterAdd",
                *cursor_id as i64,
                *key_reg as i64,
                *num_keys as i64,
                Value::build_text(""),
                0,
                format!("bloom_filter_add(r[{}..{}])", key_reg, key_reg + num_keys),
            ),
            Insn::SetCookie {
                db,
                cookie,
                value,
                p5,
            } => (
                "SetCookie",
                *db as i64,
                *cookie as i64,
                *value as i64,
                Value::build_text(""),
                *p5 as i64,
                "".to_string(),
            ),
            Insn::AutoCommit {
                auto_commit,
                rollback,
            } => (
                "AutoCommit",
                *auto_commit as i64,
                *rollback as i64,
                0,
                Value::build_text(""),
                0,
                format!("auto_commit={auto_commit}, rollback={rollback}"),
            ),
            Insn::Savepoint { op, name } => (
                "Savepoint",
                0,
                0,
                0,
                Value::build_text(name.clone()),
                0,
                format!("op={op:?}, name={name}"),
            ),
            Insn::OpenEphemeral {
                cursor_id,
                is_table,
            } => (
                "OpenEphemeral",
                *cursor_id as i64,
                *is_table as i64,
                0,
                Value::build_text(""),
                0,
                format!(
                    "cursor={} is_table={}",
                    cursor_id,
                    if *is_table { "true" } else { "false" }
                ),
            ),
            Insn::OpenAutoindex { cursor_id } => (
                "OpenAutoindex",
                *cursor_id as i64,
                0,
                0,
                Value::build_text(""),
                0,
                format!("cursor={cursor_id}"),
            ),
            Insn::OpenDup { new_cursor_id, original_cursor_id } => (
                "OpenDup",
                *new_cursor_id as i64,
                *original_cursor_id as i64,
                0,
                Value::build_text(""),
                0,
                format!("new_cursor={new_cursor_id}, original_cursor={original_cursor_id}"),
            ),
            Insn::Once {
                target_pc_when_reentered,
            } => (
                "Once",
                target_pc_when_reentered.as_debug_int() as i64,
                0,
                0,
                Value::build_text(""),
                0,
                format!("goto {}", target_pc_when_reentered.as_debug_int()),
            ),
            Insn::ResetOnce { region_end } => (
                "ResetOnce",
                region_end.as_debug_int() as i64,
                0,
                0,
                Value::build_text(""),
                0,
                format!("clear once flags before {}", region_end.as_debug_int()),
            ),
            Insn::BeginSubrtn { dest, dest_end } => (
                "BeginSubrtn",
                *dest as i64,
                dest_end.map_or(0, |end| end as i64),
                0,
                Value::build_text(""),
                0,
                dest_end.map_or(format!("r[{dest}]=NULL"), |end| {
                    format!("r[{dest}..{end}]=NULL")
                }),
            ),
            Insn::NotFound {
                cursor_id,
                target_pc,
                record_reg,
                ..
            }
            | Insn::Found {
                cursor_id,
                target_pc,
                record_reg,
                ..
            } => (
                if matches!(insn, Insn::NotFound { .. }) {
                    "NotFound"
                } else {
                    "Found"
                },
                *cursor_id as i64,
                target_pc.as_debug_int() as i64,
                *record_reg as i64,
                Value::build_text(""),
                0,
                format!(
                    "if {}found goto {}",
                    if matches!(insn, Insn::NotFound { .. }) {
                        "not "
                    } else {
                        ""
                    },
                    target_pc.as_debug_int()
                ),
            ),
            Insn::Affinity {
                start_reg,
                count,
                affinities,
            } => (
                "Affinity",
                *start_reg as i64,
                count.get() as i64,
                0,
                Value::build_text(""),
                0,
                format!(
                    "r[{}..{}] = {}",
                    start_reg,
                    start_reg + count.get(),
                    affinities
                        .chars()
                        .map(|a| a.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ),
            Insn::Count {
                cursor_id,
                target_reg,
                exact,
            } => (
                "Count",
                *cursor_id as i64,
                *target_reg as i64,
                if *exact { 0 } else { 1 },
                Value::build_text(""),
                0,
                "".to_string(),
            ),
            Insn::Int64 {
                _p1,
                out_reg,
                _p3,
                value,
            } => (
                "Int64",
                0,
                *out_reg as i64,
                0,
                Value::from_i64(*value),
                0,
                format!("r[{}]={}", *out_reg, *value),
            ),
            Insn::IntegrityCk { data } => {
                let IntegrityCkData {
                    db,
                    max_errors,
                    roots,
                    dropped_roots,
                    message_register,
                } = data.as_ref();
                (
                    "IntegrityCk",
                    *max_errors as i64,
                    0,
                    0,
                    Value::build_text(""),
                    0,
                    format!("db={db} roots={roots:?} dropped_roots={dropped_roots:?} message_register={message_register}"),
                )
            }
            Insn::RowData { cursor_id, dest } => (
                "RowData",
                *cursor_id as i64,
                *dest as i64,
                0,
                Value::build_text(""),
                0,
                format!("r[{}] = data", *dest),
            ),
            Insn::Cast { reg, affinity } => (
                "Cast",
                *reg as i64,
                0,
                0,
                Value::build_text(""),
                0,
                format!("affinity(r[{}]={:?})", *reg, affinity),
            ),
            Insn::RenameTable { db: _, from, to } => (
                "RenameTable",
                0,
                0,
                0,
                Value::build_text(""),
                0,
                format!("rename_table({from}, {to})"),
            ),
            Insn::DropColumn { db: _, table, column_index } => (
                "DropColumn",
                0,
                0,
                0,
                Value::build_text(""),
                0,
                format!("drop_column({table}, {column_index})"),
            ),
            Insn::AddColumn { data } => (
                "AddColumn",
                0,
                0,
                0,
                Value::build_text(""),
                0,
                format!("add_column({}, {:?})", data.table, data.column),
            ),
            Insn::AlterColumn { db: _, table, column_index, definition: column, rename } => (
                "AlterColumn",
                0,
                0,
                0,
                Value::build_text(""),
                0,
                format!("alter_column({table}, {column_index}, {column:?}, {rename:?})"),
            ),
            Insn::MaxPgcnt { db, dest, new_max } => (
                "MaxPgcnt",
                *db as i64,
                *dest as i64,
                *new_max as i64,
                Value::build_text(""),
                0,
                format!("r[{dest}]=max_page_count(db[{db}],{new_max})"),
            ),
            Insn::JournalMode { db, dest, new_mode } => (
                "JournalMode",
                *db as i64,
                *dest as i64,
                0,
                Value::build_text(new_mode.clone().unwrap_or(String::new())),
                0,
                format!("r[{dest}]=journal_mode(db[{db}]{})",
                    new_mode.as_ref().map_or(String::new(), |m| format!(",'{m}'"))),
            ),
            Insn::IfNeg { reg, target_pc } => (
                "IfNeg",
                *reg as i64,
                target_pc.as_debug_int() as i64,
                0,
                Value::build_text(""),
                0,
                format!("if (r[{}] < 0) goto {}", reg, target_pc.as_debug_int()),
            ),
            Insn::Explain { p1, p2, detail } => (
                "Explain",
                *p1 as i64,
                p2.as_ref().map(|p| *p).unwrap_or(0) as i64,
                0,
                Value::build_text(detail.to_string()),
                0,
                String::new(),
            ),
            Insn::MemMax { dest_reg, src_reg } => (
                "MemMax",
                *dest_reg as i64,
                *src_reg as i64,
                0,
                Value::build_text(""),
                0,
                format!("r[{dest_reg}]=Max(r[{dest_reg}],r[{src_reg}])"),
            ),
        Insn::Sequence{ cursor_id, target_reg} => (
                "Sequence",
                *cursor_id as i64,
                *target_reg as i64,
                0,
                Value::build_text(""),
                0,
                String::new(),
          ),
        Insn::SequenceTest{ cursor_id, target_pc, value_reg } => (
            "SequenceTest",
              *cursor_id as i64,
            target_pc.as_debug_int() as i64,
            *value_reg as i64,
            Value::build_text(""),
            0,
            String::new(),
        ),
        Insn::FkCounter{increment_value, deferred } => (
        "FkCounter",
            *increment_value as i64,
            *deferred as i64,
            0,
            Value::build_text(""),
            0,
            String::new(),
        ),
        Insn::FkIfZero{target_pc, deferred } => (
        "FkIfZero",
            target_pc.as_debug_int() as i64,
            *deferred as i64,
            0,
            Value::build_text(""),
            0,
            String::new(),
        ),
        Insn::FkCheck{ deferred } => (
        "FkCheck",
            *deferred as i64,
            0,
            0,
            Value::build_text(""),
            0,
            String::new(),
        ),
        Insn::HashBuild { data } => {
            let payload_info = if let Some(p_reg) = data.payload_start_reg {
                format!(" payload=r[{}]..r[{}]", p_reg, p_reg + data.num_payload - 1)
            } else {
                String::new()
            };
            (
                "HashBuild",
                data.cursor_id as i64,
                data.key_start_reg as i64,
                data.num_keys as i64,
                Value::build_text(format!("r=[{}] budget={}{payload_info}", data.hash_table_id, data.mem_budget)),
                0,
                String::new(),
            )
        }
        Insn::HashBuildFinalize{hash_table_id: hash_table_reg} => (
            "HashBuildFinalize",
            *hash_table_reg as i64,
            0,
            0,
            Value::build_text(""),
            0,
            String::new(),
        ),
        Insn::HashProbe{hash_table_id: hash_table_reg, key_start_reg, num_keys, dest_reg, target_pc, payload_dest_reg, num_payload, probe_rowid_reg: _} => {
            let payload_info = if let Some(p_reg) = payload_dest_reg {
                format!(" payload=r[{}]..r[{}]", p_reg, p_reg + num_payload - 1)
            } else {
                String::new()
            };
            (
                "HashProbe",
                *hash_table_reg as i64,
                *key_start_reg as i64,
                *num_keys as i64,
                Value::build_text(format!("r[{}]={}{}", dest_reg, target_pc.as_debug_int(), payload_info)),
                0,
                String::new(),
            )
        }
        Insn::HashNext{hash_table_id: hash_table_reg, dest_reg, target_pc, payload_dest_reg, num_payload} => {
            let payload_info = if let Some(p_reg) = payload_dest_reg {
                format!(" payload=r[{}]..r[{}]", p_reg, p_reg + num_payload - 1)
            } else {
                String::new()
            };
            (
                "HashNext",
                *hash_table_reg as i64,
                *dest_reg as i64,
                target_pc.as_debug_int() as i64,
                Value::build_text(payload_info),
                0,
                String::new(),
            )
        }
        Insn::HashDistinct { data } => (
            "HashDistinct",
            data.hash_table_id as i64,
            data.key_start_reg as i64,
            data.num_keys as i64,
            Value::build_text(format!("jmp={}", data.target_pc.as_debug_int())),
            0,
            String::new(),
        ),
        Insn::HashClose{hash_table_id: hash_table_reg} => (
            "HashClose",
            *hash_table_reg as i64,
            0,
            0,
            Value::build_text(""),
            0,
            String::new(),
        ),
        Insn::HashClear { hash_table_id: hash_table_reg } => (
            "HashClear",
            *hash_table_reg as i64,
            0,
            0,
            Value::build_text(""),
            0,
            String::new(),
        ),
        Insn::HashMarkMatched { hash_table_id } => (
            "HashMarkMatched",
            *hash_table_id as i64,
            0,
            0,
            Value::build_text(""),
            0,
            String::new(),
        ),
        Insn::HashResetMatched { hash_table_id } => (
            "HashResetMatched",
            *hash_table_id as i64,
            0,
            0,
            Value::build_text(""),
            0,
            String::new(),
        ),
        Insn::HashScanUnmatched { hash_table_id, dest_reg, target_pc, payload_dest_reg, num_payload } => {
            let payload_info = if let Some(p_reg) = payload_dest_reg {
                format!(" payload=r[{}]..r[{}]", p_reg, p_reg + num_payload - 1)
            } else {
                String::new()
            };
            (
                "HashScanUnmatched",
                *hash_table_id as i64,
                *dest_reg as i64,
                target_pc.as_debug_int() as i64,
                Value::build_text(""),
                0,
                format!("hash_table_id={hash_table_id}{payload_info}"),
            )
        },
        Insn::HashNextUnmatched { hash_table_id, dest_reg, target_pc, payload_dest_reg, num_payload } => {
            let payload_info = if let Some(p_reg) = payload_dest_reg {
                format!(" payload=r[{}]..r[{}]", p_reg, p_reg + num_payload - 1)
            } else {
                String::new()
            };
            (
                "HashNextUnmatched",
                *hash_table_id as i64,
                *dest_reg as i64,
                target_pc.as_debug_int() as i64,
                Value::build_text(""),
                0,
                format!("hash_table_id={hash_table_id}{payload_info}"),
            )
        },
        Insn::HashGraceInit { hash_table_id, target_pc } => {
            (
                "HashGraceInit",
                *hash_table_id as i64,
                0,
                target_pc.as_debug_int() as i64,
                Value::build_text(""),
                0,
                format!("hash_table_id={hash_table_id}"),
            )
        },
        Insn::HashGraceLoadPartition { hash_table_id, target_pc } => {
            (
                "HashGraceLoadPart",
                *hash_table_id as i64,
                0,
                target_pc.as_debug_int() as i64,
                Value::build_text(""),
                0,
                format!("hash_table_id={hash_table_id}"),
            )
        },
        Insn::HashGraceNextProbe { hash_table_id, key_start_reg, num_keys, probe_rowid_dest, target_pc } => {
            (
                "HashGraceNextProbe",
                *hash_table_id as i64,
                *key_start_reg as i64,
                target_pc.as_debug_int() as i64,
                Value::build_text(""),
                0,
                format!("hash_table_id={hash_table_id} keys=r[{}]..r[{}] probe_rowid_dest=r[{probe_rowid_dest}]", key_start_reg, *key_start_reg + *num_keys - 1),
            )
        },
        Insn::HashGraceAdvancePartition { hash_table_id, target_pc } => {
            (
                "HashGraceAdvPart",
                *hash_table_id as i64,
                0,
                target_pc.as_debug_int() as i64,
                Value::build_text(""),
                0,
                format!("hash_table_id={hash_table_id}"),
            )
        },
        Insn::VacuumInto { schema_name, dest_path } => (
            "VacuumInto",
            0,
            0,
            0,
            Value::build_text(dest_path.to_string()),
            0,
            format!("schema={schema_name}, dest={dest_path}"),
        ),
        Insn::Vacuum { db } => (
            "Vacuum",
            *db as i64,
            0,
            0,
            Value::Null,
            0,
            format!("db={db}"),
        ),
        Insn::InitCdcVersion { cdc_table_name, version, cdc_mode } => (
            "InitCdcVersion",
            0,
            0,
            0,
            Value::build_text(format!("{cdc_table_name}={version}")),
            0,
            format!("ensure turso_cdc_version({cdc_table_name}, {version}); set cdc={cdc_mode}"),
        ),
    }
}

pub fn insn_to_row_with_comment(
    program: &PreparedProgram,
    insn: &Insn,
    manual_comment: Option<&str>,
) -> (&'static str, i64, i64, i64, Value, i64, String) {
    let (opcode, p1, p2, p3, p4, p5, comment) = insn_to_row(program, insn);
    (
        opcode,
        p1,
        p2,
        p3,
        p4,
        p5,
        manual_comment.map_or(comment.to_string(), |mc| format!("{comment}; {mc}")),
    )
}

pub fn insn_to_str(
    program: &PreparedProgram,
    addr: InsnReference,
    insn: &Insn,
    indent: String,
    manual_comment: Option<&str>,
) -> String {
    let (opcode, p1, p2, p3, p4, p5, comment) = insn_to_row(program, insn);
    format!(
        "{:<4}  {:<17}  {:<4}  {:<4}  {:<4}  {:<13}  {:<2}  {}",
        addr,
        &(indent + opcode),
        p1,
        p2,
        p3,
        p4.to_string(),
        p5,
        manual_comment.map_or(comment.to_string(), |mc| format!("{comment}; {mc}"))
    )
}
