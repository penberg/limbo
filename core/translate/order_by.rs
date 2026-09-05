use crate::{alloc::TursoVecExt, sync::Arc};

use crate::alloc::*;
use turso_parser::ast::{self, SortOrder};

use crate::{
    emit_explain,
    schema::{Index, IndexColumn, PseudoCursorType, Schema},
    translate::{
        collate::{get_collseq_from_expr_with_symbols, CollationSeq},
        group_by::is_orderby_agg_or_const,
        plan::Aggregate,
    },
    util::exprs_are_equivalent,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::{to_u32, IdxInsertFlags, Insn, SorterOpenData},
    },
    Result,
};

use super::{
    emitter::TranslateCtx,
    expr::translate_expr,
    plan::{Distinctness, ResultSetColumn, SelectPlan, TableReferences},
    result_row::{emit_offset, emit_result_row_and_limit},
};

use crate::translate::eqp::{EqpDetail, EqpSortMethod};
use crate::vdbe::insn::SortComparatorType;

/// Maps a custom type `<` operator function name to a SortComparatorType.
/// Returns None if the function name is not recognized.
fn sort_comparator_from_func_name(func_name: &str) -> Option<SortComparatorType> {
    match func_name {
        "numeric_lt" => Some(SortComparatorType::NumericLt),
        "test_uint_lt" => Some(SortComparatorType::TestUintLt),
        "string_reverse" => Some(SortComparatorType::StringReverse),
        "array_lt" => Some(SortComparatorType::ArrayLt),
        _ => None,
    }
}

/// For an ORDER BY expression that is a column reference to a custom type,
/// returns the SortComparatorType if the type has a `<` operator with a known
/// comparator. Returns None otherwise, which causes the sorter to use encoded
/// blob ordering instead of silently wrong results.
pub(crate) fn custom_type_comparator(
    expr: &ast::Expr,
    referenced_tables: &TableReferences,
    schema: &Schema,
) -> Option<SortComparatorType> {
    if let ast::Expr::Column {
        table: table_ref_id,
        column,
        ..
    } = expr
    {
        let (_, table) = referenced_tables.find_table_by_internal_id(*table_ref_id)?;
        let col = table.get_column_at(*column)?;
        // Array columns use element-wise comparison
        if col.is_array() {
            return Some(SortComparatorType::ArrayLt);
        }
        let type_def = schema.get_type_def(&col.ty_str, table.is_strict())?;
        type_def
            .operators()
            .iter()
            .find(|op| op.op == "<")
            .and_then(|op| op.func_name.as_ref())
            .and_then(|func_name| sort_comparator_from_func_name(func_name))
    } else if super::expr::expr_is_array(expr, Some(referenced_tables)) {
        Some(SortComparatorType::ArrayLt)
    } else {
        None
    }
}

/// For a result column expression that is a column reference to a custom type,
/// returns the column definition and type definition.
fn result_column_custom_type_info<'a>(
    expr: &ast::Expr,
    referenced_tables: &'a TableReferences,
    schema: &'a Schema,
) -> Option<(
    &'a crate::schema::Column,
    std::sync::Arc<crate::schema::TypeDef>,
)> {
    if let ast::Expr::Column {
        table: table_ref_id,
        column,
        ..
    } = expr
    {
        let (_, table) = referenced_tables.find_table_by_internal_id(*table_ref_id)?;
        let col = table.get_column_at(*column)?;
        let type_def = schema.get_type_def(&col.ty_str, table.is_strict())?.clone();
        Some((col, type_def))
    } else {
        None
    }
}

/// Returns true if the expression is a column reference to a custom type
/// (with encode/decode) that does NOT have a `<` operator with a known
/// sort comparator. This includes types with no `<` operator at all, and
/// types whose `<` function is not recognized by the sorter.
fn is_custom_type_without_lt(
    expr: &ast::Expr,
    referenced_tables: &TableReferences,
    schema: &Schema,
) -> bool {
    if let ast::Expr::Column {
        table: table_ref_id,
        column,
        ..
    } = expr
    {
        if let Some((_, table)) = referenced_tables.find_table_by_internal_id(*table_ref_id) {
            if let Some(col) = table.get_column_at(*column) {
                if let Some(type_def) = schema.get_type_def(&col.ty_str, table.is_strict()) {
                    if type_def.decode().is_some() {
                        // No `<` operator at all (naked or with function)
                        return !type_def.operators().iter().any(|op| op.op == "<");
                    }
                }
            }
        }
    }
    false
}

// Metadata for handling ORDER BY operations
#[derive(Debug)]
pub struct SortMetadata {
    // cursor id for the Sorter table where the sorted rows are stored
    pub sort_cursor: usize,
    // register where the sorter data is inserted and later retrieved from
    pub reg_sorter_data: usize,
    // We need to emit result columns in the order they are present in the SELECT, but they may not be in the same order in the ORDER BY sorter.
    // This vector holds the indexes of the result columns in the ORDER BY sorter.
    // This vector must be the same length as the result columns.
    pub remappings: Vec<OrderByRemapping>,
    /// Whether we append an extra ascending "Sequence" key to the ORDER BY sort keys.
    /// This is used *only* when a GROUP BY is present *and* ORDER BY is not purely
    /// aggregates/constants, so that rows that tie on ORDER BY terms are output in
    /// the same relative order the underlying row stream produced them.
    pub has_sequence: bool,
    /// Whether to use heap-sort with BTreeIndex instead of full-collection sort through Sorter
    pub use_heap_sort: bool,
}
pub struct EmitOrderBy;

impl EmitOrderBy {
    /// Initialize resources needed for ORDER BY processing
    #[allow(clippy::too_many_arguments)]
    pub fn init(
        program: &mut ProgramBuilder,
        t_ctx: &mut TranslateCtx,
        result_columns: &[ResultSetColumn],
        order_by: &[(
            Box<ast::Expr>,
            SortOrder,
            Option<turso_parser::ast::NullsOrder>,
        )],
        referenced_tables: &TableReferences,
        has_group_by: bool,
        has_distinct: bool,
        aggregates: &[Aggregate],
    ) -> Result<()> {
        // Block ORDER BY on custom type columns without OPERATOR '<'
        for (expr, _, _) in order_by.iter() {
            if is_custom_type_without_lt(expr, referenced_tables, t_ctx.resolver.schema()) {
                if let Some((col, type_def)) =
                    result_column_custom_type_info(expr, referenced_tables, t_ctx.resolver.schema())
                {
                    let col_name = col.name.as_deref().unwrap_or("?");
                    crate::bail_parse_error!(
                    "cannot ORDER BY column '{}' of type '{}': type does not declare OPERATOR '<'",
                    col_name,
                    type_def.name
                );
                }
                crate::bail_parse_error!(
                    "cannot ORDER BY a custom type column that does not declare OPERATOR '<'"
                );
            }
        }

        let only_aggs = order_by
            .iter()
            .all(|(e, _, _)| is_orderby_agg_or_const(&t_ctx.resolver, e, aggregates));

        let has_explicit_nulls = order_by.iter().any(|(_, _, nulls)| nulls.is_some());
        let use_heap_sort =
            !has_distinct && !has_group_by && t_ctx.limit_ctx.is_some() && !has_explicit_nulls;

        // only emit sequence column if (we have GROUP BY and ORDER BY is not only aggregates or constants) OR (we decided to use heap-sort)
        let has_sequence = (has_group_by && !only_aggs) || use_heap_sort;

        let remappings =
            order_by_deduplicate_result_columns(order_by, result_columns, has_sequence)?;
        let sort_cursor = if use_heap_sort {
            let index_name = format!("heap_sort_{}", program.offset().as_offset_int()); // we don't really care about the name that much, just enough that we don't get name collisions
            let mut index_columns =
                Vec::try_with_capacity_ext(order_by.len() + result_columns.len())?;
            for (column, order, nulls) in order_by {
                if nulls.is_some() {
                    return Err(crate::LimboError::InternalError(
                        "heap sort cannot express an explicit NULLS ordering".to_string(),
                    ));
                }
                let collation = get_collseq_from_expr_with_symbols(
                    column,
                    referenced_tables,
                    Some(t_ctx.resolver.symbol_table),
                )?;
                let pos_in_table = index_columns.len();
                // Have enough space pre-allocatoed to push without realloc
                index_columns.push(IndexColumn {
                    name: pos_in_table.to_string(),
                    order: *order,
                    nulls_order: None,
                    pos_in_table,
                    collation,
                    default: None,
                    expr: None,
                });
            }
            let pos_in_table = index_columns.len();
            // add sequence number between ORDER BY columns and result column
            index_columns.try_push(IndexColumn {
                name: pos_in_table.to_string(),
                order: SortOrder::Asc,
                nulls_order: None,
                pos_in_table,
                collation: None,
                default: None,
                expr: None,
            })?;
            for _ in remappings.iter().filter(|r| !r.deduplicated) {
                let pos_in_table = index_columns.len();
                index_columns.try_push(IndexColumn::new(pos_in_table.to_string(), pos_in_table))?;
            }
            let index = Arc::new(Index {
                name: index_name,
                table_name: String::new(),
                ephemeral: true,
                root_page: 0,
                columns: index_columns,
                unique: false,
                has_rowid: false,
                where_clause: None,
                index_method: None,
                on_conflict: None,
            });
            program.alloc_cursor_id(CursorType::BTreeIndex(index))
        } else {
            program.alloc_cursor_id(CursorType::Sorter)
        };
        t_ctx.meta_sort = Some(SortMetadata {
            sort_cursor,
            reg_sorter_data: program.alloc_register(),
            remappings,
            has_sequence,
            use_heap_sort,
        });

        if use_heap_sort {
            program.emit_insn(Insn::OpenEphemeral {
                cursor_id: sort_cursor,
                is_table: false,
            });
        } else {
            /*
             * Terms of the ORDER BY clause that is part of a SELECT statement may be assigned a collating sequence using the COLLATE operator,
             * in which case the specified collating function is used for sorting.
             * Otherwise, if the expression sorted by an ORDER BY clause is a column,
             * then the collating sequence of the column is used to determine sort order.
             * If the expression is not a column and has no COLLATE clause, then the BINARY collating sequence is used.
             */
            let mut order_collations_nulls: Vec<(
                SortOrder,
                Option<CollationSeq>,
                Option<turso_parser::ast::NullsOrder>,
            )> = order_by
                .iter()
                .map(|(expr, dir, nulls)| {
                    let collation = get_collseq_from_expr_with_symbols(
                        expr,
                        referenced_tables,
                        Some(t_ctx.resolver.symbol_table),
                    )?;
                    Ok::<_, crate::LimboError>((*dir, collation, *nulls))
                })
                .try_collect::<Result<Vec<_>>>()??;

            // Resolve custom type comparators for ORDER BY columns.
            // For types with a `<` operator, the comparator is used for correct sort ordering.
            let mut comparators: Vec<Option<SortComparatorType>> = order_by
                .iter()
                .map(|(expr, _, _)| {
                    custom_type_comparator(expr, referenced_tables, t_ctx.resolver.schema())
                })
                .try_collect()?;

            if has_sequence {
                // sequence column: ascending with BINARY collation, no comparator, no nulls order
                order_collations_nulls.push((SortOrder::Asc, Some(CollationSeq::default()), None));
                comparators.try_push(None)?;
            }

            let key_len = order_collations_nulls.len();

            program.emit_insn(Insn::SorterOpen {
                data: Box::new(SorterOpenData {
                    cursor_id: sort_cursor,
                    columns: key_len,
                    order_collations_nulls,
                    comparators,
                }),
            });
        }
        Ok(())
    }

    /// Emits the bytecode for outputting rows from an ORDER BY sorter.
    /// This is called when the main query execution loop has finished processing,
    /// and we can now emit rows from the ORDER BY sorter.
    pub fn emit(
        program: &mut ProgramBuilder,
        t_ctx: &mut TranslateCtx,
        plan: &SelectPlan,
    ) -> Result<()> {
        let order_by = &plan.order_by;
        let result_columns = &plan.result_columns;
        let sort_loop_start_label = program.allocate_label();
        let sort_loop_next_label = program.allocate_label();
        let sort_loop_end_label = program.allocate_label();
        let SortMetadata {
            sort_cursor,
            reg_sorter_data,
            ref remappings,
            has_sequence,
            use_heap_sort,
        } = *t_ctx.meta_sort.as_ref().unwrap();

        let sorter_column_count = order_by.len()
            + if has_sequence { 1 } else { 0 }
            + remappings.iter().filter(|r| !r.deduplicated).count();

        if use_heap_sort {
            emit_explain!(
                program,
                false,
                EqpDetail::OrderBy {
                    method: EqpSortMethod::TempBTree,
                }
            );
        } else {
            emit_explain!(
                program,
                false,
                EqpDetail::OrderBy {
                    method: EqpSortMethod::Sorter,
                }
            );
        }

        let cursor_id = if !use_heap_sort {
            let pseudo_cursor = program.alloc_cursor_id(CursorType::Pseudo(PseudoCursorType {
                column_count: sorter_column_count,
            }));

            program.emit_insn(Insn::OpenPseudo {
                cursor_id: pseudo_cursor,
                content_reg: reg_sorter_data,
                num_fields: sorter_column_count,
            });

            program.emit_insn(Insn::SorterSort {
                cursor_id: sort_cursor,
                pc_if_empty: sort_loop_end_label,
            });
            pseudo_cursor
        } else {
            program.emit_insn(Insn::Rewind {
                cursor_id: sort_cursor,
                pc_if_empty: sort_loop_end_label,
            });
            sort_cursor
        };

        program.preassign_label_to_next_insn(sort_loop_start_label);

        emit_offset(program, sort_loop_next_label, t_ctx.reg_offset);

        if !use_heap_sort {
            program.emit_insn(Insn::SorterData {
                cursor_id: sort_cursor,
                dest_reg: reg_sorter_data,
                pseudo_cursor: cursor_id,
            });
        }

        // We emit the columns in SELECT order, not sorter order (sorter always has the sort keys first).
        // This is tracked in sort_metadata.remappings.
        let start_reg = t_ctx.reg_result_cols_start.unwrap();
        for (i, rc) in result_columns.iter().enumerate() {
            let reg = start_reg + i;
            let remapping = remappings
                .get(i)
                .expect("remapping must exist for all result columns");

            let column_idx = remapping.orderby_sorter_idx;
            program.emit_column_or_rowid(cursor_id, column_idx, reg);

            // Deduplicated columns share a sort key slot, which stores the encoded
            // (on-disk) value (decode was suppressed during sorter insert). Apply
            // DECODE now so the result set contains human-readable values.
            if remapping.deduplicated {
                if let Some((col, type_def)) = result_column_custom_type_info(
                    &rc.expr,
                    &plan.table_references,
                    t_ctx.resolver.schema(),
                ) {
                    if let Some(decode_expr) = type_def.decode() {
                        let skip_label = program.allocate_label();
                        program.emit_insn(Insn::IsNull {
                            reg,
                            target_pc: skip_label,
                        });
                        super::expr::emit_type_expr(
                            program,
                            decode_expr,
                            reg,
                            reg,
                            col,
                            &type_def,
                            &t_ctx.resolver,
                        )?;
                        program.preassign_label_to_next_insn(skip_label);
                    }
                }
            }
        }

        // Decode array blobs to JSON text for display, after extracting from sorter
        super::result_row::emit_array_decode_for_results(
            program,
            result_columns,
            &plan.table_references,
            start_reg,
            &t_ctx.resolver,
        )?;

        emit_result_row_and_limit(
            program,
            plan,
            start_reg,
            t_ctx.limit_ctx,
            if !use_heap_sort {
                Some(sort_loop_end_label)
            } else {
                None
            },
        )?;

        program.preassign_label_to_next_insn(sort_loop_next_label);
        if !use_heap_sort {
            program.emit_insn(Insn::SorterNext {
                cursor_id: sort_cursor,
                pc_if_next: sort_loop_start_label,
            });
        } else {
            program.emit_insn(Insn::Next {
                cursor_id: sort_cursor,
                pc_if_next: sort_loop_start_label,
                fullscan: false,
                is_index: false,
            });
        }
        program.preassign_label_to_next_insn(sort_loop_end_label);

        Ok(())
    }

    /// Emits the bytecode for inserting a row into an ORDER BY sorter.
    pub fn sorter_insert(
        program: &mut ProgramBuilder,
        t_ctx: &TranslateCtx,
        plan: &SelectPlan,
    ) -> Result<()> {
        let resolver = &t_ctx.resolver;
        let sort_metadata = t_ctx.meta_sort.as_ref().expect("sort metadata must exist");
        let order_by = &plan.order_by;
        let order_by_len = order_by.len();
        let result_columns = &plan.result_columns;
        let result_columns_to_skip_len = sort_metadata
            .remappings
            .iter()
            .filter(|r| r.deduplicated)
            .count();

        // The ORDER BY sorter has the sort keys first, then the result columns.
        let orderby_sorter_column_count =
            order_by_len + if sort_metadata.has_sequence { 1 } else { 0 } + result_columns.len()
                - result_columns_to_skip_len;

        let start_reg = program.alloc_registers(orderby_sorter_column_count);
        for (i, (expr, _, _)) in order_by.iter().enumerate() {
            let key_reg = start_reg + i;

            // Check if this ORDER BY expression matches a finalized aggregate
            if let Some(agg_idx) = plan
                .aggregates
                .iter()
                .position(|agg| exprs_are_equivalent(&agg.original_expr, expr))
            {
                // This ORDER BY expression is an aggregate, so copy from register
                let agg_start_reg = t_ctx
                    .reg_agg_start
                    .expect("aggregate registers must be initialized");
                let src_reg = agg_start_reg + agg_idx;
                program.emit_insn(Insn::Copy {
                    src_reg,
                    dst_reg: key_reg,
                    extra_amount: 0,
                });
            } else {
                // Sort keys must be encoded (on-disk) values. Suppress decode so the
                // sorter compares encoded representations, using either the base type's
                // built-in comparison (naked OPERATOR '<') or a custom comparator function.
                let is_custom =
                    result_column_custom_type_info(expr, &plan.table_references, resolver.schema())
                        .is_some_and(|(_, td)| td.decode().is_some());
                if is_custom {
                    program.flags.set_suppress_custom_type_decode(true);
                }
                let result = translate_expr(
                    program,
                    Some(&plan.table_references),
                    expr,
                    key_reg,
                    resolver,
                );
                if is_custom {
                    program.flags.set_suppress_custom_type_decode(false);
                }
                result?;
            }
        }

        let SortMetadata {
            sort_cursor,
            reg_sorter_data,
            use_heap_sort,
            ..
        } = sort_metadata;

        let skip_label = if *use_heap_sort {
            // skip records which greater than current top-k maintained in a separate BTreeIndex
            let insert_label = program.allocate_label();
            let skip_label = program.allocate_label();
            let limit = t_ctx.limit_ctx.as_ref().expect("limit must be set");
            let limit_reg = t_ctx.reg_limit_offset_sum.unwrap_or(limit.reg_limit);
            program.emit_insn(Insn::IfPos {
                reg: limit_reg,
                target_pc: insert_label,
                decrement_by: 1,
            });
            program.emit_insn(Insn::Last {
                cursor_id: *sort_cursor,
                pc_if_empty: insert_label,
            });
            program.emit_insn(Insn::IdxLE {
                cursor_id: *sort_cursor,
                start_reg,
                num_regs: orderby_sorter_column_count,
                target_pc: skip_label,
            });
            program.emit_insn(Insn::Delete {
                cursor_id: *sort_cursor,
                table_name: "".to_string(),
                is_part_of_update: false,
            });
            program.preassign_label_to_next_insn(insert_label);
            Some(skip_label)
        } else {
            None
        };

        let mut cur_reg = start_reg + order_by_len;
        if sort_metadata.has_sequence {
            program.emit_insn(Insn::Sequence {
                cursor_id: sort_metadata.sort_cursor,
                target_reg: cur_reg,
            });
            cur_reg += 1;
        }

        for (i, rc) in result_columns.iter().enumerate() {
            // If the result column is an exact duplicate of a sort key, we skip it.
            if sort_metadata
                .remappings
                .get(i)
                .expect("remapping must exist for all result columns")
                .deduplicated
            {
                continue;
            }
            translate_expr(
                program,
                Some(&plan.table_references),
                &rc.expr,
                cur_reg,
                resolver,
            )?;
            cur_reg += 1;
        }

        // Handle SELECT DISTINCT deduplication
        if let Distinctness::Distinct { ctx } = &plan.distinctness {
            let distinct_ctx = ctx.as_ref().expect("distinct context must exist");

            // For distinctness checking with Insn::Found, we need a contiguous run of registers containing all the result columns.
            // The emitted columns are in the ORDER BY sorter order, which may be different from the SELECT order, and obviously the
            // ORDER BY clause may not have all the result columns.
            // Hence, we need to allocate new registers and Copy from the existing ones to make a contiguous run of registers.
            let mut needs_reordering = false;

            // Check if result columns in sorter are in SELECT order
            let mut prev = None;
            for (select_idx, _rc) in result_columns.iter().enumerate() {
                let sorter_idx = sort_metadata
                    .remappings
                    .get(select_idx)
                    .expect("remapping must exist for all result columns")
                    .orderby_sorter_idx;

                if prev.is_some_and(|p| sorter_idx != p + 1) {
                    needs_reordering = true;
                    break;
                }
                prev = Some(sorter_idx);
            }

            if needs_reordering {
                // Allocate registers for reordered result columns.
                // TODO: it may be possible to optimize this to minimize the number of Insn::Copy we do, but for now
                // we will just allocate a new reg for every result column.
                let reordered_start_reg = program.alloc_registers(result_columns.len());

                for (select_idx, _rc) in result_columns.iter().enumerate() {
                    let remapping = sort_metadata
                        .remappings
                        .get(select_idx)
                        .expect("remapping must exist for all result columns");

                    let src_reg = start_reg + remapping.orderby_sorter_idx;
                    let dst_reg = reordered_start_reg + select_idx;

                    program.emit_insn(Insn::Copy {
                        src_reg,
                        dst_reg,
                        extra_amount: 0,
                    });
                }

                distinct_ctx.emit_deduplication_insns(
                    program,
                    result_columns.len(),
                    reordered_start_reg,
                );
            } else {
                // Result columns are already in SELECT order, use them directly
                let start_reg = sort_metadata
                    .remappings
                    .first()
                    .map(|r| start_reg + r.orderby_sorter_idx)
                    .expect("remapping must exist for all result columns");
                distinct_ctx.emit_deduplication_insns(program, result_columns.len(), start_reg);
            }
        }

        if *use_heap_sort {
            program.emit_insn(Insn::MakeRecord {
                start_reg: to_u32(start_reg),
                count: to_u32(orderby_sorter_column_count),
                dest_reg: to_u32(*reg_sorter_data),
                index_name: None,
                affinity_str: None,
            });
            program.emit_insn(Insn::IdxInsert {
                cursor_id: *sort_cursor,
                record_reg: *reg_sorter_data,
                unpacked_start: None,
                unpacked_count: None,
                flags: IdxInsertFlags::new(),
            });
            program.preassign_label_to_next_insn(skip_label.unwrap());
        } else {
            sorter_insert(
                program,
                start_reg,
                orderby_sorter_column_count,
                *sort_cursor,
                *reg_sorter_data,
            );
        }
        Ok(())
    }
}

/// Emits the bytecode for inserting a row into a sorter.
/// This can be either a GROUP BY sorter or an ORDER BY sorter.
pub fn sorter_insert(
    program: &mut ProgramBuilder,
    start_reg: usize,
    column_count: usize,
    cursor_id: usize,
    record_reg: usize,
) {
    program.emit_insn(Insn::MakeRecord {
        start_reg: to_u32(start_reg),
        count: to_u32(column_count),
        dest_reg: to_u32(record_reg),
        index_name: None,
        affinity_str: None,
    });
    program.emit_insn(Insn::SorterInsert {
        cursor_id,
        record_reg,
    });
}

#[derive(Debug)]
/// A mapping between a result column and its index in the ORDER BY sorter.
/// ORDER BY columns are emitted first, then the result columns.
/// If a result column is an exact duplicate of a sort key, we skip it.
/// If we skip a result column, we need to keep track which ORDER BY column it matches.
pub struct OrderByRemapping {
    pub orderby_sorter_idx: usize,
    pub deduplicated: bool,
}

/// In case any of the ORDER BY sort keys are exactly equal to a result column, we can skip emitting that result column.
/// If we skip a result column, we need to keep track what index in the ORDER BY sorter the result columns have,
/// because the result columns should be emitted in the SELECT clause order, not the ORDER BY clause order.
pub fn order_by_deduplicate_result_columns(
    order_by: &[(
        Box<ast::Expr>,
        SortOrder,
        Option<turso_parser::ast::NullsOrder>,
    )],
    result_columns: &[ResultSetColumn],
    has_sequence: bool,
) -> Result<Vec<OrderByRemapping>> {
    let mut result_column_remapping: Vec<OrderByRemapping> =
        Vec::try_with_capacity_ext(result_columns.len())?;
    let order_by_len = order_by.len();
    // `sequence_offset` shifts the base index where non-deduped SELECT columns begin,
    // because Sequence sits after ORDER BY keys but before result columns.
    let sequence_offset = if has_sequence { 1 } else { 0 };

    let mut i = 0;
    for rc in result_columns.iter() {
        let found = order_by
            .iter()
            .enumerate()
            .find(|(_, (expr, _, _))| exprs_are_equivalent(expr, &rc.expr));
        if let Some((j, _)) = found {
            result_column_remapping
                .push_within_capacity(OrderByRemapping {
                    orderby_sorter_idx: j,
                    deduplicated: true,
                })
                .expect("ORDER BY remapping vector was preallocated to result_columns.len()");
        } else {
            // This result column is not a duplicate of any ORDER BY key, so its sorter
            // index comes after all ORDER BY entries (hence the +order_by_len). The
            // counter `i` tracks how many such non-duplicate result columns we've seen.
            result_column_remapping
                .push_within_capacity(OrderByRemapping {
                    orderby_sorter_idx: order_by_len + sequence_offset + i,
                    deduplicated: false,
                })
                .expect("ORDER BY remapping vector was preallocated to result_columns.len()");
            i += 1;
        }
    }

    Ok(result_column_remapping)
}
