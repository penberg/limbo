use std::ops::Deref;

use sqlite3_parser::ast::{
    DistinctNames, Expr, InsertBody, QualifiedName, ResolveType, ResultColumn, With,
};

use crate::error::SQLITE_CONSTRAINT_PRIMARYKEY;
use crate::schema::BTreeTable;
use crate::util::normalize_ident;
use crate::vdbe::BranchOffset;
use crate::Result;
use crate::{
    schema::{Column, Schema},
    translate::expr::translate_expr,
    vdbe::{
        builder::{CursorType, ProgramBuilder},
        insn::Insn,
    },
    SymbolTable,
};

use super::emitter::Resolver;

#[allow(clippy::too_many_arguments)]
pub fn translate_insert(
    program: &mut ProgramBuilder,
    schema: &Schema,
    with: &Option<With>,
    on_conflict: &Option<ResolveType>,
    tbl_name: &QualifiedName,
    columns: &Option<DistinctNames>,
    body: &InsertBody,
    _returning: &Option<Vec<ResultColumn>>,
    syms: &SymbolTable,
) -> Result<()> {
    if with.is_some() {
        crate::bail_parse_error!("WITH clause is not supported");
    }
    if on_conflict.is_some() {
        crate::bail_parse_error!("ON CONFLICT clause is not supported");
    }
    let resolver = Resolver::new(syms);
    let init_label = program.allocate_label();
    program.emit_insn(Insn::Init {
        target_pc: init_label,
    });
    let start_offset = program.offset();

    // open table
    let table_name = &tbl_name.name;

    let table = match schema.get_table(table_name.0.as_str()) {
        Some(table) => table,
        None => crate::bail_corrupt_error!("Parse error: no such table: {}", table_name),
    };
    if !table.has_rowid {
        crate::bail_parse_error!("INSERT into WITHOUT ROWID table is not supported");
    }

    let cursor_id = program.alloc_cursor_id(
        Some(table_name.0.clone()),
        CursorType::BTreeTable(table.clone()),
    );
    let root_page = table.root_page;
    let values = match body {
        InsertBody::Select(select, None) => match &select.body.select.deref() {
            sqlite3_parser::ast::OneSelect::Values(values) => values,
            _ => todo!(),
        },
        _ => todo!(),
    };

    let column_mappings = resolve_columns_for_insert(&table, columns, values)?;
    // Check if rowid was provided (through INTEGER PRIMARY KEY as a rowid alias)
    let rowid_alias_index = table.columns.iter().position(|c| c.is_rowid_alias);
    let has_user_provided_rowid = {
        assert!(column_mappings.len() == table.columns.len());
        if let Some(index) = rowid_alias_index {
            column_mappings[index].value_index.is_some()
        } else {
            false
        }
    };

    // allocate a register for each column in the table. if not provided by user, they will simply be set as null.
    // allocate an extra register for rowid regardless of whether user provided a rowid alias column.
    let num_cols = table.columns.len();
    let rowid_reg = program.alloc_registers(num_cols + 1);
    let column_registers_start = rowid_reg + 1;
    let rowid_alias_reg = {
        if has_user_provided_rowid {
            Some(column_registers_start + rowid_alias_index.unwrap())
        } else {
            None
        }
    };

    let record_register = program.alloc_register();
    let halt_label = program.allocate_label();
    let mut loop_start_offset = BranchOffset::Offset(0);

    let inserting_multiple_rows = values.len() > 1;

    // Multiple rows - use coroutine for value population
    if inserting_multiple_rows {
        let yield_reg = program.alloc_register();
        let jump_on_definition_label = program.allocate_label();
        program.emit_insn(Insn::InitCoroutine {
            yield_reg,
            jump_on_definition: jump_on_definition_label,
            start_offset: program.offset().add(1u32),
        });

        for value in values {
            populate_column_registers(
                program,
                value,
                &column_mappings,
                column_registers_start,
                true,
                rowid_reg,
                &resolver,
            )?;
            program.emit_insn(Insn::Yield {
                yield_reg,
                end_offset: halt_label,
            });
        }
        program.emit_insn(Insn::EndCoroutine { yield_reg });
        program.resolve_label(jump_on_definition_label, program.offset());

        program.emit_insn(Insn::OpenWriteAsync {
            cursor_id,
            root_page,
        });
        program.emit_insn(Insn::OpenWriteAwait {});

        // Main loop
        // FIXME: rollback is not implemented. E.g. if you insert 2 rows and one fails to unique constraint violation,
        // the other row will still be inserted.
        loop_start_offset = program.offset();
        program.emit_insn(Insn::Yield {
            yield_reg,
            end_offset: halt_label,
        });
    } else {
        // Single row - populate registers directly
        program.emit_insn(Insn::OpenWriteAsync {
            cursor_id,
            root_page,
        });
        program.emit_insn(Insn::OpenWriteAwait {});

        populate_column_registers(
            program,
            &values[0],
            &column_mappings,
            column_registers_start,
            false,
            rowid_reg,
            &resolver,
        )?;
    }

    // Common record insertion logic for both single and multiple rows
    let check_rowid_is_integer_label = rowid_alias_reg.and(Some(program.allocate_label()));
    if let Some(reg) = rowid_alias_reg {
        // for the row record, the rowid alias column (INTEGER PRIMARY KEY) is always set to NULL
        // and its value is copied to the rowid register. in the case where a single row is inserted,
        // the value is written directly to the rowid register (see populate_column_registers()).
        // again, not sure why this only happens in the single row case, but let's mimic sqlite.
        // in the single row case we save a Copy instruction, but in the multiple rows case we do
        // it here in the loop.
        if inserting_multiple_rows {
            program.emit_insn(Insn::Copy {
                src_reg: reg,
                dst_reg: rowid_reg,
                amount: 0, // TODO: rename 'amount' to something else; amount==0 means 1
            });
            // for the row record, the rowid alias column is always set to NULL
            program.emit_insn(Insn::SoftNull { reg });
        }
        // the user provided rowid value might itself be NULL. If it is, we create a new rowid on the next instruction.
        program.emit_insn(Insn::NotNull {
            reg: rowid_reg,
            target_pc: check_rowid_is_integer_label.unwrap(),
        });
    }

    // Create new rowid if a) not provided by user or b) provided by user but is NULL
    program.emit_insn(Insn::NewRowid {
        cursor: cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });

    if let Some(must_be_int_label) = check_rowid_is_integer_label {
        program.resolve_label(must_be_int_label, program.offset());
        // If the user provided a rowid, it must be an integer.
        program.emit_insn(Insn::MustBeInt { reg: rowid_reg });
    }

    // Check uniqueness constraint for rowid if it was provided by user.
    // When the DB allocates it there are no need for separate uniqueness checks.
    if has_user_provided_rowid {
        let make_record_label = program.allocate_label();
        program.emit_insn(Insn::NotExists {
            cursor: cursor_id,
            rowid_reg,
            target_pc: make_record_label,
        });
        let rowid_column_name = if let Some(index) = rowid_alias_index {
            &table.columns.get(index).unwrap().name
        } else {
            "rowid"
        };

        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
            description: format!("{}.{}", table_name.0, rowid_column_name),
        });

        program.resolve_label(make_record_label, program.offset());
    }

    // Create and insert the record
    program.emit_insn(Insn::MakeRecord {
        start_reg: column_registers_start,
        count: num_cols,
        dest_reg: record_register,
    });

    program.emit_insn(Insn::InsertAsync {
        cursor: cursor_id,
        key_reg: rowid_reg,
        record_reg: record_register,
        flag: 0,
    });
    program.emit_insn(Insn::InsertAwait { cursor_id });

    if inserting_multiple_rows {
        // For multiple rows, loop back
        program.emit_insn(Insn::Goto {
            target_pc: loop_start_offset,
        });
    }

    program.resolve_label(halt_label, program.offset());
    program.emit_insn(Insn::Halt {
        err_code: 0,
        description: String::new(),
    });

    program.resolve_label(init_label, program.offset());
    program.emit_insn(Insn::Transaction { write: true });
    program.emit_constant_insns();
    program.emit_insn(Insn::Goto {
        target_pc: start_offset,
    });

    Ok(())
}

#[derive(Debug)]
/// Represents how a column should be populated during an INSERT.
/// Contains both the column definition and optionally the index into the VALUES tuple.
struct ColumnMapping<'a> {
    /// Reference to the column definition from the table schema
    column: &'a Column,
    /// If Some(i), use the i-th value from the VALUES tuple
    /// If None, use NULL (column was not specified in INSERT statement)
    value_index: Option<usize>,
}

/// Resolves how each column in a table should be populated during an INSERT.
/// Returns a Vec of ColumnMapping, one for each column in the table's schema.
///
/// For each column, specifies:
/// 1. The column definition (type, constraints, etc)
/// 2. Where to get the value from:
///    - Some(i) -> use i-th value from the VALUES tuple
///    - None -> use NULL (column wasn't specified in INSERT)
///
/// Two cases are handled:
/// 1. No column list specified (INSERT INTO t VALUES ...):
///    - Values are assigned to columns in table definition order
///    - If fewer values than columns, remaining columns map to None
/// 2. Column list specified (INSERT INTO t (col1, col3) VALUES ...):
///    - Named columns map to their corresponding value index
///    - Unspecified columns map to None
fn resolve_columns_for_insert<'a>(
    table: &'a BTreeTable,
    columns: &Option<DistinctNames>,
    values: &[Vec<Expr>],
) -> Result<Vec<ColumnMapping<'a>>> {
    if values.is_empty() {
        crate::bail_parse_error!("no values to insert");
    }

    let table_columns = &table.columns;

    // Case 1: No columns specified - map values to columns in order
    if columns.is_none() {
        let num_values = values[0].len();
        if num_values > table_columns.len() {
            crate::bail_parse_error!(
                "table {} has {} columns but {} values were supplied",
                &table.name,
                table_columns.len(),
                num_values
            );
        }

        // Verify all value tuples have same length
        for value in values.iter().skip(1) {
            if value.len() != num_values {
                crate::bail_parse_error!("all VALUES must have the same number of terms");
            }
        }

        // Map each column to either its corresponding value index or None
        return Ok(table_columns
            .iter()
            .enumerate()
            .map(|(i, col)| ColumnMapping {
                column: col,
                value_index: if i < num_values { Some(i) } else { None },
            })
            .collect());
    }

    // Case 2: Columns specified - map named columns to their values
    let mut mappings: Vec<_> = table_columns
        .iter()
        .map(|col| ColumnMapping {
            column: col,
            value_index: None,
        })
        .collect();

    // Map each named column to its value index
    for (value_index, column_name) in columns.as_ref().unwrap().iter().enumerate() {
        let column_name = normalize_ident(column_name.0.as_str());
        let table_index = table_columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&column_name));

        if table_index.is_none() {
            crate::bail_parse_error!("table {} has no column named {}", &table.name, column_name);
        }

        mappings[table_index.unwrap()].value_index = Some(value_index);
    }

    Ok(mappings)
}

/// Populates the column registers with values for a single row
fn populate_column_registers(
    program: &mut ProgramBuilder,
    value: &[Expr],
    column_mappings: &[ColumnMapping],
    column_registers_start: usize,
    inserting_multiple_rows: bool,
    rowid_reg: usize,
    resolver: &Resolver,
) -> Result<()> {
    for (i, mapping) in column_mappings.iter().enumerate() {
        let target_reg = column_registers_start + i;

        // Column has a value in the VALUES tuple
        if let Some(value_index) = mapping.value_index {
            // When inserting a single row, SQLite writes the value provided for the rowid alias column (INTEGER PRIMARY KEY)
            // directly into the rowid register and writes a NULL into the rowid alias column. Not sure why this only happens
            // in the single row case, but let's copy it.
            let write_directly_to_rowid_reg =
                mapping.column.is_rowid_alias && !inserting_multiple_rows;
            let reg = if write_directly_to_rowid_reg {
                rowid_reg
            } else {
                target_reg
            };
            translate_expr(
                program,
                None,
                value.get(value_index).expect("value index out of bounds"),
                reg,
                resolver,
            )?;
            if write_directly_to_rowid_reg {
                program.emit_insn(Insn::SoftNull { reg: target_reg });
            }
        } else {
            // Column was not specified - use NULL if it is nullable, otherwise error
            // Rowid alias columns can be NULL because we will autogenerate a rowid in that case.
            let is_nullable = !mapping.column.primary_key || mapping.column.is_rowid_alias;
            if is_nullable {
                program.emit_insn(Insn::Null {
                    dest: target_reg,
                    dest_end: None,
                });
                program.mark_last_insn_constant();
            } else {
                crate::bail_parse_error!("column {} is not nullable", mapping.column.name);
            }
        }
    }
    Ok(())
}
