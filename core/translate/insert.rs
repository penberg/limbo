use std::rc::Weak;
use std::{cell::RefCell, ops::Deref, rc::Rc};

use sqlite3_parser::ast::{
    DistinctNames, Expr, InsertBody, QualifiedName, ResolveType, ResultColumn, With,
};

use crate::error::SQLITE_CONSTRAINT_PRIMARYKEY;
use crate::util::normalize_ident;
use crate::{
    schema::{Schema, Table},
    storage::sqlite3_ondisk::DatabaseHeader,
    translate::expr::translate_expr,
    vdbe::{builder::ProgramBuilder, Insn, Program},
};
use crate::{Connection, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Helper enum to indicate how a column is being inserted.
/// For example:
/// CREATE TABLE t (a, b, c, d);
/// INSERT INTO t (c, b) VALUES (1, 2);
///
/// resolve_columns_for_insert() returns [
///   ColumnToInsert::AutomaticNull,
///   ColumnToInsert::UserProvided { index_in_value_tuple: 1 },
///   ColumnToInsert::UserProvided { index_in_value_tuple: 0 },
///   ColumnToInsert::AutomaticNull,
/// ]
enum ColumnToInsert {
    /// The column is provided by the user.
    UserProvided { index_in_value_tuple: usize },
    /// The column is automatically set to NULL since it was not provided by the user.
    AutomaticNull,
}

/// Resolves how each column in a table should be populated during an INSERT.
/// For each column, determines whether it will:
/// 1. Use a user-provided value from the VALUES clause, or
/// 2. Be automatically set to NULL
///
/// Two cases are handled:
/// 1. No column list specified in INSERT statement:
///    - Values are assigned to columns in table definition order
///    - If fewer values than columns, remaining columns are NULL
/// 2. Column list specified in INSERT statement:
///    - For specified columns, a ColumnToInsert::UserProvided entry is created.
///    - Any columns not listed are set to NULL, i.e. a ColumnToInsert::AutomaticNull entry is created.
///
/// Returns a Vec<ColumnToInsert> with an entry for each column in the table,
/// indicating how that column should be populated.
fn resolve_columns_for_insert(
    table: Rc<Table>,
    columns: &Option<DistinctNames>,
    values: &[Vec<Expr>],
) -> Result<Vec<ColumnToInsert>> {
    assert!(table.has_rowid());
    if values.is_empty() {
        crate::bail_parse_error!("no values to insert");
    }

    let num_cols_in_table = table.columns().len();

    if columns.is_none() {
        let num_cols = values[0].len();
        // ensure value tuples dont have more columns than the table
        if num_cols > num_cols_in_table {
            crate::bail_parse_error!(
                "table {} has {} columns but {} values were supplied",
                table.get_name(),
                num_cols_in_table,
                num_cols
            );
        }
        // ensure each value tuple has the same number of columns
        for value in values.iter().skip(1) {
            if value.len() != num_cols {
                crate::bail_parse_error!("all VALUES must have the same number of terms");
            }
        }
        let columns: Vec<ColumnToInsert> = (0..num_cols_in_table)
            .map(|i| {
                if i < num_cols {
                    ColumnToInsert::UserProvided {
                        index_in_value_tuple: i,
                    }
                } else {
                    ColumnToInsert::AutomaticNull
                }
            })
            .collect();
        return Ok(columns);
    }

    // resolve the given columns to actual table column names and ensure they exist
    let columns = columns.as_ref().unwrap();
    let mut resolved_columns: Vec<ColumnToInsert> = (0..num_cols_in_table)
        .map(|i| ColumnToInsert::AutomaticNull)
        .collect();
    for (index_in_value_tuple, column) in columns.iter().enumerate() {
        let column_name = normalize_ident(column.0.as_str());
        let column_idx = table
            .columns()
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(&column_name));
        if let Some(i) = column_idx {
            resolved_columns[i] = ColumnToInsert::UserProvided {
                index_in_value_tuple,
            };
        } else {
            crate::bail_parse_error!(
                "table {} has no column named {}",
                table.get_name(),
                column_name
            );
        }
    }

    Ok(resolved_columns)
}

#[allow(clippy::too_many_arguments)]
pub fn translate_insert(
    schema: &Schema,
    with: &Option<With>,
    or_conflict: &Option<ResolveType>,
    tbl_name: &QualifiedName,
    columns: &Option<DistinctNames>,
    body: &InsertBody,
    _returning: &Option<Vec<ResultColumn>>,
    database_header: Rc<RefCell<DatabaseHeader>>,
    connection: Weak<Connection>,
) -> Result<Program> {
    assert!(with.is_none());
    assert!(or_conflict.is_none());
    let mut program = ProgramBuilder::new();
    let init_label = program.allocate_label();
    program.emit_insn_with_label_dependency(
        Insn::Init {
            target_pc: init_label,
        },
        init_label,
    );
    let start_offset = program.offset();

    // open table
    let table_name = &tbl_name.name;

    let table = match schema.get_table(table_name.0.as_str()) {
        Some(table) => table,
        None => crate::bail_corrupt_error!("Parse error: no such table: {}", table_name),
    };
    let table = Rc::new(Table::BTree(table));
    if !table.has_rowid() {
        crate::bail_parse_error!("INSERT into WITHOUT ROWID table is not supported");
    }

    let cursor_id = program.alloc_cursor_id(
        Some(table_name.0.clone()),
        Some(table.clone().deref().clone()),
    );
    let root_page = match table.as_ref() {
        Table::BTree(btree) => btree.root_page,
        Table::Index(index) => index.root_page,
        Table::Pseudo(_) => todo!(),
    };
    let values = match body {
        InsertBody::Select(select, None) => match &select.body.select {
            sqlite3_parser::ast::OneSelect::Values(values) => values,
            _ => todo!(),
        },
        _ => todo!(),
    };

    let columns = resolve_columns_for_insert(table.clone(), columns, values)?;
    // Check if rowid was provided (through INTEGER PRIMARY KEY as a rowid alias)
    let rowid_alias_index = table.columns().iter().position(|c| c.is_rowid_alias);
    let has_user_provided_rowid = {
        assert!(columns.len() == table.columns().len());
        if let Some(index) = rowid_alias_index {
            matches!(columns[index], ColumnToInsert::UserProvided { .. })
        } else {
            false
        }
    };

    // allocate a register for each column in the table. if not provided by user, they will simply be set as null.
    // allocate an extra register for rowid regardless of whether user provided a rowid alias column.
    let num_cols = table.columns().len();
    let rowid_reg = program.alloc_registers(num_cols + 1);
    let column_registers_start = rowid_reg + 1;
    let rowid_alias_reg = {
        if has_user_provided_rowid {
            Some(column_registers_start + rowid_alias_index.unwrap())
        } else {
            None
        }
    };

    let yield_reg = program.alloc_register();
    let jump_on_definition_label = program.allocate_label();
    {
        // Coroutine for values
        // TODO/efficiency: only use coroutine when there are multiple values to insert
        program.emit_insn_with_label_dependency(
            Insn::InitCoroutine {
                yield_reg,
                jump_on_definition: jump_on_definition_label,
                start_offset: program.offset() + 1,
            },
            jump_on_definition_label,
        );

        for value in values {
            // Process each value according to resolved columns
            for (i, column) in columns.iter().enumerate() {
                match column {
                    ColumnToInsert::UserProvided {
                        index_in_value_tuple,
                    } => {
                        translate_expr(
                            &mut program,
                            None,
                            value.get(*index_in_value_tuple).expect(
                                format!(
                                    "values tuple has no value for column {}",
                                    table.column_index_to_name(i).unwrap()
                                )
                                .as_str(),
                            ),
                            column_registers_start + i,
                            None,
                        )?;
                    }
                    ColumnToInsert::AutomaticNull => {
                        program.emit_insn(Insn::Null {
                            dest: column_registers_start + i,
                            dest_end: None,
                        });
                        program.mark_last_insn_constant();
                    }
                }
            }
            program.emit_insn(Insn::Yield {
                yield_reg,
                end_offset: 0,
            });
        }
        program.emit_insn(Insn::EndCoroutine { yield_reg });
    }

    program.resolve_label(jump_on_definition_label, program.offset());
    program.emit_insn(Insn::OpenWriteAsync {
        cursor_id,
        root_page,
    });
    program.emit_insn(Insn::OpenWriteAwait {});

    // Main loop
    // FIXME: rollback is not implemented. E.g. if you insert 2 rows and one fails to unique constraint violation,
    // the other row will still be inserted.
    let record_register = program.alloc_register();
    let halt_label = program.allocate_label();
    let loop_start_offset = program.offset();
    program.emit_insn_with_label_dependency(
        Insn::Yield {
            yield_reg,
            end_offset: halt_label,
        },
        halt_label,
    );

    let check_rowid_is_integer_label = rowid_alias_reg.and(Some(program.allocate_label()));
    if let Some(reg) = rowid_alias_reg {
        program.emit_insn(Insn::Copy {
            src_reg: reg,
            dst_reg: rowid_reg,
            amount: 0, // TODO: rename 'amount' to something else; amount==0 means 1
        });
        // for the row record, the rowid alias column is always set to NULL
        program.emit_insn(Insn::SoftNull { reg });
        // the user provided rowid value might itself be NULL. If it is, we create a new rowid on the next instruction.
        program.emit_insn_with_label_dependency(
            Insn::NotNull {
                reg: rowid_reg,
                target_pc: check_rowid_is_integer_label.unwrap(),
            },
            check_rowid_is_integer_label.unwrap(),
        );
    }

    // Create new rowid if a) not provided by user or b) provided by user but is NULL
    program.emit_insn(Insn::NewRowid {
        cursor: cursor_id,
        rowid_reg: rowid_reg,
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
        program.emit_insn_with_label_dependency(
            Insn::NotExists {
                cursor: cursor_id,
                rowid_reg: rowid_reg,
                target_pc: make_record_label,
            },
            make_record_label,
        );
        let rowid_column_name = if let Some(index) = rowid_alias_index {
            table.column_index_to_name(index).unwrap()
        } else {
            "rowid"
        };

        program.emit_insn(Insn::Halt {
            err_code: SQLITE_CONSTRAINT_PRIMARYKEY,
            description: format!("{}.{}", table.get_name(), rowid_column_name),
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

    program.emit_insn(Insn::Goto {
        target_pc: loop_start_offset,
    });

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
    program.resolve_deferred_labels();
    Ok(program.build(database_header, connection))
}
