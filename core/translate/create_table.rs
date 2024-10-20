use std::{cell::RefCell, ops::Deref, rc::Rc};

use crate::schema::{create_table, Table};
use crate::storage::pager::Pager;
use crate::storage::sqlite3_ondisk::DatabaseHeader;
use crate::vdbe::builder::ProgramBuilder;
use crate::vdbe::Insn;
use crate::{schema::Schema, vdbe::Program, Result};

use sqlite3_parser::ast::{CreateTableBody, QualifiedName};

pub fn translate_create_table(
    schema: Rc<RefCell<Schema>>,
    _temporary: bool,
    _if_not_exists: bool,
    tbl_name: &QualifiedName,
    body: &CreateTableBody,
    database_header: Rc<RefCell<DatabaseHeader>>,
    pager: Rc<Pager>,
) -> Result<Program> {
    let mut program = ProgramBuilder::new();
    let init_label = program.allocate_label();
    program.emit_insn_with_label_dependency(
        Insn::Init {
            target_pc: init_label,
        },
        init_label,
    );

    let start_offset = program.offset();
    let new_page = pager.allocate_page()?;
    let new_page_id = new_page.borrow().id;

    new_page.borrow_mut().set_dirty();
    pager.add_dirty(new_page_id);

    let new_btree_table = create_table(tbl_name.clone(), body.clone(), new_page_id)?;
    let sql_text = new_btree_table.to_sql();

    let mut schema_mut = schema.borrow_mut();
    schema_mut.add_table(Rc::new(new_btree_table));

    // Retrieve the sqlite_schema table
    let schema_table = schema_mut.get_table("sqlite_schema").unwrap();

    let schema_table = Rc::new(Table::BTree(schema_table));
    let cursor_id = program.alloc_cursor_id(
        Some("sqlite_schema".to_string()),
        Some(schema_table.clone().deref().clone()),
    );

    let root_page = match schema_table.as_ref() {
        Table::BTree(btree) => btree.root_page,
        Table::Pseudo(_) => todo!(),
        Table::Index(_rc) => todo!(),
    };

    program.emit_insn(Insn::OpenWriteAsync {
        cursor_id,
        root_page,
    });
    program.emit_insn(Insn::OpenWriteAwait {});

    let reg_type = program.alloc_register();
    let reg_name = program.alloc_register();
    let reg_tbl_name = program.alloc_register();
    let reg_rootpage = program.alloc_register();
    let reg_sql = program.alloc_register();

    let table_name_str = tbl_name.name.clone();

    program.emit_insn(Insn::String8 {
        dest: reg_type,
        value: "table".to_string(),
    });
    program.emit_insn(Insn::String8 {
        dest: reg_name,
        value: table_name_str.clone().to_string(),
    });
    program.emit_insn(Insn::String8 {
        dest: reg_tbl_name,
        value: table_name_str.clone().to_string(),
    });
    program.emit_insn(Insn::Integer {
        dest: reg_rootpage,
        value: new_page_id as i64,
    });

    program.emit_insn(Insn::String8 {
        dest: reg_sql,
        value: sql_text,
    });

    let reg_rowid = program.alloc_register();

    program.emit_insn(Insn::NewRowid {
        cursor: cursor_id,
        rowid_reg: reg_rowid,
        prev_largest_reg: 0,
    });

    let record_register = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: reg_type,
        count: 5,
        dest_reg: record_register,
    });

    program.emit_insn(Insn::InsertAsync {
        cursor: cursor_id,
        key_reg: reg_rowid,
        record_reg: record_register,
        flag: 0,
    });
    program.emit_insn(Insn::InsertAwait { cursor_id });

    program.emit_insn(Insn::Halt {
        err_code: 0,
        description: String::new(),
    });
    program.resolve_label(init_label, program.offset());
    program.emit_insn(Insn::Transaction);
    program.emit_constant_insns();
    program.emit_insn(Insn::Goto {
        target_pc: start_offset,
    });
    program.resolve_deferred_labels();

    Ok(program.build(database_header))
}

// // CREATE TABLE T1(id INTEGER PRIMARY KEY, name TEXT);

// insert into sqlite_schema VALUES ('table', 'T1', 'T1', 274, 'CREATE TABLE T1 (id INTEGER PRIMARY KEY, name TEXT)');
