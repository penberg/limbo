//! The VDBE bytecode code generator.
//!
//! This module is responsible for translating the SQL AST into a sequence of
//! instructions for the VDBE. The VDBE is a register-based virtual machine that
//! executes bytecode instructions. This code generator is responsible for taking
//! the SQL AST and generating the corresponding VDBE instructions. For example,
//! a SELECT statement will be translated into a sequence of instructions that
//! will read rows from the database and filter them according to a WHERE clause.

pub(crate) mod emitter;
pub(crate) mod expr;
pub(crate) mod insert;
pub(crate) mod optimizer;
pub(crate) mod plan;
pub(crate) mod planner;
pub(crate) mod select;

use std::cell::RefCell;
use std::fmt::Display;
use std::rc::{Rc, Weak};

use crate::schema::Schema;
use crate::storage::pager::Pager;
use crate::storage::sqlite3_ondisk::{DatabaseHeader, MIN_PAGE_CACHE_SIZE};
use crate::vdbe::{builder::ProgramBuilder, Insn, Program};
use crate::{bail_parse_error, Connection, Result};
use insert::translate_insert;
use select::translate_select;
use sqlite3_parser::ast;
use sqlite3_parser::ast::fmt::ToTokens;

/// Translate SQL statement into bytecode program.
pub fn translate(
    schema: &Schema,
    stmt: ast::Stmt,
    database_header: Rc<RefCell<DatabaseHeader>>,
    pager: Rc<Pager>,
    connection: Weak<Connection>,
) -> Result<Program> {
    match stmt {
        ast::Stmt::AlterTable(_, _) => bail_parse_error!("ALTER TABLE not supported yet"),
        ast::Stmt::Analyze(_) => bail_parse_error!("ANALYZE not supported yet"),
        ast::Stmt::Attach { .. } => bail_parse_error!("ATTACH not supported yet"),
        ast::Stmt::Begin(_, _) => bail_parse_error!("BEGIN not supported yet"),
        ast::Stmt::Commit(_) => bail_parse_error!("COMMIT not supported yet"),
        ast::Stmt::CreateIndex { .. } => bail_parse_error!("CREATE INDEX not supported yet"),
        ast::Stmt::CreateTable {
            temporary,
            if_not_exists,
            tbl_name,
            body,
        } => {
            if temporary {
                bail_parse_error!("TEMPORARY table not supported yet");
            }
            translate_create_table(
                tbl_name,
                body,
                if_not_exists,
                database_header,
                connection,
                schema,
            )
        }
        ast::Stmt::CreateTrigger { .. } => bail_parse_error!("CREATE TRIGGER not supported yet"),
        ast::Stmt::CreateView { .. } => bail_parse_error!("CREATE VIEW not supported yet"),
        ast::Stmt::CreateVirtualTable { .. } => {
            bail_parse_error!("CREATE VIRTUAL TABLE not supported yet")
        }
        ast::Stmt::Delete { .. } => bail_parse_error!("DELETE not supported yet"),
        ast::Stmt::Detach(_) => bail_parse_error!("DETACH not supported yet"),
        ast::Stmt::DropIndex { .. } => bail_parse_error!("DROP INDEX not supported yet"),
        ast::Stmt::DropTable { .. } => bail_parse_error!("DROP TABLE not supported yet"),
        ast::Stmt::DropTrigger { .. } => bail_parse_error!("DROP TRIGGER not supported yet"),
        ast::Stmt::DropView { .. } => bail_parse_error!("DROP VIEW not supported yet"),
        ast::Stmt::Pragma(name, body) => {
            translate_pragma(&name, body, database_header, pager, connection)
        }
        ast::Stmt::Reindex { .. } => bail_parse_error!("REINDEX not supported yet"),
        ast::Stmt::Release(_) => bail_parse_error!("RELEASE not supported yet"),
        ast::Stmt::Rollback { .. } => bail_parse_error!("ROLLBACK not supported yet"),
        ast::Stmt::Savepoint(_) => bail_parse_error!("SAVEPOINT not supported yet"),
        ast::Stmt::Select(select) => translate_select(schema, select, database_header, connection),
        ast::Stmt::Update { .. } => bail_parse_error!("UPDATE not supported yet"),
        ast::Stmt::Vacuum(_, _) => bail_parse_error!("VACUUM not supported yet"),
        ast::Stmt::Insert {
            with,
            or_conflict,
            tbl_name,
            columns,
            body,
            returning,
        } => translate_insert(
            schema,
            &with,
            &or_conflict,
            &tbl_name,
            &columns,
            &body,
            &returning,
            database_header,
            connection,
        ),
    }
}

/* Example:

sqlite> EXPLAIN CREATE TABLE users (id INT, email TEXT);;
addr  opcode         p1    p2    p3    p4             p5  comment
----  -------------  ----  ----  ----  -------------  --  -------------
0     Init           0     30    0                    0   Start at 30
1     ReadCookie     0     3     2                    0
2     If             3     5     0                    0
3     SetCookie      0     2     4                    0
4     SetCookie      0     5     1                    0
5     CreateBtree    0     2     1                    0   r[2]=root iDb=0 flags=1
6     OpenWrite      0     1     0     5              0   root=1 iDb=0
7     NewRowid       0     1     0                    0   r[1]=rowid
8     Blob           6     3     0                   0   r[3]= (len=6)
9     Insert         0     3     1                    8   intkey=r[1] data=r[3]
10    Close          0     0     0                    0
11    Close          0     0     0                    0
12    Null           0     4     5                    0   r[4..5]=NULL
13    Noop           2     0     4                    0
14    OpenWrite      1     1     0     5              0   root=1 iDb=0; sqlite_master
15    SeekRowid      1     17    1                    0   intkey=r[1]
16    Rowid          1     5     0                    0   r[5]= rowid of 1
17    IsNull         5     26    0                    0   if r[5]==NULL goto 26
18    String8        0     6     0     table          0   r[6]='table'
19    String8        0     7     0     users          0   r[7]='users'
20    String8        0     8     0     users          0   r[8]='users'
21    Copy           2     9     0                    0   r[9]=r[2]
22    String8        0     10    0     CREATE TABLE users (id INT, email TEXT) 0   r[10]='CREATE TABLE users (id INT, email TEXT)'
23    MakeRecord     6     5     4     BBBDB          0   r[4]=mkrec(r[6..10])
24    Delete         1     68    5                    0
25    Insert         1     4     5                    0   intkey=r[5] data=r[4]
26    SetCookie      0     1     1                    0
27    ParseSchema    0     0     0     tbl_name='users' AND type!='trigger' 0
28    SqlExec        1     0     0     PRAGMA "main".integrity_check('users') 0
29    Halt           0     0     0                    0
30    Transaction    0     1     0     0              1   usesStmtJournal=1
31    Goto           0     1     0                    0

*/
fn translate_create_table(
    tbl_name: ast::QualifiedName,
    body: ast::CreateTableBody,
    if_not_exists: bool,
    database_header: Rc<RefCell<DatabaseHeader>>,
    connection: Weak<Connection>,
    schema: &Schema,
) -> Result<Program> {
    let mut program = ProgramBuilder::new();
    if schema.get_table(tbl_name.name.0.as_str()).is_some() {
        if if_not_exists {
            let init_label = program.allocate_label();
            program.emit_insn_with_label_dependency(
                Insn::Init {
                    target_pc: init_label,
                },
                init_label,
            );
            let start_offset = program.offset();
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
            return Ok(program.build(database_header, connection));
        }
        bail_parse_error!("Table {} already exists", tbl_name);
    }

    let sql = create_table_body_to_str(&tbl_name, &body);

    let parse_schema_label = program.allocate_label();
    let init_label = program.allocate_label();
    program.emit_insn_with_label_dependency(
        Insn::Init {
            target_pc: init_label,
        },
        init_label,
    );
    let start_offset = program.offset();
    // TODO: ReadCookie
    // TODO: If
    // TODO: SetCookie
    // TODO: SetCookie
    let root_reg = program.alloc_register();
    program.emit_insn(Insn::CreateBtree {
        db: 0,
        root: root_reg,
        flags: 1,
    });
    let table_id = "sqlite_schema".to_string();
    let table = schema.get_table(&table_id).unwrap();
    let table = crate::schema::Table::BTree(table.clone());
    let sqlite_schema_cursor_id =
        program.alloc_cursor_id(Some(table_id.to_owned()), Some(table.to_owned()));
    program.emit_insn(Insn::OpenWriteAsync {
        cursor_id: sqlite_schema_cursor_id,
        root_page: 1,
    });
    program.emit_insn(Insn::OpenWriteAwait {});
    let rowid_reg = program.alloc_register();
    program.emit_insn(Insn::NewRowid {
        cursor: sqlite_schema_cursor_id,
        rowid_reg,
        prev_largest_reg: 0,
    });
    let null_reg_1 = program.alloc_register();
    let null_reg_2 = program.alloc_register();
    program.emit_insn(Insn::Null {
        dest: null_reg_1,
        dest_end: Some(null_reg_2),
    });
    let type_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        value: "table".to_string(),
        dest: type_reg,
    });
    let name_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        value: tbl_name.name.0.to_string(),
        dest: name_reg,
    });
    let tbl_name_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        value: tbl_name.name.0.to_string(),
        dest: tbl_name_reg,
    });
    let rootpage_reg = program.alloc_register();
    program.emit_insn(Insn::Copy {
        src_reg: root_reg,
        dst_reg: rootpage_reg,
        amount: 1,
    });
    let sql_reg = program.alloc_register();
    program.emit_insn(Insn::String8 {
        value: sql.to_string(),
        dest: sql_reg,
    });
    let record_reg = program.alloc_register();
    program.emit_insn(Insn::MakeRecord {
        start_reg: type_reg,
        count: 5,
        dest_reg: record_reg,
    });
    // TODO: Delete
    program.emit_insn(Insn::InsertAsync {
        cursor: sqlite_schema_cursor_id,
        key_reg: rowid_reg,
        record_reg,
        flag: 0,
    });
    program.emit_insn(Insn::InsertAwait {
        cursor_id: sqlite_schema_cursor_id,
    });
    program.resolve_label(parse_schema_label, program.offset());
    // TODO: SetCookie
    //
    // TODO: remove format, it sucks for performance but is convinient
    let parse_schema_where_clause = format!("tbl_name = '{}' AND type != 'trigger'", tbl_name);
    program.emit_insn(Insn::ParseSchema {
        db: sqlite_schema_cursor_id,
        where_clause: parse_schema_where_clause,
    });

    // TODO: SqlExec
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
    Ok(program.build(database_header, connection))
}

fn translate_pragma(
    name: &ast::QualifiedName,
    body: Option<ast::PragmaBody>,
    database_header: Rc<RefCell<DatabaseHeader>>,
    pager: Rc<Pager>,
    connection: Weak<Connection>,
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
    let mut write = false;
    match body {
        None => {
            let pragma_result = program.alloc_register();

            program.emit_insn(Insn::Integer {
                value: database_header.borrow().default_cache_size.into(),
                dest: pragma_result,
            });

            let pragma_result_end = program.next_free_register();
            program.emit_insn(Insn::ResultRow {
                start_reg: pragma_result,
                count: pragma_result_end - pragma_result,
            });
        }
        Some(ast::PragmaBody::Equals(value)) => {
            let value_to_update = match value {
                ast::Expr::Literal(ast::Literal::Numeric(numeric_value)) => {
                    numeric_value.parse::<i64>().unwrap()
                }
                ast::Expr::Unary(ast::UnaryOperator::Negative, expr) => match *expr {
                    ast::Expr::Literal(ast::Literal::Numeric(numeric_value)) => {
                        -numeric_value.parse::<i64>().unwrap()
                    }
                    _ => 0,
                },
                _ => 0,
            };
            write = true;
            update_pragma(
                &name.name.0,
                value_to_update,
                database_header.clone(),
                pager,
            );
        }
        Some(ast::PragmaBody::Call(_)) => {
            todo!()
        }
    };
    program.emit_insn(Insn::Halt {
        err_code: 0,
        description: String::new(),
    });
    program.resolve_label(init_label, program.offset());
    program.emit_insn(Insn::Transaction { write });
    program.emit_constant_insns();
    program.emit_insn(Insn::Goto {
        target_pc: start_offset,
    });
    program.resolve_deferred_labels();
    Ok(program.build(database_header, connection))
}

fn update_pragma(name: &str, value: i64, header: Rc<RefCell<DatabaseHeader>>, pager: Rc<Pager>) {
    match name {
        "cache_size" => {
            let mut cache_size_unformatted = value;
            let mut cache_size = if cache_size_unformatted < 0 {
                let kb = cache_size_unformatted.abs() * 1024;
                kb / 512 // assume 512 page size for now
            } else {
                value
            } as usize;
            if cache_size < MIN_PAGE_CACHE_SIZE {
                // update both in memory and stored disk value
                cache_size = MIN_PAGE_CACHE_SIZE;
                cache_size_unformatted = MIN_PAGE_CACHE_SIZE as i64;
            }

            // update in-memory header
            header.borrow_mut().default_cache_size = cache_size_unformatted
                .try_into()
                .unwrap_or_else(|_| panic!("invalid value, too big for a i32 {}", value));

            // update in disk
            let header_copy = header.borrow().clone();
            pager.write_database_header(&header_copy);

            // update cache size
            pager.change_page_cache_size(cache_size);
        }
        _ => todo!(),
    }
}

struct TableFormatter<'a> {
    body: &'a ast::CreateTableBody,
}
impl<'a> Display for TableFormatter<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.body.to_fmt(f)
    }
}

fn create_table_body_to_str(tbl_name: &ast::QualifiedName, body: &ast::CreateTableBody) -> String {
    let mut sql = String::new();
    let formatter = TableFormatter { body };
    sql.push_str(format!("CREATE TABLE {} {}", tbl_name.name.0, formatter).as_str());
    match body {
        ast::CreateTableBody::ColumnsAndConstraints {
            columns: _,
            constraints: _,
            options: _,
        } => {}
        ast::CreateTableBody::AsSelect(_select) => todo!("as select not yet supported"),
    }
    sql
}
