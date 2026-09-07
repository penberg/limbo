#![no_main]
use core::fmt;
use std::{error::Error, sync::Arc};

use arbitrary::Arbitrary;
use libfuzzer_sys::{fuzz_target, Corpus};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Id(String);

impl<'a> Arbitrary<'a> for Id {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let len: usize = u.int_in_range(1..=10)?;
        let is_quoted = bool::arbitrary(u)?;

        let mut out = String::with_capacity(len + if is_quoted { 2 } else { 0 });

        if is_quoted {
            out.push('"');
        }

        for _ in 0..len {
            out.push(u.choose(b"abcdefghijklnmopqrstuvwxyz")?.clone() as char);
        }

        if is_quoted {
            out.push('"');
        }

        Ok(Id(out))
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Arbitrary, Clone)]
enum Type {
    None,
    Integer,
    Text,
    Real,
    Blob,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::None => Ok(()),
            Type::Integer => write!(f, "INTEGER"),
            Type::Text => write!(f, "TEXT"),
            Type::Real => write!(f, "REAL"),
            Type::Blob => write!(f, "BLOB"),
        }
    }
}

#[derive(Debug, Arbitrary, Clone)]
struct ColumnDef {
    name: Id,
    r#type: Type,
}

impl fmt::Display for ColumnDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ColumnDef { name, r#type } = self;
        write!(f, "{name} {type}",)?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Columns(Vec<ColumnDef>);

impl<'a> Arbitrary<'a> for Columns {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let len: usize = u.int_in_range(1..=4)?;

        let mut out: Vec<ColumnDef> = Vec::with_capacity(len);

        for i in 0..len {
            out.push(ColumnDef {
                name: Id(format!("c{i}")),
                r#type: u.arbitrary()?,
            });
        }

        Ok(Self(out))
    }
}

impl fmt::Display for Columns {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, column) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }

            write!(f, "{column}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct TableDef {
    name: Id,
    columns: Columns,
}

impl fmt::Display for TableDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let TableDef { name, columns } = self;

        write!(f, "CREATE TABLE {name} ( {columns} )")
    }
}

#[derive(Debug, Clone)]
struct IndexDef {
    name: Id,
    table: Id,
    columns: Vec<Id>,
}

impl fmt::Display for IndexDef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let IndexDef {
            name,
            table,
            columns,
        } = self;

        write!(f, "CREATE INDEX {name} ON {table}(")?;

        for (i, column) in columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }

            write!(f, "{column}")?;
        }

        write!(f, ")")?;

        Ok(())
    }
}

#[derive(Debug)]
enum Op {
    CreateTable(TableDef),
    CreateIndex(IndexDef),
    DropTable {
        table: Id,
    },
    DropColumn {
        table: Id,
        column: Id,
    },
    RenameTable {
        rename_from: Id,
        rename_to: Id,
    },
    RenameColumn {
        table: Id,
        rename_from: Id,
        rename_to: Id,
    },
}

impl fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::CreateTable(table_def) => write!(f, "{table_def}"),
            Op::CreateIndex(index_def) => write!(f, "{index_def}"),
            Op::DropColumn { table, column } => {
                write!(f, "ALTER TABLE {table} DROP COLUMN {column}")
            }
            Op::DropTable { table } => write!(f, "DROP TABLE {table}"),
            Op::RenameTable {
                rename_from,
                rename_to,
            } => write!(f, "ALTER TABLE {rename_from} RENAME TO {rename_to}"),
            Op::RenameColumn {
                table,
                rename_from,
                rename_to,
            } => {
                write!(
                    f,
                    "ALTER TABLE {table} RENAME COLUMN {rename_from} TO {rename_to}"
                )
            }
        }
    }
}

#[derive(Debug)]
struct Ops(Vec<Op>);

impl<'a> Arbitrary<'a> for Ops {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let mut ops = Vec::new();
        let mut tables = Vec::new();

        let mut drop_list = Vec::new();

        let mut table_index: usize = 0;

        let num_ops = u.int_in_range(1..=10)?;

        for _ in 0..num_ops {
            let op_type = if tables.is_empty() {
                0
            } else {
                u.int_in_range(0..=2)?
            };

            match op_type {
                0 => {
                    let table_def = TableDef {
                        name: {
                            let out = format!("t{table_index}");
                            table_index += 1;

                            Id(out)
                        },
                        columns: u.arbitrary()?,
                    };

                    ops.push(Op::CreateTable(table_def.clone()));

                    tables.push(table_def);
                }
                1 => {
                    let table = u.choose(&tables)?;
                    let index_def = IndexDef {
                        name: {
                            let out = format!("i{table_index}");
                            table_index += 1;

                            Id(out)
                        },
                        table: table.name.clone(),
                        columns: vec![u.choose(&table.columns.0)?.name.clone()],
                    };

                    ops.push(Op::CreateIndex(index_def.clone()));
                }
                2 => {
                    let index = u.choose_index(tables.len())?;

                    let table = &tables[index];

                    let rename_to = Id(format!("t{table_index}"));
                    table_index += 1;

                    ops.push(Op::RenameTable {
                        rename_from: table.name.clone(),
                        rename_to: rename_to.clone(),
                    });

                    tables.push(TableDef {
                        name: rename_to,
                        columns: table.columns.clone(),
                    });

                    tables.remove(index);
                }
                3 => {
                    let index = u.choose_index(tables.len())?;

                    let table = &tables[index];

                    if table.columns.0.len() == 1 {
                        let table = tables.remove(index);

                        ops.push(Op::DropTable {
                            table: table.name.clone(),
                        });

                        drop_list.push(table.name);
                    } else {
                        let table = &mut tables[index];

                        let index = u.choose_index(table.columns.0.len())?;

                        ops.push(Op::DropColumn {
                            table: table.name.clone(),
                            column: table.columns.0.remove(index).name,
                        });
                    }
                }
                4 => {
                    let index = u.choose_index(tables.len())?;

                    let table = &mut tables[index];

                    let index = u.choose_index(table.columns.0.len())?;

                    let rename_to = Id(format!("cr{table_index}"));
                    table_index += 1;

                    let column = table.columns.0[index].clone();

                    table.columns.0.insert(
                        index,
                        ColumnDef {
                            name: rename_to.clone(),
                            ..column
                        },
                    );

                    ops.push(Op::RenameColumn {
                        table: table.name.clone(),
                        rename_from: column.name,
                        rename_to,
                    });
                }
                _ => panic!(),
            }
        }

        Ok(Self(ops))
    }
}

fn do_fuzz(Ops(ops): Ops) -> Result<Corpus, Box<dyn Error>> {
    let rusqlite_conn = rusqlite::Connection::open_in_memory()?;

    let io = Arc::new(turso_core::MemoryIO::new());
    let db = turso_core::Database::open_file(
        io.clone(),
        ":memory:",
        Arc::new(turso_core::SqliteDialect),
    )?;
    let limbo_conn = db.connect()?;

    for op in ops {
        let sql = op.to_string();

        rusqlite_conn
            .execute(&sql, ())
            .inspect_err(|_| {
                dbg!(&sql);
            })
            .unwrap();

        limbo_conn
            .execute(&sql)
            .inspect_err(|_| {
                dbg!(&sql);
            })
            .unwrap()
    }

    Ok(Corpus::Keep)
}

fuzz_target!(|ops: Ops| -> Corpus { do_fuzz(ops).unwrap_or(Corpus::Keep) });
