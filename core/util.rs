use std::{rc::Rc, sync::Arc};

use crate::{
    schema::{self, Schema},
    Result, RowResult, Rows, IO,
};

pub fn normalize_ident(ident: &str) -> String {
    (if ident.starts_with('"') && ident.ends_with('"') {
        &ident[1..ident.len() - 1]
    } else {
        ident
    })
    .to_lowercase()
}

pub fn parse_schema_rows(rows: Option<Rows>, schema: &mut Schema, io: Arc<dyn IO>) -> Result<()> {
    if let Some(mut rows) = rows {
        loop {
            match rows.next_row()? {
                RowResult::Row(row) => {
                    let ty = row.get::<&str>(0)?;
                    if ty != "table" && ty != "index" {
                        continue;
                    }
                    match ty {
                        "table" => {
                            let root_page: i64 = row.get::<i64>(3)?;
                            let sql: &str = row.get::<&str>(4)?;
                            let table = schema::BTreeTable::from_sql(sql, root_page as usize)?;
                            schema.add_table(Rc::new(table));
                        }
                        "index" => {
                            let root_page: i64 = row.get::<i64>(3)?;
                            match row.get::<&str>(4) {
                                Ok(sql) => {
                                    let index = schema::Index::from_sql(sql, root_page as usize)?;
                                    schema.add_index(Rc::new(index));
                                }
                                _ => continue,
                                // TODO parse auto index structures
                            }
                        }
                        _ => continue,
                    }
                }
                RowResult::IO => {
                    // TODO: How do we ensure that the I/O we submitted to
                    // read the schema is actually complete?
                    io.run_once()?;
                }
                RowResult::Done => break,
            }
        }
    }
    Ok(())
}
