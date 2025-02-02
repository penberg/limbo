#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Arc};

    use limbo_core::Database;
    use rusqlite::params;

    fn sqlite_exec_row(conn: &rusqlite::Connection, query: &str) -> Vec<rusqlite::types::Value> {
        let mut stmt = conn.prepare(&query).unwrap();
        let mut rows = stmt.query(params![]).unwrap();
        let mut columns = Vec::new();
        let row = rows.next().unwrap().unwrap();
        for i in 0.. {
            let column: rusqlite::types::Value = match row.get(i) {
                Ok(column) => column,
                Err(rusqlite::Error::InvalidColumnIndex(_)) => break,
                Err(err) => panic!("unexpected rusqlite error: {}", err),
            };
            columns.push(column);
        }
        assert!(rows.next().unwrap().is_none());

        columns
    }

    fn limbo_exec_row(
        conn: &Rc<limbo_core::Connection>,
        query: &str,
    ) -> Vec<rusqlite::types::Value> {
        let mut stmt = conn.prepare(query).unwrap();
        let result = stmt.step().unwrap();
        let row = loop {
            match result {
                limbo_core::StepResult::Row(row) => break row,
                limbo_core::StepResult::IO => continue,
                r => panic!("unexpected result {:?}: expecting single row", r),
            }
        };
        row.values
            .iter()
            .map(|x| match x {
                limbo_core::Value::Null => rusqlite::types::Value::Null,
                limbo_core::Value::Integer(x) => rusqlite::types::Value::Integer(*x),
                limbo_core::Value::Float(x) => rusqlite::types::Value::Real(*x),
                limbo_core::Value::Text(x) => rusqlite::types::Value::Text((*x).clone()),
                limbo_core::Value::Blob(x) => rusqlite::types::Value::Blob((*x).clone()),
            })
            .collect()
    }

    #[test]
    pub fn kek() {
        let io = Arc::new(limbo_core::PlatformIO::new().unwrap());
        let limbo_db = Database::open_file(io, ":memory:").unwrap();
        let limbo_conn = limbo_db.connect();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        println!(
            "column: {:?}",
            sqlite_exec_row(&sqlite_conn, "SELECT 1 = 1.0")
        );
        println!(
            "column: {:?}",
            limbo_exec_row(&limbo_conn, "SELECT 1 = 1.0")
        );
    }
}
