pub mod grammar_generator;

#[cfg(test)]
mod tests {
    use std::{rc::Rc, sync::Arc};

    use limbo_core::Database;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;
    use rusqlite::params;

    use crate::grammar_generator::{rand_int, GrammarGenerator};

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
    pub fn arithmetic_expression_fuzz_ex1() {
        let io = Arc::new(limbo_core::PlatformIO::new().unwrap());
        let limbo_db = Database::open_file(io, ":memory:").unwrap();
        let limbo_conn = limbo_db.connect();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        for query in [
            "SELECT ~1 >> 1536",
            "SELECT ~ + 3 << - ~ (~ (8)) - + -1 - 3 >> 3 + -6 * (-7 * 9 >> - 2)",
        ] {
            let limbo = limbo_exec_row(&limbo_conn, query);
            let sqlite = sqlite_exec_row(&sqlite_conn, query);
            assert_eq!(
                limbo, sqlite,
                "query: {}, limbo: {:?}, sqlite: {:?}",
                query, limbo, sqlite
            );
        }
    }

    #[test]
    pub fn arithmetic_expression_fuzz() {
        let g = GrammarGenerator::new();
        let (expr, expr_builder) = g.create_handle();
        let (bin_op, bin_op_builder) = g.create_handle();
        let (unary_op, unary_op_builder) = g.create_handle();
        let (paren, paren_builder) = g.create_handle();

        paren_builder
            .concat("")
            .push_str("(")
            .push(expr)
            .push_str(")")
            .build();

        unary_op_builder
            .concat(" ")
            .push(g.create().choice().options_str(["~", "+", "-"]).build())
            .push(expr)
            .build();

        bin_op_builder
            .concat(" ")
            .push(expr)
            .push(
                g.create()
                    .choice()
                    .options_str(["+", "-", "*", "/", "%", "&", "|", "<<", ">>"])
                    .build(),
            )
            .push(expr)
            .build();

        expr_builder
            .choice()
            .option_w(unary_op, 1.0)
            .option_w(bin_op, 1.0)
            .option_w(paren, 1.0)
            .option_symbol_w(rand_int(-10..10), 1.0)
            .build();

        let sql = g.create().concat(" ").push_str("SELECT").push(expr).build();

        let io = Arc::new(limbo_core::PlatformIO::new().unwrap());
        let limbo_db = Database::open_file(io, ":memory:").unwrap();
        let limbo_conn = limbo_db.connect();
        let sqlite_conn = rusqlite::Connection::open_in_memory().unwrap();

        let mut rng = ChaCha8Rng::seed_from_u64(0);
        for _ in 0..16 * 1024 {
            let query = g.generate(&mut rng, sql, 50);
            let limbo = limbo_exec_row(&limbo_conn, &query);
            let sqlite = sqlite_exec_row(&sqlite_conn, &query);
            assert_eq!(
                limbo, sqlite,
                "query: {}, limbo: {:?}, sqlite: {:?}",
                query, limbo, sqlite
            );
        }
    }
}
