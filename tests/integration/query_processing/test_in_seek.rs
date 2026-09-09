use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;

fn value_as_text(value: &Value) -> Option<&str> {
    match value {
        Value::Text(v) => Some(v.as_str()),
        _ => None,
    }
}

#[test]
fn large_indexed_in_list_uses_seek_loop() {
    let tmp_db = TempDatabase::new_empty();
    let conn = tmp_db.connect_limbo();

    limbo_exec_rows(
        &conn,
        "CREATE TABLE t(id INTEGER PRIMARY KEY, x INTEGER, v TEXT)",
    );
    limbo_exec_rows(&conn, "CREATE INDEX t_x ON t(x)");
    limbo_exec_rows(
        &conn,
        "INSERT INTO t
        SELECT value, value, 'v' || value
        FROM generate_series(1, 10000)",
    );
    limbo_exec_rows(&conn, "ANALYZE");

    let values = (1..=5000)
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let query = format!("SELECT count(*) FROM t WHERE x IN ({values})");

    let eqp_rows = limbo_exec_rows(&conn, &format!("EXPLAIN QUERY PLAN {query}"));
    let plan = eqp_rows
        .iter()
        .filter_map(|row| row.get(3).and_then(value_as_text))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        plan.contains("SEARCH t USING COVERING INDEX t_x (x=?)"),
        "expected IN-list to drive indexed seeks, got:\n{plan}"
    );
    assert!(
        !plan.contains("SCAN t USING COVERING INDEX t_x"),
        "large IN-list regressed to residual scan:\n{plan}"
    );
}
