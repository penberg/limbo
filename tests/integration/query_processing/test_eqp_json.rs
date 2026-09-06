use crate::common::{limbo_exec_rows, TempDatabase};
use rusqlite::types::Value;
use std::sync::Arc;
use turso_core::Connection;

const SCHEMA: [&str; 2] = [
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
    "CREATE INDEX idx_users_age ON users(age)",
];

fn connect_with_schema(tmp_db: &TempDatabase) -> Arc<Connection> {
    let conn = tmp_db.connect_limbo();
    for ddl in SCHEMA {
        limbo_exec_rows(&conn, ddl);
    }
    conn
}

#[turso_macros::test]
fn format_json_statement_reports_one_text_column(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    let stmt = conn.prepare("EXPLAIN QUERY PLAN FORMAT=JSON SELECT 1")?;
    assert_eq!(stmt.num_columns(), 1);
    assert_eq!(stmt.get_column_name(0), "plan_json");
    assert_eq!(stmt.get_column_decltype(0).as_deref(), Some("TEXT"));
    Ok(())
}

#[turso_macros::test]
fn join_access_nodes_report_cost_and_row_estimates(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = connect_with_schema(&tmp_db);
    limbo_exec_rows(
        &conn,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total INTEGER)",
    );
    limbo_exec_rows(&conn, "CREATE INDEX idx_orders_user ON orders(user_id)");

    let plan = explain_query_plan(
        &conn,
        "SELECT users.name, orders.total
         FROM users JOIN orders ON orders.user_id = users.id
         WHERE users.id = 1",
    )?;
    let access_nodes: Vec<_> = plan["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|node| matches!(node["op"]["type"].as_str(), Some("scan" | "search")))
        .collect();

    assert_eq!(access_nodes.len(), 2);
    assert_eq!(access_nodes[0]["op"]["estimate"]["input_rows"], 1.0);
    for node in access_nodes {
        let estimate = &node["op"]["estimate"];
        assert!(estimate["rows_per_input"].as_f64().unwrap() > 0.0);
        assert!(estimate["output_rows"].as_f64().unwrap() > 0.0);
        assert!(estimate["access_cost"].as_f64().unwrap() > 0.0);
        assert!(estimate["total_cost"].as_f64().unwrap() > 0.0);
    }
    Ok(())
}

#[turso_macros::test]
fn outer_join_null_test_estimates_unmatched_rows(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    create_parent_child_data(&conn, "CREATE INDEX child_parent ON child(parent_id)");

    let query = "SELECT parent.id
                 FROM parent
                 LEFT JOIN child ON child.parent_id = parent.id AND child.kind = 1
                 WHERE child.id IS NULL";
    assert_eq!(limbo_exec_rows(&conn, query).len(), 90);

    let plan = explain_query_plan(&conn, query)?;
    let estimated_rows = final_output_rows(&plan);
    assert!(
        (80.0..99.0).contains(&estimated_rows),
        "expected 80 to 99 rows, got {estimated_rows}"
    );
    Ok(())
}

#[turso_macros::test]
fn nullable_primary_key_null_test_keeps_matched_rows(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    limbo_exec_rows(&conn, "CREATE TABLE parent (id INTEGER PRIMARY KEY)");
    limbo_exec_rows(
        &conn,
        "CREATE TABLE child (
            key TEXT PRIMARY KEY,
            nullable_value TEXT,
            parent_id INTEGER
        )",
    );
    limbo_exec_rows(&conn, "CREATE INDEX child_parent ON child(parent_id)");
    limbo_exec_rows(&conn, "INSERT INTO parent VALUES (1), (2)");
    limbo_exec_rows(
        &conn,
        "INSERT INTO child VALUES (NULL, NULL, 1), ('two', 'two', 2)",
    );
    limbo_exec_rows(&conn, "ANALYZE");

    let primary_key_plan = explain_query_plan(
        &conn,
        "SELECT parent.id
         FROM parent LEFT JOIN child ON child.parent_id = parent.id
         WHERE child.key IS NULL",
    )?;
    let nullable_column_plan = explain_query_plan(
        &conn,
        "SELECT parent.id
         FROM parent LEFT JOIN child ON child.parent_id = parent.id
         WHERE child.nullable_value IS NULL",
    )?;

    assert_eq!(
        final_output_rows(&primary_key_plan),
        final_output_rows(&nullable_column_plan)
    );
    Ok(())
}

#[turso_macros::test]
fn compound_join_lookup_estimates_its_constant_filter(tmp_db: TempDatabase) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    create_parent_child_data(
        &conn,
        "CREATE INDEX child_parent_kind ON child(parent_id, kind)",
    );

    let query = "SELECT parent.id
                 FROM parent CROSS JOIN child
                 WHERE child.parent_id = parent.id AND child.kind = 1";
    assert_eq!(limbo_exec_rows(&conn, query).len(), 10);

    let plan = explain_query_plan(&conn, query)?;
    let child_estimate = plan["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["op"]["table"] == "child")
        .map(|node| &node["op"]["estimate"])
        .unwrap();
    assert_eq!(child_estimate["input_rows"], 100.0);
    let output_rows = child_estimate["output_rows"].as_f64().unwrap();
    assert!(
        (5.0..=20.0).contains(&output_rows),
        "expected 5 to 20 rows, got {output_rows}"
    );
    Ok(())
}

#[turso_macros::test]
fn unavailable_duplicate_does_not_stop_a_compound_lookup(
    tmp_db: TempDatabase,
) -> anyhow::Result<()> {
    let conn = tmp_db.connect_limbo();
    limbo_exec_rows(&conn, "CREATE TABLE first (a INTEGER, b INTEGER)");
    limbo_exec_rows(&conn, "CREATE TABLE middle (x INTEGER, y INTEGER)");
    limbo_exec_rows(&conn, "CREATE INDEX middle_xy ON middle(x, y)");
    limbo_exec_rows(&conn, "CREATE TABLE last (z INTEGER)");

    let plan = explain_query_plan(
        &conn,
        "SELECT *
         FROM first CROSS JOIN middle CROSS JOIN last
         WHERE middle.x = first.a
           AND middle.x = last.z
           AND middle.y = first.b",
    )?;
    let middle_constraints = plan["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|node| node["op"]["table"] == "middle")
        .map(|node| node["op"]["constraints"].as_array().unwrap())
        .unwrap();
    assert_eq!(middle_constraints.len(), 2);
    assert_eq!(middle_constraints[0], "x=?");
    assert_eq!(middle_constraints[1], "y=?");
    Ok(())
}

fn explain_query_plan(conn: &Arc<Connection>, query: &str) -> anyhow::Result<serde_json::Value> {
    let rows = limbo_exec_rows(conn, &format!("EXPLAIN QUERY PLAN FORMAT=JSON {query}"));
    let Value::Text(plan) = &rows[0][0] else {
        panic!("query plan must be text");
    };
    Ok(serde_json::from_str(plan.as_str())?)
}

fn final_output_rows(plan: &serde_json::Value) -> f64 {
    plan["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|node| node["op"]["estimate"]["output_rows"].as_f64())
        .next_back()
        .unwrap()
}

fn create_parent_child_data(conn: &Arc<Connection>, index_sql: &str) {
    limbo_exec_rows(conn, "CREATE TABLE parent (id INTEGER PRIMARY KEY)");
    limbo_exec_rows(
        conn,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, kind INTEGER)",
    );
    limbo_exec_rows(conn, index_sql);

    let parent_values = (1..=100)
        .map(|value| format!("({value})"))
        .collect::<Vec<_>>()
        .join(",");
    let child_values = (1..=100)
        .map(|value| {
            let kind = if value <= 10 { 1 } else { 2 };
            format!("({value},{value},{kind})")
        })
        .collect::<Vec<_>>()
        .join(",");
    limbo_exec_rows(conn, &format!("INSERT INTO parent VALUES {parent_values}"));
    limbo_exec_rows(conn, &format!("INSERT INTO child VALUES {child_values}"));
    limbo_exec_rows(conn, "ANALYZE");
}
