use std::rc::Rc;

use limbo_core::Connection;
use rand::Rng;

use crate::{
    compare_equal_rows,
    generation::ArbitraryFrom,
    get_all_rows,
    model::{
        query::{Insert, Predicate, Query, Select},
        table::Value,
    },
    SimulatorEnv,
};

pub fn property_insert_select(env: &mut SimulatorEnv, conn: &mut Rc<Connection>) {
    // Get a random table
    let table = env.rng.gen_range(0..env.tables.len());

    // Pick a random column
    let column_index = env.rng.gen_range(0..env.tables[table].columns.len());
    let column = &env.tables[table].columns[column_index].clone();

    let mut rng = env.rng.clone();

    // Generate a random value of the column type
    let value = Value::arbitrary_from(&mut rng, &column.column_type);

    // Create a whole new row
    let mut inserted_rows = Vec::new();
    for _ in 0..env.rng.gen_range(1..10) {
        let mut row = Vec::new();
        for (i, column) in env.tables[table].columns.iter().enumerate() {
            if i == column_index {
                row.push(value.clone());
            } else {
                let value = Value::arbitrary_from(&mut rng, &column.column_type);
                row.push(value);
            }
        }
        inserted_rows.push(row);
    }

    // Insert the rows
    let query = Query::Insert(Insert {
        table: env.tables[table].name.clone(),
        values: inserted_rows.clone(),
    });
    let _ = get_all_rows(env, conn, query.to_string().as_str()).unwrap();
    // Shadow operation on the table
    env.tables[table].rows.extend(inserted_rows.clone());

    // Create a query that selects the row
    let query = Query::Select(Select {
        table: env.tables[table].name.clone(),
        predicate: Predicate::Eq(column.name.clone(), value),
    });

    // Get all rows
    let selected_rows = get_all_rows(env, conn, query.to_string().as_str()).unwrap();

    // Check that the row is there
    for row in inserted_rows {
        assert!(selected_rows.iter().any(|r| r == &row));
    }
}

pub fn property_select_all(env: &mut SimulatorEnv, conn: &mut Rc<Connection>) {
    // Get a random table
    let table = env.rng.gen_range(0..env.tables.len());

    // Create a query that selects all rows
    let query = Query::Select(Select {
        table: env.tables[table].name.clone(),
        predicate: Predicate::And(Vec::new()),
    });

    // Get all rows
    let rows = get_all_rows(env, conn, query.to_string().as_str()).unwrap();

    // Make sure the rows are the same
    compare_equal_rows(&rows, &env.tables[table].rows);
}
