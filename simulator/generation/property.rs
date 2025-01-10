use crate::{
    model::{
        query::{Create, Insert, Predicate, Query, Select},
        table::Value,
    },
    runner::env::SimulatorEnv,
};

use super::{
    frequency, pick, pick_index,
    plan::{Assertion, Interaction, InteractionStats, ResultSet},
    ArbitraryFrom,
};

/// Properties are representations of executable specifications
/// about the database behavior.
#[derive(Clone)]
pub(crate) enum Property {
    /// Insert-Select is a property in which the inserted row
    /// must be in the resulting rows of a select query that has a
    /// where clause that matches the inserted row.
    /// The execution of the property is as follows
    ///     INSERT INTO <t> VALUES (...)
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     SELECT * FROM <t> WHERE <predicate>
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - The inserted row will not be deleted.
    /// - The inserted row will not be updated.
    /// - The table `t` will not be renamed, dropped, or altered.
    InsertSelect {
        /// The insert query
        insert: Insert,
        /// Additional interactions in the middle of the property
        interactions: Vec<Query>,
        /// The select query
        select: Select,
    },
    /// Double Create Failure is a property in which creating
    /// the same table twice leads to an error.
    /// The execution of the property is as follows
    ///     CREATE TABLE <t> (...)
    ///     I_0
    ///     I_1
    ///     ...
    ///     I_n
    ///     CREATE TABLE <t> (...) -> Error
    /// The interactions in the middle has the following constraints;
    /// - There will be no errors in the middle interactions.
    /// - Table `t` will not be renamed or dropped.
    DoubleCreateFailure {
        /// The create query
        create: Create,
        /// Additional interactions in the middle of the property
        interactions: Vec<Query>,
    },
}

impl Property {
    pub(crate) fn name(&self) -> String {
        match self {
            Property::InsertSelect { .. } => "Insert-Select".to_string(),
            Property::DoubleCreateFailure { .. } => "Double-Create-Failure".to_string(),
        }
    }
    pub(crate) fn interactions(&self) -> Vec<Interaction> {
        match self {
            Property::InsertSelect {
                insert,
                interactions: _, // todo: add extensional interactions
                select,
            } => {
                // Check that the row is there
                let row = insert
                    .values
                    .first() // `.first` is safe, because we know we are inserting a row in the insert select property
                    .expect("insert query should have at least 1 value")
                    .clone();

                let assumption = Interaction::Assumption(Assertion {
                    message: format!("table {} exists", insert.table),
                    func: Box::new({
                        let table_name = insert.table.clone();
                        move |_: &Vec<ResultSet>, env: &SimulatorEnv| {
                            env.tables.iter().any(|t| t.name == table_name)
                        }
                    }),
                });

                let assertion = Interaction::Assertion(Assertion {
                    message: format!(
                        // todo: add the part inserting ({} = {})",
                        "row [{:?}] not found in table {}",
                        row.iter().map(|v| v.to_string()).collect::<Vec<String>>(),
                        insert.table,
                    ),
                    func: Box::new(move |stack: &Vec<ResultSet>, _: &SimulatorEnv| {
                        let rows = stack.last().unwrap();
                        match rows {
                            Ok(rows) => rows.iter().any(|r| r == &row),
                            Err(_) => false,
                        }
                    }),
                });

                vec![
                    assumption,
                    Interaction::Query(Query::Insert(insert.clone())),
                    Interaction::Query(Query::Select(select.clone())),
                    assertion,
                ]
            }
            Property::DoubleCreateFailure {
                create,
                interactions: _, // todo: add extensional interactions
            } => {
                let table_name = create.table.name.clone();
                let cq1 = Interaction::Query(Query::Create(create.clone()));
                let cq2 = Interaction::Query(Query::Create(create.clone()));

                let assertion = Interaction::Assertion(Assertion {
                    message:
                        "creating two tables with the name should result in a failure for the second query"
                            .to_string(),
                    func: Box::new(move |stack: &Vec<ResultSet>, _: &SimulatorEnv| {
                        let last = stack.last().unwrap();
                        match last {
                            Ok(_) => false,
                            Err(e) => e
                                .to_string()
                                .contains(&format!("Table {table_name} already exists")),
                        }
                    }),
                });

                vec![cq1, cq2, assertion]
            }
        }
    }
}

fn remaining(env: &SimulatorEnv, stats: &InteractionStats) -> (f64, f64, f64) {
    let remaining_read = ((env.opts.max_interactions as f64 * env.opts.read_percent / 100.0)
        - (stats.read_count as f64))
        .max(0.0);
    let remaining_write = ((env.opts.max_interactions as f64 * env.opts.write_percent / 100.0)
        - (stats.write_count as f64))
        .max(0.0);
    let remaining_create = ((env.opts.max_interactions as f64 * env.opts.create_percent / 100.0)
        - (stats.create_count as f64))
        .max(0.0);

    (remaining_read, remaining_write, remaining_create)
}

fn property_insert_select<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Property {
    // Get a random table
    let table = pick(&env.tables, rng);
    // Pick a random column
    let column_index = pick_index(table.columns.len(), rng);
    let column = &table.columns[column_index].clone();
    // Generate a random value of the column type
    let value = Value::arbitrary_from(rng, &column.column_type);
    // Create a whole new row
    let mut row = Vec::new();
    for (i, column) in table.columns.iter().enumerate() {
        if i == column_index {
            row.push(value.clone());
        } else {
            let value = Value::arbitrary_from(rng, &column.column_type);
            row.push(value);
        }
    }

    // Insert the row
    let insert_query = Insert {
        table: table.name.clone(),
        values: vec![row.clone()],
    };

    // Select the row
    let select_query = Select {
        table: table.name.clone(),
        predicate: Predicate::arbitrary_from(
            rng,
            &(table, &Predicate::Eq(column.name.clone(), value.clone())),
        ),
    };

    Property::InsertSelect {
        insert: insert_query,
        interactions: Vec::new(),
        select: select_query,
    }
}

fn property_double_create_failure<R: rand::Rng>(rng: &mut R, env: &SimulatorEnv) -> Property {
    // Get a random table
    let table = pick(&env.tables, rng);
    // Create the table
    let create_query = Create {
        table: table.clone(),
    };

    Property::DoubleCreateFailure {
        create: create_query,
        interactions: Vec::new(),
    }
}

impl ArbitraryFrom<(&SimulatorEnv, &InteractionStats)> for Property {
    fn arbitrary_from<R: rand::Rng>(
        rng: &mut R,
        (env, stats): &(&SimulatorEnv, &InteractionStats),
    ) -> Self {
        let (remaining_read, remaining_write, remaining_create) = remaining(env, stats);
        frequency(
            vec![
                (
                    f64::min(remaining_read, remaining_write),
                    Box::new(|rng: &mut R| property_insert_select(rng, env)),
                ),
                (
                    remaining_create / 2.0,
                    Box::new(|rng: &mut R| property_double_create_failure(rng, env)),
                ),
            ],
            rng,
        )
    }
}
