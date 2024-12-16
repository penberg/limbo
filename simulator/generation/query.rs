use crate::generation::table::{GTValue, LTValue};
use crate::generation::{Arbitrary, ArbitraryFrom};

use crate::model::query::{Create, Delete, Insert, Predicate, Query, Select};
use crate::model::table::{Table, Value};
use rand::Rng;

impl Arbitrary for Create {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        Create {
            table: Table::arbitrary(rng),
        }
    }
}

impl ArbitraryFrom<Vec<Table>> for Select {
    fn arbitrary_from<R: Rng>(rng: &mut R, tables: &Vec<Table>) -> Self {
        let table = rng.gen_range(0..tables.len());
        Select {
            table: tables[table].name.clone(),
            predicate: Predicate::arbitrary_from(rng, &tables[table]),
        }
    }
}

impl ArbitraryFrom<Vec<&Table>> for Select {
    fn arbitrary_from<R: Rng>(rng: &mut R, tables: &Vec<&Table>) -> Self {
        let table = rng.gen_range(0..tables.len());
        Select {
            table: tables[table].name.clone(),
            predicate: Predicate::arbitrary_from(rng, tables[table]),
        }
    }
}

impl ArbitraryFrom<Table> for Insert {
    fn arbitrary_from<R: Rng>(rng: &mut R, table: &Table) -> Self {
        let values = table
            .columns
            .iter()
            .map(|c| Value::arbitrary_from(rng, &c.column_type))
            .collect();
        Insert {
            table: table.name.clone(),
            values,
        }
    }
}

impl ArbitraryFrom<Table> for Delete {
    fn arbitrary_from<R: Rng>(rng: &mut R, table: &Table) -> Self {
        Delete {
            table: table.name.clone(),
            predicate: Predicate::arbitrary_from(rng, table),
        }
    }
}

impl ArbitraryFrom<Table> for Query {
    fn arbitrary_from<R: Rng>(rng: &mut R, table: &Table) -> Self {
        match rng.gen_range(0..=200) {
            0 => Query::Create(Create::arbitrary(rng)),
            1..=100 => Query::Select(Select::arbitrary_from(rng, &vec![table])),
            101..=200 => Query::Insert(Insert::arbitrary_from(rng, table)),
            // todo: This branch is currently never taken, as DELETE is not yet implemented.
            //       Change this when DELETE is implemented.
            201..=300 => Query::Delete(Delete::arbitrary_from(rng, table)),
            _ => unreachable!(),
        }
    }
}

struct CompoundPredicate(Predicate);
struct SimplePredicate(Predicate);

impl ArbitraryFrom<(&Table, bool)> for SimplePredicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (table, predicate_value): &(&Table, bool)) -> Self {
        // Pick a random column
        let column_index = rng.gen_range(0..table.columns.len());
        let column = &table.columns[column_index];
        let column_values = table
            .rows
            .iter()
            .map(|r| &r[column_index])
            .collect::<Vec<_>>();
        // Pick an operator
        let operator = match rng.gen_range(0..3) {
            0 => {
                if *predicate_value {
                    Predicate::Eq(
                        column.name.clone(),
                        Value::arbitrary_from(rng, &column_values),
                    )
                } else {
                    Predicate::Eq(
                        column.name.clone(),
                        Value::arbitrary_from(rng, &column.column_type),
                    )
                }
            }
            1 => Predicate::Gt(
                column.name.clone(),
                match predicate_value {
                    true => GTValue::arbitrary_from(rng, &column_values).0,
                    false => LTValue::arbitrary_from(rng, &column_values).0,
                },
            ),
            2 => Predicate::Lt(
                column.name.clone(),
                match predicate_value {
                    true => LTValue::arbitrary_from(rng, &column_values).0,
                    false => GTValue::arbitrary_from(rng, &column_values).0,
                },
            ),
            _ => unreachable!(),
        };

        SimplePredicate(operator)
    }
}

impl ArbitraryFrom<(&Table, bool)> for CompoundPredicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (table, predicate_value): &(&Table, bool)) -> Self {
        // Decide if you want to create an AND or an OR
        CompoundPredicate(if rng.gen_bool(0.7) {
            // An AND for true requires each of its children to be true
            // An AND for false requires at least one of its children to be false
            if *predicate_value {
                Predicate::And(
                    (0..rng.gen_range(1..=3))
                        .map(|_| SimplePredicate::arbitrary_from(rng, &(*table, true)).0)
                        .collect(),
                )
            } else {
                // Create a vector of random booleans
                let mut booleans = (0..rng.gen_range(1..=3))
                    .map(|_| rng.gen_bool(0.5))
                    .collect::<Vec<_>>();

                let len = booleans.len();

                // Make sure at least one of them is false
                if booleans.iter().all(|b| *b) {
                    booleans[rng.gen_range(0..len)] = false;
                }

                Predicate::And(
                    booleans
                        .iter()
                        .map(|b| SimplePredicate::arbitrary_from(rng, &(*table, *b)).0)
                        .collect(),
                )
            }
        } else {
            // An OR for true requires at least one of its children to be true
            // An OR for false requires each of its children to be false
            if *predicate_value {
                // Create a vector of random booleans
                let mut booleans = (0..rng.gen_range(1..=3))
                    .map(|_| rng.gen_bool(0.5))
                    .collect::<Vec<_>>();
                let len = booleans.len();
                // Make sure at least one of them is true
                if booleans.iter().all(|b| !*b) {
                    booleans[rng.gen_range(0..len)] = true;
                }

                Predicate::Or(
                    booleans
                        .iter()
                        .map(|b| SimplePredicate::arbitrary_from(rng, &(*table, *b)).0)
                        .collect(),
                )
            } else {
                Predicate::Or(
                    (0..rng.gen_range(1..=3))
                        .map(|_| SimplePredicate::arbitrary_from(rng, &(*table, false)).0)
                        .collect(),
                )
            }
        })
    }
}

impl ArbitraryFrom<Table> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, table: &Table) -> Self {
        let predicate_value = rng.gen_bool(0.5);
        CompoundPredicate::arbitrary_from(rng, &(table, predicate_value)).0
    }
}

impl ArbitraryFrom<(&str, &Value)> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (column_name, value): &(&str, &Value)) -> Self {
        match rng.gen_range(0..3) {
            0 => Predicate::Eq(column_name.to_string(), (*value).clone()),
            1 => Predicate::Gt(
                column_name.to_string(),
                LTValue::arbitrary_from(rng, *value).0,
            ),
            2 => Predicate::Lt(
                column_name.to_string(),
                LTValue::arbitrary_from(rng, *value).0,
            ),
            _ => unreachable!(),
        }
    }
}
