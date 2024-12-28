use crate::generation::table::{GTValue, LTValue};
use crate::generation::{one_of, Arbitrary, ArbitraryFrom};

use crate::model::query::{Create, Delete, Insert, Predicate, Query, Select};
use crate::model::table::{Table, Value};
use rand::Rng;

use super::{frequency, pick};

impl Arbitrary for Create {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        Create {
            table: Table::arbitrary(rng),
        }
    }
}

impl ArbitraryFrom<Vec<Table>> for Select {
    fn arbitrary_from<R: Rng>(rng: &mut R, tables: &Vec<Table>) -> Self {
        let table = pick(tables, rng);
        Select {
            table: table.name.clone(),
            predicate: Predicate::arbitrary_from(rng, table),
        }
    }
}

impl ArbitraryFrom<Vec<&Table>> for Select {
    fn arbitrary_from<R: Rng>(rng: &mut R, tables: &Vec<&Table>) -> Self {
        let table = pick(tables, rng);
        Select {
            table: table.name.clone(),
            predicate: Predicate::arbitrary_from(rng, *table),
        }
    }
}

impl ArbitraryFrom<Table> for Insert {
    fn arbitrary_from<R: Rng>(rng: &mut R, table: &Table) -> Self {
        let num_rows = rng.gen_range(1..10);
        let values: Vec<Vec<Value>> = (0..num_rows)
            .map(|_| {
                table
                    .columns
                    .iter()
                    .map(|c| Value::arbitrary_from(rng, &c.column_type))
                    .collect()
            })
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
        frequency(
            vec![
                (1, Box::new(|rng| Query::Create(Create::arbitrary(rng)))),
                (
                    100,
                    Box::new(|rng| Query::Select(Select::arbitrary_from(rng, &vec![table]))),
                ),
                (
                    100,
                    Box::new(|rng| Query::Insert(Insert::arbitrary_from(rng, table))),
                ),
                (
                    0,
                    Box::new(|rng| Query::Delete(Delete::arbitrary_from(rng, table))),
                ),
            ],
            rng,
        )
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
        let operator = match predicate_value {
            true => one_of(
                vec![
                    Box::new(|rng| {
                        Predicate::Eq(
                            column.name.clone(),
                            Value::arbitrary_from(rng, &column_values),
                        )
                    }),
                    Box::new(|rng| {
                        Predicate::Gt(
                            column.name.clone(),
                            GTValue::arbitrary_from(rng, &column_values).0,
                        )
                    }),
                    Box::new(|rng| {
                        Predicate::Lt(
                            column.name.clone(),
                            LTValue::arbitrary_from(rng, &column_values).0,
                        )
                    }),
                ],
                rng,
            ),
            false => one_of(
                vec![
                    Box::new(|rng| {
                        Predicate::Neq(
                            column.name.clone(),
                            Value::arbitrary_from(rng, &column.column_type),
                        )
                    }),
                    Box::new(|rng| {
                        Predicate::Gt(
                            column.name.clone(),
                            LTValue::arbitrary_from(rng, &column_values).0,
                        )
                    }),
                    Box::new(|rng| {
                        Predicate::Lt(
                            column.name.clone(),
                            GTValue::arbitrary_from(rng, &column_values).0,
                        )
                    }),
                ],
                rng,
            ),
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
        one_of(
            vec![
                Box::new(|rng| Predicate::Eq(column_name.to_string(), (*value).clone())),
                Box::new(|rng| {
                    Predicate::Gt(
                        column_name.to_string(),
                        GTValue::arbitrary_from(rng, *value).0,
                    )
                }),
                Box::new(|rng| {
                    Predicate::Lt(
                        column_name.to_string(),
                        LTValue::arbitrary_from(rng, *value).0,
                    )
                }),
            ],
            rng,
        )
    }
}
