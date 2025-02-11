use crate::generation::table::{GTValue, LTValue};
use crate::generation::{one_of, Arbitrary, ArbitraryFrom};

use crate::model::query::select::{Distinctness, Predicate, ResultColumn};
use crate::model::query::{Create, Delete, Drop, Insert, Query, Select};
use crate::model::table::{Table, Value};
use crate::SimulatorEnv;
use rand::seq::SliceRandom as _;
use rand::Rng;

use super::property::Remaining;
use super::table::LikeValue;
use super::{backtrack, frequency, pick, ArbitraryFromMaybe};

impl Arbitrary for Create {
    fn arbitrary<R: Rng>(rng: &mut R) -> Self {
        Create {
            table: Table::arbitrary(rng),
        }
    }
}

impl ArbitraryFrom<&SimulatorEnv> for Select {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        let table = pick(&env.tables, rng);
        Self {
            table: table.name.clone(),
            result_columns: vec![ResultColumn::Star],
            predicate: Predicate::arbitrary_from(rng, table),
            limit: Some(rng.gen_range(0..=1000)),
            distinct: Distinctness::All,
        }
    }
}

impl ArbitraryFrom<&SimulatorEnv> for Insert {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        let gen_values = |rng: &mut R| {
            let table = pick(&env.tables, rng);
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
            Some(Insert::Values {
                table: table.name.clone(),
                values,
            })
        };

        let _gen_select = |rng: &mut R| {
            // Find a non-empty table
            let table = env.tables.iter().find(|t| !t.rows.is_empty());
            if table.is_none() {
                return None;
            }

            let select_table = table.unwrap();
            let row = pick(&select_table.rows, rng);
            let predicate = Predicate::arbitrary_from(rng, (select_table, row));
            // Pick another table to insert into
            let select = Select {
                table: select_table.name.clone(),
                result_columns: vec![ResultColumn::Star],
                predicate,
                limit: None,
                distinct: Distinctness::All,
            };
            let table = pick(&env.tables, rng);
            Some(Insert::Select {
                table: table.name.clone(),
                select: Box::new(select),
            })
        };

        backtrack(
            vec![
                (1, Box::new(|rng| gen_values(rng))),
                // todo: test and enable this once `INSERT INTO <table> SELECT * FROM <table>` is supported
                // (1, Box::new(|rng| gen_select(rng))),
            ],
            rng,
        )
    }
}

impl ArbitraryFrom<&SimulatorEnv> for Delete {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        let table = pick(&env.tables, rng);
        Self {
            table: table.name.clone(),
            predicate: Predicate::arbitrary_from(rng, table),
        }
    }
}

impl ArbitraryFrom<&SimulatorEnv> for Drop {
    fn arbitrary_from<R: Rng>(rng: &mut R, env: &SimulatorEnv) -> Self {
        let table = pick(&env.tables, rng);
        Self {
            table: table.name.clone(),
        }
    }
}

impl ArbitraryFrom<(&SimulatorEnv, &Remaining)> for Query {
    fn arbitrary_from<R: Rng>(rng: &mut R, (env, remaining): (&SimulatorEnv, &Remaining)) -> Self {
        frequency(
            vec![
                (
                    remaining.create,
                    Box::new(|rng| Self::Create(Create::arbitrary(rng))),
                ),
                (
                    remaining.read,
                    Box::new(|rng| Self::Select(Select::arbitrary_from(rng, env))),
                ),
                (
                    remaining.write,
                    Box::new(|rng| Self::Insert(Insert::arbitrary_from(rng, env))),
                ),
                (
                    remaining.write,
                    Box::new(|rng| Self::Delete(Delete::arbitrary_from(rng, env))),
                ),
            ],
            rng,
        )
    }
}

struct CompoundPredicate(Predicate);
struct SimplePredicate(Predicate);

impl ArbitraryFrom<(&Table, bool)> for SimplePredicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (table, predicate_value): (&Table, bool)) -> Self {
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

        Self(operator)
    }
}

impl ArbitraryFrom<(&Table, bool)> for CompoundPredicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (table, predicate_value): (&Table, bool)) -> Self {
        // Decide if you want to create an AND or an OR
        Self(if rng.gen_bool(0.7) {
            // An AND for true requires each of its children to be true
            // An AND for false requires at least one of its children to be false
            if predicate_value {
                Predicate::And(
                    (0..rng.gen_range(0..=3))
                        .map(|_| SimplePredicate::arbitrary_from(rng, (table, true)).0)
                        .collect(),
                )
            } else {
                // Create a vector of random booleans
                let mut booleans = (0..rng.gen_range(0..=3))
                    .map(|_| rng.gen_bool(0.5))
                    .collect::<Vec<_>>();

                let len = booleans.len();

                // Make sure at least one of them is false
                if !booleans.is_empty() && booleans.iter().all(|b| *b) {
                    booleans[rng.gen_range(0..len)] = false;
                }

                Predicate::And(
                    booleans
                        .iter()
                        .map(|b| SimplePredicate::arbitrary_from(rng, (table, *b)).0)
                        .collect(),
                )
            }
        } else {
            // An OR for true requires at least one of its children to be true
            // An OR for false requires each of its children to be false
            if predicate_value {
                // Create a vector of random booleans
                let mut booleans = (0..rng.gen_range(0..=3))
                    .map(|_| rng.gen_bool(0.5))
                    .collect::<Vec<_>>();
                let len = booleans.len();
                // Make sure at least one of them is true
                if !booleans.is_empty() && booleans.iter().all(|b| !*b) {
                    booleans[rng.gen_range(0..len)] = true;
                }

                Predicate::Or(
                    booleans
                        .iter()
                        .map(|b| SimplePredicate::arbitrary_from(rng, (table, *b)).0)
                        .collect(),
                )
            } else {
                Predicate::Or(
                    (0..rng.gen_range(0..=3))
                        .map(|_| SimplePredicate::arbitrary_from(rng, (table, false)).0)
                        .collect(),
                )
            }
        })
    }
}

impl ArbitraryFrom<&Table> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, table: &Table) -> Self {
        let predicate_value = rng.gen_bool(0.5);
        CompoundPredicate::arbitrary_from(rng, (table, predicate_value)).0
    }
}

impl ArbitraryFrom<(&str, &Value)> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (column_name, value): (&str, &Value)) -> Self {
        one_of(
            vec![
                Box::new(|_| Predicate::Eq(column_name.to_string(), (*value).clone())),
                Box::new(|rng| {
                    Self::Gt(
                        column_name.to_string(),
                        GTValue::arbitrary_from(rng, value).0,
                    )
                }),
                Box::new(|rng| {
                    Self::Lt(
                        column_name.to_string(),
                        LTValue::arbitrary_from(rng, value).0,
                    )
                }),
            ],
            rng,
        )
    }
}

/// Produces a predicate that is true for the provided row in the given table
fn produce_true_predicate<R: Rng>(rng: &mut R, (t, row): (&Table, &Vec<Value>)) -> Predicate {
    // Pick a column
    let column_index = rng.gen_range(0..t.columns.len());
    let column = &t.columns[column_index];
    let value = &row[column_index];
    backtrack(
        vec![
            (
                1,
                Box::new(|_| Some(Predicate::Eq(column.name.clone(), value.clone()))),
            ),
            (
                1,
                Box::new(|rng| {
                    let v = Value::arbitrary_from(rng, &column.column_type);
                    if &v == value {
                        None
                    } else {
                        Some(Predicate::Neq(column.name.clone(), v))
                    }
                }),
            ),
            (
                1,
                Box::new(|rng| {
                    Some(Predicate::Gt(
                        column.name.clone(),
                        LTValue::arbitrary_from(rng, value).0,
                    ))
                }),
            ),
            (
                1,
                Box::new(|rng| {
                    Some(Predicate::Lt(
                        column.name.clone(),
                        GTValue::arbitrary_from(rng, value).0,
                    ))
                }),
            ),
            (
                1,
                Box::new(|rng| {
                    LikeValue::arbitrary_from_maybe(rng, value)
                        .map(|like| Predicate::Like(column.name.clone(), like.0))
                }),
            ),
        ],
        rng,
    )
}

/// Produces a predicate that is false for the provided row in the given table
fn produce_false_predicate<R: Rng>(rng: &mut R, (t, row): (&Table, &Vec<Value>)) -> Predicate {
    // Pick a column
    let column_index = rng.gen_range(0..t.columns.len());
    let column = &t.columns[column_index];
    let value = &row[column_index];
    one_of(
        vec![
            Box::new(|_| Predicate::Neq(column.name.clone(), value.clone())),
            Box::new(|rng| {
                let v = loop {
                    let v = Value::arbitrary_from(rng, &column.column_type);
                    if &v != value {
                        break v;
                    }
                };
                Predicate::Eq(column.name.clone(), v)
            }),
            Box::new(|rng| {
                Predicate::Gt(column.name.clone(), GTValue::arbitrary_from(rng, value).0)
            }),
            Box::new(|rng| {
                Predicate::Lt(column.name.clone(), LTValue::arbitrary_from(rng, value).0)
            }),
        ],
        rng,
    )
}

impl ArbitraryFrom<(&Table, &Vec<Value>)> for Predicate {
    fn arbitrary_from<R: Rng>(rng: &mut R, (t, row): (&Table, &Vec<Value>)) -> Self {
        // We want to produce a predicate that is true for the row
        // We can do this by creating several predicates that
        // are true, some that are false, combiend them in ways that correspond to the creation of a true predicate

        // Produce some true and false predicates
        let mut true_predicates = (1..=rng.gen_range(1..=4))
            .map(|_| produce_true_predicate(rng, (t, row)))
            .collect::<Vec<_>>();

        let false_predicates = (0..=rng.gen_range(0..=3))
            .map(|_| produce_false_predicate(rng, (t, row)))
            .collect::<Vec<_>>();

        // Start building a top level predicate from a true predicate
        let mut result = true_predicates.pop().unwrap();

        let mut predicates = true_predicates
            .iter()
            .map(|p| (true, p.clone()))
            .chain(false_predicates.iter().map(|p| (false, p.clone())))
            .collect::<Vec<_>>();

        predicates.shuffle(rng);

        while !predicates.is_empty() {
            // Create a new predicate from at least 1 and at most 3 predicates
            let context =
                predicates[0..rng.gen_range(0..=usize::min(3, predicates.len()))].to_vec();
            // Shift `predicates` to remove the predicates in the context
            predicates = predicates[context.len()..].to_vec();

            // `result` is true, so we have the following three options to make a true predicate:
            // T or F
            // T or T
            // T and T

            result = one_of(
                vec![
                    // T or (X1 or X2 or ... or Xn)
                    Box::new(|_| {
                        Predicate::Or(vec![
                            result.clone(),
                            Predicate::Or(context.iter().map(|(_, p)| p.clone()).collect()),
                        ])
                    }),
                    // T or (T1 and T2 and ... and Tn)
                    Box::new(|_| {
                        Predicate::Or(vec![
                            result.clone(),
                            Predicate::And(context.iter().map(|(_, p)| p.clone()).collect()),
                        ])
                    }),
                    // T and T
                    Box::new(|_| {
                        // Check if all the predicates in the context are true
                        if context.iter().all(|(b, _)| *b) {
                            // T and (X1 or X2 or ... or Xn)
                            Predicate::And(vec![
                                result.clone(),
                                Predicate::And(context.iter().map(|(_, p)| p.clone()).collect()),
                            ])
                        }
                        // Check if there is at least one true predicate
                        else if context.iter().any(|(b, _)| *b) {
                            // T and (X1 or X2 or ... or Xn)
                            Predicate::And(vec![
                                result.clone(),
                                Predicate::Or(context.iter().map(|(_, p)| p.clone()).collect()),
                            ])
                        } else {
                            // T and (X1 or X2 or ... or Xn or TRUE)
                            Predicate::And(vec![
                                result.clone(),
                                Predicate::Or(
                                    context
                                        .iter()
                                        .map(|(_, p)| p.clone())
                                        .chain(std::iter::once(Predicate::true_()))
                                        .collect(),
                                ),
                            ])
                        }
                    }),
                ],
                rng,
            );
        }

        result
    }
}
