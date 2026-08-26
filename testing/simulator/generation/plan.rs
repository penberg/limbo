use std::{num::NonZeroUsize, vec};

use sql_generation::{
    generation::{Arbitrary, ArbitraryFrom, GenerationContext, frequency},
    model::query::{
        Create,
        transaction::{Begin, Commit},
    },
};

use crate::{
    SimulatorEnv,
    generation::{
        WeightedDistribution,
        property::PropertyDistribution,
        query::{QueryDistribution, possible_queries},
    },
    model::{
        Query,
        interactions::{
            Fault, Interaction, InteractionBuilder, InteractionPlan, InteractionPlanIterator,
            InteractionType, Interactions, InteractionsType,
        },
        metrics::{InteractionStats, Remaining},
        property::Property,
    },
    runner::env::SimulationType,
};

impl InteractionPlan {
    pub fn generator<'a>(
        &'a mut self,
        rng: &'a mut impl rand::Rng,
    ) -> impl InteractionPlanIterator {
        let interactions = self.interactions_list().to_vec();
        let iter = interactions.into_iter();
        PlanGenerator {
            plan: self,
            peek: None,
            iter,
            rng,
        }
    }

    /// Appends a new [Interactions] and outputs the next set of [Interaction] to take
    pub fn generate_next_interaction(
        &mut self,
        rng: &mut impl rand::Rng,
        env: &mut SimulatorEnv,
    ) -> Option<Interactions> {
        // First interaction
        if self.len_properties() == 0 {
            // First create at least one table
            let create_query = Create::arbitrary(&mut env.rng.clone(), &env.connection_context(0));

            // initial query starts at 0th connection
            let interactions =
                Interactions::new(0, InteractionsType::Query(Query::Create(create_query)));
            return Some(interactions);
        }

        let num_interactions = env.opts.max_interactions as usize;
        // After a fault or injected query, an empty expected schema still matters:
        // it proves that the last table stayed dropped. Ordinary DML checks name
        // one table, so those still require a non-empty schema.
        if let Some(i) = self.last_interactions()
            && i.check_tables()
            && (!env
                .connection_context(i.connection_index)
                .tables()
                .is_empty()
                || !matches!(&i.interactions, InteractionsType::Query(_)))
        {
            let interactions = if let InteractionsType::Query(query) = &i.interactions {
                assert!(query.is_dml());
                let mut table = query.uses();
                assert!(table.len() == 1);
                let table = table.pop().unwrap();

                Interactions::new(
                    i.connection_index,
                    InteractionsType::Property(Property::TableHasExpectedContent { table }),
                )
            } else {
                Interactions::new(
                    i.connection_index,
                    InteractionsType::Property(Property::AllTableHaveExpectedContent {
                        tables: env
                            .connection_context(i.connection_index)
                            .tables()
                            .iter()
                            .map(|t| t.name.clone())
                            .collect(),
                    }),
                )
            };

            return Some(interactions);
        }

        if self.len_properties() < num_interactions {
            let conn_index = env.choose_conn(rng);
            let interactions = if self.mvcc && !env.conn_in_transaction(conn_index) {
                let query = Query::Begin(Begin::Concurrent);
                Interactions::new(conn_index, InteractionsType::Query(query))
            } else if self.mvcc
                && env.conn_in_transaction(conn_index)
                && env.has_conn_executed_query_after_transaction(conn_index)
                && rng.random_bool(0.4)
            {
                let query = Query::Commit(Commit);
                Interactions::new(conn_index, InteractionsType::Query(query))
            } else {
                let conn_ctx = &env.connection_context(conn_index);
                let mut interactions =
                    Interactions::arbitrary_from(rng, conn_ctx, (env, self.stats(), conn_index));

                // With 30% probability, redirect CREATE TABLE to an attached database
                if let InteractionsType::Query(Query::Create(ref mut create)) =
                    interactions.interactions
                {
                    if !env.attached_dbs.is_empty() && rng.random_bool(0.3) {
                        let db = &env.attached_dbs[rng.random_range(0..env.attached_dbs.len())];
                        create.table.name = format!("{}.{}", db, create.table.name);
                    }
                }

                interactions
            };

            tracing::debug!(
                "Generating interaction {}/{}",
                self.len_properties(),
                num_interactions
            );

            Some(interactions)
        } else {
            // after we generated all interactions if some connection is still in a transaction, commit
            (0..env.connections.len())
                .find(|idx| env.conn_in_transaction(*idx))
                .map(|conn_index| {
                    Interactions::new(conn_index, InteractionsType::Query(Query::Commit(Commit)))
                })
        }
    }
}

pub struct PlanGenerator<'a, R: rand::Rng> {
    plan: &'a mut InteractionPlan,
    peek: Option<Interaction>,
    iter: <Vec<Interaction> as IntoIterator>::IntoIter,
    rng: &'a mut R,
}

impl<'a, R: rand::Rng> PlanGenerator<'a, R> {
    fn next_interaction(&mut self, env: &mut SimulatorEnv) -> Option<Interaction> {
        self.iter
            .next()
            .or_else(|| {
                // Iterator ended, try to create a new iterator
                // This will not be an infinte sequence because generate_next_interaction will eventually
                // stop generating
                let interactions = self.plan.generate_next_interaction(self.rng, env)?;

                let id = self.plan.next_property_id();

                let iter = interactions.interactions(id);

                assert!(!iter.is_empty());

                let mut iter = iter.into_iter();

                self.plan.push_interactions(interactions);

                let next = iter.next();
                self.iter = iter;

                next
            })
            .map(|interaction| {
                // Certain properties can generate intermediate queries
                // we need to generate them here and substitute
                if let InteractionType::Query(Query::Placeholder) = &interaction.interaction {
                    let stats = self.plan.stats();

                    let conn_ctx = env.connection_context(interaction.connection_index);

                    let remaining_ = Remaining::new(
                        env.opts.max_interactions,
                        &env.profile.query,
                        stats,
                        env.profile.mvcc,
                        &conn_ctx,
                        env.sequence_info(),
                    );

                    let Some(InteractionsType::Property(property)) = self
                        .plan
                        .last_interactions()
                        .as_ref()
                        .map(|interactions| &interactions.interactions)
                    else {
                        unreachable!("only properties have extensional queries");
                    };

                    let queries = possible_queries(conn_ctx.tables());
                    let query_distr = QueryDistribution::new(queries, &remaining_, false);

                    let query_gen = property.get_extensional_query_gen_function();

                    let mut count = 0;
                    let new_query = loop {
                        if count > 1_000_000 {
                            panic!("possible infinite loop in query generation");
                        }
                        if let Some(new_query) =
                            (query_gen)(self.rng, &conn_ctx, &query_distr, property)
                        {
                            break new_query;
                        }
                        count += 1;
                    };

                    InteractionBuilder::from_interaction(&interaction)
                        .interaction(InteractionType::Query(new_query))
                        .build()
                        .unwrap()
                } else {
                    interaction
                }
            })
    }

    fn peek(&mut self, env: &mut SimulatorEnv) -> Option<&Interaction> {
        if self.peek.is_none() {
            self.peek = self.next_interaction(env);
        }
        self.peek.as_ref()
    }
}

impl<'a, R: rand::Rng> InteractionPlanIterator for PlanGenerator<'a, R> {
    /// try to generate the next [Interactions] and store it
    fn next(&mut self, env: &mut SimulatorEnv) -> Option<Interaction> {
        let mvcc = self.plan.mvcc;
        let mut next_interaction = || match self.peek(env) {
            Some(peek_interaction) => {
                if mvcc && peek_interaction.requires_exclusive_tx() {
                    // The simulator only ever opens BEGIN CONCURRENT in MVCC
                    // mode (see plan.rs above). Statements that require an
                    // exclusive write tx — DDL and setval — would be rejected
                    // there, so commit the open tx first to fall back to
                    // autocommit (which IS exclusive).

                    if let Some(conn_index) =
                        (0..env.connections.len()).find(|idx| env.conn_in_transaction(*idx))
                    {
                        return Some(
                            InteractionBuilder::from_interaction(peek_interaction)
                                .interaction(InteractionType::Query(Query::Commit(Commit)))
                                .connection_index(conn_index)
                                .build()
                                .unwrap(),
                        );
                    }
                }

                self.peek.take()
            }
            None => {
                // after we generated all interactions if some connection is still in a transaction, commit
                let commit = (0..env.connections.len())
                    .find(|idx| env.conn_in_transaction(*idx))
                    .map(|conn_index| {
                        let query = Query::Commit(Commit);
                        let interactions =
                            Interactions::new(conn_index, InteractionsType::Query(query));

                        let interaction = InteractionBuilder::with_interaction(
                            InteractionType::Query(Query::Commit(Commit)),
                        )
                        .connection_index(conn_index)
                        .id(self.plan.next_property_id())
                        .build()
                        .unwrap();

                        self.plan.push_interactions(interactions);

                        interaction
                    });

                #[cfg(debug_assertions)]
                if commit.is_none() {
                    // Do a final sanity check to make sure that all interactions are sorted by ids
                    assert!(self.plan.interactions_list().is_sorted_by_key(|a| a.id()));
                }
                commit
            }
        };
        let next_interaction = next_interaction();
        // intercept interaction to update metrics
        if let Some(next_interaction) = next_interaction.as_ref() {
            // Skip counting queries that come from Properties that only exist to check tables
            if let Some(property_meta) = next_interaction.property_meta
                && !property_meta.property.check_tables()
            {
                self.plan.stats_mut().update(next_interaction);
            }
            self.plan.push(next_interaction.clone());
        }

        next_interaction
    }
}

impl Interactions {
    pub(crate) fn interactions(&self, id: NonZeroUsize) -> Vec<Interaction> {
        let ret = match &self.interactions {
            InteractionsType::Property(property) => {
                property.interactions(self.connection_index, id)
            }
            InteractionsType::Query(query) => {
                let mut builder =
                    InteractionBuilder::with_interaction(InteractionType::Query(query.clone()));
                builder.connection_index(self.connection_index).id(id);
                let interaction = builder.build().unwrap();
                vec![interaction]
            }
            InteractionsType::Fault(fault) => {
                let mut builder =
                    InteractionBuilder::with_interaction(InteractionType::Fault(*fault));
                builder.connection_index(self.connection_index).id(id);
                let interaction = builder.build().unwrap();
                vec![interaction]
            }
        };

        assert!(!ret.is_empty());
        ret
    }
}

fn random_fault<R: rand::Rng + ?Sized>(
    rng: &mut R,
    env: &SimulatorEnv,
    conn_index: usize,
) -> Interactions {
    let mut faults = vec![Fault::Disconnect];
    if !env.opts.disable_reopen_database {
        faults.push(Fault::ReopenDatabase);
        if env.can_simulate_power_loss() {
            faults.push(Fault::PowerLoss);
        }
    }
    let fault = faults[rng.random_range(0..faults.len())];
    Interactions::new(conn_index, InteractionsType::Fault(fault))
}

impl ArbitraryFrom<(&SimulatorEnv, &InteractionStats, usize)> for Interactions {
    fn arbitrary_from<R: rand::Rng + ?Sized, C: GenerationContext>(
        rng: &mut R,
        conn_ctx: &C,
        (env, stats, conn_index): (&SimulatorEnv, &InteractionStats, usize),
    ) -> Self {
        let remaining_ = Remaining::new(
            env.opts.max_interactions,
            &env.profile.query,
            stats,
            env.profile.mvcc,
            conn_ctx,
            env.sequence_info(),
        );

        let queries = possible_queries(conn_ctx.tables());
        let allow_checkpoints =
            !env.profile.mvcc && !matches!(env.type_, SimulationType::Differential);
        let query_distr = QueryDistribution::new(queries, &remaining_, allow_checkpoints);

        #[expect(clippy::type_complexity)]
        let mut choices: Vec<(u32, Box<dyn Fn(&mut R) -> Interactions>)> = vec![
            (
                query_distr.weights().total_weight(),
                Box::new(|rng: &mut R| {
                    Interactions::new(
                        conn_index,
                        InteractionsType::Query(Query::arbitrary_from(rng, conn_ctx, &query_distr)),
                    )
                }),
            ),
            (
                remaining_
                    .select
                    .min(remaining_.insert)
                    .min(remaining_.create)
                    .max(1),
                Box::new(|rng: &mut R| random_fault(rng, env, conn_index)),
            ),
        ];

        if let Ok(property_distr) =
            PropertyDistribution::new(env, &remaining_, &query_distr, conn_ctx)
        {
            choices.push((
                property_distr.weights().total_weight(),
                Box::new(move |rng: &mut R| {
                    Interactions::new(
                        conn_index,
                        InteractionsType::Property(Property::arbitrary_from(
                            rng,
                            conn_ctx,
                            &property_distr,
                        )),
                    )
                }),
            ));
        };

        frequency(choices, rng)
    }
}
