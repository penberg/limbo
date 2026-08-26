use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
    num::NonZeroUsize,
    ops::{Deref, DerefMut, Range},
    panic::RefUnwindSafe,
    rc::Rc,
    sync::Arc,
};
use turso_core::SqliteDialect;

use indexmap::IndexSet;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sql_generation::model::table::SimValue;
use turso_core::{Connection, Result, StepResult};

use crate::{
    generation::Shadow,
    model::{
        Query, ResultSet,
        metrics::InteractionStats,
        property::{Property, PropertyDiscriminants},
    },
    runner::env::{ShadowTablesMut, SimConnection, SimulationType, SimulatorEnv},
};

#[derive(Debug, Clone)]
pub(crate) struct InteractionPlan {
    plan: Vec<Interaction>,
    stats: InteractionStats,
    // In the future, this should probably be a stack of interactions
    // so we can have nested properties
    last_interactions: Option<Interactions>,
    pub mvcc: bool,

    /// Counts [Interactions]. Should not count transactions statements, just so we can generate more meaningful interactions per run
    /// This field is only necessary and valid when generating interactions. For static iteration, we do not care about this field
    len_properties: usize,
    next_interaction_id: NonZeroUsize,
}

impl InteractionPlan {
    pub(crate) fn new(mvcc: bool) -> Self {
        Self {
            plan: Vec::new(),
            stats: InteractionStats::default(),
            last_interactions: None,
            mvcc,
            len_properties: 0,
            next_interaction_id: NonZeroUsize::new(1).unwrap(),
        }
    }

    /// Count of interactions
    #[inline]
    pub fn len(&self) -> usize {
        self.plan.len()
    }

    /// Count of properties
    #[inline]
    pub fn len_properties(&self) -> usize {
        self.len_properties
    }

    pub fn next_property_id(&mut self) -> NonZeroUsize {
        let id = self.next_interaction_id;
        self.next_interaction_id = self
            .next_interaction_id
            .checked_add(1)
            .expect("Generated too many interactions, that overflowed ID generation");
        id
    }

    pub fn last_interactions(&self) -> Option<&Interactions> {
        self.last_interactions.as_ref()
    }

    pub fn push_interactions(&mut self, interactions: Interactions) {
        if !interactions.ignore() {
            self.len_properties += 1;
        }
        self.last_interactions = Some(interactions);
    }

    pub fn push(&mut self, interaction: Interaction) {
        self.plan.push(interaction);
    }

    /// Finds the range of interactions that are contained between the start and end spans for a given ID.
    pub fn find_interactions_range(&self, id: NonZeroUsize) -> Range<usize> {
        let interactions = self.interactions_list();
        let idx = interactions
            .binary_search_by_key(&id, |interaction| interaction.id())
            .map_err(|_| format!("Interaction containing id `{id}` should be present"))
            .unwrap();
        let interaction = &interactions[idx];

        let backward = || -> usize {
            interactions
                .iter()
                .enumerate()
                .rev()
                .skip(interactions.len() - idx)
                .find(|(_, interaction)| interaction.id() != id)
                .map(|(idx, _)| idx.saturating_add(1))
                .unwrap_or(idx)
        };

        let forward = || -> usize {
            interactions
                .iter()
                .enumerate()
                .skip(idx + 1)
                .find(|(_, interaction)| interaction.id() != id)
                .map(|(idx, _)| idx.saturating_sub(1))
                .unwrap_or(idx)
        };

        let range = if interaction.property_meta.is_some() {
            // go backward and find the interaction that is not the same id
            let start_idx = backward();
            // go forward and find the interaction that is not the same id
            let end_idx = forward();

            start_idx..end_idx + 1
        } else {
            idx..idx + 1
        };

        assert!(!range.is_empty());
        range
    }

    /// Truncates up to a particular interaction
    pub fn truncate(&mut self, len: usize) {
        self.plan.truncate(len);
    }

    /// Used to remove a particular [Interactions]
    pub fn remove_property(&mut self, id: NonZeroUsize) {
        let range = self.find_interactions_range(id);
        // Consume the drain iterator just to be sure
        for _interaction in self.plan.drain(range) {}
    }

    pub fn retain_mut<F>(&mut self, f: F)
    where
        F: FnMut(&mut Interaction) -> bool,
    {
        self.plan.retain_mut(f);
    }

    #[inline]
    pub fn interactions_list(&self) -> &[Interaction] {
        &self.plan
    }

    pub fn iter_properties(
        &self,
    ) -> IterProperty<
        std::iter::Peekable<std::iter::Enumerate<std::slice::Iter<'_, Interaction>>>,
        Forward,
    > {
        IterProperty {
            iter: self.interactions_list().iter().enumerate().peekable(),
            _direction: PhantomData,
        }
    }

    pub fn rev_iter_properties(
        &self,
    ) -> IterProperty<
        std::iter::Peekable<
            std::iter::Enumerate<std::iter::Rev<std::slice::Iter<'_, Interaction>>>,
        >,
        Backward,
    > {
        IterProperty {
            iter: self.interactions_list().iter().rev().enumerate().peekable(),
            _direction: PhantomData,
        }
    }

    pub fn stats(&self) -> &InteractionStats {
        &self.stats
    }

    pub fn stats_mut(&mut self) -> &mut InteractionStats {
        &mut self.stats
    }

    pub fn static_iterator(&self) -> impl InteractionPlanIterator {
        PlanIterator {
            iter: self.interactions_list().to_vec().into_iter(),
        }
    }
}

pub struct Forward;
pub struct Backward;

pub struct IterProperty<I, Dir> {
    iter: I,
    _direction: PhantomData<Dir>,
}

impl<'a, I> IterProperty<I, Forward>
where
    I: Iterator<Item = (usize, &'a Interaction)> + itertools::PeekingNext + std::fmt::Debug,
{
    pub fn next_property(&mut self) -> Option<impl Iterator<Item = (usize, &'a Interaction)>> {
        let (idx, interaction) = self.iter.next()?;
        let id = interaction.id();
        // get interactions with a particular property
        let first = std::iter::once((idx, interaction));

        let property_interactions = first.chain(
            self.iter
                .peeking_take_while(move |(_idx, interaction)| interaction.id() == id),
        );

        Some(property_interactions)
    }
}

impl<'a, I> IterProperty<I, Backward>
where
    I: Iterator<Item = (usize, &'a Interaction)>
        + DoubleEndedIterator
        + itertools::PeekingNext
        + std::fmt::Debug,
{
    pub fn next_property(&mut self) -> Option<impl Iterator<Item = (usize, &'a Interaction)>> {
        let (idx, interaction) = self.iter.next()?;
        let id = interaction.id();
        // get interactions with a particular id

        let first = std::iter::once((idx, interaction));

        let property_interactions = self
            .iter
            .peeking_take_while(move |(_idx, interaction)| interaction.id() == id)
            .chain(first);

        Some(property_interactions.into_iter())
    }
}

pub trait InteractionPlanIterator {
    fn next(&mut self, env: &mut SimulatorEnv) -> Option<Interaction>;
}

impl<T: InteractionPlanIterator> InteractionPlanIterator for &mut T {
    #[inline]
    fn next(&mut self, env: &mut SimulatorEnv) -> Option<Interaction> {
        T::next(self, env)
    }
}

pub struct PlanIterator<I: Iterator<Item = Interaction>> {
    iter: I,
}

impl<I> InteractionPlanIterator for PlanIterator<I>
where
    I: Iterator<Item = Interaction>,
{
    #[inline]
    fn next(&mut self, _env: &mut SimulatorEnv) -> Option<Interaction> {
        self.iter.next()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct InteractionPlanState {
    pub interaction_pointer: usize,
}

#[derive(Debug, Default, Clone)]
pub struct ConnectionState {
    pub stack: Vec<ResultSet>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Interactions {
    pub connection_index: usize,
    pub interactions: InteractionsType,
}

impl Interactions {
    pub fn new(connection_index: usize, interactions: InteractionsType) -> Self {
        Self {
            connection_index,
            interactions,
        }
    }

    /// Whether the interaction needs to check the database tables
    pub fn check_tables(&self) -> bool {
        match &self.interactions {
            InteractionsType::Property(property) => property.check_tables(),
            InteractionsType::Query(query) => query.is_dml(),
            // REOPEN_DATABASE tears down all connections and re-opens the
            // database, which exercises the on-disk recovery path (WAL replay,
            // header re-read, schema reload). Any committed row must still be
            // visible afterwards, so we verify it using the shared
            // `AllTableHaveExpectedContent` check. DISCONNECT only affects a
            // single in-memory connection and doesn't touch persistence, so
            // we don't follow it with a check.
            InteractionsType::Fault(fault) => {
                matches!(fault, Fault::ReopenDatabase | Fault::PowerLoss)
            }
        }
    }

    /// Interactions that are not counted/ignored in the InteractionPlan.
    /// Used in InteractionPlan to not count certain interactions to its length, as they are just auxiliary. This allows more
    /// meaningful interactions to be generation
    fn ignore(&self) -> bool {
        self.is_transaction()
            || matches!(
                self.interactions,
                InteractionsType::Property(Property::AllTableHaveExpectedContent { .. })
                    | InteractionsType::Property(Property::TableHasExpectedContent { .. })
            )
    }
}

impl Deref for Interactions {
    type Target = InteractionsType;

    fn deref(&self) -> &Self::Target {
        &self.interactions
    }
}

impl DerefMut for Interactions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.interactions
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InteractionsType {
    Property(Property),
    Query(Query),
    Fault(Fault),
}

impl InteractionsType {
    pub fn is_transaction(&self) -> bool {
        match self {
            InteractionsType::Query(query) => query.is_transaction(),
            _ => false,
        }
    }
}

impl Display for InteractionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const PAD: usize = 4;
        let mut indentation_level: usize = 0;
        let mut iter = self.iter_properties();
        while let Some(property) = iter.next_property() {
            let mut property = property.peekable();
            let mut start = true;
            while let Some((_, interaction)) = property.next() {
                if let Some(name) = interaction.property_meta.map(|p| p.property.name())
                    && start
                {
                    indentation_level = indentation_level.saturating_add(1);
                    writeln!(f, "-- begin testing '{name}'")?;
                    start = false;
                }

                if indentation_level > 0 {
                    let padding = " ".repeat(indentation_level * PAD);
                    f.pad(&padding)?;
                }
                writeln!(f, "{interaction}")?;
                if let Some(name) = interaction.property_meta.map(|p| p.property.name())
                    && property.peek().is_none()
                {
                    indentation_level = indentation_level.saturating_sub(1);
                    writeln!(f, "-- end testing '{name}'")?;
                }
            }
        }

        Ok(())
    }
}

type AssertionFunc =
    dyn Fn(&Vec<ResultSet>, &mut SimulatorEnv) -> Result<Result<(), String>> + RefUnwindSafe;

#[derive(Clone)]
pub struct Assertion {
    pub func: Rc<AssertionFunc>,
    pub name: String,        // For display purposes in the plan
    pub tables: Vec<String>, // Tables it depends on
}

impl Debug for Assertion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Assertion")
            .field("name", &self.name)
            .finish()
    }
}

impl Assertion {
    pub fn new<F>(name: String, func: F, tables: Vec<String>) -> Self
    where
        F: Fn(&Vec<ResultSet>, &mut SimulatorEnv) -> Result<Result<(), String>>
            + 'static
            + RefUnwindSafe,
    {
        Self {
            func: Rc::new(func),
            name,
            tables,
        }
    }

    pub fn dependencies(&self) -> IndexSet<String> {
        IndexSet::from_iter(self.tables.clone())
    }

    pub fn uses(&self) -> Vec<String> {
        self.tables.clone()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Fault {
    Disconnect,
    ReopenDatabase,
    PowerLoss,
}

impl Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Fault::Disconnect => write!(f, "DISCONNECT"),
            Fault::ReopenDatabase => write!(f, "REOPEN_DATABASE"),
            Fault::PowerLoss => write!(f, "POWER_LOSS"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PropertyMetadata {
    pub property: PropertyDiscriminants,
    // If the query is an extension query
    pub extension: bool,
}

impl PropertyMetadata {
    pub fn new(property: &Property, extension: bool) -> PropertyMetadata {
        Self {
            property: property.into(),
            extension,
        }
    }
}

#[derive(Debug, Clone, derive_builder::Builder)]
pub struct Interaction {
    pub connection_index: usize,
    pub interaction: InteractionType,
    #[builder(default)]
    pub ignore_error: bool,
    #[builder(setter(strip_option), default)]
    pub property_meta: Option<PropertyMetadata>,
    /// 0 id means the ID was not set
    id: NonZeroUsize,
}

impl InteractionBuilder {
    pub fn from_interaction(interaction: &Interaction) -> Self {
        let mut builder = Self::default();
        builder
            .connection_index(interaction.connection_index)
            .id(interaction.id())
            .ignore_error(interaction.ignore_error)
            .interaction(interaction.interaction.clone());
        if let Some(property_meta) = interaction.property_meta {
            builder.property_meta(property_meta);
        }
        builder
    }

    pub fn with_interaction(interaction: InteractionType) -> Self {
        let mut builder = Self::default();
        builder.interaction(interaction);
        builder
    }

    /// Checks to see if the property metadata was already set
    pub fn has_property_meta(&self) -> bool {
        self.property_meta.is_some()
    }
}

impl Deref for Interaction {
    type Target = InteractionType;

    fn deref(&self) -> &Self::Target {
        &self.interaction
    }
}

impl DerefMut for Interaction {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.interaction
    }
}

impl Interaction {
    pub fn id(&self) -> NonZeroUsize {
        self.id
    }

    pub fn uses(&self) -> Vec<String> {
        match &self.interaction {
            InteractionType::Query(query)
            | InteractionType::FsyncQuery(query)
            | InteractionType::FaultyQuery(query) => query.uses(),
            InteractionType::Assertion(assert) | InteractionType::Assumption(assert) => {
                assert.uses()
            }
            _ => vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub enum InteractionType {
    Query(Query),
    Assumption(Assertion),
    Assertion(Assertion),
    Fault(Fault),
    /// Will attempt to run any random query. However, when the connection tries to sync it will
    /// simulate power loss on the memory WAL backend, then reopen and retry the query.
    FsyncQuery(Query),
    FaultyQuery(Query),
}

pub(crate) struct FsyncQueryOutcome {
    pub(crate) result: ResultSet,
    pub(crate) power_lost: bool,
}

// FIXME: add the connection index here later
impl Display for Interaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}; -- {}", self.interaction, self.connection_index)
    }
}

impl Display for InteractionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Query(query) => write!(f, "{query}"),
            Self::Assumption(assumption) => write!(f, "-- ASSUME {}", assumption.name),
            Self::Assertion(assertion) => {
                write!(f, "-- ASSERT {};", assertion.name)
            }
            Self::Fault(fault) => write!(f, "-- FAULT '{fault}'"),
            Self::FsyncQuery(query) => {
                writeln!(f, "-- FSYNC QUERY: retry only after injected power loss")?;
                write!(f, "{query};")
            }
            Self::FaultyQuery(query) => write!(f, "{query}; -- FAULTY QUERY"),
        }
    }
}

impl Shadow for InteractionType {
    type Result = anyhow::Result<Vec<Vec<SimValue>>>;
    fn shadow(&self, env: &mut ShadowTablesMut) -> Self::Result {
        match self {
            Self::Query(query) => query.shadow(env),
            Self::Assumption(_)
            | Self::Assertion(_)
            | Self::Fault(_)
            | Self::FaultyQuery(_)
            | Self::FsyncQuery(_) => Ok(vec![]),
        }
    }
}

impl InteractionType {
    /// Statements that, in MVCC mode, require an exclusive transaction and
    /// are rejected inside BEGIN CONCURRENT. See `Query::requires_exclusive_tx`.
    pub fn requires_exclusive_tx(&self) -> bool {
        match self {
            InteractionType::Query(query)
            | InteractionType::FsyncQuery(query)
            | InteractionType::FaultyQuery(query) => query.requires_exclusive_tx(),
            _ => false,
        }
    }

    pub(crate) fn execute_query(&self, conn: &mut Arc<Connection>) -> ResultSet {
        if let Self::Query(query) = self {
            assert!(
                !matches!(query, Query::Placeholder),
                "simulation cannot have a placeholder Query for execution"
            );

            let query_str = query.to_string();
            let rows = conn.query(&query_str);
            if rows.is_err() {
                let err = rows.err();
                tracing::debug!(
                    "Error running query '{}': {:?}",
                    &query_str[0..query_str.len().min(4096)],
                    err
                );
                // Do not panic on parse error, because DoubleCreateFailure relies on it
                return Err(err.unwrap());
            }
            let rows = rows?;
            assert!(rows.is_some());
            let mut rows = rows.unwrap();
            let mut out = Vec::new();

            rows.run_with_row_callback(|row| {
                let mut r = Vec::new();
                for v in row.get_values() {
                    let v = v.into();
                    r.push(v);
                }
                out.push(r);
                Ok(())
            })?;

            Ok(out)
        } else {
            unreachable!("unexpected: this function should only be called on queries")
        }
    }

    pub(crate) fn execute_assertion(
        &self,
        stack: &Vec<ResultSet>,
        env: &mut SimulatorEnv,
    ) -> Result<()> {
        match self {
            Self::Assertion(assertion) => {
                let result = assertion.func.as_ref()(stack, env);
                match result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(message)) => Err(turso_core::LimboError::InternalError(format!(
                        "Assertion '{}' failed: {}",
                        assertion.name, message
                    ))),
                    Err(err) => Err(turso_core::LimboError::InternalError(format!(
                        "Assertion '{}' execution error: {}",
                        assertion.name, err
                    ))),
                }
            }
            _ => {
                unreachable!("unexpected: this function should only be called on assertions")
            }
        }
    }

    pub(crate) fn execute_assumption(
        &self,
        stack: &Vec<ResultSet>,
        env: &mut SimulatorEnv,
    ) -> Result<()> {
        match self {
            Self::Assumption(assumption) => {
                let result = assumption.func.as_ref()(stack, env);
                match result {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(message)) => Err(turso_core::LimboError::InternalError(format!(
                        "Assumption '{}' failed: {}",
                        assumption.name, message
                    ))),
                    Err(err) => Err(turso_core::LimboError::InternalError(format!(
                        "Assumption '{}' execution error: {}",
                        assumption.name, err
                    ))),
                }
            }
            _ => {
                unreachable!("unexpected: this function should only be called on assumptions")
            }
        }
    }

    pub(crate) fn execute_fault(&self, env: &mut SimulatorEnv, conn_index: usize) -> Result<()> {
        match self {
            Self::Fault(fault) => {
                match fault {
                    Fault::Disconnect => {
                        if env.connections[conn_index].is_connected() {
                            if env.conn_in_transaction(conn_index) {
                                env.rollback_conn(conn_index);
                            }
                            env.connections[conn_index].disconnect();
                        } else {
                            return Err(turso_core::LimboError::InternalError(
                                "connection already disconnected".into(),
                            ));
                        }
                    }
                    Fault::ReopenDatabase => {
                        reopen_database(env, false)?;
                    }
                    Fault::PowerLoss => {
                        if !env.can_simulate_power_loss() {
                            return Err(turso_core::LimboError::InvalidArgument(
                                "POWER_LOSS requires a non-MVCC default simulation using memory I/O"
                                    .into(),
                            ));
                        }
                        reopen_database(env, true)?;
                    }
                }
                Ok(())
            }
            _ => {
                unreachable!("unexpected: this function should only be called on faults")
            }
        }
    }

    pub(crate) fn execute_fsync_query(
        &self,
        conn: Arc<Connection>,
        env: &mut SimulatorEnv,
    ) -> FsyncQueryOutcome {
        if let Self::FsyncQuery(query) = self {
            assert!(
                env.can_simulate_power_loss(),
                "FsyncNoWait requires a non-MVCC default simulation using memory I/O"
            );

            let mut power_lost = false;
            let result = (|| {
                let query_str = query.to_string();
                let rows = conn.query(&query_str);
                if rows.is_err() {
                    let err = rows.err();
                    tracing::debug!(
                        "Error running query '{}': {:?}",
                        &query_str[0..query_str.len().min(4096)],
                        err
                    );
                    return Err(err.unwrap());
                }
                let mut rows = rows.unwrap().unwrap();
                let mut out = Vec::new();

                loop {
                    match rows.step()? {
                        StepResult::Row => {
                            let row = rows.row().unwrap();
                            let mut r = Vec::new();
                            for v in row.get_values() {
                                let v = v.into();
                                r.push(v);
                            }
                            out.push(r);
                        }
                        StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                            let syncing = env.io.syncing();
                            if syncing {
                                assert!(
                                    !power_lost,
                                    "an interrupted fsync query must stop after power loss"
                                );
                                reopen_database(env, true)?;
                                power_lost = true;
                            } else {
                                rows._io().step()?;
                            }
                        }
                        StepResult::Done => {
                            break;
                        }
                        StepResult::Busy => {
                            return Err(turso_core::LimboError::Busy);
                        }
                        StepResult::Interrupt => {}
                    }
                }

                Ok(out)
            })();

            FsyncQueryOutcome { result, power_lost }
        } else {
            unreachable!("unexpected: this function should only be called on queries")
        }
    }

    pub(crate) fn execute_faulty_query(
        &self,
        conn: &Arc<Connection>,
        env: &mut SimulatorEnv,
    ) -> ResultSet {
        use rand::Rng;
        if let Self::FaultyQuery(query) = self {
            let query_str = query.to_string();
            let rows = conn.query(&query_str);
            if rows.is_err() {
                let err = rows.err();
                tracing::debug!(
                    "Error running query '{}': {:?}",
                    &query_str[0..query_str.len().min(4096)],
                    err
                );
                if let Some(turso_core::LimboError::ParseError(e)) = err {
                    // Cross-connection sequence-schema lag is a benign
                    // transient: connection A's CREATE SEQUENCE commits;
                    // connection B picks the seq from the consistent
                    // model and runs nextval before B's per-connection
                    // schema reparse has observed A's commit. The model
                    // and engine are both internally consistent. Let it
                    // through as a normal error instead of aborting the
                    // run.
                    let is_seq_schema_lag =
                        e.starts_with("sequence \"") && e.contains("does not exist");
                    if !is_seq_schema_lag {
                        panic!("Unexpected parse error: {e}");
                    }
                    return Err(turso_core::LimboError::ParseError(e));
                }
                return Err(err.unwrap());
            }
            let mut rows = rows.unwrap().unwrap();
            let mut out = Vec::new();
            let mut current_prob = 0.05;
            let mut incr = 0.001;

            // Pre-compute path stems for selective fault injection
            let main_db_stem = env
                .get_db_path()
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            let aux_db_stems: Vec<(String, String)> = env
                .attached_dbs
                .iter()
                .map(|name| {
                    let stem = env
                        .get_aux_db_path(name)
                        .file_stem()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_string();
                    (name.clone(), stem)
                })
                .collect();

            loop {
                let syncing = env.io.syncing();

                // Decide faults independently per database
                let main_fault = env.rng.random_bool(current_prob);
                let aux_prob = (current_prob * 0.5).min(1.0);
                let mut fault_pairs: Vec<(&str, bool)> = vec![(&main_db_stem, main_fault)];
                for (_, stem) in &aux_db_stems {
                    let aux_fault = env.rng.random_bool(aux_prob);
                    fault_pairs.push((stem, aux_fault));
                }
                let any_fault = fault_pairs.iter().any(|(_, f)| *f);

                // TODO: avoid for now injecting faults when syncing
                if any_fault && !syncing {
                    env.io.inject_fault_selective(&fault_pairs);
                }

                let row = rows.run_one_step_blocking(
                    || Ok(()),
                    || {
                        current_prob += incr;
                        if current_prob > 1.0 {
                            current_prob = 1.0;
                        } else {
                            incr *= 1.01;
                        }
                        Ok(())
                    },
                )?;
                match row {
                    Some(row) => {
                        let mut r = Vec::new();
                        for v in row.get_values() {
                            let v = v.into();
                            r.push(v);
                        }
                        out.push(r);
                    }
                    None => break,
                }
            }

            Ok(out)
        } else {
            unreachable!("unexpected: this function should only be called on queries")
        }
    }
}

fn reopen_database(env: &mut SimulatorEnv, power_loss: bool) -> Result<()> {
    // 1. Close all connections without default checkpoint-on-close behavior
    // to expose bugs related to how we handle WAL
    let mvcc = env.profile.mvcc;
    let num_conns = env.connections.len();

    // Clear shadow transaction state for all connections since reopening
    // the database resets all transaction state
    for conn_index in 0..num_conns {
        if env.conn_in_transaction(conn_index) {
            env.rollback_conn(conn_index);
        }
    }

    if power_loss {
        // Reject cleanup I/O from the old database objects after the crash boundary.
        env.io.power_loss()?;
    }

    env.connections.clear();
    env.db = None;

    if power_loss {
        // The interrupted statement still owns its old connection while this
        // function opens the replacement database. A real process restart has
        // no process-wide database registry, so the simulator must discard it
        // or the new open can return the old Database and its stale handles.
        turso_core::clear_database_registry();
    } else {
        // TODO: for correct reporting of faults we should get all the recorded numbers and transfer to the new file
        env.io.close_files();
    }

    // 2. Re-open the shared database object. Connections are recreated below
    // through SimulatorEnv::connect so their normal cache and ATTACH setup is
    // applied after a restart too.
    match env.type_ {
        SimulationType::Differential => {}
        SimulationType::Default | SimulationType::Doublecheck => {
            let db = match turso_core::Database::open_file_with_flags(
                env.io.clone(),
                env.get_db_path().to_str().expect("path should be 'to_str'"),
                turso_core::OpenFlags::default(),
                turso_core::DatabaseOpts::new()
                    .with_autovacuum(true)
                    .with_attach(true)
                    .with_generated_columns(true),
                None,
                Arc::new(SqliteDialect),
            ) {
                Ok(db) => db,
                Err(e) => {
                    tracing::error!(
                        "Failed to open database at {}: {}",
                        env.get_db_path().display(),
                        e
                    );
                    panic!("Failed to open database: {e}");
                }
            };

            env.db = Some(db);

            // Enable MVCC via PRAGMA if requested
            if mvcc {
                let conn = env.db.as_ref().expect("db to be Some").connect().unwrap();
                conn.pragma_update("journal_mode", "'mvcc'")
                    .expect("enable mvcc");
            }
        }
    };

    env.connections
        .extend((0..num_conns).map(|_| SimConnection::Disconnected));
    for connection_index in 0..num_conns {
        env.connect(connection_index);
    }

    Ok(())
}
