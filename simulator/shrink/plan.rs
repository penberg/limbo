use crate::{
    generation::{
        plan::{InteractionPlan, Interactions},
        property::Property,
    },
    model::query::Query,
    runner::execution::Execution,
};

impl InteractionPlan {
    /// Create a smaller interaction plan by deleting a property
    pub(crate) fn shrink_interaction_plan(&self, failing_execution: &Execution) -> InteractionPlan {
        // todo: this is a very naive implementation, next steps are;
        // - Shrink to multiple values by removing random interactions
        // - Shrink properties by removing their extensions, or shrinking their values

        let mut plan = self.clone();
        let failing_property = &self.plan[failing_execution.interaction_index];
        let depending_tables = failing_property.dependencies();

        let before = self.plan.len();

        // Remove all properties after the failing one
        plan.plan.truncate(failing_execution.interaction_index + 1);
        // Remove all properties that do not use the failing tables
        plan.plan
            .retain(|p| p.uses().iter().any(|t| depending_tables.contains(t)));

        // Remove the extensional parts of the properties
        for interaction in plan.plan.iter_mut() {
            if let Interactions::Property(p) = interaction {
                match p {
                    Property::InsertValuesSelect { queries, .. }
                    | Property::DoubleCreateFailure { queries, .. }
                    | Property::DeleteSelect { queries, .. } => {
                        queries.clear();
                    }
                    Property::SelectLimit { .. } => {}
                }
            }
        }

        plan.plan
            .retain(|p| !matches!(p, Interactions::Query(Query::Select(_))));

        let after = plan.plan.len();

        log::info!(
            "Shrinking interaction plan from {} to {} properties",
            before,
            after
        );

        plan
    }
}
