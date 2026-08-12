use std::collections::{BTreeMap, BTreeSet};

use super::super::*;

impl ApplicationState {
    pub(super) fn assign_one_evaluation(
        &mut self,
        assignment: EvaluationAssignment,
    ) -> Result<CommandResult, DomainError> {
        let result = self.assign_evaluation(assignment)?;
        if let CommandResult::EvaluationAssigned(id) = result {
            self.evaluation_batches
                .entry(id)
                .or_insert(EvaluationBatch {
                    expected_results: 1,
                    results: BTreeMap::new(),
                });
        }
        Ok(result)
    }

    pub(super) fn assign_evaluations(
        &mut self,
        assignments: Vec<EvaluationAssignment>,
    ) -> Result<CommandResult, DomainError> {
        let mut keys = BTreeSet::new();
        for assignment in &assignments {
            assignment.validate()?;
            if !keys.insert(EvaluationAssignmentKey::from(assignment)) {
                return Err(DomainError::InvalidEvaluation(
                    "evaluation assignment batch contains a duplicate executor".to_owned(),
                ));
            }
        }
        let mut accepted = BTreeMap::<EvaluationId, u16>::new();
        for assignment in assignments {
            if let CommandResult::EvaluationAssigned(id) = self.assign_evaluation(assignment)? {
                *accepted.entry(id).or_default() += 1;
            }
        }
        for (id, expected_results) in accepted {
            self.evaluation_batches
                .entry(id)
                .or_insert(EvaluationBatch {
                    expected_results,
                    results: BTreeMap::new(),
                });
        }
        Ok(CommandResult::Noop)
    }

    fn assign_evaluation(
        &mut self,
        assignment: EvaluationAssignment,
    ) -> Result<CommandResult, DomainError> {
        assignment.validate()?;
        if self.draining_nodes.contains(&assignment.executor_node_id) {
            return Ok(CommandResult::EvaluationDiscarded);
        }
        let Some(target) = self.targets.get(&assignment.id.target_id) else {
            return Ok(CommandResult::EvaluationDiscarded);
        };
        if target.paused {
            return Ok(CommandResult::EvaluationDiscarded);
        }
        if target
            .latest_evaluation
            .as_ref()
            .is_some_and(|latest| latest.id.scheduled_at_ms >= assignment.id.scheduled_at_ms)
            || target.history.contains_key(&assignment.id.scheduled_at_ms)
            || self
                .assignments
                .get(&EvaluationAssignmentKey::from(&assignment))
                .is_some_and(|current| current.attempt >= assignment.attempt)
            || self
                .assignments
                .keys()
                .any(|key| key.id.target_id == assignment.id.target_id && key.id != assignment.id)
            || self
                .evaluation_batches
                .get(&assignment.id)
                .is_some_and(|batch| batch.results.contains_key(&assignment.executor_node_id))
        {
            return Ok(CommandResult::EvaluationDiscarded);
        }
        let id = assignment.id;
        let key = EvaluationAssignmentKey::from(&assignment);
        let key_exists = self.assignments.contains_key(&key);
        if self.evaluation_batches.contains_key(&id) && !key_exists {
            let replaced = self
                .assignments
                .iter()
                .find(|(_, current)| {
                    current.id == id
                        && current.attempt < assignment.attempt
                        && current.expires_at_ms <= assignment.assigned_at_ms
                })
                .map(|(key, _)| *key);
            let Some(replaced) = replaced else {
                return Ok(CommandResult::EvaluationDiscarded);
            };
            self.assignments.remove(&replaced);
        }
        self.assignments.insert(key, assignment);
        Ok(CommandResult::EvaluationAssigned(id))
    }
}
