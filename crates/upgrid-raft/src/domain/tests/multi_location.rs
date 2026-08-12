use super::evaluation::{evaluation, id, state_with_target, target};
use super::*;

#[test]
fn location_results_are_aggregated_after_every_assigned_node_reports() {
    let (mut state, target_id, channel_id) = state_with_target();
    state
        .apply(Command::UpdateTargetWithLocations {
            target: target(target_id, channel_id),
            use_default_notifications: true,
            locations: 2,
        })
        .unwrap();
    let evaluation_id = EvaluationId {
        target_id,
        scheduled_at_ms: 1_000,
    };
    let assignments = [id(10), id(11)]
        .into_iter()
        .map(|executor_node_id| EvaluationAssignment {
            id: evaluation_id,
            executor_node_id,
            assigned_at_ms: 900,
            expires_at_ms: 2_000,
            attempt: 1,
        })
        .collect();
    state
        .apply(Command::AssignEvaluations(assignments))
        .unwrap();

    let mut passed = evaluation(target_id, 1_000, true);
    passed.executor_node_id = id(10);
    passed.http.latency_ms = 20;
    assert_eq!(
        state.apply(Command::RecordEvaluation(passed)).unwrap(),
        CommandResult::EvaluationPending(evaluation_id)
    );
    assert!(state.targets[&target_id].history.is_empty());

    let mut failed = evaluation(target_id, 1_000, false);
    failed.executor_node_id = id(11);
    failed.http.latency_ms = 80;
    assert!(matches!(
        state.apply(Command::RecordEvaluation(failed)).unwrap(),
        CommandResult::EvaluationAccepted { .. }
    ));

    let aggregate = state.targets[&target_id]
        .latest_evaluation
        .as_ref()
        .unwrap();
    assert!(!aggregate.succeeded);
    assert_eq!(aggregate.http.latency_ms, 80);
    assert_eq!(aggregate.http.received_bytes, 4);
    assert!(
        aggregate
            .diagnostic
            .as_deref()
            .is_some_and(|value| value.contains("1/2 locations failed"))
    );
    assert!(!state.has_evaluation_assignment(evaluation_id));
    assert!(state.evaluation_results(evaluation_id).is_none());
}

#[test]
fn expired_location_replacement_does_not_increase_expected_results() {
    let (mut state, target_id, _) = state_with_target();
    let evaluation_id = EvaluationId {
        target_id,
        scheduled_at_ms: 1_000,
    };
    state
        .apply(Command::AssignEvaluations(
            [id(10), id(11)]
                .into_iter()
                .map(|executor_node_id| EvaluationAssignment {
                    id: evaluation_id,
                    executor_node_id,
                    assigned_at_ms: 900,
                    expires_at_ms: 1_100,
                    attempt: 1,
                })
                .collect(),
        ))
        .unwrap();

    assert_eq!(
        state
            .apply(Command::AssignEvaluation(EvaluationAssignment {
                id: evaluation_id,
                executor_node_id: id(12),
                assigned_at_ms: 1_100,
                expires_at_ms: 2_000,
                attempt: 2,
            }))
            .unwrap(),
        CommandResult::EvaluationAssigned(evaluation_id),
    );
    assert_eq!(state.assignments.len(), 2);
    assert_eq!(state.expected_evaluation_results(evaluation_id), Some(2));
    assert!(
        !state
            .assignments
            .values()
            .any(|assignment| assignment.executor_node_id == id(10))
    );
    assert!(
        state
            .assignments
            .values()
            .any(|assignment| assignment.executor_node_id == id(12))
    );
}

#[test]
fn location_count_is_validated_before_target_mutation() {
    let (mut state, target_id, channel_id) = state_with_target();
    for locations in [0, MAX_EVALUATION_LOCATIONS + 1] {
        assert!(matches!(
            state.apply(Command::UpdateTargetWithLocations {
                target: target(target_id, channel_id),
                use_default_notifications: true,
                locations,
            }),
            Err(DomainError::InvalidTarget(_))
        ));
    }
    assert_eq!(state.target_location_count(target_id), 1);
}
