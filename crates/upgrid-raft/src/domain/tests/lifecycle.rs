#[test]
fn assignment_batch_applies_every_valid_assignment() {
    let (mut state, first_target_id, channel_id) = state_with_target();
    let second_target_id = TargetId(id(20));
    state
        .apply(Command::CreateTarget(target(second_target_id, channel_id)))
        .unwrap();
    let assignments = [first_target_id, second_target_id]
        .into_iter()
        .map(|target_id| EvaluationAssignment {
            id: EvaluationId {
                target_id,
                scheduled_at_ms: 1_000,
            },
            executor_node_id: id(10),
            assigned_at_ms: 900,
            expires_at_ms: 2_000,
            attempt: 1,
        })
        .collect::<Vec<_>>();

    assert_eq!(
        state
            .apply(Command::AssignEvaluations(assignments.clone()))
            .unwrap(),
        CommandResult::Noop
    );
    assert!(
        assignments
            .iter()
            .all(|assignment| state.assignments.contains_key(&assignment.id))
    );
}

#[test]
fn updating_a_target_preserves_runtime_state() {
    let (mut state, target_id, channel_id) = state_with_target();
    state
        .apply(Command::RecordEvaluation(evaluation(
            target_id, 1_000, true,
        )))
        .unwrap();
    let mut updated = target(target_id, channel_id);
    updated.name = "Renamed".to_owned();

    state.apply(Command::UpdateTarget(updated)).unwrap();

    let target_state = &state.targets[&target_id];
    assert_eq!(target_state.target.name, "Renamed");
    assert_eq!(target_state.availability, AvailabilityState::Up);
    assert_eq!(target_state.history.len(), 1);
}

#[test]
fn pausing_a_target_preserves_history_and_cancels_assignments() {
    let (mut state, target_id, _) = state_with_target();
    state
        .apply(Command::RecordEvaluation(evaluation(
            target_id, 1_000, true,
        )))
        .unwrap();
    let evaluation_id = EvaluationId {
        target_id,
        scheduled_at_ms: 2_000,
    };
    state
        .apply(Command::AssignEvaluation(EvaluationAssignment {
            id: evaluation_id,
            executor_node_id: id(10),
            assigned_at_ms: 1_900,
            expires_at_ms: 3_000,
            attempt: 1,
        }))
        .unwrap();

    state
        .apply(Command::SetTargetPaused {
            target_id,
            paused: true,
        })
        .unwrap();

    assert!(state.targets[&target_id].paused);
    assert_eq!(state.targets[&target_id].history.len(), 1);
    assert!(!state.assignments.contains_key(&evaluation_id));

    state
        .apply(Command::SetTargetPaused {
            target_id,
            paused: false,
        })
        .unwrap();
    assert!(!state.targets[&target_id].paused);
}

#[test]
fn delivered_alerts_cannot_regress_to_pending() {
    let (mut state, target_id, channel_id) = state_with_target();
    for scheduled_at_ms in [1_000, 2_000, 3_000] {
        state
            .apply(Command::RecordEvaluation(evaluation(
                target_id,
                scheduled_at_ms,
                false,
            )))
            .unwrap();
    }
    let alert_id = AlertId {
        target_id,
        channel_id,
        evaluation_scheduled_at_ms: 3_000,
        kind: AlertKind::Down,
    };
    state
        .apply(Command::MarkAlertDelivered {
            alert_id,
            delivered_at_ms: 3_100,
        })
        .unwrap();
    state
        .apply(Command::RecordAlertFailure {
            alert_id,
            attempted_at_ms: 3_200,
            retry_at_ms: Some(4_000),
            diagnostic: "late failure".to_owned(),
        })
        .unwrap();

    assert_eq!(
        state.alerts[&alert_id].delivery,
        AlertDelivery::Delivered {
            delivered_at_ms: 3_100
        }
    );
}

#[test]
fn history_retention_is_replicated_configuration() {
    let mut state = ApplicationState::default();

    assert_eq!(
        state
            .apply(Command::SetHistoryRetention {
                retention_ms: 6 * 60 * 60 * 1_000,
            })
            .unwrap(),
        CommandResult::HistoryRetentionSet(6 * 60 * 60 * 1_000)
    );
    assert_eq!(state.history_retention_ms, 6 * 60 * 60 * 1_000);
    assert!(
        state
            .apply(Command::SetHistoryRetention { retention_ms: 0 })
            .is_err()
    );
}

#[test]
fn join_token_is_reusable_until_expiry_or_revocation() {
    let mut state = ApplicationState::default();
    let hash = JoinTokenHash([7; 32]);
    state
        .apply(Command::PutJoinToken {
            hash,
            expires_at_ms: 2_000,
        })
        .unwrap();

    assert_eq!(
        state
            .apply(Command::AuthorizeJoinToken {
                hash,
                authorized_at_ms: 1_000,
            })
            .unwrap(),
        CommandResult::JoinTokenAuthorized
    );
    assert_eq!(
        state
            .apply(Command::AuthorizeJoinToken {
                hash,
                authorized_at_ms: 1_001,
            })
            .unwrap(),
        CommandResult::JoinTokenAuthorized
    );
    assert!(
        state
            .apply(Command::AuthorizeJoinToken {
                hash,
                authorized_at_ms: 2_001,
            })
            .is_err()
    );
    assert_eq!(
        state.apply(Command::RevokeJoinToken(hash)).unwrap(),
        CommandResult::JoinTokenRevoked
    );
    assert!(
        state
            .apply(Command::AuthorizeJoinToken {
                hash,
                authorized_at_ms: 1_000,
            })
            .is_err()
    );
}

#[test]
fn reusable_join_token_authorizes_different_nodes() {
    let mut state = ApplicationState::default();
    let token = "reusable-invitation";
    let hash = crate::token::hash_join_token(token);
    let first_node = id(1);
    let second_node = id(2);
    state
        .apply(Command::PutJoinToken {
            hash,
            expires_at_ms: 2_000,
        })
        .unwrap();

    let authorize = || Command::AuthorizeJoinToken {
        hash,
        authorized_at_ms: 1_000,
    };
    let first_operation = first_node;
    assert_eq!(
        state
            .apply_operation(first_operation, 1_000, authorize())
            .unwrap(),
        CommandResult::JoinTokenAuthorized
    );
    assert_eq!(
        state
            .apply_operation(first_operation, 1_001, authorize())
            .unwrap(),
        CommandResult::JoinTokenAuthorized
    );
    assert_eq!(
        state
            .apply_operation(second_node, 1_001, authorize(),)
            .unwrap(),
        CommandResult::JoinTokenAuthorized
    );
}
use super::evaluation::{evaluation, id, state_with_target, target};
use super::*;
