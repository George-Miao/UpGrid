use super::evaluation::{evaluation, id, state_with_target};
use super::*;

#[test]
fn trash_and_restore_preserve_target_state_and_release_assignments() {
    let (mut state, target_id, channel_id) = state_with_target();
    let target = state.targets[&target_id].target.clone();
    state
        .apply(Command::UpdateTargetWithLocations {
            target,
            use_default_notifications: false,
            locations: 3,
        })
        .unwrap();
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
            executor_node_id: id(90),
            assigned_at_ms: 1_900,
            expires_at_ms: 3_000,
            attempt: 1,
        }))
        .unwrap();

    assert_eq!(
        state
            .apply(Command::TrashTarget {
                target_id,
                deleted_at_ms: 2_000,
            })
            .unwrap(),
        CommandResult::TargetTrashed(target_id)
    );
    assert!(!state.targets.contains_key(&target_id));
    assert!(!state.has_evaluation_assignment(evaluation_id));
    assert_eq!(state.trashed_targets[&target_id].state.history.len(), 1);
    assert_eq!(state.trashed_targets[&target_id].locations, 3);
    assert!(!state.trashed_targets[&target_id].use_default_notifications);
    assert_eq!(state.trashed_targets[&target_id].history_rollups.len(), 1);
    assert!(
        state
            .apply(Command::DeleteNotificationChannel(channel_id))
            .is_err()
    );

    assert_eq!(
        state
            .apply(Command::RestoreTarget {
                target_id,
                restored_at_ms: 2_500,
            })
            .unwrap(),
        CommandResult::TargetRestored(target_id)
    );
    assert_eq!(state.targets[&target_id].history.len(), 1);
    assert_eq!(state.target_location_count(target_id), 3);
    assert!(state.default_notifications_disabled.contains(&target_id));
    assert_eq!(state.history_rollups[&target_id].len(), 1);
}

#[test]
fn expired_trash_is_pruned_before_restore() {
    let (mut state, target_id, _) = state_with_target();
    state
        .apply(Command::SetTargetTrashRetention {
            retention_ms: 100,
            now_ms: 0,
        })
        .unwrap();
    state
        .apply(Command::TrashTarget {
            target_id,
            deleted_at_ms: 1_000,
        })
        .unwrap();

    assert!(
        state
            .apply(Command::RestoreTarget {
                target_id,
                restored_at_ms: 1_100,
            })
            .is_err()
    );
    assert!(!state.trashed_targets.contains_key(&target_id));
}

#[test]
fn purged_targets_cannot_be_restored() {
    let (mut state, target_id, _) = state_with_target();
    state
        .apply(Command::TrashTarget {
            target_id,
            deleted_at_ms: 1_000,
        })
        .unwrap();
    assert_eq!(
        state.apply(Command::PurgeTarget(target_id)).unwrap(),
        CommandResult::TargetPurged(target_id)
    );
    assert!(
        state
            .apply(Command::RestoreTarget {
                target_id,
                restored_at_ms: 1_001,
            })
            .is_err()
    );
}
