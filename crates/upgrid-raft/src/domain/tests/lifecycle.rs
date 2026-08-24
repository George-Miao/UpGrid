#[test]
fn assignment_batch_applies_every_valid_assignment() {
    let (mut state, first_target_id, channel_id) = state_with_target();
    let second_target_id = TargetId(id(20));
    state
        .apply(Command::CreateTarget {
            target: target(second_target_id, channel_id),
            use_default_notifications: true,
        })
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
            .all(|assignment| state.has_evaluation_assignment(assignment.id))
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

    state
        .apply(Command::UpdateTarget {
            target: updated,
            use_default_notifications: true,
        })
        .unwrap();

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
    assert!(!state.has_evaluation_assignment(evaluation_id));

    state
        .apply(Command::SetTargetPaused {
            target_id,
            paused: false,
        })
        .unwrap();
    assert!(!state.targets[&target_id].paused);
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
fn node_names_are_trimmed_and_validated() {
    let mut state = ApplicationState::default();
    let node_id = id(3);
    assert_eq!(
        state
            .apply(Command::SetNodeName {
                node_id,
                name: "  edge-shanghai  ".to_owned(),
            })
            .unwrap(),
        CommandResult::NodeNameSet(node_id)
    );
    assert_eq!(state.node_names.get(&node_id).unwrap(), "edge-shanghai");
    assert!(
        state
            .apply(Command::SetNodeName {
                node_id,
                name: "\n".to_owned(),
            })
            .is_err()
    );
}

#[test]
fn forced_node_drain_releases_assignments_and_can_be_cancelled() {
    let (mut state, target_id, _) = state_with_target();
    let node_id = id(10);
    let evaluation_id = EvaluationId {
        target_id,
        scheduled_at_ms: 2_000,
    };
    state
        .apply(Command::AssignEvaluation(EvaluationAssignment {
            id: evaluation_id,
            executor_node_id: node_id,
            assigned_at_ms: 1_900,
            expires_at_ms: 3_000,
            attempt: 1,
        }))
        .unwrap();

    assert_eq!(
        state
            .apply(Command::SetNodeDraining {
                node_id,
                draining: true,
                force: true,
            })
            .unwrap(),
        CommandResult::NodeDrainSet {
            node_id,
            draining: true,
        }
    );
    assert!(state.draining_nodes.contains(&node_id));
    assert!(!state.has_evaluation_assignment(evaluation_id));
    let stale_id = EvaluationId {
        target_id,
        scheduled_at_ms: 3_000,
    };
    assert_eq!(
        state
            .apply(Command::AssignEvaluation(EvaluationAssignment {
                id: stale_id,
                executor_node_id: node_id,
                assigned_at_ms: 2_900,
                expires_at_ms: 4_000,
                attempt: 1,
            }))
            .unwrap(),
        CommandResult::EvaluationDiscarded
    );
    assert!(!state.has_evaluation_assignment(stale_id));

    state
        .apply(Command::SetNodeDraining {
            node_id,
            draining: false,
            force: false,
        })
        .unwrap();
    assert!(!state.draining_nodes.contains(&node_id));
}

#[test]
fn invalid_channel_does_not_store_generated_secret() {
    let mut state = ApplicationState::default();
    let secret_id = SecretId(id(10));
    let channel_id = NotificationChannelId(id(11));

    assert!(matches!(
        state.apply(Command::CreateNotificationChannel {
            channel: NotificationChannel {
                id: channel_id,
                name: String::new(),
                kind: NotificationChannelKind::Telegram {
                    bot_token: secret_id,
                    chat_id: "1234".to_owned(),
                },
            },
            generated_secret: Some(Secret {
                id: secret_id,
                name: "telegram-token".to_owned(),
                ciphertext: vec![1, 2, 3],
            }),
            is_default: true,
        }),
        Err(DomainError::InvalidNotificationChannel(_))
    ));
    assert!(!state.secrets.contains_key(&secret_id));
    assert!(!state.notification_channels.contains_key(&channel_id));
    assert!(!state.default_notification_channels.contains(&channel_id));
}

#[test]
fn channel_update_preserves_or_replaces_existing_secret() {
    let mut state = ApplicationState::default();
    let secret_id = SecretId(id(10));
    let channel_id = NotificationChannelId(id(11));
    state
        .apply(Command::CreateNotificationChannel {
            channel: NotificationChannel {
                id: channel_id,
                name: "Primary".to_owned(),
                kind: NotificationChannelKind::Telegram {
                    bot_token: secret_id,
                    chat_id: "1234".to_owned(),
                },
            },
            generated_secret: Some(Secret {
                id: secret_id,
                name: "telegram-token".to_owned(),
                ciphertext: vec![1, 2, 3],
            }),
            is_default: false,
        })
        .unwrap();

    state
        .apply(Command::UpdateNotificationChannel {
            channel: NotificationChannel {
                id: channel_id,
                name: "Renamed".to_owned(),
                kind: NotificationChannelKind::Telegram {
                    bot_token: secret_id,
                    chat_id: "1234".to_owned(),
                },
            },
            generated_secret: None,
            is_default: false,
        })
        .unwrap();
    assert_eq!(state.secrets[&secret_id].ciphertext, vec![1, 2, 3]);

    assert_eq!(
        state
            .apply(Command::UpdateNotificationChannel {
                channel: NotificationChannel {
                    id: channel_id,
                    name: "Escalations".to_owned(),
                    kind: NotificationChannelKind::Telegram {
                        bot_token: secret_id,
                        chat_id: "5678".to_owned(),
                    },
                },
                generated_secret: Some(Secret {
                    id: secret_id,
                    name: "telegram-token".to_owned(),
                    ciphertext: vec![4, 5, 6],
                }),
                is_default: true,
            })
            .unwrap(),
        CommandResult::NotificationChannelUpdated(channel_id)
    );
    assert_eq!(state.notification_channels[&channel_id].name, "Escalations");
    assert_eq!(state.secrets[&secret_id].ciphertext, vec![4, 5, 6]);
    assert!(state.default_notification_channels.contains(&channel_id));
}

#[test]
fn channel_update_rejects_missing_channel_without_storing_secret() {
    let mut state = ApplicationState::default();
    let secret_id = SecretId(id(10));
    let channel_id = NotificationChannelId(id(11));

    assert_eq!(
        state
            .apply(Command::UpdateNotificationChannel {
                channel: NotificationChannel {
                    id: channel_id,
                    name: "Escalations".to_owned(),
                    kind: NotificationChannelKind::Telegram {
                        bot_token: secret_id,
                        chat_id: "5678".to_owned(),
                    },
                },
                generated_secret: Some(Secret {
                    id: secret_id,
                    name: "telegram-token".to_owned(),
                    ciphertext: vec![4, 5, 6],
                }),
                is_default: true,
            })
            .unwrap_err(),
        DomainError::NotificationChannelNotFound(channel_id)
    );
    assert!(!state.secrets.contains_key(&secret_id));
}

#[test]
fn target_create_and_update_apply_default_policy_atomically() {
    let (mut state, _, channel_id) = state_with_target();
    let target_id = TargetId(id(12));
    state
        .apply(Command::CreateTarget {
            target: target(target_id, channel_id),
            use_default_notifications: false,
        })
        .unwrap();
    assert!(state.targets.contains_key(&target_id));
    assert!(state.default_notifications_disabled.contains(&target_id));

    let mut invalid = state.targets[&target_id].target.clone();
    invalid.name = "Rejected".to_owned();
    invalid
        .notification_channels
        .insert(NotificationChannelId(id(13)));
    assert_eq!(
        state
            .apply(Command::UpdateTarget {
                target: invalid,
                use_default_notifications: true,
            })
            .unwrap_err(),
        DomainError::NotificationChannelNotFound(NotificationChannelId(id(13)))
    );
    assert_eq!(state.targets[&target_id].target.name, "Example");
    assert!(state.default_notifications_disabled.contains(&target_id));

    let mut updated = state.targets[&target_id].target.clone();
    updated.name = "Updated".to_owned();
    state
        .apply(Command::UpdateTarget {
            target: updated,
            use_default_notifications: true,
        })
        .unwrap();
    assert_eq!(state.targets[&target_id].target.name, "Updated");
    assert!(!state.default_notifications_disabled.contains(&target_id));
}

use super::evaluation::{evaluation, id, state_with_target, target};
use super::*;
