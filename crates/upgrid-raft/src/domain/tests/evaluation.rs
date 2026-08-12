use super::*;

pub(super) fn id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

pub(super) fn target(target_id: TargetId, channel_id: NotificationChannelId) -> Target {
    Target {
        id: target_id,
        name: "Example".to_owned(),
        http: HttpTarget::get(Url::parse("https://example.com/health").unwrap()),
        policy: EvaluationPolicy::default(),
        notification_channels: BTreeSet::from([channel_id]),
    }
}

pub(super) fn evaluation(target_id: TargetId, scheduled_at_ms: u64, succeeded: bool) -> Evaluation {
    Evaluation {
        id: EvaluationId {
            target_id,
            scheduled_at_ms,
        },
        recorded_at_ms: scheduled_at_ms + 50,
        executor_node_id: id(99),
        succeeded,
        http: HttpEvaluationMetadata {
            status_code: succeeded.then_some(200),
            latency_ms: 50,
            received_bytes: 2,
            final_url: Url::parse("https://example.com/health").unwrap(),
        },
        diagnostic: (!succeeded).then(|| "connection refused".to_owned()),
    }
}

pub(super) fn state_with_target() -> (ApplicationState, TargetId, NotificationChannelId) {
    let mut state = ApplicationState::default();
    let secret_id = SecretId(id(1));
    let channel_id = NotificationChannelId(id(2));
    let target_id = TargetId(id(3));
    state
        .apply(Command::CreateNotificationChannel {
            channel: NotificationChannel {
                id: channel_id,
                name: "Operations".to_owned(),
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
        .apply(Command::CreateTarget {
            target: target(target_id, channel_id),
            use_default_notifications: true,
        })
        .unwrap();
    (state, target_id, channel_id)
}

#[test]
fn availability_transitions_create_one_alert_per_channel() {
    let (mut state, target_id, channel_id) = state_with_target();

    for scheduled_at_ms in [1_000, 2_000] {
        let result = state
            .apply(Command::RecordEvaluation(evaluation(
                target_id,
                scheduled_at_ms,
                false,
            )))
            .unwrap();
        assert_eq!(
            result,
            CommandResult::EvaluationAccepted {
                availability: AvailabilityState::Unknown,
                alerts: vec![],
            }
        );
    }

    let result = state
        .apply(Command::RecordEvaluation(evaluation(
            target_id, 3_000, false,
        )))
        .unwrap();
    let down_alert = AlertId {
        target_id,
        channel_id,
        evaluation_scheduled_at_ms: 3_000,
        kind: AlertKind::Down,
    };
    assert_eq!(
        result,
        CommandResult::EvaluationAccepted {
            availability: AvailabilityState::Down,
            alerts: vec![down_alert],
        }
    );

    let result = state
        .apply(Command::RecordEvaluation(evaluation(
            target_id, 4_000, true,
        )))
        .unwrap();
    let recovered_alert = AlertId {
        target_id,
        channel_id,
        evaluation_scheduled_at_ms: 4_000,
        kind: AlertKind::Recovered,
    };
    assert_eq!(
        result,
        CommandResult::EvaluationAccepted {
            availability: AvailabilityState::Up,
            alerts: vec![recovered_alert],
        }
    );
    assert_eq!(state.alerts.len(), 2);
    assert_eq!(state.transitions.len(), 2);
}

#[test]
fn availability_transition_is_recorded_without_a_notification_channel() {
    let (mut state, target_id, _) = state_with_target();
    state
        .targets
        .get_mut(&target_id)
        .unwrap()
        .target
        .notification_channels
        .clear();

    for scheduled_at_ms in [1_000, 2_000, 3_000] {
        state
            .apply(Command::RecordEvaluation(evaluation(
                target_id,
                scheduled_at_ms,
                false,
            )))
            .unwrap();
    }

    assert!(state.alerts.is_empty());
    assert_eq!(state.transitions.len(), 1);
    let transition = &state.transitions[&EvaluationId {
        target_id,
        scheduled_at_ms: 3_000,
    }];
    assert_eq!(transition.kind, AlertKind::Down);
}

#[test]
fn default_channels_apply_unless_target_opts_out() {
    let (mut state, target_id, channel_id) = state_with_target();
    state
        .targets
        .get_mut(&target_id)
        .unwrap()
        .target
        .notification_channels
        .clear();
    state
        .apply(Command::SetNotificationChannelDefault {
            channel_id,
            is_default: true,
        })
        .unwrap();

    for scheduled_at_ms in [1_000, 2_000, 3_000] {
        state
            .apply(Command::RecordEvaluation(evaluation(
                target_id,
                scheduled_at_ms,
                false,
            )))
            .unwrap();
    }
    assert_eq!(state.alerts.len(), 1);
    assert_eq!(
        state.alerts.values().next().unwrap().id.channel_id,
        channel_id
    );

    let (mut opted_out, target_id, channel_id) = state_with_target();
    opted_out
        .targets
        .get_mut(&target_id)
        .unwrap()
        .target
        .notification_channels
        .clear();
    opted_out
        .apply(Command::SetNotificationChannelDefault {
            channel_id,
            is_default: true,
        })
        .unwrap();
    let target = opted_out.targets[&target_id].target.clone();
    opted_out
        .apply(Command::UpdateTarget {
            target,
            use_default_notifications: false,
        })
        .unwrap();
    for scheduled_at_ms in [1_000, 2_000, 3_000] {
        opted_out
            .apply(Command::RecordEvaluation(evaluation(
                target_id,
                scheduled_at_ms,
                false,
            )))
            .unwrap();
    }
    assert!(opted_out.alerts.is_empty());
    assert_eq!(opted_out.transitions.len(), 1);
}

#[test]
fn explicit_default_channel_is_not_duplicated() {
    let (mut state, target_id, channel_id) = state_with_target();
    state
        .apply(Command::SetNotificationChannelDefault {
            channel_id,
            is_default: true,
        })
        .unwrap();

    for scheduled_at_ms in [1_000, 2_000, 3_000] {
        state
            .apply(Command::RecordEvaluation(evaluation(
                target_id,
                scheduled_at_ms,
                false,
            )))
            .unwrap();
    }
    assert_eq!(state.alerts.len(), 1);
}

#[test]
fn duplicate_and_deleted_target_results_are_discarded() {
    let (mut state, target_id, _) = state_with_target();
    let item = evaluation(target_id, 1_000, true);
    state
        .apply(Command::RecordEvaluation(item.clone()))
        .unwrap();

    assert_eq!(
        state.apply(Command::RecordEvaluation(item)).unwrap(),
        CommandResult::EvaluationDiscarded
    );
    assert_eq!(state.targets[&target_id].history.len(), 1);

    state.apply(Command::DeleteTarget(target_id)).unwrap();
    assert_eq!(
        state
            .apply(Command::RecordEvaluation(evaluation(
                target_id, 2_000, false,
            )))
            .unwrap(),
        CommandResult::EvaluationDiscarded
    );
}

#[test]
fn evaluation_history_is_pruned_by_recorded_time() {
    let (mut state, target_id, _) = state_with_target();
    state.history_retention_ms = 1_000;
    state
        .apply(Command::RecordEvaluation(evaluation(
            target_id, 1_000, true,
        )))
        .unwrap();
    state
        .apply(Command::RecordEvaluation(evaluation(
            target_id, 3_000, true,
        )))
        .unwrap();

    let history = &state.targets[&target_id].history;
    assert_eq!(history.len(), 1);
    assert!(history.contains_key(&3_000));
}

#[test]
fn targets_cannot_reference_missing_channels_or_secrets() {
    let mut state = ApplicationState::default();
    let channel_id = NotificationChannelId(id(1));
    let target_id = TargetId(id(2));
    assert_eq!(
        state
            .apply(Command::CreateTarget {
                target: target(target_id, channel_id),
                use_default_notifications: false,
            })
            .unwrap_err(),
        DomainError::NotificationChannelNotFound(channel_id)
    );
    assert!(!state.default_notifications_disabled.contains(&target_id));

    let secret_id = SecretId(id(3));
    let channel = NotificationChannel {
        id: channel_id,
        name: "Webhook".to_owned(),
        kind: NotificationChannelKind::Webhook {
            url: Url::parse("https://example.com/hook").unwrap(),
            headers: BTreeMap::from([("Authorization".to_owned(), ConfigValue::Secret(secret_id))]),
        },
    };
    assert_eq!(
        state
            .apply(Command::CreateNotificationChannel {
                channel,
                generated_secret: None,
                is_default: true,
            })
            .unwrap_err(),
        DomainError::SecretNotFound(secret_id)
    );
    assert!(!state.default_notification_channels.contains(&channel_id));
}

#[test]
fn referenced_channels_and_secrets_cannot_be_deleted() {
    let (mut state, target_id, channel_id) = state_with_target();
    let secret_id = SecretId(id(1));

    assert!(
        state
            .apply(Command::DeleteNotificationChannel(channel_id))
            .is_err()
    );
    assert!(state.apply(Command::DeleteSecret(secret_id)).is_err());

    state.apply(Command::DeleteTarget(target_id)).unwrap();
    state
        .apply(Command::DeleteNotificationChannel(channel_id))
        .unwrap();
    state.apply(Command::DeleteSecret(secret_id)).unwrap();
    assert!(state.notification_channels.is_empty());
    assert!(state.secrets.is_empty());
}

#[test]
fn operations_are_deduplicated_without_client_keys() {
    let (mut state, target_id, _) = state_with_target();
    let operation_id = id(500);
    let command = Command::RecordEvaluation(evaluation(target_id, 1_000, false));

    let first = state
        .apply_operation(operation_id, 1_050, command.clone())
        .unwrap();
    let repeated = state.apply_operation(operation_id, 1_050, command).unwrap();

    assert_eq!(first, repeated);
    assert_eq!(state.targets[&target_id].consecutive_failures, 1);
    let reused = state
        .apply_operation(
            operation_id,
            2_050,
            Command::RecordEvaluation(evaluation(target_id, 2_000, false)),
        )
        .unwrap();
    assert_eq!(first, reused);
    assert_eq!(state.targets[&target_id].consecutive_failures, 1);
}

#[test]
fn assignment_variants_preserve_existing_postcard_discriminants() {
    #[allow(dead_code)]
    #[derive(Serialize)]
    enum LegacyCommand {
        Secret(Secret),
        NotificationChannel(NotificationChannel),
        TargetCreate(Target),
        TargetUpdate(Target),
        DeleteTarget(TargetId),
    }

    let target_id = TargetId(id(77));
    let encoded = postcard::to_stdvec(&LegacyCommand::DeleteTarget(target_id)).unwrap();
    let decoded = postcard::from_bytes::<Command>(&encoded).unwrap();

    assert_eq!(decoded, Command::DeleteTarget(target_id));
}

#[test]
fn out_of_order_results_do_not_roll_state_back() {
    let (mut state, target_id, _) = state_with_target();
    state
        .apply(Command::RecordEvaluation(evaluation(
            target_id, 2_000, true,
        )))
        .unwrap();

    assert_eq!(
        state
            .apply(Command::RecordEvaluation(evaluation(
                target_id, 1_000, false,
            )))
            .unwrap(),
        CommandResult::EvaluationDiscarded
    );
    assert_eq!(
        state.targets[&target_id].availability,
        AvailabilityState::Up
    );
}

#[test]
fn committed_result_completes_the_replicated_assignment() {
    let (mut state, target_id, _) = state_with_target();
    let evaluation_id = EvaluationId {
        target_id,
        scheduled_at_ms: 1_000,
    };
    let assignment = EvaluationAssignment {
        id: evaluation_id,
        executor_node_id: id(99),
        assigned_at_ms: 900,
        expires_at_ms: 2_000,
        attempt: 1,
    };

    assert_eq!(
        state.apply(Command::AssignEvaluation(assignment)).unwrap(),
        CommandResult::EvaluationAssigned(evaluation_id)
    );
    assert!(state.has_evaluation_assignment(evaluation_id));

    state
        .apply(Command::RecordEvaluation(evaluation(
            target_id, 1_000, true,
        )))
        .unwrap();
    assert!(!state.has_evaluation_assignment(evaluation_id));
}
