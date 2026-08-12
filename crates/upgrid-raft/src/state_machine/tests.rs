use super::core::*;
use super::decode::*;
use super::version::*;

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;
    use std::rc::Rc;

    use openraft::alias::{CommittedLeaderIdOf, LogIdOf};
    use openraft::storage::RaftStateMachine;
    use openraft::{Entry, EntryPayload, StoredMembership};
    use openraft_rt_compio::futures::stream;
    use url::Url;
    use uuid::Uuid;

    use super::*;
    use crate::domain::{
        AlertKind, ApplicationState, AvailabilityTransition, Command,
        DEFAULT_HISTORY_ROLLUP_RETENTION_MS, DEFAULT_TARGET_TRASH_RETENTION_MS, Evaluation,
        EvaluationId, EvaluationPolicy, HttpAssertion, HttpEvaluationMetadata, HttpTarget,
        NotificationChannelId, Target, TargetId,
    };
    use crate::raft::TC;

    mod migration;

    #[compio::test]
    async fn batches_state_machine_checkpoints() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-state.postcard");
        let mut state_machine = Rc::new(StateMachine::open(&path).unwrap());
        let leader = CommittedLeaderIdOf::<TC> {
            term: 1,
            node_id: Uuid::now_v7(),
        };
        let entries = (1..CHECKPOINT_INTERVAL).map(|index| Entry {
            log_id: LogIdOf::<TC>::new(leader, index),
            payload: EntryPayload::Blank,
        });
        let entries = stream::iter(entries.map(|entry| Ok((entry, None))));

        RaftStateMachine::apply(&mut state_machine, entries)
            .await
            .unwrap();
        assert!(!path.exists());

        RaftStateMachine::apply(
            &mut state_machine,
            stream::iter([Ok((
                Entry {
                    log_id: LogIdOf::<TC>::new(leader, CHECKPOINT_INTERVAL),
                    payload: EntryPayload::Blank,
                },
                None,
            ))]),
        )
        .await
        .unwrap();

        let reopened = StateMachine::open(&path).unwrap();
        assert_eq!(reopened.applied_index(), Some(CHECKPOINT_INTERVAL));
        drop(reopened);
        drop(state_machine);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn application_state_with_assertions_survives_reopen() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-state.postcard");
        let target_id = TargetId(Uuid::now_v7());
        let mut http = HttpTarget::get(Url::parse("https://example.com/health").unwrap());
        http.assertions.push(HttpAssertion::BodyContains {
            value: "healthy".to_owned(),
        });
        let state_machine = StateMachine::open(&path).unwrap();
        state_machine
            .state_machine
            .borrow_mut()
            .application
            .apply(Command::CreateTarget {
                target: Target {
                    id: target_id,
                    name: "Example".to_owned(),
                    http,
                    policy: EvaluationPolicy::default(),
                    notification_channels: BTreeSet::new(),
                },
                use_default_notifications: true,
            })
            .unwrap();
        state_machine.persist().unwrap();
        drop(state_machine);

        let reopened = StateMachine::open(&path).unwrap();
        assert_eq!(
            reopened.application_state().targets[&target_id]
                .target
                .http
                .assertions,
            vec![HttpAssertion::BodyContains {
                value: "healthy".to_owned(),
            }]
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn legacy_state_is_migrated_to_the_versioned_format() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-state.postcard");
        let target_id = TargetId(Uuid::now_v7());
        let mut application = ApplicationState::default();
        application
            .apply(Command::CreateTarget {
                target: Target {
                    id: target_id,
                    name: "Migrated".to_owned(),
                    http: HttpTarget::get(Url::parse("https://example.com/health").unwrap()),
                    policy: EvaluationPolicy::default(),
                    notification_channels: BTreeSet::new(),
                },
                use_default_notifications: true,
            })
            .unwrap();
        let legacy = LegacyPersistedStateMachine {
            state_machine: LegacyStateMachineData {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                application: application.into(),
            },
            current_snapshot: None,
            snapshot_idx: 0,
        };
        fs::write(&path, postcard::to_stdvec(&legacy).unwrap()).unwrap();

        let state_machine = StateMachine::open(&path).unwrap();
        assert!(
            state_machine
                .application_state()
                .targets
                .contains_key(&target_id)
        );
        state_machine.persist().unwrap();
        assert!(fs::read(&path).unwrap().starts_with(STATE_MAGIC));

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn previous_version_is_migrated_without_losing_application_state() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-state.postcard");
        let target_id = TargetId(Uuid::now_v7());
        let mut application = ApplicationState::default();
        application
            .apply(Command::CreateTarget {
                target: Target {
                    id: target_id,
                    name: "Previous".to_owned(),
                    http: HttpTarget::get(Url::parse("https://example.com/health").unwrap()),
                    policy: EvaluationPolicy::default(),
                    notification_channels: BTreeSet::new(),
                },
                use_default_notifications: true,
            })
            .unwrap();
        let previous = PreviousPersistedStateMachine {
            state_machine: PreviousStateMachineData {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                application: application.into(),
            },
            current_snapshot: None,
            snapshot_idx: 0,
        };
        let mut encoded = PREVIOUS_STATE_MAGIC.to_vec();
        encoded.extend_from_slice(&postcard::to_stdvec(&previous).unwrap());
        fs::write(&path, encoded).unwrap();

        let state_machine = StateMachine::open(&path).unwrap();
        assert!(
            state_machine
                .application_state()
                .targets
                .contains_key(&target_id)
        );
        assert!(state_machine.application_state().join_tokens.is_empty());

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn token_version_is_migrated_without_losing_join_tokens() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-state.postcard");
        let hash = crate::domain::JoinTokenHash([9; 32]);
        let mut application = ApplicationState::default();
        application
            .apply(Command::PutJoinToken {
                hash,
                expires_at_ms: 2_000,
            })
            .unwrap();
        let previous = TokenPersistedStateMachine {
            state_machine: TokenStateMachineData {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                application: application.into(),
            },
            current_snapshot: None,
            snapshot_idx: 0,
        };
        let mut encoded = TOKEN_STATE_MAGIC.to_vec();
        encoded.extend_from_slice(&postcard::to_stdvec(&previous).unwrap());
        fs::write(&path, encoded).unwrap();

        let state_machine = StateMachine::open(&path).unwrap();
        assert_eq!(
            state_machine.application_state().join_tokens.get(&hash),
            Some(&2_000)
        );
        assert!(state_machine.application_state().join_token_uses.is_empty());
        assert!(state_machine.application_state().node_names.is_empty());

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn named_version_is_migrated_without_losing_node_names() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-state.postcard");
        let node_id = Uuid::now_v7();
        let mut application = ApplicationState::default();
        application
            .apply(Command::SetNodeName {
                node_id,
                name: "green-anchor".to_owned(),
            })
            .unwrap();
        let previous = NamedPersistedStateMachine {
            state_machine: NamedStateMachineData {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                application: application.into(),
            },
            current_snapshot: None,
            snapshot_idx: 0,
        };
        let mut encoded = NAMED_STATE_MAGIC.to_vec();
        encoded.extend_from_slice(&postcard::to_stdvec(&previous).unwrap());
        fs::write(&path, encoded).unwrap();

        let state_machine = StateMachine::open(&path).unwrap();
        assert_eq!(
            state_machine.application_state().node_names.get(&node_id),
            Some(&"green-anchor".to_owned())
        );
        assert!(state_machine.application_state().transitions.is_empty());

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn named_snapshot_is_migrated_without_losing_node_names() {
        let node_id = Uuid::now_v7();
        let mut application = ApplicationState::default();
        application
            .apply(Command::SetNodeName {
                node_id,
                name: "swift-falcon".to_owned(),
            })
            .unwrap();
        let previous: crate::domain::NamedApplicationState = application.into();
        let mut encoded = NAMED_SNAPSHOT_MAGIC.to_vec();
        encoded.extend_from_slice(&postcard::to_stdvec(&previous).unwrap());

        let migrated = decode_application(&encoded).unwrap();
        assert_eq!(
            migrated.node_names.get(&node_id),
            Some(&"swift-falcon".to_owned())
        );
        assert!(migrated.transitions.is_empty());
    }

    #[test]
    fn transition_version_is_migrated_without_losing_transitions() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-state.postcard");
        let target_id = TargetId(Uuid::now_v7());
        let evaluation = Evaluation {
            id: EvaluationId {
                target_id,
                scheduled_at_ms: 1_000,
            },
            recorded_at_ms: 1_100,
            executor_node_id: Uuid::now_v7(),
            succeeded: false,
            http: HttpEvaluationMetadata {
                status_code: Some(502),
                latency_ms: 10,
                received_bytes: 0,
                final_url: Url::parse("https://example.com").unwrap(),
            },
            diagnostic: Some("bad gateway".to_owned()),
        };
        let mut application = ApplicationState::default();
        application.transitions.insert(
            evaluation.id,
            AvailabilityTransition {
                kind: AlertKind::Down,
                target_name: "Example".to_owned(),
                target_url: Url::parse("https://example.com").unwrap(),
                evaluation: evaluation.clone(),
            },
        );
        let previous = TransitionPersistedStateMachine {
            state_machine: TransitionStateMachineData {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                application: application.into(),
            },
            current_snapshot: None,
            snapshot_idx: 0,
        };
        let mut encoded = TRANSITION_STATE_MAGIC.to_vec();
        encoded.extend_from_slice(&postcard::to_stdvec(&previous).unwrap());
        fs::write(&path, encoded).unwrap();

        let migrated = StateMachine::open(&path).unwrap().application_state();
        assert_eq!(migrated.transitions[&evaluation.id].evaluation, evaluation);
        assert!(migrated.default_notification_channels.is_empty());
        assert!(migrated.default_notifications_disabled.is_empty());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn default_channel_version_is_migrated_without_losing_defaults() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-state.postcard");
        let channel_id = NotificationChannelId(Uuid::now_v7());
        let mut application = ApplicationState::default();
        application.default_notification_channels.insert(channel_id);
        let previous = DefaultChannelPersistedStateMachine {
            state_machine: DefaultChannelStateMachineData {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                application: application.into(),
            },
            current_snapshot: None,
            snapshot_idx: 0,
        };
        let mut encoded = DEFAULT_CHANNEL_STATE_MAGIC.to_vec();
        encoded.extend_from_slice(&postcard::to_stdvec(&previous).unwrap());
        fs::write(&path, encoded).unwrap();

        let migrated = StateMachine::open(&path).unwrap().application_state();
        assert_eq!(
            migrated.default_notification_channels,
            BTreeSet::from([channel_id])
        );
        assert!(migrated.node_targets.is_empty());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn pre_acknowledgement_state_is_migrated_without_losing_cluster_data() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-state.postcard");
        let node_id = Uuid::now_v7();
        let mut application = ApplicationState::default();
        application
            .node_names
            .insert(node_id, "migrated-node".to_owned());
        application.draining_nodes.insert(node_id);
        let previous = PreAcknowledgementPersistedStateMachine {
            state_machine: PreAcknowledgementStateMachineData {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                application: application.into(),
            },
            current_snapshot: None,
            snapshot_idx: 0,
        };
        let mut encoded = PRE_ACKNOWLEDGEMENT_STATE_MAGIC.to_vec();
        encoded.extend_from_slice(&postcard::to_stdvec(&previous).unwrap());
        fs::write(&path, encoded).unwrap();

        let migrated = StateMachine::open(&path).unwrap().application_state();
        assert_eq!(migrated.node_names[&node_id], "migrated-node");
        assert_eq!(migrated.draining_nodes, BTreeSet::from([node_id]));
        assert!(migrated.alert_acknowledgements.is_empty());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn pre_drain_state_is_migrated_with_nodes_eligible() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-state.postcard");
        let node_id = Uuid::now_v7();
        let mut application = ApplicationState::default();
        application
            .node_names
            .insert(node_id, "migrated-node".to_owned());
        let previous = PreDrainPersistedStateMachine {
            state_machine: PreDrainStateMachineData {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                application: application.into(),
            },
            current_snapshot: None,
            snapshot_idx: 0,
        };
        let mut encoded = PRE_DRAIN_STATE_MAGIC.to_vec();
        encoded.extend_from_slice(&postcard::to_stdvec(&previous).unwrap());
        fs::write(&path, encoded).unwrap();

        let migrated = StateMachine::open(&path).unwrap().application_state();
        assert_eq!(migrated.node_names[&node_id], "migrated-node");
        assert!(migrated.draining_nodes.is_empty());
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn pre_authentication_state_is_migrated_without_losing_cluster_data() {
        let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("raft-state.postcard");
        let node_id = Uuid::now_v7();
        let mut application = ApplicationState::default();
        application
            .node_names
            .insert(node_id, "migrated-node".to_owned());
        let previous = PreAuthPersistedStateMachine {
            state_machine: PreAuthStateMachineData {
                last_applied_log: None,
                last_membership: StoredMembership::default(),
                application: application.into(),
            },
            current_snapshot: None,
            snapshot_idx: 0,
        };
        let mut encoded = PRE_AUTH_STATE_MAGIC.to_vec();
        encoded.extend_from_slice(&postcard::to_stdvec(&previous).unwrap());
        fs::write(&path, encoded).unwrap();

        let migrated = StateMachine::open(&path).unwrap().application_state();
        assert_eq!(migrated.node_names[&node_id], "migrated-node");
        assert!(migrated.identities.is_empty());
        assert!(migrated.api_tokens.is_empty());
        fs::remove_dir_all(directory).unwrap();
    }
}
