use super::*;

#[test]
fn pre_tls_state_migrates_without_credentials() {
    let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("raft-state.postcard");
    let target_id = TargetId(Uuid::now_v7());
    let mut http = HttpTarget::get(Url::parse("https://example.com/health").unwrap());
    http.assertions.push(HttpAssertion::Latency { max_ms: 500 });
    let mut application = ApplicationState::default();
    application
        .apply(Command::CreateTarget {
            target: Target {
                id: target_id,
                name: "Migrated".to_owned(),
                http,
                policy: EvaluationPolicy::default(),
                notification_channels: BTreeSet::new(),
            },
            use_default_notifications: true,
        })
        .unwrap();
    let previous = PreTlsPersistedStateMachine {
        state_machine: PreTlsStateMachineData {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            application: application.into(),
        },
        current_snapshot: None,
        snapshot_idx: 0,
    };
    let mut encoded = PRE_TLS_STATE_MAGIC.to_vec();
    encoded.extend_from_slice(&postcard::to_stdvec(&previous).unwrap());
    fs::write(&path, encoded).unwrap();

    let migrated = StateMachine::open(&path).unwrap().application_state();
    let http = &migrated.targets[&target_id].target.http;
    assert_eq!(
        http.assertions,
        vec![HttpAssertion::Latency { max_ms: 500 }]
    );
    assert_eq!(http.tls_ca_secret, None);
    assert_eq!(http.tls_client_certificate_secret, None);
    assert_eq!(http.tls_client_private_key_secret, None);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn pre_assertion_state_migrates_body_contains() {
    let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
    fs::create_dir_all(&directory).unwrap();
    let path = directory.join("raft-state.postcard");
    let target_id = TargetId(Uuid::now_v7());
    let mut http = HttpTarget::get(Url::parse("https://example.com/health").unwrap());
    http.assertions.push(HttpAssertion::BodyContains {
        value: "healthy".to_owned(),
    });
    let mut application = ApplicationState::default();
    application
        .apply(Command::CreateTarget {
            target: Target {
                id: target_id,
                name: "Migrated".to_owned(),
                http,
                policy: EvaluationPolicy::default(),
                notification_channels: BTreeSet::new(),
            },
            use_default_notifications: true,
        })
        .unwrap();
    let previous = PreAssertionPersistedStateMachine {
        state_machine: PreAssertionStateMachineData {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            application: application.into(),
        },
        current_snapshot: None,
        snapshot_idx: 0,
    };
    let mut encoded = PRE_ASSERTION_STATE_MAGIC.to_vec();
    encoded.extend_from_slice(&postcard::to_stdvec(&previous).unwrap());
    fs::write(&path, encoded).unwrap();

    let migrated = StateMachine::open(&path).unwrap().application_state();
    assert_eq!(
        migrated.targets[&target_id].target.http.assertions,
        vec![HttpAssertion::BodyContains {
            value: "healthy".to_owned(),
        }]
    );
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn pre_location_state_migrates_targets_to_one_location() {
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
    let previous = PreLocationPersistedStateMachine {
        state_machine: PreLocationStateMachineData {
            last_applied_log: None,
            last_membership: StoredMembership::default(),
            application: application.into(),
        },
        current_snapshot: None,
        snapshot_idx: 0,
    };
    let mut encoded = PRE_LOCATION_STATE_MAGIC.to_vec();
    encoded.extend_from_slice(&postcard::to_stdvec(&previous).unwrap());
    fs::write(&path, encoded).unwrap();

    let migrated = StateMachine::open(&path).unwrap().application_state();
    assert_eq!(migrated.target_location_count(target_id), 1);
    fs::remove_dir_all(directory).unwrap();
}
