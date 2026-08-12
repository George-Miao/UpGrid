use std::collections::BTreeSet;
use std::fs;
use std::rc::Rc;

use openraft::alias::{CommittedLeaderIdOf, LogIdOf};
use openraft::storage::RaftStateMachine;
use openraft::{Entry, EntryPayload};
use openraft_rt_compio::futures::stream;
use url::Url;
use uuid::Uuid;

use super::core::*;
use crate::database::{RaftDatabase, StateRepository};
use crate::domain::{Command, EvaluationPolicy, HttpAssertion, HttpTarget, Target, TargetId};
use crate::raft::TC;

fn test_directory() -> std::path::PathBuf {
    let directory = std::env::temp_dir().join(format!("upgrid-test-{}", Uuid::now_v7()));
    fs::create_dir_all(&directory).unwrap();
    directory
}

#[compio::test]
async fn batches_state_machine_checkpoints() {
    let directory = test_directory();
    let database = Rc::new(RaftDatabase::open(&directory).unwrap());
    let mut state_machine = Rc::new(StateMachine::open(database.clone()).unwrap());
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
    let (checkpoint, ..) = StateRepository::new(database.clone()).load().unwrap();
    assert_eq!(None, checkpoint.last_applied_log);

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

    let (checkpoint, ..) = StateRepository::new(database.clone()).load().unwrap();
    assert_eq!(
        Some(LogIdOf::<TC>::new(leader, CHECKPOINT_INTERVAL)),
        checkpoint.last_applied_log
    );
    drop(state_machine);
    drop(database);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn application_state_with_assertions_survives_reopen() {
    let directory = test_directory();
    let target_id = TargetId(Uuid::now_v7());
    let mut http = HttpTarget::get(Url::parse("https://example.com/health").unwrap());
    http.assertions.push(HttpAssertion::BodyContains {
        value: "healthy".to_owned(),
    });
    let database = Rc::new(RaftDatabase::open(&directory).unwrap());
    let state_machine = StateMachine::open(database.clone()).unwrap();
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
    drop(database);

    let database = Rc::new(RaftDatabase::open(&directory).unwrap());
    let reopened = StateMachine::open(database.clone()).unwrap();
    assert_eq!(
        reopened.application_state().targets[&target_id]
            .target
            .http
            .assertions,
        vec![HttpAssertion::BodyContains {
            value: "healthy".to_owned(),
        }]
    );
    drop(reopened);
    drop(database);
    fs::remove_dir_all(directory).unwrap();
}
