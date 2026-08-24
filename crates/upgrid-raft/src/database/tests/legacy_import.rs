use std::collections::BTreeMap;
use std::fs;

use openraft::alias::{EntryOf, LogIdOf, SnapshotMetaOf, StoredMembershipOf, VoteOf};
use redb::{Database, TableDefinition};
use serde::Serialize;

use super::{entry, leader, open_database, test_directory};
use crate::database::{LogRepository, StateRepository};
use crate::domain::{ApplicationState, ApplicationStateV20260812};
use crate::raft::TC;

#[derive(Serialize)]
struct LegacyStoreMetaFixture {
    last_purged_log_id: Option<LogIdOf<TC>>,
    committed: Option<LogIdOf<TC>>,
    vote: Option<VoteOf<TC>>,
}

#[derive(Serialize)]
struct LegacyLogFixture {
    last_purged_log_id: Option<LogIdOf<TC>>,
    log: BTreeMap<u64, EntryOf<TC>>,
    committed: Option<LogIdOf<TC>>,
    vote: Option<VoteOf<TC>>,
}

#[derive(Serialize)]
struct LegacyStateFixture {
    state_machine: LegacyStateDataFixture,
    current_snapshot: Option<LegacySnapshotFixture>,
    snapshot_idx: u64,
}

#[derive(Serialize)]
struct LegacyStateDataFixture {
    last_applied_log: Option<LogIdOf<TC>>,
    last_membership: StoredMembershipOf<TC>,
    application: ApplicationStateV20260812,
}

#[derive(Serialize)]
struct LegacySnapshotFixture {
    meta: SnapshotMetaOf<TC>,
    data: Vec<u8>,
}

fn write_legacy_state(
    directory: &std::path::Path,
    leader: openraft::alias::CommittedLeaderIdOf<TC>,
    retention_ms: u64,
) {
    let mut application = ApplicationState::default();
    application.history_retention_ms = retention_ms;
    let fixture = LegacyStateFixture {
        state_machine: LegacyStateDataFixture {
            last_applied_log: Some(LogIdOf::<TC>::new(leader, 3)),
            last_membership: StoredMembershipOf::<TC>::default(),
            application: application.into(),
        },
        current_snapshot: None,
        snapshot_idx: 6,
    };
    let mut bytes = b"v2026_08_12_initial\n".to_vec();
    bytes = postcard::to_extend(&fixture, bytes).unwrap();
    fs::write(directory.join("raft-state.postcard"), bytes).unwrap();
}

#[test]
fn redb_import_preserves_fields_and_source_files() {
    const LOG_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("raft_log");
    const META_TABLE: TableDefinition<u8, &[u8]> = TableDefinition::new("raft_meta");

    let directory = test_directory();
    let redb_path = directory.join("raft-log.redb");
    let state_path = directory.join("raft-state.postcard");
    let leader = leader();
    let committed = LogIdOf::<TC>::new(leader, 3);
    let vote = VoteOf::<TC>::new(7, leader.node_id);
    let database = Database::create(&redb_path).unwrap();
    let transaction = database.begin_write().unwrap();
    {
        let mut log = transaction.open_table(LOG_TABLE).unwrap();
        let bytes = postcard::to_stdvec(&entry(leader, 3)).unwrap();
        log.insert(3, bytes.as_slice()).unwrap();
    }
    {
        let mut meta = transaction.open_table(META_TABLE).unwrap();
        let bytes = postcard::to_stdvec(&LegacyStoreMetaFixture {
            last_purged_log_id: Some(LogIdOf::<TC>::new(leader, 2)),
            committed: Some(committed),
            vote: Some(vote),
        })
        .unwrap();
        meta.insert(0, bytes.as_slice()).unwrap();
    }
    transaction.commit().unwrap();
    drop(database);
    write_legacy_state(&directory, leader, 456_789);
    let redb_before = fs::read(&redb_path).unwrap();
    let state_before = fs::read(&state_path).unwrap();

    let database = open_database(&directory);
    let log = LogRepository::new(database.clone()).load::<TC>().unwrap();
    assert_eq!([3], log.log.keys().copied().collect::<Vec<_>>().as_slice());
    assert_eq!(Some(LogIdOf::<TC>::new(leader, 2)), log.last_purged_log_id);
    assert_eq!(Some(committed), log.committed);
    assert_eq!(Some(vote), log.vote);
    let (state, snapshot, snapshot_idx) = StateRepository::new(database.clone()).load().unwrap();
    assert_eq!(Some(LogIdOf::<TC>::new(leader, 3)), state.last_applied_log);
    assert_eq!(456_789, state.application.history_retention_ms);
    assert!(snapshot.is_none());
    assert_eq!(6, snapshot_idx);
    drop(database);

    assert_eq!(redb_before, fs::read(&redb_path).unwrap());
    assert_eq!(state_before, fs::read(&state_path).unwrap());
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn postcard_fallback_import_preserves_fields_and_source_files() {
    let directory = test_directory();
    let log_path = directory.join("raft-log.postcard");
    let state_path = directory.join("raft-state.postcard");
    let leader = leader();
    let committed = LogIdOf::<TC>::new(leader, 3);
    let vote = VoteOf::<TC>::new(7, leader.node_id);
    let fixture = LegacyLogFixture {
        last_purged_log_id: Some(LogIdOf::<TC>::new(leader, 1)),
        log: BTreeMap::from([(3, entry(leader, 3))]),
        committed: Some(committed),
        vote: Some(vote),
    };
    fs::write(&log_path, postcard::to_stdvec(&fixture).unwrap()).unwrap();
    write_legacy_state(&directory, leader, 987_654);
    let log_before = fs::read(&log_path).unwrap();
    let state_before = fs::read(&state_path).unwrap();

    let database = open_database(&directory);
    let log = LogRepository::new(database.clone()).load::<TC>().unwrap();
    assert_eq!([3], log.log.keys().copied().collect::<Vec<_>>().as_slice());
    assert_eq!(Some(LogIdOf::<TC>::new(leader, 1)), log.last_purged_log_id);
    assert_eq!(Some(committed), log.committed);
    assert_eq!(Some(vote), log.vote);
    let (state, snapshot, snapshot_idx) = StateRepository::new(database.clone()).load().unwrap();
    assert_eq!(Some(LogIdOf::<TC>::new(leader, 3)), state.last_applied_log);
    assert_eq!(987_654, state.application.history_retention_ms);
    assert!(snapshot.is_none());
    assert_eq!(6, snapshot_idx);
    drop(database);

    assert_eq!(log_before, fs::read(&log_path).unwrap());
    assert_eq!(state_before, fs::read(&state_path).unwrap());
    fs::remove_dir_all(directory).unwrap();
}
