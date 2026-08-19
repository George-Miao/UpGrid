use std::fs;
use std::process::Command;
use std::rc::Rc;
use std::time::Duration;

use openraft::EntryPayload;
use openraft::alias::{
    CommittedLeaderIdOf, EntryOf, LogIdOf, SnapshotMetaOf, StoredMembershipOf, VoteOf,
};
use sea_query::{Alias, Expr, ExprTrait, Query, SqliteQueryBuilder};
use sea_query_rusqlite::RusqliteBinder;
use uuid::Uuid;

use super::{LogRepository, RaftDatabase, StateRepository};
use crate::domain::{
    ApplicationState, EvaluationAssignment, EvaluationAssignmentKey, EvaluationId, JoinTokenHash,
    TargetId,
};
use crate::raft::TC;
use crate::state_machine::{StateMachineData, StoredSnapshot};
mod legacy_import;

fn open_database(directory: &std::path::Path) -> Rc<RaftDatabase> {
    Rc::new(RaftDatabase::open(directory).unwrap())
}

fn leader() -> CommittedLeaderIdOf<TC> {
    CommittedLeaderIdOf::<TC> {
        term: 7,
        node_id: Uuid::now_v7(),
    }
}

fn entry(leader: CommittedLeaderIdOf<TC>, index: u64) -> EntryOf<TC> {
    EntryOf::<TC> {
        log_id: LogIdOf::<TC>::new(leader, index),
        payload: EntryPayload::Blank,
    }
}

fn test_directory() -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("upgrid-database-test-{}", Uuid::now_v7()));
    fs::create_dir_all(&path).unwrap();
    path
}

type RawMeta = (Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>);

fn raw_meta(database: &RaftDatabase) -> RawMeta {
    let connection = database.connection.borrow();
    let (sql, values) = Query::select()
        .columns([
            super::schema::RaftMeta::LastPurgedLogId,
            super::schema::RaftMeta::Committed,
            super::schema::RaftMeta::Vote,
        ])
        .from(super::schema::RaftMeta::Table)
        .and_where(Expr::col(super::schema::RaftMeta::Id).eq(1))
        .build_rusqlite(SqliteQueryBuilder);
    connection
        .query_row(&sql, values.as_params().as_slice(), |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
}

#[test]
fn fresh_open_creates_field_oriented_tables_and_singletons() {
    let directory = test_directory();
    let database = RaftDatabase::open(&directory).unwrap();
    let connection = database.connection.borrow();

    let tables = ["raft_log", "raft_meta", "state_machine", "snapshot"];
    for table in tables {
        let (sql, values) = Query::select()
            .expr(Expr::col(Alias::new("name")))
            .from(Alias::new("sqlite_master"))
            .and_where(Expr::col(Alias::new("type")).eq("table"))
            .and_where(Expr::col(Alias::new("name")).eq(table))
            .build_rusqlite(SqliteQueryBuilder);
        let name: String = connection
            .query_row(&sql, values.as_params().as_slice(), |row| row.get(0))
            .unwrap();
        assert_eq!(table, name);
    }

    assert_eq!(1, database.row_count("raft_meta").unwrap());
    assert_eq!(1, database.row_count("state_machine").unwrap());
    assert_eq!(0, database.row_count("snapshot").unwrap());
    assert_eq!(0, database.row_count("raft_log").unwrap());

    drop(connection);
    drop(database);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn refinery_records_the_only_database_format_version() {
    let directory = test_directory();
    let database = RaftDatabase::open(&directory).unwrap();
    let connection = database.connection.borrow();
    let (sql, values) = Query::select()
        .columns([
            Alias::new("version"),
            Alias::new("name"),
            Alias::new("checksum"),
        ])
        .from(Alias::new("refinery_schema_history"))
        .build_rusqlite(SqliteQueryBuilder);
    let migrations = connection
        .prepare(&sql)
        .unwrap()
        .query_map(values.as_params().as_slice(), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(1, migrations.len());
    assert_eq!(20_260_812, migrations[0].0);
    assert_eq!("create_raft_tables", migrations[0].1);
    assert!(!migrations[0].2.is_empty());

    drop(connection);
    drop(database);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn metadata_columns_survive_reopen_without_record_payload() {
    let directory = test_directory();
    let database = open_database(&directory);
    let repository = LogRepository::new(database.clone());
    let leader = leader();
    let committed = LogIdOf::<TC>::new(leader, 4);
    let vote = VoteOf::<TC>::new(7, leader.node_id);
    repository.save_committed::<TC>(Some(&committed)).unwrap();
    repository.save_vote::<TC>(&vote).unwrap();
    drop(repository);
    drop(database);

    let database = open_database(&directory);
    let loaded = LogRepository::new(database.clone()).load::<TC>().unwrap();
    assert_eq!(Some(committed), loaded.committed);
    assert_eq!(Some(vote), loaded.vote);
    let connection = database.connection.borrow();
    let mut statement = connection
        .prepare("PRAGMA table_info(\"raft_meta\")")
        .unwrap();
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        ["id", "last_purged_log_id", "committed", "vote"],
        columns.as_slice()
    );
    drop(statement);
    drop(connection);
    drop(database);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn vote_and_committed_updates_do_not_change_other_metadata_columns() {
    let directory = test_directory();
    let database = open_database(&directory);
    let repository = LogRepository::new(database.clone());
    let leader = leader();
    let entries = (1..=3)
        .map(|index| entry(leader, index))
        .collect::<Vec<_>>();
    repository.append::<TC>(&entries).unwrap();
    repository
        .purge::<TC>(&LogIdOf::<TC>::new(leader, 1))
        .unwrap();
    repository
        .save_committed::<TC>(Some(&LogIdOf::<TC>::new(leader, 2)))
        .unwrap();
    let before_vote = raw_meta(&database);
    let vote = VoteOf::<TC>::new(8, leader.node_id);
    repository.save_vote::<TC>(&vote).unwrap();
    let after_vote = raw_meta(&database);
    assert_eq!(before_vote.0, after_vote.0);
    assert_eq!(before_vote.1, after_vote.1);

    repository
        .save_committed::<TC>(Some(&LogIdOf::<TC>::new(leader, 3)))
        .unwrap();
    let after_committed = raw_meta(&database);
    assert_eq!(after_vote.0, after_committed.0);
    assert_eq!(after_vote.2, after_committed.2);

    drop(repository);
    drop(database);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn conflict_delete_and_purge_touch_only_requested_rows() {
    let directory = test_directory();
    let database = open_database(&directory);
    let repository = LogRepository::new(database.clone());
    let leader = leader();
    let entries = (1..=5)
        .map(|index| entry(leader, index))
        .collect::<Vec<_>>();
    repository.append::<TC>(&entries).unwrap();
    repository
        .truncate_after::<TC>(Some(LogIdOf::<TC>::new(leader, 2)))
        .unwrap();
    let loaded = repository.load::<TC>().unwrap();
    assert_eq!(
        [1, 2],
        loaded.log.keys().copied().collect::<Vec<_>>().as_slice()
    );

    repository.append::<TC>(&entries[2..]).unwrap();
    repository
        .purge::<TC>(&LogIdOf::<TC>::new(leader, 2))
        .unwrap();
    let loaded = repository.load::<TC>().unwrap();
    assert_eq!(
        [3, 4, 5],
        loaded.log.keys().copied().collect::<Vec<_>>().as_slice()
    );
    assert_eq!(
        Some(LogIdOf::<TC>::new(leader, 2)),
        loaded.last_purged_log_id
    );

    drop(repository);
    drop(database);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_stores_each_state_machine_field_in_its_column() {
    let directory = test_directory();
    let database = open_database(&directory);
    let repository = StateRepository::new(database.clone());
    let leader = leader();
    let mut application = ApplicationState::default();
    application.history_retention_ms = 123_456;
    let state = StateMachineData {
        last_applied_log: Some(LogIdOf::<TC>::new(leader, 9)),
        last_membership: StoredMembershipOf::<TC>::default(),
        application,
    };
    repository.replace(&state, None, 11).unwrap();
    drop(repository);
    drop(database);

    let database = open_database(&directory);
    let (loaded, snapshot, snapshot_idx) = StateRepository::new(database.clone()).load().unwrap();
    assert_eq!(state.last_applied_log, loaded.last_applied_log);
    assert_eq!(state.last_membership, loaded.last_membership);
    assert_eq!(state.application, loaded.application);
    assert_eq!(11, snapshot_idx);
    assert!(snapshot.is_none());
    drop(database);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn checkpoint_round_trips_composite_map_keys_as_json_entries() {
    let directory = test_directory();
    let database = open_database(&directory);
    let repository = StateRepository::new(database.clone());
    let target_id = TargetId(Uuid::now_v7());
    let executor_node_id = Uuid::now_v7();
    let id = EvaluationId {
        target_id,
        scheduled_at_ms: 1_000,
    };
    let assignment = EvaluationAssignment {
        id,
        executor_node_id,
        assigned_at_ms: 900,
        expires_at_ms: 2_000,
        attempt: 1,
    };
    let mut application = ApplicationState::default();
    application
        .assignments
        .insert(EvaluationAssignmentKey::from(&assignment), assignment);
    application.join_tokens.insert(JoinTokenHash([7; 32]), 3);
    let state = StateMachineData {
        application,
        ..StateMachineData::default()
    };

    repository.replace(&state, None, 0).unwrap();
    let (loaded, ..) = repository.load().unwrap();

    assert_eq!(state.application, loaded.application);
    drop(repository);
    drop(database);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn snapshot_meta_and_raw_data_survive_reopen() {
    let directory = test_directory();
    let database = open_database(&directory);
    let repository = StateRepository::new(database.clone());
    let leader = leader();
    let state = StateMachineData {
        last_applied_log: Some(LogIdOf::<TC>::new(leader, 12)),
        ..StateMachineData::default()
    };
    let snapshot = StoredSnapshot {
        meta: SnapshotMetaOf::<TC> {
            last_log_id: state.last_applied_log,
            last_membership: state.last_membership.clone(),
        },
        data: vec![0, 1, 2, 255, 0, 42],
    };
    repository.replace(&state, Some(&snapshot), 4).unwrap();
    drop(repository);
    drop(database);

    let database = open_database(&directory);
    let (_, loaded, _) = StateRepository::new(database.clone()).load().unwrap();
    let loaded = loaded.unwrap();
    assert_eq!(snapshot.meta, loaded.meta);
    assert_eq!(snapshot.data, loaded.data);
    drop(database);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn null_optional_fields_round_trip_without_serialized_option_wrappers() {
    let directory = test_directory();
    let database = open_database(&directory);
    let repository = LogRepository::new(database.clone());
    let loaded = repository.load::<TC>().unwrap();
    assert!(loaded.last_purged_log_id.is_none());
    assert!(loaded.committed.is_none());
    assert!(loaded.vote.is_none());
    assert_eq!((None, None, None), raw_meta(&database));

    let (state, snapshot, _) = StateRepository::new(database.clone()).load().unwrap();
    assert!(state.last_applied_log.is_none());
    assert!(snapshot.is_none());
    drop(repository);
    drop(database);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn malformed_field_blob_reports_its_table_and_column() {
    let directory = test_directory();
    let database = open_database(&directory);
    let statement = Query::update()
        .table(super::schema::RaftMeta::Table)
        .value(super::schema::RaftMeta::Vote, Expr::val(vec![0xff, 0x00]))
        .and_where(Expr::col(super::schema::RaftMeta::Id).eq(1))
        .to_owned();
    let (sql, values) = statement.build_rusqlite(SqliteQueryBuilder);
    database
        .connection
        .borrow()
        .execute(&sql, values.as_params().as_slice())
        .unwrap();

    let error = LogRepository::new(database.clone())
        .load::<TC>()
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("raft_meta.vote"), "{message}");

    drop(database);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn failed_state_and_snapshot_replacement_keeps_all_old_columns() {
    let directory = test_directory();
    let database = open_database(&directory);
    let repository = StateRepository::new(database.clone());
    let leader = leader();
    let state = StateMachineData {
        last_applied_log: Some(LogIdOf::<TC>::new(leader, 6)),
        ..StateMachineData::default()
    };
    let snapshot = StoredSnapshot {
        meta: SnapshotMetaOf::<TC> {
            last_log_id: state.last_applied_log,
            last_membership: state.last_membership.clone(),
        },
        data: vec![3, 1, 4, 1, 5],
    };
    repository.replace(&state, Some(&snapshot), 2).unwrap();

    let mut replacement = state.clone();
    replacement.application.history_retention_ms = 99;
    repository
        .replace(&replacement, None, u64::MAX)
        .unwrap_err();
    let (loaded, loaded_snapshot, loaded_idx) = repository.load().unwrap();
    assert_eq!(state.last_applied_log, loaded.last_applied_log);
    assert_eq!(state.last_membership, loaded.last_membership);
    assert_eq!(state.application, loaded.application);
    assert_eq!(
        Some(snapshot.meta),
        loaded_snapshot.as_ref().map(|value| value.meta.clone())
    );
    assert_eq!(snapshot.data, loaded_snapshot.unwrap().data);
    assert_eq!(2, loaded_idx);

    drop(repository);
    drop(database);
    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn exclusive_lock_child() {
    let Ok(path) = std::env::var("UPGRID_LOCK_TEST_DATABASE") else {
        return;
    };
    let connection = rusqlite::Connection::open(path).unwrap();
    connection.busy_timeout(Duration::from_millis(25)).unwrap();
    let statement = Query::update()
        .table(super::schema::RaftMeta::Table)
        .value(super::schema::RaftMeta::Committed, Expr::val(vec![1_u8]))
        .and_where(Expr::col(super::schema::RaftMeta::Id).eq(1))
        .to_owned();
    let (sql, values) = statement.build_rusqlite(SqliteQueryBuilder);
    let error = connection
        .execute(&sql, values.as_params().as_slice())
        .unwrap_err();
    assert!(matches!(
        error.sqlite_error_code(),
        Some(rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked)
    ));
}

#[test]
fn second_process_cannot_write_exclusively_locked_database() {
    let directory = test_directory();
    let database = open_database(&directory);
    let executable = std::env::current_exe().unwrap();
    let output = Command::new(executable)
        .args([
            "--exact",
            "database::tests::exclusive_lock_child",
            "--nocapture",
        ])
        .env("UPGRID_LOCK_TEST_DATABASE", directory.join("raft.sqlite3"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "child process failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    drop(database);
    fs::remove_dir_all(directory).unwrap();
}
