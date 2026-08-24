use std::path::Path;

use rusqlite::Connection;
use sea_query::{OnConflict, Query};

use super::codec::{encode_application, encode_field, integer};
use super::schema::{RaftLog, RaftMeta, Snapshot, StateMachine};
use super::{count_rows, execute_insert, legacy, transaction_error};
use crate::error::DatabaseError;
use crate::state_machine::{migrate_legacy_membership, migrate_snapshot_reachability};

pub(super) fn initialize(
    connection: &mut Connection,
    data_dir: &Path,
) -> Result<(), DatabaseError> {
    let durable_rows = count_rows(connection, RaftLog::Table)?
        + count_rows(connection, RaftMeta::Table)?
        + count_rows(connection, StateMachine::Table)?
        + count_rows(connection, Snapshot::Table)?;
    if durable_rows != 0 {
        return Ok(());
    }

    let recovered = legacy::recover(data_dir)?;
    let entries = recovered
        .log
        .log
        .iter()
        .map(|(index, entry)| {
            Ok((
                integer("raft_log", "log_index", *index)?,
                encode_field("raft_log", "entry", entry)?,
            ))
        })
        .collect::<Result<Vec<_>, DatabaseError>>()?;
    let last_purged_log_id = recovered
        .log
        .last_purged_log_id
        .as_ref()
        .map(|value| encode_field("raft_meta", "last_purged_log_id", value))
        .transpose()?;
    let committed = recovered
        .log
        .committed
        .as_ref()
        .map(|value| encode_field("raft_meta", "committed", value))
        .transpose()?;
    let vote = recovered
        .log
        .vote
        .as_ref()
        .map(|value| encode_field("raft_meta", "vote", value))
        .transpose()?;
    let (mut state, mut snapshot, snapshot_idx) = recovered.state.runtime();
    migrate_legacy_membership(&mut state.application, &mut state.last_membership);
    migrate_snapshot_reachability(&mut snapshot)
        .map_err(|source| DatabaseError::LegacySnapshotMigration { source })?;
    let last_applied_log = state
        .last_applied_log
        .as_ref()
        .map(|value| encode_field("state_machine", "last_applied_log", value))
        .transpose()?;
    let last_membership = encode_field("state_machine", "last_membership", &state.last_membership)?;
    let application = encode_application(&state.application)?;
    let snapshot_idx = integer("state_machine", "snapshot_idx", snapshot_idx)?;
    let snapshot = snapshot
        .map(|snapshot| {
            Ok((
                encode_field("snapshot", "meta", &snapshot.meta)?,
                snapshot.data,
            ))
        })
        .transpose()?;

    let transaction = connection
        .transaction()
        .map_err(|source| transaction_error("import legacy persistence", source))?;
    for (index, entry) in entries {
        execute_insert(
            &transaction,
            Query::insert()
                .into_table(RaftLog::Table)
                .columns([RaftLog::LogIndex, RaftLog::Entry])
                .values_panic([index.into(), entry.into()])
                .on_conflict(
                    OnConflict::column(RaftLog::LogIndex)
                        .update_column(RaftLog::Entry)
                        .to_owned(),
                )
                .to_owned(),
            "import legacy Raft log entry",
        )?;
    }
    execute_insert(
        &transaction,
        Query::insert()
            .into_table(RaftMeta::Table)
            .columns([
                RaftMeta::Id,
                RaftMeta::LastPurgedLogId,
                RaftMeta::Committed,
                RaftMeta::Vote,
            ])
            .values_panic([
                1.into(),
                last_purged_log_id.into(),
                committed.into(),
                vote.into(),
            ])
            .to_owned(),
        "import legacy Raft metadata",
    )?;
    execute_insert(
        &transaction,
        Query::insert()
            .into_table(StateMachine::Table)
            .columns([
                StateMachine::Id,
                StateMachine::LastAppliedLog,
                StateMachine::LastMembership,
                StateMachine::Application,
                StateMachine::SnapshotIdx,
            ])
            .values_panic([
                1.into(),
                last_applied_log.into(),
                last_membership.into(),
                application.into(),
                snapshot_idx.into(),
            ])
            .to_owned(),
        "import legacy state machine",
    )?;
    if let Some((meta, data)) = snapshot {
        execute_insert(
            &transaction,
            Query::insert()
                .into_table(Snapshot::Table)
                .columns([Snapshot::Id, Snapshot::Meta, Snapshot::Data])
                .values_panic([1.into(), meta.into(), data.into()])
                .to_owned(),
            "import legacy snapshot",
        )?;
    }
    transaction
        .commit()
        .map_err(|source| transaction_error("commit legacy persistence import", source))
}
