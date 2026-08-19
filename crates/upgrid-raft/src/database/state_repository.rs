use std::rc::Rc;

use rusqlite::OptionalExtension;
use sea_query::{Expr, ExprTrait, OnConflict, Query, SqliteQueryBuilder};
use sea_query_rusqlite::RusqliteBinder;

use super::codec::{decode_application, decode_field, encode_application, encode_field, integer};
use super::schema::{Snapshot, StateMachine};
use super::{RaftDatabase, sqlite_error, transaction_error};
use crate::error::DatabaseError;
use crate::state_machine::{StateMachineData, StoredSnapshot};

#[derive(Clone, Debug)]
pub(crate) struct StateRepository {
    database: Rc<RaftDatabase>,
}

impl StateRepository {
    pub(crate) fn new(database: Rc<RaftDatabase>) -> Self {
        Self { database }
    }

    pub(crate) fn load(
        &self,
    ) -> Result<(StateMachineData, Option<StoredSnapshot>, u64), DatabaseError> {
        let connection = self.database.connection.borrow();
        let (sql, values) = Query::select()
            .columns([
                StateMachine::LastAppliedLog,
                StateMachine::LastMembership,
                StateMachine::Application,
                StateMachine::SnapshotIdx,
            ])
            .from(StateMachine::Table)
            .and_where(Expr::col(StateMachine::Id).eq(1))
            .build_rusqlite(SqliteQueryBuilder);
        let state = connection
            .query_row(&sql, values.as_params().as_slice(), |row| {
                Ok((
                    row.get::<_, Option<Vec<u8>>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .optional()
            .map_err(|source| sqlite_error("load state machine", source))?
            .ok_or(DatabaseError::MissingRow {
                table: "state_machine",
            })?;
        let last_applied_log = state
            .0
            .map(|bytes| decode_field("state_machine", "last_applied_log", &bytes))
            .transpose()?;
        let last_membership = decode_field("state_machine", "last_membership", &state.1)?;
        let application = decode_application(&state.2)?;
        let snapshot_idx =
            u64::try_from(state.3).map_err(|source| DatabaseError::IntegerRange {
                table: "state_machine",
                column: "snapshot_idx",
                value: state.3 as u64,
                source,
            })?;

        let (sql, values) = Query::select()
            .columns([Snapshot::Meta, Snapshot::Data])
            .from(Snapshot::Table)
            .and_where(Expr::col(Snapshot::Id).eq(1))
            .build_rusqlite(SqliteQueryBuilder);
        let snapshot = connection
            .query_row(&sql, values.as_params().as_slice(), |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .optional()
            .map_err(|source| sqlite_error("load current snapshot", source))?
            .map(|(meta, data)| {
                Ok(StoredSnapshot {
                    meta: decode_field("snapshot", "meta", &meta)?,
                    data,
                })
            })
            .transpose()?;

        Ok((
            StateMachineData {
                last_applied_log,
                last_membership,
                application,
            },
            snapshot,
            snapshot_idx,
        ))
    }

    pub(crate) fn replace(
        &self,
        state: &StateMachineData,
        snapshot: Option<&StoredSnapshot>,
        snapshot_idx: u64,
    ) -> Result<(), DatabaseError> {
        let last_applied_log = state
            .last_applied_log
            .as_ref()
            .map(|value| encode_field("state_machine", "last_applied_log", value))
            .transpose()?;
        let last_membership =
            encode_field("state_machine", "last_membership", &state.last_membership)?;
        let application = encode_application(&state.application)?;
        let snapshot_idx = integer("state_machine", "snapshot_idx", snapshot_idx)?;
        let snapshot = snapshot
            .map(|snapshot| {
                Ok((
                    encode_field("snapshot", "meta", &snapshot.meta)?,
                    snapshot.data.clone(),
                ))
            })
            .transpose()?;

        let mut connection = self.database.connection.borrow_mut();
        let transaction = connection
            .transaction()
            .map_err(|source| transaction_error("replace state and snapshot", source))?;
        let statement = Query::update()
            .table(StateMachine::Table)
            .values([
                (StateMachine::LastAppliedLog, last_applied_log.into()),
                (StateMachine::LastMembership, last_membership.into()),
                (StateMachine::Application, application.into()),
                (StateMachine::SnapshotIdx, snapshot_idx.into()),
            ])
            .and_where(Expr::col(StateMachine::Id).eq(1))
            .to_owned();
        let (sql, values) = statement.build_rusqlite(SqliteQueryBuilder);
        let changed = transaction
            .execute(&sql, values.as_params().as_slice())
            .map_err(|source| sqlite_error("replace state-machine fields", source))?;
        if changed != 1 {
            return Err(DatabaseError::MissingRow {
                table: "state_machine",
            });
        }

        match snapshot {
            Some((meta, data)) => {
                let statement = Query::insert()
                    .into_table(Snapshot::Table)
                    .columns([Snapshot::Id, Snapshot::Meta, Snapshot::Data])
                    .values_panic([1.into(), meta.into(), data.into()])
                    .on_conflict(
                        OnConflict::column(Snapshot::Id)
                            .update_columns([Snapshot::Meta, Snapshot::Data])
                            .to_owned(),
                    )
                    .to_owned();
                let (sql, values) = statement.build_rusqlite(SqliteQueryBuilder);
                transaction
                    .execute(&sql, values.as_params().as_slice())
                    .map_err(|source| sqlite_error("replace current snapshot", source))?;
            }
            None => {
                let statement = Query::delete()
                    .from_table(Snapshot::Table)
                    .and_where(Expr::col(Snapshot::Id).eq(1))
                    .to_owned();
                let (sql, values) = statement.build_rusqlite(SqliteQueryBuilder);
                transaction
                    .execute(&sql, values.as_params().as_slice())
                    .map_err(|source| sqlite_error("delete current snapshot", source))?;
            }
        }

        transaction
            .commit()
            .map_err(|source| transaction_error("commit state and snapshot replacement", source))
    }
}
