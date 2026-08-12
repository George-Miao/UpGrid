use std::collections::BTreeMap;
use std::rc::Rc;

use openraft::RaftTypeConfig;
use openraft::alias::{LogIdOf, VoteOf};
use openraft::entry::RaftEntry;
use rusqlite::OptionalExtension;
use sea_query::{Expr, ExprTrait, OnConflict, Order, Query, SqliteQueryBuilder};
use sea_query_rusqlite::RusqliteBinder;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::codec::{decode_field, encode_field, integer};
use super::schema::{RaftLog, RaftMeta};
use super::{RaftDatabase, execute_insert, sqlite_error, transaction_error};
use crate::error::DatabaseError;
use crate::storage::InMemStoreInner;

#[derive(Clone, Debug)]
pub(crate) struct LogRepository {
    database: Rc<RaftDatabase>,
}

impl LogRepository {
    pub(crate) fn new(database: Rc<RaftDatabase>) -> Self {
        Self { database }
    }

    pub(crate) fn load<C>(&self) -> Result<InMemStoreInner<C>, DatabaseError>
    where
        C: RaftTypeConfig,
        C::Entry: DeserializeOwned,
        LogIdOf<C>: DeserializeOwned,
        VoteOf<C>: DeserializeOwned,
    {
        let connection = self.database.connection.borrow();
        let (sql, values) = Query::select()
            .columns([
                RaftMeta::LastPurgedLogId,
                RaftMeta::Committed,
                RaftMeta::Vote,
            ])
            .from(RaftMeta::Table)
            .and_where(Expr::col(RaftMeta::Id).eq(1))
            .build_rusqlite(SqliteQueryBuilder);
        let meta = connection
            .query_row(&sql, values.as_params().as_slice(), |row| {
                Ok((
                    row.get::<_, Option<Vec<u8>>>(0)?,
                    row.get::<_, Option<Vec<u8>>>(1)?,
                    row.get::<_, Option<Vec<u8>>>(2)?,
                ))
            })
            .optional()
            .map_err(|source| sqlite_error("load Raft metadata", source))?
            .ok_or(DatabaseError::MissingRow { table: "raft_meta" })?;
        let last_purged_log_id = meta
            .0
            .map(|bytes| decode_field("raft_meta", "last_purged_log_id", &bytes))
            .transpose()?;
        let committed = meta
            .1
            .map(|bytes| decode_field("raft_meta", "committed", &bytes))
            .transpose()?;
        let vote = meta
            .2
            .map(|bytes| decode_field("raft_meta", "vote", &bytes))
            .transpose()?;

        let (sql, values) = Query::select()
            .columns([RaftLog::LogIndex, RaftLog::Entry])
            .from(RaftLog::Table)
            .order_by(RaftLog::LogIndex, Order::Asc)
            .build_rusqlite(SqliteQueryBuilder);
        let mut statement = connection
            .prepare(&sql)
            .map_err(|source| sqlite_error("prepare Raft log load", source))?;
        let rows = statement
            .query_map(values.as_params().as_slice(), |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(|source| sqlite_error("load Raft log", source))?;
        let mut log = BTreeMap::new();
        for row in rows {
            let (index, bytes) = row.map_err(|source| sqlite_error("read Raft log row", source))?;
            let index = u64::try_from(index).map_err(|source| DatabaseError::IntegerRange {
                table: "raft_log",
                column: "log_index",
                value: index as u64,
                source,
            })?;
            let entry = decode_field("raft_log", "entry", &bytes)?;
            log.insert(index, entry);
        }

        Ok(InMemStoreInner {
            last_purged_log_id,
            log,
            committed,
            vote,
        })
    }

    pub(crate) fn append<C>(&self, entries: &[C::Entry]) -> Result<(), DatabaseError>
    where
        C: RaftTypeConfig,
        C::Entry: Serialize,
    {
        let entries = entries
            .iter()
            .map(|entry| {
                Ok((
                    integer("raft_log", "log_index", entry.index())?,
                    encode_field("raft_log", "entry", entry)?,
                ))
            })
            .collect::<Result<Vec<_>, DatabaseError>>()?;
        let mut connection = self.database.connection.borrow_mut();
        let transaction = connection
            .transaction()
            .map_err(|source| transaction_error("append Raft log", source))?;
        for (index, entry) in entries {
            let statement = Query::insert()
                .into_table(RaftLog::Table)
                .columns([RaftLog::LogIndex, RaftLog::Entry])
                .values_panic([index.into(), entry.into()])
                .on_conflict(
                    OnConflict::column(RaftLog::LogIndex)
                        .update_column(RaftLog::Entry)
                        .to_owned(),
                )
                .to_owned();
            execute_insert(&transaction, statement, "append Raft log entry")?;
        }
        transaction
            .commit()
            .map_err(|source| transaction_error("commit Raft log append", source))
    }

    pub(crate) fn truncate_after<C>(
        &self,
        last_log_id: Option<LogIdOf<C>>,
    ) -> Result<(), DatabaseError>
    where
        C: RaftTypeConfig,
    {
        let start = match last_log_id {
            Some(log_id) => match log_id.index().checked_add(1) {
                Some(index) => index,
                None => return Ok(()),
            },
            None => 0,
        };
        let start = integer("raft_log", "log_index", start)?;
        let statement = Query::delete()
            .from_table(RaftLog::Table)
            .and_where(Expr::col(RaftLog::LogIndex).gte(start))
            .to_owned();
        execute_delete(
            &self.database.connection.borrow(),
            statement,
            "truncate Raft log",
        )
    }

    pub(crate) fn purge<C>(&self, log_id: &LogIdOf<C>) -> Result<(), DatabaseError>
    where
        C: RaftTypeConfig,
        LogIdOf<C>: Serialize,
    {
        let index = integer("raft_log", "log_index", log_id.index())?;
        let last_purged = encode_field("raft_meta", "last_purged_log_id", log_id)?;
        let mut connection = self.database.connection.borrow_mut();
        let transaction = connection
            .transaction()
            .map_err(|source| transaction_error("purge Raft log", source))?;
        let delete = Query::delete()
            .from_table(RaftLog::Table)
            .and_where(Expr::col(RaftLog::LogIndex).lte(index))
            .to_owned();
        execute_delete(&transaction, delete, "delete purged Raft log entries")?;
        update_meta(
            &transaction,
            RaftMeta::LastPurgedLogId,
            last_purged.into(),
            "update last purged log id",
        )?;
        transaction
            .commit()
            .map_err(|source| transaction_error("commit Raft log purge", source))
    }

    pub(crate) fn save_vote<C>(&self, vote: &VoteOf<C>) -> Result<(), DatabaseError>
    where
        C: RaftTypeConfig,
        VoteOf<C>: Serialize,
    {
        let vote = encode_field("raft_meta", "vote", vote)?;
        update_meta(
            &self.database.connection.borrow(),
            RaftMeta::Vote,
            vote.into(),
            "save Raft vote",
        )
    }

    pub(crate) fn save_committed<C>(
        &self,
        committed: Option<&LogIdOf<C>>,
    ) -> Result<(), DatabaseError>
    where
        C: RaftTypeConfig,
        LogIdOf<C>: Serialize,
    {
        let committed = committed
            .map(|value| encode_field("raft_meta", "committed", value))
            .transpose()?;
        update_meta(
            &self.database.connection.borrow(),
            RaftMeta::Committed,
            committed.into(),
            "save committed log id",
        )
    }
}

fn update_meta(
    connection: &rusqlite::Connection,
    column: RaftMeta,
    value: sea_query::Value,
    operation: &'static str,
) -> Result<(), DatabaseError> {
    let statement = Query::update()
        .table(RaftMeta::Table)
        .value(column, value)
        .and_where(Expr::col(RaftMeta::Id).eq(1))
        .to_owned();
    let (sql, values) = statement.build_rusqlite(SqliteQueryBuilder);
    let changed = connection
        .execute(&sql, values.as_params().as_slice())
        .map_err(|source| sqlite_error(operation, source))?;
    if changed != 1 {
        return Err(DatabaseError::MissingRow { table: "raft_meta" });
    }
    Ok(())
}

fn execute_delete(
    connection: &rusqlite::Connection,
    statement: sea_query::DeleteStatement,
    operation: &'static str,
) -> Result<(), DatabaseError> {
    let (sql, values) = statement.build_rusqlite(SqliteQueryBuilder);
    connection
        .execute(&sql, values.as_params().as_slice())
        .map_err(|source| sqlite_error(operation, source))?;
    Ok(())
}
