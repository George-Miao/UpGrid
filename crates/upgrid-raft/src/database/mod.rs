use std::cell::RefCell;
use std::fs;
use std::path::Path;
use std::time::Duration;

use refinery::{Migration, Runner};
use rusqlite::{Connection, ErrorCode};
#[cfg(test)]
use sea_query::Alias;
use sea_query::{Asterisk, Expr, ExprTrait, Iden, InsertStatement, Query, SqliteQueryBuilder};
use sea_query_rusqlite::RusqliteBinder;

use crate::error::DatabaseError;

mod codec;
mod import;
mod legacy;
mod log_repository;
mod schema;
mod state_repository;
mod v20260812_create_raft_tables;

pub(crate) use log_repository::LogRepository;
pub(crate) use state_repository::StateRepository;

const DATABASE_FILE: &str = "raft.sqlite3";
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(crate) struct RaftDatabase {
    pub(super) connection: RefCell<Connection>,
}

impl RaftDatabase {
    pub(crate) fn open(data_dir: &Path) -> Result<Self, DatabaseError> {
        fs::create_dir_all(data_dir).map_err(|source| DatabaseError::Directory {
            path: data_dir.to_path_buf(),
            source,
        })?;
        let path = data_dir.join(DATABASE_FILE);
        let mut connection = Connection::open(&path).map_err(|source| DatabaseError::Open {
            path: path.clone(),
            source,
        })?;
        configure(&connection)?;
        run_migrations(&mut connection)?;
        import::initialize(&mut connection, data_dir)?;

        Ok(Self {
            connection: RefCell::new(connection),
        })
    }

    #[cfg(test)]
    fn row_count(&self, table: &'static str) -> Result<u64, DatabaseError> {
        count_rows(&self.connection.borrow(), Alias::new(table))
    }
}

fn configure(connection: &Connection) -> Result<(), DatabaseError> {
    connection
        .busy_timeout(BUSY_TIMEOUT)
        .map_err(|source| sqlite_error("configure busy timeout", source))?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(|source| sqlite_error("enable WAL journal mode", source))?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(|source| sqlite_error("enable full synchronization", source))?;
    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(|source| sqlite_error("enable foreign keys", source))?;
    connection
        .pragma_update(None, "locking_mode", "EXCLUSIVE")
        .map_err(|source| sqlite_error("enable exclusive locking", source))?;
    Ok(())
}

fn run_migrations(connection: &mut Connection) -> Result<(), DatabaseError> {
    let sql = v20260812_create_raft_tables::migration();
    let migration = Migration::unapplied("V20260812__create_raft_tables.rs", &sql)
        .map_err(|source| DatabaseError::Migration { source })?;
    Runner::new(&[migration])
        .run(connection)
        .map_err(|source| DatabaseError::Migration { source })?;
    Ok(())
}

fn count_rows<T>(connection: &Connection, table: T) -> Result<u64, DatabaseError>
where
    T: Iden + 'static,
{
    let (sql, values) = Query::select()
        .expr(Expr::col(Asterisk).count())
        .from(table)
        .build_rusqlite(SqliteQueryBuilder);
    let count: i64 = connection
        .query_row(&sql, values.as_params().as_slice(), |row| row.get(0))
        .map_err(|source| sqlite_error("count durable rows", source))?;
    u64::try_from(count).map_err(|source| DatabaseError::IntegerRange {
        table: "sqlite",
        column: "count",
        value: count as u64,
        source,
    })
}

fn execute_insert(
    connection: &Connection,
    statement: InsertStatement,
    operation: &'static str,
) -> Result<(), DatabaseError> {
    let (sql, values) = statement.build_rusqlite(SqliteQueryBuilder);
    connection
        .execute(&sql, values.as_params().as_slice())
        .map_err(|source| sqlite_error(operation, source))?;
    Ok(())
}

pub(super) fn sqlite_error(operation: &'static str, source: rusqlite::Error) -> DatabaseError {
    match source.sqlite_error_code() {
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => {
            DatabaseError::Locked { operation, source }
        }
        _ => DatabaseError::Sqlite { operation, source },
    }
}

pub(super) fn transaction_error(operation: &'static str, source: rusqlite::Error) -> DatabaseError {
    match source.sqlite_error_code() {
        Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked) => {
            DatabaseError::Locked { operation, source }
        }
        _ => DatabaseError::Transaction { operation, source },
    }
}

#[cfg(test)]
mod tests;
