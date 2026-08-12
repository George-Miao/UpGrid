//! Raft and RPC runtime errors.

use std::num::TryFromIntError;
use std::path::PathBuf;

use openraft::error::{ClientWriteError, Fatal, InitializeError, LinearizableReadError, RaftError};
use openraft::metrics::WaitError;
use snafu::Snafu;
use tarpc::client::RpcError;
use url::Url;

use crate::UpgridNode;
use crate::raft::TC;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub(crate) enum DatabaseError {
    #[snafu(display("failed to create Raft data directory at {}: {source}", path.display()))]
    Directory {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("failed to open Raft database at {}: {source}", path.display()))]
    Open {
        path: PathBuf,
        source: rusqlite::Error,
    },

    #[snafu(display("failed to encode {table}.{column}: {source}"))]
    FieldEncode {
        table: &'static str,
        column: &'static str,
        source: serde_json::Error,
    },

    #[snafu(display("failed to decode {table}.{column}: {source}"))]
    FieldDecode {
        table: &'static str,
        column: &'static str,
        source: serde_json::Error,
    },

    #[snafu(display("{table}.{column} value {value} is outside SQLite INTEGER range: {source}"))]
    IntegerRange {
        table: &'static str,
        column: &'static str,
        value: u64,
        source: TryFromIntError,
    },

    #[snafu(display("SQLite database is locked during {operation}: {source}"))]
    Locked {
        operation: &'static str,
        source: rusqlite::Error,
    },

    #[snafu(display("SQLite operation `{operation}` failed: {source}"))]
    Sqlite {
        operation: &'static str,
        source: rusqlite::Error,
    },

    #[snafu(display("SQLite transaction `{operation}` failed: {source}"))]
    Transaction {
        operation: &'static str,
        source: rusqlite::Error,
    },

    #[snafu(display("Raft database migration failed: {source}"))]
    Migration { source: refinery::Error },

    #[snafu(display("Raft database has no required {table} singleton row"))]
    MissingRow { table: &'static str },

    #[snafu(display("failed to read legacy persistence at {}: {source}", path.display()))]
    LegacyRead {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("failed to access legacy redb persistence at {}: {source}", path.display()))]
    LegacyRedb { path: PathBuf, source: redb::Error },

    #[snafu(display("failed to decode legacy Postcard persistence at {}: {source}", path.display()))]
    LegacyPostcard {
        path: PathBuf,
        source: postcard::Error,
    },
}

impl From<DatabaseError> for std::io::Error {
    fn from(error: DatabaseError) -> Self {
        let kind = match error {
            DatabaseError::FieldEncode { .. }
            | DatabaseError::FieldDecode { .. }
            | DatabaseError::IntegerRange { .. }
            | DatabaseError::MissingRow { .. }
            | DatabaseError::LegacyPostcard { .. } => std::io::ErrorKind::InvalidData,
            _ => std::io::ErrorKind::Other,
        };
        Self::new(kind, error)
    }
}
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(transparent)]
    Transport { source: upgrid_transport::Error },

    #[snafu(display("failed to parse Node URL: {source}"))]
    UrlParse { source: url::ParseError },

    #[snafu(display("invalid Node scheme in {url}; expected `up`"))]
    UrlInvalidScheme { url: Url },

    #[snafu(display("Node URL has no host: {url}"))]
    UrlInvalidHost { url: Url },

    #[snafu(display("Failed to create Raft instance: {}", source))]
    RaftCreation { source: Fatal<TC> },

    #[snafu(display("Failed to initialize Raft: {}", source))]
    RaftInitialize {
        source: RaftError<TC, InitializeError<TC>>,
    },

    #[snafu(display("Failed to join Raft: {}", source))]
    RaftJoin {
        source: RaftError<TC, ClientWriteError<TC>>,
    },

    #[snafu(display("RPC error: {}", source))]
    RpcError { source: RpcError },

    #[snafu(display("leadership could not be established before the write deadline"))]
    LeadershipDeadline,

    #[snafu(display("timed out connecting to forwarded leader {node}"))]
    ForwardConnectTimeout { node: UpgridNode },

    #[snafu(display("Raft rejected a replicated write: {source}"))]
    RaftWrite {
        source: RaftError<TC, ClientWriteError<TC>>,
    },

    #[snafu(display("a linearizable read was unavailable: {source}"))]
    LinearizableRead {
        source: RaftError<TC, LinearizableReadError<TC>>,
    },

    #[snafu(display("linearizable read unavailable before the read deadline"))]
    LinearizableReadDeadline,

    #[snafu(display("timed out connecting to forwarded read leader {node}"))]
    ForwardReadConnectTimeout { node: UpgridNode },

    #[snafu(display("timed out waiting for the local read barrier: {source}"))]
    ReadBarrier { source: WaitError },

    #[snafu(display("Node RPC probe timed out for {node}"))]
    NodeProbeTimeout { node: UpgridNode },

    #[snafu(display("Failed to open Raft database at {}: {source}", path.display()))]
    DatabaseOpen {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("deployment key does not match the Cluster"))]
    DeploymentKeyMismatch,

    #[snafu(display("Cluster rejected join request: {}", source))]
    JoinRejected { source: crate::rpc::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
