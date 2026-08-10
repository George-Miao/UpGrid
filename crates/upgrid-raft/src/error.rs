//! Raft and RPC runtime errors.

use std::path::PathBuf;

use openraft::error::{ClientWriteError, Fatal, InitializeError, RaftError};
use snafu::Snafu;
use tarpc::client::RpcError;
use url::Url;

use crate::raft::TC;

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

    #[snafu(display("Failed to open Raft log at {}: {}", path.display(), source))]
    RaftLogOpen {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Failed to open Raft state machine at {}: {}", path.display(), source))]
    StateMachineOpen {
        path: PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("deployment key does not match the Cluster"))]
    DeploymentKeyMismatch,

    #[snafu(display("Cluster rejected join request: {}", source))]
    JoinRejected { source: crate::rpc::JoinError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
