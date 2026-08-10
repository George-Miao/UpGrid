//! Process orchestration errors.

use std::io;
use std::path::PathBuf;

use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("failed to create data directory {}: {source}", path.display()))]
    DataDirectory { path: PathBuf, source: io::Error },

    #[snafu(display("configured Join Token is invalid"))]
    JoinTokenInvalid,

    #[snafu(display("configured deployment key does not match the Join Token"))]
    JoinDeploymentKeyMismatch,

    #[snafu(display("Cluster request channel stopped"))]
    ClusterStopped,

    #[snafu(display("failed to update Cluster state: {reason}"))]
    ClusterWrite { reason: String },

    #[snafu(transparent)]
    Config { source: upgrid_config::Error },

    #[snafu(transparent)]
    Api { source: upgrid_api::Error },

    #[snafu(transparent)]
    Raft { source: upgrid_raft::Error },

    #[snafu(transparent)]
    Cluster { source: upgrid_raft::ClusterError },

    #[snafu(transparent)]
    Cipher { source: upgrid_config::CipherError },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
