//! Configuration and durable identity errors.

use std::io;
use std::path::PathBuf;

use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("failed to load configuration: {source}"))]
    Load {
        #[snafu(source(from(figment::Error, Box::new)))]
        source: Box<figment::Error>,
    },

    #[snafu(display("{message}"))]
    InvalidConfiguration { message: &'static str },

    #[snafu(display("failed to read {}: {source}", path.display()))]
    Read { path: PathBuf, source: io::Error },

    #[snafu(display("failed to write {}: {source}", path.display()))]
    Write { path: PathBuf, source: io::Error },

    #[snafu(display("stored Node ID in {} is invalid: {source}", path.display()))]
    NodeId { path: PathBuf, source: uuid::Error },

    #[snafu(display("node name must contain 1 to 64 printable characters"))]
    NodeNameInvalid,

    #[snafu(display("invalid OOBE phase: {phase}"))]
    OobePhaseInvalid { phase: String },

    #[snafu(display("configured deployment key does not match the data directory"))]
    DeploymentKeyMismatch,

    #[snafu(display("joining a Cluster requires a valid up:// invitation"))]
    JoinLinkRequired,

    #[snafu(transparent)]
    Cipher { source: crate::secret::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
