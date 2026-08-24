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

    #[snafu(display(
        "`raft_url` and `UPGRID_RAFT_URL` are obsolete; use `raft_port` and `reachable_addresses`"
    ))]
    ObsoleteRaftUrl,

    #[snafu(display("configured reachable address {value:?} is invalid: {source}"))]
    ConfiguredReachableAddress {
        value: String,
        #[snafu(source(from(crate::ReachableAddressError, Box::new)))]
        source: Box<crate::ReachableAddressError>,
    },

    #[snafu(display(
        "stored reachable address {value:?} in {} is invalid: {source}",
        path.display()
    ))]
    StoredReachableAddress {
        path: PathBuf,
        value: String,
        #[snafu(source(from(crate::ReachableAddressError, Box::new)))]
        source: Box<crate::ReachableAddressError>,
    },

    #[snafu(display("failed to read {}: {source}", path.display()))]
    Read { path: PathBuf, source: io::Error },

    #[snafu(display("failed to write {}: {source}", path.display()))]
    Write { path: PathBuf, source: io::Error },

    #[snafu(display("failed to remove {}: {source}", path.display()))]
    Remove { path: PathBuf, source: io::Error },

    #[snafu(display(
        "stored discovery service URL {value:?} in {} is invalid: {source}",
        path.display()
    ))]
    DiscoveryUrl {
        path: PathBuf,
        value: String,
        source: url::ParseError,
    },

    #[snafu(display(
        "stored discovery service URL {value:?} in {} must use http or https and contain no credentials, query, or fragment",
        path.display()
    ))]
    DiscoveryUrlScheme { path: PathBuf, value: String },

    #[snafu(display("stored pending join link in {} is invalid: {source}", path.display()))]
    StoredJoinLink {
        path: PathBuf,
        source: crate::admission::Error,
    },

    #[snafu(display(
        "{} contains {count} discovery service URLs; the limit is {limit}",
        path.display()
    ))]
    TooManyDiscoveryUrls {
        path: PathBuf,
        count: usize,
        limit: usize,
    },

    #[snafu(display("stored Node ID in {} is invalid: {source}", path.display()))]
    NodeId { path: PathBuf, source: uuid::Error },

    #[snafu(display("node name must contain 1 to 64 printable characters"))]
    NodeNameInvalid,

    #[snafu(display("invalid OOBE phase: {phase}"))]
    OobePhaseInvalid { phase: String },

    #[snafu(display("configured deployment key does not match the data directory"))]
    DeploymentKeyMismatch,

    #[snafu(display(
        "configured QUIC certificate-authority key does not match the data directory"
    ))]
    QuicCaKeyMismatch,

    #[snafu(display("joining a cluster requires a valid up:// join link"))]
    JoinLinkRequired,

    #[snafu(transparent)]
    Cipher { source: crate::secret::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
