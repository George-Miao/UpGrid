//! HTTP server lifecycle errors.

use std::io;

use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("failed to bind API at {address}: {source}"))]
    Bind { address: String, source: io::Error },

    #[snafu(display("failed to configure API listener: {source}"))]
    Listener { source: io::Error },

    #[snafu(display("failed to spawn API thread: {source}"))]
    ThreadSpawn { source: io::Error },

    #[snafu(display("failed to build API runtime: {source}"))]
    Runtime { source: io::Error },

    #[snafu(display("failed to configure API TLS: {source}"))]
    Tls { source: io::Error },

    #[snafu(display("API server stopped: {source}"))]
    Serve { source: io::Error },

    #[snafu(display("failed to render OpenAPI document: {source}"))]
    OpenApi { source: serde_json::Error },

    #[snafu(display("Cluster setup state was poisoned"))]
    SetupStatePoisoned,

    #[snafu(display("OOBE stopped without a Cluster choice"))]
    SetupStopped,

    #[snafu(transparent)]
    Config { source: upgrid_config::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
