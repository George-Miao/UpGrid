use compio_quic::crypto::rustls;
use openraft::error::{ClientWriteError, Fatal, InitializeError, RaftError};
use snafu::Snafu;
use tarpc::client::RpcError;
use url::Url;

use crate::raft::TC;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
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

    #[snafu(display("Failed to parse input url: {}", source))]
    UrlParse { source: Box<dyn std::error::Error> },

    #[snafu(display("Invalid scheme: {}, expected `up`", url.scheme()))]
    UrlInvalidScheme { url: Url },

    #[snafu(display("URL doesn't have host: {}", url))]
    UrlInvalidHost { url: Url },

    #[snafu(display("Failed to bind QUIC socket on port {}: {}", port, source))]
    QuicBindError { port: u16, source: std::io::Error },

    #[snafu(display("Failed to connect to <{}:{}>: {}", host, port, source))]
    QuicConnectError {
        host: String,
        port: u16,
        source: compio_quic::ConnectError,
    },

    #[snafu(display("Failed to establish connection to <{}:{}>: {}", host, port, source))]
    QuicConnectionError {
        host: String,
        port: u16,
        source: compio_quic::ConnectionError,
    },

    #[snafu(display("Incoming QUIC stream is lost: {}", source))]
    QuicIncomingStreamError {
        source: compio_quic::ConnectionError,
    },

    #[snafu(display("Failed to open QUIC stream: {}", source))]
    QuicOpeningStreamError {
        source: compio_quic::ConnectionError,
    },

    #[snafu(display("Error while resolving {}: {}", host, source))]
    ResolveError {
        host: String,
        source: std::io::Error,
    },

    #[snafu(display("Error while resolving {}: no address found", host))]
    ResolveEmpty { host: String },

    #[snafu(display("TLS error: {}", source))]
    TLSError { source: rustls::Error },

    #[snafu(display("Failed to create endpoint: {}", source))]
    EndpointCreationError { source: std::io::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
